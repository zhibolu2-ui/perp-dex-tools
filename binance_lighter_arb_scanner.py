#!/usr/bin/env python3
"""
币安(挂单) vs Lighter(市价) 深度加权价差套利扫描器
====================================================
币安使用 maker 挂单 (2 bps, 无滑点)
Lighter 使用 taker 市价单 (0 bps, 有滑点)

核心计算:
  - 往返费用 = 2 × (Lighter_taker 0 + Binance_maker 2) = 4 bps
  - 滑点仅在 Lighter 侧 (市价吃单穿越盘口)
  - 币安挂单无滑点 (你设定价格等待成交)
  - 最小入场价差 = Lighter开仓滑点 + Lighter平仓滑点 + 4bps费用

用法:
    python binance_lighter_arb_scanner.py
    python binance_lighter_arb_scanner.py --size 500
    python binance_lighter_arb_scanner.py --size 1000 --top 30
"""

import argparse
import asyncio
import json
import time
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import aiohttp
import websockets

# ─── 常量 ───────────────────────────────────────────────────────────────────────

LIGHTER_REST = "https://mainnet.zklighter.elliot.ai"
LIGHTER_WS = "wss://mainnet.zklighter.elliot.ai/stream"
BINANCE_FAPI = "https://fapi.binance.com"

LIGHTER_TAKER_BPS = 0.0
LIGHTER_MAKER_BPS = 0.0
BINANCE_MAKER_BPS = 2.0

# 往返费用: 开仓(L_taker + B_maker) + 平仓(L_taker + B_maker) = 2*(0+2) = 4 bps
ROUND_TRIP_FEE_BPS = 2.0 * (LIGHTER_TAKER_BPS + BINANCE_MAKER_BPS)

SYMBOL_REMAP = {
    "1000PEPE": "1000PEPE",
    "1000SHIB": "1000SHIB",
    "1000BONK": "1000BONK",
    "1000FLOKI": "1000FLOKI",
    "1000TOSHI": "TOSHI",
}

EXCLUDED_CATEGORIES = {
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD",
    "USDKRW",
    "XAU", "XAG", "XCU", "XPD", "XPT", "WTI", "URA",
    "NVDA", "TSLA", "AAPL", "AMZN", "MSFT", "GOOGL", "META", "HOOD", "COIN",
    "PLTR", "INTC", "AMD", "ASML", "MSTR", "SNDK",
    "SPY", "QQQ", "IWM", "DIA", "BOTZ", "MAGS",
    "SAMSUNG", "HYUNDAI", "SKHYNIX", "KRCOMP", "HANMI",
    "PAXG",
}

WS_BATCH_SIZE = 25


@dataclass
class OrderBookLevel:
    price: float
    size: float


@dataclass
class DepthEatInfo:
    """$X 市价单吃盘口的详细信息。"""
    levels_eaten: int          # 吃了几档
    total_levels: int          # 盘口总档数
    vwap_price: float          # 成交加权平均价
    best_price: float          # 最优价 (第1档)
    worst_fill_price: float    # 最差成交价 (最后一档)
    slippage_bps: float        # VWAP 相对最优价的滑点
    price_impact_bps: float    # 最差成交价相对最优价的冲击
    filled_usd: float          # 实际成交 USD
    filled_qty: float          # 实际成交数量
    sufficient: bool           # 深度是否充足
    level_details: List[Tuple[float, float, float]]  # [(price, size, fill_usd), ...]


@dataclass
class PairResult:
    symbol: str
    lighter_market_id: int
    lighter_mid: float
    binance_mid: float
    raw_spread_bps: float

    lighter_best_bid: float
    lighter_best_ask: float
    lighter_spread_bps: float
    binance_best_bid: float
    binance_best_ask: float
    binance_spread_bps: float

    lighter_bid_depth_usd: float
    lighter_ask_depth_usd: float
    binance_bid_depth_usd: float
    binance_ask_depth_usd: float

    # Lighter 市价单吃盘口的详细分析
    lighter_buy_eat: Optional[DepthEatInfo] = None   # Lighter 买入 (吃 asks)
    lighter_sell_eat: Optional[DepthEatInfo] = None   # Lighter 卖出 (吃 bids)

    # 收敛策略核心指标
    lighter_buy_slip_bps: float = 0.0     # Lighter 买入滑点
    lighter_sell_slip_bps: float = 0.0    # Lighter 卖出滑点
    total_lighter_slip_bps: float = 0.0   # 往返 Lighter 滑点 (买+卖)
    min_entry_spread_bps: float = 999.0   # 最小入场价差 = 往返滑点 + 费用
    net_after_cost_bps: float = -999.0    # 当前价差 - 最小入场 (正数=可立刻套利)
    convergence_score: str = ""
    liquidity_grade: str = ""
    depth_ok: bool = False


# ─── 核心函数 ────────────────────────────────────────────────────────────────────

def eat_depth(
    levels: List[OrderBookLevel], size_usd: float
) -> Optional[DepthEatInfo]:
    """计算 $size_usd 市价单吃穿盘口的详细信息。"""
    if not levels or size_usd <= 0:
        return None

    best_price = levels[0].price
    remaining_usd = size_usd
    total_cost = 0.0
    total_qty = 0.0
    worst_price = best_price
    eaten = 0
    details = []

    for lvl in levels:
        level_usd = lvl.price * lvl.size
        fill_usd = min(remaining_usd, level_usd)
        fill_qty = fill_usd / lvl.price
        total_cost += fill_usd
        total_qty += fill_qty
        remaining_usd -= fill_usd
        worst_price = lvl.price
        eaten += 1
        details.append((lvl.price, lvl.size, fill_usd))

        if remaining_usd <= 0.01:
            break

    sufficient = remaining_usd <= 0.01
    vwap_price = total_cost / total_qty if total_qty > 0 else best_price
    slip = abs(vwap_price - best_price) / best_price * 10_000.0 if best_price > 0 else 0
    impact = abs(worst_price - best_price) / best_price * 10_000.0 if best_price > 0 else 0

    return DepthEatInfo(
        levels_eaten=eaten,
        total_levels=len(levels),
        vwap_price=vwap_price,
        best_price=best_price,
        worst_fill_price=worst_price,
        slippage_bps=slip,
        price_impact_bps=impact,
        filled_usd=total_cost,
        filled_qty=total_qty,
        sufficient=sufficient,
        level_details=details[:10],
    )


def depth_usd(levels: List[OrderBookLevel], n: int = 20) -> float:
    return sum(lvl.price * lvl.size for lvl in levels[:n])


def liquidity_grade(bid_depth: float, ask_depth: float) -> str:
    total = bid_depth + ask_depth
    if total >= 500_000: return "A+"
    elif total >= 200_000: return "A"
    elif total >= 100_000: return "B+"
    elif total >= 50_000: return "B"
    elif total >= 20_000: return "C+"
    elif total >= 10_000: return "C"
    elif total >= 5_000: return "D"
    else: return "F"


async def fetch_lighter_markets(session: aiohttp.ClientSession) -> List[dict]:
    url = f"{LIGHTER_REST}/api/v1/orderBooks"
    async with session.get(url) as resp:
        data = await resp.json()
    books = data.get("order_books", [])
    return [
        b for b in books
        if b.get("status") == "active"
        and b.get("market_type") == "perp"
        and b["symbol"] not in EXCLUDED_CATEGORIES
    ]


async def fetch_binance_symbols(session: aiohttp.ClientSession) -> set:
    url = f"{BINANCE_FAPI}/fapi/v1/exchangeInfo"
    async with session.get(url) as resp:
        data = await resp.json()
    symbols = data.get("symbols", [])
    return {
        s["baseAsset"]
        for s in symbols
        if s.get("status") == "TRADING"
        and s.get("contractType") == "PERPETUAL"
        and s["quoteAsset"] == "USDT"
    }


async def fetch_lighter_orderbooks_ws(
    market_ids: List[int],
) -> Dict[int, Tuple[List[OrderBookLevel], List[OrderBookLevel]]]:
    result: Dict[int, Tuple[List[OrderBookLevel], List[OrderBookLevel]]] = {}
    batches = [
        market_ids[i:i + WS_BATCH_SIZE]
        for i in range(0, len(market_ids), WS_BATCH_SIZE)
    ]

    async def fetch_batch(batch: List[int]):
        try:
            async with websockets.connect(LIGHTER_WS, close_timeout=5) as ws:
                for mid in batch:
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": f"order_book/{mid}",
                    }))
                pending = set(batch)
                deadline = time.time() + 15
                while pending and time.time() < deadline:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=3)
                    except asyncio.TimeoutError:
                        continue
                    data = json.loads(raw)
                    msg_type = data.get("type", "")
                    if msg_type == "subscribed/order_book":
                        channel = data.get("channel", "")
                        try:
                            mid = int(channel.split(":")[1])
                        except (IndexError, ValueError):
                            continue
                        ob = data.get("order_book", {})
                        bids = [
                            OrderBookLevel(float(b["price"]), float(b["size"]))
                            for b in ob.get("bids", [])
                            if float(b.get("size", 0)) > 0
                        ]
                        asks = [
                            OrderBookLevel(float(a["price"]), float(a["size"]))
                            for a in ob.get("asks", [])
                            if float(a.get("size", 0)) > 0
                        ]
                        bids.sort(key=lambda x: x.price, reverse=True)
                        asks.sort(key=lambda x: x.price)
                        result[mid] = (bids, asks)
                        pending.discard(mid)
                    elif msg_type == "ping":
                        await ws.send(json.dumps({"type": "pong"}))
        except Exception as e:
            print(f"    Warning: WebSocket batch error: {e}")

    await asyncio.gather(*(fetch_batch(b) for b in batches))
    return result


async def fetch_binance_orderbook(
    session: aiohttp.ClientSession, symbol: str
) -> Tuple[List[OrderBookLevel], List[OrderBookLevel]]:
    bn_symbol = f"{symbol}USDT"
    url = f"{BINANCE_FAPI}/fapi/v1/depth?symbol={bn_symbol}&limit=50"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
        if "code" in data:
            return [], []
        bids = [OrderBookLevel(float(b[0]), float(b[1])) for b in data.get("bids", [])]
        asks = [OrderBookLevel(float(a[0]), float(a[1])) for a in data.get("asks", [])]
        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)
        return bids, asks
    except Exception:
        return [], []


def analyze_pair(
    symbol: str, market_id: int,
    l_bids: List[OrderBookLevel], l_asks: List[OrderBookLevel],
    b_bids: List[OrderBookLevel], b_asks: List[OrderBookLevel],
    trade_size_usd: float,
) -> Optional[PairResult]:
    if not l_bids or not l_asks or not b_bids or not b_asks:
        return None

    l_best_bid, l_best_ask = l_bids[0].price, l_asks[0].price
    b_best_bid, b_best_ask = b_bids[0].price, b_asks[0].price
    l_mid = (l_best_bid + l_best_ask) / 2.0
    b_mid = (b_best_bid + b_best_ask) / 2.0
    if l_mid <= 0 or b_mid <= 0:
        return None

    raw_spread = (b_mid - l_mid) / l_mid * 10_000.0
    l_spread = (l_best_ask - l_best_bid) / l_mid * 10_000.0
    b_spread = (b_best_ask - b_best_bid) / b_mid * 10_000.0
    l_bid_d, l_ask_d = depth_usd(l_bids), depth_usd(l_asks)
    b_bid_d, b_ask_d = depth_usd(b_bids), depth_usd(b_asks)

    # Lighter 市价单吃盘口分析
    buy_eat = eat_depth(l_asks, trade_size_usd)
    sell_eat = eat_depth(l_bids, trade_size_usd)

    buy_slip = buy_eat.slippage_bps if buy_eat else 999.0
    sell_slip = sell_eat.slippage_bps if sell_eat else 999.0
    total_slip = buy_slip + sell_slip

    # 最小入场价差 = Lighter开仓滑点 + Lighter平仓滑点 + 往返费用
    # (币安挂单无滑点, 所以只算 Lighter 侧)
    min_entry = total_slip + ROUND_TRIP_FEE_BPS

    depth_ok = (buy_eat is not None and buy_eat.sufficient and
                sell_eat is not None and sell_eat.sufficient)

    # 当前价差能否覆盖成本
    net_after = abs(raw_spread) - min_entry

    if min_entry <= 5:
        score = "极优"
    elif min_entry <= 8:
        score = "优秀"
    elif min_entry <= 12:
        score = "良好"
    elif min_entry <= 20:
        score = "可行"
    elif min_entry <= 40:
        score = "困难"
    else:
        score = "不可行"

    return PairResult(
        symbol=symbol, lighter_market_id=market_id,
        lighter_mid=l_mid, binance_mid=b_mid,
        raw_spread_bps=raw_spread,
        lighter_best_bid=l_best_bid, lighter_best_ask=l_best_ask,
        lighter_spread_bps=l_spread,
        binance_best_bid=b_best_bid, binance_best_ask=b_best_ask,
        binance_spread_bps=b_spread,
        lighter_bid_depth_usd=l_bid_d, lighter_ask_depth_usd=l_ask_d,
        binance_bid_depth_usd=b_bid_d, binance_ask_depth_usd=b_ask_d,
        lighter_buy_eat=buy_eat, lighter_sell_eat=sell_eat,
        lighter_buy_slip_bps=buy_slip, lighter_sell_slip_bps=sell_slip,
        total_lighter_slip_bps=total_slip,
        min_entry_spread_bps=min_entry,
        net_after_cost_bps=net_after,
        convergence_score=score,
        liquidity_grade=liquidity_grade(l_bid_d, l_ask_d),
        depth_ok=depth_ok,
    )


# ─── 主流程 ──────────────────────────────────────────────────────────────────────

async def scan(args):
    trade_size = args.size

    connector = aiohttp.TCPConnector(limit=50, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        print("  [1/4] 获取交易所市场信息...")
        lighter_markets, binance_bases = await asyncio.gather(
            fetch_lighter_markets(session),
            fetch_binance_symbols(session),
        )

        lighter_symbols = {}
        for m in lighter_markets:
            lighter_symbols[m["symbol"]] = m["market_id"]

        matched = []
        for sym, mid in lighter_symbols.items():
            bn_sym = SYMBOL_REMAP.get(sym, sym)
            if bn_sym in binance_bases:
                matched.append((sym, mid, bn_sym))

        print(f"  Lighter 活跃 perp:     {len(lighter_symbols)}")
        print(f"  币安 USDT-M 永续合约: {len(binance_bases)}")
        print(f"  共同交易对:            {len(matched)}")
        print()

        if not matched:
            print("  没有找到共同交易对！")
            return

        market_ids = [mid for _, mid, _ in matched]
        n_batches = (len(market_ids) + WS_BATCH_SIZE - 1) // WS_BATCH_SIZE
        print(f"  [2/4] 并发获取 {len(matched)} 个交易对盘口...")
        print(f"         Lighter: WebSocket {n_batches} 批  |  币安: REST 并发")

        lighter_task = fetch_lighter_orderbooks_ws(market_ids)
        binance_tasks = [fetch_binance_orderbook(session, bn) for _, _, bn in matched]
        lighter_obs_dict, *binance_obs = await asyncio.gather(lighter_task, *binance_tasks)

        l_ok = sum(1 for mid in market_ids if mid in lighter_obs_dict)
        b_ok = sum(1 for ob in binance_obs if ob[0] or ob[1])
        print(f"         成功: Lighter {l_ok}/{len(market_ids)}  币安 {b_ok}/{len(matched)}")

        print(f"  [3/4] 分析 ${trade_size:,.0f} 开仓量的深度穿透和价差...")
        results: List[PairResult] = []
        for i, (sym, mid, bn_sym) in enumerate(matched):
            l_bids, l_asks = lighter_obs_dict.get(mid, ([], []))
            b_bids, b_asks = binance_obs[i]
            r = analyze_pair(sym, mid, l_bids, l_asks, b_bids, b_asks, trade_size)
            if r is not None:
                results.append(r)

        results.sort(key=lambda r: r.min_entry_spread_bps)
        print(f"  [4/4] 生成报告...\n")
        print_report(results, trade_size, args)


def print_report(results: List[PairResult], trade_size: float, args):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    has_data = [r for r in results if r.depth_ok]
    instant_arb = [r for r in has_data if r.net_after_cost_bps > 0]

    counts = {}
    for r in has_data:
        counts[r.convergence_score] = counts.get(r.convergence_score, 0) + 1

    W = 140
    print("=" * W)
    print(f"  币安(挂单) vs Lighter(市价) 价差收敛套利扫描")
    print(f"  时间: {now}   开仓量: ${trade_size:,.0f}")
    print(f"  币安 maker 2bps(无滑点) + Lighter taker 0bps(有滑点) → 往返费用 {ROUND_TRIP_FEE_BPS:.0f} bps")
    print("=" * W)

    print(f"\n  统计总览")
    print(f"  ├─ 有效数据:           {len(has_data)}/{len(results)}")
    print(f"  ├─ 当前可即时套利:     {len(instant_arb)} 个 (当前价差 > 最小入场)")
    print(f"  ├─ 往返费用:           {ROUND_TRIP_FEE_BPS:.0f} bps (固定)")
    print(f"  │")
    print(f"  ├─ 收敛可行性 (最小入场价差 = Lighter往返滑点 + {ROUND_TRIP_FEE_BPS:.0f}bps费用):")
    for label in ["极优", "优秀", "良好", "可行", "困难", "不可行"]:
        c = counts.get(label, 0)
        if c > 0:
            ranges = {
                "极优": "≤5bps", "优秀": "≤8bps", "良好": "≤12bps",
                "可行": "≤20bps", "困难": "≤40bps", "不可行": ">40bps",
            }
            print(f"  │   {label} ({ranges[label]}): {c:>3} 个")
    print(f"  └─ 总计: {len(has_data)} 个")

    # ─── 核心排名表 ───
    top_n = args.top
    display = has_data[:top_n] if top_n > 0 else has_data

    print(f"\n  所有交易对排名 (按最小入场价差, ${trade_size:,.0f} 开仓)")
    print(f"  {'─' * (W-4)}")
    print(
        f"  {'#':>3}  {'币种':<12}"
        f" {'当前价差':>8} {'L盘口差':>7} {'B盘口差':>7}"
        f" │ {'L买吃档':>7} {'L买滑点':>7} {'L卖吃档':>7} {'L卖滑点':>7}"
        f" │ {'往返滑点':>8} {'费用':>5} {'最小入场':>8} {'评分':<6}"
        f" │ {'差值':>7} {'L深度$':>9} {'等级':>4}"
    )
    print(f"  {'─' * (W-4)}")

    for idx, r in enumerate(display):
        be = r.lighter_buy_eat
        se = r.lighter_sell_eat
        b_levels = f"{be.levels_eaten}/{be.total_levels}" if be else "N/A"
        s_levels = f"{se.levels_eaten}/{se.total_levels}" if se else "N/A"
        l_total_d = r.lighter_bid_depth_usd + r.lighter_ask_depth_usd

        marker = ""
        if r.net_after_cost_bps > 0:
            marker = " ★"

        print(
            f"  {idx+1:>3}  {r.symbol:<12}"
            f" {r.raw_spread_bps:>+8.2f} {r.lighter_spread_bps:>7.2f} {r.binance_spread_bps:>7.2f}"
            f" │ {b_levels:>7} {r.lighter_buy_slip_bps:>7.2f} {s_levels:>7} {r.lighter_sell_slip_bps:>7.2f}"
            f" │ {r.total_lighter_slip_bps:>8.2f} {ROUND_TRIP_FEE_BPS:>5.0f} {r.min_entry_spread_bps:>8.2f}"
            f" {r.convergence_score:<6}"
            f" │ {r.net_after_cost_bps:>+7.2f} {l_total_d:>9,.0f} {r.liquidity_grade:>4}{marker}"
        )

    print(f"  {'─' * (W-4)}")
    print(f"  (差值 = |当前价差| - 最小入场价差, 正数=可即时套利)")

    # ─── 可即时套利的详细分析 ───
    if instant_arb:
        print(f"\n{'=' * W}")
        print(f"  当前可即时套利交易对 ({len(instant_arb)} 个) — Lighter 盘口深度穿透详情")
        print(f"{'=' * W}")

        for r in sorted(instant_arb, key=lambda x: x.net_after_cost_bps, reverse=True):
            _print_pair_detail(r, trade_size)

    # ─── 推荐币种(低成本) 的详细分析 ───
    recommended = [r for r in has_data if r.convergence_score in ("极优", "优秀")]
    show_detail = [r for r in recommended if r not in instant_arb][:10]

    if show_detail:
        print(f"\n{'=' * W}")
        print(f"  推荐币种 (低执行成本) — Lighter 盘口深度穿透详情")
        print(f"{'=' * W}")
        for r in show_detail:
            _print_pair_detail(r, trade_size)

    # ─── 总结 ───
    print(f"\n{'=' * W}")
    print(f"  价差收敛策略总结")
    print(f"{'=' * W}")

    if instant_arb:
        print(f"\n  当前可即时套利 ({len(instant_arb)} 个):")
        for r in sorted(instant_arb, key=lambda x: x.net_after_cost_bps, reverse=True):
            direction = "L买B卖" if r.raw_spread_bps > 0 else "B买L卖"
            print(f"    ★ {r.symbol:<12} 当前价差 {r.raw_spread_bps:>+7.2f} bps  "
                  f"最小入场 {r.min_entry_spread_bps:>6.2f} bps  "
                  f"盈余 {r.net_after_cost_bps:>+6.2f} bps  方向: {direction}")

    if recommended:
        print(f"\n  推荐币种 (最小入场 ≤ 8 bps, 共 {len(recommended)} 个):")
        for r in recommended:
            print(f"    {'★' if r.net_after_cost_bps > 0 else '·'} {r.symbol:<12}"
                  f" 最小入场 {r.min_entry_spread_bps:>6.2f} bps"
                  f"  L买滑 {r.lighter_buy_slip_bps:>5.2f}"
                  f"  L卖滑 {r.lighter_sell_slip_bps:>5.2f}"
                  f"  流动性 {r.liquidity_grade}"
                  f"  评分: {r.convergence_score}")

    good_pairs = [r for r in has_data if r.convergence_score == "良好"]
    if good_pairs:
        print(f"\n  良好币种 (最小入场 8-12 bps, 共 {len(good_pairs)} 个):")
        for r in good_pairs:
            print(f"    · {r.symbol:<12}"
                  f" 最小入场 {r.min_entry_spread_bps:>6.2f} bps"
                  f"  L买滑 {r.lighter_buy_slip_bps:>5.2f}"
                  f"  L卖滑 {r.lighter_sell_slip_bps:>5.2f}"
                  f"  流动性 {r.liquidity_grade}")

    print(f"\n  策略说明:")
    print(f"    模式:    币安挂单(maker) + Lighter市价(taker)")
    print(f"    费用:    币安 maker {BINANCE_MAKER_BPS} bps × 2(往返) + Lighter 0 bps = {ROUND_TRIP_FEE_BPS:.0f} bps")
    print(f"    滑点:    仅 Lighter 侧 (币安挂单无滑点)")
    print(f"    开仓量:  ${trade_size:,.0f}")
    print(f"    入场条件: 两所价差 > 最小入场价差 时双边对冲开仓")
    print(f"    出场条件: 价差收敛至 ~0 时平仓获利")
    print(f"    风险提示: 币安挂单可能不成交(需要等待); Lighter 深度可能瞬间变化")
    print()


def _print_pair_detail(r: PairResult, trade_size: float):
    """打印单个交易对的盘口穿透详情。"""
    l_total = r.lighter_bid_depth_usd + r.lighter_ask_depth_usd
    b_total = r.binance_bid_depth_usd + r.binance_ask_depth_usd

    print(f"\n  ┌─ {r.symbol} (Lighter #{r.lighter_market_id}) {'─'*50}")
    print(f"  │  Lighter 中间价: {r.lighter_mid:.6f}  (盘口差 {r.lighter_spread_bps:.2f} bps, "
          f"深度 ${l_total:,.0f}, {r.liquidity_grade})")
    print(f"  │  币安 中间价:    {r.binance_mid:.6f}  (盘口差 {r.binance_spread_bps:.2f} bps, "
          f"深度 ${b_total:,.0f})")
    print(f"  │  当前价差: {r.raw_spread_bps:>+.2f} bps   "
          f"最小入场: {r.min_entry_spread_bps:.2f} bps   "
          f"盈余: {r.net_after_cost_bps:>+.2f} bps")

    for side, eat, label in [
        ("买入(吃asks)", r.lighter_buy_eat, "Lighter 买入"),
        ("卖出(吃bids)", r.lighter_sell_eat, "Lighter 卖出"),
    ]:
        if eat is None:
            continue
        print(f"  │")
        print(f"  │  {label} ${trade_size:,.0f} 穿透分析:")
        print(f"  │  吃了 {eat.levels_eaten} 档 (共 {eat.total_levels} 档)  "
              f"VWAP: {eat.vwap_price:.6f}  最优价: {eat.best_price:.6f}")
        print(f"  │  滑点: {eat.slippage_bps:.2f} bps  "
              f"最差成交价冲击: {eat.price_impact_bps:.2f} bps  "
              f"实际成交: ${eat.filled_usd:,.2f}")

        if eat.levels_eaten <= 8:
            print(f"  │  盘口详情:")
            print(f"  │    {'档位':>4}  {'价格':>14}  {'挂单量':>12}  {'本次吃入$':>10}  {'累计$':>10}")
            cum = 0.0
            for i, (price, size, fill) in enumerate(eat.level_details):
                cum += fill
                marker = " ← 全吃" if fill >= price * size * 0.999 else " ← 部分"
                if fill < 0.01:
                    continue
                print(f"  │    {i+1:>4}  {price:>14.6f}  {size:>12.4f}  ${fill:>9,.2f}  ${cum:>9,.2f}{marker}")
        else:
            print(f"  │  (吃了 {eat.levels_eaten} 档, 仅显示前 5 档)")
            print(f"  │    {'档位':>4}  {'价格':>14}  {'挂单量':>12}  {'本次吃入$':>10}")
            for i, (price, size, fill) in enumerate(eat.level_details[:5]):
                if fill < 0.01:
                    continue
                print(f"  │    {i+1:>4}  {price:>14.6f}  {size:>12.4f}  ${fill:>9,.2f}")

    print(f"  │")
    print(f"  │  成本汇总: L买滑 {r.lighter_buy_slip_bps:.2f} + L卖滑 {r.lighter_sell_slip_bps:.2f} "
          f"+ 费用 {ROUND_TRIP_FEE_BPS:.0f} = 最小入场 {r.min_entry_spread_bps:.2f} bps")
    if r.net_after_cost_bps > 0:
        direction = "Lighter买入 + 币安卖出(挂单)" if r.raw_spread_bps > 0 else "币安买入(挂单) + Lighter卖出"
        print(f"  │  ★ 当前可套利! 方向: {direction}")
        print(f"  │    预期利润: {r.net_after_cost_bps:.2f} bps ≈ "
              f"${trade_size * r.net_after_cost_bps / 10000:.2f}")
    else:
        print(f"  │  等待价差扩大至 {r.min_entry_spread_bps:.1f}+ bps 时入场")
    print(f"  └{'─' * 65}")


def parse_args():
    p = argparse.ArgumentParser(
        description="币安(挂单) vs Lighter(市价) 价差收敛套利扫描器",
    )
    p.add_argument("--size", type=float, default=500.0,
                    help="单次开仓金额 USD (默认: 500)")
    p.add_argument("--top", type=int, default=0,
                    help="只显示前 N 个 (默认: 全部)")
    p.add_argument("--min-spread", type=float, default=3.0,
                    help="最小原始价差 bps (默认: 3.0)")
    return p.parse_args()


def main():
    args = parse_args()
    print()
    print("  ╔══════════════════════════════════════════════════════════════╗")
    print("  ║  币安(挂单) vs Lighter(市价) — 价差收敛套利扫描器         ║")
    print(f"  ║  开仓量: ${args.size:,.0f}   币安maker 2bps   Lighter taker 0bps   ║")
    print("  ╚══════════════════════════════════════════════════════════════╝")
    print()

    try:
        asyncio.run(scan(args))
    except KeyboardInterrupt:
        print("\n  扫描已中断。")


if __name__ == "__main__":
    main()
