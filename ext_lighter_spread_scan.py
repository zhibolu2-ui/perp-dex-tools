#!/usr/bin/env python3
"""
Extended vs Lighter 多币种价差扫描器
实时采集 BTC/ETH/SOL/HYPE/XRP 在两个交易所之间的价差数据
运行 10 分钟，输出统计报告
"""
import asyncio
import json
import time
import statistics
import sys
from dataclasses import dataclass, field
from typing import Optional, Dict, List
from datetime import datetime

import websockets

LIGHTER_WS = "wss://mainnet.zklighter.elliot.ai/stream"
EXT_WS_BASE = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1"

LIGHTER_MARKETS = {"ETH": 0, "BTC": 1, "SOL": 2}
EXT_SYMBOLS = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "SOL": "SOL-USD",
    "HYPE": "HYPE-USD",
    "XRP": "XRP-USD",
}

SCAN_SECONDS = 600  # 10 min
SAMPLE_INTERVAL = 0.5  # sample every 500ms

EXT_FEE_BPS = 2.025  # 2.25 * 0.9 (10% rebate)


@dataclass
class SymbolData:
    symbol: str
    l_bid: Optional[float] = None
    l_ask: Optional[float] = None
    x_bid: Optional[float] = None
    x_ask: Optional[float] = None
    l_bid_qty: float = 0
    l_ask_qty: float = 0
    x_bid_qty: float = 0
    x_ask_qty: float = 0
    l_ts: float = 0
    x_ts: float = 0
    l_connected: bool = False
    x_connected: bool = False
    spreads: List[float] = field(default_factory=list)
    abs_spreads: List[float] = field(default_factory=list)
    open_opportunities: int = 0  # spread > 4.05 bps (breakeven)
    big_opportunities: int = 0   # spread > 6 bps
    convergence_times: List[float] = field(default_factory=list)
    l_bid_sizes: List[float] = field(default_factory=list)
    l_ask_sizes: List[float] = field(default_factory=list)
    x_bid_sizes: List[float] = field(default_factory=list)
    x_ask_sizes: List[float] = field(default_factory=list)
    last_open_time: float = 0
    in_position: bool = False

    @property
    def basis_bps(self) -> Optional[float]:
        if self.l_bid and self.l_ask and self.x_bid and self.x_ask:
            l_mid = (self.l_bid + self.l_ask) / 2
            x_mid = (self.x_bid + self.x_ask) / 2
            if l_mid > 0:
                return (l_mid - x_mid) / l_mid * 10000
            return (l_mid - x_mid) / x_mid * 10000
        return None

    @property
    def open_spread_buy(self) -> Optional[float]:
        """Lighter buy (at ask) vs Extended sell (at bid) spread"""
        if self.l_ask and self.x_bid:
            mid = (self.l_ask + self.x_bid) / 2
            return (self.x_bid - self.l_ask) / mid * 10000
        return None

    @property
    def open_spread_sell(self) -> Optional[float]:
        """Lighter sell (at bid) vs Extended buy (at ask) spread"""
        if self.l_bid and self.x_ask:
            mid = (self.l_bid + self.x_ask) / 2
            return (self.l_bid - self.x_ask) / mid * 10000
        return None


data: Dict[str, SymbolData] = {}
stop_event = asyncio.Event()


async def lighter_ws():
    id_to_sym = {v: k for k, v in LIGHTER_MARKETS.items()}
    # Lighter needs full orderbook snapshot to maintain BBO
    l_books: Dict[int, dict] = {}  # market_id -> {"bids": {price: size}, "asks": {price: size}}

    while not stop_event.is_set():
        try:
            async with websockets.connect(LIGHTER_WS, ping_interval=10, ping_timeout=5) as ws:
                for mid in LIGHTER_MARKETS.values():
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{mid}"}))
                    l_books[mid] = {"bids": {}, "asks": {}}
                print(f"[Lighter] subscribed: {list(LIGHTER_MARKETS.keys())}")

                async for raw in ws:
                    if stop_event.is_set():
                        break
                    try:
                        msg = json.loads(raw)
                        mt = msg.get("type", "")
                        if mt not in ("subscribed/order_book", "update/order_book"):
                            continue
                        ch = msg.get("channel", "")
                        if ":" not in ch:
                            continue
                        mid = int(ch.split(":")[1])
                        sym = id_to_sym.get(mid)
                        if not sym or sym not in data:
                            continue
                        d = data[sym]
                        ob = msg.get("order_book", {})
                        bids_raw = ob.get("bids", [])
                        asks_raw = ob.get("asks", [])

                        book = l_books[mid]
                        if mt == "subscribed/order_book":
                            book["bids"] = {}
                            book["asks"] = {}
                            for b in bids_raw:
                                book["bids"][float(b["price"])] = float(b["size"])
                            for a in asks_raw:
                                book["asks"][float(a["price"])] = float(a["size"])
                        else:
                            for b in bids_raw:
                                p, s = float(b["price"]), float(b["size"])
                                if s == 0:
                                    book["bids"].pop(p, None)
                                else:
                                    book["bids"][p] = s
                            for a in asks_raw:
                                p, s = float(a["price"]), float(a["size"])
                                if s == 0:
                                    book["asks"].pop(p, None)
                                else:
                                    book["asks"][p] = s

                        if book["bids"]:
                            best_bid = max(book["bids"].keys())
                            d.l_bid = best_bid
                            d.l_bid_qty = book["bids"][best_bid]
                        if book["asks"]:
                            best_ask = min(book["asks"].keys())
                            d.l_ask = best_ask
                            d.l_ask_qty = book["asks"][best_ask]
                        d.l_ts = time.time()
                        d.l_connected = True
                    except Exception:
                        pass
        except Exception as e:
            print(f"[Lighter] WS error: {e}, reconnecting...")
            await asyncio.sleep(1)


async def extended_ws_single(symbol: str):
    ext_sym = EXT_SYMBOLS.get(symbol)
    if not ext_sym:
        return
    url = f"{EXT_WS_BASE}/orderbooks/{ext_sym}?depth=1"

    while not stop_event.is_set():
        try:
            async with websockets.connect(url, ping_interval=10, ping_timeout=5) as ws:
                print(f"[Extended] connected: {symbol}")
                async for raw in ws:
                    if stop_event.is_set():
                        break
                    try:
                        msg = json.loads(raw)
                        if symbol not in data:
                            continue
                        d = data[symbol]
                        dd = msg.get("data", {})
                        bids = dd.get("b", [])
                        asks = dd.get("a", [])
                        if bids:
                            d.x_bid = float(bids[0]["p"])
                            d.x_bid_qty = float(bids[0].get("q", 0))
                        if asks:
                            d.x_ask = float(asks[0]["p"])
                            d.x_ask_qty = float(asks[0].get("q", 0))
                        d.x_ts = time.time()
                        d.x_connected = True
                    except Exception:
                        pass
        except Exception as e:
            print(f"[Extended] {symbol} WS error: {e}, reconnecting...")
            await asyncio.sleep(1)


async def sampler():
    """Sample spreads at regular intervals"""
    await asyncio.sleep(3)  # wait for connections
    start = time.time()
    sample_count = 0
    breakeven = EXT_FEE_BPS  # single-leg breakeven for opening

    while not stop_event.is_set() and (time.time() - start) < SCAN_SECONDS:
        now = time.time()
        for sym, d in data.items():
            if not d.l_connected or not d.x_connected:
                continue
            if d.l_bid is None or d.x_bid is None:
                continue
            staleness = max(now - d.l_ts, now - d.x_ts)
            if staleness > 2.0:
                continue

            spread_buy = d.open_spread_buy
            spread_sell = d.open_spread_sell
            if spread_buy is None or spread_sell is None:
                continue

            best_spread = max(spread_buy, spread_sell)
            d.spreads.append(best_spread)
            d.abs_spreads.append(abs(best_spread))

            if best_spread > breakeven:
                d.open_opportunities += 1
            if best_spread > 6.0:
                d.big_opportunities += 1

            d.l_bid_sizes.append(d.l_bid_qty)
            d.l_ask_sizes.append(d.l_ask_qty)
            d.x_bid_sizes.append(d.x_bid_qty)
            d.x_ask_sizes.append(d.x_ask_qty)

            # convergence tracking
            if not d.in_position and best_spread > breakeven:
                d.in_position = True
                d.last_open_time = now
            elif d.in_position and best_spread < 1.0:
                d.convergence_times.append(now - d.last_open_time)
                d.in_position = False

        sample_count += 1
        elapsed = time.time() - start
        if sample_count % 60 == 0:
            print(f"[{elapsed:.0f}s/{SCAN_SECONDS}s] samples={sample_count}")
            for sym, d in data.items():
                n = len(d.spreads)
                if n > 0:
                    avg = statistics.mean(d.spreads)
                    mx = max(d.spreads)
                    pct_above = d.open_opportunities / n * 100
                    print(f"  {sym}: avg={avg:.2f}bps max={mx:.2f}bps >BE={pct_above:.1f}% samples={n}")

        await asyncio.sleep(SAMPLE_INTERVAL)

    stop_event.set()


def print_report():
    breakeven = EXT_FEE_BPS
    total_breakeven = EXT_FEE_BPS * 2  # open + close

    print("\n" + "=" * 90)
    print(f"  Extended + Lighter 多币种价差扫描报告")
    print(f"  扫描时长: {SCAN_SECONDS}s  采样间隔: {SAMPLE_INTERVAL}s")
    print(f"  Extended Fee: {EXT_FEE_BPS:.3f}bps (含10%反佣)")
    print(f"  单腿盈亏平衡: {breakeven:.3f}bps  完整周期盈亏平衡: {total_breakeven:.3f}bps")
    print("=" * 90)

    results = []
    for sym in ["BTC", "ETH", "SOL", "HYPE", "XRP"]:
        d = data.get(sym)
        if not d or len(d.spreads) == 0:
            print(f"\n  {sym}: 无数据 (Lighter不支持或Extended不支持)")
            results.append((sym, None))
            continue

        spreads = d.spreads
        n = len(spreads)
        avg = statistics.mean(spreads)
        med = statistics.median(spreads)
        std = statistics.stdev(spreads) if n > 1 else 0
        mx = max(spreads)
        mn = min(spreads)
        p75 = sorted(spreads)[int(n * 0.75)]
        p90 = sorted(spreads)[int(n * 0.90)]
        p95 = sorted(spreads)[int(n * 0.95)]

        pct_above_be = d.open_opportunities / n * 100
        pct_above_6 = d.big_opportunities / n * 100

        avg_conv = statistics.mean(d.convergence_times) if d.convergence_times else float('nan')
        med_conv = statistics.median(d.convergence_times) if d.convergence_times else float('nan')
        n_conv = len(d.convergence_times)

        avg_l_bid_sz = statistics.mean(d.l_bid_sizes) if d.l_bid_sizes else 0
        avg_l_ask_sz = statistics.mean(d.l_ask_sizes) if d.l_ask_sizes else 0
        avg_x_bid_sz = statistics.mean(d.x_bid_sizes) if d.x_bid_sizes else 0
        avg_x_ask_sz = statistics.mean(d.x_ask_sizes) if d.x_ask_sizes else 0

        score = avg * (pct_above_be / 100) * (1 / (avg_conv if avg_conv > 0 and avg_conv == avg_conv else 999))

        print(f"\n{'─' * 90}")
        print(f"  {sym}")
        print(f"{'─' * 90}")
        print(f"  价差统计 (bps):")
        print(f"    平均={avg:.2f}  中位={med:.2f}  标准差={std:.2f}")
        print(f"    最小={mn:.2f}  最大={mx:.2f}")
        print(f"    P75={p75:.2f}  P90={p90:.2f}  P95={p95:.2f}")
        print(f"  机会频率:")
        print(f"    >盈亏平衡({breakeven:.1f}bps): {d.open_opportunities}/{n} = {pct_above_be:.1f}%")
        print(f"    >6bps: {d.big_opportunities}/{n} = {pct_above_6:.1f}%")
        print(f"  收敛时间:")
        print(f"    样本数={n_conv}  平均={avg_conv:.1f}s  中位={med_conv:.1f}s")
        print(f"  流动性 (BBO size):")
        print(f"    Lighter: bid={avg_l_bid_sz:.4f} ask={avg_l_ask_sz:.4f}")
        print(f"    Extended: bid={avg_x_bid_sz:.4f} ask={avg_x_ask_sz:.4f}")
        print(f"  综合评分: {score:.4f}")

        results.append((sym, score))

    print(f"\n{'=' * 90}")
    print(f"  排名 (综合评分 = 平均价差 × 机会占比 × 收敛速度)")
    print(f"{'=' * 90}")
    valid = [(s, sc) for s, sc in results if sc is not None and sc == sc]
    valid.sort(key=lambda x: x[1], reverse=True)
    for i, (sym, sc) in enumerate(valid, 1):
        print(f"  #{i} {sym}: score={sc:.4f}")
    if not valid:
        print("  无有效数据")

    none_syms = [s for s, sc in results if sc is None]
    if none_syms:
        print(f"\n  不支持/无数据: {', '.join(none_syms)}")

    print(f"\n{'=' * 90}")
    print(f"  结论")
    print(f"{'=' * 90}")
    if valid:
        best = valid[0][0]
        print(f"  最佳套利币种: {best}")
        d = data[best]
        avg_sp = statistics.mean(d.spreads)
        print(f"  平均价差: {avg_sp:.2f}bps")
        print(f"  完整周期盈亏平衡: {total_breakeven:.2f}bps")
        net = avg_sp - breakeven
        print(f"  单腿预期净收益: {net:.2f}bps (开仓)")
        print(f"  建议: 开仓spread阈值 >= {breakeven + 1:.1f}bps")


async def run():
    symbols_to_scan = list(EXT_SYMBOLS.keys())
    for sym in symbols_to_scan:
        data[sym] = SymbolData(symbol=sym)

    tasks = [
        asyncio.create_task(lighter_ws()),
        asyncio.create_task(sampler()),
    ]
    for sym in symbols_to_scan:
        tasks.append(asyncio.create_task(extended_ws_single(sym)))

    await asyncio.gather(*tasks, return_exceptions=True)
    print_report()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n中断")
        print_report()
