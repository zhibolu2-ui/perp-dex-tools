#!/usr/bin/env python3
"""
币安 vs Lighter — 多策略价差套利模拟器
=======================================
同时运行 4 种开平仓策略, 使用相同实时数据, 对比成本和收益.

真实费率 (2026年):
  Lighter: maker 0%  taker 0%  (标准账户)
  币安 Futures: maker 0.02% (2bps)  taker 0.05% (5bps)

四种策略:
  A: 币安maker + Lighter taker  (全挂单) → 往返 4 bps  最省但币安慢
  B: 币安taker + Lighter taker  (全市价) → 往返 10 bps 最快但最贵
  C: 币安taker开+maker平 + Lighter taker → 往返 7 bps  开仓快
  D: 币安maker开+taker平 + Lighter taker → 往返 7 bps  开仓省平仓快

关键区别:
  - maker 挂单: 无滑点, 但可能不立即成交 (延迟风险)
  - taker 市价: 立即成交, 但有 bid-ask 价差成本
  - Lighter taker: 有深度滑点 (VWAP计算)
"""

import asyncio
import json
import time
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Tuple
from datetime import datetime
from collections import defaultdict

import aiohttp
import websockets

# ─── 配置 ──────────────────────────────────────────────────

LIGHTER_WS = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_REST = "https://mainnet.zklighter.elliot.ai"
BINANCE_FAPI = "https://fapi.binance.com"

TRADE_SIZE_USD = 500.0

# 真实费率
B_MAKER_BPS = 2.0    # 币安 maker 0.02%
B_TAKER_BPS = 5.0    # 币安 taker 0.05%
L_MAKER_BPS = 0.0    # Lighter maker 0%
L_TAKER_BPS = 0.0    # Lighter taker 0%

# 策略定义: (名称, 币安开仓费, 币安平仓费, 开仓币安用bid/ask, 平仓币安用bid/ask)
STRATEGIES = {
    "A_B挂L市": {
        "label": "A: B全挂单+L全市价",
        "b_open_fee": B_MAKER_BPS,
        "b_close_fee": B_MAKER_BPS,
        "b_open_is_maker": True,
        "b_close_is_maker": True,
        "rt_fee": 2 * (B_MAKER_BPS + L_TAKER_BPS),  # 4 bps
    },
    "B_全市价": {
        "label": "B: B全市价+L全市价",
        "b_open_fee": B_TAKER_BPS,
        "b_close_fee": B_TAKER_BPS,
        "b_open_is_maker": False,
        "b_close_is_maker": False,
        "rt_fee": 2 * (B_TAKER_BPS + L_TAKER_BPS),  # 10 bps
    },
    "C_B市开挂平": {
        "label": "C: B市价开+挂单平, L全市价",
        "b_open_fee": B_TAKER_BPS,
        "b_close_fee": B_MAKER_BPS,
        "b_open_is_maker": False,
        "b_close_is_maker": True,
        "rt_fee": B_TAKER_BPS + L_TAKER_BPS + B_MAKER_BPS + L_TAKER_BPS,  # 7 bps
    },
    "D_B挂开市平": {
        "label": "D: B挂单开+市价平, L全市价",
        "b_open_fee": B_MAKER_BPS,
        "b_close_fee": B_TAKER_BPS,
        "b_open_is_maker": True,
        "b_close_is_maker": False,
        "rt_fee": B_MAKER_BPS + L_TAKER_BPS + B_TAKER_BPS + L_TAKER_BPS,  # 7 bps
    },
}

OPEN_THRESHOLD_BPS = 10.0
CLOSE_THRESHOLD_BPS = 2.0
MAX_HOLD_SECONDS = 600
COOLDOWN_SECONDS = 30
BINANCE_POLL_SEC = 1.0
DISPLAY_SEC = 8.0
RUN_MINUTES = 5
REPORT_FILE = "multi_strategy_report.txt"
MAX_SPREAD_BPS = 200.0

EXCLUDED = {
    "EURUSD","GBPUSD","USDJPY","USDCHF","USDCAD","AUDUSD","NZDUSD",
    "USDKRW","XAU","XAG","XCU","XPD","XPT","WTI","URA",
    "NVDA","TSLA","AAPL","AMZN","MSFT","GOOGL","META","HOOD","COIN",
    "PLTR","INTC","AMD","ASML","MSTR","SNDK",
    "SPY","QQQ","IWM","DIA","BOTZ","MAGS",
    "SAMSUNG","HYUNDAI","SKHYNIX","KRCOMP","HANMI","PAXG",
    "1000TOSHI","MEGA",
}
SYMBOL_REMAP = {
    "1000PEPE":"1000PEPE","1000SHIB":"1000SHIB",
    "1000BONK":"1000BONK","1000FLOKI":"1000FLOKI",
}

# ─── 数据结构 ──────────────────────────────────────────────

@dataclass
class PairState:
    symbol: str
    lighter_market_id: int
    binance_symbol: str
    l_bids: Dict[float, float] = field(default_factory=dict)
    l_asks: Dict[float, float] = field(default_factory=dict)
    b_bid: float = 0.0
    b_ask: float = 0.0
    l_mid: float = 0.0
    b_mid: float = 0.0
    mid_spread_bps: float = 0.0
    l_slip_buy_bps: float = 0.0
    l_slip_sell_bps: float = 0.0
    l_buy_levels: int = 0
    l_sell_levels: int = 0
    # 币安 bid-ask spread (taker额外成本)
    b_half_spread_bps: float = 0.0

@dataclass
class SimTrade:
    symbol: str
    strategy: str
    direction: str
    entry_time: float
    entry_mid_spread: float
    entry_l_slip: float
    entry_b_spread_cost: float   # 币安 taker 的 bid-ask 价差成本
    entry_l_levels: int
    close_time: float = 0.0
    close_mid_spread: float = 0.0
    close_l_slip: float = 0.0
    close_b_spread_cost: float = 0.0
    pnl_bps: float = 0.0
    pnl_usd: float = 0.0
    convergence_sec: float = 0.0
    close_reason: str = ""

@dataclass
class StrategyState:
    name: str
    config: dict
    open_pos: Dict[str, SimTrade] = field(default_factory=dict)
    closed: List[SimTrade] = field(default_factory=list)
    cooldowns: Dict[str, float] = field(default_factory=dict)


def ts():
    return datetime.now().strftime("%H:%M:%S")

def log(msg, level="INFO"):
    print(f"  [{ts()}] [{level}] {msg}", flush=True)


def vwap_slip(levels, size_usd):
    if not levels:
        return 0.0, 0, 999.0
    best = levels[0][0]
    filled_usd = 0.0
    total_qty = 0.0
    n = 0
    for price, qty in levels:
        lev_usd = price * qty
        if filled_usd + lev_usd >= size_usd:
            remain = size_usd - filled_usd
            total_qty += remain / price
            filled_usd = size_usd
            n += 1
            break
        else:
            total_qty += qty
            filled_usd += lev_usd
            n += 1
    if total_qty <= 0 or filled_usd < size_usd * 0.5:
        return 0.0, 0, 999.0
    vwap = filled_usd / total_qty
    slip = abs(vwap - best) / best * 10000.0
    return vwap, n, slip


class MultiStrategySim:
    def __init__(self):
        self.pairs: Dict[str, PairState] = {}
        self.mid_to_sym: Dict[int, str] = {}
        self.strategies: Dict[str, StrategyState] = {}
        self.running = True
        self.start_time = time.time()
        self.lighter_ok = False
        self.binance_ok = False
        self.spread_log: Dict[str, List[Tuple[float, float]]] = defaultdict(list)

        for key, cfg in STRATEGIES.items():
            self.strategies[key] = StrategyState(name=key, config=cfg)

    async def run(self):
        log("═══ 币安 vs Lighter 多策略价差套利模拟器 ═══")
        log(f"费率: 币安 maker={B_MAKER_BPS}bps taker={B_TAKER_BPS}bps | "
            f"Lighter maker={L_MAKER_BPS}bps taker={L_TAKER_BPS}bps")
        for key, cfg in STRATEGIES.items():
            log(f"  {cfg['label']}  往返费: {cfg['rt_fee']}bps")
        log(f"单笔 ${TRADE_SIZE_USD:.0f}  开仓>{OPEN_THRESHOLD_BPS}bps  "
            f"平仓<{CLOSE_THRESHOLD_BPS}bps  运行{RUN_MINUTES}分钟")

        async with aiohttp.ClientSession() as session:
            await self._discover(session)
            if not self.pairs:
                log("未找到交易对!", "错误")
                return

            log(f"共 {len(self.pairs)} 个交易对, 启动实时监控...")
            print()

            tasks = [
                asyncio.create_task(self._lighter_ws()),
                asyncio.create_task(self._binance_poll(session)),
                asyncio.create_task(self._strategy()),
                asyncio.create_task(self._display()),
            ]

            await asyncio.sleep(RUN_MINUTES * 60)
            self.running = False
            log("运行结束...")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        self._save_report()

    async def _discover(self, session):
        log("获取市场列表...")
        async with session.get(f"{LIGHTER_REST}/api/v1/orderBooks") as r:
            data = await r.json()
        lighter = {
            b["symbol"]: int(b["market_id"])
            for b in data.get("order_books", [])
            if b.get("status") == "active" and b.get("market_type") == "perp"
            and b["symbol"] not in EXCLUDED
        }
        async with session.get(f"{BINANCE_FAPI}/fapi/v1/exchangeInfo") as r:
            data = await r.json()
        binance = {
            s["baseAsset"] for s in data.get("symbols", [])
            if s.get("status") == "TRADING" and s.get("contractType") == "PERPETUAL"
            and s["quoteAsset"] == "USDT"
        }
        for sym, mid in lighter.items():
            bn = SYMBOL_REMAP.get(sym, sym)
            if bn in binance:
                self.pairs[sym] = PairState(symbol=sym, lighter_market_id=mid,
                                             binance_symbol=f"{bn}USDT")
                self.mid_to_sym[mid] = sym
        log(f"Lighter {len(lighter)} | 币安 {len(binance)} | 共同 {len(self.pairs)}")

    async def _lighter_ws(self):
        while self.running:
            try:
                async with websockets.connect(LIGHTER_WS, close_timeout=5) as ws:
                    for ps in self.pairs.values():
                        await ws.send(json.dumps({
                            "type": "subscribe",
                            "channel": f"order_book/{ps.lighter_market_id}",
                        }))
                    self.lighter_ok = True
                    log(f"Lighter WS 已连接, 订阅 {len(self.pairs)} 市场")
                    while self.running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            continue
                        data = json.loads(raw)
                        mt = data.get("type", "")
                        if mt in ("subscribed/order_book", "update/order_book"):
                            ch = data.get("channel", "")
                            try:
                                mid = int(ch.split(":")[1])
                            except (IndexError, ValueError):
                                continue
                            sym = self.mid_to_sym.get(mid)
                            if not sym:
                                continue
                            ps = self.pairs[sym]
                            ob = data.get("order_book", {})
                            if mt == "subscribed/order_book":
                                ps.l_bids = {}
                                ps.l_asks = {}
                            for b in ob.get("bids", []):
                                p, s = float(b["price"]), float(b["size"])
                                if s == 0: ps.l_bids.pop(p, None)
                                else: ps.l_bids[p] = s
                            for a in ob.get("asks", []):
                                p, s = float(a["price"]), float(a["size"])
                                if s == 0: ps.l_asks.pop(p, None)
                                else: ps.l_asks[p] = s
                            self._recalc(sym)
                        elif mt == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
            except Exception as e:
                self.lighter_ok = False
                if self.running:
                    log(f"Lighter WS 断开: {e}", "警告")
                    await asyncio.sleep(2)

    async def _binance_poll(self, session):
        while self.running:
            try:
                async with session.get(
                    f"{BINANCE_FAPI}/fapi/v1/ticker/bookTicker",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as r:
                    tickers = await r.json()
                tmap = {t["symbol"]: t for t in tickers}
                for sym, ps in self.pairs.items():
                    t = tmap.get(ps.binance_symbol)
                    if t:
                        ps.b_bid = float(t["bidPrice"])
                        ps.b_ask = float(t["askPrice"])
                        self._recalc(sym)
                self.binance_ok = True
            except Exception as e:
                self.binance_ok = False
                if self.running:
                    log(f"币安异常: {e}", "警告")
            await asyncio.sleep(BINANCE_POLL_SEC)

    def _recalc(self, sym):
        ps = self.pairs[sym]
        if not ps.l_bids or not ps.l_asks or ps.b_bid <= 0:
            return
        l_best_bid = max(ps.l_bids.keys())
        l_best_ask = min(ps.l_asks.keys())
        ps.l_mid = (l_best_bid + l_best_ask) / 2.0
        ps.b_mid = (ps.b_bid + ps.b_ask) / 2.0
        ref = (ps.l_mid + ps.b_mid) / 2.0
        if ref > 0:
            ps.mid_spread_bps = (ps.b_mid - ps.l_mid) / ref * 10000.0
            ps.b_half_spread_bps = (ps.b_ask - ps.b_bid) / 2.0 / ps.b_mid * 10000.0

        asks_sorted = sorted(ps.l_asks.items(), key=lambda x: x[0])
        _, n_buy, slip_buy = vwap_slip(asks_sorted, TRADE_SIZE_USD)
        ps.l_slip_buy_bps = slip_buy
        ps.l_buy_levels = n_buy

        bids_sorted = sorted(ps.l_bids.items(), key=lambda x: -x[0])
        _, n_sell, slip_sell = vwap_slip(bids_sorted, TRADE_SIZE_USD)
        ps.l_slip_sell_bps = slip_sell
        ps.l_sell_levels = n_sell

        now = time.time()
        h = self.spread_log[sym]
        h.append((now, ps.mid_spread_bps))
        if len(h) > 5000:
            self.spread_log[sym] = h[-3000:]

    # ─── 策略引擎 ─────────────────────────────────────────

    async def _strategy(self):
        await asyncio.sleep(5)
        log("策略引擎启动, 同时运行4种策略...")

        while self.running:
            now = time.time()
            for sym, ps in self.pairs.items():
                if ps.l_mid <= 0 or ps.b_mid <= 0:
                    continue

                abs_spread = abs(ps.mid_spread_bps)

                for skey, ss in self.strategies.items():
                    cfg = ss.config

                    if sym in ss.open_pos:
                        pos = ss.open_pos[sym]
                        hold = now - pos.entry_time

                        if pos.direction == "L多B空":
                            cur_dir = ps.mid_spread_bps
                        else:
                            cur_dir = -ps.mid_spread_bps

                        should_close = False
                        reason = ""
                        if cur_dir <= CLOSE_THRESHOLD_BPS:
                            should_close = True
                            reason = f"收敛{cur_dir:+.1f}bps"
                        elif hold >= MAX_HOLD_SECONDS:
                            should_close = True
                            reason = f"超时{hold:.0f}s"

                        if should_close:
                            self._close(ss, sym, ps, reason, cur_dir)
                    else:
                        if now < ss.cooldowns.get(sym, 0):
                            continue

                        if abs_spread >= OPEN_THRESHOLD_BPS and abs_spread <= MAX_SPREAD_BPS:
                            if ps.mid_spread_bps > 0:
                                direction = "L多B空"
                                l_slip = ps.l_slip_buy_bps
                                levels = ps.l_buy_levels
                            else:
                                direction = "B多L空"
                                l_slip = ps.l_slip_sell_bps
                                levels = ps.l_sell_levels

                            # 币安 taker 额外成本 = bid-ask半价差
                            b_spread_cost = ps.b_half_spread_bps if not cfg["b_open_is_maker"] else 0.0
                            est_close_b = ps.b_half_spread_bps if not cfg["b_close_is_maker"] else 0.0
                            est_close_l = ps.l_slip_sell_bps if direction == "L多B空" else ps.l_slip_buy_bps

                            total_cost = (cfg["rt_fee"]
                                          + l_slip + est_close_l
                                          + b_spread_cost + est_close_b)
                            net = abs_spread - total_cost

                            if net > 0:
                                self._open(ss, sym, ps, direction, l_slip, b_spread_cost, levels)

            await asyncio.sleep(0.3)

    def _open(self, ss, sym, ps, direction, l_slip, b_spread_cost, levels):
        trade = SimTrade(
            symbol=sym, strategy=ss.name, direction=direction,
            entry_time=time.time(),
            entry_mid_spread=abs(ps.mid_spread_bps),
            entry_l_slip=l_slip,
            entry_b_spread_cost=b_spread_cost,
            entry_l_levels=levels,
        )
        ss.open_pos[sym] = trade

    def _close(self, ss, sym, ps, reason, cur_dir):
        pos = ss.open_pos.pop(sym)
        cfg = ss.config
        now = time.time()

        if pos.direction == "L多B空":
            close_l_slip = ps.l_slip_sell_bps
        else:
            close_l_slip = ps.l_slip_buy_bps
        close_b_cost = ps.b_half_spread_bps if not cfg["b_close_is_maker"] else 0.0

        pos.close_time = now
        pos.close_mid_spread = max(0, cur_dir)
        pos.close_l_slip = close_l_slip
        pos.close_b_spread_cost = close_b_cost
        pos.convergence_sec = now - pos.entry_time
        pos.close_reason = reason

        pos.pnl_bps = (pos.entry_mid_spread
                        - pos.close_mid_spread
                        - pos.entry_l_slip - pos.close_l_slip
                        - pos.entry_b_spread_cost - pos.close_b_spread_cost
                        - cfg["rt_fee"])
        pos.pnl_usd = TRADE_SIZE_USD * pos.pnl_bps / 10000.0
        ss.closed.append(pos)
        ss.cooldowns[sym] = now + COOLDOWN_SECONDS

    # ─── 显示 ─────────────────────────────────────────────

    async def _display(self):
        await asyncio.sleep(8)
        while self.running:
            self._show()
            await asyncio.sleep(DISPLAY_SEC)

    def _show(self):
        now = time.time()
        elapsed = now - self.start_time
        remain = max(0, RUN_MINUTES * 60 - elapsed)
        W = 120

        print(f"\n  {'═' * W}")
        print(f"  [{ts()}] 运行{elapsed/60:.1f}分 剩余{remain/60:.1f}分  "
              f"L:{'✓' if self.lighter_ok else '✗'}  B:{'✓' if self.binance_ok else '✗'}")

        # 策略对比
        print(f"\n  ─── 4种策略实时对比 ───")
        print(f"  {'策略':<28} {'费用':>4} {'交易':>4} {'胜率':>6} "
              f"{'累计bps':>8} {'累计$':>7} {'持仓':>4} {'均收敛':>6} {'均盈亏':>7}")
        print(f"  {'─' * 85}")
        for skey in ["A_B挂L市", "B_全市价", "C_B市开挂平", "D_B挂开市平"]:
            ss = self.strategies[skey]
            cfg = ss.config
            n = len(ss.closed)
            w = sum(1 for t in ss.closed if t.pnl_bps > 0)
            tp = sum(t.pnl_bps for t in ss.closed)
            tu = sum(t.pnl_usd for t in ss.closed)
            avg = tp / n if n else 0
            ac = sum(t.convergence_sec for t in ss.closed) / n if n else 0
            wr = f"{w}/{n}" if n else "0/0"
            print(f"  {cfg['label']:<28} {cfg['rt_fee']:>4.0f} {n:>4} {wr:>6} "
                  f"{tp:>+8.1f} ${tu:>+6.2f} {len(ss.open_pos):>4} "
                  f"{ac:>5.1f}s {avg:>+7.2f}")

        # 当前最大价差
        active = [(s, p) for s, p in self.pairs.items()
                   if p.l_mid > 0 and p.b_mid > 0]
        active.sort(key=lambda x: abs(x[1].mid_spread_bps), reverse=True)

        print(f"\n  ─── 价差排行 TOP 15 ───")
        print(f"  {'币种':<11} {'价差':>6} {'L买滑':>5} {'L卖滑':>5} {'B半差':>5} "
              f"{'A净利':>6} {'B净利':>6} {'C净利':>6} {'D净利':>6}")
        for sym, ps in active[:15]:
            ab = abs(ps.mid_spread_bps)
            ls_buy = ps.l_slip_buy_bps
            ls_sell = ps.l_slip_sell_bps
            l_slip_tot = ls_buy + ls_sell
            bhs = ps.b_half_spread_bps

            netA = ab - l_slip_tot - 0 - 0 - 4
            netB = ab - l_slip_tot - bhs - bhs - 10
            netC = ab - l_slip_tot - bhs - 0 - 7
            netD = ab - l_slip_tot - 0 - bhs - 7

            print(f"  {sym:<11} {ps.mid_spread_bps:>+6.1f} {ls_buy:>5.1f} {ls_sell:>5.1f} "
                  f"{bhs:>5.1f} {netA:>+6.1f} {netB:>+6.1f} {netC:>+6.1f} {netD:>+6.1f}")

        # 最近交易 (只显示策略A的)
        ss_a = self.strategies["A_B挂L市"]
        if ss_a.closed:
            recent = ss_a.closed[-3:]
            print(f"\n  ─── 策略A最近交易 ───")
            for t in reversed(recent):
                m = "✓" if t.pnl_bps > 0 else "✗"
                print(f"  {m} {t.symbol:<10} {t.direction} "
                      f"入{t.entry_mid_spread:+.1f}→出{t.close_mid_spread:+.1f} "
                      f"L滑{t.entry_l_slip:.1f}+{t.close_l_slip:.1f} "
                      f"{t.pnl_bps:>+.2f}bps {t.convergence_sec:.1f}s")

        print(f"  {'═' * W}")
        sys.stdout.flush()

    # ─── 报告 ─────────────────────────────────────────────

    def _save_report(self):
        L = []
        W = 100
        elapsed = time.time() - self.start_time

        L.append(f"\n{'=' * W}")
        L.append(f"  币安 vs Lighter 多策略价差套利 — 最终报告")
        L.append(f"  运行: {elapsed/60:.1f} 分钟  |  单笔: ${TRADE_SIZE_USD:.0f}")
        L.append(f"  费率: 币安 maker={B_MAKER_BPS}bps taker={B_TAKER_BPS}bps | "
                 f"Lighter maker={L_MAKER_BPS}bps taker={L_TAKER_BPS}bps")
        L.append(f"  开仓: |价差|>{OPEN_THRESHOLD_BPS}bps  平仓: <{CLOSE_THRESHOLD_BPS}bps")
        L.append(f"{'=' * W}")

        # 策略对比总表
        L.append(f"\n  ╔══════════════════════════════════════════════════════════════════════════╗")
        L.append(f"  ║                     四种策略对比总结                                    ║")
        L.append(f"  ╠══════════════════════════════════════════════════════════════════════════╣")
        L.append(f"  ║ {'策略':<25} {'费用':>4} {'笔数':>4} {'胜率':>7} "
                 f"{'累计bps':>8} {'累计$':>7} {'均盈亏':>7} {'均收敛':>6} ║")
        L.append(f"  ╠{'═' * 74}╣")

        best_pnl = -9999
        best_key = ""
        for skey in ["A_B挂L市", "B_全市价", "C_B市开挂平", "D_B挂开市平"]:
            ss = self.strategies[skey]
            cfg = ss.config
            n = len(ss.closed)
            w = sum(1 for t in ss.closed if t.pnl_bps > 0)
            tp = sum(t.pnl_bps for t in ss.closed)
            tu = sum(t.pnl_usd for t in ss.closed)
            avg = tp / n if n else 0
            ac = sum(t.convergence_sec for t in ss.closed) / n if n else 0
            wr = f"{w}/{n}" if n else "-"
            wrp = f"({w/n*100:.0f}%)" if n else ""
            L.append(f"  ║ {cfg['label']:<25} {cfg['rt_fee']:>4.0f} {n:>4} {wr:>4}{wrp:>4} "
                     f"{tp:>+8.1f} ${tu:>+6.2f} {avg:>+7.2f} {ac:>5.1f}s ║")
            if tp > best_pnl:
                best_pnl = tp
                best_key = skey

        L.append(f"  ╚══════════════════════════════════════════════════════════════════════════╝")
        L.append(f"\n  >>> 最优策略: {STRATEGIES[best_key]['label']} <<<")

        # 每种策略详细
        for skey in ["A_B挂L市", "B_全市价", "C_B市开挂平", "D_B挂开市平"]:
            ss = self.strategies[skey]
            cfg = ss.config
            n = len(ss.closed)
            if n == 0:
                continue

            wins = [t for t in ss.closed if t.pnl_bps > 0]
            losses = [t for t in ss.closed if t.pnl_bps <= 0]
            tp = sum(t.pnl_bps for t in ss.closed)
            tu = sum(t.pnl_usd for t in ss.closed)
            avg = tp / n
            ac = sum(t.convergence_sec for t in ss.closed) / n
            avg_entry = sum(t.entry_mid_spread for t in ss.closed) / n
            avg_l_slip = sum(t.entry_l_slip + t.close_l_slip for t in ss.closed) / n
            avg_b_cost = sum(t.entry_b_spread_cost + t.close_b_spread_cost for t in ss.closed) / n

            L.append(f"\n  ─── {cfg['label']} (往返{cfg['rt_fee']}bps) ───")
            L.append(f"  交易: {n}笔  胜率: {len(wins)}/{n} ({len(wins)/n*100:.1f}%)")
            L.append(f"  累计: {tp:>+.2f}bps (${tu:>+.2f})")
            L.append(f"  每笔: {avg:>+.2f}bps  收敛: {ac:.1f}s")
            L.append(f"  均入场差: {avg_entry:.1f}bps  均L滑点: {avg_l_slip:.1f}bps  均B价差成本: {avg_b_cost:.1f}bps")

            if wins:
                aw = sum(t.pnl_bps for t in wins) / len(wins)
                L.append(f"  盈利笔均: {aw:>+.2f}bps")
            if losses:
                al = sum(t.pnl_bps for t in losses) / len(losses)
                L.append(f"  亏损笔均: {al:>+.2f}bps")

            # 按币种
            by_sym: Dict[str, List[SimTrade]] = defaultdict(list)
            for t in ss.closed:
                by_sym[t.symbol].append(t)
            L.append(f"  按币种:")
            for sym, trades in sorted(by_sym.items(),
                                        key=lambda x: sum(t.pnl_bps for t in x[1]),
                                        reverse=True)[:10]:
                cn = len(trades)
                ct = sum(t.pnl_bps for t in trades)
                cu = sum(t.pnl_usd for t in trades)
                cc = sum(t.convergence_sec for t in trades) / cn
                L.append(f"    {sym:<11} {cn}笔 {ct:>+7.1f}bps ${cu:>+5.2f} 均{cc:.1f}s")

        # 哪些币适合
        L.append(f"\n  ─── 最适合套利的代币 (基于策略A) ───")
        ss_a = self.strategies["A_B挂L市"]
        by_sym_a: Dict[str, List[SimTrade]] = defaultdict(list)
        for t in ss_a.closed:
            by_sym_a[t.symbol].append(t)
        suitable = []
        for sym, trades in by_sym_a.items():
            n = len(trades)
            tp = sum(t.pnl_bps for t in trades)
            wr = sum(1 for t in trades if t.pnl_bps > 0) / n * 100 if n else 0
            ac = sum(t.convergence_sec for t in trades) / n
            ae = sum(t.entry_mid_spread for t in trades) / n
            suitable.append((sym, n, tp, wr, ac, ae))
        suitable.sort(key=lambda x: x[2], reverse=True)

        L.append(f"  {'币种':<11} {'次':>3} {'累计bps':>8} {'胜率':>5} {'均收敛':>6} {'均入场':>6} {'推荐'}")
        L.append(f"  {'─' * 55}")
        for sym, n, tp, wr, ac, ae in suitable:
            rec = "★★★" if tp > 10 and wr > 80 else ("★★" if tp > 5 else ("★" if tp > 0 else ""))
            L.append(f"  {sym:<11} {n:>3} {tp:>+8.1f} {wr:>4.0f}% {ac:>5.1f}s {ae:>+6.1f} {rec}")

        # 价差波动
        L.append(f"\n  ─── 价差波动Top15 ───")
        L.append(f"  {'币种':<11} {'最小':>7} {'最大':>7} {'均值':>7} {'标准差':>7} {'样本':>5}")
        stats = []
        for sym, hist in self.spread_log.items():
            if len(hist) < 20:
                continue
            vals = [v for _, v in hist]
            mean = sum(vals) / len(vals)
            std = (sum((v - mean)**2 for v in vals) / len(vals)) ** 0.5
            stats.append((sym, min(vals), max(vals), mean, std, len(vals)))
        stats.sort(key=lambda x: x[4], reverse=True)
        for sym, mn, mx, av, sd, cn in stats[:15]:
            L.append(f"  {sym:<11} {mn:>+7.1f} {mx:>+7.1f} {av:>+7.1f} {sd:>7.1f} {cn:>5}")

        L.append(f"\n  ─── 结论 ───")
        L.append(f"  1. 最优策略: {STRATEGIES[best_key]['label']}")
        L.append(f"  2. Lighter 0费率 → 在Lighter永远用市价(taker)最划算")
        L.append(f"  3. 币安费率差距: maker(2bps) vs taker(5bps), 差3bps/腿")
        L.append(f"  4. 全挂单(A)省费用但有延迟风险; 全市价(B)快但贵")
        L.append(f"  5. 适合做套利的代币特征: 价差频繁>10bps, 收敛快(<30s), L深度好(滑点<2bps)")
        L.append(f"\n{'=' * W}\n")

        report = "\n".join(L)
        print(report, flush=True)
        try:
            with open(REPORT_FILE, "w", encoding="utf-8") as f:
                f.write(report)
            log(f"✓ 报告已保存 → {REPORT_FILE}")
        except Exception as e:
            log(f"保存失败: {e}", "错误")


async def main():
    sim = MultiStrategySim()
    await sim.run()

if __name__ == "__main__":
    print()
    print("  ╔════════════════════════════════════════════════════════╗")
    print("  ║  币安 vs Lighter — 多策略价差套利模拟器              ║")
    print("  ║  4种开平仓策略同步对比 + 实时数据 + 收敛跟踪        ║")
    print("  ╚════════════════════════════════════════════════════════╝")
    print()
    asyncio.run(main())
