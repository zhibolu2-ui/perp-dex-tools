#!/usr/bin/env python3
"""
币安 vs Lighter — 价差套利模拟器
=================================
策略: 价差收敛套利 (Convergence Arbitrage)
  - 币安挂单(maker): 无滑点, 买挂bid/卖挂ask
  - Lighter吃单(taker): 用VWAP计算$500深度执行价

PnL计算:
  开仓时锁定一个 cross-exchange 价差
  平仓时价差收敛, 利润 = 入场价差 - 出场价差 - 交易成本
  交易成本 = 入场Lighter滑点 + 出场Lighter滑点 + 4×币安maker费
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
BINANCE_MAKER_BPS = 2.0
LIGHTER_TAKER_BPS = 0.0
# 每次开仓/平仓 = 2腿(B+L), 开+平 = 4腿
# 每腿币安maker费 = 2bps, Lighter taker费 = 0bps
# 固定费用 = 4 × 2bps = 8bps... 不对, 应该是:
# 开仓: 1腿B + 1腿L = maker_fee + taker_fee = 2 + 0 = 2bps
# 平仓: 1腿B + 1腿L = maker_fee + taker_fee = 2 + 0 = 2bps
# 往返固定费用 = 4 bps
FIXED_FEE_BPS = 4.0

OPEN_THRESHOLD_BPS = 10.0    # 中间价差 > 10 bps 开仓 (需覆盖费用+滑点)
CLOSE_THRESHOLD_BPS = 2.0    # 中间价差 < 2 bps 平仓
MAX_HOLD_SECONDS = 600
COOLDOWN_SECONDS = 30

BINANCE_POLL_SEC = 1.0
DISPLAY_SEC = 5.0
RUN_MINUTES = 5
REPORT_FILE = "convergence_report.txt"
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
    # Lighter 盘口 (完整)
    l_bids: Dict[float, float] = field(default_factory=dict)
    l_asks: Dict[float, float] = field(default_factory=dict)
    # 币安 best bid/ask
    b_bid: float = 0.0
    b_ask: float = 0.0
    # 衍生
    l_mid: float = 0.0
    b_mid: float = 0.0
    # 中间价价差 (B_mid - L_mid) / L_mid * 10000
    mid_spread_bps: float = 0.0
    # Lighter VWAP滑点 (单边, 买/卖取较大值)
    l_slip_buy_bps: float = 0.0
    l_slip_sell_bps: float = 0.0
    l_buy_levels: int = 0
    l_sell_levels: int = 0
    cooldown_until: float = 0.0

@dataclass
class SimPosition:
    symbol: str
    direction: str              # "L多B空" (L mid < B mid) or "B多L空" (B mid < L mid)
    entry_time: float
    entry_l_mid: float
    entry_b_mid: float
    entry_mid_spread: float     # |B_mid - L_mid| / ref * 10000, 入场时的中间价价差
    entry_l_slip: float         # 入场Lighter滑点bps
    entry_l_levels: int
    # 平仓
    close_time: float = 0.0
    close_l_mid: float = 0.0
    close_b_mid: float = 0.0
    close_mid_spread: float = 0.0
    close_l_slip: float = 0.0   # 平仓Lighter滑点bps
    pnl_bps: float = 0.0
    pnl_usd: float = 0.0
    convergence_sec: float = 0.0
    close_reason: str = ""


def ts():
    return datetime.now().strftime("%H:%M:%S")

def log(msg, level="INFO"):
    print(f"  [{ts()}] [{level}] {msg}", flush=True)

def log_trade(msg):
    print(f"  [{ts()}] [交易] ★ {msg}", flush=True)


# ─── VWAP 滑点 ────────────────────────────────────────────

def vwap_slip(levels: List[Tuple[float, float]], size_usd: float):
    """
    给定排序好的 [(price, qty), ...], 计算吃 size_usd 的 VWAP 和滑点.
    levels 按吃单顺序排列 (asks升序 or bids降序).
    返回 (vwap, levels_eaten, slip_bps).
    """
    if not levels:
        return 0.0, 0, 0.0

    best = levels[0][0]
    filled_usd = 0.0
    total_qty = 0.0
    n = 0

    for price, qty in levels:
        lev_usd = price * qty
        if filled_usd + lev_usd >= size_usd:
            remain_usd = size_usd - filled_usd
            total_qty += remain_usd / price
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


# ─── 主类 ──────────────────────────────────────────────────

class ArbitrageSim:
    def __init__(self):
        self.pairs: Dict[str, PairState] = {}
        self.mid_to_sym: Dict[int, str] = {}
        self.open_pos: Dict[str, SimPosition] = {}
        self.closed_pos: List[SimPosition] = []
        self.running = True
        self.start_time = time.time()
        self.lighter_ok = False
        self.binance_ok = False
        self.spread_log: Dict[str, List[Tuple[float, float]]] = defaultdict(list)

    async def run(self):
        log("═══ 币安 vs Lighter 价差套利模拟器 ═══")
        log(f"策略: 币安挂单(maker) + Lighter吃单(taker)")
        log(f"单笔 ${TRADE_SIZE_USD:.0f}  开仓>|{OPEN_THRESHOLD_BPS}|bps  "
            f"平仓<|{CLOSE_THRESHOLD_BPS}|bps  固定费用={FIXED_FEE_BPS}bps/往返")
        log(f"运行 {RUN_MINUTES} 分钟后自动输出报告")

        async with aiohttp.ClientSession() as session:
            await self._discover(session)
            if not self.pairs:
                log("未找到共同交易对!", "错误")
                return

            log(f"共 {len(self.pairs)} 个交易对, 启动监控...")
            print()

            tasks = [
                asyncio.create_task(self._lighter_ws()),
                asyncio.create_task(self._binance_poll(session)),
                asyncio.create_task(self._strategy()),
                asyncio.create_task(self._display()),
            ]

            await asyncio.sleep(RUN_MINUTES * 60)
            self.running = False
            log("运行结束, 正在关闭连接...")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        self._save_report()

    # ─── 发现交易对 ────────────────────────────────────────

    async def _discover(self, session):
        log("获取两交易所市场列表...")
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
            s["baseAsset"]
            for s in data.get("symbols", [])
            if s.get("status") == "TRADING" and s.get("contractType") == "PERPETUAL"
            and s["quoteAsset"] == "USDT"
        }
        for sym, mid in lighter.items():
            bn = SYMBOL_REMAP.get(sym, sym)
            if bn in binance:
                self.pairs[sym] = PairState(symbol=sym, lighter_market_id=mid,
                                             binance_symbol=f"{bn}USDT")
                self.mid_to_sym[mid] = sym
        log(f"Lighter {len(lighter)}个  币安 {len(binance)}个  共同 {len(self.pairs)}个")

    # ─── Lighter WebSocket ─────────────────────────────────

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
                    log(f"Lighter WS 已连接, 订阅 {len(self.pairs)} 个市场")

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
                                if s == 0:
                                    ps.l_bids.pop(p, None)
                                else:
                                    ps.l_bids[p] = s
                            for a in ob.get("asks", []):
                                p, s = float(a["price"]), float(a["size"])
                                if s == 0:
                                    ps.l_asks.pop(p, None)
                                else:
                                    ps.l_asks[p] = s
                            self._recalc(sym)
                        elif mt == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
            except Exception as e:
                self.lighter_ok = False
                if self.running:
                    log(f"Lighter WS 断开: {e}", "警告")
                    await asyncio.sleep(2)

    # ─── 币安 REST ─────────────────────────────────────────

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
                    log(f"币安轮询异常: {e}", "警告")
            await asyncio.sleep(BINANCE_POLL_SEC)

    # ─── 计算中间价价差 & Lighter滑点 ────────────────────

    def _recalc(self, sym: str):
        ps = self.pairs[sym]
        if not ps.l_bids or not ps.l_asks or ps.b_bid <= 0:
            return

        l_best_bid = max(ps.l_bids.keys())
        l_best_ask = min(ps.l_asks.keys())
        ps.l_mid = (l_best_bid + l_best_ask) / 2.0
        ps.b_mid = (ps.b_bid + ps.b_ask) / 2.0

        # 中间价价差 (正=币安贵, 负=Lighter贵)
        ref = (ps.l_mid + ps.b_mid) / 2.0
        if ref > 0:
            ps.mid_spread_bps = (ps.b_mid - ps.l_mid) / ref * 10000.0

        # Lighter VWAP滑点
        asks_sorted = sorted(ps.l_asks.items(), key=lambda x: x[0])
        _, n_buy, slip_buy = vwap_slip(asks_sorted, TRADE_SIZE_USD)
        ps.l_slip_buy_bps = slip_buy
        ps.l_buy_levels = n_buy

        bids_sorted = sorted(ps.l_bids.items(), key=lambda x: -x[0])
        _, n_sell, slip_sell = vwap_slip(bids_sorted, TRADE_SIZE_USD)
        ps.l_slip_sell_bps = slip_sell
        ps.l_sell_levels = n_sell

        # 历史
        now = time.time()
        h = self.spread_log[sym]
        h.append((now, ps.mid_spread_bps))
        if len(h) > 5000:
            self.spread_log[sym] = h[-3000:]

    # ─── 策略 ─────────────────────────────────────────────

    async def _strategy(self):
        await asyncio.sleep(5)
        log("策略引擎启动, 开始扫描价差...")

        while self.running:
            now = time.time()
            for sym, ps in self.pairs.items():
                if ps.l_mid <= 0 or ps.b_mid <= 0:
                    continue

                abs_spread = abs(ps.mid_spread_bps)

                if sym in self.open_pos:
                    pos = self.open_pos[sym]
                    hold = now - pos.entry_time

                    # 当前同方向的中间价差
                    if pos.direction == "L多B空":
                        # 入场时 B_mid > L_mid, 正向价差
                        current_dir_spread = ps.mid_spread_bps
                    else:
                        # 入场时 L_mid > B_mid, 反向价差
                        current_dir_spread = -ps.mid_spread_bps

                    should_close = False
                    reason = ""
                    if current_dir_spread <= CLOSE_THRESHOLD_BPS:
                        should_close = True
                        reason = f"价差收敛→{current_dir_spread:+.1f}bps"
                    elif hold >= MAX_HOLD_SECONDS:
                        should_close = True
                        reason = f"超时{hold:.0f}s"

                    if should_close:
                        self._close(sym, ps, reason, current_dir_spread)

                else:
                    if now < ps.cooldown_until:
                        continue

                    # 开仓条件: 中间价差够大, 扣除预估滑点+费用后仍有利润
                    if abs_spread >= OPEN_THRESHOLD_BPS and abs_spread <= MAX_SPREAD_BPS:
                        if ps.mid_spread_bps > 0:
                            # B贵L便宜 → L做多 B做空
                            entry_slip = ps.l_slip_buy_bps  # 在L市价买入
                            est_close_slip = ps.l_slip_sell_bps
                            direction = "L多B空"
                            levels = ps.l_buy_levels
                        else:
                            # L贵B便宜 → B做多 L做空
                            entry_slip = ps.l_slip_sell_bps  # 在L市价卖出
                            est_close_slip = ps.l_slip_buy_bps
                            direction = "B多L空"
                            levels = ps.l_sell_levels

                        total_cost = entry_slip + est_close_slip + FIXED_FEE_BPS
                        net = abs_spread - total_cost
                        if net > 0:
                            self._open(sym, ps, direction, entry_slip, levels, total_cost, net)

            await asyncio.sleep(0.3)

    def _open(self, sym, ps, direction, entry_slip, levels, total_cost, net):
        pos = SimPosition(
            symbol=sym,
            direction=direction,
            entry_time=time.time(),
            entry_l_mid=ps.l_mid,
            entry_b_mid=ps.b_mid,
            entry_mid_spread=abs(ps.mid_spread_bps),
            entry_l_slip=entry_slip,
            entry_l_levels=levels,
        )
        self.open_pos[sym] = pos
        log_trade(
            f"开仓 {sym:<11} {direction}  "
            f"中间价差:{ps.mid_spread_bps:>+.1f}bps  "
            f"L滑点:{entry_slip:.1f}bps({levels}档)  "
            f"总成本:{total_cost:.1f}bps  预估净利:{net:+.1f}bps  "
            f"L={ps.l_mid:.6f} B={ps.b_mid:.6f}"
        )

    def _close(self, sym, ps, reason, current_dir_spread):
        pos = self.open_pos.pop(sym)
        now = time.time()

        # 平仓时的Lighter滑点 (方向相反)
        if pos.direction == "L多B空":
            close_slip = ps.l_slip_sell_bps  # 平仓: L卖出
        else:
            close_slip = ps.l_slip_buy_bps   # 平仓: L买入

        pos.close_time = now
        pos.close_l_mid = ps.l_mid
        pos.close_b_mid = ps.b_mid
        pos.close_mid_spread = max(0, current_dir_spread)
        pos.close_l_slip = close_slip
        pos.convergence_sec = now - pos.entry_time
        pos.close_reason = reason

        # PnL = 入场价差 - 出场价差 - 入场滑点 - 出场滑点 - 固定费用
        pos.pnl_bps = (pos.entry_mid_spread
                       - pos.close_mid_spread
                       - pos.entry_l_slip
                       - close_slip
                       - FIXED_FEE_BPS)
        pos.pnl_usd = TRADE_SIZE_USD * pos.pnl_bps / 10000.0

        self.closed_pos.append(pos)
        ps.cooldown_until = now + COOLDOWN_SECONDS

        marker = "盈" if pos.pnl_bps > 0 else "亏"
        log_trade(
            f"平仓 {sym:<11} {reason}  "
            f"入场差:{pos.entry_mid_spread:+.1f}→出场差:{pos.close_mid_spread:+.1f}  "
            f"收敛:{pos.entry_mid_spread - pos.close_mid_spread:.1f}bps  "
            f"滑点:{pos.entry_l_slip:.1f}+{close_slip:.1f}  "
            f"耗时:{pos.convergence_sec:.1f}s  "
            f"{marker}:{pos.pnl_bps:>+.2f}bps(${pos.pnl_usd:>+.2f})"
        )

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

        active = [(s, p) for s, p in self.pairs.items()
                   if p.l_mid > 0 and p.b_mid > 0]
        active.sort(key=lambda x: abs(x[1].mid_spread_bps), reverse=True)

        tp = sum(p.pnl_bps for p in self.closed_pos)
        tu = sum(p.pnl_usd for p in self.closed_pos)
        w = sum(1 for p in self.closed_pos if p.pnl_bps > 0)
        l = sum(1 for p in self.closed_pos if p.pnl_bps <= 0)
        ac = sum(p.convergence_sec for p in self.closed_pos) / len(self.closed_pos) if self.closed_pos else 0

        W = 115
        print(f"\n  {'═' * W}")
        print(f"  [{ts()}] 运行{elapsed/60:.1f}分 剩余{remain/60:.1f}分  "
              f"L:{'✓' if self.lighter_ok else '✗'}  B:{'✓' if self.binance_ok else '✗'}  "
              f"活跃:{len(active)}  持仓:{len(self.open_pos)}  "
              f"已平:{len(self.closed_pos)} (胜{w}/负{l})  "
              f"累计:{tp:>+.1f}bps(${tu:>+.2f})  均收敛:{ac:.1f}s")

        if self.open_pos:
            print(f"\n  [持仓中]")
            print(f"  {'币种':<11} {'方向':<7} {'入场差':>6} {'当前差':>6} "
                  f"{'浮盈':>6} {'L入滑':>5} {'档':>2} {'持仓':>5}")
            for sym, pos in sorted(self.open_pos.items(),
                                     key=lambda x: x[1].entry_mid_spread, reverse=True):
                ps = self.pairs[sym]
                hold = now - pos.entry_time
                if pos.direction == "L多B空":
                    cur = ps.mid_spread_bps
                else:
                    cur = -ps.mid_spread_bps
                close_slip_est = ps.l_slip_sell_bps if pos.direction == "L多B空" else ps.l_slip_buy_bps
                f_pnl = pos.entry_mid_spread - max(0, cur) - pos.entry_l_slip - close_slip_est - FIXED_FEE_BPS
                print(f"  {sym:<11} {pos.direction:<7} {pos.entry_mid_spread:>+6.1f} "
                      f"{cur:>+6.1f} {f_pnl:>+6.1f} {pos.entry_l_slip:>5.1f} "
                      f"{pos.entry_l_levels:>2} {hold:>4.0f}s")

        print(f"\n  [价差排行 TOP 20] (中间价差, 含$500 Lighter滑点预估)")
        print(f"  {'币种':<11} {'中间价差':>7} {'L买滑':>5} {'L卖滑':>5} "
              f"{'预估净利':>7} {'L_mid':>11} {'B_mid':>11} {'状态':<7}")
        for sym, ps in active[:20]:
            abs_s = abs(ps.mid_spread_bps)
            if ps.mid_spread_bps > 0:
                cost = ps.l_slip_buy_bps + ps.l_slip_sell_bps + FIXED_FEE_BPS
            else:
                cost = ps.l_slip_sell_bps + ps.l_slip_buy_bps + FIXED_FEE_BPS
            net = abs_s - cost
            st = ""
            if sym in self.open_pos:
                st = "● 持仓"
            elif now < ps.cooldown_until:
                st = "○ 冷却"
            elif abs_s >= OPEN_THRESHOLD_BPS and net > 0:
                st = "★ 可开"
            print(f"  {sym:<11} {ps.mid_spread_bps:>+7.1f} {ps.l_slip_buy_bps:>5.1f} "
                  f"{ps.l_slip_sell_bps:>5.1f} {net:>+7.1f} "
                  f"{ps.l_mid:>11.6f} {ps.b_mid:>11.6f} {st}")

        if self.closed_pos:
            recent = self.closed_pos[-5:]
            print(f"\n  [最近平仓]")
            print(f"  {'币种':<11} {'方向':<7} {'入场':>6} {'出场':>6} "
                  f"{'盈亏':>7} {'$':>6} {'滑点':>8} {'收敛':>5} {'原因'}")
            for p in reversed(recent):
                m = "✓" if p.pnl_bps > 0 else "✗"
                print(f"  {m}{p.symbol:<10} {p.direction:<7} "
                      f"{p.entry_mid_spread:>+6.1f} {p.close_mid_spread:>+6.1f} "
                      f"{p.pnl_bps:>+7.2f} ${p.pnl_usd:>+5.2f} "
                      f"{p.entry_l_slip:.1f}+{p.close_l_slip:.1f} "
                      f"{p.convergence_sec:>4.1f}s {p.close_reason}")

        print(f"  {'═' * W}")
        sys.stdout.flush()

    # ─── 保存报告 ─────────────────────────────────────────

    def _save_report(self):
        L = []
        W = 100
        elapsed = time.time() - self.start_time

        L.append(f"\n{'=' * W}")
        L.append(f"  币安 vs Lighter 价差套利模拟 — 最终报告")
        L.append(f"  运行: {elapsed/60:.1f} 分钟  |  单笔: ${TRADE_SIZE_USD:.0f}")
        L.append(f"  策略: 币安maker(买bid/卖ask) + Lighter taker(VWAP市价)")
        L.append(f"  开仓: |中间价差|>{OPEN_THRESHOLD_BPS}bps  平仓: <{CLOSE_THRESHOLD_BPS}bps")
        L.append(f"  固定费: {FIXED_FEE_BPS}bps/往返 (B_maker {BINANCE_MAKER_BPS}×2 + L_taker {LIGHTER_TAKER_BPS}×2)")
        L.append(f"{'=' * W}")

        if not self.closed_pos:
            L.append(f"\n  没有完成任何交易。")
            if self.open_pos:
                L.append(f"\n  未平仓 ({len(self.open_pos)} 个):")
                for sym, pos in self.open_pos.items():
                    ps = self.pairs[sym]
                    h = time.time() - pos.entry_time
                    L.append(f"    {sym:<12} {pos.direction}  入场{pos.entry_mid_spread:+.1f}bps  "
                             f"当前{ps.mid_spread_bps:+.1f}bps  持{h:.0f}s")
        else:
            n = len(self.closed_pos)
            wins = [p for p in self.closed_pos if p.pnl_bps > 0]
            losses = [p for p in self.closed_pos if p.pnl_bps <= 0]
            tp = sum(p.pnl_bps for p in self.closed_pos)
            tu = sum(p.pnl_usd for p in self.closed_pos)
            avg = tp / n
            avg_c = sum(p.convergence_sec for p in self.closed_pos) / n
            conv_count = sum(1 for p in self.closed_pos if "收敛" in p.close_reason)
            tout_count = sum(1 for p in self.closed_pos if "超时" in p.close_reason)
            avg_entry = sum(p.entry_mid_spread for p in self.closed_pos) / n
            avg_slip = sum(p.entry_l_slip + p.close_l_slip for p in self.closed_pos) / n

            L.append(f"\n  ─── 交易统计 ───")
            L.append(f"  总交易:       {n}")
            L.append(f"  胜率:         {len(wins)}/{n} ({len(wins)/n*100:.1f}%)")
            L.append(f"  累计盈亏:     {tp:>+.2f} bps (${tu:>+.2f})")
            L.append(f"  每笔均值:     {avg:>+.2f} bps")
            L.append(f"  平均入场差:   {avg_entry:.1f} bps")
            L.append(f"  平均总滑点:   {avg_slip:.1f} bps (入+出)")
            L.append(f"  平均收敛时间: {avg_c:.1f} 秒")
            L.append(f"  价差收敛平仓: {conv_count} 次")
            L.append(f"  超时强平:     {tout_count} 次")

            if wins:
                aw = sum(p.pnl_bps for p in wins) / len(wins)
                awc = sum(p.convergence_sec for p in wins) / len(wins)
                L.append(f"  盈利笔均:     {aw:>+.2f} bps (收敛{awc:.1f}s)")
            if losses:
                al = sum(p.pnl_bps for p in losses) / len(losses)
                alc = sum(p.convergence_sec for p in losses) / len(losses)
                L.append(f"  亏损笔均:     {al:>+.2f} bps (持仓{alc:.1f}s)")

            # 按币种
            ss: Dict[str, List[SimPosition]] = defaultdict(list)
            for p in self.closed_pos:
                ss[p.symbol].append(p)

            L.append(f"\n  ─── 按币种统计 ───")
            L.append(f"  {'币种':<12} {'次':>3} {'胜':>3} {'累计bps':>8} "
                     f"{'累计$':>7} {'均收敛':>6} {'均入场差':>7} {'均总滑':>6}")
            L.append(f"  {'─' * 62}")
            for sym, trades in sorted(ss.items(),
                                        key=lambda x: sum(t.pnl_bps for t in x[1]),
                                        reverse=True):
                cn = len(trades)
                cw = sum(1 for t in trades if t.pnl_bps > 0)
                ct = sum(t.pnl_bps for t in trades)
                cu = sum(t.pnl_usd for t in trades)
                cc = sum(t.convergence_sec for t in trades) / cn
                ce = sum(t.entry_mid_spread for t in trades) / cn
                cs = sum(t.entry_l_slip + t.close_l_slip for t in trades) / cn
                L.append(f"  {sym:<12} {cn:>3} {cw:>3} {ct:>+8.2f} "
                         f"${cu:>+6.2f} {cc:>5.1f}s {ce:>+7.1f} {cs:>6.1f}")

            # 明细
            L.append(f"\n  ─── 全部交易明细 ───")
            L.append(f"  {'#':>3} {'时间':>8} {'币种':<11} {'方向':<7} "
                     f"{'入场差':>6} {'出场差':>6} {'收敛':>5} "
                     f"{'滑点':>8} {'盈亏':>7} {'$':>6} {'秒':>5} {'原因'}")
            L.append(f"  {'─' * 95}")
            for i, p in enumerate(self.closed_pos):
                t = datetime.fromtimestamp(p.entry_time).strftime("%H:%M:%S")
                m = "✓" if p.pnl_bps > 0 else "✗"
                conv = p.entry_mid_spread - p.close_mid_spread
                L.append(
                    f"  {m}{i+1:>2} {t} {p.symbol:<11} {p.direction:<7} "
                    f"{p.entry_mid_spread:>+6.1f} {p.close_mid_spread:>+6.1f} {conv:>+5.1f} "
                    f"{p.entry_l_slip:.1f}+{p.close_l_slip:.1f} "
                    f"{p.pnl_bps:>+7.2f} ${p.pnl_usd:>+5.2f} "
                    f"{p.convergence_sec:>4.1f}s {p.close_reason}")

            # 收敛时间分布
            ranges = [(0,5),(5,15),(15,30),(30,60),(60,120),(120,300),(300,600)]
            L.append(f"\n  ─── 收敛时间分布 ───")
            for lo, hi in ranges:
                c = sum(1 for p in self.closed_pos if lo <= p.convergence_sec < hi)
                bar = "█" * min(c, 50)
                L.append(f"  {lo:>4}-{hi:>3}s: {c:>3} {bar}")
            ov = sum(1 for p in self.closed_pos if p.convergence_sec >= 600)
            if ov:
                L.append(f"   ≥600s: {ov:>3} {'█' * ov}")

        # 价差波动
        L.append(f"\n  ─── 价差波动统计 (Top20波动) ───")
        L.append(f"  {'币种':<12} {'最小':>7} {'最大':>7} {'均值':>7} {'标准差':>7} {'样本':>5}")
        L.append(f"  {'─' * 50}")
        stats = []
        for sym, hist in self.spread_log.items():
            if len(hist) < 20:
                continue
            vals = [v for _, v in hist]
            mean = sum(vals) / len(vals)
            std = (sum((v - mean)**2 for v in vals) / len(vals)) ** 0.5
            stats.append((sym, min(vals), max(vals), mean, std, len(vals)))
        stats.sort(key=lambda x: x[4], reverse=True)
        for sym, mn, mx, av, sd, cn in stats[:20]:
            L.append(f"  {sym:<12} {mn:>+7.1f} {mx:>+7.1f} {av:>+7.1f} {sd:>7.1f} {cn:>5}")

        if self.open_pos:
            L.append(f"\n  ─── 未平仓位 ({len(self.open_pos)} 个) ───")
            for sym, pos in self.open_pos.items():
                ps = self.pairs[sym]
                h = time.time() - pos.entry_time
                L.append(f"  {sym:<12} {pos.direction}  "
                         f"入场{pos.entry_mid_spread:+.1f}→当前{ps.mid_spread_bps:+.1f}bps  "
                         f"L滑{pos.entry_l_slip:.1f}bps  持{h:.0f}s")

        L.append(f"\n{'=' * W}\n")

        report = "\n".join(L)
        print(report, flush=True)

        try:
            with open(REPORT_FILE, "w", encoding="utf-8") as f:
                f.write(report)
            log(f"✓ 报告已保存 → {REPORT_FILE}")
        except Exception as e:
            log(f"保存报告失败: {e}", "错误")


async def main():
    sim = ArbitrageSim()
    await sim.run()

if __name__ == "__main__":
    print()
    print("  ╔════════════════════════════════════════════════════╗")
    print("  ║   币安 vs Lighter — 价差套利模拟器               ║")
    print("  ║   中间价差 + 深度滑点 + 自动开平仓 + 收敛跟踪   ║")
    print("  ╚════════════════════════════════════════════════════╝")
    print()
    asyncio.run(main())
