#!/usr/bin/env python3
"""
币安 vs Lighter — 真实延迟模拟器
=================================
模拟真实执行环境下的两种套利策略对比:

方案1: 币安市价开仓(taker) + 挂单平仓(maker), Lighter全市价(taker)
方案2: 币安全挂单(maker开+maker平), Lighter全市价(taker)

真实延迟建模:
  - Lighter 免费账户 300ms 执行延迟
  - 币安 taker: ~100ms 网络延迟 (本机)
  - 币安 maker: 挂单后需等待成交, 根据实时盘口模拟fill概率
  - 价差在延迟期间会变化, 使用延迟后的真实价格结算

关键区别:
  方案1开仓: 检测→300ms后L市价+100ms后B市价 → ~300ms对冲完成, 确定性高
  方案1平仓: 检测→B挂单等成交→成交后300ms L市价 → 平仓有延迟但省费用
  方案2开仓: 检测→B挂单等成交→成交后300ms L市价 → 开仓有延迟, 可能错过
  方案2平仓: 同开仓逻辑
"""

import asyncio
import json
import random
import time
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from collections import defaultdict

import aiohttp
import websockets

# ─── 配置 ──────────────────────────────────────────────

LIGHTER_WS = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_REST = "https://mainnet.zklighter.elliot.ai"
BINANCE_FAPI = "https://fapi.binance.com"

TRADE_SIZE_USD = 500.0

B_MAKER_BPS = 2.0
B_TAKER_BPS = 5.0
L_TAKER_BPS = 0.0

LIGHTER_DELAY_SEC = 0.300    # 免费账户300ms延迟
BINANCE_TAKER_DELAY_SEC = 0.100  # 网络延迟

OPEN_THRESHOLD_BPS = 8.0     # 降低开仓门槛, 捕捉更多波动
CLOSE_THRESHOLD_BPS = 3.0    # 放宽平仓条件
MAX_HOLD_SEC = 90            # 最长持仓90秒(强制对比)
COOLDOWN_SEC = 15            # 缩短冷却
MAX_MAKER_WAIT_SEC = 20      # 挂单最长等待
MAKER_TIMEOUT_TO_TAKER = True  # 超时后转市价

BINANCE_POLL_SEC = 0.5       # 更高频轮询, 更精确模拟maker fill
DISPLAY_SEC = 10.0
RUN_MINUTES = 5
REPORT_FILE = "delay_sim_report.txt"
MAX_SPREAD_BPS = 50.0
STRUCTURAL_WINDOW_SEC = 20   # 观察窗口
STRUCTURAL_MIN_BPS = 15.0    # 均值>15bps且标准差<3bps视为结构性

EXCLUDED = {
    "EURUSD","GBPUSD","USDJPY","USDCHF","USDCAD","AUDUSD","NZDUSD",
    "USDKRW","XAU","XAG","XCU","XPD","XPT","WTI","URA",
    "NVDA","TSLA","AAPL","AMZN","MSFT","GOOGL","META","HOOD","COIN",
    "PLTR","INTC","AMD","ASML","MSTR","SNDK",
    "SPY","QQQ","IWM","DIA","BOTZ","MAGS",
    "SAMSUNG","HYUNDAI","SKHYNIX","KRCOMP","HANMI","PAXG",
    "1000TOSHI","MEGA",
    "EDGE","STBL","DYDX","CRV","MORPHO","MET","FIL","RIVER",
}
SYMBOL_REMAP = {
    "1000PEPE":"1000PEPE","1000SHIB":"1000SHIB",
    "1000BONK":"1000BONK","1000FLOKI":"1000FLOKI",
}

# ─── 数据结构 ──────────────────────────────────────────

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
    b_half_spread_bps: float = 0.0
    b_tick_size: float = 0.01
    # 历史 bid/ask 用于判断 maker fill
    b_bid_history: List[Tuple[float, float]] = field(default_factory=list)
    b_ask_history: List[Tuple[float, float]] = field(default_factory=list)


class MakerState:
    """币安挂单的状态机"""
    PENDING = "PENDING"
    FILLED = "FILLED"
    TIMEOUT = "TIMEOUT"


@dataclass
class PendingMaker:
    """一个等待成交的币安挂单"""
    symbol: str
    side: str          # "buy" or "sell"
    price: float       # 挂单价格
    placed_at: float   # 挂单时间
    purpose: str       # "open" or "close"
    state: str = MakerState.PENDING
    filled_at: float = 0.0
    fill_delay_sec: float = 0.0


@dataclass
class Position:
    symbol: str
    strategy: str
    direction: str        # "L多B空" or "B多L空"

    detect_time: float = 0.0
    detect_spread: float = 0.0

    entry_time: float = 0.0
    entry_spread: float = 0.0
    entry_l_slip: float = 0.0
    entry_b_cost: float = 0.0    # taker的bid-ask成本 或 maker的0
    entry_b_delay: float = 0.0   # 币安侧实际延迟
    entry_l_delay: float = 0.0   # Lighter侧延迟 (300ms)
    entry_spread_loss: float = 0.0  # 延迟导致的价差损失

    close_time: float = 0.0
    close_spread: float = 0.0
    close_l_slip: float = 0.0
    close_b_cost: float = 0.0
    close_b_delay: float = 0.0
    close_spread_loss: float = 0.0
    close_reason: str = ""

    pnl_bps: float = 0.0
    pnl_usd: float = 0.0
    hold_sec: float = 0.0
    b_maker_fill_wait: float = 0.0  # 币安挂单等成交的时间


@dataclass
class StrategyEngine:
    name: str
    label: str
    b_open_type: str     # "taker" or "maker"
    b_close_type: str    # "taker" or "maker"
    rt_fee_bps: float

    open_pos: Dict[str, Position] = field(default_factory=dict)
    closed: List[Position] = field(default_factory=list)
    cooldowns: Dict[str, float] = field(default_factory=dict)

    pending_open: Dict[str, PendingMaker] = field(default_factory=dict)
    pending_close: Dict[str, PendingMaker] = field(default_factory=dict)
    pending_l_exec: Dict[str, float] = field(default_factory=dict)

    missed_count: int = 0     # 因延迟错过的机会
    timeout_count: int = 0    # 挂单超时转市价
    fill_delays: List[float] = field(default_factory=list)  # 挂单等成交时间
    spread_losses: List[float] = field(default_factory=list)  # 延迟价差损失


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


class DelaySimulator:
    def __init__(self):
        self.pairs: Dict[str, PairState] = {}
        self.mid_to_sym: Dict[int, str] = {}
        self.running = True
        self.start_time = time.time()
        self.lighter_ok = False
        self.binance_ok = False
        self.spread_log: Dict[str, List[Tuple[float, float]]] = defaultdict(list)

        self.s1 = StrategyEngine(
            name="S1_B市开挂平",
            label="方案1: B市价开+挂单平, L全市价",
            b_open_type="taker",
            b_close_type="maker",
            rt_fee_bps=B_TAKER_BPS + L_TAKER_BPS + B_MAKER_BPS + L_TAKER_BPS,  # 7
        )
        self.s2 = StrategyEngine(
            name="S2_B全挂",
            label="方案2: B全挂单, L全市价",
            b_open_type="maker",
            b_close_type="maker",
            rt_fee_bps=B_MAKER_BPS + L_TAKER_BPS + B_MAKER_BPS + L_TAKER_BPS,  # 4
        )
        self.strategies = [self.s1, self.s2]

        # 无延迟理想版本用于对比
        self.ideal_closed: List[Position] = []

    async def run(self):
        log("═══ 真实延迟模拟器: 方案1 vs 方案2 ═══")
        log(f"方案1: B市价开({B_TAKER_BPS}bps)+挂单平({B_MAKER_BPS}bps) + L全市价  往返={self.s1.rt_fee_bps}bps")
        log(f"方案2: B全挂单({B_MAKER_BPS}bps开+{B_MAKER_BPS}bps平) + L全市价  往返={self.s2.rt_fee_bps}bps")
        log(f"延迟: Lighter={LIGHTER_DELAY_SEC*1000:.0f}ms  B_taker={BINANCE_TAKER_DELAY_SEC*1000:.0f}ms")
        log(f"B挂单超时={MAX_MAKER_WAIT_SEC}s → 转市价")
        log(f"单笔 ${TRADE_SIZE_USD:.0f}  开仓>{OPEN_THRESHOLD_BPS}bps  平仓<{CLOSE_THRESHOLD_BPS}bps")
        log(f"运行 {RUN_MINUTES} 分钟")
        print()

        async with aiohttp.ClientSession() as session:
            await self._discover(session)
            if not self.pairs:
                log("未找到交易对!", "错误")
                return
            log(f"共 {len(self.pairs)} 个交易对")

            tasks = [
                asyncio.create_task(self._lighter_ws()),
                asyncio.create_task(self._binance_poll(session)),
                asyncio.create_task(self._engine()),
                asyncio.create_task(self._display()),
            ]

            await asyncio.sleep(RUN_MINUTES * 60)
            self.running = False
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        self._save_report()

    # ─── 数据获取 (与之前相同) ─────────────────────────

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
                    log(f"Lighter WS 已连接")
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

    async def _binance_poll(self, session):
        while self.running:
            try:
                async with session.get(
                    f"{BINANCE_FAPI}/fapi/v1/ticker/bookTicker",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as r:
                    tickers = await r.json()
                tmap = {t["symbol"]: t for t in tickers}
                now = time.time()
                for sym, ps in self.pairs.items():
                    t = tmap.get(ps.binance_symbol)
                    if t:
                        new_bid = float(t["bidPrice"])
                        new_ask = float(t["askPrice"])
                        ps.b_bid = new_bid
                        ps.b_ask = new_ask
                        ps.b_bid_history.append((now, new_bid))
                        ps.b_ask_history.append((now, new_ask))
                        if len(ps.b_bid_history) > 600:
                            ps.b_bid_history = ps.b_bid_history[-400:]
                            ps.b_ask_history = ps.b_ask_history[-400:]
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

    def _is_structural(self, sym: str, now: float) -> bool:
        """动态检测结构性价差: 过去N秒内均值高且波动低"""
        hist = self.spread_log.get(sym, [])
        if len(hist) < 10:
            return False
        cutoff = now - STRUCTURAL_WINDOW_SEC
        recent = [v for t, v in hist if t >= cutoff]
        if len(recent) < 5:
            return False
        abs_vals = [abs(v) for v in recent]
        mean = sum(abs_vals) / len(abs_vals)
        if mean < STRUCTURAL_MIN_BPS:
            return False
        std = (sum((v - mean)**2 for v in abs_vals) / len(abs_vals)) ** 0.5
        return std < 3.0

    # ─── 币安 Maker Fill 模拟 ────────────────────────────

    def _check_maker_fill(self, pm: PendingMaker, ps: PairState) -> bool:
        """
        判断币安挂单是否已成交.

        逻辑: 挂单价格 P 在 best_bid (买) 或 best_ask (卖).
        - 买单 P=best_bid: 当有新卖单以 ≤P 价格成交时 fill.
          实际表现: 当 best_bid 在我的价格或更低时(卖压到达), 有fill概率.
        - 卖单 P=best_ask: 当 best_ask 在我的价格或更高时(买压到达), 有fill概率.

        $500 是小单, 在活跃市场中每个 tick 处理量远大于此.
        使用概率模型: 每个 tick 有 fill_prob 概率成交.
        价格在我方 → 高概率; 价格远离 → 低概率.
        """
        now = time.time()
        elapsed = now - pm.placed_at
        if elapsed < 0.05:
            return False

        if pm.side == "buy":
            distance_bps = (ps.b_bid - pm.price) / pm.price * 10000.0 if pm.price > 0 else 0
            # distance_bps > 0 → 市场bid已高于我的挂单价 (市场涨了, 有人愿意以更高价买)
            # distance_bps = 0 → bid还在我的价位 (正常)
            # distance_bps < 0 → bid低于我的挂单价 (市场跌了, 我的价应该被fill了)
            if distance_bps <= -2.0:
                return True  # 市场卖穿我的价格
            if distance_bps <= 0:
                fill_prob = 0.35  # 在我的价位, 每tick 35%概率 ($500 小单)
            elif distance_bps <= 3.0:
                fill_prob = 0.08  # 稍微远离
            elif distance_bps <= 8.0:
                fill_prob = 0.02  # 较远
            else:
                fill_prob = 0.005  # 很远

        else:  # sell
            distance_bps = (pm.price - ps.b_ask) / pm.price * 10000.0 if pm.price > 0 else 0
            if distance_bps <= -2.0:
                return True
            if distance_bps <= 0:
                fill_prob = 0.35
            elif distance_bps <= 3.0:
                fill_prob = 0.08
            elif distance_bps <= 8.0:
                fill_prob = 0.02
            else:
                fill_prob = 0.005

        return random.random() < fill_prob

    # ─── 策略引擎 ─────────────────────────────────────────

    async def _engine(self):
        await asyncio.sleep(5)
        log("策略引擎启动 (含真实延迟模拟)...")

        while self.running:
            now = time.time()

            for se in self.strategies:
                self._process_pending_makers(se, now)
                self._process_lighter_delays(se, now)

            for sym, ps in self.pairs.items():
                if ps.l_mid <= 0 or ps.b_mid <= 0:
                    continue

                abs_spread = abs(ps.mid_spread_bps)

                for se in self.strategies:
                    # === 有持仓且已完全开仓: 检查平仓 ===
                    if sym in se.open_pos:
                        pos = se.open_pos[sym]
                        if pos.entry_time <= 0:
                            continue
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
                        elif hold >= MAX_HOLD_SEC:
                            should_close = True
                            reason = f"超时{hold:.0f}s"

                        if (should_close
                            and sym not in se.pending_close
                            and f"close_{sym}" not in se.pending_l_exec):
                            self._initiate_close(se, sym, ps, reason)

                    # === 无持仓也无待开仓: 寻找机会 ===
                    elif sym not in se.pending_open:
                        if now < se.cooldowns.get(sym, 0):
                            continue
                        if abs_spread >= OPEN_THRESHOLD_BPS and abs_spread <= MAX_SPREAD_BPS:
                            if not self._is_structural(sym, now):
                                self._initiate_open(se, sym, ps)

            await asyncio.sleep(0.3)

    def _initiate_open(self, se: StrategyEngine, sym: str, ps: PairState):
        """发起开仓: 根据策略决定是立即执行(taker)还是挂单等待(maker)"""
        now = time.time()

        if ps.mid_spread_bps > 0:
            direction = "L多B空"
            l_slip = ps.l_slip_buy_bps
        else:
            direction = "B多L空"
            l_slip = ps.l_slip_sell_bps

        if l_slip > 50:
            return

        detect_spread = abs(ps.mid_spread_bps)

        if se.b_open_type == "taker":
            # 币安市价: 立即成交(+100ms延迟), 然后Lighter 300ms后执行
            # 总延迟 ~300ms, 使用300ms后的价格
            pos = Position(
                symbol=sym, strategy=se.name, direction=direction,
                detect_time=now, detect_spread=detect_spread,
            )
            se.open_pos[sym] = pos
            # 调度 300ms 后 Lighter 执行 + 计算实际成本
            se.pending_l_exec[f"open_{sym}"] = now + LIGHTER_DELAY_SEC

        else:
            # 币安挂单: 放一个 maker 订单, 等待成交
            if direction == "L多B空":
                maker_side = "sell"
                maker_price = ps.b_ask
            else:
                maker_side = "buy"
                maker_price = ps.b_bid

            pm = PendingMaker(
                symbol=sym, side=maker_side, price=maker_price,
                placed_at=now, purpose="open",
            )
            se.pending_open[sym] = pm

            pos = Position(
                symbol=sym, strategy=se.name, direction=direction,
                detect_time=now, detect_spread=detect_spread,
            )
            se.open_pos[sym] = pos

    def _initiate_close(self, se: StrategyEngine, sym: str, ps: PairState, reason: str):
        """发起平仓"""
        now = time.time()
        pos = se.open_pos.get(sym)
        if not pos:
            return

        pos.close_reason = reason

        if se.b_close_type == "taker":
            pos.close_b_cost = ps.b_half_spread_bps
            se.pending_l_exec[f"close_{sym}"] = now + LIGHTER_DELAY_SEC
        else:
            if pos.direction == "L多B空":
                maker_side = "buy"
                maker_price = ps.b_bid
            else:
                maker_side = "sell"
                maker_price = ps.b_ask

            pm = PendingMaker(
                symbol=sym, side=maker_side, price=maker_price,
                placed_at=now, purpose="close",
            )
            se.pending_close[sym] = pm

    def _process_pending_makers(self, se: StrategyEngine, now: float):
        """处理等待成交的币安挂单"""
        # 开仓挂单
        to_remove_open = []
        for sym, pm in se.pending_open.items():
            ps = self.pairs.get(sym)
            if not ps:
                continue

            elapsed = now - pm.placed_at

            if self._check_maker_fill(pm, ps):
                pm.state = MakerState.FILLED
                pm.filled_at = now
                pm.fill_delay_sec = elapsed
                se.fill_delays.append(elapsed)
                # 币安 maker 成交 → 调度 Lighter taker (300ms后)
                se.pending_l_exec[f"open_{sym}"] = now + LIGHTER_DELAY_SEC
                to_remove_open.append(sym)

            elif elapsed >= MAX_MAKER_WAIT_SEC:
                # 超时: 放弃这个机会
                pm.state = MakerState.TIMEOUT
                se.timeout_count += 1
                se.missed_count += 1
                to_remove_open.append(sym)
                # 移除关联的 position
                se.open_pos.pop(sym, None)
                se.cooldowns[sym] = now + COOLDOWN_SEC

        for sym in to_remove_open:
            se.pending_open.pop(sym, None)

        # 平仓挂单
        to_remove_close = []
        for sym, pm in se.pending_close.items():
            ps = self.pairs.get(sym)
            if not ps:
                continue

            elapsed = now - pm.placed_at

            if self._check_maker_fill(pm, ps):
                pm.state = MakerState.FILLED
                pm.filled_at = now
                pm.fill_delay_sec = elapsed
                se.fill_delays.append(elapsed)
                # 币安 maker 平仓成交 → 调度 Lighter taker
                se.pending_l_exec[f"close_{sym}"] = now + LIGHTER_DELAY_SEC
                to_remove_close.append(sym)

            elif elapsed >= MAX_MAKER_WAIT_SEC:
                if MAKER_TIMEOUT_TO_TAKER:
                    # 超时转市价平仓
                    se.timeout_count += 1
                    se.pending_l_exec[f"close_{sym}"] = now + LIGHTER_DELAY_SEC
                    to_remove_close.append(sym)
                    pos = se.open_pos.get(sym)
                    if pos:
                        pos.close_b_cost = ps.b_half_spread_bps
                        pos.close_reason = f"挂单超时{elapsed:.0f}s→市价"
                else:
                    pm.placed_at = now
                    if pm.side == "buy":
                        pm.price = ps.b_bid
                    else:
                        pm.price = ps.b_ask

        for sym in to_remove_close:
            se.pending_close.pop(sym, None)

    def _process_lighter_delays(self, se: StrategyEngine, now: float):
        """处理 Lighter 300ms 延迟后的执行"""
        to_remove = []
        for key, exec_time in se.pending_l_exec.items():
            if now < exec_time:
                continue

            to_remove.append(key)
            parts = key.split("_", 1)
            action = parts[0]
            sym = parts[1]
            ps = self.pairs.get(sym)
            pos = se.open_pos.get(sym)
            if not ps or not pos:
                continue

            if action == "open":
                self._execute_open(se, sym, ps, pos, now)
            elif action == "close":
                self._execute_close(se, sym, ps, pos, now)

        for key in to_remove:
            se.pending_l_exec.pop(key, None)

    def _execute_open(self, se: StrategyEngine, sym: str, ps: PairState, pos: Position, now: float):
        """Lighter 延迟结束, 用当前价格完成开仓"""
        pos.entry_time = now
        current_spread = abs(ps.mid_spread_bps)
        pos.entry_spread = current_spread
        pos.entry_spread_loss = max(0, pos.detect_spread - current_spread)
        se.spread_losses.append(pos.entry_spread_loss)

        if pos.direction == "L多B空":
            pos.entry_l_slip = ps.l_slip_buy_bps
        else:
            pos.entry_l_slip = ps.l_slip_sell_bps

        if se.b_open_type == "taker":
            pos.entry_b_cost = ps.b_half_spread_bps
            pos.entry_b_delay = BINANCE_TAKER_DELAY_SEC
        else:
            pos.entry_b_cost = 0.0

        pos.entry_l_delay = LIGHTER_DELAY_SEC

        total_open_cost = (pos.entry_l_slip + pos.entry_b_cost + se.rt_fee_bps)
        if current_spread < total_open_cost * 0.5:
            se.open_pos.pop(sym, None)
            se.missed_count += 1
            se.cooldowns[sym] = now + COOLDOWN_SEC
            return

        log(f"[{se.name}] 开仓 {sym} {pos.direction} "
            f"检测{pos.detect_spread:+.1f}→执行{current_spread:+.1f} "
            f"L滑{pos.entry_l_slip:.1f} B成本{pos.entry_b_cost:.1f}")

    def _execute_close(self, se: StrategyEngine, sym: str, ps: PairState, pos: Position, now: float):
        """完成平仓, 计算最终PnL"""
        pos.close_time = now
        pos.hold_sec = now - pos.entry_time

        if pos.direction == "L多B空":
            cur_dir = ps.mid_spread_bps
            pos.close_l_slip = ps.l_slip_sell_bps
        else:
            cur_dir = -ps.mid_spread_bps
            pos.close_l_slip = ps.l_slip_buy_bps

        pos.close_spread = max(0, cur_dir)

        # 币安平仓成本
        if se.b_close_type == "taker" or "市价" in (pos.close_reason or ""):
            pos.close_b_cost = ps.b_half_spread_bps
        else:
            pos.close_b_cost = 0.0

        # PnL = 入场价差 - 出场价差 - L入滑 - L出滑 - B入差成本 - B出差成本 - 固定费
        pos.pnl_bps = (pos.entry_spread
                        - pos.close_spread
                        - pos.entry_l_slip - pos.close_l_slip
                        - pos.entry_b_cost - pos.close_b_cost
                        - se.rt_fee_bps)
        pos.pnl_usd = TRADE_SIZE_USD * pos.pnl_bps / 10000.0

        if not pos.close_reason:
            pos.close_reason = f"收敛{cur_dir:+.1f}bps"

        se.open_pos.pop(sym, None)
        se.closed.append(pos)
        se.cooldowns[sym] = now + COOLDOWN_SEC

        m = "✓" if pos.pnl_bps > 0 else "✗"
        log(f"[{se.name}] {m} 平仓 {sym} {pos.pnl_bps:+.2f}bps "
            f"持{pos.hold_sec:.1f}s [{pos.close_reason}]")

    # ─── 显示 ─────────────────────────────────────────────

    async def _display(self):
        await asyncio.sleep(10)
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

        print(f"\n  ─── 方案1 vs 方案2 (含真实延迟) ───")
        print(f"  {'方案':<38} {'费用':>4} {'完成':>4} {'胜率':>6} {'累bps':>7} "
              f"{'累$':>7} {'均盈亏':>6} {'错过':>4} {'超时':>4} {'持仓':>4} {'待开':>4} {'待平':>4} "
              f"{'均B等':>6} {'均差损':>6}")
        print(f"  {'─' * 115}")

        for se in self.strategies:
            n = len(se.closed)
            w = sum(1 for t in se.closed if t.pnl_bps > 0)
            tp = sum(t.pnl_bps for t in se.closed)
            tu = sum(t.pnl_usd for t in se.closed)
            avg = tp / n if n else 0
            wr = f"{w}/{n}" if n else "0/0"
            avg_fd = sum(se.fill_delays) / len(se.fill_delays) if se.fill_delays else 0
            avg_sl = sum(se.spread_losses) / len(se.spread_losses) if se.spread_losses else 0
            po = len(se.pending_open)
            pc = len(se.pending_close)
            active = sum(1 for p in se.open_pos.values() if p.entry_time > 0)

            print(f"  {se.label:<38} {se.rt_fee_bps:>4.0f} {n:>4} {wr:>6} {tp:>+7.1f} "
                  f"${tu:>+6.2f} {avg:>+6.2f} {se.missed_count:>4} {se.timeout_count:>4} "
                  f"{active:>4} {po:>4} {pc:>4} {avg_fd:>5.1f}s {avg_sl:>+5.1f}")

        # 最近交易
        for se in self.strategies:
            if se.closed:
                recent = se.closed[-2:]
                print(f"\n  ─── {se.name} 最近交易 ───")
                for t in reversed(recent):
                    m = "✓" if t.pnl_bps > 0 else "✗"
                    delay_info = ""
                    if t.b_maker_fill_wait > 0:
                        delay_info = f" B等{t.b_maker_fill_wait:.1f}s"
                    print(f"  {m} {t.symbol:<10} {t.direction} "
                          f"检测{t.detect_spread:+.1f}→执行{t.entry_spread:+.1f}→出{t.close_spread:+.1f} "
                          f"L滑{t.entry_l_slip:.1f}+{t.close_l_slip:.1f} "
                          f"B成本{t.entry_b_cost:.1f}+{t.close_b_cost:.1f}{delay_info} "
                          f"{t.pnl_bps:>+.2f}bps {t.hold_sec:.1f}s [{t.close_reason}]")

        # 价差排行
        active = [(s, p) for s, p in self.pairs.items()
                   if p.l_mid > 0 and p.b_mid > 0]
        active.sort(key=lambda x: abs(x[1].mid_spread_bps), reverse=True)
        print(f"\n  ─── 价差 TOP 10 ───")
        for sym, ps in active[:10]:
            print(f"  {sym:<11} {ps.mid_spread_bps:>+6.1f} L滑买{ps.l_slip_buy_bps:>4.1f} "
                  f"卖{ps.l_slip_sell_bps:>4.1f} B半差{ps.b_half_spread_bps:>4.1f}")

        print(f"  {'═' * W}")
        sys.stdout.flush()

    # ─── 报告 ─────────────────────────────────────────────

    def _save_report(self):
        L = []
        W = 105
        elapsed = time.time() - self.start_time

        L.append(f"\n{'=' * W}")
        L.append(f"  币安 vs Lighter — 真实延迟模拟对比报告")
        L.append(f"  运行: {elapsed/60:.1f} 分钟  |  单笔: ${TRADE_SIZE_USD:.0f}")
        L.append(f"  延迟参数: Lighter={LIGHTER_DELAY_SEC*1000:.0f}ms  "
                 f"B_taker={BINANCE_TAKER_DELAY_SEC*1000:.0f}ms  "
                 f"B_maker最长等待={MAX_MAKER_WAIT_SEC}s")
        L.append(f"  开仓: |价差|>{OPEN_THRESHOLD_BPS}bps  平仓: <{CLOSE_THRESHOLD_BPS}bps")
        L.append(f"{'=' * W}")

        # 总对比
        L.append(f"\n  ╔{'═' * 100}╗")
        L.append(f"  ║  {'方案对比总结':^96}  ║")
        L.append(f"  ╠{'═' * 100}╣")

        header = (f"  ║ {'方案':<35} {'费用':>4} {'笔':>3} {'胜率':>7} "
                  f"{'累bps':>8} {'累$':>7} {'均盈':>6} {'错过':>4} {'超时':>4} "
                  f"{'均等fill':>7} {'均差损':>6} ║")
        L.append(header)
        L.append(f"  ╠{'═' * 100}╣")

        for se in self.strategies:
            n = len(se.closed)
            w = sum(1 for t in se.closed if t.pnl_bps > 0)
            tp = sum(t.pnl_bps for t in se.closed)
            tu = sum(t.pnl_usd for t in se.closed)
            avg = tp / n if n else 0
            wr = f"{w}/{n}" if n else "-"
            wrp = f"({w/n*100:.0f}%)" if n else ""
            avg_fd = sum(se.fill_delays) / len(se.fill_delays) if se.fill_delays else 0
            avg_sl = sum(se.spread_losses) / len(se.spread_losses) if se.spread_losses else 0

            L.append(f"  ║ {se.label:<35} {se.rt_fee_bps:>4.0f} {n:>3} "
                     f"{wr:>4}{wrp:>4} {tp:>+8.1f} ${tu:>+6.2f} "
                     f"{avg:>+6.2f} {se.missed_count:>4} {se.timeout_count:>4} "
                     f"{avg_fd:>6.1f}s {avg_sl:>+5.1f} ║")

        L.append(f"  ╚{'═' * 100}╝")

        # 每个策略详细
        for se in self.strategies:
            n = len(se.closed)
            if n == 0:
                L.append(f"\n  ─── {se.label}: 无成交 (错过{se.missed_count}个机会) ───")
                continue

            wins = [t for t in se.closed if t.pnl_bps > 0]
            losses = [t for t in se.closed if t.pnl_bps <= 0]
            tp = sum(t.pnl_bps for t in se.closed)
            tu = sum(t.pnl_usd for t in se.closed)

            L.append(f"\n  ═══ {se.label} (详细) ═══")
            L.append(f"  成交: {n}笔  胜率: {len(wins)}/{n} ({len(wins)/n*100:.1f}%)")
            L.append(f"  累计: {tp:>+.2f}bps (${tu:>+.2f})")
            L.append(f"  每笔: {tp/n:>+.2f}bps  盈利笔均: {sum(t.pnl_bps for t in wins)/len(wins) if wins else 0:>+.2f}bps")
            if losses:
                L.append(f"  亏损笔均: {sum(t.pnl_bps for t in losses)/len(losses):>+.2f}bps")
            L.append(f"  错过机会: {se.missed_count}  挂单超时: {se.timeout_count}")

            avg_detect = sum(t.detect_spread for t in se.closed) / n
            avg_entry = sum(t.entry_spread for t in se.closed) / n
            avg_loss = sum(t.entry_spread_loss for t in se.closed) / n
            avg_l_slip = sum(t.entry_l_slip + t.close_l_slip for t in se.closed) / n
            avg_b_cost = sum(t.entry_b_cost + t.close_b_cost for t in se.closed) / n
            avg_hold = sum(t.hold_sec for t in se.closed) / n

            L.append(f"\n  成本分解:")
            L.append(f"    均检测价差:  {avg_detect:>+.2f}bps (检测到机会时)")
            L.append(f"    均执行价差:  {avg_entry:>+.2f}bps (实际开仓时)")
            L.append(f"    延迟价差损:  {avg_loss:>+.2f}bps (因延迟丢失)")
            L.append(f"    均L滑点:     {avg_l_slip:>.2f}bps (开+平)")
            L.append(f"    均B价差成本: {avg_b_cost:>.2f}bps (taker才有)")
            L.append(f"    固定费用:    {se.rt_fee_bps:>.1f}bps (往返)")
            L.append(f"    均持仓:      {avg_hold:>.1f}s")

            if se.fill_delays:
                fd = se.fill_delays
                L.append(f"\n  币安挂单等成交时间:")
                L.append(f"    均值: {sum(fd)/len(fd):.1f}s  "
                         f"最快: {min(fd):.1f}s  最慢: {max(fd):.1f}s")
                fast = sum(1 for d in fd if d < 1)
                med = sum(1 for d in fd if 1 <= d < 5)
                slow = sum(1 for d in fd if 5 <= d < 15)
                vslow = sum(1 for d in fd if d >= 15)
                L.append(f"    <1s: {fast}  1-5s: {med}  5-15s: {slow}  >15s: {vslow}")

            if se.spread_losses:
                sl = se.spread_losses
                L.append(f"\n  延迟导致的价差损失:")
                L.append(f"    均值: {sum(sl)/len(sl):.2f}bps  "
                         f"最大: {max(sl):.2f}bps")
                no_loss = sum(1 for s in sl if s <= 0.5)
                L.append(f"    无明显损失(<0.5bps): {no_loss}/{len(sl)} ({no_loss/len(sl)*100:.0f}%)")

            # 按币种
            by_sym: Dict[str, List[Position]] = defaultdict(list)
            for t in se.closed:
                by_sym[t.symbol].append(t)
            L.append(f"\n  按币种:")
            for sym, trades in sorted(by_sym.items(),
                                        key=lambda x: sum(t.pnl_bps for t in x[1]),
                                        reverse=True):
                cn = len(trades)
                ct = sum(t.pnl_bps for t in trades)
                cu = sum(t.pnl_usd for t in trades)
                L.append(f"    {sym:<11} {cn}笔 {ct:>+7.1f}bps ${cu:>+5.2f}")

            # 每笔详情
            L.append(f"\n  每笔交易详情:")
            L.append(f"  {'#':>2} {'币种':<10} {'方向':<8} {'检测差':>6} {'执行差':>6} "
                     f"{'出场差':>6} {'L滑':>5} {'B成本':>5} {'PnL':>7} {'等fill':>6} "
                     f"{'持仓':>5} {'原因':<20}")
            for i, t in enumerate(se.closed):
                L.append(f"  {i+1:>2} {t.symbol:<10} {t.direction:<8} "
                         f"{t.detect_spread:>+6.1f} {t.entry_spread:>+6.1f} "
                         f"{t.close_spread:>+6.1f} {t.entry_l_slip+t.close_l_slip:>5.1f} "
                         f"{t.entry_b_cost+t.close_b_cost:>5.1f} {t.pnl_bps:>+7.2f} "
                         f"{t.b_maker_fill_wait:>5.1f}s {t.hold_sec:>5.1f}s "
                         f"{t.close_reason:<20}")

        # 结论
        s1n = len(self.s1.closed)
        s2n = len(self.s2.closed)
        s1p = sum(t.pnl_bps for t in self.s1.closed) if s1n else 0
        s2p = sum(t.pnl_bps for t in self.s2.closed) if s2n else 0

        L.append(f"\n  ═══ 结论 ═══")
        better = "方案1" if s1p >= s2p else "方案2"
        L.append(f"  最终赢家: {better}")
        L.append(f"")
        L.append(f"  方案1 (B市价开+挂平, 往返7bps):")
        L.append(f"    优势: 开仓快(~300ms对冲), 确定性高, 不会错过机会")
        L.append(f"    劣势: 开仓贵(B taker 5bps), 平仓等挂单有延迟风险")
        L.append(f"    实测: {s1n}笔 {s1p:+.1f}bps 错过{self.s1.missed_count}个")
        L.append(f"")
        L.append(f"  方案2 (B全挂, 往返4bps):")
        L.append(f"    优势: 费用最低(全maker 4bps)")
        L.append(f"    劣势: 开仓平仓都等挂单, 延迟大, 错过多")
        L.append(f"    实测: {s2n}笔 {s2p:+.1f}bps 错过{self.s2.missed_count}个")
        L.append(f"")
        L.append(f"  延迟影响:")
        s1_sl = self.s1.spread_losses
        s2_sl = self.s2.spread_losses
        L.append(f"    方案1 均价差损失: {sum(s1_sl)/len(s1_sl):.2f}bps" if s1_sl else "    方案1 均价差损失: N/A")
        L.append(f"    方案2 均价差损失: {sum(s2_sl)/len(s2_sl):.2f}bps" if s2_sl else "    方案2 均价差损失: N/A")
        s2_fd = self.s2.fill_delays
        L.append(f"    方案2 均挂单等待: {sum(s2_fd)/len(s2_fd):.1f}s" if s2_fd else "    方案2 均挂单等待: N/A")
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
    sim = DelaySimulator()
    await sim.run()

if __name__ == "__main__":
    print()
    print("  ╔════════════════════════════════════════════════════════════════╗")
    print("  ║  币安 vs Lighter — 真实延迟模拟器                           ║")
    print("  ║  方案1: B市价开+挂平 vs 方案2: B全挂  (含300ms+fill等待)    ║")
    print("  ╚════════════════════════════════════════════════════════════════╝")
    print()
    asyncio.run(main())
