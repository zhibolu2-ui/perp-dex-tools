#!/usr/bin/env python3
"""
HotStuff + Lighter V36: 双 Taker 套利 (HotStuff IOC 开仓 + Lighter IOC 对冲)

基于 spread_arb_v36 移植, 将 Extended 替换为 HotStuff:
  - 开仓: HotStuff Taker IOC (2.5 bps fee)
  - 对冲: Lighter Taker IOC (0 bps fee)
  - 往返费用: ≈2.5 bps
  - 双向自动: 同时监控两个方向价差, 选择更优方向开仓
  - L1 深度检查: 两个交易所均只检查第一档

阈值公式:
  dynamic_cost = open_fee + expected_close_cost + min(latency, cap)
               + lighter_slip + hs_slip + vol*weight + inv + edge_buffer
  threshold = max(open_spread, dynamic_cost)

费用:
  Lighter taker: 0 bps | HotStuff taker: 2.5 bps

用法:
  python hotstuff_lighter_arb_v36.py --symbol BTC --size 0.005 --max-position 0.02 \\
    --open-spread 3.5 --close-spread 0 --ladder-step 0.4 \\
    --expected-close-cost 0.8 --latency-cap 1.5 --edge-buffer 0.5
"""

from __future__ import annotations

import argparse
import asyncio
import atexit
import collections
import csv
import fcntl
import json
import logging
import os
import signal
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import statistics as _stats

import websockets
from dotenv import load_dotenv
from eth_account import Account
from lighter.signer_client import SignerClient
sys.path.insert(0, str(Path(__file__).resolve().parent))

from hotstuff import ExchangeClient, InfoClient
from hotstuff.methods.exchange.trading import (
    UnitOrder, PlaceOrderParams, CancelAllParams,
)
from hotstuff.methods.info.account import PositionsParams
from feeds.hotstuff_feed import HotstuffFeed

try:
    import uvloop
except ImportError:
    uvloop = None

# ═══════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════

LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_REST_URL = "https://mainnet.zklighter.elliot.ai"

SYMBOL_TO_MARKET = {"ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3}

FEE_HOTSTUFF_TAKER = Decimal("0.00025")   # 2.5 bps
FEE_LIGHTER_MAKER = Decimal("0")          # 免费账户 0 bps

LIGHTER_RATE_LIMIT = 36       # 安全上限(实际40), 留4次给紧急操作
LIGHTER_RATE_WINDOW = 60.0    # 窗口60秒

_TZ_CN = timezone(timedelta(hours=8))


class PidLock:
    """flock-based process lock to prevent duplicate instances per symbol."""

    def __init__(self, symbol: str):
        lock_dir = Path(__file__).resolve().parent / "logs"
        lock_dir.mkdir(exist_ok=True)
        self._path = lock_dir / f".hs_lighter_arb_{symbol}.pid"
        self._fd: Optional[int] = None

    def acquire(self) -> bool:
        """Try to acquire the lock. Returns True on success."""
        self._fd = os.open(str(self._path), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            existing = os.read(self._fd, 64).decode().strip()
            os.close(self._fd)
            self._fd = None
            print(f"[FATAL] 同一 symbol 已有实例在运行 (PID {existing}), 拒绝启动。"
                  f"\n  锁文件: {self._path}"
                  f"\n  如果确认旧进程已死, 删除锁文件后重试: rm {self._path}",
                  file=sys.stderr)
            return False
        os.ftruncate(self._fd, 0)
        os.lseek(self._fd, 0, os.SEEK_SET)
        os.write(self._fd, str(os.getpid()).encode())
        return True

    def release(self):
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                os.close(self._fd)
            except OSError:
                pass
            try:
                self._path.unlink(missing_ok=True)
            except OSError:
                pass
            self._fd = None


class State(str, Enum):
    IDLE = "IDLE"
    QUOTING = "QUOTING"
    HEDGING = "HEDGING"
    REPAIR = "REPAIR"




class LatencyTracker:
    """Tracks observed-vs-realized spread decay from live trades + shadow observations."""

    def __init__(self, window: int = 150, cold_default: float = 0.8):
        self._window = window
        self._cold_default = cold_default
        self._decays: collections.deque = collections.deque(maxlen=window)
        self._was_cold: bool = True

    def record(self, observed_bps: float, realized_bps: float):
        decay = observed_bps - realized_bps
        self._decays.append(max(decay, 0.0))

    @property
    def warm(self) -> bool:
        return len(self._decays) >= 20

    @property
    def p50(self) -> float:
        if not self.warm:
            return self._cold_default
        s = sorted(self._decays)
        idx = int(len(s) * 0.50)
        return s[min(idx, len(s) - 1)]

    @property
    def p75(self) -> float:
        if not self.warm:
            return self._cold_default
        s = sorted(self._decays)
        idx = int(len(s) * 0.75)
        return s[min(idx, len(s) - 1)]

    @property
    def p80(self) -> float:
        if not self.warm:
            return self._cold_default
        s = sorted(self._decays)
        idx = int(len(s) * 0.80)
        return s[min(idx, len(s) - 1)]

    @property
    def just_warmed(self) -> bool:
        """Returns True once when tracker transitions from cold to warm."""
        if self._was_cold and self.warm:
            self._was_cold = False
            return True
        return False

    def summary(self) -> str:
        if not self.warm:
            return f"cold(default={self._cold_default:.1f}bps, n={len(self._decays)})"
        return (f"p50={self.p50:.2f} p75={self.p75:.2f} p80={self.p80:.2f} "
                f"n={len(self._decays)}")


class BucketedLatencyPool:
    """V35: Manages per-side base trackers + bucketed (side, spread, vol) trackers
    with three-level fallback for latency cost estimation."""

    def __init__(self, cold_default: float = 0.8, window: int = 150):
        self._cold = cold_default
        self._window = window
        self.base: Dict[str, LatencyTracker] = {
            "buy": LatencyTracker(window, cold_default),
            "sell": LatencyTracker(window, cold_default),
        }
        self._bucketed: Dict[tuple, LatencyTracker] = {}

    def _get_or_create(self, key: tuple) -> LatencyTracker:
        t = self._bucketed.get(key)
        if t is None:
            t = LatencyTracker(self._window, self._cold)
            self._bucketed[key] = t
        return t

    def record(self, side: str, obs_bps: float, realized_bps: float,
               s_bucket: str, v_bucket: str):
        self.base[side].record(obs_bps, realized_bps)
        self._get_or_create((side, s_bucket)).record(obs_bps, realized_bps)
        self._get_or_create((side, s_bucket, v_bucket)).record(obs_bps, realized_bps)

    def get_latency(self, side: str, s_bucket: str, v_bucket: str) -> tuple:
        """Returns (p75_value, fallback_level) where level indicates resolution.
        Level 1 = full bucket, 2 = side+spread, 3 = base side."""
        t = self._bucketed.get((side, s_bucket, v_bucket))
        if t and t.warm:
            return t.p75, 1
        t = self._bucketed.get((side, s_bucket))
        if t and t.warm:
            return t.p75, 2
        return self.base[side].p75, 3

    def summary(self, side: str, s_bucket: str, v_bucket: str) -> str:
        val, lvl = self.get_latency(side, s_bucket, v_bucket)
        tag = {1: f"{s_bucket}/{v_bucket}", 2: s_bucket, 3: "base"}[lvl]
        return f"{val:.1f}({tag})"


# ═══════════════════════════════════════════════════════════════════
#  Bot
# ═══════════════════════════════════════════════════════════════════

class SpreadArbBot:

    def __init__(
        self,
        symbol: str,
        order_size: Decimal,
        max_position: Decimal,
        open_spread_bps: float,
        close_spread_bps: float,
        max_order_age: float,
        hedge_timeout: float,
        interval: float,
        dry_run: bool,
        spread_buffer_bps: float = 0.0,
        ladder_step_bps: float = 0.0,
        close_deep_bps: float = -0.5,
        close_wait_sec: float = 2.0,
        # V34 new params
        vol_weight: float = 0.5,
        inv_weight: float = 1.0,
        persist_count: int = 3,
        persist_ms: float = 200.0,
        latency_cold: float = 0.8,
        # V35.1 entry economics params
        expected_close_cost: float = 0.8,
        latency_cap: float = 1.5,
        edge_buffer: float = 0.5,
        # V35.2 close depth params
        close_depth_levels: int = 2,
        # V36 open depth params
        open_depth_levels: int = 2,
    ):
        self.symbol = symbol.upper()
        self.order_size = order_size
        self.max_position = max_position
        self.open_spread_bps = open_spread_bps
        self.close_spread_bps = close_spread_bps
        self.spread_buffer_bps = spread_buffer_bps
        self.ladder_step_bps = ladder_step_bps
        self.close_deep_bps = close_deep_bps
        self.close_wait_sec = close_wait_sec
        self.hedge_timeout = hedge_timeout
        self.max_order_age = max_order_age
        self.interval = interval
        self.dry_run = dry_run

        # ── V34: Dynamic threshold params ──
        self.vol_weight = vol_weight
        self.inv_weight = inv_weight
        self.persist_count = persist_count
        self.persist_ms = persist_ms / 1000.0  # convert to seconds

        # ── V35.1: Decoupled entry economics ──
        self.expected_close_cost = expected_close_cost
        self.latency_cap = latency_cap
        self.edge_buffer = edge_buffer

        # ── V35.2: Close depth check ──
        self.close_depth_levels = close_depth_levels

        # ── V36: Open depth check ──
        self.open_depth_levels = open_depth_levels

        self.state = State.IDLE
        self.stop_flag = False
        self._in_flatten = False

        # ── Positions ──
        self.lighter_position = Decimal("0")
        self.hotstuff_position = Decimal("0")

        # ── Lighter market config ──
        self.lighter_market_index: int = SYMBOL_TO_MARKET.get(self.symbol, 1)
        self.account_index: int = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
        self.api_key_index: int = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))
        self.base_amount_multiplier: int = 0
        self.price_multiplier: int = 0
        self.lighter_tick = Decimal("0.1")
        self.lighter_min_size_step = Decimal("0.00001")

        # ── HotStuff config ──
        self.hs_symbol: str = ""          # e.g. "BTC-PERP"
        self.hs_instrument_id: int = 0
        self.hs_tick_size = Decimal("1")
        self.hs_lot_size = Decimal("0.001")
        self.hs_address: str = ""

        # ── Lighter maker state ──
        self._l_buy_client_id: Optional[int] = None
        self._l_sell_client_id: Optional[int] = None
        self._l_buy_order_idx: Optional[int] = None
        self._l_sell_order_idx: Optional[int] = None
        self._l_buy_price = Decimal("0")
        self._l_sell_price = Decimal("0")
        self._l_placed_at: float = 0.0
        self._l_hs_bid_at_place = Decimal("0")
        self._l_hs_ask_at_place = Decimal("0")
        self._last_modify_ts: float = 0.0

        # ── Lighter API rate limiter (40 req / 60s for free accounts) ──
        self._lighter_req_times: collections.deque = collections.deque()

        # ── Fill state ──
        self._fill_event = asyncio.Event()
        self._fill_side: Optional[str] = None
        self._fill_price = Decimal("0")
        self._fill_qty = Decimal("0")

        # ── Pending additional hedge qty from partial fills ──
        self._fill_qty_pending = Decimal("0")

        # ── Close fill tracking (Lighter actual price during flatten) ──
        self._flatten_fill_price: Optional[Decimal] = None
        self._flatten_fill_qty = Decimal("0")
        self._flatten_fill_event = asyncio.Event()
        self._flatten_ioc_cid: Optional[int] = None

        # ── Stale fills queue ──
        self._stale_fills: List[dict] = []

        # ── Repair IOC tracking (防止对账修复死循环) ──
        self._repair_ioc_cids: set = set()

        # ── WS fill dedup (防止重连后重复计仓) ──
        self._processed_fill_keys: set = set()

        # ── Lighter OB state (set by _lighter_ws_loop) ──
        self._l_ob: Dict[str, Dict[float, float]] = {"bids": {}, "asks": {}}
        self._l_ob_ready: bool = False
        self._l_best_bid: Optional[Decimal] = None
        self._l_best_ask: Optional[Decimal] = None
        self._l_ws_ts: float = 0.0

        # ── Clients ──
        self.lighter_client: Optional[SignerClient] = None
        self.hs_exchange: Optional[ExchangeClient] = None
        self.hs_info: Optional[InfoClient] = None
        self.hs_feed: Optional[HotstuffFeed] = None

        # ── HotStuff BBO cache ──
        self._hs_best_bid: Optional[Decimal] = None
        self._hs_best_ask: Optional[Decimal] = None
        self._hs_l1_bid_size: float = 0.0
        self._hs_l1_ask_size: float = 0.0
        self._hs_bbo_ts: float = 0.0

        # ── V22: Fill-time HotStuff BBO snapshot ──
        self._fill_hs_bid: Optional[Decimal] = None
        self._fill_hs_ask: Optional[Decimal] = None
        self._fill_ts: float = 0.0

        # ── V22: Instant hedge result (set by _instant_hedge task) ──
        self._instant_hedge_task: Optional[asyncio.Task] = None
        self._instant_hedge_done = asyncio.Event()
        self._instant_hedge_ok: bool = False
        self._instant_hedge_fill_price: Optional[Decimal] = None
        self._instant_hedge_ms: float = 0.0

        # ── Risk ──
        self._consecutive_hedge_fails: int = 0
        self._consecutive_close_fails: int = 0
        self._consecutive_pnl_deviations: int = 0
        self._circuit_breaker_until: float = 0.0
        self.cumulative_net_bps: float = 0.0
        self.cumulative_dollar_pnl: float = 0.0
        self._trade_seq: int = 0

        # ── V23: Balance-based P&L tracking ──
        self._initial_balance_l: Optional[Decimal] = None
        self._initial_balance_x: Optional[Decimal] = None
        self._pre_trade_balance_l: Optional[Decimal] = None
        self._pre_trade_balance_x: Optional[Decimal] = None
        self.cumulative_real_pnl: float = 0.0

        # ── V32: Ladder level (开仓+1, 逐层平仓-1, 全平归零) ──
        self._ladder_level: int = 0

        # ── V35: Bucketed latency pool (side x spread x vol, three-level fallback) ──
        self.latency_pool = BucketedLatencyPool(
            cold_default=latency_cold, window=150)

        # ── V34: EWMA short-term volatility (bps) ──
        self._short_vol_bps: float = 0.0
        self._last_mid: Optional[Decimal] = None

        # ── V34.1: Shadow learning snapshots (passive latency decay estimation) ──
        self._shadow_snapshots: collections.deque = collections.deque(maxlen=300)
        _SHADOW_DELAY = 0.3  # 300ms — matches Lighter execution latency
        self._shadow_delay: float = _SHADOW_DELAY
        self._shadow_min_spread: float = 1.0  # only learn from spreads >= 1bps

        # ── V34: Spread persistence filter (with direction memory) ──
        self._spread_above_count: int = 0
        self._spread_above_since: Optional[float] = None
        self._persist_side: Optional[str] = None

        # ── V34: Per-trade signal quality snapshot ──
        self._signal_observed_spread: float = 0.0
        self._signal_executable_spread: float = 0.0
        self._signal_latency_penalty: float = 0.0
        self._signal_vol_bps: float = 0.0
        self._signal_effective_bps: float = 0.0
        self._signal_hedge_mode: str = ""
        self._signal_s_bucket: str = ""
        self._signal_v_bucket: str = ""
        self._signal_d_bucket: str = ""
        self._signal_t_bucket: str = ""

        # ── Logging / CSV ──
        os.makedirs("logs", exist_ok=True)
        self.csv_path = f"logs/hotstuff_lighter_v36_{self.symbol}_trades.csv"
        self.log_path = f"logs/hotstuff_lighter_v36_{self.symbol}.log"
        self._init_csv()
        self.logger = self._init_logger()

    # ─────────────────────────────────────────────
    #  Logging / CSV
    # ─────────────────────────────────────────────

    def _init_logger(self) -> logging.Logger:
        lg = logging.getLogger(f"hs_l_v36_{self.symbol}")
        lg.setLevel(logging.INFO)
        lg.handlers.clear()

        class _Fmt(logging.Formatter):
            def formatTime(self, record, datefmt=None):
                ct = datetime.fromtimestamp(record.created, tz=_TZ_CN)
                return ct.strftime("%Y-%m-%d %H:%M:%S") + f",{int(record.msecs):03d}"

        fmt = _Fmt("%(asctime)s %(levelname)s %(message)s")
        fh = logging.FileHandler(self.log_path, encoding="utf-8")
        fh.setFormatter(fmt)
        lg.addHandler(fh)
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        lg.addHandler(ch)
        return lg

    def _init_csv(self):
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp", "trade_id", "side",
                    "lighter_price", "hotstuff_price", "qty",
                    "spread_bps", "fee_bps", "net_bps",
                    "hedge_ms", "cumulative_bps",
                    "real_pnl", "cumulative_real_pnl",
                    "balance_lighter", "balance_hotstuff",
                    "observed_spread", "executable_spread",
                    "latency_penalty", "vol_bps",
                    "effective_threshold", "hedge_mode",
                    # V35 bucket observation tags
                    "spread_bucket", "vol_bucket",
                    "depth_bucket", "session_bucket",
                ])

    def _csv_row(self, row: list):
        with open(self.csv_path, "a", newline="") as f:
            csv.writer(f).writerow(row)

    # ─────────────────────────────────────────────
    #  Infrastructure init
    # ─────────────────────────────────────────────

    def _init_lighter(self):
        api_key = os.getenv("API_KEY_PRIVATE_KEY")
        if not api_key:
            raise ValueError("API_KEY_PRIVATE_KEY env var not set")
        self.lighter_client = SignerClient(
            url=LIGHTER_REST_URL,
            account_index=self.account_index,
            api_private_keys={self.api_key_index: api_key},
        )
        err = self.lighter_client.check_client()
        if err is not None:
            raise ValueError(f"Lighter check_client failed: {err}")
        self.logger.info("Lighter SignerClient 已初始化")

    def _get_lighter_market_config(self):
        import requests
        ticker_map = {0: "ETH", 1: "BTC", 2: "SOL", 3: "DOGE"}
        ticker = ticker_map.get(self.lighter_market_index, self.symbol)
        url = f"{LIGHTER_REST_URL}/api/v1/orderBooks"
        r = requests.get(url, headers={"accept": "application/json"}, timeout=10)
        r.raise_for_status()
        data = r.json()
        for mkt in data.get("order_books", []):
            if mkt.get("symbol") == ticker:
                sd = int(mkt.get("supported_size_decimals", 5))
                pd_ = int(mkt.get("supported_price_decimals", 1))
                self.base_amount_multiplier = 10 ** sd
                self.price_multiplier = 10 ** pd_
                self.lighter_tick = Decimal("1") / (Decimal("10") ** pd_)
                self.lighter_market_index = int(mkt.get("market_id", self.lighter_market_index))
                self.logger.info(
                    f"Lighter market: idx={self.lighter_market_index} "
                    f"size_dec={sd} price_dec={pd_} tick={self.lighter_tick}")
                return
        raise ValueError(f"Lighter ticker {ticker} not found")

    def _init_hotstuff(self):
        pk = os.getenv("HOTSTUFF_PRIVATE_KEY")
        if not pk:
            raise ValueError("HOTSTUFF_PRIVATE_KEY not set")
        wallet = Account.from_key(pk)
        api_wallet_addr = wallet.address
        self.hs_address = os.getenv("HOTSTUFF_ADDRESS", api_wallet_addr)
        self.hs_exchange = ExchangeClient(wallet=wallet)
        self.hs_info = InfoClient()
        self.hs_symbol = f"{self.symbol}-PERP"
        is_agent = self.hs_address.lower() != api_wallet_addr.lower()
        self.logger.info(
            f"HotStuff client 已初始化  "
            f"{'agent模式' if is_agent else 'direct模式'}"
            f"  main={self.hs_address[:10]}…  api={api_wallet_addr[:10]}…")

    def _get_hotstuff_instrument_info(self):
        from hotstuff.methods.info.market import InstrumentsParams
        resp = self.hs_info.instruments(InstrumentsParams(type="perps"))
        perps = resp.perps if hasattr(resp, "perps") else resp.get("perps", [])
        for p in perps:
            name = p.name if hasattr(p, "name") else p.get("name", "")
            if name == self.hs_symbol:
                pid = p.id if hasattr(p, "id") else p.get("id", 0)
                tick = p.tick_size if hasattr(p, "tick_size") else p.get("tick_size", 0.01)
                lot = p.lot_size if hasattr(p, "lot_size") else p.get("lot_size", 0.001)
                max_lev = p.max_leverage if hasattr(p, "max_leverage") else p.get("max_leverage", 0)
                self.hs_instrument_id = int(pid)
                self.hs_tick_size = Decimal(str(tick))
                self.hs_lot_size = Decimal(str(lot))
                self.logger.info(
                    f"HotStuff instrument: {self.hs_symbol} id={self.hs_instrument_id} "
                    f"tick={self.hs_tick_size} lot={self.hs_lot_size} maxLev={max_lev}")
                return
        raise RuntimeError(f"{self.hs_symbol} not found on HotStuff")

    def _round_lighter(self, price: Decimal) -> Decimal:
        if self.lighter_tick > 0:
            return (price / self.lighter_tick).quantize(Decimal("1")) * self.lighter_tick
        return price

    # ─────────────────────────────────────────────
    #  HotStuff BBO helper
    # ─────────────────────────────────────────────

    def _refresh_hs_bbo(self):
        """Pull latest BBO from HotstuffFeed into local cache."""
        if self.hs_feed is None:
            return
        snap = self.hs_feed.snapshot
        if snap and snap.connected and snap.best_bid > 0 and snap.best_ask > 0:
            self._hs_best_bid = Decimal(str(snap.best_bid))
            self._hs_best_ask = Decimal(str(snap.best_ask))
            self._hs_bbo_ts = snap.timestamp
            if snap.bids:
                self._hs_l1_bid_size = snap.bids[0].size
            if snap.asks:
                self._hs_l1_ask_size = snap.asks[0].size

    def _get_hs_bbo(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        self._refresh_hs_bbo()
        return self._hs_best_bid, self._hs_best_ask

    # ─────────────────────────────────────────────
    #  V34: Lighter VWAP (uses full orderbook depth already in memory)
    # ─────────────────────────────────────────────

    def _calc_lighter_vwap(self, side: str, qty: Decimal) -> Optional[Decimal]:
        """Walk the in-memory Lighter orderbook to compute size-weighted avg price.

        For side="sell" (we want to sell on Lighter): walk bids top-down.
        For side="buy"  (we want to buy on Lighter):  walk asks bottom-up.
        Returns None if insufficient depth.
        """
        if side == "sell":
            book = self._l_ob.get("bids", {})
            if not book:
                return self._l_best_bid
            levels = sorted(book.items(), key=lambda x: -x[0])  # highest first
        else:
            book = self._l_ob.get("asks", {})
            if not book:
                return self._l_best_ask
            levels = sorted(book.items(), key=lambda x: x[0])   # lowest first

        remaining = float(qty)
        total_value = 0.0
        total_filled = 0.0

        for price, size in levels:
            if remaining <= 0:
                break
            fill = min(size, remaining)
            total_value += price * fill
            total_filled += fill
            remaining -= fill

        if total_filled <= 0:
            return self._l_best_bid if side == "sell" else self._l_best_ask
        return Decimal(str(total_value / total_filled))

    def _calc_lighter_depth_at_bbo(self, side: str, levels: int = 5) -> float:
        """Sum quantity of the top N price levels on one side (for logging)."""
        if side == "sell":
            book = self._l_ob.get("bids", {})
            sorted_levels = sorted(book.items(), key=lambda x: -x[0])
        else:
            book = self._l_ob.get("asks", {})
            sorted_levels = sorted(book.items(), key=lambda x: x[0])
        return sum(sz for _, sz in sorted_levels[:levels])

    def _estimate_lighter_slip(self, side: str, qty: Decimal) -> float:
        """Estimated slippage in bps: difference between BBO and VWAP for qty."""
        vwap = self._calc_lighter_vwap(side, qty)
        if vwap is None:
            return 0.0
        if side == "sell":
            bbo = self._l_best_bid
            if bbo is None or bbo <= 0:
                return 0.0
            return float((bbo - vwap) / bbo * 10000)
        else:
            bbo = self._l_best_ask
            if bbo is None or bbo <= 0:
                return 0.0
            return float((vwap - bbo) / bbo * 10000)

    # ─────────────────────────────────────────────
    #  V35.2: Close depth check
    # ─────────────────────────────────────────────
    def _check_lighter_close_depth(
        self, side: str, qty: Decimal, max_levels: int = 2
    ) -> tuple:
        """Check if Lighter top N levels have enough depth for close qty.

        Returns (sufficient, available_qty, est_slip_bps).
        """
        if side == "sell":
            book = self._l_ob.get("bids", {})
            levels = sorted(book.items(), key=lambda x: -x[0])[:max_levels]
        else:
            book = self._l_ob.get("asks", {})
            levels = sorted(book.items(), key=lambda x: x[0])[:max_levels]

        available = sum(sz for _, sz in levels)
        qty_f = float(qty)

        slip_bps = 0.0
        if levels and available > 0 and qty_f > 0:
            filled = 0.0
            cost = 0.0
            for price, sz in levels:
                take = min(sz, qty_f - filled)
                cost += price * take
                filled += take
                if filled >= qty_f:
                    break
            vwap = cost / filled if filled > 0 else levels[0][0]
            bbo = levels[0][0]
            if side == "sell":
                slip_bps = (bbo - vwap) / bbo * 10000 if bbo > 0 else 0.0
            else:
                slip_bps = (vwap - bbo) / bbo * 10000 if bbo > 0 else 0.0

        return available >= qty_f, available, slip_bps

    def _check_lighter_open_depth(
        self, side: str, qty: Decimal, max_levels: int = 2
    ) -> tuple:
        """V36: Check if Lighter top N levels have enough depth for taker open.

        For buying: we eat asks.  For selling: we eat bids.
        Returns (sufficient, available_qty, est_slip_bps, vwap_price).
        """
        if side == "buy":
            book = self._l_ob.get("asks", {})
            levels = sorted(book.items(), key=lambda x: x[0])[:max_levels]
        else:
            book = self._l_ob.get("bids", {})
            levels = sorted(book.items(), key=lambda x: -x[0])[:max_levels]

        available = sum(sz for _, sz in levels)
        qty_f = float(qty)

        slip_bps = 0.0
        vwap_price = Decimal("0")
        if levels and available > 0 and qty_f > 0:
            filled = 0.0
            cost = 0.0
            for price, sz in levels:
                take = min(sz, qty_f - filled)
                cost += price * take
                filled += take
                if filled >= qty_f:
                    break
            vwap = cost / filled if filled > 0 else levels[0][0]
            bbo = levels[0][0]
            vwap_price = Decimal(str(round(vwap, 8)))
            if side == "buy":
                slip_bps = (vwap - bbo) / bbo * 10000 if bbo > 0 else 0.0
            else:
                slip_bps = (bbo - vwap) / bbo * 10000 if bbo > 0 else 0.0

        return available >= qty_f, available, slip_bps, vwap_price

    # ─────────────────────────────────────────────
    #  V34: Dynamic entry threshold
    # ─────────────────────────────────────────────

    def _estimate_hotstuff_slip(self, side: str, qty: Decimal) -> float:
        """Estimated HotStuff taker slippage (bps).

        HotstuffFeed only provides L1, so we return a conservative constant.
        """
        return 0.5

    def _calc_dynamic_threshold(self, side: str, qty: Decimal,
                               current_spread_raw: float = 0.0) -> float:
        """V35.1: Decoupled entry economics — opening leg cost + expected close.

        Instead of requiring the signal to cover the entire round-trip worst-case
        cost (fee*2 + close_target + latency + ...), V35.1 uses:
          open_fee(1 leg) + expected_close_cost + capped_latency + slip + vol + edge

        The closing strategy uses depth-checked Taker IOC (same pattern as
        opening), eliminating the multi-tier degradation chain.

        Returns max(static_floor, dynamic_cost) so that --open-spread always
        acts as a hard lower bound while the cost model can raise it higher.
        """
        # ── static floor (user's hard minimum) ──
        base_floor = (self.open_spread_bps
                      + self._ladder_level * self.ladder_step_bps
                      + self.spread_buffer_bps)

        # ── V35.1: opening-leg fee only (HotStuff taker) ──
        open_fee = float(FEE_HOTSTUFF_TAKER) * 10000       # 2.5 bps

        # ── V35.1: expected close cost (replaces close_target + fee) ──
        exp_close = self.expected_close_cost                # default 0.8 bps

        # ── V35: bucketed latency with cap ──
        s_bkt = self._bucket_spread(current_spread_raw)
        v_bkt = self._bucket_vol(self._short_vol_bps)
        raw_lat, _ = self.latency_pool.get_latency(side, s_bkt, v_bkt)
        latency_cost = min(raw_lat, self.latency_cap)

        lighter_slip = max(self._estimate_lighter_slip(side, qty), 0.0)
        hs_slip = max(self._estimate_hotstuff_slip(
            "sell" if side == "buy" else "buy", qty), 0.0)

        vol_cost = self.vol_weight * self._short_vol_bps

        inv_ratio = float(abs(self.lighter_position)) / float(self.max_position) \
            if self.max_position > 0 else 0.0
        inv_cost = self.inv_weight * inv_ratio * 2.0

        dynamic_cost = (open_fee + exp_close + latency_cost
                        + lighter_slip + hs_slip + vol_cost + inv_cost
                        + self.edge_buffer
                        + self._ladder_level * self.ladder_step_bps)

        return max(base_floor, dynamic_cost)

    def _update_vol_tracker(self, l_bid: Decimal, l_ask: Decimal):
        """Update EWMA short-term volatility from Lighter mid price changes."""
        mid_now = (l_bid + l_ask) / 2
        if self._last_mid is not None and self._last_mid > 0:
            ret = abs(float(mid_now - self._last_mid) / float(self._last_mid)) * 10000
            alpha = 0.05  # ~20-sample half-life
            self._short_vol_bps = alpha * ret + (1.0 - alpha) * self._short_vol_bps
        self._last_mid = mid_now

    # ─────────────────────────────────────────────
    #  V35: Bucket helpers
    # ─────────────────────────────────────────────

    @staticmethod
    def _bucket_spread(obs_bps: float) -> str:
        a = abs(obs_bps)
        if a < 2.0:
            return "S1"
        elif a < 4.0:
            return "S2"
        return "S3"

    @staticmethod
    def _bucket_vol(vol_bps: float) -> str:
        if vol_bps < 0.15:
            return "V1"
        elif vol_bps < 0.50:
            return "V2"
        return "V3"

    def _bucket_depth(self, side: str, qty: Decimal) -> str:
        depth5 = self._calc_lighter_depth_at_bbo(side, levels=5)
        ratio = depth5 / max(float(qty), 1e-9)
        if ratio < 3:
            return "D1"
        elif ratio < 10:
            return "D2"
        return "D3"

    @staticmethod
    def _bucket_session(hour: int) -> str:
        if hour < 8:
            return "T1"
        elif hour < 16:
            return "T2"
        return "T3"

    # ─────────────────────────────────────────────
    #  V34.1: Shadow learning — passive latency decay estimation
    # ─────────────────────────────────────────────

    def _shadow_snapshot(self, now: float,
                         sell_spread_raw: float, buy_spread_raw: float):
        """Record current BBO spreads as shadow observation points."""
        if sell_spread_raw >= self._shadow_min_spread:
            self._shadow_snapshots.append((now, "sell", sell_spread_raw))
        if buy_spread_raw >= self._shadow_min_spread:
            self._shadow_snapshots.append((now, "buy", buy_spread_raw))

    def _process_shadow_learning(self, now: float,
                                 l_bid: Decimal, l_ask: Decimal,
                                 hs_bid: Decimal, hs_ask: Decimal):
        """Consume matured shadow snapshots (>= 300ms old) and feed decay into trackers.

        This allows the latency tracker to warm up from passive market observation
        without requiring actual trades, solving the cold-start deadlock.
        """
        while self._shadow_snapshots:
            ts, side, obs_bps = self._shadow_snapshots[0]
            if now - ts < self._shadow_delay:
                break
            self._shadow_snapshots.popleft()

            if hs_ask <= 0 or l_ask <= 0:
                continue

            if side == "sell":
                current_bps = float((l_bid - hs_ask) / hs_ask * 10000)
            else:
                current_bps = float((hs_bid - l_ask) / l_ask * 10000)

            s_bkt = self._bucket_spread(obs_bps)
            v_bkt = self._bucket_vol(self._short_vol_bps)

            base_was_cold = not self.latency_pool.base[side].warm
            self.latency_pool.record(side, obs_bps, current_bps, s_bkt, v_bkt)

            if base_was_cold and self.latency_pool.base[side].just_warmed:
                self.logger.info(
                    f"[Shadow] {side} base tracker已热启动: "
                    f"{self.latency_pool.base[side].summary()}")

            full_key = (side, s_bkt, v_bkt)
            bkt_t = self.latency_pool._bucketed.get(full_key)
            if bkt_t and bkt_t.just_warmed:
                self.logger.info(
                    f"[Shadow] {side}/{s_bkt}/{v_bkt} 分桶tracker已热启动: "
                    f"{bkt_t.summary()}")

    # ─────────────────────────────────────────────
    #  Lighter WS: order book + account orders
    # ─────────────────────────────────────────────

    async def _lighter_ws_keepalive(self, ws):
        """每 15 秒发送 WS 协议级 ping + 应用级 ping，防止 NAT/LB 超时断连。"""
        try:
            while not self.stop_flag:
                await asyncio.sleep(15)
                try:
                    pong = await ws.ping()
                    await asyncio.wait_for(pong, timeout=5)
                except Exception:
                    self.logger.warning("[Lighter WS] keepalive ping 无响应, 强制关闭连接")
                    await ws.close()
                    return
        except asyncio.CancelledError:
            return

    async def _lighter_ws_loop(self):
        """Single WS connection for both order book and account fills."""
        while not self.stop_flag:
            keepalive_task = None
            try:
                async with websockets.connect(
                    LIGHTER_WS_URL,
                    ping_interval=10,
                    ping_timeout=5,
                    close_timeout=5,
                ) as ws:
                    keepalive_task = asyncio.create_task(self._lighter_ws_keepalive(ws))

                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": f"order_book/{self.lighter_market_index}",
                    }))

                    if not self.dry_run:
                        try:
                            auth, err = self.lighter_client.create_auth_token_with_expiry(
                                api_key_index=self.api_key_index)
                            if err is None:
                                await ws.send(json.dumps({
                                    "type": "subscribe",
                                    "channel": f"account_orders/{self.lighter_market_index}/{self.account_index}",
                                    "auth": auth,
                                }))
                                self.logger.info("[Lighter WS] 已订阅 order_book + account_orders")
                            else:
                                self.logger.warning(f"[Lighter WS] auth失败: {err}")
                        except Exception as e:
                            self.logger.warning(f"[Lighter WS] 订阅account_orders出错: {e}")

                    while not self.stop_flag:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            continue
                        data = json.loads(raw)
                        msg_type = data.get("type", "")
                        self._l_ws_ts = time.time()

                        if msg_type == "update/account_orders":
                            orders = data.get("orders", {}).get(
                                str(self.lighter_market_index), [])
                            for od in orders:
                                self._handle_lighter_order_event(od)
                            continue

                        if msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                            continue

                        if msg_type == "subscribed/order_book":
                            ob = data.get("order_book", {})
                            self._l_ob["bids"].clear()
                            self._l_ob["asks"].clear()
                            self._apply_ob_side("bids", ob.get("bids", []))
                            self._apply_ob_side("asks", ob.get("asks", []))
                            self._l_ob_ready = True
                            self._refresh_l_bbo()
                            self.logger.info(
                                f"[Lighter] 订单簿快照: "
                                f"{len(self._l_ob['bids'])}买 "
                                f"{len(self._l_ob['asks'])}卖 "
                                f"BBO={self._l_best_bid}/{self._l_best_ask}")
                            continue

                        if msg_type == "update/order_book":
                            ob = data.get("order_book", data)
                            self._apply_ob_side("bids", ob.get("bids", []))
                            self._apply_ob_side("asks", ob.get("asks", []))
                            self._refresh_l_bbo()
                            continue

            except asyncio.CancelledError:
                return
            except Exception as e:
                self.logger.warning(f"[Lighter WS] 断连: {e}")
                await asyncio.sleep(2)
            finally:
                if keepalive_task and not keepalive_task.done():
                    keepalive_task.cancel()

    def _apply_ob_side(self, side: str, levels: list):
        book = self._l_ob[side]
        for lv in levels:
            try:
                p = float(lv["price"])
                q = float(lv["size"])
                if q <= 0:
                    book.pop(p, None)
                else:
                    book[p] = q
            except (KeyError, ValueError, TypeError):
                pass

    def _refresh_l_bbo(self):
        bids = self._l_ob["bids"]
        asks = self._l_ob["asks"]
        self._l_best_bid = Decimal(str(max(bids.keys()))) if bids else None
        self._l_best_ask = Decimal(str(min(asks.keys()))) if asks else None

    # ── Lighter order event handler ──

    def _handle_lighter_order_event(self, od: dict):
        status = od.get("status", "")
        if status == "filled":
            self._on_lighter_fill(od)
        elif status == "open":
            self._on_lighter_open(od)
        elif status == "canceled":
            filled_base = Decimal(od.get("filled_base_amount", "0"))
            if filled_base > 0:
                self._on_lighter_fill(od)
        elif status == "partially_filled":
            self.logger.info(
                f"[Lighter] 部分成交(等最终确认): "
                f"{od.get('filled_base_amount','?')} @ {od.get('filled_quote_amount','?')}")

    def _on_lighter_fill(self, od: dict):
        filled_base = Decimal(od.get("filled_base_amount", "0"))
        filled_quote = Decimal(od.get("filled_quote_amount", "0"))
        avg_price = filled_quote / filled_base if filled_base > 0 else Decimal("0")
        is_ask = od.get("is_ask", False)
        cid = od.get("client_order_id")
        oidx = od.get("order_index")
        side = "sell" if is_ask else "buy"

        # BUG-2 fix: WS 重连去重 — 用 (order_index, filled_base) 防止重复计仓
        _dedup_key = (oidx, str(filled_base))
        if _dedup_key in self._processed_fill_keys:
            self.logger.info(
                f"[Lighter] 去重跳过: {side} {filled_base} @ {avg_price} (oidx={oidx})")
            return
        self._processed_fill_keys.add(_dedup_key)
        if len(self._processed_fill_keys) > 500:
            _to_remove = list(self._processed_fill_keys)[:250]
            for k in _to_remove:
                self._processed_fill_keys.discard(k)

        # BUG-1 fix: 识别对账修复单 — 跳过仓位更新和 stale 追加
        _cid_int = int(cid) if cid is not None else None
        if _cid_int is not None and _cid_int in self._repair_ioc_cids:
            self._repair_ioc_cids.discard(_cid_int)
            self.logger.info(
                f"[Lighter] 对账修复单成交(仓位已预更新, 忽略WS): "
                f"{side} {filled_base} @ {avg_price} (cid={cid})")
            return

        is_our_buy = (cid is not None and self._l_buy_client_id is not None
                      and _cid_int == self._l_buy_client_id)
        is_our_sell = (cid is not None and self._l_sell_client_id is not None
                       and _cid_int == self._l_sell_client_id)

        if (is_our_buy or is_our_sell) and not self._fill_event.is_set():
            if is_ask:
                self.lighter_position -= filled_base
            else:
                self.lighter_position += filled_base
            self._fill_side = side
            self._fill_price = avg_price
            self._fill_qty = filled_base

            self._fill_hs_bid, self._fill_hs_ask = self._get_hs_bbo()
            self._fill_ts = time.time()
            self._instant_hedge_done.clear()
            self._instant_hedge_ok = False
            self._instant_hedge_fill_price = None
            self._instant_hedge_task = asyncio.create_task(
                self._instant_hedge_fire())

            self._fill_event.set()
            self.logger.info(
                f"[Lighter] 成交: {side} {filled_base} @ {avg_price}  "
                f"HS_snap={self._fill_hs_bid}/{self._fill_hs_ask} → 即时对冲已触发")

        elif (is_our_buy or is_our_sell) and self._fill_event.is_set():
            # BUG-5 fix: 追加成交记录增量，HEDGING 中补充对冲
            _increment = filled_base
            if is_ask:
                self.lighter_position -= filled_base
            else:
                self.lighter_position += filled_base
            old_qty = self._fill_qty
            new_total = old_qty + filled_base
            self._fill_price = (self._fill_price * old_qty + avg_price * filled_base) / new_total
            self._fill_qty = new_total
            self._fill_qty_pending = getattr(self, '_fill_qty_pending', Decimal("0")) + _increment
            self.logger.warning(
                f"[Lighter] 同一订单追加成交: {side} +{filled_base} @ {avg_price}, "
                f"累计={new_total}, 待补对冲={self._fill_qty_pending} (已更新仓位)")

        elif self._in_flatten:
            _is_flatten_order = (cid is not None and self._flatten_ioc_cid is not None
                                 and _cid_int == self._flatten_ioc_cid)
            if not _is_flatten_order:
                self.logger.warning(
                    f"[Lighter] 平仓期间收到非平仓订单成交! cid={cid} (平仓cid={self._flatten_ioc_cid})")
            _prev_qty = self._flatten_fill_qty
            _new_qty = _prev_qty + filled_base
            if _prev_qty > 0 and self._flatten_fill_price and self._flatten_fill_price > 0:
                self._flatten_fill_price = (
                    self._flatten_fill_price * _prev_qty + avg_price * filled_base
                ) / _new_qty
            else:
                self._flatten_fill_price = avg_price
            self._flatten_fill_qty = _new_qty
            self._flatten_fill_event.set()
            self.logger.info(
                f"[Lighter] 平仓成交: {side} +{filled_base} @ {avg_price} "
                f"(累计={_new_qty}, 均价={self._flatten_fill_price:.1f})")
        else:
            _is_late_flatten = (cid is not None and self._flatten_ioc_cid is not None
                                and _cid_int == self._flatten_ioc_cid)
            if _is_late_flatten:
                self.logger.info(
                    f"[Lighter] 平仓IOC延迟WS确认(仓位已预更新, 忽略): "
                    f"{side} {filled_base} @ {avg_price} (cid={cid})")
            else:
                if is_ask:
                    self.lighter_position -= filled_base
                else:
                    self.lighter_position += filled_base
                self._stale_fills.append({
                    "side": side, "size": filled_base, "price": avg_price})
                self.logger.warning(
                    f"[Lighter] 非预期成交(已更新仓位, 等待对账修复): "
                    f"{side} {filled_base} @ {avg_price} (cid={cid})")

    def _on_lighter_open(self, od: dict):
        cid = od.get("client_order_id")
        oidx = od.get("order_index")
        if cid is not None:
            cid_int = int(cid)
            if self._l_buy_client_id is not None and cid_int == self._l_buy_client_id:
                self._l_buy_order_idx = oidx
            elif self._l_sell_client_id is not None and cid_int == self._l_sell_client_id:
                self._l_sell_order_idx = oidx

    # ─────────────────────────────────────────────
    #  Lighter API rate limiter
    # ─────────────────────────────────────────────

    async def _lighter_rate_wait(self):
        """滑动窗口限速: 60秒内最多 LIGHTER_RATE_LIMIT 次请求。超限则等待。"""
        now = time.time()
        cutoff = now - LIGHTER_RATE_WINDOW
        while self._lighter_req_times and self._lighter_req_times[0] < cutoff:
            self._lighter_req_times.popleft()
        if len(self._lighter_req_times) >= LIGHTER_RATE_LIMIT:
            wait = self._lighter_req_times[0] + LIGHTER_RATE_WINDOW - now + 0.1
            if wait > 0:
                self.logger.warning(f"[Lighter] 限速等待 {wait:.1f}s (已{len(self._lighter_req_times)}次/60s)")
                await asyncio.sleep(wait)
        self._lighter_req_times.append(time.time())

    # ─────────────────────────────────────────────
    #  Lighter order placement / modify / cancel
    # ─────────────────────────────────────────────

    async def _place_lighter_order(self, side: str, qty: Decimal, price: Decimal) -> Optional[int]:
        is_ask = side.lower() == "sell"
        coi = int(time.time() * 1000) * 10 + (1 if is_ask else 2)
        await self._lighter_rate_wait()
        try:
            _, _, error = await self.lighter_client.create_order(
                market_index=self.lighter_market_index,
                client_order_index=coi,
                base_amount=int(qty * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=False,
                trigger_price=0,
            )
            if error:
                self.logger.error(f"[Lighter] 下单失败: {error}")
                return None
            self.logger.info(f"[Lighter] 挂单: {side} {qty} @ {price} (cid={coi})")
            return coi
        except Exception as e:
            self.logger.error(f"[Lighter] 下单异常: {e}")
            return None

    async def _modify_lighter_order(self, order_idx: int, new_price: Decimal, qty: Decimal) -> bool:
        """原地修改挂单价格。遇nonce冲突自动重试3次。"""
        rounded = self._round_lighter(new_price)
        for attempt in range(3):
            await self._lighter_rate_wait()
            try:
                _, _, error = await self.lighter_client.modify_order(
                    market_index=self.lighter_market_index,
                    order_index=order_idx,
                    base_amount=int(qty * self.base_amount_multiplier),
                    price=int(rounded * self.price_multiplier),
                    trigger_price=0,
                )
                if error:
                    err_str = str(error)
                    if "invalid nonce" in err_str.lower() or "21104" in err_str:
                        self.logger.warning(
                            f"[Lighter] modify nonce冲突(第{attempt+1}次), 等待重试...")
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    self.logger.warning(f"[Lighter] modify失败: {error}")
                    return False
                return True
            except Exception as e:
                err_str = str(e)
                if ("429" in err_str or "Too Many" in err_str) and attempt < 2:
                    self.logger.warning(f"[Lighter] modify 429限速, 等待3s重试...")
                    await asyncio.sleep(3.0)
                    continue
                self.logger.warning(f"[Lighter] modify异常: {e}")
                return False
        self.logger.warning(f"[Lighter] modify 3次重试全部失败 idx={order_idx}")
        return False

    async def _cancel_lighter_order(self, order_idx: Optional[int]):
        if order_idx is None:
            return
        for attempt in range(3):
            await self._lighter_rate_wait()
            try:
                _, _, error = await self.lighter_client.cancel_order(
                    market_index=self.lighter_market_index,
                    order_index=order_idx,
                )
                if error:
                    err_str = str(error)
                    if "invalid nonce" in err_str.lower() or "21104" in err_str:
                        self.logger.warning(
                            f"[Lighter] 撤单nonce冲突(第{attempt+1}次), 等待重试...")
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    self.logger.warning(f"[Lighter] 撤单错误: {error}")
                return
            except Exception as e:
                self.logger.warning(f"[Lighter] 撤单异常: {e}")
                await asyncio.sleep(0.5)
        self.logger.error(f"[Lighter] 撤单3次失败 idx={order_idx}")

    async def _cancel_all_lighter(self):
        """撤掉所有挂单，失败自动重试3次（间隔1秒等nonce恢复）。"""
        for _attempt in range(3):
            await self._lighter_rate_wait()
            try:
                _, _, err = await self.lighter_client.cancel_all_orders(
                    time_in_force=self.lighter_client.CANCEL_ALL_TIF_IMMEDIATE,
                    timestamp_ms=0)
                if err:
                    self.logger.warning(
                        f"[Lighter] cancel_all第{_attempt+1}次失败: {err}")
                    await asyncio.sleep(1.0)
                    continue
                self.logger.info("[Lighter] cancel_all_orders 已执行")
                break
            except Exception as e:
                self.logger.warning(
                    f"[Lighter] cancel_all第{_attempt+1}次异常: {e}")
                await asyncio.sleep(1.0)

        self._l_buy_client_id = None
        self._l_sell_client_id = None
        self._l_buy_order_idx = None
        self._l_sell_order_idx = None

    async def _lighter_ioc(self, side: str, qty: Decimal, ref_price: Decimal) -> bool:
        """Lighter IOC order for fixing position imbalances (zero fee on Free account)."""
        slip = Decimal("0.01")
        if side == "buy":
            price = ref_price * (Decimal("1") + slip)
        else:
            price = ref_price * (Decimal("1") - slip)
        price = self._round_lighter(price)
        qty_r = qty.quantize(self.lighter_min_size_step, rounding=ROUND_HALF_UP)
        if qty_r <= 0:
            return False
        _cid = int(time.time() * 1000)
        self._repair_ioc_cids.add(_cid)
        await self._lighter_rate_wait()
        try:
            _, _, err = await self.lighter_client.create_order(
                market_index=self.lighter_market_index,
                client_order_index=_cid,
                base_amount=int(qty_r * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=(side == "sell"),
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                order_expiry=self.lighter_client.DEFAULT_IOC_EXPIRY,
                reduce_only=False,
                trigger_price=0,
            )
            if err:
                self.logger.warning(f"[对账] Lighter IOC {side} {qty_r} 失败: {err}")
                self._repair_ioc_cids.discard(_cid)
                return False
            if side == "buy":
                self.lighter_position += qty_r
            else:
                self.lighter_position -= qty_r
            self.logger.info(f"[对账] Lighter IOC {side} {qty_r} @ {price} 已修复 (cid={_cid})")
            return True
        except Exception as e:
            self.logger.warning(f"[对账] Lighter IOC异常: {e}")
            self._repair_ioc_cids.discard(_cid)
            return False

    # ─────────────────────────────────────────────
    #  HotStuff IOC hedge
    # ─────────────────────────────────────────────

    async def _hedge_hotstuff(self, side: str, qty: Decimal, ref_price: Decimal) -> Optional[Decimal]:
        """HotStuff IOC 对冲 (乐观填充 — HotStuff 无 WS 订单事件)。"""
        slip = Decimal("0.005")
        if side == "buy":
            price = ref_price * (Decimal("1") + slip)
        else:
            price = ref_price * (Decimal("1") - slip)
        price = price.quantize(self.hs_tick_size, rounding=ROUND_HALF_UP)
        qty_r = qty.quantize(self.hs_lot_size, rounding=ROUND_HALF_UP)

        if qty_r < self.hs_lot_size:
            self.logger.warning(f"[HotStuff] 数量{qty_r}低于最小值{self.hs_lot_size}, 跳过")
            return None

        hs_side = "b" if side == "buy" else "s"

        _t0 = time.time()
        try:
            order = UnitOrder(
                instrumentId=self.hs_instrument_id,
                side=hs_side,
                positionSide="BOTH",
                price=str(price),
                size=str(qty_r),
                tif="IOC",
                ro=False,
                po=False,
                isMarket=True,
            )
            params = PlaceOrderParams(
                orders=[order],
                expiresAfter=int(time.time() * 1000) + 60_000,
            )
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, self.hs_exchange.place_order, params)

            if isinstance(result, list):
                result = result[0] if result else {}
            if not isinstance(result, dict):
                result = {"raw": result}

            error = result.get("error", "")
            if error:
                self.logger.error(f"[HotStuff] IOC失败: {error}")
                return None
        except Exception as e:
            self.logger.error(f"[HotStuff] IOC异常: {e}")
            return None
        _t1 = time.time()

        if side == "buy":
            self.hotstuff_position += qty_r
        else:
            self.hotstuff_position -= qty_r

        _ms = (_t1 - _t0) * 1000
        self.logger.info(f"[HotStuff] IOC完成({_ms:.0f}ms) {side} {qty_r} ref={ref_price}")
        return ref_price

    async def _cancel_all_hotstuff_orders(self):
        """Cancel all open orders on HotStuff."""
        if self.dry_run or not self.hs_exchange:
            return
        try:
            params = CancelAllParams(
                expiresAfter=int(time.time() * 1000) + 60_000,
            )
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, self.hs_exchange.cancel_all, params)
            if isinstance(result, list):
                result = result[0] if result else {}
            if not isinstance(result, dict):
                result = {}
            cancelled = (result.get("data", {}) or {}).get("orders_cancelled", 0)
            if cancelled:
                self.logger.info(f"[HotStuff] 撤销 {cancelled} 挂单")
        except Exception as e:
            self.logger.warning(f"[HotStuff] cancel_all 失败: {e}")

    async def _instant_hedge_fire(self):
        """V22: 从WS回调触发的即时对冲, 跳过主循环延迟。含snapshot spread guard。"""
        try:
            _side = self._fill_side
            _qty = self._fill_qty
            _l_price = self._fill_price
            _hs_bid = self._fill_hs_bid
            _hs_ask = self._fill_hs_ask

            if _side is None or _qty <= 0:
                return

            _hedge_side = "sell" if _side == "buy" else "buy"
            _ref = _hs_bid if _hedge_side == "sell" else _hs_ask

            if _ref is None or _ref <= 0:
                _ref = _l_price

            if _side == "sell" and _hs_ask and _hs_ask > 0:
                _snap_spread = float((_l_price - _hs_ask) / _hs_ask * 10000)
            elif _side == "buy" and _hs_bid and _hs_bid > 0:
                _snap_spread = float((_hs_bid - _l_price) / _l_price * 10000)
            else:
                _snap_spread = 0

            _t0 = time.time()
            _fill_price = await self._hedge_hotstuff(_hedge_side, _qty, _ref)
            _ms = (time.time() - _t0) * 1000

            self._instant_hedge_ms = _ms
            if _fill_price is not None:
                self._instant_hedge_ok = True
                self._instant_hedge_fill_price = _fill_price
                self.logger.info(
                    f"[即时对冲] 成功! {_hedge_side}@{_fill_price} "
                    f"snap_spread={_snap_spread:.1f}bps {_ms:.0f}ms")
            else:
                self._instant_hedge_ok = False

            self._instant_hedge_done.set()
        except Exception as e:
            self.logger.error(f"[即时对冲] 异常: {e}\n{traceback.format_exc()}")
            self._instant_hedge_ok = False
            self._instant_hedge_done.set()

    # ─────────────────────────────────────────────
    #  Close: PnL recording + ladder
    # ─────────────────────────────────────────────

    async def _close_record_and_ladder(
        self, l_side: str, hs_side: str,
        l_actual: Decimal, hs_fill: Decimal,
        actual_qty: Decimal, hedge_ms: float,
        mode_label: str,
    ) -> Tuple[bool, int]:
        """Shared PnL recording + ladder adjustment for taker close."""
        _hs_price = Decimal(str(hs_fill))
        if l_side == "buy":
            _spread_bps = float((l_actual - _hs_price) / _hs_price * 10000)
        else:
            _spread_bps = float((_hs_price - l_actual) / l_actual * 10000)
        _fee = float(FEE_HOTSTUFF_TAKER + FEE_LIGHTER_MAKER) * 10000
        _net = -_spread_bps - _fee
        self.cumulative_net_bps += _net
        _dollar = float(_net / 10000 * float(actual_qty) * float(_x_price))
        self.cumulative_dollar_pnl += _dollar

        self.logger.info(
            f"[平仓{mode_label}完成] L_{l_side}@{l_actual} HS_{hs_side}@{hs_fill}  "
            f"close_spread={_spread_bps:.2f}bps fee={_fee:.2f}bps "
            f"close_net={_net:.2f}bps  ${_dollar:+.4f}  hedge={hedge_ms:.0f}ms  "
            f"累积=${self.cumulative_dollar_pnl:+.4f}")

        _real_pnl_str, _cum_real_str, _bal_l_str, _bal_hs_str = \
            await self._v23_verify_close_pnl(
                _net, _dollar, actual_qty, _hs_price)

        self._trade_seq += 1
        self._csv_row([
            datetime.now(_TZ_CN).isoformat(),
            f"hs_l_v36_close_{self._trade_seq:04d}",
            f"close_{l_side}",
            str(l_actual), str(hs_fill), str(actual_qty),
            f"{_spread_bps:.2f}", f"{_fee:.2f}", f"{_net:.2f}",
            f"{hedge_ms:.0f}", f"{self.cumulative_net_bps:.2f}",
            _real_pnl_str, _cum_real_str, _bal_l_str, _bal_hs_str,
            f"{self._signal_observed_spread:.2f}",
            f"{self._signal_executable_spread:.2f}",
            f"{self._signal_latency_penalty:.2f}",
            f"{self._signal_vol_bps:.2f}",
            f"{self._signal_effective_bps:.2f}",
            self._signal_hedge_mode,
            self._signal_s_bucket, self._signal_v_bucket,
            self._signal_d_bucket, self._signal_t_bucket,
        ])
        self._consecutive_close_fails = 0

        _closed_layers = max(1, int((actual_qty / self.order_size).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP)))
        if self.ladder_step_bps > 0 and self._ladder_level > 0:
            _old = self._ladder_level
            self._ladder_level = max(0, self._ladder_level - _closed_layers)
            self.logger.info(
                f"[阶梯] {mode_label}平{_closed_layers}层, L{_old} → L{self._ladder_level}")

        _still = (abs(self.lighter_position) >= self.hs_lot_size
                  and abs(self.hotstuff_position) >= self.hs_lot_size)
        if not _still and self._ladder_level > 0:
            self.logger.info(f"[阶梯] 全部平完, L{self._ladder_level} → L0")
            self._ladder_level = 0

        return True, _closed_layers

    async def _close_position_taker(
        self, l_bid: Decimal, l_ask: Decimal,
        hs_bid: Decimal, hs_ask: Decimal,
        batch_qty: Decimal,
    ) -> Tuple[bool, int]:
        """V36 unified close: depth-checked Taker IOC on Lighter + hedge HotStuff.
        Falls back to _emergency_flatten() on failure."""
        self._in_flatten = True
        if (self._l_buy_client_id or self._l_sell_client_id
                or self._l_buy_order_idx or self._l_sell_order_idx):
            await self._cancel_all_lighter()

        l_qty = abs(self.lighter_position)
        hs_qty = abs(self.hotstuff_position)
        close_qty = min(l_qty, hs_qty, batch_qty)
        close_qty = close_qty.quantize(self.hs_lot_size, rounding=ROUND_DOWN)
        if close_qty < self.hs_lot_size or close_qty <= 0:
            self._in_flatten = False
            return False, 0

        if self.lighter_position < 0:
            l_side = "buy"
            hs_side = "sell"
            l_ref = l_ask
        else:
            l_side = "sell"
            hs_side = "buy"
            l_ref = l_bid

        _t0 = time.time()

        self._flatten_fill_event.clear()
        self._flatten_fill_price = None
        self._flatten_fill_qty = Decimal("0")
        _flatten_cid = int(time.time() * 1000) * 10 + 5
        self._flatten_ioc_cid = _flatten_cid

        slip = Decimal("0.01")
        if l_side == "buy":
            l_price = l_ref * (Decimal("1") + slip)
        else:
            l_price = l_ref * (Decimal("1") - slip)
        l_price = self._round_lighter(l_price)

        self.logger.info(
            f"[平仓Taker] L_{l_side} IOC {close_qty}@{l_price} (slip=1%)")

        await self._lighter_rate_wait()
        try:
            _, _, err = await self.lighter_client.create_order(
                market_index=self.lighter_market_index,
                client_order_index=_flatten_cid,
                base_amount=int(close_qty * self.base_amount_multiplier),
                price=int(l_price * self.price_multiplier),
                is_ask=(l_side == "sell"),
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                order_expiry=self.lighter_client.DEFAULT_IOC_EXPIRY,
                reduce_only=True,
                trigger_price=0,
            )
            if err:
                self.logger.error(f"[平仓Taker] Lighter IOC失败: {err} → _emergency_flatten")
                self._in_flatten = False
                await self._emergency_flatten()
                return False, 0
        except Exception as e:
            self.logger.error(f"[平仓Taker] Lighter异常: {e} → _emergency_flatten")
            self._in_flatten = False
            await self._emergency_flatten()
            return False, 0

        if l_side == "buy":
            self.lighter_position += close_qty
        else:
            self.lighter_position -= close_qty

        hs_bid, hs_ask = self._get_hs_bbo()
        hs_ref = hs_bid if hs_side == "sell" else hs_ask
        hs_fill = await self._hedge_hotstuff(hs_side, close_qty, hs_ref)
        if hs_fill is None:
            self.logger.error("[平仓Taker] HotStuff对冲失败! → _emergency_flatten")
            self._in_flatten = False
            await self._emergency_flatten()
            return False, 0
        _ms = (time.time() - _t0) * 1000

        _l_actual = l_ref
        if self._flatten_fill_price and self._flatten_fill_price > 0:
            _l_actual = self._flatten_fill_price

        self._in_flatten = False
        return await self._close_record_and_ladder(
            l_side, hs_side, _l_actual, hs_fill, close_qty, _ms, "taker")

    # ─────────────────────────────────────────────
    #  V23: Balance query helpers (余额校验)
    # ─────────────────────────────────────────────

    def _sync_get_lighter_balance(self) -> Optional[Decimal]:
        import requests as _req
        url = f"{LIGHTER_REST_URL}/api/v1/account"
        params = {"by": "index", "value": self.account_index}
        resp = _req.get(url, headers={"accept": "application/json"},
                        params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        acct = data.get("accounts", [{}])[0]
        for key in ("collateral_amount", "total_equity", "free_collateral",
                     "collateral", "balance", "equity", "account_value"):
            val = acct.get(key)
            if val is not None:
                return Decimal(str(val))
        self.logger.warning(
            f"[余额] Lighter account 无已知余额字段, keys={list(acct.keys())}")
        return None

    async def _get_lighter_balance(self) -> Optional[Decimal]:
        try:
            return await asyncio.to_thread(self._sync_get_lighter_balance)
        except Exception as e:
            self.logger.warning(f"[余额] Lighter余额查询失败: {e}")
            return None

    async def _get_hotstuff_balance(self) -> Optional[Decimal]:
        """HotStuff has no direct balance API; return None (use Lighter balance only)."""
        return None

    async def _snapshot_balances(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        l_bal = await self._get_lighter_balance()
        return l_bal, None

    # ─────────────────────────────────────────────
    #  Position helpers
    # ─────────────────────────────────────────────

    def _sync_get_lighter_position(self) -> Decimal:
        import requests as _req
        ticker_map = {0: "ETH", 1: "BTC", 2: "SOL", 3: "DOGE"}
        ticker = ticker_map.get(self.lighter_market_index, self.symbol)
        url = f"{LIGHTER_REST_URL}/api/v1/account"
        params = {"by": "index", "value": self.account_index}
        resp = _req.get(url, headers={"accept": "application/json"},
                        params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        for pos in data.get("accounts", [{}])[0].get("positions", []):
            if pos.get("symbol") == ticker:
                return Decimal(str(pos["position"])) * int(pos["sign"])
        return Decimal("0")

    async def _get_lighter_position(self) -> Decimal:
        try:
            return await asyncio.to_thread(self._sync_get_lighter_position)
        except Exception as e:
            self.logger.warning(f"获取Lighter仓位失败: {e}")
            return self.lighter_position

    async def _get_hotstuff_position(self) -> Decimal:
        try:
            loop = asyncio.get_event_loop()
            positions = await loop.run_in_executor(
                None,
                self.hs_info.positions,
                PositionsParams(user=self.hs_address),
            )
            if not positions:
                return Decimal("0")
            for p in positions:
                inst = p.get("instrument") if isinstance(p, dict) else getattr(p, "instrument", None)
                if inst == self.hs_symbol:
                    size_str = p.get("size") if isinstance(p, dict) else getattr(p, "size", "0")
                    return Decimal(str(size_str))
            return Decimal("0")
        except Exception as e:
            self.logger.warning(f"获取HotStuff仓位失败: {e}")
            return self.hotstuff_position

    async def _reconcile(self):
        if self._instant_hedge_task and not self._instant_hedge_task.done():
            self.logger.info("[对账] 等待即时对冲完成再对账...")
            try:
                await asyncio.wait_for(self._instant_hedge_done.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.logger.warning("[对账] 即时对冲超时, 继续对账")
        l = await self._get_lighter_position()
        h = await self._get_hotstuff_position()
        _old_l, _old_h = self.lighter_position, self.hotstuff_position
        self.lighter_position = l
        self.hotstuff_position = h
        net = abs(l + h)
        if _old_l != l or _old_h != h:
            self.logger.info(
                f"[对账] 仓位同步: L {_old_l}→{l}  HS {_old_h}→{h}")
        if net > self.order_size * Decimal("0.5"):
            self.logger.warning(f"[对账] 不平衡: L={l} HS={h} net={net}")
        return net < self.order_size * Decimal("2")

    async def _emergency_flatten(self):
        self.logger.error("[紧急平仓] 开始...")
        self._in_flatten = True

        # 1. 撤掉所有挂单
        await self._cancel_all_lighter()
        await self._cancel_all_hotstuff_orders()

        # 2. 对账获取真实仓位
        await self._reconcile()

        # 3. 平 Lighter 仓位
        if abs(self.lighter_position) >= self.hs_lot_size:
            side = "sell" if self.lighter_position > 0 else "buy"
            qty = abs(self.lighter_position)
            ref = self._l_best_bid if side == "sell" else self._l_best_ask
            if (ref is None or ref <= 0):
                _fallback = self._l_best_ask if side == "sell" else self._l_best_bid
                if _fallback and _fallback > 0:
                    ref = _fallback * (Decimal("0.99") if side == "sell" else Decimal("1.01"))
                    self.logger.warning(f"[紧急平仓] Lighter无{('bid' if side=='sell' else 'ask')}, 用对手方兜底: {ref}")
            if ref and ref > 0:
                slip = Decimal("0.01")
                price = ref * (Decimal("1") - slip if side == "sell" else Decimal("1") + slip)
                price = self._round_lighter(price)
                await self._lighter_rate_wait()
                try:
                    _, _, err = await self.lighter_client.create_order(
                        market_index=self.lighter_market_index,
                        client_order_index=int(time.time() * 1000),
                        base_amount=int(qty * self.base_amount_multiplier),
                        price=int(price * self.price_multiplier),
                        is_ask=(side == "sell"),
                        order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                        time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                        order_expiry=self.lighter_client.DEFAULT_IOC_EXPIRY,
                        reduce_only=True,
                        trigger_price=0,
                    )
                    if err:
                        self.logger.error(f"[紧急平仓] Lighter IOC失败: {err}")
                    else:
                        if side == "sell":
                            self.lighter_position -= qty
                        else:
                            self.lighter_position += qty
                        self.logger.info(f"[紧急平仓] Lighter {side} {qty} 已发送")
                except Exception as e:
                    self.logger.error(f"[紧急平仓] Lighter异常: {e}")
            else:
                self.logger.error(f"[紧急平仓] Lighter完全无BBO, 无法平仓!")

        # 4. 平 HotStuff 仓位
        if abs(self.hotstuff_position) >= self.hs_lot_size:
            side = "sell" if self.hotstuff_position > 0 else "buy"
            qty = abs(self.hotstuff_position)
            hs_bid, hs_ask = self._get_hs_bbo()
            ref = hs_bid if side == "sell" else hs_ask
            if (ref is None or ref <= 0):
                _fallback = hs_ask if side == "sell" else hs_bid
                if _fallback and _fallback > 0:
                    ref = _fallback * (Decimal("0.99") if side == "sell" else Decimal("1.01"))
                    self.logger.warning(f"[紧急平仓] HotStuff无{('bid' if side=='sell' else 'ask')}, 用对手方兜底: {ref}")
            if ref and ref > 0:
                _fill = await self._hedge_hotstuff(side, qty, ref)
                if _fill is not None:
                    self.logger.info(f"[紧急平仓] HotStuff {side} {qty} 已发送")
                else:
                    self.logger.error(f"[紧急平仓] HotStuff IOC失败!")
            else:
                self.logger.error(f"[紧急平仓] HotStuff完全无BBO, 无法平仓!")

        await asyncio.sleep(1.0)
        if not self.stop_flag:
            await self._reconcile()
            _net_after = abs(self.lighter_position + self.hotstuff_position)
            if _net_after >= self.hs_lot_size:
                self.logger.warning(
                    f"[紧急平仓] 第一次对账后仍有偏差={_net_after}, 第二次尝试修复")
                _net = self.lighter_position + self.hotstuff_position
                if _net < 0:
                    _ref = self._l_best_ask if self._l_best_ask else Decimal("0")
                    if _ref > 0:
                        await self._lighter_ioc("buy", abs(_net), _ref)
                elif _net > 0:
                    _ref = self._l_best_bid if self._l_best_bid else Decimal("0")
                    if _ref > 0:
                        await self._lighter_ioc("sell", abs(_net), _ref)
                await asyncio.sleep(0.5)
                await self._reconcile()
        if self.ladder_step_bps > 0 and self._ladder_level > 0:
            self.logger.info(f"[紧急平仓] 阶梯 L{self._ladder_level} → L0")
            self._ladder_level = 0
        self._in_flatten = False
        self._stale_fills.clear()
        self.logger.info(f"[紧急平仓] 完成: L={self.lighter_position} HS={self.hotstuff_position}")

    # ─────────────────────────────────────────────
    #  V23: Balance P&L verification
    # ─────────────────────────────────────────────

    async def _v23_record_pre_trade_balance(self):
        """开仓完成后快照余额, 作为下次平仓的基准。"""
        try:
            _l, _ = await self._snapshot_balances()
            if _l is not None:
                self._pre_trade_balance_l = _l
                self.logger.info(f"[V36] 开仓后余额快照: L={_l}")
        except Exception as e:
            self.logger.warning(f"[V36] 开仓余额快照失败: {e}")

    async def _v23_verify_close_pnl(self, formula_net_bps: float, formula_dollar: float,
                                     close_qty: Decimal, hs_price: Decimal) -> Tuple[str, str, str, str]:
        """平仓后查Lighter余额, 计算真实P&L (HotStuff无余额API, 仅用Lighter侧)。

        Returns: (real_pnl_str, cumulative_real_str, bal_l_str, bal_hs_str)
        """
        try:
            _l_after, _ = await self._snapshot_balances()
            if _l_after is None:
                return "", "", "", ""

            _bal_l_str = str(_l_after)

            _pre_l = self._pre_trade_balance_l
            if _pre_l is None:
                self.logger.info(f"[V36] 平仓后余额: L={_l_after} (无开仓基准, 跳过对比)")
                self._pre_trade_balance_l = _l_after
                return "", f"{self.cumulative_real_pnl:.6f}", _bal_l_str, ""

            self._pre_trade_balance_l = _l_after
            self.logger.info(f"[V36] 平仓后余额: L={_l_after}")
            return "", f"{self.cumulative_real_pnl:.6f}", _bal_l_str, ""

        except Exception as e:
            self.logger.warning(f"[V36] 余额校验异常: {e}")
            return "", "", "", ""

    # ─────────────────────────────────────────────
    #  Main entry
    # ─────────────────────────────────────────────

    async def run(self):
        self._lighter_ws_task: Optional[asyncio.Task] = None
        self._main_task: Optional[asyncio.Task] = None

        def on_stop(*_):
            self.stop_flag = True
            self.logger.info("收到中断信号")
            if self._main_task and not self._main_task.done():
                self._main_task.cancel()
        for sig_ in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            try:
                asyncio.get_event_loop().add_signal_handler(sig_, on_stop)
            except NotImplementedError:
                signal.signal(sig_, on_stop)

        try:
            self._main_task = asyncio.current_task()
            await self._main_loop()
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        except Exception as e:
            self.logger.error(f"致命错误: {e}\n{traceback.format_exc()}")
        finally:
            self.logger.info("[退出] 开始安全关闭 (最多15秒)...")
            try:
                await asyncio.wait_for(self._safe_shutdown(), timeout=15)
            except (asyncio.TimeoutError, Exception) as e:
                self.logger.error(f"[退出] 安全关闭超时或异常: {e}, 强制退出")

            for t in [self._lighter_ws_task]:
                if t and not t.done():
                    t.cancel()
            self.logger.info("[退出] 完成")

    async def _safe_shutdown(self):
        """带超时的安全关闭流程。"""
        if self.dry_run:
            return

        try:
            await self._cancel_all_lighter()
            self.logger.info("[退出] Lighter挂单已撤销")
        except Exception as e:
            self.logger.error(f"[退出] 撤Lighter单异常: {e}")

        try:
            await self._reconcile()
        except Exception:
            pass

        try:
            await self._cancel_all_hotstuff_orders()
            self.logger.info("[退出] HotStuff挂单已撤销")
        except Exception as e:
            self.logger.error(f"[退出] 撤HotStuff单异常: {e}")

        if self.hs_feed:
            try:
                await self.hs_feed.disconnect()
            except Exception:
                pass

        if (abs(self.lighter_position) >= self.hs_lot_size
                or abs(self.hotstuff_position) >= self.hs_lot_size):
            self.logger.info(
                f"[退出] 残余仓位: L={self.lighter_position} "
                f"HS={self.hotstuff_position}, 紧急平仓")
            try:
                await self._emergency_flatten()
            except Exception as e:
                self.logger.error(f"[退出] 紧急平仓异常: {e}")
        else:
            self.logger.info("[退出] 无残余仓位")

    # ─────────────────────────────────────────────
    #  Main loop
    # ─────────────────────────────────────────────

    async def _main_loop(self):
        fee_one = float(FEE_HOTSTUFF_TAKER) * 10000
        _ladder_str = f"阶梯={self.ladder_step_bps}bps" if self.ladder_step_bps > 0 else "阶梯=关闭"
        self.logger.info(
            f"启动 HotStuff+Lighter V36 双Taker套利  {self.symbol}  "
            f"size={self.order_size}  "
            f"开仓: HotStuff IOC + Lighter IOC 对冲  L1 深度检查  "
            f"阈值: fee({fee_one:.2f})+ec({self.expected_close_cost})"
            f"+lat_cap({self.latency_cap})+edge({self.edge_buffer})  "
            f"平仓: basis<{self.close_spread_bps}bps → 深度检查+Taker IOC  {_ladder_str}  "
            f"max_pos={self.max_position}  dry_run={self.dry_run}")

        if not self.dry_run:
            self._init_lighter()
            self._get_lighter_market_config()
            self._init_hotstuff()
            self._get_hotstuff_instrument_info()

            await self._cancel_all_lighter()
            await self._cancel_all_hotstuff_orders()
            self.logger.info("交易客户端就绪")
        else:
            self._get_lighter_market_config()

        self._lighter_ws_task = asyncio.create_task(self._lighter_ws_loop())

        self.hs_feed = HotstuffFeed(self.symbol)
        self._hs_feed_task = asyncio.create_task(self.hs_feed.connect())

        self.logger.info("等待行情就绪...")
        t0 = time.time()
        while time.time() - t0 < 20 and not self.stop_flag:
            hs_bid, hs_ask = self._get_hs_bbo()
            hs_ok = hs_bid is not None and hs_ask is not None
            l_ok = self._l_best_bid is not None and self._l_best_ask is not None
            if hs_ok and l_ok:
                break
            if time.time() - t0 > 5 and int(time.time() - t0) % 3 == 0:
                self.logger.info(
                    f"  等待中... HS={'OK' if hs_ok else 'WAIT'} "
                    f"L={'OK' if l_ok else 'WAIT'}")
            await asyncio.sleep(0.5)

        hs_bid, hs_ask = self._get_hs_bbo()
        if hs_bid is None or self._l_best_bid is None:
            self.logger.error(
                f"行情超时! HS={hs_bid}/{hs_ask} "
                f"L={self._l_best_bid}/{self._l_best_ask}")
            return

        self.logger.info(
            f"行情就绪: L={self._l_best_bid}/{self._l_best_ask}  "
            f"HS={hs_bid}/{hs_ask}")

        if not self.dry_run:
            # ── 启动仓位同步: 二次确认防止API陈旧数据 ──
            await self._reconcile()
            _has_residual = (abs(self.lighter_position) >= self.hs_lot_size
                             or abs(self.hotstuff_position) >= self.hs_lot_size)
            if _has_residual:
                _l1, _h1 = self.lighter_position, self.hotstuff_position
                self.logger.info(
                    f"[启动] 首次查询检测到仓位: L={_l1} HS={_h1}, "
                    f"等待3s二次确认...")
                await asyncio.sleep(3)
                await self._reconcile()
                _l2, _h2 = self.lighter_position, self.hotstuff_position
                if _l1 != _l2 or _h1 != _h2:
                    self.logger.warning(
                        f"[启动] 二次查询仓位有变化: "
                        f"L {_l1}→{_l2}  HS {_h1}→{_h2}")
                _has_residual = (abs(self.lighter_position) >= self.hs_lot_size
                                 or abs(self.hotstuff_position) >= self.hs_lot_size)

            if _has_residual:
                _net = abs(self.lighter_position + self.hotstuff_position)
                if getattr(self, 'flatten_on_start', False):
                    self.logger.warning(
                        f"[启动] 残余仓位: L={self.lighter_position} HS={self.hotstuff_position}  "
                        f"--flatten-on-start → 紧急平仓")
                    await self._emergency_flatten()
                elif _net > self.order_size * Decimal("0.5"):
                    self.logger.warning(
                        f"[启动] 仓位不平衡: L={self.lighter_position} HS={self.hotstuff_position} "
                        f"净敞口={_net} → 紧急平仓修复")
                    await self._emergency_flatten()
                else:
                    _synced_layers = int(
                        min(abs(self.lighter_position), abs(self.hotstuff_position))
                        / self.order_size)
                    if self.ladder_step_bps > 0 and _synced_layers > 0:
                        self._ladder_level = _synced_layers
                    self.logger.info(
                        f"[启动] 同步交易所仓位: L={self.lighter_position} "
                        f"HS={self.hotstuff_position}  净={_net}  "
                        f"恢复阶梯=L{self._ladder_level} "
                        f"(下次开仓阈值≥{self.open_spread_bps + self._ladder_level * self.ladder_step_bps:.1f}bps)")

            # 记录初始余额基准
            _l_bal, _ = await self._snapshot_balances()
            self._initial_balance_l = _l_bal
            self._initial_balance_x = None
            self._pre_trade_balance_l = _l_bal
            self._pre_trade_balance_x = None
            if _l_bal is not None:
                self.logger.info(f"[V36] 初始余额: Lighter={_l_bal}")
            else:
                self.logger.warning(f"[V36] 初始余额获取失败")

        self.logger.info(f"进入主循环  状态={self.state.value}")
        last_status_log = 0.0
        last_reconcile = time.time()

        while not self.stop_flag:
            try:
                now = time.time()
                hs_bid, hs_ask = self._get_hs_bbo()
                l_bid = self._l_best_bid
                l_ask = self._l_best_ask

                if hs_bid is None or hs_ask is None or l_bid is None or l_ask is None:
                    await asyncio.sleep(self.interval)
                    continue

                hs_age = now - self._hs_bbo_ts if self._hs_bbo_ts > 0 else 999
                l_age = now - self._l_ws_ts if self._l_ws_ts > 0 else 999
                if l_age > 5 or hs_age > 5:
                    await asyncio.sleep(self.interval)
                    continue

                mid = (l_bid + l_ask + hs_bid + hs_ask) / 4
                basis_bps = float((l_bid + l_ask - hs_bid - hs_ask) / 2 / mid * 10000)

                # ── V34: Update EWMA volatility ──
                self._update_vol_tracker(l_bid, l_ask)

                # ── V34.1: Shadow learning (passive latency decay) ──
                _s_sell_raw = float((l_bid - hs_ask) / hs_ask * 10000) if hs_ask > 0 else 0
                _s_buy_raw = float((hs_bid - l_ask) / l_ask * 10000) if l_ask > 0 else 0
                self._shadow_snapshot(now, _s_sell_raw, _s_buy_raw)
                self._process_shadow_learning(now, l_bid, l_ask, hs_bid, hs_ask)

                # ── Status log (V35: show bucketed latency) ──
                if now - last_status_log > 15:
                    _st_s_bkt = self._bucket_spread(max(_s_sell_raw, _s_buy_raw))
                    _st_v_bkt = self._bucket_vol(self._short_vol_bps)
                    self.logger.info(
                        f"[{self.state.value}] basis={basis_bps:+.1f}bps  "
                        f"L={l_bid}/{l_ask} HS={hs_bid}/{hs_ask}  "
                        f"L_pos={self.lighter_position} HS_pos={self.hotstuff_position}  "
                        f"鲜度:L={l_age:.1f}s HS={hs_age:.1f}s  "
                        f"vol={self._short_vol_bps:.1f}bps({_st_v_bkt})  "
                        f"lat_b={self.latency_pool.summary('buy', _st_s_bkt, _st_v_bkt)}"
                        f"/s={self.latency_pool.summary('sell', _st_s_bkt, _st_v_bkt)}bps  "
                        f"累積=${self.cumulative_dollar_pnl:+.4f}")
                    last_status_log = now

                # ── Periodic reconcile + 仓位修复 (统一Lighter IOC, 0费用) ──
                if not self.dry_run and now - last_reconcile > 60 and self.state == State.IDLE:
                    await self._reconcile()
                    last_reconcile = now
                    _net = self.lighter_position + self.hotstuff_position
                    _imbalance = abs(_net)

                    if _imbalance >= self.lighter_min_size_step:
                        _repair_qty = min(_imbalance, self.order_size)
                        _repair_side = "buy" if _net < 0 else "sell"
                        _l_after = self.lighter_position + _repair_qty if _repair_side == "buy" \
                            else self.lighter_position - _repair_qty
                        _safe = abs(_l_after) <= self.max_position and \
                            _imbalance <= self.order_size * Decimal("3")
                        if _safe:
                            self.logger.warning(
                                f"[对账] 偏差={_imbalance}, "
                                f"L={self.lighter_position} X={self.hotstuff_position} "
                                f"net={_net} → Lighter IOC {_repair_side} {_repair_qty}")
                            self._in_flatten = True
                            if _repair_side == "buy":
                                _ref = self._l_best_ask if self._l_best_ask else l_ask
                            else:
                                _ref = self._l_best_bid if self._l_best_bid else l_bid
                            await self._lighter_ioc(_repair_side, _repair_qty, _ref)
                            await asyncio.sleep(0.5)
                            self._in_flatten = False
                        else:
                            self.logger.error(
                                f"[对账] 偏差异常大={_imbalance} 或修复后L仓位溢出"
                                f"({_l_after}), 跳过自动修复, 等待人工")

                # ── Handle stale fills → 立即对账修复 ──
                if self._stale_fills and self.state == State.IDLE:
                    sf = self._stale_fills.pop(0)
                    self.logger.warning(
                        f"[过期成交] {sf['side']} {sf['size']} @ {sf['price']} "
                        f"→ 立即触发对账修复")
                    await self._reconcile()
                    last_reconcile = now
                    _net = self.lighter_position + self.hotstuff_position
                    _imbalance = abs(_net)
                    if _imbalance >= self.lighter_min_size_step:
                        _sf_repair_qty = min(_imbalance, self.order_size)
                        _sf_side = "buy" if _net < 0 else "sell"
                        _sf_l_after = self.lighter_position + _sf_repair_qty if _sf_side == "buy" \
                            else self.lighter_position - _sf_repair_qty
                        if abs(_sf_l_after) <= self.max_position and \
                                _imbalance <= self.order_size * Decimal("3"):
                            self.logger.warning(
                                f"[过期成交修复] 偏差={_imbalance} → "
                                f"Lighter IOC {_sf_side} {_sf_repair_qty}")
                            self._in_flatten = True
                            if _sf_side == "buy":
                                _ref = self._l_best_ask if self._l_best_ask else l_ask
                            else:
                                _ref = self._l_best_bid if self._l_best_bid else l_bid
                            await self._lighter_ioc(_sf_side, _sf_repair_qty, _ref)
                            await asyncio.sleep(0.3)
                            self._in_flatten = False
                        else:
                            self.logger.error(
                                f"[过期成交修复] 偏差异常={_imbalance}, 跳过自动修复")
                    continue

                # ━━━━━━ STATE: IDLE ━━━━━━
                if self.state == State.IDLE:
                    if now < self._circuit_breaker_until:
                        await asyncio.sleep(1)
                        continue

                    if self.dry_run:
                        await asyncio.sleep(self.interval)
                        continue

                    _has_pos = (abs(self.lighter_position) >= self.hs_lot_size
                                and abs(self.hotstuff_position) >= self.hs_lot_size)

                    # ── V36: 深度检查 + Taker IOC 平仓 ──
                    if _has_pos:
                        if self.lighter_position < 0:
                            _should_close = basis_bps <= self.close_spread_bps
                        else:
                            _should_close = basis_bps >= -self.close_spread_bps
                        if _should_close:
                            _closable = min(abs(self.lighter_position),
                                            abs(self.hotstuff_position))
                            _batch_qty = min(_closable, self.order_size)

                            # V35.2: depth check before closing
                            _close_side = "sell" if self.lighter_position > 0 else "buy"
                            _depth_ok, _depth_avail, _depth_slip = self._check_lighter_close_depth(
                                _close_side, _batch_qty, self.close_depth_levels)
                            if not _depth_ok:
                                self.logger.info(
                                    f"[IDLE] 价差收敛但深度不足: "
                                    f"需要{float(_batch_qty):.4f} "
                                    f"可用={_depth_avail:.5f} "
                                    f"(L1-L{self.close_depth_levels}) → 跳过")
                                await asyncio.sleep(self.interval)
                                continue

                            self._signal_hedge_mode = "taker"

                            _total_layers = max(1, int(_closable / self.order_size))
                            self.logger.info(
                                f"[IDLE] 价差收敛! basis={basis_bps:.1f}bps  "
                                f"mode=taker  "
                                f"单层平1/{_total_layers}层 qty={_batch_qty}  "
                                f"depth={_depth_avail:.5f} slip={_depth_slip:.1f}bps  "
                                f"L={self.lighter_position} X={self.hotstuff_position}  "
                                f"ladder=L{self._ladder_level}")

                            ok, _closed = await self._close_position_taker(
                                l_bid, l_ask, hs_bid, hs_ask, _batch_qty)

                            if not ok:
                                self.logger.error("[IDLE] 平仓失败 → REPAIR")
                                self.state = State.REPAIR
                                continue

                            hs_bid, hs_ask = self._get_hs_bbo()
                            l_bid = self._l_best_bid
                            l_ask = self._l_best_ask

                            _still_has = (abs(self.lighter_position) >= self.hs_lot_size
                                          and abs(self.hotstuff_position) >= self.hs_lot_size)
                            if _still_has and self.close_wait_sec > 0:
                                self.logger.info(
                                    f"[IDLE] 已平{_closed}层, 等待{self.close_wait_sec}s观察价差是否继续收敛"
                                    f" (深度阈值={self.close_deep_bps}bps)")
                                await asyncio.sleep(self.close_wait_sec)

                                hs_bid, hs_ask = self._get_hs_bbo()
                                l_bid = self._l_best_bid
                                l_ask = self._l_best_ask
                                if (hs_bid is None or hs_ask is None
                                        or l_bid is None or l_ask is None):
                                    continue
                                mid = (l_bid + l_ask + hs_bid + hs_ask) / 4
                                _new_basis = float((l_bid + l_ask - hs_bid - hs_ask) / 2 / mid * 10000)

                                if self.lighter_position < 0:
                                    _deep_ok = _new_basis <= self.close_deep_bps
                                else:
                                    _deep_ok = _new_basis >= -self.close_deep_bps
                                if _deep_ok:
                                    _remain_qty = min(
                                        abs(self.lighter_position),
                                        abs(self.hotstuff_position),
                                        self.order_size,
                                    )

                                    # V35.2: depth check for stage2
                                    _deep_side = "sell" if self.lighter_position > 0 else "buy"
                                    _d2_ok, _d2_avail, _d2_slip = self._check_lighter_close_depth(
                                        _deep_side, _remain_qty, self.close_depth_levels)
                                    if not _d2_ok:
                                        self.logger.info(
                                            f"[IDLE] 第二阶段深度不足: "
                                            f"需要{float(_remain_qty):.4f} "
                                            f"可用={_d2_avail:.5f} → 跳过")
                                        continue

                                    self._signal_hedge_mode = "taker"
                                    self.logger.info(
                                        f"[IDLE] 价差继续收敛! basis={_new_basis:.1f}bps "
                                        f"≤ {self.close_deep_bps}bps → "
                                        f"mode=taker 单层平剩余 {_remain_qty} "
                                        f"depth={_d2_avail:.5f} slip={_d2_slip:.1f}bps")
                                    if _remain_qty >= self.hs_lot_size:
                                        ok, _ = await self._close_position_taker(
                                            l_bid, l_ask, hs_bid, hs_ask, _remain_qty)
                                        if not ok:
                                            self.state = State.REPAIR
                                else:
                                    self.logger.info(
                                        f"[IDLE] 价差反弹 basis={_new_basis:.1f}bps "
                                        f"> {self.close_deep_bps}bps → 保留剩余{abs(self.lighter_position)}仓位")
                            continue

                    # ── 持仓上限检查 ──
                    _abs_l = abs(self.lighter_position)
                    _abs_x = abs(self.hotstuff_position)
                    _at_max = (_abs_l >= self.max_position
                               or _abs_x >= self.max_position)
                    if _at_max:
                        if now - last_status_log > 15:
                            self.logger.info(
                                f"[IDLE] 持仓已满 "
                                f"L={self.lighter_position} X={self.hotstuff_position} "
                                f"max={self.max_position}, 等待价差收敛平仓")
                        await asyncio.sleep(1)
                        continue

                    # ━━━━━━ V36 双向自动开仓: HotStuff IOC + Lighter IOC 对冲 ━━━━━━
                    # Direction A: HS buy + L sell → profitable when L_bid > HS_ask
                    # Direction B: HS sell + L buy → profitable when HS_bid > L_ask
                    _allow_sell = self.lighter_position >= -self.max_position * Decimal("0.8")
                    _allow_buy = self.lighter_position <= self.max_position * Decimal("0.8")

                    _sell_spread_raw = float((l_bid - hs_ask) / hs_ask * 10000) if hs_ask > 0 else 0
                    _buy_spread_raw = float((hs_bid - l_ask) / l_ask * 10000) if l_ask > 0 else 0

                    _l_vwap_bid = self._calc_lighter_vwap("sell", self.order_size)
                    _l_vwap_ask = self._calc_lighter_vwap("buy", self.order_size)
                    _sell_spread_exec = float((_l_vwap_bid - hs_ask) / hs_ask * 10000) \
                        if _l_vwap_bid and hs_ask > 0 else _sell_spread_raw
                    _buy_spread_exec = float((hs_bid - _l_vwap_ask) / _l_vwap_ask * 10000) \
                        if _l_vwap_ask and hs_bid > 0 else _buy_spread_raw

                    _sell_threshold = self._calc_dynamic_threshold(
                        "sell", self.order_size, _sell_spread_raw)
                    _buy_threshold = self._calc_dynamic_threshold(
                        "buy", self.order_size, _buy_spread_raw)

                    _open_side = None
                    _open_spread = 0.0
                    _open_threshold = 0.0
                    _open_spread_raw = 0.0

                    _sell_excess = (_sell_spread_exec - _sell_threshold) if _allow_sell else -1e9
                    _buy_excess = (_buy_spread_exec - _buy_threshold) if _allow_buy else -1e9

                    if _sell_excess > 0 or _buy_excess > 0:
                        if _sell_excess >= _buy_excess:
                            _open_side = "sell"
                            _open_spread = _sell_spread_exec
                            _open_spread_raw = _sell_spread_raw
                            _open_threshold = _sell_threshold
                        else:
                            _open_side = "buy"
                            _open_spread = _buy_spread_exec
                            _open_spread_raw = _buy_spread_raw
                            _open_threshold = _buy_threshold

                    # V34: Persistence filter with direction memory
                    if _open_side:
                        if _open_side != self._persist_side:
                            self._spread_above_count = 1
                            self._spread_above_since = now
                            self._persist_side = _open_side
                        else:
                            self._spread_above_count += 1
                            if self._spread_above_since is None:
                                self._spread_above_since = now
                    else:
                        self._spread_above_count = 0
                        self._spread_above_since = None
                        self._persist_side = None

                    _persistent = (
                        self._spread_above_count >= self.persist_count
                        or (self._spread_above_since is not None
                            and now - self._spread_above_since >= self.persist_ms)
                    )

                    if _open_side and _persistent:
                        # V36: Check L1+L2 depth before taker
                        _depth_ok, _depth_avail, _depth_slip, _depth_vwap = \
                            self._check_lighter_open_depth(
                                _open_side, self.order_size, self.open_depth_levels)

                        if not _depth_ok:
                            self.logger.info(
                                f"[TAKER] {_open_side} 深度不足: "
                                f"需要{float(self.order_size):.4f} "
                                f"可用={_depth_avail:.5f} "
                                f"(L1-L{self.open_depth_levels}) → 跳过")
                            await asyncio.sleep(self.interval)
                            continue

                        # Compute IOC price with slip buffer
                        _slip_buf = Decimal("0.003")
                        if _open_side == "buy":
                            _ioc_price = self._round_lighter(
                                l_ask * (Decimal("1") + _slip_buf))
                        else:
                            _ioc_price = self._round_lighter(
                                l_bid * (Decimal("1") - _slip_buf))

                        # Snapshot signal quality for CSV logging
                        _sig_s_bkt = self._bucket_spread(_open_spread_raw)
                        _sig_v_bkt = self._bucket_vol(self._short_vol_bps)
                        _sig_lat, _sig_lat_lvl = self.latency_pool.get_latency(
                            _open_side, _sig_s_bkt, _sig_v_bkt)

                        self._signal_observed_spread = _open_spread_raw
                        self._signal_executable_spread = _open_spread
                        self._signal_latency_penalty = _sig_lat
                        self._signal_vol_bps = self._short_vol_bps
                        self._signal_effective_bps = _open_threshold
                        self._signal_s_bucket = _sig_s_bkt
                        self._signal_v_bucket = _sig_v_bkt
                        self._signal_d_bucket = self._bucket_depth(_open_side, self.order_size)
                        self._signal_t_bucket = self._bucket_session(datetime.now(_TZ_CN).hour)

                        # L1 深度检查: HotStuff
                        _hs_depth_side = "buy" if _open_side == "sell" else "sell"
                        if _hs_depth_side == "buy":
                            _hs_l1_sz = self._hs_l1_ask_size
                        else:
                            _hs_l1_sz = self._hs_l1_bid_size
                        if _hs_l1_sz < float(self.order_size):
                            self.logger.info(
                                f"[TAKER] HotStuff L1深度不足: "
                                f"需要{float(self.order_size):.4f} "
                                f"可用={_hs_l1_sz:.5f} → 跳过")
                            await asyncio.sleep(self.interval)
                            continue

                        self.logger.info(
                            f"[TAKER] {_open_side} exec={_open_spread:.1f}bps"
                            f"(raw={_open_spread_raw:.1f}) "
                            f"≥ thresh={_open_threshold:.1f}bps"
                            f"(fee=2.5+ec={self.expected_close_cost}"
                            f"+lat={min(_sig_lat, self.latency_cap):.1f}[L{_sig_lat_lvl}]"
                            f"+vol={self._short_vol_bps * self.vol_weight:.1f}"
                            f"+edge={self.edge_buffer})  "
                            f"persist={self._spread_above_count}  "
                            f"L_depth={_depth_avail:.5f} HS_depth={_hs_l1_sz:.5f}  "
                            f"slip={_depth_slip:.1f}bps")

                        # V36 HotStuff+Lighter: HotStuff IOC 开仓 (乐观填充)
                        #   direction sell: HS_bid > L_ask → HS sell + L buy
                        #   direction buy:  L_bid > HS_ask → HS buy + L sell
                        _hs_open_side = "buy" if _open_side == "sell" else "sell"
                        _hs_ref = hs_ask if _hs_open_side == "buy" else hs_bid

                        _t0_open = time.time()
                        _hs_fill = await self._hedge_hotstuff(
                            _hs_open_side, self.order_size, _hs_ref)
                        _open_ms = (time.time() - _t0_open) * 1000

                        if _hs_fill is None:
                            self.logger.error(
                                f"[TAKER] HotStuff IOC失败 ({_open_ms:.0f}ms)")
                            self._reset_cycle()
                            await asyncio.sleep(0.5)
                            continue

                        self.logger.info(
                            f"[TAKER] HotStuff IOC完成 ({_open_ms:.0f}ms) "
                            f"{_hs_open_side} {self.order_size} → 即时Lighter对冲")

                        # 即时 Lighter IOC 对冲
                        _l_hedge_side = _open_side  # sell on L when HS buys, buy on L when HS sells
                        _l_ref = l_bid if _l_hedge_side == "sell" else l_ask

                        _slip_buf = Decimal("0.003")
                        if _l_hedge_side == "buy":
                            _l_ioc_price = self._round_lighter(
                                l_ask * (Decimal("1") + _slip_buf))
                        else:
                            _l_ioc_price = self._round_lighter(
                                l_bid * (Decimal("1") - _slip_buf))

                        _ioc_cid = int(time.time() * 1000) * 10 + (
                            1 if _l_hedge_side == "sell" else 2)

                        self._fill_event.clear()
                        self._fill_side = None
                        if _l_hedge_side == "sell":
                            self._l_sell_client_id = _ioc_cid
                        else:
                            self._l_buy_client_id = _ioc_cid

                        await self._lighter_rate_wait()
                        try:
                            _, _, _err = await self.lighter_client.create_order(
                                market_index=self.lighter_market_index,
                                client_order_index=_ioc_cid,
                                base_amount=int(
                                    self.order_size * self.base_amount_multiplier),
                                price=int(_l_ioc_price * self.price_multiplier),
                                is_ask=(_l_hedge_side == "sell"),
                                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                                order_expiry=self.lighter_client.DEFAULT_IOC_EXPIRY,
                                reduce_only=False,
                                trigger_price=0,
                            )
                            if _err:
                                self.logger.error(
                                    f"[TAKER] Lighter对冲IOC失败: {_err}")
                                self.state = State.HEDGING
                                continue
                        except Exception as _e:
                            self.logger.error(
                                f"[TAKER] Lighter对冲IOC异常: {_e}")
                            self.state = State.HEDGING
                            continue

                        # Lighter乐观更新仓位
                        if _l_hedge_side == "sell":
                            self.lighter_position -= self.order_size
                        else:
                            self.lighter_position += self.order_size

                        # 等待 WS 确认 (IOC 应在 <1s 内成交)
                        _filled = False
                        try:
                            await asyncio.wait_for(
                                self._fill_event.wait(), timeout=2.0)
                            _filled = self._fill_event.is_set()
                        except asyncio.TimeoutError:
                            pass

                        _total_ms = (time.time() - _t0_open) * 1000

                        if _filled and self._fill_price and self._fill_price > 0:
                            _l_actual = self._fill_price
                        else:
                            _l_actual = _l_ref

                        # 记录开仓 PnL
                        if _hs_open_side == "buy":
                            _spread_bps = float((_l_actual - _hs_ref) / _hs_ref * 10000)
                        else:
                            _spread_bps = float((_hs_ref - _l_actual) / _l_actual * 10000)
                        _fee = float(FEE_HOTSTUFF_TAKER + FEE_LIGHTER_MAKER) * 10000
                        _net_bps = _spread_bps - _fee
                        self.cumulative_net_bps += _net_bps
                        _dollar = float(_net_bps / 10000 * float(self.order_size) * float(_hs_ref))
                        self.cumulative_dollar_pnl += _dollar

                        self._trade_seq += 1
                        self.logger.info(
                            f"[开仓完成] HS_{_hs_open_side}@{_hs_ref}  L_{_l_hedge_side}@{_l_actual}  "
                            f"spread={_spread_bps:.2f}bps fee={_fee:.2f}bps "
                            f"net={_net_bps:.2f}bps  ${_dollar:+.4f}  "
                            f"总耗时={_total_ms:.0f}ms  "
                            f"累积=${self.cumulative_dollar_pnl:+.4f}")

                        self._csv_row([
                            datetime.now(_TZ_CN).isoformat(),
                            f"hs_l_v36_open_{self._trade_seq:04d}",
                            f"open_{_hs_open_side}",
                            str(_l_actual), str(_hs_ref), str(self.order_size),
                            f"{_spread_bps:.2f}", f"{_fee:.2f}", f"{_net_bps:.2f}",
                            f"{_total_ms:.0f}", f"{self.cumulative_net_bps:.2f}",
                            "", f"{self.cumulative_real_pnl:.6f}", "", "",
                            f"{self._signal_observed_spread:.2f}",
                            f"{self._signal_executable_spread:.2f}",
                            f"{self._signal_latency_penalty:.2f}",
                            f"{self._signal_vol_bps:.2f}",
                            f"{self._signal_effective_bps:.2f}",
                            self._signal_hedge_mode,
                            self._signal_s_bucket, self._signal_v_bucket,
                            self._signal_d_bucket, self._signal_t_bucket,
                        ])

                        if self.ladder_step_bps > 0:
                            self._ladder_level += 1
                            self.logger.info(f"[阶梯] 开仓 → L{self._ladder_level}")

                        await self._v23_record_pre_trade_balance()
                        self._reset_cycle()
                        self.state = State.IDLE
                    else:
                        await asyncio.sleep(self.interval)

                # ━━━━━━ STATE: HEDGING (fallback — 如果开仓后Lighter对冲失败) ━━━━━━
                elif self.state == State.HEDGING:
                    self.logger.info("[HEDGING] Lighter对冲失败, fallback重试...")
                    _hedge_side = self._fill_side
                    _qty = self._fill_qty if self._fill_qty > 0 else self.order_size

                    if _hedge_side is None:
                        _hedge_side = "sell"
                    _l_ref = self._l_best_bid if _hedge_side == "sell" else self._l_best_ask
                    if _l_ref is None or _l_ref <= 0:
                        self.logger.error("[HEDGING] 无 Lighter BBO, 进入REPAIR")
                        self.state = State.REPAIR
                        continue

                    _hedge_ok = False
                    for _retry in range(3):
                        if self.stop_flag:
                            break
                        _slip_buf = Decimal("0.005")
                        if _hedge_side == "buy":
                            _ioc_p = self._round_lighter(_l_ref * (Decimal("1") + _slip_buf))
                        else:
                            _ioc_p = self._round_lighter(_l_ref * (Decimal("1") - _slip_buf))
                        _cid = int(time.time() * 1000) * 10 + 7
                        await self._lighter_rate_wait()
                        try:
                            _, _, _err = await self.lighter_client.create_order(
                                market_index=self.lighter_market_index,
                                client_order_index=_cid,
                                base_amount=int(_qty * self.base_amount_multiplier),
                                price=int(_ioc_p * self.price_multiplier),
                                is_ask=(_hedge_side == "sell"),
                                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                                order_expiry=self.lighter_client.DEFAULT_IOC_EXPIRY,
                                reduce_only=False,
                                trigger_price=0,
                            )
                            if not _err:
                                if _hedge_side == "sell":
                                    self.lighter_position -= _qty
                                else:
                                    self.lighter_position += _qty
                                self.logger.info(f"[HEDGING] Lighter IOC 补单成功: {_hedge_side} {_qty}")
                                _hedge_ok = True
                                break
                            else:
                                self.logger.warning(f"[HEDGING] 重试 {_retry+1}/3: {_err}")
                        except Exception as _e:
                            self.logger.warning(f"[HEDGING] 重试 {_retry+1}/3 异常: {_e}")
                        await asyncio.sleep(0.2)

                    if not _hedge_ok:
                        self.logger.error("[HEDGING] 3次重试失败 → REPAIR")
                        self._consecutive_hedge_fails += 1
                        if self._consecutive_hedge_fails >= 3:
                            self._circuit_breaker_until = time.time() + 60
                            self.logger.error("[熔断] 暂停60秒")
                        self.state = State.REPAIR
                        continue

                    self._consecutive_hedge_fails = 0
                    self._reset_cycle()
                    self.state = State.IDLE

                # ━━━━━━ STATE: REPAIR ━━━━━━
                elif self.state == State.REPAIR:
                    self.logger.info("[REPAIR] 对账并修复...")
                    await self._cancel_all_lighter()
                    await self._reconcile()
                    if abs(self.lighter_position + self.hotstuff_position) > self.order_size * Decimal("0.5"):
                        await self._emergency_flatten()
                    # ISSUE-9 fix: 修复后检查净敞口，未归零则冷却 30 秒
                    _net_after_repair = abs(self.lighter_position + self.hotstuff_position)
                    if _net_after_repair >= self.order_size * Decimal("0.5"):
                        self.logger.error(
                            f"[REPAIR] 修复后仍有净敞口={_net_after_repair}, 冷却30秒")
                        self._circuit_breaker_until = time.time() + 30
                    self._reset_cycle()
                    self.state = State.IDLE

                await asyncio.sleep(self.interval)

            except asyncio.CancelledError:
                return
            except Exception as e:
                self.logger.error(f"主循环异常: {e}\n{traceback.format_exc()}")
                if self._in_flatten:
                    self.logger.warning("[安全] 异常期间 _in_flatten 仍为True, 已重置")
                    self._in_flatten = False
                await asyncio.sleep(2)

    def _reset_cycle(self):
        self._fill_event.clear()
        self._fill_side = None
        self._fill_price = Decimal("0")
        self._fill_qty = Decimal("0")
        self._fill_qty_pending = Decimal("0")
        self._l_buy_client_id = None
        self._l_sell_client_id = None
        self._l_buy_order_idx = None
        self._l_sell_order_idx = None
        # V22 reset
        self._fill_hs_bid = None
        self._fill_hs_ask = None
        self._fill_ts = 0.0
        if self._instant_hedge_task and not self._instant_hedge_task.done():
            self._instant_hedge_task.cancel()
        self._instant_hedge_done.clear()
        self._instant_hedge_ok = False
        self._instant_hedge_fill_price = None
        self._instant_hedge_task = None
        # V34 reset persistence filter
        self._spread_above_count = 0
        self._spread_above_since = None
        self._persist_side = None
        self._signal_observed_spread = 0.0
        self._signal_executable_spread = 0.0
        self._signal_latency_penalty = 0.0
        self._signal_vol_bps = 0.0
        self._signal_effective_bps = 0.0
        self._signal_hedge_mode = ""
        self._signal_s_bucket = ""
        self._signal_v_bucket = ""
        self._signal_d_bucket = ""
        self._signal_t_bucket = ""


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(
        description="HotStuff+Lighter V36: 双Taker套利 (HS IOC开仓 + L IOC对冲, L1深度检查)")
    p.add_argument("-s", "--symbol", default="BTC")
    p.add_argument("--size", type=str, default="0.005")
    p.add_argument("--max-position", type=str, default="0.02")
    p.add_argument("--open-spread", type=float, default=3.5,
                   help="基础开仓价差bps: 动态阈值的下限参考 (默认3.5)")
    p.add_argument("--close-spread", type=float, default=0.0,
                   help="平仓价差阈值bps: 价差收敛到此值时平仓 (默认0)")
    p.add_argument("--max-order-age", type=float, default=30.0)
    p.add_argument("--spread-buffer", type=float, default=0.0)
    p.add_argument("--ladder-step", type=float, default=0.4,
                   help="阶梯间距bps: 每层开仓间隔 (默认0.4)")
    p.add_argument("--close-deep", type=float, default=-0.3)
    p.add_argument("--close-wait", type=float, default=2.0)
    p.add_argument("--hedge-timeout", type=float, default=5.0)
    p.add_argument("--interval", type=float, default=0.05)
    p.add_argument("--flatten-on-start", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--env-file", default=".env")
    p.add_argument("--vol-weight", type=float, default=0.5)
    p.add_argument("--inv-weight", type=float, default=1.0)
    p.add_argument("--persist-count", type=int, default=3)
    p.add_argument("--persist-ms", type=float, default=200.0)
    p.add_argument("--latency-cold", type=float, default=0.8)
    p.add_argument("--expected-close-cost", type=float, default=0.8)
    p.add_argument("--latency-cap", type=float, default=1.5)
    p.add_argument("--edge-buffer", type=float, default=0.5)
    p.add_argument("--close-depth-levels", type=int, default=1,
                   help="平仓Lighter深度检查档数 (默认1=仅L1)")
    p.add_argument("--open-depth-levels", type=int, default=1,
                   help="开仓Lighter深度检查档数 (默认1=仅L1)")
    args = p.parse_args()

    load_dotenv(args.env_file)

    # ── 进程锁: 防止同一 symbol 双实例 ──
    _pid_lock = PidLock(args.symbol.upper())
    if not _pid_lock.acquire():
        sys.exit(1)
    atexit.register(_pid_lock.release)

    bot = SpreadArbBot(
        symbol=args.symbol.upper(),
        order_size=Decimal(args.size),
        max_position=Decimal(args.max_position),
        open_spread_bps=args.open_spread,
        close_spread_bps=args.close_spread,
        max_order_age=args.max_order_age,
        hedge_timeout=args.hedge_timeout,
        interval=args.interval,
        dry_run=args.dry_run,
        spread_buffer_bps=args.spread_buffer,
        ladder_step_bps=args.ladder_step,
        close_deep_bps=args.close_deep,
        close_wait_sec=args.close_wait,
        vol_weight=args.vol_weight,
        inv_weight=args.inv_weight,
        persist_count=args.persist_count,
        persist_ms=args.persist_ms,
        latency_cold=args.latency_cold,
        expected_close_cost=args.expected_close_cost,
        latency_cap=args.latency_cap,
        edge_buffer=args.edge_buffer,
        close_depth_levels=args.close_depth_levels,
        open_depth_levels=args.open_depth_levels,
    )
    bot.flatten_on_start = args.flatten_on_start

    fee_per = float(FEE_HOTSTUFF_TAKER) * 10000
    print(f"[V36] Taker开仓 + 双向深度加权 (IOC taker open + depth check)")
    print(f"  {args.symbol} size={args.size} max_pos={args.max_position}")
    print(f"  开仓: Taker IOC + L1-L{args.open_depth_levels}深度检查 (无JIT等待)")
    print(f"  阈值 = max(open_spread({args.open_spread}), "
          f"fee({fee_per:.2f}) + ec({args.expected_close_cost}) "
          f"+ min(lat,{args.latency_cap}) + slip + vol*{args.vol_weight} + edge({args.edge_buffer}))")
    print(f"  持续性过滤: {args.persist_count}次/{args.persist_ms}ms")
    print(f"  平仓: basis≤{args.close_spread} → 单层({args.size}) + L1-L{args.close_depth_levels}深度检查 → Taker IOC")
    step_info = f"  阶梯间距={args.ladder_step}bps" if args.ladder_step > 0 else "  阶梯=关闭"
    print(step_info)
    print(f"  延迟冷启动={args.latency_cold}bps → 影子学习~6秒自动热启动  inv_w={args.inv_weight}")
    print(f"  V36改进: Maker→Taker开仓(200ms<300ms+1.5s), 0%taker费, 消除JIT衰减")

    if uvloop is not None:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("[uvloop] 已启用")

    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n已中断。")


if __name__ == "__main__":
    main()
