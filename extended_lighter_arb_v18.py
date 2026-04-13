#!/usr/bin/env python3
"""
Extended + Lighter Maker-Maker 套利 V18

V18 在 V17 基础上解决了关键的生产级问题:
  - 分级对冲: 对冲/平仓第二腿不再裸等 45-60s, 改为条件分级
    edge足够+BN稳 → 短maker(默认3/5s), 否则立即taker
  - route-aware 费用: 按实际路径(MM/MT/TT)分别记费,
    开仓+平仓费用独立追踪并累加, 日志精确反映真实成本
  - ladder/refill 独立门槛: taker补仓必须超过 PAIR_FEE_TT(5bps)才放行,
    不再被maker全局0.5bps误放行
  - reduce_only: close/hedge路径的Lighter单默认reduce_only=True, 防止翻仓
  - adverse selection 防护: Binance领先价+adverse_buffer+recovery_window+max_quote_age
  - CLI参数全链路生效: --smart-hedge-wait/edge, --smart-close-wait/gap 直控分级逻辑

策略（Maker-Maker + Binance 领先定价）:
  开仓: Extended双向maker挂单 → 成交后分级对冲Lighter(maker/taker视条件)
  平仓: Extended maker + Lighter分级(maker/taker视条件) → 双taker兜底
  补仓: ladder/refill走双taker路径, 独立使用TT费用门槛

费用 (PAIR_FEE, 单次交易对):
  MM: ≈0.5 bps (Extended maker + Lighter maker)
  MT: ≈2.5 bps (Extended maker + Lighter taker)
  TT: ≈5.0 bps (Extended taker + Lighter taker)

用法:
  python extended_lighter_arb_v18.py --symbol BTC --size 0.005 --target-spread 3.0 \\
      --close-tiers "1.5:50,0.5:100" --dyn-percentile 75
  python extended_lighter_arb_v18.py --symbol ETH --no-maker --target-spread 7.5
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import csv
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
from typing import List, Optional, Tuple

import aiohttp
import requests
import websockets

try:
    import uvloop
except ImportError:
    uvloop = None

from dotenv import load_dotenv
from lighter.signer_client import SignerClient
from x10.perpetual.orders import TimeInForce, OrderSide

sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.extended import ExtendedClient
from feeds.lighter_feed import LighterFeed
from feeds.existing_feeds import ExtendedFeed
from feeds.binance_feed import BinanceFeed


class State(str, Enum):
    IDLE = "IDLE"
    IN_POSITION = "IN_POSITION"


class _Config:
    def __init__(self, d: dict):
        for k, v in d.items():
            setattr(self, k, v)


class TakerBot:

    # Per-leg exchange fees (bps, one-way)
    FEE_EXTENDED_TAKER: float = 2.5
    FEE_EXTENDED_MAKER: float = 0.0
    FEE_LIGHTER_TAKER: float = 2.5
    FEE_LIGHTER_MAKER: float = 0.0

    # Composed pair fees (bps) — sum of both legs for a single trade-pair
    PAIR_FEE_TT: float = 5.0     # X taker + L taker
    PAIR_FEE_MT: float = 2.5     # X maker + L taker
    PAIR_FEE_MM: float = 0.5     # X maker + L maker (0+0 + safety margin)

    # Legacy aliases — kept so old non-V18 mode branches still work
    ROUND_TRIP_FEE_BPS_TAKER = PAIR_FEE_TT
    ROUND_TRIP_FEE_BPS_MAKER = PAIR_FEE_MT

    def __init__(
        self,
        symbol: str,
        order_size: Decimal,
        max_position: Decimal,
        target_spread: float,
        close_gap_bps: float,
        max_hold_sec: float,
        interval: float,
        dry_run: bool,
        ladder_step: float = 0.0,
        close_tiers: Optional[List[Tuple[float, float]]] = None,
    ):
        self.symbol = symbol.upper()
        self.order_size = order_size
        self.max_position = max_position
        self.target_spread = target_spread
        self.close_gap_bps = close_gap_bps
        self.max_hold_sec = max_hold_sec
        self.interval = interval
        self.dry_run = dry_run
        self.ladder_step = ladder_step

        if close_tiers and len(close_tiers) >= 1:
            self.close_tiers = sorted(close_tiers, key=lambda t: -t[0])
            if self.close_tiers[-1][1] < 100:
                self.close_tiers.append((self.close_gap_bps, 100.0))
        else:
            self.close_tiers = [(self.close_gap_bps, 100.0)]

        self.state = State.IDLE
        self.stop_flag = False
        self.flatten_on_start = False

        self.lighter_position = Decimal("0")
        self.extended_position = Decimal("0")
        self.position_opened_at: Optional[float] = None
        self.position_direction: Optional[str] = None

        self._open_x_refs: List[Tuple[Decimal, Decimal]] = []
        self._open_l_refs: List[Tuple[Decimal, Decimal]] = []
        self._last_open_spread_bps: float = 0.0
        self._last_ladder_attempt: float = 0.0
        self._open_route_fee_bps: float = 0.0
        self._last_hedge_used_maker: bool = False

        # ── Stats / trade tracking ──
        self._trade_seq: int = 0
        self._current_trade_id: str = ""
        self._open_hedge_latency_ms: float = 0.0
        self._open_bn_vol: float = 0.0
        self._open_x_side: str = ""
        self._open_route_tag: str = ""
        self._markout_task: Optional[asyncio.Task] = None

        self._tier_closed_pct: float = 0.0
        self._tier_close_records: List[dict] = []
        self._total_open_qty: Decimal = Decimal("0")

        # ── Extended IOC tracking (dual-taker / fallback) ──
        self._x_ioc_pending_id: Optional[str] = None
        self._x_ioc_confirmed = asyncio.Event()
        self._x_ioc_fill_qty: Decimal = Decimal("0")
        self._x_ioc_fill_price: Optional[Decimal] = None
        self._x_ioc_recent_ids: List[str] = []

        # ── Extended Maker (POST_ONLY) tracking — used by _close_position_mt ──
        self._x_maker_pending_id: Optional[str] = None
        self._x_maker_confirmed = asyncio.Event()
        self._x_maker_fill_qty: Decimal = Decimal("0")
        self._x_maker_fill_price: Optional[Decimal] = None
        self._x_maker_recent_ids: List[str] = []
        self._x_maker_timeout: float = 15.0
        self._x_maker_close_timeout: float = 3.0

        # ── Dual-sided maker state ──
        self._x_maker_buy_id: Optional[str] = None
        self._x_maker_sell_id: Optional[str] = None
        self._x_maker_buy_price: Decimal = Decimal("0")
        self._x_maker_sell_price: Decimal = Decimal("0")
        self._x_maker_buy_placed_at: float = 0.0
        self._x_maker_sell_placed_at: float = 0.0
        self._x_dm_fill_event = asyncio.Event()
        self._x_dm_fill_side: Optional[str] = None
        self._x_dm_fill_qty: Decimal = Decimal("0")
        self._x_dm_fill_price: Optional[Decimal] = None
        self._x_dm_buy_partial_qty: Decimal = Decimal("0")
        self._x_dm_sell_partial_qty: Decimal = Decimal("0")
        self._x_dm_buy_partial_price: Optional[Decimal] = None
        self._x_dm_sell_partial_price: Optional[Decimal] = None
        self._min_spread_gap_bps: float = 2.0

        # ── Maker management state ──
        self.maker_mode: bool = True
        self.ROUND_TRIP_FEE_BPS: float = self.ROUND_TRIP_FEE_BPS_MAKER
        self._price_tolerance_bps: float = 2.5
        self._maker_max_age: float = 25.0
        self._hedge_timeout: float = 3.0
        self._maker_cancel_timestamps: List[float] = []
        self._maker_cancel_cooldown: float = 2.0
        self._dm_last_full_cancel_ts: float = 0.0
        self._dm_repost_cooldown: float = 3.0
        self._dm_lighter_mid_at_place: float = 0.0
        self._dm_lighter_drift_bps: float = 3.5
        self._dm_force_cancel: bool = False
        self._dm_drift_up: bool = True

        # ── Binance leading indicator state ──
        self.binance_feed: Optional[BinanceFeed] = None
        self._bn_mid_at_place: float = 0.0
        self._bn_drift_bps: float = 1.5
        self._bn_force_cancel: bool = False
        self._bn_drift_up: bool = True
        self._bn_buy_cancel_ts: float = 0.0
        self._bn_sell_cancel_ts: float = 0.0
        self._bn_cancel_cooldown: float = 3.0
        self._urgent_wake = asyncio.Event()
        self._bn_vol_wide_threshold: float = 3.0
        self._bn_vol_narrow_threshold: float = 1.0
        
        self._consecutive_hedge_failures: int = 0
        self._max_hedge_failures: int = 5
        self._hedge_circuit_breaker_until: float = 0.0
        self._hedge_circuit_pause: float = 60.0

        # ── Dynamic threshold state ──
        self._spread_obs: collections.deque = collections.deque(maxlen=50000)
        self._dyn_window: float = 300.0
        self._dyn_percentile: float = 75.0
        self._dyn_min_samples: int = 100
        self._dyn_recalc_interval: float = 5.0
        self._dyn_last_calc_ts: float = 0.0
        self._dyn_current_threshold: float = target_spread
        self._dyn_fee_floor: float = self.ROUND_TRIP_FEE_BPS_MAKER

        # ── Lighter WS ──
        self._lighter_fill_event = asyncio.Event()
        self._lighter_fill_data: Optional[dict] = None
        self._last_lighter_avg_price: Optional[Decimal] = None
        self._lighter_ws_task: Optional[asyncio.Task] = None
        self._lighter_pending_coi: Optional[int] = None

        self.lighter_client: Optional[SignerClient] = None
        self.extended_client: Optional[ExtendedClient] = None
        self.lighter_feed: Optional[LighterFeed] = None
        self.extended_feed: Optional[ExtendedFeed] = None

        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
        self.api_key_index = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))
        self.lighter_market_index: int = 0
        self.base_amount_multiplier: int = 0
        self.price_multiplier: int = 0
        self.lighter_tick: Decimal = Decimal("0.01")

        self.extended_contract_id: str = ""
        self.extended_tick_size: Decimal = Decimal("0.01")
        self.extended_min_order_size: Decimal = Decimal("0.001")

        self.cumulative_net_bps: float = 0.0
        self._nonce_counter: int = 0
        self._nonce_error_count: int = 0

        self._tx_timestamps: List[float] = []
        self._tx_backoff_until: float = 0.0

        self._trade_stats: List[dict] = []
        self._open_dev_l_bps: float = 0.0
        self._open_dev_x_bps: float = 0.0
        self._open_excess_bps: float = 0.0
        self._open_theoretical_spread: float = 0.0
        self._last_x_fill_bbo: Optional[Tuple] = None
        self._last_extended_avg_price: Optional[Decimal] = None

        self._force_reconcile: bool = False
        self._trade_lock = asyncio.Lock()
        self._last_trade_time: float = 0.0
        self._min_hold_sec: float = 5.0
        self._ladder_cooldown: float = 10.0
        self._max_slippage_bps: float = 5.0
        self._neg_spread_reverse_bps: float = 1.0
        self._aio_session: Optional[aiohttp.ClientSession] = None
        self._bbo_event = asyncio.Event()

        # ── V18 Maker-Maker parameters ──
        self._smart_hedge_max_wait: float = 3.0
        self._smart_hedge_min_edge: float = 2.0
        self._smart_close_max_wait: float = 5.0
        self._smart_close_min_gap: float = 1.5
        self._maker_hedge_timeout: float = 5.0
        self._maker_adjust_interval: float = 2.0
        self._maker_adjust_bps: float = 3.0
        self._maker_bn_emergency_bps: float = 5.0
        self._adverse_buffer_bps: float = 0.5
        self._recovery_window_ms: float = 500.0
        self._max_quote_age_ms: float = 8000.0

        os.makedirs("logs", exist_ok=True)
        self.csv_path = f"logs/arb_v18_{self.symbol}_trades.csv"
        self.log_path = f"logs/arb_v18_{self.symbol}.log"
        self._init_csv()
        self.logger = self._init_logger()

    # ═══════════════════════════════════════════════
    #  Logging / CSV
    # ═══════════════════════════════════════════════

    def _init_logger(self) -> logging.Logger:
        lg = logging.getLogger(f"arb_v18_{self.symbol}")
        lg.setLevel(logging.INFO)
        lg.handlers.clear()
        _tz_cn = timezone(timedelta(hours=8))

        class _CNFormatter(logging.Formatter):
            converter = None
            def formatTime(self, record, datefmt=None):
                ct = datetime.fromtimestamp(record.created, tz=_tz_cn)
                if datefmt:
                    return ct.strftime(datefmt)
                return ct.strftime("%Y-%m-%d %H:%M:%S") + f",{int(record.msecs):03d}"

        fmt = _CNFormatter("%(asctime)s %(levelname)s %(message)s")
        fh = logging.FileHandler(self.log_path)
        fh.setFormatter(fmt)
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        lg.addHandler(fh)
        lg.addHandler(ch)
        lg.propagate = False
        for noisy in ("urllib3", "requests", "websockets", "lighter", "aiohttp"):
            logging.getLogger(noisy).setLevel(logging.WARNING)
        return lg

    def _init_csv(self):
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp", "action", "direction",
                    "lighter_side", "lighter_ref", "lighter_fill", "lighter_qty",
                    "extended_side", "extended_ref", "extended_fill", "extended_qty",
                    "spread_bps", "total_ms", "mode",
                    "dev_l", "dev_x", "excess_bps", "actual_net_bps",
                    "net_bps", "cumulative_net_bps",
                    "trade_id", "route", "open_fee_bps", "close_fee_bps",
                    "total_fee_bps", "hedge_latency_ms", "hedge_maker",
                    "x_side", "bn_vol_bps",
                    "markout_500ms_bps", "markout_1s_bps",
                ])

    def _csv_row(self, row: list):
        with open(self.csv_path, "a", newline="") as f:
            csv.writer(f).writerow(row)

    def _generate_trade_id(self) -> str:
        self._trade_seq += 1
        ts = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S")
        return f"{self.symbol}_{ts}_{self._trade_seq:04d}"

    async def _record_markout(self, trade_id: str, x_side: str,
                               fill_price: float, direction: str):
        """Record mid-price markout at 500ms and 1s after fill."""
        markouts = {}
        for delay_ms, label in [(500, "500ms"), (1000, "1s")]:
            await asyncio.sleep(delay_ms / 1000.0)
            ls = self.lighter_feed.snapshot if self.lighter_feed else None
            if ls and ls.best_bid and ls.best_ask:
                mid_now = float(ls.best_bid + ls.best_ask) / 2
                if fill_price > 0:
                    if direction == "long_X_short_L":
                        markouts[label] = (mid_now - fill_price) / fill_price * 10000
                    else:
                        markouts[label] = (fill_price - mid_now) / fill_price * 10000
                else:
                    markouts[label] = 0.0
            else:
                markouts[label] = None
        _m500 = markouts.get('500ms')
        _m1s = markouts.get('1s')
        self._csv_row([
            datetime.now(timezone(timedelta(hours=8))).isoformat(),
            "MARKOUT", direction,
            "", "", "", "",
            x_side, "", f"{fill_price:.2f}", "",
            "", "", "",
            "", "", "", "",
            "", "",
            trade_id, "", "", "", "",
            "", "", x_side, "",
            f"{_m500:.2f}" if _m500 is not None else "",
            f"{_m1s:.2f}" if _m1s is not None else "",
        ])
        m500 = markouts.get('500ms')
        m1s = markouts.get('1s')
        self.logger.info(
            f"[Markout|{trade_id}] {x_side} @ {fill_price:.2f} "
            f"500ms={f'{m500:.1f}' if m500 is not None else 'N/A'}bps "
            f"1s={f'{m1s:.1f}' if m1s is not None else 'N/A'}bps")

    # ═══════════════════════════════════════════════
    #  Client Initialization
    # ═══════════════════════════════════════════════

    def _init_lighter_signer(self):
        api_key = os.getenv("API_KEY_PRIVATE_KEY")
        if not api_key:
            raise ValueError("API_KEY_PRIVATE_KEY not set")
        self.lighter_client = SignerClient(
            url=self.lighter_base_url,
            account_index=self.account_index,
            api_private_keys={self.api_key_index: api_key},
        )
        err = self.lighter_client.check_client()
        if err is not None:
            raise RuntimeError(f"Lighter CheckClient error: {err}")
        self._nonce_error_count = 0
        self.logger.info("Lighter SignerClient 已初始化")

    def _check_nonce_error(self, error) -> bool:
        err_str = str(error).lower()
        if "nonce" not in err_str:
            self._nonce_error_count = 0
            return False
        self._nonce_error_count += 1
        try:
            self.lighter_client.nonce_manager.hard_refresh_nonce(self.api_key_index)
            self.logger.info(f"[Lighter] nonce 错误 (第{self._nonce_error_count}次), 已刷新")
        except Exception as e:
            self.logger.error(f"[Lighter] nonce 刷新失败: {e}")
        if self._nonce_error_count >= 5:
            self.logger.warning("[Lighter] nonce 连续失败, 重建 SignerClient")
            try:
                self._init_lighter_signer()
            except Exception as e:
                self.logger.error(f"[Lighter] 重建失败: {e}")
        return True

    def _get_lighter_market_config(self):
        url = f"{self.lighter_base_url}/api/v1/orderBooks"
        resp = requests.get(url, headers={"accept": "application/json"}, timeout=10)
        resp.raise_for_status()
        for m in resp.json().get("order_books", []):
            if m["symbol"] == self.symbol:
                self.lighter_market_index = m["market_id"]
                self.base_amount_multiplier = pow(10, m["supported_size_decimals"])
                self.price_multiplier = pow(10, m["supported_price_decimals"])
                self.lighter_tick = Decimal("1") / Decimal(10) ** m["supported_price_decimals"]
                self.logger.info(
                    f"Lighter market: idx={self.lighter_market_index} "
                    f"size_dec={m['supported_size_decimals']} "
                    f"price_dec={m['supported_price_decimals']}")
                return
        raise RuntimeError(f"{self.symbol} not found on Lighter")

    def _init_extended_client(self):
        cfg = _Config({
            "ticker": self.symbol, "contract_id": "",
            "quantity": self.order_size, "tick_size": Decimal("0.01"),
            "close_order_side": "sell",
        })
        self.extended_client = ExtendedClient(cfg)
        self.extended_client.setup_order_update_handler(self._on_extended_order_event)
        self.extended_client._ob_update_callback = self._on_bbo_update
        self.logger.info("Extended client 已初始化")

    async def _get_extended_contract_info(self):
        cid, tick = await self.extended_client.get_contract_attributes()
        self.extended_contract_id = cid
        self.extended_tick_size = tick
        if hasattr(self.extended_client, "min_order_size"):
            self.extended_min_order_size = self.extended_client.min_order_size
        self.logger.info(f"Extended contract: {cid}  tick={tick}")

    async def _warmup_extended(self):
        self.logger.info("[预热] Extended 连接预热中...")
        expire = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        try:
            t0 = time.perf_counter()
            await self.extended_client.perpetual_trading_client.place_order(
                market_name=self.extended_contract_id,
                amount_of_synthetic=Decimal("0.1"), price=Decimal("1.00"),
                side=OrderSide.BUY, post_only=False,
                time_in_force=TimeInForce.IOC, expire_time=expire)
            ms1 = (time.perf_counter() - t0) * 1000
            t0 = time.perf_counter()
            await self.extended_client.perpetual_trading_client.place_order(
                market_name=self.extended_contract_id,
                amount_of_synthetic=Decimal("0.1"), price=Decimal("1.00"),
                side=OrderSide.BUY, post_only=False,
                time_in_force=TimeInForce.IOC, expire_time=expire)
            ms2 = (time.perf_counter() - t0) * 1000
            self.logger.info(f"[预热] 冷启动={ms1:.0f}ms 预热后={ms2:.0f}ms")
        except Exception as e:
            self.logger.warning(f"[预热] 异常 (可忽略): {e}")

    # ═══════════════════════════════════════════════
    #  BBO
    # ═══════════════════════════════════════════════

    def _get_extended_ws_bbo(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        if self.extended_client and self.extended_client.orderbook:
            ob = self.extended_client.orderbook
            bids = ob.get('bid', [])
            asks = ob.get('ask', [])
            if bids and asks:
                try:
                    return Decimal(bids[0]['p']), Decimal(asks[0]['p'])
                except (KeyError, IndexError, TypeError):
                    pass
        return None, None

    def _get_l1_depth(self, l_side: str, x_side: str
                      ) -> Tuple[Decimal, Decimal]:
        ls = self.lighter_feed.snapshot if self.lighter_feed else None
        es = self.extended_feed.snapshot if self.extended_feed else None
        if l_side == "sell" and ls and ls.bids:
            l1_l = Decimal(str(ls.bids[0].size))
        elif l_side == "buy" and ls and ls.asks:
            l1_l = Decimal(str(ls.asks[0].size))
        else:
            l1_l = Decimal("999")
        if x_side == "buy" and es and es.asks:
            l1_x = Decimal(str(es.asks[0].size))
        elif x_side == "sell" and es and es.bids:
            l1_x = Decimal(str(es.bids[0].size))
        else:
            l1_x = Decimal("999")
        return l1_l, l1_x

    def _get_vwap(self, exchange: str, side: str, qty: float
                  ) -> Tuple[Optional[Decimal], Decimal]:
        """
        Get VWAP price and total available depth for `qty` on `exchange`.
        Returns (vwap_price, total_available_qty).
        vwap_price is None if insufficient depth to fill full qty.
        """
        if exchange == "lighter":
            snap = self.lighter_feed.snapshot if self.lighter_feed else None
        else:
            snap = self.extended_feed.snapshot if self.extended_feed else None
        if not snap:
            return None, Decimal("0")

        if side == "sell":
            vwap_f = snap.vwap_sell(float(qty))
            levels = snap.bids
        else:
            vwap_f = snap.vwap_buy(float(qty))
            levels = snap.asks

        total = Decimal(str(sum(lv.size for lv in levels))) if levels else Decimal("0")
        vwap = Decimal(str(vwap_f)) if vwap_f is not None else None
        return vwap, total

    def _on_bbo_update(self):
        self._bbo_event.set()

    async def _interruptible_sleep(self, seconds: float):
        """Sleep that can be interrupted by urgent cancel events."""
        self._urgent_wake.clear()
        try:
            await asyncio.wait_for(self._urgent_wake.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    def _on_lighter_bbo_update(self):
        """Lighter 价格变动回调: 检测是否需要紧急撤单(逆向选择保护)"""
        if self._dm_lighter_mid_at_place <= 0:
            return
        if self._x_maker_buy_id is None and self._x_maker_sell_id is None:
            return
        ls = self.lighter_feed.snapshot if self.lighter_feed else None
        if not ls or ls.mid <= 0:
            return
        drift_bps = abs(ls.mid - self._dm_lighter_mid_at_place) / self._dm_lighter_mid_at_place * 10000
        if drift_bps > self._dm_lighter_drift_bps:
            self._dm_force_cancel = True
            self._dm_drift_up = (ls.mid > self._dm_lighter_mid_at_place)
            self._urgent_wake.set()

    def _on_binance_bbo_update(self):
        """Binance 领先价格回调: 比 Lighter 快 200-1000ms 的逆选保护"""
        if self._bn_mid_at_place <= 0:
            return
        if self._x_maker_buy_id is None and self._x_maker_sell_id is None:
            return
        bf = self.binance_feed
        if not bf or bf.mid <= 0:
            return
        drift_bps = abs(bf.mid - self._bn_mid_at_place) / self._bn_mid_at_place * 10000
        if drift_bps > self._bn_drift_bps:
            self._bn_force_cancel = True
            self._bn_drift_up = (bf.mid > self._bn_mid_at_place)
            self._urgent_wake.set()

    def _get_bn_adaptive_spread(self) -> float:
        """根据 Binance 30s 波动率动态调整挂单价差"""
        bf = self.binance_feed
        if not bf or not bf.connected:
            return 0.0
        vol = bf.realized_vol_bps
        if vol >= self._bn_vol_wide_threshold:
            return 1.5
        elif vol <= self._bn_vol_narrow_threshold:
            return -0.5
        return 0.0

    # ═══════════════════════════════════════════════
    #  Extended WS Event (IOC + Maker tracking)
    # ═══════════════════════════════════════════════

    def _on_extended_order_event(self, event: dict):
        order_id = str(event.get("order_id", ""))
        status = event.get("status", "")
        filled_size_str = event.get("filled_size", "0")

        # ── IOC order tracking (dual-taker / fallback) ──
        if self._x_ioc_pending_id and order_id == self._x_ioc_pending_id:
            if status in ("FILLED", "CANCELED", "EXPIRED"):
                self._x_ioc_fill_qty = Decimal(str(filled_size_str))
                avg = event.get("avg_price")
                if avg:
                    try:
                        self._x_ioc_fill_price = Decimal(str(avg))
                    except Exception:
                        pass
                self._x_ioc_confirmed.set()
            return

        # ── Single maker tracking (used by _close_position_mt) ──
        if self._x_maker_pending_id and order_id == self._x_maker_pending_id:
            if status == "FILLED":
                self._x_maker_fill_qty = Decimal(str(filled_size_str))
                avg = event.get("avg_price")
                if avg:
                    try:
                        self._x_maker_fill_price = Decimal(str(avg))
                    except Exception:
                        pass
                self._x_maker_confirmed.set()
                self.logger.info(
                    f"[Extended Maker] 成交确认: id={order_id} "
                    f"filled={filled_size_str} avg={avg}")
            elif status == "PARTIALLY_FILLED":
                pf = Decimal(str(filled_size_str))
                if pf > self._x_maker_fill_qty:
                    self._x_maker_fill_qty = pf
                    avg = event.get("avg_price")
                    if avg:
                        try:
                            self._x_maker_fill_price = Decimal(str(avg))
                        except Exception:
                            pass
                self.logger.info(f"[Extended Maker] 部分成交: filled={pf}")
            elif status in ("CANCELED", "EXPIRED"):
                pf = Decimal(str(filled_size_str))
                if pf > 0:
                    self._x_maker_fill_qty = pf
                    avg = event.get("avg_price")
                    if avg:
                        try:
                            self._x_maker_fill_price = Decimal(str(avg))
                        except Exception:
                            pass
                self._x_maker_confirmed.set()
                self.logger.info(
                    f"[Extended Maker] 订单结束: status={status} filled={pf}")
            return

        # ── Dual-sided maker tracking (IDLE) ──
        _is_buy = self._x_maker_buy_id and order_id == self._x_maker_buy_id
        _is_sell = self._x_maker_sell_id and order_id == self._x_maker_sell_id
        if _is_buy or _is_sell:
            side_tag = "buy" if _is_buy else "sell"
            pf = Decimal(str(filled_size_str))
            avg_raw = event.get("avg_price")
            avg_dec = None
            if avg_raw:
                try:
                    avg_dec = Decimal(str(avg_raw))
                except Exception:
                    pass

            if status == "FILLED":
                self._x_dm_fill_side = side_tag
                self._x_dm_fill_qty = pf
                self._x_dm_fill_price = avg_dec
                self._x_dm_fill_event.set()
                self.logger.info(
                    f"[双向Maker] {side_tag}侧成交: filled={pf} avg={avg_raw}")
            elif status == "PARTIALLY_FILLED":
                if _is_buy:
                    if pf > self._x_dm_buy_partial_qty:
                        self._x_dm_buy_partial_qty = pf
                        self._x_dm_buy_partial_price = avg_dec
                else:
                    if pf > self._x_dm_sell_partial_qty:
                        self._x_dm_sell_partial_qty = pf
                        self._x_dm_sell_partial_price = avg_dec
                self.logger.info(
                    f"[双向Maker] {side_tag}侧部分成交: filled={pf}")
            elif status in ("CANCELED", "EXPIRED"):
                if pf > 0:
                    self._x_dm_fill_side = side_tag
                    self._x_dm_fill_qty = pf
                    self._x_dm_fill_price = avg_dec
                    self._x_dm_fill_event.set()
                    self.logger.info(
                        f"[双向Maker] {side_tag}侧撤单后有成交: filled={pf}")
                else:
                    if _is_buy:
                        self._x_maker_buy_id = None
                    else:
                        self._x_maker_sell_id = None
                    self.logger.info(
                        f"[双向Maker] {side_tag}侧已撤/过期(无成交)")
            return

        if order_id in self._x_ioc_recent_ids:
            return
        if order_id in self._x_maker_recent_ids:
            return

        if status in ("FILLED", "PARTIALLY_FILLED"):
            filled = Decimal(str(filled_size_str))
            if filled > 0:
                self.logger.error(
                    f"[Extended] 孤立订单成交! id={order_id} status={status} "
                    f"filled={filled}  触发立即对账")
                self._force_reconcile = True

    # ═══════════════════════════════════════════════
    #  Transaction Rate Limiting
    # ═══════════════════════════════════════════════

    def _check_tx_rate(self) -> bool:
        now = time.time()
        if now < self._tx_backoff_until:
            return False
        cutoff = now - 60.0
        self._tx_timestamps = [t for t in self._tx_timestamps if t > cutoff]
        return len(self._tx_timestamps) < 38

    def _record_tx(self):
        self._tx_timestamps.append(time.time())

    def _check_maker_cancel_rate(self) -> bool:
        now = time.time()
        cutoff = now - 60.0
        self._maker_cancel_timestamps = [
            t for t in self._maker_cancel_timestamps if t > cutoff]
        if len(self._maker_cancel_timestamps) >= 15:
            return False
        if (self._maker_cancel_timestamps
                and now - self._maker_cancel_timestamps[-1]
                < self._maker_cancel_cooldown):
            return False
        return True

    # ═══════════════════════════════════════════════
    #  Lighter Taker (IOC)
    # ═══════════════════════════════════════════════

    def _round_lighter_price(self, price: Decimal) -> Decimal:
        if self.lighter_tick > 0:
            return (price / self.lighter_tick).quantize(Decimal("1")) * self.lighter_tick
        return price

    async def _place_lighter_taker(self, side: str, qty: Decimal,
                                    ref_price: Decimal,
                                    reduce_only: bool = False) -> Optional[Decimal]:
        is_ask = side.lower() == "sell"
        slip = min(Decimal(str(self._max_slippage_bps)),
                   Decimal("20")) / Decimal("10000")
        price = self._round_lighter_price(
            ref_price * (Decimal("1") - slip if is_ask else Decimal("1") + slip))

        pre_pos = self.lighter_position
        self._nonce_counter += 1
        coi = int(time.time() * 1000) * 100 + (self._nonce_counter % 100)

        self._lighter_fill_event.clear()
        self._lighter_fill_data = None
        self._last_lighter_avg_price = None
        self._lighter_pending_coi = coi

        try:
            _, _, error = await self.lighter_client.create_order(
                market_index=self.lighter_market_index,
                client_order_index=coi,
                base_amount=int(qty * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                order_expiry=self.lighter_client.DEFAULT_IOC_EXPIRY,
                reduce_only=reduce_only,
                trigger_price=0,
            )
            self._record_tx()
            if error:
                self._check_nonce_error(error)
                self.logger.error(f"[Lighter] taker 失败: {error}")
                return None
            self._nonce_error_count = 0
            self.logger.info(f"[Lighter] taker {side} {qty} ref={ref_price:.2f}")

            try:
                await asyncio.wait_for(self._lighter_fill_event.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass

            fd = self._lighter_fill_data
            if fd is not None:
                filled_base = fd.get("filled_base_amount")
                if filled_base is not None:
                    ws_filled = Decimal(str(filled_base))
                    if ws_filled >= qty * Decimal("0.5") and ws_filled <= qty * Decimal("1.5"):
                        self._extract_lighter_fill_price(fd, ref_price)
                        if side.lower() == "sell":
                            new_pos = pre_pos - ws_filled
                        else:
                            new_pos = pre_pos + ws_filled
                        self.lighter_position = new_pos
                        avg_p = self._last_lighter_avg_price
                        self.logger.info(
                            f"[Lighter] WS成交确认: filled={ws_filled} "
                            f"pos {pre_pos} → {new_pos}"
                            + (f" avg={avg_p:.2f}" if avg_p else ""))
                        return ws_filled

            for attempt, delay in enumerate([0.1, 0.2, 0.3]):
                await asyncio.sleep(delay)
                try:
                    new_pos = await self._get_lighter_position()
                    if side.lower() == "buy":
                        delta = new_pos - pre_pos
                    else:
                        delta = pre_pos - new_pos
                    if delta >= qty * Decimal("0.5"):
                        self.lighter_position = new_pos
                        if self._last_lighter_avg_price is None:
                            self._last_lighter_avg_price = ref_price
                        actual_qty = abs(delta)
                        self.logger.info(
                            f"[Lighter] REST成交确认(第{attempt+1}次): "
                            f"pos {pre_pos} → {new_pos} Δ={actual_qty}")
                        return actual_qty
                except Exception as e:
                    self.logger.warning(f"[Lighter] 验证失败: {e}")

            self.logger.warning("[Lighter] IOC 未确认成交")
            return None
        except Exception as e:
            self._check_nonce_error(e)
            self.logger.error(f"[Lighter] taker 异常: {e}")
            return None

    # ═══════════════════════════════════════════════
    #  V18: Lighter Maker Order + Modify
    # ═══════════════════════════════════════════════

    async def _place_lighter_maker(
        self, side: str, qty: Decimal, maker_price: Decimal,
        reduce_only: bool = False,
    ) -> Tuple[Optional[int], bool]:
        """Place a non-IOC LIMIT order on Lighter (maker).
        Returns (client_order_index, success).
        """
        is_ask = side.lower() == "sell"
        price = self._round_lighter_price(maker_price)

        pre_pos = self.lighter_position
        self._nonce_counter += 1
        coi = int(time.time() * 1000) * 100 + (self._nonce_counter % 100)

        self._lighter_fill_event.clear()
        self._lighter_fill_data = None
        self._last_lighter_avg_price = None
        self._lighter_pending_coi = coi

        try:
            _, _, error = await self.lighter_client.create_order(
                market_index=self.lighter_market_index,
                client_order_index=coi,
                base_amount=int(qty * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=reduce_only,
                trigger_price=0,
            )
            self._record_tx()
            if error:
                self._check_nonce_error(error)
                self.logger.error(f"[Lighter] maker下单失败: {error}")
                return (None, False)
            self._nonce_error_count = 0
            self.logger.info(
                f"[Lighter] maker {side} {qty} @ {price:.2f} coi={coi}")
            return (coi, True)
        except Exception as e:
            self._check_nonce_error(e)
            self.logger.error(f"[Lighter] maker异常: {e}")
            return (None, False)

    async def _modify_lighter_order(
        self, coi: int, new_price: Decimal, qty: Decimal,
    ) -> bool:
        """Modify an existing Lighter order's price via modify_order."""
        try:
            rounded = self._round_lighter_price(new_price)
            _, _, error = await self.lighter_client.modify_order(
                market_index=self.lighter_market_index,
                order_index=coi,
                base_amount=int(qty * self.base_amount_multiplier),
                price=int(rounded * self.price_multiplier),
                trigger_price=0,
            )
            self._record_tx()
            if error:
                self.logger.warning(f"[Lighter] modify失败: {error}")
                return False
            self.logger.debug(f"[Lighter] maker调价→{rounded:.2f}")
            return True
        except Exception as e:
            self.logger.warning(f"[Lighter] modify异常: {e}")
            return False

    async def _lighter_maker_with_fallback(
        self, side: str, qty: Decimal, ref_price: Decimal,
        maker_timeout_s: Optional[float] = None,
        reduce_only: bool = False,
    ) -> Optional[Decimal]:
        """Place Lighter maker at mid-price with progressive adjustment.
        Falls back to taker on timeout or Binance emergency.
        Returns filled qty or None.
        """
        timeout = maker_timeout_s or self._maker_hedge_timeout
        adjust_interval = self._maker_adjust_interval
        adjust_bps = self._maker_adjust_bps

        ls = self.lighter_feed.snapshot if self.lighter_feed else None
        if not ls or not ls.best_bid or not ls.best_ask:
            self.logger.warning("[V18] Lighter BBO不可用, 回退taker")
            return await self._place_lighter_taker(
                side, qty, ref_price, reduce_only=reduce_only)

        mid = Decimal(str((ls.best_bid + ls.best_ask) / 2))

        coi, ok = await self._place_lighter_maker(
            side, qty, mid, reduce_only=reduce_only)
        if not ok:
            self.logger.warning("[V18] maker挂单失败, 回退taker")
            return await self._place_lighter_taker(
                side, qty, ref_price, reduce_only=reduce_only)

        pre_pos = self.lighter_position
        t0 = time.time()
        current_price = mid
        bn_mid_start = self.binance_feed.mid if (self.binance_feed
                                                  and self.binance_feed.connected) else 0
        adjust_count = 0

        self.logger.info(
            f"[V18] Lighter maker等待: {side} {qty} @ {mid:.2f} "
            f"超时{timeout:.0f}s 调价间隔{adjust_interval:.1f}s/{adjust_bps:.1f}bps")

        while time.time() - t0 < timeout:
            try:
                await asyncio.wait_for(
                    self._lighter_fill_event.wait(), timeout=adjust_interval)
            except asyncio.TimeoutError:
                pass

            if self._lighter_fill_event.is_set():
                fd = self._lighter_fill_data
                if fd is not None:
                    filled_base = fd.get("filled_base_amount")
                    if filled_base is not None:
                        ws_filled = Decimal(str(filled_base))
                        if ws_filled >= qty * Decimal("0.5"):
                            self._extract_lighter_fill_price(fd, ref_price)
                            if side.lower() == "sell":
                                self.lighter_position = pre_pos - ws_filled
                            else:
                                self.lighter_position = pre_pos + ws_filled
                            avg_p = self._last_lighter_avg_price
                            elapsed = time.time() - t0
                            self.logger.info(
                                f"[V18] Lighter maker成交! {ws_filled} "
                                f"@ {avg_p if avg_p else mid:.2f} "
                                f"{elapsed:.1f}s 调价{adjust_count}次 "
                                f"(省taker费用)")
                            return ws_filled

                self._lighter_fill_event.clear()
                self._lighter_fill_data = None

            if bn_mid_start > 0 and self.binance_feed and self.binance_feed.connected:
                bn_drift = (abs(self.binance_feed.mid - bn_mid_start)
                            / bn_mid_start * 10000)
                if bn_drift > self._maker_bn_emergency_bps:
                    elapsed = time.time() - t0
                    self.logger.warning(
                        f"[V18] BN紧急漂移{bn_drift:.1f}bps "
                        f">{self._maker_bn_emergency_bps:.1f}bps "
                        f"({elapsed:.1f}s), 撤maker回退taker")
                    await self._do_lighter_cancel_all()
                    return await self._place_lighter_taker(
                        side, qty, ref_price, reduce_only=reduce_only)

            ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
            if ls_now and ls_now.best_bid and ls_now.best_ask:
                fresh_mid = Decimal(str((ls_now.best_bid + ls_now.best_ask) / 2))
                step = fresh_mid * Decimal(str(adjust_bps)) / Decimal("10000")
                if side.lower() == "buy":
                    current_price = min(fresh_mid + step,
                                        Decimal(str(ls_now.best_ask)))
                else:
                    current_price = max(fresh_mid - step,
                                        Decimal(str(ls_now.best_bid)))
            else:
                step = current_price * Decimal(str(adjust_bps)) / Decimal("10000")
                if side.lower() == "buy":
                    current_price += step
                else:
                    current_price -= step

            ok = await self._modify_lighter_order(coi, current_price, qty)
            if ok:
                adjust_count += 1

        elapsed = time.time() - t0
        self.logger.warning(
            f"[V18] maker超时{elapsed:.1f}s (调价{adjust_count}次), 回退taker")
        await self._do_lighter_cancel_all()
        await asyncio.sleep(0.2)

        new_ls = self.lighter_feed.snapshot if self.lighter_feed else None
        taker_ref = ref_price
        if new_ls and new_ls.best_bid and new_ls.best_ask:
            taker_ref = (Decimal(str(new_ls.best_bid)) if side.lower() == "sell"
                         else Decimal(str(new_ls.best_ask)))
        return await self._place_lighter_taker(
            side, qty, taker_ref, reduce_only=reduce_only)

    async def _hedge_lighter_smart(
        self, side: str, qty: Decimal, ref_price: Decimal,
        est_spread_bps: float,
        reduce_only: bool = False,
    ) -> Optional[Decimal]:
        """Tiered hedge: only use short maker if conditions are ideal,
        otherwise immediate taker. Max naked exposure ~3s, not 45s.
        """
        _min_edge = self._smart_hedge_min_edge
        _max_wait = self._smart_hedge_max_wait
        _bn_stable = True

        if self.binance_feed and self.binance_feed.connected:
            vol = self.binance_feed.realized_vol_bps
            if vol > 2.0:
                _bn_stable = False

        ls = self.lighter_feed.snapshot if self.lighter_feed else None
        _book_ok = (ls and ls.best_bid and ls.best_ask)

        use_maker = (
            est_spread_bps >= _min_edge
            and _bn_stable
            and _book_ok
        )

        if use_maker:
            self.logger.info(
                f"[V18对冲] edge={est_spread_bps:.1f}bps≥{_min_edge:.1f} "
                f"BN稳 → 短maker({_max_wait:.0f}s)")
            result = await self._lighter_maker_with_fallback(
                side, qty, ref_price,
                maker_timeout_s=_max_wait,
                reduce_only=reduce_only)
            self._last_hedge_used_maker = True
            return result
        else:
            reason = []
            if est_spread_bps < _min_edge:
                reason.append(f"edge={est_spread_bps:.1f}<{_min_edge:.1f}")
            if not _bn_stable:
                reason.append("BN波动大")
            if not _book_ok:
                reason.append("L盘口不可用")
            self.logger.info(
                f"[V18对冲] 立即taker ({', '.join(reason)})")
            self._last_hedge_used_maker = False
            return await self._place_lighter_taker(side, qty, ref_price)

    def _extract_lighter_fill_price(self, fd: dict, fallback: Decimal):
        try:
            fq = fd.get("filled_quote_amount")
            fb = fd.get("filled_base_amount")
            if fq is not None and fb is not None:
                quote = Decimal(str(fq))
                base = Decimal(str(fb))
                if base > 0:
                    self._last_lighter_avg_price = quote / base
                    return
        except Exception:
            pass
        self._last_lighter_avg_price = fallback

    # ═══════════════════════════════════════════════
    #  Extended IOC (fallback for --no-maker)
    # ═══════════════════════════════════════════════

    async def _place_extended_ioc(self, side: str, qty: Decimal,
                                   ref_price: Decimal) -> Optional[Decimal]:
        slip = min(Decimal(str(self._max_slippage_bps)),
                   Decimal("50")) / Decimal("10000")
        if side == "buy":
            aggressive = ref_price * (Decimal("1") + slip)
        else:
            aggressive = ref_price * (Decimal("1") - slip)
        aggressive = aggressive.quantize(self.extended_tick_size, rounding=ROUND_HALF_UP)
        qty_r = qty.quantize(self.extended_min_order_size, rounding=ROUND_HALF_UP)

        pre_x_pos = self.extended_position

        self._x_ioc_pending_id = None
        self._x_ioc_confirmed.clear()
        self._x_ioc_fill_qty = Decimal("0")
        self._x_ioc_fill_price = None

        try:
            result = await self.extended_client.place_ioc_order(
                contract_id=self.extended_contract_id,
                quantity=qty_r, side=side, aggressive_price=aggressive)
            if result.success:
                self._x_ioc_pending_id = str(result.order_id)
                try:
                    await asyncio.wait_for(self._x_ioc_confirmed.wait(), timeout=1.5)
                except asyncio.TimeoutError:
                    pass
                if self._x_ioc_fill_qty > 0:
                    actual = self._x_ioc_fill_qty
                    _xb, _xa = self._get_extended_ws_bbo()
                    self._last_x_fill_bbo = (_xb, _xa, side)
                elif self._x_ioc_confirmed.is_set():
                    self._x_ioc_recent_ids.append(self._x_ioc_pending_id)
                    if len(self._x_ioc_recent_ids) > 20:
                        self._x_ioc_recent_ids = self._x_ioc_recent_ids[-10:]
                    self._x_ioc_pending_id = None
                    return Decimal("0")
                else:
                    self.logger.warning("[Extended] IOC WS超时, REST验证中...")
                    try:
                        rest_pos = await self._get_extended_position()
                        delta = (rest_pos - pre_x_pos if side == "buy"
                                 else pre_x_pos - rest_pos)
                        if delta >= qty_r * Decimal("0.5"):
                            actual = abs(delta)
                            _xb, _xa = self._get_extended_ws_bbo()
                            self._last_x_fill_bbo = (_xb, _xa, side)
                        else:
                            self._x_ioc_recent_ids.append(self._x_ioc_pending_id)
                            if len(self._x_ioc_recent_ids) > 20:
                                self._x_ioc_recent_ids = self._x_ioc_recent_ids[-10:]
                            self._x_ioc_pending_id = None
                            return None
                    except Exception:
                        self._x_ioc_recent_ids.append(self._x_ioc_pending_id)
                        if len(self._x_ioc_recent_ids) > 20:
                            self._x_ioc_recent_ids = self._x_ioc_recent_ids[-10:]
                        self._x_ioc_pending_id = None
                        return None
                self._x_ioc_recent_ids.append(self._x_ioc_pending_id)
                if len(self._x_ioc_recent_ids) > 20:
                    self._x_ioc_recent_ids = self._x_ioc_recent_ids[-10:]
                self._x_ioc_pending_id = None
                return actual
            self.logger.error(f"[Extended] IOC 失败: {result.error_message}")
        except Exception as e:
            self.logger.error(f"[Extended] IOC 异常: {e}")
        self._x_ioc_pending_id = None
        return None

    # ═══════════════════════════════════════════════
    #  Extended POST_ONLY Maker
    # ═══════════════════════════════════════════════

    def _calc_proactive_maker_price(
        self, x_side: str,
        l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
        spread_bps: float,
    ) -> Optional[Decimal]:
        """Calculate Extended maker price based on Lighter BBO + desired spread.
        Returns None if price would be invalid (e.g., would cross Extended book).
        """
        sp = Decimal(str(spread_bps)) / Decimal("10000")

        if x_side == "buy":
            target = l_bid * (Decimal("1") - sp)
            post_only_max = x_ask - self.extended_tick_size
            price = min(target, post_only_max)
            floor = x_bid * (Decimal("1") - Decimal("0.005"))
            price = max(price, floor)
        else:
            target = l_ask * (Decimal("1") + sp)
            post_only_min = x_bid + self.extended_tick_size
            price = max(target, post_only_min)
            ceiling = x_ask * (Decimal("1") + Decimal("0.005"))
            price = min(price, ceiling)

        price = price.quantize(self.extended_tick_size, rounding=ROUND_HALF_UP)

        if x_side == "buy" and price >= x_ask:
            return None
        if x_side == "sell" and price <= x_bid:
            return None

        return price

    def _calc_dual_maker_prices(
        self,
        l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
        spread_bps: float,
        l_vwap_sell: Optional[Decimal] = None,
        l_vwap_buy: Optional[Decimal] = None,
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Calculate both buy and sell maker prices with VWAP-aware pricing.
        l_vwap_sell: actual price we'd get selling on Lighter (for buy maker)
        l_vwap_buy: actual price we'd pay buying on Lighter (for sell maker)
        Returns (buy_price, sell_price). Either can be None if invalid.
        """
        sp = Decimal(str(spread_bps)) / Decimal("10000")
        ab = Decimal(str(self._adverse_buffer_bps)) / Decimal("10000")
        tick = self.extended_tick_size

        l_sell_ref = l_vwap_sell if l_vwap_sell else l_bid
        buy_target = l_sell_ref * (Decimal("1") - sp - ab)
        buy_post_only_max = x_ask - tick
        buy_price = min(buy_target, buy_post_only_max)
        buy_floor = x_bid * (Decimal("1") - Decimal("0.005"))
        buy_price = max(buy_price, buy_floor)
        buy_price = buy_price.quantize(tick, rounding=ROUND_HALF_UP)

        l_buy_ref = l_vwap_buy if l_vwap_buy else l_ask
        sell_target = l_buy_ref * (Decimal("1") + sp + ab)
        sell_post_only_min = x_bid + tick
        sell_price = max(sell_target, sell_post_only_min)
        sell_ceiling = x_ask * (Decimal("1") + Decimal("0.005"))
        sell_price = min(sell_price, sell_ceiling)
        sell_price = sell_price.quantize(tick, rounding=ROUND_HALF_UP)

        if buy_price >= x_ask:
            buy_price = None
        if sell_price is not None and sell_price <= x_bid:
            sell_price = None

        if buy_price is not None and sell_price is not None:
            min_gap = Decimal(str(self._min_spread_gap_bps)) / Decimal("10000")
            min_gap_abs = ((buy_price + sell_price) / 2) * min_gap
            min_gap_abs = max(min_gap_abs, tick * 2)

            if buy_price >= sell_price - min_gap_abs:
                mid = (buy_price + sell_price) / 2
                buy_price = (mid - min_gap_abs / 2).quantize(tick, rounding=ROUND_HALF_UP)
                sell_price = (mid + min_gap_abs / 2).quantize(tick, rounding=ROUND_HALF_UP)

                if buy_price >= x_ask or sell_price <= x_bid:
                    return None, None
                if buy_price >= sell_price:
                    return None, None

        return buy_price, sell_price

    async def _place_extended_post_only(
        self, side: str, qty: Decimal, price: Decimal,
    ) -> Optional[str]:
        """Place a POST_ONLY limit order on Extended.
        Returns order_id if placed, None on failure.
        """
        order_side = OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL
        rounded_price = price.quantize(self.extended_tick_size, rounding=ROUND_HALF_UP)
        qty_r = qty.quantize(self.extended_min_order_size, rounding=ROUND_HALF_UP)

        self._x_maker_pending_id = None
        self._x_maker_confirmed.clear()
        self._x_maker_fill_qty = Decimal("0")
        self._x_maker_fill_price = None

        expire = datetime.now(tz=timezone.utc) + timedelta(seconds=60)
        max_attempts = 3

        for attempt in range(max_attempts):
            try:
                result = await self.extended_client.perpetual_trading_client.place_order(
                    market_name=self.extended_contract_id,
                    amount_of_synthetic=qty_r,
                    price=rounded_price,
                    side=order_side,
                    time_in_force=TimeInForce.GTT,
                    post_only=True,
                    expire_time=expire,
                )
                self._record_tx()

                if not result or not result.data or result.status != 'OK':
                    self.logger.warning(
                        f"[Extended Maker] 下单失败 (attempt {attempt+1})")
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(0.1)
                        x_bid_new, x_ask_new = self._get_extended_ws_bbo()
                        if x_bid_new and x_ask_new:
                            if side == "buy":
                                rounded_price = min(rounded_price, x_ask_new - self.extended_tick_size)
                            else:
                                rounded_price = max(rounded_price, x_bid_new + self.extended_tick_size)
                        continue
                    return None

                order_id = str(result.data.id)
                self._x_maker_pending_id = order_id
                self.logger.info(
                    f"[Extended Maker] 挂单成功: {side} {qty_r} @ {rounded_price} "
                    f"id={order_id}")

                await asyncio.sleep(0.05)
                if self._x_maker_confirmed.is_set():
                    if self._x_maker_fill_qty > 0:
                        self.logger.info(
                            f"[Extended Maker] 立即成交: {self._x_maker_fill_qty}")
                    else:
                        self.logger.warning("[Extended Maker] POST_ONLY 被拒绝")
                        if attempt < max_attempts - 1:
                            self._x_maker_pending_id = None
                            self._x_maker_confirmed.clear()
                            x_bid_new, x_ask_new = self._get_extended_ws_bbo()
                            if x_bid_new and x_ask_new:
                                if side == "buy":
                                    rounded_price = x_bid_new
                                else:
                                    rounded_price = x_ask_new
                            continue
                        return None
                return order_id

            except Exception as e:
                self.logger.error(f"[Extended Maker] 异常: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(0.2)
                    continue
                return None
        return None

    async def _wait_maker_fill(
        self, order_id: str, timeout: float,
    ) -> Tuple[Decimal, Optional[Decimal]]:
        """Wait for maker order fill via WS. Returns (filled_qty, fill_price)."""
        if self._x_maker_confirmed.is_set() and self._x_maker_fill_qty > 0:
            return self._x_maker_fill_qty, self._x_maker_fill_price

        deadline = time.time() + timeout
        while time.time() < deadline and not self.stop_flag:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            wait_time = min(0.1, remaining)
            try:
                await asyncio.wait_for(
                    self._x_maker_confirmed.wait(), timeout=wait_time)
                if self._x_maker_fill_qty > 0:
                    return self._x_maker_fill_qty, self._x_maker_fill_price
                else:
                    return Decimal("0"), None
            except asyncio.TimeoutError:
                if self._x_maker_fill_qty > 0:
                    return self._x_maker_fill_qty, self._x_maker_fill_price
                continue

        return self._x_maker_fill_qty, self._x_maker_fill_price

    async def _cancel_extended_order(self, order_id: str) -> Decimal:
        """Cancel an Extended order. Returns filled qty (may be partial)."""
        try:
            await self.extended_client.perpetual_trading_client.orders.cancel_order(
                order_id)
            self.logger.info(f"[Extended Maker] 撤单: id={order_id}")
        except Exception as e:
            self.logger.warning(f"[Extended Maker] 撤单失败: {e}")

        await asyncio.sleep(0.15)
        self._maker_cancel_timestamps.append(time.time())
        filled = self._x_maker_fill_qty
        self._x_maker_recent_ids.append(order_id)
        if len(self._x_maker_recent_ids) > 20:
            self._x_maker_recent_ids = self._x_maker_recent_ids[-10:]
        self._x_maker_pending_id = None
        return filled

    def _reset_maker_order_state(self):
        """Reset single-maker tracking (used by _close_position_mt)."""
        if self._x_maker_pending_id:
            self._x_maker_recent_ids.append(self._x_maker_pending_id)
            if len(self._x_maker_recent_ids) > 20:
                self._x_maker_recent_ids = self._x_maker_recent_ids[-10:]
        self._x_maker_pending_id = None
        self._x_maker_confirmed.clear()
        self._x_maker_fill_qty = Decimal("0")
        self._x_maker_fill_price = None

    # ═══════════════════════════════════════════════
    #  Dual-Sided Maker Helpers
    # ═══════════════════════════════════════════════

    async def _place_dm_order(
        self, side: str, qty: Decimal, price: Decimal,
    ) -> Optional[str]:
        """Place a dual-maker POST_ONLY order on Extended. Returns order_id."""
        order_side = OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL
        rounded_price = price.quantize(self.extended_tick_size, rounding=ROUND_HALF_UP)
        qty_r = qty.quantize(self.extended_min_order_size, rounding=ROUND_HALF_UP)

        expire = datetime.now(tz=timezone.utc) + timedelta(seconds=60)
        try:
            result = await self.extended_client.perpetual_trading_client.place_order(
                market_name=self.extended_contract_id,
                amount_of_synthetic=qty_r,
                price=rounded_price,
                side=order_side,
                time_in_force=TimeInForce.GTT,
                post_only=True,
                expire_time=expire,
            )
            self._record_tx()

            if not result or not result.data or result.status != 'OK':
                self.logger.warning(f"[双向Maker] {side}侧下单失败")
                return None

            order_id = str(result.data.id)
            now = time.time()

            if side == "buy":
                self._x_maker_buy_id = order_id
                self._x_maker_buy_price = rounded_price
                self._x_maker_buy_placed_at = now
                self._x_dm_buy_partial_qty = Decimal("0")
                self._x_dm_buy_partial_price = None
            else:
                self._x_maker_sell_id = order_id
                self._x_maker_sell_price = rounded_price
                self._x_maker_sell_placed_at = now
                self._x_dm_sell_partial_qty = Decimal("0")
                self._x_dm_sell_partial_price = None

            ls_snap = self.lighter_feed.snapshot if self.lighter_feed else None
            if ls_snap and ls_snap.mid > 0:
                self._dm_lighter_mid_at_place = ls_snap.mid
            self._dm_force_cancel = False
            bf = self.binance_feed
            if bf and bf.mid > 0:
                self._bn_mid_at_place = bf.mid
            self._bn_force_cancel = False

            _bn_tag = f" B_mid={self._bn_mid_at_place:.1f}" if self._bn_mid_at_place > 0 else ""
            self.logger.info(
                f"[双向Maker] {side}挂单成功: {qty_r} @ {rounded_price} "
                f"id={order_id} L_mid={self._dm_lighter_mid_at_place:.1f}{_bn_tag}")

            await asyncio.sleep(0.05)
            if side == "buy" and self._x_maker_buy_id is None:
                self.logger.warning(f"[双向Maker] buy POST_ONLY 被拒")
                return None
            if side == "sell" and self._x_maker_sell_id is None:
                self.logger.warning(f"[双向Maker] sell POST_ONLY 被拒")
                return None

            return order_id

        except Exception as e:
            self.logger.error(f"[双向Maker] {side}下单异常: {e}")
            return None

    async def _cancel_dm_order(self, side: str,
                              force: bool = False) -> Decimal:
        """Cancel one side of dual-maker. Returns partial filled qty.
        force=True bypasses fill-event guard (used by _handle_dm_fill).
        """
        if (not force and self._x_dm_fill_event.is_set()
                and self._x_dm_fill_side == side):
            self.logger.info(
                f"[双向Maker] {side}侧已有成交事件, 跳过撤单")
            return Decimal("0")

        order_id = (self._x_maker_buy_id if side == "buy"
                    else self._x_maker_sell_id)
        if not order_id:
            return Decimal("0")

        try:
            await self.extended_client.perpetual_trading_client.orders.cancel_order(
                order_id)
            self.logger.info(f"[双向Maker] 撤{side}单: id={order_id}")
        except Exception as e:
            self.logger.warning(f"[双向Maker] 撤{side}单失败: {e}")
            return Decimal("0")

        await asyncio.sleep(0.15)
        self._maker_cancel_timestamps.append(time.time())

        if side == "buy":
            filled = self._x_dm_buy_partial_qty
            self._x_maker_recent_ids.append(order_id)
            self._x_maker_buy_id = None
        else:
            filled = self._x_dm_sell_partial_qty
            self._x_maker_recent_ids.append(order_id)
            self._x_maker_sell_id = None

        if len(self._x_maker_recent_ids) > 20:
            self._x_maker_recent_ids = self._x_maker_recent_ids[-10:]
        return filled

    async def _cancel_all_dm_orders(self):
        """Cancel both sides of dual-maker orders."""
        tasks = []
        if self._x_maker_buy_id:
            tasks.append(self._cancel_dm_order("buy", force=True))
        if self._x_maker_sell_id:
            tasks.append(self._cancel_dm_order("sell", force=True))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            self._dm_last_full_cancel_ts = time.time()

    def _reset_dm_fill_state(self):
        """Clear the dual-maker fill event and data."""
        self._x_dm_fill_event.clear()
        self._x_dm_fill_side = None
        self._x_dm_fill_qty = Decimal("0")
        self._x_dm_fill_price = None
        self._x_dm_buy_partial_qty = Decimal("0")
        self._x_dm_sell_partial_qty = Decimal("0")
        self._x_dm_buy_partial_price = None
        self._x_dm_sell_partial_price = None

    # ═══════════════════════════════════════════════
    #  Dynamic Threshold (Percentile)
    # ═══════════════════════════════════════════════

    def _record_spread_obs(self, gap1_bps: float, gap2_bps: float):
        self._spread_obs.append((time.time(), gap1_bps, gap2_bps))

    def _calc_dynamic_threshold(self) -> float:
        now = time.time()
        if now - self._dyn_last_calc_ts < self._dyn_recalc_interval:
            return self._dyn_current_threshold

        if len(self._spread_obs) < self._dyn_min_samples:
            return self.target_spread

        cutoff = now - self._dyn_window
        spreads = []
        for ts, g1, g2 in self._spread_obs:
            if ts >= cutoff:
                spreads.append(max(g1, g2))

        if len(spreads) < self._dyn_min_samples:
            return self.target_spread

        spreads.sort()
        idx = min(int(len(spreads) * self._dyn_percentile / 100.0),
                  len(spreads) - 1)
        pct_val = spreads[idx]
        threshold = max(pct_val, self._dyn_fee_floor)
        self._dyn_current_threshold = threshold
        self._dyn_last_calc_ts = now
        return threshold

    # ═══════════════════════════════════════════════
    #  Lighter WS (taker fill confirmation)
    # ═══════════════════════════════════════════════

    async def _lighter_account_ws_loop(self):
        ws_url = "wss://mainnet.zklighter.elliot.ai/stream"
        reconnect_delay = 1
        while not self.stop_flag:
            try:
                async with websockets.connect(ws_url) as ws:
                    auth_token, err = self.lighter_client.create_auth_token_with_expiry(
                        api_key_index=self.api_key_index)
                    if err is not None:
                        self.logger.warning(f"[Lighter WS] auth token 失败: {err}")
                        await asyncio.sleep(5)
                        continue
                    channel = f"account_orders/{self.lighter_market_index}/{self.account_index}"
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": channel,
                        "auth": auth_token,
                    }))
                    self.logger.info("[Lighter WS] account_orders 已连接")
                    reconnect_delay = 1
                    token_ts = time.time()

                    while not self.stop_flag:
                        if time.time() - token_ts > 500:
                            auth_token, err = self.lighter_client.create_auth_token_with_expiry(
                                api_key_index=self.api_key_index)
                            if err is None:
                                await ws.send(json.dumps({
                                    "type": "subscribe",
                                    "channel": channel,
                                    "auth": auth_token,
                                }))
                                token_ts = time.time()

                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            continue

                        data = json.loads(raw)
                        msg_type = data.get("type", "")

                        if msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                        elif msg_type == "update/account_orders":
                            orders = data.get("orders", {}).get(
                                str(self.lighter_market_index), [])
                            for od in orders:
                                status = od.get("status", "").upper()
                                if status not in ("FILLED", "PARTIALLY_FILLED", "CANCELED"):
                                    continue
                                if self._lighter_pending_coi is not None:
                                    ws_coi = od.get("client_order_index")
                                    if (ws_coi is not None
                                            and int(ws_coi) != self._lighter_pending_coi):
                                        continue
                                self._lighter_fill_data = od
                                self._lighter_fill_event.set()

            except (websockets.exceptions.ConnectionClosed, OSError) as exc:
                self.logger.warning(f"[Lighter WS] 断开: {exc}")
            except Exception as exc:
                self.logger.error(f"[Lighter WS] 错误: {exc}")

            if not self.stop_flag:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)

    # ═══════════════════════════════════════════════
    #  Cancel Helpers
    # ═══════════════════════════════════════════════

    async def _do_lighter_cancel_all(self) -> bool:
        if self.dry_run or not self.lighter_client:
            return True
        for attempt in range(3):
            try:
                future_ts_ms = int((time.time() + 300) * 1000)
                _, _, error = await self.lighter_client.cancel_all_orders(
                    time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                    timestamp_ms=future_ts_ms)
                self._record_tx()
                if error is None:
                    return True
                self._check_nonce_error(error)
            except Exception as e:
                self._check_nonce_error(e)
            await asyncio.sleep(0.5)
        self.logger.error("[Lighter] cancel_all 3次重试均失败")
        return False

    async def _cancel_all_extended_orders_rest(self):
        if self.dry_run or not self.extended_client:
            return
        try:
            data = await self.extended_client.perpetual_trading_client.account.get_open_orders(
                market_names=[self.extended_contract_id])
            if not data or not hasattr(data, "data") or not data.data:
                return
            for order in data.data:
                if order.status in ("NEW", "OPEN", "PARTIALLY_FILLED"):
                    oid = str(order.id)
                    self.logger.info(f"[清理] 撤销遗留Extended挂单 id={oid}")
                    try:
                        await self.extended_client.perpetual_trading_client.orders.cancel_order(oid)
                    except Exception as ce:
                        self.logger.warning(f"[清理] 撤单失败 id={oid}: {ce}")
                    await asyncio.sleep(0.15)
        except Exception as e:
            self.logger.warning(f"[清理] REST 查询Extended挂单失败: {e}")

    # ═══════════════════════════════════════════════
    #  Position Queries
    # ═══════════════════════════════════════════════

    async def _get_aio_session(self) -> aiohttp.ClientSession:
        if self._aio_session is None or self._aio_session.closed:
            conn = aiohttp.TCPConnector(limit=10, keepalive_timeout=60)
            self._aio_session = aiohttp.ClientSession(
                connector=conn,
                timeout=aiohttp.ClientTimeout(total=5),
                headers={"accept": "application/json"},
            )
        return self._aio_session

    async def _get_lighter_position(self) -> Decimal:
        url = f"{self.lighter_base_url}/api/v1/account"
        params = {"by": "index", "value": self.account_index}
        try:
            session = await self._get_aio_session()
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                for pos in data.get("accounts", [{}])[0].get("positions", []):
                    if pos.get("symbol") == self.symbol:
                        return Decimal(str(pos["position"])) * int(pos["sign"])
            return Decimal("0")
        except Exception as e:
            self.logger.error(f"查询 Lighter 仓位出错: {e}")
            return self.lighter_position

    async def _get_extended_position(self) -> Decimal:
        try:
            data = await self.extended_client.perpetual_trading_client.account.get_positions(
                market_names=[f"{self.symbol}-USD"])
            if not data or not hasattr(data, "data") or not data.data:
                return Decimal("0")
            for p in data.data:
                if p.market == self.extended_contract_id:
                    side_mult = Decimal("1") if p.side.lower() == "long" else Decimal("-1")
                    return Decimal(str(p.size)) * side_mult
            return Decimal("0")
        except Exception as e:
            self.logger.error(f"查询 Extended 仓位出错: {e}")
            return self.extended_position

    async def _get_extended_entry_price(self) -> Optional[Decimal]:
        try:
            data = await self.extended_client.perpetual_trading_client.account.get_positions(
                market_names=[f"{self.symbol}-USD"])
            if not data or not hasattr(data, "data") or not data.data:
                return None
            for p in data.data:
                if p.market != self.extended_contract_id:
                    continue
                for attr in (
                    "entry_price", "entryPrice", "average_entry_price",
                    "averageEntryPrice", "avg_entry_price", "open_price",
                    "avgEntryPrice",
                ):
                    raw = getattr(p, attr, None)
                    if raw is not None:
                        try:
                            pr = Decimal(str(raw))
                            if pr > 0:
                                return pr
                        except Exception:
                            pass
                return None
            return None
        except Exception:
            return None

    async def _use_extended_fallback_price(
            self, fallback: Decimal, is_close: bool) -> None:
        if not is_close:
            await asyncio.sleep(0.05)
            entry_price = await self._get_extended_entry_price()
            if entry_price is not None and entry_price > 0:
                diff_bps = abs(float(
                    (entry_price - fallback) / fallback * 10000
                )) if fallback > 0 else 0
                if diff_bps < 100:
                    self._last_extended_avg_price = entry_price
                    return
        bbo = getattr(self, "_last_x_fill_bbo", None)
        if bbo:
            bid, ask, side = bbo
            if side == "buy" and ask and ask > 0:
                self._last_extended_avg_price = ask
                return
            if side == "sell" and bid and bid > 0:
                self._last_extended_avg_price = bid
                return
        self._last_extended_avg_price = fallback

    async def _resolve_extended_fill_price(
            self, x_ref: Decimal, is_close: bool) -> None:
        self._last_extended_avg_price = None
        if (self._x_ioc_fill_price is not None
                and self._x_ioc_fill_price > 0):
            diff = abs(float(
                (self._x_ioc_fill_price - x_ref) / x_ref * 10000
            )) if x_ref > 0 else 0
            if diff < 200:
                self._last_extended_avg_price = self._x_ioc_fill_price
                return
        await self._use_extended_fallback_price(x_ref, is_close)

    async def _resolve_maker_fill_price(
            self, x_ref: Decimal, is_close: bool) -> None:
        """Resolve fill price for maker orders (use WS avg_price directly)."""
        self._last_extended_avg_price = None
        if (self._x_maker_fill_price is not None
                and self._x_maker_fill_price > 0):
            diff = abs(float(
                (self._x_maker_fill_price - x_ref) / x_ref * 10000
            )) if x_ref > 0 else 0
            if diff < 200:
                self._last_extended_avg_price = self._x_maker_fill_price
                return
        await self._use_extended_fallback_price(x_ref, is_close)

    async def _reconcile_positions(self) -> bool:
        l = await self._get_lighter_position()
        x = await self._get_extended_position()
        self.lighter_position = l
        self.extended_position = x
        net = l + x
        balanced = abs(net) < self.order_size * Decimal("0.5")
        if not balanced:
            self.logger.warning(f"[对账] 不平衡: L={l} X={x} net={net}")
        return balanced

    # ═══════════════════════════════════════════════
    #  Spread Detection
    # ═══════════════════════════════════════════════

    def _check_open_opportunity(
        self, l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
    ) -> Optional[Tuple[str, str, str, Decimal, Decimal, float]]:
        gap_sell_l = l_bid - x_ask
        ref1 = (l_bid + x_ask) / 2
        gap1_bps = float(gap_sell_l / ref1 * 10000) if ref1 > 0 else 0.0

        gap_buy_l = x_bid - l_ask
        ref2 = (x_bid + l_ask) / 2
        gap2_bps = float(gap_buy_l / ref2 * 10000) if ref2 > 0 else 0.0

        threshold = self.target_spread
        if self.maker_mode:
            threshold = self._calc_dynamic_threshold()

        if gap1_bps >= threshold and gap1_bps >= gap2_bps:
            return ("long_X_short_L", "sell", "buy", l_bid, x_ask, gap1_bps)
        elif gap2_bps >= threshold:
            return ("long_L_short_X", "buy", "sell", l_ask, x_bid, gap2_bps)
        return None

    def _calc_directional_gaps(
        self, l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
    ) -> Tuple[float, float]:
        """Return (gap1_bps, gap2_bps) for both directions."""
        ref1 = (l_bid + x_ask) / 2
        gap1 = float((l_bid - x_ask) / ref1 * 10000) if ref1 > 0 else 0.0
        ref2 = (x_bid + l_ask) / 2
        gap2 = float((x_bid - l_ask) / ref2 * 10000) if ref2 > 0 else 0.0
        return gap1, gap2

    def _get_current_gap_bps(
        self, l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
    ) -> float:
        if self.position_direction == "long_X_short_L":
            current_gap = l_ask - x_bid
            ref = x_bid if x_bid > 0 else l_ask
        elif self.position_direction == "long_L_short_X":
            current_gap = x_ask - l_bid
            ref = l_bid if l_bid > 0 else x_ask
        elif self.extended_position > 0:
            current_gap = l_ask - x_bid
            ref = x_bid if x_bid > 0 else l_ask
        elif self.extended_position < 0:
            current_gap = x_ask - l_bid
            ref = l_bid if l_bid > 0 else x_ask
        else:
            return 999.0
        return float(current_gap / ref * 10000) if ref > 0 else 999.0

    def _check_close_condition(
        self, l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
    ) -> Optional[Tuple[float, float]]:
        gap_bps = self._get_current_gap_bps(l_bid, l_ask, x_bid, x_ask)
        for tier_spread, cum_pct in self.close_tiers:
            if gap_bps <= tier_spread and self._tier_closed_pct < cum_pct:
                batch_pct = cum_pct - self._tier_closed_pct
                return (tier_spread, batch_pct)
        return None

    # ═══════════════════════════════════════════════
    #  Open Position — Dual Taker (V14, for --no-maker)
    # ═══════════════════════════════════════════════

    async def _open_position(
        self, direction: str, l_side: str, x_side: str,
        l_ref: Decimal, x_ref: Decimal, qty: Decimal,
        spread_bps: float,
    ) -> bool:
        async with self._trade_lock:
            ls2 = self.lighter_feed.snapshot
            x2_bid, x2_ask = self._get_extended_ws_bbo()
            if (ls2.best_bid is not None and ls2.best_ask is not None
                    and x2_bid is not None and x2_ask is not None):
                l2_bid = Decimal(str(ls2.best_bid))
                l2_ask = Decimal(str(ls2.best_ask))
                recheck = self._check_open_opportunity(l2_bid, l2_ask, x2_bid, x2_ask)
                if recheck is None:
                    self.logger.info("[开仓] 二次验证价差已消失, 放弃")
                    return False
                if recheck[0] != direction:
                    self.logger.info(f"[开仓] 二次验证方向翻转 {direction}→{recheck[0]}, 放弃")
                    return False
                _, _, _, l_ref, x_ref, spread_bps = recheck

            l_vwap, l_depth = self._get_vwap("lighter", l_side, float(qty))
            x_vwap, x_depth = self._get_vwap("extended", x_side, float(qty))

            if l_vwap is None or x_vwap is None:
                self.logger.info(
                    f"[开仓] 深度不足无法100%成交: L深度={l_depth} "
                    f"X深度={x_depth} 需要={qty}, 跳过")
                return False

            if direction == "long_X_short_L":
                vwap_spread = float((l_vwap - x_vwap) / l_vwap * 10000)
            else:
                vwap_spread = float((x_vwap - l_vwap) / l_vwap * 10000)
            _open_fee = self.PAIR_FEE_TT
            if vwap_spread < _open_fee:
                self.logger.info(
                    f"[开仓] VWAP价差={vwap_spread:.1f}bps < "
                    f"费用{_open_fee}bps (L_VWAP={l_vwap:.1f} "
                    f"X_VWAP={x_vwap:.1f}), 跳过")
                return False
            l_ref = l_vwap
            x_ref = x_vwap
            spread_bps = vwap_spread

            t0 = time.time()
            results = await asyncio.gather(
                self._place_extended_ioc(x_side, qty, x_ref),
                self._place_lighter_taker(l_side, qty, l_ref),
                return_exceptions=True,
            )
            x_fill = results[0] if not isinstance(results[0], Exception) else None
            l_fill = results[1] if not isinstance(results[1], Exception) else None
            if isinstance(results[0], Exception):
                self.logger.error(f"[开仓] Extended异常: {results[0]}")
            if isinstance(results[1], Exception):
                self.logger.error(f"[开仓] Lighter异常: {results[1]}")

            total_ms = (time.time() - t0) * 1000
            x_ok = x_fill is not None and x_fill > 0
            l_ok = l_fill is not None and l_fill > 0

            if x_ok and l_ok:
                hedge_qty = min(x_fill, l_fill)
                if direction == "long_L_short_X":
                    self.extended_position -= x_fill
                else:
                    self.extended_position += x_fill

                await self._resolve_extended_fill_price(x_ref, is_close=False)
                l_fill_price = self._last_lighter_avg_price or l_ref
                x_fill_price = self._last_extended_avg_price or x_ref
                x_estimated = (self._last_extended_avg_price is None
                               or self._last_extended_avg_price == x_ref)

                self._open_route_fee_bps = self.PAIR_FEE_TT
                self._record_open(direction, l_fill_price, l_fill,
                                  x_fill_price, x_fill, spread_bps, x_estimated)

                self._log_open_result(direction, l_side, x_side, l_ref, x_ref,
                                      l_fill_price, x_fill_price, l_fill, x_fill,
                                      spread_bps, total_ms, x_estimated, "taker")

                self._last_trade_time = time.time()
                self._last_ladder_attempt = time.time()

                if l_fill != x_fill:
                    await self._handle_fill_imbalance_open(
                        direction, l_side, x_side, l_ref, x_ref,
                        l_fill, x_fill, hedge_qty)
                return True

            elif x_ok and not l_ok:
                if direction == "long_L_short_X":
                    self.extended_position -= x_fill
                else:
                    self.extended_position += x_fill
                self.logger.error(
                    f"[开仓] Lighter未成交, 先尝试L对冲(免费) "
                    f"{l_side} {x_fill} ({total_ms:.0f}ms)")
                ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
                retry_ref = l_ref
                if ls_now and ls_now.best_bid and ls_now.best_ask:
                    retry_ref = (Decimal(str(ls_now.best_bid)) if l_side == "sell"
                                 else Decimal(str(ls_now.best_ask)))
                l_retry = await self._place_lighter_taker(l_side, x_fill, retry_ref)
                if not (l_retry and l_retry > 0):
                    self.logger.warning("[开仓] L对冲也失败, 回撤X")
                    undo_x_side = "sell" if x_side == "buy" else "buy"
                    xb, xa = self._get_extended_ws_bbo()
                    undo_x_ref = (xb if undo_x_side == "sell" else xa) or x_ref
                    undo_fill = await self._place_extended_ioc(undo_x_side, x_fill, undo_x_ref)
                    if undo_fill and undo_fill > 0:
                        if direction == "long_L_short_X":
                            self.extended_position += undo_fill
                        else:
                            self.extended_position -= undo_fill
                    else:
                        self._force_reconcile = True
                return False

            elif l_ok and not x_ok:
                self.logger.error(f"[开仓] Extended未成交, 回撤Lighter ({total_ms:.0f}ms)")
                undo_l_side = "sell" if l_side == "buy" else "buy"
                ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
                undo_l_ref = l_ref
                if ls_now and ls_now.best_bid and ls_now.best_ask:
                    undo_l_ref = (Decimal(str(ls_now.best_bid)) if undo_l_side == "sell"
                                  else Decimal(str(ls_now.best_ask)))
                undo_fill = await self._place_lighter_taker(undo_l_side, l_fill, undo_l_ref)
                if not (undo_fill and undo_fill > 0):
                    self._force_reconcile = True
                return False

            else:
                self.logger.warning(f"[开仓] 两边都未成交 ({total_ms:.0f}ms)")
                return False

    def _record_open(self, direction, l_price, l_qty, x_price, x_qty,
                     spread_bps, x_estimated):
        self._open_l_refs.append((l_price, l_qty))
        self._open_x_refs.append((x_price, x_qty))
        if self.position_opened_at is None:
            self.position_opened_at = time.time()
            self._tier_closed_pct = 0.0
            self._tier_close_records.clear()
        self.position_direction = direction
        self._last_open_spread_bps = max(spread_bps, self.target_spread)
        self._total_open_qty = min(
            abs(self.extended_position), abs(self.lighter_position))
        self._open_theoretical_spread = spread_bps

    def _log_open_result(self, direction, l_side, x_side, l_ref, x_ref,
                         l_fill_price, x_fill_price, l_fill, x_fill,
                         spread_bps, total_ms, x_estimated, mode_tag):
        if direction == "long_X_short_L":
            actual_sp = float(
                (l_fill_price - x_fill_price) / l_fill_price * 10000
            ) if l_fill_price > 0 else 0.0
        else:
            actual_sp = float(
                (x_fill_price - l_fill_price) / l_fill_price * 10000
            ) if l_fill_price > 0 else 0.0
        sp_q = ("OK" if actual_sp >= self.PAIR_FEE_TT
                else "LOW" if actual_sp > 0 else "NEG")

        if l_side == "buy":
            dev_l = float((l_fill_price - l_ref) / l_ref * 10000) if l_ref > 0 else 0.0
        else:
            dev_l = float((l_ref - l_fill_price) / l_ref * 10000) if l_ref > 0 else 0.0
        if x_side == "buy":
            dev_x = float((x_fill_price - x_ref) / x_ref * 10000) if x_ref > 0 else 0.0
        else:
            dev_x = float((x_ref - x_fill_price) / x_ref * 10000) if x_ref > 0 else 0.0
        excess = spread_bps - self.target_spread

        self._open_dev_l_bps = dev_l
        self._open_dev_x_bps = dev_x
        self._open_excess_bps = excess
        est_tag = "(估)" if x_estimated else ""

        tid = self._generate_trade_id()
        self._current_trade_id = tid
        self._open_hedge_latency_ms = total_ms
        self._open_x_side = x_side
        self._open_route_tag = mode_tag
        _bn_vol = (self.binance_feed.realized_vol_bps
                   if self.binance_feed and self.binance_feed.connected else 0.0)
        self._open_bn_vol = _bn_vol
        _hedge_mk = "Y" if self._last_hedge_used_maker else "N"

        self.logger.info(
            f"[开仓成功|{mode_tag}|{tid}] {total_ms:.0f}ms "
            f"实际价差={actual_sp:.1f}bps({sp_q}) "
            f"L_{l_side}={l_fill_price:.2f}x{l_fill} "
            f"X_{x_side}={x_fill_price:.2f}{est_tag}x{x_fill} "
            f"偏移:L={dev_l:+.2f}bps X={dev_x:+.2f}bps "
            f"超额={excess:+.2f}bps 费={self._open_route_fee_bps:.1f}bps "
            f"对冲maker={_hedge_mk} BNvol={_bn_vol:.1f}bps")

        self._csv_row([
            datetime.now(timezone(timedelta(hours=8))).isoformat(),
            "OPEN", direction,
            l_side, f"{l_ref:.2f}", f"{l_fill_price:.2f}", str(l_fill),
            x_side, f"{x_ref:.2f}", f"{x_fill_price:.2f}", str(x_fill),
            f"{spread_bps:.2f}", f"{total_ms:.0f}", mode_tag,
            f"{dev_l:.2f}", f"{dev_x:.2f}", f"{excess:.2f}", "",
            "", "",
            tid, mode_tag, f"{self._open_route_fee_bps:.2f}", "", "",
            f"{total_ms:.0f}", _hedge_mk,
            x_side, f"{_bn_vol:.2f}",
            "", "",
        ])

        if self._markout_task and not self._markout_task.done():
            self._markout_task.cancel()
        _fill_mid = float(x_fill_price) if x_fill_price else 0.0
        self._markout_task = asyncio.create_task(
            self._record_markout(tid, x_side, _fill_mid, direction))

    async def _handle_fill_imbalance_open(self, direction, l_side, x_side,
                                           l_ref, x_ref, l_fill, x_fill,
                                           hedge_qty):
        excess_l = l_fill - hedge_qty
        excess_x = x_fill - hedge_qty
        min_excess = self.order_size * Decimal("0.01")
        if excess_l > min_excess:
            self.logger.warning(f"[开仓] L多成交{excess_l}, 回撤")
            undo_l_side = "sell" if l_side == "buy" else "buy"
            ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
            undo_l_ref = l_ref
            if ls_now and ls_now.best_bid and ls_now.best_ask:
                undo_l_ref = (Decimal(str(ls_now.best_bid)) if undo_l_side == "sell"
                              else Decimal(str(ls_now.best_ask)))
            undo = await self._place_lighter_taker(undo_l_side, excess_l, undo_l_ref)
            if not (undo and undo > 0):
                self._force_reconcile = True
        elif excess_x > min_excess:
            self.logger.warning(f"[开仓] X多成交{excess_x}, L补仓(免费)")
            ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
            add_l_ref = l_ref
            if ls_now and ls_now.best_bid and ls_now.best_ask:
                add_l_ref = (Decimal(str(ls_now.best_bid)) if l_side == "sell"
                             else Decimal(str(ls_now.best_ask)))
            add_ok = await self._place_lighter_taker(l_side, excess_x, add_l_ref)
            if not (add_ok and add_ok > 0):
                self.logger.warning("[开仓] L补仓失败, 回撤X")
                undo_x_side = "sell" if x_side == "buy" else "buy"
                xb, xa = self._get_extended_ws_bbo()
                undo_x_ref = (xb if undo_x_side == "sell" else xa) or x_ref
                undo = await self._place_extended_ioc(undo_x_side, excess_x, undo_x_ref)
                if undo and undo > 0:
                    if direction == "long_L_short_X":
                        self.extended_position += undo
                    else:
                        self.extended_position -= undo
                else:
                    self._force_reconcile = True

    # ═══════════════════════════════════════════════
    #  Hedge After Proactive Maker Fill (NEW)
    # ═══════════════════════════════════════════════

    async def _hedge_after_maker_fill(
        self, direction: str, x_side: str, filled_qty: Decimal,
        fill_price: Decimal, spread_bps: float,
    ) -> bool:
        """After Extended maker fills, immediately hedge on Lighter.
        Returns True if hedge succeeded and position is established.
        """
        async with self._trade_lock:
            t0 = time.time()

            if direction == "long_X_short_L":
                l_side = "sell"
                self.extended_position += filled_qty
            else:
                l_side = "buy"
                self.extended_position -= filled_qty

            l_vwap, l_total_depth = self._get_vwap(
                "lighter", l_side, float(filled_qty))

            if l_vwap is None:
                self.logger.error(
                    f"[Maker对冲] Lighter深度不足! 需要{filled_qty} "
                    f"可用深度={l_total_depth} 无法100%对冲 → 紧急平仓")
                self._force_reconcile = True
                return False

            l_ref = l_vwap
            hedge_qty = filled_qty

            if fill_price and l_vwap > 0:
                if direction == "long_X_short_L":
                    est_sp = float((l_vwap - fill_price) / l_vwap * 10000)
                else:
                    est_sp = float((fill_price - l_vwap) / l_vwap * 10000)
                ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
                l1_price = l_vwap
                if ls_now:
                    l1_price = (Decimal(str(ls_now.best_bid)) if l_side == "sell"
                                else Decimal(str(ls_now.best_ask)))
                slip = abs(float((l_vwap - l1_price) / l1_price * 10000)
                           ) if l1_price > 0 else 0
                self.logger.info(
                    f"[Maker对冲] VWAP={l_vwap:.2f} L1={l1_price:.2f} "
                    f"滑点={slip:.1f}bps 预估价差={est_sp:.1f}bps "
                    f"深度={l_total_depth}")
                if est_sp < -self._max_slippage_bps:
                    self.logger.error(
                        f"[Maker对冲] 价格保护! VWAP价差={est_sp:.1f}bps "
                        f"超过-{self._max_slippage_bps}bps → 紧急平仓")
                    self._force_reconcile = True
                    return False

            if hedge_qty < self.order_size * Decimal("0.05"):
                self.logger.error(
                    f"[Maker对冲] 对冲量太小 {hedge_qty}, 紧急平仓")
                self._force_reconcile = True
                return False

            l_fill = await self._hedge_lighter_smart(
                l_side, hedge_qty, l_ref, est_sp)
            total_ms = (time.time() - t0) * 1000

            if l_fill and l_fill > 0:
                l_fill_price = self._last_lighter_avg_price or l_ref
                x_fill_price = fill_price or l_ref

                self._open_route_fee_bps = (
                    self.PAIR_FEE_MM if self._last_hedge_used_maker
                    else self.PAIR_FEE_MT)
                self._record_open(direction, l_fill_price, l_fill,
                                  x_fill_price, filled_qty, spread_bps, False)

                _open_tag = "maker-maker" if self._last_hedge_used_maker else "maker-taker"
                x_ref = fill_price
                self._log_open_result(
                    direction, l_side, x_side, l_ref, x_ref,
                    l_fill_price, x_fill_price, l_fill, filled_qty,
                    spread_bps, total_ms, False, _open_tag)

                self._last_trade_time = time.time()
                self._last_ladder_attempt = time.time()
                self._consecutive_hedge_failures = 0

                if l_fill < filled_qty:
                    excess = filled_qty - l_fill
                    if excess > self.order_size * Decimal("0.01"):
                        self.logger.warning(
                            f"[Maker对冲] Lighter成交不足 {l_fill}<{filled_qty}, "
                            f"差额{excess} 触发对账")
                        self._force_reconcile = True

                return True
            else:
                self.logger.error(
                    f"[Maker对冲] Lighter taker 失败! {total_ms:.0f}ms "
                    f"Extended已成交{filled_qty} 需紧急平仓")
                self._consecutive_hedge_failures += 1
                if self._consecutive_hedge_failures >= self._max_hedge_failures:
                    self._hedge_circuit_breaker_until = (
                        time.time() + self._hedge_circuit_pause)
                    self.logger.error(
                        f"[熔断] 连续{self._consecutive_hedge_failures}次对冲失败! "
                        f"暂停maker {self._hedge_circuit_pause}s")
                self._force_reconcile = True
                return False

    # ═══════════════════════════════════════════════
    #  Dual-Maker Fill + Hedge
    # ═══════════════════════════════════════════════

    async def _handle_dm_fill(self) -> bool:
        """Handle a dual-maker fill event:
        1. Cancel the opposite side (get partial fill info)
        2. Compute net exposure
        3. Hedge on Lighter
        Returns True if position is established.
        """
        async with self._trade_lock:
            return await self._handle_dm_fill_inner()

    async def _handle_dm_fill_inner(self) -> bool:
        fill_side = self._x_dm_fill_side
        fill_qty = self._x_dm_fill_qty
        fill_price = self._x_dm_fill_price
        if not fill_side or fill_qty <= 0:
            self.logger.warning(
                f"[双向Maker] fill event数据无效: side={fill_side} "
                f"qty={fill_qty}, 清除event")
            self._reset_dm_fill_state()
            return False

        other_side = "sell" if fill_side == "buy" else "buy"
        self.logger.info(
            f"[双向Maker成交] {fill_side}侧成交 qty={fill_qty} price={fill_price} "
            f"→ 撤{other_side}侧")

        other_partial = await self._cancel_dm_order(other_side, force=True)
        if fill_side == "buy":
            self._cancel_dm_order_sync_buy()
        else:
            self._cancel_dm_order_sync_sell()

        if other_partial > 0:
            self.logger.info(
                f"[双向Maker成交] 撤{other_side}侧时已部分成交 {other_partial}")

        if fill_side == "buy":
            direction = "long_X_short_L"
            x_side = "buy"
            net_qty = fill_qty - other_partial
            self.extended_position += fill_qty
            if other_partial > 0:
                self.extended_position -= other_partial
        else:
            direction = "long_L_short_X"
            x_side = "sell"
            net_qty = fill_qty - other_partial
            self.extended_position -= fill_qty
            if other_partial > 0:
                self.extended_position += other_partial

        net_qty = net_qty.quantize(self.extended_min_order_size, rounding=ROUND_DOWN)

        if net_qty <= self.order_size * Decimal("0.01"):
            self.logger.info(
                "[双向Maker] 双侧成交抵消，净敞口≈0 → 回到IDLE")
            fee_cost = float(fill_qty + other_partial) * 0.05
            self.logger.info(f"[双向Maker] 双侧成交费用损失≈{fee_cost:.4f}")
            self._reset_dm_fill_state()
            return False

        l_side = "sell" if fill_side == "buy" else "buy"

        l_vwap, l_total_depth = self._get_vwap(
            "lighter", l_side, float(net_qty))
        ls_now = self.lighter_feed.snapshot if self.lighter_feed else None

        if l_vwap is None:
            self.logger.error(
                f"[双向Maker对冲] Lighter深度不足! 需要{net_qty} "
                f"可用深度={l_total_depth} 无法100%对冲 → 触发紧急对账")
            self._force_reconcile = True
            self._reset_dm_fill_state()
            return False

        l_ref = l_vwap
        hedge_qty = net_qty

        if fill_price and l_vwap > 0:
            if direction == "long_X_short_L":
                est_spread = float((l_vwap - fill_price) / l_vwap * 10000)
            else:
                est_spread = float((fill_price - l_vwap) / l_vwap * 10000)
            l1_price = (Decimal(str(ls_now.best_bid)) if l_side == "sell"
                        else Decimal(str(ls_now.best_ask))) if ls_now else l_vwap
            slip_bps = abs(float((l_vwap - l1_price) / l1_price * 10000)
                          ) if l1_price > 0 else 0
            self.logger.info(
                f"[双向Maker对冲] VWAP={l_vwap:.2f} L1={l1_price:.2f} "
                f"滑点={slip_bps:.1f}bps 预估价差={est_spread:.1f}bps "
                f"深度={l_total_depth}")
            if est_spread < 0:
                _recovery_ms = self._recovery_window_ms
                _cat_threshold = -self._neg_spread_reverse_bps
                if est_spread < _cat_threshold:
                    self.logger.warning(
                        f"[对冲] 严重逆选={est_spread:.1f}bps "
                        f"< {_cat_threshold:.1f}bps → 直接scratch")
                else:
                    self.logger.info(
                        f"[对冲] 负价差={est_spread:.1f}bps → "
                        f"进入recovery窗口{_recovery_ms:.0f}ms")
                    _rec_start = time.time()
                    _recovered = False
                    while (time.time() - _rec_start) * 1000 < _recovery_ms:
                        await asyncio.sleep(0.05)
                        ls_rec = (self.lighter_feed.snapshot
                                  if self.lighter_feed else None)
                        if ls_rec and ls_rec.best_bid and ls_rec.best_ask:
                            l_rec_ref = (Decimal(str(ls_rec.best_bid))
                                         if l_side == "sell"
                                         else Decimal(str(ls_rec.best_ask)))
                            if fill_price and l_rec_ref > 0:
                                if direction == "long_X_short_L":
                                    rec_spread = float(
                                        (l_rec_ref - fill_price)
                                        / l_rec_ref * 10000)
                                else:
                                    rec_spread = float(
                                        (fill_price - l_rec_ref)
                                        / l_rec_ref * 10000)
                                if rec_spread >= 0:
                                    self.logger.info(
                                        f"[对冲] recovery成功! "
                                        f"价差恢复到{rec_spread:.1f}bps "
                                        f"({(time.time()-_rec_start)*1000:.0f}ms)")
                                    l_ref = l_rec_ref
                                    est_spread = rec_spread
                                    _recovered = True
                                    break
                    if _recovered:
                        pass
                    else:
                        self.logger.warning(
                            f"[对冲] recovery失败 {_recovery_ms:.0f}ms内未恢复 "
                            f"→ scratch Extended")

                if est_spread < 0:
                    unwind_side = "sell" if fill_side == "buy" else "buy"
                    unwind_ref = fill_price or l_vwap
                    try:
                        uw_fill = await self._place_extended_ioc(
                            unwind_side, net_qty, unwind_ref)
                        if uw_fill and uw_fill > 0:
                            if fill_side == "buy":
                                self.extended_position -= uw_fill
                            else:
                                self.extended_position += uw_fill
                            self.logger.info(
                                f"[对冲] scratch成功 {unwind_side} "
                                f"{uw_fill} (逆选{est_spread:.1f}bps)")
                        else:
                            self.logger.error(
                                "[对冲] scratch失败 → 对账")
                            self._force_reconcile = True
                    except Exception as e:
                        self.logger.error(
                            f"[对冲] scratch异常: {e}")
                        self._force_reconcile = True
                    self._reset_dm_fill_state()
                    return False

        t0 = time.time()
        l_fill = await self._hedge_lighter_smart(
            l_side, hedge_qty, l_ref, est_spread)
        total_ms = (time.time() - t0) * 1000

        if l_fill and l_fill > 0:
            l_fill_price = self._last_lighter_avg_price or l_ref
            x_fill_price = fill_price or l_ref

            spread_bps = 0.0
            if direction == "long_X_short_L":
                if l_fill_price > 0:
                    spread_bps = float(
                        (l_fill_price - x_fill_price) / l_fill_price * 10000)
            else:
                if l_fill_price > 0:
                    spread_bps = float(
                        (x_fill_price - l_fill_price) / l_fill_price * 10000)

            self._open_route_fee_bps = (
                self.PAIR_FEE_MM if self._last_hedge_used_maker
                else self.PAIR_FEE_MT)
            self._record_open(direction, l_fill_price, l_fill,
                              x_fill_price, net_qty, spread_bps, False)

            _open_tag = "maker-maker" if self._last_hedge_used_maker else "maker-taker"
            x_ref = fill_price or Decimal("0")
            self._log_open_result(
                direction, l_side, x_side, l_ref, x_ref,
                l_fill_price, x_fill_price, l_fill, net_qty,
                spread_bps, total_ms, False, _open_tag)

            self._last_trade_time = time.time()
            self._last_ladder_attempt = time.time()
            self._consecutive_hedge_failures = 0

            if l_fill < net_qty:
                excess = net_qty - l_fill
                if excess > self.order_size * Decimal("0.01"):
                    self.logger.warning(
                        f"[双向Maker对冲] Lighter成交不足 {l_fill}<{net_qty} "
                        f"差额{excess} 触发对账")
                    self._force_reconcile = True

            self._reset_dm_fill_state()
            return True
        else:
            self.logger.error(
                f"[双向Maker对冲] Lighter taker失败! {total_ms:.0f}ms "
                f"Extended净敞口{net_qty} 需紧急处理")
            self._consecutive_hedge_failures += 1
            if self._consecutive_hedge_failures >= self._max_hedge_failures:
                self._hedge_circuit_breaker_until = (
                    time.time() + self._hedge_circuit_pause)
                self.logger.error(
                    f"[熔断] 连续{self._consecutive_hedge_failures}次对冲失败! "
                    f"暂停maker {self._hedge_circuit_pause}s")
            self._force_reconcile = True
            self._reset_dm_fill_state()
            return False

    def _cancel_dm_order_sync_buy(self):
        """Sync-cleanup buy side state after fill processing."""
        oid = self._x_maker_buy_id
        if oid:
            self._x_maker_recent_ids.append(oid)
            if len(self._x_maker_recent_ids) > 20:
                self._x_maker_recent_ids = self._x_maker_recent_ids[-10:]
        self._x_maker_buy_id = None

    def _cancel_dm_order_sync_sell(self):
        """Sync-cleanup sell side state after fill processing."""
        oid = self._x_maker_sell_id
        if oid:
            self._x_maker_recent_ids.append(oid)
            if len(self._x_maker_recent_ids) > 20:
                self._x_maker_recent_ids = self._x_maker_recent_ids[-10:]
        self._x_maker_sell_id = None

    # ═══════════════════════════════════════════════
    #  Close Position — Dual Taker (V14, for fallback)
    # ═══════════════════════════════════════════════

    async def _close_position(
        self, l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
        batch_pct: float = 100.0,
    ) -> bool:
        async with self._trade_lock:
            full_qty = min(abs(self.lighter_position), abs(self.extended_position))
            if full_qty < self.order_size * Decimal("0.01"):
                return False

            if batch_pct < 100.0 and self._total_open_qty > 0:
                target_close = self._total_open_qty * Decimal(str(batch_pct / 100.0))
                close_qty = min(full_qty, target_close)
            else:
                close_qty = full_qty

            close_qty = close_qty.quantize(self.extended_min_order_size, rounding=ROUND_DOWN)
            if close_qty < self.order_size * Decimal("0.01"):
                return False

            if self.position_direction == "long_X_short_L":
                close_l_side, close_x_side = "buy", "sell"
                l_ref = l_ask if l_ask > 0 else (l_bid + l_ask) / 2
                x_ref = x_bid
            else:
                close_l_side, close_x_side = "sell", "buy"
                l_ref = l_bid if l_bid > 0 else (l_bid + l_ask) / 2
                x_ref = x_ask

            l_vwap_c, l_depth_c = self._get_vwap(
                "lighter", close_l_side, float(close_qty))
            x_vwap_c, x_depth_c = self._get_vwap(
                "extended", close_x_side, float(close_qty))

            if l_vwap_c is None or x_vwap_c is None:
                return False

            l_ref = l_vwap_c
            x_ref = x_vwap_c

            is_partial = batch_pct < 100.0
            tier_label = f"T{self._tier_closed_pct:.0f}→{self._tier_closed_pct + batch_pct:.0f}%"

            t0 = time.time()
            results = await asyncio.gather(
                self._place_extended_ioc(close_x_side, close_qty, x_ref),
                self._place_lighter_taker(close_l_side, close_qty, l_ref,
                                           reduce_only=True),
                return_exceptions=True,
            )
            x_fill = results[0] if not isinstance(results[0], Exception) else None
            l_fill = results[1] if not isinstance(results[1], Exception) else None
            total_ms = (time.time() - t0) * 1000
            x_ok = x_fill is not None and x_fill > 0
            l_ok = l_fill is not None and l_fill > 0

            if x_ok and l_ok:
                hedge_qty = min(x_fill, l_fill)
                await self._resolve_extended_fill_price(x_ref, is_close=True)
                l_fill_price = self._last_lighter_avg_price or l_ref
                x_fill_price = self._last_extended_avg_price or x_ref
                x_estimated = (self._last_extended_avg_price is None
                               or self._last_extended_avg_price == x_ref)

                if close_x_side == "sell":
                    self.extended_position -= x_fill
                else:
                    self.extended_position += x_fill

                return self._finalize_close(
                    tier_label, hedge_qty, l_fill_price, x_fill_price,
                    x_estimated, l_bid, l_ask, x_bid, x_ask,
                    close_l_side, close_x_side, l_ref, x_ref,
                    l_fill, x_fill, total_ms, batch_pct, "taker")

            elif x_ok and not l_ok:
                if close_x_side == "sell":
                    self.extended_position -= x_fill
                else:
                    self.extended_position += x_fill
                self.logger.error(f"[平仓] Lighter未成交, L对冲 {close_l_side} {x_fill}")
                ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
                retry_ref = l_ref
                if ls_now and ls_now.best_bid and ls_now.best_ask:
                    retry_ref = (Decimal(str(ls_now.best_bid)) if close_l_side == "sell"
                                 else Decimal(str(ls_now.best_ask)))
                l_retry = await self._place_lighter_taker(
                    close_l_side, x_fill, retry_ref, reduce_only=True)
                if not (l_retry and l_retry > 0):
                    self._force_reconcile = True
                return False

            elif l_ok and not x_ok:
                self.logger.error(f"[平仓] Extended未成交, 回撤Lighter")
                undo_l_side = "sell" if close_l_side == "buy" else "buy"
                ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
                undo_l_ref = l_ref
                if ls_now and ls_now.best_bid and ls_now.best_ask:
                    undo_l_ref = (Decimal(str(ls_now.best_bid)) if undo_l_side == "sell"
                                  else Decimal(str(ls_now.best_ask)))
                undo_fill = await self._place_lighter_taker(undo_l_side, l_fill, undo_l_ref)
                if not (undo_fill and undo_fill > 0):
                    self._force_reconcile = True
                return False

            else:
                self.logger.warning(f"[平仓] 双边均未成交({total_ms:.0f}ms)")
                return False

    # ═══════════════════════════════════════════════
    #  Close Position — Maker-First (NEW)
    # ═══════════════════════════════════════════════

    async def _close_position_mt(
        self, l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
        batch_pct: float = 100.0,
        maker_wait: Optional[float] = None,
    ) -> bool:
        """Close using Extended maker first, then Lighter taker.
        Falls back to dual taker on timeout.
        maker_wait: Binance-aware dynamic timeout (None = use default).
        """
        async with self._trade_lock:
            full_qty = min(abs(self.lighter_position), abs(self.extended_position))
            if full_qty < self.order_size * Decimal("0.01"):
                return False

            if batch_pct < 100.0 and self._total_open_qty > 0:
                target_close = self._total_open_qty * Decimal(str(batch_pct / 100.0))
                close_qty = min(full_qty, target_close)
            else:
                close_qty = full_qty

            close_qty = close_qty.quantize(self.extended_min_order_size, rounding=ROUND_DOWN)
            if close_qty < self.order_size * Decimal("0.01"):
                return False

            if self.position_direction == "long_X_short_L":
                close_l_side, close_x_side = "buy", "sell"
                l_ref = l_ask
                x_ref = x_bid
            else:
                close_l_side, close_x_side = "sell", "buy"
                l_ref = l_bid
                x_ref = x_ask

            tier_label = f"T{self._tier_closed_pct:.0f}→{self._tier_closed_pct + batch_pct:.0f}%"
            self.logger.info(
                f"[平仓MT] {tier_label} Extended maker {close_x_side} "
                f"{close_qty} @ {x_ref}")

            t0 = time.time()

            if close_x_side == "sell":
                maker_price = (x_bid + self.extended_tick_size).quantize(
                    self.extended_tick_size, rounding=ROUND_HALF_UP)
            else:
                maker_price = (x_ask - self.extended_tick_size).quantize(
                    self.extended_tick_size, rounding=ROUND_HALF_UP)

            order_id = await self._place_extended_post_only(
                close_x_side, close_qty, maker_price)
            if not order_id:
                self.logger.warning("[平仓MT] maker下单失败, 回退双taker")
                return False

            _effective_timeout = maker_wait if maker_wait is not None else self._x_maker_close_timeout
            x_fill_qty, x_fill_price = await self._wait_maker_fill(
                order_id, _effective_timeout)
            total_ms = (time.time() - t0) * 1000

            if x_fill_qty > 0:
                if x_fill_qty < close_qty * Decimal("0.95"):
                    remaining = await self._cancel_extended_order(order_id)
                    x_fill_qty = max(x_fill_qty, remaining)
                else:
                    self._x_maker_recent_ids.append(order_id)
                    if len(self._x_maker_recent_ids) > 20:
                        self._x_maker_recent_ids = self._x_maker_recent_ids[-10:]
                    self._x_maker_pending_id = None

                if close_x_side == "sell":
                    self.extended_position -= x_fill_qty
                else:
                    self.extended_position += x_fill_qty

                x_actual = x_fill_price or x_ref
                self._last_extended_avg_price = x_actual

                ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
                if ls_now and ls_now.best_bid and ls_now.best_ask:
                    l_ref_now = (Decimal(str(ls_now.best_bid)) if close_l_side == "sell"
                                 else Decimal(str(ls_now.best_ask)))
                else:
                    l_ref_now = l_ref

                l_fill = await self._place_lighter_taker(
                    close_l_side, x_fill_qty, l_ref_now)

                if l_fill and l_fill > 0:
                    l_fill_price = self._last_lighter_avg_price or l_ref_now
                    return self._finalize_close(
                        tier_label, min(x_fill_qty, l_fill),
                        l_fill_price, x_actual, False,
                        l_bid, l_ask, x_bid, x_ask,
                        close_l_side, close_x_side, l_ref_now, x_ref,
                        l_fill, x_fill_qty, total_ms, batch_pct, "maker-taker")
                else:
                    self.logger.error("[平仓MT] Lighter对冲失败! 触发对账")
                    self._force_reconcile = True
                    return False
            else:
                await self._cancel_extended_order(order_id)
                self.logger.warning(
                    f"[平仓MT] maker {_effective_timeout:.0f}s超时, "
                    f"回退双taker")
                return False

    # ═══════════════════════════════════════════════
    #  V18: Close Position — Maker-Maker
    # ═══════════════════════════════════════════════

    async def _close_position_mm(
        self, l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
        batch_pct: float = 100.0,
    ) -> bool:
        """V18: Close using Extended maker (with progressive re-posting)
        + Lighter maker. Falls back to _close_position (dual taker) on failure.
        """
        async with self._trade_lock:
            full_qty = min(abs(self.lighter_position), abs(self.extended_position))
            if full_qty < self.order_size * Decimal("0.01"):
                return False

            if batch_pct < 100.0 and self._total_open_qty > 0:
                target_close = self._total_open_qty * Decimal(str(batch_pct / 100.0))
                close_qty = min(full_qty, target_close)
            else:
                close_qty = full_qty

            close_qty = close_qty.quantize(
                self.extended_min_order_size, rounding=ROUND_DOWN)
            if close_qty < self.order_size * Decimal("0.01"):
                return False

            if self.position_direction == "long_X_short_L":
                close_l_side, close_x_side = "buy", "sell"
                l_ref = l_ask
                x_ref = x_bid
            else:
                close_l_side, close_x_side = "sell", "buy"
                l_ref = l_bid
                x_ref = x_ask

            tier_label = (f"T{self._tier_closed_pct:.0f}"
                          f"→{self._tier_closed_pct + batch_pct:.0f}%")
            t0 = time.time()
            _x_total_timeout = 30.0
            _x_repost_interval = 10.0

            # -- Step 1: Extended maker close with progressive re-posting --
            x_fill_qty = Decimal("0")
            x_fill_price = None
            order_id = None
            repost_count = 0

            while time.time() - t0 < _x_total_timeout:
                x_bid_now, x_ask_now = self._get_extended_ws_bbo()
                if not x_bid_now or not x_ask_now:
                    await asyncio.sleep(0.5)
                    continue

                if close_x_side == "sell":
                    maker_price = (x_bid_now + self.extended_tick_size).quantize(
                        self.extended_tick_size, rounding=ROUND_HALF_UP)
                else:
                    maker_price = (x_ask_now - self.extended_tick_size).quantize(
                        self.extended_tick_size, rounding=ROUND_HALF_UP)

                if order_id:
                    await self._cancel_extended_order(order_id)
                    x_fill_qty = self._x_maker_fill_qty
                    x_fill_price = self._x_maker_fill_price
                    if x_fill_qty > 0:
                        break
                    order_id = None

                self.logger.info(
                    f"[V18平仓] {tier_label} Ext maker {close_x_side} "
                    f"{close_qty} @ {maker_price} (第{repost_count+1}次)")

                order_id = await self._place_extended_post_only(
                    close_x_side, close_qty, maker_price)
                if not order_id:
                    self.logger.warning("[V18平仓] Extended maker失败, 回退双taker")
                    return False

                repost_count += 1
                remaining = min(_x_repost_interval,
                                _x_total_timeout - (time.time() - t0))
                if remaining <= 0:
                    break

                x_fill_qty, x_fill_price = await self._wait_maker_fill(
                    order_id, remaining)
                if x_fill_qty > 0:
                    break

            if x_fill_qty <= 0 and order_id:
                await self._cancel_extended_order(order_id)
                x_fill_qty = self._x_maker_fill_qty
                x_fill_price = self._x_maker_fill_price

            if x_fill_qty <= 0:
                elapsed = time.time() - t0
                self.logger.warning(
                    f"[V18平仓] Ext maker {repost_count}次调价后仍超时"
                    f"({elapsed:.0f}s), 回退双taker")
                return False

            if x_fill_qty < close_qty * Decimal("0.95") and order_id:
                await self._cancel_extended_order(order_id)
            else:
                if order_id:
                    self._x_maker_recent_ids.append(order_id)
                    if len(self._x_maker_recent_ids) > 20:
                        self._x_maker_recent_ids = self._x_maker_recent_ids[-10:]
                    self._x_maker_pending_id = None

            if close_x_side == "sell":
                self.extended_position -= x_fill_qty
            else:
                self.extended_position += x_fill_qty

            x_actual = x_fill_price or x_ref
            self._last_extended_avg_price = x_actual

            ext_ms = (time.time() - t0) * 1000
            self.logger.info(
                f"[V18平仓] Ext成交: {x_fill_qty} @ {x_actual} "
                f"({ext_ms:.0f}ms, 调价{repost_count}次)")

            # -- Step 2: Lighter close with tiered logic (same as hedge) --
            ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
            if ls_now and ls_now.best_bid and ls_now.best_ask:
                l_ref_now = (Decimal(str(ls_now.best_bid))
                             if close_l_side == "sell"
                             else Decimal(str(ls_now.best_ask)))
            else:
                l_ref_now = l_ref

            _CLOSE_MAKER_SPREAD_MIN = self._smart_close_min_gap
            _CLOSE_MAKER_MAX_WAIT = self._smart_close_max_wait
            _bn_stable = True
            if self.binance_feed and self.binance_feed.connected:
                vol = self.binance_feed.realized_vol_bps
                if vol > 2.0:
                    _bn_stable = False
            _close_book_ok = (ls_now and ls_now.best_bid and ls_now.best_ask)

            _close_gap = 0.0
            if x_actual and l_ref_now and l_ref_now > 0:
                if self.position_direction == "long_X_short_L":
                    _close_gap = float((l_ref_now - x_actual) / l_ref_now * 10000)
                else:
                    _close_gap = float((x_actual - l_ref_now) / l_ref_now * 10000)

            _use_close_maker = (
                _close_gap >= _CLOSE_MAKER_SPREAD_MIN
                and _bn_stable
                and _close_book_ok
            )

            if _use_close_maker:
                self.logger.info(
                    f"[V18平仓L] 残余gap={_close_gap:.1f}bps "
                    f"→ 短maker({_CLOSE_MAKER_MAX_WAIT:.0f}s)")
                l_fill = await self._lighter_maker_with_fallback(
                    close_l_side, x_fill_qty, l_ref_now,
                    maker_timeout_s=_CLOSE_MAKER_MAX_WAIT,
                    reduce_only=True)
            else:
                self.logger.info(
                    f"[V18平仓L] gap={_close_gap:.1f}bps/BN不稳 "
                    f"→ 立即taker")
                l_fill = await self._place_lighter_taker(
                    close_l_side, x_fill_qty, l_ref_now,
                    reduce_only=True)

            total_ms = (time.time() - t0) * 1000
            _l_used_maker = _use_close_maker
            _actual_mode = "maker-maker" if _l_used_maker else "maker-taker"

            if l_fill and l_fill > 0:
                l_fill_price = self._last_lighter_avg_price or l_ref_now
                return self._finalize_close(
                    tier_label, min(x_fill_qty, l_fill),
                    l_fill_price, x_actual, False,
                    l_bid, l_ask, x_bid, x_ask,
                    close_l_side, close_x_side, l_ref_now, x_ref,
                    l_fill, x_fill_qty, total_ms, batch_pct, _actual_mode)
            else:
                self.logger.error(
                    "[V18平仓] Lighter对冲失败! 触发对账")
                self._force_reconcile = True
                return False

    def _finalize_close(self, tier_label, hedge_qty, l_fill_price, x_fill_price,
                        x_estimated, l_bid, l_ask, x_bid, x_ask,
                        close_l_side, close_x_side, l_ref, x_ref,
                        l_fill, x_fill, total_ms, batch_pct, mode_tag):
        gap_at_close = self._get_current_gap_bps(l_bid, l_ask, x_bid, x_ask)
        self._tier_close_records.append({
            "qty": float(hedge_qty),
            "gap_bps": gap_at_close,
            "l_price": float(l_fill_price),
            "x_price": float(x_fill_price),
        })
        if self._total_open_qty > 0:
            closed_so_far = sum(r["qty"] for r in self._tier_close_records)
            self._tier_closed_pct = min(
                100.0,
                float(Decimal(str(closed_so_far)) / self._total_open_qty * 100))

        wt_close_spread = 0.0
        total_closed_q = sum(r["qty"] for r in self._tier_close_records)
        if total_closed_q > 0:
            wt_close_spread = sum(
                r["gap_bps"] * r["qty"]
                for r in self._tier_close_records) / total_closed_q
        if mode_tag == "maker-maker":
            _close_fee = self.PAIR_FEE_MM
        elif mode_tag in ("maker", "maker-taker"):
            _close_fee = self.PAIR_FEE_MT
        else:
            _close_fee = self.PAIR_FEE_TT
        _total_fee = self._open_route_fee_bps + _close_fee
        convergence = self._open_theoretical_spread - wt_close_spread
        net_after_fee = convergence - _total_fee

        net_bps = self._log_ref_pnl(
            x_fill_price, l_fill_price, hedge_qty,
            x_estimated=x_estimated, route_fee_bps=_total_fee)
        self.cumulative_net_bps += net_bps

        est_tag = "(估)" if x_estimated else ""
        self.logger.info(
            f"[平仓完成|{mode_tag}] {tier_label} {total_ms:.0f}ms "
            f"本批净利={net_bps:+.2f}bps{est_tag} "
            f"加权平仓spread={wt_close_spread:.2f}bps "
            f"总收敛={convergence:.2f}bps "
            f"费用=开{self._open_route_fee_bps:.1f}+关{_close_fee:.1f}={_total_fee:.1f}bps "
            f"扣费后={net_after_fee:+.2f}bps "
            f"已平={self._tier_closed_pct:.0f}% "
            f"累积={self.cumulative_net_bps:+.2f}bps")

        self._csv_row([
            datetime.now(timezone(timedelta(hours=8))).isoformat(),
            "CLOSE", f"tier_{self._tier_closed_pct:.0f}pct",
            close_l_side, f"{l_ref:.2f}", f"{l_fill_price:.2f}", str(l_fill),
            close_x_side, f"{x_ref:.2f}", f"{x_fill_price:.2f}", str(x_fill),
            f"{self._open_theoretical_spread:.2f}", f"{total_ms:.0f}", mode_tag,
            "", "", "", "",
            f"{net_bps:.2f}", f"{self.cumulative_net_bps:.2f}",
            self._current_trade_id, mode_tag,
            f"{self._open_route_fee_bps:.2f}", f"{_close_fee:.2f}",
            f"{_total_fee:.2f}", f"{total_ms:.0f}", "",
            self._open_x_side, f"{self._open_bn_vol:.2f}",
            "", "",
        ])
        self._last_trade_time = time.time()

        pos_remaining = abs(self.extended_position)
        if pos_remaining < self.order_size * Decimal("0.01"):
            self._reset_position_state()
            self.state = State.IDLE
            return True
        else:
            self._normalize_refs(pos_remaining)
            self.state = State.IN_POSITION
            return True

    # ═══════════════════════════════════════════════
    #  Emergency / Shutdown
    # ═══════════════════════════════════════════════

    async def _emergency_flatten(self):
        try:
            await self._cancel_all_dm_orders()
            if self._x_maker_pending_id:
                await self._cancel_extended_order(self._x_maker_pending_id)
            await self._do_lighter_cancel_all()
            await self._cancel_all_extended_orders_rest()
            await asyncio.sleep(0.5)

            for attempt in range(3):
                l_qty = abs(self.lighter_position)
                x_qty = abs(self.extended_position)
                if (l_qty < self.order_size * Decimal("0.01")
                        and x_qty < self.order_size * Decimal("0.01")):
                    break

                tasks = []
                if l_qty >= self.order_size * Decimal("0.01"):
                    l_side = "sell" if self.lighter_position > 0 else "buy"
                    ls = self.lighter_feed.snapshot if self.lighter_feed else None
                    l_ref = None
                    if ls and ls.mid:
                        ref_val = ((ls.best_bid or ls.mid) if l_side == "sell"
                                   else (ls.best_ask or ls.mid))
                        l_ref = Decimal(str(ref_val or 0))
                    if l_ref and l_ref > 0:
                        tasks.append(self._place_lighter_taker(l_side, l_qty, l_ref))
                    else:
                        tasks.append(asyncio.sleep(0))

                if x_qty >= self.order_size * Decimal("0.01"):
                    x_side = "sell" if self.extended_position > 0 else "buy"
                    x_bid, x_ask = self._get_extended_ws_bbo()
                    x_ref = x_bid if x_side == "sell" else x_ask
                    if x_ref and x_ref > 0:
                        tasks.append(self._place_extended_ioc(x_side, x_qty, x_ref))
                    else:
                        tasks.append(asyncio.sleep(0))

                x_task_idx = -1
                if tasks:
                    if l_qty >= self.order_size * Decimal("0.01"):
                        x_task_idx = 1
                    else:
                        x_task_idx = 0
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    if (x_task_idx < len(results)
                            and not isinstance(results[x_task_idx], Exception)
                            and results[x_task_idx] is not None
                            and results[x_task_idx] > 0):
                        x_fill = results[x_task_idx]
                        x_side_used = ("sell" if self.extended_position > 0 else "buy")
                        if x_side_used == "sell":
                            self.extended_position -= x_fill
                        else:
                            self.extended_position += x_fill

                await asyncio.sleep(2)
                try:
                    await self._reconcile_positions()
                except Exception:
                    pass

            l_final = abs(self.lighter_position)
            x_final = abs(self.extended_position)
            if (l_final >= self.order_size * Decimal("0.01")
                    or x_final >= self.order_size * Decimal("0.01")):
                self.logger.error(
                    f"[紧急平仓] 3轮后仍有残余! L={self.lighter_position} "
                    f"X={self.extended_position} 请手动处理!")
            else:
                self.logger.info("[紧急平仓] 仓位已清零")
        except Exception as e:
            self.logger.error(f"紧急平仓异常: {e}\n{traceback.format_exc()}")

    async def _shutdown(self):
        self.stop_flag = True
        self.logger.info("关闭中...")

        async def _cancel_with_timeout():
            self.logger.info("[关闭] 撤销所有挂单...")
            try:
                await asyncio.wait_for(self._cancel_all_dm_orders(), timeout=5)
            except Exception as e:
                self.logger.warning(f"[关闭] 撤双向Maker单异常: {e}")
            if self._x_maker_pending_id:
                try:
                    await asyncio.wait_for(
                        self._cancel_extended_order(self._x_maker_pending_id),
                        timeout=5)
                except Exception as e:
                    self.logger.warning(f"[关闭] 撤单边Maker单异常: {e}")
            try:
                await asyncio.wait_for(self._do_lighter_cancel_all(), timeout=5)
            except Exception as e:
                self.logger.warning(f"[关闭] 撤Lighter单异常: {e}")
            try:
                await asyncio.wait_for(
                    self._cancel_all_extended_orders_rest(), timeout=5)
            except Exception as e:
                self.logger.warning(f"[关闭] 撤Extended REST单异常: {e}")
            self.logger.info("[关闭] 挂单清理完成")

        try:
            await asyncio.wait_for(_cancel_with_timeout(), timeout=20)
        except asyncio.TimeoutError:
            self.logger.error("[关闭] 撤单超时20s! 部分挂单可能遗留(GTT 60s后自动过期)")

        if self._lighter_ws_task and not self._lighter_ws_task.done():
            self._lighter_ws_task.cancel()
            try:
                await self._lighter_ws_task
            except (asyncio.CancelledError, Exception):
                pass
        for feed in (self.lighter_feed, self.extended_feed, self.binance_feed):
            if feed:
                try:
                    await feed.disconnect()
                except Exception:
                    pass
        if self.extended_client:
            try:
                self.extended_client._stop_event.set()
                for t in self.extended_client._tasks:
                    t.cancel()
                await asyncio.gather(*self.extended_client._tasks, return_exceptions=True)
            except Exception:
                pass
        if self._aio_session and not self._aio_session.closed:
            await self._aio_session.close()
        await asyncio.sleep(0.5)
        self.logger.info("已关闭")

    # ═══════════════════════════════════════════════
    #  Main entry
    # ═══════════════════════════════════════════════

    async def run(self):
        def on_stop(*_):
            self.stop_flag = True
            self.logger.info("收到中断信号")
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                asyncio.get_event_loop().add_signal_handler(sig, on_stop)
            except NotImplementedError:
                signal.signal(sig, on_stop)
        try:
            await self._main_loop_wrapper()
        except KeyboardInterrupt:
            self.logger.info("用户中断")
        except Exception as e:
            self.logger.error(f"致命错误: {e}\n{traceback.format_exc()}")
        finally:
            self.logger.info("[退出] 清理...")
            if not self.dry_run:
                try:
                    await self._reconcile_positions()
                except Exception:
                    pass
            if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                self.logger.warning(
                    f"退出仍有仓位: L={self.lighter_position} X={self.extended_position}")
                if not self.dry_run:
                    try:
                        await self._emergency_flatten()
                    except Exception as e:
                        self.logger.error(f"[退出] 紧急平仓异常: {e}")
            else:
                self.logger.info("无持仓，直接退出")
            try:
                await self._shutdown()
            except Exception as e:
                self.logger.warning(f"[退出] 关闭异常: {e}")

    # ═══════════════════════════════════════════════
    #  Main Loop
    # ═══════════════════════════════════════════════

    async def _main_loop_wrapper(self):
        mode_desc = "★ V18-Maker-Maker" if self.maker_mode else "双Taker"
        ladder_desc = (f"阶梯加仓={self.ladder_step}bps/层 "
                       f"最多{int(self.max_position / self.order_size)}层"
                       if self.ladder_step > 0 else "阶梯=关")
        tier_desc = ""
        if len(self.close_tiers) > 1:
            parts = [f"≤{sp:.1f}bps→{pct:.0f}%" for sp, pct in self.close_tiers]
            tier_desc = f"  分批平仓=[{', '.join(parts)}]"
        else:
            tier_desc = f"  单层平仓≤{self.close_gap_bps}bps"
        self.logger.info(
            f"启动 Extended+Lighter V18-MakerMaker {mode_desc}  {self.symbol}  "
            f"size={self.order_size} max_pos={self.max_position}  "
            f"目标价差≥{self.target_spread}bps{tier_desc}  "
            f"dry_run={self.dry_run}  "
            f"费用(MM/MT/TT)≈{self.PAIR_FEE_MM:.1f}/{self.PAIR_FEE_MT:.1f}/{self.PAIR_FEE_TT:.1f}bps  {ladder_desc}  "
            f"滑点保护≤{self._max_slippage_bps}bps")
        if self.maker_mode:
            self.logger.info(
                f"  V18-Binance做市: 窗口={self._dyn_window}s P{self._dyn_percentile:.0f} "
                f"价格容忍={self._price_tolerance_bps}bps "
                f"最大挂龄={self._maker_max_age}s "
                f"对冲超时={self._hedge_timeout}s "
                f"最小价差间距={self._min_spread_gap_bps}bps")

        if not self.dry_run:
            self._init_lighter_signer()
            self._get_lighter_market_config()
            self._init_extended_client()
            await self.extended_client.connect()
            await self._get_extended_contract_info()
            await self._warmup_extended()
            self.logger.info("交易客户端就绪")
            self.logger.info("[启动] 清理遗留挂单...")
            await self._do_lighter_cancel_all()
            await self._cancel_all_extended_orders_rest()
        else:
            self.logger.info("[DRY-RUN] 跳过交易客户端初始化")
            self._get_lighter_market_config()

        self.lighter_feed = LighterFeed(self.symbol)
        self.lighter_feed._ob_update_callback = self._on_lighter_bbo_update
        self.extended_feed = ExtendedFeed(self.symbol)
        self.binance_feed = BinanceFeed(self.symbol)
        self.binance_feed._callback = self._on_binance_bbo_update
        asyncio.create_task(self.lighter_feed.connect())
        asyncio.create_task(self.extended_feed.connect())
        asyncio.create_task(self.binance_feed.connect())
        if not self.dry_run:
            self._lighter_ws_task = asyncio.create_task(
                self._lighter_account_ws_loop())
        self.logger.info("等待行情数据 (6s)...")
        await asyncio.sleep(6)

        ls = self.lighter_feed.snapshot
        x_bid, x_ask = (self._get_extended_ws_bbo()
                        if not self.dry_run else (None, None))
        es = self.extended_feed.snapshot
        if not ls.mid or (not es.mid and x_bid is None):
            self.logger.error("行情不可用")
            return
        x_mid_val = float((x_bid + x_ask) / 2) if x_bid and x_ask else es.mid
        self.logger.info(
            f"行情就绪: Lighter={ls.mid:.2f} Extended={x_mid_val:.2f}")

        if not self.dry_run:
            await self._reconcile_positions()
            if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                self.logger.warning(
                    f"启动检测到仓位: L={self.lighter_position} "
                    f"X={self.extended_position}")
                if self.flatten_on_start:
                    self.logger.warning(
                        "[启动] --flatten-on-start 已启用，立即紧急平仓!")
                    await self._emergency_flatten()
                    await self._reconcile_positions()
                    if (abs(self.lighter_position) > self.order_size * Decimal("0.01")
                            or abs(self.extended_position) > self.order_size * Decimal("0.01")):
                        self.logger.error(
                            f"[启动] 紧急平仓后仍有残余: "
                            f"L={self.lighter_position} X={self.extended_position}")
                    else:
                        self.logger.info("[启动] 遗留仓位已清零")
                        self.lighter_position = Decimal("0")
                        self.extended_position = Decimal("0")
                else:
                    self.state = State.IN_POSITION
                    self.position_opened_at = time.time()
                    if self.extended_position > 0:
                        self.position_direction = "long_X_short_L"
                    elif self.extended_position < 0:
                        self.position_direction = "long_L_short_X"

                    x_bid_s, x_ask_s = self._get_extended_ws_bbo()
                    x_est = ((x_bid_s + x_ask_s) / 2 if x_bid_s and x_ask_s
                             else Decimal(str(ls.mid or 0)))
                    l_est = Decimal(str(ls.mid or 0))
                    if x_est > 0 and l_est > 0:
                        x_qty = abs(self.extended_position)
                        l_qty = abs(self.lighter_position)
                        self._open_x_refs = [(x_est, x_qty)] if x_qty > 0 else []
                        self._open_l_refs = [(l_est, l_qty)] if l_qty > 0 else []
                        self._total_open_qty = min(x_qty, l_qty)
                        self._tier_closed_pct = 0.0
                        self._tier_close_records.clear()
                        if self.position_direction == "long_X_short_L":
                            _est_sp = float((l_est - x_est) / l_est * 10000)
                        else:
                            _est_sp = float((x_est - l_est) / l_est * 10000)
                        self._last_open_spread_bps = max(_est_sp, self.target_spread)
                        self._open_theoretical_spread = self._last_open_spread_bps
                        self.logger.info(
                            f"[启动恢复] _total_open_qty={self._total_open_qty} "
                            f"方向={self.position_direction}")

        self.logger.info(f"进入主循环  状态={self.state.value}")
        last_reconcile = time.time()
        last_status_log = 0.0

        while not self.stop_flag:
            try:
                ls = self.lighter_feed.snapshot
                now = time.time()

                # ━━ CRITICAL: fill event 最高优先级检查 ━━
                # 必须在对账/订单管理之前处理，否则对账会抢先清除 fill event
                if self._x_dm_fill_event.is_set() and self.state == State.IDLE:
                    self.logger.info("[主循环] 检测到成交事件, 立即处理对冲")
                    ok = await self._handle_dm_fill()
                    if ok:
                        self.state = State.IN_POSITION
                    continue

                x_ws_bid, x_ws_ask = (None, None)
                if not self.dry_run:
                    x_ws_bid, x_ws_ask = self._get_extended_ws_bbo()

                if x_ws_bid is None or x_ws_ask is None:
                    es = self.extended_feed.snapshot
                    if not es.mid:
                        await self._interruptible_sleep(self.interval)
                        continue
                    if es.best_bid > 0 and es.best_ask > 0:
                        x_ws_bid = Decimal(str(es.best_bid))
                        x_ws_ask = Decimal(str(es.best_ask))
                    x_mid = Decimal(str(es.mid))
                else:
                    x_mid = (x_ws_bid + x_ws_ask) / 2

                if not ls.mid or ls.best_bid is None or ls.best_ask is None:
                    await self._interruptible_sleep(self.interval)
                    continue

                l_bid = Decimal(str(ls.best_bid))
                l_ask = Decimal(str(ls.best_ask))
                l_mid = Decimal(str(ls.mid))

                if l_bid >= l_ask:
                    await self._interruptible_sleep(self.interval)
                    continue

                l_age = now - ls.timestamp if ls.timestamp > 0 else 999
                if (not self.dry_run and self.extended_client
                        and self.extended_client._ob_last_update_ts > 0):
                    x_age = now - self.extended_client._ob_last_update_ts
                elif self.extended_feed and self.extended_feed.snapshot.timestamp > 0:
                    x_age = now - self.extended_feed.snapshot.timestamp
                else:
                    x_age = 999

                if l_age > 5.0 or x_age > 5.0:
                    await self._interruptible_sleep(self.interval)
                    continue

                if x_ws_bid is not None and x_ws_ask is not None:
                    gap1, gap2 = self._calc_directional_gaps(
                        l_bid, l_ask, x_ws_bid, x_ws_ask)
                    self._record_spread_obs(gap1, gap2)

                mid_spread_bps = float((x_mid - l_mid) / l_mid * 10000)
                cur_pos = abs(self.extended_position)

                # ── Status log ──
                if now - last_status_log > 15:
                    dyn_thr = self._calc_dynamic_threshold() if self.maker_mode else 0.0
                    hold_s = ""
                    if self.position_opened_at:
                        hold_s = f" hold={now - self.position_opened_at:.0f}s"
                    basis_info = ""
                    if (self.state == State.IN_POSITION
                            and x_ws_bid is not None and x_ws_ask is not None):
                        _gap_bps = self._get_current_gap_bps(
                            l_bid, l_ask, x_ws_bid, x_ws_ask)
                        _hold = (now - self.position_opened_at
                                 if self.position_opened_at else 0)
                        _next_tier_sp = None
                        for _ts, _cp in self.close_tiers:
                            if self._tier_closed_pct < _cp:
                                _next_tier_sp = _ts
                                _next_tier_pct = _cp
                                break
                        _close_ok = (_next_tier_sp is not None
                                     and _gap_bps <= _next_tier_sp)
                        _tier_desc = (f"≤{_next_tier_sp:.1f}→{_next_tier_pct:.0f}%"
                                      if _next_tier_sp is not None else "done")
                        basis_info = (
                            f" gap={_gap_bps:.1f}bps/{_tier_desc}"
                            f"({'CLOSE' if _close_ok else 'HOLD'}"
                            f" hold={_hold:.0f}s 已平={self._tier_closed_pct:.0f}%)")
                    maker_info = ""
                    if self.maker_mode:
                        _bn_adj_disp = self._get_bn_adaptive_spread()
                        _ps_disp = max(self._dyn_fee_floor + _bn_adj_disp,
                                       self._dyn_fee_floor)
                        maker_info = (f"  dyn={dyn_thr:.1f}bps"
                                      f" ps={_ps_disp:.1f}bps")
                        if self.state == State.IDLE:
                            dm_parts = []
                            if self._x_maker_buy_id:
                                b_age = now - self._x_maker_buy_placed_at
                                dm_parts.append(
                                    f"B@{self._x_maker_buy_price:.2f}/{b_age:.0f}s")
                            if self._x_maker_sell_id:
                                s_age = now - self._x_maker_sell_placed_at
                                dm_parts.append(
                                    f"S@{self._x_maker_sell_price:.2f}/{s_age:.0f}s")
                            if dm_parts:
                                maker_info += " 挂单:" + " ".join(dm_parts)
                    ladder_info = ""
                    if (self.state == State.IN_POSITION and self.position_direction):
                        _layers = max(1, int(cur_pos / self.order_size))
                        _max_layers = int(self.max_position / self.order_size)
                        if self._tier_closed_pct > 0:
                            ladder_info = (
                                f"  层={_layers}/{_max_layers}"
                                f" 回补≥{self.target_spread:.1f}bps"
                                f"(待补={self._tier_closed_pct:.0f}%)")
                        elif self.ladder_step > 0:
                            _next = self._last_open_spread_bps + self.ladder_step
                            ladder_info = (
                                f"  层={_layers}/{_max_layers} 下层≥{_next:.1f}bps")
                    _bf = self.binance_feed
                    _bn_age = _bf.age if _bf else 999
                    _bn_vol = _bf.realized_vol_bps if _bf else 0
                    _bn_mid = _bf.mid if _bf else 0
                    _bn_tag = (f" BN={_bn_mid:.1f}({_bn_age:.1f}s "
                               f"vol={_bn_vol:.1f}bps)")
                    self.logger.info(
                        f"[{self.state.value}] mid={mid_spread_bps:+.2f}bps  "
                        f"X_pos={self.extended_position} "
                        f"L_pos={self.lighter_position}{hold_s}  "
                        f"L={l_bid}/{l_ask} X={x_ws_bid}/{x_ws_ask}"
                        f"{basis_info}{ladder_info}{maker_info}  "
                        f"鲜度:L={l_age:.1f}s X={x_age:.1f}s{_bn_tag}  "
                        f"累积={self.cumulative_net_bps:+.2f}bps")
                    last_status_log = now

                # ── Force reconcile (skip when fill pending) ──
                if self._force_reconcile and not self.dry_run:
                    if self._x_dm_fill_event.is_set():
                        self.logger.info(
                            "[强制对账] fill event待处理, 延后对账")
                    else:
                        self._force_reconcile = False
                        self.logger.warning("[强制对账] 触发")
                        await self._cancel_all_dm_orders()
                        if self._x_maker_pending_id:
                            await self._cancel_extended_order(
                                self._x_maker_pending_id)
                        await self._reconcile_positions()
                        last_reconcile = now
                        cur_pos = abs(self.extended_position)
                        if abs(self.lighter_position + self.extended_position) > self.order_size * Decimal("0.5"):
                            self.logger.error("[强制对账] 不平衡, 紧急平仓")
                            await self._emergency_flatten()
                            self._reset_position_state()
                            self.state = State.IDLE
                            last_reconcile = time.time() - 50
                            continue

                # ── Periodic reconcile (skip when fill pending) ──
                _reconcile_interval = 30.0
                if ((self._x_maker_buy_id or self._x_maker_sell_id)
                        and self.state == State.IDLE):
                    _reconcile_interval = 120.0
                if (not self.dry_run and now - last_reconcile > _reconcile_interval
                        and self.state in (State.IDLE, State.IN_POSITION)):
                    if self._x_dm_fill_event.is_set():
                        pass
                    else:
                        await self._reconcile_positions()
                        last_reconcile = now
                        cur_pos = abs(self.extended_position)
                        if (self.state == State.IN_POSITION
                                and self._total_open_qty > 0):
                            actual_pos = min(abs(self.extended_position),
                                             abs(self.lighter_position))
                            if actual_pos > self._total_open_qty:
                                self._total_open_qty = actual_pos
                        net_imbalance = abs(
                            self.lighter_position + self.extended_position)
                        if net_imbalance > self.order_size * Decimal("0.5"):
                            self.logger.error(
                                f"[周期对账] 不平衡! L={self.lighter_position} "
                                f"X={self.extended_position}")
                            await self._emergency_flatten()
                            self._reset_position_state()
                            self.state = State.IDLE
                            last_reconcile = time.time() - 50
                            continue

                # ━━━━━━ STATE: IDLE ━━━━━━
                if self.state == State.IDLE:
                    has_l = abs(self.lighter_position) >= self.order_size * Decimal("0.01")
                    has_x = abs(self.extended_position) >= self.order_size * Decimal("0.01")
                    if has_l or has_x:
                        _idle_min_trade = self.order_size * Decimal("0.05")
                        _idle_both_dust = (
                            abs(self.extended_position) < _idle_min_trade
                            and abs(self.lighter_position) < _idle_min_trade)
                        if _idle_both_dust:
                            pass
                        elif has_l and has_x:
                            self.logger.warning(
                                f"[IDLE] 检测到残余双边仓位, 恢复为IN_POSITION")
                            self.state = State.IN_POSITION
                            self.position_opened_at = time.time()
                            if self.extended_position > 0:
                                self.position_direction = "long_X_short_L"
                            else:
                                self.position_direction = "long_L_short_X"
                            continue
                        else:
                            if self._x_dm_fill_event.is_set():
                                self.logger.info(
                                    f"[IDLE] 单腿仓位但fill event待处理, "
                                    f"跳过紧急平仓")
                            else:
                                self.logger.error(
                                    f"[IDLE] 单腿仓位! "
                                    f"L={self.lighter_position} "
                                    f"X={self.extended_position}, 紧急平仓")
                                await self._emergency_flatten()
                                self._reset_position_state()
                                last_reconcile = time.time() - 50
                                continue

                    if x_ws_bid is None or x_ws_ask is None:
                        await asyncio.sleep(self.interval)
                        continue

                    if cur_pos >= self.max_position:
                        await asyncio.sleep(self.interval)
                        continue

                    if not self._check_tx_rate():
                        await asyncio.sleep(2.0)
                        continue

                    # ── Maker mode: dual-sided order management ──
                    if self.maker_mode and not self.dry_run:
                        if now < self._hedge_circuit_breaker_until:
                            await self._cancel_all_dm_orders()
                            await asyncio.sleep(self.interval)
                            continue

                        dyn_threshold = self._calc_dynamic_threshold()
                        gap1, gap2 = self._calc_directional_gaps(
                            l_bid, l_ask, x_ws_bid, x_ws_ask)
                        best_gap = max(gap1, gap2)
                        self._record_spread_obs(gap1, gap2)

                        fee_floor = self._dyn_fee_floor
                        _dm_min_age = 5.0
                        _bn_adj = self._get_bn_adaptive_spread()
                        pricing_spread = max(fee_floor + _bn_adj, fee_floor)

                        _qty_f = float(self.order_size)
                        _l_vwap_sell, _ = self._get_vwap("lighter", "sell", _qty_f)
                        _l_vwap_buy, _ = self._get_vwap("lighter", "buy", _qty_f)

                        buy_price, sell_price = self._calc_dual_maker_prices(
                            l_bid, l_ask, x_ws_bid, x_ws_ask, pricing_spread,
                            l_vwap_sell=_l_vwap_sell,
                            l_vwap_buy=_l_vwap_buy)

                        _bf = self.binance_feed
                        if _bf and _bf.connected and _bf.mid > 0:
                            _l_mid_now = float(l_bid + l_ask) / 2
                            _bn_l_gap = (_bf.mid - _l_mid_now) / _l_mid_now * 10000
                            if _bn_l_gap < -4.0:
                                if buy_price is not None:
                                    self.logger.debug(
                                        f"[BN-L偏离] BN低于L {_bn_l_gap:.1f}bps "
                                        f"抑制buy挂单")
                                buy_price = None
                            elif _bn_l_gap > 4.0:
                                if sell_price is not None:
                                    self.logger.debug(
                                        f"[BN-L偏离] BN高于L {_bn_l_gap:.1f}bps "
                                        f"抑制sell挂单")
                                sell_price = None

                        if buy_price is None and sell_price is None:
                            _has_orders = (self._x_maker_buy_id
                                           or self._x_maker_sell_id)
                            if _has_orders:
                                _buy_old = (not self._x_maker_buy_id
                                            or now - self._x_maker_buy_placed_at
                                            >= _dm_min_age)
                                _sell_old = (not self._x_maker_sell_id
                                             or now - self._x_maker_sell_placed_at
                                             >= _dm_min_age)
                                if _buy_old and _sell_old:
                                    if self._check_maker_cancel_rate():
                                        await self._cancel_all_dm_orders()
                            await asyncio.sleep(self.interval)
                            continue

                        # ── Check for fill event ──
                        if self._x_dm_fill_event.is_set():
                            ok = await self._handle_dm_fill()
                            if ok:
                                self.state = State.IN_POSITION
                            continue

                        # ── Binance leading drift cancel (方向性逆选保护) ──
                        if self._bn_force_cancel:
                            self._bn_force_cancel = False
                            bf = self.binance_feed
                            _bn_mid = bf.mid if bf else 0
                            _bn_drift = (abs(_bn_mid - self._bn_mid_at_place)
                                         / self._bn_mid_at_place * 10000
                                         if self._bn_mid_at_place > 0
                                         else 0)
                            if _bn_drift > self._bn_drift_bps:
                                _up = self._bn_drift_up
                                _risk = "sell" if _up else "buy"
                                _safe = "buy" if _up else "sell"
                                _arrow = "↑" if _up else "↓"
                                _risk_id = (self._x_maker_sell_id if _up
                                            else self._x_maker_buy_id)
                                if _risk_id:
                                    self.logger.warning(
                                        f"[逆选保护-BN] Binance{_arrow}"
                                        f"{_bn_drift:.1f}bps "
                                        f">{self._bn_drift_bps}bps "
                                        f"撤{_risk}单(保留{_safe}) "
                                        f"冷却{self._bn_cancel_cooldown:.0f}s")
                                    if _risk == "buy":
                                        self._bn_buy_cancel_ts = time.time()
                                    else:
                                        self._bn_sell_cancel_ts = time.time()
                                    partial = await self._cancel_dm_order(
                                        _risk, force=True)
                                    if self._x_dm_fill_event.is_set():
                                        ok = await self._handle_dm_fill()
                                        if ok:
                                            self.state = State.IN_POSITION
                                        continue
                                    if partial > 0:
                                        self._x_dm_fill_side = _risk
                                        self._x_dm_fill_qty = partial
                                        self._x_dm_fill_price = (
                                            self._x_dm_sell_partial_price
                                            if _up else
                                            self._x_dm_buy_partial_price)
                                        ok = await self._handle_dm_fill()
                                        if ok:
                                            self.state = State.IN_POSITION
                                        continue
                            continue

                        # ── Lighter drift force cancel (方向性备用逆选保护) ──
                        if self._dm_force_cancel:
                            self._dm_force_cancel = False
                            ls_now = (self.lighter_feed.snapshot
                                      if self.lighter_feed else None)
                            _l_mid = ls_now.mid if ls_now else 0
                            _drift = (abs(_l_mid - self._dm_lighter_mid_at_place)
                                      / self._dm_lighter_mid_at_place * 10000
                                      if self._dm_lighter_mid_at_place > 0
                                      else 0)
                            if _drift > self._dm_lighter_drift_bps:
                                _up = self._dm_drift_up
                                _risk = "sell" if _up else "buy"
                                _safe = "buy" if _up else "sell"
                                _arrow = "↑" if _up else "↓"
                                _risk_id = (self._x_maker_sell_id if _up
                                            else self._x_maker_buy_id)
                                if _risk_id:
                                    self.logger.warning(
                                        f"[逆选保护-L] Lighter{_arrow}"
                                        f"{_drift:.1f}bps "
                                        f">{self._dm_lighter_drift_bps}bps "
                                        f"撤{_risk}单(保留{_safe}) "
                                        f"冷却{self._bn_cancel_cooldown:.0f}s")
                                    if _risk == "buy":
                                        self._bn_buy_cancel_ts = time.time()
                                    else:
                                        self._bn_sell_cancel_ts = time.time()
                                    partial = await self._cancel_dm_order(
                                        _risk, force=True)
                                    if self._x_dm_fill_event.is_set():
                                        ok = await self._handle_dm_fill()
                                        if ok:
                                            self.state = State.IN_POSITION
                                        continue
                                    if partial > 0:
                                        self._x_dm_fill_side = _risk
                                        self._x_dm_fill_qty = partial
                                        self._x_dm_fill_price = (
                                            self._x_dm_sell_partial_price
                                            if _up else
                                            self._x_dm_buy_partial_price)
                                        ok = await self._handle_dm_fill()
                                        if ok:
                                            self.state = State.IN_POSITION
                                        continue
                            continue

                        # ── Manage buy side ──
                        if self._x_maker_buy_id is not None and buy_price is not None:
                            buy_age = now - self._x_maker_buy_placed_at
                            buy_drift = 0.0
                            if self._x_maker_buy_price > 0:
                                buy_drift = abs(float(
                                    (buy_price - self._x_maker_buy_price)
                                    / self._x_maker_buy_price * 10000))
                            _buy_same_price = (buy_price == self._x_maker_buy_price)

                            _buy_stale = (
                                self._x_maker_buy_price is not None
                                and buy_price is not None
                                and self._x_maker_buy_price > buy_price
                                and buy_age >= 5.0
                                and buy_drift > 1.0)
                            _max_age_s = self._max_quote_age_ms / 1000.0
                            need_replace_buy = (
                                _buy_stale
                                or (buy_age >= 2.0
                                    and not _buy_same_price
                                    and (buy_drift > self._price_tolerance_bps
                                         or buy_age > _max_age_s)))
                            if need_replace_buy and self._check_maker_cancel_rate():
                                partial = await self._cancel_dm_order("buy")
                                if self._x_dm_fill_event.is_set():
                                    ok = await self._handle_dm_fill()
                                    if ok:
                                        self.state = State.IN_POSITION
                                    continue
                                if partial > 0:
                                    self._x_dm_fill_side = "buy"
                                    self._x_dm_fill_qty = partial
                                    self._x_dm_fill_price = self._x_dm_buy_partial_price
                                    ok = await self._handle_dm_fill()
                                    if ok:
                                        self.state = State.IN_POSITION
                                    continue
                        elif self._x_maker_buy_id is not None and buy_price is None:
                            _b_age = now - self._x_maker_buy_placed_at
                            if _b_age >= _dm_min_age and self._check_maker_cancel_rate():
                                partial = await self._cancel_dm_order("buy")
                                if self._x_dm_fill_event.is_set():
                                    ok = await self._handle_dm_fill()
                                    if ok:
                                        self.state = State.IN_POSITION
                                    continue
                                if partial > 0:
                                    self._x_dm_fill_side = "buy"
                                    self._x_dm_fill_qty = partial
                                    self._x_dm_fill_price = self._x_dm_buy_partial_price
                                    ok = await self._handle_dm_fill()
                                    if ok:
                                        self.state = State.IN_POSITION
                                    continue

                        # ── Manage sell side ──
                        if self._x_maker_sell_id is not None and sell_price is not None:
                            sell_age = now - self._x_maker_sell_placed_at
                            sell_drift = 0.0
                            if self._x_maker_sell_price > 0:
                                sell_drift = abs(float(
                                    (sell_price - self._x_maker_sell_price)
                                    / self._x_maker_sell_price * 10000))
                            _sell_same_price = (sell_price == self._x_maker_sell_price)

                            _sell_stale = (
                                self._x_maker_sell_price is not None
                                and sell_price is not None
                                and self._x_maker_sell_price < sell_price
                                and sell_age >= 5.0
                                and sell_drift > 1.0)
                            need_replace_sell = (
                                _sell_stale
                                or (sell_age >= 2.0
                                    and not _sell_same_price
                                    and (sell_drift > self._price_tolerance_bps
                                         or sell_age > _max_age_s)))
                            if need_replace_sell and self._check_maker_cancel_rate():
                                partial = await self._cancel_dm_order("sell")
                                if self._x_dm_fill_event.is_set():
                                    ok = await self._handle_dm_fill()
                                    if ok:
                                        self.state = State.IN_POSITION
                                    continue
                                if partial > 0:
                                    self._x_dm_fill_side = "sell"
                                    self._x_dm_fill_qty = partial
                                    self._x_dm_fill_price = self._x_dm_sell_partial_price
                                    ok = await self._handle_dm_fill()
                                    if ok:
                                        self.state = State.IN_POSITION
                                    continue
                        elif self._x_maker_sell_id is not None and sell_price is None:
                            _s_age = now - self._x_maker_sell_placed_at
                            if _s_age >= _dm_min_age and self._check_maker_cancel_rate():
                                partial = await self._cancel_dm_order("sell")
                                if self._x_dm_fill_event.is_set():
                                    ok = await self._handle_dm_fill()
                                    if ok:
                                        self.state = State.IN_POSITION
                                    continue
                                if partial > 0:
                                    self._x_dm_fill_side = "sell"
                                    self._x_dm_fill_qty = partial
                                    self._x_dm_fill_price = self._x_dm_sell_partial_price
                                    ok = await self._handle_dm_fill()
                                    if ok:
                                        self.state = State.IN_POSITION
                                    continue

                        # ── Pre-trade freshness gate ──
                        _now2 = time.time()
                        _x_age2 = (_now2 - self.extended_client._ob_last_update_ts
                                   if self.extended_client
                                   and self.extended_client._ob_last_update_ts > 0
                                   else 999)
                        if _x_age2 > 3.0:
                            await asyncio.sleep(self.interval)
                            continue

                        # ── Place missing sides (with cooldown) ──
                        _both_empty = (self._x_maker_buy_id is None
                                       and self._x_maker_sell_id is None)
                        if (_both_empty
                                and now - self._dm_last_full_cancel_ts
                                < self._dm_repost_cooldown):
                            await asyncio.sleep(self.interval)
                            continue

                        _buy_cooled = (now - self._bn_buy_cancel_ts
                                       >= self._bn_cancel_cooldown)
                        _sell_cooled = (now - self._bn_sell_cancel_ts
                                        >= self._bn_cancel_cooldown)

                        if (self._x_maker_buy_id is None
                                and buy_price is not None and _buy_cooled):
                            if self._check_tx_rate():
                                await self._place_dm_order(
                                    "buy", self.order_size, buy_price)
                                await asyncio.sleep(0.05)
                                if self._x_dm_fill_event.is_set():
                                    ok = await self._handle_dm_fill()
                                    if ok:
                                        self.state = State.IN_POSITION
                                    continue

                        if (self._x_maker_sell_id is None
                                and sell_price is not None and _sell_cooled):
                            if self._check_tx_rate():
                                await self._place_dm_order(
                                    "sell", self.order_size, sell_price)
                                await asyncio.sleep(0.05)
                                if self._x_dm_fill_event.is_set():
                                    ok = await self._handle_dm_fill()
                                    if ok:
                                        self.state = State.IN_POSITION
                                    continue

                    # ── Taker mode (--no-maker fallback) ──
                    elif not self.maker_mode and not self.dry_run:
                        opp = self._check_open_opportunity(
                            l_bid, l_ask, x_ws_bid, x_ws_ask)
                        if opp:
                            direction, l_side, x_side, l_ref, x_ref, sp_bps = opp
                            self.logger.info(
                                f"[发现机会] {direction} 价差={sp_bps:.1f}bps")
                            ok = await self._open_position(
                                direction, l_side, x_side,
                                l_ref, x_ref, self.order_size, sp_bps)
                            if ok:
                                self.state = State.IN_POSITION
                            else:
                                await asyncio.sleep(1.0)

                # ━━━━━━ STATE: IN_POSITION ━━━━━━
                elif self.state == State.IN_POSITION:
                    if (abs(self.extended_position) < self.order_size * Decimal("0.01")
                            and abs(self.lighter_position) < self.order_size * Decimal("0.01")):
                        self._reset_position_state()
                        self.state = State.IDLE
                        continue

                    min_tradeable = self.order_size * Decimal("0.05")
                    x_dust = abs(self.extended_position) < min_tradeable
                    l_dust = abs(self.lighter_position) < min_tradeable
                    if x_dust and l_dust and not self.dry_run:
                        net_imb = abs(self.lighter_position + self.extended_position)
                        self.logger.info(
                            f"[残余清理] X={self.extended_position} "
                            f"L={self.lighter_position} → IDLE")
                        if net_imb > self.order_size * Decimal("0.5"):
                            await self._emergency_flatten()
                        self._reset_position_state()
                        self.state = State.IDLE
                        last_reconcile = time.time() - 50
                        continue

                    if (self.position_opened_at
                            and now - self.position_opened_at > self.max_hold_sec):
                        self.logger.warning(
                            f"[超时强平] 持仓{now - self.position_opened_at:.0f}s")
                        await self._emergency_flatten()
                        self._reset_position_state()
                        self.state = State.IDLE
                        last_reconcile = time.time() - 50
                        continue

                    x_zero = abs(self.extended_position) < self.order_size * Decimal("0.01")
                    l_zero = abs(self.lighter_position) < self.order_size * Decimal("0.01")
                    if x_zero != l_zero and not self.dry_run:
                        self.logger.warning("[单边仓位?] 先对账...")
                        await self._reconcile_positions()
                        x_zero2 = abs(self.extended_position) < self.order_size * Decimal("0.01")
                        l_zero2 = abs(self.lighter_position) < self.order_size * Decimal("0.01")
                        if x_zero2 != l_zero2:
                            await self._emergency_flatten()
                            self._reset_position_state()
                            self.state = State.IDLE
                            last_reconcile = time.time() - 50
                            continue
                        last_reconcile = now

                    if (x_ws_bid is not None and x_ws_ask is not None
                            and not self.dry_run):

                        just_traded = (now - self._last_trade_time < self._min_hold_sec)

                        # ── Ladder ──
                        ladder_opened = False
                        if (self.ladder_step > 0
                                and self._tier_closed_pct <= 0
                                and cur_pos < self.max_position - self.order_size * Decimal("0.01")
                                and self.position_direction
                                and now - self._last_ladder_attempt > self._ladder_cooldown
                                and not just_traded):
                            hold_elapsed = (now - self.position_opened_at
                                            if self.position_opened_at else 0)
                            if hold_elapsed <= self.max_hold_sec * 0.7:
                                if self.position_direction == "long_X_short_L":
                                    dir_sp = float((l_mid - x_mid) / l_mid * 10000) if l_mid > 0 else 0.0
                                else:
                                    dir_sp = float((x_mid - l_mid) / l_mid * 10000) if l_mid > 0 else 0.0
                                _ladder_fee = self.PAIR_FEE_TT
                                if dir_sp > (self._last_open_spread_bps + self.ladder_step) and dir_sp >= _ladder_fee:
                                    self._last_ladder_attempt = now
                                    opp = self._check_open_opportunity(
                                        l_bid, l_ask, x_ws_bid, x_ws_ask)
                                    if opp and opp[0] == self.position_direction:
                                        d, ls2, xs, lr, xr, sp = opp
                                        if sp < _ladder_fee:
                                            self.logger.info(
                                                f"[Ladder] spread={sp:.1f}bps < "
                                                f"TT费{_ladder_fee:.1f}bps, 跳过")
                                        else:
                                            ok = await self._open_position(
                                                d, ls2, xs, lr, xr,
                                                self.order_size, sp)
                                            if ok:
                                                ladder_opened = True
                                                self._total_open_qty = min(
                                                    abs(self.extended_position),
                                                    abs(self.lighter_position))
                                                self._tier_closed_pct = 0.0
                                                self._tier_close_records.clear()
                                            else:
                                                await asyncio.sleep(1.0)

                        # ── Re-fill ──
                        refilled = False
                        if (not just_traded and not ladder_opened
                                and self._tier_closed_pct > 0
                                and self._tier_closed_pct < 100
                                and self._total_open_qty > 0
                                and self.position_direction
                                and now - self._last_trade_time > self._ladder_cooldown):
                            opp = self._check_open_opportunity(
                                l_bid, l_ask, x_ws_bid, x_ws_ask)
                            if opp and opp[0] == self.position_direction:
                                cur_x = abs(self.extended_position)
                                cur_l = abs(self.lighter_position)
                                cur_min = min(cur_x, cur_l)
                                refill_qty = self._total_open_qty - cur_min
                                if refill_qty > Decimal("0"):
                                    refill_qty = refill_qty.quantize(
                                        self.extended_min_order_size,
                                        rounding=ROUND_DOWN)
                                if refill_qty >= self.order_size * Decimal("0.05"):
                                    d, ls2, xs, lr, xr, sp = opp
                                    _refill_fee = self.PAIR_FEE_TT
                                    if sp < _refill_fee:
                                        self.logger.info(
                                            f"[回补] 价差={sp:.1f}bps < "
                                            f"TT费{_refill_fee:.1f}bps, 跳过")
                                    else:
                                        self.logger.info(
                                            f"[回补] 价差={sp:.1f}bps "
                                            f"回补 {refill_qty}")
                                        ok = await self._open_position(
                                            d, ls2, xs, lr, xr, refill_qty, sp)
                                        if ok:
                                            refilled = True
                                            self._tier_closed_pct = 0.0
                                            self._tier_close_records.clear()
                                            self._total_open_qty = min(
                                                abs(self.extended_position),
                                                abs(self.lighter_position))
                                        else:
                                            self._last_trade_time = time.time()

                        # ── Close check (V18: maker-maker优先, taker兜底) ──
                        if not just_traded and not ladder_opened and not refilled:
                            tier_hit = self._check_close_condition(
                                l_bid, l_ask, x_ws_bid, x_ws_ask)
                            if tier_hit is not None:
                                _tier_sp, _batch_pct = tier_hit
                                close_ok = await self._close_position_mm(
                                    l_bid, l_ask, x_ws_bid, x_ws_ask,
                                    batch_pct=_batch_pct)
                                if not close_ok:
                                    close_ok = await self._close_position(
                                        l_bid, l_ask, x_ws_bid, x_ws_ask,
                                        batch_pct=_batch_pct)
                                if not close_ok:
                                    await asyncio.sleep(1.0)

                self._bbo_event.clear()
                try:
                    await asyncio.wait_for(
                        self._bbo_event.wait(), timeout=self.interval)
                except asyncio.TimeoutError:
                    pass

            except Exception as e:
                self.logger.error(
                    f"主循环异常: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(2)

    # ═══════════════════════════════════════════════
    #  P&L / Reset
    # ═══════════════════════════════════════════════

    def _log_ref_pnl(self, close_x_ref: Decimal, close_l_ref: Decimal,
                     close_qty: Optional[Decimal] = None,
                     x_estimated: bool = False,
                     route_fee_bps: Optional[float] = None) -> float:
        if not self._open_x_refs or not self._open_l_refs:
            return 0.0
        x_open_qty = sum(q for _, q in self._open_x_refs)
        l_open_qty = sum(q for _, q in self._open_l_refs)
        x_avg = sum(p * q for p, q in self._open_x_refs) / x_open_qty
        l_avg = sum(p * q for p, q in self._open_l_refs) / l_open_qty
        qty = close_qty if close_qty else max(x_open_qty, l_open_qty)

        if self.position_direction == "long_X_short_L":
            x_pnl = float((close_x_ref - x_avg) * qty)
            l_pnl = float((l_avg - close_l_ref) * qty)
        else:
            x_pnl = float((x_avg - close_x_ref) * qty)
            l_pnl = float((close_l_ref - l_avg) * qty)

        _fee = route_fee_bps if route_fee_bps is not None else self.ROUND_TRIP_FEE_BPS
        total_gross = x_pnl + l_pnl
        notional = float(qty) * float(close_x_ref)
        fee_cost = notional * _fee / 10000
        total_net = total_gross - fee_cost
        gross_bps = total_gross / notional * 10000 if notional > 0 else 0.0
        net_bps = total_net / notional * 10000 if notional > 0 else 0.0
        est = "(估) " if x_estimated else ""
        self.logger.info(
            f"[P&L] {est}qty={qty} X={x_pnl:+.3f}$ L={l_pnl:+.3f}$ "
            f"毛利={total_gross:+.3f}$({gross_bps:+.1f}bps) "
            f"手续费={fee_cost:.3f}$ "
            f"净利={total_net:+.3f}$({net_bps:+.1f}bps)")
        return net_bps

    def _normalize_refs(self, remaining_qty: Decimal):
        if remaining_qty <= 0:
            return
        if self._open_x_refs:
            x_qty = sum(q for _, q in self._open_x_refs)
            if x_qty > remaining_qty:
                x_avg = sum(p * q for p, q in self._open_x_refs) / x_qty
                self._open_x_refs = [(x_avg, remaining_qty)]
        if self._open_l_refs:
            l_qty = sum(q for _, q in self._open_l_refs)
            if l_qty > remaining_qty:
                l_avg = sum(p * q for p, q in self._open_l_refs) / l_qty
                self._open_l_refs = [(l_avg, remaining_qty)]

    def _reset_position_state(self):
        self.position_opened_at = None
        self.position_direction = None
        self._open_x_refs.clear()
        self._open_l_refs.clear()
        self._last_open_spread_bps = 0.0
        self._open_dev_l_bps = 0.0
        self._open_dev_x_bps = 0.0
        self._open_excess_bps = 0.0
        self._open_theoretical_spread = 0.0
        self._tier_closed_pct = 0.0
        self._tier_close_records.clear()
        self._total_open_qty = Decimal("0")
        self._current_trade_id = ""
        self._open_route_fee_bps = 0.0
        self._open_hedge_latency_ms = 0.0
        self._open_bn_vol = 0.0
        self._open_x_side = ""
        self._open_route_tag = ""
        self._reset_dm_fill_state()


# ═══════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════

def _parse_close_tiers(s: str) -> List[Tuple[float, float]]:
    tiers = []
    for part in s.split(","):
        part = part.strip()
        if ":" not in part:
            raise ValueError(f"Invalid tier format '{part}', expected 'spread:pct'")
        sp, pct = part.split(":", 1)
        tiers.append((float(sp), float(pct)))
    tiers.sort(key=lambda t: -t[0])
    return tiers


def main():
    p = argparse.ArgumentParser(
        description="V18 Maker-Maker套利 (Lighter+Extended)")
    p.add_argument("-s", "--symbol", default="ETH")
    p.add_argument("--size", type=str, default="0.1")
    p.add_argument("--max-position", type=str, default="0.5")
    p.add_argument("--target-spread", type=float, default=3.5,
                   help="开仓价差目标bps (maker模式下作为动态阈值预热后备)")
    p.add_argument("--close-gap-bps", type=float, default=1.0,
                   help="最终平仓阈值bps, 分批模式下作为最后一层兜底")
    p.add_argument("--close-tiers", type=str, default="",
                   help="分批平仓 'spread1:pct1,spread2:pct2'")
    p.add_argument("--max-hold-sec", type=float, default=1800.0)
    p.add_argument("--max-slippage-bps", type=float, default=5.0)
    p.add_argument("--interval", type=float, default=0.02)
    p.add_argument("--ladder-step", type=float, default=0.0)
    p.add_argument("--ladder-cooldown", type=float, default=10.0)
    p.add_argument("--no-maker", action="store_true",
                   help="禁用Maker模式, 回退到V14双taker逻辑(往返5bps)")
    p.add_argument("--dyn-window", type=float, default=300.0,
                   help="动态阈值观测窗口秒数 (默认300)")
    p.add_argument("--dyn-percentile", type=float, default=75.0,
                   help="动态阈值百分位 (默认75)")
    p.add_argument("--price-tolerance", type=float, default=2.5,
                   help="maker价格更新容忍度bps (默认2.5)")
    p.add_argument("--maker-max-age", type=float, default=25.0,
                   help="单笔maker最大挂单秒数 (默认25)")
    p.add_argument("--maker-close-timeout", type=float, default=30.0,
                   help="平仓maker等待秒数 (默认30)")
    p.add_argument("--hedge-timeout", type=float, default=3.0,
                   help="成交后对冲最大秒数 (默认3)")
    p.add_argument("--min-spread-gap", type=float, default=2.0,
                   help="buy/sell maker之间的最小价差间距bps (默认2.0)")
    p.add_argument("--repost-cooldown", type=float, default=3.0,
                   help="全部撤单后重新挂单冷却秒数 (默认3)")
    p.add_argument("--bn-drift", type=float, default=1.5,
                   help="Binance 漂移撤单阈值bps (默认2.0)")
    p.add_argument("--bn-vol-wide", type=float, default=3.0,
                   help="Binance 高波动阈值bps → 加宽价差 (默认3.0)")
    p.add_argument("--reverse-spread", type=float, default=1.0,
                   help="负价差反转阈值bps: 开仓价差<-X则立即反转 (默认1.0)")
    p.add_argument("--flatten-on-start", action="store_true",
                   help="启动时检测到遗留仓位自动紧急平仓而非恢复")
    p.add_argument("--smart-hedge-wait", type=float, default=3.0,
                   help="V18: 对冲分级maker最大等待秒数 (默认3.0)")
    p.add_argument("--smart-hedge-edge", type=float, default=2.0,
                   help="V18: 对冲允许用maker的最低edgebps (默认2.0)")
    p.add_argument("--smart-close-wait", type=float, default=5.0,
                   help="V18: 平仓分级maker最大等待秒数 (默认5.0)")
    p.add_argument("--smart-close-gap", type=float, default=1.5,
                   help="V18: 平仓允许用maker的最低残余gapbps (默认1.5)")
    p.add_argument("--maker-adjust-interval", type=float, default=2.0,
                   help="V18: maker渐进调价间隔秒数 (默认2.0)")
    p.add_argument("--maker-adjust-bps", type=float, default=3.0,
                   help="V18: 每次调价幅度bps (默认3.0)")
    p.add_argument("--maker-bn-emergency", type=float, default=5.0,
                   help="V18: BN紧急漂移阈值bps → 立即回退taker (默认5.0)")
    p.add_argument("--adverse-buffer", type=float, default=0.5,
                   help="V18: 逆选缓冲bps,买更低卖更高 (默认0.5)")
    p.add_argument("--recovery-window", type=float, default=500.0,
                   help="V18: 负价差后recovery窗口ms (默认500)")
    p.add_argument("--max-quote-age", type=float, default=8000.0,
                   help="V18: 最大挂单存活ms (默认8000)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--env-file", default=".env")
    args = p.parse_args()

    load_dotenv(args.env_file)

    if args.target_spread < 0:
        print(f"[警告] target_spread={args.target_spread} 为负数，已修正为 0")
        args.target_spread = 0

    close_tiers = None
    if args.close_tiers.strip():
        try:
            close_tiers = _parse_close_tiers(args.close_tiers)
            print(f"[分批平仓] 层级: {close_tiers}")
        except ValueError as e:
            print(f"[错误] --close-tiers 格式错误: {e}")
            sys.exit(1)

    bot = TakerBot(
        symbol=args.symbol.upper(),
        order_size=Decimal(args.size),
        max_position=Decimal(args.max_position),
        target_spread=args.target_spread,
        close_gap_bps=args.close_gap_bps,
        max_hold_sec=args.max_hold_sec,
        interval=args.interval,
        dry_run=args.dry_run,
        ladder_step=args.ladder_step,
        close_tiers=close_tiers,
    )
    bot._max_slippage_bps = args.max_slippage_bps
    bot._ladder_cooldown = args.ladder_cooldown
    bot.flatten_on_start = args.flatten_on_start

    if args.no_maker:
        bot.maker_mode = False
        bot.ROUND_TRIP_FEE_BPS = bot.PAIR_FEE_TT
        print(f"[模式] 双Taker模式 (往返≈{bot.PAIR_FEE_TT:.1f}bps)")
        if args.target_spread < 6.0:
            print(f"[提示] 双taker模式需≥6bps覆盖费用, "
                  f"已将target_spread从{args.target_spread}调整为6.0")
            bot.target_spread = 6.0
    else:
        bot.maker_mode = True
        bot.ROUND_TRIP_FEE_BPS = bot.PAIR_FEE_MT
        bot._dyn_window = args.dyn_window
        bot._dyn_percentile = args.dyn_percentile
        bot._price_tolerance_bps = args.price_tolerance
        bot._maker_max_age = args.maker_max_age
        bot._x_maker_close_timeout = args.maker_close_timeout
        bot._hedge_timeout = args.hedge_timeout
        bot._min_spread_gap_bps = args.min_spread_gap
        bot._dm_repost_cooldown = args.repost_cooldown
        bot._bn_drift_bps = args.bn_drift
        bot._bn_vol_wide_threshold = args.bn_vol_wide
        bot._neg_spread_reverse_bps = args.reverse_spread
        bot._smart_hedge_max_wait = args.smart_hedge_wait
        bot._smart_hedge_min_edge = args.smart_hedge_edge
        bot._smart_close_max_wait = args.smart_close_wait
        bot._smart_close_min_gap = args.smart_close_gap
        bot._maker_adjust_interval = args.maker_adjust_interval
        bot._maker_adjust_bps = args.maker_adjust_bps
        bot._maker_bn_emergency_bps = args.maker_bn_emergency
        bot._adverse_buffer_bps = args.adverse_buffer
        bot._recovery_window_ms = args.recovery_window
        bot._max_quote_age_ms = args.max_quote_age
        bot._dyn_fee_floor = 0.5
        bot.ROUND_TRIP_FEE_BPS = bot.PAIR_FEE_MM
        bot.PAIR_FEE_MM = 0.5
        bot.PAIR_FEE_MT = 2.5
        bot.PAIR_FEE_TT = 5.0
        print(f"[模式] ★ V18-Maker-Maker套利 "
              f"(MM≈{bot.PAIR_FEE_MM:.1f} MT≈{bot.PAIR_FEE_MT:.1f} TT≈{bot.PAIR_FEE_TT:.1f}bps)")
        print(f"  Binance逆选保护: 漂移>{args.bn_drift}bps方向性撤单 "
              f"高波动>{args.bn_vol_wide}bps加宽价差")
        print(f"  逆选反转阈值: <-{bot._neg_spread_reverse_bps:.1f}bps 即反向平仓")
        print(f"  费用底线(maker挂单): {bot._dyn_fee_floor:.1f}bps  "
              f"费用底线(taker加仓): {bot.PAIR_FEE_TT:.1f}bps")
        print(f"  分级对冲: edge≥{args.smart_hedge_edge:.1f}bps+BN稳→"
              f"maker({args.smart_hedge_wait:.0f}s), 否则立即taker")
        print(f"  分级平仓: gap≥{args.smart_close_gap:.1f}bps+BN稳→"
              f"maker({args.smart_close_wait:.0f}s), 否则立即taker")
        print(f"  调价={args.maker_adjust_bps:.1f}bps/{args.maker_adjust_interval:.1f}s "
              f"BN紧急={args.maker_bn_emergency:.1f}bps")
        print(f"  逆选防护: 缓冲={args.adverse_buffer:.1f}bps "
              f"recovery={args.recovery_window:.0f}ms "
              f"最大挂龄={args.max_quote_age:.0f}ms")
        print(f"  动态阈值: 窗口={args.dyn_window}s P{args.dyn_percentile:.0f} "
              f"价格容忍={args.price_tolerance}bps "
              f"最大挂龄={args.maker_max_age}s "
              f"价差间距={args.min_spread_gap}bps")

    if uvloop is not None:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("[uvloop] 已启用高性能事件循环")
    else:
        print("[uvloop] 未安装, 使用默认 asyncio")

    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n已中断。")


if __name__ == "__main__":
    main()
