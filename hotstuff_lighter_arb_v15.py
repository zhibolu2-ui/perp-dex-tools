#!/usr/bin/env python3
"""
Hotstuff + Lighter 价差收敛套利 V15（主动 Maker-First + 动态阈值）

Hotstuff maker 模式使用 POST_ONLY (po=True, tif="GTC") 挂单,
通过 REST 仓位轮询检测成交（Hotstuff WS feeds/hotstuff_feed.py 仅有 BBO 无订单事件）。

在 V14 基础上全面升级：
  - 主动挂单策略：在 Hotstuff 上持续维护 POST_ONLY maker 单，被动等待成交后
    立即在 Lighter 上 taker 对冲。Maker 先行 → 未成交即撤单 → 零风险暴露。
  - 百分位动态阈值：根据近 N 分钟观测到的价差分布自适应调整开仓阈值，
    波动大时自动保守，波动小时更激进。
  - 费用降低：Hotstuff maker -0.2bps rebate + Lighter taker 0% → 往返≈2.3 bps（含缓冲）
  - 保留 V14 全部功能：分批平仓、加权收敛跟踪、阶梯加仓、回补、尘仓清理
  - --no-maker 回退到 V14 双 taker 逻辑（往返≈5.0 bps）

策略（Maker 模式）：
  开仓: 根据 Lighter BBO + 动态阈值 计算 Hotstuff maker 价格 → 持续挂单 →
        成交后（REST仓位变化检测）立即 Lighter taker 对冲
  平仓: 分批触发 → Hotstuff maker 平仓 → 成交后 Lighter taker 对冲
        超时回退到双 taker
  阶梯/回补: 沿用 V14 逻辑，执行方式走 maker 通道

费用:
  Maker 模式: ≈2.3 bps 往返 (Hotstuff maker -0.2bps rebate + Lighter taker 0%, 含缓冲)
  Taker 回退: ≈5.0 bps 往返 (Hotstuff taker 2.5bps×2)

用法:
  python hotstuff_lighter_arb_v15.py --symbol BTC --size 0.005 --target-spread 3.0 \\
      --close-tiers "1.5:50,0.5:100" --dyn-percentile 75
  python hotstuff_lighter_arb_v15.py --symbol ETH --no-maker --target-spread 7.5
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
from datetime import datetime, timezone
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
from eth_account import Account
from lighter.signer_client import SignerClient

from hotstuff import ExchangeClient, InfoClient
from hotstuff.methods.exchange.trading import (
    UnitOrder, PlaceOrderParams, CancelAllParams,
)
from hotstuff.methods.info.account import PositionsParams

sys.path.insert(0, str(Path(__file__).resolve().parent))

from feeds.lighter_feed import LighterFeed
from feeds.hotstuff_feed import HotstuffFeed


class State(str, Enum):
    IDLE = "IDLE"
    IN_POSITION = "IN_POSITION"


class _Config:
    def __init__(self, d: dict):
        for k, v in d.items():
            setattr(self, k, v)


class TakerBot:

    ROUND_TRIP_FEE_BPS_TAKER = 5.0
    ROUND_TRIP_FEE_BPS_MAKER = 2.3

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

        self.lighter_position = Decimal("0")
        self.hotstuff_position = Decimal("0")
        self.position_opened_at: Optional[float] = None
        self.position_direction: Optional[str] = None

        self._open_h_refs: List[Tuple[Decimal, Decimal]] = []
        self._open_l_refs: List[Tuple[Decimal, Decimal]] = []
        self._last_open_spread_bps: float = 0.0
        self._last_ladder_attempt: float = 0.0

        self._tier_closed_pct: float = 0.0
        self._tier_close_records: List[dict] = []
        self._total_open_qty: Decimal = Decimal("0")

        # ── Hotstuff Maker (POST_ONLY) tracking ──
        self._hs_maker_placed: bool = False
        self._hs_maker_pre_pos: Decimal = Decimal("0")
        self._hs_maker_side: Optional[str] = None
        self._hs_maker_qty: Decimal = Decimal("0")

        # ── Proactive maker management state ──
        self.maker_mode: bool = True
        self.ROUND_TRIP_FEE_BPS: float = self.ROUND_TRIP_FEE_BPS_MAKER
        self._maker_placed_at: float = 0.0
        self._maker_current_price: Decimal = Decimal("0")
        self._maker_current_direction: Optional[str] = None
        self._maker_current_h_side: Optional[str] = None
        self._maker_last_direction: Optional[str] = None
        self._maker_direction_change_ts: float = 0.0
        self._price_tolerance_bps: float = 0.5
        self._maker_max_age: float = 30.0
        self._direction_cooldown: float = 2.0
        self._hedge_timeout: float = 5.0
        self._maker_cancel_timestamps: List[float] = []
        self._maker_cancel_cooldown: float = 0.2
        self._consecutive_hedge_failures: int = 0
        self._max_hedge_failures: int = 5
        self._hedge_circuit_breaker_until: float = 0.0
        self._hedge_circuit_pause: float = 60.0
        self._maker_close_timeout: float = 30.0

        # ── Dynamic threshold state ──
        self._spread_obs: collections.deque = collections.deque(maxlen=50000)
        self._dyn_window: float = 300.0
        self._dyn_percentile: float = 75.0
        self._dyn_min_samples: int = 100
        self._dyn_recalc_interval: float = 5.0
        self._dyn_last_calc_ts: float = 0.0
        self._dyn_current_threshold: float = target_spread
        self._dyn_fee_floor: float = self.ROUND_TRIP_FEE_BPS_MAKER + 0.3

        # ── Lighter WS ──
        self._lighter_fill_event = asyncio.Event()
        self._lighter_fill_data: Optional[dict] = None
        self._last_lighter_avg_price: Optional[Decimal] = None
        self._lighter_ws_task: Optional[asyncio.Task] = None
        self._lighter_pending_coi: Optional[int] = None

        self.lighter_client: Optional[SignerClient] = None
        self.hs_exchange: Optional[ExchangeClient] = None
        self.hs_info: Optional[InfoClient] = None
        self.lighter_feed: Optional[LighterFeed] = None
        self.hs_feed: Optional[HotstuffFeed] = None

        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
        self.api_key_index = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))
        self.lighter_market_index: int = 0
        self.base_amount_multiplier: int = 0
        self.price_multiplier: int = 0
        self.lighter_tick: Decimal = Decimal("0.01")

        self.hs_address: str = ""
        self.hs_instrument_id: int = 0
        self.hs_tick_size: Decimal = Decimal("0.01")
        self.hs_lot_size: Decimal = Decimal("0.001")
        self.hs_symbol: str = f"{self.symbol}-PERP"

        self.cumulative_net_bps: float = 0.0
        self._nonce_counter: int = 0
        self._nonce_error_count: int = 0

        self._tx_timestamps: List[float] = []
        self._tx_backoff_until: float = 0.0

        self._trade_stats: List[dict] = []
        self._open_dev_l_bps: float = 0.0
        self._open_dev_h_bps: float = 0.0
        self._open_excess_bps: float = 0.0
        self._open_theoretical_spread: float = 0.0
        self._last_h_fill_bbo: Optional[Tuple] = None

        self._force_reconcile: bool = False
        self._trade_lock = asyncio.Lock()
        self._last_trade_time: float = 0.0
        self._min_hold_sec: float = 5.0
        self._ladder_cooldown: float = 10.0
        self._max_slippage_bps: float = 5.0
        self._aio_session: Optional[aiohttp.ClientSession] = None
        self._bbo_event = asyncio.Event()

        os.makedirs("logs", exist_ok=True)
        self.csv_path = f"logs/arb_v15_hs_{self.symbol}_trades.csv"
        self.log_path = f"logs/arb_v15_hs_{self.symbol}.log"
        self._init_csv()
        self.logger = self._init_logger()

    # ═══════════════════════════════════════════════
    #  Logging / CSV
    # ═══════════════════════════════════════════════

    def _init_logger(self) -> logging.Logger:
        lg = logging.getLogger(f"hs_arb_v15_{self.symbol}")
        lg.setLevel(logging.INFO)
        lg.handlers.clear()
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
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
                    "hotstuff_side", "hotstuff_ref", "hotstuff_fill", "hotstuff_qty",
                    "spread_bps", "total_ms", "mode",
                    "dev_l", "dev_h", "excess_bps", "actual_net_bps",
                    "net_bps", "cumulative_net_bps",
                ])

    def _csv_row(self, row: list):
        with open(self.csv_path, "a", newline="") as f:
            csv.writer(f).writerow(row)

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

    def _init_hotstuff_client(self):
        pk = os.getenv("HOTSTUFF_PRIVATE_KEY")
        if not pk:
            raise ValueError("HOTSTUFF_PRIVATE_KEY not set")
        wallet = Account.from_key(pk)
        api_wallet_addr = wallet.address
        self.hs_address = os.getenv("HOTSTUFF_ADDRESS", api_wallet_addr)
        self.hs_api_wallet_addr = api_wallet_addr
        self.hs_exchange = ExchangeClient(wallet=wallet)
        self.hs_info = InfoClient()
        is_agent = self.hs_address.lower() != api_wallet_addr.lower()
        self.logger.info(
            f"Hotstuff client 已初始化  "
            f"{'agent模式' if is_agent else 'direct模式'}"
            f"  main={self.hs_address[:10]}…"
            f"  api={api_wallet_addr[:10]}…")

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
                    f"Hotstuff instrument: {self.hs_symbol} id={self.hs_instrument_id} "
                    f"tick={self.hs_tick_size} lot={self.hs_lot_size} "
                    f"maxLev={max_lev}")
                return
        raise RuntimeError(f"{self.hs_symbol} not found on Hotstuff")

    # ═══════════════════════════════════════════════
    #  SDK Validation — POST_ONLY support check
    # ═══════════════════════════════════════════════

    async def _validate_post_only(self) -> bool:
        """Try a small POST_ONLY order at unrealistic price to verify SDK support."""
        self.logger.info("[SDK验证] 测试 Hotstuff POST_ONLY (po=True, tif=GTC)...")
        try:
            test_price = Decimal("1.00")
            order = UnitOrder(
                instrumentId=self.hs_instrument_id,
                side="b",
                positionSide="BOTH",
                price=str(test_price),
                size=str(self.hs_lot_size),
                tif="GTC",
                ro=False,
                po=True,
                isMarket=False,
            )
            params = PlaceOrderParams(
                orders=[order],
                expiresAfter=int(time.time() * 1000) + 10_000,
            )
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, self.hs_exchange.place_order, params)
            if isinstance(result, list):
                result = result[0] if result else {}
            if not isinstance(result, dict):
                result = {"raw": result}
            error = result.get("error", "")
            if error and ("post" in str(error).lower() or "po" in str(error).lower()):
                self.logger.warning(f"[SDK验证] POST_ONLY 不支持: {error}")
                return False
            await self._cancel_all_hotstuff_orders()
            self.logger.info("[SDK验证] POST_ONLY 支持确认")
            return True
        except Exception as e:
            err_str = str(e).lower()
            if "post" in err_str or "po" in err_str or "unsupported" in err_str:
                self.logger.warning(f"[SDK验证] POST_ONLY 不支持: {e}")
                return False
            self.logger.info(f"[SDK验证] 异常(可忽略): {e}")
            try:
                await self._cancel_all_hotstuff_orders()
            except Exception:
                pass
            return True

    # ═══════════════════════════════════════════════
    #  BBO
    # ═══════════════════════════════════════════════

    def _get_hotstuff_ws_bbo(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        if self.hs_feed and self.hs_feed.snapshot.connected:
            snap = self.hs_feed.snapshot
            if snap.best_bid and snap.best_ask:
                return Decimal(str(snap.best_bid)), Decimal(str(snap.best_ask))
        return None, None

    def _get_l1_depth(self, l_side: str, h_side: str
                      ) -> Tuple[Decimal, Decimal]:
        ls = self.lighter_feed.snapshot if self.lighter_feed else None
        hs = self.hs_feed.snapshot if self.hs_feed else None

        if l_side == "sell" and ls and ls.bids:
            l1_l = Decimal(str(ls.bids[0].size))
        elif l_side == "buy" and ls and ls.asks:
            l1_l = Decimal(str(ls.asks[0].size))
        else:
            l1_l = Decimal("999")

        if h_side == "buy" and hs and hs.asks:
            l1_h = Decimal(str(hs.asks[0].size))
        elif h_side == "sell" and hs and hs.bids:
            l1_h = Decimal(str(hs.bids[0].size))
        else:
            l1_h = Decimal("999")

        return l1_l, l1_h

    def _on_bbo_update(self):
        self._bbo_event.set()

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
        if len(self._maker_cancel_timestamps) >= 30:
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
                                    ref_price: Decimal) -> Optional[Decimal]:
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
                reduce_only=False,
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
                await asyncio.wait_for(self._lighter_fill_event.wait(), timeout=0.8)
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
                        actual_qty = abs(delta)
                        if self._last_lighter_avg_price is None:
                            self._last_lighter_avg_price = ref_price
                            self.logger.warning(
                                f"[Lighter] REST成交确认(第{attempt+1}次): "
                                f"pos {pre_pos} → {new_pos} Δ={actual_qty}"
                                f" 价格使用ref={ref_price:.2f}(估)")
                        else:
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
    #  Hotstuff IOC (fallback for --no-maker)
    # ═══════════════════════════════════════════════

    _last_hotstuff_avg_price: Optional[Decimal] = None
    _last_hs_fill_id: Optional[str] = None
    _fills_api_broken: bool = False

    async def _fetch_hotstuff_fill_price(self, fallback: Decimal,
                                          max_attempts: int = 4,
                                          delay: float = 0.3,
                                          is_close: bool = False):
        self._last_hotstuff_avg_price = None

        if self._fills_api_broken:
            await self._use_fallback_price(fallback, is_close)
            return

        from hotstuff.methods.info.account import FillsParams
        import inspect
        fp_params = inspect.signature(FillsParams).parameters

        addr_key = None
        for c in ("address", "user", "account"):
            if c in fp_params:
                addr_key = c
                break

        addrs_to_try = [self.hs_address]
        if (hasattr(self, 'hs_api_wallet_addr')
                and self.hs_api_wallet_addr.lower() != self.hs_address.lower()):
            addrs_to_try.append(self.hs_api_wallet_addr)

        if not hasattr(self, '_fills_debug_done'):
            self._fills_debug_done = True
            self.logger.info(
                f"[fills调试] FillsParams签名: {list(fp_params.keys())} "
                f"addr_key={addr_key}")

        def _build_params(addr, page_val):
            kw = {}
            if addr_key:
                kw[addr_key] = addr
            if "page" in fp_params:
                kw["page"] = page_val
            if "limit" in fp_params:
                kw["limit"] = 10
            return FillsParams(**kw)

        loop = asyncio.get_event_loop()

        if not hasattr(self, '_fills_working_addr'):
            found = False
            for addr in addrs_to_try:
                for pv in (0, 1):
                    try:
                        p = _build_params(addr, pv)
                        r = await loop.run_in_executor(
                            None, self.hs_info.fills, p)
                        fills = (r.fills if hasattr(r, "fills")
                                 else r.get("fills", []) if isinstance(r, dict)
                                 else r if isinstance(r, list) else [])
                        if fills:
                            self._fills_working_addr = addr
                            self._fills_working_page = pv
                            found = True
                            break
                    except Exception:
                        pass
                if found:
                    break
            if not found:
                self._fills_api_broken = True
                self.logger.warning(
                    "[Hotstuff] fills API 所有地址均返回空, "
                    "后续使用BBO/持仓均价估算")
                await self._use_fallback_price(fallback, is_close)
                return

        fills_addr = getattr(self, '_fills_working_addr', self.hs_address)
        fills_page = getattr(self, '_fills_working_page', 0)

        for attempt in range(max_attempts):
            if attempt > 0:
                await asyncio.sleep(delay)
            try:
                params = _build_params(fills_addr, fills_page)
                resp = await loop.run_in_executor(
                    None, self.hs_info.fills, params)
                fills = (resp.fills if hasattr(resp, "fills")
                         else resp.get("fills", []) if isinstance(resp, dict)
                         else resp if isinstance(resp, list) else [])
                if not fills:
                    continue

                matching = []
                for ff in fills:
                    inst = (ff.get("instrument") if isinstance(ff, dict)
                            else getattr(ff, "instrument", None))
                    sym = (ff.get("symbol") if isinstance(ff, dict)
                           else getattr(ff, "symbol", None))
                    if (inst == self.hs_symbol or sym == self.hs_symbol
                            or (inst is None and sym is None)):
                        matching.append(ff)
                if not matching:
                    matching = fills

                f = matching[0]
                fill_id = None
                for idf in ("id", "fillId", "fill_id",
                             "tradeId", "trade_id"):
                    fill_id = (f.get(idf) if isinstance(f, dict)
                               else getattr(f, idf, None))
                    if fill_id is not None:
                        break
                if fill_id and fill_id == self._last_hs_fill_id:
                    if attempt < max_attempts - 1:
                        continue

                price = None
                for pf in ("price", "fillPrice", "fill_price",
                           "avgPrice", "avg_price",
                           "executionPrice", "execution_price"):
                    raw = (f.get(pf) if isinstance(f, dict)
                           else getattr(f, pf, None))
                    if raw is not None:
                        try:
                            price = Decimal(str(raw))
                            if price > 0:
                                break
                        except Exception:
                            pass

                if price and price > 0:
                    diff_bps = abs(float(
                        (price - fallback) / fallback * 10000
                    )) if fallback > 0 else 0
                    if diff_bps > 50 and attempt < max_attempts - 1:
                        continue
                    self._last_hotstuff_avg_price = price
                    if fill_id:
                        self._last_hs_fill_id = fill_id
                    self.logger.info(
                        f"[Hotstuff] 成交价确认: {price:.2f} "
                        f"(ref={fallback:.2f} 偏差={diff_bps:.1f}bps)")
                    return
            except Exception as e:
                self.logger.debug(
                    f"[Hotstuff] fills API attempt {attempt+1}: {e}")

        await self._use_fallback_price(fallback, is_close)

    async def _use_fallback_price(self, fallback: Decimal, is_close: bool):
        if not is_close:
            await asyncio.sleep(0.05)
            entry_price = await self._get_hotstuff_entry_price()
            if entry_price is not None and entry_price > 0:
                diff_bps = abs(float(
                    (entry_price - fallback) / fallback * 10000
                )) if fallback > 0 else 0
                if diff_bps < 100:
                    self._last_hotstuff_avg_price = entry_price
                    self.logger.info(
                        f"[Hotstuff] 持仓均价={entry_price:.2f} "
                        f"(ref={fallback:.2f} 差={diff_bps:.1f}bps)")
                    return

        bbo = getattr(self, '_last_h_fill_bbo', None)
        if bbo:
            bid, ask, side = bbo
            if side == "buy" and ask and ask > 0:
                self._last_hotstuff_avg_price = ask
                return
            if side == "sell" and bid and bid > 0:
                self._last_hotstuff_avg_price = bid
                return

        self._last_hotstuff_avg_price = fallback
        self.logger.warning(f"[Hotstuff] ref={fallback:.2f}(估算)")

    async def _get_hotstuff_entry_price(self) -> Optional[Decimal]:
        try:
            loop = asyncio.get_event_loop()
            positions = await loop.run_in_executor(
                None,
                self.hs_info.positions,
                PositionsParams(user=self.hs_address),
            )
            if not positions:
                return None
            for p in positions:
                inst = (p.get("instrument") if isinstance(p, dict)
                        else getattr(p, "instrument", None))
                if inst == self.hs_symbol:
                    for field in ("entryPrice", "avgEntryPrice", "avgPrice",
                                  "entry_price", "avg_entry_price",
                                  "averagePrice", "average_price"):
                        val = (p.get(field) if isinstance(p, dict)
                               else getattr(p, field, None))
                        if val is not None:
                            try:
                                price = Decimal(str(val))
                                if price > 0:
                                    return price
                            except Exception:
                                pass
                    return None
        except Exception:
            return None

    async def _place_hotstuff_ioc(self, side: str, qty: Decimal,
                                   ref_price: Decimal) -> Optional[Decimal]:
        slip = min(Decimal(str(self._max_slippage_bps)),
                   Decimal("50")) / Decimal("10000")
        if side == "buy":
            aggressive = ref_price * (Decimal("1") + slip)
        else:
            aggressive = ref_price * (Decimal("1") - slip)
        aggressive = aggressive.quantize(self.hs_tick_size, rounding=ROUND_HALF_UP)
        qty_r = qty.quantize(self.hs_lot_size, rounding=ROUND_HALF_UP)

        pre_pos = self.hotstuff_position
        hs_side = "b" if side == "buy" else "s"

        try:
            order = UnitOrder(
                instrumentId=self.hs_instrument_id,
                side=hs_side,
                positionSide="BOTH",
                price=str(aggressive),
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
                self.logger.error(f"[Hotstuff] IOC 失败: {error}")
                return None

            self.logger.info(
                f"[Hotstuff] IOC {side} {qty_r} ref={ref_price:.2f}")

            has_tx = bool(result.get("tx_hash", ""))
            initial_wait = 0.02 if has_tx else 0.08
            await asyncio.sleep(initial_wait)
            try:
                delays = [0.1, 0.2, 0.3] if not has_tx else [0.08, 0.15]
                new_pos = await self._get_hotstuff_position()
                if side == "buy":
                    delta = new_pos - pre_pos
                else:
                    delta = pre_pos - new_pos
                if delta > 0:
                    actual_qty = abs(delta)
                    self._last_hotstuff_avg_price = None
                    _fb, _fa = self._get_hotstuff_ws_bbo()
                    self._last_h_fill_bbo = (_fb, _fa, side)
                    self.logger.info(
                        f"[Hotstuff] REST确认成交: "
                        f"pos {pre_pos} → {new_pos} Δ={actual_qty}")
                    return actual_qty
                else:
                    for attempt, d in enumerate(delays):
                        await asyncio.sleep(d)
                        new_pos = await self._get_hotstuff_position()
                        if side == "buy":
                            delta = new_pos - pre_pos
                        else:
                            delta = pre_pos - new_pos
                        if delta > 0:
                            actual_qty = abs(delta)
                            self._last_hotstuff_avg_price = None
                            _fb, _fa = self._get_hotstuff_ws_bbo()
                            self._last_h_fill_bbo = (_fb, _fa, side)
                            self.logger.info(
                                f"[Hotstuff] REST确认成交(第{attempt+2}次): "
                                f"pos {pre_pos} → {new_pos} Δ={actual_qty}")
                            return actual_qty
                    self.logger.warning("[Hotstuff] IOC 未确认成交")
                    return None
            except Exception as ve:
                self.logger.error(f"[Hotstuff] REST验证失败: {ve}")
                return None
        except Exception as e:
            self.logger.error(f"[Hotstuff] IOC 异常: {e}")
            return None

    # ═══════════════════════════════════════════════
    #  Hotstuff POST_ONLY Maker
    # ═══════════════════════════════════════════════

    async def _place_hotstuff_post_only(self, side: str, qty: Decimal,
                                         price: Decimal) -> bool:
        """Place POST_ONLY limit order on Hotstuff. Returns True if order placed."""
        hs_side = "b" if side == "buy" else "s"
        qty_r = qty.quantize(self.hs_lot_size, rounding=ROUND_HALF_UP)
        price_r = price.quantize(self.hs_tick_size, rounding=ROUND_HALF_UP)

        self._hs_maker_pre_pos = self.hotstuff_position
        self._hs_maker_placed = True
        self._hs_maker_side = side
        self._hs_maker_qty = qty_r

        try:
            order = UnitOrder(
                instrumentId=self.hs_instrument_id,
                side=hs_side,
                positionSide="BOTH",
                price=str(price_r),
                size=str(qty_r),
                tif="GTC",
                ro=False,
                po=True,
                isMarket=False,
            )
            params = PlaceOrderParams(
                orders=[order],
                expiresAfter=int(time.time() * 1000) + 300_000,
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
                self.logger.error(f"[Hotstuff Maker] 下单失败: {error}")
                self._hs_maker_placed = False
                return False

            self.logger.info(
                f"[Hotstuff Maker] 挂单: {side} {qty_r} @ {price_r}")
            return True
        except Exception as e:
            self.logger.error(f"[Hotstuff Maker] 下单异常: {e}")
            self._hs_maker_placed = False
            return False

    async def _poll_hotstuff_maker_fill(
        self, timeout: float,
    ) -> Tuple[Decimal, Optional[Decimal]]:
        """Poll Hotstuff position to detect maker fill. Returns (filled_qty, fill_price)."""
        deadline = time.time() + timeout
        pre_pos = self._hs_maker_pre_pos
        expected_side = self._hs_maker_side
        qty = self._hs_maker_qty

        while time.time() < deadline and not self.stop_flag:
            try:
                current_pos = await self._get_hotstuff_position()
                if expected_side == "buy":
                    delta = current_pos - pre_pos
                else:
                    delta = pre_pos - current_pos

                if delta >= qty * Decimal("0.5"):
                    self.hotstuff_position = current_pos
                    fill_price = await self._get_hotstuff_entry_price()
                    return abs(delta), fill_price
            except Exception:
                pass

            remaining = deadline - time.time()
            if remaining <= 0:
                break
            await asyncio.sleep(min(0.2, remaining))

        return Decimal("0"), None

    async def _check_hotstuff_maker_fill_once(
        self,
    ) -> Tuple[Decimal, Optional[Decimal]]:
        """Single quick position check for maker fill in IDLE loop."""
        if not self._hs_maker_placed:
            return Decimal("0"), None
        pre_pos = self._hs_maker_pre_pos
        expected_side = self._hs_maker_side
        qty = self._hs_maker_qty
        try:
            current_pos = await self._get_hotstuff_position()
            if expected_side == "buy":
                delta = current_pos - pre_pos
            else:
                delta = pre_pos - current_pos
            if delta >= qty * Decimal("0.5"):
                self.hotstuff_position = current_pos
                fill_price = await self._get_hotstuff_entry_price()
                return abs(delta), fill_price
        except Exception:
            pass
        return Decimal("0"), None

    async def _cancel_hotstuff_maker(self):
        """Cancel hotstuff maker order (uses cancel_all since no single cancel)."""
        self._hs_maker_placed = False
        self._maker_cancel_timestamps.append(time.time())
        await self._cancel_all_hotstuff_orders()

    def _reset_maker_order_state(self):
        self._hs_maker_placed = False
        self._hs_maker_pre_pos = Decimal("0")
        self._hs_maker_side = None
        self._hs_maker_qty = Decimal("0")
        self._maker_current_price = Decimal("0")
        self._maker_current_direction = None
        self._maker_current_h_side = None

    def _calc_proactive_maker_price(
        self, h_side: str,
        l_bid: Decimal, l_ask: Decimal,
        h_bid: Decimal, h_ask: Decimal,
        spread_bps: float,
    ) -> Optional[Decimal]:
        """Calculate Hotstuff maker price based on Lighter BBO + desired spread.
        Returns None if price would be invalid (cross book).
        """
        sp = Decimal(str(spread_bps)) / Decimal("10000")

        if h_side == "buy":
            target = l_bid * (Decimal("1") - sp)
            post_only_max = h_ask - self.hs_tick_size
            price = min(target, post_only_max)
            floor = h_bid * (Decimal("1") - Decimal("0.005"))
            price = max(price, floor)
        else:
            target = l_ask * (Decimal("1") + sp)
            post_only_min = h_bid + self.hs_tick_size
            price = max(target, post_only_min)
            ceiling = h_ask * (Decimal("1") + Decimal("0.005"))
            price = min(price, ceiling)

        price = price.quantize(self.hs_tick_size, rounding=ROUND_HALF_UP)

        if h_side == "buy" and price >= h_ask:
            return None
        if h_side == "sell" and price <= h_bid:
            return None

        return price

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
                                self.logger.info("[Lighter WS] auth token 已刷新")

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
                                if status not in (
                                        "FILLED", "PARTIALLY_FILLED",
                                        "CANCELED"):
                                    continue
                                if self._lighter_pending_coi is not None:
                                    ws_coi = od.get("client_order_index")
                                    if (ws_coi is not None
                                            and int(ws_coi)
                                                != self._lighter_pending_coi):
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

    async def _cancel_all_hotstuff_orders(self):
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
                self.logger.info(f"[Hotstuff] 撤销 {cancelled} 挂单")
        except Exception as e:
            self.logger.warning(f"[清理] Hotstuff cancel_all 失败: {e}")

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
            self.logger.error(f"查询 Hotstuff 仓位出错: {e}")
            return self.hotstuff_position

    async def _reconcile_positions(self) -> bool:
        l = await self._get_lighter_position()
        h = await self._get_hotstuff_position()
        self.lighter_position = l
        self.hotstuff_position = h
        net = l + h
        balanced = abs(net) < self.order_size * Decimal("0.5")
        if not balanced:
            self.logger.warning(f"[对账] 不平衡: L={l} H={h} net={net}")
        return balanced

    # ═══════════════════════════════════════════════
    #  Spread Detection
    # ═══════════════════════════════════════════════

    def _check_open_opportunity(
        self, l_bid: Decimal, l_ask: Decimal,
        h_bid: Decimal, h_ask: Decimal,
    ) -> Optional[Tuple[str, str, str, Decimal, Decimal, float]]:
        gap_sell_l = l_bid - h_ask
        ref1 = (l_bid + h_ask) / 2
        gap1_bps = float(gap_sell_l / ref1 * 10000) if ref1 > 0 else 0.0

        gap_buy_l = h_bid - l_ask
        ref2 = (h_bid + l_ask) / 2
        gap2_bps = float(gap_buy_l / ref2 * 10000) if ref2 > 0 else 0.0

        threshold = self.target_spread
        if self.maker_mode:
            threshold = self._calc_dynamic_threshold()

        if gap1_bps >= threshold and gap1_bps >= gap2_bps:
            return ("long_X_short_L", "sell", "buy", l_bid, h_ask, gap1_bps)
        elif gap2_bps >= threshold:
            return ("long_L_short_X", "buy", "sell", l_ask, h_bid, gap2_bps)
        return None

    def _calc_directional_gaps(
        self, l_bid: Decimal, l_ask: Decimal,
        h_bid: Decimal, h_ask: Decimal,
    ) -> Tuple[float, float]:
        ref1 = (l_bid + h_ask) / 2
        gap1 = float((l_bid - h_ask) / ref1 * 10000) if ref1 > 0 else 0.0
        ref2 = (h_bid + l_ask) / 2
        gap2 = float((h_bid - l_ask) / ref2 * 10000) if ref2 > 0 else 0.0
        return gap1, gap2

    def _get_current_gap_bps(
        self, l_bid: Decimal, l_ask: Decimal,
        h_bid: Decimal, h_ask: Decimal,
    ) -> float:
        if self.position_direction == "long_X_short_L":
            current_gap = l_ask - h_bid
            ref = h_bid if h_bid > 0 else l_ask
        elif self.position_direction == "long_L_short_X":
            current_gap = h_ask - l_bid
            ref = l_bid if l_bid > 0 else h_ask
        elif self.hotstuff_position > 0:
            current_gap = l_ask - h_bid
            ref = h_bid if h_bid > 0 else l_ask
        elif self.hotstuff_position < 0:
            current_gap = h_ask - l_bid
            ref = l_bid if l_bid > 0 else h_ask
        else:
            return 999.0
        return float(current_gap / ref * 10000) if ref > 0 else 999.0

    def _check_close_condition(
        self, l_bid: Decimal, l_ask: Decimal,
        h_bid: Decimal, h_ask: Decimal,
    ) -> Optional[Tuple[float, float]]:
        gap_bps = self._get_current_gap_bps(l_bid, l_ask, h_bid, h_ask)
        for tier_spread, cum_pct in self.close_tiers:
            if gap_bps <= tier_spread and self._tier_closed_pct < cum_pct:
                batch_pct = cum_pct - self._tier_closed_pct
                return (tier_spread, batch_pct)
        return None

    # ═══════════════════════════════════════════════
    #  Open Position — Dual Taker (for --no-maker)
    # ═══════════════════════════════════════════════

    async def _open_position(
        self, direction: str, l_side: str, h_side: str,
        l_ref: Decimal, h_ref: Decimal, qty: Decimal,
        spread_bps: float,
    ) -> bool:
        async with self._trade_lock:
            ls2 = self.lighter_feed.snapshot
            h2_bid, h2_ask = self._get_hotstuff_ws_bbo()
            if (ls2.best_bid is not None and ls2.best_ask is not None
                    and h2_bid is not None and h2_ask is not None):
                l2_bid = Decimal(str(ls2.best_bid))
                l2_ask = Decimal(str(ls2.best_ask))
                recheck = self._check_open_opportunity(
                    l2_bid, l2_ask, h2_bid, h2_ask)
                if recheck is None:
                    self.logger.info("[开仓] 二次验证价差已消失, 放弃")
                    return False
                if recheck[0] != direction:
                    self.logger.info(
                        f"[开仓] 二次验证方向翻转 "
                        f"{direction}→{recheck[0]}, 放弃")
                    return False
                _, _, _, l_ref, h_ref, spread_bps = recheck

            l1_l, l1_h = self._get_l1_depth(l_side, h_side)
            l1_qty = min(qty, l1_l, l1_h)
            l1_qty = l1_qty.quantize(self.hs_lot_size, rounding=ROUND_DOWN)
            min_trade = self.order_size * Decimal("0.05")
            if l1_qty < min_trade:
                self.logger.info(
                    f"[开仓] L1深度不足: L1_L={l1_l} L1_H={l1_h}"
                    f" 需要={qty} 可吃={l1_qty} < 最小={min_trade}, 跳过")
                return False
            qty = l1_qty

            t0 = time.time()

            results = await asyncio.gather(
                self._place_hotstuff_ioc(h_side, qty, h_ref),
                self._place_lighter_taker(l_side, qty, l_ref),
                return_exceptions=True,
            )

            h_fill = results[0] if not isinstance(results[0], Exception) else None
            l_fill = results[1] if not isinstance(results[1], Exception) else None
            if isinstance(results[0], Exception):
                self.logger.error(f"[开仓] Hotstuff异常: {results[0]}")
            if isinstance(results[1], Exception):
                self.logger.error(f"[开仓] Lighter异常: {results[1]}")

            total_ms = (time.time() - t0) * 1000
            h_ok = h_fill is not None and h_fill > 0
            l_ok = l_fill is not None and l_fill > 0

            if h_ok and l_ok:
                hedge_qty = min(h_fill, l_fill)
                if direction == "long_L_short_X":
                    self.hotstuff_position -= h_fill
                else:
                    self.hotstuff_position += h_fill

                await self._fetch_hotstuff_fill_price(h_ref)
                l_fill_price = self._last_lighter_avg_price or l_ref
                h_fill_price = self._last_hotstuff_avg_price or h_ref
                h_estimated = (self._last_hotstuff_avg_price is None
                               or self._last_hotstuff_avg_price == h_ref)

                self._record_open(direction, l_fill_price, l_fill,
                                  h_fill_price, h_fill, spread_bps, h_estimated)
                self._log_open_result(direction, l_side, h_side, l_ref, h_ref,
                                      l_fill_price, h_fill_price, l_fill, h_fill,
                                      spread_bps, total_ms, h_estimated, "taker")
                self._last_trade_time = time.time()
                self._last_ladder_attempt = time.time()

                if l_fill != h_fill:
                    await self._handle_fill_imbalance_open(
                        direction, l_side, h_side, l_ref, h_ref,
                        l_fill, h_fill, hedge_qty)
                return True

            elif h_ok and not l_ok:
                if direction == "long_L_short_X":
                    self.hotstuff_position -= h_fill
                else:
                    self.hotstuff_position += h_fill
                self.logger.error(
                    f"[开仓] Lighter未成交, 先尝试L对冲(免费) "
                    f"{l_side} {h_fill} ({total_ms:.0f}ms)")
                ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
                retry_ref = l_ref
                if ls_now and ls_now.best_bid and ls_now.best_ask:
                    retry_ref = (Decimal(str(ls_now.best_bid)) if l_side == "sell"
                                 else Decimal(str(ls_now.best_ask)))
                l_retry = await self._place_lighter_taker(l_side, h_fill, retry_ref)
                if not (l_retry and l_retry > 0):
                    self.logger.warning("[开仓] L对冲也失败, 回撤H")
                    undo_h_side = "sell" if h_side == "buy" else "buy"
                    hb, ha = self._get_hotstuff_ws_bbo()
                    undo_h_ref = (hb if undo_h_side == "sell" else ha) or h_ref
                    undo_fill = await self._place_hotstuff_ioc(
                        undo_h_side, h_fill, undo_h_ref)
                    if undo_fill and undo_fill > 0:
                        if direction == "long_L_short_X":
                            self.hotstuff_position += undo_fill
                        else:
                            self.hotstuff_position -= undo_fill
                    else:
                        self._force_reconcile = True
                return False

            elif l_ok and not h_ok:
                self.logger.error(
                    f"[开仓] Hotstuff未成交, 回撤Lighter "
                    f"{l_side}→{l_fill} ({total_ms:.0f}ms)")
                undo_l_side = "sell" if l_side == "buy" else "buy"
                ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
                undo_l_ref = l_ref
                if ls_now and ls_now.best_bid and ls_now.best_ask:
                    undo_l_ref = (Decimal(str(ls_now.best_bid)) if undo_l_side == "sell"
                                  else Decimal(str(ls_now.best_ask)))
                undo_fill = await self._place_lighter_taker(
                    undo_l_side, l_fill, undo_l_ref)
                if not (undo_fill and undo_fill > 0):
                    self._force_reconcile = True
                return False

            else:
                self.logger.warning(f"[开仓] 两边都未成交 ({total_ms:.0f}ms)")
                return False

    def _record_open(self, direction, l_price, l_qty, h_price, h_qty,
                     spread_bps, h_estimated):
        self._open_l_refs.append((l_price, l_qty))
        self._open_h_refs.append((h_price, h_qty))
        if self.position_opened_at is None:
            self.position_opened_at = time.time()
            self._tier_closed_pct = 0.0
            self._tier_close_records.clear()
        self.position_direction = direction
        self._last_open_spread_bps = max(spread_bps, self.target_spread)
        self._total_open_qty = min(
            abs(self.hotstuff_position), abs(self.lighter_position))
        self._open_theoretical_spread = spread_bps

    def _log_open_result(self, direction, l_side, h_side, l_ref, h_ref,
                         l_fill_price, h_fill_price, l_fill, h_fill,
                         spread_bps, total_ms, h_estimated, mode_tag):
        if direction == "long_X_short_L":
            actual_sp = float(
                (l_fill_price - h_fill_price) / l_fill_price * 10000
            ) if l_fill_price > 0 else 0.0
        else:
            actual_sp = float(
                (h_fill_price - l_fill_price) / l_fill_price * 10000
            ) if l_fill_price > 0 else 0.0
        sp_q = ("OK" if actual_sp >= self.ROUND_TRIP_FEE_BPS
                else "LOW" if actual_sp > 0 else "NEG")

        if l_side == "buy":
            dev_l = float((l_fill_price - l_ref) / l_ref * 10000) if l_ref > 0 else 0.0
        else:
            dev_l = float((l_ref - l_fill_price) / l_ref * 10000) if l_ref > 0 else 0.0
        if h_side == "buy":
            dev_h = float((h_fill_price - h_ref) / h_ref * 10000) if h_ref > 0 else 0.0
        else:
            dev_h = float((h_ref - h_fill_price) / h_ref * 10000) if h_ref > 0 else 0.0
        excess = spread_bps - self.target_spread

        self._open_dev_l_bps = dev_l
        self._open_dev_h_bps = dev_h
        self._open_excess_bps = excess
        est_tag = "(估)" if h_estimated else ""

        self.logger.info(
            f"[开仓成功|{mode_tag}] {total_ms:.0f}ms "
            f"实际价差={actual_sp:.1f}bps({sp_q}) "
            f"L_{l_side}={l_fill_price:.2f}x{l_fill} "
            f"H_{h_side}={h_fill_price:.2f}{est_tag}x{h_fill} "
            f"偏移:L={dev_l:+.2f}bps H={dev_h:+.2f}bps "
            f"超额={excess:+.2f}bps")

        self._csv_row([
            datetime.now(timezone.utc).isoformat(),
            "OPEN", direction,
            l_side, f"{l_ref:.2f}", f"{l_fill_price:.2f}", str(l_fill),
            h_side, f"{h_ref:.2f}", f"{h_fill_price:.2f}", str(h_fill),
            f"{spread_bps:.2f}", f"{total_ms:.0f}", mode_tag,
            f"{dev_l:.2f}", f"{dev_h:.2f}", f"{excess:.2f}", "",
            "", "",
        ])

    async def _handle_fill_imbalance_open(self, direction, l_side, h_side,
                                           l_ref, h_ref, l_fill, h_fill,
                                           hedge_qty):
        excess_l = l_fill - hedge_qty
        excess_h = h_fill - hedge_qty
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
        elif excess_h > min_excess:
            self.logger.warning(f"[开仓] H多成交{excess_h}, L补仓(免费)")
            ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
            add_l_ref = l_ref
            if ls_now and ls_now.best_bid and ls_now.best_ask:
                add_l_ref = (Decimal(str(ls_now.best_bid)) if l_side == "sell"
                             else Decimal(str(ls_now.best_ask)))
            add_ok = await self._place_lighter_taker(l_side, excess_h, add_l_ref)
            if not (add_ok and add_ok > 0):
                self.logger.warning("[开仓] L补仓失败, 回撤H")
                undo_h_side = "sell" if h_side == "buy" else "buy"
                hb, ha = self._get_hotstuff_ws_bbo()
                undo_h_ref = (hb if undo_h_side == "sell" else ha) or h_ref
                undo = await self._place_hotstuff_ioc(undo_h_side, excess_h, undo_h_ref)
                if undo and undo > 0:
                    if direction == "long_L_short_X":
                        self.hotstuff_position += undo
                    else:
                        self.hotstuff_position -= undo
                else:
                    self._force_reconcile = True

    # ═══════════════════════════════════════════════
    #  Hedge After Proactive Maker Fill
    # ═══════════════════════════════════════════════

    async def _hedge_after_maker_fill(
        self, direction: str, h_side: str, filled_qty: Decimal,
        fill_price: Optional[Decimal], spread_bps: float,
    ) -> bool:
        """After Hotstuff maker fills, immediately hedge on Lighter."""
        async with self._trade_lock:
            t0 = time.time()

            if direction == "long_X_short_L":
                l_side = "sell"
                self.hotstuff_position += filled_qty
            else:
                l_side = "buy"
                self.hotstuff_position -= filled_qty

            ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
            if ls_now and ls_now.best_bid and ls_now.best_ask:
                l_ref = (Decimal(str(ls_now.best_bid)) if l_side == "sell"
                         else Decimal(str(ls_now.best_ask)))
            else:
                self.logger.error("[Maker对冲] Lighter BBO不可用!")
                self._force_reconcile = True
                return False

            l1_l, _ = self._get_l1_depth(l_side, "buy")
            hedge_qty = min(filled_qty, l1_l)
            hedge_qty = hedge_qty.quantize(self.hs_lot_size, rounding=ROUND_DOWN)
            if hedge_qty < self.order_size * Decimal("0.05"):
                self.logger.error(
                    f"[Maker对冲] Lighter L1深度不足 {l1_l}, 紧急平仓")
                self._force_reconcile = True
                return False

            l_fill = await self._place_lighter_taker(l_side, hedge_qty, l_ref)
            total_ms = (time.time() - t0) * 1000

            if l_fill and l_fill > 0:
                l_fill_price = self._last_lighter_avg_price or l_ref
                h_fill_price = fill_price or l_ref

                self._record_open(direction, l_fill_price, l_fill,
                                  h_fill_price, filled_qty, spread_bps, False)
                h_ref = fill_price or l_ref
                self._log_open_result(
                    direction, l_side, h_side, l_ref, h_ref,
                    l_fill_price, h_fill_price, l_fill, filled_qty,
                    spread_bps, total_ms, False, "maker")

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
                    f"Hotstuff已成交{filled_qty} 需紧急平仓")
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
    #  Close Position — Dual Taker (for fallback)
    # ═══════════════════════════════════════════════

    async def _close_position(
        self, l_bid: Decimal, l_ask: Decimal,
        h_bid: Decimal, h_ask: Decimal,
        batch_pct: float = 100.0,
    ) -> bool:
        async with self._trade_lock:
            full_qty = min(abs(self.lighter_position), abs(self.hotstuff_position))
            if full_qty < self.order_size * Decimal("0.01"):
                return False

            if batch_pct < 100.0 and self._total_open_qty > 0:
                target_close = self._total_open_qty * Decimal(str(batch_pct / 100.0))
                close_qty = min(full_qty, target_close)
            else:
                close_qty = full_qty

            close_qty = close_qty.quantize(self.hs_lot_size, rounding=ROUND_DOWN)
            if close_qty < self.order_size * Decimal("0.01"):
                return False

            if self.position_direction == "long_X_short_L":
                close_l_side, close_h_side = "buy", "sell"
                l_ref = l_ask if l_ask > 0 else (l_bid + l_ask) / 2
                h_ref = h_bid
            else:
                close_l_side, close_h_side = "sell", "buy"
                l_ref = l_bid if l_bid > 0 else (l_bid + l_ask) / 2
                h_ref = h_ask

            l1_l, l1_h = self._get_l1_depth(close_l_side, close_h_side)
            l1_qty = min(close_qty, l1_l, l1_h)
            l1_qty = l1_qty.quantize(self.hs_lot_size, rounding=ROUND_DOWN)
            if l1_qty < self.order_size * Decimal("0.05"):
                return False
            close_qty = l1_qty

            tier_label = f"T{self._tier_closed_pct:.0f}→{self._tier_closed_pct + batch_pct:.0f}%"

            t0 = time.time()
            results = await asyncio.gather(
                self._place_hotstuff_ioc(close_h_side, close_qty, h_ref),
                self._place_lighter_taker(close_l_side, close_qty, l_ref),
                return_exceptions=True,
            )
            h_fill = results[0] if not isinstance(results[0], Exception) else None
            l_fill = results[1] if not isinstance(results[1], Exception) else None
            total_ms = (time.time() - t0) * 1000
            h_ok = h_fill is not None and h_fill > 0
            l_ok = l_fill is not None and l_fill > 0

            if h_ok and l_ok:
                hedge_qty = min(h_fill, l_fill)
                await self._fetch_hotstuff_fill_price(h_ref, is_close=True)
                l_fill_price = self._last_lighter_avg_price or l_ref
                h_fill_price = self._last_hotstuff_avg_price or h_ref
                h_estimated = (self._last_hotstuff_avg_price is None
                               or self._last_hotstuff_avg_price == h_ref)
                if close_h_side == "sell":
                    self.hotstuff_position -= h_fill
                else:
                    self.hotstuff_position += h_fill
                return self._finalize_close(
                    tier_label, hedge_qty, l_fill_price, h_fill_price,
                    h_estimated, l_bid, l_ask, h_bid, h_ask,
                    close_l_side, close_h_side, l_ref, h_ref,
                    l_fill, h_fill, total_ms, batch_pct, "taker")

            elif h_ok and not l_ok:
                if close_h_side == "sell":
                    self.hotstuff_position -= h_fill
                else:
                    self.hotstuff_position += h_fill
                self.logger.error(
                    f"[平仓] Lighter未成交, L对冲 {close_l_side} {h_fill}")
                ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
                retry_ref = l_ref
                if ls_now and ls_now.best_bid and ls_now.best_ask:
                    retry_ref = (Decimal(str(ls_now.best_bid)) if close_l_side == "sell"
                                 else Decimal(str(ls_now.best_ask)))
                l_retry = await self._place_lighter_taker(close_l_side, h_fill, retry_ref)
                if not (l_retry and l_retry > 0):
                    self._force_reconcile = True
                return False

            elif l_ok and not h_ok:
                self.logger.error(f"[平仓] Hotstuff未成交, 回撤Lighter")
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
    #  Close Position — Maker-First
    # ═══════════════════════════════════════════════

    async def _close_position_mt(
        self, l_bid: Decimal, l_ask: Decimal,
        h_bid: Decimal, h_ask: Decimal,
        batch_pct: float = 100.0,
    ) -> bool:
        """Close using Hotstuff maker first, then Lighter taker.
        Falls back to dual taker on timeout.
        """
        async with self._trade_lock:
            full_qty = min(abs(self.lighter_position), abs(self.hotstuff_position))
            if full_qty < self.order_size * Decimal("0.01"):
                return False

            if batch_pct < 100.0 and self._total_open_qty > 0:
                target_close = self._total_open_qty * Decimal(str(batch_pct / 100.0))
                close_qty = min(full_qty, target_close)
            else:
                close_qty = full_qty

            close_qty = close_qty.quantize(self.hs_lot_size, rounding=ROUND_DOWN)
            if close_qty < self.order_size * Decimal("0.01"):
                return False

            if self.position_direction == "long_X_short_L":
                close_l_side, close_h_side = "buy", "sell"
                l_ref = l_ask
                h_ref = h_bid
            else:
                close_l_side, close_h_side = "sell", "buy"
                l_ref = l_bid
                h_ref = h_ask

            tier_label = f"T{self._tier_closed_pct:.0f}→{self._tier_closed_pct + batch_pct:.0f}%"
            self.logger.info(
                f"[平仓MT] {tier_label} Hotstuff maker {close_h_side} "
                f"{close_qty} @ {h_ref}")

            t0 = time.time()

            maker_price = self._calc_proactive_maker_price(
                close_h_side, l_bid, l_ask, h_bid, h_ask,
                spread_bps=0.3)
            if maker_price is None:
                if close_h_side == "sell":
                    maker_price = h_ask
                else:
                    maker_price = h_bid
                maker_price = maker_price.quantize(
                    self.hs_tick_size, rounding=ROUND_HALF_UP)

            ok = await self._place_hotstuff_post_only(
                close_h_side, close_qty, maker_price)
            if not ok:
                self.logger.warning("[平仓MT] maker下单失败, 回退双taker")
                return False

            h_fill_qty, h_fill_price = await self._poll_hotstuff_maker_fill(
                self._maker_close_timeout)
            total_ms = (time.time() - t0) * 1000

            await self._cancel_hotstuff_maker()

            if h_fill_qty > 0:
                if close_h_side == "sell":
                    self.hotstuff_position -= h_fill_qty
                else:
                    self.hotstuff_position += h_fill_qty

                h_actual = h_fill_price or maker_price
                self._last_hotstuff_avg_price = h_actual

                ls_now = self.lighter_feed.snapshot if self.lighter_feed else None
                if ls_now and ls_now.best_bid and ls_now.best_ask:
                    l_ref_now = (Decimal(str(ls_now.best_bid)) if close_l_side == "sell"
                                 else Decimal(str(ls_now.best_ask)))
                else:
                    l_ref_now = l_ref

                l_fill = await self._place_lighter_taker(
                    close_l_side, h_fill_qty, l_ref_now)

                if l_fill and l_fill > 0:
                    l_fill_price = self._last_lighter_avg_price or l_ref_now
                    return self._finalize_close(
                        tier_label, min(h_fill_qty, l_fill),
                        l_fill_price, h_actual, False,
                        l_bid, l_ask, h_bid, h_ask,
                        close_l_side, close_h_side, l_ref_now, h_ref,
                        l_fill, h_fill_qty, total_ms, batch_pct, "maker")
                else:
                    self.logger.error("[平仓MT] Lighter对冲失败! 触发对账")
                    self._force_reconcile = True
                    return False
            else:
                self.logger.warning(
                    f"[平仓MT] maker {self._maker_close_timeout}s超时, "
                    f"回退双taker")
                return False

    def _finalize_close(self, tier_label, hedge_qty, l_fill_price, h_fill_price,
                        h_estimated, l_bid, l_ask, h_bid, h_ask,
                        close_l_side, close_h_side, l_ref, h_ref,
                        l_fill, h_fill, total_ms, batch_pct, mode_tag):
        gap_at_close = self._get_current_gap_bps(l_bid, l_ask, h_bid, h_ask)
        self._tier_close_records.append({
            "qty": float(hedge_qty),
            "gap_bps": gap_at_close,
            "l_price": float(l_fill_price),
            "h_price": float(h_fill_price),
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
        convergence = self._open_theoretical_spread - wt_close_spread
        net_after_fee = convergence - self.ROUND_TRIP_FEE_BPS

        net_bps = self._log_ref_pnl(
            h_fill_price, l_fill_price, hedge_qty, h_estimated=h_estimated)
        self.cumulative_net_bps += net_bps

        est_tag = "(估)" if h_estimated else ""
        self.logger.info(
            f"[平仓完成|{mode_tag}] {tier_label} {total_ms:.0f}ms "
            f"本批净利={net_bps:+.2f}bps{est_tag} "
            f"加权平仓spread={wt_close_spread:.2f}bps "
            f"总收敛={convergence:.2f}bps "
            f"扣费后={net_after_fee:+.2f}bps "
            f"已平={self._tier_closed_pct:.0f}% "
            f"累积={self.cumulative_net_bps:+.2f}bps")

        self._csv_row([
            datetime.now(timezone.utc).isoformat(),
            "CLOSE", f"tier_{self._tier_closed_pct:.0f}pct",
            close_l_side, f"{l_ref:.2f}", f"{l_fill_price:.2f}", str(l_fill),
            close_h_side, f"{h_ref:.2f}", f"{h_fill_price:.2f}", str(h_fill),
            f"{self._open_theoretical_spread:.2f}", f"{total_ms:.0f}", mode_tag,
            "", "", "", "",
            f"{net_bps:.2f}", f"{self.cumulative_net_bps:.2f}",
        ])
        self._last_trade_time = time.time()

        pos_remaining = abs(self.hotstuff_position)
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
            if self._hs_maker_placed:
                await self._cancel_hotstuff_maker()
            await self._do_lighter_cancel_all()
            await self._cancel_all_hotstuff_orders()
            await asyncio.sleep(0.5)

            for attempt in range(3):
                l_qty = abs(self.lighter_position)
                h_qty = abs(self.hotstuff_position)
                if (l_qty < self.order_size * Decimal("0.01")
                        and h_qty < self.order_size * Decimal("0.01")):
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
                        self.logger.info(
                            f"[紧急平仓] #{attempt+1} Lighter IOC "
                            f"{l_side} {l_qty} @ {l_ref}")
                        tasks.append(
                            self._place_lighter_taker(l_side, l_qty, l_ref))
                    else:
                        tasks.append(asyncio.sleep(0))

                if h_qty >= self.order_size * Decimal("0.01"):
                    h_side = "sell" if self.hotstuff_position > 0 else "buy"
                    hb, ha = self._get_hotstuff_ws_bbo()
                    h_ref = hb if h_side == "sell" else ha
                    if h_ref and h_ref > 0:
                        self.logger.info(
                            f"[紧急平仓] #{attempt+1} Hotstuff IOC "
                            f"{h_side} {h_qty} @ {h_ref}")
                        tasks.append(
                            self._place_hotstuff_ioc(h_side, h_qty, h_ref))
                    else:
                        tasks.append(asyncio.sleep(0))

                h_task_idx = -1
                if tasks:
                    if l_qty >= self.order_size * Decimal("0.01"):
                        h_task_idx = 1
                    else:
                        h_task_idx = 0
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for i, r in enumerate(results):
                        if isinstance(r, Exception):
                            self.logger.error(f"[紧急平仓] 任务{i}异常: {r}")

                    if (h_task_idx < len(results)
                            and not isinstance(results[h_task_idx], Exception)
                            and results[h_task_idx] is not None
                            and results[h_task_idx] > 0):
                        h_fill = results[h_task_idx]
                        h_side_used = ("sell" if self.hotstuff_position > 0
                                       else "buy")
                        if h_side_used == "sell":
                            self.hotstuff_position -= h_fill
                        else:
                            self.hotstuff_position += h_fill

                await asyncio.sleep(2)
                try:
                    await self._reconcile_positions()
                except Exception:
                    pass

            l_final = abs(self.lighter_position)
            h_final = abs(self.hotstuff_position)
            if (l_final >= self.order_size * Decimal("0.01")
                    or h_final >= self.order_size * Decimal("0.01")):
                self.logger.error(
                    f"[紧急平仓] 3轮后仍有残余! "
                    f"L={self.lighter_position} "
                    f"H={self.hotstuff_position} 请手动处理!")
            else:
                self.logger.info("[紧急平仓] 仓位已清零")
        except Exception as e:
            self.logger.error(
                f"紧急平仓异常: {e}\n{traceback.format_exc()}")

    async def _shutdown(self):
        self.stop_flag = True
        self.logger.info("关闭中...")
        if self._hs_maker_placed:
            try:
                await self._cancel_hotstuff_maker()
            except Exception:
                pass
        await self._do_lighter_cancel_all()
        await self._cancel_all_hotstuff_orders()
        if self._lighter_ws_task and not self._lighter_ws_task.done():
            self._lighter_ws_task.cancel()
            try:
                await self._lighter_ws_task
            except (asyncio.CancelledError, Exception):
                pass
        for feed in (self.lighter_feed, self.hs_feed):
            if feed:
                try:
                    await feed.disconnect()
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
            if abs(self.lighter_position) > 0 or abs(self.hotstuff_position) > 0:
                self.logger.warning(
                    f"退出仍有仓位: L={self.lighter_position} "
                    f"H={self.hotstuff_position}")
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
        mode_desc = "★ Maker-First主动挂单(REST轮询)" if self.maker_mode else "双Taker"
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
            f"启动 Hotstuff+Lighter V15 HS {mode_desc}  {self.symbol}  "
            f"size={self.order_size} max_pos={self.max_position}  "
            f"目标价差≥{self.target_spread}bps{tier_desc}  "
            f"dry_run={self.dry_run}  "
            f"往返费用≈{self.ROUND_TRIP_FEE_BPS:.1f}bps  {ladder_desc}  "
            f"滑点保护≤{self._max_slippage_bps}bps")
        if self.maker_mode:
            self.logger.info(
                f"  动态阈值: 窗口={self._dyn_window}s P{self._dyn_percentile:.0f} "
                f"价格容忍={self._price_tolerance_bps}bps "
                f"最大挂龄={self._maker_max_age}s "
                f"对冲超时={self._hedge_timeout}s")

        if not self.dry_run:
            self._init_lighter_signer()
            self._get_lighter_market_config()
            self._init_hotstuff_client()
            self._get_hotstuff_instrument_info()
            self.logger.info("交易客户端就绪")

            if self.maker_mode:
                po_ok = await self._validate_post_only()
                if not po_ok:
                    self.logger.warning(
                        "[自动回退] POST_ONLY 不支持, 切换为 --no-maker 模式")
                    self.maker_mode = False
                    self.ROUND_TRIP_FEE_BPS = self.ROUND_TRIP_FEE_BPS_TAKER
                    if self.target_spread < 6.0:
                        self.target_spread = 6.0
                        self.logger.info(
                            f"[自动回退] target_spread 调整为 {self.target_spread}")

            self.logger.info("[启动] 清理遗留挂单...")
            await self._do_lighter_cancel_all()
            await self._cancel_all_hotstuff_orders()
        else:
            self.logger.info("[DRY-RUN] 跳过交易客户端初始化")
            self._get_lighter_market_config()

        self.lighter_feed = LighterFeed(self.symbol)
        self.hs_feed = HotstuffFeed(self.symbol, on_update=self._on_bbo_update)
        asyncio.create_task(self.lighter_feed.connect())
        asyncio.create_task(self.hs_feed.connect())
        if not self.dry_run:
            self._lighter_ws_task = asyncio.create_task(
                self._lighter_account_ws_loop())
        self.logger.info("等待行情数据 (6s)...")
        await asyncio.sleep(6)

        ls = self.lighter_feed.snapshot
        hs = self.hs_feed.snapshot if self.hs_feed else None
        h_bid, h_ask = (self._get_hotstuff_ws_bbo()
                        if not self.dry_run else (None, None))
        if not ls.mid or (not (hs and hs.mid) and h_bid is None):
            self.logger.error("行情不可用")
            return
        h_mid_val = float((h_bid + h_ask) / 2) if h_bid and h_ask else (hs.mid if hs else 0)
        self.logger.info(
            f"行情就绪: Lighter={ls.mid:.2f} Hotstuff={h_mid_val:.2f}")

        if not self.dry_run:
            await self._reconcile_positions()
            if abs(self.lighter_position) > 0 or abs(self.hotstuff_position) > 0:
                self.logger.warning(
                    f"启动检测到仓位: L={self.lighter_position} "
                    f"H={self.hotstuff_position}")
                self.state = State.IN_POSITION
                self.position_opened_at = time.time()
                if self.hotstuff_position > 0:
                    self.position_direction = "long_X_short_L"
                elif self.hotstuff_position < 0:
                    self.position_direction = "long_L_short_X"

                h_bid_s, h_ask_s = self._get_hotstuff_ws_bbo()
                h_est = ((h_bid_s + h_ask_s) / 2 if h_bid_s and h_ask_s
                         else Decimal(str(ls.mid or 0)))
                l_est = Decimal(str(ls.mid or 0))
                if h_est > 0 and l_est > 0:
                    h_qty = abs(self.hotstuff_position)
                    l_qty = abs(self.lighter_position)
                    self._open_h_refs = [(h_est, h_qty)] if h_qty > 0 else []
                    self._open_l_refs = [(l_est, l_qty)] if l_qty > 0 else []
                    self._total_open_qty = min(h_qty, l_qty)
                    self._tier_closed_pct = 0.0
                    self._tier_close_records.clear()
                    if self.position_direction == "long_X_short_L":
                        _est_sp = float((l_est - h_est) / l_est * 10000)
                    else:
                        _est_sp = float((h_est - l_est) / l_est * 10000)
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

                h_ws_bid, h_ws_ask = (None, None)
                if not self.dry_run:
                    h_ws_bid, h_ws_ask = self._get_hotstuff_ws_bbo()

                if h_ws_bid is None or h_ws_ask is None:
                    hs_snap = self.hs_feed.snapshot if self.hs_feed else None
                    if not hs_snap or not hs_snap.mid:
                        await asyncio.sleep(self.interval)
                        continue
                    if hs_snap.best_bid > 0 and hs_snap.best_ask > 0:
                        h_ws_bid = Decimal(str(hs_snap.best_bid))
                        h_ws_ask = Decimal(str(hs_snap.best_ask))
                    h_mid = Decimal(str(hs_snap.mid))
                else:
                    h_mid = (h_ws_bid + h_ws_ask) / 2

                if not ls.mid or ls.best_bid is None or ls.best_ask is None:
                    await asyncio.sleep(self.interval)
                    continue

                l_bid = Decimal(str(ls.best_bid))
                l_ask = Decimal(str(ls.best_ask))
                l_mid = Decimal(str(ls.mid))

                if l_bid >= l_ask:
                    await asyncio.sleep(self.interval)
                    continue

                l_age = now - ls.timestamp if ls.timestamp > 0 else 999
                h_age = (now - self.hs_feed.last_update_ts
                         if self.hs_feed
                            and self.hs_feed.last_update_ts > 0
                         else 999)

                if l_age > 5.0 or h_age > 5.0:
                    await asyncio.sleep(self.interval)
                    continue

                if h_ws_bid is not None and h_ws_ask is not None:
                    gap1, gap2 = self._calc_directional_gaps(
                        l_bid, l_ask, h_ws_bid, h_ws_ask)
                    self._record_spread_obs(gap1, gap2)

                mid_spread_bps = float((h_mid - l_mid) / l_mid * 10000)
                cur_pos = abs(self.hotstuff_position)

                # ── Status log ──
                if now - last_status_log > 15:
                    dyn_thr = self._calc_dynamic_threshold() if self.maker_mode else 0.0
                    hold_s = ""
                    if self.position_opened_at:
                        hold_s = f" hold={now - self.position_opened_at:.0f}s"
                    basis_info = ""
                    if (self.state == State.IN_POSITION
                            and h_ws_bid is not None and h_ws_ask is not None):
                        _gap_bps = self._get_current_gap_bps(
                            l_bid, l_ask, h_ws_bid, h_ws_ask)
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
                        maker_info = f"  dyn={dyn_thr:.1f}bps"
                        if self._hs_maker_placed and self.state == State.IDLE:
                            age = now - self._maker_placed_at
                            maker_info += (
                                f" 挂单中:{self._maker_current_h_side}"
                                f"@{self._maker_current_price:.2f} {age:.0f}s")
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
                    self.logger.info(
                        f"[{self.state.value}] mid={mid_spread_bps:+.2f}bps  "
                        f"H_pos={self.hotstuff_position} "
                        f"L_pos={self.lighter_position}{hold_s}  "
                        f"L={l_bid}/{l_ask} H={h_ws_bid}/{h_ws_ask}"
                        f"{basis_info}{ladder_info}{maker_info}  "
                        f"鲜度:L={l_age:.1f}s H={h_age:.1f}s  "
                        f"累积={self.cumulative_net_bps:+.2f}bps")
                    last_status_log = now

                # ── Force reconcile ──
                if self._force_reconcile and not self.dry_run:
                    self._force_reconcile = False
                    self.logger.warning("[强制对账] 触发")
                    if self._hs_maker_placed:
                        await self._cancel_hotstuff_maker()
                    await self._reconcile_positions()
                    last_reconcile = now
                    cur_pos = abs(self.hotstuff_position)
                    if abs(self.lighter_position + self.hotstuff_position) > self.order_size * Decimal("0.5"):
                        self.logger.error("[强制对账] 不平衡, 紧急平仓")
                        await self._emergency_flatten()
                        self._reset_position_state()
                        self.state = State.IDLE
                        last_reconcile = time.time() - 50
                        continue

                # ── Periodic reconcile ──
                if (not self.dry_run and now - last_reconcile > 30
                        and self.state in (State.IDLE, State.IN_POSITION)):
                    if self._hs_maker_placed and self.state == State.IDLE:
                        pass
                    else:
                        await self._reconcile_positions()
                        last_reconcile = now
                        cur_pos = abs(self.hotstuff_position)
                        if (self.state == State.IN_POSITION
                                and self._total_open_qty > 0):
                            actual_pos = min(abs(self.hotstuff_position),
                                             abs(self.lighter_position))
                            if actual_pos > self._total_open_qty:
                                self._total_open_qty = actual_pos
                        net_imbalance = abs(
                            self.lighter_position + self.hotstuff_position)
                        if net_imbalance > self.order_size * Decimal("0.5"):
                            self.logger.error(
                                f"[周期对账] 不平衡! L={self.lighter_position} "
                                f"H={self.hotstuff_position}")
                            await self._emergency_flatten()
                            self._reset_position_state()
                            self.state = State.IDLE
                            last_reconcile = time.time() - 50
                            continue

                # ━━━━━━ STATE: IDLE ━━━━━━
                if self.state == State.IDLE:
                    has_l = abs(self.lighter_position) >= self.order_size * Decimal("0.01")
                    has_h = abs(self.hotstuff_position) >= self.order_size * Decimal("0.01")
                    if has_l or has_h:
                        _idle_min_trade = self.order_size * Decimal("0.05")
                        _idle_both_dust = (
                            abs(self.hotstuff_position) < _idle_min_trade
                            and abs(self.lighter_position) < _idle_min_trade)
                        if _idle_both_dust:
                            pass
                        elif has_l and has_h:
                            self.logger.warning(
                                f"[IDLE] 检测到残余双边仓位, 恢复为IN_POSITION")
                            self.state = State.IN_POSITION
                            self.position_opened_at = time.time()
                            if self.hotstuff_position > 0:
                                self.position_direction = "long_X_short_L"
                            else:
                                self.position_direction = "long_L_short_X"
                            continue
                        else:
                            self.logger.error(
                                f"[IDLE] 单腿仓位! L={self.lighter_position} "
                                f"H={self.hotstuff_position}, 紧急平仓")
                            await self._emergency_flatten()
                            self._reset_position_state()
                            last_reconcile = time.time() - 50
                            continue

                    if h_ws_bid is None or h_ws_ask is None:
                        await asyncio.sleep(self.interval)
                        continue

                    if cur_pos >= self.max_position:
                        await asyncio.sleep(self.interval)
                        continue

                    if not self._check_tx_rate():
                        await asyncio.sleep(2.0)
                        continue

                    # ── Maker mode: proactive order management ──
                    if self.maker_mode and not self.dry_run:
                        if now < self._hedge_circuit_breaker_until:
                            if self._hs_maker_placed:
                                await self._cancel_hotstuff_maker()
                                self._reset_maker_order_state()
                            await asyncio.sleep(self.interval)
                            continue

                        dyn_threshold = self._calc_dynamic_threshold()
                        gap1, gap2 = self._calc_directional_gaps(
                            l_bid, l_ask, h_ws_bid, h_ws_ask)
                        best_gap = max(gap1, gap2)

                        if best_gap < dyn_threshold * 0.3:
                            if self._hs_maker_placed and self._check_maker_cancel_rate():
                                await self._cancel_hotstuff_maker()
                                self._reset_maker_order_state()
                            await asyncio.sleep(self.interval)
                            continue

                        if gap1 >= gap2:
                            direction = "long_X_short_L"
                            h_side = "buy"
                        else:
                            direction = "long_L_short_X"
                            h_side = "sell"

                        maker_price = self._calc_proactive_maker_price(
                            h_side, l_bid, l_ask, h_ws_bid, h_ws_ask,
                            dyn_threshold)
                        if maker_price is None:
                            await asyncio.sleep(self.interval)
                            continue

                        if (self._maker_last_direction is not None
                                and self._maker_last_direction != direction):
                            if now - self._maker_direction_change_ts < self._direction_cooldown:
                                await asyncio.sleep(self.interval)
                                continue
                            self._maker_direction_change_ts = now
                        self._maker_last_direction = direction

                        # ── Check existing maker for fill ──
                        if self._hs_maker_placed:
                            filled_qty, fill_price = await self._check_hotstuff_maker_fill_once()

                            if filled_qty > 0:
                                saved_dir = self._maker_current_direction
                                saved_side = self._maker_current_h_side
                                await self._cancel_hotstuff_maker()
                                self._reset_maker_order_state()

                                if saved_dir and saved_side:
                                    self.logger.info(
                                        f"[Maker成交] {saved_dir} "
                                        f"{saved_side} {filled_qty} "
                                        f"@{fill_price} → 对冲Lighter")
                                    ok = await self._hedge_after_maker_fill(
                                        saved_dir, saved_side, filled_qty,
                                        fill_price or maker_price,
                                        dyn_threshold)
                                    if ok:
                                        self.state = State.IN_POSITION
                                    continue
                            else:
                                direction_changed = (
                                    direction != self._maker_current_direction)
                                price_drift = 0.0
                                if self._maker_current_price > 0:
                                    price_drift = abs(float(
                                        (maker_price - self._maker_current_price)
                                        / self._maker_current_price * 10000))
                                order_age = now - self._maker_placed_at

                                need_replace = (
                                    price_drift > self._price_tolerance_bps
                                    or order_age > self._maker_max_age
                                    or direction_changed)

                                if need_replace and self._check_maker_cancel_rate():
                                    await self._cancel_hotstuff_maker()
                                    self._reset_maker_order_state()

                        # ── Place new maker order ──
                        if not self._hs_maker_placed:
                            ok = await self._place_hotstuff_post_only(
                                h_side, self.order_size, maker_price)
                            if ok:
                                self._maker_placed_at = now
                                self._maker_current_price = maker_price
                                self._maker_current_direction = direction
                                self._maker_current_h_side = h_side

                                await asyncio.sleep(0.1)
                                filled_qty, fill_price = await self._check_hotstuff_maker_fill_once()
                                if filled_qty > 0:
                                    await self._cancel_hotstuff_maker()
                                    self._reset_maker_order_state()
                                    self.logger.info(
                                        f"[Maker即时成交] {direction} "
                                        f"{h_side} {filled_qty} → 对冲")
                                    ok = await self._hedge_after_maker_fill(
                                        direction, h_side, filled_qty,
                                        fill_price or maker_price,
                                        dyn_threshold)
                                    if ok:
                                        self.state = State.IN_POSITION
                                    continue

                    # ── Taker mode (--no-maker fallback) ──
                    elif not self.maker_mode and not self.dry_run:
                        opp = self._check_open_opportunity(
                            l_bid, l_ask, h_ws_bid, h_ws_ask)
                        if opp:
                            direction, l_side, h_side, l_ref, h_ref, sp_bps = opp
                            self.logger.info(
                                f"[发现机会] {direction} 价差={sp_bps:.1f}bps")
                            ok = await self._open_position(
                                direction, l_side, h_side,
                                l_ref, h_ref, self.order_size, sp_bps)
                            if ok:
                                self.state = State.IN_POSITION
                            else:
                                await asyncio.sleep(1.0)

                    elif self.dry_run:
                        opp = self._check_open_opportunity(
                            l_bid, l_ask, h_ws_bid or Decimal("0"), h_ws_ask or Decimal("0"))
                        if opp and now - last_status_log > 5:
                            _, _, _, _, _, sp_bps = opp
                            self.logger.info(
                                f"[DRY-RUN] {opp[0]} 价差={sp_bps:.1f}bps")

                # ━━━━━━ STATE: IN_POSITION ━━━━━━
                elif self.state == State.IN_POSITION:
                    if (abs(self.hotstuff_position) < self.order_size * Decimal("0.01")
                            and abs(self.lighter_position) < self.order_size * Decimal("0.01")):
                        self._reset_position_state()
                        self.state = State.IDLE
                        continue

                    min_tradeable = self.order_size * Decimal("0.05")
                    h_dust = abs(self.hotstuff_position) < min_tradeable
                    l_dust = abs(self.lighter_position) < min_tradeable
                    if h_dust and l_dust and not self.dry_run:
                        net_imb = abs(self.lighter_position + self.hotstuff_position)
                        self.logger.info(
                            f"[残余清理] H={self.hotstuff_position} "
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

                    h_zero = abs(self.hotstuff_position) < self.order_size * Decimal("0.01")
                    l_zero = abs(self.lighter_position) < self.order_size * Decimal("0.01")
                    if h_zero != l_zero and not self.dry_run:
                        self.logger.warning("[单边仓位?] 先对账...")
                        await self._reconcile_positions()
                        h_zero2 = abs(self.hotstuff_position) < self.order_size * Decimal("0.01")
                        l_zero2 = abs(self.lighter_position) < self.order_size * Decimal("0.01")
                        if h_zero2 != l_zero2:
                            await self._emergency_flatten()
                            self._reset_position_state()
                            self.state = State.IDLE
                            last_reconcile = time.time() - 50
                            continue
                        last_reconcile = now

                    if (h_ws_bid is not None and h_ws_ask is not None
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
                                    dir_sp = float((l_mid - h_mid) / l_mid * 10000) if l_mid > 0 else 0.0
                                else:
                                    dir_sp = float((h_mid - l_mid) / l_mid * 10000) if l_mid > 0 else 0.0
                                if dir_sp > (self._last_open_spread_bps + self.ladder_step):
                                    self._last_ladder_attempt = now
                                    opp = self._check_open_opportunity(
                                        l_bid, l_ask, h_ws_bid, h_ws_ask)
                                    if opp and opp[0] == self.position_direction:
                                        d, ls2, hs2, lr, hr, sp = opp
                                        ok = await self._open_position(
                                            d, ls2, hs2, lr, hr,
                                            self.order_size, sp)
                                        if ok:
                                            ladder_opened = True
                                            self._total_open_qty = min(
                                                abs(self.hotstuff_position),
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
                                l_bid, l_ask, h_ws_bid, h_ws_ask)
                            if opp and opp[0] == self.position_direction:
                                cur_h = abs(self.hotstuff_position)
                                cur_l = abs(self.lighter_position)
                                cur_min = min(cur_h, cur_l)
                                refill_qty = self._total_open_qty - cur_min
                                if refill_qty > Decimal("0"):
                                    refill_qty = refill_qty.quantize(
                                        self.hs_lot_size, rounding=ROUND_DOWN)
                                if refill_qty >= self.order_size * Decimal("0.05"):
                                    d, ls2, hs2, lr, hr, sp = opp
                                    self.logger.info(
                                        f"[回补] 价差={sp:.1f}bps "
                                        f"回补 {refill_qty}")
                                    ok = await self._open_position(
                                        d, ls2, hs2, lr, hr, refill_qty, sp)
                                    if ok:
                                        refilled = True
                                        self._tier_closed_pct = 0.0
                                        self._tier_close_records.clear()
                                        self._total_open_qty = min(
                                            abs(self.hotstuff_position),
                                            abs(self.lighter_position))
                                    else:
                                        self._last_trade_time = time.time()

                        # ── Close check ──
                        if not just_traded and not ladder_opened and not refilled:
                            tier_hit = self._check_close_condition(
                                l_bid, l_ask, h_ws_bid, h_ws_ask)
                            if tier_hit is not None:
                                _tier_sp, _batch_pct = tier_hit
                                if self.maker_mode:
                                    close_ok = await self._close_position_mt(
                                        l_bid, l_ask, h_ws_bid, h_ws_ask,
                                        batch_pct=_batch_pct)
                                    if not close_ok:
                                        close_ok = await self._close_position(
                                            l_bid, l_ask, h_ws_bid, h_ws_ask,
                                            batch_pct=_batch_pct)
                                else:
                                    close_ok = await self._close_position(
                                        l_bid, l_ask, h_ws_bid, h_ws_ask,
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

    def _log_ref_pnl(self, close_h_ref: Decimal, close_l_ref: Decimal,
                     close_qty: Optional[Decimal] = None,
                     h_estimated: bool = False) -> float:
        if not self._open_h_refs or not self._open_l_refs:
            return 0.0
        h_open_qty = sum(q for _, q in self._open_h_refs)
        l_open_qty = sum(q for _, q in self._open_l_refs)
        h_avg = sum(p * q for p, q in self._open_h_refs) / h_open_qty
        l_avg = sum(p * q for p, q in self._open_l_refs) / l_open_qty
        qty = close_qty if close_qty else max(h_open_qty, l_open_qty)

        if self.position_direction == "long_X_short_L":
            h_pnl = float((close_h_ref - h_avg) * qty)
            l_pnl = float((l_avg - close_l_ref) * qty)
        else:
            h_pnl = float((h_avg - close_h_ref) * qty)
            l_pnl = float((close_l_ref - l_avg) * qty)

        total_gross = h_pnl + l_pnl
        notional = float(qty) * float(close_h_ref)
        fee_cost = notional * self.ROUND_TRIP_FEE_BPS / 10000
        total_net = total_gross - fee_cost
        gross_bps = total_gross / notional * 10000 if notional > 0 else 0.0
        net_bps = total_net / notional * 10000 if notional > 0 else 0.0
        est = "(估) " if h_estimated else ""
        self.logger.info(
            f"[P&L] {est}qty={qty} H={h_pnl:+.3f}$ L={l_pnl:+.3f}$ "
            f"毛利={total_gross:+.3f}$({gross_bps:+.1f}bps) "
            f"手续费={fee_cost:.3f}$ "
            f"净利={total_net:+.3f}$({net_bps:+.1f}bps)")
        return net_bps

    def _normalize_refs(self, remaining_qty: Decimal):
        if remaining_qty <= 0:
            return
        if self._open_h_refs:
            h_qty = sum(q for _, q in self._open_h_refs)
            if h_qty > remaining_qty:
                h_avg = sum(p * q for p, q in self._open_h_refs) / h_qty
                self._open_h_refs = [(h_avg, remaining_qty)]
        if self._open_l_refs:
            l_qty = sum(q for _, q in self._open_l_refs)
            if l_qty > remaining_qty:
                l_avg = sum(p * q for p, q in self._open_l_refs) / l_qty
                self._open_l_refs = [(l_avg, remaining_qty)]

    def _reset_position_state(self):
        self.position_opened_at = None
        self.position_direction = None
        self._open_h_refs.clear()
        self._open_l_refs.clear()
        self._last_open_spread_bps = 0.0
        self._open_dev_l_bps = 0.0
        self._open_dev_h_bps = 0.0
        self._open_excess_bps = 0.0
        self._open_theoretical_spread = 0.0
        self._tier_closed_pct = 0.0
        self._tier_close_records.clear()
        self._total_open_qty = Decimal("0")


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
        description="V15 HS Maker-First 主动挂单策略套利 (Lighter+Hotstuff)")
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
    p.add_argument("--price-tolerance", type=float, default=0.5,
                   help="maker价格更新容忍度bps (默认0.5)")
    p.add_argument("--maker-max-age", type=float, default=30.0,
                   help="单笔maker最大挂单秒数 (默认30)")
    p.add_argument("--maker-close-timeout", type=float, default=30.0,
                   help="平仓maker等待秒数 (默认30)")
    p.add_argument("--hedge-timeout", type=float, default=5.0,
                   help="成交后对冲最大秒数 (默认5, 因REST轮询延迟)")
    p.add_argument("--direction-cooldown", type=float, default=2.0,
                   help="方向切换冷却秒数 (默认2)")
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

    if args.no_maker:
        bot.maker_mode = False
        bot.ROUND_TRIP_FEE_BPS = bot.ROUND_TRIP_FEE_BPS_TAKER
        print("[模式] 双Taker模式 (往返≈5.0bps)")
        if args.target_spread < 6.0:
            print(f"[提示] 双taker模式需≥6bps覆盖费用, "
                  f"已将target_spread从{args.target_spread}调整为6.0")
            bot.target_spread = 6.0
    else:
        bot.maker_mode = True
        bot.ROUND_TRIP_FEE_BPS = bot.ROUND_TRIP_FEE_BPS_MAKER
        bot._dyn_window = args.dyn_window
        bot._dyn_percentile = args.dyn_percentile
        bot._price_tolerance_bps = args.price_tolerance
        bot._maker_max_age = args.maker_max_age
        bot._maker_close_timeout = args.maker_close_timeout
        bot._hedge_timeout = args.hedge_timeout
        bot._direction_cooldown = args.direction_cooldown
        bot._dyn_fee_floor = bot.ROUND_TRIP_FEE_BPS_MAKER + 0.3
        print(f"[模式] ★ Maker-First主动挂单(REST轮询) (往返≈{bot.ROUND_TRIP_FEE_BPS:.1f}bps)")
        print(f"  动态阈值: 窗口={args.dyn_window}s P{args.dyn_percentile:.0f} "
              f"价格容忍={args.price_tolerance}bps "
              f"最大挂龄={args.maker_max_age}s")

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
