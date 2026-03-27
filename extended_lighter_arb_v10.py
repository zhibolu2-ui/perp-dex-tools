#!/usr/bin/env python3
"""
Extended + Lighter 价差收敛套利 V10（基于V9, 平仓改为双边市价）

策略：
  开仓: Lighter POST_ONLY maker 挂单（双边挂单），价格基于 Extended BBO 计算。
         被成交后 → Extended IOC taker 对冲。
  平仓: 检测价差收敛 → Lighter IOC taker 市价平仓 + Extended IOC taker 市价平仓。

费用: ~5.0 bps 往返 (Extended taker 2.5bps 开仓对冲 + Extended taker 2.5bps 平仓对冲)
      Lighter maker/taker 均 0% (Premium 账户 0 延迟下单)

与 V9 的区别：
  V9: 平仓使用 Lighter maker 挂单等待被吃单, 有延迟和孤儿订单风险
  V10: 平仓使用 Lighter IOC taker 市价立即成交, 速度更快, 无孤儿风险

用法:
  python extended_lighter_arb_v10.py --symbol BTC --size 0.002 --target-spread 6
  python extended_lighter_arb_v10.py --symbol ETH --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import signal
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
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


class State(str, Enum):
    QUOTING = "QUOTING"
    HEDGING = "HEDGING"
    IN_POSITION = "IN_POSITION"
    CLOSING_HEDGE = "CLOSING_HEDGE"


@dataclass
class PendingMaker:
    order_id: str  # client_order_index (Lighter)
    side: str
    direction: str
    qty_target: Decimal
    price: Decimal
    placed_at: float
    filled_qty: Decimal = Decimal("0")
    hedged_qty: Decimal = Decimal("0")
    hedge_qty_needed: Decimal = Decimal("0")
    avg_price: Optional[Decimal] = None
    phase: str = "open"
    x_ref_at_place: Optional[Decimal] = None
    lighter_order_index: Optional[int] = None


class _Config:
    def __init__(self, d: dict):
        for k, v in d.items():
            setattr(self, k, v)


class ExtMakerBot:

    ROUND_TRIP_FEE_BPS = 5.0

    def __init__(
        self,
        symbol: str,
        order_size: Decimal,
        max_position: Decimal,
        target_spread: float,
        close_gap_bps: float,
        max_hold_sec: float,
        interval: float,
        reprice_interval: float,
        dry_run: bool,
        ladder_step: float = 0.0,
    ):
        self.symbol = symbol.upper()
        self.order_size = order_size
        self.max_position = max_position
        self.target_spread = target_spread
        self.close_gap_bps = close_gap_bps
        self.max_hold_sec = max_hold_sec
        self.interval = interval
        self.reprice_interval = reprice_interval
        self.dry_run = dry_run
        self.ladder_step = ladder_step

        self.state = State.QUOTING
        self.stop_flag = False

        self.lighter_position = Decimal("0")
        self.extended_position = Decimal("0")
        self.position_opened_at: Optional[float] = None
        self.position_direction: Optional[str] = None

        self._pending: Optional[PendingMaker] = None
        self._fill_event = asyncio.Event()
        self._hedge_lock = asyncio.Lock()

        self._open_buy: Optional[PendingMaker] = None
        self._open_sell: Optional[PendingMaker] = None
        self._open_buy_reprice_at: float = 0.0
        self._open_sell_reprice_at: float = 0.0

        self._x_ioc_pending_id: Optional[str] = None
        self._x_ioc_confirmed = asyncio.Event()
        self._x_ioc_fill_qty: Decimal = Decimal("0")
        self._x_ioc_fill_price: Optional[Decimal] = None
        self._x_ioc_recent_ids: List[str] = []

        self._open_x_refs: List[Tuple[Decimal, Decimal]] = []
        self._open_l_refs: List[Tuple[Decimal, Decimal]] = []

        self._lighter_fill_event = asyncio.Event()
        self._lighter_fill_data: Optional[dict] = None
        self._lighter_ws_task: Optional[asyncio.Task] = None
        self._last_lighter_avg_price: Optional[Decimal] = None

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
        self._last_reprice: float = 0.0
        self._maker_pulled: bool = False
        self._nonce_counter: int = 0
        self._nonce_error_count: int = 0

        self._tx_timestamps: List[float] = []
        self._tx_backoff_until: float = 0.0

        self._post_close_cooldown: float = 30.0
        self._last_close_time: float = 0.0
        self._consecutive_losses: int = 0
        self._loss_streak_limit: int = 3
        self._loss_breaker_until: float = 0.0
        self._loss_breaker_threshold: float = -15.0
        self._loss_breaker_duration: float = 300.0

        self._cancel_grace_until: float = 0.0
        self._force_reconcile: bool = False
        self._last_open_spread_bps: float = 0.0
        self._last_ladder_attempt: float = 0.0

        self._active_lighter_oidxs: set = set()
        self._oidx_cleared_event = asyncio.Event()
        self._last_modify_ts: dict = {}
        self._min_modify_interval: float = 1.0

        os.makedirs("logs", exist_ok=True)
        self.csv_path = f"logs/arb_v10_{self.symbol}_trades.csv"
        self.log_path = f"logs/arb_v10_{self.symbol}.log"
        self._init_csv()
        self.logger = self._init_logger()

    # ═══════════════════════════════════════════════
    #  Logging / CSV
    # ═══════════════════════════════════════════════

    def _init_logger(self) -> logging.Logger:
        lg = logging.getLogger(f"arb_v9_{self.symbol}")
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
                    "lighter_side", "lighter_price", "lighter_qty",
                    "extended_side", "extended_price", "extended_qty",
                    "spread_bps", "hedge_ms", "net_bps", "cumulative_net_bps",
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

    def _init_extended_client(self):
        cfg = _Config({
            "ticker": self.symbol, "contract_id": "",
            "quantity": self.order_size, "tick_size": Decimal("0.01"),
            "close_order_side": "sell",
        })
        self.extended_client = ExtendedClient(cfg)
        self.extended_client.setup_order_update_handler(self._on_extended_order_event)
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
    #  Extended WS BBO (reference prices for Lighter maker)
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

    # ═══════════════════════════════════════════════
    #  Extended WS Event (IOC fill tracking only)
    # ═══════════════════════════════════════════════

    def _on_extended_order_event(self, event: dict):
        order_id = str(event.get("order_id", ""))
        status = event.get("status", "")
        filled_size_str = event.get("filled_size", "0")

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

        if order_id in self._x_ioc_recent_ids:
            return

        if status in ("FILLED", "PARTIALLY_FILLED"):
            filled = Decimal(str(filled_size_str))
            if filled > 0:
                self.logger.error(
                    f"[Extended] 孤立订单成交! id={order_id} status={status} "
                    f"filled={filled}  触发立即对账+紧急平仓")
                self._force_reconcile = True
                self._fill_event.set()

    # ═══════════════════════════════════════════════
    #  Lighter WS Maker Fill Detection
    # ═══════════════════════════════════════════════

    def _on_lighter_maker_fill(self, od: dict):
        """Handle Lighter WS order updates for our maker orders."""
        try:
            order_idx_raw = od.get("order_index")
            ws_status = od.get("status", "").upper()
            if order_idx_raw is not None:
                oidx_int = int(order_idx_raw)
                if ws_status == "OPEN":
                    self._active_lighter_oidxs.add(oidx_int)
                elif ws_status in ("CANCELED", "FILLED"):
                    self._active_lighter_oidxs.discard(oidx_int)
                    self._oidx_cleared_event.set()

            coi = od.get("client_order_index")
            if coi is None:
                return
            coi_str = str(coi)

            if self._pending and self._pending.order_id == coi_str:
                self._handle_lighter_pending_fill(od)
                return

            for side_name in ("buy", "sell"):
                maker = self._open_buy if side_name == "buy" else self._open_sell
                if maker and maker.order_id == coi_str:
                    self._handle_lighter_open_fill(od, side_name)
                    return

            status = ws_status
            filled_base = od.get("filled_base_amount")
            if status in ("FILLED", "PARTIALLY_FILLED") and filled_base is not None:
                fb = Decimal(str(filled_base))
                if fb > 0:
                    self.logger.error(
                        f"[Lighter] 孤立订单成交! coi={coi_str} status={status} "
                        f"filled={fb} 触发强制对账")
                    self._force_reconcile = True
                    self._fill_event.set()
        except Exception as e:
            self.logger.error(
                f"处理 Lighter maker 事件出错: {e}\n{traceback.format_exc()}")

    def _handle_lighter_pending_fill(self, od: dict):
        try:
            status = od.get("status", "").upper()
            order_idx = od.get("order_index")
            filled_base = od.get("filled_base_amount")

            if order_idx is not None and self._pending.lighter_order_index is None:
                self._pending.lighter_order_index = int(order_idx)

            if status == "OPEN":
                self.logger.info(
                    f"[Lighter] Maker 挂单已上线 coi={self._pending.order_id} "
                    f"oidx={order_idx}")
                if order_idx is not None:
                    self._pending.lighter_order_index = int(order_idx)
                return

            if status in ("FILLED", "PARTIALLY_FILLED"):
                if filled_base is None:
                    return
                filled_size = Decimal(str(filled_base))
                if filled_size <= self._pending.filled_qty:
                    return

                self._pending.filled_qty = filled_size
                self._extract_lighter_fill_price(od, self._pending.price)
                if self._last_lighter_avg_price:
                    self._pending.avg_price = self._last_lighter_avg_price

                self._pending.hedge_qty_needed = filled_size - self._pending.hedged_qty

                if self._pending.hedge_qty_needed >= self.order_size * Decimal("0.01"):
                    if self._pending.phase == "open":
                        self.state = State.HEDGING
                    else:
                        self.state = State.CLOSING_HEDGE
                    self.logger.info(
                        f"[Lighter FILL] {self._pending.side} filled={filled_size} "
                        f"avg={self._pending.avg_price} 待对冲={self._pending.hedge_qty_needed}")
                    self._fill_event.set()
                    asyncio.ensure_future(self._fast_hedge())

            elif status == "CANCELED":
                if filled_base is not None:
                    fb = Decimal(str(filled_base))
                    if fb > 0 and fb > (self._pending.filled_qty if self._pending else Decimal("0")):
                        self._handle_lighter_pending_fill({**od, "status": "FILLED"})
                        return
                if time.time() < self._cancel_grace_until:
                    return
                self.logger.info(f"[Lighter] Maker 挂单已取消 coi={self._pending.order_id}")
                if self._pending and self.state in (State.QUOTING, State.IN_POSITION):
                    self._pending = None
        except Exception as e:
            self.logger.error(f"处理 Lighter pending fill 出错: {e}\n{traceback.format_exc()}")

    def _handle_lighter_open_fill(self, od: dict, side: str):
        try:
            maker = self._open_buy if side == "buy" else self._open_sell
            if maker is None:
                return

            status = od.get("status", "").upper()
            order_idx = od.get("order_index")
            filled_base = od.get("filled_base_amount")

            if order_idx is not None and maker.lighter_order_index is None:
                maker.lighter_order_index = int(order_idx)

            if status == "OPEN":
                self.logger.info(
                    f"[Lighter] Open {side} maker 已上线 coi={maker.order_id} "
                    f"oidx={order_idx}")
                if order_idx is not None:
                    maker.lighter_order_index = int(order_idx)
                return

            if status in ("FILLED", "PARTIALLY_FILLED"):
                if filled_base is None:
                    return
                filled_size = Decimal(str(filled_base))
                if filled_size <= maker.filled_qty:
                    return

                maker.filled_qty = filled_size
                self._extract_lighter_fill_price(od, maker.price)
                if self._last_lighter_avg_price:
                    maker.avg_price = self._last_lighter_avg_price

                maker.hedge_qty_needed = filled_size - maker.hedged_qty

                if maker.hedge_qty_needed >= self.order_size * Decimal("0.01"):
                    self._pending = maker
                    other_side = "sell" if side == "buy" else "buy"
                    other = self._open_sell if side == "buy" else self._open_buy
                    if other:
                        self.logger.info(
                            f"[双边] {side} 成交, 撤销对侧 {other_side} "
                            f"oidx={other.lighter_order_index}")
                        self._cancel_grace_until = time.time() + 2.0
                        if other.lighter_order_index is not None:
                            asyncio.ensure_future(
                                self._cancel_lighter_order(other.lighter_order_index))
                        else:
                            asyncio.ensure_future(self._do_lighter_cancel_all())

                    self._open_buy = None
                    self._open_sell = None
                    self.state = State.HEDGING
                    self.logger.info(
                        f"[Lighter FILL] {side} filled={filled_size} "
                        f"avg={maker.avg_price} 待对冲={maker.hedge_qty_needed}")
                    self._fill_event.set()
                    asyncio.ensure_future(self._fast_hedge())

            elif status == "CANCELED":
                if filled_base is not None:
                    fb = Decimal(str(filled_base))
                    if fb > 0 and fb > maker.filled_qty:
                        self._handle_lighter_open_fill({**od, "status": "FILLED"}, side)
                        return
                if time.time() < self._cancel_grace_until:
                    return
                self.logger.info(f"[Lighter] Open {side} maker 已取消 coi={maker.order_id}")
                if self.state in (State.QUOTING, State.IN_POSITION):
                    if side == "buy":
                        self._open_buy = None
                    else:
                        self._open_sell = None
        except Exception as e:
            self.logger.error(
                f"处理 Lighter open {side} fill 出错: {e}\n{traceback.format_exc()}")

    # ═══════════════════════════════════════════════
    #  Fast Hedge (Extended IOC Taker)
    # ═══════════════════════════════════════════════

    async def _fast_hedge(self):
        async with self._hedge_lock:
            if self._pending is None or self._pending.hedge_qty_needed <= 0:
                return

            hedge_qty = self._pending.hedge_qty_needed
            phase = self._pending.phase
            direction = self._pending.direction
            l_fill_price = self._pending.avg_price or self._pending.price

            if phase == "open":
                hedge_side = "sell" if direction == "long_L_short_X" else "buy"
            else:
                l_side = self._pending.side
                hedge_side = "sell" if l_side == "buy" else "buy"

            x_bid, x_ask = self._get_extended_ws_bbo()
            if x_bid is None or x_ask is None:
                self.logger.warning("[FAST_HEDGE] Extended 行情不可用")
                return
            x_ref = x_bid if hedge_side == "sell" else x_ask

            t0 = time.time()
            actual_fill = await self._place_extended_ioc(hedge_side, hedge_qty, x_ref)
            hedge_ms = (time.time() - t0) * 1000

            if actual_fill is None or actual_fill <= 0:
                self.logger.warning(
                    f"[FAST_HEDGE] Extended IOC 失败({hedge_ms:.0f}ms), 回退主循环")
                return

            self._pending.hedged_qty += actual_fill
            self._pending.hedge_qty_needed -= actual_fill
            x_actual = self._x_ioc_fill_price or x_ref

            ls = self.lighter_feed.snapshot if self.lighter_feed else None
            l_mid = Decimal(str(ls.mid)) if ls and ls.mid else l_fill_price
            x_mid = (x_bid + x_ask) / 2
            mid_spread_bps = float(
                (x_mid - l_mid) / l_mid * 10000) if l_mid > 0 else 0.0

            if phase == "open":
                self._open_l_refs.append((l_fill_price, actual_fill))
                self._open_x_refs.append((x_actual, actual_fill))

                if direction == "long_L_short_X":
                    self.lighter_position += actual_fill
                    self.extended_position -= actual_fill
                else:
                    self.lighter_position -= actual_fill
                    self.extended_position += actual_fill

                if direction == "long_X_short_L":
                    _fill_sp = float(
                        (l_fill_price - x_actual) / l_fill_price * 10000
                    ) if l_fill_price > 0 else 0.0
                else:
                    _fill_sp = float(
                        (x_actual - l_fill_price) / l_fill_price * 10000
                    ) if l_fill_price > 0 else 0.0
                _sp_quality = ("OK" if _fill_sp >= self.ROUND_TRIP_FEE_BPS
                               else "LOW" if _fill_sp > 0 else "NEG")
                self.logger.info(
                    f"[FAST_HEDGE] {hedge_ms:.0f}ms fill={actual_fill}/{hedge_qty} "
                    f"实际价差={_fill_sp:.1f}bps({_sp_quality}) "
                    f"L={l_fill_price:.2f} X={x_actual:.2f} "
                    f"L_pos={self.lighter_position} X_pos={self.extended_position}")

                if self._pending.hedged_qty >= self._pending.filled_qty:
                    if self._pending.filled_qty < self._pending.qty_target:
                        self.logger.info("[FAST_HEDGE] 部分成交已对冲, 撤销剩余Lighter挂单")
                        self._cancel_grace_until = time.time() + 2.0
                        await self._safe_cancel_lighter(self._pending)
                    if self.position_opened_at is None:
                        self.position_opened_at = time.time()
                    self.position_direction = direction
                    self._last_open_spread_bps = max(_fill_sp, self.target_spread)
                    self._csv_row([
                        datetime.now(timezone.utc).isoformat(),
                        "OPEN", direction,
                        self._pending.side, f"{l_fill_price:.2f}", str(actual_fill),
                        hedge_side, f"{x_actual:.2f}", str(actual_fill),
                        f"{mid_spread_bps:.2f}", f"{hedge_ms:.0f}", "", "",
                    ])
                    self._pending = None
                    self.state = State.IN_POSITION
                else:
                    self.logger.info(
                        f"[FAST_HEDGE] 部分对冲完成, 剩余待冲="
                        f"{self._pending.hedge_qty_needed}")
                    self.state = State.HEDGING

            else:
                l_side = self._pending.side
                if l_side == "buy":
                    self.lighter_position += actual_fill
                else:
                    self.lighter_position -= actual_fill
                if hedge_side == "sell":
                    self.extended_position -= actual_fill
                else:
                    self.extended_position += actual_fill

                net_bps = self._log_ref_pnl(x_actual, l_fill_price, actual_fill)
                self.cumulative_net_bps += net_bps

                self.logger.info(
                    f"[FAST_CLOSE] {hedge_ms:.0f}ms fill={actual_fill}/{hedge_qty} "
                    f"净利={net_bps:+.2f}bps 累积={self.cumulative_net_bps:+.2f}bps")
                self._csv_row([
                    datetime.now(timezone.utc).isoformat(),
                    "CLOSE", "convergence",
                    self._pending.side, f"{l_fill_price:.2f}", str(actual_fill),
                    hedge_side, f"{x_actual:.2f}", str(actual_fill),
                    f"{mid_spread_bps:.2f}", f"{hedge_ms:.0f}",
                    f"{net_bps:.2f}", f"{self.cumulative_net_bps:.2f}",
                ])

                if self._pending.hedged_qty >= self._pending.filled_qty:
                    if self._pending.filled_qty < self._pending.qty_target:
                        self.logger.info("[FAST_CLOSE] 部分成交已对冲, 撤销剩余Lighter挂单")
                        self._cancel_grace_until = time.time() + 2.0
                        await self._safe_cancel_lighter(self._pending)
                    self._pending = None
                    pos_remaining = abs(self.extended_position)
                    if pos_remaining < self.order_size * Decimal("0.01"):
                        self._last_close_time = time.time()
                        if net_bps < 0:
                            self._consecutive_losses += 1
                        else:
                            self._consecutive_losses = 0
                        if (self._consecutive_losses >= self._loss_streak_limit
                                or self.cumulative_net_bps < self._loss_breaker_threshold):
                            self._loss_breaker_until = time.time() + self._loss_breaker_duration
                            self.logger.error(
                                f"[熔断] 连亏={self._consecutive_losses} "
                                f"累积={self.cumulative_net_bps:+.2f}bps, "
                                f"暂停{self._loss_breaker_duration:.0f}s")
                        self._reset_position_state()
                        self.state = State.QUOTING
                    else:
                        self._normalize_refs(pos_remaining)
                        self.state = State.IN_POSITION
                else:
                    self.logger.info(
                        f"[FAST_CLOSE] 部分对冲完成, "
                        f"剩余待冲={self._pending.hedge_qty_needed}")
                    self.state = State.CLOSING_HEDGE

    # ═══════════════════════════════════════════════
    #  Close condition check
    # ═══════════════════════════════════════════════

    def _should_close_now(
        self, l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
    ) -> bool:
        if self.extended_position > 0:
            current_gap = l_ask - x_bid
            ref = x_bid if x_bid > 0 else l_ask
            gap_bps = float(current_gap / ref * 10000) if ref > 0 else 999.0
            return gap_bps <= self.close_gap_bps
        elif self.extended_position < 0:
            current_gap = x_ask - l_bid
            ref = l_bid if l_bid > 0 else x_ask
            gap_bps = float(current_gap / ref * 10000) if ref > 0 else 999.0
            return gap_bps <= self.close_gap_bps
        return False

    # ═══════════════════════════════════════════════
    #  Maker Pricing (Lighter maker, Extended BBO reference)
    # ═══════════════════════════════════════════════

    def _round_lighter_price(self, price: Decimal) -> Decimal:
        if self.lighter_tick > 0:
            return (price / self.lighter_tick).quantize(Decimal("1")) * self.lighter_tick
        return price

    def _calc_open_targets(
        self, l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
    ) -> Tuple[Optional[Tuple[str, str, Decimal]],
               Optional[Tuple[str, str, Decimal]]]:
        """Calculate BOTH buy and sell Lighter maker targets for opening.
        Returns (buy_target, sell_target).
        Each is (side, direction, price) or None if not viable.
        POST_ONLY constraint: buy < l_ask, sell > l_bid."""
        spread_bps = Decimal(str(self.target_spread))
        min_profit = x_bid * Decimal("0.5") / Decimal("10000")

        buy_target_price = self._round_lighter_price(
            x_bid * (Decimal("1") - spread_bps / Decimal("10000")))
        buy_price = min(buy_target_price, l_ask - self.lighter_tick)
        buy_result = None
        if x_bid - buy_price >= min_profit and buy_price > 0 and buy_price < l_ask:
            buy_result = ("buy", "long_L_short_X", buy_price)

        sell_target_price = self._round_lighter_price(
            x_ask * (Decimal("1") + spread_bps / Decimal("10000")))
        sell_price = max(sell_target_price, l_bid + self.lighter_tick)
        sell_result = None
        if sell_price - x_ask >= min_profit and sell_price > l_bid and sell_price > 0:
            sell_result = ("sell", "long_X_short_L", sell_price)

        return buy_result, sell_result

    # ═══════════════════════════════════════════════
    #  Lighter Maker Order Management
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

    async def _place_lighter_maker_at(self, side: str, qty: Decimal,
                                       price: Decimal) -> Optional[int]:
        if not self._check_tx_rate():
            self.logger.debug("[Lighter] 跳过下单: 接近速率限制")
            return None
        self._nonce_counter += 1
        client_order_index = int(time.time() * 1000) * 100 + (self._nonce_counter % 100)
        is_ask = side.lower() == "sell"
        rounded_price = self._round_lighter_price(price)
        try:
            result, _, error = await self.lighter_client.create_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=int(qty * self.base_amount_multiplier),
                price=int(rounded_price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_POST_ONLY,
                reduce_only=False,
                trigger_price=0,
            )
            self._record_tx()
            if error is not None:
                if self._check_nonce_error(error):
                    return None
                err_str = str(error).lower()
                if "rate" in err_str or "quota" in err_str:
                    self._tx_backoff_until = time.time() + 5.0
                    self.logger.warning(f"[Lighter] maker 速率限制: {error}")
                    return None
                self.logger.error(f"[Lighter] maker 下单失败: {error}")
                return None
            self._nonce_error_count = 0
            self.logger.info(
                f"[Lighter] maker {side} {qty} @ {rounded_price}  "
                f"coi={client_order_index}")
            return client_order_index
        except Exception as e:
            self._check_nonce_error(e)
            self.logger.error(f"[Lighter] maker 异常: {e}")
            return None

    async def _safe_cancel_lighter(self, maker: PendingMaker) -> bool:
        """Cancel a Lighter maker order safely, falling back to cancel_all."""
        if maker.lighter_order_index is not None:
            return await self._cancel_lighter_order(maker.lighter_order_index)
        self.logger.warning("[撤单] order_index 未知, cancel_all 兜底")
        return await self._do_lighter_cancel_all()

    async def _cancel_lighter_order(self, order_index: int) -> bool:
        if self.dry_run or not self.lighter_client:
            return True
        try:
            _, _, error = await self.lighter_client.cancel_order(
                market_index=self.lighter_market_index,
                order_index=order_index)
            self._record_tx()
            if error is None:
                self.logger.info(f"[Lighter] 撤单成功 oidx={order_index}")
                return True
            self._check_nonce_error(error)
            self.logger.error(f"[Lighter] 撤单失败 oidx={order_index}: {error}")
            return False
        except Exception as e:
            self._check_nonce_error(e)
            self.logger.error(f"[Lighter] 撤单异常 oidx={order_index}: {e}")
            return False

    async def _modify_lighter_order(
        self, order_index: int, new_price: Decimal, new_qty: Decimal,
    ) -> Optional[bool]:
        """返回 True=成功, False=真正失败, None=节流跳过"""
        if self.dry_run or not self.lighter_client:
            return True
        now = time.time()
        last_ts = self._last_modify_ts.get(order_index, 0.0)
        if now - last_ts < self._min_modify_interval:
            return None
        if not self._check_tx_rate():
            return None
        rounded_price = self._round_lighter_price(new_price)
        try:
            _, _, error = await self.lighter_client.modify_order(
                market_index=self.lighter_market_index,
                order_index=order_index,
                base_amount=int(new_qty * self.base_amount_multiplier),
                price=int(rounded_price * self.price_multiplier),
            )
            self._record_tx()
            self._last_modify_ts[order_index] = time.time()
            if error is not None:
                self._check_nonce_error(error)
                self.logger.warning(
                    f"[Lighter] modify 失败 oidx={order_index}: {error}")
                return False
            self._nonce_error_count = 0
            self.logger.info(
                f"[Lighter] modify oidx={order_index} → {rounded_price}")
            return True
        except Exception as e:
            self._check_nonce_error(e)
            self.logger.warning(f"[Lighter] modify 异常 oidx={order_index}: {e}")
            return False

    async def _wait_orders_cleared(self, timeout: float = 3.0) -> bool:
        if not self._active_lighter_oidxs:
            return True
        t0 = time.time()
        while self._active_lighter_oidxs and time.time() - t0 < timeout:
            self._oidx_cleared_event.clear()
            try:
                remaining = timeout - (time.time() - t0)
                await asyncio.wait_for(
                    self._oidx_cleared_event.wait(),
                    timeout=min(remaining, 0.5))
            except asyncio.TimeoutError:
                pass
        if self._active_lighter_oidxs:
            self.logger.warning(
                f"[等待撤单] 超时({timeout:.1f}s), 仍有 "
                f"{len(self._active_lighter_oidxs)} 个活跃订单: "
                f"{self._active_lighter_oidxs}")
            return False
        return True

    async def _ensure_no_orphans(self) -> bool:
        expected_oidxs = set()
        for m in [self._open_buy, self._open_sell, self._pending]:
            if m and m.lighter_order_index is not None:
                expected_oidxs.add(m.lighter_order_index)

        orphans = self._active_lighter_oidxs - expected_oidxs
        if not orphans:
            return True

        self.logger.warning(
            f"[孤儿检测] 发现 {len(orphans)} 个未跟踪订单: {orphans}, "
            f"执行 cancel_all (预期={expected_oidxs} "
            f"WS追踪={self._active_lighter_oidxs})")
        self._cancel_grace_until = time.time() + 3.0
        await self._do_lighter_cancel_all()
        cleared = await self._wait_orders_cleared(3.0)
        remaining = self._active_lighter_oidxs - expected_oidxs
        if remaining:
            for oidx in list(remaining):
                try:
                    await self._cancel_lighter_order(oidx)
                except Exception:
                    pass
            await self._wait_orders_cleared(2.0)
            remaining2 = self._active_lighter_oidxs - expected_oidxs
            if remaining2:
                self.logger.error(
                    f"[孤儿检测] 逐个撤单后仍有 {len(remaining2)} 个孤儿: "
                    f"{remaining2}")
                return False
        self.logger.info("[孤儿检测] 孤儿订单已清除")
        return True

    async def _rest_reconcile_orders(self) -> int:
        if self.dry_run or not self.lighter_client:
            return 0
        try:
            auth_token, err = self.lighter_client.create_auth_token_with_expiry(
                api_key_index=self.api_key_index)
            if err is not None:
                self.logger.warning(f"[REST对账] auth token 失败: {err}")
                return -1

            orders_resp = await self.lighter_client.order_api.account_active_orders(
                account_index=self.account_index,
                market_id=self.lighter_market_index,
                auth=auth_token)

            if orders_resp is None:
                self.logger.info("[REST对账] 响应为空, 视为无活跃订单")
                phantom = set(self._active_lighter_oidxs)
                if phantom:
                    self._active_lighter_oidxs.clear()
                    self._oidx_cleared_event.set()
                    self.logger.info(
                        f"[REST对账] 清除WS幻影: {phantom}")
                return 0

            rest_oidxs = set()
            for o in orders_resp.orders:
                rest_oidxs.add(int(o.order_index))

            self.logger.info(
                f"[REST对账] 链上活跃={len(rest_oidxs)} "
                f"WS追踪={len(self._active_lighter_oidxs)} "
                f"链上={rest_oidxs} WS={self._active_lighter_oidxs}")

            expected_oidxs = set()
            for m in [self._open_buy, self._open_sell, self._pending]:
                if m and m.lighter_order_index is not None:
                    expected_oidxs.add(m.lighter_order_index)

            ws_missed = rest_oidxs - self._active_lighter_oidxs
            if ws_missed:
                self.logger.warning(
                    f"[REST对账] WS未追踪的链上订单: {ws_missed}")
                self._active_lighter_oidxs.update(ws_missed)

            orphans = rest_oidxs - expected_oidxs
            if orphans:
                self.logger.warning(
                    f"[REST对账] 发现 {len(orphans)} 个孤儿订单: {orphans}, "
                    f"执行cancel_all")
                self._cancel_grace_until = time.time() + 3.0
                await self._do_lighter_cancel_all()
                await self._wait_orders_cleared(3.0)
                return len(orphans)

            phantom = self._active_lighter_oidxs - rest_oidxs
            if phantom:
                self.logger.info(
                    f"[REST对账] WS多追踪的幻影订单(已不在链上): {phantom}")
                self._active_lighter_oidxs -= phantom
                self._oidx_cleared_event.set()

            return 0
        except Exception as e:
            self.logger.warning(
                f"[REST对账] 查询异常: {e}\n{traceback.format_exc()}")
            return -1

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

    async def _update_maker(
        self, target_side: str, target_direction: str,
        target_price: Decimal, qty: Decimal, phase: str,
        x_ref_now: Optional[Decimal] = None,
    ) -> bool:
        now = time.time()

        if self._pending is not None:
            if self._pending.filled_qty > 0 and self._pending.hedge_qty_needed > 0:
                self.logger.info(
                    f"[调价] 跳过: 待对冲={self._pending.hedge_qty_needed}")
                if not self._hedge_lock.locked():
                    asyncio.ensure_future(self._fast_hedge())
                return True

            if self._pending.filled_qty >= self._pending.qty_target:
                self.logger.info("[调价] 跳过: 订单已完全成交, 等待对冲完成")
                return True

            same_side = self._pending.side == target_side
            price_diff = abs(self._pending.price - target_price)
            needs_reprice = price_diff >= self.lighter_tick

            if same_side and not needs_reprice:
                return True

            urgent = not same_side or price_diff >= self.lighter_tick * 5
            if not urgent and now - self._last_reprice < self.reprice_interval:
                return self._pending is not None

            tag = "[紧急调价]" if urgent else "[调价]"

            if (same_side and needs_reprice
                    and self._pending.lighter_order_index is not None):
                mod_ok = await self._modify_lighter_order(
                    self._pending.lighter_order_index, target_price, qty)
                if mod_ok is True:
                    self._pending.price = target_price
                    self._pending.x_ref_at_place = x_ref_now
                    self._last_reprice = now
                    return True
                if same_side:
                    if mod_ok is False:
                        self.logger.info(
                            f"{tag} modify 失败, 下次重试")
                    return True

            self.logger.info(
                f"{tag} {self._pending.side}@{self._pending.price} → "
                f"{target_side}@{target_price}  diff={price_diff}")
            self._cancel_grace_until = time.time() + 2.0
            saved = self._pending
            if saved.lighter_order_index is not None:
                await self._cancel_lighter_order(saved.lighter_order_index)
            else:
                self.logger.warning("[调价] order_index 未知, cancel_all 兜底")
                await self._do_lighter_cancel_all()
            um_cleared = await self._wait_orders_cleared(2.0)
            if self._pending is not saved:
                self.logger.warning(f"{tag} cancel 期间发生成交, 保持当前状态")
                return self._pending is not None
            if not um_cleared:
                self.logger.warning(f"{tag} 撤单未确认, 跳过本次调价")
                return self._pending is not None
            if self._pending and self._pending.filled_qty > self._pending.hedged_qty:
                return True
            self._pending = None

        if not await self._ensure_no_orphans():
            self.logger.warning(f"[调价] 仍有孤儿订单, 跳过下单")
            return self._pending is not None

        coi = await self._place_lighter_maker_at(target_side, qty, target_price)
        if coi is not None:
            self._pending = PendingMaker(
                order_id=str(coi),
                side=target_side,
                direction=target_direction,
                qty_target=qty,
                price=target_price,
                placed_at=now,
                phase=phase,
                x_ref_at_place=x_ref_now,
            )
            self._fill_event.clear()
            self._last_reprice = now
            return True
        return False

    async def _update_open_side(
        self, side: str, target_price: Decimal,
        direction: str, x_ref: Decimal,
    ) -> bool:
        now = time.time()

        if self.state != State.QUOTING:
            return True

        maker = self._open_buy if side == "buy" else self._open_sell
        last_reprice = (self._open_buy_reprice_at if side == "buy"
                        else self._open_sell_reprice_at)

        if maker is not None:
            if maker.filled_qty > 0:
                return True

            price_diff = abs(maker.price - target_price)
            if price_diff < self.lighter_tick:
                return True

            urgent = price_diff >= self.lighter_tick * 5
            if not urgent and now - last_reprice < self.reprice_interval:
                return True

            if maker.lighter_order_index is not None:
                mod_ok = await self._modify_lighter_order(
                    maker.lighter_order_index, target_price, self.order_size)
                if mod_ok is True:
                    maker.price = target_price
                    maker.x_ref_at_place = x_ref
                    if side == "buy":
                        self._open_buy_reprice_at = now
                    else:
                        self._open_sell_reprice_at = now
                    return True
                if mod_ok is False:
                    self.logger.info(
                        f"[调价] {side} modify 失败, 下次重试")
                return True

            self._cancel_grace_until = time.time() + 2.0
            self.logger.warning(
                f"[调价] {side} order_index 未知, cancel_all 兜底")
            await self._do_lighter_cancel_all()
            cleared = await self._wait_orders_cleared(2.0)
            if not cleared:
                self.logger.warning(
                    f"[调价] {side} 撤单未确认, 跳过本次调价")
                return False

            if self.state != State.QUOTING:
                return True

            current = self._open_buy if side == "buy" else self._open_sell
            if current is None or current.order_id != maker.order_id:
                return True
            if current.filled_qty > 0:
                return True
            if side == "buy":
                self._open_buy = None
            else:
                self._open_sell = None

        if self.state != State.QUOTING:
            return True

        if not await self._ensure_no_orphans():
            self.logger.warning(
                f"[调价] {side} 仍有孤儿订单, 跳过下单")
            return False

        coi = await self._place_lighter_maker_at(side, self.order_size, target_price)
        if coi:
            if self.state != State.QUOTING:
                self.logger.warning(
                    f"[竞态保护] 下单期间状态变为 {self.state.value}, "
                    f"撤销刚下的 {side} maker")
                self._cancel_grace_until = time.time() + 2.0
                await self._do_lighter_cancel_all()
                return True

            new_maker = PendingMaker(
                order_id=str(coi), side=side, direction=direction,
                qty_target=self.order_size, price=target_price,
                placed_at=now, phase="open", x_ref_at_place=x_ref,
            )
            if side == "buy":
                self._open_buy = new_maker
                self._open_buy_reprice_at = now
            else:
                self._open_sell = new_maker
                self._open_sell_reprice_at = now
            self._fill_event.clear()
            return True
        return False

    async def _cancel_open_makers(self):
        need_cancel_all = False
        self._cancel_grace_until = time.time() + 3.0
        for side, maker in [("buy", self._open_buy), ("sell", self._open_sell)]:
            if maker and maker.filled_qty == 0:
                if maker.lighter_order_index is not None:
                    await self._cancel_lighter_order(maker.lighter_order_index)
                else:
                    need_cancel_all = True
        if need_cancel_all:
            self.logger.warning("[撤单] 有挂单缺少order_index, cancel_all兜底")
            await self._do_lighter_cancel_all()
        cleared = await self._wait_orders_cleared(2.0)
        if cleared:
            if self._open_buy and self._open_buy.filled_qty == 0:
                self._open_buy = None
            if self._open_sell and self._open_sell.filled_qty == 0:
                self._open_sell = None
        else:
            self.logger.warning(
                f"[撤单] 等待确认超时, 不清除本地状态, "
                f"残留={len(self._active_lighter_oidxs)}")

    # ═══════════════════════════════════════════════
    #  Lighter Taker (IOC — kept for emergency flatten)
    # ═══════════════════════════════════════════════

    async def _place_lighter_taker(self, side: str, qty: Decimal,
                                    ref_price: Decimal) -> Optional[Decimal]:
        is_ask = side.lower() == "sell"
        price = self._round_lighter_price(
            ref_price * (Decimal("0.998") if is_ask else Decimal("1.002")))

        pre_pos = self.lighter_position
        self._nonce_counter += 1
        coi = int(time.time() * 1000) * 100 + (self._nonce_counter % 100)

        self._lighter_fill_event.clear()
        self._lighter_fill_data = None
        self._last_lighter_avg_price = None

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
                await asyncio.wait_for(self._lighter_fill_event.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass

            fd = self._lighter_fill_data
            if fd is not None:
                status = fd.get("status", "").upper()
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
                            f"[Lighter] WS成交确认: status={status} filled={ws_filled} "
                            f"pos {pre_pos} → {new_pos}"
                            + (f" avg_price={avg_p:.2f}" if avg_p else ""))
                        return ws_filled

            for attempt, delay in enumerate([0.08, 0.15, 0.3]):
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

            self.logger.warning(f"[Lighter] IOC 未确认成交")
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
                    self.logger.info("[Lighter] cancel_all 执行成功")
                    return True
                self.logger.warning(f"[Lighter] cancel_all 返回错误: {error}")
                self._check_nonce_error(error)
            except Exception as e:
                self.logger.warning(f"[Lighter] cancel_all 异常: {e}")
                self._check_nonce_error(e)
            await asyncio.sleep(0.5)
        self.logger.error("[Lighter] cancel_all 3次重试均失败")
        return False

    # ═══════════════════════════════════════════════
    #  Lighter WS account_orders (maker fill detection)
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
                                if status in ("FILLED", "PARTIALLY_FILLED",
                                              "CANCELED", "OPEN"):
                                    self._on_lighter_maker_fill(od)
                                if status in ("FILLED", "PARTIALLY_FILLED", "CANCELED"):
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
    #  Extended IOC (对冲 + 紧急平仓)
    # ═══════════════════════════════════════════════

    async def _place_extended_ioc(self, side: str, qty: Decimal,
                                   ref_price: Decimal) -> Optional[Decimal]:
        if side == "buy":
            aggressive = ref_price * Decimal("1.005")
        else:
            aggressive = ref_price * Decimal("0.995")
        aggressive = aggressive.quantize(self.extended_tick_size, rounding=ROUND_HALF_UP)
        qty_r = qty.quantize(self.extended_min_order_size, rounding=ROUND_HALF_UP)

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
                    await asyncio.wait_for(self._x_ioc_confirmed.wait(), timeout=3.0)
                except asyncio.TimeoutError:
                    pass
                if self._x_ioc_fill_qty > 0:
                    actual = self._x_ioc_fill_qty
                elif self._x_ioc_confirmed.is_set():
                    self.logger.warning("[Extended] IOC 已确认但 fill=0")
                    self._x_ioc_recent_ids.append(self._x_ioc_pending_id)
                    if len(self._x_ioc_recent_ids) > 20:
                        self._x_ioc_recent_ids = self._x_ioc_recent_ids[-10:]
                    self._x_ioc_pending_id = None
                    return Decimal("0")
                else:
                    self.logger.warning(f"[Extended] IOC 超时未确认, 假定成交 {qty_r}")
                    actual = qty_r
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
    #  Position Queries
    # ═══════════════════════════════════════════════

    async def _get_lighter_position(self) -> Decimal:
        url = f"{self.lighter_base_url}/api/v1/account"
        params = {"by": "index", "value": self.account_index}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={"accept": "application/json"},
                                       params=params,
                                       timeout=aiohttp.ClientTimeout(total=10)) as resp:
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

    async def _reconcile_positions(self) -> bool:
        state_before = self.state
        l = await self._get_lighter_position()
        x = await self._get_extended_position()
        if self.state != state_before:
            return True
        self.lighter_position = l
        self.extended_position = x
        net = l + x
        balanced = abs(net) < self.order_size * Decimal("0.5")
        if not balanced:
            self.logger.warning(f"[对账] 不平衡: L={l} X={x} net={net}")
        return balanced

    # ═══════════════════════════════════════════════
    #  Emergency / Shutdown
    # ═══════════════════════════════════════════════

    async def _emergency_flatten(self):
        try:
            await self._do_lighter_cancel_all()
            await self._cancel_all_extended_orders_rest()
            await asyncio.sleep(0.5)

            if abs(self.lighter_position) > 0:
                side = "sell" if self.lighter_position > 0 else "buy"
                ls = self.lighter_feed.snapshot if self.lighter_feed else None
                if ls and ls.mid:
                    ref_val = (ls.best_bid or ls.mid) if side == "sell" else (ls.best_ask or ls.mid)
                    ref = Decimal(str(ref_val or 0))
                    if ref > 0:
                        self.logger.info(
                            f"[紧急平仓] Lighter IOC {side} {abs(self.lighter_position)} @ {ref}")
                        await self._place_lighter_taker(side, abs(self.lighter_position), ref)

            if abs(self.extended_position) > 0:
                side = "sell" if self.extended_position > 0 else "buy"
                x_bid, x_ask = self._get_extended_ws_bbo()
                x_ref = x_bid if side == "sell" else x_ask
                if x_ref:
                    self.logger.info(
                        f"[紧急平仓] Extended IOC {side} {abs(self.extended_position)} @ {x_ref}")
                    ioc_fill = await self._place_extended_ioc(side, abs(self.extended_position), x_ref)
                    if ioc_fill is not None and ioc_fill > 0:
                        if side == "sell":
                            self.extended_position -= ioc_fill
                        else:
                            self.extended_position += ioc_fill

            await asyncio.sleep(3)
            try:
                await self._reconcile_positions()
                if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                    self.logger.error(
                        f"[紧急平仓] 仍有残余仓位! L={self.lighter_position} "
                        f"X={self.extended_position} 请手动处理!")
                else:
                    self.logger.info("[紧急平仓] 仓位已清零")
            except Exception as re:
                self.logger.warning(f"[紧急平仓] 最终对账失败: {re}")
        except Exception as e:
            self.logger.error(f"紧急平仓异常: {e}\n{traceback.format_exc()}")

    async def _shutdown(self):
        self.stop_flag = True
        self.logger.info("关闭中...")
        await self._do_lighter_cancel_all()
        await self._cancel_all_extended_orders_rest()
        if self._lighter_ws_task and not self._lighter_ws_task.done():
            self._lighter_ws_task.cancel()
            try:
                await self._lighter_ws_task
            except (asyncio.CancelledError, Exception):
                pass
        for feed in (self.lighter_feed, self.extended_feed):
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
            self.logger.info("[退出] 清理所有挂单...")
            try:
                await self._cancel_open_makers()
            except Exception as e:
                self.logger.warning(f"[退出] 撤open makers异常: {e}")
            if self._pending:
                try:
                    await self._safe_cancel_lighter(self._pending)
                    self._pending = None
                except Exception as e:
                    self.logger.warning(f"[退出] 撤pending异常: {e}")

            for attempt in range(3):
                try:
                    await self._do_lighter_cancel_all()
                except Exception as e:
                    self.logger.warning(f"[退出] cancel_all 异常: {e}")
                cleared = await self._wait_orders_cleared(4.0)
                if cleared:
                    self.logger.info("[退出] 所有Lighter订单已确认取消")
                    break
                self.logger.warning(
                    f"[退出] 第{attempt+1}次 仍有"
                    f" {len(self._active_lighter_oidxs)} 个活跃订单, "
                    f"逐个撤单...")
                for oidx in list(self._active_lighter_oidxs):
                    try:
                        await self._cancel_lighter_order(oidx)
                    except Exception:
                        pass
                    await asyncio.sleep(0.2)
                await self._wait_orders_cleared(3.0)
                if not self._active_lighter_oidxs:
                    self.logger.info("[退出] 逐个撤单后全部确认取消")
                    break

            if self._active_lighter_oidxs:
                self.logger.error(
                    f"[退出] WS追踪仍有 {len(self._active_lighter_oidxs)} 个订单"
                    f"未确认取消: {self._active_lighter_oidxs}")

            if not self.dry_run:
                for rest_attempt in range(3):
                    try:
                        rest_orphans = await self._rest_reconcile_orders()
                        if rest_orphans == 0:
                            self.logger.info(
                                "[退出] REST对账确认: 无残留Lighter订单")
                            break
                        if rest_orphans and rest_orphans > 0:
                            self.logger.warning(
                                f"[退出] REST对账发现 {rest_orphans} 个残留订单, "
                                f"第{rest_attempt+1}次 cancel_all...")
                            try:
                                await self._do_lighter_cancel_all()
                            except Exception:
                                pass
                            await asyncio.sleep(2.0)
                    except Exception as e:
                        self.logger.warning(f"[退出] REST对账异常: {e}")
                        try:
                            await self._do_lighter_cancel_all()
                        except Exception:
                            pass
                        await asyncio.sleep(1.0)

            if not self.dry_run:
                try:
                    await self._cancel_all_extended_orders_rest()
                except Exception as e:
                    self.logger.warning(f"[退出] Extended REST兜底撤单异常: {e}")
            if not self.dry_run:
                try:
                    await self._reconcile_positions()
                except Exception as e:
                    self.logger.warning(f"[退出] 对账异常: {e}")
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
    #  Main loop
    # ═══════════════════════════════════════════════

    async def _main_loop_wrapper(self):
        ladder_desc = (f"阶梯加仓={self.ladder_step}bps/层 "
                       f"最多{int(self.max_position / self.order_size)}层"
                       if self.ladder_step > 0 else "阶梯=关")
        self.logger.info(
            f"启动 V10 Lighter-Maker开仓+双边Taker平仓 套利  {self.symbol}  "
            f"size={self.order_size} max_pos={self.max_position}  "
            f"目标价差≥{self.target_spread}bps  "
            f"平仓价差≤{self.close_gap_bps}bps  "
            f"调价间隔={self.reprice_interval}s  dry_run={self.dry_run}  "
            f"往返费用≈{self.ROUND_TRIP_FEE_BPS:.1f}bps  {ladder_desc}")

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
            await asyncio.sleep(2.0)
            try:
                startup_orphans = await self._rest_reconcile_orders()
                if startup_orphans and startup_orphans > 0:
                    self.logger.warning(
                        f"[启动] REST发现 {startup_orphans} 个残留订单, "
                        f"再次 cancel_all")
                    await self._do_lighter_cancel_all()
                    await asyncio.sleep(2.0)
                else:
                    self.logger.info("[启动] Lighter 订单已清理干净")
            except Exception as e:
                self.logger.warning(f"[启动] REST对账异常: {e}")
            await self._cancel_all_extended_orders_rest()
        else:
            self.logger.info("[DRY-RUN] 跳过交易客户端初始化")
            self._get_lighter_market_config()

        self.lighter_feed = LighterFeed(self.symbol)
        self.extended_feed = ExtendedFeed(self.symbol)
        asyncio.create_task(self.lighter_feed.connect())
        asyncio.create_task(self.extended_feed.connect())
        if not self.dry_run:
            self._lighter_ws_task = asyncio.create_task(self._lighter_account_ws_loop())
        self.logger.info("等待行情数据 (6s)...")
        await asyncio.sleep(6)

        ls = self.lighter_feed.snapshot
        x_bid, x_ask = self._get_extended_ws_bbo() if not self.dry_run else (None, None)
        es = self.extended_feed.snapshot
        if not ls.mid or (not es.mid and x_bid is None):
            self.logger.error("行情不可用")
            return
        x_mid_val = float((x_bid + x_ask) / 2) if x_bid and x_ask else es.mid
        self.logger.info(f"行情就绪: Lighter={ls.mid:.2f} Extended={x_mid_val:.2f}")

        if not self.dry_run:
            await self._reconcile_positions()
            if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                self.logger.warning(
                    f"启动检测到仓位: L={self.lighter_position} X={self.extended_position}")
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
                    if self.position_direction == "long_X_short_L":
                        _est_sp = float((l_est - x_est) / l_est * 10000)
                    else:
                        _est_sp = float((x_est - l_est) / l_est * 10000)
                    self._last_open_spread_bps = max(_est_sp, self.target_spread)
                    self.logger.info(
                        f"[启动] 使用当前价格估算: X_ref={x_est:.2f}x{x_qty} "
                        f"L_ref={l_est:.2f}x{l_qty} spread={self._last_open_spread_bps:.1f}bps")

        self.logger.info(f"进入主循环  状态={self.state.value}")
        last_reconcile = time.time()
        last_status_log = 0.0
        last_order_cleanup = time.time()
        last_rest_order_reconcile = time.time()

        while not self.stop_flag:
            try:
                ls = self.lighter_feed.snapshot
                now = time.time()

                x_ws_bid, x_ws_ask = (None, None)
                if not self.dry_run:
                    x_ws_bid, x_ws_ask = self._get_extended_ws_bbo()

                if x_ws_bid is not None and x_ws_ask is not None:
                    x_mid = (x_ws_bid + x_ws_ask) / 2
                else:
                    es = self.extended_feed.snapshot
                    if not es.mid:
                        await asyncio.sleep(self.interval)
                        continue
                    x_mid = Decimal(str(es.mid))

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
                x_age = (now - self.extended_client._ob_last_update_ts
                         if not self.dry_run and self.extended_client
                            and self.extended_client._ob_last_update_ts > 0 else 999)

                data_stale = l_age > 5.0 or x_age > 5.0

                if data_stale and not self.dry_run:
                    has_open = self._open_buy or self._open_sell
                    if (self._pending or has_open) and not self._maker_pulled:
                        self.logger.warning("[数据过期] 撤回所有 Lighter 挂单")
                        if has_open:
                            await self._cancel_open_makers()
                        if self._pending:
                            self._cancel_grace_until = time.time() + 2.0
                            if self._pending.lighter_order_index is not None:
                                await self._cancel_lighter_order(
                                    self._pending.lighter_order_index)
                            else:
                                await self._do_lighter_cancel_all()
                            ds_cleared = await self._wait_orders_cleared(2.0)
                            if ds_cleared:
                                if self._pending and self._pending.filled_qty <= self._pending.hedged_qty:
                                    self._pending = None
                            else:
                                self.logger.warning(
                                    "[数据过期] pending撤单未确认, 保留状态")
                        self._maker_pulled = True
                    if self.state not in (State.HEDGING, State.CLOSING_HEDGE):
                        await asyncio.sleep(self.interval)
                        continue
                else:
                    self._maker_pulled = False

                mid_spread_bps = float((x_mid - l_mid) / l_mid * 10000)
                cur_pos = abs(self.extended_position)

                if now - last_status_log > 15:
                    hold_s = ""
                    if self.position_opened_at:
                        hold_s = f" hold={now - self.position_opened_at:.0f}s"
                    pend = ""
                    if self._pending:
                        pend = f" maker={self._pending.side}@{self._pending.price}"
                    if self._open_buy:
                        pend += f" LB@{self._open_buy.price}"
                    if self._open_sell:
                        pend += f" LS@{self._open_sell.price}"
                    basis_info = ""
                    if (self.state == State.IN_POSITION
                            and x_ws_bid is not None and l_ask is not None):
                        if self.extended_position > 0:
                            _cur_gap = l_ask - x_ws_bid
                            _ref = x_ws_bid
                        else:
                            _cur_gap = (x_ws_ask - l_bid if x_ws_ask else Decimal("0"))
                            _ref = l_bid
                        _gap_bps = float(_cur_gap / _ref * 10000) if _ref > 0 else 999.0
                        _close_ok = _gap_bps <= self.close_gap_bps
                        _hold = now - self.position_opened_at if self.position_opened_at else 0
                        _open_sp_bps = 0.0
                        if self._open_x_refs and self._open_l_refs:
                            _x_avg = sum(p * q for p, q in self._open_x_refs) / sum(
                                q for _, q in self._open_x_refs)
                            _l_avg = sum(p * q for p, q in self._open_l_refs) / sum(
                                q for _, q in self._open_l_refs)
                            if self.extended_position > 0:
                                _open_sp_bps = float(
                                    (_l_avg - _x_avg) / _x_avg * 10000) if _x_avg > 0 else 0.0
                            else:
                                _open_sp_bps = float(
                                    (_x_avg - _l_avg) / _x_avg * 10000) if _x_avg > 0 else 0.0
                        basis_info = (
                            f" gap={_gap_bps:.1f}bps/≤{self.close_gap_bps}bps"
                            f"({'CLOSE' if _close_ok else 'HOLD'}"
                            f" open={_open_sp_bps:.1f}bps hold={_hold:.0f}s)")
                    ladder_info = ""
                    if (self.ladder_step > 0
                            and self.state == State.IN_POSITION
                            and self.position_direction):
                        _layers = max(1, int(cur_pos / self.order_size))
                        _max_layers = int(self.max_position / self.order_size)
                        _next = self._last_open_spread_bps + self.ladder_step
                        ladder_info = f"  层={_layers}/{_max_layers} 下层≥{_next:.1f}bps"
                    self.logger.info(
                        f"[{self.state.value}] mid={mid_spread_bps:+.2f}bps  "
                        f"X_pos={self.extended_position} L_pos={self.lighter_position}{hold_s}  "
                        f"L={l_bid}/{l_ask} X={x_ws_bid}/{x_ws_ask}{pend}{basis_info}{ladder_info}  "
                        f"鲜度:L={l_age:.1f}s X={x_age:.1f}s  "
                        f"累积={self.cumulative_net_bps:+.2f}bps")
                    last_status_log = now

                if self._force_reconcile and not self.dry_run:
                    if self.state in (State.HEDGING, State.CLOSING_HEDGE):
                        self.logger.warning(
                            "[强制对账] 延迟: 当前处于 %s, 等对冲完成后执行",
                            self.state.value)
                    else:
                        self._force_reconcile = False
                        self.logger.warning("[强制对账] 孤立成交触发")
                        await self._cancel_open_makers()
                        await self._reconcile_positions()
                        last_reconcile = now
                        cur_pos = abs(self.extended_position)
                        if abs(self.lighter_position + self.extended_position) > self.order_size * Decimal("0.5"):
                            self.logger.error("[强制对账] 不平衡, 紧急平仓")
                            await self._emergency_flatten()
                            self._reset_position_state()
                            self.state = State.QUOTING
                            last_reconcile = time.time() - 50
                            continue

                if (not self.dry_run and now - last_reconcile > 30
                        and self.state in (State.QUOTING, State.IN_POSITION)):
                    await self._reconcile_positions()
                    last_reconcile = now
                    cur_pos = abs(self.extended_position)

                if (not self.dry_run
                        and now - last_rest_order_reconcile > 30
                        and self.state in (State.QUOTING, State.IN_POSITION)):
                    last_rest_order_reconcile = now
                    await self._rest_reconcile_orders()

                # ━━━━━━ STATE: QUOTING (双边Lighter挂单) ━━━━━━
                if self.state == State.QUOTING:
                    if x_ws_bid is None or x_ws_ask is None:
                        await asyncio.sleep(self.interval)
                        continue

                    need_cancel_all = False
                    if now < self._loss_breaker_until:
                        need_cancel_all = True
                    cooldown_remaining = self._post_close_cooldown - (now - self._last_close_time)
                    if self._last_close_time > 0 and cooldown_remaining > 0:
                        need_cancel_all = True
                    if cur_pos >= self.max_position:
                        need_cancel_all = True

                    if need_cancel_all:
                        if self._open_buy or self._open_sell:
                            await self._cancel_open_makers()
                        if now < self._loss_breaker_until:
                            await asyncio.sleep(1.0)
                        elif cooldown_remaining > 0:
                            await asyncio.sleep(min(cooldown_remaining, 1.0))
                        else:
                            await asyncio.sleep(self.interval)
                        continue

                    if now - last_order_cleanup > 30:
                        last_order_cleanup = now
                        self._cancel_grace_until = time.time() + 3.0
                        await self._do_lighter_cancel_all()
                        cleared = await self._wait_orders_cleared(3.0)
                        if not cleared:
                            for oidx in list(self._active_lighter_oidxs):
                                await self._cancel_lighter_order(oidx)
                            cleared = await self._wait_orders_cleared(2.0)
                        if cleared:
                            if self._open_buy and self._open_buy.filled_qty == 0:
                                self._open_buy = None
                            if self._open_sell and self._open_sell.filled_qty == 0:
                                self._open_sell = None
                        else:
                            self.logger.warning(
                                "[定期清理] 订单未全部确认取消, "
                                "保留本地状态防止孤儿订单")
                        self.logger.info(
                            f"[定期清理] 完成, 残留活跃订单="
                            f"{len(self._active_lighter_oidxs)}")

                    as_threshold = max(self.target_spread * 0.5, 1.0)
                    for side_name, maker, x_ref_now in [
                        ("buy", self._open_buy, x_ws_bid),
                        ("sell", self._open_sell, x_ws_ask),
                    ]:
                        if (maker and maker.x_ref_at_place is not None
                                and maker.filled_qty == 0):
                            x_move = abs(x_ref_now - maker.x_ref_at_place)
                            x_move_bps = float(
                                x_move / maker.x_ref_at_place * 10000)
                            if x_move_bps > as_threshold:
                                self.logger.warning(
                                    f"[逆选保护] {side_name} Extended移动"
                                    f" {x_move_bps:.1f}bps, 撤单")
                                self._cancel_grace_until = time.time() + 2.0
                                await self._safe_cancel_lighter(maker)
                                as_cleared = await self._wait_orders_cleared(2.0)
                                if as_cleared:
                                    if side_name == "buy":
                                        if (self._open_buy
                                                and self._open_buy.filled_qty == 0):
                                            self._open_buy = None
                                    else:
                                        if (self._open_sell
                                                and self._open_sell.filled_qty == 0):
                                            self._open_sell = None
                                else:
                                    self.logger.warning(
                                        f"[逆选保护] {side_name} 撤单未确认, "
                                        f"保留本地状态")

                    buy_target, sell_target = self._calc_open_targets(
                        l_bid, l_ask, x_ws_bid, x_ws_ask)

                    if not self.dry_run:
                        if buy_target:
                            _, b_dir, b_price = buy_target
                            await self._update_open_side(
                                "buy", b_price, b_dir, x_ws_bid)
                        elif self._open_buy and self._open_buy.filled_qty == 0:
                            self._cancel_grace_until = time.time() + 2.0
                            await self._safe_cancel_lighter(self._open_buy)
                            b_cleared = await self._wait_orders_cleared(1.5)
                            if b_cleared:
                                if (self._open_buy
                                        and self._open_buy.filled_qty == 0):
                                    self._open_buy = None

                        if sell_target:
                            _, s_dir, s_price = sell_target
                            await self._update_open_side(
                                "sell", s_price, s_dir, x_ws_ask)
                        elif self._open_sell and self._open_sell.filled_qty == 0:
                            self._cancel_grace_until = time.time() + 2.0
                            await self._safe_cancel_lighter(self._open_sell)
                            s_cleared = await self._wait_orders_cleared(1.5)
                            if s_cleared:
                                if (self._open_sell
                                        and self._open_sell.filled_qty == 0):
                                    self._open_sell = None
                    elif buy_target or sell_target:
                        if now - last_status_log > 10:
                            parts = []
                            if buy_target:
                                parts.append(f"LB@{buy_target[2]}")
                            if sell_target:
                                parts.append(f"LS@{sell_target[2]}")
                            self.logger.info(
                                f"[DRY-RUN] {' '.join(parts)}")

                # ━━━━━━ STATE: HEDGING (Extended IOC 对冲) ━━━━━━
                elif self.state == State.HEDGING:
                    if self._hedge_lock.locked():
                        await asyncio.sleep(0.01)
                        continue
                    async with self._hedge_lock:
                        if not (self._pending and self._pending.hedge_qty_needed > 0):
                            continue
                        hedge_qty = self._pending.hedge_qty_needed
                        direction = self._pending.direction

                        if direction == "long_L_short_X":
                            hedge_side = "sell"
                            x_ref = x_ws_bid if x_ws_bid else Decimal("0")
                        else:
                            hedge_side = "buy"
                            x_ref = x_ws_ask if x_ws_ask else Decimal("0")

                        self.logger.info(
                            f"[HEDGE] Extended IOC {hedge_side} {hedge_qty} ref={x_ref:.2f}")

                        t0 = time.time()
                        actual_fill = await self._place_extended_ioc(
                            hedge_side, hedge_qty, x_ref)
                        hedge_ms = (time.time() - t0) * 1000

                        if actual_fill is not None and actual_fill > 0:
                            self._pending.hedged_qty += actual_fill
                            self._pending.hedge_qty_needed -= actual_fill
                            l_fill_price = self._pending.avg_price or self._pending.price
                            x_actual = self._x_ioc_fill_price or x_ref
                            self._open_l_refs.append((l_fill_price, actual_fill))
                            self._open_x_refs.append((x_actual, actual_fill))

                            if direction == "long_L_short_X":
                                self.lighter_position += actual_fill
                                self.extended_position -= actual_fill
                            else:
                                self.lighter_position -= actual_fill
                                self.extended_position += actual_fill

                            self.logger.info(
                                f"[HEDGE OK] {hedge_ms:.0f}ms fill={actual_fill}/{hedge_qty} "
                                f"L={l_fill_price:.2f} X={x_actual:.2f} "
                                f"L_pos={self.lighter_position} X_pos={self.extended_position}")

                            if self._pending.hedged_qty >= self._pending.filled_qty:
                                if self._pending.filled_qty < self._pending.qty_target:
                                    self.logger.info("[HEDGE OK] 部分成交已对冲, 撤销剩余Lighter挂单")
                                    self._cancel_grace_until = time.time() + 2.0
                                    await self._safe_cancel_lighter(self._pending)
                                if self.position_opened_at is None:
                                    self.position_opened_at = time.time()
                                self.position_direction = direction
                                if direction == "long_X_short_L":
                                    _actual_sp = float(
                                        (l_fill_price - x_actual) / l_fill_price * 10000
                                    ) if l_fill_price > 0 else 0.0
                                else:
                                    _actual_sp = float(
                                        (x_actual - l_fill_price) / l_fill_price * 10000
                                    ) if l_fill_price > 0 else 0.0
                                self._last_open_spread_bps = max(
                                    _actual_sp, self.target_spread)
                                self._csv_row([
                                    datetime.now(timezone.utc).isoformat(),
                                    "OPEN", direction,
                                    self._pending.side, f"{l_fill_price:.2f}",
                                    str(actual_fill),
                                    hedge_side, f"{x_actual:.2f}", str(actual_fill),
                                    f"{mid_spread_bps:.2f}", f"{hedge_ms:.0f}", "", "",
                                ])
                                self._pending = None
                                self.state = State.IN_POSITION
                        else:
                            self.logger.error("[HEDGE FAIL] Extended 对冲失败, 紧急反向平 Lighter")
                            l_side = self._pending.side
                            undo_side = "sell" if l_side == "buy" else "buy"
                            l_ref = Decimal(str(
                                (ls.best_bid or ls.mid or 0) if undo_side == "sell"
                                else (ls.best_ask or ls.mid or 0)))
                            undo_ok = False
                            if l_ref > 0:
                                undo_fill = await self._place_lighter_taker(
                                    undo_side, hedge_qty, l_ref)
                                undo_ok = undo_fill is not None and undo_fill > 0
                            if not undo_ok:
                                self.logger.error("[HEDGE FAIL] undo 失败, 触发强制对账")
                                self._force_reconcile = True
                            self._pending = None
                            await self._do_lighter_cancel_all()
                            remaining = abs(self.extended_position)
                            if remaining >= self.order_size * Decimal("0.01"):
                                if self.position_opened_at is None:
                                    self.position_opened_at = time.time()
                                if self.position_direction is None:
                                    self.position_direction = direction
                                self.state = State.IN_POSITION
                            else:
                                self._reset_position_state()
                                self.state = State.QUOTING
                            last_reconcile = time.time() - 50

                # ━━━━━━ STATE: IN_POSITION (Lighter maker 平仓) ━━━━━━
                elif self.state == State.IN_POSITION:
                    if (abs(self.extended_position) < self.order_size * Decimal("0.01")
                            and abs(self.lighter_position) < self.order_size * Decimal("0.01")):
                        self.logger.info("[IN_POSITION] 仓位已清零, 回到 QUOTING")
                        self._pending = None
                        self._reset_position_state()
                        self.state = State.QUOTING
                        continue

                    if (self.position_opened_at
                            and now - self.position_opened_at > self.max_hold_sec):
                        self.logger.warning(
                            f"[超时强平] 持仓 {now - self.position_opened_at:.0f}s "
                            f"> {self.max_hold_sec}s")
                        await self._emergency_flatten()
                        self._pending = None
                        self._reset_position_state()
                        self.state = State.QUOTING
                        last_reconcile = time.time() - 50
                        continue

                    x_zero = abs(self.extended_position) < self.order_size * Decimal("0.01")
                    l_zero = abs(self.lighter_position) < self.order_size * Decimal("0.01")
                    if x_zero != l_zero and not self.dry_run:
                        self.logger.warning(
                            f"[单边仓位?] 本地: X={self.extended_position} "
                            f"L={self.lighter_position}, 先对账确认...")
                        await self._reconcile_positions()
                        x_zero2 = abs(self.extended_position) < self.order_size * Decimal("0.01")
                        l_zero2 = abs(self.lighter_position) < self.order_size * Decimal("0.01")
                        if x_zero2 != l_zero2:
                            self.logger.error(
                                f"[单边仓位] 对账确认不平衡: "
                                f"X={self.extended_position} L={self.lighter_position}")
                            await self._emergency_flatten()
                            self._pending = None
                            self._reset_position_state()
                            self.state = State.QUOTING
                            last_reconcile = time.time() - 50
                            continue
                        else:
                            self.logger.info("[单边仓位] 对账后恢复正常")
                            last_reconcile = now

                    if x_ws_bid is not None and x_ws_ask is not None and not self.dry_run:
                        should_add_layer = False
                        current_dir_spread = 0.0
                        if (self.ladder_step > 0
                                and cur_pos < self.max_position - self.order_size * Decimal("0.01")
                                and self.position_direction
                                and not data_stale):
                            hold_elapsed = (now - self.position_opened_at
                                            if self.position_opened_at else 0)
                            if hold_elapsed <= self.max_hold_sec * 0.7:
                                if self.position_direction == "long_X_short_L":
                                    current_dir_spread = float(
                                        (l_mid - x_mid) / l_mid * 10000) if l_mid > 0 else 0.0
                                else:
                                    current_dir_spread = float(
                                        (x_mid - l_mid) / l_mid * 10000) if l_mid > 0 else 0.0
                                if current_dir_spread > self._last_open_spread_bps + self.ladder_step:
                                    should_add_layer = True

                        if should_add_layer and now - self._last_ladder_attempt < 3.0:
                            should_add_layer = False
                        if should_add_layer:
                            self._last_ladder_attempt = now
                            if self._pending is None or self._pending.phase == "open":
                                max_layers = int(self.max_position / self.order_size)
                                cur_layers = int(cur_pos / self.order_size) + 1
                                spread_d = Decimal(str(self.target_spread))
                                is_new_add = self._pending is None
                                if self.position_direction == "long_X_short_L":
                                    add_price = self._round_lighter_price(
                                        x_ws_ask * (Decimal("1") + spread_d / Decimal("10000")))
                                    add_price = max(add_price, l_bid + self.lighter_tick)
                                    if add_price > l_bid:
                                        if is_new_add:
                                            self.logger.info(
                                                f"[阶梯] 加仓 {cur_layers}/{max_layers} "
                                                f"spread={current_dir_spread:.1f}bps")
                                        await self._update_maker(
                                            "sell", "long_X_short_L",
                                            add_price, self.order_size, "open", x_ws_ask)
                                else:
                                    add_price = self._round_lighter_price(
                                        x_ws_bid * (Decimal("1") - spread_d / Decimal("10000")))
                                    add_price = min(add_price, l_ask - self.lighter_tick)
                                    if add_price > 0 and add_price < l_ask:
                                        if is_new_add:
                                            self.logger.info(
                                                f"[阶梯] 加仓 {cur_layers}/{max_layers} "
                                                f"spread={current_dir_spread:.1f}bps")
                                        await self._update_maker(
                                            "buy", "long_L_short_X",
                                            add_price, self.order_size, "open", x_ws_bid)
                        else:
                            if (self._pending and self._pending.phase == "open"
                                    and self._pending.filled_qty <= self._pending.hedged_qty):
                                self.logger.info("[阶梯] 价差收窄, 取消加仓 maker 转为平仓")
                                self._cancel_grace_until = time.time() + 2.0
                                if self._pending.lighter_order_index is not None:
                                    await self._cancel_lighter_order(
                                        self._pending.lighter_order_index)
                                else:
                                    self.logger.warning(
                                        "[阶梯] order_index 未知, cancel_all 兜底")
                                    await self._do_lighter_cancel_all()
                                ladder_cleared = await self._wait_orders_cleared(2.0)
                                if ladder_cleared:
                                    if (self._pending
                                            and self._pending.filled_qty
                                            <= self._pending.hedged_qty):
                                        self._pending = None
                                else:
                                    self.logger.warning(
                                        "[阶梯] 撤单未确认, 保留pending状态")

                            if self._pending is None:
                                if self._should_close_now(
                                        l_bid, l_ask, x_ws_bid, x_ws_ask):
                                    close_qty = abs(self.lighter_position)
                                    if close_qty > 0:
                                        if self.position_direction == "long_X_short_L":
                                            close_l_side = "buy"
                                            close_x_side = "sell"
                                            l_ref = l_ask if l_ask > 0 else l_mid
                                            x_ref = x_ws_bid
                                        else:
                                            close_l_side = "sell"
                                            close_x_side = "buy"
                                            l_ref = l_bid if l_bid > 0 else l_mid
                                            x_ref = x_ws_ask

                                        self.logger.info(
                                            f"[平仓] 双边市价: Lighter IOC {close_l_side} "
                                            f"{close_qty} @ {l_ref}, Extended IOC "
                                            f"{close_x_side} {close_qty} @ {x_ref}")

                                        t0 = time.time()
                                        l_fill = await self._place_lighter_taker(
                                            close_l_side, close_qty, l_ref)
                                        l_ms = (time.time() - t0) * 1000

                                        if l_fill is not None and l_fill > 0:
                                            if close_l_side == "buy":
                                                self.lighter_position += l_fill
                                            else:
                                                self.lighter_position -= l_fill

                                            l_fill_price = (
                                                self._last_lighter_avg_price or l_ref)

                                            t1 = time.time()
                                            x_fill = await self._place_extended_ioc(
                                                close_x_side, l_fill, x_ref)
                                            x_ms = (time.time() - t1) * 1000

                                            if x_fill is not None and x_fill > 0:
                                                if close_x_side == "sell":
                                                    self.extended_position -= x_fill
                                                else:
                                                    self.extended_position += x_fill
                                                x_actual = (
                                                    self._x_ioc_fill_price or x_ref)

                                                net_bps = self._log_ref_pnl(
                                                    x_actual, l_fill_price, l_fill)
                                                self.cumulative_net_bps += net_bps
                                                self.logger.info(
                                                    f"[平仓完成] L={l_ms:.0f}ms "
                                                    f"X={x_ms:.0f}ms "
                                                    f"净利={net_bps:+.2f}bps "
                                                    f"累积="
                                                    f"{self.cumulative_net_bps:+.2f}bps")
                                                self._csv_row([
                                                    datetime.now(timezone.utc).isoformat(),
                                                    "CLOSE", "convergence",
                                                    close_l_side,
                                                    f"{l_fill_price:.2f}",
                                                    str(l_fill),
                                                    close_x_side,
                                                    f"{x_actual:.2f}",
                                                    str(x_fill),
                                                    f"{mid_spread_bps:.2f}",
                                                    f"{l_ms + x_ms:.0f}",
                                                    f"{net_bps:.2f}",
                                                    f"{self.cumulative_net_bps:.2f}",
                                                ])
                                            else:
                                                self.logger.error(
                                                    "[平仓] Extended IOC 失败, "
                                                    "触发对账")
                                                self._force_reconcile = True

                                            pos_remaining = abs(
                                                self.extended_position)
                                            if pos_remaining < (
                                                    self.order_size
                                                    * Decimal("0.01")):
                                                self._last_close_time = time.time()
                                                if net_bps < 0:
                                                    self._consecutive_losses += 1
                                                else:
                                                    self._consecutive_losses = 0
                                                if (self._consecutive_losses >= self._loss_streak_limit
                                                        or self.cumulative_net_bps
                                                        < self._loss_breaker_threshold):
                                                    self._loss_breaker_until = (
                                                        time.time()
                                                        + self._loss_breaker_duration)
                                                    self.logger.error(
                                                        f"[熔断] 连亏="
                                                        f"{self._consecutive_losses} "
                                                        f"累积="
                                                        f"{self.cumulative_net_bps:+.2f}"
                                                        f"bps")
                                                self._reset_position_state()
                                                self.state = State.QUOTING
                                            else:
                                                self._normalize_refs(
                                                    pos_remaining)
                                                self.state = State.IN_POSITION
                                            last_reconcile = time.time() - 50
                                        else:
                                            self.logger.warning(
                                                "[平仓] Lighter IOC 未成交, "
                                                "下次循环重试")

                # ━━━━━━ STATE: CLOSING_HEDGE (Extended IOC 平仓对冲) ━━━━━━
                elif self.state == State.CLOSING_HEDGE:
                    if self._hedge_lock.locked():
                        await asyncio.sleep(0.01)
                        continue
                    async with self._hedge_lock:
                        if not (self._pending and self._pending.hedge_qty_needed > 0):
                            continue
                        hedge_qty = self._pending.hedge_qty_needed
                        l_side = self._pending.side
                        hedge_side = "sell" if l_side == "buy" else "buy"

                        x_ref = (x_ws_bid if hedge_side == "sell" else x_ws_ask) or Decimal("0")

                        self.logger.info(
                            f"[CLOSE_HEDGE] Extended IOC {hedge_side} {hedge_qty}")

                        t0 = time.time()
                        actual_fill = await self._place_extended_ioc(
                            hedge_side, hedge_qty, x_ref)
                        hedge_ms = (time.time() - t0) * 1000

                        if actual_fill is not None and actual_fill > 0:
                            self._pending.hedged_qty += actual_fill
                            self._pending.hedge_qty_needed -= actual_fill
                            l_fill_price = self._pending.avg_price or self._pending.price
                            x_actual = self._x_ioc_fill_price or x_ref

                            if l_side == "buy":
                                self.lighter_position += actual_fill
                            else:
                                self.lighter_position -= actual_fill
                            if hedge_side == "sell":
                                self.extended_position -= actual_fill
                            else:
                                self.extended_position += actual_fill

                            net_bps = self._log_ref_pnl(x_actual, l_fill_price, actual_fill)
                            self.cumulative_net_bps += net_bps
                            self.logger.info(
                                f"[平仓完成] {hedge_ms:.0f}ms 净利={net_bps:+.2f}bps "
                                f"累积={self.cumulative_net_bps:+.2f}bps")
                            self._csv_row([
                                datetime.now(timezone.utc).isoformat(),
                                "CLOSE", "convergence",
                                self._pending.side, f"{l_fill_price:.2f}",
                                str(actual_fill),
                                hedge_side, f"{x_actual:.2f}", str(actual_fill),
                                f"{mid_spread_bps:.2f}", f"{hedge_ms:.0f}",
                                f"{net_bps:.2f}", f"{self.cumulative_net_bps:.2f}",
                            ])

                            if self._pending.hedged_qty >= self._pending.filled_qty:
                                if self._pending.filled_qty < self._pending.qty_target:
                                    self.logger.info(
                                        "[CLOSE_HEDGE] 部分成交已对冲, 撤销剩余Lighter挂单")
                                    self._cancel_grace_until = time.time() + 2.0
                                    await self._safe_cancel_lighter(self._pending)
                                self._pending = None
                                pos_remaining = abs(self.extended_position)
                                if pos_remaining < self.order_size * Decimal("0.01"):
                                    self._last_close_time = time.time()
                                    if net_bps < 0:
                                        self._consecutive_losses += 1
                                    else:
                                        self._consecutive_losses = 0
                                    if (self._consecutive_losses >= self._loss_streak_limit
                                            or self.cumulative_net_bps < self._loss_breaker_threshold):
                                        self._loss_breaker_until = (
                                            time.time() + self._loss_breaker_duration)
                                        self.logger.error(
                                            f"[熔断] 连亏={self._consecutive_losses} "
                                            f"累积={self.cumulative_net_bps:+.2f}bps")
                                    self._reset_position_state()
                                    self.state = State.QUOTING
                                else:
                                    self._normalize_refs(pos_remaining)
                                    self.state = State.IN_POSITION
                                last_reconcile = time.time() - 50
                        else:
                            self.logger.error("[CLOSE_HEDGE FAIL] 紧急平仓")
                            await self._emergency_flatten()
                            self._pending = None
                            self._reset_position_state()
                            self.state = State.QUOTING
                            last_reconcile = time.time() - 50

                if self.state in (State.QUOTING, State.IN_POSITION):
                    try:
                        self._fill_event.clear()
                        await asyncio.wait_for(
                            self._fill_event.wait(), timeout=self.interval)
                    except asyncio.TimeoutError:
                        pass
                else:
                    await asyncio.sleep(0.05)

            except Exception as e:
                self.logger.error(f"主循环异常: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(2)

    # ═══════════════════════════════════════════════
    #  P&L / Reset
    # ═══════════════════════════════════════════════

    def _log_ref_pnl(self, close_x_ref: Decimal, close_l_ref: Decimal,
                     close_qty: Optional[Decimal] = None) -> float:
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

        total_gross = x_pnl + l_pnl
        notional = float(qty) * float(close_x_ref)
        fee_cost = notional * self.ROUND_TRIP_FEE_BPS / 10000
        total_net = total_gross - fee_cost
        gross_bps = total_gross / notional * 10000 if notional > 0 else 0.0
        net_bps = total_net / notional * 10000 if notional > 0 else 0.0
        self.logger.info(
            f"[P&L] qty={qty} X={x_pnl:+.3f}$ L={l_pnl:+.3f}$ "
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
        self._pending = None
        self._open_buy = None
        self._open_sell = None
        self._last_open_spread_bps = 0.0


def main():
    p = argparse.ArgumentParser(
        description="V9 Lighter-Maker+Extended-Taker 套利 (角色互换)")
    p.add_argument("-s", "--symbol", default="ETH")
    p.add_argument("--size", type=str, default="0.1")
    p.add_argument("--max-position", type=str, default="0.5")
    p.add_argument("--target-spread", type=float, default=6.0,
                   help="开仓最低利润目标 (bps, 需覆盖5bps往返费用)")
    p.add_argument("--close-gap-bps", type=float, default=1.0,
                   help="平仓阈值: 价差收敛到≤此值(bps)即平仓")
    p.add_argument("--max-hold-sec", type=float, default=1800.0)
    p.add_argument("--cooldown", type=float, default=15.0)
    p.add_argument("--loss-breaker-bps", type=float, default=-15.0)
    p.add_argument("--loss-breaker-sec", type=float, default=300.0)
    p.add_argument("--loss-streak", type=int, default=3,
                   help="连续亏损多少次触发熔断 (默认3)")
    p.add_argument("--interval", type=float, default=0.1)
    p.add_argument("--reprice-interval", type=float, default=1.0,
                   help="最小调价间隔 (秒, Lighter下单较快)")
    p.add_argument("--ladder-step", type=float, default=0.0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--env-file", default=".env")
    args = p.parse_args()

    load_dotenv(args.env_file)

    if args.target_spread < 0:
        print(f"[警告] target_spread={args.target_spread} 为负数，已修正为 0")
        args.target_spread = 0

    bot = ExtMakerBot(
        symbol=args.symbol.upper(),
        order_size=Decimal(args.size),
        max_position=Decimal(args.max_position),
        target_spread=args.target_spread,
        close_gap_bps=args.close_gap_bps,
        max_hold_sec=args.max_hold_sec,
        interval=args.interval,
        reprice_interval=args.reprice_interval,
        dry_run=args.dry_run,
        ladder_step=args.ladder_step,
    )
    bot._post_close_cooldown = args.cooldown
    bot._loss_breaker_threshold = args.loss_breaker_bps
    bot._loss_breaker_duration = args.loss_breaker_sec
    bot._loss_streak_limit = args.loss_streak

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
