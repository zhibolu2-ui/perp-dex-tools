#!/usr/bin/env python3
"""
Extended + Lighter 价差收敛套利 V11（双边全市价 Taker-Taker）

策略：
  开仓: 检测价差(taker执行价) > target_spread →
        Lighter IOC 市价 + Extended IOC 市价 对向开仓
  平仓: 检测价差收敛(taker执行价) ≤ close_gap →
        Lighter IOC 市价 + Extended IOC 市价 平仓
  阶梯: 持仓中价差继续扩大 → 同方向追加仓位

费用: ~5.0 bps 往返 (Extended taker 2.5bps×2, Lighter taker 0% Premium)

与 V10 的区别：
  V10: 开仓使用 Lighter POST_ONLY maker 挂单, 存在挂单管理/孤儿订单问题
  V11: 开仓平仓全部 taker 市价, 无挂单, 无孤儿订单, 极简逻辑

用法:
  python extended_lighter_arb_v11.py --symbol BTC --size 0.002 --target-spread 6
  python extended_lighter_arb_v11.py --symbol ETH --dry-run
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
    IDLE = "IDLE"
    IN_POSITION = "IN_POSITION"


class _Config:
    def __init__(self, d: dict):
        for k, v in d.items():
            setattr(self, k, v)


class TakerBot:

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
        self.dry_run = dry_run
        self.ladder_step = ladder_step

        self.state = State.IDLE
        self.stop_flag = False

        self.lighter_position = Decimal("0")
        self.extended_position = Decimal("0")
        self.position_opened_at: Optional[float] = None
        self.position_direction: Optional[str] = None

        self._open_x_refs: List[Tuple[Decimal, Decimal]] = []
        self._open_l_refs: List[Tuple[Decimal, Decimal]] = []
        self._last_open_spread_bps: float = 0.0
        self._last_ladder_attempt: float = 0.0

        self._x_ioc_pending_id: Optional[str] = None
        self._x_ioc_confirmed = asyncio.Event()
        self._x_ioc_fill_qty: Decimal = Decimal("0")
        self._x_ioc_fill_price: Optional[Decimal] = None
        self._x_ioc_recent_ids: List[str] = []

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


        self._force_reconcile: bool = False
        self._trade_lock = asyncio.Lock()
        self._last_trade_time: float = 0.0
        self._min_hold_sec: float = 5.0
        self._ladder_cooldown: float = 10.0
        self._max_slippage_bps: float = 5.0
        self._aio_session: Optional[aiohttp.ClientSession] = None
        self._bbo_event = asyncio.Event()

        os.makedirs("logs", exist_ok=True)
        self.csv_path = f"logs/arb_v11_{self.symbol}_trades.csv"
        self.log_path = f"logs/arb_v11_{self.symbol}.log"
        self._init_csv()
        self.logger = self._init_logger()

    # ═══════════════════════════════════════════════
    #  Logging / CSV
    # ═══════════════════════════════════════════════

    def _init_logger(self) -> logging.Logger:
        lg = logging.getLogger(f"arb_v11_{self.symbol}")
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
                    "spread_bps", "total_ms", "net_bps", "cumulative_net_bps",
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

    def _on_bbo_update(self):
        self._bbo_event.set()

    # ═══════════════════════════════════════════════
    #  Extended WS Event (IOC fill tracking)
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
                    f"filled={filled}  触发立即对账")
                self._force_reconcile = True

    # ═══════════════════════════════════════════════
    #  Lighter Taker (IOC 市价)
    # ═══════════════════════════════════════════════

    def _round_lighter_price(self, price: Decimal) -> Decimal:
        if self.lighter_tick > 0:
            return (price / self.lighter_tick).quantize(Decimal("1")) * self.lighter_tick
        return price

    def _check_tx_rate(self) -> bool:
        now = time.time()
        if now < self._tx_backoff_until:
            return False
        cutoff = now - 60.0
        self._tx_timestamps = [t for t in self._tx_timestamps if t > cutoff]
        return len(self._tx_timestamps) < 38

    def _record_tx(self):
        self._tx_timestamps.append(time.time())

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
    #  Extended IOC
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
                elif self._x_ioc_confirmed.is_set():
                    self.logger.warning("[Extended] IOC 已确认但 fill=0")
                    self._x_ioc_recent_ids.append(self._x_ioc_pending_id)
                    if len(self._x_ioc_recent_ids) > 20:
                        self._x_ioc_recent_ids = self._x_ioc_recent_ids[-10:]
                    self._x_ioc_pending_id = None
                    return Decimal("0")
                else:
                    self.logger.warning(
                        "[Extended] IOC WS超时, REST验证中...")
                    try:
                        rest_pos = await self._get_extended_position()
                        if side == "buy":
                            delta = rest_pos - pre_x_pos
                        else:
                            delta = pre_x_pos - rest_pos
                        if delta >= qty_r * Decimal("0.5"):
                            actual = abs(delta)
                            self.logger.info(
                                f"[Extended] REST确认成交 Δ={actual}")
                        else:
                            self.logger.warning(
                                "[Extended] REST未确认成交, 视为未成交")
                            self._x_ioc_recent_ids.append(
                                self._x_ioc_pending_id)
                            if len(self._x_ioc_recent_ids) > 20:
                                self._x_ioc_recent_ids = (
                                    self._x_ioc_recent_ids[-10:])
                            self._x_ioc_pending_id = None
                            return None
                    except Exception as ve:
                        self.logger.error(
                            f"[Extended] REST验证失败: {ve}, "
                            f"安全起见视为未成交")
                        self._x_ioc_recent_ids.append(
                            self._x_ioc_pending_id)
                        if len(self._x_ioc_recent_ids) > 20:
                            self._x_ioc_recent_ids = (
                                self._x_ioc_recent_ids[-10:])
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
    #  Lighter WS (taker fill confirmation only)
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
                                        self.logger.debug(
                                            f"[Lighter WS] 忽略非当前订单 "
                                            f"coi={ws_coi} 期望="
                                            f"{self._lighter_pending_coi}")
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
    #  Cancel helpers (cleanup only — no active orders in V11)
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
        """Check if spread is wide enough to open. Uses taker execution prices.
        Returns (direction, l_side, x_side, l_ref, x_ref, spread_bps) or None.
        """
        gap_sell_l = l_bid - x_ask
        ref1 = (l_bid + x_ask) / 2
        gap1_bps = float(gap_sell_l / ref1 * 10000) if ref1 > 0 else 0.0

        gap_buy_l = x_bid - l_ask
        ref2 = (x_bid + l_ask) / 2
        gap2_bps = float(gap_buy_l / ref2 * 10000) if ref2 > 0 else 0.0

        if gap1_bps >= self.target_spread and gap1_bps >= gap2_bps:
            return ("long_X_short_L", "sell", "buy", l_bid, x_ask, gap1_bps)
        elif gap2_bps >= self.target_spread:
            return ("long_L_short_X", "buy", "sell", l_ask, x_bid, gap2_bps)
        return None

    def _check_close_condition(
        self, l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
    ) -> bool:
        """Check if spread has converged enough to close."""
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
    #  Open / Close Position (双边 Taker)
    # ═══════════════════════════════════════════════

    async def _open_position(
        self, direction: str, l_side: str, x_side: str,
        l_ref: Decimal, x_ref: Decimal, qty: Decimal,
        spread_bps: float,
    ) -> bool:
        """Open position using parallel taker IOC on both exchanges."""
        async with self._trade_lock:
            ls2 = self.lighter_feed.snapshot
            x2_bid, x2_ask = self._get_extended_ws_bbo()
            if (ls2.best_bid is not None and ls2.best_ask is not None
                    and x2_bid is not None and x2_ask is not None):
                l2_bid = Decimal(str(ls2.best_bid))
                l2_ask = Decimal(str(ls2.best_ask))
                recheck = self._check_open_opportunity(
                    l2_bid, l2_ask, x2_bid, x2_ask)
                if recheck is None:
                    self.logger.info(
                        "[开仓] 二次验证价差已消失, 放弃")
                    return False
                _, _, _, l_ref, x_ref, spread_bps = recheck

            t0 = time.time()

            l_fill, x_fill = await asyncio.gather(
                self._place_lighter_taker(l_side, qty, l_ref),
                self._place_extended_ioc(x_side, qty, x_ref),
            )
            total_ms = (time.time() - t0) * 1000

            l_ok = l_fill is not None and l_fill > 0
            x_ok = x_fill is not None and x_fill > 0

            if l_ok and x_ok:
                l_fill_price = self._last_lighter_avg_price or l_ref
                x_actual = self._x_ioc_fill_price or x_ref

                if direction == "long_L_short_X":
                    self.extended_position -= x_fill
                else:
                    self.extended_position += x_fill

                self._open_l_refs.append((l_fill_price, l_fill))
                self._open_x_refs.append((x_actual, x_fill))

                if self.position_opened_at is None:
                    self.position_opened_at = time.time()
                self.position_direction = direction
                self._last_open_spread_bps = max(spread_bps, self.target_spread)

                if direction == "long_X_short_L":
                    actual_sp = float(
                        (l_fill_price - x_actual) / l_fill_price * 10000
                    ) if l_fill_price > 0 else 0.0
                else:
                    actual_sp = float(
                        (x_actual - l_fill_price) / l_fill_price * 10000
                    ) if l_fill_price > 0 else 0.0
                sp_q = ("OK" if actual_sp >= self.ROUND_TRIP_FEE_BPS
                        else "LOW" if actual_sp > 0 else "NEG")

                self.logger.info(
                    f"[开仓成功] {total_ms:.0f}ms(并行) "
                    f"实际价差={actual_sp:.1f}bps({sp_q}) "
                    f"L_{l_side}={l_fill_price:.2f}x{l_fill} "
                    f"X_{x_side}={x_actual:.2f}x{x_fill} "
                    f"L_pos={self.lighter_position} "
                    f"X_pos={self.extended_position}")

                mid_sp = float(
                    (x_actual - l_fill_price) / l_fill_price * 10000
                ) if l_fill_price > 0 else 0.0
                self._csv_row([
                    datetime.now(timezone.utc).isoformat(),
                    "OPEN", direction,
                    l_side, f"{l_fill_price:.2f}", str(l_fill),
                    x_side, f"{x_actual:.2f}", str(x_fill),
                    f"{mid_sp:.2f}", f"{total_ms:.0f}", "", "",
                ])
                self._last_trade_time = time.time()
                self._last_ladder_attempt = time.time()

                fill_diff = abs(l_fill - x_fill)
                if fill_diff > qty * Decimal("0.1"):
                    self.logger.warning(
                        f"[开仓] 双边成交量偏差! L={l_fill} X={x_fill} "
                        f"Δ={fill_diff}, 触发对账")
                    self._force_reconcile = True

                return True

            elif l_ok and not x_ok:
                self.logger.error(
                    f"[开仓] Extended未成交, 回撤Lighter "
                    f"{l_side}→{l_fill} ({total_ms:.0f}ms)")
                undo_side = "sell" if l_side == "buy" else "buy"
                undo_fill = await self._place_lighter_taker(
                    undo_side, l_fill, l_ref)
                if not (undo_fill and undo_fill > 0):
                    self.logger.error("[开仓] Lighter 回撤失败! 触发对账")
                    self._force_reconcile = True
                return False

            elif not l_ok and x_ok:
                self.logger.error(
                    f"[开仓] Lighter未成交, 回撤Extended "
                    f"{x_side}→{x_fill} ({total_ms:.0f}ms)")
                undo_x_side = "sell" if x_side == "buy" else "buy"
                x_bid_now, x_ask_now = self._get_extended_ws_bbo()
                undo_x_ref = (x_bid_now if undo_x_side == "sell"
                              else x_ask_now) or x_ref
                undo_fill = await self._place_extended_ioc(
                    undo_x_side, x_fill, undo_x_ref)
                if not (undo_fill and undo_fill > 0):
                    self.logger.error("[开仓] Extended 回撤失败! 触发对账")
                    self._force_reconcile = True
                return False

            else:
                self.logger.warning(
                    f"[开仓] 双边均未成交({total_ms:.0f}ms), 放弃")
                return False

    async def _close_position(
        self, l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
    ) -> bool:
        """Close position using parallel taker IOC on both exchanges."""
        async with self._trade_lock:
            close_qty = min(abs(self.lighter_position), abs(self.extended_position))
            if close_qty < self.order_size * Decimal("0.01"):
                return False

            if self.position_direction == "long_X_short_L":
                close_l_side = "buy"
                close_x_side = "sell"
                l_ref = l_ask if l_ask > 0 else (l_bid + l_ask) / 2
                x_ref = x_bid
            else:
                close_l_side = "sell"
                close_x_side = "buy"
                l_ref = l_bid if l_bid > 0 else (l_bid + l_ask) / 2
                x_ref = x_ask

            self.logger.info(
                f"[平仓] 并行: Lighter IOC {close_l_side} {close_qty} @ {l_ref}, "
                f"Extended IOC {close_x_side} {close_qty} @ {x_ref}")

            t0 = time.time()
            l_fill, x_fill = await asyncio.gather(
                self._place_lighter_taker(close_l_side, close_qty, l_ref),
                self._place_extended_ioc(close_x_side, close_qty, x_ref),
            )
            total_ms = (time.time() - t0) * 1000

            l_ok = l_fill is not None and l_fill > 0
            x_ok = x_fill is not None and x_fill > 0

            if l_ok and x_ok:
                l_fill_price = self._last_lighter_avg_price or l_ref
                x_actual = self._x_ioc_fill_price or x_ref

                if close_x_side == "sell":
                    self.extended_position -= x_fill
                else:
                    self.extended_position += x_fill

                net_bps = self._log_ref_pnl(x_actual, l_fill_price, l_fill)
                self.cumulative_net_bps += net_bps

                self.logger.info(
                    f"[平仓完成] {total_ms:.0f}ms(并行) "
                    f"净利={net_bps:+.2f}bps "
                    f"累积={self.cumulative_net_bps:+.2f}bps")

                mid_sp = float(
                    (x_actual - l_fill_price) / l_fill_price * 10000
                ) if l_fill_price > 0 else 0.0
                self._csv_row([
                    datetime.now(timezone.utc).isoformat(),
                    "CLOSE", "convergence",
                    close_l_side, f"{l_fill_price:.2f}", str(l_fill),
                    close_x_side, f"{x_actual:.2f}", str(x_fill),
                    f"{mid_sp:.2f}", f"{total_ms:.0f}",
                    f"{net_bps:.2f}", f"{self.cumulative_net_bps:.2f}",
                ])
                self._last_trade_time = time.time()

                fill_diff = abs(l_fill - x_fill)
                if fill_diff > close_qty * Decimal("0.1"):
                    self.logger.warning(
                        f"[平仓] 双边成交量偏差! L={l_fill} X={x_fill} "
                        f"Δ={fill_diff}, 触发对账")
                    self._force_reconcile = True

            elif l_ok and not x_ok:
                self.logger.error(
                    f"[平仓] Extended未成交, 回撤Lighter "
                    f"{close_l_side}→{l_fill} ({total_ms:.0f}ms)")
                undo_side = "sell" if close_l_side == "buy" else "buy"
                ls = self.lighter_feed.snapshot if self.lighter_feed else None
                undo_ref = l_ref
                if ls and ls.mid:
                    undo_ref_val = (
                        (ls.best_bid or ls.mid) if undo_side == "sell"
                        else (ls.best_ask or ls.mid))
                    undo_ref = Decimal(str(undo_ref_val or l_ref))
                undo_fill = await self._place_lighter_taker(
                    undo_side, l_fill, undo_ref)
                if not (undo_fill and undo_fill > 0):
                    self.logger.error("[平仓] Lighter 回撤失败! 触发对账")
                    self._force_reconcile = True
                return False

            elif not l_ok and x_ok:
                self.logger.error(
                    f"[平仓] Lighter未成交, 回撤Extended "
                    f"{close_x_side}→{x_fill} ({total_ms:.0f}ms)")
                undo_x_side = "sell" if close_x_side == "buy" else "buy"
                x_bid_now, x_ask_now = self._get_extended_ws_bbo()
                undo_x_ref = (x_bid_now if undo_x_side == "sell"
                              else x_ask_now) or x_ref
                undo_fill = await self._place_extended_ioc(
                    undo_x_side, x_fill, undo_x_ref)
                if not (undo_fill and undo_fill > 0):
                    self.logger.error("[平仓] Extended 回撤失败! 触发对账")
                    self._force_reconcile = True
                return False

            else:
                self.logger.warning(
                    f"[平仓] 双边均未成交({total_ms:.0f}ms), 下次重试")
                return False

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
            await self._do_lighter_cancel_all()
            await self._cancel_all_extended_orders_rest()
            await asyncio.sleep(0.5)

            for attempt in range(3):
                l_qty = abs(self.lighter_position)
                x_qty = abs(self.extended_position)
                if l_qty < self.order_size * Decimal("0.01") and x_qty < self.order_size * Decimal("0.01"):
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

                if x_qty >= self.order_size * Decimal("0.01"):
                    x_side = "sell" if self.extended_position > 0 else "buy"
                    x_bid, x_ask = self._get_extended_ws_bbo()
                    x_ref = x_bid if x_side == "sell" else x_ask
                    if x_ref and x_ref > 0:
                        self.logger.info(
                            f"[紧急平仓] #{attempt+1} Extended IOC "
                            f"{x_side} {x_qty} @ {x_ref}")
                        tasks.append(
                            self._place_extended_ioc(x_side, x_qty, x_ref))
                    else:
                        tasks.append(asyncio.sleep(0))

                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for i, r in enumerate(results):
                        if isinstance(r, Exception):
                            self.logger.error(
                                f"[紧急平仓] 任务{i}异常: {r}")

                    if (len(results) >= 2
                            and not isinstance(results[-1], Exception)
                            and results[-1] is not None
                            and results[-1] > 0):
                        x_fill = results[-1]
                        x_side_used = ("sell" if self.extended_position > 0
                                       else "buy")
                        if x_side_used == "sell":
                            self.extended_position -= x_fill
                        else:
                            self.extended_position += x_fill

                await asyncio.sleep(2)
                try:
                    await self._reconcile_positions()
                except Exception as e:
                    self.logger.warning(f"[紧急平仓] 对账失败: {e}")

            l_final = abs(self.lighter_position)
            x_final = abs(self.extended_position)
            if (l_final >= self.order_size * Decimal("0.01")
                    or x_final >= self.order_size * Decimal("0.01")):
                self.logger.error(
                    f"[紧急平仓] 3轮后仍有残余! "
                    f"L={self.lighter_position} "
                    f"X={self.extended_position} 请手动处理!")
            else:
                self.logger.info("[紧急平仓] 仓位已清零")
        except Exception as e:
            self.logger.error(
                f"紧急平仓异常: {e}\n{traceback.format_exc()}")

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
                    f"退出仍有仓位: L={self.lighter_position} "
                    f"X={self.extended_position}")
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
            f"启动 V11 双边Taker全市价套利  {self.symbol}  "
            f"size={self.order_size} max_pos={self.max_position}  "
            f"目标价差≥{self.target_spread}bps  "
            f"平仓价差≤{self.close_gap_bps}bps  "
            f"dry_run={self.dry_run}  "
            f"往返费用≈{self.ROUND_TRIP_FEE_BPS:.1f}bps  {ladder_desc}  "
            f"滑点保护≤{self._max_slippage_bps}bps")

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
        self.extended_feed = ExtendedFeed(self.symbol)
        asyncio.create_task(self.lighter_feed.connect())
        asyncio.create_task(self.extended_feed.connect())
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

        self.logger.info(f"进入主循环  状态={self.state.value}")
        last_reconcile = time.time()
        last_status_log = 0.0

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
                            and self.extended_client._ob_last_update_ts > 0
                         else 999)

                data_stale = l_age > 5.0 or x_age > 5.0
                if data_stale:
                    await asyncio.sleep(self.interval)
                    continue

                mid_spread_bps = float((x_mid - l_mid) / l_mid * 10000)
                cur_pos = abs(self.extended_position)

                if now - last_status_log > 15:
                    hold_s = ""
                    if self.position_opened_at:
                        hold_s = f" hold={now - self.position_opened_at:.0f}s"
                    basis_info = ""
                    if (self.state == State.IN_POSITION
                            and x_ws_bid is not None and x_ws_ask is not None):
                        if self.extended_position > 0:
                            _cur_gap = l_ask - x_ws_bid
                            _ref = x_ws_bid
                        else:
                            _cur_gap = x_ws_ask - l_bid
                            _ref = l_bid
                        _gap_bps = float(_cur_gap / _ref * 10000) if _ref > 0 else 999.0
                        _close_ok = _gap_bps <= self.close_gap_bps
                        _hold = (now - self.position_opened_at
                                 if self.position_opened_at else 0)
                        _open_sp_bps = 0.0
                        if self._open_x_refs and self._open_l_refs:
                            _x_avg = sum(p * q for p, q in self._open_x_refs) / sum(
                                q for _, q in self._open_x_refs)
                            _l_avg = sum(p * q for p, q in self._open_l_refs) / sum(
                                q for _, q in self._open_l_refs)
                            if self.extended_position > 0:
                                _open_sp_bps = float(
                                    (_l_avg - _x_avg) / _x_avg * 10000
                                ) if _x_avg > 0 else 0.0
                            else:
                                _open_sp_bps = float(
                                    (_x_avg - _l_avg) / _x_avg * 10000
                                ) if _x_avg > 0 else 0.0
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
                        ladder_info = (
                            f"  层={_layers}/{_max_layers} 下层≥{_next:.1f}bps")
                    self.logger.info(
                        f"[{self.state.value}] mid={mid_spread_bps:+.2f}bps  "
                        f"X_pos={self.extended_position} "
                        f"L_pos={self.lighter_position}{hold_s}  "
                        f"L={l_bid}/{l_ask} X={x_ws_bid}/{x_ws_ask}"
                        f"{basis_info}{ladder_info}  "
                        f"鲜度:L={l_age:.1f}s X={x_age:.1f}s  "
                        f"累积={self.cumulative_net_bps:+.2f}bps")
                    last_status_log = now

                if self._force_reconcile and not self.dry_run:
                    self._force_reconcile = False
                    self.logger.warning("[强制对账] 触发")
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

                if (not self.dry_run and now - last_reconcile > 30
                        and self.state in (State.IDLE, State.IN_POSITION)):
                    await self._reconcile_positions()
                    last_reconcile = now
                    cur_pos = abs(self.extended_position)
                    net_imbalance = abs(
                        self.lighter_position + self.extended_position)
                    if net_imbalance > self.order_size * Decimal("0.5"):
                        self.logger.error(
                            f"[周期对账] 仓位不平衡! "
                            f"L={self.lighter_position} "
                            f"X={self.extended_position} "
                            f"偏差={net_imbalance}")
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
                        if has_l and has_x:
                            self.logger.warning(
                                f"[IDLE] 检测到残余双边仓位 "
                                f"L={self.lighter_position} "
                                f"X={self.extended_position}, 恢复为IN_POSITION")
                            self.state = State.IN_POSITION
                            self.position_opened_at = time.time()
                            if self.extended_position > 0:
                                self.position_direction = "long_X_short_L"
                            else:
                                self.position_direction = "long_L_short_X"
                            continue
                        else:
                            self.logger.error(
                                f"[IDLE] 检测到单腿仓位! "
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

                    opp = self._check_open_opportunity(
                        l_bid, l_ask, x_ws_bid, x_ws_ask)
                    if opp and not self.dry_run:
                        direction, l_side, x_side, l_ref, x_ref, sp_bps = opp
                        self.logger.info(
                            f"[发现机会] {direction} "
                            f"价差={sp_bps:.1f}bps "
                            f"L_{l_side}@{l_ref} X_{x_side}@{x_ref}")
                        ok = await self._open_position(
                            direction, l_side, x_side,
                            l_ref, x_ref, self.order_size, sp_bps)
                        if ok:
                            self.state = State.IN_POSITION
                        else:
                            await asyncio.sleep(1.0)
                    elif opp and self.dry_run:
                        direction, l_side, x_side, l_ref, x_ref, sp_bps = opp
                        if now - last_status_log > 5:
                            self.logger.info(
                                f"[DRY-RUN] {direction} "
                                f"价差={sp_bps:.1f}bps")

                # ━━━━━━ STATE: IN_POSITION ━━━━━━
                elif self.state == State.IN_POSITION:
                    if (abs(self.extended_position) < self.order_size * Decimal("0.01")
                            and abs(self.lighter_position) < self.order_size * Decimal("0.01")):
                        self.logger.info("[IN_POSITION] 仓位已清零, 回到 IDLE")
                        self._reset_position_state()
                        self.state = State.IDLE
                        continue

                    if (self.position_opened_at
                            and now - self.position_opened_at > self.max_hold_sec):
                        self.logger.warning(
                            f"[超时强平] 持仓 "
                            f"{now - self.position_opened_at:.0f}s "
                            f"> {self.max_hold_sec}s")
                        await self._emergency_flatten()
                        self._reset_position_state()
                        self.state = State.IDLE
                        last_reconcile = time.time() - 50
                        continue

                    x_zero = abs(self.extended_position) < self.order_size * Decimal("0.01")
                    l_zero = abs(self.lighter_position) < self.order_size * Decimal("0.01")
                    if x_zero != l_zero and not self.dry_run:
                        self.logger.warning(
                            f"[单边仓位?] X={self.extended_position} "
                            f"L={self.lighter_position}, 先对账...")
                        await self._reconcile_positions()
                        x_zero2 = abs(self.extended_position) < self.order_size * Decimal("0.01")
                        l_zero2 = abs(self.lighter_position) < self.order_size * Decimal("0.01")
                        if x_zero2 != l_zero2:
                            self.logger.error("[单边仓位] 对账确认不平衡")
                            await self._emergency_flatten()
                            self._reset_position_state()
                            self.state = State.IDLE
                            last_reconcile = time.time() - 50
                            continue
                        else:
                            self.logger.info("[单边仓位] 对账后恢复正常")
                            last_reconcile = now

                    if (x_ws_bid is not None and x_ws_ask is not None
                            and not self.dry_run):

                        just_traded = (now - self._last_trade_time
                                       < self._min_hold_sec)

                        # Ladder: add layer if spread widens
                        ladder_opened = False
                        if (self.ladder_step > 0
                                and cur_pos < self.max_position
                                    - self.order_size * Decimal("0.01")
                                and self.position_direction
                                and now - self._last_ladder_attempt > self._ladder_cooldown
                                and not just_traded):
                            hold_elapsed = (now - self.position_opened_at
                                            if self.position_opened_at else 0)
                            if hold_elapsed <= self.max_hold_sec * 0.7:
                                if self.position_direction == "long_X_short_L":
                                    dir_sp = float(
                                        (l_mid - x_mid) / l_mid * 10000
                                    ) if l_mid > 0 else 0.0
                                else:
                                    dir_sp = float(
                                        (x_mid - l_mid) / l_mid * 10000
                                    ) if l_mid > 0 else 0.0
                                if dir_sp > (self._last_open_spread_bps
                                             + self.ladder_step):
                                    self._last_ladder_attempt = now
                                    opp = self._check_open_opportunity(
                                        l_bid, l_ask, x_ws_bid, x_ws_ask)
                                    if (opp and
                                            opp[0] == self.position_direction):
                                        cur_layers = int(
                                            cur_pos / self.order_size) + 1
                                        max_layers = int(
                                            self.max_position / self.order_size)
                                        self.logger.info(
                                            f"[阶梯] 加仓 {cur_layers}"
                                            f"/{max_layers} "
                                            f"spread={dir_sp:.1f}bps")
                                        d, ls2, xs, lr, xr, sp = opp
                                        ok = await self._open_position(
                                            d, ls2, xs, lr, xr,
                                            self.order_size, sp)
                                        if ok:
                                            ladder_opened = True
                                        else:
                                            await asyncio.sleep(1.0)

                        # Close check (skip if just opened/added layer)
                        if (not just_traded and not ladder_opened
                                and self._check_close_condition(
                                    l_bid, l_ask, x_ws_bid, x_ws_ask)):
                            close_ok = await self._close_position(
                                l_bid, l_ask, x_ws_bid, x_ws_ask)
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
        self._last_open_spread_bps = 0.0


def main():
    p = argparse.ArgumentParser(
        description="V11 双边Taker全市价套利 (Lighter+Extended)")
    p.add_argument("-s", "--symbol", default="ETH")
    p.add_argument("--size", type=str, default="0.1")
    p.add_argument("--max-position", type=str, default="0.5")
    p.add_argument("--target-spread", type=float, default=6.0,
                   help="开仓最低价差目标 (bps, 需覆盖5bps往返费用)")
    p.add_argument("--close-gap-bps", type=float, default=1.0,
                   help="平仓阈值: 价差收敛到≤此值(bps)即平仓")
    p.add_argument("--max-hold-sec", type=float, default=1800.0)
    p.add_argument("--max-slippage-bps", type=float, default=5.0,
                   help="IOC限价滑点保护(bps), 防止价格剧变时成交在极差价位")
    p.add_argument("--interval", type=float, default=0.02,
                   help="最大轮询间隔(s), BBO事件驱动时实际更快")
    p.add_argument("--ladder-step", type=float, default=0.0)
    p.add_argument("--ladder-cooldown", type=float, default=10.0,
                   help="阶梯加仓最小间隔(秒), 防止短时间内连续加仓")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--env-file", default=".env")
    args = p.parse_args()

    load_dotenv(args.env_file)

    if args.target_spread < 0:
        print(f"[警告] target_spread={args.target_spread} 为负数，已修正为 0")
        args.target_spread = 0

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
    )
    bot._max_slippage_bps = args.max_slippage_bps
    bot._ladder_cooldown = args.ladder_cooldown

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
