#!/usr/bin/env python3
"""
Extended + Lighter 价差收敛套利 V4（Lighter Maker + Extended Taker 模式）

策略：
  检测到价差偏离 → 在 Lighter 挂 POST_ONLY maker 单（0.4 bps 费用，升级账户无延迟）
  → Lighter 成交后立刻在 Extended IOC taker 对冲（~2.5 bps 费用）
  → 价差收敛时反向操作平仓（同样 Lighter maker + Extended taker）
  往返总费用: ~5.8 bps (Lighter maker 0.4% x2 + Extended taker 2.5% x2)

与 V3 的区别：
  V3: Extended maker(0bps) + Lighter taker(0bps) — Extended 挂单几乎不成交
  V4: Lighter maker(0.4bps) + Extended taker(2.5bps) — Lighter 成交率高，无延迟

数据源：
  Lighter → WebSocket 实时深度订单簿 + account_orders 填单推送
  Extended → ExtendedClient WebSocket 实时 BBO + 账户订单状态

用法:
  python extended_lighter_arb_v4.py --symbol ETH --size 0.1 --open-bps 8 --close-bps 1
  python extended_lighter_arb_v4.py --symbol ETH --dry-run
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
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import requests
import websockets
from dotenv import load_dotenv
from lighter.signer_client import SignerClient

sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.extended import ExtendedClient
from feeds.lighter_feed import LighterFeed
from feeds.existing_feeds import ExtendedFeed
from x10.perpetual.orders import TimeInForce, OrderSide


# ═══════════════════════════════════════════════════════
#  States & data types
# ═══════════════════════════════════════════════════════

class State(str, Enum):
    IDLE = "IDLE"
    MAKING = "MAKING"           # Lighter maker placed, waiting for fill
    HEDGING = "HEDGING"         # Lighter filled, sending Extended IOC
    IN_POSITION = "IN_POSITION"
    CLOSING_MAKE = "CLOSING_MAKE"
    CLOSING_HEDGE = "CLOSING_HEDGE"


@dataclass
class PendingMaker:
    client_order_index: int
    order_index: Optional[int]  # assigned by Lighter on 'open' event
    side: str                   # "buy" or "sell"
    direction: str              # "long_L_short_X" or "long_X_short_L"
    qty_target: Decimal
    price: Decimal
    placed_at: float
    deadline: float
    filled_qty: Decimal = Decimal("0")
    hedged_qty: Decimal = Decimal("0")
    hedge_qty_needed: Decimal = Decimal("0")
    avg_price: Optional[Decimal] = None
    phase: str = "open"         # "open" or "close"


class _Config:
    """Minimal config wrapper expected by ExtendedClient."""
    def __init__(self, d: dict):
        for k, v in d.items():
            setattr(self, k, v)


def _utc_now():
    return datetime.now(tz=timezone.utc)


# ═══════════════════════════════════════════════════════
#  Main Bot
# ═══════════════════════════════════════════════════════

class LighterMakerArbBot:

    LIGHTER_MAKER_FEE_BPS = 0.4
    EXTENDED_TAKER_FEE_BPS = 2.5
    ROUND_TRIP_FEE_BPS = (LIGHTER_MAKER_FEE_BPS + EXTENDED_TAKER_FEE_BPS) * 2

    def __init__(
        self,
        symbol: str,
        order_size: Decimal,
        max_position: Decimal,
        open_bps: float,
        close_bps: float,
        max_hold_sec: float,
        interval: float,
        maker_timeout: float,
        dry_run: bool,
    ):
        self.symbol = symbol.upper()
        self.order_size = order_size
        self.max_position = max_position
        self.open_bps = open_bps
        self.close_bps = close_bps
        self.max_hold_sec = max_hold_sec
        self.interval = interval
        self.maker_timeout = maker_timeout
        self.dry_run = dry_run

        self.state = State.IDLE
        self.stop_flag = False

        self.lighter_position = Decimal("0")
        self.extended_position = Decimal("0")
        self.position_opened_at: Optional[float] = None
        self.position_direction: Optional[str] = None

        self._pending: Optional[PendingMaker] = None
        self._last_open_spread: float = 0.0
        self._stale_fills: List[dict] = []

        # Lighter WS state for account_orders
        self._lighter_ws_task: Optional[asyncio.Task] = None
        self._lighter_fill_event = asyncio.Event()
        self._nonce_counter: int = 0

        # Extended WS BBO freshness
        self._x_ws_bbo_ts: float = 0.0
        self._x_ws_last_ob_id: int = 0
        self.STALE_THRESHOLD_SEC: float = 5.0

        # Extended IOC fill confirmation
        self._x_ioc_pending_id: Optional[str] = None
        self._x_ioc_confirmed = asyncio.Event()
        self._x_ioc_fill_qty: Decimal = Decimal("0")

        self._open_l_refs: List[Tuple[Decimal, Decimal]] = []
        self._open_x_refs: List[Tuple[Decimal, Decimal]] = []

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

        os.makedirs("logs", exist_ok=True)
        self.csv_path = f"logs/arb_v4_{self.symbol}_trades.csv"
        self.log_path = f"logs/arb_v4_{self.symbol}.log"
        self._init_csv()
        self.logger = self._init_logger()

    # ═══════════════════════════════════════════════
    #  Logging / CSV
    # ═══════════════════════════════════════════════

    def _init_logger(self) -> logging.Logger:
        lg = logging.getLogger(f"arb_v4_{self.symbol}")
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
                    "spread_bps", "maker_wait_ms", "hedge_ms",
                    "gross_bps", "net_bps", "cumulative_net_bps",
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
        self.logger.info("Lighter SignerClient 已初始化")

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
            "ticker": self.symbol,
            "contract_id": "",
            "quantity": self.order_size,
            "tick_size": Decimal("0.01"),
            "close_order_side": "sell",
        })
        self.extended_client = ExtendedClient(cfg)
        self.extended_client.setup_order_update_handler(self._on_extended_order_event)
        self.logger.info("Extended client 已初始化")

    def _on_extended_order_event(self, event: dict):
        """Track Extended IOC fill confirmation via WS."""
        order_id = str(event.get("order_id", ""))
        status = event.get("status", "")
        if self._x_ioc_pending_id and order_id == self._x_ioc_pending_id:
            if status == "FILLED":
                filled = Decimal(str(event.get("filled_size", "0")))
                self._x_ioc_fill_qty = filled
                self._x_ioc_confirmed.set()
            elif status in ("CANCELED", "EXPIRED"):
                self._x_ioc_fill_qty = Decimal(str(event.get("filled_size", "0")))
                self._x_ioc_confirmed.set()
                self.logger.warning(
                    f"[Extended] IOC 订单 {order_id} 状态={status} "
                    f"已成交={self._x_ioc_fill_qty} (可能未完全成交)")

    async def _get_extended_contract_info(self):
        cid, tick = await self.extended_client.get_contract_attributes()
        self.extended_contract_id = cid
        self.extended_tick_size = tick
        if hasattr(self.extended_client, "min_order_size"):
            self.extended_min_order_size = self.extended_client.min_order_size
        self.logger.info(f"Extended contract: {cid}  tick={tick}")

    # ═══════════════════════════════════════════════
    #  Extended WS BBO
    # ═══════════════════════════════════════════════

    def _get_extended_ws_bbo(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        if self.extended_client and self.extended_client.orderbook:
            ob = self.extended_client.orderbook
            ob_id = id(ob)
            if ob_id != self._x_ws_last_ob_id:
                self._x_ws_bbo_ts = time.time()
                self._x_ws_last_ob_id = ob_id
            bids = ob.get('bid', [])
            asks = ob.get('ask', [])
            if bids and asks:
                try:
                    return Decimal(bids[0]['p']), Decimal(asks[0]['p'])
                except (KeyError, IndexError, TypeError):
                    pass
        return None, None

    # ═══════════════════════════════════════════════
    #  Lighter WS: account_orders (fill detection)
    # ═══════════════════════════════════════════════

    async def _lighter_account_ws_loop(self):
        """Subscribe to Lighter account_orders WS for real-time fill detection."""
        url = "wss://mainnet.zklighter.elliot.ai/stream"

        while not self.stop_flag:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    auth_token, err = self.lighter_client.create_auth_token_with_expiry(
                        api_key_index=self.api_key_index)
                    if err is not None:
                        self.logger.error(f"Lighter auth token 创建失败: {err}")
                        await asyncio.sleep(5)
                        continue

                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": f"account_orders/{self.lighter_market_index}/{self.account_index}",
                        "auth": auth_token,
                    }))
                    self.logger.info(
                        f"已订阅 Lighter account_orders/{self.lighter_market_index}/{self.account_index}")

                    while not self.stop_flag:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                            data = json.loads(raw)
                            msg_type = data.get("type", "")

                            if msg_type == "ping":
                                await ws.send(json.dumps({"type": "pong"}))
                                continue

                            if msg_type == "update/account_orders":
                                orders = data.get("orders", {}).get(
                                    str(self.lighter_market_index), [])
                                for od in orders:
                                    status = od.get("status", "")
                                    if status == "filled":
                                        self._on_lighter_fill(od)
                                    elif status == "open":
                                        self._on_lighter_order_open(od)
                                    elif status == "canceled":
                                        self._on_lighter_order_canceled(od)

                        except asyncio.TimeoutError:
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            self.logger.warning("Lighter account_orders WS 断开")
                            break
                        except Exception as e:
                            self.logger.error(f"Lighter account_orders WS 错误: {e}")
                            break

            except Exception as e:
                self.logger.error(f"Lighter account_orders WS 连接失败: {e}")

            if not self.stop_flag:
                await asyncio.sleep(2)

    def _is_current_order(self, od: dict) -> bool:
        if self._pending is None:
            return False
        fill_order_idx = od.get("order_index")
        fill_client_id = od.get("client_order_id")
        if self._pending.order_index is not None and fill_order_idx is not None:
            return str(fill_order_idx) == str(self._pending.order_index)
        if fill_client_id is not None:
            return int(fill_client_id) == self._pending.client_order_index
        return False

    def _on_lighter_fill(self, od: dict):
        """Handle a Lighter maker order fill event."""
        try:
            filled_base = Decimal(od.get("filled_base_amount", "0"))
            filled_quote = Decimal(od.get("filled_quote_amount", "0"))
            avg_price = filled_quote / filled_base if filled_base > 0 else Decimal("0")
            is_ask = od.get("is_ask", False)
            side_str = "sell" if is_ask else "buy"

            if not self._is_current_order(od):
                self.logger.warning(
                    f"[Lighter] 非当前订单成交: {side_str} {filled_base} @ {avg_price} "
                    f"(order_idx={od.get('order_index')}) → 加入修复队列")
                self._stale_fills.append({
                    "side": side_str,
                    "size": filled_base,
                    "price": avg_price,
                })
                return

            if self._pending is None:
                return

            old_filled = self._pending.filled_qty
            if filled_base <= old_filled:
                return

            delta = filled_base - old_filled
            self._pending.filled_qty = filled_base
            self._pending.avg_price = avg_price
            self._pending.hedge_qty_needed = filled_base - self._pending.hedged_qty

            if self._pending.hedge_qty_needed >= self.order_size * Decimal("0.01"):
                if self._pending.phase == "open":
                    self.state = State.HEDGING
                else:
                    self.state = State.CLOSING_HEDGE
                self.logger.info(
                    f"[Lighter FILL] {side_str} filled={filled_base} avg={avg_price:.2f} "
                    f"待对冲={self._pending.hedge_qty_needed}")
                self._lighter_fill_event.set()

        except Exception as e:
            self.logger.error(f"处理 Lighter 成交出错: {e}\n{traceback.format_exc()}")

    def _on_lighter_order_open(self, od: dict):
        """Capture exchange-assigned order_index when maker order goes live."""
        try:
            client_id = od.get("client_order_id")
            if self._pending is None:
                return
            if client_id is not None and int(client_id) == self._pending.client_order_index:
                new_idx = od.get("order_index")
                if new_idx is not None and new_idx != self._pending.order_index:
                    self._pending.order_index = new_idx
                    self.logger.info(f"[Lighter] 挂单已上链 order_index={new_idx}")
        except Exception as e:
            self.logger.error(f"处理 Lighter 订单上链出错: {e}")

    def _on_lighter_order_canceled(self, od: dict):
        """Handle cancellation — process any partial fills."""
        try:
            filled_base = Decimal(od.get("filled_base_amount", "0"))
            if filled_base > 0:
                self._on_lighter_fill(od)
                return

            if not self._is_current_order(od):
                return

            self.logger.info("[Lighter] 挂单已取消 (无成交)")
            if self._pending is not None:
                if self.state in (State.MAKING, State.CLOSING_MAKE):
                    if self._pending.phase == "close":
                        self.state = State.IN_POSITION
                    else:
                        self.state = State.IDLE
                    self._pending = None

        except Exception as e:
            self.logger.error(f"处理 Lighter 订单取消出错: {e}")

    # ═══════════════════════════════════════════════
    #  Order Execution: Lighter Maker
    # ═══════════════════════════════════════════════

    def _round_lighter_price(self, price: Decimal) -> Decimal:
        if self.lighter_tick > 0:
            return (price / self.lighter_tick).quantize(Decimal("1")) * self.lighter_tick
        return price

    async def _place_lighter_maker(self, side: str, qty: Decimal) -> Optional[int]:
        """Place POST_ONLY maker on Lighter. Returns client_order_index or None.
        On 'invalid nonce' or similar API errors where the order may have executed,
        still returns the client_order_index so the caller tracks it via WS."""
        ls = self.lighter_feed.snapshot
        if ls.best_bid is None or ls.best_ask is None:
            self.logger.error("[Lighter] 盘口不可用")
            return None

        l_bid = Decimal(str(ls.best_bid))
        l_ask = Decimal(str(ls.best_ask))

        if l_bid >= l_ask:
            self.logger.warning(f"[Lighter] 盘口交叉 bid={l_bid} >= ask={l_ask}，跳过")
            return None

        spread_ticks = (l_ask - l_bid) / self.lighter_tick

        if side == "buy":
            if spread_ticks > 1:
                price = l_bid + self.lighter_tick
            else:
                price = l_bid
            price = min(price, l_ask - self.lighter_tick)
        else:
            if spread_ticks > 1:
                price = l_ask - self.lighter_tick
            else:
                price = l_ask
            price = max(price, l_bid + self.lighter_tick)

        rounded_price = self._round_lighter_price(price)
        if rounded_price <= 0:
            self.logger.error(f"[Lighter] 无效价格: {rounded_price}")
            return None

        self._nonce_counter += 1
        client_order_index = int(time.time() * 1000) * 100 + (self._nonce_counter % 100)
        is_ask = side.lower() == "sell"

        try:
            _, _, error = await self.lighter_client.create_order(
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
            if error is not None:
                err_str = str(error)
                if "nonce" in err_str.lower():
                    self.logger.warning(
                        f"[Lighter] maker 下单返回 nonce 错误: {error} — "
                        f"订单可能已上链, 仍然追踪 client_id={client_order_index}")
                    return client_order_index
                self.logger.error(f"[Lighter] maker 下单失败: {error}")
                return None

            self.logger.info(
                f"[Lighter] maker {side} {qty} @ {rounded_price}  "
                f"spread={spread_ticks:.0f}tick  client_id={client_order_index}")
            return client_order_index

        except Exception as e:
            self.logger.error(f"[Lighter] maker 下单异常: {e}")
            return None

    async def _cancel_pending_maker(self):
        """Cancel the pending Lighter maker order."""
        if not self._pending:
            return
        order_idx = self._pending.order_index
        if order_idx is not None:
            try:
                _, _, error = await self.lighter_client.cancel_order(
                    market_index=self.lighter_market_index,
                    order_index=order_idx,
                )
                if error is not None:
                    self.logger.error(f"[Lighter] 撤单错误: {error}")
                else:
                    self.logger.info(f"[Lighter] 撤单成功 order_index={order_idx}")
                    return
            except Exception as e:
                self.logger.error(f"[Lighter] 撤单异常: {e}")

        self.logger.warning("[Lighter] order_index 不可用或撤单失败，使用 cancel_all 兜底")
        try:
            future_ts_ms = int((time.time() + 300) * 1000)
            _, _, error = await self.lighter_client.cancel_all_orders(
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                timestamp_ms=future_ts_ms,
            )
            if error:
                self.logger.error(f"[Lighter] cancel_all 错误: {error}")
            else:
                self.logger.info("[Lighter] cancel_all 成功")
        except Exception as e:
            self.logger.error(f"[Lighter] cancel_all 异常: {e}")

    # ═══════════════════════════════════════════════
    #  Order Execution: Extended Taker (IOC)
    # ═══════════════════════════════════════════════

    async def _place_extended_ioc(self, side: str, qty: Decimal, ref_price: Decimal,
                                  max_retries: int = 2) -> bool:
        """Place true IOC taker order on Extended with fill confirmation via WS.
        Retries up to max_retries on failure."""
        qty_rounded = qty.quantize(self.extended_min_order_size, rounding=ROUND_HALF_UP)

        for attempt in range(max_retries):
            x_bid, x_ask = self._get_extended_ws_bbo()
            if side == "buy":
                fresh_ref = x_ask if x_ask else ref_price
                aggressive = fresh_ref * Decimal("1.005")
            else:
                fresh_ref = x_bid if x_bid else ref_price
                aggressive = fresh_ref * Decimal("0.995")
            aggressive = aggressive.quantize(self.extended_tick_size, rounding=ROUND_HALF_UP)

            self._x_ioc_pending_id = None
            self._x_ioc_confirmed.clear()
            self._x_ioc_fill_qty = Decimal("0")

            try:
                result = await self.extended_client.place_ioc_order(
                    contract_id=self.extended_contract_id,
                    quantity=qty_rounded,
                    side=side,
                    aggressive_price=aggressive,
                )
                if result.success:
                    self._x_ioc_pending_id = str(result.order_id)
                    self.logger.info(
                        f"[Extended] IOC {side} {qty_rounded} @ ~{aggressive:.2f}  "
                        f"id={result.order_id}")

                    try:
                        await asyncio.wait_for(self._x_ioc_confirmed.wait(), timeout=3.0)
                        if self._x_ioc_fill_qty < qty_rounded:
                            self.logger.warning(
                                f"[Extended] IOC 部分成交: {self._x_ioc_fill_qty}/{qty_rounded}")
                    except asyncio.TimeoutError:
                        self.logger.warning(
                            f"[Extended] IOC 成交确认超时 (3s)，"
                            f"假设已成交 (将在下次对账验证)")

                    self._x_ioc_pending_id = None
                    return True

                self.logger.error(
                    f"[Extended] IOC 失败 (尝试{attempt+1}/{max_retries}): "
                    f"{result.error_message}")
            except Exception as e:
                self.logger.error(
                    f"[Extended] IOC 异常 (尝试{attempt+1}/{max_retries}): {e}")

            if attempt < max_retries - 1:
                await asyncio.sleep(0.3)

        self._x_ioc_pending_id = None
        return False

    # ═══════════════════════════════════════════════
    #  Lighter Taker (for emergency flatten only)
    # ═══════════════════════════════════════════════

    async def _place_lighter_taker(self, side: str, qty: Decimal, ref_price: Decimal) -> bool:
        """Aggressive IOC taker on Lighter — fills immediately or is cancelled."""
        is_ask = side.lower() == "sell"
        if is_ask:
            price = self._round_lighter_price(ref_price * Decimal("0.995"))
        else:
            price = self._round_lighter_price(ref_price * Decimal("1.005"))
        self._nonce_counter += 1
        client_order_index = int(time.time() * 1000) * 100 + (self._nonce_counter % 100)
        try:
            _, _, error = await self.lighter_client.create_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=int(qty * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=False,
                trigger_price=0,
            )
            if error is not None:
                self.logger.error(f"[Lighter] taker 下单失败: {error}")
                return False
            self.logger.info(f"[Lighter] taker {side} {qty} ref={ref_price:.2f}")
            return True
        except Exception as e:
            self.logger.error(f"[Lighter] taker 下单异常: {e}")
            return False

    # ═══════════════════════════════════════════════
    #  Position Queries (REST)
    # ═══════════════════════════════════════════════

    async def _get_lighter_position(self) -> Decimal:
        url = f"{self.lighter_base_url}/api/v1/account"
        params = {"by": "index", "value": self.account_index}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={"accept": "application/json"},
                                       params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
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
            self.logger.warning(f"[对账] 不平衡: Lighter={l} Extended={x} net={net}")
        return balanced

    # ═══════════════════════════════════════════════
    #  Emergency flatten
    # ═══════════════════════════════════════════════

    async def _emergency_flatten(self):
        try:
            if self.lighter_client:
                try:
                    future_ts_ms = int((time.time() + 300) * 1000)
                    await self.lighter_client.cancel_all_orders(
                        time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                        timestamp_ms=future_ts_ms,
                    )
                except Exception:
                    pass
                await asyncio.sleep(0.5)

            if abs(self.lighter_position) > 0:
                l_side = "sell" if self.lighter_position > 0 else "buy"
                l_ref = Decimal(str(self.lighter_feed.snapshot.mid or 0))
                if l_ref > 0:
                    await self._place_lighter_taker(l_side, abs(self.lighter_position), l_ref)
            if abs(self.extended_position) > 0:
                x_side = "sell" if self.extended_position > 0 else "buy"
                x_bid, x_ask = self._get_extended_ws_bbo()
                x_ref = x_bid if x_side == "sell" else x_ask
                if x_ref:
                    await self._place_extended_ioc(x_side, abs(self.extended_position), x_ref)
            await asyncio.sleep(3)
            await self._reconcile_positions()
        except Exception as e:
            self.logger.error(f"紧急平仓异常: {e}")

    # ═══════════════════════════════════════════════
    #  Shutdown
    # ═══════════════════════════════════════════════

    async def _shutdown(self):
        self.stop_flag = True
        self.logger.info("关闭中...")

        if self.lighter_client:
            try:
                self.logger.info("关闭清理: 取消 Lighter 所有挂单...")
                future_ts_ms = int((time.time() + 300) * 1000)
                _, _, error = await asyncio.wait_for(
                    self.lighter_client.cancel_all_orders(
                        time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                        timestamp_ms=future_ts_ms,
                    ),
                    timeout=5.0,
                )
                if error:
                    self.logger.warning(f"cancel_all 错误: {error}")
                else:
                    self.logger.info("Lighter 挂单已全部取消")
            except Exception as e:
                self.logger.warning(f"cancel_all 异常: {e}")

        if self._lighter_ws_task and not self._lighter_ws_task.done():
            self._lighter_ws_task.cancel()

        if self.lighter_feed:
            try:
                await self.lighter_feed.disconnect()
            except Exception:
                pass

        if self.extended_feed:
            try:
                await self.extended_feed.disconnect()
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
            self.logger.error(f"致命错误: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            if self._pending:
                self.logger.warning("退出前撤销挂单...")
                try:
                    await self._cancel_pending_maker()
                    await asyncio.sleep(0.5)
                except Exception:
                    pass

            if not self.dry_run:
                await self._reconcile_positions()
            if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                self.logger.warning(
                    f"退出时仍有仓位: L={self.lighter_position} X={self.extended_position}")
                self.logger.warning("尝试紧急平仓...")
                if not self.dry_run:
                    await self._emergency_flatten()
            else:
                self.logger.info("无持仓，直接退出")
            await self._shutdown()

    # ═══════════════════════════════════════════════
    #  Main loop
    # ═══════════════════════════════════════════════

    async def _main_loop_wrapper(self):
        self.logger.info(
            f"启动 V4 Lighter-Maker+Extended-Taker 套利  {self.symbol}  "
            f"size={self.order_size} max_pos={self.max_position}  "
            f"开仓≥{self.open_bps}bps 平仓≤{self.close_bps}bps  "
            f"maker超时={self.maker_timeout}s  dry_run={self.dry_run}  "
            f"往返费用≈{self.ROUND_TRIP_FEE_BPS:.1f}bps")

        if not self.dry_run:
            self._init_lighter_signer()
            self._get_lighter_market_config()
            self._init_extended_client()
            await self.extended_client.connect()
            await self._get_extended_contract_info()
            self.logger.info("交易客户端就绪")

            self._lighter_ws_task = asyncio.create_task(self._lighter_account_ws_loop())
            self.logger.info("Lighter account_orders WS 已启动")
        else:
            self.logger.info("[DRY-RUN] 跳过交易客户端初始化")
            self._get_lighter_market_config()

        self.lighter_feed = LighterFeed(self.symbol)
        self.extended_feed = ExtendedFeed(self.symbol)
        bg_tasks = [
            asyncio.create_task(self.lighter_feed.connect()),
            asyncio.create_task(self.extended_feed.connect()),
        ]
        self.logger.info("等待行情数据 (8s)...")
        await asyncio.sleep(8)

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
                if self.lighter_position > 0:
                    self.position_direction = "long_L_short_X"
                elif self.lighter_position < 0:
                    self.position_direction = "long_X_short_L"

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

                if not ls.mid:
                    await asyncio.sleep(self.interval)
                    continue

                # ── Freshness check ──
                l_age = now - ls.timestamp if ls.timestamp > 0 else 999
                if not self.dry_run:
                    x_age = now - self._x_ws_bbo_ts if self._x_ws_bbo_ts > 0 else 999
                else:
                    es_ts = self.extended_feed.snapshot.timestamp if self.extended_feed else 0
                    x_age = now - es_ts if es_ts > 0 else 999

                data_stale = l_age > self.STALE_THRESHOLD_SEC or x_age > self.STALE_THRESHOLD_SEC
                if data_stale:
                    if now - last_status_log > 5:
                        stale_parts = []
                        if l_age > self.STALE_THRESHOLD_SEC:
                            stale_parts.append(f"Lighter={l_age:.1f}s")
                        if x_age > self.STALE_THRESHOLD_SEC:
                            stale_parts.append(f"Extended={x_age:.1f}s")
                        self.logger.warning(
                            f"[数据过期] {' '.join(stale_parts)} > {self.STALE_THRESHOLD_SEC}s "
                            f"(状态={self.state.value})")
                        last_status_log = now
                    if self.state not in (State.HEDGING, State.CLOSING_HEDGE):
                        await asyncio.sleep(self.interval)
                        continue

                # ── Price & spread calculation ──
                l_mid = Decimal(str(ls.mid))
                mid_spread_bps = float((x_mid - l_mid) / l_mid * 10000)
                abs_mid_spread = abs(mid_spread_bps)

                qty_f = float(self.order_size)
                l_vwap_buy = ls.vwap_buy(qty_f)
                l_vwap_sell = ls.vwap_sell(qty_f)

                if x_ws_bid is not None and x_ws_ask is not None:
                    x_vwap_buy = float(x_ws_ask)
                    x_vwap_sell = float(x_ws_bid)
                else:
                    x_vwap_buy = self.extended_feed.snapshot.vwap_buy(qty_f) if self.extended_feed else None
                    x_vwap_sell = self.extended_feed.snapshot.vwap_sell(qty_f) if self.extended_feed else None

                if all([l_vwap_buy, l_vwap_sell, x_vwap_buy, x_vwap_sell]):
                    vwap_spread_lx = (x_vwap_sell - l_vwap_buy) / l_vwap_buy * 10000
                    vwap_spread_xl = (l_vwap_sell - x_vwap_buy) / x_vwap_buy * 10000
                    if vwap_spread_lx >= vwap_spread_xl:
                        vwap_spread_bps = vwap_spread_lx
                        vwap_direction = "long_L_short_X"
                    else:
                        vwap_spread_bps = vwap_spread_xl
                        vwap_direction = "long_X_short_L"
                else:
                    vwap_spread_bps = 0.0
                    vwap_direction = None

                cur_pos = abs(self.lighter_position)

                # ── Status log ──
                if now - last_status_log > 15:
                    hold_s = ""
                    if self.position_opened_at:
                        hold_s = f" hold={now - self.position_opened_at:.0f}s"
                    pending_str = ""
                    if self._pending:
                        wait = now - self._pending.placed_at
                        pending_str = (
                            f" maker_wait={wait:.0f}s filled={self._pending.filled_qty}"
                            f" hedged={self._pending.hedged_qty}")
                    self.logger.info(
                        f"[{self.state.value}] mid={mid_spread_bps:+.2f}bps "
                        f"vwap={vwap_spread_bps:+.2f}bps  "
                        f"L_pos={self.lighter_position} X_pos={self.extended_position}{hold_s}  "
                        f"L_mid={l_mid:.2f} X_mid={x_mid:.2f}  "
                        f"鲜度:L={l_age:.1f}s X={x_age:.1f}s{pending_str}  "
                        f"累积净利={self.cumulative_net_bps:+.2f}bps")
                    last_status_log = now

                # ── Periodic reconciliation ──
                if (not self.dry_run
                        and now - last_reconcile > 60
                        and self.state in (State.IDLE, State.IN_POSITION)):
                    await self._reconcile_positions()
                    last_reconcile = now
                    cur_pos = abs(self.lighter_position)

                # ── Stale fill repair (runs in any non-hedging state) ──
                if (self._stale_fills
                        and self.state not in (State.HEDGING, State.CLOSING_HEDGE)
                        and not self.dry_run):
                    repaired = []
                    for sf in self._stale_fills:
                        hedge_side = "sell" if sf["side"] == "buy" else "buy"
                        x_ref = x_ws_bid if hedge_side == "sell" else x_ws_ask
                        if x_ref:
                            self.logger.warning(
                                f"[修复] 过期成交对冲: Extended IOC {hedge_side} "
                                f"{sf['size']} ref={x_ref}")
                            ok = await self._place_extended_ioc(
                                hedge_side, sf["size"], x_ref)
                            if ok:
                                repaired.append(sf)
                            else:
                                self.logger.error(
                                    "[修复] Extended IOC 失败，保留待下次重试")
                                break
                        else:
                            self.logger.error(
                                f"[修复] Extended BBO 不可用，无法对冲过期成交")
                            break
                    for sf in repaired:
                        self._stale_fills.remove(sf)
                    if repaired:
                        last_reconcile = time.time() - 50

                # ━━━━━━ STATE: IDLE ━━━━━━
                if self.state == State.IDLE:
                    if (vwap_direction
                            and not data_stale
                            and vwap_spread_bps >= self.open_bps
                            and cur_pos < self.max_position):
                        direction = vwap_direction
                        # Lighter maker side: if direction is long_L_short_X,
                        # we BUY on Lighter (maker) and SELL on Extended (taker)
                        if direction == "long_L_short_X":
                            maker_side = "buy"
                        else:
                            maker_side = "sell"

                        if l_vwap_buy is None or l_vwap_sell is None:
                            self.logger.warning("[跳过] Lighter 深度不足")
                            await asyncio.sleep(self.interval)
                            continue

                        self.logger.info(
                            f"[信号] VWAP={vwap_spread_bps:+.2f}bps >= {self.open_bps}  "
                            f"mid={mid_spread_bps:+.2f}bps  方向={direction}  "
                            f"Lighter maker {maker_side}")

                        if self.dry_run:
                            self.logger.info("[DRY-RUN] 模拟 maker 下单")
                            await asyncio.sleep(self.interval)
                            continue

                        coi = await self._place_lighter_maker(maker_side, self.order_size)
                        if coi is not None:
                            l_ref = Decimal(str(
                                ls.best_bid if maker_side == "buy" else ls.best_ask))
                            t_placed = time.time()
                            self._pending = PendingMaker(
                                client_order_index=coi,
                                order_index=None,
                                side=maker_side,
                                direction=direction,
                                qty_target=self.order_size,
                                price=l_ref,
                                placed_at=t_placed,
                                deadline=t_placed + self.maker_timeout,
                                phase="open",
                            )
                            self._last_open_spread = vwap_spread_bps
                            self._lighter_fill_event.clear()
                            self.state = State.MAKING
                            self.logger.info(
                                f"[MAKING] 等待 Lighter {maker_side} 成交  "
                                f"超时={self.maker_timeout}s")
                        else:
                            self.logger.warning("[MAKING] Lighter 下单失败，回到 IDLE")

                # ━━━━━━ STATE: MAKING ━━━━━━
                elif self.state == State.MAKING:
                    should_cancel = False
                    cancel_reason = ""

                    if self._pending and now > self._pending.deadline:
                        should_cancel = True
                        cancel_reason = f"超时 {self.maker_timeout}s"

                    elif self._pending and vwap_spread_bps < self.open_bps * 0.5:
                        wait_sec = now - self._pending.placed_at
                        if wait_sec > 3:
                            should_cancel = True
                            cancel_reason = (
                                f"价差消失 VWAP={vwap_spread_bps:+.1f}bps < "
                                f"{self.open_bps * 0.5:.1f}bps (等了{wait_sec:.0f}s)")

                    if should_cancel and self._pending:
                        self.logger.info(f"[撤单] {cancel_reason}")
                        await self._cancel_pending_maker()
                        await asyncio.sleep(1.0)
                        if self.state == State.MAKING:
                            if (self._pending
                                    and self._pending.filled_qty > self._pending.hedged_qty):
                                delta = self._pending.filled_qty - self._pending.hedged_qty
                                if delta >= self.order_size * Decimal("0.01"):
                                    self._pending.hedge_qty_needed = delta
                                    self.state = State.HEDGING
                                    self.logger.info(
                                        f"[撤单后检查] 发现未对冲成交 {delta}，转入 HEDGING")
                                    continue
                            self._pending = None
                            self.state = State.IDLE

                # ━━━━━━ STATE: HEDGING ━━━━━━
                elif self.state == State.HEDGING:
                    if self._pending and self._pending.hedge_qty_needed > 0:
                        hedge_qty = self._pending.hedge_qty_needed
                        direction = self._pending.direction

                        # Extended taker side: opposite of Lighter maker
                        if direction == "long_L_short_X":
                            hedge_side = "sell"
                            x_ref = x_ws_bid if x_ws_bid else x_mid
                        else:
                            hedge_side = "buy"
                            x_ref = x_ws_ask if x_ws_ask else x_mid

                        self.logger.info(
                            f"[HEDGE] Extended IOC {hedge_side} {hedge_qty}  "
                            f"ref={x_ref:.2f}")

                        t0 = time.time()
                        ok = await self._place_extended_ioc(hedge_side, hedge_qty, x_ref)
                        hedge_ms = (time.time() - t0) * 1000

                        if ok:
                            self._pending.hedged_qty += hedge_qty
                            self._pending.hedge_qty_needed = Decimal("0")
                            l_fill_price = self._pending.avg_price or self._pending.price
                            self._open_l_refs.append((l_fill_price, hedge_qty))
                            self._open_x_refs.append((x_ref, hedge_qty))

                            if direction == "long_L_short_X":
                                self.lighter_position += hedge_qty
                                self.extended_position -= hedge_qty
                            else:
                                self.lighter_position -= hedge_qty
                                self.extended_position += hedge_qty

                            maker_wait_ms = (time.time() - self._pending.placed_at) * 1000
                            self.logger.info(
                                f"[HEDGE OK] 耗时={hedge_ms:.0f}ms  "
                                f"maker等待={maker_wait_ms:.0f}ms  "
                                f"已对冲={self._pending.hedged_qty}/{self._pending.filled_qty}  "
                                f"L_pos={self.lighter_position} X_pos={self.extended_position}")

                            if self._pending.hedged_qty >= self._pending.filled_qty:
                                if self.position_opened_at is None:
                                    self.position_opened_at = time.time()
                                    self._last_open_spread = vwap_spread_bps
                                self.position_direction = direction
                                self.state = State.IN_POSITION
                                self._csv_row([
                                    datetime.now(timezone.utc).isoformat(),
                                    "OPEN", direction,
                                    self._pending.side, f"{l_fill_price:.2f}",
                                    str(hedge_qty),
                                    hedge_side, f"{x_ref:.2f}",
                                    str(self._pending.filled_qty),
                                    f"{vwap_spread_bps:.2f}",
                                    f"{maker_wait_ms:.0f}", f"{hedge_ms:.0f}",
                                    "", "", "",
                                ])
                                self._pending = None
                            else:
                                self.state = State.MAKING
                        else:
                            self.logger.error(
                                "[HEDGE FAIL] Extended IOC 失败! 紧急平 Lighter")
                            l_reverse = "sell" if self._pending.side == "buy" else "buy"
                            if l_reverse == "sell":
                                l_ref_flat = Decimal(str(ls.best_bid or ls.mid or 0))
                            else:
                                l_ref_flat = Decimal(str(ls.best_ask or ls.mid or 0))
                            if l_ref_flat > 0:
                                await self._place_lighter_taker(
                                    l_reverse, hedge_qty, l_ref_flat)
                            self._pending = None
                            self.state = State.IDLE
                            last_reconcile = time.time() - 50

                # ━━━━━━ STATE: IN_POSITION ━━━━━━
                elif self.state == State.IN_POSITION:
                    if not data_stale and abs_mid_spread <= self.close_bps:
                        self.logger.info(
                            f"[平仓信号] mid价差收敛到 {mid_spread_bps:+.2f} bps")

                        if self.dry_run:
                            self._reset_position_state()
                            self.state = State.IDLE
                            continue

                        # Lighter maker side for closing: opposite of opening
                        if self.lighter_position > 0:
                            close_maker_side = "sell"
                        elif self.lighter_position < 0:
                            close_maker_side = "buy"
                        else:
                            self._reset_position_state()
                            self.state = State.IDLE
                            continue

                        close_qty = abs(self.lighter_position)
                        coi = await self._place_lighter_maker(close_maker_side, close_qty)
                        if coi is not None:
                            l_ref = Decimal(str(
                                ls.best_bid if close_maker_side == "sell" else ls.best_ask))
                            t_placed = time.time()
                            self._pending = PendingMaker(
                                client_order_index=coi,
                                order_index=None,
                                side=close_maker_side,
                                direction=self.position_direction or "",
                                qty_target=close_qty,
                                price=l_ref,
                                placed_at=t_placed,
                                deadline=t_placed + self.maker_timeout,
                                phase="close",
                            )
                            self._lighter_fill_event.clear()
                            self.state = State.CLOSING_MAKE
                            self.logger.info(
                                f"[CLOSING_MAKE] Lighter maker {close_maker_side} {close_qty}")
                        else:
                            self.logger.warning("[CLOSING] Lighter maker 失败")

                    elif (self.position_opened_at
                          and now - self.position_opened_at > self.max_hold_sec):
                        self.logger.warning(
                            f"[超时强平] 持仓 {now - self.position_opened_at:.0f}s "
                            f"> {self.max_hold_sec}s → IOC 平仓")
                        await self._emergency_flatten()
                        self._reset_position_state()
                        self.state = State.IDLE
                        last_reconcile = time.time() - 50
                        continue

                    elif (vwap_direction
                          and not data_stale
                          and vwap_spread_bps >= self.open_bps
                          and cur_pos + self.order_size <= self.max_position
                          and self._pending is None):
                        direction = self.position_direction or vwap_direction
                        if vwap_direction == direction:
                            if direction == "long_L_short_X":
                                maker_side = "buy"
                            else:
                                maker_side = "sell"
                            self.logger.info(
                                f"[加仓信号] VWAP={vwap_spread_bps:+.2f}bps "
                                f"当前={cur_pos} 加 {self.order_size}")

                            if not self.dry_run:
                                coi = await self._place_lighter_maker(
                                    maker_side, self.order_size)
                                if coi is not None:
                                    l_ref = Decimal(str(
                                        ls.best_bid if maker_side == "buy" else ls.best_ask))
                                    t_placed = time.time()
                                    self._pending = PendingMaker(
                                        client_order_index=coi,
                                        order_index=None,
                                        side=maker_side,
                                        direction=direction,
                                        qty_target=self.order_size,
                                        price=l_ref,
                                        placed_at=t_placed,
                                        deadline=t_placed + self.maker_timeout,
                                        phase="open",
                                    )
                                    self._last_open_spread = vwap_spread_bps
                                    self._lighter_fill_event.clear()
                                    self.state = State.MAKING

                # ━━━━━━ STATE: CLOSING_MAKE ━━━━━━
                elif self.state == State.CLOSING_MAKE:
                    should_cancel = False
                    cancel_reason = ""

                    if self._pending and now > self._pending.deadline:
                        should_cancel = True
                        cancel_reason = f"平仓 maker 超时 {self.maker_timeout}s"
                    elif self._pending and abs_mid_spread > self.close_bps * 3:
                        wait_sec = now - self._pending.placed_at
                        if wait_sec > 3:
                            should_cancel = True
                            cancel_reason = (
                                f"价差反弹 mid={mid_spread_bps:+.1f}bps > "
                                f"{self.close_bps * 3:.1f}bps (等了{wait_sec:.0f}s)")

                    if should_cancel and self._pending:
                        self.logger.info(f"[撤单] {cancel_reason}")
                        await self._cancel_pending_maker()
                        await asyncio.sleep(1.0)
                        if self.state == State.CLOSING_MAKE:
                            if (self._pending
                                    and self._pending.filled_qty > self._pending.hedged_qty):
                                delta = self._pending.filled_qty - self._pending.hedged_qty
                                if delta >= self.order_size * Decimal("0.01"):
                                    self._pending.hedge_qty_needed = delta
                                    self.state = State.CLOSING_HEDGE
                                    self.logger.info(
                                        f"[撤单后检查] 发现未对冲平仓成交 {delta}，转入 CLOSING_HEDGE")
                                    continue
                            self._pending = None
                            self.state = State.IN_POSITION

                # ━━━━━━ STATE: CLOSING_HEDGE ━━━━━━
                elif self.state == State.CLOSING_HEDGE:
                    if self._pending and self._pending.hedge_qty_needed > 0:
                        hedge_qty = self._pending.hedge_qty_needed

                        if self.extended_position > 0:
                            hedge_side = "sell"
                            x_ref = x_ws_bid if x_ws_bid else x_mid
                        else:
                            hedge_side = "buy"
                            x_ref = x_ws_ask if x_ws_ask else x_mid

                        self.logger.info(
                            f"[CLOSE_HEDGE] Extended IOC {hedge_side} {hedge_qty}")

                        t0 = time.time()
                        ok = await self._place_extended_ioc(hedge_side, hedge_qty, x_ref)
                        hedge_ms = (time.time() - t0) * 1000

                        if ok:
                            self._pending.hedged_qty += hedge_qty
                            self._pending.hedge_qty_needed = Decimal("0")
                            l_fill_price = self._pending.avg_price or self._pending.price

                            if self.lighter_position > 0:
                                self.lighter_position -= hedge_qty
                            else:
                                self.lighter_position += hedge_qty
                            if self.extended_position > 0:
                                self.extended_position -= hedge_qty
                            else:
                                self.extended_position += hedge_qty

                            net_bps = self._log_ref_pnl(
                                l_fill_price, x_ref,
                                self._pending.side, hedge_side)
                            self.cumulative_net_bps += net_bps

                            self.logger.info(
                                f"[平仓完成] hedge耗时={hedge_ms:.0f}ms  "
                                f"净利={net_bps:+.2f}bps  "
                                f"累积={self.cumulative_net_bps:+.2f}bps  "
                                f"L_pos={self.lighter_position} "
                                f"X_pos={self.extended_position}")

                            self._csv_row([
                                datetime.now(timezone.utc).isoformat(),
                                "CLOSE", "convergence",
                                self._pending.side, f"{l_fill_price:.2f}",
                                str(hedge_qty),
                                hedge_side, f"{x_ref:.2f}",
                                str(self._pending.filled_qty),
                                f"{mid_spread_bps:.2f}", "", f"{hedge_ms:.0f}",
                                "", f"{net_bps:.2f}",
                                f"{self.cumulative_net_bps:.2f}",
                            ])

                            if self._pending.hedged_qty >= self._pending.filled_qty:
                                self._pending = None
                                self._reset_position_state()
                                self.state = State.IDLE
                                last_reconcile = time.time() - 50
                            else:
                                self.state = State.CLOSING_MAKE
                        else:
                            self.logger.error("[CLOSE_HEDGE FAIL] 紧急 IOC 平仓")
                            await self._emergency_flatten()
                            self._pending = None
                            self._reset_position_state()
                            self.state = State.IDLE
                            last_reconcile = time.time() - 50

                if self.state in (State.MAKING, State.CLOSING_MAKE):
                    try:
                        self._lighter_fill_event.clear()
                        await asyncio.wait_for(
                            self._lighter_fill_event.wait(), timeout=self.interval)
                    except asyncio.TimeoutError:
                        pass
                else:
                    await asyncio.sleep(self.interval)

            except Exception as e:
                self.logger.error(f"主循环异常: {e}")
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(2)

    # ═══════════════════════════════════════════════
    #  P&L estimation
    # ═══════════════════════════════════════════════

    def _log_ref_pnl(self, close_l_ref: Decimal, close_x_ref: Decimal,
                     l_close_side: str, x_close_side: str) -> float:
        if not self._open_l_refs or not self._open_x_refs:
            return 0.0
        l_open_cost = sum(p * q for p, q in self._open_l_refs)
        l_open_qty = sum(q for _, q in self._open_l_refs)
        x_open_cost = sum(p * q for p, q in self._open_x_refs)
        x_open_qty = sum(q for _, q in self._open_x_refs)

        if self.position_direction == "long_L_short_X":
            l_pnl = float(close_l_ref * l_open_qty - l_open_cost)
            x_pnl = float(x_open_cost - close_x_ref * x_open_qty)
        else:
            l_pnl = float(l_open_cost - close_l_ref * l_open_qty)
            x_pnl = float(close_x_ref * x_open_qty - x_open_cost)

        total = l_pnl + x_pnl
        notional = float(max(l_open_qty, x_open_qty)) * float(close_l_ref)
        fee_bps = self.ROUND_TRIP_FEE_BPS
        fee_dollar = notional * fee_bps / 10000
        net = total - fee_dollar
        net_bps = net / notional * 10000 if notional > 0 else 0.0

        self.logger.info(
            f"[P&L估算] Lighter={l_pnl:+.3f}$ Extended={x_pnl:+.3f}$ "
            f"费用={fee_dollar:.3f}$({fee_bps:.1f}bps) "
            f"净={net:+.3f}$ ({net_bps:+.1f}bps)")
        return net_bps

    def _reset_position_state(self):
        self.position_opened_at = None
        self.position_direction = None
        self._last_open_spread = 0.0
        self._open_l_refs.clear()
        self._open_x_refs.clear()


def main():
    p = argparse.ArgumentParser(
        description="Extended + Lighter V4 Lighter-Maker + Extended-Taker 套利 (~5.8bps费用)")
    p.add_argument("-s", "--symbol", default="ETH")
    p.add_argument("--size", type=str, default="0.1", help="每笔数量")
    p.add_argument("--max-position", type=str, default="0.5", help="最大累积仓位")
    p.add_argument("--open-bps", type=float, default=8.0,
                   help="开仓 VWAP 价差阈值 (bps)")
    p.add_argument("--close-bps", type=float, default=1.0,
                   help="平仓 |mid价差| 阈值 (bps)")
    p.add_argument("--max-hold-sec", type=float, default=600.0,
                   help="最大持仓秒数")
    p.add_argument("--interval", type=float, default=0.5,
                   help="主循环间隔 (秒)")
    p.add_argument("--maker-timeout", type=float, default=10.0,
                   help="Lighter maker 最大等待秒数")
    p.add_argument("--dry-run", action="store_true", help="只看信号不下单")
    p.add_argument("--env-file", default=".env", help=".env 文件路径")
    args = p.parse_args()

    load_dotenv(args.env_file)

    if args.close_bps >= args.open_bps:
        args.close_bps = max(0.0, args.open_bps - 0.5)

    bot = LighterMakerArbBot(
        symbol=args.symbol.upper(),
        order_size=Decimal(args.size),
        max_position=Decimal(args.max_position),
        open_bps=args.open_bps,
        close_bps=args.close_bps,
        max_hold_sec=args.max_hold_sec,
        interval=args.interval,
        maker_timeout=args.maker_timeout,
        dry_run=args.dry_run,
    )
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n已中断。")


if __name__ == "__main__":
    main()
