"""
Extended + Lighter Hedge Bot V2: Lighter Maker + Extended Hedge

Strategy: Place passive post-only orders on Lighter (maker leg),
          hedge with aggressive IOC on Extended (taker leg).

Roles:
  - Lighter: maker (post-only). Benefits from no additional delay on post-only orders.
  - Extended: taker (IOC hedge). Supports IOC-style execution and WS order updates.

Architecture:
  - MD Gateway:     Lighter WS (order_book + account_orders) + Extended WS (orderbook + account)
  - Signal Engine:  Computes net edge after fees/slippage/skew/funding, produces maker quotes
  - Execution:      Lighter maker manager + Extended hedge executor
  - Risk:           Position limits, single-leg exposure timeout, funding guard, WS health
"""

import asyncio
import json
import signal
import logging
import os
import sys
import time
import requests
import argparse
import traceback
import csv
from decimal import Decimal
from typing import Tuple, Optional, Dict

from lighter.signer_client import SignerClient

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.extended import ExtendedClient
import websockets
from datetime import datetime
import pytz


# ─────────────────────────────────────────────────
# State constants
# ─────────────────────────────────────────────────
STATE_IDLE = 'IDLE'
STATE_QUOTING = 'QUOTING'
STATE_HEDGING = 'HEDGING'


class Config:
    """Simple config class to wrap dictionary for Extended client."""
    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


class HedgeBot:
    """Lighter Maker + Extended Hedge bot."""

    def __init__(
        self,
        ticker: str,
        order_quantity: Decimal,
        fill_timeout: int = 5,
        max_position: Decimal = Decimal('0'),
        safety_buffer: Decimal = Decimal('0.0008'),
        close_buffer: Decimal = Decimal('-0.0003'),
        max_hold_minutes: int = 30,
        requote_ticks: int = 2,
        max_order_age: int = 30,
        funding_guard_minutes: int = 5,
    ):
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.fill_timeout = fill_timeout
        self.max_position = max_position
        self.open_buffer = safety_buffer
        self.close_buffer = close_buffer
        self.max_hold_minutes = max_hold_minutes
        self.requote_ticks = requote_ticks
        self.max_order_age = max_order_age
        self.funding_guard_minutes = funding_guard_minutes

        # ── State machine ──
        self.state = STATE_IDLE
        self.stop_flag = False

        # ── Lighter maker order state ──
        self.lighter_maker_client_order_index: Optional[int] = None
        self.lighter_maker_order_index: Optional[int] = None
        self.lighter_maker_side: Optional[str] = None
        self.lighter_maker_price: Optional[Decimal] = None
        self.lighter_maker_size: Optional[Decimal] = None
        self.lighter_maker_placed_at: float = 0
        self.lighter_maker_filled = False
        self.lighter_maker_fill_price: Optional[Decimal] = None
        self.lighter_maker_fill_size: Optional[Decimal] = None

        # ── Extended hedge state ──
        self.hedge_sent = False
        self.hedge_retries = 0
        self.max_hedge_retries = 5
        self.extended_hedge_order_id: Optional[str] = None
        self.extended_hedge_filled = False
        self.extended_hedge_fill_price: Optional[Decimal] = None
        self.extended_hedge_fill_size: Optional[Decimal] = None

        # ── Position tracking ──
        self.lighter_position = Decimal('0')
        self.extended_position = Decimal('0')
        self.position_opened_at: Optional[float] = None
        self._last_close_log_time: float = 0

        # ── Extended client state ──
        self.extended_client = None
        self.extended_contract_id = None
        self.extended_tick_size = None
        self.extended_order_book = {'bids': {}, 'asks': {}}
        self.extended_best_bid: Optional[Decimal] = None
        self.extended_best_ask: Optional[Decimal] = None
        self.extended_order_book_ready = False

        # ── Lighter order book state ──
        self.lighter_client = None
        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_best_bid: Optional[Decimal] = None
        self.lighter_best_ask: Optional[Decimal] = None
        self.lighter_order_book_ready = False
        self.lighter_order_book_offset = 0
        self.lighter_order_book_sequence_gap = False
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_lock = asyncio.Lock()

        # ── Lighter WS task ──
        self.lighter_ws_task = None

        # ── Fill event for zero-latency notification ──
        self.fill_event = asyncio.Event()

        # ── Stale fill queue: fills from old orders that need independent hedging ──
        self._stale_fills: list = []

        # ── WS health tracking ──
        self.lighter_ws_connected = False
        self.lighter_ws_last_msg_time: float = 0
        self.extended_ws_connected = False
        self.extended_ws_last_msg_time: float = 0
        self.ws_health_timeout = 15  # seconds without message = unhealthy

        # ── Latency timestamps per cycle ──
        self.cycle_timestamps: Dict[str, float] = {}

        # ── Lighter API config ──
        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX'))
        self.api_key_index = int(os.getenv('LIGHTER_API_KEY_INDEX'))

        # ── Extended config ──
        self.extended_vault = os.getenv('EXTENDED_VAULT')
        self.extended_stark_key_private = os.getenv('EXTENDED_STARK_KEY_PRIVATE')
        self.extended_stark_key_public = os.getenv('EXTENDED_STARK_KEY_PUBLIC')
        self.extended_api_key = os.getenv('EXTENDED_API_KEY')

        # ── Logging setup ──
        os.makedirs("logs", exist_ok=True)
        self.log_filename = f"logs/ext_v2_{ticker}_hedge_mode_log.txt"
        self.csv_filename = f"logs/ext_v2_{ticker}_trades.csv"
        self.latency_csv_filename = f"logs/ext_v2_{ticker}_latency.csv"

        self._initialize_csv_file()
        self._initialize_latency_csv()

        self.logger = logging.getLogger(f"ext_v2_hedge_{ticker}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)

        file_handler = logging.FileHandler(self.log_filename)
        file_handler.setLevel(logging.INFO)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        console_handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(message)s'))

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False

    # ═════════════════════════════════════════════
    #  CSV Logging
    # ═════════════════════════════════════════════

    def _initialize_csv_file(self):
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='') as f:
                csv.writer(f).writerow([
                    'exchange', 'timestamp', 'side', 'price', 'quantity', 'role'
                ])

    def _initialize_latency_csv(self):
        if not os.path.exists(self.latency_csv_filename):
            with open(self.latency_csv_filename, 'w', newline='') as f:
                csv.writer(f).writerow([
                    'timestamp',
                    't_signal_ms', 't_maker_send_ms', 't_maker_fill_ms',
                    't_hedge_send_ms', 't_hedge_fill_ms',
                    'maker_wait_s', 'hedge_latency_ms', 'total_cycle_s',
                    'maker_side', 'maker_price', 'hedge_price',
                ])

    def log_trade_to_csv(self, exchange: str, side: str, price: str, quantity: str, role: str):
        ts = datetime.now(pytz.UTC).isoformat()
        with open(self.csv_filename, 'a', newline='') as f:
            csv.writer(f).writerow([exchange, ts, side, price, quantity, role])
        self.logger.info(f"交易已记录: {exchange} {side} {quantity} @ {price} [{role}]")

    def log_latency(self):
        ct = self.cycle_timestamps
        now = time.time()
        ts = datetime.now(pytz.UTC).isoformat()

        t_signal = ct.get('t_signal', 0)
        t_maker_send = ct.get('t_maker_send', 0)
        t_maker_fill = ct.get('t_maker_fill', 0)
        t_hedge_send = ct.get('t_hedge_send', 0)
        t_hedge_fill = ct.get('t_hedge_fill', now)

        signal_ms = (t_maker_send - t_signal) * 1000 if t_signal and t_maker_send else 0
        maker_send_ms = (t_maker_send - t_signal) * 1000 if t_signal and t_maker_send else 0
        maker_fill_ms = (t_maker_fill - t_maker_send) * 1000 if t_maker_send and t_maker_fill else 0
        hedge_send_ms = (t_hedge_send - t_maker_fill) * 1000 if t_maker_fill and t_hedge_send else 0
        hedge_fill_ms = (t_hedge_fill - t_hedge_send) * 1000 if t_hedge_send and t_hedge_fill else 0
        maker_wait_s = t_maker_fill - t_maker_send if t_maker_send and t_maker_fill else 0
        hedge_latency_ms = (t_hedge_fill - t_maker_fill) * 1000 if t_maker_fill and t_hedge_fill else 0
        total_cycle_s = t_hedge_fill - t_signal if t_signal and t_hedge_fill else 0

        try:
            with open(self.latency_csv_filename, 'a', newline='') as f:
                csv.writer(f).writerow([
                    ts,
                    f"{signal_ms:.2f}", f"{maker_send_ms:.2f}", f"{maker_fill_ms:.2f}",
                    f"{hedge_send_ms:.2f}", f"{hedge_fill_ms:.2f}",
                    f"{maker_wait_s:.3f}", f"{hedge_latency_ms:.2f}", f"{total_cycle_s:.3f}",
                    self.lighter_maker_side or '',
                    str(self.lighter_maker_fill_price or ''),
                    str(self.extended_hedge_fill_price or ''),
                ])
        except Exception as e:
            self.logger.error(f"写入延迟CSV出错: {e}")

        self.logger.info(
            f"周期延迟: 挂单等待={maker_wait_s:.3f}s "
            f"成交→对冲发送={hedge_send_ms:.1f}ms "
            f"对冲成交={hedge_fill_ms:.1f}ms "
            f"总耗时={total_cycle_s:.3f}s"
        )

    # ═════════════════════════════════════════════
    #  Shutdown / Signals
    # ═════════════════════════════════════════════

    def setup_signal_handlers(self):
        def handler(signum, frame):
            self.stop_flag = True
            self.logger.info("收到中断信号")
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    async def async_shutdown(self):
        self.stop_flag = True
        self.logger.info("正在关闭...")

        if self.lighter_client:
            try:
                self.logger.info("关闭清理: 取消Lighter所有挂单...")
                future_ts_ms = int((time.time() + 300) * 1000)
                _, _, error = await asyncio.wait_for(
                    self.lighter_client.cancel_all_orders(
                        time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                        timestamp_ms=future_ts_ms,
                    ),
                    timeout=5.0,
                )
                if error:
                    self.logger.warning(f"关闭清理: cancel_all错误: {error}")
                else:
                    self.logger.info("关闭清理: Lighter挂单已全部取消")
            except Exception as e:
                self.logger.warning(f"关闭清理: cancel_all异常: {e}")

        if self.lighter_ws_task and not self.lighter_ws_task.done():
            self.lighter_ws_task.cancel()
            await asyncio.sleep(0.1)

        if self.extended_client:
            try:
                await asyncio.wait_for(self.extended_client.disconnect(), timeout=3.0)
            except Exception:
                pass

        for h in self.logger.handlers[:]:
            try:
                h.close()
                self.logger.removeHandler(h)
            except Exception:
                pass

    # ═════════════════════════════════════════════
    #  Client Initialization
    # ═════════════════════════════════════════════

    def initialize_lighter_client(self):
        if self.lighter_client is None:
            api_key = os.getenv('API_KEY_PRIVATE_KEY')
            if not api_key:
                raise Exception("API_KEY_PRIVATE_KEY not set")
            self.lighter_client = SignerClient(
                url=self.lighter_base_url,
                account_index=self.account_index,
                api_private_keys={self.api_key_index: api_key}
            )
            err = self.lighter_client.check_client()
            if err is not None:
                raise Exception(f"Lighter CheckClient error: {err}")
            self.logger.info("Lighter 客户端已初始化")
        return self.lighter_client

    def initialize_extended_client(self):
        if not all([self.extended_vault, self.extended_stark_key_private,
                    self.extended_stark_key_public, self.extended_api_key]):
            raise ValueError("Extended env vars not fully set")
        config = Config({
            'ticker': self.ticker,
            'contract_id': '',
            'quantity': self.order_quantity,
            'tick_size': Decimal('0.01'),
            'close_order_side': 'sell'
        })
        self.extended_client = ExtendedClient(config)
        self.logger.info("Extended 客户端已初始化")
        return self.extended_client

    def get_lighter_market_config(self) -> Tuple[int, int, int, Decimal]:
        url = f"{self.lighter_base_url}/api/v1/orderBooks"
        response = requests.get(url, headers={"accept": "application/json"}, timeout=10)
        response.raise_for_status()
        data = response.json()
        for market in data.get("order_books", []):
            if market["symbol"] == self.ticker:
                price_mult = pow(10, market["supported_price_decimals"])
                tick = Decimal("1") / (Decimal("10") ** market["supported_price_decimals"])
                return (market["market_id"],
                        pow(10, market["supported_size_decimals"]),
                        price_mult, tick)
        raise Exception(f"Ticker {self.ticker} not found on Lighter")

    async def get_extended_contract_info(self) -> Tuple[str, Decimal]:
        contract_id, tick_size = await self.extended_client.get_contract_attributes()
        return contract_id, tick_size

    # ═════════════════════════════════════════════
    #  Lighter Order Book Management
    # ═════════════════════════════════════════════

    async def reset_lighter_order_book(self):
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_order_book_offset = 0
            self.lighter_order_book_sequence_gap = False
            self.lighter_snapshot_loaded = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None

    def update_lighter_order_book(self, side: str, levels: list):
        for level in levels:
            if isinstance(level, list) and len(level) >= 2:
                price, size = Decimal(level[0]), Decimal(level[1])
            elif isinstance(level, dict):
                price = Decimal(level.get("price", 0))
                size = Decimal(level.get("size", 0))
            else:
                continue
            if size > 0:
                self.lighter_order_book[side][price] = size
            else:
                self.lighter_order_book[side].pop(price, None)

    def get_lighter_best_levels(self) -> Tuple[Optional[Tuple[Decimal, Decimal]],
                                                Optional[Tuple[Decimal, Decimal]]]:
        best_bid = None
        best_ask = None
        if self.lighter_order_book["bids"]:
            p = max(self.lighter_order_book["bids"].keys())
            best_bid = (p, self.lighter_order_book["bids"][p])
        if self.lighter_order_book["asks"]:
            p = min(self.lighter_order_book["asks"].keys())
            best_ask = (p, self.lighter_order_book["asks"][p])
        return best_bid, best_ask

    async def _request_lighter_ob_refresh(self, ws, reason: str):
        """Request a fresh OB snapshot in-band (unsubscribe + subscribe)."""
        channel = f"order_book/{self.lighter_market_index}"
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_ready = False
        await ws.send(json.dumps({"type": "unsubscribe", "channel": channel}))
        await asyncio.sleep(0.2)
        await ws.send(json.dumps({"type": "subscribe", "channel": channel}))
        self.logger.info(f"Lighter 订单簿原地刷新 ({reason})")

    # ═════════════════════════════════════════════
    #  Lighter WebSocket (order book + account orders)
    # ═════════════════════════════════════════════

    def handle_lighter_maker_fill(self, order_data):
        """Called when a Lighter maker order is filled.

        Verifies the fill belongs to the currently tracked order.
        Stale fills (from canceled-but-already-filled orders) are
        queued for independent hedging to prevent single-leg exposure.
        """
        try:
            filled_base = Decimal(order_data["filled_base_amount"])
            filled_quote = Decimal(order_data["filled_quote_amount"])
            avg_price = filled_quote / filled_base if filled_base > 0 else Decimal('0')
            is_ask = order_data.get("is_ask", False)

            fill_order_idx = order_data.get("order_index")
            fill_client_id = order_data.get("client_order_id")

            is_current_order = False
            if self.lighter_maker_order_index is not None and fill_order_idx is not None:
                is_current_order = (str(fill_order_idx) == str(self.lighter_maker_order_index))
            elif self.lighter_maker_client_order_index is not None and fill_client_id is not None:
                is_current_order = (int(fill_client_id) == self.lighter_maker_client_order_index)

            if is_ask:
                self.lighter_position -= filled_base
            else:
                self.lighter_position += filled_base

            side_str = 'sell' if is_ask else 'buy'

            if is_current_order:
                if self.lighter_maker_filled:
                    self.logger.warning(
                        f"[Lighter] 追加成交(已在对冲中): {side_str} {filled_base} @ {avg_price}, "
                        f"加入修复队列")
                    self._stale_fills.append({
                        'side': side_str,
                        'size': filled_base,
                        'price': avg_price,
                    })
                else:
                    self.lighter_maker_filled = True
                    self.lighter_maker_fill_price = avg_price
                    self.lighter_maker_fill_size = filled_base
                    self.cycle_timestamps['t_maker_fill'] = time.time()
                    self.fill_event.set()
                    self.logger.info(
                        f"[Lighter] 挂单已成交: {side_str} {filled_base} @ {avg_price}")
            else:
                self.logger.warning(
                    f"[Lighter] 过期成交: {side_str} {filled_base} @ {avg_price} "
                    f"(order_idx={fill_order_idx}, 当前跟踪={self.lighter_maker_order_index})")
                self._stale_fills.append({
                    'side': side_str,
                    'size': filled_base,
                    'price': avg_price,
                })

        except Exception as e:
            self.logger.error(f"处理Lighter挂单成交出错: {e}")

    def handle_lighter_order_open(self, order_data):
        """Capture exchange-assigned order_index when our maker order opens."""
        try:
            client_id = order_data.get("client_order_id")
            if client_id is not None and int(client_id) == self.lighter_maker_client_order_index:
                new_idx = order_data.get("order_index")
                if new_idx != self.lighter_maker_order_index:
                    self.lighter_maker_order_index = new_idx
                    self.logger.info(
                        f"[Lighter] 挂单已上链, order_index={self.lighter_maker_order_index}")
        except Exception as e:
            self.logger.error(f"处理Lighter订单上链出错: {e}")

    def handle_lighter_order_canceled(self, order_data):
        """Handle cancellation of a maker order.

        Always processes non-zero fills regardless of order ID, because
        requoting may change the tracked ID before the cancel arrives.
        """
        try:
            filled_base = Decimal(order_data.get("filled_base_amount", "0"))
            if filled_base > 0:
                self.handle_lighter_maker_fill(order_data)
                return

            client_id = order_data.get("client_order_id")
            if client_id is not None and int(client_id) == self.lighter_maker_client_order_index:
                self.lighter_maker_filled = False
                self.state = STATE_IDLE
        except Exception as e:
            self.logger.error(f"处理Lighter订单撤销出错: {e}")

    async def handle_lighter_ws(self):
        """Lighter WebSocket: order book + account orders."""
        url = "wss://mainnet.zklighter.elliot.ai/stream"

        while not self.stop_flag:
            timeout_count = 0
            try:
                await self.reset_lighter_order_book()
                async with websockets.connect(url) as ws:
                    # Subscribe to order book
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": f"order_book/{self.lighter_market_index}"
                    }))

                    # Subscribe to account orders (authenticated)
                    try:
                        auth_token, err = self.lighter_client.create_auth_token_with_expiry(
                            api_key_index=self.api_key_index)
                        if err is None:
                            await ws.send(json.dumps({
                                "type": "subscribe",
                                "channel": f"account_orders/{self.lighter_market_index}/{self.account_index}",
                                "auth": auth_token
                            }))
                            self.logger.info("已订阅 Lighter 账户订单")
                        else:
                            self.logger.warning(f"创建Lighter认证令牌失败: {err}")
                    except Exception as e:
                        self.logger.warning(f"订阅Lighter账户订单出错: {e}")

                    ob_refresh_time = time.time()
                    while not self.stop_flag:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)
                            data = json.loads(msg)
                            timeout_count = 0
                            self.lighter_ws_connected = True
                            self.lighter_ws_last_msg_time = time.time()

                            msg_type = data.get("type", "")

                            # Account orders: processed OUTSIDE the OB lock
                            # for zero-latency fill detection.
                            if msg_type == "update/account_orders":
                                orders = data.get("orders", {}).get(
                                    str(self.lighter_market_index), [])
                                for od in orders:
                                    status = od.get("status", "")
                                    if status == "filled":
                                        self.handle_lighter_maker_fill(od)
                                    elif status == "open":
                                        self.handle_lighter_order_open(od)
                                    elif status == "canceled":
                                        self.handle_lighter_order_canceled(od)
                                continue

                            if msg_type == "ping":
                                await ws.send(json.dumps({"type": "pong"}))
                                continue

                            async with self.lighter_order_book_lock:
                                if msg_type == "subscribed/order_book":
                                    self.lighter_order_book["bids"].clear()
                                    self.lighter_order_book["asks"].clear()
                                    ob = data.get("order_book", {})
                                    if ob and "offset" in ob:
                                        self.lighter_order_book_offset = ob["offset"]
                                    self.update_lighter_order_book("bids", ob.get("bids", []))
                                    self.update_lighter_order_book("asks", ob.get("asks", []))
                                    self.lighter_snapshot_loaded = True
                                    self.lighter_order_book_ready = True
                                    bb, ba = self.get_lighter_best_levels()
                                    if bb:
                                        self.lighter_best_bid = bb[0]
                                    if ba:
                                        self.lighter_best_ask = ba[0]
                                    self.logger.info(
                                        f"Lighter 订单簿快照: {len(self.lighter_order_book['bids'])}买, "
                                        f"{len(self.lighter_order_book['asks'])}卖, "
                                        f"bid={self.lighter_best_bid} ask={self.lighter_best_ask}")

                                elif msg_type == "update/order_book" and self.lighter_snapshot_loaded:
                                    ob = data.get("order_book", {})
                                    if not ob or "offset" not in ob:
                                        continue
                                    new_offset = ob["offset"]
                                    if new_offset <= self.lighter_order_book_offset:
                                        self.logger.warning(
                                            f"Lighter 订单簿offset过期/重复: 当前={self.lighter_order_book_offset} "
                                            f"收到={new_offset}")
                                        continue
                                    self.lighter_order_book_offset = new_offset
                                    self.update_lighter_order_book("bids", ob.get("bids", []))
                                    self.update_lighter_order_book("asks", ob.get("asks", []))
                                    bb, ba = self.get_lighter_best_levels()
                                    if bb:
                                        self.lighter_best_bid = bb[0]
                                    if ba:
                                        self.lighter_best_ask = ba[0]
                                    if (self.lighter_best_bid and self.lighter_best_ask
                                            and self.lighter_best_bid >= self.lighter_best_ask):
                                        self.logger.warning(
                                            f"Lighter 订单簿交叉: 买一={self.lighter_best_bid} "
                                            f">= 卖一={self.lighter_best_ask}")

                            if (self.lighter_best_bid and self.lighter_best_ask
                                    and self.lighter_best_bid >= self.lighter_best_ask):
                                await self._request_lighter_ob_refresh(ws, "crossed book")
                                ob_refresh_time = time.time()
                            elif time.time() - ob_refresh_time > 15:
                                await self._request_lighter_ob_refresh(ws, "periodic 15s")
                                ob_refresh_time = time.time()

                        except asyncio.TimeoutError:
                            timeout_count += 1
                            if timeout_count % 10 == 0:
                                self.logger.warning(
                                    f"Lighter WS 无消息已 {timeout_count}秒")
                            if time.time() - ob_refresh_time > 15:
                                try:
                                    await self._request_lighter_ob_refresh(ws, "periodic 15s")
                                    ob_refresh_time = time.time()
                                except Exception:
                                    break
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            self.logger.warning("Lighter WS 连接断开")
                            self.lighter_ws_connected = False
                            break
                        except Exception as e:
                            self.logger.error(f"Lighter WS 错误: {e}")
                            self.lighter_ws_connected = False
                            break
            except Exception as e:
                self.logger.error(f"Lighter WS 连接失败: {e}")

            self.lighter_ws_connected = False
            if not self.stop_flag:
                await asyncio.sleep(2)

    # ═════════════════════════════════════════════
    #  Extended WebSocket (order book + account)
    # ═════════════════════════════════════════════

    def handle_extended_order_book_update(self, message):
        """Handle Extended order book WS messages.

        For depth=1 subscription, every SNAPSHOT and DELTA carries the
        current top-of-book.  We clear and rebuild on EVERY message to
        prevent stale price levels from accumulating (the root cause of
        the earlier "reverse-arb" bug where extended_best_bid was too high).
        """
        try:
            if isinstance(message, str):
                message = json.loads(message)
            if message.get("type") not in ["SNAPSHOT", "DELTA"]:
                return
            data = message.get("data", {})
            if not data:
                return

            self.extended_ws_connected = True
            self.extended_ws_last_msg_time = time.time()

            self.extended_order_book['bids'].clear()
            self.extended_order_book['asks'].clear()

            for bid in data.get('b', []):
                if isinstance(bid, dict):
                    p, q = Decimal(bid.get('p', '0')), Decimal(bid.get('q', '0'))
                else:
                    p, q = Decimal(bid[0]), Decimal(bid[1])
                if q > 0:
                    self.extended_order_book['bids'][p] = q

            for ask in data.get('a', []):
                if isinstance(ask, dict):
                    p, q = Decimal(ask.get('p', '0')), Decimal(ask.get('q', '0'))
                else:
                    p, q = Decimal(ask[0]), Decimal(ask[1])
                if q > 0:
                    self.extended_order_book['asks'][p] = q

            if self.extended_order_book['bids']:
                self.extended_best_bid = max(self.extended_order_book['bids'].keys())
            else:
                self.extended_best_bid = None
            if self.extended_order_book['asks']:
                self.extended_best_ask = min(self.extended_order_book['asks'].keys())
            else:
                self.extended_best_ask = None

            if not self.extended_order_book_ready and self.extended_best_bid and self.extended_best_ask:
                self.extended_order_book_ready = True
                self.logger.info(
                    f"Extended 订单簿就绪: 买一={self.extended_best_bid} 卖一={self.extended_best_ask}")

        except Exception as e:
            self.logger.error(f"Extended 订单簿更新出错: {e}")

    def handle_extended_order_update(self, order_data):
        """Handle Extended order status updates from WS (for hedge tracking)."""
        try:
            if order_data.get('contract_id') != self.extended_contract_id:
                return

            order_id = order_data.get('order_id')
            status = order_data.get('status')
            side = order_data.get('side', '').lower()
            filled_size = Decimal(order_data.get('filled_size', '0'))
            fill_price = order_data.get('avg_price') or order_data.get('price', '0')

            self.logger.info(f"[Extended] [{status}] {side} {filled_size} @ {fill_price}")

            if status == 'FILLED' and order_id == self.extended_hedge_order_id:
                if side == 'buy':
                    self.extended_position += filled_size
                else:
                    self.extended_position -= filled_size

                self.extended_hedge_filled = True
                self.extended_hedge_fill_price = Decimal(str(fill_price))
                self.extended_hedge_fill_size = filled_size
                self.cycle_timestamps['t_hedge_fill'] = time.time()

                self.log_trade_to_csv(
                    'Extended', side, str(fill_price), str(filled_size), 'hedge')

            elif status in ['CANCELED', 'CANCELLED'] and order_id == self.extended_hedge_order_id:
                if filled_size > 0:
                    if side == 'buy':
                        self.extended_position += filled_size
                    else:
                        self.extended_position -= filled_size
                    self.extended_hedge_filled = True
                    self.extended_hedge_fill_price = Decimal(str(fill_price))
                    self.extended_hedge_fill_size = filled_size
                    self.cycle_timestamps['t_hedge_fill'] = time.time()
                    self.logger.warning(
                        f"[Extended] 对冲部分成交后被撤销: {filled_size} @ {fill_price}")
                    self.log_trade_to_csv(
                        'Extended', side, str(fill_price), str(filled_size), 'hedge_partial')
                else:
                    self.logger.error("[Extended] 对冲订单被撤销, 成交量为0!")

        except Exception as e:
            self.logger.error(f"处理Extended订单更新出错: {e}")

    async def setup_extended_websocket(self):
        """Setup Extended WS for account + order book."""
        if not self.extended_client:
            raise Exception("Extended client not initialized")

        def order_update_handler(order_data):
            order_id = order_data.get('order_id')
            status = order_data.get('status')
            side = order_data.get('side', '').lower()

            if status == "NEW":
                status = "OPEN"
            elif status == "CANCELLED":
                status = "CANCELED"

            self.handle_extended_order_update({
                'order_id': order_id,
                'side': side,
                'status': status,
                'size': order_data.get('size'),
                'price': order_data.get('price'),
                'avg_price': order_data.get('avg_price'),
                'contract_id': order_data.get('contract_id'),
                'filled_size': order_data.get('filled_size', '0')
            })

        self.extended_client.setup_order_update_handler(order_update_handler)
        await self.extended_client.connect()
        self.logger.info("Extended 账户WS已连接")

        await self._start_extended_depth_ws()

    async def _start_extended_depth_ws(self):
        """Start Extended order book depth WS in background."""
        market_name = f"{self.ticker}-USD"
        url = (f"wss://api.starknet.extended.exchange/stream.extended.exchange"
               f"/v1/orderbooks/{market_name}?depth=1")

        async def depth_loop():
            while not self.stop_flag:
                try:
                    async with websockets.connect(url) as ws:
                        self.logger.info(f"Extended 深度WS已连接: {market_name}")
                        async for message in ws:
                            if self.stop_flag:
                                break
                            try:
                                data = json.loads(message)
                                if data.get("type") in ["SNAPSHOT", "DELTA"]:
                                    self.handle_extended_order_book_update(data)
                            except Exception as e:
                                self.logger.error(f"Extended 深度消息出错: {e}")
                except websockets.exceptions.ConnectionClosed:
                    self.logger.warning("Extended 深度WS断开, 重连中...")
                    self.extended_ws_connected = False
                except Exception as e:
                    self.logger.error(f"Extended 深度WS错误: {e}")
                    self.extended_ws_connected = False
                if not self.stop_flag:
                    await asyncio.sleep(2)

        asyncio.create_task(depth_loop())

    # ═════════════════════════════════════════════
    #  Signal Engine
    # ═════════════════════════════════════════════

    def compute_signal(self) -> Optional[Tuple[str, Decimal, Decimal]]:
        """Compute maker signal with asymmetric open/close thresholds.

        Opening uses open_buffer (higher) to capture enough spread for the
        full round trip. Closing uses close_buffer (lower) to unwind quickly.
        Close signals always take priority over open signals.
        """
        if (self.lighter_best_bid is None or self.lighter_best_ask is None or
                self.extended_best_bid is None or self.extended_best_ask is None):
            return None

        lighter_mid = (self.lighter_best_bid + self.lighter_best_ask) / 2
        extended_mid = (self.extended_best_bid + self.extended_best_ask) / 2
        mid_divergence = abs(lighter_mid - extended_mid) / lighter_mid
        if mid_divergence > Decimal('0.003'):
            self.logger.warning(
                f"价格偏离过大: Lighter中间价={lighter_mid:.2f} "
                f"Extended中间价={extended_mid:.2f} 偏离度={mid_divergence:.4f}")
            return None

        has_position = (self.lighter_position != 0)

        minutes_in_hour = datetime.now(pytz.UTC).minute
        if (minutes_in_hour >= (60 - self.funding_guard_minutes) or
                minutes_in_hour < self.funding_guard_minutes):
            if not has_position:
                now = time.time()
                if now - getattr(self, '_last_funding_log', 0) > 30:
                    self._last_funding_log = now
                    self.logger.info(
                        f"[信号] funding guard: UTC分钟={minutes_in_hour}, "
                        f"无仓位暂停开仓 (每小时前后{self.funding_guard_minutes}分钟)")
                return None

        skew = Decimal('0')
        if self.max_position > 0:
            net_pos = self.lighter_position + self.extended_position
            skew = net_pos / self.max_position

        price_ref = lighter_mid

        close_side = None
        position_abs = abs(self.lighter_position)
        if self.lighter_position > 0 and position_abs >= self.order_quantity * Decimal('0.01'):
            close_side = 'sell'
        elif self.lighter_position < 0 and position_abs >= self.order_quantity * Decimal('0.01'):
            close_side = 'buy'

        effective_close_buffer = self.close_buffer
        if close_side and self.position_opened_at:
            hold_min = (time.time() - self.position_opened_at) / 60
            if hold_min > self.max_hold_minutes:
                decay = min(Decimal('1'), Decimal(str(
                    (hold_min - self.max_hold_minutes) / 10)))
                decay_target = -self.open_buffer
                effective_close_buffer = (
                    self.close_buffer + decay * (decay_target - self.close_buffer))

        open_safety = price_ref * self.open_buffer
        close_safety = price_ref * effective_close_buffer

        close_qty = min(self.order_quantity, position_abs) if close_side else self.order_quantity

        # ── BUY on Lighter (sell hedge on Extended) ──
        buy_is_close = (close_side == 'buy')
        buy_safety = close_safety if buy_is_close else open_safety
        skew_adj = skew * open_safety

        buy_target = self.extended_best_bid - buy_safety
        buy_target -= skew_adj
        buy_price = min(buy_target, self.lighter_best_ask - self.tick_size)
        buy_edge = self.extended_best_bid - buy_price - buy_safety

        # ── SELL on Lighter (buy hedge on Extended) ──
        sell_is_close = (close_side == 'sell')
        sell_safety = close_safety if sell_is_close else open_safety

        sell_target = self.extended_best_ask + sell_safety
        sell_target += skew_adj
        sell_price = max(sell_target, self.lighter_best_bid + self.tick_size)
        sell_edge = sell_price - self.extended_best_ask - sell_safety

        can_buy = self.max_position <= 0 or (self.lighter_position < self.max_position)
        can_sell = self.max_position <= 0 or (self.lighter_position > -self.max_position)

        # Priority 1: closing signals
        if sell_is_close and sell_edge > 0 and can_sell:
            rounded = self._round_lighter_price(sell_price)
            if rounded > 0:
                now = time.time()
                if now - self._last_close_log_time >= 10:
                    hold_str = ""
                    if self.position_opened_at:
                        hold_str = f" hold={((now - self.position_opened_at) / 60):.1f}min"
                    self.logger.info(
                        f"[平仓] 卖出价差=${sell_edge:.4f} "
                        f"平仓安全距离=${sell_safety:.4f} "
                        f"数量={close_qty}{hold_str}")
                    self._last_close_log_time = now
                return ('sell', rounded, close_qty)

        if buy_is_close and buy_edge > 0 and can_buy:
            rounded = self._round_lighter_price(buy_price)
            if rounded > 0:
                now = time.time()
                if now - self._last_close_log_time >= 10:
                    hold_str = ""
                    if self.position_opened_at:
                        hold_str = f" hold={((now - self.position_opened_at) / 60):.1f}min"
                    self.logger.info(
                        f"[平仓] 买入价差=${buy_edge:.4f} "
                        f"平仓安全距离=${buy_safety:.4f} "
                        f"数量={close_qty}{hold_str}")
                    self._last_close_log_time = now
                return ('buy', rounded, close_qty)

        # When held too long, block all new opens and focus on closing
        if close_side is not None and self.position_opened_at:
            hold_min = (time.time() - self.position_opened_at) / 60
            if hold_min > self.max_hold_minutes:
                now = time.time()
                if now - getattr(self, '_last_closewait_log', 0) > 30:
                    self._last_closewait_log = now
                    self.logger.info(
                        f"[信号] 超时平仓模式: close_side={close_side} "
                        f"pos={self.lighter_position} hold={hold_min:.1f}min "
                        f"sell_edge={sell_edge:.4f} buy_edge={buy_edge:.4f}")
                return None

        # Priority 2: opening signals (same-direction accumulation)
        best_signal = None

        if buy_edge > 0 and can_buy and not buy_is_close:
            rounded = self._round_lighter_price(buy_price)
            if rounded > 0:
                best_signal = ('buy', rounded, self.order_quantity)

        if sell_edge > 0 and can_sell and not sell_is_close:
            rounded = self._round_lighter_price(sell_price)
            if rounded > 0:
                if best_signal is None or sell_edge > buy_edge:
                    best_signal = ('sell', rounded, self.order_quantity)

        if best_signal is None:
            now = time.time()
            if now - getattr(self, '_last_nosig_log', 0) > 30:
                self._last_nosig_log = now
                self.logger.info(
                    f"[信号] 无开仓机会: buy_edge={buy_edge:.4f} sell_edge={sell_edge:.4f} "
                    f"can_buy={can_buy} can_sell={can_sell}")

        return best_signal

    def _round_lighter_price(self, price: Decimal) -> Decimal:
        if self.tick_size and self.tick_size > 0:
            return (price / self.tick_size).quantize(Decimal('1')) * self.tick_size
        return price

    # ═════════════════════════════════════════════
    #  Execution: Lighter Maker
    # ═════════════════════════════════════════════

    async def place_lighter_maker_order(self, side: str, quantity: Decimal, price: Decimal):
        """Place a post-only limit order on Lighter."""
        is_ask = (side.lower() == 'sell')
        client_order_index = int(time.time() * 1000)

        self.lighter_maker_client_order_index = client_order_index
        self.lighter_maker_order_index = None
        self.lighter_maker_side = side
        self.lighter_maker_price = price
        self.lighter_maker_size = quantity
        self.lighter_maker_placed_at = time.time()
        self.lighter_maker_filled = False
        self.lighter_maker_fill_price = None
        self.lighter_maker_fill_size = None

        self.cycle_timestamps['t_maker_send'] = time.time()

        try:
            tx, tx_hash, error = await self.lighter_client.create_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=int(quantity * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=False,
                trigger_price=0,
            )
            if error is not None:
                raise Exception(f"Lighter create_order error: {error}")

            self.logger.info(
                f"[Lighter] 挂单已发送: {side} {quantity} @ {price} "
                f"(client_id={client_order_index})")

        except Exception as e:
            self.logger.error(f"Lighter下单出错: {e}")
            self.state = STATE_IDLE

    async def cancel_lighter_maker_order(self):
        """Cancel the current Lighter maker order."""
        order_idx = self.lighter_maker_order_index
        if order_idx is None:
            self.logger.warning("无order_index可撤, 使用client_order_index回退")
            order_idx = self.lighter_maker_client_order_index
        if order_idx is None:
            return

        try:
            _, _, error = await self.lighter_client.cancel_order(
                market_index=self.lighter_market_index,
                order_index=order_idx,
            )
            if error is not None:
                self.logger.error(f"Lighter撤单错误: {error}")
            else:
                self.logger.info(f"[Lighter] 挂单已撤销 (idx={order_idx})")
        except Exception as e:
            self.logger.error(f"Lighter撤单出错: {e}")

    def should_requote(self) -> bool:
        """Check if current maker order price has drifted too far from optimal."""
        if self.lighter_maker_price is None:
            return False

        new_signal = self.compute_signal()
        if new_signal is None:
            return True

        new_side, new_price, _ = new_signal
        if new_side != self.lighter_maker_side:
            return True

        price_diff = abs(new_price - self.lighter_maker_price)
        if price_diff > self.tick_size * self.requote_ticks:
            return True

        return False

    # ═════════════════════════════════════════════
    #  Execution: Extended Hedge
    # ═════════════════════════════════════════════

    async def place_extended_hedge(self, side: str, quantity: Decimal):
        """Place aggressive IOC-style order on Extended to hedge Lighter fill."""
        if not self.extended_client:
            raise Exception("Extended client not initialized")

        if side.lower() == 'buy':
            if not self.extended_best_ask or self.extended_best_ask <= 0:
                self.logger.error("无法对冲: Extended无卖价")
                return
            aggressive_price = self.extended_best_ask * Decimal('1.005')
        else:
            if not self.extended_best_bid or self.extended_best_bid <= 0:
                self.logger.error("无法对冲: Extended无买价")
                return
            aggressive_price = self.extended_best_bid * Decimal('0.995')

        self.cycle_timestamps['t_hedge_send'] = time.time()

        result = await self.extended_client.place_ioc_order(
            contract_id=self.extended_contract_id,
            quantity=quantity,
            side=side,
            aggressive_price=aggressive_price,
        )

        # Log AFTER the API call to keep the hot path clean
        if result.success:
            self.extended_hedge_order_id = result.order_id
            self.hedge_sent = True
            self.logger.info(
                f"[Extended] 对冲已发送: {side} {quantity} @ ~{aggressive_price:.2f} "
                f"id={result.order_id}")
        else:
            self.logger.error(
                f"[Extended] 对冲失败: {side} {quantity} @ ~{aggressive_price:.2f} "
                f"err={result.error_message}")

    # ═════════════════════════════════════════════
    #  Position Management
    # ═════════════════════════════════════════════

    async def get_lighter_position_from_api(self) -> Decimal:
        """Fetch actual Lighter position from REST API."""
        url = f"{self.lighter_base_url}/api/v1/account"
        params = {"by": "index", "value": self.account_index}
        try:
            resp = requests.get(url, headers={"accept": "application/json"},
                                params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            for pos in data.get('accounts', [{}])[0].get('positions', []):
                if pos.get('symbol') == self.ticker:
                    return Decimal(pos['position']) * pos['sign']
            return Decimal('0')
        except Exception as e:
            self.logger.error(f"获取Lighter仓位出错: {e}")
            return self.lighter_position

    async def get_extended_signed_position(self) -> Decimal:
        """Get Extended position with sign preserved (long=positive, short=negative)."""
        try:
            positions_data = await self.extended_client.perpetual_trading_client.account.get_positions(
                market_names=[f"{self.ticker}-USD"])
            if not positions_data or not hasattr(positions_data, 'data') or not positions_data.data:
                return Decimal('0')
            for p in positions_data.data:
                if p.market == self.extended_contract_id:
                    size = Decimal(str(p.size))
                    if str(p.side).upper() == 'SHORT':
                        return -size
                    return size
            return Decimal('0')
        except Exception as e:
            self.logger.error(f"获取Extended仓位出错: {e}")
            return self.extended_position

    async def check_position_balance(self, log: bool = True) -> bool:
        """Verify positions are within tolerance."""
        lighter_pos = await self.get_lighter_position_from_api()
        extended_pos = await self.get_extended_signed_position()

        self.lighter_position = lighter_pos
        self.extended_position = extended_pos

        net = abs(self.lighter_position + self.extended_position)
        if log:
            self.logger.info(
                f"仓位: Lighter={self.lighter_position} Extended={self.extended_position} "
                f"净值={net}")

        self._update_position_timer()

        if net > self.order_quantity * 2:
            self.logger.error(f"仓位不平衡过大: {net}")
            return False
        return True

    # ═════════════════════════════════════════════
    #  Risk Control
    # ═════════════════════════════════════════════

    def is_ws_healthy(self) -> bool:
        """Check if both WebSocket connections are alive and receiving data."""
        now = time.time()
        lighter_ok = (self.lighter_ws_connected and
                      now - self.lighter_ws_last_msg_time < self.ws_health_timeout)
        extended_ok = (self.extended_ws_connected and
                       now - self.extended_ws_last_msg_time < self.ws_health_timeout)

        if not lighter_ok:
            self.logger.warning(
                f"Lighter WS异常: 连接={self.lighter_ws_connected} "
                f"上次消息={now - self.lighter_ws_last_msg_time:.1f}秒前")
        if not extended_ok:
            self.logger.warning(
                f"Extended WS异常: 连接={self.extended_ws_connected} "
                f"上次消息={now - self.extended_ws_last_msg_time:.1f}秒前")

        return lighter_ok and extended_ok

    async def emergency_cancel_all(self):
        """Cancel all Lighter maker orders due to WS disconnect or risk event."""
        self.logger.warning("风控: 紧急撤销所有Lighter订单")
        if self.state == STATE_QUOTING and self.lighter_maker_client_order_index:
            await self.cancel_lighter_maker_order()
        self.reset_cycle()
        self.state = STATE_IDLE

    async def repair_single_leg_exposure(self):
        """When hedge fails/times out, send reduce-only on Extended to flatten."""
        if not self.lighter_maker_filled or not self.lighter_maker_fill_size:
            return

        repair_side = 'sell' if self.lighter_maker_side == 'buy' else 'buy'
        repair_size = self.lighter_maker_fill_size

        self.logger.warning(
            f"风控: 修复单腿暴露. "
            f"发送reduce-only {repair_side} {repair_size} 到Extended")

        if repair_side == 'buy':
            if not self.extended_best_ask or self.extended_best_ask <= 0:
                self.logger.error("风控: 无法修复 - Extended无卖价")
                return
            aggressive_price = self.extended_best_ask * Decimal('1.01')
        else:
            if not self.extended_best_bid or self.extended_best_bid <= 0:
                self.logger.error("风控: 无法修复 - Extended无买价")
                return
            aggressive_price = self.extended_best_bid * Decimal('0.99')

        try:
            result = await self.extended_client.place_ioc_order(
                contract_id=self.extended_contract_id,
                quantity=repair_size,
                side=repair_side,
                aggressive_price=aggressive_price,
            )
            if result.success:
                self.logger.warning(
                    f"风控: 修复订单已发送到Extended: id={result.order_id}")
                # Wait briefly for fill confirmation via WS
                await asyncio.sleep(3)
            else:
                self.logger.error(
                    f"风控: 修复订单失败: {result.error_message}")
        except Exception as e:
            self.logger.error(f"风控: 发送修复订单出错: {e}")

    # ═════════════════════════════════════════════
    #  Cycle Reset
    # ═════════════════════════════════════════════

    def reset_cycle(self):
        """Reset all per-cycle state."""
        self.lighter_maker_client_order_index = None
        self.lighter_maker_order_index = None
        self.lighter_maker_side = None
        self.lighter_maker_price = None
        self.lighter_maker_size = None
        self.lighter_maker_placed_at = 0
        self.lighter_maker_filled = False
        self.lighter_maker_fill_price = None
        self.lighter_maker_fill_size = None

        self.hedge_sent = False
        self.hedge_retries = 0
        self.extended_hedge_order_id = None
        self.extended_hedge_filled = False
        self.extended_hedge_fill_price = None
        self.extended_hedge_fill_size = None

        self.fill_event.clear()
        if hasattr(self, '_fill_logged'):
            del self._fill_logged

        self.cycle_timestamps = {}

    def _update_position_timer(self):
        """Track when position first opens; clear when it returns to zero."""
        has_pos = (self.lighter_position != 0)
        if has_pos and self.position_opened_at is None:
            self.position_opened_at = time.time()
            self.logger.info(
                f"仓位建立: Lighter={self.lighter_position}")
        elif not has_pos and self.position_opened_at is not None:
            hold_min = (time.time() - self.position_opened_at) / 60
            self.logger.info(
                f"仓位平仓 (持有{hold_min:.1f}分钟)")
            self.position_opened_at = None

    # ═════════════════════════════════════════════
    #  Main Trading Loop
    # ═════════════════════════════════════════════

    async def trading_loop(self):
        self.logger.info(f"启动 ext_v2 对冲机器人: {self.ticker}")

        # ── Initialize clients ──
        try:
            self.initialize_lighter_client()
            self.initialize_extended_client()

            self.extended_contract_id, self.extended_tick_size = \
                await self.get_extended_contract_info()
            self.lighter_market_index, self.base_amount_multiplier, \
                self.price_multiplier, self.tick_size = self.get_lighter_market_config()

            self.logger.info(
                f"合约: Extended={self.extended_contract_id} "
                f"Lighter市场={self.lighter_market_index}")
        except Exception as e:
            self.logger.error(f"初始化失败: {e}")
            return

        # ── Setup WebSockets ──
        try:
            await self.setup_extended_websocket()
            self.logger.info("等待Extended订单簿数据...")
            t0 = time.time()
            while not self.extended_order_book_ready and not self.stop_flag:
                if time.time() - t0 > 15:
                    self.logger.warning("等待Extended订单簿超时")
                    break
                await asyncio.sleep(0.5)
        except Exception as e:
            self.logger.error(f"Extended WS设置失败: {e}")
            return

        try:
            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())
            self.logger.info("等待Lighter订单簿数据...")
            t0 = time.time()
            while not self.lighter_order_book_ready and not self.stop_flag:
                if time.time() - t0 > 15:
                    self.logger.warning("等待Lighter订单簿超时")
                    break
                await asyncio.sleep(0.5)
        except Exception as e:
            self.logger.error(f"Lighter WS设置失败: {e}")
            return

        # ── Cancel any leftover Lighter orders from previous session ──
        try:
            self.logger.info("启动清理: 取消Lighter所有残留挂单...")
            future_ts_ms = int((time.time() + 300) * 1000)
            _, _, error = await self.lighter_client.cancel_all_orders(
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                timestamp_ms=future_ts_ms,
            )
            if error:
                self.logger.warning(f"启动清理: cancel_all_orders返回错误: {error}")
            else:
                self.logger.info("启动清理: Lighter残留挂单已全部取消")
        except Exception as e:
            self.logger.warning(f"启动清理: cancel_all_orders异常(可忽略): {e}")
        await asyncio.sleep(2)

        # ── Initial position check ──
        if not await self.check_position_balance():
            self.logger.error("初始仓位不平衡, 中止启动")
            return

        await asyncio.sleep(3)
        self.logger.info("机器人就绪, 进入主循环")

        last_position_check = time.time()
        last_log = time.time()

        # ── Main loop ──
        while not self.stop_flag:
            try:
                # Periodic position balance check
                now = time.time()
                if now - last_position_check > 30:
                    if not await self.check_position_balance(log=(now - last_log > 10)):
                        self.logger.error("检测到仓位不平衡, 停止运行")
                        break
                    last_position_check = now
                    if now - last_log > 10:
                        last_log = now

                # WS health check: if unhealthy, cancel orders and wait
                if not self.is_ws_healthy():
                    if self.state == STATE_QUOTING:
                        self.logger.warning("风控: WS异常, 紧急撤单")
                        await self.emergency_cancel_all()
                    elif self.state == STATE_HEDGING:
                        pass  # don't cancel mid-hedge, let it resolve
                    await asyncio.sleep(1)
                    continue

                # Drain stale fills
                while self._stale_fills and self.state == STATE_IDLE:
                    sf = self._stale_fills.pop(0)
                    hedge_side = 'sell' if sf['side'] == 'buy' else 'buy'
                    if sf['size'] < self.order_quantity:
                        self.logger.info(
                            f"[修复] 过期小额成交 {sf['side']} {sf['size']} @ {sf['price']}, "
                            f"仓位已调整, 由正常交易周期自然平仓")
                        continue
                    self.logger.warning(
                        f"[修复] 对冲过期成交: {sf['side']} {sf['size']} @ {sf['price']} "
                        f"→ 发送{hedge_side}到Extended")
                    await self.place_extended_hedge(hedge_side, sf['size'])
                    if self.hedge_sent:
                        self.log_trade_to_csv(
                            'Lighter', sf['side'], str(sf['price']),
                            str(sf['size']), 'stale_maker')
                        self.hedge_sent = False
                        self.extended_hedge_order_id = None

                # Data readiness check
                if not (self.lighter_order_book_ready and self.extended_order_book_ready):
                    await asyncio.sleep(0.1)
                    continue

                # ────────────────── STATE: IDLE ──────────────────
                if self.state == STATE_IDLE:
                    sig = self.compute_signal()
                    if sig:
                        side, price, size = sig
                        self.cycle_timestamps['t_signal'] = time.time()
                        await self.place_lighter_maker_order(side, size, price)
                        self.state = STATE_QUOTING
                    else:
                        if (self.lighter_best_bid and self.extended_best_bid and
                                now - getattr(self, '_last_debug_log', 0) > 30):
                            self._last_debug_log = now
                            price_ref = (self.lighter_best_bid + self.lighter_best_ask) / 2
                            long_spread = self.extended_best_bid - self.lighter_best_ask
                            short_spread = self.lighter_best_bid - self.extended_best_ask
                            threshold = price_ref * self.open_buffer
                            self.logger.info(
                                f"[监控] L买={self.lighter_best_bid} L卖={self.lighter_best_ask} "
                                f"E买={self.extended_best_bid} E卖={self.extended_best_ask} | "
                                f"做多(E买-L卖)={long_spread:.2f} "
                                f"做空(L买-E卖)={short_spread:.2f} "
                                f"阈值={threshold:.2f}")
                        await asyncio.sleep(0.5)
                    continue

                # ────────────────── STATE: QUOTING ──────────────────
                elif self.state == STATE_QUOTING:
                    if self.lighter_maker_filled:
                        self.fill_event.clear()
                        self.state = STATE_HEDGING
                        continue

                    # Check order age
                    age = time.time() - self.lighter_maker_placed_at
                    if age > self.max_order_age:
                        self.logger.info(
                            f"挂单超时 ({age:.1f}秒), 撤销中")
                        await self.cancel_lighter_maker_order()
                        self.reset_cycle()
                        self.state = STATE_IDLE
                        continue

                    if age > 2.0 and self.should_requote():
                        self.logger.info("价格偏移, 重新报价")
                        await self.cancel_lighter_maker_order()
                        self.reset_cycle()
                        self.state = STATE_IDLE
                        continue

                    # Wait for fill event instead of polling (0 latency vs 10ms)
                    try:
                        await asyncio.wait_for(self.fill_event.wait(), timeout=0.5)
                    except asyncio.TimeoutError:
                        pass

                # ────────────────── STATE: HEDGING ──────────────────
                elif self.state == STATE_HEDGING:
                    if self.lighter_maker_filled and not hasattr(self, '_fill_logged'):
                        side_str = "sell" if self.lighter_maker_side == 'sell' else "buy"
                        self.log_trade_to_csv(
                            'Lighter', side_str,
                            str(self.lighter_maker_fill_price),
                            str(self.lighter_maker_fill_size), 'maker')
                        self._fill_logged = True

                    if not self.hedge_sent:
                        if self.hedge_retries >= self.max_hedge_retries:
                            self.logger.error(
                                f"对冲失败 重试{self.max_hedge_retries}次后, 修复中")
                            await self.repair_single_leg_exposure()
                            self.reset_cycle()
                            self.state = STATE_IDLE
                            continue
                        hedge_side = 'sell' if self.lighter_maker_side == 'buy' else 'buy'
                        await self.place_extended_hedge(
                            hedge_side, self.lighter_maker_fill_size)
                        if not self.hedge_sent:
                            self.hedge_retries += 1
                            await asyncio.sleep(1)
                            continue

                    if self.extended_hedge_filled:
                        self.log_latency()
                        self.logger.info(
                            f"周期完成: Lighter {self.lighter_maker_side} "
                            f"{self.lighter_maker_fill_size} @ {self.lighter_maker_fill_price} | "
                            f"Extended对冲 @ {self.extended_hedge_fill_price}")
                        if self.lighter_maker_order_index is not None:
                            await self.cancel_lighter_maker_order()
                        self.reset_cycle()
                        self._update_position_timer()
                        self.state = STATE_IDLE
                        continue

                    # Hedge timeout: attempt reduce-only repair instead of just resetting
                    t_hs = self.cycle_timestamps.get('t_hedge_send', 0)
                    if t_hs and time.time() - t_hs > 60:
                        self.logger.error(
                            "对冲超时(60秒), 尝试reduce-only修复")
                        await self.repair_single_leg_exposure()
                        self.reset_cycle()
                        self.state = STATE_IDLE
                        continue

                    await asyncio.sleep(0.01)

            except Exception as e:
                self.logger.error(f"交易循环错误: {e}")
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(1)

    async def run(self):
        self.setup_signal_handlers()
        try:
            await self.trading_loop()
        except KeyboardInterrupt:
            self.logger.info("用户中断")
        except Exception as e:
            self.logger.error(f"致命错误: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            try:
                await self.async_shutdown()
            except Exception:
                pass


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Extended V2 Hedge Bot: Lighter Maker + Extended Hedge')
    parser.add_argument('--exchange', type=str)
    parser.add_argument('--ticker', type=str, default='BTC')
    parser.add_argument('--size', type=str, required=True)
    parser.add_argument('--max-position', type=Decimal, default=Decimal('0'))
    parser.add_argument('--safety-buffer', type=Decimal, default=Decimal('0.0003'))
    parser.add_argument('--requote-ticks', type=int, default=2)
    parser.add_argument('--max-order-age', type=int, default=30)
    parser.add_argument('--funding-guard', type=int, default=5)
    return parser.parse_args()
