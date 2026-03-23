"""
01 Exchange + Lighter Hedge Bot: 01 Maker + Lighter Standard Taker (方案C)

Strategy: Place passive POST_ONLY limit orders on 01 Exchange (maker leg, 0.01% fee),
          hedge with aggressive LIMIT on Lighter Standard (taker leg, 0% fee, 300ms latency).

Roles:
  - 01 Exchange: maker (POST_ONLY limit orders, 0.01% maker fee)
  - Lighter: taker (aggressive LIMIT orders, Standard account — 0% fee, 300ms latency)

Architecture:
  - MD Gateway:     01 WS (deltas@{symbol} + account) + Lighter WS (order_book)
  - Account:        01 WS (account) for maker fill detection
  - Signal Engine:  Computes net edge after fees/slippage/skew, produces maker quotes on 01
  - Execution:      01 maker manager + Lighter Standard taker hedge executor
  - Risk:           Position limits, single-leg exposure timeout, WS health, slippage budget
"""

import asyncio
import json
import signal as signal_mod
import logging
import os
import sys
import time
import requests
import traceback
import csv
from decimal import Decimal
from typing import Tuple, Optional, Dict

from lighter.signer_client import SignerClient

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.o1_client import O1Client
import schema_pb2
import websockets
from datetime import datetime
import pytz


STATE_IDLE = 'IDLE'
STATE_QUOTING = 'QUOTING'
STATE_HEDGING = 'HEDGING'


class HedgeBot:
    """01 Exchange Maker + Lighter Taker hedge bot."""

    def __init__(
        self,
        ticker: str,
        order_quantity: Decimal,
        fill_timeout: int = 5,
        max_position: Decimal = Decimal('0'),
        safety_buffer: Decimal = Decimal('0.0005'),
        close_buffer: Decimal = Decimal('0'),
        max_hold_minutes: int = 30,
        requote_ticks: int = 3,
        max_order_age: int = 30,
        funding_guard_minutes: int = 2,
        close_decay_multiplier: Decimal = Decimal('2.5'),
        ema_window: int = 100,
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
        self.close_decay_multiplier = close_decay_multiplier
        self.ema_window = ema_window

        self.state = STATE_IDLE
        self.stop_flag = False

        # 01 maker order state (方案C: 01挂单)
        self.o1_maker_order_id: Optional[str] = None
        self.o1_maker_client_order_id: Optional[str] = None
        self.o1_maker_side: Optional[str] = None
        self.o1_maker_price: Optional[Decimal] = None
        self.o1_maker_size: Optional[Decimal] = None
        self.o1_maker_placed_at: float = 0
        self.o1_maker_filled = False
        self.o1_maker_fill_price: Optional[Decimal] = None
        self.o1_maker_fill_size: Optional[Decimal] = None

        # Lighter taker hedge state (方案C: Lighter Standard吃单对冲)
        self.hedge_sent = False
        self.hedge_retries = 0
        self.max_hedge_retries = 5
        self.lighter_hedge_filled = False
        self.lighter_hedge_fill_price: Optional[Decimal] = None
        self.lighter_hedge_fill_size: Optional[Decimal] = None

        # Slippage tracking for Lighter Standard 300ms latency
        self._slippage_ema: Decimal = Decimal('0')
        self._slippage_count: int = 0
        self._slippage_alpha: Decimal = Decimal(str(2 / (20 + 1)))

        # Position tracking
        self.o1_position = Decimal('0')
        self.lighter_position = Decimal('0')
        self.position_opened_at: Optional[float] = None
        self._last_close_log_time: float = 0
        self._last_open_cycle_time: float = 0

        # Track weighted-average premium at which position was opened
        self._open_premium_sum = Decimal('0')
        self._open_premium_qty = Decimal('0')

        # 01 Exchange client state
        self.o1_client: Optional[O1Client] = None
        self.o1_market_id: Optional[int] = None
        self.o1_price_decimals: int = 2
        self.o1_size_decimals: int = 4
        self.o1_tick_size: Decimal = Decimal('0.01')
        self.o1_order_book = {"bids": {}, "asks": {}}
        self.o1_best_bid: Optional[Decimal] = None
        self.o1_best_ask: Optional[Decimal] = None
        self.o1_order_book_ready = False
        self.o1_ob_update_id: int = 0

        # Lighter client state
        self.lighter_client = None
        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_best_bid: Optional[Decimal] = None
        self.lighter_best_ask: Optional[Decimal] = None
        self.lighter_order_book_ready = False
        self.lighter_order_book_offset = 0
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_lock = asyncio.Lock()

        # WS tasks
        self.lighter_ws_task = None
        self.o1_ws_task = None

        # Fill event for zero-latency notification
        self.fill_event = asyncio.Event()

        # Stale fill queue
        self._stale_fills: list = []

        # Track processed fill order IDs to prevent double-counting
        self._processed_fill_ids: set = set()

        # Track all placed order IDs for cleanup
        self._all_placed_order_ids: set = set()

        # Map order_id → side ('buy'/'sell') for correct stale-fill handling
        self._order_side_map: Dict[int, str] = {}

        # WS health tracking
        self.lighter_ws_connected = False
        self.lighter_ws_last_msg_time: float = 0
        self.o1_ob_ws_connected = False
        self.o1_ob_ws_last_msg_time: float = 0
        self.o1_acct_ws_connected = False
        self.o1_acct_ws_last_msg_time: float = 0
        self.ws_health_timeout = 30

        # Premium EMA tracker for smart open/close timing
        self._ema_alpha = Decimal(str(2 / (self.ema_window + 1)))
        self._premium_ema: Optional[Decimal] = None
        self._premium_ema_count: int = 0
        self._last_premium_log: float = 0

        # Latency timestamps per cycle
        self.cycle_timestamps: Dict[str, float] = {}

        # Lighter API config
        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX'))
        self.api_key_index = int(os.getenv('LIGHTER_API_KEY_INDEX'))

        # 01 config
        self.o1_private_key = os.getenv('O1_PRIVATE_KEY')
        self.o1_account_id = int(os.getenv('O1_ACCOUNT_ID', '0'))

        # Logging setup
        os.makedirs("logs", exist_ok=True)
        self.log_filename = f"logs/o1_{ticker}_hedge_mode_log.txt"
        self.csv_filename = f"logs/o1_{ticker}_trades.csv"
        self.latency_csv_filename = f"logs/o1_{ticker}_latency.csv"

        self._initialize_csv_file()
        self._initialize_latency_csv()

        self.logger = logging.getLogger(f"o1_hedge_{ticker}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)

        file_handler = logging.FileHandler(self.log_filename)
        file_handler.setLevel(logging.INFO)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        console_handler.setFormatter(logging.Formatter(
            '%(levelname)s:%(name)s:%(message)s'))

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

    def log_trade_to_csv(self, exchange: str, side: str, price: str,
                         quantity: str, role: str):
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

        maker_fill_ms = (t_maker_fill - t_maker_send) * 1000 if t_maker_send and t_maker_fill else 0
        hedge_send_ms = (t_hedge_send - t_maker_fill) * 1000 if t_maker_fill and t_hedge_send else 0
        hedge_fill_ms = (t_hedge_fill - t_hedge_send) * 1000 if t_hedge_send and t_hedge_fill else 0
        maker_wait_s = t_maker_fill - t_maker_send if t_maker_send and t_maker_fill else 0
        total_cycle_s = t_hedge_fill - t_signal if t_signal and t_hedge_fill else 0

        try:
            with open(self.latency_csv_filename, 'a', newline='') as f:
                csv.writer(f).writerow([
                    ts,
                    f"{(t_maker_send - t_signal) * 1000:.2f}" if t_signal and t_maker_send else "0",
                    f"{(t_maker_send - t_signal) * 1000:.2f}" if t_signal and t_maker_send else "0",
                    f"{maker_fill_ms:.2f}",
                    f"{hedge_send_ms:.2f}", f"{hedge_fill_ms:.2f}",
                    f"{maker_wait_s:.3f}", f"{hedge_send_ms + hedge_fill_ms:.2f}",
                    f"{total_cycle_s:.3f}",
                    self.o1_maker_side or '',
                    str(self.o1_maker_fill_price or ''),
                    str(self.lighter_hedge_fill_price or ''),
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
        signal_mod.signal(signal_mod.SIGINT, handler)
        signal_mod.signal(signal_mod.SIGTERM, handler)

    async def async_shutdown(self):
        self.stop_flag = True
        self.logger.info("正在关闭...")

        for task in [self.lighter_ws_task, self.o1_ws_task]:
            if task and not task.done():
                task.cancel()
                await asyncio.sleep(0.1)

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

    def initialize_o1_client(self):
        if self.o1_client is None:
            if not self.o1_private_key:
                raise ValueError("O1_PRIVATE_KEY not set")
            if not self.o1_account_id:
                raise ValueError("O1_ACCOUNT_ID not set")
            self.o1_client = O1Client(self.o1_private_key, self.o1_account_id)
            self.o1_client.create_session()
            self.logger.info("01 Exchange 客户端已初始化")
        return self.o1_client

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

    def get_o1_market_config(self):
        """Fetch 01 Exchange market info for the given ticker."""
        o1_symbol = f"{self.ticker}USD"
        markets = self.o1_client.get_market_info()
        if o1_symbol not in markets:
            raise Exception(f"Symbol {o1_symbol} not found on 01 Exchange")
        info = markets[o1_symbol]
        self.o1_market_id = info["market_id"]
        self.o1_price_decimals = info["price_decimals"]
        self.o1_size_decimals = info["size_decimals"]
        self.o1_tick_size = Decimal("1") / (Decimal("10") ** self.o1_price_decimals)
        self.logger.info(
            f"01 市场: {o1_symbol} id={self.o1_market_id} "
            f"价格精度={self.o1_price_decimals} 数量精度={self.o1_size_decimals} "
            f"tick={self.o1_tick_size}")

    # ═════════════════════════════════════════════
    #  01 Exchange Order Book Management
    # ═════════════════════════════════════════════

    def update_o1_order_book(self, side: str, levels: list):
        for price_f, size_f in levels:
            price = Decimal(str(price_f))
            size = Decimal(str(size_f))
            if size > 0:
                self.o1_order_book[side][price] = size
            else:
                self.o1_order_book[side].pop(price, None)

    def refresh_o1_bbo(self):
        if self.o1_order_book["bids"]:
            self.o1_best_bid = max(self.o1_order_book["bids"].keys())
        else:
            self.o1_best_bid = None
        if self.o1_order_book["asks"]:
            self.o1_best_ask = min(self.o1_order_book["asks"].keys())
        else:
            self.o1_best_ask = None

    # ═════════════════════════════════════════════
    #  Lighter Order Book Management
    # ═════════════════════════════════════════════

    async def reset_lighter_order_book(self):
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_order_book_offset = 0
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

    def get_lighter_vwap(self, side: str, size: Decimal
                         ) -> Tuple[Optional[Decimal], Decimal, bool]:
        """Depth-weighted average price for executing *size* on Lighter.

        Args:
            side: 'buy' – we hit asks (ascending price);
                  'sell' – we hit bids (descending price).
            size: quantity in base asset (e.g. ETH).

        Returns:
            (vwap, top_fill_ratio, is_thin)
            vwap: volume-weighted average price, None if no liquidity.
            top_fill_ratio: fraction of *size* available at the best level (0-1).
            is_thin: True when best-level alone covers < 50 % of *size*.
        """
        if side == 'buy':
            levels = sorted(self.lighter_order_book['asks'].items())
        else:
            levels = sorted(self.lighter_order_book['bids'].items(),
                            reverse=True)

        if not levels:
            return None, Decimal('0'), True

        remaining = size
        total_cost = Decimal('0')
        top_level_size = levels[0][1]

        for price, level_size in levels:
            fill = min(remaining, level_size)
            total_cost += fill * price
            remaining -= fill
            if remaining <= 0:
                break

        filled = size - remaining
        if filled <= 0:
            return None, Decimal('0'), True

        vwap = total_cost / filled
        top_fill_ratio = min(top_level_size, size) / size
        is_thin = top_fill_ratio < Decimal('0.5')

        if remaining > 0:
            return vwap, top_fill_ratio, True

        return vwap, top_fill_ratio, is_thin

    async def _request_lighter_ob_refresh(self, ws, reason: str):
        channel = f"order_book/{self.lighter_market_index}"
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_ready = False
        await ws.send(json.dumps({"type": "unsubscribe", "channel": channel}))
        await asyncio.sleep(0.2)
        await ws.send(json.dumps({"type": "subscribe", "channel": channel}))
        self.logger.info(f"Lighter 订单簿原地刷新 ({reason})")

    # ═════════════════════════════════════════════
    #  01 Exchange WebSocket: Combined (OB + Account)
    # ═════════════════════════════════════════════

    async def handle_o1_combined_ws(self):
        """Single WS connection for both orderbook deltas and account updates.

        Key: 01 server sends pings; client must NOT send its own protocol pings
        (ping_interval=None).  The websockets library auto-responds to server
        pings.  We detect dead connections via recv() timeout instead.
        """
        o1_symbol = f"{self.ticker}USD"
        url = (f"wss://zo-mainnet.n1.xyz/ws/"
               f"deltas@{o1_symbol}&account@{self.o1_account_id}")
        retry_delay = 0.5
        recv_timeout = 30
        saved_ob_update_id = 0

        while not self.stop_flag:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=5,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self.logger.info(
                        f"01 WS已连接: {o1_symbol} + account@{self.o1_account_id}")
                    self.o1_ob_ws_connected = True
                    self.o1_acct_ws_connected = True

                    if saved_ob_update_id == 0:
                        self.o1_order_book["bids"].clear()
                        self.o1_order_book["asks"].clear()
                        self.o1_ob_update_id = 0
                    else:
                        self.logger.info(
                            f"01 WS快速重连, 保留订单簿 (update_id={saved_ob_update_id})")

                    retry_delay = 0.5
                    consecutive_timeouts = 0

                    while not self.stop_flag:
                        try:
                            message = await asyncio.wait_for(
                                ws.recv(), timeout=recv_timeout)
                            consecutive_timeouts = 0
                        except asyncio.TimeoutError:
                            consecutive_timeouts += 1
                            if consecutive_timeouts >= 2:
                                self.logger.warning(
                                    f"01 WS: {recv_timeout * consecutive_timeouts}秒无消息, 重连")
                                break
                            continue

                        if self.stop_flag:
                            break
                        try:
                            data = json.loads(message)
                            now = time.time()

                            delta = data.get("delta")
                            if delta:
                                self.o1_ob_ws_last_msg_time = now
                                self.o1_ob_ws_connected = True

                                new_id = delta.get("update_id", 0)
                                if new_id > self.o1_ob_update_id:
                                    self.o1_ob_update_id = new_id
                                    saved_ob_update_id = new_id
                                    self.update_o1_order_book(
                                        "bids", delta.get("bids", []))
                                    self.update_o1_order_book(
                                        "asks", delta.get("asks", []))
                                    self.refresh_o1_bbo()

                                    if (not self.o1_order_book_ready and
                                            self.o1_best_bid and self.o1_best_ask):
                                        self.o1_order_book_ready = True
                                        self.logger.info(
                                            f"01 订单簿就绪: 买一="
                                            f"{self.o1_best_bid} "
                                            f"卖一={self.o1_best_ask}")

                            account = data.get("account")
                            if account:
                                self.o1_acct_ws_last_msg_time = now
                                self.o1_acct_ws_connected = True

                                for order_key, fill_data in account.get(
                                        "fills", {}).items():
                                    self._handle_o1_fill(fill_data, order_key)
                                for order_key, place_data in account.get(
                                        "places", {}).items():
                                    self._handle_o1_place(order_key, place_data)
                                for order_key in account.get("cancels", {}):
                                    self._handle_o1_cancel(order_key)

                        except Exception as e:
                            self.logger.error(f"01 WS消息出错: {e}")

            except websockets.exceptions.ConnectionClosed as e:
                self.logger.warning(
                    f"01 WS断开 (code={e.code}), {retry_delay:.1f}秒后重连...")
                self.o1_ob_ws_connected = False
                self.o1_acct_ws_connected = False
            except Exception as e:
                self.logger.error(f"01 WS连接错误: {e}")
                self.o1_ob_ws_connected = False
                self.o1_acct_ws_connected = False

            if not self.stop_flag:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 10)

    @staticmethod
    def _normalize_o1_side(side: str) -> str:
        """Normalize 01 WS side values: ask→sell, bid→buy."""
        if not side:
            return side
        s = side.lower()
        if s in ('ask', 'sell'):
            return 'sell'
        if s in ('bid', 'buy'):
            return 'buy'
        return s

    def _handle_o1_fill(self, fill_data: dict, our_order_key: str = ""):
        """Handle 01 Exchange fill events (maker fill detection in 方案C)."""
        try:
            fill_order_id = fill_data.get("order_id")
            fill_price = Decimal(str(fill_data.get("price", 0)))
            fill_qty = Decimal(str(fill_data.get("quantity", 0)))

            if fill_qty <= 0:
                return

            fill_key = f"{our_order_key}_{fill_order_id}_{fill_price}_{fill_qty}"
            if fill_key in self._processed_fill_ids:
                return
            self._processed_fill_ids.add(fill_key)
            if len(self._processed_fill_ids) > 200:
                oldest = list(self._processed_fill_ids)[:100]
                for k in oldest:
                    self._processed_fill_ids.discard(k)

            fill_side_raw = None
            for key_candidate in [our_order_key, fill_order_id]:
                if key_candidate is not None:
                    try:
                        fill_side_raw = self._order_side_map.get(int(key_candidate))
                    except (ValueError, TypeError):
                        fill_side_raw = self._order_side_map.get(key_candidate)
                    if fill_side_raw:
                        break

            if not fill_side_raw:
                fill_side_raw = fill_data.get("side")

            if not fill_side_raw:
                self.logger.warning(
                    f"[01] 无法确定成交方向: order_key={our_order_key} "
                    f"fill_id={fill_order_id} {fill_qty} @ {fill_price}")
                return

            fill_side = self._normalize_o1_side(fill_side_raw)

            is_our_maker = False
            if self.o1_maker_order_id is not None:
                is_our_maker = (str(our_order_key) == str(self.o1_maker_order_id))
            if not is_our_maker and self.o1_maker_client_order_id is not None:
                is_our_maker = (str(our_order_key) == str(self.o1_maker_client_order_id))
            if not is_our_maker and self.state == STATE_QUOTING and self.o1_maker_side:
                is_our_maker = (fill_side == self.o1_maker_side)

            if is_our_maker:
                if self.o1_maker_fill_price is not None and self.o1_maker_fill_size:
                    weighted_cost = (self.o1_maker_fill_price * self.o1_maker_fill_size
                                     + fill_price * fill_qty)
                    total_size = self.o1_maker_fill_size + fill_qty
                    self.o1_maker_fill_price = weighted_cost / total_size
                    self.o1_maker_fill_size = total_size
                    self.logger.info(
                        f"[01] maker追加成交: {fill_side} {fill_qty} @ {fill_price} "
                        f"(累计={self.o1_maker_fill_size} 均价={self.o1_maker_fill_price:.2f})")
                else:
                    self.o1_maker_fill_price = fill_price
                    self.o1_maker_fill_size = fill_qty

                min_hedge_qty = max(
                    Decimal('0.001'),
                    self.order_quantity * Decimal('0.5'))
                if not self.o1_maker_filled and (
                        self.o1_maker_fill_size >= min_hedge_qty
                        or self.o1_maker_fill_size >= self.o1_maker_size):
                    self.o1_maker_filled = True
                    self.cycle_timestamps['t_maker_fill'] = time.time()
                    self.fill_event.set()
                    self.logger.info(
                        f"[01] maker已成交: {fill_side} {self.o1_maker_fill_size} "
                        f"@ {self.o1_maker_fill_price}")
                elif not self.o1_maker_filled:
                    self.logger.info(
                        f"[01] maker部分成交: {fill_qty} @ {fill_price} "
                        f"(累计={self.o1_maker_fill_size}/{self.o1_maker_size}, "
                        f"阈值={min_hedge_qty})")
            else:
                if self.state != STATE_IDLE:
                    self.logger.info(
                        f"[01] 非当前maker单成交: {fill_side}({fill_side_raw}) "
                        f"{fill_qty} @ {fill_price} order_key={our_order_key} "
                        f"maker_oid={self.o1_maker_order_id} "
                        f"maker_cid={self.o1_maker_client_order_id}")

        except Exception as e:
            self.logger.error(f"处理01成交出错: {e}")

    def _handle_o1_place(self, order_key: str, place_data: dict):
        """Handle 01 order placement confirmations (maker orders in 方案C)."""
        try:
            side_raw = place_data.get('side')
            side = self._normalize_o1_side(side_raw) if side_raw else None
            self._order_side_map[int(order_key)] = side_raw
            if len(self._order_side_map) > 200:
                oldest = list(self._order_side_map.keys())[:100]
                for k in oldest:
                    self._order_side_map.pop(k, None)

            is_our_maker = False
            client_oid = place_data.get('client_order_id')
            if client_oid and self.o1_maker_client_order_id is not None:
                is_our_maker = (str(client_oid) == str(self.o1_maker_client_order_id))
            if not is_our_maker and self.o1_maker_client_order_id is not None:
                is_our_maker = (str(order_key) == str(self.o1_maker_client_order_id))
            if not is_our_maker and self.state == STATE_QUOTING and self.o1_maker_side:
                if side == self.o1_maker_side and self.o1_maker_order_id is None:
                    is_our_maker = True

            if is_our_maker:
                self.o1_maker_order_id = str(order_key)
                self.logger.info(
                    f"[01] maker挂单已上链: order_key={order_key} side={side}")
        except Exception as e:
            self.logger.error(f"处理01订单上链出错: {e}")

    def _handle_o1_cancel(self, order_key: str):
        """Handle 01 order cancellation (maker orders in 方案C)."""
        if self.o1_maker_order_id is not None:
            if (str(order_key) == str(self.o1_maker_order_id) and
                    not self.o1_maker_filled):
                self.logger.info(
                    f"[01] maker挂单被撤销: order_key={order_key}")

    # ═════════════════════════════════════════════
    #  Lighter WebSocket (order book only, 方案C: Lighter is taker)
    # ═════════════════════════════════════════════

    async def handle_lighter_ws(self):
        url = "wss://mainnet.zklighter.elliot.ai/stream"

        while not self.stop_flag:
            timeout_count = 0
            try:
                await self.reset_lighter_order_book()
                async with websockets.connect(url) as ws:
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": f"order_book/{self.lighter_market_index}"
                    }))

                    ob_refresh_time = time.time()
                    while not self.stop_flag:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)
                            data = json.loads(msg)
                            timeout_count = 0
                            self.lighter_ws_connected = True
                            self.lighter_ws_last_msg_time = time.time()

                            msg_type = data.get("type", "")

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
                                        f"Lighter 订单簿快照: "
                                        f"{len(self.lighter_order_book['bids'])}买, "
                                        f"{len(self.lighter_order_book['asks'])}卖, "
                                        f"bid={self.lighter_best_bid} ask={self.lighter_best_ask}")

                                elif msg_type == "update/order_book" and self.lighter_snapshot_loaded:
                                    ob = data.get("order_book", {})
                                    if not ob or "offset" not in ob:
                                        continue
                                    new_offset = ob["offset"]
                                    if new_offset <= self.lighter_order_book_offset:
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
    #  Signal Engine
    # ═════════════════════════════════════════════

    def compute_signal(self) -> Optional[Tuple[str, Decimal, Decimal]]:
        """Compute 01 maker signal (方案C).

        The maker side is 01 Exchange; the hedge side is Lighter Standard (taker).
        - BUY on 01 (maker) → hedge SELL on Lighter (taker) → profit = lighter_bid - o1_buy_price
        - SELL on 01 (maker) → hedge BUY on Lighter (taker) → profit = o1_sell_price - lighter_ask
        """
        if (self.o1_best_bid is None or self.o1_best_ask is None or
                self.lighter_best_bid is None or self.lighter_best_ask is None):
            return None

        has_position = (self.o1_position != 0)

        o1_spread = self.o1_best_ask - self.o1_best_bid
        o1_spread_pct = o1_spread / self.o1_best_ask
        o1_ob_wide = o1_spread_pct > Decimal('0.005')

        o1_mid = (self.o1_best_bid + self.o1_best_ask) / 2
        lighter_mid = (self.lighter_best_bid + self.lighter_best_ask) / 2
        mid_divergence = abs(o1_mid - lighter_mid) / lighter_mid

        current_premium = o1_mid - lighter_mid
        if self._premium_ema is None:
            self._premium_ema = current_premium
            self._premium_ema_count = 1
        else:
            self._premium_ema_count += 1
            self._premium_ema = (
                self._ema_alpha * current_premium +
                (Decimal('1') - self._ema_alpha) * self._premium_ema)

        premium_deviation = current_premium - self._premium_ema
        ema_ready = self._premium_ema_count >= 200

        now_ts = time.time()
        if now_ts - self._last_premium_log > 60:
            self._last_premium_log = now_ts
            avg_p = (self._open_premium_sum / self._open_premium_qty
                     if self._open_premium_qty > 0 else Decimal('0'))
            self.logger.info(
                f"[溢价] 当前={current_premium:.2f} "
                f"EMA={self._premium_ema:.2f} "
                f"偏差={premium_deviation:.2f} "
                f"开仓均价溢价={avg_p:.2f} "
                f"样本={self._premium_ema_count} "
                f"滑点EMA={self._slippage_ema:.4f}")

        if mid_divergence > Decimal('0.003'):
            if has_position:
                now = time.time()
                if now - getattr(self, '_last_diverge_log', 0) > 10:
                    self._last_diverge_log = now
                    self.logger.info(
                        f"[平仓模式] 价格偏离={mid_divergence:.4f}, "
                        f"允许继续平仓")
            else:
                now = time.time()
                if now - getattr(self, '_last_diverge_log', 0) > 10:
                    self._last_diverge_log = now
                    self.logger.warning(
                        f"价格偏离过大: 01中间价={o1_mid:.2f} "
                        f"Lighter中间价={lighter_mid:.2f} 偏离度={mid_divergence:.4f}")
                return None

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
            net_pos = self.o1_position + self.lighter_position
            skew = net_pos / self.max_position

        price_ref = o1_mid

        # ── Close direction ──
        close_side = None
        position_abs = abs(self.o1_position)
        if self.o1_position > 0 and position_abs >= self.order_quantity * Decimal('0.01'):
            close_side = 'sell'
        elif self.o1_position < 0 and position_abs >= self.order_quantity * Decimal('0.01'):
            close_side = 'buy'

        effective_close_buffer = self.close_buffer
        if close_side and self.position_opened_at:
            decay_target = -self.open_buffer * self.close_decay_multiplier
            premium_boost = Decimal('0')
            if (ema_ready and premium_deviation < Decimal('0') and
                    self._premium_ema is not None and self._premium_ema > 0):
                premium_boost = min(Decimal('1'),
                                    abs(premium_deviation) / self._premium_ema * Decimal('3'))
            if premium_boost > Decimal('0'):
                effective_close_buffer = (
                    self.close_buffer + premium_boost * (decay_target - self.close_buffer))

        effective_open_buffer = self.open_buffer
        if ema_ready and self._premium_ema is not None and self._premium_ema > 0:
            premium_ratio = premium_deviation / self._premium_ema
            adjustment = Decimal('1') - premium_ratio * Decimal('1.5')
            adjustment = max(Decimal('0.7'), min(Decimal('1.3'), adjustment))
            effective_open_buffer = self.open_buffer * adjustment

        close_qty = min(self.order_quantity, position_abs) if close_side else self.order_quantity

        # ── Fee structure (方案C: 01 maker 0.01% + Lighter Standard 0%) ──
        fee_per_leg = price_ref * Decimal('0.0001')

        # Slippage budget for Lighter Standard 300ms latency
        # Cap slippage EMA contribution to avoid threshold runaway
        slippage_for_open = min(
            max(self._slippage_ema, price_ref * Decimal('0.00005')),
            price_ref * Decimal('0.0003'))
        slippage_for_close = min(
            max(self._slippage_ema * Decimal('0.5'), Decimal('0')),
            price_ref * Decimal('0.0002'))

        open_safety = price_ref * effective_open_buffer + fee_per_leg + slippage_for_open
        close_safety = price_ref * effective_close_buffer + fee_per_leg + slippage_for_close

        # ── BUY on 01 (maker) → hedge SELL on Lighter (taker) ──
        # We post buy on 01 at o1_bid. When filled, sell on Lighter at lighter_bid.
        # Edge = lighter_bid - o1_buy_price - safety
        o1_buy_price = self.o1_best_bid
        buy_is_close = (close_side == 'buy')
        buy_safety = close_safety if buy_is_close else open_safety
        skew_adj = skew * open_safety

        buy_edge = self.lighter_best_bid - o1_buy_price - buy_safety - skew_adj

        # ── SELL on 01 (maker) → hedge BUY on Lighter (taker) ──
        # We post sell on 01 at o1_ask. When filled, buy on Lighter at lighter_ask.
        # Edge = o1_sell_price - lighter_ask - safety
        o1_sell_price = self.o1_best_ask
        sell_is_close = (close_side == 'sell')
        sell_safety = close_safety if sell_is_close else open_safety

        sell_edge = o1_sell_price - self.lighter_best_ask - sell_safety + skew_adj

        can_buy = self.max_position <= 0 or (self.o1_position < self.max_position)
        can_sell = self.max_position <= 0 or (self.o1_position > -self.max_position)

        min_close_narrowing = price_ref * Decimal('0.0002')
        avg_open_premium = (self._open_premium_sum / self._open_premium_qty
                            if self._open_premium_qty > 0 else None)
        premium_narrowed_enough = True
        if avg_open_premium is not None and close_side:
            premium_narrowing = avg_open_premium - current_premium
            if premium_narrowing < min_close_narrowing:
                premium_narrowed_enough = False

        # Priority 1: closing signals (no premium_narrowed check — close ASAP)
        # Close 01-SHORT (buy on 01 maker) → sell on Lighter taker
        if buy_is_close and buy_edge > 0 and can_buy:
            rounded = self._round_o1_price(o1_buy_price)
            if rounded > 0:
                now = time.time()
                if now - self._last_close_log_time >= 10:
                    hold_str = ""
                    if self.position_opened_at:
                        hold_str = f" hold={((now - self.position_opened_at) / 60):.1f}min"
                    pnl_str = ""
                    if avg_open_premium is not None:
                        est_pnl = (avg_open_premium - current_premium
                                   - fee_per_leg * 2)
                        pnl_str = f" 预估盈亏={est_pnl:+.2f}/ETH"
                    narrowing_str = "N/A"
                    if avg_open_premium is not None:
                        narrowing_str = f"{avg_open_premium - current_premium:+.2f}"
                    self.logger.info(
                        f"[平仓] 买入信号 edge=${buy_edge:.4f} "
                        f"溢价收窄={narrowing_str} "
                        f"数量={close_qty}{hold_str}{pnl_str}")
                    self._last_close_log_time = now
                return ('buy', rounded, close_qty)

        # Close 01-LONG (sell on 01 maker) → buy on Lighter taker
        if sell_is_close and sell_edge > 0 and can_sell:
            rounded = self._round_o1_price(o1_sell_price)
            if rounded > 0:
                now = time.time()
                if now - self._last_close_log_time >= 10:
                    hold_str = ""
                    if self.position_opened_at:
                        hold_str = f" hold={((now - self.position_opened_at) / 60):.1f}min"
                    pnl_str = ""
                    if avg_open_premium is not None:
                        est_pnl = (avg_open_premium - current_premium
                                   - fee_per_leg * 2)
                        pnl_str = f" 预估盈亏={est_pnl:+.2f}/ETH"
                    narrowing_str = "N/A"
                    if avg_open_premium is not None:
                        narrowing_str = f"{avg_open_premium - current_premium:+.2f}"
                    self.logger.info(
                        f"[平仓] 卖出信号 edge=${sell_edge:.4f} "
                        f"溢价收窄={narrowing_str} "
                        f"数量={close_qty}{hold_str}{pnl_str}")
                    self._last_close_log_time = now
                return ('sell', rounded, close_qty)

        # Priority 2: block new opens if holding too long
        if close_side is not None and self.position_opened_at:
            hold_min = (time.time() - self.position_opened_at) / 60
            if hold_min > self.max_hold_minutes:
                now = time.time()
                if now - getattr(self, '_last_closewait_log', 0) > 30:
                    self._last_closewait_log = now
                    prem_str = ""
                    if avg_open_premium is not None:
                        narrowing = avg_open_premium - current_premium
                        prem_str = (f" 开仓溢价={avg_open_premium:.2f}"
                                    f" 当前={current_premium:.2f}"
                                    f" 收窄={narrowing:+.2f}"
                                    f" 需>{min_close_narrowing:.2f}")
                    self.logger.info(
                        f"[等待平仓] hold={hold_min:.1f}min "
                        f"pos={self.o1_position}{prem_str}")
                return None

        best_signal = None

        # Opening: SELL on 01 (maker) → BUY on Lighter (taker)
        # This captures 01 premium over Lighter (01 ask > Lighter ask).
        if sell_edge > 0 and can_sell and not sell_is_close:
            if ema_ready and premium_deviation < Decimal('0'):
                pass
            else:
                rounded = self._round_o1_price(o1_sell_price)
                if rounded > 0:
                    best_signal = ('sell', rounded, self.order_quantity)

        # Opening: BUY on 01 (maker) → SELL on Lighter (taker)
        # This captures Lighter premium over 01 (Lighter bid > 01 bid).
        if best_signal is None and buy_edge > 0 and can_buy and not buy_is_close:
            if ema_ready and premium_deviation > Decimal('0'):
                pass
            else:
                rounded = self._round_o1_price(o1_buy_price)
                if rounded > 0:
                    best_signal = ('buy', rounded, self.order_quantity)

        if best_signal is None:
            now = time.time()
            if now - getattr(self, '_last_nosig_log', 0) > 30:
                self._last_nosig_log = now
                self.logger.info(
                    f"[信号] 无开仓机会: buy_edge={buy_edge:.4f} sell_edge={sell_edge:.4f} "
                    f"can_buy={can_buy} can_sell={can_sell} "
                    f"滑点开={slippage_for_open:.2f} 滑点平={slippage_for_close:.2f}")

        return best_signal

    def _round_o1_price(self, price: Decimal) -> Decimal:
        if self.o1_tick_size and self.o1_tick_size > 0:
            return (price / self.o1_tick_size).quantize(Decimal('1')) * self.o1_tick_size
        return price

    def _round_lighter_price(self, price: Decimal) -> Decimal:
        if self.lighter_tick_size and self.lighter_tick_size > 0:
            return (price / self.lighter_tick_size).quantize(Decimal('1')) * self.lighter_tick_size
        return price

    # ═════════════════════════════════════════════
    #  Execution: 01 Maker (方案C)
    # ═════════════════════════════════════════════

    async def place_o1_maker_order(self, side: str, quantity: Decimal,
                                   price: Decimal):
        """Place a POST_ONLY limit order on 01 Exchange (maker leg)."""
        client_order_id = int(time.time() * 1000)

        self.o1_maker_client_order_id = str(client_order_id)
        self.o1_maker_order_id = None
        self.o1_maker_side = side
        self.o1_maker_price = price
        self.o1_maker_size = quantity
        self.o1_maker_placed_at = time.time()
        self.o1_maker_filled = False
        self.o1_maker_fill_price = None
        self.o1_maker_fill_size = None

        self.cycle_timestamps['t_maker_send'] = time.time()

        price_raw = int(self._round_o1_price(price) *
                        (Decimal('10') ** self.o1_price_decimals))
        size_raw = int(quantity * (Decimal('10') ** self.o1_size_decimals))

        try:
            receipt = self.o1_client.place_order(
                market_id=self.o1_market_id,
                side=side,
                price_raw=price_raw,
                size_raw=size_raw,
                fill_mode=schema_pb2.FillMode.POST_ONLY,
                client_order_id=client_order_id,
            )

            self.logger.info(
                f"[01] maker挂单已发送: {side} {quantity} @ {price} "
                f"(client_id={client_order_id})")

        except Exception as e:
            err_msg = str(e)
            if "POST_ONLY" in err_msg.upper() or "WOULD_CROSS" in err_msg.upper():
                self.logger.info(
                    f"[01] POST_ONLY被拒(价格穿透盘口): {side} @ {price}")
            else:
                self.logger.error(f"01 maker挂单出错: {e}")
            self.state = STATE_IDLE

    async def cancel_o1_maker_order(self):
        """Cancel the current 01 Exchange maker order."""
        if self.o1_maker_order_id is None and self.o1_maker_client_order_id is None:
            return

        for attempt in range(3):
            try:
                self.o1_client.cancel_order_by_client_id(
                    client_order_id=int(self.o1_maker_client_order_id))
                self.logger.info(
                    f"[01] maker挂单已撤销 (client_id={self.o1_maker_client_order_id})")
                return
            except Exception as e:
                err_msg = str(e)
                if "NOT_FOUND" in err_msg.upper() or "ALREADY" in err_msg.upper():
                    return
                self.logger.error(f"01撤单出错 (尝试{attempt+1}): {e}")
            if attempt < 2:
                await asyncio.sleep(0.3)

    async def cleanup_stale_orders(self):
        """Cancel any leftover orders that the bot no longer tracks."""
        pass

    def should_requote(self) -> bool:
        """Check if 01 maker order needs to be re-quoted based on Lighter price changes."""
        if self.o1_maker_price is None:
            return False

        new_signal = self.compute_signal()
        if new_signal is None:
            return True

        new_side, new_price, _ = new_signal
        if new_side != self.o1_maker_side:
            return True

        price_diff = abs(new_price - self.o1_maker_price)
        if price_diff > self.o1_tick_size * self.requote_ticks:
            return True

        return False

    # ═════════════════════════════════════════════
    #  Execution: Lighter Standard Taker Hedge (方案C)
    # ═════════════════════════════════════════════

    async def place_lighter_taker_hedge(self, side: str, quantity: Decimal,
                                        record_slippage: bool = False):
        """Place aggressive limit order on Lighter Standard (taker hedge).

        Uses dynamic pricing based on slippage EMA to compensate for 300ms latency.
        When record_slippage=True, records actual vs expected slippage for EMA updates.
        """
        min_lighter_qty = Decimal('0.01')
        if quantity < min_lighter_qty:
            self.logger.warning(
                f"[Lighter] 对冲数量{quantity}低于最小下单量{min_lighter_qty}, 跳过")
            return
        is_ask = (side.lower() == 'sell')

        if side.lower() == 'buy':
            if not self.lighter_best_ask or self.lighter_best_ask <= 0:
                self.logger.error("无法对冲: Lighter无卖价")
                return
            expected_fill_price = self.lighter_best_ask
            price_ref = self.lighter_best_ask
            slip_mult = max(Decimal('0.003'),
                            self._slippage_ema / price_ref * 3
                            if price_ref > 0 else Decimal('0.003'))
            aggressive_price = price_ref * (1 + slip_mult)
        else:
            if not self.lighter_best_bid or self.lighter_best_bid <= 0:
                self.logger.error("无法对冲: Lighter无买价")
                return
            expected_fill_price = self.lighter_best_bid
            price_ref = self.lighter_best_bid
            slip_mult = max(Decimal('0.003'),
                            self._slippage_ema / price_ref * 3
                            if price_ref > 0 else Decimal('0.003'))
            aggressive_price = price_ref * (1 - slip_mult)

        aggressive_price = self._round_lighter_price(aggressive_price)
        self.cycle_timestamps['t_hedge_send'] = time.time()
        client_order_index = int(time.time() * 1000)

        try:
            tx, tx_hash, error = await self.lighter_client.create_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=int(quantity * self.base_amount_multiplier),
                price=int(aggressive_price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=False,
                trigger_price=0,
            )
            if error is not None:
                raise Exception(f"Lighter create_order error: {error}")

            self.hedge_sent = True
            self.logger.info(
                f"[Lighter] taker对冲已发送: {side} {quantity} @ {aggressive_price} "
                f"(预期成交~{expected_fill_price}, slip_mult={slip_mult:.4f})")

            for attempt in range(5):
                await asyncio.sleep(0.8)
                new_pos = await self.get_lighter_position_from_api()
                old_pos = self.lighter_position
                fill_delta = abs(new_pos - old_pos)
                if fill_delta >= quantity * Decimal('0.5'):
                    actual_fill_price = None
                    if record_slippage and fill_delta > 0:
                        actual_fill_price = await self._get_lighter_avg_entry()

                    self.lighter_position = new_pos
                    self.lighter_hedge_filled = True
                    self.lighter_hedge_fill_size = fill_delta
                    self.lighter_hedge_fill_price = actual_fill_price or expected_fill_price
                    self.cycle_timestamps['t_hedge_fill'] = time.time()

                    if record_slippage and actual_fill_price:
                        self._update_slippage_ema(
                            expected_fill_price, actual_fill_price, side)

                    self.logger.info(
                        f"[Lighter] taker对冲确认: {side} {fill_delta} "
                        f"(预期={expected_fill_price} 实际={self.lighter_hedge_fill_price})")
                    self.log_trade_to_csv(
                        'Lighter', side, str(self.lighter_hedge_fill_price),
                        str(fill_delta), 'hedge_taker')
                    break
            else:
                new_pos = await self.get_lighter_position_from_api()
                fill_delta = abs(new_pos - self.lighter_position)
                if fill_delta > 0:
                    self.lighter_position = new_pos
                    self.lighter_hedge_filled = True
                    self.lighter_hedge_fill_size = fill_delta
                    self.lighter_hedge_fill_price = expected_fill_price
                    self.cycle_timestamps['t_hedge_fill'] = time.time()
                    self.logger.info(
                        f"[Lighter] taker对冲延迟确认: {side} {fill_delta}")
                    self.log_trade_to_csv(
                        'Lighter', side, str(expected_fill_price),
                        str(fill_delta), 'hedge_taker')
                else:
                    self.logger.warning(
                        f"[Lighter] taker对冲未能确认成交 (5次检查)")

        except Exception as e:
            self.logger.error(
                f"[Lighter] taker对冲失败: {side} {quantity} err={e}")

    def _update_slippage_ema(self, expected_price: Decimal,
                             actual_price: Decimal, side: str):
        """Update slippage EMA after a Lighter taker fill."""
        if side == 'buy':
            slippage = actual_price - expected_price
        else:
            slippage = expected_price - actual_price
        slippage = max(Decimal('0'), slippage)

        if self._slippage_count == 0:
            self._slippage_ema = slippage
        else:
            self._slippage_ema = (
                self._slippage_alpha * slippage +
                (Decimal('1') - self._slippage_alpha) * self._slippage_ema)
        self._slippage_count += 1

        self.logger.info(
            f"[滑点] 本次={slippage:.4f} EMA={self._slippage_ema:.4f} "
            f"样本={self._slippage_count}")

    # ═════════════════════════════════════════════
    #  Position Management
    # ═════════════════════════════════════════════

    async def get_lighter_position_from_api(self) -> Decimal:
        pos_data = await self._get_lighter_position_detail()
        if pos_data is not None:
            return Decimal(pos_data['position']) * pos_data['sign']
        return self.lighter_position

    async def _get_lighter_avg_entry(self) -> Optional[Decimal]:
        pos_data = await self._get_lighter_position_detail()
        if pos_data and 'avg_entry_price' in pos_data:
            try:
                return Decimal(pos_data['avg_entry_price'])
            except Exception:
                pass
        return None

    async def _get_lighter_position_detail(self) -> Optional[dict]:
        url = f"{self.lighter_base_url}/api/v1/account"
        params = {"by": "index", "value": self.account_index}
        try:
            resp = requests.get(url, headers={"accept": "application/json"},
                                params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            for pos in data.get('accounts', [{}])[0].get('positions', []):
                if pos.get('symbol') == self.ticker:
                    return pos
            return None
        except Exception as e:
            self.logger.error(f"获取Lighter仓位出错: {e}")
            return None

    async def get_o1_position_from_api(self) -> Decimal:
        try:
            data = self.o1_client.get_account()
            for pos in data.get("positions", []):
                market_id = pos.get("market_id") or pos.get("marketId")
                if market_id == self.o1_market_id:
                    perp = pos.get("perp", {})
                    base_size = Decimal(str(
                        perp.get("baseSize", 0) or
                        pos.get("baseSize", 0) or
                        pos.get("size", 0)
                    ))
                    if base_size == 0:
                        return Decimal('0')
                    is_long = perp.get("isLong", True)
                    if not is_long:
                        return -abs(base_size)
                    return abs(base_size)
            return Decimal('0')
        except Exception as e:
            self.logger.error(f"获取01仓位出错: {e}")
            return self.o1_position

    async def check_position_balance(self, log: bool = True) -> bool:
        lighter_pos = await self.get_lighter_position_from_api()
        o1_pos = await self.get_o1_position_from_api()

        self.lighter_position = lighter_pos
        self.o1_position = o1_pos

        net = abs(self.o1_position + self.lighter_position)
        if log:
            self.logger.info(
                f"仓位: 01={self.o1_position} Lighter={self.lighter_position} "
                f"净值={net}")

        self._update_position_timer()

        if net > self.order_quantity * 5:
            self.logger.error(f"仓位不平衡过大: {net}")
            return False
        return True

    async def _auto_repair_net_position(self):
        """Fix small net position residual left over from previous sessions."""
        net = self.o1_position + self.lighter_position
        min_repair = Decimal('0.001')

        if abs(net) < min_repair:
            return

        if abs(net) >= self.order_quantity * 3:
            self.logger.warning(
                f"[自动修复] 净值偏差过大 ({net:.4f} >= {self.order_quantity * 3}), "
                f"跳过自动修复, 请手动处理")
            return

        if net < 0:
            repair_side = 'buy'
        else:
            repair_side = 'sell'
        repair_size = abs(net)

        self.logger.info(
            f"[自动修复] 检测到净值残留={net:.4f}, "
            f"Lighter {repair_side} {repair_size} 修正中...")

        try:
            await self.place_lighter_taker_hedge(repair_side, repair_size)
            await asyncio.sleep(2)
            new_lighter = await self.get_lighter_position_from_api()
            self.lighter_position = new_lighter
            new_net = self.o1_position + new_lighter
            self.logger.info(
                f"[自动修复] 完成. Lighter={new_lighter} 新净值={new_net:.4f}")
            self.hedge_sent = False
        except Exception as e:
            self.logger.warning(f"[自动修复] 修复失败 ({e}), 继续运行")
            self.hedge_sent = False

    async def _force_close_both_legs(self):
        """Force-close all positions on both exchanges when hold time far exceeds limit."""
        o1_pos = self.o1_position
        lighter_pos = self.lighter_position
        hold_min = 0
        if self.position_opened_at:
            hold_min = (time.time() - self.position_opened_at) / 60

        self.logger.warning(
            f"[强制平仓] 持仓超时 {hold_min:.1f}min > 60min, "
            f"01={o1_pos} Lighter={lighter_pos}, 启动双腿强制平仓")

        try:
            if o1_pos != 0:
                close_side = 'buy' if o1_pos < 0 else 'sell'
                close_size = abs(o1_pos)

                if close_side == 'buy' and self.o1_best_ask:
                    aggressive_price = self.o1_best_ask * Decimal('1.002')
                elif close_side == 'sell' and self.o1_best_bid:
                    aggressive_price = self.o1_best_bid * Decimal('0.998')
                else:
                    self.logger.error("[强制平仓] 无法获取01价格, 跳过")
                    return

                price_raw = int(self._round_o1_price(aggressive_price) *
                                (Decimal('10') ** self.o1_price_decimals))
                size_raw = int(close_size *
                               (Decimal('10') ** self.o1_size_decimals))

                self.logger.info(
                    f"[强制平仓] 01 {close_side} {close_size} @ ~{aggressive_price:.2f}")
                try:
                    self.o1_client.place_order(
                        market_id=self.o1_market_id,
                        side=close_side,
                        price_raw=price_raw,
                        size_raw=size_raw,
                        fill_mode=schema_pb2.FillMode.LIMIT,
                        client_order_id=int(time.time() * 1000),
                    )
                except RuntimeError as e:
                    self.logger.error(f"[强制平仓] 01下单失败: {e}")

            if lighter_pos != 0:
                lighter_close_side = 'sell' if lighter_pos > 0 else 'buy'
                lighter_close_size = abs(lighter_pos)
                self.logger.info(
                    f"[强制平仓] Lighter {lighter_close_side} {lighter_close_size}")
                await self.place_lighter_taker_hedge(
                    lighter_close_side, lighter_close_size)

            await asyncio.sleep(3)

            real_o1 = await self.get_o1_position_from_api()
            real_lighter = await self.get_lighter_position_from_api()
            self.o1_position = real_o1
            self.lighter_position = real_lighter
            net = abs(real_o1 + real_lighter)

            self.logger.info(
                f"[强制平仓] 完成. 01={real_o1} Lighter={real_lighter} 净值={net:.4f}")

            self.position_opened_at = None
            self._open_premium_sum = Decimal('0')
            self._open_premium_qty = Decimal('0')
            self.reset_cycle()
            self.state = STATE_IDLE
            self.hedge_sent = False

        except Exception as e:
            self.logger.error(f"[强制平仓] 异常: {e}")
            self.logger.error(traceback.format_exc())
            self.reset_cycle()
            self.state = STATE_IDLE
            self.hedge_sent = False

    # ═════════════════════════════════════════════
    #  Risk Control
    # ═════════════════════════════════════════════

    def is_ws_healthy(self, log: bool = True) -> bool:
        now = time.time()
        lighter_ok = (self.lighter_ws_connected and
                      now - self.lighter_ws_last_msg_time < self.ws_health_timeout)
        o1_ob_ok = (self.o1_ob_ws_connected and
                    now - self.o1_ob_ws_last_msg_time < self.ws_health_timeout)

        if log and now - getattr(self, '_last_ws_health_log', 0) > 15:
            if not lighter_ok:
                self.logger.warning(
                    f"Lighter WS异常: 连接={self.lighter_ws_connected} "
                    f"上次消息={now - self.lighter_ws_last_msg_time:.1f}秒前")
            if not o1_ob_ok:
                self.logger.warning(
                    f"01 OB WS异常: 连接={self.o1_ob_ws_connected} "
                    f"上次消息={now - self.o1_ob_ws_last_msg_time:.1f}秒前")
            if not lighter_ok or not o1_ob_ok:
                self._last_ws_health_log = now

        return lighter_ok and o1_ob_ok

    async def emergency_cancel_all(self):
        self.logger.warning("风控: 紧急撤销所有01 maker挂单")
        if self.state == STATE_QUOTING and (
                self.o1_maker_order_id or self.o1_maker_client_order_id):
            await self.cancel_o1_maker_order()
        self.reset_cycle()
        self.state = STATE_IDLE

    async def repair_single_leg_exposure(self):
        """When hedge fails, check real positions first then repair if needed."""
        # Refresh real positions from API before repairing
        real_o1 = await self.get_o1_position_from_api()
        real_lighter = await self.get_lighter_position_from_api()
        self.o1_position = real_o1
        self.lighter_position = real_lighter
        net = real_o1 + real_lighter

        self.logger.info(
            f"[修复] 检查真实仓位: 01={real_o1} Lighter={real_lighter} 净值={net}")

        if abs(net) < self.order_quantity * Decimal('0.5'):
            self.logger.info("[修复] 仓位已平衡, 无需修复")
            return

        # Determine what needs fixing on Lighter side
        if net > 0:
            repair_side = 'sell'
            repair_size = abs(net)
        else:
            repair_side = 'buy'
            repair_size = abs(net)

        self.logger.warning(
            f"风控: 修复单腿暴露 (净值={net}). "
            f"发送{repair_side} {repair_size} 到Lighter")

        await self.place_lighter_taker_hedge(repair_side, repair_size)
        await asyncio.sleep(1)

    # ═════════════════════════════════════════════
    #  Cycle Reset
    # ═════════════════════════════════════════════

    def reset_cycle(self):
        self.o1_maker_client_order_id = None
        self.o1_maker_order_id = None
        self.o1_maker_side = None
        self.o1_maker_price = None
        self.o1_maker_size = None
        self.o1_maker_placed_at = 0
        self.o1_maker_filled = False
        self.o1_maker_fill_price = None
        self.o1_maker_fill_size = None

        self.hedge_sent = False
        self.hedge_retries = 0
        self.lighter_hedge_filled = False
        self.lighter_hedge_fill_price = None
        self.lighter_hedge_fill_size = None

        self.fill_event.clear()
        if hasattr(self, '_fill_logged'):
            del self._fill_logged

        self._signal_lighter_bid = None
        self._signal_lighter_ask = None
        self._cycle_is_close = False
        self.cycle_timestamps = {}

    def _update_position_timer(self):
        has_pos = (self.o1_position != 0)
        if has_pos and self.position_opened_at is None:
            self.position_opened_at = time.time()
            self.logger.info(f"仓位建立: 01={self.o1_position}")
        elif not has_pos and self.position_opened_at is not None:
            hold_min = (time.time() - self.position_opened_at) / 60
            self.logger.info(f"仓位平仓 (持有{hold_min:.1f}分钟)")
            self.position_opened_at = None
            self._open_premium_sum = Decimal('0')
            self._open_premium_qty = Decimal('0')

    # ═════════════════════════════════════════════
    #  Main Trading Loop
    # ═════════════════════════════════════════════

    async def trading_loop(self):
        self.logger.info(f"启动 01+Lighter 对冲机器人: {self.ticker}")

        try:
            self.initialize_lighter_client()
            self.initialize_o1_client()

            self.lighter_market_index, self.base_amount_multiplier, \
                self.price_multiplier, self.lighter_tick_size = self.get_lighter_market_config()
            self.get_o1_market_config()

            self.logger.info(
                f"合约: 01市场={self.o1_market_id} "
                f"Lighter市场={self.lighter_market_index}")
        except Exception as e:
            self.logger.error(f"初始化失败: {e}")
            traceback.print_exc()
            return

        # Start 01 combined WS (orderbook + account in one connection)
        self.o1_ws_task = asyncio.create_task(self.handle_o1_combined_ws())
        self.logger.info("等待01订单簿数据...")
        t0 = time.time()
        while not self.o1_order_book_ready and not self.stop_flag:
            if time.time() - t0 > 15:
                self.logger.warning("等待01订单簿超时")
                break
            await asyncio.sleep(0.5)

        # Start Lighter WS
        self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())
        self.logger.info("等待Lighter订单簿数据...")
        t0 = time.time()
        while not self.lighter_order_book_ready and not self.stop_flag:
            if time.time() - t0 > 15:
                self.logger.warning("等待Lighter订单簿超时")
                break
            await asyncio.sleep(0.5)

        # Initial position check
        if not await self.check_position_balance():
            self.logger.error("初始仓位不平衡, 中止启动")
            return

        # Auto-repair small net position residual from previous sessions
        await self._auto_repair_net_position()

        await asyncio.sleep(3)
        self.logger.info("机器人就绪, 进入主循环")

        last_position_check = time.time()
        last_log = time.time()

        while not self.stop_flag:
            try:
                now = time.time()
                if now - last_position_check > 30:
                    if not await self.check_position_balance(log=(now - last_log > 10)):
                        self.logger.error("检测到仓位不平衡, 停止运行")
                        break
                    last_position_check = now
                    if now - last_log > 10:
                        last_log = now
                    if self.state == STATE_IDLE:
                        net = self.o1_position + self.lighter_position
                        if abs(net) >= Decimal('0.001'):
                            self.logger.warning(
                                f"[运行时修复] 检测到净暴露={net:.4f}, 自动修复中")
                            await self._auto_repair_net_position()
                            last_position_check = time.time()

                # Refresh 01 session if needed
                if self.o1_client:
                    try:
                        self.o1_client.ensure_session()
                    except Exception as e:
                        self.logger.error(f"01 session刷新失败: {e}")
                        await asyncio.sleep(5)
                        continue

                if not self.is_ws_healthy():
                    has_pos = (self.o1_position != 0)
                    lighter_ok = (self.lighter_ws_connected and
                                  time.time() - self.lighter_ws_last_msg_time
                                  < self.ws_health_timeout)
                    if has_pos and lighter_ok and self.state == STATE_IDLE:
                        pass
                    elif self.state == STATE_QUOTING:
                        await asyncio.sleep(1.5)
                        if not self.is_ws_healthy(log=False):
                            self.logger.warning("风控: WS持续异常, 紧急撤单")
                            await self.emergency_cancel_all()
                        continue
                    elif self.state == STATE_HEDGING:
                        await asyncio.sleep(1)
                        continue
                    else:
                        await asyncio.sleep(1)
                        continue

                # Drain stale fills — verify real positions first
                if self._stale_fills and self.state == STATE_IDLE:
                    real_o1 = await self.get_o1_position_from_api()
                    real_lighter = await self.get_lighter_position_from_api()
                    self.o1_position = real_o1
                    self.lighter_position = real_lighter
                    net = real_o1 + real_lighter
                    if abs(net) < self.order_quantity * Decimal('0.1'):
                        self.logger.info(
                            f"[修复] 有{len(self._stale_fills)}个过期成交, "
                            f"但仓位已平衡(净值={net}), 丢弃")
                        self._stale_fills.clear()
                    else:
                        sf = self._stale_fills.pop(0)
                        # Use real net to determine repair direction
                        if net > 0:
                            hedge_side = 'sell'
                            repair_size = min(abs(net), sf['size'])
                        else:
                            hedge_side = 'buy'
                            repair_size = min(abs(net), sf['size'])

                        self.logger.warning(
                            f"[修复] 过期成交 {sf['side']} {sf['size']} @ {sf['price']}, "
                            f"净暴露={net}, 发送{hedge_side} {repair_size}到Lighter")
                        await self.place_lighter_taker_hedge(hedge_side, repair_size)
                        if self.hedge_sent:
                            self.log_trade_to_csv(
                                '01', sf['side'], str(sf['price']),
                                str(sf['size']), 'stale_maker')
                            self.hedge_sent = False

                if not (self.lighter_order_book_ready and self.o1_order_book_ready):
                    if self.o1_position != 0 and self.lighter_order_book_ready and \
                            self.o1_best_bid is not None and self.o1_best_ask is not None:
                        pass
                    else:
                        await asyncio.sleep(0.1)
                        continue

                # ────────────────── STATE: IDLE ──────────────────
                if self.state == STATE_IDLE:
                    if (self.o1_position != 0 and self.position_opened_at):
                        hold_min = (time.time() - self.position_opened_at) / 60
                        if hold_min > 60:
                            await self._force_close_both_legs()
                            continue

                    sig = self.compute_signal()
                    if sig:
                        side, price, size = sig
                        # In 方案C: side is the 01 maker side
                        # buy on 01 (maker) → sell on Lighter (taker)
                        # sell on 01 (maker) → buy on Lighter (taker)
                        is_close = ((side == 'sell' and self.o1_position > 0) or
                                    (side == 'buy' and self.o1_position < 0))
                        self._cycle_is_close = is_close

                        if not is_close:
                            cooldown = time.time() - self._last_open_cycle_time
                            if cooldown < 3:
                                sig = None

                    if sig:
                        side, price, size = sig
                        self.cycle_timestamps['t_signal'] = time.time()
                        self._signal_lighter_bid = self.lighter_best_bid
                        self._signal_lighter_ask = self.lighter_best_ask

                        await self.place_o1_maker_order(side, size, price)
                        self.state = STATE_QUOTING
                    else:
                        if (self.o1_best_bid and self.lighter_best_bid and
                                now - getattr(self, '_last_debug_log', 0) > 30):
                            self._last_debug_log = now
                            price_ref = (self.o1_best_bid + self.o1_best_ask) / 2
                            fee_per_leg = price_ref * Decimal('0.0001')
                            slip_open = min(
                                max(self._slippage_ema, price_ref * Decimal('0.00005')),
                                price_ref * Decimal('0.0003'))
                            threshold = price_ref * self.open_buffer + fee_per_leg + slip_open
                            spread = self.o1_best_bid - self.lighter_best_bid
                            self.logger.info(
                                f"[监控] O1买={self.o1_best_bid} O1卖={self.o1_best_ask} "
                                f"L买={self.lighter_best_bid} L卖={self.lighter_best_ask} | "
                                f"价差={spread:.2f} 阈值={threshold:.2f} "
                                f"滑点EMA={self._slippage_ema:.4f}")
                        await asyncio.sleep(0.5)
                    continue

                # ────────────────── STATE: QUOTING (01 maker) ──────────────────
                elif self.state == STATE_QUOTING:
                    if self.o1_maker_filled:
                        self.fill_event.clear()
                        self.state = STATE_HEDGING
                        continue

                    age = time.time() - self.o1_maker_placed_at
                    if age > self.max_order_age:
                        self.logger.info(f"01 maker挂单超时 ({age:.1f}秒), 撤销中")
                        await self.cancel_o1_maker_order()
                        self.reset_cycle()
                        self.state = STATE_IDLE
                        continue

                    if age > 5.0 and self.should_requote():
                        self.logger.info("价格偏移, 重新报价")
                        await self.cancel_o1_maker_order()
                        self.reset_cycle()
                        self.state = STATE_IDLE
                        continue

                    try:
                        await asyncio.wait_for(self.fill_event.wait(), timeout=0.5)
                    except asyncio.TimeoutError:
                        pass

                # ────────────────── STATE: HEDGING (Lighter Standard taker) ──────────────────
                elif self.state == STATE_HEDGING:
                    if self.o1_maker_filled and not hasattr(self, '_fill_logged'):
                        fill_size = self.o1_maker_fill_size or self.o1_maker_size
                        self.log_trade_to_csv(
                            '01', self.o1_maker_side,
                            str(self.o1_maker_fill_price),
                            str(fill_size), 'maker')

                        if self.o1_maker_side == 'buy':
                            self.o1_position += fill_size
                        else:
                            self.o1_position -= fill_size

                        self._fill_logged = True

                    if not self.hedge_sent:
                        if self.hedge_retries >= self.max_hedge_retries:
                            self.logger.error(
                                f"Lighter对冲失败 重试{self.max_hedge_retries}次后, 修复中")
                            await self.repair_single_leg_exposure()
                            self.reset_cycle()
                            self.state = STATE_IDLE
                            continue
                        hedge_side = 'sell' if self.o1_maker_side == 'buy' else 'buy'
                        fill_size = self.o1_maker_fill_size
                        net = self.o1_position + self.lighter_position
                        ideal_hedge = abs(net)
                        if (ideal_hedge > fill_size and
                                ideal_hedge <= fill_size * Decimal('3')):
                            hedge_size = ideal_hedge
                            self.logger.info(
                                f"[对冲] 含净值修正: {fill_size} → {hedge_size} "
                                f"(净值={net:.4f})")
                        else:
                            hedge_size = fill_size
                        await self.place_lighter_taker_hedge(
                            hedge_side, hedge_size, record_slippage=True)
                        if not self.hedge_sent:
                            self.hedge_retries += 1
                            await asyncio.sleep(1)
                            continue

                    if self.lighter_hedge_filled:
                        self.log_latency()
                        is_open_cycle = not getattr(self, '_cycle_is_close', False)

                        if (is_open_cycle and self.o1_maker_fill_price
                                and self.lighter_hedge_fill_price
                                and self.o1_maker_fill_size):
                            if self.o1_maker_side == 'sell':
                                cycle_premium = (self.o1_maker_fill_price
                                                 - self.lighter_hedge_fill_price)
                            else:
                                cycle_premium = (self.lighter_hedge_fill_price
                                                 - self.o1_maker_fill_price)
                            self._open_premium_sum += (
                                cycle_premium * self.o1_maker_fill_size)
                            self._open_premium_qty += self.o1_maker_fill_size
                            avg_prem = (self._open_premium_sum / self._open_premium_qty
                                        if self._open_premium_qty > 0 else Decimal('0'))
                            status = "✓" if cycle_premium > 0 else "✗亏损"
                            self.logger.info(
                                f"[开仓完成] 01 {self.o1_maker_side} "
                                f"{self.o1_maker_fill_size} @ "
                                f"{self.o1_maker_fill_price} | "
                                f"Lighter对冲 @ {self.lighter_hedge_fill_price} "
                                f"本笔溢价={cycle_premium:+.2f} "
                                f"均价溢价={avg_prem:+.2f} {status}")
                        elif not is_open_cycle:
                            self.logger.info(
                                f"[平仓完成] 01 {self.o1_maker_side} "
                                f"{self.o1_maker_fill_size} @ "
                                f"{self.o1_maker_fill_price} | "
                                f"Lighter对冲 @ {self.lighter_hedge_fill_price}")
                        else:
                            self.logger.info(
                                f"周期完成: 01 {self.o1_maker_side} "
                                f"{self.o1_maker_fill_size} @ "
                                f"{self.o1_maker_fill_price} | "
                                f"Lighter对冲 @ {self.lighter_hedge_fill_price}")

                        sig_l_bid = getattr(self, '_signal_lighter_bid', None)
                        sig_l_ask = getattr(self, '_signal_lighter_ask', None)
                        if sig_l_bid and sig_l_ask and self.lighter_hedge_fill_price:
                            if self.o1_maker_side == 'sell':
                                drift = sig_l_ask - self.lighter_hedge_fill_price
                            else:
                                drift = self.lighter_hedge_fill_price - sig_l_bid
                            self.logger.info(
                                f"[对冲漂移] 信号时Lighter={sig_l_bid}/{sig_l_ask} "
                                f"实际对冲={self.lighter_hedge_fill_price} "
                                f"漂移={drift:+.2f}")

                        is_opening_cycle = not getattr(self, '_cycle_is_close', False)
                        if is_opening_cycle:
                            self._last_open_cycle_time = time.time()

                        self.reset_cycle()

                        self._update_position_timer()
                        self.state = STATE_IDLE
                        continue

                    t_hs = self.cycle_timestamps.get('t_hedge_send', 0)
                    if t_hs and time.time() - t_hs > 30:
                        self.logger.error("Lighter对冲超时(30秒), 检查实际仓位...")
                        real_o1 = await self.get_o1_position_from_api()
                        real_lighter = await self.get_lighter_position_from_api()
                        self.o1_position = real_o1
                        self.lighter_position = real_lighter
                        net = abs(real_o1 + real_lighter)
                        if net < self.order_quantity * Decimal('0.5'):
                            self.logger.info(
                                f"仓位实际已平衡 (01={real_o1} Lighter={real_lighter})")
                            self.reset_cycle()
                            self.state = STATE_IDLE
                            continue
                        self.logger.error(f"仓位确认不平衡 (净值={net}), 尝试修复")
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
