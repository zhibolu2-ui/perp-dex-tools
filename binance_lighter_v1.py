#!/usr/bin/env python3
"""
Binance-Lighter V1: 币安 USDC-M (JIT Maker 0%) + Lighter (Taker 0%) 价差套利

基于 V34，移植 V33 全部审计修复:
  BUG-1: _repair_ioc_cids 防对账死循环
  BUG-2: _processed_fill_keys WS 重连去重
  BUG-5: _fill_qty_pending 追加成交补充对冲
  ISSUE-7: 紧急平仓 BBO 兜底
  ISSUE-8: 会话累积 P&L 偏差熔断
  ISSUE-9: REPAIR 后净敞口冷却

费用:
  币安 USDC-M maker: 0 bps (促销期间)  |  taker: 4.0 bps (兜底用)
  Lighter 免费账户 maker: 0 bps  |  taker: 0 bps

用法:
  python binance_lighter_v1.py --symbol BTC --size 0.002 --max-position 0.01 \\
    --open-spread 2 --close-spread 0 --ladder-step 1.5 \\
    --open-jit-wait 2.0 --close-jit-wait 1.0 --open-jit-price at_best
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
from typing import Dict, List, Optional, Tuple

import aiohttp
import websockets
from dotenv import load_dotenv
from lighter.signer_client import SignerClient

sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    import uvloop
except ImportError:
    uvloop = None

# ═══════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════

LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_REST_URL = "https://mainnet.zklighter.elliot.ai"

BINANCE_FAPI = "https://fapi.binance.com"
BINANCE_WS = "wss://fstream.binance.com/ws"

SYMBOL_TO_MARKET = {"ETH": 0, "BTC": 1, "SOL": 2, "DOGE": 3}
SYMBOL_TO_BINANCE = {"BTC": "BTCUSDC", "ETH": "ETHUSDC", "SOL": "SOLUSDC"}

FEE_BINANCE_MAKER = Decimal("0")          # 促销期间 0 bps
FEE_BINANCE_TAKER = Decimal("0.0004")     # 4.0 bps (兜底用)
FEE_LIGHTER_TAKER = Decimal("0")          # 免费账户 0 bps

LIGHTER_RATE_LIMIT = 36
LIGHTER_RATE_WINDOW = 60.0

_TZ_CN = timezone(timedelta(hours=8))


class State(str, Enum):
    IDLE = "IDLE"
    QUOTING = "QUOTING"
    HEDGING = "HEDGING"
    REPAIR = "REPAIR"


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
        open_jit_wait: float = 2.0,
        close_jit_wait: float = 1.0,
        open_jit_price_mode: str = "at_best",
        close_jit_price_mode: str = "at_best",
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
        self.open_jit_wait = open_jit_wait
        self.close_jit_wait = close_jit_wait
        self.open_jit_price_mode = open_jit_price_mode
        self.close_jit_price_mode = close_jit_price_mode
        self.hedge_timeout = hedge_timeout
        self.max_order_age = max_order_age
        self.interval = interval
        self.dry_run = dry_run

        self.state = State.IDLE
        self.stop_flag = False
        self._in_flatten = False

        # ── Positions ──
        self.lighter_position = Decimal("0")
        self.binance_position = Decimal("0")

        # ── Lighter market config ──
        self.lighter_market_index: int = SYMBOL_TO_MARKET.get(self.symbol, 1)
        self.account_index: int = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
        self.api_key_index: int = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))
        self.base_amount_multiplier: int = 0
        self.price_multiplier: int = 0
        self.lighter_tick = Decimal("0.1")
        self.lighter_min_size_step = Decimal("0.00001")

        # ── Binance config ──
        self._b_symbol = SYMBOL_TO_BINANCE.get(self.symbol, f"{self.symbol}USDC")
        self._b_ccxt_symbol = f"{self.symbol}/USDC:USDC"
        self._b_tick = Decimal("0.10")
        self._b_min_qty = Decimal("0.001")
        self._b_session: Optional[aiohttp.ClientSession] = None
        self._b_api_key = ""
        self._b_api_secret = ""
        self._b_listen_key = ""

        # ── Binance BBO (from WS bookTicker) ──
        self._b_best_bid: Optional[Decimal] = None
        self._b_best_ask: Optional[Decimal] = None
        self._b_ws_ts: float = 0.0

        # ── Binance JIT maker order state ──
        self._b_order_id: Optional[str] = None
        self._b_order_side: Optional[str] = None
        self._b_order_price = Decimal("0")

        # ── Binance fill event (from user data stream) ──
        self._b_fill_event = asyncio.Event()
        self._b_fill_side: Optional[str] = None
        self._b_fill_price = Decimal("0")
        self._b_fill_qty = Decimal("0")

        # ── Lighter API rate limiter ──
        self._lighter_req_times: collections.deque = collections.deque()

        # ── Lighter taker fill tracking (hedge side) ──
        self._flatten_fill_price: Optional[Decimal] = None
        self._flatten_fill_qty = Decimal("0")
        self._flatten_fill_event = asyncio.Event()
        self._flatten_ioc_cid: Optional[int] = None

        # ── Lighter OB state ──
        self._l_ob: Dict[str, Dict[float, float]] = {"bids": {}, "asks": {}}
        self._l_ob_ready: bool = False
        self._l_best_bid: Optional[Decimal] = None
        self._l_best_ask: Optional[Decimal] = None
        self._l_ws_ts: float = 0.0

        # ── Clients ──
        self.lighter_client: Optional[SignerClient] = None
        self._binance_exchange = None  # ccxt async

        # ── Instant hedge (Lighter taker after Binance fill) ──
        self._instant_hedge_task: Optional[asyncio.Task] = None
        self._instant_hedge_done = asyncio.Event()
        self._instant_hedge_ok: bool = False
        self._instant_hedge_fill_price: Optional[Decimal] = None
        self._instant_hedge_ms: float = 0.0

        # ── Fill-time BBO snapshot ──
        self._fill_l_bid: Optional[Decimal] = None
        self._fill_l_ask: Optional[Decimal] = None
        self._fill_ts: float = 0.0

        # ── Stale fills ──
        self._stale_fills: List[dict] = []

        # ── Repair IOC tracking (BUG-1: 防止对账修复死循环) ──
        self._repair_ioc_cids: set = set()

        # ── Lighter nonce: 毫秒时间戳*100+seq, 上限 2^48-1 ──
        self._lighter_nonce: int = int(time.time() * 1000) * 100

        # ── WS fill dedup (BUG-2: 防止重连后重复计仓) ──
        self._processed_fill_keys: set = set()

        # ── Pending additional hedge qty (BUG-5: 追加成交补充对冲) ──
        self._fill_qty_pending = Decimal("0")

        # ── Risk ──
        self._consecutive_hedge_fails: int = 0
        self._consecutive_close_fails: int = 0
        self._consecutive_pnl_deviations: int = 0
        self._circuit_breaker_until: float = 0.0
        self.cumulative_net_bps: float = 0.0
        self.cumulative_dollar_pnl: float = 0.0
        self._trade_seq: int = 0

        # ── Balance P&L tracking ──
        self._initial_balance_l: Optional[Decimal] = None
        self._initial_balance_b: Optional[Decimal] = None
        self._pre_trade_balance_l: Optional[Decimal] = None
        self._pre_trade_balance_b: Optional[Decimal] = None
        self.cumulative_real_pnl: float = 0.0

        # ── Ladder ──
        self._ladder_level: int = 0

        # ── Logging / CSV ──
        os.makedirs("logs", exist_ok=True)
        self.csv_path = f"logs/bl_v1_{self.symbol}_trades.csv"
        self.log_path = f"logs/bl_v1_{self.symbol}.log"
        self._init_csv()
        self.logger = self._init_logger()

    # ─────────────────────────────────────────────
    #  Logging / CSV
    # ─────────────────────────────────────────────

    def _init_logger(self) -> logging.Logger:
        lg = logging.getLogger(f"bl_v1_{self.symbol}")
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
                    "binance_price", "lighter_price", "qty",
                    "spread_bps", "fee_bps", "net_bps",
                    "hedge_ms", "cumulative_bps",
                    "real_pnl", "cumulative_real_pnl",
                    "balance_lighter", "balance_binance",
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

    async def _init_binance(self):
        import ccxt.async_support as ccxt_async
        self._b_api_key = os.getenv("BINANCE_API_KEY", "")
        self._b_api_secret = os.getenv("BINANCE_API_SECRET", "")
        if not self._b_api_key or not self._b_api_secret:
            raise ValueError("BINANCE_API_KEY / BINANCE_API_SECRET env vars not set")
        self._binance_exchange = ccxt_async.binanceusdm({
            "apiKey": self._b_api_key,
            "secret": self._b_api_secret,
            "options": {"defaultType": "future"},
        })
        await self._binance_exchange.load_markets()
        mkt = self._binance_exchange.market(self._b_ccxt_symbol)
        if mkt:
            self._b_tick = Decimal(str(mkt.get("precision", {}).get("price", 0.1)))
            min_amt = mkt.get("limits", {}).get("amount", {}).get("min")
            if min_amt:
                self._b_min_qty = Decimal(str(min_amt))
        self._b_session = aiohttp.ClientSession()
        self.logger.info(
            f"Binance USDC-M 已初始化: {self._b_ccxt_symbol} "
            f"tick={self._b_tick} min_qty={self._b_min_qty}")

    def _round_lighter(self, price: Decimal) -> Decimal:
        if self.lighter_tick > 0:
            return (price / self.lighter_tick).quantize(Decimal("1")) * self.lighter_tick
        return price

    def _round_binance(self, price: Decimal) -> Decimal:
        if self._b_tick > 0:
            return (price / self._b_tick).quantize(Decimal("1")) * self._b_tick
        return price

    # ─────────────────────────────────────────────
    #  Binance JIT price computation
    # ─────────────────────────────────────────────

    def _compute_binance_jit_price(
        self, side: str, b_bid: Decimal, b_ask: Decimal, mode: str,
    ) -> Decimal:
        """Compute Binance maker price. Post-only ensures maker status.

        For sell maker: price >= best ask (sit on ask side)
        For buy maker: price <= best bid (sit on bid side)

        Modes:
          at_best  — join the queue at current best (default)
          improve  — 1 tick inside spread (faster fill, might reject if spread=1tick)
          behind   — 1 tick behind best (slowest fill, safest maker)
        """
        if side == "sell":
            if mode == "improve":
                price = b_ask - self._b_tick
                if price <= b_bid:
                    price = b_ask
            elif mode == "at_best":
                price = b_ask
            else:
                price = b_ask + self._b_tick
        else:
            if mode == "improve":
                price = b_bid + self._b_tick
                if price >= b_ask:
                    price = b_bid
            elif mode == "at_best":
                price = b_bid
            else:
                price = b_bid - self._b_tick
        return self._round_binance(price)

    # ─────────────────────────────────────────────
    #  Binance BBO helper
    # ─────────────────────────────────────────────

    def _get_binance_bbo(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        return self._b_best_bid, self._b_best_ask

    # ─────────────────────────────────────────────
    #  Binance WS: bookTicker (market data)
    # ─────────────────────────────────────────────

    async def _binance_book_ws_loop(self):
        stream = f"{self._b_symbol.lower()}@bookTicker"
        url = f"{BINANCE_WS}/{stream}"
        reconnect_delay = 1

        while not self.stop_flag:
            try:
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=10, close_timeout=5,
                ) as ws:
                    self.logger.info(f"[Binance] bookTicker WS 已连接: {self._b_symbol}")
                    reconnect_delay = 1
                    async for raw in ws:
                        if self.stop_flag:
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        bid = msg.get("b")
                        ask = msg.get("a")
                        if bid and ask:
                            self._b_best_bid = Decimal(bid)
                            self._b_best_ask = Decimal(ask)
                            self._b_ws_ts = time.time()
            except asyncio.CancelledError:
                return
            except Exception as e:
                self.logger.warning(f"[Binance] bookTicker WS 断连: {e}")
            if self.stop_flag:
                return
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30)

    # ─────────────────────────────────────────────
    #  Binance WS: User Data Stream (fills)
    # ─────────────────────────────────────────────

    async def _binance_create_listen_key(self) -> str:
        headers = {"X-MBX-APIKEY": self._b_api_key}
        async with self._b_session.post(
            f"{BINANCE_FAPI}/fapi/v1/listenKey", headers=headers
        ) as resp:
            data = await resp.json()
            return data.get("listenKey", "")

    async def _binance_renew_listen_key(self):
        headers = {"X-MBX-APIKEY": self._b_api_key}
        async with self._b_session.put(
            f"{BINANCE_FAPI}/fapi/v1/listenKey", headers=headers
        ) as resp:
            if resp.status != 200:
                self.logger.warning(f"[Binance] listenKey续期失败: {resp.status}")

    async def _binance_user_ws_loop(self):
        reconnect_delay = 1
        while not self.stop_flag:
            try:
                self._b_listen_key = await self._binance_create_listen_key()
                if not self._b_listen_key:
                    self.logger.error("[Binance] 获取listenKey失败")
                    await asyncio.sleep(5)
                    continue

                url = f"{BINANCE_WS}/{self._b_listen_key}"
                renew_task = asyncio.create_task(self._binance_listen_key_renewer())

                try:
                    async with websockets.connect(
                        url, ping_interval=20, ping_timeout=10, close_timeout=5,
                    ) as ws:
                        self.logger.info("[Binance] User Data Stream 已连接")
                        reconnect_delay = 1
                        async for raw in ws:
                            if self.stop_flag:
                                break
                            try:
                                msg = json.loads(raw)
                            except json.JSONDecodeError:
                                continue
                            event_type = msg.get("e")
                            if event_type == "ORDER_TRADE_UPDATE":
                                self._handle_binance_order_event(msg)
                            elif event_type == "ACCOUNT_UPDATE":
                                self._handle_binance_account_update(msg)
                finally:
                    renew_task.cancel()
                    try:
                        await renew_task
                    except asyncio.CancelledError:
                        pass

            except asyncio.CancelledError:
                return
            except Exception as e:
                self.logger.warning(f"[Binance] User WS 断连: {e}")

            if self.stop_flag:
                return
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30)

    async def _binance_listen_key_renewer(self):
        try:
            while not self.stop_flag:
                await asyncio.sleep(1800)
                await self._binance_renew_listen_key()
                self.logger.info("[Binance] listenKey 已续期")
        except asyncio.CancelledError:
            return

    def _handle_binance_order_event(self, msg: dict):
        order_data = msg.get("o", {})
        symbol = order_data.get("s", "")
        if symbol != self._b_symbol:
            return

        status = order_data.get("X", "")  # FILLED, PARTIALLY_FILLED, NEW, CANCELED, etc.
        order_id = str(order_data.get("i", ""))
        side = order_data.get("S", "").lower()  # BUY / SELL
        avg_price = order_data.get("ap", "0")
        filled_qty = order_data.get("z", "0")
        last_price = order_data.get("L", "0")
        last_qty = order_data.get("l", "0")
        exec_type = order_data.get("x", "")  # TRADE, NEW, CANCELED

        if exec_type == "TRADE" and self._b_order_id and order_id == str(self._b_order_id):
            _qty = Decimal(filled_qty) if Decimal(filled_qty) > 0 else Decimal(last_qty)
            _price = Decimal(avg_price) if Decimal(avg_price) > 0 else Decimal(last_price)

            if not self._b_fill_event.is_set():
                if side == "buy":
                    self.binance_position += _qty
                else:
                    self.binance_position -= _qty
                self._b_fill_side = side
                self._b_fill_price = _price
                self._b_fill_qty = _qty

                self._fill_l_bid = self._l_best_bid
                self._fill_l_ask = self._l_best_ask
                self._fill_ts = time.time()
                self._instant_hedge_done.clear()
                self._instant_hedge_ok = False
                self._instant_hedge_fill_price = None
                self._instant_hedge_task = asyncio.create_task(
                    self._instant_hedge_fire())

                self._b_fill_event.set()
                self.logger.info(
                    f"[Binance] 成交: {side} {_qty} @ {_price}  "
                    f"L_snap={self._fill_l_bid}/{self._fill_l_ask} → 即时Lighter对冲已触发")
            elif self._b_fill_event.is_set():
                _increment = Decimal(last_qty)
                if side == "buy":
                    self.binance_position += _increment
                else:
                    self.binance_position -= _increment
                new_total = Decimal(filled_qty)
                self._b_fill_price = _price
                self._b_fill_qty = new_total
                self._fill_qty_pending = getattr(
                    self, '_fill_qty_pending', Decimal("0")) + _increment
                self.logger.warning(
                    f"[Binance] 追加成交: {side} +{last_qty} @ {last_price}, "
                    f"累计={new_total}, 待补对冲={self._fill_qty_pending}")

        if status == "FILLED" and order_id == str(self._b_order_id):
            self.logger.info(f"[Binance] 订单完全成交: {order_id}")
        elif status == "CANCELED" and order_id == str(self._b_order_id):
            filled = Decimal(filled_qty)
            if filled > 0 and not self._b_fill_event.is_set():
                self.logger.warning(
                    f"[Binance] 撤单但有部分成交: {filled} @ {avg_price}")

    def _handle_binance_account_update(self, msg: dict):
        account_data = msg.get("a", {})
        positions = account_data.get("P", [])
        for pos in positions:
            if pos.get("s") == self._b_symbol:
                pa = pos.get("pa", "0")
                self.logger.info(f"[Binance] 仓位更新: {self._b_symbol} = {pa}")

    # ─────────────────────────────────────────────
    #  Lighter WS: order book + account fills
    # ─────────────────────────────────────────────

    async def _lighter_ws_keepalive(self, ws):
        try:
            while not self.stop_flag:
                await asyncio.sleep(15)
                try:
                    pong = await ws.ping()
                    await asyncio.wait_for(pong, timeout=5)
                except Exception:
                    self.logger.warning("[Lighter WS] keepalive ping 无响应")
                    await ws.close()
                    return
        except asyncio.CancelledError:
            return

    async def _lighter_ws_loop(self):
        while not self.stop_flag:
            keepalive_task = None
            try:
                async with websockets.connect(
                    LIGHTER_WS_URL,
                    ping_interval=10, ping_timeout=5, close_timeout=5,
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

    # ── Lighter order event handler (for taker IOC fill tracking) ──

    def _handle_lighter_order_event(self, od: dict):
        status = od.get("status", "")
        if status in ("filled", "canceled", "partially_filled"):
            self._on_lighter_fill(od)

    def _on_lighter_fill(self, od: dict):
        filled_base = Decimal(od.get("filled_base_amount", "0"))
        if filled_base <= 0:
            return
        filled_quote = Decimal(od.get("filled_quote_amount", "0"))
        avg_price = filled_quote / filled_base if filled_base > 0 else Decimal("0")
        is_ask = od.get("is_ask", False)
        cid = od.get("client_order_id")
        oidx = od.get("order_index")
        side = "sell" if is_ask else "buy"

        # BUG-2: WS 重连去重
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

        # BUG-1: 识别对账修复单 — 跳过仓位更新和 stale 追加
        _cid_int = int(cid) if cid is not None else None
        if _cid_int is not None and _cid_int in self._repair_ioc_cids:
            self._repair_ioc_cids.discard(_cid_int)
            self.logger.info(
                f"[Lighter] 对账修复单成交(仓位已预更新, 忽略WS): "
                f"{side} {filled_base} @ {avg_price} (cid={cid})")
            return

        if self._in_flatten:
            _is_flatten_order = (cid is not None and self._flatten_ioc_cid is not None
                                 and int(cid) == self._flatten_ioc_cid)
            self._flatten_fill_price = avg_price
            self._flatten_fill_qty = filled_base
            self._flatten_fill_event.set()
            self.logger.info(
                f"[Lighter] {'平仓' if _is_flatten_order else '对冲'}成交: "
                f"{side} {filled_base} @ {avg_price}")
        else:
            if is_ask:
                self.lighter_position -= filled_base
            else:
                self.lighter_position += filled_base
            self._stale_fills.append({
                "side": side, "size": filled_base, "price": avg_price})
            self.logger.warning(
                f"[Lighter] 非预期成交(已更新仓位): "
                f"{side} {filled_base} @ {avg_price} (cid={cid})")

    # ─────────────────────────────────────────────
    #  Lighter API rate limiter
    # ─────────────────────────────────────────────

    async def _lighter_rate_wait(self):
        now = time.time()
        cutoff = now - LIGHTER_RATE_WINDOW
        while self._lighter_req_times and self._lighter_req_times[0] < cutoff:
            self._lighter_req_times.popleft()
        if len(self._lighter_req_times) >= LIGHTER_RATE_LIMIT:
            wait = self._lighter_req_times[0] + LIGHTER_RATE_WINDOW - now + 0.1
            if wait > 0:
                self.logger.warning(
                    f"[Lighter] 限速等待 {wait:.1f}s ({len(self._lighter_req_times)}次/60s)")
                await asyncio.sleep(wait)
        self._lighter_req_times.append(time.time())

    # ─────────────────────────────────────────────
    #  Lighter nonce generator (guaranteed unique)
    # ─────────────────────────────────────────────

    def _next_nonce(self) -> int:
        self._lighter_nonce += 1
        return self._lighter_nonce

    # ─────────────────────────────────────────────
    #  Lighter order placement (taker hedge)
    # ─────────────────────────────────────────────

    async def _lighter_taker_ioc(
        self, side: str, qty: Decimal, ref_price: Decimal,
        cid: Optional[int] = None, reduce_only: bool = False,
    ) -> bool:
        slip = Decimal("0.005")
        if side == "buy":
            price = ref_price * (Decimal("1") + slip)
        else:
            price = ref_price * (Decimal("1") - slip)
        price = self._round_lighter(price)
        qty_r = qty.quantize(self.lighter_min_size_step, rounding=ROUND_HALF_UP)
        if qty_r <= 0:
            return False
        _cid = cid or self._next_nonce()
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
                reduce_only=reduce_only,
                trigger_price=0,
            )
            if err:
                self.logger.warning(f"[Lighter] IOC {side} {qty_r} 失败: {err}")
                return False
            self.logger.info(f"[Lighter] IOC {side} {qty_r} @ {price}")
            return True
        except Exception as e:
            self.logger.warning(f"[Lighter] IOC异常: {e}")
            return False

    async def _cancel_all_lighter(self):
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

    # ─────────────────────────────────────────────
    #  Binance order placement
    # ─────────────────────────────────────────────

    async def _place_binance_limit(
        self, side: str, qty: Decimal, price: Decimal,
    ) -> Optional[str]:
        """Post-Only (GTX) limit order on Binance. Returns order ID."""
        try:
            order = await self._binance_exchange.create_order(
                symbol=self._b_ccxt_symbol,
                type="limit",
                side=side,
                amount=float(qty),
                price=float(price),
                params={"timeInForce": "GTX"},
            )
            oid = str(order.get("id", ""))
            self.logger.info(
                f"[Binance] 挂单(GTX): {side} {qty} @ {price} → id={oid}")
            return oid
        except Exception as e:
            err_str = str(e)
            if "would immediately" in err_str.lower() or "post only" in err_str.lower():
                self.logger.warning(
                    f"[Binance] GTX挂单被拒(会穿透): {side} @ {price}")
            else:
                self.logger.error(f"[Binance] 挂单异常: {e}")
            return None

    async def _cancel_binance_order(self, order_id: Optional[str]):
        if not order_id:
            return
        try:
            await self._binance_exchange.cancel_order(order_id, self._b_ccxt_symbol)
            self.logger.info(f"[Binance] 撤单: {order_id}")
        except Exception as e:
            err_str = str(e)
            if "unknown order" in err_str.lower() or "order does not exist" in err_str.lower():
                pass
            else:
                self.logger.warning(f"[Binance] 撤单异常: {e}")

    async def _binance_market_order(
        self, side: str, qty: Decimal,
    ) -> Optional[Decimal]:
        """Market order on Binance (taker, 4bps). Used as fallback."""
        try:
            order = await self._binance_exchange.create_order(
                symbol=self._b_ccxt_symbol,
                type="market",
                side=side,
                amount=float(qty),
            )
            avg = order.get("average") or order.get("price")
            filled = order.get("filled", 0)
            if avg and float(filled) > 0:
                _price = Decimal(str(avg))
                _qty = Decimal(str(filled))
                if side == "buy":
                    self.binance_position += _qty
                else:
                    self.binance_position -= _qty
                self.logger.info(
                    f"[Binance] 市价单: {side} {_qty} @ {_price} (taker)")
                return _price
            self.logger.warning(f"[Binance] 市价单成交异常: {order}")
            return None
        except Exception as e:
            self.logger.error(f"[Binance] 市价单异常: {e}")
            return None

    async def _check_binance_order(self, order_id: str) -> dict:
        """REST check order status (fallback if WS misses)."""
        try:
            order = await self._binance_exchange.fetch_order(
                order_id, self._b_ccxt_symbol)
            return order
        except Exception as e:
            self.logger.warning(f"[Binance] 查询订单异常: {e}")
            return {}

    # ─────────────────────────────────────────────
    #  Instant hedge: Lighter taker after Binance fill
    # ─────────────────────────────────────────────

    async def _instant_hedge_fire(self):
        """Binance fill 触发 → 立即 Lighter taker IOC 对冲。"""
        try:
            _b_side = self._b_fill_side
            _b_qty = self._b_fill_qty
            _b_price = self._b_fill_price
            _l_bid = self._fill_l_bid
            _l_ask = self._fill_l_ask

            if _b_side is None or _b_qty <= 0:
                return

            _hedge_side = "sell" if _b_side == "buy" else "buy"
            _ref = _l_bid if _hedge_side == "sell" else _l_ask

            if _ref is None or _ref <= 0:
                _ref = _b_price

            _cid = self._next_nonce()
            self._flatten_fill_event.clear()
            self._flatten_fill_price = None
            self._flatten_fill_qty = Decimal("0")
            self._flatten_ioc_cid = _cid
            self._in_flatten = True

            _t0 = time.time()
            ok = await self._lighter_taker_ioc(
                _hedge_side, _b_qty, _ref, cid=_cid)
            _ms = (time.time() - _t0) * 1000

            if ok:
                if _hedge_side == "buy":
                    self.lighter_position += _b_qty
                else:
                    self.lighter_position -= _b_qty

                if not self._flatten_fill_event.is_set():
                    try:
                        await asyncio.wait_for(
                            self._flatten_fill_event.wait(), timeout=1.0)
                    except asyncio.TimeoutError:
                        pass

                _actual_price = _ref
                if self._flatten_fill_price and self._flatten_fill_price > 0:
                    _actual_price = self._flatten_fill_price

                self._instant_hedge_ok = True
                self._instant_hedge_fill_price = _actual_price
                self._instant_hedge_ms = _ms
                self.logger.info(
                    f"[即时对冲] Lighter {_hedge_side} 成功! @ {_actual_price}  {_ms:.0f}ms")
            else:
                self._instant_hedge_ok = False
                self._instant_hedge_ms = _ms
                self.logger.warning(f"[即时对冲] Lighter IOC失败 ({_ms:.0f}ms)")

            self._in_flatten = False
            self._instant_hedge_done.set()
        except Exception as e:
            self.logger.error(f"[即时对冲] 异常: {e}\n{traceback.format_exc()}")
            self._instant_hedge_ok = False
            self._in_flatten = False
            self._instant_hedge_done.set()

    # ─────────────────────────────────────────────
    #  Close position
    # ─────────────────────────────────────────────

    async def _close_position(self, l_bid: Decimal, l_ask: Decimal,
                               b_bid: Decimal, b_ask: Decimal) -> bool:
        """逐层平仓: Binance JIT maker (0%) 先成交 → Lighter taker (0%) 跟进。
        和开仓完全对称: 不使用taker兜底, 未成交就撤单等下次收敛。"""

        l_qty = abs(self.lighter_position)
        b_qty = abs(self.binance_position)
        close_qty = min(l_qty, b_qty, self.order_size)

        if close_qty < self._b_min_qty:
            return False
        close_qty = close_qty.quantize(self._b_min_qty, rounding=ROUND_DOWN)
        if close_qty <= 0:
            return False

        if self.binance_position > 0:
            b_side = "sell"
            l_side = "buy"
            l_ref = l_ask
        else:
            b_side = "buy"
            l_side = "sell"
            l_ref = l_bid

        _t0 = time.time()

        # ── Step 1: Binance JIT maker (0%) — 先成交再动 Lighter ──
        jit_price = self._compute_binance_jit_price(
            b_side, b_bid, b_ask, self.close_jit_price_mode)

        self._b_fill_event.clear()
        self._b_fill_side = None
        self._b_order_side = b_side
        oid = await self._place_binance_limit(b_side, close_qty, jit_price)

        if not oid:
            self.logger.info("[平仓] Binance GTX挂单失败, 等下次收敛")
            return False

        self._b_order_id = oid
        _b_filled = False

        try:
            await asyncio.wait_for(
                self._b_fill_event.wait(), timeout=self.close_jit_wait)
            _b_filled = self._b_fill_event.is_set()
        except asyncio.TimeoutError:
            pass

        if not _b_filled:
            await self._cancel_binance_order(oid)
            await asyncio.sleep(0.1)
            if self._b_fill_event.is_set():
                _b_filled = True
                self.logger.info("[平仓] Binance 撤单时成交")
            else:
                rest_order = await self._check_binance_order(oid)
                _rest_filled = Decimal(str(rest_order.get("filled", 0)))
                if _rest_filled >= close_qty * Decimal("0.5"):
                    _rest_avg = rest_order.get("average") or rest_order.get("price")
                    self._b_fill_price = Decimal(str(_rest_avg)) if _rest_avg else jit_price
                    self._b_fill_qty = _rest_filled
                    self._b_fill_side = b_side
                    if b_side == "buy":
                        self.binance_position += _rest_filled
                    else:
                        self.binance_position -= _rest_filled
                    _b_filled = True
                    self.logger.info(
                        f"[平仓] Binance REST确认: {_rest_filled} @ {self._b_fill_price}")

        self._b_order_id = None

        if not _b_filled:
            self.logger.info(
                f"[平仓] Binance JIT {self.close_jit_wait}s未成交, "
                f"撤单等下次收敛 (0费用,不浪费)")
            return False

        _b_fill_price = self._b_fill_price
        _b_fill_qty = self._b_fill_qty

        # ── Step 2: Binance 已成交 → Lighter taker IOC 平仓 (0%) ──
        self._in_flatten = True
        self._flatten_fill_event.clear()
        self._flatten_fill_price = None
        self._flatten_fill_qty = Decimal("0")
        _flatten_cid = self._next_nonce()
        self._flatten_ioc_cid = _flatten_cid

        _close_qty_l = min(_b_fill_qty, close_qty)
        l_bid_now = self._l_best_bid
        l_ask_now = self._l_best_ask
        l_ref = l_ask_now if l_side == "buy" else l_bid_now
        if l_ref is None or l_ref <= 0:
            l_ref = _b_fill_price

        l_ok = await self._lighter_taker_ioc(
            l_side, _close_qty_l, l_ref, cid=_flatten_cid, reduce_only=True)
        if not l_ok:
            self.logger.error("[平仓] Lighter taker失败! Binance已成交, 进入REPAIR")
            self._in_flatten = False
            return False

        if l_side == "buy":
            self.lighter_position += _close_qty_l
        else:
            self.lighter_position -= _close_qty_l

        # ── 等 Lighter WS 拿实际成交价 ──
        if not self._flatten_fill_event.is_set():
            try:
                await asyncio.wait_for(
                    self._flatten_fill_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

        _l_actual = l_ref
        if self._flatten_fill_price and self._flatten_fill_price > 0:
            _l_actual = self._flatten_fill_price

        _l_real_qty = self._flatten_fill_qty if self._flatten_fill_qty > 0 else _close_qty_l
        if _l_real_qty != _close_qty_l:
            _diff = _close_qty_l - _l_real_qty
            if l_side == "buy":
                self.lighter_position -= _diff
            else:
                self.lighter_position += _diff
            self.logger.warning(
                f"[平仓] Lighter部分成交: 预期={_close_qty_l} 实际={_l_real_qty}")

        # ── 记账: 双侧都是 0 费用 ──
        _ms = (time.time() - _t0) * 1000
        _b_price = Decimal(str(_b_fill_price))
        if l_side == "buy":
            _spread_bps = float((_l_actual - _b_price) / _b_price * 10000)
        else:
            _spread_bps = float((_b_price - _l_actual) / _l_actual * 10000)
        _fee = (float(FEE_BINANCE_MAKER) + float(FEE_LIGHTER_TAKER)) * 10000
        _net = -_spread_bps - _fee
        self.cumulative_net_bps += _net
        _actual_qty = min(_l_real_qty, _b_fill_qty)
        _dollar = float(_net / 10000 * float(_actual_qty) * float(_b_price))
        self.cumulative_dollar_pnl += _dollar

        self.logger.info(
            f"[平仓完成] B_{b_side}@{_b_fill_price} → L_{l_side}@{_l_actual}  "
            f"spread={_spread_bps:.2f}bps fee={_fee:.2f}bps "
            f"net={_net:.2f}bps  ${_dollar:+.4f}  {_ms:.0f}ms  "
            f"累积=${self.cumulative_dollar_pnl:+.4f}")

        _real_pnl_str, _cum_real_str, _bal_l_str, _bal_b_str = \
            await self._bl_v1_verify_close_pnl(
                _net, _dollar, _actual_qty, _b_price)

        self._trade_seq += 1
        self._csv_row([
            datetime.now(_TZ_CN).isoformat(),
            f"bl_v1_close_{self._trade_seq:04d}",
            f"close_{l_side}",
            str(_b_fill_price), str(_l_actual), str(_actual_qty),
            f"{_spread_bps:.2f}", f"{_fee:.2f}", f"{_net:.2f}",
            f"{_ms:.0f}", f"{self.cumulative_net_bps:.2f}",
            _real_pnl_str, _cum_real_str, _bal_l_str, _bal_b_str,
        ])

        self._consecutive_close_fails = 0
        if self.ladder_step_bps > 0 and self._ladder_level > 0:
            _old = self._ladder_level
            self._ladder_level = max(0, self._ladder_level - 1)
            self.logger.info(f"[阶梯] 平仓1层, L{_old} → L{self._ladder_level}")

        _still_has = (abs(self.lighter_position) >= self._b_min_qty
                      and abs(self.binance_position) >= self._b_min_qty)
        if not _still_has and self._ladder_level > 0:
            self.logger.info(f"[阶梯] 全部平完, L{self._ladder_level} → L0")
            self._ladder_level = 0

        self._in_flatten = False
        return True

    # ─────────────────────────────────────────────
    #  Balance / Position helpers
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
        return None

    async def _get_lighter_balance(self) -> Optional[Decimal]:
        try:
            return await asyncio.to_thread(self._sync_get_lighter_balance)
        except Exception as e:
            self.logger.warning(f"[余额] Lighter余额查询失败: {e}")
            return None

    async def _get_binance_balance(self) -> Optional[Decimal]:
        try:
            balance = await self._binance_exchange.fetch_balance()
            usdc = balance.get("USDC", {})
            total = usdc.get("total")
            if total is not None:
                return Decimal(str(total))
            info = balance.get("info", {})
            assets = info.get("assets", [])
            for a in assets:
                if a.get("asset") == "USDC":
                    return Decimal(str(a.get("walletBalance", "0")))
            return None
        except Exception as e:
            self.logger.warning(f"[余额] Binance余额查询失败: {e}")
            return None

    async def _snapshot_balances(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        l_bal, b_bal = await asyncio.gather(
            self._get_lighter_balance(),
            self._get_binance_balance(),
        )
        return l_bal, b_bal

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

    async def _get_binance_position(self) -> Decimal:
        try:
            positions = await self._binance_exchange.fetch_positions(
                [self._b_ccxt_symbol])
            for pos in positions:
                contracts = pos.get("contracts", 0)
                if contracts and float(contracts) != 0:
                    side = pos.get("side", "")
                    qty = Decimal(str(abs(float(contracts))))
                    if side == "short":
                        return -qty
                    return qty
            return Decimal("0")
        except Exception as e:
            self.logger.warning(f"获取Binance仓位失败: {e}")
            return self.binance_position

    async def _reconcile(self):
        if self._instant_hedge_task and not self._instant_hedge_task.done():
            try:
                await asyncio.wait_for(self._instant_hedge_done.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass
        l = await self._get_lighter_position()
        b = await self._get_binance_position()
        _old_l, _old_b = self.lighter_position, self.binance_position
        self.lighter_position = l
        self.binance_position = b
        net = abs(l + b)
        if _old_l != l or _old_b != b:
            self.logger.info(f"[对账] 仓位同步: L {_old_l}→{l}  B {_old_b}→{b}")
        if net > self.order_size * Decimal("0.5"):
            self.logger.warning(f"[对账] 不平衡: L={l} B={b} net={net}")
        return net < self.order_size * Decimal("2")

    async def _emergency_flatten(self):
        self.logger.error("[紧急平仓] 开始...")
        self._in_flatten = True
        await self._cancel_all_lighter()
        await self._reconcile()

        if abs(self.lighter_position) >= self._b_min_qty:
            side = "sell" if self.lighter_position > 0 else "buy"
            qty = abs(self.lighter_position)
            ref = self._l_best_bid if side == "sell" else self._l_best_ask
            if ref is None or ref <= 0:
                _fallback = self._l_best_ask if side == "sell" else self._l_best_bid
                if _fallback and _fallback > 0:
                    ref = _fallback * (Decimal("0.99") if side == "sell" else Decimal("1.01"))
                    self.logger.warning(
                        f"[紧急平仓] Lighter无{('bid' if side=='sell' else 'ask')}, "
                        f"用对手方兜底: {ref}")
            if ref and ref > 0:
                _cid = self._next_nonce()
                self._flatten_ioc_cid = _cid
                await self._lighter_taker_ioc(side, qty, ref, cid=_cid, reduce_only=True)
                if side == "sell":
                    self.lighter_position -= qty
                else:
                    self.lighter_position += qty
                self.logger.info(f"[紧急平仓] Lighter {side} {qty} 已发送")
            else:
                self.logger.error(f"[紧急平仓] Lighter完全无BBO, 无法平仓!")

        if abs(self.binance_position) >= self._b_min_qty:
            side = "sell" if self.binance_position > 0 else "buy"
            qty = abs(self.binance_position)
            _price = await self._binance_market_order(side, qty)
            if _price:
                self.logger.info(f"[紧急平仓] Binance {side} {qty} @ {_price}")
            else:
                self.logger.error(f"[紧急平仓] Binance失败!")

        await asyncio.sleep(1.0)
        if not self.stop_flag:
            await self._reconcile()
            _net = abs(self.lighter_position + self.binance_position)
            if _net >= self._b_min_qty:
                self.logger.warning(f"[紧急平仓] 残余偏差={_net}, 尝试修复")
                _imbalance = self.lighter_position + self.binance_position
                if _imbalance < 0:
                    _ref = self._l_best_ask if self._l_best_ask else Decimal("0")
                    if _ref > 0:
                        await self._lighter_taker_ioc("buy", abs(_imbalance), _ref)
                elif _imbalance > 0:
                    _ref = self._l_best_bid if self._l_best_bid else Decimal("0")
                    if _ref > 0:
                        await self._lighter_taker_ioc("sell", abs(_imbalance), _ref)
                await asyncio.sleep(0.5)
                await self._reconcile()

        if self.ladder_step_bps > 0 and self._ladder_level > 0:
            self._ladder_level = 0
        self._in_flatten = False
        self._stale_fills.clear()
        self.logger.info(
            f"[紧急平仓] 完成: L={self.lighter_position} B={self.binance_position}")

    # ─────────────────────────────────────────────
    #  BL_V1: Balance P&L verification
    # ─────────────────────────────────────────────

    async def _bl_v1_record_pre_trade_balance(self):
        try:
            _l, _b = await self._snapshot_balances()
            if _l is not None:
                self._pre_trade_balance_l = _l
            if _b is not None:
                self._pre_trade_balance_b = _b
            if _l is not None and _b is not None:
                self.logger.info(
                    f"[BL_V1] 开仓后余额快照: L={_l} B={_b} 合计={_l+_b}")
        except Exception as e:
            self.logger.warning(f"[BL_V1] 开仓余额快照失败: {e}")

    async def _bl_v1_verify_close_pnl(self, formula_net_bps: float, formula_dollar: float,
                                     close_qty: Decimal, ref_price: Decimal
                                     ) -> Tuple[str, str, str, str]:
        try:
            _l_after, _b_after = await self._snapshot_balances()
            if _l_after is None or _b_after is None:
                return "", "", "", ""

            _bal_l_str = str(_l_after)
            _bal_b_str = str(_b_after)

            _pre_l = self._pre_trade_balance_l
            _pre_b = self._pre_trade_balance_b
            if _pre_l is None or _pre_b is None:
                self._pre_trade_balance_l = _l_after
                self._pre_trade_balance_b = _b_after
                return "", f"{self.cumulative_real_pnl:.6f}", _bal_l_str, _bal_b_str

            _real_pnl = float((_l_after + _b_after) - (_pre_l + _pre_b))
            self.cumulative_real_pnl += _real_pnl

            _real_bps = _real_pnl / (float(close_qty) * float(ref_price)) * 10000 \
                if close_qty > 0 and ref_price > 0 else 0
            _diff = _real_pnl - formula_dollar
            _diff_bps = abs(_real_bps - formula_net_bps)

            self.logger.info(
                f"[BL_V1 余额校验] "
                f"公式P&L=${formula_dollar:+.6f}({formula_net_bps:+.2f}bps)  "
                f"真实P&L=${_real_pnl:+.6f}({_real_bps:+.2f}bps)  "
                f"偏差=${_diff:+.6f}({_diff_bps:.2f}bps)")

            if _diff_bps > 2.0:
                self.logger.info(
                    f"[BL_V1] 单笔偏差={_diff_bps:.2f}bps (持仓期间未实现盈亏所致, 非异常)")

            if self._initial_balance_l is not None and self._initial_balance_b is not None:
                _total_now = _l_after + _b_after
                _total_init = self._initial_balance_l + self._initial_balance_b
                _session_real = float(_total_now - _total_init)
                _session_formula = self.cumulative_dollar_pnl
                _session_diff = abs(_session_real - _session_formula)
                self.logger.info(
                    f"[BL_V1 会话] "
                    f"公式累积=${_session_formula:+.6f}  "
                    f"真实累积=${_session_real:+.6f}  "
                    f"偏差=${_session_diff:.6f}")
                if _session_diff > 0.50:
                    self._consecutive_pnl_deviations += 1
                    self.logger.warning(
                        f"[BL_V1 !!] 会话累积偏差过大=${_session_diff:.4f} "
                        f"(>$0.50阈值) 连续{self._consecutive_pnl_deviations}次")
                    if self._consecutive_pnl_deviations >= 3:
                        self._circuit_breaker_until = time.time() + 300
                        self.logger.error(
                            f"[BL_V1 熔断] 会话累积偏差连续"
                            f"{self._consecutive_pnl_deviations}次>$0.50! "
                            f"熔断300秒 → 可能有隐藏费用或系统性滑点")
                        self._consecutive_pnl_deviations = 0
                else:
                    self._consecutive_pnl_deviations = 0

            self._pre_trade_balance_l = _l_after
            self._pre_trade_balance_b = _b_after
            return f"{_real_pnl:+.6f}", f"{self.cumulative_real_pnl:.6f}", _bal_l_str, _bal_b_str

        except Exception as e:
            self.logger.warning(f"[BL_V1] 余额校验异常: {e}")
            return "", "", "", ""

    # ─────────────────────────────────────────────
    #  Main entry
    # ─────────────────────────────────────────────

    async def run(self):
        self._lighter_ws_task: Optional[asyncio.Task] = None
        self._binance_book_ws_task: Optional[asyncio.Task] = None
        self._binance_user_ws_task: Optional[asyncio.Task] = None
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
            self.logger.info("[退出] 开始安全关闭...")
            try:
                await asyncio.wait_for(self._safe_shutdown(), timeout=15)
            except (asyncio.TimeoutError, Exception) as e:
                self.logger.error(f"[退出] 安全关闭异常: {e}")

            for t in [self._lighter_ws_task, self._binance_book_ws_task,
                       self._binance_user_ws_task]:
                if t and not t.done():
                    t.cancel()

            if self._binance_exchange:
                try:
                    await self._binance_exchange.close()
                except Exception:
                    pass
            if self._b_session:
                try:
                    await self._b_session.close()
                except Exception:
                    pass
            self.logger.info("[退出] 完成")

    async def _safe_shutdown(self):
        if self.dry_run:
            return
        try:
            await self._cancel_all_lighter()
        except Exception as e:
            self.logger.error(f"[退出] 撤Lighter单异常: {e}")
        if self._b_order_id:
            try:
                await self._cancel_binance_order(self._b_order_id)
            except Exception as e:
                self.logger.error(f"[退出] 撤Binance单异常: {e}")

        try:
            await self._reconcile()
        except Exception:
            pass

        if (abs(self.lighter_position) >= self._b_min_qty
                or abs(self.binance_position) >= self._b_min_qty):
            self.logger.info(
                f"[退出] 残余仓位: L={self.lighter_position} "
                f"B={self.binance_position}, 紧急平仓")
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
        fee_maker = float(FEE_BINANCE_MAKER) * 10000
        fee_taker = float(FEE_BINANCE_TAKER) * 10000
        _ladder_str = f"阶梯={self.ladder_step_bps}bps" if self.ladder_step_bps > 0 else "阶梯=关闭"

        self.logger.info(
            f"启动 BL_V1 币安USDC-M+Lighter  {self.symbol}  "
            f"size={self.order_size}  "
            f"开仓: B_maker(0bps) spread≥{self.open_spread_bps}bps "
            f"→ {self.open_jit_price_mode}挂单{self.open_jit_wait}秒 "
            f"→ L_taker对冲(0bps)  "
            f"平仓: B_JIT({self.close_jit_wait}s,0bps)→L_taker(0bps) 未成交则撤单等下次  "
            f"平仓<{self.close_spread_bps}bps  {_ladder_str}  "
            f"max_pos={self.max_position}  dry_run={self.dry_run}")

        if not self.dry_run:
            self._init_lighter()
            self._get_lighter_market_config()
            await self._init_binance()
            await self._cancel_all_lighter()

        self._lighter_ws_task = asyncio.create_task(self._lighter_ws_loop())
        self._binance_book_ws_task = asyncio.create_task(self._binance_book_ws_loop())
        if not self.dry_run:
            self._binance_user_ws_task = asyncio.create_task(self._binance_user_ws_loop())

        self.logger.info("等待行情就绪...")
        t0 = time.time()
        while time.time() - t0 < 20 and not self.stop_flag:
            b_bid, b_ask = self._get_binance_bbo()
            b_ok = b_bid is not None and b_ask is not None
            l_ok = self._l_best_bid is not None and self._l_best_ask is not None
            if b_ok and l_ok:
                break
            if time.time() - t0 > 5 and int(time.time() - t0) % 3 == 0:
                self.logger.info(
                    f"  等待中... B={'OK' if b_ok else 'WAIT'} "
                    f"L={'OK' if l_ok else 'WAIT'}")
            await asyncio.sleep(0.5)

        b_bid, b_ask = self._get_binance_bbo()
        if b_bid is None or self._l_best_bid is None:
            self.logger.error(
                f"行情超时! B={b_bid}/{b_ask} L={self._l_best_bid}/{self._l_best_ask}")
            return

        self.logger.info(
            f"行情就绪: L={self._l_best_bid}/{self._l_best_ask}  "
            f"B={b_bid}/{b_ask}")

        if not self.dry_run:
            await self._reconcile()
            _has_residual = (abs(self.lighter_position) >= self._b_min_qty
                             or abs(self.binance_position) >= self._b_min_qty)
            if _has_residual:
                _net = abs(self.lighter_position + self.binance_position)
                if getattr(self, "flatten_on_start", False):
                    await self._emergency_flatten()
                elif _net > self.order_size * Decimal("0.5"):
                    self.logger.warning(
                        f"[启动] 仓位不平衡: L={self.lighter_position} "
                        f"B={self.binance_position} → 紧急平仓")
                    await self._emergency_flatten()
                else:
                    self.logger.info(
                        f"[启动] 平衡仓位: L={self.lighter_position} "
                        f"B={self.binance_position}, 继续交易")

            _l_bal, _b_bal = await self._snapshot_balances()
            self._initial_balance_l = _l_bal
            self._initial_balance_b = _b_bal
            self._pre_trade_balance_l = _l_bal
            self._pre_trade_balance_b = _b_bal
            if _l_bal is not None and _b_bal is not None:
                self.logger.info(
                    f"[BL_V1] 初始余额: Lighter={_l_bal}  Binance={_b_bal}  "
                    f"合计={_l_bal + _b_bal}")

        self.logger.info(f"进入主循环  状态={self.state.value}")
        last_status_log = 0.0
        last_reconcile = time.time()

        while not self.stop_flag:
            try:
                now = time.time()
                b_bid, b_ask = self._get_binance_bbo()
                l_bid = self._l_best_bid
                l_ask = self._l_best_ask

                if b_bid is None or b_ask is None or l_bid is None or l_ask is None:
                    await asyncio.sleep(self.interval)
                    continue

                b_age = now - self._b_ws_ts if self._b_ws_ts > 0 else 999
                l_age = now - self._l_ws_ts if self._l_ws_ts > 0 else 999
                if l_age > 5 or b_age > 5:
                    await asyncio.sleep(self.interval)
                    continue

                mid = (l_bid + l_ask + b_bid + b_ask) / 4
                basis_bps = float((l_bid + l_ask - b_bid - b_ask) / 2 / mid * 10000)

                # ── Status log ──
                if now - last_status_log > 15:
                    self.logger.info(
                        f"[{self.state.value}] basis={basis_bps:+.1f}bps  "
                        f"L={l_bid}/{l_ask} B={b_bid}/{b_ask}  "
                        f"L_pos={self.lighter_position} B_pos={self.binance_position}  "
                        f"鲜度:L={l_age:.1f}s B={b_age:.1f}s  "
                        f"累积=${self.cumulative_dollar_pnl:+.4f}")
                    last_status_log = now

                # ── Periodic reconcile ──
                if not self.dry_run and now - last_reconcile > 60 and self.state == State.IDLE:
                    await self._reconcile()
                    last_reconcile = now
                    _net = self.lighter_position + self.binance_position
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
                                f"[对账] 偏差={_imbalance} → Lighter IOC {_repair_side} {_repair_qty}")
                            _repair_cid = self._next_nonce()
                            self._repair_ioc_cids.add(_repair_cid)
                            self._in_flatten = True
                            _ref = self._l_best_ask if _repair_side == "buy" else self._l_best_bid
                            if _ref and _ref > 0:
                                _rok = await self._lighter_taker_ioc(
                                    _repair_side, _repair_qty, _ref, cid=_repair_cid)
                                if _rok:
                                    if _repair_side == "buy":
                                        self.lighter_position += _repair_qty
                                    else:
                                        self.lighter_position -= _repair_qty
                                else:
                                    self._repair_ioc_cids.discard(_repair_cid)
                            else:
                                self._repair_ioc_cids.discard(_repair_cid)
                            await asyncio.sleep(0.5)
                            self._in_flatten = False
                        else:
                            self.logger.error(
                                f"[对账] 偏差异常={_imbalance}, 跳过自动修复")

                # ── Stale fills ──
                if self._stale_fills and self.state == State.IDLE:
                    sf = self._stale_fills.pop(0)
                    self.logger.warning(f"[过期成交] {sf} → 触发对账")
                    await self._reconcile()
                    last_reconcile = now
                    continue

                # ━━━━━━ STATE: IDLE ━━━━━━
                if self.state == State.IDLE:
                    if now < self._circuit_breaker_until:
                        await asyncio.sleep(1)
                        continue

                    if self.dry_run:
                        await asyncio.sleep(self.interval)
                        continue

                    _has_pos = (abs(self.lighter_position) >= self._b_min_qty
                                and abs(self.binance_position) >= self._b_min_qty)

                    # ── 平仓检测 ──
                    if _has_pos:
                        if self.lighter_position < 0:
                            _should_close = basis_bps <= self.close_spread_bps
                        else:
                            _should_close = basis_bps >= -self.close_spread_bps
                        if _should_close:
                            _total_layers = int(min(abs(self.lighter_position),
                                                    abs(self.binance_position)) / self.order_size)
                            _fast_layers = min(_total_layers, 3)
                            self.logger.info(
                                f"[IDLE] 价差收敛! basis={basis_bps:.1f}bps  "
                                f"平{_fast_layers}/{_total_layers}层")

                            _closed = 0
                            _failed = False
                            for _ in range(_fast_layers):
                                if self.stop_flag:
                                    break
                                ok = await self._close_position(
                                    l_bid, l_ask, b_bid, b_ask)
                                if ok:
                                    _closed += 1
                                    b_bid, b_ask = self._get_binance_bbo()
                                    l_bid = self._l_best_bid
                                    l_ask = self._l_best_ask
                                else:
                                    _failed = True
                                    break

                            if _failed:
                                self.state = State.REPAIR
                                continue

                            _still = (abs(self.lighter_position) >= self._b_min_qty
                                      and abs(self.binance_position) >= self._b_min_qty)
                            if _still and self.close_wait_sec > 0:
                                self.logger.info(
                                    f"[IDLE] 已平{_closed}层, 等待{self.close_wait_sec}s")
                                await asyncio.sleep(self.close_wait_sec)

                                b_bid, b_ask = self._get_binance_bbo()
                                l_bid = self._l_best_bid
                                l_ask = self._l_best_ask
                                if b_bid is None or l_bid is None:
                                    continue
                                mid = (l_bid + l_ask + b_bid + b_ask) / 4
                                _new_basis = float(
                                    (l_bid + l_ask - b_bid - b_ask) / 2 / mid * 10000)

                                if self.lighter_position < 0:
                                    _deep_ok = _new_basis <= self.close_deep_bps
                                else:
                                    _deep_ok = _new_basis >= -self.close_deep_bps
                                if _deep_ok:
                                    self.logger.info(
                                        f"[IDLE] 价差继续收敛 → 平剩余仓位")
                                    while (abs(self.lighter_position) >= self._b_min_qty
                                           and abs(self.binance_position) >= self._b_min_qty):
                                        if self.stop_flag:
                                            break
                                        ok = await self._close_position(
                                            l_bid, l_ask, b_bid, b_ask)
                                        if not ok:
                                            self.state = State.REPAIR
                                            break
                                        b_bid, b_ask = self._get_binance_bbo()
                                        l_bid = self._l_best_bid
                                        l_ask = self._l_best_ask
                                else:
                                    self.logger.info(
                                        f"[IDLE] 价差反弹 basis={_new_basis:.1f}bps → 保留仓位")
                            continue

                    # ── 持仓上限检查 ──
                    _abs_l = abs(self.lighter_position)
                    _abs_b = abs(self.binance_position)
                    _at_max = (_abs_l >= self.max_position or _abs_b >= self.max_position)
                    if _at_max:
                        await asyncio.sleep(1)
                        continue

                    # ━━━━━━ BL_V1 JIT开仓: Binance maker + Lighter taker ━━━━━━
                    if self.lighter_position < -self.max_position * Decimal("0.8"):
                        _allow_sell_b = False
                    else:
                        _allow_sell_b = True
                    if self.lighter_position > self.max_position * Decimal("0.8"):
                        _allow_buy_b = False
                    else:
                        _allow_buy_b = True

                    effective_bps = (self.open_spread_bps
                                     + self._ladder_level * self.ladder_step_bps
                                     + self.spread_buffer_bps)

                    _sell_b_spread = float((b_ask - l_ask) / l_ask * 10000) if l_ask > 0 else 0
                    _buy_b_spread = float((l_bid - b_bid) / b_bid * 10000) if b_bid > 0 else 0

                    _jit_side = None
                    _jit_price = None
                    _jit_spread = 0

                    if _allow_sell_b and _sell_b_spread >= effective_bps:
                        _jit_side = "sell"
                        _jit_price = self._compute_binance_jit_price(
                            "sell", b_bid, b_ask, self.open_jit_price_mode)
                        _jit_spread = _sell_b_spread
                    elif _allow_buy_b and _buy_b_spread >= effective_bps:
                        _jit_side = "buy"
                        _jit_price = self._compute_binance_jit_price(
                            "buy", b_bid, b_ask, self.open_jit_price_mode)
                        _jit_spread = _buy_b_spread

                    if _jit_side and _jit_price and _jit_price > 0:
                        self.logger.info(
                            f"[JIT] B_{_jit_side} spread={_jit_spread:.1f}bps "
                            f"≥ {effective_bps:.1f}bps  "
                            f"挂@{_jit_price} ({self.open_jit_price_mode}) "
                            f"→ 等{self.open_jit_wait}秒")

                        self._b_fill_event.clear()
                        self._b_fill_side = None
                        self._b_order_side = _jit_side
                        oid = await self._place_binance_limit(
                            _jit_side, self.order_size, _jit_price)

                        if oid:
                            self._b_order_id = oid

                            _filled = False
                            try:
                                await asyncio.wait_for(
                                    self._b_fill_event.wait(),
                                    timeout=self.open_jit_wait)
                                _filled = self._b_fill_event.is_set()
                            except asyncio.TimeoutError:
                                pass

                            if _filled:
                                self.logger.info("[JIT] Binance成交! → HEDGING")
                                self.state = State.HEDGING
                                self._b_order_id = None
                                continue
                            else:
                                await self._cancel_binance_order(oid)
                                await asyncio.sleep(0.1)
                                if self._b_fill_event.is_set():
                                    self.logger.info(
                                        "[JIT] Binance撤单时成交 → HEDGING")
                                    self.state = State.HEDGING
                                    self._b_order_id = None
                                    continue

                                rest_order = await self._check_binance_order(oid)
                                _rest_filled = Decimal(
                                    str(rest_order.get("filled", 0)))
                                if _rest_filled >= self.order_size * Decimal("0.5"):
                                    _rest_avg = rest_order.get("average") or \
                                        rest_order.get("price")
                                    self._b_fill_price = Decimal(str(_rest_avg)) \
                                        if _rest_avg else _jit_price
                                    self._b_fill_qty = _rest_filled
                                    self._b_fill_side = _jit_side
                                    if _jit_side == "buy":
                                        self.binance_position += _rest_filled
                                    else:
                                        self.binance_position -= _rest_filled
                                    self._fill_l_bid = self._l_best_bid
                                    self._fill_l_ask = self._l_best_ask
                                    self._fill_ts = time.time()
                                    self._b_fill_event.set()
                                    self._instant_hedge_done.clear()
                                    self._instant_hedge_ok = False
                                    self._instant_hedge_task = asyncio.create_task(
                                        self._instant_hedge_fire())
                                    self.logger.warning(
                                        f"[JIT] WS丢失fill! REST确认: "
                                        f"{_rest_filled} @ {self._b_fill_price} → HEDGING")
                                    self.state = State.HEDGING
                                    self._b_order_id = None
                                    continue

                                self._b_order_id = None
                                self.logger.info(
                                    f"[JIT] {self.open_jit_wait}秒未成交, 撤单")
                        else:
                            await asyncio.sleep(0.5)
                    else:
                        await asyncio.sleep(self.interval)

                # ━━━━━━ STATE: HEDGING ━━━━━━
                elif self.state == State.HEDGING:
                    _b_side = self._b_fill_side
                    _b_qty = self._b_fill_qty
                    _b_price = self._b_fill_price

                    try:
                        await asyncio.wait_for(
                            self._instant_hedge_done.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        self.logger.warning("[HEDGING] 即时对冲超时")

                    _hedge_ok = self._instant_hedge_ok
                    _l_fill_price = self._instant_hedge_fill_price
                    _hedge_ms = self._instant_hedge_ms

                    if not _hedge_ok:
                        l_bid = self._l_best_bid
                        l_ask = self._l_best_ask
                        _hedge_side = "sell" if _b_side == "buy" else "buy"
                        _ref = l_bid if _hedge_side == "sell" else l_ask
                        if _ref is None or _ref <= 0:
                            _ref = _b_price

                        self.logger.info(
                            f"[HEDGING] 即时对冲失败, fallback重试 ref={_ref}")
                        _t_start = time.time()
                        for _retry in range(3):
                            if self.stop_flag:
                                break
                            _cid = self._next_nonce()
                            self._flatten_fill_event.clear()
                            self._flatten_fill_price = None
                            self._flatten_ioc_cid = _cid
                            self._in_flatten = True
                            ok = await self._lighter_taker_ioc(
                                _hedge_side, _b_qty, _ref, cid=_cid)
                            if ok:
                                if _hedge_side == "buy":
                                    self.lighter_position += _b_qty
                                else:
                                    self.lighter_position -= _b_qty

                                try:
                                    await asyncio.wait_for(
                                        self._flatten_fill_event.wait(), timeout=1.0)
                                except asyncio.TimeoutError:
                                    pass

                                _l_fill_price = _ref
                                if self._flatten_fill_price and self._flatten_fill_price > 0:
                                    _l_fill_price = self._flatten_fill_price
                                _hedge_ok = True
                                self._in_flatten = False
                                break
                            await asyncio.sleep(0.2)
                            l_bid = self._l_best_bid
                            l_ask = self._l_best_ask
                            if l_bid and l_ask:
                                _ref = l_bid if _hedge_side == "sell" else l_ask
                        self._in_flatten = False
                        _hedge_ms = (time.time() - _t_start) * 1000

                    if _hedge_ok and _l_fill_price:
                        # BUG-5: 检查追加成交，补充对冲增量
                        _pending = self._fill_qty_pending
                        if _pending > 0:
                            self.logger.info(f"[HEDGING] 追加成交待补对冲: {_pending}")
                            _hedge_side = "sell" if _b_side == "buy" else "buy"
                            l_bid = self._l_best_bid
                            l_ask = self._l_best_ask
                            _sup_ref = l_bid if _hedge_side == "sell" else l_ask
                            if _sup_ref and _sup_ref > 0:
                                _sup_cid = self._next_nonce()
                                self._flatten_fill_event.clear()
                                self._flatten_ioc_cid = _sup_cid
                                self._in_flatten = True
                                _sup_ok = await self._lighter_taker_ioc(
                                    _hedge_side, _pending, _sup_ref, cid=_sup_cid)
                                if _sup_ok:
                                    if _hedge_side == "buy":
                                        self.lighter_position += _pending
                                    else:
                                        self.lighter_position -= _pending
                                    self.logger.info(
                                        f"[HEDGING] 追加对冲成功: {_hedge_side} {_pending}")
                                else:
                                    self.logger.warning(
                                        f"[HEDGING] 追加对冲失败, 将在对账中修复")
                                self._in_flatten = False
                            self._fill_qty_pending = Decimal("0")

                        if _b_side == "buy":
                            _spread = float((_l_fill_price - _b_price) / _b_price * 10000)
                        else:
                            _spread = float((_b_price - _l_fill_price) / _l_fill_price * 10000)
                        _fee = (float(FEE_BINANCE_MAKER) + float(FEE_LIGHTER_TAKER)) * 10000
                        _net = _spread - _fee
                        self.cumulative_net_bps += _net
                        _dollar = _net / 10000 * float(_b_qty) * float(_b_price)
                        self.cumulative_dollar_pnl += _dollar

                        self.logger.info(
                            f"[成交] B_{_b_side}@{_b_price} → L_{'sell' if _b_side == 'buy' else 'buy'}"
                            f"@{_l_fill_price}  "
                            f"spread={_spread:.2f}bps fee={_fee:.2f}bps "
                            f"net={_net:+.2f}bps  "
                            f"${_dollar:+.4f}  hedge={_hedge_ms:.0f}ms  "
                            f"累积=${self.cumulative_dollar_pnl:+.4f}")

                        self._trade_seq += 1
                        self._csv_row([
                            datetime.now(_TZ_CN).isoformat(),
                            f"bl_v1_{self._trade_seq:04d}",
                            _b_side,
                            str(_b_price), str(_l_fill_price), str(_b_qty),
                            f"{_spread:.2f}", f"{_fee:.2f}", f"{_net:.2f}",
                            f"{_hedge_ms:.0f}",
                            f"{self.cumulative_net_bps:.2f}",
                            "", "", "", "",
                        ])
                        self._consecutive_hedge_fails = 0

                        if self.ladder_step_bps > 0:
                            self._ladder_level += 1
                            self.logger.info(
                                f"[阶梯] 升级到 L{self._ladder_level}")

                        asyncio.create_task(self._bl_v1_record_pre_trade_balance())
                    else:
                        self.logger.error("[HEDGING] 对冲失败! 进入修复")
                        self._consecutive_hedge_fails += 1
                        if self._consecutive_hedge_fails >= 3:
                            self._circuit_breaker_until = time.time() + 60
                            self.logger.error("[熔断] 暂停60秒")
                        self.state = State.REPAIR
                        continue

                    self._reset_cycle()
                    self.state = State.IDLE

                # ━━━━━━ STATE: REPAIR ━━━━━━
                elif self.state == State.REPAIR:
                    self.logger.info("[REPAIR] 对账并修复...")
                    await self._cancel_all_lighter()
                    if self._b_order_id:
                        await self._cancel_binance_order(self._b_order_id)
                        self._b_order_id = None
                    await self._reconcile()
                    if abs(self.lighter_position + self.binance_position) > self.order_size * Decimal("0.5"):
                        await self._emergency_flatten()
                    _net_after_repair = abs(self.lighter_position + self.binance_position)
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
                    self._in_flatten = False
                await asyncio.sleep(2)

    def _reset_cycle(self):
        self._b_fill_event.clear()
        self._b_fill_side = None
        self._b_fill_price = Decimal("0")
        self._b_fill_qty = Decimal("0")
        self._b_order_id = None
        self._b_order_side = None
        self._fill_l_bid = None
        self._fill_l_ask = None
        self._fill_ts = 0.0
        self._fill_qty_pending = Decimal("0")
        if self._instant_hedge_task and not self._instant_hedge_task.done():
            self._instant_hedge_task.cancel()
        self._instant_hedge_done.clear()
        self._instant_hedge_ok = False
        self._instant_hedge_fill_price = None
        self._instant_hedge_task = None


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(
        description="Binance-Lighter V1: 币安 USDC-M (JIT Maker 0%) + Lighter (Taker 0%)")
    p.add_argument("-s", "--symbol", default="BTC")
    p.add_argument("--size", type=str, default="0.001")
    p.add_argument("--max-position", type=str, default="0.01")
    p.add_argument("--open-spread", type=float, default=2.0,
                   help="开仓价差阈值bps (默认2.0, 0费率所以阈值可更低)")
    p.add_argument("--close-spread", type=float, default=0.0,
                   help="平仓价差阈值bps (默认0)")
    p.add_argument("--max-order-age", type=float, default=30.0)
    p.add_argument("--spread-buffer", type=float, default=0.0,
                   help="价差缓冲bps (默认0)")
    p.add_argument("--ladder-step", type=float, default=1.5,
                   help="阶梯间距bps (默认1.5)")
    p.add_argument("--close-deep", type=float, default=-0.5)
    p.add_argument("--close-wait", type=float, default=2.0)
    p.add_argument("--open-jit-wait", type=float, default=2.0,
                   help="开仓Binance JIT等待秒数 (默认2.0)")
    p.add_argument("--close-jit-wait", type=float, default=1.0,
                   help="平仓Binance JIT等待秒数 (默认1.0, 0=直接taker)")
    p.add_argument("--open-jit-price", type=str, default="at_best",
                   choices=["at_best", "improve", "behind"],
                   help="开仓Binance JIT定价 (默认at_best)")
    p.add_argument("--close-jit-price", type=str, default="at_best",
                   choices=["at_best", "improve", "behind"],
                   help="平仓Binance JIT定价 (默认at_best)")
    p.add_argument("--hedge-timeout", type=float, default=5.0)
    p.add_argument("--interval", type=float, default=0.05)
    p.add_argument("--flatten-on-start", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--env-file", default=".env")
    args = p.parse_args()

    load_dotenv(args.env_file)

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
        open_jit_wait=args.open_jit_wait,
        close_jit_wait=args.close_jit_wait,
        open_jit_price_mode=args.open_jit_price,
        close_jit_price_mode=args.close_jit_price,
    )
    bot.flatten_on_start = args.flatten_on_start

    fee_round_trip = (float(FEE_BINANCE_MAKER) + float(FEE_LIGHTER_TAKER)) * 10000 * 2
    min_profit_jit = args.open_spread - abs(args.close_spread) - fee_round_trip

    step_info = f"  阶梯={args.ladder_step}bps" if args.ladder_step > 0 else "  阶梯=关闭"
    print(f"[BL_V1] 币安USDC-M(maker 0%) + Lighter(taker 0%)")
    print(f"  {args.symbol} size={args.size} max_pos={args.max_position}")
    print(f"  开仓: B_maker(0bps) spread≥{args.open_spread}bps "
          f"→ {args.open_jit_price}挂单{args.open_jit_wait}秒 → L_taker对冲(0bps)")
    print(f"  平仓: B_JIT({args.close_jit_wait}s,0bps) → L_taker(0bps)")
    print(f"  平仓策略: 未成交则撤单等下次收敛, 不用taker兜底")
    print(f"  平仓阈值: basis≤{args.close_spread}bps")
    print(step_info)
    print(f"  开仓费=0bps  平仓费=0bps  往返=0bps")
    print(f"  预期每笔净利={min_profit_jit:+.2f}bps (纯利润, 无手续费)")

    if uvloop is not None:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("[uvloop] 已启用")

    print(f"\n需要环境变量: BINANCE_API_KEY, BINANCE_API_SECRET, "
          f"API_KEY_PRIVATE_KEY (Lighter)")

    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n已中断。")


if __name__ == "__main__":
    main()
