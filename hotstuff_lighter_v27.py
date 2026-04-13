#!/usr/bin/env python3
"""
Hotstuff + Lighter V27: 双腿Taker套利 + 阶梯建仓 + 快速平仓

核心: 检测到价差 → 同时在Lighter和Hotstuff发IOC taker → 即时锁定价差
  - 不挂maker单, 不等待被吃, 主动出手
  - 开仓: Lighter IOC sell(吃bid) + Hotstuff IOC buy(吃ask) 同时发出
  - 平仓: Lighter IOC buy + Hotstuff IOC sell (快速3层+深度观察)

费用:
  Lighter 免费账户 taker: 0 bps  |  Hotstuff taker: 2.5 bps

用法:
  python hotstuff_lighter_v27.py --symbol BTC --size 0.005 --max-position 0.025 \
      --open-spread 5.0 --close-spread 0 --ladder-step 1.5
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
from eth_account import Account
from lighter.signer_client import SignerClient

from hotstuff import ExchangeClient, InfoClient
from hotstuff.methods.exchange.trading import (
    UnitOrder, PlaceOrderParams, CancelAllParams,
)
from hotstuff.methods.exchange.op_codes import EXCHANGE_OP_CODES
from hotstuff.methods.info.account import PositionsParams, FillsParams
from hotstuff.utils.signing import sign_action
from hotstuff.utils.nonce import NonceManager

sys.path.insert(0, str(Path(__file__).resolve().parent))
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

FEE_HOTSTUFF_TAKER = Decimal("0.00025")  # 2.5 bps
FEE_LIGHTER_MAKER = Decimal("0")          # 免费账户 0 bps

LIGHTER_RATE_LIMIT = 36       # 安全上限(实际40), 留4次给紧急操作
LIGHTER_RATE_WINDOW = 60.0    # 窗口60秒

_TZ_CN = timezone(timedelta(hours=8))


class State(str, Enum):
    IDLE = "IDLE"
    REPAIR = "REPAIR"


# ═══════════════════════════════════════════════════════════════════
#  Bot
# ═══════════════════════════════════════════════════════════════════

class HotstuffLighterBot:

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

        # ── Hotstuff config ──
        self.hs_symbol: str = f"{self.symbol}-PERP"
        self.hs_instrument_id: int = 0
        self.hs_tick_size: Decimal = Decimal("0.01")
        self.hs_lot_size: Decimal = Decimal("0.001")
        self.hs_address: str = ""

        # ── Lighter API rate limiter (40 req / 60s for free accounts) ──
        self._lighter_req_times: collections.deque = collections.deque()

        # ── Close fill tracking (Lighter actual price during flatten) ──
        self._flatten_fill_price: Optional[Decimal] = None
        self._flatten_fill_qty = Decimal("0")
        self._flatten_fill_event = asyncio.Event()
        self._flatten_ioc_cid: Optional[int] = None

        # ── Stale fills queue ──
        self._stale_fills: List[dict] = []

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

        # ── Async Hotstuff direct client (bypass SDK, aiohttp keep-alive) ──
        self._hs_session: Optional[aiohttp.ClientSession] = None
        self._hs_wallet = None
        self._hs_nonce = NonceManager()
        self._hs_api_url = "https://api.hotstuff.trade/"

        # ── Slippage protection ──
        self._max_slippage_bps: float = 5.0

        # ── Fire-and-forget safety lock ──
        self._pending_hs_verify: bool = False
        self._pending_open_context: Optional[dict] = None

        # ── Open fill tracking (Lighter WS confirmation) ──
        self._open_fill_event = asyncio.Event()
        self._open_fill_data: Optional[dict] = None
        self._open_pending_coi: Optional[int] = None
        self._last_lighter_avg_price: Optional[Decimal] = None
        self._last_hotstuff_avg_price: Optional[Decimal] = None
        self._last_hs_fill_id: Optional[str] = None
        self._fills_api_broken: bool = False

        # ── Risk ──
        self._consecutive_hedge_fails: int = 0
        self._circuit_breaker_until: float = 0.0
        self.cumulative_net_bps: float = 0.0
        self.cumulative_dollar_pnl: float = 0.0
        self._trade_seq: int = 0
        self.max_loss: float = 0.0
        self._loss_breaker: bool = False

        # ── Balance-based P&L tracking ──
        self._initial_balance_l: Optional[Decimal] = None
        self._initial_balance_h: Optional[Decimal] = None
        self._pre_trade_balance_l: Optional[Decimal] = None
        self._pre_trade_balance_h: Optional[Decimal] = None
        self.cumulative_real_pnl: float = 0.0

        # ── Ladder level (开仓+1, 逐层平仓-1, 全平归零) ──
        self._ladder_level: int = 0

        # ── Logging / CSV ──
        os.makedirs("logs", exist_ok=True)
        self.csv_path = f"logs/hs_lighter_v27_{self.symbol}_trades.csv"
        self.log_path = f"logs/hs_lighter_v27_{self.symbol}.log"
        self._init_csv()
        self.logger = self._init_logger()

    # ─────────────────────────────────────────────
    #  Logging / CSV
    # ─────────────────────────────────────────────

    def _init_logger(self) -> logging.Logger:
        lg = logging.getLogger(f"hs_v27_{self.symbol}")
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

    def _init_hotstuff_client(self):
        pk = os.getenv("HOTSTUFF_PRIVATE_KEY")
        if not pk:
            raise ValueError("HOTSTUFF_PRIVATE_KEY not set")
        wallet = Account.from_key(pk)
        api_wallet_addr = wallet.address
        self.hs_address = os.getenv("HOTSTUFF_ADDRESS", api_wallet_addr)
        self._hs_wallet = wallet
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
                self.hs_instrument_id = int(pid)
                self.hs_tick_size = Decimal(str(tick))
                self.hs_lot_size = Decimal(str(lot))
                self.logger.info(
                    f"Hotstuff instrument: {self.hs_symbol} id={self.hs_instrument_id} "
                    f"tick={self.hs_tick_size} lot={self.hs_lot_size}")
                return
        raise RuntimeError(f"{self.hs_symbol} not found on Hotstuff")

    def _round_lighter(self, price: Decimal) -> Decimal:
        if self.lighter_tick > 0:
            return (price / self.lighter_tick).quantize(Decimal("1")) * self.lighter_tick
        return price

    # ─────────────────────────────────────────────
    #  Hotstuff BBO helper
    # ─────────────────────────────────────────────

    def _get_hotstuff_ws_bbo(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        if self.hs_feed and self.hs_feed.snapshot.connected:
            snap = self.hs_feed.snapshot
            if snap.best_bid and snap.best_ask:
                return Decimal(str(snap.best_bid)), Decimal(str(snap.best_ask))
        return None, None

    def _get_l1_depth(self, l_side: str, h_side: str
                      ) -> Tuple[Decimal, Decimal]:
        """读取两个交易所第一档挂单量, 限制IOC不超过L1深度。"""
        if l_side == "sell" and self._l_ob["bids"]:
            best_bid_p = max(self._l_ob["bids"].keys())
            l1_l = Decimal(str(self._l_ob["bids"][best_bid_p]))
        elif l_side == "buy" and self._l_ob["asks"]:
            best_ask_p = min(self._l_ob["asks"].keys())
            l1_l = Decimal(str(self._l_ob["asks"][best_ask_p]))
        else:
            l1_l = Decimal("999")

        hs = self.hs_feed.snapshot if self.hs_feed else None
        if h_side == "buy" and hs and hs.asks:
            l1_h = Decimal(str(hs.asks[0].size))
        elif h_side == "sell" and hs and hs.bids:
            l1_h = Decimal(str(hs.bids[0].size))
        else:
            l1_h = Decimal("999")

        return l1_l, l1_h

    # ─────────────────────────────────────────────
    #  Lighter WS: order book + account orders
    # ─────────────────────────────────────────────

    async def _lighter_ws_keepalive(self, ws):
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
        elif status == "canceled":
            filled_base = Decimal(od.get("filled_base_amount", "0"))
            if filled_base > 0:
                self._on_lighter_fill(od)
        elif status == "partially_filled":
            self.logger.info(
                f"[Lighter] 部分成交(等最终确认): "
                f"{od.get('filled_base_amount','?')} @ {od.get('filled_quote_amount','?')}")

    def _on_lighter_fill(self, od: dict):
        """处理平仓和开仓IOC的WS成交确认。"""
        try:
            filled_base = Decimal(od.get("filled_base_amount", "0"))
            filled_quote = Decimal(od.get("filled_quote_amount", "0"))
        except Exception:
            return
        avg_price = filled_quote / filled_base if filled_base > 0 else Decimal("0")
        is_ask = od.get("is_ask", False)
        cid = od.get("client_order_id")
        side = "sell" if is_ask else "buy"

        def _cid_match(target) -> bool:
            if cid is None or target is None:
                return False
            try:
                return int(cid) == int(target)
            except (ValueError, TypeError):
                return False

        if self._open_pending_coi is not None and _cid_match(self._open_pending_coi):
            self._open_fill_data = od
            self._last_lighter_avg_price = avg_price
            self._open_fill_event.set()
            self.logger.info(
                f"[Lighter] 开仓WS确认: {side} {filled_base} @ {avg_price}")
            return

        if self._in_flatten:
            if not _cid_match(self._flatten_ioc_cid):
                self.logger.warning(
                    f"[Lighter] 平仓期间收到非平仓成交! cid={cid}")
            self._flatten_fill_price = avg_price
            self._flatten_fill_qty = filled_base
            self._flatten_fill_event.set()
            self.logger.info(
                f"[Lighter] 平仓成交(实际价={avg_price}): {side} {filled_base}")
        else:
            if _cid_match(self._flatten_ioc_cid):
                return
            self.logger.info(
                f"[Lighter] WS成交通知: {side} {filled_base} @ {avg_price}")

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
                self.logger.warning(f"[Lighter] 限速等待 {wait:.1f}s (已{len(self._lighter_req_times)}次/60s)")
                await asyncio.sleep(wait)
        self._lighter_req_times.append(time.time())

    # ─────────────────────────────────────────────
    #  Lighter order placement / cancel
    # ─────────────────────────────────────────────

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
        await self._lighter_rate_wait()
        try:
            _, _, err = await self.lighter_client.create_order(
                market_index=self.lighter_market_index,
                client_order_index=int(time.time() * 1000),
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
                return False
            if side == "buy":
                self.lighter_position += qty_r
            else:
                self.lighter_position -= qty_r
            self.logger.info(f"[对账] Lighter IOC {side} {qty_r} @ {price} 已修复")
            return True
        except Exception as e:
            self.logger.warning(f"[对账] Lighter IOC异常: {e}")
            return False

    # ─────────────────────────────────────────────
    #  Async Hotstuff direct API (aiohttp, bypass SDK)
    # ─────────────────────────────────────────────

    async def _hs_ensure_session(self):
        if self._hs_session is None or self._hs_session.closed:
            self._hs_session = aiohttp.ClientSession(
                headers={"Content-Type": "application/json",
                         "Accept-Encoding": "gzip, deflate, br"},
                timeout=aiohttp.ClientTimeout(total=5),
            )
            await self._hs_info_post("positions", {"user": self.hs_address})
            self.logger.info("[Hotstuff] aiohttp session 已创建 (keep-alive 已预热)")

    def _hs_sdk_prepare_order(self, params_dict: dict) -> dict:
        """用 SDK 签名引擎准备 placeOrder 请求, 返回可直接发送的 payload。"""
        prepared = self.hs_exchange._execute_action(
            {"action": "placeOrder", "params": params_dict},
            execute=False,
        )
        return {
            "action": {"data": prepared["params"],
                       "type": str(EXCHANGE_OP_CODES["placeOrder"])},
            "signature": prepared["signature"],
            "nonce": prepared["params"]["nonce"],
        }

    async def _hs_exchange_send(self, payload: dict) -> dict:
        """通过 aiohttp keep-alive 发送已签名的 payload。"""
        async with self._hs_session.post(
            self._hs_api_url + "exchange", json=payload
        ) as resp:
            body = await resp.json()
            if isinstance(body, dict) and body.get("type") == "error":
                raise RuntimeError(body.get("message", "API error"))
            return body

    async def _hs_info_post(self, method: str, params: dict):
        async with self._hs_session.post(
            self._hs_api_url + "info",
            json={"method": method, "params": params},
        ) as resp:
            return await resp.json()

    # ─────────────────────────────────────────────
    #  Hotstuff IOC (taker) — fire-and-forget, 后台验证
    # ─────────────────────────────────────────────

    async def _place_hotstuff_ioc(self, side: str, qty: Decimal,
                                   ref_price: Decimal,
                                   fire_and_forget: bool = True) -> Optional[Decimal]:
        """Hotstuff限价IOC.
        fire_and_forget=True: 签名后立即返回(~7ms), HTTP在后台发送 (开仓用)
        fire_and_forget=False: 等待HTTP响应(~650ms), 确认后返回 (平仓用)"""
        slip = min(Decimal(str(self._max_slippage_bps)),
                   Decimal("50")) / Decimal("10000")
        if side == "buy":
            aggressive = ref_price * (Decimal("1") + slip)
        else:
            aggressive = ref_price * (Decimal("1") - slip)
        aggressive = aggressive.quantize(self.hs_tick_size, rounding=ROUND_HALF_UP)
        qty_r = qty.quantize(self.hs_lot_size, rounding=ROUND_HALF_UP)

        if qty_r < self.hs_lot_size:
            self.logger.warning(f"[Hotstuff] 数量{qty_r}低于最小值{self.hs_lot_size}, 跳过")
            return None

        hs_side = "b" if side == "buy" else "s"
        order = UnitOrder(
            instrumentId=self.hs_instrument_id,
            side=hs_side, positionSide="BOTH",
            price=str(aggressive), size=str(qty_r),
            tif="IOC", ro=False, po=False, isMarket=True,
        )
        from dataclasses import asdict
        ordered_params = {
            "orders": [asdict(order)],
            "expiresAfter": int(time.time() * 1000) + 60_000,
        }

        _t0 = time.time()
        try:
            payload = self._hs_sdk_prepare_order(ordered_params)
        except Exception as e:
            self.logger.error(f"[Hotstuff] 签名异常: {e}")
            return None
        _sign_ms = (time.time() - _t0) * 1000

        if fire_and_forget:
            _pre_pos = self.hotstuff_position
            if side == "buy":
                self.hotstuff_position += qty_r
            else:
                self.hotstuff_position -= qty_r

            self._pending_hs_verify = True
            asyncio.create_task(
                self._hs_fire_and_verify(
                    payload, side, qty_r, ref_price, aggressive, _pre_pos))

            self.logger.info(
                f"[Hotstuff] IOC已签名发射({_sign_ms:.0f}ms): {side} {qty_r} "
                f"@ lim={aggressive} (BBO={ref_price})")
            return ref_price

        # ── 等待模式 (平仓用): 等HTTP响应 + 查仓位确认 ──
        _pre_pos = self.hotstuff_position
        try:
            body = await self._hs_exchange_send(payload)
            _ms = (time.time() - _t0) * 1000

            real_pos = await self._async_get_hs_position()
            if side == "buy":
                delta = real_pos - _pre_pos
            else:
                delta = _pre_pos - real_pos

            _total_ms = (time.time() - _t0) * 1000
            if delta > 0:
                self.hotstuff_position = real_pos
                self.logger.info(
                    f"[Hotstuff] IOC成交({_total_ms:.0f}ms): {side} {abs(delta)} "
                    f"@ lim={aggressive} (BBO={ref_price})")
                return ref_price
            else:
                self.logger.warning(
                    f"[Hotstuff] IOC未成交({_total_ms:.0f}ms): "
                    f"pre={_pre_pos} now={real_pos}")
                return None
        except Exception as e:
            _ms = (time.time() - _t0) * 1000
            self.logger.error(f"[Hotstuff] IOC异常({_ms:.0f}ms): {e}")
            return None

    async def _hs_fire_and_verify(self, payload: dict, side: str, qty: Decimal,
                                   ref_price: Decimal, aggressive: Decimal,
                                   pre_pos: Decimal = None):
        """后台: 发送HTTP + 等响应 + 查仓位确认成交。~665ms 完成。
        增强: 超时保护 + Lighter联动检查 + 安全锁。
        try-finally保证_pending_hs_verify一定会被重置。"""
        if pre_pos is None:
            pre_pos = self.hotstuff_position - (qty if side == "buy" else -qty)

        _t0 = time.time()
        try:
            order_ok = False
            try:
                body = await asyncio.wait_for(
                    self._hs_exchange_send(payload), timeout=3.0)
                _ms = (time.time() - _t0) * 1000
                order_ok = True
                self.logger.info(f"[Hotstuff] IOC后台响应({_ms:.0f}ms)")
            except asyncio.TimeoutError:
                _ms = (time.time() - _t0) * 1000
                self.logger.error(
                    f"[Hotstuff] IOC后台超时({_ms:.0f}ms >3s)! → REPAIR")
            except Exception as e:
                _ms = (time.time() - _t0) * 1000
                self.logger.error(f"[Hotstuff] IOC后台失败({_ms:.0f}ms): {e}")

            if not order_ok:
                if side == "buy":
                    self.hotstuff_position -= qty
                else:
                    self.hotstuff_position += qty
                self.state = State.REPAIR
                return

            real_pos = await self._async_get_hs_position()
            if side == "buy":
                delta = real_pos - pre_pos
            else:
                delta = pre_pos - real_pos

            _total_ms = (time.time() - _t0) * 1000
            if delta > 0:
                self.hotstuff_position = real_pos
                self.logger.info(
                    f"[Hotstuff] IOC成交确认({_total_ms:.0f}ms): {side} {abs(delta)} "
                    f"(仓位={real_pos})")
                await self._correct_open_pnl(ref_price)
            else:
                self.logger.warning(
                    f"[Hotstuff] IOC未成交({_total_ms:.0f}ms): 仓位未变 "
                    f"pre={pre_pos} now={real_pos} → 回滚+REPAIR")
                self.hotstuff_position = real_pos
                self._pending_open_context = None
                self.state = State.REPAIR
        finally:
            self._pending_hs_verify = False

    # ─────────────────────────────────────────────
    #  P&L correction after fire-and-verify confirms
    # ─────────────────────────────────────────────

    async def _correct_open_pnl(self, h_ref: Decimal):
        """后台确认H成交后，查真实成交价并修正主循环的估算P&L。"""
        ctx = self._pending_open_context
        if ctx is None:
            return
        self._pending_open_context = None

        await self._fetch_hotstuff_fill_price(h_ref)
        h_real = self._last_hotstuff_avg_price
        if h_real is None or h_real == h_ref:
            return

        l_actual = ctx["l_actual"]
        qty = ctx["qty"]
        _fee = float(FEE_HOTSTUFF_TAKER + FEE_LIGHTER_MAKER) * 10000

        if ctx["l_side"] == "sell":
            real_spread = float((l_actual - h_real) / h_real * 10000)
        else:
            real_spread = float((h_real - l_actual) / l_actual * 10000)
        real_net = real_spread - _fee
        real_dollar = real_net / 10000 * float(qty) * float(h_real)

        corr_dollar = real_dollar - ctx["est_dollar"]
        corr_bps = real_net - ctx["est_net_bps"]
        self.cumulative_dollar_pnl += corr_dollar
        self.cumulative_net_bps += corr_bps

        self.logger.info(
            f"[成交价修正] H真实={h_real:.2f}(BBO估={h_ref:.2f})  "
            f"spread={real_spread:.2f}bps net={real_net:+.2f}bps  "
            f"${real_dollar:+.4f}(修正{corr_dollar:+.4f})  "
            f"累积=${self.cumulative_dollar_pnl:+.4f}")

        _seq = ctx.get("seq", 0)
        self._csv_row([
            datetime.now(_TZ_CN).isoformat(),
            f"hsv27_correct_{_seq:04d}", ctx["l_side"],
            str(l_actual), str(h_real), str(qty),
            f"{real_spread:.2f}", f"{_fee:.2f}", f"{real_net:.2f}",
            "", f"{self.cumulative_net_bps:.2f}",
            "", "", "", f"corr={corr_dollar:+.4f}",
        ])

    # ─────────────────────────────────────────────
    #  Hotstuff fill price query (aiohttp)
    # ─────────────────────────────────────────────

    async def _fetch_hotstuff_fill_price(self, fallback: Decimal,
                                          max_attempts: int = 3,
                                          delay: float = 0.3):
        """查询Hotstuff最新成交价。
        只信任 fills API 返回的精确成交价，不使用 positions entry_price
        (那是持仓加权均价，多仓时会与单笔成交价严重偏离)。
        如果 fills API 不可用，直接使用 BBO ref_price。"""
        self._last_hotstuff_avg_price = None

        if self._fills_api_broken:
            self._last_hotstuff_avg_price = fallback
            return

        for attempt in range(max_attempts):
            if attempt > 0:
                await asyncio.sleep(delay)
            try:
                resp = await self._hs_info_post(
                    "fills", {"user": self.hs_address, "limit": 5})
                if not resp:
                    continue
                fills = (resp if isinstance(resp, list)
                         else (resp.get("data") or resp.get("fills") or []))
                if not fills:
                    continue

                f = fills[0]
                fill_id = f.get("id") or f.get("fill_id") or f.get("tx_hash")

                if fill_id and fill_id == self._last_hs_fill_id:
                    if attempt < max_attempts - 1:
                        continue

                if attempt == 0 and isinstance(f, dict):
                    self.logger.info(
                        f"[Hotstuff] fills API 返回字段: {list(f.keys())}")

                price = None
                for pf in ("price", "fillPrice", "fill_price",
                           "avgPrice", "avg_price", "px",
                           "executionPrice", "execution_price"):
                    raw = f.get(pf) if isinstance(f, dict) else getattr(f, pf, None)
                    if raw is not None:
                        try:
                            price = Decimal(str(raw))
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
                if "not found" in str(e).lower() or "404" in str(e):
                    self._fills_api_broken = True
                    self.logger.warning(f"[Hotstuff] fills API 不可用: {e}")
                    break
                self.logger.warning(f"[Hotstuff] fills查询异常(第{attempt+1}次): {e}")

        self._last_hotstuff_avg_price = fallback
        self.logger.info(
            f"[Hotstuff] 使用BBO参考价={fallback:.2f} "
            f"(fills API 未返回有效成交价)")

    # ─────────────────────────────────────────────
    #  V27: Dual taker open (parallel IOC)
    # ─────────────────────────────────────────────

    async def _lighter_ioc_open(self, side: str, qty: Decimal, price: Decimal):
        """Lighter IOC for opening with WS fill confirmation.
        Returns (success: bool, error_msg: Optional[str])."""
        is_ask = side == "sell"
        coi = int(time.time() * 1000) * 10 + (1 if is_ask else 2)

        self._open_fill_event.clear()
        self._open_fill_data = None
        self._last_lighter_avg_price = None
        self._open_pending_coi = coi

        await self._lighter_rate_wait()
        try:
            _, _, err = await self.lighter_client.create_order(
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
            if err:
                self._open_pending_coi = None
                return False, str(err)

            try:
                await asyncio.wait_for(self._open_fill_event.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass

            fd = self._open_fill_data
            if fd is not None:
                filled_base = Decimal(fd.get("filled_base_amount", "0"))
                if filled_base >= qty * Decimal("0.5"):
                    self.logger.info(
                        f"[Lighter] 开仓IOC WS确认: filled={filled_base} "
                        f"avg={self._last_lighter_avg_price}")
                    self._open_pending_coi = None
                    return True, None

            self._open_pending_coi = None
            return True, None
        except Exception as e:
            self._open_pending_coi = None
            return False, str(e)

    async def _dual_taker_open(self, l_side: str, l_ref: Decimal,
                                h_side: str, h_ref: Decimal):
        """同时发出Lighter IOC + Hotstuff IOC, 并行执行。
        BBO二次验证 + L1深度限制 + 安全单腿处理。
        Returns: dict(成功) / "SKIP"(放弃) / "REPAIR"(单腿失败)"""

        # ── BBO 二次验证: 重新读取最新价格, 防止价差已消失 ──
        h2_bid, h2_ask = self._get_hotstuff_ws_bbo()
        l2_bid = self._l_best_bid
        l2_ask = self._l_best_ask
        if h2_bid is None or h2_ask is None or l2_bid is None or l2_ask is None:
            self.logger.info("[开仓] BBO二次验证: 行情不可用, 放弃")
            return "SKIP"

        if l_side == "sell":
            recheck_spread = float((l2_bid - h2_ask) / h2_ask * 10000) if h2_ask > 0 else 0
            l_ref = l2_bid
            h_ref = h2_ask
        else:
            recheck_spread = float((h2_bid - l2_ask) / l2_ask * 10000) if l2_ask > 0 else 0
            l_ref = l2_ask
            h_ref = h2_bid

        effective_bps = (self.open_spread_bps
                         + self._ladder_level * self.ladder_step_bps
                         + self.spread_buffer_bps)
        if recheck_spread < effective_bps:
            self.logger.info(
                f"[开仓] BBO二次验证: 价差{recheck_spread:.1f}bps < {effective_bps:.1f}bps, 放弃")
            return "SKIP"

        # ── L1 深度检查: 限制下单量不超过第一档 ──
        l1_l, l1_h = self._get_l1_depth(l_side, h_side)
        open_qty = min(self.order_size, l1_l, l1_h)
        open_qty = open_qty.quantize(self.hs_lot_size, rounding=ROUND_DOWN)
        if open_qty < self.order_size * Decimal("0.05"):
            self.logger.info(
                f"[开仓] L1深度不足: L1_L={l1_l} L1_H={l1_h} "
                f"需要={self.order_size} 可吃={open_qty}, 跳过")
            return "SKIP"

        # ── 计算Lighter限价 ──
        slip = min(Decimal(str(self._max_slippage_bps)),
                   Decimal("20")) / Decimal("10000")
        if l_side == "sell":
            l_price = self._round_lighter(l_ref - l_ref * slip)
        else:
            l_price = self._round_lighter(l_ref + l_ref * slip)

        _t0 = time.time()

        _l_task = asyncio.create_task(
            self._lighter_ioc_open(l_side, open_qty, l_price))
        _h_task = asyncio.create_task(
            self._place_hotstuff_ioc(h_side, open_qty, h_ref))

        _l_result, _h_fill = await asyncio.gather(_l_task, _h_task, return_exceptions=True)

        _ms = (time.time() - _t0) * 1000

        if isinstance(_l_result, Exception):
            _l_ok, _l_err = False, str(_l_result)
        else:
            _l_ok, _l_err = _l_result

        if isinstance(_h_fill, Exception):
            _h_fill = None

        # ── 双腿都成功 ──
        if _l_ok and _h_fill is not None:
            if l_side == "sell":
                self.lighter_position -= open_qty
            else:
                self.lighter_position += open_qty

            return {"l_side": l_side, "l_price": l_ref, "h_side": h_side,
                    "h_price": _h_fill, "ms": _ms, "qty": open_qty}

        # ── Lighter成功 + Hotstuff失败 → 进REPAIR, 对账后用Lighter修复(0费用) ──
        if _l_ok and _h_fill is None:
            if l_side == "sell":
                self.lighter_position -= open_qty
            else:
                self.lighter_position += open_qty
            self.logger.error(
                f"[开仓] L成功H失败! L_{l_side}@{l_ref} qty={open_qty} → REPAIR(Lighter修复)")
            return "REPAIR"

        # ── Hotstuff成功 + Lighter失败 → 立即用Lighter补仓(0费用) ──
        if not _l_ok and _h_fill is not None:
            self.logger.warning(
                f"[开仓] H成功L失败({_l_err})! H_{h_side}@{_h_fill} → Lighter补仓对冲")
            _retry_ref = self._l_best_bid if l_side == "sell" else self._l_best_ask
            if _retry_ref and _retry_ref > 0:
                _retry_ok = await self._lighter_ioc(l_side, open_qty, _retry_ref)
                if _retry_ok:
                    self.logger.info(
                        f"[开仓] Lighter补仓成功! 视为正常开仓")
                    return {"l_side": l_side, "l_price": _retry_ref, "h_side": h_side,
                            "h_price": _h_fill, "ms": (time.time() - _t0) * 1000,
                            "qty": open_qty}
            self.logger.error("[开仓] Lighter补仓也失败 → REPAIR")
            return "REPAIR"

        # ── 双腿都失败 → 安全, 跳过 ──
        self.logger.warning(f"[开仓] 双腿都失败: L={_l_err}")
        return "SKIP"

    # ─────────────────────────────────────────────
    #  Close position (spread convergence)
    # ─────────────────────────────────────────────

    async def _close_position(self, l_bid: Decimal, l_ask: Decimal,
                               h_bid: Decimal, h_ask: Decimal) -> bool:
        """逐层平仓: 每次只平1层(order_size), 减少滑点。"""
        self._in_flatten = True
        await self._cancel_all_lighter()

        l_qty = abs(self.lighter_position)
        h_qty = abs(self.hotstuff_position)
        close_qty = min(l_qty, h_qty, self.order_size)

        if close_qty < self.hs_lot_size:
            self._in_flatten = False
            return False

        close_qty = close_qty.quantize(self.hs_lot_size, rounding=ROUND_DOWN)
        if close_qty <= 0:
            self._in_flatten = False
            return False

        if self.lighter_position < 0:
            l_side = "buy"
            h_side = "sell"
            l_ref = l_ask
            h_ref = h_bid
        else:
            l_side = "sell"
            h_side = "buy"
            l_ref = l_bid
            h_ref = h_ask

        l1_l, l1_h = self._get_l1_depth(l_side, h_side)
        l1_qty = min(close_qty, l1_l, l1_h)
        l1_qty = l1_qty.quantize(self.hs_lot_size, rounding=ROUND_DOWN)
        if l1_qty < self.order_size * Decimal("0.05"):
            self.logger.info(
                f"[平仓] L1深度不足: L1_L={l1_l} L1_H={l1_h} 跳过")
            self._in_flatten = False
            return False
        close_qty = l1_qty

        self.logger.info(
            f"[逐层平仓] L_{l_side} {close_qty} ref={l_ref} + "
            f"H_{h_side} {close_qty} ref={h_ref}  "
            f"(剩余L_pos={self.lighter_position} H_pos={self.hotstuff_position})")

        _t0 = time.time()

        # ── Lighter + Hotstuff 并行发射 (fire-and-forget) ──
        _close_slip = min(Decimal(str(self._max_slippage_bps + 3)),
                          Decimal("20")) / Decimal("10000")
        if l_side == "buy":
            l_price = self._round_lighter(l_ref * (Decimal("1") + _close_slip))
        else:
            l_price = self._round_lighter(l_ref * (Decimal("1") - _close_slip))

        self._flatten_fill_event.clear()
        self._flatten_fill_price = None
        self._flatten_fill_qty = Decimal("0")
        _flatten_cid = int(time.time() * 1000) * 10 + 9
        self._flatten_ioc_cid = _flatten_cid

        async def _lighter_close():
            await self._lighter_rate_wait()
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
                raise RuntimeError(str(err))
            return True

        _l_task = asyncio.create_task(_lighter_close())
        _h_task = asyncio.create_task(
            self._place_hotstuff_ioc(h_side, close_qty, h_ref,
                                     fire_and_forget=False))

        _l_result, _h_fill = await asyncio.gather(
            _l_task, _h_task, return_exceptions=True)

        _l_failed = isinstance(_l_result, Exception)
        _h_failed = isinstance(_h_fill, Exception) or _h_fill is None

        if _l_failed and _h_failed:
            self.logger.error("[平仓] 双腿都失败, 安全跳过")
            self._in_flatten = False
            return False

        if _l_failed and not _h_failed:
            self.logger.error(
                f"[平仓] L失败H成功! → REPAIR  (L_err={_l_result})")
            self._in_flatten = False
            self.state = State.REPAIR
            return False

        if l_side == "buy":
            self.lighter_position += close_qty
        else:
            self.lighter_position -= close_qty

        if _h_failed:
            self.logger.error(
                f"[平仓] L成功H失败! (H_err={_h_fill}) → REPAIR")
            self._in_flatten = False
            self.state = State.REPAIR
            return False

        # 等Lighter WS确认拿实际成交价 (最多100ms, 通常已到)
        if not self._flatten_fill_event.is_set():
            try:
                await asyncio.wait_for(self._flatten_fill_event.wait(), timeout=0.1)
            except asyncio.TimeoutError:
                pass

        _l_actual = l_ref
        if self._flatten_fill_price and self._flatten_fill_price > 0:
            _l_actual = self._flatten_fill_price

        _l_real_qty = self._flatten_fill_qty if self._flatten_fill_qty > 0 else close_qty
        if _l_real_qty != close_qty:
            _diff = close_qty - _l_real_qty
            if l_side == "buy":
                self.lighter_position -= _diff
            else:
                self.lighter_position += _diff
            self.logger.warning(
                f"[平仓] Lighter部分成交: 预期={close_qty} 实际={_l_real_qty} → 仓位已修正")

        _actual_qty = _l_real_qty

        await self._fetch_hotstuff_fill_price(h_ref)
        _h_actual = self._last_hotstuff_avg_price or h_ref

        _ms = (time.time() - _t0) * 1000
        _h_price = Decimal(str(_h_actual))
        if l_side == "buy":
            _spread_bps = float((_l_actual - _h_price) / _h_price * 10000)
        else:
            _spread_bps = float((_h_price - _l_actual) / _l_actual * 10000)
        _fee = float(FEE_HOTSTUFF_TAKER + FEE_LIGHTER_MAKER) * 10000
        _net = -_spread_bps - _fee
        self.cumulative_net_bps += _net
        _dollar = float(_net / 10000 * float(_actual_qty) * float(_h_price))
        self.cumulative_dollar_pnl += _dollar

        self.logger.info(
            f"[平仓完成] L_{l_side}@{_l_actual} H_{h_side}@{_h_actual}  "
            f"close_spread={_spread_bps:.2f}bps fee={_fee:.2f}bps "
            f"close_net={_net:.2f}bps  ${_dollar:+.4f}  {_ms:.0f}ms  "
            f"累积=${self.cumulative_dollar_pnl:+.4f}")

        _real_pnl_str, _cum_real_str, _bal_l_str, _bal_h_str = \
            await self._verify_close_pnl(
                _net, _dollar, _actual_qty, Decimal(str(_h_actual)))

        self._trade_seq += 1
        self._csv_row([
            datetime.now(_TZ_CN).isoformat(),
            f"hsv27_close_{self._trade_seq:04d}",
            f"close_{l_side}",
            str(_l_actual), str(_h_actual), str(_actual_qty),
            f"{_spread_bps:.2f}", f"{_fee:.2f}", f"{_net:.2f}",
            f"{_ms:.0f}", f"{self.cumulative_net_bps:.2f}",
            _real_pnl_str, _cum_real_str, _bal_l_str, _bal_h_str,
        ])
        if self.ladder_step_bps > 0 and self._ladder_level > 0:
            _old_level = self._ladder_level
            self._ladder_level = max(0, self._ladder_level - 1)
            self.logger.info(
                f"[阶梯] 平仓1层, L{_old_level} → L{self._ladder_level}")

        _still_has_pos = (abs(self.lighter_position) >= self.hs_lot_size
                          and abs(self.hotstuff_position) >= self.hs_lot_size)
        if not _still_has_pos and self._ladder_level > 0:
            self.logger.info(f"[阶梯] 全部平完, L{self._ladder_level} → L0")
            self._ladder_level = 0

        self._in_flatten = False
        return True

    # ─────────────────────────────────────────────
    #  Balance query helpers
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
        """通过 account_summary API 获取 Hotstuff 权益。"""
        try:
            if self._hs_session and not self._hs_session.closed:
                resp = await self._hs_info_post(
                    "account_summary", {"user": self.hs_address})
                if resp:
                    for key in ("total_account_equity", "margin_balance"):
                        val = resp.get(key)
                        if val is not None:
                            return Decimal(str(val))
                return None
            from hotstuff.methods.info.account import AccountSummaryParams
            loop = asyncio.get_event_loop()
            summary = await loop.run_in_executor(
                None, self.hs_info.account_summary,
                AccountSummaryParams(user=self.hs_address))
            if summary:
                equity = getattr(summary, 'total_account_equity', None)
                if equity is not None:
                    return Decimal(str(equity))
                margin = getattr(summary, 'margin_balance', None)
                if margin is not None:
                    return Decimal(str(margin))
        except Exception as e:
            self.logger.warning(f"[余额] Hotstuff余额查询失败: {e}")
        return None

    async def _snapshot_balances(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        l_bal, h_bal = await asyncio.gather(
            self._get_lighter_balance(),
            self._get_hotstuff_balance(),
        )
        return l_bal, h_bal

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

    async def _async_get_hs_position(self) -> Decimal:
        """aiohttp 直接查询 Hotstuff 仓位 (~11ms keep-alive)。"""
        try:
            resp = await self._hs_info_post(
                "positions", {"user": self.hs_address})
            if not resp:
                return Decimal("0")
            positions = resp if isinstance(resp, list) else []
            for p in positions:
                if p.get("instrument") == self.hs_symbol:
                    return Decimal(str(p.get("size", "0")))
            return Decimal("0")
        except Exception as e:
            self.logger.error(f"查询Hotstuff仓位出错: {e}")
            return self.hotstuff_position

    async def _get_hotstuff_position(self) -> Decimal:
        if self._hs_session and not self._hs_session.closed:
            return await self._async_get_hs_position()
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
                inst = (p.get("instrument") if isinstance(p, dict)
                        else getattr(p, "instrument", None))
                if inst == self.hs_symbol:
                    size_str = (p.get("size") if isinstance(p, dict)
                                else getattr(p, "size", "0"))
                    return Decimal(str(size_str))
            return Decimal("0")
        except Exception as e:
            self.logger.error(f"查询Hotstuff仓位出错: {e}")
            return self.hotstuff_position

    async def _reconcile(self):
        l = await self._get_lighter_position()
        h = await self._get_hotstuff_position()
        _old_l, _old_h = self.lighter_position, self.hotstuff_position
        self.lighter_position = l
        self.hotstuff_position = h
        net = abs(l + h)
        if _old_l != l or _old_h != h:
            self.logger.info(
                f"[对账] 仓位同步: L {_old_l}→{l}  H {_old_h}→{h}")
        if net > self.order_size * Decimal("0.5"):
            self.logger.warning(f"[对账] 不平衡: L={l} H={h} net={net}")
        return net < self.order_size * Decimal("2")

    async def _emergency_flatten(self):
        self.logger.error("[紧急平仓] 开始...")
        self._in_flatten = True

        await self._cancel_all_lighter()
        await self._reconcile()

        # 平 Lighter 仓位
        if abs(self.lighter_position) >= self.hs_lot_size:
            side = "sell" if self.lighter_position > 0 else "buy"
            qty = abs(self.lighter_position)
            ref = self._l_best_bid if side == "sell" else self._l_best_ask
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
                self.logger.warning("[紧急平仓] Lighter无BBO, 跳过")

        # 平 Hotstuff 仓位
        if abs(self.hotstuff_position) >= self.hs_lot_size:
            side = "sell" if self.hotstuff_position > 0 else "buy"
            qty = abs(self.hotstuff_position)
            h_bid, h_ask = self._get_hotstuff_ws_bbo()
            ref = h_bid if side == "sell" else h_ask
            if ref and ref > 0:
                result = await self._place_hotstuff_ioc(
                    side, qty, ref, fire_and_forget=False)
                if result is not None:
                    self.logger.info(f"[紧急平仓] Hotstuff {side} {qty} 已发送")
                else:
                    self.logger.error(f"[紧急平仓] Hotstuff {side} {qty} 失败")
            else:
                self.logger.warning("[紧急平仓] Hotstuff无BBO, 跳过")

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
        self.logger.info(f"[紧急平仓] 完成: L={self.lighter_position} H={self.hotstuff_position}")

    # ─────────────────────────────────────────────
    #  Balance P&L verification
    # ─────────────────────────────────────────────

    async def _record_pre_trade_balance(self):
        try:
            _l, _h = await self._snapshot_balances()
            if _l is not None:
                self._pre_trade_balance_l = _l
            if _h is not None:
                self._pre_trade_balance_h = _h
            if _l is not None and _h is not None:
                self.logger.info(
                    f"[余额] 开仓后快照: L={_l} H={_h} 合计={_l+_h}")
            elif _l is not None:
                self.logger.info(f"[余额] 开仓后快照: L={_l} (Hotstuff余额不可用)")
        except Exception as e:
            self.logger.warning(f"[余额] 开仓快照失败: {e}")

    async def _verify_close_pnl(self, formula_net_bps: float, formula_dollar: float,
                                 close_qty: Decimal, h_price: Decimal) -> Tuple[str, str, str, str]:
        try:
            _l_after, _h_after = await self._snapshot_balances()
            if _l_after is None:
                return "", "", "", ""

            _bal_l_str = str(_l_after)
            _bal_h_str = str(_h_after) if _h_after is not None else ""

            _pre_l = self._pre_trade_balance_l
            _pre_h = self._pre_trade_balance_h

            if _pre_l is None or _h_after is None or _pre_h is None:
                self.logger.info(
                    f"[余额] 平仓后: L={_l_after} H={_h_after} (基准不完整, 跳过对比)")
                self._pre_trade_balance_l = _l_after
                self._pre_trade_balance_h = _h_after
                return "", f"{self.cumulative_real_pnl:.6f}", _bal_l_str, _bal_h_str

            _real_pnl = float((_l_after + _h_after) - (_pre_l + _pre_h))
            self.cumulative_real_pnl += _real_pnl

            _real_bps = _real_pnl / (float(close_qty) * float(h_price)) * 10000 if close_qty > 0 and h_price > 0 else 0
            _diff = _real_pnl - formula_dollar
            _diff_bps = abs(_real_bps - formula_net_bps)

            self.logger.info(
                f"[余额校验] "
                f"公式P&L=${formula_dollar:+.6f}({formula_net_bps:+.2f}bps)  "
                f"真实P&L=${_real_pnl:+.6f}({_real_bps:+.2f}bps)  "
                f"偏差=${_diff:+.6f}({_diff_bps:.2f}bps)  "
                f"累积真实=${self.cumulative_real_pnl:+.6f}")

            if _diff_bps > 2.0:
                self.logger.warning(
                    f"[!!] P&L偏差过大! 差={_diff_bps:.2f}bps (>2bps阈值)")

            if self._initial_balance_l is not None and self._initial_balance_h is not None:
                _total_now = _l_after + _h_after
                _total_init = self._initial_balance_l + self._initial_balance_h
                _session_real = float(_total_now - _total_init)
                _session_formula = self.cumulative_dollar_pnl
                _session_diff = _session_real - _session_formula
                self.logger.info(
                    f"[会话] "
                    f"公式累积=${_session_formula:+.6f}  "
                    f"真实累积=${_session_real:+.6f}  "
                    f"偏差=${_session_diff:+.6f}")

            self._pre_trade_balance_l = _l_after
            self._pre_trade_balance_h = _h_after
            return f"{_real_pnl:+.6f}", f"{self.cumulative_real_pnl:.6f}", _bal_l_str, _bal_h_str

        except Exception as e:
            self.logger.warning(f"[余额校验] 异常: {e}")
            return "", "", "", ""

    # ─────────────────────────────────────────────
    #  Main entry
    # ─────────────────────────────────────────────

    async def run(self):
        self._lighter_ws_task: Optional[asyncio.Task] = None
        self._hs_feed_task: Optional[asyncio.Task] = None
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

            for t in [self._lighter_ws_task, self._hs_feed_task]:
                if t and not t.done():
                    t.cancel()
            if self._hs_session and not self._hs_session.closed:
                await self._hs_session.close()
            self.logger.info("[退出] 完成")

    async def _safe_shutdown(self):
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

        if (abs(self.lighter_position) >= self.hs_lot_size
                or abs(self.hotstuff_position) >= self.hs_lot_size):
            self.logger.info(
                f"[退出] 残余仓位: L={self.lighter_position} "
                f"H={self.hotstuff_position}, 紧急平仓")
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
            f"启动 Hotstuff+Lighter V27 双腿Taker套利  {self.symbol}  "
            f"size={self.order_size}  "
            f"开仓≥{self.open_spread_bps}bps(+{self.spread_buffer_bps}bps缓冲)→双腿IOC  "
            f"平仓≤{self.close_spread_bps}bps(快速3层+深度观察)  {_ladder_str}  "
            f"fee={fee_one:.2f}bps/笔  "
            f"滑点保护≤{self._max_slippage_bps}bps(isMarket=True,L1深度检查)  "
            f"max_pos={self.max_position}  dry_run={self.dry_run}")

        if not self.dry_run:
            self._init_lighter()
            self._get_lighter_market_config()
            self._init_hotstuff_client()
            self._get_hotstuff_instrument_info()
            await self._hs_ensure_session()
            await self._cancel_all_lighter()
        else:
            self._get_lighter_market_config()

        self._lighter_ws_task = asyncio.create_task(self._lighter_ws_loop())

        self.hs_feed = HotstuffFeed(self.symbol)
        self._hs_feed_task = asyncio.create_task(self.hs_feed.connect())

        self.logger.info("等待行情就绪...")
        t0 = time.time()
        while time.time() - t0 < 20 and not self.stop_flag:
            h_bid, h_ask = self._get_hotstuff_ws_bbo()
            h_ok = h_bid is not None and h_ask is not None
            l_ok = self._l_best_bid is not None and self._l_best_ask is not None
            if h_ok and l_ok:
                break
            if time.time() - t0 > 5 and int(time.time() - t0) % 3 == 0:
                self.logger.info(
                    f"  等待中... H={'OK' if h_ok else 'WAIT'} "
                    f"L={'OK' if l_ok else 'WAIT'}")
            await asyncio.sleep(0.5)

        h_bid, h_ask = self._get_hotstuff_ws_bbo()
        if h_bid is None or self._l_best_bid is None:
            self.logger.error(
                f"行情超时! H={h_bid}/{h_ask} "
                f"L={self._l_best_bid}/{self._l_best_ask}")
            return

        self.logger.info(
            f"行情就绪: L={self._l_best_bid}/{self._l_best_ask}  "
            f"H={h_bid}/{h_ask}")

        if not self.dry_run:
            await self._reconcile()
            _has_residual = (abs(self.lighter_position) >= self.hs_lot_size
                             or abs(self.hotstuff_position) >= self.hs_lot_size)
            if _has_residual:
                _net = abs(self.lighter_position + self.hotstuff_position)
                if getattr(self, 'flatten_on_start', False):
                    self.logger.warning(
                        f"[启动] 残余仓位: L={self.lighter_position} H={self.hotstuff_position}  "
                        f"--flatten-on-start → 紧急平仓")
                    await self._emergency_flatten()
                elif _net > self.order_size * Decimal("0.5"):
                    self.logger.warning(
                        f"[启动] 仓位不平衡: L={self.lighter_position} H={self.hotstuff_position} "
                        f"净敞口={_net} → 紧急平仓修复")
                    await self._emergency_flatten()
                else:
                    self.logger.info(
                        f"[启动] 平衡仓位: L={self.lighter_position} H={self.hotstuff_position} "
                        f"净={_net}, 继续交易(阶梯从L0计算)")

            _l_bal, _h_bal = await self._snapshot_balances()
            self._initial_balance_l = _l_bal
            self._initial_balance_h = _h_bal
            self._pre_trade_balance_l = _l_bal
            self._pre_trade_balance_h = _h_bal
            if _l_bal is not None:
                _h_str = str(_h_bal) if _h_bal is not None else "N/A"
                self.logger.info(
                    f"[初始余额] Lighter={_l_bal}  Hotstuff={_h_str}")
            else:
                self.logger.warning(
                    f"[初始余额] 获取失败: L={_l_bal} H={_h_bal}")

        self.logger.info(f"进入主循环  状态={self.state.value}")
        last_status_log = 0.0
        last_reconcile = time.time()

        while not self.stop_flag:
            try:
                now = time.time()
                h_bid, h_ask = self._get_hotstuff_ws_bbo()
                l_bid = self._l_best_bid
                l_ask = self._l_best_ask

                if h_bid is None or h_ask is None or l_bid is None or l_ask is None:
                    await asyncio.sleep(self.interval)
                    continue

                h_age = now - self.hs_feed._last_update_ts if self.hs_feed else 999
                l_age = now - self._l_ws_ts if self._l_ws_ts > 0 else 999
                if l_age > 5 or h_age > 5:
                    await asyncio.sleep(self.interval)
                    continue

                mid = (l_bid + l_ask + h_bid + h_ask) / 4
                basis_bps = float((l_bid + l_ask - h_bid - h_ask) / 2 / mid * 10000)

                # ── Status log ──
                if now - last_status_log > 15:
                    self.logger.info(
                        f"[{self.state.value}] basis={basis_bps:+.1f}bps  "
                        f"L={l_bid}/{l_ask} H={h_bid}/{h_ask}  "
                        f"L_pos={self.lighter_position} H_pos={self.hotstuff_position}  "
                        f"鲜度:L={l_age:.1f}s H={h_age:.1f}s  "
                        f"累积=${self.cumulative_dollar_pnl:+.4f}")
                    last_status_log = now

                # ── Periodic reconcile ──
                if not self.dry_run and now - last_reconcile > 60 and self.state == State.IDLE:
                    await self._reconcile()
                    last_reconcile = now
                    _net = self.lighter_position + self.hotstuff_position
                    _imbalance = abs(_net)

                    if _imbalance > self.order_size * Decimal("1.5"):
                        self.logger.error(
                            f"[对账] 大偏差{_imbalance}, 紧急平仓!")
                        await self._emergency_flatten()
                        continue
                    elif _imbalance >= self.hs_lot_size:
                        self.logger.warning(
                            f"[对账] 偏差={_imbalance}, "
                            f"L={self.lighter_position} H={self.hotstuff_position} "
                            f"net={_net} → Lighter IOC修复(0费用)")
                        self._in_flatten = True
                        if _net < 0:
                            _ref = self._l_best_ask if self._l_best_ask else l_ask
                            await self._lighter_ioc("buy", _imbalance, _ref)
                        else:
                            _ref = self._l_best_bid if self._l_best_bid else l_bid
                            await self._lighter_ioc("sell", _imbalance, _ref)
                        await asyncio.sleep(0.5)
                        self._in_flatten = False

                # ── Handle stale fills ──
                if self._stale_fills and self.state == State.IDLE:
                    sf = self._stale_fills.pop(0)
                    self.logger.warning(
                        f"[过期成交] {sf['side']} {sf['size']} @ {sf['price']} "
                        f"→ 立即触发对账修复")
                    await self._reconcile()
                    last_reconcile = now
                    _net = self.lighter_position + self.hotstuff_position
                    _imbalance = abs(_net)
                    if _imbalance >= self.hs_lot_size:
                        self.logger.warning(
                            f"[过期成交修复] 偏差={_imbalance} → Lighter IOC修复")
                        self._in_flatten = True
                        if _net < 0:
                            _ref = self._l_best_ask if self._l_best_ask else l_ask
                            await self._lighter_ioc("buy", _imbalance, _ref)
                        else:
                            _ref = self._l_best_bid if self._l_best_bid else l_bid
                            await self._lighter_ioc("sell", _imbalance, _ref)
                        await asyncio.sleep(0.3)
                        self._in_flatten = False
                    continue

                # ━━━━━━ STATE: IDLE ━━━━━━
                if self.state == State.IDLE:
                    if now < self._circuit_breaker_until:
                        await asyncio.sleep(1)
                        continue

                    if self.max_loss > 0 and not self._loss_breaker:
                        _check_loss = self.cumulative_real_pnl if self.cumulative_real_pnl != 0.0 else self.cumulative_dollar_pnl
                        if _check_loss < -self.max_loss:
                            self._loss_breaker = True
                            self.logger.error(
                                f"[风控] 累积亏损 ${_check_loss:.4f} 超过阈值 "
                                f"-${self.max_loss:.2f} → 停止开仓, 平仓现有仓位!")
                            if (abs(self.lighter_position) >= self.hs_lot_size
                                    or abs(self.hotstuff_position) >= self.hs_lot_size):
                                await self._emergency_flatten()

                    if self._loss_breaker:
                        if now - last_status_log > 30:
                            self.logger.info(
                                f"[风控] 已触发亏损熔断, 停止交易. "
                                f"累积=${self.cumulative_dollar_pnl:+.4f}")
                            last_status_log = now
                        await asyncio.sleep(5)
                        continue

                    if self.dry_run:
                        await asyncio.sleep(self.interval)
                        continue

                    _has_pos = (abs(self.lighter_position) >= self.hs_lot_size
                                and abs(self.hotstuff_position) >= self.hs_lot_size)

                    # ── 快速平仓+深度观察 ──
                    if _has_pos:
                        if self.lighter_position < 0:
                            _should_close = basis_bps <= self.close_spread_bps
                        else:
                            _should_close = basis_bps >= -self.close_spread_bps
                        if _should_close:
                            _total_layers = int(min(abs(self.lighter_position),
                                                    abs(self.hotstuff_position)) / self.order_size)
                            if _total_layers <= 3:
                                _fast_layers = _total_layers
                            else:
                                _fast_layers = 3
                            self.logger.info(
                                f"[IDLE] 价差收敛! basis={basis_bps:.1f}bps  "
                                f"{'全部' if _fast_layers == _total_layers else '快速'}"
                                f"平{_fast_layers}/{_total_layers}层  "
                                f"L={self.lighter_position} H={self.hotstuff_position}  "
                                f"ladder=L{self._ladder_level}")

                            await self._cancel_all_lighter()

                            _closed = 0
                            _failed = False
                            for _ in range(_fast_layers):
                                if self.stop_flag:
                                    break
                                ok = await self._close_position(
                                    l_bid, l_ask, h_bid, h_ask)
                                if ok:
                                    _closed += 1
                                    h_bid, h_ask = self._get_hotstuff_ws_bbo()
                                    l_bid = self._l_best_bid
                                    l_ask = self._l_best_ask
                                else:
                                    _failed = True
                                    break

                            if _failed:
                                self.logger.error("[IDLE] 快速平仓失败 → REPAIR")
                                self.state = State.REPAIR
                                continue

                            _still_has = (abs(self.lighter_position) >= self.hs_lot_size
                                          and abs(self.hotstuff_position) >= self.hs_lot_size)
                            if _still_has and self.close_wait_sec > 0:
                                self.logger.info(
                                    f"[IDLE] 已平{_closed}层, 等待{self.close_wait_sec}s观察"
                                    f" (深度阈值={self.close_deep_bps}bps)")
                                await asyncio.sleep(self.close_wait_sec)

                                h_bid, h_ask = self._get_hotstuff_ws_bbo()
                                l_bid = self._l_best_bid
                                l_ask = self._l_best_ask
                                if (h_bid is None or h_ask is None
                                        or l_bid is None or l_ask is None):
                                    continue
                                mid = (l_bid + l_ask + h_bid + h_ask) / 4
                                _new_basis = float((l_bid + l_ask - h_bid - h_ask) / 2 / mid * 10000)

                                if self.lighter_position < 0:
                                    _deep_ok = _new_basis <= self.close_deep_bps
                                else:
                                    _deep_ok = _new_basis >= -self.close_deep_bps
                                if _deep_ok:
                                    self.logger.info(
                                        f"[IDLE] 价差继续收敛! basis={_new_basis:.1f}bps "
                                        f"≤ {self.close_deep_bps}bps → 平剩余仓位")
                                    while (abs(self.lighter_position) >= self.hs_lot_size
                                           and abs(self.hotstuff_position) >= self.hs_lot_size):
                                        if self.stop_flag:
                                            break
                                        h_bid, h_ask = self._get_hotstuff_ws_bbo()
                                        l_bid = self._l_best_bid
                                        l_ask = self._l_best_ask
                                        if (h_bid is None or h_ask is None
                                                or l_bid is None or l_ask is None):
                                            break
                                        _loop_mid = (l_bid + l_ask + h_bid + h_ask) / 4
                                        _loop_basis = float(
                                            (l_bid + l_ask - h_bid - h_ask) / 2 / _loop_mid * 10000)
                                        if self.lighter_position < 0:
                                            _still_ok = _loop_basis <= self.close_deep_bps + 1.0
                                        else:
                                            _still_ok = _loop_basis >= -(self.close_deep_bps + 1.0)
                                        if not _still_ok:
                                            self.logger.info(
                                                f"[IDLE] 深度平仓中止: basis={_loop_basis:.1f}bps "
                                                f"已反弹, 保留剩余仓位")
                                            break
                                        ok = await self._close_position(
                                            l_bid, l_ask, h_bid, h_ask)
                                        if not ok:
                                            self.state = State.REPAIR
                                            break
                                else:
                                    self.logger.info(
                                        f"[IDLE] 价差反弹 basis={_new_basis:.1f}bps "
                                        f"> {self.close_deep_bps}bps → 保留剩余{abs(self.lighter_position)}仓位")
                            continue

                    # ── 持仓上限检查 ──
                    _abs_l = abs(self.lighter_position)
                    _abs_h = abs(self.hotstuff_position)
                    _at_max = (_abs_l >= self.max_position
                               or _abs_h >= self.max_position)
                    if _at_max:
                        if now - last_status_log > 15:
                            self.logger.info(
                                f"[IDLE] 持仓已满 "
                                f"L={self.lighter_position} H={self.hotstuff_position} "
                                f"max={self.max_position}, 等待平仓")
                        await asyncio.sleep(1)
                        continue

                    # ── 安全锁: 等待上次fire-and-forget后台确认完成 ──
                    if self._pending_hs_verify:
                        await asyncio.sleep(0.1)
                        continue

                    # ── 双腿Taker开仓 ──
                    if self.lighter_position < -self.max_position * Decimal("0.8"):
                        _allow_sell = False
                    else:
                        _allow_sell = True
                    if self.lighter_position > self.max_position * Decimal("0.8"):
                        _allow_buy = False
                    else:
                        _allow_buy = True

                    effective_bps = (self.open_spread_bps
                                     + self._ladder_level * self.ladder_step_bps
                                     + self.spread_buffer_bps)

                    _sell_spread = float((l_bid - h_ask) / h_ask * 10000) if h_ask > 0 else 0
                    _buy_spread = float((h_bid - l_ask) / l_ask * 10000) if l_ask > 0 else 0

                    _did_trade = False
                    _open_side = None
                    _open_l_ref = None
                    _open_h_ref = None

                    if _allow_sell and _sell_spread >= effective_bps:
                        _open_side = "sell"
                        _open_l_ref = l_bid
                        _open_h_ref = h_ask
                        _open_spread = _sell_spread
                    elif _allow_buy and _buy_spread >= effective_bps:
                        _open_side = "buy"
                        _open_l_ref = l_ask
                        _open_h_ref = h_bid
                        _open_spread = _buy_spread

                    if _open_side:
                        _h_side = "buy" if _open_side == "sell" else "sell"
                        self.logger.info(
                            f"[开仓] {_open_side}价差={_open_spread:.1f}bps ≥ {effective_bps:.1f}bps  "
                            f"L={_open_l_ref} H={_open_h_ref} → 并行双腿IOC!")

                        result = await self._dual_taker_open(
                            _open_side, _open_l_ref, _h_side, _open_h_ref)

                        if result == "REPAIR":
                            self._consecutive_hedge_fails += 1
                            if self._consecutive_hedge_fails >= 3:
                                self._circuit_breaker_until = time.time() + 60
                                self.logger.error("[熔断] 暂停60秒")
                            self.state = State.REPAIR
                            continue
                        elif result == "SKIP":
                            await asyncio.sleep(1)
                            continue

                        _l_ref_price = result["l_price"]
                        _h_ref_price = result["h_price"]
                        _hedge_ms = result["ms"]
                        _l_s = result["l_side"]
                        _open_qty = result.get("qty", self.order_size)

                        if not self._pending_hs_verify:
                            await self._fetch_hotstuff_fill_price(Decimal(str(_h_ref_price)))
                            _h_actual = self._last_hotstuff_avg_price or Decimal(str(_h_ref_price))
                        else:
                            _h_actual = Decimal(str(_h_ref_price))
                        _l_actual = self._last_lighter_avg_price or Decimal(str(_l_ref_price))
                        _h_estimated = self._pending_hs_verify or (
                            self._last_hotstuff_avg_price is None
                            or self._last_hotstuff_avg_price == Decimal(str(_h_ref_price)))

                        if _l_s == "sell":
                            _spread = float((_l_actual - _h_actual) / _h_actual * 10000)
                        else:
                            _spread = float((_h_actual - _l_actual) / _l_actual * 10000)
                        _fee = float(FEE_HOTSTUFF_TAKER + FEE_LIGHTER_MAKER) * 10000
                        _net = _spread - _fee
                        self.cumulative_net_bps += _net
                        _dollar = _net / 10000 * float(_open_qty) * float(_h_actual)
                        self.cumulative_dollar_pnl += _dollar
                        self._consecutive_hedge_fails = 0
                        _did_trade = True

                        _est_tag = "(估)" if _h_estimated else ""
                        _dev_l = float((_l_actual - Decimal(str(_l_ref_price))) / Decimal(str(_l_ref_price)) * 10000) if Decimal(str(_l_ref_price)) > 0 else 0
                        _dev_h = float((_h_actual - Decimal(str(_h_ref_price))) / Decimal(str(_h_ref_price)) * 10000) if Decimal(str(_h_ref_price)) > 0 else 0

                        self.logger.info(
                            f"[开仓成交] L_{_l_s}@{_l_actual} H_{_h_side}@{_h_actual}{_est_tag}  "
                            f"qty={_open_qty}  "
                            f"spread={_spread:.2f}bps fee={_fee:.2f}bps net={_net:+.2f}bps  "
                            f"偏移:L={_dev_l:+.1f}bps H={_dev_h:+.1f}bps  "
                            f"${_dollar:+.4f}  {_hedge_ms:.0f}ms  "
                            f"累积=${self.cumulative_dollar_pnl:+.4f}")

                        if _h_estimated:
                            self._pending_open_context = {
                                "l_actual": _l_actual, "l_side": _l_s,
                                "qty": _open_qty, "h_ref": Decimal(str(_h_ref_price)),
                                "est_dollar": _dollar, "est_net_bps": _net,
                                "seq": self._trade_seq + 1,
                            }
                        else:
                            self._pending_open_context = None

                        self._trade_seq += 1
                        self._csv_row([
                            datetime.now(_TZ_CN).isoformat(),
                            f"hsv27_open_{self._trade_seq:04d}", _l_s,
                            str(_l_actual), str(_h_actual), str(_open_qty),
                            f"{_spread:.2f}", f"{_fee:.2f}", f"{_net:.2f}",
                            f"{_hedge_ms:.0f}", f"{self.cumulative_net_bps:.2f}",
                            "", "", "", "",
                        ])
                        if self.ladder_step_bps > 0:
                            self._ladder_level += 1
                            self.logger.info(f"[阶梯] 升级到 L{self._ladder_level}")
                        asyncio.create_task(self._record_pre_trade_balance())

                    if not _did_trade:
                        await asyncio.sleep(self.interval)

                # ━━━━━━ STATE: REPAIR ━━━━━━
                elif self.state == State.REPAIR:
                    self.logger.info("[REPAIR] 分级修复...")
                    await self._cancel_all_lighter()
                    await self._reconcile()
                    _net = self.lighter_position + self.hotstuff_position
                    _imbalance = abs(_net)

                    if _imbalance < self.hs_lot_size:
                        self.logger.info(
                            f"[REPAIR] 偏差{_imbalance}极小, 忽略")
                    elif _imbalance <= self.order_size * Decimal("1.5"):
                        self.logger.info(
                            f"[REPAIR] 偏差{_imbalance}, Lighter IOC修复(0费用)")
                        _fix_ref = None
                        if _net < 0:
                            _fix_ref = self._l_best_ask
                            _fix_side = "buy"
                        else:
                            _fix_ref = self._l_best_bid
                            _fix_side = "sell"
                        if _fix_ref and _fix_ref > 0:
                            _fix_ok = await self._lighter_ioc(
                                _fix_side, _imbalance, _fix_ref)
                            if _fix_ok:
                                self.logger.info("[REPAIR] Lighter修复成功")
                            else:
                                self.logger.warning("[REPAIR] Lighter修复失败, 重试对账")
                            await asyncio.sleep(0.5)
                            await self._reconcile()
                        else:
                            self.logger.warning("[REPAIR] 无BBO, 等待下次修复")
                    else:
                        self.logger.error(
                            f"[REPAIR] 大偏差{_imbalance} > {self.order_size * Decimal('1.5')}, 紧急平仓!")
                        await self._emergency_flatten()
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


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description="Hotstuff+Lighter V27 双腿Taker套利")
    p.add_argument("-s", "--symbol", default="BTC")
    p.add_argument("--size", type=str, default="0.005")
    p.add_argument("--max-position", type=str, default="0.025")
    p.add_argument("--open-spread", type=float, default=5.0,
                   help="开仓价差阈值bps (默认5.0)")
    p.add_argument("--close-spread", type=float, default=0,
                   help="平仓价差阈值bps (默认0)")
    p.add_argument("--max-order-age", type=float, default=60.0)
    p.add_argument("--spread-buffer", type=float, default=0.0,
                   help="价差缓冲bps (默认0)")
    p.add_argument("--ladder-step", type=float, default=1.5,
                   help="阶梯间距bps (默认1.5, 设0禁用)")
    p.add_argument("--close-deep", type=float, default=-0.5,
                   help="深度平仓阈值bps (默认-0.5)")
    p.add_argument("--close-wait", type=float, default=2.0,
                   help="快速平仓后等待秒数 (默认2.0)")
    p.add_argument("--hedge-timeout", type=float, default=5.0)
    p.add_argument("--interval", type=float, default=0.05)
    p.add_argument("--max-loss", type=float, default=0,
                   help="最大累积亏损(美元), 超过则停止交易 (默认0=不限)")
    p.add_argument("--max-slippage-bps", type=float, default=5.0,
                   help="IOC滑点保护上限bps (默认5.0, 紧急平仓不受限)")
    p.add_argument("--flatten-on-start", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--env-file", default=".env")
    args = p.parse_args()

    load_dotenv(args.env_file)

    bot = HotstuffLighterBot(
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
    )
    bot.flatten_on_start = args.flatten_on_start
    bot.max_loss = args.max_loss
    bot._max_slippage_bps = args.max_slippage_bps

    fee_per = float(FEE_HOTSTUFF_TAKER) * 10000
    rt_fee = fee_per * 2
    min_profit = args.open_spread - abs(args.close_spread) - rt_fee
    step_info = f"  阶梯间距={args.ladder_step}bps" if args.ladder_step > 0 else "  阶梯=关闭"
    print(f"[Hotstuff+Lighter V27] 双腿Taker套利 (低滑点优化版)")
    print(f"  {args.symbol} size={args.size} max_pos={args.max_position}")
    print(f"  开仓: spread≥{args.open_spread}bps(+{args.spread_buffer}缓冲) → 双腿IOC(BBO二次验证+L1深度检查)")
    print(f"  平仓: basis≤{args.close_spread} → 快速平3层 → 等{args.close_wait}s → basis≤{args.close_deep}再平剩余")
    print(step_info)
    print(f"  滑点保护≤{args.max_slippage_bps}bps  isMarket=True")
    print(f"  往返费用={rt_fee:.2f}bps  预期每笔净利={min_profit:+.2f}bps")

    if uvloop is not None:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("[uvloop] 已启用")

    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n已中断。")


if __name__ == "__main__":
    main()
