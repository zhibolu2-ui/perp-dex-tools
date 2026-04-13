#!/usr/bin/env python3
"""
V23: 价差锁定 + 即时对冲 + 余额校验 (Spread Lock + Instant Hedge + Balance Verify)

基于V22, 新增:
  5. 每次开仓前记录两个交易所的总余额
  6. 每次平仓后查询真实余额, 计算真实P&L
  7. 对比公式P&L和余额P&L, 偏差过大自动报警
  8. CSV新增 real_pnl / balance_lighter / balance_extended 列

原有V22功能不变:
  1. WS回调中锁定Extended BBO快照 + 即时fire对冲, 消除主循环延迟
  2. Extended开仓/平仓统一IOC taker
  3. 内部价差缓冲(--spread-buffer), 补偿200ms延迟滑点
  4. 追踪Extended实际成交价

费用:
  Lighter 免费账户 maker: 0 bps
  Extended taker(开仓+平仓): 2.25 bps
  单笔开仓费用: 2.25 bps, 平仓费用: 2.25 bps

用法:
  python spread_arb_v23.py --symbol BTC --size 0.001 --open-spread 5 --close-spread 0
  python spread_arb_v23.py --symbol BTC --size 0.001 --open-spread 5 --spread-buffer 2.0
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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import websockets
from dotenv import load_dotenv
from lighter.signer_client import SignerClient
from x10.perpetual.orders import TimeInForce, OrderSide

sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.extended import ExtendedClient

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

FEE_EXTENDED_TAKER = Decimal("0.000225")  # 2.25 bps
FEE_LIGHTER_MAKER = Decimal("0")          # 免费账户 0 bps

LIGHTER_RATE_LIMIT = 36       # 安全上限(实际40), 留4次给紧急操作
LIGHTER_RATE_WINDOW = 60.0    # 窗口60秒

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
    ):
        self.symbol = symbol.upper()
        self.order_size = order_size
        self.max_position = max_position
        self.open_spread_bps = open_spread_bps
        self.close_spread_bps = close_spread_bps
        self.spread_buffer_bps = spread_buffer_bps
        self.hedge_timeout = hedge_timeout
        self.max_order_age = max_order_age
        self.interval = interval
        self.dry_run = dry_run

        self.state = State.IDLE
        self.stop_flag = False
        self._in_flatten = False

        # ── Positions ──
        self.lighter_position = Decimal("0")
        self.extended_position = Decimal("0")

        # ── Lighter market config ──
        self.lighter_market_index: int = SYMBOL_TO_MARKET.get(self.symbol, 1)
        self.account_index: int = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
        self.api_key_index: int = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))
        self.base_amount_multiplier: int = 0
        self.price_multiplier: int = 0
        self.lighter_tick = Decimal("0.1")
        self.lighter_min_size_step = Decimal("0.00001")

        # ── Extended config ──
        self.extended_contract_id: str = ""
        self.extended_tick_size = Decimal("1")
        self.extended_min_order_size = Decimal("0.001")

        # ── Lighter maker state ──
        self._l_buy_client_id: Optional[int] = None
        self._l_sell_client_id: Optional[int] = None
        self._l_buy_order_idx: Optional[int] = None
        self._l_sell_order_idx: Optional[int] = None
        self._l_buy_price = Decimal("0")
        self._l_sell_price = Decimal("0")
        self._l_placed_at: float = 0.0
        self._l_ext_bid_at_place = Decimal("0")
        self._l_ext_ask_at_place = Decimal("0")
        self._last_modify_ts: float = 0.0

        # ── Lighter API rate limiter (40 req / 60s for free accounts) ──
        self._lighter_req_times: collections.deque = collections.deque()

        # ── Fill state ──
        self._fill_event = asyncio.Event()
        self._fill_side: Optional[str] = None
        self._fill_price = Decimal("0")
        self._fill_qty = Decimal("0")

        # ── Close fill tracking (Lighter actual price during flatten) ──
        self._flatten_fill_price: Optional[Decimal] = None
        self._flatten_fill_qty = Decimal("0")
        self._flatten_fill_event = asyncio.Event()
        self._flatten_ioc_cid: Optional[int] = None

        # ── Stale fills queue + accumulator ──
        self._stale_fills: List[dict] = []
        self._stale_buy_accum = Decimal("0")
        self._stale_sell_accum = Decimal("0")

        # ── Lighter OB state (set by _lighter_ws_loop) ──
        self._l_ob: Dict[str, Dict[float, float]] = {"bids": {}, "asks": {}}
        self._l_ob_ready: bool = False
        self._l_best_bid: Optional[Decimal] = None
        self._l_best_ask: Optional[Decimal] = None
        self._l_ws_ts: float = 0.0

        # ── Clients ──
        self.lighter_client: Optional[SignerClient] = None
        self.extended_client: Optional[ExtendedClient] = None

        # ── Extended WS state (IOC tracking) ──
        self._x_ioc_pending_id: Optional[str] = None
        self._x_ioc_confirmed = asyncio.Event()
        self._x_ioc_fill_qty = Decimal("0")
        self._x_ioc_fill_price: Optional[Decimal] = None

        # ── V22: Fill-time Extended BBO snapshot ──
        self._fill_ext_bid: Optional[Decimal] = None
        self._fill_ext_ask: Optional[Decimal] = None
        self._fill_ts: float = 0.0

        # ── V22: Instant hedge result (set by _instant_hedge task) ──
        self._instant_hedge_task: Optional[asyncio.Task] = None
        self._instant_hedge_done = asyncio.Event()
        self._instant_hedge_ok: bool = False
        self._instant_hedge_fill_price: Optional[Decimal] = None
        self._instant_hedge_ms: float = 0.0

        # ── Risk ──
        self._consecutive_hedge_fails: int = 0
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

        # ── Logging / CSV ──
        os.makedirs("logs", exist_ok=True)
        self.csv_path = f"logs/spread_v23_{self.symbol}_trades.csv"
        self.log_path = f"logs/spread_v23_{self.symbol}.log"
        self._init_csv()
        self.logger = self._init_logger()

    # ─────────────────────────────────────────────
    #  Logging / CSV
    # ─────────────────────────────────────────────

    def _init_logger(self) -> logging.Logger:
        lg = logging.getLogger(f"v23_{self.symbol}")
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
                    "lighter_price", "extended_price", "qty",
                    "spread_bps", "fee_bps", "net_bps",
                    "hedge_ms", "cumulative_bps",
                    "real_pnl", "cumulative_real_pnl",
                    "balance_lighter", "balance_extended",
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

    def _init_extended(self):
        class _Cfg:
            def __init__(self, d):
                for k, v in d.items():
                    setattr(self, k, v)
        cfg = _Cfg({
            "ticker": self.symbol,
            "contract_id": f"{self.symbol}-USD",
            "take_profit": 0.1,
            "tick_size": Decimal("1") if self.symbol == "BTC" else Decimal("0.01"),
            "close_order_side": "sell",
        })
        self.extended_client = ExtendedClient(cfg)
        self.extended_tick_size = cfg.tick_size
        self.extended_contract_id = cfg.contract_id
        self.extended_min_order_size = Decimal("0.001") if self.symbol == "BTC" else Decimal("0.01")
        self.logger.info("Extended client 已初始化")

    def _round_lighter(self, price: Decimal) -> Decimal:
        if self.lighter_tick > 0:
            return (price / self.lighter_tick).quantize(Decimal("1")) * self.lighter_tick
        return price

    # ─────────────────────────────────────────────
    #  Extended BBO helper
    # ─────────────────────────────────────────────

    def _get_ext_bbo(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        if self.extended_client and self.extended_client.orderbook:
            ob = self.extended_client.orderbook
            bids = ob.get("bid", [])
            asks = ob.get("ask", [])
            bid = Decimal(bids[0]["p"]) if bids else None
            ask = Decimal(asks[0]["p"]) if asks else None
            return bid, ask
        return None, None

    # ─────────────────────────────────────────────
    #  Pricing engine
    # ─────────────────────────────────────────────

    def compute_maker_prices(
        self, ext_bid: Decimal, ext_ask: Decimal,
        l_bid: Decimal, l_ask: Decimal,
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Compute Lighter maker order prices based on Extended BBO.

        V22: 内部价差 = open_spread + spread_buffer, 补偿200ms延迟滑点。
        用户设 --open-spread 5 --spread-buffer 2 时, 挂单按7bps距离,
        即使损失2bps滑点, 实际成交仍>=5bps。
        """
        effective_bps = self.open_spread_bps + self.spread_buffer_bps
        spread = Decimal(str(effective_bps)) / Decimal("10000")
        basis = l_bid - ext_ask
        basis_bps = float(basis / ext_ask * 10000) if ext_ask > 0 else 0

        sell_price = None
        if basis_bps > 0:
            sell_target = ext_ask * (Decimal("1") + spread)
            sell_price = self._round_lighter(sell_target)
            if sell_price <= l_ask:
                sell_price = l_ask + self.lighter_tick
            if sell_price <= 0:
                sell_price = None

        buy_price = None
        if basis_bps < -1.0:
            buy_target = ext_bid * (Decimal("1") - spread)
            buy_price = self._round_lighter(buy_target)
            if buy_price >= l_bid:
                buy_price = l_bid - self.lighter_tick
            if buy_price <= 0:
                buy_price = None

        return buy_price, sell_price

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
        side = "sell" if is_ask else "buy"

        is_our_buy = (cid is not None and self._l_buy_client_id is not None
                      and int(cid) == self._l_buy_client_id)
        is_our_sell = (cid is not None and self._l_sell_client_id is not None
                       and int(cid) == self._l_sell_client_id)

        if (is_our_buy or is_our_sell) and not self._fill_event.is_set():
            if is_ask:
                self.lighter_position -= filled_base
            else:
                self.lighter_position += filled_base
            self._fill_side = side
            self._fill_price = avg_price
            self._fill_qty = filled_base

            # V22: 立即锁定Extended BBO快照 + 异步发起对冲
            self._fill_ext_bid, self._fill_ext_ask = self._get_ext_bbo()
            self._fill_ts = time.time()
            self._instant_hedge_done.clear()
            self._instant_hedge_ok = False
            self._instant_hedge_fill_price = None
            self._instant_hedge_task = asyncio.create_task(
                self._instant_hedge_fire())

            self._fill_event.set()
            self.logger.info(
                f"[Lighter] 成交: {side} {filled_base} @ {avg_price}  "
                f"X_snap={self._fill_ext_bid}/{self._fill_ext_ask} → 即时对冲已触发")
        elif self._in_flatten:
            _is_flatten_order = (cid is not None and self._flatten_ioc_cid is not None
                                 and int(cid) == self._flatten_ioc_cid)
            if not _is_flatten_order:
                self.logger.warning(
                    f"[Lighter] 平仓期间收到非平仓订单成交! cid={cid} (平仓cid={self._flatten_ioc_cid})")
            self._flatten_fill_price = avg_price
            self._flatten_fill_qty = filled_base
            self._flatten_fill_event.set()
            self.logger.info(
                f"[Lighter] 平仓成交(实际价={avg_price}): {side} {filled_base} @ {avg_price}")
        else:
            _is_late_flatten = (cid is not None and self._flatten_ioc_cid is not None
                                and int(cid) == self._flatten_ioc_cid)
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
    #  Extended IOC hedge
    # ─────────────────────────────────────────────

    def _on_extended_order_update(self, event: dict):
        oid = str(event.get("order_id", ""))
        status = event.get("status")

        # IOC hedge tracking
        if oid and self._x_ioc_pending_id and oid == str(self._x_ioc_pending_id):
            filled = event.get("filled_size")
            avg = event.get("avg_price")
            if filled:
                self._x_ioc_fill_qty = Decimal(str(filled))
            if avg:
                try:
                    self._x_ioc_fill_price = Decimal(str(avg))
                except Exception:
                    pass
            if status in ("FILLED", "CANCELED", "PARTIALLY_FILLED"):
                self._x_ioc_confirmed.set()

    async def _hedge_extended(self, side: str, qty: Decimal, ref_price: Decimal) -> Optional[Decimal]:
        """V22对冲：IOC + 200ms等WS确认获取实际成交价。"""
        slip = Decimal("0.005")
        if side == "buy":
            price = ref_price * (Decimal("1") + slip)
        else:
            price = ref_price * (Decimal("1") - slip)
        price = price.quantize(self.extended_tick_size, rounding=ROUND_HALF_UP)
        qty_r = qty.quantize(self.extended_min_order_size, rounding=ROUND_HALF_UP)

        if qty_r < self.extended_min_order_size:
            self.logger.warning(f"[Extended] 数量{qty_r}低于最小值{self.extended_min_order_size}, 跳过")
            return None

        self._x_ioc_confirmed.clear()
        self._x_ioc_fill_price = None
        self._x_ioc_fill_qty = Decimal("0")
        self._x_ioc_pending_id = None

        _t0 = time.time()
        result = await self.extended_client.place_ioc_order(
            contract_id=self.extended_contract_id,
            quantity=qty_r,
            side=side,
            aggressive_price=price,
        )
        _t1 = time.time()
        if not result.success:
            self.logger.error(f"[Extended] IOC失败({(_t1-_t0)*1000:.0f}ms): {result.error_message}")
            return None

        if result.order_id:
            self._x_ioc_pending_id = str(result.order_id)

        if side == "buy":
            self.extended_position += qty_r
        else:
            self.extended_position -= qty_r

        _actual_price = ref_price
        if self._x_ioc_confirmed.is_set():
            if self._x_ioc_fill_price and self._x_ioc_fill_price > 0:
                _actual_price = self._x_ioc_fill_price

        _ms = (_t1 - _t0) * 1000
        self.logger.info(f"[Extended] IOC完成({_ms:.0f}ms) 价={_actual_price}")
        return _actual_price

    async def _instant_hedge_fire(self):
        """V22: 从WS回调触发的即时对冲, 跳过主循环延迟。含snapshot spread guard。"""
        try:
            _side = self._fill_side
            _qty = self._fill_qty
            _l_price = self._fill_price
            _ext_bid = self._fill_ext_bid
            _ext_ask = self._fill_ext_ask

            if _side is None or _qty <= 0:
                return

            _hedge_side = "sell" if _side == "buy" else "buy"
            _ref = _ext_bid if _hedge_side == "sell" else _ext_ask

            if _ref is None or _ref <= 0:
                _ref = _l_price

            if _side == "sell" and _ext_ask and _ext_ask > 0:
                _snap_spread = float((_l_price - _ext_ask) / _ext_ask * 10000)
            elif _side == "buy" and _ext_bid and _ext_bid > 0:
                _snap_spread = float((_ext_bid - _l_price) / _l_price * 10000)
            else:
                _snap_spread = 0

            _t0 = time.time()
            _fill_price = await self._hedge_extended(_hedge_side, _qty, _ref)
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
    #  Close position (spread convergence)
    # ─────────────────────────────────────────────

    async def _close_position(self, l_bid: Decimal, l_ask: Decimal,
                               ext_bid: Decimal, ext_ask: Decimal) -> bool:
        """V22平仓: Lighter IOC taker(0费) + Extended IOC taker(2.25bps)。"""
        self._in_flatten = True
        await self._cancel_all_lighter()

        l_qty = abs(self.lighter_position)
        x_qty = abs(self.extended_position)
        close_qty = min(l_qty, x_qty, self.max_position)

        if close_qty < self.extended_min_order_size:
            self._in_flatten = False
            return False

        close_qty = close_qty.quantize(self.extended_min_order_size, rounding=ROUND_DOWN)
        if close_qty <= 0:
            self._in_flatten = False
            return False

        if self.lighter_position < 0:
            l_side = "buy"
            x_side = "sell"
            l_ref = l_ask
            x_ref = ext_bid
        else:
            l_side = "sell"
            x_side = "buy"
            l_ref = l_bid
            x_ref = ext_ask

        self.logger.info(
            f"[平仓] 价差收敛! L_{l_side} {close_qty} ref={l_ref} + "
            f"X_{x_side} {close_qty} ref={x_ref}")

        _t0 = time.time()

        # Lighter taker (0 fee, 300ms latency)
        l_price = l_ref
        slip = Decimal("0.005")
        if l_side == "buy":
            l_price = l_ref * (Decimal("1") + slip)
        else:
            l_price = l_ref * (Decimal("1") - slip)
        l_price = self._round_lighter(l_price)

        self._flatten_fill_event.clear()
        self._flatten_fill_price = None
        self._flatten_fill_qty = Decimal("0")
        _flatten_cid = int(time.time() * 1000) * 10 + 9
        self._flatten_ioc_cid = _flatten_cid

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
                self.logger.error(f"[平仓] Lighter IOC失败: {err}")
                self._in_flatten = False
                return False
            if l_side == "buy":
                self.lighter_position += close_qty
            else:
                self.lighter_position -= close_qty
        except Exception as e:
            self.logger.error(f"[平仓] Lighter异常: {e}")
            self._in_flatten = False
            return False

        # Extended taker平仓 (2.25bps) — 不等Lighter WS, 立刻发出
        x_fill = await self._hedge_extended(x_side, close_qty, x_ref)
        if x_fill is None:
            self.logger.error("[平仓] Extended对冲失败! 立即重试")
            ext_bid, ext_ask = self._get_ext_bbo()
            _retry_ref = ext_bid if x_side == "sell" else ext_ask
            if _retry_ref and _retry_ref > 0:
                for _r in range(2):
                    await asyncio.sleep(0.2)
                    x_fill = await self._hedge_extended(x_side, close_qty, _retry_ref)
                    if x_fill is not None:
                        self.logger.info(f"[平仓] Extended重试{_r+1}成功")
                        break
                    ext_bid, ext_ask = self._get_ext_bbo()
                    if ext_bid and ext_ask:
                        _retry_ref = ext_bid if x_side == "sell" else ext_ask
            if x_fill is None:
                self.logger.error("[平仓] Extended 3次全部失败! 进入REPAIR")
                self._in_flatten = False
                return False

        # 两腿IOC都已发出, 现在统一等WS拿实际成交价(不影响执行速度)
        _ws_tasks = []
        if not self._flatten_fill_event.is_set():
            _ws_tasks.append(asyncio.wait_for(
                self._flatten_fill_event.wait(), timeout=0.5))
        if not self._x_ioc_confirmed.is_set():
            _ws_tasks.append(asyncio.wait_for(
                self._x_ioc_confirmed.wait(), timeout=0.5))
        if _ws_tasks:
            await asyncio.gather(*_ws_tasks, return_exceptions=True)

        _l_actual = l_ref
        if self._flatten_fill_price and self._flatten_fill_price > 0:
            _l_actual = self._flatten_fill_price

        _x_actual = x_fill
        if self._x_ioc_fill_price and self._x_ioc_fill_price > 0:
            _x_actual = self._x_ioc_fill_price

        # 仓位修正: 如果WS确认的实际成交量和预期不同, 用实际量修正
        _l_real_qty = self._flatten_fill_qty if self._flatten_fill_qty > 0 else close_qty
        if _l_real_qty != close_qty:
            _diff = close_qty - _l_real_qty
            if l_side == "buy":
                self.lighter_position -= _diff
            else:
                self.lighter_position += _diff
            self.logger.warning(
                f"[平仓] Lighter部分成交: 预期={close_qty} 实际={_l_real_qty} → 仓位已修正")

        _x_real_qty = self._x_ioc_fill_qty if self._x_ioc_fill_qty > 0 else close_qty
        if _x_real_qty != close_qty:
            _diff = close_qty - _x_real_qty
            if x_side == "buy":
                self.extended_position -= _diff
            else:
                self.extended_position += _diff
            self.logger.warning(
                f"[平仓] Extended部分成交: 预期={close_qty} 实际={_x_real_qty} → 仓位已修正")

        _actual_qty = min(_l_real_qty, _x_real_qty)

        _ms = (time.time() - _t0) * 1000
        _x_price = Decimal(str(_x_actual))
        if l_side == "buy":
            _spread_bps = float((_l_actual - _x_price) / _x_price * 10000)
        else:
            _spread_bps = float((_x_price - _l_actual) / _l_actual * 10000)
        _fee = float(FEE_EXTENDED_TAKER + FEE_LIGHTER_MAKER) * 10000
        _net = -_spread_bps - _fee
        self.cumulative_net_bps += _net
        _dollar = float(_net / 10000 * float(_actual_qty) * float(_x_price))
        self.cumulative_dollar_pnl += _dollar

        self.logger.info(
            f"[平仓完成] L_{l_side}@{_l_actual} X_{x_side}@{_x_actual}  "
            f"close_spread={_spread_bps:.2f}bps fee={_fee:.2f}bps "
            f"close_net={_net:.2f}bps  ${_dollar:+.4f}  {_ms:.0f}ms  "
            f"累积=${self.cumulative_dollar_pnl:+.4f}")

        # V23: 平仓后余额校验(异步, 不阻塞下一轮交易)
        _real_pnl_str, _cum_real_str, _bal_l_str, _bal_x_str = \
            await self._v23_verify_close_pnl(
                _net, _dollar, _actual_qty, Decimal(str(_x_actual)))

        self._trade_seq += 1
        self._csv_row([
            datetime.now(_TZ_CN).isoformat(),
            f"v23_close_{self._trade_seq:04d}",
            f"close_{l_side}",
            str(_l_actual), str(_x_actual), str(_actual_qty),
            f"{_spread_bps:.2f}", f"{_fee:.2f}", f"{_net:.2f}",
            f"{_ms:.0f}", f"{self.cumulative_net_bps:.2f}",
            _real_pnl_str, _cum_real_str, _bal_l_str, _bal_x_str,
        ])
        self._in_flatten = False
        return True

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

    async def _get_extended_balance(self) -> Optional[Decimal]:
        try:
            bal_resp = await self.extended_client.perpetual_trading_client.account.get_balance()
            if bal_resp and hasattr(bal_resp, "data") and bal_resp.data:
                return bal_resp.data.equity
            return None
        except Exception as e:
            self.logger.warning(f"[余额] Extended余额查询失败: {e}")
            return None

    async def _snapshot_balances(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        l_bal, x_bal = await asyncio.gather(
            self._get_lighter_balance(),
            self._get_extended_balance(),
        )
        return l_bal, x_bal

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

    async def _get_extended_position(self) -> Decimal:
        try:
            positions_data = await self.extended_client.perpetual_trading_client.account.get_positions(
                market_names=[self.extended_contract_id])
            if not positions_data or not hasattr(positions_data, "data") or not positions_data.data:
                return Decimal("0")
            for p in positions_data.data:
                if p.market == self.extended_contract_id:
                    size = Decimal(str(p.size))
                    if str(p.side).upper() == "SHORT":
                        return -size
                    return size
            return Decimal("0")
        except Exception as e:
            self.logger.warning(f"获取Extended仓位失败: {e}")
            return self.extended_position

    async def _reconcile(self):
        if self._instant_hedge_task and not self._instant_hedge_task.done():
            self.logger.info("[对账] 等待即时对冲完成再对账...")
            try:
                await asyncio.wait_for(self._instant_hedge_done.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.logger.warning("[对账] 即时对冲超时, 继续对账")
        l = await self._get_lighter_position()
        x = await self._get_extended_position()
        _old_l, _old_x = self.lighter_position, self.extended_position
        self.lighter_position = l
        self.extended_position = x
        net = abs(l + x)
        if _old_l != l or _old_x != x:
            self.logger.info(
                f"[对账] 仓位同步: L {_old_l}→{l}  X {_old_x}→{x}")
        if net > self.order_size * Decimal("0.5"):
            self.logger.warning(f"[对账] 不平衡: L={l} X={x} net={net}")
        return net < self.order_size * Decimal("2")

    async def _emergency_flatten(self):
        self.logger.error("[紧急平仓] 开始...")
        self._in_flatten = True

        # 1. 先撤掉所有 Lighter 挂单
        await self._cancel_all_lighter()

        # 2. 对账获取真实仓位
        await self._reconcile()

        # 3. 平 Lighter 仓位
        if abs(self.lighter_position) >= self.extended_min_order_size:
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
                self.logger.warning(f"[紧急平仓] Lighter无BBO, 跳过")

        # 4. 平 Extended 仓位
        if abs(self.extended_position) >= self.extended_min_order_size:
            side = "sell" if self.extended_position > 0 else "buy"
            qty = abs(self.extended_position)
            ext_bid, ext_ask = self._get_ext_bbo()
            ref = ext_bid if side == "sell" else ext_ask
            if ref and ref > 0:
                price = ref * (Decimal("0.99") if side == "sell" else Decimal("1.01"))
                result = await self.extended_client.place_ioc_order(
                    contract_id=self.extended_contract_id,
                    quantity=qty, side=side, aggressive_price=price)
                if result.success:
                    if side == "sell":
                        self.extended_position -= qty
                    else:
                        self.extended_position += qty
                    self.logger.info(f"[紧急平仓] Extended {side} {qty} 已发送")
                else:
                    self.logger.error(f"[紧急平仓] Extended失败: {result.error_message}")
            else:
                self.logger.warning(f"[紧急平仓] Extended无BBO, 跳过")

        await asyncio.sleep(1.0)
        if not self.stop_flag:
            await self._reconcile()
            _net_after = abs(self.lighter_position + self.extended_position)
            if _net_after >= self.extended_min_order_size:
                self.logger.warning(
                    f"[紧急平仓] 第一次对账后仍有偏差={_net_after}, 第二次尝试修复")
                _net = self.lighter_position + self.extended_position
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
        self._in_flatten = False
        self._stale_fills.clear()
        self.logger.info(f"[紧急平仓] 完成: L={self.lighter_position} X={self.extended_position}")

    # ─────────────────────────────────────────────
    #  V23: Balance P&L verification
    # ─────────────────────────────────────────────

    async def _v23_record_pre_trade_balance(self):
        """开仓完成后快照余额, 作为下次平仓的基准。"""
        try:
            _l, _x = await self._snapshot_balances()
            if _l is not None:
                self._pre_trade_balance_l = _l
            if _x is not None:
                self._pre_trade_balance_x = _x
            if _l is not None and _x is not None:
                self.logger.info(
                    f"[V23] 开仓后余额快照: L={_l} X={_x} 合计={_l+_x}")
        except Exception as e:
            self.logger.warning(f"[V23] 开仓余额快照失败: {e}")

    async def _v23_verify_close_pnl(self, formula_net_bps: float, formula_dollar: float,
                                     close_qty: Decimal, x_price: Decimal) -> Tuple[str, str, str, str]:
        """平仓后查余额, 计算真实P&L, 与公式P&L对比。

        Returns: (real_pnl_str, cumulative_real_str, bal_l_str, bal_x_str)
        """
        try:
            _l_after, _x_after = await self._snapshot_balances()
            if _l_after is None or _x_after is None:
                return "", "", "", ""

            _bal_l_str = str(_l_after)
            _bal_x_str = str(_x_after)

            _pre_l = self._pre_trade_balance_l
            _pre_x = self._pre_trade_balance_x
            if _pre_l is None or _pre_x is None:
                self.logger.info(
                    f"[V23] 平仓后余额: L={_l_after} X={_x_after} (无开仓基准, 跳过对比)")
                self._pre_trade_balance_l = _l_after
                self._pre_trade_balance_x = _x_after
                return "", f"{self.cumulative_real_pnl:.6f}", _bal_l_str, _bal_x_str

            _real_pnl = float((_l_after + _x_after) - (_pre_l + _pre_x))
            self.cumulative_real_pnl += _real_pnl

            _real_bps = _real_pnl / (float(close_qty) * float(x_price)) * 10000 if close_qty > 0 and x_price > 0 else 0
            _diff = _real_pnl - formula_dollar
            _diff_bps = abs(_real_bps - formula_net_bps)

            self.logger.info(
                f"[V23 余额校验] "
                f"公式P&L=${formula_dollar:+.6f}({formula_net_bps:+.2f}bps)  "
                f"真实P&L=${_real_pnl:+.6f}({_real_bps:+.2f}bps)  "
                f"偏差=${_diff:+.6f}({_diff_bps:.2f}bps)  "
                f"累积真实=${self.cumulative_real_pnl:+.6f}")

            if _diff_bps > 2.0:
                self.logger.warning(
                    f"[V23 !!] P&L偏差过大! 公式vs真实差={_diff_bps:.2f}bps "
                    f"(>2bps阈值) → 可能有隐藏费用或滑点")

            # 初始余额累计校验
            if self._initial_balance_l is not None and self._initial_balance_x is not None:
                _total_now = _l_after + _x_after
                _total_init = self._initial_balance_l + self._initial_balance_x
                _session_real = float(_total_now - _total_init)
                _session_formula = self.cumulative_dollar_pnl
                _session_diff = _session_real - _session_formula
                self.logger.info(
                    f"[V23 会话] "
                    f"公式累积=${_session_formula:+.6f}  "
                    f"真实累积=${_session_real:+.6f}  "
                    f"偏差=${_session_diff:+.6f}")

            self._pre_trade_balance_l = _l_after
            self._pre_trade_balance_x = _x_after
            return f"{_real_pnl:+.6f}", f"{self.cumulative_real_pnl:.6f}", _bal_l_str, _bal_x_str

        except Exception as e:
            self.logger.warning(f"[V23] 余额校验异常: {e}")
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

        if (abs(self.lighter_position) >= self.extended_min_order_size
                or abs(self.extended_position) >= self.extended_min_order_size):
            self.logger.info(
                f"[退出] 残余仓位: L={self.lighter_position} "
                f"X={self.extended_position}, 紧急平仓")
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
        fee_one = float(FEE_EXTENDED_TAKER) * 10000
        self.logger.info(
            f"启动 V23 即时对冲+余额校验  {self.symbol}  "
            f"size={self.order_size}  "
            f"开仓>{self.open_spread_bps}bps(+{self.spread_buffer_bps}bps缓冲)  "
            f"平仓<{self.close_spread_bps}bps  "
            f"fee={fee_one:.2f}bps/笔  "
            f"max_age={self.max_order_age}s  "
            f"max_pos={self.max_position}  dry_run={self.dry_run}")

        if not self.dry_run:
            self._init_lighter()
            self._get_lighter_market_config()
            self._init_extended()
            await self.extended_client.connect()
            self.extended_client.setup_order_update_handler(self._on_extended_order_update)

            # 预热 Extended SDK 市场缓存 + HTTP 连接池
            _t0 = time.time()
            try:
                _markets = await self.extended_client.perpetual_trading_client.markets_info.get_markets()
                if _markets and _markets.data:
                    self.extended_client.perpetual_trading_client._PerpetualTradingClient__markets = {
                        m.name: m for m in _markets.data}
                _warmup_ms = (time.time() - _t0) * 1000
                self.logger.info(f"交易客户端就绪 (Extended预热={_warmup_ms:.0f}ms)")
            except Exception as e:
                self.logger.warning(f"Extended预热失败: {e}")
                self.logger.info("交易客户端就绪")

            await self._cancel_all_lighter()
        else:
            self._get_lighter_market_config()

        self._lighter_ws_task = asyncio.create_task(self._lighter_ws_loop())

        self.logger.info("等待行情就绪...")
        t0 = time.time()
        while time.time() - t0 < 20 and not self.stop_flag:
            ext_bid, ext_ask = self._get_ext_bbo()
            ext_ok = ext_bid is not None and ext_ask is not None
            l_ok = self._l_best_bid is not None and self._l_best_ask is not None
            if ext_ok and l_ok:
                break
            if time.time() - t0 > 5 and int(time.time() - t0) % 3 == 0:
                self.logger.info(
                    f"  等待中... X={'OK' if ext_ok else 'WAIT'} "
                    f"L={'OK' if l_ok else 'WAIT'}")
            await asyncio.sleep(0.5)

        ext_bid, ext_ask = self._get_ext_bbo()
        if ext_bid is None or self._l_best_bid is None:
            self.logger.error(
                f"行情超时! X={ext_bid}/{ext_ask} "
                f"L={self._l_best_bid}/{self._l_best_ask}")
            return

        self.logger.info(
            f"行情就绪: L={self._l_best_bid}/{self._l_best_ask}  "
            f"X={ext_bid}/{ext_ask}")

        if not self.dry_run:
            await self._reconcile()
            if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                self.logger.warning(
                    f"启动残余仓位: L={self.lighter_position} X={self.extended_position}, 紧急平仓")
                await self._emergency_flatten()

            # V23: 记录初始余额基准
            _l_bal, _x_bal = await self._snapshot_balances()
            self._initial_balance_l = _l_bal
            self._initial_balance_x = _x_bal
            self._pre_trade_balance_l = _l_bal
            self._pre_trade_balance_x = _x_bal
            if _l_bal is not None and _x_bal is not None:
                self.logger.info(
                    f"[V23] 初始余额: Lighter={_l_bal}  Extended={_x_bal}  "
                    f"合计={_l_bal + _x_bal}")
            else:
                self.logger.warning(
                    f"[V23] 初始余额部分获取失败: L={_l_bal} X={_x_bal}")

        self.logger.info(f"进入主循环  状态={self.state.value}")
        last_status_log = 0.0
        last_reconcile = time.time()

        while not self.stop_flag:
            try:
                now = time.time()
                ext_bid, ext_ask = self._get_ext_bbo()
                l_bid = self._l_best_bid
                l_ask = self._l_best_ask

                if ext_bid is None or ext_ask is None or l_bid is None or l_ask is None:
                    await asyncio.sleep(self.interval)
                    continue

                x_age = now - self.extended_client._ob_last_update_ts if self.extended_client else 999
                l_age = now - self._l_ws_ts if self._l_ws_ts > 0 else 999
                if l_age > 5 or x_age > 5:
                    await asyncio.sleep(self.interval)
                    continue

                mid = (l_bid + l_ask + ext_bid + ext_ask) / 4
                basis_bps = float((l_bid + l_ask - ext_bid - ext_ask) / 2 / mid * 10000)

                # ── Status log ──
                if now - last_status_log > 15:
                    self.logger.info(
                        f"[{self.state.value}] basis={basis_bps:+.1f}bps  "
                        f"L={l_bid}/{l_ask} X={ext_bid}/{ext_ask}  "
                        f"L_pos={self.lighter_position} X_pos={self.extended_position}  "
                        f"鲜度:L={l_age:.1f}s X={x_age:.1f}s  "
                        f"累积=${self.cumulative_dollar_pnl:+.4f}")
                    last_status_log = now

                # ── Periodic reconcile + 仓位修复 (统一Lighter IOC, 0费用) ──
                if not self.dry_run and now - last_reconcile > 60 and self.state == State.IDLE:
                    await self._reconcile()
                    last_reconcile = now
                    _net = self.lighter_position + self.extended_position
                    _imbalance = abs(_net)

                    if _imbalance >= self.lighter_min_size_step:
                        self.logger.warning(
                            f"[对账] 偏差={_imbalance}, "
                            f"L={self.lighter_position} X={self.extended_position} "
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

                # ── Handle stale fills → 立即对账修复 ──
                if self._stale_fills and self.state == State.IDLE:
                    sf = self._stale_fills.pop(0)
                    self.logger.warning(
                        f"[过期成交] {sf['side']} {sf['size']} @ {sf['price']} "
                        f"→ 立即触发对账修复")
                    await self._reconcile()
                    last_reconcile = now
                    _net = self.lighter_position + self.extended_position
                    _imbalance = abs(_net)
                    if _imbalance >= self.lighter_min_size_step:
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

                    if self.dry_run:
                        await asyncio.sleep(self.interval)
                        continue

                    _has_pos = (abs(self.lighter_position) >= self.extended_min_order_size
                                and abs(self.extended_position) >= self.extended_min_order_size)

                    # ── 平仓检查: 有仓位且价差收敛到阈值 (方向敏感) ──
                    if self.lighter_position < 0:
                        _should_close = basis_bps <= self.close_spread_bps
                    else:
                        _should_close = basis_bps >= -self.close_spread_bps

                    if _has_pos and _should_close:
                        self.logger.info(
                            f"[IDLE] 价差收敛触发平仓! "
                            f"basis={basis_bps:.1f}bps  阈值={'≤' if self.lighter_position < 0 else '≥'}"
                            f"{self.close_spread_bps if self.lighter_position < 0 else -self.close_spread_bps}bps  "
                            f"L={self.lighter_position} X={self.extended_position}")
                        ok = await self._close_position(
                            l_bid, l_ask, ext_bid, ext_ask)
                        if not ok:
                            self.logger.error("[IDLE] 平仓失败 → REPAIR")
                            self.state = State.REPAIR
                        continue

                    # ── 持仓上限检查 ──
                    _abs_l = abs(self.lighter_position)
                    _abs_x = abs(self.extended_position)
                    _at_max = (_abs_l >= self.max_position
                               or _abs_x >= self.max_position)
                    if _at_max:
                        if now - last_status_log > 15:
                            self.logger.info(
                                f"[IDLE] 持仓已满 "
                                f"L={self.lighter_position} X={self.extended_position} "
                                f"max={self.max_position}, 等待价差收敛平仓")
                        await asyncio.sleep(1)
                        continue

                    # 挂新单前先清理残余
                    if (self._l_buy_client_id or self._l_sell_client_id
                            or self._l_buy_order_idx or self._l_sell_order_idx):
                        await self._cancel_all_lighter()

                    buy_price, sell_price = self.compute_maker_prices(
                        ext_bid, ext_ask, l_bid, l_ask)

                    if self.lighter_position < -self.max_position * Decimal("0.8"):
                        sell_price = None
                    if self.lighter_position > self.max_position * Decimal("0.8"):
                        buy_price = None

                    self._fill_event.clear()
                    self._fill_side = None

                    if buy_price:
                        cid = await self._place_lighter_order("buy", self.order_size, buy_price)
                        if cid:
                            self._l_buy_client_id = cid
                            self._l_buy_price = buy_price

                    if sell_price:
                        cid = await self._place_lighter_order("sell", self.order_size, sell_price)
                        if cid:
                            self._l_sell_client_id = cid
                            self._l_sell_price = sell_price

                    if self._l_buy_client_id or self._l_sell_client_id:
                        self._l_placed_at = now
                        self._l_ext_bid_at_place = ext_bid
                        self._l_ext_ask_at_place = ext_ask
                        self.state = State.QUOTING
                    else:
                        await asyncio.sleep(1)

                # ━━━━━━ STATE: QUOTING ━━━━━━
                elif self.state == State.QUOTING:
                    if self._fill_event.is_set():
                        _f_side = self._fill_side
                        _f_price = self._fill_price
                        _f_qty = self._fill_qty

                        # V22: 用fill时的Extended快照计算spread
                        _snap_bid = self._fill_ext_bid
                        _snap_ask = self._fill_ext_ask
                        if _f_side == "sell" and _snap_ask and _snap_ask > 0:
                            _real_spread = float((_f_price - _snap_ask) / _snap_ask * 10000)
                        elif _f_side == "buy" and _snap_bid and _snap_bid > 0:
                            _real_spread = float((_snap_bid - _f_price) / _f_price * 10000)
                        else:
                            _real_spread = 0

                        _snap_ms = (time.time() - self._fill_ts) * 1000 if self._fill_ts > 0 else 0
                        self.logger.info(
                            f"[QUOTING] 成交! snap_spread={_real_spread:.1f}bps "
                            f"(snap延迟{_snap_ms:.0f}ms) → 等待即时对冲结果")
                        self.state = State.HEDGING
                        continue

                    # ── QUOTING中也检测平仓条件 (方向敏感) ──
                    _has_pos = (abs(self.lighter_position) >= self.extended_min_order_size
                                and abs(self.extended_position) >= self.extended_min_order_size)
                    if self.lighter_position < 0:
                        _should_close_q = basis_bps <= self.close_spread_bps
                    else:
                        _should_close_q = basis_bps >= -self.close_spread_bps
                    if _has_pos and _should_close_q:
                        self.logger.info(
                            f"[QUOTING] 价差收敛! basis={basis_bps:.1f}bps "
                            f"→ 撤单+平仓")
                        await self._cancel_all_lighter()
                        await asyncio.sleep(0.05)
                        if self._fill_event.is_set():
                            self.state = State.HEDGING
                            continue
                        self._reset_cycle()
                        ok = await self._close_position(
                            l_bid, l_ask, ext_bid, ext_ask)
                        if not ok:
                            self.logger.error("[QUOTING] 平仓失败 → REPAIR")
                            self.state = State.REPAIR
                        else:
                            self.state = State.IDLE
                        continue

                    age = now - self._l_placed_at
                    need_requote = False

                    if age > self.max_order_age:
                        self.logger.info(f"[QUOTING] 超时({age:.0f}s), 撤单回IDLE")
                        await self._cancel_all_lighter()
                        await asyncio.sleep(0.3)
                        if self._fill_event.is_set():
                            self.state = State.HEDGING
                            continue
                        self._reset_cycle()
                        self.state = State.IDLE
                        continue

                    # V22: 限频modify — 最少2秒间隔, 至少3tick变化才触发
                    _min_move = self.extended_tick_size * 3
                    if abs(ext_ask - self._l_ext_ask_at_place) >= _min_move:
                        need_requote = True
                    elif abs(ext_bid - self._l_ext_bid_at_place) >= _min_move:
                        need_requote = True

                    _since_last_mod = now - self._last_modify_ts
                    if need_requote and age >= 0.5 and _since_last_mod >= 2.0:
                        new_buy, new_sell = self.compute_maker_prices(
                            ext_bid, ext_ask, l_bid, l_ask)

                        _modified = False
                        if self._l_sell_order_idx and new_sell and new_sell > self._l_sell_price:
                            ok = await self._modify_lighter_order(
                                self._l_sell_order_idx, new_sell, self.order_size)
                            if ok:
                                self._l_sell_price = new_sell
                                self._l_ext_ask_at_place = ext_ask
                                _modified = True
                                self.logger.info(
                                    f"[QUOTING] Modify sell ↑ {new_sell} "
                                    f"(X_ask: {ext_ask})")

                        if self._l_buy_order_idx and new_buy and new_buy < self._l_buy_price:
                            ok = await self._modify_lighter_order(
                                self._l_buy_order_idx, new_buy, self.order_size)
                            if ok:
                                self._l_buy_price = new_buy
                                self._l_ext_bid_at_place = ext_bid
                                _modified = True
                                self.logger.info(
                                    f"[QUOTING] Modify buy ↓ {new_buy} "
                                    f"(X_bid: {ext_bid})")

                        if _modified:
                            self._last_modify_ts = time.time()

                        if self._fill_event.is_set():
                            continue

                    try:
                        await asyncio.wait_for(self._fill_event.wait(), timeout=0.5)
                        if self._fill_event.is_set():
                            continue
                    except asyncio.TimeoutError:
                        pass

                # ━━━━━━ STATE: HEDGING ━━━━━━
                elif self.state == State.HEDGING:
                    _side = self._fill_side
                    _qty = self._fill_qty
                    _l_price = self._fill_price
                    _hedge_side = "sell" if _side == "buy" else "buy"

                    # V22: 等待WS回调触发的即时对冲完成 (最多3秒)
                    try:
                        await asyncio.wait_for(
                            self._instant_hedge_done.wait(), timeout=3.0)
                    except asyncio.TimeoutError:
                        self.logger.warning("[HEDGING] 即时对冲超时, 重试")

                    _hedge_ok = self._instant_hedge_ok
                    _x_fill_price = self._instant_hedge_fill_price
                    _hedge_ms = self._instant_hedge_ms

                    # 即时对冲失败(网络/API异常) → fallback重试
                    if not _hedge_ok:
                        ext_bid, ext_ask = self._get_ext_bbo()
                        _ref = ext_bid if _hedge_side == "sell" else ext_ask
                        if _ref is None or _ref <= 0:
                            _ref = _l_price

                        if _side == "sell" and ext_ask and ext_ask > 0:
                            _cur_spread = float((_l_price - ext_ask) / ext_ask * 10000)
                        elif _side == "buy" and ext_bid and ext_bid > 0:
                            _cur_spread = float((ext_bid - _l_price) / _l_price * 10000)
                        else:
                            _cur_spread = 0

                        self.logger.info(
                            f"[HEDGING] 即时对冲失败, fallback重试 "
                            f"ref={_ref} cur_spread={_cur_spread:.1f}bps")
                        _hedge_start = time.time()
                        for _retry in range(3):
                            if self.stop_flag:
                                break
                            _x_fill_price = await self._hedge_extended(
                                _hedge_side, _qty, _ref)
                            if _x_fill_price is not None:
                                _hedge_ok = True
                                break
                            self.logger.warning(f"[HEDGING] 重试 {_retry+1}/3")
                            await asyncio.sleep(0.1)
                            ext_bid, ext_ask = self._get_ext_bbo()
                            if ext_bid and ext_ask:
                                _ref = ext_bid if _hedge_side == "sell" else ext_ask
                        _hedge_ms = (time.time() - _hedge_start) * 1000

                    if _hedge_ok and _x_fill_price:
                        # 对冲已完成, 在记账前等WS拿实际成交价(不影响对冲速度)
                        if not self._x_ioc_confirmed.is_set():
                            try:
                                await asyncio.wait_for(
                                    self._x_ioc_confirmed.wait(), timeout=0.5)
                            except asyncio.TimeoutError:
                                self.logger.warning(
                                    "[HEDGING] Extended WS 500ms未确认, 用快照价记账(可能有偏差)")
                        if self._x_ioc_fill_price and self._x_ioc_fill_price > 0:
                            _x_fill_price = self._x_ioc_fill_price

                        if _side == "buy":
                            _spread = float((_x_fill_price - _l_price) / _l_price * 10000)
                        else:
                            _spread = float((_l_price - _x_fill_price) / _x_fill_price * 10000)
                        _fee = float(FEE_EXTENDED_TAKER + FEE_LIGHTER_MAKER) * 10000
                        _net = _spread - _fee
                        self.cumulative_net_bps += _net
                        _dollar = _net / 10000 * float(_qty) * float(_x_fill_price)
                        self.cumulative_dollar_pnl += _dollar

                        self.logger.info(
                            f"[成交] L_{_side}@{_l_price} → X_{_hedge_side}@{_x_fill_price}  "
                            f"spread={_spread:.2f}bps fee={_fee:.2f}bps net={_net:+.2f}bps  "
                            f"${_dollar:+.4f}  hedge={_hedge_ms:.0f}ms  "
                            f"累积=${self.cumulative_dollar_pnl:+.4f}")

                        self._trade_seq += 1
                        self._csv_row([
                            datetime.now(_TZ_CN).isoformat(),
                            f"v23_{self._trade_seq:04d}",
                            _side,
                            str(_l_price), str(_x_fill_price), str(_qty),
                            f"{_spread:.2f}", f"{_fee:.2f}", f"{_net:.2f}",
                            f"{_hedge_ms:.0f}",
                            f"{self.cumulative_net_bps:.2f}",
                            "", "", "", "",
                        ])
                        self._consecutive_hedge_fails = 0

                        # V23: 开仓完成后异步快照余额(作为下次平仓的基准)
                        asyncio.create_task(self._v23_record_pre_trade_balance())
                    else:
                        self.logger.error("[HEDGING] 对冲失败! 进入修复")
                        self._consecutive_hedge_fails += 1
                        if self._consecutive_hedge_fails >= 3:
                            self._circuit_breaker_until = time.time() + 60
                            self.logger.error("[熔断] 暂停60秒")
                        self.state = State.REPAIR
                        continue

                    await self._cancel_all_lighter()
                    self._reset_cycle()
                    self.state = State.IDLE

                # ━━━━━━ STATE: REPAIR ━━━━━━
                elif self.state == State.REPAIR:
                    self.logger.info("[REPAIR] 对账并修复...")
                    await self._cancel_all_lighter()
                    await self._reconcile()
                    if abs(self.lighter_position + self.extended_position) > self.order_size * Decimal("0.5"):
                        await self._emergency_flatten()
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
        self._l_buy_client_id = None
        self._l_sell_client_id = None
        self._l_buy_order_idx = None
        self._l_sell_order_idx = None
        # V22 reset
        self._fill_ext_bid = None
        self._fill_ext_ask = None
        self._fill_ts = 0.0
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
    p = argparse.ArgumentParser(description="V23 即时对冲+余额校验")
    p.add_argument("-s", "--symbol", default="BTC")
    p.add_argument("--size", type=str, default="0.001")
    p.add_argument("--max-position", type=str, default="0.01")
    p.add_argument("--open-spread", type=float, default=4.5,
                   help="开仓价差阈值bps: Lighter挂单距Extended的距离 (默认4.5)")
    p.add_argument("--close-spread", type=float, default=-0.5,
                   help="平仓价差阈值bps: 价差收敛到此值时平仓 (默认-0.5)")
    p.add_argument("--max-order-age", type=float, default=60.0,
                   help="最大挂单存活秒数 (默认60)")
    p.add_argument("--spread-buffer", type=float, default=3.0,
                   help="内部价差缓冲bps: 补偿延迟滑点 (默认3.0, 内部挂单按open-spread+buffer)")
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
    )
    bot.flatten_on_start = args.flatten_on_start

    fee_per = float(FEE_EXTENDED_TAKER) * 10000
    rt_fee = fee_per * 2
    min_profit = args.open_spread - abs(args.close_spread) - rt_fee
    print(f"[V23] 即时对冲+余额校验")
    print(f"  {args.symbol} size={args.size} max_pos={args.max_position}")
    print(f"  开仓价差>{args.open_spread}bps(内部+{args.spread_buffer}bps缓冲)")
    print(f"  平仓价差<{args.close_spread}bps")
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
