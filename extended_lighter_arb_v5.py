#!/usr/bin/env python3
"""
Extended + Lighter 价差收敛套利 V5（Strategy B: Always-On Lighter Maker + Extended Taker）

策略：
  持续在 Lighter 维护 POST_ONLY maker 挂单，价格基于 Extended BBO 跨交易所价差计算。
  当挂单被成交 → 立刻在 Extended IOC taker 对冲（VPS延迟~10-23ms）。
  持仓后在 Lighter 维护平仓 maker 挂单，被成交后 Extended 对冲平仓。

与 V4 的区别：
  V4 (Strategy A): 检测到价差 → 挂单 → 等成交 → 对冲（~3-12秒）
  V5 (Strategy B): 持续挂单 → 被动成交 → 即时对冲（~30-50ms）

费用: ~5.8 bps 往返 (Lighter maker 0.4bps x2 + Extended taker 2.5bps x2)

用法:
  python extended_lighter_arb_v5.py --symbol ETH --size 0.1 --target-spread 5
  python extended_lighter_arb_v5.py --symbol ETH --dry-run
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

sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.extended import ExtendedClient
from feeds.lighter_feed import LighterFeed
from feeds.existing_feeds import ExtendedFeed
from x10.perpetual.orders import TimeInForce, OrderSide


# ═══════════════════════════════════════════════════════
#  States & data types
# ═══════════════════════════════════════════════════════

class State(str, Enum):
    QUOTING = "QUOTING"
    HEDGING = "HEDGING"
    IN_POSITION = "IN_POSITION"
    CLOSING_HEDGE = "CLOSING_HEDGE"


@dataclass
class PendingMaker:
    client_order_index: int
    order_index: Optional[int]
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


class _Config:
    def __init__(self, d: dict):
        for k, v in d.items():
            setattr(self, k, v)


# ═══════════════════════════════════════════════════════
#  Main Bot
# ═══════════════════════════════════════════════════════

class AlwaysOnMakerBot:

    LIGHTER_MAKER_FEE_BPS = 0.4
    EXTENDED_TAKER_FEE_BPS = 2.5
    ONE_LEG_FEE_BPS = LIGHTER_MAKER_FEE_BPS + EXTENDED_TAKER_FEE_BPS   # 2.9
    ROUND_TRIP_FEE_BPS = ONE_LEG_FEE_BPS * 2                           # 5.8

    def __init__(
        self,
        symbol: str,
        order_size: Decimal,
        max_position: Decimal,
        target_spread: float,
        close_bps: float,
        max_hold_sec: float,
        interval: float,
        reprice_interval: float,
        dry_run: bool,
    ):
        self.symbol = symbol.upper()
        self.order_size = order_size
        self.max_position = max_position
        self.target_spread = target_spread
        self.close_bps = close_bps
        self.max_hold_sec = max_hold_sec
        self.interval = interval
        self.reprice_interval = reprice_interval
        self.dry_run = dry_run

        self.state = State.QUOTING
        self.stop_flag = False

        self.lighter_position = Decimal("0")
        self.extended_position = Decimal("0")
        self.position_opened_at: Optional[float] = None
        self.position_direction: Optional[str] = None

        self._pending: Optional[PendingMaker] = None
        self._stale_fills: List[dict] = []
        self._zombie_cois: set = set()

        self._lighter_ws_task: Optional[asyncio.Task] = None
        self._lighter_fill_event = asyncio.Event()
        self._hedge_lock = asyncio.Lock()
        self._nonce_counter: int = 0
        self._modify_grace_until: float = 0.0
        self._liquidation_detected: bool = False
        self._processed_order_indices: List[str] = []
        self._modify_fail_count: int = 0
        self._modify_disabled: bool = False

        self._post_close_cooldown: float = 30.0
        self._last_close_time: float = 0.0
        self._consecutive_losses: int = 0
        self._loss_breaker_until: float = 0.0
        self._loss_breaker_threshold: float = -15.0
        self._loss_breaker_duration: float = 300.0
        self._close_profit_buffer_bps: float = 1.0
        self._max_stale_retries: int = 5
        self._stale_retry_counts: dict = {}
        self._last_open_direction: Optional[str] = None
        self._direction_lock_until: float = 0.0
        self._min_lighter_spread_ticks: int = 2

        self._x_ws_bbo_ts: float = 0.0
        self._x_ws_last_ob_id: int = 0
        self.STALE_THRESHOLD_SEC: float = 5.0

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
        self._last_reprice: float = 0.0
        self._maker_pulled: bool = False
        self._tx_count_window: List[float] = []
        self._tx_backoff_until: float = 0.0
        self._volume_quota_remaining: Optional[int] = None

        os.makedirs("logs", exist_ok=True)
        self.csv_path = f"logs/arb_v5_{self.symbol}_trades.csv"
        self.log_path = f"logs/arb_v5_{self.symbol}.log"
        self._init_csv()
        self.logger = self._init_logger()

    # ═══════════════════════════════════════════════
    #  Logging / CSV
    # ═══════════════════════════════════════════════

    def _init_logger(self) -> logging.Logger:
        lg = logging.getLogger(f"arb_v5_{self.symbol}")
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
                    "spread_bps", "hedge_ms",
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
        """Check if error is a nonce error. The library's acknowledge_failure only
        decrements the nonce by 1, which doesn't fix a large desync. We call
        hard_refresh_nonce to re-fetch the correct nonce from the API."""
        err_str = str(error).lower()
        if "nonce" not in err_str:
            self._nonce_error_count = 0
            return False
        self._nonce_error_count += 1
        try:
            self.lighter_client.nonce_manager.hard_refresh_nonce(self.api_key_index)
            self.logger.info(
                f"[Lighter] nonce 错误 (第{self._nonce_error_count}次), "
                f"已从API刷新nonce")
        except Exception as e:
            self.logger.error(f"[Lighter] nonce 刷新失败: {e}")
        self._tx_backoff_until = max(self._tx_backoff_until, time.time() + 1.0)
        if self._nonce_error_count >= 5:
            self.logger.warning(
                f"[Lighter] nonce 连续失败{self._nonce_error_count}次, 重建 SignerClient")
            try:
                self._init_lighter_signer()
                self._tx_backoff_until = time.time() + 3.0
            except Exception as e:
                self.logger.error(f"[Lighter] 重建 SignerClient 失败: {e}")
                self._tx_backoff_until = time.time() + 10.0
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

    def _on_extended_order_event(self, event: dict):
        order_id = str(event.get("order_id", ""))
        status = event.get("status", "")
        if self._x_ioc_pending_id and order_id == self._x_ioc_pending_id:
            if status == "FILLED":
                self._x_ioc_fill_qty = Decimal(str(event.get("filled_size", "0")))
                self._x_ioc_confirmed.set()
            elif status in ("CANCELED", "EXPIRED"):
                self._x_ioc_fill_qty = Decimal(str(event.get("filled_size", "0")))
                self._x_ioc_confirmed.set()
                self.logger.warning(
                    f"[Extended] IOC {order_id} 状态={status} "
                    f"已成交={self._x_ioc_fill_qty}")

    async def _get_extended_contract_info(self):
        cid, tick = await self.extended_client.get_contract_attributes()
        self.extended_contract_id = cid
        self.extended_tick_size = tick
        if hasattr(self.extended_client, "min_order_size"):
            self.extended_min_order_size = self.extended_client.min_order_size
        self.logger.info(f"Extended contract: {cid}  tick={tick}")

    async def _warmup_extended(self):
        """Pre-warm Extended HTTP session by placing a far-from-market IOC."""
        self.logger.info("[预热] Extended 连接预热中...")
        expire = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        try:
            t0 = time.perf_counter()
            await self.extended_client.perpetual_trading_client.place_order(
                market_name=self.extended_contract_id,
                amount_of_synthetic=Decimal("0.1"),
                price=Decimal("1.00"),
                side=OrderSide.BUY,
                post_only=False,
                time_in_force=TimeInForce.IOC,
                expire_time=expire,
            )
            ms = (time.perf_counter() - t0) * 1000
            self.logger.info(f"[预热] Extended 首次下单 {ms:.0f}ms (市场数据+连接已建立)")
            t0 = time.perf_counter()
            await self.extended_client.perpetual_trading_client.place_order(
                market_name=self.extended_contract_id,
                amount_of_synthetic=Decimal("0.1"),
                price=Decimal("1.00"),
                side=OrderSide.BUY,
                post_only=False,
                time_in_force=TimeInForce.IOC,
                expire_time=expire,
            )
            ms = (time.perf_counter() - t0) * 1000
            self.logger.info(f"[预热] Extended 预热后延迟 {ms:.0f}ms")
        except Exception as e:
            self.logger.warning(f"[预热] Extended 预热异常 (可忽略): {e}")

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
            elif (self.extended_client._tasks
                  and any(not t.done() for t in self.extended_client._tasks)):
                self._x_ws_bbo_ts = time.time()
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
        url = "wss://mainnet.zklighter.elliot.ai/stream"
        while not self.stop_flag:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    auth_token, err = self.lighter_client.create_auth_token_with_expiry(
                        api_key_index=self.api_key_index)
                    if err is not None:
                        self.logger.error(f"Lighter auth token 失败: {err}")
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
                            if data.get("type") == "ping":
                                await ws.send(json.dumps({"type": "pong"}))
                                continue
                            if data.get("type") == "update/account_orders":
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
                            self.logger.error(f"Lighter WS 错误: {e}")
                            break
            except Exception as e:
                self.logger.error(f"Lighter WS 连接失败: {e}")
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

    def _record_processed_order(self, order_idx: str):
        self._processed_order_indices.append(order_idx)
        if len(self._processed_order_indices) > 200:
            self._processed_order_indices = self._processed_order_indices[-100:]

    def _on_lighter_fill(self, od: dict):
        try:
            filled_base = Decimal(od.get("filled_base_amount", "0"))
            filled_quote = Decimal(od.get("filled_quote_amount", "0"))
            avg_price = filled_quote / filled_base if filled_base > 0 else Decimal("0")
            is_ask = od.get("is_ask", False)
            side_str = "sell" if is_ask else "buy"
            order_type = od.get("type", "")
            fill_order_idx = str(od.get("order_index", ""))

            if "liquidat" in str(order_type).lower() or "forced" in str(order_type).lower():
                self.logger.error(
                    f"[强制平仓] Lighter 账户被强制平仓! {side_str} {filled_base} @ {avg_price}")
                self.logger.error("[强制平仓] 需要立即关闭 Extended 对冲仓位!")
                self._liquidation_detected = True
                return

            if not self._is_current_order(od):
                if fill_order_idx and fill_order_idx in self._processed_order_indices:
                    self.logger.debug(
                        f"[Lighter] 已处理订单的重复fill事件: idx={fill_order_idx}, 忽略")
                    return
                existing_indices = {sf.get("order_index") for sf in self._stale_fills}
                if fill_order_idx and fill_order_idx in existing_indices:
                    self.logger.debug(
                        f"[Lighter] stale队列已有该订单: idx={fill_order_idx}, 忽略")
                    return
                self.logger.warning(
                    f"[Lighter] 非当前订单成交: {side_str} {filled_base} @ {avg_price} "
                    f"idx={fill_order_idx} → 修复队列")
                self._stale_fills.append({
                    "side": side_str, "size": filled_base, "price": avg_price,
                    "order_index": fill_order_idx,
                })
                return

            if self._pending is None:
                return
            old_filled = self._pending.filled_qty
            if filled_base <= old_filled:
                return

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
                asyncio.ensure_future(self._fast_hedge())

        except Exception as e:
            self.logger.error(f"处理 Lighter 成交出错: {e}\n{traceback.format_exc()}")

    def _on_lighter_order_open(self, od: dict):
        try:
            client_id = od.get("client_order_id")
            if client_id is not None and int(client_id) in self._zombie_cois:
                new_idx = od.get("order_index")
                self._zombie_cois.discard(int(client_id))
                if new_idx is not None:
                    self.logger.warning(
                        f"[僵尸检测] 已取消的订单重新上链! "
                        f"coi={client_id} idx={new_idx}, 立即撤单")
                    asyncio.ensure_future(self._cancel_zombie_order(new_idx))
                return
            if self._pending is None:
                return
            if client_id is not None and int(client_id) == self._pending.client_order_index:
                new_idx = od.get("order_index")
                if new_idx is not None and new_idx != self._pending.order_index:
                    self._pending.order_index = new_idx
                    self.logger.info(f"[Lighter] 挂单上链 order_index={new_idx}")
                return
            if (time.time() < self._modify_grace_until
                    and self._pending.order_index is None):
                new_idx = od.get("order_index")
                is_ask = od.get("is_ask", False)
                ws_side = "sell" if is_ask else "buy"
                if ws_side == self._pending.side and new_idx is not None:
                    self._pending.order_index = new_idx
                    self.logger.info(
                        f"[Lighter] 侧匹配确认 order_index={new_idx} "
                        f"(client_id 未匹配, 保护窗口内)")
            elif client_id is not None:
                self.logger.debug(
                    f"[Lighter] 忽略非当前订单的 open 事件: "
                    f"ws_client_id={client_id} pending_coi={self._pending.client_order_index}")
        except Exception as e:
            self.logger.error(f"处理 Lighter 订单上链出错: {e}")

    def _on_lighter_order_canceled(self, od: dict):
        try:
            filled_base = Decimal(od.get("filled_base_amount", "0"))
            if filled_base > 0:
                self._on_lighter_fill(od)
                return
            if not self._is_current_order(od):
                return
            if time.time() < self._modify_grace_until:
                self.logger.info("[Lighter] modify/cancel 保护窗口内, 忽略 cancel 事件")
                return
            self.logger.info("[Lighter] 挂单已取消 (无成交)")
            if self._pending is not None and self.state in (State.QUOTING, State.IN_POSITION):
                self._pending = None
        except Exception as e:
            self.logger.error(f"处理 Lighter 取消出错: {e}")

    # ═══════════════════════════════════════════════
    #  Fast Hedge (bypass main loop for min latency)
    # ═══════════════════════════════════════════════

    async def _fast_hedge(self):
        """Direct hedge from WS fill handler — bypasses main loop overhead."""
        async with self._hedge_lock:
            if self._pending is None or self._pending.hedge_qty_needed <= 0:
                return

            hedge_qty = self._pending.hedge_qty_needed
            phase = self._pending.phase
            direction = self._pending.direction

            if phase == "open":
                hedge_side = "sell" if direction == "long_L_short_X" else "buy"
            else:
                l_side = self._pending.side
                hedge_side = "sell" if l_side == "buy" else "buy"

            x_bid, x_ask = self._get_extended_ws_bbo()
            x_ref = x_bid if hedge_side == "sell" else x_ask
            if x_ref is None:
                return

            t0 = time.time()
            actual_fill = await self._place_extended_ioc(hedge_side, hedge_qty, x_ref)
            hedge_ms = (time.time() - t0) * 1000

            if actual_fill is None:
                self.logger.warning(f"[FAST_HEDGE] IOC 失败({hedge_ms:.0f}ms), 回退主循环")
                return

            self._pending.hedged_qty += actual_fill
            self._pending.hedge_qty_needed = hedge_qty - actual_fill
            l_fill_price = self._pending.avg_price or self._pending.price

            ls = self.lighter_feed.snapshot if self.lighter_feed else None
            l_mid = Decimal(str(ls.mid)) if ls and ls.mid else l_fill_price
            x_mid_val = (x_bid + x_ask) / 2 if x_bid and x_ask else x_ref
            mid_spread_bps = float((x_mid_val - l_mid) / l_mid * 10000) if l_mid > 0 else 0.0

            if phase == "open":
                self._open_l_refs.append((l_fill_price, actual_fill))
                self._open_x_refs.append((x_ref, actual_fill))
                if direction == "long_L_short_X":
                    self.lighter_position += actual_fill
                    self.extended_position -= actual_fill
                else:
                    self.lighter_position -= actual_fill
                    self.extended_position += actual_fill

                self.logger.info(
                    f"[FAST_HEDGE] {hedge_ms:.0f}ms fill={actual_fill}/{hedge_qty} "
                    f"L_pos={self.lighter_position} X_pos={self.extended_position}")

                if self._pending.hedged_qty >= self._pending.filled_qty:
                    if self.position_opened_at is None:
                        self.position_opened_at = time.time()
                    self.position_direction = direction
                    self._csv_row([
                        datetime.now(timezone.utc).isoformat(),
                        "OPEN", direction,
                        self._pending.side, f"{l_fill_price:.2f}",
                        str(actual_fill),
                        hedge_side, f"{x_ref:.2f}", str(actual_fill),
                        f"{mid_spread_bps:.2f}", f"{hedge_ms:.0f}", "", "",
                    ])
                    if self._pending.order_index is not None:
                        self._record_processed_order(str(self._pending.order_index))
                    self._pending = None
                    self.state = State.IN_POSITION
                    await self._do_cancel_all()
                    await asyncio.sleep(0.5)
            else:
                l_side = self._pending.side
                if l_side == "buy":
                    self.lighter_position += actual_fill
                    self.extended_position -= actual_fill
                else:
                    self.lighter_position -= actual_fill
                    self.extended_position += actual_fill

                net_bps = self._log_ref_pnl(
                    l_fill_price, x_ref,
                    self._pending.side, hedge_side)
                self.cumulative_net_bps += net_bps

                self.logger.info(
                    f"[FAST_CLOSE_HEDGE] {hedge_ms:.0f}ms fill={actual_fill}/{hedge_qty} "
                    f"净利={net_bps:+.2f}bps 累积={self.cumulative_net_bps:+.2f}bps")
                self._csv_row([
                    datetime.now(timezone.utc).isoformat(),
                    "CLOSE", "convergence",
                    self._pending.side, f"{l_fill_price:.2f}",
                    str(actual_fill),
                    hedge_side, f"{x_ref:.2f}", str(actual_fill),
                    f"{mid_spread_bps:.2f}", f"{hedge_ms:.0f}",
                    f"{net_bps:.2f}", f"{self.cumulative_net_bps:.2f}",
                ])

                if self._pending.hedged_qty >= self._pending.filled_qty:
                    if self._pending.filled_qty >= self._pending.qty_target:
                        if self._pending.order_index is not None:
                            self._record_processed_order(str(self._pending.order_index))
                        self._pending = None
                        pos_remaining = abs(self.lighter_position)
                        if pos_remaining < self.order_size * Decimal("0.01"):
                            self._last_close_time = time.time()
                            if net_bps < 0:
                                self._consecutive_losses += 1
                            else:
                                self._consecutive_losses = 0
                            if (self._consecutive_losses >= 3
                                    or self.cumulative_net_bps < self._loss_breaker_threshold):
                                self._loss_breaker_until = time.time() + self._loss_breaker_duration
                                self.logger.error(
                                    f"[熔断] 触发亏损熔断! "
                                    f"连亏={self._consecutive_losses} "
                                    f"累积={self.cumulative_net_bps:+.2f}bps, "
                                    f"暂停{self._loss_breaker_duration:.0f}s")
                            self._reset_position_state()
                            self.state = State.QUOTING
                            await self._do_cancel_all()
                        else:
                            self.state = State.IN_POSITION
                    else:
                        self.logger.info(
                            f"[CLOSE] 部分成交 {self._pending.filled_qty}/"
                            f"{self._pending.qty_target}, 继续跟踪剩余挂单")
                        self.state = State.IN_POSITION
                else:
                    self.state = State.IN_POSITION

    # ═══════════════════════════════════════════════
    #  Maker Pricing & Lifecycle
    # ═══════════════════════════════════════════════

    def _check_tx_rate(self) -> bool:
        """Return True if we're under the 40 tx/min limit, False to skip."""
        now = time.time()
        if now < self._tx_backoff_until:
            return False
        cutoff = now - 60
        self._tx_count_window = [t for t in self._tx_count_window if t > cutoff]
        if len(self._tx_count_window) >= 38:
            return False
        return True

    def _record_tx(self):
        self._tx_count_window.append(time.time())

    def _parse_quota(self, result) -> Optional[int]:
        """Extract volume quota remaining from Lighter API response."""
        try:
            txt = str(result)
            if "volume quota remaining" in txt.lower():
                import re
                m = re.search(r"(\d+)\s+volume quota remaining", txt, re.IGNORECASE)
                if m:
                    q = int(m.group(1))
                    self._volume_quota_remaining = q
                    if q < 100:
                        self.logger.warning(f"[配额] 剩余配额低: {q}")
                    return q
        except Exception:
            pass
        return None

    def _handle_rate_limit_error(self, error) -> bool:
        """Check if error is rate-limit or persistent error; if so, set backoff. Returns True if should back off."""
        err_str = str(error).lower()
        if "429" in err_str or "rate limit" in err_str or "too many" in err_str:
            self._tx_backoff_until = time.time() + 5.0
            self.logger.warning(f"[限流] Lighter 速率限制，暂停 5s: {error}")
            return True
        if "quota" in err_str and ("exceeded" in err_str or "remaining" in err_str or "0 volume" in err_str):
            self._tx_backoff_until = time.time() + 15.0
            self.logger.warning(f"[配额] 配额不足，暂停 15s: {error}")
            return True
        if "margin" in err_str or "not enough" in err_str or "insufficient" in err_str:
            self._tx_backoff_until = time.time() + 60.0
            self._liquidation_detected = True
            self.logger.error(f"[保证金] 余额不足，触发清算检查: {error}")
            return True
        return False

    def _round_lighter_price(self, price: Decimal) -> Decimal:
        if self.lighter_tick > 0:
            return (price / self.lighter_tick).quantize(Decimal("1")) * self.lighter_tick
        return price

    def _calc_maker_target(
        self, x_bid: Decimal, x_ask: Decimal,
        l_bid: Decimal, l_ask: Decimal,
    ) -> Optional[Tuple[str, str, Decimal]]:
        """Calculate target maker side, direction and price.
        Opening offset uses ONE_LEG_FEE (2.9 bps) + target_spread, NOT round-trip.
        Close pricing separately covers the closing leg's fees.
        POST_ONLY constraints: buy < l_ask, sell > l_bid."""
        lighter_spread_ticks = (l_ask - l_bid) / self.lighter_tick
        if lighter_spread_ticks < self._min_lighter_spread_ticks:
            return None

        spread_bps = Decimal(str(self.target_spread))
        fee_bps = Decimal(str(self.ONE_LEG_FEE_BPS))

        buy_target = self._round_lighter_price(
            x_bid * (Decimal("1") - (spread_bps + fee_bps) / Decimal("10000")))
        sell_target = self._round_lighter_price(
            x_ask * (Decimal("1") + (spread_bps + fee_bps) / Decimal("10000")))

        buy_price = min(buy_target, l_ask - self.lighter_tick)
        sell_price = max(sell_target, l_bid + self.lighter_tick)

        buy_profit = x_bid - buy_price
        sell_profit = sell_price - x_ask

        min_profit = x_bid * Decimal(str(self.ONE_LEG_FEE_BPS * 0.5)) / Decimal("10000")
        if buy_profit < min_profit and sell_profit < min_profit:
            return None

        if buy_profit >= sell_profit and buy_price > 0 and buy_price < l_ask:
            return ("buy", "long_L_short_X", buy_price)
        elif sell_price > l_bid and sell_price > 0:
            return ("sell", "long_X_short_L", sell_price)
        return None

    def _calc_close_maker_target(
        self, x_bid: Decimal, x_ask: Decimal,
        l_bid: Decimal, l_ask: Decimal,
    ) -> Optional[Tuple[str, Decimal]]:
        """Calculate closing maker price with profitability constraint.
        Uses ROUND_TRIP_FEE because min_sell/max_buy computes total position P&L."""
        fees_pct = Decimal(str(self.ROUND_TRIP_FEE_BPS)) / Decimal("10000")

        profit_buffer = Decimal(str(self._close_profit_buffer_bps)) / Decimal("10000")

        if self.lighter_position > 0:
            close_side = "sell"
            min_sell = l_bid + self.lighter_tick
            if self._open_l_refs and self._open_x_refs:
                l_avg = sum(p * q for p, q in self._open_l_refs) / sum(q for _, q in self._open_l_refs)
                x_avg = sum(p * q for p, q in self._open_x_refs) / sum(q for _, q in self._open_x_refs)
                fees_abs = l_avg * fees_pct
                min_sell = l_avg - x_avg + x_ask + fees_abs + l_avg * profit_buffer

            spread_ticks = (l_ask - l_bid) / self.lighter_tick
            if spread_ticks > 1:
                price = l_ask - self.lighter_tick
            else:
                price = l_ask
            price = max(price, min_sell)
            price = max(price, l_bid + self.lighter_tick)
            return (close_side, self._round_lighter_price(price))

        elif self.lighter_position < 0:
            close_side = "buy"
            max_buy = l_ask - self.lighter_tick
            if self._open_l_refs and self._open_x_refs:
                l_avg = sum(p * q for p, q in self._open_l_refs) / sum(q for _, q in self._open_l_refs)
                x_avg = sum(p * q for p, q in self._open_x_refs) / sum(q for _, q in self._open_x_refs)
                fees_abs = l_avg * fees_pct
                max_buy = l_avg + x_bid - x_avg - fees_abs - l_avg * profit_buffer

            spread_ticks = (l_ask - l_bid) / self.lighter_tick
            if spread_ticks > 1:
                price = l_bid + self.lighter_tick
            else:
                price = l_bid
            price = min(price, max_buy)
            price = min(price, l_ask - self.lighter_tick)
            if price <= 0:
                return None
            return (close_side, self._round_lighter_price(price))

        return None

    async def _place_lighter_maker_at(self, side: str, qty: Decimal,
                                       price: Decimal) -> Optional[int]:
        """Place POST_ONLY maker on Lighter at a specific price."""
        if not self._check_tx_rate():
            self.logger.debug("[Lighter] 跳过下单: 接近速率限制")
            return None
        self._nonce_counter += 1
        client_order_index = int(time.time() * 1000) * 100 + (self._nonce_counter % 100)
        is_ask = side.lower() == "sell"
        try:
            result, _, error = await self.lighter_client.create_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=int(qty * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_POST_ONLY,
                reduce_only=False,
                trigger_price=0,
            )
            self._record_tx()
            self._parse_quota(result)
            if error is not None:
                if self._check_nonce_error(error):
                    self.logger.warning(f"[Lighter] maker nonce 错误: {error}")
                    return None
                if self._handle_rate_limit_error(error):
                    return None
                self.logger.error(f"[Lighter] maker 下单失败: {error}")
                return None
            self.logger.info(
                f"[Lighter] maker {side} {qty} @ {price}  id={client_order_index}  "
                f"L_pos={self.lighter_position} X_pos={self.extended_position}")
            return client_order_index
        except Exception as e:
            self._handle_rate_limit_error(e)
            self.logger.error(f"[Lighter] maker 异常: {e}")
            return None

    async def _cancel_pending_maker(self) -> bool:
        """Cancel pending maker. Returns True if confirmed cancelled."""
        if not self._pending:
            return True
        order_idx = self._pending.order_index
        if order_idx is not None:
            try:
                _, _, error = await self.lighter_client.cancel_order(
                    market_index=self.lighter_market_index, order_index=order_idx)
                if error is None:
                    self._nonce_error_count = 0
                    self.logger.info(f"[Lighter] 撤单成功 idx={order_idx}")
                    return True
                if self._check_nonce_error(error):
                    self.logger.error(f"[Lighter] 撤单 nonce 错误: {error}")
                else:
                    self.logger.error(f"[Lighter] 撤单错误: {error}")
            except Exception as e:
                self._check_nonce_error(e)
                self.logger.error(f"[Lighter] 撤单异常: {e}")
        self._zombie_cois.add(self._pending.client_order_index)
        self.logger.info(
            f"[僵尸追踪] 添加 coi={self._pending.client_order_index} "
            f"(order_index={'未知' if order_idx is None else order_idx})")
        ok = await self._do_cancel_all()
        if not ok:
            self.logger.error("[撤单] cancel_order 和 cancel_all 均失败，不能下新单")
        return ok

    async def _do_cancel_all(self) -> bool:
        """Execute cancel_all with nonce error recovery. Returns True on success."""
        self.logger.warning("[Lighter] 使用 cancel_all 兜底")
        for attempt in range(3):
            try:
                future_ts_ms = int((time.time() + 300) * 1000)
                _, _, error = await self.lighter_client.cancel_all_orders(
                    time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                    timestamp_ms=future_ts_ms)
                if error is None:
                    self._nonce_error_count = 0
                    return True
                if self._check_nonce_error(error):
                    self.logger.warning(
                        f"[Lighter] cancel_all nonce 错误 ({attempt+1}/3), 重试")
                    await asyncio.sleep(2.0)
                    continue
                self.logger.error(f"[Lighter] cancel_all 错误: {error}")
                return False
            except Exception as e:
                if self._check_nonce_error(e):
                    self.logger.warning(
                        f"[Lighter] cancel_all 异常 ({attempt+1}/3): {e}")
                    await asyncio.sleep(2.0)
                    continue
                self.logger.error(f"[Lighter] cancel_all 异常: {e}")
                return False
        self.logger.error("[Lighter] cancel_all 3次重试均失败!")
        self._tx_backoff_until = time.time() + 10.0
        return False

    async def _cancel_zombie_order(self, order_index):
        """Cancel a zombie order that was supposed to be cancelled but reappeared on WS."""
        try:
            _, _, error = await self.lighter_client.cancel_order(
                market_index=self.lighter_market_index, order_index=order_index)
            if error is None:
                self._nonce_error_count = 0
                self.logger.info(f"[僵尸撤单] 成功 idx={order_index}")
            else:
                self._check_nonce_error(error)
                self.logger.error(f"[僵尸撤单] 失败: {error}, 尝试 cancel_all")
                await self._do_cancel_all()
        except Exception as e:
            self._check_nonce_error(e)
            self.logger.error(f"[僵尸撤单] 异常: {e}, 尝试 cancel_all")
            await self._do_cancel_all()

    async def _cancel_all_makers(self):
        """Pull ALL Lighter orders off the book (safety)."""
        if self._pending and self._pending.order_index is not None:
            self._record_processed_order(str(self._pending.order_index))
        if self.lighter_client:
            await self._do_cancel_all()
        await asyncio.sleep(0.5)
        self._pending = None
        self._zombie_cois.clear()

    async def _update_maker(
        self, target_side: str, target_direction: str,
        target_price: Decimal, qty: Decimal, phase: str,
    ) -> bool:
        """Ensure a maker order is live at the target price. Returns True if order is active."""
        now = time.time()

        if self._pending is not None:
            same_side = self._pending.side == target_side
            price_diff = abs(self._pending.price - target_price)
            needs_reprice = price_diff >= self.lighter_tick

            if same_side and not needs_reprice:
                return True

            if now - self._last_reprice < self.reprice_interval:
                return self._pending is not None

            if (same_side and self._pending.order_index is not None
                    and not self._modify_disabled):
                if not self._check_tx_rate():
                    return self._pending is not None
                saved_pending = self._pending
                saved_order_idx = self._pending.order_index
                try:
                    self._modify_grace_until = time.time() + 2.0
                    result, _, error = await self.lighter_client.modify_order(
                        market_index=self.lighter_market_index,
                        order_index=saved_order_idx,
                        base_amount=int(qty * self.base_amount_multiplier),
                        price=int(target_price * self.price_multiplier),
                    )
                    self._record_tx()
                    self._parse_quota(result)
                    if self._pending is not saved_pending:
                        self.logger.warning(
                            f"[调价] modify 期间 _pending 已变更 (WS fill?), 保持当前状态")
                        return self._pending is not None
                    elif error is None:
                        self._modify_fail_count = 0
                        self._pending.price = target_price
                        self._pending.qty_target = qty
                        self._pending.placed_at = now
                        self._last_reprice = now
                        return True
                    elif self._handle_rate_limit_error(error):
                        self._modify_grace_until = 0.0
                        return self._pending is not None
                    else:
                        self._modify_grace_until = 0.0
                        self._check_nonce_error(error)
                        self._modify_fail_count += 1
                        if self._modify_fail_count >= 3:
                            self._modify_disabled = True
                            self.logger.warning(
                                f"[调价] modify_order 连续失败{self._modify_fail_count}次,"
                                f" 自动禁用, 改用 cancel+recreate")
                        else:
                            self.logger.warning(f"[调价] modify_order 失败: {error}")
                except Exception as e:
                    self._modify_grace_until = 0.0
                    self._modify_fail_count += 1
                    self._check_nonce_error(e)
                    self._handle_rate_limit_error(e)
                    self.logger.warning(f"[调价] modify_order 异常: {e}")

            had_order_index = self._pending.order_index is not None
            self.logger.info(
                f"[调价] {self._pending.side}@{self._pending.price} → "
                f"{target_side}@{target_price}  diff={price_diff}")
            self._modify_grace_until = time.time() + 1.0
            cancel_ok = await self._cancel_pending_maker()
            if not cancel_ok:
                self.logger.error("[调价] 撤单失败，禁止下新单防止重复挂单")
                return self._pending is not None
            if time.time() < self._tx_backoff_until:
                self.logger.warning("[调价] nonce恢复中, 暂不下新单")
                if self._pending and self._pending.filled_qty <= self._pending.hedged_qty:
                    self._pending = None
                return self._pending is not None
            await asyncio.sleep(0.3 if not had_order_index else 0.05)
            if self._pending is not None:
                if self._pending.filled_qty > self._pending.hedged_qty:
                    return True
                self._pending = None

        coi = await self._place_lighter_maker_at(target_side, qty, target_price)
        if coi is not None:
            self._pending = PendingMaker(
                client_order_index=coi,
                order_index=None,
                side=target_side,
                direction=target_direction,
                qty_target=qty,
                price=target_price,
                placed_at=now,
                phase=phase,
            )
            self._lighter_fill_event.clear()
            self._last_reprice = now
            return True

        return False

    # ═══════════════════════════════════════════════
    #  Extended Taker (IOC) — reused from V4
    # ═══════════════════════════════════════════════

    async def _place_extended_ioc(self, side: str, qty: Decimal, ref_price: Decimal,
                                  max_retries: int = 2) -> Optional[Decimal]:
        """Returns actual filled quantity, or None if completely failed."""
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
                    quantity=qty_rounded, side=side, aggressive_price=aggressive)
                if result.success:
                    self._x_ioc_pending_id = str(result.order_id)
                    self.logger.info(
                        f"[Extended] IOC {side} {qty_rounded} @ ~{aggressive:.2f} id={result.order_id}")
                    try:
                        await asyncio.wait_for(self._x_ioc_confirmed.wait(), timeout=3.0)
                    except asyncio.TimeoutError:
                        self.logger.warning("[Extended] IOC 确认超时(3s)")
                    actual_fill = self._x_ioc_fill_qty if self._x_ioc_fill_qty > 0 else qty_rounded
                    if actual_fill < qty_rounded:
                        self.logger.warning(
                            f"[Extended] IOC 部分成交: {actual_fill}/{qty_rounded}")
                    self._x_ioc_pending_id = None
                    return actual_fill
                self.logger.error(
                    f"[Extended] IOC 失败 ({attempt+1}/{max_retries}): {result.error_message}")
            except Exception as e:
                self.logger.error(f"[Extended] IOC 异常 ({attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(0.3)
        self._x_ioc_pending_id = None
        return None

    # ═══════════════════════════════════════════════
    #  Lighter Taker (emergency only)
    # ═══════════════════════════════════════════════

    async def _place_lighter_taker(self, side: str, qty: Decimal, ref_price: Decimal) -> bool:
        is_ask = side.lower() == "sell"
        price = self._round_lighter_price(
            ref_price * (Decimal("0.995") if is_ask else Decimal("1.005")))
        self._nonce_counter += 1
        coi = int(time.time() * 1000) * 100 + (self._nonce_counter % 100)
        try:
            _, _, error = await self.lighter_client.create_order(
                market_index=self.lighter_market_index, client_order_index=coi,
                base_amount=int(qty * self.base_amount_multiplier),
                price=int(price * self.price_multiplier), is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                order_expiry=self.lighter_client.DEFAULT_IOC_EXPIRY,
                reduce_only=False, trigger_price=0)
            if error:
                self._check_nonce_error(error)
                self.logger.error(f"[Lighter] taker 失败: {error}")
                return False
            self._nonce_error_count = 0
            self.logger.info(f"[Lighter] taker {side} {qty} ref={ref_price:.2f}")
            return True
        except Exception as e:
            self._check_nonce_error(e)
            self.logger.error(f"[Lighter] taker 异常: {e}")
            return False

    # ═══════════════════════════════════════════════
    #  Order Verification (API check)
    # ═══════════════════════════════════════════════

    async def _refresh_order_index(self):
        """After modify_order, fetch the real order_index from API and update _pending."""
        if self._pending is None:
            return
        try:
            auth_token, err = self.lighter_client.create_auth_token_with_expiry(
                api_key_index=self.api_key_index)
            if err:
                return
            url = f"{self.lighter_base_url}/api/v1/accountActiveOrders"
            params = {"account_index": self.account_index,
                      "market_id": self.lighter_market_index, "auth": auth_token}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params,
                                       headers={"accept": "application/json"},
                                       timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    orders = data.get("orders", [])
                    if orders and self._pending:
                        new_idx = orders[0].get("order_index")
                        if new_idx and str(new_idx) != str(self._pending.order_index):
                            old = self._pending.order_index
                            self._pending.order_index = new_idx
                            self.logger.info(
                                f"[验证] API 刷新 order_index: {old} → {new_idx}")
                    elif not orders and self._pending:
                        self.logger.warning("[验证] modify 后无活跃订单, 清除 _pending")
                        self._pending = None
        except Exception as e:
            self.logger.error(f"[验证] refresh_order_index 失败: {e}")

    async def _verify_lighter_order_exists(self) -> bool:
        """Check via REST API if the current pending order actually exists on Lighter."""
        if self._pending is None or self._pending.order_index is None:
            return False
        try:
            auth_token, err = self.lighter_client.create_auth_token_with_expiry(
                api_key_index=self.api_key_index)
            if err is not None:
                self.logger.error(f"[验证] 生成 auth token 失败: {err}")
                return True
        except Exception as e:
            self.logger.error(f"[验证] auth token 异常: {e}")
            return True

        url = f"{self.lighter_base_url}/api/v1/accountActiveOrders"
        params = {
            "account_index": self.account_index,
            "market_id": self.lighter_market_index,
            "auth": auth_token,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={"accept": "application/json"},
                                       params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    orders = data.get("orders", [])
                    self.logger.debug(
                        f"[验证] API 返回 code={data.get('code')} "
                        f"订单数={len(orders)}")
                    target_idx = str(self._pending.order_index)
                    for o in orders:
                        if str(o.get("order_index", "")) == target_idx:
                            return True
                    if orders and len(orders) == 1 and self._pending:
                        found = orders[0]
                        new_idx = found.get("order_index")
                        found_price = found.get("price", "")
                        expected_price = str(int(self._pending.price * self.price_multiplier))
                        if new_idx and found_price == expected_price:
                            self.logger.info(
                                f"[验证] 认领活跃订单: {target_idx} → {new_idx}"
                                f" (modify 后 order_index 已变更)")
                            self._pending.order_index = new_idx
                            return True
                        elif new_idx:
                            self.logger.warning(
                                f"[验证] 活跃订单价格不匹配 "
                                f"(找到={found_price} 期望={expected_price}), "
                                f"可能是僵尸订单 → 撤销")
                            asyncio.ensure_future(
                                self._cancel_zombie_order(new_idx))
                            return False
                    if orders and len(orders) > 1:
                        self.logger.warning(
                            f"[验证] 发现 {len(orders)} 个活跃订单 (追踪={target_idx})"
                            f" → cancel_all 清理幽灵订单")
                        await self._do_cancel_all()
                        return False
                    if not orders:
                        self.logger.warning(
                            f"[验证] Lighter 无活跃订单 (脚本追踪: {self._pending.side}@{self._pending.price})")
                    return False
        except Exception as e:
            self.logger.error(f"[验证] 查询活跃订单失败: {e}")
            return True

    # ═══════════════════════════════════════════════
    #  Position Queries (async)
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
        state_before = self.state
        l = await self._get_lighter_position()
        x = await self._get_extended_position()
        if self.state != state_before:
            self.logger.warning(
                f"[对账] 对账期间状态变更 {state_before}->{self.state}, 跳过覆写")
            return True
        self.lighter_position = l
        self.extended_position = x
        net = l + x
        balanced = abs(net) < self.order_size * Decimal("0.5")
        if not balanced:
            self.logger.warning(f"[对账] 不平衡: L={l} X={x} net={net}")
        return balanced

    # ═══════════════════════════════════════════════
    #  Emergency flatten / Shutdown
    # ═══════════════════════════════════════════════

    async def _emergency_flatten(self):
        try:
            await self._cancel_all_makers()
            await asyncio.sleep(0.5)
            if abs(self.lighter_position) > 0:
                side = "sell" if self.lighter_position > 0 else "buy"
                ref = Decimal(str(self.lighter_feed.snapshot.mid or 0))
                if ref > 0:
                    await self._place_lighter_taker(side, abs(self.lighter_position), ref)
            if abs(self.extended_position) > 0:
                async with self._hedge_lock:
                    side = "sell" if self.extended_position > 0 else "buy"
                    x_bid, x_ask = self._get_extended_ws_bbo()
                    ref = x_bid if side == "sell" else x_ask
                    if ref:
                        await self._place_extended_ioc(side, abs(self.extended_position), ref)
            await asyncio.sleep(3)
            await self._reconcile_positions()
        except Exception as e:
            self.logger.error(f"紧急平仓异常: {e}")

    async def _shutdown(self):
        self.stop_flag = True
        self.logger.info("关闭中...")
        await self._cancel_all_makers()
        if self._lighter_ws_task and not self._lighter_ws_task.done():
            self._lighter_ws_task.cancel()
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
            self.logger.error(f"致命错误: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            if self._pending:
                try:
                    await self._cancel_pending_maker()
                    await asyncio.sleep(0.5)
                except Exception:
                    pass
            if not self.dry_run:
                await self._reconcile_positions()
            if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                self.logger.warning(
                    f"退出仍有仓位: L={self.lighter_position} X={self.extended_position}")
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
            f"启动 V5 Always-On-Maker 套利  {self.symbol}  "
            f"size={self.order_size} max_pos={self.max_position}  "
            f"目标价差≥{self.target_spread}bps  "
            f"平仓利润缓冲={self._close_profit_buffer_bps}bps  "
            f"调价间隔={self.reprice_interval}s  dry_run={self.dry_run}  "
            f"往返费用≈{self.ROUND_TRIP_FEE_BPS:.1f}bps")
        self.logger.info(
            f"[风控] 冷却={self._post_close_cooldown}s  "
            f"熔断阈值={self._loss_breaker_threshold}bps  "
            f"熔断暂停={self._loss_breaker_duration}s  "
            f"最大持仓={self.max_hold_sec}s")
        self.logger.info(
            f"[性能] uvloop={'已启用' if uvloop else '未安装'}  "
            f"fast_hedge=ON  modify_order={'OFF' if self._modify_disabled else 'ON'}  interval={self.interval}s")
        self.logger.info(
            "[提示] 建议使用 sudo nice -20 taskset -c 0 python ... 绑核+最高优先级运行")

        if not self.dry_run:
            self._init_lighter_signer()
            self._get_lighter_market_config()
            self._init_extended_client()
            await self.extended_client.connect()
            await self._get_extended_contract_info()
            await self._warmup_extended()
            self.logger.info("交易客户端就绪 (Extended 连接已预热)")

            self._lighter_ws_task = asyncio.create_task(self._lighter_account_ws_loop())
            self.logger.info("Lighter account_orders WS 已启动")
        else:
            self.logger.info("[DRY-RUN] 跳过交易客户端初始化")
            self._get_lighter_market_config()

        self.lighter_feed = LighterFeed(self.symbol)
        self.extended_feed = ExtendedFeed(self.symbol)
        asyncio.create_task(self.lighter_feed.connect())
        asyncio.create_task(self.extended_feed.connect())
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

        self.logger.info("[启动] 清理残留订单...")
        await self._do_cancel_all()
        await asyncio.sleep(3.0)
        self._tx_backoff_until = 0
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

                # ── Freshness check ──
                l_age = now - ls.timestamp if ls.timestamp > 0 else 999
                if not self.dry_run:
                    x_age = now - self._x_ws_bbo_ts if self._x_ws_bbo_ts > 0 else 999
                else:
                    es_ts = self.extended_feed.snapshot.timestamp if self.extended_feed else 0
                    x_age = now - es_ts if es_ts > 0 else 999

                data_stale = l_age > self.STALE_THRESHOLD_SEC or x_age > self.STALE_THRESHOLD_SEC

                if data_stale and not self.dry_run:
                    if self._pending and not self._maker_pulled:
                        self.logger.warning("[数据过期] 撤回 Lighter 挂单 (保留WS追踪)")
                        saved = self._pending
                        await self._cancel_pending_maker()
                        await asyncio.sleep(1.0)
                        if (self._pending is saved
                                and saved.filled_qty <= saved.hedged_qty):
                            self._pending = None
                        self._maker_pulled = True
                    if self.state not in (State.HEDGING, State.CLOSING_HEDGE):
                        if now - last_status_log > 5:
                            stale_parts = []
                            if l_age > self.STALE_THRESHOLD_SEC:
                                stale_parts.append(f"L={l_age:.1f}s")
                            if x_age > self.STALE_THRESHOLD_SEC:
                                stale_parts.append(f"X={x_age:.1f}s")
                            self.logger.warning(
                                f"[数据过期] {' '.join(stale_parts)} (状态={self.state.value})")
                            last_status_log = now
                        await asyncio.sleep(self.interval)
                        continue
                else:
                    self._maker_pulled = False

                if self._liquidation_detected:
                    self.logger.error("[强制平仓] 检测到 Lighter 清算，立即处理 Extended 仓位...")
                    await self._cancel_all_makers()
                    await self._reconcile_positions()
                    if abs(self.extended_position) > 0:
                        side = "sell" if self.extended_position > 0 else "buy"
                        x_ref = x_ws_bid if side == "sell" else x_ws_ask
                        if x_ref:
                            self.logger.error(
                                f"[强制平仓] 平掉 Extended {side} {abs(self.extended_position)}")
                            await self._place_extended_ioc(
                                side, abs(self.extended_position), x_ref)
                    self._reset_position_state()
                    self.lighter_position = Decimal("0")
                    self.extended_position = Decimal("0")
                    self._liquidation_detected = False
                    self.state = State.QUOTING
                    self.logger.error("[强制平仓] 处理完成，暂停 60s")
                    await asyncio.sleep(60)
                    await self._reconcile_positions()
                    continue

                mid_spread_bps = float((x_mid - l_mid) / l_mid * 10000)
                abs_mid_spread = abs(mid_spread_bps)
                cur_pos = abs(self.lighter_position)

                # ── Status log ──
                if now - last_status_log > 15:
                    hold_s = ""
                    if self.position_opened_at:
                        hold_s = f" hold={now - self.position_opened_at:.0f}s"
                    pend = ""
                    if self._pending:
                        pend = f" maker={self._pending.side}@{self._pending.price}"
                    quota_s = f" 配额={self._volume_quota_remaining}" if self._volume_quota_remaining is not None else ""
                    tx_rate = len([t for t in self._tx_count_window if t > now - 60])
                    self.logger.info(
                        f"[{self.state.value}] mid={mid_spread_bps:+.2f}bps  "
                        f"L_pos={self.lighter_position} X_pos={self.extended_position}{hold_s}  "
                        f"L={l_bid}/{l_ask} X={x_ws_bid}/{x_ws_ask}{pend}  "
                        f"鲜度:L={l_age:.1f}s X={x_age:.1f}s  "
                        f"tx/min={tx_rate}{quota_s}  "
                        f"累积={self.cumulative_net_bps:+.2f}bps")
                    last_status_log = now

                # ── Periodic reconciliation + order verification ──
                if (not self.dry_run and now - last_reconcile > 20
                        and self.state in (State.QUOTING, State.IN_POSITION)):
                    await self._reconcile_positions()
                    last_reconcile = now
                    cur_pos = abs(self.lighter_position)

                    if self._pending and self._pending.order_index is not None:
                        order_age = now - self._pending.placed_at
                        if order_age < 5.0:
                            self.logger.debug(
                                f"[验证] 订单刚放置 {order_age:.1f}s, 跳过验证")
                        else:
                            order_exists = await self._verify_lighter_order_exists()
                            if not order_exists:
                                self.logger.warning(
                                    f"[验证] 订单丢失! 清除 _pending 并重新挂单")
                                self._pending = None

                # ── Stale fill repair ──
                if (self._stale_fills
                        and self.state not in (State.HEDGING, State.CLOSING_HEDGE)
                        and not self.dry_run):
                    repaired = []
                    for sf in self._stale_fills:
                        sf_idx = sf.get("order_index", "")
                        if sf_idx and sf_idx in self._processed_order_indices:
                            self.logger.info(f"[修复] 跳过已处理订单: idx={sf_idx}")
                            repaired.append(sf)
                            continue
                        if sf["size"] < self.extended_min_order_size:
                            self.logger.warning(
                                f"[修复] 丢弃低于最小单量的stale fill: "
                                f"size={sf['size']} < min={self.extended_min_order_size}")
                            repaired.append(sf)
                            continue
                        retry_key = sf_idx or id(sf)
                        retries = self._stale_retry_counts.get(retry_key, 0)
                        if retries >= self._max_stale_retries:
                            self.logger.error(
                                f"[修复] stale fill 重试{retries}次仍失败, 放弃: "
                                f"idx={sf_idx} size={sf['size']}")
                            repaired.append(sf)
                            continue
                        projected_pos = abs(self.lighter_position) + sf["size"]
                        if projected_pos > self.max_position * Decimal("1.5"):
                            self.logger.warning(
                                f"[修复] 跳过: 修复后仓位{projected_pos}将超限 "
                                f"(max={self.max_position})")
                            repaired.append(sf)
                            continue
                        hedge_side = "sell" if sf["side"] == "buy" else "buy"
                        x_ref = x_ws_bid if hedge_side == "sell" else x_ws_ask
                        if x_ref:
                            self.logger.warning(
                                f"[修复] 过期成交: Extended IOC {hedge_side} {sf['size']} "
                                f"idx={sf_idx}")
                            actual_fill = await self._place_extended_ioc(
                                hedge_side, sf["size"], x_ref)
                            if actual_fill is not None:
                                if sf_idx:
                                    self._record_processed_order(sf_idx)
                                if sf["side"] == "buy":
                                    self.lighter_position += sf["size"]
                                    self.extended_position -= actual_fill
                                else:
                                    self.lighter_position -= sf["size"]
                                    self.extended_position += actual_fill

                                is_closing = (
                                    self.state == State.IN_POSITION
                                    and self.position_direction
                                    and self._open_l_refs
                                    and (
                                        (self.position_direction == "long_L_short_X"
                                         and sf["side"] == "sell")
                                        or (self.position_direction == "long_X_short_L"
                                            and sf["side"] == "buy")
                                    )
                                )
                                if is_closing:
                                    net_bps = self._log_ref_pnl(
                                        sf["price"], x_ref, sf["side"], hedge_side)
                                    self.cumulative_net_bps += net_bps
                                    self._csv_row([
                                        datetime.now(timezone.utc).isoformat(),
                                        "CLOSE_REPAIR",
                                        self.position_direction or "",
                                        sf["side"], f"{sf['price']:.2f}",
                                        str(sf["size"]),
                                        hedge_side, f"{x_ref:.2f}",
                                        str(actual_fill),
                                        "", "",
                                        f"{net_bps:.2f}",
                                        f"{self.cumulative_net_bps:.2f}",
                                    ])
                                    if abs(self.lighter_position) < self.order_size * Decimal("0.01"):
                                        if self._pending:
                                            self.logger.info(
                                                "[修复] 平仓完成, 撤销残留挂单")
                                            await self._cancel_pending_maker()
                                        self._last_close_time = time.time()
                                        self._reset_position_state()
                                elif (self.state == State.QUOTING
                                      and abs(self.lighter_position) >= self.order_size * Decimal("0.5")):
                                    self.state = State.IN_POSITION
                                    self.position_opened_at = self.position_opened_at or time.time()
                                    if sf["side"] == "buy":
                                        self.position_direction = "long_L_short_X"
                                    else:
                                        self.position_direction = "long_X_short_L"
                                    self.logger.warning(
                                        f"[修复] stale repair 创建仓位, 转入 IN_POSITION")

                                self.logger.info(
                                    f"[修复完成] L_pos={self.lighter_position} "
                                    f"X_pos={self.extended_position}")
                                repaired.append(sf)
                                if retry_key in self._stale_retry_counts:
                                    del self._stale_retry_counts[retry_key]
                            else:
                                self._stale_retry_counts[retry_key] = retries + 1
                                break
                        else:
                            self._stale_retry_counts[retry_key] = retries + 1
                            break
                    for sf in repaired:
                        self._stale_fills.remove(sf)
                    if repaired:
                        last_reconcile = time.time() - 50

                # ━━━━━━ STATE: QUOTING ━━━━━━
                if self.state == State.QUOTING:
                    if x_ws_bid is None or x_ws_ask is None:
                        await asyncio.sleep(self.interval)
                        continue

                    if now < self._loss_breaker_until:
                        if self._pending:
                            await self._cancel_pending_maker()
                            self._pending = None
                        if now - last_status_log > 15:
                            remaining = self._loss_breaker_until - now
                            self.logger.warning(
                                f"[熔断] 亏损熔断中, 剩余 {remaining:.0f}s  "
                                f"累积={self.cumulative_net_bps:+.2f}bps")
                            last_status_log = now
                        await asyncio.sleep(1.0)
                        continue

                    cooldown_remaining = self._post_close_cooldown - (now - self._last_close_time)
                    if self._last_close_time > 0 and cooldown_remaining > 0:
                        if self._pending:
                            await self._cancel_pending_maker()
                            self._pending = None
                        if now - last_status_log > 10:
                            self.logger.info(
                                f"[冷却] 平仓冷却中, 剩余 {cooldown_remaining:.0f}s")
                            last_status_log = now
                        await asyncio.sleep(min(cooldown_remaining, 1.0))
                        continue

                    if cur_pos >= self.max_position:
                        if self._pending:
                            await self._cancel_pending_maker()
                            self._pending = None
                        await asyncio.sleep(self.interval)
                        continue

                    if self._pending is None and not self.dry_run:
                        real_l = await self._get_lighter_position()
                        real_x = await self._get_extended_position()
                        real_pos = abs(real_l)
                        if real_pos >= self.max_position:
                            self.logger.warning(
                                f"[安全] API 仓位超限! L={real_l} X={real_x} "
                                f"(内部: L={self.lighter_position} X={self.extended_position})")
                            self.lighter_position = real_l
                            self.extended_position = real_x
                            cur_pos = real_pos
                            if real_pos > 0:
                                self.state = State.IN_POSITION
                                self.position_opened_at = self.position_opened_at or time.time()
                                if real_l > 0:
                                    self.position_direction = "long_L_short_X"
                                else:
                                    self.position_direction = "long_X_short_L"
                            continue
                        elif abs(real_l - self.lighter_position) > self.order_size * Decimal("0.5"):
                            self.logger.warning(
                                f"[安全] 仓位追踪偏差! API: L={real_l} X={real_x} "
                                f"vs 内部: L={self.lighter_position} X={self.extended_position}")
                            self.lighter_position = real_l
                            self.extended_position = real_x
                            cur_pos = abs(real_l)

                    target = self._calc_maker_target(x_ws_bid, x_ws_ask, l_bid, l_ask)
                    if target and not self.dry_run:
                        t_side, t_dir, t_price = target
                        await self._update_maker(t_side, t_dir, t_price,
                                                 self.order_size, "open")
                    elif self.dry_run and target:
                        t_side, t_dir, t_price = target
                        if now - last_status_log > 10:
                            self.logger.info(
                                f"[DRY-RUN QUOTE] {t_side}@{t_price} dir={t_dir}")

                # ━━━━━━ STATE: HEDGING ━━━━━━
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
                            x_ref = x_ws_bid if x_ws_bid else x_mid
                        else:
                            hedge_side = "buy"
                            x_ref = x_ws_ask if x_ws_ask else x_mid

                        self.logger.info(
                            f"[HEDGE] Extended IOC {hedge_side} {hedge_qty} ref={x_ref:.2f}")

                        t0 = time.time()
                        actual_fill = await self._place_extended_ioc(hedge_side, hedge_qty, x_ref)
                        hedge_ms = (time.time() - t0) * 1000

                        if actual_fill is not None:
                            self._pending.hedged_qty += actual_fill
                            self._pending.hedge_qty_needed = hedge_qty - actual_fill
                            l_fill_price = self._pending.avg_price or self._pending.price
                            self._open_l_refs.append((l_fill_price, actual_fill))
                            self._open_x_refs.append((x_ref, actual_fill))

                            if direction == "long_L_short_X":
                                self.lighter_position += actual_fill
                                self.extended_position -= actual_fill
                            else:
                                self.lighter_position -= actual_fill
                                self.extended_position += actual_fill

                            self.logger.info(
                                f"[HEDGE OK] {hedge_ms:.0f}ms fill={actual_fill}/{hedge_qty} "
                                f"L_pos={self.lighter_position} X_pos={self.extended_position}")

                            if self._pending.hedged_qty >= self._pending.filled_qty:
                                if self.position_opened_at is None:
                                    self.position_opened_at = time.time()
                                self.position_direction = direction
                                self._csv_row([
                                    datetime.now(timezone.utc).isoformat(),
                                    "OPEN", direction,
                                    self._pending.side, f"{l_fill_price:.2f}",
                                    str(actual_fill),
                                    hedge_side, f"{x_ref:.2f}", str(actual_fill),
                                    f"{mid_spread_bps:.2f}", f"{hedge_ms:.0f}", "", "",
                                ])
                                if self._pending.order_index is not None:
                                    self._record_processed_order(str(self._pending.order_index))
                                if self._pending.filled_qty < self._pending.qty_target:
                                    self.logger.info(
                                        f"[HEDGE] 部分成交 {self._pending.filled_qty}/"
                                        f"{self._pending.qty_target}, 取消剩余挂单")
                                    await self._cancel_pending_maker()
                                self._pending = None
                                self.state = State.IN_POSITION
                            else:
                                pass
                        else:
                            self.logger.error("[HEDGE FAIL] 紧急平 Lighter")
                            fill_side = self._pending.side
                            if fill_side == "buy":
                                self.lighter_position += hedge_qty
                            else:
                                self.lighter_position -= hedge_qty
                            self.logger.warning(
                                f"[HEDGE FAIL] 先记录Lighter仓位变化: L_pos={self.lighter_position}")
                            l_reverse = "sell" if fill_side == "buy" else "buy"
                            if l_reverse == "sell":
                                l_ref = Decimal(str(ls.best_bid or ls.mid or 0))
                            else:
                                l_ref = Decimal(str(ls.best_ask or ls.mid or 0))
                            if l_ref > 0:
                                await self._place_lighter_taker(l_reverse, hedge_qty, l_ref)
                            self._pending = None
                            self.state = State.QUOTING
                            last_reconcile = time.time() - 55

                # ━━━━━━ STATE: IN_POSITION ━━━━━━
                elif self.state == State.IN_POSITION:
                    if (abs(self.lighter_position) < self.order_size * Decimal("0.01")
                            and abs(self.extended_position) < self.order_size * Decimal("0.01")):
                        if self._open_l_refs:
                            net_bps = self._log_ref_pnl(l_mid, x_mid, "", "")
                            self.cumulative_net_bps += net_bps
                            self.logger.info(
                                f"[仓位清零] 估算P&L={net_bps:+.2f}bps "
                                f"累积={self.cumulative_net_bps:+.2f}bps")
                        self.logger.info("[IN_POSITION] 回到 QUOTING")
                        self._pending = None
                        self._reset_position_state()
                        self.state = State.QUOTING
                        await self._do_cancel_all()
                        continue

                    if (self.position_opened_at
                            and now - self.position_opened_at > self.max_hold_sec):
                        self.logger.warning(
                            f"[超时强平] 持仓 {now - self.position_opened_at:.0f}s "
                            f"> {self.max_hold_sec}s")
                        await self._emergency_flatten()
                        self._pending = None
                        self._reset_position_state()
                        await self._do_cancel_all()
                        self.state = State.QUOTING
                        last_reconcile = time.time() - 50
                        continue

                    l_zero = abs(self.lighter_position) < self.order_size * Decimal("0.01")
                    x_zero = abs(self.extended_position) < self.order_size * Decimal("0.01")
                    if l_zero != x_zero and not self.dry_run:
                        self.logger.warning(
                            f"[单边仓位] L={self.lighter_position} X={self.extended_position} "
                            f"直接平掉残留仓位")
                        if not l_zero:
                            side = "sell" if self.lighter_position > 0 else "buy"
                            ref = Decimal(str(ls.best_bid if side == "sell" else ls.best_ask))
                            if ref > 0:
                                await self._place_lighter_taker(
                                    side, abs(self.lighter_position), ref)
                        if not x_zero:
                            async with self._hedge_lock:
                                side = "sell" if self.extended_position > 0 else "buy"
                                x_ref = x_ws_bid if side == "sell" else x_ws_ask
                                if x_ref:
                                    await self._place_extended_ioc(
                                        side, abs(self.extended_position), x_ref)
                        await asyncio.sleep(2)
                        await self._reconcile_positions()
                        if (abs(self.lighter_position) < self.order_size * Decimal("0.01")
                                and abs(self.extended_position) < self.order_size * Decimal("0.01")):
                            if self._pending:
                                self.logger.info(
                                    "[单边仓位] 撤销残留平仓挂单")
                                await self._cancel_pending_maker()
                            self._reset_position_state()
                            self.state = State.QUOTING
                            self.logger.info("[单边仓位] 清仓完成, 回到 QUOTING")
                        continue

                    if x_ws_bid is not None and x_ws_ask is not None and not self.dry_run:
                        close_target = self._calc_close_maker_target(
                            x_ws_bid, x_ws_ask, l_bid, l_ask)
                        if close_target:
                            c_side, c_price = close_target
                            close_qty = abs(self.lighter_position)
                            if close_qty > 0:
                                await self._update_maker(
                                    c_side, self.position_direction or "",
                                    c_price, close_qty, "close")

                # ━━━━━━ STATE: CLOSING_HEDGE ━━━━━━
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
                        if hedge_side == "sell":
                            x_ref = x_ws_bid if x_ws_bid else x_mid
                        else:
                            x_ref = x_ws_ask if x_ws_ask else x_mid

                        self.logger.info(
                            f"[CLOSE_HEDGE] Extended IOC {hedge_side} {hedge_qty}")

                        t0 = time.time()
                        actual_fill = await self._place_extended_ioc(hedge_side, hedge_qty, x_ref)
                        hedge_ms = (time.time() - t0) * 1000

                        if actual_fill is not None:
                            self._pending.hedged_qty += actual_fill
                            self._pending.hedge_qty_needed = hedge_qty - actual_fill
                            l_fill_price = self._pending.avg_price or self._pending.price

                            l_side = self._pending.side
                            if l_side == "buy":
                                self.lighter_position += actual_fill
                                self.extended_position -= actual_fill
                            else:
                                self.lighter_position -= actual_fill
                                self.extended_position += actual_fill

                            net_bps = self._log_ref_pnl(
                                l_fill_price, x_ref,
                                self._pending.side, hedge_side)
                            self.cumulative_net_bps += net_bps

                            self.logger.info(
                                f"[平仓完成] {hedge_ms:.0f}ms fill={actual_fill}/{hedge_qty} "
                                f"净利={net_bps:+.2f}bps 累积={self.cumulative_net_bps:+.2f}bps")
                            self._csv_row([
                                datetime.now(timezone.utc).isoformat(),
                                "CLOSE", "convergence",
                                self._pending.side, f"{l_fill_price:.2f}",
                                str(actual_fill),
                                hedge_side, f"{x_ref:.2f}", str(actual_fill),
                                f"{mid_spread_bps:.2f}", f"{hedge_ms:.0f}",
                                f"{net_bps:.2f}", f"{self.cumulative_net_bps:.2f}",
                            ])

                            if self._pending.hedged_qty >= self._pending.filled_qty:
                                if self._pending.filled_qty >= self._pending.qty_target:
                                    if self._pending.order_index is not None:
                                        self._record_processed_order(str(self._pending.order_index))
                                    self._pending = None
                                    pos_remaining = abs(self.lighter_position)
                                    if pos_remaining < self.order_size * Decimal("0.01"):
                                        self._last_close_time = time.time()
                                        if net_bps < 0:
                                            self._consecutive_losses += 1
                                        else:
                                            self._consecutive_losses = 0
                                        if (self._consecutive_losses >= 3
                                                or self.cumulative_net_bps < self._loss_breaker_threshold):
                                            self._loss_breaker_until = (
                                                time.time() + self._loss_breaker_duration)
                                            self.logger.error(
                                                f"[熔断] 触发亏损熔断! "
                                                f"连亏={self._consecutive_losses} "
                                                f"累积={self.cumulative_net_bps:+.2f}bps, "
                                                f"暂停{self._loss_breaker_duration:.0f}s")
                                        self._reset_position_state()
                                        self.state = State.QUOTING
                                        await self._do_cancel_all()
                                    else:
                                        self.state = State.IN_POSITION
                                    last_reconcile = time.time() - 50
                                else:
                                    self.logger.info(
                                        f"[CLOSE_HEDGE] 部分成交 "
                                        f"{self._pending.filled_qty}/"
                                        f"{self._pending.qty_target}, 继续跟踪")
                                    self.state = State.IN_POSITION
                            else:
                                self.state = State.IN_POSITION
                        else:
                            self.logger.error("[CLOSE_HEDGE FAIL] 紧急平仓")
                            await self._emergency_flatten()
                            self._pending = None
                            self._reset_position_state()
                            self.state = State.QUOTING
                            await self._do_cancel_all()
                            last_reconcile = time.time() - 50

                # ── Sleep / fill event wait ──
                if self.state in (State.QUOTING, State.IN_POSITION):
                    try:
                        self._lighter_fill_event.clear()
                        await asyncio.wait_for(
                            self._lighter_fill_event.wait(), timeout=self.interval)
                    except asyncio.TimeoutError:
                        pass
                else:
                    await asyncio.sleep(0.05)

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
        fee_dollar = notional * self.ROUND_TRIP_FEE_BPS / 10000
        net = total - fee_dollar
        net_bps = net / notional * 10000 if notional > 0 else 0.0

        self.logger.info(
            f"[P&L] L={l_pnl:+.3f}$ X={x_pnl:+.3f}$ "
            f"费用={fee_dollar:.3f}$({self.ROUND_TRIP_FEE_BPS:.1f}bps) "
            f"净={net:+.3f}$({net_bps:+.1f}bps)")
        return net_bps

    def _reset_position_state(self):
        self.position_opened_at = None
        self.position_direction = None
        self._open_l_refs.clear()
        self._open_x_refs.clear()
        self._pending = None


def main():
    p = argparse.ArgumentParser(
        description="V5 Always-On Lighter Maker + Extended Taker (~5.8bps费用)")
    p.add_argument("-s", "--symbol", default="ETH")
    p.add_argument("--size", type=str, default="0.1")
    p.add_argument("--max-position", type=str, default="0.5")
    p.add_argument("--target-spread", type=float, default=3.0,
                   help="开仓最低利润目标 (bps, 必须>=0)")
    p.add_argument("--close-bps", type=float, default=1.0,
                   help="平仓最低利润缓冲 (bps)")
    p.add_argument("--max-hold-sec", type=float, default=300.0)
    p.add_argument("--cooldown", type=float, default=30.0,
                   help="平仓后冷却时间 (秒)")
    p.add_argument("--loss-breaker-bps", type=float, default=-15.0,
                   help="累积亏损熔断阈值 (bps)")
    p.add_argument("--loss-breaker-sec", type=float, default=300.0,
                   help="亏损熔断暂停时间 (秒)")
    p.add_argument("--min-l-spread", type=int, default=2,
                   help="Lighter最小价差ticks数 (低于此不开仓)")
    p.add_argument("--interval", type=float, default=0.1,
                   help="主循环间隔 (秒, co-located 推荐 0.1)")
    p.add_argument("--reprice-interval", type=float, default=1.5,
                   help="最小调价间隔 (秒, Lighter限制40次/分钟)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--env-file", default=".env")
    args = p.parse_args()

    load_dotenv(args.env_file)

    if args.target_spread < 0:
        print(f"[警告] target_spread={args.target_spread} 为负数，数学上必然亏损！"
              f"已自动修正为 0")
        args.target_spread = 0

    bot = AlwaysOnMakerBot(
        symbol=args.symbol.upper(),
        order_size=Decimal(args.size),
        max_position=Decimal(args.max_position),
        target_spread=args.target_spread,
        close_bps=args.close_bps,
        max_hold_sec=args.max_hold_sec,
        interval=args.interval,
        reprice_interval=args.reprice_interval,
        dry_run=args.dry_run,
    )
    bot._post_close_cooldown = args.cooldown
    bot._close_profit_buffer_bps = max(args.close_bps, 0.5)
    bot._loss_breaker_threshold = args.loss_breaker_bps
    bot._loss_breaker_duration = args.loss_breaker_sec
    bot._min_lighter_spread_ticks = args.min_l_spread
    if uvloop is not None:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("[uvloop] 已启用高性能事件循环")
    else:
        print("[uvloop] 未安装, 使用默认 asyncio (pip install uvloop 可提速 2-4x)")

    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n已中断。")


if __name__ == "__main__":
    main()
