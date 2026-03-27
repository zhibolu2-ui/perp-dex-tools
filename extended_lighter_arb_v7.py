#!/usr/bin/env python3
"""
Extended + Lighter 价差收敛套利 V7（开仓 Taker + 平仓 Maker 混合策略）

策略：
  开仓: 检测到价差后，Extended IOC 市价吃单 + Lighter IOC 同步对冲（双边 taker）
  平仓: Extended POST_ONLY maker 挂单等待成交 → Lighter IOC taker 对冲（同 V6）

费用: ~2.5 bps 往返 (Extended taker 2.5bps 开仓 + Extended maker 0% 平仓 + Lighter taker 0%)

与 V6 的区别：
  V6: 开仓被动 maker，等待成交 → 成交率低，但 0 费用
  V7: 开仓主动 taker，立即成交 → 成交率高，付 2.5bps 开仓费

用法:
  python extended_lighter_arb_v7.py --symbol BTC --size 0.002 --target-spread 3.5
  python extended_lighter_arb_v7.py --symbol ETH --dry-run
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
    order_id: str
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


class ExtMakerBot:

    ROUND_TRIP_FEE_BPS = 2.5

    def __init__(
        self,
        symbol: str,
        order_size: Decimal,
        max_position: Decimal,
        target_spread: float,
        close_profit_bps: float,
        max_hold_sec: float,
        interval: float,
        reprice_interval: float,
        dry_run: bool,
    ):
        self.symbol = symbol.upper()
        self.order_size = order_size
        self.max_position = max_position
        self.target_spread = target_spread
        self.close_profit_bps = close_profit_bps
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
        self._fill_event = asyncio.Event()
        self._hedge_lock = asyncio.Lock()

        self._x_ioc_pending_id: Optional[str] = None
        self._x_ioc_confirmed = asyncio.Event()
        self._x_ioc_fill_qty: Decimal = Decimal("0")

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

        self._post_close_cooldown: float = 30.0
        self._last_close_time: float = 0.0
        self._consecutive_losses: int = 0
        self._loss_breaker_until: float = 0.0
        self._loss_breaker_threshold: float = -15.0
        self._loss_breaker_duration: float = 300.0

        self._cancel_grace_until: float = 0.0
        self._maker_pull_until: float = 0.0
        self._decay_phase: str = "P1"

        os.makedirs("logs", exist_ok=True)
        self.csv_path = f"logs/arb_v7_{self.symbol}_trades.csv"
        self.log_path = f"logs/arb_v7_{self.symbol}.log"
        self._init_csv()
        self.logger = self._init_logger()

    # ═══════════════════════════════════════════════
    #  Logging / CSV
    # ═══════════════════════════════════════════════

    def _init_logger(self) -> logging.Logger:
        lg = logging.getLogger(f"arb_v7_{self.symbol}")
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
                    "extended_side", "extended_price", "extended_qty",
                    "lighter_side", "lighter_price", "lighter_qty",
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
    #  Extended WS BBO
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

    def _extended_ob_age_ms(self) -> float:
        if not self.extended_client or self.extended_client._ob_last_update_ts == 0:
            return 999999.0
        return (time.time() - self.extended_client._ob_last_update_ts) * 1000

    # ═══════════════════════════════════════════════
    #  Extended WS Fill Detection
    # ═══════════════════════════════════════════════

    def _on_extended_order_event(self, event: dict):
        order_id = str(event.get("order_id", ""))
        status = event.get("status", "")
        filled_size_str = event.get("filled_size", "0")

        if self._pending and order_id == self._pending.order_id:
            self._handle_maker_fill(event)
            return

        if self._x_ioc_pending_id and order_id == self._x_ioc_pending_id:
            if status in ("FILLED", "CANCELED", "EXPIRED"):
                self._x_ioc_fill_qty = Decimal(str(filled_size_str))
                self._x_ioc_confirmed.set()
            return

        if status in ("FILLED", "PARTIALLY_FILLED"):
            filled = Decimal(str(filled_size_str))
            if filled > 0:
                self.logger.warning(
                    f"[Extended] 孤立订单成交! id={order_id} status={status} "
                    f"filled={filled}  触发强制对账")
                self._fill_event.set()

    def _handle_maker_fill(self, event: dict):
        try:
            status = event.get("status", "")
            filled_size = Decimal(str(event.get("filled_size", "0")))
            avg_price_str = event.get("avg_price")

            if status == "OPEN":
                self.logger.info(f"[Extended] Maker 挂单已上线 id={self._pending.order_id}")
                return

            if status in ("FILLED", "PARTIALLY_FILLED"):
                if filled_size <= self._pending.filled_qty:
                    return

                self._pending.filled_qty = filled_size
                if avg_price_str:
                    try:
                        self._pending.avg_price = Decimal(str(avg_price_str))
                    except Exception:
                        pass
                self._pending.hedge_qty_needed = filled_size - self._pending.hedged_qty

                if self._pending.hedge_qty_needed >= self.order_size * Decimal("0.01"):
                    if self._pending.phase == "open":
                        self.state = State.HEDGING
                    else:
                        self.state = State.CLOSING_HEDGE
                    self.logger.info(
                        f"[Extended FILL] {self._pending.side} filled={filled_size} "
                        f"avg={avg_price_str} 待对冲={self._pending.hedge_qty_needed}")
                    self._fill_event.set()
                    asyncio.ensure_future(self._fast_hedge())

            elif status == "CANCELED":
                if time.time() < self._cancel_grace_until:
                    self.logger.info("[Extended] cancel 保护窗口内, 忽略 CANCELED 事件")
                    return
                if filled_size > 0 and filled_size > (self._pending.filled_qty if self._pending else Decimal("0")):
                    self._handle_maker_fill({**event, "status": "FILLED"})
                    return
                self.logger.info(f"[Extended] Maker 挂单已取消 (filled={filled_size})")
                if self._pending and self.state in (State.QUOTING, State.IN_POSITION):
                    self._pending = None
        except Exception as e:
            self.logger.error(f"处理 Extended maker 事件出错: {e}\n{traceback.format_exc()}")

    # ═══════════════════════════════════════════════
    #  Fast Hedge (Lighter Taker)
    # ═══════════════════════════════════════════════

    async def _fast_hedge(self):
        async with self._hedge_lock:
            if self._pending is None or self._pending.hedge_qty_needed <= 0:
                return

            hedge_qty = self._pending.hedge_qty_needed
            phase = self._pending.phase
            direction = self._pending.direction

            if phase == "open":
                hedge_side = "sell" if direction == "long_X_short_L" else "buy"
            else:
                x_side = self._pending.side
                hedge_side = "sell" if x_side == "buy" else "buy"

            ls = self.lighter_feed.snapshot if self.lighter_feed else None
            if not ls or not ls.mid:
                self.logger.warning("[FAST_HEDGE] Lighter 行情不可用")
                return
            if hedge_side == "sell":
                l_ref = Decimal(str(ls.best_bid or ls.mid))
            else:
                l_ref = Decimal(str(ls.best_ask or ls.mid))

            t0 = time.time()
            actual_fill = await self._place_lighter_taker(hedge_side, hedge_qty, l_ref)
            hedge_ms = (time.time() - t0) * 1000

            if actual_fill is None:
                self.logger.warning(f"[FAST_HEDGE] Lighter IOC 失败({hedge_ms:.0f}ms), 回退主循环")
                return

            self._pending.hedged_qty += actual_fill
            self._pending.hedge_qty_needed -= actual_fill
            x_fill_price = self._pending.avg_price or self._pending.price
            l_actual = self._last_lighter_avg_price or l_ref

            x_bid, x_ask = self._get_extended_ws_bbo()
            x_mid = (x_bid + x_ask) / 2 if x_bid and x_ask else x_fill_price
            l_mid = Decimal(str(ls.mid)) if ls.mid else l_ref
            mid_spread_bps = float((x_mid - l_mid) / l_mid * 10000) if l_mid > 0 else 0.0

            if phase == "open":
                self._open_x_refs.append((x_fill_price, actual_fill))
                self._open_l_refs.append((l_actual, actual_fill))
                if direction == "long_X_short_L":
                    self.extended_position += actual_fill
                else:
                    self.extended_position -= actual_fill

                self.logger.info(
                    f"[FAST_HEDGE] {hedge_ms:.0f}ms fill={actual_fill}/{hedge_qty} "
                    f"X_pos={self.extended_position} L_pos={self.lighter_position}")

                if self._pending.hedged_qty >= self._pending.filled_qty:
                    if self._pending.filled_qty < self._pending.qty_target:
                        self.logger.info(
                            f"[FAST_HEDGE] 部分成交已对冲, 撤销剩余挂单 "
                            f"filled={self._pending.filled_qty}/{self._pending.qty_target}")
                        self._cancel_grace_until = time.time() + 2.0
                        await self._cancel_extended_order(self._pending.order_id)
                    if self.position_opened_at is None:
                        self.position_opened_at = time.time()
                    self.position_direction = direction
                    self._csv_row([
                        datetime.now(timezone.utc).isoformat(),
                        "OPEN", direction,
                        self._pending.side, f"{x_fill_price:.2f}", str(actual_fill),
                        hedge_side, f"{l_actual:.2f}", str(actual_fill),
                        f"{mid_spread_bps:.2f}", f"{hedge_ms:.0f}", "", "",
                    ])
                    self._pending = None
                    self.state = State.IN_POSITION
                else:
                    self.logger.info(
                        f"[FAST_HEDGE] 部分对冲完成, 剩余待冲={self._pending.hedge_qty_needed}")
                    self.state = State.HEDGING
            else:
                x_side = self._pending.side
                if x_side == "buy":
                    self.extended_position += actual_fill
                else:
                    self.extended_position -= actual_fill

                net_bps = self._log_ref_pnl(x_fill_price, l_actual)
                self.cumulative_net_bps += net_bps

                self.logger.info(
                    f"[FAST_CLOSE] {hedge_ms:.0f}ms fill={actual_fill}/{hedge_qty} "
                    f"净利={net_bps:+.2f}bps 累积={self.cumulative_net_bps:+.2f}bps")
                self._csv_row([
                    datetime.now(timezone.utc).isoformat(),
                    "CLOSE", "convergence",
                    self._pending.side, f"{x_fill_price:.2f}", str(actual_fill),
                    hedge_side, f"{l_actual:.2f}", str(actual_fill),
                    f"{mid_spread_bps:.2f}", f"{hedge_ms:.0f}",
                    f"{net_bps:.2f}", f"{self.cumulative_net_bps:.2f}",
                ])

                if self._pending.hedged_qty >= self._pending.filled_qty:
                    if self._pending.filled_qty < self._pending.qty_target:
                        self.logger.info(
                            f"[FAST_CLOSE] 部分成交已对冲, 撤销剩余挂单 "
                            f"filled={self._pending.filled_qty}/{self._pending.qty_target}")
                        self._cancel_grace_until = time.time() + 2.0
                        await self._cancel_extended_order(self._pending.order_id)
                    self._pending = None
                    pos_remaining = abs(self.extended_position)
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
                                f"[熔断] 连亏={self._consecutive_losses} "
                                f"累积={self.cumulative_net_bps:+.2f}bps, "
                                f"暂停{self._loss_breaker_duration:.0f}s")
                        self._reset_position_state()
                        self.state = State.QUOTING
                    else:
                        self.state = State.IN_POSITION
                else:
                    self.logger.info(
                        f"[FAST_CLOSE] 部分对冲完成, 剩余待冲={self._pending.hedge_qty_needed}")
                    self.state = State.CLOSING_HEDGE

    # ═══════════════════════════════════════════════
    #  Maker Pricing
    # ═══════════════════════════════════════════════

    def _round_lighter_price(self, price: Decimal) -> Decimal:
        if self.lighter_tick > 0:
            return (price / self.lighter_tick).quantize(Decimal("1")) * self.lighter_tick
        return price

    def _round_extended_price(self, price: Decimal) -> Decimal:
        return price.quantize(self.extended_tick_size, rounding=ROUND_HALF_UP)

    def _calc_taker_signal(
        self, l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
    ) -> Optional[Tuple[str, str, Decimal, Decimal]]:
        """Check if spread is wide enough for taker-taker open.
        Returns (x_side, direction, x_ref_price, l_ref_price) or None."""
        threshold = Decimal(str(self.target_spread))

        buy_spread_bps = (l_bid - x_ask) / x_ask * Decimal("10000")
        sell_spread_bps = (x_bid - l_ask) / l_ask * Decimal("10000")

        if buy_spread_bps >= threshold:
            return ("buy", "long_X_short_L", x_ask, l_bid)
        if sell_spread_bps >= threshold:
            return ("sell", "long_L_short_X", x_bid, l_ask)
        return None

    async def _execute_taker_open(
        self, x_side: str, direction: str,
        x_ref: Decimal, l_ref: Decimal,
    ):
        """Execute taker-taker open: Extended IOC → Lighter IOC."""
        t0 = time.time()

        x_fill = await self._place_extended_ioc(x_side, self.order_size, x_ref)
        if x_fill is None:
            self.logger.error("[OPEN] Extended IOC 失败, 放弃本次开仓")
            return

        if direction == "long_X_short_L":
            self.extended_position += x_fill
        else:
            self.extended_position -= x_fill

        hedge_side = "sell" if direction == "long_X_short_L" else "buy"
        l_fill = await self._place_lighter_taker(hedge_side, x_fill, l_ref)
        open_ms = (time.time() - t0) * 1000

        if l_fill is None:
            self.logger.error(
                f"[OPEN] Lighter 对冲失败, 反向平 Extended ({open_ms:.0f}ms)")
            undo_side = "sell" if x_side == "buy" else "buy"
            x_bid, x_ask = self._get_extended_ws_bbo()
            undo_ref = x_bid if undo_side == "sell" else x_ask
            if undo_ref:
                await self._place_extended_ioc(undo_side, x_fill, undo_ref)
            if direction == "long_X_short_L":
                self.extended_position -= x_fill
            else:
                self.extended_position += x_fill
            return

        l_actual = self._last_lighter_avg_price or l_ref
        self._open_x_refs.append((x_ref, x_fill))
        self._open_l_refs.append((l_actual, l_fill))
        self.position_opened_at = time.time()
        self.position_direction = direction
        self.state = State.IN_POSITION

        spread_bps = float((l_actual - x_ref) / x_ref * 10000) if direction == "long_X_short_L" \
            else float((x_ref - l_actual) / l_actual * 10000)

        self.logger.info(
            f"[OPEN] {open_ms:.0f}ms X={x_side}@{x_ref} L={hedge_side}@{l_actual:.2f} "
            f"spread={spread_bps:+.1f}bps X_pos={self.extended_position} "
            f"L_pos={self.lighter_position}")
        self._csv_row([
            datetime.now(timezone.utc).isoformat(),
            "OPEN", direction,
            x_side, f"{x_ref:.2f}", str(x_fill),
            hedge_side, f"{l_actual:.2f}", str(l_fill),
            f"{spread_bps:.2f}", f"{open_ms:.0f}", "", "",
        ])

    def _calc_decay_bonus(self, fee_cost: Decimal) -> Tuple[Decimal, str]:
        """Based on hold duration, progressively relax max_gap."""
        hold_sec = time.time() - self.position_opened_at if self.position_opened_at else 0
        if hold_sec < 300:
            return Decimal("0"), "P1"
        elif hold_sec < 600:
            return fee_cost * Decimal("0.3"), "P2"
        elif hold_sec < 900:
            return fee_cost * Decimal("0.7"), "P3"
        else:
            return fee_cost * Decimal("1.0"), "P4"

    def _calc_close_maker_target(
        self, l_bid: Decimal, l_ask: Decimal,
        x_bid: Decimal, x_ask: Decimal,
    ) -> Optional[Tuple[str, Decimal]]:
        """Conditional close maker with time-based decay.

        Returns a competitive price (near BBO) when the current basis gap
        is small enough for a profitable close. Returns None when the basis
        is too wide, signaling the caller to PULL any existing maker.

        As hold time increases, max_gap is progressively widened via
        _calc_decay_bonus to avoid costly timeout emergency flattens.
        """
        if self.extended_position > 0:
            close_side = "sell"
            if not (self._open_x_refs and self._open_l_refs):
                return None

            x_avg = sum(p * q for p, q in self._open_x_refs) / sum(q for _, q in self._open_x_refs)
            l_avg = sum(p * q for p, q in self._open_l_refs) / sum(q for _, q in self._open_l_refs)

            opening_spread = l_avg - x_avg
            fee_cost = x_avg * Decimal(str(self.ROUND_TRIP_FEE_BPS)) / Decimal("10000")
            profit_target = x_avg * Decimal(str(self.close_profit_bps)) / Decimal("10000")
            max_gap = opening_spread - fee_cost - profit_target

            decay_bonus, self._decay_phase = self._calc_decay_bonus(fee_cost)
            max_gap += decay_bonus

            current_gap = l_ask - x_bid

            if current_gap > max_gap:
                return None

            price = x_bid + self.extended_tick_size
            if x_ask - x_bid > self.extended_tick_size * 2:
                price = x_ask - self.extended_tick_size
            return (close_side, self._round_extended_price(price))

        elif self.extended_position < 0:
            close_side = "buy"
            if not (self._open_x_refs and self._open_l_refs):
                return None

            x_avg = sum(p * q for p, q in self._open_x_refs) / sum(q for _, q in self._open_x_refs)
            l_avg = sum(p * q for p, q in self._open_l_refs) / sum(q for _, q in self._open_l_refs)

            opening_spread = x_avg - l_avg
            fee_cost = x_avg * Decimal(str(self.ROUND_TRIP_FEE_BPS)) / Decimal("10000")
            profit_target = x_avg * Decimal(str(self.close_profit_bps)) / Decimal("10000")
            max_gap = opening_spread - fee_cost - profit_target

            decay_bonus, self._decay_phase = self._calc_decay_bonus(fee_cost)
            max_gap += decay_bonus

            current_gap = x_ask - l_bid

            if current_gap > max_gap:
                return None

            price = x_ask - self.extended_tick_size
            if x_ask - x_bid > self.extended_tick_size * 2:
                price = x_bid + self.extended_tick_size
            if price <= 0:
                return None
            return (close_side, self._round_extended_price(price))

        return None

    # ═══════════════════════════════════════════════
    #  Extended Maker Order Management
    # ═══════════════════════════════════════════════

    async def _place_extended_maker(self, side: str, qty: Decimal,
                                     price: Decimal) -> Optional[str]:
        order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
        rounded_price = self._round_extended_price(price)
        qty_rounded = qty.quantize(self.extended_min_order_size, rounding=ROUND_HALF_UP)
        try:
            result = await self.extended_client.perpetual_trading_client.place_order(
                market_name=self.extended_contract_id,
                amount_of_synthetic=qty_rounded,
                price=rounded_price,
                side=order_side,
                time_in_force=TimeInForce.GTT,
                post_only=True,
                expire_time=datetime.now(tz=timezone.utc) + timedelta(hours=24),
            )
            if result and result.data and result.status == 'OK':
                oid = str(result.data.id)
                self.logger.info(
                    f"[Extended] maker {side} {qty_rounded} @ {rounded_price} id={oid}")
                return oid
            self.logger.error(f"[Extended] maker 下单失败: {result}")
            return None
        except Exception as e:
            self.logger.error(f"[Extended] maker 异常: {e}")
            return None

    async def _cancel_extended_order(self, order_id: str) -> bool:
        try:
            await self.extended_client.perpetual_trading_client.orders.cancel_order(order_id)
            self.logger.info(f"[Extended] 撤单成功 id={order_id}")
            return True
        except Exception as e:
            self.logger.error(f"[Extended] 撤单失败 id={order_id}: {e}")
            return False

    async def _cancel_all_extended_orders_rest(self):
        """通过 REST API 查询并撤销所有 Extended 挂单（兜底，防僵尸）。"""
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
                    self.logger.info(f"[清理] 撤销遗留挂单 id={oid} side={order.side} "
                                     f"price={order.price} status={order.status}")
                    try:
                        await self.extended_client.perpetual_trading_client.orders.cancel_order(oid)
                    except Exception as ce:
                        self.logger.warning(f"[清理] 撤单失败 id={oid}: {ce}")
                    await asyncio.sleep(0.15)
        except Exception as e:
            self.logger.warning(f"[清理] REST 查询挂单失败: {e}")

    async def _cancel_all_extended_orders(self):
        if self._pending:
            await self._cancel_extended_order(self._pending.order_id)
            await asyncio.sleep(0.3)
            if self._pending and self._pending.filled_qty <= self._pending.hedged_qty:
                self._pending = None
        await self._cancel_all_extended_orders_rest()

    async def _update_maker(
        self, target_side: str, target_direction: str,
        target_price: Decimal, qty: Decimal, phase: str,
    ) -> bool:
        now = time.time()

        if self._pending is not None:
            if self._pending.filled_qty > 0 and self._pending.hedge_qty_needed > 0:
                self.logger.info(
                    f"[调价] 跳过: 待对冲={self._pending.hedge_qty_needed} "
                    f"filled={self._pending.filled_qty} hedged={self._pending.hedged_qty}")
                if not self._hedge_lock.locked():
                    asyncio.ensure_future(self._fast_hedge())
                return True

            if self._pending.filled_qty >= self._pending.qty_target:
                self.logger.info("[调价] 跳过: 订单已完全成交, 等待对冲完成")
                return True

            same_side = self._pending.side == target_side
            price_diff = abs(self._pending.price - target_price)
            needs_reprice = price_diff >= self.extended_tick_size

            if same_side and not needs_reprice:
                return True

            if now - self._last_reprice < self.reprice_interval:
                return self._pending is not None

            self.logger.info(
                f"[调价] {self._pending.side}@{self._pending.price} → "
                f"{target_side}@{target_price}  diff={price_diff}")
            self._cancel_grace_until = time.time() + 2.0
            saved = self._pending
            cancel_ok = await self._cancel_extended_order(self._pending.order_id)
            if not cancel_ok:
                return self._pending is not None
            await asyncio.sleep(0.2)
            if self._pending is not saved:
                self.logger.warning("[调价] cancel 期间发生成交, 保持当前状态")
                return self._pending is not None
            if self._pending and self._pending.filled_qty > self._pending.hedged_qty:
                return True
            self._pending = None

        oid = await self._place_extended_maker(target_side, qty, target_price)
        if oid is not None:
            self._pending = PendingMaker(
                order_id=oid,
                side=target_side,
                direction=target_direction,
                qty_target=qty,
                price=target_price,
                placed_at=now,
                phase=phase,
            )
            self._fill_event.clear()
            self._last_reprice = now
            return True
        return False

    # ═══════════════════════════════════════════════
    #  Lighter Taker (IOC with fill verification)
    # ═══════════════════════════════════════════════

    async def _place_lighter_taker(self, side: str, qty: Decimal,
                                    ref_price: Decimal) -> Optional[Decimal]:
        """Returns actual filled quantity, or None if failed.

        Fill confirmation strategy (in priority order):
          1. WS account_orders push (typically <100ms)
          2. REST position polling as fallback
        """
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
            if error:
                self._check_nonce_error(error)
                self.logger.error(f"[Lighter] taker 失败: {error}")
                return None
            self._nonce_error_count = 0
            self.logger.info(f"[Lighter] taker {side} {qty} ref={ref_price:.2f} limit={price:.2f}")

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
                    if ws_filled >= qty * Decimal("0.5"):
                        self._extract_lighter_fill_price(fd, ref_price)
                        if side.lower() == "sell":
                            new_pos = pre_pos - qty
                        else:
                            new_pos = pre_pos + qty
                        self.lighter_position = new_pos
                        avg_p = self._last_lighter_avg_price
                        self.logger.info(
                            f"[Lighter] WS成交确认: status={status} filled={ws_filled} "
                            f"pos {pre_pos} → {new_pos}"
                            + (f" avg_price={avg_p:.2f}" if avg_p else ""))
                        return qty

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
                            self.logger.warning(
                                f"[Lighter] REST确认无实际价格, 使用ref={ref_price:.2f}")
                        self.logger.info(
                            f"[Lighter] REST成交确认(第{attempt+1}次 {delay}s): "
                            f"pos {pre_pos} → {new_pos} Δ={delta}")
                        return qty
                except Exception as e:
                    self.logger.warning(f"[Lighter] 验证失败: {e}")

            self.logger.warning(
                f"[Lighter] IOC 未确认成交 (limit={price:.2f} ref={ref_price:.2f})")
            return None
        except Exception as e:
            self._check_nonce_error(e)
            self.logger.error(f"[Lighter] taker 异常: {e}")
            return None

    def _extract_lighter_fill_price(self, fd: dict, fallback: Decimal):
        """Extract actual average fill price from Lighter WS order data."""
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
                if error is None:
                    return True
                self._check_nonce_error(error)
            except Exception as e:
                self._check_nonce_error(e)
            await asyncio.sleep(0.5)
        return False

    # ═══════════════════════════════════════════════
    #  Lighter WS account_orders (fill confirmation)
    # ═══════════════════════════════════════════════

    async def _lighter_account_ws_loop(self):
        """Subscribe to Lighter account_orders WS for near-instant fill detection."""
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
    #  Extended IOC (V7: 开仓 + 紧急平仓)
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
                actual = self._x_ioc_fill_qty if self._x_ioc_fill_qty > 0 else qty_r
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
            await self._cancel_all_extended_orders()
            await self._do_lighter_cancel_all()
            await asyncio.sleep(0.5)

            if abs(self.extended_position) > 0:
                side = "sell" if self.extended_position > 0 else "buy"
                x_bid, x_ask = self._get_extended_ws_bbo()
                x_ref = x_bid if side == "sell" else x_ask
                if x_ref:
                    self.logger.info(
                        f"[紧急平仓] Extended IOC {side} {abs(self.extended_position)} @ {x_ref}")
                    await self._place_extended_ioc(side, abs(self.extended_position), x_ref)
                else:
                    self.logger.warning("[紧急平仓] Extended BBO 不可用, 尝试 REST 查价")
                    try:
                        x_pos = await self._get_extended_position()
                        if abs(x_pos) > 0:
                            self.logger.warning(
                                f"[紧急平仓] Extended 仍有仓位 {x_pos}, 需手动平仓!")
                    except Exception:
                        pass

            if abs(self.lighter_position) > 0:
                side = "sell" if self.lighter_position > 0 else "buy"
                ls = self.lighter_feed.snapshot if self.lighter_feed else None
                if ls and ls.mid:
                    ref = Decimal(str(ls.best_bid if side == "sell" else ls.best_ask))
                    if ref > 0:
                        self.logger.info(
                            f"[紧急平仓] Lighter IOC {side} {abs(self.lighter_position)} @ {ref}")
                        await self._place_lighter_taker(side, abs(self.lighter_position), ref)
                else:
                    self.logger.warning("[紧急平仓] Lighter 行情不可用, 尝试 REST 查仓")
                    try:
                        l_pos = await self._get_lighter_position()
                        if abs(l_pos) > 0:
                            self.logger.warning(
                                f"[紧急平仓] Lighter 仍有仓位 {l_pos}, 需手动平仓!")
                    except Exception:
                        pass

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
        await self._cancel_all_extended_orders()
        await self._do_lighter_cancel_all()
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
                await self._cancel_all_extended_orders()
            except Exception as e:
                self.logger.warning(f"[退出] Extended 撤单异常: {e}")
            try:
                await self._do_lighter_cancel_all()
            except Exception:
                pass
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
        self.logger.info(
            f"启动 V7 Taker-Open+Maker-Close 套利  {self.symbol}  "
            f"size={self.order_size} max_pos={self.max_position}  "
            f"开仓价差≥{self.target_spread}bps  "
            f"平仓利润缓冲={self.close_profit_bps}bps  "
            f"调价间隔={self.reprice_interval}s  dry_run={self.dry_run}  "
            f"往返费用≈{self.ROUND_TRIP_FEE_BPS:.1f}bps")

        if not self.dry_run:
            self._init_lighter_signer()
            self._get_lighter_market_config()
            self._init_extended_client()
            await self.extended_client.connect()
            await self._get_extended_contract_info()
            await self._warmup_extended()
            self.logger.info("交易客户端就绪")
            self.logger.info("[启动] 清理遗留挂单...")
            await self._cancel_all_extended_orders_rest()
            await self._do_lighter_cancel_all()
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
                            and self.extended_client._ob_last_update_ts > 0 else 999)

                data_stale = l_age > 5.0 or x_age > 5.0

                if data_stale and not self.dry_run:
                    if self._pending and not self._maker_pulled:
                        self.logger.warning("[数据过期] 撤回 Extended 挂单")
                        self._cancel_grace_until = time.time() + 2.0
                        await self._cancel_extended_order(self._pending.order_id)
                        await asyncio.sleep(0.5)
                        if self._pending and self._pending.filled_qty <= self._pending.hedged_qty:
                            self._pending = None
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
                    basis_info = ""
                    if (self.state == State.IN_POSITION
                            and self._open_x_refs and self._open_l_refs
                            and x_ws_bid is not None and l_ask is not None):
                        _x_avg = sum(p * q for p, q in self._open_x_refs) / sum(
                            q for _, q in self._open_x_refs)
                        _l_avg = sum(p * q for p, q in self._open_l_refs) / sum(
                            q for _, q in self._open_l_refs)
                        if self.extended_position > 0:
                            _open_sp = _l_avg - _x_avg
                            _fee = _x_avg * Decimal(str(self.ROUND_TRIP_FEE_BPS)) / Decimal("10000")
                            _prof = _x_avg * Decimal(str(self.close_profit_bps)) / Decimal("10000")
                            _max_gap = _open_sp - _fee - _prof
                            _cur_gap = l_ask - x_ws_bid
                        else:
                            _open_sp = _x_avg - _l_avg
                            _fee = _x_avg * Decimal(str(self.ROUND_TRIP_FEE_BPS)) / Decimal("10000")
                            _prof = _x_avg * Decimal(str(self.close_profit_bps)) / Decimal("10000")
                            _max_gap = _open_sp - _fee - _prof
                            _cur_gap = x_ws_ask - l_bid if x_ws_ask else Decimal("0")
                        _decay_b, _dphase = self._calc_decay_bonus(_fee)
                        _max_gap += _decay_b
                        _hold = now - self.position_opened_at if self.position_opened_at else 0
                        basis_info = (
                            f" gap={float(_cur_gap):.1f}/{float(_max_gap):.1f}"
                            f"({'OK' if _cur_gap <= _max_gap else 'WIDE'}"
                            f"/{_dphase} hold={_hold:.0f}s)")
                    self.logger.info(
                        f"[{self.state.value}] mid={mid_spread_bps:+.2f}bps  "
                        f"X_pos={self.extended_position} L_pos={self.lighter_position}{hold_s}  "
                        f"L={l_bid}/{l_ask} X={x_ws_bid}/{x_ws_ask}{pend}{basis_info}  "
                        f"鲜度:L={l_age:.1f}s X={x_age:.1f}s  "
                        f"累积={self.cumulative_net_bps:+.2f}bps")
                    last_status_log = now

                if (not self.dry_run and now - last_reconcile > 30
                        and self.state in (State.QUOTING, State.IN_POSITION)):
                    await self._reconcile_positions()
                    last_reconcile = now
                    cur_pos = abs(self.extended_position)

                # ━━━━━━ STATE: QUOTING ━━━━━━
                if self.state == State.QUOTING:
                    if x_ws_bid is None or x_ws_ask is None:
                        await asyncio.sleep(self.interval)
                        continue

                    if now < self._loss_breaker_until:
                        await asyncio.sleep(1.0)
                        continue

                    cooldown_remaining = self._post_close_cooldown - (now - self._last_close_time)
                    if self._last_close_time > 0 and cooldown_remaining > 0:
                        await asyncio.sleep(min(cooldown_remaining, 1.0))
                        continue

                    if cur_pos >= self.max_position:
                        await asyncio.sleep(self.interval)
                        continue

                    signal = self._calc_taker_signal(l_bid, l_ask, x_ws_bid, x_ws_ask)
                    if signal and not self.dry_run:
                        t_side, t_dir, x_ref, l_ref_price = signal
                        await self._execute_taker_open(
                            t_side, t_dir, x_ref, l_ref_price)
                    elif self.dry_run and signal:
                        t_side, t_dir, x_ref, l_ref_price = signal
                        spread = float((l_ref_price - x_ref) / x_ref * 10000) \
                            if t_dir == "long_X_short_L" \
                            else float((x_ref - l_ref_price) / l_ref_price * 10000)
                        if now - last_status_log > 10:
                            self.logger.info(
                                f"[DRY-RUN] {t_side} X@{x_ref} L@{l_ref_price} "
                                f"spread={spread:+.1f}bps")

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

                        if direction == "long_X_short_L":
                            hedge_side = "sell"
                            l_ref = Decimal(str(ls.best_bid or ls.mid or 0))
                        else:
                            hedge_side = "buy"
                            l_ref = Decimal(str(ls.best_ask or ls.mid or 0))

                        self.logger.info(
                            f"[HEDGE] Lighter IOC {hedge_side} {hedge_qty} ref={l_ref:.2f}")

                        t0 = time.time()
                        actual_fill = await self._place_lighter_taker(
                            hedge_side, hedge_qty, l_ref)
                        hedge_ms = (time.time() - t0) * 1000

                        if actual_fill is not None:
                            self._pending.hedged_qty += actual_fill
                            self._pending.hedge_qty_needed -= actual_fill
                            x_fill_price = self._pending.avg_price or self._pending.price
                            self._open_x_refs.append((x_fill_price, actual_fill))
                            self._open_l_refs.append((l_ref, actual_fill))

                            if direction == "long_X_short_L":
                                self.extended_position += actual_fill
                            else:
                                self.extended_position -= actual_fill

                            self.logger.info(
                                f"[HEDGE OK] {hedge_ms:.0f}ms fill={actual_fill}/{hedge_qty} "
                                f"X_pos={self.extended_position} L_pos={self.lighter_position}")

                            if self._pending.hedged_qty >= self._pending.filled_qty:
                                if self._pending.filled_qty < self._pending.qty_target:
                                    self.logger.info(
                                        f"[HEDGE OK] 部分成交已对冲, 撤销剩余挂单")
                                    self._cancel_grace_until = time.time() + 2.0
                                    await self._cancel_extended_order(
                                        self._pending.order_id)
                                if self.position_opened_at is None:
                                    self.position_opened_at = time.time()
                                self.position_direction = direction
                                self._csv_row([
                                    datetime.now(timezone.utc).isoformat(),
                                    "OPEN", direction,
                                    self._pending.side, f"{x_fill_price:.2f}",
                                    str(actual_fill),
                                    hedge_side, f"{l_ref:.2f}", str(actual_fill),
                                    f"{mid_spread_bps:.2f}", f"{hedge_ms:.0f}", "", "",
                                ])
                                self._pending = None
                                self.state = State.IN_POSITION
                        else:
                            self.logger.error("[HEDGE FAIL] Lighter 对冲失败, 紧急反向平 Extended")
                            x_side = self._pending.side
                            undo_side = "sell" if x_side == "buy" else "buy"
                            x_ref = x_ws_bid if undo_side == "sell" else x_ws_ask
                            if x_ref:
                                await self._place_extended_ioc(
                                    undo_side, hedge_qty, x_ref)
                            self._pending = None
                            await self._cancel_all_extended_orders_rest()
                            self.state = State.QUOTING
                            last_reconcile = time.time() - 50

                # ━━━━━━ STATE: IN_POSITION ━━━━━━
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
                            self.logger.info(
                                f"[单边仓位] 对账后恢复正常: "
                                f"X={self.extended_position} L={self.lighter_position}")
                            last_reconcile = now

                    if x_ws_bid is not None and x_ws_ask is not None and not self.dry_run:
                        close_target = self._calc_close_maker_target(
                            l_bid, l_ask, x_ws_bid, x_ws_ask)
                        if close_target:
                            c_side, c_price = close_target
                            close_qty = abs(self.extended_position)
                            if close_qty > 0 and now >= self._maker_pull_until:
                                await self._update_maker(
                                    c_side, self.position_direction or "",
                                    c_price, close_qty, "close")
                        elif self._pending and self._pending.phase == "close":
                            if self._pending.filled_qty <= self._pending.hedged_qty:
                                self.logger.info(
                                    f"[PULL] basis too wide, cancelling close maker "
                                    f"id={self._pending.order_id}")
                                self._cancel_grace_until = time.time() + 2.0
                                await self._cancel_extended_order(
                                    self._pending.order_id)
                                await asyncio.sleep(0.1)
                                if (self._pending
                                        and self._pending.filled_qty
                                        <= self._pending.hedged_qty):
                                    self._pending = None
                                self._maker_pull_until = time.time() + 0.5

                # ━━━━━━ STATE: CLOSING_HEDGE ━━━━━━
                elif self.state == State.CLOSING_HEDGE:
                    if self._hedge_lock.locked():
                        await asyncio.sleep(0.01)
                        continue
                    async with self._hedge_lock:
                        if not (self._pending and self._pending.hedge_qty_needed > 0):
                            continue
                        hedge_qty = self._pending.hedge_qty_needed
                        x_side = self._pending.side
                        hedge_side = "sell" if x_side == "buy" else "buy"

                        if hedge_side == "sell":
                            l_ref = Decimal(str(ls.best_bid or ls.mid or 0))
                        else:
                            l_ref = Decimal(str(ls.best_ask or ls.mid or 0))

                        self.logger.info(
                            f"[CLOSE_HEDGE] Lighter IOC {hedge_side} {hedge_qty}")

                        t0 = time.time()
                        actual_fill = await self._place_lighter_taker(
                            hedge_side, hedge_qty, l_ref)
                        hedge_ms = (time.time() - t0) * 1000

                        if actual_fill is not None:
                            self._pending.hedged_qty += actual_fill
                            self._pending.hedge_qty_needed -= actual_fill
                            x_fill_price = self._pending.avg_price or self._pending.price
                            l_actual_close = self._last_lighter_avg_price or l_ref

                            if x_side == "buy":
                                self.extended_position += actual_fill
                            else:
                                self.extended_position -= actual_fill

                            net_bps = self._log_ref_pnl(x_fill_price, l_actual_close)
                            self.cumulative_net_bps += net_bps
                            self.logger.info(
                                f"[平仓完成] {hedge_ms:.0f}ms 净利={net_bps:+.2f}bps "
                                f"累积={self.cumulative_net_bps:+.2f}bps")
                            self._csv_row([
                                datetime.now(timezone.utc).isoformat(),
                                "CLOSE", "convergence",
                                self._pending.side, f"{x_fill_price:.2f}",
                                str(actual_fill),
                                hedge_side, f"{l_actual_close:.2f}", str(actual_fill),
                                f"{mid_spread_bps:.2f}", f"{hedge_ms:.0f}",
                                f"{net_bps:.2f}", f"{self.cumulative_net_bps:.2f}",
                            ])

                            if self._pending.hedged_qty >= self._pending.filled_qty:
                                if self._pending.filled_qty < self._pending.qty_target:
                                    self.logger.info(
                                        "[CLOSE_HEDGE] 部分成交已对冲, 撤销剩余挂单")
                                    self._cancel_grace_until = time.time() + 2.0
                                    await self._cancel_extended_order(
                                        self._pending.order_id)
                                self._pending = None
                                pos_remaining = abs(self.extended_position)
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
                                            f"[熔断] 连亏={self._consecutive_losses} "
                                            f"累积={self.cumulative_net_bps:+.2f}bps")
                                    self._reset_position_state()
                                    self.state = State.QUOTING
                                else:
                                    self.state = State.IN_POSITION
                                last_reconcile = time.time() - 50
                            else:
                                self.logger.info(
                                    f"[CLOSE_HEDGE] 部分对冲, 剩余={self._pending.hedge_qty_needed}")
                        else:
                            self.logger.error("[CLOSE_HEDGE FAIL] 紧急平仓")
                            await self._emergency_flatten()
                            self._pending = None
                            self._reset_position_state()
                            self.state = State.QUOTING
                            last_reconcile = time.time() - 50

                if self.state == State.QUOTING:
                    await asyncio.sleep(self.interval)
                elif self.state == State.IN_POSITION:
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

    def _log_ref_pnl(self, close_x_ref: Decimal, close_l_ref: Decimal) -> float:
        if not self._open_x_refs or not self._open_l_refs:
            return 0.0
        x_open_cost = sum(p * q for p, q in self._open_x_refs)
        x_open_qty = sum(q for _, q in self._open_x_refs)
        l_open_cost = sum(p * q for p, q in self._open_l_refs)
        l_open_qty = sum(q for _, q in self._open_l_refs)

        if self.position_direction == "long_X_short_L":
            x_pnl = float(close_x_ref * x_open_qty - x_open_cost)
            l_pnl = float(l_open_cost - close_l_ref * l_open_qty)
        else:
            x_pnl = float(x_open_cost - close_x_ref * x_open_qty)
            l_pnl = float(close_l_ref * l_open_qty - l_open_cost)

        total_gross = x_pnl + l_pnl
        notional = float(max(x_open_qty, l_open_qty)) * float(close_x_ref)
        fee_cost = notional * self.ROUND_TRIP_FEE_BPS / 10000
        total_net = total_gross - fee_cost
        gross_bps = total_gross / notional * 10000 if notional > 0 else 0.0
        net_bps = total_net / notional * 10000 if notional > 0 else 0.0
        self.logger.info(
            f"[P&L] X={x_pnl:+.3f}$ L={l_pnl:+.3f}$ "
            f"毛利={total_gross:+.3f}$({gross_bps:+.1f}bps) "
            f"手续费={fee_cost:.3f}$ "
            f"净利={total_net:+.3f}$({net_bps:+.1f}bps)")
        return net_bps

    def _reset_position_state(self):
        self.position_opened_at = None
        self.position_direction = None
        self._open_x_refs.clear()
        self._open_l_refs.clear()
        self._pending = None


def main():
    p = argparse.ArgumentParser(
        description="V7 开仓Taker + 平仓Maker 混合套利 (Extended taker开仓)")
    p.add_argument("-s", "--symbol", default="ETH")
    p.add_argument("--size", type=str, default="0.1")
    p.add_argument("--max-position", type=str, default="0.5")
    p.add_argument("--target-spread", type=float, default=3.5,
                   help="开仓最低利润目标 (bps, 需覆盖2.5bps taker费)")
    p.add_argument("--close-profit-bps", type=float, default=0.0,
                   help="平仓额外利润缓冲 (bps, 条件maker已内置盈亏门槛)")
    p.add_argument("--max-hold-sec", type=float, default=600.0)
    p.add_argument("--cooldown", type=float, default=15.0,
                   help="平仓后冷却 (秒)")
    p.add_argument("--loss-breaker-bps", type=float, default=-15.0)
    p.add_argument("--loss-breaker-sec", type=float, default=300.0)
    p.add_argument("--interval", type=float, default=0.1,
                   help="主循环间隔 (秒)")
    p.add_argument("--reprice-interval", type=float, default=1.5,
                   help="最小调价间隔 (秒, 较短=对basis变化反应更快)")
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
        close_profit_bps=args.close_profit_bps,
        max_hold_sec=args.max_hold_sec,
        interval=args.interval,
        reprice_interval=args.reprice_interval,
        dry_run=args.dry_run,
    )
    bot._post_close_cooldown = args.cooldown
    bot._loss_breaker_threshold = args.loss_breaker_bps
    bot._loss_breaker_duration = args.loss_breaker_sec

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
