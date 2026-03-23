#!/usr/bin/env python3
"""
Extended + Lighter 价差收敛套利 V3（Maker + Hedge 模式）

策略：
  检测到价差偏离 → 在 Extended 挂 POST_ONLY maker 单（0 费用）
  → Extended 成交后立刻在 Lighter taker 对冲（0 费用）
  → 价差收敛时反向操作平仓（同样 maker + hedge）
  往返总费用: 0 bps (Extended maker 0% + Lighter taker 0%)

与 V2 的区别：
  V2: 双边 taker（5 bps 费用），适合大价差快速执行
  V3: maker + hedge（0 bps 费用），适合小价差高频捕获

数据源：
  Lighter → WebSocket 实时深度订单簿
  Extended → ExtendedClient WebSocket 实时 BBO + 订单状态推送

用法:
  python extended_lighter_arb_v3.py --symbol ETH --size 0.1 --open-bps 3 --close-bps 0.5
  python extended_lighter_arb_v3.py --symbol ETH --dry-run
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

import requests

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
    MAKING = "MAKING"
    HEDGING = "HEDGING"
    IN_POSITION = "IN_POSITION"
    CLOSING_MAKE = "CLOSING_MAKE"
    CLOSING_HEDGE = "CLOSING_HEDGE"


@dataclass
class PendingMaker:
    order_id: str
    side: str           # "buy" or "sell"
    direction: str      # "long_X_short_L" or "long_L_short_X"
    qty_target: Decimal
    price: Decimal
    placed_at: float
    deadline: float
    filled_qty: Decimal = Decimal("0")
    hedged_qty: Decimal = Decimal("0")
    hedge_qty_needed: Decimal = Decimal("0")
    avg_price: Optional[Decimal] = None
    phase: str = "open"  # "open" or "close"


@dataclass
class FillEvent:
    order_id: str
    status: str
    filled_qty: Decimal
    avg_price: Optional[Decimal]
    side: str
    raw: Dict


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

class MakerHedgeArbBot:

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
        self._fill_queue: asyncio.Queue = asyncio.Queue(maxsize=200)

        self._x_ws_bbo_ts: float = 0.0
        self._x_ws_last_ob_id: int = 0
        self.STALE_THRESHOLD_SEC: float = 5.0

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
        self.csv_path = f"logs/arb_v3_{self.symbol}_trades.csv"
        self.log_path = f"logs/arb_v3_{self.symbol}.log"
        self._init_csv()
        self.logger = self._init_logger()

    # ═══════════════════════════════════════════════
    #  Logging / CSV
    # ═══════════════════════════════════════════════

    def _init_logger(self) -> logging.Logger:
        lg = logging.getLogger(f"arb_v3_{self.symbol}")
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
    #  Client Initialization (reused from V2)
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
        self.logger.info("Extended client 已初始化")

    async def _get_extended_contract_info(self):
        cid, tick = await self.extended_client.get_contract_attributes()
        self.extended_contract_id = cid
        self.extended_tick_size = tick
        if hasattr(self.extended_client, "min_order_size"):
            self.extended_min_order_size = self.extended_client.min_order_size
        self.logger.info(f"Extended contract: {cid}  tick={tick}")

    # ═══════════════════════════════════════════════
    #  Extended WS BBO + Fill pipeline
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

    def _on_extended_order_event(self, event: dict):
        """Called by ExtendedClient.handle_account in WS coroutine."""
        try:
            fe = FillEvent(
                order_id=str(event.get('order_id', '')),
                status=event.get('status', ''),
                filled_qty=Decimal(str(event.get('filled_size', '0'))),
                avg_price=Decimal(str(event['avg_price'])) if event.get('avg_price') else None,
                side=event.get('side', ''),
                raw=event,
            )
            self._fill_queue.put_nowait(fe)
        except asyncio.QueueFull:
            pass
        except Exception:
            pass

    def _drain_fill_events(self):
        """Non-blocking: consume fill events and update pending maker state."""
        while not self._fill_queue.empty():
            try:
                fe: FillEvent = self._fill_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            if self._pending is None or fe.order_id != self._pending.order_id:
                continue

            if fe.status in ('FILLED', 'PARTIALLY_FILLED'):
                old_filled = self._pending.filled_qty
                new_filled = fe.filled_qty
                if new_filled > old_filled:
                    self._pending.filled_qty = new_filled
                    if fe.avg_price:
                        self._pending.avg_price = fe.avg_price

                delta = new_filled - self._pending.hedged_qty
                if fe.status == 'FILLED' and delta > 0:
                    self._pending.hedge_qty_needed = delta
                    if self._pending.phase == "open":
                        self.state = State.HEDGING
                    else:
                        self.state = State.CLOSING_HEDGE
                    self.logger.info(
                        f"[FILL] Extended {self._pending.side} 完全成交 "
                        f"filled={new_filled} 待对冲={delta}")
                elif fe.status == 'PARTIALLY_FILLED' and delta >= self.extended_min_order_size:
                    self._pending.hedge_qty_needed = delta
                    if self._pending.phase == "open":
                        self.state = State.HEDGING
                    else:
                        self.state = State.CLOSING_HEDGE
                    self.logger.info(
                        f"[PARTIAL_FILL] Extended {self._pending.side} 部分成交 "
                        f"filled={new_filled} delta={delta}")

            elif fe.status == 'CANCELED':
                filled = self._pending.filled_qty
                hedged = self._pending.hedged_qty
                if filled > hedged and filled - hedged >= self.extended_min_order_size:
                    self._pending.hedge_qty_needed = filled - hedged
                    if self._pending.phase == "open":
                        self.state = State.HEDGING
                    else:
                        self.state = State.CLOSING_HEDGE
                    self.logger.info(
                        f"[CANCEL+FILL] Extended 撤单但有部分成交 "
                        f"filled={filled} hedged={hedged}")
                else:
                    self.logger.info(f"[CANCEL] Extended 撤单无需对冲")
                    if self.state in (State.MAKING, State.CLOSING_MAKE):
                        if self._pending.phase == "close":
                            self.state = State.IN_POSITION
                        else:
                            self.state = State.IDLE
                        self._pending = None

    # ═══════════════════════════════════════════════
    #  Position Queries (REST)
    # ═══════════════════════════════════════════════

    async def _get_lighter_position(self) -> Decimal:
        url = f"{self.lighter_base_url}/api/v1/account"
        params = {"by": "index", "value": self.account_index}
        try:
            resp = requests.get(url, headers={"accept": "application/json"},
                                params=params, timeout=10)
            resp.raise_for_status()
            for pos in resp.json().get("accounts", [{}])[0].get("positions", []):
                if pos.get("symbol") == self.symbol:
                    return Decimal(pos["position"]) * pos["sign"]
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
    #  Order Execution
    # ═══════════════════════════════════════════════

    def _round_lighter_price(self, price: Decimal) -> Decimal:
        if self.lighter_tick > 0:
            return (price / self.lighter_tick).quantize(Decimal("1")) * self.lighter_tick
        return price

    async def _place_lighter_taker(self, side: str, qty: Decimal, ref_price: Decimal) -> bool:
        is_ask = side.lower() == "sell"
        if is_ask:
            price = self._round_lighter_price(ref_price * Decimal("0.995"))
        else:
            price = self._round_lighter_price(ref_price * Decimal("1.005"))

        client_order_index = int(time.time() * 1000)
        try:
            _, _, error = await self.lighter_client.create_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=int(qty * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=False,
                trigger_price=0,
            )
            if error is not None:
                self.logger.error(f"[Lighter] 下单失败: {error}")
                return False
            self.logger.info(f"[Lighter] taker {side} {qty} ref={ref_price:.2f} limit={price:.2f}")
            return True
        except Exception as e:
            self.logger.error(f"[Lighter] 下单异常: {e}")
            return False

    async def _place_extended_maker(self, side: str, qty: Decimal) -> Optional[str]:
        """Place POST_ONLY maker on Extended. Returns order_id or None."""
        x_bid, x_ask = self._get_extended_ws_bbo()
        if x_bid is None or x_ask is None:
            self.logger.error("[Extended] WS BBO 不可用")
            return None

        spread_ticks = (x_ask - x_bid) / self.extended_tick_size
        if side == "buy":
            if spread_ticks > 1:
                price = x_bid + self.extended_tick_size
            else:
                price = x_bid
        else:
            if spread_ticks > 1:
                price = x_ask - self.extended_tick_size
            else:
                price = x_ask

        rounded_price = self.extended_client.round_to_tick(price)

        if side == "buy" and rounded_price >= x_ask:
            rounded_price = x_ask - self.extended_tick_size
            self.logger.warning(f"[Extended] maker buy 价格调整以避免穿越: {rounded_price}")
        elif side == "sell" and rounded_price <= x_bid:
            rounded_price = x_bid + self.extended_tick_size
            self.logger.warning(f"[Extended] maker sell 价格调整以避免穿越: {rounded_price}")

        order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
        qty_rounded = qty.quantize(self.extended_min_order_size, rounding=ROUND_HALF_UP)

        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                result = await self.extended_client.perpetual_trading_client.place_order(
                    market_name=self.extended_contract_id,
                    amount_of_synthetic=qty_rounded,
                    price=rounded_price,
                    side=order_side,
                    time_in_force=TimeInForce.GTT,
                    post_only=True,
                    expire_time=_utc_now() + timedelta(seconds=self.maker_timeout + 30),
                )
                if not result or not result.data or result.status != 'OK':
                    err_str = str(result) if result else "None"
                    if "post-only" in err_str.lower() or "contradict" in err_str.lower():
                        if side == "buy":
                            rounded_price -= self.extended_tick_size
                        else:
                            rounded_price += self.extended_tick_size
                        self.logger.warning(
                            f"[Extended] POST_ONLY拒绝(第{attempt+1}次), 回退价格到 {rounded_price}")
                        continue
                    self.logger.error(f"[Extended] maker 下单失败: {result}")
                    return None

                order_id = result.data.id
                if not order_id:
                    return None

                self.logger.info(
                    f"[Extended] maker {side} {qty_rounded} @ {rounded_price}  "
                    f"id={order_id}  spread={spread_ticks:.0f}tick")
                return str(order_id)

            except Exception as e:
                err_str = str(e).lower()
                if ("post-only" in err_str or "contradict" in err_str) and attempt < max_attempts - 1:
                    if side == "buy":
                        rounded_price -= self.extended_tick_size
                    else:
                        rounded_price += self.extended_tick_size
                    self.logger.warning(
                        f"[Extended] POST_ONLY异常(第{attempt+1}次), 回退价格到 {rounded_price}")
                    continue
                self.logger.error(f"[Extended] maker 下单异常: {e}")
                return None
        self.logger.error(f"[Extended] maker 下单 {max_attempts} 次均被拒绝")
        return None

    async def _place_extended_ioc(self, side: str, qty: Decimal, ref_price: Decimal) -> bool:
        """Emergency IOC on Extended to flatten a single-leg exposure."""
        if side == "buy":
            aggressive = ref_price * Decimal("1.005")
        else:
            aggressive = ref_price * Decimal("0.995")
        aggressive = aggressive.quantize(self.extended_tick_size, rounding=ROUND_HALF_UP)
        qty_rounded = qty.quantize(self.extended_min_order_size, rounding=ROUND_HALF_UP)
        try:
            result = await self.extended_client.place_ioc_order(
                contract_id=self.extended_contract_id,
                quantity=qty_rounded,
                side=side,
                aggressive_price=aggressive,
            )
            if result.success:
                self.logger.info(f"[Extended] IOC {side} {qty_rounded} @ {aggressive}  id={result.order_id}")
                return True
            self.logger.error(f"[Extended] IOC 失败: {result.error_message}")
            return False
        except Exception as e:
            self.logger.error(f"[Extended] IOC 异常: {e}")
            return False

    async def _cancel_pending_maker(self):
        """Cancel the pending Extended maker order and handle partial fills."""
        if not self._pending:
            return
        oid = self._pending.order_id
        try:
            result = await self.extended_client.perpetual_trading_client.orders.cancel_order(oid)
            self.logger.info(f"[Extended] 撤单 {oid}")
            await asyncio.sleep(0.3)
            self._drain_fill_events()
        except Exception as e:
            self.logger.error(f"[Extended] 撤单异常: {e}")

    # ═══════════════════════════════════════════════
    #  Shutdown
    # ═══════════════════════════════════════════════

    async def _shutdown(self):
        self.stop_flag = True
        self.logger.info("关闭中...")
        if self._pending:
            try:
                await self._cancel_pending_maker()
            except Exception:
                pass
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
                    self._drain_fill_events()
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

    async def _emergency_flatten(self):
        """Emergency close all positions with IOC orders."""
        try:
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
    #  Main loop
    # ═══════════════════════════════════════════════

    async def _main_loop_wrapper(self):
        self.logger.info(
            f"启动 V3 Maker+Hedge 套利  {self.symbol}  "
            f"size={self.order_size} max_pos={self.max_position}  "
            f"开仓≥{self.open_bps}bps 平仓≤{self.close_bps}bps  "
            f"maker超时={self.maker_timeout}s  dry_run={self.dry_run}")

        if not self.dry_run:
            self._init_lighter_signer()
            self._get_lighter_market_config()
            self._init_extended_client()
            await self.extended_client.connect()
            await self._get_extended_contract_info()
            self.extended_client.setup_order_update_handler(self._on_extended_order_event)
            self.logger.info("交易客户端就绪 (含WS订单推送)")
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
                self._drain_fill_events()

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
                            f"[数据过期] {' '.join(stale_parts)} > {self.STALE_THRESHOLD_SEC}s 阈值，"
                            f"跳过新开仓/平仓判断 (当前状态={self.state.value})")
                        last_status_log = now
                    if self.state not in (State.HEDGING, State.CLOSING_HEDGE):
                        await asyncio.sleep(self.interval)
                        continue

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

                if now - last_status_log > 15:
                    hold_s = ""
                    if self.position_opened_at:
                        hold_s = f" hold={now - self.position_opened_at:.0f}s"
                    pending_str = ""
                    if self._pending:
                        wait = now - self._pending.placed_at
                        pending_str = f" maker_wait={wait:.0f}s filled={self._pending.filled_qty}"
                    self.logger.info(
                        f"[{self.state.value}] mid={mid_spread_bps:+.2f}bps "
                        f"vwap={vwap_spread_bps:+.2f}bps  "
                        f"L_pos={self.lighter_position} X_pos={self.extended_position}{hold_s}  "
                        f"L_mid={l_mid:.2f} X_mid={x_mid:.2f}  "
                        f"鲜度:L={l_age:.1f}s X={x_age:.1f}s{pending_str}  "
                        f"累积净利={self.cumulative_net_bps:+.2f}bps")
                    last_status_log = now

                if (not self.dry_run
                        and now - last_reconcile > 60
                        and self.state in (State.IDLE, State.IN_POSITION)):
                    await self._reconcile_positions()
                    last_reconcile = now
                    cur_pos = abs(self.lighter_position)

                # ────── STATE: IDLE ──────
                if self.state == State.IDLE:
                    if (vwap_direction
                            and not data_stale
                            and vwap_spread_bps >= self.open_bps
                            and cur_pos < self.max_position):
                        direction = vwap_direction
                        if direction == "long_X_short_L":
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
                            f"Extended maker {maker_side}")

                        if self.dry_run:
                            self.logger.info("[DRY-RUN] 模拟 maker 下单")
                            await asyncio.sleep(self.interval)
                            continue

                        order_id = await self._place_extended_maker(maker_side, self.order_size)
                        if order_id:
                            x_ref = x_ws_bid if maker_side == "buy" else x_ws_ask
                            t_placed = time.time()
                            self._pending = PendingMaker(
                                order_id=order_id,
                                side=maker_side,
                                direction=direction,
                                qty_target=self.order_size,
                                price=x_ref or x_mid,
                                placed_at=t_placed,
                                deadline=t_placed + self.maker_timeout,
                                phase="open",
                            )
                            self._last_open_spread = vwap_spread_bps
                            self.state = State.MAKING
                            self.logger.info(
                                f"[MAKING] 等待 Extended {maker_side} 成交  "
                                f"超时={self.maker_timeout}s")
                        else:
                            self.logger.warning("[MAKING] Extended 下单失败，回到 IDLE")

                # ────── STATE: MAKING ──────
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
                                f"{self.open_bps*0.5:.1f}bps (等了{wait_sec:.0f}s)")

                    if should_cancel and self._pending:
                        self.logger.info(f"[撤单] {cancel_reason}")
                        await self._cancel_pending_maker()
                        await asyncio.sleep(0.3)
                        self._drain_fill_events()
                        if self.state == State.MAKING:
                            self._pending = None
                            self.state = State.IDLE

                # ────── STATE: HEDGING ──────
                elif self.state == State.HEDGING:
                    if self._pending and self._pending.hedge_qty_needed > 0:
                        hedge_qty = self._pending.hedge_qty_needed
                        direction = self._pending.direction

                        if direction == "long_X_short_L":
                            hedge_side = "sell"
                            l_ref = Decimal(str(ls.best_bid or ls.mid))
                        else:
                            hedge_side = "buy"
                            l_ref = Decimal(str(ls.best_ask or ls.mid))

                        self.logger.info(
                            f"[HEDGE] Lighter taker {hedge_side} {hedge_qty}  "
                            f"ref={l_ref:.2f}")

                        t0 = time.time()
                        ok = await self._place_lighter_taker(hedge_side, hedge_qty, l_ref)
                        hedge_ms = (time.time() - t0) * 1000

                        if ok:
                            self._pending.hedged_qty += hedge_qty
                            x_fill_price = self._pending.avg_price or self._pending.price
                            self._open_l_refs.append((l_ref, hedge_qty))
                            self._open_x_refs.append((x_fill_price, hedge_qty))

                            if direction == "long_X_short_L":
                                self.extended_position += hedge_qty
                                self.lighter_position -= hedge_qty
                            else:
                                self.extended_position -= hedge_qty
                                self.lighter_position += hedge_qty

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
                                    hedge_side, f"{l_ref:.2f}", str(hedge_qty),
                                    self._pending.side, f"{x_fill_price:.2f}",
                                    str(self._pending.filled_qty),
                                    f"{vwap_spread_bps:.2f}",
                                    f"{maker_wait_ms:.0f}", f"{hedge_ms:.0f}",
                                    "", "", "",
                                ])
                                self._pending = None
                            else:
                                self.state = State.MAKING
                        else:
                            self.logger.error("[HEDGE FAIL] Lighter 对冲失败! 紧急平 Extended")
                            x_reverse = "sell" if self._pending.side == "buy" else "buy"
                            x_bid, x_ask = self._get_extended_ws_bbo()
                            x_ref = x_bid if x_reverse == "sell" else x_ask
                            if x_ref:
                                await self._place_extended_ioc(
                                    x_reverse, hedge_qty, x_ref)
                            self._pending = None
                            self.state = State.IDLE
                            last_reconcile = time.time() - 50

                # ────── STATE: IN_POSITION ──────
                elif self.state == State.IN_POSITION:
                    if not data_stale and abs_mid_spread <= self.close_bps:
                        self.logger.info(
                            f"[平仓信号] mid价差收敛到 {mid_spread_bps:+.2f} bps")

                        if self.dry_run:
                            self._reset_position_state()
                            self.state = State.IDLE
                            continue

                        if self.extended_position > 0:
                            close_side = "sell"
                        elif self.extended_position < 0:
                            close_side = "buy"
                        else:
                            self._reset_position_state()
                            self.state = State.IDLE
                            continue

                        close_qty = abs(self.extended_position)
                        order_id = await self._place_extended_maker(close_side, close_qty)
                        if order_id:
                            x_ref = x_ws_bid if close_side == "sell" else x_ws_ask
                            t_placed = time.time()
                            self._pending = PendingMaker(
                                order_id=order_id,
                                side=close_side,
                                direction=self.position_direction or "",
                                qty_target=close_qty,
                                price=x_ref or x_mid,
                                placed_at=t_placed,
                                deadline=t_placed + self.maker_timeout,
                                phase="close",
                            )
                            self.state = State.CLOSING_MAKE
                            self.logger.info(
                                f"[CLOSING_MAKE] Extended maker {close_side} {close_qty}")
                        else:
                            self.logger.warning("[CLOSING] Extended maker 失败")

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
                            if direction == "long_X_short_L":
                                maker_side = "buy"
                            else:
                                maker_side = "sell"
                            self.logger.info(
                                f"[加仓信号] VWAP={vwap_spread_bps:+.2f}bps "
                                f"当前={cur_pos} 加 {self.order_size}")

                            if not self.dry_run:
                                order_id = await self._place_extended_maker(
                                    maker_side, self.order_size)
                                if order_id:
                                    x_ref = x_ws_bid if maker_side == "buy" else x_ws_ask
                                    t_placed = time.time()
                                    self._pending = PendingMaker(
                                        order_id=order_id,
                                        side=maker_side,
                                        direction=direction,
                                        qty_target=self.order_size,
                                        price=x_ref or x_mid,
                                        placed_at=t_placed,
                                        deadline=t_placed + self.maker_timeout,
                                        phase="open",
                                    )
                                    self._last_open_spread = vwap_spread_bps
                                    self.state = State.MAKING

                # ────── STATE: CLOSING_MAKE ──────
                elif self.state == State.CLOSING_MAKE:
                    if self._pending and now > self._pending.deadline:
                        self.logger.info(
                            f"[超时] 平仓 maker 等待 {self.maker_timeout}s 未成交，撤单")
                        await self._cancel_pending_maker()
                        await asyncio.sleep(0.5)
                        self._drain_fill_events()
                        if self.state == State.CLOSING_MAKE:
                            self._pending = None
                            self.state = State.IN_POSITION

                # ────── STATE: CLOSING_HEDGE ──────
                elif self.state == State.CLOSING_HEDGE:
                    if self._pending and self._pending.hedge_qty_needed > 0:
                        hedge_qty = self._pending.hedge_qty_needed
                        if self.lighter_position > 0:
                            hedge_side = "sell"
                            l_ref = Decimal(str(ls.best_bid or ls.mid))
                        else:
                            hedge_side = "buy"
                            l_ref = Decimal(str(ls.best_ask or ls.mid))

                        self.logger.info(
                            f"[CLOSE_HEDGE] Lighter taker {hedge_side} {hedge_qty}")

                        t0 = time.time()
                        ok = await self._place_lighter_taker(hedge_side, hedge_qty, l_ref)
                        hedge_ms = (time.time() - t0) * 1000

                        if ok:
                            self._pending.hedged_qty += hedge_qty
                            x_fill_price = self._pending.avg_price or self._pending.price

                            if self.lighter_position > 0:
                                self.lighter_position -= hedge_qty
                            else:
                                self.lighter_position += hedge_qty
                            if self.extended_position > 0:
                                self.extended_position -= hedge_qty
                            else:
                                self.extended_position += hedge_qty

                            net_bps = self._log_ref_pnl(l_ref, x_fill_price,
                                                        hedge_side,
                                                        self._pending.side)
                            self.cumulative_net_bps += net_bps

                            self.logger.info(
                                f"[平仓完成] hedge耗时={hedge_ms:.0f}ms  "
                                f"净利={net_bps:+.2f}bps (0费用)  "
                                f"累积={self.cumulative_net_bps:+.2f}bps  "
                                f"L_pos={self.lighter_position} X_pos={self.extended_position}")

                            self._csv_row([
                                datetime.now(timezone.utc).isoformat(),
                                "CLOSE", "convergence",
                                hedge_side, f"{l_ref:.2f}", str(hedge_qty),
                                self._pending.side, f"{x_fill_price:.2f}",
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

                await asyncio.sleep(self.interval)

            except Exception as e:
                self.logger.error(f"主循环异常: {e}")
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(2)

    def _log_ref_pnl(self, close_l_ref: Decimal, close_x_ref: Decimal,
                     l_close_side: str, x_close_side: str) -> float:
        if not self._open_l_refs or not self._open_x_refs:
            return 0.0
        l_open_cost = sum(p * q for p, q in self._open_l_refs)
        l_open_qty = sum(q for _, q in self._open_l_refs)
        x_open_cost = sum(p * q for p, q in self._open_x_refs)
        x_open_qty = sum(q for _, q in self._open_x_refs)

        if self.position_direction == "long_X_short_L":
            l_pnl = float(l_open_cost - close_l_ref * l_open_qty)
            x_pnl = float(close_x_ref * x_open_qty - x_open_cost)
        else:
            l_pnl = float(close_l_ref * l_open_qty - l_open_cost)
            x_pnl = float(x_open_cost - close_x_ref * x_open_qty)

        total = l_pnl + x_pnl
        notional = float(max(l_open_qty, x_open_qty)) * float(close_l_ref)
        net_bps = total / notional * 10000 if notional > 0 else 0.0

        self.logger.info(
            f"[P&L估算] Lighter={l_pnl:+.3f}$ Extended={x_pnl:+.3f}$ "
            f"费用=0$ 净={total:+.3f}$ ({net_bps:+.1f}bps)")
        return net_bps

    def _reset_position_state(self):
        self.position_opened_at = None
        self.position_direction = None
        self._last_open_spread = 0.0
        self._open_l_refs.clear()
        self._open_x_refs.clear()


def main():
    p = argparse.ArgumentParser(
        description="Extended + Lighter V3 Maker+Hedge 套利 (0费用)")
    p.add_argument("-s", "--symbol", default="ETH")
    p.add_argument("--size", type=str, default="0.1", help="每笔数量")
    p.add_argument("--max-position", type=str, default="0.5", help="最大累积仓位")
    p.add_argument("--open-bps", type=float, default=3.0,
                   help="开仓 VWAP 价差阈值 (bps)")
    p.add_argument("--close-bps", type=float, default=0.5,
                   help="平仓 |mid价差| 阈值 (bps)")
    p.add_argument("--max-hold-sec", type=float, default=600.0,
                   help="最大持仓秒数")
    p.add_argument("--interval", type=float, default=0.5,
                   help="主循环间隔 (秒)")
    p.add_argument("--maker-timeout", type=float, default=10.0,
                   help="Extended maker 最大等待秒数")
    p.add_argument("--dry-run", action="store_true", help="只看信号不下单")
    p.add_argument("--env-file", default=".env", help=".env 文件路径")
    args = p.parse_args()

    load_dotenv(args.env_file)

    if args.close_bps >= args.open_bps:
        args.close_bps = max(0.0, args.open_bps - 0.5)

    bot = MakerHedgeArbBot(
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
