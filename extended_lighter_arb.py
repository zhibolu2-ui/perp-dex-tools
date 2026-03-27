#!/usr/bin/env python3
"""
Extended + Lighter 价差收敛套利（双 taker，实盘）

策略：
  VWAP 深度加权价差 >= 开仓阈值 → 两所同时 taker 开仓（便宜所买 + 贵所卖）
  |Mid 价差| <= 平仓阈值 → 一次性全部平仓
  支持累积加仓（到 --max-position 封顶），全平时一次性清零。

  数据源：
    Lighter → WebSocket 实时深度订单簿 (用于 VWAP 计算)
    Extended → ExtendedClient WebSocket 实时 BBO (depth=1, 用于信号和执行)
              ExtendedFeed REST 轮询仅作为备用/dry-run数据源

用法:
  python extended_lighter_arb.py --symbol ETH --size 0.1 --open-bps 5 --close-bps 1
  python extended_lighter_arb.py --symbol ETH --dry-run
  python extended_lighter_arb.py --symbol ETH --size 0.1 --max-position 0.5 --max-hold-sec 300
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
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import websockets

from dotenv import load_dotenv

from lighter.signer_client import SignerClient
from x10.perpetual.orders import TimeInForce, OrderSide

sys.path.insert(0, str(Path(__file__).resolve().parent))

from exchanges.extended import ExtendedClient
from feeds.lighter_feed import LighterFeed
from feeds.existing_feeds import ExtendedFeed

STATE_IDLE = "IDLE"
STATE_OPENING = "OPENING"
STATE_IN_POSITION = "IN_POSITION"
STATE_CLOSING = "CLOSING"


class _Config:
    """Minimal config wrapper expected by ExtendedClient."""

    def __init__(self, d: dict):
        for k, v in d.items():
            setattr(self, k, v)


class ConvergenceArbBot:

    def __init__(
        self,
        symbol: str,
        order_size: Decimal,
        max_position: Decimal,
        open_bps: float,
        close_bps: float,
        max_hold_sec: float,
        interval: float,
        dry_run: bool,
        exec_buffer_bps: float = 0.0,
    ):
        self.symbol = symbol.upper()
        self.order_size = order_size
        self.max_position = max_position
        self.open_bps = open_bps
        self.close_bps = close_bps
        self.max_hold_sec = max_hold_sec
        self.interval = interval
        self.dry_run = dry_run
        self.exec_buffer_bps = exec_buffer_bps
        self.effective_open_bps = open_bps + exec_buffer_bps

        self.state = STATE_IDLE
        self.stop_flag = False

        # cumulative position (signed: +long Lighter / -short Lighter)
        self.lighter_position = Decimal("0")
        self.extended_position = Decimal("0")
        self.position_opened_at: Optional[float] = None
        self.position_direction: Optional[str] = None  # "long_L_short_X" or "long_X_short_L"
        self._open_l_refs: List[Tuple[Decimal, Decimal]] = []  # (ref_price, qty)
        self._open_x_refs: List[Tuple[Decimal, Decimal]] = []
        self._last_extended_trade_ts: float = 0.0  # for add-cooldown after consuming OB depth

        # clients
        self.lighter_client: Optional[SignerClient] = None
        self.extended_client: Optional[ExtendedClient] = None

        # feeds (read-only orderbook)
        self.lighter_feed: Optional[LighterFeed] = None
        self.extended_feed: Optional[ExtendedFeed] = None

        # Lighter market config
        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
        self.api_key_index = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))
        self.lighter_market_index: int = 0
        self.base_amount_multiplier: int = 0
        self.price_multiplier: int = 0
        self.lighter_tick: Decimal = Decimal("0.01")

        # Extended config
        self.extended_contract_id: str = ""
        self.extended_tick_size: Decimal = Decimal("0.01")
        self.extended_min_order_size: Decimal = Decimal("0.001")

        # trade history
        self.completed_trades: List[Dict] = []
        self._open_spreads_weighted: List[Tuple[float, Decimal]] = []

        # staleness tracking
        self._stale_count = 0

        # Extended WS fill confirmation (replaces REST polling)
        self._x_ioc_pending_id: Optional[str] = None
        self._x_ioc_confirmed = asyncio.Event()
        self._x_ioc_fill_qty: Decimal = Decimal("0")

        # risk controls
        self._post_trade_cooldown = 15.0
        self._cooldown_until = 0.0
        self._cumulative_pnl_bps = 0.0
        self._loss_breaker_threshold_bps = -50.0
        self._loss_breaker_until = 0.0
        self._loss_breaker_pause_sec = 300.0
        self._nonce_error_count = 0

        # logging
        os.makedirs("logs", exist_ok=True)
        self.csv_path = f"logs/arb_{self.symbol}_trades.csv"
        self.log_path = f"logs/arb_{self.symbol}.log"
        self._init_csv()
        self.logger = self._init_logger()

    # ═══════════════════════════════════════════════
    #  Logging / CSV
    # ═══════════════════════════════════════════════

    def _init_logger(self) -> logging.Logger:
        lg = logging.getLogger(f"arb_{self.symbol}")
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
                    "spread_bps", "gross_bps", "net_bps", "cumulative_net_bps",
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
        self.logger.info("Extended client 已初始化 (WS订单事件已注册)")

    def _on_extended_order_event(self, event: dict):
        """Handle Extended WS order status updates for real-time fill confirmation."""
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
                    f"[Extended] IOC {order_id} {status} filled={self._x_ioc_fill_qty}")

    async def _get_extended_contract_info(self):
        cid, tick = await self.extended_client.get_contract_attributes()
        self.extended_contract_id = cid
        self.extended_tick_size = tick
        if hasattr(self.extended_client, "min_order_size"):
            self.extended_min_order_size = self.extended_client.min_order_size
        self.logger.info(f"Extended contract: {cid}  tick={tick}")

    async def _warmup_extended(self):
        """Pre-warm Extended HTTP session + Stark signing to eliminate cold-start latency."""
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
            cold_ms = (time.perf_counter() - t0) * 1000
            self.logger.info(f"[预热] 冷启动首单 {cold_ms:.0f}ms")

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
            warm_ms = (time.perf_counter() - t0) * 1000
            self.logger.info(f"[预热] 预热后延迟 {warm_ms:.0f}ms (冷启动 {cold_ms:.0f}ms → {warm_ms:.0f}ms)")
        except Exception as e:
            self.logger.warning(f"[预热] 异常 (可忽略): {e}")

    # ═══════════════════════════════════════════════
    #  Real-time Extended WS price
    # ═══════════════════════════════════════════════

    def _get_extended_ws_bbo(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Get best bid/ask from ExtendedClient WebSocket full-depth orderbook.
        Returns (bid, ask) or (None, None) if unavailable."""
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
        """Milliseconds since the last Extended orderbook update."""
        if not self.extended_client or self.extended_client._ob_last_update_ts == 0:
            return 999999.0
        return (time.time() - self.extended_client._ob_last_update_ts) * 1000

    def _get_extended_ws_vwap(self, qty: float) -> Tuple[Optional[float], Optional[float]]:
        """Calculate VWAP buy/sell prices from Extended full-depth orderbook.
        Returns (vwap_buy_cost, vwap_sell_proceeds) or (None, None)."""
        if not self.extended_client or not self.extended_client.orderbook:
            return None, None
        ob = self.extended_client.orderbook
        bids = ob.get('bid', [])
        asks = ob.get('ask', [])
        if not bids or not asks:
            return None, None

        def _vwap(levels: list, target_qty: float) -> Optional[float]:
            filled = 0.0
            cost = 0.0
            for lvl in levels:
                try:
                    p = float(lvl['p'])
                    q = float(lvl.get('c', lvl.get('q', '0')))
                except (ValueError, KeyError):
                    continue
                if q <= 0:
                    continue
                take = min(q, target_qty - filled)
                cost += p * take
                filled += take
                if filled >= target_qty - 1e-12:
                    return cost / filled
            if filled > 0:
                return cost / filled
            return None

        vwap_buy = _vwap(asks, qty)
        vwap_sell = _vwap(bids, qty)
        return vwap_buy, vwap_sell

    # ═══════════════════════════════════════════════
    #  Position Queries (REST)
    # ═══════════════════════════════════════════════

    def _get_lighter_position_sync(self) -> Decimal:
        url = f"{self.lighter_base_url}/api/v1/account"
        params = {"by": "index", "value": self.account_index}
        resp = requests.get(url, headers={"accept": "application/json"},
                            params=params, timeout=10)
        resp.raise_for_status()
        for pos in resp.json().get("accounts", [{}])[0].get("positions", []):
            if pos.get("symbol") == self.symbol:
                return Decimal(pos["position"]) * pos["sign"]
        return Decimal("0")

    async def _get_lighter_position(self) -> Decimal:
        try:
            return await asyncio.to_thread(self._get_lighter_position_sync)
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

    # ═══════════════════════════════════════════════
    #  Order Execution
    # ═══════════════════════════════════════════════

    def _round_lighter_price(self, price: Decimal) -> Decimal:
        if self.lighter_tick > 0:
            return (price / self.lighter_tick).quantize(Decimal("1")) * self.lighter_tick
        return price

    async def _place_lighter_taker(self, side: str, qty: Decimal, ref_price: Decimal) -> bool:
        """Place IOC market order on Lighter and verify fill via REST position check.
        Returns True only when the fill is confirmed by position change."""
        is_ask = side.lower() == "sell"
        if is_ask:
            price = self._round_lighter_price(ref_price * Decimal("0.9985"))
        else:
            price = self._round_lighter_price(ref_price * Decimal("1.0015"))

        pre_trade_pos = self.lighter_position

        client_order_index = int(time.time() * 1000)
        try:
            _, _, error = await self.lighter_client.create_market_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=int(qty * self.base_amount_multiplier),
                avg_execution_price=int(price * self.price_multiplier),
                is_ask=is_ask,
            )
            if error is not None:
                if "nonce" in str(error).lower():
                    self.lighter_client.nonce_manager.hard_refresh_nonce(self.api_key_index)
                self.logger.error(f"[Lighter] IOC 下单失败: {error}")
                return False
            self.logger.info(f"[Lighter] IOC {side} {qty} ref={ref_price:.2f} limit={price:.2f}")

            for verify_attempt in range(4):
                await asyncio.sleep(0.3)
                try:
                    new_pos = await self._get_lighter_position()
                    if side.lower() == "buy":
                        delta = new_pos - pre_trade_pos
                    else:
                        delta = pre_trade_pos - new_pos
                    if delta >= qty * Decimal("0.5"):
                        self.lighter_position = new_pos
                        self.logger.info(
                            f"[Lighter] 成交确认(第{verify_attempt+1}次): "
                            f"pos {pre_trade_pos} → {new_pos} Δ={delta}")
                        return True
                except Exception as e:
                    self.logger.warning(f"[Lighter] 成交验证查询失败: {e}")

            self.logger.warning(
                f"[Lighter] IOC 提交成功但未确认成交 "
                f"(限价={price:.2f} ref={ref_price:.2f} side={side})")
            return False
        except Exception as e:
            if "nonce" in str(e).lower():
                try:
                    self.lighter_client.nonce_manager.hard_refresh_nonce(self.api_key_index)
                except Exception:
                    pass
            self.logger.error(f"[Lighter] IOC 下单异常: {e}")
            return False

    async def _place_extended_taker(self, side: str, qty: Decimal, ref_price: Decimal) -> bool:
        """Place IOC order on Extended with WS real-time fill confirmation."""
        if side.lower() == "buy":
            aggressive_price = ref_price * Decimal("1.0005")
        else:
            aggressive_price = ref_price * Decimal("0.9995")
        aggressive_price = aggressive_price.quantize(self.extended_tick_size, rounding=ROUND_HALF_UP)
        qty_rounded = qty.quantize(self.extended_min_order_size, rounding=ROUND_HALF_UP)

        self._x_ioc_pending_id = None
        self._x_ioc_confirmed.clear()
        self._x_ioc_fill_qty = Decimal("0")

        try:
            result = await self.extended_client.place_ioc_order(
                contract_id=self.extended_contract_id,
                quantity=qty_rounded,
                side=side.lower(),
                aggressive_price=aggressive_price,
            )
            if result.success:
                self._x_ioc_pending_id = str(result.order_id)
                self.logger.info(
                    f"[Extended] taker {side} {qty_rounded} ref={ref_price:.2f} "
                    f"limit={aggressive_price:.2f}  id={result.order_id}")
                try:
                    await asyncio.wait_for(self._x_ioc_confirmed.wait(), timeout=3.0)
                    if self._x_ioc_fill_qty > 0:
                        self.logger.info(
                            f"[Extended] WS确认成交 qty={self._x_ioc_fill_qty}")
                        if self.extended_client:
                            self.extended_client.deduct_filled_qty(
                                side.lower(), float(self._x_ioc_fill_qty))
                except asyncio.TimeoutError:
                    self.logger.warning("[Extended] WS确认超时(3s), 继续")
                self._x_ioc_pending_id = None
                return True
            else:
                self.logger.error(f"[Extended] 下单失败: {result.error_message}")
                self._x_ioc_pending_id = None
                return False
        except Exception as e:
            self.logger.error(f"[Extended] 下单异常: {e}")
            self._x_ioc_pending_id = None
            return False

    # ═══════════════════════════════════════════════
    #  Open / Close / Repair
    # ═══════════════════════════════════════════════

    async def _open_positions(self, direction: str, qty: Decimal,
                              l_mid: Decimal, x_mid: Decimal, spread_bps: float) -> bool:
        """Open one leg-pair on both exchanges. Returns True if at least one leg succeeded."""
        x_ws_bid, x_ws_ask = self._get_extended_ws_bbo()
        if direction == "long_L_short_X":
            l_side, x_side = "buy", "sell"
            l_ref = Decimal(str(self.lighter_feed.snapshot.best_ask or l_mid))
            x_ref = x_ws_bid if x_ws_bid else Decimal(str(self.extended_feed.snapshot.best_bid or x_mid))
        else:
            l_side, x_side = "sell", "buy"
            l_ref = Decimal(str(self.lighter_feed.snapshot.best_bid or l_mid))
            x_ref = x_ws_ask if x_ws_ask else Decimal(str(self.extended_feed.snapshot.best_ask or x_mid))

        self.logger.info(
            f"[开仓] {direction}  {qty} {self.symbol}  价差={spread_bps:+.2f} bps  "
            f"L_{l_side}@~{l_ref:.2f}  X_{x_side}@~{x_ref:.2f}")

        if self.dry_run:
            self._open_l_refs.append((l_ref, qty))
            self._open_x_refs.append((x_ref, qty))
            self.logger.info("[DRY-RUN] 跳过实际下单")
            return True

        t0 = time.time()
        ok_l, ok_x = await asyncio.gather(
            self._place_lighter_taker(l_side, qty, l_ref),
            self._place_extended_taker(x_side, qty, x_ref),
        )
        elapsed_ms = (time.time() - t0) * 1000

        if ok_l and ok_x:
            self.logger.info(f"[开仓] 两腿已发送  耗时={elapsed_ms:.0f}ms")
            self._open_l_refs.append((l_ref, qty))
            self._open_x_refs.append((x_ref, qty))
        elif ok_l and not ok_x:
            self.logger.warning("[开仓] Extended 失败，尝试修复...")
            repaired = await self._repair_single_leg("extended", x_side, qty, x_ref)
            if not repaired:
                await self._do_cancel_all()
                return False
            self._open_l_refs.append((l_ref, qty))
            self._open_x_refs.append((x_ref, qty))
        elif not ok_l and ok_x:
            self.logger.warning("[开仓] Lighter 失败，尝试修复...")
            repaired = await self._repair_single_leg("lighter", l_side, qty, l_ref)
            if not repaired:
                await self._do_cancel_all()
                return False
            self._open_l_refs.append((l_ref, qty))
            self._open_x_refs.append((x_ref, qty))
        else:
            self.logger.error("[开仓] 两腿均失败，放弃本次")
            await self._do_cancel_all()
            return False

        await self._do_cancel_all()
        return True

    async def _close_all_positions(self, l_mid: Decimal, x_mid: Decimal, reason: str):
        """Close each leg with its own actual position size."""
        l_pos = abs(self.lighter_position)
        x_pos = abs(self.extended_position)
        if l_pos <= 0 and x_pos <= 0:
            return

        x_ws_bid, x_ws_ask = self._get_extended_ws_bbo()
        if self.lighter_position > 0:
            l_side = "sell"
            l_ref = Decimal(str(self.lighter_feed.snapshot.best_bid or l_mid))
        else:
            l_side = "buy"
            l_ref = Decimal(str(self.lighter_feed.snapshot.best_ask or l_mid))

        if self.extended_position > 0:
            x_side = "sell"
            x_ref = x_ws_bid if x_ws_bid else Decimal(str(self.extended_feed.snapshot.best_bid or x_mid))
        else:
            x_side = "buy"
            x_ref = x_ws_ask if x_ws_ask else Decimal(str(self.extended_feed.snapshot.best_ask or x_mid))

        spread_bps = float((x_mid - l_mid) / l_mid * 10000) if l_mid > 0 else 0

        self.logger.info(
            f"[平仓-{reason}] L_{l_side} {l_pos}  X_{x_side} {x_pos}  "
            f"价差={spread_bps:+.2f} bps")

        if self.dry_run:
            self.logger.info("[DRY-RUN] 跳过实际平仓")
            self.state = STATE_IDLE
            return

        tasks = []
        if l_pos > 0:
            tasks.append(self._place_lighter_taker(l_side, l_pos, l_ref))
        if x_pos > 0:
            tasks.append(self._place_extended_taker(x_side, x_pos, x_ref))

        t0 = time.time()
        results = await asyncio.gather(*tasks)
        elapsed_ms = (time.time() - t0) * 1000
        self.logger.info(f"[平仓] 两腿已发送  耗时={elapsed_ms:.0f}ms")

        close_ok = True
        idx = 0
        if l_pos > 0:
            if not results[idx]:
                self.logger.warning("[平仓] Lighter 失败，重试修复...")
                repaired = await self._repair_single_leg("lighter", l_side, l_pos, l_ref)
                if not repaired:
                    close_ok = False
            idx += 1
        if x_pos > 0:
            if not results[idx]:
                self.logger.warning("[平仓] Extended 失败，重试修复...")
                repaired = await self._repair_single_leg("extended", x_side, x_pos, x_ref)
                if not repaired:
                    close_ok = False

        await self._do_cancel_all()
        if not close_ok:
            self.logger.error("[平仓] 修复失败，需对账确认仓位状态")
        self._log_ref_pnl(l_ref, x_ref, l_side, x_side)

    def _log_ref_pnl(self, close_l_ref: Decimal, close_x_ref: Decimal,
                      l_close_side: str, x_close_side: str):
        """Estimate P&L from recorded ref prices (best bid/ask at order time)."""
        if not self._open_l_refs or not self._open_x_refs:
            return
        threshold = self.order_size * Decimal("0.1")
        l_has = abs(self.lighter_position) >= threshold
        x_has = abs(self.extended_position) >= threshold
        if l_has != x_has:
            self.logger.warning(
                f"[P&L估算] 跳过: 单腿无持仓 L={self.lighter_position} "
                f"X={self.extended_position} (ref价格不可靠)")
            return
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

        x_fee = float(x_open_cost + close_x_ref * x_open_qty) * 0.00025
        total = l_pnl + x_pnl - x_fee
        notional = float(max(l_open_qty, x_open_qty)) * float(close_l_ref)
        net_bps = total / notional * 10000 if notional > 0 else 0

        self.logger.info(
            f"[P&L估算] Lighter={l_pnl:+.3f}$ Extended={x_pnl:+.3f}$ "
            f"ExtFee=-{x_fee:.3f}$ 净={total:+.3f}$ ({net_bps:+.1f}bps)  "
            f"⚠ 基于ref价格，实际成交可能偏差")

    def _fresh_ref_price(self, exchange: str, side: str, fallback: Decimal) -> Decimal:
        """Get fresh bid/ask for repair retries instead of using stale ref."""
        try:
            if exchange == "lighter":
                ls = self.lighter_feed.snapshot if self.lighter_feed else None
                if ls:
                    if side in ("buy",):
                        return Decimal(str(ls.best_ask or fallback))
                    else:
                        return Decimal(str(ls.best_bid or fallback))
            else:
                x_bid, x_ask = self._get_extended_ws_bbo()
                if side in ("buy",):
                    return x_ask if x_ask else fallback
                else:
                    return x_bid if x_bid else fallback
        except Exception:
            pass
        return fallback

    async def _repair_single_leg(self, which: str, side: str, qty: Decimal, ref_price: Decimal) -> bool:
        """Retry a failed leg up to 3 times; if still failing, close the other leg.
        `which` = failed exchange, `side` = what the failed exchange was supposed to do.
        Returns True if repair succeeded, False if had to undo."""
        pre_repair_pos = self.lighter_position if which == "lighter" else self.extended_position

        for attempt in range(1, 4):
            try:
                if which == "lighter":
                    cur = await self._get_lighter_position()
                    self.lighter_position = cur
                else:
                    cur = await self._get_extended_position()
                    self.extended_position = cur
                if side.lower() == "buy":
                    delta = cur - pre_repair_pos
                else:
                    delta = pre_repair_pos - cur
                if delta >= qty * Decimal("0.5"):
                    self.logger.info(
                        f"[修复] {which} 延迟成交已确认: pos {pre_repair_pos} → {cur}")
                    return True
            except Exception:
                pass

            fresh_ref = self._fresh_ref_price(which, side, ref_price)
            self.logger.info(f"[修复] {which} {side} {qty} 第 {attempt}/3 次重试 ref={fresh_ref:.2f}")
            await asyncio.sleep(0.3)
            if which == "lighter":
                ok = await self._place_lighter_taker(side, qty, fresh_ref)
            else:
                ok = await self._place_extended_taker(side, qty, fresh_ref)
            if ok:
                self.logger.info(f"[修复] {which} 重试成功")
                return True

        self.logger.error(f"[修复] {which} 3 次重试均失败，尝试反向平掉另一腿")
        undo_side = side
        other = "extended" if which == "lighter" else "lighter"
        undo_ref = self._fresh_ref_price(other, undo_side, ref_price)
        if which == "lighter":
            self.logger.info(f"[修复] 反向平: Extended {undo_side} {qty} ref={undo_ref:.2f}")
            await self._place_extended_taker(undo_side, qty, undo_ref)
        else:
            self.logger.info(f"[修复] 反向平: Lighter {undo_side} {qty} ref={undo_ref:.2f}")
            await self._place_lighter_taker(undo_side, qty, undo_ref)
        return False

    # ═══════════════════════════════════════════════
    #  Cancel All / Fill Confirmation
    # ═══════════════════════════════════════════════

    async def _do_cancel_all(self) -> bool:
        """Cancel all Lighter orders to prevent any residual hanging orders."""
        if self.dry_run or not self.lighter_client:
            return True
        for attempt in range(3):
            try:
                future_ts_ms = int((time.time() + 300) * 1000)
                _, _, error = await self.lighter_client.cancel_all_orders(
                    time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                    timestamp_ms=future_ts_ms)
                if error is None:
                    self._nonce_error_count = 0
                    self.logger.info("[Lighter] cancel_all 成功")
                    return True
                if "nonce" in str(error).lower():
                    self._nonce_error_count += 1
                    try:
                        self.lighter_client.nonce_manager.hard_refresh_nonce(self.api_key_index)
                    except Exception:
                        pass
                self.logger.warning(f"[Lighter] cancel_all 失败 (第{attempt+1}次): {error}")
            except Exception as e:
                if "nonce" in str(e).lower():
                    try:
                        self.lighter_client.nonce_manager.hard_refresh_nonce(self.api_key_index)
                    except Exception:
                        pass
                self.logger.error(f"[Lighter] cancel_all 异常 (第{attempt+1}次): {e}")
            await asyncio.sleep(0.5)
        self.logger.error("[Lighter] cancel_all 3次重试均失败")
        return False

    async def _confirm_fills(self, expected_l_qty: Decimal, expected_x_qty: Decimal,
                             action: str, retries: int = 3) -> bool:
        """Reconcile positions after trade to confirm fills landed correctly.
        Retries multiple times to account for propagation delay (Lighter 300ms, etc.)."""
        if self.dry_run:
            return True
        for attempt in range(retries):
            await asyncio.sleep(1.0)
            await self._reconcile_positions()
            l_ok = abs(abs(self.lighter_position) - expected_l_qty) < self.order_size * Decimal("0.5")
            x_ok = abs(abs(self.extended_position) - expected_x_qty) < self.order_size * Decimal("0.5")
            if l_ok and x_ok:
                self.logger.info(
                    f"[{action}确认] 两腿成交确认: L={self.lighter_position} X={self.extended_position}")
                return True
            if attempt < retries - 1:
                self.logger.warning(
                    f"[{action}确认] 仓位偏差 (第{attempt+1}/{retries}次重试): "
                    f"L={self.lighter_position}(期望±{expected_l_qty}) "
                    f"X={self.extended_position}(期望±{expected_x_qty})")
        self.logger.error(
            f"[{action}确认] 仓位不平衡! L={self.lighter_position}(期望±{expected_l_qty}) "
            f"X={self.extended_position}(期望±{expected_x_qty})")
        return False

    # ═══════════════════════════════════════════════
    #  Position Reconciliation
    # ═══════════════════════════════════════════════

    async def _reconcile_positions(self) -> bool:
        """Fetch real positions in parallel and update local tracking. Return True if balanced."""
        l, x = await asyncio.gather(
            self._get_lighter_position(),
            self._get_extended_position(),
        )
        self.lighter_position = l
        self.extended_position = x
        net = l + x
        balanced = abs(net) < self.order_size * Decimal("0.5")
        if not balanced:
            self.logger.warning(
                f"[对账] 不平衡: Lighter={l} Extended={x} net={net}")
        return balanced

    # ═══════════════════════════════════════════════
    #  Shutdown
    # ═══════════════════════════════════════════════

    async def _shutdown(self):
        self.stop_flag = True
        self.logger.info("关闭中...")
        try:
            await self._do_cancel_all()
        except Exception as e:
            self.logger.error(f"退出 cancel_all 失败: {e}")
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
                await asyncio.wait_for(self.extended_client.disconnect(), timeout=5)
            except Exception:
                pass
        await asyncio.sleep(0.5)
        self.logger.info("已关闭")

    # ═══════════════════════════════════════════════
    #  Main Loop
    # ═══════════════════════════════════════════════

    async def run(self):
        # ── Signal handlers ──
        def on_stop(*_):
            self.stop_flag = True
            self.logger.info("收到中断信号")

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                asyncio.get_event_loop().add_signal_handler(sig, on_stop)
            except NotImplementedError:
                signal.signal(sig, on_stop)

        try:
            await self._main_loop()
        except KeyboardInterrupt:
            self.logger.info("用户中断")
        except Exception as e:
            self.logger.error(f"致命错误: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                self.logger.warning(
                    f"⚠️ 退出前自动平仓: L={self.lighter_position} X={self.extended_position}  "
                    f"请勿手动重复平仓！")
                ls = self.lighter_feed.snapshot if self.lighter_feed else None
                es = self.extended_feed.snapshot if self.extended_feed else None
                if ls and es and ls.mid and es.mid:
                    await self._close_all_positions(
                        Decimal(str(ls.mid)), Decimal(str(es.mid)), "退出")
                    await asyncio.sleep(5)
                    await self._reconcile_positions()
                    self.logger.warning(
                        f"退出平仓完成: L={self.lighter_position} X={self.extended_position}")
            else:
                self.logger.info("无持仓，直接退出")
            await self._shutdown()

    async def _main_loop(self):
        # ── 1. Initialize trading clients ──
        x_src = "WS实时BBO" if not self.dry_run else "REST轮询(dry-run)"
        self.logger.info(
            f"启动 Extended+Lighter 收敛套利  {self.symbol}  "
            f"size={self.order_size} max_pos={self.max_position}  "
            f"开仓≥{self.effective_open_bps}bps(VWAP+buf{self.exec_buffer_bps}) "
            f"平仓≤{self.close_bps}bps({'反转' if self.close_bps <= 0 else 'mid'})  "
            f"超时={self.max_hold_sec}s  dry_run={self.dry_run}  "
            f"Extended数据源={x_src}")

        if not self.dry_run:
            self._init_lighter_signer()
            self._get_lighter_market_config()
            self._init_extended_client()
            await self.extended_client.connect()
            await self._get_extended_contract_info()
            self.logger.info("交易客户端就绪")
            await self._warmup_extended()
        else:
            self.logger.info("[DRY-RUN] 跳过交易客户端初始化")
            self._get_lighter_market_config()

        # ── 2. Start market-data feeds ──
        self.lighter_feed = LighterFeed(self.symbol)
        self.extended_feed = ExtendedFeed(self.symbol)
        bg_tasks = [
            asyncio.create_task(self.lighter_feed.connect()),
            asyncio.create_task(self.extended_feed.connect()),
        ]
        self.logger.info("等待行情数据...")
        for _wait in range(10):
            await asyncio.sleep(2)
            ls = self.lighter_feed.snapshot
            es = self.extended_feed.snapshot
            if ls.mid and es.mid:
                break
            self.logger.info(f"  等待中... L={ls.mid} X={es.mid}")
        ls = self.lighter_feed.snapshot
        es = self.extended_feed.snapshot
        if not ls.mid or not es.mid:
            self.logger.error(
                f"行情不可用(20s超时): Lighter mid={ls.mid} Extended mid={es.mid}")
            return
        self.logger.info(f"行情就绪: Lighter={ls.mid:.2f} Extended={es.mid:.2f}")

        # ── 3. Initial position check (double-confirm with delay) ──
        startup_grace_until = 0.0
        if not self.dry_run:
            await self._reconcile_positions()
            if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                self.logger.warning(
                    f"启动时检测到仓位: L={self.lighter_position} X={self.extended_position}")
                self.logger.info("等待 5s 后二次确认仓位...")
                await asyncio.sleep(5)
                await self._reconcile_positions()
                if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                    self.logger.warning(
                        f"二次确认仍有仓位: L={self.lighter_position} X={self.extended_position}  "
                        f"进入 IN_POSITION，15s 内不执行开平仓")
                    self.state = STATE_IN_POSITION
                    self.position_opened_at = time.time()
                    startup_grace_until = time.time() + 15
                    if self.lighter_position > 0:
                        self.position_direction = "long_L_short_X"
                    elif self.lighter_position < 0:
                        self.position_direction = "long_X_short_L"
                else:
                    self.logger.info("二次确认仓位已清零，忽略首次检测结果")

        self.logger.info(f"进入主循环  状态={self.state}")

        last_reconcile = time.time()
        last_status_log = 0.0
        cumulative_net_bps = 0.0

        while not self.stop_flag:
            try:
                ls = self.lighter_feed.snapshot
                es = self.extended_feed.snapshot
                now = time.time()

                # Safety: recover from stuck transient states
                if self.state in (STATE_OPENING, STATE_CLOSING):
                    self.logger.warning(f"[恢复] 状态卡在 {self.state}，执行对账后重置")
                    if not self.dry_run:
                        try:
                            await self._reconcile_positions()
                        except Exception:
                            pass
                    if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                        self.state = STATE_IN_POSITION
                        if self.position_opened_at is None:
                            self.position_opened_at = now
                    else:
                        self._reset_position_state()
                        self.state = STATE_IDLE

                if not ls.mid or not es.mid:
                    await asyncio.sleep(self.interval)
                    continue

                l_mid = Decimal(str(ls.mid))

                # ── Extended price: prefer real-time WS over stale REST ──
                x_ws_bid, x_ws_ask = (None, None)
                if not self.dry_run:
                    x_ws_bid, x_ws_ask = self._get_extended_ws_bbo()

                if x_ws_bid is not None and x_ws_ask is not None:
                    x_mid = (x_ws_bid + x_ws_ask) / 2
                else:
                    x_mid = Decimal(str(es.mid))

                mid_spread_bps = float((x_mid - l_mid) / l_mid * 10000)
                abs_mid_spread = abs(mid_spread_bps)

                # ── VWAP spread: Lighter uses depth, Extended uses BBO (depth=1) ──
                qty_f = float(self.order_size)
                l_vwap_buy = ls.vwap_buy(qty_f)
                l_vwap_sell = ls.vwap_sell(qty_f)

                x_vwap_buy_depth, x_vwap_sell_depth = (None, None)
                if not self.dry_run:
                    x_vwap_buy_depth, x_vwap_sell_depth = self._get_extended_ws_vwap(qty_f)

                if x_vwap_buy_depth is not None and x_vwap_sell_depth is not None:
                    x_vwap_buy = x_vwap_buy_depth
                    x_vwap_sell = x_vwap_sell_depth
                elif x_ws_bid is not None and x_ws_ask is not None:
                    x_vwap_buy = float(x_ws_ask)
                    x_vwap_sell = float(x_ws_bid)
                else:
                    x_vwap_buy = es.vwap_buy(qty_f)
                    x_vwap_sell = es.vwap_sell(qty_f)

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

                # periodic status log
                if now - last_status_log > 15:
                    hold_s = ""
                    if self.position_opened_at:
                        hold_s = f" hold={now - self.position_opened_at:.0f}s"
                    self.logger.info(
                        f"[{self.state}] mid={mid_spread_bps:+.2f}bps vwap={vwap_spread_bps:+.2f}bps  "
                        f"L_pos={self.lighter_position} X_pos={self.extended_position}{hold_s}  "
                        f"L_mid={l_mid:.2f} X_mid={x_mid:.2f}  累积净利={cumulative_net_bps:+.2f}bps")
                    last_status_log = now

                # periodic position reconciliation (every 60s, not during opening/closing)
                if (not self.dry_run
                        and now - last_reconcile > 60
                        and self.state in (STATE_IDLE, STATE_IN_POSITION)):
                    await self._reconcile_positions()
                    last_reconcile = now
                    cur_pos = abs(self.lighter_position)

                    if self.state == STATE_IDLE:
                        has_l = abs(self.lighter_position) > 0
                        has_x = abs(self.extended_position) > 0
                        if has_l or has_x:
                            self.logger.warning(
                                f"[对账修复] IDLE状态发现残余仓位: L={self.lighter_position} X={self.extended_position}，自动清理")
                            await self._close_all_positions(l_mid, x_mid, "对账自动清理")
                            await asyncio.sleep(1.5)
                            await self._reconcile_positions()
                            if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                                self.logger.error(
                                    f"[对账修复] 自动清理失败，仍有: L={self.lighter_position} X={self.extended_position}，切换IN_POSITION防止开新仓")
                                self.state = STATE_IN_POSITION
                                self.position_opened_at = time.time()
                                if self.lighter_position > 0:
                                    self.position_direction = "long_L_short_X"
                                elif self.lighter_position < 0:
                                    self.position_direction = "long_X_short_L"
                                else:
                                    if self.extended_position > 0:
                                        self.position_direction = "long_X_short_L"
                                    else:
                                        self.position_direction = "long_L_short_X"
                            else:
                                self.logger.info("[对账修复] 残余仓位已清理完毕")
                            last_reconcile = time.time()

                # startup grace period: skip all trading actions
                if now < startup_grace_until:
                    await asyncio.sleep(self.interval)
                    continue

                # ────── STATE: IDLE ──────
                if self.state == STATE_IDLE:
                    if now < self._cooldown_until:
                        await asyncio.sleep(self.interval)
                        continue
                    if now < self._loss_breaker_until:
                        await asyncio.sleep(self.interval)
                        continue

                    ob_age = self._extended_ob_age_ms() if not self.dry_run else 0
                    if (vwap_direction
                            and vwap_spread_bps >= self.effective_open_bps
                            and cur_pos < self.max_position
                            and ob_age < 500):
                        direction = vwap_direction
                        self.state = STATE_OPENING
                        self._last_open_spread = vwap_spread_bps
                        self.logger.info(
                            f"[信号] VWAP价差={vwap_spread_bps:+.2f}bps >= "
                            f"{self.effective_open_bps}({self.open_bps}+{self.exec_buffer_bps})  "
                            f"mid={mid_spread_bps:+.2f}bps  方向={direction}  OB_age={ob_age:.0f}ms")
                        t_signal = time.time()
                        open_ok = await self._open_positions(direction, self.order_size, l_mid, x_mid, vwap_spread_bps)

                        if not open_ok:
                            self.state = STATE_IDLE
                            self._cooldown_until = time.time() + 5.0
                            continue

                        if not self.dry_run:
                            total_ms = (time.time() - t_signal) * 1000
                            self.logger.info(
                                f"[开仓确认] 方向={direction} 信号→完成={total_ms:.0f}ms")
                            expected_pos = cur_pos + self.order_size
                            fills_ok = await self._confirm_fills(expected_pos, expected_pos, "开仓")

                            if not fills_ok:
                                self.logger.error(
                                    "[开仓] 仓位不平衡! 立即平掉已成交的腿，防止裸露风险")
                                await self._close_all_positions(l_mid, x_mid, "单腿失败-紧急平仓")
                                await asyncio.sleep(1.5)
                                await self._reconcile_positions()
                                self._open_l_refs.clear()
                                self._open_x_refs.clear()
                                self._open_spreads_weighted.clear()
                                if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                                    self.logger.error(
                                        f"[紧急] 平仓后仍有残余仓位: "
                                        f"L={self.lighter_position} X={self.extended_position}")
                                    self.state = STATE_IN_POSITION
                                    if self.position_opened_at is None:
                                        self.position_opened_at = time.time()
                                    if self.lighter_position > 0:
                                        self.position_direction = "long_L_short_X"
                                    elif self.lighter_position < 0:
                                        self.position_direction = "long_X_short_L"
                                    elif self.extended_position > 0:
                                        self.position_direction = "long_X_short_L"
                                    elif self.extended_position < 0:
                                        self.position_direction = "long_L_short_X"
                                else:
                                    self._reset_position_state()
                                    self.state = STATE_IDLE
                                self._cooldown_until = time.time() + 15.0
                                last_reconcile = time.time()
                                continue

                            self._open_spreads_weighted.append((vwap_spread_bps, self.order_size))
                            self._last_extended_trade_ts = time.time()
                            self.state = STATE_IN_POSITION
                            if self.position_opened_at is None:
                                self.position_opened_at = now
                            self.position_direction = direction
                            self._csv_row([
                                datetime.now(timezone.utc).isoformat(), "OPEN", direction,
                                "buy" if direction == "long_L_short_X" else "sell",
                                f"{l_mid:.2f}", str(self.order_size),
                                "sell" if direction == "long_L_short_X" else "buy",
                                f"{x_mid:.2f}", str(self.order_size),
                                f"{vwap_spread_bps:.2f}", "", "", "",
                            ])
                            last_reconcile = now - 50
                        else:
                            self.state = STATE_IDLE

                # ────── STATE: IN_POSITION ──────
                elif self.state == STATE_IN_POSITION:
                    # close condition: directional convergence
                    # close_bps > 0: abs(spread) <= close_bps (classic convergence)
                    # close_bps <= 0: spread must flip past |close_bps|
                    #   long_L_short_X opened when spread > 0 → close when spread <= close_bps
                    #   long_X_short_L opened when spread < 0 → close when spread >= -close_bps
                    if self.close_bps > 0:
                        _converged = abs_mid_spread <= self.close_bps
                    else:
                        if self.position_direction == "long_L_short_X":
                            _converged = mid_spread_bps <= self.close_bps
                        else:
                            _converged = mid_spread_bps >= -self.close_bps
                    if _converged:
                        t_close = time.time()
                        self.logger.info(f"[平仓信号] mid价差收敛到 {mid_spread_bps:+.2f} bps")
                        self.state = STATE_CLOSING
                        open_spread = self._weighted_avg_open_spread()
                        await self._close_all_positions(l_mid, x_mid, "收敛")

                        if self.position_direction == "long_X_short_L":
                            gross_bps = open_spread + mid_spread_bps if open_spread else 0
                        else:
                            gross_bps = open_spread - mid_spread_bps if open_spread else 0
                        fee_bps = 5.0
                        net_bps = gross_bps - fee_bps
                        cumulative_net_bps += net_bps
                        self._cumulative_pnl_bps += net_bps
                        close_ms = (time.time() - t_close) * 1000
                        n_legs = len(self._open_spreads_weighted)
                        self.logger.info(
                            f"[平仓完成] 信号→完成={close_ms:.0f}ms  "
                            f"加权开仓={open_spread:+.2f}bps({n_legs}笔) "
                            f"平仓mid={mid_spread_bps:+.2f}bps  "
                            f"毛利={gross_bps:+.2f} 费用={fee_bps:.1f} "
                            f"净利={net_bps:+.2f}bps 累积={cumulative_net_bps:+.2f}bps")
                        self._csv_row([
                            datetime.now(timezone.utc).isoformat(), "CLOSE", "convergence",
                            "", f"{l_mid:.2f}", "",
                            "", f"{x_mid:.2f}", "",
                            f"{mid_spread_bps:.2f}", f"{gross_bps:.2f}",
                            f"{net_bps:.2f}", f"{cumulative_net_bps:.2f}",
                        ])

                        close_ok = await self._confirm_fills(Decimal("0"), Decimal("0"), "平仓")
                        if not close_ok:
                            self.logger.error("[平仓] 仓位未完全清零，重试平仓...")
                            await self._close_all_positions(l_mid, x_mid, "残余清理")
                            await asyncio.sleep(1.5)
                            await self._reconcile_positions()
                            if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                                self.logger.error(
                                    f"[平仓] 重试后仍有残余: L={self.lighter_position} X={self.extended_position}, 保持IN_POSITION")
                                self.state = STATE_IN_POSITION
                                self._cooldown_until = time.time() + 15.0
                                last_reconcile = time.time()
                                continue

                        self._reset_position_state()
                        self.state = STATE_IDLE
                        self._cooldown_until = time.time() + self._post_trade_cooldown

                        if self._cumulative_pnl_bps <= self._loss_breaker_threshold_bps:
                            self._loss_breaker_until = time.time() + self._loss_breaker_pause_sec
                            self.logger.warning(
                                f"[熔断] 累积亏损={self._cumulative_pnl_bps:+.1f}bps "
                                f"超过阈值{self._loss_breaker_threshold_bps}bps, "
                                f"暂停{self._loss_breaker_pause_sec}s")

                        last_reconcile = now - 50
                        continue

                    # close condition: timeout
                    if self.position_opened_at and now - self.position_opened_at > self.max_hold_sec:
                        self.logger.warning(
                            f"[超时强平] 持仓 {now - self.position_opened_at:.0f}s > {self.max_hold_sec}s")
                        self.state = STATE_CLOSING
                        await self._close_all_positions(l_mid, x_mid, "超时")

                        self._csv_row([
                            datetime.now(timezone.utc).isoformat(), "CLOSE", "timeout",
                            "", f"{l_mid:.2f}", "",
                            "", f"{x_mid:.2f}", "",
                            f"{mid_spread_bps:.2f}", "", "", f"{cumulative_net_bps:.2f}",
                        ])
                        timeout_close_ok = await self._confirm_fills(Decimal("0"), Decimal("0"), "超时平仓")
                        if not timeout_close_ok:
                            self.logger.error("[超时平仓] 仓位未完全清零，重试平仓...")
                            await self._close_all_positions(l_mid, x_mid, "超时残余清理")
                            await asyncio.sleep(1.5)
                            await self._reconcile_positions()
                            if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                                self.logger.error(
                                    f"[超时平仓] 重试后仍有残余: L={self.lighter_position} X={self.extended_position}, 保持IN_POSITION")
                                self.state = STATE_IN_POSITION
                                self._cooldown_until = time.time() + 15.0
                                last_reconcile = time.time()
                                continue
                        self._reset_position_state()
                        self.state = STATE_IDLE
                        self._cooldown_until = time.time() + self._post_trade_cooldown
                        last_reconcile = now - 50
                        continue

                    # accumulation: add more if VWAP spread still wide (same direction only)
                    add_cooldown_ok = (time.time() - self._last_extended_trade_ts) > 0.5
                    ob_age_add = self._extended_ob_age_ms() if not self.dry_run else 0
                    if (vwap_direction
                            and vwap_spread_bps >= self.effective_open_bps
                            and cur_pos + self.order_size <= self.max_position
                            and add_cooldown_ok
                            and ob_age_add < 500):
                        direction = self.position_direction or vwap_direction
                        if vwap_direction == direction:
                            self.logger.info(
                                f"[加仓] VWAP={vwap_spread_bps:+.2f}bps mid={mid_spread_bps:+.2f}bps "
                                f"当前仓位={cur_pos} 加 {self.order_size}  OB_age={ob_age_add:.0f}ms")
                            self.state = STATE_OPENING
                            add_ok = await self._open_positions(direction, self.order_size, l_mid, x_mid, vwap_spread_bps)
                            if not add_ok:
                                self.state = STATE_IN_POSITION
                                continue
                            expected_add = cur_pos + self.order_size
                            add_confirmed = await self._confirm_fills(expected_add, expected_add, "加仓")
                            if not add_confirmed:
                                self.logger.error(
                                    "[加仓] 仓位不平衡! 立即平掉全部仓位止损")
                                await self._close_all_positions(l_mid, x_mid, "加仓单腿失败-全平")
                                await asyncio.sleep(1.5)
                                await self._reconcile_positions()
                                self._open_l_refs.clear()
                                self._open_x_refs.clear()
                                self._open_spreads_weighted.clear()
                                if abs(self.lighter_position) > 0 or abs(self.extended_position) > 0:
                                    self.logger.error(
                                        f"[加仓] 全平后仍有残余: L={self.lighter_position} X={self.extended_position}")
                                    self.state = STATE_IN_POSITION
                                    if self.position_opened_at is None:
                                        self.position_opened_at = time.time()
                                else:
                                    self._reset_position_state()
                                    self.state = STATE_IDLE
                                self._cooldown_until = time.time() + 15.0
                                last_reconcile = time.time()
                                continue
                            self._open_spreads_weighted.append((vwap_spread_bps, self.order_size))
                            self._last_extended_trade_ts = time.time()
                            self._csv_row([
                                datetime.now(timezone.utc).isoformat(), "ADD", direction,
                                "buy" if direction == "long_L_short_X" else "sell",
                                f"{l_mid:.2f}", str(self.order_size),
                                "sell" if direction == "long_L_short_X" else "buy",
                                f"{x_mid:.2f}", str(self.order_size),
                                f"{vwap_spread_bps:.2f}", "", "", "",
                            ])
                            self.state = STATE_IN_POSITION

                await asyncio.sleep(self.interval)

            except Exception as e:
                self.logger.error(f"主循环异常: {e}")
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(2)

    def _reset_position_state(self):
        self.position_opened_at = None
        self.position_direction = None
        self._last_open_spread = 0.0
        self._open_spreads_weighted: List[Tuple[float, Decimal]] = []
        self._open_l_refs.clear()
        self._open_x_refs.clear()

    def _weighted_avg_open_spread(self) -> float:
        """Position-weighted average open spread across all opens and adds."""
        if not self._open_spreads_weighted:
            return self._last_open_spread
        total_qty = sum(q for _, q in self._open_spreads_weighted)
        if total_qty <= 0:
            return self._last_open_spread
        return float(sum(s * float(q) for s, q in self._open_spreads_weighted) / float(total_qty))


def main():
    p = argparse.ArgumentParser(description="Extended + Lighter 价差收敛套利")
    p.add_argument("-s", "--symbol", default="ETH")
    p.add_argument("--size", type=str, default="0.1", help="每笔数量 (ETH)")
    p.add_argument("--max-position", type=str, default="0.5", help="最大累积仓位")
    p.add_argument("--open-bps", type=float, default=4.5, help="开仓 VWAP 深度加权价差 ≥ (bps)")
    p.add_argument("--close-bps", type=float, default=-1.0,
                   help="平仓阈值(bps). >0=收敛平仓, <=0=价差反转|N|bps才平仓")
    p.add_argument("--max-hold-sec", type=float, default=1200.0, help="最大持仓秒数")
    p.add_argument("--interval", type=float, default=0.1, help="主循环间隔 (秒)")
    p.add_argument("--exec-buffer-bps", type=float, default=0.0,
                   help="执行滑点缓冲 (bps), 实际开仓阈值 = open-bps + exec-buffer-bps (WS实时数据下通常设0)")
    p.add_argument("--cooldown", type=float, default=15.0, help="交易后冷却秒数")
    p.add_argument("--loss-breaker-bps", type=float, default=-50.0,
                   help="累积亏损熔断阈值 (bps, 负数)")
    p.add_argument("--loss-breaker-pause", type=float, default=300.0,
                   help="熔断暂停秒数")
    p.add_argument("--dry-run", action="store_true", help="只看信号不下单")
    p.add_argument("--env-file", default=".env", help=".env 文件路径")
    args = p.parse_args()

    load_dotenv(args.env_file)

    if args.close_bps >= args.open_bps:
        args.close_bps = max(0.0, args.open_bps - 1.0)

    bot = ConvergenceArbBot(
        symbol=args.symbol.upper(),
        order_size=Decimal(args.size),
        max_position=Decimal(args.max_position),
        open_bps=args.open_bps,
        close_bps=args.close_bps,
        max_hold_sec=args.max_hold_sec,
        interval=args.interval,
        dry_run=args.dry_run,
        exec_buffer_bps=args.exec_buffer_bps,
    )
    bot._post_trade_cooldown = args.cooldown
    bot._loss_breaker_threshold_bps = args.loss_breaker_bps
    bot._loss_breaker_pause_sec = args.loss_breaker_pause

    try:
        import uvloop
        uvloop.install()
        print("uvloop 已启用 (更快的事件循环)")
    except ImportError:
        pass

    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n已中断。")


if __name__ == "__main__":
    main()
