#!/usr/bin/env python3
"""
01 Exchange + Lighter 收敛套利（双 Taker 策略）

Strategy: Monitor mid-price spread between 01 Exchange and Lighter.
  Open: when VWAP spread >= threshold, taker-buy on cheap side + taker-sell on expensive side.
  Close: when mid spread converges past close threshold, or timeout.

Fee structure:
  01 Exchange taker: 5.0 bps (0.05%) per side
  Lighter taker:     0.0 bps (standard account)
  Round-trip total:   10.0 bps

Usage:
  # Dry-run (observe spreads only, no real orders):
  python3 o1_lighter_arb.py --symbol BTC --size 0.002 --dry-run

  # Live trading:
  python3 o1_lighter_arb.py --symbol BTC --size 0.002 --max-position 0.006 \\
    --open-bps 12 --close-bps -1.0 --max-hold-sec 1200
"""

import argparse
import asyncio
import csv
import logging
import os
import signal as signal_mod
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from lighter.signer_client import SignerClient

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from exchanges.o1_client import O1Client
import schema_pb2
from feeds.lighter_feed import LighterFeed
from feeds.existing_feeds import O1Feed

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("uvloop 已启用")
except ImportError:
    pass

O1_TAKER_FEE_BPS = 3.5
LIGHTER_TAKER_FEE_BPS = 0.0
ROUND_TRIP_FEE_BPS = (O1_TAKER_FEE_BPS + LIGHTER_TAKER_FEE_BPS) * 2

STATE_IDLE = "IDLE"
STATE_OPENING = "OPENING"
STATE_IN_POSITION = "IN_POSITION"
STATE_CLOSING = "CLOSING"


class O1LighterArb:

    def __init__(self, args):
        self.symbol = args.symbol.upper()
        self.order_size = Decimal(str(args.size))
        self.max_position = Decimal(str(args.max_position))
        self.open_bps = args.open_bps
        self.close_bps = args.close_bps
        self.max_hold_sec = args.max_hold_sec
        self.interval = args.interval
        self.dry_run = args.dry_run
        self.cooldown_sec = args.cooldown

        self.effective_open_bps = self.open_bps

        self.state = STATE_IDLE
        self.lighter_position = Decimal("0")
        self.o1_position = Decimal("0")
        self.position_opened_at: Optional[float] = None
        self.position_direction: Optional[str] = None
        self._open_spreads_weighted: List[Tuple[float, Decimal]] = []
        self._cooldown_until: float = 0
        self._post_trade_cooldown = self.cooldown_sec
        self._last_open_spread: float = 0

        # 01 client
        self.o1_client: Optional[O1Client] = None
        self.o1_market_id: Optional[int] = None
        self.o1_price_decimals: int = 2
        self.o1_size_decimals: int = 4
        self.o1_tick_size: Decimal = Decimal("0.01")

        # Lighter client
        self.lighter_client: Optional[SignerClient] = None
        self.lighter_market_index: int = 0
        self.base_amount_multiplier: int = 0
        self.price_multiplier: int = 0
        self.lighter_tick_size: Decimal = Decimal("0.1")

        # Feeds
        self.lighter_feed: Optional[LighterFeed] = None
        self.o1_feed: Optional[O1Feed] = None

        self.logger = logging.getLogger("O1LighterArb")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            h = logging.StreamHandler()
            h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            self.logger.addHandler(h)

        self._csv_file = None
        self._csv_writer = None
        self._stop = False

    def _csv_row(self, row):
        if self._csv_writer:
            self._csv_writer.writerow(row)
            self._csv_file.flush()

    def _reset_position_state(self):
        self.position_opened_at = None
        self.position_direction = None
        self._open_spreads_weighted.clear()
        self._last_open_spread = 0

    def _weighted_avg_open_spread(self) -> float:
        if not self._open_spreads_weighted:
            return 0.0
        total_spread_qty = sum(s * float(q) for s, q in self._open_spreads_weighted)
        total_qty = sum(float(q) for _, q in self._open_spreads_weighted)
        return total_spread_qty / total_qty if total_qty > 0 else 0.0

    # ══════════════════════════════════════════════
    #  O1 order helpers
    # ══════════════════════════════════════════════

    def _round_o1_price(self, price: Decimal) -> Decimal:
        return price.quantize(self.o1_tick_size, rounding=ROUND_HALF_UP)

    async def _place_o1_ioc(self, side: str, qty: Decimal, ref_price: Decimal) -> bool:
        """Place IOC order on 01 Exchange."""
        if side.lower() == "buy":
            aggressive_price = ref_price * Decimal("1.0005")
        else:
            aggressive_price = ref_price * Decimal("0.9995")

        price_rounded = self._round_o1_price(aggressive_price)
        price_raw = int(price_rounded * (Decimal("10") ** self.o1_price_decimals))
        size_raw = int(qty * (Decimal("10") ** self.o1_size_decimals))

        try:
            receipt = self.o1_client.place_order(
                market_id=self.o1_market_id,
                side=side.lower(),
                price_raw=price_raw,
                size_raw=size_raw,
                fill_mode=schema_pb2.FillMode.IMMEDIATE_OR_CANCEL,
                client_order_id=int(time.time() * 1000),
            )
            self.logger.info(
                f"[01] IOC {side} {qty} ref={ref_price:.2f} limit={price_rounded:.2f}")
            return True
        except Exception as e:
            self.logger.error(f"[01] IOC 下单失败: {e}")
            return False

    async def _place_lighter_ioc(self, side: str, qty: Decimal,
                                  ref_price: Decimal) -> bool:
        """Place aggressive limit (IOC-like) on Lighter."""
        is_ask = side.lower() == "sell"
        if is_ask:
            price = ref_price * Decimal("0.9995")
        else:
            price = ref_price * Decimal("1.0005")

        try:
            client_order_index = int(time.time() * 1000)
            _, _, error = await self.lighter_client.create_market_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=int(qty * self.base_amount_multiplier),
                avg_execution_price=int(price * self.price_multiplier),
                is_ask=is_ask,
            )
            if error is not None:
                if "nonce" in str(error).lower():
                    self.lighter_client.nonce_manager.hard_refresh_nonce(
                        int(os.getenv("LIGHTER_API_KEY_INDEX", "0")))
                self.logger.error(f"[Lighter] IOC 下单失败: {error}")
                return False
            self.logger.info(
                f"[Lighter] IOC {side} {qty} ref={ref_price:.2f} limit={price:.2f}")
            return True
        except Exception as e:
            self.logger.error(f"[Lighter] IOC 下单异常: {e}")
            return False

    # ══════════════════════════════════════════════
    #  Open / Close
    # ══════════════════════════════════════════════

    async def _open_positions(self, direction: str, qty: Decimal,
                              l_mid: Decimal, o1_mid: Decimal,
                              spread_bps: float) -> bool:
        if self.dry_run:
            self.logger.info(
                f"[DRY-RUN 开仓] {direction} {qty} 价差={spread_bps:+.2f}bps")
            return True

        ls = self.lighter_feed.snapshot
        if direction == "long_L_short_O1":
            l_ref = Decimal(str(ls.best_ask or l_mid))
            o1_ref = o1_mid
            l_side, o1_side = "buy", "sell"
        else:
            l_ref = Decimal(str(ls.best_bid or l_mid))
            o1_ref = o1_mid
            l_side, o1_side = "sell", "buy"

        t0 = time.time()
        l_ok, o1_ok = await asyncio.gather(
            self._place_lighter_ioc(l_side, qty, l_ref),
            self._place_o1_ioc(o1_side, qty, o1_ref),
        )
        elapsed = (time.time() - t0) * 1000
        self.logger.info(f"[开仓] 两腿已发送 耗时={elapsed:.0f}ms")

        if not l_ok and not o1_ok:
            self.logger.error("[开仓] 两腿都失败")
            return False

        if l_ok and not o1_ok:
            self.logger.warning("[开仓] 01 失败，平掉 Lighter 腿")
            await self._place_lighter_ioc(
                "sell" if l_side == "buy" else "buy", qty, l_ref)
            return False

        if o1_ok and not l_ok:
            self.logger.warning("[开仓] Lighter 失败，平掉 01 腿")
            await self._place_o1_ioc(
                "buy" if o1_side == "sell" else "sell", qty, o1_ref)
            return False

        return True

    async def _close_all_positions(self, l_mid: Decimal, o1_mid: Decimal, reason: str):
        if self.dry_run:
            self.logger.info(f"[DRY-RUN 平仓] {reason}")
            return

        l_pos = abs(self.lighter_position)
        o1_pos = abs(self.o1_position)

        if self.position_direction == "long_L_short_O1":
            l_side, o1_side = "sell", "buy"
        else:
            l_side, o1_side = "buy", "sell"

        ls = self.lighter_feed.snapshot
        l_ref = Decimal(str(ls.best_bid if l_side == "sell" else ls.best_ask or l_mid))
        o1_ref = o1_mid

        t0 = time.time()
        tasks = []
        if l_pos > 0:
            tasks.append(self._place_lighter_ioc(l_side, l_pos, l_ref))
        if o1_pos > 0:
            tasks.append(self._place_o1_ioc(o1_side, o1_pos, o1_ref))
        if tasks:
            await asyncio.gather(*tasks)
        elapsed = (time.time() - t0) * 1000
        self.logger.info(f"[平仓-{reason}] 两腿已发送 耗时={elapsed:.0f}ms")

    # ══════════════════════════════════════════════
    #  Position reconciliation
    # ══════════════════════════════════════════════

    async def _reconcile_positions(self):
        if self.dry_run:
            return
        try:
            o1_pos = await asyncio.to_thread(self._get_o1_position_sync)
            self.o1_position = o1_pos
        except Exception as e:
            self.logger.error(f"[对账] 01 仓位查询失败: {e}")
        try:
            l_data = self.lighter_client.get_account_info()
            accounts = l_data.get("accounts", [{}])
            for pos in accounts[0].get("positions", []):
                if pos.get("symbol") == self.symbol:
                    self.lighter_position = Decimal(str(pos.get("base_amount", 0)))
                    break
            else:
                self.lighter_position = Decimal("0")
        except Exception as e:
            self.logger.error(f"[对账] Lighter 仓位查询失败: {e}")

        net = self.lighter_position + self.o1_position
        if abs(net) > self.order_size * Decimal("0.1"):
            self.logger.warning(
                f"[对账] 不平衡: Lighter={self.lighter_position} "
                f"01={self.o1_position} net={net}")

    def _get_o1_position_sync(self) -> Decimal:
        data = self.o1_client.get_account()
        for pos in data.get("positions", []):
            market_id = pos.get("market_id") or pos.get("marketId")
            if market_id == self.o1_market_id:
                perp = pos.get("perp", {})
                base_size = Decimal(str(
                    perp.get("baseSize", 0) or
                    pos.get("baseSize", 0) or pos.get("size", 0)))
                if base_size == 0:
                    return Decimal("0")
                is_long = perp.get("isLong", True)
                return abs(base_size) if is_long else -abs(base_size)
        return Decimal("0")

    # ══════════════════════════════════════════════
    #  Main loop
    # ══════════════════════════════════════════════

    async def run(self):
        loop = asyncio.get_running_loop()
        for sig in (signal_mod.SIGINT, signal_mod.SIGTERM):
            loop.add_signal_handler(sig, self._handle_signal)

        csv_name = f"o1_lighter_{self.symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self._csv_file = open(csv_name, "w", newline="")
        self._csv_writer = csv.writer(self._csv_file)
        self._csv_writer.writerow([
            "timestamp", "action", "direction",
            "l_side", "l_price", "l_qty",
            "o1_side", "o1_price", "o1_qty",
            "spread_bps", "gross_bps", "net_bps", "cumulative_bps",
        ])

        mode = "DRY-RUN 观测" if self.dry_run else "实盘"
        self.logger.info(
            f"启动 01+Lighter 收敛套利 [{mode}]  {self.symbol}  "
            f"size={self.order_size} max_pos={self.max_position}  "
            f"开仓≥{self.open_bps}bps 平仓≤{self.close_bps}bps  "
            f"超时={self.max_hold_sec}s  "
            f"往返费用={ROUND_TRIP_FEE_BPS:.1f}bps "
            f"(01_taker={O1_TAKER_FEE_BPS}+Lighter={LIGHTER_TAKER_FEE_BPS})")

        # ── 1. Init Lighter ──
        if not self.dry_run:
            pk = os.getenv("API_KEY_PRIVATE_KEY")
            if not pk:
                raise RuntimeError("API_KEY_PRIVATE_KEY not set")
            self.lighter_client = SignerClient(
                private_key=pk,
                api_base="https://mainnet.zklighter.elliot.ai",
                web_socket_base="wss://mainnet.zklighter.elliot.ai/stream",
            )
            self.logger.info("Lighter SignerClient 已初始化")

            market_info = self.lighter_client.get_market_info()
            for m in market_info.get("order_books", []):
                if m["symbol"] == self.symbol:
                    self.lighter_market_index = m["market_id"]
                    self.base_amount_multiplier = pow(10, m["supported_size_decimals"])
                    self.price_multiplier = pow(10, m["supported_price_decimals"])
                    self.lighter_tick_size = Decimal("1") / (
                        Decimal("10") ** m["supported_price_decimals"])
                    break
            self.logger.info(
                f"Lighter market: idx={self.lighter_market_index} "
                f"size_dec={m['supported_size_decimals']} "
                f"price_dec={m['supported_price_decimals']}")

        # ── 2. Init 01 Exchange ──
        if not self.dry_run:
            o1_pk = os.getenv("O1_PRIVATE_KEY")
            o1_acct = os.getenv("O1_ACCOUNT_ID")
            if not o1_pk or not o1_acct:
                raise RuntimeError("O1_PRIVATE_KEY or O1_ACCOUNT_ID not set")
            self.o1_client = O1Client(o1_pk, int(o1_acct))
            self.logger.info("01 Exchange client 已初始化")

            markets = self.o1_client.get_market_info()
            o1_symbol = f"{self.symbol}USD"
            if o1_symbol not in markets:
                raise RuntimeError(f"01 market {o1_symbol} not found")
            info = markets[o1_symbol]
            self.o1_market_id = info["market_id"]
            self.o1_price_decimals = info["price_decimals"]
            self.o1_size_decimals = info["size_decimals"]
            self.o1_tick_size = Decimal("1") / (
                Decimal("10") ** self.o1_price_decimals)
            self.logger.info(
                f"01 market: {o1_symbol} id={self.o1_market_id} "
                f"price_dec={self.o1_price_decimals} size_dec={self.o1_size_decimals}")

            self.logger.info("01 创建 session...")
            self.o1_client.create_session(ttl_seconds=7200)
            self.logger.info("01 session 就绪")

        # ── 3. Start feeds ──
        self.lighter_feed = LighterFeed(self.symbol)
        self.o1_feed = O1Feed(self.symbol)
        bg_tasks = [
            asyncio.create_task(self.lighter_feed.connect()),
            asyncio.create_task(self.o1_feed.connect()),
        ]

        self.logger.info("等待行情数据...")
        for _ in range(30):
            await asyncio.sleep(2)
            ls = self.lighter_feed.snapshot
            o1s = self.o1_feed.snapshot
            if ls.mid and o1s.mid:
                break
            self.logger.info(f"  等待中... L={ls.mid} 01={o1s.mid}")

        ls = self.lighter_feed.snapshot
        o1s = self.o1_feed.snapshot
        if not ls.mid or not o1s.mid:
            self.logger.error("行情超时，退出")
            return

        self.logger.info(f"行情就绪: Lighter={ls.mid:.2f} 01={o1s.mid:.2f}")

        # ── 4. Main loop ──
        cumulative_net_bps = 0.0
        last_status_log = 0.0
        last_reconcile = time.time()
        self.state = STATE_IDLE
        self.logger.info(f"进入主循环 状态={self.state}")

        while not self._stop:
            try:
                await asyncio.sleep(self.interval)
                now = time.time()

                ls = self.lighter_feed.snapshot
                o1s = self.o1_feed.snapshot

                if not ls.mid or not o1s.mid:
                    continue

                l_mid = Decimal(str(ls.mid))
                o1_mid = Decimal(str(o1s.mid))

                mid_spread_bps = float((o1_mid - l_mid) / l_mid * 10000)

                qty_f = float(self.order_size)
                l_vwap_buy = ls.vwap_buy(qty_f)
                l_vwap_sell = ls.vwap_sell(qty_f)
                o1_vwap_buy = o1s.vwap_buy(qty_f)
                o1_vwap_sell = o1s.vwap_sell(qty_f)

                if all([l_vwap_buy, l_vwap_sell, o1_vwap_buy, o1_vwap_sell]):
                    vwap_spread_lo = (o1_vwap_sell - l_vwap_buy) / l_vwap_buy * 10000
                    vwap_spread_ol = (l_vwap_sell - o1_vwap_buy) / o1_vwap_buy * 10000
                    if vwap_spread_lo >= vwap_spread_ol:
                        vwap_spread_bps = vwap_spread_lo
                        vwap_direction = "long_L_short_O1"
                    else:
                        vwap_spread_bps = vwap_spread_ol
                        vwap_direction = "long_O1_short_L"
                else:
                    vwap_spread_bps = 0.0
                    vwap_direction = None

                cur_pos = abs(self.lighter_position)

                if now - last_status_log >= 15:
                    hold_s = ""
                    if self.position_opened_at:
                        hold_s = f" hold={now - self.position_opened_at:.0f}s"
                    self.logger.info(
                        f"[{self.state}] mid={mid_spread_bps:+.2f}bps "
                        f"vwap={vwap_spread_bps:+.2f}bps  "
                        f"L_pos={self.lighter_position} O1_pos={self.o1_position}{hold_s}  "
                        f"L_mid={l_mid:.2f} O1_mid={o1_mid:.2f}  "
                        f"累积净利={cumulative_net_bps:+.2f}bps")
                    last_status_log = now

                # ── Periodic reconciliation ──
                if not self.dry_run and now - last_reconcile > 60:
                    try:
                        await self._reconcile_positions()
                    except Exception:
                        pass
                    last_reconcile = now

                # ────── STATE: IDLE ──────
                if self.state == STATE_IDLE:
                    if now < self._cooldown_until:
                        continue

                    if (vwap_direction
                            and vwap_spread_bps >= self.effective_open_bps
                            and cur_pos < self.max_position):
                        direction = vwap_direction
                        self.state = STATE_OPENING
                        self._last_open_spread = vwap_spread_bps
                        self.logger.info(
                            f"[信号] VWAP价差={vwap_spread_bps:+.2f}bps >= "
                            f"{self.effective_open_bps}  "
                            f"mid={mid_spread_bps:+.2f}bps  方向={direction}")

                        open_ok = await self._open_positions(
                            direction, self.order_size, l_mid, o1_mid, vwap_spread_bps)

                        if not open_ok:
                            self.state = STATE_IDLE
                            self._cooldown_until = time.time() + 5.0
                            continue

                        if self.dry_run:
                            self._open_spreads_weighted.append(
                                (vwap_spread_bps, self.order_size))
                            self.state = STATE_IN_POSITION
                            self.position_opened_at = now
                            self.position_direction = direction
                            self.lighter_position = (
                                self.order_size if "long_L" in direction
                                else -self.order_size)
                            self.o1_position = (
                                -self.order_size if "long_L" in direction
                                else self.order_size)
                        else:
                            await asyncio.sleep(1.5)
                            await self._reconcile_positions()
                            if (abs(self.lighter_position) > 0 and
                                    abs(self.o1_position) > 0):
                                self._open_spreads_weighted.append(
                                    (vwap_spread_bps, self.order_size))
                                self.state = STATE_IN_POSITION
                                self.position_opened_at = now
                                self.position_direction = direction
                            else:
                                self.logger.warning("[开仓] 对账显示未成交，回退IDLE")
                                self._reset_position_state()
                                self.state = STATE_IDLE
                                self._cooldown_until = time.time() + 10.0

                        self._csv_row([
                            datetime.now(timezone.utc).isoformat(), "OPEN", direction,
                            "buy" if "long_L" in direction else "sell",
                            f"{l_mid:.2f}", str(self.order_size),
                            "sell" if "long_L" in direction else "buy",
                            f"{o1_mid:.2f}", str(self.order_size),
                            f"{vwap_spread_bps:.2f}", "", "", "",
                        ])
                        continue

                # ────── STATE: IN_POSITION ──────
                elif self.state == STATE_IN_POSITION:
                    abs_mid_spread = abs(mid_spread_bps)

                    # Close condition
                    if self.close_bps >= 0:
                        _converged = abs_mid_spread <= self.close_bps
                    else:
                        if self.position_direction == "long_L_short_O1":
                            _converged = mid_spread_bps <= self.close_bps
                        else:
                            _converged = mid_spread_bps >= -self.close_bps

                    if _converged:
                        self.logger.info(
                            f"[平仓信号] mid价差收敛到 {mid_spread_bps:+.2f} bps")
                        self.state = STATE_CLOSING
                        open_spread = self._weighted_avg_open_spread()
                        await self._close_all_positions(l_mid, o1_mid, "收敛")

                        if self.position_direction == "long_O1_short_L":
                            gross_bps = open_spread + mid_spread_bps
                        else:
                            gross_bps = open_spread - mid_spread_bps
                        fee_bps = ROUND_TRIP_FEE_BPS / 2
                        net_bps = gross_bps - fee_bps
                        cumulative_net_bps += net_bps

                        n_legs = len(self._open_spreads_weighted)
                        self.logger.info(
                            f"[平仓完成] 加权开仓={open_spread:+.2f}bps({n_legs}笔) "
                            f"平仓mid={mid_spread_bps:+.2f}bps  "
                            f"毛利={gross_bps:+.2f} 费用={fee_bps:.1f} "
                            f"净利={net_bps:+.2f}bps 累积={cumulative_net_bps:+.2f}bps")

                        if self.dry_run:
                            self.lighter_position = Decimal("0")
                            self.o1_position = Decimal("0")
                        else:
                            await asyncio.sleep(1.5)
                            await self._reconcile_positions()

                        self._reset_position_state()
                        self.state = STATE_IDLE
                        self._cooldown_until = time.time() + self._post_trade_cooldown
                        continue

                    # Timeout close
                    if (self.position_opened_at and
                            now - self.position_opened_at > self.max_hold_sec):
                        self.logger.warning(
                            f"[超时平仓] 持仓 "
                            f"{now - self.position_opened_at:.0f}s > {self.max_hold_sec}s")
                        self.state = STATE_CLOSING
                        await self._close_all_positions(l_mid, o1_mid, "超时")

                        if self.dry_run:
                            self.lighter_position = Decimal("0")
                            self.o1_position = Decimal("0")
                        else:
                            await asyncio.sleep(1.5)
                            await self._reconcile_positions()

                        self._reset_position_state()
                        self.state = STATE_IDLE
                        self._cooldown_until = time.time() + self._post_trade_cooldown
                        continue

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"主循环异常: {e}", exc_info=True)
                await asyncio.sleep(5)

        # Cleanup
        self.logger.info("关闭中...")
        if self._csv_file:
            self._csv_file.close()
        for t in bg_tasks:
            t.cancel()

    def _handle_signal(self):
        self.logger.info("收到中断信号")
        self._stop = True


def main():
    parser = argparse.ArgumentParser(description="01+Lighter 收敛套利")
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--size", type=float, default=0.002)
    parser.add_argument("--max-position", type=float, default=0.006)
    parser.add_argument("--open-bps", type=float, default=9.0,
                        help="VWAP 开仓阈值 (默认9, 因为往返费7bps)")
    parser.add_argument("--close-bps", type=float, default=-1.0,
                        help="平仓mid阈值 (负数=需反转)")
    parser.add_argument("--max-hold-sec", type=float, default=1200)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--cooldown", type=float, default=10.0)
    parser.add_argument("--dry-run", action="store_true",
                        help="模拟模式，只观测价差不下单")
    args = parser.parse_args()

    bot = O1LighterArb(args)
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()
