#!/usr/bin/env python3
"""
真实对冲交易模拟器 v2

改进:
  - 支持负数 close-bps（价差必须反转才平仓，捕获更多利润）
  - 可配置超时秒数（默认1200s）
  - 记录交易量、计算每100万美金交易量的手续费和亏损率
  - 多阈值扫描包含负数 close-bps
  - 方向性平仓逻辑（不再用 abs）

成本模型:
  - Lighter: 穿越 spread + 300ms延迟滑移, 0手续费
  - Extended: 穿越 spread + 2.5bps taker手续费/单边
"""

from __future__ import annotations
import argparse, asyncio, sys, time, statistics, datetime
from collections import deque
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Deque

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from feeds.lighter_feed import LighterFeed
from exchanges.extended import ExtendedClient


class _Cfg:
    def __init__(self, d):
        for k, v in d.items():
            setattr(self, k, v)


EXTENDED_FEE_RATE = 0.00025
DELAY_MS = 300
TICK_MS  = 100


def ts() -> str:
    return datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]


def bps_color(val: float) -> str:
    if val > 0:
        return f"\033[92m{val:+.2f}\033[0m"
    elif val < 0:
        return f"\033[91m{val:+.2f}\033[0m"
    return f"{val:+.2f}"


def _should_close_directional(direction: str, spread_bps: float, close_bps: float) -> bool:
    """
    方向性平仓判断。
    close_bps > 0: 价差收敛到 close_bps 以内就平仓
    close_bps = 0: 价差完全收敛（过零点）才平仓
    close_bps < 0: 价差必须反转 |close_bps| 才平仓（捕获更多利润）

    buyL_sellX: 开仓时 spread > 0，我们赚 spread 下降
      → 平仓条件: spread <= close_bps
    buyX_sellL: 开仓时 spread < 0，我们赚 spread 上升
      → 平仓条件: spread >= -close_bps
    """
    if direction == "buyL_sellX":
        return spread_bps <= close_bps
    else:
        return spread_bps >= -close_bps


class SymbolSimulator:

    def __init__(self, sym: str, lighter: LighterFeed, ext: ExtendedClient,
                 open_bps: float, close_bps: float, trade_size_usd: float,
                 timeout_sec: float):
        self.sym = sym
        self.lighter = lighter
        self.ext = ext
        self.open_bps = open_bps
        self.close_bps = close_bps
        self.trade_size_usd = trade_size_usd
        self.timeout_sec = timeout_sec

        self.tick_buf: Deque[dict] = deque(maxlen=20000)
        self.delay_buf: Deque[dict] = deque(maxlen=50)

        self.in_position = False
        self.direction = ""
        self.open_l_fill = 0.0
        self.open_x_fill = 0.0
        self.open_x_fee = 0.0
        self.open_signal_spread = 0.0
        self.open_time = 0.0
        self.open_l_signal_price = 0.0
        self.qty = 0.0
        self.price_at_open = 0.0

        self.trades: List[dict] = []
        self.total_pnl = 0.0
        self.total_pnl_bps = 0.0
        self.total_volume = 0.0
        self.total_fees = 0.0
        self.wins = 0
        self.losses = 0
        self.tick_count = 0
        self.spreads: List[float] = []

    def _get_tick(self) -> Optional[dict]:
        ls = self.lighter.snapshot
        ob = self.ext.orderbook
        if not ls.mid or not ls.best_bid or not ls.best_ask:
            return None
        if not ob or not ob.get("bid") or not ob.get("ask"):
            return None
        return {
            "ts": time.time(),
            "l_bid": ls.best_bid, "l_ask": ls.best_ask, "l_mid": ls.mid,
            "x_bid": float(ob["bid"][0]["p"]),
            "x_ask": float(ob["ask"][0]["p"]),
            "x_mid": (float(ob["bid"][0]["p"]) + float(ob["ask"][0]["p"])) / 2,
        }

    def process_tick(self) -> Optional[str]:
        tick = self._get_tick()
        if not tick:
            return None

        self.tick_buf.append(tick)
        self.delay_buf.append(tick)
        self.tick_count += 1

        spread_bps = (tick["x_mid"] - tick["l_mid"]) / tick["l_mid"] * 10000
        self.spreads.append(spread_bps)

        delay_ticks = max(1, DELAY_MS // TICK_MS)

        if not self.in_position:
            if abs(spread_bps) >= self.open_bps and len(self.delay_buf) > delay_ticks:
                self.direction = "buyL_sellX" if spread_bps > 0 else "buyX_sellL"
                self.open_time = tick["ts"]
                self.open_signal_spread = spread_bps
                price = tick["l_mid"]
                self.price_at_open = price
                self.qty = self.trade_size_usd / price

                signal_tick = self.delay_buf[-delay_ticks - 1] if len(self.delay_buf) > delay_ticks else tick
                delayed_tick = tick

                if self.direction == "buyL_sellX":
                    self.open_l_fill = delayed_tick["l_ask"]
                    self.open_x_fill = signal_tick["x_bid"]
                    self.open_l_signal_price = signal_tick["l_ask"]
                    l_slip = abs(delayed_tick["l_ask"] - signal_tick["l_ask"]) / signal_tick["l_ask"] * 10000
                else:
                    self.open_l_fill = delayed_tick["l_bid"]
                    self.open_x_fill = signal_tick["x_ask"]
                    self.open_l_signal_price = signal_tick["l_bid"]
                    l_slip = abs(delayed_tick["l_bid"] - signal_tick["l_bid"]) / signal_tick["l_bid"] * 10000

                self.open_x_fee = abs(self.open_x_fill) * self.qty * EXTENDED_FEE_RATE
                self.in_position = True

                dir_cn = "买L卖X" if self.direction == "buyL_sellX" else "买X卖L"
                return (
                    f"\033[93m[{ts()}] ★ {self.sym} 开仓 {dir_cn}\033[0m\n"
                    f"  信号价差: {spread_bps:+.2f}bps (阈值={self.open_bps}bps)\n"
                    f"  L bid/ask: {signal_tick['l_bid']:.2f}/{signal_tick['l_ask']:.2f} → "
                    f"延迟后: {delayed_tick['l_bid']:.2f}/{delayed_tick['l_ask']:.2f}\n"
                    f"  X bid/ask: {signal_tick['x_bid']:.2f}/{signal_tick['x_ask']:.2f}\n"
                    f"  L成交: {self.open_l_fill:.4f} (延迟滑移={l_slip:.2f}bps)\n"
                    f"  X成交: {self.open_x_fill:.4f} (费用=${self.open_x_fee:.4f})\n"
                    f"  量: {self.qty:.6f} ({self.sym}) ≈ ${self.trade_size_usd:.0f}"
                )

        else:
            hold_sec = tick["ts"] - self.open_time
            converged = _should_close_directional(self.direction, spread_bps, self.close_bps)
            timed_out = hold_sec > self.timeout_sec
            should_close = converged or timed_out

            if should_close:
                signal_tick = self.delay_buf[-delay_ticks - 1] if len(self.delay_buf) > delay_ticks else tick
                delayed_tick = tick

                if self.direction == "buyL_sellX":
                    close_l = delayed_tick["l_bid"]
                    close_x = signal_tick["x_ask"]
                    l_slip = abs(delayed_tick["l_bid"] - signal_tick["l_bid"]) / signal_tick["l_bid"] * 10000
                    l_pnl = (close_l - self.open_l_fill) * self.qty
                    x_pnl = (self.open_x_fill - close_x) * self.qty
                else:
                    close_l = delayed_tick["l_ask"]
                    close_x = signal_tick["x_bid"]
                    l_slip = abs(delayed_tick["l_ask"] - signal_tick["l_ask"]) / signal_tick["l_ask"] * 10000
                    l_pnl = (self.open_l_fill - close_l) * self.qty
                    x_pnl = (close_x - self.open_x_fill) * self.qty

                close_x_fee = abs(close_x) * self.qty * EXTENDED_FEE_RATE
                total_fee = self.open_x_fee + close_x_fee
                net_pnl = l_pnl + x_pnl - total_fee
                notional = self.price_at_open * self.qty
                net_bps = net_pnl / notional * 10000 if notional > 0 else 0

                trade_vol = notional * 2  # both legs
                self.total_volume += trade_vol
                self.total_fees += total_fee
                self.total_pnl += net_pnl
                self.total_pnl_bps += net_bps
                if net_pnl > 0:
                    self.wins += 1
                else:
                    self.losses += 1

                close_reason = "超时平仓" if timed_out else "收敛平仓"
                trade = {
                    "open_spread": self.open_signal_spread,
                    "close_spread": spread_bps,
                    "l_pnl": l_pnl, "x_pnl": x_pnl,
                    "fee": total_fee, "net_pnl": net_pnl, "net_bps": net_bps,
                    "hold_sec": hold_sec, "direction": self.direction,
                    "volume": trade_vol,
                }
                self.trades.append(trade)
                total_n = len(self.trades)
                wr = self.wins / total_n * 100 if total_n > 0 else 0

                vol_m = self.total_volume / 1_000_000 if self.total_volume > 0 else 0
                loss_per_m = (self.total_pnl / self.total_volume * 1_000_000) if self.total_volume > 0 else 0
                loss_pct_per_m = (self.total_pnl / self.total_volume * 100) if self.total_volume > 0 else 0
                fee_bps_per_m = (self.total_fees / self.total_volume * 10000) if self.total_volume > 0 else 0

                result = (
                    f"\033[96m[{ts()}] ✦ {self.sym} {close_reason}\033[0m "
                    f"(持仓 {hold_sec:.1f}s)\n"
                    f"  平仓价差: {spread_bps:+.2f}bps (阈值={self.close_bps}bps)\n"
                    f"  L平仓: {close_l:.4f}  X平仓: {close_x:.4f}  延迟滑移: {l_slip:.2f}bps\n"
                    f"  ┌─ P&L ───────────────────────────────────┐\n"
                    f"  │ L腿: {bps_color(l_pnl/notional*10000 if notional else 0)}bps (${l_pnl:+.4f})\n"
                    f"  │ X腿: {bps_color(x_pnl/notional*10000 if notional else 0)}bps (${x_pnl:+.4f})\n"
                    f"  │ 费用: \033[91m-{total_fee:.4f}$\033[0m ({total_fee/notional*10000:.2f}bps)\n"
                    f"  │ \033[1m净利: {bps_color(net_bps)}bps (${net_pnl:+.4f})\033[0m\n"
                    f"  └──────────────────────────────────────────┘\n"
                    f"  📊 {total_n}笔 W{self.wins}/L{self.losses} ({wr:.0f}%) "
                    f"| 累计{bps_color(self.total_pnl_bps)}bps (${self.total_pnl:+.4f})\n"
                    f"  💰 交易量${self.total_volume:,.0f} "
                    f"| 每$1M亏损: ${loss_per_m:+,.2f} ({loss_pct_per_m:+.4f}%) "
                    f"| 费率: {fee_bps_per_m:.2f}bps/1M"
                )

                self.in_position = False
                self.direction = ""
                return result

        return None

    def status_line(self) -> str:
        tick = self._get_tick()
        if not tick:
            return f"  {self.sym}: 等待行情..."

        spread = (tick["x_mid"] - tick["l_mid"]) / tick["l_mid"] * 10000
        l_sp = (tick["l_ask"] - tick["l_bid"]) / tick["l_mid"] * 10000
        x_sp = (tick["x_ask"] - tick["x_bid"]) / tick["x_mid"] * 10000

        if self.in_position:
            hold = time.time() - self.open_time
            dir_cn = "买L卖X" if self.direction == "buyL_sellX" else "买X卖L"
            pos_str = f" | \033[93m持仓 {dir_cn} {hold:.0f}s 开={self.open_signal_spread:+.2f}\033[0m"
        else:
            pos_str = " | 空仓"

        stat = f"{len(self.trades)}笔 W{self.wins}/L{self.losses}" if self.trades else "0笔"
        vol_k = self.total_volume / 1000

        return (
            f"  {self.sym}: L={tick['l_mid']:.2f} X={tick['x_mid']:.2f} "
            f"差={bps_color(spread)}bps "
            f"(L={l_sp:.1f} X={x_sp:.1f}){pos_str} | {stat} "
            f"累计={bps_color(self.total_pnl_bps)}bps 量${vol_k:.0f}k"
        )


async def run_simulation(symbols: List[str], duration_sec: int,
                         trade_size_usd: float, open_bps: float,
                         close_bps: float, timeout_sec: float):
    load_dotenv(".env")

    close_desc = f"{close_bps}bps" if close_bps >= 0 else f"{close_bps}bps(反转)"
    print("\033[1m" + "=" * 110 + "\033[0m")
    print(f"\033[1m  🔄 真实对冲交易模拟器 v2\033[0m")
    print(f"  Lighter(免费+300ms延迟) ↔ Extended(taker 2.5bps/侧) | 双 taker 市价成交")
    print(f"  币种: {', '.join(symbols)}  |  运行: {duration_sec}s  |  每笔: ${trade_size_usd}")
    print(f"  开仓: ≥{open_bps}bps  |  平仓: ≤{close_desc}  |  超时: {timeout_sec:.0f}s")
    print("\033[1m" + "=" * 110 + "\033[0m")

    lighter_feeds: Dict[str, LighterFeed] = {}
    ext_clients: Dict[str, ExtendedClient] = {}

    for sym in symbols:
        lighter_feeds[sym] = LighterFeed(sym)
        cfg = _Cfg({
            "ticker": sym, "contract_id": "", "quantity": Decimal("0.1"),
            "tick_size": Decimal("0.01"), "close_order_side": "sell",
        })
        ext_clients[sym] = ExtendedClient(cfg)

    for sym in symbols:
        asyncio.create_task(lighter_feeds[sym].connect())
        await ext_clients[sym].connect()
        await ext_clients[sym].get_contract_attributes()
        print(f"  ✓ {sym} 连接完成")

    print("\n  等待行情 (5s)...")
    await asyncio.sleep(5)

    for sym in symbols:
        ls = lighter_feeds[sym].snapshot
        ob = ext_clients[sym].orderbook
        l_ok = ls.mid is not None and ls.mid > 0
        x_ok = ob and ob.get("bid") and ob.get("ask")
        l_str = f"{ls.mid:.2f}" if l_ok else "N/A"
        x_str = f"{float(ob['bid'][0]['p']):.2f}/{float(ob['ask'][0]['p']):.2f}" if x_ok else "N/A"
        print(f"  {'✓' if l_ok and x_ok else '✗'} {sym} L={l_str}  X={x_str}")

    sims: Dict[str, SymbolSimulator] = {}
    for sym in symbols:
        sims[sym] = SymbolSimulator(
            sym, lighter_feeds[sym], ext_clients[sym],
            open_bps, close_bps, trade_size_usd, timeout_sec,
        )

    print(f"\n\033[1m{'─'*110}\033[0m")
    print(f"\033[1m  实时模拟开始  ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\033[0m")
    print(f"\033[1m{'─'*110}\033[0m\n")

    start_time = time.time()
    last_status = 0

    try:
        while time.time() - start_time < duration_sec:
            now = time.time()

            for sym in symbols:
                log = sims[sym].process_tick()
                if log:
                    print(log)
                    print()

            if now - last_status >= 5:
                elapsed = now - start_time
                remaining = duration_sec - elapsed
                pct = elapsed / duration_sec * 100
                print(f"\033[2m[{ts()}] ── ({elapsed:.0f}s/{duration_sec}s  "
                      f"{pct:.0f}%  剩余{remaining:.0f}s) ──\033[0m")
                for sym in symbols:
                    print(f"\033[2m{sims[sym].status_line()}\033[0m")
                print()
                last_status = now

            await asyncio.sleep(TICK_MS / 1000.0)

    except KeyboardInterrupt:
        print("\n  ⚡ 中断")

    # ═══ Summary ═══
    total_time = time.time() - start_time
    print(f"\n\033[1m{'═'*110}\033[0m")
    print(f"\033[1m  总结报告  (运行 {total_time:.0f}s)  "
          f"开仓≥{open_bps}bps  平仓≤{close_desc}  超时{timeout_sec:.0f}s\033[0m")
    print(f"\033[1m{'═'*110}\033[0m")

    grand_vol = 0.0
    grand_pnl = 0.0
    grand_fee = 0.0
    grand_trades = 0

    for sym in symbols:
        s = sims[sym]
        grand_vol += s.total_volume
        grand_pnl += s.total_pnl
        grand_fee += s.total_fees
        grand_trades += len(s.trades)

        print(f"\n\033[1m  ── {sym} ──\033[0m  ({s.tick_count} ticks)")

        if not s.trades:
            print(f"  ⚠ 无交易触发 (阈值={open_bps}bps)")
            if s.spreads:
                abs_sp = [abs(x) for x in s.spreads]
                srt = sorted(abs_sp)
                print(f"  价差: 均值={statistics.mean(s.spreads):+.2f}  "
                      f"|abs| P50={srt[len(srt)//2]:.2f}  "
                      f"P90={srt[int(len(srt)*0.9)]:.2f}  "
                      f"P99={srt[int(len(srt)*0.99)]:.2f}  "
                      f"Max={srt[-1]:.2f}bps")
            continue

        wr = s.wins / len(s.trades) * 100
        pnl_list = [t["net_bps"] for t in s.trades]
        hold_list = [t["hold_sec"] for t in s.trades]
        vol_m = s.total_volume / 1_000_000
        loss_per_m = s.total_pnl / s.total_volume * 1_000_000 if s.total_volume > 0 else 0
        loss_pct = s.total_pnl / s.total_volume * 100 if s.total_volume > 0 else 0
        fee_bps = s.total_fees / s.total_volume * 10000 if s.total_volume > 0 else 0

        print(f"  交易: {len(s.trades)}笔  W{s.wins}/L{s.losses}  胜率{wr:.1f}%")
        print(f"  净利: {bps_color(s.total_pnl_bps)}bps (${s.total_pnl:+.4f})")
        if len(pnl_list) > 1:
            print(f"  单笔: 均值={statistics.mean(pnl_list):+.2f}bps  "
                  f"最佳={max(pnl_list):+.2f}  最差={min(pnl_list):+.2f}  "
                  f"σ={statistics.stdev(pnl_list):.2f}")
        print(f"  持仓: 均值={statistics.mean(hold_list):.1f}s  "
              f"最短={min(hold_list):.1f}s  最长={max(hold_list):.1f}s")
        print(f"  \033[1m交易量: ${s.total_volume:,.0f} ({vol_m:.4f}M)\033[0m")
        print(f"  \033[1m每$1M亏损: ${loss_per_m:+,.2f} ({loss_pct:+.4f}%)\033[0m  "
              f"目标≤0.001%={'✓ 达标' if abs(loss_pct) <= 0.001 else '✗ 未达标'}")
        print(f"  每$1M费用: {fee_bps:.2f}bps (=${s.total_fees/vol_m:,.2f}/M)" if vol_m > 0 else "")

        if s.spreads:
            abs_sp = [abs(x) for x in s.spreads]
            srt = sorted(abs_sp)
            print(f"  价差: |abs| P50={srt[len(srt)//2]:.2f}  "
                  f"P90={srt[int(len(srt)*0.9)]:.2f}  "
                  f"P99={srt[int(len(srt)*0.99)]:.2f}  "
                  f"Max={srt[-1]:.2f}bps")

        print(f"\n  {'#':>3} {'方向':>8} {'开差':>6} {'平差':>6} "
              f"{'L$':>8} {'X$':>8} {'费$':>7} {'净$':>8} {'净bps':>7} {'持s':>5}")
        print(f"  {'─'*75}")
        for i, t in enumerate(s.trades):
            d = "买L卖X" if t["direction"] == "buyL_sellX" else "买X卖L"
            print(f"  {i+1:>3} {d:>8} {t['open_spread']:>+6.1f} {t['close_spread']:>+6.1f} "
                  f"{t['l_pnl']:>+8.4f} {t['x_pnl']:>+8.4f} {-t['fee']:>7.4f} "
                  f"{t['net_pnl']:>+8.4f} {bps_color(t['net_bps']):>16} {t['hold_sec']:>5.1f}")

    # Grand total
    if grand_vol > 0:
        g_m = grand_vol / 1_000_000
        g_loss_pct = grand_pnl / grand_vol * 100
        g_fee_bps = grand_fee / grand_vol * 10000
        print(f"\n\033[1m{'─'*110}\033[0m")
        print(f"\033[1m  全币种合计\033[0m")
        print(f"  交易: {grand_trades}笔  交易量: ${grand_vol:,.0f} ({g_m:.4f}M)")
        print(f"  净利: ${grand_pnl:+.4f}")
        print(f"  \033[1m每$1M亏损率: {g_loss_pct:+.4f}%  "
              f"({'✓ 达标 ≤0.001%' if abs(g_loss_pct) <= 0.001 else '✗ 未达标 >0.001%'})\033[0m")
        print(f"  每$1M费用: {g_fee_bps:.2f}bps")

    # ═══ Multi-threshold backtest ═══
    print(f"\n\033[1m{'═'*110}\033[0m")
    print(f"\033[1m  多阈值扫描 (含负数close-bps, 超时{timeout_sec:.0f}s)\033[0m")
    print(f"\033[1m{'═'*110}\033[0m")

    for sym in symbols:
        s = sims[sym]
        ticks = list(s.tick_buf)
        if len(ticks) < 100:
            print(f"\n  {sym}: ticks不足, 跳过")
            continue

        price = ticks[0]["l_mid"]
        qty_bt = trade_size_usd / price
        delay_n = max(1, DELAY_MS // TICK_MS)

        print(f"\n  \033[1m{sym}\033[0m  ({len(ticks)} ticks, ${price:.2f})")
        print(f"  {'开bps':>6} {'平bps':>6} | {'笔':>4} {'W':>3} {'L':>3} "
              f"{'胜率':>5} | {'均bps':>7} {'总bps':>8} {'总$':>8} "
              f"| {'量$':>8} {'$1M亏%':>8} {'$1M费bps':>8} | {'均持s':>5}")
        print(f"  {'─'*110}")

        for ot in [3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 7.0, 8.0, 10.0]:
            for ct in [-2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0]:
                bt_trades = []
                bt_vol = 0.0
                bt_fee = 0.0
                in_pos = False
                o_data = {}

                for i in range(len(ticks) - delay_n):
                    t = ticks[i]
                    sp = (t["x_mid"] - t["l_mid"]) / t["l_mid"] * 10000

                    if not in_pos:
                        if abs(sp) >= ot:
                            d = "buyL_sellX" if sp > 0 else "buyX_sellL"
                            td = ticks[i + delay_n]
                            if d == "buyL_sellX":
                                ol = td["l_ask"]; ox = t["x_bid"]
                            else:
                                ol = td["l_bid"]; ox = t["x_ask"]
                            o_data = {"dir": d, "ol": ol, "ox": ox, "ot": t["ts"],
                                      "ofee": abs(ox) * qty_bt * EXTENDED_FEE_RATE, "osp": sp}
                            in_pos = True
                    else:
                        ht = t["ts"] - o_data["ot"]
                        converged = _should_close_directional(o_data["dir"], sp, ct)
                        if converged or ht > timeout_sec:
                            td = ticks[i + delay_n]
                            if o_data["dir"] == "buyL_sellX":
                                cl = td["l_bid"]; cx = t["x_ask"]
                                lp = (cl - o_data["ol"]) * qty_bt
                                xp = (o_data["ox"] - cx) * qty_bt
                            else:
                                cl = td["l_ask"]; cx = t["x_bid"]
                                lp = (o_data["ol"] - cl) * qty_bt
                                xp = (cx - o_data["ox"]) * qty_bt
                            cf = abs(cx) * qty_bt * EXTENDED_FEE_RATE
                            tf = o_data["ofee"] + cf
                            np_ = lp + xp - tf
                            nb = np_ / (price * qty_bt) * 10000
                            tv = price * qty_bt * 2
                            bt_trades.append({"net_bps": nb, "net_pnl": np_, "hold": ht, "vol": tv})
                            bt_vol += tv
                            bt_fee += tf
                            in_pos = False

                if not bt_trades:
                    continue

                n = len(bt_trades)
                w = sum(1 for x in bt_trades if x["net_pnl"] > 0)
                lo = n - w
                wr = w / n * 100
                avg_b = statistics.mean([x["net_bps"] for x in bt_trades])
                tot_b = sum(x["net_bps"] for x in bt_trades)
                tot_d = sum(x["net_pnl"] for x in bt_trades)
                avg_h = statistics.mean([x["hold"] for x in bt_trades])
                loss_pct = tot_d / bt_vol * 100 if bt_vol > 0 else 0
                fee_per_m = bt_fee / bt_vol * 10000 if bt_vol > 0 else 0
                vol_k = bt_vol / 1000

                flag = ""
                if abs(loss_pct) <= 0.001:
                    flag = " ✓✓"
                elif avg_b > 0:
                    flag = " ★"
                elif abs(loss_pct) <= 0.01:
                    flag = " ◎"

                print(f"  {ot:>6.1f} {ct:>+6.1f} | {n:>4} {w:>3} {lo:>3} "
                      f"{wr:>4.0f}% | {avg_b:>+7.2f} {tot_b:>+8.2f} {tot_d:>+8.4f} "
                      f"| {vol_k:>7.0f}k {loss_pct:>+8.4f} {fee_per_m:>8.2f} "
                      f"| {avg_h:>5.1f}{flag}")

    print(f"\n  标记: ★=均值盈利  ✓✓=每$1M亏≤0.001%  ◎=每$1M亏≤0.01%")
    print(f"\033[1m{'═'*110}\033[0m\n")

    for sym in symbols:
        try:
            await lighter_feeds[sym].disconnect()
        except Exception:
            pass
        try:
            await asyncio.wait_for(ext_clients[sym].disconnect(), timeout=3)
        except Exception:
            pass


def main():
    p = argparse.ArgumentParser(description="真实对冲交易模拟器 v2")
    p.add_argument("--symbols", nargs="+", default=["ETH", "BTC"])
    p.add_argument("--duration", type=int, default=900, help="运行秒数")
    p.add_argument("--trade-size-usd", type=float, default=1000)
    p.add_argument("--open-bps", type=float, default=5.0)
    p.add_argument("--close-bps", type=float, default=-1.0,
                   help="平仓阈值(可为负数: -1=价差反转1bps)")
    p.add_argument("--timeout", type=float, default=1200, help="持仓超时秒数")
    args = p.parse_args()

    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass

    asyncio.run(run_simulation(
        args.symbols, args.duration, args.trade_size_usd,
        args.open_bps, args.close_bps, args.timeout,
    ))


if __name__ == "__main__":
    main()
