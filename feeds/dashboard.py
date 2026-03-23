"""
Terminal dashboard for the spread scanner.
Renders a coloured, auto-refreshing table with spread rankings
and writes every tick to a CSV for later analysis.
"""

import csv
import os
import sys
import time
from datetime import datetime, timezone
from typing import List, Optional

from .spread_calculator import SpreadResult
from .funding_monitor import FundingSpread


# ─── ANSI colours ───────────────────────────────────────────
class C:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RED     = "\033[91m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    CYAN    = "\033[96m"
    WHITE   = "\033[97m"
    BG_BLK  = "\033[40m"
    MAGENTA = "\033[95m"


def _c(val: float, threshold: float = 0.0) -> str:
    """Colour a numeric value: green if > threshold, red if < -threshold."""
    if val > threshold:
        return f"{C.GREEN}{val:+8.2f}{C.RESET}"
    elif val < -threshold:
        return f"{C.RED}{val:+8.2f}{C.RESET}"
    return f"{C.DIM}{val:+8.2f}{C.RESET}"


def _status_dot(connected: bool) -> str:
    return f"{C.GREEN}●{C.RESET}" if connected else f"{C.RED}○{C.RESET}"


class Dashboard:
    """
    Renders the scanner dashboard to the terminal.
    Optionally appends each tick to a CSV file.
    """

    def __init__(
        self,
        symbol: str,
        csv_dir: str = "logs",
        csv_enabled: bool = True,
    ):
        self.symbol = symbol.upper()
        self.csv_enabled = csv_enabled
        self._csv_path = os.path.join(csv_dir, f"spread_scanner_{self.symbol}.csv")
        self._csv_initialized = False
        if csv_enabled:
            os.makedirs(csv_dir, exist_ok=True)

    def render(
        self,
        spreads: List[SpreadResult],
        funding: Optional[List[FundingSpread]] = None,
        trade_size_usd: float = 10_000.0,
    ) -> None:
        """Clear screen and draw the full dashboard."""
        # Clear
        sys.stdout.write("\033[2J\033[H")

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        lighter_mid = spreads[0].lighter_mid if spreads else 0.0

        # ── Header ──
        print(f"{C.BOLD}{C.CYAN}{'═' * 90}{C.RESET}")
        print(f"{C.BOLD}{C.WHITE}  Lighter vs All Exchanges ─ {self.symbol} Spread Scanner{C.RESET}")
        print(f"  {C.DIM}时间: {now_str}  |  Lighter Mid: ${lighter_mid:,.2f}  |  "
              f"交易量: ${trade_size_usd:,.0f}{C.RESET}")
        print(f"{C.BOLD}{C.CYAN}{'═' * 90}{C.RESET}")
        print()

        # ── Spread table header ──
        print(f"  {C.DIM}Mid价差=对手Mid-Lighter Mid | 往返毛利=腿1+腿2(见下) | "
              f"费用=2×(L_t+X_t) taker | 净利=往返毛利-费用 | "
              f"腿1=(X卖-L买) 腿2=(L卖-X买) 恒相加=往返毛利{C.RESET}")
        hdr = (
            f"  {'排名':>4}  {'状态':>2}  {'交易所':<12}  {'Mid价格':>12}  "
            f"{'Mid价差':>8}  {'腿1':>7}  {'腿2':>7}  {'往返毛':>7}  {'往返费':>7}  "
            f"{'滑L':>6}  {'滑X':>6}  {'净利':>7}  {'方向':<22}"
        )
        print(f"{C.BOLD}{hdr}{C.RESET}")
        print(f"  {C.DIM}{'─' * 110}{C.RESET}")

        for i, r in enumerate(spreads, 1):
            rank_str = f"{i:>4}"
            status = _status_dot(r.lighter_connected and r.exchange_connected)
            name = f"{r.exchange:<12}"
            mid = f"${r.exchange_mid:>11,.2f}" if r.exchange_mid else f"{'N/A':>12}"
            raw = _c(r.raw_spread_bps)
            l1 = f"{C.DIM}{r.open_leg_buyL_sellX_bps:+7.1f}{C.RESET}"
            l2 = f"{C.DIM}{r.open_leg_buyX_sellL_bps:+7.1f}{C.RESET}"
            gr = f"{C.DIM}{r.round_trip_gross_bps:+7.1f}{C.RESET}"
            fee = f"{C.DIM}{r.total_fee_bps:7.1f}{C.RESET}"
            sl = f"{C.DIM}{r.slippage_lighter_bps:6.1f}{C.RESET}"
            se = f"{C.DIM}{r.slippage_exchange_bps:6.1f}{C.RESET}"
            net = _c(r.net_profit_bps, threshold=0.5)
            direction = r.direction

            if r.net_profit_bps > 0:
                net_col = C.GREEN
            elif r.net_profit_bps > -2:
                net_col = C.YELLOW
            else:
                net_col = C.DIM

            print(f"  {rank_str}  {status}  {name}  {mid}  {raw}  {l1}  {l2}  {gr}  {fee}  "
                  f"{sl}  {se}  {net}  {net_col}{direction}{C.RESET}")

        # ── Depth summary ──
        print()
        print(f"  {C.BOLD}深度概览 (前5档, USD){C.RESET}")
        print(f"  {C.DIM}{'─' * 60}{C.RESET}")
        for r in spreads:
            bar_l = self._bar(r.depth_usd_lighter, 5_000_000)
            bar_e = self._bar(r.depth_usd_exchange, 5_000_000)
            print(f"  {r.exchange:<14}  Lighter: ${r.depth_usd_lighter:>12,.0f} {bar_l}  "
                  f"{r.exchange:<8}: ${r.depth_usd_exchange:>12,.0f} {bar_e}")

        # ── Funding rates ──
        if funding:
            print()
            print(f"{C.BOLD}{C.CYAN}{'═' * 90}{C.RESET}")
            print(f"  {C.BOLD}资金费率差异 (归一化为每小时费率){C.RESET}")
            print(f"  {'交易所':<14}  {'交易所费率/h':>14}  {'Lighter费率/h':>14}  {'1h差异':>12}  {'年化差异':>10}")
            print(f"  {C.DIM}{'─' * 70}{C.RESET}")
            for fs in funding:
                ex_r = f"{fs.exchange_rate * 100:>13.6f}%" if fs.exchange_rate is not None else f"{'N/A':>14}"
                li_r = f"{fs.lighter_rate * 100:>13.6f}%" if fs.lighter_rate is not None else f"{'N/A':>14}"
                sp1 = f"{fs.spread_1h * 100:>11.6f}%" if fs.spread_1h is not None else f"{'N/A':>12}"
                ann = _c(fs.annualized_pct, 5.0) + "%" if fs.annualized_pct is not None else f"{'N/A':>10}"
                print(f"  {fs.exchange:<14}  {ex_r}  {li_r}  {sp1}  {ann}")

        print(f"\n  {C.DIM}按 Ctrl+C 退出{C.RESET}")
        sys.stdout.flush()

        # ── CSV logging ──
        if self.csv_enabled:
            self._write_csv(spreads)

    @staticmethod
    def _bar(value: float, max_val: float, width: int = 15) -> str:
        ratio = min(value / max_val, 1.0) if max_val else 0
        filled = int(ratio * width)
        return f"{C.GREEN}{'█' * filled}{C.DIM}{'░' * (width - filled)}{C.RESET}"

    def _write_csv(self, spreads: List[SpreadResult]) -> None:
        try:
            if not self._csv_initialized:
                if not os.path.exists(self._csv_path):
                    with open(self._csv_path, "w", newline="") as f:
                        csv.writer(f).writerow([
                            "timestamp", "exchange", "lighter_mid", "exchange_mid",
                            "raw_spread_bps",
                            "open_leg_buyL_sellX_bps", "open_leg_buyX_sellL_bps",
                            "round_trip_gross_bps", "total_fee_bps",
                            "slippage_lighter_bps", "slippage_exchange_bps",
                            "net_profit_bps", "direction",
                            "lighter_spread_bps", "exchange_spread_bps",
                            "depth_usd_lighter", "depth_usd_exchange",
                        ])
                self._csv_initialized = True

            ts = datetime.now(timezone.utc).isoformat()
            with open(self._csv_path, "a", newline="") as f:
                w = csv.writer(f)
                for r in spreads:
                    w.writerow([
                        ts, r.exchange,
                        f"{r.lighter_mid:.4f}", f"{r.exchange_mid:.4f}",
                        f"{r.raw_spread_bps:.2f}",
                        f"{r.open_leg_buyL_sellX_bps:.2f}", f"{r.open_leg_buyX_sellL_bps:.2f}",
                        f"{r.round_trip_gross_bps:.2f}", f"{r.total_fee_bps:.2f}",
                        f"{r.slippage_lighter_bps:.2f}", f"{r.slippage_exchange_bps:.2f}",
                        f"{r.net_profit_bps:.2f}", r.direction,
                        f"{r.lighter_spread_bps:.2f}", f"{r.exchange_spread_bps:.2f}",
                        f"{r.depth_usd_lighter:.0f}", f"{r.depth_usd_exchange:.0f}",
                    ])
        except Exception:
            pass
