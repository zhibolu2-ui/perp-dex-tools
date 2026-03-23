#!/usr/bin/env python3
"""
终端：价差收敛策略模拟（与 Web 面板逻辑一致）

|Mid 价差| ≥ 开仓阈值 → 虚拟开仓；|Mid 价差| ≤ 平仓阈值 → 虚拟平仓。
盈亏按开/平两个时刻的 VWAP 计算，并扣 2×(L_taker+X_taker) bps。

用法:
  python convergence_monitor.py --symbol ETH
  python convergence_monitor.py -s ETH --open 10 --close 4 --interval 1
  python convergence_monitor.py -e hyperliquid,grvt --open 8 --close 3
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys

import spread_scanner as ss
from feeds.convergence_tracker import ConvergenceTrackerGroup


def clear():
    sys.stdout.write("\033[2J\033[H")


async def run(args):
    M = ss._lazy_imports()
    symbol = args.symbol.upper()

    if args.exchanges:
        names = [x.strip().lower() for x in args.exchanges.split(",")]
    else:
        names = list(ss.DEFAULT_DEX_EXCHANGES)

    lighter = M["LighterFeed"](symbol, market_index=args.lighter_market)
    targets = []
    for name in names:
        feed = ss._create_feed(M, name, symbol)
        if feed:
            targets.append(feed)

    if not targets:
        print("没有可用的目标交易所")
        return

    conv = ConvergenceTrackerGroup()
    conv.configure_all(args.open, args.close)

    tasks = [asyncio.create_task(lighter.connect())]
    for f in targets:
        tasks.append(asyncio.create_task(f.connect()))

    await asyncio.sleep(4)
    stop = asyncio.Event()

    def on_sig():
        stop.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, on_sig)
        except NotImplementedError:
            pass

    try:
        while not stop.is_set():
            ls = lighter.snapshot
            lt = lighter.TAKER_FEE_BPS
            conv.tick_all(ls, targets, args.size, lt, args.open, args.close)
            rows = conv.snapshot(ls, targets, args.size, lt, args.open, args.close)

            clear()
            print(f"  收敛策略监控  {symbol}  名义 ${args.size:,.0f}  "
                  f"开仓≥{args.open}bps  平仓≤{args.close}bps  刷新 {args.interval}s")
            print(f"  Lighter Mid: {ls.mid:,.2f}  |  Ctrl+C 退出\n")
            hdr = (
                f"  {'交易所':<14} {'状态':<8} {'|ΔMid|':>8} {'持仓s':>8} {'方向':<10} "
                f"{'笔数':>5} {'上次净':>9} {'平均净':>9} {'胜率':>7}"
            )
            print(hdr)
            print("  " + "─" * 86)
            for r in rows:
                st = "持仓" if r.get("in_position") else "空仓"
                pos_s = r.get("seconds_in_position")
                pos_s = f"{pos_s:.0f}" if pos_s is not None else "—"
                d = r.get("direction") or "—"
                cn = r.get("completed_count", 0)
                ln = r.get("last_net_bps")
                an = r.get("avg_net_bps")
                wr = r.get("win_rate_pct")
                ln_s = f"{ln:+.2f}" if ln is not None else "—"
                an_s = f"{an:+.2f}" if an is not None else "—"
                wr_s = f"{wr:.1f}%" if wr is not None else "—"
                print(
                    f"  {r['exchange']:<14} {st:<8} {r['abs_spread_bps']:8.2f} "
                    f"{pos_s:>8} {d:<10} {cn:5d} {ln_s:>9} {an_s:>9} {wr_s:>7}"
                )
            sys.stdout.flush()

            try:
                await asyncio.wait_for(stop.wait(), timeout=args.interval)
            except asyncio.TimeoutError:
                pass
    finally:
        await lighter.disconnect()
        for f in targets:
            await f.disconnect()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main():
    p = argparse.ArgumentParser(description="价差收敛策略终端监控")
    p.add_argument("-s", "--symbol", default="ETH")
    p.add_argument("--size", type=float, default=10_000.0)
    p.add_argument("-e", "--exchanges", default=None)
    p.add_argument("--open", type=float, default=8.0, help="开仓 |ΔMid| ≥ (bps)")
    p.add_argument("--close", type=float, default=3.0, help="平仓 |ΔMid| ≤ (bps)")
    p.add_argument("--interval", type=float, default=2.0, help="刷新秒数")
    p.add_argument("--lighter-market", type=int, default=None)
    args = p.parse_args()
    if args.close >= args.open:
        args.close = max(0.0, args.open - 0.5)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\n已停止。")


if __name__ == "__main__":
    main()
