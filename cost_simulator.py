#!/usr/bin/env python3
"""
开平仓成本模拟器 — 测量 Lighter vs Extended 真实执行损耗

模拟条件：
  - Lighter: 免费账户 (0 手续费, 300ms 额外延迟)
  - Extended: 市价 taker (0.025% = 2.5bps 手续费/单边)
  - 双 taker 策略：两所同时下市价单

测量项目：
  1. Lighter bid-ask spread (每次穿越成本)
  2. Extended bid-ask spread (每次穿越成本)
  3. Lighter 300ms 延迟价格滑移 (delay slippage)
  4. Extended 手续费 (固定 2.5bps/单边)
  5. 总成本 = 以上全部 × 2 (开仓+平仓)

用法:
  python cost_simulator.py --symbols ETH BTC SOL --samples 200
"""

from __future__ import annotations
import argparse, asyncio, os, signal, sys, time, statistics
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from feeds.lighter_feed import LighterFeed
from exchanges.extended import ExtendedClient


class _Cfg:
    def __init__(self, d):
        for k, v in d.items():
            setattr(self, k, v)


EXTENDED_TAKER_FEE_BPS = 2.5
LIGHTER_DELAY_MS = 300


async def run_simulator(symbols: List[str], samples: int, trade_size_usd: float):
    load_dotenv(".env")

    print("=" * 80)
    print(f"  成本模拟器 — Lighter(免费,+300ms) vs Extended(taker 2.5bps)")
    print(f"  币种: {', '.join(symbols)}  |  采样: {samples}次  |  模拟单量: ${trade_size_usd}")
    print("=" * 80)

    # Initialize feeds per symbol
    lighter_feeds: Dict[str, LighterFeed] = {}
    ext_clients: Dict[str, ExtendedClient] = {}

    for sym in symbols:
        lighter_feeds[sym] = LighterFeed(sym)
        cfg = _Cfg({
            "ticker": sym, "contract_id": "",
            "quantity": Decimal("0.1"), "tick_size": Decimal("0.01"),
            "close_order_side": "sell",
        })
        ext_clients[sym] = ExtendedClient(cfg)

    # Connect all
    tasks = []
    for sym in symbols:
        tasks.append(asyncio.create_task(lighter_feeds[sym].connect()))
        await ext_clients[sym].connect()
        await ext_clients[sym].get_contract_attributes()

    print("等待行情就绪 (5s)...")
    await asyncio.sleep(5)

    # Verify feeds
    for sym in symbols:
        ls = lighter_feeds[sym].snapshot
        if not ls.mid:
            print(f"  ⚠ {sym} Lighter 行情未就绪")
        else:
            print(f"  ✓ {sym} Lighter mid={ls.mid:.2f}")

        ob = ext_clients[sym].orderbook
        if ob and ob.get("bid") and ob.get("ask"):
            bid = float(ob["bid"][0]["p"])
            ask = float(ob["ask"][0]["p"])
            print(f"  ✓ {sym} Extended bid={bid:.2f} ask={ask:.2f}")
        else:
            print(f"  ⚠ {sym} Extended 行情未就绪")

    print(f"\n开始采集 {samples} 个样本 (每秒1个, ~{samples}秒)...\n")

    # Data collection
    data: Dict[str, List[dict]] = {sym: [] for sym in symbols}

    for i in range(samples):
        for sym in symbols:
            ls = lighter_feeds[sym].snapshot
            if not ls.mid or not ls.best_bid or not ls.best_ask:
                continue

            l_bid = ls.best_bid
            l_ask = ls.best_ask
            l_mid = ls.mid
            l_spread_bps = (l_ask - l_bid) / l_mid * 10000

            ob = ext_clients[sym].orderbook
            if not ob or not ob.get("bid") or not ob.get("ask"):
                continue
            x_bid = float(ob["bid"][0]["p"])
            x_ask = float(ob["ask"][0]["p"])
            x_mid = (x_bid + x_ask) / 2
            x_spread_bps = (x_ask - x_bid) / x_mid * 10000

            data[sym].append({
                "ts": time.time(),
                "l_bid": l_bid, "l_ask": l_ask, "l_mid": l_mid,
                "l_spread_bps": l_spread_bps,
                "x_bid": x_bid, "x_ask": x_ask, "x_mid": x_mid,
                "x_spread_bps": x_spread_bps,
            })

        if (i + 1) % 50 == 0:
            print(f"  采样 {i+1}/{samples}")
        await asyncio.sleep(1)

    # Measure 300ms delay slippage
    print(f"\n测量 Lighter 300ms 延迟滑移 ({samples} 个样本)...\n")
    delay_data: Dict[str, List[float]] = {sym: [] for sym in symbols}

    for i in range(min(samples, 200)):
        for sym in symbols:
            ls = lighter_feeds[sym].snapshot
            if not ls.mid:
                continue
            price_before = ls.mid

            await asyncio.sleep(0.3)

            ls2 = lighter_feeds[sym].snapshot
            if not ls2.mid:
                continue
            price_after = ls2.mid
            slip_bps = abs(price_after - price_before) / price_before * 10000
            delay_data[sym].append(slip_bps)

        if (i + 1) % 50 == 0:
            print(f"  延迟测量 {i+1}/{min(samples, 200)}")

    # Analysis
    print("\n" + "=" * 80)
    print("  分析结果")
    print("=" * 80)

    for sym in symbols:
        d = data[sym]
        dd = delay_data[sym]
        if len(d) < 10 or len(dd) < 10:
            print(f"\n{sym}: 数据不足 (样本={len(d)}, 延迟={len(dd)})")
            continue

        l_spreads = [x["l_spread_bps"] for x in d]
        x_spreads = [x["x_spread_bps"] for x in d]
        mid_spreads = [(x["x_mid"] - x["l_mid"]) / x["l_mid"] * 10000 for x in d]

        l_half = [s / 2 for s in l_spreads]
        x_half = [s / 2 for s in x_spreads]

        l_spread_avg = statistics.mean(l_spreads)
        x_spread_avg = statistics.mean(x_spreads)
        delay_avg = statistics.mean(dd)
        delay_p50 = statistics.median(dd)
        delay_p90 = sorted(dd)[int(len(dd) * 0.9)]
        delay_p95 = sorted(dd)[int(len(dd) * 0.95)]

        # Single-leg costs
        lighter_cross_cost = l_spread_avg / 2
        extended_cross_cost = x_spread_avg / 2
        extended_fee_cost = EXTENDED_TAKER_FEE_BPS
        lighter_delay_cost = delay_avg

        single_leg_cost = (lighter_cross_cost + extended_cross_cost
                           + extended_fee_cost + lighter_delay_cost)

        round_trip_cost = single_leg_cost * 2

        # With P90 delay (conservative)
        single_leg_p90 = (lighter_cross_cost + extended_cross_cost
                          + extended_fee_cost + delay_p90)
        round_trip_p90 = single_leg_p90 * 2

        price = d[0]["l_mid"]
        trade_qty = trade_size_usd / price
        cost_usd = round_trip_cost / 10000 * trade_size_usd
        cost_usd_p90 = round_trip_p90 / 10000 * trade_size_usd

        print(f"\n{'─' * 60}")
        print(f"  {sym}  (价格 ~${price:.2f}  模拟量 {trade_qty:.4f})")
        print(f"{'─' * 60}")
        print(f"  Lighter bid-ask spread:  {l_spread_avg:.2f} bps (半幅={lighter_cross_cost:.2f})")
        print(f"  Extended bid-ask spread: {x_spread_avg:.2f} bps (半幅={extended_cross_cost:.2f})")
        print(f"  Extended taker 手续费:   {extended_fee_cost:.2f} bps (固定)")
        print(f"  Lighter 300ms 延迟滑移:")
        print(f"    均值={delay_avg:.2f}bps  中位={delay_p50:.2f}bps  "
              f"P90={delay_p90:.2f}bps  P95={delay_p95:.2f}bps")
        print()
        print(f"  ┌─────────────────────────────────────────────┐")
        print(f"  │  单腿成本 (均值):                            │")
        print(f"  │    L穿越 {lighter_cross_cost:.2f} + X穿越 {extended_cross_cost:.2f}"
              f" + X费 {extended_fee_cost:.2f} + L延迟 {delay_avg:.2f}"
              f" = {single_leg_cost:.2f} bps │")
        print(f"  │                                             │")
        print(f"  │  往返总成本 (开仓+平仓):                      │")
        print(f"  │    均值: {round_trip_cost:.2f} bps  (≈ ${cost_usd:.4f}/{trade_size_usd}$)   │")
        print(f"  │    保守 (P90延迟): {round_trip_p90:.2f} bps  (≈ ${cost_usd_p90:.4f}/{trade_size_usd}$) │")
        print(f"  │                                             │")
        print(f"  │  ★ 价差需收敛 ≥ {round_trip_cost:.1f} bps 才能无损 (均值)  │")
        print(f"  │  ★ 价差需收敛 ≥ {round_trip_p90:.1f} bps 才能无损 (保守)  │")
        print(f"  └─────────────────────────────────────────────┘")

        # Spread distribution
        abs_mids = [abs(s) for s in mid_spreads]
        print(f"\n  两所 Mid 价差分布:")
        print(f"    均值={statistics.mean(mid_spreads):+.2f}  |abs|均值={statistics.mean(abs_mids):.2f}"
              f"  标准差={statistics.stdev(mid_spreads):.2f}")
        print(f"    最小={min(mid_spreads):+.2f}  最大={max(mid_spreads):+.2f}")

        s = sorted(abs_mids)
        print(f"    P50={s[len(s)//2]:.2f}  P75={s[int(len(s)*0.75)]:.2f}"
              f"  P90={s[int(len(s)*0.9)]:.2f}  P95={s[int(len(s)*0.95)]:.2f}")

        # Feasibility
        pct_above = sum(1 for a in abs_mids if a >= round_trip_cost) / len(abs_mids) * 100
        pct_above_p90 = sum(1 for a in abs_mids if a >= round_trip_p90) / len(abs_mids) * 100
        print(f"\n  价差 ≥ {round_trip_cost:.1f}bps (均值阈值) 的占比: {pct_above:.1f}%")
        print(f"  价差 ≥ {round_trip_p90:.1f}bps (保守阈值) 的占比: {pct_above_p90:.1f}%")

        if pct_above > 5:
            print(f"  → {sym} 有一定套利机会 ({pct_above:.0f}% 时间)")
        elif pct_above > 0:
            print(f"  → {sym} 极少有机会 ({pct_above:.1f}% 时间)")
        else:
            print(f"  → {sym} 当前无套利机会")

    print(f"\n{'=' * 80}")
    print("  模拟完成")
    print(f"{'=' * 80}")

    # Cleanup
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
    p = argparse.ArgumentParser(description="开平仓成本模拟器")
    p.add_argument("--symbols", nargs="+", default=["ETH", "BTC", "SOL"])
    p.add_argument("--samples", type=int, default=200, help="采样数量")
    p.add_argument("--trade-size-usd", type=float, default=1000, help="模拟交易金额($)")
    args = p.parse_args()

    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass

    asyncio.run(run_simulator(args.symbols, args.samples, args.trade_size_usd))


if __name__ == "__main__":
    main()
