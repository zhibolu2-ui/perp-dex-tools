#!/usr/bin/env python3
"""
Extended + Lighter 完整套利回测 (30天)

模拟 spread_arb_v33.py 的真实逻辑:
  - 开仓: basis_bps >= open_spread 时触发
  - 平仓: basis_bps <= close_spread 时触发
  - close_spread 负数 = 多赚(等反转), 正数 = 提前平(放弃利润)

统计维度:
  1. 开仓阈值触发次数 (每月/周/天/小时)
  2. 平仓收敛触发次数 (每月/周/天/小时)
  3. 完整套利周期: 开仓→平仓的时间、捕获价差、净收益
  4. 不同参数组合的效果对比
"""
import asyncio
import aiohttp
import time
import statistics
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

LIGHTER_REST = "https://mainnet.zklighter.elliot.ai"
EXTENDED_REST = "https://api.starknet.extended.exchange"

LIGHTER_MARKETS = {"ETH": 0, "BTC": 1, "SOL": 2}
EXTENDED_MARKETS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD"}

EXTENDED_TAKER_FEE = 2.25  # bps per leg (before rebate)
REBATE = 0.10
EFFECTIVE_FEE = EXTENDED_TAKER_FEE * (1 - REBATE)  # 2.025 bps
ROUND_TRIP_FEE = EFFECTIVE_FEE * 2  # 4.05 bps


@dataclass
class Candle:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float


@dataclass
class ArbCycle:
    open_ts: int
    open_basis: float  # detected basis at open
    close_ts: int = 0
    close_basis: float = 0.0
    outcome: str = ""  # "closed" / "timeout" / "still_open"
    duration_min: float = 0.0
    gross_pnl_bps: float = 0.0
    net_pnl_bps: float = 0.0


async def fetch_lighter_candles(session, mid, start, end):
    out = []
    cur = start
    while cur < end:
        ce = min(cur + 500 * 60, end)
        url = (f"{LIGHTER_REST}/api/v1/candles?market_id={mid}&resolution=1m"
               f"&start_timestamp={cur}&end_timestamp={ce}&count_back=500")
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200:
                    break
                d = await r.json()
                raw = d.get("c", [])
                if not raw:
                    break
                for c in raw:
                    ts = int(c["t"])
                    if ts > 1e12:
                        ts //= 1000
                    o = float(c["o"])
                    if o > 0:
                        out.append(Candle(ts=ts, o=o, h=float(c["h"]),
                                          l=float(c["l"]), c=float(c["c"]),
                                          v=float(c["v"])))
        except Exception as e:
            print(f"    Lighter err: {e}")
            break
        cur = ce
        await asyncio.sleep(0.25)
    return out


async def fetch_extended_candles(session, mkt, start, end):
    out = []
    cur_end = end * 1000
    start_ms = start * 1000
    hdr = {"User-Agent": "full-bt/1.0"}
    while cur_end > start_ms:
        url = (f"{EXTENDED_REST}/api/v1/info/candles/{mkt}/trades"
               f"?interval=PT1M&limit=1000&endTime={cur_end}")
        try:
            async with session.get(url, headers=hdr,
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200:
                    break
                body = await r.json()
                raw = body.get("data", [])
                if not raw:
                    break
                for c in raw:
                    ts = int(c.get("T", 0))
                    if ts > 1e12:
                        ts //= 1000
                    o = float(c.get("o", 0))
                    if o > 0:
                        out.append(Candle(ts=ts, o=o, h=float(c["h"]),
                                          l=float(c["l"]), c=float(c["c"]),
                                          v=float(c["v"])))
                earliest = min(c.ts for c in out[-len(raw):])
                cur_end = earliest * 1000 - 1
        except Exception as e:
            print(f"    Extended err: {e}")
            break
        await asyncio.sleep(0.2)
    return out


def align_and_compute_basis(lighter_candles, extended_candles):
    """
    Align by 1-min bucket, compute basis_bps = (lighter_mid - extended_mid) / extended_mid * 10000
    Returns: [(ts, basis_bps, lighter_mid, extended_mid)]
    """
    ext_map = {}
    for c in extended_candles:
        bucket = c.ts // 60 * 60
        ext_map[bucket] = c

    result = []
    for lc in lighter_candles:
        bucket = lc.ts // 60 * 60
        ec = ext_map.get(bucket)
        if not ec:
            ec = ext_map.get(bucket - 60) or ext_map.get(bucket + 60)
        if not ec:
            continue
        l_mid = (lc.o + lc.c) / 2
        e_mid = (ec.o + ec.c) / 2
        if e_mid == 0:
            continue
        basis = (l_mid - e_mid) / e_mid * 10000
        result.append((lc.ts, basis, l_mid, e_mid))

    result.sort(key=lambda x: x[0])
    return result


def simulate_arb(basis_series, open_thresh, close_thresh):
    """
    Simulate spread_arb_v33 logic:
    - Open when |basis| >= open_thresh
    - Close when basis crosses close_thresh (considering direction)

    close_thresh in V33:
      - negative = wait for reversal (多赚)
      - positive = close early (放弃利润)
      - 0 = close when basis returns to 0

    Returns list of ArbCycle
    """
    cycles = []
    i = 0
    n = len(basis_series)

    while i < n:
        ts, basis, _, _ = basis_series[i]

        # Look for open signal: |basis| >= open_thresh
        if abs(basis) >= open_thresh:
            direction = 1 if basis > 0 else -1  # 1=lighter>extended, -1=lighter<extended
            open_ts = ts
            open_basis = basis

            # Search for close signal
            j = i + 1
            closed = False
            while j < n:
                ts_j, basis_j, _, _ = basis_series[j]

                # V33 logic: 
                # if lighter_position < 0 (we sold lighter): close when basis <= close_thresh
                # if lighter_position > 0 (we bought lighter): close when basis >= -close_thresh
                if direction > 0:
                    # We sold lighter (short), hedged long on extended
                    # basis was positive (lighter > extended)
                    # Close when basis drops to close_thresh
                    should_close = basis_j <= close_thresh
                else:
                    # We bought lighter (long), hedged short on extended
                    # basis was negative (lighter < extended)
                    # Close when basis rises to -close_thresh
                    should_close = basis_j >= -close_thresh

                if should_close:
                    duration = (ts_j - open_ts) / 60.0
                    gross = abs(open_basis) - abs(basis_j)
                    net = gross - ROUND_TRIP_FEE
                    cycles.append(ArbCycle(
                        open_ts=open_ts,
                        open_basis=open_basis,
                        close_ts=ts_j,
                        close_basis=basis_j,
                        outcome="closed",
                        duration_min=duration,
                        gross_pnl_bps=gross,
                        net_pnl_bps=net,
                    ))
                    i = j + 1
                    closed = True
                    break

                # Timeout: 24h max hold
                if (ts_j - open_ts) > 86400:
                    duration = (ts_j - open_ts) / 60.0
                    gross = abs(open_basis) - abs(basis_j)
                    net = gross - ROUND_TRIP_FEE
                    cycles.append(ArbCycle(
                        open_ts=open_ts,
                        open_basis=open_basis,
                        close_ts=ts_j,
                        close_basis=basis_j,
                        outcome="timeout_24h",
                        duration_min=duration,
                        gross_pnl_bps=gross,
                        net_pnl_bps=net,
                    ))
                    i = j + 1
                    closed = True
                    break

                j += 1

            if not closed:
                # Still open at end of data
                last_ts, last_basis, _, _ = basis_series[-1]
                duration = (last_ts - open_ts) / 60.0
                gross = abs(open_basis) - abs(last_basis)
                net = gross - ROUND_TRIP_FEE
                cycles.append(ArbCycle(
                    open_ts=open_ts,
                    open_basis=open_basis,
                    close_ts=last_ts,
                    close_basis=last_basis,
                    outcome="still_open",
                    duration_min=duration,
                    gross_pnl_bps=gross,
                    net_pnl_bps=net,
                ))
                break
        else:
            i += 1

    return cycles


def ts_to_dt(ts):
    return datetime.fromtimestamp(ts, tz=timezone(timedelta(hours=8)))


def print_time_distribution(label, timestamps, total_days):
    """Print per-month/week/day/hour distribution"""
    if not timestamps:
        print(f"    {label}: 0次")
        return

    dts = [ts_to_dt(t) for t in timestamps]

    # Per month
    monthly = defaultdict(int)
    for dt in dts:
        monthly[dt.strftime("%Y-%m")] += 1

    # Per week (ISO week)
    weekly = defaultdict(int)
    for dt in dts:
        weekly[dt.strftime("%Y-W%W")] += 1

    # Per day
    daily = defaultdict(int)
    for dt in dts:
        daily[dt.strftime("%m-%d")] += 1

    # Per hour of day
    hourly = defaultdict(int)
    for dt in dts:
        hourly[dt.hour] += 1

    total = len(timestamps)
    avg_daily = total / max(total_days, 1)
    avg_hourly = total / max(total_days * 24, 1)

    print(f"    {label}: 总{total}次, 日均{avg_daily:.1f}次, 时均{avg_hourly:.2f}次")

    # Monthly breakdown
    if monthly:
        parts = [f"{k}:{v}次" for k, v in sorted(monthly.items())]
        print(f"      按月: {', '.join(parts)}")

    # Weekly breakdown
    if weekly:
        vals = list(weekly.values())
        print(f"      按周: 平均{statistics.mean(vals):.1f}次/周, "
              f"最少{min(vals)}, 最多{max(vals)}")

    # Daily breakdown
    if daily:
        vals = list(daily.values())
        print(f"      按天: 平均{statistics.mean(vals):.1f}次/天, "
              f"最少{min(vals)}, 最多{max(vals)}, 中位{statistics.median(vals):.0f}")

    # Hourly breakdown (which hours are best)
    if hourly:
        sorted_hours = sorted(hourly.items(), key=lambda x: -x[1])
        top5 = sorted_hours[:5]
        top_str = ", ".join([f"{h}时:{c}次" for h, c in top5])
        print(f"      最活跃时段(UTC+8): {top_str}")


def print_full_report(symbol, basis_series, total_days):
    print(f"\n{'━' * 110}")
    print(f"  {symbol} — Extended + Lighter 完整套利回测 (30天)")
    print(f"  手续费: Extended taker {EFFECTIVE_FEE:.3f}bps × 2 = {ROUND_TRIP_FEE:.2f}bps 往返")
    print(f"{'━' * 110}")

    # ── 1. Basic basis stats ──
    all_basis = [abs(b[1]) for b in basis_series]
    if not all_basis:
        print("  数据不足")
        return

    avg_b = statistics.mean(all_basis)
    med_b = statistics.median(all_basis)
    p75 = sorted(all_basis)[int(len(all_basis) * 0.75)]
    p90 = sorted(all_basis)[int(len(all_basis) * 0.90)]

    print(f"\n  ┌─ 基础价差(|basis|)统计 ({len(basis_series)}个1分钟样本, {total_days:.0f}天) ──────────┐")
    print(f"  │ 平均={avg_b:.2f}bps  中位={med_b:.2f}bps  P75={p75:.2f}bps  P90={p90:.2f}bps          │")
    print(f"  │                                                                                            │")
    for thresh in [4.0, 4.3, 4.5, 5.0, 5.5, 6.0, 7.0, 8.0]:
        pct = sum(1 for b in all_basis if b >= thresh) / len(all_basis) * 100
        cnt = sum(1 for b in all_basis if b >= thresh)
        print(f"  │   |basis|>={thresh}bps: {pct:5.1f}% ({cnt}次/30天, 日均{cnt/total_days:.1f}次)                       │")
    print(f"  │                                                                                            │")
    for thresh in [0.0, 0.5, 1.0, 1.5, 2.0]:
        pct = sum(1 for b in all_basis if b <= thresh) / len(all_basis) * 100
        cnt = sum(1 for b in all_basis if b <= thresh)
        print(f"  │   |basis|<={thresh}bps: {pct:5.1f}% ({cnt}次/30天, 日均{cnt/total_days:.1f}次)                       │")
    print(f"  └────────────────────────────────────────────────────────────────────────────────────────────┘")

    # ── 2. Open signal time distribution ──
    print(f"\n  ┌─ 开仓信号时间分布 ────────────────────────────────────────────────────────────────────┐")
    for thresh in [4.3, 5.0, 5.5, 6.0]:
        open_ts_list = [b[0] for b in basis_series if abs(b[1]) >= thresh]
        print_time_distribution(f"|basis|>={thresh}bps", open_ts_list, total_days)
    print(f"  └────────────────────────────────────────────────────────────────────────────────────────────┘")

    # ── 3. Close signal time distribution ──
    print(f"\n  ┌─ 平仓信号(价差收敛)时间分布 ──────────────────────────────────────────────────────────┐")
    for thresh in [0.0, 0.5, 1.0, 1.5, 2.0]:
        close_ts_list = [b[0] for b in basis_series if abs(b[1]) <= thresh]
        print_time_distribution(f"|basis|<={thresh}bps", close_ts_list, total_days)
    print(f"  └────────────────────────────────────────────────────────────────────────────────────────────┘")

    # ── 4. Full arb cycle simulation ──
    print(f"\n  ┌─ 完整套利周期模拟 ────────────────────────────────────────────────────────────────────┐")
    print(f"  │  模拟V33逻辑: 开仓→持仓→平仓, 计算完整周期的收益                                       │")
    print(f"  │  close_spread负数=多赚(等反转), 正数=提前平(放弃利润), 0=归零平仓                        │")
    print(f"  │                                                                                            │")

    param_combos = [
        (4.3, 0.0, "当前参数"),
        (4.3, 0.5, "提前平仓0.5bps"),
        (4.3, -0.5, "多赚0.5bps"),
        (5.0, 0.0, "open=5.0, close=0"),
        (5.0, 0.5, "open=5.0, 提前0.5"),
        (5.0, -0.5, "open=5.0, 多赚0.5"),
        (5.5, 0.0, "open=5.5, close=0"),
        (5.5, -0.5, "open=5.5, 多赚0.5"),
        (6.0, 0.0, "open=6.0, close=0"),
        (6.0, -0.5, "open=6.0, 多赚0.5"),
    ]

    best_total_net = -999
    best_combo = None

    for open_t, close_t, label in param_combos:
        cycles = simulate_arb(basis_series, open_t, close_t)
        if not cycles:
            print(f"  │  open={open_t}, close={close_t} ({label}): 0次完整周期                               │")
            continue

        closed = [c for c in cycles if c.outcome == "closed"]
        timeout = [c for c in cycles if c.outcome == "timeout_24h"]
        still_open = [c for c in cycles if c.outcome == "still_open"]

        total = len(cycles)
        n_closed = len(closed)
        n_timeout = len(timeout)

        if closed:
            avg_dur = statistics.mean([c.duration_min for c in closed])
            med_dur = statistics.median([c.duration_min for c in closed])
            avg_gross = statistics.mean([c.gross_pnl_bps for c in closed])
            avg_net = statistics.mean([c.net_pnl_bps for c in closed])
            total_net = sum(c.net_pnl_bps for c in closed)
            profitable = sum(1 for c in closed if c.net_pnl_bps > 0)
            win_rate = profitable / n_closed * 100

            if total_net > best_total_net:
                best_total_net = total_net
                best_combo = (open_t, close_t, label)

            print(f"  │                                                                                            │")
            print(f"  │  ★ open={open_t}, close={close_t} ({label}):                                               │")
            print(f"  │    完整周期: {n_closed}次平仓 + {n_timeout}次超时 + {len(still_open)}次未平                  │")
            print(f"  │    平仓成功率: {n_closed}/{total} = {n_closed/total*100:.1f}%                               │")
            print(f"  │    持仓时间: 平均{avg_dur:.0f}分钟, 中位{med_dur:.0f}分钟                                   │")
            print(f"  │    毛利: {avg_gross:.2f}bps/次  净利: {avg_net:.2f}bps/次                                    │")
            print(f"  │    胜率(净利>0): {win_rate:.1f}%                                                             │")
            print(f"  │    30天总净利: {total_net:.1f}bps ({n_closed}次 × {avg_net:.2f}bps)                          │")
            print(f"  │    日均: {total_net/total_days:.1f}bps/天, {n_closed/total_days:.1f}次/天                    │")

            # Duration distribution
            dur_buckets = [
                (0, 5, "<5分钟"),
                (5, 30, "5-30分钟"),
                (30, 60, "30-60分钟"),
                (60, 240, "1-4小时"),
                (240, 720, "4-12小时"),
                (720, 99999, ">12小时"),
            ]
            dur_parts = []
            for lo, hi, lbl in dur_buckets:
                cnt = sum(1 for c in closed if lo <= c.duration_min < hi)
                if cnt > 0:
                    dur_parts.append(f"{lbl}:{cnt}({cnt/n_closed*100:.0f}%)")
            if dur_parts:
                print(f"  │    持仓分布: {', '.join(dur_parts[:4])}  │")
                if len(dur_parts) > 4:
                    print(f"  │               {', '.join(dur_parts[4:])}                                             │")

        else:
            print(f"  │  open={open_t}, close={close_t} ({label}): {n_timeout}次超时, 0次成功平仓                   │")

    print(f"  │                                                                                            │")
    if best_combo:
        print(f"  │  ★★ 30天总净利最高的参数组合:                                                           │")
        print(f"  │     open={best_combo[0]}, close={best_combo[1]} ({best_combo[2]})                       │")
        print(f"  │     30天总净利: {best_total_net:.1f}bps                                                  │")
    print(f"  └────────────────────────────────────────────────────────────────────────────────────────────┘")

    # ── 5. Detailed cycle analysis for best combo ──
    if best_combo:
        open_t, close_t, label = best_combo
        cycles = simulate_arb(basis_series, open_t, close_t)
        closed = [c for c in cycles if c.outcome == "closed"]

        if closed:
            print(f"\n  ┌─ 最优参数详细分析: open={open_t}, close={close_t} ──────────────────────────────────┐")

            # Time distribution of completed cycles
            close_timestamps = [c.close_ts for c in closed]
            open_timestamps = [c.open_ts for c in closed]

            print(f"  │  开仓时间分布:                                                                         │")
            print_time_distribution("开仓", open_timestamps, total_days)

            print(f"  │  平仓时间分布:                                                                         │")
            print_time_distribution("平仓", close_timestamps, total_days)

            # PnL distribution
            nets = [c.net_pnl_bps for c in closed]
            sorted_nets = sorted(nets)
            p10 = sorted_nets[int(len(sorted_nets) * 0.10)]
            p25 = sorted_nets[int(len(sorted_nets) * 0.25)]
            p50 = sorted_nets[int(len(sorted_nets) * 0.50)]
            p75 = sorted_nets[int(len(sorted_nets) * 0.75)]
            p90 = sorted_nets[int(len(sorted_nets) * 0.90)]

            print(f"  │                                                                                            │")
            print(f"  │  净利分布:                                                                                 │")
            print(f"  │    P10={p10:.2f}  P25={p25:.2f}  P50={p50:.2f}  P75={p75:.2f}  P90={p90:.2f}bps           │")
            print(f"  │    亏损次数: {sum(1 for n in nets if n < 0)} ({sum(1 for n in nets if n < 0)/len(nets)*100:.1f}%)  │")
            print(f"  │    盈利次数: {sum(1 for n in nets if n > 0)} ({sum(1 for n in nets if n > 0)/len(nets)*100:.1f}%)  │")

            # Daily PnL
            daily_pnl = defaultdict(float)
            daily_cnt = defaultdict(int)
            for c in closed:
                day = ts_to_dt(c.close_ts).strftime("%m-%d")
                daily_pnl[day] += c.net_pnl_bps
                daily_cnt[day] += 1

            if daily_pnl:
                vals = list(daily_pnl.values())
                print(f"  │                                                                                            │")
                print(f"  │  每日净利:                                                                                 │")
                print(f"  │    平均={statistics.mean(vals):.1f}bps/天  最好={max(vals):.1f}bps  最差={min(vals):.1f}bps │")
                profitable_days = sum(1 for v in vals if v > 0)
                print(f"  │    盈利天数: {profitable_days}/{len(vals)} ({profitable_days/len(vals)*100:.0f}%)           │")

                # Show top 5 best and worst days
                sorted_days = sorted(daily_pnl.items(), key=lambda x: -x[1])
                best5 = sorted_days[:3]
                worst3 = sorted_days[-3:]
                print(f"  │    最好3天: {', '.join([f'{d}:{v:.1f}bps({daily_cnt[d]}次)' for d,v in best5])}  │")
                print(f"  │    最差3天: {', '.join([f'{d}:{v:.1f}bps({daily_cnt[d]}次)' for d,v in worst3])}  │")

            print(f"  └────────────────────────────────────────────────────────────────────────────────────────────┘")


async def run_symbol(session, symbol):
    now = int(time.time())
    start = now - 30 * 86400
    total_days = 30.0

    mid = LIGHTER_MARKETS[symbol]
    mkt = EXTENDED_MARKETS[symbol]

    print(f"\n  [{symbol}] 获取Lighter 1m K线...")
    l_candles = await fetch_lighter_candles(session, mid, start, now)
    print(f"  [{symbol}] Lighter: {len(l_candles)} candles")

    print(f"  [{symbol}] 获取Extended 1m K线...")
    e_candles = await fetch_extended_candles(session, mkt, start, now)
    print(f"  [{symbol}] Extended: {len(e_candles)} candles")

    basis_series = align_and_compute_basis(l_candles, e_candles)
    print(f"  [{symbol}] 对齐: {len(basis_series)} 个basis样本")

    if len(basis_series) < 100:
        print(f"  [{symbol}] 数据不足")
        return

    actual_days = (basis_series[-1][0] - basis_series[0][0]) / 86400
    print_full_report(symbol, basis_series, actual_days)


async def main():
    print("=" * 110)
    print("  Extended + Lighter 完整套利回测 (30天)")
    print("  模拟 spread_arb_v33.py 真实逻辑")
    print("  费用: Extended taker 2.025bps(含10%返佣) × 2 = 4.05bps 往返")
    print("=" * 110)

    async with aiohttp.ClientSession() as session:
        for sym in ["BTC", "ETH", "SOL"]:
            await run_symbol(session, sym)

    print(f"\n{'=' * 110}")
    print("  回测完成")
    print(f"{'=' * 110}")


if __name__ == "__main__":
    asyncio.run(main())
