#!/usr/bin/env python3
"""
Extended vs Lighter 30天精细化回测
输出: 每日/每周/每月/每小时 的开仓机会占比、收敛统计、反转统计
使用5分钟K线，30天数据
"""
import asyncio
import aiohttp
import time
import statistics
import json
import math
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

LIGHTER_REST = "https://mainnet.zklighter.elliot.ai"
EXTENDED_REST = "https://api.starknet.extended.exchange"

LIGHTER_MARKETS = {"ETH": 0, "BTC": 1, "SOL": 2}
EXTENDED_MARKETS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD"}

EXT_FEE_BPS = 2.025
ROUND_TRIP_FEE = EXT_FEE_BPS * 2  # 4.05

DAYS = 30
RESOLUTION = "5m"


@dataclass
class Candle:
    ts: int  # unix seconds
    o: float
    h: float
    l: float
    c: float
    v: float = 0


async def fetch_lighter(session, mid, start, end):
    out = []
    cur = start
    while cur < end:
        ce = min(cur + 500 * 300, end)
        url = (f"{LIGHTER_REST}/api/v1/candles?market_id={mid}&resolution={RESOLUTION}"
               f"&start_timestamp={cur}&end_timestamp={ce}&count_back=500")
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    d = await r.json()
                    raw = d.get("c", [])
                    if not raw:
                        break
                    for c in raw:
                        ts = int(c["t"])
                        if ts > 1e12:
                            ts //= 1000
                        if float(c["o"]) > 0:
                            out.append(Candle(ts, float(c["o"]), float(c["h"]),
                                              float(c["l"]), float(c["c"]), float(c["v"])))
                else:
                    break
        except Exception as e:
            print(f"  [L] err: {e}")
            break
        cur = ce
        await asyncio.sleep(0.25)
    return out


async def fetch_extended(session, mkt, start, end):
    out = []
    cur_end = end * 1000
    start_ms = start * 1000
    hdr = {"User-Agent": "bt/1.0"}
    while cur_end > start_ms:
        url = (f"{EXTENDED_REST}/api/v1/info/candles/{mkt}/trades"
               f"?interval=PT5M&limit=1000&endTime={cur_end}")
        try:
            async with session.get(url, headers=hdr, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    body = await r.json()
                    raw = body.get("data", [])
                    if not raw:
                        break
                    batch = []
                    for c in raw:
                        ts = int(c["T"])
                        if ts > 1e12:
                            ts //= 1000
                        if float(c["o"]) > 0:
                            batch.append(Candle(ts, float(c["o"]), float(c["h"]),
                                                float(c["l"]), float(c["c"]), float(c["v"])))
                    if not batch:
                        break
                    out.extend(batch)
                    cur_end = min(c.ts for c in batch) * 1000 - 1
                else:
                    break
        except Exception as e:
            print(f"  [X] err: {e}")
            break
        await asyncio.sleep(0.2)
    return out


def align(lc, xc, tol=300):
    lmap = {}
    for c in lc:
        b = (c.ts // tol) * tol
        if b not in lmap or c.v > lmap[b].v:
            lmap[b] = c
    pairs = []
    for c in xc:
        b = (c.ts // tol) * tol
        if b in lmap:
            pairs.append((lmap[b], c))
    pairs.sort(key=lambda x: x[0].ts)
    return pairs


def compute_spreads(pairs):
    """Return list of (ts, spread_bps) for each aligned pair"""
    out = []
    for lc, xc in pairs:
        lm = (lc.h + lc.l) / 2
        xm = (xc.h + xc.l) / 2
        if lm == 0 or xm == 0:
            continue
        avg = (lm + xm) / 2
        sp = abs(xm - lm) / avg * 10000
        out.append((lc.ts, sp))
    return out


def analyze_convergence(spreads):
    """
    Track spread episodes:
    - When spread > 4bps, mark as "open opportunity"
    - Track what happens next: converge to <=1bps, converge to 0, or reverse (widen further)
    Returns list of episodes: (open_ts, open_spread, outcome, time_to_outcome_s, final_spread)
    outcome: 'converge_1bps', 'converge_0', 'reverse', 'timeout'
    """
    episodes = []
    i = 0
    n = len(spreads)
    while i < n:
        ts, sp = spreads[i]
        if sp > ROUND_TRIP_FEE:
            open_ts = ts
            open_sp = sp
            peak_sp = sp
            j = i + 1
            outcome = "timeout"
            outcome_ts = 0
            final_sp = sp
            while j < n and (spreads[j][0] - open_ts) < 7200:  # 2h window
                t2, s2 = spreads[j]
                if s2 > peak_sp:
                    peak_sp = s2
                if s2 <= 0.5:
                    outcome = "converge_0"
                    outcome_ts = t2
                    final_sp = s2
                    break
                elif s2 <= 1.0:
                    outcome = "converge_1bps"
                    outcome_ts = t2
                    final_sp = s2
                    break
                elif s2 > open_sp * 1.5 and s2 > peak_sp * 0.95:
                    outcome = "reverse"
                    outcome_ts = t2
                    final_sp = s2
                    break
                j += 1

            if outcome == "timeout" and j < n:
                final_sp = spreads[min(j, n - 1)][1]

            time_to = (outcome_ts - open_ts) if outcome_ts > 0 else (spreads[min(j, n - 1)][0] - open_ts)
            episodes.append((open_ts, open_sp, outcome, time_to, final_sp))
            i = j + 1
        else:
            i += 1
    return episodes


def get_week_key(ts):
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    iso = dt.isocalendar()
    return f"W{iso[1]:02d}"


def get_day_key(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%m-%d")


def get_hour(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).hour


def run_analysis(sym, spreads, episodes):
    n = len(spreads)
    if n == 0:
        print(f"\n  {sym}: 无数据")
        return

    # ── Group by time periods ──
    daily = defaultdict(list)
    weekly = defaultdict(list)
    hourly = defaultdict(list)
    for ts, sp in spreads:
        daily[get_day_key(ts)].append(sp)
        weekly[get_week_key(ts)].append(sp)
        hourly[get_hour(ts)].append(sp)

    all_sp = [s for _, s in spreads]
    avg = statistics.mean(all_sp)
    above4 = sum(1 for s in all_sp if s > ROUND_TRIP_FEE)
    above6 = sum(1 for s in all_sp if s > 6)
    above8 = sum(1 for s in all_sp if s > 8)

    print(f"\n{'━' * 110}")
    print(f"  {sym}  —  30天精细回测")
    print(f"{'━' * 110}")
    print(f"  总样本: {n}根5分钟K线 ({n / 288:.1f}天)")
    print(f"  平均价差: {avg:.2f}bps  盈亏平衡线: {ROUND_TRIP_FEE:.2f}bps")
    print(f"  月度 >4bps: {above4}/{n} = {above4/n*100:.1f}%")
    print(f"  月度 >6bps: {above6}/{n} = {above6/n*100:.1f}%")
    print(f"  月度 >8bps: {above8}/{n} = {above8/n*100:.1f}%")

    # ═══ 1. 每日 >4bps 占比 ═══
    print(f"\n  ┌{'─' * 106}┐")
    print(f"  │{'每日 >4bps 开仓机会占比':^50}│")
    print(f"  ├{'─' * 106}┤")
    print(f"  │ {'日期':<8} {'样本':>6} {'>4bps':>8} {'占比':>8} {'>6bps':>8} {'占比':>8} {'平均差':>8} {'P90':>8} {'P95':>8} │")
    print(f"  ├{'─' * 106}┤")
    for day in sorted(daily.keys()):
        sp_list = daily[day]
        nd = len(sp_list)
        a4 = sum(1 for s in sp_list if s > ROUND_TRIP_FEE)
        a6 = sum(1 for s in sp_list if s > 6)
        davg = statistics.mean(sp_list)
        ss = sorted(sp_list)
        p90 = ss[int(nd * 0.9)] if nd > 10 else max(sp_list)
        p95 = ss[int(nd * 0.95)] if nd > 20 else max(sp_list)
        print(f"  │ {day:<8} {nd:>6} {a4:>8} {a4/nd*100:>7.1f}% {a6:>8} {a6/nd*100:>7.1f}% {davg:>7.2f} {p90:>7.2f} {p95:>7.2f} │")
    print(f"  └{'─' * 106}┘")

    # ═══ 2. 每周 >4bps 占比 ═══
    print(f"\n  ┌{'─' * 90}┐")
    print(f"  │{'每周 >4bps 开仓机会占比':^45}│")
    print(f"  ├{'─' * 90}┤")
    print(f"  │ {'周':<6} {'样本':>6} {'>4bps':>8} {'占比':>8} {'>6bps':>8} {'占比':>8} {'平均差':>8} {'最大':>8} │")
    print(f"  ├{'─' * 90}┤")
    for wk in sorted(weekly.keys()):
        sp_list = weekly[wk]
        nw = len(sp_list)
        a4 = sum(1 for s in sp_list if s > ROUND_TRIP_FEE)
        a6 = sum(1 for s in sp_list if s > 6)
        wavg = statistics.mean(sp_list)
        wmx = max(sp_list)
        print(f"  │ {wk:<6} {nw:>6} {a4:>8} {a4/nw*100:>7.1f}% {a6:>8} {a6/nw*100:>7.1f}% {wavg:>7.2f} {wmx:>7.2f} │")
    print(f"  └{'─' * 90}┘")

    # ═══ 3. 每小时 >4bps 占比 ═══
    print(f"\n  ┌{'─' * 90}┐")
    print(f"  │{'每小时 >4bps 开仓机会占比 (UTC)':^45}│")
    print(f"  ├{'─' * 90}┤")
    print(f"  │ {'时段':<8} {'样本':>6} {'>4bps':>8} {'占比':>8} {'>6bps':>8} {'占比':>8} {'平均差':>8} │")
    print(f"  ├{'─' * 90}┤")
    for h in range(24):
        sp_list = hourly.get(h, [])
        if not sp_list:
            continue
        nh = len(sp_list)
        a4 = sum(1 for s in sp_list if s > ROUND_TRIP_FEE)
        a6 = sum(1 for s in sp_list if s > 6)
        havg = statistics.mean(sp_list)
        print(f"  │ {h:02d}:00   {nh:>6} {a4:>8} {a4/nh*100:>7.1f}% {a6:>8} {a6/nh*100:>7.1f}% {havg:>7.2f} │")
    print(f"  └{'─' * 90}┘")

    # ═══ 4. 收敛统计 ═══
    n_ep = len(episodes)
    if n_ep == 0:
        print(f"\n  收敛统计: 无>4bps的开仓事件")
        return

    conv1 = [e for e in episodes if e[2] == "converge_1bps"]
    conv0 = [e for e in episodes if e[2] == "converge_0"]
    reverse = [e for e in episodes if e[2] == "reverse"]
    timeout = [e for e in episodes if e[2] == "timeout"]

    print(f"\n  ┌{'─' * 100}┐")
    print(f"  │{'价差收敛/反转统计 (开仓>4bps后2小时内)':^50}│")
    print(f"  ├{'─' * 100}┤")
    print(f"  │ 总开仓事件: {n_ep:>6}                                                                     │")
    print(f"  │                                                                                          │")
    print(f"  │ 收敛到 ≤1bps:  {len(conv1):>5} / {n_ep} = {len(conv1)/n_ep*100:>5.1f}%                                              │")
    if conv1:
        t1 = [e[3] for e in conv1]
        print(f"  │   平均收敛时间: {statistics.mean(t1)/60:>6.1f}分钟  中位: {statistics.median(t1)/60:>6.1f}分钟                            │")
    print(f"  │                                                                                          │")
    print(f"  │ 收敛到 ≤0.5bps: {len(conv0):>5} / {n_ep} = {len(conv0)/n_ep*100:>5.1f}%                                              │")
    if conv0:
        t0 = [e[3] for e in conv0]
        print(f"  │   平均收敛时间: {statistics.mean(t0)/60:>6.1f}分钟  中位: {statistics.median(t0)/60:>6.1f}分钟                            │")
    print(f"  │                                                                                          │")
    print(f"  │ 价差反转(扩大): {len(reverse):>5} / {n_ep} = {len(reverse)/n_ep*100:>5.1f}%                                              │")
    if reverse:
        tr = [e[4] for e in reverse]
        print(f"  │   反转后平均价差: {statistics.mean(tr):>6.2f}bps                                                      │")
    print(f"  │                                                                                          │")
    print(f"  │ 超时(2h未收敛): {len(timeout):>5} / {n_ep} = {len(timeout)/n_ep*100:>5.1f}%                                              │")
    if timeout:
        tt = [e[4] for e in timeout]
        print(f"  │   超时后平均价差: {statistics.mean(tt):>6.2f}bps                                                      │")
    print(f"  └{'─' * 100}┘")

    # ═══ 5. 收敛按日/周/小时分布 ═══
    ep_daily = defaultdict(lambda: {"total": 0, "conv1": 0, "conv0": 0, "rev": 0, "to": 0})
    ep_weekly = defaultdict(lambda: {"total": 0, "conv1": 0, "conv0": 0, "rev": 0, "to": 0})
    ep_hourly = defaultdict(lambda: {"total": 0, "conv1": 0, "conv0": 0, "rev": 0, "to": 0})

    for ts, sp, outcome, dt, fsp in episodes:
        dk = get_day_key(ts)
        wk = get_week_key(ts)
        hk = get_hour(ts)
        ep_daily[dk]["total"] += 1
        ep_weekly[wk]["total"] += 1
        ep_hourly[hk]["total"] += 1
        if outcome == "converge_1bps":
            ep_daily[dk]["conv1"] += 1
            ep_weekly[wk]["conv1"] += 1
            ep_hourly[hk]["conv1"] += 1
        elif outcome == "converge_0":
            ep_daily[dk]["conv0"] += 1
            ep_weekly[wk]["conv0"] += 1
            ep_hourly[hk]["conv0"] += 1
        elif outcome == "reverse":
            ep_daily[dk]["rev"] += 1
            ep_weekly[wk]["rev"] += 1
            ep_hourly[hk]["rev"] += 1
        else:
            ep_daily[dk]["to"] += 1
            ep_weekly[wk]["to"] += 1
            ep_hourly[hk]["to"] += 1

    print(f"\n  ┌{'─' * 100}┐")
    print(f"  │{'每日收敛/反转分布':^50}│")
    print(f"  ├{'─' * 100}┤")
    print(f"  │ {'日期':<8} {'事件':>5} {'收敛≤1':>7} {'占比':>7} {'收敛≤0':>7} {'占比':>7} {'反转':>5} {'占比':>7} {'超时':>5} {'占比':>7} │")
    print(f"  ├{'─' * 100}┤")
    for dk in sorted(ep_daily.keys()):
        d = ep_daily[dk]
        t = d["total"]
        if t == 0:
            continue
        c1p = d["conv1"] / t * 100
        c0p = d["conv0"] / t * 100
        rp = d["rev"] / t * 100
        tp = d["to"] / t * 100
        print(f"  │ {dk:<8} {t:>5} {d['conv1']:>7} {c1p:>6.1f}% {d['conv0']:>7} {c0p:>6.1f}% {d['rev']:>5} {rp:>6.1f}% {d['to']:>5} {tp:>6.1f}% │")
    print(f"  └{'─' * 100}┘")

    print(f"\n  ┌{'─' * 100}┐")
    print(f"  │{'每周收敛/反转分布':^50}│")
    print(f"  ├{'─' * 100}┤")
    print(f"  │ {'周':<6} {'事件':>5} {'收敛≤1':>7} {'占比':>7} {'收敛≤0':>7} {'占比':>7} {'反转':>5} {'占比':>7} {'超时':>5} {'占比':>7} │")
    print(f"  ├{'─' * 100}┤")
    for wk in sorted(ep_weekly.keys()):
        d = ep_weekly[wk]
        t = d["total"]
        if t == 0:
            continue
        c1p = d["conv1"] / t * 100
        c0p = d["conv0"] / t * 100
        rp = d["rev"] / t * 100
        tp = d["to"] / t * 100
        print(f"  │ {wk:<6} {t:>5} {d['conv1']:>7} {c1p:>6.1f}% {d['conv0']:>7} {c0p:>6.1f}% {d['rev']:>5} {rp:>6.1f}% {d['to']:>5} {tp:>6.1f}% │")
    print(f"  └{'─' * 100}┘")

    print(f"\n  ┌{'─' * 100}┐")
    print(f"  │{'每小时收敛/反转分布 (UTC)':^50}│")
    print(f"  ├{'─' * 100}┤")
    print(f"  │ {'时段':<8} {'事件':>5} {'收敛≤1':>7} {'占比':>7} {'收敛≤0':>7} {'占比':>7} {'反转':>5} {'占比':>7} {'超时':>5} {'占比':>7} │")
    print(f"  ├{'─' * 100}┤")
    for h in range(24):
        d = ep_hourly.get(h, {"total": 0, "conv1": 0, "conv0": 0, "rev": 0, "to": 0})
        t = d["total"]
        if t == 0:
            continue
        c1p = d["conv1"] / t * 100
        c0p = d["conv0"] / t * 100
        rp = d["rev"] / t * 100
        tp = d["to"] / t * 100
        print(f"  │ {h:02d}:00   {t:>5} {d['conv1']:>7} {c1p:>6.1f}% {d['conv0']:>7} {c0p:>6.1f}% {d['rev']:>5} {rp:>6.1f}% {d['to']:>5} {tp:>6.1f}% │")
    print(f"  └{'─' * 100}┘")


async def main():
    now = int(time.time())
    start = now - DAYS * 86400
    print(f"回测: {datetime.fromtimestamp(start, tz=timezone.utc):%Y-%m-%d} → "
          f"{datetime.fromtimestamp(now, tz=timezone.utc):%Y-%m-%d} ({DAYS}天)")
    print(f"K线: {RESOLUTION}  盈亏平衡: {ROUND_TRIP_FEE:.2f}bps\n")

    async with aiohttp.ClientSession() as session:
        for sym in ["BTC", "ETH", "SOL"]:
            print(f"{'─' * 50}")
            print(f"获取 {sym}...")
            lc = await fetch_lighter(session, LIGHTER_MARKETS[sym], start, now)
            print(f"  Lighter: {len(lc)} candles")
            xc = await fetch_extended(session, EXTENDED_MARKETS[sym], start, now)
            print(f"  Extended: {len(xc)} candles")
            pairs = align(lc, xc)
            print(f"  匹配: {len(pairs)} pairs")
            spreads = compute_spreads(pairs)
            episodes = analyze_convergence(spreads)
            run_analysis(sym, spreads, episodes)


if __name__ == "__main__":
    asyncio.run(main())
