#!/usr/bin/env python3
"""
Extended + Lighter 300ms BBO Drift 回测 (30天)

核心目标：
1. 用30天历史1分钟K线，计算Extended vs Lighter的跨交易所价差
2. 模拟在300ms延迟下，开仓/平仓各会流失多少bps
3. 给出补偿drift后的真实参数建议

方法：
- 获取Lighter和Extended的1分钟K线
- 对齐时间戳，计算每分钟的价差(spread)
- 用相邻K线的价格变化来估算300ms内的drift
- 统计开仓(spread>阈值时)和平仓(spread→0时)的drift分布
"""
import asyncio
import aiohttp
import json
import time
import math
import statistics
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict

LIGHTER_REST = "https://mainnet.zklighter.elliot.ai"
EXTENDED_REST = "https://api.starknet.extended.exchange"

LIGHTER_MARKETS = {"ETH": 0, "BTC": 1, "SOL": 2}
EXTENDED_MARKETS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD"}

# Extended fee: taker 2.25bps, rebate 10% => effective 2.025bps
# Lighter fee: taker 0bps
# Round-trip: 2.025bps (Extended open taker) + 2.025bps (Extended close taker) = 4.05bps
EXTENDED_TAKER_FEE = 2.025  # bps (after 10% rebate)
ROUND_TRIP_FEE = 4.05  # bps


@dataclass
class Candle:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float


async def fetch_lighter_candles(session: aiohttp.ClientSession, mid: int,
                                start: int, end: int) -> List[Candle]:
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


async def fetch_extended_candles(session: aiohttp.ClientSession, mkt: str,
                                 start: int, end: int) -> List[Candle]:
    out = []
    cur_end = end * 1000
    start_ms = start * 1000
    hdr = {"User-Agent": "drift-bt/1.0"}
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
                batch = []
                for c in raw:
                    ts = int(c.get("T", 0))
                    if ts > 1e12:
                        ts //= 1000
                    o = float(c.get("o", 0))
                    if o > 0:
                        batch.append(Candle(ts=ts, o=o, h=float(c["h"]),
                                            l=float(c["l"]), c=float(c["c"]),
                                            v=float(c["v"])))
                if not batch:
                    break
                out.extend(batch)
                earliest_ts = min(c.ts for c in batch)
                cur_end = earliest_ts * 1000 - 1
        except Exception as e:
            print(f"    Extended err: {e}")
            break
        await asyncio.sleep(0.2)
    return out


def align_candles(lighter: List[Candle], extended: List[Candle],
                  tolerance: int = 120) -> List[Tuple[Candle, Candle]]:
    """Align candles by timestamp within tolerance seconds"""
    ext_map = {}
    for c in extended:
        bucket = c.ts // 60 * 60
        ext_map[bucket] = c

    pairs = []
    for lc in lighter:
        bucket = lc.ts // 60 * 60
        for offset in [0, -60, 60]:
            ec = ext_map.get(bucket + offset)
            if ec and abs(ec.ts - lc.ts) <= tolerance:
                pairs.append((lc, ec))
                break
    return pairs


def compute_spreads(pairs: List[Tuple[Candle, Candle]]) -> List[Tuple[int, float, float, float]]:
    """
    Returns: [(timestamp, spread_bps, lighter_mid, extended_mid)]
    spread = |lighter_mid - extended_mid| / avg_mid * 10000
    """
    spreads = []
    for lc, ec in pairs:
        l_mid = (lc.o + lc.c) / 2
        e_mid = (ec.o + ec.c) / 2
        avg = (l_mid + e_mid) / 2
        if avg == 0:
            continue
        sp = abs(l_mid - e_mid) / avg * 10000
        spreads.append((lc.ts, sp, l_mid, e_mid))
    return spreads


def estimate_300ms_drift_from_candles(pairs: List[Tuple[Candle, Candle]]) -> Dict:
    """
    Estimate 300ms drift using 1-minute candle data.

    For each candle pair, compute:
    1. Lighter intra-candle volatility (H-L range)
    2. Extended intra-candle volatility
    3. Cross-exchange spread change between consecutive candles

    300ms ≈ 0.5% of 1 minute → scale by sqrt(0.005)
    """
    lighter_drifts = []
    extended_drifts = []
    spread_drifts = []

    for i in range(1, len(pairs)):
        lc0, ec0 = pairs[i - 1]
        lc1, ec1 = pairs[i]

        # Skip if gap > 2 minutes
        if lc1.ts - lc0.ts > 180:
            continue

        # Lighter 1-min volatility
        l_mid = (lc0.h + lc0.l) / 2
        if l_mid > 0:
            l_hl_bps = (lc0.h - lc0.l) / l_mid * 10000
            l_oc_bps = abs(lc0.c - lc0.o) / l_mid * 10000
            lighter_drifts.append(l_oc_bps)

        # Extended 1-min volatility
        e_mid = (ec0.h + ec0.l) / 2
        if e_mid > 0:
            e_oc_bps = abs(ec0.c - ec0.o) / e_mid * 10000
            extended_drifts.append(e_oc_bps)

        # Cross-exchange spread change
        avg0 = ((lc0.o + lc0.c) / 2 + (ec0.o + ec0.c) / 2) / 2
        avg1 = ((lc1.o + lc1.c) / 2 + (ec1.o + ec1.c) / 2) / 2
        if avg0 > 0 and avg1 > 0:
            sp0 = abs((lc0.o + lc0.c) / 2 - (ec0.o + ec0.c) / 2) / avg0 * 10000
            sp1 = abs((lc1.o + lc1.c) / 2 - (ec1.o + ec1.c) / 2) / avg1 * 10000
            spread_drifts.append(abs(sp1 - sp0))

    scale = math.sqrt(0.005)  # 300ms / 60s

    return {
        "lighter_1m_oc": lighter_drifts,
        "extended_1m_oc": extended_drifts,
        "spread_change_1m": spread_drifts,
        "scale_factor": scale,
    }


def simulate_execution_drift(spreads: List[Tuple[int, float, float, float]],
                              open_threshold: float = 4.3,
                              close_threshold: float = 0.0) -> Dict:
    """
    Simulate what happens when you try to open at spread >= open_threshold
    and close at spread <= close_threshold, with 300ms execution delay.

    Using consecutive 1-min spread changes as proxy:
    - When spread >= threshold, the NEXT candle's spread shows what you'd actually get
    """
    open_events = []  # (detected_spread, actual_spread_1min_later, drift)
    close_events = []

    for i in range(len(spreads) - 1):
        ts0, sp0, _, _ = spreads[i]
        ts1, sp1, _, _ = spreads[i + 1]

        if ts1 - ts0 > 180:
            continue

        # Open signal: spread >= threshold
        if sp0 >= open_threshold:
            drift = sp0 - sp1  # positive = spread narrowed (you lost)
            open_events.append({
                "detected": sp0,
                "next": sp1,
                "drift": drift,
                "drift_pct": drift / sp0 * 100 if sp0 > 0 else 0,
            })

        # Close signal: spread <= close_threshold
        if sp0 <= close_threshold + 1.0:  # near close threshold
            drift = sp1 - sp0  # positive = spread widened (you lost on close)
            close_events.append({
                "detected": sp0,
                "next": sp1,
                "drift": drift,
            })

    return {"open_events": open_events, "close_events": close_events}


def print_report(symbol: str, spreads, drift_data, sim_data, pairs):
    n_candles = len(pairs)
    n_spreads = len(spreads)
    sp_values = [s[1] for s in spreads]

    print(f"\n{'━' * 100}")
    print(f"  {symbol} — Extended + Lighter 300ms Drift 回测报告 (30天)")
    print(f"{'━' * 100}")

    # Basic spread stats
    if sp_values:
        avg_sp = statistics.mean(sp_values)
        med_sp = statistics.median(sp_values)
        p75_sp = sorted(sp_values)[int(len(sp_values) * 0.75)]
        p90_sp = sorted(sp_values)[int(len(sp_values) * 0.90)]
        p95_sp = sorted(sp_values)[int(len(sp_values) * 0.95)]

        above_4 = sum(1 for s in sp_values if s >= 4.0) / len(sp_values) * 100
        above_4_3 = sum(1 for s in sp_values if s >= 4.3) / len(sp_values) * 100
        above_5 = sum(1 for s in sp_values if s >= 5.0) / len(sp_values) * 100
        above_6 = sum(1 for s in sp_values if s >= 6.0) / len(sp_values) * 100
        below_1 = sum(1 for s in sp_values if s <= 1.0) / len(sp_values) * 100
        below_0_5 = sum(1 for s in sp_values if s <= 0.5) / len(sp_values) * 100

        print(f"\n  ┌─ 基础价差统计 ({n_candles}对K线, {n_spreads}个价差样本) ─────────────┐")
        print(f"  │ 平均: {avg_sp:.2f}bps  中位: {med_sp:.2f}bps  P75: {p75_sp:.2f}bps  P90: {p90_sp:.2f}bps  P95: {p95_sp:.2f}bps │")
        print(f"  │                                                                              │")
        print(f"  │ 价差分布:                                                                    │")
        print(f"  │   >=4.0bps: {above_4:.1f}%   >=4.3bps: {above_4_3:.1f}%   >=5.0bps: {above_5:.1f}%   >=6.0bps: {above_6:.1f}% │")
        print(f"  │   <=1.0bps: {below_1:.1f}%   <=0.5bps: {below_0_5:.1f}%                                     │")
        print(f"  │ 手续费盈亏线: 4.05bps (Extended往返)                                         │")
        print(f"  └──────────────────────────────────────────────────────────────────────────────┘")

    # Drift estimation
    dd = drift_data
    scale = dd["scale_factor"]

    if dd["lighter_1m_oc"]:
        l_avg = statistics.mean(dd["lighter_1m_oc"])
        l_med = statistics.median(dd["lighter_1m_oc"])
        l_p90 = sorted(dd["lighter_1m_oc"])[int(len(dd["lighter_1m_oc"]) * 0.9)]

        e_avg = statistics.mean(dd["extended_1m_oc"]) if dd["extended_1m_oc"] else 0
        e_med = statistics.median(dd["extended_1m_oc"]) if dd["extended_1m_oc"] else 0
        e_p90 = sorted(dd["extended_1m_oc"])[int(len(dd["extended_1m_oc"]) * 0.9)] if dd["extended_1m_oc"] else 0

        sc_avg = statistics.mean(dd["spread_change_1m"]) if dd["spread_change_1m"] else 0
        sc_med = statistics.median(dd["spread_change_1m"]) if dd["spread_change_1m"] else 0
        sc_p90 = sorted(dd["spread_change_1m"])[int(len(dd["spread_change_1m"]) * 0.9)] if dd["spread_change_1m"] else 0

        print(f"\n  ┌─ 1分钟价格波动 → 300ms drift估算 (×{scale:.4f}) ──────────────────┐")
        print(f"  │                                                                              │")
        print(f"  │  Lighter 1分钟|open-close|:                                                  │")
        print(f"  │    平均={l_avg:.2f}bps  中位={l_med:.2f}bps  P90={l_p90:.2f}bps                    │")
        print(f"  │    → 300ms估算: 平均={l_avg*scale:.3f}bps  P90={l_p90*scale:.3f}bps               │")
        print(f"  │                                                                              │")
        print(f"  │  Extended 1分钟|open-close|:                                                 │")
        print(f"  │    平均={e_avg:.2f}bps  中位={e_med:.2f}bps  P90={e_p90:.2f}bps                    │")
        print(f"  │    → 300ms估算: 平均={e_avg*scale:.3f}bps  P90={e_p90*scale:.3f}bps               │")
        print(f"  │                                                                              │")
        print(f"  │  跨交易所价差变化(1分钟):                                                     │")
        print(f"  │    平均={sc_avg:.2f}bps  中位={sc_med:.2f}bps  P90={sc_p90:.2f}bps                 │")
        print(f"  │    → 300ms估算: 平均={sc_avg*scale:.3f}bps  P90={sc_p90*scale:.3f}bps             │")
        print(f"  └──────────────────────────────────────────────────────────────────────────────┘")

    # Execution simulation
    oe = sim_data["open_events"]
    ce = sim_data["close_events"]

    if oe:
        drifts = [e["drift"] for e in oe]
        drift_pcts = [e["drift_pct"] for e in oe]
        positive_drifts = [d for d in drifts if d > 0]  # spread narrowed = lost
        negative_drifts = [d for d in drifts if d < 0]  # spread widened = gained

        avg_drift = statistics.mean(drifts)
        med_drift = statistics.median(drifts)
        avg_pct = statistics.mean(drift_pcts)
        pct_lost = len(positive_drifts) / len(drifts) * 100

        avg_loss = statistics.mean(positive_drifts) if positive_drifts else 0
        avg_gain = abs(statistics.mean(negative_drifts)) if negative_drifts else 0

        # Percentile of drift
        sorted_d = sorted(drifts)
        p75_d = sorted_d[int(len(sorted_d) * 0.75)]
        p90_d = sorted_d[int(len(sorted_d) * 0.90)]
        p95_d = sorted_d[int(len(sorted_d) * 0.95)]

        # Actual captured spread
        actual_caps = [e["next"] for e in oe]
        avg_actual = statistics.mean(actual_caps)
        detected_avg = statistics.mean([e["detected"] for e in oe])

        print(f"\n  ┌─ 开仓模拟 (检测spread>=4.3bps, {len(oe)}次机会) ─────────────────┐")
        print(f"  │                                                                              │")
        print(f"  │  检测到的平均价差: {detected_avg:.2f}bps                                      │")
        print(f"  │  1分钟后实际价差: {avg_actual:.2f}bps                                         │")
        print(f"  │  平均drift: {avg_drift:.2f}bps ({avg_pct:.1f}%)                               │")
        print(f"  │  中位drift: {med_drift:.2f}bps                                                │")
        print(f"  │  P75 drift: {p75_d:.2f}bps  P90: {p90_d:.2f}bps  P95: {p95_d:.2f}bps          │")
        print(f"  │                                                                              │")
        print(f"  │  价差收窄(流失): {pct_lost:.1f}% 的次数, 平均流失={avg_loss:.2f}bps             │")
        print(f"  │  价差扩大(有利): {100-pct_lost:.1f}% 的次数, 平均获利={avg_gain:.2f}bps         │")
        print(f"  │                                                                              │")

        # Bucket analysis
        buckets = [(4.0, 5.0), (5.0, 6.0), (6.0, 8.0), (8.0, 999)]
        print(f"  │  按开仓价差分桶:                                                              │")
        for lo, hi in buckets:
            bucket_events = [e for e in oe if lo <= e["detected"] < hi]
            if bucket_events:
                b_det = statistics.mean([e["detected"] for e in bucket_events])
                b_act = statistics.mean([e["next"] for e in bucket_events])
                b_drift = statistics.mean([e["drift"] for e in bucket_events])
                b_pct = statistics.mean([e["drift_pct"] for e in bucket_events])
                b_lost = sum(1 for e in bucket_events if e["drift"] > 0) / len(bucket_events) * 100
                label = f"{lo:.0f}-{hi:.0f}" if hi < 100 else f">{lo:.0f}"
                print(f"  │    [{label}bps] {len(bucket_events)}次: 检测{b_det:.1f}→实际{b_act:.1f}bps, 流失{b_drift:.2f}bps({b_pct:.0f}%), 流失率{b_lost:.0f}% │")
        print(f"  └──────────────────────────────────────────────────────────────────────────────┘")

    if ce:
        close_drifts = [e["drift"] for e in ce]
        avg_cd = statistics.mean(close_drifts)
        med_cd = statistics.median(close_drifts)
        pct_widen = sum(1 for d in close_drifts if d > 0) / len(close_drifts) * 100
        widen_events = [d for d in close_drifts if d > 0]
        avg_widen = statistics.mean(widen_events) if widen_events else 0

        sorted_cd = sorted(close_drifts)
        p75_cd = sorted_cd[int(len(sorted_cd) * 0.75)]
        p90_cd = sorted_cd[int(len(sorted_cd) * 0.90)]

        print(f"\n  ┌─ 平仓模拟 (检测spread<=1.0bps, {len(ce)}次) ────────────────────┐")
        print(f"  │                                                                              │")
        print(f"  │  平均drift: {avg_cd:.2f}bps  中位: {med_cd:.2f}bps                            │")
        print(f"  │  P75: {p75_cd:.2f}bps  P90: {p90_cd:.2f}bps                                   │")
        print(f"  │  价差反弹(流失): {pct_widen:.1f}%, 平均反弹={avg_widen:.2f}bps                  │")
        print(f"  │                                                                              │")
        print(f"  │  含义: 当你检测到spread≈0想平仓时,                                            │")
        print(f"  │  1分钟后spread平均变成了 {avg_cd:.2f}bps (反弹了)                              │")
        print(f"  │  → 你的taker平仓单实际成交时spread已经不是0了                                  │")
        print(f"  └──────────────────────────────────────────────────────────────────────────────┘")

    # Parameter recommendations
    if oe:
        avg_open_loss = statistics.mean([e["drift"] for e in oe if e["drift"] > 0]) if [e for e in oe if e["drift"] > 0] else 0
        p75_open_loss = sorted([e["drift"] for e in oe])[int(len(oe) * 0.75)]
        p90_open_loss = sorted([e["drift"] for e in oe])[int(len(oe) * 0.90)]

    if ce:
        avg_close_loss = statistics.mean([d for d in close_drifts if d > 0]) if widen_events else 0
        p75_close_loss = sorted_cd[int(len(sorted_cd) * 0.75)]
        p90_close_loss = sorted_cd[int(len(sorted_cd) * 0.90)]

    if oe and ce:
        print(f"\n  ┌─ ★ 参数补偿建议 ─────────────────────────────────────────────────────────┐")
        print(f"  │                                                                              │")
        print(f"  │  当前参数: open=4.3bps, close=0bps                                           │")
        print(f"  │  手续费: 4.05bps (往返)                                                      │")
        print(f"  │                                                                              │")

        # Scenario 1: current params
        if oe:
            actual_open = detected_avg - avg_drift
            actual_close_loss = avg_widen if widen_events else 0
            net = actual_open - actual_close_loss - ROUND_TRIP_FEE
            print(f"  │  场景1 (当前参数 open=4.3, close=0):                                       │")
            print(f"  │    开仓: 检测{detected_avg:.1f} → 实际捕获{actual_open:.2f}bps               │")
            print(f"  │    平仓: 检测0 → 实际流失{actual_close_loss:.2f}bps                          │")
            print(f"  │    净收益: {actual_open:.2f} - {actual_close_loss:.2f} - {ROUND_TRIP_FEE} = {net:.2f}bps │")
            print(f"  │                                                                              │")

        # Scenario 2: compensated params
        comp_open = 4.3 + p75_open_loss
        comp_close = 0 - p75_close_loss
        print(f"  │  场景2 (P75补偿: open={comp_open:.1f}, close={comp_close:.1f}):                │")
        print(f"  │    开仓多要{p75_open_loss:.2f}bps补偿 → 机会减少但捕获更稳                     │")
        print(f"  │    平仓提前{abs(p75_close_loss):.2f}bps → 避免反弹流失                          │")
        print(f"  │                                                                              │")

        # Scenario 3: aggressive
        comp_open_90 = 4.3 + p90_open_loss
        comp_close_90 = 0 - p90_close_loss
        print(f"  │  场景3 (P90补偿: open={comp_open_90:.1f}, close={comp_close_90:.1f}):          │")
        print(f"  │    更保守, 但几乎消除drift流失                                                │")
        print(f"  │                                                                              │")

        # Different open thresholds
        print(f"  │  不同开仓阈值的实际效果:                                                      │")
        for thresh in [4.0, 4.3, 4.5, 5.0, 5.5, 6.0]:
            t_events = [e for e in oe if e["detected"] >= thresh]
            if t_events:
                t_det = statistics.mean([e["detected"] for e in t_events])
                t_act = statistics.mean([e["next"] for e in t_events])
                t_net = t_act - actual_close_loss - ROUND_TRIP_FEE
                print(f"  │    open>={thresh}: {len(t_events)}次, 检测{t_det:.1f}→实际{t_act:.1f}, 净{t_net:+.2f}bps │")
        print(f"  │                                                                              │")

        # Different close thresholds
        print(f"  │  不同平仓阈值的效果:                                                          │")
        for ct in [0.0, -0.5, -1.0, -1.5, -2.0]:
            # close at spread <= |ct|, but ct is negative meaning "give up |ct| bps"
            # Actually in the script, close_spread = ct means close when spread <= |ct|
            # Negative close_spread means close even when spread is slightly negative
            label = f"{ct}" if ct <= 0 else f"+{ct}"
            # Estimate: if you close earlier (higher threshold), less drift
            # close_threshold = abs(ct) means you give up |ct| bps but avoid drift
            give_up = abs(ct)
            avoided_drift = avg_widen * (1 - give_up / max(avg_widen, 0.01))
            actual_loss = max(0, avg_widen - give_up)
            print(f"  │    close={label}: 放弃{give_up:.1f}bps, 预计避免{avg_widen - actual_loss:.2f}bps反弹流失 │")
        print(f"  └──────────────────────────────────────────────────────────────────────────────┘")

    # Summary with optimal params
    print(f"\n  ┌─ ★★ 最终推荐 ─────────────────────────────────────────────────────────────┐")
    print(f"  │                                                                              │")
    if oe:
        # Find threshold where net > 0
        best_thresh = None
        best_net = -999
        for thresh in [x / 10 for x in range(40, 100)]:
            t_events = [e for e in oe if e["detected"] >= thresh]
            if len(t_events) >= 5:
                t_act = statistics.mean([e["next"] for e in t_events])
                t_net = t_act - (avg_widen if widen_events else 0) - ROUND_TRIP_FEE
                if t_net > best_net:
                    best_net = t_net
                    best_thresh = thresh
        if best_thresh:
            t_events = [e for e in oe if e["detected"] >= best_thresh]
            print(f"  │  最优开仓阈值: {best_thresh:.1f}bps (净收益{best_net:+.2f}bps, {len(t_events)}次机会/30天) │")
        else:
            print(f"  │  警告: 未找到净收益为正的开仓阈值                                          │")
        print(f"  │  推荐平仓阈值: -0.5 到 -1.0bps (提前平仓避免反弹)                             │")
    print(f"  │                                                                              │")
    print(f"  └──────────────────────────────────────────────────────────────────────────────┘")


async def run_symbol(session, symbol):
    now = int(time.time())
    start = now - 30 * 86400

    mid = LIGHTER_MARKETS[symbol]
    mkt = EXTENDED_MARKETS[symbol]

    print(f"\n  [{symbol}] 获取Lighter 1m K线...")
    l_candles = await fetch_lighter_candles(session, mid, start, now)
    print(f"  [{symbol}] Lighter: {len(l_candles)} candles")

    print(f"  [{symbol}] 获取Extended 1m K线...")
    e_candles = await fetch_extended_candles(session, mkt, start, now)
    print(f"  [{symbol}] Extended: {len(e_candles)} candles")

    pairs = align_candles(l_candles, e_candles)
    print(f"  [{symbol}] 对齐: {len(pairs)} pairs")

    if len(pairs) < 100:
        print(f"  [{symbol}] 数据不足, 跳过")
        return

    spreads = compute_spreads(pairs)
    drift_data = estimate_300ms_drift_from_candles(pairs)
    sim_data = simulate_execution_drift(spreads, open_threshold=4.3, close_threshold=0.0)

    print_report(symbol, spreads, drift_data, sim_data, pairs)


async def main():
    print("=" * 100)
    print("  Extended + Lighter 300ms BBO Drift 回测 (30天历史数据)")
    print("  手续费: Extended taker 2.025bps (含10%返佣) × 2 = 4.05bps 往返")
    print("=" * 100)

    async with aiohttp.ClientSession() as session:
        for sym in ["BTC", "ETH", "SOL"]:
            await run_symbol(session, sym)

    print(f"\n{'=' * 100}")
    print("  回测完成")
    print(f"{'=' * 100}")


if __name__ == "__main__":
    asyncio.run(main())
