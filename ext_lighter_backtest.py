#!/usr/bin/env python3
"""
Extended vs Lighter 30天历史价差回测
使用两个交易所的1分钟K线数据计算历史价差
币种: BTC, ETH, SOL, HYPE, XRP
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

# ── API endpoints ──
LIGHTER_REST = "https://mainnet.zklighter.elliot.ai"
EXTENDED_REST = "https://api.starknet.extended.exchange"

# ── Market mappings ──
LIGHTER_MARKETS = {"ETH": 0, "BTC": 1, "SOL": 2}
EXTENDED_MARKETS = {
    "BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD",
    "HYPE": "HYPE-USD", "XRP": "XRP-USD",
}

# ── Fee structure ──
EXT_TAKER_FEE_BPS = 2.025  # 2.25 * 0.9 (10% rebate)
LIGHTER_TAKER_FEE_BPS = 0.0
ROUND_TRIP_FEE_BPS = EXT_TAKER_FEE_BPS * 2  # open + close both on Extended

DAYS = 30
RESOLUTION = "5m"  # 5-min candles for reasonable data volume
CANDLES_PER_DAY = 288  # 24*60/5


@dataclass
class CandleData:
    timestamp: int  # unix seconds
    open: float
    high: float
    low: float
    close: float
    volume: float = 0


@dataclass
class SymbolResult:
    symbol: str
    candle_count: int = 0
    matched_count: int = 0
    spreads: List[float] = field(default_factory=list)
    abs_spreads: List[float] = field(default_factory=list)
    above_breakeven: int = 0
    above_6bps: int = 0
    above_8bps: int = 0
    convergence_events: List[float] = field(default_factory=list)
    lighter_volumes: List[float] = field(default_factory=list)
    extended_volumes: List[float] = field(default_factory=list)
    hourly_spreads: Dict[int, List[float]] = field(default_factory=lambda: {h: [] for h in range(24)})
    daily_spreads: Dict[str, List[float]] = field(default_factory=dict)
    lighter_available: bool = False
    extended_available: bool = False


async def fetch_lighter_candles(session: aiohttp.ClientSession, market_id: int,
                                 start_ts: int, end_ts: int) -> List[CandleData]:
    """Fetch Lighter candles in chunks. API returns {code, r, c:[{t,o,h,l,c,v,V,i}]}"""
    all_candles = []
    chunk_size = 500
    current_start = start_ts

    while current_start < end_ts:
        chunk_end = min(current_start + chunk_size * 300, end_ts)
        url = (f"{LIGHTER_REST}/api/v1/candles"
               f"?market_id={market_id}&resolution={RESOLUTION}"
               f"&start_timestamp={current_start}&end_timestamp={chunk_end}"
               f"&count_back={chunk_size}")
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    candles_raw = data.get("c", [])
                    if not candles_raw:
                        break
                    for c in candles_raw:
                        ts = int(c.get("t", 0))
                        if ts > 1_000_000_000_000:
                            ts = ts // 1000
                        o = float(c.get("o", 0))
                        if o > 0:
                            all_candles.append(CandleData(
                                timestamp=ts,
                                open=o,
                                high=float(c.get("h", 0)),
                                low=float(c.get("l", 0)),
                                close=float(c.get("c", 0)),
                                volume=float(c.get("v", 0)),
                            ))
                elif resp.status == 403:
                    print(f"  [Lighter] 403 for market_id={market_id}, skipping")
                    break
                else:
                    text = await resp.text()
                    print(f"  [Lighter] {resp.status}: {text[:100]}")
                    break
        except Exception as e:
            print(f"  [Lighter] error: {e}")
            break

        current_start = chunk_end
        await asyncio.sleep(0.3)

    return all_candles


async def fetch_extended_candles(session: aiohttp.ClientSession, market: str,
                                  start_ts: int, end_ts: int) -> List[CandleData]:
    """Fetch Extended candles. API returns {status, data:[{o,h,l,c,v,T(ms)}]}"""
    all_candles = []
    current_end = end_ts * 1000
    start_ms = start_ts * 1000
    chunk_limit = 1000
    headers = {"User-Agent": "spread-backtest/1.0"}

    while current_end > start_ms:
        url = (f"{EXTENDED_REST}/api/v1/info/candles/{market}/trades"
               f"?interval=PT5M&limit={chunk_limit}&endTime={current_end}")
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    body = await resp.json()
                    candles_raw = body.get("data", [])
                    if not candles_raw:
                        break
                    batch = []
                    for c in candles_raw:
                        ts = int(c.get("T", 0))
                        if ts > 1_000_000_000_000:
                            ts = ts // 1000
                        o = float(c.get("o", 0))
                        if o > 0:
                            batch.append(CandleData(
                                timestamp=ts,
                                open=o,
                                high=float(c.get("h", 0)),
                                low=float(c.get("l", 0)),
                                close=float(c.get("c", 0)),
                                volume=float(c.get("v", 0)),
                            ))
                    if not batch:
                        break
                    all_candles.extend(batch)
                    oldest = min(c.timestamp for c in batch)
                    current_end = oldest * 1000 - 1
                elif resp.status == 404:
                    print(f"  [Extended] {market} not found (404)")
                    break
                else:
                    text = await resp.text()
                    print(f"  [Extended] {market} {resp.status}: {text[:100]}")
                    break
        except Exception as e:
            print(f"  [Extended] {market} error: {e}")
            break

        await asyncio.sleep(0.2)

    return all_candles


def align_candles(lighter: List[CandleData], extended: List[CandleData],
                  tolerance_s: int = 300) -> List[Tuple[CandleData, CandleData]]:
    """Align candles by timestamp within tolerance"""
    l_map = {}
    for c in lighter:
        bucket = (c.timestamp // tolerance_s) * tolerance_s
        if bucket not in l_map or c.volume > l_map[bucket].volume:
            l_map[bucket] = c

    pairs = []
    for c in extended:
        bucket = (c.timestamp // tolerance_s) * tolerance_s
        if bucket in l_map:
            pairs.append((l_map[bucket], c))

    return pairs


def analyze_symbol(symbol: str, pairs: List[Tuple[CandleData, CandleData]],
                   l_candles: List[CandleData], x_candles: List[CandleData]) -> SymbolResult:
    result = SymbolResult(symbol=symbol)
    result.candle_count = len(pairs)
    result.matched_count = len(pairs)
    result.lighter_available = len(l_candles) > 0
    result.extended_available = len(x_candles) > 0

    if not pairs:
        return result

    prev_spread = None
    in_position = False
    open_time = 0

    for l_c, x_c in pairs:
        l_mid = (l_c.high + l_c.low) / 2
        x_mid = (x_c.high + x_c.low) / 2
        if l_mid == 0 or x_mid == 0:
            continue

        # spread = (Extended_price - Lighter_price) / mid * 10000
        avg_mid = (l_mid + x_mid) / 2
        spread_bps = abs(x_mid - l_mid) / avg_mid * 10000

        result.spreads.append(spread_bps)
        result.abs_spreads.append(spread_bps)

        if spread_bps > ROUND_TRIP_FEE_BPS:
            result.above_breakeven += 1
        if spread_bps > 6.0:
            result.above_6bps += 1
        if spread_bps > 8.0:
            result.above_8bps += 1

        result.lighter_volumes.append(l_c.volume)
        result.extended_volumes.append(x_c.volume)

        # hourly distribution
        dt = datetime.fromtimestamp(l_c.timestamp, tz=timezone.utc)
        hour = dt.hour
        result.hourly_spreads[hour].append(spread_bps)

        day_str = dt.strftime("%Y-%m-%d")
        if day_str not in result.daily_spreads:
            result.daily_spreads[day_str] = []
        result.daily_spreads[day_str].append(spread_bps)

        # convergence tracking
        if not in_position and spread_bps > ROUND_TRIP_FEE_BPS:
            in_position = True
            open_time = l_c.timestamp
        elif in_position and spread_bps < 1.0:
            conv_time = l_c.timestamp - open_time
            if 0 < conv_time < 86400:
                result.convergence_events.append(conv_time)
            in_position = False

    return result


def print_report(results: Dict[str, SymbolResult]):
    print("\n" + "=" * 100)
    print(f"  Extended + Lighter 30天历史价差回测报告")
    print(f"  回测周期: {DAYS}天  K线周期: {RESOLUTION}")
    print(f"  Extended Taker Fee: {EXT_TAKER_FEE_BPS:.3f}bps (含10%反佣)")
    print(f"  完整套利周期费用: {ROUND_TRIP_FEE_BPS:.3f}bps (开仓+平仓)")
    print(f"  Lighter Taker Fee: {LIGHTER_TAKER_FEE_BPS:.1f}bps")
    print("=" * 100)

    ranking = []

    for sym in ["BTC", "ETH", "SOL", "HYPE", "XRP"]:
        r = results.get(sym)
        if not r:
            print(f"\n  {sym}: 未检测")
            continue

        if not r.lighter_available:
            print(f"\n  {sym}: Lighter不支持此币种")
            continue
        if not r.extended_available:
            print(f"\n  {sym}: Extended不支持此币种")
            continue
        if not r.spreads:
            print(f"\n  {sym}: 无匹配数据")
            continue

        n = len(r.spreads)
        avg = statistics.mean(r.spreads)
        med = statistics.median(r.spreads)
        std = statistics.stdev(r.spreads) if n > 1 else 0
        mx = max(r.spreads)
        mn = min(r.spreads)
        sorted_sp = sorted(r.spreads)
        p25 = sorted_sp[int(n * 0.25)]
        p75 = sorted_sp[int(n * 0.75)]
        p90 = sorted_sp[int(n * 0.90)]
        p95 = sorted_sp[int(n * 0.95)]

        pct_be = r.above_breakeven / n * 100
        pct_6 = r.above_6bps / n * 100
        pct_8 = r.above_8bps / n * 100

        avg_conv = statistics.mean(r.convergence_events) if r.convergence_events else float('nan')
        med_conv = statistics.median(r.convergence_events) if r.convergence_events else float('nan')
        n_conv = len(r.convergence_events)

        avg_l_vol = statistics.mean(r.lighter_volumes) if r.lighter_volumes else 0
        avg_x_vol = statistics.mean(r.extended_volumes) if r.extended_volumes else 0
        total_l_vol = sum(r.lighter_volumes)
        total_x_vol = sum(r.extended_volumes)

        # estimated daily profit (very rough)
        net_per_trade = avg - EXT_TAKER_FEE_BPS  # single-leg net
        trades_per_day = pct_be / 100 * CANDLES_PER_DAY * 0.3  # assume 30% of opportunities captured
        est_daily_bps = net_per_trade * trades_per_day if net_per_trade > 0 else 0

        # composite score
        score = 0
        if avg_conv > 0 and avg_conv == avg_conv:
            score = avg * (pct_be / 100) * min(3600 / avg_conv, 5)
        else:
            score = avg * (pct_be / 100)

        ranking.append((sym, score, avg, pct_be, avg_conv, avg_l_vol, avg_x_vol, est_daily_bps))

        print(f"\n{'─' * 100}")
        print(f"  {sym}")
        print(f"{'─' * 100}")
        print(f"  数据量: {n}根K线 ({n/CANDLES_PER_DAY:.1f}天)")
        print(f"")
        print(f"  ┌─ 价差统计 (bps) ─────────────────────────────────────────┐")
        print(f"  │ 平均={avg:6.2f}  中位={med:6.2f}  标准差={std:6.2f}          │")
        print(f"  │ 最小={mn:6.2f}  最大={mx:6.2f}                            │")
        print(f"  │ P25={p25:6.2f}  P75={p75:6.2f}  P90={p90:6.2f}  P95={p95:6.2f}  │")
        print(f"  └──────────────────────────────────────────────────────────┘")
        print(f"")
        print(f"  ┌─ 套利机会频率 ───────────────────────────────────────────┐")
        print(f"  │ >盈亏平衡({ROUND_TRIP_FEE_BPS:.1f}bps): {r.above_breakeven:5d}/{n} = {pct_be:5.1f}%     │")
        print(f"  │ >6bps:              {r.above_6bps:5d}/{n} = {pct_6:5.1f}%     │")
        print(f"  │ >8bps:              {r.above_8bps:5d}/{n} = {pct_8:5.1f}%     │")
        print(f"  └──────────────────────────────────────────────────────────┘")
        print(f"")
        print(f"  ┌─ 收敛时间 ───────────────────────────────────────────────┐")
        print(f"  │ 样本数={n_conv:4d}  平均={avg_conv/60:6.1f}分钟  中位={med_conv/60:6.1f}分钟│")
        print(f"  └──────────────────────────────────────────────────────────┘")
        print(f"")
        print(f"  ┌─ 流动性 (5分钟平均成交量) ───────────────────────────────┐")
        print(f"  │ Lighter: {avg_l_vol:12.4f} (30天总量: {total_l_vol:12.2f})     │")
        print(f"  │ Extended: {avg_x_vol:12.4f} (30天总量: {total_x_vol:12.2f})    │")
        print(f"  └──────────────────────────────────────────────────────────┘")

        # hourly heatmap
        print(f"\n  价差按小时分布 (UTC):")
        for h in range(24):
            hdata = r.hourly_spreads.get(h, [])
            if hdata:
                havg = statistics.mean(hdata)
                bar = "█" * int(havg)
                print(f"    {h:02d}:00  {havg:5.2f}bps  {bar}")

    # Final ranking
    print(f"\n{'=' * 100}")
    print(f"  综合排名")
    print(f"{'=' * 100}")
    ranking.sort(key=lambda x: x[1], reverse=True)
    print(f"  {'#':<3} {'币种':<6} {'评分':<10} {'平均价差':<10} {'>BE占比':<10} {'收敛(分)':<10} {'L成交量':<12} {'X成交量':<12} {'日估bps':<10}")
    print(f"  {'─'*3} {'─'*6} {'─'*10} {'─'*10} {'─'*10} {'─'*10} {'─'*12} {'─'*12} {'─'*10}")
    for i, (sym, sc, avg_sp, pct, conv, lvol, xvol, daily) in enumerate(ranking, 1):
        conv_min = conv / 60 if conv == conv else float('nan')
        print(f"  {i:<3} {sym:<6} {sc:<10.3f} {avg_sp:<10.2f} {pct:<10.1f} {conv_min:<10.1f} {lvol:<12.4f} {xvol:<12.4f} {daily:<10.1f}")

    print(f"\n{'=' * 100}")
    print(f"  结论与建议")
    print(f"{'=' * 100}")
    if ranking:
        best_sym = ranking[0][0]
        best_avg = ranking[0][2]
        best_pct = ranking[0][3]
        best_conv = ranking[0][4]
        print(f"  最佳套利币种: {best_sym}")
        print(f"  - 平均价差: {best_avg:.2f}bps")
        print(f"  - 超过盈亏平衡线占比: {best_pct:.1f}%")
        if best_conv == best_conv:
            print(f"  - 平均收敛时间: {best_conv/60:.1f}分钟")
        print(f"  - 完整周期费用: {ROUND_TRIP_FEE_BPS:.2f}bps")
        net = best_avg - EXT_TAKER_FEE_BPS
        print(f"  - 单腿净收益: {net:.2f}bps")
        print(f"")
        if best_avg < ROUND_TRIP_FEE_BPS:
            print(f"  ⚠ 警告: 即使最佳币种的平均价差({best_avg:.2f}bps)也低于完整周期费用({ROUND_TRIP_FEE_BPS:.2f}bps)")
            print(f"  ⚠ Extended+Lighter套利在当前费率下可能不可行")
            print(f"  ⚠ 建议: 只在价差 > {ROUND_TRIP_FEE_BPS + 2:.0f}bps 时开仓，或寻找更低费率的方案")
        else:
            print(f"  ✓ 平均价差高于费用，策略可行")
            print(f"  建议开仓阈值: >= {EXT_TAKER_FEE_BPS + 2:.1f}bps")

        not_on_lighter = [s for s in ["BTC", "ETH", "SOL", "HYPE", "XRP"]
                          if s not in LIGHTER_MARKETS]
        if not_on_lighter:
            print(f"\n  注意: {', '.join(not_on_lighter)} 在Lighter上不可用，无法进行套利")


async def main():
    now = int(time.time())
    start = now - DAYS * 86400
    print(f"回测区间: {datetime.fromtimestamp(start, tz=timezone.utc):%Y-%m-%d} → "
          f"{datetime.fromtimestamp(now, tz=timezone.utc):%Y-%m-%d} ({DAYS}天)")
    print(f"K线周期: {RESOLUTION}")
    print()

    results = {}

    async with aiohttp.ClientSession() as session:
        for sym in ["BTC", "ETH", "SOL", "HYPE", "XRP"]:
            print(f"{'─'*60}")
            print(f"正在获取 {sym} 数据...")

            # Lighter
            l_candles = []
            l_mid = LIGHTER_MARKETS.get(sym)
            if l_mid is not None:
                print(f"  Lighter market_id={l_mid}...")
                l_candles = await fetch_lighter_candles(session, l_mid, start, now)
                print(f"  Lighter: {len(l_candles)} candles")
            else:
                print(f"  Lighter: 不支持 {sym}")

            # Extended
            x_market = EXTENDED_MARKETS.get(sym)
            print(f"  Extended market={x_market}...")
            x_candles = await fetch_extended_candles(session, x_market, start, now)
            print(f"  Extended: {len(x_candles)} candles")

            # Align & analyze
            if l_candles and x_candles:
                pairs = align_candles(l_candles, x_candles)
                print(f"  匹配: {len(pairs)} pairs")
                results[sym] = analyze_symbol(sym, pairs, l_candles, x_candles)
            else:
                results[sym] = SymbolResult(
                    symbol=sym,
                    lighter_available=len(l_candles) > 0,
                    extended_available=len(x_candles) > 0,
                )

    print_report(results)


if __name__ == "__main__":
    asyncio.run(main())
