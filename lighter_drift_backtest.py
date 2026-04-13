#!/usr/bin/env python3
"""
Lighter 300ms BBO Drift 回测
用1分钟K线的 open vs close 和 high-low 来估算短期价格波动
然后用实时WS采集精确的300ms drift数据（10分钟样本）

目标：测量在300ms延迟下，开仓和平仓各会流失多少bps
"""
import asyncio
import aiohttp
import json
import time
import statistics
import websockets
from datetime import datetime, timezone
from typing import List, Dict
from dataclasses import dataclass, field
from collections import defaultdict

LIGHTER_REST = "https://mainnet.zklighter.elliot.ai"
LIGHTER_WS = "wss://mainnet.zklighter.elliot.ai/stream"
EXTENDED_REST = "https://api.starknet.extended.exchange"

LIGHTER_MARKETS = {"ETH": 0, "BTC": 1, "SOL": 2}
EXTENDED_MARKETS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD"}

# ── Phase 1: Historical candle-based drift estimation (30 days) ──

async def fetch_lighter_candles_1m(session, mid, start, end):
    """Fetch 1-minute candles for finer granularity"""
    out = []
    cur = start
    while cur < end:
        ce = min(cur + 500 * 60, end)
        url = (f"{LIGHTER_REST}/api/v1/candles?market_id={mid}&resolution=1m"
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
                        o = float(c["o"])
                        if o > 0:
                            out.append({
                                "ts": ts, "o": o, "h": float(c["h"]),
                                "l": float(c["l"]), "c": float(c["c"]),
                                "v": float(c["v"])
                            })
                else:
                    break
        except Exception as e:
            print(f"  err: {e}")
            break
        cur = ce
        await asyncio.sleep(0.25)
    return out


async def fetch_extended_candles_1m(session, mkt, start, end):
    out = []
    cur_end = end * 1000
    start_ms = start * 1000
    hdr = {"User-Agent": "drift/1.0"}
    while cur_end > start_ms:
        url = (f"{EXTENDED_REST}/api/v1/info/candles/{mkt}/trades"
               f"?interval=PT1M&limit=1000&endTime={cur_end}")
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
                        o = float(c["o"])
                        if o > 0:
                            batch.append({"ts": ts, "o": o, "h": float(c["h"]),
                                          "l": float(c["l"]), "c": float(c["c"]),
                                          "v": float(c["v"])})
                    if not batch:
                        break
                    out.extend(batch)
                    cur_end = min(c["ts"] for c in batch) * 1000 - 1
                else:
                    break
        except Exception as e:
            print(f"  err: {e}")
            break
        await asyncio.sleep(0.2)
    return out


def estimate_candle_drift(candles):
    """
    Estimate intra-candle price movement as a proxy for 300ms drift.
    For each 1-min candle: the open-to-close move and high-low range
    give us the typical price volatility within 1 minute.
    300ms ≈ 0.5% of 1 minute, so drift ≈ sqrt(0.005) * 1min_volatility
    (using square-root-of-time scaling)
    """
    drifts_bps = []
    for c in candles:
        mid = (c["h"] + c["l"]) / 2
        if mid == 0:
            continue
        hl_range_bps = (c["h"] - c["l"]) / mid * 10000
        oc_move_bps = abs(c["c"] - c["o"]) / mid * 10000
        # 300ms / 60000ms = 0.005, sqrt scaling
        drift_300ms = oc_move_bps * (0.005 ** 0.5)
        drifts_bps.append(drift_300ms)
    return drifts_bps


# ── Phase 2: Real-time WS measurement of actual 300ms drift ──

async def measure_realtime_drift(market_id: int, symbol: str, duration_s: int = 600):
    """
    Connect to Lighter WS, record BBO snapshots, then compute
    the actual price change over 300ms windows.
    """
    bbo_history = []  # [(timestamp_ms, bid, ask)]
    book = {"bids": {}, "asks": {}}

    print(f"\n  [{symbol}] 实时采集 {duration_s}s BBO数据...")

    try:
        async with websockets.connect(LIGHTER_WS, ping_interval=10, ping_timeout=5) as ws:
            await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{market_id}"}))

            start = time.time()
            while (time.time() - start) < duration_s:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=2)
                    msg = json.loads(raw)
                    mt = msg.get("type", "")
                    if mt not in ("subscribed/order_book", "update/order_book"):
                        continue

                    ob = msg.get("order_book", {})
                    bids_raw = ob.get("bids", [])
                    asks_raw = ob.get("asks", [])

                    if mt == "subscribed/order_book":
                        book["bids"] = {}
                        book["asks"] = {}
                        for b in bids_raw:
                            book["bids"][float(b["price"])] = float(b["size"])
                        for a in asks_raw:
                            book["asks"][float(a["price"])] = float(a["size"])
                    else:
                        for b in bids_raw:
                            p, s = float(b["price"]), float(b["size"])
                            if s == 0:
                                book["bids"].pop(p, None)
                            else:
                                book["bids"][p] = s
                        for a in asks_raw:
                            p, s = float(a["price"]), float(a["size"])
                            if s == 0:
                                book["asks"].pop(p, None)
                            else:
                                book["asks"][p] = s

                    if book["bids"] and book["asks"]:
                        best_bid = max(book["bids"].keys())
                        best_ask = min(book["asks"].keys())
                        now_ms = int(time.time() * 1000)
                        bbo_history.append((now_ms, best_bid, best_ask))

                except asyncio.TimeoutError:
                    continue
    except Exception as e:
        print(f"  [{symbol}] WS error: {e}")

    print(f"  [{symbol}] 采集到 {len(bbo_history)} 个BBO快照")
    return bbo_history


def compute_drift_from_bbo(bbo_history, delay_ms=300):
    """
    For each BBO snapshot, find the snapshot ~300ms later
    and compute the price change.
    """
    if len(bbo_history) < 10:
        return [], []

    ask_drifts = []  # buy side: ask price change (开仓买入流失)
    bid_drifts = []  # sell side: bid price change (平仓卖出流失)

    n = len(bbo_history)
    j = 0
    for i in range(n):
        t0, bid0, ask0 = bbo_history[i]
        mid0 = (bid0 + ask0) / 2
        if mid0 == 0:
            continue

        # Find snapshot closest to t0 + delay_ms
        while j < n and bbo_history[j][0] < t0 + delay_ms:
            j += 1
        if j >= n:
            break

        t1, bid1, ask1 = bbo_history[j]
        if (t1 - t0) > delay_ms * 3:
            continue

        # Ask drift: when you want to buy at ask0, after 300ms the ask might move
        ask_change_bps = (ask1 - ask0) / mid0 * 10000
        ask_drifts.append(ask_change_bps)

        # Bid drift: when you want to sell at bid0, after 300ms the bid might move
        bid_change_bps = (bid1 - bid0) / mid0 * 10000
        bid_drifts.append(bid_change_bps)

    return ask_drifts, bid_drifts


def analyze_drift(symbol, ask_drifts, bid_drifts, candle_drifts=None):
    print(f"\n{'━' * 100}")
    print(f"  {symbol} — 300ms BBO Drift 分析")
    print(f"{'━' * 100}")

    if candle_drifts:
        avg_cd = statistics.mean(candle_drifts)
        med_cd = statistics.median(candle_drifts)
        p90_cd = sorted(candle_drifts)[int(len(candle_drifts) * 0.9)]
        print(f"\n  ┌─ K线估算的300ms drift (30天, {len(candle_drifts)}根1分钟K线) ─┐")
        print(f"  │ 平均: {avg_cd:.3f}bps  中位: {med_cd:.3f}bps  P90: {p90_cd:.3f}bps │")
        print(f"  └──────────────────────────────────────────────────────────┘")

    if not ask_drifts or not bid_drifts:
        print(f"  实时数据不足")
        return

    n = len(ask_drifts)
    abs_ask = [abs(d) for d in ask_drifts]
    abs_bid = [abs(d) for d in bid_drifts]

    avg_ask = statistics.mean(abs_ask)
    med_ask = statistics.median(abs_ask)
    p75_ask = sorted(abs_ask)[int(n * 0.75)]
    p90_ask = sorted(abs_ask)[int(n * 0.90)]
    p95_ask = sorted(abs_ask)[int(n * 0.95)]

    avg_bid = statistics.mean(abs_bid)
    med_bid = statistics.median(abs_bid)
    p75_bid = sorted(abs_bid)[int(n * 0.75)]
    p90_bid = sorted(abs_bid)[int(n * 0.90)]
    p95_bid = sorted(abs_bid)[int(n * 0.95)]

    # Adverse drift: price moves against you
    # Opening buy: ask goes UP (you pay more) = adverse
    ask_adverse = [d for d in ask_drifts if d > 0]
    # Closing sell: bid goes DOWN (you receive less) = adverse
    bid_adverse = [d for d in bid_drifts if d < 0]

    pct_ask_adverse = len(ask_adverse) / n * 100
    pct_bid_adverse = len(bid_adverse) / n * 100

    avg_ask_adv = statistics.mean(ask_adverse) if ask_adverse else 0
    avg_bid_adv = abs(statistics.mean(bid_adverse)) if bid_adverse else 0

    print(f"\n  ┌─ 实时300ms Drift ({n}个样本) ──────────────────────────────────────────────┐")
    print(f"  │                                                                              │")
    print(f"  │  开仓(买入ask侧) |abs| drift:                                               │")
    print(f"  │    平均={avg_ask:.3f}bps  中位={med_ask:.3f}bps  P75={p75_ask:.3f}bps  P90={p90_ask:.3f}bps  P95={p95_ask:.3f}bps │")
    print(f"  │    不利方向(价格上涨): {len(ask_adverse)}/{n} = {pct_ask_adverse:.1f}%  平均流失={avg_ask_adv:.3f}bps │")
    print(f"  │                                                                              │")
    print(f"  │  平仓(卖出bid侧) |abs| drift:                                               │")
    print(f"  │    平均={avg_bid:.3f}bps  中位={med_bid:.3f}bps  P75={p75_bid:.3f}bps  P90={p90_bid:.3f}bps  P95={p95_bid:.3f}bps │")
    print(f"  │    不利方向(价格下跌): {len(bid_adverse)}/{n} = {pct_bid_adverse:.1f}%  平均流失={avg_bid_adv:.3f}bps │")
    print(f"  │                                                                              │")
    print(f"  │  总流失估算:                                                                  │")
    total_drift = avg_ask_adv * (pct_ask_adverse / 100) + avg_bid_adv * (pct_bid_adverse / 100)
    print(f"  │    开仓预期流失: {avg_ask_adv * pct_ask_adverse / 100:.3f}bps                                          │")
    print(f"  │    平仓预期流失: {avg_bid_adv * pct_bid_adverse / 100:.3f}bps                                          │")
    print(f"  │    单次完整周期预期流失: {total_drift:.3f}bps                                    │")
    print(f"  └──────────────────────────────────────────────────────────────────────────────┘")

    # Parameter recommendations
    print(f"\n  ┌─ 参数补偿建议 ──────────────────────────────────────────────────────────────┐")
    print(f"  │                                                                              │")
    print(f"  │  你设置 open_spread=4.3bps, close_spread=0bps 时:                            │")
    open_actual = 4.3 - avg_ask_adv * (pct_ask_adverse / 100)
    close_actual = 0 + avg_bid_adv * (pct_bid_adverse / 100)
    print(f"  │    开仓实际捕获: 4.3 - {avg_ask_adv * pct_ask_adverse / 100:.2f} = ~{open_actual:.2f}bps              │")
    print(f"  │    平仓实际流失: 0 + {avg_bid_adv * pct_bid_adverse / 100:.2f} = ~{close_actual:.2f}bps (多付出)       │")
    print(f"  │    净收益: {open_actual:.2f} - {close_actual:.2f} - 4.05(fee) = {open_actual - close_actual - 4.05:.2f}bps │")
    print(f"  │                                                                              │")

    # P90 worst case
    open_p90 = 4.3 - p90_ask
    close_p90 = p90_bid
    print(f"  │  P90最坏情况:                                                                │")
    print(f"  │    开仓实际: 4.3 - {p90_ask:.2f} = {open_p90:.2f}bps                                    │")
    print(f"  │    平仓流失: {p90_bid:.2f}bps                                                    │")
    print(f"  │                                                                              │")

    # Recommended parameters
    open_compensated = 4.3 + p75_ask
    close_compensated = 0 - p75_bid
    print(f"  │  ★ 推荐补偿参数:                                                             │")
    print(f"  │    open_spread = 4.3 + {p75_ask:.2f}(P75补偿) = {open_compensated:.2f}bps                  │")
    print(f"  │    close_spread = 0 - {p75_bid:.2f}(P75补偿) = {close_compensated:.2f}bps                  │")
    print(f"  │    (即: 开仓多要{p75_ask:.2f}bps, 平仓多让{p75_bid:.2f}bps)                      │")
    print(f"  │                                                                              │")

    # More aggressive
    open_agg = 4.3 + p90_ask
    close_agg = 0 - p90_bid
    print(f"  │  ★★ 激进补偿参数 (P90):                                                      │")
    print(f"  │    open_spread = {open_agg:.2f}bps                                                │")
    print(f"  │    close_spread = {close_agg:.2f}bps                                              │")
    print(f"  └──────────────────────────────────────────────────────────────────────────────┘")


async def main():
    now = int(time.time())
    start_30d = now - 30 * 86400

    print("=" * 100)
    print("  Lighter 300ms BBO Drift 回测 + 实时测量")
    print("=" * 100)

    # Phase 1: Historical candle-based estimation
    print("\n[Phase 1] 30天1分钟K线 drift 估算...")
    candle_drifts = {}
    async with aiohttp.ClientSession() as session:
        for sym in ["BTC", "ETH", "SOL"]:
            mid = LIGHTER_MARKETS[sym]
            print(f"  获取 {sym} 1m candles...")
            candles = await fetch_lighter_candles_1m(session, mid, start_30d, now)
            print(f"  {sym}: {len(candles)} candles")
            if candles:
                candle_drifts[sym] = estimate_candle_drift(candles)

    # Phase 2: Real-time WS measurement (3 min each)
    print("\n[Phase 2] 实时300ms drift测量 (每币种3分钟)...")
    for sym in ["BTC", "ETH", "SOL"]:
        mid = LIGHTER_MARKETS[sym]
        bbo = await measure_realtime_drift(mid, sym, duration_s=180)
        ask_drifts, bid_drifts = compute_drift_from_bbo(bbo, delay_ms=300)
        analyze_drift(sym, ask_drifts, bid_drifts, candle_drifts.get(sym))


if __name__ == "__main__":
    asyncio.run(main())
