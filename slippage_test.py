#!/usr/bin/env python3
"""
Lighter IOC slippage test: measure how much the orderbook moves in ~300ms.
Captures 10000+ snapshots, measures BBO drift between consecutive updates.
Tests BTC (market 1) and ETH (market 0).
"""
import asyncio, json, time, statistics
import websockets

LIGHTER_WS = "wss://mainnet.zklighter.elliot.ai/stream"
MARKETS = {"ETH": 0, "BTC": 1}

async def collect_snapshots(symbol, market_id, target_count=5000):
    """Collect orderbook BBO snapshots and measure drift."""
    snapshots = []
    
    async with websockets.connect(LIGHTER_WS, ping_interval=10) as ws:
        await ws.send(json.dumps({
            "type": "subscribe",
            "channel": f"order_book/{market_id}",
        }))

        ob_bids = {}
        ob_asks = {}
        initialized = False

        while len(snapshots) < target_count:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                data = json.loads(msg)
                mt = data.get("type", "")

                if mt == "ping":
                    await ws.send(json.dumps({"type": "pong"}))
                    continue

                if mt == "subscribed/order_book":
                    ob = data.get("order_book", {})
                    ob_bids.clear()
                    ob_asks.clear()
                    for b in ob.get("bids", []):
                        ob_bids[b["price"]] = float(b["size"])
                    for a in ob.get("asks", []):
                        ob_asks[a["price"]] = float(a["size"])
                    initialized = True

                elif mt == "update/order_book" and initialized:
                    channel = data.get("channel", "")
                    if f"order_book:{market_id}" not in channel:
                        continue
                    ob = data.get("order_book", {})
                    for b in ob.get("bids", []):
                        sz = float(b["size"])
                        if sz == 0:
                            ob_bids.pop(b["price"], None)
                        else:
                            ob_bids[b["price"]] = sz
                    for a in ob.get("asks", []):
                        sz = float(a["size"])
                        if sz == 0:
                            ob_asks.pop(a["price"], None)
                        else:
                            ob_asks[a["price"]] = sz

                if initialized and ob_bids and ob_asks:
                    sorted_bids = sorted(ob_bids.keys(), key=lambda x: float(x), reverse=True)
                    sorted_asks = sorted(ob_asks.keys(), key=lambda x: float(x))
                    bb = float(sorted_bids[0])
                    ba = float(sorted_asks[0])

                    bv5 = sum(ob_bids[p] for p in sorted_bids[:5])
                    av5 = sum(ob_asks[p] for p in sorted_asks[:5])

                    snapshots.append({
                        "ts": time.time(),
                        "bid": bb,
                        "ask": ba,
                        "mid": (bb + ba) / 2,
                        "bv5": bv5,
                        "av5": av5,
                    })

                    if len(snapshots) % 2000 == 0:
                        print(f"  {symbol}: {len(snapshots)}/{target_count} snapshots...")

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"  WS error: {e}")
                break

    return snapshots

def analyze_drift(snapshots, symbol, windows_ms=[100, 200, 300, 500, 1000]):
    """Analyze BBO drift over different time windows."""
    n = len(snapshots)
    if n < 100:
        print(f"  {symbol}: insufficient data ({n} snapshots)")
        return

    print(f"\n{'=' * 60}")
    print(f"  {symbol} Lighter Orderbook Drift Analysis")
    print(f"  Total snapshots: {n}")
    duration = snapshots[-1]["ts"] - snapshots[0]["ts"]
    print(f"  Duration: {duration:.1f}s  Rate: {n/duration:.1f} updates/sec")
    print(f"{'=' * 60}")

    ref_price = snapshots[0]["mid"]

    for window_ms in windows_ms:
        window_sec = window_ms / 1000.0

        ask_drifts_bps = []
        bid_drifts_bps = []
        mid_drifts_bps = []
        abs_ask_drifts = []
        abs_bid_drifts = []

        for i in range(n):
            t0 = snapshots[i]["ts"]
            target_t = t0 + window_sec

            j = i + 1
            while j < n and snapshots[j]["ts"] < target_t:
                j += 1
            if j >= n:
                break

            s0 = snapshots[i]
            s1 = snapshots[j]
            dt = s1["ts"] - s0["ts"]

            ask_drift = (s1["ask"] - s0["ask"]) / s0["ask"] * 10000
            bid_drift = (s1["bid"] - s0["bid"]) / s0["bid"] * 10000
            mid_drift = (s1["mid"] - s0["mid"]) / s0["mid"] * 10000

            ask_drifts_bps.append(ask_drift)
            bid_drifts_bps.append(bid_drift)
            mid_drifts_bps.append(mid_drift)
            abs_ask_drifts.append(abs(ask_drift))
            abs_bid_drifts.append(abs(bid_drift))

        if not ask_drifts_bps:
            print(f"\n  Window {window_ms}ms: no data")
            continue

        cnt = len(ask_drifts_bps)

        print(f"\n  --- {window_ms}ms window ({cnt} pairs) ---")
        print(f"  Ask drift (IOC buy slippage):")
        print(f"    avg:    {statistics.mean(ask_drifts_bps):+.3f} bps")
        print(f"    |avg|:  {statistics.mean(abs_ask_drifts):.3f} bps")
        print(f"    median: {statistics.median(ask_drifts_bps):+.3f} bps")
        print(f"    stdev:  {statistics.stdev(ask_drifts_bps):.3f} bps")
        print(f"    P75:    {sorted(abs_ask_drifts)[int(cnt*0.75)]:.3f} bps")
        print(f"    P90:    {sorted(abs_ask_drifts)[int(cnt*0.90)]:.3f} bps")
        print(f"    P95:    {sorted(abs_ask_drifts)[int(cnt*0.95)]:.3f} bps")
        print(f"    P99:    {sorted(abs_ask_drifts)[int(cnt*0.99)]:.3f} bps")
        print(f"    max:    {max(abs_ask_drifts):.3f} bps")

        gt1 = sum(1 for x in abs_ask_drifts if x >= 1.0)
        gt2 = sum(1 for x in abs_ask_drifts if x >= 2.0)
        gt3 = sum(1 for x in abs_ask_drifts if x >= 3.0)
        gt5 = sum(1 for x in abs_ask_drifts if x >= 5.0)
        print(f"    >=1bps: {gt1}/{cnt} ({gt1/cnt*100:.1f}%)")
        print(f"    >=2bps: {gt2}/{cnt} ({gt2/cnt*100:.1f}%)")
        print(f"    >=3bps: {gt3}/{cnt} ({gt3/cnt*100:.1f}%)")
        print(f"    >=5bps: {gt5}/{cnt} ({gt5/cnt*100:.1f}%)")

        print(f"\n  Bid drift (IOC sell slippage):")
        print(f"    avg:    {statistics.mean(bid_drifts_bps):+.3f} bps")
        print(f"    |avg|:  {statistics.mean(abs_bid_drifts):.3f} bps")
        print(f"    P90:    {sorted(abs_bid_drifts)[int(cnt*0.90)]:.3f} bps")
        print(f"    P95:    {sorted(abs_bid_drifts)[int(cnt*0.95)]:.3f} bps")
        print(f"    P99:    {sorted(abs_bid_drifts)[int(cnt*0.99)]:.3f} bps")

    # Spread analysis
    spreads = [(s["ask"] - s["bid"]) / s["bid"] * 10000 for s in snapshots]
    print(f"\n  --- Lighter internal spread ---")
    print(f"    avg:    {statistics.mean(spreads):.3f} bps")
    print(f"    median: {statistics.median(spreads):.3f} bps")
    print(f"    min:    {min(spreads):.3f} bps")
    print(f"    max:    {max(spreads):.3f} bps")

    return {
        "symbol": symbol,
        "n": n,
        "duration": duration,
        "ref_price": ref_price,
    }

async def collect_both(target=5000):
    """Collect ETH and BTC simultaneously from same WS connection."""
    snapshots = {"ETH": [], "BTC": []}
    ob_state = {}

    async with websockets.connect(LIGHTER_WS, ping_interval=10) as ws:
        for sym, mid in MARKETS.items():
            await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{mid}"}))
            ob_state[sym] = {"bids": {}, "asks": {}, "init": False}

        done = set()
        while len(done) < 2:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                data = json.loads(msg)
                mt = data.get("type", "")

                if mt == "ping":
                    await ws.send(json.dumps({"type": "pong"}))
                    continue

                channel = data.get("channel", "")
                sym = None
                for s, mid in MARKETS.items():
                    if f"order_book:{mid}" in channel or (mt == "subscribed/order_book" and not ob_state[s]["init"]):
                        if mt == "subscribed/order_book":
                            ob = data.get("order_book", {})
                            bids = ob.get("bids", [])
                            if bids:
                                bp = float(bids[0]["price"])
                                if bp > 1000:
                                    if s == "ETH":
                                        sym = s
                                elif bp > 50000:
                                    if s == "BTC":
                                        sym = s
                                # fallback
                                if sym is None and not ob_state[s]["init"]:
                                    if (s == "ETH" and 500 < bp < 10000) or (s == "BTC" and bp > 50000):
                                        sym = s
                        elif f"order_book:{mid}" in channel:
                            sym = s
                        if sym:
                            break

                if not sym:
                    for s, mid in MARKETS.items():
                        if f"order_book:{mid}" in channel:
                            sym = s
                            break

                if not sym:
                    continue

                if mt == "subscribed/order_book":
                    ob = data.get("order_book", {})
                    ob_state[sym]["bids"].clear()
                    ob_state[sym]["asks"].clear()
                    for b in ob.get("bids", []):
                        ob_state[sym]["bids"][b["price"]] = float(b["size"])
                    for a in ob.get("asks", []):
                        ob_state[sym]["asks"][a["price"]] = float(a["size"])
                    ob_state[sym]["init"] = True

                elif mt == "update/order_book" and ob_state.get(sym, {}).get("init"):
                    ob = data.get("order_book", {})
                    for b in ob.get("bids", []):
                        sz = float(b["size"])
                        if sz == 0:
                            ob_state[sym]["bids"].pop(b["price"], None)
                        else:
                            ob_state[sym]["bids"][b["price"]] = sz
                    for a in ob.get("asks", []):
                        sz = float(a["size"])
                        if sz == 0:
                            ob_state[sym]["asks"].pop(a["price"], None)
                        else:
                            ob_state[sym]["asks"][a["price"]] = sz

                st = ob_state.get(sym)
                if st and st["init"] and st["bids"] and st["asks"] and sym not in done:
                    sb = sorted(st["bids"].keys(), key=lambda x: float(x), reverse=True)
                    sa = sorted(st["asks"].keys(), key=lambda x: float(x))
                    bb = float(sb[0])
                    ba = float(sa[0])
                    snapshots[sym].append({
                        "ts": time.time(), "bid": bb, "ask": ba,
                        "mid": (bb + ba) / 2,
                        "bv5": sum(st["bids"][p] for p in sb[:5]),
                        "av5": sum(st["asks"][p] for p in sa[:5]),
                    })
                    c = len(snapshots[sym])
                    if c % 1000 == 0:
                        print(f"  {sym}: {c}/{target}...")
                    if c >= target:
                        done.add(sym)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"  WS error: {e}")
                break

    return snapshots

async def main():
    TARGET = 5000
    print("=" * 60)
    print(f"  Lighter IOC Slippage Test ({TARGET} snapshots per coin)")
    print(f"  Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    print("\n>>> Collecting ETH + BTC simultaneously...")
    all_snaps = await collect_both(target=TARGET)

    for symbol in ["ETH", "BTC"]:
        snaps = all_snaps.get(symbol, [])
        print(f"\n  {symbol}: collected {len(snaps)} snapshots")
        if snaps:
            analyze_drift(snaps, symbol, windows_ms=[50, 100, 200, 300, 500, 1000])

    print(f"\n{'=' * 60}")
    print("  CONCLUSION FOR STRATEGY")
    print("=" * 60)
    print("""
  The 300ms window drift = expected slippage for Lighter IOC.
  
  For open-spread threshold:
    open_spread >= expected_net_profit + 300ms_drift_P90
    
  For close-spread:
    close_spread should account for the same drift.
    If 300ms drift P90 = X bps, then:
    - You lose ~X bps on open (Lighter IOC buy after B sell)
    - You lose ~X bps on close (Lighter IOC sell after B buy)
    - Total round-trip slippage = ~2X bps
""")

asyncio.run(main())
