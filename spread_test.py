#!/usr/bin/env python3
"""Binance USDC-M vs Lighter: cross-exchange spread analysis."""
import asyncio, json, time, sys
import websockets
import ccxt

LIGHTER_WS = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_MID = {"BTC": 1, "ETH": 0, "SOL": 2, "HYPE": 24}
MID_TO_SYM = {v: k for k, v in LIGHTER_MID.items()}
BINANCE_PAIRS = {"BTC": "BTC/USDC:USDC", "ETH": "ETH/USDC:USDC", "SOL": "SOL/USDC:USDC"}
DURATION = 45

async def collect(symbols, duration=DURATION):
    ob_state = {}
    lighter_snaps = {s: [] for s in symbols}
    binance_snaps = {s: [] for s in symbols}

    b_ex = ccxt.binanceusdm({"options": {"fetchCurrencies": False}})
    b_ex.load_markets()

    async def b_poll():
        for _ in range(9):
            for sym in symbols:
                pair = BINANCE_PAIRS.get(sym)
                if not pair:
                    continue
                try:
                    ob = b_ex.fetch_order_book(pair, limit=20)
                    bids, asks = ob.get("bids", []), ob.get("asks", [])
                    if bids and asks:
                        binance_snaps[sym].append({
                            "ts": time.time(),
                            "bid": bids[0][0], "ask": asks[0][0],
                            "bv5": sum(x[1] for x in bids[:5]),
                            "av5": sum(x[1] for x in asks[:5]),
                            "bv10": sum(x[1] for x in bids[:10]),
                            "av10": sum(x[1] for x in asks[:10]),
                        })
                except:
                    pass
            await asyncio.sleep(5)

    async def l_collect():
        async with websockets.connect(LIGHTER_WS, ping_interval=10) as ws:
            for sym in symbols:
                mid = LIGHTER_MID.get(sym)
                if mid is not None:
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{mid}"}))

            t0 = time.time()
            while time.time() - t0 < duration:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                    data = json.loads(msg)
                    mt = data.get("type", "")

                    if mt == "ping":
                        await ws.send(json.dumps({"type": "pong"}))
                        continue

                    channel = data.get("channel", "")
                    sym = None
                    for s, mid in LIGHTER_MID.items():
                        if s in symbols and (f"order_book:{mid}" in channel or f"order_book/{mid}" in channel):
                            sym = s
                            break

                    if mt == "subscribed/order_book" and sym is None:
                        ob = data.get("order_book", {})
                        bids = ob.get("bids", [])
                        asks = ob.get("asks", [])
                        if bids:
                            bp = float(bids[0]["price"])
                            if bp > 50000:
                                sym = "BTC"
                            elif bp > 1000:
                                sym = "ETH"
                            elif bp > 60:
                                sym = "SOL"
                            else:
                                sym = "HYPE"

                    if not sym or sym not in symbols:
                        continue

                    if mt == "subscribed/order_book":
                        ob = data.get("order_book", {})
                        ob_state[sym] = {"bids": {}, "asks": {}}
                        for b in ob.get("bids", []):
                            ob_state[sym]["bids"][b["price"]] = float(b["size"])
                        for a in ob.get("asks", []):
                            ob_state[sym]["asks"][a["price"]] = float(a["size"])

                    elif mt == "update/order_book":
                        if sym not in ob_state:
                            continue
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

                    if sym in ob_state and ob_state[sym]["bids"] and ob_state[sym]["asks"]:
                        sb = sorted(ob_state[sym]["bids"].keys(), key=lambda x: float(x), reverse=True)
                        sa = sorted(ob_state[sym]["asks"].keys(), key=lambda x: float(x))
                        bb = float(sb[0])
                        ba = float(sa[0])
                        bv5 = sum(ob_state[sym]["bids"][p] for p in sb[:5])
                        av5 = sum(ob_state[sym]["asks"][p] for p in sa[:5])
                        bv10 = sum(ob_state[sym]["bids"][p] for p in sb[:10])
                        av10 = sum(ob_state[sym]["asks"][p] for p in sa[:10])
                        lighter_snaps[sym].append({
                            "ts": time.time(), "bid": bb, "ask": ba,
                            "bv5": bv5, "av5": av5, "bv10": bv10, "av10": av10,
                            "bl": len(sb), "al": len(sa),
                        })

                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    print(f"WS err: {e}")
                    break

    await asyncio.gather(l_collect(), b_poll())
    return lighter_snaps, binance_snaps

async def main():
    symbols = ["BTC", "ETH", "SOL", "HYPE"]
    SEP = "=" * 60
    LINE = "-" * 50

    print(SEP)
    print("  Binance USDC-M vs Lighter: Spread Analysis")
    print(f"  Time: {time.strftime('%Y-%m-%d %H:%M:%S')}  Duration: {DURATION}s")
    print(SEP)

    print(f"\nCollecting {DURATION}s of data...")
    l_snaps, b_snaps = await collect(symbols, DURATION)

    print(f"\n{SEP}")
    print("  DETAILED REPORT")
    print(SEP)

    results = {}

    for sym in symbols:
        print(f"\n{LINE}")
        print(f"  {sym}")
        print(LINE)

        ls = l_snaps.get(sym, [])
        bs = b_snaps.get(sym, [])

        has_binance = sym in BINANCE_PAIRS
        if not has_binance:
            print(f"  Binance USDC-M: NOT AVAILABLE")

        if not ls:
            print(f"  Lighter: NO DATA ({len(ls)} snapshots)")
            if bs:
                b0 = bs[0]
                sp = (b0["ask"] - b0["bid"]) / b0["bid"] * 10000
                print(f"  Binance: {b0['bid']}/{b0['ask']} spread={sp:.2f}bps")
            print("  VERDICT: Cannot arb")
            results[sym] = {"ok": False, "reason": "no Lighter" if has_binance else "no Binance"}
            continue

        if not bs:
            print(f"  Lighter: {len(ls)} snapshots, but no Binance data")
            results[sym] = {"ok": False, "reason": "no Binance"}
            continue

        n = len(ls)
        l_sps = [(s["ask"] - s["bid"]) / s["bid"] * 10000 for s in ls]

        cross_sell = []
        cross_buy = []
        for lsnap in ls:
            closest = min(bs, key=lambda b: abs(b["ts"] - lsnap["ts"]))
            cs = (closest["ask"] - lsnap["ask"]) / lsnap["ask"] * 10000
            cb = (lsnap["bid"] - closest["bid"]) / closest["bid"] * 10000
            cross_sell.append(cs)
            cross_buy.append(cb)

        b0 = bs[len(bs)//2]
        b_sp = (b0["ask"] - b0["bid"]) / b0["bid"] * 10000
        mid = (b0["bid"] + b0["ask"]) / 2

        avg_l_sp = sum(l_sps) / n
        avg_cs = sum(cross_sell) / n
        avg_cb = sum(cross_buy) / n

        gt3s = sum(1 for x in cross_sell if x >= 3.0)
        gt5s = sum(1 for x in cross_sell if x >= 5.0)
        gt8s = sum(1 for x in cross_sell if x >= 8.0)
        gt3b = sum(1 for x in cross_buy if x >= 3.0)
        gt5b = sum(1 for x in cross_buy if x >= 5.0)
        gt8b = sum(1 for x in cross_buy if x >= 8.0)
        conv_s = sum(1 for x in cross_sell if abs(x) < 1.0)
        conv_b = sum(1 for x in cross_buy if abs(x) < 1.0)

        print(f"\n  Binance USDC-M: ({len(bs)} snapshots)")
        print(f"    BBO: {b0['bid']}/{b0['ask']}  spread={b_sp:.2f}bps")
        print(f"    Top5 vol: {b0['bv5']:.4f} / {b0['av5']:.4f}")
        print(f"    Top10 vol: {b0['bv10']:.4f} / {b0['av10']:.4f}")

        print(f"\n  Lighter: ({n} snapshots)")
        print(f"    spread: avg={avg_l_sp:.2f}  min={min(l_sps):.2f}  max={max(l_sps):.2f} bps")
        print(f"    Top5 vol: bid={sum(s['bv5'] for s in ls)/n:.4f}  ask={sum(s['av5'] for s in ls)/n:.4f}")
        print(f"    Top10 vol: bid={sum(s['bv10'] for s in ls)/n:.4f}  ask={sum(s['av10'] for s in ls)/n:.4f}")
        print(f"    depth: {sum(s['bl'] for s in ls)/n:.0f}b / {sum(s['al'] for s in ls)/n:.0f}a")

        print(f"\n  Cross spread (B_sell: B_ask - L_ask):")
        print(f"    avg={avg_cs:+.2f}  max={max(cross_sell):+.2f}  min={min(cross_sell):+.2f} bps")
        print(f"    >=3bps: {gt3s}/{n} ({gt3s/n*100:.1f}%)  >=5bps: {gt5s}/{n} ({gt5s/n*100:.1f}%)  >=8bps: {gt8s}/{n} ({gt8s/n*100:.1f}%)")

        print(f"\n  Cross spread (B_buy: L_bid - B_bid):")
        print(f"    avg={avg_cb:+.2f}  max={max(cross_buy):+.2f}  min={min(cross_buy):+.2f} bps")
        print(f"    >=3bps: {gt3b}/{n} ({gt3b/n*100:.1f}%)  >=5bps: {gt5b}/{n} ({gt5b/n*100:.1f}%)  >=8bps: {gt8b}/{n} ({gt8b/n*100:.1f}%)")

        print(f"\n  Convergence (<1bps): sell={conv_s}/{n} ({conv_s/n*100:.1f}%)  buy={conv_b}/{n} ({conv_b/n*100:.1f}%)")
        print(f"  Ref price: ${mid:.2f}")

        min_sizes = {"BTC": 0.005, "ETH": 0.05, "SOL": 1.0, "HYPE": 5.0}
        ms = min_sizes.get(sym, 0.01)
        dpt = mid * ms
        print(f"  Trade size: {ms} {sym} = ${dpt:.2f}")
        print(f"  Est profit: @3bps=${dpt*3/10000:.4f}  @5bps=${dpt*5/10000:.4f}")

        arb_freq = (gt5s + gt5b) / 2
        conv_freq = (conv_s + conv_b) / 2
        l_vol = sum(s["bv5"] + s["av5"] for s in ls) / n

        results[sym] = {
            "ok": True,
            "l_spread": avg_l_sp,
            "cs_avg": avg_cs, "cs_max": max(cross_sell),
            "cb_avg": avg_cb, "cb_max": max(cross_buy),
            "gt5_sell": gt5s, "gt5_buy": gt5b,
            "gt5_pct": (gt5s + gt5b) / (2 * n) * 100,
            "conv_sell": conv_s, "conv_buy": conv_b,
            "conv_pct": (conv_s + conv_b) / (2 * n) * 100,
            "l_vol5": l_vol,
            "b_vol5": b0["bv5"] + b0["av5"],
            "price": mid, "n": n,
            "arb_per_min": arb_freq / (DURATION / 60),
            "conv_per_min": conv_freq / (DURATION / 60),
        }

    print(f"\n{SEP}")
    print("  SUMMARY & RANKING")
    print(SEP)

    viable = [(s, r) for s, r in results.items() if r.get("ok")]
    if not viable:
        print("\n  No viable arb pairs found!")
        return

    viable.sort(key=lambda x: -x[1]["gt5_pct"])

    print(f"\n  {'Sym':<6} {'L_spread':<10} {'Xsell_avg':<11} {'Xbuy_avg':<11} {'>=5bps%':<9} {'Conv%':<9} {'L_vol5':<9} {'Arb/min':<9}")
    for sym, r in viable:
        print(f"  {sym:<6} {r['l_spread']:.2f}bps   {r['cs_avg']:+.2f}bps   {r['cb_avg']:+.2f}bps   "
              f"{r['gt5_pct']:.1f}%     {r['conv_pct']:.1f}%     {r['l_vol5']:.2f}     {r['arb_per_min']:.1f}")

    best = viable[0]
    print(f"\n  RECOMMENDATION: {best[0]}")
    print(f"    - Highest >=5bps opportunity rate: {best[1]['gt5_pct']:.1f}%")
    print(f"    - Convergence rate: {best[1]['conv_pct']:.1f}%")
    print(f"    - ~{best[1]['arb_per_min']:.1f} arb opportunities per minute")

asyncio.run(main())
