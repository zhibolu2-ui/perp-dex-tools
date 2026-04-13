#!/usr/bin/env python3
"""快速调试: 确认两个交易所WS数据正确接收, 实时观察价差"""
import asyncio
import json
import time
import websockets

LIGHTER_WS = "wss://mainnet.zklighter.elliot.ai/stream"
EXT_WS = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1/orderbooks/BTC-USD?depth=1"

l_bid = l_ask = x_bid = x_ask = None
x_bid_qty = x_ask_qty = 0.0

l_ob_bids = {}
l_ob_asks = {}

async def lighter():
    global l_bid, l_ask
    async with websockets.connect(LIGHTER_WS, ping_interval=20) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": "order_book/1"}))
        t0 = time.time()
        while time.time() - t0 < 25:
            raw = await asyncio.wait_for(ws.recv(), timeout=5)
            msg = json.loads(raw)
            mt = msg.get("type", "")
            data = msg.get("data", {})
            if mt == "pong":
                continue
            ob = None
            if mt == "subscribed/order_book":
                l_ob_bids.clear()
                l_ob_asks.clear()
                ob = data.get("order_book", {})
            elif mt == "update/order_book":
                ob = data.get("order_book", data)
            if ob:
                for lv in ob.get("bids", []):
                    p, q = float(lv["price"]), float(lv["size"])
                    if q <= 0:
                        l_ob_bids.pop(p, None)
                    else:
                        l_ob_bids[p] = q
                for lv in ob.get("asks", []):
                    p, q = float(lv["price"]), float(lv["size"])
                    if q <= 0:
                        l_ob_asks.pop(p, None)
                    else:
                        l_ob_asks[p] = q
                if l_ob_bids:
                    l_bid = max(l_ob_bids.keys())
                if l_ob_asks:
                    l_ask = min(l_ob_asks.keys())

async def extended():
    global x_bid, x_ask, x_bid_qty, x_ask_qty
    async with websockets.connect(EXT_WS, ping_interval=20) as ws:
        t0 = time.time()
        while time.time() - t0 < 25:
            raw = await asyncio.wait_for(ws.recv(), timeout=5)
            msg = json.loads(raw)
            data = msg.get("data", {})
            b = data.get("b", [])
            a = data.get("a", [])
            if b and a:
                x_bid = float(b[0]["p"])
                x_ask = float(a[0]["p"])
                x_bid_qty = float(b[0].get("c", b[0].get("q", "0")))
                x_ask_qty = float(a[0].get("c", a[0].get("q", "0")))

async def monitor():
    await asyncio.sleep(3)
    t0 = time.time()
    spreads = []
    while time.time() - t0 < 20:
        await asyncio.sleep(0.5)
        if l_bid and x_bid:
            l_mid = (l_bid + l_ask) / 2
            x_mid = (x_bid + x_ask) / 2
            basis = (l_mid - x_mid) / x_mid * 10000
            sell_sp = (l_bid - x_ask) / x_ask * 10000
            buy_sp = (x_bid - l_ask) / l_ask * 10000
            spreads.append(abs(basis))
            print("  L=%.1f/%.1f  X=%.0f/%.0f  basis=%+.2fbps  "
                  "sell_sp=%+.2f  buy_sp=%+.2f  xBidQ=%.3f" %
                  (l_bid, l_ask, x_bid, x_ask, basis,
                   sell_sp, buy_sp, x_bid_qty))
        else:
            print("  等待数据... L_bid=%s X_bid=%s" % (l_bid, x_bid))

    if spreads:
        import statistics
        print("\n--- 价差统计 (20秒) ---")
        print("  |basis| 均值=%.2f  最大=%.2f  >=3bps: %d次  >=5bps: %d次" %
              (statistics.mean(spreads), max(spreads),
               sum(1 for s in spreads if s >= 3),
               sum(1 for s in spreads if s >= 5)))

async def main():
    await asyncio.gather(lighter(), extended(), monitor(), return_exceptions=True)

asyncio.run(main())
