#!/usr/bin/env python3
import asyncio, json, websockets

async def debug():
    ws_url = "wss://mainnet.zklighter.elliot.ai/stream"
    async with websockets.connect(ws_url, ping_interval=10) as ws:
        for mid in [0, 1, 2]:
            await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{mid}"}))

        count = 0
        for i in range(30):
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                data = json.loads(msg)
                mt = data.get("type", "")
                if mt == "ping":
                    await ws.send(json.dumps({"type": "pong"}))
                    continue
                ch = data.get("channel", "NO_CHANNEL")
                if mt == "update/order_book" and count < 10:
                    ob = data.get("order_book", {})
                    bids = ob.get("bids", [])
                    asks = ob.get("asks", [])
                    print(f"type={mt} channel='{ch}' bids={len(bids)} asks={len(asks)}")
                    if bids:
                        print(f"  bid sample: {bids[0]}")
                    count += 1
                elif mt == "subscribed/order_book":
                    ob = data.get("order_book", {})
                    bids = ob.get("bids", [])
                    asks = ob.get("asks", [])
                    bp = float(bids[0]["price"]) if bids else 0
                    print(f"type={mt} channel='{ch}' bids={len(bids)} asks={len(asks)} top_bid={bp}")
            except asyncio.TimeoutError:
                continue

asyncio.run(debug())
