#!/usr/bin/env python3
import asyncio, json, websockets

async def test_ws():
    ws_url = "wss://mainnet.zklighter.elliot.ai/stream"
    async with websockets.connect(ws_url, ping_interval=20) as ws:
        subs = [
            {"type": "subscribe", "channel": "order_book", "market_id": 1},
            {"type": "subscribe", "channel": "orderbook", "market_id": 1},
        ]
        for s in subs:
            print(f"Sending: {json.dumps(s)}")
            await ws.send(json.dumps(s))

        for i in range(20):
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                data = json.loads(msg)
                keys = list(data.keys())
                ch = data.get("channel", data.get("type", "?"))
                mid = data.get("market_id", "?")
                print(f"  [{i}] keys={keys} ch={ch} mid={mid}")
                if "bids" in data or "asks" in data:
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    print(f"       bids={len(bids)} asks={len(asks)}")
                    if bids:
                        print(f"       top bid: {bids[0]}")
                    if asks:
                        print(f"       top ask: {asks[0]}")
                else:
                    txt = json.dumps(data)
                    if len(txt) < 500:
                        print(f"       raw: {txt}")
                    else:
                        print(f"       raw({len(txt)}): {txt[:300]}...")
            except asyncio.TimeoutError:
                print(f"  [{i}] timeout")

asyncio.run(test_ws())
