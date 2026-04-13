#!/usr/bin/env python3
import asyncio, json, websockets

async def debug():
    ws_url = "wss://mainnet.zklighter.elliot.ai/stream"
    async with websockets.connect(ws_url, ping_interval=10) as ws:
        for mid in [0, 1, 2, 24]:
            sub = json.dumps({"type": "subscribe", "channel": f"order_book/{mid}"})
            print(f"Sub: {sub}")
            await ws.send(sub)

        seen_types = set()
        for i in range(80):
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                data = json.loads(msg)
                mt = data.get("type", "")
                if mt == "ping":
                    await ws.send(json.dumps({"type": "pong"}))
                    continue

                if mt.startswith("subscribed"):
                    ob = data.get("order_book", {})
                    bids = ob.get("bids", [])
                    asks = ob.get("asks", [])
                    bb = bids[0]["price"] if bids else "N/A"
                    ba = asks[0]["price"] if asks else "N/A"
                    print(f"  type={mt}  bids={len(bids)} asks={len(asks)}  BBO={bb}/{ba}")

                elif mt.startswith("channel_data"):
                    if mt not in seen_types:
                        bids = data.get("bids", [])
                        asks = data.get("asks", [])
                        print(f"  type={mt}  bids_upd={len(bids)} asks_upd={len(asks)}")
                        seen_types.add(mt)

                elif "error" in data:
                    print(f"  ERROR: {data}")

                else:
                    print(f"  type={mt} keys={list(data.keys())[:8]}")

            except asyncio.TimeoutError:
                continue

asyncio.run(debug())
