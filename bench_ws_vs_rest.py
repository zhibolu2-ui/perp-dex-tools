#!/usr/bin/env python3
"""
Benchmark: Hotstuff REST vs WebSocket order placement latency.

Tests:
  1. REST (aiohttp keep-alive) placeOrder round-trip
  2. WebSocket placeOrder round-trip (same payload, WS transport)
  3. WebSocket fills subscription notification latency

Usage:
  cd /root/perp-dex-tools && source venv/bin/activate && python3 bench_ws_vs_rest.py
"""

import asyncio
import json
import os
import time
from dataclasses import asdict
from decimal import Decimal, ROUND_HALF_UP

import aiohttp
import websockets
from dotenv import load_dotenv
from eth_account import Account

from hotstuff import ExchangeClient, InfoClient
from hotstuff.methods.exchange.trading import UnitOrder
from hotstuff.methods.exchange.op_codes import EXCHANGE_OP_CODES

load_dotenv()

API_URL = "https://api.hotstuff.trade/"
WS_URL = "wss://api.hotstuff.trade/ws"
PK = os.getenv("HOTSTUFF_PRIVATE_KEY")
ADDRESS = None
INSTRUMENT_ID = 1
TICK = Decimal("1")
LOT = Decimal("0.001")
ROUNDS = 3


def init_sdk():
    global ADDRESS
    wallet = Account.from_key(PK)
    ADDRESS = os.getenv("HOTSTUFF_ADDRESS", wallet.address)
    exchange = ExchangeClient(wallet=wallet)
    info = InfoClient()
    return exchange, info


def sdk_prepare_order(exchange, side: str, price: Decimal, qty: Decimal) -> dict:
    hs_side = "b" if side == "buy" else "s"
    order = UnitOrder(
        instrumentId=INSTRUMENT_ID,
        side=hs_side, positionSide="BOTH",
        price=str(price), size=str(qty),
        tif="IOC", ro=False, po=False, isMarket=False,
    )
    params = {
        "orders": [asdict(order)],
        "expiresAfter": int(time.time() * 1000) + 60_000,
    }
    prepared = exchange._execute_action(
        {"action": "placeOrder", "params": params},
        execute=False,
    )
    return {
        "action": {"data": prepared["params"],
                   "type": str(EXCHANGE_OP_CODES["placeOrder"])},
        "signature": prepared["signature"],
        "nonce": prepared["params"]["nonce"],
    }


async def get_bbo(session) -> dict:
    async with session.post(
        API_URL + "info",
        json={"method": "bbo", "params": {"symbol": "BTC-PERP"}},
    ) as resp:
        data = await resp.json()
        if isinstance(data, list) and data:
            return data[0]
        return data.get("data", data) if isinstance(data, dict) else data


async def bench_rest(exchange, session, bbo):
    """REST (aiohttp keep-alive) placeOrder round-trip."""
    ask = Decimal(str(bbo["best_ask_price"]))
    slip = Decimal("0.002")
    limit_price = (ask * (1 + slip)).quantize(TICK, rounding=ROUND_HALF_UP)

    results = []
    for i in range(ROUNDS):
        payload = sdk_prepare_order(exchange, "buy", limit_price, LOT)
        t0 = time.time()
        async with session.post(API_URL + "exchange", json=payload) as resp:
            body = await resp.json()
        ms = (time.time() - t0) * 1000
        ok = not (isinstance(body, dict) and body.get("type") == "error")
        results.append(ms)
        print(f"  REST #{i+1}: {ms:.0f}ms  ok={ok}")
        await asyncio.sleep(0.3)

    avg = sum(results) / len(results)
    print(f"  REST avg: {avg:.0f}ms\n")
    return avg


async def bench_ws_order(exchange, ws, bbo):
    """WebSocket placeOrder round-trip (JSON-RPC post action)."""
    ask = Decimal(str(bbo["best_ask_price"]))
    slip = Decimal("0.002")
    limit_price = (ask * (1 + slip)).quantize(TICK, rounding=ROUND_HALF_UP)

    results = []
    for i in range(ROUNDS):
        payload = sdk_prepare_order(exchange, "buy", limit_price, LOT)
        rpc_msg = {
            "jsonrpc": "2.0",
            "id": 100 + i,
            "method": "post",
            "params": {
                "type": "action",
                "payload": payload,
            },
        }

        t0 = time.time()
        await ws.send(json.dumps(rpc_msg))

        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
            msg = json.loads(raw)
            if msg.get("id") == 100 + i:
                break

        ms = (time.time() - t0) * 1000
        has_error = "error" in msg
        results.append(ms)
        print(f"  WS order #{i+1}: {ms:.0f}ms  error={has_error}")
        if has_error:
            print(f"    detail: {msg.get('error', {}).get('message', msg)}")
        await asyncio.sleep(0.3)

    avg = sum(results) / len(results)
    print(f"  WS order avg: {avg:.0f}ms\n")
    return avg


async def bench_ws_fill_notification(exchange, ws, bbo):
    """Send order via WS, measure time until fills push notification."""
    ask = Decimal(str(bbo["best_ask_price"]))
    slip = Decimal("0.002")
    limit_price = (ask * (1 + slip)).quantize(TICK, rounding=ROUND_HALF_UP)

    await asyncio.sleep(0.5)

    sub_msg = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "subscribe",
        "params": {"channel": "fills", "user": ADDRESS},
    }
    await ws.send(json.dumps(sub_msg))
    raw = await asyncio.wait_for(ws.recv(), timeout=5)
    print(f"  Fills sub response: {raw[:200]}")

    await asyncio.sleep(0.2)

    sub_orders = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "subscribe",
        "params": {"channel": "orders", "user": ADDRESS},
    }
    await ws.send(json.dumps(sub_orders))
    raw = await asyncio.wait_for(ws.recv(), timeout=5)
    print(f"  Orders sub response: {raw[:200]}")

    await asyncio.sleep(0.2)

    results = []
    for i in range(ROUNDS):
        payload = sdk_prepare_order(exchange, "buy", limit_price, LOT)
        rpc_msg = {
            "jsonrpc": "2.0",
            "id": 200 + i,
            "method": "post",
            "params": {"type": "action", "payload": payload},
        }

        t0 = time.time()
        await ws.send(json.dumps(rpc_msg))

        fill_ms = None
        order_ms = None
        rpc_ms = None
        deadline = time.time() + 10

        while time.time() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                break
            msg = json.loads(raw)
            now_ms = (time.time() - t0) * 1000

            if msg.get("id") == 200 + i:
                rpc_ms = now_ms
                print(f"    RPC response: {rpc_ms:.0f}ms")

            if msg.get("method") == "event":
                ch = msg.get("params", {}).get("channel", "")
                if "fills@" in ch and fill_ms is None:
                    fill_ms = now_ms
                    data = msg["params"]["data"]
                    print(f"    Fill push: {fill_ms:.0f}ms  "
                          f"price={data.get('price')} size={data.get('size')} "
                          f"fee={data.get('fee')}")
                elif "orders@" in ch and order_ms is None:
                    order_ms = now_ms
                    state = msg["params"]["data"].get("state", "?")
                    print(f"    Order push: {order_ms:.0f}ms  state={state}")

            if fill_ms and order_ms and rpc_ms:
                break

        best = fill_ms or rpc_ms or 9999
        results.append(best)
        print(f"  Round #{i+1}: rpc={rpc_ms:.0f}ms  "
              f"order_push={order_ms and f'{order_ms:.0f}ms' or 'N/A'}  "
              f"fill_push={fill_ms and f'{fill_ms:.0f}ms' or 'N/A'}")
        print()

        await asyncio.sleep(0.5)

    avg = sum(results) / len(results)
    print(f"  Fill notification avg: {avg:.0f}ms\n")
    return avg


async def main():
    print("=" * 60)
    print("  Hotstuff REST vs WebSocket Latency Benchmark")
    print("=" * 60)

    exchange, info = init_sdk()
    print(f"Address: {ADDRESS}")
    print(f"WS URL: {WS_URL}")
    print(f"Rounds: {ROUNDS}\n")

    session = aiohttp.ClientSession(
        headers={"Content-Type": "application/json"},
        timeout=aiohttp.ClientTimeout(total=10),
    )

    try:
        bbo = await get_bbo(session)
        print(f"BBO: bid={bbo.get('best_bid_price')} ask={bbo.get('best_ask_price')}\n")

        print("── Test 1: REST (aiohttp keep-alive) ──")
        rest_avg = await bench_rest(exchange, session, bbo)

        print("── Test 2: WS order round-trip ──")
        ws = await websockets.connect(WS_URL, ping_interval=20)
        try:
            ws_avg = await bench_ws_order(exchange, ws, bbo)
        finally:
            await ws.close()

        print("── Test 3: WS order + fill/order notifications ──")
        ws2 = await websockets.connect(WS_URL, ping_interval=20)
        try:
            fill_avg = await bench_ws_fill_notification(exchange, ws2, bbo)
        finally:
            await ws2.close()

        print("=" * 60)
        print(f"  REST avg:          {rest_avg:.0f}ms")
        print(f"  WS order avg:      {ws_avg:.0f}ms")
        print(f"  WS fill push avg:  {fill_avg:.0f}ms")
        diff = rest_avg - ws_avg
        print(f"  WS improvement:    {diff:.0f}ms ({diff/rest_avg*100:.0f}%)")
        print("=" * 60)

    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())
