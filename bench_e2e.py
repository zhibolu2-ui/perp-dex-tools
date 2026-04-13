#!/usr/bin/env python3
"""End-to-end latency benchmark: aiohttp vs SDK for Hotstuff exchange calls."""
import asyncio, time, os
from dotenv import load_dotenv; load_dotenv()
from eth_account import Account
from decimal import Decimal
from dataclasses import asdict
import aiohttp

from hotstuff.utils.signing import sign_action
from hotstuff.methods.exchange.op_codes import EXCHANGE_OP_CODES
from hotstuff.utils.nonce import NonceManager
from hotstuff import ExchangeClient
from hotstuff.methods.exchange.trading import PlaceOrderParams, UnitOrder, CancelAllParams

pk = os.getenv("HOTSTUFF_PRIVATE_KEY")
wallet = Account.from_key(pk)
nm = NonceManager()
api = "https://api.hotstuff.trade/"

async def bench_aiohttp_exchange():
    """Benchmark aiohttp direct exchange calls."""
    async with aiohttp.ClientSession(
        headers={"Content-Type": "application/json"},
        timeout=aiohttp.ClientTimeout(total=5)
    ) as sess:
        # warmup TLS
        async with sess.post(api + "info", json={"method": "positions", "params": {"user": wallet.address}}) as r:
            await r.json()

        # cancelAll via aiohttp (safe no-op)
        times = []
        for _ in range(10):
            nonce = nm.get_nonce()
            d = {"expiresAfter": int(time.time()*1000)+60000, "nonce": nonce}
            t0 = time.perf_counter()
            sig = sign_action(wallet=wallet, action=d, tx_type=EXCHANGE_OP_CODES["cancelAll"])
            pl = {"action": {"data": d, "type": str(EXCHANGE_OP_CODES["cancelAll"])},
                  "signature": sig, "nonce": nonce}
            async with sess.post(api + "exchange", json=pl) as r:
                body = await r.json()
            ms = (time.perf_counter() - t0) * 1000
            times.append(ms)
        print(f"[aiohttp exchange/cancelAll] avg={sum(times)/len(times):.1f}ms  min={min(times):.1f}ms  max={max(times):.1f}ms")

        # placeOrder IOC that will likely fail (price way off) — tests exchange latency
        times2 = []
        for _ in range(5):
            nonce = nm.get_nonce()
            d = {
                "orders": [{
                    "instrumentId": 1, "side": "b", "positionSide": "BOTH",
                    "price": "1000", "size": "0.001",  # price way below market
                    "tif": "IOC", "ro": False, "po": False, "isMarket": False,
                }],
                "expiresAfter": int(time.time()*1000)+60000,
                "nonce": nonce,
            }
            t0 = time.perf_counter()
            sig = sign_action(wallet=wallet, action=d, tx_type=EXCHANGE_OP_CODES["placeOrder"])
            pl = {"action": {"data": d, "type": str(EXCHANGE_OP_CODES["placeOrder"])},
                  "signature": sig, "nonce": nonce}
            async with sess.post(api + "exchange", json=pl) as r:
                body = await r.json()
            ms = (time.perf_counter() - t0) * 1000
            times2.append(ms)
            status = body.get("type", "ok") if isinstance(body, dict) else "ok"
            print(f"  placeOrder IOC: {ms:.1f}ms  status={status}")
        print(f"[aiohttp exchange/placeOrder] avg={sum(times2)/len(times2):.1f}ms  min={min(times2):.1f}ms  max={max(times2):.1f}ms")

        # simulate dual taker open timing
        async def lighter_sim():
            """Simulate Lighter IOC latency."""
            await asyncio.sleep(0.08)  # ~80ms typical
            return True

        async def hs_ioc_sim():
            """Simulate optimized Hotstuff IOC."""
            nonce = nm.get_nonce()
            d = {"orders": [{"instrumentId": 1, "side": "b", "positionSide": "BOTH",
                             "price": "1000", "size": "0.001", "tif": "IOC",
                             "ro": False, "po": False, "isMarket": False}],
                 "expiresAfter": int(time.time()*1000)+60000, "nonce": nonce}
            sig = sign_action(wallet=wallet, action=d, tx_type=EXCHANGE_OP_CODES["placeOrder"])
            pl = {"action": {"data": d, "type": str(EXCHANGE_OP_CODES["placeOrder"])},
                  "signature": sig, "nonce": nonce}
            async with sess.post(api + "exchange", json=pl) as r:
                return await r.json()

        dual_times = []
        for _ in range(5):
            t0 = time.perf_counter()
            await asyncio.gather(lighter_sim(), hs_ioc_sim())
            ms = (time.perf_counter() - t0) * 1000
            dual_times.append(ms)
        print(f"\n[模拟双腿并行] avg={sum(dual_times)/len(dual_times):.1f}ms  min={min(dual_times):.1f}ms  max={max(dual_times):.1f}ms")

    print("\n=== DONE ===")

asyncio.run(bench_aiohttp_exchange())
