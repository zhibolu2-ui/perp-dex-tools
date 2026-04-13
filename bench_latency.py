#!/usr/bin/env python3
"""Benchmark each component of Hotstuff order flow."""
import time, os, json, requests, asyncio
from dotenv import load_dotenv
from eth_account import Account
from decimal import Decimal
from dataclasses import asdict

load_dotenv()

from hotstuff.utils.signing import sign_action
from hotstuff.methods.exchange.trading import UnitOrder, PlaceOrderParams, CancelAllParams
from hotstuff.methods.exchange.op_codes import EXCHANGE_OP_CODES
from hotstuff.utils.nonce import NonceManager

pk = os.getenv("HOTSTUFF_PRIVATE_KEY")
wallet = Account.from_key(pk)
address = os.getenv("HOTSTUFF_ADDRESS", wallet.address)
nonce_mgr = NonceManager()
api_url = "https://api.hotstuff.trade/"

order = UnitOrder(instrumentId=1, side="b", positionSide="BOTH",
                  price="70000", size="0.001", tif="IOC",
                  ro=False, po=False, isMarket=False)
params_dict = {"orders": [asdict(order)], "expiresAfter": int(time.time()*1000)+60000}

# Warmup signing
nonce = nonce_mgr.get_nonce(); params_dict["nonce"] = nonce
sign_action(wallet=wallet, action=params_dict, tx_type=EXCHANGE_OP_CODES["placeOrder"])

# 1. Signing
times = []
for _ in range(10):
    nonce = nonce_mgr.get_nonce(); params_dict["nonce"] = nonce
    t0 = time.perf_counter()
    sig = sign_action(wallet=wallet, action=params_dict, tx_type=EXCHANGE_OP_CODES["placeOrder"])
    t1 = time.perf_counter()
    times.append((t1-t0)*1000)
print(f"[签名] avg={sum(times)/len(times):.1f}ms  min={min(times):.1f}ms  max={max(times):.1f}ms")

# 2. HTTP with keep-alive
sess = requests.Session()
sess.headers.update({"Content-Type": "application/json"})
sess.post(api_url + "info", json={"method": "positions", "params": {"user": address}}, timeout=5)

times_pos = []
for _ in range(10):
    t0 = time.perf_counter()
    r = sess.post(api_url + "info", json={"method": "positions", "params": {"user": address}}, timeout=5)
    t1 = time.perf_counter()
    times_pos.append((t1-t0)*1000)
print(f"[info/positions keep-alive] avg={sum(times_pos)/len(times_pos):.1f}ms  min={min(times_pos):.1f}ms  max={max(times_pos):.1f}ms")

times_fills = []
for _ in range(5):
    t0 = time.perf_counter()
    r = sess.post(api_url + "info", json={"method": "fills", "params": {"user": address, "limit": 3}}, timeout=5)
    t1 = time.perf_counter()
    times_fills.append((t1-t0)*1000)
print(f"[info/fills keep-alive] avg={sum(times_fills)/len(times_fills):.1f}ms  min={min(times_fills):.1f}ms  max={max(times_fills):.1f}ms")

# 3. Exchange sign+send
cancel_dict = asdict(CancelAllParams())
times_ex = []
for _ in range(5):
    nonce = nonce_mgr.get_nonce(); cancel_dict["nonce"] = nonce
    t0 = time.perf_counter()
    sig = sign_action(wallet=wallet, action=cancel_dict, tx_type=EXCHANGE_OP_CODES["cancelAll"])
    payload = {"action": {"data": cancel_dict, "type": str(EXCHANGE_OP_CODES["cancelAll"])}, "signature": sig, "nonce": nonce}
    r = sess.post(api_url + "exchange", json=payload, timeout=5)
    t1 = time.perf_counter()
    times_ex.append((t1-t0)*1000)
    print(f"  exchange: {r.status_code} {r.text[:120]}")
print(f"[exchange/sign+send keep-alive] avg={sum(times_ex)/len(times_ex):.1f}ms  min={min(times_ex):.1f}ms  max={max(times_ex):.1f}ms")

# 4. New connection overhead
times_new = []
for _ in range(3):
    s2 = requests.Session()
    t0 = time.perf_counter()
    r = s2.post(api_url + "info", json={"method": "positions", "params": {"user": address}}, timeout=5)
    t1 = time.perf_counter()
    times_new.append((t1-t0)*1000); s2.close()
print(f"[info/positions 新连接] avg={sum(times_new)/len(times_new):.1f}ms")

# 5. run_in_executor overhead
async def test_executor():
    loop = asyncio.get_event_loop()
    times_exec = []
    def noop(): return 42
    for _ in range(10):
        t0 = time.perf_counter()
        await loop.run_in_executor(None, noop)
        t1 = time.perf_counter()
        times_exec.append((t1-t0)*1000)
    print(f"[run_in_executor 空函数] avg={sum(times_exec)/len(times_exec):.2f}ms")
asyncio.run(test_executor())

# 6. aiohttp benchmark
async def test_aiohttp():
    import aiohttp
    async with aiohttp.ClientSession() as asess:
        await asess.post(api_url + "info", json={"method": "positions", "params": {"user": address}})
        times_aio = []
        for _ in range(10):
            t0 = time.perf_counter()
            async with asess.post(api_url + "info", json={"method": "positions", "params": {"user": address}}) as r:
                await r.json()
            t1 = time.perf_counter()
            times_aio.append((t1-t0)*1000)
        print(f"[aiohttp positions keep-alive] avg={sum(times_aio)/len(times_aio):.1f}ms  min={min(times_aio):.1f}ms  max={max(times_aio):.1f}ms")
try:
    asyncio.run(test_aiohttp())
except Exception as e:
    print(f"[aiohttp] 不可用: {e}")

print("\n=== DONE ===")
