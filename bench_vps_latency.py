"""
VPS 延迟基准测试 — 测量到 Lighter / Extended 的网络延迟和下单耗时
用法: python3 bench_vps_latency.py [--symbol ETH]
"""
from __future__ import annotations
import argparse, asyncio, json, os, sys, time
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from statistics import mean, median

import requests, websockets, aiohttp
from dotenv import load_dotenv
load_dotenv()

# ─── 颜色输出 ───
G = "\033[92m"; Y = "\033[93m"; R = "\033[91m"; C = "\033[96m"; RST = "\033[0m"

def ms(t): return f"{t*1000:.0f}ms"

LIGHTER_WS = "wss://stream.lighter.xyz/stream"
LIGHTER_REST = "https://api.lighter.xyz"
EXTENDED_WS_TPL = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1/orderbooks/{symbol}-USD?depth=1"
EXTENDED_REST = "https://api.starknet.extended.exchange"


async def test_lighter_ws(symbol: str):
    """Lighter WebSocket: 连接 + 首条数据延迟"""
    url = LIGHTER_WS
    t0 = time.perf_counter()
    try:
        async with websockets.connect(url, ping_interval=10) as ws:
            t_conn = time.perf_counter() - t0
            sub = {"type": "subscribe", "channels": [f"orderbook.{symbol}"]}
            await ws.send(json.dumps(sub))
            await asyncio.wait_for(ws.recv(), timeout=5)
            t_data = time.perf_counter() - t0
            return t_conn, t_data
    except Exception as e:
        print(f"  {R}Lighter WS 失败: {e}{RST}")
        return None, None


async def test_extended_ws(symbol: str):
    """Extended WebSocket: 连接 + 首条数据延迟"""
    url = EXTENDED_WS_TPL.format(symbol=symbol)
    t0 = time.perf_counter()
    try:
        async with websockets.connect(url, ping_interval=10) as ws:
            t_conn = time.perf_counter() - t0
            await asyncio.wait_for(ws.recv(), timeout=5)
            t_data = time.perf_counter() - t0
            return t_conn, t_data
    except Exception as e:
        print(f"  {R}Extended WS 失败: {e}{RST}")
        return None, None


def test_lighter_rest(symbol: str, n: int = 5):
    """Lighter REST API 延迟 (orderbook)"""
    url = f"{LIGHTER_REST}/api/v1/orderbook"
    params = {"market": symbol, "limit": 5}
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        try:
            r = requests.get(url, params=params, timeout=5)
            r.raise_for_status()
            times.append(time.perf_counter() - t0)
        except Exception as e:
            print(f"  {R}Lighter REST 失败: {e}{RST}")
    return times


def test_extended_rest(symbol: str, n: int = 5):
    """Extended REST API 延迟 (orderbook)"""
    url = f"{EXTENDED_REST}/v1/orderbook/{symbol}-USD"
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        try:
            r = requests.get(url, timeout=5)
            r.raise_for_status()
            times.append(time.perf_counter() - t0)
        except Exception as e:
            print(f"  {R}Extended REST 失败: {e}{RST}")
    return times


async def test_lighter_order(symbol: str):
    """Lighter SDK 下单延迟 (不会成交的极端价格)"""
    try:
        from lighter.signer_client import SignerClient
    except ImportError:
        print(f"  {R}lighter-sdk 未安装{RST}")
        return None

    pk = os.getenv("API_KEY_PRIVATE_KEY", "")
    acct = int(os.getenv("LIGHTER_ACCOUNT_INDEX", 0))
    api_idx = int(os.getenv("LIGHTER_API_KEY_INDEX", 0))
    if not pk:
        print(f"  {R}缺少 API_KEY_PRIVATE_KEY{RST}")
        return None

    client = SignerClient(private_key=pk, api_key_index=api_idx, account_index=acct)

    market_map = {"ETH": 0, "BTC": 1, "SOL": 4}
    market_index = market_map.get(symbol, 0)
    price_dec_map = {"ETH": 2, "BTC": 1, "SOL": 3}
    size_dec_map = {"ETH": 4, "BTC": 5, "SOL": 2}
    p_dec = price_dec_map.get(symbol, 2)
    s_dec = size_dec_map.get(symbol, 4)
    price_mult = 10 ** p_dec
    size_mult = 10 ** s_dec

    extreme_price = 1
    min_size = 1

    times = []
    for i in range(3):
        t0 = time.perf_counter()
        try:
            _, _, error = await client.create_market_order(
                market_index=market_index,
                client_order_index=int(time.time() * 1000) + i,
                base_amount=min_size,
                avg_execution_price=extreme_price,
                is_ask=False,
            )
            elapsed = time.perf_counter() - t0
            times.append(elapsed)
            status = f"err={error}" if error else "ok"
            print(f"    Lighter 下单 #{i+1}: {ms(elapsed)} ({status})")
        except Exception as e:
            elapsed = time.perf_counter() - t0
            times.append(elapsed)
            print(f"    Lighter 下单 #{i+1}: {ms(elapsed)} (异常: {e})")
        await asyncio.sleep(0.2)

    # cancel all
    try:
        future_ts = int((time.time() + 300) * 1000)
        t0 = time.perf_counter()
        await client.cancel_all_orders(
            time_in_force=client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            timestamp_ms=future_ts)
        cancel_t = time.perf_counter() - t0
        print(f"    Lighter cancel_all: {ms(cancel_t)}")
    except Exception:
        pass

    return times


async def test_extended_order(symbol: str):
    """Extended SDK 下单延迟 (不会成交的极端价格)"""
    try:
        from exchanges.extended import ExtendedClient
    except ImportError:
        print(f"  {R}exchanges.extended 导入失败{RST}")
        return None

    api_key = os.getenv("EXTENDED_API_KEY", "")
    stark_pub = os.getenv("EXTENDED_STARK_KEY_PUBLIC", "")
    stark_priv = os.getenv("EXTENDED_STARK_KEY_PRIVATE", "")
    vault = os.getenv("EXTENDED_VAULT", "")
    if not all([api_key, stark_pub, stark_priv, vault]):
        print(f"  {R}缺少 Extended 环境变量{RST}")
        return None

    client = ExtendedClient(
        api_key=api_key,
        stark_key_public=stark_pub,
        stark_key_private=stark_priv,
        vault=vault,
    )
    await client.initialize()
    contract_id = f"{symbol}-USD"

    extreme_price = Decimal("1.0")
    min_qty = Decimal("0.0001") if symbol == "BTC" else Decimal("0.001")

    times = []
    for i in range(3):
        t0 = time.perf_counter()
        try:
            result = await client.place_ioc_order(
                contract_id=contract_id,
                quantity=min_qty,
                side="buy",
                aggressive_price=extreme_price,
            )
            elapsed = time.perf_counter() - t0
            times.append(elapsed)
            status = f"ok id={result.order_id}" if result.success else f"fail: {result.error_message}"
            print(f"    Extended 下单 #{i+1}: {ms(elapsed)} ({status})")
        except Exception as e:
            elapsed = time.perf_counter() - t0
            times.append(elapsed)
            print(f"    Extended 下单 #{i+1}: {ms(elapsed)} (异常: {e})")
        await asyncio.sleep(0.2)

    return times


async def test_gather_both(symbol: str):
    """模拟 asyncio.gather 双腿并发下单的总耗时"""
    print(f"\n{C}═══ 5. 双腿并发 asyncio.gather 测试 ═══{RST}")

    try:
        from lighter.signer_client import SignerClient
        from exchanges.extended import ExtendedClient
    except ImportError:
        print(f"  {R}SDK 导入失败{RST}")
        return

    pk = os.getenv("API_KEY_PRIVATE_KEY", "")
    acct = int(os.getenv("LIGHTER_ACCOUNT_INDEX", 0))
    api_idx = int(os.getenv("LIGHTER_API_KEY_INDEX", 0))
    lighter_client = SignerClient(private_key=pk, api_key_index=api_idx, account_index=acct)

    ext_client = ExtendedClient(
        api_key=os.getenv("EXTENDED_API_KEY", ""),
        stark_key_public=os.getenv("EXTENDED_STARK_KEY_PUBLIC", ""),
        stark_key_private=os.getenv("EXTENDED_STARK_KEY_PRIVATE", ""),
        vault=os.getenv("EXTENDED_VAULT", ""),
    )
    await ext_client.initialize()

    market_map = {"ETH": 0, "BTC": 1, "SOL": 4}
    mi = market_map.get(symbol, 0)

    async def lighter_order():
        t0 = time.perf_counter()
        await lighter_client.create_market_order(
            market_index=mi,
            client_order_index=int(time.time() * 1000),
            base_amount=1,
            avg_execution_price=1,
            is_ask=False,
        )
        return time.perf_counter() - t0

    async def extended_order():
        t0 = time.perf_counter()
        await ext_client.place_ioc_order(
            contract_id=f"{symbol}-USD",
            quantity=Decimal("0.0001"),
            side="buy",
            aggressive_price=Decimal("1.0"),
        )
        return time.perf_counter() - t0

    for i in range(3):
        t0 = time.perf_counter()
        l_t, x_t = await asyncio.gather(lighter_order(), extended_order())
        total = time.perf_counter() - t0
        print(f"  gather #{i+1}:  Lighter={ms(l_t)}  Extended={ms(x_t)}  "
              f"总耗时={ms(total)}  {'⚠ 串行!' if total > max(l_t, x_t) * 1.3 else '✅ 并发'}")
        await asyncio.sleep(0.5)

    # 测试 to_thread 包装
    print(f"\n  {Y}── 使用 asyncio.to_thread 解除阻塞 ──{RST}")

    async def lighter_order_threaded():
        t0 = time.perf_counter()
        await asyncio.to_thread(
            asyncio.run,
            lighter_client.create_market_order(
                market_index=mi,
                client_order_index=int(time.time() * 1000) + 999,
                base_amount=1,
                avg_execution_price=1,
                is_ask=False,
            )
        )
        return time.perf_counter() - t0

    # cancel all at end
    try:
        future_ts = int((time.time() + 300) * 1000)
        await lighter_client.cancel_all_orders(
            time_in_force=lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            timestamp_ms=future_ts)
    except Exception:
        pass


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="ETH")
    args = parser.parse_args()
    sym = args.symbol.upper()

    print(f"\n{'='*56}")
    print(f"  VPS 延迟基准测试  |  {sym}  |  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"{'='*56}\n")

    # 1. Lighter WS
    print(f"{C}═══ 1. Lighter WebSocket 延迟 ═══{RST}")
    lw_conn, lw_data = await test_lighter_ws(sym)
    if lw_conn:
        print(f"  连接: {ms(lw_conn)}  首条数据: {ms(lw_data)}")

    # 2. Extended WS
    print(f"\n{C}═══ 2. Extended WebSocket 延迟 ═══{RST}")
    xw_conn, xw_data = await test_extended_ws(sym)
    if xw_conn:
        print(f"  连接: {ms(xw_conn)}  首条数据: {ms(xw_data)}")

    # 3. REST API
    print(f"\n{C}═══ 3. REST API 延迟 (5次取中位数) ═══{RST}")
    lt = test_lighter_rest(sym)
    xt = test_extended_rest(sym)
    if lt:
        print(f"  Lighter REST:  中位数={ms(median(lt))}  平均={ms(mean(lt))}  "
              f"最快={ms(min(lt))}  最慢={ms(max(lt))}")
    if xt:
        print(f"  Extended REST: 中位数={ms(median(xt))}  平均={ms(mean(xt))}  "
              f"最快={ms(min(xt))}  最慢={ms(max(xt))}")

    # 4. 单独下单延迟
    print(f"\n{C}═══ 4a. Lighter 下单延迟 (极端价格不会成交) ═══{RST}")
    l_order_t = await test_lighter_order(sym)
    if l_order_t:
        print(f"  中位数={ms(median(l_order_t))}  平均={ms(mean(l_order_t))}")

    print(f"\n{C}═══ 4b. Extended 下单延迟 (极端价格不会成交) ═══{RST}")
    x_order_t = await test_extended_order(sym)
    if x_order_t:
        print(f"  中位数={ms(median(x_order_t))}  平均={ms(mean(x_order_t))}")

    # 5. gather 并发测试
    await test_gather_both(sym)

    # 汇总
    print(f"\n{'='*56}")
    print(f"  {G}汇总{RST}")
    print(f"{'='*56}")
    if lw_data and xw_data:
        print(f"  WS 首条数据:  Lighter={ms(lw_data)}  Extended={ms(xw_data)}")
    if lt and xt:
        print(f"  REST 中位数:  Lighter={ms(median(lt))}  Extended={ms(median(xt))}")
    if l_order_t and x_order_t:
        print(f"  下单 中位数:  Lighter={ms(median(l_order_t))}  Extended={ms(median(x_order_t))}")
        theoretical_gather = max(median(l_order_t), median(x_order_t))
        sequential = median(l_order_t) + median(x_order_t)
        print(f"  理论并发耗时: {ms(theoretical_gather)}  (串行={ms(sequential)})")
    print()


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass
    asyncio.run(main())
