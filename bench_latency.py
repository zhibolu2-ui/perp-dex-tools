#!/usr/bin/env python3
"""
交易所延迟基准测试 — 测量从本机到两个交易所的真实下单延迟
"""
import time, asyncio, os, sys
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
load_dotenv()

async def bench_extended():
    from x10.perpetual.accounts import StarkPerpetualAccount
    from x10.perpetual.configuration import STARKNET_MAINNET_CONFIG
    from x10.perpetual.orders import OrderSide, TimeInForce
    from x10.perpetual.trading_client import PerpetualTradingClient

    account = StarkPerpetualAccount(
        vault=os.getenv('EXTENDED_VAULT'),
        private_key=os.getenv('EXTENDED_STARK_KEY_PRIVATE'),
        public_key=os.getenv('EXTENDED_STARK_KEY_PUBLIC'),
        api_key=os.getenv('EXTENDED_API_KEY'),
    )
    client = PerpetualTradingClient(STARKNET_MAINNET_CONFIG, account)
    expire = datetime.now(tz=timezone.utc) + timedelta(hours=1)

    print("=== Extended 下单延迟 ===")
    for i in range(5):
        t0 = time.perf_counter()
        try:
            result = await client.place_order(
                market_name='ETH-USD',
                amount_of_synthetic=Decimal('0.1'),
                price=Decimal('1500.0'),
                side=OrderSide.BUY,
                post_only=False,
                time_in_force=TimeInForce.IOC,
                expire_time=expire,
            )
            ms = (time.perf_counter() - t0) * 1000
            print(f"  #{i+1}: {ms:.0f}ms  status={result.status}")
        except Exception as e:
            ms = (time.perf_counter() - t0) * 1000
            print(f"  #{i+1}: {ms:.0f}ms  error={e}")
    await client.close()

async def bench_lighter():
    from lighter.signer_client import SignerClient
    api_key = os.getenv('API_KEY_PRIVATE_KEY')
    client = SignerClient(
        url='https://mainnet.zklighter.elliot.ai',
        account_index=int(os.getenv('LIGHTER_ACCOUNT_INDEX', '0')),
        api_private_keys={int(os.getenv('LIGHTER_API_KEY_INDEX', '0')): api_key},
    )

    print("\n=== Lighter 下单延迟 ===")
    for i in range(5):
        coi = int(time.time() * 1000) * 100 + i + 99
        t0 = time.perf_counter()
        try:
            _, _, error = await client.create_order(
                market_index=0,
                client_order_index=coi,
                base_amount=1000,
                price=150000,
                is_ask=False,
                order_type=client.ORDER_TYPE_LIMIT,
                time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY,
                reduce_only=False,
                trigger_price=0,
            )
            ms = (time.perf_counter() - t0) * 1000
            print(f"  #{i+1}: {ms:.0f}ms  error={error}")
        except Exception as e:
            ms = (time.perf_counter() - t0) * 1000
            print(f"  #{i+1}: {ms:.0f}ms  exc={e}")
        if i < 4:
            await asyncio.sleep(0.05)

    try:
        future_ts = int((time.time() + 300) * 1000)
        await client.cancel_all_orders(
            time_in_force=client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            timestamp_ms=future_ts,
        )
        print("  (已清理测试挂单)")
    except:
        pass

async def bench_network():
    import aiohttp
    print("=== 网络 RTT (TCP连接时间) ===")
    targets = [
        ("Extended API", "https://api.starknet.extended.exchange/api/v1/markets"),
        ("Lighter API", "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"),
    ]
    for name, url in targets:
        times = []
        for _ in range(5):
            async with aiohttp.ClientSession() as session:
                t0 = time.perf_counter()
                async with session.get(url) as resp:
                    await resp.read()
                ms = (time.perf_counter() - t0) * 1000
                times.append(ms)
        avg = sum(times[1:]) / len(times[1:])
        print(f"  {name}: 冷启动={times[0]:.0f}ms  预热后avg={avg:.0f}ms")

async def main():
    print(f"延迟基准测试 @ {datetime.now()}\n")
    await bench_network()
    await bench_extended()
    await bench_lighter()
    
    print("\n=== 总结 ===")
    print("对冲完成估算 = WS延迟(~50ms) + Extended IOC(预热后)")
    print("策略B可行条件: Extended IOC预热后 < 200ms")

asyncio.run(main())
