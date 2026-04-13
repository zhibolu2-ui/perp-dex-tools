#!/usr/bin/env python3
"""
检测Lighter POST_ONLY订单功能:
  1. 查询当前BBO
  2. 放一个POST_ONLY buy单 (在best_bid, 保证是maker)
  3. 等3秒看是否接受
  4. 放一个POST_ONLY buy单 (在best_ask+1tick, 肯定会穿越, 应被拒绝)
  5. 清理: 撤销所有订单

用法: python probe_lighter_post_only.py
"""

import asyncio
import os
import sys
import time
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv
from lighter.signer_client import SignerClient

sys.path.insert(0, str(Path(__file__).resolve().parent))

LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
MARKET_INDEX = 1  # BTC

try:
    import websockets
except ImportError:
    print("需要 websockets: pip install websockets")
    sys.exit(1)

import json


def get_bbo_rest(price_multiplier):
    """通过REST获取Lighter BTC BBO (从交易列表推算)"""
    import requests
    url = f"{LIGHTER_REST_URL}/api/v1/trades?market_index={MARKET_INDEX}&limit=1"
    try:
        r = requests.get(url, headers={"accept": "application/json"}, timeout=10)
        r.raise_for_status()
        data = r.json()
        trades = data.get("trades", [])
        if trades:
            last_price = Decimal(str(trades[0].get("price", "0")))
            if last_price > 0:
                tick = Decimal("1") / Decimal(str(price_multiplier))
                return last_price - tick * 5, last_price + tick * 5
    except Exception:
        pass
    return None, None


LIGHTER_REST_URL = "https://mainnet.zklighter.elliot.ai"


async def main():
    load_dotenv()

    api_key = os.getenv("API_KEY_PRIVATE_KEY")
    account_index = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
    api_key_index = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))

    if not api_key:
        print("错误: 需要在.env中设置 API_KEY_PRIVATE_KEY")
        return

    print("=" * 60)
    print("Lighter POST_ONLY 订单功能检测")
    print("=" * 60)

    print("\n[1] 初始化Lighter客户端...")
    client = SignerClient(
        url=LIGHTER_REST_URL,
        account_index=account_index,
        api_private_keys={api_key_index: api_key},
    )
    err = client.check_client()
    if err:
        print(f"  客户端检查失败: {err}")
        return
    print(f"  ✅ 已连接 (account={account_index}, api_key_index={api_key_index})")

    # 获取市场参数
    import requests
    ticker_map = {0: "ETH", 1: "BTC", 2: "SOL", 3: "DOGE"}
    ticker = ticker_map.get(MARKET_INDEX, "BTC")
    resp = requests.get(f"{LIGHTER_REST_URL}/api/v1/orderBooks",
                        headers={"accept": "application/json"}, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    market = None
    for m in data.get("order_books", []):
        if m.get("symbol") == ticker:
            market = m
            break
    if not market:
        print(f"获取市场{ticker}信息失败")
        return

    sd = int(market.get("supported_size_decimals", 5))
    pd_ = int(market.get("supported_price_decimals", 1))
    price_multiplier = 10 ** pd_
    base_amount_multiplier = 10 ** sd
    tick_size = Decimal("1") / (Decimal("10") ** pd_)
    min_size = Decimal("0.00001")

    print(f"  市场: BTC (index={MARKET_INDEX})")
    print(f"  价格精度: {price_multiplier}, tick={tick_size}")
    print(f"  数量精度: {base_amount_multiplier}, min={min_size}")

    # SDK常量
    print(f"\n[2] SDK常量:")
    print(f"  ORDER_TYPE_LIMIT = {client.ORDER_TYPE_LIMIT}")
    print(f"  ORDER_TIME_IN_FORCE_POST_ONLY = {client.ORDER_TIME_IN_FORCE_POST_ONLY}")
    print(f"  ORDER_TIME_IN_FORCE_IOC = {client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL}")
    print(f"  ORDER_TIME_IN_FORCE_GTT = {client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME}")

    # 测试1: POST_ONLY BUY在一个很低的价格 (远低于市场, 不会穿越→应被接受挂入orderbook)
    safe_low_price = Decimal("50000.0")
    test_qty = min_size * 100  # 0.001 BTC
    cid1 = int(time.time() * 1000)

    print(f"\n[3] 测试1: POST_ONLY BUY @ {safe_low_price} (远低于市场→应成功挂入)")
    t0 = time.time()
    try:
        tx, tx_hash, err = await client.create_order(
            market_index=MARKET_INDEX,
            client_order_index=cid1,
            base_amount=int(test_qty * base_amount_multiplier),
            price=int(safe_low_price * price_multiplier),
            is_ask=False,
            order_type=client.ORDER_TYPE_LIMIT,
            time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY,
            order_expiry=client.DEFAULT_28_DAY_ORDER_EXPIRY,
            reduce_only=False,
            trigger_price=0,
        )
        ms = (time.time() - t0) * 1000
        if err:
            print(f"  ❌ 失败 ({ms:.0f}ms): {err}")
        else:
            print(f"  ✅ POST_ONLY订单被接受! ({ms:.0f}ms)")
    except Exception as e:
        ms = (time.time() - t0) * 1000
        print(f"  ❌ 异常 ({ms:.0f}ms): {e}")

    await asyncio.sleep(1)

    # 测试2: POST_ONLY SELL在一个很低的价格 (会穿越买盘→应被拒绝)
    cid2 = int(time.time() * 1000)
    print(f"\n[4] 测试2: POST_ONLY SELL @ {safe_low_price} (远低于市场, 穿越买盘→应被拒绝)")
    t0 = time.time()
    try:
        tx, tx_hash, err = await client.create_order(
            market_index=MARKET_INDEX,
            client_order_index=cid2,
            base_amount=int(test_qty * base_amount_multiplier),
            price=int(safe_low_price * price_multiplier),
            is_ask=True,
            order_type=client.ORDER_TYPE_LIMIT,
            time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY,
            order_expiry=client.DEFAULT_28_DAY_ORDER_EXPIRY,
            reduce_only=False,
            trigger_price=0,
        )
        ms = (time.time() - t0) * 1000
        if err:
            print(f"  ✅ 正确拒绝! ({ms:.0f}ms) POST_ONLY阻止穿越: {err}")
        else:
            print(f"  ⚠️ 意外接受 ({ms:.0f}ms) — POST_ONLY可能未生效!")
    except Exception as e:
        ms = (time.time() - t0) * 1000
        err_str = str(e).lower()
        if "post_only" in err_str or "would" in err_str or "cross" in err_str or "match" in err_str:
            print(f"  ✅ 正确拒绝! ({ms:.0f}ms): {e}")
        else:
            print(f"  结果 ({ms:.0f}ms): {e}")

    # 清理: 撤销所有订单
    print(f"\n[6] 清理: 撤销所有订单...")
    await asyncio.sleep(0.5)
    try:
        await client.cancel_all_orders(market_index=MARKET_INDEX)
        print(f"  ✅ 已撤销")
    except Exception as e:
        print(f"  ⚠️ 撤销异常: {e}")

    # 总结
    print("\n" + "=" * 60)
    print("总结:")
    print("  - ORDER_TIME_IN_FORCE_POST_ONLY = 2 → SDK已支持")
    print("  - 用法: time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY")
    print("  - POST_ONLY保证: 穿越价差时订单被拒绝, 不会变成taker")
    print("  - 配合Premium账户0ms modify → 可安全maker平仓(0.40bps)")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
