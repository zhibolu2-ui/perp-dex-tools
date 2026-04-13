"""清理两个交易所的所有挂单和仓位"""
import asyncio, os, requests, time
from decimal import Decimal

for line in open(".env"):
    if "=" in line and not line.startswith("#"):
        k, v = line.strip().split("=", 1)
        os.environ[k] = v

LIGHTER_URL = "https://mainnet.zklighter.elliot.ai"
ACCT = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))

async def main():
    from lighter.signer_client import SignerClient
    lc = SignerClient(
        url=LIGHTER_URL, account_index=ACCT,
        api_private_keys={int(os.getenv("LIGHTER_API_KEY_INDEX", "0")): os.getenv("API_KEY_PRIVATE_KEY")})

    # 1. Lighter: 查真实数据
    r = requests.get(f"{LIGHTER_URL}/api/v1/account", params={"by": "index", "value": ACCT}, timeout=10)
    acct = r.json().get("accounts", [{}])[0]

    # 撤所有挂单
    orders = acct.get("open_orders", {})
    total = sum(len(v) for v in orders.values())
    print(f"[Lighter] 挂单: {total}")
    for mkt, ords in orders.items():
        for o in ords:
            idx = o.get("order_index")
            if idx:
                await asyncio.sleep(0.5)
                _, _, err = await lc.cancel_order(market_index=int(mkt), order_index=idx)
                print(f"  撤单 idx={idx}: {'OK' if not err else err}")

    # 平仓位
    for pos in acct.get("positions", []):
        sym = pos.get("symbol")
        amt = float(pos.get("position", "0"))
        sign = int(pos.get("sign", 1))
        real_amt = amt * sign
        print(f"[Lighter] {sym}: position={amt} sign={sign} → {real_amt}")
        if abs(amt) >= 0.00001 and amt > 0:
            r2 = requests.get(f"{LIGHTER_URL}/api/v1/orderBooks", timeout=10)
            for mkt in r2.json().get("order_books", []):
                if mkt.get("symbol") == sym:
                    bm = pow(10, mkt["supported_size_decimals"])
                    pm = pow(10, mkt["supported_price_decimals"])
                    mid = mkt["market_id"]
                    if sign < 0:  # short, need buy
                        price = float(mkt.get("best_ask_price", 0)) * 1.005
                        is_ask = False
                    else:  # long, need sell
                        price = float(mkt.get("best_bid_price", 0)) * 0.995
                        is_ask = True
                    print(f"  平仓: {'SELL' if is_ask else 'BUY'} {amt} @ {price:.1f}")
                    await asyncio.sleep(0.5)
                    _, _, err = await lc.create_order(
                        market_index=mid, client_order_index=int(time.time()*1000),
                        base_amount=int(amt * bm), price=int(price * pm),
                        is_ask=is_ask, order_type=lc.ORDER_TYPE_LIMIT,
                        time_in_force=lc.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                        order_expiry=lc.DEFAULT_IOC_EXPIRY, reduce_only=True, trigger_price=0)
                    print(f"  结果: {'OK' if not err else err}")
                    break

    # 2. Extended
    from exchanges.extended import ExtendedClient
    from types import SimpleNamespace
    cfg = SimpleNamespace(ticker="BTC", contract_id="BTC-USD", take_profit=0.1,
                          tick_size=Decimal("1"), close_order_side="sell")
    xc = ExtendedClient(cfg)
    await xc.connect()
    await asyncio.sleep(2)
    pd = await xc.perpetual_trading_client.account.get_positions(market_names=["BTC-USD"])
    if pd and pd.data:
        for p in pd.data:
            if p.market == "BTC-USD" and Decimal(str(p.size)) > 0:
                side_str = str(p.side).upper()
                ioc_side = "buy" if side_str == "SHORT" else "sell"
                bb, ba = await xc.fetch_bbo_prices("BTC-USD")
                ref = bb if ioc_side == "sell" else ba
                price = ref * (Decimal("0.99") if ioc_side == "sell" else Decimal("1.01"))
                print(f"[Extended] {side_str} {p.size} → {ioc_side}")
                result = await xc.place_ioc_order("BTC-USD", Decimal(str(p.size)), ioc_side, price)
                print(f"  结果: {'OK' if result.success else result.error_message}")
    else:
        print("[Extended] 无仓位")

    # 验证
    await asyncio.sleep(2)
    r3 = requests.get(f"{LIGHTER_URL}/api/v1/account", params={"by": "index", "value": ACCT}, timeout=10)
    a3 = r3.json().get("accounts", [{}])[0]
    for pos in a3.get("positions", []):
        if pos.get("symbol") == "BTC":
            amt = float(pos.get("position", "0"))
            print(f"[验证] Lighter BTC: {amt * int(pos.get('sign', 1))}")
    print("[清理完成]")

asyncio.run(main())
