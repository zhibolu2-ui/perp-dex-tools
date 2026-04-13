import asyncio, os
for line in open(".env"):
    if "=" in line and not line.startswith("#"):
        k, v = line.strip().split("=", 1)
        os.environ[k] = v

from exchanges.extended import ExtendedClient
from types import SimpleNamespace
from decimal import Decimal

async def main():
    cfg = SimpleNamespace(
        ticker="BTC", contract_id="BTC-USD",
        take_profit=0.1, tick_size=Decimal("1"),
        close_order_side="sell")
    c = ExtendedClient(cfg)
    await c.connect()
    await asyncio.sleep(2)
    pd = await c.perpetual_trading_client.account.get_positions(
        market_names=["BTC-USD"])
    if pd and pd.data:
        for p in pd.data:
            if p.market == "BTC-USD":
                size = Decimal(str(p.size))
                side_str = str(p.side).upper()
                print(f"Extended: size={size} side={side_str}")
                if size > 0:
                    ioc_side = "buy" if side_str == "SHORT" else "sell"
                    bb, ba = await c.fetch_bbo_prices("BTC-USD")
                    ref = bb if ioc_side == "sell" else ba
                    price = ref * (Decimal("0.99") if ioc_side == "sell" else Decimal("1.01"))
                    r = await c.place_ioc_order("BTC-USD", size, ioc_side, price)
                    status = "OK" if r.success else str(r.error_message)
                    print(f"  Close {ioc_side} {size}: {status}")
    else:
        print("Extended: no position")

asyncio.run(main())
