#!/usr/bin/env python3
"""Compare bot's recorded (BBO) prices vs Hotstuff actual fill prices."""
import os, csv
from dotenv import load_dotenv
load_dotenv()
from hotstuff import InfoClient
from hotstuff.methods.info.account import FillsParams

info = InfoClient()
addr = os.getenv("HOTSTUFF_ADDRESS", "")

all_fills = []
for page in range(1, 10):
    resp = info.fills(FillsParams(user=addr, limit=10, page=page))
    entries = resp.entries if hasattr(resp, "entries") else []
    all_fills.extend(entries)
    if not (resp.has_next if hasattr(resp, "has_next") else False):
        break

all_fills.sort(key=lambda x: x["block_timestamp"])

bot_trades = []
with open("logs/hs_lighter_v27_BTC_trades.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        bot_trades.append(row)

print(f"Bot trades: {len(bot_trades)}, Hotstuff fills: {len(all_fills)}")
print()
print(f"{'Bot Trade':<20} {'Side':<6} {'BBO Ref':>10} {'Actual':>12} {'Slippage':>10} {'Slip bps':>9}")
print("=" * 80)

total_slip = 0.0
count = 0
for i, fill in enumerate(all_fills):
    side = "SELL" if fill["side"] == "s" else "BUY"
    actual = float(fill["price"])
    if i < len(bot_trades):
        bt = bot_trades[i]
        ref = float(bt["hotstuff_price"])
        if side == "SELL":
            slip = ref - actual
        else:
            slip = actual - ref
        slip_bps = slip / actual * 10000
        total_slip += slip
        count += 1
        tid = bt["trade_id"]
        print(f"{tid:<20} {side:<6} {ref:>10.1f} {actual:>12.3f} {slip:>+10.3f} {slip_bps:>+9.2f}")

print("=" * 80)
avg_slip = total_slip / count if count > 0 else 0
avg_bps = avg_slip / 70800 * 10000
print(f"Total slippage cost: ${total_slip * 0.005:.4f} (avg {avg_bps:.2f} bps/trade)")
print(f"Avg slippage per fill: ${avg_slip:.2f}")
