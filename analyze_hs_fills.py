#!/usr/bin/env python3
import os
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

print(f"Total fills: {len(all_fills)}")
print()
print(f"{'Time(UTC)':<20} {'Side':<5} {'Real Price':>12} {'Size':>8} {'Fee':>10} {'Closed PnL':>12}")
print("-" * 80)
total_fee = 0.0
total_cpnl = 0.0
for f in reversed(all_fills):
    ts = f["block_timestamp"][11:19]
    side = "SELL" if f["side"] == "s" else "BUY"
    price = f["price"]
    size = f["size"]
    fee = float(f["fee"])
    cpnl = float(f["closed_pnl"])
    total_fee += fee
    total_cpnl += cpnl
    print(f"{ts:<20} {side:<5} {price:>12} {size:>8} {fee:>10.6f} {cpnl:>+12.6f}")

print("-" * 80)
print(f"Total fees paid:     ${total_fee:.6f}")
print(f"Total closed PnL:    ${total_cpnl:.6f}")
print(f"Net (cpnl - fee):    ${total_cpnl - total_fee:.6f}")
