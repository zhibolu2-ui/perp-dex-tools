#!/usr/bin/env python3
"""分析 V21 交易日志，找出亏损原因"""
import csv
import statistics
import sys

CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "logs/spread_v21_BTC_trades.csv"

opens = []
closes = []
with open(CSV_PATH) as f:
    reader = csv.DictReader(f)
    for row in reader:
        rec = {
            "ts": row["timestamp"][:19],
            "tid": row["trade_id"],
            "spread": float(row["spread_bps"]),
            "fee": float(row["fee_bps"]),
            "net": float(row["net_bps"]),
            "qty": float(row["qty"]),
            "side": row["side"],
            "l_price": float(row["lighter_price"]),
            "x_price": float(row["extended_price"]),
        }
        if "close" in rec["tid"]:
            closes.append(rec)
        else:
            opens.append(rec)

print(f"总开仓: {len(opens)}笔  总平仓: {len(closes)}笔")

open_spreads = [o["spread"] for o in opens]
open_nets = [o["net"] for o in opens]
close_nets = [c["net"] for c in closes]
close_spreads = [c["spread"] for c in closes]
close_fees = [c["fee"] for c in closes]

print(f"\n{'='*60}")
print(f"  开仓分析")
print(f"{'='*60}")
print(f"  spread: 均值={statistics.mean(open_spreads):.2f}  "
      f"中位={statistics.median(open_spreads):.2f}  "
      f"最小={min(open_spreads):.2f}  最大={max(open_spreads):.2f}")
print(f"  net:    均值={statistics.mean(open_nets):.2f}  "
      f"中位={statistics.median(open_nets):.2f}")

below_3 = sum(1 for s in open_spreads if s < 3.0)
below_2 = sum(1 for s in open_spreads if s < 2.0)
below_1 = sum(1 for s in open_spreads if s < 1.0)
negative = sum(1 for s in open_spreads if s < 0)
loss_opens = sum(1 for n in open_nets if n < 0)
print(f"  spread<3bps: {below_3}/{len(opens)} ({below_3/len(opens)*100:.0f}%)")
print(f"  spread<2bps: {below_2}/{len(opens)} ({below_2/len(opens)*100:.0f}%)")
print(f"  spread<1bps: {below_1}/{len(opens)} ({below_1/len(opens)*100:.0f}%)")
print(f"  spread<0:    {negative}/{len(opens)}")
print(f"  亏损开仓(net<0): {loss_opens}/{len(opens)} ({loss_opens/len(opens)*100:.0f}%)")

print(f"\n  -- 低spread开仓明细(spread<2.5bps) --")
for o in opens:
    if o["spread"] < 2.5:
        print(f"    {o['ts']} spread={o['spread']:+.2f}bps net={o['net']:+.2f} "
              f"L={o['l_price']} X={o['x_price']}")

print(f"\n{'='*60}")
print(f"  平仓分析")
print(f"{'='*60}")
print(f"  close_cost: 均值={statistics.mean(close_nets):.2f}  "
      f"中位={statistics.median(close_nets):.2f}")
print(f"  close_spread: 均值={statistics.mean(close_spreads):.2f}  "
      f"中位={statistics.median(close_spreads):.2f}")
print(f"  fee分布: 2.25bps={sum(1 for f in close_fees if abs(f-2.25)<0.01)}笔  "
      f"2.65bps={sum(1 for f in close_fees if abs(f-2.65)<0.01)}笔")

REF_PRICE = 68500
total_open_dollar = sum(o["net"] / 10000 * o["qty"] * REF_PRICE for o in opens)
total_close_dollar = sum(c["net"] / 10000 * c["qty"] * REF_PRICE for c in closes)

print(f"\n{'='*60}")
print(f"  美元 P&L")
print(f"{'='*60}")
print(f"  开仓盈利: ${total_open_dollar:+.4f}")
print(f"  平仓成本: ${total_close_dollar:+.4f}")
print(f"  净 P&L:   ${total_open_dollar + total_close_dollar:+.4f}")

avg_close_cost_abs = statistics.mean([abs(c["net"]) for c in closes])
avg_open_gain = statistics.mean(open_nets)
print(f"\n{'='*60}")
print(f"  单笔分析")
print(f"{'='*60}")
print(f"  平均开仓净利:  {avg_open_gain:+.2f} bps")
print(f"  平均平仓成本:  -{avg_close_cost_abs:.2f} bps")
print(f"  平均每轮净利:  {avg_open_gain - avg_close_cost_abs:+.2f} bps (理论)")
print(f"  breakeven需:   开仓spread > {avg_close_cost_abs + 2.25:.2f} bps")

maker_close_spread_cost = statistics.mean([abs(s) for s in close_spreads])
saved_fee_total = sum(c["fee"] / 10000 * c["qty"] * REF_PRICE for c in closes)
print(f"\n{'='*60}")
print(f"  如果平仓改用 Maker 挂单 (0费用)")
print(f"{'='*60}")
print(f"  当前平仓成本: fee={statistics.mean(close_fees):.2f}bps + 价差滑点={maker_close_spread_cost:.2f}bps")
print(f"  Maker平仓成本: 仅价差滑点={maker_close_spread_cost:.2f}bps (省掉fee)")
print(f"  每笔可省:     {statistics.mean(close_fees):.2f} bps")
print(f"  总可省金额:   ${saved_fee_total:.4f}")
print(f"  净P&L变为:    ${total_open_dollar + total_close_dollar + saved_fee_total:+.4f}")
