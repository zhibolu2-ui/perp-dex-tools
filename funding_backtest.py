#!/usr/bin/env python3
"""
Backtest: Binance USDC-M funding rates for BTC/ETH/SOL over 1 year.
Also fetch Lighter funding rates for comparison.
Analyze holding cost at different durations.
"""
import requests, json, time, sys
from datetime import datetime, timedelta
from collections import defaultdict

BINANCE_FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
LIGHTER_REST = "https://mainnet.zklighter.elliot.ai"

def fetch_binance_funding(symbol, start_ts, end_ts, limit=1000):
    """Fetch Binance USDC-M funding rate history."""
    all_rates = []
    current_start = start_ts
    while current_start < end_ts:
        params = {
            "symbol": symbol,
            "startTime": int(current_start * 1000),
            "endTime": int(end_ts * 1000),
            "limit": limit,
        }
        try:
            r = requests.get(BINANCE_FUNDING_URL, params=params, timeout=15)
            data = r.json()
            if not data or not isinstance(data, list):
                break
            for d in data:
                all_rates.append({
                    "ts": d["fundingTime"] / 1000,
                    "rate": float(d["fundingRate"]),
                    "dt": datetime.utcfromtimestamp(d["fundingTime"] / 1000),
                })
            if len(data) < limit:
                break
            current_start = data[-1]["fundingTime"] / 1000 + 1
            time.sleep(0.2)
        except Exception as e:
            print(f"  Error fetching {symbol}: {e}")
            break
    return all_rates

def fetch_lighter_funding_info():
    """Try to get Lighter funding rate info."""
    try:
        r = requests.get(f"{LIGHTER_REST}/api/v1/fundingRates", timeout=10)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    try:
        r = requests.get(f"{LIGHTER_REST}/api/v1/markets", timeout=10)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None

def analyze_funding(rates, symbol):
    """Comprehensive funding rate analysis."""
    if not rates:
        return None

    n = len(rates)
    fr = [r["rate"] for r in rates]
    abs_fr = [abs(r) for r in fr]
    pos_fr = [r for r in fr if r > 0]
    neg_fr = [r for r in fr if r < 0]

    days = (rates[-1]["ts"] - rates[0]["ts"]) / 86400
    settlements_per_day = n / days if days > 0 else 3

    # Per-settlement stats
    avg_rate = sum(fr) / n
    avg_abs = sum(abs_fr) / n
    max_rate = max(fr)
    min_rate = min(fr)
    median_rate = sorted(fr)[n // 2]

    # Annualized
    annual_rate = avg_rate * settlements_per_day * 365 * 100
    annual_abs = avg_abs * settlements_per_day * 365 * 100

    # Monthly breakdown
    monthly = defaultdict(list)
    for r in rates:
        key = r["dt"].strftime("%Y-%m")
        monthly[key].append(r["rate"])

    # Holding cost per hour (worst case: always paying)
    cost_per_settlement_bps = avg_abs * 10000
    hours_per_settlement = 24 / settlements_per_day

    # Simulate holding costs at different durations
    holding_costs = {}
    for hours in [0.5, 1, 2, 4, 8, 12, 24, 48, 72, 168]:
        expected_settlements = hours / hours_per_settlement
        expected_cost_bps = cost_per_settlement_bps * expected_settlements
        holding_costs[hours] = expected_cost_bps

    return {
        "symbol": symbol,
        "n": n,
        "days": days,
        "settlements_per_day": settlements_per_day,
        "avg_rate": avg_rate,
        "avg_abs_rate": avg_abs,
        "max_rate": max_rate,
        "min_rate": min_rate,
        "median_rate": median_rate,
        "pos_count": len(pos_fr),
        "neg_count": len(neg_fr),
        "pos_pct": len(pos_fr) / n * 100,
        "annual_rate_pct": annual_rate,
        "annual_abs_pct": annual_abs,
        "cost_per_settlement_bps": cost_per_settlement_bps,
        "hours_per_settlement": hours_per_settlement,
        "holding_costs": holding_costs,
        "monthly": monthly,
    }

def main():
    symbols = {
        "BTC": "BTCUSDC",
        "ETH": "ETHUSDC",
        "SOL": "SOLUSDC",
    }

    end_time = time.time()
    start_time = end_time - 365 * 86400

    print("=" * 70)
    print("  Binance USDC-M Funding Rate Backtest (1 Year)")
    print(f"  Period: {datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%d')} to {datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%d')}")
    print("=" * 70)

    all_results = {}

    for name, bsym in symbols.items():
        print(f"\n>>> Fetching {name} ({bsym}) funding rates...")
        rates = fetch_binance_funding(bsym, start_time, end_time)
        print(f"    Got {len(rates)} records")

        if not rates:
            print(f"    No data for {name}, trying USDT pair...")
            bsym_usdt = name + "USDT"
            rates = fetch_binance_funding(bsym_usdt, start_time, end_time)
            print(f"    Got {len(rates)} records (USDT)")

        result = analyze_funding(rates, name)
        if result:
            all_results[name] = result

    # Lighter funding info
    print("\n>>> Checking Lighter funding rates...")
    lighter_info = fetch_lighter_funding_info()
    if lighter_info:
        print(f"    Lighter funding data: {json.dumps(lighter_info)[:300]}")
    else:
        print("    Lighter funding API not accessible (will use known info)")
    print("    Lighter: hourly settlement, rate typically 0.001%-0.01% per hour")

    # Report
    print(f"\n{'=' * 70}")
    print("  FUNDING RATE ANALYSIS")
    print("=" * 70)

    for name, r in all_results.items():
        print(f"\n{'-' * 50}")
        print(f"  {name}")
        print(f"{'-' * 50}")
        print(f"  Data: {r['n']} settlements over {r['days']:.0f} days")
        print(f"  Settlements/day: {r['settlements_per_day']:.1f} (every {r['hours_per_settlement']:.1f}h)")
        print(f"  Avg rate: {r['avg_rate']*10000:+.4f} bps/settlement")
        print(f"  Avg |rate|: {r['avg_abs_rate']*10000:.4f} bps/settlement")
        print(f"  Max rate: {r['max_rate']*10000:+.4f} bps")
        print(f"  Min rate: {r['min_rate']*10000:+.4f} bps")
        print(f"  Positive: {r['pos_pct']:.1f}% ({r['pos_count']}/{r['n']})")
        print(f"  Annualized avg: {r['annual_rate_pct']:+.2f}%")
        print(f"  Annualized |avg|: {r['annual_abs_pct']:.2f}%")

    # Holding cost comparison
    print(f"\n{'=' * 70}")
    print("  HOLDING COST vs SPREAD PROFIT (Binance side only)")
    print("  Assumption: you pay funding every settlement while holding")
    print("=" * 70)

    hours_list = [0.5, 1, 2, 4, 8, 12, 24, 48, 72, 168]
    header = f"  {'Hours':<8}"
    for name in all_results:
        header += f" {name:>10}"
    print(header)
    print("  " + "-" * (8 + 11 * len(all_results)))

    for h in hours_list:
        row = f"  {h:<8.1f}"
        for name, r in all_results.items():
            cost = r["holding_costs"].get(h, 0)
            row += f" {cost:>9.2f}bp"
        print(row)

    # Break-even analysis
    print(f"\n{'=' * 70}")
    print("  BREAK-EVEN: How long can you hold before funding eats your profit?")
    print("  (Binance funding only, Lighter funding is separate)")
    print("=" * 70)

    for spread_bps in [1.0, 2.0, 3.0, 5.0]:
        print(f"\n  If net spread profit = {spread_bps:.1f} bps:")
        for name, r in all_results.items():
            cost_per_hour = r["cost_per_settlement_bps"] / r["hours_per_settlement"]
            if cost_per_hour > 0:
                breakeven_hours = spread_bps / cost_per_hour
                print(f"    {name}: breakeven at {breakeven_hours:.1f}h ({breakeven_hours/24:.1f} days)")
            else:
                print(f"    {name}: no cost (rate=0)")

    # Monthly trend
    print(f"\n{'=' * 70}")
    print("  MONTHLY FUNDING RATE TREND (avg bps per settlement)")
    print("=" * 70)

    all_months = set()
    for r in all_results.values():
        all_months.update(r["monthly"].keys())
    months_sorted = sorted(all_months)

    header = f"  {'Month':<10}"
    for name in all_results:
        header += f" {name:>10}"
    print(header)
    print("  " + "-" * (10 + 11 * len(all_results)))

    for m in months_sorted:
        row = f"  {m:<10}"
        for name, r in all_results.items():
            rates = r["monthly"].get(m, [])
            if rates:
                avg = sum(rates) / len(rates) * 10000
                row += f" {avg:>+9.3f}bp"
            else:
                row += f" {'N/A':>10}"
        print(row)

    # Lighter vs Binance funding comparison
    print(f"\n{'=' * 70}")
    print("  LIGHTER vs BINANCE FUNDING COMPARISON")
    print("=" * 70)
    print("""
  Lighter: hourly settlement
    - Rate: typically 0.001% - 0.01% per hour (1-10 bps)
    - Settlement: every 1 hour
    - Direction: depends on market skew

  Binance USDC-M: 8h settlement (3x/day)
    - Rate: see above data
    - Settlement: every 8 hours (00:00, 08:00, 16:00 UTC)

  KEY INSIGHT for arb:
    When you hold Lighter LONG + Binance SHORT:
    - You PAY Lighter funding if rate > 0 (longs pay shorts)
    - You RECEIVE Binance funding if rate > 0 (shorts receive)
    - Net cost = Lighter_rate - Binance_rate (per settlement period)
    
    When you hold Lighter SHORT + Binance LONG:
    - You RECEIVE Lighter funding if rate > 0
    - You PAY Binance funding if rate > 0
    - Net cost = Binance_rate - Lighter_rate
""")

    # Final recommendation
    print(f"{'=' * 70}")
    print("  FINAL RECOMMENDATION")
    print("=" * 70)

    for name, r in all_results.items():
        cost_1h = r["holding_costs"].get(1, 0)
        cost_4h = r["holding_costs"].get(4, 0)
        cost_8h = r["holding_costs"].get(8, 0)
        cost_24h = r["holding_costs"].get(24, 0)
        print(f"\n  {name}:")
        print(f"    Funding cost: 1h={cost_1h:.2f}bp  4h={cost_4h:.2f}bp  8h={cost_8h:.2f}bp  24h={cost_24h:.2f}bp")
        be_3 = 3.0 / (r["cost_per_settlement_bps"] / r["hours_per_settlement"]) if r["cost_per_settlement_bps"] > 0 else 999
        be_5 = 5.0 / (r["cost_per_settlement_bps"] / r["hours_per_settlement"]) if r["cost_per_settlement_bps"] > 0 else 999
        print(f"    Breakeven: @3bps={be_3:.1f}h  @5bps={be_5:.1f}h")
        if be_3 > 4:
            print(f"    -> Safe to hold up to ~{be_3/2:.0f}h with 3bps profit")
        else:
            print(f"    -> WARNING: funding eats profit fast, close within {be_3/2:.0f}h")

if __name__ == "__main__":
    main()
