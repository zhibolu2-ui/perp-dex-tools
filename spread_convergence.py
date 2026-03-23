#!/usr/bin/env python3
"""
价差收敛套利精确模拟 (Lighter vs Drift)
策略: 价差大时同时开仓 → 价差缩小时同时平仓 → 赚取价差变动

使用之前采集的 2000 个数据点中的 mid 价差时序做回测。
同时重新采集一批数据确保准确。
"""
import asyncio, json, time, statistics, sys

TRADE_USD = 10000

# 费率
LIGHTER_FEE_BPS = 0      # maker/taker 0%
DRIFT_TAKER_FEE_BPS = 2.5  # taker 0.025%

def pr(*args, **kwargs):
    print(*args, **kwargs, flush=True)

async def main():
    from feeds.lighter_feed import LighterFeed
    from feeds.dex_feeds import DriftFeed

    lighter = LighterFeed('ETH')
    drift = DriftFeed('ETH')

    asyncio.create_task(lighter.connect())
    asyncio.create_task(drift.connect())

    pr("等待交易所连接 (8秒)...")
    await asyncio.sleep(8)

    ls = lighter.snapshot
    ds = drift.snapshot
    if not ls.mid or not ds.mid:
        pr("ERROR: 交易所未连接"); return

    pr(f"Lighter mid={ls.mid:.2f}, Drift mid={ds.mid:.2f}")
    pr(f"采集 2000 个数据点...")

    data = []
    fail = 0
    trade_qty = TRADE_USD / ls.mid

    for i in range(2000):
        ls = lighter.snapshot
        ds = drift.snapshot

        if not ls.mid or not ds.mid:
            fail += 1
            await asyncio.sleep(1)
            continue

        qty = trade_qty

        l_buy_vwap = ls.vwap_buy(qty)
        l_sell_vwap = ls.vwap_sell(qty)
        d_buy_vwap = ds.vwap_buy(qty)
        d_sell_vwap = ds.vwap_sell(qty)

        mid_spread_bps = (ds.mid - ls.mid) / ls.mid * 10000
        mid_spread_usd = ds.mid - ls.mid

        # 开仓成本: 买Lighter + 卖Drift (当 Drift 价格高时)
        open_cost_d1 = 0
        if l_buy_vwap and d_sell_vwap:
            cost_l_buy = (l_buy_vwap - ls.mid) / ls.mid * 10000  # Lighter买入滑损(正值=成本)
            cost_d_sell = (ds.mid - d_sell_vwap) / ds.mid * 10000  # Drift卖出滑损(正值=成本)
            open_cost_d1 = abs(cost_l_buy) + abs(cost_d_sell) + DRIFT_TAKER_FEE_BPS + LIGHTER_FEE_BPS

        # 平仓成本: 卖Lighter + 买Drift
        close_cost_d1 = 0
        if l_sell_vwap and d_buy_vwap:
            cost_l_sell = (ls.mid - l_sell_vwap) / ls.mid * 10000
            cost_d_buy = (d_buy_vwap - ds.mid) / ds.mid * 10000
            close_cost_d1 = abs(cost_l_sell) + abs(cost_d_buy) + DRIFT_TAKER_FEE_BPS + LIGHTER_FEE_BPS

        # 反方向: 买Drift + 卖Lighter (当 Lighter 价格高时)
        open_cost_d2 = 0
        if d_buy_vwap and l_sell_vwap:
            cost_d_buy2 = (d_buy_vwap - ds.mid) / ds.mid * 10000
            cost_l_sell2 = (ls.mid - l_sell_vwap) / ls.mid * 10000
            open_cost_d2 = abs(cost_d_buy2) + abs(cost_l_sell2) + DRIFT_TAKER_FEE_BPS + LIGHTER_FEE_BPS

        close_cost_d2 = 0
        if d_sell_vwap and l_buy_vwap:
            cost_d_sell2 = (ds.mid - d_sell_vwap) / ds.mid * 10000
            cost_l_buy2 = (l_buy_vwap - ls.mid) / ls.mid * 10000
            close_cost_d2 = abs(cost_d_sell2) + abs(cost_l_buy2) + DRIFT_TAKER_FEE_BPS + LIGHTER_FEE_BPS

        data.append({
            'ts': time.time(),
            'l_mid': ls.mid,
            'd_mid': ds.mid,
            'mid_sp_bps': round(mid_spread_bps, 2),
            'mid_sp_usd': round(mid_spread_usd, 2),
            'l_spread_bps': round(ls.spread_bps, 2),
            'd_spread_bps': round(ds.spread_bps, 2),
            'open_cost_d1': round(open_cost_d1, 2),  # 买L卖D的开仓成本
            'close_cost_d1': round(close_cost_d1, 2),  # 卖L买D的平仓成本
            'open_cost_d2': round(open_cost_d2, 2),  # 买D卖L的开仓成本
            'close_cost_d2': round(close_cost_d2, 2),  # 卖D买L的平仓成本
        })

        if (i+1) % 200 == 0:
            pr(f"  {i+1}/2000 ({time.strftime('%H:%M:%S')})")

        await asyncio.sleep(1)

    pr(f"\n采集完成! 有效={len(data)}, 失败={fail}")
    n = len(data)
    if n < 100:
        pr("数据不足"); return

    # ═══════════════════════════════════════════════════════════════
    # 分析
    # ═══════════════════════════════════════════════════════════════

    mids = [d['mid_sp_bps'] for d in data]
    mids_usd = [d['mid_sp_usd'] for d in data]
    eth_price = statistics.mean([d['l_mid'] for d in data])

    pr("\n" + "═"*72)
    pr("  一、你的策略逻辑确认")
    pr("═"*72)
    pr("  ETH价差 = Drift价格 - Lighter价格")
    pr("  开仓: 价差大时 → 买便宜的(Lighter) + 卖贵的(Drift)")
    pr("  平仓: 价差缩小时 → 卖Lighter + 买Drift")
    pr("  毛利 = 开仓时价差 - 平仓时价差")
    pr("  净利 = 毛利 - 开仓执行成本 - 平仓执行成本")

    # ═══ 执行成本统计 ═══
    pr("\n" + "═"*72)
    pr("  二、每笔交易的真实执行成本 ($10,000 仓位)")
    pr("═"*72)

    oc_d1 = [d['open_cost_d1'] for d in data if d['open_cost_d1'] > 0]
    cc_d1 = [d['close_cost_d1'] for d in data if d['close_cost_d1'] > 0]

    avg_open = statistics.mean(oc_d1) if oc_d1 else 0
    avg_close = statistics.mean(cc_d1) if cc_d1 else 0
    avg_total = avg_open + avg_close

    pr(f"  开仓成本(买L+卖D): 均值 {avg_open:.2f} bps = ${avg_open/10000*TRADE_USD:.2f}")
    pr(f"    其中: Lighter买入滑点≈0.38bps + Drift卖出滑点≈12.6bps + Drift费2.5bps")
    pr(f"  平仓成本(卖L+买D): 均值 {avg_close:.2f} bps = ${avg_close/10000*TRADE_USD:.2f}")
    pr(f"    其中: Lighter卖出滑点≈0.40bps + Drift买入滑点≈14.2bps + Drift费2.5bps")
    pr(f"  ────────────────────────────────────────")
    pr(f"  往返总成本: {avg_total:.2f} bps = ${avg_total/10000*TRADE_USD:.2f}")
    pr(f"  换算成ETH价格: ${avg_total/10000*eth_price:.2f}/ETH")
    pr(f"  → 价差变动 > {avg_total:.1f} bps (>${avg_total/10000*eth_price:.2f}/ETH) 才有净利")

    # ═══ Mid价差分布 ═══
    pr("\n" + "═"*72)
    pr("  三、Mid 价差分布 (Drift - Lighter)")
    pr("═"*72)
    pr(f"  ETH均价: ${eth_price:.2f}")
    pr(f"  平均价差: {statistics.mean(mids):+.2f} bps = ${statistics.mean(mids_usd):+.2f}")
    pr(f"  中位价差: {statistics.median(mids):+.2f} bps = ${statistics.median(mids_usd):+.2f}")
    pr(f"  标准差:   {statistics.stdev(mids):.2f} bps = ${statistics.stdev(mids_usd):.2f}")
    pr(f"  最小:     {min(mids):+.2f} bps = ${min(mids_usd):+.2f}")
    pr(f"  最大:     {max(mids):+.2f} bps = ${max(mids_usd):+.2f}")
    pr(f"  总波动范围: {max(mids)-min(mids):.2f} bps = ${max(mids_usd)-min(mids_usd):.2f}")

    # ═══ 频率直方图 ═══
    pr("\n" + "═"*72)
    pr("  四、Mid 价差频率分布 (bps / USD)")
    pr("═"*72)

    buckets = [
        (-999,-20), (-20,-15), (-15,-10), (-10,-7), (-7,-5), (-5,-3),
        (-3,-1), (-1,0), (0,1), (1,3), (3,5), (5,7), (7,10),
        (10,15), (15,20), (20,999),
    ]
    for lo, hi in buckets:
        cnt = sum(1 for s in mids if lo <= s < hi)
        pct = cnt / n * 100
        lo_usd = lo / 10000 * eth_price
        hi_usd = hi / 10000 * eth_price
        bar = '█' * int(pct / 2)
        label = f"{lo:+d}~{hi:+d}" if abs(lo) < 900 else (f"<{hi:+d}" if lo < 0 else f">{lo:+d}")
        pr(f"  {label:>10s} bps | {cnt:4d} ({pct:5.1f}%) {bar}")

    # ═══ 核心：价差收敛模拟 ═══
    pr("\n" + "═"*72)
    pr("  五、价差收敛套利模拟 (你的策略)")
    pr("═"*72)
    pr(f"  逻辑: Mid价差 > 开仓阈值时开仓, 缩小到 < 平仓阈值时平仓")
    pr(f"  往返执行成本: ~{avg_total:.1f} bps")
    pr(f"  方向: 价差>0时买Lighter卖Drift, 价差<0时买Drift卖Lighter")
    pr()

    # 使用有符号的mid价差做回测
    # 策略: 当 |mid_spread| > open_thresh, 开仓(方向取决于符号)
    #        当 |mid_spread| < close_thresh 或 符号翻转, 平仓
    results_table = []

    for open_thresh in [3, 5, 7, 10, 15, 20]:
        for close_thresh in [0, 1, 2, 3]:
            if close_thresh >= open_thresh:
                continue

            trades = []
            i = 0
            while i < n:
                sp = mids[i]
                if abs(sp) >= open_thresh:
                    direction = 1 if sp > 0 else -1  # 1=买L卖D, -1=买D卖L
                    open_sp = sp
                    open_idx = i

                    if direction == 1:
                        open_cost = data[i]['open_cost_d1']
                    else:
                        open_cost = data[i]['open_cost_d2']

                    # 找平仓点
                    closed = False
                    for j in range(i+1, min(i+600, n)):  # 最多等600秒
                        sp_j = mids[j]
                        # 平仓条件: 价差回到接近0 / 低于阈值
                        if direction == 1 and sp_j <= close_thresh:
                            close_sp = sp_j
                            if direction == 1:
                                close_cost = data[j]['close_cost_d1']
                            else:
                                close_cost = data[j]['close_cost_d2']

                            gross = (open_sp - close_sp)  # 价差缩小 = 毛利
                            net = gross - open_cost - close_cost
                            net_usd = net / 10000 * TRADE_USD

                            trades.append({
                                'gross': gross,
                                'net': net,
                                'net_usd': net_usd,
                                'wait': j - i,
                                'open_cost': open_cost,
                                'close_cost': close_cost,
                            })
                            i = j + 1
                            closed = True
                            break
                        elif direction == -1 and sp_j >= -close_thresh:
                            close_sp = sp_j
                            close_cost = data[j]['close_cost_d2']
                            gross = abs(open_sp) - abs(close_sp)
                            net = gross - open_cost - close_cost
                            net_usd = net / 10000 * TRADE_USD

                            trades.append({
                                'gross': gross,
                                'net': net,
                                'net_usd': net_usd,
                                'wait': j - i,
                                'open_cost': open_cost,
                                'close_cost': close_cost,
                            })
                            i = j + 1
                            closed = True
                            break

                    if not closed:
                        i += 1
                else:
                    i += 1

            if not trades:
                results_table.append({
                    'open': open_thresh, 'close': close_thresh,
                    'count': 0, 'close_rate': 0,
                })
                continue

            # 统计开仓机会数
            open_opportunities = sum(1 for s in mids if abs(s) >= open_thresh)

            nets = [t['net'] for t in trades]
            nets_usd = [t['net_usd'] for t in trades]
            waits = [t['wait'] for t in trades]
            profitable = [t for t in trades if t['net'] > 0]
            losing = [t for t in trades if t['net'] <= 0]

            dur_min = (data[-1]['ts'] - data[0]['ts']) / 60

            results_table.append({
                'open': open_thresh, 'close': close_thresh,
                'count': len(trades),
                'opportunities': open_opportunities,
                'opp_pct': round(open_opportunities/n*100, 1),
                'trades_per_hour': round(len(trades) / (dur_min/60), 1),
                'close_rate': round(len(trades) / max(open_opportunities,1) * 100, 1),
                'avg_gross': round(statistics.mean([t['gross'] for t in trades]), 2),
                'avg_net': round(statistics.mean(nets), 2),
                'avg_net_usd': round(statistics.mean(nets_usd), 2),
                'avg_wait': round(statistics.mean(waits), 0),
                'median_wait': round(statistics.median(waits), 0),
                'win_rate': round(len(profitable)/len(trades)*100, 1),
                'avg_win_usd': round(statistics.mean([t['net_usd'] for t in profitable]), 2) if profitable else 0,
                'avg_loss_usd': round(statistics.mean([t['net_usd'] for t in losing]), 2) if losing else 0,
            })

    # 打印结果表
    pr(f"  {'开仓':>6s} {'平仓':>6s} | {'笔数':>4s} {'频率/h':>6s} | {'平仓率':>6s} {'胜率':>6s} | "
       f"{'毛利bps':>8s} {'净利bps':>8s} {'净利$':>7s} | {'等待s':>5s} {'均赢$':>7s} {'均亏$':>7s}")
    pr("  " + "─"*100)

    for r in results_table:
        if r['count'] == 0:
            pr(f"  >={r['open']:+d}  <={r['close']:+d}  | {'无':>4s} {'---':>6s} | {'---':>6s} {'---':>6s} | "
               f"{'---':>8s} {'---':>8s} {'---':>7s} | {'---':>5s} {'---':>7s} {'---':>7s}")
            continue
        pr(f"  >={r['open']:+2d}   <={r['close']:+2d}  | {r['count']:4d} {r['trades_per_hour']:5.1f}h | "
           f"{r['close_rate']:5.1f}% {r['win_rate']:5.1f}% | "
           f"{r['avg_gross']:+7.2f}  {r['avg_net']:+7.2f}  {r['avg_net_usd']:+6.2f} | "
           f"{r['avg_wait']:5.0f} {r['avg_win_usd']:+6.2f}  {r['avg_loss_usd']:+6.2f}")

    # ═══ 杠杆收益 ═══
    pr("\n" + "═"*72)
    pr("  六、杠杆收益换算 (50x 杠杆, $10,000 名义, $200 保证金)")
    pr("═"*72)

    best = [r for r in results_table if r['count'] > 0 and r.get('avg_net', 0) > 0]
    if best:
        for r in best:
            margin = TRADE_USD / 50
            net_per_trade = r['avg_net_usd']
            roi_per_trade = net_per_trade / margin * 100
            trades_per_day = r['trades_per_hour'] * 24
            daily_profit = net_per_trade * trades_per_day
            daily_roi = daily_profit / margin * 100

            pr(f"  策略: 开仓>={r['open']}bps, 平仓<={r['close']}bps")
            pr(f"    每笔净利: ${net_per_trade:.2f} (保证金ROI: {roi_per_trade:.2f}%)")
            pr(f"    频率: {r['trades_per_hour']:.1f}笔/小时, 胜率: {r['win_rate']:.1f}%")
            pr(f"    日交易量: ~{trades_per_day:.0f}笔")
            pr(f"    日利润预估: ${daily_profit:.2f} (日ROI: {daily_roi:.2f}%)")
            pr()
    else:
        pr("  所有策略组合的平均净利润均为负，不建议执行。\n")

    # ═══ 总结 ═══
    pr("═"*72)
    pr("  七、最终结论")
    pr("═"*72)

    any_profitable = any(r.get('avg_net', 0) > 0 for r in results_table if r['count'] > 0)

    if any_profitable:
        best_r = max([r for r in results_table if r['count'] > 0], key=lambda x: x.get('avg_net', -999))
        pr(f"  ✅ 最优策略: 开仓>={best_r['open']}bps, 平仓<={best_r['close']}bps")
        pr(f"     均净利={best_r['avg_net']:+.2f}bps, 胜率={best_r['win_rate']}%, "
           f"频率={best_r['trades_per_hour']:.1f}/h")
    else:
        pr(f"  ❌ Lighter vs Drift $10K taker-taker 收敛套利不可行")
        pr(f"     原因: Drift 盘口价差 ~{statistics.mean([d['d_spread_bps'] for d in data]):.1f} bps，")
        pr(f"     往返执行成本 ~{avg_total:.1f} bps 远超 Mid 价差波动")
        pr(f"     Mid价差标准差仅 {statistics.stdev(mids):.2f} bps")
        pr()
        pr(f"  💡 建议:")
        pr(f"     1. 换盘口更紧的交易所 (GRVT盘口~3bps, Hyperliquid~1bps)")
        pr(f"     2. 在Drift用Maker单 (0费率+吃不到盘口价差)，但有填充风险")
        pr(f"     3. 等极端行情 (价差>30bps)，但频率极低")

    for f in [lighter, drift]:
        await f.disconnect()

asyncio.run(main())
