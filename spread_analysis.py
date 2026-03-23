#!/usr/bin/env python3
"""
Lighter vs Drift 价差深度分析：采集 2000 个实时数据点，
输出频率分布、开平仓模拟、真实盈利空间分析。
"""
import asyncio, json, time, statistics, sys

SAMPLES = 2000
TRADE_USD = 10000

# Drift 费率 (bps)
DRIFT_MAKER_FEE = 0     # maker 0%
DRIFT_TAKER_FEE = 2.5   # taker 0.025%
LIGHTER_FEE = 0          # 0%

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
    if not ls.mid:
        pr("ERROR: Lighter 未连接，退出"); return
    if not ds.mid:
        pr("ERROR: Drift 未连接，退出"); return

    pr(f"连接成功! Lighter mid={ls.mid:.2f}, Drift mid={ds.mid:.2f}")
    pr(f"开始采集 {SAMPLES} 个数据点 (每秒1个, ~{SAMPLES//60}分钟)...")
    pr(f"开始时间: {time.strftime('%H:%M:%S')}\n")

    trade_qty = TRADE_USD / ls.mid
    data = []
    fail = 0

    for i in range(SAMPLES):
        ls = lighter.snapshot
        ds = drift.snapshot

        if not ls.mid or not ds.mid:
            fail += 1
            await asyncio.sleep(1)
            continue

        mid_sp = (ds.mid - ls.mid) / ls.mid * 10000

        qty = trade_qty
        l_buy = ls.vwap_buy(qty)
        l_sell = ls.vwap_sell(qty)
        d_buy = ds.vwap_buy(qty)
        d_sell = ds.vwap_sell(qty)

        # 方向1: Lighter买 + Drift卖 (Drift价高时)
        vwap_d1 = (d_sell - l_buy) / l_buy * 10000 if (l_buy and d_sell and l_buy > 0) else None
        # 方向2: Drift买 + Lighter卖 (Lighter价高时)
        vwap_d2 = (l_sell - d_buy) / d_buy * 10000 if (d_buy and l_sell and d_buy > 0) else None

        slip_lb = abs(l_buy - ls.best_ask) / ls.best_ask * 10000 if (l_buy and ls.best_ask) else None
        slip_ls = abs(l_sell - ls.best_bid) / ls.best_bid * 10000 if (l_sell and ls.best_bid) else None
        slip_db = abs(d_buy - ds.best_ask) / ds.best_ask * 10000 if (d_buy and ds.best_ask) else None
        slip_ds = abs(d_sell - ds.best_bid) / ds.best_bid * 10000 if (d_sell and ds.best_bid) else None

        data.append({
            'ts': time.time(),
            'l_mid': ls.mid, 'd_mid': ds.mid,
            'l_bid': ls.best_bid, 'l_ask': ls.best_ask,
            'd_bid': ds.best_bid, 'd_ask': ds.best_ask,
            'l_spread': ls.spread_bps, 'd_spread': ds.spread_bps,
            'mid_sp': round(mid_sp, 2),
            'vwap_d1': round(vwap_d1, 2) if vwap_d1 is not None else None,
            'vwap_d2': round(vwap_d2, 2) if vwap_d2 is not None else None,
            'slip_lb': round(slip_lb, 2) if slip_lb is not None else None,
            'slip_ls': round(slip_ls, 2) if slip_ls is not None else None,
            'slip_db': round(slip_db, 2) if slip_db is not None else None,
            'slip_ds': round(slip_ds, 2) if slip_ds is not None else None,
        })

        if (i + 1) % 200 == 0:
            pr(f"  进度: {i+1}/{SAMPLES}  ({time.strftime('%H:%M:%S')})")

        await asyncio.sleep(1)

    pr(f"\n采集完成! 有效: {len(data)}, 失败: {fail}")
    if len(data) < 10:
        pr("数据太少，退出"); return

    # ═══════════════════════════════════════
    # 1. 基础统计
    # ═══════════════════════════════════════
    mids = [d['mid_sp'] for d in data]
    abs_mids = [abs(m) for m in mids]

    pr("\n" + "="*70)
    pr("  1. 基础价差统计 (Mid 价格, Drift - Lighter, bps)")
    pr("="*70)
    pr(f"  平均价差:  {statistics.mean(mids):+.2f} bps")
    pr(f"  中位价差:  {statistics.median(mids):+.2f} bps")
    pr(f"  标准差:    {statistics.stdev(mids):.2f} bps")
    pr(f"  最小价差:  {min(mids):+.2f} bps")
    pr(f"  最大价差:  {max(mids):+.2f} bps")
    pr(f"  |绝对值|均:  {statistics.mean(abs_mids):.2f} bps")
    pr(f"  Lighter 平均盘口: {statistics.mean([d['l_spread'] for d in data]):.2f} bps")
    pr(f"  Drift   平均盘口: {statistics.mean([d['d_spread'] for d in data]):.2f} bps")

    # ═══════════════════════════════════════
    # 2. 滑点统计
    # ═══════════════════════════════════════
    sl_keys = [('slip_lb','Lighter买入'), ('slip_ls','Lighter卖出'),
               ('slip_db','Drift买入'), ('slip_ds','Drift卖出')]
    pr("\n" + "="*70)
    pr(f"  2. ${TRADE_USD:,} 交易滑点统计 (bps)")
    pr("="*70)
    for k, name in sl_keys:
        vals = [d[k] for d in data if d[k] is not None]
        if vals:
            pr(f"  {name}: 均值={statistics.mean(vals):.2f}, 中位={statistics.median(vals):.2f}, "
               f"P90={sorted(vals)[int(len(vals)*0.9)]:.2f}")
        else:
            pr(f"  {name}: 无数据")

    # ═══════════════════════════════════════
    # 3. Mid 价差频率分布
    # ═══════════════════════════════════════
    pr("\n" + "="*70)
    pr("  3. Mid 价差频率分布 (Drift - Lighter)")
    pr("="*70)
    buckets = [
        (-999,-20,'< -20'), (-20,-15,'-20~-15'), (-15,-10,'-15~-10'),
        (-10,-5,'-10~-5'), (-5,-3,'-5~-3'), (-3,-1,'-3~-1'), (-1,0,'-1~0'),
        (0,1,'0~+1'), (1,3,'+1~+3'), (3,5,'+3~+5'), (5,10,'+5~+10'),
        (10,15,'+10~+15'), (15,20,'+15~+20'), (20,999,'> +20'),
    ]
    n = len(mids)
    for lo, hi, label in buckets:
        cnt = sum(1 for s in mids if lo <= s < hi)
        pct = cnt / n * 100
        bar = '█' * int(pct / 2)
        pr(f"  {label:>10s} | {cnt:4d} ({pct:5.1f}%) {bar}")

    # ═══════════════════════════════════════
    # 4. VWAP 价差 (含滑点) 分析
    # ═══════════════════════════════════════
    vwap1 = [d['vwap_d1'] for d in data if d['vwap_d1'] is not None]
    vwap2 = [d['vwap_d2'] for d in data if d['vwap_d2'] is not None]

    pr("\n" + "="*70)
    pr(f"  4. VWAP 价差分析 (含 ${TRADE_USD:,} 滑点, bps)")
    pr("="*70)
    if vwap1:
        pr(f"  方向1 (买Lighter+卖Drift): 均值={statistics.mean(vwap1):+.2f}, "
           f"中位={statistics.median(vwap1):+.2f}, >0占比={sum(1 for v in vwap1 if v>0)/len(vwap1)*100:.1f}%")
    if vwap2:
        pr(f"  方向2 (买Drift+卖Lighter): 均值={statistics.mean(vwap2):+.2f}, "
           f"中位={statistics.median(vwap2):+.2f}, >0占比={sum(1 for v in vwap2 if v>0)/len(vwap2)*100:.1f}%")

    # ═══════════════════════════════════════
    # 5. 开仓价差幅度分布 (取最优方向)
    # ═══════════════════════════════════════
    open_spreads = []
    for d in data:
        v1, v2 = d['vwap_d1'], d['vwap_d2']
        best = 0
        if v1 is not None and v2 is not None:
            best = max(abs(v1), abs(v2))
        elif v1 is not None:
            best = abs(v1)
        elif v2 is not None:
            best = abs(v2)
        open_spreads.append(best)

    pr("\n" + "="*70)
    pr("  5. 最优方向 VWAP 价差幅度分布 (bps, 仅开仓一腿)")
    pr("="*70)
    pbuckets = [(0,2,'0~2'), (2,4,'2~4'), (4,6,'4~6'), (6,8,'6~8'),
                (8,10,'8~10'), (10,15,'10~15'), (15,20,'15~20'), (20,999,'>20')]
    for lo, hi, label in pbuckets:
        cnt = sum(1 for p in open_spreads if lo <= p < hi)
        pct = cnt / n * 100
        bar = '█' * int(pct / 2)
        pr(f"  {label:>7s} bps | {cnt:4d} ({pct:5.1f}%) {bar}")

    srt = sorted(open_spreads)
    pr(f"\n  P25={srt[n//4]:.2f}  P50={srt[n//2]:.2f}  P75={srt[n*3//4]:.2f}  "
       f"P90={srt[int(n*0.9)]:.2f}  均值={statistics.mean(open_spreads):.2f}")

    # ═══════════════════════════════════════
    # 6. 核心：开仓→平仓 全流程利润模拟
    # ═══════════════════════════════════════
    pr("\n" + "="*70)
    pr("  6. 开仓→平仓 全流程利润模拟")
    pr("     费用: Lighter 0% | Drift taker 2.5bps | 开+平各一次")
    pr("     总固定费用 = 2 × (Lighter费 + Drift费) = 2 × (0 + 2.5) = 5 bps")
    pr("="*70)

    TOTAL_FEE = 2 * (LIGHTER_FEE + DRIFT_TAKER_FEE)  # 5 bps

    for open_thresh in [5, 6, 8, 10, 12, 15, 20]:
        opens_idx = [i for i, s in enumerate(open_spreads) if s >= open_thresh]
        if not opens_idx:
            pr(f"\n  ── 开仓阈值 >= {open_thresh} bps: 无机会 ──")
            continue

        open_pct = len(opens_idx) / n * 100
        dur = (data[-1]['ts'] - data[0]['ts']) / 60 if len(data) > 1 else 1
        freq = len(opens_idx) / dur

        pr(f"\n  ── 开仓阈值 >= {open_thresh} bps ──")
        pr(f"     出现次数: {len(opens_idx)} / {n} = {open_pct:.1f}%")
        pr(f"     频率: {freq:.1f} 次/分钟")

        for close_thresh in [1, 2, 3, 5]:
            trades = []
            for idx in opens_idx:
                open_sp = open_spreads[idx]
                # 查找后续300秒内价差缩小到 close_thresh 以下
                for j in range(idx + 1, min(idx + 300, len(data))):
                    if open_spreads[j] <= close_thresh:
                        close_sp = open_spreads[j]
                        wait = j - idx
                        # 滑点：取开仓和平仓时的实际滑点
                        open_slips = (data[idx].get('slip_lb', 0) or 0) + (data[idx].get('slip_ds', 0) or 0)
                        close_slips = (data[j].get('slip_ls', 0) or 0) + (data[j].get('slip_db', 0) or 0)
                        gross = open_sp - close_sp
                        net = gross - TOTAL_FEE - open_slips - close_slips
                        trades.append({
                            'gross': gross,
                            'net': net,
                            'wait': wait,
                            'open_sp': open_sp,
                            'close_sp': close_sp,
                            'open_slip': open_slips,
                            'close_slip': close_slips,
                        })
                        break

            if not trades:
                pr(f"     平仓 <= {close_thresh} bps: 无法平仓")
                continue

            close_pct = len(trades) / len(opens_idx) * 100
            nets = [t['net'] for t in trades]
            gross_list = [t['gross'] for t in trades]
            waits = [t['wait'] for t in trades]
            profitable = sum(1 for n_ in nets if n_ > 0)
            prof_pct = profitable / len(nets) * 100

            # $10K 交易的实际美元利润
            avg_net = statistics.mean(nets)
            avg_usd = avg_net / 10000 * TRADE_USD

            pr(f"     平仓 <= {close_thresh} bps:")
            pr(f"       可平仓率: {close_pct:.1f}% ({len(trades)}/{len(opens_idx)})")
            pr(f"       等待时间: 均值={statistics.mean(waits):.0f}s, 中位={statistics.median(waits):.0f}s")
            pr(f"       毛利: 均值={statistics.mean(gross_list):.2f} bps")
            pr(f"       净利: 均值={avg_net:.2f} bps = ${avg_usd:.2f}/笔")
            pr(f"       盈利比例: {prof_pct:.1f}% ({profitable}/{len(trades)})")
            if profitable > 0:
                prof_nets = [n_ for n_ in nets if n_ > 0]
                pr(f"       盈利笔均利: {statistics.mean(prof_nets):.2f} bps = "
                   f"${statistics.mean(prof_nets)/10000*TRADE_USD:.2f}")

    # ═══════════════════════════════════════
    # 7. 最终结论
    # ═══════════════════════════════════════
    pr("\n" + "="*70)
    pr("  7. 套利可行性总结")
    pr("="*70)

    avg_abs = statistics.mean(abs_mids)
    pct_gt5 = sum(1 for s in open_spreads if s >= 5) / n * 100
    pct_gt8 = sum(1 for s in open_spreads if s >= 8) / n * 100
    pct_gt10 = sum(1 for s in open_spreads if s >= 10) / n * 100
    pct_lt3 = sum(1 for s in open_spreads if s <= 3) / n * 100

    pr(f"  平均|价差|: {avg_abs:.2f} bps")
    pr(f"  价差 >= 5 bps 出现率: {pct_gt5:.1f}%  (可开仓机会)")
    pr(f"  价差 >= 8 bps 出现率: {pct_gt8:.1f}%  (优质开仓机会)")
    pr(f"  价差 >= 10 bps 出现率: {pct_gt10:.1f}%  (极佳开仓机会)")
    pr(f"  价差 <= 3 bps 出现率: {pct_lt3:.1f}%  (平仓窗口)")
    pr(f"  总固定费用: {TOTAL_FEE:.1f} bps/往返")
    pr(f"  要求: 开仓价差 - 平仓价差 > {TOTAL_FEE:.1f} + 开平滑点 才有净利")

    for f in [lighter, drift]:
        await f.disconnect()

asyncio.run(main())
