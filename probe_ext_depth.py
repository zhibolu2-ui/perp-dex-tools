#!/usr/bin/env python3
"""
Extended 订单簿深度探测器
分析挂单(Maker)平仓的可行性：价差、深度、价格稳定性
仅使用 depth=1 WS (Extended仅支持depth=1)
"""
import asyncio
import json
import time
import statistics

import websockets

WS_BASE = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1"
SYMBOL = "BTC-USD"
SAMPLE_SECONDS = 90


async def main():
    url = f"{WS_BASE}/orderbooks/{SYMBOL}?depth=1"
    print(f"连接 {url} ...")
    print(f"采样 {SAMPLE_SECONDS} 秒\n")

    spreads = []
    bbo_changes = 0
    prev_bid = prev_ask = None
    bid_qtys = []
    ask_qtys = []
    bid_prices = []
    ask_prices = []
    samples = 0
    t0 = time.time()

    price_durations = []
    last_change_t = t0

    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
        while time.time() - t0 < SAMPLE_SECONDS:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                continue

            msg = json.loads(raw)
            data = msg.get("data", {})
            bids = data.get("b", [])
            asks = data.get("a", [])
            if not bids or not asks:
                continue

            bp = float(bids[0]["p"])
            ap = float(asks[0]["p"])
            bq = float(bids[0].get("c", bids[0].get("q", "0")))
            aq = float(asks[0].get("c", asks[0].get("q", "0")))

            if bp <= 0 or ap <= 0:
                continue

            mid = (bp + ap) / 2
            spread_bps = (ap - bp) / mid * 10000
            spreads.append(spread_bps)
            bid_qtys.append(bq)
            ask_qtys.append(aq)
            bid_prices.append(bp)
            ask_prices.append(ap)

            if prev_bid is not None and (bp != prev_bid or ap != prev_ask):
                bbo_changes += 1
                now = time.time()
                price_durations.append(now - last_change_t)
                last_change_t = now
            prev_bid, prev_ask = bp, ap
            samples += 1

            elapsed = time.time() - t0
            if samples % 300 == 0:
                print(f"  [{elapsed:.0f}s] {samples}样本 spread={spread_bps:.2f}bps "
                      f"bid={bp}({bq:.3f}) ask={ap}({aq:.3f})")

    elapsed = time.time() - t0
    sep = "=" * 65
    print(f"\n{sep}")
    print(f"  Extended {SYMBOL} 流动性分析 ({samples}样本, {elapsed:.0f}秒)")
    print(f"{sep}")

    if not spreads:
        print("没有收到数据")
        return

    print(f"\n--- 价差 ---")
    print(f"  均值={statistics.mean(spreads):.2f}bps  中位={statistics.median(spreads):.2f}bps")
    print(f"  最小={min(spreads):.2f}bps  最大={max(spreads):.2f}bps")
    if len(spreads) > 1:
        print(f"  标准差={statistics.stdev(spreads):.2f}bps")

    print(f"\n--- BBO稳定性 ---")
    print(f"  BBO变化: {bbo_changes}次 ({bbo_changes / elapsed:.2f}次/秒)")
    if price_durations:
        print(f"  价格持续时间: 均值={statistics.mean(price_durations)*1000:.0f}ms  "
              f"中位={statistics.median(price_durations)*1000:.0f}ms  "
              f"最长={max(price_durations)*1000:.0f}ms")
    unique_bids = len(set(bid_prices))
    unique_asks = len(set(ask_prices))
    print(f"  唯一bid价格: {unique_bids}个  唯一ask价格: {unique_asks}个")

    print(f"\n--- 最优档量 (BTC) ---")
    print(f"  Bid: 均值={statistics.mean(bid_qtys):.3f}  "
          f"中位={statistics.median(bid_qtys):.3f}  "
          f"最小={min(bid_qtys):.3f}  最大={max(bid_qtys):.3f}")
    print(f"  Ask: 均值={statistics.mean(ask_qtys):.3f}  "
          f"中位={statistics.median(ask_qtys):.3f}  "
          f"最小={min(ask_qtys):.3f}  最大={max(ask_qtys):.3f}")

    our_size = 0.001
    avg_spread = statistics.mean(spreads)
    avg_mid = statistics.mean([(b + a) / 2 for b, a in zip(bid_prices, ask_prices)])
    changes_per_sec = bbo_changes / elapsed

    print(f"\n{'='*65}")
    print(f"  Maker平仓可行性评估 (单笔 {our_size} BTC)")
    print(f"{'='*65}")

    print(f"\n  1) 费用对比:")
    print(f"     IOC(Taker)费:  2.25 bps = ${2.25 / 10000 * our_size * avg_mid:.4f}/笔")
    print(f"     Maker挂单费:   0 bps")
    print(f"     可省:          2.25 bps = ${2.25 / 10000 * our_size * avg_mid:.4f}/笔")

    fills_ok = sum(1 for q in bid_qtys if q >= our_size)
    fill_pct = fills_ok / len(bid_qtys) * 100
    print(f"\n  2) 流动性覆盖: {fill_pct:.1f}% 时间 best_bid量 >= {our_size} BTC")

    if changes_per_sec < 1:
        risk = "低风险 — 价格非常稳定, 挂单几乎不会被跳过"
    elif changes_per_sec < 5:
        risk = "中风险 — 偶有变动, 可设超时回退IOC"
    else:
        risk = "高风险 — 频繁跳动, Maker可能miss"
    print(f"\n  3) 价格变动: {risk} ({changes_per_sec:.2f}次/秒)")

    print(f"\n  4) 挂单等待时间估算:")
    if price_durations:
        med_dur = statistics.median(price_durations)
        print(f"     价格中位持续={med_dur*1000:.0f}ms")
        if med_dur > 1:
            print(f"     Extended价格经常>1秒不变, Maker挂单有充足时间被对手方吃到")
        else:
            print(f"     价格变动较快, Maker可能需要频繁更新")

    print(f"\n--- 总结 ---")
    maker_benefit = 2.25
    if fill_pct > 95 and changes_per_sec < 2:
        print(f"  >>> 强烈推荐 Maker平仓!")
        print(f"      流动性充足({fill_pct:.0f}%), 价格稳定({changes_per_sec:.1f}次/秒)")
        print(f"      每笔省 2.25bps, 无额外滑点风险")
        print(f"      建议: 平仓时先尝试Maker挂单, 超时N秒后回退IOC")
    elif fill_pct > 80:
        print(f"  >>> 推荐 Maker平仓 (带IOC回退)")
        print(f"      流动性基本充足, 但需设置超时保护")
    else:
        print(f"  >>> 不推荐 — 流动性或稳定性不足, 保持IOC")


if __name__ == "__main__":
    asyncio.run(main())
