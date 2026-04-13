#!/usr/bin/env python3
"""
Extended Maker平仓可行性深度测试
核心问题: 在BBO挂一个0.001 BTC的maker单, 多久能被对手方吃掉?

检测方法:
1. 监控BBO量的减少 → 说明有人在吃单(taker activity)
2. 监控价格跳动 → 说明某一档被吃完
3. 模拟: 如果我在best_bid挂了0.001, 按当前吃单速度多久能成交
4. 统计不同超时窗口下的成交概率
"""
import asyncio
import json
import time
import statistics

import websockets

WS_BASE = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1"
SYMBOL = "BTC-USD"
SAMPLE_SECONDS = 180
OUR_SIZE = 0.001


async def main():
    url = f"{WS_BASE}/orderbooks/{SYMBOL}?depth=1"
    print(f"连接 {url} ...")
    print(f"采样 {SAMPLE_SECONDS} 秒, 监控吃单活跃度\n")

    prev_bid_p = prev_ask_p = None
    prev_bid_q = prev_ask_q = None
    prev_ts = None

    bid_fills = []
    ask_fills = []
    bid_fill_sizes = []
    ask_fill_sizes = []

    bid_level_eaten = 0
    ask_level_eaten = 0

    sim_bid_orders = []
    sim_ask_orders = []
    sim_bid_remaining = None
    sim_ask_remaining = None
    sim_bid_place_time = None
    sim_ask_place_time = None
    sim_bid_results = []
    sim_ask_results = []

    samples = 0
    t0 = time.time()

    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
        while time.time() - t0 < SAMPLE_SECONDS:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                continue

            now = time.time()
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

            samples += 1

            if prev_bid_p is not None:
                dt = now - prev_ts

                if bp == prev_bid_p and bq < prev_bid_q:
                    eaten = prev_bid_q - bq
                    bid_fills.append(now)
                    bid_fill_sizes.append(eaten)

                    if sim_bid_remaining is not None and sim_bid_remaining > 0:
                        sim_bid_remaining -= eaten
                        if sim_bid_remaining <= 0:
                            fill_time = now - sim_bid_place_time
                            sim_bid_results.append(fill_time)
                            sim_bid_remaining = OUR_SIZE
                            sim_bid_place_time = now

                if bp != prev_bid_p:
                    if bp < prev_bid_p:
                        bid_level_eaten += 1
                        bid_fills.append(now)
                        bid_fill_sizes.append(prev_bid_q)
                        if sim_bid_remaining is not None:
                            sim_bid_remaining = 0
                            fill_time = now - sim_bid_place_time
                            sim_bid_results.append(fill_time)
                    sim_bid_remaining = OUR_SIZE
                    sim_bid_place_time = now

                if ap == prev_ask_p and aq < prev_ask_q:
                    eaten = prev_ask_q - aq
                    ask_fills.append(now)
                    ask_fill_sizes.append(eaten)

                    if sim_ask_remaining is not None and sim_ask_remaining > 0:
                        sim_ask_remaining -= eaten
                        if sim_ask_remaining <= 0:
                            fill_time = now - sim_ask_place_time
                            sim_ask_results.append(fill_time)
                            sim_ask_remaining = OUR_SIZE
                            sim_ask_place_time = now

                if ap != prev_ask_p:
                    if ap > prev_ask_p:
                        ask_level_eaten += 1
                        ask_fills.append(now)
                        ask_fill_sizes.append(prev_ask_q)
                        if sim_ask_remaining is not None:
                            sim_ask_remaining = 0
                            fill_time = now - sim_ask_place_time
                            sim_ask_results.append(fill_time)
                    sim_ask_remaining = OUR_SIZE
                    sim_ask_place_time = now
            else:
                sim_bid_remaining = OUR_SIZE
                sim_ask_remaining = OUR_SIZE
                sim_bid_place_time = now
                sim_ask_place_time = now

            prev_bid_p, prev_ask_p = bp, ap
            prev_bid_q, prev_ask_q = bq, aq
            prev_ts = now

            elapsed = time.time() - t0
            if samples % 500 == 0:
                bf = len(bid_fills)
                af = len(ask_fills)
                print(f"  [{elapsed:.0f}s] {samples}样本  "
                      f"bid吃单={bf}次  ask吃单={af}次  "
                      f"bid模拟成交={len(sim_bid_results)}次  "
                      f"ask模拟成交={len(sim_ask_results)}次")

    elapsed = time.time() - t0
    sep = "=" * 65

    print(f"\n{sep}")
    print(f"  Extended {SYMBOL} Maker平仓可行性深度测试")
    print(f"  {samples} 样本, {elapsed:.0f} 秒")
    print(f"{sep}")

    print(f"\n--- Bid侧 (你要close short → 挂buy在best_bid) ---")
    if bid_fills:
        intervals = [bid_fills[i] - bid_fills[i-1]
                     for i in range(1, len(bid_fills))]
        print(f"  吃单事件: {len(bid_fills)}次 ({len(bid_fills)/elapsed:.2f}次/秒)")
        print(f"  吃单量:   均值={statistics.mean(bid_fill_sizes):.4f} BTC  "
              f"中位={statistics.median(bid_fill_sizes):.4f}  "
              f"最大={max(bid_fill_sizes):.4f}")
        print(f"  总吃单量: {sum(bid_fill_sizes):.4f} BTC ({sum(bid_fill_sizes)/elapsed*60:.2f} BTC/分钟)")
        print(f"  整档被吃: {bid_level_eaten}次")
        if intervals:
            print(f"  吃单间隔: 均值={statistics.mean(intervals)*1000:.0f}ms  "
                  f"中位={statistics.median(intervals)*1000:.0f}ms  "
                  f"最大={max(intervals)*1000:.0f}ms")
    else:
        print(f"  没有检测到吃单活动!")

    print(f"\n--- Ask侧 (你要close long → 挂sell在best_ask) ---")
    if ask_fills:
        intervals = [ask_fills[i] - ask_fills[i-1]
                     for i in range(1, len(ask_fills))]
        print(f"  吃单事件: {len(ask_fills)}次 ({len(ask_fills)/elapsed:.2f}次/秒)")
        print(f"  吃单量:   均值={statistics.mean(ask_fill_sizes):.4f} BTC  "
              f"中位={statistics.median(ask_fill_sizes):.4f}  "
              f"最大={max(ask_fill_sizes):.4f}")
        print(f"  总吃单量: {sum(ask_fill_sizes):.4f} BTC ({sum(ask_fill_sizes)/elapsed*60:.2f} BTC/分钟)")
        print(f"  整档被吃: {ask_level_eaten}次")
        if intervals:
            print(f"  吃单间隔: 均值={statistics.mean(intervals)*1000:.0f}ms  "
                  f"中位={statistics.median(intervals)*1000:.0f}ms  "
                  f"最大={max(intervals)*1000:.0f}ms")
    else:
        print(f"  没有检测到吃单活动!")

    print(f"\n{sep}")
    print(f"  模拟挂单成交测试 (假设在BBO挂 {OUR_SIZE} BTC)")
    print(f"{sep}")

    print(f"\n--- Bid侧模拟 (close short: 挂buy) ---")
    if sim_bid_results:
        print(f"  模拟成交次数: {len(sim_bid_results)}")
        print(f"  成交时间: 均值={statistics.mean(sim_bid_results):.1f}s  "
              f"中位={statistics.median(sim_bid_results):.1f}s  "
              f"最小={min(sim_bid_results):.1f}s  "
              f"最大={max(sim_bid_results):.1f}s")
        within_3s = sum(1 for t in sim_bid_results if t <= 3) / len(sim_bid_results) * 100
        within_5s = sum(1 for t in sim_bid_results if t <= 5) / len(sim_bid_results) * 100
        within_10s = sum(1 for t in sim_bid_results if t <= 10) / len(sim_bid_results) * 100
        within_30s = sum(1 for t in sim_bid_results if t <= 30) / len(sim_bid_results) * 100
        print(f"  3秒内成交:  {within_3s:.0f}%")
        print(f"  5秒内成交:  {within_5s:.0f}%")
        print(f"  10秒内成交: {within_10s:.0f}%")
        print(f"  30秒内成交: {within_30s:.0f}%")
    else:
        pending_time = elapsed - (sim_bid_place_time - t0) if sim_bid_place_time else elapsed
        print(f"  整个{elapsed:.0f}秒内没有一次模拟成交! (已等待{pending_time:.0f}秒)")
        print(f"  剩余未成交: {sim_bid_remaining:.5f} BTC")

    print(f"\n--- Ask侧模拟 (close long: 挂sell) ---")
    if sim_ask_results:
        print(f"  模拟成交次数: {len(sim_ask_results)}")
        print(f"  成交时间: 均值={statistics.mean(sim_ask_results):.1f}s  "
              f"中位={statistics.median(sim_ask_results):.1f}s  "
              f"最小={min(sim_ask_results):.1f}s  "
              f"最大={max(sim_ask_results):.1f}s")
        within_3s = sum(1 for t in sim_ask_results if t <= 3) / len(sim_ask_results) * 100
        within_5s = sum(1 for t in sim_ask_results if t <= 5) / len(sim_ask_results) * 100
        within_10s = sum(1 for t in sim_ask_results if t <= 10) / len(sim_ask_results) * 100
        within_30s = sum(1 for t in sim_ask_results if t <= 30) / len(sim_ask_results) * 100
        print(f"  3秒内成交:  {within_3s:.0f}%")
        print(f"  5秒内成交:  {within_5s:.0f}%")
        print(f"  10秒内成交: {within_10s:.0f}%")
        print(f"  30秒内成交: {within_30s:.0f}%")
    else:
        pending_time = elapsed - (sim_ask_place_time - t0) if sim_ask_place_time else elapsed
        print(f"  整个{elapsed:.0f}秒内没有一次模拟成交! (已等待{pending_time:.0f}秒)")
        print(f"  剩余未成交: {sim_ask_remaining:.5f} BTC")

    print(f"\n{sep}")
    print(f"  综合结论")
    print(f"{sep}")

    all_sim = sim_bid_results + sim_ask_results
    if all_sim:
        avg_fill = statistics.mean(all_sim)
        med_fill = statistics.median(all_sim)
        w5 = sum(1 for t in all_sim if t <= 5) / len(all_sim) * 100
        w10 = sum(1 for t in all_sim if t <= 10) / len(all_sim) * 100

        if med_fill <= 3 and w5 >= 80:
            print(f"\n  >>> 强烈推荐 Maker平仓!")
            print(f"      中位成交={med_fill:.1f}s, 5秒内{w5:.0f}%成交")
            print(f"      建议: 超时5秒回退IOC, 几乎不会触发")
        elif med_fill <= 10 and w10 >= 70:
            print(f"\n  >>> 推荐 Maker平仓 (带IOC回退)")
            print(f"      中位成交={med_fill:.1f}s, 10秒内{w10:.0f}%成交")
            print(f"      建议: 超时10秒回退IOC")
        elif med_fill <= 30:
            print(f"\n  >>> 谨慎推荐")
            print(f"      中位成交={med_fill:.1f}s, 较慢")
            print(f"      风险: 等待期间价差可能反转")
            print(f"      建议: 超时5秒就回退IOC, 牺牲部分成功率换安全")
        else:
            print(f"\n  >>> 不推荐 Maker平仓")
            print(f"      成交太慢(均值{avg_fill:.0f}s), 价差风险太大")
    else:
        total_fills = len(bid_fills) + len(ask_fills)
        if total_fills == 0:
            print(f"\n  >>> 不推荐! 完全没有检测到吃单活动")
            print(f"      Extended BTC市场可能流动性不足")
        else:
            print(f"\n  >>> 需要更长时间测试")
            print(f"      检测到{total_fills}次吃单, 但量不足以填满{OUR_SIZE} BTC模拟单")


if __name__ == "__main__":
    asyncio.run(main())
