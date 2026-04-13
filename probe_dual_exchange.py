#!/usr/bin/env python3
"""
双交易所联合模拟测试
完整模拟: Lighter Maker开仓 + Extended Taker对冲 → Extended Maker平仓

重点测试:
1. 价差出现时, Extended taker对冲可行性 (已有, 作为baseline)
2. 价差收敛时, Extended maker平仓的实际成交速度
3. 如果maker没成交, 价差是否会反转导致更大亏损
4. 对比: IOC平仓 vs Maker平仓 的成本差异
"""
import asyncio
import json
import time
import statistics
from dataclasses import dataclass, field
from typing import Optional, List

import websockets

LIGHTER_WS = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_MARKET_INDEX = 1  # BTC
EXT_WS_BASE = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1"
EXT_SYMBOL = "BTC-USD"

OPEN_SPREAD_BPS = 3.0
CLOSE_SPREAD_BPS = 1.0
EXT_TAKER_FEE_BPS = 2.25
MAKER_TIMEOUT_S = 5.0
OUR_SIZE = 0.001
SAMPLE_SECONDS = 300


@dataclass
class SimTrade:
    open_time: float = 0
    open_spread: float = 0
    close_trigger_time: float = 0
    close_trigger_spread: float = 0
    maker_filled: bool = False
    maker_fill_time: float = 0
    maker_wait_s: float = 0
    ioc_fallback: bool = False
    ioc_spread_at_fallback: float = 0
    pnl_maker_bps: float = 0
    pnl_ioc_bps: float = 0


async def main():
    l_bid: Optional[float] = None
    l_ask: Optional[float] = None
    x_bid: Optional[float] = None
    x_ask: Optional[float] = None
    x_bid_qty: float = 0
    x_ask_qty: float = 0
    prev_x_bid_qty: float = 0
    prev_x_ask_qty: float = 0
    prev_x_bid: Optional[float] = None
    prev_x_ask: Optional[float] = None
    l_ts: float = 0
    x_ts: float = 0

    state = "IDLE"
    open_side = ""
    current_trade: Optional[SimTrade] = None
    maker_remaining = 0.0
    maker_price = 0.0

    completed_trades: List[SimTrade] = []
    spread_samples: List[float] = []
    t0 = time.time()

    l_ob_bids: dict = {}
    l_ob_asks: dict = {}
    l_first_snapshot = True

    async def on_lighter(raw):
        nonlocal l_bid, l_ask, l_ts, l_first_snapshot
        msg = json.loads(raw)

        if msg.get("type") in ("connected", "pong", None) and "order_book" not in msg:
            return

        ob = msg.get("order_book")
        if not ob:
            return

        if l_first_snapshot:
            l_ob_bids.clear()
            l_ob_asks.clear()
            l_first_snapshot = False

        for lv in ob.get("bids", []):
            try:
                p = float(lv["price"])
                q = float(lv["size"])
                if q <= 0:
                    l_ob_bids.pop(p, None)
                else:
                    l_ob_bids[p] = q
            except (KeyError, ValueError, TypeError):
                pass
        for lv in ob.get("asks", []):
            try:
                p = float(lv["price"])
                q = float(lv["size"])
                if q <= 0:
                    l_ob_asks.pop(p, None)
                else:
                    l_ob_asks[p] = q
            except (KeyError, ValueError, TypeError):
                pass

        if l_ob_bids:
            l_bid = max(l_ob_bids.keys())
        if l_ob_asks:
            l_ask = min(l_ob_asks.keys())
        l_ts = time.time()

    async def on_extended(raw):
        nonlocal x_bid, x_ask, x_bid_qty, x_ask_qty, x_ts
        nonlocal prev_x_bid, prev_x_ask, prev_x_bid_qty, prev_x_ask_qty
        msg = json.loads(raw)
        data = msg.get("data", {})
        bids = data.get("b", [])
        asks = data.get("a", [])
        if bids and asks:
            prev_x_bid = x_bid
            prev_x_ask = x_ask
            prev_x_bid_qty = x_bid_qty
            prev_x_ask_qty = x_ask_qty
            x_bid = float(bids[0]["p"])
            x_ask = float(asks[0]["p"])
            x_bid_qty = float(bids[0].get("c", bids[0].get("q", "0")))
            x_ask_qty = float(asks[0].get("c", asks[0].get("q", "0")))
            x_ts = time.time()

    async def lighter_ws():
        while time.time() - t0 < SAMPLE_SECONDS + 5:
            try:
                async with websockets.connect(LIGHTER_WS, ping_interval=20) as ws:
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": f"order_book/{LIGHTER_MARKET_INDEX}",
                    }))
                    while time.time() - t0 < SAMPLE_SECONDS + 5:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        await on_lighter(raw)
            except Exception:
                await asyncio.sleep(1)

    async def extended_ws():
        url = f"{EXT_WS_BASE}/orderbooks/{EXT_SYMBOL}?depth=1"
        while time.time() - t0 < SAMPLE_SECONDS + 5:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    while time.time() - t0 < SAMPLE_SECONDS + 5:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        await on_extended(raw)
            except Exception:
                await asyncio.sleep(1)

    async def strategy_loop():
        nonlocal state, open_side, current_trade, maker_remaining, maker_price

        await asyncio.sleep(3)
        print("双交易所连接就绪, 开始模拟...\n")

        tick_count = 0
        while time.time() - t0 < SAMPLE_SECONDS:
            await asyncio.sleep(0.05)

            if l_bid is None or x_bid is None:
                continue

            now = time.time()
            l_age = now - l_ts
            x_age = now - x_ts
            if l_age > 5 or x_age > 5:
                continue

            l_mid = (l_bid + l_ask) / 2
            x_mid = (x_bid + x_ask) / 2
            basis_bps = (l_mid - x_mid) / x_mid * 10000

            spread_samples.append(basis_bps)

            tick_count += 1
            if tick_count % 200 == 0:
                elapsed = now - t0
                print(f"  [{elapsed:.0f}s] basis={basis_bps:+.1f}bps  "
                      f"L={l_bid}/{l_ask}  X={x_bid}/{x_ask}  "
                      f"state={state}  trades={len(completed_trades)}")

            if state == "IDLE":
                sell_spread = (l_bid - x_ask) / x_ask * 10000
                buy_spread = (x_bid - l_ask) / l_ask * 10000

                if sell_spread >= OPEN_SPREAD_BPS:
                    state = "OPEN"
                    open_side = "sell"
                    current_trade = SimTrade(
                        open_time=now,
                        open_spread=sell_spread,
                    )
                elif buy_spread >= OPEN_SPREAD_BPS:
                    state = "OPEN"
                    open_side = "buy"
                    current_trade = SimTrade(
                        open_time=now,
                        open_spread=buy_spread,
                    )

            elif state == "OPEN":
                state = "CLOSING"
                current_trade.close_trigger_time = 0

            elif state == "CLOSING":
                if open_side == "sell":
                    close_spread = (l_ask - x_bid) / x_bid * 10000
                else:
                    close_spread = (x_ask - l_bid) / l_bid * 10000

                if close_spread <= CLOSE_SPREAD_BPS and current_trade.close_trigger_time == 0:
                    current_trade.close_trigger_time = now
                    current_trade.close_trigger_spread = close_spread
                    maker_remaining = OUR_SIZE

                    if open_side == "sell":
                        maker_price = x_bid
                    else:
                        maker_price = x_ask

                    state = "MAKER_WAIT"

            elif state == "MAKER_WAIT":
                wait_elapsed = now - current_trade.close_trigger_time

                filled = False
                if open_side == "sell":
                    if prev_x_bid is not None and x_bid is not None:
                        if x_bid == prev_x_bid and x_bid_qty < prev_x_bid_qty:
                            eaten = prev_x_bid_qty - x_bid_qty
                            maker_remaining -= eaten
                        elif x_bid != prev_x_bid and x_bid < prev_x_bid:
                            maker_remaining = 0
                    if maker_remaining <= 0:
                        filled = True
                else:
                    if prev_x_ask is not None and x_ask is not None:
                        if x_ask == prev_x_ask and x_ask_qty < prev_x_ask_qty:
                            eaten = prev_x_ask_qty - x_ask_qty
                            maker_remaining -= eaten
                        elif x_ask != prev_x_ask and x_ask > prev_x_ask:
                            maker_remaining = 0
                    if maker_remaining <= 0:
                        filled = True

                if filled:
                    current_trade.maker_filled = True
                    current_trade.maker_fill_time = now
                    current_trade.maker_wait_s = now - current_trade.close_trigger_time
                    current_trade.ioc_fallback = False
                    current_trade.pnl_maker_bps = current_trade.open_spread - EXT_TAKER_FEE_BPS - 0
                    current_trade.pnl_ioc_bps = current_trade.open_spread - EXT_TAKER_FEE_BPS - EXT_TAKER_FEE_BPS
                    completed_trades.append(current_trade)
                    state = "IDLE"
                    current_trade = None

                elif wait_elapsed >= MAKER_TIMEOUT_S:
                    if open_side == "sell":
                        fb_spread = (l_ask - x_bid) / x_bid * 10000
                    else:
                        fb_spread = (x_ask - l_bid) / l_bid * 10000
                    current_trade.maker_filled = False
                    current_trade.ioc_fallback = True
                    current_trade.maker_wait_s = wait_elapsed
                    current_trade.ioc_spread_at_fallback = fb_spread
                    current_trade.pnl_maker_bps = 0
                    current_trade.pnl_ioc_bps = current_trade.open_spread - EXT_TAKER_FEE_BPS - EXT_TAKER_FEE_BPS
                    completed_trades.append(current_trade)
                    state = "IDLE"
                    current_trade = None

    tasks = [
        asyncio.create_task(lighter_ws()),
        asyncio.create_task(extended_ws()),
        asyncio.create_task(strategy_loop()),
    ]

    await asyncio.gather(*tasks, return_exceptions=True)

    sep = "=" * 65
    elapsed = time.time() - t0
    print(f"\n{sep}")
    print(f"  双交易所联合模拟结果 ({elapsed:.0f}秒)")
    print(f"{sep}")

    if spread_samples:
        print(f"\n--- 价差统计 ---")
        print(f"  采样: {len(spread_samples)}次")
        print(f"  均值={statistics.mean(spread_samples):.2f}bps  "
              f"中位={statistics.median(spread_samples):.2f}bps")
        above5 = sum(1 for s in spread_samples if abs(s) >= OPEN_SPREAD_BPS)
        print(f"  |basis|>=5bps: {above5}/{len(spread_samples)} "
              f"({above5/len(spread_samples)*100:.1f}%)")

    if not completed_trades:
        print(f"\n  {elapsed:.0f}秒内没有完整的开仓→平仓交易")
        print(f"  可能是价差不够大或收敛太慢")
        return

    total = len(completed_trades)
    maker_filled = [t for t in completed_trades if t.maker_filled]
    ioc_fallback = [t for t in completed_trades if t.ioc_fallback]

    print(f"\n--- 交易统计 ---")
    print(f"  总轮数:       {total}")
    print(f"  Maker成交:    {len(maker_filled)} ({len(maker_filled)/total*100:.0f}%)")
    print(f"  IOC兜底:      {len(ioc_fallback)} ({len(ioc_fallback)/total*100:.0f}%)")

    if maker_filled:
        wait_times = [t.maker_wait_s for t in maker_filled]
        print(f"\n--- Maker成交速度 ---")
        print(f"  均值={statistics.mean(wait_times):.2f}s  "
              f"中位={statistics.median(wait_times):.2f}s  "
              f"最大={max(wait_times):.2f}s")
        w1 = sum(1 for w in wait_times if w <= 1) / len(wait_times) * 100
        w3 = sum(1 for w in wait_times if w <= 3) / len(wait_times) * 100
        w5 = sum(1 for w in wait_times if w <= 5) / len(wait_times) * 100
        print(f"  1秒内: {w1:.0f}%  3秒内: {w3:.0f}%  5秒内: {w5:.0f}%")

    if ioc_fallback:
        print(f"\n--- IOC兜底详情 ---")
        for t in ioc_fallback:
            print(f"  开仓spread={t.open_spread:.1f}bps → "
                  f"等{t.maker_wait_s:.1f}s超时 → "
                  f"兜底时spread={t.ioc_spread_at_fallback:.1f}bps")

    print(f"\n--- P&L对比 (每轮) ---")
    maker_pnls = [t.pnl_maker_bps for t in maker_filled]
    all_ioc_pnls = [t.pnl_ioc_bps for t in completed_trades]

    if maker_pnls:
        print(f"  Maker平仓:")
        print(f"    均值={statistics.mean(maker_pnls):+.2f}bps  "
              f"中位={statistics.median(maker_pnls):+.2f}bps")

    print(f"  全IOC (当前方案):")
    print(f"    均值={statistics.mean(all_ioc_pnls):+.2f}bps  "
          f"中位={statistics.median(all_ioc_pnls):+.2f}bps")

    hybrid_pnls = []
    for t in completed_trades:
        if t.maker_filled:
            hybrid_pnls.append(t.pnl_maker_bps)
        else:
            hybrid_pnls.append(t.pnl_ioc_bps)

    print(f"  混合方案 (Maker优先+IOC兜底):")
    print(f"    均值={statistics.mean(hybrid_pnls):+.2f}bps  "
          f"中位={statistics.median(hybrid_pnls):+.2f}bps")

    print(f"\n--- 总P&L (美元估算, 按{OUR_SIZE}BTC x $68500) ---")
    ref = OUR_SIZE * 68500
    all_ioc_dollar = sum(p / 10000 * ref for p in all_ioc_pnls)
    hybrid_dollar = sum(p / 10000 * ref for p in hybrid_pnls)
    print(f"  全IOC:   ${all_ioc_dollar:+.4f}")
    print(f"  混合:    ${hybrid_dollar:+.4f}")
    print(f"  差额:    ${hybrid_dollar - all_ioc_dollar:+.4f} (Maker方案多赚)")

    print(f"\n{sep}")
    print(f"  综合结论")
    print(f"{sep}")

    maker_rate = len(maker_filled) / total * 100 if total > 0 else 0
    if maker_rate >= 95:
        print(f"\n  >>> Maker平仓成功率 {maker_rate:.0f}%, 强烈推荐!")
        if maker_filled:
            med_wait = statistics.median([t.maker_wait_s for t in maker_filled])
            print(f"      中位等待 {med_wait:.1f}s, 几乎不需要IOC兜底")
    elif maker_rate >= 80:
        print(f"\n  >>> Maker平仓成功率 {maker_rate:.0f}%, 推荐使用(带IOC兜底)")
    elif maker_rate >= 50:
        print(f"\n  >>> Maker平仓成功率仅 {maker_rate:.0f}%, 需要谨慎评估")
    else:
        print(f"\n  >>> Maker平仓成功率低({maker_rate:.0f}%), 不建议使用")

    if hybrid_dollar > all_ioc_dollar:
        diff = hybrid_dollar - all_ioc_dollar
        print(f"      混合方案比纯IOC每{total}轮多赚 ${diff:.4f}")


if __name__ == "__main__":
    print(f"双交易所联合模拟")
    print(f"开仓条件: |spread| >= {OPEN_SPREAD_BPS}bps")
    print(f"平仓条件: |spread| <= {CLOSE_SPREAD_BPS}bps")
    print(f"Maker超时: {MAKER_TIMEOUT_S}s → IOC兜底")
    print(f"采样时间: {SAMPLE_SECONDS}s\n")
    asyncio.run(main())
