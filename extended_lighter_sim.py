#!/usr/bin/env python3
"""
Extended + Lighter 价差收敛模拟（务实版）

与 feeds/convergence_tracker.py 一致：
  |Mid 价差| ≥ 开仓阈值 → 按 VWAP 虚拟开仓
  |Mid 价差| ≤ 平仓阈值 → 按 VWAP 虚拟平仓
  净利 = 毛利(bps) − 往返 taker 费 = 毛利 − 2×(Lighter_taker + Extended_taker)

阶段：
  1) 实时采集：连接两所 WebSocket，按间隔记录 Mid + 四向 VWAP（名义 size）
  2) 网格回测：对采集序列扫描多组 (开仓bps, 平仓bps)，输出笔数/均净利/胜率等

用法:
  python extended_lighter_sim.py -s ETH --samples 2000 --interval 1
  python extended_lighter_sim.py -s ETH --replay data/eth_ext_lighter.jsonl
  python extended_lighter_sim.py -s BTC --samples 500 --funding-drag-bps-hour 0.5

说明：资金费为简化项 — 用 --funding-drag-bps-hour 按持仓时长从净利中扣除 |小时bps|×(持仓小时)。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# 项目根目录
sys.path.insert(0, str(Path(__file__).resolve().parent))

from feeds.base_feed import OrderBookLevel, OrderBookSnapshot
from feeds.existing_feeds import ExtendedFeed
from feeds.lighter_feed import LighterFeed


@dataclass
class Sample:
    ts: float
    l_mid: float
    x_mid: float
    lbuy: float
    lsell: float
    xbuy: float
    xsell: float
    qty: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts": self.ts,
            "l_mid": self.l_mid,
            "x_mid": self.x_mid,
            "lbuy": self.lbuy,
            "lsell": self.lsell,
            "xbuy": self.xbuy,
            "xsell": self.xsell,
            "qty": self.qty,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Sample":
        return cls(
            ts=float(d["ts"]),
            l_mid=float(d["l_mid"]),
            x_mid=float(d["x_mid"]),
            lbuy=float(d["lbuy"]),
            lsell=float(d["lsell"]),
            xbuy=float(d["xbuy"]),
            xsell=float(d["xsell"]),
            qty=float(d["qty"]),
        )


@dataclass
class SimTrade:
    open_ts: float
    close_ts: float
    wait_sec: float
    open_abs_spread_bps: float
    close_abs_spread_bps: float
    gross_bps: float
    fee_rt_bps: float
    funding_cost_bps: float
    net_bps: float


def _snap_from_sample(
    exchange: str,
    symbol: str,
    mid: float,
    lbuy: float,
    lsell: float,
    qty: float,
) -> OrderBookSnapshot:
    """
    构造单档簿：VWAP 与记录值一致，Mid 与记录 mid 一致（用于阈值判断）。
    """
    eps = max(mid * 1e-7, 1e-9)
    snap = OrderBookSnapshot(
        exchange=exchange,
        symbol=symbol,
        best_bid=mid - eps,
        best_ask=mid + eps,
        bids=[OrderBookLevel(lsell, qty)],
        asks=[OrderBookLevel(lbuy, qty)],
        connected=True,
        timestamp=time.time(),
    )
    return snap


def simulate_convergence(
    samples: List[Sample],
    open_thresh_bps: float,
    close_thresh_bps: float,
    fee_rt_bps: float,
    funding_drag_bps_per_hour: float = 0.0,
    exchange_name: str = "Extended",
    symbol: str = "ETH",
) -> List[SimTrade]:
    """
    与 PerExchangeConvergenceTracker 相同的状态机与毛利公式。
    """
    open_bps = max(0.1, float(open_thresh_bps))
    close_bps = max(0.0, float(close_thresh_bps))
    if close_bps >= open_bps:
        close_bps = max(0.0, open_bps - 0.5)

    pos: Optional[Dict[str, Any]] = None
    trades: List[SimTrade] = []

    for s in samples:
        if s.l_mid <= 0 or s.qty <= 0:
            continue

        ls = _snap_from_sample("Lighter", symbol, s.l_mid, s.lbuy, s.lsell, s.qty)
        es = _snap_from_sample(exchange_name, symbol, s.x_mid, s.xbuy, s.xsell, s.qty)

        mid_spread_bps = (es.mid - ls.mid) / ls.mid * 10_000.0
        abs_sp = abs(mid_spread_bps)

        lbuy, lsell, xbuy, xsell = s.lbuy, s.lsell, s.xbuy, s.xsell
        vwap_ok = all(v > 0 for v in (lbuy, lsell, xbuy, xsell))

        if pos is None:
            if vwap_ok and abs_sp >= open_bps:
                x_rich = mid_spread_bps >= 0
                pos = {
                    "ts": s.ts,
                    "l_mid": s.l_mid,
                    "mid_spread_bps": mid_spread_bps,
                    "abs_spread_bps": abs_sp,
                    "x_rich": x_rich,
                    "lbuy": lbuy,
                    "lsell": lsell,
                    "xbuy": xbuy,
                    "xsell": xsell,
                    "qty": s.qty,
                }
        else:
            if vwap_ok and abs_sp <= close_bps:
                o = pos
                cl_buy_l, cl_sell_l = lbuy, lsell
                cl_buy_x, cl_sell_x = xbuy, xsell
                if o["x_rich"]:
                    gross_per_unit = (o["xsell"] - o["lbuy"]) + (cl_sell_l - cl_buy_x)
                else:
                    gross_per_unit = (o["lsell"] - o["xbuy"]) + (cl_sell_x - cl_buy_l)

                ref_mid = o["l_mid"] if o["l_mid"] > 0 else s.l_mid
                gross_bps = gross_per_unit / ref_mid * 10_000.0
                wait_sec = s.ts - o["ts"]
                fh = max(0.0, wait_sec) / 3600.0
                funding_cost = funding_drag_bps_per_hour * fh
                net_bps = gross_bps - fee_rt_bps - funding_cost

                trades.append(
                    SimTrade(
                        open_ts=o["ts"],
                        close_ts=s.ts,
                        wait_sec=wait_sec,
                        open_abs_spread_bps=o["abs_spread_bps"],
                        close_abs_spread_bps=abs_sp,
                        gross_bps=gross_bps,
                        fee_rt_bps=fee_rt_bps,
                        funding_cost_bps=funding_cost,
                        net_bps=net_bps,
                    )
                )
                pos = None

    return trades


def grid_search(
    samples: List[Sample],
    fee_rt_bps: float,
    funding_drag_bps_per_hour: float,
    symbol: str,
) -> List[Dict[str, Any]]:
    open_grid = [3, 5, 7, 10, 12, 15, 20, 25, 30]
    close_grid = [0, 1, 2, 3, 4, 5]
    rows: List[Dict[str, Any]] = []

    if not samples:
        return rows

    t0, t1 = samples[0].ts, samples[-1].ts
    dur_h = max((t1 - t0) / 3600.0, 1e-6)

    for ob in open_grid:
        for cb in close_grid:
            if cb >= ob:
                continue
            tr = simulate_convergence(
                samples, ob, cb, fee_rt_bps, funding_drag_bps_per_hour, symbol=symbol
            )
            if not tr:
                rows.append(
                    {
                        "open_bps": ob,
                        "close_bps": cb,
                        "count": 0,
                        "trades_per_h": 0.0,
                        "avg_gross_bps": None,
                        "avg_net_bps": None,
                        "median_net_bps": None,
                        "win_rate_pct": None,
                        "avg_wait_sec": None,
                        "sum_net_bps": None,
                    }
                )
                continue

            nets = [x.net_bps for x in tr]
            gross = [x.gross_bps for x in tr]
            waits = [x.wait_sec for x in tr]
            wins = sum(1 for x in nets if x > 0)

            rows.append(
                {
                    "open_bps": ob,
                    "close_bps": cb,
                    "count": len(tr),
                    "trades_per_h": len(tr) / dur_h,
                    "avg_gross_bps": round(statistics.mean(gross), 3),
                    "avg_net_bps": round(statistics.mean(nets), 3),
                    "median_net_bps": round(statistics.median(nets), 3),
                    "win_rate_pct": round(100.0 * wins / len(tr), 1),
                    "avg_wait_sec": round(statistics.mean(waits), 1),
                    "sum_net_bps": round(sum(nets), 3),
                }
            )

    rows.sort(key=lambda r: (r.get("avg_net_bps") or -1e9), reverse=True)
    return rows


def save_jsonl(path: Path, samples: List[Sample]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for s in samples:
            f.write(json.dumps(s.to_dict(), ensure_ascii=False) + "\n")


def load_jsonl(path: Path) -> List[Sample]:
    out: List[Sample] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(Sample.from_dict(json.loads(line)))
    return out


async def collect_samples(
    symbol: str,
    trade_size_usd: float,
    n: int,
    interval_sec: float,
    lighter_market: Optional[int],
    warmup_sec: float = 5.0,
) -> Tuple[List[Sample], float]:
    lighter = (
        LighterFeed(symbol, market_index=lighter_market)
        if lighter_market is not None
        else LighterFeed(symbol)
    )
    extended = ExtendedFeed(symbol)

    bg = [
        asyncio.create_task(lighter.connect()),
        asyncio.create_task(extended.connect()),
    ]
    await asyncio.sleep(warmup_sec)

    samples: List[Sample] = []
    max_attempts = max(n * 80, 10_000)
    attempts = 0

    try:
        while len(samples) < n:
            attempts += 1
            if attempts > max_attempts:
                raise RuntimeError(
                    f"采集超时：{max_attempts} 次尝试后仍不足 {n} 条有效快照 "
                    f"（可能深度不足以覆盖名义 ${trade_size_usd:,.0f}，请减小 --size）"
                )

            ls = lighter.snapshot
            es = extended.snapshot
            if not ls.mid or not es.mid:
                await asyncio.sleep(interval_sec)
                continue

            qty = trade_size_usd / ls.mid
            lbuy = ls.vwap_buy(qty)
            lsell = ls.vwap_sell(qty)
            xbuy = es.vwap_buy(qty)
            xsell = es.vwap_sell(qty)

            if not all(v is not None and v > 0 for v in (lbuy, lsell, xbuy, xsell)):
                await asyncio.sleep(interval_sec)
                continue

            samples.append(
                Sample(
                    ts=time.time(),
                    l_mid=float(ls.mid),
                    x_mid=float(es.mid),
                    lbuy=float(lbuy),
                    lsell=float(lsell),
                    xbuy=float(xbuy),
                    xsell=float(xsell),
                    qty=float(qty),
                )
            )
            if len(samples) % max(1, n // 10) == 0:
                print(f"  采集 {len(samples)}/{n} ...", flush=True)

            await asyncio.sleep(interval_sec)
    finally:
        await lighter.disconnect()
        await extended.disconnect()
        for t in bg:
            t.cancel()
        await asyncio.gather(*bg, return_exceptions=True)

    fee_rt = 2.0 * (lighter.TAKER_FEE_BPS + extended.TAKER_FEE_BPS)
    return samples, fee_rt


def print_summary(samples: List[Sample], fee_rt_bps: float) -> None:
    if len(samples) < 10:
        return
    mids_sp = [
        (s.x_mid - s.l_mid) / s.l_mid * 10_000.0 for s in samples if s.l_mid > 0
    ]
    print("\n" + "═" * 72)
    print("  Extended − Lighter ｜Mid 价差｜ 分布（bps，符号 = Extended 相对 Lighter）")
    print("═" * 72)
    print(f"  样本数: {len(mids_sp)}")
    print(f"  均值: {statistics.mean(mids_sp):+.2f}  中位: {statistics.median(mids_sp):+.2f}")
    sd = statistics.stdev(mids_sp) if len(mids_sp) > 1 else 0.0
    print(f"  标准差: {sd:.2f}")
    print(f"  范围: [{min(mids_sp):+.2f}, {max(mids_sp):+.2f}]")
    print(f"  往返 taker 费（模型）: {fee_rt_bps:.2f} bps  (= 2×(Lighter+Extended) taker)")


def print_grid_table(rows: List[Dict[str, Any]], fee_rt_bps: float, funding_drag: float) -> None:
    print("\n" + "═" * 96)
    print(
        f"  网格回测  费 {fee_rt_bps:.1f} bps"
        + (f"  资金费拖累 {funding_drag:.2f} bps/h × 持仓时长" if funding_drag > 0 else "")
    )
    print("═" * 96)
    hdr = (
        f"  {'开仓≥':>6} {'平仓≤':>6} | {'笔数':>5} {'笔/h':>7} | "
        f"{'均毛利':>9} {'均净利':>9} {'中位净':>9} | {'胜率%':>7} {'均等待s':>8}"
    )
    print(hdr)
    print("  " + "─" * 92)

    for r in rows[:24]:
        if r["count"] == 0:
            print(
                f"  {r['open_bps']:6.0f} {r['close_bps']:6.0f} | "
                f"{'0':>5} {'—':>7} | {'—':>9} {'—':>9} {'—':>9} | {'—':>7} {'—':>8}"
            )
            continue
        print(
            f"  {r['open_bps']:6.0f} {r['close_bps']:6.0f} | "
            f"{r['count']:5d} {r['trades_per_h']:7.2f} | "
            f"{r['avg_gross_bps']:+9.2f} {r['avg_net_bps']:+9.2f} {r['median_net_bps']:+9.2f} | "
            f"{r['win_rate_pct']:7.1f} {r['avg_wait_sec']:8.1f}"
        )

    best = next((x for x in rows if x["count"] > 0), None)
    if best and best.get("avg_net_bps") is not None:
        print("\n  ▶ 按「平均净利 bps」排序的第一名参数：")
        print(
            f"     开仓≥ {best['open_bps']:.0f} bps, 平仓≤ {best['close_bps']:.0f} bps  |  "
            f"均净利 {best['avg_net_bps']:+.2f} bps, 胜率 {best['win_rate_pct']:.1f}%, "
            f"约 {best['trades_per_h']:.2f} 笔/小时"
        )


async def async_main(args: argparse.Namespace) -> None:
    symbol = args.symbol.upper()
    funding_drag = max(0.0, float(args.funding_drag_bps_hour))

    if args.replay:
        path = Path(args.replay)
        if not path.is_file():
            print(f"文件不存在: {path}")
            return
        samples = load_jsonl(path)
        fee_rt = 2.0 * (LighterFeed.TAKER_FEE_BPS + ExtendedFeed.TAKER_FEE_BPS)
        print(f"已从 {path} 加载 {len(samples)} 条样本")
    else:
        print(
            f"采集 Extended + Lighter  {symbol}  名义 ${args.size:,.0f}  "
            f"共 {args.samples} 点  间隔 {args.interval}s ..."
        )
        samples, fee_rt = await collect_samples(
            symbol=symbol,
            trade_size_usd=args.size,
            n=args.samples,
            interval_sec=args.interval,
            lighter_market=args.lighter_market,
            warmup_sec=args.warmup,
        )
        print(f"采集完成，有效样本 {len(samples)}，模型往返费 {fee_rt:.2f} bps")
        if args.output:
            outp = Path(args.output)
            save_jsonl(outp, samples)
            print(f"已写入 {outp}")

    if len(samples) < 30:
        print("样本过少（<30），回测意义不大")
        return

    print_summary(samples, fee_rt)
    rows = grid_search(samples, fee_rt, funding_drag, symbol=symbol)
    print_grid_table(rows, fee_rt, funding_drag)

    pos = [r for r in rows if r["count"] > 0 and (r.get("avg_net_bps") or 0) > 0]
    if not pos:
        print(
            "\n  在当前样本与参数网格下，未发现平均净利为正的阈值组合。"
            " 可加长采集时间、换时段或缩小名义以降低 VWAP 冲击后再试。"
        )


def main() -> None:
    p = argparse.ArgumentParser(description="Extended + Lighter VWAP 收敛模拟")
    p.add_argument("-s", "--symbol", default="ETH")
    p.add_argument("--size", type=float, default=10_000.0, help="名义 USD（用于 VWAP 数量）")
    p.add_argument("--samples", type=int, default=2000, help="采集快照条数")
    p.add_argument("--interval", type=float, default=1.0, help="采集间隔（秒）")
    p.add_argument("--warmup", type=float, default=5.0, help="连接后等待秒数")
    p.add_argument("--lighter-market", type=int, default=None)
    p.add_argument("-o", "--output", default=None, help="保存 JSONL 路径")
    p.add_argument("--replay", default=None, help="从 JSONL 回测，不再采集")
    p.add_argument(
        "--funding-drag-bps-hour",
        type=float,
        default=0.0,
        help="保守资金费：每小时从净利扣除此 bps（绝对值，默认 0）",
    )
    args = p.parse_args()
    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        print("\n已中断。")


if __name__ == "__main__":
    main()
