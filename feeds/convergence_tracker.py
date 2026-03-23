"""
价差收敛策略模拟：|Mid 价差| 扩大到阈值时「虚拟开仓」，收窄到阈值时「虚拟平仓」，
用开仓/平仓时刻的真实 VWAP 估算毛利，并扣除往返 taker 手续费（bps）。

Mid 价差 (signed) = (对手 Mid − Lighter Mid) / Lighter Mid × 10000

开仓方向（对冲）：
  - 对手 Mid > Lighter Mid → 多 L + 空对手（Lighter 买 + 对手卖）
  - 对手 Mid < Lighter Mid → 多对手 + 空 L（对手买 + Lighter 卖）

平仓：反向两腿，使用 **平仓时刻** 的 VWAP。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .base_feed import OrderBookSnapshot


@dataclass
class _OpenBook:
    ts: float
    l_mid: float
    x_mid: float
    mid_spread_bps: float
    abs_spread_bps: float
    x_rich: bool  # True: 多L空X
    lbuy: float
    lsell: float
    xbuy: float
    xsell: float
    qty: float


@dataclass
class ConvergenceTrade:
    exchange: str
    open_ts: float
    close_ts: float
    wait_sec: float
    open_abs_spread_bps: float
    close_abs_spread_bps: float
    gross_bps: float
    fee_rt_bps: float
    net_bps: float


class PerExchangeConvergenceTracker:
    """单交易所与 Lighter 的收敛状态机。"""

    def __init__(self, exchange_name: str):
        self.exchange_name = exchange_name
        self.open_thresh_bps = 8.0
        self.close_thresh_bps = 3.0
        self._pos: Optional[_OpenBook] = None
        self._trade_history: List[ConvergenceTrade] = []

    def configure(self, open_bps: float, close_bps: float) -> None:
        self.open_thresh_bps = max(0.1, float(open_bps))
        self.close_thresh_bps = max(0.0, float(close_bps))
        if self.close_thresh_bps >= self.open_thresh_bps:
            self.close_thresh_bps = max(0.0, self.open_thresh_bps - 0.5)

    def tick(
        self,
        ts: float,
        ls: OrderBookSnapshot,
        es: OrderBookSnapshot,
        trade_qty: float,
        fee_rt_bps: float,
    ) -> Optional[ConvergenceTrade]:
        """
        用当前快照更新状态；若本 tick 完成平仓则返回 ConvergenceTrade，否则 None。
        """
        if not ls.mid or not es.mid or trade_qty <= 0:
            return None

        mid_spread_bps = (es.mid - ls.mid) / ls.mid * 10_000.0
        abs_sp = abs(mid_spread_bps)

        lbuy = ls.vwap_buy(trade_qty)
        lsell = ls.vwap_sell(trade_qty)
        xbuy = es.vwap_buy(trade_qty)
        xsell = es.vwap_sell(trade_qty)
        vwap_ok = all(v is not None and v > 0 for v in (lbuy, lsell, xbuy, xsell))

        completed: Optional[ConvergenceTrade] = None

        if self._pos is None:
            if vwap_ok and abs_sp >= self.open_thresh_bps:
                x_rich = mid_spread_bps >= 0
                self._pos = _OpenBook(
                    ts=ts,
                    l_mid=ls.mid,
                    x_mid=es.mid,
                    mid_spread_bps=mid_spread_bps,
                    abs_spread_bps=abs_sp,
                    x_rich=x_rich,
                    lbuy=float(lbuy),
                    lsell=float(lsell),
                    xbuy=float(xbuy),
                    xsell=float(xsell),
                    qty=trade_qty,
                )
        else:
            if vwap_ok and abs_sp <= self.close_thresh_bps:
                o = self._pos
                # 平仓 VWAP（当前快照）
                cl_buy_l = float(lbuy)
                cl_sell_l = float(lsell)
                cl_buy_x = float(xbuy)
                cl_sell_x = float(xsell)

                if o.x_rich:
                    # 开: 买L 卖X ； 平: 卖L 买X
                    gross_per_unit = (o.xsell - o.lbuy) + (cl_sell_l - cl_buy_x)
                else:
                    # 开: 买X 卖L ； 平: 卖X 买L
                    gross_per_unit = (o.lsell - o.xbuy) + (cl_sell_x - cl_buy_l)

                ref_mid = o.l_mid if o.l_mid > 0 else ls.mid
                gross_bps = gross_per_unit / ref_mid * 10_000.0
                net_bps = gross_bps - fee_rt_bps

                completed = ConvergenceTrade(
                    exchange=self.exchange_name,
                    open_ts=o.ts,
                    close_ts=ts,
                    wait_sec=ts - o.ts,
                    open_abs_spread_bps=o.abs_spread_bps,
                    close_abs_spread_bps=abs_sp,
                    gross_bps=gross_bps,
                    fee_rt_bps=fee_rt_bps,
                    net_bps=net_bps,
                )
                self._trade_history.append(completed)
                if len(self._trade_history) > 200:
                    self._trade_history = self._trade_history[-200:]
                self._pos = None

        return completed

    @property
    def state(self) -> str:
        return "in_position" if self._pos else "idle"

    def snapshot_row(
        self,
        ts: float,
        ls: OrderBookSnapshot,
        es: OrderBookSnapshot,
        fee_rt_bps: float,
    ) -> Dict[str, Any]:
        abs_now = 0.0
        if ls.mid and es.mid:
            abs_now = abs((es.mid - ls.mid) / ls.mid * 10_000.0)

        row: Dict[str, Any] = {
            "exchange": self.exchange_name,
            "state": self.state,
            "abs_spread_bps": round(abs_now, 2),
            "open_thresh": self.open_thresh_bps,
            "close_thresh": self.close_thresh_bps,
            "fee_rt_bps": round(fee_rt_bps, 2),
            "in_position": self._pos is not None,
        }

        if self._pos:
            row["open_abs_spread_bps"] = round(self._pos.abs_spread_bps, 2)
            row["open_signed_spread_bps"] = round(self._pos.mid_spread_bps, 2)
            row["seconds_in_position"] = round(ts - self._pos.ts, 1)
            row["direction"] = "多L空X" if self._pos.x_rich else "多X空L"
        else:
            row["open_abs_spread_bps"] = None
            row["open_signed_spread_bps"] = None
            row["seconds_in_position"] = None
            row["direction"] = None

        hist = self._trade_history
        row["completed_count"] = len(hist)
        if hist:
            last = hist[-1]
            row["last_net_bps"] = round(last.net_bps, 2)
            row["last_wait_sec"] = round(last.wait_sec, 1)
            row["last_gross_bps"] = round(last.gross_bps, 2)
            nets = [t.net_bps for t in hist]
            waits = [t.wait_sec for t in hist]
            row["avg_net_bps"] = round(sum(nets) / len(nets), 2)
            row["avg_wait_sec"] = round(sum(waits) / len(waits), 1)
            wins = sum(1 for t in hist if t.net_bps > 0)
            row["win_rate_pct"] = round(100.0 * wins / len(hist), 1)
        else:
            row["last_net_bps"] = None
            row["last_wait_sec"] = None
            row["last_gross_bps"] = None
            row["avg_net_bps"] = None
            row["avg_wait_sec"] = None
            row["win_rate_pct"] = None

        return row


def rows_when_no_lighter_mid(
    targets: List[Any],
    lighter_taker_bps: float,
    open_bps: float,
    close_bps: float,
) -> List[Dict[str, Any]]:
    """Lighter 尚无 Mid 时仍返回占位行，避免前端表格空白。"""
    out: List[Dict[str, Any]] = []
    for feed in targets:
        fee_rt = 2.0 * (lighter_taker_bps + getattr(feed, "TAKER_FEE_BPS", 0.0))
        out.append({
            "exchange": feed.EXCHANGE_NAME,
            "state": "idle",
            "abs_spread_bps": 0.0,
            "open_thresh": open_bps,
            "close_thresh": close_bps,
            "fee_rt_bps": round(fee_rt, 2),
            "in_position": False,
            "open_abs_spread_bps": None,
            "open_signed_spread_bps": None,
            "seconds_in_position": None,
            "direction": "等待 Lighter Mid",
            "completed_count": 0,
            "last_net_bps": None,
            "last_wait_sec": None,
            "last_gross_bps": None,
            "avg_net_bps": None,
            "avg_wait_sec": None,
            "win_rate_pct": None,
            "placeholder": True,
        })
    return out


class ConvergenceTrackerGroup:
    """每个目标交易所一个 tracker。"""

    def __init__(self):
        self._by_name: Dict[str, PerExchangeConvergenceTracker] = {}

    def configure_all(self, open_bps: float, close_bps: float) -> None:
        for t in self._by_name.values():
            t.configure(open_bps, close_bps)

    def ensure(self, exchange_name: str) -> PerExchangeConvergenceTracker:
        if exchange_name not in self._by_name:
            self._by_name[exchange_name] = PerExchangeConvergenceTracker(exchange_name)
        return self._by_name[exchange_name]

    def sync_exchanges(self, names: List[str]) -> None:
        """移除已不在列表中的交易所 tracker（可选清理）。"""
        names_set = set(names)
        dead = [k for k in self._by_name if k not in names_set]
        for k in dead:
            del self._by_name[k]

    def tick_all(
        self,
        ls: OrderBookSnapshot,
        targets: List[Any],
        trade_size_usd: float,
        lighter_taker_bps: float,
        open_bps: float,
        close_bps: float,
    ) -> List[ConvergenceTrade]:
        """对所有 target 跑一轮；返回本 tick 新完成的交易列表。"""
        if not ls.mid:
            return []
        trade_qty = trade_size_usd / ls.mid
        completed: List[ConvergenceTrade] = []
        ts = time.time()

        for feed in targets:
            name = feed.EXCHANGE_NAME
            tr = self.ensure(name)
            tr.configure(open_bps, close_bps)
            es = feed.snapshot
            fee_rt = 2.0 * (lighter_taker_bps + getattr(feed, "TAKER_FEE_BPS", 0.0))
            done = tr.tick(ts, ls, es, trade_qty, fee_rt)
            if done:
                completed.append(done)

        return completed

    def snapshot(
        self,
        ls: OrderBookSnapshot,
        targets: List[Any],
        trade_size_usd: float,
        lighter_taker_bps: float,
        open_bps: float,
        close_bps: float,
    ) -> List[Dict[str, Any]]:
        if not ls.mid:
            return rows_when_no_lighter_mid(
                targets, lighter_taker_bps, open_bps, close_bps
            )
        ts = time.time()
        rows = []
        for feed in targets:
            name = feed.EXCHANGE_NAME
            tr = self.ensure(name)
            tr.configure(open_bps, close_bps)
            es = feed.snapshot
            fee_rt = 2.0 * (lighter_taker_bps + getattr(feed, "TAKER_FEE_BPS", 0.0))
            rows.append(tr.snapshot_row(ts, ls, es, fee_rt))
        rows.sort(key=lambda r: r.get("abs_spread_bps", 0), reverse=True)
        return rows
