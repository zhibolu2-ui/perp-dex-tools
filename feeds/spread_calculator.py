"""
SpreadCalculator – computes effective arbitrage metrics between
Lighter and each target feed.

**往返净利 (net_profit_bps)** 定义（与用户「开仓+平仓」一致，保守估算）：

- 假设四笔全部 **taker 吃单**，在当前盘口用 VWAP 一次性完成：
  - 开仓：Lighter 买/卖 + 对手所 卖/买（两腿）
  - 平仓：反向再两腿（仍按当前同一盘口 VWAP 估算「若现在平仓」的账面结果）
- **往返毛利 (bps)** = ((X卖VWAP − L买VWAP) + (L卖VWAP − X买VWAP)) / L_mid × 10000  
  （代数上与开仓方向无关，本质是「跨所价差 − 两边完整盘口成本」的瞬时快照）
- **往返费用** = 2 × (Lighter_taker + 对手_taker) （bps，按名义各收一次开、一次平）
- **净利润** = 往返毛利 − 往返费用  
  （VWAP 已含吃单滑点，不再重复扣「滑点L/X」列里的数值）

「滑点L / 滑点X」列：开仓名义下，该所 **买+卖** 相对最优价的平均冲击 (bps)，仅作参考。

**为何以前「单边」看起来像赚钱、往返却大亏？**

记 (同一数量、VWAP 价格)：

- **腿1** (bps) = (X卖 − L买) / L_mid × 10000  → 对应「Lighter 买 + 对手卖」这一侧的开仓现金流
- **腿2** (bps) = (L卖 − X买) / L_mid × 10000  → 对应平仓侧（或另一种开仓定义的另一半）

恒有 **腿1 + 腿2 = 往返毛利**。若腿1 为 +10 bps，则腿2 必为 (往返−10)；旧版往往只强调 |腿1| 或单边 VWAP，再 **重复扣滑点**，会严重高估净利。
"""

from dataclasses import dataclass
from typing import List

import time

from .base_feed import BaseFeed, OrderBookSnapshot


@dataclass
class SpreadResult:
    """Result of a single spread calculation for one exchange pair."""
    exchange: str
    lighter_mid: float
    exchange_mid: float
    raw_spread_bps: float          # Mid-Mid: (exchange_mid - lighter_mid) / lighter_mid * 10000
    direction: str                 # 建议开仓方向（较大单边 VWAP 优势的一侧）
    total_fee_bps: float           # 往返 taker 费用: 2*(L_taker + X_taker)
    slippage_lighter_bps: float    # Lighter 买/卖平均相对盘口的冲击 (参考)
    slippage_exchange_bps: float   # 对手所 买/卖平均冲击 (参考)
    net_profit_bps: float          # 往返净利 = round_trip_gross_bps - total_fee_bps
    round_trip_gross_bps: float    # 四笔 VWAP 在同一快照下的往返毛利 (bps)
    open_leg_buyL_sellX_bps: float # 腿1 = (X卖VWAP-L买VWAP)/L_mid*1e4；与腿2之和=往返毛利
    open_leg_buyX_sellL_bps: float # 腿2 = (L卖VWAP-X买VWAP)/L_mid*1e4
    lighter_spread_bps: float
    exchange_spread_bps: float
    depth_usd_lighter: float
    depth_usd_exchange: float
    timestamp: float = 0.0
    lighter_connected: bool = False
    exchange_connected: bool = False

    @property
    def profitable(self) -> bool:
        return self.net_profit_bps > 0


class SpreadCalculator:
    """
    Reference feed (Lighter) + target feeds → ranked SpreadResult list.
    """

    def __init__(
        self,
        lighter_feed: BaseFeed,
        target_feeds: List[BaseFeed],
        trade_size_usd: float = 10_000.0,
    ):
        self.lighter = lighter_feed
        self.targets = target_feeds
        self.trade_size_usd = trade_size_usd

    def compute_all(self) -> List[SpreadResult]:
        results: List[SpreadResult] = []
        ls = self.lighter.snapshot
        for feed in self.targets:
            results.append(self._compute_one(ls, feed))
        results.sort(key=lambda r: r.net_profit_bps, reverse=True)
        return results

    def _compute_one(self, ls: OrderBookSnapshot, feed: BaseFeed) -> SpreadResult:
        es = feed.snapshot
        now = time.time()

        if not ls.mid or not es.mid:
            return SpreadResult(
                exchange=feed.EXCHANGE_NAME,
                lighter_mid=ls.mid,
                exchange_mid=es.mid,
                raw_spread_bps=0.0,
                direction="等待数据",
                total_fee_bps=0.0,
                slippage_lighter_bps=0.0,
                slippage_exchange_bps=0.0,
                net_profit_bps=0.0,
                round_trip_gross_bps=0.0,
                open_leg_buyL_sellX_bps=0.0,
                open_leg_buyX_sellL_bps=0.0,
                lighter_spread_bps=ls.spread_bps,
                exchange_spread_bps=es.spread_bps,
                depth_usd_lighter=0.0,
                depth_usd_exchange=0.0,
                timestamp=now,
                lighter_connected=ls.connected,
                exchange_connected=es.connected,
            )

        raw_spread_bps = (es.mid - ls.mid) / ls.mid * 10_000.0
        trade_qty = self.trade_size_usd / ls.mid if ls.mid else 0.0

        lbuy = ls.vwap_buy(trade_qty)
        lsell = ls.vwap_sell(trade_qty)
        xbuy = es.vwap_buy(trade_qty)
        xsell = es.vwap_sell(trade_qty)

        # 往返毛利（同一快照、四笔吃单 VWAP；已含盘口冲击，勿再减滑点列）
        round_trip_gross_bps = 0.0
        leg1_bps = 0.0
        leg2_bps = 0.0
        direction = "深度不足"
        slip_l = 0.0
        slip_e = 0.0

        if lbuy and lsell and xbuy and xsell and ls.mid > 0:
            leg1_bps = (xsell - lbuy) / ls.mid * 10_000.0
            leg2_bps = (lsell - xbuy) / ls.mid * 10_000.0
            round_trip_gross_bps = leg1_bps + leg2_bps  # 恒等于两腿之和

            # 开仓方向标签：哪一侧单边 VWAP 优势更大（仅展示）
            edge1, edge2 = leg1_bps, leg2_bps
            if edge1 >= edge2:
                direction = f"Lighter买/{feed.EXCHANGE_NAME}卖 →平"
            else:
                direction = f"{feed.EXCHANGE_NAME}买/Lighter卖 →平"

            slip_l = (
                self._slippage_bps(ls, "buy", trade_qty)
                + self._slippage_bps(ls, "sell", trade_qty)
            ) / 2.0
            slip_e = (
                self._slippage_bps(es, "buy", trade_qty)
                + self._slippage_bps(es, "sell", trade_qty)
            ) / 2.0
        else:
            slip_l = (
                self._slippage_bps(ls, "buy", trade_qty)
                + self._slippage_bps(ls, "sell", trade_qty)
            ) / 2.0
            slip_e = (
                self._slippage_bps(es, "buy", trade_qty)
                + self._slippage_bps(es, "sell", trade_qty)
            ) / 2.0

        # 保守：开+平各两腿，全部按 taker 费率
        total_fee_bps = 2.0 * (
            self.lighter.TAKER_FEE_BPS + feed.TAKER_FEE_BPS
        )

        net = round_trip_gross_bps - total_fee_bps

        depth_l = self._depth_usd(ls.bids[:5]) + self._depth_usd(ls.asks[:5])
        depth_e = self._depth_usd(es.bids[:5]) + self._depth_usd(es.asks[:5])

        return SpreadResult(
            exchange=feed.EXCHANGE_NAME,
            lighter_mid=ls.mid,
            exchange_mid=es.mid,
            raw_spread_bps=raw_spread_bps,
            direction=direction,
            total_fee_bps=total_fee_bps,
            slippage_lighter_bps=slip_l,
            slippage_exchange_bps=slip_e,
            net_profit_bps=net,
            round_trip_gross_bps=round_trip_gross_bps,
            open_leg_buyL_sellX_bps=leg1_bps,
            open_leg_buyX_sellL_bps=leg2_bps,
            lighter_spread_bps=ls.spread_bps,
            exchange_spread_bps=es.spread_bps,
            depth_usd_lighter=depth_l,
            depth_usd_exchange=depth_e,
            timestamp=now,
            lighter_connected=ls.connected,
            exchange_connected=es.connected,
        )

    @staticmethod
    def _slippage_bps(snap: OrderBookSnapshot, side: str, qty: float) -> float:
        """VWAP 相对最优价的额外冲击 (bps)。"""
        if not snap.mid or qty <= 0:
            return 0.0
        if side == "buy":
            vwap = snap.vwap_buy(qty)
            ref = snap.best_ask
        else:
            vwap = snap.vwap_sell(qty)
            ref = snap.best_bid
        if vwap is None or ref is None or ref == 0:
            return 50.0
        return abs(vwap - ref) / ref * 10_000.0

    @staticmethod
    def _depth_usd(levels) -> float:
        return sum(lvl.price * lvl.size for lvl in levels)
