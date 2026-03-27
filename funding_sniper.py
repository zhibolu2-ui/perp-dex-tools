#!/usr/bin/env python3
"""
资金费率狙击监控 (Funding Rate Sniper)

扫描交易所的所有重叠币种，找出单次结算收益 > 手续费的狙击机会。
策略：结算前毫秒级开仓 → 收取费率 → 立即平仓。

支持交易所: Lighter, Binance, Hyperliquid
交易对: Lighter↔Binance, Lighter↔Hyperliquid
"""

import argparse, asyncio, aiohttp, json, time, os, csv, signal, logging, sys
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from math import ceil

# ═══════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════

LIGHTER_REST = "https://mainnet.zklighter.elliot.ai"
BINANCE_API = "https://fapi.binance.com"
HYPERLIQUID_API = "https://api.hyperliquid.xyz"

TAKER_BPS = {"Lighter": 0.0, "Binance": 4.0, "Hyperliquid": 3.5}
MAKER_BPS = {"Lighter": 0.0, "Binance": 2.0, "Hyperliquid": 1.0}
SETTLEMENT_H = {"Lighter": 1.0, "Binance": 8.0, "Hyperliquid": 1.0}

# ═══════════════════════════════════════════════════
#  Data Classes
# ═══════════════════════════════════════════════════

@dataclass
class CoinRate:
    symbol: str
    exchange: str
    rate_per_period: float  # raw decimal rate for one settlement period
    period_h: float
    rate_bps: float  # rate in basis points for one period
    next_settlement: float = 0.0  # unix timestamp

@dataclass
class LiquidityInfo:
    symbol: str
    exchange: str
    bid: float = 0.0
    ask: float = 0.0
    spread_bps: float = 0.0        # bid-ask spread in bps
    volume_24h_usd: float = 0.0    # 24h volume in USD
    open_interest_usd: float = 0.0 # open interest in USD
    score: str = "?"               # A/B/C/D/F

@dataclass
class SnipeOpportunity:
    symbol: str
    short_exchange: str
    long_exchange: str
    short_rate_bps: float  # rate the short side RECEIVES (positive = receives)
    long_rate_bps: float   # rate the long side RECEIVES (positive = receives)
    gross_bps: float       # total bps received from both sides
    taker_fee_bps: float   # round-trip taker fees
    maker_fee_bps: float   # round-trip maker fees (best side)
    net_taker_bps: float   # gross - taker_fees
    net_maker_bps: float   # gross - maker_fees
    next_settlement: float # soonest aligned settlement
    countdown_s: float     # seconds until settlement
    liq_score: str = "?"   # combined liquidity grade
    est_slippage_bps: float = 0.0  # estimated round-trip slippage
    basis_risk_bps: float = 0.0    # estimated basis volatility risk

# ═══════════════════════════════════════════════════
#  Scanner
# ═══════════════════════════════════════════════════

class FundingSniper:
    def __init__(self, min_net_bps: float = 0.0, poll_interval: float = 30.0,
                 top_n: int = 30):
        self.min_net_bps = min_net_bps
        self.poll_interval = poll_interval
        self.top_n = top_n
        self.stop_flag = False
        self._session: Optional[aiohttp.ClientSession] = None

        self._listings: Dict[str, Set[str]] = {}  # exchange -> set of symbols
        self._rates: Dict[str, Dict[str, CoinRate]] = {}  # exchange -> {symbol: CoinRate}
        self._opportunities: List[SnipeOpportunity] = []
        self._adapters: Dict[str, object] = {}  # exchange -> ExchangeAdapter

        # Liquidity & basis risk tracking
        self._liquidity: Dict[str, Dict[str, LiquidityInfo]] = {}  # exchange -> {symbol: info}
        self._mid_history: Dict[str, List[Tuple[float, float]]] = {}  # "sym:exA-exB" -> [(ts, spread_bps)]
        self._basis_vol: Dict[str, float] = {}  # "sym:exA-exB" -> rolling std of spread (bps)

        # Multi-symbol WS feeds (single connection per exchange)
        self._lighter_multi_feed = None   # LighterMultiFeed
        self._hl_multi_feed = None        # HyperliquidMultiFeed
        self._lighter_market_ids: Dict[str, int] = {}  # symbol -> market_id (from REST)
        self._feeds_task_lighter = None   # asyncio.Task for lighter WS
        self._feeds_task_hl = None        # asyncio.Task for HL WS

        # Snipe execution parameters (set via CLI)
        self.snipe_size_usd: float = 100.0
        self.snipe_window_s: float = 120.0
        self.pre_open_s: float = 5.0
        self.trade_exchanges: List[str] = ["Lighter", "Hyperliquid"]

        # Convergence parameters for Phase 3 close logic
        self.max_hold_after: float = 60.0
        self.basis_stop_loss: float = 5.0
        self.convergence_target: float = 0.5

        self.logger = logging.getLogger("sniper")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            h = logging.StreamHandler()
            h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            self.logger.addHandler(h)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15))
        return self._session

    # ─── Listing Scanners ───

    async def _scan_listings(self):
        """Scan Lighter, Binance, Hyperliquid for available perpetual symbols."""
        s = await self._get_session()
        await asyncio.gather(
            self._scan_lighter(s),
            self._scan_binance(s),
            self._scan_hyperliquid(s),
            return_exceptions=True)
        total_overlap = sum(1 for sym in self._get_all_symbols()
                           if self._count_exchanges(sym) >= 2)
        self.logger.info(f"币种扫描完成: {total_overlap} 个重叠币种")

    def _get_all_symbols(self) -> Set[str]:
        all_s = set()
        for syms in self._listings.values():
            all_s.update(syms)
        return all_s

    def _count_exchanges(self, sym: str) -> int:
        return sum(1 for syms in self._listings.values() if sym in syms)

    async def _scan_lighter(self, s):
        try:
            async with s.get(f"{LIGHTER_REST}/api/v1/funding-rates") as r:
                data = await r.json(content_type=None)
            syms = set()
            for it in (data or {}).get("funding_rates", []):
                if it.get("exchange") == "lighter":
                    syms.add(it["symbol"].upper())
            self._listings["Lighter"] = syms
        except Exception as e:
            self.logger.debug(f"scan lighter: {e}")

    async def _scan_binance(self, s):
        try:
            async with s.get(f"{BINANCE_API}/fapi/v1/exchangeInfo") as r:
                data = await r.json(content_type=None)
            syms = set()
            for si in data.get("symbols", []):
                if si.get("contractType") == "PERPETUAL" and si.get("quoteAsset") == "USDT":
                    syms.add(si["baseAsset"].upper())
            self._listings["Binance"] = syms
        except Exception as e:
            self.logger.debug(f"scan binance: {e}")

    async def _scan_hyperliquid(self, s):
        try:
            async with s.post(f"{HYPERLIQUID_API}/info",
                              json={"type": "metaAndAssetCtxs"}) as r:
                data = await r.json(content_type=None)
            syms = set()
            for m in data[0].get("universe", []):
                syms.add(m["name"].upper())
            self._listings["Hyperliquid"] = syms
        except Exception as e:
            self.logger.debug(f"scan hyperliquid: {e}")

    # ─── Batch Rate Fetchers ───

    async def _fetch_all_rates(self):
        """Fetch funding rates from Lighter, Binance, Hyperliquid in parallel."""
        s = await self._get_session()
        await asyncio.gather(
            self._rates_lighter(s),
            self._rates_binance(s),
            self._rates_hyperliquid(s),
            return_exceptions=True)

    async def _rates_lighter(self, s):
        try:
            async with s.get(f"{LIGHTER_REST}/api/v1/funding-rates") as r:
                data = await r.json(content_type=None)
            now = time.time()
            next_hour = (int(now) // 3600 + 1) * 3600
            rates = {}
            for it in (data or {}).get("funding_rates", []):
                if it.get("exchange") != "lighter":
                    continue
                sym = it["symbol"].upper()
                raw = float(it.get("rate", 0))
                period_h = 1.0
                rates[sym] = CoinRate(
                    symbol=sym, exchange="Lighter",
                    rate_per_period=raw, period_h=period_h,
                    rate_bps=raw * 10000,
                    next_settlement=float(next_hour))
            self._rates["Lighter"] = rates
        except Exception as e:
            self.logger.debug(f"rates lighter: {e}")

    async def _rates_binance(self, s):
        try:
            async with s.get(f"{BINANCE_API}/fapi/v1/premiumIndex") as r:
                data = await r.json(content_type=None)
            rates = {}
            for it in data:
                sym_raw = it.get("symbol", "")
                if not sym_raw.endswith("USDT"):
                    continue
                sym = sym_raw.replace("USDT", "").upper()
                raw = float(it.get("lastFundingRate", 0))
                nft = float(it.get("nextFundingTime", 0)) / 1000
                rates[sym] = CoinRate(
                    symbol=sym, exchange="Binance",
                    rate_per_period=raw, period_h=8.0,
                    rate_bps=raw * 10000,
                    next_settlement=nft)
            self._rates["Binance"] = rates
        except Exception as e:
            self.logger.debug(f"rates binance: {e}")

    async def _rates_hyperliquid(self, s):
        try:
            async with s.post(f"{HYPERLIQUID_API}/info",
                              json={"type": "metaAndAssetCtxs"}) as r:
                data = await r.json(content_type=None)
            meta = data[0].get("universe", [])
            ctxs = data[1]
            now = time.time()
            next_hour = (int(now) // 3600 + 1) * 3600
            rates = {}
            for i, m in enumerate(meta):
                sym = m["name"].upper()
                raw = float(ctxs[i].get("funding", 0))
                rates[sym] = CoinRate(
                    symbol=sym, exchange="Hyperliquid",
                    rate_per_period=raw, period_h=1.0,
                    rate_bps=raw * 10000,
                    next_settlement=float(next_hour))
            self._rates["Hyperliquid"] = rates
        except Exception as e:
            self.logger.debug(f"rates hyperliquid: {e}")

    # ─── Liquidity & Basis Risk ───

    async def _fetch_liquidity(self):
        """Fetch bid/ask spreads for Lighter & Hyperliquid."""
        s = await self._get_session()
        await asyncio.gather(
            self._liq_lighter(s),
            self._liq_hyperliquid(s),
            return_exceptions=True)
        self._update_basis_risk()

    async def _liq_lighter(self, s):
        """Lighter: single multi-symbol WS feed for real-time bid/ask spreads."""
        if not self._lighter_market_ids:
            try:
                async with s.get(f"{LIGHTER_REST}/api/v1/orderBooks",
                                 headers={"accept": "application/json"}) as r:
                    data = await r.json(content_type=None)
                for ob in (data or {}).get("order_books", []):
                    sym = ob.get("symbol", "").upper()
                    if sym and ob.get("status") == "active":
                        self._lighter_market_ids[sym] = ob["market_id"]
            except Exception as e:
                self.logger.debug(f"lighter market ids: {e}")

        lighter_syms = self._listings.get("Lighter", set())
        hl_syms = self._listings.get("Hyperliquid", set())
        bn_syms = self._listings.get("Binance", set())
        overlap = (lighter_syms & hl_syms) | (lighter_syms & bn_syms)
        ws_syms = [c for c in overlap if c in self._lighter_market_ids]

        if ws_syms and self._lighter_multi_feed is None:
            from feeds.lighter_multi_feed import LighterMultiFeed
            self._lighter_multi_feed = LighterMultiFeed()
            for sym in ws_syms:
                mid = self._lighter_market_ids[sym]
                self._lighter_multi_feed.add_market(sym, mid)
            self._feeds_task_lighter = asyncio.create_task(
                self._lighter_multi_feed.connect()
            )
            self.logger.info(
                "Lighter multi-feed 启动: 1 WS, %d markets (仅重叠币): %s",
                len(ws_syms), ws_syms[:10],
            )
        elif self._lighter_multi_feed is not None:
            existing = set(self._lighter_multi_feed.symbols)
            for sym in ws_syms:
                if sym not in existing:
                    mid = self._lighter_market_ids[sym]
                    self._lighter_multi_feed.add_market(sym, mid)

        liq = {}
        if self._lighter_multi_feed:
            for sym in ws_syms:
                snap = self._lighter_multi_feed.snapshot(sym)
                if snap and snap.connected and snap.best_bid > 0 and snap.best_ask > 0:
                    sp = snap.spread_bps
                    liq[sym] = LiquidityInfo(
                        symbol=sym, exchange="Lighter",
                        bid=snap.best_bid, ask=snap.best_ask,
                        spread_bps=sp,
                        score=self._grade_lighter_liq(sp))
                else:
                    liq[sym] = LiquidityInfo(
                        symbol=sym, exchange="Lighter",
                        spread_bps=2.0, score="B")

        for sym in lighter_syms:
            if sym not in liq:
                liq[sym] = LiquidityInfo(
                    symbol=sym, exchange="Lighter",
                    spread_bps=3.0, score="C")
        self._liquidity["Lighter"] = liq

    async def _liq_hyperliquid(self, s):
        """Hyperliquid: single multi-symbol WS + REST fallback for mid prices."""
        hl_syms = self._listings.get("Hyperliquid", set())
        lighter_syms = self._listings.get("Lighter", set())
        overlap_syms = hl_syms & lighter_syms

        if overlap_syms and self._hl_multi_feed is None:
            from feeds.hyperliquid_multi_feed import HyperliquidMultiFeed
            self._hl_multi_feed = HyperliquidMultiFeed()
            for sym in overlap_syms:
                self._hl_multi_feed.add_coin(sym)
            self._feeds_task_hl = asyncio.create_task(
                self._hl_multi_feed.connect()
            )
            self.logger.info(
                "Hyperliquid multi-feed 启动: 1 WS, %d coins",
                len(overlap_syms),
            )
        elif self._hl_multi_feed is not None:
            existing = set(self._hl_multi_feed.symbols)
            for sym in overlap_syms:
                if sym not in existing:
                    self._hl_multi_feed.add_coin(sym)

        rest_mids: Dict[str, float] = {}
        try:
            async with s.post(f"{HYPERLIQUID_API}/info",
                              json={"type": "allMids"}) as r:
                data = await r.json(content_type=None)
            if isinstance(data, dict):
                for coin, mid_str in data.items():
                    rest_mids[coin.upper()] = float(mid_str)
        except Exception as e:
            self.logger.debug(f"HL allMids: {e}")

        liq = {}
        for sym in overlap_syms:
            if self._hl_multi_feed:
                snap = self._hl_multi_feed.snapshot(sym)
                if snap and snap.connected and snap.best_bid > 0 and snap.best_ask > 0:
                    sp = snap.spread_bps
                    liq[sym] = LiquidityInfo(
                        symbol=sym, exchange="Hyperliquid",
                        bid=snap.best_bid, ask=snap.best_ask,
                        spread_bps=sp,
                        score=self._grade_hl_liq(sp))
                    continue

            mid = rest_mids.get(sym)
            if mid and mid > 0:
                est_spread = 1.5
                liq[sym] = LiquidityInfo(
                    symbol=sym, exchange="Hyperliquid",
                    bid=mid * (1 - est_spread / 20000),
                    ask=mid * (1 + est_spread / 20000),
                    spread_bps=est_spread,
                    score="B")
            else:
                liq[sym] = LiquidityInfo(
                    symbol=sym, exchange="Hyperliquid",
                    spread_bps=3.0, score="C")

        self._liquidity["Hyperliquid"] = liq

    @staticmethod
    def _grade_hl_liq(spread_bps: float) -> str:
        if spread_bps <= 0.5:
            return "A"
        elif spread_bps <= 2.0:
            return "B"
        elif spread_bps <= 5.0:
            return "C"
        elif spread_bps <= 15.0:
            return "D"
        return "F"

    @staticmethod
    def _grade_lighter_liq(spread_bps: float) -> str:
        """Grade Lighter liquidity based on spread (no volume/OI data available)."""
        # Lighter has zero fees, so even moderate spreads are acceptable
        if spread_bps <= 1.0:
            return "A"
        elif spread_bps <= 3.0:
            return "B"
        elif spread_bps <= 8.0:
            return "C"
        elif spread_bps <= 20.0:
            return "D"
        return "F"

    def _combined_liq_score(self, sym: str, ex_a: str, ex_b: str) -> Tuple[str, float]:
        """Return (grade, estimated_slippage_bps) for a pair.
        Uses VWAP depth-weighted slippage when real orderbook is available,
        falls back to spread-based estimate otherwise."""
        la = (self._liquidity.get(ex_a) or {}).get(sym)
        lb = (self._liquidity.get(ex_b) or {}).get(sym)

        est_slip = self._depth_slippage(sym, ex_a, ex_b)

        grade_map = {"A": 4, "B": 3, "C": 2, "D": 1, "F": 0, "?": 0}
        ga = grade_map.get(la.score if la else "?", 0)
        gb = grade_map.get(lb.score if lb else "?", 0)
        avg = (ga + gb) / 2
        if avg >= 3.5:
            g = "A"
        elif avg >= 2.5:
            g = "B"
        elif avg >= 1.5:
            g = "C"
        elif avg >= 0.5:
            g = "D"
        else:
            g = "F"
        return g, est_slip

    def _depth_slippage(self, sym: str, ex_a: str, ex_b: str) -> float:
        """Calculate round-trip slippage in bps using VWAP against orderbook depth.
        For a hedged trade: open short on A + open long on B → close both.
        4 legs total: sell A, buy B, buy A, sell B.
        Each leg's slippage = |vwap - mid| / mid."""
        trade_usd = self.snipe_size_usd

        slip_a = self._single_exchange_slip(sym, ex_a, trade_usd)
        slip_b = self._single_exchange_slip(sym, ex_b, trade_usd)
        return slip_a + slip_b

    def _single_exchange_slip(self, sym: str, exchange: str, trade_usd: float) -> float:
        """Round-trip slippage for one exchange (open + close) in bps."""
        snap = self._get_ob_snapshot(sym, exchange)
        if snap is None or not snap.connected or snap.mid <= 0:
            spread = (self._liquidity.get(exchange) or {}).get(sym)
            fallback = spread.spread_bps if spread else 10.0
            return 2 * fallback

        mid = snap.mid
        qty = trade_usd / mid
        slip_bps = 0.0

        vwap_sell = snap.vwap_sell(qty)
        if vwap_sell and vwap_sell > 0:
            slip_bps += (mid - vwap_sell) / mid * 10000
        else:
            slip_bps += snap.spread_bps

        vwap_buy = snap.vwap_buy(qty)
        if vwap_buy and vwap_buy > 0:
            slip_bps += (vwap_buy - mid) / mid * 10000
        else:
            slip_bps += snap.spread_bps

        return max(slip_bps, 0.0)

    def _get_ob_snapshot(self, sym: str, exchange: str):
        """Get live OrderBookSnapshot from the multi-feed."""
        if exchange == "Lighter" and self._lighter_multi_feed:
            return self._lighter_multi_feed.snapshot(sym)
        if exchange == "Hyperliquid" and self._hl_multi_feed:
            return self._hl_multi_feed.snapshot(sym)
        return None

    def _update_basis_risk(self):
        """Track cross-exchange price spread volatility for all monitored pairs."""
        now = time.time()
        all_liq = {
            ex: liq for ex, liq in self._liquidity.items()
            if ex in ("Lighter", "Hyperliquid")
        }
        exchanges = list(all_liq.keys())
        for i, ex_a in enumerate(exchanges):
            for ex_b in exchanges[i + 1:]:
                liq_a = all_liq[ex_a]
                liq_b = all_liq[ex_b]
                common = set(liq_a.keys()) & set(liq_b.keys())
                for sym in common:
                    la = liq_a[sym]
                    lb = liq_b[sym]
                    if la.bid <= 0 or la.ask <= 0 or lb.bid <= 0 or lb.ask <= 0:
                        continue
                    mid_a = (la.bid + la.ask) / 2
                    mid_b = (lb.bid + lb.ask) / 2
                    avg_mid = (mid_a + mid_b) / 2
                    spread_bps = (mid_a - mid_b) / avg_mid * 10000 if avg_mid > 0 else 0

                    key = f"{sym}:{ex_a}-{ex_b}:cross"
                    if key not in self._mid_history:
                        self._mid_history[key] = []
                    self._mid_history[key].append((now, spread_bps))
                    self._mid_history[key] = self._mid_history[key][-60:]

                    samples = self._mid_history[key]
                    if len(samples) >= 3:
                        vals = [s[1] for s in samples]
                        mean = sum(vals) / len(vals)
                        var = sum((v - mean) ** 2 for v in vals) / len(vals)
                        std_bps = var ** 0.5
                        self._basis_vol[f"{sym}:{ex_a}-{ex_b}"] = std_bps
                        self._basis_vol[f"{sym}:{ex_b}-{ex_a}"] = std_bps

    def _get_basis_risk(self, sym: str, ex_a: str, ex_b: str) -> float:
        """Estimated basis risk in bps: 2-sigma of cross-exchange spread movement
        during the hold period (~10s open-to-close)."""
        key1 = f"{sym}:{ex_a}-{ex_b}"
        key2 = f"{sym}:{ex_b}-{ex_a}"
        vol = self._basis_vol.get(key1) or self._basis_vol.get(key2, 0.0)
        return vol * 2.0  # 2-sigma

    # ─── Pre-Execution Depth Check ───

    async def _check_depth_before_snipe(self, opp: SnipeOpportunity) -> Tuple[bool, float]:
        """Check actual orderbook depth via adapters before executing.
        Recalculates VWAP slippage at execution time.
        Returns (ok, cross_spread_bps)."""
        short_a = self._adapters.get(opp.short_exchange)
        long_a = self._adapters.get(opp.long_exchange)
        if not short_a or not long_a:
            return False, 999.0

        short_mid = await short_a.get_mid_price()
        long_mid = await long_a.get_mid_price()
        if not short_mid or not long_mid:
            return False, 999.0

        avg_mid = (float(short_mid) + float(long_mid)) / 2
        cross_spread_bps = abs(float(short_mid) - float(long_mid)) / avg_mid * 10000

        if cross_spread_bps > opp.gross_bps * 0.5:
            self.logger.warning(
                f"跨所价差过大: {cross_spread_bps:.1f} bps > 毛利50%({opp.gross_bps * 0.5:.1f}bps)")
            return False, cross_spread_bps

        live_slip = self._depth_slippage(opp.symbol, opp.short_exchange, opp.long_exchange)
        adjusted = opp.net_taker_bps - live_slip - opp.basis_risk_bps
        self.logger.info(
            f"开仓前深度加权滑点: {live_slip:.1f}bps (调整后净利={adjusted:+.1f}bps)")
        if adjusted <= 0:
            self.logger.warning(
                f"深度加权滑点过大: 净利{opp.net_taker_bps:.1f} - 滑点{live_slip:.1f} - 基差{opp.basis_risk_bps:.1f} = {adjusted:+.1f}bps <= 0")
            return False, cross_spread_bps

        return True, cross_spread_bps

    # ─── Opportunity Calculator ───

    def _compute_opportunities(self):
        """Find all profitable snipe opportunities across exchange pairs."""
        now = time.time()
        opps: List[SnipeOpportunity] = []
        exchanges = list(self._rates.keys())

        for i, ex_a in enumerate(exchanges):
            for ex_b in exchanges[i + 1:]:
                rates_a = self._rates.get(ex_a, {})
                rates_b = self._rates.get(ex_b, {})
                common = set(rates_a.keys()) & set(rates_b.keys())

                for sym in common:
                    ra = rates_a[sym]
                    rb = rates_b[sym]

                    # Normalize rates to SAME period for comparison
                    # For sniping, we care about ONE settlement on each side
                    # Rate is already per-period, convert to bps
                    rate_a_bps = ra.rate_bps  # bps for one period on exchange A
                    rate_b_bps = rb.rate_bps

                    # Funding logic: positive rate → longs pay shorts
                    # Short side RECEIVES positive rate; Long side RECEIVES negative rate
                    # Best strategy: short on exchange with highest rate, long on lowest

                    if rate_a_bps >= rate_b_bps:
                        short_ex, long_ex = ex_a, ex_b
                        short_rate = rate_a_bps   # short receives this
                        long_rate = -rate_b_bps   # long receives -rate (positive when rate is negative)
                        short_period = ra.period_h
                        long_period = rb.period_h
                        short_next = ra.next_settlement
                        long_next = rb.next_settlement
                    else:
                        short_ex, long_ex = ex_b, ex_a
                        short_rate = rate_b_bps
                        long_rate = -rate_a_bps
                        short_period = rb.period_h
                        long_period = ra.period_h
                        short_next = rb.next_settlement
                        long_next = ra.next_settlement

                    # Gross = what short receives + what long receives for ONE settlement
                    # For different period exchanges, we get charged on each settlement
                    # For sniping, we hold across ONE settlement on the target exchange
                    # We earn from the side with the bigger rate
                    gross = short_rate + long_rate  # = rate_a_bps - rate_b_bps (always positive)

                    if gross <= 0:
                        continue

                    # Fees: round-trip (open + close) on both exchanges
                    tk_a = TAKER_BPS.get(short_ex, 5.0)
                    tk_b = TAKER_BPS.get(long_ex, 5.0)
                    taker_fee = 2 * (tk_a + tk_b)

                    mk_a = MAKER_BPS.get(short_ex, 2.0)
                    mk_b = MAKER_BPS.get(long_ex, 2.0)
                    maker_fee = min(2 * (mk_a + tk_b), 2 * (tk_a + mk_b))

                    net_taker = gross - taker_fee
                    net_maker = gross - maker_fee

                    # Find next aligned settlement
                    next_s = 0.0
                    if short_next > now and long_next > now:
                        next_s = min(short_next, long_next)
                    elif short_next > now:
                        next_s = short_next
                    elif long_next > now:
                        next_s = long_next

                    countdown = max(0, next_s - now) if next_s > 0 else 0

                    liq_grade, est_slip = self._combined_liq_score(sym, short_ex, long_ex)
                    basis_risk = self._get_basis_risk(sym, short_ex, long_ex)

                    opps.append(SnipeOpportunity(
                        symbol=sym, short_exchange=short_ex, long_exchange=long_ex,
                        short_rate_bps=short_rate, long_rate_bps=long_rate,
                        gross_bps=gross,
                        taker_fee_bps=taker_fee, maker_fee_bps=maker_fee,
                        net_taker_bps=net_taker, net_maker_bps=net_maker,
                        next_settlement=next_s, countdown_s=countdown,
                        liq_score=liq_grade,
                        est_slippage_bps=est_slip,
                        basis_risk_bps=basis_risk,
                    ))

        # Sort by risk-adjusted net profit using TAKER fees (matches actual execution)
        opps.sort(key=lambda o: o.net_taker_bps - o.est_slippage_bps - o.basis_risk_bps,
                  reverse=True)
        self._opportunities = opps

    # ─── Dashboard ───

    def _display(self):
        now_s = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        now = time.time()
        W = 155
        sep = "═" * W
        lines = ["", sep, f"  资金费率狙击监控   {now_s}", sep]

        # Exchange summary
        ex_counts = {ex: len(rates) for ex, rates in self._rates.items() if rates}
        lines.append(f"  交易所: {', '.join(f'{ex}({cnt})' for ex, cnt in sorted(ex_counts.items()))}")

        for exn in ("Lighter", "Hyperliquid"):
            liq = self._liquidity.get(exn, {})
            if liq:
                coins = sorted(liq.values(), key=lambda x: x.volume_24h_usd, reverse=True)[:5]
                parts = []
                for c in coins:
                    vol_m = c.volume_24h_usd / 1e6
                    parts.append(f"{c.symbol}({c.score} sp={c.spread_bps:.1f}bps vol=${vol_m:.0f}M)")
                lines.append(f"  {exn} 流动性: {', '.join(parts)}")

        # Settlement countdown
        lines.append("")
        lines.append("  ┌─ 最近结算倒计时 ────────────────────────────────────┐")
        settlements = []
        for ex, rates in self._rates.items():
            for cr in rates.values():
                if cr.next_settlement > now:
                    settlements.append((ex, cr.next_settlement))
                    break
        settlements.sort(key=lambda x: x[1])
        seen_ex = set()
        for ex, ts in settlements:
            if ex in seen_ex:
                continue
            seen_ex.add(ex)
            cd = ts - now
            t_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S")
            if cd < 60:
                cd_str = f"{cd:.0f}s ⚡"
            elif cd < 3600:
                cd_str = f"{cd / 60:.1f}min"
            else:
                cd_str = f"{cd / 3600:.1f}h"
            lines.append(f"  │ {ex:<14} {t_str} UTC  倒计时 {cd_str:>10} │")
        lines.append("  └───────────────────────────────────────────────────┘")

        # Profitable opportunities (using taker fees = actual execution cost)
        profitable_taker = [o for o in self._opportunities if o.net_taker_bps > 0]
        safe_opps = [o for o in profitable_taker
                     if o.net_taker_bps > o.est_slippage_bps + o.basis_risk_bps
                     and o.liq_score in ("A", "B")]

        lines += [
            "",
            f"  吃单盈利 {len(profitable_taker)} 个 | 风险调整后可执行 {len(safe_opps)} 个",
            "",
            f"  ┌─ TOP {self.top_n} 狙击机会 (按吃单风险调整后净利排序) ───────────────────────────────────────────────────────────────────────────────┐",
            f"  │ {'币种':<8} {'做空':<12} {'做多':<12} "
            f"{'毛利':>6} {'吃单费':>6} {'净利':>6} "
            f"{'流动':>4} {'深度滑点':>7} {'基差风险':>6} {'调整后':>7} "
            f"{'倒计时':>10} {'信号':>4} │",
            f"  │ {'─' * (W - 6)} │",
        ]

        for o in self._opportunities[:self.top_n]:
            cd_str = ""
            if o.countdown_s > 0:
                if o.countdown_s < 60:
                    cd_str = f"{o.countdown_s:.0f}s ⚡"
                elif o.countdown_s < 3600:
                    cd_str = f"{o.countdown_s / 60:.1f}m"
                else:
                    cd_str = f"{o.countdown_s / 3600:.1f}h"
            else:
                cd_str = "—"

            adjusted = o.net_taker_bps - o.est_slippage_bps - o.basis_risk_bps

            if adjusted > 0 and o.liq_score in ("A", "B"):
                signal = "🟢"
            elif o.net_taker_bps > 0 and o.liq_score in ("A", "B", "C"):
                signal = "🟡"
            elif o.net_taker_bps > 0:
                signal = "🔴"
            else:
                signal = "⚫"

            lines.append(
                f"  │ {o.symbol:<8} {o.short_exchange:<12} {o.long_exchange:<12} "
                f"{o.gross_bps:>+6.1f} {o.taker_fee_bps:>6.0f} {o.net_taker_bps:>+6.1f} "
                f"{'  ' + o.liq_score:>4} {o.est_slippage_bps:>7.1f} {o.basis_risk_bps:>6.1f} {adjusted:>+7.1f} "
                f"{cd_str:>10} {signal:>4} │")

        if not self._opportunities:
            lines.append(f"  │ {'(无数据 - 等待首次扫描...)':^{W - 6}} │")

        lines.append(f"  └{'─' * (W - 4)}┘")

        # Signal legend
        lines.append("  信号: 🟢 可执行(净利>滑点+基差, 流动A/B)  🟡 谨慎(有利但风险中)  🔴 高风险(流动性差)  ⚫ 亏损")

        # Lighter-Binance specific table
        lb_opps = [o for o in self._opportunities
                   if {o.short_exchange, o.long_exchange} == {"Lighter", "Binance"}]
        if lb_opps:
            lines += [
                "",
                f"  ┌─ Lighter ↔ Binance 专区 (吃单费: Lighter=0 + Binance=4bps×2=8bps 往返) ──────────────────────┐",
                f"  │ {'币种':<8} {'做空':<12} {'做多':<12} "
                f"{'毛利':>6} {'吃单费':>6} {'净利':>6} {'滑点':>5} {'基差':>5} {'调整后':>7} {'倒计时':>10} {'信号':>4} │",
                f"  │ {'─' * 105} │",
            ]
            for o in lb_opps[:15]:
                cd_str = ""
                if o.countdown_s > 0:
                    if o.countdown_s < 60:
                        cd_str = f"{o.countdown_s:.0f}s ⚡"
                    elif o.countdown_s < 3600:
                        cd_str = f"{o.countdown_s / 60:.1f}m"
                    else:
                        cd_str = f"{o.countdown_s / 3600:.1f}h"
                else:
                    cd_str = "—"
                adj = o.net_taker_bps - o.est_slippage_bps - o.basis_risk_bps
                sig = "🟢" if adj > 0 and o.liq_score in ("A", "B") else ("🟡" if o.net_taker_bps > 0 else "⚫")
                lines.append(
                    f"  │ {o.symbol:<8} {o.short_exchange:<12} {o.long_exchange:<12} "
                    f"{o.gross_bps:>+6.1f} {o.taker_fee_bps:>6.0f} {o.net_taker_bps:>+6.1f} {o.est_slippage_bps:>5.1f} "
                    f"{o.basis_risk_bps:>5.1f} {adj:>+7.1f} {cd_str:>10} {sig:>4} │")
            lines.append(f"  └{'─' * 111}┘")

        # Lighter-Hyperliquid specific table
        lh_opps = [o for o in self._opportunities
                   if {o.short_exchange, o.long_exchange} == {"Lighter", "Hyperliquid"}]
        if lh_opps:
            lines += [
                "",
                f"  ┌─ Lighter ↔ Hyperliquid 专区 (吃单费: Lighter=0 + HL=3.5bps×2=7bps 往返) ─────────────────────┐",
                f"  │ {'币种':<8} {'做空':<12} {'做多':<12} "
                f"{'毛利':>6} {'吃单费':>6} {'净利':>6} {'滑点':>5} {'基差':>5} {'调整后':>7} {'倒计时':>10} {'信号':>4} │",
                f"  │ {'─' * 105} │",
            ]
            for o in lh_opps[:15]:
                cd_str = ""
                if o.countdown_s > 0:
                    if o.countdown_s < 60:
                        cd_str = f"{o.countdown_s:.0f}s ⚡"
                    elif o.countdown_s < 3600:
                        cd_str = f"{o.countdown_s / 60:.1f}m"
                    else:
                        cd_str = f"{o.countdown_s / 3600:.1f}h"
                else:
                    cd_str = "—"
                adj = o.net_taker_bps - o.est_slippage_bps - o.basis_risk_bps
                sig = "🟢" if adj > 0 and o.liq_score in ("A", "B") else ("🟡" if o.net_taker_bps > 0 else "⚫")
                lines.append(
                    f"  │ {o.symbol:<8} {o.short_exchange:<12} {o.long_exchange:<12} "
                    f"{o.gross_bps:>+6.1f} {o.taker_fee_bps:>6.0f} {o.net_taker_bps:>+6.1f} {o.est_slippage_bps:>5.1f} "
                    f"{o.basis_risk_bps:>5.1f} {adj:>+7.1f} {cd_str:>10} {sig:>4} │")
            lines.append(f"  └{'─' * 111}┘")

        lines += ["", sep]
        self.logger.info("\n".join(lines))

    # ─── Snipe Execution Engine ───

    async def _init_adapters(self, exchanges: List[str], symbol: str):
        """Initialize exchange adapters for trading."""
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from funding_arb import (LighterAdapter, CexAdapter, HyperliquidAdapter)

        factory = {
            "Lighter": lambda: LighterAdapter(),
            "Binance": lambda: CexAdapter("binance"),
            "Hyperliquid": lambda: HyperliquidAdapter(),
        }
        for ex in exchanges:
            if ex in self._adapters:
                continue
            builder = factory.get(ex)
            if not builder:
                self.logger.warning(f"无交易适配器: {ex}")
                continue
            try:
                adapter = builder()
                await adapter.initialize(symbol)
                self._adapters[ex] = adapter
                self.logger.info(f"✓ {ex} 适配器就绪 ({symbol})")
            except Exception as e:
                self.logger.error(f"✗ {ex} 初始化失败: {e}")

    async def _shutdown_adapters(self):
        for name, adapter in self._adapters.items():
            try:
                await adapter.shutdown()
            except Exception:
                pass
        self._adapters.clear()

    async def _shutdown_feeds(self):
        if self._lighter_multi_feed:
            try:
                await self._lighter_multi_feed.disconnect()
            except Exception:
                pass
            self._lighter_multi_feed = None
        if self._feeds_task_lighter and not self._feeds_task_lighter.done():
            self._feeds_task_lighter.cancel()
            self._feeds_task_lighter = None

        if self._hl_multi_feed:
            try:
                await self._hl_multi_feed.disconnect()
            except Exception:
                pass
            self._hl_multi_feed = None
        if self._feeds_task_hl and not self._feeds_task_hl.done():
            self._feeds_task_hl.cancel()
            self._feeds_task_hl = None

    async def _execute_snipe(self, opp: SnipeOpportunity, size_usd: float):
        """Execute a single snipe: open hedge → wait for settlement → close."""
        short_ex = opp.short_exchange
        long_ex = opp.long_exchange
        sym = opp.symbol

        if short_ex not in self._adapters or long_ex not in self._adapters:
            self.logger.error(f"适配器未就绪: {short_ex} 或 {long_ex}")
            return False

        short_adapter = self._adapters[short_ex]
        long_adapter = self._adapters[long_ex]

        ref_price = await short_adapter.get_mid_price()
        if not ref_price or ref_price <= 0:
            ref_price = await long_adapter.get_mid_price()
        if not ref_price or ref_price <= 0:
            self.logger.error("无法获取参考价格")
            return False

        size = Decimal(str(size_usd)) / ref_price
        size = size.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)

        self.logger.info(
            f"🎯 狙击开始: {sym} short@{short_ex} long@{long_ex} "
            f"size={size} ref={ref_price} 毛利={opp.gross_bps:.1f}bps")

        # Phase 1: Open hedged positions in parallel (per-exchange latency)
        t_open = time.time()

        async def _timed_open(adapter, side, sz, ref):
            t0 = time.time()
            result = await adapter.open_position(side, sz, ref)
            return result, (time.time() - t0) * 1000

        short_task = asyncio.create_task(
            _timed_open(short_adapter, "sell", size, ref_price))
        long_task = asyncio.create_task(
            _timed_open(long_adapter, "buy", size, ref_price))

        short_result, long_result = await asyncio.gather(
            short_task, long_task, return_exceptions=True)

        if isinstance(short_result, Exception):
            short_fill, short_open_ms = None, 0
        else:
            short_fill, short_open_ms = short_result

        if isinstance(long_result, Exception):
            long_fill, long_open_ms = None, 0
        else:
            long_fill, long_open_ms = long_result

        if short_fill is None:
            self.logger.error(f"做空失败: {short_result if isinstance(short_result, Exception) else '无成交'}")
            if long_fill:
                self.logger.warning("回滚做多仓位...")
                await long_adapter.open_position("sell", long_fill, ref_price)
            return False

        if long_fill is None:
            self.logger.error(f"做多失败: {long_result if isinstance(long_result, Exception) else '无成交'}")
            self.logger.warning("回滚做空仓位...")
            await short_adapter.open_position("buy", short_fill, ref_price)
            return False

        open_ms = (time.time() - t_open) * 1000
        entry_short = short_adapter.last_fill_price or ref_price
        entry_long = long_adapter.last_fill_price or ref_price
        notional = float(size) * float(ref_price)

        self.logger.info(
            f"✓ 开仓完成 (总{open_ms:.0f}ms): "
            f"{short_ex}做空={short_open_ms:.0f}ms @{entry_short} | "
            f"{long_ex}做多={long_open_ms:.0f}ms @{entry_long} | "
            f"名义值=${notional:.2f}")

        # Phase 2: Wait for settlement
        wait_s = opp.countdown_s
        if wait_s > 0:
            self.logger.info(f"⏳ 等待结算: {wait_s:.0f}s")
            # Wait with periodic checks
            end_time = time.time() + wait_s + 2  # +2s buffer after settlement
            while time.time() < end_time and not self.stop_flag:
                remaining = end_time - time.time()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(1.0, remaining))
                if remaining < 10:
                    self.logger.info(f"  结算倒计时: {remaining:.1f}s")
        else:
            self.logger.info("结算时间已过,等待3s确认...")
            await asyncio.sleep(3)

        # Phase 3: Convergence-aware close
        # Track entry spread to detect convergence
        entry_spread_bps = (float(entry_short) - float(entry_long)) / float(ref_price) * 10000
        max_hold_end = time.time() + self.max_hold_after
        close_reason = "timeout"

        self.logger.info(
            f"📊 价差收敛监控: 入场价差={entry_spread_bps:+.2f}bps "
            f"目标={self.convergence_target:.1f}bps 止损={self.basis_stop_loss:.1f}bps "
            f"最大持仓={self.max_hold_after:.0f}s")

        while time.time() < max_hold_end and not self.stop_flag:
            short_mid = await short_adapter.get_mid_price()
            long_mid = await long_adapter.get_mid_price()
            if short_mid and long_mid:
                current_spread = (float(short_mid) - float(long_mid)) / float(ref_price) * 10000
                # Positive basis_pnl means spread converged (favorable)
                basis_pnl = entry_spread_bps - current_spread
                remaining = max_hold_end - time.time()

                if basis_pnl >= self.convergence_target:
                    close_reason = "converged"
                    self.logger.info(
                        f"✓ 价差收敛: {basis_pnl:+.2f}bps >= 目标{self.convergence_target:.1f}bps")
                    break
                if basis_pnl < -self.basis_stop_loss:
                    close_reason = "stop_loss"
                    self.logger.warning(
                        f"⚠ 基差止损: {basis_pnl:+.2f}bps < -{self.basis_stop_loss:.1f}bps")
                    break

                if int(remaining) % 5 == 0:
                    self.logger.info(
                        f"  收敛等待: 基差P&L={basis_pnl:+.2f}bps 剩余={remaining:.0f}s")

            await asyncio.sleep(0.5)

        if close_reason == "timeout":
            self.logger.info(f"⏰ 持仓超时 ({self.max_hold_after:.0f}s), 执行平仓")

        # Execute close (per-exchange latency)
        self.logger.info(f"⚡ 平仓中 (原因: {close_reason})...")
        t_close = time.time()

        close_ref = await short_adapter.get_mid_price() or ref_price

        close_short = asyncio.create_task(
            _timed_open(short_adapter, "buy", short_fill, close_ref))
        close_long = asyncio.create_task(
            _timed_open(long_adapter, "sell", long_fill, close_ref))

        cs_result, cl_result = await asyncio.gather(
            close_short, close_long, return_exceptions=True)

        if isinstance(cs_result, Exception):
            cs_fill, short_close_ms = None, 0
        else:
            cs_fill, short_close_ms = cs_result

        if isinstance(cl_result, Exception):
            cl_fill, long_close_ms = None, 0
        else:
            cl_fill, long_close_ms = cl_result

        close_ms = (time.time() - t_close) * 1000

        exit_short = short_adapter.last_fill_price or close_ref
        exit_long = long_adapter.last_fill_price or close_ref

        # P&L calculation
        short_pnl_bps = (float(entry_short) - float(exit_short)) / float(entry_short) * 10000
        long_pnl_bps = (float(exit_long) - float(entry_long)) / float(entry_long) * 10000
        basis_pnl_bps = short_pnl_bps + long_pnl_bps

        fee_bps = opp.taker_fee_bps
        funding_bps = opp.gross_bps
        net_bps = funding_bps + basis_pnl_bps - fee_bps
        net_usd = net_bps / 10000 * notional

        close_ok = (cs_fill is not None and cl_fill is not None)

        status = "✓" if close_ok else "⚠ 部分平仓"
        self.logger.info(
            f"{status} 狙击完成 ({close_reason}):\n"
            f"  ┌─ 延迟 ─────────────────────────────────────────┐\n"
            f"  │ 开仓: {short_ex}={short_open_ms:.0f}ms  {long_ex}={long_open_ms:.0f}ms  总={open_ms:.0f}ms │\n"
            f"  │ 平仓: {short_ex}={short_close_ms:.0f}ms  {long_ex}={long_close_ms:.0f}ms  总={close_ms:.0f}ms │\n"
            f"  └───────────────────────────────────────────────┘\n"
            f"  费率收益: {funding_bps:+.2f} bps\n"
            f"  基差损益: {basis_pnl_bps:+.2f} bps (空腿={short_pnl_bps:+.2f} 多腿={long_pnl_bps:+.2f})\n"
            f"  手续费:   {fee_bps:.2f} bps\n"
            f"  净P&L:    {net_bps:+.2f} bps = ${net_usd:+.4f}")

        # Log to CSV (includes close_reason + latency)
        self._log_snipe_csv(opp, notional, funding_bps, basis_pnl_bps,
                            fee_bps, net_bps, net_usd, close_reason,
                            open_ms, short_open_ms, long_open_ms,
                            close_ms, short_close_ms, long_close_ms)

        if not close_ok:
            self.logger.error("⚠ 有仓位未完全平仓,请手动检查!")

        return close_ok

    def _log_snipe_csv(self, opp, notional, funding, basis, fees, net_bps, net_usd,
                       close_reason: str = "immediate",
                       open_ms: float = 0, short_open_ms: float = 0, long_open_ms: float = 0,
                       close_ms: float = 0, short_close_ms: float = 0, long_close_ms: float = 0):
        csv_path = "snipe_trades.csv"
        exists = os.path.exists(csv_path)
        with open(csv_path, "a", newline="") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["timestamp", "symbol", "short_ex", "long_ex",
                            "notional_usd", "funding_bps", "basis_bps",
                            "fee_bps", "net_bps", "net_usd", "close_reason",
                            "open_total_ms", "short_open_ms", "long_open_ms",
                            "close_total_ms", "short_close_ms", "long_close_ms"])
            w.writerow([
                datetime.now(timezone.utc).isoformat(),
                opp.symbol, opp.short_exchange, opp.long_exchange,
                f"{notional:.2f}", f"{funding:.2f}", f"{basis:.2f}",
                f"{fees:.2f}", f"{net_bps:.2f}", f"{net_usd:.4f}",
                close_reason,
                f"{open_ms:.0f}", f"{short_open_ms:.0f}", f"{long_open_ms:.0f}",
                f"{close_ms:.0f}", f"{short_close_ms:.0f}", f"{long_close_ms:.0f}",
            ])

    async def _snipe_loop(self):
        """Auto-snipe: pick best opportunity approaching settlement, execute."""
        self._current_symbol: Optional[str] = None

        while not self.stop_flag:
            await asyncio.gather(
                self._fetch_all_rates(),
                self._fetch_liquidity(),
                return_exceptions=True)
            self._compute_opportunities()
            self._display()

            # Find best opportunity: must pass liquidity + risk filters (taker fees)
            best = None
            for opp in self._opportunities:
                if opp.net_taker_bps <= self.min_net_bps:
                    continue
                if opp.short_exchange not in self.trade_exchanges:
                    continue
                if opp.long_exchange not in self.trade_exchanges:
                    continue
                if opp.countdown_s <= 0 or opp.countdown_s > self.snipe_window_s:
                    continue
                # Liquidity filter: only A/B grade
                if opp.liq_score not in ("A", "B"):
                    self.logger.debug(f"跳过 {opp.symbol}: 流动性={opp.liq_score}")
                    continue
                # Risk-adjusted profit must be positive
                adjusted = opp.net_taker_bps - opp.est_slippage_bps - opp.basis_risk_bps
                if adjusted <= 0:
                    self.logger.debug(f"跳过 {opp.symbol}: 调整后净利={adjusted:.1f}bps")
                    continue
                best = opp
                break

            if best:
                adjusted = best.net_taker_bps - best.est_slippage_bps - best.basis_risk_bps
                self.logger.info(
                    f"🎯 发现狙击目标: {best.symbol} "
                    f"{best.short_exchange}→{best.long_exchange} "
                    f"吃单净利={best.net_taker_bps:.1f}bps 调整后={adjusted:.1f}bps "
                    f"流动={best.liq_score} 倒计时={best.countdown_s:.0f}s")

                # Re-init adapters if symbol changed
                needed_exs = [best.short_exchange, best.long_exchange]
                if best.symbol != self._current_symbol:
                    self.logger.info(f"切换币种: {self._current_symbol} → {best.symbol}")
                    await self._shutdown_adapters()
                    await self._init_adapters(needed_exs, best.symbol)
                    self._current_symbol = best.symbol

                if (best.short_exchange not in self._adapters or
                        best.long_exchange not in self._adapters):
                    self.logger.error("适配器初始化失败,跳过此机会")
                    await asyncio.sleep(self.poll_interval)
                    continue

                # Wait until pre_open_s before settlement
                wait_until_open = best.countdown_s - self.pre_open_s
                if wait_until_open > 0:
                    self.logger.info(f"等待 {wait_until_open:.0f}s 后开仓...")
                    for _ in range(int(wait_until_open * 10)):
                        if self.stop_flag:
                            return
                        await asyncio.sleep(0.1)

                # Re-check: rates still valid?
                await self._fetch_all_rates()
                self._compute_opportunities()
                still_valid = False
                for opp in self._opportunities:
                    if (opp.symbol == best.symbol and
                        opp.short_exchange == best.short_exchange and
                        opp.long_exchange == best.long_exchange and
                        opp.net_taker_bps > self.min_net_bps):
                        best = opp
                        still_valid = True
                        break

                if not still_valid:
                    self.logger.info("机会已消失,跳过")
                    continue

                # Pre-execution depth check via live adapters
                depth_ok, cross_spread = await self._check_depth_before_snipe(best)
                if not depth_ok:
                    self.logger.warning(
                        f"深度检查未通过 (跨所价差={cross_spread:.1f}bps),跳过")
                    continue

                self.logger.info(f"✓ 深度检查通过 (跨所价差={cross_spread:.1f}bps)")
                await self._execute_snipe(best, self.snipe_size_usd)
                self._current_symbol = None  # force re-init for next trade
                await asyncio.sleep(5)  # cooldown
            else:
                await asyncio.sleep(self.poll_interval)

    # ─── Main Loop ───

    async def run(self, mode: str = "monitor"):
        def on_stop(*_):
            self.stop_flag = True
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                asyncio.get_event_loop().add_signal_handler(sig, on_stop)
            except NotImplementedError:
                signal.signal(sig, on_stop)

        self.logger.info("扫描交易所上线币种...")
        await self._scan_listings()

        if mode == "snipe":
            self.logger.info(
                f"🔫 狙击模式: 交易所={self.trade_exchanges} "
                f"仓位=${self.snipe_size_usd} 窗口={self.snipe_window_s}s "
                f"提前={self.pre_open_s}s 最低净利={self.min_net_bps}bps\n"
                f"  收敛策略: 最大持仓={self.max_hold_after}s "
                f"收敛目标={self.convergence_target}bps "
                f"止损={self.basis_stop_loss}bps")
            try:
                await self._snipe_loop()
            finally:
                await self._shutdown_adapters()
                await self._shutdown_feeds()
                if self._session and not self._session.closed:
                    await self._session.close()
        else:
            self.logger.info("📊 监控模式 (不执行交易)")
            try:
                while not self.stop_flag:
                    t0 = time.time()
                    await asyncio.gather(
                        self._fetch_all_rates(),
                        self._fetch_liquidity(),
                        return_exceptions=True)

                    total_rates = sum(len(r) for r in self._rates.values())
                    self.logger.info(f"获取 {total_rates} 条费率数据")

                    self._compute_opportunities()
                    self._display()

                    elapsed = time.time() - t0
                    wait = max(1, self.poll_interval - elapsed)
                    for _ in range(int(wait * 10)):
                        if self.stop_flag:
                            break
                        await asyncio.sleep(0.1)
            finally:
                await self._shutdown_feeds()
                if self._session and not self._session.closed:
                    await self._session.close()
                self.logger.info("已停止")


def main():
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="资金费率狙击系统")
    parser.add_argument("--mode", choices=["monitor", "snipe"], default="monitor",
                        help="运行模式: monitor=仅监控, snipe=自动狙击")
    parser.add_argument("--min-net-bps", type=float, default=5.0,
                        help="最低净利润(bps)")
    parser.add_argument("--poll-interval", type=float, default=30.0,
                        help="轮询间隔(秒)")
    parser.add_argument("--top", type=int, default=30,
                        help="显示前N个机会")
    parser.add_argument("--size-usd", type=float, default=100.0,
                        help="每次狙击仓位(USD)")
    parser.add_argument("--window", type=float, default=120.0,
                        help="狙击窗口: 结算前多少秒内触发(秒)")
    parser.add_argument("--pre-open", type=float, default=5.0,
                        help="结算前多少秒开仓")
    parser.add_argument("--exchanges", type=str, default="Lighter,Hyperliquid",
                        help="狙击模式使用的交易所(逗号分隔)")
    parser.add_argument("--max-hold-after", type=float, default=60.0,
                        help="结算后最大持仓等待时间(秒), 等待价差收敛")
    parser.add_argument("--basis-stop-loss", type=float, default=5.0,
                        help="基差止损阈值(bps), 价差扩大超过此值立即平仓")
    parser.add_argument("--convergence-target", type=float, default=0.5,
                        help="价差收敛目标(bps), 基差P&L达到此值触发平仓")
    args = parser.parse_args()

    sniper = FundingSniper(
        min_net_bps=args.min_net_bps,
        poll_interval=args.poll_interval,
        top_n=args.top,
    )
    sniper.snipe_size_usd = args.size_usd
    sniper.snipe_window_s = args.window
    sniper.pre_open_s = args.pre_open
    sniper.trade_exchanges = [e.strip() for e in args.exchanges.split(",")]
    sniper.max_hold_after = args.max_hold_after
    sniper.basis_stop_loss = args.basis_stop_loss
    sniper.convergence_target = args.convergence_target

    asyncio.run(sniper.run(mode=args.mode))


if __name__ == "__main__":
    main()
