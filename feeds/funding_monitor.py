"""
FundingRateMonitor – periodically fetches funding rates from
all connected exchanges and computes the annualised spread
relative to Lighter.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import aiohttp

from .base_feed import BaseFeed

logger = logging.getLogger(__name__)

LIGHTER_REST = "https://mainnet.zklighter.elliot.ai"


@dataclass
class FundingSnapshot:
    exchange: str
    rate: Optional[float] = None          # current 8h rate, e.g. 0.0001 = 0.01%
    interval_h: float = 8.0
    annualized_pct: Optional[float] = None
    timestamp: float = 0.0


@dataclass
class FundingSpread:
    exchange: str
    lighter_rate: Optional[float]          # per-hour rate
    exchange_rate: Optional[float]         # per-hour rate (normalised)
    spread_1h: Optional[float] = None      # exchange_hourly - lighter_hourly
    annualized_pct: Optional[float] = None


class FundingRateMonitor:
    """
    Periodically polls funding rates from all feeds that support it,
    plus Lighter's own funding rate, and computes spreads.
    """

    def __init__(
        self,
        symbol: str,
        feeds: List[BaseFeed],
        poll_interval: float = 60.0,
    ):
        self.symbol = symbol.upper()
        self.feeds = feeds
        self.poll_interval = poll_interval
        self._lighter_rate: Optional[float] = None
        self._rates: Dict[str, FundingSnapshot] = {}
        self._running = False

    @property
    def lighter_funding(self) -> Optional[float]:
        return self._lighter_rate

    def get_spreads(self) -> List[FundingSpread]:
        results = []
        lighter_hourly = self._lighter_rate  # already per-hour
        for name, snap in self._rates.items():
            spread_1h = None
            ann = None
            ex_hourly = None
            if snap.rate is not None:
                ex_hourly = snap.rate / snap.interval_h
            if ex_hourly is not None and lighter_hourly is not None:
                spread_1h = ex_hourly - lighter_hourly
                ann = spread_1h * 365 * 24 * 100  # in %
            results.append(FundingSpread(
                exchange=name,
                lighter_rate=lighter_hourly,
                exchange_rate=ex_hourly,
                spread_1h=spread_1h,
                annualized_pct=ann,
            ))
        results.sort(key=lambda x: abs(x.annualized_pct or 0), reverse=True)
        return results

    async def run(self) -> None:
        self._running = True
        while self._running:
            await self._poll_all()
            await asyncio.sleep(self.poll_interval)

    async def stop(self) -> None:
        self._running = False

    async def _poll_all(self) -> None:
        tasks = [self._poll_lighter()]
        for feed in self.feeds:
            tasks.append(self._poll_feed(feed))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _poll_lighter(self) -> None:
        """Fetch funding rates from Lighter aggregated endpoint.

        The ``/api/v1/funding-rates`` endpoint returns rates for **all**
        exchanges (binance, bybit, hyperliquid, lighter) in one call.
        We use the ``exchange == "lighter"`` entry for Lighter's own rate
        and cache the rest as fallback for feeds whose individual
        ``fetch_funding_rate`` might fail.
        """
        try:
            url = f"{LIGHTER_REST}/api/v1/funding-rates"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        logger.debug("[FundingMonitor] Lighter API %s returned %s", url, resp.status)
                        return
                    data = await resp.json()

            rates_list = data.get("funding_rates", [])

            exchange_name_map = {
                "lighter": None,
                "binance": "Binance",
                "bybit": "Bybit",
                "hyperliquid": "Hyperliquid",
                "okx": "OKX",
            }

            for item in rates_list:
                sym = item.get("symbol", "")
                if sym.upper() != self.symbol:
                    continue

                rate = item.get("rate")
                if rate is None:
                    continue
                rate = float(rate)

                src = item.get("exchange", "").lower()

                if src == "lighter":
                    # Lighter settles every 1h; API returns per-hour rate
                    self._lighter_rate = rate
                    continue

                display_name = exchange_name_map.get(src)
                if display_name is None:
                    continue

                # All rates from aggregated API are in 8h-premium format
                hourly = rate / 8.0
                if display_name not in self._rates:
                    self._rates[display_name] = FundingSnapshot(
                        exchange=display_name,
                        rate=rate,
                        interval_h=8.0,
                        annualized_pct=hourly * (365 * 24) * 100,
                        timestamp=time.time(),
                    )
                else:
                    existing = self._rates[display_name]
                    if existing.timestamp < time.time() - self.poll_interval * 0.8:
                        existing.rate = rate
                        existing.interval_h = 8.0
                        existing.annualized_pct = hourly * (365 * 24) * 100
                        existing.timestamp = time.time()

        except Exception as exc:
            logger.debug("[FundingMonitor] Lighter funding error: %s", exc)

    async def _poll_feed(self, feed: BaseFeed) -> None:
        """Try the feed's own fetch_funding_rate, else skip.

        Rates from individual feeds are per-period: Hyperliquid is 1h,
        most CEX are 8h. We record ``interval_h`` so ``get_spreads``
        can normalise before comparing with Lighter's hourly rate.
        """
        try:
            fn = getattr(feed, "fetch_funding_rate", None)
            if fn is None:
                return
            rate = await fn()
            if rate is not None:
                interval = getattr(feed, "_funding_interval_h", None)
                if interval is None:
                    interval = 1.0 if "hyperliquid" in feed.EXCHANGE_NAME.lower() else 8.0
                hourly = rate / interval
                self._rates[feed.EXCHANGE_NAME] = FundingSnapshot(
                    exchange=feed.EXCHANGE_NAME,
                    rate=rate,
                    interval_h=interval,
                    annualized_pct=hourly * 365 * 24 * 100,
                    timestamp=time.time(),
                )
        except Exception as exc:
            logger.debug("[FundingMonitor] %s error: %s", feed.EXCHANGE_NAME, exc)
