"""
Generic ccxt-pro feed for any CEX that ccxt supports.
Covers Binance Futures, Bybit, OKX, dYdX, etc.
Uses ccxt.pro (async WebSocket) watch_order_book.
"""

import asyncio
import logging
from typing import Dict, Optional

from .base_feed import BaseFeed

logger = logging.getLogger(__name__)

EXCHANGE_CONFIGS: Dict[str, Dict] = {
    # ─── CEX ───
    "binance": {
        "class": "binanceusdm",
        "name": "Binance",
        "maker_bps": 2.0,
        "taker_bps": 4.0,
        "symbol_fmt": "{coin}/USDT:USDT",
    },
    "bybit": {
        "class": "bybit",
        "name": "Bybit",
        "maker_bps": 2.0,
        "taker_bps": 5.5,
        "symbol_fmt": "{coin}/USDT:USDT",
        "options": {"defaultType": "swap", "obLimit": 50},
    },
    "okx": {
        "class": "okx",
        "name": "OKX",
        "maker_bps": 2.0,
        "taker_bps": 5.0,
        "symbol_fmt": "{coin}/USDT:USDT",
        "options": {"defaultType": "swap"},
    },
    "gate": {
        "class": "gateio",
        "name": "Gate.io",
        "maker_bps": 1.5,
        "taker_bps": 5.0,
        "symbol_fmt": "{coin}/USDT:USDT",
        "options": {"defaultType": "swap"},
    },
    # ─── DEX (via ccxt) ───
    "dydx": {
        "class": "dydx",
        "name": "dYdX",
        "maker_bps": 1.0,
        "taker_bps": 5.0,
        "symbol_fmt": "{coin}-USD",
    },
    "paradex": {
        "class": "paradex",
        "name": "Paradex",
        "maker_bps": 2.0,
        "taker_bps": 4.0,
        "symbol_fmt": "{coin}/USD:USDC",
    },
}


class CcxtFeed(BaseFeed):
    """
    Wraps ccxt.pro async watch_order_book for any supported CEX.

    Usage:
        feed = CcxtFeed("ETH", exchange_id="binance")
    """

    def __init__(self, symbol: str, exchange_id: str = "binance"):
        self._exchange_id = exchange_id.lower()
        cfg = EXCHANGE_CONFIGS.get(self._exchange_id)
        if cfg is None:
            raise ValueError(
                f"Unsupported ccxt exchange: {exchange_id}. "
                f"Available: {list(EXCHANGE_CONFIGS.keys())}"
            )

        self.EXCHANGE_NAME = cfg["name"]
        self.MAKER_FEE_BPS = cfg["maker_bps"]
        self.TAKER_FEE_BPS = cfg["taker_bps"]
        super().__init__(symbol)

        self._ccxt_symbol = cfg["symbol_fmt"].format(coin=symbol.upper())
        self._ccxt_class_name = cfg["class"]
        self._extra_options = cfg.get("options", {})
        self._exchange = None

    async def connect(self) -> None:
        import ccxt.pro as ccxtpro

        self._running = True
        reconnect_delay = 1

        while self._running:
            try:
                ExchangeClass = getattr(ccxtpro, self._ccxt_class_name)
                self._exchange = ExchangeClass({
                    "enableRateLimit": True,
                    "options": self._extra_options,
                })
                logger.info("[%s] connecting, symbol=%s",
                            self.EXCHANGE_NAME, self._ccxt_symbol)

                ob_limit = self._extra_options.get("obLimit", 50)
                while self._running:
                    try:
                        ob = await asyncio.wait_for(
                            self._exchange.watch_order_book(
                                self._ccxt_symbol, limit=ob_limit
                            ),
                            timeout=10,
                        )
                    except asyncio.TimeoutError:
                        continue

                    bids = [(float(b[0]), float(b[1])) for b in ob.get("bids", [])]
                    asks = [(float(a[0]), float(a[1])) for a in ob.get("asks", [])]
                    await self._update_book(bids, asks)
                    reconnect_delay = 1

            except Exception as exc:
                logger.warning("[%s] error: %s", self.EXCHANGE_NAME, exc)

            finally:
                if self._exchange:
                    try:
                        await self._exchange.close()
                    except Exception:
                        pass
                    self._exchange = None

            if self._running:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)

    async def disconnect(self) -> None:
        self._running = False
        if self._exchange:
            try:
                await self._exchange.close()
            except Exception:
                pass

    async def fetch_funding_rate(self) -> Optional[float]:
        """Fetch current funding rate via ccxt REST."""
        import ccxt.pro as ccxtpro
        try:
            if self._exchange is None:
                ExchangeClass = getattr(ccxtpro, self._ccxt_class_name)
                self._exchange = ExchangeClass({
                    "enableRateLimit": True,
                    "options": self._extra_options,
                })

            fr = await self._exchange.fetch_funding_rate(self._ccxt_symbol)
            rate = fr.get("fundingRate")
            if rate is not None:
                self._snapshot.funding_rate = float(rate)
                return float(rate)
        except Exception as exc:
            logger.warning("[%s] funding fetch error: %s", self.EXCHANGE_NAME, exc)
        return None
