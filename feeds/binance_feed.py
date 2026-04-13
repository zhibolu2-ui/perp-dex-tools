"""
Binance USDT-M Futures BBO feed — public WS, no auth needed.
Provides real-time best bid/ask for predictive pricing.
Push frequency: ~100ms (bookTicker stream).
"""

import asyncio
import collections
import json
import logging
import time
from typing import Optional

import websockets

log = logging.getLogger("binance_feed")

WS_URL = "wss://fstream.binance.com/ws"


class BinanceFeed:
    """Lightweight Binance futures BBO feed for price prediction."""

    def __init__(self, symbol: str = "BTC"):
        _s = symbol.upper()
        self._ws_symbol = f"{_s.lower()}usdt"
        self._running = False
        self._ws = None

        self.best_bid: float = 0.0
        self.best_ask: float = 0.0
        self.last_update_ts: float = 0.0

        self._price_history: collections.deque = collections.deque(maxlen=100)

    @property
    def connected(self) -> bool:
        return self.last_update_ts > 0 and (time.time() - self.last_update_ts) < 5

    @property
    def mid(self) -> float:
        if self.best_bid > 0 and self.best_ask > 0:
            return (self.best_bid + self.best_ask) / 2.0
        return 0.0

    def get_momentum(self, lookback_sec: float = 2.0) -> float:
        """Calculate price momentum over lookback period.
        Returns: predicted price change in $ (positive = price going up)."""
        now = time.time()
        cutoff = now - lookback_sec
        recent = [(ts, mid) for ts, mid in self._price_history if ts >= cutoff]
        if len(recent) < 2:
            return 0.0
        oldest_mid = recent[0][1]
        newest_mid = recent[-1][1]
        return newest_mid - oldest_mid

    def get_momentum_bps(self, lookback_sec: float = 2.0) -> float:
        """Momentum in bps."""
        mom = self.get_momentum(lookback_sec)
        if self.mid > 0:
            return mom / self.mid * 10000
        return 0.0

    async def connect(self) -> None:
        self._running = True
        stream = f"{self._ws_symbol}@bookTicker"
        url = f"{WS_URL}/{stream}"
        reconnect_delay = 1

        while self._running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self._ws = ws
                    log.info(f"[Binance] BBO WS connected: {self._ws_symbol}")
                    reconnect_delay = 1

                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        bid = msg.get("b")
                        ask = msg.get("a")
                        if bid and ask:
                            self.best_bid = float(bid)
                            self.best_ask = float(ask)
                            self.last_update_ts = time.time()
                            _mid = (self.best_bid + self.best_ask) / 2.0
                            self._price_history.append((self.last_update_ts, _mid))

            except asyncio.CancelledError:
                return
            except Exception as e:
                log.warning(f"[Binance] WS error: {e}")

            self._ws = None
            if self._running:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)

    async def disconnect(self) -> None:
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
