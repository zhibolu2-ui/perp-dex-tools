"""
Base class for all exchange price feeds used by the spread scanner.
Each feed connects to an exchange via WebSocket (or REST polling)
and exposes a standardised OrderBookSnapshot.
"""

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class OrderBookLevel:
    price: float
    size: float


@dataclass
class OrderBookSnapshot:
    """Normalised order-book snapshot shared across all feeds."""
    exchange: str
    symbol: str
    best_bid: float = 0.0
    best_ask: float = 0.0
    bids: List[OrderBookLevel] = field(default_factory=list)  # sorted desc
    asks: List[OrderBookLevel] = field(default_factory=list)  # sorted asc
    funding_rate: Optional[float] = None
    funding_interval_h: float = 8.0
    timestamp: float = 0.0
    connected: bool = False

    @property
    def mid(self) -> float:
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / 2.0
        return 0.0

    @property
    def spread_bps(self) -> float:
        if self.mid:
            return (self.best_ask - self.best_bid) / self.mid * 10_000
        return 0.0

    def vwap_buy(self, qty: float) -> Optional[float]:
        """Weighted-average price to *buy* `qty` base (lift asks)."""
        return self._vwap(self.asks, qty)

    def vwap_sell(self, qty: float) -> Optional[float]:
        """Weighted-average price to *sell* `qty` base (hit bids)."""
        return self._vwap(self.bids, qty)

    @staticmethod
    def _vwap(levels: List[OrderBookLevel], qty: float) -> Optional[float]:
        if not levels or qty <= 0:
            return None
        remaining = qty
        cost = 0.0
        for lvl in levels:
            fill = min(remaining, lvl.size)
            cost += fill * lvl.price
            remaining -= fill
            if remaining <= 1e-12:
                break
        if remaining > 1e-12:
            return None  # not enough liquidity
        return cost / qty


class BaseFeed(ABC):
    """Abstract base for all exchange feeds."""

    # Subclasses must set these
    EXCHANGE_NAME: str = ""
    MAKER_FEE_BPS: float = 0.0
    TAKER_FEE_BPS: float = 0.0

    def __init__(self, symbol: str):
        self.symbol = symbol
        self._snapshot = OrderBookSnapshot(
            exchange=self.EXCHANGE_NAME, symbol=symbol
        )
        self._lock = asyncio.Lock()
        self._running = False

    @property
    def snapshot(self) -> OrderBookSnapshot:
        return self._snapshot

    @abstractmethod
    async def connect(self) -> None:
        """Start the WebSocket / polling loop. Blocks until disconnect."""
        ...

    async def disconnect(self) -> None:
        self._running = False

    async def _update_book(
        self,
        bids: List[Tuple[float, float]],
        asks: List[Tuple[float, float]],
    ) -> None:
        """Helper – atomically update the snapshot."""
        async with self._lock:
            self._snapshot.bids = [
                OrderBookLevel(p, s) for p, s in sorted(bids, reverse=True)
            ]
            self._snapshot.asks = [
                OrderBookLevel(p, s) for p, s in sorted(asks)
            ]
            if self._snapshot.bids:
                self._snapshot.best_bid = self._snapshot.bids[0].price
            if self._snapshot.asks:
                self._snapshot.best_ask = self._snapshot.asks[0].price
            self._snapshot.timestamp = time.time()
            self._snapshot.connected = True
