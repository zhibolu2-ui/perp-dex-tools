"""
Hyperliquid multi-symbol feed: ONE WebSocket connection for all coins.
Avoids 429 / 1008 rate-limiting caused by opening N connections.
"""

import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Set

import websockets

from .base_feed import OrderBookLevel, OrderBookSnapshot

logger = logging.getLogger(__name__)

HL_WS_URL = "wss://api.hyperliquid.xyz/ws"


class HyperliquidMultiFeed:
    """Single WS connection that subscribes to multiple l2Book channels."""

    EXCHANGE_NAME = "Hyperliquid"
    MAKER_FEE_BPS = 1.5
    TAKER_FEE_BPS = 4.5

    def __init__(self):
        self._coins: Set[str] = set()
        self._snapshots: Dict[str, OrderBookSnapshot] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._pending_subs: List[str] = []

    def add_coin(self, symbol: str) -> None:
        coin = symbol.upper()
        if coin in self._coins:
            return
        self._coins.add(coin)
        self._snapshots[coin] = OrderBookSnapshot(
            exchange=self.EXCHANGE_NAME, symbol=coin
        )
        self._pending_subs.append(coin)

    def snapshot(self, symbol: str) -> Optional[OrderBookSnapshot]:
        return self._snapshots.get(symbol.upper())

    @property
    def symbols(self) -> List[str]:
        return list(self._coins)

    async def connect(self) -> None:
        self._running = True
        reconnect_delay = 2

        while self._running:
            try:
                async with websockets.connect(
                    HL_WS_URL, ping_interval=30, ping_timeout=20,
                    close_timeout=5, max_size=2**22,
                ) as ws:
                    self._ws = ws

                    for coin in self._coins:
                        await ws.send(json.dumps({
                            "method": "subscribe",
                            "subscription": {"type": "l2Book", "coin": coin},
                        }))
                        await asyncio.sleep(0.1)

                    self._pending_subs.clear()
                    reconnect_delay = 2
                    logger.info(
                        "[Hyperliquid] multi-feed connected, %d coins: %s",
                        len(self._coins),
                        sorted(self._coins),
                    )

                    while self._running:
                        if self._pending_subs:
                            for coin in list(self._pending_subs):
                                await ws.send(json.dumps({
                                    "method": "subscribe",
                                    "subscription": {"type": "l2Book", "coin": coin},
                                }))
                                await asyncio.sleep(0.1)
                            self._pending_subs.clear()

                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            continue

                        data = json.loads(raw)
                        channel = data.get("channel")
                        if channel != "l2Book":
                            continue

                        book_data = data.get("data", {})
                        coin = book_data.get("coin", "").upper()
                        if not coin or coin not in self._coins:
                            continue

                        levels = book_data.get("levels", [[], []])
                        if len(levels) < 2:
                            continue

                        bid_raw, ask_raw = levels[0], levels[1]
                        bids = [(float(b["px"]), float(b["sz"])) for b in bid_raw]
                        asks = [(float(a["px"]), float(a["sz"])) for a in ask_raw]
                        await self._update_book(coin, bids, asks)

            except (websockets.exceptions.ConnectionClosed, OSError) as exc:
                logger.debug("[Hyperliquid] WS disconnected: %s", exc)
            except Exception as exc:
                logger.warning("[Hyperliquid] multi-feed error: %s", exc)

            self._ws = None
            if self._running:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)

    async def _update_book(
        self, coin: str,
        bids: List, asks: List,
    ) -> None:
        snap = self._snapshots.get(coin)
        if not snap:
            return
        async with self._lock:
            snap.bids = [
                OrderBookLevel(p, s) for p, s in sorted(bids, reverse=True)
            ]
            snap.asks = [
                OrderBookLevel(p, s) for p, s in sorted(asks)
            ]
            if snap.bids:
                snap.best_bid = snap.bids[0].price
            if snap.asks:
                snap.best_ask = snap.asks[0].price
            snap.timestamp = time.time()
            snap.connected = True

    async def disconnect(self) -> None:
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._ws = None
