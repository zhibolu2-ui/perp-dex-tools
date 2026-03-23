"""
Lighter exchange price feed via native WebSocket.
Reuses the protocol from exchanges/lighter_custom_websocket.py but in a
read-only, scan-only mode (no auth / account subscription required).
"""

import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Tuple

import websockets

from .base_feed import BaseFeed, OrderBookLevel

logger = logging.getLogger(__name__)

LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_REST_URL = "https://mainnet.zklighter.elliot.ai"

SYMBOL_TO_MARKET: Dict[str, int] = {
    "ETH": 0,
    "BTC": 1,
    "SOL": 2,
    "DOGE": 3,
    "LINK": 8,
    "AVAX": 9,
    "SUI": 16,
    "TRUMP": 15,
    "HYPE": 24,
    "BNB": 25,
}


class LighterFeed(BaseFeed):
    EXCHANGE_NAME = "Lighter"
    MAKER_FEE_BPS = 0.0
    TAKER_FEE_BPS = 0.0  # Standard account

    def __init__(self, symbol: str, market_index: Optional[int] = None):
        super().__init__(symbol)
        self._market_index = market_index or SYMBOL_TO_MARKET.get(symbol.upper(), 2)
        self._book: Dict[str, Dict[float, float]] = {"bids": {}, "asks": {}}
        self._offset: Optional[int] = None
        self._snapshot_loaded = False
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

    async def connect(self) -> None:
        self._running = True
        reconnect_delay = 1
        while self._running:
            try:
                async with websockets.connect(LIGHTER_WS_URL) as ws:
                    self._ws = ws
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": f"order_book/{self._market_index}",
                    }))
                    reconnect_delay = 1
                    logger.info("[Lighter] WS connected, market=%s", self._market_index)

                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            continue

                        data = json.loads(raw)
                        msg_type = data.get("type", "")

                        if msg_type == "subscribed/order_book":
                            ob = data.get("order_book", {})
                            self._book = {"bids": {}, "asks": {}}
                            self._apply_levels("bids", ob.get("bids", []))
                            self._apply_levels("asks", ob.get("asks", []))
                            self._offset = ob.get("offset")
                            self._snapshot_loaded = True
                            await self._flush_book()
                            logger.info("[Lighter] snapshot: %d bids, %d asks",
                                        len(self._book["bids"]), len(self._book["asks"]))

                        elif msg_type == "update/order_book" and self._snapshot_loaded:
                            ob = data.get("order_book", {})
                            new_off = ob.get("offset")
                            if new_off is not None and self._offset is not None:
                                if new_off < self._offset + 1:
                                    continue
                                self._offset = new_off
                            self._apply_levels("bids", ob.get("bids", []))
                            self._apply_levels("asks", ob.get("asks", []))
                            await self._flush_book()

                        elif msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong"}))

            except (websockets.exceptions.ConnectionClosed, OSError) as exc:
                logger.warning("[Lighter] WS disconnected: %s", exc)
            except Exception as exc:
                logger.error("[Lighter] WS error: %s", exc)

            if self._running:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)

    def _apply_levels(self, side: str, updates: list) -> None:
        book = self._book[side]
        for u in updates:
            try:
                price = float(u["price"])
                size = float(u["size"])
                if size == 0:
                    book.pop(price, None)
                else:
                    book[price] = size
            except (KeyError, ValueError, TypeError):
                pass

    async def _flush_book(self) -> None:
        bids = list(self._book["bids"].items())
        asks = list(self._book["asks"].items())
        await self._update_book(bids, asks)
