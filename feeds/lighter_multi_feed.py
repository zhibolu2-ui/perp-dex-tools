"""
Lighter multi-symbol feed: ONE WebSocket connection for all markets.
Avoids 429 rate-limiting caused by opening N connections (one per symbol).

Uses sequential subscribe-and-wait to correctly assign initial snapshots,
then routes incremental updates via the 'channel' field or per-market
offset tracking.
"""

import asyncio
import json
import logging
import time
from collections import deque
from typing import Dict, List, Optional, Set, Tuple

import websockets

from .base_feed import OrderBookLevel, OrderBookSnapshot

logger = logging.getLogger(__name__)

LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"


class LighterMultiFeed:
    """Single WS connection that subscribes to multiple order_book channels."""

    EXCHANGE_NAME = "Lighter"
    MAKER_FEE_BPS = 0.0
    TAKER_FEE_BPS = 0.0

    def __init__(self):
        self._markets: Dict[str, int] = {}           # symbol -> market_id
        self._mid_to_sym: Dict[int, str] = {}         # market_id -> symbol
        self._books: Dict[int, Dict[str, Dict[float, float]]] = {}
        self._offsets: Dict[int, Optional[int]] = {}
        self._snapshot_loaded: Set[int] = set()
        self._snapshots: Dict[str, OrderBookSnapshot] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._pending_subs: List[str] = []
        self._snapshot_queue: deque = deque()

    def add_market(self, symbol: str, market_id: int) -> None:
        sym = symbol.upper()
        self._markets[sym] = market_id
        self._mid_to_sym[market_id] = sym
        self._books[market_id] = {"bids": {}, "asks": {}}
        self._offsets[market_id] = None
        self._snapshots[sym] = OrderBookSnapshot(
            exchange=self.EXCHANGE_NAME, symbol=sym
        )
        self._pending_subs.append(sym)

    def snapshot(self, symbol: str) -> Optional[OrderBookSnapshot]:
        return self._snapshots.get(symbol.upper())

    @property
    def symbols(self) -> List[str]:
        return list(self._markets.keys())

    async def connect(self) -> None:
        self._running = True
        reconnect_delay = 2

        while self._running:
            try:
                async with websockets.connect(
                    LIGHTER_WS_URL, ping_interval=30, ping_timeout=20,
                    close_timeout=5, max_size=2**22,
                ) as ws:
                    self._ws = ws
                    self._snapshot_loaded.clear()
                    self._snapshot_queue.clear()
                    for mid in self._markets.values():
                        self._offsets[mid] = None

                    await self._subscribe_all(ws)
                    self._pending_subs.clear()
                    reconnect_delay = 2
                    logger.info(
                        "[Lighter] multi-feed connected, %d markets: %s",
                        len(self._markets),
                        list(self._markets.keys())[:15],
                    )

                    while self._running:
                        if self._pending_subs:
                            new_syms = list(self._pending_subs)
                            self._pending_subs.clear()
                            for sym in new_syms:
                                mid = self._markets.get(sym)
                                if mid is not None:
                                    self._snapshot_queue.append(mid)
                                    await ws.send(json.dumps({
                                        "type": "subscribe",
                                        "channel": f"order_book/{mid}",
                                    }))
                                    await asyncio.sleep(0.1)

                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            continue

                        data = json.loads(raw)
                        msg_type = data.get("type", "")

                        if msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                            continue

                        if msg_type == "subscribed/order_book":
                            mid = self._resolve_snapshot_market(data)
                            if mid is None:
                                continue
                            ob = data.get("order_book", {})
                            self._books[mid] = {"bids": {}, "asks": {}}
                            self._apply_levels(mid, "bids", ob.get("bids", []))
                            self._apply_levels(mid, "asks", ob.get("asks", []))
                            self._offsets[mid] = ob.get("offset")
                            self._snapshot_loaded.add(mid)
                            await self._flush_book(mid)
                            sym = self._mid_to_sym.get(mid, "?")
                            logger.info(
                                "[Lighter] snapshot %s(mid=%d): %d bids, %d asks",
                                sym, mid,
                                len(self._books[mid]["bids"]),
                                len(self._books[mid]["asks"]),
                            )

                        elif msg_type == "update/order_book":
                            mid = self._resolve_update_market(data)
                            if mid is None or mid not in self._snapshot_loaded:
                                continue
                            ob = data.get("order_book", {})
                            new_off = ob.get("offset")
                            if new_off is not None and self._offsets[mid] is not None:
                                if new_off < self._offsets[mid] + 1:
                                    continue
                                self._offsets[mid] = new_off
                            self._apply_levels(mid, "bids", ob.get("bids", []))
                            self._apply_levels(mid, "asks", ob.get("asks", []))
                            await self._flush_book(mid)

            except (websockets.exceptions.ConnectionClosed, OSError) as exc:
                logger.debug("[Lighter] WS disconnected: %s", exc)
            except Exception as exc:
                logger.warning("[Lighter] multi-feed error: %s", exc)

            self._ws = None
            if self._running:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)

    async def _subscribe_all(self, ws) -> None:
        """Subscribe to all markets sequentially."""
        self._snapshot_queue.clear()
        for sym in list(self._markets.keys()):
            mid = self._markets[sym]
            self._snapshot_queue.append(mid)
            await ws.send(json.dumps({
                "type": "subscribe",
                "channel": f"order_book/{mid}",
            }))
            await asyncio.sleep(0.08)

    def _parse_channel_mid(self, data: dict) -> Optional[int]:
        """Try to extract market_id from the 'channel' field."""
        channel = data.get("channel", "")
        if "/" in channel:
            try:
                return int(channel.rsplit("/", 1)[1])
            except (ValueError, IndexError):
                pass
        return None

    def _resolve_snapshot_market(self, data: dict) -> Optional[int]:
        """Identify which market a 'subscribed/order_book' belongs to.
        Priority: channel field > subscription queue order."""
        mid = self._parse_channel_mid(data)
        if mid is not None and mid in self._mid_to_sym:
            if mid in self._snapshot_queue:
                self._snapshot_queue.remove(mid)
            return mid

        if self._snapshot_queue:
            return self._snapshot_queue.popleft()

        return None

    def _resolve_update_market(self, data: dict) -> Optional[int]:
        """Identify which market an 'update/order_book' belongs to.
        Priority: channel field > offset sequence matching."""
        mid = self._parse_channel_mid(data)
        if mid is not None and mid in self._mid_to_sym:
            return mid

        ob = data.get("order_book", {})
        new_off = ob.get("offset")
        if new_off is not None:
            for candidate_mid, cur_off in self._offsets.items():
                if candidate_mid not in self._snapshot_loaded:
                    continue
                if cur_off is not None and new_off == cur_off + 1:
                    return candidate_mid

        return None

    def _apply_levels(self, mid: int, side: str, updates: list) -> None:
        book = self._books[mid][side]
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

    async def _flush_book(self, mid: int) -> None:
        sym = self._mid_to_sym.get(mid)
        if not sym:
            return
        book = self._books[mid]
        bids = sorted(book["bids"].items(), reverse=True)
        asks = sorted(book["asks"].items())

        snap = self._snapshots[sym]
        async with self._lock:
            snap.bids = [OrderBookLevel(p, s) for p, s in bids]
            snap.asks = [OrderBookLevel(p, s) for p, s in asks]
            if snap.bids:
                snap.best_bid = snap.bids[0].price
            if snap.asks:
                snap.best_ask = snap.asks[0].price
            snap.timestamp = time.time()
            if snap.best_bid > 0 and snap.best_ask > 0 and snap.best_bid >= snap.best_ask:
                snap.connected = False
                logger.debug(
                    "[Lighter] crossed book %s: bid=%.6f >= ask=%.6f, skipping",
                    sym, snap.best_bid, snap.best_ask,
                )
                return
            snap.connected = True

    async def disconnect(self) -> None:
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._ws = None
