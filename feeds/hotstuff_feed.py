"""
Hotstuff BBO feed — WS primary + REST fallback polling.

When the WS BBO stream goes stale (>STALE_THRESHOLD), a lightweight REST
poller kicks in automatically to keep data fresh.
"""

import asyncio
import json
import logging
import time
from typing import Callable, Optional

import websockets

from .base_feed import BaseFeed, OrderBookSnapshot

log = logging.getLogger("hotstuff_feed")

STALE_MS = 500          # trigger REST after this many ms without WS update
REST_POLL_MS = 400      # REST poll interval when WS is stale


class HotstuffFeed(BaseFeed):

    EXCHANGE_NAME = "Hotstuff"
    MAKER_FEE_BPS = -0.2
    TAKER_FEE_BPS = 2.5

    WS_URL = "wss://api.hotstuff.trade/ws"

    def __init__(self, symbol: str, on_update: Optional[Callable] = None):
        hs_symbol = symbol if "-PERP" in symbol else f"{symbol}-PERP"
        super().__init__(hs_symbol)
        self._raw_symbol = symbol
        self._on_update = on_update
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._last_update_ts: float = 0.0
        self._rest_task: Optional[asyncio.Task] = None
        self._info_client = None

    @property
    def last_update_ts(self) -> float:
        return self._last_update_ts

    def _ensure_info_client(self):
        if self._info_client is None:
            from hotstuff import InfoClient
            self._info_client = InfoClient()

    def _update_bbo(self, bid: float, ask: float,
                    bid_size: float = 0.0, ask_size: float = 0.0):
        from .base_feed import OrderBookLevel
        self._snapshot.best_bid = bid
        self._snapshot.best_ask = ask
        if bid_size > 0:
            self._snapshot.bids = [OrderBookLevel(bid, bid_size)]
        if ask_size > 0:
            self._snapshot.asks = [OrderBookLevel(ask, ask_size)]
        self._snapshot.timestamp = time.time()
        self._snapshot.connected = True
        self._last_update_ts = time.time()
        if self._on_update:
            self._on_update()

    async def _rest_poller(self):
        """Fallback REST poller — only fires when WS data is stale."""
        from hotstuff.methods.info.market import BBOParams
        self._ensure_info_client()
        params = BBOParams(symbol=self.symbol)

        while self._running:
            try:
                age_ms = (time.time() - self._last_update_ts) * 1000
                if age_ms < STALE_MS:
                    await asyncio.sleep(STALE_MS / 1000)
                    continue

                result = await asyncio.get_event_loop().run_in_executor(
                    None, self._info_client.bbo, params)

                if result and len(result) > 0:
                    item = result[0]
                    bid_str = (item.best_bid_price
                               if hasattr(item, "best_bid_price")
                               else item.get("best_bid_price"))
                    ask_str = (item.best_ask_price
                               if hasattr(item, "best_ask_price")
                               else item.get("best_ask_price"))
                    bid_sz = (item.best_bid_size
                              if hasattr(item, "best_bid_size")
                              else item.get("best_bid_size", "0"))
                    ask_sz = (item.best_ask_size
                              if hasattr(item, "best_ask_size")
                              else item.get("best_ask_size", "0"))
                    if bid_str and ask_str:
                        self._update_bbo(
                            float(bid_str), float(ask_str),
                            float(bid_sz or 0), float(ask_sz or 0))

            except Exception as e:
                log.debug(f"[Hotstuff] REST poll error: {e}")

            await asyncio.sleep(REST_POLL_MS / 1000)

    async def connect(self) -> None:
        self._running = True
        self._rest_task = asyncio.ensure_future(self._rest_poller())
        reconnect_delay = 1

        while self._running:
            try:
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=2**20,
                ) as ws:
                    self._ws = ws
                    sub = json.dumps({
                        "jsonrpc": "2.0",
                        "id": "1",
                        "method": "subscribe",
                        "params": {
                            "channel": "bbo",
                            "symbol": self.symbol,
                        },
                    })
                    await ws.send(sub)
                    log.info(f"[Hotstuff] BBO WS+REST connected: {self.symbol}")
                    self._snapshot.connected = True
                    reconnect_delay = 1

                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if msg.get("method") != "event":
                            continue
                        params = msg.get("params", {})
                        data = params.get("data", {})
                        if data.get("symbol") != self.symbol:
                            continue
                        bid = data.get("best_bid_price")
                        ask = data.get("best_ask_price")
                        if bid is not None and ask is not None:
                            bid_sz = float(data.get("best_bid_size", 0) or 0)
                            ask_sz = float(data.get("best_ask_size", 0) or 0)
                            self._update_bbo(
                                float(bid), float(ask), bid_sz, ask_sz)

            except websockets.exceptions.ConnectionClosed as e:
                log.debug(f"[Hotstuff] BBO WS disconnected: {e}")
            except Exception as e:
                log.warning(f"[Hotstuff] BBO WS error: {e}")

            self._snapshot.connected = False
            self._ws = None
            if self._running:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)

    async def disconnect(self) -> None:
        self._running = False
        if self._rest_task and not self._rest_task.done():
            self._rest_task.cancel()
            try:
                await self._rest_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._ws = None
