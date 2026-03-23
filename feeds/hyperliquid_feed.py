"""
Hyperliquid price feed via the official Python SDK.
No API key needed for public market data (l2Book, meta).
"""

import asyncio
import json
import logging
import time
import threading
from typing import Dict, Optional

from .base_feed import BaseFeed

logger = logging.getLogger(__name__)

HL_API_URL = "https://api.hyperliquid.xyz"
HL_WS_URL = "wss://api.hyperliquid.xyz/ws"


class HyperliquidFeed(BaseFeed):
    EXCHANGE_NAME = "Hyperliquid"
    # 官方 Perps Tier0（14d 加权成交量）：Taker 0.045% / Maker 0.015%
    # https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees
    MAKER_FEE_BPS = 1.5
    TAKER_FEE_BPS = 4.5

    def __init__(self, symbol: str):
        super().__init__(symbol)
        self._coin = symbol.upper()

    async def connect(self) -> None:
        """Connect via raw websocket (avoids SDK thread-model issues)."""
        import websockets

        self._running = True
        reconnect_delay = 1

        while self._running:
            try:
                async with websockets.connect(HL_WS_URL) as ws:
                    sub_msg = json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "l2Book", "coin": self._coin},
                    })
                    await ws.send(sub_msg)
                    reconnect_delay = 1
                    logger.info("[Hyperliquid] WS connected, coin=%s", self._coin)

                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            continue

                        data = json.loads(raw)
                        channel = data.get("channel")
                        if channel != "l2Book":
                            continue

                        book_data = data.get("data", {})
                        levels = book_data.get("levels", [[], []])
                        if len(levels) < 2:
                            continue

                        bid_raw, ask_raw = levels[0], levels[1]
                        bids = [(float(b["px"]), float(b["sz"])) for b in bid_raw]
                        asks = [(float(a["px"]), float(a["sz"])) for a in ask_raw]
                        await self._update_book(bids, asks)

            except Exception as exc:
                logger.warning("[Hyperliquid] WS error: %s", exc)

            if self._running:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)

    async def fetch_funding_rate(self) -> Optional[float]:
        """Fetch current predicted funding rate via REST."""
        import aiohttp
        try:
            async with aiohttp.ClientSession() as session:
                payload = {"type": "metaAndAssetCtxs"}
                async with session.post(
                    f"{HL_API_URL}/info", json=payload
                ) as resp:
                    data = await resp.json()
                    asset_ctxs = data[1] if isinstance(data, list) and len(data) > 1 else []
                    meta = data[0] if isinstance(data, list) else {}
                    universe = meta.get("universe", [])
                    for i, asset in enumerate(universe):
                        if asset.get("name", "").upper() == self._coin:
                            ctx = asset_ctxs[i] if i < len(asset_ctxs) else {}
                            rate = ctx.get("funding")
                            if rate is not None:
                                self._snapshot.funding_rate = float(rate)
                                return float(rate)
        except Exception as exc:
            logger.warning("[Hyperliquid] funding fetch error: %s", exc)
        return None
