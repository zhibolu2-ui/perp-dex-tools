"""
DEX price feeds for additional decentralized perpetual exchanges.

Each feed connects to the exchange's public orderbook API (WebSocket or
REST polling) and normalises the data into the common OrderBookSnapshot
format used by the spread scanner.

Feeds that cannot connect (network issues, geo-blocking, service down)
will silently retry and show as disconnected in the dashboard.
"""

import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Tuple

import aiohttp

from .base_feed import BaseFeed

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Aevo  (wss://ws.aevo.xyz – orderbook throttled channel)
# REST fallback: https://api.aevo.xyz/orderbook?instrument_name=ETH-PERP
# ─────────────────────────────────────────────────────────────
class AevoFeed(BaseFeed):
    EXCHANGE_NAME = "Aevo"
    MAKER_FEE_BPS = 2.0   # 0.02%
    TAKER_FEE_BPS = 5.0   # 0.05%

    REST_URL = "https://api.aevo.xyz/orderbook"

    def __init__(self, symbol: str):
        super().__init__(symbol)
        self._instrument = f"{symbol.upper()}-PERP"

    async def connect(self) -> None:
        self._running = True
        poll_interval = 1.0

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    logger.info("[Aevo] REST polling: %s", self._instrument)
                    while self._running:
                        try:
                            params = {"instrument_name": self._instrument}
                            async with session.get(
                                self.REST_URL, params=params,
                                timeout=aiohttp.ClientTimeout(total=5),
                            ) as resp:
                                if resp.status != 200:
                                    await asyncio.sleep(poll_interval)
                                    continue
                                data = await resp.json()
                                bids = [(float(b[0]), float(b[1])) for b in data.get("bids", [])]
                                asks = [(float(a[0]), float(a[1])) for a in data.get("asks", [])]
                                await self._update_book(bids, asks)
                        except Exception as exc:
                            logger.warning("[Aevo] poll error: %s", exc)
                        await asyncio.sleep(poll_interval)
            except Exception as exc:
                logger.warning("[Aevo] session error: %s", exc)
                await asyncio.sleep(3)

    async def fetch_funding_rate(self) -> Optional[float]:
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://api.aevo.xyz/funding?instrument_name={self._instrument}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    rate = data.get("funding_rate")
                    if rate is not None:
                        self._snapshot.funding_rate = float(rate)
                        return float(rate)
        except Exception as exc:
            logger.warning("[Aevo] funding error: %s", exc)
        return None


# ─────────────────────────────────────────────────────────────
# Drift Protocol  (REST polling – https://dlob.drift.trade/l2)
# Prices are in 1e6 precision, sizes in 1e9
# ─────────────────────────────────────────────────────────────
class DriftFeed(BaseFeed):
    EXCHANGE_NAME = "Drift"
    MAKER_FEE_BPS = 0.0
    TAKER_FEE_BPS = 5.0   # ~0.05% for taker

    REST_URL = "https://dlob.drift.trade/l2"
    PRICE_PRECISION = 1e6
    SIZE_PRECISION = 1e9

    def __init__(self, symbol: str):
        super().__init__(symbol)
        self._market_name = f"{symbol.upper()}-PERP"

    async def connect(self) -> None:
        self._running = True
        poll_interval = 1.0

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    while self._running:
                        try:
                            params = {"marketName": self._market_name, "depth": 20}
                            async with session.get(
                                self.REST_URL, params=params,
                                timeout=aiohttp.ClientTimeout(total=5),
                            ) as resp:
                                if resp.status != 200:
                                    await asyncio.sleep(poll_interval)
                                    continue
                                data = await resp.json(content_type=None)
                                bids = [
                                    (float(b["price"]) / self.PRICE_PRECISION,
                                     float(b["size"]) / self.SIZE_PRECISION)
                                    for b in data.get("bids", [])
                                ]
                                asks = [
                                    (float(a["price"]) / self.PRICE_PRECISION,
                                     float(a["size"]) / self.SIZE_PRECISION)
                                    for a in data.get("asks", [])
                                ]
                                await self._update_book(bids, asks)
                        except Exception as exc:
                            logger.warning("[Drift] poll error: %s", exc)
                        await asyncio.sleep(poll_interval)
            except Exception as exc:
                logger.warning("[Drift] session error: %s", exc)
                await asyncio.sleep(3)


# ─────────────────────────────────────────────────────────────
# Vertex Protocol  (REST polling – Arbitrum DEX)
# May be geo-blocked from China
# ─────────────────────────────────────────────────────────────
class VertexFeed(BaseFeed):
    EXCHANGE_NAME = "Vertex"
    MAKER_FEE_BPS = 0.0
    TAKER_FEE_BPS = 2.0   # 0.02% for sequencer + maker rebate

    GATEWAY_URL = "https://gateway.prod.vertexprotocol.com/v1/query"
    PRODUCT_IDS = {"ETH": 4, "BTC": 2, "SOL": 6, "ARB": 8}

    def __init__(self, symbol: str):
        super().__init__(symbol)
        self._product_id = self.PRODUCT_IDS.get(symbol.upper(), 4)

    async def connect(self) -> None:
        self._running = True
        poll_interval = 1.5

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    while self._running:
                        try:
                            payload = {
                                "type": "market_liquidity",
                                "product_id": self._product_id,
                                "depth": 20,
                            }
                            async with session.post(
                                self.GATEWAY_URL, json=payload,
                                timeout=aiohttp.ClientTimeout(total=8),
                            ) as resp:
                                if resp.status != 200:
                                    await asyncio.sleep(poll_interval)
                                    continue
                                data = await resp.json()
                                ob = data.get("data", data)
                                bids_raw = ob.get("bids", [])
                                asks_raw = ob.get("asks", [])
                                bids = [(float(b[0]), float(b[1])) for b in bids_raw if len(b) >= 2]
                                asks = [(float(a[0]), float(a[1])) for a in asks_raw if len(a) >= 2]
                                await self._update_book(bids, asks)
                        except Exception as exc:
                            logger.warning("[Vertex] poll error: %s", exc)
                        await asyncio.sleep(poll_interval)
            except Exception as exc:
                logger.warning("[Vertex] session error: %s", exc)
                await asyncio.sleep(5)


# ─────────────────────────────────────────────────────────────
# Orderly Network  (WS public orderbook – Cross-chain DEX)
# Public WS does not need account_id for market data
# ─────────────────────────────────────────────────────────────
class OrderlyFeed(BaseFeed):
    EXCHANGE_NAME = "Orderly"
    MAKER_FEE_BPS = 2.0
    TAKER_FEE_BPS = 5.0

    WS_URL = "wss://ws-evm.orderly.org/ws/stream/0"

    def __init__(self, symbol: str):
        super().__init__(symbol)
        self._orderly_symbol = f"PERP_{symbol.upper()}_USDC"

    async def connect(self) -> None:
        import websockets

        self._running = True
        reconnect_delay = 1

        while self._running:
            try:
                async with websockets.connect(
                    self.WS_URL, ping_interval=20, close_timeout=5,
                ) as ws:
                    sub = json.dumps({
                        "id": "ob_sub",
                        "event": "subscribe",
                        "topic": f"{self._orderly_symbol}@orderbook",
                    })
                    await ws.send(sub)
                    logger.info("[Orderly] WS connected: %s", self._orderly_symbol)
                    reconnect_delay = 1

                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=10)
                        except asyncio.TimeoutError:
                            continue
                        msg = json.loads(raw)
                        topic = msg.get("topic", "")
                        if "orderbook" not in topic:
                            continue
                        data = msg.get("data", {})
                        bids = [(float(b[0]), float(b[1])) for b in data.get("bids", [])]
                        asks = [(float(a[0]), float(a[1])) for a in data.get("asks", [])]
                        if bids or asks:
                            await self._update_book(bids, asks)

            except Exception as exc:
                logger.warning("[Orderly] WS error: %s", exc)
            if self._running:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)


# ─────────────────────────────────────────────────────────────
# RabbitX  (REST polling – StarkNet-based perp DEX)
# May be geo-blocked
# ─────────────────────────────────────────────────────────────
class RabbitXFeed(BaseFeed):
    EXCHANGE_NAME = "RabbitX"
    MAKER_FEE_BPS = 2.0
    TAKER_FEE_BPS = 7.0   # 0.07%

    REST_URL = "https://api.prod.rabbitx.io/markets/orderbook"

    def __init__(self, symbol: str):
        super().__init__(symbol)
        self._market_id = f"{symbol.upper()}-USD"

    async def connect(self) -> None:
        self._running = True
        poll_interval = 1.5

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    while self._running:
                        try:
                            params = {"market_id": self._market_id}
                            async with session.get(
                                self.REST_URL, params=params,
                                timeout=aiohttp.ClientTimeout(total=8),
                            ) as resp:
                                if resp.status != 200:
                                    await asyncio.sleep(poll_interval)
                                    continue
                                data = await resp.json()
                                result = data if isinstance(data, dict) else {}
                                bids_raw = result.get("bids", [])
                                asks_raw = result.get("asks", [])
                                bids, asks = [], []
                                for b in bids_raw:
                                    if isinstance(b, (list, tuple)) and len(b) >= 2:
                                        bids.append((float(b[0]), float(b[1])))
                                    elif isinstance(b, dict):
                                        bids.append((float(b.get("price", 0)), float(b.get("size", b.get("quantity", 0)))))
                                for a in asks_raw:
                                    if isinstance(a, (list, tuple)) and len(a) >= 2:
                                        asks.append((float(a[0]), float(a[1])))
                                    elif isinstance(a, dict):
                                        asks.append((float(a.get("price", 0)), float(a.get("size", a.get("quantity", 0)))))
                                await self._update_book(bids, asks)
                        except Exception as exc:
                            logger.warning("[RabbitX] poll error: %s", exc)
                        await asyncio.sleep(poll_interval)
            except Exception as exc:
                logger.warning("[RabbitX] session error: %s", exc)
                await asyncio.sleep(5)


# ─────────────────────────────────────────────────────────────
# Bluefin  (REST polling – Sui-based perp DEX)
# ─────────────────────────────────────────────────────────────
class BluefinFeed(BaseFeed):
    EXCHANGE_NAME = "Bluefin"
    MAKER_FEE_BPS = 2.0
    TAKER_FEE_BPS = 6.0   # 0.06%

    REST_URL = "https://dapi.api.sui-prod.bluefin.io/orderbook"

    def __init__(self, symbol: str):
        super().__init__(symbol)
        self._bf_symbol = f"{symbol.upper()}-PERP"

    async def connect(self) -> None:
        self._running = True
        poll_interval = 1.5

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    while self._running:
                        try:
                            params = {"symbol": self._bf_symbol}
                            async with session.get(
                                self.REST_URL, params=params,
                                timeout=aiohttp.ClientTimeout(total=8),
                            ) as resp:
                                if resp.status != 200:
                                    await asyncio.sleep(poll_interval)
                                    continue
                                data = await resp.json()
                                ob = data.get("data", data) if isinstance(data, dict) else {}
                                bids_raw = ob.get("bids", [])
                                asks_raw = ob.get("asks", [])
                                bids = [(float(b[0]), float(b[1])) for b in bids_raw if isinstance(b, (list, tuple)) and len(b) >= 2]
                                asks = [(float(a[0]), float(a[1])) for a in asks_raw if isinstance(a, (list, tuple)) and len(a) >= 2]
                                if not bids and bids_raw and isinstance(bids_raw[0], dict):
                                    bids = [(float(b["price"]), float(b.get("size", b.get("quantity", 0)))) for b in bids_raw]
                                    asks = [(float(a["price"]), float(a.get("size", a.get("quantity", 0)))) for a in asks_raw]
                                await self._update_book(bids, asks)
                        except Exception as exc:
                            logger.warning("[Bluefin] poll error: %s", exc)
                        await asyncio.sleep(poll_interval)
            except Exception as exc:
                logger.warning("[Bluefin] session error: %s", exc)
                await asyncio.sleep(5)


# ─────────────────────────────────────────────────────────────
# Zeta Markets  (REST polling – Solana-based perp DEX)
# ─────────────────────────────────────────────────────────────
class ZetaFeed(BaseFeed):
    EXCHANGE_NAME = "Zeta"
    MAKER_FEE_BPS = 2.0
    TAKER_FEE_BPS = 5.0

    REST_URL = "https://dex-mainnet-webserver-ecs.zeta.markets"

    def __init__(self, symbol: str):
        super().__init__(symbol)
        self._zeta_market = f"{symbol.upper()}-PERP"

    async def connect(self) -> None:
        self._running = True
        poll_interval = 2.0

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    while self._running:
                        try:
                            url = f"{self.REST_URL}/orderbooks/{self._zeta_market}"
                            async with session.get(
                                url, timeout=aiohttp.ClientTimeout(total=8),
                            ) as resp:
                                if resp.status != 200:
                                    await asyncio.sleep(poll_interval)
                                    continue
                                data = await resp.json()
                                bids_raw = data.get("bids", [])
                                asks_raw = data.get("asks", [])
                                bids, asks = [], []
                                for b in bids_raw:
                                    if isinstance(b, (list, tuple)) and len(b) >= 2:
                                        bids.append((float(b[0]), float(b[1])))
                                    elif isinstance(b, dict):
                                        bids.append((float(b.get("price", 0)), float(b.get("size", 0))))
                                for a in asks_raw:
                                    if isinstance(a, (list, tuple)) and len(a) >= 2:
                                        asks.append((float(a[0]), float(a[1])))
                                    elif isinstance(a, dict):
                                        asks.append((float(a.get("price", 0)), float(a.get("size", 0))))
                                await self._update_book(bids, asks)
                        except Exception as exc:
                            logger.warning("[Zeta] poll error: %s", exc)
                        await asyncio.sleep(poll_interval)
            except Exception as exc:
                logger.warning("[Zeta] session error: %s", exc)
                await asyncio.sleep(5)
