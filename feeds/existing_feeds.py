"""
Lightweight read-only feeds for exchanges already integrated in
the hedge_mode modules.  These connect directly to public WebSocket
endpoints — no API keys or trading credentials required.
"""

import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Tuple

import websockets
import aiohttp

from .base_feed import BaseFeed, OrderBookLevel

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# 01 Exchange  (wss://zo-mainnet.n1.xyz/ws/)
# ─────────────────────────────────────────────────────────────
class O1Feed(BaseFeed):
    EXCHANGE_NAME = "01 Exchange"
    MAKER_FEE_BPS = 1.0   # 0.01%
    TAKER_FEE_BPS = 3.5   # 0.035%

    def __init__(self, symbol: str):
        super().__init__(symbol)
        self._o1_symbol = f"{symbol.upper()}USD"
        self._book: Dict[str, Dict[float, float]] = {"bids": {}, "asks": {}}

    async def connect(self) -> None:
        self._running = True
        url = f"wss://zo-mainnet.n1.xyz/ws/deltas@{self._o1_symbol}"
        reconnect_delay = 1

        while self._running:
            try:
                async with websockets.connect(
                    url, ping_interval=None, ping_timeout=None, close_timeout=5,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self._book = {"bids": {}, "asks": {}}
                    logger.info("[01 Exchange] WS connected: %s", self._o1_symbol)
                    reconnect_delay = 1

                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            continue
                        data = json.loads(raw)
                        delta = data.get("delta")
                        if not delta:
                            continue
                        for p, s in delta.get("bids", []):
                            pf, sf = float(p), float(s)
                            if sf > 0:
                                self._book["bids"][pf] = sf
                            else:
                                self._book["bids"].pop(pf, None)
                        for p, s in delta.get("asks", []):
                            pf, sf = float(p), float(s)
                            if sf > 0:
                                self._book["asks"][pf] = sf
                            else:
                                self._book["asks"].pop(pf, None)
                        await self._update_book(
                            list(self._book["bids"].items()),
                            list(self._book["asks"].items()),
                        )

            except Exception as exc:
                logger.warning("[01 Exchange] WS error: %s", exc)
            if self._running:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)


# ─────────────────────────────────────────────────────────────
# GRVT  (REST polling – POST /full/v1/book)
# ─────────────────────────────────────────────────────────────
class GrvtFeed(BaseFeed):
    EXCHANGE_NAME = "GRVT"
    MAKER_FEE_BPS = -0.1   # -0.001% rebate
    TAKER_FEE_BPS = 4.5    # 0.045%

    REST_URL = "https://market-data.grvt.io/full/v1/book"
    SYMBOL_MAP = {"BTC": "BTC_USDT_Perp", "ETH": "ETH_USDT_Perp", "SOL": "SOL_USDT_Perp"}

    def __init__(self, symbol: str):
        super().__init__(symbol)
        self._grvt_symbol = self.SYMBOL_MAP.get(symbol.upper(), f"{symbol.upper()}_USDT_Perp")

    async def connect(self) -> None:
        self._running = True
        poll_interval = 1.0

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    logger.info("[GRVT] REST polling: %s", self._grvt_symbol)
                    while self._running:
                        try:
                            body = {"instrument": self._grvt_symbol, "depth": 10}
                            async with session.post(
                                self.REST_URL, json=body,
                                timeout=aiohttp.ClientTimeout(total=5),
                            ) as resp:
                                if resp.status != 200:
                                    await asyncio.sleep(poll_interval)
                                    continue
                                data = await resp.json(content_type=None)
                                result = data.get("result", data)
                                bids = [
                                    (float(b["price"]), float(b["size"]))
                                    for b in result.get("bids", [])
                                ]
                                asks = [
                                    (float(a["price"]), float(a["size"]))
                                    for a in result.get("asks", [])
                                ]
                                await self._update_book(bids, asks)
                        except Exception as exc:
                            logger.warning("[GRVT] poll error: %s", exc)
                        await asyncio.sleep(poll_interval)
            except Exception as exc:
                logger.warning("[GRVT] session error: %s", exc)
                await asyncio.sleep(3)


# ─────────────────────────────────────────────────────────────
# EdgeX  (wss://quote.edgex.exchange/api/v1/public/ws)
# Subscribe to depth.{contractId}.15 after fetching metadata
# ─────────────────────────────────────────────────────────────
class EdgeXFeed(BaseFeed):
    EXCHANGE_NAME = "EdgeX"
    MAKER_FEE_BPS = 2.0
    TAKER_FEE_BPS = 5.0

    WS_URL = "wss://quote.edgex.exchange/api/v1/public/ws"
    CONTRACT_IDS = {"ETH": "10000002", "BTC": "10000001", "SOL": "10000003"}

    def __init__(self, symbol: str):
        super().__init__(symbol)
        self._contract_id = self.CONTRACT_IDS.get(symbol.upper(), "10000002")
        self._book: Dict[str, Dict[float, float]] = {"bids": {}, "asks": {}}

    async def connect(self) -> None:
        self._running = True
        reconnect_delay = 1

        while self._running:
            try:
                async with websockets.connect(
                    self.WS_URL, ping_interval=None, close_timeout=5,
                ) as ws:
                    channel = f"depth.{self._contract_id}.15"
                    await ws.send(json.dumps({"type": "subscribe", "channel": channel}))
                    logger.info("[EdgeX] WS connected, subscribed: %s", channel)
                    reconnect_delay = 1
                    self._book = {"bids": {}, "asks": {}}

                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=15)
                        except asyncio.TimeoutError:
                            continue
                        msg = json.loads(raw)
                        msg_type = msg.get("type", "")

                        if msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong", "time": msg.get("time", "")}))
                            continue

                        if msg_type not in ("quote-event", "payload"):
                            continue

                        content = msg.get("content", {})
                        items = content.get("data", [])
                        if not items:
                            continue
                        item = items[0]
                        depth_type = item.get("depthType", content.get("dataType", ""))
                        is_snapshot = depth_type.upper() == "SNAPSHOT"

                        if is_snapshot:
                            self._book = {"bids": {}, "asks": {}}

                        for entry in item.get("bids", []):
                            p, s = float(entry["price"]), float(entry["size"])
                            if s > 0:
                                self._book["bids"][p] = s
                            else:
                                self._book["bids"].pop(p, None)

                        for entry in item.get("asks", []):
                            p, s = float(entry["price"]), float(entry["size"])
                            if s > 0:
                                self._book["asks"][p] = s
                            else:
                                self._book["asks"].pop(p, None)

                        await self._update_book(
                            list(self._book["bids"].items()),
                            list(self._book["asks"].items()),
                        )

            except Exception as exc:
                logger.warning("[EdgeX] WS error: %s", exc)
            if self._running:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)


# ─────────────────────────────────────────────────────────────
# Backpack  (REST polling – public orderbook)
# ─────────────────────────────────────────────────────────────
class BackpackFeed(BaseFeed):
    EXCHANGE_NAME = "Backpack"
    MAKER_FEE_BPS = 2.0
    TAKER_FEE_BPS = 5.0

    def __init__(self, symbol: str):
        super().__init__(symbol)
        self._bp_symbol = f"{symbol.upper()}_USDC_PERP"

    async def connect(self) -> None:
        self._running = True
        url = f"https://api.backpack.exchange/api/v1/depth?symbol={self._bp_symbol}"
        poll_interval = 1.0

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    while self._running:
                        try:
                            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                                if resp.status != 200:
                                    await asyncio.sleep(poll_interval)
                                    continue
                                data = await resp.json()
                                bids = [(float(b[0]), float(b[1])) for b in data.get("bids", [])]
                                asks = [(float(a[0]), float(a[1])) for a in data.get("asks", [])]
                                await self._update_book(bids, asks)
                        except Exception as exc:
                            logger.warning("[Backpack] poll error: %s", exc)
                        await asyncio.sleep(poll_interval)
            except Exception as exc:
                logger.warning("[Backpack] session error: %s", exc)
                await asyncio.sleep(3)


# ─────────────────────────────────────────────────────────────
# Apex Omni  (REST polling – public orderbook)
# Symbol format: ETHUSDT (no separator). Data in "a" (asks) / "b" (bids).
# ─────────────────────────────────────────────────────────────
class ApexFeed(BaseFeed):
    EXCHANGE_NAME = "Apex"
    MAKER_FEE_BPS = 2.0
    TAKER_FEE_BPS = 5.0

    REST_URL = "https://omni.apex.exchange/api/v3/depth"

    def __init__(self, symbol: str):
        super().__init__(symbol)
        self._apex_symbol = f"{symbol.upper()}USDT"

    async def connect(self) -> None:
        self._running = True
        poll_interval = 1.0

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    logger.info("[Apex] REST polling: %s", self._apex_symbol)
                    while self._running:
                        try:
                            params = {"symbol": self._apex_symbol, "limit": 20}
                            async with session.get(
                                self.REST_URL, params=params,
                                timeout=aiohttp.ClientTimeout(total=5),
                            ) as resp:
                                if resp.status != 200:
                                    await asyncio.sleep(poll_interval)
                                    continue
                                data = await resp.json(content_type=None)
                                ob = data.get("data", data)
                                raw_asks = ob.get("a", ob.get("asks", []))
                                raw_bids = ob.get("b", ob.get("bids", []))
                                if raw_asks is None:
                                    raw_asks = []
                                if raw_bids is None:
                                    raw_bids = []
                                bids = [(float(b[0]), float(b[1])) for b in raw_bids if isinstance(b, (list, tuple)) and len(b) >= 2]
                                asks = [(float(a[0]), float(a[1])) for a in raw_asks if isinstance(a, (list, tuple)) and len(a) >= 2]
                                await self._update_book(bids, asks)
                        except Exception as exc:
                            logger.warning("[Apex] poll error: %s", exc)
                        await asyncio.sleep(poll_interval)
            except Exception as exc:
                logger.warning("[Apex] session error: %s", exc)
                await asyncio.sleep(3)


# ─────────────────────────────────────────────────────────────
# Extended (Starknet) — REST orderbook
# Docs: https://x10xchange.github.io/x10-documentation/
# GET /api/v1/info/markets/{market}/orderbook  (User-Agent required)
# ─────────────────────────────────────────────────────────────
class ExtendedFeed(BaseFeed):
    EXCHANGE_NAME = "Extended"
    # 官方：Maker 0%、Taker 0.025%（maker 另有成交量占比返利，见文档）
    # https://docs.extended.exchange/extended-resources/trading/trading-fees-and-rebates
    MAKER_FEE_BPS = 0.0
    TAKER_FEE_BPS = 2.5

    API_BASE = "https://api.starknet.extended.exchange/api/v1"
    USER_AGENT = "perp-dex-scanner/1.0"
    SYMBOL_MAP = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD"}

    def __init__(self, symbol: str):
        super().__init__(symbol)
        self._market = self.SYMBOL_MAP.get(symbol.upper(), f"{symbol.upper()}-USD")
        # Extended 资金费：每分钟计算、每小时结算；与 Lighter 对比时按 1h 处理
        self._funding_interval_h = 1.0

    async def fetch_funding_rate(self) -> Optional[float]:
        """当前 fundingRate（字符串小数），按 1h 归一化由 FundingMonitor 处理。"""
        url = f"{self.API_BASE}/info/markets"
        params = {"market": self._market}
        try:
            headers = {"User-Agent": self.USER_AGENT}
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(
                    url, params=params,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    if resp.status != 200:
                        return None
                    payload = await resp.json(content_type=None)
            if (payload.get("status") or "").upper() != "OK":
                return None
            for row in payload.get("data") or []:
                if row.get("name") != self._market:
                    continue
                stats = row.get("marketStats") or {}
                fr = stats.get("fundingRate")
                if fr is None:
                    return None
                return float(fr)
        except Exception as exc:
            logger.debug("[Extended] funding fetch: %s", exc)
            return None

    async def connect(self) -> None:
        self._running = True
        poll_interval = 1.0
        url = f"{self.API_BASE}/info/markets/{self._market}/orderbook"
        headers = {"User-Agent": self.USER_AGENT}

        while self._running:
            try:
                async with aiohttp.ClientSession(headers=headers) as session:
                    logger.info("[Extended] REST polling: %s", self._market)
                    while self._running:
                        try:
                            async with session.get(
                                url,
                                timeout=aiohttp.ClientTimeout(total=8),
                            ) as resp:
                                if resp.status != 200:
                                    await asyncio.sleep(poll_interval)
                                    continue
                                data = await resp.json(content_type=None)
                                if (data.get("status") or "").upper() != "OK":
                                    await asyncio.sleep(poll_interval)
                                    continue
                                ob = data.get("data") or {}
                                raw_bids = ob.get("bid") or []
                                raw_asks = ob.get("ask") or []
                                bids = [
                                    (float(x["price"]), float(x["qty"]))
                                    for x in raw_bids
                                    if x.get("price") is not None and x.get("qty") is not None
                                ]
                                asks = [
                                    (float(x["price"]), float(x["qty"]))
                                    for x in raw_asks
                                    if x.get("price") is not None and x.get("qty") is not None
                                ]
                                await self._update_book(bids, asks)
                        except Exception as exc:
                            logger.warning("[Extended] poll error: %s", exc)
                        await asyncio.sleep(poll_interval)
            except Exception as exc:
                logger.warning("[Extended] session error: %s", exc)
                await asyncio.sleep(3)
