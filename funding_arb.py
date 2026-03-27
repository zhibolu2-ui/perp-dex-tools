#!/usr/bin/env python3
"""
跨交易所资金费率套利机器人 (Funding Rate Arbitrage Bot)

策略：
  监控多个交易所的永续合约资金费率差异，在费率差显著时：
  - 在费率高的交易所做空（收取资金费）
  - 在费率低的交易所做多（对冲价格风险）
  - 持仓收取费率差收益，费率差缩小时平仓

支持交易所：
  Lighter (0% taker), Extended (2.5bps taker),
  Binance (4bps taker)

用法:
  # 仅监控（Phase 1）
  python funding_arb.py --symbol BTC --monitor-only

  # Lighter+Extended 对冲交易
  python funding_arb.py --symbol BTC --size 0.005 --trade-pair Lighter,Extended

  # Lighter+Binance 对冲交易
  python funding_arb.py --symbol BTC --size 0.005 --trade-pair Lighter,Binance

  # DRY-RUN 模拟
  python funding_arb.py --symbol BTC --size 0.005 --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import signal
import sys
import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp

try:
    import uvloop
except ImportError:
    uvloop = None

from dotenv import load_dotenv

# ═══════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════

LIGHTER_REST = "https://mainnet.zklighter.elliot.ai"
EXTENDED_API = "https://api.starknet.extended.exchange/api/v1"
EDGEX_API = "https://pro.edgex.exchange"
VARIATIONAL_API = "https://omni-client-api.prod.ap-northeast-1.variational.io"
GRVT_API = "https://market-data.grvt.io"
ZO_API = "https://zo-mainnet.n1.xyz"

TAKER_FEES_BPS: Dict[str, float] = {
    "Lighter": 0.0,
    "Extended": 2.5,
    "Binance": 4.0,

    "Hyperliquid": 3.5,
    "OKX": 5.0,
    "EdgeX": 3.0,
    "Variational": 0.0,
    "GRVT": 3.0,
    "01": 3.0,
}

MAKER_FEES_BPS: Dict[str, float] = {
    "Lighter": 0.0,
    "Extended": 0.0,
    "Binance": 2.0,

    "Hyperliquid": 1.0,
    "OKX": 2.0,
    "EdgeX": 1.0,
    "Variational": 0.0,
    "GRVT": 1.0,
    "01": 1.0,
}

FUNDING_INTERVAL_H: Dict[str, float] = {
    "Lighter": 1.0,      # hourly settlement, API returns per-hour rate
    "Extended": 1.0,      # own REST API → per-hour
    "Binance": 8.0,       # aggregated API → 8h premium

    "Hyperliquid": 8.0,   # aggregated API → 8h premium
    "OKX": 8.0,           # aggregated API → 8h premium
    "EdgeX": 4.0,         # fundingRateIntervalMin=240
    "Variational": 8.0,   # funding_interval_s=28800
    "GRVT": 8.0,          # funding_interval_hours=8
    "01": 1.0,            # hourly settlement
}

EDGEX_CONTRACT_IDS: Dict[str, str] = {
    "BTC": "10000001", "ETH": "10000002", "SOL": "10000004",
}
ZO_MARKET_IDS: Dict[str, int] = {
    "BTC": 0, "ETH": 1, "SOL": 2,
}


# ═══════════════════════════════════════════════════
#  Data Classes
# ═══════════════════════════════════════════════════

class State(str, Enum):
    IDLE = "IDLE"
    IN_POSITION = "IN_POSITION"


@dataclass
class FundingRateInfo:
    exchange: str
    rate_1h: float
    raw_rate: float
    interval_h: float
    annualized_pct: float
    timestamp: float = 0.0


@dataclass
class PairSpread:
    exchange_a: str
    exchange_b: str
    spread_1h: float
    annualized_pct: float
    direction: str
    round_trip_fee_bps: float
    break_even_hours: float
    short_exchange: str
    long_exchange: str
    mixed_frequency: bool = False
    maker_rt_fee_bps: float = 0.0
    maker_be_hours: float = 0.0


@dataclass
class PriceInfo:
    exchange: str
    bid: float
    ask: float
    mid: float
    timestamp: float = 0.0


@dataclass
class PriceSpread:
    exchange_a: str
    exchange_b: str
    spread_bps: float
    direction: str
    funding_aligned: bool = False
    combined_score: float = 0.0


# ═══════════════════════════════════════════════════
#  Exchange Adapters
# ═══════════════════════════════════════════════════

class ExchangeAdapter(ABC):
    name: str = ""
    taker_fee_bps: float = 0.0
    last_fill_price: Optional[Decimal] = None

    @abstractmethod
    async def initialize(self, symbol: str) -> None: ...

    @abstractmethod
    async def open_position(
        self, side: str, size: Decimal, ref_price: Decimal,
    ) -> Optional[Decimal]: ...

    @abstractmethod
    async def get_position(self) -> Decimal: ...

    @abstractmethod
    async def get_mid_price(self) -> Optional[Decimal]: ...

    @abstractmethod
    async def shutdown(self) -> None: ...


class LighterAdapter(ExchangeAdapter):
    name = "Lighter"
    taker_fee_bps = 0.0

    def __init__(self):
        self.client = None
        self.symbol: str = ""
        self.market_index: int = 0
        self.base_amount_multiplier: int = 0
        self.price_multiplier: int = 0
        self.tick: Decimal = Decimal("0.01")
        self.account_index: int = 0
        self.api_key_index: int = 0
        self._nonce_counter: int = 0
        self._fill_event = asyncio.Event()
        self._fill_data: Optional[dict] = None
        self._pending_coi: Optional[int] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._stop = False
        self._session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger("funding_arb.lighter")

    async def initialize(self, symbol: str) -> None:
        import requests
        from lighter.signer_client import SignerClient

        self.symbol = symbol.upper()
        self.account_index = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
        self.api_key_index = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))

        api_key = os.getenv("API_KEY_PRIVATE_KEY")
        if not api_key:
            raise ValueError("API_KEY_PRIVATE_KEY not set")

        self.client = SignerClient(
            url=LIGHTER_REST,
            account_index=self.account_index,
            api_private_keys={self.api_key_index: api_key},
        )
        err = self.client.check_client()
        if err is not None:
            raise RuntimeError(f"Lighter CheckClient: {err}")

        url = f"{LIGHTER_REST}/api/v1/orderBooks"
        resp = requests.get(url, headers={"accept": "application/json"}, timeout=10)
        resp.raise_for_status()
        for m in resp.json().get("order_books", []):
            if m["symbol"] == self.symbol:
                self.market_index = m["market_id"]
                self.base_amount_multiplier = pow(10, m["supported_size_decimals"])
                self.price_multiplier = pow(10, m["supported_price_decimals"])
                self.tick = Decimal("1") / Decimal(10) ** m["supported_price_decimals"]
                break
        else:
            raise RuntimeError(f"{self.symbol} not found on Lighter")

        self._ws_task = asyncio.create_task(self._account_ws_loop())
        self.logger.info("Lighter adapter ready")

    def _round_price(self, price: Decimal) -> Decimal:
        if self.tick > 0:
            return (price / self.tick).quantize(Decimal("1")) * self.tick
        return price

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5),
                headers={"accept": "application/json"},
            )
        return self._session

    async def get_mid_price(self) -> Optional[Decimal]:
        try:
            session = await self._ensure_session()
            url = f"{LIGHTER_REST}/api/v1/orderBook/{self.symbol}/0"
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                if bids and asks:
                    b = Decimal(str(bids[0]["price"]))
                    a = Decimal(str(asks[0]["price"]))
                    return (b + a) / 2
        except Exception as e:
            self.logger.debug(f"mid price error: {e}")
        return None

    async def open_position(
        self, side: str, size: Decimal, ref_price: Decimal,
    ) -> Optional[Decimal]:
        if not self.client:
            return None

        is_ask = side.lower() == "sell"
        slip = Decimal("20") / Decimal("10000")
        price = self._round_price(
            ref_price * (Decimal("1") - slip if is_ask else Decimal("1") + slip))

        pre_pos = await self.get_position()

        self._nonce_counter += 1
        coi = int(time.time() * 1000) * 100 + (self._nonce_counter % 100)
        self._fill_event.clear()
        self._fill_data = None
        self._pending_coi = coi

        try:
            _, _, error = await self.client.create_order(
                market_index=self.market_index,
                client_order_index=coi,
                base_amount=int(size * self.base_amount_multiplier),
                price=int(price * self.price_multiplier),
                is_ask=is_ask,
                order_type=self.client.ORDER_TYPE_LIMIT,
                time_in_force=self.client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                order_expiry=self.client.DEFAULT_IOC_EXPIRY,
                reduce_only=False, trigger_price=0,
            )
            if error:
                self.logger.error(f"[Lighter] taker error: {error}")
                return None

            try:
                await asyncio.wait_for(self._fill_event.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass

            fd = self._fill_data
            if fd is not None:
                filled_raw = fd.get("filled_base_amount")
                if filled_raw is not None:
                    ws_filled = Decimal(str(filled_raw))
                    if ws_filled >= size * Decimal("0.5"):
                        avg_raw = fd.get("average_fill_price")
                        if avg_raw is not None:
                            self.last_fill_price = Decimal(str(avg_raw))
                        else:
                            self.last_fill_price = ref_price
                        self.logger.info(f"[Lighter] WS fill={ws_filled} avg={self.last_fill_price}")
                        return ws_filled

            for delay in [0.1, 0.2, 0.3]:
                await asyncio.sleep(delay)
                try:
                    new_pos = await self.get_position()
                    delta = abs(new_pos - pre_pos)
                    if delta >= size * Decimal("0.5"):
                        self.last_fill_price = ref_price
                        self.logger.info(f"[Lighter] REST fill={delta}")
                        return delta
                except Exception:
                    pass

            self.logger.warning("[Lighter] fill not confirmed")
            return None
        except Exception as e:
            self.logger.error(f"[Lighter] exception: {e}")
            return None

    async def get_position(self) -> Decimal:
        session = await self._ensure_session()
        url = f"{LIGHTER_REST}/api/v1/account"
        params = {"by": "index", "value": self.account_index}
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
            for pos in data.get("accounts", [{}])[0].get("positions", []):
                if pos.get("symbol") == self.symbol:
                    return Decimal(str(pos["position"])) * int(pos["sign"])
        return Decimal("0")

    async def _account_ws_loop(self):
        import websockets as ws_lib

        ws_url = "wss://mainnet.zklighter.elliot.ai/stream"
        delay = 1
        while not self._stop:
            try:
                async with ws_lib.connect(ws_url) as ws:
                    auth, err = self.client.create_auth_token_with_expiry(
                        api_key_index=self.api_key_index)
                    if err:
                        await asyncio.sleep(5)
                        continue
                    ch = f"account_orders/{self.market_index}/{self.account_index}"
                    await ws.send(json.dumps({
                        "type": "subscribe", "channel": ch, "auth": auth}))
                    self.logger.info("[Lighter WS] connected")
                    delay = 1
                    token_ts = time.time()

                    while not self._stop:
                        if time.time() - token_ts > 500:
                            auth, err = self.client.create_auth_token_with_expiry(
                                api_key_index=self.api_key_index)
                            if err is None:
                                await ws.send(json.dumps({
                                    "type": "subscribe", "channel": ch, "auth": auth}))
                                token_ts = time.time()
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            continue
                        data = json.loads(raw)
                        if data.get("type") == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                        elif data.get("type") == "update/account_orders":
                            for od in data.get("orders", {}).get(
                                    str(self.market_index), []):
                                st = od.get("status", "").upper()
                                if st not in ("FILLED", "PARTIALLY_FILLED", "CANCELED"):
                                    continue
                                if self._pending_coi is not None:
                                    coi = od.get("client_order_index")
                                    if coi is not None and int(coi) != self._pending_coi:
                                        continue
                                self._fill_data = od
                                self._fill_event.set()
            except Exception as exc:
                self.logger.warning(f"[Lighter WS] {exc}")
            if not self._stop:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def shutdown(self) -> None:
        self._stop = True
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._session and not self._session.closed:
            await self._session.close()


class ExtendedAdapter(ExchangeAdapter):
    name = "Extended"
    taker_fee_bps = 2.5

    def __init__(self):
        self.client = None
        self.symbol: str = ""
        self.contract_id: str = ""
        self.tick_size: Decimal = Decimal("0.01")
        self.min_order_size: Decimal = Decimal("0.001")
        self._ioc_pending_id: Optional[str] = None
        self._ioc_confirmed = asyncio.Event()
        self._ioc_fill_qty: Decimal = Decimal("0")
        self._ioc_fill_price: Optional[Decimal] = None
        self.logger = logging.getLogger("funding_arb.extended")

    async def initialize(self, symbol: str) -> None:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from exchanges.extended import ExtendedClient

        self.symbol = symbol.upper()

        class _Cfg:
            def __init__(self, d):
                for k, v in d.items():
                    setattr(self, k, v)

        cfg = _Cfg({
            "ticker": self.symbol, "contract_id": "",
            "quantity": Decimal("0.001"), "tick_size": Decimal("0.01"),
            "close_order_side": "sell",
        })
        self.client = ExtendedClient(cfg)
        self.client.setup_order_update_handler(self._on_order_event)
        await self.client.connect()

        cid, tick = await self.client.get_contract_attributes()
        self.contract_id = cid
        self.tick_size = tick
        if hasattr(self.client, "min_order_size"):
            self.min_order_size = self.client.min_order_size

        self.logger.info(f"Extended adapter ready: {cid}")

    def _on_order_event(self, event: dict):
        oid = str(event.get("order_id", ""))
        status = event.get("status", "")
        filled = event.get("filled_size", "0")
        if self._ioc_pending_id and oid == self._ioc_pending_id:
            if status in ("FILLED", "CANCELED", "EXPIRED"):
                self._ioc_fill_qty = Decimal(str(filled))
                avg = event.get("avg_price")
                if avg:
                    try:
                        self._ioc_fill_price = Decimal(str(avg))
                    except Exception:
                        pass
                self._ioc_confirmed.set()

    async def get_mid_price(self) -> Optional[Decimal]:
        if self.client and self.client.orderbook:
            ob = self.client.orderbook
            bids = ob.get("bid", [])
            asks = ob.get("ask", [])
            if bids and asks:
                try:
                    b = Decimal(bids[0]["p"])
                    a = Decimal(asks[0]["p"])
                    return (b + a) / 2
                except (KeyError, IndexError):
                    pass
        return None

    async def open_position(
        self, side: str, size: Decimal, ref_price: Decimal,
    ) -> Optional[Decimal]:
        if not self.client:
            return None

        slip = Decimal("50") / Decimal("10000")
        if side == "buy":
            aggressive = ref_price * (Decimal("1") + slip)
        else:
            aggressive = ref_price * (Decimal("1") - slip)
        aggressive = aggressive.quantize(self.tick_size, rounding=ROUND_HALF_UP)
        qty = size.quantize(self.min_order_size, rounding=ROUND_HALF_UP)

        self._ioc_pending_id = None
        self._ioc_confirmed.clear()
        self._ioc_fill_qty = Decimal("0")
        self._ioc_fill_price = None

        try:
            result = await self.client.place_ioc_order(
                contract_id=self.contract_id,
                quantity=qty, side=side, aggressive_price=aggressive)
            if result.success:
                self._ioc_pending_id = str(result.order_id)
                try:
                    await asyncio.wait_for(self._ioc_confirmed.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
                if self._ioc_fill_qty > 0:
                    self.last_fill_price = self._ioc_fill_price or ref_price
                    self.logger.info(f"[Extended] fill={self._ioc_fill_qty} avg={self.last_fill_price}")
                    return self._ioc_fill_qty
                self.logger.warning("[Extended] no fill confirmed")
                return None
            self.logger.error(f"[Extended] IOC fail: {result.error_message}")
            return None
        except Exception as e:
            self.logger.error(f"[Extended] exception: {e}")
            return None

    async def get_position(self) -> Decimal:
        data = await self.client.perpetual_trading_client.account.get_positions(
            market_names=[f"{self.symbol}-USD"])
        if not data or not hasattr(data, "data") or not data.data:
            return Decimal("0")
        for p in data.data:
            if p.market == self.contract_id:
                mult = Decimal("1") if p.side.lower() == "long" else Decimal("-1")
                return Decimal(str(p.size)) * mult
        return Decimal("0")

    async def shutdown(self) -> None:
        if self.client:
            try:
                self.client._stop_event.set()
                for t in self.client._tasks:
                    t.cancel()
                await asyncio.gather(*self.client._tasks, return_exceptions=True)
            except Exception:
                pass


class CexAdapter(ExchangeAdapter):
    """CEX adapter for Binance via ccxt async."""

    _CONFIGS = {
        "binance": {
            "class": "binanceusdm", "name": "Binance", "fee": 4.0,
            "symbol_fmt": "{coin}/USDT:USDT",
            "key_env": "BINANCE_API_KEY", "secret_env": "BINANCE_API_SECRET",
        },
    }

    def __init__(self, exchange_id: str):
        eid = exchange_id.lower()
        cfg = self._CONFIGS.get(eid)
        if not cfg:
            raise ValueError(f"Unsupported CEX: {exchange_id}")
        self.name = cfg["name"]
        self.taker_fee_bps = cfg["fee"]
        self._ccxt_class = cfg["class"]
        self._symbol_fmt = cfg["symbol_fmt"]
        self._key_env = cfg["key_env"]
        self._secret_env = cfg["secret_env"]
        self._extra_opts = cfg.get("options", {})
        self._exchange = None
        self._ccxt_symbol: str = ""
        self.logger = logging.getLogger(f"funding_arb.{eid}")

    async def initialize(self, symbol: str) -> None:
        import ccxt.pro as ccxtpro

        self._ccxt_symbol = self._symbol_fmt.format(coin=symbol.upper())
        api_key = os.getenv(self._key_env)
        api_secret = os.getenv(self._secret_env)
        if not api_key or not api_secret:
            raise ValueError(f"{self._key_env} / {self._secret_env} not set")

        ExCls = getattr(ccxtpro, self._ccxt_class)
        self._exchange = ExCls({
            "apiKey": api_key, "secret": api_secret,
            "enableRateLimit": True, "options": self._extra_opts,
        })
        await self._exchange.load_markets()
        self.logger.info(f"[{self.name}] ready: {self._ccxt_symbol}")

    async def get_mid_price(self) -> Optional[Decimal]:
        if not self._exchange:
            return None
        try:
            ticker = await self._exchange.fetch_ticker(self._ccxt_symbol)
            bid = ticker.get("bid")
            ask = ticker.get("ask")
            if bid and ask:
                return (Decimal(str(bid)) + Decimal(str(ask))) / 2
        except Exception as e:
            self.logger.debug(f"ticker error: {e}")
        return None

    async def open_position(
        self, side: str, size: Decimal, ref_price: Decimal,
    ) -> Optional[Decimal]:
        if not self._exchange:
            return None
        try:
            order = await self._exchange.create_order(
                self._ccxt_symbol, "market", side, float(size))
            filled = order.get("filled", 0)
            if filled and float(filled) > 0:
                avg = order.get("average") or order.get("price")
                self.last_fill_price = Decimal(str(avg)) if avg else ref_price
                self.logger.info(f"[{self.name}] {side} filled={filled} avg={self.last_fill_price}")
                return Decimal(str(filled))
            self.logger.warning(f"[{self.name}] no fill")
            return None
        except Exception as e:
            self.logger.error(f"[{self.name}] order error: {e}")
            return None

    async def get_position(self) -> Decimal:
        if not self._exchange:
            return Decimal("0")
        try:
            positions = await self._exchange.fetch_positions([self._ccxt_symbol])
            for pos in positions:
                contracts = pos.get("contracts", 0)
                if contracts and float(contracts) != 0:
                    side = pos.get("side", "")
                    qty = Decimal(str(abs(float(contracts))))
                    return qty if side == "long" else -qty
            return Decimal("0")
        except Exception as e:
            self.logger.error(f"[{self.name}] pos error: {e}")
            raise

    async def shutdown(self) -> None:
        if self._exchange:
            try:
                await self._exchange.close()
            except Exception:
                pass


class HyperliquidAdapter(ExchangeAdapter):
    """Adapter for Hyperliquid using the official Python SDK."""

    name = "Hyperliquid"
    taker_fee_bps = 3.5

    def __init__(self):
        self._info = None
        self._exchange = None
        self._address: str = ""
        self._coin: str = ""
        self.logger = logging.getLogger("funding_arb.hyperliquid")

    async def initialize(self, symbol: str) -> None:
        from hyperliquid.info import Info
        from hyperliquid.exchange import Exchange
        from hyperliquid.utils import constants
        import eth_account

        self._coin = symbol.upper()
        secret = os.getenv("HL_SECRET_KEY", "")
        self._address = os.getenv("HL_ACCOUNT_ADDRESS", "")
        if not secret or not self._address:
            raise ValueError("HL_SECRET_KEY / HL_ACCOUNT_ADDRESS not set in .env")

        account = eth_account.Account.from_key(secret)
        self._info = Info(constants.MAINNET_API_URL, skip_ws=True)
        self._exchange = Exchange(account, constants.MAINNET_API_URL)
        self.logger.info(f"[Hyperliquid] ready: {self._coin} addr={self._address[:10]}...")

    async def get_mid_price(self) -> Optional[Decimal]:
        if not self._info:
            return None
        try:
            mids = self._info.all_mids()
            mid = mids.get(self._coin)
            if mid is not None:
                return Decimal(str(mid))
        except Exception as e:
            self.logger.debug(f"mid price error: {e}")
        return None

    async def open_position(
        self, side: str, size: Decimal, ref_price: Decimal,
    ) -> Optional[Decimal]:
        if not self._exchange:
            return None
        try:
            is_buy = side.lower() == "buy"
            sz = float(size)
            result = self._exchange.market_open(
                self._coin, is_buy, sz, None, 0.01)

            if result.get("status") != "ok":
                self.logger.error(f"[Hyperliquid] order rejected: {result}")
                return None

            statuses = result.get("response", {}).get("data", {}).get("statuses", [])
            total_filled = Decimal("0")
            weighted_price = Decimal("0")
            for st in statuses:
                filled = st.get("filled")
                if filled:
                    qty = Decimal(str(filled["totalSz"]))
                    avg = Decimal(str(filled["avgPx"]))
                    weighted_price += qty * avg
                    total_filled += qty

            if total_filled > 0:
                self.last_fill_price = weighted_price / total_filled
                self.logger.info(
                    f"[Hyperliquid] {side} filled={total_filled} avg={self.last_fill_price}")
                return total_filled

            self.logger.warning("[Hyperliquid] no fill")
            return None
        except Exception as e:
            self.logger.error(f"[Hyperliquid] order error: {e}")
            return None

    async def get_position(self) -> Decimal:
        if not self._info:
            return Decimal("0")
        try:
            state = self._info.user_state(self._address)
            for pos_data in state.get("assetPositions", []):
                pos = pos_data.get("position", {})
                if pos.get("coin", "").upper() == self._coin:
                    szi = pos.get("szi", "0")
                    return Decimal(str(szi))
            return Decimal("0")
        except Exception as e:
            self.logger.error(f"[Hyperliquid] position error: {e}")
            return Decimal("0")

    async def shutdown(self) -> None:
        pass


# ═══════════════════════════════════════════════════
#  Funding Rate Arbitrage Bot
# ═══════════════════════════════════════════════════

class FundingArbBot:

    def __init__(
        self,
        symbol: str,
        order_size: Decimal,
        poll_interval: float,
        entry_spread_ann: float,
        exit_spread_ann: float,
        max_hold_hours: float,
        monitor_only: bool,
        dry_run: bool,
        trade_exchanges: Tuple[str, str] = ("Lighter", "Extended"),
    ):
        self.symbol = symbol.upper()
        self.order_size = order_size
        self.poll_interval = poll_interval
        self.entry_spread_ann = entry_spread_ann
        self.exit_spread_ann = exit_spread_ann
        self.max_hold_hours = max_hold_hours
        self.monitor_only = monitor_only
        self.dry_run = dry_run
        self.trade_exchanges = trade_exchanges

        self.state = State.IDLE
        self.stop_flag = False

        self._rates: Dict[str, FundingRateInfo] = {}
        self._pair_spreads: List[PairSpread] = []
        self._prices: Dict[str, PriceInfo] = {}
        self._price_spreads: List[PriceSpread] = []

        self._pos_a: Decimal = Decimal("0")
        self._pos_b: Decimal = Decimal("0")
        self._pos_dir: Optional[str] = None
        self._pos_opened_at: Optional[float] = None
        self._pos_ex_short: str = ""
        self._pos_ex_long: str = ""

        self._entry_price_short: Optional[Decimal] = None
        self._entry_price_long: Optional[Decimal] = None
        self._entry_notional: float = 0.0
        self._entry_fee_usd: float = 0.0
        self._cumulative_funding_usd: float = 0.0
        self._last_funding_ts: float = 0.0
        self._spread_history: List[Tuple[float, float]] = []
        self._spread_trend_window: int = 5

        self._stable_count: int = 0
        self._last_stable_key: Optional[str] = None
        self._min_stable: int = 3

        self._adapters: Dict[str, ExchangeAdapter] = {}
        self._session: Optional[aiohttp.ClientSession] = None

        os.makedirs("logs", exist_ok=True)
        self.csv_path = f"logs/funding_arb_{self.symbol}_rates.csv"
        self.trade_csv_path = f"logs/funding_arb_{self.symbol}_trades.csv"
        self.log_path = f"logs/funding_arb_{self.symbol}.log"
        self._init_csv()
        self.logger = self._init_logger()

    # ─── Logging / CSV ───

    def _init_logger(self) -> logging.Logger:
        lg = logging.getLogger(f"funding_arb_{self.symbol}")
        lg.setLevel(logging.INFO)
        lg.handlers.clear()
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        fh = logging.FileHandler(self.log_path)
        fh.setFormatter(fmt)
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        lg.addHandler(fh)
        lg.addHandler(ch)
        lg.propagate = False
        for n in ("urllib3", "requests", "websockets", "lighter", "aiohttp", "ccxt"):
            logging.getLogger(n).setLevel(logging.WARNING)
        return lg

    def _init_csv(self):
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp",
                    "lighter_rate_1h", "extended_rate_1h",
                    "binance_rate_1h",
                    "best_pair", "best_spread_1h", "best_ann_pct",
                    "best_direction",
                ])
        if not os.path.exists(self.trade_csv_path):
            with open(self.trade_csv_path, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp", "action", "short_exchange", "long_exchange",
                    "size", "spread_ann_pct", "hold_hours",
                ])

    # ─── HTTP session ───

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15))
        return self._session

    # ─── Rate Polling ───

    async def _poll_all_rates(self) -> Dict[str, FundingRateInfo]:
        rates: Dict[str, FundingRateInfo] = {}
        lighter_task = self._poll_lighter_aggregated()
        ext_task = self._poll_extended_direct()
        edgex_task = self._poll_edgex()
        var_task = self._poll_variational()
        grvt_task = self._poll_grvt()
        zo_task = self._poll_01()

        results = await asyncio.gather(
            lighter_task, ext_task, edgex_task, var_task, grvt_task, zo_task,
            return_exceptions=True)

        if isinstance(results[0], dict):
            rates.update(results[0])
        if isinstance(results[1], FundingRateInfo):
            rates["Extended"] = results[1]
        for i, name in [(2, "EdgeX"), (3, "Variational"), (4, "GRVT"), (5, "01")]:
            r = results[i]
            if isinstance(r, tuple) and len(r) == 2:
                fri, pi = r
                if fri:
                    rates[name] = fri
                if pi:
                    self._prices[name] = pi
        self._rates = rates
        return rates

    async def _poll_lighter_aggregated(self) -> Dict[str, FundingRateInfo]:
        rates: Dict[str, FundingRateInfo] = {}
        try:
            session = await self._get_session()
            url = f"{LIGHTER_REST}/api/v1/funding-rates"
            async with session.get(url) as resp:
                if resp.status != 200:
                    return rates
                data = await resp.json()

            name_map = {
                "lighter": "Lighter", "binance": "Binance",
                "hyperliquid": "Hyperliquid", "okx": "OKX",
            }
            for item in data.get("funding_rates", []):
                if item.get("symbol", "").upper() != self.symbol:
                    continue
                rate_val = item.get("rate")
                if rate_val is None:
                    continue
                rate_val = float(rate_val)
                src = item.get("exchange", "").lower()
                dname = name_map.get(src)
                if not dname:
                    continue
                interval_h = FUNDING_INTERVAL_H.get(dname, 8.0)
                # Lighter aggregated API returns ALL rates in 8h-premium format
                # (verified against UI: API 0.0096% / 8 = 0.0012% ≡ UI "0.0012%/h")
                rate_1h = rate_val / interval_h
                rates[dname] = FundingRateInfo(
                    exchange=dname, rate_1h=rate_1h,
                    raw_rate=rate_val, interval_h=interval_h,
                    annualized_pct=rate_1h * 24 * 365 * 100,
                    timestamp=time.time(),
                )
        except Exception as e:
            self.logger.debug(f"lighter aggregated poll: {e}")
        return rates

    async def _poll_extended_direct(self) -> Optional[FundingRateInfo]:
        try:
            session = await self._get_session()
            market = f"{self.symbol}-USD"
            url = f"{EXTENDED_API}/info/markets"
            params = {"market": market}
            headers = {"User-Agent": "funding-arb/1.0"}
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status != 200:
                    return None
                payload = await resp.json(content_type=None)
            if (payload.get("status") or "").upper() != "OK":
                return None
            for row in payload.get("data") or []:
                if row.get("name") != market:
                    continue
                stats = row.get("marketStats") or {}
                fr = stats.get("fundingRate")
                if fr is None:
                    return None
                rate = float(fr)
                return FundingRateInfo(
                    exchange="Extended", rate_1h=rate, raw_rate=rate,
                    interval_h=1.0, annualized_pct=rate * 24 * 365 * 100,
                    timestamp=time.time(),
                )
        except Exception as e:
            self.logger.debug(f"extended direct poll: {e}")
        return None

    async def _poll_edgex(self) -> Tuple[Optional[FundingRateInfo], Optional[PriceInfo]]:
        cid = EDGEX_CONTRACT_IDS.get(self.symbol)
        if not cid:
            return None, None
        try:
            session = await self._get_session()
            url = f"{EDGEX_API}/api/v1/public/funding/getLatestFundingRate"
            async with session.get(url, params={"contractId": cid}) as resp:
                if resp.status != 200:
                    return None, None
                data = await resp.json(content_type=None)
            items = data.get("data", [])
            if not items:
                return None, None
            it = items[0]
            raw_rate = float(it.get("fundingRate", 0))
            interval_h = float(it.get("fundingRateIntervalMin", 240)) / 60
            rate_1h = raw_rate / interval_h
            fri = FundingRateInfo(
                exchange="EdgeX", rate_1h=rate_1h,
                raw_rate=raw_rate, interval_h=interval_h,
                annualized_pct=rate_1h * 24 * 365 * 100,
                timestamp=time.time())
            bid = float(it.get("impactBidPrice", 0) or 0)
            ask = float(it.get("impactAskPrice", 0) or 0)
            mark = float(it.get("markPrice", 0) or 0)
            if bid <= 0 or ask <= 0:
                bid = ask = mark if mark > 0 else 0
            pi = PriceInfo("EdgeX", bid, ask, (bid + ask) / 2, time.time()) if bid > 0 else None
            return fri, pi
        except Exception as e:
            self.logger.debug(f"edgex poll: {e}")
            return None, None

    async def _poll_variational(self) -> Tuple[Optional[FundingRateInfo], Optional[PriceInfo]]:
        try:
            session = await self._get_session()
            url = f"{VARIATIONAL_API}/metadata/stats"
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None, None
                data = await resp.json(content_type=None)
            for item in data.get("listings", []):
                if item.get("ticker", "").upper() != self.symbol:
                    continue
                api_val = float(item.get("funding_rate", 0))
                raw_rate = api_val / 100  # API returns %, convert to decimal
                interval_s = int(item.get("funding_interval_s", 28800))
                interval_h = interval_s / 3600
                rate_1h = raw_rate / interval_h
                fri = FundingRateInfo(
                    exchange="Variational", rate_1h=rate_1h,
                    raw_rate=raw_rate, interval_h=interval_h,
                    annualized_pct=rate_1h * 24 * 365 * 100,
                    timestamp=time.time())
                quotes = item.get("quotes", {}).get("base", {})
                bid = float(quotes.get("bid", 0) or 0)
                ask = float(quotes.get("ask", 0) or 0)
                if bid <= 0 or ask <= 0:
                    mark = float(item.get("mark_price", 0) or 0)
                    if mark > 0:
                        bid = ask = mark
                pi = PriceInfo("Variational", bid, ask, (bid + ask) / 2,
                               time.time()) if bid > 0 else None
                return fri, pi
        except Exception as e:
            self.logger.debug(f"variational poll: {e}")
        return None, None

    async def _poll_grvt(self) -> Tuple[Optional[FundingRateInfo], Optional[PriceInfo]]:
        fri, pi = None, None
        try:
            session = await self._get_session()
            inst = f"{self.symbol}_USDT_Perp"
            url_f = f"{GRVT_API}/full/v1/funding"
            async with session.post(url_f, json={"instrument": inst}) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    items = data.get("result", [])
                    if items:
                        it = items[0]
                        api_val = float(it.get("funding_rate", 0))
                        raw_rate = api_val / 100  # API returns %, convert to decimal
                        interval_h = float(it.get("funding_interval_hours", 8))
                        rate_1h = raw_rate / interval_h
                        fri = FundingRateInfo(
                            exchange="GRVT", rate_1h=rate_1h,
                            raw_rate=raw_rate, interval_h=interval_h,
                            annualized_pct=rate_1h * 24 * 365 * 100,
                            timestamp=time.time())
            url_t = f"{GRVT_API}/full/v1/mini"
            async with session.post(url_t, json={"instrument": inst}) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    r = data.get("result", {})
                    bid = float(r.get("best_bid_price", 0) or 0)
                    ask = float(r.get("best_ask_price", 0) or 0)
                    if bid > 0 and ask > 0:
                        pi = PriceInfo("GRVT", bid, ask, (bid + ask) / 2,
                                       time.time())
        except Exception as e:
            self.logger.debug(f"grvt poll: {e}")
        return fri, pi

    async def _poll_01(self) -> Tuple[Optional[FundingRateInfo], Optional[PriceInfo]]:
        mid = ZO_MARKET_IDS.get(self.symbol)
        if mid is None:
            return None, None
        fri, pi = None, None
        try:
            session = await self._get_session()
            url_s = f"{ZO_API}/market/{mid}/stats"
            async with session.get(url_s) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    ps = data.get("perpStats", {})
                    raw_rate = float(ps.get("funding_rate", 0))
                    rate_1h = raw_rate
                    fri = FundingRateInfo(
                        exchange="01", rate_1h=rate_1h,
                        raw_rate=raw_rate, interval_h=1.0,
                        annualized_pct=rate_1h * 24 * 365 * 100,
                        timestamp=time.time())
                    mark = float(ps.get("mark_price", 0) or 0)
                    if mark > 0:
                        pi = PriceInfo("01", mark, mark, mark, time.time())

            url_ob = f"{ZO_API}/market/{mid}/orderbook"
            async with session.get(url_ob) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    if bids and asks:
                        bid = float(bids[0][0])
                        ask = float(asks[0][0])
                        if bid > 0 and ask > 0:
                            pi = PriceInfo("01", bid, ask, (bid + ask) / 2,
                                           time.time())
        except Exception as e:
            self.logger.debug(f"01 poll: {e}")
        return fri, pi

    # ─── Price Polling ───

    async def _poll_all_prices(self) -> Dict[str, PriceInfo]:
        prices: Dict[str, PriceInfo] = {}
        results = await asyncio.gather(
            self._poll_price_lighter(),
            self._poll_price_extended(),
            self._poll_price_binance(),
            self._poll_price_hyperliquid(),
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, PriceInfo):
                prices[r.exchange] = r
        self._prices = prices
        return prices

    async def _poll_price_lighter(self) -> Optional[PriceInfo]:
        ad = self._adapters.get("Lighter")
        if ad:
            try:
                mid = await ad.get_mid_price()
                if mid and float(mid) > 0:
                    m = float(mid)
                    return PriceInfo("Lighter", m, m, m, time.time())
            except Exception:
                pass
        return None

    async def _poll_price_extended(self) -> Optional[PriceInfo]:
        try:
            session = await self._get_session()
            market = f"{self.symbol}-USD"
            url = f"{EXTENDED_API}/info/markets"
            params = {"market": market}
            headers = {"User-Agent": "funding-arb/1.0"}
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status != 200:
                    return None
                payload = await resp.json(content_type=None)
            for row in (payload.get("data") or []):
                if row.get("name") != market:
                    continue
                stats = row.get("marketStats") or {}
                bid = float(stats.get("bidPrice", 0) or 0)
                ask = float(stats.get("askPrice", 0) or 0)
                mark = float(stats.get("markPrice", 0) or 0)
                if bid <= 0 or ask <= 0:
                    if mark > 0:
                        bid = ask = mark
                    else:
                        return None
                return PriceInfo("Extended", bid, ask, (bid + ask) / 2, time.time())
        except Exception as e:
            self.logger.debug(f"price extended: {e}")
        return None

    async def _poll_price_binance(self) -> Optional[PriceInfo]:
        try:
            session = await self._get_session()
            symbol = f"{self.symbol}USDT"
            url = f"https://fapi.binance.com/fapi/v1/ticker/bookTicker"
            async with session.get(url, params={"symbol": symbol}) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
            bid = float(data.get("bidPrice", 0))
            ask = float(data.get("askPrice", 0))
            if bid <= 0 or ask <= 0:
                return None
            return PriceInfo("Binance", bid, ask, (bid + ask) / 2, time.time())
        except Exception as e:
            self.logger.debug(f"price binance: {e}")
            return None

    async def _poll_price_hyperliquid(self) -> Optional[PriceInfo]:
        try:
            session = await self._get_session()
            url = "https://api.hyperliquid.xyz/info"
            payload = {"type": "allMids"}
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
            mid = float(data.get(self.symbol, 0))
            if mid <= 0:
                return None
            spread_est = mid * 0.00005
            return PriceInfo("Hyperliquid", mid - spread_est, mid + spread_est,
                             mid, time.time())
        except Exception as e:
            self.logger.debug(f"price hl: {e}")
            return None

    # ─── Price Spread Computation ───

    def _compute_price_spreads(self):
        self._price_spreads.clear()
        names = [n for n in ["Lighter", "Extended", "Binance", "Hyperliquid",
                             "EdgeX", "Variational", "GRVT", "01"]
                 if n in self._prices]
        for i, a in enumerate(names):
            for b in names[i + 1:]:
                pa = self._prices[a]
                pb = self._prices[b]
                ref = (pa.mid + pb.mid) / 2
                if ref <= 0:
                    continue
                spread_bps = (pa.mid - pb.mid) / ref * 10000
                if spread_bps > 0:
                    direction = f"short_{a}_long_{b}"
                else:
                    direction = f"short_{b}_long_{a}"

                ra = self._rates.get(a)
                rb = self._rates.get(b)
                funding_aligned = False
                if ra and rb and abs(spread_bps) > 0.5:
                    if spread_bps > 0 and ra.rate_1h > rb.rate_1h:
                        funding_aligned = True
                    elif spread_bps < 0 and rb.rate_1h > ra.rate_1h:
                        funding_aligned = True

                fr_ann = 0.0
                if ra and rb:
                    fr_ann = abs(ra.rate_1h - rb.rate_1h) * 24 * 365 * 100
                combined = abs(spread_bps) + fr_ann * 0.1

                self._price_spreads.append(PriceSpread(
                    exchange_a=a, exchange_b=b,
                    spread_bps=spread_bps,
                    direction=direction,
                    funding_aligned=funding_aligned,
                    combined_score=combined,
                ))
        self._price_spreads.sort(key=lambda x: abs(x.spread_bps), reverse=True)

    # ─── Funding Spread Computation ───

    def _compute_pair_spreads(self) -> List[PairSpread]:
        names = list(self._rates.keys())
        spreads: List[PairSpread] = []
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                a, b = names[i], names[j]
                ra = self._rates[a].rate_1h
                rb = self._rates[b].rate_1h
                raw_spread = ra - rb

                if raw_spread >= 0:
                    short_ex, long_ex = a, b
                    direction = f"short_{a}_long_{b}"
                    spread = raw_spread
                else:
                    short_ex, long_ex = b, a
                    direction = f"short_{b}_long_{a}"
                    spread = -raw_spread

                taker_a = TAKER_FEES_BPS.get(a, 5.0)
                taker_b = TAKER_FEES_BPS.get(b, 5.0)
                rt_fee = 2 * (taker_a + taker_b)
                ann = spread * 24 * 365 * 100
                spread_bps_h = spread * 10000
                be_hours = rt_fee / spread_bps_h if spread_bps_h > 0 else float("inf")

                maker_a = MAKER_FEES_BPS.get(a, 2.0)
                maker_b = MAKER_FEES_BPS.get(b, 2.0)
                maker_rt = min(
                    2 * (maker_a + taker_b),
                    2 * (taker_a + maker_b),
                )
                maker_beh = maker_rt / spread_bps_h if spread_bps_h > 0 else float("inf")

                ia = FUNDING_INTERVAL_H.get(a, 1.0)
                ib = FUNDING_INTERVAL_H.get(b, 1.0)
                mixed = ia != ib

                spreads.append(PairSpread(
                    exchange_a=a, exchange_b=b, spread_1h=spread,
                    annualized_pct=ann, direction=direction,
                    round_trip_fee_bps=rt_fee, break_even_hours=be_hours,
                    short_exchange=short_ex, long_exchange=long_ex,
                    mixed_frequency=mixed,
                    maker_rt_fee_bps=maker_rt, maker_be_hours=maker_beh,
                ))
        spreads.sort(key=lambda s: s.annualized_pct, reverse=True)
        self._pair_spreads = spreads
        return spreads

    # ─── Display ───

    def _display_dashboard(self):
        now_s = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        sep = "═" * 95

        lines = [
            "", sep,
            f"  综合套利监控  {self.symbol}   {now_s}",
            sep,
        ]

        # ── Section 1: Prices ──
        lines += [
            f"  ┌─ 实时价格 ────────────────────────────────────────────────┐",
            f"  │ {'交易所':<14} {'买一(Bid)':>14} {'卖一(Ask)':>14} {'中间价':>14} │",
            f"  │ {'─' * 56} │",
        ]
        for n in ["Lighter", "Extended", "Binance", "Hyperliquid",
                  "EdgeX", "Variational", "GRVT", "01"]:
            p = self._prices.get(n)
            if p is None:
                continue
            lines.append(
                f"  │ {n:<14} {p.bid:>14.2f} {p.ask:>14.2f} {p.mid:>14.2f} │")
        lines.append(f"  └────────────────────────────────────────────────────────┘")

        # ── Section 2: Funding Rates ──
        lines += [
            "",
            f"  ┌─ 资金费率 ──────────────────────────────────────────────────────┐",
            f"  │ {'交易所':<14} {'结算':>4} {'原始费率':>14} {'归一/1h':>14} {'年化':>10} │",
            f"  │ {'─' * 62} │",
        ]
        for n in ["Lighter", "Extended", "Binance", "Hyperliquid", "OKX",
                  "EdgeX", "Variational", "GRVT", "01"]:
            info = self._rates.get(n)
            if info is None:
                continue
            ivl = f"{info.interval_h:.0f}h"
            raw = f"{info.raw_rate * 100:+.6f}%/{ivl}"
            r1 = f"{info.rate_1h * 100:+.6f}%"
            ann = f"{info.annualized_pct:+.2f}%"
            lines.append(f"  │ {n:<14} {ivl:>4} {raw:>14} {r1:>14} {ann:>10} │")
        lines.append(f"  └──────────────────────────────────────────────────────────────┘")

        # ── Section 3: Price Spreads ──
        if self._price_spreads:
            lines += [
                "",
                f"  ┌─ 价差监控 (实时) ─────────────────────────────────────────────────────┐",
                f"  │ {'组合':<22} {'价差bps':>10} {'费率对齐':>8} {'综合分':>8} {'方向':<28} │",
                f"  │ {'─' * 80} │",
            ]
            for ps in self._price_spreads[:6]:
                pair = f"{ps.exchange_a}-{ps.exchange_b}"
                sp = f"{ps.spread_bps:+.2f}"
                aligned = "✓ 同向" if ps.funding_aligned else "✗ 反向"
                score = f"{ps.combined_score:.1f}"
                d = ps.direction[:28]
                lines.append(
                    f"  │ {pair:<22} {sp:>10} {aligned:>8} {score:>8} {d:<28} │")
            lines.append(f"  └──────────────────────────────────────────────────────────────────────────┘")

        # ── Section 4: Funding Rate Spreads (3 tables) ──
        def _fmt_spread_table(title: str, items: List[PairSpread], max_rows: int = 8):
            tbl = [
                "",
                f"  ┌─ {title} ─{'─' * (100 - len(title) - 6)}┐",
                f"  │ {'组合':<22} {'差/1h':>12} {'年化%':>8} "
                f"{'吃单费':>6} {'吃单回本':>8} {'挂单费':>6} {'挂单回本':>8} {'频率':>6} {'方向':<20} │",
                f"  │ {'─' * 102} │",
            ]
            for ps in items[:max_rows]:
                pair = f"{ps.exchange_a}-{ps.exchange_b}"
                sp = f"{ps.spread_1h * 100:+.6f}%"
                ann = f"{ps.annualized_pct:+.1f}%"
                fee_t = f"{ps.round_trip_fee_bps:.0f}"
                beh_t = f"{ps.break_even_hours:.1f}h" if ps.break_even_hours < 99999 else "inf"
                fee_m = f"{ps.maker_rt_fee_bps:.0f}"
                beh_m = f"{ps.maker_be_hours:.1f}h" if ps.maker_be_hours < 99999 else "inf"
                ia = FUNDING_INTERVAL_H.get(ps.exchange_a, 1.0)
                ib = FUNDING_INTERVAL_H.get(ps.exchange_b, 1.0)
                freq = "1h+1h" if ia == ib == 1.0 else (
                    f"{ia:.0f}h+{ib:.0f}h" if ia == ib else "混合!")
                d = ps.direction[:20]
                tbl.append(
                    f"  │ {pair:<22} {sp:>12} {ann:>8} "
                    f"{fee_t:>6} {beh_t:>8} {fee_m:>6} {beh_m:>8} {freq:>6} {d:<20} │")
            if not items:
                tbl.append(f"  │ {'(无数据)':^102} │")
            tbl.append(f"  └{'─' * 106}┘")
            return tbl

        lighter_pairs = [p for p in self._pair_spreads
                         if "Lighter" in (p.exchange_a, p.exchange_b)]
        var_pairs = [p for p in self._pair_spreads
                     if "Variational" in (p.exchange_a, p.exchange_b)]

        lines += _fmt_spread_table("全交易所费率差排名", self._pair_spreads, 8)
        lines += _fmt_spread_table("Lighter 对冲排名", lighter_pairs, 10)
        lines += _fmt_spread_table("Variational 对冲排名", var_pairs, 10)

        # ── Section 5: Position / Status ──
        status = f"  [状态] {self.state.value}"
        if self.state == State.IN_POSITION:
            hold_h = (time.time() - self._pos_opened_at) / 3600 if self._pos_opened_at else 0
            status += (f"  {self._pos_ex_short}(S)+{self._pos_ex_long}(L)  "
                       f"持仓={hold_h:.1f}h")
            funding, _, fees, net = self._get_net_pnl()
            qty = float(min(abs(self._pos_a), abs(self._pos_b)))
            entry_s = f"@{self._entry_price_short}" if self._entry_price_short else ""
            entry_l = f"@{self._entry_price_long}" if self._entry_price_long else ""
            trend = "↓↓" if self._is_spread_declining() else "──"
            pnl_lines = [
                f"  ┌─ 持仓P&L ─────────────────────────────────┐",
                f"  │ 数量: {qty:.5f}  名义值: ${self._entry_notional:.2f}",
                f"  │ 开仓: short{entry_s}  long{entry_l}",
                f"  │ 累计费率收益: ${funding:+.4f}",
                f"  │ 往返手续费:   ${fees:.4f}  (开+平估)",
                f"  │ 净P&L(不含基差): ${net:+.4f}",
                f"  │ 回本进度: {funding / fees * 100:.1f}%  趋势: {trend}" if fees > 0 else
                f"  │ 回本进度: N/A  趋势: {trend}",
                f"  └────────────────────────────────────────────┘",
            ]
            lines += pnl_lines
        else:
            status += "  无持仓"
        lines += ["", status, sep]

        self.logger.info("\n".join(lines))

    # ─── CSV ───

    def _log_rates_csv(self):
        now_s = datetime.now(timezone.utc).isoformat()
        lr = self._rates.get("Lighter")
        er = self._rates.get("Extended")
        br = self._rates.get("Binance")
        best = self._pair_spreads[0] if self._pair_spreads else None
        with open(self.csv_path, "a", newline="") as f:
            csv.writer(f).writerow([
                now_s,
                f"{lr.rate_1h:.8f}" if lr else "",
                f"{er.rate_1h:.8f}" if er else "",
                f"{br.rate_1h:.8f}" if br else "",
                f"{best.exchange_a}-{best.exchange_b}" if best else "",
                f"{best.spread_1h:.8f}" if best else "",
                f"{best.annualized_pct:.4f}" if best else "",
                best.direction if best else "",
            ])

    def _log_trade_csv(self, action: str, short_ex: str, long_ex: str,
                       size: Decimal, spread_ann: float, hold_h: float = 0):
        with open(self.trade_csv_path, "a", newline="") as f:
            csv.writer(f).writerow([
                datetime.now(timezone.utc).isoformat(),
                action, short_ex, long_ex,
                str(size), f"{spread_ann:.4f}", f"{hold_h:.2f}",
            ])

    # ─── Signal Detection ───

    def _find_best_signal(self) -> Optional[PairSpread]:
        tradable = set(self._adapters.keys())
        for ps in self._pair_spreads:
            if ps.short_exchange not in tradable or ps.long_exchange not in tradable:
                continue
            if ps.annualized_pct < self.entry_spread_ann:
                continue
            if ps.break_even_hours >= self.max_hold_hours:
                continue
            if ps.mixed_frequency:
                # Mixed-frequency pairs (e.g. 1h+8h) have settlement timing
                # risk: rate on the 8h exchange may change before settlement.
                # Require 2x higher threshold and warn.
                if ps.annualized_pct < self.entry_spread_ann * 2:
                    self.logger.debug(
                        f"[Signal] {ps.exchange_a}-{ps.exchange_b} 混合结算频率, "
                        f"ann={ps.annualized_pct:.1f}% < 2x阈值"
                        f"{self.entry_spread_ann * 2:.0f}%, 跳过")
                    continue
                self.logger.warning(
                    f"[Signal] 混合结算频率对 {ps.exchange_a}-{ps.exchange_b}, "
                    f"存在结算时差风险")
            return ps
        return None

    def _check_signal_stability(self, sig: Optional[PairSpread]) -> bool:
        if sig is None:
            self._stable_count = 0
            self._last_stable_key = None
            return False
        key = f"{sig.short_exchange}_{sig.long_exchange}"
        if key == self._last_stable_key:
            self._stable_count += 1
        else:
            self._stable_count = 1
            self._last_stable_key = key
        return self._stable_count >= self._min_stable

    # ─── Trading ───

    async def _get_ref_price(self) -> Decimal:
        for adapter in self._adapters.values():
            try:
                mid = await adapter.get_mid_price()
                if mid and mid > 0:
                    return mid
            except Exception:
                pass
        return Decimal("70000") if self.symbol == "BTC" else Decimal("3500")

    async def _open_hedge(self, sig: PairSpread) -> bool:
        short_ad = self._adapters.get(sig.short_exchange)
        long_ad = self._adapters.get(sig.long_exchange)
        if not short_ad or not long_ad:
            return False

        ref = await self._get_ref_price()
        self.logger.info(
            f"[开仓] {sig.short_exchange}(SHORT) + {sig.long_exchange}(LONG) "
            f"size={self.order_size} spread_ann={sig.annualized_pct:.1f}% "
            f"ref={ref:.2f}")

        if self.dry_run:
            self.logger.info("[开仓] DRY-RUN 跳过实际下单")
            self._set_position(sig, self.order_size)
            return True

        s_fill, l_fill = await asyncio.gather(
            short_ad.open_position("sell", self.order_size, ref),
            long_ad.open_position("buy", self.order_size, ref),
        )
        s_ok = s_fill is not None and s_fill > 0
        l_ok = l_fill is not None and l_fill > 0

        if s_ok and l_ok:
            sp = short_ad.last_fill_price
            lp = long_ad.last_fill_price
            self._set_position(sig, min(s_fill, l_fill), sp, lp)
            basis = float(sp - lp) if sp and lp else 0
            self.logger.info(
                f"[开仓成功] {sig.short_exchange} sell={s_fill}@{sp} "
                f"{sig.long_exchange} buy={l_fill}@{lp} 基差={basis:.2f}")
            self._log_trade_csv("OPEN", sig.short_exchange, sig.long_exchange,
                                self.order_size, sig.annualized_pct)
            return True

        if s_ok and not l_ok:
            self.logger.error(f"[开仓] {sig.long_exchange}未成交, 回撤{sig.short_exchange}")
            await short_ad.open_position("buy", s_fill, ref)
        elif not s_ok and l_ok:
            self.logger.error(f"[开仓] {sig.short_exchange}未成交, 回撤{sig.long_exchange}")
            await long_ad.open_position("sell", l_fill, ref)
        else:
            self.logger.warning("[开仓] 双边未成交")
        return False

    def _set_position(self, sig: PairSpread, qty: Decimal,
                       price_short: Optional[Decimal] = None,
                       price_long: Optional[Decimal] = None):
        self.state = State.IN_POSITION
        self._pos_a = -qty
        self._pos_b = qty
        self._pos_dir = sig.direction
        self._pos_opened_at = time.time()
        self._pos_ex_short = sig.short_exchange
        self._pos_ex_long = sig.long_exchange

        self._entry_price_short = price_short
        self._entry_price_long = price_long
        mid = float(price_short or price_long or Decimal("70000"))
        self._entry_notional = float(qty) * mid
        short_fee = TAKER_FEES_BPS.get(sig.short_exchange, 5.0)
        long_fee = TAKER_FEES_BPS.get(sig.long_exchange, 5.0)
        self._entry_fee_usd = self._entry_notional * (short_fee + long_fee) / 10000
        self._cumulative_funding_usd = 0.0
        self._last_funding_ts = time.time()
        self._spread_history.clear()

    async def _close_hedge(self) -> bool:
        short_ad = self._adapters.get(self._pos_ex_short)
        long_ad = self._adapters.get(self._pos_ex_long)
        if not short_ad or not long_ad:
            return False

        close_qty = min(abs(self._pos_a), abs(self._pos_b))
        if close_qty <= 0:
            return False

        hold_h = (time.time() - self._pos_opened_at) / 3600 if self._pos_opened_at else 0
        ref = await self._get_ref_price()

        self.logger.info(
            f"[平仓] {self._pos_ex_short}(BUY) + {self._pos_ex_long}(SELL) "
            f"qty={close_qty} held={hold_h:.1f}h")

        if self.dry_run:
            self.logger.info("[平仓] DRY-RUN 跳过")
            self._log_trade_csv("CLOSE", self._pos_ex_short, self._pos_ex_long,
                                close_qty, 0, hold_h)
            self._reset_position()
            return True

        b_fill, s_fill = await asyncio.gather(
            short_ad.open_position("buy", close_qty, ref),
            long_ad.open_position("sell", close_qty, ref),
        )
        b_ok = b_fill is not None and b_fill > 0
        s_ok = s_fill is not None and s_fill > 0

        if b_ok and s_ok:
            close_sp = short_ad.last_fill_price
            close_lp = long_ad.last_fill_price
            basis_pnl = 0.0
            if self._entry_price_short and self._entry_price_long and close_sp and close_lp:
                qty_f = float(close_qty)
                basis_pnl = ((float(self._entry_price_short) - float(close_sp))
                             + (float(close_lp) - float(self._entry_price_long))) * qty_f
            funding, _, fees, net = self._get_net_pnl(basis_pnl)
            self.logger.info(
                f"[平仓成功] held={hold_h:.1f}h  "
                f"费率收益=${funding:.4f}  基差P&L=${basis_pnl:.4f}  "
                f"手续费=${fees:.4f}  净P&L=${net:.4f}")
            self._log_trade_csv("CLOSE", self._pos_ex_short, self._pos_ex_long,
                                close_qty, net, hold_h)
            self._reset_position()
            return True

        if b_ok and not s_ok:
            self.logger.error("[平仓] long side未成交, 回撤short side")
            await short_ad.open_position("sell", b_fill, ref)
        elif not b_ok and s_ok:
            self.logger.error("[平仓] short side未成交, 回撤long side")
            await long_ad.open_position("buy", s_fill, ref)
        return False

    def _reset_position(self):
        self.state = State.IDLE
        self._pos_a = Decimal("0")
        self._pos_b = Decimal("0")
        self._pos_dir = None
        self._pos_opened_at = None
        self._pos_ex_short = ""
        self._pos_ex_long = ""
        self._entry_price_short = None
        self._entry_price_long = None
        self._entry_notional = 0.0
        self._entry_fee_usd = 0.0
        self._cumulative_funding_usd = 0.0
        self._last_funding_ts = 0.0
        self._spread_history.clear()
        self._stable_count = 0
        self._last_stable_key = None

    # ─── Risk Controls ───

    def _accumulate_funding(self):
        """Each poll, accumulate estimated funding earned since last update."""
        if not self._pos_ex_short or not self._pos_ex_long:
            return
        ra = self._rates.get(self._pos_ex_short)
        rb = self._rates.get(self._pos_ex_long)
        if not ra or not rb:
            return
        now = time.time()
        dt_h = (now - self._last_funding_ts) / 3600 if self._last_funding_ts else 0
        if dt_h > 0:
            spread_1h = ra.rate_1h - rb.rate_1h
            funding_this_period = spread_1h * self._entry_notional * dt_h
            self._cumulative_funding_usd += funding_this_period
        self._last_funding_ts = now

        current_ann = (ra.rate_1h - rb.rate_1h) * 24 * 365 * 100
        self._spread_history.append((now, current_ann))
        if len(self._spread_history) > 20:
            self._spread_history = self._spread_history[-20:]

    def _get_basis_pnl(self) -> float:
        """Basis P&L = how much we'd gain/lose from price divergence if closing now."""
        if not self._entry_price_short or not self._entry_price_long:
            return 0.0
        short_ad = self._adapters.get(self._pos_ex_short)
        long_ad = self._adapters.get(self._pos_ex_long)
        if not short_ad or not long_ad:
            return 0.0
        rs = self._rates.get(self._pos_ex_short)
        rl = self._rates.get(self._pos_ex_long)
        short_now = float(self._entry_price_short)
        long_now = float(self._entry_price_long)
        if rs:
            short_now = float(self._entry_price_short)
        if rl:
            long_now = float(self._entry_price_long)
        entry_basis = float(self._entry_price_short) - float(self._entry_price_long)
        qty = float(min(abs(self._pos_a), abs(self._pos_b)))
        return entry_basis * qty * 0

    async def _get_basis_pnl_live(self) -> float:
        """Live basis P&L by querying current mid prices from adapters."""
        if not self._entry_price_short or not self._entry_price_long:
            return 0.0
        short_ad = self._adapters.get(self._pos_ex_short)
        long_ad = self._adapters.get(self._pos_ex_long)
        if not short_ad or not long_ad:
            return 0.0
        try:
            short_mid, long_mid = await asyncio.gather(
                short_ad.get_mid_price(), long_ad.get_mid_price())
        except Exception:
            return 0.0
        if short_mid is None or long_mid is None:
            return 0.0
        qty = float(min(abs(self._pos_a), abs(self._pos_b)))
        short_pnl = (float(self._entry_price_short) - float(short_mid)) * qty
        long_pnl = (float(long_mid) - float(self._entry_price_long)) * qty
        return short_pnl + long_pnl

    def _get_net_pnl(self, basis_pnl: float = 0.0) -> Tuple[float, float, float, float]:
        """Returns (funding_usd, basis_usd, total_fees_usd, net_pnl_usd).
        total_fees = open_fees + estimated_close_fees."""
        close_fee = self._entry_fee_usd
        total_fees = self._entry_fee_usd + close_fee
        net = self._cumulative_funding_usd + basis_pnl - total_fees
        return self._cumulative_funding_usd, basis_pnl, total_fees, net

    def _is_spread_declining(self) -> bool:
        """True if spread has been declining for the last N polls."""
        if len(self._spread_history) < self._spread_trend_window:
            return False
        recent = self._spread_history[-self._spread_trend_window:]
        for i in range(1, len(recent)):
            if recent[i][1] >= recent[i - 1][1]:
                return False
        return True

    def _should_close(self) -> bool:
        if not self._pos_ex_short or not self._pos_ex_long:
            return False

        ra = self._rates.get(self._pos_ex_short)
        rb = self._rates.get(self._pos_ex_long)
        if not ra or not rb:
            return False

        current_spread = ra.rate_1h - rb.rate_1h
        current_ann = current_spread * 24 * 365 * 100

        if current_spread < 0:
            self.logger.warning(
                f"[退出] 费率差反转! {self._pos_ex_short}={ra.rate_1h*100:.6f}% "
                f"{self._pos_ex_long}={rb.rate_1h*100:.6f}%  → 立即平仓")
            return True

        hold_h = 0.0
        if self._pos_opened_at:
            hold_h = (time.time() - self._pos_opened_at) / 3600
            if hold_h >= self.max_hold_hours:
                self.logger.info(f"[退出] 持仓{hold_h:.1f}h >= 上限{self.max_hold_hours:.0f}h")
                return True

        if current_ann < self.exit_spread_ann:
            self.logger.info(
                f"[退出] 年化差={current_ann:.1f}% < 阈值{self.exit_spread_ann:.0f}%")
            return True

        funding, _, fees, net = self._get_net_pnl()
        if hold_h > 2 and net < 0 and self._is_spread_declining():
            self.logger.warning(
                f"[退出] P&L亏损且趋势恶化: 累计费率=${funding:.4f} "
                f"手续费=${fees:.4f} 净=${net:.4f}  "
                f"连续{self._spread_trend_window}轮下降 → 止损平仓")
            return True

        return False

    async def _reconcile(self) -> bool:
        if self.dry_run or not self._pos_ex_short:
            return True
        short_ad = self._adapters.get(self._pos_ex_short)
        long_ad = self._adapters.get(self._pos_ex_long)
        if not short_ad or not long_ad:
            return True
        try:
            pa = await short_ad.get_position()
            pb = await long_ad.get_position()
            self._pos_a = pa
            self._pos_b = pb
            net = pa + pb
            if abs(net) > self.order_size * Decimal("0.5"):
                self.logger.warning(
                    f"[对账] 不平衡 {self._pos_ex_short}={pa} "
                    f"{self._pos_ex_long}={pb} net={net}")
                return False
            return True
        except Exception as e:
            self.logger.error(f"[对账] {e}")
            return True

    async def _emergency_flatten(self):
        self.logger.warning("[紧急平仓] 开始...")
        ref = await self._get_ref_price()
        for name, ad in self._adapters.items():
            try:
                pos = await ad.get_position()
                if abs(pos) > self.order_size * Decimal("0.01"):
                    side = "sell" if pos > 0 else "buy"
                    fill = await ad.open_position(side, abs(pos), ref)
                    self.logger.info(f"[紧急平仓] {name} {side} {abs(pos)} fill={fill}")
            except Exception as e:
                self.logger.error(f"[紧急平仓] {name}: {e}")
        self._reset_position()

    # ─── Adapter Init ───

    async def _init_adapters(self):
        factory: Dict[str, callable] = {
            "Lighter": lambda: LighterAdapter(),
            "Extended": lambda: ExtendedAdapter(),
            "Binance": lambda: CexAdapter("binance"),
        }
        for name in self.trade_exchanges:
            fn = factory.get(name)
            if not fn:
                raise ValueError(f"Unknown exchange: {name}")
            ad = fn()
            await ad.initialize(self.symbol)
            self._adapters[name] = ad
            self.logger.info(f"[Adapter] {name} ready")

    # ─── Main ───

    async def run(self):
        def on_stop(*_):
            self.stop_flag = True
            self.logger.info("收到中断信号")
        for s in (signal.SIGINT, signal.SIGTERM):
            try:
                asyncio.get_event_loop().add_signal_handler(s, on_stop)
            except NotImplementedError:
                signal.signal(s, on_stop)

        try:
            await self._main_loop()
        except KeyboardInterrupt:
            self.logger.info("用户中断")
        except Exception as e:
            self.logger.error(f"致命错误: {e}\n{traceback.format_exc()}")
        finally:
            self.logger.info("[退出] 清理...")
            if not self.monitor_only and not self.dry_run:
                if self.state == State.IN_POSITION:
                    try:
                        await self._emergency_flatten()
                    except Exception as e:
                        self.logger.error(f"退出平仓: {e}")
            for ad in self._adapters.values():
                try:
                    await ad.shutdown()
                except Exception:
                    pass
            if self._session and not self._session.closed:
                await self._session.close()

    async def _main_loop(self):
        mode = "监控" if self.monitor_only else ("DRY-RUN" if self.dry_run else "交易")
        pair_str = (f" 交易对={self.trade_exchanges[0]}+{self.trade_exchanges[1]}"
                    if not self.monitor_only else "")
        self.logger.info(
            f"启动 资金费率套利  {self.symbol}  模式={mode}  "
            f"轮询={self.poll_interval}s  "
            f"入场≥{self.entry_spread_ann:.0f}%年化  "
            f"退出≤{self.exit_spread_ann:.0f}%年化  "
            f"最大持仓={self.max_hold_hours:.0f}h{pair_str}")

        if not self.monitor_only:
            await self._init_adapters()

        self.logger.info("首次获取费率+价格...")
        await asyncio.gather(self._poll_all_rates(), self._poll_all_prices())
        if not self._rates:
            self.logger.error("无法获取费率数据")
            return
        self.logger.info(f"已获取 {len(self._rates)} 个交易所费率, {len(self._prices)} 个价格")

        last_reconcile = time.time()

        while not self.stop_flag:
            try:
                await asyncio.gather(
                    self._poll_all_rates(), self._poll_all_prices())
                if not self._rates:
                    await asyncio.sleep(self.poll_interval)
                    continue

                self._compute_pair_spreads()
                self._compute_price_spreads()
                self._display_dashboard()
                self._log_rates_csv()

                if not self.monitor_only:
                    now = time.time()

                    if self.state == State.IDLE:
                        sig = self._find_best_signal()
                        if self._check_signal_stability(sig) and sig:
                            ok = await self._open_hedge(sig)
                            if not ok:
                                await asyncio.sleep(5)

                    elif self.state == State.IN_POSITION:
                        self._accumulate_funding()
                        if self._should_close():
                            await self._close_hedge()

                        if not self.dry_run and now - last_reconcile > 120:
                            balanced = await self._reconcile()
                            last_reconcile = now
                            if not balanced:
                                self.logger.warning("[对账] 不平衡, 紧急平仓")
                                await self._emergency_flatten()

                await asyncio.sleep(self.poll_interval)
            except Exception as e:
                self.logger.error(f"主循环: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(5)


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description="跨交易所资金费率套利机器人")
    p.add_argument("-s", "--symbol", default="BTC",
                   help="交易品种 (BTC/ETH/SOL)")
    p.add_argument("--size", type=str, default="0.005",
                   help="每边仓位大小")
    p.add_argument("--poll-interval", type=float, default=60.0,
                   help="费率轮询间隔(秒)")
    p.add_argument("--entry-spread", type=float, default=50.0,
                   help="入场: 年化费率差 > X%%")
    p.add_argument("--exit-spread", type=float, default=10.0,
                   help="退出: 年化费率差 < X%%")
    p.add_argument("--max-hold-hours", type=float, default=72.0,
                   help="最大持仓时间(小时)")
    p.add_argument("--monitor-only", action="store_true",
                   help="仅监控, 不交易")
    p.add_argument("--dry-run", action="store_true",
                   help="模拟交易, 不下单")
    p.add_argument("--trade-pair", type=str, default="Lighter,Extended",
                   help="交易所对 (如 Lighter,Extended 或 Lighter,Binance)")
    p.add_argument("--env-file", default=".env")
    args = p.parse_args()

    load_dotenv(args.env_file)

    pair = tuple(args.trade_pair.split(","))
    if len(pair) != 2:
        print("[错误] --trade-pair 需要两个交易所, 如: Lighter,Extended")
        sys.exit(1)

    bot = FundingArbBot(
        symbol=args.symbol.upper(),
        order_size=Decimal(args.size),
        poll_interval=args.poll_interval,
        entry_spread_ann=args.entry_spread,
        exit_spread_ann=args.exit_spread,
        max_hold_hours=args.max_hold_hours,
        monitor_only=args.monitor_only,
        dry_run=args.dry_run,
        trade_exchanges=pair,
    )

    if uvloop is not None:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("[uvloop] 已启用")

    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n已中断。")


if __name__ == "__main__":
    main()
