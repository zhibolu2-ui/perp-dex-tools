"""
01 Exchange (01.xyz) client for trading via Nord API.
Uses Protobuf + Ed25519 session-based authentication.
"""

import binascii
import json
import time
import logging
import requests
from typing import Optional, Tuple, Dict, Any

from google.protobuf.internal import encoder, decoder
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from base58 import b58decode, b58encode

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import schema_pb2

logger = logging.getLogger(__name__)

API_URL = "https://zo-mainnet.n1.xyz"


def _varint_bytes(value: int) -> bytes:
    return encoder._VarintBytes(value)


def _read_varint(buf: bytes, offset: int = 0) -> Tuple[int, int]:
    return decoder._DecodeVarint32(buf, offset)


def _user_sign(key: Ed25519PrivateKey, msg: bytes) -> bytes:
    return key.sign(binascii.hexlify(msg))


def _session_sign(key: Ed25519PrivateKey, msg: bytes) -> bytes:
    return key.sign(msg)


class O1Client:
    """Client for 01 Exchange trading operations."""

    def __init__(self, private_key_b58: str, account_id: int):
        key_bytes = b58decode(private_key_b58)
        self.user_key = Ed25519PrivateKey.from_private_bytes(key_bytes[:32])
        self.user_pubkey = self.user_key.public_key().public_bytes_raw()
        self.account_id = account_id

        self.session_id: Optional[int] = None
        self.session_key: Optional[Ed25519PrivateKey] = None
        self.session_expiry: float = 0

        self._market_cache: Dict[str, Dict] = {}

    def _get_server_time(self) -> int:
        for attempt in range(3):
            try:
                resp = requests.get(f"{API_URL}/timestamp", timeout=15)
                resp.raise_for_status()
                return int(resp.json())
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt < 2:
                    logger.warning(f"获取01服务器时间超时, 重试 {attempt+1}/2")
                    time.sleep(2)
                    continue
                raise

    def _execute_action(
        self,
        action: schema_pb2.Action,
        signing_key: Ed25519PrivateKey,
        sign_func,
    ) -> schema_pb2.Receipt:
        """Send a single action. NO internal retries to avoid DUPLICATE errors."""
        payload = action.SerializeToString()
        message = _varint_bytes(len(payload)) + payload
        signature = sign_func(signing_key, message)

        resp = requests.post(
            f"{API_URL}/action",
            data=message + signature,
            headers={"Content-Type": "application/octet-stream"},
            timeout=30,
        )
        resp.raise_for_status()

        msg_len, pos = _read_varint(resp.content, 0)
        receipt = schema_pb2.Receipt()
        receipt.ParseFromString(resp.content[pos : pos + msg_len])
        return receipt

    def create_session(self, ttl_seconds: int = 3600) -> int:
        """Create a new session. Uses long timeout since server may need to
        clean up old sessions (can take 30-60s with many active sessions)."""
        self.session_key = Ed25519PrivateKey.generate()
        session_pubkey = self.session_key.public_key().public_bytes_raw()
        session_pubkey_b58 = b58encode(session_pubkey).decode()

        server_time = self._get_server_time()

        action = schema_pb2.Action()
        action.current_timestamp = server_time
        action.nonce = 0
        action.create_session.user_pubkey = self.user_pubkey
        action.create_session.session_pubkey = session_pubkey
        action.create_session.expiry_timestamp = server_time + ttl_seconds

        payload = action.SerializeToString()
        message = _varint_bytes(len(payload)) + payload
        signature = _user_sign(self.user_key, message)
        body = message + signature

        logger.info("01 正在创建session (首次可能需要30-60秒清理旧session)...")

        try:
            resp = requests.post(
                f"{API_URL}/action",
                data=body,
                headers={"Content-Type": "application/octet-stream"},
                timeout=120,
            )
            resp.raise_for_status()

            msg_len, pos = _read_varint(resp.content, 0)
            receipt = schema_pb2.Receipt()
            receipt.ParseFromString(resp.content[pos:pos + msg_len])

            if receipt.HasField("err"):
                err_name = schema_pb2.Error.Name(receipt.err)
                if err_name == "DUPLICATE":
                    found = self._find_session_by_pubkey(session_pubkey_b58)
                    if found:
                        self.session_id = found
                        self.session_expiry = time.time() + ttl_seconds - 60
                        logger.info(f"01 session已存在: id={found}")
                        return found
                raise RuntimeError(f"CreateSession failed: {err_name}")

            self.session_id = receipt.create_session_result.session_id
            self.session_expiry = time.time() + ttl_seconds - 60
            logger.info(f"01 session created: id={self.session_id}")
            return self.session_id

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            logger.warning("01 CreateSession超时, 轮询检查是否已创建...")
            for poll in range(12):
                time.sleep(10)
                found = self._find_session_by_pubkey(session_pubkey_b58)
                if found:
                    self.session_id = found
                    self.session_expiry = time.time() + ttl_seconds - 60
                    logger.info(f"01 session超时但已创建成功: id={found}")
                    return found
                logger.info(f"01 等待session创建... ({(poll+1)*10}秒)")
            raise RuntimeError("CreateSession超时且120秒后仍未找到")

    def _find_session_by_pubkey(self, session_pubkey_b58: str) -> Optional[int]:
        """Check if a session with the given pubkey exists on the server."""
        try:
            user_pubkey_b58 = b58encode(self.user_pubkey).decode()
            resp = requests.get(
                f"{API_URL}/user/{user_pubkey_b58}", timeout=15)
            resp.raise_for_status()
            data = resp.json()
            sessions = data.get("sessions", {})
            for sid_str, info in sessions.items():
                if info.get("pubkey") == session_pubkey_b58:
                    return int(sid_str)
        except Exception as e:
            logger.warning(f"01 查找session失败: {e}")
        return None

    def ensure_session(self):
        if self.session_id is None or time.time() > self.session_expiry:
            self.create_session()

    def get_market_info(self) -> Dict[str, Dict]:
        if self._market_cache:
            return self._market_cache
        resp = requests.get(f"{API_URL}/info", timeout=15)
        resp.raise_for_status()
        for m in resp.json()["markets"]:
            self._market_cache[m["symbol"]] = {
                "market_id": m["marketId"],
                "price_decimals": m["priceDecimals"],
                "size_decimals": m["sizeDecimals"],
            }
        return self._market_cache

    def get_orderbook(self, market_id: int) -> Dict:
        resp = requests.get(f"{API_URL}/market/{market_id}/orderbook", timeout=15)
        resp.raise_for_status()
        return resp.json()

    def get_account(self) -> Dict:
        resp = requests.get(f"{API_URL}/account/{self.account_id}", timeout=15)
        resp.raise_for_status()
        return resp.json()

    def place_order(
        self,
        market_id: int,
        side: str,
        price_raw: int,
        size_raw: int,
        fill_mode: int = schema_pb2.FillMode.LIMIT,
        reduce_only: bool = False,
        client_order_id: Optional[int] = None,
    ) -> schema_pb2.Receipt:
        self.ensure_session()
        server_time = self._get_server_time()

        action = schema_pb2.Action()
        action.current_timestamp = server_time
        action.place_order.session_id = self.session_id
        action.place_order.market_id = market_id
        action.place_order.side = (
            schema_pb2.Side.BID if side.lower() == "buy" else schema_pb2.Side.ASK
        )
        action.place_order.fill_mode = fill_mode
        action.place_order.is_reduce_only = reduce_only
        action.place_order.price = price_raw
        action.place_order.size = size_raw

        if client_order_id is not None:
            action.place_order.client_order_id = client_order_id

        action.place_order.sender_account_id = self.account_id

        receipt = self._execute_action(action, self.session_key, _session_sign)

        if receipt.HasField("err"):
            err_name = schema_pb2.Error.Name(receipt.err)
            raise RuntimeError(f"PlaceOrder failed: {err_name}")

        return receipt

    def cancel_order(self, order_id: int) -> schema_pb2.Receipt:
        self.ensure_session()
        server_time = self._get_server_time()

        action = schema_pb2.Action()
        action.current_timestamp = server_time
        action.cancel_order_by_id.session_id = self.session_id
        action.cancel_order_by_id.order_id = order_id
        action.cancel_order_by_id.sender_account_id = self.account_id

        receipt = self._execute_action(action, self.session_key, _session_sign)

        if receipt.HasField("err"):
            err_name = schema_pb2.Error.Name(receipt.err)
            raise RuntimeError(f"CancelOrder failed: {err_name}")

        return receipt

    def cancel_order_by_client_id(self, client_order_id: int) -> schema_pb2.Receipt:
        self.ensure_session()
        server_time = self._get_server_time()

        action = schema_pb2.Action()
        action.current_timestamp = server_time
        action.cancel_order_by_client_id.session_id = self.session_id
        action.cancel_order_by_client_id.client_order_id = client_order_id
        action.cancel_order_by_client_id.sender_account_id = self.account_id

        receipt = self._execute_action(action, self.session_key, _session_sign)

        if receipt.HasField("err"):
            err_name = schema_pb2.Error.Name(receipt.err)
            raise RuntimeError(f"CancelOrderByClientId failed: {err_name}")

        return receipt
