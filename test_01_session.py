#!/usr/bin/env python3
"""Diagnose 01 Exchange session creation step by step."""
import time, sys, os, binascii, requests
from dotenv import load_dotenv
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from base58 import b58decode, b58encode
from google.protobuf.internal import encoder, decoder

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import schema_pb2

load_dotenv()

API_URL = "https://zo-mainnet.n1.xyz"

pk = os.getenv("O1_PRIVATE_KEY")
key_bytes = b58decode(pk)
user_key = Ed25519PrivateKey.from_private_bytes(key_bytes[:32])
user_pubkey = user_key.public_key().public_bytes_raw()
user_pubkey_b58 = b58encode(user_pubkey).decode()

print(f"钱包公钥: {user_pubkey_b58}")
print(f"Account ID: {os.getenv('O1_ACCOUNT_ID')}")
print()

# Step 1: Get timestamp
print("=== Step 1: 获取服务器时间 ===")
t0 = time.time()
resp = requests.get(f"{API_URL}/timestamp", timeout=15)
server_time = int(resp.json())
print(f"服务器时间: {server_time} (耗时: {time.time()-t0:.3f}s)")
print()

# Step 2: Check existing sessions
print("=== Step 2: 检查现有sessions ===")
t0 = time.time()
resp = requests.get(f"{API_URL}/user/{user_pubkey_b58}", timeout=15)
data = resp.json()
sessions = data.get("sessions", {})
print(f"现有session数量: {len(sessions)} (耗时: {time.time()-t0:.3f}s)")
for sid, info in list(sessions.items())[:5]:
    print(f"  session {sid}: pubkey={info['pubkey'][:16]}... 过期={info['expiry']}")
if len(sessions) > 5:
    print(f"  ... 还有 {len(sessions)-5} 个")
print()

# Step 3: Build CreateSession protobuf
print("=== Step 3: 构建CreateSession请求 ===")
session_key = Ed25519PrivateKey.generate()
session_pubkey = session_key.public_key().public_bytes_raw()
session_pubkey_b58 = b58encode(session_pubkey).decode()

action = schema_pb2.Action()
action.current_timestamp = server_time
action.nonce = 0
action.create_session.user_pubkey = user_pubkey
action.create_session.session_pubkey = session_pubkey
action.create_session.expiry_timestamp = server_time + 3600

payload = action.SerializeToString()
message = encoder._VarintBytes(len(payload)) + payload
signature = user_key.sign(binascii.hexlify(message))
body = message + signature

print(f"Payload大小: {len(payload)} bytes")
print(f"签名大小: {len(signature)} bytes")
print(f"总请求大小: {len(body)} bytes")
print(f"Session pubkey: {session_pubkey_b58}")
print()

# Step 4: Send with curl subprocess (to compare with requests)
print("=== Step 4: 用curl发送POST ===")
import subprocess, tempfile
with tempfile.NamedTemporaryFile(delete=False, suffix='.bin') as f:
    f.write(body)
    tmpfile = f.name

t0 = time.time()
result = subprocess.run(
    ['curl', '-s', '-o', '/dev/null', '-w', 'HTTP状态:%{http_code} 耗时:%{time_total}s',
     '--max-time', '30',
     '-X', 'POST', f'{API_URL}/action',
     '-H', 'Content-Type: application/octet-stream',
     '--data-binary', f'@{tmpfile}'],
    capture_output=True, text=True, timeout=35
)
print(f"curl结果: {result.stdout} (Python端耗时: {time.time()-t0:.3f}s)")
os.unlink(tmpfile)
print()

# Step 5: Send with Python requests
print("=== Step 5: 用Python requests发送POST ===")
t0 = time.time()
try:
    resp = requests.post(
        f"{API_URL}/action",
        data=body,
        headers={"Content-Type": "application/octet-stream"},
        timeout=30,
    )
    elapsed = time.time() - t0
    print(f"状态: {resp.status_code} 耗时: {elapsed:.3f}s")
    print(f"响应大小: {len(resp.content)} bytes")

    msg_len, pos = decoder._DecodeVarint32(resp.content, 0)
    receipt = schema_pb2.Receipt()
    receipt.ParseFromString(resp.content[pos:pos + msg_len])

    if receipt.HasField("err"):
        err_name = schema_pb2.Error.Name(receipt.err)
        print(f"错误: {err_name} (code={receipt.err})")
    elif receipt.HasField("create_session_result"):
        sid = receipt.create_session_result.session_id
        print(f"成功! Session ID: {sid}")
    else:
        print(f"未知响应: {receipt}")
except requests.exceptions.Timeout:
    print(f"超时! 耗时: {time.time()-t0:.3f}s")
except Exception as e:
    print(f"错误: {e} 耗时: {time.time()-t0:.3f}s")
