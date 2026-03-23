#!/bin/bash
#
# 部署 Extended+Lighter 套利脚本到 VPS
# 用法: bash deploy_arb_vps.sh
#

VPS_IP="103.53.81.249"
VPS_USER="root"
REMOTE_DIR="/opt/ext-lighter-arb"

echo "============================================"
echo "  Extended+Lighter 套利 VPS 部署脚本"
echo "  目标: ${VPS_USER}@${VPS_IP}:${REMOTE_DIR}"
echo "============================================"
echo ""

# ── 1. 打包 ──
echo "[1/4] 打包文件..."
TMPDIR=$(mktemp -d)
DEPLOY_DIR="${TMPDIR}/arb"
mkdir -p "${DEPLOY_DIR}/feeds" "${DEPLOY_DIR}/exchanges" "${DEPLOY_DIR}/helpers" "${DEPLOY_DIR}/logs"

# 主脚本
cp extended_lighter_arb.py "${DEPLOY_DIR}/"
cp .env "${DEPLOY_DIR}/"

# feeds (只拷贝套利需要的)
cp feeds/base_feed.py "${DEPLOY_DIR}/feeds/"
cp feeds/lighter_feed.py "${DEPLOY_DIR}/feeds/"
cp feeds/existing_feeds.py "${DEPLOY_DIR}/feeds/"

# 精简版 feeds/__init__.py —— 只导入套利需要的
cat > "${DEPLOY_DIR}/feeds/__init__.py" << 'PYEOF'
from .base_feed import BaseFeed, OrderBookSnapshot
from .lighter_feed import LighterFeed
from .existing_feeds import ExtendedFeed
PYEOF

# exchanges
cp exchanges/base.py "${DEPLOY_DIR}/exchanges/"
cp exchanges/extended.py "${DEPLOY_DIR}/exchanges/"

# 精简版 exchanges/__init__.py
cat > "${DEPLOY_DIR}/exchanges/__init__.py" << 'PYEOF'
from .base import BaseExchangeClient, query_retry
PYEOF

# helpers
cp helpers/__init__.py "${DEPLOY_DIR}/helpers/"
cp helpers/logger.py "${DEPLOY_DIR}/helpers/"

# requirements (只安装套利需要的包)
cat > "${DEPLOY_DIR}/requirements.txt" << 'REQEOF'
python-dotenv>=1.0.0
pytz>=2025.2
aiohttp>=3.8.0
websockets>=12.0
requests>=2.31.0
tenacity>=9.1.2
pycryptodome>=3.15.0
ecdsa>=0.17.0
cryptography>=41.0.0
pydantic>=2.0.0
eval_type_backport
lighter-sdk==1.0.2
x10-python-trading-starknet==0.0.10
uvloop>=0.19.0
REQEOF

FILE_COUNT=$(find "${DEPLOY_DIR}" -type f | wc -l | tr -d ' ')
echo "  已打包 ${FILE_COUNT} 个文件"

# ── 2. 上传 ──
echo ""
echo "[2/4] 上传到 VPS... (需要输入密码)"
echo ""

ssh "${VPS_USER}@${VPS_IP}" "mkdir -p ${REMOTE_DIR}/{feeds,exchanges,helpers,logs}"

scp -r "${DEPLOY_DIR}"/* "${VPS_USER}@${VPS_IP}:${REMOTE_DIR}/"
if [ $? -ne 0 ]; then
    echo "ERROR: 上传失败"
    rm -rf "${TMPDIR}"
    exit 1
fi
echo "  上传完成!"

# ── 3. 安装依赖 ──
echo ""
echo "[3/4] 在 VPS 上安装依赖..."
ssh "${VPS_USER}@${VPS_IP}" << 'REMOTE_SCRIPT'
set -e
cd /opt/ext-lighter-arb

if ! command -v python3 &> /dev/null; then
    echo "安装 Python3..."
    apt-get update -qq && apt-get install -y -qq python3 python3-venv python3-pip python3-dev build-essential
fi

if [ ! -d "venv" ]; then
    echo "创建虚拟环境..."
    python3 -m venv venv
fi

echo "安装 Python 依赖..."
./venv/bin/pip install --upgrade pip -q
./venv/bin/pip install -r requirements.txt -q

echo "依赖安装完成!"
./venv/bin/python3 -c "import lighter; import x10; print('SDK 验证通过')"
REMOTE_SCRIPT

if [ $? -ne 0 ]; then
    echo "ERROR: 远程安装失败"
    rm -rf "${TMPDIR}"
    exit 1
fi

# ── 4. 创建 systemd 服务 ──
echo ""
echo "[4/4] 配置 systemd 服务..."

ssh "${VPS_USER}@${VPS_IP}" << 'REMOTE_SCRIPT'
set -e

# ETH 服务
cat > /etc/systemd/system/arb-eth.service << 'SVC'
[Unit]
Description=Extended-Lighter ARB ETH
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/ext-lighter-arb
ExecStart=/opt/ext-lighter-arb/venv/bin/python3 -u extended_lighter_arb.py \
    --symbol ETH --size 0.02 --max-position 0.06 \
    --open-bps 4.5 --close-bps -1.0 --max-hold-sec 1200 \
    --cooldown 10 --loss-breaker-bps -30 --loss-breaker-pause 600
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
SVC

# BTC 服务
cat > /etc/systemd/system/arb-btc.service << 'SVC'
[Unit]
Description=Extended-Lighter ARB BTC
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/ext-lighter-arb
ExecStart=/opt/ext-lighter-arb/venv/bin/python3 -u extended_lighter_arb.py \
    --symbol BTC --size 0.002 --max-position 0.006 \
    --open-bps 4.5 --close-bps -1.0 --max-hold-sec 1200 \
    --cooldown 10 --loss-breaker-bps -30 --loss-breaker-pause 600
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
SVC

systemctl daemon-reload

# 停旧启新
systemctl stop arb-eth 2>/dev/null || true
systemctl stop arb-btc 2>/dev/null || true

systemctl enable arb-eth arb-btc
systemctl start arb-eth
systemctl start arb-btc

sleep 3

echo ""
echo "============================================"
echo "  部署完成!"
echo "============================================"
echo ""

for svc in arb-eth arb-btc; do
    if systemctl is-active --quiet $svc; then
        echo "  ✅ $svc: 运行中"
    else
        echo "  ❌ $svc: 启动失败"
    fi
done

echo ""
echo "常用命令:"
echo "  查看 ETH 日志:  journalctl -u arb-eth -f"
echo "  查看 BTC 日志:  journalctl -u arb-btc -f"
echo "  重启 ETH:       systemctl restart arb-eth"
echo "  重启 BTC:       systemctl restart arb-btc"
echo "  停止全部:       systemctl stop arb-eth arb-btc"
echo "  查看状态:       systemctl status arb-eth arb-btc"
REMOTE_SCRIPT

# 清理
rm -rf "${TMPDIR}"

echo ""
echo "部署完成! 在 VPS 上查看日志:"
echo "  ssh ${VPS_USER}@${VPS_IP} 'journalctl -u arb-eth -f'"
echo "  ssh ${VPS_USER}@${VPS_IP} 'journalctl -u arb-btc -f'"
