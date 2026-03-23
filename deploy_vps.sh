#!/bin/bash
#
# 部署 Lighter 价差扫描器到 VPS
# 用法: bash deploy_vps.sh
#

VPS_IP="36.50.84.220"
VPS_USER="root"
REMOTE_DIR="/opt/lighter-scanner"

echo "============================================"
echo "  Lighter 价差扫描器 VPS 部署脚本"
echo "  目标: ${VPS_USER}@${VPS_IP}:${REMOTE_DIR}"
echo "============================================"
echo ""

# 1. 创建部署包
echo "[1/4] 打包文件..."
TMPDIR=$(mktemp -d)
DEPLOY_DIR="${TMPDIR}/lighter-scanner"
mkdir -p "${DEPLOY_DIR}/feeds"

# 核心文件
cp web_scanner.py "${DEPLOY_DIR}/"
cp web_dashboard.html "${DEPLOY_DIR}/"
cp spread_scanner.py "${DEPLOY_DIR}/"

# feeds 模块
cp feeds/__init__.py "${DEPLOY_DIR}/feeds/"
cp feeds/base_feed.py "${DEPLOY_DIR}/feeds/"
cp feeds/lighter_feed.py "${DEPLOY_DIR}/feeds/"
cp feeds/hyperliquid_feed.py "${DEPLOY_DIR}/feeds/"
cp feeds/ccxt_feed.py "${DEPLOY_DIR}/feeds/"
cp feeds/existing_feeds.py "${DEPLOY_DIR}/feeds/"
cp feeds/dex_feeds.py "${DEPLOY_DIR}/feeds/"
cp feeds/spread_calculator.py "${DEPLOY_DIR}/feeds/"
cp feeds/funding_monitor.py "${DEPLOY_DIR}/feeds/"
cp feeds/dashboard.py "${DEPLOY_DIR}/feeds/"

# 精简版 requirements
cat > "${DEPLOY_DIR}/requirements.txt" << 'EOF'
aiohttp>=3.9.0
websockets>=12.0
ccxt>=4.0.0
hyperliquid-python-sdk>=0.4.0
EOF

# systemd 服务文件
cat > "${DEPLOY_DIR}/lighter-scanner.service" << 'EOF'
[Unit]
Description=Lighter Spread Scanner Web Dashboard
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/lighter-scanner
ExecStart=/opt/lighter-scanner/venv/bin/python3 web_scanner.py --symbol ETH --port 8888
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

echo "  已打包 $(find ${DEPLOY_DIR} -type f | wc -l | tr -d ' ') 个文件"

# 2. 上传到 VPS
echo ""
echo "[2/4] 上传到 VPS..."
echo "  (需要输入 VPS 密码)"
echo ""

ssh "${VPS_USER}@${VPS_IP}" "mkdir -p ${REMOTE_DIR}/feeds"
scp -r "${DEPLOY_DIR}"/* "${VPS_USER}@${VPS_IP}:${REMOTE_DIR}/"

if [ $? -ne 0 ]; then
    echo "ERROR: 上传失败，请检查 VPS 连接"
    rm -rf "${TMPDIR}"
    exit 1
fi
echo "  上传完成!"

# 3. 远程安装依赖
echo ""
echo "[3/4] 在 VPS 上安装依赖..."
ssh "${VPS_USER}@${VPS_IP}" << 'REMOTE_SCRIPT'
set -e
cd /opt/lighter-scanner

# 安装 Python3 和 venv (如果没有)
if ! command -v python3 &> /dev/null; then
    echo "安装 Python3..."
    apt-get update -qq && apt-get install -y -qq python3 python3-venv python3-pip
fi

# 创建虚拟环境
if [ ! -d "venv" ]; then
    echo "创建虚拟环境..."
    python3 -m venv venv
fi

# 安装依赖
echo "安装 Python 依赖..."
./venv/bin/pip install --upgrade pip -q
./venv/bin/pip install -r requirements.txt -q

echo "依赖安装完成!"
REMOTE_SCRIPT

if [ $? -ne 0 ]; then
    echo "ERROR: 远程安装失败"
    rm -rf "${TMPDIR}"
    exit 1
fi

# 4. 配置并启动服务
echo ""
echo "[4/4] 配置并启动服务..."
ssh "${VPS_USER}@${VPS_IP}" << 'REMOTE_SCRIPT'
set -e
cd /opt/lighter-scanner

# 复制 systemd 服务
cp lighter-scanner.service /etc/systemd/system/
systemctl daemon-reload

# 停止旧服务 (如果存在)
systemctl stop lighter-scanner 2>/dev/null || true

# 启动服务
systemctl enable lighter-scanner
systemctl start lighter-scanner

sleep 2

# 检查状态
if systemctl is-active --quiet lighter-scanner; then
    echo ""
    echo "============================================"
    echo "  部署成功!"
    echo "  服务状态: 运行中"
    echo "  访问地址: http://36.50.84.220:8888"
    echo "============================================"
    echo ""
    echo "常用命令:"
    echo "  查看状态:  systemctl status lighter-scanner"
    echo "  查看日志:  journalctl -u lighter-scanner -f"
    echo "  重启服务:  systemctl restart lighter-scanner"
    echo "  停止服务:  systemctl stop lighter-scanner"
else
    echo "WARNING: 服务启动可能失败，请检查日志:"
    echo "  journalctl -u lighter-scanner -n 50"
fi
REMOTE_SCRIPT

# 清理
rm -rf "${TMPDIR}"

echo ""
echo "完成! 在浏览器打开 http://36.50.84.220:8888"
