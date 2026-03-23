#!/usr/bin/env python3
"""
Web-based cross-exchange spread scanner dashboard.

Reuses the same feed infrastructure as spread_scanner.py but serves
a real-time browser dashboard via aiohttp + WebSocket.

Usage:
    python web_scanner.py --symbol ETH
    python web_scanner.py --symbol ETH --port 8080 --size 50000
    python web_scanner.py --symbol ETH --with-cex
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from aiohttp import web

from feeds.convergence_tracker import ConvergenceTrackerGroup

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("web_scanner")
logger.setLevel(logging.INFO)

ROOT_DIR = Path(__file__).parent

# ─── Feed registry (same as spread_scanner.py) ───────────────

FEED_REGISTRY_SPEC = {
    "binance":     ("CcxtFeed", ("binance",)),
    "bybit":       ("CcxtFeed", ("bybit",)),
    "okx":         ("CcxtFeed", ("okx",)),
    "gate":        ("CcxtFeed", ("gate",)),
    "hyperliquid": ("HyperliquidFeed", ()),
    "aevo":        ("AevoFeed", ()),
    "dydx":        ("CcxtFeed", ("dydx",)),
    "paradex":     ("CcxtFeed", ("paradex",)),
    "vertex":      ("VertexFeed", ()),
    "orderly":     ("OrderlyFeed", ()),
    "rabbitx":     ("RabbitXFeed", ()),
    "bluefin":     ("BluefinFeed", ()),
    "zeta":        ("ZetaFeed", ()),
    "01":          ("O1Feed", ()),
    "grvt":        ("GrvtFeed", ()),
    "edgex":       ("EdgeXFeed", ()),
    "backpack":    ("BackpackFeed", ()),
    "apex":        ("ApexFeed", ()),
    "extended":    ("ExtendedFeed", ()),
}

DEFAULT_DEX_EXCHANGES = [
    "hyperliquid",
    "01", "grvt", "edgex", "backpack", "extended",
]


def _lazy_imports():
    from feeds.base_feed import BaseFeed
    from feeds.lighter_feed import LighterFeed
    from feeds.hyperliquid_feed import HyperliquidFeed
    from feeds.ccxt_feed import CcxtFeed
    from feeds.existing_feeds import (
        O1Feed, GrvtFeed, EdgeXFeed, BackpackFeed, ApexFeed, ExtendedFeed,
    )
    from feeds.dex_feeds import (
        AevoFeed, VertexFeed, OrderlyFeed,
        RabbitXFeed, BluefinFeed, ZetaFeed,
    )
    from feeds.spread_calculator import SpreadCalculator
    from feeds.funding_monitor import FundingRateMonitor
    return {
        "BaseFeed": BaseFeed,
        "LighterFeed": LighterFeed,
        "HyperliquidFeed": HyperliquidFeed,
        "CcxtFeed": CcxtFeed,
        "O1Feed": O1Feed,
        "GrvtFeed": GrvtFeed,
        "EdgeXFeed": EdgeXFeed,
        "BackpackFeed": BackpackFeed,
        "ApexFeed": ApexFeed,
        "ExtendedFeed": ExtendedFeed,
        "AevoFeed": AevoFeed,
        "VertexFeed": VertexFeed,
        "OrderlyFeed": OrderlyFeed,
        "RabbitXFeed": RabbitXFeed,
        "BluefinFeed": BluefinFeed,
        "ZetaFeed": ZetaFeed,
        "SpreadCalculator": SpreadCalculator,
        "FundingRateMonitor": FundingRateMonitor,
    }


def _create_feed(modules, name: str, symbol: str):
    spec = FEED_REGISTRY_SPEC.get(name)
    if spec is None:
        return None
    cls_key, extra = spec
    cls = modules[cls_key]
    return cls(symbol, *extra)


# ─── Scanner engine ──────────────────────────────────────────

class ScannerEngine:
    """Manages all feeds, computes spreads, and serialises data to JSON."""

    def __init__(self, symbol: str, trade_size_usd: float,
                 exchange_names: list, lighter_market=None,
                 conv_open_bps: float = 8.0, conv_close_bps: float = 3.0):
        self.symbol = symbol.upper()
        self.trade_size_usd = trade_size_usd
        self._exchange_names = exchange_names
        self._lighter_market = lighter_market
        self.conv_open_bps = float(conv_open_bps)
        self.conv_close_bps = float(conv_close_bps)
        self._convergence = ConvergenceTrackerGroup()
        self._M = None
        self._lighter = None
        self._targets = []
        self._calculator = None
        self._funding = None
        self._tasks = []

    async def start(self):
        self._M = _lazy_imports()
        self._lighter = self._M["LighterFeed"](
            self.symbol, market_index=self._lighter_market
        )
        for name in self._exchange_names:
            try:
                feed = _create_feed(self._M, name, self.symbol)
                if feed:
                    self._targets.append(feed)
                    logger.info("已注册: %s", feed.EXCHANGE_NAME)
            except Exception as exc:
                logger.warning("初始化 %s 失败: %s", name, exc)

        self._calculator = self._M["SpreadCalculator"](
            self._lighter, self._targets, trade_size_usd=self.trade_size_usd
        )
        self._funding = self._M["FundingRateMonitor"](
            self.symbol, self._targets, poll_interval=60.0
        )

        self._tasks.append(asyncio.create_task(self._lighter.connect()))
        for feed in self._targets:
            self._tasks.append(asyncio.create_task(feed.connect()))
        self._tasks.append(asyncio.create_task(self._funding.run()))

        logger.info("已启动 %d 个数据源", 1 + len(self._targets))

    def get_snapshot(self) -> dict:
        spreads = self._calculator.compute_all() if self._calculator else []
        funding = self._funding.get_spreads() if self._funding else []

        lighter_mid = spreads[0].lighter_mid if spreads else 0.0
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        spread_rows = []
        for i, r in enumerate(spreads, 1):
            spread_rows.append({
                "rank": i,
                "exchange": r.exchange,
                "connected": r.lighter_connected and r.exchange_connected,
                "exchange_mid": round(r.exchange_mid, 2) if r.exchange_mid else None,
                "raw_spread_bps": round(r.raw_spread_bps, 2),
                "round_trip_gross_bps": round(r.round_trip_gross_bps, 2),
                "open_leg_buyL_sellX_bps": round(r.open_leg_buyL_sellX_bps, 2),
                "open_leg_buyX_sellL_bps": round(r.open_leg_buyX_sellL_bps, 2),
                "total_fee_bps": round(r.total_fee_bps, 2),
                "slippage_lighter_bps": round(r.slippage_lighter_bps, 2),
                "slippage_exchange_bps": round(r.slippage_exchange_bps, 2),
                "net_profit_bps": round(r.net_profit_bps, 2),
                "direction": r.direction,
                "depth_lighter": round(r.depth_usd_lighter, 0),
                "depth_exchange": round(r.depth_usd_exchange, 0),
            })

        funding_rows = []
        for fs in funding:
            funding_rows.append({
                "exchange": fs.exchange,
                "exchange_rate": fs.exchange_rate,
                "lighter_rate": fs.lighter_rate,
                "spread_1h": fs.spread_1h,
                "annualized_pct": round(fs.annualized_pct, 2) if fs.annualized_pct is not None else None,
            })

        conv_payload = {
            "open_bps": self.conv_open_bps,
            "close_bps": self.conv_close_bps,
            "rows": [],
        }
        if self._lighter and self._targets:
            ls_snap = self._lighter.snapshot
            lt = self._lighter.TAKER_FEE_BPS
            if ls_snap.mid:
                self._convergence.tick_all(
                    ls_snap,
                    self._targets,
                    self.trade_size_usd,
                    lt,
                    self.conv_open_bps,
                    self.conv_close_bps,
                )
            conv_payload["rows"] = self._convergence.snapshot(
                ls_snap,
                self._targets,
                self.trade_size_usd,
                lt,
                self.conv_open_bps,
                self.conv_close_bps,
            )

        return {
            "symbol": self.symbol,
            "lighter_mid": round(lighter_mid, 2),
            "trade_size_usd": self.trade_size_usd,
            "timestamp": now_str,
            "spreads": spread_rows,
            "funding": funding_rows,
            "convergence": conv_payload,
        }

    async def stop(self):
        tasks = [self._lighter.disconnect()] if self._lighter else []
        for feed in self._targets:
            tasks.append(feed.disconnect())
        if self._funding:
            tasks.append(self._funding.stop())
        await asyncio.gather(*tasks, return_exceptions=True)
        for t in self._tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)


# ─── Web handlers ────────────────────────────────────────────

_state = {
    "clients": set(),
    "engine": None,
}


async def handle_index(request):
    html_path = ROOT_DIR / "web_dashboard.html"
    return web.FileResponse(html_path)


async def handle_ws(request):
    ws_resp = web.WebSocketResponse()
    await ws_resp.prepare(request)
    _state["clients"].add(ws_resp)
    logger.info("WebSocket 客户端连接 (%d 在线)", len(_state["clients"]))
    try:
        async for msg in ws_resp:
            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)
                eng = _state["engine"]
                if data.get("type") == "set_size" and eng:
                    new_size = float(data.get("size", 10000))
                    eng.trade_size_usd = new_size
                    if eng._calculator:
                        eng._calculator.trade_size_usd = new_size
                if data.get("type") == "set_convergence" and eng:
                    eng.conv_open_bps = float(data.get("open_bps", eng.conv_open_bps))
                    eng.conv_close_bps = float(data.get("close_bps", eng.conv_close_bps))
                    if eng.conv_close_bps >= eng.conv_open_bps:
                        eng.conv_close_bps = max(0.0, eng.conv_open_bps - 0.5)
            elif msg.type == web.WSMsgType.ERROR:
                break
    finally:
        _state["clients"].discard(ws_resp)
        logger.info("WebSocket 客户端断开 (%d 在线)", len(_state["clients"]))
    return ws_resp


async def handle_api_data(request):
    eng = _state["engine"]
    if eng is None:
        return web.json_response({"error": "not ready"}, status=503)
    data = eng.get_snapshot()
    return web.json_response(data)


async def broadcast_loop(refresh: float):
    while True:
        clients = _state["clients"]
        eng = _state["engine"]
        if clients and eng:
            data = eng.get_snapshot()
            payload = json.dumps(data)
            dead = set()
            for ws_conn in clients:
                try:
                    await ws_conn.send_str(payload)
                except Exception:
                    dead.add(ws_conn)
            if dead:
                _state["clients"] -= dead
        await asyncio.sleep(refresh)


# ─── Main ────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Web 版跨交易所价差扫描器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python web_scanner.py --symbol ETH
  python web_scanner.py --symbol ETH --port 8080 --size 50000
  python web_scanner.py --symbol ETH --with-cex
""",
    )
    p.add_argument("--symbol", "-s", default="ETH")
    p.add_argument("--size", type=float, default=10_000.0)
    p.add_argument("--port", type=int, default=8888)
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--exchanges", "-e", default=None)
    p.add_argument("--all", action="store_true")
    p.add_argument("--with-cex", action="store_true")
    p.add_argument("--refresh", type=float, default=2.0)
    p.add_argument("--lighter-market", type=int, default=None)
    p.add_argument("--conv-open", type=float, default=8.0,
                   help="收敛策略: |Mid价差|≥此值(bps)虚拟开仓")
    p.add_argument("--conv-close", type=float, default=3.0,
                   help="收敛策略: |Mid价差|≤此值(bps)虚拟平仓")
    return p.parse_args()


async def run(args):
    cex_list = ["binance", "bybit", "okx", "gate"]
    if args.exchanges:
        exchange_names = [x.strip().lower() for x in args.exchanges.split(",")]
    elif args.all:
        exchange_names = list(FEED_REGISTRY_SPEC.keys())
    elif args.with_cex:
        exchange_names = DEFAULT_DEX_EXCHANGES + cex_list
    else:
        exchange_names = DEFAULT_DEX_EXCHANGES

    eng = ScannerEngine(
        symbol=args.symbol.upper(),
        trade_size_usd=args.size,
        exchange_names=exchange_names,
        lighter_market=args.lighter_market,
        conv_open_bps=args.conv_open,
        conv_close_bps=args.conv_close,
    )
    _state["engine"] = eng
    await eng.start()

    await asyncio.sleep(2)

    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/ws", handle_ws)
    app.router.add_get("/api/data", handle_api_data)

    broadcast_task = asyncio.create_task(broadcast_loop(args.refresh))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, args.host, args.port)
    await site.start()

    logger.info("=" * 60)
    logger.info("  Web 扫描器已启动: http://localhost:%d", args.port)
    logger.info("=" * 60)
    print(f"\n  Web 扫描器已启动!")
    print(f"  在浏览器中打开: http://localhost:{args.port}")
    print(f"  按 Ctrl+C 停止\n")

    stop = asyncio.Event()

    def on_signal():
        stop.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, on_signal)
        except NotImplementedError:
            pass

    try:
        await stop.wait()
    finally:
        broadcast_task.cancel()
        await eng.stop()
        await runner.cleanup()


def main():
    args = parse_args()
    print(f"\n  启动 Web 价差扫描器: {args.symbol.upper()} / ${args.size:,.0f}")
    print(f"  端口: {args.port}  |  交易所: {args.exchanges or '默认DEX列表'}")
    print(f"  收敛策略: 开仓≥{args.conv_open}bps / 平仓≤{args.conv_close}bps (页面可改)\n")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\n  扫描器已停止。")


if __name__ == "__main__":
    main()
