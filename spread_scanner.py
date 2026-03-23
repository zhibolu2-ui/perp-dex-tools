#!/usr/bin/env python3
"""
跨交易所价差扫描器 (Cross-Exchange Spread Scanner)

实时对比 Lighter 与多个 DEX/CEX 的价格差异、滑点、费用，
自动排名最优对冲套利机会。

用法:
    python spread_scanner.py --symbol ETH
    python spread_scanner.py --symbol BTC --size 50000
    python spread_scanner.py --symbol ETH --exchanges binance,hyperliquid,grvt
    python spread_scanner.py --symbol ETH --all
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
import time

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("scanner")
logger.setLevel(logging.INFO)


def _lazy_imports():
    """Import heavy modules only when actually running the scanner."""
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
    from feeds.dashboard import Dashboard
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
        "Dashboard": Dashboard,
    }


FEED_REGISTRY_SPEC = {
    # name ->  (module_class_key, extra_args)
    # ─── CEX (via ccxt) ───
    "binance":     ("CcxtFeed", ("binance",)),
    "bybit":       ("CcxtFeed", ("bybit",)),
    "okx":         ("CcxtFeed", ("okx",)),
    "gate":        ("CcxtFeed", ("gate",)),
    # ─── DEX ───
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

DEFAULT_EXCHANGES = DEFAULT_DEX_EXCHANGES


def _create_feed(modules, name: str, symbol: str):
    """Create a feed instance by name using the lazy-loaded module dict."""
    spec = FEED_REGISTRY_SPEC.get(name)
    if spec is None:
        return None
    cls_key, extra = spec
    cls = modules[cls_key]
    return cls(symbol, *extra)


def parse_args():
    p = argparse.ArgumentParser(
        description="跨交易所价差扫描器 – Lighter vs All",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python spread_scanner.py --symbol ETH
  python spread_scanner.py --symbol ETH --dex-only
  python spread_scanner.py --symbol BTC --size 50000
  python spread_scanner.py --symbol ETH --exchanges hyperliquid,aevo,dydx
  python spread_scanner.py --symbol ETH --all

可用交易所 (默认只启动DEX):
  DEX:  hyperliquid, aevo, dydx, paradex, vertex, orderly,
        rabbitx, bluefin, zeta, 01, grvt, edgex, backpack, apex, extended
  CEX:  binance, bybit, okx, gate
""",
    )
    p.add_argument("--symbol", "-s", default="ETH",
                   help="交易币种 (默认: ETH)")
    p.add_argument("--size", type=float, default=10_000.0,
                   help="模拟交易金额 USD (默认: 10000)")
    p.add_argument("--exchanges", "-e", default=None,
                   help="逗号分隔的交易所列表 (默认: 全部主要交易所)")
    p.add_argument("--all", action="store_true",
                   help="启用所有已注册的交易所 (DEX + CEX)")
    p.add_argument("--dex-only", action="store_true", default=True,
                   help="只启用 DEX 交易所 (默认)")
    p.add_argument("--with-cex", action="store_true",
                   help="同时启用 CEX 交易所 (Binance/Bybit/OKX/Gate)")
    p.add_argument("--refresh", type=float, default=2.0,
                   help="刷新间隔秒数 (默认: 2)")
    p.add_argument("--no-funding", action="store_true",
                   help="禁用资金费率监控")
    p.add_argument("--no-csv", action="store_true",
                   help="禁用 CSV 日志")
    p.add_argument("--lighter-market", type=int, default=None,
                   help="Lighter market index 覆盖 (默认: 自动检测)")
    p.add_argument("--list", action="store_true",
                   help="列出所有可用交易所并退出")
    return p.parse_args()


def list_exchanges():
    print("\n可用交易所:")
    print(f"  {'名称':<16} {'类型':<8} {'链/平台':<12} {'Maker(bps)':<12} {'Taker(bps)':<12}")
    print(f"  {'─' * 62}")

    all_feeds = {
        # CEX
        "binance":     ("CEX", "─",        2.0, 4.0),
        "bybit":       ("CEX", "─",        2.0, 5.5),
        "okx":         ("CEX", "─",        2.0, 5.0),
        "gate":        ("CEX", "─",        1.5, 5.0),
        # DEX
        "hyperliquid": ("DEX", "HyperEVM", 1.5, 4.5),
        "aevo":        ("DEX", "Ethereum",  2.0, 5.0),
        "dydx":        ("DEX", "Cosmos",    1.0, 5.0),
        "paradex":     ("DEX", "StarkNet",  2.0, 4.0),
        "vertex":      ("DEX", "Arbitrum",  0.0, 2.0),
        "orderly":     ("DEX", "Cross-chain", 2.0, 5.0),
        "rabbitx":     ("DEX", "StarkNet",  2.0, 7.0),
        "bluefin":     ("DEX", "Sui",       2.0, 6.0),
        "zeta":        ("DEX", "Solana",    2.0, 5.0),
        "01":          ("DEX", "Solana",    1.0, 5.0),
        "grvt":        ("DEX", "zkSync",   -0.1, 4.5),
        "edgex":       ("DEX", "─",        2.0, 5.0),
        "backpack":    ("DEX", "─",        2.0, 5.0),
        "apex":        ("DEX", "StarkEx",   2.0, 5.0),
        "extended":    ("DEX", "Starknet",  0.0, 2.5),
    }
    for name, (typ, chain, maker, taker) in all_feeds.items():
        print(f"  {name:<16} {typ:<8} {chain:<12} {maker:<12.1f} {taker:<12.1f}")

    print(f"\n  Lighter (参考):  DEX      zkSync       0.0          0.0")
    print(f"\n  共 {len(all_feeds)} 个交易所 ({sum(1 for v in all_feeds.values() if v[0]=='DEX')} DEX + {sum(1 for v in all_feeds.values() if v[0]=='CEX')} CEX)")
    print()


async def run_scanner(args):
    # Lazy-load all heavy modules
    M = _lazy_imports()

    symbol = args.symbol.upper()

    # ── Build feeds ──
    cex_list = ["binance", "bybit", "okx", "gate"]
    if args.exchanges:
        exchange_names = [x.strip().lower() for x in args.exchanges.split(",")]
    elif args.all:
        exchange_names = list(FEED_REGISTRY_SPEC.keys())
    elif args.with_cex:
        exchange_names = DEFAULT_DEX_EXCHANGES + cex_list
    else:
        exchange_names = DEFAULT_DEX_EXCHANGES

    lighter_feed = M["LighterFeed"](symbol, market_index=args.lighter_market)
    target_feeds = []

    for name in exchange_names:
        try:
            feed = _create_feed(M, name, symbol)
            if feed is None:
                logger.warning("未知交易所: %s (跳过)", name)
                continue
            target_feeds.append(feed)
            logger.info("已注册: %s", feed.EXCHANGE_NAME)
        except Exception as exc:
            logger.warning("初始化 %s 失败: %s", name, exc)

    if not target_feeds:
        print("错误: 没有成功初始化任何目标交易所。")
        return

    calculator = M["SpreadCalculator"](lighter_feed, target_feeds, trade_size_usd=args.size)
    dashboard = M["Dashboard"](symbol, csv_enabled=not args.no_csv)

    funding_monitor = None
    if not args.no_funding:
        funding_monitor = M["FundingRateMonitor"](symbol, target_feeds, poll_interval=60.0)

    # ── Launch all feed tasks ──
    tasks = []
    tasks.append(asyncio.create_task(lighter_feed.connect()))
    for feed in target_feeds:
        tasks.append(asyncio.create_task(feed.connect()))
    if funding_monitor:
        tasks.append(asyncio.create_task(funding_monitor.run()))

    logger.info("已启动 %d 个数据源 (Lighter + %d 目标交易所)",
                1 + len(target_feeds), len(target_feeds))
    logger.info("等待数据流建立...")

    # Wait a moment for initial connections
    await asyncio.sleep(3)

    # ── Main render loop ──
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
        while not stop.is_set():
            spreads = calculator.compute_all()
            funding = funding_monitor.get_spreads() if funding_monitor else None
            dashboard.render(spreads, funding=funding, trade_size_usd=args.size)

            try:
                await asyncio.wait_for(stop.wait(), timeout=args.refresh)
            except asyncio.TimeoutError:
                pass
    finally:
        logger.info("正在关闭所有连接...")
        # Disconnect all feeds
        disconnect_tasks = [lighter_feed.disconnect()]
        for feed in target_feeds:
            disconnect_tasks.append(feed.disconnect())
        if funding_monitor:
            disconnect_tasks.append(funding_monitor.stop())
        await asyncio.gather(*disconnect_tasks, return_exceptions=True)

        # Cancel background tasks
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main():
    args = parse_args()

    if args.list:
        list_exchanges()
        sys.exit(0)

    print(f"\n  正在启动价差扫描器: {args.symbol.upper()} / ${args.size:,.0f}")
    print(f"  目标交易所: {args.exchanges or '默认列表'}\n")

    try:
        asyncio.run(run_scanner(args))
    except KeyboardInterrupt:
        print("\n\n  扫描器已停止。")


if __name__ == "__main__":
    main()
