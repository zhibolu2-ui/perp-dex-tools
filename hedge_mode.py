#!/usr/bin/env python3
"""
Hedge Mode Entry Point

This script serves as the main entry point for hedge mode trading.
It imports and runs the appropriate hedge mode implementation based on the exchange parameter.

Usage:
    python hedge_mode.py --exchange <exchange> [other arguments]

Supported exchanges:
    - backpack: Uses HedgeBot from hedge_mode_bp.py (Backpack + Lighter)
    - extended: Uses HedgeBot from hedge_mode_ext.py (Extended maker + Lighter taker)
    - extended_v2: Uses HedgeBot from hedge_mode_ext_v2.py (Lighter maker + Extended hedge)
    - apex: Uses HedgeBot from hedge_mode_apex.py (Apex + Lighter)
    - grvt: Uses HedgeBot from hedge_mode_grvt.py (GRVT + Lighter)
      Use --v2 flag to use hedge_mode_grvt_v2.py instead
    - edgex: Uses HedgeBot from hedge_mode_edgex.py (edgeX + Lighter)
    - nado: Uses HedgeBot from hedge_mode_nado.py (Nado + Lighter)
    - standx: Uses HedgeBot from hedge_mode_standx.py (StandX + Lighter)
    - o1: Uses HedgeBot from hedge_mode_01.py (01 Exchange maker + Lighter taker)

Cross-platform compatibility:
    - Works on Linux, macOS, and Windows
    - Direct imports instead of subprocess calls for better performance
"""

import asyncio
import sys
import argparse
from decimal import Decimal
from pathlib import Path
import dotenv

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Hedge Mode Trading Bot Entry Point',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python hedge_mode.py --exchange backpack --ticker BTC --size 0.002 --iter 10
    python hedge_mode.py --exchange extended --ticker ETH --size 0.1 --iter 5
    python hedge_mode.py --exchange extended_v2 --ticker ETH --size 0.1 --iter 1 --max-position 0.5
    python hedge_mode.py --exchange apex --ticker BTC --size 0.002 --iter 10
    python hedge_mode.py --exchange grvt --ticker BTC --size 0.05 --iter 10 --max-position 0.1
    python hedge_mode.py --exchange grvt --v2 --ticker BTC --size 0.05 --iter 10 --max-position 0.1
    python hedge_mode.py --exchange edgex --ticker BTC --size 0.001 --iter 20
    python hedge_mode.py --exchange nado --ticker BTC --size 0.003 --iter 20 --max-position 0.05
    python hedge_mode.py --exchange standx --ticker BTC --size 0.003 --iter 20 --max-position 0.05
    python hedge_mode.py --exchange o1 --ticker ETH --size 0.05 --iter 1 --max-position 1 --open-buffer 0.0005 --close-buffer 0
        """
    )
    
    parser.add_argument('--exchange', type=str, required=True,
                        help='Exchange to use (backpack, extended, extended_v2, apex, grvt, edgex, nado, standx, o1)')
    parser.add_argument('--ticker', type=str, default='BTC',
                        help='Ticker symbol (default: BTC)')
    parser.add_argument('--size', type=str, required=True,
                        help='Number of tokens to buy/sell per order')
    parser.add_argument('--iter', type=int, required=True,
                        help='Number of iterations to run')
    parser.add_argument('--fill-timeout', type=int, default=5,
                        help='Timeout in seconds for maker order fills (default: 5)')
    parser.add_argument('--sleep', type=int, default=0,
                        help='Sleep time in seconds after each step (default: 0)')
    parser.add_argument('--env-file', type=str, default=".env",
                        help=".env file path (default: .env)")
    parser.add_argument('--max-position', type=Decimal, default=Decimal('0'),
                        help='Maximum position to hold (default: 0)')
    parser.add_argument('--safety-buffer', '--open-buffer', type=Decimal, default=Decimal('0.0008'),
                        dest='open_buffer',
                        help='Open buffer as fraction of price (default: 0.0008 = 0.08%%)')
    parser.add_argument('--close-buffer', type=Decimal, default=Decimal('-0.0003'),
                        help='Close buffer as fraction of price, negative = accept loss on close (default: -0.0003)')
    parser.add_argument('--max-hold-minutes', type=int, default=30,
                        help='Max hold time before close_buffer decays to 0 (default: 30)')
    parser.add_argument('--close-decay-mult', type=Decimal, default=Decimal('2.5'),
                        help='Close buffer decay target multiplier (default: 2.5, decay_target = -open_buffer * mult)')
    parser.add_argument('--ema-window', type=int, default=100,
                        help='Premium EMA window for open/close timing (default: 100)')
    parser.add_argument('--v2', action='store_true',
                        help='Use v2 implementation (currently only supported for grvt exchange)')
    
    return parser.parse_args()


def validate_exchange(exchange):
    """Validate that the exchange is supported."""
    supported_exchanges = ['backpack', 'extended', 'extended_v2', 'apex', 'grvt', 'edgex', 'nado', 'standx', 'o1']
    if exchange.lower() not in supported_exchanges:
        print(f"Error: Unsupported exchange '{exchange}'")
        print(f"Supported exchanges: {', '.join(supported_exchanges)}")
        sys.exit(1)


def get_hedge_bot_class(exchange, v2=False):
    """Import and return the appropriate HedgeBot class."""
    try:
        if exchange.lower() == 'backpack':
            from hedge.hedge_mode_bp import HedgeBot
            return HedgeBot
        elif exchange.lower() == 'extended':
            from hedge.hedge_mode_ext import HedgeBot
            return HedgeBot
        elif exchange.lower() == 'extended_v2':
            from hedge.hedge_mode_ext_v2 import HedgeBot
            return HedgeBot
        elif exchange.lower() == 'apex':
            from hedge.hedge_mode_apex import HedgeBot
            return HedgeBot
        elif exchange.lower() == 'grvt':
            if v2:
                from hedge.hedge_mode_grvt_v2 import HedgeBot
            else:
                from hedge.hedge_mode_grvt import HedgeBot
            return HedgeBot
        elif exchange.lower() == 'edgex':
            from hedge.hedge_mode_edgex import HedgeBot
            return HedgeBot
        elif exchange.lower() == 'nado':
            from hedge.hedge_mode_nado import HedgeBot
            return HedgeBot
        elif exchange.lower() == 'standx':
            from hedge.hedge_mode_standx import HedgeBot
            return HedgeBot
        elif exchange.lower() == 'o1':
            from hedge.hedge_mode_01 import HedgeBot
            return HedgeBot
        else:
            raise ValueError(f"Unsupported exchange: {exchange}")
    except ImportError as e:
        print(f"Error importing hedge mode implementation: {e}")
        sys.exit(1)


async def main():
    """Main entry point that creates and runs the appropriate hedge bot."""
    args = parse_arguments()

    env_path = Path(args.env_file)
    if not env_path.exists():
        print(f"Env file not find: {env_path.resolve()}")
        sys.exit(1)
    dotenv.load_dotenv(args.env_file)
    
    # Validate exchange
    validate_exchange(args.exchange)
    
    # Validate v2 flag usage
    if args.v2 and args.exchange.lower() not in ['grvt']:
        print(f"Error: --v2 flag is only supported for grvt exchange. "
              f"For Extended v2, use --exchange extended_v2")
        sys.exit(1)
    
    # Get the appropriate HedgeBot class
    try:
        HedgeBotClass = get_hedge_bot_class(args.exchange, v2=args.v2)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    version_str = " v2" if args.v2 else ""
    print(f"启动对冲模式: {args.exchange} 交易所{version_str}...")
    print(f"币种: {args.ticker}, 数量: {args.size}, 迭代: {args.iter}")
    print("-" * 50)
    
    try:
        # v2 / o1 bots have different constructor signature (no iterations/sleep_time)
        if args.exchange.lower() == 'o1':
            bot = HedgeBotClass(
                ticker=args.ticker.upper(),
                order_quantity=Decimal(args.size),
                fill_timeout=args.fill_timeout,
                max_position=args.max_position,
                safety_buffer=args.open_buffer,
                close_buffer=args.close_buffer,
                max_hold_minutes=args.max_hold_minutes,
                close_decay_multiplier=args.close_decay_mult,
                ema_window=args.ema_window,
            )
        elif args.exchange.lower() == 'extended_v2':
            bot = HedgeBotClass(
                ticker=args.ticker.upper(),
                order_quantity=Decimal(args.size),
                fill_timeout=args.fill_timeout,
                max_position=args.max_position,
                safety_buffer=args.open_buffer,
                close_buffer=args.close_buffer,
                max_hold_minutes=args.max_hold_minutes,
            )
        elif args.v2 and args.exchange.lower() == 'grvt':
            bot = HedgeBotClass(
                ticker=args.ticker.upper(),
                order_quantity=Decimal(args.size),
                fill_timeout=args.fill_timeout,
                max_position=args.max_position
            )
        elif args.exchange in ['backpack', 'edgex', 'nado', 'grvt', 'standx']:
            bot = HedgeBotClass(
                ticker=args.ticker.upper(),
                order_quantity=Decimal(args.size),
                fill_timeout=args.fill_timeout,
                iterations=args.iter,
                sleep_time=args.sleep,
                max_position=args.max_position
            )
        else:
            bot = HedgeBotClass(
                ticker=args.ticker.upper(),
                order_quantity=Decimal(args.size),
                fill_timeout=args.fill_timeout,
                iterations=args.iter,
                sleep_time=args.sleep
            )
        
        # Run the bot
        await bot.run()
        
    except KeyboardInterrupt:
        print("\n用户中断对冲模式")
        return 1
    except Exception as e:
        print(f"运行对冲模式出错: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))