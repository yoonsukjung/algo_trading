# main.py

import asyncio
import logging
from typing import List

from trader import BinanceTrader
from data_handler import BinanceDataHandler
from config import BINANCE_API_KEY, BINANCE_API_SECRET, ENVIRONMENT

# 로깅 설정 (필요 시 재설정 가능)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


async def main():
    symbols = ['LDOUSDT', 'AAVEUSDT']  # 거래할 심볼 리스트

    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        logging.error("Binance API 키가 설정되지 않았습니다. config.py를 확인하세요.")
        return

    trader = BinanceTrader(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET, symbols=symbols)
    handler = BinanceDataHandler(symbols, trader=trader)
    await handler.run()


if __name__ == "__main__":
    asyncio.run(main())