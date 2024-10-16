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
    symbols = ['NEOUSDT', 'ONTUSDT']  # 거래할 심볼 리스트

    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        logging.error("Binance API 키가 설정되지 않았습니다. config.py를 확인하세요.")
        return

    trader = BinanceTrader(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET, symbols=symbols)
    handler = BinanceDataHandler(symbols,hr= 0.96, trader=trader)
    await handler.run()


if __name__ == "__main__":
    asyncio.run(main())

#
# if __name__ == "__main__":
#     # Trader 초기화
#     trader = Trader(client)
#
#     # TradingBot 초기화
#     symbols = ['BTCUSDT', 'ETHUSDT']  # 거래할 심볼 설정
#     bot = TradingBot(trader, symbols, stop_loss_threshold=3.0)
#
#
#     # AR(1) 모델을 사용하여 z_score 시계열 생성
#     z_scores = [
#         0.0, 1.6, 1.8, 4.2, 1.7, 1.9, 0.1, -1.6, -1.7, -0.15,
#         -4.1, -1.6, -4.5, -1.8, 0.0, -2.0, -2.5, -4.3, 1.9, 1.7,
#         0.0, 2.1, 2.3, 0.1, -3.0, -4.5, -5.0, 0.0, 3.2, 0.1
#     ]
#
#     # 현재 UTC 시간 가져오기 (시간대 설정 포함)
#     now = pd.Timestamp.now(tz='UTC')
#
#     # 초와 마이크로초를 0으로 설정
#     next_minute = now.replace(second=0, microsecond=0)
#
#     # 현재 시간이 이미 00초라면 그대로 사용, 아니면 분을 1 증가시킴
#     if now.second != 0 or now.microsecond != 0:
#         next_minute += pd.Timedelta(minutes=1)
#
#     # num_steps 만큼의 timestamps 생성 (모두 UTC 시간대)
#     num_steps = 30
#     timestamps = [next_minute + pd.Timedelta(minutes=i) for i in range(num_steps)]
#
#     for ts, z in zip(timestamps, z_scores):
#         # 현재 시각이 ts와 일치할 때까지 기다림
#         while True:
#             current_now = pd.Timestamp.now(tz='UTC')  # timezone-aware
#             if current_now >= ts:
#                 break
#             else:
#                 # 남은 시간만큼 대기
#                 time_to_wait = (ts - current_now).total_seconds()
#                 # 너무 짧은 시간에 대한 예외 처리
#                 if time_to_wait > 0:
#                     # 대기가 너무 길지 않도록 최대 0.5초 단위로 대기
#                     time.sleep(min(time_to_wait, 0.5))  # 최대 0.5초 단위로 대기
#                 else:
#                     # 만약 현재 시각이 ts보다 약간 앞서 있다면 바로 진행
#                     break
#
#         # 거래 로직 실행
#         bot.execute_trading_logic(ts, z)