import ccxt
import logging
import time
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_ohlcv(exchange, symbol, timeframe, since, limit):
    try:
        logging.info(f"Fetching data from {datetime.utcfromtimestamp(since / 1000).strftime('%Y-%m-%d %H:%M:%S')}...")
        data = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
        if not data:
            logging.warning(f"No data fetched for {datetime.utcfromtimestamp(since / 1000).strftime('%Y-%m-%d %H:%M:%S')}")
        return data
    except ccxt.NetworkError as e:
        logging.error(f"Network error: {e}. Retrying...")
        time.sleep(30)  # 더 긴 대기 시간
        return fetch_ohlcv(exchange, symbol, timeframe, since, limit)
    except ccxt.ExchangeError as e:
        logging.error(f"Exchange error: {e}. Retrying...")
        return None

def save_to_csv(data, file_path):
    df = pd.DataFrame(data, columns=[
        "timestamp", "open", "high", "low", "close", "volume"
    ])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

    # 1분 간격의 완전한 타임스탬프 생성
    full_time_range = pd.date_range(start=df['timestamp'].min(), end=df['timestamp'].max(), freq='1T')

    # 누락된 타임스탬프 채우기
    df = df.set_index('timestamp').reindex(full_time_range)

    # 채우는 방법 선택
    df.fillna(method='ffill', inplace=True)  # Forward fill (이전 값으로 채우기)
    df.reset_index(inplace=True)
    df.columns = ["timestamp", "open", "high", "low", "close", "volume"]

    # 최종적으로 저장
    df.to_csv(file_path, index=False)

def fetch_and_save_data(symbol, start_date, end_date, save_path):
    exchange = ccxt.binanceusdm()
    timeframe = '1m'
    limit = 1500  # 바이낸스 선물 시장의 최대 limit
    start_time = exchange.parse8601(start_date.isoformat())
    end_time = exchange.parse8601(end_date.isoformat())
    full_data = []

    # 병렬 처리를 위한 ThreadPoolExecutor 생성
    with ThreadPoolExecutor(max_workers=2) as executor:  # max_workers를 줄여서 호출 빈도 감소
        futures = []
        current_time = start_time

        while current_time < end_time:
            data = fetch_ohlcv(exchange, symbol, timeframe, current_time, limit)
            if data is None:
                logging.error("Error fetching data. Exiting loop.")
                return None

            future = executor.submit(fetch_ohlcv, exchange, symbol, timeframe, current_time, limit)
            futures.append(future)
            current_time += limit * 60 * 1000  # 1분 간격으로 limit 만큼의 데이터

            # Rate limiting: 요청 간의 간격을 조절
            time.sleep((exchange.rateLimit * 2) / 1000)  # rateLimit을 두 배로 늘려서 호출 빈도 감소

        for future in as_completed(futures):
            data = future.result()
            if data:
                full_data.extend(data)
                logging.info(f"Fetched {len(data)} records. Total records: {len(full_data)}")
            else:
                logging.warning("No data fetched in this batch.")

    if full_data:
        save_to_csv(full_data, save_path)
        logging.info(f"Data saved to {save_path}")

save_directory = "/Users/yoonsukjung/Desktop/data/futures/1m"

# Futures Market exchangeInfo 엔드포인트
url = "https://fapi.binance.com/fapi/v1/exchangeInfo"

response = requests.get(url)
data = response.json()

# USDT 페어 심볼 필터링
usdt_pairs = [symbol['symbol'] for symbol in data['symbols'] if symbol['quoteAsset'] == 'USDT']
logging.info(f"USDT 페어 심볼 수: {len(usdt_pairs)}")

print('LDOUSDT' in usdt_pairs)


# 데이터 수집 기간 설정
start_date = datetime(2023, 1, 1, 0, 0)
end_date = datetime(2024, 8, 27, 23, 59)
#
# 각 코인에 대해 데이터 수집 및 저장
for coin in usdt_pairs:
    save_path = os.path.join(save_directory, f"{coin}_1m.csv")
    logging.info(f"{coin} 데이터 수집 시작")

    if os.path.exists(save_path):
        logging.info(f"{coin} 데이터가 이미 존재합니다: {save_path}")
        continue

    try:
        fetch_and_save_data(coin, start_date, end_date, save_path)
        logging.info(f"{coin} 데이터 저장 완료: {save_path}")
    except Exception as e:
        logging.error(f"{coin} 데이터 수집 실패: {e}")

# fetch_and_save_data('LDOUSDT',start_date,end_date,os.path.join(save_directory,"LDOUSDT_1m.csv"))