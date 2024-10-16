import ccxt
import logging
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
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
        time.sleep(30)
        return fetch_ohlcv(exchange, symbol, timeframe, since, limit)
    except ccxt.ExchangeError as e:
        logging.error(f"Exchange error: {e}. Retrying...")
        return None

def save_to_csv(data, save_path):
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.to_csv(save_path, index=False, mode='a', header=not os.path.exists(save_path))

def fetch_and_save_data(symbol, save_path):
    exchange = ccxt.binanceusdm()
    timeframe = '1m'
    limit = 1500
    full_data = []

    if os.path.exists(save_path):
        df = pd.read_csv(save_path)
        last_timestamp = pd.to_datetime(df['timestamp']).max()
        start_time = int(last_timestamp.timestamp() * 1000) + 60000  # 1분 더하기
    else:
        start_time = exchange.parse8601('2021-01-01T00:00:00Z')  # 시작 날짜를 설정
    end_time = int(datetime.now(timezone.utc).timestamp() * 1000)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        current_time = start_time

        while current_time < end_time:
            future = executor.submit(fetch_ohlcv, exchange, symbol, timeframe, current_time, limit)
            futures.append(future)
            current_time += limit * 60 * 1000
            time.sleep((exchange.rateLimit * 2) / 1000)

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

# # 디렉토리 내의 모든 CSV 파일을 찾기
# for filename in os.listdir(save_directory):
#     if filename.endswith(".csv"):
#         symbol = filename.replace("_1m.csv", "").replace("USDT", "/USDT")
#         save_path = os.path.join(save_directory, filename)
#         logging.info(f"Updating data for {symbol}...")
#         fetch_and_save_data(symbol, save_path)

fetch_and_save_data("NEO/USDT",os.path.join(save_directory,"NEOUSDT_1m.csv"))
fetch_and_save_data("ONT/USDT",os.path.join(save_directory,"ONTUSDT_1m.csv"))