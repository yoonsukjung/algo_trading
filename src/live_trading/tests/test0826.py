import pandas as pd
import numpy as np
import asyncio
import websockets
import requests
from datetime import datetime
import json
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BinanceDataHandler:
    def __init__(self, symbols, hr=0/71):
        self.symbols = symbols
        self.hr = hr
        self.df = pd.DataFrame()
        logging.info("Initializing BinanceDataHandler with symbols: %s and hr: %d", symbols, hr)
        self.load_data()

    def load_data(self):
        logging.info("Loading data from CSV files")
        for symbol in self.symbols:
            file_path = f'/Users/yoonsukjung/Desktop/data/futures/1m/{symbol}_1m.csv'
            logging.info("Reading data for symbol: %s from file: %s", symbol, file_path)
            data = pd.read_csv(file_path, usecols=['timestamp', 'close'])
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            data.set_index('timestamp', inplace=True)
            data = data[-40000:]  # 최근 40000개의 데이터만 사용
            if self.df.empty:
                self.df = data.rename(columns={'close': symbol})
            else:
                self.df = self.df.join(data.rename(columns={'close': symbol}), how='outer')

        self.df['spread'] = self.df[self.symbols[0]] - self.df[self.symbols[1]] * self.hr
        logging.info("Data loaded successfully")

    def fill_missing(self):
        logging.info("Filling missing data")
        now = datetime.utcnow()
        end_time = now.replace(second=0, microsecond=0)
        end_time = pd.Timestamp(end_time)
        start_time = end_time - pd.Timedelta(minutes=40000)
        all_timestamps = pd.date_range(start=start_time, end=end_time, freq='min')

        missing_timestamps = all_timestamps.difference(self.df.index)
        print(missing_timestamps)
        for timestamp in missing_timestamps:
            print(timestamp)
            timestamp_ms = int(timestamp.timestamp() * 1000)

            # 첫 번째 심볼에 대한 요청
            url1 = f'https://api.binance.com/api/v3/klines?symbol={self.symbols[0]}&interval=1m&startTime={timestamp_ms}&endTime={timestamp_ms + 60000}'
            response1 = requests.get(url1)
            if response1.status_code == 200:
                data1 = response1.json()
                if data1:
                    close_price1 = float(data1[0][4])
                    self.df.at[timestamp, self.symbols[0]] = close_price1
                    logging.info("Filled missing data for symbol: %s at timestamp: %s", self.symbols[0], timestamp)
            else:
                logging.warning("Failed to fetch data for symbol: %s at timestamp: %s", self.symbols[0], timestamp)

            # 두 번째 심볼에 대한 요청
            url2 = f'https://api.binance.com/api/v3/klines?symbol={self.symbols[1]}&interval=1m&startTime={timestamp_ms}&endTime={timestamp_ms + 60000}'
            response2 = requests.get(url2)
            if response2.status_code == 200:
                data2 = response2.json()
                if data2:
                    close_price2 = float(data2[0][4])
                    self.df.at[timestamp, self.symbols[1]] = close_price2
                    logging.info("Filled missing data for symbol: %s at timestamp: %s", self.symbols[1], timestamp)
            else:
                logging.warning("Failed to fetch data for symbol: %s at timestamp: %s", self.symbols[1], timestamp)

            if self.symbols[0] in self.df.columns and self.symbols[1] in self.df.columns:
                self.df.at[timestamp, 'spread'] = self.df.at[timestamp, self.symbols[0]] - self.df.at[timestamp, self.symbols[1]] * self.hr

        logging.info("Missing data filled successfully")

    async def fetch_generate(self):
        logging.info("Starting WebSocket connection")
        async with websockets.connect('wss://stream.binance.com:9443/ws') as websocket:
            streams = [f'{symbol.lower()}@kline_1m' for symbol in self.symbols]
            await websocket.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": streams,
                "id": 1
            }))
            logging.info("Subscribed to WebSocket streams: %s", streams)

            while True:
                response = await websocket.recv()
                data = json.loads(response)
                if 'k' in data:
                    symbol = data['s']
                    close_price = float(data['k']['c'])
                    timestamp = pd.to_datetime(data['k']['t'], unit='ms')

                    if timestamp not in self.df.index:
                        self.df.loc[timestamp] = [np.nan] * len(self.df.columns)

                    if pd.isna(self.df.at[timestamp, symbol]):
                        self.df.at[timestamp, symbol] = close_price
                        self.df.at[timestamp, 'spread'] = self.df.at[timestamp, self.symbols[0]] - self.df.at[
                            timestamp, self.symbols[1]] * self.hr

                        # rolling_mean 및 rolling_std 계산
                        if len(self.df) >= 40000:
                            self.df.at[timestamp, 'rolling_mean'] = self.df['spread'][-40000:].mean()
                            self.df.at[timestamp, 'rolling_std'] = self.df['spread'][-40000:].std()
                        else:
                            self.df.at[timestamp, 'rolling_mean'] = self.df['spread'].mean()
                            self.df.at[timestamp, 'rolling_std'] = self.df['spread'].std()

                        print(self.df[timestamp])

                        logging.info("Updated data for symbol: %s at timestamp: %s", symbol, timestamp)

async def main():
    symbols = ['LDOUSDT', 'AAVEUSDT']
    handler = BinanceDataHandler(symbols)
    handler.fill_missing()
    await handler.fetch_generate()


if __name__ == "__main__":
    asyncio.run(main())