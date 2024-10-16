# data_handler.py

import pandas as pd
import numpy as np
import asyncio
import websockets
import requests
from datetime import datetime, timezone
import json
import logging
from typing import List
import time
from collections import deque
import os

class BinanceDataHandler:
    def __init__(self, symbols: List[str], hr: float = 0.71):
        self.symbols = symbols
        self.hr = hr
        self.df = pd.DataFrame()
        self.processed_timestamps = deque(maxlen=100000)  # Track (open_time, symbol)
        logging.info("심볼: %s 및 hr: %.2f와 함께 BinanceDataHandler 초기화 중", symbols, hr)
        # 데이터 로드 및 누락된 데이터 채우기
        self.load_data()
        self.fill_missing()

        # Stop Loss Threshold 설정
        self.STOP_LOSS_THRESHOLD = 4.0  # 예: z-score가 3.0 이상 또는 -3.0 이하일 때

    def load_data(self):
        logging.info("CSV 파일에서 데이터 로드 중")
        for symbol in self.symbols:
            file_path = f'/Users/yoonsukjung/Desktop/data/futures/1m/{symbol}_1m.csv'
            logging.info("심볼: %s의 데이터를 파일: %s에서 읽는 중", symbol, file_path)
            try:
                data = pd.read_csv(file_path, usecols=['timestamp', 'close'])

                # timestamp가 문자열 형식인 경우 변환
                data['timestamp'] = pd.to_datetime(data['timestamp'], utc=True)

                data.set_index('timestamp', inplace=True)
                data = data[~data.index.duplicated(keep='last')]  # 중복 인덱스 제거
                data = data.resample('1min').ffill()  # 1분 간격으로 리샘플링 및 전방 채우기
                if self.df.empty:
                    self.df = data.rename(columns={'close': symbol})
                else:
                    self.df = self.df.join(data.rename(columns={'close': symbol}), how='outer')
            except Exception as e:
                logging.error("심볼 %s의 데이터 로드 중 오류 발생: %s", symbol, e)

        # 초기 스프레드 계산
        if all(symbol in self.df.columns for symbol in self.symbols):
            self.df['spread'] = self.df[self.symbols[0]] - self.df[self.symbols[1]] * self.hr
            logging.info("초기 스프레드가 성공적으로 계산되었습니다")
        else:
            logging.warning("데이터에 모든 심볼이 존재하지 않아 스프레드를 계산할 수 없습니다.")

    def fill_missing(self):
        logging.info("누락된 데이터 채우기 시작")
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        end_time = pd.Timestamp(now)
        start_time = end_time - pd.Timedelta(minutes=40000)
        all_timestamps = pd.date_range(start=start_time, end=end_time, freq='min', tz=timezone.utc)

        missing_timestamps = all_timestamps.difference(self.df.index)
        if missing_timestamps.empty:
            logging.info("누락된 타임스탬프가 없습니다")
            return

        logging.info("누락된 타임스탬프 %d개를 발견했습니다", len(missing_timestamps))

        # 연속된 범위로 누락된 타임스탬프 그룹화
        missing_groups = []
        current_group = []
        previous = None
        for timestamp in missing_timestamps:
            if previous is None or (timestamp - previous) == pd.Timedelta(minutes=1):
                current_group.append(timestamp)
            else:
                missing_groups.append(current_group)
                current_group = [timestamp]
            previous = timestamp
        if current_group:
            missing_groups.append(current_group)

        # 특정 시간 범위에 대해 심볼의 klines를 가져오는 함수 with 재시도 로직
        def fetch_klines(symbol, start_ts, end_ts, limit=1000, max_retries=5):
            url = 'https://api.binance.com/api/v3/klines'
            params = {
                'symbol': symbol,
                'interval': '1m',
                'startTime': int(start_ts.timestamp() * 1000),
                'endTime': int(end_ts.timestamp() * 1000),
                'limit': limit
            }
            retries = 0
            wait_time = 1  # 초기 대기 시간 (초)
            while retries < max_retries:
                try:
                    response = requests.get(url, params=params, timeout=10)
                    response.raise_for_status()
                    return response.json()
                except Exception as e:
                    retries += 1
                    logging.error("심볼 %s의 klines 가져오기 중 오류 발생 (시도 %d/%d): %s", symbol, retries, max_retries, e)
                    if retries < max_retries:
                        logging.info("다시 시도하기 전에 %d초 대기 중...", wait_time)
                        time.sleep(wait_time)
                        wait_time *= 2  # 지수 백오프
                    else:
                        logging.error("심볼 %s의 klines 가져오기에 실패하였습니다. 최대 재시도 횟수 초과.", symbol)
                        return []

        for group in missing_groups:
            group_start = group[0]
            group_end = group[-1]
            # Binance API는 최대 1000개의 데이터 포인트를 한 번에 요청할 수 있음
            total_minutes = len(group)
            batch_size = 1000
            for i in range(0, total_minutes, batch_size):
                batch_group = group[i:i + batch_size]
                batch_start = batch_group[0]
                batch_end = batch_group[-1]
                for symbol in self.symbols:
                    klines = fetch_klines(symbol, batch_start, batch_end)
                    if not klines:
                        continue
                    for kline in klines:
                        open_time = pd.to_datetime(kline[0], unit='ms', utc=True)
                        close_price = float(kline[4])
                        self.df.at[open_time, symbol] = close_price
                        logging.info("심볼: %s의 타임스탬프: %s에서 누락된 데이터 채움", symbol, open_time)
                # 배치 채움 후 스프레드 업데이트
                if all(sym in self.df.columns for sym in self.symbols):
                    for symbol in self.symbols:
                        klines = fetch_klines(symbol, batch_start, batch_end)
                        batch_filled_times = [pd.to_datetime(k[0], unit='ms', utc=True) for k in klines] if klines else []
                        for timestamp in batch_filled_times:
                            spread = self.df.at[timestamp, self.symbols[0]] - self.df.at[
                                timestamp, self.symbols[1]] * self.hr
                            self.df.at[timestamp, 'spread'] = spread
                # API 레이트 리밋을 준수하기 위해 짧은 대기 시간 추가 가능
                # time.sleep(0.5)

        # 채운 후, DataFrame 정렬
        self.df = self.df.sort_index()

        logging.info("누락된 데이터가 성공적으로 채워졌습니다")

    async def fetch_generate(self):
        logging.info("WebSocket 연결 시작")
        streams = '/'.join([f'{symbol.lower()}@kline_1m' for symbol in self.symbols])
        url = f"wss://stream.binance.com:9443/stream?streams={streams}"

        while True:
            try:
                async with websockets.connect(url) as websocket:
                    logging.info("WebSocket 스트림에 연결됨: %s", streams)

                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            if 'data' not in data or 'k' not in data['data']:
                                continue

                            k = data['data']['k']
                            symbol = k['s']
                            close_price = float(k['c'])
                            open_time = pd.to_datetime(k['t'], unit='ms', utc=True).floor('min')
                            is_kline_closed = k['x']  # 클라인 종료 여부

                            # Check if this specific (open_time, symbol) has been processed
                            if (open_time, symbol) in self.processed_timestamps:
                                logging.debug("이미 처리된 타임스탬프 및 심볼: %s, %s. 건너뜀.", open_time, symbol)
                                continue

                            if not is_kline_closed:
                                # 클라인이 종료되지 않은 경우, 실시간 가격 업데이트만 수행
                                self.df.at[open_time, symbol] = close_price
                                logging.debug("실시간 업데이트: 심볼: %s, 타임스탬프: %s, 가격: %.5f", symbol, open_time, close_price)
                                continue  # 클라인이 종료되지 않았으므로 추가 처리하지 않음

                            # 클라인이 종료된 경우 데이터 처리
                            if open_time not in self.df.index:
                                self.df.loc[open_time] = [np.nan] * len(self.df.columns)

                            self.df.at[open_time, symbol] = close_price

                            # 두 심볼 모두 데이터가 있는 경우 스프레드 업데이트
                            if all(sym in self.df.columns for sym in self.symbols):
                                spread = self.df.at[open_time, self.symbols[0]] - self.df.at[
                                    open_time, self.symbols[1]] * self.hr
                                self.df.at[open_time, 'spread'] = spread

                                # 롤링 통계 업데이트
                                window = min(40000, len(self.df))
                                if window > 0:
                                    rolling_mean = self.df['spread'].rolling(window=window, min_periods=1).mean().iloc[-1]
                                    rolling_std = self.df['spread'].rolling(window=window, min_periods=1).std().iloc[-1]
                                    z_score = (spread - rolling_mean) / rolling_std if rolling_std != 0 else 0
                                    self.df.at[open_time, 'rolling_mean'] = rolling_mean
                                    self.df.at[open_time, 'rolling_std'] = rolling_std
                                    self.df.at[open_time, 'z_score'] = z_score

                                logging.info("심볼: %s의 타임스탬프: %s에서 데이터 업데이트됨", symbol, open_time)
                                logging.debug("스프레드: %.5f, 롤링 평균: %.5f, 롤링 표준편차: %.5f",
                                              spread, rolling_mean, rolling_std)
                                logging.info("z score: %.5f", z_score)

                                # 매매 로직 실행
                                if self.trader:
                                    self.execute_trading_logic(open_time, z_score)

                            # 처리된 타임스탬프와 심볼을 기록하여 중복 처리를 방지
                            self.processed_timestamps.append((open_time, symbol))
                        except Exception as e:
                            logging.error("WebSocket 메시지 처리 중 오류 발생: %s", e)
            except Exception as e:
                logging.error("WebSocket 연결 오류: %s", e)
                logging.info("WebSocket 다시 연결 시도 중...")
                await asyncio.sleep(5)  # 재접속 전에 5초 대기
