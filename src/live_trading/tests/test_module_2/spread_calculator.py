import asyncio
import json
import websockets
import numpy as np
from collections import deque
from config_test import api_key, api_secret, base_url

class SpreadCalculator:
    def __init__(self, pair_a, pair_b, HR, window_size=1000):
        self.pair_a = pair_a
        self.pair_b = pair_b
        self.HR = HR
        self.window_size = window_size
        self.closing_prices_a = deque(maxlen=window_size)
        self.closing_prices_b = deque(maxlen=window_size)
        self.spreads = deque(maxlen=window_size)
        self.mean_spread = None
        self.std_spread = None


    async def fetch_15m_data(self):
        url_a = f"wss://stream.binancefuture.com/ws/{self.pair_a}@kline_15m"
        url_b = f"wss://stream.binancefuture.com/ws/{self.pair_b}@kline_15m"

        async with websockets.connect(url_a) as ws_a, websockets.connect(url_b) as ws_b:
            while True:
                response_a = await ws_a.recv()
                response_b = await ws_b.recv()

                data_a = json.loads(response_a)
                data_b = json.loads(response_b)

                close_price_a = float(data_a['k']['c'])
                close_price_b = float(data_b['k']['c'])

                self.closing_prices_a.append(close_price_a)
                self.closing_prices_b.append(close_price_b)

                if len(self.closing_prices_a) == self.window_size and len(self.closing_prices_b) == self.window_size:
                    self.calculate_spread()
                    self.calculate_rolling_stats()

    def calculate_spread(self):
        spread = np.array(self.closing_prices_a) - np.array(self.closing_prices_b)
        self.spreads.append(spread[-1])

    def calculate_rolling_stats(self):
        if len(self.spreads) == self.window_size:
            self.mean_spread = np.mean(self.spreads)
            self.std_spread = np.std(self.spreads)