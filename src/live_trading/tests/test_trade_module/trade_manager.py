import asyncio
import json
import websockets
import numpy as np
from collections import deque
from config_test import api_key, api_secret, base_url
from src.live_trading.tests.test_trade_module.spread_calculator import SpreadCalculator


class TradeManager:
    def __init__(self, spread_calculator, entry_threshold=2.0, exit_threshold=0.5, stop_loss_threshold=-2.0):
        self.spread_calculator = spread_calculator
        self.entry_threshold = entry_threshold
        self.exit_threshold = exit_threshold
        self.stop_loss_threshold = stop_loss_threshold
        self.position = None
        self.z_scores = deque(maxlen=spread_calculator.window_size)

    async def fetch_1m_data(self):
        url_a = f"wss://testnet.binance.vision/ws/{self.spread_calculator.pair_a}@kline_1m"
        url_b = f"wss://testnet.binance.vision/ws/{self.spread_calculator.pair_b}@kline_1m"

        async with websockets.connect(url_a) as ws_a, websockets.connect(url_b) as ws_b:
            while True:
                response_a = await ws_a.recv()
                response_b = await ws_b.recv()

                data_a = json.loads(response_a)
                data_b = json.loads(response_b)

                close_price_a = float(data_a['k']['c'])
                close_price_b = float(data_b['k']['c'])

                self.calculate_z_score(close_price_a, close_price_b)
                self.check_entry_exit()

    def calculate_z_score(self, close_price_a, close_price_b):
        if self.spread_calculator.mean_spread is None or self.spread_calculator.std_spread is None:
            return

        spread = close_price_a - close_price_b
        z_score = (spread - self.spread_calculator.mean_spread) / self.spread_calculator.std_spread
        self.z_scores.append(z_score)

    def check_entry_exit(self):
        if len(self.z_scores) == 0:
            return

        z_score = self.z_scores[-1]

        if self.position is None:
            if z_score > self.entry_threshold:
                self.enter_position('short')
            elif z_score < -self.entry_threshold:
                self.enter_position('long')
        else:
            if (self.position == 'long' and z_score > self.exit_threshold) or (
                    self.position == 'short' and z_score < -self.exit_threshold):
                self.exit_position()
            elif (self.position == 'long' and z_score < self.stop_loss_threshold) or (
                    self.position == 'short' and z_score > -self.stop_loss_threshold):
                self.exit_position()

    def enter_position(self, direction):
        self.position = direction
        # Implement the logic to enter the position
        print(f"Entering {direction} position")

    def exit_position(self):
        # Implement the logic to exit the position
        print(f"Exiting {self.position} position")
        self.position = None


async def main():
    spread_calculator = SpreadCalculator(pair_a="pairA", pair_b="pairB")
    trade_manager = TradeManager(spread_calculator)

    await asyncio.gather(
        spread_calculator.fetch_15m_data(),
        trade_manager.fetch_1m_data()
    )


# Run the main function
asyncio.get_event_loop().run_until_complete(main())