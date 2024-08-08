import numpy as np
from time import sleep
import logging

logger = logging.getLogger(__name__)


class CointegrationStrategy:
    def __init__(self, exchange, symbol1: str, symbol2: str, slope: float, spread_mean: float, spread_std: float,
                 entry_z_score=1.2, exit_z_score=0.2, stop_z_score=3, fee=0.001, slippage=0.001):
        self.exchange = exchange
        self.symbol1 = symbol1
        self.symbol2 = symbol2
        self.slope = slope
        self.entry_z_score = entry_z_score
        self.exit_z_score = exit_z_score
        self.stop_z_score = stop_z_score
        self.fee = fee
        self.slippage = slippage
        self.spread_mean = spread_mean
        self.spread_std = spread_std
        self.position = 0

    def fetch_latest_data(self):
        ohlcv1 = self.exchange.fetch_ohlcv(self.symbol1, '1m', limit=1)
        ohlcv2 = self.exchange.fetch_ohlcv(self.symbol2, '1m', limit=1)
        return {
            'asset1': {'close': ohlcv1[0][4]},
            'asset2': {'close': ohlcv2[0][4]}
        }

    def calculate_z_score(self, data: dict) -> float:
        spread = np.log(data['asset1']['close']) - self.slope * np.log(data['asset2']['close'])
        return (spread - self.spread_mean) / self.spread_std

    def generate_signal(self, z_score: float) -> str:
        if self.position == 0:
            if z_score > self.entry_z_score:
                return 'short'
            elif z_score < -self.entry_z_score:
                return 'long'
        elif self.position != 0:
            if abs(z_score) < self.exit_z_score or abs(z_score) > self.stop_z_score:
                return 'close'
        return None

    def execute_trade(self, signal: str, data: dict):
        if signal == 'long':
            self.exchange.create_market_buy_order(self.symbol1, 1)
            self.exchange.create_market_sell_order(self.symbol2, 1)
            self.position = 1
            logger.info(f"Executed long trade: Buy {self.symbol1}, Sell {self.symbol2}")
        elif signal == 'short':
            self.exchange.create_market_sell_order(self.symbol1, 1)
            self.exchange.create_market_buy_order(self.symbol2, 1)
            self.position = -1
            logger.info(f"Executed short trade: Sell {self.symbol1}, Buy {self.symbol2}")
        elif signal == 'close':
            if self.position == 1:
                self.exchange.create_market_sell_order(self.symbol1, 1)
                self.exchange.create_market_buy_order(self.symbol2, 1)
            elif self.position == -1:
                self.exchange.create_market_buy_order(self.symbol1, 1)
                self.exchange.create_market_sell_order(self.symbol2, 1)
            self.position = 0
            logger.info(f"Closed position")

    def run(self):
        while True:
            try:
                data = self.fetch_latest_data()
                z_score = self.calculate_z_score(data)
                signal = self.generate_signal(z_score)

                if signal:
                    self.execute_trade(signal, data)

                logger.info(f"Current z-score: {z_score}, Position: {self.position}")
                sleep(60)  # Wait for 1 minute before next iteration
            except Exception as e:
                logger.error(f"An error occurred: {e}")
                sleep(60)
