import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class Strategy:
    def __init__(self, data: dict):
        self.data = data
        self.signals = pd.DataFrame(index=data['asset1'].index)
        self.position = 0

    def generate_signals(self):
        raise NotImplementedError("Should implement generate_signals method")

    def execute_trade(self, index, signal):
        if signal == "long":
            self.position = 1
            self.signals.loc[index, 'trade'] = "long"
            logger.info(f"Executed long trade at index {index}")
        elif signal == "short":
            self.position = -1
            self.signals.loc[index, 'trade'] = "short"
            logger.info(f"Executed short trade at index {index}")
        elif signal == "close":
            self.position = 0
            self.signals.loc[index, 'trade'] = "close"
            logger.info(f"Closed position at index {index}")

class CointegrationStrategy(Strategy):
    def __init__(self, data, slope, crypto1, crypto2, entry_z_score=1.5, exit_z_score=0.5, stop_z_score=5, fee=0.001, slippage=0.001):
        super().__init__(data)
        self.data1 = data['asset1'].copy()
        self.data2 = data['asset2'].copy()
        self.slope = slope
        self.crypto1 = crypto1
        self.crypto2 = crypto2
        self.entry_z_score = entry_z_score
        self.exit_z_score = exit_z_score
        self.stop_z_score = stop_z_score
        self.fee = fee
        self.slippage = slippage

    def calculate_theta(self):
        logger.info("Calculating theta")
        spread = np.log(self.data1['close']) - self.slope * np.log(self.data2['close'])
        mu = spread.mean()
        numerator = ((spread[:-1] - mu) * (spread[1:].values - spread[:-1].values)).sum()
        denominator = ((spread[:-1] - mu) ** 2).sum()
        theta = -numerator / denominator if denominator != 0 else 0
        return theta

    def calculate_rolling_window(self, theta, delta_t=1):
        t_half = np.log(2) / theta if theta != 0 else float('inf')
        lambda_ema = 1 - np.exp(-theta * delta_t) if theta != 0 else 0
        n_sma = int(np.round((2 / lambda_ema) - 1)) if lambda_ema != 0 else float('inf')
        return n_sma

    def calculate_z_score(self):
        logger.info("Calculating z-score")
        theta = self.calculate_theta()
        logger.info(f"Calculated theta: {theta}")
        n_sma = self.calculate_rolling_window(theta)
        logger.info(f"Calculated rolling window size: {n_sma}")

        spread = np.log(self.data1['close']) - self.slope * np.log(self.data2['close'])
        rolling_mean = spread.rolling(window=n_sma).mean()
        rolling_std = spread.rolling(window=n_sma).std()

        self.data1['z_score'] = (spread - rolling_mean) / rolling_std
        self.signals['z_score'] = self.data1['z_score']

        logger.info(f"Sample z_score values: {self.data1['z_score'].head()}")

    def generate_trade_signals(self):
        logger.info("Generating trade signals")
        self.signals['trade'] = np.nan
        self.signals['price1'] = np.nan
        self.signals['price2'] = np.nan

        entry_short = self.signals['z_score'] > self.entry_z_score
        entry_long = self.signals['z_score'] < -self.entry_z_score
        exit_position = self.signals['z_score'].abs() < self.exit_z_score
        stop_loss = self.signals['z_score'].abs() > self.stop_z_score

        for index in self.signals.index:
            if self.position == 0:
                self.check_for_entry(index, entry_short, entry_long, stop_loss)
            else:
                self.check_for_exit(index, exit_position, stop_loss)

        self.signals['position'] = self.signals['trade'].ffill().shift().fillna(0).replace(
            {'long': 1, 'short': -1, 'close': 0})

    def check_for_entry(self, index, entry_short, entry_long, stop_loss):
        if stop_loss[index]:
            logger.info(f"Stop loss triggered at index {index}, not entering new position")
        elif entry_short[index]:
            self.signals.at[index, 'price1'] = self.data1.at[index, 'close'] * (1 + self.slippage)
            self.signals.at[index, 'price2'] = self.data2.at[index, 'close'] * (1 - self.slippage)
            self.execute_trade(index, 'short')
        elif entry_long[index]:
            self.signals.at[index, 'price1'] = self.data1.at[index, 'close'] * (1 - self.slippage)
            self.signals.at[index, 'price2'] = self.data2.at[index, 'close'] * (1 + self.slippage)
            self.execute_trade(index, 'long')

    def check_for_exit(self, index, exit_position, stop_loss):
        if exit_position[index] or stop_loss[index]:
            self.signals.at[index, 'price1'] = self.data1.at[index, 'close']
            self.signals.at[index, 'price2'] = self.data2.at[index, 'close']
            self.execute_trade(index, 'close')
            if stop_loss[index]:
                logger.info(f"Stop loss triggered at index {index}")

    def generate_signals(self):
        logger.info("Generating signals")
        try:
            self.calculate_z_score()
            self.generate_trade_signals()
            logger.info("Signals generated")
            print(self.signals[['z_score', 'trade', 'price1', 'price2']])
        except Exception as e:
            logger.error(f"Error in generating signals: {e}")
