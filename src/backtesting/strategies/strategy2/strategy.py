import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class Strategy:
    def __init__(self, data: dict):
        self.data = data
        self.signals = pd.DataFrame(index=data['asset1'].index)
        self.position = 0
        self.indicators = pd.DataFrame(index=self.signals.index)
        self.indicators['stop_loss'] = False

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
    def __init__(self, data, slope, crypto1, crypto2, spread_mean, spread_std, entry_z_score=1.2, exit_z_score=0.2, stop_z_score=3, fee=0.001, slippage=0.001):
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
        self.spread_mean = spread_mean
        self.spread_std = spread_std


    def calculate_z_score(self):
        logger.info("Calculating z-score")
        spread = np.log(self.data1['close']) - self.slope * np.log(self.data2['close'])


        self.data1['z_score'] = (spread - self.spread_mean) / self.spread_std
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
            self.update_indicators(index, exit_position, stop_loss)
            if self.position == 0:
                if not self.indicators.loc[index, 'stop_loss']:
                    self.check_for_entry(index, entry_short, entry_long, stop_loss)
                else:
                    continue

            else:
                self.check_for_exit(index, exit_position, stop_loss)



        result = self.signals['trade'].ffill().shift().fillna(0).replace(
            {'long': 1, 'short': -1, 'close': 0})
        self.signals['position'] = result.infer_objects(copy=False)


    def update_indicators(self, index, exit_position, stop_loss):
        if stop_loss.loc[index]:
            self.indicators.loc[index:, 'stop_loss'] = True
        elif exit_position.loc[index]:
            self.indicators.loc[index:, 'stop_loss'] = False
        else:
            if index > self.indicators.index[0]:
                previous_index = self.indicators.index[self.indicators.index.get_loc(index) - 1]
                self.indicators.loc[index, 'stop_loss'] = self.indicators.loc[previous_index, 'stop_loss']

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
        if exit_position.loc[index] or stop_loss.loc[index]:
            self.signals.loc[index, 'price1'] = self.data1.loc[index, 'close']
            self.signals.loc[index, 'price2'] = self.data2.loc[index, 'close']
            self.execute_trade(index, 'close')
            if stop_loss.loc[index]:
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