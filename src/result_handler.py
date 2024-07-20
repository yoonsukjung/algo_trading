import os
import pandas as pd
import logging
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

class ResultHandler:
    def __init__(self, strategy, result_path):
        self.strategy = strategy
        self.result_path = result_path

    def save_trade_log(self, trade_log):
        if self.result_path:
            crypto_pair_folder = os.path.join(self.result_path, f"{self.strategy.crypto1}_{self.strategy.crypto2}")
            os.makedirs(crypto_pair_folder, exist_ok=True)
            csv_folder = os.path.join(crypto_pair_folder, 'csv')
            os.makedirs(csv_folder, exist_ok=True)

            trade_log_df = pd.DataFrame(trade_log)
            file_name = f"{self.strategy.entry_z_score}_{self.strategy.exit_z_score}_{self.strategy.stop_z_score}.csv"
            trade_log_path = os.path.join(csv_folder, file_name)
            trade_log_df.to_csv(trade_log_path, index=False)
            logger.info(f"Trade log saved to {trade_log_path}")

    def save_performance_metrics(self, performance_metrics):
        try:
            crypto_pair_folder = os.path.join(self.result_path, f"{self.strategy.crypto1}_{self.strategy.crypto2}")
            os.makedirs(crypto_pair_folder, exist_ok=True)

            result_file_path = os.path.join(crypto_pair_folder, f"{self.strategy.crypto1}_{self.strategy.crypto2}.csv")
            if not os.path.exists(result_file_path):
                columns = ['crypto1', 'crypto2', 'entry_z', 'exit_z', 'stop_z', 'total_ret', 'ann_ret',
                           'ann_vol', 'sharpe', 'max_dd', 'win_rate', 'avg_trade_ret', 'num_trades', 'rolling_window']
                df = pd.DataFrame(columns=columns)
                df.to_csv(result_file_path, index=False)
                logger.info(f"Created new file: {result_file_path}")
            else:
                df = pd.read_csv(result_file_path)

            n_sma = self.strategy.calculate_rolling_window(self.strategy.calculate_theta())

            new_row = {
                'crypto1': self.strategy.crypto1,
                'crypto2': self.strategy.crypto2,
                'entry_z': self.strategy.entry_z_score,
                'exit_z': self.strategy.exit_z_score,
                'stop_z': self.strategy.stop_z_score,
                'total_ret': performance_metrics.get('total_return', 0),
                'ann_ret': performance_metrics.get('annualized_return', 0),
                'ann_vol': performance_metrics.get('annualized_volatility', 0),
                'sharpe': performance_metrics.get('sharpe_ratio', 0),
                'max_dd': performance_metrics.get('max_drawdown', 0),
                'win_rate': performance_metrics.get('win_rate', 0),
                'avg_trade_ret': performance_metrics.get('average_profit_per_trade', 0),
                'num_trades': performance_metrics.get('num_trades', 0),
                'rolling_window': n_sma
            }

            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            df.to_csv(result_file_path, index=False)
            logger.info(f"Appended new data to {result_file_path}")
        except Exception as e:
            logger.error(f"Error in saving new file: {e}")

    def plot_performance(self):
        logger.info("Plotting performance")
        try:
            plt.figure(figsize=(14, 10))

            plot_title = f"{self.strategy.crypto1} and {self.strategy.crypto2} Performance"

            plt.subplot(3, 1, 1)
            plt.plot(self.strategy.data1.index, self.strategy.data1['close'], label='Asset 1 Close Price')
            plt.plot(self.strategy.data2.index, self.strategy.data2['close'], label='Asset 2 Close Price')
            if 'log_spread' in self.strategy.data1.columns:
                plt.plot(self.strategy.data1.index, self.strategy.data1['log_spread'], label='Log Spread')

            long_signals = self.strategy.signals[self.strategy.signals['trade'] == 'long']
            short_signals = self.strategy.signals[self.strategy.signals['trade'] == 'short']
            close_signals = self.strategy.signals[self.strategy.signals['trade'] == 'close']

            plt.scatter(long_signals.index, self.strategy.data1.loc[long_signals.index, 'close'], marker='^', label='Long Signal', alpha=1)
            plt.scatter(short_signals.index, self.strategy.data1.loc[short_signals.index, 'close'], marker='v', label='Short Signal', alpha=1)
            plt.scatter(close_signals.index, self.strategy.data1.loc[close_signals.index, 'close'], marker='o', label='Close Signal', alpha=1)

            plt.legend(loc='best', fontsize='small')
            plt.title('Asset Prices and Trading Signals')
            plt.xlim(self.strategy.data1.index.min(), self.strategy.data1.index.max())

            plt.subplot(3, 1, 2)
            plt.plot(self.strategy.signals.index, self.strategy.signals['equity'], label='Equity Curve', color='purple')
            plt.legend(loc='best', fontsize='small')
            plt.title('Equity Curve')
            plt.xlim(self.strategy.data1.index.min(), self.strategy.data1.index.max())

            plt.subplot(3, 1, 3)
            plt.plot(self.strategy.signals.index, self.strategy.signals['z_score'], label='Z-score')
            plt.axhline(y=self.strategy.entry_z_score, color='r', linestyle='--', label='Entry Z-score')
            plt.axhline(y=-self.strategy.entry_z_score, color='r', linestyle='--')
            plt.axhline(y=self.strategy.exit_z_score, color='g', linestyle='--', label='Exit Z-score')
            plt.axhline(y=-self.strategy.exit_z_score, color='g', linestyle='--')
            plt.axhline(y=self.strategy.stop_z_score, color='y', linestyle='--', label='Stop-loss Z-score')
            plt.axhline(y=-self.strategy.stop_z_score, color='y', linestyle='--')

            plt.scatter(long_signals.index, self.strategy.signals.loc[long_signals.index, 'z_score'], marker='^', color='g', label='Long Signal', alpha=1)
            plt.scatter(short_signals.index, self.strategy.signals.loc[short_signals.index, 'z_score'], marker='v', color='r', label='Short Signal', alpha=1)
            plt.scatter(close_signals.index, self.strategy.signals.loc[close_signals.index, 'z_score'], marker='o', color='b', label='Close Signal', alpha=1)

            plt.legend(loc='best', fontsize='small')
            plt.title('Z-score and Trading Signals')
            plt.xlim(self.strategy.data1.index.min(), self.strategy.data1.index.max())

            plt.suptitle(plot_title)
            plt.tight_layout(rect=[0, 0, 1, 0.96])

            png_folder = os.path.join(self.result_path, f"{self.strategy.crypto1}_{self.strategy.crypto2}", 'png')
            os.makedirs(png_folder, exist_ok=True)
            image_filename = os.path.join(png_folder, f"{self.strategy.entry_z_score}_{self.strategy.exit_z_score}_{self.strategy.stop_z_score}.png")
            plt.savefig(image_filename)
        except Exception as e:
            logger.error(f"Error in plotting performance: {e}")
