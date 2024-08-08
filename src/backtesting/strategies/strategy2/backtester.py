
import numpy as np
import os
import pandas as pd
import logging
import matplotlib.pyplot as plt


logger = logging.getLogger(__name__)
class Backtester:
    def __init__(self, strategy, start_date=None, end_date=None, fee=0.001, slippage=0.001, result_path=None, category_name=None):
        self.strategy = strategy
        self.start_date = start_date
        self.end_date = end_date
        self.fee = fee
        self.slippage = slippage
        self.result_path = result_path
        self.trade_log = []
        self.performance_metrics = {}
        self.category_name = category_name

    def run_backtest(self):
        logger.info("Running backtest")
        try:
            self.prepare_data()
            self.strategy.generate_signals()
            self.calculate_performance()
            self.save_trade_log()
            self.save_results()
        except Exception as e:
            logger.error(f"Error in running backtest: {e}")

    def prepare_data(self):
        logger.info("Preparing data")
        if self.start_date and self.end_date:
            self.strategy.data1 = self.strategy.data1.loc[
                (self.strategy.data1.index >= self.start_date) &
                (self.strategy.data1.index <= self.end_date)
                ]
            self.strategy.data2 = self.strategy.data2.loc[
                (self.strategy.data2.index >= self.start_date) &
                (self.strategy.data2.index <= self.end_date)
                ]

    def calculate_performance(self):
        logger.info("Calculating performance")
        try:
            returns = self.calculate_trade_returns()
            self.add_returns_to_signals(returns)
            self.strategy.signals['equity'] = (self.strategy.signals['returns'] + 1).cumprod()
            self.calculate_metrics()
            self.print_report(len(returns))
            self.plot_performance()
        except Exception as e:
            logger.error(f"Error in calculating performance: {e}")

    def calculate_trade_returns(self):
        logger.info("Calculating trade returns")
        returns = []
        trades = self.strategy.signals[self.strategy.signals['trade'].notna()]
        entry_price1 = 0
        entry_price2 = 0
        entry_index = None
        entry_type = None

        for index, trade in trades.iterrows():
            if trade['trade'] in ['long', 'short']:
                entry_price1 = self.strategy.data1.loc[index, 'close']
                entry_price2 = self.strategy.data2.loc[index, 'close']
                entry_index = index
                entry_type = trade['trade']
                self.trade_log.append({
                    'index': index,
                    'trade': trade['trade'],
                    'entry_price1': entry_price1,
                    'exit_price1': None,
                    'entry_price2': entry_price2,
                    'exit_price2': None,
                    'return': None,
                    'z_score': self.strategy.signals.loc[index, 'z_score'],
                    'position': self.strategy.signals.loc[index, 'position']
                })
            elif trade['trade'] == 'close' and entry_price1 != 0 and entry_price2 != 0:
                exit_price1 = self.strategy.data1.loc[index, 'close']
                exit_price2 = self.strategy.data2.loc[index, 'close']
                trade_return = self.calculate_single_trade_return(entry_price1, exit_price1, entry_price2, exit_price2,
                                                                  entry_type)
                returns.append((index, trade_return))
                self.trade_log.append({
                    'index': index,
                    'trade': trade['trade'],
                    'entry_price1': entry_price1,
                    'exit_price1': exit_price1,
                    'entry_price2': entry_price2,
                    'exit_price2': exit_price2,
                    'return': trade_return,
                    'z_score': self.strategy.signals.loc[index, 'z_score'],
                    'position': self.strategy.signals.loc[index, 'position']
                })
                entry_price1 = 0
                entry_price2 = 0
                entry_index = None
                entry_type = None

        return returns

    def calculate_single_trade_return(self, entry_price1, exit_price1, entry_price2, exit_price2, entry_type):
        logger.info("Calculating single trade return")
        if entry_type == 'long':
            trade_return = (exit_price1 - entry_price1) / entry_price1 - \
                           (exit_price2 - entry_price2) / entry_price2
        elif entry_type == 'short':
            trade_return = (entry_price1 - exit_price1) / entry_price1 - \
                           (entry_price2 - exit_price2) / entry_price2
        trade_return -= self.fee * 2  # 진입 및 청산 시 수수료
        trade_return -= self.slippage * 2  # 진입 및 청산 시 슬리피지
        return trade_return

    def add_returns_to_signals(self, returns):
        logger.info("Adding returns to signals")
        if returns:
            returns_df = pd.DataFrame(returns, columns=['index', 'returns']).set_index('index')
            self.strategy.signals = self.strategy.signals.join(returns_df, how='left')
            self.strategy.signals['returns'].fillna(0, inplace=True)
        else:
            self.strategy.signals['returns'] = 0

    def save_trade_log(self):
        if self.result_path:
            crypto_pair_folder = os.path.join(self.result_path, f"{self.strategy.crypto1}_{self.strategy.crypto2}")
            os.makedirs(crypto_pair_folder, exist_ok=True)
            csv_folder = os.path.join(crypto_pair_folder, 'csv')
            os.makedirs(csv_folder, exist_ok=True)

            trade_log_df = pd.DataFrame(self.trade_log)
            file_name = f"{self.strategy.entry_z_score}_{self.strategy.exit_z_score}_{self.strategy.stop_z_score}.csv"
            trade_log_path = os.path.join(csv_folder, file_name)
            trade_log_df.to_csv(trade_log_path, index=False)
            logger.info(f"Trade log saved to {trade_log_path}")

    def calculate_metrics(self):
        logger.info("Calculating performance metrics")
        try:
            total_returns = self.strategy.signals['equity'].iloc[-1] - 1
            annualized_returns = (1 + total_returns) ** (365 / len(self.strategy.signals)) - 1
            volatility = self.strategy.signals['returns'].std() * np.sqrt(365)
            sharpe_ratio = ((1+ self.strategy.signals['returns'].mean()) ** 365 - 1 - 0.03) / volatility
            max_drawdown = self.calculate_max_drawdown(self.strategy.signals['equity'])
            winning_trades = (self.strategy.signals['returns'] > 0).sum()
            win_rate = winning_trades / len(self.trade_log) if len(self.trade_log) > 0 else 0
            avg_trade_return = self.strategy.signals[self.strategy.signals['returns'] != 0]['returns'].mean()
            num_trades = len(self.trade_log)

            self.performance_metrics = {
                'total_return': total_returns,
                'annualized_return': annualized_returns,
                'annualized_volatility': volatility,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown,
                'win_rate': win_rate,
                'average_profit_per_trade': avg_trade_return,
                'num_trades': num_trades
            }
        except Exception as e:
            logger.error(f"Error in calculating performance metrics: {e}")

    def print_report(self, total_trades):
        logger.info("Printing report")
        try:
            metrics = self.performance_metrics

            print(f"Total Returns: {metrics['total_return']:.2%}")
            print(f"Annualized Returns: {metrics['annualized_return']:.2%}")
            print(f"Annualized Volatility: {metrics['annualized_volatility']:.2%}")
            print(f"Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")
            print(f"Max Drawdown: {metrics['max_drawdown']:.2%}")
            print(f"Win Rate: {metrics['win_rate']:.2%}")
            print(f"Average Trade Return: {metrics['average_profit_per_trade']:.2%}")
            print(f"Total Number of Trades: {metrics['num_trades']}")
        except Exception as e:
            logger.error(f"Error in printing report: {e}")

    def calculate_max_drawdown(self, equity_curve):
        logger.info("Calculating max drawdown")
        try:
            drawdown = equity_curve / equity_curve.cummax() - 1
            return drawdown.min()
        except Exception as e:
            logger.error(f"Error in calculating max drawdown: {e}")
            return 0

    def plot_performance(self):
        logger.info("Plotting performance")
        try:
            plt.figure(figsize=(14, 10))

            plot_title = f"{self.strategy.crypto1} and {self.strategy.crypto2} Performance"

            # 첫 번째 그래프: 자산 가격 및 트레이딩 신호
            plt.subplot(3, 1, 1)
            plt.plot(self.strategy.data1.index, self.strategy.data1['close'], label='Asset 1 Close Price')
            plt.plot(self.strategy.data2.index, self.strategy.data2['close'], label='Asset 2 Close Price')
            if 'log_spread' in self.strategy.data1.columns:
                plt.plot(self.strategy.data1.index, self.strategy.data1['log_spread'], label='Log Spread')

            long_signals = self.strategy.signals[self.strategy.signals['trade'] == 'long']
            short_signals = self.strategy.signals[self.strategy.signals['trade'] == 'short']
            close_signals = self.strategy.signals[self.strategy.signals['trade'] == 'close']

            plt.scatter(long_signals.index, self.strategy.data1.loc[long_signals.index, 'close'], marker='^',
                        label='Long Signal', alpha=1)
            plt.scatter(short_signals.index, self.strategy.data1.loc[short_signals.index, 'close'], marker='v',
                        label='Short Signal', alpha=1)
            plt.scatter(close_signals.index, self.strategy.data1.loc[close_signals.index, 'close'], marker='o',
                        label='Close Signal', alpha=1)

            plt.legend(loc='best', fontsize='small')
            plt.title('Asset Prices and Trading Signals')
            plt.xlim(self.strategy.data1.index.min(), self.strategy.data1.index.max())

            # 두 번째 그래프: Equity Curve
            plt.subplot(3, 1, 2)
            plt.plot(self.strategy.signals.index, self.strategy.signals['equity'], label='Equity Curve', color='purple')
            plt.legend(loc='best', fontsize='small')
            plt.title('Equity Curve')
            plt.xlim(self.strategy.data1.index.min(), self.strategy.data1.index.max())

            # 세 번째 그래프: Z-score 및 트레이딩 신호
            plt.subplot(3, 1, 3)
            plt.plot(self.strategy.signals.index, self.strategy.signals['z_score'], label='Z-score')
            plt.axhline(y=self.strategy.entry_z_score, color='r', linestyle='--', label='Entry Z-score')
            plt.axhline(y=-self.strategy.entry_z_score, color='r', linestyle='--')
            plt.axhline(y=self.strategy.exit_z_score, color='g', linestyle='--', label='Exit Z-score')
            plt.axhline(y=-self.strategy.exit_z_score, color='g', linestyle='--')
            plt.axhline(y=self.strategy.stop_z_score, color='y', linestyle='--', label='Stop-loss Z-score')
            plt.axhline(y=-self.strategy.stop_z_score, color='y', linestyle='--')

            plt.scatter(long_signals.index, self.strategy.signals.loc[long_signals.index, 'z_score'], marker='^',
                        color='g', label='Long Signal', alpha=1)
            plt.scatter(short_signals.index, self.strategy.signals.loc[short_signals.index, 'z_score'], marker='v',
                        color='r', label='Short Signal', alpha=1)
            plt.scatter(close_signals.index, self.strategy.signals.loc[close_signals.index, 'z_score'], marker='o',
                        color='b', label='Close Signal', alpha=1)

            plt.legend(loc='best', fontsize='small')
            plt.title('Z-score and Trading Signals')
            plt.xlim(self.strategy.data1.index.min(), self.strategy.data1.index.max())

            plt.suptitle(plot_title)
            plt.tight_layout(rect=[0, 0, 1, 0.96])

            # Save the plot as an image file in the result_path
            png_folder = os.path.join(self.result_path, f"{self.strategy.crypto1}_{self.strategy.crypto2}", 'png')
            os.makedirs(png_folder, exist_ok=True)
            image_filename = os.path.join(png_folder,
                                          f"{self.strategy.entry_z_score}_{self.strategy.exit_z_score}_{self.strategy.stop_z_score}.png")
            plt.savefig(image_filename)
            # plt.show()  # 이 줄을 주석 처리하여 plot을 띄우지 않음
        except Exception as e:
            logger.error(f"Error in plotting performance: {e}")

    def save_results(self):
        try:
            crypto_pair_folder = os.path.join(self.result_path, f"{self.strategy.crypto1}_{self.strategy.crypto2}")
            os.makedirs(crypto_pair_folder, exist_ok=True)

            result_file_path = os.path.join(crypto_pair_folder, f"{self.strategy.crypto1}_{self.strategy.crypto2}.csv")
            if not os.path.exists(result_file_path):
                columns = ['crypto1', 'crypto2', 'entry_z', 'exit_z', 'stop_z', 'total_ret', 'ann_ret',
                           'ann_vol', 'sharpe', 'max_dd', 'win_rate', 'avg_trade_ret', 'num_trades']
                df = pd.DataFrame(columns=columns)
                df.to_csv(result_file_path, index=False)
                logger.info(f"Created new file: {result_file_path}")
            else:
                df = pd.read_csv(result_file_path)


            new_row = {
                'category': self.category_name,
                'crypto1': self.strategy.crypto1,
                'crypto2': self.strategy.crypto2,
                'entry_z': self.strategy.entry_z_score,
                'exit_z': self.strategy.exit_z_score,
                'stop_z': self.strategy.stop_z_score,
                'total_ret': self.performance_metrics.get('total_return', 0),
                'ann_ret': self.performance_metrics.get('annualized_return', 0),
                'ann_vol': self.performance_metrics.get('annualized_volatility', 0),
                'sharpe': self.performance_metrics.get('sharpe_ratio', 0),
                'max_dd': self.performance_metrics.get('max_drawdown', 0),
                'win_rate': self.performance_metrics.get('win_rate', 0),
                'avg_trade_ret': self.performance_metrics.get('average_profit_per_trade', 0),
                'num_trades': self.performance_metrics.get('num_trades', 0)
            }

            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            df.to_csv(result_file_path, index=False)
            logger.info(f"Appended new data to {result_file_path}")
        except Exception as e:
            logger.error(f"Error in saving new file: {e}")