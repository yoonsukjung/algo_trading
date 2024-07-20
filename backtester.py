import logging
import pandas as pd
import numpy as np
from result_handler import ResultHandler

logger = logging.getLogger(__name__)

class Backtester:
    def __init__(self, strategy, start_date=None, end_date=None, fee=0.001, slippage=0.001, result_path=None):
        self.strategy = strategy
        self.start_date = start_date
        self.end_date = end_date
        self.fee = fee
        self.slippage = slippage
        self.result_handler = ResultHandler(strategy, result_path)

    def run_backtest(self):
        logger.info("Running backtest")
        try:
            self.prepare_data()
            self.strategy.generate_signals()
            trade_log, performance_metrics = self.calculate_performance()
            self.result_handler.save_trade_log(trade_log)
            self.result_handler.save_performance_metrics(performance_metrics)
            self.result_handler.plot_performance()
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
            trade_log = self.calculate_trade_returns()
            self.add_returns_to_signals(trade_log)
            self.strategy.signals['equity'] = (self.strategy.signals['returns'] + 1).cumprod()
            performance_metrics = self.calculate_performance_metrics()
            return trade_log, performance_metrics
        except Exception as e:
            logger.error(f"Error in calculating performance: {e}")
            return [], {}

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
                self.result_handler.trade_log.append({
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
                trade_return = self.calculate_single_trade_return(entry_price1, exit_price1, entry_price2, exit_price2, entry_type)
                returns.append((index, trade_return))
                self.result_handler.trade_log.append({
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
            trade_return = (exit_price1 - entry_price1) / entry_price1 - (exit_price2 - entry_price2) / entry_price2
        elif entry_type == 'short':
            trade_return = (entry_price1 - exit_price1) / entry_price1 - (entry_price2 - exit_price2) / entry_price2
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

    def calculate_performance_metrics(self):
        logger.info("Calculating performance metrics")
        try:
            total_returns = self.strategy.signals['equity'].iloc[-1] - 1
            annualized_returns = (1 + total_returns) ** (365 / len(self.strategy.signals)) - 1
            volatility = self.strategy.signals['returns'].std() * np.sqrt(252)
            sharpe_ratio = annualized_returns / volatility if volatility != 0 else 0
            max_drawdown = self.calculate_max_drawdown(self.strategy.signals['equity'])
            winning_trades = (self.strategy.signals['returns'] > 0).sum()
            win_rate = winning_trades / len(self.result_handler.trade_log) if len(self.result_handler.trade_log) > 0 else 0
            avg_trade_return = self.strategy.signals[self.strategy.signals['returns'] != 0]['returns'].mean()
            num_trades = len(self.result_handler.trade_log)

            performance_metrics = {
                'total_return': total_returns,
                'annualized_return': annualized_returns,
                'annualized_volatility': volatility,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown,
                'win_rate': win_rate,
                'average_profit_per_trade': avg_trade_return,
                'num_trades': num_trades
            }
            return performance_metrics
        except Exception as e:
            logger.error(f"Error in calculating performance metrics: {e}")
            return {}

    def calculate_max_drawdown(self, equity_curve):
        logger.info("Calculating max drawdown")
        try:
            drawdown = equity_curve / equity_curve.cummax() - 1
            return drawdown.min()
        except Exception as e:
            logger.error(f"Error in calculating max drawdown: {e}")
            return 0
