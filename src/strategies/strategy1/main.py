import os
import pandas as pd
from ..utils import data_loader
from strategy import CointegrationStrategy
from backtester import Backtester
from ..utils import utils
from config import data_path, result_path_strategy1

logger = utils.setup_logging()

# 사용자가 쉽게 수정할 수 있는 경로 변수 설정

category_path = os.path.join(result_path_strategy1, "Layer1_Layer2")
coint_file_path = os.path.join(category_path, "coint_pairs.csv")

def run_backtest_for_row(row_index, base_path, result_path, coint_file_path):
    try:
        file1 = pd.read_csv(coint_file_path)
        row = file1.iloc[row_index]
        crypto1 = row['crypto1']
        crypto2 = row['crypto2']
        slope = row['HR']
        categories = row['categories']

        data1_path = os.path.join(base_path, f"{crypto1}_USDT_15m.csv")
        data2_path = os.path.join(base_path, f"{crypto2}_USDT_15m.csv")

        data1 = pd.read_csv(data1_path)
        data1['timestamp'] = pd.to_datetime(data1['timestamp'])

        data2 = pd.read_csv(data2_path)
        data2['timestamp'] = pd.to_datetime(data2['timestamp'])

        data = {'asset1': data1.set_index('timestamp'), 'asset2': data2.set_index('timestamp')}

        start_date = '2024-01-01'
        end_date = '2024-06-30'

        strategy = CointegrationStrategy(data, slope, crypto1, crypto2)
        strategy.categories = categories
        backtester = Backtester(strategy, start_date=start_date, end_date=end_date, fee=0.001, slippage=0.001, result_path=result_path)
        backtester.run_backtest()
    except Exception as e:
        logger.error(f"Error in the main execution: {e}")

def run_backtest_for_all_rows(base_path, result_path, coint_file_path):
    try:
        file1 = pd.read_csv(coint_file_path)
        num_rows = len(file1)

        for row_index in range(num_rows):
            run_backtest_for_row(row_index, base_path, result_path, coint_file_path)
    except Exception as e:
        logger.error(f"Error in running backtest for all rows: {e}")

if __name__ == "__main__":
    run_backtest_for_all_rows(data_path, category_path, coint_file_path)
