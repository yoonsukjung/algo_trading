import os
import pandas as pd
from strategy import CointegrationStrategy
from backtester import Backtester
from src.strategies.utils import logger
from config import data_path, result_path_strategy2

logger = logger.setup_logging()

# 사용자가 쉽게 수정할 수 있는 경로 변수 설정

category_path = os.path.join(result_path_strategy2, "Metaverse")
coint_file_path = os.path.join(category_path, "coint_pairs.csv")

def run_backtest_for_row(row_index, base_path, result_path, coint_file_path):
    try:
        file1 = pd.read_csv(coint_file_path)
        row = file1.iloc[row_index]
        crypto1 = row['crypto1']
        crypto2 = row['crypto2']
        slope = row['HR']
        categories = row['categories']
        spread_mean = row['spread_mean']
        spread_std = row['spread_std']


        data1_path = os.path.join(base_path, f"{crypto1}_USDT_15m.csv")
        data2_path = os.path.join(base_path, f"{crypto2}_USDT_15m.csv")

        data1 = pd.read_csv(data1_path)
        data1['timestamp'] = pd.to_datetime(data1['timestamp'])

        data2 = pd.read_csv(data2_path)
        data2['timestamp'] = pd.to_datetime(data2['timestamp'])

        data = {'asset1': data1.set_index('timestamp'), 'asset2': data2.set_index('timestamp')}

        start_date = '2024-01-01'
        end_date = '2024-06-30'

        strategy = CointegrationStrategy(data, slope, crypto1, crypto2, spread_mean, spread_std)
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

# if __name__ == "__main__":
#     run_backtest_for_all_rows(data_path, category_path, coint_file_path)

# if __name__ == "__main__":
#     run_backtest_for_row(9, data_path, category_path, coint_file_path)

# 접근하고자 하는 디렉토리 경로
directory = result_path_strategy2

# 디렉토리 내의 모든 항목에 대해 반복
if __name__ == "__main__":
    for item in os.listdir(directory):
        item_path = os.path.join(directory, item)
        item_coint_path = os.path.join(item_path, "coint_pairs.csv")
        if os.path.isdir(item_path):
            run_backtest_for_all_rows(data_path, item_path, item_coint_path)