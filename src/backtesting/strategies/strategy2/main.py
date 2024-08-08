import os
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from strategy import CointegrationStrategy
from backtester import Backtester
from src.strategies.utils import logger
from config import data_path, result_path_strategy2

logger = logger.setup_logging()

# 사용자가 쉽게 수정할 수 있는 경로 변수 설정
category_name = "Metaverse"
category_path = os.path.join(result_path_strategy2, category_name)
coint_file_path = os.path.join(category_path, "coint_pairs.csv")

def run_backtest_for_row(row_index, base_path, result_path, coint_file_path, category_name, entry_z_score, exit_z_score, stop_z_score):
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

        strategy = CointegrationStrategy(data, slope, crypto1, crypto2, spread_mean, spread_std, entry_z_score, exit_z_score, stop_z_score)
        strategy.categories = categories
        backtester = Backtester(strategy, start_date=start_date, end_date=end_date, fee=0.001, slippage=0.001, result_path=result_path, category_name=category_name)
        backtester.run_backtest()
    except Exception as e:
        logger.error(f"Error in the main execution: {e}")

if __name__ == "__main__":
    exit_z_scores = [0, 0.1, 0.2, 0.3, 0.4, 0.5]
    entry_z_scores = [1.0, 1.2, 1.4, 1.6, 1.8, 2.0]
    stop_z_scores = [2.5, 3.0, 3.5, 4.0]

    tasks = []
    with ProcessPoolExecutor(max_workers = 4) as executor:
        for exit_z_score in exit_z_scores:
            for entry_z_score in entry_z_scores:
                for stop_z_score in stop_z_scores:
                    logger.info(f"Running backtest with entry_z_score={entry_z_score}, exit_z_score={exit_z_score}, stop_z_score={stop_z_score}")
                    for item in os.listdir(result_path_strategy2):
                        item_path = os.path.join(result_path_strategy2, item)
                        item_coint_path = os.path.join(item_path, "coint_pairs.csv")
                        if os.path.isdir(item_path):
                            try:
                                file1 = pd.read_csv(item_coint_path)
                                num_rows = len(file1)
                                for row_index in range(num_rows):
                                    tasks.append(executor.submit(run_backtest_for_row, row_index, data_path, item_path, item_coint_path, category_name, entry_z_score, exit_z_score, stop_z_score))
                            except Exception as e:
                                logger.error(f"Error in running backtest for item: {item}, error: {e}")
        for future in as_completed(tasks):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error in future result: {e}")
