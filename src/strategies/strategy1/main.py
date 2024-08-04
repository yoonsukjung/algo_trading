import os
import pandas as pd
import matplotlib.pyplot as plt
from strategy import CointegrationStrategy
from backtester import Backtester
from src.strategies.utils import logger
from config import data_path, result_path_strategy1

logger = logger.setup_logging()

# 사용자가 쉽게 수정할 수 있는 경로 변수 설정

category_path = os.path.join(result_path_strategy1, "Layer1_Layer2")
coint_file_path = os.path.join(category_path, "coint_pairs.csv")

def run_backtest_for_row(row_index, base_path, result_path, coint_file_path, entry_z_score, exit_z_score, stop_z_score):
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

        strategy = CointegrationStrategy(data, slope, crypto1, crypto2, entry_z_score, exit_z_score, stop_z_score)
        strategy.categories = categories
        backtester = Backtester(strategy, start_date=start_date, end_date=end_date, fee=0.001, slippage=0.001, result_path=result_path)
        backtester.run_backtest()

        if backtester.strategy.signals['z_score'].isna().all():
            logger.warning(f"All z_scores are NaN for row {row_index}")
            return None

        return backtester.performance_metrics  # 결과 반환
    except Exception as e:
        logger.error(f"Error in the main execution: {e}")
        return None

def run_backtest_for_all_rows(base_path, result_path, coint_file_path, entry_z_score, exit_z_score, stop_z_score):
    try:
        file1 = pd.read_csv(coint_file_path)
        num_rows = len(file1)

        all_results = []

        for row_index in range(num_rows):
            result = run_backtest_for_row(row_index, base_path, result_path, coint_file_path, entry_z_score, exit_z_score, stop_z_score)
            if result:
                result['entry_z_score'] = entry_z_score
                result['exit_z_score'] = exit_z_score
                result['stop_z_score'] = stop_z_score
                all_results.append(result)

        return all_results
    except Exception as e:
        logger.error(f"Error in running backtest for all rows: {e}")
        return []

def plot_combined_performance(results, result_path):
    try:
        fig, ax1 = plt.subplots(figsize=(14, 10))

        # Extract metrics
        entry_z_scores = [result['entry_z_score'] for result in results]
        exit_z_scores = [result['exit_z_score'] for result in results]
        stop_z_scores = [result['stop_z_score'] for result in results]
        total_returns = [result['total_return'] for result in results]
        annualized_volatility = [result['annualized_volatility'] for result in results]
        sharpe_ratios = [result['sharpe_ratio'] for result in results]

        # Plot total returns
        color = 'tab:blue'
        ax1.set_xlabel('Parameter Combinations')
        ax1.set_ylabel('Total Returns', color=color)
        ax1.plot(range(len(results)), total_returns, 'o', label='Total Returns', color=color)
        ax1.tick_params(axis='y', labelcolor=color)

        # Instantiate a second y-axis to plot volatility and sharpe ratio
        ax2 = ax1.twinx()
        color = 'tab:red'
        ax2.set_ylabel('Volatility & Sharpe Ratio', color=color)
        ax2.plot(range(len(results)), annualized_volatility, 'x', label='Volatility', color='tab:orange')
        ax2.plot(range(len(results)), sharpe_ratios, '^', label='Sharpe Ratio', color='tab:green')
        ax2.tick_params(axis='y', labelcolor=color)

        # Title and legends
        plt.title('Performance Metrics for Different Parameter Combinations')
        fig.tight_layout()
        fig.legend(loc='upper left', bbox_to_anchor=(0.1,0.9))

        # Save the plot as an image file in the result_path
        png_folder = os.path.join(result_path, 'combined_png')
        os.makedirs(png_folder, exist_ok=True)
        image_filename = os.path.join(png_folder, 'combined_performance.png')
        plt.savefig(image_filename)
        plt.close()
    except Exception as e:
        logger.error(f"Error in plotting combined performance: {e}")

if __name__ == "__main__":
    entry_z_scores = [1.0, 1.2, 1.4, 1.6, 1.8, 2.0]
    exit_z_scores = [0.1, 0.2, 0.3, 0.4, 0.5]
    stop_z_scores = [2.5, 3.0, 3.5, 4.0]

    all_results = []

    for entry_z_score in entry_z_scores:
        for exit_z_score in exit_z_scores:
            for stop_z_score in stop_z_scores:
                results = run_backtest_for_all_rows(data_path, category_path, coint_file_path, entry_z_score, exit_z_score, stop_z_score)
                if results:
                    all_results.extend(results)

    # Plot combined performance
    plot_combined_performance(all_results, category_path)
