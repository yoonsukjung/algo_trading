import pandas as pd
import numpy as np
import os
import statsmodels.api as sm
from scipy.stats import skew, kurtosis
from statsmodels.tsa.stattools import coint
import logging
from config import data_path, result_path_strategy1

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Parameters
data_dir = data_path
output_dir = result_path_strategy1
start_date = '2023-01-01T00:00:00Z'
end_date = '2024-01-01T00:00:00Z'

# Load data
logging.info("Loading categorized_binance.csv")
categories_binance = pd.read_csv(os.path.join(output_dir, "categorized_binance.csv"))

# Time range
start_timestamp = pd.to_datetime(start_date, format='%Y-%m-%dT%H:%M:%SZ')
end_timestamp = pd.to_datetime(end_date, format='%Y-%m-%dT%H:%M:%SZ')
timestamps = pd.date_range(start=start_timestamp, end=end_timestamp, freq='1min')

# Function to find cointegrated pairs

def find_cointegrated_pairs(data, chunk_size=100):
    logging.info("Finding cointegrated pairs")
    n = data.shape[1]
    logging.info(f"Number of columns in data: {n}")
    pvalue_matrix = np.ones((n, n))
    keys = data.keys()
    pairs = []
    data_values = data.values  # 데이터 값을 미리 배열로 변환하여 접근 속도 향상

    # 청크 단위로 데이터 처리
    for start in range(0, n, chunk_size):
        end = min(start + chunk_size, n)
        for i in range(start, end):
            for j in range(i + 1, n):
                logging.info(f"Testing pair: ({keys[i]}, {keys[j]})")
                result = coint(data_values[:, i], data_values[:, j])
                pvalue_matrix[i, j] = result[1]
                logging.info(f"Tested pair: ({keys[i]}, {keys[j]}) - p-value: {result[1]}")
                if result[1] < 0.05:
                    pairs.append((keys[i], keys[j]))
                    logging.info(f"Cointegrated pair found: ({keys[i]}, {keys[j]})")

    return pvalue_matrix, pairs

# Function to calculate HR, correlation, spread mean, and spread std
def calculate_HR(data, pairs):
    logging.info("Calculating HR, correlation, spread mean, and spread std")
    log_data = np.log(data)
    results = []
    for crypto1, crypto2 in pairs:
        y = log_data[crypto1]
        x = log_data[crypto2]
        x = sm.add_constant(x)
        model = sm.OLS(y, x).fit()
        HR = model.params.iloc[1]
        correlation = np.corrcoef(log_data[crypto1], log_data[crypto2])[0, 1]

        # Calculate spread
        spread = y - HR * log_data[crypto2]
        spread_mean = spread.mean()
        spread_std = spread.std()

        results.append([crypto1, crypto2, HR, correlation, spread_mean, spread_std])
    df_results = pd.DataFrame(results, columns=['crypto1', 'crypto2', 'HR', 'correlation', 'spread_mean', 'spread_std'])
    return df_results

# Initialize the final DataFrame
all_results = pd.DataFrame()

# Iterate over each category column
for category_name in categories_binance.columns:
    logging.info(f"Processing category: {category_name}")
    category = categories_binance[category_name]
    category_top20 = category.head(20)
    target_category = category_top20

    # Initialize DataFrame for close prices
    close = pd.DataFrame(index=timestamps)
    symbols_list = []

    # Load price data
    for symbol in target_category:
        csv_file = os.path.join(data_dir, f"{symbol}_USDT_1m.csv")
        if not os.path.isfile(csv_file):
            logging.warning(f"Data for {symbol} not found.")
            continue
        data = pd.read_csv(csv_file, parse_dates=['timestamp'], index_col='timestamp')

        if data.index.min() > start_timestamp or data.index.max() < end_timestamp:
            logging.warning(f"Data for {symbol} does not cover the full date range from {start_date} to {end_date}.")
            continue
        data = data['close'].reindex(timestamps, method='nearest')

        close[symbol] = data
        symbols_list.append(symbol)

    # Filter normal symbols
    symbols_list_normal = []
    for symbol in symbols_list:
        skewness_value = skew(close[symbol])
        kurtosis_value = kurtosis(close[symbol], fisher=False)
        if -1.5 < skewness_value < 1.5 and -1 < kurtosis_value < 3:
            symbols_list_normal.append(symbol)

    close_normal = close[symbols_list_normal]

    # Calculate p-values and find cointegrated pairs
    pvalues, pairs = find_cointegrated_pairs(close_normal)

    # Calculate HR and correlation
    results_pairs = calculate_HR(close_normal, pairs)
    results_pairs['categories'] = category_name

    # Create directory for category if it doesn't exist
    category_output_dir = os.path.join(output_dir, category_name)
    os.makedirs(category_output_dir, exist_ok=True)

    # Save results to CSV
    output_file_path = os.path.join(category_output_dir, f"coint_pairs.csv")
    results_pairs.to_csv(output_file_path, index=False)
    logging.info(f"Results saved for category {category_name} in {output_file_path}")