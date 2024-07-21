# Necessary imports
import pandas as pd
import numpy as np
import os
import statsmodels.api as sm
from scipy.stats import skew, kurtosis
from statsmodels.tsa.stattools import coint

# Parameters
data_dir = '/Users/yoonsukjung/PycharmProjects/Trading/data/15m_2024-07-12'
output_file_path = '/Users/yoonsukjung/PycharmProjects/algo_trading/results/coint_pairs.csv'
start_date = '2023-01-01T00:00:00Z'
end_date = '2024-01-01T00:00:00Z'

# Load data
categories_binance = pd.read_csv("/Users/yoonsukjung/PycharmProjects/algo_trading/results/categorized_binance.csv")

# Time range
start_timestamp = pd.to_datetime(start_date, format='%Y-%m-%dT%H:%M:%SZ')
end_timestamp = pd.to_datetime(end_date, format='%Y-%m-%dT%H:%M:%SZ')
timestamps = pd.date_range(start=start_timestamp, end=end_timestamp, freq='15T')

# Function to find cointegrated pairs
def find_cointegrated_pairs(data):
    n = data.shape[1]
    pvalue_matrix = np.ones((n, n))
    keys = data.keys()
    pairs = []
    for i in range(n):
        for j in range(i + 1, n):
            result = coint(data[keys[i]], data[keys[j]])
            pvalue_matrix[i, j] = result[1]
            if result[1] < 0.05:
                pairs.append((keys[i], keys[j]))
    return pvalue_matrix, pairs

# Function to calculate HR, correlation, spread mean, and spread std
def calculate_HR(data, pairs):
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
    print(f"Processing category: {category_name}")
    category = categories_binance[category_name]
    category_top20 = category.head(20)
    target_category = category_top20

    # Initialize DataFrame for close prices
    close = pd.DataFrame(index=timestamps)
    symbols_list = []

    # Load price data
    for symbol in target_category:
        csv_file = os.path.join(data_dir, f"{symbol}_USDT_15m.csv")
        if not os.path.isfile(csv_file):
            print(f"Warning: Data for {symbol} not found.")
            continue
        data = pd.read_csv(csv_file, parse_dates=['timestamp'], index_col='timestamp')

        if data.index.min() > start_timestamp or data.index.max() < end_timestamp:
            print(f"Data for {symbol} does not cover the full date range from {start_date} to {end_date}.")
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

    # Calculate p-values and cointegrated pairs
    pvalues, pairs = find_cointegrated_pairs(close_normal)

    # Calculate HR, correlation, spread mean, and spread std for pairs
    results_pairs = calculate_HR(close_normal, pairs)
    results_pairs['categories'] = category_name

    # Append results to the final DataFrame
    all_results = pd.concat([all_results, results_pairs], ignore_index=True)

# Load existing data if available and append
if os.path.isfile(output_file_path):
    existing_data = pd.read_csv(output_file_path)
    all_results = pd.concat([existing_data, all_results], ignore_index=True)

# Save to CSV
all_results.to_csv(output_file_path, index=False)
print(all_results)
