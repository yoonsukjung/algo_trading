import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def load_data(file1_path, base_path):
    logger.info("Loading data from file1")
    try:
        file1 = pd.read_csv(file1_path)
        first_row = file1.iloc[0]
        crypto1 = first_row['crypto1']
        crypto2 = first_row['crypto2']
        spread_mean = first_row['spread_mean']
        spread_std = first_row['spread_std']
        slope = first_row['HR']

        data1_path = os.path.join(base_path, f"{crypto1}_USDT_15m.csv")
        data2_path = os.path.join(base_path, f"{crypto2}_USDT_15m.csv")

        logger.info(f"Loading data1 from {data1_path}")
        if not os.path.exists(data1_path):
            raise FileNotFoundError(f"{data1_path} not found")
        data1 = pd.read_csv(data1_path)
        data1['timestamp'] = pd.to_datetime(data1['timestamp'])

        logger.info(f"Loading data2 from {data2_path}")
        if not os.path.exists(data2_path):
            raise FileNotFoundError(f"{data2_path} not found")
        data2 = pd.read_csv(data2_path)
        data2['timestamp'] = pd.to_datetime(data2['timestamp'])

        if 'close' not in data1.columns or 'close' not in data2.columns:
            raise ValueError("Both data1 and data2 must contain 'close' column")

        data = {'asset1': data1.set_index('timestamp'), 'asset2': data2.set_index('timestamp')}
        return data, spread_mean, spread_std, slope, crypto1, crypto2
    except Exception as e:
        logger.error(f"Error in loading data: {e}")
        raise
