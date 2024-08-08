import yaml
from threading import Thread
from src.live_trading.testnet_trading.exchange.binance_trader import BinanceTrader
from src.live_trading.testnet_trading.utils.logger import setup_logger

logger = setup_logger()

def load_config(file_path: str) -> dict:
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def run_trader(api_key: str, secret_key: str, symbol1: str, symbol2: str, strategy_params: dict):
    trader = BinanceTrader(api_key, secret_key)
    trader.run_strategy(symbol1, symbol2, strategy_params)

if __name__ == "__main__":
    config = load_config('config.yaml')
    api_key = config['api_key']
    secret_key = config['secret_key']

    threads = []
    for pair in config['trading_pairs']:
        symbol1 = pair['symbol1']
        symbol2 = pair['symbol2']
        strategy_params = pair['strategy_params']
        thread = Thread(target=run_trader, args=(api_key, secret_key, symbol1, symbol2, strategy_params))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
