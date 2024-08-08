import ccxt
from src.live_trading.testnet_trading.strategy.cointegration_strategy import CointegrationStrategy


class BinanceTrader:
    def __init__(self, api_key: str, secret_key: str):
        self.exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': secret_key,
            'enableRateLimit': True,
            'options': {'defaultType': 'future'},  # Use 'future' for futures trading
            'urls': {
                'api': {
                    'public': 'https://testnet.binancefuture.com/fapi/v1',
                    'private': 'https://testnet.binancefuture.com/fapi/v1',
                }
            }
        })

    def run_strategy(self, symbol1: str, symbol2: str, strategy_params: dict):
        strategy = CointegrationStrategy(self.exchange, symbol1, symbol2, **strategy_params)
        strategy.run()
