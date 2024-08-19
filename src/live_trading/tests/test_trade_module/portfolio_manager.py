import requests
import time
from config_test import api_key, api_secret, base_url
from order_manager import OrderManager

class PortfolioManager:
    def __init__(self):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.order_manager = OrderManager()

    def get_account_info(self):
        endpoint = f"{self.base_url}/fapi/v2/account"
        headers = {
            'X-MBX-APIKEY': self.api_key
        }
        params = {
            'timestamp': int(time.time() * 1000)
        }
        params['signature'] = self.order_manager.create_signature(params, api_secret)


        response = requests.get(endpoint, headers=headers, params=params)
        return response.json()