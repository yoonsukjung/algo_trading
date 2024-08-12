import requests
from config_test import api_key, api_secret, base_url

class PortfolioManager:
    def __init__(self):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url

    def get_account_info(self):
        endpoint = f"{self.base_url}/fapi/v1/account"
        headers = {
            'X-MBX-APIKEY': self.api_key
        }
        response = requests.get(endpoint, headers=headers)
        return response.json()