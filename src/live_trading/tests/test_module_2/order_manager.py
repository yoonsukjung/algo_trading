import json
import requests
import time
import urllib.parse
import hmac
import hashlib
from config_test import api_key, api_secret, base_url
from notifier import Notifier

class OrderManager:
    def __init__(self):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.notifier = Notifier()

    def create_signature(self, params, secret):
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
        return signature

    def place_order(self, symbol, side, order_type, quantity, price=None):
        endpoint = f"{self.base_url}/fapi/v1/order"
        params = {
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'quantity': quantity,
            'timestamp': int(time.time() * 1000)
        }
        if price:
            params['price'] = price
        params['signature'] = self.create_signature(params, self.api_secret)

        headers = {
            'X-MBX-APIKEY': self.api_key
        }

        def place_order(self, symbol, side, order_type, quantity, price=None, max_retries=3):
            endpoint = f"{self.base_url}/fapi/v1/order"
            params = {
                'symbol': symbol,
                'side': side,
                'type': order_type,
                'quantity': quantity,
                'timestamp': int(time.time() * 1000)
            }
            if price:
                params['price'] = price
            params['signature'] = self.create_signature(params, self.api_secret)

            headers = {
                'X-MBX-APIKEY': self.api_key
            }

            # Error handling
            for attempt in range(max_retries):
                try:
                    response = requests.post(endpoint, headers=headers, params=params)
                    response.raise_for_status()  # Raise an HTTPError for bad responses
                    order_response = response.json()
                    self.notifier.send_message(f"Order response: {json.dumps(order_response, indent=4)}")
                    return order_response
                except requests.exceptions.RequestException as e:
                    self.notifier.send_message(f"Error placing order: {e}")
                    if attempt < max_retries - 1:
                        self.notifier.send_message(f"Retrying... ({attempt + 1}/{max_retries})")
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        self.notifier.send_message("Max retries reached. Order failed.")
                        return None

    def get_order_status(self, symbol, order_id):
        endpoint = f"{self.base_url}/fapi/v1/order"
        params = {
            'symbol': symbol,
            'orderId': order_id,
            'timestamp': int(time.time() * 1000)
        }

        headers = {
            'X-MBX-APIKEY': self.api_key
        }

        response = requests.get(endpoint, headers=headers, params=params)
        order_status = response.json()

        self.notifier.send_message(f"Order Status: {order_status}")

        return order_status

    def enter_position(self, symbol, quantity, price=None):
        return self.place_order(symbol, 'BUY', 'LIMIT' if price else 'MARKET', quantity, price)

    def exit_position(self, symbol, quantity, price=None):
        return self.place_order(symbol, 'SELL', 'LIMIT' if price else 'MARKET', quantity, price)

# Example usage:
# order_manager = OrderManager()
# order_manager.enter_position('BTCUSDT', 0.001, 50000)
# order_manager.exit_position('BTCUSDT', 0.001)