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
        self.notifer = Notifier()

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
        params['signature'] = self.create_signature(params, api_secret)
        if price:
            params['price'] = price

        headers = {
            'X-MBX-APIKEY': self.api_key
        }

        response = requests.post(endpoint, headers=headers, params=params)
        order_response = response.json()

        self.notifer.send_message(f"Order response: {order_response}")

        return order_response

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

        # 주문 상태를 Slack으로 전송
        self.notifier.send_message(f"Order Status: {order_status}")

        return order_status