import urllib.parse
import requests
import time
import hmac
import hashlib
import yaml


with open('../../../config.yaml', 'r') as file:
    config = yaml.safe_load(file)

api_key = config['api_key']
api_secret = config['api_secret']
base_url = config['base_url']
websocket_url = config['websocket_url']
slack_webhook_url = config['slack_webhook_url']

# 서명 생성 함수
def create_signature(params, secret):
    query_string = urllib.parse.urlencode(params)
    signature = hmac.new(secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
    return signature

def send_slack_message(message):
    payload = {
        "text": message
    }
    response = requests.post(slack_webhook_url, json=payload)
    return response.status_code

# 주문 생성 함수
def place_order(symbol, side, quantity, order_type="MARKET"):
    url = "https://testnet.binancefuture.com/fapi/v1/order"
    headers = {
        'X-MBX-APIKEY': api_key
    }
    params = {
        'symbol': symbol,
        'side': side,
        'type': order_type,
        'quantity': quantity,
        'timestamp': int(time.time() * 1000)
    }
    params['signature'] = create_signature(params, api_secret)
    response = requests.post(url, headers=headers, params=params)
    if response.status_code == 200:
        send_slack_message(f"Order placed: {side} {quantity} {symbol}")
    return response.json()


# BTCUSDT 페어에서 0.001 BTC 매수 주문
symbol = 'BTCUSDT'
side = 'SELL'
quantity = 0.01

response = place_order(symbol, side, quantity)
print(response)
order_id = response['orderId']