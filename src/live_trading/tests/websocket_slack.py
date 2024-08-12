import json
import requests
import time
import threading
import websocket
import yaml
import sys
import signal


with open('../../../config.yaml', 'r') as file:
    config = yaml.safe_load(file)

api_key = config['api_key']
api_secret = config['api_secret']
base_url = config['base_url']
websocket_url = config['websocket_url']
slack_webhook_url = config['slack_webhook_url']

# Listen Key 생성 함수
def get_listen_key():
    url = 'https://testnet.binancefuture.com/fapi/v1/listenKey'
    headers = {
        'X-MBX-APIKEY': api_key
    }
    response = requests.post(url, headers=headers)
    return response.json()['listenKey']


# Listen Key 갱신 함수 (30분마다 호출 필요)
def keep_alive_listen_key(listen_key):
    url = f'https://testnet.binancefuture.com/fapi/v1/listenKey'
    headers = {
        'X-MBX-APIKEY': api_key
    }
    data = {
        'listenKey': listen_key
    }
    response = requests.put(url, headers=headers, data=data)
    return response.status_code == 200


# Listen Key 생성
listen_key = get_listen_key()
print(f"Listen Key: {listen_key}")


# WebSocket 메시지 처리 함수
def on_message(ws, message):
    data = json.loads(message)
    if data['e'] == 'ORDER_TRADE_UPDATE' and data['o']['X'] == 'FILLED':
        order_id = data['o']['i']
        symbol = data['o']['s']
        side = data['o']['S']
        price = data['o']['p']
        quantity = data['o']['q']

        # Slack 알림 메시지 생성
        slack_message = {
            "text": f"Order Filled: {order_id}\nSymbol: {symbol}\nSide: {side}\nPrice: {price}\nQuantity: {quantity}"
        }

        # Slack Webhook으로 알림 전송
        requests.post(slack_webhook_url, json=slack_message)


# WebSocket 열기
def on_open(ws):
    print("WebSocket opened")


# WebSocket 에러 처리
def on_error(ws, error):
    print(f"WebSocket error: {error}")


# WebSocket 종료 처리
def on_close(ws):
    print("WebSocket closed")


# WebSocket 실행 함수
def run_websocket():
    listen_key = get_listen_key()
    ws_url = f"wss://stream.binancefuture.com/ws/{listen_key}"
    ws = websocket.WebSocketApp(ws_url, on_message=on_message, on_open=on_open, on_error=on_error, on_close=on_close)

    # Listen Key 갱신 스레드
    def keep_alive():
        while True:
            time.sleep(1800)  # 30분마다 갱신
            keep_alive_listen_key(listen_key)

    threading.Thread(target=keep_alive).start()
    ws.run_forever()

def signal_handler(sig, frame):
    print('Exiting WebSocket...')
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    run_websocket()
