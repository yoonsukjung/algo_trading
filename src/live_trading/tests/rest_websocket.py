import json
import requests
import time
import threading
import websocket
import hmac
import hashlib
import signal
import sys
import yaml


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


# 주문 상태 확인 함수 (REST API)
def check_order_status(order_id, symbol):
    url = f'https://testnet.binancefuture.com/fapi/v1/order'
    headers = {
        'X-MBX-APIKEY': api_key
    }
    timestamp = int(time.time() * 1000)
    params = {
        'symbol': symbol,
        'orderId': order_id,
        'timestamp': timestamp
    }

    # Query string 생성
    query_string = '&'.join([f"{key}={value}" for key, value in params.items()])

    # 서명 생성
    signature = hmac.new(api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    # 서명 추가
    params['signature'] = signature

    response = requests.get(url, headers=headers, params=params)
    return response.json()


# 메인 로직 실행 함수
def run_main_logic():
    # 여기에 메인 로직을 추가합니다.
    # 예시로 주문을 생성하고 상태를 확인하는 로직을 추가합니다.
    order_id = '123456789'  # 예시 주문 ID
    symbol = 'BTCUSDT'  # 예시 심볼

    # 주문 상태 확인
    while True:
        order_status = check_order_status(order_id, symbol)
        print(f"Order Status: {order_status}")

        if order_status['status'] == 'FILLED':
            # 주문이 체결된 경우 Slack 알림 전송
            slack_message = {
                "text": f"Order Filled: {order_id}\nSymbol: {symbol}\nStatus: {order_status['status']}"
            }
            requests.post(slack_webhook_url, json=slack_message)
            break

        time.sleep(5)  # 5초마다 상태 확인


# 종료 신호 처리 함수
def signal_handler(sig, frame):
    print('Exiting WebSocket...')
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    # WebSocket 스레드 시작
    websocket_thread = threading.Thread(target=run_websocket)
    websocket_thread.start()

    # 메인 로직 실행
    run_main_logic()