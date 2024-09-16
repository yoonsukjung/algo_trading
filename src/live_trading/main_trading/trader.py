# trader.py

import hmac
import hashlib
import urllib.parse
import requests
import time
import logging
from typing import List

class BinanceTrader:
    def __init__(self, api_key: str, api_secret: str, symbols: List[str]):
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.base_url = 'https://fapi.binance.com'
        self.symbols = symbols
        self.session = requests.Session()
        self.session.headers.update({'X-MBX-APIKEY': self.api_key})
        logging.info("BinanceTrader 초기화 완료")

    def _get_timestamp(self):
        return int(time.time() * 1000)

    def _sign(self, params: dict) -> str:
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(self.api_secret, query_string.encode(), hashlib.sha256).hexdigest()
        return signature

    def _send_request(self, method: str, endpoint: str, params: dict) -> dict:
        # Binance 요구 사항에 따라 X-MBX-APIKEY 헤더가 이미 설정됨
        params['timestamp'] = self._get_timestamp()
        params['signature'] = self._sign(params)
        url = self.base_url + endpoint
        try:
            if method == 'GET':
                response = self.session.get(url, params=params, timeout=10)
            elif method == 'POST':
                response = self.session.post(url, params=params, timeout=10)
            else:
                raise ValueError("Unsupported HTTP method")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error("API 요청 오류: %s", e)
            return {}

    def place_order(self, symbol: str, side: str, amount: float, order_type: str = 'MARKET'):
        """
        금액 기반으로 심볼에 대한 주문을 실행합니다.
        :param symbol: 거래할 심볼 (예: 'LDOUSDT')
        :param side: 'BUY' 또는 'SELL'
        :param amount: 주문할 금액 (USD 등)
        :param order_type: 주문 유형 (기본값은 'MARKET')
        """
        current_price = self.get_current_price(symbol)
        if current_price is None:
            logging.error("현재 가격을 가져올 수 없어 주문을 실행할 수 없습니다.")
            return

        quantity = amount / current_price
        quantity = self._adjust_quantity(symbol, quantity)

        if quantity <= 0:
            logging.error("계산된 수량이 0 이하입니다. 주문을 건너뜁니다.")
            return

        endpoint = '/fapi/v1/order'
        params = {
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'quantity': quantity,
            'recvWindow': 5000
        }
        response = self._send_request('POST', endpoint, params)
        if response:
            logging.info("주문 성공: %s %s %.6f", symbol, side, quantity)
            logging.debug("주문 응답: %s", response)
        else:
            logging.error("주문 실패: %s %s %.6f", symbol, side, quantity)

    def get_current_price(self, symbol: str) -> float:
        """
        심볼의 현재 가격을 가져옵니다.
        :param symbol: 심볼 (예: 'LDOUSDT')
        :return: 현재 가격 또는 None
        """
        endpoint = '/fapi/v1/ticker/price'
        params = {'symbol': symbol}
        response = self._send_request('GET', endpoint, params)
        if response and 'price' in response:
            return float(response['price'])
        logging.error("현재 가격을 가져오는 데 실패했습니다: %s", symbol)
        return None

    def _adjust_quantity(self, symbol: str, quantity: float) -> float:
        """
        Binance 규칙에 맞게 수량을 조정합니다 (소수점 제한 등).
        :param symbol: 심볼 (예: 'LDOUSDT')
        :param quantity: 원래 계산된 수량
        :return: 조정된 수량
        """
        endpoint = '/fapi/v1/exchangeInfo'
        params = {}
        response = self._send_request('GET', endpoint, params)
        if response and 'symbols' in response:
            for sym in response['symbols']:
                if sym['symbol'] == symbol:
                    for filter in sym['filters']:
                        if filter['filterType'] == 'LOT_SIZE':
                            step_size = float(filter['stepSize'])
                            min_qty = float(filter['minQty'])
                            max_qty = float(filter['maxQty'])
                            # 수량 제한에 맞게 반올림
                            quantity = max(min(quantity, max_qty), min_qty)
                            quantity = step_size * (int(quantity / step_size))
                            return quantity
        logging.error("LOT_SIZE 필터 정보를 가져오는 데 실패했습니다: %s", symbol)
        return 0.0

    def get_position_size(self, symbol: str) -> float:
        """
        현재 포지션 사이즈를 가져옵니다.
        :param symbol: 심볼 (예: 'LDOUSDT')
        :return: 포지션 수량
        """
        endpoint = '/fapi/v2/positionRisk'
        params = {}
        response = self._send_request('GET', endpoint, params)
        if response:
            for pos in response:
                if pos['symbol'] == symbol:
                    return float(pos['positionAmt'])
        return 0.0