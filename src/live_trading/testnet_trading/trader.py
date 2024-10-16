import pandas as pd
import numpy as np
import time
import logging
from binance.client import Client
from binance.exceptions import BinanceAPIException

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

# Binance Testnet API 키와 시크릿 설정
API_KEY = 'c9bf6be04e128c777ce42cec916e2a13f86a4ff8f7a96d72f7b22228f56678c9'
API_SECRET = '28b16206a64c17f060171dda13736a23d941e653778110ac7106ddf31332163b'

# Binance Testnet 클라이언트 설정
client = Client(API_KEY, API_SECRET)
client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'


class Trader:
    def __init__(self, client):
        self.client = client
        self.positions = {}  # 각 심볼별 포지션을 저장하는 딕셔너리

    def place_order(self, symbol, side, amount):
        """
        Binance API를 사용하여 주문을 실행합니다.
        amount는 해당 거래에 사용할 총 자산(USDT) 금액으로 가정합니다.
        """
        try:
            # 현재 가격 조회
            price = self.get_current_price(symbol)
            if price is None:
                logging.error(f"{symbol}의 현재 가격을 가져올 수 없습니다. 주문이 실행되지 않았습니다.")
                return

            # 심볼 정보 가져오기 (최소 주문 수량 및 단위)
            symbol_info = self.client.futures_exchange_info()
            symbol_filters = next((item for item in symbol_info['symbols'] if item["symbol"] == symbol), None)
            if symbol_filters is None:
                logging.error(f"{symbol}의 정보를 가져올 수 없습니다. 주문이 실행되지 않았습니다.")
                return

            # LOT_SIZE 필터 찾기
            lot_size_filter = next((f for f in symbol_filters['filters'] if f['filterType'] == 'LOT_SIZE'), None)
            if lot_size_filter is None:
                logging.error(f"{symbol}의 LOT_SIZE 필터를 찾을 수 없습니다. 주문이 실행되지 않았습니다.")
                return

            step_size = float(lot_size_filter['stepSize'])
            min_qty = float(lot_size_filter['minQty'])

            # PRICE_FILTER 필터 찾기
            price_filter = next((f for f in symbol_filters['filters'] if f['filterType'] == 'PRICE_FILTER'), None)
            if price_filter is None:
                logging.error(f"{symbol}의 PRICE_FILTER를 찾을 수 없습니다. 주문이 실행되지 않았습니다.")
                return

            tick_size = float(price_filter['tickSize'])

            # 수량 계산
            quantity = amount / price

            # 수량 및 가격 조정
            quantity_precision = int(round(-np.log10(step_size)))
            price_precision = int(round(-np.log10(tick_size)))

            quantity = round(quantity, quantity_precision)
            price = round(price, price_precision)

            if quantity < min_qty:
                logging.error(f"수량 {quantity}이 최소 거래 수량 {min_qty}보다 적습니다. 주문이 실행되지 않았습니다.")
                return

            # 시장가 주문 실행
            order = self.client.futures_create_order(
                symbol=symbol,
                side=side,
                type='MARKET',
                quantity=quantity
            )
            logging.info(f"주문이 실행되었습니다: {order}")

            # 포지션 크기 업데이트
            if side == 'BUY':
                self.positions[symbol] = self.positions.get(symbol, 0) + quantity
            elif side == 'SELL':
                self.positions[symbol] = self.positions.get(symbol, 0) - quantity

        except BinanceAPIException as e:
            logging.error(f"Binance API 예외 발생: {e.message}")
        except Exception as e:
            logging.error(f"예외가 발생했습니다: {e}")

    def get_position_size(self, symbol):
        """
        해당 심볼의 현재 포지션 크기를 반환합니다.
        """
        return self.positions.get(symbol, 0.0)

    def get_current_price(self, symbol):
        """
        해당 심볼의 현재 가격을 가져옵니다.
        """
        try:
            ticker = self.client.futures_symbol_ticker(symbol=symbol)
            return float(ticker['price'])
        except BinanceAPIException as e:
            logging.error(f"Binance API 예외 발생: {e.message}")
            return None
        except Exception as e:
            logging.error(f"예외가 발생했습니다: {e}")
            return None


class TradingBot:
    def __init__(self, trader, symbols, stop_loss_threshold=3.0):
        self.trader = trader
        self.symbols = symbols
        self.STOP_LOSS_THRESHOLD = stop_loss_threshold
        self.position_open = False  # 포지션이 열려있는지 여부
        self.lockout = False  # 잠금 상태 여부

    def close_all_positions(self, timestamp: pd.Timestamp):
        """
        현재 열린 모든 포지션을 청산합니다.
        :param timestamp: 데이터 타임스탬프
        """
        logging.info(f"[{timestamp}] 모든 포지션 청산 시작")
        for symbol in self.symbols:
            position_size = self.trader.get_position_size(symbol)
            if position_size != 0:
                side = 'SELL' if position_size > 0 else 'BUY'
                current_price = self.trader.get_current_price(symbol)
                if current_price is not None:
                    amount = abs(position_size) * current_price
                    self.trader.place_order(symbol, side, amount=amount)
                    logging.info(f"[{timestamp}] {symbol} - {side} - 금액: {amount}")
        self.position_open = False
        logging.info(f"[{timestamp}] 모든 포지션 청산 완료")

    def execute_trading_logic(self, timestamp: pd.Timestamp, z_score: float):
        """
        z_score에 따라 매매 조건을 평가하고 주문을 실행합니다.
        :param timestamp: 데이터 타임스탬프
        :param z_score: 계산된 z-score
        """
        # 매매 조건 설정
        ENTRY_THRESHOLD = 1.5
        EXIT_THRESHOLD = 0.2
        STOP_LOSS_THRESHOLD = self.STOP_LOSS_THRESHOLD

        abs_z = abs(z_score)

        # 로그에 타임스탬프 추가
        logging.info(f"[{timestamp}] Evaluating z_score: {z_score}")

        # 잠금 상태인 경우
        if self.lockout:
            if abs_z < EXIT_THRESHOLD:
                self.lockout = False
                logging.info(f"[{timestamp}] |z_score| < {EXIT_THRESHOLD}: 잠금 상태 해제")
            else:
                logging.info(f"[{timestamp}] 잠금 상태 유지 |z_score|: {z_score}")
            return  # 잠금 상태일 때는 추가적인 행동 불가

        # 포지션이 열려있는 경우
        if self.position_open:
            # 강제 포지션 청산 조건
            if abs_z > STOP_LOSS_THRESHOLD:
                logging.info(
                    f"[{timestamp}] |z_score| {z_score} > STOP_LOSS_THRESHOLD {STOP_LOSS_THRESHOLD}: 손절매 트리거 - 포지션 청산")
                self.close_all_positions(timestamp)
                self.lockout = True  # 강제 청산 후 잠금 상태 설정
                return

            # 일반 포지션 청산 조건
            if abs_z < EXIT_THRESHOLD:
                logging.info(f"[{timestamp}] |z_score| {z_score} < EXIT_THRESHOLD {EXIT_THRESHOLD}: 포지션 청산")
                self.close_all_positions(timestamp)
                return

            # 포지션 유지
            logging.info(f"[{timestamp}] 포지션 유지 |z_score|: {z_score}")
            return

        # 포지션이 열려있지 않고 잠금 상태도 아닌 경우
        if abs_z > STOP_LOSS_THRESHOLD:
            # 포지션이 열려있지 않을 때 |z| > STOP_LOSS_THRESHOLD 이면 포지션을 열지 않음
            logging.info(f"[{timestamp}] |z_score| {z_score} > STOP_LOSS_THRESHOLD {STOP_LOSS_THRESHOLD} (포지션 미오픈)")
            return

        if abs_z > ENTRY_THRESHOLD:
            # 포지션 열기
            if z_score > ENTRY_THRESHOLD:
                logging.info(f"[{timestamp}] z_score {z_score} > ENTRY_THRESHOLD {ENTRY_THRESHOLD}: 포지션 진입 (매도)")
                self.trader.place_order(self.symbols[0], 'SELL', amount=1000.0)
                self.trader.place_order(self.symbols[1], 'BUY', amount=1000.0)
            elif z_score < -ENTRY_THRESHOLD:
                logging.info(f"[{timestamp}] z_score {z_score} < -ENTRY_THRESHOLD {-ENTRY_THRESHOLD}: 포지션 진입 (매수)")
                self.trader.place_order(self.symbols[0], 'BUY', amount=1000.0)
                self.trader.place_order(self.symbols[1], 'SELL', amount=1000.0)
            self.position_open = True
            return

        # z_score가 모든 조건에 해당하지 않을 경우
        logging.info(f"[{timestamp}] |z_score| {z_score} 이 모든 조건에 해당하지 않음: 아무 행동도 취하지 않음")
