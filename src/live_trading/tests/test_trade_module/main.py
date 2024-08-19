from data_manager import DataManager
from order_manager import OrderManager
from portfolio_manager import PortfolioManager
from config_test import websocket_url

def main():
    order_manager = OrderManager()
    portfolio_manager = PortfolioManager()


    # 예제 주문 실행
    order_response = order_manager.place_order('BTCUSDT', 'BUY', 'MARKET', 0.005)
    print(order_response)

    # 계좌 정보 확인
    account_info = portfolio_manager.get_account_info()
    print(account_info)

if __name__ == "__main__":
    main()