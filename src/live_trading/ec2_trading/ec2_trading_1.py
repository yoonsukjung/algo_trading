import pandas as pd
import json
import os
import datetime
import pytz
import ccxt
import websocket
import threading
import time
import logging
from logging.handlers import RotatingFileHandler
from functools import wraps
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ==========================
# 설정 및 환경 변수
# ==========================

# CSV 파일의 로컬 경로 설정
CSV_PATHS = os.getenv('CSV_PATHS', '/Users/yoonsukjung/Desktop/data/futures/1m/NEOUSDT_1m.csv,'
                                   '/Users/yoonsukjung/Desktop/data/futures/1m/ONTUSDT_1m.csv').split(',')  # 처리할 CSV 파일 목록
SYMBOLS = os.getenv('SYMBOLS', 'NEO/USDT,ONT/USDT').split(',')  # 각 CSV 파일에 대응하는 심볼
INTERVAL = '1m'  # 시간 간격 설정 (ccxt에서는 '1m' 형식으로 사용)

# API 설정 (ccxt 사용)
EXCHANGE_NAME = 'binanceusdm'  # Binance Futures (USDT-M) 사용
TIMEZONE = pytz.UTC  # 필요에 따라 변경

# ccxt 거래소 객체 초기화
exchange = getattr(ccxt, EXCHANGE_NAME)({
    'enableRateLimit': True,  # API 호출 시 rate limit 준수
})

# Slack 설정
SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL', 'C07GEFT13CK')
if not SLACK_BOT_TOKEN:
    raise EnvironmentError("SLACK_BOT_TOKEN 환경 변수가 설정되어 있지 않습니다.")

slack_client = WebClient(token=SLACK_BOT_TOKEN)

# ==========================
# 로깅 설정
# ==========================
logger = logging.getLogger('CryptoDataCollector')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# 콘솔 핸들러
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# 파일 핸들러 (로그 회전 설정)
file_handler = RotatingFileHandler('./crypto_data_collector.log', maxBytes=5 * 1024 * 1024, backupCount=5)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# ==========================
# Slack 메시지 전송 함수
# ==========================

def send_slack_message(message, level='INFO'):
    """
    Slack으로 메시지를 전송합니다.
    :param message: 전송할 메시지 내용
    :param level: 메시지의 중요도 (INFO, WARNING, ERROR, CRITICAL)
    """
    if level not in ['WARNING', 'ERROR', 'CRITICAL']:
        return  # INFO 레벨 메시지는 전송하지 않음

    formatted_message = f"*{level}*: {message}"

    try:
        response = slack_client.chat_postMessage(
            channel=SLACK_CHANNEL,
            text=formatted_message
        )
        if not response['ok']:
            logger.error(f"Slack 메시지 전송 실패: {response['error']}")
    except SlackApiError as e:
        logger.error(f"Slack 메시지 전송 중 오류 발생: {e.response['error']}")


# ==========================
# 유틸리티 함수
# ==========================

def retry(ExceptionToCheck, tries=3, delay=2, backoff=2, logger=None):
    """
    재시도 데코레이터
    :param ExceptionToCheck: 재시도할 Exception
    :param tries: 시도 횟수
    :param delay: 초기 지연 시간
    :param backoff: 지연 시간 배수
    :param logger: 로거 객체
    """

    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = f"{e}, 재시도 {tries - mtries + 1}/{tries} in {mdelay} seconds..."
                    if logger:
                        logger.warning(msg)
                        send_slack_message(msg, level='WARNING')  # Slack으로 경고 메시지 전송
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry

    return deco_retry


@retry(Exception, tries=5, delay=2, backoff=2, logger=logger)
def load_existing_data(file_path):
    try:
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            # 지정된 timestamp 형식으로 파싱
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
            info_msg = f"[{file_path}] 기존 데이터 로드 성공. 총 {len(df)} 행."
            logger.info(info_msg)
            send_slack_message(info_msg, level='INFO')
            return df
        else:
            # 파일이 존재하지 않을 경우 빈 DataFrame 생성
            columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            info_msg = f"[{file_path}] 파일이 존재하지 않아 빈 DataFrame을 생성합니다."
            logger.info(info_msg)
            send_slack_message(info_msg, level='INFO')
            return pd.DataFrame(columns=columns)
    except Exception as e:
        error_msg = f"[{file_path}] 데이터 로드 중 오류 발생: {str(e)}"
        logger.error(error_msg)
        send_slack_message(error_msg, level='ERROR')
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])


@retry(Exception, tries=5, delay=2, backoff=2, logger=logger)
def save_to_local(file_path, df):
    try:
        # timestamp를 지정된 형식으로 포맷팅
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df.to_csv(file_path, index=False)
        info_msg = f"[{file_path}] 로컬에 데이터 저장 완료. 총 {len(df)} 행."
        logger.info(info_msg)
        send_slack_message(info_msg, level='INFO')
    except Exception as e:
        error_msg = f"[{file_path}] 로컬에 저장 중 오류 발생: {str(e)}"
        logger.error(error_msg)
        send_slack_message(error_msg, level='ERROR')


@retry((ccxt.NetworkError, ccxt.ExchangeError), tries=5, delay=2, backoff=2, logger=logger)
def get_historical_data_via_ccxt(symbol, start_time, limit):
    """
    ccxt를 사용하여 과거 OHLCV 데이터를 가져옵니다.
    """
    try:
        since = exchange.parse8601(start_time.isoformat())  # ccxt의 parse8601 사용
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=INTERVAL, since=since, limit=limit)

        if not ohlcv:
            warning_msg = f"[{symbol}] ccxt로부터 데이터를 가져오지 못했습니다."
            logger.warning(warning_msg)
            send_slack_message(warning_msg, level='WARNING')
            return None

        parsed_data = []
        for entry in ohlcv:
            parsed_entry = {
                'timestamp': datetime.datetime.fromtimestamp(entry[0] / 1000, tz=TIMEZONE),
                'open': float(entry[1]),
                'high': float(entry[2]),
                'low': float(entry[3]),
                'close': float(entry[4]),
                'volume': float(entry[5])
            }
            parsed_data.append(parsed_entry)

        info_msg = f"[{symbol}] ccxt를 통해 {len(parsed_data)}개의 OHLCV 데이터를 가져왔습니다."
        logger.info(info_msg)
        send_slack_message(info_msg, level='INFO')
        return pd.DataFrame(parsed_data)

    except Exception as e:
        error_msg = f"[{symbol}] ccxt에서 데이터 가져오기 오류: {str(e)}"
        logger.error(error_msg)
        send_slack_message(error_msg, level='ERROR')
        return None


def get_latest_data_via_ws(symbol):
    """
    WebSocket을 통해 최신 1분 데이터를 가져옵니다.
    """
    latest_data_container = []

    # ccxt 심볼을 WebSocket URL 형식으로 변환 (예: 'BTC/USDT' -> 'btcusdt')
    ws_symbol = symbol.replace('/', '').lower()
    ws_url = f"wss://fstream.binance.com/ws/{ws_symbol}@kline_1m"

    def on_message(ws, message):
        try:
            msg = json.loads(message)
            kline = msg['k']
            if kline['x']:  # 캔들이 종료된 경우
                parsed_entry = {
                    'timestamp': datetime.datetime.fromtimestamp(kline['t'] / 1000, tz=TIMEZONE),
                    'open': float(kline['o']),
                    'high': float(kline['h']),
                    'low': float(kline['l']),
                    'close': float(kline['c']),
                    'volume': float(kline['v'])
                }
                latest_data_container.append(parsed_entry)
                ws.close()
        except Exception as e:
            error_msg = f"[{symbol}] WebSocket 메시지 처리 오류: {str(e)}"
            logger.error(error_msg)
            send_slack_message(error_msg, level='ERROR')
            ws.close()

    def on_error(ws, error):
        error_msg = f"[{symbol}] WebSocket 오류: {error}"
        logger.error(error_msg)
        send_slack_message(error_msg, level='ERROR')
        ws.close()

    def on_close(ws, close_status_code, close_msg):
        info_msg = f"[{symbol}] WebSocket 연결 종료."
        logger.info(info_msg)
        send_slack_message(info_msg, level='INFO')

    ws = websocket.WebSocketApp(ws_url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    # WebSocket을 별도의 스레드에서 실행
    wst = threading.Thread(target=ws.run_forever)
    wst.start()

    # 일정 시간 대기 (예: 10초)
    timeout = 10
    start_time = time.time()
    while wst.is_alive():
        if time.time() - start_time > timeout:
            ws.close()
            warning_msg = f"[{symbol}] WebSocket 타임아웃"
            logger.warning(warning_msg)
            send_slack_message(warning_msg, level='WARNING')
            break
        time.sleep(0.1)

    if latest_data_container:
        info_msg = f"[{symbol}] WebSocket을 통해 최신 1분 데이터 가져오기 성공."
        logger.info(info_msg)
        send_slack_message(info_msg, level='INFO')
        return pd.DataFrame([latest_data_container[-1]])
    else:
        warning_msg = f"[{symbol}] WebSocket으로부터 데이터를 받지 못했습니다."
        logger.warning(warning_msg)
        send_slack_message(warning_msg, level='WARNING')
        return None


def update_data_for_file(file_path, symbol):
    try:
        df = load_existing_data(file_path)
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        now = datetime.datetime.now(TIMEZONE).replace(second=0, microsecond=0)
        if not df.empty:
            last_timestamp = df['timestamp'].max()
            if last_timestamp.tzinfo is None:
                last_timestamp = last_timestamp.replace(tzinfo=TIMEZONE)
        else:
            last_timestamp = now - datetime.timedelta(minutes=1)  # 초기값 설정

        delta = now - last_timestamp
        missing_minutes = int(delta.total_seconds() / 60) - 1  # 현재 1분 데이터는 아직 도착 안 했을 수 있음

        info_msg = f"[{file_path}] 현재 시간: {now.strftime('%Y-%m-%d %H:%M:%S')}, 마지막 데이터 시간: {last_timestamp.strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(last_timestamp) else '없음'}, 누락된 분: {missing_minutes}"
        logger.info(info_msg)
        send_slack_message(info_msg, level='INFO')

        if missing_minutes > 0:
            historical_msg = f"[{file_path}] ccxt를 통해 {missing_minutes}개의 누락된 분 데이터를 가져옵니다."
            logger.info(historical_msg)
            send_slack_message(historical_msg, level='INFO')

            historical_data = get_historical_data_via_ccxt(symbol, last_timestamp + datetime.timedelta(minutes=1),
                                                           missing_minutes)
            if historical_data is not None:
                df = pd.concat([df, historical_data], ignore_index=True)
                success_msg = f"[{file_path}] 누락된 데이터 추가 완료. 총 행 수: {len(df)}"
                logger.info(success_msg)
                send_slack_message(success_msg, level='INFO')
            else:
                warning_msg = f"[{file_path}] 누락된 데이터를 가져오지 못했습니다."
                logger.warning(warning_msg)
                send_slack_message(warning_msg, level='WARNING')

        # 최신 1분 데이터 가져오기 (WebSocket)
        latest_msg = f"[{file_path}] WebSocket을 통해 최신 1분 데이터를 가져옵니다."
        logger.info(latest_msg)
        send_slack_message(latest_msg, level='INFO')

        latest_data = get_latest_data_via_ws(symbol)
        if latest_data is not None:
            df = pd.concat([df, latest_data], ignore_index=True)
            success_msg = f"[{file_path}] 최신 데이터 추가 완료. 총 행 수: {len(df)}"
            logger.info(success_msg)
            send_slack_message(success_msg, level='INFO')
        else:
            warning_msg = f"[{file_path}] 최신 데이터를 가져오지 못했습니다."
            logger.warning(warning_msg)
            send_slack_message(warning_msg, level='WARNING')

        # 중복 제거 및 정렬
        before_dedup = len(df)
        df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)
        after_dedup = len(df)
        if before_dedup != after_dedup:
            dedup_msg = f"[{file_path}] 중복 데이터를 제거하였습니다. {before_dedup} -> {after_dedup} 행."
            logger.info(dedup_msg)
            send_slack_message(dedup_msg, level='INFO')
        else:
            no_dedup_msg = f"[{file_path}] 중복 데이터가 발견되지 않았습니다."
            logger.info(no_dedup_msg)
            send_slack_message(no_dedup_msg, level='INFO')

        # 데이터 유효성 검사 (옵션)
        # 예: 가격이 음수가 아닌지 확인
        if not df[['open', 'high', 'low', 'close', 'volume']].ge(0).all().all():
            validation_warning = f"[{file_path}] 데이터 유효성 검사 실패: 음수 값이 존재합니다."
            logger.warning(validation_warning)
            send_slack_message(validation_warning, level='WARNING')
            df = df[df[['open', 'high', 'low', 'close', 'volume']].ge(0).all(axis=1)]

        # 로컬에 저장
        save_to_local(file_path, df)

    except Exception as e:
        error_msg = f"[{file_path}] 데이터 업데이트 중 오류 발생: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_slack_message(error_msg, level='ERROR')


def update_all_files():
    threads = []
    for file_path, symbol in zip(CSV_PATHS, SYMBOLS):
        thread = threading.Thread(target=update_data_for_file, args=(file_path, symbol))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


def main():
    while True:
        try:
            update_all_files()
        except Exception as e:
            critical_error = f"데이터 업데이트 중 치명적인 오류 발생: {str(e)}"
            logger.critical(critical_error, exc_info=True)
            send_slack_message(critical_error, level='CRITICAL')

        # 다음 업데이트까지 대기 (예: 60초)
        time.sleep(60)


if __name__ == "__main__":
    main()