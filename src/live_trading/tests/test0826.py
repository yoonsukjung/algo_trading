import asyncio
import websockets
import json
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta


# Function to fetch historical data
def fetch_historical_data(symbol, interval, limit, end_time=None):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    if end_time:
        url += f"&endTime={int(end_time.timestamp() * 1000)}"
    response = requests.get(url)
    data = response.json()
    historical_data = []
    for entry in data:
        close_time = datetime.fromtimestamp(entry[6] / 1000, tz=timezone.utc) + timedelta(seconds=1)
        close_time_str = close_time.strftime('%Y-%m-%d %H:%M:%S')
        close_price = entry[4]
        historical_data.append([close_time_str, symbol, close_price])
    return historical_data


# Fetch historical data
symbol = "BTCUSDT"
interval = "1m"
limit = 40000
historical_data = fetch_historical_data(symbol, interval, limit)

# Create initial DataFrame
df = pd.DataFrame(historical_data, columns=['Close Time', 'Symbol', 'Close Price'])
df['Close Price'] = df['Close Price'].astype(float)

# Get the last close time from historical data
last_historical_close_time = datetime.strptime(df['Close Time'].iloc[-1], '%Y-%m-%d %H:%M:%S').replace(
    tzinfo=timezone.utc)


async def listen():
    url = "wss://stream.binancefuture.com/ws/btcusdt@kline_1m"
    last_close_time = None
    first_websocket_data = True

    async with websockets.connect(url) as websocket:
        while True:
            response = await websocket.recv()
            data = json.loads(response)

            # Extract relevant information
            symbol = data['s']
            close_time = data['k']['T']
            close_price = data['k']['c']

            # Convert close time to human-readable format and add 1 second
            close_time_human = datetime.fromtimestamp(close_time / 1000, tz=timezone.utc) + timedelta(seconds=1)
            close_time_human_str = close_time_human.strftime('%Y-%m-%d %H:%M:%S')

            # Check if this is the first WebSocket data
            if first_websocket_data:
                first_websocket_data = False
                # Check for missing data between last historical data and first WebSocket data
                if close_time_human > last_historical_close_time + timedelta(minutes=1):
                    missing_data = fetch_historical_data(symbol, interval, limit,
                                                         end_time=close_time_human - timedelta(minutes=1))
                    missing_df = pd.DataFrame(missing_data, columns=['Close Time', 'Symbol', 'Close Price'])
                    missing_df['Close Price'] = missing_df['Close Price'].astype(float)
                    global df
                    df = pd.concat([df, missing_df], ignore_index=True)

            # Check for duplicate messages
            if close_time == last_close_time:
                continue

            # Update last close time
            last_close_time = close_time

            # Append data to DataFrame
            new_data = pd.DataFrame([[close_time_human_str, symbol, float(close_price)]],
                                    columns=['Close Time', 'Symbol', 'Close Price'])
            df = pd.concat([df, new_data], ignore_index=True)

            # Calculate rolling mean and std if we have enough data
            if len(df) >= 100:
                df['Rolling Mean'] = df['Close Price'].rolling(window=40000).mean()
                df['Rolling Std'] = df['Close Price'].rolling(window=40000).std()

            # Print the DataFrame
            print(df.tail(5))  # Print the last 5 rows for brevity


asyncio.get_event_loop().run_until_complete(listen())