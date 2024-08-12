import websocket
import json

class DataManager:
    def __init__(self, socket_url):
        self.socket_url = socket_url

    def on_message(self, ws, message):
        data = json.loads(message)
        print(data)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print("### closed ###")

    def on_open(self, ws):
        print("### opened ###")

    def start(self):
        ws = websocket.WebSocketApp(self.socket_url, on_message=self.on_message, on_error=self.on_error, on_close=self.on_close)
        ws.on_open = self.on_open
        ws.run_forever()