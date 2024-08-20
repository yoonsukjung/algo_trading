from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from config_test import slack_token, slack_channel

class Notifier:
    def __init__(self):
        self.client = WebClient(token=slack_token)
        self.channel = slack_channel

    def send_message(self, message):
        try:
            response = self.client.chat_postMessage(
                channel=self.channel,
                text=message
            )
            print(f"Message sent to Slack: {message}")
        except SlackApiError as e:
            print(f"Error sending message to Slack: {e.response['error']}")