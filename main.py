import websocket
import json
import requests
import pandas as pd
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TIME_TO_TELEGRAM_SEND = 10 * 60
FUNDING_RATE_TO_TELEGRAM_SEND = 1.5

TOP_AMOUNT = 3

sent_notifications = {}

def send_telegram_message(message, symbol):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        print(f"✅ Telegram message sent: {symbol}\n")
    else:
        print(f"⛔️ Error sending telegram message: {response.text}")

def on_message(_, message):
    global sent_notifications
    data = json.loads(message)
    if isinstance(data, list):
        processed_data = []
        current_time = datetime.now(timezone.utc)

        for item in data:
            symbol = item['s']
            funding_rate = float(item['r']) * 100

            next_funding_time = datetime.fromtimestamp(item['T'] / 1000, tz=timezone.utc)
            time_remaining = next_funding_time - current_time

            if time_remaining.total_seconds() < 2 * 3600:
                time_remaining_str = str(time_remaining).split(".")[0]
                processed_data.append({
                    "Symbol": symbol,
                    "Funding Rate": funding_rate,
                    "Next Funding": time_remaining_str
                })

                if time_remaining.total_seconds() < TIME_TO_TELEGRAM_SEND and abs(funding_rate) > FUNDING_RATE_TO_TELEGRAM_SEND:
                    if symbol not in sent_notifications:
                        direction_icon = "🔼" if funding_rate > 0 else "🔽"
                        message = (
                            f"⚠️ <b>{symbol}</b>\n"
                            f"{direction_icon} Funding Rate: <b><u>{funding_rate:.2f}%</u></b>\n"
                            f"⏰ Next Funding in: <b>{time_remaining_str}</b>"
                        )
                        send_telegram_message(message, symbol)
                        sent_notifications[symbol] = True

            if time_remaining.total_seconds() <= 0:
                sent_notifications.pop(symbol, None)

        sorted_data = sorted(processed_data, key=lambda x: abs(x['Funding Rate']), reverse=True)

        top_amount = sorted_data[:TOP_AMOUNT]

        if top_amount:
            df = pd.DataFrame(top_amount)
            print(df.to_string(index=False))
    else:
        print("Wrong data format:", data)


if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        "wss://fstream.binance.com/ws/!markPrice@arr",
        on_message=on_message,
    )
    ws.run_forever()