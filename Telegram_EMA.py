import os
import requests
bot_token = os.environ.get("TELEGRAM_BOT_TOKEN_21_EMA")
chat_id = os.environ.get("TELEGRAM_CHAT_ID_21_EMA") #

def Send_EMA_Telegram_Message(message):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,  # Escape special characters like < > &
    }
    try:
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            print("✅ Message sent successfully")
        else:
            print("⚠️ Failed to send message:", response.text)
    except Exception as e:
        print("⚠️ Error:", e)
