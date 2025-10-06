
import os
import requests
bot_token = "8449258086:AAEwiLWhaNGqzHBqLMP51y5AFnqXi4FYOxk"
chat_id = -1003024572760 

def Telegram_Alert_EMA_Crossover(message):
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




