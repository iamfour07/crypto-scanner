import os
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from Telegram_Alert import send_telegram_message
import html
import hmac
import hashlib
import json
import time

# ====== API Credentials ======
key = "xxx"
secret = "yyy"
secret_bytes = bytes(secret, encoding='utf-8')

# ====== Settings ======
resolution = "60"   # candle resolution = 1 hour
limit_hours = 1000  # history length
EMA_PERIOD = 45     # 45 EMA
IST = timezone(timedelta(hours=5, minutes=30))

# ==============================
# AUTH SIGNATURE
# ==============================
def get_signature(body):
    json_body = json.dumps(body, separators=(',', ':'))
    signature = hmac.new(secret_bytes, json_body.encode(), hashlib.sha256).hexdigest()
    return signature, json_body

# ==============================
# Fetch active USDT futures coins
# ==============================
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error fetching active USDT coins: {e}")
        return []

# ==============================
# GET 1D % CHANGE
# ==============================
def fetch_coin_data(pair):
    timeStamp = int(round(time.time() * 1000))
    body = {"timestamp": timeStamp}
    signature, json_body = get_signature(body)

    url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": key,
        "X-AUTH-SIGNATURE": signature
    }

    try:
        response = requests.get(url, data=json_body, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching stats for {pair}: {e}")
        return None

# ==============================
# FETCH OHLCV DATA + EMA LOGIC
# ==============================
def fetch_candles(pair):
    now = int(time.time())
    from_time = now - limit_hours * 3600

    url = "https://public.coindcx.com/market_data/candlesticks"
    query_params = {
        "pair": pair,
        "from": from_time,
        "to": now,
        "resolution": resolution,
        "pcode": "f"
    }

    try:
        resp = requests.get(url, params=query_params)
        resp.raise_for_status()
        data = resp.json()["data"]

        if not data or len(data) < EMA_PERIOD + 2:
            return None

        df = pd.DataFrame(data)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Calculate EMA
        df['ema_45'] = df['close'].ewm(span=EMA_PERIOD, adjust=False).mean()

        last_candle = df.iloc[-2]  # current-1
        prev_candles = df.iloc[:-2] # all previous candles before last closed

        # First-time below EMA logic
        first_time_below_ema = last_candle['close'] < last_candle['ema_45'] and all(prev_candles['close'] >= prev_candles['ema_45'])
        
        return {
            "pair": pair,
            "close": float(last_candle["close"]),
            "high": float(last_candle["high"]),
            "low": float(last_candle["low"]),
            "volume": float(last_candle["volume"]),
            "time": datetime.fromtimestamp(last_candle["time"]/1000, tz=timezone.utc)
                    .astimezone(IST).strftime("%Y-%m-%d %I:%M:%S %p"),
            "ema_45": float(last_candle['ema_45']),
            "first_time_below_ema": first_time_below_ema
        }
    

    except Exception as e:
        print(f"Error fetching candles for {pair}: {e}")
        return None

# ==============================
# MAIN RUNNER
# ==============================
print("\n=== SHORT SELL OPPORTUNITIES ===\n")

CoinList = get_active_usdt_coins()
filtered_coins = []

# Step 1: Filter coins with 1D gain >=10%
for coin in CoinList:
    stats = fetch_coin_data(coin)
    if stats and "price_change_percent" in stats:
        change_1d = stats["price_change_percent"].get("1D", None)
        if change_1d is not None and change_1d >= 10:
            filtered_coins.append(coin)

# Step 2: Fetch OHLCV and check first-time close below 45 EMA
sell_signals = []

for pair in filtered_coins:
    result = fetch_candles(pair)
    if result and result["first_time_below_ema"]:
        sell_signals.append(result)

# Step 3: Send Telegram if any signals
if sell_signals:
    message_lines = ["📊 Short-Sell Opportunities (First-time below 45 EMA):\n"]
    for res in sell_signals:
        pair_safe = html.escape(res['pair'])
        link = f"https://coindcx.com/futures/{res['pair']}"
        message_lines.append(
            f"{pair_safe}\nEntry (Low): {res['low']}\nStop-loss (High): {res['high']}\n"
            f"Close: {res['close']} | EMA45: {res['ema_45']} | Volume: {res['volume']}\n{link}\n"
        )
           
    message_lines.append("\n===============================")
    final_message = "\n".join(message_lines)
    print(final_message)
    send_telegram_message(final_message)
