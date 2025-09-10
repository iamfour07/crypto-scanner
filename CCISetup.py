import os
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from CCI_Calculater import calculate_cci
from Telegram_Alert import send_telegram_message
import html

# --- Configuration ---
resolution = "60"
limit_hours = 1000  # reduce payload
IST = timezone(timedelta(hours=5, minutes=30))

# --- RSI & CCI Settings ---
RSI_PERIOD = 21
RSI_THRESHOLD = 50
CCI_PERIOD = 200
CCI_UPPER = 200
CCI_LOWER = -200
PREV_CANDLES = 5  # number of previous candles to check for false signal

# --- Fetch active USDT futures coins ---
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching coins: {e}")
        return []

CoinList = get_active_usdt_coins()

# --- Helper Functions ---
def calculate_rsi(close_prices, period=100):
    delta = close_prices.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# --- Core function ---
def fetch_and_check(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - limit_hours * 3600

    url = "https://public.coindcx.com/market_data/candlesticks"
    params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()['data']

        df = pd.DataFrame(data)
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['volume'] = df['volume'].astype(float)

        # last closed candle
        last_candle = df.iloc[-2]

        # Calculate RSI & CCI
        df['rsi'] = calculate_rsi(df['close'], RSI_PERIOD)
        df['cci'] = calculate_cci(df['high'], df['low'], df['close'], period=CCI_PERIOD)

        last_rsi = df['rsi'].iloc[-2]
        last_cci = df['cci'].iloc[-2]

        # previous N candles for false signal check
        prev_cci_window = df['cci'].iloc[-(PREV_CANDLES+2):-2]

        signal_type = None

        # BUY: First time CCI crosses above 200 and RSI > 50
        if last_cci > CCI_UPPER and last_rsi > RSI_THRESHOLD:
            if all(prev_cci_window <= CCI_UPPER):
                signal_type = "BUY"

        # SELL: First time CCI crosses below -200 and RSI < 50
        elif last_cci < CCI_LOWER and last_rsi < RSI_THRESHOLD:
            if all(prev_cci_window >= CCI_LOWER):
                signal_type = "SELL"

        if signal_type:
            return {
                "pair": pair,
                "close": last_candle['close'],
                "high": last_candle['high'],
                "low": last_candle['low'],
                "volume": last_candle['volume'],
                "rsi": round(last_rsi, 2),
                "cci": round(last_cci, 2),
                "signal": signal_type
            }

    except Exception as e:
        print(f"Error fetching {pair}: {e}")

    return None

# --- Scan all coins ---
buy_signals, sell_signals = [], []

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(fetch_and_check, coin): coin for coin in CoinList}
    for future in as_completed(futures):
        result = future.result()
        if result:
            if result['signal'] == "BUY":
                buy_signals.append(result)
            elif result['signal'] == "SELL":
                sell_signals.append(result)

# --- Sort by Volume ---
buy_signals.sort(key=lambda x: x['volume'], reverse=True)
sell_signals.sort(key=lambda x: x['volume'], reverse=True)

# --- Prepare Telegram message ---
if buy_signals or sell_signals:
    message_lines = ["📊 Hourly Crypto Scan Results\n"]

    if buy_signals:
        message_lines.append("🟢 BUY Signals:\n")
        for res in buy_signals:
            pair_safe = html.escape(res['pair'])
            link = f"https://coindcx.com/futures/{res['pair']}"
            message_lines.append(f"{pair_safe} \nClose: {res['close']}\nVolume: {res['volume']}\nCCI: {res['cci']} \nRSI: {res['rsi']}\n{link}\n")

    if sell_signals:
        message_lines.append("\n🔴 SELL Signals:\n")
        for res in sell_signals:
            pair_safe = html.escape(res['pair'])
            link = f"https://coindcx.com/futures/{res['pair']}"
            message_lines.append(f"{pair_safe} \nClose: {res['close']}\nVolume: {res['volume']}\nCCI: {res['cci']} \nRSI: {res['rsi']}\n{link}\n")

    message_lines.append("\n===============================")
    final_message = "\n".join(message_lines)

    print(final_message)  # For local logging
    send_telegram_message(final_message)
