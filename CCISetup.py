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
PREV_CANDLES = 10 # number of previous candles to check for false signal

# --- Risk Management ---
MAX_RISK = 100   # INR
LEVERAGE = 10    # leverage

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



def calculate_position(entry, stop_loss, max_risk=MAX_RISK, leverage=LEVERAGE):
    risk_per_coin = abs(entry - stop_loss)
    if risk_per_coin <= 0:
        return 0
    qty = int((max_risk * leverage) / risk_per_coin)
    return qty

def calculate_targets(entry, stop_loss, signal_type):
    risk = abs(entry - stop_loss)
    targets = {}
    if signal_type == "BUY":
        for rr in range(1, 6):
            targets[f"1:{rr}"] = float(round(entry + rr * risk, 6))
    else:  # SELL
        for rr in range(1, 6):
            targets[f"1:{rr}"] = float(round(entry - rr * risk, 6))
    return targets

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

        # Calculate CCI only
        df['cci'] = calculate_cci(df['high'], df['low'], df['close'], period=CCI_PERIOD)
        last_cci = df['cci'].iloc[-2]

        # previous N candles for false signal check
        prev_cci_window = df['cci'].iloc[-(PREV_CANDLES+2):-2]

        signal_type = None

        # BUY: First time CCI crosses above 200
        if last_cci > CCI_UPPER:
            if all(prev_cci_window <= CCI_UPPER):
                signal_type = "BUY"

        # SELL: First time CCI crosses below -200
        elif last_cci < CCI_LOWER:
            if all(prev_cci_window >= CCI_LOWER):
                signal_type = "SELL"

        if signal_type:
            if signal_type == "BUY":
                entry_price = last_candle['high']
                stop_loss = last_candle['low']
            else:  # SELL
                entry_price = last_candle['low']
                stop_loss = last_candle['high']

            qty = calculate_position(entry_price, stop_loss)
            targets = calculate_targets(entry_price, stop_loss, signal_type)

            return {
                "pair": pair,
                "close": last_candle['close'],
                "high": last_candle['high'],
                "low": last_candle['low'],
                "volume": last_candle['volume'],
                "cci": round(last_cci, 2),
                "signal": signal_type,
                "entry": entry_price,
                "stop_loss": stop_loss,
                "quantity": qty,
                "targets": targets
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
            message_lines.append(
                f"{pair_safe}\nEntry (High): {res['entry']}\nStop-loss (Low): {res['stop_loss']}\n"
                f"CCI: {res['cci']}\n"
                f"Targets: {res['targets']}\n{link}\n"
            )

    if sell_signals:
        message_lines.append("\n🔴 SELL Signals:\n")
        for res in sell_signals:
            pair_safe = html.escape(res['pair'])
            link = f"https://coindcx.com/futures/{res['pair']}"
            message_lines.append(
                f"{pair_safe}\nEntry (Low): {res['entry']}\nStop-loss (High): {res['stop_loss']}\n"
                f"\nCCI: {res['cci']}\n"
                f"Targets: {res['targets']}\n{link}\n"
            )

    message_lines.append("\n===============================")
    final_message = "\n".join(message_lines)

    print(final_message)  # For local logging
    send_telegram_message(final_message)
