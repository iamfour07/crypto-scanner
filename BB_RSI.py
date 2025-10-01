import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Alert_Swing import send_telegram_message

# =========================
# CONFIGURATION
# =========================
resolution = "60"  # 1-hour candles
limit_hours = 1000
IST = timezone(timedelta(hours=5, minutes=30))

# Bollinger settings
BB_PERIOD = 20
BB_STD = 3

# RSI
RSI_PERIOD = 21

# FILES
BUY_FILE = "buy_watchlist.json"
SELL_FILE = "sell_watchlist.json"

# =========================
# UTILS
# =========================
def load_watchlist(filename):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return set(json.load(f))
    return set()

def save_watchlist(filename, data_set):
    with open(filename, "w") as f:
        json.dump(list(data_set), f)
# =========================
# FETCH ACTIVE COINS
# =========================
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error fetching coins: {e}")
        return []

CoinList = get_active_usdt_coins()

# =========================
# INDICATOR CALCULATIONS
# =========================
def calculate_bollinger(close_prices, period=200, std=3):
    ma = close_prices.rolling(window=period).mean()
    stddev = close_prices.rolling(window=period).std()
    upper_band = ma + stddev * std
    lower_band = ma - stddev * std
    return upper_band, lower_band

def calculate_rsi(close_prices, period=28):
    delta = close_prices.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# =========================
# HEIKIN-ASHI CALCULATION
# =========================
def heikin_ashi(df):
    ha_df = df.copy()
    ha_df['HA_Close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    ha_df['HA_Open'] = 0.0
    ha_df['HA_High'] = 0.0
    ha_df['HA_Low'] = 0.0

    for i in range(len(df)):
        if i == 0:
            ha_df.iat[0, ha_df.columns.get_loc('HA_Open')] = (df['open'].iloc[0] + df['close'].iloc[0]) / 2
        else:
            ha_df.iat[i, ha_df.columns.get_loc('HA_Open')] = (ha_df['HA_Open'].iloc[i-1] + ha_df['HA_Close'].iloc[i-1]) / 2

        ha_df.iat[i, ha_df.columns.get_loc('HA_High')] = max(df['high'].iloc[i], ha_df['HA_Open'].iloc[i], ha_df['HA_Close'].iloc[i])
        ha_df.iat[i, ha_df.columns.get_loc('HA_Low')] = min(df['low'].iloc[i], ha_df['HA_Open'].iloc[i], ha_df['HA_Close'].iloc[i])

    return ha_df

# =========================
# FETCH & PROCESS ONE COIN
# =========================
def fetch_coin_data(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - limit_hours * 3600
    url = "https://public.coindcx.com/market_data/candlesticks"
    params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json().get('data', [])

    if not data:
        return None

    df = pd.DataFrame(data)
    df[['open','high','low','close']] = df[['open','high','low','close']].astype(float)

    # Indicators
    df['upper'], df['lower'] = calculate_bollinger(df['close'], BB_PERIOD, BB_STD)
    df['rsi'] = calculate_rsi(df['close'], RSI_PERIOD)
    df = heikin_ashi(df)

    df['pair'] = pair
    return df
# =========================
# MAIN SCANNER
# =========================
def main():
    buy_watchlist = load_watchlist(BUY_FILE)
    sell_watchlist = load_watchlist(SELL_FILE)

    new_buy_candidates, new_sell_candidates = set(), set()
    buy_signals, sell_signals = [], []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_coin_data, coin): coin for coin in CoinList}
        for future in as_completed(futures):
            try:
                df = future.result()
                if df is None or len(df) < 3:
                    continue

                pair = df['pair'].iloc[0]
                prev = df.iloc[-2]  # last closed HA candle

                # -------------------
                # BUY LOGIC
                # -------------------
                # Step 1: Check existing buy watchlist for RSI
                if pair in buy_watchlist:
                    if prev['rsi'] > 50:
                        buy_signals.append((pair, prev))
                        buy_watchlist.remove(pair)  # remove only when RSI > 50

                # Step 2: Check BB + HA condition to add to watchlist
                if prev['HA_Low'] <= prev['lower']:
                    new_buy_candidates.add(pair)

                # -------------------
                # SELL LOGIC
                # -------------------
                # Step 1: Check existing sell watchlist for RSI
                if pair in sell_watchlist:
                    if prev['rsi'] < 50:
                        sell_signals.append((pair, prev))
                        sell_watchlist.remove(pair)  # remove only when RSI < 50

                # Step 2: Check BB + HA condition to add to sell watchlist
                if prev['HA_High'] >= prev['upper']:
                    new_sell_candidates.add(pair)

            except Exception as e:
                print(f"Error processing coin {futures[future]}: {e}")

    # Update watchlists
    buy_watchlist.update(new_buy_candidates)
    sell_watchlist.update(new_sell_candidates)
    save_watchlist(BUY_FILE, buy_watchlist)
    save_watchlist(SELL_FILE, sell_watchlist)

    # -------------------
    # TELEGRAM MESSAGE
    # -------------------
    if buy_signals or sell_signals:
        message_lines = [f"📊 Hourly Crypto Signals (BB + RSI + HA)\n"]

        if buy_signals:
            message_lines.append("🟢 BUY Signals:\n")
            for pair, candle in buy_signals:
                link = f"https://coindcx.com/futures/{pair}"
                message_lines.append(
                    f"{pair}\nHA Low: {candle['HA_Low']}\nRSI: {round(candle['rsi'],2)} > 50\n{link}\n"
                )

        if sell_signals:
            message_lines.append("\n🔴 SELL Signals:\n")
            for pair, candle in sell_signals:
                link = f"https://coindcx.com/futures/{pair}"
                message_lines.append(
                    f"{pair}\nHA High: {candle['HA_High']}\nRSI: {round(candle['rsi'],2)} < 50\n{link}\n"
                )

        message_lines.append("\n===============================")
        final_message = "\n".join(message_lines)
        send_telegram_message(final_message)


if __name__ == "__main__":
    main()
