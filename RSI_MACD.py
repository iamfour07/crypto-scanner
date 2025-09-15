import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import html
from Telegram_Alert import send_telegram_message

# =========================
# CONFIGURATION
# =========================
resolution = "60"  # 1-hour candles
limit_hours = 1000  # fetch 500 hours history
IST = timezone(timedelta(hours=5, minutes=30))

# MACD settings
FAST_EMA = 12
SLOW_EMA = 26
SIGNAL_EMA = 9

# RSI settings
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# FILES TO STORE WATCHLISTS
BUY_FILE = "buy_watchlist.json"
SELL_FILE = "sell_watchlist.json"
# =========================
# UTILS: LOAD & SAVE WATCHLISTS
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
def calculate_macd(close_prices):
    ema_fast = close_prices.ewm(span=FAST_EMA, adjust=False).mean()
    ema_slow = close_prices.ewm(span=SLOW_EMA, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=SIGNAL_EMA, adjust=False).mean()
    return macd_line, signal_line

def calculate_rsi(close_prices, period=14):
    delta = close_prices.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

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
    data = resp.json()['data']

    df = pd.DataFrame(data)
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['volume'] = df['volume'].astype(float)

    # Indicators
    df['macd'], df['signal'] = calculate_macd(df['close'])
    df['rsi'] = calculate_rsi(df['close'], RSI_PERIOD)

    # Last closed candle (index -2)
    prev_macd = df['macd'].iloc[-2]
    prev_signal = df['signal'].iloc[-2]
    last_macd = df['macd'].iloc[-1]
    last_signal = df['signal'].iloc[-1]
    last_rsi = df['rsi'].iloc[-1]
    last_candle = df.iloc[-1]

    return {
        "pair": pair,
        "close": last_candle['close'],
        "macd": round(last_macd, 1),
        "signal_line": round(last_signal, 1),
        "rsi": round(last_rsi, 1),
        "volume": last_candle['volume'],
        "macd_buy": prev_macd < prev_signal and last_macd > last_signal,
        "macd_sell": prev_macd > prev_signal and last_macd < last_signal,
        "rsi_buy": last_rsi <= RSI_OVERSOLD,
        "rsi_sell": last_rsi >= RSI_OVERBOUGHT,
    }

# =========================
# MAIN SCANNER
# =========================
def main():
    # Load watchlists
    buy_watchlist = load_watchlist(BUY_FILE)
    sell_watchlist = load_watchlist(SELL_FILE)

    new_buy_candidates = set()
    new_sell_candidates = set()

    buy_signals, sell_signals = [], []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_coin_data, coin): coin for coin in CoinList}
        for future in as_completed(futures):
            try:
                data = future.result()
                pair = data['pair']

                # Step 1: RSI check → add to watchlist
                if data['rsi_buy']:
                    new_buy_candidates.add(pair)
                if data['rsi_sell']:
                    new_sell_candidates.add(pair)

                # Step 2: MACD check on watchlists
                if pair in buy_watchlist and data['macd_buy']:
                    buy_signals.append(data)
                    buy_watchlist.remove(pair)

                if pair in sell_watchlist and data['macd_sell']:
                    sell_signals.append(data)
                    sell_watchlist.remove(pair)

            except Exception as e:
                print(f"Error processing coin: {e}")

    # Update watchlists (avoid duplicates)
    buy_watchlist.update(new_buy_candidates)
    sell_watchlist.update(new_sell_candidates)

    save_watchlist(BUY_FILE, buy_watchlist)
    save_watchlist(SELL_FILE, sell_watchlist)

    # Sort by volume
    buy_signals.sort(key=lambda x: x['volume'], reverse=True)
    sell_signals.sort(key=lambda x: x['volume'], reverse=True)

    # =========================
    # PREPARE TELEGRAM MESSAGE
    # =========================
    if buy_signals or sell_signals:
        message_lines = [f"📊 Hourly Crypto Signals (RSI+MACD)\n"]

        if buy_signals:
            message_lines.append("🟢 BUY Signals:\n")
            for res in buy_signals:
                pair_safe = html.escape(res['pair'])
                link = f"https://coindcx.com/futures/{res['pair']}"
                message_lines.append(
                    f"{pair_safe}\nClose: {res['close']}\nMACD: {res['macd']} > {res['signal_line']}\n"
                    f"RSI: {res['rsi']}\nVolume: {res['volume']}\n{link}\n"
                )

        if sell_signals:
            message_lines.append("\n🔴 SELL Signals:\n")
            for res in sell_signals:
                pair_safe = html.escape(res['pair'])
                link = f"https://coindcx.com/futures/{res['pair']}"
                message_lines.append(
                    f"{pair_safe}\nClose: {res['close']}\nMACD: {res['macd']} < {res['signal_line']}\n"
                    f"RSI: {res['rsi']}\nVolume: {res['volume']}\n{link}\n"
                )

        message_lines.append("\n===============================")
        final_message = "\n".join(message_lines)

        # print(final_message)  # Local log
        send_telegram_message(final_message)

if __name__ == "__main__":
    main()
