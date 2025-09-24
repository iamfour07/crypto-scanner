import sys, os
import json
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from coindcx_api import get_active_usdt_coins, fetch_coin_data
from Indicators import calculate_bollinger, calculate_rsi, supertrend
from Telegram_Alert import send_telegram_message

# =========================
# CONFIGURATION
# =========================
BB_PERIOD = 200
BB_STD = 3
RSI_PERIOD = 28
ST_LENGTH = 10
ST_FACTOR = 2

BUY_FILE = "buy_watchlist.json"
SELL_FILE = "sell_watchlist.json"
ST_BUY_FILE = "Supertrend_buy_watchlist.json"
ST_SELL_FILE = "Supertrend_sell_watchlist.json"

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
# PROCESS COIN
# =========================
def process_coin(pair):
    df = fetch_coin_data(pair)
    if df is None or len(df) < 3:
        return None

    # Indicators
    df["upper"], df["lower"] = calculate_bollinger(df["close"], BB_PERIOD, BB_STD)
    df["rsi"] = calculate_rsi(df["close"], RSI_PERIOD)
    df = supertrend(df, ST_LENGTH, ST_FACTOR)
    df["pair"] = pair

    return df

# =========================
# MAIN SCANNER
# =========================
def main():
    buy_watchlist = load_watchlist(BUY_FILE)
    sell_watchlist = load_watchlist(SELL_FILE)
    st_buy_watchlist = load_watchlist(ST_BUY_FILE)
    st_sell_watchlist = load_watchlist(ST_SELL_FILE)

    new_buy_candidates, new_sell_candidates = set(), set()
    buy_signals, sell_signals = [], []

    CoinList = get_active_usdt_coins()


    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_coin, coin): coin for coin in CoinList}
        for future in as_completed(futures):
            try:
                df = future.result()
                if df is None:
                    continue

                pair = df["pair"].iloc[0]
                last = df.iloc[-2]  # prev closed candle
                prev = df.iloc[-3]  # second prev candle

                # Bollinger Watchlists
                if last["close"] > last["upper"]:
                    new_sell_candidates.add(pair)
                if last["close"] < last["lower"]:
                    new_buy_candidates.add(pair)

                # Buy condition
                if pair in buy_watchlist:
                    if last["Trend"] == "BUY" and prev["Trend"] != "BUY":
                        if last["rsi"] > 52:
                            buy_signals.append((pair, last))
                            buy_watchlist.remove(pair)
                        else:
                            st_buy_watchlist.add(pair)

                # Sell condition
                if pair in sell_watchlist:
                    if last["Trend"] == "SELL" and prev["Trend"] != "SELL":
                        if last["rsi"] < 52:
                            sell_signals.append((pair, last))
                            sell_watchlist.remove(pair)
                        else:
                            st_sell_watchlist.add(pair)

                # Re-check ST Buy Watchlist
                if pair in st_buy_watchlist:
                    if last["Trend"] == "BUY" and last["rsi"] > 52:
                        buy_signals.append((pair, last))
                        st_buy_watchlist.remove(pair)

                # Re-check ST Sell Watchlist
                if pair in st_sell_watchlist:
                    if last["Trend"] == "SELL" and last["rsi"] < 52:
                        sell_signals.append((pair, last))
                        st_sell_watchlist.remove(pair)

            except Exception as e:
                print(f"Error processing coin {futures[future]}: {e}")

    # Save watchlists
    buy_watchlist.update(new_buy_candidates)
    sell_watchlist.update(new_sell_candidates)
    save_watchlist(BUY_FILE, buy_watchlist)
    save_watchlist(SELL_FILE, sell_watchlist)
    save_watchlist(ST_BUY_FILE, st_buy_watchlist)
    save_watchlist(ST_SELL_FILE, st_sell_watchlist)

    # Telegram alerts
    if buy_signals or sell_signals:
        message_lines = ["📊 Hourly Crypto Signals (BB + ST + RSI)\n"]

        if buy_signals:
            message_lines.append("🟢 BUY Signals:\n")
            for pair, candle in buy_signals:
                link = f"https://coindcx.com/futures/{pair}"
                message_lines.append(
                    f"{pair}\nClose: {candle['close']}\nRSI: {round(candle['rsi'],2)} > 52\nSupertrend: GREEN\n{link}\n"
                )

        if sell_signals:
            message_lines.append("\n🔴 SELL Signals:\n")
            for pair, candle in sell_signals:
                link = f"https://coindcx.com/futures/{pair}"
                message_lines.append(
                    f"{pair}\nClose: {candle['close']}\nRSI: {round(candle['rsi'],2)} < 52\nSupertrend: RED\n{link}\n"
                )

        message_lines.append("\n===============================")
        send_telegram_message("\n".join(message_lines))

if __name__ == "__main__":
    main()
