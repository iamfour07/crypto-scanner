import json
import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Alert_Swing import send_telegram_message

# =====================
# CONFIG
# =====================
resolution = "60"
limit_hours = 1000
MAX_WORKERS = 15

BUY_FILE = "BuyWatchlist.json"
SELL_FILE = "SellWatchlist.json"

BB_LENGTH = 200
BB_MULT = 2.5

ST_LENGTH = 9
ST_FACTOR = 1.5


# ===================================================
# API
# ===================================================
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    pairs = []
    for x in data:
        if isinstance(x, str):
            pairs.append(x)
        elif isinstance(x, dict) and "pair" in x:
            pairs.append(x["pair"])
    return pairs


# ===================================================
# INDICATORS
# ===================================================
def calculate_heikin_ashi(df):
    ha = df.copy()

    ha["HA_close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    ha["HA_open"] = (df["open"].shift(1) + df["close"].shift(1)) / 2
    ha.iloc[0, ha.columns.get_loc("HA_open")] = (df.iloc[0]["open"] + df.iloc[0]["close"]) / 2

    ha["HA_high"] = ha[["HA_open", "HA_close", "high"]].max(axis=1)
    ha["HA_low"] = ha[["HA_open", "HA_close", "low"]].min(axis=1)

    return ha


def calculate_bollinger(df):
    df["BB_mid"] = df["HA_close"].rolling(BB_LENGTH).mean()
    df["BB_std"] = df["HA_close"].rolling(BB_LENGTH).std()
    df["BB_upper"] = df["BB_mid"] + BB_MULT * df["BB_std"]
    df["BB_lower"] = df["BB_mid"] - BB_MULT * df["BB_std"]
    return df


def rma(series, period):
    return series.ewm(alpha=1/period, adjust=False).mean()


def calculate_atr(df, period=9):
    df["H-L"] = df["HA_high"] - df["HA_low"]
    df["H-PC"] = abs(df["HA_high"] - df["HA_close"].shift(1))
    df["L-PC"] = abs(df["HA_low"] - df["HA_close"].shift(1))
    df["TR"] = df[["H-L", "H-PC", "L-PC"]].max(axis=1)
    df["ATR"] = rma(df["TR"], period)
    return df


def calculate_supertrend(df):
    df = calculate_atr(df, ST_LENGTH)

    hl2 = (df["HA_high"] + df["HA_low"]) / 2
    df["basic_upperband"] = hl2 + ST_FACTOR * df["ATR"]
    df["basic_lowerband"] = hl2 - ST_FACTOR * df["ATR"]

    final_upperband = [0] * len(df)
    final_lowerband = [0] * len(df)
    supertrend = [True] * len(df)

    for i in range(len(df)):
        if i == 0:
            final_upperband[i] = df["basic_upperband"].iloc[i]
            final_lowerband[i] = df["basic_lowerband"].iloc[i]
            supertrend[i] = True
            continue

        if (
            df["basic_upperband"].iloc[i] < final_upperband[i - 1]
            or df["HA_close"].iloc[i - 1] > final_upperband[i - 1]
        ):
            final_upperband[i] = df["basic_upperband"].iloc[i]
        else:
            final_upperband[i] = final_upperband[i - 1]

        if (
            df["basic_lowerband"].iloc[i] > final_lowerband[i - 1]
            or df["HA_close"].iloc[i - 1] < final_lowerband[i - 1]
        ):
            final_lowerband[i] = df["basic_lowerband"].iloc[i]
        else:
            final_lowerband[i] = final_lowerband[i - 1]

        if supertrend[i - 1]:
            supertrend[i] = df["HA_close"].iloc[i] >= final_lowerband[i]
        else:
            supertrend[i] = df["HA_close"].iloc[i] > final_upperband[i]

    df["supertrend"] = supertrend
    return df


# ===================================================
# FETCH CANDLES
# ===================================================
def fetch_last_n_candles(pair):
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        from_time = now - limit_hours * 3600

        url = "https://public.coindcx.com/market_data/candlesticks"
        params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}

        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()

        data = resp.json().get("data", [])
        if not data or len(data) < 250:
            return None

        df = pd.DataFrame(data)

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = calculate_heikin_ashi(df)
        df = calculate_bollinger(df)
        df = calculate_supertrend(df)

        df = df.dropna().reset_index(drop=True)
        return df

    except Exception as e:
        print(f"[candles] {pair} error: {e}")
        return None


# ===================================================
# WATCHLIST HELPERS
# ===================================================
def load_watchlist(file):
    try:
        with open(file, "r") as f:
            return json.load(f)
    except:
        return []


def save_watchlist(file, data):
    with open(file, "w") as f:
        json.dump(data, f, indent=2)


# ===================================================
# MAIN
# ===================================================
def main():
    pairs = get_active_usdt_coins()
    print(f"Fetched {len(pairs)} active USDT futures pairs.")

    buy_watch = load_watchlist(BUY_FILE)
    sell_watch = load_watchlist(SELL_FILE)

    # STEP 1: Bollinger touch
    def check_bollinger(pair):
        if pair in buy_watch or pair in sell_watch:
            return None

        df = fetch_last_n_candles(pair)
        if df is None:
            return None

        last = df.iloc[-2]

        if last["HA_low"] <= last["BB_lower"]:
            return ("buy", pair)

        if last["HA_high"] >= last["BB_upper"]:
            return ("sell", pair)

        return None

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(check_bollinger, p) for p in pairs]
        for f in as_completed(futures):
            result = f.result()
            if result:
                side, pair = result
                if side == "buy":
                    buy_watch.append(pair)
                    print("BUY watch:", pair)  # Debug line to check watchlist creation
                else:
                    sell_watch.append(pair)
                    print("SELL watch:", pair)  # Debug line to check watchlist creation

    # STEP 2: Supertrend flip
    buy_signals = []
    sell_signals = []

    def check_buy_flip(pair):
        df = fetch_last_n_candles(pair)
        if df is None:
            return None
        prev2, prev1 = df.iloc[-3], df.iloc[-2]
        current = df.iloc[-1]

        # Debug: print current and previous supertrend states
        # print(f"[DEBUG] {pair} | prev2: {prev2['supertrend']} | prev1: {prev1['supertrend']} | current: {current['supertrend']}")

        if not prev2["supertrend"] and prev1["supertrend"]:
            return pair
        return None

    def check_sell_flip(pair):
        df = fetch_last_n_candles(pair)
        if df is None:
            return None
        prev2, prev1 = df.iloc[-3], df.iloc[-2]
        current = df.iloc[-1]

        # Debug: print current and previous supertrend states
        # print(f"[DEBUG] {pair} | prev2: {prev2['supertrend']} | prev1: {prev1['supertrend']} | current: {current['supertrend']}")

        if prev2["supertrend"] and not prev1["supertrend"]:
            return pair
        return None

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        buy_signals = [f.result() for f in as_completed([executor.submit(check_buy_flip, p) for p in buy_watch]) if f.result()]

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        sell_signals = [f.result() for f in as_completed([executor.submit(check_sell_flip, p) for p in sell_watch]) if f.result()]

    # STEP 3: Alerts
    if buy_signals:
        send_telegram_message("🟢 BUY Signals:\n" + "\n".join(buy_signals))
        # Remove coins from the buy_watchlist
        buy_watch = [p for p in buy_watch if p not in buy_signals]

    if sell_signals:
        send_telegram_message("🔴 SELL Signals:\n" + "\n".join(sell_signals))
        # Remove coins from the sell_watchlist
        sell_watch = [p for p in sell_watch if p not in sell_signals]

    # Save updated watchlists
    save_watchlist(BUY_FILE, buy_watch)
    save_watchlist(SELL_FILE, sell_watch)

    print("Watchlists updated.")

if __name__ == "__main__":
    main()