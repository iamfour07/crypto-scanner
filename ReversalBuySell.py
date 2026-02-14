import json
import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Swing import Send_Swing_Telegram_Message

# CONFIG
resolution = "60"
limit_hours = 1000
MAX_WORKERS = 15

BUY_FILE = "ReversalBuyWatchlist.json"
SELL_FILE = "ReversalSellWatchlist.json"

BB_LENGTH = 200
BB_MULT = 2.5

ST_LENGTH = 9
ST_FACTOR = 1.5

CAPITAL_RS = 1000
MAX_LOSS_RS = 100
MAX_ALLOWED_LEVERAGE = 20
MIN_LEVERAGE = 5


# ================= API =================
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


# ================= INDICATORS =================
def calculate_heikin_ashi(df):
    df["HA_close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    df["HA_open"] = (df["open"].shift(1) + df["close"].shift(1)) / 2
    df.iloc[0, df.columns.get_loc("HA_open")] = (df.iloc[0]["open"] + df.iloc[0]["close"]) / 2
    df["HA_high"] = df[["HA_open", "HA_close", "high"]].max(axis=1)
    df["HA_low"] = df[["HA_open", "HA_close", "low"]].min(axis=1)
    return df


def calculate_bollinger(df):
    df["BB_mid"] = df["HA_close"].rolling(BB_LENGTH).mean()
    df["BB_std"] = df["HA_close"].rolling(BB_LENGTH).std()
    df["BB_upper"] = df["BB_mid"] + BB_MULT * df["BB_std"]
    df["BB_lower"] = df["BB_mid"] - BB_MULT * df["BB_std"]
    return df


def rma(series, period):
    return series.ewm(alpha=1/period, adjust=False).mean()


def calculate_supertrend(df):
    hl = df["HA_high"] - df["HA_low"]
    hc = (df["HA_high"] - df["HA_close"].shift()).abs()
    lc = (df["HA_low"] - df["HA_close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)

    atr = rma(tr, ST_LENGTH)

    hl2 = (df["HA_high"] + df["HA_low"]) / 2
    upperband = hl2 + ST_FACTOR * atr
    lowerband = hl2 - ST_FACTOR * atr

    supertrend = [True] * len(df)
    st_value = [0] * len(df)

    for i in range(1, len(df)):
        if supertrend[i - 1]:
            if df["HA_close"].iloc[i] < lowerband.iloc[i]:
                supertrend[i] = False
                st_value[i] = upperband.iloc[i]
            else:
                supertrend[i] = True
                st_value[i] = lowerband.iloc[i]
        else:
            if df["HA_close"].iloc[i] > upperband.iloc[i]:
                supertrend[i] = True
                st_value[i] = lowerband.iloc[i]
            else:
                supertrend[i] = False
                st_value[i] = upperband.iloc[i]

    df["supertrend"] = supertrend
    df["ST_value"] = st_value
    return df


# ================= FETCH =================
def fetch_and_prepare(pair):
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        from_time = now - limit_hours * 3600

        url = "https://public.coindcx.com/market_data/candlesticks"
        params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}

        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()

        data = r.json().get("data", [])
        if not data or len(data) < BB_LENGTH + 10:
            return None

        df = pd.DataFrame(data)
        df = df.sort_values("time").reset_index(drop=True)
        df = df.iloc[:-1].copy()

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = calculate_heikin_ashi(df)
        df = calculate_bollinger(df)
        df = calculate_supertrend(df)

        return df.dropna().reset_index(drop=True)

    except:
        return None


# ================= WATCHLIST =================
def load_watchlist(file):
    try:
        with open(file) as f:
            return json.load(f)
    except:
        return []


def save_watchlist(file, data):
    with open(file, "w") as f:
        json.dump(data, f, indent=2)


def find_entry(pair, watchlist):
    for x in watchlist:
        if x["pair"] == pair:
            return x
    return None


# ================= PROCESS =================
def process_pair(pair, buy_watch, sell_watch):
    df = fetch_and_prepare(pair)
    if df is None:
        return None

    last = df.iloc[-1]
    current_state = last["supertrend"]

    results = []

    # STEP 1: Add to watchlist
    if not find_entry(pair, buy_watch) and not find_entry(pair, sell_watch):

        # SELL setup
        if last["HA_high"] >= last["BB_upper"] and current_state == True:
            results.append(("add_sell", pair, current_state))

        # BUY setup
        elif last["HA_low"] <= last["BB_lower"] and current_state == False:
            results.append(("add_buy", pair, current_state))

    # STEP 2: Check state change

    sell_entry = find_entry(pair, sell_watch)
    if sell_entry:
        if sell_entry["entry_state"] == True and current_state == False:
            results.append(("sell_signal", pair))

    buy_entry = find_entry(pair, buy_watch)
    if buy_entry:
        if buy_entry["entry_state"] == False and current_state == True:
            results.append(("buy_signal", pair))

    return results


# ================= MAIN =================
def main():
    pairs = get_active_usdt_coins()

    buy_watch = load_watchlist(BUY_FILE)
    sell_watch = load_watchlist(SELL_FILE)

    alerts = []

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(process_pair, p, buy_watch, sell_watch) for p in pairs]

        for f in as_completed(futures):
            res = f.result()
            if not res:
                continue

            for r in res:
                action = r[0]

                if action == "add_buy":
                    buy_watch.append({"pair": r[1], "entry_state": r[2]})

                elif action == "add_sell":
                    sell_watch.append({"pair": r[1], "entry_state": r[2]})

                elif action == "buy_signal":
                    alerts.append(f"🟢 BUY {r[1]}")
                    buy_watch = [x for x in buy_watch if x["pair"] != r[1]]

                elif action == "sell_signal":
                    alerts.append(f"🔴 SELL {r[1]}")
                    sell_watch = [x for x in sell_watch if x["pair"] != r[1]]

    if alerts:
        Send_Swing_Telegram_Message("\n\n".join(alerts))

    save_watchlist(BUY_FILE, buy_watch)
    save_watchlist(SELL_FILE, sell_watch)


if __name__ == "__main__":
    main()