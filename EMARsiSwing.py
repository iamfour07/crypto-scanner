import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from Telegram_Reversal import Send_Reversal_Telegram_Message
except ImportError:
    def Send_Reversal_Telegram_Message(msg):
        print(msg)

# ================================================================
# CONFIG
# ================================================================
RESOLUTION = "30"

COINS = [
    "B-XRP_USDT",
    "B-ETH_USDT",
]
LEVERAGE = 5
RISK_PER_TRADE_INR = 100
INR_TO_USDT_RATE = None

SWING_CANDLES = 10
MAX_WORKERS = 10

# ================================================================
# INDICATORS
# ================================================================
def calculate_indicators(df):

    df["ema5"] = df["close"].ewm(span=5, adjust=False).mean()
    df["ema12"] = df["close"].ewm(span=12, adjust=False).mean()
    df["ema19"] = df["close"].ewm(span=19, adjust=False).mean()

    return df

# ================================================================
# FETCH DATA
# ================================================================
def fetch_data(pair):

    url = "https://public.coindcx.com/market_data/candlesticks"

    now = int(datetime.now(timezone.utc).timestamp())

    params = {
        "pair": pair,
        "from": now - 500 * 3600,
        "to": now,
        "resolution": RESOLUTION,
        "pcode": "f"
    }

    try:

        r = requests.get(
            url,
            params=params,
            timeout=15
        ).json()

        df = pd.DataFrame(r["data"])
        df = df.sort_values("time").iloc[:-1]

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col])

        return calculate_indicators(df)

    except Exception as e:

        print(f"{pair}: {e}")
        return None

# ================================================================
# INR RATE
# ================================================================
def get_inr_rate():

    if INR_TO_USDT_RATE is not None:
        return INR_TO_USDT_RATE

    try:

        r = requests.get(
            "https://api.coindcx.com/exchange/v1/markets_details",
            timeout=5
        ).json()

        for m in r:

            if m.get("symbol") == "USDTINR":

                return float(
                    m.get("last_price", 84.0)
                )

    except:
        pass

    return 84.0

# ================================================================
# POSITION SIZE
# ================================================================
def calc_position(entry, sl):

    sl_pct = abs(entry - sl) / entry * 100

    if sl_pct == 0:
        return None

    rate = get_inr_rate()

    risk_usdt = RISK_PER_TRADE_INR / rate

    position_usdt = round(
        risk_usdt / (sl_pct / 100),
        2
    )

    capital_usdt = round(
        position_usdt / LEVERAGE,
        2
    )

    capital_inr = round(
        capital_usdt * rate,
        2
    )

    quantity = round(
        position_usdt / entry,
        6
    )

    return {
        "capital_inr": capital_inr,
        "capital_usdt": capital_usdt,
        "quantity": quantity
    }

# ================================================================
# SIGNAL
# ================================================================
def check_signal(df):

    prev = df.iloc[-2]
    last = df.iloc[-1]
    
    buy = (
        prev["ema12"] <= prev["ema19"]
        and last["ema12"] > last["ema19"]
        and last["ema5"] > last["ema12"]
    )

    sell = (
        prev["ema12"] >= prev["ema19"]
        and last["ema12"] < last["ema19"]
        and last["ema5"] < last["ema12"]
    )

    if buy:
        return "BUY"

    if sell:
        return "SELL"

    return None

# ================================================================
# LEVELS
# ================================================================
def get_buy_levels(df):

    last = df.iloc[-1]

    entry = float(last["high"])

    sl = float(
        df.iloc[-(SWING_CANDLES + 1):-1]["low"].min()
    )

    risk = entry - sl

    if risk <= 0:
        return None

    t2 = entry + (risk * 2)
    t3 = entry + (risk * 3)

    return entry, sl, t2, t3


def get_sell_levels(df):

    last = df.iloc[-1]

    entry = float(last["low"])

    sl = float(
        df.iloc[-(SWING_CANDLES + 1):-1]["high"].max()
    )

    risk = sl - entry

    if risk <= 0:
        return None

    t2 = entry - (risk * 2)
    t3 = entry - (risk * 3)

    return entry, sl, t2, t3

# ================================================================
# MESSAGE
# ================================================================
def build_message(pair, side, entry, sl, t2, t3, current_price):

    pos = calc_position(entry, sl)

    capital = (
        f"₹{pos['capital_inr']} (~${pos['capital_usdt']})"
        if pos else "N/A"
    )

    qty = (
        pos["quantity"]
        if pos else "N/A"
    )

    return (
        f"🚨 {side} SETUP\n\n"
        f"Coin : {pair}\n"
        f"Current Price : {round(current_price,6)}\n"
        f"Entry Trigger : {round(entry,6)}\n"
        f"SL : {round(sl,6)}\n\n"
        f"Target 1 (1:2) : {round(t2,6)}\n"
        f"Target 2 (1:3) : {round(t3,6)}\n\n"
        f"Capital : {capital}\n"
        f"Quantity : {qty}"
    )

# ================================================================
# SCAN
# ================================================================
def scan_pair(pair):

    df = fetch_data(pair)

    if df is None or len(df) < 25:
        return None

    signal = check_signal(df)

    if signal == "BUY":

        levels = get_buy_levels(df)

        if not levels:
            return None

        entry, sl, t2, t3 = levels

        return build_message(
            pair,
            "BUY",
            entry,
            sl,
            t2,
            t3,
            df.iloc[-1]["close"]
        )

    if signal == "SELL":

        levels = get_sell_levels(df)

        if not levels:
            return None

        entry, sl, t2, t3 = levels

        return build_message(
            pair,
            "SELL",
            entry,
            sl,
            t2,
            t3,
            df.iloc[-1]["close"]
        )

    return None

# ================================================================
# MAIN
# ================================================================
def main():

    alerts = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [
            executor.submit(scan_pair, pair)
            for pair in COINS
        ]

        for future in as_completed(futures):

            result = future.result()

            if result:
                alerts.append(result)

    if alerts:

        msg = "\n\n-------------------\n\n".join(alerts)

        Send_Reversal_Telegram_Message(msg)

        print(msg)

    else:

        print("No Signal Found")

if __name__ == "__main__":
    main()
