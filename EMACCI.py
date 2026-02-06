import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Alert import send_telegram_message


# ===================================================
# HEADER (Telegram Format)
# ===================================================

HEADER = """
================================================
📊 EMA CROSSOVER FUTURES SCANNER (15m)
================================================
Logic:
• EMA10/20 cross EMA89
• Trend confirmation EMA200
• Futures candles (15m)
• Auto leverage + RR targets
================================================
"""


# ===================================================
# CONFIG
# ===================================================

INTERVAL = "1h"
EMA_PERIODS = [10, 20, 89, 200]
MAX_WORKERS = 12

CAPITAL_RS = 1000
MAX_LOSS_RS = 100
MAX_ALLOWED_LEVERAGE = 10
MIN_LEVERAGE = 5

# ===================================================
# SESSIONS
# ===================================================

session = requests.Session()
BINANCE_URL = "https://fapi.binance.com/fapi/v1/klines"


# ===================================================
# ACTIVE FUTURES (CoinDCX)
# ===================================================

def get_active_usdt_coins():
    url = (
        "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
        "active_instruments?margin_currency_short_name[]=USDT"
    )
    return requests.get(url, timeout=30).json()


# ===================================================
# SYMBOL CONVERSION
# ===================================================

def convert_symbol(pair):
    return pair.replace("B-", "").replace("_", "")


# ===================================================
# FETCH 15m CANDLES (Binance Futures)
# ===================================================

def fetch_candles(pair):

    symbol = convert_symbol(pair)

    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "limit": 400
    }

    try:
        data = session.get(BINANCE_URL, params=params, timeout=10).json()

        if len(data) < 300:
            return None

        df = pd.DataFrame(data, columns=[
            "time","open","high","low","close","volume",
            "c1","c2","c3","c4","c5","c6"
        ])

        df = df[["open","high","low","close","volume"]]

        for c in df.columns:
            df[c] = pd.to_numeric(df[c])

        return df.reset_index(drop=True)

    except:
        return None


# ===================================================
# EMA LOGIC
# ===================================================

def calculate_emas(df):
    for p in EMA_PERIODS:
        df[f"EMA_{p}"] = df["close"].ewm(span=p, adjust=False).mean()
    return df


def bullish_signal(last, prev):
    return (
        last["EMA_10"] > last["EMA_89"] and
        prev["EMA_10"] <= prev["EMA_89"] and
        last["EMA_20"] > last["EMA_89"] and
        prev["EMA_20"] <= prev["EMA_89"] and
        last["EMA_89"] > last["EMA_200"]
    )


def bearish_signal(last, prev):
    return (
        last["EMA_10"] < last["EMA_89"] and
        prev["EMA_10"] >= prev["EMA_89"] and
        last["EMA_20"] < last["EMA_89"] and
        prev["EMA_20"] >= prev["EMA_89"] and
        last["EMA_89"] < last["EMA_200"]
    )


# ===================================================
# RISK MANAGEMENT
# ===================================================

def calculate_trade_levels(entry, sl, side):

    risk = abs(entry - sl)

    for lev in range(MAX_ALLOWED_LEVERAGE, MIN_LEVERAGE - 1, -1):
        position_value = CAPITAL_RS * lev
        loss = (risk / entry) * position_value

        if loss <= MAX_LOSS_RS:
            leverage = lev
            break
    else:
        leverage = MIN_LEVERAGE

    used_capital = CAPITAL_RS

    if side == "BUY":
        t2 = entry + risk * 2
        t3 = entry + risk * 3
        t4 = entry + risk * 4
    else:
        t2 = entry - risk * 2
        t3 = entry - risk * 3
        t4 = entry - risk * 4

    return entry, sl, leverage, used_capital, t2, t3, t4


# ===================================================
# WORKER
# ===================================================

def process_pair(pair):

    df = fetch_candles(pair)
    if df is None:
        return None

    df = calculate_emas(df)

    last = df.iloc[-1]
    prev = df.iloc[-2]

    symbol = convert_symbol(pair)
    print(prev,symbol)
    # BUY
    if bullish_signal(last, prev):

        entry = last["close"]
        sl = last["low"]

        e, s, lev, cap, t2, t3, t4 = calculate_trade_levels(entry, sl, "BUY")

        return (
            f"🟢 BUY {symbol}\n"
            f"Entry   : {round(e,4)}\n"
            f"SL      : {round(s,4)}\n"
            f"Capital : ₹{cap} ({lev}×)\n\n"
            f"Targets\n"
            f"2R → {round(t2,4)}\n"
            f"3R → {round(t3,4)}\n"
            f"4R → {round(t4,4)}\n"
            f"------------------------------------------------"
        )

    # SELL
    if bearish_signal(last, prev):

        entry = last["close"]
        sl = last["high"]

        e, s, lev, cap, t2, t3, t4 = calculate_trade_levels(entry, sl, "SELL")

        return (
            f"🔴 SELL {symbol}\n"
            f"Entry   : {round(e,4)}\n"
            f"SL      : {round(s,4)}\n"
            f"Capital : ₹{cap} ({lev}×)\n\n"
            f"Targets\n"
            f"2R → {round(t2,4)}\n"
            f"3R → {round(t3,4)}\n"
            f"4R → {round(t4,4)}\n"
            f"------------------------------------------------"
        )

    return None


# ===================================================
# MAIN (manual run only)
# ===================================================

def main():

    active_pairs = get_active_usdt_coins()

    alerts = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_pair, pair) for pair in active_pairs]

        for f in as_completed(futures):
            result = f.result()
            if result:
                alerts.append(result)

    if alerts:
        summary = f"Scanned: {len(active_pairs)} coins | Signals: {len(alerts)}\n\n"
        message = HEADER + "\n" + summary + "\n\n".join(alerts)
        send_telegram_message(message)


# ===================================================
# RUN ONCE ONLY (manual)
# ===================================================

if __name__ == "__main__":
    main()