import requests
import pandas as pd
import json
import os
from zoneinfo import ZoneInfo

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from Telegram_EMA import Send_EMA_Telegram_Message

# ================= CONFIG =================

MAX_WORKERS = 20

LIMIT_HOURS = 1000

DAILY_RESOLUTION = "1D"
INTRADAY_RESOLUTION = "60"

BB_LENGTH = 20
BB_STD = 2

RISK_PER_TRADE = 150
LEVERAGE = 10

BUY_FILE = "BuyMomentum.json"
SELL_FILE = "SellMomentum.json"

# ================= LOAD / SAVE =================

def load_json(file, default):

    if os.path.exists(file):

        try:
            with open(file, "r") as f:
                return json.load(f)

        except:
            return default

    return default


def save_json(file, data):

    with open(file, "w") as f:
        json.dump(data, f, indent=2)


# ================= FETCH CANDLES =================

def fetch_candles(pair, resolution):

    url = "https://public.coindcx.com/market_data/candlesticks"

    now = int(datetime.now(timezone.utc).timestamp())

    params = {
        "pair": pair,
        "from": now - LIMIT_HOURS * 3600,
        "to": now,
        "resolution": resolution,
        "pcode": "f"
    }

    try:

        response = requests.get(
            url,
            params=params,
            timeout=10
        )

        data = response.json()

        if not isinstance(data, dict):
            return None

        if "data" not in data:
            return None

        df = (
            pd.DataFrame(data["data"])
            .sort_values("time")
            .reset_index(drop=True)
        )

        for col in ["open", "high", "low", "close"]:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        # remove incomplete candle
        df = df.iloc[:-1]

        if len(df) < 30:
            return None

        return df

    except:
        return None


# ================= BOLLINGER BANDS =================

def add_bollinger_bands(df):

    df["basis"] = (
        df["close"]
        .rolling(BB_LENGTH)
        .mean()
    )

    df["std"] = (
        df["close"]
        .rolling(BB_LENGTH)
        .std()
    )

    df["upper_bb"] = (
        df["basis"] + (BB_STD * df["std"])
    )

    df["lower_bb"] = (
        df["basis"] - (BB_STD * df["std"])
    )

    return df


# ================= FETCH ALL PAIRS =================

def get_all_pairs():

    url = (
        "https://api.coindcx.com/exchange/v1/"
        "derivatives/futures/data/"
        "active_instruments?"
        "margin_currency_short_name[]=USDT"
    )

    try:

        data = requests.get(url).json()

        return [
            p for p in data
            if isinstance(p, str)
        ]

    except:
        return []


# ================= DAILY BB PROCESS =================

def process_daily_pair(pair):

    df = fetch_candles(
        pair,
        DAILY_RESOLUTION
    )

    if df is None:
        return None

    df = add_bollinger_bands(df)

    if len(df) < BB_LENGTH + 5:
        return None

    prev = df.iloc[-2]

    last = df.iloc[-1]

    # ================= BUY =================

    bullish = (

        prev["close"] <= prev["upper_bb"]

        and

        last["close"] > last["upper_bb"]
    )

    # ================= SELL =================

    bearish = (

        prev["close"] >= prev["lower_bb"]

        and

        last["close"] < last["lower_bb"]
    )

    if bullish:

        return {
            "side": "BUY",
            "data": {
                "name": pair,
                "high": float(last["high"]),
                "low": float(last["low"]),
                "close": float(last["close"]),
                "time": str(last["time"])
            }
        }

    if bearish:

        return {
            "side": "SELL",
            "data": {
                "name": pair,
                "high": float(last["high"]),
                "low": float(last["low"]),
                "close": float(last["close"]),
                "time": str(last["time"])
            }
        }

    return None


# ================= RUN DAILY SCAN =================

def run_daily_scan():

    print("🚀 Running Daily BB Scan")

    buy_list = load_json(
        BUY_FILE,
        []
    )

    sell_list = load_json(
        SELL_FILE,
        []
    )

    pairs = get_all_pairs()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        results = list(
            executor.map(
                process_daily_pair,
                pairs
            )
        )

    for result in results:

        if result is None:
            continue

        pair = result["data"]["name"]

        # ================= BUY =================

        if result["side"] == "BUY":

            if not any(
                c["name"] == pair
                for c in buy_list
            ):

                buy_list.append(
                    result["data"]
                )

                print(f"🟢 Added BUY: {pair}")

        # ================= SELL =================

        elif result["side"] == "SELL":

            if not any(
                c["name"] == pair
                for c in sell_list
            ):

                sell_list.append(
                    result["data"]
                )

                print(f"🔴 Added SELL: {pair}")

    save_json(BUY_FILE, buy_list)

    save_json(SELL_FILE, sell_list)

    Send_EMA_Telegram_Message(
        f"✅ Daily BB Scan Completed\n\n"
        f"🟢 Buy Watchlist: {len(buy_list)}\n"
        f"🔴 Sell Watchlist: {len(sell_list)}"
    )


# ================= BUY MONITOR =================

def monitor_buy_watchlist():

    buy_list = load_json(
        BUY_FILE,
        []
    )

    updated_buy = []

    for coin in buy_list:

        pair = coin["name"]

        breakout_high = coin["high"]

        breakout_low = coin["low"]

        # ================= DAILY INVALIDATION =================

        daily_df = fetch_candles(
            pair,
            DAILY_RESOLUTION
        )

        if daily_df is None:

            updated_buy.append(coin)

            continue

        last_daily = daily_df.iloc[-1]

        # invalidate if daily close below breakout low
        if last_daily["close"] < breakout_low:

            print(f"❌ BUY INVALIDATED: {pair}")

            continue

        # ================= 1H BREAKOUT =================

        intraday_df = fetch_candles(
            pair,
            INTRADAY_RESOLUTION
        )

        if intraday_df is None:

            updated_buy.append(coin)

            continue

        last_1h = intraday_df.iloc[-1]

        # CLOSE BREAKOUT
        if last_1h["close"] > breakout_high:

            entry = breakout_high

            sl = breakout_low

            risk = entry - sl

            if risk <= 0:
                continue

            qty = RISK_PER_TRADE / risk

            margin = (qty * entry) / LEVERAGE

            # ===== TARGETS =====
            t1 = entry + (risk * 1)
            t2 = entry + (risk * 2)
            t3 = entry + (risk * 3)
            t4 = entry + (risk * 4)

            Send_EMA_Telegram_Message(
                f"🟢 BUY BREAKOUT CONFIRMED\n\n"
                f"Pair: {pair}\n\n"
                f"1H Candle Close Above Breakout High\n\n"
                f"Entry: {entry:.6f}\n"
                f"Stop Loss: {sl:.6f}\n\n"
                f"Risk Per Trade: ₹{RISK_PER_TRADE}\n"
                f"Leverage: {LEVERAGE}x\n"
                f"Quantity: {qty:.4f}\n"
                f"Margin Required: ₹{margin:.2f}\n\n"
                f"Targets:\n"
                f"1:1 → {t1:.6f}\n"
                f"1:2 → {t2:.6f}\n"
                f"1:3 → {t3:.6f}\n"
                f"1:4 → {t4:.6f}"
            )

            print(f"🟢 BUY ALERT: {pair}")

        else:

            updated_buy.append(coin)

    save_json(
        BUY_FILE,
        updated_buy
    )


# ================= SELL MONITOR =================

def monitor_sell_watchlist():

    sell_list = load_json(
        SELL_FILE,
        []
    )

    updated_sell = []

    for coin in sell_list:

        pair = coin["name"]

        breakdown_high = coin["high"]

        breakdown_low = coin["low"]

        # ================= DAILY INVALIDATION =================

        daily_df = fetch_candles(
            pair,
            DAILY_RESOLUTION
        )

        if daily_df is None:

            updated_sell.append(coin)

            continue

        last_daily = daily_df.iloc[-1]

        # invalidate if daily close above breakdown high
        if last_daily["close"] > breakdown_high:

            print(f"❌ SELL INVALIDATED: {pair}")

            continue

        # ================= 1H BREAKDOWN =================

        intraday_df = fetch_candles(
            pair,
            INTRADAY_RESOLUTION
        )

        if intraday_df is None:

            updated_sell.append(coin)

            continue

        last_1h = intraday_df.iloc[-1]

        # CLOSE BREAKDOWN
        if last_1h["close"] < breakdown_low:

            entry = breakdown_low

            sl = breakdown_high

            risk = sl - entry

            if risk <= 0:
                continue

            qty = RISK_PER_TRADE / risk

            margin = (qty * entry) / LEVERAGE

            # ===== TARGETS =====
            t1 = entry - (risk * 1)
            t2 = entry - (risk * 2)
            t3 = entry - (risk * 3)
            t4 = entry - (risk * 4)

            Send_EMA_Telegram_Message(
                f"🔴 SELL BREAKDOWN CONFIRMED\n\n"
                f"Pair: {pair}\n\n"
                f"1H Candle Close Below Breakdown Low\n\n"
                f"Entry: {entry:.6f}\n"
                f"Stop Loss: {sl:.6f}\n\n"
                f"Risk Per Trade: ₹{RISK_PER_TRADE}\n"
                f"Leverage: {LEVERAGE}x\n"
                f"Quantity: {qty:.4f}\n"
                f"Margin Required: ₹{margin:.2f}\n\n"
                f"Targets:\n"
                f"1:1 → {t1:.6f}\n"
                f"1:2 → {t2:.6f}\n"
                f"1:3 → {t3:.6f}\n"
                f"1:4 → {t4:.6f}"
            )

            print(f"🔴 SELL ALERT: {pair}")

        else:

            updated_sell.append(coin)

    save_json(
        SELL_FILE,
        updated_sell
    )


# ================= MAIN =================

def main():

    print("🚀 Script Started")

    ist = ZoneInfo.timezone(
        "Asia/Kolkata"
    )

    now = datetime.now(ist)

    # # ================= TIME DEBUG =================

    # print("\n================ TIME DEBUG ================\n")

    # print(f"Current IST Time: {now}")

    # print(f"Hour: {now.hour}")

    # print(f"Minute: {now.minute}")

    # print(f"Second: {now.second}")

    # print("\n===========================================\n")

    # ================= RUN ONLY AT 5:30 AM =================

    if now.hour == 5 and 30 <= now.minute <= 35:

        print("✅ DAILY SCAN CONDITION MATCHED")

        run_daily_scan()

    else:

        print("❌ DAILY SCAN CONDITION NOT MATCHED")


    # ================= MONITOR WATCHLIST =================

    monitor_buy_watchlist()

    monitor_sell_watchlist()

    print("✅ Script Completed")


if __name__ == "__main__":
    main()