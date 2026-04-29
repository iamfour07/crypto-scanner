import requests
import pandas as pd
import json
import os
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from Telegram_EMA import Send_EMA_Telegram_Message

# ================= CONFIG =================
RESOLUTION = "60"
LIMIT_HOURS = 1000
MAX_WORKERS = 20

BUY_FILE = "BuyMomentum.json"
SELL_FILE = "SellMomentum.json"

RISK_PER_TRADE = 500
MAX_CAPITAL = 5000
LEVERAGE = 10


# ================= LOAD / SAVE =================
def load_list(file):
    if os.path.exists(file):
        try:
            with open(file, "r") as f:
                return json.load(f)
        except:
            return []
    return []

def save_list(file, data):
    with open(file, "w") as f:
        json.dump(data, f, indent=2)


# ================= FETCH CANDLES =================
def fetch_candles(pair):
    url = "https://public.coindcx.com/market_data/candlesticks"
    now = int(datetime.now(timezone.utc).timestamp())

    params = {
        "pair": pair,
        "from": now - LIMIT_HOURS * 3600,
        "to": now,
        "resolution": RESOLUTION,
        "pcode": "f"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        if not isinstance(data, dict) or "data" not in data:
            return None

        df = pd.DataFrame(data["data"]).sort_values("time").reset_index(drop=True)

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.iloc[:-1]  # remove running candle

        if len(df) < 120:
            return None

        return df

    except:
        return None


# ================= INDICATORS =================
def add_indicators(df):
    df["ema30"] = df["close"].ewm(span=30, adjust=False).mean()
    df["ema100"] = df["close"].ewm(span=100, adjust=False).mean()
    df["diff"] = df["ema30"] - df["ema100"]

    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(10).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(10).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))

    return df


# ================= EMA CROSS =================
def check_ema_cross(df):
    prev = df.iloc[-2]
    last = df.iloc[-1]

    bullish = prev["diff"] <= 0 and last["diff"] > 0
    bearish = prev["diff"] >= 0 and last["diff"] < 0

    return bullish, bearish


# ================= PIVOT =================
def find_pivot_low(df, left=3, right=3):
    for i in range(len(df) - right - 1, left, -1):
        if all(df["low"].iloc[i] < df["low"].iloc[i - j] for j in range(1, left + 1)) and \
           all(df["low"].iloc[i] < df["low"].iloc[i + j] for j in range(1, right + 1)):
            return df["low"].iloc[i]
    return None

def find_pivot_high(df, left=3, right=3):
    for i in range(len(df) - right - 1, left, -1):
        if all(df["high"].iloc[i] > df["high"].iloc[i - j] for j in range(1, left + 1)) and \
           all(df["high"].iloc[i] > df["high"].iloc[i + j] for j in range(1, right + 1)):
            return df["high"].iloc[i]
    return None


# ================= FALLBACK =================
def fallback_swing_low(df):
    return df["low"].iloc[-10:].min()

def fallback_swing_high(df):
    return df["high"].iloc[-10:].max()


# ================= FETCH PAIRS =================
def get_all_pairs():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    try:
        return [p for p in requests.get(url).json() if isinstance(p, str)]
    except:
        return []


# ================= STATS =================
def get_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        data = requests.get(url, timeout=5).json()
        change = data.get("price_change_percent", {}).get("1D", 0)
        return {"pair": pair, "change": float(change)}
    except:
        return None


# ================= MAIN =================
def main():
    buy_list = load_list(BUY_FILE)
    sell_list = load_list(SELL_FILE)

    pairs = get_all_pairs()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        stats = [r for r in executor.map(get_pair_stats, pairs) if r]

    top_gainers = sorted(stats, key=lambda x: x["change"], reverse=True)[:30]
    top_losers = sorted(stats, key=lambda x: x["change"])[:30]

    # ===== Add Crossovers =====
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        gainers_data = list(executor.map(fetch_candles, [g["pair"] for g in top_gainers]))

    for pair, df in zip([g["pair"] for g in top_gainers], gainers_data):
        if df is None: continue
        df = add_indicators(df)
        bullish, _ = check_ema_cross(df)

        if bullish and not any(c["name"] == pair for c in buy_list):
            buy_list.append({"name": pair, "state": "WAIT_COOLDOWN"})

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        losers_data = list(executor.map(fetch_candles, [l["pair"] for l in top_losers]))

    for pair, df in zip([l["pair"] for l in top_losers], losers_data):
        if df is None: continue
        df = add_indicators(df)
        _, bearish = check_ema_cross(df)

        if bearish and not any(c["name"] == pair for c in sell_list):
            sell_list.append({"name": pair, "state": "WAIT_COOLDOWN"})


    # ===== BUY =====
    updated_buy = []
    for coin in buy_list:
        pair = coin["name"]
        df = fetch_candles(pair)
        if df is None: continue

        df = add_indicators(df)
        prev = df.iloc[-2]
        last = df.iloc[-1]

        # ❌ Remove if trend fails
        if last["ema30"] <= last["ema100"]:
            continue

        state = coin.get("state", "WAIT_COOLDOWN")

        if state == "WAIT_COOLDOWN":
            if prev["rsi"] >= 45 and last["rsi"] < 45:
                coin["state"] = "COOLDOWN_DONE"

        elif state == "COOLDOWN_DONE":
            if prev["rsi"] <= 55 and last["rsi"] > 55:

                entry = last["high"]

                pivot = find_pivot_low(df)
                sl_base = pivot if pivot else fallback_swing_low(df)
                sl = sl_base * 0.998

                risk = entry - sl
                if risk <= 0: 
                    updated_buy.append(coin)
                    continue

                qty = min(RISK_PER_TRADE / risk, (MAX_CAPITAL * LEVERAGE) / entry)
                margin = round((qty * entry) / LEVERAGE, 2)

                Send_EMA_Telegram_Message(
                    f"Status: Buy\nPair: {pair}\nEntry: {entry:.6f}\nStop Loss: {sl:.6f}\nMargin Used: ₹{margin}"
                )

                coin["state"] = "WAIT_COOLDOWN"

        updated_buy.append(coin)


    # ===== SELL =====
    updated_sell = []
    for coin in sell_list:
        pair = coin["name"]
        df = fetch_candles(pair)
        if df is None: continue

        df = add_indicators(df)
        prev = df.iloc[-2]
        last = df.iloc[-1]

        # ❌ Remove if trend fails
        if last["ema30"] >= last["ema100"]:
            continue

        state = coin.get("state", "WAIT_COOLDOWN")

        if state == "WAIT_COOLDOWN":
            if prev["rsi"] <= 55 and last["rsi"] > 55:
                coin["state"] = "COOLDOWN_DONE"

        elif state == "COOLDOWN_DONE":
            if prev["rsi"] >= 45 and last["rsi"] < 45:

                entry = last["low"]

                pivot = find_pivot_high(df)
                sl_base = pivot if pivot else fallback_swing_high(df)
                sl = sl_base * 1.002

                risk = sl - entry
                if risk <= 0:
                    updated_sell.append(coin)
                    continue

                qty = min(RISK_PER_TRADE / risk, (MAX_CAPITAL * LEVERAGE) / entry)
                margin = round((qty * entry) / LEVERAGE, 2)

                Send_EMA_Telegram_Message(
                    f"Status: Sell\nPair: {pair}\nEntry: {entry:.6f}\nStop Loss: {sl:.6f}\nMargin Used: ₹{margin}"
                )

                coin["state"] = "WAIT_COOLDOWN"

        updated_sell.append(coin)


    save_list(BUY_FILE, updated_buy)
    save_list(SELL_FILE, updated_sell)

    print("✅ Done")


if __name__ == "__main__":
    main()