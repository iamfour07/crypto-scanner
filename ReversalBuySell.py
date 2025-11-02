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
upperLimit = 20     # For reversal short
lowerLimit = -20    # For reversal long
ema_periods = [9, 100]

BUY_FILE = "BuyWatchlist.json"
SELL_FILE = "SellWatchlist.json"


# ---------------------
# Fetch active USDT coins
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


# Fetch 1D% change
def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        pc = data.get("price_change_percent", {}).get("1D")
        if pc is None:
            return None
        return {"pair": pair, "change": float(pc)}
    except:
        return None


# Fetch last n candles + EMA computation
def fetch_last_n_candles(pair, n=200):
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        from_time = now - limit_hours * 3600
        url = "https://public.coindcx.com/market_data/candlesticks"
        params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            return None

        df = pd.DataFrame(data)

        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if len(df) > n:
            df = df.iloc[-n:]

        # Compute EMAs
        for p in ema_periods:
            df[f"EMA_{p}"] = df["close"].ewm(span=p, adjust=False).mean()

        df = df.dropna(subset=[f"EMA_{p}" for p in ema_periods]).reset_index(drop=True)

        if len(df) < 3:
            return None

        return df

    except Exception as e:
        print(f"[candles] {pair} error: {e}")
        return None


# Save watchlists
def save_watchlist(buy, sell):
    with open(BUY_FILE, "w") as f:
        json.dump(buy, f, indent=2)
    with open(SELL_FILE, "w") as f:
        json.dump(sell, f, indent=2)


# ---------------------
def main():
    pairs = get_active_usdt_coins()

    changes = []
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_pair_stats, p) for p in pairs]
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                changes.append(res)

    df = pd.DataFrame(changes).dropna()

    new_sell_candidates = df[df["change"] >= upperLimit]["pair"].tolist()
    new_buy_candidates  = df[df["change"] <= lowerLimit]["pair"].tolist()

    # Load previous watchlists
    try:
        with open(SELL_FILE, "r") as f:
            sell_watch = json.load(f)
    except:
        sell_watch = []

    try:
        with open(BUY_FILE, "r") as f:
            buy_watch = json.load(f)
    except:
        buy_watch = []

    # Merge watchlists (unique)
    sell_watch = list(set(sell_watch + new_sell_candidates))
    buy_watch = list(set(buy_watch + new_buy_candidates))


    # ✅ EMA crossover check - BUY (same logic like sell)
    def check_buy(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None: return None

        prev2 = df_c.iloc[-3]
        prev1 = df_c.iloc[-2]

        # ✅ Bullish crossover: EMA9 crosses ABOVE EMA100
        if prev2["EMA_9"] <= prev2["EMA_100"] and prev1["EMA_9"] > prev1["EMA_100"]:
            return pair

        return None


    # ✅ EMA crossover check - SELL
    def check_sell(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None: return None

        prev2 = df_c.iloc[-3]
        prev1 = df_c.iloc[-2]

        # ✅ Bearish crossover: EMA9 crosses BELOW EMA100
        if prev2["EMA_9"] >= prev2["EMA_100"] and prev1["EMA_9"] < prev1["EMA_100"]:
            return pair

        return None


    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        buy_signals  = [f.result() for f in as_completed([executor.submit(check_buy, p) for p in buy_watch]) if f.result()]
        sell_signals = [f.result() for f in as_completed([executor.submit(check_sell, p) for p in sell_watch]) if f.result()]


    # ------------------------------
    # ✅ ALERTS + REMOVE alert fired coins from watchlist
    # ------------------------------
    if buy_signals:
        # print("🟢 Buy Signals:")
        # print("\n".join(buy_signals))
        send_telegram_message("🟢 Buy Signals:\n" + "\n".join(buy_signals))
        buy_watch = [p for p in buy_watch if p not in buy_signals]

    if sell_signals:
        # print("🔴 Sell Signals:")
        # print("\n".join(sell_signals))
        send_telegram_message("🔴 Sell Signals:\n" + "\n".join(sell_signals))
        sell_watch = [p for p in sell_watch if p not in sell_signals]


    # Save updated lists
    save_watchlist(buy_watch, sell_watch)


if __name__ == "__main__":
    main()
