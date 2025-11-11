"""
===========================================================
📊 COINDCX REVERSAL SCANNER (1-Hour Timeframe)
===========================================================

🔹 PURPOSE:
    Detect potential reversal setups among *all USDT futures coins*
    using Bollinger Bands and confirm entry via EMA(9,100) crossover.

🔹 LOGIC SUMMARY:
    1. Fetch all active USDT futures pairs.
    2. For each coin:
         - If previous candle's *low* touches/goes below lower band → add to BUY watchlist.
         - If previous candle's *high* touches/goes above upper band → add to SELL watchlist.
    3. Use saved watchlists to monitor EMA(9,100) crossovers:
         - BUY: EMA9 crosses above EMA100.
         - SELL: EMA9 crosses below EMA100.
    4. Send Telegram alerts and remove alerted coins from watchlists.
===========================================================
"""

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

# 🔥 EMA periods
EMA_FAST = 9
EMA_SLOW = 100
ema_periods = [EMA_FAST, EMA_SLOW]

# 📊 Bollinger Bands
BB_PERIOD = 200
BB_STD = 2

BUY_FILE = "BuyWatchlist.json"
SELL_FILE = "SellWatchlist.json"

# =====================
# TOGGLE OPTIONS
# =====================
ENABLE_BUY = True
ENABLE_SELL = True
# =====================


# ---------------------
# Fetch active USDT futures pairs
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


# Fetch last n candles + compute Bollinger Bands + EMA
def fetch_last_n_candles(pair, n=1000):
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        from_time = now - limit_hours * 3600
        url = "https://public.coindcx.com/market_data/candlesticks"
        params = {
            "pair": pair,
            "from": from_time,
            "to": now,
            "resolution": resolution,
            "pcode": "f",
        }
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

        # ✅ Compute Bollinger Bands
        df["MA"] = df["close"].rolling(BB_PERIOD).mean()
        df["STD"] = df["close"].rolling(BB_PERIOD).std()
        df["Upper_Band"] = df["MA"] + (BB_STD * df["STD"])
        df["Lower_Band"] = df["MA"] - (BB_STD * df["STD"])

        # ✅ Compute EMAs
        for p in ema_periods:
            df[f"EMA_{p}"] = df["close"].ewm(span=p, adjust=False).mean()

        df = df.dropna().reset_index(drop=True)
        if len(df) < 3:
            return None
        return df

    except Exception as e:
        print(f"[candles] {pair} error: {e}")
        return None


# ---------------------
# Save watchlists
def save_watchlist(buy, sell):
    with open(BUY_FILE, "w") as f:
        json.dump(buy, f, indent=2)
    with open(SELL_FILE, "w") as f:
        json.dump(sell, f, indent=2)


# ---------------------
def main():
    pairs = get_active_usdt_coins()
    print(f"Fetched {len(pairs)} active USDT futures pairs.")

    buy_watch, sell_watch = [], []

    # Try loading existing lists
    try:
        with open(BUY_FILE, "r") as f:
            buy_watch = json.load(f)
    except:
        buy_watch = []

    try:
        with open(SELL_FILE, "r") as f:
            sell_watch = json.load(f)
    except:
        sell_watch = []

    # Step 1: Detect new Bollinger Band breakouts
    def check_bollinger(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None or len(df_c) < 3:
            return None

        prev1 = df_c.iloc[-2]  # previous closed candle
        result = {}

        # BUY candidate → Low touches/goes below lower band
        if prev1["low"] <= prev1["Lower_Band"]:
            result["buy"] = pair

        # SELL candidate → High touches/goes above upper band
        if prev1["high"] >= prev1["Upper_Band"]:
            result["sell"] = pair

        return result if result else None

    results = []
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(check_bollinger, p) for p in pairs]
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                results.append(res)

    # Aggregate new candidates
    for r in results:
        if "buy" in r:
            buy_watch.append(r["buy"])
        if "sell" in r:
            sell_watch.append(r["sell"])

    # Deduplicate
    buy_watch = list(set(buy_watch))
    sell_watch = list(set(sell_watch))



    # -------------------------------
    # Step 2: EMA crossover confirmation
    # -------------------------------

    def check_buy(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None or len(df_c) < 3:
            return None

        prev2 = df_c.iloc[-3]
        prev1 = df_c.iloc[-2]

        # Bullish crossover
        if prev2[f"EMA_{EMA_FAST}"] <= prev2[f"EMA_{EMA_SLOW}"] and prev1[f"EMA_{EMA_FAST}"] > prev1[f"EMA_{EMA_SLOW}"]:
            return pair
        return None

    def check_sell(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None or len(df_c) < 3:
            return None

        prev2 = df_c.iloc[-3]
        prev1 = df_c.iloc[-2]

        # Bearish crossover
        if prev2[f"EMA_{EMA_FAST}"] >= prev2[f"EMA_{EMA_SLOW}"] and prev1[f"EMA_{EMA_FAST}"] < prev1[f"EMA_{EMA_SLOW}"]:
            return pair
        return None

    buy_signals, sell_signals = [], []

    if ENABLE_BUY and buy_watch:
        with ThreadPoolExecutor(MAX_WORKERS) as executor:
            buy_signals = [f.result() for f in as_completed([executor.submit(check_buy, p) for p in buy_watch]) if f.result()]

    if ENABLE_SELL and sell_watch:
        with ThreadPoolExecutor(MAX_WORKERS) as executor:
            sell_signals = [f.result() for f in as_completed([executor.submit(check_sell, p) for p in sell_watch]) if f.result()]

    # ------------------------------
    # Step 3: Alerts & update lists
    # ------------------------------
    if ENABLE_BUY and buy_signals:
        send_telegram_message("🟢 BUY Signals (Bollinger + EMA 9/100):\n" + "\n".join(buy_signals))
        buy_watch = [p for p in buy_watch if p not in buy_signals]

    if ENABLE_SELL and sell_signals:
        send_telegram_message("🔴 SELL Signals (Bollinger + EMA 9/100):\n" + "\n".join(sell_signals))
        sell_watch = [p for p in sell_watch if p not in sell_signals]

    # Save updated watchlists
    save_watchlist(buy_watch, sell_watch)


if __name__ == "__main__":
    main()
