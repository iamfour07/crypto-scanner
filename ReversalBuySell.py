
"""
===========================================================
📊 COINDCX REVERSAL SCANNER (1-Hour Timeframe) — 1D% VERSION
===========================================================

🔹 PURPOSE:
    Detect reversal opportunities among top 10 gainers & losers
    using EMA(9,100) crossover confirmation.

🔹 NEW LOGIC SUMMARY:
    1. Fetch all USDT futures coins
    2. Pull 1D% change for each pair
    3. Select:
        - Top 10 gainers → SELL WATCHLIST
        - Top 10 losers  → BUY WATCHLIST
    4. For watchlisted coins:
        - BUY: EMA9 crosses above EMA100
        - SELL: EMA9 crosses below EMA100
    5. Send telegram alerts
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

EMA_FAST = 9
EMA_SLOW = 100

BUY_FILE = "BuyWatchlist.json"
SELL_FILE = "SellWatchlist.json"

ENABLE_BUY = False
ENABLE_SELL = True


# ===================================================
# API: Fetch all USDT futures pairs
# ===================================================
def get_active_usdt_coins():
    url = (
        "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
        "active_instruments?margin_currency_short_name[]=USDT"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ===================================================
# API: Fetch 1D % change
# ===================================================
def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        pc = r.json().get("price_change_percent", {}).get("1D")
        return {"pair": pair, "change": float(pc)} if pc else None
    except:
        return None


# ===================================================
# API: Fetch last N candles and EMA
# ===================================================
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
        if not data or len(data) < 3:  # Only use valid data
            return None

        df = pd.DataFrame(data)
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # ✅ Compute EMAs
        df["EMA9"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
        df["EMA100"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()

        df = df.dropna().reset_index(drop=True)
        return df

    except Exception as e:
        print(f"[candles] {pair} error: {e}")
        return None


# ===================================================
# Save Watchlist
# ===================================================
def save_watchlist(buy, sell):
    with open(BUY_FILE, "w") as f:
        json.dump(buy, f, indent=2)
    with open(SELL_FILE, "w") as f:
        json.dump(sell, f, indent=2)


# ===================================================
# Main Logic: 1D% Gainers and Losers with EMA crossover
# ===================================================
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

    # Step 1: Fetch 1D% Change and Identify Top 10 Gainers and Losers
    def get_top_gainers_and_losers(pairs):
        changes = []
        with ThreadPoolExecutor(MAX_WORKERS) as executor:
            futures = [executor.submit(fetch_pair_stats, p) for p in pairs]
            for fut in as_completed(futures):
                result = fut.result()
                if result:
                    changes.append(result)

        # Sort the coins by 1D % change and select the top gainers and losers
        changes.sort(key=lambda x: x["change"], reverse=True)

        # Top 10 gainers and losers
        top_gainers = [x["pair"] for x in changes[:5]]
        top_losers = [x["pair"] for x in changes[-5:]]
        return top_gainers, top_losers

    # Get the top 10 gainers and losers
    top_gainers, top_losers = get_top_gainers_and_losers(pairs)

    # Add top gainers to SELL watchlist and top losers to BUY watchlist
    sell_watch.extend(top_gainers)
    buy_watch.extend(top_losers)

    # Remove duplicates
    sell_watch = list(set(sell_watch))
    buy_watch = list(set(buy_watch))

    # ----------------------------------------
    # Step 2: EMA Crossover Confirmation
    # ----------------------------------------

    def check_buy(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None or len(df_c) < 3:
            return None

        prev2 = df_c.iloc[-3]
        prev1 = df_c.iloc[-2]

        # Bullish crossover (EMA9 crosses above EMA100)
        if prev2["EMA9"] <= prev2["EMA100"] and prev1["EMA9"] > prev1["EMA100"]:
            return pair
        return None

    def check_sell(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None or len(df_c) < 3:
            return None

        prev2 = df_c.iloc[-3]
        prev1 = df_c.iloc[-2]

        # Bearish crossover (EMA9 crosses below EMA100)
        if prev2["EMA9"] >= prev2["EMA100"] and prev1["EMA9"] < prev1["EMA100"]:
            return pair
        return None

    buy_signals, sell_signals = [], []

    # Check buy signals for top gainers
    if ENABLE_BUY and buy_watch:
        with ThreadPoolExecutor(MAX_WORKERS) as executor:
            buy_signals = [f.result() for f in as_completed([executor.submit(check_buy, p) for p in buy_watch]) if f.result()]

    # Check sell signals for top losers
    if ENABLE_SELL and sell_watch:
        with ThreadPoolExecutor(MAX_WORKERS) as executor:
            sell_signals = [f.result() for f in as_completed([executor.submit(check_sell, p) for p in sell_watch]) if f.result()]

    # ------------------------------
    # Step 3: Alerts & update lists
    # ------------------------------
    if ENABLE_BUY and buy_signals:
        send_telegram_message("🟢 BUY Signals (1D% + EMA 9/100):\n" + "\n".join(buy_signals))
        buy_watch = [p for p in buy_watch if p not in buy_signals]

    if ENABLE_SELL and sell_signals:
        send_telegram_message("🔴 SELL Signals (1D% + EMA 9/100):\n" + "\n".join(sell_signals))
        sell_watch = [p for p in sell_watch if p not in sell_signals]

    # Save updated watchlists
    save_watchlist(buy_watch, sell_watch)


if __name__ == "__main__":
    main()
