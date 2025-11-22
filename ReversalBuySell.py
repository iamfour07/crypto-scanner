"""
===========================================================
📊 COINDCX REVERSAL SCANNER (1-Hour Timeframe) — 1D% VERSION
===========================================================

PURPOSE:
- Top gainers -> SELL watchlist
- Top losers  -> BUY watchlist

SIGNALS:
- Supertrend flips (on Heikin-Ashi) => send alerts (can repeat)
- Bollinger Band touch => used ONLY to REMOVE coins from watchlist (no alert)
  - SELL: remove when price <= LowerBB
  - BUY : remove when price >= UpperBB

Everything else same as your previous script.
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

BUY_FILE = "BuyWatchlist.json"
SELL_FILE = "SellWatchlist.json"

ENABLE_BUY = False      # Supertrend buy alerts
ENABLE_SELL = True      # Supertrend sell alerts

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
# API: Fetch Candles
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

        if not data or len(data) < 20:
            return None

        df = pd.DataFrame(data)

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df.dropna().reset_index(drop=True)

    except Exception as e:
        print(f"[candles] {pair} error: {e}")
        return None


# ===================================================
# Heikin-Ashi Conversion
# ===================================================
def convert_to_heikin_ashi(df):
    ha = df.copy()

    ha["HA_Close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4

    ha_open = [(df["open"].iloc[0] + df["close"].iloc[0]) / 2]
    for i in range(1, len(df)):
        ha_open.append((ha_open[i - 1] + ha["HA_Close"].iloc[i - 1]) / 2)

    ha["HA_Open"] = ha_open
    ha["HA_High"] = ha[["HA_Open", "HA_Close", "high"]].max(axis=1)
    ha["HA_Low"] = ha[["HA_Open", "HA_Close", "low"]].min(axis=1)

    return ha


# ===================================================
# Supertrend Calculation (on Heikin-Ashi)
# ===================================================
def compute_supertrend(df, period=90, multiplier=2):
    hl2 = (df["HA_High"] + df["HA_Low"]) / 2
    # ATR-like using rolling high-low range
    df["atr"] = hl2.rolling(period).apply(lambda x: x.max() - x.min(), raw=True)

    df["upperband"] = hl2 + multiplier * df["atr"]
    df["lowerband"] = hl2 - multiplier * df["atr"]

    trend = [True]  # True = Uptrend (GREEN)
    for i in range(1, len(df)):
        if df["HA_Close"].iloc[i] > df["upperband"].iloc[i - 1]:
            trend.append(True)
        elif df["HA_Close"].iloc[i] < df["lowerband"].iloc[i - 1]:
            trend.append(False)
        else:
            trend.append(trend[-1])

    df["supertrend"] = trend
    return df


# ===================================================
# Bollinger Bands (for removal only)
# ===================================================
def get_bollinger_last(df, period=200, mult=2):
    # Uses normal close prices (not HA). If you prefer HA_Close, change to ha["HA_Close"]
    df["MA20"] = df["close"].rolling(period).mean()
    df["STD"] = df["close"].rolling(period).std()
    df["UpperBB"] = df["MA20"] + mult * df["STD"]
    df["LowerBB"] = df["MA20"] - mult * df["STD"]

    last = df.iloc[-1]
    return float(last["close"]), float(last["UpperBB"]), float(last["LowerBB"])


# ===================================================
# Save Watchlist
# ===================================================
def save_watchlist(buy, sell):
    with open(BUY_FILE, "w") as f:
        json.dump(buy, f, indent=2)
    with open(SELL_FILE, "w") as f:
        json.dump(sell, f, indent=2)


# ===================================================
# Main Logic
# ===================================================
def main():
    pairs = get_active_usdt_coins()
    buy_watch, sell_watch = [], []

    # Load existing watchlists if present
    try:
        buy_watch = json.load(open(BUY_FILE))
    except:
        buy_watch = []

    try:
        sell_watch = json.load(open(SELL_FILE))
    except:
        sell_watch = []

    # Step 1: Fetch 1D% Change and select top gainers/losers
    def get_top_gainers_and_losers(pairs):
        changes = []
        with ThreadPoolExecutor(MAX_WORKERS) as executor:
            futures = [executor.submit(fetch_pair_stats, p) for p in pairs]
            for fut in as_completed(futures):
                res = fut.result()
                if res:
                    changes.append(res)

        changes.sort(key=lambda x: x["change"], reverse=True)

        # selecting top 5 and bottom 5 (previous code used 5)
        top_gainers = [x["pair"] for x in changes[:5]]
        top_losers = [x["pair"] for x in changes[-5:]]
        return top_gainers, top_losers

    top_gainers, top_losers = get_top_gainers_and_losers(pairs)

    # Add to watchlists (keep existing ones)
    sell_watch.extend(top_gainers)
    buy_watch.extend(top_losers)

    # dedupe
    sell_watch = list(dict.fromkeys(sell_watch))
    buy_watch = list(dict.fromkeys(buy_watch))

    # ===================================================
    # SUPERTREND ALERTS (Heikin-Ashi) - these are sent and do NOT remove coins
    # ===================================================
    buy_signals, sell_signals = [], []

    def check_buy_supertrend(pair):
        df = fetch_last_n_candles(pair)
        if df is None or len(df) < 25:  # need enough data
            return None
        ha = convert_to_heikin_ashi(df)
        st = compute_supertrend(ha)
        prev = st.iloc[-2]["supertrend"]
        last = st.iloc[-1]["supertrend"]
        # BUY if ST flips RED -> GREEN
        if prev is False and last is True:
            return pair
        return None

    def check_sell_supertrend(pair):
        df = fetch_last_n_candles(pair)
        if df is None or len(df) < 25:
            return None
        ha = convert_to_heikin_ashi(df)
        st = compute_supertrend(ha)
        prev = st.iloc[-2]["supertrend"]
        last = st.iloc[-1]["supertrend"]
        # SELL if ST flips GREEN -> RED
        if prev is True and last is False:
            return pair
        return None

    # Run supertrend checks concurrently, collect results (these will be alerted and NOT removed)
    if ENABLE_BUY and buy_watch:
        with ThreadPoolExecutor(MAX_WORKERS) as executor:
            futures = [executor.submit(check_buy_supertrend, p) for p in buy_watch]
            for fut in as_completed(futures):
                res = fut.result()
                if res:
                    buy_signals.append(res)

    if ENABLE_SELL and sell_watch:
        with ThreadPoolExecutor(MAX_WORKERS) as executor:
            futures = [executor.submit(check_sell_supertrend, p) for p in sell_watch]
            for fut in as_completed(futures):
                res = fut.result()
                if res:
                    sell_signals.append(res)

    # Send Supertrend alerts (these do NOT remove coins)
    if ENABLE_BUY and buy_signals:
        send_telegram_message("🟢 BUY Signals (1D% + SUPER-TREND):\n" + "\n".join(buy_signals))

    if ENABLE_SELL and sell_signals:
        send_telegram_message("🔴 SELL Signals (1D% + SUPER-TREND):\n" + "\n".join(sell_signals))

    # ===================================================
    # BOLLINGER-BASED REMOVAL (NO ALERTS WHEN REMOVING)
    # - SELL watchlist: remove when close <= LowerBB
    # - BUY watchlist:  remove when close >= UpperBB
    # ===================================================
    sell_remove = []
    for coin in sell_watch:
        df = fetch_last_n_candles(coin)
        if df is None or len(df) < 25:
            continue
        close, upper, lower = get_bollinger_last(df)
        # Remove silently when touching LOWER band
        if close <= lower:
            sell_remove.append(coin)

    buy_remove = []
    for coin in buy_watch:
        df = fetch_last_n_candles(coin)
        if df is None or len(df) < 25:
            continue
        close, upper, lower = get_bollinger_last(df)
        # Remove silently when touching UPPER band
        if close >= upper:
            buy_remove.append(coin)

    # Apply removals (silent: NO telegram)
    if sell_remove:
        sell_watch = [c for c in sell_watch if c not in sell_remove]

    if buy_remove:
        buy_watch = [c for c in buy_watch if c not in buy_remove]

    # Save watchlists
    save_watchlist(buy_watch, sell_watch)


if __name__ == "__main__":
    main()
