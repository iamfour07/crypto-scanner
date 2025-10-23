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
ema_periods = [9, 30, 100]

# Filters (set to False for pure crossover without candle-close logic)
USE_PRICE_FILTER = False     # require price to be above/below both EMAs after cross
USE_SLOPE_FILTER = False     # require EMA60 slope in signal direction
EPSILON = 1e-8               # tiny gap to avoid float jitter

BUY_FILE = "BuyWatchlist.json"
SELL_FILE = "SellWatchlist.json"

# Debounce memory: last processed bar open time per pair
last_bar_time = {}

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
    except Exception as e:
        print(f"[stats] {pair} error: {e}")
        return None

# Fetch last n candles
def fetch_last_n_candles(pair, n=200):
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        from_time = now - limit_hours * 3600
        url = "https://public.coindcx.com/market_data/candlesticks"
        params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data or len(data) < max(ema_periods) + 5:
            return None
        df = pd.DataFrame(data)

        # Ensure numeric types
        for col in ["open","high","low","close","volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        # Use only the last n rows to reduce compute
        if len(df) > n:
            df = df.iloc[-n:].copy()

        # Compute EMAs
        for p in ema_periods:
            df[f"EMA_{p}"] = df["close"].ewm(span=p, adjust=False).mean()

        # Drop rows until EMAs are valid
        df = df.dropna(subset=[f"EMA_{p}" for p in [15, 60]]).reset_index(drop=True)
        if len(df) < 3:
            return None

        return df
    except Exception as e:
        print(f"[candles] {pair} error: {e}")
        return None

# Intrabar EMA crossover (no candle close requirement)
def check_crossover_intrabar(df):
    cur = df.iloc[-1]
    prev = df.iloc[-2]

    ema15_cur = cur["EMA_9"]
    ema60_cur = cur["EMA_100"]
    ema15_prev = prev["EMA_9"]
    ema60_prev = prev["EMA_100"]

    cross_up_raw = (ema15_prev <= ema60_prev) and (ema15_cur > ema60_cur + EPSILON)
    cross_dn_raw = (ema15_prev >= ema60_prev) and (ema15_cur < ema60_cur - EPSILON)

    if USE_SLOPE_FILTER:
        slope_up = ema60_cur > ema60_prev
        slope_dn = ema60_cur < ema60_prev
    else:
        slope_up = slope_dn = True

    if USE_PRICE_FILTER:
        close_cur = cur["close"]
        price_ok_up = (close_cur > ema60_cur) and (close_cur > ema15_cur)
        price_ok_dn = (close_cur < ema60_cur) and (close_cur < ema15_cur)
    else:
        price_ok_up = price_ok_dn = True

    cross_up = cross_up_raw and slope_up and price_ok_up
    cross_down = cross_dn_raw and slope_dn and price_ok_dn

    bar_time_col = "timestamp" if "timestamp" in df.columns else ("time" if "time" in df.columns else None)
    cur_bar_time = int(cur[bar_time_col]) if (bar_time_col and pd.notna(cur[bar_time_col])) else df.index[-1]

    return cross_up, cross_down, cur_bar_time

def save_watchlist(buy_list, sell_list):
    with open(BUY_FILE, "w") as f:
        json.dump(buy_list, f, indent=2)
    with open(SELL_FILE, "w") as f:
        json.dump(sell_list, f, indent=2)

# ---------------------
def main():
    pairs = get_active_usdt_coins()

    # Step 1: Fetch 1D changes
    changes = []
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_pair_stats, p) for p in pairs]
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                changes.append(res)

    df = pd.DataFrame(changes).dropna()

    # Step 2: Find new candidates
    new_sell_candidates = df[df["change"] >= upperLimit].sort_values("change", ascending=False)["pair"].tolist()
    new_buy_candidates  = df[df["change"] <= lowerLimit].sort_values("change", ascending=True)["pair"].tolist()

    # Step 3: Load old watchlists
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

    # Step 4: Merge new + old (no duplicates)
    sell_watch = list(set(sell_watch + new_sell_candidates))
    buy_watch  = list(set(buy_watch + new_buy_candidates))

    # ------------------------------
    # Step 5: EMA SCAN on Watchlists
    # ------------------------------
    buy_signals = []
    sell_signals = []

    # --- Buy Watchlist Check ---
    def process_buy(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None or len(df_c) < 3:
            return None
        prev2 = df_c.iloc[-3]  # candle before previous
        prev1 = df_c.iloc[-2]  # previous closed candle
        if prev2["EMA_9"] <= prev2["EMA_100"] and prev1["EMA_9"] > prev1["EMA_100"]:
            return pair
        return None

    # --- Sell Watchlist Check ---
    def process_sell(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None or len(df_c) < 3:
            return None
        prev2 = df_c.iloc[-3]  # candle before previous
        prev1 = df_c.iloc[-2]  # previous closed candle
        if prev2["EMA_9"] >= prev2["EMA_100"] and prev1["EMA_9"] < prev1["EMA_100"]:
            return pair
        return None

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        buy_futs = [executor.submit(process_buy, p) for p in buy_watch]
        sell_futs = [executor.submit(process_sell, p) for p in sell_watch]

        buy_signals = [f.result() for f in as_completed(buy_futs) if f.result()]
        sell_signals = [f.result() for f in as_completed(sell_futs) if f.result()]

    # ------------------------------
    # Step 6: Handle Signals
    # ------------------------------
    if buy_signals:
        print("🟢 Buy Signals:")
        for p in buy_signals:
            print(f"  {p}")
        send_telegram_message("🟢 Buy Signals:\n" + "\n".join(buy_signals))
        buy_watch = [p for p in buy_watch if p not in buy_signals]

    if sell_signals:
        print("🔴 Sell Signals:")
        for p in sell_signals:
            print(f"  {p}")
        send_telegram_message("🔴 Sell Signals:\n" + "\n".join(sell_signals))
        sell_watch = [p for p in sell_watch if p not in sell_signals]

    # ------------------------------
    # Step 7: Save Updated Watchlists
    # ------------------------------
    save_watchlist(buy_watch, sell_watch)

if __name__ == "__main__":
    main()
