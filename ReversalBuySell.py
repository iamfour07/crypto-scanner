
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
upperLimit = 20      # For reversal short
lowerLimit = -20     # For reversal long
ema_periods = [9, 30, 100]

# Filters (set to False for pure crossover without candle-close logic)
USE_PRICE_FILTER = False     # require price to be above/below both EMAs after cross
USE_SLOPE_FILTER = False     # require EMA100 slope in signal direction
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
        # Log and skip this pair
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
        df = df.dropna(subset=[f"EMA_{p}" for p in [9, 100]]).reset_index(drop=True)
        if len(df) < 3:
            return None

        return df
    except Exception as e:
        print(f"[candles] {pair} error: {e}")
        return None

# Intrabar EMA crossover (no candle close requirement)
def check_crossover_intrabar(df):
    # current bar and previous bar
    cur = df.iloc[-1]       # live/last bar
    prev = df.iloc[-2]      # prior bar

    ema9_cur = cur["EMA_9"]
    ema100_cur = cur["EMA_100"]
    ema9_prev = prev["EMA_9"]
    ema100_prev = prev["EMA_100"]

    # raw crosses
    cross_up_raw = (ema9_prev <= ema100_prev) and (ema9_cur > ema100_cur + EPSILON)
    cross_dn_raw = (ema9_prev >= ema100_prev) and (ema9_cur < ema100_cur - EPSILON)

    if USE_SLOPE_FILTER:
        slope_up = ema100_cur > ema100_prev
        slope_dn = ema100_cur < ema100_prev
    else:
        slope_up = slope_dn = True

    if USE_PRICE_FILTER:
        close_cur = cur["close"]
        price_ok_up = (close_cur > ema100_cur) and (close_cur > ema9_cur)
        price_ok_dn = (close_cur < ema100_cur) and (close_cur < ema9_cur)
    else:
        price_ok_up = price_ok_dn = True

    cross_up = cross_up_raw and slope_up and price_ok_up
    cross_down = cross_dn_raw and slope_dn and price_ok_dn

    # Debounce per bar using bar start time if provided; else index timestamp if available
    # CoinDCX candlesticks usually include "timestamp" or "time"
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

    # Fetch 1D% changes
    changes = []
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_pair_stats, p) for p in pairs]
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                changes.append(res)

    df = pd.DataFrame(changes).dropna()
    sell_watch = df[df["change"] >= upperLimit].sort_values("change", ascending=False).head(10)["pair"].tolist()
    buy_watch  = df[df["change"] <= lowerLimit].sort_values("change", ascending=True).head(10)["pair"].tolist()

    # print(f"Buy Watchlist: {buy_watch}")
    # print(f"Sell Watchlist: {sell_watch}")

    # Processing helpers (collect results first; avoid mutating lists during iteration)
    def process_pair_for_buy(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None:
            return None
        cross_up, _, bar_time = check_crossover_intrabar(df_c)
        if not cross_up:
            return None
        # Debounce: only one signal per bar
        if last_bar_time.get(("BUY", pair)) == bar_time:
            return None
        last_bar_time[("BUY", pair)] = bar_time
        return pair

    def process_pair_for_sell(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None:
            return None
        _, cross_down, bar_time = check_crossover_intrabar(df_c)
        if not cross_down:
            return None
        if last_bar_time.get(("SELL", pair)) == bar_time:
            return None
        last_bar_time[("SELL", pair)] = bar_time
        return pair

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        buy_futs = [executor.submit(process_pair_for_buy, p) for p in buy_watch]
        sell_futs = [executor.submit(process_pair_for_sell, p) for p in sell_watch]

        buy_signals = [f.result() for f in as_completed(buy_futs)]
        sell_signals = [f.result() for f in as_completed(sell_futs)]

    buy_signals = [p for p in buy_signals if p]
    sell_signals = [p for p in sell_signals if p]

    # Optionally remove signaled pairs from watch lists
    buy_watch = [p for p in buy_watch if p not in buy_signals]
    sell_watch = [p for p in sell_watch if p not in sell_signals]

    save_watchlist(buy_watch, sell_watch)

    if buy_signals:
        print("🟢 Buy Signals:")
        for p in buy_signals:
            print(f"  {p}")
        send_telegram_message("🟢 Buy Signals:\n" + "\n".join(f"  {p}" for p in buy_signals))

    if sell_signals:
        print("🔴 Sell Signals:")
        for p in sell_signals:
            print(f"  {p}")
        send_telegram_message("🔴 Sell Signals:\n" + "\n".join(f"  {p}" for p in sell_signals))

if __name__ == "__main__":
    main()
