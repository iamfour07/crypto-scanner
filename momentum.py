import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from Telegram_Alert import send_telegram_message

# =====================
# CONFIG
# =====================
MAX_WORKERS = 15
ema_periods = [14, 200]
resolution = "60"  # 1-hour candles
limit_hours = 1000

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

# Fetch last n candles and compute EMAs
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
        # Ensure numeric
        for col in ["open","high","low","close","volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        # Use last n rows
        if len(df) > n:
            df = df.iloc[-n:].copy()
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

# ---------------------
def main():
    print("Fetching active USDT pairs...")
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
    if df.empty:
        print("No data fetched!")
        return

    # Step 2: Get top gainers and losers
    top_gainers = df.sort_values("change", ascending=False).head(10)["pair"].tolist()
    top_losers  = df.sort_values("change", ascending=True).head(10)["pair"].tolist()

    # Step 3: EMA Filter
    filtered_gainers = []
    filtered_losers = []

    def check_gainer(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None or len(df_c) < 3:
            return None
        prev2 = df_c.iloc[-3]  # candle before previous
        prev1 = df_c.iloc[-2]  # previous candle
        # Just crossed above EMA100
        if prev2["EMA_14"] <= prev2["EMA_200"] and prev1["EMA_14"] > prev1["EMA_200"]:
            return pair
        return None

    def check_loser(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None or len(df_c) < 3:
            return None
        prev2 = df_c.iloc[-3]  # candle before previous
        prev1 = df_c.iloc[-2]  # previous candle
        # Just crossed below EMA100
        if prev2["EMA_14"] >= prev2["EMA_200"] and prev1["EMA_14"] < prev1["EMA_200"]:
            return pair
        return None

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        gain_futs = [executor.submit(check_gainer, p) for p in top_gainers]
        lose_futs = [executor.submit(check_loser, p) for p in top_losers]
        filtered_gainers = [f.result() for f in as_completed(gain_futs) if f.result()]
        filtered_losers = [f.result() for f in as_completed(lose_futs) if f.result()]

    # Step 4: Print & Telegram alerts
    if filtered_gainers:
        msg = "🟢 Gainers (EMA15 crossed above EMA60 on prev candle):\n" + "\n".join(filtered_gainers)
        print(msg)
        send_telegram_message(msg)

    if filtered_losers:
        msg = "🔴 Losers (EMA15 crossed below EMA60 on prev candle):\n" + "\n".join(filtered_losers)
        print(msg)
        send_telegram_message(msg)

if __name__ == "__main__":
    main()
