"""
===========================================================
📊 COINDCX EMA CROSSOVER SCANNER (1-Hour Timeframe)
===========================================================

🔹 PURPOSE:
    Detect bullish and bearish EMA crossovers among top
    USDT futures coins on Coindcx and send Telegram alerts.

🔹 WORKFLOW (Step-by-Step Summary):
    1. Fetch all active USDT futures pairs from Coindcx API.
    2. Get each pair's 1-day percentage change using stats API.
    3. Identify top 10 gainers and top 10 losers.
    4. For these pairs, fetch recent 1-hour candle data.
    5. Compute EMA(14) and EMA(200) from closing prices.
    6. Check for crossover patterns:
        - Bullish → EMA14 crossed above EMA200 on previous candle.
        - Bearish → EMA14 crossed below EMA200 on previous candle.
    7. Send formatted alerts on Telegram:
        🟢 Gainers → EMA14 > EMA200 (Bullish)
        🔴 Losers  → EMA14 < EMA200 (Bearish)
    8. Runs all API calls in parallel using ThreadPoolExecutor
       for faster performance.

🔹 CONFIGURATION:
    - MAX_WORKERS = 15 (threads)
    - EMA periods = [14, 200]
    - Resolution = 1-hour candles ("60")
    - Historical limit = 1000 hours
    - Telegram alerts handled by `send_telegram_message()`

🔹 OUTPUT:
    - Console logs and Telegram messages listing pairs
      where EMA crossovers occurred on the last closed candle.
===========================================================
"""


import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from Telegram_Alert import send_telegram_message
from ADX_Calculater import calculate_adx   

# =====================
# CONFIG
# =====================
MAX_WORKERS = 15
ema_periods = [9, 200]
resolution = "60"  # 1-hour candles
limit_hours = 1000
Adx_Limit = 20
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


    # Check if ADX meets your relaxed pattern
    def is_adx_pattern(adx_series):
        if len(adx_series) < 4:
            return False
        # last closed candle
        last = adx_series.iloc[-2]  # current -1
        if last <= Adx_Limit:
            return False

        # previous 3 candles
        prev3 = adx_series.iloc[-5:-2].tolist()  # current -2, -3, -4

        # relaxed ascending: if any previous candle < last
        for val in prev3:
            if val < last:
                return True
        return False

    def check_gainer(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None or len(df_c) < 3:
            return None
        prev2 = df_c.iloc[-3]  # candle before previous
        prev1 = df_c.iloc[-2]  # previous candle
        # Just crossed above EMA100
        if prev2["EMA_14"] <= prev2["EMA_200"] and prev1["EMA_14"] > prev1["EMA_200"]:
        # Compute ADX
            adx_series = calculate_adx(df_c["high"], df_c["low"], df_c["close"],28)
            if is_adx_pattern(adx_series):
                last_close = prev1["close"]
                last_adx = adx_series.iloc[-2]
                return {"pair": pair, "close": last_close, "adx": round(last_adx, 2)}
        return None

    def check_loser(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None or len(df_c) < 3:
            return None
        prev2 = df_c.iloc[-3]  # candle before previous
        prev1 = df_c.iloc[-2]  # previous candle
        # Just crossed below EMA100
        if prev2["EMA_14"] >= prev2["EMA_200"] and prev1["EMA_14"] < prev1["EMA_200"]:
         # Compute ADX
            adx_series = calculate_adx(df_c["high"], df_c["low"], df_c["close"],28)
            if is_adx_pattern(adx_series):
                last_close = prev1["close"]
                last_adx = adx_series.iloc[-2]  
                return {"pair": pair, "close": last_close, "adx": round(last_adx, 2)}
        return None

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        gain_futs = [executor.submit(check_gainer, p) for p in top_gainers]
        lose_futs = [executor.submit(check_loser, p) for p in top_losers]
        filtered_gainers = [f.result() for f in as_completed(gain_futs) if f.result()]
        filtered_losers = [f.result() for f in as_completed(lose_futs) if f.result()]

    # Step 4: Print & Telegram alerts
    if filtered_gainers:
        msg = "🟢 Gainers (EMA14 crossed above EMA200 + ADX pattern):\n"
        for item in filtered_gainers:
            msg += f"{item['pair']} | Close: {item['close']} | ADX: {item['adx']}\n"
        # print(msg)
        send_telegram_message(msg)

    if filtered_losers:
        msg = "🔴 Losers (EMA14 crossed below EMA200 + ADX pattern):\n"
        for item in filtered_losers:
             msg += f"{item['pair']} | Close: {item['close']} | ADX: {item['adx']}\n"
        # print(msg)
        send_telegram_message(msg)

if __name__ == "__main__":
    main()
