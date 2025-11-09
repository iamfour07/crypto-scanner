"""
===========================================================
📊 COINDCX STOCHASTIC OSCILLATOR SCANNER (1-Hour Timeframe)
===========================================================

🔹 PURPOSE:
    - Get Top 10 Gainers & Top 10 Losers (based on 1D change)
    - On gainers: detect BUY signal (%K cross above %D below 20)
    - On losers: detect SELL signal (%K cross below %D above 80)
    - Send Telegram alerts for signals.

🔹 INDICATOR:
    Stochastic Oscillator (21, 5, 5)

===========================================================
"""

import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from Telegram_Alert_EMA_Crossover import Telegram_Alert_EMA_Crossover

# =====================
# CONFIG
# =====================
MAX_WORKERS = 15
resolution = "60"  # 1-hour candles
limit_hours = 1000

# Stochastic configuration
STOCH_PERIOD = 21
STOCH_K_SMOOTH = 5
STOCH_D_SMOOTH = 5


# =====================
# Fetch active USDT coins
# =====================
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[ERROR] Unable to fetch active pairs: {e}")
        return []


# =====================
# Fetch 1D % change
# =====================
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
        print(f"[STATS ERROR] {pair}: {e}")
        return None


# =====================
# Fetch last n candles + Stochastic Oscillator
# =====================
def fetch_last_n_candles(pair, n=200):
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        from_time = now - limit_hours * 3600
        url = "https://public.coindcx.com/market_data/candlesticks"
        params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data or len(data) < STOCH_PERIOD + 5:
            return None

        df = pd.DataFrame(data)
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Compute Stochastic Oscillator
        low_min = df["low"].rolling(window=STOCH_PERIOD).min()
        high_max = df["high"].rolling(window=STOCH_PERIOD).max()
        df["%K"] = 100 * (df["close"] - low_min) / (high_max - low_min)
        df["%K"] = df["%K"].rolling(window=STOCH_K_SMOOTH).mean()
        df["%D"] = df["%K"].rolling(window=STOCH_D_SMOOTH).mean()
        df = df.dropna().reset_index(drop=True)

        if len(df) < 3:
            return None
        return df

    except Exception as e:
        print(f"[CANDLES ERROR] {pair}: {e}")
        return None


# =====================
# MAIN LOGIC
# =====================
def main():
    print("🚀 Fetching active USDT pairs...")
    pairs = get_active_usdt_coins()
    if not pairs:
        print("❌ No pairs found.")
        return

    # Step 1: Get % change data
    changes = []
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_pair_stats, p) for p in pairs]
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                changes.append(res)

    df = pd.DataFrame(changes).dropna()
    if df.empty:
        print("⚠️ No stats data available.")
        return

    # Step 2: Identify Top 10 gainers & losers
    top_gainers = df.sort_values("change", ascending=False).head(10)["pair"].tolist()
    top_losers  = df.sort_values("change", ascending=True).head(10)["pair"].tolist()

    # Step 3: Define signal checkers
    def check_buy_signal(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None:
            return None

        prev2 = df_c.iloc[-3]
        prev1 = df_c.iloc[-2]

        # 🟢 Bullish crossover: %K crosses above %D below 20
        if (
            prev2["%K"] < prev2["%D"]
            and prev1["%K"] > prev1["%D"]
            and prev1["%K"] < 20
            and prev1["%D"] < 20
        ):
            return pair
        return None

    def check_sell_signal(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None:
            return None

        prev2 = df_c.iloc[-3]
        prev1 = df_c.iloc[-2]

        # 🔴 Bearish crossover: %K crosses below %D above 80
        if (
            prev2["%K"] > prev2["%D"]
            and prev1["%K"] < prev1["%D"]
            and prev1["%K"] > 80
            and prev1["%D"] > 80
        ):
            return pair
        return None

    # Step 4: Run checks in parallel
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        buy_futures = [executor.submit(check_buy_signal, p) for p in top_gainers]
        sell_futures = [executor.submit(check_sell_signal, p) for p in top_losers]

        buy_signals = [f.result() for f in as_completed(buy_futures) if f.result()]
        sell_signals = [f.result() for f in as_completed(sell_futures) if f.result()]

    # Step 5: Send Telegram Alerts
    if buy_signals:
        msg = "🟢 *BUY Signals (Top Gainers)* — %K cross above %D below 20:\n" + "\n".join(buy_signals)
        print(msg)
        Telegram_Alert_EMA_Crossover(msg)

    if sell_signals:
        msg = "🔴 *SELL Signals (Top Losers)* — %K cross below %D above 80:\n" + "\n".join(sell_signals)
        Telegram_Alert_EMA_Crossover(msg)

if __name__ == "__main__":
    main()
