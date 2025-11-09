"""
===========================================================
📊 COINDCX SUPERTREND SCANNER (1-Hour Timeframe)
===========================================================

🔹 PURPOSE:
    Detect bullish and bearish Supertrend signals among top
    USDT futures coins on CoinDCX and send Telegram alerts.

🔹 LOGIC UPDATE:
    ✅ Only check Bullish (Red→Green) in Top 10 Gainers
    ✅ Only check Bearish (Green→Red) in Top 10 Losers

🔹 DEBUG MODE:
    Prints current Supertrend trend (Bullish/Bearish)
    for every analyzed pair.
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
RESOLUTION = "60"   # 1-hour candles
LIMIT_HOURS = 1000  # lookback window

# ✅ Supertrend parameters
ST_LENGTH = 14
ST_FACTOR = 2.0

# =========================================================
# HELPERS
# =========================================================
def rma(series: pd.Series, period: int) -> pd.Series:
    """Wilder's RMA (used by TradingView)"""
    return series.ewm(alpha=1/period, adjust=False).mean()

def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    pairs = []
    for x in data:
        if isinstance(x, str):
            pairs.append(x)
        elif isinstance(x, dict):
            if 'pair' in x:
                pairs.append(x['pair'])
            elif 'symbol' in x:
                pairs.append(x['symbol'])
    return list(dict.fromkeys(pairs))

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

# =========================================================
# SUPERTREND CALCULATION
# =========================================================
def calculate_supertrend(df, period=ST_LENGTH, multiplier=ST_FACTOR):
    """
    TradingView-accurate Supertrend.
    Adds columns: FinalUpper, FinalLower, Supertrend, ST_Trend (True=Bullish)
    """
    for col in ["high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ATR (RMA)
    hl = df["high"] - df["low"]
    hc = (df["high"] - df["close"].shift()).abs()
    lc = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    atr = rma(tr, period)

    # Basic bands
    hl2 = (df["high"] + df["low"]) / 2.0
    upper_basic = hl2 + multiplier * atr
    lower_basic = hl2 - multiplier * atr

    # Final bands
    final_upper = upper_basic.copy()
    final_lower = lower_basic.copy()
    for i in range(1, len(df)):
        prev_fu = final_upper.iloc[i - 1]
        prev_fl = final_lower.iloc[i - 1]
        prev_close = df["close"].iloc[i - 1]

        if (upper_basic.iloc[i] < prev_fu) or (prev_close > prev_fu):
            final_upper.iloc[i] = upper_basic.iloc[i]
        else:
            final_upper.iloc[i] = prev_fu

        if (lower_basic.iloc[i] > prev_fl) or (prev_close < prev_fl):
            final_lower.iloc[i] = lower_basic.iloc[i]
        else:
            final_lower.iloc[i] = prev_fl

    # Determine direction
    st = pd.Series(0.0, index=df.index)
    trend = pd.Series(True, index=df.index)

    if len(df) > 0:
        if df["close"].iloc[period] >= final_lower.iloc[period]:
            st.iloc[period] = final_lower.iloc[period]
            trend.iloc[period] = True
        else:
            st.iloc[period] = final_upper.iloc[period]
            trend.iloc[period] = False

    for i in range(period + 1, len(df)):
        if st.iloc[i - 1] == final_upper.iloc[i - 1]:
            if df["close"].iloc[i] <= final_upper.iloc[i]:
                st.iloc[i] = final_upper.iloc[i]
                trend.iloc[i] = False
            else:
                st.iloc[i] = final_lower.iloc[i]
                trend.iloc[i] = True
        else:
            if df["close"].iloc[i] >= final_lower.iloc[i]:
                st.iloc[i] = final_lower.iloc[i]
                trend.iloc[i] = True
            else:
                st.iloc[i] = final_upper.iloc[i]
                trend.iloc[i] = False

    out = df.copy()
    out["FinalUpper"] = final_upper
    out["FinalLower"] = final_lower
    out["Supertrend"] = st
    out["ST_Trend"] = trend
    return out

# =========================================================
# FETCH CANDLE DATA
# =========================================================
def fetch_last_n_candles(pair, n=200):
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        from_time = now - LIMIT_HOURS * 3600
        url = "https://public.coindcx.com/market_data/candlesticks"
        params = {"pair": pair, "from": from_time, "to": now, "resolution": RESOLUTION, "pcode": "f"}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            return None

        df = pd.DataFrame(data)
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        if "time" in df.columns:
            df = df.sort_values("time").reset_index(drop=True)

        if len(df) > n:
            df = df.iloc[-n:].copy()

        df = calculate_supertrend(df)
        return df.dropna().reset_index(drop=True)
    except Exception as e:
        print(f"[candles] {pair} error: {e}")
        return None

# =========================================================
# MAIN
# =========================================================
def main():
    print("Fetching active USDT pairs...")
    pairs = get_active_usdt_coins()

    # Fetch % change
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

    # Select top 10 gainers & losers
    top_gainers = df.sort_values("change", ascending=False).head(10)["pair"].tolist()
    top_losers  = df.sort_values("change", ascending=True).head(10)["pair"].tolist()

    bullish_signals, bearish_signals = [], []

    def check_signal(pair, side):
        df_c = fetch_last_n_candles(pair)
        if df_c is None or len(df_c) < (ST_LENGTH + 5):
            return None

        prev2, prev1 = df_c.iloc[-3], df_c.iloc[-2]
        latest = df_c.iloc[-1]

        # trend_text = "🟢 Bullish" if latest["ST_Trend"] else "🔴 Bearish"
        # print(f"[DEBUG] {pair} | Close={latest['close']:.4f} | ST={latest['Supertrend']:.4f} | Trend={trend_text}")

        # Check only respective signal type
        if side == "bullish" and (not prev2["ST_Trend"]) and (prev1["ST_Trend"]):
            return ("bullish", pair)
        elif side == "bearish" and (prev2["ST_Trend"]) and (not prev1["ST_Trend"]):
            return ("bearish", pair)
        return None

    # 🔹 Run in parallel (gainers for bullish, losers for bearish)
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        gain_futs = [executor.submit(check_signal, p, "bullish") for p in top_gainers]
        lose_futs = [executor.submit(check_signal, p, "bearish") for p in top_losers]

        for fut in as_completed(gain_futs + lose_futs):
            res = fut.result()
            if res:
                side, pair = res
                if side == "bullish":
                    bullish_signals.append(pair)
                elif side == "bearish":
                    bearish_signals.append(pair)

    # Results
    if bullish_signals:
        msg = "🟢 Bullish Supertrend (1H) signals (Top Gainers):\n" + "\n".join(sorted(bullish_signals))
        # print(msg)
        Telegram_Alert_EMA_Crossover(msg)

    if bearish_signals:
        msg = "🔴 Bearish Supertrend (1H) signals (Top Losers):\n" + "\n".join(sorted(bearish_signals))
        # print(msg)
        Telegram_Alert_EMA_Crossover(msg)

# =========================================================
if __name__ == "__main__":
    main()
