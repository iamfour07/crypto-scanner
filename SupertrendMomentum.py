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
from Telegram_Alert import send_telegram_message
"""
===========================================================
📊 COINDCX SUPERTREND SCANNER (1-Hour Timeframe)
===========================================================

🔹 BEHAVIOR:
    ✅ Supertrend calculated using LIVE candle
    ✅ Signal evaluated on LAST CLOSED candle (current-1)
    ✅ Telegram alerts with Entry, SL, Qty, RR

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
RESOLUTION = "60"      # 1-hour candles
LIMIT_HOURS = 1000

# Supertrend parameters
ST_LENGTH = 14
ST_FACTOR = 2.0

# Risk config
MAX_LOSS_RS = 200
LEVERAGE = 10
RR_LEVELS = [1, 2, 3]

# =========================================================
# HELPERS
# =========================================================
def rma(series, period):
    return series.ewm(alpha=1 / period, adjust=False).mean()

def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    data = requests.get(url, timeout=30).json()
    pairs = []
    for x in data:
        if isinstance(x, str):
            pairs.append(x)
        elif isinstance(x, dict):
            pairs.append(x.get("pair") or x.get("symbol"))
    return list(dict.fromkeys(filter(None, pairs)))

def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        pc = requests.get(url, timeout=10).json().get("price_change_percent", {}).get("1D")
        return {"pair": pair, "change": float(pc)} if pc is not None else None
    except:
        return None

# =========================================================
# SUPERTREND (LIVE-INFLUENCED)
# =========================================================
def calculate_supertrend(df):
    for c in ["high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs()
    ], axis=1).max(axis=1)

    atr = rma(tr, ST_LENGTH)
    hl2 = (df["high"] + df["low"]) / 2

    upper = hl2 + ST_FACTOR * atr
    lower = hl2 - ST_FACTOR * atr

    fu, fl = upper.copy(), lower.copy()

    for i in range(1, len(df)):
        fu.iloc[i] = upper.iloc[i] if upper.iloc[i] < fu.iloc[i-1] or df["close"].iloc[i-1] > fu.iloc[i-1] else fu.iloc[i-1]
        fl.iloc[i] = lower.iloc[i] if lower.iloc[i] > fl.iloc[i-1] or df["close"].iloc[i-1] < fl.iloc[i-1] else fl.iloc[i-1]

    st = pd.Series(index=df.index, dtype=float)
    trend = pd.Series(index=df.index, dtype=bool)

    if df["close"].iloc[ST_LENGTH] >= fl.iloc[ST_LENGTH]:
        st.iloc[ST_LENGTH] = fl.iloc[ST_LENGTH]
        trend.iloc[ST_LENGTH] = True
    else:
        st.iloc[ST_LENGTH] = fu.iloc[ST_LENGTH]
        trend.iloc[ST_LENGTH] = False

    for i in range(ST_LENGTH + 1, len(df)):
        if st.iloc[i-1] == fu.iloc[i-1]:
            if df["close"].iloc[i] <= fu.iloc[i]:
                st.iloc[i] = fu.iloc[i]
                trend.iloc[i] = False
            else:
                st.iloc[i] = fl.iloc[i]
                trend.iloc[i] = True
        else:
            if df["close"].iloc[i] >= fl.iloc[i]:
                st.iloc[i] = fl.iloc[i]
                trend.iloc[i] = True
            else:
                st.iloc[i] = fu.iloc[i]
                trend.iloc[i] = False

    df["Supertrend"] = st
    df["ST_Trend"] = trend
    return df

# =========================================================
# FETCH CANDLES (INCLUDES LIVE)
# =========================================================
def fetch_candles(pair, n=300):
    now = int(datetime.now(timezone.utc).timestamp())
    url = "https://public.coindcx.com/market_data/candlesticks"
    params = {
        "pair": pair,
        "from": now - LIMIT_HOURS * 3600,
        "to": now,
        "resolution": RESOLUTION,
        "pcode": "f",
    }

    data = requests.get(url, params=params, timeout=10).json().get("data", [])
    if not data:
        return None

    df = pd.DataFrame(data).sort_values("time").reset_index(drop=True)
    df = calculate_supertrend(df.iloc[-n:].copy())
    return df.dropna().reset_index(drop=True)

# =========================================================
# RISK & RR CALC
# =========================================================
def calculate_trade_levels(side, c):
    entry, sl = (c["high"], c["low"]) if side == "bullish" else (c["low"], c["high"])
    risk = abs(entry - sl)
    if risk <= 0:
        return None

    qty = int(MAX_LOSS_RS // risk)
    if qty <= 0:
        return None

    return {
        "entry": round(entry, 4),
        "sl": round(sl, 4),
        "qty": qty,
        "capital": round((entry * qty) / LEVERAGE, 2),
        "targets": {
            f"{r}R": round(entry + r * risk if side == "bullish" else entry - r * risk, 4)
            for r in RR_LEVELS
        }
    }

# =========================================================
# MAIN
# =========================================================
def main():
    pairs = get_active_usdt_coins()
    changes = []

    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        for f in as_completed([ex.submit(fetch_pair_stats, p) for p in pairs]):
            if f.result():
                changes.append(f.result())

    df = pd.DataFrame(changes)
    if df.empty:
        return

    top_gainers = df.sort_values("change", ascending=False).head(10)["pair"]
    top_losers = df.sort_values("change").head(10)["pair"]

    def check_signal(pair, side):
        df_c = fetch_candles(pair)
        if df_c is None or len(df_c) < ST_LENGTH + 3:
            return None

        c = df_c.iloc[-2]  # last CLOSED candle

        if side == "bullish" and c["ST_Trend"] and c["low"] <= c["Supertrend"] and c["close"] > c["Supertrend"]:
            trade = calculate_trade_levels("bullish", c)
            if trade:
                return ("BUY", pair, trade)

        if side == "bearish" and (not c["ST_Trend"]) and c["high"] >= c["Supertrend"] and c["close"] < c["Supertrend"]:
            trade = calculate_trade_levels("bearish", c)
            if trade:
                return ("SELL", pair, trade)

        return None

    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        tasks = []
        tasks += [ex.submit(check_signal, p, "bullish") for p in top_gainers]
        tasks += [ex.submit(check_signal, p, "bearish") for p in top_losers]

        for f in as_completed(tasks):
            res = f.result()
            if res:
                side, pair, t = res

                emoji = "🟢" if side == "BUY" else "🔴"
                msg = (
                    f"{emoji} {side} – Supertrend Pullback (1H)\n\n"
                    f"Symbol   : {pair}\n"
                    f"Entry    : {t['entry']}\n"
                    f"SL       : {t['sl']}\n"
                    f"Qty      : {t['qty']}\n"
                    f"Capital  : ₹{t['capital']} (10×)\n\n"
                    f"Targets:\n"
                )

                for rr, price in t["targets"].items():
                    msg += f"{rr} → {price}\n"

                Telegram_Alert_EMA_Crossover(msg)

# =========================================================
if __name__ == "__main__":
    main()

