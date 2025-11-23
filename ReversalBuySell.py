"""
===========================================================
📊 COINDCX REVERSAL SCANNER (1-Hour Timeframe) — 1D% VERSION
===========================================================

PURPOSE:
- Top gainers -> SELL watchlist
- Top losers  -> BUY watchlist

SIGNALS:
- Supertrend flips (on Heikin-Ashi) → send alerts (can repeat)
- Bollinger Band touch → used ONLY to REMOVE coins from watchlist (no alert)
  - SELL: remove when price <= LowerBB
  - BUY : remove when price >= UpperBB
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

ENABLE_BUY = False      # BUY alerts ON/OFF
ENABLE_SELL = True      # SELL alerts ON/OFF


# ===================================================
# 1. FETCH ACTIVE FUTURES PAIRS
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
# 2. FETCH 1D% CHANGE
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
# 3. FETCH CANDLE DATA
# ===================================================
def fetch_last_n_candles(pair):
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

        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("data", [])

        if not data or len(data) < 30:
            return None

        df = pd.DataFrame(data)
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df.dropna().reset_index(drop=True)

    except Exception as e:
        print(f"[candles-error] {pair} → {e}")
        return None


# ===================================================
# 4. HEIKIN-ASHI CONVERSION
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
# 5. TRADINGVIEW ACCURATE SUPERTREND (HA VERSION)
# ===================================================
def supertrend_tv_ha(df, period=90, multiplier=1.8):
    df = df.copy()

    # True Range
    hl = df["HA_High"] - df["HA_Low"]
    hc = (df["HA_High"] - df["HA_Close"].shift()).abs()
    lc = (df["HA_Low"] - df["HA_Close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)

    # ATR = RMA/Wilder
    atr = tr.ewm(alpha=1/period, adjust=False).mean()

    # Basic Bands
    hl2 = (df["HA_High"] + df["HA_Low"]) / 2
    upper_basic = hl2 + multiplier * atr
    lower_basic = hl2 - multiplier * atr

    # Final Bands
    final_upper = upper_basic.copy()
    final_lower = lower_basic.copy()

    for i in range(1, len(df)):
        prev_fu = final_upper.iloc[i-1]
        prev_fl = final_lower.iloc[i-1]
        prev_close = df["HA_Close"].iloc[i-1]

        final_upper.iloc[i] = (
            upper_basic.iloc[i]
            if (upper_basic.iloc[i] < prev_fu) or (prev_close > prev_fu)
            else prev_fu
        )

        final_lower.iloc[i] = (
            lower_basic.iloc[i]
            if (lower_basic.iloc[i] > prev_fl) or (prev_close < prev_fl)
            else prev_fl
        )

    # Supertrend Line + Direction
    st_val = pd.Series(index=df.index, dtype=float)
    trend = pd.Series(index=df.index, dtype=bool)

    start = period
    if df["HA_Close"].iloc[start] >= final_lower.iloc[start]:
        st_val.iloc[start] = final_lower.iloc[start]
        trend.iloc[start] = True
    else:
        st_val.iloc[start] = final_upper.iloc[start]
        trend.iloc[start] = False

    for i in range(start+1, len(df)):
        prev_st = st_val.iloc[i-1]
        close = df["HA_Close"].iloc[i]

        if prev_st == final_upper.iloc[i-1]:  # DOWN TREND
            if close <= final_upper.iloc[i]:
                st_val.iloc[i] = final_upper.iloc[i]
                trend.iloc[i] = False
            else:
                st_val.iloc[i] = final_lower.iloc[i]
                trend.iloc[i] = True

        else:  # UP TREND
            if close >= final_lower.iloc[i]:
                st_val.iloc[i] = final_lower.iloc[i]
                trend.iloc[i] = True
            else:
                st_val.iloc[i] = final_upper.iloc[i]
                trend.iloc[i] = False

    df["ST_Trend"] = trend           # True = GREEN, False = RED
    df["Supertrend"] = st_val
    df["FinalUpper"] = final_upper
    df["FinalLower"] = final_lower

    return df


# ===================================================
# 6. BOLLINGER BANDS
# ===================================================
def get_bollinger_last(df, period=200, mult=2):
    df["MA20"] = df["close"].rolling(period).mean()
    df["STD"] = df["close"].rolling(period).std()
    df["UpperBB"] = df["MA20"] + mult * df["STD"]
    df["LowerBB"] = df["MA20"] - mult * df["STD"]

    last = df.iloc[-1]
    return float(last["close"]), float(last["UpperBB"]), float(last["LowerBB"])


# ===================================================
# 7. SAVE WATCHLISTS
# ===================================================
def save_watchlist(buy, sell):
    with open(BUY_FILE, "w") as f:
        json.dump(buy, f, indent=2)
    with open(SELL_FILE, "w") as f:
        json.dump(sell, f, indent=2)


# ===================================================
# 8. MAIN LOGIC
# ===================================================
def main():
    pairs = get_active_usdt_coins()

    # Load existing lists
    try: buy_watch = json.load(open(BUY_FILE))
    except: buy_watch = []

    try: sell_watch = json.load(open(SELL_FILE))
    except: sell_watch = []

    # ------- Top gainers/losers (1D%) ----------
    def get_top(pairs):
        changes = []
        with ThreadPoolExecutor(MAX_WORKERS) as ex:
            for fut in as_completed([ex.submit(fetch_pair_stats, p) for p in pairs]):
                if fut.result():
                    changes.append(fut.result())

        changes.sort(key=lambda x: x["change"], reverse=True)
        return [x["pair"] for x in changes[:10]], [x["pair"] for x in changes[-10:]]

    top_gainers, top_losers = get_top(pairs)

    sell_watch = list(dict.fromkeys(sell_watch + top_gainers))
    buy_watch = list(dict.fromkeys(buy_watch + top_losers))

    # ------- Supertrend Scan (HA) ----------
    buy_signals, sell_signals = [], []

    def check_buy(pair):
        df = fetch_last_n_candles(pair)
        if df is None: return None

        ha = convert_to_heikin_ashi(df)
        st = supertrend_tv_ha(ha)

        prev, last = st.iloc[-2]["ST_Trend"], st.iloc[-1]["ST_Trend"]
        if prev is False and last is True:
            return f"{pair} | HA Close = {round(st.iloc[-1]['HA_Close'], 6)}"

    def check_sell(pair):
        df = fetch_last_n_candles(pair)
        if df is None: return None

        ha = convert_to_heikin_ashi(df)
        st = supertrend_tv_ha(ha)

        prev, last = st.iloc[-2]["ST_Trend"], st.iloc[-1]["ST_Trend"]
        if prev is True and last is False:
            return f"{pair} | HA Close = {round(st.iloc[-1]['HA_Close'], 6)}"

    if ENABLE_BUY:
        with ThreadPoolExecutor(MAX_WORKERS) as ex:
            for res in as_completed([ex.submit(check_buy, p) for p in buy_watch]):
                if res.result(): buy_signals.append(res.result())

    if ENABLE_SELL:
        with ThreadPoolExecutor(MAX_WORKERS) as ex:
            for res in as_completed([ex.submit(check_sell, p) for p in sell_watch]):
                if res.result(): sell_signals.append(res.result())

    # ------- Send Alerts -------
    if buy_signals:
        send_telegram_message("🟢 BUY (HA Supertrend Flip)\n" + "\n".join(buy_signals))

    if sell_signals:
        send_telegram_message("🔴 SELL (HA Supertrend Flip)\n" + "\n".join(sell_signals))

    # ------- Bollinger Remove -------
    sell_remove, buy_remove = [], []

    for coin in sell_watch:
        df = fetch_last_n_candles(coin)
        if df is None: continue
        close, upper, lower = get_bollinger_last(df)
        if close <= lower:
            sell_remove.append(coin)

    for coin in buy_watch:
        df = fetch_last_n_candles(coin)
        if df is None: continue
        close, upper, lower = get_bollinger_last(df)
        if close >= upper:
            buy_remove.append(coin)

    sell_watch = [c for c in sell_watch if c not in sell_remove]
    buy_watch = [c for c in buy_watch if c not in buy_remove]

    save_watchlist(buy_watch, sell_watch)


# ===================================================
if __name__ == "__main__":
    main()
