"""
===========================================================
📊 COINDCX SUPERTREND SCANNER (1-Hour Timeframe)
===========================================================

STRICT RULES:
✔ Heikin Ashi candles only
✔ Supertrend on HA candles
✔ Latest CLOSED candle only (current - 1)
✔ Green HA only with Bullish Supertrend
✔ Red HA only with Bearish Supertrend
✔ Entry & SL from SAME HA candle
✔ Capital fixed at ₹600
✔ Max loss ₹200
✔ Leverage auto-selected (safe & capped)
===========================================================
"""

import requests
import pandas as pd
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from Telegram_Alert import send_telegram_message

# =====================
# CONFIG
# =====================
MAX_WORKERS = 15
RESOLUTION = "60"
LIMIT_HOURS = 1000

# Supertrend
ST_LENGTH = 10
ST_FACTOR = 2.5

# Risk & Capital
CAPITAL_RS = 600
MAX_LOSS_RS = 50
MAX_ALLOWED_LEVERAGE = 10
RR_LEVELS = [2, 3, 4]

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
# HEIKIN ASHI
# =========================================================
def to_heikin_ashi(df):
    ha = df.copy()

    ha["HA_Close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4

    ha_open = [(df["open"].iloc[0] + df["close"].iloc[0]) / 2]
    for i in range(1, len(df)):
        ha_open.append((ha_open[i-1] + ha["HA_Close"].iloc[i-1]) / 2)

    ha["HA_Open"] = ha_open
    ha["HA_High"] = ha[["high", "HA_Open", "HA_Close"]].max(axis=1)
    ha["HA_Low"]  = ha[["low", "HA_Open", "HA_Close"]].min(axis=1)

    ha["open"]  = ha["HA_Open"]
    ha["high"]  = ha["HA_High"]
    ha["low"]   = ha["HA_Low"]
    ha["close"] = ha["HA_Close"]

    return ha

# =========================================================
# SUPERTREND
# =========================================================
def calculate_supertrend(df):
    if len(df) <= ST_LENGTH:
        return df

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

    st.iloc[ST_LENGTH] = fl.iloc[ST_LENGTH]
    trend.iloc[ST_LENGTH] = True

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
# FETCH CANDLES
# =========================================================
def fetch_candles(pair, n=300):
    now = int(datetime.now(timezone.utc).timestamp())

    data = requests.get(
        "https://public.coindcx.com/market_data/candlesticks",
        params={
            "pair": pair,
            "from": now - LIMIT_HOURS * 3600,
            "to": now,
            "resolution": RESOLUTION,
            "pcode": "f",
        },
        timeout=10
    ).json().get("data", [])

    if not data:
        return None

    df = pd.DataFrame(data).sort_values("time").reset_index(drop=True)
    df = to_heikin_ashi(df)
    df = calculate_supertrend(df.iloc[-n:].copy())

    return df.dropna().reset_index(drop=True)

# =========================================================
# POSITION SIZING + LEVERAGE
# =========================================================
def calculate_trade_levels(side, c):
    entry, sl = (c["high"], c["low"]) if side == "bullish" else (c["low"], c["high"])
    risk = abs(entry - sl)

    if risk <= 0:
        return None

    # Qty by risk
    qty_risk = int(MAX_LOSS_RS // risk)
    if qty_risk <= 0:
        return None

    # Qty by max leverage
    max_position_value = CAPITAL_RS * MAX_ALLOWED_LEVERAGE
    qty_capital = int(max_position_value // entry)

    qty = min(qty_risk, qty_capital)
    if qty <= 0:
        return None

    position_value = entry * qty
    leverage = math.ceil(position_value / CAPITAL_RS)

    if leverage < 1 or leverage > MAX_ALLOWED_LEVERAGE:
        return None

    used_capital = round(position_value / leverage, 2)

    return {
        "entry": round(entry, 4),
        "sl": round(sl, 4),
        "qty": qty,
        "leverage": leverage,
        "capital": used_capital,
        "targets": {
            f"{r}R": round(
                entry + r * risk if side == "bullish"
                else entry - r * risk,
                4
            )
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

    top_gainers = df.sort_values("change", ascending=False).iloc[5:20]["pair"]
    top_losers  = df.sort_values("change").iloc[5:20]["pair"]

    def check_signal(pair, side):
        df_c = fetch_candles(pair)
        if df_c is None or len(df_c) < ST_LENGTH + 3:
            return None

        c = df_c.iloc[-2]  # LAST CLOSED candle

        # STRICT COLOR BINDING
        if side == "bullish":
            if c["close"] <= c["open"]:
                return None
            if c["ST_Trend"] and c["low"] <= c["Supertrend"] and c["close"] > c["Supertrend"]:
                trade = calculate_trade_levels("bullish", c)
                if trade:
                    return ("BUY", pair, trade)

        if side == "bearish":
            if c["close"] >= c["open"]:
                return None
            if not c["ST_Trend"] and c["high"] >= c["Supertrend"] and c["close"] < c["Supertrend"]:
                trade = calculate_trade_levels("bearish", c)
                if trade:
                    return ("SELL", pair, trade)

        return None

    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        tasks = [ex.submit(check_signal, p, "bullish") for p in top_gainers]
        tasks += [ex.submit(check_signal, p, "bearish") for p in top_losers]

        for f in as_completed(tasks):
            if f.result():
                side, pair, t = f.result()
                emoji = "🟢" if side == "BUY" else "🔴"

                msg = (
                    f"{emoji} {side} – HA Supertrend Pullback (1H)\n\n"
                    f"Symbol   : {pair}\n"
                    f"Entry    : {t['entry']}\n"
                    f"SL       : {t['sl']}\n"
                    f"Qty      : {t['qty']}\n"
                    f"Capital  : ₹{t['capital']} ({t['leverage']}×)\n\n"
                    "Targets:\n" +
                    "\n".join([f"{k} → {v}" for k, v in t["targets"].items()])
                )

                send_telegram_message(msg)

if __name__ == "__main__":
    main()
