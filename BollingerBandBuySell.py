"""
===========================================================
📊 COINDCX BREAKOUT CONTINUATION SCANNER (1-Hour)
===========================================================
"""

import requests
import pandas as pd
from datetime import datetime, timezone
from Telegram_Alert import send_telegram_message

# =====================
# CONFIG
# =====================
RESOLUTION = "60"          # 1-HOUR
LIMIT_HOURS = 2000

GAINER_THRESHOLD = 5.0
LOSER_THRESHOLD = -5.0

CAPITAL_RS = 500
MAX_LOSS_RS = 50
MAX_ALLOWED_LEVERAGE = 30
MIN_LEVERAGE = 5

# ===================================================
# 1. ACTIVE USDT FUTURES
# ===================================================
def get_active_usdt_coins():
    url = (
        "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
        "active_instruments?margin_currency_short_name[]=USDT"
    )
    data = requests.get(url, timeout=30).json()
    return [x["pair"] if isinstance(x, dict) else x for x in data]

# ===================================================
# 2. 1D % CHANGE
# ===================================================
def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        r = requests.get(url, timeout=10).json()
        pc = r.get("price_change_percent", {}).get("1D")
        return float(pc) if pc is not None else None
    except:
        return None

# ===================================================
# 3. CANDLES
# ===================================================
def fetch_candles(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    frm = now - LIMIT_HOURS * 3600

    url = "https://public.coindcx.com/market_data/candlesticks"
    params = {
        "pair": pair,
        "from": frm,
        "to": now,
        "resolution": RESOLUTION,
        "pcode": "f",
    }

    data = requests.get(url, params=params, timeout=15).json().get("data", [])
    if len(data) < 50:
        return None

    df = pd.DataFrame(data)
    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df.dropna().reset_index(drop=True)

# ===================================================
# 4. HEIKIN ASHI
# ===================================================
def heikin_ashi(df):
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
# 5. BOLLINGER (20, 2.0)
# ===================================================
def bollinger(df, period=20, mult=2.0):
    df = df.copy()
    df["MA"] = df["close"].rolling(period).mean()
    df["STD"] = df["close"].rolling(period).std(ddof=0)
    df["UpperBB"] = df["MA"] + mult * df["STD"]
    df["LowerBB"] = df["MA"] - mult * df["STD"]
    return df

# ===================================================
# 6. CAPITAL + AUTO LEVERAGE
# ===================================================
def calculate_trade_levels(entry, sl_hint, side):

    for leverage in range(MAX_ALLOWED_LEVERAGE, MIN_LEVERAGE - 1, -1):
        position_value = CAPITAL_RS * leverage

        if side == "BUY":
            risk = entry - sl_hint
        else:
            risk = sl_hint - entry

        actual_loss = (risk / entry) * position_value
        if actual_loss <= MAX_LOSS_RS:
            break
    else:
        leverage = MIN_LEVERAGE

    if side == "BUY":
        t2 = entry + risk * 2
        t3 = entry + risk * 3
        t4 = entry + risk * 4
    else:
        t2 = entry - risk * 2
        t3 = entry - risk * 3
        t4 = entry - risk * 4

    return {
        "entry": round(entry, 4),
        "sl": round(sl_hint, 4),
        "capital": CAPITAL_RS,
        "leverage": leverage,
        "targets": {
            "1:2": round(t2, 4),
            "1:3": round(t3, 4),
            "1:4": round(t4, 4),
        }
    }

# ===================================================
# 7. MAIN
# ===================================================
def main():

    active_pairs = get_active_usdt_coins()

    stats = []
    for pair in active_pairs:
        chg = fetch_pair_stats(pair)
        if chg is not None:
            stats.append({"pair": pair, "change": chg})

    gainers = [x for x in stats if x["change"] >= GAINER_THRESHOLD]
    losers  = [x for x in stats if x["change"] <= LOSER_THRESHOLD]

    alerts = []

    for item in gainers + losers:
        pair = item["pair"]

        df = fetch_candles(pair)
        if df is None:
            continue

        ha = heikin_ashi(df)

        bb_src = ha[["HA_Close"]].rename(columns={"HA_Close": "close"})
        bb = bollinger(bb_src)

        if bb["UpperBB"].isna().iloc[-3]:
            continue

        c2 = ha.iloc[-3]   # pullback candle
        c1 = ha.iloc[-2]   # entry candle
        bb2 = bb.iloc[-3]
        # ================= DEBUG =================
        print("\n==============================")
        print(f"PAIR        : {pair}")
        print(f"-2 HA Close : {c2['HA_Close']:.5f}")
        print(f"-1 HA Close : {c1['HA_Close']:.5f}")
        print(f"BB Upper    : {bb2['UpperBB']:.5f}")
        print(f"BB Lower    : {bb2['LowerBB']:.5f}")
        print("==============================")

        # ================= BUY =================
        if item["change"] >= GAINER_THRESHOLD:
            if (
                c2["HA_Close"] < c2["HA_Open"] and
                c2["HA_Low"] <= bb2["LowerBB"] and
                c1["HA_Close"] > c1["HA_Open"]
            ):
                trade = calculate_trade_levels(
                    entry=c1["HA_High"],
                    sl_hint=c1["HA_Low"],
                    side="BUY"
                )
                alerts.append(
                    f"🟢 BUY {pair}\n"
                    f"Entry   : {trade['entry']}\n"
                    f"SL      : {trade['sl']}\n"
                    f"Capital : ₹{trade['capital']} ({trade['leverage']}×)\n\n"
                    "Targets:\n" +
                    "\n".join(f"{k} → {v}" for k, v in trade["targets"].items())
                )

        # ================= SELL =================
        if item["change"] <= LOSER_THRESHOLD:
            if (
                c2["HA_Close"] > c2["HA_Open"] and
                c2["HA_High"] >= bb2["UpperBB"] and
                c1["HA_Close"] < c1["HA_Open"]
            ):
                trade = calculate_trade_levels(
                    entry=c1["HA_Low"],
                    sl_hint=c1["HA_High"],
                    side="SELL"
                )
                alerts.append(
                    f"🔴 SELL {pair}\n"
                    f"Entry   : {trade['entry']}\n"
                    f"SL      : {trade['sl']}\n"
                    f"Capital : ₹{trade['capital']} ({trade['leverage']}×)\n\n"
                    "Targets:\n" +
                    "\n".join(f"{k} → {v}" for k, v in trade["targets"].items())
                )

    if alerts:
        send_telegram_message("\n\n".join(alerts))

# ===================================================
if __name__ == "__main__":
    main()
