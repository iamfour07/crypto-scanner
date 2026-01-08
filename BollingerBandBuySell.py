"""
===========================================================
📊 COINDCX PULLBACK CONTINUATION SCANNER (1-Hour)
===========================================================
"""

import json
import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Alert import send_telegram_message

# =====================
# CONFIG
# =====================
RESOLUTION = "60"
LIMIT_HOURS = 1000
MAX_WORKERS = 15

CAPITAL_RS = 500
MAX_LOSS_RS = 50
MAX_ALLOWED_LEVERAGE = 30
MIN_LEVERAGE = 1

# ===================================================
def get_active_usdt_coins():
    url = (
        "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
        "active_instruments?margin_currency_short_name[]=USDT"
    )
    return requests.get(url, timeout=30).json()

def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        r = requests.get(url, timeout=10).json()
        pc = r.get("price_change_percent", {}).get("1D")
        return {"pair": pair, "change": float(pc)} if pc is not None else None
    except:
        return None

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
def heikin_ashi(df):
    ha = df.copy()
    ha["HA_Close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4

    ha_open = [(df["open"].iloc[0] + df["close"].iloc[0]) / 2]
    for i in range(1, len(df)):
        ha_open.append((ha_open[i - 1] + ha["HA_Close"].iloc[i - 1]) / 2)

    ha["HA_Open"] = ha_open
    ha["HA_High"] = ha[["HA_Open", "HA_Close", "high"]].max(axis=1)
    ha["HA_Low"] = ha[["HA_Open", "HA_Close", "low"]].min(axis=1)
    ha["HA_Color"] = ha.apply(
        lambda x: "GREEN" if x["HA_Close"] >= x["HA_Open"] else "RED",
        axis=1
    )
    return ha

def bollinger(df, period=20, mult=2):
    df["MA"] = df["close"].rolling(period).mean()
    df["STD"] = df["close"].rolling(period).std()
    df["UpperBB"] = df["MA"] + mult * df["STD"]
    df["LowerBB"] = df["MA"] - mult * df["STD"]
    return df

# ===================================================
# CAPITAL + AUTO-LEVERAGE LOGIC (UNCHANGED)
# ===================================================
def calculate_trade_levels(entry, sl_hint, side):

    for leverage in range(MAX_ALLOWED_LEVERAGE, MIN_LEVERAGE - 1, -1):
        position_value = CAPITAL_RS * leverage
        loss_pct = MAX_LOSS_RS / position_value

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
def main():
    pairs = get_active_usdt_coins()

    stats = []
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        for r in as_completed([ex.submit(fetch_pair_stats, p) for p in pairs]):
            if r.result():
                stats.append(r.result())

    stats.sort(key=lambda x: x["change"], reverse=True)

    top_gainers = [x["pair"] for x in stats if x["change"] >= 0.8][:30]
    top_losers  = [x["pair"] for x in reversed(stats) if x["change"] <= -0.8][:30]

    alerts = []

    def process(pair, side):
        try:
            df = fetch_candles(pair)
            if df is None:
                return

            ha = heikin_ashi(df)
            bb = bollinger(df)

            c3 = ha.iloc[-3]
            c2 = ha.iloc[-2]
            bb3 = bb.iloc[-3]

            # ================= BUY =================
            if side == "BUY":
                if (
                    c3["HA_Color"] == "RED"
                    and c2["HA_Color"] == "GREEN"
                    and bb3["low"] <= bb3["LowerBB"]
                ):
                    trade = calculate_trade_levels(
                        entry=c2["HA_High"],
                        sl_hint=c3["HA_Low"],
                        side="BUY"
                    )
                    alerts.append(
                        f"🟢 BUY {pair}\n"
                        f"Entry   : {trade['entry']}\n"
                        f"SL      : {trade['sl']}\n"
                        f"Capital : ₹{trade['capital']} ({trade['leverage']}×)\n\n"
                        "Targets:\n" +
                        "\n".join([f"{k} → {v}" for k, v in trade["targets"].items()])
                    )

            # ================= SELL =================
            if side == "SELL":
                if (
                    c3["HA_Color"] == "GREEN"
                    and c2["HA_Color"] == "RED"
                    and bb3["high"] >= bb3["UpperBB"]
                ):
                    trade = calculate_trade_levels(
                        entry=c2["HA_Low"],
                        sl_hint=c3["HA_High"],
                        side="SELL"
                    )
                    alerts.append(
                        f"🔴 SELL {pair}\n"
                        f"Entry   : {trade['entry']}\n"
                        f"SL      : {trade['sl']}\n"
                        f"Capital : ₹{trade['capital']} ({trade['leverage']}×)\n\n"
                        "Targets:\n" +
                        "\n".join([f"{k} → {v}" for k, v in trade["targets"].items()])
                    )

        except Exception as e:
            print(f"[ERROR] {pair} → {e}")

    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        for p in top_gainers:
            ex.submit(process, p, "BUY")
        for p in top_losers:
            ex.submit(process, p, "SELL")

    if alerts:
        send_telegram_message("\n\n".join(alerts))

# ===================================================
if __name__ == "__main__":
    main()