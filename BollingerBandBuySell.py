"""
===========================================================
📊 COINDCX PULLBACK CONTINUATION SCANNER (1-Hour)
===========================================================

LOGIC SUMMARY:
- Top 30 Gainers (>= +2%):
    RED Heikin-Ashi touches Lower BB → BUY Watchlist
- Top 30 Losers (<= -2%):
    GREEN Heikin-Ashi touches Upper BB → SELL Watchlist

ENTRY:
- BUY  → First GREEN HA after BB-touch (Entry = High)
- SELL → First RED HA after BB-touch (Entry = Low)

STOP LOSS:
- BUY  → Low of latest RED BB-touch candle
- SELL → High of latest GREEN BB-touch candle

REMOVE RULES (ONLY):
1️⃣ Entry hit → remove
2️⃣ Opposite BB touched before entry → remove

RISK MANAGEMENT:
✔ Capital fixed
✔ Max loss per trade
✔ Auto leverage (capped)
✔ RR targets
===========================================================
"""

import json
import math
import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Alert_Swing import send_telegram_message

# =====================
# CONFIG
# =====================
RESOLUTION = "60"
LIMIT_HOURS = 1000
MAX_WORKERS = 15

BUY_FILE = "BuyWatchlist.json"
SELL_FILE = "SellWatchlist.json"

# ---- Risk & Capital ----
CAPITAL_RS = 600
MAX_LOSS_RS = 50
MAX_ALLOWED_LEVERAGE = 10
RR_LEVELS = [2, 3, 4]

# ===================================================
# 1. ACTIVE FUTURES
# ===================================================
def get_active_usdt_coins():
    url = (
        "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
        "active_instruments?margin_currency_short_name[]=USDT"
    )
    return requests.get(url, timeout=30).json()

# ===================================================
# 2. 1D % CHANGE
# ===================================================
def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        r = requests.get(url, timeout=10).json()
        pc = r.get("price_change_percent", {}).get("1D")
        return {"pair": pair, "change": float(pc)} if pc is not None else None
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
        ha_open.append((ha_open[i-1] + ha["HA_Close"].iloc[i-1]) / 2)

    ha["HA_Open"] = ha_open
    ha["HA_High"] = ha[["HA_Open", "HA_Close", "high"]].max(axis=1)
    ha["HA_Low"]  = ha[["HA_Open", "HA_Close", "low"]].min(axis=1)

    ha["HA_Color"] = ha.apply(
        lambda x: "GREEN" if x["HA_Close"] >= x["HA_Open"] else "RED",
        axis=1
    )

    return ha

# ===================================================
# 5. BOLLINGER BANDS
# ===================================================
def bollinger(df, period=20, mult=2):
    df["MA"] = df["close"].rolling(period).mean()
    df["STD"] = df["close"].rolling(period).std()
    df["UpperBB"] = df["MA"] + mult * df["STD"]
    df["LowerBB"] = df["MA"] - mult * df["STD"]
    return df

# ===================================================
# 6. POSITION SIZING + RR
# ===================================================
def calculate_trade_levels(entry, sl, side):
    risk = abs(entry - sl)
    if risk <= 0:
        return None

    qty_risk = int(MAX_LOSS_RS // risk)
    if qty_risk <= 0:
        return None

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

    targets = {
        f"{r}R": round(
            entry + r * risk if side == "BUY"
            else entry - r * risk,
            4
        )
        for r in RR_LEVELS
    }

    return {
        "entry": round(entry, 4),
        "sl": round(sl, 4),
        "qty": qty,
        "leverage": leverage,
        "capital": used_capital,
        "targets": targets
    }

# ===================================================
# 7. MAIN
# ===================================================
def main():
    pairs = get_active_usdt_coins()

    try:
        buy_watch = json.load(open(BUY_FILE))
    except FileNotFoundError:
        buy_watch = {}

    try:
        sell_watch = json.load(open(SELL_FILE))
    except FileNotFoundError:
        sell_watch = {}

    # ---------- TOP 30 WITH % FILTER ----------
    stats = []
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        for r in as_completed([ex.submit(fetch_pair_stats, p) for p in pairs]):
            if r.result():
                stats.append(r.result())

    stats.sort(key=lambda x: x["change"], reverse=True)

    top_gainers = [x["pair"] for x in stats if x["change"] >= 2.0][:30]
    top_losers  = [x["pair"] for x in reversed(stats) if x["change"] <= -2.0][:30]

    alerts = []

    def process(pair, side):
        df = fetch_candles(pair)
        if df is None:
            return

        ha = heikin_ashi(df)
        df = bollinger(df)

        last = ha.iloc[-2]      # last CLOSED candle
        last_bb = df.iloc[-2]

        # ================= BUY =================
        if side == "BUY":
            if last["HA_Color"] == "RED" and last_bb["low"] <= last_bb["LowerBB"]:
                buy_watch[pair] = {"bb_low": last["HA_Low"]}
                return

            if pair in buy_watch:
                if last["HA_Color"] == "GREEN":
                    entry = last["HA_High"]
                    sl = buy_watch[pair]["bb_low"]
                    trade = calculate_trade_levels(entry, sl, "BUY")
                    if trade:
                        alerts.append(
                            f"🟢 BUY {pair}\n"
                            f"Entry   : {trade['entry']}\n"
                            f"SL      : {trade['sl']}\n"
                            f"Qty     : {trade['qty']}\n"
                            f"Capital : ₹{trade['capital']} ({trade['leverage']}×)\n\n"
                            "Targets:\n" +
                            "\n".join([f"{k} → {v}" for k, v in trade["targets"].items()])
                        )
                    buy_watch.pop(pair)
                    return

        # ================= SELL =================
        if side == "SELL":
            if last["HA_Color"] == "GREEN" and last_bb["high"] >= last_bb["UpperBB"]:
                sell_watch[pair] = {"bb_high": last["HA_High"]}
                return

            if pair in sell_watch:
                if last["HA_Color"] == "RED":
                    entry = last["HA_Low"]
                    sl = sell_watch[pair]["bb_high"]
                    trade = calculate_trade_levels(entry, sl, "SELL")
                    if trade:
                        alerts.append(
                            f"🔴 SELL {pair}\n"
                            f"Entry   : {trade['entry']}\n"
                            f"SL      : {trade['sl']}\n"
                            f"Qty     : {trade['qty']}\n"
                            f"Capital : ₹{trade['capital']} ({trade['leverage']}×)\n\n"
                            "Targets:\n" +
                            "\n".join([f"{k} → {v}" for k, v in trade["targets"].items()])
                        )
                    sell_watch.pop(pair)
                    return

    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        for p in top_gainers:
            ex.submit(process, p, "BUY")
        for p in top_losers:
            ex.submit(process, p, "SELL")

    if alerts:
        send_telegram_message("\n\n".join(alerts))

    json.dump(buy_watch, open(BUY_FILE, "w"), indent=2)
    json.dump(sell_watch, open(SELL_FILE, "w"), indent=2)

# ===================================================
if __name__ == "__main__":
    main()
