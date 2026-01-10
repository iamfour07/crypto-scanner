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
PAIRS = [
    "B-BTC_USDT",
    "B-ETH_USDT",
    "B-SOL_USDT",
    "B-XRP_USDT",
    "B-ZEC_USDT",
]

RESOLUTION = "60"   # 1-HOUR candles
LIMIT_HOURS = 2000

CAPITAL_RS = 500
MAX_LOSS_RS = 50
MAX_ALLOWED_LEVERAGE = 30
MIN_LEVERAGE = 5

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

def bollinger(df, period=20, mult=1.3):
    df = df.copy()
    df["MA"] = df["close"].rolling(period).mean()
    df["STD"] = df["close"].rolling(period).std(ddof=0)
    df["UpperBB"] = df["MA"] + mult * df["STD"]
    df["LowerBB"] = df["MA"] - mult * df["STD"]
    return df

# ===================================================
# CAPITAL + AUTO-LEVERAGE LOGIC
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
def main():
    alerts = []

    for pair in PAIRS:
        df = fetch_candles(pair)
        if df is None:
            continue

        ha = heikin_ashi(df)

        # Bollinger on Heikin-Ashi CLOSE
        bb_source = ha[["HA_Close"]].copy()
        bb_source.rename(columns={"HA_Close": "close"}, inplace=True)
        bb = bollinger(bb_source)

        # ensure BB is ready
        if bb["UpperBB"].isna().iloc[-1]:
            continue

        # last CLOSED candle
        ha_run = ha.iloc[-2]
        bb_run = bb.iloc[-2]
        # print("\n==============================")
        # print(f"PAIR : {pair}")
        # print(f"HA High  : {ha_run['HA_High']}")
        # print(f"HA Low   : {ha_run['HA_Low']}")
        # print(f"HA Close : {ha_run['HA_Close']}")
        # print(f"BB Upper : {bb_run['UpperBB']}")
        # print(f"BB Lower : {bb_run['LowerBB']}")

        # print("BUY COND:",
        #     ha_run["HA_High"] > bb_run["UpperBB"],
        #     ha_run["HA_Low"] > bb_run["UpperBB"],
        #     ha_run["HA_Close"] > bb_run["UpperBB"])

        # print("SELL COND:",
        #     ha_run["HA_Low"] < bb_run["LowerBB"],
        #     ha_run["HA_High"] < bb_run["LowerBB"],
        #     ha_run["HA_Close"] < bb_run["LowerBB"])
        # print("==============================")

        # ================= BUY =================
        if (
            ha_run["HA_High"] > bb_run["UpperBB"]
            and ha_run["HA_Low"] > bb_run["UpperBB"]
            and ha_run["HA_Close"] > bb_run["UpperBB"]
        ):
            trade = calculate_trade_levels(
                entry=ha_run["HA_Low"],
                sl_hint=ha_run["HA_High"],
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
        if (
            ha_run["HA_Low"] < bb_run["LowerBB"]
            and ha_run["HA_High"] < bb_run["LowerBB"]
            and ha_run["HA_Close"] < bb_run["LowerBB"]
        ):
            trade = calculate_trade_levels(
                entry=ha_run["HA_High"],
                sl_hint=ha_run["HA_Low"],
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
        print("\n\n".join(alerts))
        send_telegram_message("\n\n".join(alerts))

# ===================================================
if __name__ == "__main__":
    main()
