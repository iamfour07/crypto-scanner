"""
===========================================================
📊 COINDCX HEIKIN-ASHI REVERSAL SCANNER (1-Hour Timeframe)
===========================================================

Final Script:
✓ HA candles
✓ EMA on HA_Close
✓ Reversal based on current -2 (SL) & -1 (Entry)
✓ Bullish: -2 red → -1 green (Entry = -1 high, SL = -2 low)
✓ Bearish: -2 green → -1 red (Entry = -1 low, SL = -2 high)
✓ RR: 1:1, 1:2, 1:3, 1:4
✓ Position sizing: Max margin ₹500, Leverage 5x, Max loss ₹100
✓ Telegram alerts with candle colors and all levels
===========================================================
"""

import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from Telegram_Alert import send_telegram_message

# =====================
# CONFIG
# =====================
MAX_WORKERS = 15
resolution = "60"  
limit_hours = 1000

EMA_20 = 20
EMA_50 = 50

# Position sizing config
MAX_RISK = 100
MARGIN_PER_TRADE = 500
LEVERAGE = 5
POSITION_SIZE = MARGIN_PER_TRADE * LEVERAGE  

# IST timezone
IST_OFFSET = timedelta(hours=5, minutes=30)


# ---------------------
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


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
    except:
        return None


# ---------------------
def fetch_last_n_candles(pair, n=1000):
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        from_time = now - limit_hours * 3600

        url = "https://public.coindcx.com/market_data/candlesticks"
        params = {
            "pair": pair,
            "from": from_time,
            "to": now,
            "resolution": resolution,
            "pcode": "f"
        }

        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("data", [])

        if not data or len(data) < 60:
            return None

        df = pd.DataFrame(data)

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.tail(n).copy()

        # ----- HA CALC -----
        ha = pd.DataFrame(index=df.index)
        ha["HA_Close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
        ha["HA_Open"] = 0.0
        ha.iloc[0, ha.columns.get_loc("HA_Open")] = (df.iloc[0]["open"] + df.iloc[0]["close"]) / 2

        for i in range(1, len(df)):
            ha.iloc[i, ha.columns.get_loc("HA_Open")] = (
                ha.iloc[i - 1]["HA_Open"] + ha.iloc[i - 1]["HA_Close"]
            ) / 2

        ha["HA_High"] = df[["high", "open", "close"]].max(axis=1)
        ha["HA_Low"] = df[["low", "open", "close"]].min(axis=1)

        df = pd.concat([df.reset_index(drop=True), ha.reset_index(drop=True)], axis=1)

        # ----- EMA on HA_Close -----
        df[f"EMA_{EMA_20}"] = df["HA_Close"].ewm(span=EMA_20, adjust=False).mean()
        df[f"EMA_{EMA_50}"] = df["HA_Close"].ewm(span=EMA_50, adjust=False).mean()

        df = df.dropna().reset_index(drop=True)
        return df

    except:
        return None


# ---------------------
def main():
    print("Fetching active USDT pairs...")
    pairs = get_active_usdt_coins()

    # Fetch 1D changes
    changes = []
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_pair_stats, p) for p in pairs]
        for fut in as_completed(futures):
            if fut.result():
                changes.append(fut.result())

    df = pd.DataFrame(changes).dropna()
    if df.empty:
        return

    top_gainers = df.sort_values("change", ascending=False).head(10)["pair"].tolist()
    top_losers = df.sort_values("change", ascending=True).head(10)["pair"].tolist()

    # ===================================================
    # ALERT BUILDER
    # ===================================================
    def build_alert(pairs, bullish=True):
        messages = []
        for pair in pairs:
            df_c = fetch_last_n_candles(pair)
            if df_c is None or len(df_c) < 3:
                continue

            prev2 = df_c.iloc[-3]   # SL candle
            prev1 = df_c.iloc[-2]   # Entry candle

            # Candle colors
            c2 = "GREEN" if prev2["HA_Close"] > prev2["HA_Open"] else "RED"
            c1 = "GREEN" if prev1["HA_Close"] > prev1["HA_Open"] else "RED"

            # ----- Setup Logic -----
            bullish_signal = (c2 == "RED" and c1 == "GREEN")
            bearish_signal = (c2 == "GREEN" and c1 == "RED")

            if bullish and not bullish_signal:
                continue
            if not bullish and not bearish_signal:
                continue

            # ----- Entry & SL -----
            if bullish_signal:
                entry = prev1["HA_High"]
                sl = prev2["HA_Low"]
            else:
                entry = prev1["HA_Low"]
                sl = prev2["HA_High"]

            risk = abs(entry - sl)
            if risk <= 0:
                continue

            # ----- Quantity -----
            qty_risk = MAX_RISK / risk
            qty_margin = POSITION_SIZE / entry
            qty = int(min(qty_risk, qty_margin))
            if qty <= 0:
                continue

            margin_used = (entry * qty) / LEVERAGE

            # Targets
            if bullish_signal:
                t1 = entry + risk * 1
                t2 = entry + risk * 2
                t3 = entry + risk * 3
                t4 = entry + risk * 4
            else:
                t1 = entry - risk * 1
                t2 = entry - risk * 2
                t3 = entry - risk * 3
                t4 = entry - risk * 4

            # Timestamp
            ts = float(prev1["time"])
            if ts > 1e12:
                ts /= 1000

            time_str = (datetime.utcfromtimestamp(ts) + IST_OFFSET).strftime("%Y-%m-%d %I:%M %p IST")

            # ----- Build message -----
            msg = (
                f"Name: {pair}\n"
                f"HA(-2): {round(prev2['HA_Close'],4)} ({c2})\n"
                f"HA(-1): {round(prev1['HA_Close'],4)} ({c1})\n\n"
                f"Entry: {round(entry,4)}\n"
                f"Stop Loss: {round(sl,4)}\n"
                f"Margin Used: ₹{round(margin_used,2)}\n\n"
                f"🎯 Targets:\n"
                f"• 1:2 → {round(t2,4)}\n"
                f"• 1:3 → {round(t3,4)}\n"
                f"• 1:4 → {round(t4,4)}\n"
                f"-----------------------\n"
            )

            messages.append(msg)

        return messages

    # RUN
    bullish_msgs = build_alert(top_gainers, bullish=True)
    bearish_msgs = build_alert(top_losers, bullish=False)

    if bullish_msgs:
        # print("🟢 *Bullish HA Reversals*\n\n" + "\n".join(bullish_msgs))
        send_telegram_message("🟢 *Bullish HA Reversals*\n\n" + "\n\n".join(bullish_msgs))

    if bearish_msgs:
        # print("🔴 *Bearish HA Reversals*\n\n" + "\n\n".join(bearish_msgs))
        send_telegram_message("🔴 *Bearish HA Reversals*\n\n" + "\n\n".join(bearish_msgs))


if __name__ == "__main__":
    main()
