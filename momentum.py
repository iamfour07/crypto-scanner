

"""
===========================================================
📊 COINDCX HEIKIN-ASHI REVERSAL SCANNER (1-Hour Timeframe)
===========================================================
"""

import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from Telegram_Alert import send_telegram_message

# ======================
# CONFIG
# ======================
MAX_WORKERS = 15
resolution = "60"
limit_hours = 1000

EMA_9 = 9
EMA_30 = 30
EMA_100 = 100

MAX_RISK = 100
MARGIN_PER_TRADE = 500
LEVERAGE = 5
POSITION_SIZE = MARGIN_PER_TRADE * LEVERAGE

IST_OFFSET = timedelta(hours=5, minutes=30)

# ======================
# API FUNCTIONS
# ======================

def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        pc = r.json().get("price_change_percent", {}).get("1D")
        return {"pair": pair, "change": float(pc)} if pc else None
    except:
        return None


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

        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json().get("data", [])
        if not data or len(data) < 60: 
            return None

        df = pd.DataFrame(data)
        df = df.astype({"open": float, "high": float, "low": float, "close": float})
        df = df.tail(n).copy()

        # HEIKIN ASHI CALCULATIONS
        ha = pd.DataFrame(index=df.index)
        ha["HA_Close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
        ha["HA_Open"] = (df["open"] + df["close"]) / 2
        for i in range(1, len(df)):
            ha.loc[i, "HA_Open"] = (ha.loc[i-1, "HA_Open"] + ha.loc[i-1, "HA_Close"]) / 2

        ha["HA_High"] = df[["high", "open", "close"]].max(axis=1)
        ha["HA_Low"]  = df[["low",  "open", "close"]].min(axis=1)

        df = pd.concat([df.reset_index(drop=True), ha.reset_index(drop=True)], axis=1)

        # EMAS
        df["EMA9"]   = df["HA_Close"].ewm(span=EMA_9,   adjust=False).mean()
        df["EMA30"]  = df["HA_Close"].ewm(span=EMA_30,  adjust=False).mean()
        df["EMA100"] = df["HA_Close"].ewm(span=EMA_100, adjust=False).mean()

        return df.dropna().reset_index(drop=True)

    except:
        return None


# ==========================================================
# MAIN
# ==========================================================

def main():
    print("Fetching active USDT pairs...")
    pairs = get_active_usdt_coins()

    # Fetch 1D change %
    changes = []
    with ThreadPoolExecutor(MAX_WORKERS) as exe:
        futures = [exe.submit(fetch_pair_stats, p) for p in pairs]
        for f in as_completed(futures):
            if f.result(): 
                changes.append(f.result())

    df = pd.DataFrame(changes).dropna()
    if df.empty: 
        return

    # top 40 for more signals
    # BULLISH RANGE:  +5% to +15%
    bullish_candidates = df[(df["change"] >= 8) & (df["change"] <= 15)]["pair"].tolist()

    # BEARISH RANGE: -5% to -15%
    bearish_candidates = df[(df["change"] <= -8) & (df["change"] >= -15)]["pair"].tolist()



    # ===================================================
    # ALERT BUILDER
    # ===================================================
    def build_alert(pairs, bullish=True):
        messages = []

        for pair in pairs:

            df_c = fetch_last_n_candles(pair)
            if df_c is None or len(df_c) < 3:
                continue

            prev2 = df_c.iloc[-3]   # -3 candle (SL)
            prev1 = df_c.iloc[-2]   # -2 candle (ENTRY)

            # HA COLORS
            c2 = "GREEN" if prev2["HA_Close"] > prev2["HA_Open"] else "RED"
            c1 = "GREEN" if prev1["HA_Close"] > prev1["HA_Open"] else "RED"

            bullish_signal = (c2 == "RED"   and c1 == "GREEN")
            bearish_signal = (c2 == "GREEN" and c1 == "RED")

            EMA9   = prev1["EMA9"]
            EMA30  = prev1["EMA30"]
            EMA100 = prev1["EMA100"]

            # -----------------------------------------
            # 🔥 FINAL BULLISH LOGIC
            # -----------------------------------------
            if bullish and bullish_signal:

                # 1. Trend — 9 > 30 > 100
                if not (EMA9 > EMA30 > EMA100):
                    continue

                # 2. HA(-3) and HA(-2) between EMA9 and EMA100
                if prev2["HA_Close"] <= EMA100:
                    continue
                if prev1["HA_Close"] <= EMA100:
                    continue

                entry = prev1["HA_High"]
                sl    = prev2["HA_Low"]


            # -----------------------------------------
            # 🔥 FINAL BEARISH LOGIC
            # -----------------------------------------
            elif not bullish and bearish_signal:

                # 1. Trend — 9 < 30 < 100
                if not (EMA9 < EMA30 < EMA100):
                    continue

                # 2. HA(-3) and HA(-2) between EMA9 and EMA100
                if prev2["HA_Close"] >= EMA100:
                    continue
                if prev1["HA_Close"] >= EMA100:
                    continue

                

                entry = prev1["HA_Low"]
                sl    = prev2["HA_High"]

            else:
                continue

            # -----------------------------------------
            # RISK + QUANTITY
            # -----------------------------------------
            risk = abs(entry - sl)
            if risk <= 0: 
                continue

            qty_risk   = MAX_RISK / risk
            qty_margin = POSITION_SIZE / entry
            qty = int(min(qty_risk, qty_margin))
            if qty <= 0: 
                continue

            margin_used = (entry * qty) / LEVERAGE

            # TARGETS
            if bullish:
                t2 = entry + 2*risk
                t3 = entry + 3*risk
                t4 = entry + 4*risk
            else:
                t2 = entry - 2*risk
                t3 = entry - 3*risk
                t4 = entry - 4*risk

            # TIME
            ts = float(prev1["time"])
            if ts > 1e12: ts /= 1000
            time_str = (datetime.utcfromtimestamp(ts) + IST_OFFSET).strftime("%Y-%m-%d %I:%M %p IST")

            # MESSAGE
            msg = (
                f"Name: {pair}\n"
                f"HA(-3): {round(prev2['HA_Close'],4)} ({c2})\n"
                f"HA(-2): {round(prev1['HA_Close'],4)} ({c1})\n\n"
                f"Entry: {entry:.4f}\n"
                f"Stop Loss: {sl:.4f}\n"
                f"Margin Used: ₹{margin_used:.2f}\n\n"
                f"🎯 Targets:\n"
                f"• 1:2 → {t2:.4f}\n"
                f"• 1:3 → {t3:.4f}\n"
                f"• 1:4 → {t4:.4f}\n"
                f"-----------------------\n"
            )

            messages.append(msg)

        return messages


    # ==========================================================
    # RUN SCANNER
    # ==========================================================

    bullish_msgs = build_alert(bullish_candidates, bullish=True)
    bearish_msgs = build_alert(bearish_candidates, bullish=False)


    if bullish_msgs:
        # print("🟢 *Bullish HA Reversals*\n\n" + "\n".join(bullish_msgs))
        send_telegram_message("🟢 *Bullish HA Reversals*\n\n" + "\n\n".join(bullish_msgs))

    if bearish_msgs:
        # print("🔴 *Bearish HA Reversals*\n\n" + "\n".join(bearish_msgs))
        send_telegram_message("🔴 *Bearish HA Reversals*\n\n" + "\n\n".join(bearish_msgs))


if __name__ == "__main__":
    main()
