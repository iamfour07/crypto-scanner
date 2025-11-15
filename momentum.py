"""
===========================================================
📊 COINDCX HEIKIN-ASHI REVERSAL SCANNER (1-Hour Timeframe)
===========================================================

🔹 PURPOSE:
    Detect bullish and bearish Heikin-Ashi reversal patterns
    among top USDT futures coins on CoinDCX and send Telegram alerts.

🔹 LOGIC:
    Bullish → (-3 red, -2 green) HA_Close above EMA20 & EMA50 (wicks allowed)
    Bearish → (-3 green, -2 red) HA_Close below EMA20 & EMA50 (wicks allowed)

🔹 EXTRA:
    Alert shows last two Heikin-Ashi Close prices with candle timestamp (IST)
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
resolution = "60"  # 1-hour candles
limit_hours = 1000

EMA_20 = 20
EMA_50 = 50
ema_periods = [EMA_20, EMA_50]

# IST timezone offset (+5:30)
IST_OFFSET = timedelta(hours=5, minutes=30)


# ---------------------
# Fetch active USDT futures pairs
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


# Fetch 1D % change
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


# ---------------------
# Fetch candle data and compute Heikin-Ashi + EMAs
def fetch_last_n_candles(pair, n=1000):
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        from_time = now - limit_hours * 3600
        url = "https://public.coindcx.com/market_data/candlesticks"
        params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data or len(data) < max(ema_periods) + 5:
            return None

        df = pd.DataFrame(data)
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.tail(n).copy()

        # ----------------------------
        # 1) CREATE HEIKIN-ASHI FIRST
        # ----------------------------
        ha_df = pd.DataFrame()
        ha_df["HA_Close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
        ha_df["HA_Open"] = 0.0
        ha_df.loc[0, "HA_Open"] = (df.loc[0, "open"] + df.loc[0, "close"]) / 2

        for i in range(1, len(df)):
            ha_df.loc[i, "HA_Open"] = (ha_df.loc[i - 1, "HA_Open"] + ha_df.loc[i - 1, "HA_Close"]) / 2

        ha_df["HA_High"] = df[["high", "open", "close"]].max(axis=1)
        ha_df["HA_Low"] = df[["low", "open", "close"]].min(axis=1)

        # Attach HA columns
        df = pd.concat([df, ha_df], axis=1)

        # ----------------------------
        # 2) NOW CALCULATE EMA ON HA
        # ----------------------------
        df[f"EMA_{EMA_20}"] = df["HA_Close"].ewm(span=EMA_20, adjust=False).mean()
        df[f"EMA_{EMA_50}"] = df["HA_Close"].ewm(span=EMA_50, adjust=False).mean()

        df = df.dropna().reset_index(drop=True)
        return df

    except Exception as e:
        print(f"[candles] {pair} error: {e}")
        return None


# ---------------------
def main():
    print("Fetching active USDT pairs...")
    pairs = get_active_usdt_coins()

    # Step 1: Fetch 1D changes
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

    # Step 2: Top gainers and losers
    top_gainers = df.sort_values("change", ascending=False).head(10)["pair"].tolist()
    top_losers = df.sort_values("change", ascending=True).head(10)["pair"].tolist()

    filtered_gainers = []
    filtered_losers = []

    # ==========================
    # Bullish (Gainers)
    # ==========================
    def check_gainer(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None or len(df_c) < 3:
            return None

        prev2 = df_c.iloc[-3]
        prev1 = df_c.iloc[-2]

        # HA Reversal: Red → Green
        cond_red_to_green = (
            prev2["HA_Close"] < prev2["HA_Open"]
            and prev1["HA_Close"] > prev1["HA_Open"]
        )

        # Only HA_Close matters (touch or wick allowed)
        cond_above_ema = (
            prev1["HA_Close"] >= prev1[f"EMA_{EMA_20}"]
            and prev1["HA_Close"] >= prev1[f"EMA_{EMA_50}"]
            and prev2["HA_Close"] >= prev2[f"EMA_{EMA_20}"]
            and prev2["HA_Close"] >= prev2[f"EMA_{EMA_50}"]
        )

        if cond_red_to_green and cond_above_ema:
            return pair
        return None

    # ==========================
    # Bearish (Losers)
    # ==========================
    def check_loser(pair):
        df_c = fetch_last_n_candles(pair)
        if df_c is None or len(df_c) < 3:
            return None

        prev2 = df_c.iloc[-3]
        prev1 = df_c.iloc[-2]

        # HA Reversal: Green → Red
        cond_green_to_red = (
            prev2["HA_Close"] > prev2["HA_Open"]
            and prev1["HA_Close"] < prev1["HA_Open"]
        )

        # Only HA_Close matters (wick/touch allowed)
        cond_below_ema = (
            prev1["HA_Close"] <= prev1[f"EMA_{EMA_20}"]
            and prev1["HA_Close"] <= prev1[f"EMA_{EMA_50}"]
            and prev2["HA_Close"] <= prev2[f"EMA_{EMA_20}"]
            and prev2["HA_Close"] <= prev2[f"EMA_{EMA_50}"]
        )

        if cond_green_to_red and cond_below_ema:
            return pair
        return None

    # Run checks concurrently
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        gain_futs = [executor.submit(check_gainer, p) for p in top_gainers]
        lose_futs = [executor.submit(check_loser, p) for p in top_losers]
        filtered_gainers = [f.result() for f in as_completed(gain_futs) if f.result()]
        filtered_losers = [f.result() for f in as_completed(lose_futs) if f.result()]

    # ===================================================
    # Step 4: Format Alerts with Two HA Close Prices (IST)
    # ===================================================
    def format_with_ha_close(pairs):
        formatted = []
        for pair in pairs:
            df_c = fetch_last_n_candles(pair)
            if df_c is not None and len(df_c) >= 2:
                prev1 = df_c.iloc[-3]
                last = df_c.iloc[-2]

                ha_close_prev = round(prev1["HA_Close"], 4)
                ha_close_last = round(last["HA_Close"], 4)

                # Fix timestamp (CoinDCX gives milliseconds)
                if "time" in df_c.columns:
                    ts = float(last["time"])
                    if ts > 1e12:  # milliseconds → seconds
                        ts = ts / 1000
                    ist_time = datetime.utcfromtimestamp(ts) + IST_OFFSET
                    time_str = ist_time.strftime("%Y-%m-%d %I:%M %p IST")
                else:
                    time_str = ""

                formatted.append(
                    f"{pair} → HA_Close(-3): {ha_close_prev}, HA_Close(-2): {ha_close_last}, Time: {time_str}"
                )
        return formatted

    # Step 5: Send Telegram alerts
    if filtered_gainers:
        gain_msg_list = format_with_ha_close(filtered_gainers)
        msg = "🟢 Bullish Heikin-Ashi Reversal (Above EMA20 & EMA50):\n" + "\n".join(gain_msg_list)
        print(msg)
        send_telegram_message(msg)

    if filtered_losers:
        lose_msg_list = format_with_ha_close(filtered_losers)
        msg = "🔴 Bearish Heikin-Ashi Reversal (Below EMA20 & EMA50):\n" + "\n".join(lose_msg_list)
        print(msg)
        send_telegram_message(msg)


if __name__ == "__main__":
    main()
