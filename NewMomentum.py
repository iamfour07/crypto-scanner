import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_EMA import Send_EMA_Telegram_Message

# ================= CONFIGURATION =================
MAX_WORKERS = 20
RESOLUTION = "60" 
LIMIT_HOURS = 400           
CANDLE_URL = "https://public.coindcx.com/market_data/candlesticks"
ACTIVE_INST_URL = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"

# INDICATORS & RISK
BB_PERIOD, BB_STD = 20, 2
RISK_INR = 100               # New Risk: ₹100
LEVERAGE = 5

# ================= HEIKIN-ASHI CONVERSION =================

def convert_to_heikin_ashi(df):
    ha_df = df.copy()
    ha_df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    
    ha_open = np.zeros(len(df))
    ha_open[0] = (df['open'].iloc[0] + df['close'].iloc[0]) / 2
    for i in range(1, len(df)):
        ha_open[i] = (ha_open[i-1] + ha_df['ha_close'].iloc[i-1]) / 2
    ha_df['ha_open'] = ha_open
    
    ha_df['ha_high'] = ha_df[['high', 'ha_open', 'ha_close']].max(axis=1)
    ha_df['ha_low'] = ha_df[['low', 'ha_open', 'ha_close']].min(axis=1)
    return ha_df

# ================= API FUNCTIONS =================

def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        r = requests.get(url, timeout=10)
        pc = r.json().get("price_change_percent", {}).get("1D")
        return {"pair": pair, "change": float(pc)} if pc else None
    except: return None

def fetch_candle_data(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    params = {"pair": pair, "from": now - (LIMIT_HOURS * 3600), "to": now, "resolution": RESOLUTION, "pcode": "f"}
    try:
        r = requests.get(CANDLE_URL, params=params, timeout=10).json()
        if not r.get("data") or len(r["data"]) < 50: return None

        df = pd.DataFrame(r["data"]).sort_values("time")
        df = df.iloc[:-1] # Last closed candle tak data
        for col in ["open", "high", "low", "close"]: df[col] = pd.to_numeric(df[col])

        # 1. HA Conversion
        ha_df = convert_to_heikin_ashi(df)

        # 2. Indicators (Based on HA Close)
        sma = ha_df["ha_close"].rolling(window=BB_PERIOD).mean()
        std = ha_df["ha_close"].rolling(window=BB_PERIOD).std(ddof=0)
        ha_df["bb_up"] = sma + (BB_STD * std)
        ha_df["bb_low"] = sma - (BB_STD * std)

        curr = ha_df.iloc[-1]   # Last Closed Candle
        prev = ha_df.iloc[-2]   # Prev Candle (Jo touch karni chahiye)

        # --- LOGIC ---
        # Prev high touches or crosses Upper BB
        prev_touches = prev['ha_high'] >= prev['bb_up']
        # Current high does NOT touch Upper BB
        curr_cools = curr['ha_high'] < curr['bb_up']

        if prev_touches and curr_cools:
            entry = curr['ha_low']
            sl = prev['ha_high']
            target = curr['bb_low']
            risk_per_unit = sl - entry

            if risk_per_unit > 0:
                qty = RISK_INR / risk_per_unit
                margin = (qty * entry) / LEVERAGE
                
                return {
                    "pair": pair, "entry": entry, "sl": sl, "target": target,
                    "margin": margin, "change": None # Added in main
                }
        return None
    except: return None

# ================= MAIN LOGIC =================

def main():
    print(f"📉 Scanning Top Losers | Time: {datetime.now().strftime('%H:%M:%S')}")
    try:
        all_pairs = requests.get(ACTIVE_INST_URL).json()
    except: return

    stats_list = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_pair_stats, p) for p in all_pairs if isinstance(p, str)]
        for f in as_completed(futures):
            res = f.result()
            if res: stats_list.append(res)

    if not stats_list: return

    # Sort by Losers (Ascending order of price change)
    df_stats = pd.DataFrame(stats_list).sort_values("change", ascending=True)
    candidates = df_stats.head(20)["pair"].tolist()

    signals = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = [executor.submit(fetch_candle_data, p) for p in candidates]
        for f in as_completed(results):
            sig = f.result()
            if sig: signals.append(sig)

    if signals:
        for s in signals:
            msg = (
                f"❄️ **HA COOLDOWN SHORT**\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🪙 **Pair:** `{s['pair']}`\n"
                f"📉 **Entry (Curr Low):** `{s['entry']:.6f}`\n"
                f"🛡️ **SL (Prev High):** `{s['sl']:.6f}`\n"
                f"🎯 **Target (Lower BB):** `{s['target']:.6f}`\n"
                f"💰 **Margin (5x):** `₹{s['margin']:.2f}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ Risk: ₹100 per trade"
            )
            Send_EMA_Telegram_Message(msg)
            print(f"✅ Alert Sent for {s['pair']}")
    else:
        print("ℹ️ Scan Complete: No setups found.")

if __name__ == "__main__":
    main()
