import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_EMA import Send_EMA_Telegram_Message

# ================= CONFIGURATION =================
MAX_WORKERS = 25            # Candidates badh gaye hain, isliye workers badha diye
RESOLUTION = "60" 
LIMIT_HOURS = 1000        
CANDLE_URL = "https://public.coindcx.com/market_data/candlesticks"
ACTIVE_INST_URL = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"

# INDICATORS & RISK
BB_PERIOD, BB_STD = 20, 2
RISK_INR = 100               # Fixed Loss per trade
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
        return {"pair": pair, "change": float(pc)} if pc is not None else None
    except: return None

def scan_strategy(pair, change_pct):
    now = int(datetime.now(timezone.utc).timestamp())
    params = {"pair": pair, "from": now - (LIMIT_HOURS * 3600), "to": now, "resolution": RESOLUTION, "pcode": "f"}
    try:
        r = requests.get(CANDLE_URL, params=params, timeout=10).json()
        if not r.get("data") or len(r["data"]) < 50: return None

        df = pd.DataFrame(r["data"]).sort_values("time")
        df = df.iloc[:-1] # Only closed candles
        for col in ["open", "high", "low", "close"]: df[col] = pd.to_numeric(df[col])

        # 1. HA Conversion & Indicators
        ha_df = convert_to_heikin_ashi(df)
        sma = ha_df["ha_close"].rolling(window=BB_PERIOD).mean()
        std = ha_df["ha_close"].rolling(window=BB_PERIOD).std(ddof=0)
        ha_df["bb_up"] = sma + (BB_STD * std)
        ha_df["bb_low"] = sma - (BB_STD * std)

        curr = ha_df.iloc[-1]   
        prev = ha_df.iloc[-2]   

        # --- LOGIC 1: SHORT (Momentum Cooldown in Losers) ---
        if change_pct < 0:
            prev_touches_up = prev['ha_high'] >= prev['bb_up']
            curr_cools_up = curr['ha_high'] < curr['bb_up']
            
            if prev_touches_up and curr_cools_up:
                entry, sl, target = curr['ha_low'], prev['ha_high'], curr['bb_low']
                risk = sl - entry
                if risk > 0:
                    return {"side": "SHORT", "pair": pair, "entry": entry, "sl": sl, "target": target, "m": (RISK_INR/risk * entry)/LEVERAGE, "ch": change_pct}

        # --- LOGIC 2: LONG (Bullish Pullback in Gainers) ---
        else:
            prev_is_red = prev['ha_close'] < prev['ha_open']
            prev_touches_low = prev['ha_low'] <= prev['bb_low']
            curr_cools_low = curr['ha_low'] > curr['bb_low']
            
            if prev_is_red and prev_touches_low and curr_cools_low:
                entry, sl, target = curr['ha_high'], prev['ha_low'], curr['bb_up']
                risk = entry - sl
                if risk > 0:
                    return {"side": "LONG", "pair": pair, "entry": entry, "sl": sl, "target": target, "m": (RISK_INR/risk * entry)/LEVERAGE, "ch": change_pct}
        
        return None
    except: return None

# ================= MAIN LOGIC =================

def main():
    print(f"🔄 Scanning Market (Dual Logic) | {datetime.now().strftime('%H:%M:%S')}")
    try:
        all_pairs = requests.get(ACTIVE_INST_URL).json()
    except: return

    stats = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(fetch_pair_stats, p) for p in all_pairs if isinstance(p, str)]
        for f in as_completed(futs):
            r = f.result()
            if r: stats.append(r)

    if not stats: return
    df_stats = pd.DataFrame(stats)

    # Candidates: Top 20 Gainers + Top 20 Losers
    top_gainers = df_stats.sort_values("change", ascending=False).head(40)
    top_losers = df_stats.sort_values("change", ascending=True).head(40)
    candidates = pd.concat([top_gainers, top_losers])

    signals = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        results = [ex.submit(scan_strategy, row['pair'], row['change']) for _, row in candidates.iterrows()]
        for f in as_completed(results):
            sig = f.result()
            if sig: signals.append(sig)

    if signals:
        for s in signals:
            icon = "🔥" if s['side'] == "LONG" else "❄️"
            msg = (
                f"{icon} **HA {s['side']} ALERT**\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🪙 **Pair:** `{s['pair']}` ({s['ch']}%)\n"
                f"⚡ **Entry:** `{s['entry']:.6f}`\n"
                f"🛡️ **SL:** `{s['sl']:.6f}`\n"
                f"🎯 **Target:** `{s['target']:.6f}`\n"
                f"💰 **Margin (5x):** `₹{s['m']:.2f}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ Risk: ₹100 | Leverage: 5x"
            )
            Send_EMA_Telegram_Message(msg)
            print(f"✅ {s['side']} Signal Sent for {s['pair']}")
    else:
        print("ℹ️ Scan Complete: No setups found.")

if __name__ == "__main__":
    main()
