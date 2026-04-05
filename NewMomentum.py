import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_EMA import Send_EMA_Telegram_Message

# ================= CONFIGURATION =================
MAX_WORKERS = 20
RESOLUTION = "60" 
LIMIT_HOURS = 400           # Stability ke liye data depth 400 hours
CANDLE_URL = "https://public.coindcx.com/market_data/candlesticks"
ACTIVE_INST_URL = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"

# INDICATORS & RISK
BB_PERIOD, BB_STD = 20, 2
RSI_PERIOD = 14
RISK_INR = 50               # Per trade fixed loss
LEVERAGE = 5

# ================= API FUNCTIONS =================

def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        r = requests.get(url, timeout=10)
        pc = r.json().get("price_change_percent", {}).get("1D")
        if pc is None: return None
        return {"pair": pair, "change": float(pc)}
    except: return None

def fetch_candle_data(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    params = {"pair": pair, "from": now - (LIMIT_HOURS * 3600), "to": now, "resolution": RESOLUTION, "pcode": "f"}
    try:
        r = requests.get(CANDLE_URL, params=params, timeout=10).json()
        if not r.get("data"): return None
        
        # Sort and take data till last CLOSED candle
        df = pd.DataFrame(r["data"]).sort_values("time")
        # iloc[:-1] ensure karta hai ki current running candle ignore ho
        df = df.iloc[:-1] 
        
        for col in ["open", "high", "low", "close"]: df[col] = pd.to_numeric(df[col])
        
        if len(df) < 50: return None

        # 1. Bollinger Bands Calculation
        sma = df["close"].rolling(window=BB_PERIOD).mean()
        std = df["close"].rolling(window=BB_PERIOD).std(ddof=0)
        df["bb_up"] = sma + (BB_STD * std)

        # 2. RSI Calculation (Wilder's Smoothing/RMA)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).ewm(alpha=1/RSI_PERIOD, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/RSI_PERIOD, adjust=False).mean()
        df['rsi'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))

        curr = df.iloc[-1]   # Last Closed Candle
        prev = df.iloc[-2]   # Previous Closed Candle
        
        # --- STRATEGY CONDITIONS ---
        # Cond 1: BB Breakout (Current Close > Upper BB, Prev Close <= Upper BB)
        bb_breakout = (curr['close'] > curr['bb_up']) and (prev['close'] <= prev['bb_up'])
        
        # Cond 2: RSI Cross 60 (Current RSI > 60, Prev RSI <= 60)
        rsi_breakout = (curr['rsi'] > 60) and (prev['rsi'] <= 60)

        if bb_breakout and rsi_breakout:
            entry = curr['high']
            sl = curr['low']
            dist = entry - sl
            
            if dist > 0:
                # Quantity and Margin based on ₹50 Risk
                qty = RISK_INR / dist
                margin = (qty * entry) / LEVERAGE
                
                return {
                    "pair": pair, "entry": entry, "sl": sl, "r": dist, 
                    "margin": margin, "rsi_now": curr['rsi'], "rsi_prev": prev['rsi']
                }
        return None
    except: return None

# ================= MAIN LOGIC =================

def main():
    print(f"\n🚀 Scanning Top 20 Gainers | RSI + BB Strategy | Time: {datetime.now().strftime('%H:%M:%S')}")
    
    try:
        all_pairs = requests.get(ACTIVE_INST_URL).json()
    except: return

    # Step 1: Filter Top 20 Gainers
    stats_list = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_pair_stats, p) for p in all_pairs if isinstance(p, str)]
        for f in as_completed(futures):
            res = f.result()
            if res: stats_list.append(res)

    if not stats_list: return
    
    df_stats = pd.DataFrame(stats_list)
    candidates = df_stats.sort_values("change", ascending=False).head(20)["pair"].tolist()

    # Step 2: Technical Scan on Candidates
    signals = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = [executor.submit(fetch_candle_data, p) for p in candidates]
        for f in as_completed(results):
            sig = f.result()
            if sig: signals.append(sig)

    # Step 3: Alerts Generation
    if signals:
        for s in signals:
            msg = (
                f"🔥 **BB + RSI BREAKOUT (BUY)**\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🪙 **Pair:** `{s['pair']}`\n"
                f"📈 **RSI:** `{s['rsi_prev']:.1f}` ➔ `{s['rsi_now']:.1f}`\n"
                f"⚡ **Entry (High):** `{s['entry']:.6f}`\n"
                f"🛡️ **SL (Low):** `{s['sl']:.6f}`\n"
                f"💰 **Margin (5x):** `₹{s['margin']:.2f}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🎯 **T1 (1:2):** `{s['entry'] + (s['r'] * 2):.6f}`\n"
                f"🎯 **T2 (1:3):** `{s['entry'] + (s['r'] * 3):.6f}`\n"
                f"🎯 **T3 (1:4):** `{s['entry'] + (s['r'] * 4):.6f}`"
            )
            Send_EMA_Telegram_Message(msg)
            print(f"✅ Alert Sent for {s['pair']}")
    else:
        print("ℹ️ Scan Complete: No coins met both RSI and BB breakout conditions.")

if __name__ == "__main__":
    main()