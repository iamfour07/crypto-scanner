import json, requests, pandas as pd, os
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= TELEGRAM CONFIG =================
try:
    from Telegram_Momentum import Send_Momentum_Telegram_Message
except ImportError:
    def Send_Momentum_Telegram_Message(msg): 
        print(f"\n--- TELEGRAM ALERT ---\n{msg}\n----------------------")

# ================= STRATEGY CONFIG =================
RESOLUTION = "60"           # 1 Hour timeframe
LIMIT_HOURS = 1000           # Accurate RSI ke liye data badha diya hai
MAX_WORKERS = 20            
FILE_NAME = "ReversalSellWatchlist.json"
RSI_PERIOD = 14
RSI_THRESHOLD = 60

RISK_PER_TRADE = 250       
LEVERAGE = 5                

# ================= INDICATOR CALCULATIONS =================
def calculate_indicators(df):
   # 1. PEHLE HEIKIN-ASHI CALCULATE KARO
    ha_close = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    ha_open = np.zeros(len(df))
    ha_open[0] = (df['open'].iloc[0] + df['close'].iloc[0]) / 2
    for i in range(1, len(df)):
        ha_open[i] = (ha_open[i-1] + ha_close.iloc[i-1]) / 2
        
    df['HA_Open'] = ha_open
    df['HA_Close'] = ha_close
    df['HA_High'] = df[['high', 'HA_Open', 'HA_Close']].max(axis=1)
    df['HA_Low'] = df[['low', 'HA_Open', 'HA_Close']].min(axis=1)

    # 2. AB RSI ME 'HA_Close' USE KARO (Agar HA based RSI chahiye)
    # Note: Agar normal RSI chahiye jo TV se match kare, toh 'close' hi rehne do
    delta = df['HA_Close'].diff() 
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.ewm(alpha=1/RSI_PERIOD, min_periods=RSI_PERIOD, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/RSI_PERIOD, min_periods=RSI_PERIOD, adjust=False).mean()

    rs = avg_gain / (avg_loss + 1e-9)
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # ... baaki Heikin-Ashi wala code bilkul sahi hai ...

    # 2. HEIKIN-ASHI Calculation
    ha_df = pd.DataFrame(index=df.index)
    ha_df["HA_Close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    
    # Correct HA Open calculation
    ha_open = np.zeros(len(df))
    ha_open[0] = (df["open"].iloc[0] + df["close"].iloc[0]) / 2
    for i in range(1, len(df)):
        ha_open[i] = (ha_open[i-1] + ha_df["HA_Close"].iloc[i-1]) / 2
    
    df["HA_Open"] = ha_open
    df["HA_Close"] = ha_df["HA_Close"]
    df["HA_High"] = df[["high", "HA_Open", "HA_Close"]].max(axis=1)
    df["HA_Low"] = df[["low", "HA_Open", "HA_Close"]].min(axis=1)
    
    return df

# ================= DATA FETCHING =================
def fetch_candles(pair):
    url = "https://public.coindcx.com/market_data/candlesticks"
    now = int(datetime.now(timezone.utc).timestamp())
    params = {"pair": pair, "from": now - LIMIT_HOURS * 3600, "to": now, "resolution": RESOLUTION, "pcode": "f"}
    try:
        r = requests.get(url, params=params, timeout=10).json()
        # Last closed candle tak ka data (iloc[:-1] current running candle hata deta hai)
        df = pd.DataFrame(r["data"]).sort_values("time").iloc[:-1] 
        for col in ["open", "high", "low", "close"]: df[col] = pd.to_numeric(df[col])
        
        if len(df) < 50: return None # Accurate RSI needs enough history
        return calculate_indicators(df)
    except: return None

# ================= CORE SIGNAL LOGIC =================
def process_logic(pair, watch_list):
    df = fetch_candles(pair)
    if df is None or df.empty: return None

    last = df.iloc[-1]   # Current HA Candle
    prev = df.iloc[-2]   # Previous HA Candle
    
    current_rsi = round(last['rsi'], 2)

    # # Watchlist monitoring ke liye RSI print
    # if pair in watch_list:
    #     print(f"👀 WATCHING: {pair.ljust(12)} | RSI: {current_rsi}")

    # Logic 1: Add to Watchlist if RSI > 60
    if pair not in watch_list:
        if last['rsi'] > RSI_THRESHOLD:
            # print(f"➕ ADDED: {pair} (RSI: {current_rsi})")
            return {"type": "ADD", "pair": pair}
        return None

    # Logic 2: Signal if RSI crosses below 60 (FOR WATCHLIST COINS)
    # prev rsi 60 ke upar tha aur ab 60 ke niche close hua hai
    if prev['rsi'] >= RSI_THRESHOLD and last['rsi'] < RSI_THRESHOLD:
        entry = last["HA_Low"]   
        sl = prev["HA_High"]     
        risk = sl - entry
        
        if risk <= 0: return {"type": "KEEP", "pair": pair}
        
        qty = RISK_PER_TRADE / risk
        margin = (qty * entry) / LEVERAGE

        return {
            "type": "SIGNAL", "pair": pair, "entry": entry, "sl": sl, 
            "margin": margin, "rsi_now": current_rsi,
            "t2": entry - (risk * 2), "t3": entry - (risk * 3)
        }
    
    # Logic 3: Always KEEP in watchlist until signal
    return {"type": "KEEP", "pair": pair}

# ================= MAIN EXECUTION =================
def main():
    if os.path.exists(FILE_NAME):
        with open(FILE_NAME, "r") as f: 
            try: watch_list = json.load(f)
            except: watch_list = []
    else: watch_list = []

    try:
        url_active = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
        all_pairs = [p for p in requests.get(url_active).json() if isinstance(p, str)]
    except: return

    def get_stats(p):
        try:
            d = requests.get(f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={p}", timeout=5).json()
            pc = d.get("price_change_percent", {}).get("1D", 0)
            return {"pair": p, "change": float(pc)}
        except: return None

    # print("\n--- Fetching Market Stats ---")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        stats = [r for r in ex.map(get_stats, all_pairs) if r]
    
    top_gainers = [x["pair"] for x in sorted(stats, key=lambda x: x["change"], reverse=True)[:5]]
    scan_pool = list(set(watch_list + top_gainers))

    alerts, new_watchlist, signaled_pairs = [], [], []

    # print(f"--- Scanning {len(scan_pool)} pairs ---\n")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [executor.submit(process_logic, p, watch_list) for p in scan_pool]
        for f in as_completed(tasks):
            res = f.result()
            if not res: continue
            
            if res["type"] == "SIGNAL":
                msg = (
                    f"🔴 **HA RSI REVERSAL (SHORT)**: {res['pair']}\n"
                    f"RSI Now: {res['rsi_now']}\n"
                    f"Entry (HA Low): {res['entry']:.6f}\n"
                    f"Stop Loss (HA High): {res['sl']:.6f}\n"
                    f"Capital: ₹{res['margin']:.2f}\n"
                    f"🎯 T2: {res['t2']:.6f}"
                )
                alerts.append(msg)
                signaled_pairs.append(res['pair'])
            elif res["type"] in ["KEEP", "ADD"]:
                new_watchlist.append(res["pair"])

    # Final Cleanup: Save coins that didn't signal
    final_watchlist = sorted(list(set([p for p in new_watchlist if p not in signaled_pairs])))
    with open(FILE_NAME, "w") as f:
        json.dump(final_watchlist, f, indent=2)

    if alerts:
        for alert_msg in alerts:
            Send_Momentum_Telegram_Message(alert_msg)
    
    # print(f"\nScan complete. Signals: {len(alerts)} | Watchlist updated: {len(final_watchlist)}")

if __name__ == "__main__":
    main()