import json, requests, pandas as pd, os
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
LIMIT_HOURS = 200           
MAX_WORKERS = 20            
FILE_NAME = "ReversalSellWatchlist.json"
RSI_PERIOD = 14
RSI_THRESHOLD = 60

RISK_PER_TRADE = 100        
LEVERAGE = 5                

# ================= INDICATOR CALCULATIONS =================
def calculate_indicators(df):
    # 1. RSI Calculation (On Standard Close)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0))
    loss = (-delta.where(delta < 0, 0))
    avg_gain = gain.ewm(alpha=1/RSI_PERIOD, min_periods=RSI_PERIOD, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/RSI_PERIOD, min_periods=RSI_PERIOD, adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    df['rsi'] = 100 - (100 / (1 + rs))

    # 2. HEIKIN-ASHI Calculation
    ha_df = pd.DataFrame(index=df.index)
    ha_df["HA_Close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    ha_df["HA_Open"] = (df["open"] + df["close"]) / 2 # Seed value
    
    for i in range(1, len(df)):
        ha_df.iloc[i, ha_df.columns.get_loc("HA_Open")] = (ha_df.iloc[i-1]["HA_Open"] + ha_df.iloc[i-1]["HA_Close"]) / 2
    
    ha_df["HA_High"] = df[["high", "open", "close"]].max(axis=1)
    ha_df["HA_Low"] = df[["low", "open", "close"]].min(axis=1)
    
    df["HA_Open"], df["HA_Close"] = ha_df["HA_Open"], ha_df["HA_Close"]
    df["HA_High"], df["HA_Low"] = ha_df["HA_High"], ha_df["HA_Low"]
    
    return df

# ================= DATA FETCHING =================
def fetch_candles(pair):
    url = "https://public.coindcx.com/market_data/candlesticks"
    now = int(datetime.now(timezone.utc).timestamp())
    params = {"pair": pair, "from": now - LIMIT_HOURS * 3600, "to": now, "resolution": RESOLUTION, "pcode": "f"}
    try:
        r = requests.get(url, params=params, timeout=10).json()
        df = pd.DataFrame(r["data"]).sort_values("time").iloc[:-1] 
        for col in ["open", "high", "low", "close"]: df[col] = pd.to_numeric(df[col])
        if len(df) < 30: return None
        return calculate_indicators(df).dropna()
    except: return None

# ================= CORE SIGNAL LOGIC =================
def process_logic(pair, watch_list):
    df = fetch_candles(pair)
    if df is None or df.empty: return None

    last = df.iloc[-1]   # Current HA Candle
    prev = df.iloc[-2]   # Previous HA Candle

    # Logic 1: Add to Watchlist if RSI > 60
    if pair not in watch_list:
        if last['rsi'] > RSI_THRESHOLD:
            return {"type": "ADD", "pair": pair}
        return None

    # Logic 2: Signal if RSI crosses below 60 (FOR WATCHLIST COINS)
    if prev['rsi'] > RSI_THRESHOLD and last['rsi'] < RSI_THRESHOLD:
        entry = last["HA_Low"]   # Current Heikin-Ashi Low
        sl = prev["HA_High"]     # Previous Heikin-Ashi High
        risk = sl - entry
        
        if risk <= 0: return {"type": "KEEP", "pair": pair}
        
        qty = RISK_PER_TRADE / risk
        margin = (qty * entry) / LEVERAGE

        return {
            "type": "SIGNAL", "pair": pair, "entry": entry, "sl": sl, 
            "margin": margin, "rsi_now": round(last['rsi'], 2),
            "t2": entry - (risk * 2), "t3": entry - (risk * 3)
        }
    
    # Logic 3: Always KEEP in watchlist until signal (Removed Auto-Cleanup)
    return {"type": "KEEP", "pair": pair}

# ================= MAIN EXECUTION =================
def main():
    if os.path.exists(FILE_NAME):
        with open(FILE_NAME, "r") as f: watch_list = json.load(f)
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

    print("Fetching Top 10 Gainers & Scanning Watchlist...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        stats = [r for r in ex.map(get_stats, all_pairs) if r]
    
    top_gainers = [x["pair"] for x in sorted(stats, key=lambda x: x["change"], reverse=True)[:10]]
    scan_pool = list(set(watch_list + top_gainers))

    alerts, new_watchlist, signaled_pairs = [], [], []

    print(f"Scanning {len(scan_pool)} pairs in Heikin-Ashi mode...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [executor.submit(process_logic, p, watch_list) for p in scan_pool]
        for f in as_completed(tasks):
            res = f.result()
            if not res: continue
            
            if res["type"] == "SIGNAL":
                msg = (
                    f"🔴 **HA RSI REVERSAL (SHORT)**: {res['pair']}\n"
                    f"Logic: RSI Breakdown below {RSI_THRESHOLD}\n"
                    f"RSI Now: {res['rsi_now']}\n"
                    f"Entry (HA Low): {res['entry']:.6f}\n"
                    f"Stop Loss (HA High): {res['sl']:.6f}\n"
                    f"Capital: ₹{res['margin']:.2f}\n"
                    f"🎯 T2: {res['t2']:.6f} | T3: {res['t3']:.6f}"
                )
                alerts.append(msg)
                # Coin will NOT be in new_watchlist, effectively removing it
                signaled_pairs.append(res['pair'])
            elif res["type"] in ["KEEP", "ADD"]:
                new_watchlist.append(res["pair"])

    # Final Cleanup: Save coins that are in new_watchlist but NOT the ones that gave alert
    final_watchlist = sorted(list(set(new_watchlist)))
    with open(FILE_NAME, "w") as f:
        json.dump(final_watchlist, f, indent=2)

    if alerts:
        Send_Momentum_Telegram_Message("\n\n".join(alerts))
    
    print(f"Scan complete. Signals: {len(alerts)} | Watchlist updated: {len(final_watchlist)}")

if __name__ == "__main__":
    main()
