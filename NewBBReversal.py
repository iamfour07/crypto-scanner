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
LIMIT_HOURS = 500           # Indicator smoothing ke liye accurate lookback
MAX_WORKERS = 25            
FILE_NAME = "ReversalSellWatchlist.json"

# Indicators Parameters
BB_LENGTH, BB_MULT = 200, 2.5
ST_LENGTH, ST_MULT = 20, 2
RSI_PERIOD = 14

# Risk Management
RISK_PER_TRADE = 100        # ₹100 Risk amount
LEVERAGE = 5                # 5x Leverage

# ================= INDICATOR CALCULATIONS =================
def calculate_indicators(df):
    # 1. Bollinger Bands
    mid = df["close"].rolling(BB_LENGTH).mean()
    std = df["close"].rolling(BB_LENGTH).std()
    df["BB_upper"] = mid + BB_MULT * std

    # 2. SuperTrend (TradingView Style with RMA/Wilder's ATR)
    hl2 = (df["high"] + df["low"]) / 2
    df["tr"] = pd.concat([
        df["high"] - df["low"],
        abs(df["high"] - df["close"].shift()),
        abs(df["low"] - df["close"].shift())
    ], axis=1).max(axis=1)
    
    # ATR using Running Moving Average (RMA)
    df["atr"] = df["tr"].ewm(alpha=1/ST_LENGTH, min_periods=ST_LENGTH, adjust=False).mean()
    df["upperband"] = hl2 + ST_MULT * df["atr"]
    df["lowerband"] = hl2 - ST_MULT * df["atr"]

    st_list, dir_list = [0.0] * len(df), [True] * len(df)
    for i in range(1, len(df)):
        if df["close"].iloc[i] > st_list[i-1]: dir_list[i] = True
        elif df["close"].iloc[i] < st_list[i-1]: dir_list[i] = False
        else: dir_list[i] = dir_list[i-1]
        
        if dir_list[i]:
            st_list[i] = max(df["lowerband"].iloc[i], st_list[i-1])
        else:
            st_list[i] = min(df["upperband"].iloc[i], st_list[i-1])
            
    df["supertrend"], df["st_dir"] = st_list, dir_list

    # 3. RSI (Wilder's Smoothing / RMA Style)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0))
    loss = (-delta.where(delta < 0, 0))
    
    avg_gain = gain.ewm(alpha=1/RSI_PERIOD, min_periods=RSI_PERIOD, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/RSI_PERIOD, min_periods=RSI_PERIOD, adjust=False).mean()
    
    rs = avg_gain / (avg_loss + 1e-9)
    df['rsi'] = 100 - (100 / (1 + rs))

    return df

# ================= DATA FETCHING =================
def fetch_candles(pair):
    url = "https://public.coindcx.com/market_data/candlesticks"
    now = int(datetime.now(timezone.utc).timestamp())
    params = {
        "pair": pair, 
        "from": now - LIMIT_HOURS * 3600, 
        "to": now, 
        "resolution": RESOLUTION, 
        "pcode": "f"
    }
    try:
        r = requests.get(url, params=params, timeout=10).json()
        df = pd.DataFrame(r["data"]).sort_values("time").iloc[:-1] 
        for col in ["open", "high", "low", "close"]: 
            df[col] = pd.to_numeric(df[col])
        
        if len(df) < BB_LENGTH: return None
        return calculate_indicators(df).dropna()
    except Exception: return None

# ================= CORE SIGNAL LOGIC =================
def process_logic(pair, watch_list):
    df = fetch_candles(pair)
    if df is None or df.empty: return None

    last = df.iloc[-1]
    last_rsi = round(last['rsi'], 2)

    # --- CASE 1: Check for Sell Signal (Already in Watchlist) ---
    if pair in watch_list:
        # UPDATED LOGIC: No flip required, just check CURRENT state
        # Agar SuperTrend RED (st_dir is False) AUR RSI < 45
        if last["st_dir"] == False and last['rsi'] < 45:
            entry = last["close"]
            sl = max(last["supertrend"], last["high"])
            risk_per_coin = sl - entry
            
            if risk_per_coin <= 1e-9: return None 

            qty = RISK_PER_TRADE / risk_per_coin
            margin = (qty * entry) / LEVERAGE

            return {
                "type": "SIGNAL", "pair": pair, "entry": entry, "sl": sl, 
                "margin": margin, "rsi": last_rsi,
                "t2": entry - (risk_per_coin * 2), 
                "t3": entry - (risk_per_coin * 3), 
                "t4": entry - (risk_per_coin * 4)
            }
        
        # Agar condition meet nahi hui, list mein hi rakho
        return {"type": "KEEP", "pair": pair}

    # --- CASE 2: Discovery (Add new coins to Watchlist) ---
    else:
        # Breakout condition: Price BB Upper ke upar ho aur SuperTrend Green ho
        breakout = last["close"] > last["BB_upper"]
        if breakout and last["st_dir"] == True:
            return {"type": "ADD", "pair": pair}

    return None

# ================= MAIN EXECUTION =================
def main():
    if os.path.exists(FILE_NAME):
        with open(FILE_NAME, "r") as f: watch_list = json.load(f)
    else: watch_list = []

    try:
        url_active = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
        all_pairs_raw = requests.get(url_active).json()
        all_pairs = [p for p in all_pairs_raw if isinstance(p, str)]
    except Exception as e:
        print(f"Error fetching active pairs: {e}")
        return

    def get_stats(p):
        try:
            d = requests.get(f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={p}", timeout=5).json()
            pc = d.get("price_change_percent", {}).get("1D", 0)
            return {"pair": p, "change": float(pc)}
        except: return None

    print("Fetching market stats...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        stats = [r for r in ex.map(get_stats, all_pairs) if r]
    
    top_gainers = [x["pair"] for x in sorted(stats, key=lambda x: x["change"], reverse=True)[:15]]
    scan_pool = list(set(watch_list + top_gainers))

    alerts, new_watchlist = [], []

    print(f"Scanning {len(scan_pool)} coins...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [executor.submit(process_logic, p, watch_list) for p in scan_pool]
        for f in as_completed(tasks):
            res = f.result()
            if not res: continue
            
            if res["type"] == "SIGNAL":
                msg = (
                    f"🔴 **REVERSAL SELL**: {res['pair']}\n"
                    f"RSI: {res['rsi']} (Target < 45)\n"
                    f"Entry: {res['entry']:.6f}\n"
                    f"Stop Loss: {res['sl']:.6f}\n"
                    f"Capital: ₹{res['margin']:.2f} (5x)\n\n"
                    f"🎯 T2: {res['t2']:.6f}\n"
                    f"🎯 T3: {res['t3']:.6f}\n"
                    f"🎯 T4: {res['t4']:.6f}"
                )
                alerts.append(msg)
            elif res["type"] in ["KEEP", "ADD"]:
                new_watchlist.append(res["pair"])

    if alerts:
        Send_Momentum_Telegram_Message("\n\n".join(alerts))
    
    # Signaled coins ko list se remove karna taaki repeat alert na aaye
    signaled_pairs = []
    for a in alerts:
        try:
            p_name = a.split("**")[1].split("**")[0].split(":")[1].strip()
            signaled_pairs.append(p_name)
        except: continue

    final_list = sorted(list(set([p for p in new_watchlist if p not in signaled_pairs])))

    with open(FILE_NAME, "w") as f:
        json.dump(final_list, f, indent=2)
    
    print(f"Scan complete. Watchlist size: {len(final_list)}")

if __name__ == "__main__":
    main()