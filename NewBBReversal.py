import json, requests, pandas as pd, os, time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Telegram Module
try:
    from Telegram_Momentum import Send_Momentum_Telegram_Message
except ImportError:
    print("Error: Telegram module not found!")

# ================= CONFIG =================
RESOLUTION = "60"
LIMIT_HOURS = 1000
MAX_WORKERS = 20 # Stats fetch karne ke liye zyada workers
FILE_NAME = "ReversalSellWatchlist.json"

BB_LENGTH, BB_MULT = 200, 2.5  # COS-USDT detect karne ke liye 20 standard hai
EMA_FAST, EMA_SLOW = 15, 45

# ================= API HELPERS =================
def safe_get(url, params=None, timeout=10):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        return r.json() if r.status_code == 200 else None
    except: return None

# Step 2: Individual stats nikalne ka function (Nested JSON logic)
def fetch_pair_stats(pair):
    url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
    data = safe_get(url, timeout=8)
    if not data: return None
    # Aapke naye nested API structure ke hisaab se:
    pc = data.get("price_change_percent", {}).get("1D")
    return {"pair": pair, "change": float(pc)} if pc is not None else None

def calculate_indicators(df):
    df["EMA15"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["EMA45"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    mid = df["close"].rolling(BB_LENGTH).mean()
    std = df["close"].rolling(BB_LENGTH).std()
    df["BB_upper"] = mid + BB_MULT * std
    return df

def fetch_candles(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    url = "https://public.coindcx.com/market_data/candlesticks"
    params = {
        "pair": pair, 
        "from": now - LIMIT_HOURS*3600, 
        "to": now, 
        "resolution": RESOLUTION, 
        "pcode": "f"
    }
    # Yahan params add karein
    data = safe_get(url, params=params) 
    
    if not data or "data" not in data: return None
    df = pd.DataFrame(data["data"]).sort_values("time").iloc[:-1]
    
    # Check if we have enough data for BB_LENGTH
    if len(df) < BB_LENGTH:
        return None
        
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return calculate_indicators(df).dropna()

# ================= CORE LOGIC =================
def process_logic(pair, watch_list):
    df = fetch_candles(pair)
    if df is None or df.empty: 
        return None
    
    last = df.iloc[-1]        # Current Candle
    prev_candle = df.iloc[-2]  # Just pichli Candle
    
    if pair in watch_list:
        # --- ACCURATE CROSSOVER LOGIC ---
        # 1. Abhi EMA15, EMA45 ke niche hai
        # 2. Pichli candle par EMA15, EMA45 ke upar tha (Strict Cross)
        # 3. Close Price bhi EMA45 ke niche hai (Price Confirmation)
        
        is_cross = last["EMA15"] < last["EMA45"] and prev_candle["EMA15"] > prev_candle["EMA45"]
        price_confirmed = last["close"] < last["EMA45"]
        
        if is_cross and price_confirmed:
            return ("SIGNAL", pair, last["close"])
        
        # Watchlist mein banaye rakhne ke liye check
        # Agar trend abhi bhi upar hai ya BB ke pas hai, toh list mein rakho
        if last["EMA15"] > last["EMA45"] or last["close"] > last["BB_upper"] * 0.98:
            return ("KEEP", pair)
        return None # Agar trend kharab ho gaya bina signal ke, toh list se hata do
        
    else:
        # Watchlist mein add karne ka criteria (BB Upper Breakout)
        if last["close"] > last["BB_upper"]:
            # print(f"✅ MATCH FOUND: {pair} | Price: {last['close']} > BB: {round(last['BB_upper'], 4)}")
            return ("ADD", pair)
            
    return None
# ================= MAIN FLOW =================
def main():
    # print(f"[{datetime.now().strftime('%H:%M:%S')}] Reversal Scanner Started...")
    
    # 1. Watchlist load karein
    if os.path.exists(FILE_NAME):
        with open(FILE_NAME, "r") as f: watch_list = json.load(f)
    else: watch_list = []

    # STEP 1: Pehle active coins nikal (Active Instruments)
    # print("Fetching active coins list...")
    url_all = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    all_pairs_raw = safe_get(url_all)
    if not all_pairs_raw: return
    all_pairs = [p for p in all_pairs_raw if isinstance(p, str)]

    # STEP 2: Uske baad % change nikal (Individual Stats)
    # print(f"Calculating % change for {len(all_pairs)} coins...")
    stats_list = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        stats_list = [r for r in ex.map(fetch_pair_stats, all_pairs) if r]

    # STEP 3: Sort by % change (Gainers)
    sorted_gainers = sorted(stats_list, key=lambda x: x["change"], reverse=True)
    
    # Debug Table
    # print("\n" + "="*35)
    # print(f"{'TOP GAINERS':<18} | {'CHANGE 1D':<10}")
    # print("-" * 35)
    # for x in sorted_gainers[:20]: # Top 20 Gainers dikhayega
    #     print(f"{x['pair']:<18} | {x['change']:>8}%")
    # print("="*35 + "\n")

    top_pairs = [x['pair'] for x in sorted_gainers[:10]] # Scanning pool

    # STEP 4: Indicators aur Logic check karein
    tasks, results, alerts = [], [], []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        scan_pool = list(set(watch_list + top_pairs))
        for p in scan_pool:
            tasks.append(executor.submit(process_logic, p, watch_list))
        
        for future in as_completed(tasks):
            res = future.result()
            if not res: continue
            if res[0] == "SIGNAL":
                alerts.append(f"🔴 **REVERSAL SELL**: {res[1]}\nPrice: {res[2]}")
            elif res[0] in ["KEEP", "ADD"]:
                results.append(res[1])

    # STEP 5: Telegram aur Save
    if alerts:
        Send_Momentum_Telegram_Message("\n\n".join(alerts))

    with open(FILE_NAME, "w") as f:
        json.dump(list(set(results)), f, indent=2)
    
    # print(f"[{datetime.now().strftime('%H:%M:%S')}] Done. Watchlist Size: {len(results)}")

if __name__ == "__main__":
    main()