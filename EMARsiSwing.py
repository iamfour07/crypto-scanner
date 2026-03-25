import json, requests, pandas as pd, os
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= CONFIG =================
RESOLUTION = "60" 
GAINER_FILE = "TopGainerWatchlist.json"
LOSER_FILE = "TopLoserWatchlist.json"
MAX_WORKERS = 20

try:
    from Telegram_Swing import Send_Swing_Telegram_Message
except ImportError:
    def Send_Swing_Telegram_Message(msg): 
        print(f"\n--- TELEGRAM ALERT ---\n{msg}\n----------------------")
Send_Swing_Telegram_Message("df")

# ================= INDICATORS (TRADINGVIEW STYLE) =================
def calculate_indicators(df):
    
    # EMA Calculation
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    
    # RSI 14 (Wilder's Smoothing Style to match Charts)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0))
    loss = (-delta.where(delta < 0, 0))
    
    # Use RMA (Running Moving Average) for RSI smoothing
    avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    
    rs = avg_gain / (avg_loss + 1e-9)
    df['rsi'] = 100 - (100 / (1 + rs))
    return df

def fetch_data(pair):
    url = "https://public.coindcx.com/market_data/candlesticks"
    now = int(datetime.now(timezone.utc).timestamp())
    # 500 hours data for better RSI accuracy (Smoothing window)
    params = {"pair": pair, "from": now - 500*3600, "to": now, "resolution": RESOLUTION, "pcode": "f"}
    try:
        r = requests.get(url, params=params, timeout=10).json()
        # Strictly removing the running candle to get the one you highlighted
        df = pd.DataFrame(r["data"]).sort_values("time").iloc[:-1] 
        for col in ["open", "high", "low", "close"]: 
            df[col] = pd.to_numeric(df[col])
        return calculate_indicators(df).dropna()
    except: 
        return None

# ================= MODULE 1: ALERT LOGIC =================
def check_alerts(pair, g_watch, l_watch):
    df = fetch_data(pair)
    if df is None or df.empty: return None
    last = df.iloc[-1] 
    last_rsi = round(last['rsi'], 2)

    if pair in g_watch:
        if last['rsi'] > 55:
            return ("ALERT_G", pair, last_rsi)
            
    if pair in l_watch:
        if last['rsi'] < 45:
            return ("ALERT_L", pair, last_rsi)
            
    return ("STAY", pair)

# ================= MODULE 2: WATCHLIST ADD LOGIC =================
def check_watchlist_add(pair):
    df = fetch_data(pair)
    if df is None or df.empty: return None
    
    last = df.iloc[-1] # This is the candle you highlighted in the screenshot
    last_rsi = round(last['rsi'], 2)
    last_close = round(last['close'], 6)
    
    # Condition: 20 > 50 > 200 AND RSI < 45
    uptrend = (last['ema20'] > last['ema50'] > last['ema200'])
    if uptrend and last['rsi'] < 45:
        # print(f"✅ ADDING GAINER: {pair} | RSI: {last_rsi} | Close: {last_close}")
        return ("ADD_G", pair)
    
    # Condition: 20 < 50 < 200 AND RSI > 55
    downtrend = (last['ema20'] < last['ema50'] < last['ema200'])
    if downtrend and last['rsi'] > 55:
        # print(f"✅ ADDING LOSER: {pair} | RSI: {last_rsi} | Close: {last_close}")
        return ("ADD_L", pair)
    
    return None

# ================= MAIN FLOW =================
def main():
    g_watch = json.load(open(GAINER_FILE)) if os.path.exists(GAINER_FILE) else []
    l_watch = json.load(open(LOSER_FILE)) if os.path.exists(LOSER_FILE) else []

    updated_g_watch = list(g_watch)
    updated_l_watch = list(l_watch)
    alerts_to_send = []

    # 1. Check Alerts First
    if g_watch or l_watch:
        # print("--- Checking Watchlists for Alerts ---")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            tasks = [executor.submit(check_alerts, p, g_watch, l_watch) for p in set(g_watch + l_watch)]
            for f in as_completed(tasks):
                res = f.result()
                if not res: continue
                if res[0] == "ALERT_G":
                    alerts_to_send.append(f"🚀 **GAINER ALERT**: {res[1]}\nRSI: {res[2]} (>55)\nAction: Removed.")
                    if res[1] in updated_g_watch: updated_g_watch.remove(res[1])
                elif res[0] == "ALERT_L":
                    alerts_to_send.append(f"📉 **LOSER ALERT**: {res[1]}\nRSI: {res[2]} (<45)\nAction: Removed.")
                    if res[1] in updated_l_watch: updated_l_watch.remove(res[1])

    if alerts_to_send:
        Send_Swing_Telegram_Message("\n\n".join(alerts_to_send))

    # 2. Scan Top 10 for New Adds (Always Runs)
    # print("--- Scanning Top 10 Gainers/Losers ---")
    all_pairs_raw = requests.get("https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT").json()
    all_pairs = [p for p in all_pairs_raw if isinstance(p, str)]

    def get_stats(p):
        try:
            d = requests.get(f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={p}", timeout=5).json()
            pc = d.get("price_change_percent", {}).get("1D", 0)
            return {"pair": p, "change": float(pc)}
        except: return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        stats = [r for r in ex.map(get_stats, all_pairs) if r]

    sorted_stats = sorted(stats, key=lambda x: x["change"])
    top__scan = [x["pair"] for x in sorted_stats[-15:]] + [x["pair"] for x in sorted_stats[:15]]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [executor.submit(check_watchlist_add, p) for p in top__scan]
        for f in as_completed(tasks):
            res = f.result()
            if not res: continue
            if res[0] == "ADD_G" and res[1] not in updated_g_watch:
                updated_g_watch.append(res[1])
            elif res[0] == "ADD_L" and res[1] not in updated_l_watch:
                updated_l_watch.append(res[1])

    with open(GAINER_FILE, "w") as f: json.dump(list(set(updated_g_watch)), f, indent=2)
    with open(LOSER_FILE, "w") as f: json.dump(list(set(updated_l_watch)), f, indent=2)
    print("Scan complete.")

if __name__ == "__main__":
    main()