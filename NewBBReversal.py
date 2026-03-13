import json
import requests
import pandas as pd
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Dono Telegram Modules import karein
try:
    from Telegram_Swing import Send_Swing_Telegram_Message
    from Telegram_Momentum import Send_Momentum_Telegram_Message
except ImportError:
    print("Error: Telegram modules (Telegram_Swing/Telegram_EMA) not found!")

# ================= MASTER CONFIG =================
resolution = "60"
limit_hours = 500
MAX_WORKERS = 15 

# Persistence Files
SWING_SELL_FILE = "ReversalSellWatchlist.json"
EMA_BUY_FILE    = "TrendPullbackBuy.json"
EMA_SELL_FILE   = "TrendPullbackSell.json"

# Strategy 1 Settings (Swing)
BB_LENGTH, BB_MULT = 200, 2.5
EMA_S1_FAST, EMA_S1_SLOW = 15, 45

# Strategy 2 Settings (Trend Pullback)
EMA_S2_FAST, EMA_S2_SLOW = 50, 200
RSI_P_BUY, RSI_T_BUY = 45, 55
RSI_P_SELL, RSI_T_SELL = 55, 45

# ================= INDICATORS =================
def calculate_all_indicators(df):
    # EMAs for Strategy 1
    df["EMA15"] = df["close"].ewm(span=EMA_S1_FAST, adjust=False).mean()
    df["EMA45"] = df["close"].ewm(span=EMA_S1_SLOW, adjust=False).mean()
    
    # EMAs for Strategy 2
    df["EMA20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["EMA50"] = df["close"].ewm(span=EMA_S2_FAST, adjust=False).mean()
    df["EMA200"] = df["close"].ewm(span=EMA_S2_SLOW, adjust=False).mean()

    # Bollinger Bands
    mid = df["close"].rolling(BB_LENGTH).mean()
    std = df["close"].rolling(BB_LENGTH).std()
    df["BB_upper"] = mid + BB_MULT * std

    # RSI
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df["RSI"] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
    return df

# ================= API HELPERS =================
def safe_get(url, params=None):
    try:
        r = requests.get(url, params=params, timeout=10)
        return r.json() if r.status_code == 200 else None
    except: return None

def fetch_candles(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - limit_hours * 3600
    url = "https://public.coindcx.com/market_data/candlesticks"
    params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}
    data = safe_get(url, params)
    if not data or "data" not in data: return None
    df = pd.DataFrame(data["data"]).sort_values("time").iloc[:-1]
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return calculate_all_indicators(df).dropna()

# ================= CORE LOGIC FUNCTIONS =================

def process_swing_logic(pair, swing_watch):
    """Bollinger Breakout + EMA 15/45 Cross Below"""
    df = fetch_candles(pair)
    if df is None: return None
    last = df.iloc[-1]
    
    # Check if already in watchlist
    if pair in swing_watch:
        prev_5 = df.iloc[-6:-1]
        # Just Cross Below: Current 15 < 45, but was above in last 5
        if last["EMA15"] < last["EMA45"] and (prev_5["EMA15"] > prev_5["EMA45"]).any():
            return ("SIGNAL_SWING", pair, last["close"])
        return ("KEEP_SWING", pair)
    else:
        # Scan New: Price above BB_upper and EMA 15 still above 45
        if last["close"] > last["BB_upper"] and last["EMA15"] > last["EMA45"]:
            return ("ADD_SWING", pair)
    return None

def process_ema_logic(pair, side, item):
    """EMA 50/200 Just-Cross + RSI Pullback"""
    df = fetch_candles(pair)
    if df is None: return None
    last = df.iloc[-1]
    
    if item: # Checking existing watchlist items
        prev_rsi = df.iloc[-2]["RSI"]
        if side == "BUY":
            if not (last["EMA50"] > last["EMA200"]): return ("REMOVE_EMA", pair)
            if last["RSI"] < RSI_P_BUY: item["pullback_done"] = True
            if item["pullback_done"] and last["RSI"] > RSI_T_BUY and prev_rsi <= RSI_T_BUY:
                return ("SIGNAL_EMA", pair, "BUY", last["close"])
        else: # SELL
            if not (last["EMA50"] < last["EMA200"]): return ("REMOVE_EMA", pair)
            if last["RSI"] > RSI_P_SELL: item["pullback_done"] = True
            if item["pullback_done"] and last["RSI"] < RSI_T_SELL and prev_rsi >= RSI_T_SELL:
                return ("SIGNAL_EMA", pair, "SELL", last["close"])
        return ("KEEP_EMA", item)
    else: # Scanning new coins
        prev_5 = df.iloc[-6:-1]
        if side == "BUY" and last["EMA20"] > last["EMA50"] > last["EMA200"]:
            if (prev_5["EMA50"] < prev_5["EMA200"]).any():
                return ("ADD_EMA", {"pair": pair, "pullback_done": False, "side": "BUY"})
        elif side == "SELL" and last["EMA20"] < last["EMA50"] < last["EMA200"]:
            if (prev_5["EMA50"] > prev_5["EMA200"]).any():
                return ("ADD_EMA", {"pair": pair, "pullback_done": False, "side": "SELL"})
    return None

# ================= MAIN MASTER =================
def main():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Master Scanner Started...")
    
    # 1. Load Persistence
    def load_json(file):
        try:
            with open(file, "r") as f: return json.load(f)
        except: return []

    s_watch = load_json(SW_FILE := SWING_SELL_FILE)
    e_buy_watch = load_json(EB_FILE := EMA_BUY_FILE)
    e_sell_watch = load_json(ES_FILE := EMA_SELL_FILE)

    # 2. Market Stats & Sorting
    url_stats = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    all_pairs = [x["pair"] if isinstance(x, dict) else x for x in (safe_get(url_stats) or [])]
    
    stats = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        def get_stat(p):
            res = safe_get(f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={p}")
            pc = res.get("price_change_percent", {}).get("1D") if res else None
            return {"pair": p, "change": float(pc)} if pc else None
        stats = [r for r in ex.map(get_stat, all_pairs) if r]

    gainers = sorted(stats, key=lambda x: x["change"], reverse=True)
    losers = sorted(stats, key=lambda x: x["change"])
    
    g_top8 = [x["pair"] for x in gainers[:8]]
    g_6_20 = [x["pair"] for x in gainers[5:20]]
    l_6_20 = [x["pair"] for x in losers[5:20]]

    # 3. Parallel Execution
    new_sw, sw_alerts = [], []
    new_eb, new_es, em_alerts = [], [], []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Strategy 1 Tasks
        tasks = [executor.submit(process_swing_logic, p, s_watch) for p in set(s_watch + g_top8)]
        # Strategy 2 Tasks
        tasks += [executor.submit(process_ema_logic, item['pair'], "BUY", item) for item in e_buy_watch]
        tasks += [executor.submit(process_ema_logic, item['pair'], "SELL", item) for item in e_sell_watch]
        tasks += [executor.submit(process_ema_logic, p, "BUY", None) for p in g_6_20]
        tasks += [executor.submit(process_ema_logic, p, "SELL", None) for p in l_6_20]

        for future in as_completed(tasks):
            res = future.result()
            if not res: continue
            
            tag = res[0]
            if tag == "SIGNAL_SWING": sw_alerts.append(f"🔴 **SWING SELL**: {res[1]}\nPrice: {res[2]}")
            elif tag in ["KEEP_SWING", "ADD_SWING"]: new_sw.append(res[1])
            elif tag == "SIGNAL_EMA": em_alerts.append(f"{'🟢' if res[2]=='BUY' else '🔴'} **EMA {res[2]}**: {res[1]}\nPrice: {res[3]}")
            elif tag == "KEEP_EMA" or tag == "ADD_EMA":
                if res[1]["side"] == "BUY": new_eb.append(res[1])
                else: new_es.append(res[1])

    # 4. Final Save & Telegram
    if sw_alerts: Send_Swing_Telegram_Message("\n\n".join(sw_alerts))
    if em_alerts: Send_Momentum_Telegram_Message("\n\n".join(em_alerts))

    with open(SW_FILE, "w") as f: json.dump(list(set(new_sw)), f, indent=2)
    with open(EB_FILE, "w") as f: json.dump(new_eb, f, indent=2)
    with open(ES_FILE, "w") as f: json.dump(new_es, f, indent=2)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Master Run Complete. Files Updated.")

if __name__ == "__main__":
    main()