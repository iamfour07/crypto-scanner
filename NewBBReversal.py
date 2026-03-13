import json
import requests
import pandas as pd
import time
import os
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Dono Telegram Modules import karein
try:
    from Telegram_Swing import Send_Swing_Telegram_Message
    from Telegram_Momentum import Send_Momentum_Telegram_Message
except ImportError:
    print("Error: Telegram modules not found!")

# ================= MASTER CONFIG =================
resolution = "60"
limit_hours = 500
MAX_WORKERS = 15 

SWING_SELL_FILE = "ReversalSellWatchlist.json"
EMA_BUY_FILE    = "TrendPullbackBuy.json"
EMA_SELL_FILE   = "TrendPullbackSell.json"

BB_LENGTH, BB_MULT = 200, 2.5
EMA_S1_FAST, EMA_S1_SLOW = 15, 45
EMA_S2_FAST, EMA_S2_SLOW = 50, 200
RSI_P_BUY, RSI_T_BUY = 45, 55
RSI_P_SELL, RSI_T_SELL = 55, 45

# ================= INDICATORS =================
def calculate_all_indicators(df):
    df["EMA15"] = df["close"].ewm(span=EMA_S1_FAST, adjust=False).mean()
    df["EMA45"] = df["close"].ewm(span=EMA_S1_SLOW, adjust=False).mean()
    df["EMA20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["EMA50"] = df["close"].ewm(span=EMA_S2_FAST, adjust=False).mean()
    df["EMA200"] = df["close"].ewm(span=EMA_S2_SLOW, adjust=False).mean()

    mid = df["close"].rolling(BB_LENGTH).mean()
    std = df["close"].rolling(BB_LENGTH).std()
    df["BB_upper"] = mid + BB_MULT * std

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
    df = fetch_candles(pair)
    if df is None: return None
    last = df.iloc[-1]
    
    if pair in swing_watch:
        prev_5 = df.iloc[-6:-1]
        if last["EMA15"] < last["EMA45"] and (prev_5["EMA15"] > prev_5["EMA45"]).any():
            return ("SIGNAL_SWING", pair, last["close"])
        return ("KEEP_SWING", pair)
    else:
        if last["close"] > last["BB_upper"] and last["EMA15"] > last["EMA45"]:
            return ("ADD_SWING", pair)
    return None

def process_ema_logic(pair, side, item):
    df = fetch_candles(pair)
    if df is None: return None
    last = df.iloc[-1]
    
    if item:
        prev_rsi = df.iloc[-2]["RSI"]
        if side == "BUY":
            if not (last["EMA50"] > last["EMA200"]): return ("REMOVE_EMA", pair)
            if last["RSI"] < RSI_P_BUY: item["pullback_done"] = True
            if item.get("pullback_done") and last["RSI"] > RSI_T_BUY and prev_rsi <= RSI_T_BUY:
                return ("SIGNAL_EMA", pair, "BUY", last["close"])
        else:
            if not (last["EMA50"] < last["EMA200"]): return ("REMOVE_EMA", pair)
            if last["RSI"] > RSI_P_SELL: item["pullback_done"] = True
            if item.get("pullback_done") and last["RSI"] < RSI_T_SELL and prev_rsi >= RSI_T_SELL:
                return ("SIGNAL_EMA", pair, "SELL", last["close"])
        return ("KEEP_EMA", item)
    else:
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
    
    def load_json(file):
        try:
            with open(file, "r") as f: return json.load(f)
        except: return []

    s_watch = load_json(SWING_SELL_FILE)
    e_buy_watch = load_json(EMA_BUY_FILE)
    e_sell_watch = load_json(EMA_SELL_FILE)

    existing_ema = {item['pair'] for item in e_buy_watch} | {item['pair'] for item in e_sell_watch}

    url_stats = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    all_pairs_data = safe_get(url_stats)
    if not all_pairs_data: return
    all_pairs = [x["pair"] if isinstance(x, dict) else x for x in all_pairs_data]
    
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

    new_sw, sw_alerts = [], []
    new_eb, new_es, em_alerts = [], [], []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = []
        for p in set(s_watch + g_top8):
            tasks.append(executor.submit(process_swing_logic, p, s_watch))
        for item in e_buy_watch: tasks.append(executor.submit(process_ema_logic, item['pair'], "BUY", item))
        for item in e_sell_watch: tasks.append(executor.submit(process_ema_logic, item['pair'], "SELL", item))
        for p in g_6_20:
            if p not in existing_ema: tasks.append(executor.submit(process_ema_logic, p, "BUY", None))
        for p in l_6_20:
            if p not in existing_ema: tasks.append(executor.submit(process_ema_logic, p, "SELL", None))

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

    # Final Deduplication
    final_sw = list(set(new_sw))
    final_eb = list({v['pair']: v for v in new_eb}.values())
    final_es = list({v['pair']: v for v in new_es}.values())

    if sw_alerts: Send_Swing_Telegram_Message("\n\n".join(sw_alerts))
    if em_alerts: Send_Momentum_Telegram_Message("\n\n".join(em_alerts))

    with open(SWING_SELL_FILE, "w") as f: json.dump(final_sw, f, indent=2)
    with open(EMA_BUY_FILE, "w") as f: json.dump(final_eb, f, indent=2)
    with open(EMA_SELL_FILE, "w") as f: json.dump(final_es, f, indent=2)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Master Run Complete.")

if __name__ == "__main__":
    main()