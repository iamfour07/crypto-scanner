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
LIMIT_HOURS = 500           
MAX_WORKERS = 25            
FILE_NAME = "ReversalSellWatchlist.json"

# EMA Periods
EMA_FAST = 5
EMA_MEDIUM = 10
EMA_SLOW = 100

# Indicators Parameters
BB_LENGTH, BB_MULT = 200, 2.5
RSI_PERIOD = 14

# Risk Management
RISK_PER_TRADE = 100        
LEVERAGE = 5                

# ================= MARKET STATUS MODULE =================
def get_market_sentiment(stats):
    """
    Calculates the net difference between gainer and loser coins 
    to determine overall market direction.
    """
    total_coins = len(stats)
    if total_coins == 0:
        return "⚠️ Market Status: No data available."

    gainers = [s for s in stats if s['change'] > 0]
    losers = [s for s in stats if s['change'] < 0]
    
    num_gainers = len(gainers)
    num_losers = len(losers)
    
    # Decision Logic based on Difference
    diff = num_gainers - num_losers
    
    if diff > 0:
        status = "🟢 BULLISH (Buy Side)"
        advice = "Buyers are dominating the market."
    elif diff < 0:
        status = "🔴 BEARISH (Sell Side)"
        advice = "Sellers are dominating the market."
    else:
        status = "⚪ NEUTRAL"
        advice = "Market is perfectly balanced."

    msg = (
        f"📊 **MARKET STATUS REPORT**\n"
        f"Status: **{status}**\n\n"
        f"✅ Buy Side (Gainers): {num_gainers}\n"
        f"❌ Sell Side (Losers): {num_losers}\n"
        f"⚖️ Net Difference: {diff:+}\n\n"
        f"Note: {advice}"
    )
    return msg

# ================= INDICATOR CALCULATIONS =================
def calculate_indicators(df):
    mid = df["close"].rolling(BB_LENGTH).mean()
    std = df["close"].rolling(BB_LENGTH).std()
    df["BB_upper"] = mid + BB_MULT * std

    df['ema_fast'] = df['close'].ewm(span=EMA_FAST, adjust=False).mean()
    df['ema_med'] = df['close'].ewm(span=EMA_MEDIUM, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=EMA_SLOW, adjust=False).mean()

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
        
        if len(df) < max(BB_LENGTH, EMA_SLOW): return None
        return calculate_indicators(df).dropna()
    except Exception: return None

# ================= CORE SIGNAL LOGIC =================
def process_logic(pair, watch_list):
    df = fetch_candles(pair)
    if df is None or df.empty: return None

    last = df.iloc[-1]
    prev = df.iloc[-2]
    last_rsi = round(last['rsi'], 2)

    if pair in watch_list:
        momentum_bearish = last['ema_fast'] < last['ema_med']
        ema_crossover = (prev['ema_med'] >= prev['ema_slow']) and (last['ema_med'] < last['ema_slow'])

        if momentum_bearish and ema_crossover:
            entry = last["close"]
            sl = max(last["ema_slow"], last["high"])
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
        return {"type": "KEEP", "pair": pair}
    else:
        if last["close"] > last["BB_upper"]:
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

    print("Fetching market stats for Market Status...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        stats = [r for r in ex.map(get_stats, all_pairs) if r]
    
    # --- 1. SEND MARKET STATUS ALERT ---
    market_msg = get_market_sentiment(stats)
    print(market_msg)
    Send_Momentum_Telegram_Message(market_msg)
    
    # --- 2. PREPARE DISCOVERY ---
    top_gainers = [x["pair"] for x in sorted(stats, key=lambda x: x["change"], reverse=True)[:15]]
    scan_pool = list(set(watch_list + top_gainers))

    alerts, new_watchlist = [], []

    print(f"Scanning {len(scan_pool)} coins for signals...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [executor.submit(process_logic, p, watch_list) for p in scan_pool]
        for f in as_completed(tasks):
            res = f.result()
            if not res: continue
            
            if res["type"] == "SIGNAL":
                msg = (
                    f"🔴 **REVERSAL SELL**: {res['pair']}\n"
                    f"Logic: EMA Cross + Bearish Momentum\n"
                    f"RSI: {res['rsi']}\n"
                    f"Entry: {res['entry']:.6f}\n"
                    f"Stop Loss: {res['sl']:.6f}\n"
                    f"Capital: ₹{res['margin']:.2f} (5x)\n\n"
                    f"🎯 T2: {res['t2']:.6f} | T3: {res['t3']:.6f} | T4: {res['t4']:.6f}"
                )
                alerts.append(msg)
            elif res["type"] in ["KEEP", "ADD"]:
                new_watchlist.append(res["pair"])

    if alerts:
        Send_Momentum_Telegram_Message("\n\n".join(alerts))
    
    # Cleanup Watchlist
    signaled_pairs = []
    for a in alerts:
        try:
            p_name = a.split("**")[1].split(":")[1].strip() if ":" in a.split("**")[1] else a.split("**")[1].split(" ")[-1].strip()
            signaled_pairs.append(p_name)
        except: pass

    final_list = sorted(list(set([p for p in new_watchlist if p not in signaled_pairs])))

    with open(FILE_NAME, "w") as f:
        json.dump(final_list, f, indent=2)
    
    print(f"Scan complete. Signals: {len(alerts)}, Watchlist: {len(final_list)}")

if __name__ == "__main__":
    main()