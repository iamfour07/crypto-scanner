import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_EMA import Send_EMA_Telegram_Message

# ================= CONFIGURATION =================
MAX_WORKERS = 20
RESOLUTION = "60" 
LIMIT_HOURS = 100
ACTIVE_INST_URL = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
CANDLE_URL = "https://public.coindcx.com/market_data/candlesticks"

# INDICATORS & RISK
BB_PERIOD, BB_STD = 20, 2
RSI_PERIOD = 14
RISK_INR = 50
LEVERAGE = 5

# ================= API FUNCTIONS =================

def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        pc = r.json().get("price_change_percent", {}).get("1D")
        if pc is None: return None
        return {"pair": pair, "change": float(pc)}
    except:
        return None

def fetch_candle_data(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    params = {"pair": pair, "from": now - (LIMIT_HOURS * 3600), "to": now, "resolution": RESOLUTION, "pcode": "f"}
    try:
        r = requests.get(CANDLE_URL, params=params, timeout=10).json()
        if not r.get("data"): return None
        df = pd.DataFrame(r["data"]).sort_values("time")
        for col in ["open", "high", "low", "close"]: df[col] = pd.to_numeric(df[col])
        
        if len(df) < 30: return None

        sma = df["close"].rolling(window=BB_PERIOD).mean()
        std = df["close"].rolling(window=BB_PERIOD).std(ddof=0)
        df["bb_up"] = sma + (BB_STD * std)
        df["bb_low"] = sma - (BB_STD * std)

        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).ewm(alpha=1/RSI_PERIOD, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/RSI_PERIOD, adjust=False).mean()
        df['rsi'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))

        curr = df.iloc[-2]  
        prev = df.iloc[-3]  
        
        return {
            "pair": pair, "close": curr["close"], "high": curr["high"], "low": curr["low"],
            "rsi": curr["rsi"], "bb_up": curr["bb_up"], "bb_low": curr["bb_low"],
            "p_high": prev["high"], "p_low": prev["low"],
            "p_bb_up": prev["bb_up"], "p_bb_low": prev["bb_low"]
        }
    except: return None

# ================= MAIN LOGIC =================

def main():
    # print("\n" + "="*40)
    # print("🚀 STARTING MARKET ANALYSIS")
    # print("="*40)
    
    try:
        all_pairs = requests.get(ACTIVE_INST_URL).json()
    except: return

    stats_list = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_pair_stats, p) for p in all_pairs if isinstance(p, str)]
        for f in as_completed(futures):
            res = f.result()
            if res: stats_list.append(res)

    if not stats_list: return
    df_stats = pd.DataFrame(stats_list)

    # --- NEW COUNTERS ---
    total_pairs = len(df_stats)
    num_gainers = len(df_stats[df_stats['change'] > 0])
    num_losers = total_pairs - num_gainers
    net_diff = ((num_gainers - num_losers) / total_pairs) * 100

    # print(f"📊 Total Pairs Found: {total_pairs}")
    # print(f"🟢 Gainer Coins: {num_gainers}")
    # print(f"🔴 Loser Coins: {num_losers}")
    # print(f"🏛️ Market Net Sentiment: {net_diff:.2f}%")

    # # Top Lists Print
    # print("\n🔥 TOP 5 GAINERS: " + ", ".join([f"{r['pair']}({r['change']:.1f}%)" for _,r in df_stats.sort_values("change", ascending=False).head(5).iterrows()]))
    # print("❄️ TOP 5 LOSERS: " + ", ".join([f"{r['pair']}({r['change']:.1f}%)" for _,r in df_stats.sort_values("change", ascending=True).head(5).iterrows()]))

    mode = "NONE"
    if net_diff >= 25:
        mode = "BUY"
        candidates = df_stats.sort_values("change", ascending=False).iloc[3:20]["pair"].tolist()
    elif net_diff <= -25:
        mode = "SELL"
        candidates = df_stats.sort_values("change", ascending=True).iloc[3:20]["pair"].tolist()
    else:
        print("\n🚫 MARKET NEUTRAL: Skipping Breakout Scan.")
        return

    # print(f"\n🔎 MODE: {mode} | Scanning {len(candidates)} High-Quality Candidates...")

    # Technical Scan
    signals = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = [executor.submit(fetch_candle_data, p) for p in candidates]
        for f in as_completed(results):
            row = f.result()
            if not row: continue

            if mode == "BUY":
                if row['close'] > row['bb_up'] and row['rsi'] > 60 and row['p_high'] < row['p_bb_up']:
                    entry, sl = row['high'], row['low']
                    dist = entry - sl
                    if dist > 0:
                        cap = ( (RISK_INR / dist) * entry ) / LEVERAGE
                        signals.append({"pair": row['pair'], "side": "BUY", "entry": entry, "sl": sl, "cap": cap, "r": dist})

            elif mode == "SELL":
                if row['close'] < row['bb_low'] and row['rsi'] < 40 and row['p_low'] > row['p_bb_low']:
                    entry, sl = row['low'], row['high']
                    dist = sl - entry
                    if dist > 0:
                        cap = ( (RISK_INR / dist) * entry ) / LEVERAGE
                        signals.append({"pair": row['pair'], "side": "SELL", "entry": entry, "sl": sl, "cap": cap, "r": dist})

    if signals:
        # print(f"✅ FOUND {len(signals)} SIGNALS! Sending to Telegram...")
        for s in signals:
            side_m = 1 if s['side'] == "BUY" else -1
            emoji = "🟢" if s['side'] == "BUY" else "🔴"
            msg = (
                f"{emoji} **BREAKOUT {s['side']}**\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🏛️ **Sentiment:** `{net_diff:.1f}%`\n"
                f"🪙 **Pair:** `{s['pair']}`\n"
                f"⚡ **Entry:** `{s['entry']:.6f}`\n"
                f"🛡️ **SL:** `{s['sl']:.6f}`\n"
                f"💰 **Margin (5x):** `₹{s['cap']:.2f}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🎯 **T1 (1:2):** `{s['entry'] + (side_m * s['r'] * 2):.6f}`\n"
                f"🎯 **T2 (1:3):** `{s['entry'] + (side_m * s['r'] * 3):.6f}`"
            )
            Send_EMA_Telegram_Message(msg)
    else:
        print(f"✅ SCAN COMPLETE: No fresh breakouts in Rank 4-20.")
    # print("="*40 + "\n")

if __name__ == "__main__":
    main()