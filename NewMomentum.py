import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from Telegram_EMA import Send_EMA_Telegram_Message

# ================= CONFIGURATION =================
MAX_WORKERS = 25   
RESOLUTION = "60"  # 1 Hour
LIMIT_HOURS = 100  
ACTIVE_INST_URL = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
CANDLE_URL = "https://public.coindcx.com/market_data/candlesticks"

# INDICATORS & RISK
BB_PERIOD, BB_STD = 20, 2
RSI_PERIOD = 14
RISK_INR = 50      # Your fixed risk per trade
LEVERAGE = 10      # Change this to your preferred leverage

def fetch_market_data(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    params = {"pair": pair, "from": now - (LIMIT_HOURS) * 3600, "to": now, "resolution": RESOLUTION, "pcode": "f"}
    try:
        r = requests.get(CANDLE_URL, params=params, timeout=10).json()
        if not r.get("data"): return None
        df = pd.DataFrame(r["data"]).sort_values("time")
        for col in ["open", "high", "low", "close"]: df[col] = pd.to_numeric(df[col])
        
        if len(df) < 30: return None

        # BB (ddof=0 for exact chart matching)
        sma = df["close"].rolling(window=BB_PERIOD).mean()
        std = df["close"].rolling(window=BB_PERIOD).std(ddof=0)
        df["bb_up"] = sma + (BB_STD * std)
        df["bb_low"] = sma - (BB_STD * std)

        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).ewm(alpha=1/RSI_PERIOD, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/RSI_PERIOD, adjust=False).mean()
        df['rsi'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))

        # CANDLE DEFINITIONS
        curr = df.iloc[-2]  # The Alert Candle (Last Closed)
        prev = df.iloc[-3]  # The Previous Candle
        
        # 24h Change for Ranking
        open_24h = df.iloc[-26]["open"] if len(df) >= 26 else df.iloc[0]["open"]
        change_pct = ((curr["close"] - open_24h) / open_24h) * 100
        
        return {
            "pair": pair, "close": curr["close"], "high": curr["high"], "low": curr["low"],
            "change": change_pct, "rsi": curr["rsi"],
            "bb_up": curr["bb_up"], "bb_low": curr["bb_low"],
            "p_high": prev["high"], "p_low": prev["low"],
            "p_bb_up": prev["bb_up"], "p_bb_low": prev["bb_low"]
        }
    except: return None

def main():
    # print("🚀 Scanning Market Sentiment and First-Touch Breakouts...")
    try:
        all_pairs = [p for p in requests.get(ACTIVE_INST_URL).json() if isinstance(p, str)]
    except: return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        all_stats = [r for r in executor.map(fetch_market_data, all_pairs) if r]

    if not all_stats: return
    df_all = pd.DataFrame(all_stats)
    
    # 1. Market Status Logic
    num_gainers = len(df_all[df_all['change'] > 0])
    total_pairs = len(df_all)
    net_diff = ((num_gainers - (total_pairs - num_gainers)) / total_pairs) * 100
    
    market_report = f"📊 **MARKET STATUS REPORT**\nNet Difference: **{net_diff:.1f}%**\n"
    
    mode = "NONE"
    if net_diff >= 50:
        mode = "BUY"
        market_report += "Status: 🟢 **BULLISH** (Scanning Buys)"
        scan_list = df_all.sort_values("change", ascending=False).iloc[3:15]
    elif net_diff <= -50:
        mode = "SELL"
        market_report += "Status: 🔴 **BEARISH** (Scanning Sells)"
        scan_list = df_all.sort_values("change", ascending=True).iloc[3:15]
    else:
        market_report += "Status: ⚪ **NEUTRAL** (No Signals)"
        # print(market_report)
        return

    # print(market_report)
    # Optional: Uncomment the next line if you want the market report on Telegram every run
    # Send_EMA_Telegram_Message(market_report)

    # 2. Strict Signal Logic
    signals = []
    for _, row in scan_list.iterrows():
        if mode == "BUY":
            if row['close'] > row['bb_up'] and row['rsi'] > 60 and row['p_high'] < row['p_bb_up']:
                entry, sl = row['high'], row['low']
                point_risk = entry - sl
                if point_risk > 0:
                    qty = RISK_INR / point_risk
                    capital_inr = (qty * entry) / LEVERAGE
                    signals.append({
                        "pair": row['pair'], "side": "BUY", "entry": entry, "sl": sl, 
                        "capital": capital_inr, "risk_dist": point_risk, "rsi": row['rsi']
                    })
        
        elif mode == "SELL":
            if row['close'] < row['bb_low'] and row['rsi'] < 40 and row['p_low'] > row['p_bb_low']:
                entry, sl = row['low'], row['high']
                point_risk = sl - entry
                if point_risk > 0:
                    qty = RISK_INR / point_risk
                    capital_inr = (qty * entry) / LEVERAGE
                    signals.append({
                        "pair": row['pair'], "side": "SELL", "entry": entry, "sl": sl, 
                        "capital": capital_inr, "risk_dist": point_risk, "rsi": row['rsi']
                    })

    # 3. Output Trade Details & Telegram Alerts
    if signals:
        for s in signals:
            r = s['risk_dist']
            side_mult = 1 if s['side'] == "BUY" else -1
            t1_2 = s['entry'] + (side_mult * r * 2)
            t1_3 = s['entry'] + (side_mult * r * 3)
            t1_4 = s['entry'] + (side_mult * r * 4)

            # Console Output
            # print(f"🔥 {s['side']} SIGNAL: {s['pair']}")
            
            # Telegram Message Construction
            emoji = "🟢" if s['side'] == "BUY" else "🔴"
            tele_msg = (
                f"{emoji} **BREAKOUT {s['side']}**\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"⚖️ **Market Status:** `{net_diff:.1f}%`\n"  # Added this line
                f"🪙 **Pair:** `{s['pair']}`\n"
                f"⚡ **Entry:** `{s['entry']:.6f}`\n"
                f"🛡️ **SL:** `{s['sl']:.6f}`\n"
                f"💰 **Margin:** `₹{s['capital']:.2f}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🎯 **T1 (1:2):** `{s['entry'] + (side_mult * r * 2):.6f}`\n"
                f"🎯 **T2 (1:3):** `{s['entry'] + (side_mult * r * 3):.6f}`\n"
                f"🎯 **T3 (1:4):** `{s['entry'] + (side_mult * r * 4):.6f}`"
            )
            Send_EMA_Telegram_Message(tele_msg)
    else:
         print("\nNo 'First-Touch' breakout signals detected.")

if __name__ == "__main__":
    main()