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
RESOLUTION = "60"           
LIMIT_HOURS = 1000           
MAX_WORKERS = 20            
FILE_NAME = "ReversalSellWatchlist.json"

ST_PERIOD = 20
ST_MULTIPLIER = 2

RISK_PER_TRADE = 500      
LEVERAGE = 10  
MAX_CAPITAL = 5000               

# ================= INDICATOR CALCULATIONS =================
def calculate_indicators(df):
    ha_close = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    ha_open = np.zeros(len(df))
    ha_open[0] = (df['open'].iloc[0] + df['close'].iloc[0]) / 2
    for i in range(1, len(df)):
        ha_open[i] = (ha_open[i-1] + ha_close.iloc[i-1]) / 2

    df['HA_Open'] = ha_open
    df['HA_Close'] = ha_close
    df['HA_High'] = df[['high', 'HA_Open', 'HA_Close']].max(axis=1)
    df['HA_Low'] = df[['low', 'HA_Open', 'HA_Close']].min(axis=1)

    tr1 = df['HA_High'] - df['HA_Low']
    tr2 = (df['HA_High'] - df['HA_Close'].shift(1)).abs()
    tr3 = (df['HA_Low'] - df['HA_Close'].shift(1)).abs()
    df['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['ATR'] = df['TR'].ewm(alpha=1/ST_PERIOD, min_periods=ST_PERIOD, adjust=False).mean()

    df['hl2'] = (df['HA_High'] + df['HA_Low']) / 2
    df['upper_basic'] = df['hl2'] + (ST_MULTIPLIER * df['ATR'])
    df['lower_basic'] = df['hl2'] - (ST_MULTIPLIER * df['ATR'])

    final_upper = [0.0] * len(df)
    final_lower = [0.0] * len(df)
    direction = [1] * len(df)
    flip_level = [np.nan] * len(df) 

    first_valid = df['upper_basic'].first_valid_index()
    if first_valid is None:
        df['st_dir'], df['st_upper'], df['st_lower'], df['flip_level'] = direction, final_upper, final_lower, flip_level
        return df

    final_upper[first_valid] = df['upper_basic'].iloc[first_valid]
    final_lower[first_valid] = df['lower_basic'].iloc[first_valid]

    for i in range(first_valid + 1, len(df)):
        if df['upper_basic'].iloc[i] < final_upper[i-1] or df['HA_Close'].iloc[i-1] > final_upper[i-1]:
            final_upper[i] = df['upper_basic'].iloc[i]
        else:
            final_upper[i] = final_upper[i-1]

        if df['lower_basic'].iloc[i] > final_lower[i-1] or df['HA_Close'].iloc[i-1] < final_lower[i-1]:
            final_lower[i] = df['lower_basic'].iloc[i]
        else:
            final_lower[i] = final_lower[i-1]

        if direction[i-1] == 1:
            if df['HA_Close'].iloc[i] < final_lower[i]:
                direction[i] = -1
                flip_level[i] = final_upper[i-1]
            else:
                direction[i] = 1        
        else:
            if df['HA_Close'].iloc[i] > final_upper[i]:
                direction[i] = 1
                flip_level[i] = final_lower[i-1]
            else:
                direction[i] = -1

    df['st_dir'] = direction
    df['st_upper'] = final_upper
    df['st_lower'] = final_lower
    df['flip_level'] = flip_level 

    return df

# ================= DATA FETCHING =================
def fetch_candles(pair):
    url = "https://public.coindcx.com/market_data/candlesticks"
    now = int(datetime.now(timezone.utc).timestamp())
    params = {"pair": pair, "from": now - LIMIT_HOURS * 3600, "to": now, "resolution": RESOLUTION, "pcode": "f"}
    try:
        response = requests.get(url, params=params, timeout=10)
        r = response.json()
        if not isinstance(r, dict) or "data" not in r or not r["data"]: return None
        df = pd.DataFrame(r["data"]).sort_values("time").reset_index(drop=True)
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        if len(df) < 52: return None
        df = df.iloc[:-1]
        return calculate_indicators(df)
    except: return None

# ================= CORE SIGNAL LOGIC =================
def process_logic(pair, watch_list):
    df = fetch_candles(pair)
    if df is None or df.empty: return None

    last = df.iloc[-1]
    prev = df.iloc[-2]

    last_dir = int(last['st_dir'])
    prev_dir = int(prev['st_dir'])

    if pair not in watch_list:
        if last_dir == 1: return {"type": "ADD", "pair": pair}
        return None

    # REVERSAL ALERT: Green to Red
    if prev_dir == 1 and last_dir == -1:
        
        flip_candle = df.iloc[-1]

        entry = flip_candle["low"]
        sl = flip_candle["flip_level"]
        
        risk = sl - entry
        if risk <= 0: return {"type": "KEEP", "pair": pair}

        # ✅ NEW: SL % filter
        sl_pct = abs(risk) / entry * 100
        if sl_pct < 1.0:
            return {"type": "KEEP", "pair": pair}

        # ✅ NEW: risk-based qty
        qty_risk = RISK_PER_TRADE / risk

        # ✅ NEW: margin cap
        # change as per your capital

        qty_margin_cap = (MAX_CAPITAL * LEVERAGE) / entry

        # ✅ FINAL qty
        qty = min(qty_risk, qty_margin_cap)

        margin = (qty * entry) / LEVERAGE

        return {
            "type": "SIGNAL", "pair": pair,
            "entry": entry, "sl": sl,
            "margin": margin,
            "t2": entry - (risk * 2),
            "t3": entry - (risk * 3)
        }

    if last_dir == 1: return {"type": "KEEP", "pair": pair}
    return None

# ================= MAIN EXECUTION =================
def main():
    if os.path.exists(FILE_NAME):
        with open(FILE_NAME, "r") as f:
            try: watch_list = json.load(f)
            except: watch_list = []
    else:
        watch_list = []

    try:
        url_active = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
        all_pairs = [p for p in requests.get(url_active).json() if isinstance(p, str)]
    except Exception as e:
        print(f"❌ Failed to fetch active pairs: {e}")
        return

    def get_stats(p):
        try:
            d = requests.get(f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={p}", timeout=5).json()
            pc = d.get("price_change_percent", {}).get("1D", 0)
            return {"pair": p, "change": float(pc)}
        except: return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        stats = [r for r in ex.map(get_stats, all_pairs) if r]

    top_gainers = [x["pair"] for x in sorted(stats, key=lambda x: x["change"], reverse=True)[:5]]
    scan_pool = list(set(watch_list + top_gainers))

    alerts, new_watchlist, signaled_pairs = [], [], []

    print(f"\n--- Scanning {len(scan_pool)} pairs ---")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = {executor.submit(process_logic, p, watch_list): p for p in scan_pool}
        for future in as_completed(tasks):
            pair = tasks[future]
            try:
                res = future.result()
                if not res: continue
                if res["type"] == "SIGNAL":
                    msg = (
                        f"🔴 **HA REVERSAL (SELL)**: {res['pair']}\n"
                        f"Entry (Low): {res['entry']:.6f}\n"
                        f"SL (ST Upper): {res['sl']:.6f}\n"
                        f"Margin: ₹{res['margin']:.2f}\n"
                        f"🎯 T2: {res['t2']:.6f}"
                    )
                    alerts.append(msg)
                    signaled_pairs.append(res['pair'])
                elif res["type"] in ["KEEP", "ADD"]:
                    new_watchlist.append(res["pair"])
            except: continue

    final_watchlist = sorted(list(set([p for p in new_watchlist if p not in signaled_pairs])))
    with open(FILE_NAME, "w") as f:
        json.dump(final_watchlist, f, indent=2)

    if alerts:
        for alert_msg in alerts:
            Send_Momentum_Telegram_Message(alert_msg)

    print(f"\nScan complete. Signals: {len(alerts)} | Watchlist: {len(final_watchlist)}")

if __name__ == "__main__":
    main()