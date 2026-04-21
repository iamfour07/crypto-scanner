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

# ================= INDICATOR CALCULATIONS =================
def calculate_indicators(df):
    # 1. HEIKIN-ASHI
    ha_close = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    ha_open = np.zeros(len(df))
    ha_open[0] = (df['open'].iloc[0] + df['close'].iloc[0]) / 2
    for i in range(1, len(df)):
        ha_open[i] = (ha_open[i-1] + ha_close.iloc[i-1]) / 2
        
    df['HA_Open'] = ha_open
    df['HA_Close'] = ha_close
    df['HA_High'] = df[['high', 'HA_Open', 'HA_Close']].max(axis=1)
    df['HA_Low'] = df[['low', 'HA_Open', 'HA_Close']].min(axis=1)

    # 2. ATR using RMA on HA candles
    tr1 = df['HA_High'] - df['HA_Low']
    tr2 = (df['HA_High'] - df['HA_Close'].shift(1)).abs()
    tr3 = (df['HA_Low'] - df['HA_Close'].shift(1)).abs()
    df['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['ATR'] = df['TR'].ewm(alpha=1/ST_PERIOD, min_periods=ST_PERIOD, adjust=False).mean()

    # 3. Basic Bands
    df['hl2'] = (df['HA_High'] + df['HA_Low']) / 2
    df['upper_basic'] = df['hl2'] + (ST_MULTIPLIER * df['ATR'])
    df['lower_basic'] = df['hl2'] - (ST_MULTIPLIER * df['ATR'])

    # 4. Final Bands + Direction (loop required)
    final_upper = [0.0] * len(df)
    final_lower = [0.0] * len(df)
    direction = [1] * len(df)
    flip_level = [np.nan] * len(df)  # <-- NEW: store flip SL

    first_valid = df['upper_basic'].first_valid_index()
    if first_valid is None:
        df['st_dir'] = direction
        df['st_upper'] = final_upper
        df['st_lower'] = final_lower
        df['flip_level'] = flip_level
        return df

    final_upper[first_valid] = df['upper_basic'].iloc[first_valid]
    final_lower[first_valid] = df['lower_basic'].iloc[first_valid]

    for i in range(first_valid + 1, len(df)):
        # Upper band locking
        if df['upper_basic'].iloc[i] < final_upper[i-1] or df['HA_Close'].iloc[i-1] > final_upper[i-1]:
            final_upper[i] = df['upper_basic'].iloc[i]
        else:
            final_upper[i] = final_upper[i-1]

        # Lower band locking
        if df['lower_basic'].iloc[i] > final_lower[i-1] or df['HA_Close'].iloc[i-1] < final_lower[i-1]:
            final_lower[i] = df['lower_basic'].iloc[i]
        else:
            final_lower[i] = final_lower[i-1]

        # Direction flip with flip_level tracking
        if direction[i-1] == 1:
            if df['HA_Close'].iloc[i] < final_lower[i]:
                direction[i] = -1
                flip_level[i] = final_upper[i-1]   # 🔥 THIS IS THE FIX
            else:
                direction[i] = 1        
        else:
            if df['HA_Close'].iloc[i] > final_upper[i]:
                direction[i] = 1
                flip_level[i] = final_upper[i]
            else:
                direction[i] = -1

    df['st_dir'] = direction
    df['st_upper'] = final_upper
    df['st_lower'] = final_lower
    df['flip_level'] = flip_level  # <-- NEW column

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
        response = requests.get(url, params=params, timeout=10)
        r = response.json()

        if not isinstance(r, dict) or "data" not in r or not r["data"]:
            print(f"⚠️ No data for {pair}: {r}")
            return None

        df = pd.DataFrame(r["data"]).sort_values("time").reset_index(drop=True)

        for col in ["open", "high", "low", "close"]:
            if col not in df.columns:
                print(f"⚠️ Missing column '{col}' for {pair}. Got: {df.columns.tolist()}")
                return None
            df[col] = pd.to_numeric(df[col], errors='coerce')

        if len(df) < 52:
            return None
        df = df.iloc[:-1]

        return calculate_indicators(df)

    except Exception as e:
        print(f"❌ fetch_candles error [{pair}]: {e}")
        return None

# ================= CORE SIGNAL LOGIC =================
def process_logic(pair, watch_list):
    df = fetch_candles(pair)
    if df is None or df.empty: return None

    last = df.iloc[-1]
    prev = df.iloc[-2]

    last_dir = int(last['st_dir'])
    prev_dir = int(prev['st_dir'])

    if pair in watch_list:
        st_val = last['st_lower'] if last_dir == 1 else last['st_upper']
        trend_label = "🟢 GREEN" if last_dir == 1 else "🔴 RED"

    # Logic 1: Add to watchlist if last closed HA candle ST is GREEN
    if pair not in watch_list:
        if last_dir == 1:
            return {"type": "ADD", "pair": pair}
        return None

    # Logic 2: Alert when ST flips GREEN → RED
    if prev_dir == 1 and last_dir == -1:
        entry = last["HA_Close"]
        sl = last["flip_level"] if not np.isnan(last["flip_level"]) else prev["st_lower"]  # <-- FIXED
        risk = sl - entry

        if risk <= 0:
            return {"type": "KEEP", "pair": pair}

        qty = RISK_PER_TRADE / risk
        margin = (qty * entry) / LEVERAGE

        return {
            "type": "SIGNAL", "pair": pair,
            "entry": entry, "sl": sl,
            "margin": margin,
            "t2": entry - (risk * 2),
            "t3": entry - (risk * 3)
        }

    # Logic 3: Keep in watchlist while ST stays GREEN
    if last_dir == 1:
        return {"type": "KEEP", "pair": pair}

    # ST already RED — remove silently
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
            d = requests.get(
                f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={p}",
                timeout=5
            ).json()
            pc = d.get("price_change_percent", {}).get("1D", 0)
            return {"pair": p, "change": float(pc)}
        except:
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        stats = [r for r in ex.map(get_stats, all_pairs) if r]

    top_gainers = [x["pair"] for x in sorted(stats, key=lambda x: x["change"], reverse=True)[:5]]
    scan_pool = list(set(watch_list + top_gainers))

    alerts, new_watchlist, signaled_pairs = [], [], []

    print(f"\n--- Scanning {len(scan_pool)} pairs (Watchlist: {len(watch_list)}) ---\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = {executor.submit(process_logic, p, watch_list): p for p in scan_pool}
        for future in as_completed(tasks):
            pair = tasks[future]
            try:
                res = future.result()
            except Exception as e:
                print(f"❌ process_logic error [{pair}]: {e}")
                continue

            if not res:
                continue

            if res["type"] == "SIGNAL":
                msg = (
                    f"🔴 **HA SUPERTREND REVERSAL**: {res['pair']}\n"
                    f"Entry: {res['entry']:.6f}\n"
                    f"Stop Loss: {res['sl']:.6f}\n"
                    f"Capital: ₹{res['margin']:.2f}\n"
                    f"🎯 T2: {res['t2']:.6f}"
                )
                alerts.append(msg)
                signaled_pairs.append(res['pair'])
            elif res["type"] in ["KEEP", "ADD"]:
                new_watchlist.append(res["pair"])

    final_watchlist = sorted(list(set([p for p in new_watchlist if p not in signaled_pairs])))
    with open(FILE_NAME, "w") as f:
        json.dump(final_watchlist, f, indent=2)

    if alerts:
        for alert_msg in alerts:
            Send_Momentum_Telegram_Message(alert_msg)

    print(f"\nScan complete. Signals: {len(alerts)} | Watchlist updated: {len(final_watchlist)}")

if __name__ == "__main__":
    main()
