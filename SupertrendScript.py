import requests
import sys, os
import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..')))
from CoinList.CoinList import CoinList

# ==============================
# SUPERTREND CALCULATION
# ==============================
def supertrend(df, period=10, multiplier=3):
    df = df.copy()
    
    df['H-L'] = df['high'] - df['low']
    df['H-C'] = abs(df['high'] - df['close'].shift(1))
    df['L-C'] = abs(df['low'] - df['close'].shift(1))
    df['TR'] = df[['H-L', 'H-C', 'L-C']].max(axis=1)
    df['ATR'] = df['TR'].rolling(period).mean()

    hl2 = (df['high'] + df['low']) / 2
    df['UpperBand'] = hl2 + multiplier * df['ATR']
    df['LowerBand'] = hl2 - multiplier * df['ATR']

    df['Trend'] = None
    prev_trend = None

    for i in range(period, len(df)):
        if df['close'].iloc[i] > df['UpperBand'].iloc[i-1]:
            current_trend = 'BUY'
        elif df['close'].iloc[i] < df['LowerBand'].iloc[i-1]:
            current_trend = 'SELL'
        else:
            current_trend = prev_trend

        df.at[df.index[i], 'Trend'] = current_trend
        prev_trend = current_trend

    return df

# ==============================
# FETCH COINDCX DATA (1H)
# ==============================
def fetch_candles(symbol, resolution="120", hours_back=200):
    url = "https://public.coindcx.com/market_data/candlesticks"
    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(hours=hours_back)

    params = {
        "pair": symbol,
        "resolution": resolution,
        "from": int(start_utc.timestamp()),
        "to": int(now_utc.timestamp()),
        "pcode": "f"
    }

    try:
        r = requests.get(url, params=params)
        r.raise_for_status()
        data = r.json().get("data", [])
        if not data:
            return None
        df = pd.DataFrame(data)
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        return df
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

# ==============================
# PROCESS EACH COIN
# ==============================
def process_coin(coin):
    df = fetch_candles(coin, resolution="120", hours_back=200)
    if df is None or len(df) < 3:
        return

    df = supertrend(df)

    # Only check previous 2 fully closed candles (ignore running candle)
    trend_2nd_prev = df['Trend'].iloc[-3]  # 2nd previous fully closed candle
    trend_1st_prev = df['Trend'].iloc[-2]  # 1st previous fully closed candle

    if trend_2nd_prev != trend_1st_prev:
        print(f"{coin} → Trend flipped from {trend_2nd_prev} to {trend_1st_prev} at {df['time'].iloc[-2]} (Close={df['close'].iloc[-2]})")

# ==============================
# PARALLEL SCAN
# ==============================
if __name__ == "__main__":
    print("Starting 1H Supertrend Scanner...\n")
    with ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(process_coin, CoinList)
