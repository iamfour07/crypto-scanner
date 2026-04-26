import json, requests, pandas as pd, os
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= INDICATOR CALCULATIONS =================
def calculate_indicators(df):
    # 1. ATR using Normal Candles
    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - df['close'].shift(1)).abs()
    tr3 = (df['low'] - df['close'].shift(1)).abs()
    df['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['ATR'] = df['TR'].ewm(alpha=1/ST_PERIOD, min_periods=ST_PERIOD, adjust=False).mean()

    # 2. Basic Bands
    df['hl2'] = (df['high'] + df['low']) / 2
    df['upper_basic'] = df['hl2'] + (ST_MULTIPLIER * df['ATR'])
    df['lower_basic'] = df['hl2'] - (ST_MULTIPLIER * df['ATR'])

    # 3. Final Bands + Direction
    final_upper = [0.0] * len(df)
    final_lower = [0.0] * len(df)
    direction = [1] * len(df)
    flip_level = [np.nan] * len(df) 

    first_valid = df['upper_basic'].first_valid_index()
    if first_valid is None:
        return df

    final_upper[first_valid] = df['upper_basic'].iloc[first_valid]
    final_lower[first_valid] = df['lower_basic'].iloc[first_valid]

    for i in range(first_valid + 1, len(df)):
        # Upper band locking logic
        if df['upper_basic'].iloc[i] < final_upper[i-1] or df['close'].iloc[i-1] > final_upper[i-1]:
            final_upper[i] = df['upper_basic'].iloc[i]
        else:
            final_upper[i] = final_upper[i-1]

        # Lower band locking logic
        if df['lower_basic'].iloc[i] > final_lower[i-1] or df['close'].iloc[i-1] < final_lower[i-1]:
            final_lower[i] = df['lower_basic'].iloc[i]
        else:
            final_lower[i] = final_lower[i-1]

        # Direction flip logic: Triggered by CLOSE price
        if direction[i-1] == 1:
            if df['close'].iloc[i] < final_lower[i]: # Trend turns RED on CLOSE
                direction[i] = -1
                flip_level[i] = final_upper[i-1]   # SL set to the upper level at flip
            else:
                direction[i] = 1        
        else:
            if df['close'].iloc[i] > final_upper[i]: # Trend turns GREEN on CLOSE
                direction[i] = 1
                flip_level[i] = final_lower[i-1]
            else:
                direction[i] = -1

    df['st_dir'] = direction
    df['st_upper'] = final_upper
    df['st_lower'] = final_lower
    df['flip_level'] = flip_level 

    return df

# ================= CORE SIGNAL LOGIC =================
def process_logic(pair, watch_list):
    df = fetch_candles(pair)
    if df is None or df.empty: return None

    last = df.iloc[-1]   # This is the candle that just CLOSED
    prev = df.iloc[-2]   # This was the candle before it

    last_dir = int(last['st_dir'])
    prev_dir = int(prev['st_dir'])

    # Logic: Only alert if previous was GREEN and current closed RED
    if prev_dir == 1 and last_dir == -1:
        # Entry is the LOW of the candle that caused the flip
        entry = last["low"]  
        sl = last["flip_level"] 
        
        risk = sl - entry
        if risk <= 0: return None

        qty = RISK_PER_TRADE / risk
        margin = (qty * entry) / LEVERAGE

        return {
            "type": "SIGNAL", "pair": pair,
            "entry": entry, "sl": sl,
            "margin": margin,
            "t2": entry - (risk * 2),
            "t3": entry - (risk * 3)
        }

    # Watchlist management (Keep if trend is Green)
    if pair not in watch_list and last_dir == 1:
        return {"type": "ADD", "pair": pair}
    
    if pair in watch_list and last_dir == 1:
        return {"type": "KEEP", "pair": pair}

    return None
