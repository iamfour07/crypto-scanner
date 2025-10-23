# ADX_Calculater.py
import pandas as pd
import numpy as np

def calculate_adx(high, low, close, period=14):
    """
    Calculate ADX (Average Directional Index) using Wilder's smoothing.
    Returns a pd.Series aligned with the input index.
    """
    high = pd.Series(high).astype(float)
    low = pd.Series(low).astype(float)
    close = pd.Series(close).astype(float)

    df = pd.DataFrame({"high": high, "low": low, "close": close})

    # True Range (TR)
    df["prev_close"] = df["close"].shift(1)
    df["tr1"] = df["high"] - df["low"]
    df["tr2"] = (df["high"] - df["prev_close"]).abs()
    df["tr3"] = (df["low"] - df["prev_close"]).abs()
    df["TR"] = df[["tr1", "tr2", "tr3"]].max(axis=1)

    # Directional Movements
    df["up_move"] = df["high"] - df["high"].shift(1)
    df["down_move"] = df["low"].shift(1) - df["low"]

    df["plus_dm"] = np.where((df["up_move"] > df["down_move"]) & (df["up_move"] > 0),
                             df["up_move"], 0.0)
    df["minus_dm"] = np.where((df["down_move"] > df["up_move"]) & (df["down_move"] > 0),
                              df["down_move"], 0.0)

    # Wilder smoothing with EWM (alpha = 1/period, adjust=False)
    tr_smooth = df["TR"].ewm(alpha=1/period, adjust=False).mean()
    plus_dm_smooth = df["plus_dm"].ewm(alpha=1/period, adjust=False).mean()
    minus_dm_smooth = df["minus_dm"].ewm(alpha=1/period, adjust=False).mean()

    # Avoid division by zero
    plus_di = 100 * (plus_dm_smooth / tr_smooth).replace([np.inf, -np.inf], np.nan)
    minus_di = 100 * (minus_dm_smooth / tr_smooth).replace([np.inf, -np.inf], np.nan)

    dx = ( (plus_di - minus_di).abs() / (plus_di + minus_di) ) * 100
    adx = dx.ewm(alpha=1/period, adjust=False).mean()

    # Return ADX series
    adx.name = "ADX"
    return adx
