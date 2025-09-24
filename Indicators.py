import pandas as pd
import numpy as np

# ========== Bollinger Bands ==========
def calculate_bollinger(close, period=20, std=3):
    ma = close.rolling(window=period).mean()
    stddev = close.rolling(window=period).std(ddof=1)
    upper_band = ma + stddev * std
    lower_band = ma - stddev * std
    return upper_band, lower_band

# ========== RSI ==========
def calculate_rsi(close, period=28):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# ========== Supertrend ==========
def supertrend(df, period=10, multiplier=3):
    df = df.copy()

    df["H-L"] = df["high"] - df["low"]
    df["H-C"] = abs(df["high"] - df["close"].shift())
    df["L-C"] = abs(df["low"] - df["close"].shift())
    df["TR"] = df[["H-L", "H-C", "L-C"]].max(axis=1)
    df["ATR"] = df["TR"].rolling(period).mean()

    hl2 = (df["high"] + df["low"]) / 2
    df["UpperBand"] = hl2 + (multiplier * df["ATR"])
    df["LowerBand"] = hl2 - (multiplier * df["ATR"])

    df["Final_UpperBand"] = df["UpperBand"]
    df["Final_LowerBand"] = df["LowerBand"]
    df["Supertrend"] = np.nan
    df["Trend"] = None

    for i in range(period, len(df)):
        # Final upper band
        if (df["UpperBand"].iloc[i] < df["Final_UpperBand"].iloc[i-1]) or (df["close"].iloc[i-1] > df["Final_UpperBand"].iloc[i-1]):
            df.at[df.index[i], "Final_UpperBand"] = df["UpperBand"].iloc[i]
        else:
            df.at[df.index[i], "Final_UpperBand"] = df["Final_UpperBand"].iloc[i-1]

        # Final lower band
        if (df["LowerBand"].iloc[i] > df["Final_LowerBand"].iloc[i-1]) or (df["close"].iloc[i-1] < df["Final_LowerBand"].iloc[i-1]):
            df.at[df.index[i], "Final_LowerBand"] = df["LowerBand"].iloc[i]
        else:
            df.at[df.index[i], "Final_LowerBand"] = df["Final_LowerBand"].iloc[i-1]

        # Trend decision
        if df["close"].iloc[i] > df["Final_UpperBand"].iloc[i-1]:
            df.at[df.index[i], "Supertrend"] = df["Final_LowerBand"].iloc[i]
            df.at[df.index[i], "Trend"] = "BUY"
        elif df["close"].iloc[i] < df["Final_LowerBand"].iloc[i-1]:
            df.at[df.index[i], "Supertrend"] = df["Final_UpperBand"].iloc[i]
            df.at[df.index[i], "Trend"] = "SELL"
        else:
            df.at[df.index[i], "Supertrend"] = df["Supertrend"].iloc[i-1]
            df.at[df.index[i], "Trend"] = df["Trend"].iloc[i-1]

    return df
