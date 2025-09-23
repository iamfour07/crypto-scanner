import pandas as pd

# ==============================
# SUPERTREND CALCULATION
# ==============================
def supertrend(df, period=10, multiplier=3):
    """
    Calculate Supertrend indicator for a given OHLC DataFrame.

    Parameters:
        df : pd.DataFrame
            DataFrame must have 'high', 'low', 'close' columns.
        period : int
            ATR period for Supertrend.
        multiplier : float
            Factor to calculate upper and lower bands.

    Returns:
        df : pd.DataFrame
            Original DataFrame with new columns:
            'UpperBand', 'LowerBand', 'Trend' ('BUY'/'SELL')
    """
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
