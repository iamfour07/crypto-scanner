import pandas as pd

def calculate_cci(high, low, close, period=200, constant=0.015):
    """
    Calculate Commodity Channel Index (CCI).
    """
    high = pd.Series(high)
    low = pd.Series(low)
    close = pd.Series(close)

    # Typical Price
    tp = (high + low + close) / 3

    # Simple Moving Average
    sma = tp.rolling(window=period).mean()

    # Mean Absolute Deviation
    mad = tp.rolling(window=period).apply(
        lambda x: (x - x.mean()).abs().mean(),
        raw=False
    )

    # CCI formula
    cci = (tp - sma) / (constant * mad)

    # Last value (with sign preserved)
    last_cci = cci.iloc[-1]

    # Ensure it's float with correct sign
    return float(round(last_cci, 2))
