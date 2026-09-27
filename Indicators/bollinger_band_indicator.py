"""
Bollinger Bands — reusable indicator.

Usage
-----
    from Indicators.bollinger_band_indicator import calculate_bollinger

    df = calculate_bollinger(df)                          # BB(20, 2) -> BB_mid / BB_upper / BB_lower
    df = calculate_bollinger(df, length=100, mult=3)      # BB(100, 3)
    df = calculate_bollinger(df, prefix="BB100")          # BB100_mid / BB100_upper / BB100_lower
    df = calculate_bollinger(df, source="HA_Close")       # bands on any column
    df = calculate_bollinger(df, ma_type="ema")           # EMA basis instead of SMA

Output columns (with default prefix "BB"):
    BB_mid, BB_upper, BB_lower, BB_width (% of mid), BB_percent_b (0 = lower, 1 = upper)
"""

import pandas as pd


def bollinger_bands(source, length=20, mult=2.0, ma_type="sma", ddof=0):
    """
    Returns (mid, upper, lower) Series for any price Series.

    ma_type : "sma" (default, standard) or "ema" for the middle band
    ddof    : 0 = population std (same as TradingView), 1 = sample std (pandas default)
    """
    source = pd.Series(source).astype(float)

    if ma_type == "sma":
        mid = source.rolling(length).mean()
    elif ma_type == "ema":
        mid = source.ewm(span=length, min_periods=length, adjust=False).mean()
    else:
        raise ValueError(f"Unknown ma_type: {ma_type!r} (use 'sma' or 'ema')")

    std = source.rolling(length).std(ddof=ddof)
    upper = mid + mult * std
    lower = mid - mult * std
    return mid, upper, lower


def calculate_bollinger(df, length=20, mult=2.0, source="close", prefix="BB",
                        ma_type="sma", ddof=0):
    """
    Adds Bollinger Band columns to a DataFrame and returns the DataFrame.

    length  : lookback period
    mult    : standard-deviation multiplier
    source  : column to calculate on (default "close")
    prefix  : output column prefix (default "BB")
    ma_type : "sma" or "ema" middle band
    ddof    : 0 = TradingView-style std, 1 = sample std
    """
    mid, upper, lower = bollinger_bands(df[source], length=length, mult=mult,
                                        ma_type=ma_type, ddof=ddof)
    df[f"{prefix}_mid"] = mid
    df[f"{prefix}_upper"] = upper
    df[f"{prefix}_lower"] = lower
    df[f"{prefix}_width"] = (upper - lower) / mid * 100
    df[f"{prefix}_percent_b"] = (df[source] - lower) / (upper - lower)
    return df
