"""
RSI (Relative Strength Index) — reusable indicator.

Usage
-----
    from Indicators.RSI_indicator import calculate_rsi

    df = calculate_rsi(df)                              # RSI(14) on close -> df["RSI"]
    df = calculate_rsi(df, length=28)                   # RSI(28)          -> df["RSI"]
    df = calculate_rsi(df, length=7, column="RSI_7")    # custom column name
    df = calculate_rsi(df, source="HA_Close")           # RSI on any column
    df = calculate_rsi(df, ma_type="sma", ma_length=14) # + TradingView smoothing line -> df["RSI_MA"]

    rsi = rsi_series(df["close"], length=14)            # just the Series
"""

import pandas as pd


def rma(series, length):
    """
    Wilder's moving average, seeded with the SMA of the first `length`
    values — identical to TradingView's ta.rma().
    """
    series = pd.Series(series).astype(float)
    values = series.to_numpy()
    out = [float("nan")] * len(values)

    valid = series.dropna()
    if len(valid) < length:
        return pd.Series(out, index=series.index)

    start = series.index.get_loc(valid.index[length - 1])
    prev = valid.iloc[:length].mean()
    out[start] = prev
    for i in range(start + 1, len(values)):
        prev = (prev * (length - 1) + values[i]) / length
        out[i] = prev
    return pd.Series(out, index=series.index)


def rsi_series(source, length=14, method="wilder"):
    """
    Returns RSI as a pd.Series for any price Series.

    method:
        "wilder" -> Wilder's RMA smoothing (same as TradingView ta.rsi)
        "ema"    -> standard EMA smoothing (span=length)
        "sma"    -> simple rolling average (Cutler's RSI)
    """
    source = pd.Series(source).astype(float)
    delta = source.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    if method == "wilder":
        avg_gain = rma(gain, length)
        avg_loss = rma(loss, length)
    elif method == "ema":
        avg_gain = gain.ewm(span=length, min_periods=length, adjust=False).mean()
        avg_loss = loss.ewm(span=length, min_periods=length, adjust=False).mean()
    elif method == "sma":
        avg_gain = gain.rolling(length).mean()
        avg_loss = loss.rolling(length).mean()
    else:
        raise ValueError(f"Unknown RSI method: {method!r} (use 'wilder', 'ema' or 'sma')")

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    # avg_loss == 0 -> only gains -> RSI 100 ; both zero -> flat -> RSI 50
    rsi = rsi.where(avg_loss != 0, 100.0)
    rsi = rsi.where(~((avg_gain == 0) & (avg_loss == 0)), 50.0)
    rsi[avg_gain.isna() | avg_loss.isna()] = float("nan")

    return rsi


def calculate_rsi(df, length=14, source="close", column="RSI", method="wilder",
                  ma_type=None, ma_length=14):
    """
    Adds an RSI column to a DataFrame and returns the DataFrame.

    length    : RSI period
    source    : column to calculate RSI on (default "close")
    column    : name of the output column (default "RSI")
    method    : "wilder" (default), "ema" or "sma"
    ma_type   : optional smoothing line on the RSI, like TradingView's
                "Smoothing Line" — "sma" or "ema". Adds "<column>_MA".
    ma_length : smoothing line length (default 14)
    """
    df[column] = rsi_series(df[source], length=length, method=method)

    if ma_type == "sma":
        df[f"{column}_MA"] = df[column].rolling(ma_length).mean()
    elif ma_type == "ema":
        df[f"{column}_MA"] = df[column].ewm(span=ma_length, min_periods=ma_length, adjust=False).mean()
    elif ma_type is not None:
        raise ValueError(f"Unknown ma_type: {ma_type!r} (use 'sma', 'ema' or None)")

    return df
