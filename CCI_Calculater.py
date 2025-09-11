import pandas as pd

def calculate_cci(high, low, close, period=20):
    tp = (high + low + close) / 3
    sma = tp.rolling(period).mean()
    mean_dev = tp.rolling(period).apply(lambda x: (x - x.mean()).abs().mean(), raw=False)  # raw=False!
    cci = (tp - sma) / (0.015 * mean_dev)
    return cci.round(2)
