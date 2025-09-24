import requests
import pandas as pd
from datetime import datetime, timezone

# Config
RESOLUTION = "60"
LIMIT_HOURS = 1000

def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error fetching coins: {e}")
        return []

def fetch_coin_data(pair):
    """
    Fetch OHLCV data for one coin
    """
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - LIMIT_HOURS * 3600
    url = "https://public.coindcx.com/market_data/candlesticks"
    params = {"pair": pair, "from": from_time, "to": now, "resolution": RESOLUTION, "pcode": "f"}

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            return None

        df = pd.DataFrame(data)
        df["close"] = df["close"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        return df

    except Exception as e:
        print(f"Error fetching data for {pair}: {e}")
        return None
