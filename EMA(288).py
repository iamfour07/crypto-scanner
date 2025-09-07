import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from CoinListNew import CoinList
from CCI_Calculater import calculate_cci

# --- Configuration ---
resolution = "60"
limit_hours = 1000

# --- Condition Flags ---
ENABLE_EMA288 = False
ENABLE_CCI200 = False
ENABLE_RSI14 = True

NEAR_EMA_THRESHOLD = 0.005
CCI_BUY = 98
CCI_SELL = -98
RSI_BUY = 62
RSI_SELL = 40
RSI_PERIOD = 21

IST = timezone(timedelta(hours=5, minutes=30))

# --- Helper Functions ---
def convert_to_ist_12h(timestamp_ms):
    timestamp_s = timestamp_ms / 1000
    dt_utc = datetime.fromtimestamp(timestamp_s, tz=timezone.utc)
    dt_ist = dt_utc.astimezone(IST)
    return dt_ist.strftime("%Y-%m-%d %I:%M:%S %p %Z")

def calculate_rsi(close_prices, period=21):
    """
    Calculates RSI using the standard exponential moving average method.
    close_prices: pandas Series
    """
    delta = close_prices.diff()

    # Separate gains and losses
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    # Use exponential weighted average for smoothing
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def fetch_and_check(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - limit_hours * 3600

    url = "https://public.coindcx.com/market_data/candlesticks"
    query_params = {
        "pair": pair,
        "from": from_time,
        "to": now,
        "resolution": resolution,
        "pcode": "f"
    }

    try:
        response = requests.get(url, params=query_params)
        response.raise_for_status()
        data = response.json()['data']

        df = pd.DataFrame(data)
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)

        if ENABLE_EMA288:
            df['ema_288'] = df['close'].ewm(span=288, adjust=False).mean()
        if ENABLE_RSI14:
            df['rsi'] = calculate_rsi(df['close'], RSI_PERIOD)

        last_candle = df.iloc[-2]
        close = last_candle['close']
        ema = df['ema_288'].iloc[-2] if ENABLE_EMA288 else None
        last_cci = calculate_cci(df['high'], df['low'], df['close'], period=200) if ENABLE_CCI200 else None
        last_rsi = last_candle['rsi'] if ENABLE_RSI14 else None

        signal_type = None

        # Buy Condition
        buy_condition = True
        if ENABLE_EMA288:
            buy_condition &= abs(close - ema)/ema <= NEAR_EMA_THRESHOLD
        if ENABLE_CCI200:
            buy_condition &= abs(last_cci - CCI_BUY) <= 2
        if ENABLE_RSI14:
            buy_condition &= abs(last_rsi - RSI_BUY) <= 2
        if buy_condition:
            signal_type = "BUY"

        # Sell Condition
        sell_condition = True
        if ENABLE_EMA288:
            sell_condition &= abs(close - ema)/ema <= NEAR_EMA_THRESHOLD
        if ENABLE_CCI200:
            sell_condition &= abs(last_cci - CCI_SELL) <= 2
        if ENABLE_RSI14:
            sell_condition &= abs(last_rsi - RSI_SELL) <= 2
        if sell_condition:
            signal_type = "SELL"

        if signal_type:
            return {
                "pair": pair,
                "close": close,
                "high": last_candle['high'],
                "low": last_candle['low'],
                "ema_288": round(ema, 6) if ENABLE_EMA288 else None,
                "cci_200": last_cci if ENABLE_CCI200 else None,
                "rsi": round(last_rsi, 2) if ENABLE_RSI14 else None,
                "signal": signal_type
            }

    except Exception as e:
        print(f"Error fetching {pair}: {e}")

    return None

# --- One-time Scan ---
buy_signals = []
sell_signals = []

with ThreadPoolExecutor(max_workers=10) as executor:
    future_to_coin = {executor.submit(fetch_and_check, coin): coin for coin in CoinList}
    for future in as_completed(future_to_coin):
        result = future.result()
        if result:
            if result['signal'] == "BUY":
                buy_signals.append(result)
            elif result['signal'] == "SELL":
                sell_signals.append(result)

# --- Print Results ---
print("\n================== SCAN RESULTS ==================")
if buy_signals:
    print("\n🟢 BUY Signals:")
    for res in buy_signals:
        print(f"{res['pair']} | Close: {res['close']} | EMA288: {res['ema_288']} | CCI200: {res['cci_200']} | RSI: {res['rsi']}")
else:
    print("\nNo BUY signals at this time.")

if sell_signals:
    print("\n🔴 SELL Signals:")
    for res in sell_signals:
        print(f"{res['pair']} | Close: {res['close']} | EMA288: {res['ema_288']} | CCI200: {res['cci_200']} | RSI: {res['rsi']}")
else:
    print("\nNo SELL signals at this time.")

print("=================================================")
