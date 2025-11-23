import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Alert_EMA_Crossover import Telegram_Alert_EMA_Crossover

# =========================
# CONFIGURATION
# =========================
BASE_URL = "https://api.india.delta.exchange"
CANDLES_ENDPOINT = f"{BASE_URL}/v2/history/candles"

EMA1 = 9
EMA2 = 100

RSI_PERIOD = 21
RSI_THRESHOLD = 51
ENABLE_RSI = False

SymbolList = ["ETHUSD", "SOLUSD"]

# =========================
# RSI
# =========================
def calculate_rsi(close_prices, period=21):
    delta = close_prices.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# =========================
# FETCH SYMBOL DATA
# =========================
def fetch_symbol_data(symbol):
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - 24 * 3600 * 7

    params = {
        "symbol": symbol,
        "resolution": "15m",
        "start": str(from_time),   # <<< FIX 1
        "end": str(now)            # <<< FIX 2
    }

    try:
        resp = requests.get(
            f"{BASE_URL}/v2/history/candles",
            params=params,
            timeout=20
        )
        resp.raise_for_status()
        result = resp.json().get("result", [])
    except Exception as e:
        print(f"API ERROR for {symbol}: {e}")
        return None

    if not result:
        print(f"No candles for {symbol}")
        return None

    df = pd.DataFrame(result)

    # <<< FIX 3 - Sort by timestamp >>>
    df = df.sort_values("time").reset_index(drop=True)

    needed_cols = ['open', 'high', 'low', 'close', 'volume']
    for col in needed_cols:
        df[col] = df[col].astype(float)

    if len(df) < 50:
        return None

    # EMAs + RSI
    df[f'ema{EMA1}'] = df['close'].ewm(span=EMA1, adjust=False).mean()
    df[f'ema{EMA2}'] = df['close'].ewm(span=EMA2, adjust=False).mean()
    df['rsi'] = calculate_rsi(df['close'], RSI_PERIOD)

    # Use last fully closed candle
    last = df.iloc[-2]
    prev = df.iloc[-3]

    # CROSSOVER LOGIC
    bullish_cross = prev[f'ema{EMA1}'] < prev[f'ema{EMA2}'] and last[f'ema{EMA1}'] > last[f'ema{EMA2}']
    bearish_cross = prev[f'ema{EMA1}'] > prev[f'ema{EMA2}'] and last[f'ema{EMA1}'] < last[f'ema{EMA2}']

    rsi_ok = True
    if ENABLE_RSI:
        rsi_ok = last['rsi'] >= RSI_THRESHOLD

    return {
        "symbol": symbol,
        "close": last['close'],

        # Debug EMAs
        "ema1": last[f'ema{EMA1}'],
        "ema2": last[f'ema{EMA2}'],
        "prev_ema1": prev[f'ema{EMA1}'],
        "prev_ema2": prev[f'ema{EMA2}'],

        "rsi": round(last['rsi'], 2),
        "volume": last['volume'],

        # Signals
        "bullish": bullish_cross and rsi_ok,
        "bearish": bearish_cross and rsi_ok,

        # Raw values
        "bull_cross_raw": bullish_cross,
        "bear_cross_raw": bearish_cross,
    }

# =========================
# MAIN
# =========================
def main():
    bullish, bearish = [], []

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_symbol_data, sym): sym for sym in SymbolList}

        for fut in as_completed(futures):
            data = fut.result()
            if not data:
                continue
            
            if data['bullish']:
                bullish.append(data)
            elif data['bearish']:
                bearish.append(data)

    if bullish or bearish:
        message = ["📊 EMA 9 / EMA 100 CROSSOVER"]

        if bullish:
            message.append("\n🟢 Bullish Cross:")
            for s in bullish:
                message.append(
                    f"{s['symbol']} — Close: {s['close']} — RSI: {s['rsi']} — Vol: {s['volume']}"
                )

        if bearish:
            message.append("\n🔴 Bearish Cross:")
            for s in bearish:
                message.append(
                    f"{s['symbol']} — Close: {s['close']} — RSI: {s['rsi']} — Vol: {s['volume']}"
                )

        Telegram_Alert_EMA_Crossover("\n".join(message))

if __name__ == "__main__":
    main()
