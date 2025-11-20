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
ENABLE_RSI = False   # <<<<<<<<<< ENABLE / DISABLE RSI

SymbolList = ["ETHUSD", "SOLUSDT"]

# =========================
# INDICATOR: RSI
# =========================
def calculate_rsi(close_prices, period=21):
    delta = close_prices.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# =========================
# FETCH & PROCESS ONE SYMBOL
# =========================
def fetch_symbol_data(symbol):
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - 24 * 3600 * 7  # 7 days

    params = {
        "symbol": symbol,
        "resolution": "1m",
        "start": from_time,
        "end": now
    }

    try:
        resp = requests.get(f"{BASE_URL}/v2/history/candles", params=params, timeout=20)
        resp.raise_for_status()
        result = resp.json().get("result", [])
    except:
        return None

    if not result:
        return None

    df = pd.DataFrame(result)

    required_cols = {'open', 'high', 'low', 'close', 'volume'}
    if not required_cols.issubset(df.columns):
        return None

    for col in required_cols:
        df[col] = df[col].astype(float)

    if len(df) < 50:
        return None

    # Compute indicators
    df[f'ema{EMA1}'] = df['close'].ewm(span=EMA1, adjust=False).mean()
    df[f'ema{EMA2}'] = df['close'].ewm(span=EMA2, adjust=False).mean()
    df['rsi'] = calculate_rsi(df['close'], RSI_PERIOD)

    last = df.iloc[-2]
    prev = df.iloc[-3]

    # ============================
    # CROSSOVER ONLY (NO PULLBACK)
    # ============================
    bullish_cross = prev[f'ema{EMA1}'] < prev[f'ema{EMA2}'] and last[f'ema{EMA1}'] > last[f'ema{EMA2}']
    bearish_cross = prev[f'ema{EMA1}'] > prev[f'ema{EMA2}'] and last[f'ema{EMA1}'] < last[f'ema{EMA2}']

    # RSI ENABLE/DISABLE FILTER
    rsi_ok = True
    if ENABLE_RSI:
        rsi_ok = last['rsi'] >= RSI_THRESHOLD

    return {
        "symbol": symbol,
        "close": last['close'],
        "rsi": round(last['rsi'], 1),
        "volume": last['volume'],
        "bullish": bullish_cross and rsi_ok,
        "bearish": bearish_cross and rsi_ok
    }

# =========================
# MAIN SCANNER
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

    bullish = sorted(bullish, key=lambda x: x['volume'], reverse=True)
    bearish = sorted(bearish, key=lambda x: x['volume'], reverse=True)

    if bullish or bearish:
        message_lines = ["📊 EMA 4 / EMA 21 CROSSOVER"]

        if bullish:
            message_lines.append("\n🟢 Bullish Crossovers:\n")
            for res in bullish:
                message_lines.append(
                    f"{res['symbol']}\nClose: {res['close']}\nRSI: {res['rsi']}\nVolume: {res['volume']}\n"
                )

        if bearish:
            message_lines.append("\n🔴 Bearish Crossovers:\n")
            for res in bearish:
                message_lines.append(
                    f"{res['symbol']}\nClose: {res['close']}\nRSI: {res['rsi']}\nVolume: {res['volume']}\n"
                )
        Telegram_Alert_EMA_Crossover(message_lines)
        # print("\n".join(message_lines))

if __name__ == "__main__":
    main()
