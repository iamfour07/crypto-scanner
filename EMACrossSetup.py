import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import html
from Telegram_Alert_EMA_Crossover import Telegram_Alert_EMA_Crossover

# =========================
# CONFIGURATION
# =========================
resolution = "1h"
limit_hours = 1000
IST = timezone(timedelta(hours=5, minutes=30))

WMA_PERIOD = 5

CoinList = ['SOLUSD','ETHUSD']

# =========================
# WMA CALCULATION
# =========================
def calculate_wma(series, period):
    weights = list(range(1, period + 1))
    return series.rolling(period).apply(
        lambda prices: sum(prices * weights) / sum(weights),
        raw=True
    )

# =========================
# FETCH & PROCESS ONE COIN
# =========================
def fetch_coin_data(symbol):
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - limit_hours * 3600

    url = "https://api.india.delta.exchange/v2/history/candles"
    headers = {"Accept": "application/json"}
    params = {
        "resolution": resolution,
        "symbol": symbol,
        "start": str(from_time),
        "end": str(now)
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        print(f"⚠️ {symbol}: Delta API request failed: {e}")
        return None

    candles = payload.get("result", [])
    if not candles or all(c['close'] == 0 for c in candles):
        print(f"⚠️ {symbol}: No usable candles returned by Delta")
        return None

    df = pd.DataFrame(candles)

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("time").reset_index(drop=True)

    # WMA
    df['wma5'] = calculate_wma(df['close'], WMA_PERIOD)

    # Use previous closed candle (-2)
    if len(df) < WMA_PERIOD + 2:
        return None

    signal_candle = df.iloc[-2]
    wma_value = df.iloc[-2]['wma5']
    # =========================
    # DEBUG OUTPUT
    # =========================
    candle_time = datetime.fromtimestamp(signal_candle['time'], tz=timezone.utc).astimezone(IST)

    print(f"\n----- DEBUG {symbol} -----")
    print(f"Candle Time (IST): {candle_time}")
    print(f"Close: {signal_candle['close']}")
    print(f"High:  {signal_candle['high']}")
    print(f"Low:   {signal_candle['low']}")
    print(f"WMA5:  {round(wma_value, 4) if pd.notnull(wma_value) else None}")
    print(f"Gap Above High (Buy Condition): {wma_value > signal_candle['high'] if pd.notnull(wma_value) else None}")
    print(f"Gap Below Low (Sell Condition): {wma_value < signal_candle['low'] if pd.notnull(wma_value) else None}")
    print("----------------------------")

    bullish = False
    bearish = False
    entry = None
    sl = None

    if pd.notnull(wma_value):

        # =========================
        # BUY SETUP
        # WMA above candle high (gap exists)
        # =========================
        if wma_value > signal_candle['high']:
            bullish = True
            entry = float(signal_candle['high'])
            sl = float(signal_candle['low'])

        # =========================
        # SELL SETUP
        # WMA below candle low (gap exists)
        # =========================
        elif wma_value < signal_candle['low']:
            bearish = True
            entry = float(signal_candle['low'])
            sl = float(signal_candle['high'])

    return {
        "pair": symbol,
        "close": float(signal_candle['close']),
        "volume": float(signal_candle['volume']),
        "bullish": bullish,
        "bearish": bearish,
        "entry": entry,
        "sl": sl
    }

# =========================
# MAIN SCANNER
# =========================
def main():
    bullish, bearish = [], []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_coin_data, coin): coin for coin in CoinList}
        for future in as_completed(futures):
            try:
                data = future.result()
                if not data:
                    continue
                if data['bullish']:
                    bullish.append(data)
                elif data['bearish']:
                    bearish.append(data)
            except Exception as e:
                print(f"Error processing coin: {e}")

    bullish = sorted(bullish, key=lambda x: x['volume'], reverse=True)
    bearish = sorted(bearish, key=lambda x: x['volume'], reverse=True)

    if bullish or bearish:
        message_lines = [f"📊 WMA(5) Gap Setup\n"]

        if bullish:
            message_lines.append("🟢 Buy Setup (WMA above candle, no touch):\n")
            for res in bullish:
                pair_safe = html.escape(res['pair'])
                link = f"https://coindcx.com/futures/{res['pair']}"
                message_lines.append(
                    f"{pair_safe}\nClose: {res['close']}\nEntry: {res['entry']}\nSL: {res['sl']}\nVolume: {res['volume']}\n{link}\n"
                )

        if bearish:
            message_lines.append("\n🔴 Sell Setup (WMA below candle, no touch):\n")
            for res in bearish:
                pair_safe = html.escape(res['pair'])
                link = f"https://coindcx.com/futures/{res['pair']}"
                message_lines.append(
                    f"{pair_safe}\nClose: {res['close']}\nEntry: {res['entry']}\nSL: {res['sl']}\nVolume: {res['volume']}\n{link}\n"
                )

        message_lines.append("\n===============================")
        final_message = "\n".join(message_lines)

        Telegram_Alert_EMA_Crossover(final_message)

if __name__ == "__main__":
    main()