import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import html
from Telegram_Alert_EMA_Crossover import Telegram_Alert_EMA_Crossover

# =========================
# CONFIGURATION
# =========================
resolution = "60"   # 1-hour candles
limit_hours = 1000  # fetch 1000 hours history
IST = timezone(timedelta(hours=5, minutes=30))

# RSI settings
RSI_PERIOD = 21
RSI_THRESHOLD = 51  # custom condition

EMA1 = 4   # short EMA
EMA2 = 21  # long EMA

# =========================
# FETCH ACTIVE COINS
# =========================
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error fetching coins: {e}")
        return []

CoinList = get_active_usdt_coins()

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
# FETCH & PROCESS ONE COIN
# =========================
def fetch_coin_data(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - limit_hours * 3600

    url = "https://public.coindcx.com/market_data/candlesticks"
    params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()['data']

    df = pd.DataFrame(data)
    df['close'] = df['close'].astype(float)
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['volume'] = df['volume'].astype(float)

    # EMA calculations
    df[f'ema{EMA1}'] = df['close'].ewm(span=EMA1, adjust=False).mean()
    df[f'ema{EMA2}'] = df['close'].ewm(span=EMA2, adjust=False).mean()

    # RSI
    df['rsi'] = calculate_rsi(df['close'], RSI_PERIOD)

    # Use last CLOSED candle (not running one)
    last = df.iloc[-2]
    prev = df.iloc[-3]

    last_rsi = round(last['rsi'], 1)

    # Crossover logic
    bullish_cross = (
        prev[f'ema{EMA1}'] < prev[f'ema{EMA2}'] and
        last[f'ema{EMA1}'] > last[f'ema{EMA2}'] and
        last['rsi'] > RSI_THRESHOLD
    )
    bearish_cross = (
        prev[f'ema{EMA1}'] > prev[f'ema{EMA2}'] and
        last[f'ema{EMA1}'] < last[f'ema{EMA2}'] and
        last['rsi'] > RSI_THRESHOLD
    )

    entry = stoploss = None

    # ============ Post-Crossover Candle Logic ============
    if bullish_cross:
        # Index of the crossover candle (second-last)
        crossover_index = len(df) - 2
        next_two = df.iloc[crossover_index+1:crossover_index+3]

        for _, row in next_two.iterrows():
            # Red candle → close < open
            # ✅ New condition: low >= EMA21 (pullback above EMA21)
            if row['close'] < row['open'] and row['low'] >= row[f'ema{EMA2}']:
                entry = row['high']
                stoploss = row['low']
                break

    elif bearish_cross:
        crossover_index = len(df) - 2
        next_two = df.iloc[crossover_index+1:crossover_index+3]

        for _, row in next_two.iterrows():
            # Green candle → close > open
            # ✅ New condition: high <= EMA21 (pullback below EMA21)
            if row['close'] > row['open'] and row['high'] <= row[f'ema{EMA2}']:
                entry = row['low']
                stoploss = row['high']
                break

    return {
        "pair": pair,
        "close": last['close'],
        "rsi": last_rsi,
        "volume": last['volume'],
        "bullish_cross": bullish_cross,
        "bearish_cross": bearish_cross,
        "entry": entry,
        "stoploss": stoploss
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
                if data['bullish_cross'] and data['entry'] and data['stoploss']:
                    bullish.append(data)
                elif data['bearish_cross'] and data['entry'] and data['stoploss']:
                    bearish.append(data)
            except Exception as e:
                print(f"Error processing coin: {e}")

    bullish = sorted(bullish, key=lambda x: x['volume'], reverse=True)
    bearish = sorted(bearish, key=lambda x: x['volume'], reverse=True)

    # =========================
    # TELEGRAM MESSAGE
    # =========================
    if bullish or bearish:
        message_lines = [f"📊 21 EMA Crossover\n"]

        if bullish:
            message_lines.append("🟢 Bullish EMA5>EMA21 Cross + Pullback Entry (Low ≥ EMA21):\n")
            for res in bullish:
                pair_safe = html.escape(res['pair'])
                link = f"https://coindcx.com/futures/{res['pair']}"
                message_lines.append(
                    f"{pair_safe}\nClose: {res['close']}\nRSI: {res['rsi']}\n"
                    f"Entry: {res['entry']}\nStoploss: {res['stoploss']}\n"
                    f"Volume: {res['volume']}\n{link}\n"
                )

        if bearish:
            message_lines.append("\n🔴 Bearish EMA5<EMA21 Cross + Pullback Entry (High ≤ EMA21):\n")
            for res in bearish:
                pair_safe = html.escape(res['pair'])
                link = f"https://coindcx.com/futures/{res['pair']}"
                message_lines.append(
                    f"{pair_safe}\nClose: {res['close']}\nRSI: {res['rsi']}\n"
                    f"Entry: {res['entry']}\nStoploss: {res['stoploss']}\n"
                    f"Volume: {res['volume']}\n{link}\n"
                )

        message_lines.append("\n===============================")
        final_message = "\n".join(message_lines)
        # print(final_message)
        Telegram_Alert_EMA_Crossover(final_message)

if __name__ == "__main__":
    main()
