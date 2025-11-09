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

# Stochastic settings
STOCH_PERIOD = 21
STOCH_K_SMOOTH = 5
STOCH_D_SMOOTH = 5

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
# INDICATOR: STOCHASTIC OSCILLATOR
# =========================
def calculate_stochastic(df, k_period=14, k_smooth=3, d_smooth=3):
    low_min = df['low'].rolling(window=k_period).min()
    high_max = df['high'].rolling(window=k_period).max()

    df['%K'] = 100 * (df['close'] - low_min) / (high_max - low_min)
    df['%K'] = df['%K'].rolling(window=k_smooth).mean()
    df['%D'] = df['%K'].rolling(window=d_smooth).mean()
    return df

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

    df = calculate_stochastic(df, STOCH_PERIOD, STOCH_K_SMOOTH, STOCH_D_SMOOTH)

    # Use last CLOSED candle (not running one)
    last = df.iloc[-2]
    prev = df.iloc[-3]

    last_k, last_d = last['%K'], last['%D']
    prev_k, prev_d = prev['%K'], prev['%D']

    # ✅ Latest candle crossover only
    buy_signal = (
        prev_k < prev_d and last_k > last_d and
        last_k < 20 and last_d < 20
    )

    sell_signal = (
        prev_k > prev_d and last_k < last_d and
        last_k > 80 and last_d > 80
    )

    # Skip if Stochastic not valid (NaN due to rolling window)
    if pd.isna(last_k) or pd.isna(last_d):
        return None

    return {
        "pair": pair,
        "close": last['close'],
        "%K": round(last_k, 2),
        "%D": round(last_d, 2),
        "volume": last['volume'],
        "buy_signal": buy_signal,
        "sell_signal": sell_signal,
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
                if data['buy_signal']:
                    bullish.append(data)
                elif data['sell_signal']:
                    bearish.append(data)
            except Exception as e:
                print(f"Error processing coin: {e}")

    bullish = sorted(bullish, key=lambda x: x['volume'], reverse=True)
    bearish = sorted(bearish, key=lambda x: x['volume'], reverse=True)

    # =========================
    # TELEGRAM MESSAGE
    # =========================
    if bullish or bearish:
        message_lines = [f"📊 Latest Stochastic Oscillator (14,3,3) Signal — {resolution}\n"]

        if bullish:
            message_lines.append("🟢 **BUY Signal (%K cross above %D below 20)**\n")
            for res in bullish:
                pair_safe = html.escape(res['pair'])
                link = f"https://coindcx.com/futures/{res['pair']}"
                message_lines.append(
                    f"{pair_safe}\nClose: {res['close']}\n%K: {res['%K']}\n%D: {res['%D']}\n"
                    f"Volume: {res['volume']}\n{link}\n"
                )

        if bearish:
            message_lines.append("\n🔴 **SELL Signal (%K cross below %D above 80)**\n")
            for res in bearish:
                pair_safe = html.escape(res['pair'])
                link = f"https://coindcx.com/futures/{res['pair']}"
                message_lines.append(
                    f"{pair_safe}\nClose: {res['close']}\n%K: {res['%K']}\n%D: {res['%D']}\n"
                    f"Volume: {res['volume']}\n{link}\n"
                )

        message_lines.append("\n===============================")
        final_message = "\n".join(message_lines)
        # print(final_message)
        Telegram_Alert_EMA_Crossover(final_message)
   

if __name__ == "__main__":
    main()
