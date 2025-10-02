import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import html
from Telegram_Alert import send_telegram_message

# =========================
# CONFIGURATION
# =========================
resolution = "60"   # 1-hour candles
limit_hours = 1000  # fetch 1000 hours history
IST = timezone(timedelta(hours=5, minutes=30))

# RSI settings
RSI_PERIOD = 21
RSI_OVERBOUGHT = 73
RSI_OVERSOLD = 27

# Track alerted coins (state memory)
alerted_coins = {}   # { "BTCUSDT": "overbought" / "oversold" / "neutral" }

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
    df['volume'] = df['volume'].astype(float)

    # RSI
    df['rsi'] = calculate_rsi(df['close'], RSI_PERIOD)

    last_rsi = df['rsi'].iloc[-1]
    last_candle = df.iloc[-1]

    # Check existing state
    coin_state = alerted_coins.get(pair, "neutral")

    buy_signal = False
    sell_signal = False

    # RSI conditions
    if last_rsi > RSI_OVERBOUGHT:
        if coin_state != "overbought":   # only trigger once
            buy_signal = True
            alerted_coins[pair] = "overbought"

    elif last_rsi < RSI_OVERSOLD:
        if coin_state != "oversold":     # only trigger once
            sell_signal = True
            alerted_coins[pair] = "oversold"

    else:
        # Reset to neutral so alerts can fire again next time
        alerted_coins[pair] = "neutral"

    return {
        "pair": pair,
        "close": last_candle['close'],
        "rsi": round(last_rsi, 1),
        "volume": last_candle['volume'],
        "buy_signal": buy_signal,
        "sell_signal": sell_signal,
    }

# =========================
# MAIN SCANNER
# =========================
def main():
    buy_signals, sell_signals = [], []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_coin_data, coin): coin for coin in CoinList}
        for future in as_completed(futures):
            try:
                data = future.result()
                if data['buy_signal']:
                    buy_signals.append(data)
                elif data['sell_signal']:
                    sell_signals.append(data)
            except Exception as e:
                print(f"Error processing coin: {e}")

    # Sort by volume
    buy_signals.sort(key=lambda x: x['volume'], reverse=True)
    sell_signals.sort(key=lambda x: x['volume'], reverse=True)

    # =========================
    # PREPARE TELEGRAM MESSAGE
    # =========================
    if buy_signals or sell_signals:
        message_lines = [f"📊 Hourly RSI Signals\n"]

        if buy_signals:
            message_lines.append("🟢 BUY (RSI > 75):\n")
            for res in buy_signals:
                pair_safe = html.escape(res['pair'])
                link = f"https://coindcx.com/futures/{res['pair']}"
                message_lines.append(
                    f"{pair_safe}\nClose: {res['close']}\nRSI: {res['rsi']}\nVolume: {res['volume']}\n{link}\n"
                )

        if sell_signals:
            message_lines.append("\n🔴 SELL (RSI < 25):\n")
            for res in sell_signals:
                pair_safe = html.escape(res['pair'])
                link = f"https://coindcx.com/futures/{res['pair']}"
                message_lines.append(
                    f"{pair_safe}\nClose: {res['close']}\nRSI: {res['rsi']}\nVolume: {res['volume']}\n{link}\n"
                )

        message_lines.append("\n===============================")
        final_message = "\n".join(message_lines)
        # print(final_message)
        send_telegram_message(final_message)

if __name__ == "__main__":
    main()
