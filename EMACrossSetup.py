import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import html
from Telegram_Alert_EMA_Crossover import Telegram_Alert_EMA_Crossover

# =========================
# CONFIGURATION
# =========================
resolution = "15m"
limit_hours = 1000
IST = timezone(timedelta(hours=5, minutes=30))

EMA = 21
USE_RSI = True
RSI_PERIOD = 21
RSI_THRESHOLD_BULL = 50
RSI_THRESHOLD_BEAR = 50

CoinList = ['BTCUSD',"ETHUSD",'SOLUSD',"XRPUSD","BNBUSD","DOGEUSD",'TRXUSD',
            "LINKUSD","SUIUSD","LTCUSD","ASTERUSD"]

# =========================
# RSI CALCULATION
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

    # EMA21
    df[f'ema{EMA}'] = df['close'].ewm(span=EMA, adjust=False).mean()
    # RSI
    df['rsi'] = calculate_rsi(df['close'], RSI_PERIOD)

    last = df.iloc[-2]   # last closed candle
    prev5 = df.iloc[-6:-2]  # previous 5 candles

    bullish = bearish = False
    last_rsi = round(last['rsi'], 1) if pd.notnull(last['rsi']) else None

    # =========================
    # BUY SETUP
    # =========================
    if last['close'] < last['open'] and last['close'] > last[f'ema{EMA}']:
        if not USE_RSI or (last_rsi is not None and last_rsi > RSI_THRESHOLD_BULL):
            for i in range(len(prev5) - 1):
                c1 = prev5.iloc[i]
                c2 = prev5.iloc[i + 1]
                if c1['close'] < c1[f'ema{EMA}'] and c2['close'] > c2[f'ema{EMA}']:
                    bullish = True
                    break

    # =========================
    # SELL SETUP
    # =========================
    if last['close'] > last['open'] and last['close'] < last[f'ema{EMA}']:
        if not USE_RSI or (last_rsi is not None and last_rsi < RSI_THRESHOLD_BEAR):
            for i in range(len(prev5) - 1):
                c1 = prev5.iloc[i]
                c2 = prev5.iloc[i + 1]
                if c1['close'] > c1[f'ema{EMA}'] and c2['close'] < c2[f'ema{EMA}']:
                    bearish = True
                    break

    if bullish or bearish:
        print(f"✅ {symbol}: Bullish={bullish}, Bearish={bearish}, Close={last['close']}, RSI={last_rsi}")

    return {
        "pair": symbol,
        "close": float(last['close']),
        "volume": float(last['volume']),
        "bullish": bullish,
        "bearish": bearish,
        "rsi": last_rsi
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
        message_lines = [f"📊 21 EMA Red/Green Setup\n"]

        if bullish:
            message_lines.append("🟢 Buy Setup (Red Candle Above EMA21 + Recent Bullish Cross + RSI>50):\n")
            for res in bullish:
                pair_safe = html.escape(res['pair'])
                link = f"https://coindcx.com/futures/{res['pair']}"
                message_lines.append(
                    f"{pair_safe}\nClose: {res['close']}\nRSI: {res['rsi']}\nVolume: {res['volume']}\n{link}\n"
                )

        if bearish:
            message_lines.append("\n🔴 Sell Setup (Green Candle Below EMA21 + Recent Bearish Cross + RSI<50):\n")
            for res in bearish:
                pair_safe = html.escape(res['pair'])
                link = f"https://coindcx.com/futures/{res['pair']}"
                message_lines.append(
                    f"{pair_safe}\nClose: {res['close']}\nRSI: {res['rsi']}\nVolume: {res['volume']}\n{link}\n"
                )

        message_lines.append("\n===============================")
        final_message = "\n".join(message_lines)
        # print(final_message)
        Telegram_Alert_EMA_Crossover(final_message)

if __name__ == "__main__":
    main()
