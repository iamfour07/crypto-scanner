import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import html
from Telegram_Alert_Swing import send_telegram_message

# =========================
# CONFIGURATION
# =========================
resolution = "60"   # 1-hour candles
limit_hours = 1000  # fetch 1000 hours history
IST = timezone(timedelta(hours=5, minutes=30))

# Indicator settings
CCI_PERIOD = 200       # common CCI default [web:18]
WILLR_PERIOD = 140    # common Williams %R default [web:4]

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
# INDICATORS: CCI and Williams %R
# =========================
def calculate_cci(df: pd.DataFrame, period: int = CCI_PERIOD):
    # Typical Price TP = (High + Low + Close) / 3 [web:18]
    tp = (df['high'] + df['low'] + df['close']) / 3  # [web:18]
    sma_tp = tp.rolling(window=period).mean()        # [web:18]
    # Mean Deviation over period [web:18]
    md = tp.rolling(window=period).apply(lambda x: (abs(x - x.mean())).mean(), raw=False)  # [web:18]
    # CCI = (TP - SMA(TP)) / (0.015 * MD) [web:18]
    df['CCI'] = (tp - sma_tp) / (0.015 * md)         # [web:18]
    return df

def calculate_williams_r(df: pd.DataFrame, period: int = WILLR_PERIOD):
    # %R = (Highest High - Close) / (Highest High - Lowest Low) * -100 [web:4]
    highest_high = df['high'].rolling(window=period).max()  # [web:4]
    lowest_low = df['low'].rolling(window=period).min()     # [web:4]
    rng = (highest_high - lowest_low)                       # [web:4]
    rng = rng.replace(0, pd.NA)                             # [web:4]
    df['WILLR'] = (highest_high - df['close']) / rng * -100 # [web:4]
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
    if df.empty or len(df) < 3:
        return None

    # Ensure float types
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = df[col].astype(float)

    # Compute indicators
    df = calculate_cci(df, CCI_PERIOD)           # [web:18]
    df = calculate_williams_r(df, WILLR_PERIOD)  # [web:4]

    # Use last CLOSED candle (not running one)
    last = df.iloc[-2]
    prev = df.iloc[-3]

    last_cci, prev_cci = last['CCI'], prev['CCI']
    last_wr, prev_wr = last['WILLR'], prev['WILLR']

    # Skip if indicators not valid (NaN)
    if any(pd.isna(x) for x in [last_cci, prev_cci, last_wr, prev_wr]):
        return None

    # "Just crossed" logic:
    # Buy:
    # - Williams %R: previously <= -20 and now > -20 (crossed up through -20) [web:4][web:27]
    # - CCI: previously <= +100 and now > +100 (crossed up through +100) [web:18][web:35]
    wr_cross_up_20 = (prev_wr <= -20) and (last_wr > -20)      # [web:4]
    cci_cross_up_100 = (prev_cci <= 100) and (last_cci > 100)  # [web:18]

    # Sell:
    # - Williams %R: previously >= -80 and now < -80 (crossed down through -80) [web:4][web:27]
    # - CCI: previously >= -100 and now < -100 (crossed down through -100) [web:18][web:35]
    wr_cross_down_80 = (prev_wr >= -80) and (last_wr < -80)     # [web:4]
    cci_cross_down_100 = (prev_cci >= -100) and (last_cci < -100)  # [web:18]

    # Both conditions must happen on the same latest closed candle
    buy_signal = wr_cross_up_20 and cci_cross_up_100             # [web:4][web:18]
    sell_signal = wr_cross_down_80 and cci_cross_down_100        # [web:4][web:18]

    return {
        "pair": pair,
        "close": last['close'],
        "CCI": round(float(last_cci), 2),
        "WILLR": round(float(last_wr), 2),
        "volume": last['volume'],
        "wr_cross_up_20": wr_cross_up_20,
        "cci_cross_up_100": cci_cross_up_100,
        "wr_cross_down_80": wr_cross_down_80,
        "cci_cross_down_100": cci_cross_down_100,
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

    if bullish or bearish:
        header = f"📊 CCI ({CCI_PERIOD}) + Williams %R ({WILLR_PERIOD}) — Just-cross signals — {resolution}\n"
        message_lines = [header]  # [web:4][web:18]

        if bullish:
            message_lines.append("🟢 Buy (W%R ↑ -20 and CCI ↑ +100)\n")  # [web:4][web:18]
            for res in bullish:
                pair_safe = html.escape(res['pair'])
                link = f"https://coindcx.com/futures/{res['pair']}"
                message_lines.append(
                    f"{pair_safe}\nClose: {res['close']}\nCCI: {res['CCI']}\nW%R: {res['WILLR']}\n"
                    f"Volume: {res['volume']}\nReason: W%R cross up -20, CCI cross up +100\n{link}\n"
                )

        if bearish:
            message_lines.append("\n🔴 Sell (W%R ↓ -80 and CCI ↓ -100)\n")  # [web:4][web:18]
            for res in bearish:
                pair_safe = html.escape(res['pair'])
                link = f"https://coindcx.com/futures/{res['pair']}"
                message_lines.append(
                    f"{pair_safe}\nClose: {res['close']}\nCCI: {res['CCI']}\nW%R: {res['WILLR']}\n"
                    f"Volume: {res['volume']}\nReason: W%R cross down -80, CCI cross down -100\n{link}\n"
                )

        message_lines.append("\n===============================")
        final_message = "\n".join(message_lines)
        # print(final_message)
        send_telegram_message(final_message)
   
if __name__ == "__main__":
    main()
