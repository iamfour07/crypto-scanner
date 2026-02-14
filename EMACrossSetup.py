import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import html
from Telegram_EMA import Send_EMA_Telegram_Message

# =========================
# CONFIGURATION
# =========================
resolution = "60"
limit_hours = 2000
IST = timezone(timedelta(hours=5, minutes=30))

WMA_PERIOD = 5

CoinList = ['B-SOL_USDT','B-DOGE_USDT']

CAPITAL_RS = 800
MAX_LOSS_RS = 100
MAX_ALLOWED_LEVERAGE = 10
MIN_LEVERAGE = 5

# =========================
# WMA CALCULATION
# =========================
def calculate_wma(series, period):
    weights = list(range(1, period + 1))
    return series.rolling(period).apply(
        lambda prices: sum(prices * weights) / sum(weights),
        raw=True
    )

def fetch_candles(symbol):

    try:
        now = int(datetime.now(timezone.utc).timestamp())
        from_time = now - limit_hours * 3600

        url = "https://public.coindcx.com/market_data/candlesticks"
        params = {
            "pair": symbol,
            "from": from_time,
            "to": now,
            "resolution": resolution,
            "pcode": "f"
        }

        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json().get("data", [])

        if not data or len(data) < WMA_PERIOD + 2:
            return None

        df = pd.DataFrame(data)

        for col in ["open","high","low","close","volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.sort_values("time").reset_index(drop=True)

        # WMA
        df["wma5"] = calculate_wma(df["close"], WMA_PERIOD)

        signal_candle = df.iloc[-2]
        wma_value = df.iloc[-2]["wma5"]

        if pd.isna(wma_value):
            return None

        ts = signal_candle["time"]

        # CoinDCX returns milliseconds
        if ts > 1e12:
         ts = ts / 1000

        candle_time = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST)

        # print(f"\n----- DEBUG {symbol} -----")
        # print(f"Candle Time (IST): {candle_time}")
        # print(f"Close: {signal_candle['close']}")
        # print(f"High:  {signal_candle['high']}")
        # print(f"Low:   {signal_candle['low']}")
        # print(f"WMA5:  {round(wma_value,4)}")
        # print("----------------------------")

        bullish = False
        bearish = False
        entry = None
        sl = None

        if wma_value > signal_candle["high"]:
            bullish = True
            entry = float(signal_candle["high"])
            sl = float(signal_candle["low"])

        elif wma_value < signal_candle["low"]:
            bearish = True
            entry = float(signal_candle["low"])
            sl = float(signal_candle["high"])

        return {
            "symbol": symbol,
            "close": float(signal_candle["close"]),
            "volume": float(signal_candle["volume"]),
            "bullish": bullish,
            "bearish": bearish,
            "entry": entry,
            "sl": sl
        }

    except Exception as e:
        print("Candle error:", symbol, e)
        return None



def calculate_trade_levels(entry, sl, side):

    risk = abs(entry - sl)

    for lev in range(MAX_ALLOWED_LEVERAGE, MIN_LEVERAGE - 1, -1):
        position_value = CAPITAL_RS * lev
        loss = (risk / entry) * position_value

        if loss <= MAX_LOSS_RS:
            leverage = lev
            break
    else:
        leverage = MIN_LEVERAGE

    used_capital = CAPITAL_RS

    if side == "BUY":
        t2 = entry + risk * 2
        t3 = entry + risk * 3
        t4 = entry + risk * 4
    else:
        t2 = entry - risk * 2
        t3 = entry - risk * 3
        t4 = entry - risk * 4

    return entry, sl, leverage, used_capital, t2, t3, t4


# =========================
# MAIN SCANNER
# =========================
def main():
    bullish, bearish = [], []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_candles, coin): coin for coin in CoinList}
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
        message_lines = ["📊 WMA Setup\n"]

        # ================= BUY =================
        if bullish:
            message_lines.append("🟢 Buy Setup:\n")

            for res in bullish:
                e, s, lev, cap, t2, t3, t4 = calculate_trade_levels(
                    res['entry'], res['sl'], "BUY"
                )

                pair_safe = html.escape(res['symbol'])
                link = f"https://coindcx.com/futures/{res['symbol']}"

                message_lines.append(
                    f"🟢 BUY {pair_safe}\n"
                    f"Entry   : {round(e,4)}\n"
                    f"SL      : {round(s,4)}\n"
                    f"Capital : ₹{cap} ({lev}×)\n\n"
                    f"Targets\n"
                    f"2R → {round(t2,4)}\n"
                    f"3R → {round(t3,4)}\n"
                    f"4R → {round(t4,4)}\n"
                    f"Volume  : {res['volume']}\n"
                    f"{link}\n"
                    f"------------------------------------------------"
                )

        # ================= SELL =================
        if bearish:
            message_lines.append("\n🔴 Sell Setup:\n")

            for res in bearish:
                e, s, lev, cap, t2, t3, t4 = calculate_trade_levels(
                    res['entry'], res['sl'], "SELL"
                )

                pair_safe = html.escape(res['symbol'])
                link = f"https://coindcx.com/futures/{res['symbol']}"

                message_lines.append(
                    f"🔴 SELL {pair_safe}\n"
                    f"Entry   : {round(e,4)}\n"
                    f"SL      : {round(s,4)}\n"
                    f"Capital : ₹{cap} ({lev}×)\n\n"
                    f"Targets\n"
                    f"2R → {round(t2,4)}\n"
                    f"3R → {round(t3,4)}\n"
                    f"4R → {round(t4,4)}\n"
                    f"Volume  : {res['volume']}\n"
                    f"{link}\n"
                    f"------------------------------------------------"
                )

        message_lines.append("\n===============================")
        final_message = "\n".join(message_lines)

        Send_EMA_Telegram_Message(final_message)

if __name__ == "__main__":
    main()