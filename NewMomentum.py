import json
import os
import requests
import pandas as pd
from datetime import datetime, timezone
from Telegram_Swing import Send_Swing_Telegram_Message

# ================= CONFIG =================
PAIR = "B-XAU_USDT"      # CoinDCX futures pair for BTC
resolution = "60"        # 1 hour candles
limit_hours = 1000       # how far back to fetch

RSI_LENGTH = 28
RSI_UPPER = 60
RSI_LOWER = 40

EMA_FAST = 9
EMA_MID = 21
EMA_SLOW = 55

WATCHLIST_FILE = "IntradayWatchlist.json"


# ================= UTIL =================
def safe_get(url, params=None, timeout=10):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


# ================= INDICATORS =================
def rma(series, period):
    return series.ewm(alpha=1 / period, adjust=False).mean()


def calculate_rsi(df, length=RSI_LENGTH):
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = rma(gain, length)
    avg_loss = rma(loss, length)

    rs = avg_gain / avg_loss
    df["RSI"] = 100 - (100 / (1 + rs))
    return df


def calculate_emas(df):
    df["EMA9"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["EMA21"] = df["close"].ewm(span=EMA_MID, adjust=False).mean()
    df["EMA55"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    return df


# ================= FETCH =================
def fetch_candles(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - limit_hours * 3600

    url = "https://public.coindcx.com/market_data/candlesticks"
    params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}

    data = safe_get(url, params)
    if not data or "data" not in data:
        return None

    candles = data["data"]
    if len(candles) < max(RSI_LENGTH, EMA_SLOW) + 5:
        return None

    df = pd.DataFrame(candles).sort_values("time").iloc[:-1]  # drop unclosed candle

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = calculate_rsi(df)
    df = calculate_emas(df)

    return df.dropna()


# ================= WATCHLIST =================
def ensure_watchlist_file(file):
    """Create the watchlist file with {} only if it doesn't already exist."""
    if not os.path.exists(file):
        with open(file, "w") as f:
            json.dump({}, f, indent=2)
        print(f"{file} not found — created a new empty watchlist.")


def load_watchlist(file):
    try:
        with open(file) as f:
            data = json.load(f)
            return data if data else {}
    except Exception:
        return {}


def save_watchlist(file, data):
    with open(file, "w") as f:
        json.dump(data, f, indent=2)


# ================= STEP 1: SEED WATCHLIST FROM RSI =================
def check_rsi_and_seed(last):
    """Only called when watchlist is empty."""
    rsi_val = float(last["RSI"])
    close_val = float(last["close"])

    if rsi_val < RSI_LOWER:
        return {"last_close_price": close_val, "setup_type": "buy"}

    if rsi_val > RSI_UPPER:
        return {"last_close_price": close_val, "setup_type": "sell"}

    return None


# ================= STEP 2: CHECK EMA TRIGGER =================
def check_ema_trigger(watch, last, prev):
    """Only called when watchlist already has an entry."""
    setup_type = watch.get("setup_type")

    if setup_type == "sell":
        nine_below_21 = last["EMA9"] < last["EMA21"]
        crossed_below_55 = prev["EMA21"] >= prev["EMA55"] and last["EMA21"] < last["EMA55"]
        if nine_below_21 and crossed_below_55:
            return True

    elif setup_type == "buy":
        nine_above_21 = last["EMA9"] > last["EMA21"]
        crossed_above_55 = prev["EMA21"] <= prev["EMA55"] and last["EMA21"] > last["EMA55"]
        if nine_above_21 and crossed_above_55:
            return True

    return False


def build_alert_message(watch, last):
    setup_type = watch.get("setup_type")
    emoji = "🟢" if setup_type == "buy" else "🔴"
    link = f"https://coindcx.com/futures/{PAIR}"

    msg = (
        f"{emoji} {setup_type.upper()} SETUP TRIGGERED — BTC\n"
        f"Watchlisted Price : {round(watch.get('last_close_price'), 2)}\n"
        f"Current Close     : {round(float(last['close']), 2)}\n"
        f"EMA9  : {round(float(last['EMA9']), 2)}\n"
        f"EMA21 : {round(float(last['EMA21']), 2)}\n"
        f"EMA55 : {round(float(last['EMA55']), 2)}\n"
        f"{link}\n"
        f"------------------------------------------------"
    )
    return msg


# ================= MAIN =================
def main():
    df = fetch_candles(PAIR)
    if df is None or len(df) < 10:
        print("Not enough candle data, skipping this run.")
        return

    last = df.iloc[-1]
    prev = df.iloc[-2]

    ensure_watchlist_file(WATCHLIST_FILE)
    watch = load_watchlist(WATCHLIST_FILE)

    if not watch:
        # Step 1: try to seed a new buy/sell watch from RSI
        new_watch = check_rsi_and_seed(last)
        if new_watch:
            save_watchlist(WATCHLIST_FILE, new_watch)
            print(f"Watchlist seeded: {new_watch}")
        else:
            print(f"RSI={round(float(last['RSI']), 2)} — inside 40-60 band, no action.")
    else:
        # Step 2: check EMA trigger for the pending setup
        triggered = check_ema_trigger(watch, last, prev)
        if triggered:
            msg = build_alert_message(watch, last)
            Send_Swing_Telegram_Message(msg)
            save_watchlist(WATCHLIST_FILE, {})
            print("Trigger fired, alert sent, watchlist cleared.")
        else:
            print(f"Watching pending '{watch.get('setup_type')}' setup, condition not yet met.")


if __name__ == "__main__":
    main()