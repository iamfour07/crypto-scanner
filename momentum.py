import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from Telegram_Swing import Send_Swing_Telegram_Message

# ================= CONFIG =================
TOP_COINS = 30
MAX_WORKERS = 20
RESOLUTION = "60"
LIMIT_HOURS = 600

BB_LENGTH = 20
BB_MULT = 2

RISK_RS = 100
LEVERAGE = 10
MAX_CAPITAL = 5000

ATR_PERIOD = 14
ATR_SL_MULT = 1.5
RSI_PERIOD = 14
VOL_AVG_PERIOD = 20
BB_WIDTH_AVG_PERIOD = 20


# ================= FETCH PAIRS =================
def get_all_pairs():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    try:
        data = requests.get(url).json()
        return [p for p in data if isinstance(p, str)]
    except:
        return []


# ================= FETCH STATS =================
def get_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        data = requests.get(url, timeout=5).json()
        change = data.get("price_change_percent", {}).get("1D", 0)
        return {"pair": pair, "change": float(change)}
    except:
        return None


# ================= FETCH CANDLES =================
def fetch_candles(pair):
    url = "https://public.coindcx.com/market_data/candlesticks"
    now = int(datetime.now(timezone.utc).timestamp())

    params = {
        "pair": pair,
        "from": now - LIMIT_HOURS * 3600,
        "to": now,
        "resolution": RESOLUTION,
        "pcode": "f"
    }

    try:
        data = requests.get(url, params=params, timeout=10).json()

        if "data" not in data:
            return None

        df = pd.DataFrame(data["data"]).sort_values("time").reset_index(drop=True)

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.iloc[:-1]  # drop last incomplete candle

        if len(df) < 210:
            return None

        return df

    except:
        return None


# ================= INDICATORS =================
def add_indicators(df):
    df["close"] = df["close"].astype("float64")

    # ── EMA 200 ──────────────────────────────────────────────
    df["ema200"] = df["close"].ewm(span=200, adjust=False, min_periods=200).mean()

    # ── Bollinger Bands (20, 2) ───────────────────────────────
    mid = df["close"].rolling(BB_LENGTH).mean()
    std = df["close"].rolling(BB_LENGTH).std(ddof=0)
    df["bb_upper"] = mid + BB_MULT * std
    df["bb_lower"] = mid - BB_MULT * std

    # ── BB Width (Squeeze Detection) ──────────────────────────
    df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / mid
    df["bb_width_avg"] = df["bb_width"].rolling(BB_WIDTH_AVG_PERIOD).mean()

    # ── RSI (14) ──────────────────────────────────────────────
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(RSI_PERIOD).mean()
    loss = (-delta.clip(upper=0)).rolling(RSI_PERIOD).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))

    # ── ATR (14) ──────────────────────────────────────────────
    high_low   = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close  = (df["low"]  - df["close"].shift()).abs()
    df["atr"] = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1).rolling(ATR_PERIOD).mean()

    # ── Volume Average ────────────────────────────────────────
    df["vol_avg"] = df["volume"].rolling(VOL_AVG_PERIOD).mean()

    return df.dropna()


# ================= SIGNAL LOGIC =================
def check_signal(df):
    prev = df.iloc[-2]
    last = df.iloc[-1]

    # ── Body Strength (avoid wicks/doji) ─────────────────────
    body_size  = abs(last["close"] - last["open"])
    range_size = last["high"] - last["low"]

    if range_size == 0:
        return None

    strong_body = body_size > 0.6 * range_size

    # ── Shared Filters ────────────────────────────────────────
    volume_spike  = last["volume"] > 1.5 * last["vol_avg"]   # real participation
    was_squeezed  = prev["bb_width"] < prev["bb_width_avg"]  # bands were tight before
    is_expanding  = last["bb_width"] > prev["bb_width"]      # now exploding outward

    # ── BUY CONDITIONS ────────────────────────────────────────
    # 1. Strict EMA 200 crossover (prev below, now above)
    # 2. Close above BB upper (momentum burst)
    # 3. RSI in momentum zone (not overbought)
    # 4. Volume spike (real breakout)
    # 5. BB was squeezed and now expanding (energy release)
    cross_up = prev["close"] < prev["ema200"] and last["close"] > last["ema200"]
    rsi_buy  = 55 < last["rsi"] < 75

    if (cross_up
            and last["close"] > last["bb_upper"]
            and strong_body
            and volume_spike
            and was_squeezed
            and is_expanding
            and rsi_buy):
        return "BUY", last["close"], last["atr"]

    # ── SELL CONDITIONS ───────────────────────────────────────
    cross_down = prev["close"] > prev["ema200"] and last["close"] < last["ema200"]
    rsi_sell   = 25 < last["rsi"] < 45

    if (cross_down
            and last["close"] < last["bb_lower"]
            and strong_body
            and volume_spike
            and was_squeezed
            and is_expanding
            and rsi_sell):
        return "SELL", last["close"], last["atr"]

    return None


# ================= RISK MANAGEMENT =================
def calculate_trade(entry, atr, side):
    sl_distance = ATR_SL_MULT * atr  # dynamic SL based on volatility

    if sl_distance == 0:
        return None

    if side == "BUY":
        sl = entry - sl_distance
    else:
        sl = entry + sl_distance

    qty = min(
        RISK_RS / sl_distance,
        (MAX_CAPITAL * LEVERAGE) / entry
    )

    position_value = qty * entry
    capital = position_value / LEVERAGE

    if side == "BUY":
        t1 = entry + sl_distance
        t2 = entry + 2 * sl_distance
        t3 = entry + 3 * sl_distance
    else:
        t1 = entry - sl_distance
        t2 = entry - 2 * sl_distance
        t3 = entry - 3 * sl_distance

    return qty, capital, sl, t1, t2, t3


# ================= MAIN =================
def main():
    print("🚀 Fetching pairs...")
    pairs = get_all_pairs()

    print("📊 Fetching stats...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        stats = [r for r in executor.map(get_pair_stats, pairs) if r]

    gainers = sorted(stats, key=lambda x: x["change"], reverse=True)[:TOP_COINS]
    losers  = sorted(stats, key=lambda x: x["change"])[:TOP_COINS]

    selected = [x["pair"] for x in gainers + losers]

    print("📈 Fetching candles...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        dfs = list(executor.map(fetch_candles, selected))

    alerts = []

    print("\n📊 SIGNALS\n")

    for pair, df in zip(selected, dfs):
        if df is None:
            continue

        df = add_indicators(df)
        signal = check_signal(df)

        if not signal:
            continue

        side, entry, atr = signal
        trade = calculate_trade(entry, atr, side)

        if not trade:
            continue

        qty, capital, sl, t1, t2, t3 = trade

        msg = (
            f"{'🟢 BUY' if side == 'BUY' else '🔴 SELL'} | {pair}\n"
            f"Entry  : {round(entry, 5)}\n"
            f"SL     : {round(sl, 5)}  (ATR x{ATR_SL_MULT})\n\n"
            f"Risk   : ₹{RISK_RS}\n"
            f"Capital: ₹{round(capital, 2)}\n\n"
            f"Targets:\n"
            f"  1R → {round(t1, 5)}\n"
            f"  2R → {round(t2, 5)}\n"
            f"  3R → {round(t3, 5)}\n"
            f"----------------------------------"
        )

        print(msg)
        alerts.append(msg)

    if alerts:
        Send_Swing_Telegram_Message("\n\n".join(alerts))
    else:
        print("No signals found.")


if __name__ == "__main__":
    main()