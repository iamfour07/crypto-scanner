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

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.iloc[:-1]

        if len(df) < 210:
            return None

        return df

    except:
        return None


# ================= INDICATORS =================
def add_indicators(df):
    df["close"] = df["close"].astype("float64")

    # EMA 200
    df["ema200"] = df["close"].ewm(span=200, adjust=False, min_periods=200).mean()

    # BB (20,2) → TradingView match
    mid = df["close"].rolling(BB_LENGTH).mean()
    std = df["close"].rolling(BB_LENGTH).std(ddof=0)

    df["bb_upper"] = mid + BB_MULT * std
    df["bb_lower"] = mid - BB_MULT * std

    return df.dropna()


# ================= SIGNAL LOGIC =================
def check_signal(df):
    prev = df.iloc[-2]
    last = df.iloc[-1]

    # BUY
    if (
        prev["close"] < prev["ema200"] and
        last["close"] > last["ema200"] and
        last["close"] > last["bb_upper"]
    ):
        return "BUY", last["high"], last["low"]

    # SELL
    if (
        prev["close"] > prev["ema200"] and
        last["close"] < last["ema200"] and
        last["close"] < last["bb_lower"]
    ):
        return "SELL", last["low"], last["high"]

    return None


# ================= RISK MANAGEMENT =================
def calculate_trade(entry, sl, side):
    risk_per_unit = abs(entry - sl)

    if risk_per_unit == 0:
        return None

    qty = min(
        RISK_RS / risk_per_unit,
        (MAX_CAPITAL * LEVERAGE) / entry
    )

    position_value = qty * entry
    capital = position_value / LEVERAGE

    if side == "BUY":
        t1 = entry + risk_per_unit
        t2 = entry + 2 * risk_per_unit
        t3 = entry + 3 * risk_per_unit
    else:
        t1 = entry - risk_per_unit
        t2 = entry - 2 * risk_per_unit
        t3 = entry - 3 * risk_per_unit

    return qty, capital, t1, t2, t3


# ================= MAIN =================
# ================= MAIN =================
def main():
    print("🚀 Fetching pairs...")
    pairs = get_all_pairs()

    print("📊 Fetching stats...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        stats = [r for r in executor.map(get_pair_stats, pairs) if r]

    gainers = sorted(stats, key=lambda x: x["change"], reverse=True)[:TOP_COINS]
    losers = sorted(stats, key=lambda x: x["change"])[:TOP_COINS]

    selected = [x["pair"] for x in gainers + losers]

    print("📈 Fetching candles...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        dfs = list(executor.map(fetch_candles, selected))

    alerts = []  # ✅ IMPORTANT

    print("\n📊 SIGNALS\n")

    for pair, df in zip(selected, dfs):
        if df is None:
            continue

        df = add_indicators(df)
        signal = check_signal(df)

        if not signal:
            continue

        side, entry, sl = signal
        trade = calculate_trade(entry, sl, side)

        if not trade:
            continue

        capital, t2, t3 = trade  # ✅ FIXED

        msg = (
            f"{'🟢 BUY' if side=='BUY' else '🔴 SELL'} | {pair}\n"
            f"Entry : {round(entry,5)}\n"
            f"SL    : {round(sl,5)}\n\n"
            f"Risk  : ₹{RISK_RS}\n"
            f"Capital Used : ₹{round(capital,2)}\n\n"
            f"Targets:\n"
            f"2R → {round(t2,5)}\n"
            f"3R → {round(t3,5)}\n"
            f"----------------------------------"
        )

        print(msg)
        alerts.append(msg)  # ✅ collect alerts

    # ✅ SEND ONCE
    if alerts:
        Send_Swing_Telegram_Message("\n\n".join(alerts))
    else:
        print("No signals found.")

if __name__ == "__main__":
    main()
