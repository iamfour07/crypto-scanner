import json, requests, pandas as pd, os
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Telegram Module
try:
    from Telegram_Momentum import Send_Momentum_Telegram_Message
except ImportError:
    print("Error: Telegram module not found!")

# ================= CONFIG =================
RESOLUTION = "60"
LIMIT_HOURS = 300
MAX_WORKERS = 20
FILE_NAME = "ReversalSellWatchlist.json"

BB_LENGTH, BB_MULT = 200, 2.5

ST_LENGTH = 20
ST_MULT = 2

RISK_PER_TRADE = 50
LEVERAGE = 5

# ================= API HELPERS =================
def safe_get(url, params=None, timeout=10):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        return r.json() if r.status_code == 200 else None
    except:
        return None


# ================= SUPER TREND =================
def calculate_supertrend(df, period=20, multiplier=2):

    hl2 = (df["high"] + df["low"]) / 2

    df["tr"] = pd.concat([
        df["high"] - df["low"],
        abs(df["high"] - df["close"].shift()),
        abs(df["low"] - df["close"].shift())
    ], axis=1).max(axis=1)

    df["atr"] = df["tr"].rolling(period).mean()

    df["upperband"] = hl2 + multiplier * df["atr"]
    df["lowerband"] = hl2 - multiplier * df["atr"]

    supertrend = []
    direction = []

    for i in range(len(df)):

        if i == 0:
            supertrend.append(df["upperband"].iloc[i])
            direction.append(True)
            continue

        if df["close"].iloc[i] > supertrend[i-1]:
            direction.append(True)
        elif df["close"].iloc[i] < supertrend[i-1]:
            direction.append(False)
        else:
            direction.append(direction[i-1])

        if direction[i]:
            supertrend.append(max(df["lowerband"].iloc[i], supertrend[i-1]))
        else:
            supertrend.append(min(df["upperband"].iloc[i], supertrend[i-1]))

    df["supertrend"] = supertrend
    df["st_dir"] = direction

    return df


# ================= INDICATORS =================
def calculate_indicators(df):

    mid = df["close"].rolling(BB_LENGTH).mean()
    std = df["close"].rolling(BB_LENGTH).std()

    df["BB_upper"] = mid + BB_MULT * std

    df = calculate_supertrend(df, ST_LENGTH, ST_MULT)

    return df


# ================= FETCH CANDLES =================
def fetch_candles(pair):

    now = int(datetime.now(timezone.utc).timestamp())

    url = "https://public.coindcx.com/market_data/candlesticks"

    params = {
        "pair": pair,
        "from": now - LIMIT_HOURS * 3600,
        "to": now,
        "resolution": RESOLUTION,
        "pcode": "f"
    }

    data = safe_get(url, params=params)

    if not data or "data" not in data:
        return None

    df = pd.DataFrame(data["data"]).sort_values("time").iloc[:-1]

    if len(df) < BB_LENGTH:
        return None

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return calculate_indicators(df).dropna()


# ================= CORE LOGIC =================
def process_logic(pair, watch_list):

    df = fetch_candles(pair)

    if df is None or df.empty:
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2]

    # ===== WATCHLIST COINS =====
    if pair in watch_list:

        flip_red = prev["st_dir"] == True and last["st_dir"] == False

        if flip_red:

            entry = last["low"]
            sl = last["supertrend"]

            risk_per_coin = sl - entry

            if risk_per_coin <= 0:
                return None

            qty = RISK_PER_TRADE / risk_per_coin

            position_value = qty * entry
            margin = position_value / LEVERAGE

            target2 = entry - (risk_per_coin * 2)
            target3 = entry - (risk_per_coin * 3)
            target4 = entry - (risk_per_coin * 4)

            profit2 = RISK_PER_TRADE * 2
            profit3 = RISK_PER_TRADE * 3
            profit4 = RISK_PER_TRADE * 4

            return (
                "SIGNAL",
                pair,
                entry,
                sl,
                margin,
                target2,
                target3,
                target4,
                profit2,
                profit3,
                profit4
            )

        return ("KEEP", pair)

    # ===== ADD TO WATCHLIST =====
    else:

        breakout = last["close"] > last["BB_upper"]
        supertrend_green = last["st_dir"] == True

        if breakout and supertrend_green:
            return ("ADD", pair)

    return None


# ================= MAIN FLOW =================
def main():

    if os.path.exists(FILE_NAME):
        with open(FILE_NAME, "r") as f:
            watch_list = json.load(f)
    else:
        watch_list = []

    # STEP 1: Active Coins
    url_all = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"

    all_pairs_raw = safe_get(url_all)

    if not all_pairs_raw:
        return

    all_pairs = [p for p in all_pairs_raw if isinstance(p, str)]

    # STEP 2: Stats
    def fetch_pair_stats(pair):

        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        data = safe_get(url)

        if not data:
            return None

        pc = data.get("price_change_percent", {}).get("1D")

        return {"pair": pair, "change": float(pc)} if pc is not None else None


    stats_list = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        stats_list = [r for r in ex.map(fetch_pair_stats, all_pairs) if r]

    sorted_gainers = sorted(stats_list, key=lambda x: x["change"], reverse=True)

    top_pairs = [x["pair"] for x in sorted_gainers[:10]]

    tasks = []
    results = []
    alerts = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        scan_pool = list(set(watch_list + top_pairs))

        for p in scan_pool:
            tasks.append(executor.submit(process_logic, p, watch_list))

        for future in as_completed(tasks):

            res = future.result()

            if not res:
                continue

            if res[0] == "SIGNAL":

                alerts.append(
f"""
🔴 REVERSAL SELL: {res[1]}

Entry: {res[2]:.6f}
StopLoss: {res[3]:.6f}

Capital Required (5x): ₹{res[4]:.2f}

Target 1 (1:2): {res[5]:.6f}
Profit: ₹{res[8]}

Target 2 (1:3): {res[6]:.6f}
Profit: ₹{res[9]}

Target 3 (1:4): {res[7]:.6f}
Profit: ₹{res[10]}
"""
                )

            elif res[0] in ["KEEP", "ADD"]:
                results.append(res[1])

    # Telegram Alert
    if alerts:
        Send_Momentum_Telegram_Message("\n\n".join(alerts))

    clean = sorted(list(set(results)))

    with open(FILE_NAME, "w") as f:
        json.dump(clean, f, indent=2)


if __name__ == "__main__":
    main()