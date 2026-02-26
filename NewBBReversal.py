import json
import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_EMA import Send_EMA_Telegram_Message


# ================= CONFIG =================
resolution = "60"
limit_hours = 500
TOP_COINS_TO_SCAN = 5
MAX_WORKERS = 8

ENABLE_BUY  = True
ENABLE_SELL = True

BUY_FILE  = "NewReversalBuyWatchlist.json"
SELL_FILE = "NewReversalSellWatchlist.json"

BB_LENGTH = 20
BB_MULT   = 2

RSI_LENGTH = 14
RSI_UPPER  = 70
RSI_LOWER  = 30

RISK_RS  = 200
LEVERAGE = 5


# ================= UTIL =================
def safe_get(url, params=None, timeout=10):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except:
        return None


# ================= RISK =================
def calculate_trade_levels(entry, sl, side):
    risk_per_unit = abs(entry - sl)
    if risk_per_unit == 0:
        return None

    position_value = (RISK_RS / risk_per_unit) * entry
    used_capital   = round(position_value / LEVERAGE, 2)
    expected_loss  = RISK_RS

    if side == "BUY":
        t2 = entry + risk_per_unit * 2
        t3 = entry + risk_per_unit * 3
        t4 = entry + risk_per_unit * 4
    else:
        t2 = entry - risk_per_unit * 2
        t3 = entry - risk_per_unit * 3
        t4 = entry - risk_per_unit * 4

    return entry, sl, LEVERAGE, used_capital, expected_loss, t2, t3, t4


# ================= API =================
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    data = safe_get(url, timeout=30)
    if not data:
        return []
    return [x["pair"] if isinstance(x, dict) else x for x in data]


def fetch_pair_stats(pair):
    url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
    data = safe_get(url, timeout=8)
    if not data:
        return None

    pc = data.get("price_change_percent", {}).get("1D")
    if pc is None:
        return None

    return {"pair": pair, "change": float(pc)}


def get_top_movers(pairs):
    gainers, losers = [], []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_pair_stats, p) for p in pairs]
        for f in as_completed(futures):
            res = f.result()
            if not res:
                continue
            if res["change"] > 0:
                gainers.append(res)
            else:
                losers.append(res)

    gainers = sorted(gainers, key=lambda x: x["change"], reverse=True)[:TOP_COINS_TO_SCAN]
    losers  = sorted(losers, key=lambda x: x["change"])[:TOP_COINS_TO_SCAN]

    return [x["pair"] for x in gainers + losers]


# ================= INDICATORS =================
def calculate_heikin_ashi(df):
    df["HA_close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    df["HA_open"]  = (df["open"].shift(1) + df["close"].shift(1)) / 2
    df.iloc[0, df.columns.get_loc("HA_open")] = (df.iloc[0]["open"] + df.iloc[0]["close"]) / 2
    df["HA_high"]  = df[["HA_open", "HA_close", "high"]].max(axis=1)
    df["HA_low"]   = df[["HA_open", "HA_close", "low"]].min(axis=1)
    return df


def calculate_bollinger(df):
    mid = df["HA_close"].rolling(BB_LENGTH).mean()
    std = df["HA_close"].rolling(BB_LENGTH).std()
    df["BB_upper"] = mid + BB_MULT * std
    df["BB_lower"] = mid - BB_MULT * std
    return df


def calculate_rsi(df):
    delta = df["HA_close"].diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / RSI_LENGTH, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / RSI_LENGTH, adjust=False).mean()

    rs = avg_gain / avg_loss
    df["RSI"] = 100 - (100 / (1 + rs))

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
    if len(candles) < BB_LENGTH + 5:
        return None

    df = pd.DataFrame(candles).sort_values("time").iloc[:-1]

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = calculate_heikin_ashi(df)
    df = calculate_bollinger(df)
    df = calculate_rsi(df)

    return df.dropna()


# ================= WATCHLIST =================
def load_watchlist(file):
    try:
        with open(file) as f:
            return json.load(f)
    except:
        return []


def save_watchlist(file, data):
    with open(file, "w") as f:
        json.dump(data, f, indent=2)


# ================= WATCHLIST SIGNAL CHECK =================
def check_watchlist_for_signals(watchlist, side):
    updated_watchlist = []
    alerts = []

    def process_pair(pair):
        df = fetch_candles(pair)
        if df is None or len(df) < 10:
            return ("KEEP", pair)

        last = df.iloc[-1]

        if side == "SELL" and last["RSI"] < RSI_UPPER:
            return ("SIGNAL", pair, f"🔴 SELL {pair}")

        if side == "BUY" and last["RSI"] > RSI_LOWER:
            return ("SIGNAL", pair, f"🟢 BUY {pair}")

        return ("KEEP", pair)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_pair, p) for p in watchlist]

        for f in as_completed(futures):
            res = f.result()
            if res[0] == "SIGNAL":
                alerts.append(res[2])
            else:
                updated_watchlist.append(res[1])

    return updated_watchlist, alerts


# ================= ADD TO WATCHLIST =================
def scan_for_breakouts(top_pairs, buy_watch, sell_watch):

    buy_set  = set(buy_watch)
    sell_set = set(sell_watch)

    def process_pair(pair):
        if pair in buy_set or pair in sell_set:
            return None

        df = fetch_candles(pair)
        if df is None:
            return None

        last = df.iloc[-1]

        if last["RSI"] > RSI_UPPER:
            return ("SELL", pair)

        if last["RSI"] < RSI_LOWER:
            return ("BUY", pair)

        return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_pair, p) for p in top_pairs]

        for f in as_completed(futures):
            res = f.result()
            if not res:
                continue
            if res[0] == "BUY":
                buy_watch.append(res[1])
            else:
                sell_watch.append(res[1])

    return buy_watch, sell_watch


# ================= MAIN =================
def main():
    buy_watch  = load_watchlist(BUY_FILE)
    sell_watch = load_watchlist(SELL_FILE)

    alerts = []

    sell_watch, sell_alerts = check_watchlist_for_signals(sell_watch, "SELL")
    buy_watch,  buy_alerts  = check_watchlist_for_signals(buy_watch,  "BUY")

    alerts += sell_alerts + buy_alerts

    if alerts:
        Send_EMA_Telegram_Message("\n\n".join(alerts))

    pairs = get_active_usdt_coins()
    top_pairs = get_top_movers(pairs)

    buy_watch, sell_watch = scan_for_breakouts(top_pairs, buy_watch, sell_watch)

    save_watchlist(BUY_FILE, buy_watch)
    save_watchlist(SELL_FILE, sell_watch)


if __name__ == "__main__":
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Scanner running...")
    main()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Done.")