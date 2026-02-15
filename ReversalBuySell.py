import json
import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Swing import Send_Swing_Telegram_Message

# CONFIG
resolution = "60"
limit_hours = 1000
MAX_WORKERS = 8
TOP_COINS_TO_SCAN = 30

BUY_FILE = "ReversalBuyWatchlist.json"
SELL_FILE = "ReversalSellWatchlist.json"

BB_LENGTH = 200
BB_MULT = 2.5
ST_LENGTH = 9
ST_FACTOR = 1.5

CAPITAL_RS = 1000
MAX_LOSS_RS = 100
MAX_ALLOWED_LEVERAGE = 20
MIN_LEVERAGE = 5
TOP_COINS_TO_SCAN = 20


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
    risk = abs(entry - sl)
    risk_percent = (risk / entry) * 100

    leverage = MIN_LEVERAGE
    for lev in range(MAX_ALLOWED_LEVERAGE, MIN_LEVERAGE - 1, -1):
        position_value = CAPITAL_RS * lev
        loss = (risk / entry) * position_value
        if loss <= MAX_LOSS_RS:
            leverage = lev
            break

    if side == "BUY":
        targets = [entry + risk * x for x in (2, 3, 4)]
    else:
        targets = [entry - risk * x for x in (2, 3, 4)]

    return entry, sl, leverage, targets, risk_percent


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
    return {"pair": pair, "change": float(pc)} if pc else None


def get_top_movers(pairs):
    gainers, losers = [], []

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(fetch_pair_stats, p) for p in pairs]
        for f in as_completed(futures):
            res = f.result()
            if not res:
                continue
            (gainers if res["change"] > 0 else losers).append(res)

    gainers = sorted(gainers, key=lambda x: x["change"], reverse=True)[:TOP_COINS_TO_SCAN]
    losers = sorted(losers, key=lambda x: x["change"])[:TOP_COINS_TO_SCAN]

    return [x["pair"] for x in gainers + losers]


# ================= INDICATORS =================
def calculate_heikin_ashi(df):
    df["HA_close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    df["HA_open"] = (df["open"].shift(1) + df["close"].shift(1)) / 2
    df.iloc[0, df.columns.get_loc("HA_open")] = (df.iloc[0]["open"] + df.iloc[0]["close"]) / 2
    df["HA_high"] = df[["HA_open", "HA_close", "high"]].max(axis=1)
    df["HA_low"] = df[["HA_open", "HA_close", "low"]].min(axis=1)
    return df


def calculate_bollinger(df):
    mid = df["HA_close"].rolling(BB_LENGTH).mean()
    std = df["HA_close"].rolling(BB_LENGTH).std()
    df["BB_upper"] = mid + BB_MULT * std
    df["BB_lower"] = mid - BB_MULT * std
    return df


def rma(series, period):
    return series.ewm(alpha=1/period, adjust=False).mean()


def calculate_supertrend(df):
    hl = df["HA_high"] - df["HA_low"]
    hc = (df["HA_high"] - df["HA_close"].shift()).abs()
    lc = (df["HA_low"] - df["HA_close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)

    atr = rma(tr, ST_LENGTH)
    hl2 = (df["HA_high"] + df["HA_low"]) / 2

    upper = hl2 + ST_FACTOR * atr
    lower = hl2 - ST_FACTOR * atr

    trend = [True] * len(df)
    st_val = [0] * len(df)

    for i in range(1, len(df)):
        if trend[i - 1]:
            trend[i] = df["HA_high"].iloc[i] >= lower.iloc[i]
        else:
            trend[i] = df["HA_low"].iloc[i] > upper.iloc[i]

        st_val[i] = lower.iloc[i] if trend[i] else upper.iloc[i]

    df["supertrend"] = trend
    df["ST_value"] = st_val
    return df


# ================= FETCH =================
def fetch_and_prepare(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - limit_hours * 3600

    url = "https://public.coindcx.com/market_data/candlesticks"
    params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}

    data = safe_get(url, params)
    if not data or "data" not in data:
        return None

    candles = data["data"]
    if len(candles) < BB_LENGTH + 10:
        return None

    df = pd.DataFrame(candles).sort_values("time").iloc[:-1]

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = calculate_heikin_ashi(df)
    df = calculate_bollinger(df)
    df = calculate_supertrend(df)

    return df.dropna()


# ================= WATCHLIST =================
def load_watchlist(file):
    try:
        with open(file) as f:
            return json.load(f)
    except:
        return []


def save_watchlist(file, data):
    clean_data = [{"pair": str(x["pair"]), "entry_state": bool(x["entry_state"])} for x in data]
    with open(file, "w") as f:
        json.dump(clean_data, f, indent=2)


# ================= WATCHLIST FLIP MODULE =================
def check_watchlist_flips(watchlist, side):
    updated = []
    alerts = []

    def check_pair(pair):
        df = fetch_and_prepare(pair)
        if df is None or len(df) < 3:
            return ("KEEP", pair)

        last = df.iloc[-1]
        prev = df.iloc[-2]

        st_now = last["HA_close"] > last["ST_value"]
        st_prev = prev["HA_close"] > prev["ST_value"]

        if side == "BUY":
            if (not st_prev) and st_now:
                return ("SIGNAL", pair, float(last["HA_high"]), float(last["ST_value"]))

        if side == "SELL":
            if st_prev and not st_now:
                return ("SIGNAL", pair, float(last["HA_low"]), float(last["ST_value"]))

        return ("KEEP", pair)

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(check_pair, x["pair"]) for x in watchlist]

        for f in as_completed(futures):
            res = f.result()
            if res[0] == "SIGNAL":
                alerts.append(format_signal(side, res[1], res[2], res[3]))
            else:
                updated.append({"pair": res[1]})

    return updated, alerts


# ================= SETUP MODULE =================
def scan_for_setups(top_pairs, buy_watch, sell_watch):

    buy_set = {x["pair"] for x in buy_watch}
    sell_set = {x["pair"] for x in sell_watch}

    new_buy = []
    new_sell = []

    def process_pair(pair):
        if pair in buy_set or pair in sell_set:
            return None

        df = fetch_and_prepare(pair)
        if df is None:
            return None

        last = df.iloc[-1]
        st_now = last["HA_close"] > last["ST_value"]

        # DEBUG PRINT
        st_color = "GREEN" if st_now else "RED"

        # SELL setup
        if last["HA_high"] >= last["BB_upper"] and st_now:
            print(f"[DEBUG ADD SELL] {pair} | ST={st_color} | Close={last['HA_close']:.4f} | ST_Value={last['ST_value']:.4f}")
            return ("SELL", pair)

        # BUY setup
        if last["HA_low"] <= last["BB_lower"] and not st_now:
            print(f"[DEBUG ADD BUY] {pair} | ST={st_color} | Close={last['HA_close']:.4f} | ST_Value={last['ST_value']:.4f}")
            return ("BUY", pair)

        return None

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(process_pair, p) for p in top_pairs]

        for f in as_completed(futures):
            res = f.result()
            if not res:
                continue
            if res[0] == "BUY":
                new_buy.append({"pair": res[1]})
            else:
                new_sell.append({"pair": res[1]})

    buy_watch.extend(new_buy)
    sell_watch.extend(new_sell)

    return buy_watch, sell_watch


# ================= MAIN =================
def main():

    buy_watch = load_watchlist(BUY_FILE)
    sell_watch = load_watchlist(SELL_FILE)

    buy_watch, buy_alerts = check_watchlist_flips(buy_watch, "BUY")
    sell_watch, sell_alerts = check_watchlist_flips(sell_watch, "SELL")

    alerts = buy_alerts + sell_alerts

    top_pairs = get_top_movers(get_active_usdt_coins())
    buy_watch, sell_watch = scan_for_setups(top_pairs, buy_watch, sell_watch)

    if alerts:
        Send_Swing_Telegram_Message("\n\n".join(alerts))

    save_watchlist(BUY_FILE, buy_watch)
    save_watchlist(SELL_FILE, sell_watch)


if __name__ == "__main__":
    main()