import json
import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Alert import send_telegram_message

# =====================
# CONFIG
# =====================
MAX_WORKERS = 15
RESOLUTION = "60"
LIMIT_HOURS = 2000

EMA_10 = 10
EMA_30 = 30
EMA_100 = 100

BUY_FILE  = "ReversalBuyWatchlist.json"
SELL_FILE = "ReversalSellWatchlist.json"

# =====================
# HELPERS
# =====================

def get_active_usdt_coins():
    url = (
        "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
        "active_instruments?margin_currency_short_name[]=USDT"
    )
    return requests.get(url, timeout=30).json()

def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        r = requests.get(url, timeout=10).json()
        pc = r.get("price_change_percent", {}).get("1D")
        return {"pair": pair, "change": float(pc)} if pc else None
    except:
        return None

def fetch_candles(pair):
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        frm = now - LIMIT_HOURS * 3600

        url = "https://public.coindcx.com/market_data/candlesticks"
        params = {
            "pair": pair,
            "from": frm,
            "to": now,
            "resolution": RESOLUTION,
            "pcode": "f",
        }

        data = requests.get(url, params=params, timeout=15).json().get("data", [])
        if len(data) < 200:
            return pair, None

        df = pd.DataFrame(data)
        for c in ["open", "high", "low", "close"]:
            df[c] = pd.to_numeric(df[c])

        df["EMA10"]  = df["close"].ewm(span=EMA_10, adjust=False).mean()
        df["EMA30"]  = df["close"].ewm(span=EMA_30, adjust=False).mean()
        df["EMA100"] = df["close"].ewm(span=EMA_100, adjust=False).mean()

        return pair, df.dropna().reset_index(drop=True)

    except:
        return pair, None

def load_list(path):
    try:
        return json.load(open(path))
    except:
        return []

def save_lists(buy, sell):
    json.dump(buy, open(BUY_FILE, "w"), indent=2)
    json.dump(sell, open(SELL_FILE, "w"), indent=2)

# =====================
# EMA10 ENTRY CONDITIONS
# =====================

def is_ema10_support(c):
    return c["close"] > c["EMA10"] and c["low"] <= c["EMA10"]

def is_ema10_rejection(c):
    return c["close"] < c["EMA10"] and c["high"] >= c["EMA10"]

# =====================
# MAIN
# =====================

def main():

    buy_watch  = load_list(BUY_FILE)
    sell_watch = load_list(SELL_FILE)

    # ---------- TOP GAINERS ----------
    pairs = get_active_usdt_coins()
    changes = []

    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        for f in as_completed([ex.submit(fetch_pair_stats, p) for p in pairs]):
            if f.result():
                changes.append(f.result())

    df = pd.DataFrame(changes)
    gainers = df[df["change"] > 1].sort_values("change", ascending=False)["pair"].tolist()

    # ---------- PARALLEL CANDLE FETCH ----------
    candle_map = {}
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        for f in as_completed([ex.submit(fetch_candles, p) for p in gainers]):
            pair, dfc = f.result()
            if dfc is not None:
                candle_map[pair] = dfc

    # ---------- EMA30–EMA100 PURE CROSS (NO CANDLE LOGIC) ----------
    for pair, dfc in candle_map.items():
        if len(dfc) < 5:
            continue

        e4 = dfc.iloc[-4]
        e3 = dfc.iloc[-3]
        e2 = dfc.iloc[-2]   # latest CLOSED EMA values

        # BUY EMA CROSS
        if (
            e4["EMA30"] <= e4["EMA100"] and
            e3["EMA30"] <= e3["EMA100"] and
            e2["EMA30"] >  e2["EMA100"]
        ):
            if pair not in buy_watch:
                buy_watch.append(pair)

        # SELL EMA CROSS
        if (
            e4["EMA30"] >= e4["EMA100"] and
            e3["EMA30"] >= e3["EMA100"] and
            e2["EMA30"] <  e2["EMA100"]
        ):
            if pair not in sell_watch:
                sell_watch.append(pair)

    # ---------- BUY WATCHLIST ENTRY ----------
    for pair in buy_watch.copy():
        dfc = candle_map.get(pair)
        if dfc is None or len(dfc) < 3:
            continue

        c2 = dfc.iloc[-2]   # closed candle for entry
        if is_ema10_support(c2):
            send_telegram_message(
                f"🟢 BUY SETUP\n{pair}\nEntry: {c2['high']:.4f}\nSL: {c2['low']:.4f}"
            )
            buy_watch.remove(pair)

    # ---------- SELL WATCHLIST ENTRY ----------
    for pair in sell_watch.copy():
        dfc = candle_map.get(pair)
        if dfc is None or len(dfc) < 3:
            continue

        c2 = dfc.iloc[-2]
        if is_ema10_rejection(c2):
            send_telegram_message(
                f"🔴 SELL SETUP\n{pair}\nEntry: {c2['low']:.4f}\nSL: {c2['high']:.4f}"
            )
            sell_watch.remove(pair)

    save_lists(buy_watch, sell_watch)

# =====================
if __name__ == "__main__":
    main()
