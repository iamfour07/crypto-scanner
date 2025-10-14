import os
import hmac
import hashlib
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Alert_Swing import send_telegram_message

# =========================
# CONFIGURATION
# =========================
API_KEY = "xxx"
API_SECRET = "yyy"

resolution = "5m"   # 5-min candles for EMA
limit_hours = 1000
MAX_WORKERS = 10

# Thresholds
BUY_THRESHOLD = -25.0
SELL_THRESHOLD = 25.0

# Watchlist files
BUY_FILE = "BuyWatchlist.json"
SELL_FILE = "SellWatchlist.json"

# EMA settings
EMA_SHORT = 9
EMA_LONG = 100

# =========================
# WATCHLIST HELPERS
# =========================
def load_watchlist(filename):
    if os.path.exists(filename):
        try:
            with open(filename, "r") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()

def save_watchlist(filename, data_set):
    with open(filename, "w") as f:
        json.dump(list(data_set), f)

# =========================
# API HELPERS
# =========================
def sign_payload(secret: str, body: dict):
    payload = json.dumps(body, separators=(",", ":"))
    signature = hmac.new(secret.encode("utf-8"), payload.encode(), hashlib.sha256).hexdigest()
    return payload, signature

def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

def extract_pair(item):
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        return item.get("pair") or item.get("symbol") or item.get("market")
    return None

def fetch_pair_stats(pair: str):
    body = {"timestamp": int(round(time.time() * 1000))}
    payload, signature = sign_payload(API_SECRET, body)
    url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": API_KEY,
        "X-AUTH-SIGNATURE": signature,
    }
    resp = requests.get(url, data=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()

def fetch_candles(pair: str):
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - limit_hours * 3600
    url = "https://public.coindcx.com/market_data/candlesticks"
    params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json().get("data", [])
    if not data:
        return None
    df = pd.DataFrame(data)
    df['close'] = df['close'].astype(float)
    return df

# =========================
# EMA LOGIC
# =========================
def check_ema_cross(df):
    df['ema_short'] = df['close'].ewm(span=EMA_SHORT, adjust=False).mean()
    df['ema_long'] = df['close'].ewm(span=EMA_LONG, adjust=False).mean()
    last = df.iloc[-2]
    prev = df.iloc[-3]
    bullish = prev['ema_short'] < prev['ema_long'] and last['ema_short'] > last['ema_long']
    bearish = prev['ema_short'] > prev['ema_long'] and last['ema_short'] < last['ema_long']
    return bullish, bearish, last['close']

# =========================
# MAIN
# =========================
def main():
    buy_watch = load_watchlist(BUY_FILE)
    sell_watch = load_watchlist(SELL_FILE)

    # Fetch active pairs
    raw = get_active_usdt_coins()
    pairs = list({extract_pair(it) for it in raw if "_USDT" in extract_pair(it)})

    # Fetch 1D% for all and update watchlists
    def get_1d(pair):
        try:
            stats = fetch_pair_stats(pair)
            pc = stats.get("price_change_percent", {})
            return pair, pc.get("1D")
        except:
            return pair, None

    one_d = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(get_1d, p): p for p in pairs}
        for fut in as_completed(futures):
            pair, v = fut.result()
            one_d[pair] = v

    for pair, v in one_d.items():
        if isinstance(v, (int, float)):
            if v <= BUY_THRESHOLD:
                buy_watch.add(pair)
            if v >= SELL_THRESHOLD:
                sell_watch.add(pair)

    # EMA check on watchlist coins
    buy_hits, sell_hits = [], []

    def check_buy(pair):
        try:
            df = fetch_candles(pair)
            if df is not None:
                bullish, _, close = check_ema_cross(df)
                if bullish:
                    return {"pair": pair, "pc_1d": one_d.get(pair), "close": close}
        except:
            pass
        return None

    def check_sell(pair):
        try:
            df = fetch_candles(pair)
            if df is not None:
                _, bearish, close = check_ema_cross(df)
                if bearish:
                    return {"pair": pair, "pc_1d": one_d.get(pair), "close": close}
        except:
            pass
        return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(check_buy, p): ("buy", p) for p in list(buy_watch)}
        futures.update({ex.submit(check_sell, p): ("sell", p) for p in list(sell_watch)})
        for fut in as_completed(futures):
            kind, symbol = futures[fut]
            res = fut.result()
            if res:
                if kind == "buy":
                    buy_hits.append(res)
                    buy_watch.discard(symbol)
                else:
                    sell_hits.append(res)
                    sell_watch.discard(symbol)

    save_watchlist(BUY_FILE, buy_watch)
    save_watchlist(SELL_FILE, sell_watch)

    # Send Telegram
    lines = []
    if buy_hits:
        lines.append("🟢 Buy signals")
        for r in buy_hits:
            lines.append(f"Pair- {r['pair']}")
            lines.append(f"1D%-{r['pc_1d']:.2f}")
            lines.append(f"Close- {r['close']:.4f}")
            lines.append("")
    if sell_hits:
        lines.append("🔴 Sell signals")
        for r in sell_hits:
            lines.append(f"Pair- {r['pair']}")
            lines.append(f"1D%-{r['pc_1d']:.2f}")
            lines.append(f"Close- {r['close']:.4f}")
            lines.append("")

    if lines:
        send_telegram_message("\n".join(lines))

if __name__ == "__main__":
    main()
