import os
import hmac
import hashlib
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Alert import send_telegram_message

# =========================
# CONFIGURATION
# =========================
API_KEY = "xxx"
API_SECRET = "yyy"

# Candles
resolution = "60"    # 1-hour candles
limit_hours = 500    # enough for stable EMA100, faster than 1000
IST = timezone(timedelta(hours=5, minutes=30))
MAX_WORKERS = 10

# Thresholds
BUY_THRESHOLD = -25.0   # 1D% <= -15 goes into BuyWatchlist
SELL_THRESHOLD = 25.0   # 1D% >= +15 goes into SellWatchlist

# Watchlist files
BUY_FILE  = "BuyWatchlist.json"
SELL_FILE = "SellWatchlist.json"

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
def sign_payload(secret: str, body: dict) -> tuple[str, str]:
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

def fetch_candles_via_from_to(pair: str):
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
    for col in ("open", "high", "low", "close"):
        if col in df.columns:
            df[col] = df[col].astype(float)
    return df

# =========================
# EMA LOGIC
# =========================
def ema_cols(df: pd.DataFrame):
    df["ema9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema100"] = df["close"].ewm(span=100, adjust=False).mean()
    return df

def just_crossed_above_on_prev(df: pd.DataFrame) -> bool:
    """
    Check if EMA9 just crossed ABOVE EMA100 on the previous closed candle (current-1).
    That means looking at bars prev-1 and prev:
      prev-1: ema9 <= ema100
      prev:   ema9 >  ema100
    """
    if df is None or len(df) < 122:
        return False
    df = ema_cols(df)
    prev_idx = -2
    prev1_idx = -3
    prev1_diff = df["ema9"].iloc[prev1_idx] - df["ema100"].iloc[prev1_idx]
    prev_diff  = df["ema9"].iloc[prev_idx]  - df["ema100"].iloc[prev_idx]
    return (prev1_diff <= 0) and (prev_diff > 0)

def just_crossed_below_on_prev(df: pd.DataFrame) -> bool:
    """
    Check if EMA9 just crossed BELOW EMA100 on the previous closed candle (current-1).
    That means looking at bars prev-1 and prev:
      prev-1: ema9 >= ema100
      prev:   ema9 <  ema100
    """
    if df is None or len(df) < 122:
        return False
    df = ema_cols(df)
    prev_idx = -2
    prev1_idx = -3
    prev1_diff = df["ema9"].iloc[prev1_idx] - df["ema100"].iloc[prev1_idx]
    prev_diff  = df["ema9"].iloc[prev_idx]  - df["ema100"].iloc[prev_idx]
    return (prev1_diff >= 0) and (prev_diff < 0)

# =========================
# FORMAT
# =========================
def fmt_pct(x):
    return "" if x is None else f"{x:.2f}"

def fmt_price(x):
    if x is None:
        return ""
    try:
        x = float(x)
    except:
        return ""
    return f"{x:.4f}" if x < 1 else f"{x:.2f}"

# =========================
# MAIN
# =========================
def main():
    # Load persistent watchlists
    buy_watch  = load_watchlist(BUY_FILE)
    sell_watch = load_watchlist(SELL_FILE)

    # 1) Active pairs
    raw = get_active_usdt_coins()
    pairs = []
    for it in raw:
        p = extract_pair(it)
        if isinstance(p, str) and "_USDT" in p:
            pairs.append(p)
    pairs = list(dict.fromkeys(pairs))

    # 2) Fetch 1D% for all, and candles only for symbols to be checked later
    def get_1d(pair):
        try:
            stats = fetch_pair_stats(pair)
            pc = stats.get("price_change_percent", {})
            return pair, pc.get("1D")
        except Exception:
            return pair, None

    one_d = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(get_1d, p): p for p in pairs}
        for fut in as_completed(futs):
            pair, v = fut.result()
            one_d[pair] = v

    # 3) Update watchlists by thresholds
    # BuyWatchlist: 1D% <= -15
    # SellWatchlist: 1D% >= +15
    for pair, v in one_d.items():
        if isinstance(v, (int, float)):
            if v <= BUY_THRESHOLD:
                buy_watch.add(pair)
            if v >= SELL_THRESHOLD:
                sell_watch.add(pair)

    # 4) EMA checks only on the current contents of watchlists
    def check_buy(pair):
        # 9 EMA just crossed above 100 EMA on previous candle
        try:
            df = fetch_candles_via_from_to(pair)
            if just_crossed_above_on_prev(df):
                prev_close = float(df["close"].iloc[-2]) if df is not None and len(df) >= 2 else None
                return {"pair": pair, "pc_1d": one_d.get(pair), "prev_close": prev_close}
        except Exception:
            pass
        return None

    def check_sell(pair):
        # 9 EMA just crossed below 100 EMA on previous candle
        try:
            df = fetch_candles_via_from_to(pair)
            if just_crossed_below_on_prev(df):
                prev_close = float(df["close"].iloc[-2]) if df is not None and len(df) >= 2 else None
                return {"pair": pair, "pc_1d": one_d.get(pair), "prev_close": prev_close}
        except Exception:
            pass
        return None

    buy_hits, sell_hits = [], []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(check_buy, p): ("buy", p) for p in list(buy_watch)}
        futs.update({ex.submit(check_sell, p): ("sell", p) for p in list(sell_watch)})
        for fut in as_completed(futs):
            kind, symbol = futs[fut]
            res = fut.result()
            if res:
                if kind == "buy":
                    buy_hits.append(res)
                    buy_watch.discard(symbol)  # remove only when signal triggers
                else:
                    sell_hits.append(res)
                    sell_watch.discard(symbol)

    # 5) Persist updated watchlists
    save_watchlist(BUY_FILE, buy_watch)
    save_watchlist(SELL_FILE, sell_watch)

    # 6) Send signals if any
    lines = []
    if buy_hits:
        lines.append("🟢 Buy signals")
        for r in buy_hits:
            pair = r["pair"]
            pc1d = fmt_pct(r.get("pc_1d"))
            close = fmt_price(r.get("prev_close"))
            lines.append(f"Pair- {pair}")
            lines.append(f"1D%-{pc1d}")
            lines.append(f"close price- {close}")
            lines.append("")
    if sell_hits:
        lines.append("🔴 Sell signals")
        for r in sell_hits:
            pair = r["pair"]
            pc1d = fmt_pct(r.get("pc_1d"))
            close = fmt_price(r.get("prev_close"))
            lines.append(f"Pair- {pair}")
            lines.append(f"1D%-{pc1d}")
            lines.append(f"close price- {close}")
            lines.append("")
    if lines:
        send_telegram_message("\n".join(lines))
        # print("\n".join(lines))
    # Silent if no signals this scan

if __name__ == "__main__":
    main()
