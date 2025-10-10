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

# Candle settings (match prior working code)
resolution = "60"   # 1-hour candles
limit_hours = 1000  # history window for from/to
IST = timezone(timedelta(hours=5, minutes=30))

MAX_WORKERS = 10

# =========================
# DISCOVERY
# =========================
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()  # active USDT-margined futures [web:1]

def extract_pair(item):
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        return item.get("pair") or item.get("symbol") or item.get("market")
    return None

# =========================
# AUTH / SIGN
# =========================
def sign_payload(secret: str, body: dict) -> tuple[str, str]:
    payload = json.dumps(body, separators=(",", ":"))
    signature = hmac.new(secret.encode("utf-8"), payload.encode(), hashlib.sha256).hexdigest()
    return payload, signature

# =========================
# API CALLS
# =========================
def fetch_pair_stats(pair: str):
    # Signed stats (provides price_change_percent with "1D") [web:1]
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
    # Public candlesticks with from/to, resolution=60, pcode=f [web:1][web:3]
    now = int(datetime.now(timezone.utc).timestamp())  # seconds
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

def fetch_prev_close_via_from_to(pair: str):
    df = fetch_candles_via_from_to(pair)
    if df is None or len(df) < 2:
        return None
    prev = df.iloc[-2]
    return float(prev["close"])

# =========================
# EMA / CROSSOVER
# =========================
def compute_ema(df: pd.DataFrame, span: int, col: str = "close", out: str = None):
    if out is None:
        out = f"ema_{span}"
    df[out] = df[col].ewm(span=span, adjust=False).mean()
    return df

def get_recent_crossover(pair: str):
    """
    Determine if the most recent closed candle has a fresh EMA9/EMA100 crossover.
    Returns a tuple (cross_down, cross_up):
      - cross_down = EMA9 crossed below EMA100 on the last closed candle
      - cross_up   = EMA9 crossed above EMA100 on the last closed candle
    """
    df = fetch_candles_via_from_to(pair)
    if df is None:
        return (False, False)
    # Need at least 120 + 2 bars to stabilize EMA100 and compare last two bars
    if len(df) < 122:
        return (False, False)

    compute_ema(df, 9, "close", "ema9")
    compute_ema(df, 100, "close", "ema100")

    # Use the last two completed candles (prev = -2, last = -1)
    prev = df.iloc[-2]
    last = df.iloc[-1]
    prev_diff = prev["ema9"] - prev["ema100"]
    last_diff = last["ema9"] - last["ema100"]

    cross_down = (prev_diff >= 0) and (last_diff < 0)
    cross_up   = (prev_diff <= 0) and (last_diff > 0)
    return (cross_down, cross_up)

# =========================
# PROCESS
# =========================
def process_pair(pair: str):
    row = {"pair": pair, "prev_close": None, "pc_1d": None, "error": None}
    try:
        stats = fetch_pair_stats(pair)
        pc = stats.get("price_change_percent", {})
        row["pc_1d"] = pc.get("1D")
    except Exception as e:
        row["error"] = f"stats_failed: {e}"

    try:
        row["prev_close"] = fetch_prev_close_via_from_to(pair)
    except Exception as e:
        row["error"] = (row["error"] + f"; candles_failed: {e}") if row["error"] else f"candles_failed: {e}"
    return row

# =========================
# OUTPUT HELPERS
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
    # 1) Discover pairs [web:1]
    raw = get_active_usdt_coins()
    pairs = []
    for it in raw:
        p = extract_pair(it)
        if isinstance(p, str) and "_USDT" in p:
            pairs.append(p)
    pairs = list(dict.fromkeys(pairs))

    # 2) Process all (stats + prev close)
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(process_pair, p): p for p in pairs}
        for fut in as_completed(futs):
            results.append(fut.result())

    # 3) Split by 1D%
    winners, losers = [], []
    for r in results:
        pc1d = r.get("pc_1d")
        if isinstance(pc1d, (int, float)):
            if pc1d > 0:
                winners.append(r)
            elif pc1d < 0:
                losers.append(r)

    # Sort by 1D%
    winners.sort(key=lambda x: x.get("pc_1d") if isinstance(x.get("pc_1d"), (int, float)) else float("-inf"), reverse=True)
    losers.sort(key=lambda x: x.get("pc_1d") if isinstance(x.get("pc_1d"), (int, float)) else float("inf"))

    # 4) Apply EMA filters for recent crossover
    # For Gainers (buy): EMA9 just crossed below EMA100
    filtered_gainers = []
    for r in winners:
        try:
            down, up = get_recent_crossover(r["pair"])
            if down:
                filtered_gainers.append(r)
        except Exception:
            pass

    # For Losers (sell): EMA9 just crossed above EMA100
    filtered_losers = []
    for r in losers:
        try:
            down, up = get_recent_crossover(r["pair"])
            if up:
                filtered_losers.append(r)
        except Exception:
            pass

    # Keep top 5 after EMA filters
    top5_gainers = winners[:5]   # highest positive 1D%
    top5_losers  = losers[:5]    # most negative 1D%

    # 4) Apply EMA filters only to these 10 coins
    filtered_gainers = []
    for r in top5_gainers:
        try:
            down, up = get_recent_crossover(r["pair"])
            # For Gainers (buy): EMA9 just crossed below EMA100
            if down:
                filtered_gainers.append(r)
        except Exception:
            pass

    filtered_losers = []
    for r in top5_losers:
        try:
            down, up = get_recent_crossover(r["pair"])
            # For Losers (sell): EMA9 just crossed above EMA100
            if up:
                filtered_losers.append(r)
        except Exception:
            pass

    # 5) Build requested format
    if filtered_gainers or filtered_losers:

        lines = []

        if filtered_gainers:
            lines.append("🏆 Gainers (buy)")
            for r in filtered_gainers:
                pair = r["pair"]
                pc1d = fmt_pct(r.get("pc_1d"))
                close = fmt_price(r.get("prev_close"))
                lines.append(f"Pair- {pair}")
                lines.append(f"1D%-{pc1d}")
                lines.append(f"close price- {close}")
                lines.append("")

        if filtered_losers:
            lines.append("🔻Losers (sell)")
            for r in filtered_losers:
                pair = r["pair"]
                pc1d = fmt_pct(r.get("pc_1d"))
                close = fmt_price(r.get("prev_close"))
                lines.append(f"Pair- {pair}")
                lines.append(f"1D%-{pc1d}")
                lines.append(f"close price- {close}")
                lines.append("")
        send_telegram_message("\n".join(lines))

if __name__ == "__main__":
    main()
