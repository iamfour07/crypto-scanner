import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from Telegram_Momentum import Send_Momentum_Telegram_Message

# ── CONFIG ──
TOP_N        = 10    # Scan top N gainers & losers
MAX_WORKERS  = 20
RESOLUTION   = "60"  # 1H candles
CANDLE_HOURS = 250

# ── EMA PERIODS ──
EMA_FAST = 20
EMA_MID  = 50
EMA_SLOW = 200


# ── RSI SETTINGS ──
RSI_LENGTH = 14
RSI_UPPER  = 55   # BUY : RSI crosses above this
RSI_LOWER  = 45   # SELL: RSI crosses below this


# ================= UTIL =================
def safe_get(url, params=None, timeout=10):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


# ================= API =================
def get_active_usdt_coins():
    url = (
        "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
        "active_instruments?margin_currency_short_name[]=USDT"
    )
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
        futures = {executor.submit(fetch_pair_stats, p): p for p in pairs}
        for f in as_completed(futures):
            res = f.result()
            if not res:
                continue
            if res["change"] > 0:
                gainers.append(res)
            elif res["change"] < 0:
                losers.append(res)

    gainers = sorted(gainers, key=lambda x: x["change"], reverse=True)[:TOP_N]
    losers  = sorted(losers,  key=lambda x: x["change"])[:TOP_N]
    return gainers, losers


# ================= CANDLE + INDICATORS =================
def calc_rsi(series, length=RSI_LENGTH):
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs       = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def fetch_indicators(pair):
    """
    Fetch closed candles, compute EMA20/50/200 and RSI(14).
    Returns dict or None on failure.

    RSI cross uses:
      prev = df.iloc[-3]  (current-3)
      curr = df.iloc[-2]  (current-2 — confirmed closed cross candle)
    """
    now             = int(datetime.now(timezone.utc).timestamp())
    from_time       = now - CANDLE_HOURS * 3600
    current_hour_ms = (now // 3600) * 3600 * 1000

    data = safe_get(
        "https://public.coindcx.com/market_data/candlesticks",
        params={"pair": pair, "from": from_time, "to": now,
                "resolution": RESOLUTION, "pcode": "f"},
        timeout=10,
    )
    if not data or "data" not in data:
        return None

    df = pd.DataFrame(data["data"]).sort_values("time")
    df = df[df["time"] < current_hour_ms].reset_index(drop=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df.dropna(subset=["close"], inplace=True)

    if len(df) < EMA_SLOW + 10:
        return None

    df["ema20"]  = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema50"]  = df["close"].ewm(span=EMA_MID,  adjust=False).mean()
    df["ema200"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    df["rsi"]    = calc_rsi(df["close"])

    if len(df) < 3:
        return None

    prev = df.iloc[-3]   # current-3
    curr = df.iloc[-2]   # current-2 (cross candle)

    return {
        "close":      float(curr["close"]),
        "ema20":      float(curr["ema20"]),
        "ema50":      float(curr["ema50"]),
        "ema200":     float(curr["ema200"]),
        "prev_rsi":   float(prev["rsi"]),
        "curr_rsi":   float(curr["rsi"]),
    }


# ================= SCAN =================
def scan(rows, side):
    """
    BUY  : EMA20 > EMA50 > EMA200  AND  RSI crossed above 55 (prev < 55, curr > 55)
    SELL : EMA20 < EMA50 < EMA200  AND  RSI crossed below 45 (prev > 45, curr < 45)
    """
    results = {}

    def fetch(row):
        return row["pair"], fetch_indicators(row["pair"])

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch, r): r for r in rows}
        for f in as_completed(futures):
            pair, ind = f.result()
            results[pair] = ind

    matched = []
    for row in rows:
        ind = results.get(row["pair"])
        if not ind:
            continue

        ema_aligned = (
            ind["ema20"] > ind["ema50"] > ind["ema200"]  if side == "BUY"
            else ind["ema20"] < ind["ema50"] < ind["ema200"]
        )
        rsi_cross = (
            ind["prev_rsi"] < RSI_UPPER and ind["curr_rsi"] > RSI_UPPER  if side == "BUY"
            else ind["prev_rsi"] > RSI_LOWER and ind["curr_rsi"] < RSI_LOWER
        )

        if ema_aligned and rsi_cross:
            matched.append({**row, **ind})

    return matched


# ================= PRINT / ALERT =================
def send_alerts(buy_signals, sell_signals):
    if not buy_signals and not sell_signals:
        return

    parts = []
    for s in buy_signals:
        parts.append(
            f"🟢 BUY  {s['pair']}\n"
            f"Close  : {s['close']}\n"
            f"{'─'*35}"
        )
    for s in sell_signals:
        parts.append(
            f"🔴 SELL {s['pair']}\n"
            f"Close  : {s['close']}\n"
            f"{'─'*35}"
        )

    Send_Momentum_Telegram_Message("\n".join(parts))


# ================= MAIN =================
def main():
    pairs = get_active_usdt_coins()
    if not pairs:
        return

    gainers, losers = get_top_movers(pairs)

    buy_signals  = scan(gainers, "BUY")
    sell_signals = scan(losers,  "SELL")

    send_alerts(buy_signals, sell_signals)


if __name__ == "__main__":
    main()
