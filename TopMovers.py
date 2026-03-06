import requests
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from Telegram_Swing import Send_Swing_Telegram_Message
import os

# ── CONFIG ──
TOP_N        = 20    # Scan top N gainers & losers
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
RSI_BUY_ACTIVATION = 45   # Buy watch activation when RSI goes below this
RSI_SELL_ACTIVATION = 55  # Sell watch activation when RSI goes above this
RSI_REENTRY_LEVEL = 50  # Pullback re-entry trigger for alert


# ================= UTIL =================
def safe_get(url, params=None, timeout=10):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


# ================= JSON LOAD/SAVE =================
def load_json(filename):
    if not os.path.exists(filename):
        return []
    with open(filename, 'r') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []

def save_to_json(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)


def normalize_watch_item(item):
    """Keep watchlist schema minimal and consistent."""
    return {
        "pair": item.get("pair", ""),
        "prev_rsi": item.get("prev_rsi", ""),
        "curr_rsi": item.get("curr_rsi", ""),
    }

# ================= ACTIVE TRACKING SCAN =================
def scan_active_tracking(filename, side):
    """
    Reads existing JSON. Checks RSI for each coin.
    BUY side:
      - If prev_rsi == "", waits for RSI to cross BELOW RSI_BUY_ACTIVATION. Once it does, sets prev_rsi.
      - If prev_rsi != "", waits for RSI to cross ABOVE RSI_REENTRY_LEVEL. Once it does, sets curr_rsi, ALERTS and REMOVES.
    SELL side:
      - If prev_rsi == "", waits for RSI to cross ABOVE RSI_SELL_ACTIVATION. Once it does, sets prev_rsi.
      - If prev_rsi != "", waits for RSI to cross BELOW RSI_REENTRY_LEVEL. Once it does, sets curr_rsi, ALERTS and REMOVES.
    """
    items = [normalize_watch_item(x) for x in load_json(filename)]
    if not items:
        return items
        
    updated_items = []
    alerts = []
    
    # Check all active items
    def fetch(item):
        return item, fetch_indicators(item["pair"])
        
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch, it): it for it in items}
        for f in as_completed(futures):
            item, ind = f.result()
            if not ind:
                    updated_items.append(normalize_watch_item(item))
                    continue
                
            pair = item["pair"]
            remove_item = False
            
            if side == "BUY":
                # Wait for RSI drop below buy activation level
                if item["prev_rsi"] == "":
                    if ind["curr_rsi"] < RSI_BUY_ACTIVATION:
                        item["prev_rsi"] = round(ind["curr_rsi"], 2)
                        item["curr_rsi"] = ""
                # Wait for RSI recovering above re-entry level for alert/exit
                elif item["prev_rsi"] != "":
                    if ind["curr_rsi"] > RSI_REENTRY_LEVEL:
                        item["curr_rsi"] = round(ind["curr_rsi"], 2)
                        alerts.append(
                            f"🟢 BUY ALERT: {pair} - prev_rsi={item['prev_rsi']}, curr_rsi={item['curr_rsi']}"
                        )
                        remove_item = True
                        
            elif side == "SELL":
                # Wait for RSI spike above sell activation level
                if item["prev_rsi"] == "":
                    if ind["curr_rsi"] > RSI_SELL_ACTIVATION:
                        item["prev_rsi"] = round(ind["curr_rsi"], 2)
                        item["curr_rsi"] = ""
                # Wait for RSI dropping below re-entry level for alert/exit
                elif item["prev_rsi"] != "":
                    if ind["curr_rsi"] < RSI_REENTRY_LEVEL:
                        item["curr_rsi"] = round(ind["curr_rsi"], 2)
                        alerts.append(
                            f"🔴 SELL ALERT: {pair} - prev_rsi={item['prev_rsi']}, curr_rsi={item['curr_rsi']}"
                        )
                        remove_item = True
                        
            if not remove_item:
                updated_items.append(normalize_watch_item(item))
                
    # Send all alerts found
    if alerts:
        msg = "\n".join(alerts)
        print(f"\n📤 Sending Telegram alerts for tracked coins:\n{msg}")
        Send_Swing_Telegram_Message(msg)
        
    return [normalize_watch_item(x) for x in updated_items]


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
      prev = df.iloc[-2]  (second last closed candle)
      curr = df.iloc[-1]  (last closed candle — fires immediately on close)
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

    prev = df.iloc[-2]   # second last closed candle
    curr = df.iloc[-1]   # last closed candle (most recent confirmed)

    return {
        "close":      float(curr["close"]),
        "ema20":      float(curr["ema20"]),
        "ema50":      float(curr["ema50"]),
        "ema200":     float(curr["ema200"]),
        "prev_ema50": float(prev["ema50"]),
        "prev_ema200": float(prev["ema200"]),
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


# ================= EMA-ONLY SCAN (ALL PAIRS) =================
def scan_pullback_ema(gainer_rows, loser_rows):
    """
    Checks EMA alignment only (no RSI filter) on Top-N gainers & losers.
      bullish : EMA20 > EMA50 > EMA200  (checked on gainers)
      bearish : EMA20 < EMA50 < EMA200  (checked on losers)
    """
    all_rows = gainer_rows + loser_rows
    results  = {}

    def fetch(row):
        return row["pair"], fetch_indicators(row["pair"])

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch, r): r for r in all_rows}
        for f in as_completed(futures):
            pair, ind = f.result()
            results[pair] = ind

    buy_signals, sell_signals = [], []
    for row in gainer_rows:
        ind = results.get(row["pair"])
        if ind:
            is_20_above_50 = ind["ema20"] > ind["ema50"]
            is_50_cross_above_200 = (ind["prev_ema50"] < ind["prev_ema200"]) and (ind["ema50"] > ind["ema200"])
            
            if is_20_above_50 and is_50_cross_above_200:
                buy_signals.append({
                    "pair": row["pair"],
                    "prev_rsi": "",
                    "curr_rsi": ""
                })

    for row in loser_rows:
        ind = results.get(row["pair"])
        if ind:
            is_20_below_50 = ind["ema20"] < ind["ema50"]
            is_50_cross_below_200 = (ind["prev_ema50"] > ind["prev_ema200"]) and (ind["ema50"] < ind["ema200"])
            
            if is_20_below_50 and is_50_cross_below_200:
                sell_signals.append({
                    "pair": row["pair"],
                    "prev_rsi": "",
                    "curr_rsi": ""
                })

    buy_signals.sort(key=lambda x: x["pair"])
    sell_signals.sort(key=lambda x: x["pair"])
    return buy_signals, sell_signals


# ================= MAIN =================
def main():
    pairs = get_active_usdt_coins()
    if not pairs:
        return

    # ── 1. Process existing active tracked coins ──
    print("Scanning active tracked coins for RSI triggers...")
    tracked_buy = scan_active_tracking("PullBackBuy.json", "BUY")
    tracked_sell = scan_active_tracking("PullBackSell.json", "SELL")

    # ── Top-N movers ──
    print(f"Fetching Top-{TOP_N} Gainers & Losers...")
    gainers, losers = get_top_movers(pairs)

    # ── 2. EMA Pullback Scan for NEW coins ──
    print("Checking EMA Pullback Strategy conditions for new coins...")
    new_buy_signals, new_sell_signals = scan_pullback_ema(gainers, losers)
    
    # ── Merge new coins into tracked list ──
    tracked_buy_pairs = {x["pair"] for x in tracked_buy}
    for new_b in new_buy_signals:
        if new_b["pair"] not in tracked_buy_pairs:
            tracked_buy.append(new_b)
            
    tracked_sell_pairs = {x["pair"] for x in tracked_sell}
    for new_s in new_sell_signals:
        if new_s["pair"] not in tracked_sell_pairs:
            tracked_sell.append(new_s)
    
    # ── Save updated lists to JSON ──
    save_to_json("PullBackBuy.json", [normalize_watch_item(x) for x in tracked_buy])
    save_to_json("PullBackSell.json", [normalize_watch_item(x) for x in tracked_sell])
    
    print(f"✅ Saved total {len(tracked_buy)} tracked pairs to PullBackBuy.json")
    print(f"✅ Saved total {len(tracked_sell)} tracked pairs to PullBackSell.json")


if __name__ == "__main__":
    main()
