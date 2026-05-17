import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

try:
    from Telegram_Momentum import Send_Momentum_Telegram_Message
except ImportError:
    def Send_Momentum_Telegram_Message(msg):
        print(f"\n--- TELEGRAM ---\n{msg}\n---------------")

# ================================================================
#  CONFIG
# ================================================================
TOP_N              = 15
MAX_WORKERS        = 20
RESOLUTION         = "15"       # 15 min timeframe
CANDLE_HOURS       = 100        # 100 hours data (enough for 15min BB)
BB_LENGTH          = 20         # Bollinger Band period
BB_STD             = 2.0        # Standard deviation
RISK_AMOUNT_RS     = 150        # Risk per trade INR
LEVERAGE           = 7
RISK_REWARD_RATIOS = (2, 3)


# ================================================================
#  API HELPERS
# ================================================================
def safe_get(url, params=None, timeout=10):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except:
        return None


def get_active_usdt_pairs():
    url = (
        "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
        "active_instruments?margin_currency_short_name[]=USDT"
    )
    data = safe_get(url, timeout=30)
    if not data:
        return []
    return [item["pair"] if isinstance(item, dict) else item for item in data]


def fetch_pair_stats(pair):
    url  = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
    data = safe_get(url, timeout=8)
    if not data:
        return None
    change = data.get("price_change_percent", {}).get("1D")
    if change is None:
        return None
    return {"pair": pair, "change": float(change)}


def get_top_movers(pairs):
    gainers, losers = [], []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_pair_stats, p): p for p in pairs}
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


# ================================================================
#  HEIKIN ASHI CALCULATION
# ================================================================
def compute_heikin_ashi(df):
    """
    HA Close = (Open + High + Low + Close) / 4
    HA Open  = (Prev HA Open + Prev HA Close) / 2
    HA High  = Max(High, HA Open, HA Close)
    HA Low   = Min(Low,  HA Open, HA Close)

    Green HA : HA Close > HA Open
    Red   HA : HA Close < HA Open
    """
    ha = pd.DataFrame(index=df.index)
    ha["ha_close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4

    ha_open = [0.0] * len(df)
    ha_open[0] = (df["open"].iloc[0] + df["close"].iloc[0]) / 2
    for i in range(1, len(df)):
        ha_open[i] = (ha_open[i-1] + ha["ha_close"].iloc[i-1]) / 2
    ha["ha_open"]  = ha_open
    ha["ha_high"]  = pd.concat([df["high"], ha["ha_open"], ha["ha_close"]], axis=1).max(axis=1)
    ha["ha_low"]   = pd.concat([df["low"],  ha["ha_open"], ha["ha_close"]], axis=1).min(axis=1)
    ha["ha_green"] = ha["ha_close"] > ha["ha_open"]   # True = Green, False = Red

    return ha


# ================================================================
#  FETCH 15MIN DATA + BOLLINGER BANDS + HEIKIN ASHI
# ================================================================
def fetch_data(pair):
    now       = int(datetime.now(timezone.utc).timestamp())
    from_time = now - CANDLE_HOURS * 3600
    # Remove running candle — current 15min candle jo abhi ban rahi hai
    current_15min_ms = (now // 900) * 900 * 1000

    data = safe_get(
        "https://public.coindcx.com/market_data/candlesticks",
        params={
            "pair"      : pair,
            "from"      : from_time,
            "to"        : now,
            "resolution": RESOLUTION,
            "pcode"     : "f",
        },
        timeout=10,
    )
    if not data or "data" not in data:
        return None

    df = pd.DataFrame(data["data"]).sort_values("time")
    df = df[df["time"] < current_15min_ms].reset_index(drop=True)   # running candle remove

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.dropna(subset=["open", "high", "low", "close"], inplace=True)

    if len(df) < BB_LENGTH + 2:
        return None

    # Bollinger Bands — real candles pe calculate
    basis       = df["close"].rolling(BB_LENGTH).mean()
    std         = df["close"].rolling(BB_LENGTH).std(ddof=0)
    df["bb_upper"] = basis + BB_STD * std
    df["bb_lower"] = basis - BB_STD * std

    # Heikin Ashi
    ha = compute_heikin_ashi(df)
    df["ha_open"]  = ha["ha_open"]
    df["ha_close"] = ha["ha_close"]
    df["ha_high"]  = ha["ha_high"]
    df["ha_low"]   = ha["ha_low"]
    df["ha_green"] = ha["ha_green"]

    return df.dropna(subset=["bb_upper", "bb_lower"]).reset_index(drop=True)


# ================================================================
#  SIGNAL CHECK
#
#  BUY SIDE (Top Gainers):
#    prev candle  = Red  HA  AND  Red HA LOW <= Lower BB   ← compulsory
#    last candle  = Green HA  (touch lower BB ya na kare — no issue)
#    Entry : last green HA ka REAL HIGH
#    SL    : prev red  HA ka REAL LOW
#
#  SELL SIDE (Top Losers):
#    prev candle  = Green HA  AND  Green HA HIGH >= Upper BB  ← compulsory
#    last candle  = Red   HA  (touch upper BB ya na kare — no issue)
#    Entry : last red   HA ka REAL LOW
#    SL    : prev green HA ka REAL HIGH
# ================================================================
def check_signal(pair, side):
    df = fetch_data(pair)
    if df is None or len(df) < 2:
        return None

    last = df.iloc[-1]   # last closed 15min candle
    prev = df.iloc[-2]   # usse pehle wali candle

    if side == "buy":
        # prev = Red HA
        prev_is_red   = not prev["ha_green"]
        # prev Red HA ne lower BB touch kiya
        red_touch_lower = prev["ha_low"] <= prev["bb_lower"]
        # last = Green HA
        last_is_green = last["ha_green"]

        if not (prev_is_red and red_touch_lower and last_is_green):
            return None

        entry = round(float(last["high"]), 6)   # Real candle HIGH
        sl    = round(float(prev["low"]),  6)   # Real candle LOW of red candle
        if entry <= sl:
            return None

    else:  # sell
        # prev = Green HA
        prev_is_green    = prev["ha_green"]
        # prev Green HA ne upper BB touch kiya
        green_touch_upper = prev["ha_high"] >= prev["bb_upper"]
        # last = Red HA
        last_is_red      = not last["ha_green"]

        if not (prev_is_green and green_touch_upper and last_is_red):
            return None

        entry = round(float(last["low"]),  6)   # Real candle LOW
        sl    = round(float(prev["high"]), 6)   # Real candle HIGH of green candle
        if sl <= entry:
            return None

    # Position sizing
    risk_per_unit  = abs(entry - sl)
    quantity       = round(RISK_AMOUNT_RS / risk_per_unit, 4)
    position_value = round(quantity * entry, 2)
    capital        = round(position_value / LEVERAGE, 2)

    targets = {
        f"t{r}r": round(entry + r * risk_per_unit, 6) if side == "buy"
                  else round(entry - r * risk_per_unit, 6)
        for r in RISK_REWARD_RATIOS
    }

    return {
        "entry"   : entry,
        "sl"      : sl,
        "quantity": quantity,
        "capital" : capital,
        "targets" : targets,
    }


# ================================================================
#  ALERT MESSAGE
# ================================================================
def build_msg(row, signal, side):
    pair  = row["pair"]
    label = "BUY" if side == "buy" else "SELL"
    e     = signal["entry"]
    sl    = signal["sl"]
    cap   = signal["capital"]
    qty   = signal["quantity"]

    target_lines = "\n".join(
        f"  {k.upper()} → {v}  (₹{round(RISK_AMOUNT_RS * int(k[1]), 2)})"
        for k, v in signal["targets"].items()
    )

    return (
        f"{label} — {pair}\n"
        f"1D Change : {row['change']:.2f}%\n\n"
        f"Entry     : {e}\n"
        f"SL        : {sl}\n"
        f"Capital   : ₹{cap}  ({LEVERAGE}x)\n"
        f"Quantity  : {qty}\n\n"
        f"Targets:\n"
        f"{target_lines}"
    )


# ================================================================
#  MAIN
# ================================================================
def main():
    pairs = get_active_usdt_pairs()
    if not pairs:
        print("No pairs found.")
        return

    gainers, losers = get_top_movers(pairs)
    print(f"Top {TOP_N} Gainers: {[g['pair'] for g in gainers]}")
    print(f"Top {TOP_N} Losers:  {[l['pair'] for l in losers]}")

    alerts = []

    # ── BUY signals — Top Gainers ──
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(check_signal, row["pair"], "buy"): row for row in gainers}
        for f in as_completed(futures):
            row    = futures[f]
            signal = f.result()
            if signal:
                alerts.append(build_msg(row, signal, "buy"))
                print(f"  🟢 BUY signal: {row['pair']}")

    # ── SELL signals — Top Losers ──
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(check_signal, row["pair"], "sell"): row for row in losers}
        for f in as_completed(futures):
            row    = futures[f]
            signal = f.result()
            if signal:
                alerts.append(build_msg(row, signal, "sell"))
                print(f"  🔴 SELL signal: {row['pair']}")

    if alerts:
        msg = "Momentum Alert\n\n" + "\n\n---\n\n".join(alerts)
        Send_Momentum_Telegram_Message(msg)
        print(f"\n{len(alerts)} alert(s) sent.")
    else:
        print("\nNo signals this scan.")


if __name__ == "__main__":
    main()
