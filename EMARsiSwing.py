"""
=====================================================================================
CoinDCX Futures — Top-Gainer EMA 9 Breakdown Scanner (SHORT)
=====================================================================================

Strategy   : Fade extended pumps once price closes below EMA 9 (SHORT-only)
Candles    : normal 1H candlesticks (no Heikin Ashi)
Watchlist  : top gainers (24h change > MIN_GAINER_PCT)
Signal     : first closed 1H candle that closes below EMA 9
Risk       : Fixed Rs.100 risk per trade (margin capped at MAX_CAPITAL_INR)
Leverage   : 7x (fixed)

Flow (every run — designed to be triggered hourly via cron)
-------------------------------------------------------------------------------------
STEP 1 - Watchlist update (ALWAYS runs):
    - Pull 24h % change for every active USDT-M futures pair.
    - Keep pairs with change > MIN_GAINER_PCT (30%), top TOP_N (10) by change.
    - Add them to ReversalWatchlist.json (no duplicates).

STEP 2 - EMA 9 check (for every pair on the watchlist):
    - Setup: the LAST closed 1H candle closes below EMA 9, and the candle
      before it closed at/above its EMA 9 (= first close below EMA 9).
    - Entry   = LOW of that candle (sell below it)
    - SL      = previous confirmed swing high (pivot high with SWING_LEFT_BARS
                candles on the left and SWING_RIGHT_BARS closed candles on the
                right), the most recent one above the entry.
                No swing high -> no alert.
    - Targets = 2R / 3R / 4R below entry.
    - On a successful alert the pair is removed from the watchlist. If the
      Telegram send fails, the pair is kept.
=====================================================================================
"""

import json
import os
import requests
import pandas as pd
import logging

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from Telegram_EMA import Send_EMA_Telegram_Message
except ImportError:
    def Send_EMA_Telegram_Message(msg):
        print(f"\n--- TELEGRAM ALERT ---\n{msg}\n----------------------")
        return False


# =====================================================================================
# LOGGING
# =====================================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


# =====================================================================================
# CONFIG
# =====================================================================================

# ---- Watchlist entry filter ----
TOP_N               = 10            # top N gainers considered each run
MIN_GAINER_PCT       = 30.0          # only pairs with 24h change strictly above this

# ---- EMA ----
EMA_LENGTH           = 9

# ---- Swing high (stop loss) ----
SWING_LEFT_BARS      = 3            # pivot high: higher than N candles on the left ...
SWING_RIGHT_BARS     = 3            # ... and N closed candles on the right (confirmed)
SWING_MAX_LOOKBACK   = 10           # search the last N 1H candles for the swing high

# ---- Risk management ----
RISK_INR             = 100          # fixed Rs. risk per trade
LEVERAGE             = 7            # fixed leverage
MAX_CAPITAL_INR      = 3000         # maximum margin/capital allowed per trade
INR_TO_USDT_RATE     = None         # None = fetch live

# ---- Candles ----
RESOLUTION           = "60"         # 1H candles for everything
HOURLY_LOOKBACK_HOURS = 300         # enough 1H history for EMA 9 + swing detection

# ---- Threading ----
MAX_WORKERS          = 15

# ---- Watchlist file ----
WATCHLIST_FILE       = "ReversalWatchlist.json"

# ---- API endpoints ----
ACTIVE_INSTRUMENTS_URL = (
    "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
    "active_instruments?margin_currency_short_name[]=USDT"
)
STATS_URL_TMPL = "https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
CANDLES_URL = "https://public.coindcx.com/market_data/candlesticks"
TICKER_URL = "https://api.coindcx.com/exchange/ticker"


# =====================================================================================
# GENERIC HELPERS
# =====================================================================================

def safe_get(url, params=None, timeout=15):
    """Wrapper around requests.get() that never raises — returns None on any failure."""
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.debug(f"safe_get failed for {url}: {e}")
        return None


def load_watchlist(file):
    """Load the watchlist JSON file. Returns [] if missing/corrupt.

    Each entry is a dict: {"pair": <str>}.
    """
    if not os.path.exists(file):
        return []
    try:
        with open(file, "r") as f:
            data = json.load(f)
    except Exception:
        return []

    normalized = []
    for item in data:
        if isinstance(item, dict) and "pair" in item:
            normalized.append({"pair": item["pair"]})
        elif isinstance(item, str):
            normalized.append({"pair": item})
    return normalized


def save_watchlist(file, data):
    """Overwrite the watchlist JSON file, de-duplicated by pair name."""
    deduped = {}
    for entry in data:
        pair = entry["pair"]
        if pair not in deduped:
            deduped[pair] = entry
    with open(file, "w") as f:
        json.dump(list(deduped.values()), f, indent=2)


# =====================================================================================
# TOP GAINERS (24h % change)
# =====================================================================================

def get_active_usdt_pairs():
    """Fetches every active USDT-margined futures pair from CoinDCX."""
    data = safe_get(ACTIVE_INSTRUMENTS_URL, timeout=30)
    if not data:
        return []
    pairs = []
    for item in data:
        if isinstance(item, dict):
            pair = item.get("pair") or item.get("symbol") or item.get("instrument")
        else:
            pair = item
        if pair:
            pairs.append(pair)
    return pairs


def fetch_pair_change(pair):
    """Returns {"pair": pair, "change": float} or None."""
    data = safe_get(STATS_URL_TMPL.format(pair=pair), timeout=8)
    if not data:
        return None
    change = data.get("price_change_percent", {}).get("1D")
    if change is None:
        return None
    return {"pair": pair, "change": float(change)}


def get_top_gainers(pairs):
    """
    Returns the top TOP_N pairs by 24h % change, filtered to change > MIN_GAINER_PCT.
    """
    gainers = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_pair_change, p): p for p in pairs}
        for f in as_completed(futures):
            result = f.result()
            if result and result["change"] > MIN_GAINER_PCT:
                gainers.append(result)

    gainers = sorted(gainers, key=lambda x: x["change"], reverse=True)[:TOP_N]
    return gainers


# =====================================================================================
# CANDLE FETCHING (normal candlesticks)
# =====================================================================================

def _resolution_to_seconds(resolution):
    if resolution == "1D":
        return 86400
    try:
        return int(resolution) * 60
    except (TypeError, ValueError):
        return None


def _normalize_candle_time(raw_time):
    if pd.isna(raw_time):
        return None
    try:
        ts = int(raw_time)
    except (TypeError, ValueError):
        return None
    if ts > 10**12:  # ms -> s
        ts //= 1000
    return ts


def _drop_incomplete_last_candle(df, resolution):
    """Removes the final row only if that candle is still forming right now."""
    if df.empty:
        return df

    candle_seconds = _resolution_to_seconds(resolution)
    last_candle_time = _normalize_candle_time(df.iloc[-1].get("time"))

    if candle_seconds is None or last_candle_time is None:
        return df.iloc[:-1].reset_index(drop=True)

    now_ts = int(datetime.now(timezone.utc).timestamp())
    last_candle_close_time = last_candle_time + candle_seconds

    if now_ts < last_candle_close_time:
        return df.iloc[:-1].reset_index(drop=True)

    return df.reset_index(drop=True)


def fetch_raw_candles(pair, resolution=RESOLUTION):
    """Fetches raw OHLC candles, drops the still-forming candle if present."""
    now = int(datetime.now(timezone.utc).timestamp())
    params = {
        "pair": pair,
        "from": now - HOURLY_LOOKBACK_HOURS * 3600,
        "to": now,
        "resolution": resolution,
        "pcode": "f",
    }

    data = safe_get(CANDLES_URL, params=params)
    if not data or "data" not in data or not data["data"]:
        return None

    df = pd.DataFrame(data["data"]).sort_values("time").reset_index(drop=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = _drop_incomplete_last_candle(df, resolution)
    df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)

    if len(df) < max(EMA_LENGTH, SWING_LEFT_BARS + SWING_RIGHT_BARS) + 2:
        return None

    return df


# =====================================================================================
# INDICATORS — EMA 9 + swing high
# =====================================================================================

def add_ema(df, length=EMA_LENGTH):
    df["EMA"] = df["close"].ewm(span=length, adjust=False, min_periods=length).mean()
    return df


def find_swing_high(df, upto_idx, left=SWING_LEFT_BARS, right=SWING_RIGHT_BARS,
                    max_lookback=SWING_MAX_LOOKBACK, above=None):
    """
    Index of the most recent CONFIRMED pivot high at or before `upto_idx`, else None.
    Confirmed = its `right` candles are all closed (i + right <= upto_idx).
    Pivot: high[i] > every high in the `left` candles before it and >= every high in
    the `right` candles after it. With `above`, the pivot high must also be > above.
    """
    highs = df["high"].to_numpy()
    stop = max(left, upto_idx - max_lookback)
    for i in range(upto_idx - right, stop - 1, -1):
        h = highs[i]
        if above is not None and h <= above:
            continue
        if h > highs[i - left:i].max() and h >= highs[i + 1:i + right + 1].max():
            return i
    return None


# =====================================================================================
# RISK / POSITION SIZING (fixed Rs.100 risk, 7x leverage, INR only)
# =====================================================================================

def get_inr_rate():
    """
    Live USDT->INR conversion rate. Used ONLY internally to size the position
    correctly against a Rs.-denominated risk budget — never shown to the user.
    """
    if INR_TO_USDT_RATE is not None:
        return INR_TO_USDT_RATE
    try:
        data = safe_get(TICKER_URL, timeout=5)
        if data:
            for m in data:
                if m.get("market") == "USDTINR":
                    return float(m.get("last_price"))
    except Exception:
        pass
    return 88.0  # emergency fallback only, used if the ticker call fails


def calc_position(entry, sl):
    """
    Fixed-risk position sizing with a maximum INR capital/margin cap.

    Risk target = RISK_INR.
    Final margin is capped at MAX_CAPITAL_INR.
    If the capital cap is reached, actual risk becomes lower than RISK_INR.
    """
    risk_abs = abs(entry - sl)

    if entry <= 0 or risk_abs <= 0:
        return None

    # USDT -> INR conversion, used internally for USDT-margined futures.
    rate = get_inr_rate()

    # Convert the fixed INR risk budget to USDT.
    risk_usdt = RISK_INR / rate

    # Risk-based position value:
    # Risk = Position Value * (SL distance / Entry)
    sl_pct_decimal = risk_abs / entry
    position_usdt_risk = risk_usdt / sl_pct_decimal

    # Maximum position value allowed by the INR capital/margin cap:
    # Margin = Position Value / Leverage
    max_capital_usdt = MAX_CAPITAL_INR / rate
    position_usdt_cap = max_capital_usdt * LEVERAGE

    # Use the smaller position size.
    position_usdt = min(position_usdt_risk, position_usdt_cap)

    # Final margin/capital.
    capital_usdt = position_usdt / LEVERAGE
    capital_inr = capital_usdt * rate

    # Quantity.
    quantity = position_usdt / entry

    # Actual INR risk after applying the capital cap.
    actual_risk_usdt = quantity * risk_abs
    actual_risk_inr = actual_risk_usdt * rate

    return {
        "capital_inr": round(capital_inr, 2),
        "capital_usdt": round(capital_usdt, 2),
        "quantity": round(quantity, 8),
        "actual_risk_inr": round(actual_risk_inr, 2),
    }


# =====================================================================================
# ALERT MESSAGE (INR only)
# =====================================================================================

def build_short_msg(pair, entry, sl, t2, t3, t4):
    pos = calc_position(entry, sl)

    if not pos:
        return None

    return (
        f"Name - {pair}\n"
        f"Entry (sell below) - {entry}\n"
        f"SL (swing high) - {sl}\n"
        f"Quantity - {pos['quantity']:.6g}\n"
        f"Capital - Rs.{pos['capital_inr']:.2f}\n"
        f"Risk per trade - Rs.{pos['actual_risk_inr']:.2f}\n"
        f"\n"
        f"-----------------\n"
        f"T1 - {t2}\n"
        f"T2 - {t3}\n"
        f"T3 - {t4}"
  )


# =====================================================================================
# STEP 1 — WATCHLIST UPDATE
# =====================================================================================

def update_watchlist():
    """
    STEP 1 — always runs.
    Adds new top gainers (>MIN_GAINER_PCT change) to the watchlist.
    Pairs already on the watchlist are skipped (no duplicate).
    """
    watchlist = load_watchlist(WATCHLIST_FILE)
    existing_pairs = {e["pair"] for e in watchlist}

    log.info("Fetching active USDT futures pairs...")
    all_pairs = get_active_usdt_pairs()
    if not all_pairs:
        log.warning("No active pairs fetched — skipping watchlist update this run.")
        return watchlist

    gainers = get_top_gainers(all_pairs)
    log.info(
        f"Top gainers (>{MIN_GAINER_PCT}%): "
        f"{[g['pair'] + ' ' + str(round(g['change'], 1)) + '%' for g in gainers]}"
    )

    candidate_pairs = [g["pair"] for g in gainers if g["pair"] not in existing_pairs]

    newly_added = [{"pair": p} for p in candidate_pairs]

    if newly_added:
        watchlist = watchlist + newly_added
        save_watchlist(WATCHLIST_FILE, watchlist)
        log.info(f"Added {len(newly_added)} new pair(s) to watchlist: "
                  f"{[e['pair'] for e in newly_added]}")
    else:
        log.info("No new pairs qualified for the watchlist this run.")

    return watchlist


# =====================================================================================
# STEP 2 — EMA 9 BREAKDOWN CHECK
# =====================================================================================

def check_ema_breakdown(pair):
    """
    Returns (pair, alert_message) if the last closed 1H candle is the first close
    below EMA 9 and a valid swing high exists, else (pair, None).
    """
    df = fetch_raw_candles(pair)
    if df is None:
        return pair, None

    df = add_ema(df)
    last_idx = len(df) - 1
    prev, last = df.iloc[-2], df.iloc[-1]
    if pd.isna(last["EMA"]) or pd.isna(prev["EMA"]):
        return pair, None

    first_close_below = last["close"] < last["EMA"] and prev["close"] >= prev["EMA"]
    if not first_close_below:
        return pair, None

    entry = round(float(last["low"]), 6)
    sw_idx = find_swing_high(df, last_idx, above=entry)
    if sw_idx is None:
        log.info(f"{pair}: closed below EMA {EMA_LENGTH} but no confirmed swing high — skipped")
        return pair, None
    sl = round(float(df.iloc[sw_idx]["high"]), 6)

    risk = sl - entry
    t2 = round(entry - 2 * risk, 6)
    t3 = round(entry - 3 * risk, 6)
    t4 = round(entry - 4 * risk, 6)

    log.info(f"\U0001F534 EMA {EMA_LENGTH} BREAKDOWN: {pair} | close={last['close']} "
             f"ema={last['EMA']:.6f} entry={entry} sl={sl}")
    msg = build_short_msg(pair, entry, sl, t2, t3, t4)
    return pair, msg


def run_signal_check(watchlist):
    """
    STEP 2 — only runs if watchlist is non-empty.
    Checks every watchlisted pair for the EMA 9 breakdown setup. Sends alerts and
    removes alerted pairs from the watchlist (only if Telegram accepted the message).
    """
    if not watchlist:
        log.info("Watchlist empty — skipping EMA check this run.")
        return

    pairs = [e["pair"] for e in watchlist]
    alerts = []
    triggered_pairs = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(check_ema_breakdown, p): p for p in pairs}
        for f in as_completed(futures):
            pair, msg = f.result()
            if msg:
                alerts.append(msg)
                triggered_pairs.add(pair)

    if not alerts:
        log.info(f"No EMA {EMA_LENGTH} breakdowns this run.")
        return

    try:
        sent = Send_EMA_Telegram_Message("\n\n---\n\n".join(alerts))
    except Exception as e:
        log.warning(f"Failed to send Telegram message: {e}")
        sent = False

    if not sent:
        log.warning(f"Telegram send FAILED — {len(alerts)} pair(s) kept on the watchlist.")
        return

    log.info(f"Sent {len(alerts)} alert(s).")
    remaining = [e for e in watchlist if e["pair"] not in triggered_pairs]
    save_watchlist(WATCHLIST_FILE, remaining)
    log.info(f"Removed {len(triggered_pairs)} triggered pair(s) from watchlist: "
             f"{sorted(triggered_pairs)}")


# =====================================================================================
# MAIN
# =====================================================================================

def main():
    log.info("=== EMA 9 Breakdown Scanner run started ===")

    watchlist = update_watchlist()
    run_signal_check(watchlist)

    log.info("=== Run complete ===")


if __name__ == "__main__":
    main()