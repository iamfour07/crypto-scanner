"""
=====================================================================================
CoinDCX Futures — Momentum Reversal Scanner (RSI-40 Cross-Down after 30%+ Daily Pump)
=====================================================================================

Strategy
--------
1. WATCHLIST BUILD (runs every time you execute the script):
   - Pulls EVERY active USDT-margined futures pair from CoinDCX
     (active_instruments endpoint).
   - For each pair, checks the CURRENT (in-progress) 1D candle's gain:
         pct_change = (close - open) / open * 100
     using the latest available 1D candle data.
   - Ranks all pairs with pct_change > 30% by size, takes the TOP 5.
   - Adds them to ReversalWatchlist.json, skipping any pair already present
     (no duplicates). Existing watchlist entries are never removed here —
     only the RSI check step (below) removes them.

2. RSI REVERSAL CHECK (runs every time, on everything in ReversalWatchlist):
   - On the 1H timeframe, calculates RSI (Wilder's smoothing, period 14).
   - Signal condition (SHORT / SELL):
         previous closed candle RSI >= 40
         AND last closed candle RSI  < 40
     i.e. momentum just cooled off and crossed below 40 for the first time
     after the pump — the logic being: ride the exhaustion of the move.
   - On trigger:
         Entry = last closed 1H candle's LOW
         SL    = highest HIGH over the last 10 closed 1H candles (swing high)
     -> Telegram alert sent, coin removed from ReversalWatchlist.

Risk / Reward (unchanged from original script)
-----------------------------------------------
   - Fixed ₹100 risk per trade regardless of stop-loss distance.
   - Fixed 7x leverage.
   - Targets at 2R / 3R / 4R.

Usage
-----
This script is designed to be run MANUALLY, once per hour (no scheduling
window logic — every run does both the watchlist build AND the RSI check).

    python3 coindcx_reversal_scanner.py

Before running: insert your real Telegram sender in place of the import
below, keeping the exact function name `Send_EMA_Telegram_Message`.

Assumptions made (flag if any of these are wrong):
   - RSI period = 14 (standard, Wilder's smoothing) — wasn't specified.
   - "30% on 1 day" = (close - open) / open * 100 on the current 1D candle,
     not vs. a rolling 24h ticker or previous day's close.
   - Swing-high lookback of 10 candles INCLUDES the triggering candle itself
     (i.e. the last 10 closed 1H candles, last one included).
=====================================================================================
"""

import json
import os
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from Telegram_EMA import Send_EMA_Telegram_Message


# =====================================================================================
# CONFIG
# =====================================================================================

# ---- Gainer scan settings ----
MIN_DAILY_GAIN_PCT = 30    # only coins with >30% gain on the current 1D candle
TOP_N_GAINERS = 5          # cap on how many new coins get added per run

# ---- RSI settings ----
RSI_LENGTH = 14
RSI_TRIGGER_LEVEL = 40     # cross below this level (from >= it) = SHORT signal

# ---- Swing-high (SL) settings ----
SWING_LOOKBACK = 10        # last N closed 1H candles used to find swing high

# ---- Risk management ----
RISK_RS = 100               # Fixed ₹ risk per trade, regardless of SL distance
LEVERAGE = 7                 # Fixed leverage

# ---- Debugging ----
# When True, rsi_check() prints WHY each watchlisted coin did or didn't
# fire — RSI values for the 1H check. Turn on if signals aren't coming
# through and you want to see why.
DEBUG_MODE = True

# ---- Threading ----
MAX_WORKERS = 10

# ---- Watchlist file ----
REVERSAL_FILE = "ReversalWatchlist.json"

# ---- API endpoints ----
ACTIVE_INSTRUMENTS_URL = (
    "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
    "active_instruments?margin_currency_short_name[]=USDT"
)
CANDLES_URL = "https://public.coindcx.com/market_data/candlesticks"
STATS_URL = "https://api.coindcx.com/api/v1/derivatives/futures/data/stats"

# ---- Candle lookback windows ----
# (Daily gain % now comes from the stats API — no 1D candle fetch needed.)
HOURLY_LOOKBACK_HOURS = 200      # comfortably exceeds RSI_LENGTH + SWING_LOOKBACK + buffer

IST = timezone(timedelta(hours=5, minutes=30))


# =====================================================================================
# GENERIC HELPERS
# =====================================================================================

def safe_get(url, params=None, timeout=15):
    """Wrapper around requests.get() that never raises — returns None on any failure."""
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def load_watchlist(file):
    """Load a watchlist JSON file. Returns [] if missing/corrupt."""
    if not os.path.exists(file):
        return []
    try:
        with open(file, "r") as f:
            return json.load(f)
    except Exception:
        return []


def save_watchlist(file, data):
    """Overwrite a watchlist JSON file with a de-duplicated, sorted list."""
    with open(file, "w") as f:
        json.dump(sorted(set(data)), f, indent=2)


# =====================================================================================
# CANDLE FETCHING
# =====================================================================================

def fetch_candles(pair, resolution, lookback_seconds):
    """
    Fetch OHLC candles for a pair from CoinDCX and return a DataFrame with
    the currently-forming (incomplete) candle dropped where appropriate.

    resolution : "60"  -> hourly candles
                 "1D"  -> daily candles

    Returns None if data is insufficient.
    """
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - lookback_seconds

    params = {
        "pair": pair,
        "from": from_time,
        "to": now,
        "resolution": resolution,
        "pcode": "f",
    }

    data = safe_get(CANDLES_URL, params=params)
    if not data or "data" not in data or not data["data"]:
        return None

    candles = data["data"]
    if len(candles) < 2:
        return None

    df = pd.DataFrame(candles).sort_values("time").reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _resolution_to_seconds(resolution):
    """Converts CoinDCX resolution values to candle duration in seconds."""
    if resolution == "1D":
        return 86400
    try:
        return int(resolution) * 60
    except (TypeError, ValueError):
        return None


def _normalize_candle_time(raw_time):
    """Normalizes CoinDCX candle timestamps to UTC epoch seconds."""
    if pd.isna(raw_time):
        return None
    try:
        ts = int(raw_time)
    except (TypeError, ValueError):
        return None
    if ts > 10**12:  # 13 digits usually means milliseconds
        ts //= 1000
    return ts


def _drop_incomplete_last_candle(df, resolution):
    """Removes the final row only if it belongs to the candle still forming."""
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


def _format_candle_time(raw_time):
    """Formats a candle timestamp for debug logs."""
    ts = _normalize_candle_time(raw_time)
    if ts is None:
        return "unknown"
    return datetime.fromtimestamp(ts, timezone.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M")


# =====================================================================================
# RSI (manual — Wilder's smoothing, no TA library)
# =====================================================================================

def calculate_rsi(df, length=RSI_LENGTH):
    """
    Adds an 'RSI' column using Wilder's smoothing method (the standard
    definition used by TradingView / most platforms).
    """
    delta = df["close"].diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    # Wilder's smoothing = an EMA with alpha = 1/length
    avg_gain = gain.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()

    rs = avg_gain / avg_loss
    df["RSI"] = 100 - (100 / (1 + rs))

    # Where avg_loss is 0 (straight up move), RSI is defined as 100
    df.loc[avg_loss == 0, "RSI"] = 100

    return df


# =====================================================================================
# RISK / TRADE LEVEL CALCULATION
# =====================================================================================

def calculate_trade_levels(entry, sl, side):
    """
    Fixed-risk position sizing.

    Risk is always exactly RISK_RS, regardless of stop distance:
        position_size (units) = RISK_RS / |entry - sl|
        capital_used          = (position_size * entry) / LEVERAGE

    Returns a dict with everything needed for the alert message.
    """
    risk_per_unit = abs(entry - sl)
    if risk_per_unit <= 0:
        return None

    position_size = RISK_RS / risk_per_unit
    position_value = position_size * entry
    capital_used = position_value / LEVERAGE
    expected_loss = RISK_RS

    if side == "BUY":
        t2 = entry + risk_per_unit * 2
        t3 = entry + risk_per_unit * 3
        t4 = entry + risk_per_unit * 4
    else:  # SELL
        t2 = entry - risk_per_unit * 2
        t3 = entry - risk_per_unit * 3
        t4 = entry - risk_per_unit * 4

    return {
        "entry": entry,
        "sl": sl,
        "position_size": position_size,
        "capital_used": round(capital_used, 2),
        "expected_loss": expected_loss,
        "leverage": LEVERAGE,
        "t2": t2,
        "t3": t3,
        "t4": t4,
    }


# =====================================================================================
# ALERTS
# =====================================================================================

def build_message(pair, side, levels):
    """Formats the Telegram alert message for a signal."""
    emoji = "🟢" if side == "BUY" else "🔴"
    link = f"https://coindcx.com/futures/{pair}"

    msg = (
        f"{emoji} {side} {pair}\n\n"
        f"Entry : {round(levels['entry'], 6)}\n"
        f"SL    : {round(levels['sl'], 6)}\n"
        f"Qty   : {round(levels['position_size'], 4)}\n"
        f"Capital : ₹{levels['capital_used']}\n"
        f"Leverage : {levels['leverage']}x\n\n"
        f"Risk : ₹{levels['expected_loss']}\n\n"
        f"Targets\n"
        f"2R : {round(levels['t2'], 6)}\n"
        f"3R : {round(levels['t3'], 6)}\n"
        f"4R : {round(levels['t4'], 6)}\n\n"
        f"{link}\n"
        f"-----------------------------------"
    )
    return msg


def send_alert(message):
    """Sends a single alert (or batch of alerts joined together) via Telegram."""
    if not message:
        return
    Send_EMA_Telegram_Message(message)


# =====================================================================================
# ACTIVE PAIR LIST
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


# =====================================================================================
# STEP 1 — WATCHLIST BUILD (top 5 gainers > 30% via the stats API's 1D % change)
# =====================================================================================

def _check_daily_gain(pair):
    """
    Returns (pair, pct_change) using CoinDCX's own 1D % change figure from
    the stats endpoint (data["price_change_percent"]["1D"]).
    Returns (pair, None) if the API call fails or the field is missing.
    """
    data = safe_get(f"{STATS_URL}?pair={pair}", timeout=8)
    if not data:
        if DEBUG_MODE:
            print(f"[DEBUG][SCAN] {pair}: skipped — stats API call failed")
        return (pair, None)

    change = data.get("price_change_percent", {}).get("1D")
    if change is None:
        if DEBUG_MODE:
            print(f"[DEBUG][SCAN] {pair}: skipped — no price_change_percent.1D in response")
        return (pair, None)

    try:
        pct_change = float(change)
    except (TypeError, ValueError):
        return (pair, None)

    return (pair, pct_change)


def build_reversal_watchlist():
    """
    Scans every active USDT futures pair, finds the top 5 gainers with a
    current 1D gain > MIN_DAILY_GAIN_PCT, and adds them to
    ReversalWatchlist.json (skipping duplicates, never removing existing
    entries here).
    """
    print("[SCAN] Fetching active USDT futures pairs...")
    pairs = get_active_usdt_pairs()
    print(f"[SCAN] {len(pairs)} pairs found. Checking 1D gains...")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_check_daily_gain, p): p for p in pairs}
        for future in as_completed(futures):
            pair, pct_change = future.result()
            if pct_change is not None and pct_change > MIN_DAILY_GAIN_PCT:
                results.append((pair, pct_change))

    # Rank by gain, take top N
    results.sort(key=lambda x: x[1], reverse=True)
    top_gainers = results[:TOP_N_GAINERS]

    if DEBUG_MODE:
        for pair, pct in top_gainers:
            print(f"[DEBUG][SCAN] Candidate: {pair} +{pct:.2f}%")

    existing = load_watchlist(REVERSAL_FILE)
    existing_set = set(existing)

    added = []
    for pair, _ in top_gainers:
        if pair not in existing_set:
            added.append(pair)
            existing_set.add(pair)

    if added:
        save_watchlist(REVERSAL_FILE, existing_set)
        print(f"[SCAN] Added {len(added)} new coin(s) to ReversalWatchlist: {added}")
    else:
        print("[SCAN] No new coins qualified (or all already on the watchlist).")


# =====================================================================================
# STEP 2 — RSI REVERSAL CHECK (cross below 40 -> SHORT)
# =====================================================================================

def _rsi_check_pair(pair):
    """
    Evaluates a single watchlisted pair on the 1H timeframe.
    Returns (pair, "SELL", levels) on a valid signal, else (pair, None, None).
    """
    df = fetch_candles(pair, "60", HOURLY_LOOKBACK_HOURS * 3600)
    if df is None:
        if DEBUG_MODE:
            print(f"[DEBUG][RSI] {pair}: skipped — no candle data")
        return (pair, None, None)

    df = _drop_incomplete_last_candle(df, "60")
    if len(df) < RSI_LENGTH + SWING_LOOKBACK + 2:
        if DEBUG_MODE:
            print(f"[DEBUG][RSI] {pair}: skipped — insufficient closed candles")
        return (pair, None, None)

    df = calculate_rsi(df)
    df = df.dropna(subset=["RSI"]).reset_index(drop=True)

    if len(df) < 2:
        if DEBUG_MODE:
            print(f"[DEBUG][RSI] {pair}: skipped — insufficient RSI data")
        return (pair, None, None)

    last = df.iloc[-1]
    prev = df.iloc[-2]

    crossed_down = prev["RSI"] >= RSI_TRIGGER_LEVEL and last["RSI"] < RSI_TRIGGER_LEVEL

    if DEBUG_MODE:
        print(
            f"[DEBUG][RSI] {pair}: prev_time={_format_candle_time(prev.get('time'))} "
            f"last_time={_format_candle_time(last.get('time'))} "
            f"prev_rsi={prev['RSI']:.2f} last_rsi={last['RSI']:.2f} "
            f"crossed_down={crossed_down}"
        )

    if not crossed_down:
        return (pair, None, None)

    # Entry = last closed candle's LOW
    entry = float(last["low"])

    # SL = highest HIGH over the last SWING_LOOKBACK closed candles (swing high)
    swing_window = df.tail(SWING_LOOKBACK)
    sl = float(swing_window["high"].max())

    if sl <= entry:
        # Sanity guard: SL must sit above entry for a short. If the swing
        # high is somehow at/below the entry low, skip rather than send a
        # broken trade (this can happen on very choppy/insufficient data).
        if DEBUG_MODE:
            print(f"[DEBUG][RSI] {pair}: skipped — swing high ({sl}) <= entry ({entry})")
        return (pair, None, None)

    levels = calculate_trade_levels(entry, sl, "SELL")
    if levels is None:
        return (pair, None, None)

    return (pair, "SELL", levels)


def rsi_check():
    """
    Runs every execution. Checks ReversalWatchlist for RSI cross-below-40
    signals on the 1H timeframe, sends Telegram alerts, and removes any
    triggered coin from the watchlist to prevent duplicate signals.
    """
    watchlist = load_watchlist(REVERSAL_FILE)
    if not watchlist:
        print("[RSI] Watchlist is empty — nothing to check.")
        return

    alerts = []
    triggered = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_rsi_check_pair, p): p for p in watchlist}
        for future in as_completed(futures):
            pair, side, levels = future.result()
            if side == "SELL":
                alerts.append(build_message(pair, "SELL", levels))
                triggered.add(pair)

    if alerts:
        send_alert("\n\n".join(alerts))
        print(f"[RSI] Sent {len(alerts)} alert(s).")
    else:
        print("[RSI] No signals this run.")

    if triggered:
        remaining = [p for p in watchlist if p not in triggered]
        save_watchlist(REVERSAL_FILE, remaining)
        print(f"[RSI] Removed from watchlist: {sorted(triggered)}")


# =====================================================================================
# MAIN
# =====================================================================================

def main():
    now_ist = datetime.now(IST)
    print(f"[RUN] {now_ist.strftime('%Y-%m-%d %H:%M:%S')} IST")

    build_reversal_watchlist()
    rsi_check()


if __name__ == "__main__":
    main()