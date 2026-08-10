"""
=====================================================================================
CoinDCX Futures — Bollinger Band Breakout Scanner
=====================================================================================

Strategy   : Pure Bollinger Band breakout (NO Supertrend, NO third-party TA libs)
Bollinger  : Length = 20, Multiplier = 2   (calculated manually with pandas)
Risk       : Fixed ₹100 risk per trade regardless of stop-loss distance
Leverage   : 7x (fixed)

Flow
----
1. Daily Scanner (runs only between 05:30 - 05:35 IST):
   - Pulls EVERY active USDT-margined futures pair from CoinDCX
     (active_instruments endpoint — NOT top-movers / price_change).
   - Adds a coin to GainerWatchlist.json the day it FIRST closes above its
     1D upper band (prev close <= upper, today's close > upper), and
     records that candle's LOW as its "invalidation" level.
   - Adds a coin to LoserWatchlist.json the day it FIRST closes below its
     1D lower band, and records that candle's HIGH as its invalidation
     level.
   - Every day after that, a coin ALREADY on a watchlist is re-checked
     only against its own stored invalidation level — not against the
     band — and:
       * Gainer: stays on the list unless today's daily close < the low
         of the day it was added (then it's removed as invalidated).
       * Loser: stays on the list unless today's daily close > the high
         of the day it was added (then it's removed as invalidated).
     This means a coin is NOT removed just because a day passes without
     an hourly alert — it stays on watch until it either fires on the 1H
     timeframe, or the daily close breaks the level from its add-day.

2. Hourly Scanner (runs every execution):
   - Loads GainerWatchlist.json / LoserWatchlist.json.
   - On the 1H timeframe, looks for a fresh breakout confirmation.
   - On a BUY signal  -> Telegram alert + coin removed from GainerWatchlist.
   - On a SELL signal -> Telegram alert + coin removed from LoserWatchlist.
   - Invalidation (the day's-low / day's-high breach) is checked only by
     the daily scanner, once per day, using the daily close.

Scheduling
----------
This script is designed to be triggered once every hour (e.g. via cron or
Windows Task Scheduler) at the top of the hour — 5:30, 6:30, 7:30 ... IST.
Each run decides internally whether the Daily Scanner should also fire.

Example cron (runs at HH:30 every hour, server in any timezone — IST check
is handled inside the script):
    30 * * * * /usr/bin/python3 /path/to/coindcx_bb_scanner.py

Before running: insert your real Telegram sender in place of the import
below, keeping the exact function name `Send_EMA_Telegram_Message`.
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

# ---- Bollinger Band settings ----
BB_LENGTH = 20
BB_MULT = 2

# ---- Risk management ----
RISK_RS = 100          # Fixed ₹ risk per trade, regardless of SL distance
LEVERAGE = 7            # Fixed leverage

# ---- Debugging ----
# When True, hourly_scan() prints WHY each watchlisted coin did or didn't
# fire — close vs Bollinger bands for the 1H check.
# Turn this on if signals aren't coming through and you want to see why.
DEBUG_MODE = True

# ---- Threading ----
MAX_WORKERS = 10

# ---- Watchlist files ----
GAINER_FILE = "GainerWatchlist.json"
LOSER_FILE = "LoserWatchlist.json"

# ---- API endpoints ----
ACTIVE_INSTRUMENTS_URL = (
    "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
    "active_instruments?margin_currency_short_name[]=USDT"
)
CANDLES_URL = "https://public.coindcx.com/market_data/candlesticks"

# ---- Candle lookback windows (must comfortably exceed BB_LENGTH + buffer) ----
DAILY_LOOKBACK_DAYS = 400      # for 1D candles
HOURLY_LOOKBACK_HOURS = 1500   # for 60m candles

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
    """
    Load a watchlist JSON file. Returns [] if missing/corrupt.

    Each entry is a dict: {"pair": <str>, "invalidation": <float or None>}.
    "invalidation" is the day's-low (gainer) / day's-high (loser) of the
    candle on the day the coin was added — used by daily_scan() to decide
    whether the coin stays on the list. Old-format files (plain list of
    pair name strings) are auto-upgraded with invalidation=None, meaning
    that entry will only ever leave the list via an hourly alert until the
    next time it's naturally re-added with a real invalidation level.
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
            normalized.append({"pair": item["pair"], "invalidation": item.get("invalidation")})
        elif isinstance(item, str):
            normalized.append({"pair": item, "invalidation": None})
    return normalized


def save_watchlist(file, data):
    """
    Overwrite a watchlist JSON file with the given list of
    {"pair": ..., "invalidation": ...} dicts, de-duplicated by pair name
    (first occurrence wins).
    """
    deduped = {}
    for entry in data:
        pair = entry["pair"]
        if pair not in deduped:
            deduped[pair] = entry
    with open(file, "w") as f:
        json.dump(list(deduped.values()), f, indent=2)


# =====================================================================================
# CANDLE FETCHING
# =====================================================================================

def fetch_candles(pair, resolution):
    """
    Fetch OHLC candles for a pair from CoinDCX and return a Bollinger-annotated
    DataFrame with the currently-forming (incomplete) candle already dropped.

    resolution : "60"  -> hourly candles
                 "1D"  -> daily candles

    Returns None if data is insufficient for a reliable BB calculation.
    """
    now = int(datetime.now(timezone.utc).timestamp())

    if resolution == "1D":
        lookback_seconds = DAILY_LOOKBACK_DAYS * 86400
    else:
        lookback_seconds = HOURLY_LOOKBACK_HOURS * 3600

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
    if len(candles) < BB_LENGTH + 5:
        return None

    df = pd.DataFrame(candles).sort_values("time").reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop the last candle only when it is still forming. Some API responses
    # already contain only closed candles, and blindly removing the last row
    # can make us miss the most recent 1H breakout candle.
    df = _drop_incomplete_last_candle(df, resolution)

    if len(df) < BB_LENGTH + 2:
        return None

    df = calculate_bollinger(df)
    df = df.dropna().reset_index(drop=True)

    if len(df) < 2:
        return None

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
    """
    Normalizes CoinDCX candle timestamps to UTC epoch seconds.
    Handles both seconds and milliseconds.
    """
    if pd.isna(raw_time):
        return None

    try:
        ts = int(raw_time)
    except (TypeError, ValueError):
        return None

    # 13 digits usually means milliseconds.
    if ts > 10**12:
        ts //= 1000

    return ts


def _drop_incomplete_last_candle(df, resolution):
    """
    Removes the final row only if it belongs to the candle that is still
    forming at the current moment.
    """
    if df.empty:
        return df

    candle_seconds = _resolution_to_seconds(resolution)
    last_candle_time = _normalize_candle_time(df.iloc[-1].get("time"))

    if candle_seconds is None or last_candle_time is None:
        # Fallback to the old safe behavior if we cannot determine timing.
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
# BOLLINGER BANDS (manual — no TA library)
# =====================================================================================

def calculate_bollinger(df, length=BB_LENGTH, mult=BB_MULT):
    """Adds BB_mid / BB_upper / BB_lower columns using a simple rolling mean + std."""
    mid = df["close"].rolling(length).mean()
    std = df["close"].rolling(length).std()
    df["BB_mid"] = mid
    df["BB_upper"] = mid + mult * std
    df["BB_lower"] = mid - mult * std
    # Band width as a % of the middle band — a small value = a "squeeze".
    df["BB_width_pct"] = (df["BB_upper"] - df["BB_lower"]) / df["BB_mid"] * 100
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
    """Formats the Telegram alert message for a BUY or SELL signal."""
    emoji = "🟢" if side == "BUY" else "🔴"
    link = f"https://coindcx.com/futures/{pair}"

    msg = (
        f"{emoji} {side} {pair}\n\n"
        f"Entry : {round(levels['entry'], 6)}\n"
        f"SL    : {round(levels['sl'], 6)}\n"
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
    try:
        Send_EMA_Telegram_Message(message)
    except Exception as e:
        # Don't let a Telegram failure kill the run — we still want the
        # watchlist files updated below even if the alert send fails.
        print(f"[ALERT] Failed to send Telegram message: {e}")


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
# DAILY SCANNER
# =====================================================================================

def _daily_check_new_pair(pair, existing_gainer_pairs, existing_loser_pairs):
    """
    Checks a pair NOT currently on either watchlist for a fresh 1D
    breakout. Returns ("GAIN", entry), ("LOSE", entry), or (None, None).

    entry = {"pair": pair, "invalidation": <day's low (GAIN) or high (LOSE)>}
    """
    if pair in existing_gainer_pairs or pair in existing_loser_pairs:
        return (None, None)

    df = fetch_candles(pair, "1D")
    if df is None or len(df) < 2:
        return (None, None)

    last = df.iloc[-1]
    prev = df.iloc[-2]

    # Fresh breakout ABOVE the upper band -> add, invalidation = today's low
    if prev["close"] <= prev["BB_upper"] and last["close"] > last["BB_upper"]:
        return ("GAIN", {"pair": pair, "invalidation": float(last["low"])})

    # Fresh breakdown BELOW the lower band -> add, invalidation = today's high
    if prev["close"] >= prev["BB_lower"] and last["close"] < last["BB_lower"]:
        return ("LOSE", {"pair": pair, "invalidation": float(last["high"])})

    return (None, None)


def _daily_recheck_existing(entry, side):
    """
    Re-checks a coin ALREADY on a watchlist against its own stored
    invalidation level (not the band). Returns the entry unchanged if it
    should stay, or None if it should be dropped.

    - side == "GAIN": drop if today's daily close < entry's invalidation
      (the low of the candle on the day it was added).
    - side == "LOSE": drop if today's daily close > entry's invalidation
      (the high of the candle on the day it was added).

    If candle data can't be fetched (transient API issue) or the entry has
    no invalidation level (old-format file), the entry is kept as-is —
    we never want a data hiccup to silently drop a valid watch.
    """
    invalidation = entry.get("invalidation")
    if invalidation is None:
        return entry

    df = fetch_candles(entry["pair"], "1D")
    if df is None or len(df) < 1:
        if DEBUG_MODE:
            print(f"[DEBUG][DAILY-RECHECK] {entry['pair']}: no candle data, keeping as-is")
        return entry

    last_close = float(df.iloc[-1]["close"])

    if side == "GAIN":
        if last_close < invalidation:
            if DEBUG_MODE:
                print(
                    f"[DEBUG][DAILY-RECHECK] {entry['pair']}: INVALIDATED "
                    f"(close={last_close:.6f} < add-day low={invalidation:.6f})"
                )
            return None
    else:
        if last_close > invalidation:
            if DEBUG_MODE:
                print(
                    f"[DEBUG][DAILY-RECHECK] {entry['pair']}: INVALIDATED "
                    f"(close={last_close:.6f} > add-day high={invalidation:.6f})"
                )
            return None

    return entry


def daily_scan():
    """
    Runs once per day (05:30-05:35 IST window).

    Step 1 - Recheck coins already on each watchlist against their own
             invalidation level (day's low/high from the day they were
             added). Coins that haven't breached it stay, no matter how
             many days have passed without an hourly alert.

    Step 2 - Scan every active USDT pair NOT already on a watchlist for a
             fresh 1D breakout, and add any new ones found (recording
             today's low/high as their invalidation level).

    The two watchlists are then saved as the recheck-survivors plus the
    newly-added coins.
    """
    existing_gainers = load_watchlist(GAINER_FILE)
    existing_losers = load_watchlist(LOSER_FILE)
    existing_gainer_pairs = {e["pair"] for e in existing_gainers}
    existing_loser_pairs = {e["pair"] for e in existing_losers}

    print(
        f"[DAILY] Rechecking {len(existing_gainers)} existing gainer(s) and "
        f"{len(existing_losers)} existing loser(s) against invalidation levels..."
    )

    kept_gainers = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_daily_recheck_existing, e, "GAIN"): e for e in existing_gainers
        }
        for future in as_completed(futures):
            result = future.result()
            if result:
                kept_gainers.append(result)

    kept_losers = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_daily_recheck_existing, e, "LOSE"): e for e in existing_losers
        }
        for future in as_completed(futures):
            result = future.result()
            if result:
                kept_losers.append(result)

    print("[DAILY] Fetching active USDT futures pairs for new breakouts...")
    pairs = get_active_usdt_pairs()
    print(f"[DAILY] {len(pairs)} pairs found. Scanning for fresh 1D breakouts...")

    new_gainers, new_losers = [], []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                _daily_check_new_pair, p, existing_gainer_pairs, existing_loser_pairs
            ): p
            for p in pairs
        }
        for future in as_completed(futures):
            tag, entry = future.result()
            if tag == "GAIN":
                new_gainers.append(entry)
            elif tag == "LOSE":
                new_losers.append(entry)

    final_gainers = kept_gainers + new_gainers
    final_losers = kept_losers + new_losers

    save_watchlist(GAINER_FILE, final_gainers)
    save_watchlist(LOSER_FILE, final_losers)

    print(
        f"[DAILY] GainerWatchlist: {len(final_gainers)} "
        f"({len(kept_gainers)} kept + {len(new_gainers)} new) | "
        f"LoserWatchlist: {len(final_losers)} "
        f"({len(kept_losers)} kept + {len(new_losers)} new)"
    )


# =====================================================================================
# HOURLY SCANNER
# =====================================================================================

def _hourly_check_buy(pair):
    """Evaluates a gainer-watchlist pair on the 1H timeframe for a BUY signal."""
    df = fetch_candles(pair, "60")
    if df is None or len(df) < 2:
        if DEBUG_MODE:
            print(f"[DEBUG][BUY] {pair}: skipped — no/insufficient candle data")
        return (pair, None, None)

    last = df.iloc[-1]
    prev = df.iloc[-2]

    crossed = prev["close"] <= prev["BB_upper"] and last["close"] > last["BB_upper"]
    if DEBUG_MODE:
        print(
            f"[DEBUG][BUY] {pair}: prev_time={_format_candle_time(prev.get('time'))} "
            f"last_time={_format_candle_time(last.get('time'))} "
            f"prev_close={prev['close']:.6f} "
            f"prev_upper={prev['BB_upper']:.6f} last_close={last['close']:.6f} "
            f"last_upper={last['BB_upper']:.6f} crossed={crossed}"
        )

    if crossed:
        entry = float(last["high"])
        sl = float(last["low"])
        levels = calculate_trade_levels(entry, sl, "BUY")
        if levels:
            return (pair, "BUY", levels)

    return (pair, None, None)


def _hourly_check_sell(pair):
    """Evaluates a loser-watchlist pair on the 1H timeframe for a SELL signal."""
    df = fetch_candles(pair, "60")
    if df is None or len(df) < 2:
        if DEBUG_MODE:
            print(f"[DEBUG][SELL] {pair}: skipped — no/insufficient candle data")
        return (pair, None, None)

    last = df.iloc[-1]
    prev = df.iloc[-2]

    crossed = prev["close"] >= prev["BB_lower"] and last["close"] < last["BB_lower"]
    if DEBUG_MODE:
        print(
            f"[DEBUG][SELL] {pair}: prev_time={_format_candle_time(prev.get('time'))} "
            f"last_time={_format_candle_time(last.get('time'))} "
            f"prev_close={prev['close']:.6f} "
            f"prev_lower={prev['BB_lower']:.6f} last_close={last['close']:.6f} "
            f"last_lower={last['BB_lower']:.6f} crossed={crossed}"
        )

    if crossed:
        entry = float(last["low"])
        sl = float(last["high"])
        levels = calculate_trade_levels(entry, sl, "SELL")
        if levels:
            return (pair, "SELL", levels)

    return (pair, None, None)


def hourly_scan():
    """
    Runs every execution.
    Checks GainerWatchlist for BUY breakouts and LoserWatchlist for SELL
    breakdowns on the 1H timeframe, sends Telegram alerts, and removes any
    triggered coin from its watchlist to prevent duplicate signals.
    Invalidation (day's-low / day's-high breach) is handled separately by
    daily_scan() and is not touched here.
    """
    gainer_watch = load_watchlist(GAINER_FILE)   # list of {"pair", "invalidation"}
    loser_watch = load_watchlist(LOSER_FILE)

    alerts = []
    triggered_gainer_pairs = set()
    triggered_loser_pairs = set()

    # ---- BUY signals from GainerWatchlist ----
    if gainer_watch:
        gainer_pairs = [e["pair"] for e in gainer_watch]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_hourly_check_buy, p): p for p in gainer_pairs}
            for future in as_completed(futures):
                pair, side, levels = future.result()
                if side == "BUY":
                    alerts.append(build_message(pair, "BUY", levels))
                    triggered_gainer_pairs.add(pair)

    # ---- SELL signals from LoserWatchlist ----
    if loser_watch:
        loser_pairs = [e["pair"] for e in loser_watch]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_hourly_check_sell, p): p for p in loser_pairs}
            for future in as_completed(futures):
                pair, side, levels = future.result()
                if side == "SELL":
                    alerts.append(build_message(pair, "SELL", levels))
                    triggered_loser_pairs.add(pair)

    # ---- Send alerts ----
    if alerts:
        send_alert("\n\n".join(alerts))
        print(f"[HOURLY] Sent {len(alerts)} alert(s).")
    else:
        print("[HOURLY] No signals this run.")

    # ---- Remove triggered coins so they don't re-fire ----
    if triggered_gainer_pairs:
        gainer_watch = [e for e in gainer_watch if e["pair"] not in triggered_gainer_pairs]
        save_watchlist(GAINER_FILE, gainer_watch)

    if triggered_loser_pairs:
        loser_watch = [e for e in loser_watch if e["pair"] not in triggered_loser_pairs]
        save_watchlist(LOSER_FILE, loser_watch)


# =====================================================================================
# SCHEDULER LOGIC
# =====================================================================================

def is_daily_scan_window():
    """True if current IST time is between 05:30 and 05:35 (inclusive)."""
    now_ist = datetime.now(IST)
    window_start = now_ist.replace(hour=5, minute=30, second=0, microsecond=0)
    window_end = now_ist.replace(hour=5, minute=35, second=0, microsecond=0)
    return window_start <= now_ist <= window_end


# =====================================================================================
# MAIN
# =====================================================================================

def main():
    now_ist = datetime.now(IST)
    print(f"[RUN] {now_ist.strftime('%Y-%m-%d %H:%M:%S')} IST")

    if is_daily_scan_window():
        daily_scan()
    else:
        print("[DAILY] Skipped — outside 05:30–05:35 IST window.")

    hourly_scan()


if __name__ == "__main__":
    main()