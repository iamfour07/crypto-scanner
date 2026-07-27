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
   - On the 1D timeframe, flags fresh upper-band breakouts into
     GainerWatchlist.json and fresh lower-band breakdowns into
     LoserWatchlist.json. Both files are overwritten every day.

2. Hourly Scanner (runs every execution):
   - Loads GainerWatchlist.json / LoserWatchlist.json.
   - On the 1H timeframe, looks for the same breakout confirmation.
   - On a BUY signal  -> Telegram alert + coin removed from GainerWatchlist.
   - On a SELL signal -> Telegram alert + coin removed from LoserWatchlist.

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
ENTRY_BUFFER = 2        # Points added/subtracted to breakout candle high/low

# ---- Bollinger Squeeze filter (hourly signals only) ----
# Only take a breakout if the bands were "tight" (squeezed) right before it fired.
# Band width % = (BB_upper - BB_lower) / BB_mid * 100, measured on the candle
# BEFORE the breakout candle (i.e. the squeeze state, not the expanding one).
ENABLE_SQUEEZE_FILTER = True   # Set False to disable this filter entirely
MAX_BB_WIDTH_PCT = 1.0          # Only fire if band width <= this % of price

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
    """Load a watchlist JSON file. Returns [] if missing/corrupt."""
    if not os.path.exists(file):
        return []
    try:
        with open(file, "r") as f:
            return json.load(f)
    except Exception:
        return []


def save_watchlist(file, data):
    """Overwrite a watchlist JSON file with the given list."""
    with open(file, "w") as f:
        json.dump(sorted(set(data)), f, indent=2)


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

    # Drop the currently-forming (incomplete) candle — always the last row.
    df = df.iloc[:-1].reset_index(drop=True)

    if len(df) < BB_LENGTH + 2:
        return None

    df = calculate_bollinger(df)
    df = df.dropna().reset_index(drop=True)

    if len(df) < 2:
        return None

    return df


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
# DAILY SCANNER
# =====================================================================================

def _daily_check_pair(pair):
    """
    Evaluates a single pair on the 1D timeframe.
    Returns ("GAIN", pair), ("LOSE", pair) or (None, None).
    """
    df = fetch_candles(pair, "1D")
    if df is None or len(df) < 2:
        return (None, None)

    last = df.iloc[-1]
    prev = df.iloc[-2]

    # Fresh breakout ABOVE the upper band
    if prev["close"] <= prev["BB_upper"] and last["close"] > last["BB_upper"]:
        return ("GAIN", pair)

    # Fresh breakdown BELOW the lower band
    if prev["close"] >= prev["BB_lower"] and last["close"] < last["BB_lower"]:
        return ("LOSE", pair)

    return (None, None)


def daily_scan():
    """
    Scans every active USDT futures pair on the 1D timeframe and rebuilds
    GainerWatchlist.json / LoserWatchlist.json from scratch.
    """
    print("[DAILY] Fetching active USDT futures pairs...")
    pairs = get_active_usdt_pairs()
    print(f"[DAILY] {len(pairs)} pairs found. Scanning 1D Bollinger breakouts...")

    gainers, losers = [], []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_daily_check_pair, p): p for p in pairs}
        for future in as_completed(futures):
            tag, pair = future.result()
            if tag == "GAIN":
                gainers.append(pair)
            elif tag == "LOSE":
                losers.append(pair)

    save_watchlist(GAINER_FILE, gainers)
    save_watchlist(LOSER_FILE, losers)

    print(f"[DAILY] GainerWatchlist: {len(gainers)} | LoserWatchlist: {len(losers)}")


# =====================================================================================
# HOURLY SCANNER
# =====================================================================================

def _is_squeezed(prev_row):
    """
    Returns True if the squeeze filter is disabled, OR the band width on the
    pre-breakout candle was at/under MAX_BB_WIDTH_PCT.
    """
    if not ENABLE_SQUEEZE_FILTER:
        return True
    return prev_row["BB_width_pct"] <= MAX_BB_WIDTH_PCT


def _hourly_check_buy(pair):
    """Evaluates a gainer-watchlist pair on the 1H timeframe for a BUY signal."""
    df = fetch_candles(pair, "60")
    if df is None or len(df) < 2:
        return (pair, None, None)

    last = df.iloc[-1]
    prev = df.iloc[-2]

    if (
        prev["close"] <= prev["BB_upper"]
        and last["close"] > last["BB_upper"]
        and _is_squeezed(prev)
    ):
        entry = float(last["high"]) + ENTRY_BUFFER
        sl = float(last["low"]) - ENTRY_BUFFER
        levels = calculate_trade_levels(entry, sl, "BUY")
        if levels:
            return (pair, "BUY", levels)

    return (pair, None, None)


def _hourly_check_sell(pair):
    """Evaluates a loser-watchlist pair on the 1H timeframe for a SELL signal."""
    df = fetch_candles(pair, "60")
    if df is None or len(df) < 2:
        return (pair, None, None)

    last = df.iloc[-1]
    prev = df.iloc[-2]

    if (
        prev["close"] >= prev["BB_lower"]
        and last["close"] < last["BB_lower"]
        and _is_squeezed(prev)
    ):
        entry = float(last["low"]) - ENTRY_BUFFER
        sl = float(last["high"]) + ENTRY_BUFFER
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
    """
    gainer_watch = load_watchlist(GAINER_FILE)
    loser_watch = load_watchlist(LOSER_FILE)

    alerts = []
    triggered_gainers = set()
    triggered_losers = set()

    # ---- BUY signals from GainerWatchlist ----
    if gainer_watch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_hourly_check_buy, p): p for p in gainer_watch}
            for future in as_completed(futures):
                pair, side, levels = future.result()
                if side == "BUY":
                    alerts.append(build_message(pair, "BUY", levels))
                    triggered_gainers.add(pair)

    # ---- SELL signals from LoserWatchlist ----
    if loser_watch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_hourly_check_sell, p): p for p in loser_watch}
            for future in as_completed(futures):
                pair, side, levels = future.result()
                if side == "SELL":
                    alerts.append(build_message(pair, "SELL", levels))
                    triggered_losers.add(pair)

    # ---- Send alerts ----
    if alerts:
        send_alert("\n\n".join(alerts))
        print(f"[HOURLY] Sent {len(alerts)} alert(s).")
    else:
        print("[HOURLY] No signals this run.")

    # ---- Remove triggered coins so they don't re-fire ----
    if triggered_gainers:
        gainer_watch = [p for p in gainer_watch if p not in triggered_gainers]
        save_watchlist(GAINER_FILE, gainer_watch)

    if triggered_losers:
        loser_watch = [p for p in loser_watch if p not in triggered_losers]
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