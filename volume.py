"""
=====================================================================================
CoinDCX Futures — Volume Spike Scanner
=====================================================================================

Strategy   : Single filter — Volume only.
Condition  : Latest CLOSED candle volume >= VOLUME_MULTIPLIER x that coin's own
             average volume over its previous VOLUME_LOOKBACK candles.

This scans EVERY active USDT-margined futures pair on CoinDCX (no gainer/loser
watchlists, no Bollinger Bands, no trade-level math — just the volume filter).

Scheduling
----------
Meant to be triggered on a schedule (e.g. every hour via cron), same as your
other scanner:
    30 * * * * /usr/bin/python3 /path/to/coindcx_volume_scanner.py

Before running: insert your real Telegram sender in place of the import below,
keeping the exact function name `Send_EMA_Telegram_Message`.
=====================================================================================
"""

import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from Telegram_EMA import Send_EMA_Telegram_Message


# =====================================================================================
# CONFIG
# =====================================================================================

# ---- Volume filter settings ----
RESOLUTION = "60"          # "60" = 1H candles, "1D" = daily candles
VOLUME_LOOKBACK = 20        # Number of prior closed candles used for the average
VOLUME_MULTIPLIER = 2.0    # Trigger when current volume >= this x the average

# ---- Threading ----
MAX_WORKERS = 10

# ---- API endpoints ----
ACTIVE_INSTRUMENTS_URL = (
    "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
    "active_instruments?margin_currency_short_name[]=USDT"
)
CANDLES_URL = "https://public.coindcx.com/market_data/candlesticks"

# ---- Candle lookback window (must comfortably exceed VOLUME_LOOKBACK + buffer) ----
if RESOLUTION == "1D":
    HISTORY_LOOKBACK_SECONDS = 120 * 86400        # ~120 days of daily candles
else:
    HISTORY_LOOKBACK_SECONDS = 120 * 3600         # ~120 hours of hourly candles


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
# CANDLE FETCHING
# =====================================================================================

def fetch_candles(pair, resolution):
    """
    Fetch OHLCV candles for a pair from CoinDCX, with the currently-forming
    (incomplete) candle already dropped.

    Returns None if data is missing or insufficient for the volume average.
    """
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - HISTORY_LOOKBACK_SECONDS

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
    if len(candles) < VOLUME_LOOKBACK + 3:
        return None

    df = pd.DataFrame(candles).sort_values("time").reset_index(drop=True)

    if "volume" not in df.columns:
        return None

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop the currently-forming (incomplete) candle — always the last row.
    df = df.iloc[:-1].reset_index(drop=True)

    if len(df) < VOLUME_LOOKBACK + 1:
        return None

    return df.dropna().reset_index(drop=True)


# =====================================================================================
# VOLUME FILTER
# =====================================================================================

def calculate_avg_volume(df, lookback=VOLUME_LOOKBACK):
    """
    Adds an AVG_VOLUME column — the rolling average volume of the PRIOR
    `lookback` candles (current candle itself is excluded from its own average).
    """
    df["AVG_VOLUME"] = df["volume"].shift(1).rolling(lookback).mean()
    return df


def check_volume_spike(pair):
    """
    Evaluates a single pair's latest closed candle against its own rolling
    average volume. Returns a result dict on a spike, otherwise None.
    """
    df = fetch_candles(pair, RESOLUTION)
    if df is None or len(df) < VOLUME_LOOKBACK + 1:
        return None

    df = calculate_avg_volume(df)
    last = df.iloc[-1]

    avg_vol = last["AVG_VOLUME"]
    cur_vol = last["volume"]

    if pd.isna(avg_vol) or avg_vol <= 0:
        return None

    if cur_vol >= VOLUME_MULTIPLIER * avg_vol:
        return {
            "pair": pair,
            "current_volume": float(cur_vol),
            "average_volume": float(avg_vol),
            "multiple": float(cur_vol / avg_vol),
            "close": float(last["close"]),
        }

    return None


# =====================================================================================
# ALERTS
# =====================================================================================

def build_message(result):
    """Formats the Telegram alert message for a volume-spike signal."""
    link = f"https://coindcx.com/futures/{result['pair']}"

    msg = (
        f"📊 VOLUME SPIKE — {result['pair']}\n\n"
        f"Close        : {round(result['close'], 6)}\n"
        f"Current Vol  : {round(result['current_volume'], 2)}\n"
        f"Average Vol  : {round(result['average_volume'], 2)}\n"
        f"Multiple     : {round(result['multiple'], 2)}x\n\n"
        f"{link}\n"
        f"-----------------------------------"
    )
    return msg


def send_alert(message):
    """Sends a single alert (or batch of alerts joined together) via Telegram."""
    if not message:
        return
    print(message)
    # Send_EMA_Telegram_Message(message)


# =====================================================================================
# SCANNER
# =====================================================================================

def volume_scan():
    """
    Scans every active USDT futures pair for a volume spike and sends
    a single combined Telegram alert for all matches found this run.
    """
    print("[SCAN] Fetching active USDT futures pairs...")
    pairs = get_active_usdt_pairs()
    print(f"[SCAN] {len(pairs)} pairs found. Checking volume spikes on {RESOLUTION}...")

    alerts = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(check_volume_spike, p): p for p in pairs}
        for future in as_completed(futures):
            result = future.result()
            if result:
                alerts.append(build_message(result))

    if alerts:
        send_alert("\n\n".join(alerts))
        print(f"[SCAN] Sent {len(alerts)} volume-spike alert(s).")
    else:
        print("[SCAN] No volume spikes this run.")


# =====================================================================================
# MAIN
# =====================================================================================

def main():
    now = datetime.now(timezone.utc)
    print(f"[RUN] {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    volume_scan()


if __name__ == "__main__":
    main()