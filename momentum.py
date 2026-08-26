"""
=====================================================================================
CoinDCX Futures — Heikin Ashi Supertrend Reversal Scanner (+ Market Sentiment Filter)
=====================================================================================

Strategy   : Fade extended pumps once momentum flips (SHORT-only)
Candles    : Heikin Ashi (NOT raw candlesticks) — used for BOTH stages below
Watchlist  : Bollinger Band(length=100, mult=3) touch/close on HA candles
Signal     : Supertrend(length=9, factor=1.5) flip from GREEN -> RED
Risk       : Fixed Rs.100 risk per trade regardless of stop-loss distance
Leverage   : 7x (fixed)
Timeframe  : 1H candles for everything (watchlist entry AND signal check)

NEW — Market sentiment (breadth) filter
-------------------------------------------------------------------------------------
Before firing any SHORT alert, the scanner checks the 24h change of EVERY active
USDT-M futures pair (~450 coins) and classifies each as a "gainer" (change > 0)
or a "loser" (change < 0). It then computes:

    loser_pct = (number of losers / number of pairs with valid data) * 100

Since this is a SHORT-only strategy, we only want to actually fire alerts when
the broader market is already leaning bearish (more coins red than green, and
by a meaningful margin). So:

    - If loser_pct >= MIN_LOSER_PCT_FOR_ALERT (default 56%):
          -> market sentiment is bearish enough -> run Step 2 (Supertrend flip
             check) as normal and send alerts.
    - Otherwise (market is more green / not bearish enough):
          -> Step 2 is SKIPPED entirely for this run. No Supertrend check is
             performed, no Telegram message is sent. The watchlist is left
             untouched so nothing is lost — it will be re-checked on the next
             run once/if sentiment flips bearish enough.

Note: Step 1 (building/maintaining the watchlist) ALWAYS runs regardless of
sentiment — sentiment only gates whether we act on flips (Step 2), not whether
we keep tracking extended pumps.

Flow (every run — designed to be triggered hourly via cron)
-------------------------------------------------------------------------------------
STEP 0 - Market sentiment check (ALWAYS runs, every execution):
    - Fetch 24h % change for every active USDT-M futures pair.
    - Count gainers (change > 0) vs losers (change < 0).
    - Compute loser_pct. Gate Step 2 on loser_pct >= MIN_LOSER_PCT_FOR_ALERT.

STEP 1 - Watchlist update (ALWAYS runs, every execution):
    - Reuse the same 24h change data fetched in Step 0 (no duplicate API calls).
    - Keep only pairs with change > MIN_GAINER_PCT (30%), take top 10 by change.
    - Skip any pair that is ALREADY on ReversalWatchlist.json (no duplicate add,
      no re-check of a pair that's already being watched).
    - For each remaining candidate: build 1H Heikin Ashi candles, compute
      Bollinger Bands (length=100, mult=3) on the HA close.
    - If the last CLOSED HA candle's high touches OR its close is above the
      upper band -> add the pair to ReversalWatchlist.json.

STEP 2 - Supertrend flip check (runs ONLY if the watchlist is non-empty AFTER
          Step 1, AND ONLY IF the Step 0 sentiment gate passed):
    - For every pair on the watchlist: build 1H Heikin Ashi candles, compute
      Supertrend(length=9, factor=1.5).
    - Compare the PREVIOUS closed HA candle's Supertrend colour against the
      LAST closed HA candle's colour.
    - GREEN -> RED flip on the last closed candle triggers a SHORT alert:
        Entry = low of the flip candle (last closed HA candle)
        SL    = that same candle's Supertrend value (sits ABOVE price,
                since the trend just flipped to red)
        Targets = 2R / 3R / 4R below entry (short direction)
    - On alert, the pair is REMOVED from the watchlist immediately so it
      cannot re-fire on the next run.
    - If the watchlist is still empty after Step 1, or the sentiment gate
      did not pass, Step 2 is skipped.

Before running: insert your real Telegram sender in place of the import
below, keeping the exact function name `Send_EMA_Telegram_Message`.
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

# ---- Market sentiment (breadth) filter — NEW ----
MIN_LOSER_PCT_FOR_ALERT = 56.0      # % of coins (by count) that must be red (24h)
                                     # for Step 2 (Supertrend check + alert) to run.
                                     # If fewer than this % are red, market is not
                                     # bearish enough -> Step 2 is skipped entirely.

# ---- Bollinger Band settings (watchlist entry, on HA close) ----
BB_LENGTH            = 100
BB_MULT              = 3

# ---- Supertrend settings (signal check, on HA candles) ----
ST_LENGTH            = 9
ST_FACTOR            = 1.5

# ---- Risk management ----
RISK_INR             = 100          # fixed Rs. risk per trade
LEVERAGE             = 7            # fixed leverage
INR_TO_USDT_RATE     = None         # None = fetch live

# ---- Candles ----
RESOLUTION           = "60"         # 1H candles for everything
HOURLY_LOOKBACK_HOURS = 1500          # buffer comfortably above BB_LENGTH=100

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
# 24H % CHANGE — fetched ONCE per run, shared by gainer-list AND sentiment filter
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


def fetch_all_changes(pairs):
    """
    Fetches 24h % change for EVERY given pair (no filtering). This is the
    single shared dataset used both to derive the top-gainer watchlist
    candidates AND to compute overall market sentiment/breadth.
    """
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_pair_change, p): p for p in pairs}
        for f in as_completed(futures):
            result = f.result()
            if result:
                results.append(result)
    return results


def get_top_gainers(all_changes):
    """
    Returns the top TOP_N pairs by 24h % change, filtered to change > MIN_GAINER_PCT.
    Takes the pre-fetched all_changes list (see fetch_all_changes) — no extra
    API calls are made here.
    """
    gainers = [c for c in all_changes if c["change"] > MIN_GAINER_PCT]
    gainers = sorted(gainers, key=lambda x: x["change"], reverse=True)[:TOP_N]
    return gainers


def compute_market_sentiment(all_changes):
    """
    Classifies every pair with valid 24h change data as a 'gainer' (change > 0)
    or 'loser' (change < 0). Pairs with change exactly 0 are counted in the
    total but not in either bucket.

    Returns a dict:
        {
          "total": int,          # pairs with valid data
          "gainers": int,
          "losers": int,
          "gainer_pct": float,
          "loser_pct": float,
          "bearish_enough": bool # loser_pct >= MIN_LOSER_PCT_FOR_ALERT
        }
    """
    total = len(all_changes)
    if total == 0:
        return {
            "total": 0, "gainers": 0, "losers": 0,
            "gainer_pct": 0.0, "loser_pct": 0.0,
            "bearish_enough": False,
        }

    gainers = sum(1 for c in all_changes if c["change"] > 0)
    losers = sum(1 for c in all_changes if c["change"] < 0)

    gainer_pct = round(gainers / total * 100, 2)
    loser_pct = round(losers / total * 100, 2)

    return {
        "total": total,
        "gainers": gainers,
        "losers": losers,
        "gainer_pct": gainer_pct,
        "loser_pct": loser_pct,
        "bearish_enough": loser_pct >= MIN_LOSER_PCT_FOR_ALERT,
    }


# =====================================================================================
# CANDLE FETCHING + HEIKIN ASHI CONVERSION
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

    if len(df) < BB_LENGTH + 2:
        return None

    return df


def heikin_ashi(df):
    """Converts a raw OHLC DataFrame to Heikin Ashi OHLC. Returns a new DataFrame."""
    ha = df.copy().reset_index(drop=True)
    ha_close = (df["open"] + df["high"] + df["low"] + df["close"]) / 4.0

    ha_open = pd.Series(index=df.index, dtype="float64")
    ha_open.iloc[0] = (df["open"].iloc[0] + df["close"].iloc[0]) / 2.0
    for i in range(1, len(df)):
        ha_open.iloc[i] = (ha_open.iloc[i - 1] + ha_close.iloc[i - 1]) / 2.0

    ha_high = pd.concat([df["high"], ha_open, ha_close], axis=1).max(axis=1)
    ha_low = pd.concat([df["low"], ha_open, ha_close], axis=1).min(axis=1)

    ha["open"] = ha_open
    ha["high"] = ha_high
    ha["low"] = ha_low
    ha["close"] = ha_close
    return ha


def fetch_ha_candles(pair, resolution=RESOLUTION):
    """Fetches raw candles and converts to Heikin Ashi. Returns None if unavailable."""
    df = fetch_raw_candles(pair, resolution)
    if df is None:
        return None
    return heikin_ashi(df)


# =====================================================================================
# BOLLINGER BANDS (manual, on HA close) — watchlist entry
# =====================================================================================

def calculate_bollinger(df, length=BB_LENGTH, mult=BB_MULT):
    mid = df["close"].rolling(length).mean()
    std = df["close"].rolling(length).std()
    df["BB_mid"] = mid
    df["BB_upper"] = mid + mult * std
    df["BB_lower"] = mid - mult * std
    return df


# =====================================================================================
# SUPERTREND (manual, on HA candles) — signal check
# =====================================================================================

def calculate_supertrend(df, length=ST_LENGTH, factor=ST_FACTOR):
    """
    Adds 'ST_value' and 'ST_direction' ('green'/'red') columns.
    Standard Supertrend recursion using Wilder's ATR on HA high/low/close.
    """
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)

    atr = tr.ewm(alpha=1.0 / length, adjust=False).mean()

    hl2 = (high + low) / 2.0
    basic_upper = hl2 + factor * atr
    basic_lower = hl2 - factor * atr

    n = len(df)
    final_upper = [0.0] * n
    final_lower = [0.0] * n
    st_value = [0.0] * n
    st_dir = [None] * n

    for i in range(n):
        if i == 0:
            final_upper[i] = basic_upper.iloc[i]
            final_lower[i] = basic_lower.iloc[i]
            st_value[i] = final_upper[i]
            st_dir[i] = "red"
            continue

        # Final upper band
        if basic_upper.iloc[i] < final_upper[i - 1] or close.iloc[i - 1] > final_upper[i - 1]:
            final_upper[i] = basic_upper.iloc[i]
        else:
            final_upper[i] = final_upper[i - 1]

        # Final lower band
        if basic_lower.iloc[i] > final_lower[i - 1] or close.iloc[i - 1] < final_lower[i - 1]:
            final_lower[i] = basic_lower.iloc[i]
        else:
            final_lower[i] = final_lower[i - 1]

        # Supertrend value / direction
        if st_value[i - 1] == final_upper[i - 1]:
            if close.iloc[i] <= final_upper[i]:
                st_value[i] = final_upper[i]
                st_dir[i] = "red"
            else:
                st_value[i] = final_lower[i]
                st_dir[i] = "green"
        else:  # previous was tracking the lower band (green)
            if close.iloc[i] >= final_lower[i]:
                st_value[i] = final_lower[i]
                st_dir[i] = "green"
            else:
                st_value[i] = final_upper[i]
                st_dir[i] = "red"

    df["ST_value"] = st_value
    df["ST_direction"] = st_dir
    return df


# =====================================================================================
# RISK / POSITION SIZING (fixed Rs.100 risk, 7x leverage, INR only)
# =====================================================================================

_inr_rate_cache = {"rate": None}  # cached once per run — see get_inr_rate() below


def get_inr_rate():
    """
    Live USDT->INR conversion rate. Used ONLY internally to size the position
    correctly against a Rs.-denominated risk budget — never shown to the user.

    FIX: previously this hit the ticker endpoint fresh on EVERY call to
    calc_position() — i.e. once per triggered alert. If the rate shifted
    slightly between two alerts in the same run, or one call happened to
    time out and silently fall back to 88.0 while another succeeded with a
    live rate, two alerts in the SAME run could show different capital_inr
    for the same Rs.100 risk. Now the rate is fetched once and cached for
    the remainder of the run, so every alert in a run uses the identical
    rate and capital figures stay consistent.
    """
    if INR_TO_USDT_RATE is not None:
        return INR_TO_USDT_RATE

    if _inr_rate_cache["rate"] is not None:
        return _inr_rate_cache["rate"]

    try:
        data = safe_get(TICKER_URL, timeout=5)
        if data:
            for m in data:
                if m.get("market") == "USDTINR":
                    rate = float(m.get("last_price"))
                    _inr_rate_cache["rate"] = rate
                    log.info(f"Fetched live USDT/INR rate: {rate}")
                    return rate
    except Exception as e:
        log.warning(f"Failed to fetch live USDT/INR rate: {e}")

    fallback = 88.0  # emergency fallback only, used if the ticker call fails
    log.warning(f"Using emergency fallback USDT/INR rate: {fallback}")
    _inr_rate_cache["rate"] = fallback
    return fallback


def calc_position(entry, sl):
    """
    Fixed-risk position sizing, fully in INR terms.

    RISK_INR (Rs.) is the only risk input. The live USDT/INR rate is used
    solely as an internal conversion step to size the position against the
    exchange's USDT-margined contracts — the final capital figure returned
    is in INR (capital_inr), which is what should be shown in alerts.
    """
    sl_pct = abs(entry - sl) / entry * 100
    if sl_pct == 0:
        return None

    rate = get_inr_rate()                                   # USDT -> INR, internal use only
    risk_usdt = RISK_INR / rate                              # convert Rs. risk budget to USDT
    position_usdt = round(risk_usdt / (sl_pct / 100), 2)
    capital_usdt = round(position_usdt / LEVERAGE, 2)
    capital_inr = round(capital_usdt * rate, 2)              # convert back to INR for display
    quantity = round(position_usdt / entry, 4)

    return {
        "capital_inr": capital_inr,   # <-- use this for display
        "capital_usdt": capital_usdt, # internal only, not shown in alerts
        "quantity": quantity,
    }


# =====================================================================================
# ALERT MESSAGE (INR only)
# =====================================================================================

def build_short_msg(pair, entry, sl, t2, t3, t4):
    pos = calc_position(entry, sl)
    cap = f"Rs.{pos['capital_inr']}" if pos else "N/A"

    return (
        f"\U0001F534 SHORT (Supertrend Flip)\n\n"
        f"Name- {pair}\n"
        f"Entry- {entry}\n"
        f"SL- {sl}\n"
        f"Capital- {cap}\n"
        f"Risk Per Trade- Rs.{RISK_INR}\n"
        f"-----------------\n"
        f"T2- {t2}\n"
        f"T3- {t3}\n"
        f"T4- {t4}"
    )


# =====================================================================================
# STEP 1 — WATCHLIST UPDATE
# =====================================================================================

def check_bb_touch(pair, existing_pairs):
    """
    Returns pair (str) if it should be added to the watchlist, else None.
    Condition: last closed HA candle's high touches/exceeds OR close is
    above the upper Bollinger Band (length=100, mult=3) computed on HA close.
    """
    if pair in existing_pairs:
        return None

    ha_df = fetch_ha_candles(pair)
    if ha_df is None or len(ha_df) < BB_LENGTH + 2:
        return None

    ha_df = calculate_bollinger(ha_df)
    ha_df = ha_df.dropna(subset=["BB_upper"]).reset_index(drop=True)
    if ha_df.empty:
        return None

    last = ha_df.iloc[-1]
    touched = last["high"] >= last["BB_upper"] or last["close"] > last["BB_upper"]

    if touched:
        log.info(
            f"[WATCHLIST-ADD] {pair} | HA high={last['high']:.6f} "
            f"HA close={last['close']:.6f} BB_upper={last['BB_upper']:.6f}"
        )
        return pair

    return None


def update_watchlist(all_changes):
    """
    STEP 1 — always runs, regardless of market sentiment.
    Adds newly-qualifying top gainers (>30% change, HA candle touching
    upper BB(100,3)) to the watchlist. Pairs already on the watchlist are
    skipped entirely (no re-check, no duplicate).

    Takes the pre-fetched all_changes list (see fetch_all_changes) so no
    extra API calls are made here — same dataset drives both the gainer
    list and the market sentiment filter.
    """
    watchlist = load_watchlist(WATCHLIST_FILE)
    existing_pairs = {e["pair"] for e in watchlist}

    gainers = get_top_gainers(all_changes)
    log.info(
        f"Top gainers (>{MIN_GAINER_PCT}%): "
        f"{[g['pair'] + ' ' + str(round(g['change'], 1)) + '%' for g in gainers]}"
    )

    candidate_pairs = [g["pair"] for g in gainers if g["pair"] not in existing_pairs]

    newly_added = []
    if candidate_pairs:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(check_bb_touch, p, existing_pairs): p for p in candidate_pairs}
            for f in as_completed(futures):
                result = f.result()
                if result:
                    newly_added.append({"pair": result})

    if newly_added:
        watchlist = watchlist + newly_added
        save_watchlist(WATCHLIST_FILE, watchlist)
        log.info(f"Added {len(newly_added)} new pair(s) to watchlist: "
                  f"{[e['pair'] for e in newly_added]}")
    else:
        log.info("No new pairs qualified for the watchlist this run.")

    return watchlist


# =====================================================================================
# STEP 2 — SUPERTREND FLIP CHECK
# =====================================================================================

def check_supertrend_flip(pair):
    """
    Returns (pair, alert_message) if a green->red Supertrend flip fired on
    the last closed HA candle, else (pair, None).
    """
    ha_df = fetch_ha_candles(pair)
    if ha_df is None or len(ha_df) < ST_LENGTH + 2:
        return pair, None

    ha_df = calculate_supertrend(ha_df)
    if len(ha_df) < 2:
        return pair, None

    prev = ha_df.iloc[-2]
    last = ha_df.iloc[-1]

    flipped = prev["ST_direction"] == "green" and last["ST_direction"] == "red"
    if not flipped:
        return pair, None

    entry = round(float(last["low"]), 6)
    sl = round(float(last["ST_value"]), 6)

    if sl <= entry:
        # Shouldn't happen given a red Supertrend sits above price, but
        # never fire on a broken/degenerate level.
        return pair, None

    risk = sl - entry
    t2 = round(entry - 2 * risk, 6)
    t3 = round(entry - 3 * risk, 6)
    t4 = round(entry - 4 * risk, 6)

    log.info(f"\U0001F534 SUPERTREND FLIP: {pair} | entry={entry} sl={sl}")
    msg = build_short_msg(pair, entry, sl, t2, t3, t4)
    return pair, msg


def run_signal_check(watchlist):
    """
    STEP 2 — only called if the watchlist is non-empty AND the market
    sentiment gate passed (see main()).
    Checks every watchlisted pair for a green->red Supertrend flip on the
    last closed HA candle. Fires alerts and removes triggered pairs from
    the watchlist immediately.
    """
    if not watchlist:
        log.info("Watchlist empty — skipping Supertrend flip check this run.")
        return

    pairs = [e["pair"] for e in watchlist]
    alerts = []
    triggered_pairs = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(check_supertrend_flip, p): p for p in pairs}
        for f in as_completed(futures):
            pair, msg = f.result()
            if msg:
                alerts.append(msg)
                triggered_pairs.add(pair)

    if alerts:
        try:
            Send_EMA_Telegram_Message("\n\n---\n\n".join(alerts))
        except Exception as e:
            log.warning(f"Failed to send Telegram message: {e}")
        log.info(f"Sent {len(alerts)} alert(s).")
    else:
        log.info("No Supertrend flips this run.")

    if triggered_pairs:
        remaining = [e for e in watchlist if e["pair"] not in triggered_pairs]
        save_watchlist(WATCHLIST_FILE, remaining)
        log.info(f"Removed {len(triggered_pairs)} triggered pair(s) from watchlist: "
                  f"{sorted(triggered_pairs)}")


# =====================================================================================
# MAIN
# =====================================================================================

def main():
    log.info("=== HA Reversal Scanner run started ===")

    # --- STEP 0: fetch all pair changes once, derive market sentiment ---
    log.info("Fetching active USDT futures pairs...")
    all_pairs = get_active_usdt_pairs()
    if not all_pairs:
        log.warning("No active pairs fetched — aborting this run.")
        return

    all_changes = fetch_all_changes(all_pairs)
    sentiment = compute_market_sentiment(all_changes)

    log.info(
        f"Market sentiment: {sentiment['total']} pairs checked | "
        f"Gainers={sentiment['gainers']} ({sentiment['gainer_pct']}%) | "
        f"Losers={sentiment['losers']} ({sentiment['loser_pct']}%) | "
        f"Threshold={MIN_LOSER_PCT_FOR_ALERT}% | "
        f"Bearish enough for alerts: {sentiment['bearish_enough']}"
    )

    # --- STEP 1: watchlist update ALWAYS runs, using the same change data ---
    watchlist = update_watchlist(all_changes)

    # --- STEP 2: only runs if market is bearish enough (loser_pct >= threshold) ---
    if sentiment["bearish_enough"]:
        run_signal_check(watchlist)
    else:
        log.info(
            f"Skipping Supertrend flip check / alerts this run — "
            f"only {sentiment['loser_pct']}% of coins are red "
            f"(need >= {MIN_LOSER_PCT_FOR_ALERT}%). Watchlist left untouched."
        )

    log.info("=== Run complete ===")


if __name__ == "__main__":
    main()