"""
=====================================================================================
CoinDCX Futures — Bollinger Band Reversal Scanner (SHORT)
=====================================================================================

Timezone : Asia/Kolkata (IST). The CoinDCX daily candle runs 05:30 IST -> 05:30 IST.
State    : ONE file only — reversal.json (written atomically).

STEP 1 — DAILY BB BREAKOUT (05:30 IST run), on CLOSED daily candles, BB(20, 2):
      day-before-yesterday close < its upper BB   (a wick above the band is fine)
      yesterday close            > its upper BB
      -> added to reversal.json with status "NO" (no duplicates)

STEP 2 — DAILY EMA 9 CONFIRMATION (05:30 IST run), for every "NO" coin:
      previous day close < previous day EMA 9
      -> status "YES", trigger_price = previous day LOW
         (+ ema_confirmation_date, confirmation_close, ema_9, stop_loss)
      otherwise the record stays "NO" and is checked again tomorrow.
      A "YES" record is never reset to "NO".

STEP 3 — 1H BREAKDOWN (every run, incl. the 05:30 run, after steps 1-2),
      for every "YES" coin, on every CLOSED 1H candle not yet checked:
      1H close < trigger_price -> SHORT Telegram alert, record removed.
      The CLOSE is used, never the LOW. Each 1H candle is processed once
      (last_checked_ts). If Telegram fails the record is kept and the same
      breakdown is alerted on the next run.

Trade    : SHORT. Entry = breakdown 1H close.
           SL = HIGH of the daily candle that closed below EMA 9 (confirmation day).
           RISK_PER_TRADE_INR is the loss at SL. Quantity = risk / (SL - entry).
           Leverage only changes the capital (margin) shown, never the quantity.
Targets  : 1:2, 1:3, 1:4 (a target at or below 0 is shown as n/a)

Usage
-----
    python ReversalBuySell.py                 # cron run: 05:30 IST = daily + hourly, else hourly
    python ReversalBuySell.py --dry-run       # same, but alerts are logged, not sent
    python ReversalBuySell.py --mode daily    # force steps 1-2 now (then step 3)
    python ReversalBuySell.py --mode hourly   # only step 3
=====================================================================================
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from Indicators.bollinger_band_indicator import calculate_bollinger

try:
    from Telegram_Reversal import Send_Reversal_Telegram_Message
except ImportError:
    def Send_Reversal_Telegram_Message(msg):
        print(f"\n--- TELEGRAM ALERT (no sender configured) ---\n{msg}\n")
        return False


# =====================================================================================
# CONFIG
# =====================================================================================

# ---- Risk management ----
RISK_PER_TRADE_INR = 100        # loss at SL, in Rs.
LEVERAGE = 5                    # used ONLY for the capital (margin) shown
INR_TO_USDT_RATE = None         # None = fetch live USDT/INR rate
INR_RATE_FALLBACK = 99.0        # used only if the live rate cannot be fetched

# ---- Bollinger Band (TradingView defaults: SMA basis, close, offset 0) ----
BB_LENGTH = 20
BB_MULT = 2.0

# ---- EMA ----
EMA_LENGTH = 9

# ---- Schedule ----
TIMEZONE = ZoneInfo("Asia/Kolkata")
DAILY_SCAN_HOUR = 5             # the hourly run between 05:30 and 05:59 IST
DAILY_SCAN_MINUTE = 30          # also runs the daily scan + EMA confirmation

# ---- Candles ----
DAILY_LOOKBACK_DAYS = 500       # long history so BB / EMA match TradingView
HOURLY_LOOKBACK_HOURS = 10      # catch-up window after missed runs; older 1H candles are skipped

# ---- Threading / network ----
MAX_WORKERS = 10
HTTP_RETRIES = 3

# ---- State file (the ONLY file this strategy writes) ----
REVERSAL_FILE = "reversal.json"

# ---- Dry run: log alerts instead of sending (or set env DRY_RUN=1) ----
DRY_RUN = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")

# ---- API endpoints ----
ACTIVE_INSTRUMENTS_URL = (
    "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
    "active_instruments?margin_currency_short_name[]=USDT"
)
CANDLES_URL = "https://public.coindcx.com/market_data/candlesticks"
TICKER_URL = "https://api.coindcx.com/exchange/ticker"

RESOLUTION_SECONDS = {"60": 3600, "1D": 86400}


# =====================================================================================
# LOGGING (timestamps always in IST, whatever the server timezone)
# =====================================================================================

class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        return datetime.fromtimestamp(record.created, TIMEZONE).strftime("%Y-%m-%d %H:%M:%S IST")


_handler = logging.StreamHandler()
_handler.setFormatter(ISTFormatter("%(asctime)s | %(levelname)s | %(message)s"))
log = logging.getLogger("BBReversal")
log.addHandler(_handler)
log.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
log.propagate = False


# =====================================================================================
# UTIL
# =====================================================================================

def safe_get(url, params=None, timeout=15, retries=HTTP_RETRIES):
    """requests.get() with retries + backoff. Never raises — returns None on failure."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.debug(f"GET failed ({attempt}/{retries}) {url} {params}: {e}")
            if attempt < retries:
                time.sleep(attempt)
    return None


def display_symbol(pair):
    """B-BTC_USDT -> BTC_USDT"""
    return pair[2:] if pair.startswith("B-") else pair


def fmt_price(value):
    """More decimals for low-priced coins so levels stay readable."""
    if value is None or value <= 0:
        return "n/a"
    if value >= 1:
        return f"{value:.2f}"
    if value >= 0.01:
        return f"{value:.4f}"
    return f"{value:.8f}"


def ts_to_ist(ts_seconds):
    return datetime.fromtimestamp(ts_seconds, TIMEZONE)


def now_ts():
    return int(datetime.now(TIMEZONE).timestamp())


# =====================================================================================
# STATE — single reversal.json, atomic writes
# =====================================================================================

def load_records():
    """
    Returns the list of records. A missing file starts empty.
    A corrupt file stops the run (exit 1) — it is never silently overwritten.
    """
    if not os.path.exists(REVERSAL_FILE):
        log.info(f"{REVERSAL_FILE} not found — starting with an empty list.")
        return []
    try:
        with open(REVERSAL_FILE) as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("top level is not a list")
        return data
    except Exception as e:
        log.error(f"{REVERSAL_FILE} is unreadable ({e}) — fix or delete it. Nothing was changed.")
        sys.exit(1)


def save_records(records):
    """Atomic write: temp file in the same folder, then os.replace()."""
    folder = os.path.dirname(os.path.abspath(REVERSAL_FILE))
    fd, tmp = tempfile.mkstemp(prefix=".reversal-", suffix=".json", dir=folder)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(records, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, 0o644)                  # mkstemp creates 0600; keep a normal file mode
        os.replace(tmp, REVERSAL_FILE)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


def new_record(pair, breakout_date):
    return {
        "symbol": display_symbol(pair),
        "pair": pair,
        "breakout_date": breakout_date,
        "status": "NO",
        "trigger_price": None,
        "ema_confirmation_date": None,
        "confirmation_close": None,
        "ema_9": None,
        "stop_loss": None,
        "last_checked_ts": None,
    }


# =====================================================================================
# API
# =====================================================================================

def get_active_usdt_coins():
    """Active USDT-M futures pairs only (B-XXX_USDT)."""
    data = safe_get(ACTIVE_INSTRUMENTS_URL, timeout=30)
    if not isinstance(data, list):
        return []
    pairs = [x["pair"] if isinstance(x, dict) else x for x in data]
    return [p for p in pairs if isinstance(p, str) and p.startswith("B-") and p.endswith("_USDT")]


def get_inr_rate():
    """Live USDT -> INR rate, used to convert the Rs. risk against USDT prices."""
    if INR_TO_USDT_RATE is not None:
        return INR_TO_USDT_RATE
    data = safe_get(TICKER_URL, timeout=10)
    if isinstance(data, list):
        for m in data:
            if isinstance(m, dict) and m.get("market") == "USDTINR":
                try:
                    return float(m.get("last_price"))
                except (TypeError, ValueError):
                    break
    log.warning(f"Could not fetch live USDT/INR rate — using fallback {INR_RATE_FALLBACK}")
    return INR_RATE_FALLBACK


def fetch_closed_candles(pair, resolution, from_ts):
    """
    OHLC candles from `from_ts` to now, oldest first, with open_ts / close_ts in seconds
    and an IST `date` column. Only CLOSED candles are returned.
    """
    now = now_ts()
    params = {"pair": pair, "from": int(from_ts), "to": now, "resolution": resolution, "pcode": "f"}
    data = safe_get(CANDLES_URL, params)
    if not isinstance(data, dict) or not data.get("data"):
        return None

    df = pd.DataFrame(data["data"])
    if not {"open", "high", "low", "close", "time"}.issubset(df.columns):
        return None

    for col in ["open", "high", "low", "close", "time"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close", "time"])

    df["open_ts"] = df["time"].astype("int64")
    df.loc[df["open_ts"] > 10**12, "open_ts"] //= 1000        # ms -> s
    df["close_ts"] = df["open_ts"] + RESOLUTION_SECONDS[resolution]

    df = df.drop_duplicates("open_ts").sort_values("open_ts")
    df = df[df["close_ts"] <= now].reset_index(drop=True)      # drop forming candle
    if df.empty:
        return None
    df["date"] = df["open_ts"].map(lambda t: ts_to_ist(int(t)).strftime("%Y-%m-%d"))
    return df


# =====================================================================================
# INDICATORS / RULES (pure functions — easy to backtest)
# =====================================================================================

MIN_DAILY_CANDLES = max(BB_LENGTH, EMA_LENGTH) + 1


def add_daily_indicators(daily):
    df = calculate_bollinger(daily.copy(), length=BB_LENGTH, mult=BB_MULT)
    df["EMA9"] = df["close"].ewm(span=EMA_LENGTH, adjust=False, min_periods=EMA_LENGTH).mean()
    return df


def is_bb_breakout(df):
    """day-before-yesterday close < its upper BB  AND  yesterday close > its upper BB."""
    if len(df) < MIN_DAILY_CANDLES:
        return False
    day, before = df.iloc[-1], df.iloc[-2]
    if pd.isna(day["BB_upper"]) or pd.isna(before["BB_upper"]):
        return False
    return before["close"] < before["BB_upper"] and day["close"] > day["BB_upper"]


def ema_confirmation(df, breakout_date):
    """
    Returns the confirmation fields if the last closed daily candle (a day AFTER the
    breakout day) closed below its EMA 9, else None.
    """
    day = df.iloc[-1]
    if day["date"] <= breakout_date or pd.isna(day["EMA9"]):
        return None
    if not day["close"] < day["EMA9"]:
        return None

    return {
        "status": "YES",
        "trigger_price": float(day["low"]),
        "ema_confirmation_date": day["date"],
        "confirmation_close": float(day["close"]),
        "ema_9": float(day["EMA9"]),
        "stop_loss": float(day["high"]),             # HIGH of the candle that closed below EMA 9
        "last_checked_ts": int(day["close_ts"]),     # 1H monitoring starts after this day
    }


def find_breakdown(hourly, trigger_price, last_checked_ts):
    """First CLOSED 1H candle after last_checked_ts whose CLOSE < trigger_price, else None."""
    fresh = hourly[hourly["open_ts"] >= last_checked_ts]
    hits = fresh[fresh["close"] < trigger_price]
    return None if hits.empty else hits.iloc[0]


# =====================================================================================
# RISK
# =====================================================================================

def calculate_position(entry, sl, rate, risk_per_trade=RISK_PER_TRADE_INR, leverage=LEVERAGE):
    """SHORT sizing from risk only. Prices are USDT, converted to Rs. with `rate`."""
    risk_per_unit = (sl - entry) * rate
    if risk_per_unit <= 0:
        return None
    quantity = risk_per_trade / risk_per_unit
    position_value = entry * rate * quantity
    return {
        "quantity": quantity,
        "position_value": position_value,
        "margin_required": position_value / leverage,
        "loss_at_sl": quantity * risk_per_unit,
    }


def build_trade(record, candle, rate):
    entry, sl = float(candle["close"]), float(record["stop_loss"])
    pos = calculate_position(entry, sl, rate)
    if pos is None:
        return None
    risk = sl - entry
    t1, t2, t3 = (entry - risk * r for r in (2, 3, 4))
    return {"entry": entry, "sl": sl, "t1": t1, "t2": t2, "t3": t3, "pos": pos}


# =====================================================================================
# TELEGRAM
# =====================================================================================

def build_message(record, candle, trade):
    start = ts_to_ist(int(candle["open_ts"])).strftime("%Y-%m-%d %H:%M")
    end = ts_to_ist(int(candle["close_ts"])).strftime("%H:%M")
    msg = (
        f"🔻 Bollinger Band Reversal\n\n"
        f"Setup Type - SHORT\n\n"
        f"Coin Name - {record['symbol']}\n"
        f"Breakout Date - {record['breakout_date']}\n"
        f"Daily EMA 9 - {fmt_price(record['ema_9'])}\n"
        f"Trigger (prev day low) - {fmt_price(record['trigger_price'])}\n"
        f"1H Close - {fmt_price(float(candle['close']))}\n"
        f"Breakdown Candle - {start}–{end} IST\n"
    )
    if trade:
        msg += (
            f"\nEntry - {fmt_price(trade['entry'])}\n"
            f"SL - {fmt_price(trade['sl'])}\n\n"
            f"T1 - {fmt_price(trade['t1'])} (1:2)\n"
            f"T2 - {fmt_price(trade['t2'])} (1:3)\n"
            f"T3 - {fmt_price(trade['t3'])} (1:4)\n\n"
            f"Quantity - {trade['pos']['quantity']:.6g}\n"
            f"Leverage - {LEVERAGE}x\n"
            f"Capital - ₹{trade['pos']['margin_required']:,.2f}"
        )
    return msg


def send_alert(message):
    if DRY_RUN:
        log.info(f"[DRY RUN] Telegram message not sent:\n{message}")
        return True
    return bool(Send_Reversal_Telegram_Message(message))


# =====================================================================================
# STEP 1 + 2 — DAILY SCAN (05:30 IST)
# =====================================================================================

def is_daily_scan_time(now):
    """True only for the one hourly run between 05:30 and 05:59 IST."""
    return now.hour == DAILY_SCAN_HOUR and now.minute >= DAILY_SCAN_MINUTE


def load_daily(pair):
    """Closed daily candles with BB + EMA9, or None if missing / stale."""
    daily = fetch_closed_candles(pair, "1D", now_ts() - DAILY_LOOKBACK_DAYS * 86400)
    if daily is None or len(daily) < MIN_DAILY_CANDLES:
        return pair, None
    # the last closed daily candle must be the day that just ended — not stale data
    if now_ts() - int(daily.iloc[-1]["close_ts"]) >= 86400:
        return pair, None
    return pair, add_daily_indicators(daily)


def run_daily_scan(records):
    """Mutates and returns `records`."""
    log.info("[05:30 IST] Daily scan started — EMA 9 confirmation + BB breakout")

    pairs = get_active_usdt_coins()
    if not pairs:
        log.error("No active futures pairs fetched — new-breakout scan skipped.")

    known = {r["pair"] for r in records}
    to_fetch = sorted(set(pairs) | {r["pair"] for r in records if r["status"] == "NO"})
    log.info(f"Fetching 1D candles for {len(to_fetch)} pairs")

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        daily_by_pair = dict(executor.map(load_daily, to_fetch))

    # ---- STEP 2: EMA 9 confirmation for "NO" records ----
    confirmed = 0
    for r in records:
        if r["status"] != "NO":
            continue                                  # YES is never reset
        df = daily_by_pair.get(r["pair"])
        if df is None:
            log.warning(f"{r['symbol']}: no fresh daily data — kept as NO")
            continue
        day = df.iloc[-1]
        conf = ema_confirmation(df, r["breakout_date"])
        if conf:
            r.update(conf)
            confirmed += 1
            log.info(f"{r['symbol']}: EMA 9 CONFIRMED — close {fmt_price(conf['confirmation_close'])} "
                     f"< EMA9 {fmt_price(conf['ema_9'])} | trigger {fmt_price(conf['trigger_price'])} "
                     f"| SL {fmt_price(conf['stop_loss'])} -> status YES")
        else:
            log.info(f"{r['symbol']}: close {fmt_price(float(day['close']))} vs EMA9 "
                     f"{fmt_price(float(day['EMA9']))} — not confirmed, stays NO")

    # ---- STEP 1: new BB breakouts ----
    added = 0
    for pair in pairs:
        df = daily_by_pair.get(pair)
        if df is None or pair in known or not is_bb_breakout(df):
            continue
        day, before = df.iloc[-1], df.iloc[-2]
        records.append(new_record(pair, day["date"]))
        known.add(pair)
        added += 1
        log.info(f"{display_symbol(pair)}: BB BREAKOUT — prev close {fmt_price(float(before['close']))} "
                 f"< upper {fmt_price(float(before['BB_upper']))}, last close "
                 f"{fmt_price(float(day['close']))} > upper {fmt_price(float(day['BB_upper']))} -> added (NO)")

    log.info(f"Daily scan done — new breakouts: {added} | newly confirmed: {confirmed} | "
             f"total records: {len(records)}")
    return records


# =====================================================================================
# STEP 3 — 1H BREAKDOWN MONITORING
# =====================================================================================

def check_record(record):
    """Returns (record, breakdown_candle_or_None, newest_closed_open_ts_or_None)."""
    window_start = now_ts() - HOURLY_LOOKBACK_HOURS * 3600
    last_checked = int(record.get("last_checked_ts") or window_start)
    if last_checked < window_start:
        log.warning(f"{record['symbol']}: 1H candles before "
                    f"{ts_to_ist(window_start).strftime('%Y-%m-%d %H:%M')} IST were missed — skipped")
        last_checked = window_start

    hourly = fetch_closed_candles(record["pair"], "60", last_checked - 3600)
    if hourly is None:
        return record, None, None
    candle = find_breakdown(hourly, record["trigger_price"], last_checked)
    return record, candle, int(hourly.iloc[-1]["close_ts"])


def run_hourly_scan(records):
    """Mutates and returns `records`."""
    active = [r for r in records if r["status"] == "YES"]
    log.info(f"Hourly scan — {len(active)} confirmed (YES) / {len(records)} total")
    if not active:
        return records

    rate = get_inr_rate()
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        results = list(executor.map(check_record, active))

    remove = set()
    for record, candle, newest_close_ts in results:
        symbol = record["symbol"]
        if newest_close_ts is None:
            log.warning(f"{symbol}: no 1H candle data — kept")
            continue

        if candle is None:
            record["last_checked_ts"] = newest_close_ts          # never re-process these candles
            log.info(f"{symbol}: no 1H close below trigger {fmt_price(record['trigger_price'])} — watching")
            continue

        trade = build_trade(record, candle, rate)
        if trade is None:
            log.warning(f"{symbol}: breakdown but SL {fmt_price(record['stop_loss'])} is not above "
                        f"entry — alert sent without trade levels")
        msg = build_message(record, candle, trade)

        if send_alert(msg):
            remove.add(record["pair"])
            log.info(f"{symbol}: BREAKDOWN — 1H close {fmt_price(float(candle['close']))} < trigger "
                     f"{fmt_price(record['trigger_price'])}. Telegram alert sent, record removed.")
        else:
            # last_checked_ts NOT advanced -> the same breakdown is alerted next run
            log.warning(f"{symbol}: Telegram send FAILED — record kept, will retry")

    return [r for r in records if r["pair"] not in remove]


# =====================================================================================
# MAIN
# =====================================================================================

def main():
    global DRY_RUN

    parser = argparse.ArgumentParser(description="CoinDCX Bollinger Band Reversal scanner")
    parser.add_argument("--mode", choices=["auto", "daily", "hourly"], default="auto",
                        help="auto = daily scan + 1H check in the 05:30 IST run, 1H check only in "
                             "every other run (default); daily / hourly = force one now")
    parser.add_argument("--dry-run", action="store_true", help="log alerts instead of sending them")
    args = parser.parse_args()
    DRY_RUN = DRY_RUN or args.dry_run

    now = datetime.now(TIMEZONE)
    mode = args.mode
    if mode == "auto":
        mode = "daily" if is_daily_scan_time(now) else "hourly"
    log.info(f"=== BB Reversal run started — mode={mode}{' [DRY RUN]' if DRY_RUN else ''} ===")

    records = load_records()

    # Each step saves on its own, so a crash in one never loses the other's work.
    if mode == "daily":
        records = run_daily_scan(records)
        save_records(records)

    records = run_hourly_scan(records)
    save_records(records)

    log.info("=== Run complete ===")


if __name__ == "__main__":
    main()
