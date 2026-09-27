"""
=====================================================================================
CoinDCX Futures — Bollinger Band Breakout Scanner (LONG + SHORT)
=====================================================================================

Timezone : Asia/Kolkata (IST). The CoinDCX daily candle runs 05:30 IST -> 05:30 IST,
           so the daily watchlist scan runs once, in the 05:30 IST run.

STEP 1 — DAILY WATCHLIST (only in the 05:30 IST run)
    On the last CLOSED daily candle, with BB(20, 2) and RSI(14):
      LONG  -> close > BB upper  AND  RSI crossed up through 70   (day-2 < 70, day-1 >= 70)
               -> GainerWatchlist.json
      SHORT -> close < BB lower  AND  RSI crossed down through 30 (day-2 > 30, day-1 <= 30)
               -> LoserWatchlist.json
    Each coin is saved with that daily candle's HIGH / LOW (previous day high / low).

STEP 2 — HOURLY BREAKOUT (every other run), on the last CLOSED 1H candle:
      Gainer: close > prev day high -> LONG  alert, entry = candle HIGH, SL = prev day low
              close < prev day low  -> invalidated, removed, no alert
      Loser : close < prev day low  -> SHORT alert, entry = candle LOW,  SL = prev day high
              close > prev day high -> invalidated, removed, no alert
    A coin that alerts is removed, so the same signal is never sent twice.

Risk     : RISK_PER_TRADE_INR is the loss at SL. Quantity = risk / (entry - SL).
           Leverage only changes the capital (margin) shown, never the quantity.
Targets  : 1:2, 1:3, 1:4

Usage
-----
    python BBBreakout.py                  # normal cron run: 05:30 IST = daily scan, else hourly
    python BBBreakout.py --dry-run        # same, but alerts are logged, not sent
    python BBBreakout.py --mode daily     # force the daily watchlist scan now
    python BBBreakout.py --mode hourly    # only run the hourly breakout check
=====================================================================================
"""

import argparse
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from Indicators.RSI_indicator import calculate_rsi
from Indicators.bollinger_band_indicator import calculate_bollinger

try:
    from Telegram_Swing import Send_Swing_Telegram_Message
except ImportError:
    def Send_Swing_Telegram_Message(msg):
        print(f"\n--- TELEGRAM ALERT (no sender configured) ---\n{msg}\n")
        return False


# =====================================================================================
# CONFIG
# =====================================================================================

# ---- Risk management ----
RISK_PER_TRADE_INR = 150        # loss at SL, in Rs.
LEVERAGE = 5                    # used ONLY for the capital (margin) shown
INR_TO_USDT_RATE = None         # None = fetch live USDT/INR rate
INR_RATE_FALLBACK = 99.0        # used only if the live rate cannot be fetched

# ---- Bollinger Band (TradingView defaults: SMA basis, close, offset 0) ----
BB_LENGTH = 20
BB_MULT = 2.0

# ---- RSI (TradingView defaults) ----
RSI_LENGTH = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# ---- Schedule ----
TIMEZONE = ZoneInfo("Asia/Kolkata")
DAILY_SCAN_HOUR = 5             # the hourly run between 05:30 and 05:59 IST
DAILY_SCAN_MINUTE = 30          # also builds the watchlists

# ---- Candles ----
DAILY_LOOKBACK_DAYS = 500       # long history so RSI matches TradingView
HOURLY_LOOKBACK_HOURS = 10

# ---- Threading ----
MAX_WORKERS = 10

# ---- Watchlist files ----
LONG_WATCHLIST_FILE = "GainerWatchlist.json"
SHORT_WATCHLIST_FILE = "LoserWatchlist.json"

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
log = logging.getLogger("BBBreakout")
log.addHandler(_handler)
log.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
log.propagate = False


# =====================================================================================
# UTIL
# =====================================================================================

def safe_get(url, params=None, timeout=15):
    """Wrapper around requests.get() that never raises — returns None on any failure."""
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug(f"GET failed {url} {params}: {e}")
        return None


def display_symbol(pair):
    """B-BTC_USDT -> BTC_USDT"""
    return pair[2:] if pair.startswith("B-") else pair


def fmt_price(value):
    """More decimals for low-priced coins so levels stay readable."""
    if value >= 1:
        return f"{value:.2f}"
    if value >= 0.01:
        return f"{value:.4f}"
    return f"{value:.8f}"


def ts_to_ist(ts_seconds):
    return datetime.fromtimestamp(ts_seconds, TIMEZONE)


# =====================================================================================
# WATCHLIST
# =====================================================================================

def load_watchlist(file):
    if not os.path.exists(file):
        save_watchlist(file, [])
        log.info(f"{file} not found — created a new empty watchlist.")
        return []
    try:
        with open(file) as f:
            return json.load(f)
    except Exception:
        return []


def save_watchlist(file, data):
    with open(file, "w") as f:
        json.dump(data, f, indent=2)


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


def fetch_closed_candles(pair, resolution, lookback_seconds):
    """
    OHLC candles, oldest first, with open_ts / close_ts in seconds.
    The still-forming candle is always removed — only CLOSED candles are returned.
    """
    now_ts = int(datetime.now(TIMEZONE).timestamp())
    params = {"pair": pair, "from": now_ts - lookback_seconds, "to": now_ts,
              "resolution": resolution, "pcode": "f"}
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
    df = df[df["close_ts"] <= now_ts].reset_index(drop=True)   # drop forming candle
    return df if not df.empty else None


def get_last_closed_candle(pair, resolution, lookback_seconds):
    df = fetch_closed_candles(pair, resolution, lookback_seconds)
    return None if df is None else df.iloc[-1]


# =====================================================================================
# RISK
# =====================================================================================

def calculate_position(entry, sl, side, rate,
                       risk_per_trade=RISK_PER_TRADE_INR, leverage=LEVERAGE):
    """
    Quantity is sized from RISK ONLY. Leverage is applied only to the capital.

        risk_per_unit   = entry - sl  (LONG)  |  sl - entry  (SHORT)
        quantity        = risk_per_trade / risk_per_unit
        position_value  = entry * quantity
        margin_required = position_value / leverage     (shown as "Capital")

    CoinDCX futures prices are in USDT, so entry / SL are converted to Rs.
    with `rate` first — this keeps every money value in the formulas in Rs.
    """
    entry_inr = entry * rate
    sl_inr = sl * rate
    risk_per_unit = entry_inr - sl_inr if side == "LONG" else sl_inr - entry_inr

    if risk_per_unit <= 0:
        return None

    quantity = risk_per_trade / risk_per_unit
    position_value = entry_inr * quantity
    return {
        "quantity": quantity,
        "position_value": position_value,
        "margin_required": position_value / leverage,
        "loss_at_sl": quantity * risk_per_unit,
    }


def calculate_targets(entry, sl, side):
    """Returns (T1 1:2, T2 1:3, T3 1:4)."""
    risk = abs(entry - sl)
    sign = 1 if side == "LONG" else -1
    return tuple(entry + sign * risk * r for r in (2, 3, 4))


# =====================================================================================
# STRATEGY RULES (pure functions — take DataFrames / rows, easy to backtest)
# =====================================================================================

MIN_DAILY_CANDLES = max(BB_LENGTH, RSI_LENGTH + 1) + 1


def evaluate_daily_setup(daily):
    """
    daily: CLOSED daily candles, oldest first. Last row = previous day,
    row before it = day before previous day. Returns dict or None.
    """
    if daily is None or len(daily) < MIN_DAILY_CANDLES:
        return None

    df = calculate_bollinger(daily.copy(), length=BB_LENGTH, mult=BB_MULT)
    df = calculate_rsi(df, length=RSI_LENGTH)

    day = df.iloc[-1]
    before = df.iloc[-2]
    if pd.isna(day["BB_upper"]) or pd.isna(day["RSI"]) or pd.isna(before["RSI"]):
        return None

    setup = None
    if day["close"] > day["BB_upper"] and before["RSI"] < RSI_OVERBOUGHT <= day["RSI"]:
        setup = "LONG"
    elif day["close"] < day["BB_lower"] and before["RSI"] > RSI_OVERSOLD >= day["RSI"]:
        setup = "SHORT"

    return {
        "setup": setup,
        "close": float(day["close"]),
        "upper_band": float(day["BB_upper"]),
        "lower_band": float(day["BB_lower"]),
        "rsi": float(day["RSI"]),
        "previous_rsi": float(before["RSI"]),
        "high": float(day["high"]),
        "low": float(day["low"]),
    }


def evaluate_hourly_candle(setup, pdh, pdl, candle):
    """Returns "BREAKOUT", "INVALIDATED" or None for one CLOSED 1H candle (uses CLOSE only)."""
    close = candle["close"]
    if setup == "LONG":
        if close > pdh:
            return "BREAKOUT"
        if close < pdl:
            return "INVALIDATED"
    else:
        if close < pdl:
            return "BREAKOUT"
        if close > pdh:
            return "INVALIDATED"
    return None


def build_trade(setup, pdh, pdl, candle, rate):
    """LONG: entry = breakout candle HIGH, SL = prev day LOW. SHORT: entry = LOW, SL = prev day HIGH."""
    if setup == "LONG":
        entry, sl = float(candle["high"]), pdl
    else:
        entry, sl = float(candle["low"]), pdh

    pos = calculate_position(entry, sl, setup, rate)
    if pos is None:
        return None

    t1, t2, t3 = calculate_targets(entry, sl, setup)
    return {"side": setup, "entry": entry, "sl": sl, "t1": t1, "t2": t2, "t3": t3, "pos": pos}


# =====================================================================================
# TELEGRAM MESSAGE
# =====================================================================================

def build_message(symbol, trade):
    return (
        f"🚨 Bollinger Band Breakout\n\n"
        f"Setup Type - {trade['side']}\n\n"
        f"Coin Name - {symbol}\n\n"
        f"Entry - {fmt_price(trade['entry'])}\n"
        f"SL - {fmt_price(trade['sl'])}\n\n"
        f"T1 - {fmt_price(trade['t1'])} (1:2)\n"
        f"T2 - {fmt_price(trade['t2'])} (1:3)\n"
        f"T3 - {fmt_price(trade['t3'])} (1:4)\n\n"
        f"Leverage - {LEVERAGE}x\n"
        f"Capital - ₹{trade['pos']['margin_required']:,.2f}"
    )


def send_alert(message):
    if DRY_RUN:
        log.info(f"[DRY RUN] Telegram message not sent:\n{message}")
        return True
    return bool(Send_Swing_Telegram_Message(message))


# =====================================================================================
# STEP 1 — DAILY WATCHLIST (05:30 IST)
# =====================================================================================

def is_daily_scan_time(now):
    """True only for the one hourly run between 05:30 and 05:59 IST."""
    return now.hour == DAILY_SCAN_HOUR and now.minute >= DAILY_SCAN_MINUTE


def analyze_daily(pair):
    daily = fetch_closed_candles(pair, "1D", DAILY_LOOKBACK_DAYS * 86400)
    if daily is None:
        return pair, None

    # the last closed daily candle must be the day that just ended — not stale data
    now_ts = int(datetime.now(TIMEZONE).timestamp())
    if now_ts - int(daily.iloc[-1]["close_ts"]) >= 86400:
        return pair, None

    return pair, evaluate_daily_setup(daily)


def add_to_watchlist(watchlist, pair, r, name):
    """Adds the coin with its previous day high / low. Skips coins already on the list."""
    if any(e["pair"] == pair for e in watchlist):
        log.info(f"{display_symbol(pair)}: already in {name} — skipped")
        return
    watchlist.append({
        "pair": pair,
        "previous_day_high": r["high"],
        "previous_day_low": r["low"],
    })
    log.info(f"{display_symbol(pair)}: added to {name}")


def run_daily_scan():
    log.info("[05:30 IST] Daily watchlist scan started")

    pairs = get_active_usdt_coins()
    if not pairs:
        log.error("No active futures pairs fetched — daily scan skipped.")
        return
    log.info(f"Scanning {len(pairs)} active USDT-M futures pairs on the 1D timeframe")

    gainer_watch = load_watchlist(LONG_WATCHLIST_FILE)
    loser_watch = load_watchlist(SHORT_WATCHLIST_FILE)
    long_count = short_count = 0

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(analyze_daily, p) for p in pairs]
        for f in as_completed(futures):
            pair, r = f.result()
            if not r or not r["setup"]:
                continue

            band = r["upper_band"] if r["setup"] == "LONG" else r["lower_band"]
            log.info(
                f"{display_symbol(pair)}: Close = {fmt_price(r['close'])} | "
                f"BB {'Upper' if r['setup'] == 'LONG' else 'Lower'} = {fmt_price(band)} | "
                f"RSI Previous = {r['previous_rsi']:.2f} | RSI Current = {r['rsi']:.2f} | "
                f"{r['setup']} candidate = YES"
            )
            if r["setup"] == "LONG":
                add_to_watchlist(gainer_watch, pair, r, "GainerWatchlist")
                long_count += 1
            else:
                add_to_watchlist(loser_watch, pair, r, "LoserWatchlist")
                short_count += 1

    save_watchlist(LONG_WATCHLIST_FILE, gainer_watch)
    save_watchlist(SHORT_WATCHLIST_FILE, loser_watch)
    log.info(f"LONG candidates: {long_count} | SHORT candidates: {short_count}")
    log.info(f"GainerWatchlist updated — {len(gainer_watch)} coin(s)")
    log.info(f"LoserWatchlist updated — {len(loser_watch)} coin(s)")


# =====================================================================================
# STEP 2 — HOURLY BREAKOUT CHECK
# =====================================================================================

def check_watchlist_for_signals(watchlist, setup, rate):
    """Returns (updated_watchlist, alerts)."""
    name = "GainerWatchlist" if setup == "LONG" else "LoserWatchlist"

    def process_pair(entry):
        pair = entry["pair"]
        symbol = display_symbol(pair)
        pdh, pdl = entry["previous_day_high"], entry["previous_day_low"]

        candle = get_last_closed_candle(pair, "60", HOURLY_LOOKBACK_HOURS * 3600)
        if candle is None:
            log.warning(f"{symbol}: no 1H candle data — kept")
            return ("KEEP", entry)

        candle_time = ts_to_ist(int(candle["open_ts"])).strftime("%Y-%m-%d %H:%M")
        event = evaluate_hourly_candle(setup, pdh, pdl, candle)

        if event is None:
            log.info(f"{symbol} ({setup}): 1H close {fmt_price(candle['close'])} inside "
                     f"PDL {fmt_price(pdl)} / PDH {fmt_price(pdh)} — still watching")
            return ("KEEP", entry)

        if event == "INVALIDATED":
            log.info(f"{symbol}: {setup} setup INVALIDATED — 1H close {fmt_price(candle['close'])} "
                     f"at {candle_time} IST. Removed from {name}, no signal sent.")
            return ("REMOVE", entry)

        trade = build_trade(setup, pdh, pdl, candle, rate)
        if trade is None:
            log.warning(f"{symbol}: breakout at {candle_time} IST but Entry/SL are invalid — removed")
            return ("REMOVE", entry)

        pos = trade["pos"]
        log.info(
            f"{symbol} {setup} BREAKOUT CONFIRMED\n"
            f"    Previous Day High = {fmt_price(pdh)} | Previous Day Low = {fmt_price(pdl)}\n"
            f"    1H candle {candle_time} IST: High = {fmt_price(candle['high'])} | "
            f"Low = {fmt_price(candle['low'])} | Close = {fmt_price(candle['close'])}\n"
            f"    Entry = {fmt_price(trade['entry'])} | SL = {fmt_price(trade['sl'])} | "
            f"Risk per unit = {fmt_price(abs(trade['entry'] - trade['sl']))}\n"
            f"    Quantity = {pos['quantity']:.6f} | Position = ₹{pos['position_value']:,.2f} | "
            f"Capital ({LEVERAGE}x) = ₹{pos['margin_required']:,.2f} | "
            f"Loss at SL = ₹{pos['loss_at_sl']:,.2f}"
        )
        return ("SIGNAL", entry, build_message(symbol, trade))

    updated_watchlist = []
    alerts = []

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(process_pair, e) for e in watchlist]
        for f in as_completed(futures):
            res = f.result()
            if res[0] == "KEEP":
                updated_watchlist.append(res[1])
            elif res[0] == "SIGNAL":
                # Signal fired -> coin is removed from watchlist (not re-added)
                alerts.append((res[1], res[2]))

    return updated_watchlist, alerts


def run_hourly_scan():
    gainer_watch = load_watchlist(LONG_WATCHLIST_FILE)
    loser_watch = load_watchlist(SHORT_WATCHLIST_FILE)
    log.info(f"Hourly scan — GainerWatchlist: {len(gainer_watch)} | LoserWatchlist: {len(loser_watch)}")
    if not gainer_watch and not loser_watch:
        return

    rate = get_inr_rate()

    for file, watchlist, setup in [(LONG_WATCHLIST_FILE, gainer_watch, "LONG"),
                                   (SHORT_WATCHLIST_FILE, loser_watch, "SHORT")]:
        updated, alerts = check_watchlist_for_signals(watchlist, setup, rate)
        for entry, msg in alerts:
            if send_alert(msg):
                log.info(f"{display_symbol(entry['pair'])}: Telegram alert sent. Coin removed.")
            else:
                updated.append(entry)   # send failed -> keep the coin
                log.warning(f"{display_symbol(entry['pair'])}: Telegram send FAILED — kept")
        save_watchlist(file, updated)

    log.info("Hourly scan done")


# =====================================================================================
# MAIN
# =====================================================================================

def main():
    global DRY_RUN

    parser = argparse.ArgumentParser(description="CoinDCX Bollinger Band Breakout scanner")
    parser.add_argument("--mode", choices=["auto", "daily", "hourly"], default="auto",
                        help="auto = 1D watchlist scan in the 05:30 IST run, 1H breakout check in "
                             "every other run (default); daily / hourly = force one of them now")
    parser.add_argument("--dry-run", action="store_true", help="log alerts instead of sending them")
    args = parser.parse_args()
    DRY_RUN = DRY_RUN or args.dry_run

    now = datetime.now(TIMEZONE)
    log.info(f"=== BB Breakout run started — mode={args.mode}{' [DRY RUN]' if DRY_RUN else ''} ===")

    if args.mode == "auto":
        mode = "daily" if is_daily_scan_time(now) else "hourly"
    else:
        mode = args.mode

    # 05:30 IST run -> 1D timeframe only (BB + RSI watchlist scan)
    # every other run -> 1H timeframe only (breakout check)
    if mode == "daily":
        run_daily_scan()
    else:
        run_hourly_scan()

    log.info("=== Run complete ===")


if __name__ == "__main__":
    main()
