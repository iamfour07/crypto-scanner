"""
=====================================================================================
CoinDCX Futures — XAU_USDT RSI(52) / EMA(52) Buy Scanner (30 min)
=====================================================================================

Indicator : TradingView RSI, Length 52, Smoothing Line = EMA, Smoothing Length 52,
            on CLOSED 30-minute candles of B-XAU_USDT.

Rules (run every 30 minutes, checks the last CLOSED 30m candle once):
    No active watch:
        RSI < EMA52(RSI)            -> BUY alert, save this RSI as last_rsi
    Active watch:
        RSI >= EMA52(RSI)           -> SELL / book profit alert, watch closed;
                                       the next close below EMA52 starts a new cycle
        RSI <  last_rsi             -> BUY alert again, last_rsi = this RSI
        RSI >= last_rsi             -> no alert

Money     : fixed CAPITAL_INR margin x LEVERAGE = position size per BUY alert.
            Quantity = position / (XAU price x USDT/INR rate). No SL.

State     : IntradayWatchlist.json (written atomically).
            If Telegram fails, state is not updated, so the alert is retried next run.

Usage
-----
    python NewMomentum.py              # normal run (cron every 30 min)
    python NewMomentum.py --dry-run    # log the alert instead of sending it
=====================================================================================
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from Indicators.RSI_indicator import calculate_rsi

try:
    from Telegram_Swing import Send_Swing_Telegram_Message
except ImportError:
    def Send_Swing_Telegram_Message(msg):
        print(f"\n--- TELEGRAM ALERT (no sender configured) ---\n{msg}\n")
        return False


# =====================================================================================
# CONFIG
# =====================================================================================

PAIR = "B-XAU_USDT"
RESOLUTION = "30"                # 30-minute candles
CANDLE_SECONDS = 30 * 60
LOOKBACK_DAYS = 60               # long history so RSI / EMA match TradingView

RSI_LENGTH = 52
RSI_EMA_LENGTH = 52              # EMA smoothing line on the RSI

# ---- Money ----
CAPITAL_INR = 300                # fixed margin per alert, in Rs.
LEVERAGE = 7
INR_TO_USDT_RATE = None          # None = fetch live USDT/INR rate
INR_RATE_FALLBACK = 99.0         # used only if the live rate cannot be fetched

WATCHLIST_FILE = "IntradayWatchlist.json"
TIMEZONE = ZoneInfo("Asia/Kolkata")
HTTP_RETRIES = 3

DRY_RUN = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")

CANDLES_URL = "https://public.coindcx.com/market_data/candlesticks"
TICKER_URL = "https://api.coindcx.com/exchange/ticker"


# =====================================================================================
# LOGGING (IST timestamps)
# =====================================================================================

class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        return datetime.fromtimestamp(record.created, TIMEZONE).strftime("%Y-%m-%d %H:%M:%S IST")


_handler = logging.StreamHandler()
_handler.setFormatter(ISTFormatter("%(asctime)s | %(levelname)s | %(message)s"))
log = logging.getLogger("XAU_RSI")
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
            log.debug(f"GET failed ({attempt}/{retries}) {url}: {e}")
            if attempt < retries:
                time.sleep(attempt)
    return None


def ist(ts):
    return datetime.fromtimestamp(ts, TIMEZONE)


def now_ts():
    return int(time.time())


# =====================================================================================
# STATE — IntradayWatchlist.json
# =====================================================================================
#   {
#     "last_processed_ts": <open_ts of the last 30m candle handled>,
#     "active": true/false,
#     "last_rsi": <RSI of the last alert>,
#     "alert_count": <alerts in the current watch>,
#     "watch_started": "YYYY-MM-DD HH:MM IST",
#     "last_alert": "YYYY-MM-DD HH:MM IST",
#     "buy_prices": [<close of each BUY alert in the current cycle>]
#   }

EMPTY_STATE = {"last_processed_ts": None, "active": False, "last_rsi": None,
               "alert_count": 0, "watch_started": None, "last_alert": None,
               "buy_prices": []}


def load_state():
    if not os.path.exists(WATCHLIST_FILE):
        return dict(EMPTY_STATE)
    try:
        with open(WATCHLIST_FILE) as f:
            data = json.load(f)
    except Exception as e:
        log.error(f"{WATCHLIST_FILE} is unreadable ({e}) — fix or delete it. Nothing was changed.")
        sys.exit(1)
    if not isinstance(data, dict):
        log.error(f"{WATCHLIST_FILE} is not a JSON object — fix or delete it.")
        sys.exit(1)
    return {**EMPTY_STATE, **data}


def save_state(state):
    """Atomic write: temp file in the same folder, then os.replace()."""
    folder = os.path.dirname(os.path.abspath(WATCHLIST_FILE))
    fd, tmp = tempfile.mkstemp(prefix=".intraday-", suffix=".json", dir=folder)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, WATCHLIST_FILE)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


# =====================================================================================
# DATA
# =====================================================================================

def fetch_closed_candles():
    """CLOSED 30m candles, oldest first, with RSI and RSI_MA (EMA 52 of RSI)."""
    now = now_ts()
    params = {"pair": PAIR, "from": now - LOOKBACK_DAYS * 86400, "to": now,
              "resolution": RESOLUTION, "pcode": "f"}
    data = safe_get(CANDLES_URL, params)
    if not isinstance(data, dict) or not data.get("data"):
        return None

    df = pd.DataFrame(data["data"])
    for col in ["open", "high", "low", "close", "time"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close", "time"])

    df["open_ts"] = df["time"].astype("int64")
    df.loc[df["open_ts"] > 10**12, "open_ts"] //= 1000        # ms -> s
    df["close_ts"] = df["open_ts"] + CANDLE_SECONDS
    df = df.drop_duplicates("open_ts").sort_values("open_ts")
    df = df[df["close_ts"] <= now].reset_index(drop=True)      # drop forming candle

    if len(df) < RSI_LENGTH + RSI_EMA_LENGTH + 2:
        return None
    df = calculate_rsi(df, length=RSI_LENGTH, ma_type="ema", ma_length=RSI_EMA_LENGTH)
    return df.dropna(subset=["RSI", "RSI_MA"]).reset_index(drop=True)


def get_inr_rate():
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


# =====================================================================================
# RULES (pure function — easy to backtest)
# =====================================================================================

def evaluate(state, rsi, rsi_ema):
    """Returns "ALERT" (buy), "SELL" (book profit) or None for one closed candle."""
    if rsi >= rsi_ema:
        return "SELL" if state["active"] else None   # first close back above EMA 52
    if not state["active"]:
        return "ALERT"                       # first close below EMA 52
    if rsi < state["last_rsi"]:
        return "ALERT"                       # RSI made a lower value
    return None


# =====================================================================================
# ALERT
# =====================================================================================

def build_message(candle, state, rate):
    price = float(candle["close"])
    position_inr = CAPITAL_INR * LEVERAGE
    quantity = position_inr / (price * rate)
    start = ist(int(candle["open_ts"])).strftime("%Y-%m-%d %H:%M")
    end = ist(int(candle["close_ts"])).strftime("%H:%M")
    kind = "First signal" if not state["active"] else (
        f"Repeat #{state['alert_count'] + 1} — RSI lower than last {state['last_rsi']:.2f}")

    return (
        f"🟢 BUY XAU_USDT\n\n"
        f"{kind}\n"
        f"RSI(52) - {float(candle['RSI']):.2f}\n"
        f"EMA52 of RSI - {float(candle['RSI_MA']):.2f}\n"
        f"Candle (30m) - {start}–{end} IST\n\n"
        f"Price - {price:.2f}\n"
        f"Quantity - {quantity:.4f}\n"
        f"Leverage - {LEVERAGE}x\n"
        f"Capital - ₹{CAPITAL_INR:,.2f}\n"
        f"Position - ₹{position_inr:,.2f}"
    )


def build_sell_message(candle, state):
    price = float(candle["close"])
    buys = state.get("buy_prices") or []
    start = ist(int(candle["open_ts"])).strftime("%Y-%m-%d %H:%M")
    end = ist(int(candle["close_ts"])).strftime("%H:%M")

    msg = (
        f"🔴 SELL XAU_USDT — BOOK PROFIT\n\n"
        f"RSI closed back above EMA52\n"
        f"RSI(52) - {float(candle['RSI']):.2f}\n"
        f"EMA52 of RSI - {float(candle['RSI_MA']):.2f}\n"
        f"Candle (30m) - {start}–{end} IST\n\n"
        f"Exit Price - {price:.2f}\n"
        f"Buy Alerts - {state['alert_count']}"
    )
    if buys:
        avg = sum(buys) / len(buys)
        msg += (f"\nAvg Buy Price - {avg:.2f}\n"
                f"Move - {(price - avg) / avg * 100:+.2f}% "
                f"({(price - avg) / avg * 100 * LEVERAGE:+.2f}% on margin at {LEVERAGE}x)")
    return msg


def send_alert(message):
    if DRY_RUN:
        log.info(f"[DRY RUN] Telegram message not sent:\n{message}")
        return True
    return bool(Send_Swing_Telegram_Message(message))


# =====================================================================================
# MAIN
# =====================================================================================

def main():
    global DRY_RUN
    parser = argparse.ArgumentParser(description="XAU_USDT RSI(52)/EMA(52) 30m buy scanner")
    parser.add_argument("--dry-run", action="store_true", help="log alerts instead of sending them")
    DRY_RUN = DRY_RUN or parser.parse_args().dry_run

    log.info(f"=== XAU RSI scan started{' [DRY RUN]' if DRY_RUN else ''} ===")
    state = load_state()

    df = fetch_closed_candles()
    if df is None or df.empty:
        log.error("Not enough 30m candle data — skipped this run.")
        return

    candle = df.iloc[-1]
    open_ts = int(candle["open_ts"])
    when = ist(open_ts).strftime("%Y-%m-%d %H:%M")
    rsi, rsi_ema = float(candle["RSI"]), float(candle["RSI_MA"])

    if state["last_processed_ts"] is not None and open_ts <= state["last_processed_ts"]:
        log.info(f"30m candle {when} IST already processed — nothing to do.")
        return

    log.info(f"30m candle {when} IST | close {float(candle['close']):.2f} | "
             f"RSI {rsi:.2f} | EMA52 {rsi_ema:.2f} | watch "
             f"{'ACTIVE, last RSI ' + format(state['last_rsi'], '.2f') if state['active'] else 'inactive'}")

    event = evaluate(state, rsi, rsi_ema)

    if event == "ALERT":
        msg = build_message(candle, state, get_inr_rate())
        if not send_alert(msg):
            log.warning("Telegram send FAILED — state not updated, will retry next run.")
            return
        if not state["active"]:
            state.update(active=True, alert_count=0, buy_prices=[],
                         watch_started=ist(int(candle["close_ts"])).strftime("%Y-%m-%d %H:%M IST"))
        state.update(last_rsi=rsi, alert_count=state["alert_count"] + 1,
                     buy_prices=state["buy_prices"] + [float(candle["close"])],
                     last_alert=ist(int(candle["close_ts"])).strftime("%Y-%m-%d %H:%M IST"))
        log.info(f"BUY alert sent (#{state['alert_count']}) — saved last RSI {rsi:.2f}")
    elif event == "SELL":
        if not send_alert(build_sell_message(candle, state)):
            log.warning("Telegram send FAILED — state not updated, will retry next run.")
            return
        log.info(f"RSI {rsi:.2f} closed back above EMA52 {rsi_ema:.2f} — SELL / book profit "
                 f"alert sent, cycle closed after {state['alert_count']} buy alert(s).")
        state.update(active=False, last_rsi=None, alert_count=0, watch_started=None,
                     buy_prices=[],
                     last_alert=ist(int(candle["close_ts"])).strftime("%Y-%m-%d %H:%M IST"))
    elif state["active"]:
        log.info(f"RSI {rsi:.2f} not below last alert RSI {state['last_rsi']:.2f} — no alert.")
    else:
        log.info(f"RSI {rsi:.2f} above EMA52 {rsi_ema:.2f} — no signal.")

    state["last_processed_ts"] = open_ts
    save_state(state)
    log.info("=== Run complete ===")


if __name__ == "__main__":
    main()
