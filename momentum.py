import json
import requests
import pandas as pd
import os
import logging

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum

try:
    from Telegram_Swing import Send_Swing_Telegram_Message
except ImportError:
    def Send_Swing_Telegram_Message(msg):
        print(f"\n--- TELEGRAM ALERT ---\n{msg}\n----------------------")


# ================================================================
# LOGGING
# ================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


# ================================================================
# CONFIG
# ================================================================
RESOLUTION          = "60"          # 1H candles

BUY_FILE            = "MomentumBuyWatchlist.json"
SELL_FILE           = "MomentumSellWatchlist.json"

MAX_WORKERS         = 20
TOP_N               = 25            # Top 15 gainers & losers

USE_VOLUME_FILTER   = False
MIN_VOLUME_USDT     = 10_000_000

LEVERAGE            = 7
INR_TO_USDT_RATE    = None          # None = fetch live
RISK_PER_TRADE_INR  = 200

# MOVER THRESHOLD
MIN_GAINER_PCT      = 5.0           # Gainers: minimum +5% 1D change
MIN_LOSER_PCT       = -5.0          # Losers:  maximum -5% 1D change

# RSI SETTINGS
RSI_LENGTH          = 14
RSI_UPPER_LEVEL     = 55            # RSI above this = overbought bounce
RSI_LOWER_LEVEL     = 45            # RSI below this = oversold dip

# SWING CANDLES for SL calculation
SWING_CANDLES       = 10


# ================================================================
# ENUMS
# ================================================================
class BuyState(str, Enum):
    DIP_DONE    = "dip_done"        # RSI < 45 confirmed, waiting for RSI > 55

class SellState(str, Enum):
    BOUNCE_DONE = "bounce_done"     # RSI > 55 confirmed, waiting for RSI < 45


# ================================================================
# RSI — TRADINGVIEW STYLE (RMA)
# ================================================================
def rma(series, length):
    return series.ewm(alpha=1 / length, adjust=False).mean()


def calculate_tv_rsi(close, length=14):
    delta    = close.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = rma(gain, length)
    avg_loss = rma(loss, length)
    rs       = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


# ================================================================
# INDICATORS
# ================================================================
def calculate_indicators(df):
    df['ema20']  = df['close'].ewm(span=20,  adjust=False).mean()
    df['ema50']  = df['close'].ewm(span=50,  adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    df['rsi']    = calculate_tv_rsi(df['close'], RSI_LENGTH)
    return df


# ================================================================
# FETCH OHLCV DATA
# ================================================================
def fetch_data(pair):
    url = "https://public.coindcx.com/market_data/candlesticks"
    now = int(datetime.now(timezone.utc).timestamp())
    params = {
        "pair":       pair,
        "from":       now - 500 * 3600,
        "to":         now,
        "resolution": RESOLUTION,
        "pcode":      "f"
    }
    try:
        r  = requests.get(url, params=params, timeout=10).json()
        df = pd.DataFrame(r["data"]).sort_values("time").iloc[:-1]  # drop live candle
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col])
        return calculate_indicators(df).dropna()
    except Exception as e:
        log.debug(f"fetch_data failed for {pair}: {e}")
        return None


# ================================================================
# TOP MOVERS
# ================================================================
def fetch_pair_stats(pair):
    url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
    try:
        data = requests.get(url, timeout=8).json()
    except Exception:
        return None

    if not data:
        return None

    change = data.get("price_change_percent", {}).get("1D")
    if change is None:
        return None

    return {"pair": pair, "change": float(change)}


def get_top_movers(pairs):
    """Returns top N gainers and top N losers sorted by % change."""
    gainers, losers = [], []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_pair_stats, p): p for p in pairs}
        for f in as_completed(futures):
            result = f.result()
            if not result:
                continue
            if result["change"] > MIN_GAINER_PCT:       # Sirf 5%+ gainers
                gainers.append(result)
            elif result["change"] < MIN_LOSER_PCT:       # Sirf 5%- losers
                losers.append(result)

    gainers = sorted(gainers, key=lambda x: x["change"], reverse=True)[:TOP_N]
    losers  = sorted(losers,  key=lambda x: x["change"])[:TOP_N]

    return gainers, losers


# ================================================================
# VOLUME FILTER
# ================================================================
def get_volume(pair):
    if not USE_VOLUME_FILTER:
        return float('inf')
    try:
        d = requests.get(
            f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}",
            timeout=5
        ).json()
        return float(d.get("volume_24h", 0))
    except Exception:
        return 0


# ================================================================
# INR RATE
# ================================================================
def get_inr_rate():
    if INR_TO_USDT_RATE is not None:
        return INR_TO_USDT_RATE
    try:
        r = requests.get(
            "https://api.coindcx.com/exchange/v1/markets_details",
            timeout=5
        ).json()
        for m in r:
            if m.get("symbol") == "USDTINR":
                return float(m.get("last_price", 84.0))
    except Exception:
        pass
    return 84.0


# ================================================================
# POSITION SIZE
# ================================================================
def calc_position(entry, sl):
    sl_pct = abs(entry - sl) / entry * 100
    if sl_pct == 0:
        return None

    rate         = get_inr_rate()
    risk_usdt    = RISK_PER_TRADE_INR / rate
    position_usdt = round(risk_usdt / (sl_pct / 100), 2)
    capital_usdt  = round(position_usdt / LEVERAGE, 2)
    capital_inr   = round(capital_usdt * rate, 2)
    quantity      = round(position_usdt / entry, 4)

    return {
        "capital_inr":  capital_inr,
        "capital_usdt": capital_usdt,
        "quantity":     quantity
    }


# ================================================================
# TRADE LEVELS
# ================================================================
def trade_levels_buy(df):
    """
    BUY entry at current candle high,
    SL at swing LOW of last SWING_CANDLES candles.
    Targets at 1:2 and 1:3 RR.
    """
    last   = df.iloc[-1]
    swing  = df.iloc[-SWING_CANDLES:]

    entry  = round(last['high'], 6)
    sl     = round(swing['low'].min(), 6)
    risk   = entry - sl
    t2     = round(entry + 2 * risk, 6)
    t3     = round(entry + 3 * risk, 6)

    return entry, sl, t2, t3


def trade_levels_sell(df):
    """
    SELL entry at current candle low,
    SL at swing HIGH of last SWING_CANDLES candles.
    Targets at 1:2 and 1:3 RR.
    """
    last  = df.iloc[-1]
    swing = df.iloc[-SWING_CANDLES:]

    entry = round(last['low'], 6)
    sl    = round(swing['high'].max(), 6)
    risk  = sl - entry
    t2    = round(entry - 2 * risk, 6)
    t3    = round(entry - 3 * risk, 6)

    return entry, sl, t2, t3


# ================================================================
# ALERT MESSAGE BUILDER
# ================================================================
def build_buy_msg(pair, entry, sl, t2, t3):
    pos = calc_position(entry, sl)
    cap = f"Rs.{pos['capital_inr']} (~${pos['capital_usdt']} USDT)" if pos else "N/A"
    qty = pos['quantity'] if pos else "N/A"

    return (
        f"🟢 BUY — {pair}\n\n"
        f"Entry    : {entry}\n"
        f"SL       : {sl}\n"
        f"Capital  : {cap}\n"
        f"Quantity : {qty}\n"
        f"Targets:\n"
        f"  1:2 → {t2}\n"
        f"  1:3 → {t3}"
    )


def build_sell_msg(pair, entry, sl, t2, t3):
    pos = calc_position(entry, sl)
    cap = f"Rs.{pos['capital_inr']} (~${pos['capital_usdt']} USDT)" if pos else "N/A"
    qty = pos['quantity'] if pos else "N/A"

    return (
        f"🔴 SELL — {pair}\n\n"
        f"Entry    : {entry}\n"
        f"SL       : {sl}\n"
        f"Capital  : {cap}\n"
        f"Quantity : {qty}\n"
        f"Targets:\n"
        f"  1:2 → {t2}\n"
        f"  1:3 → {t3}"
    )


# ================================================================
# SCAN — ADD TO BUY WATCHLIST
#
# Condition:
#   • Top 15 gainer
#   • EMA20 > EMA50 > EMA200  (bullish trend)
#   • RSI < RSI_LOWER_LEVEL   (dip happened)
#   → Add with state "dip_done"
# ================================================================
def scan_top_gainer_for_buy(pair, existing_buy_pairs):
    if pair in existing_buy_pairs:
        return None

    if get_volume(pair) < MIN_VOLUME_USDT:
        return None

    df = fetch_data(pair)
    if df is None or len(df) < 2:
        return None

    last = df.iloc[-1]

    bullish_trend = (
        last['ema20']  > last['ema50'] and
        last['ema50']  > last['ema200']
    )

    rsi_dipped = last['rsi'] < RSI_LOWER_LEVEL   # RSI < 45

    if bullish_trend and rsi_dipped:
        log.info(f"🟢 BUY Watchlist ADD: {pair} | RSI: {round(last['rsi'], 1)}")
        return ("ADD_BUY", pair)

    return None


# ================================================================
# SCAN — ADD TO SELL WATCHLIST
#
# Condition:
#   • Top 15 loser
#   • EMA20 < EMA50 < EMA200  (bearish trend)
#   • RSI > RSI_UPPER_LEVEL   (bounce happened)
#   → Add with state "bounce_done"
# ================================================================
def scan_top_loser_for_sell(pair, existing_sell_pairs):
    if pair in existing_sell_pairs:
        return None

    if get_volume(pair) < MIN_VOLUME_USDT:
        return None

    df = fetch_data(pair)
    if df is None or len(df) < 2:
        return None

    last = df.iloc[-1]

    bearish_trend = (
        last['ema20']  < last['ema50'] and
        last['ema50']  < last['ema200']
    )

    rsi_bounced = last['rsi'] > RSI_UPPER_LEVEL  # RSI > 55

    if bearish_trend and rsi_bounced:
        log.info(f"🔴 SELL Watchlist ADD: {pair} | RSI: {round(last['rsi'], 1)}")
        return ("ADD_SELL", pair)

    return None


# ================================================================
# STATE MACHINE
#
# BUY  — "dip_done"    → waits for RSI > 55  → ALERT_BUY
# SELL — "bounce_done" → waits for RSI < 45  → ALERT_SELL
# ================================================================
def check_state(entry):
    pair  = entry["pair"]
    state = entry["state"]

    df = fetch_data(pair)
    if df is None or df.empty:
        return ("STAY", entry)

    last = df.iloc[-1]
    rsi  = last['rsi']

    # ============================================================
    # BUY SIDE — dip_done → waiting for RSI > 55
    # ============================================================
    if state == BuyState.DIP_DONE:

        # Volume check (optional)
        if get_volume(pair) < MIN_VOLUME_USDT:
            log.info(f"❌ BUY Remove (low volume): {pair}")
            return ("REMOVE", entry)

        # Trend must still be bullish
        trend_broken = not (
            last['ema20'] > last['ema50'] and
            last['ema50'] > last['ema200']
        )

        if trend_broken:
            log.info(f"❌ BUY Remove (trend broken): {pair}")
            return ("REMOVE", entry)

        # ALERT: RSI crossed above 55
        if rsi > RSI_UPPER_LEVEL:
            e, sl, t2, t3 = trade_levels_buy(df)
            msg = build_buy_msg(pair, e, sl, t2, t3)
            log.info(f"📈 ALERT BUY: {pair} | RSI: {round(rsi, 1)}")
            return ("ALERT_BUY", entry, msg)

    # ============================================================
    # SELL SIDE — bounce_done → waiting for RSI < 45
    # ============================================================
    elif state == SellState.BOUNCE_DONE:

        # Volume check (optional)
        if get_volume(pair) < MIN_VOLUME_USDT:
            log.info(f"❌ SELL Remove (low volume): {pair}")
            return ("REMOVE", entry)

        # Trend must still be bearish
        trend_broken = not (
            last['ema20'] < last['ema50'] and
            last['ema50'] < last['ema200']
        )

        if trend_broken:
            log.info(f"❌ SELL Remove (trend broken): {pair}")
            return ("REMOVE", entry)

        # ALERT: RSI dropped below 45
        if rsi < RSI_LOWER_LEVEL:
            e, sl, t2, t3 = trade_levels_sell(df)
            msg = build_sell_msg(pair, e, sl, t2, t3)
            log.info(f"📉 ALERT SELL: {pair} | RSI: {round(rsi, 1)}")
            return ("ALERT_SELL", entry, msg)

    return ("STAY", entry)


# ================================================================
# LOAD / SAVE WATCHLIST
# ================================================================
def load_watchlist(filepath):
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            return json.load(f)
    return []


def save_watchlist(filepath, data):
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)


# ================================================================
# PROCESS WATCHLIST RESULTS
# ================================================================
def process_watchlist(watchlist):
    """
    Runs check_state on all watchlist entries concurrently.
    Returns (updated_list, alerts).
    """
    updated  = list(watchlist)
    alerts   = []

    if not watchlist:
        return updated, alerts

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(check_state, e) for e in watchlist]

        for f in as_completed(futures):
            res = f.result()
            if not res:
                continue

            code = res[0]

            if code in ("ALERT_BUY", "ALERT_SELL"):
                _, entry, msg = res
                alerts.append(msg)
                # Remove from watchlist after alert
                updated = [e for e in updated if e["pair"] != entry["pair"]]

            elif code == "REMOVE":
                _, entry = res
                updated = [e for e in updated if e["pair"] != entry["pair"]]

            # STAY → no change

    return updated, alerts


# ================================================================
# MAIN
# ================================================================
def main():

    now_str = datetime.now().isoformat(timespec="seconds")

    # ============================================================
    # LOAD WATCHLISTS
    # ============================================================
    buy_list  = load_watchlist(BUY_FILE)
    sell_list = load_watchlist(SELL_FILE)

    buy_pairs  = {e["pair"] for e in buy_list}
    sell_pairs = {e["pair"] for e in sell_list}

    # ============================================================
    # CHECK EXISTING WATCHLISTS (State Machine)
    # ============================================================
    log.info(f"Checking BUY  watchlist ({len(buy_list)}  pairs)...")
    updated_buy,  buy_alerts  = process_watchlist(buy_list)

    log.info(f"Checking SELL watchlist ({len(sell_list)} pairs)...")
    updated_sell, sell_alerts = process_watchlist(sell_list)

    all_alerts = buy_alerts + sell_alerts

    # ============================================================
    # SEND ALERTS
    # ============================================================
    if all_alerts:
        Send_Swing_Telegram_Message("\n\n---\n\n".join(all_alerts))

    # ============================================================
    # FETCH ALL FUTURES PAIRS
    # ============================================================
    try:
        raw = requests.get(
            "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments"
            "?margin_currency_short_name[]=USDT",
            timeout=10
        ).json()
        all_pairs = [p for p in raw if isinstance(p, str)]
    except Exception as e:
        log.error(f"Error fetching pairs: {e}")
        all_pairs = []

    if not all_pairs:
        log.warning("No pairs fetched. Saving watchlists and exiting.")
        save_watchlist(BUY_FILE,  updated_buy)
        save_watchlist(SELL_FILE, updated_sell)
        return

    # ============================================================
    # GET TOP 15 GAINERS & LOSERS
    # ============================================================
    gainers, losers = get_top_movers(all_pairs)

    log.info(
        f"Top {TOP_N} Gainers: {[g['pair'] + ' ' + str(round(g['change'],1))+'%' for g in gainers]}"
    )
    log.info(
        f"Top {TOP_N} Losers:  {[l['pair'] + ' ' + str(round(l['change'],1))+'%' for l in losers]}"
    )

    gainer_pairs = [g["pair"] for g in gainers]
    loser_pairs  = [l["pair"] for l in losers]

    # Updated sets after state machine processing
    current_buy_pairs  = {e["pair"] for e in updated_buy}
    current_sell_pairs = {e["pair"] for e in updated_sell}

    # ============================================================
    # SCAN TOP GAINERS → BUY WATCHLIST
    # Condition: EMA20>EMA50>EMA200 AND RSI < 45
    # ============================================================
    log.info(f"Scanning {len(gainer_pairs)} TOP GAINERS for BUY setup...")
    new_buy = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [
            ex.submit(scan_top_gainer_for_buy, p, current_buy_pairs)
            for p in gainer_pairs
        ]
        for f in as_completed(futures):
            res = f.result()
            if not res:
                continue

            code, pair = res

            if code == "ADD_BUY" and pair not in current_buy_pairs:
                updated_buy.append({
                    "pair":  pair,
                    "state": BuyState.DIP_DONE,
                    "added": now_str
                })
                current_buy_pairs.add(pair)
                new_buy += 1

    # ============================================================
    # SCAN TOP LOSERS → SELL WATCHLIST
    # Condition: EMA20<EMA50<EMA200 AND RSI > 55
    # ============================================================
    log.info(f"Scanning {len(loser_pairs)} TOP LOSERS for SELL setup...")
    new_sell = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [
            ex.submit(scan_top_loser_for_sell, p, current_sell_pairs)
            for p in loser_pairs
        ]
        for f in as_completed(futures):
            res = f.result()
            if not res:
                continue

            code, pair = res

            if code == "ADD_SELL" and pair not in current_sell_pairs:
                updated_sell.append({
                    "pair":  pair,
                    "state": SellState.BOUNCE_DONE,
                    "added": now_str
                })
                current_sell_pairs.add(pair)
                new_sell += 1

    # ============================================================
    # SAVE WATCHLISTS
    # ============================================================
    save_watchlist(BUY_FILE,  updated_buy)
    save_watchlist(SELL_FILE, updated_sell)

    # ============================================================
    # SUMMARY
    # ============================================================
    log.info(
        f"\n{'='*45}\n"
        f"  Alerts Sent   : {len(all_alerts)}\n"
        f"  New BUY  Added: {new_buy}  | Total: {len(updated_buy)}\n"
        f"  New SELL Added: {new_sell} | Total: {len(updated_sell)}\n"
        f"{'='*45}"
    )


# ================================================================
# START
# ================================================================
if __name__ == "__main__":
    main()