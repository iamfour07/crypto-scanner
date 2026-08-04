import requests
import pandas as pd
import logging

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

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

MAX_WORKERS         = 20
TOP_N               = 10             # Top 5 gainers & losers

# EXCLUDE extreme movers (already pumped/dumped 50%+ in 24h)
MAX_GAINER_PCT      = 50.0          # exclude gainers >= 50%
MIN_LOSER_PCT       = -50.0         # exclude losers <= -50%

# Still require some minimum move to bother looking at the pair
MIN_GAINER_PCT      = 2.0
MAX_LOSER_PCT       = -2.0

USE_VOLUME_FILTER   = False
MIN_VOLUME_USDT     = 10_000_000

LEVERAGE            = 7             # 7x margin
INR_TO_USDT_RATE    = None          # None = fetch live
RISK_PER_TRADE_INR  = 100           # max loss per trade

EMA_LENGTH          = 5
SWING_CANDLES       = 7             # candles checked BEFORE the gap candle for SL


# ================================================================
# INDICATORS
# ================================================================
def calculate_indicators(df):
    df['ema5'] = df['close'].ewm(span=EMA_LENGTH, adjust=False).mean()
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
# TOP MOVERS  (excluding coins that already moved 50%+)
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
    """
    Returns top N gainers and top N losers by 24h % change,
    excluding coins that already gained >= MAX_GAINER_PCT or
    lost <= MIN_LOSER_PCT (too extended / already blown out).
    """
    gainers, losers = [], []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_pair_stats, p): p for p in pairs}
        for f in as_completed(futures):
            result = f.result()
            if not result:
                continue

            chg = result["change"]

            # Gainer bucket: positive move, but not an already-blown-out 50%+ pump
            if MIN_GAINER_PCT < chg < MAX_GAINER_PCT:
                gainers.append(result)

            # Loser bucket: negative move, but not an already-blown-out 50%+ dump
            elif MAX_LOSER_PCT > chg > MIN_LOSER_PCT:
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

    rate          = get_inr_rate()
    risk_usdt     = RISK_PER_TRADE_INR / rate
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
# BUY SETUP  (from top gainers)
#
#   • Last CLOSED candle closes BELOW 5 EMA
#   • That candle's HIGH also stays below 5 EMA (real gap, no wick touch)
#   • Entry = gap candle's high
#   • SL    = lowest low among {gap candle, previous 7 candles}
#             (gap candle is its own SL only if it IS the swing low;
#              otherwise the true swing low among the previous 7 is used)
# ================================================================
def check_buy_setup(df):
    if len(df) < SWING_CANDLES + 1:
        return None

    gap_candle = df.iloc[-1]
    prev       = df.iloc[-(SWING_CANDLES + 1):-1]   # previous 7 candles

    closed_below_ema = gap_candle['close'] < gap_candle['ema5']
    high_below_ema    = gap_candle['high']  < gap_candle['ema5']   # gap = no touch

    if not (closed_below_ema and high_below_ema):
        return None

    entry = round(gap_candle['high'], 6)
    sl    = round(min(gap_candle['low'], prev['low'].min()), 6)

    if sl >= entry:
        # sanity guard — shouldn't happen given the gap condition, but never trade a bad SL
        return None

    risk = entry - sl
    t2   = round(entry + 2 * risk, 6)
    t3   = round(entry + 3 * risk, 6)
    t4   = round(entry + 4 * risk, 6)

    return entry, sl, t2, t3, t4


# ================================================================
# SELL SETUP  (from top losers)
#
#   • Last CLOSED candle closes ABOVE 5 EMA
#   • That candle's LOW also stays above 5 EMA (real gap, no wick touch)
#   • Entry = gap candle's low
#   • SL    = highest high among {gap candle, previous 7 candles}
# ================================================================
def check_sell_setup(df):
    if len(df) < SWING_CANDLES + 1:
        return None

    gap_candle = df.iloc[-1]
    prev       = df.iloc[-(SWING_CANDLES + 1):-1]   # previous 7 candles

    closed_above_ema = gap_candle['close'] > gap_candle['ema5']
    low_above_ema     = gap_candle['low']   > gap_candle['ema5']   # gap = no touch

    if not (closed_above_ema and low_above_ema):
        return None

    entry = round(gap_candle['low'], 6)
    sl    = round(max(gap_candle['high'], prev['high'].max()), 6)

    if sl <= entry:
        return None

    risk = sl - entry
    t2   = round(entry - 2 * risk, 6)
    t3   = round(entry - 3 * risk, 6)
    t4   = round(entry - 4 * risk, 6)

    return entry, sl, t2, t3, t4


# ================================================================
# ALERT MESSAGE BUILDERS
# ================================================================
def build_buy_msg(pair, entry, sl, t2, t3, t4):
    pos = calc_position(entry, sl)
    cap = f"Rs.{pos['capital_inr']} (~${pos['capital_usdt']} USDT)" if pos else "N/A"

    return (
        f"🟢 BUY (5 EMA Gap)\n\n"
        f"Name- {pair}\n"
        f"Entry- {entry}\n"
        f"SL- {sl}\n"
        f"Capital- {cap}\n"
        f"Risk Per Trade- Rs.{RISK_PER_TRADE_INR}\n"
        f"-----------------\n"
        f"T2- {t2}\n"
        f"T3- {t3}\n"
        f"T4- {t4}"
    )


def build_sell_msg(pair, entry, sl, t2, t3, t4):
    pos = calc_position(entry, sl)
    cap = f"Rs.{pos['capital_inr']} (~${pos['capital_usdt']} USDT)" if pos else "N/A"

    return (
        f"🔴 SELL (5 EMA Gap)\n\n"
        f"Name- {pair}\n"
        f"Entry- {entry}\n"
        f"SL- {sl}\n"
        f"Capital- {cap}\n"
        f"Risk Per Trade- Rs.{RISK_PER_TRADE_INR}\n"
        f"-----------------\n"
        f"T2- {t2}\n"
        f"T3- {t3}\n"
        f"T4- {t4}"
    )


# ================================================================
# SCAN
# ================================================================
def scan_pair(pair, side):
    if get_volume(pair) < MIN_VOLUME_USDT:
        return None

    df = fetch_data(pair)
    if df is None:
        return None

    if side == "buy":
        result = check_buy_setup(df)
        if result:
            entry, sl, t2, t3, t4 = result
            log.info(f"🟢 BUY setup: {pair} | entry={entry} sl={sl}")
            return build_buy_msg(pair, entry, sl, t2, t3, t4)
    else:
        result = check_sell_setup(df)
        if result:
            entry, sl, t2, t3, t4 = result
            log.info(f"🔴 SELL setup: {pair} | entry={entry} sl={sl}")
            return build_sell_msg(pair, entry, sl, t2, t3, t4)

    return None


# ================================================================
# MAIN
# ================================================================
def main():
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
        log.warning("No pairs fetched. Exiting.")
        return

    # ============================================================
    # TOP 5 GAINERS & LOSERS (excluding 50%+ already-extended moves)
    # ============================================================
    gainers, losers = get_top_movers(all_pairs)

    log.info(
        f"Top {TOP_N} Gainers (<{MAX_GAINER_PCT}%): "
        f"{[g['pair'] + ' ' + str(round(g['change'],1)) + '%' for g in gainers]}"
    )
    log.info(
        f"Top {TOP_N} Losers  (>{MIN_LOSER_PCT}%): "
        f"{[l['pair'] + ' ' + str(round(l['change'],1)) + '%' for l in losers]}"
    )

    gainer_pairs = [g["pair"] for g in gainers]
    loser_pairs  = [l["pair"] for l in losers]

    # ============================================================
    # SCAN GAINERS FOR BUY SETUP, LOSERS FOR SELL SETUP
    # ============================================================
    alerts = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = []
        futures += [ex.submit(scan_pair, p, "buy")  for p in gainer_pairs]
        futures += [ex.submit(scan_pair, p, "sell") for p in loser_pairs]

        for f in as_completed(futures):
            msg = f.result()
            if msg:
                alerts.append(msg)

    # ============================================================
    # SEND ALERTS
    # ============================================================
    if alerts:
        Send_Swing_Telegram_Message("\n\n---\n\n".join(alerts))
    else:
        log.info("No 5 EMA gap setups found this run.")

    log.info(f"Scan complete. Setups found: {len(alerts)}")


# ================================================================
# START
# ================================================================
if __name__ == "__main__":
    main()