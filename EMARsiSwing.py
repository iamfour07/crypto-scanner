import json
import requests
import pandas as pd
import os

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from Telegram_Swing import Send_Swing_Telegram_Message
except ImportError:
    def Send_Swing_Telegram_Message(msg):
        print(f"\n--- TELEGRAM ALERT ---\n{msg}\n----------------------")


# ================================================================
# CONFIG
# ================================================================
RESOLUTION            = "60"

SELL_FILE             = "SellWatchlist.json"

MAX_WORKERS           = 20

USE_VOLUME_FILTER     = False
MIN_VOLUME_USDT       = 10_000_000

LEVERAGE              = 7
INR_TO_USDT_RATE      = None
RISK_PER_TRADE_INR    = 200

TOP_N                 = 20

# RSI SETTINGS
RSI_LENGTH            = 14
RSI_UPPER_LEVEL       = 55
RSI_LOWER_LEVEL       = 45

# SWING SETTINGS
SWING_CANDLES         = 10


# ================================================================
# RSI (TRADINGVIEW STYLE)
# ================================================================
def rma(series, length):

    return series.ewm(
        alpha=1 / length,
        adjust=False
    ).mean()


def calculate_tv_rsi(close, length=14):

    delta = close.diff()

    gain = delta.clip(lower=0)

    loss = -delta.clip(upper=0)

    avg_gain = rma(gain, length)

    avg_loss = rma(loss, length)

    rs = avg_gain / avg_loss

    rsi = 100 - (100 / (1 + rs))

    return rsi


# ================================================================
# INDICATORS
# ================================================================
def calculate_indicators(df):

    df['ema20'] = df['close'].ewm(
        span=20,
        adjust=False
    ).mean()

    df['ema50'] = df['close'].ewm(
        span=50,
        adjust=False
    ).mean()

    df['ema200'] = df['close'].ewm(
        span=200,
        adjust=False
    ).mean()

    # TradingView RSI
    df['rsi'] = calculate_tv_rsi(
        df['close'],
        RSI_LENGTH
    )

    return df


# ================================================================
# FETCH DATA
# ================================================================
def fetch_data(pair):

    url = "https://public.coindcx.com/market_data/candlesticks"

    now = int(datetime.now(timezone.utc).timestamp())

    params = {
        "pair": pair,
        "from": now - 500 * 3600,
        "to": now,
        "resolution": RESOLUTION,
        "pcode": "f"
    }

    try:

        r = requests.get(
            url,
            params=params,
            timeout=10
        ).json()

        df = pd.DataFrame(
            r["data"]
        ).sort_values("time").iloc[:-1]

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col])

        return calculate_indicators(df).dropna()

    except:
        return None


# ================================================================
# TOP MOVERS
# ================================================================
def fetch_pair_stats(pair):

    url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"

    try:

        data = requests.get(
            url,
            timeout=8
        ).json()

    except:
        return None

    if not data:
        return None

    change_percent = (
        data.get("price_change_percent", {})
        .get("1D")
    )

    if change_percent is None:
        return None

    return {
        "pair": pair,
        "change": float(change_percent),
    }


def get_top_movers(pairs):

    gainers = []
    losers = []

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                fetch_pair_stats,
                pair
            ): pair for pair in pairs
        }

        for future in as_completed(futures):

            result = future.result()

            if not result:
                continue

            if result["change"] > 0:
                gainers.append(result)

            elif result["change"] < 0:
                losers.append(result)

    gainers = sorted(
        gainers,
        key=lambda item: item["change"],
        reverse=True
    )[:TOP_N]

    losers = sorted(
        losers,
        key=lambda item: item["change"]
    )[:TOP_N]

    return gainers, losers

# ================================================================
# VOLUME
# ================================================================
def get_volume(pair):

    if not USE_VOLUME_FILTER:
        return float('inf')

    try:

        d = requests.get(
            f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}",
            timeout=5
        ).json()

        return float(
            d.get("volume_24h", 0)
        )

    except:
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

                return float(
                    m.get("last_price", 84.0)
                )

    except:
        pass

    return 84.0


# ================================================================
# POSITION SIZE
# ================================================================
def calc_position(entry, sl):

    sl_pct = abs(entry - sl) / entry * 100

    if sl_pct == 0:
        return None

    rate = get_inr_rate()

    risk_usdt = RISK_PER_TRADE_INR / rate

    position_usdt = round(
        risk_usdt / (sl_pct / 100),
        2
    )

    capital_usdt = round(
        position_usdt / LEVERAGE,
        2
    )

    capital_inr = round(
        capital_usdt * rate,
        2
    )

    quantity = round(
        position_usdt / entry,
        4
    )

    return {
        "capital_inr": capital_inr,
        "capital_usdt": capital_usdt,
        "quantity": quantity
    }


# ================================================================
# TRADE LEVELS
# ================================================================
def trade_levels(df, side):

    last = df.iloc[-1]

    swing = df.iloc[-SWING_CANDLES:]

    if side == "sell":

        entry = round(
            last['low'],
            6
        )

        sl = round(
            swing['high'].max(),
            6
        )

        risk = sl - entry

        t2 = round(
            entry - 2 * risk,
            6
        )

        t3 = round(
            entry - 3 * risk,
            6
        )

        return entry, sl, t2, t3


# ================================================================
# ALERT MESSAGE
# ================================================================
def build_msg(pair, entry, sl, t2, t3, gap=None):

    pos = calc_position(entry, sl)

    cap = (
        f"Rs.{pos['capital_inr']} (~${pos['capital_usdt']} USDT)"
        if pos else "N/A"
    )

    qty = (
        pos['quantity']
        if pos else "N/A"
    )

    return (
        f"SELL — {pair}\n\n"
        f"Entry    : {entry}\n"
        f"SL       : {sl}\n"
        f"Capital  : {cap}\n"
        f"Quantity : {qty}\n"
        f"Targets:\n"
        f"1:2 → {t2}\n"
        f"1:3 → {t3}"
    )


# ================================================================
# TOP GAINER REVERSAL SCAN
# ================================================================
def scan_top_gainer_reversal(pair, sell_pairs):

    # already watchlist me hai
    if pair in sell_pairs:
        return None

    # volume filter
    if get_volume(pair) < MIN_VOLUME_USDT:
        return None

    df = fetch_data(pair)

    if df is None or len(df) < 2:
        return None

    last = df.iloc[-1]

    # ONLY bullish coins
    if last['ema50'] > last['ema200']:

        print(
            f"🔥 Added To SELL Watchlist: {pair}"
        )

        return ("ADD_SELL", pair)

    return None


# ================================================================
# STATE MACHINE
# ================================================================
def check_state(entry, sell_pairs):

    pair = entry["pair"]

    state = entry["state"]

    df = fetch_data(pair)

    if df is None or df.empty:
        return ("STAY", entry)

    last = df.iloc[-1]

    rsi = last['rsi']

    # ============================================================
    # SELL SIDE
    # ============================================================
    if state in ("waiting_bounce", "bounce_done"):

        # volume remove
        if get_volume(pair) < MIN_VOLUME_USDT:

            print(
                f"❌ SELL Remove (low volume): {pair}"
            )

            return ("REMOVE_SELL", entry)

        # waiting_bounce -> bounce_done
        if (
            state == "waiting_bounce"
            and rsi > RSI_UPPER_LEVEL
        ):

            print(
                f"🔄 SELL bounce_done: {pair} | RSI: {round(rsi, 1)}"
            )

            return (
                "UPDATE",
                {
                    **entry,
                    "state": "bounce_done"
                }
            )

        # FINAL ALERT
        if (
            state == "bounce_done"
            and rsi < RSI_LOWER_LEVEL
            and last['ema50'] < last['ema200']
        ):

            e, sl, t2, t3 = trade_levels(
                df,
                "sell"
            )

            gap = (
                abs(
                    last['close'] - last['ema20']
                ) / last['ema20']
            ) * 100

            return (
                "ALERT_SELL",
                entry,
                build_msg(
                    pair,
                    e,
                    sl,
                    t2,
                    t3,
                    gap
                )
            )

    return ("STAY", entry)


# ================================================================
# MAIN
# ================================================================
def main():

    # ============================================================
    # LOAD SELL WATCHLIST
    # ============================================================
    sell_list = (
        json.load(open(SELL_FILE))
        if os.path.exists(SELL_FILE)
        else []
    )

    sell_pairs = {
        e["pair"]
        for e in sell_list
    }

    updated_sell = list(sell_list)

    alerts = []

    # ============================================================
    # CHECK EXISTING WATCHLIST
    # ============================================================
    all_watched = sell_list

    if all_watched:

        with ThreadPoolExecutor(
            max_workers=MAX_WORKERS
        ) as ex:

            futures = [
                ex.submit(
                    check_state,
                    e,
                    sell_pairs
                )
                for e in all_watched
            ]

            for f in as_completed(futures):

                res = f.result()

                if not res:
                    continue

                code = res[0]

                # ====================================================
                # ALERT SELL
                # ====================================================
                if code == "ALERT_SELL":

                    _, entry, msg = res

                    alerts.append(msg)

                    updated_sell = [
                        e for e in updated_sell
                        if e["pair"] != entry["pair"]
                    ]

                    print(
                        f"📉 ALERT SELL: {entry['pair']}"
                    )

                # ====================================================
                # UPDATE STATE
                # ====================================================
                elif code == "UPDATE":

                    _, upd = res

                    pair = upd["pair"]

                    updated_sell = [
                        upd if e["pair"] == pair else e
                        for e in updated_sell
                    ]

                # ====================================================
                # REMOVE
                # ====================================================
                elif code == "REMOVE_SELL":

                    _, entry = res

                    updated_sell = [
                        e for e in updated_sell
                        if e["pair"] != entry["pair"]
                    ]

    # ============================================================
    # SEND ALERTS
    # ============================================================
    if alerts:

        Send_Swing_Telegram_Message(
            "\n\n---\n\n".join(alerts)
        )

    # ============================================================
    # FETCH ALL FUTURES PAIRS
    # ============================================================
    try:

        raw = requests.get(
            "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments"
            "?margin_currency_short_name[]=USDT",
            timeout=10
        ).json()

        all_pairs = [
            p for p in raw
            if isinstance(p, str)
        ]

    except Exception as e:

        print(
            f"Error fetching pairs: {e}"
        )

        all_pairs = []

    # ============================================================
    # GET TOP GAINERS
    # ============================================================
    gainers, losers = get_top_movers(all_pairs)

    top_gainer_pairs = [
        g["pair"]
        for g in gainers
    ]

    sell_pairs_now = {
        e["pair"]
        for e in updated_sell
    }

    print(
        f"\nScanning {len(top_gainer_pairs)} "
        f"TOP GAINERS for reversal setup..."
    )

    new_sell = 0

    # ============================================================
    # SCAN TOP GAINERS
    # ============================================================
    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as ex:

        futures = [
            ex.submit(
                scan_top_gainer_reversal,
                p,
                sell_pairs_now
            )
            for p in top_gainer_pairs
        ]

        for f in as_completed(futures):

            res = f.result()

            if not res:
                continue

            code, pair = res

            now_str = datetime.now().isoformat(
                timespec="seconds"
            )

            if (
                code == "ADD_SELL"
                and pair not in sell_pairs_now
            ):

                updated_sell.append({
                    "pair": pair,
                    "state": "waiting_bounce",
                    "added": now_str
                })

                sell_pairs_now.add(pair)

                new_sell += 1

    # ============================================================
    # SAVE WATCHLIST
    # ============================================================
    with open(SELL_FILE, "w") as f:

        json.dump(
            updated_sell,
            f,
            indent=2
        )

    # ============================================================
    # DONE
    # ============================================================
    print(
        f"\nDone. "
        f"New SELL Watchlist Added: {new_sell}"
    )

    print(
        f"SELL Watchlist Total: {len(updated_sell)}"
    )


# ================================================================
# START
# ================================================================
if __name__ == "__main__":

    main()