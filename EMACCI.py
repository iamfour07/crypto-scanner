import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from CCI_Calculater import calculate_cci
from Telegram_Alert import send_telegram_message


# ===================================================
# CONFIG
# ===================================================

RESOLUTION = "60"
LIMIT_HOURS = 1000

EMA_PERIODS = [20, 50, 100, 300]
CCI_PERIOD = 200

EXCLUDE_TOP = 4
SELECT_COUNT = 25

MAX_WORKERS = 12


# ---- Risk & Capital ----
CAPITAL_RS = 500
MAX_LOSS_RS = 50
MAX_ALLOWED_LEVERAGE = 30
MIN_LEVERAGE = 5



def fetch_stats_parallel(pairs):

    stats = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = {executor.submit(fetch_pair_stats, p): p for p in pairs}

        for f in as_completed(futures):
            s = f.result()
            if s:
                stats.append(s)

    return stats


# ===================================================
# ACTIVE FUTURES
# ===================================================

def get_active_usdt_coins():
    url = (
        "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
        "active_instruments?margin_currency_short_name[]=USDT"
    )
    return requests.get(url, timeout=30).json()


# ===================================================
# 1D % CHANGE
# ===================================================

def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        r = requests.get(url, timeout=10).json()
        pc = r.get("price_change_percent", {}).get("1D")
        return {"pair": pair, "change": float(pc)} if pc is not None else None
    except:
        return None


# ===================================================
# CANDLES
# ===================================================

def fetch_candles(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    frm = now - LIMIT_HOURS * 3600

    url = "https://public.coindcx.com/market_data/candlesticks"

    params = {
        "pair": pair,
        "from": frm,
        "to": now,
        "resolution": RESOLUTION,
        "pcode": "f",
    }

    data = requests.get(url, params=params, timeout=15).json().get("data", [])

    if len(data) < max(EMA_PERIODS) + CCI_PERIOD + 5:
        return None

    df = pd.DataFrame(data)

    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df.dropna().reset_index(drop=True)


# ===================================================
# EMA + STRUCTURE
# ===================================================

def calculate_emas(df):
    for p in EMA_PERIODS:
        df[f"EMA_{p}"] = df["close"].ewm(span=p, adjust=False).mean()
    return df


def is_bullish_stack(row):
    return row["EMA_20"] > row["EMA_50"] > row["EMA_100"] > row["EMA_300"]


def is_bearish_stack(row):
    return row["EMA_20"] < row["EMA_50"] < row["EMA_100"] < row["EMA_300"]


# ===================================================
# RISK / POSITION SIZING
# ===================================================

def calculate_trade_levels(entry, sl, side):

    for lev in range(MAX_ALLOWED_LEVERAGE, MIN_LEVERAGE - 1, -1):

        position_value = CAPITAL_RS * lev

        if side == "BUY":
            risk = entry - sl
        else:
            risk = sl - entry

        loss = (risk / entry) * position_value

        if loss <= MAX_LOSS_RS:
            leverage = lev
            break
    else:
        leverage = MIN_LEVERAGE

    if side == "BUY":
        t2 = entry + risk * 2
        t3 = entry + risk * 3
        t4 = entry + risk * 4
    else:
        t2 = entry - risk * 2
        t3 = entry - risk * 3
        t4 = entry - risk * 4

    return {
        "entry": round(entry, 4),
        "sl": round(sl, 4),
        "lev": leverage,
        "t2": round(t2, 4),
        "t3": round(t3, 4),
        "t4": round(t4, 4),
    }


# ===================================================
# MID MOVERS
# ===================================================

def select_mid_movers(stats):

    sorted_gainers = sorted(stats, key=lambda x: x["change"], reverse=True)
    sorted_losers = sorted(stats, key=lambda x: x["change"])

    return (
        sorted_gainers[EXCLUDE_TOP: EXCLUDE_TOP + SELECT_COUNT],
        sorted_losers[EXCLUDE_TOP: EXCLUDE_TOP + SELECT_COUNT]
    )


# ===================================================
# 🔥 THREAD WORKER
# ===================================================

def process_pair(pair, side):

    df = fetch_candles(pair)
    if df is None:
        return None

    df = calculate_emas(df)
    df["CCI"] = calculate_cci(df["high"], df["low"], df["close"], CCI_PERIOD)

    last = df.iloc[-1]
    prev = df.iloc[-2]

    # -------- BUY --------
    if side == "bull" and is_bullish_stack(last) and prev["CCI"] < 100 and last["CCI"] > 100:

        entry = last["close"]
        sl = last["low"]

        trade = calculate_trade_levels(entry, sl, "BUY")

        return (
            f"🟢 BUY {pair}\n"
            f"Entry : {trade['entry']}\n"
            f"SL    : {trade['sl']}\n"
            f"Lev   : {trade['lev']}x\n"
            f"T2:{trade['t2']}  T3:{trade['t3']}  T4:{trade['t4']}"
        )

    # -------- SELL --------
    if side == "bear" and is_bearish_stack(last) and prev["CCI"] > -100 and last["CCI"] < -100:

        entry = last["close"]
        sl = last["high"]

        trade = calculate_trade_levels(entry, sl, "SELL")

        return (
            f"🔴 SELL {pair}\n"
            f"Entry : {trade['entry']}\n"
            f"SL    : {trade['sl']}\n"
            f"Lev   : {trade['lev']}x\n"
            f"T2:{trade['t2']}  T3:{trade['t3']}  T4:{trade['t4']}"
        )

    return None


# ===================================================
# MAIN
# ===================================================

def main():
    active = get_active_usdt_coins()

    stats = fetch_stats_parallel(active)

    gainers, losers = select_mid_movers(stats)

    alerts = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = []

        for g in gainers:
            futures.append(executor.submit(process_pair, g["pair"], "bull"))

        for l in losers:
            futures.append(executor.submit(process_pair, l["pair"], "bear"))

        for f in as_completed(futures):
            r = f.result()
            if r:
                alerts.append(r)
                print(r)

    # 🔥 TELEGRAM ALERT
    if alerts:
        send_telegram_message("\n\n".join(alerts))


# ===================================================

if __name__ == "__main__":
    main()
