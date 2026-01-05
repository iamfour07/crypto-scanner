import requests
import pandas as pd
import math
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Alert import send_telegram_message

# =====================
# CONFIG
# =====================
MAX_WORKERS = 15
RESOLUTION = "60"
LIMIT_HOURS = 2000

EMA_10 = 10
EMA_20 = 20
EMA_100 = 100

CAPITAL_RS = 600
MAX_LOSS_RS = 200
MAX_LEVERAGE = 10
RR_LEVELS = [2, 3, 4]

# =====================
# HELPERS
# =====================

def get_active_usdt_coins():
    url = (
        "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
        "active_instruments?margin_currency_short_name[]=USDT"
    )
    return requests.get(url, timeout=30).json()

def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        r = requests.get(url, timeout=10).json()
        pc = r.get("price_change_percent", {}).get("1D")
        return {"pair": pair, "change": float(pc)} if pc else None
    except:
        return None

def fetch_candles(pair):
    try:
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
        if len(data) < 200:
            return pair, None

        df = pd.DataFrame(data)
        for c in ["open", "high", "low", "close"]:
            df[c] = pd.to_numeric(df[c])

        df["EMA10"] = df["close"].ewm(span=EMA_10, adjust=False).mean()
        df["EMA20"] = df["close"].ewm(span=EMA_20, adjust=False).mean()
        df["EMA100"] = df["close"].ewm(span=EMA_100, adjust=False).mean()

        return pair, df.dropna().reset_index(drop=True)

    except:
        return pair, None

# =====================
# LOGIC HELPERS
# =====================

def green(c): return c["close"] > c["open"]
def red(c): return c["close"] < c["open"]

def ema10_support(c):
    return c["close"] > c["EMA10"] and (c["low"] <= c["EMA10"] or c["low"] <= c["EMA20"])

def ema10_rejection(c):
    return c["close"] < c["EMA10"] and (c["high"] >= c["EMA10"] or c["high"] >= c["EMA20"])

def calc_trade(entry, sl):
    risk = abs(entry - sl)
    qty = math.floor(MAX_LOSS_RS / risk)

    position_value = qty * entry
    leverage = min(math.ceil(position_value / CAPITAL_RS), MAX_LEVERAGE)

    capital_used = position_value / leverage

    targets = {
        f"1:{rr}": entry + rr * risk if entry > sl else entry - rr * risk
        for rr in RR_LEVELS
    }

    return leverage, capital_used, targets


# =====================
# MAIN
# =====================

def main():

    pairs = get_active_usdt_coins()
    changes = []

    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        for f in as_completed([ex.submit(fetch_pair_stats, p) for p in pairs]):
            if f.result():
                changes.append(f.result())

    df = pd.DataFrame(changes)

    gainers = df.sort_values("change", ascending=False).iloc[6:15]["pair"].tolist()
    losers  = df.sort_values("change", ascending=True).iloc[6:15]["pair"].tolist()

    scan_pairs = set(gainers + losers)

    candle_map = {}
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        for f in as_completed([ex.submit(fetch_candles, p) for p in scan_pairs]):
            pair, dfc = f.result()
            if dfc is not None:
                candle_map[pair] = dfc

    # BUY
    for pair in gainers:
        dfc = candle_map.get(pair)
        if dfc is None or len(dfc) < 5:
            continue

        setup = dfc.iloc[-3]
        confirm = dfc.iloc[-2]

        if (
            green(setup) and green(confirm) and
            setup["close"] > setup["EMA100"] and
            confirm["close"] > confirm["EMA100"] and
            ema10_support(setup) and
            confirm["high"] > setup["high"]
        ):
            entry = confirm["high"]
            sl = confirm["low"]
            lev, capital_used, targets = calc_trade(entry, sl)
            print(
                f"🟢 BUY SETUP\n{pair}\n"
                f"Entry: {entry:.4f}\nSL: {sl:.4f}\n"
                f"Capital Used: ₹{capital_used:.2f}\n"
                f"Leverage: {lev}x\n"
                + "\n".join([f"{k}: {v:.4f}" for k, v in targets.items()])
            )

            send_telegram_message(
                f"🟢 BUY SETUP\n{pair}\n"
                f"Entry: {entry:.4f}\nSL: {sl:.4f}\n"
                f"Capital Used: ₹{capital_used:.2f}\n"
                f"Leverage: {lev}x\n"
                + "\n".join([f"{k}: {v:.4f}" for k, v in targets.items()])
            )

    # SELL
    for pair in losers:
        dfc = candle_map.get(pair)
        if dfc is None or len(dfc) < 5:
            continue

        setup = dfc.iloc[-3]
        confirm = dfc.iloc[-2]

        if (
            red(setup) and red(confirm) and
            setup["close"] < setup["EMA100"] and
            confirm["close"] < confirm["EMA100"] and
            ema10_rejection(setup) and
            confirm["low"] < setup["low"]
        ):
            entry = confirm["low"]
            sl = confirm["high"]
            lev, capital_used, targets = calc_trade(entry, sl)
            print(
                f"🔴 SELL SETUP\n{pair}\n"
                f"Entry: {entry:.4f}\nSL: {sl:.4f}\n"
                f"Capital Used: ₹{capital_used:.2f}\n"
                f"Leverage: {lev}x\n"
                + "\n".join([f"{k}: {v:.4f}" for k, v in targets.items()])
            )

            send_telegram_message(
                f"🔴 SELL SETUP\n{pair}\n"
                f"Entry: {entry:.4f}\nSL: {sl:.4f}\n"
                f"Capital Used: ₹{capital_used:.2f}\n"
                f"Leverage: {lev}x\n"
                + "\n".join([f"{k}: {v:.4f}" for k, v in targets.items()])
            )


# =====================
if __name__ == "__main__":
    main()
