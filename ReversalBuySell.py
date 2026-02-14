import json
import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Swing import Send_Swing_Telegram_Message

# CONFIG
resolution = "60"
limit_hours = 1000
MAX_WORKERS = 15

BUY_FILE = "ReversalBuyWatchlist.json"
SELL_FILE = "ReversalSellWatchlist.json"

BB_LENGTH = 200
BB_MULT = 2.5

ST_LENGTH = 9
ST_FACTOR = 1.5

CAPITAL_RS = 1000
MAX_LOSS_RS = 100
MAX_ALLOWED_LEVERAGE = 20
MIN_LEVERAGE = 5

# Risk Filters
MIN_RISK_PERCENT = 0.2
IDEAL_MIN_RISK = 0.4
IDEAL_MAX_RISK = 1.2
MAX_RISK_PERCENT = 2.5
TOP_COINS_TO_SCAN = 20   # change to 5, 15, 20 anytime

# ================= RISK CALC =================
def calculate_trade_levels(entry, sl, side):
    risk = abs(entry - sl)
    risk_percent = (risk / entry) * 100

    if risk_percent < MIN_RISK_PERCENT:
        return None
    if risk_percent > MAX_RISK_PERCENT:
        return None

    for lev in range(MAX_ALLOWED_LEVERAGE, MIN_LEVERAGE - 1, -1):
        position_value = CAPITAL_RS * lev
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

    return entry, sl, leverage, CAPITAL_RS, t2, t3, t4, risk_percent


# ================= API =================
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    pairs = []
    for x in data:
        if isinstance(x, str):
            pairs.append(x)
        elif isinstance(x, dict) and "pair" in x:
            pairs.append(x["pair"])
    return pairs


# ================= TOP MOVERS =================
def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        r = requests.get(url, timeout=8)
        r.raise_for_status()
        pc = r.json().get("price_change_percent", {}).get("1D")
        if pc is None:
            return None
        return {"pair": pair, "change": float(pc)}
    except:
        return None


def get_top_movers(pairs, top_n=20):
    gainers = []
    losers = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_pair_stats, p) for p in pairs]

        for f in as_completed(futures):
            res = f.result()
            if not res:
                continue

            if res["change"] > 0:
                gainers.append(res)
            else:
                losers.append(res)

    gainers = sorted(gainers, key=lambda x: x["change"], reverse=True)[:top_n]
    losers = sorted(losers, key=lambda x: x["change"])[:top_n]

    # print("\nTop Gainers:")
    # for g in gainers:
    #     print(g["pair"], f"{g['change']:.2f}%")

    # print("\nTop Losers:")
    # for l in losers:
    #     print(l["pair"], f"{l['change']:.2f}%")

    return [x["pair"] for x in gainers + losers]


# ================= INDICATORS =================
def calculate_heikin_ashi(df):
    df["HA_close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    df["HA_open"] = (df["open"].shift(1) + df["close"].shift(1)) / 2
    df.iloc[0, df.columns.get_loc("HA_open")] = (df.iloc[0]["open"] + df.iloc[0]["close"]) / 2
    df["HA_high"] = df[["HA_open", "HA_close", "high"]].max(axis=1)
    df["HA_low"] = df[["HA_open", "HA_close", "low"]].min(axis=1)
    return df


def calculate_bollinger(df):
    df["BB_mid"] = df["HA_close"].rolling(BB_LENGTH).mean()
    df["BB_std"] = df["HA_close"].rolling(BB_LENGTH).std()
    df["BB_upper"] = df["BB_mid"] + BB_MULT * df["BB_std"]
    df["BB_lower"] = df["BB_mid"] - BB_MULT * df["BB_std"]
    return df


def rma(series, period):
    return series.ewm(alpha=1/period, adjust=False).mean()


def calculate_supertrend(df):
    hl = df["HA_high"] - df["HA_low"]
    hc = (df["HA_high"] - df["HA_close"].shift()).abs()
    lc = (df["HA_low"] - df["HA_close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)

    atr = rma(tr, ST_LENGTH)

    hl2 = (df["HA_high"] + df["HA_low"]) / 2
    upperband = hl2 + ST_FACTOR * atr
    lowerband = hl2 - ST_FACTOR * atr

    supertrend = [True] * len(df)
    st_value = [0] * len(df)

    for i in range(1, len(df)):
        if supertrend[i - 1]:
            if df["HA_high"].iloc[i] < lowerband.iloc[i]:
                supertrend[i] = False
                st_value[i] = upperband.iloc[i]
            else:
                supertrend[i] = True
                st_value[i] = lowerband.iloc[i]
        else:
            if df["HA_low"].iloc[i] > upperband.iloc[i]:
                supertrend[i] = True
                st_value[i] = lowerband.iloc[i]
            else:
                supertrend[i] = False
                st_value[i] = upperband.iloc[i]

    df["supertrend"] = supertrend
    df["ST_value"] = st_value
    return df


# ================= FETCH =================
def fetch_and_prepare(pair):
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        from_time = now - limit_hours * 3600

        url = "https://public.coindcx.com/market_data/candlesticks"
        params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}

        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()

        data = r.json().get("data", [])
        if not data or len(data) < BB_LENGTH + 10:
            return None

        df = pd.DataFrame(data)
        df = df.sort_values("time").reset_index(drop=True)
        df = df.iloc[:-1].copy()

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = calculate_heikin_ashi(df)
        df = calculate_bollinger(df)
        df = calculate_supertrend(df)

        return df.dropna().reset_index(drop=True)

    except:
        return None


# ================= WATCHLIST =================
def load_watchlist(file):
    try:
        with open(file) as f:
            data = json.load(f)
            for x in data:
                x["entry_state"] = bool(x["entry_state"])
            return data
    except:
        return []


def save_watchlist(file, data):
    with open(file, "w") as f:
        json.dump(data, f, indent=2)


def find_entry(pair, watchlist):
    for x in watchlist:
        if x["pair"] == pair:
            return x
    return None


# ================= PROCESS =================
def process_pair(pair, buy_watch, sell_watch):
    df = fetch_and_prepare(pair)
    if df is None:
        return None

    last_closed = df.iloc[-1]
    st_state = bool(last_closed["supertrend"])

    results = []

    # Remove invalid setups
    if find_entry(pair, sell_watch) and not st_state:
        sell_watch[:] = [x for x in sell_watch if x["pair"] != pair]

    if find_entry(pair, buy_watch) and st_state:
        buy_watch[:] = [x for x in buy_watch if x["pair"] != pair]

    # Add to watchlist
    if not find_entry(pair, buy_watch) and not find_entry(pair, sell_watch):

        if last_closed["HA_close"] >= last_closed["BB_upper"] and st_state:
            results.append(("add_sell", pair, True))

        elif last_closed["HA_close"] <= last_closed["BB_lower"] and not st_state:
            results.append(("add_buy", pair, False))

    # Flip check
    if find_entry(pair, sell_watch) and not st_state:
        results.append(("sell_signal", pair,
                        float(last_closed["HA_low"]),
                        float(last_closed["ST_value"])))

    if find_entry(pair, buy_watch) and st_state:
        results.append(("buy_signal", pair,
                        float(last_closed["HA_high"]),
                        float(last_closed["ST_value"])))

    return results


# ================= MAIN =================
def main():
    all_pairs = get_active_usdt_coins()
    pairs = get_top_movers(all_pairs, top_n=TOP_COINS_TO_SCAN)

    buy_watch = load_watchlist(BUY_FILE)
    sell_watch = load_watchlist(SELL_FILE)
    buy_watch = [x for x in buy_watch if x["pair"] in pairs]
    sell_watch = [x for x in sell_watch if x["pair"] in pairs]

    alerts = []

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(process_pair, p, buy_watch, sell_watch) for p in pairs]

        for f in as_completed(futures):
            res = f.result()
            if not res:
                continue

            for r in res:
                action = r[0]

                if action == "add_buy":
                    buy_watch.append({"pair": r[1], "entry_state": r[2]})

                elif action == "add_sell":
                    sell_watch.append({"pair": r[1], "entry_state": r[2]})

                elif action == "buy_signal":
                    levels = calculate_trade_levels(r[2], r[3], "BUY")
                    if levels:
                        entry, sl, lev, margin_used, t2, t3, t4, risk_pct = levels

                        # Ideal zone tag
                        if IDEAL_MIN_RISK <= risk_pct <= IDEAL_MAX_RISK:
                            risk_label = " (Ideal Zone)"
                        else:
                            risk_label = ""

                        msg = (
                            f"🟢 BUY SIGNAL\n\n"
                            f"Name: {r[1]}\n"
                            f"Entry: {entry:.4f}\n"
                            f"Stop Loss: {sl:.4f}\n"
                            f"Risk: {risk_pct:.2f}%{risk_label}\n"
                            f"Margin Used: ₹{margin_used:.2f} ({lev}x)\n\n"
                            f"🎯 Targets:\n"
                            f"• 1:2 → {t2:.4f}\n"
                            f"• 1:3 → {t3:.4f}\n"
                            f"• 1:4 → {t4:.4f}\n"
                            f"-----------------------\n"
                        )

                        alerts.append(msg)

                    buy_watch = [x for x in buy_watch if x["pair"] != r[1]]

                elif action == "sell_signal":
                    levels = calculate_trade_levels(r[2], r[3], "SELL")
                    if levels:
                        entry, sl, lev, margin_used, t2, t3, t4, risk_pct = levels

                        if IDEAL_MIN_RISK <= risk_pct <= IDEAL_MAX_RISK:
                            risk_label = " (Ideal Zone)"
                        else:
                            risk_label = ""

                        msg = (
                            f"🔴 SELL SIGNAL\n\n"
                            f"Name: {r[1]}\n"
                            f"Entry: {entry:.4f}\n"
                            f"Stop Loss: {sl:.4f}\n"
                            f"Risk: {risk_pct:.2f}%{risk_label}\n"
                            f"Margin Used: ₹{margin_used:.2f} ({lev}x)\n\n"
                            f"🎯 Targets:\n"
                            f"• 1:2 → {t2:.4f}\n"
                            f"• 1:3 → {t3:.4f}\n"
                            f"• 1:4 → {t4:.4f}\n"
                            f"-----------------------\n"
                        )

                        alerts.append(msg)

                    sell_watch = [x for x in sell_watch if x["pair"] != r[1]]

    if alerts:
        Send_Swing_Telegram_Message("\n\n".join(alerts))

    save_watchlist(BUY_FILE, buy_watch)
    save_watchlist(SELL_FILE, sell_watch)


if __name__ == "__main__":
    main()