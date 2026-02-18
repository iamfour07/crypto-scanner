# import json
# import requests
# import pandas as pd
# from datetime import datetime, timezone
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from Telegram_Swing import Send_Swing_Telegram_Message

# # CONFIG
# resolution = "60"
# limit_hours = 1000
# MAX_WORKERS = 8
# TOP_COINS_TO_SCAN = 30

# BUY_FILE = "ReversalBuyWatchlist.json"
# SELL_FILE = "ReversalSellWatchlist.json"

# BB_LENGTH = 200
# BB_MULT = 2.5
# ST_LENGTH = 9
# ST_FACTOR = 1.5

# CAPITAL_RS = 1000
# MAX_LOSS_RS = 100
# MAX_ALLOWED_LEVERAGE = 20
# MIN_LEVERAGE = 5
# TOP_COINS_TO_SCAN = 20


# # ================= UTIL =================
# def safe_get(url, params=None, timeout=10):
#     try:
#         r = requests.get(url, params=params, timeout=timeout)
#         r.raise_for_status()
#         return r.json()
#     except:
#         return None


# # ================= RISK =================
# def calculate_trade_levels(entry, sl, side):
#     risk = abs(entry - sl)
#     risk_percent = (risk / entry) * 100

#     leverage = MIN_LEVERAGE
#     for lev in range(MAX_ALLOWED_LEVERAGE, MIN_LEVERAGE - 1, -1):
#         position_value = CAPITAL_RS * lev
#         loss = (risk / entry) * position_value
#         if loss <= MAX_LOSS_RS:
#             leverage = lev
#             break

#     if side == "BUY":
#         targets = [entry + risk * x for x in (2, 3, 4)]
#     else:
#         targets = [entry - risk * x for x in (2, 3, 4)]

#     return entry, sl, leverage, targets, risk_percent


# # ================= API =================
# def get_active_usdt_coins():
#     url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
#     data = safe_get(url, timeout=30)
#     if not data:
#         return []
#     return [x["pair"] if isinstance(x, dict) else x for x in data]


# def fetch_pair_stats(pair):
#     url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
#     data = safe_get(url, timeout=8)
#     if not data:
#         return None
#     pc = data.get("price_change_percent", {}).get("1D")
#     return {"pair": pair, "change": float(pc)} if pc else None


# def get_top_movers(pairs):
#     gainers, losers = [], []

#     with ThreadPoolExecutor(max_workers=8) as executor:
#         futures = [executor.submit(fetch_pair_stats, p) for p in pairs]
#         for f in as_completed(futures):
#             res = f.result()
#             if not res:
#                 continue
#             (gainers if res["change"] > 0 else losers).append(res)

#     gainers = sorted(gainers, key=lambda x: x["change"], reverse=True)[:TOP_COINS_TO_SCAN]
#     losers = sorted(losers, key=lambda x: x["change"])[:TOP_COINS_TO_SCAN]

#     return [x["pair"] for x in gainers + losers]


# # ================= INDICATORS =================
# def calculate_heikin_ashi(df):
#     df["HA_close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
#     df["HA_open"] = (df["open"].shift(1) + df["close"].shift(1)) / 2
#     df.iloc[0, df.columns.get_loc("HA_open")] = (df.iloc[0]["open"] + df.iloc[0]["close"]) / 2
#     df["HA_high"] = df[["HA_open", "HA_close", "high"]].max(axis=1)
#     df["HA_low"] = df[["HA_open", "HA_close", "low"]].min(axis=1)
#     return df


# def calculate_bollinger(df):
#     mid = df["HA_close"].rolling(BB_LENGTH).mean()
#     std = df["HA_close"].rolling(BB_LENGTH).std()
#     df["BB_upper"] = mid + BB_MULT * std
#     df["BB_lower"] = mid - BB_MULT * std
#     return df


# def rma(series, period):
#     return series.ewm(alpha=1/period, adjust=False).mean()


# def calculate_supertrend(df):
#     # ATR
#     df["H-L"] = df["HA_high"] - df["HA_low"]
#     df["H-PC"] = abs(df["HA_high"] - df["HA_close"].shift(1))
#     df["L-PC"] = abs(df["HA_low"] - df["HA_close"].shift(1))
#     df["TR"] = df[["H-L", "H-PC", "L-PC"]].max(axis=1)
#     df["ATR"] = rma(df["TR"], ST_LENGTH)

#     hl2 = (df["HA_high"] + df["HA_low"]) / 2
#     df["basic_upperband"] = hl2 + ST_FACTOR * df["ATR"]
#     df["basic_lowerband"] = hl2 - ST_FACTOR * df["ATR"]

#     final_upperband = [0] * len(df)
#     final_lowerband = [0] * len(df)
#     supertrend = [True] * len(df)

#     for i in range(len(df)):
#         if i == 0:
#             final_upperband[i] = df["basic_upperband"].iloc[i]
#             final_lowerband[i] = df["basic_lowerband"].iloc[i]
#             supertrend[i] = True
#             continue

#         if (
#             df["basic_upperband"].iloc[i] < final_upperband[i - 1]
#             or df["HA_close"].iloc[i - 1] > final_upperband[i - 1]
#         ):
#             final_upperband[i] = df["basic_upperband"].iloc[i]
#         else:
#             final_upperband[i] = final_upperband[i - 1]

#         if (
#             df["basic_lowerband"].iloc[i] > final_lowerband[i - 1]
#             or df["HA_close"].iloc[i - 1] < final_lowerband[i - 1]
#         ):
#             final_lowerband[i] = df["basic_lowerband"].iloc[i]
#         else:
#             final_lowerband[i] = final_lowerband[i - 1]

#         if supertrend[i - 1]:
#             supertrend[i] = df["HA_close"].iloc[i] >= final_lowerband[i]
#         else:
#             supertrend[i] = df["HA_close"].iloc[i] > final_upperband[i]

#     df["supertrend"] = supertrend
#     df["ST_value"] = [
#         final_lowerband[i] if supertrend[i] else final_upperband[i]
#         for i in range(len(df))
#     ]

#     return df

# # ================= FETCH =================
# def fetch_and_prepare(pair):
#     now = int(datetime.now(timezone.utc).timestamp())
#     from_time = now - limit_hours * 3600

#     url = "https://public.coindcx.com/market_data/candlesticks"
#     params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}

#     data = safe_get(url, params)
#     if not data or "data" not in data:
#         return None

#     candles = data["data"]
#     if len(candles) < BB_LENGTH + 10:
#         return None

#     df = pd.DataFrame(candles).sort_values("time").iloc[:-1]

#     for col in ["open", "high", "low", "close"]:
#         df[col] = pd.to_numeric(df[col], errors="coerce")

#     df = calculate_heikin_ashi(df)
#     df = calculate_bollinger(df)
#     df = calculate_supertrend(df)

#     return df.dropna()


# # ================= WATCHLIST =================
# def load_watchlist(file):
#     try:
#         with open(file) as f:
#             data = json.load(f)
#             return [{"pair": str(x["pair"])} for x in data if "pair" in x]
#     except:
#         return []


# def save_watchlist(file, data):
#     clean_data = [{"pair": str(x["pair"])} for x in data]
#     with open(file, "w") as f:
#         json.dump(clean_data, f, indent=2)

# def format_signal(side, pair, entry, sl):
#     entry, sl, lev, targets, risk_percent = calculate_trade_levels(entry, sl, side)

#     msg = (
#         f"{'🟢 BUY' if side=='BUY' else '🔴 SELL'} {pair}\n"
#         f"Entry: {entry:.4f}\n"
#         f"SL: {sl:.4f}\n"
#         f"Leverage: {lev}x\n"
#         f"Targets: {', '.join([f'{t:.4f}' for t in targets])}\n"
#         f"Risk: {risk_percent:.2f}%"
#     )
#     return msg

# # ================= WATCHLIST FLIP MODULE =================
# def check_watchlist_flips(watchlist, side):
#     updated = []
#     alerts = []

#     def check_pair(pair):
#         df = fetch_and_prepare(pair)
#         if df is None or len(df) < 5:
#             return ("KEEP", pair)

#         prev2 = df.iloc[-3]
#         prev1 = df.iloc[-2]

#         st_prev = prev2["supertrend"]
#         st_now = prev1["supertrend"]

#         if side == "BUY":
#             if (not st_prev) and st_now:
#                 return ("SIGNAL", pair, float(prev1["HA_high"]), float(prev1["ST_value"]))

#         if side == "SELL":
#             if st_prev and not st_now:
#                 return ("SIGNAL", pair, float(prev1["HA_low"]), float(prev1["ST_value"]))

#         return ("KEEP", pair)

#     with ThreadPoolExecutor(MAX_WORKERS) as executor:
#         futures = [executor.submit(check_pair, x["pair"]) for x in watchlist]

#         for f in as_completed(futures):
#             res = f.result()
#             if res[0] == "SIGNAL":
#                 alerts.append(format_signal(side, res[1], res[2], res[3]))
#             else:
#                 updated.append({"pair": res[1]})

#     return updated, alerts


# # ================= SETUP MODULE =================
# def scan_for_setups(top_pairs, buy_watch, sell_watch):

#     buy_set = {x["pair"] for x in buy_watch}
#     sell_set = {x["pair"] for x in sell_watch}

#     new_buy = []
#     new_sell = []

#     def process_pair(pair):
#         if pair in buy_set or pair in sell_set:
#             return None

#         df = fetch_and_prepare(pair)
#         if df is None:
#             return None

#         last = df.iloc[-1]
#         st_now = last["HA_close"] > last["ST_value"]

#         # DEBUG PRINT
#         st_color = "GREEN" if st_now else "RED"

#         # SELL setup
#         if last["HA_high"] >= last["BB_upper"] and st_now:
#             return ("SELL", pair)

#         # BUY setup
#         if last["HA_low"] <= last["BB_lower"] and not st_now:
#             return ("BUY", pair)

#         return None

#     with ThreadPoolExecutor(MAX_WORKERS) as executor:
#         futures = [executor.submit(process_pair, p) for p in top_pairs]

#         for f in as_completed(futures):
#             res = f.result()
#             if not res:
#                 continue
#             if res[0] == "BUY":
#                 new_buy.append({"pair": res[1]})
#             else:
#                 new_sell.append({"pair": res[1]})

#     buy_watch.extend(new_buy)
#     sell_watch.extend(new_sell)

#     return buy_watch, sell_watch


# # ================= MAIN =================
# def main():

#     buy_watch = load_watchlist(BUY_FILE)
#     sell_watch = load_watchlist(SELL_FILE)

#     buy_watch, buy_alerts = check_watchlist_flips(buy_watch, "BUY")
#     sell_watch, sell_alerts = check_watchlist_flips(sell_watch, "SELL")

#     alerts = buy_alerts + sell_alerts

#     top_pairs = get_top_movers(get_active_usdt_coins())
#     buy_watch, sell_watch = scan_for_setups(top_pairs, buy_watch, sell_watch)

#     if alerts:
#         Send_Swing_Telegram_Message("\n\n".join(alerts))

#     save_watchlist(BUY_FILE, buy_watch)
#     save_watchlist(SELL_FILE, sell_watch)


# if __name__ == "__main__":
#     main()

import json
import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Swing import Send_Swing_Telegram_Message

# CONFIG
resolution = "60"
limit_hours = 500
TOP_COINS_TO_SCAN = 20
MAX_WORKERS = 8

BUY_FILE = "ReversalBuyWatchlist.json"
SELL_FILE = "ReversalSellWatchlist.json"

BB_LENGTH = 200
BB_MULT = 2.5

CAPITAL_RS = 600
MAX_LOSS_RS = 50
MAX_ALLOWED_LEVERAGE = 30
MIN_LEVERAGE = 5


# ================= UTIL =================
def safe_get(url, params=None, timeout=10):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except:
        return None


# ================= RISK =================
def calculate_trade_levels(entry, sl, side):

    risk_per_unit = abs(entry - sl)
    if risk_per_unit == 0:
        return None

    # Position value required to risk ₹50
    required_position_value = (MAX_LOSS_RS / risk_per_unit) * entry

    # Capital required at 5x leverage
    capital_needed_at_5x = required_position_value / MIN_LEVERAGE

    # Decide leverage
    if capital_needed_at_5x <= CAPITAL_RS:
        leverage = MIN_LEVERAGE
    else:
        leverage = required_position_value / CAPITAL_RS
        leverage = min(leverage, MAX_ALLOWED_LEVERAGE)

    leverage = round(leverage, 2)

    used_capital = CAPITAL_RS
    position_value = used_capital * leverage
    expected_loss = (risk_per_unit / entry) * position_value

    # Targets
    if side == "BUY":
        t2 = entry + risk_per_unit * 2
        t3 = entry + risk_per_unit * 3
        t4 = entry + risk_per_unit * 4
    else:
        t2 = entry - risk_per_unit * 2
        t3 = entry - risk_per_unit * 3
        t4 = entry - risk_per_unit * 4

    return entry, sl, leverage, used_capital, expected_loss, t2, t3, t4


# ================= API =================
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    data = safe_get(url, timeout=30)
    if not data:
        return []
    return [x["pair"] if isinstance(x, dict) else x for x in data]


def fetch_pair_stats(pair):
    url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
    data = safe_get(url, timeout=8)
    if not data:
        return None
    pc = data.get("price_change_percent", {}).get("1D")
    return {"pair": pair, "change": float(pc)} if pc else None


def get_top_movers(pairs):
    gainers, losers = [], []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_pair_stats, p) for p in pairs]
        for f in as_completed(futures):
            res = f.result()
            if not res:
                continue
            (gainers if res["change"] > 0 else losers).append(res)

    gainers = sorted(gainers, key=lambda x: x["change"], reverse=True)[:TOP_COINS_TO_SCAN]
    losers = sorted(losers, key=lambda x: x["change"])[:TOP_COINS_TO_SCAN]

    return [x["pair"] for x in gainers + losers]


# ================= HEIKIN ASHI =================
def calculate_heikin_ashi(df):
    df["HA_close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    df["HA_open"] = (df["open"].shift(1) + df["close"].shift(1)) / 2
    df.iloc[0, df.columns.get_loc("HA_open")] = (df.iloc[0]["open"] + df.iloc[0]["close"]) / 2
    df["HA_high"] = df[["HA_open", "HA_close", "high"]].max(axis=1)
    df["HA_low"] = df[["HA_open", "HA_close", "low"]].min(axis=1)
    return df


# ================= BOLLINGER =================
def calculate_bollinger(df):
    mid = df["HA_close"].rolling(BB_LENGTH).mean()
    std = df["HA_close"].rolling(BB_LENGTH).std()
    df["BB_upper"] = mid + BB_MULT * std
    df["BB_lower"] = mid - BB_MULT * std
    return df


def calculate_sma(df):
    df["SMA5"] = df["HA_close"].rolling(5).mean()
    return df


# ================= FETCH =================
def fetch_candles(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - limit_hours * 3600

    url = "https://public.coindcx.com/market_data/candlesticks"
    params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}

    data = safe_get(url, params)
    if not data or "data" not in data:
        return None

    candles = data["data"]
    if len(candles) < BB_LENGTH + 5:
        return None

    df = pd.DataFrame(candles).sort_values("time").iloc[:-1]

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = calculate_heikin_ashi(df)
    df = calculate_bollinger(df)
    df = calculate_sma(df)

    return df.dropna()


# ================= WATCHLIST =================
def load_watchlist(file):
    try:
        with open(file) as f:
            return json.load(f)
    except:
        return []


def save_watchlist(file, data):
    with open(file, "w") as f:
        json.dump(data, f, indent=2)


# ================= WATCHLIST ALERT MODULE =================
def check_watchlist_for_signals(watchlist, side):

    updated_watchlist = []
    alerts = []

    def process_pair(pair):
        df = fetch_candles(pair)
        if df is None or len(df) < 10:
            return ("KEEP", pair)

        last = df.iloc[-1]

        if side == "SELL":
            if (
                last["HA_close"] < last["BB_upper"] and
                last["HA_close"] < last["SMA5"] and
                last["HA_high"] <= last["BB_upper"]
            ):
                entry = float(last["HA_low"])
                sl = float(last["HA_high"])

                e, s, lev, cap, loss, t2, t3, t4 = calculate_trade_levels(entry, sl, "SELL")

                link = f"https://coindcx.com/futures/{pair}"

                msg = (
                    f"🔴 SELL {pair}\n"
                    f"Entry   : {round(e,4)}\n"
                    f"SL      : {round(s,4)}\n"
                    f"Capital : ₹{cap} ({lev}×)\n\n"
                    f"Risk    : ₹{round(loss,2)}\n"
                    f"Targets\n"
                    f"2R → {round(t2,4)}\n"
                    f"3R → {round(t3,4)}\n"
                    f"4R → {round(t4,4)}\n"
                    f"{link}\n"
                    f"------------------------------------------------"
                )
                return ("SIGNAL", pair, msg)

        if side == "BUY":
            if (
                last["HA_close"] > last["BB_lower"] and
                last["HA_close"] > last["SMA5"] and
                last["HA_low"] >= last["BB_lower"]
            ):
                entry = float(last["HA_high"])
                sl = float(last["HA_low"])

                e, s, lev, cap, loss, t2, t3, t4 = calculate_trade_levels(entry, sl, "BUY")

                link = f"https://coindcx.com/futures/{pair}"

                msg = (
                    f"🟢 BUY {pair}\n"
                    f"Entry   : {round(e,4)}\n"
                    f"SL      : {round(s,4)}\n"
                    f"Capital : ₹{cap} ({lev}×)\n\n"
                    f"Risk    : ₹{round(loss,2)}\n"
                    f"Targets\n"
                    f"2R → {round(t2,4)}\n"
                    f"3R → {round(t3,4)}\n"
                    f"4R → {round(t4,4)}\n"
                    f"{link}\n"
                    f"------------------------------------------------"
                )
                return ("SIGNAL", pair, msg)

        return ("KEEP", pair)

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(process_pair, p) for p in watchlist]

        for f in as_completed(futures):
            res = f.result()
            if res[0] == "SIGNAL":
                alerts.append(res[2])
            else:
                updated_watchlist.append(res[1])

    return updated_watchlist, alerts


# ================= ADD TO WATCHLIST MODULE =================
def scan_for_breakouts(top_pairs, buy_watch, sell_watch):

    buy_set = set(buy_watch)
    sell_set = set(sell_watch)

    new_buy = []
    new_sell = []

    def process_pair(pair):
        if pair in buy_set or pair in sell_set:
            return None

        df = fetch_candles(pair)
        if df is None:
            return None

        last = df.iloc[-1]

        if last["HA_close"] > last["BB_upper"]:
            return ("SELL", pair)

        if last["HA_close"] < last["BB_lower"]:
            return ("BUY", pair)

        return None

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(process_pair, p) for p in top_pairs]

        for f in as_completed(futures):
            res = f.result()
            if not res:
                continue
            if res[0] == "BUY":
                new_buy.append(res[1])
            else:
                new_sell.append(res[1])

    for coin in new_buy:
        if coin not in buy_watch:
            buy_watch.append(coin)

    for coin in new_sell:
        if coin not in sell_watch:
            sell_watch.append(coin)

    return buy_watch, sell_watch


# ================= MAIN =================
def main():
    buy_watch = load_watchlist(BUY_FILE)
    sell_watch = load_watchlist(SELL_FILE)

    # Step 1: Scan watchlist for signals
    sell_watch, sell_alerts = check_watchlist_for_signals(sell_watch, "SELL")
    buy_watch, buy_alerts = check_watchlist_for_signals(buy_watch, "BUY")

    alerts = sell_alerts + buy_alerts

    if alerts:
        Send_Swing_Telegram_Message("\n\n".join(alerts))

    # Step 2: Scan market for new breakouts
    pairs = get_active_usdt_coins()
    top_pairs = get_top_movers(pairs)

    buy_watch, sell_watch = scan_for_breakouts(top_pairs, buy_watch, sell_watch)

    save_watchlist(BUY_FILE, buy_watch)
    save_watchlist(SELL_FILE, sell_watch)

    print("BUY WATCHLIST:", buy_watch)
    print("SELL WATCHLIST:", sell_watch)


if __name__ == "__main__":
    main()
