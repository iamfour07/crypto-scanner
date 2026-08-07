import json
import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Swing import Send_Swing_Telegram_Message

# CONFIG
resolution = "60"
limit_hours = 1000
TOP_GAINERS_TO_SCAN = 5
MAX_WORKERS = 8

# ── Toggle Signals ON/OFF ──
ENABLE_SELL = True   # Set False to disable SELL scanning

SELL_FILE = "ReversalSellWatchlist.json"

RSI_LENGTH = 14
RSI_THRESHOLD = 40

RISK_RS = 100        # Fixed risk per trade in ₹
LEVERAGE = 5         # Fixed leverage


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

    # Pure Risk/Reward flow:
    # Position Value = (Risk / risk_per_unit) * entry
    # Capital Used   = Position Value / Leverage
    position_value = (RISK_RS / risk_per_unit) * entry
    used_capital = round(position_value / LEVERAGE, 2)
    expected_loss = RISK_RS  # Always exactly RISK_RS

    # Sell-side targets
    t2 = entry - risk_per_unit * 2
    t3 = entry - risk_per_unit * 3
    t4 = entry - risk_per_unit * 4

    return entry, sl, LEVERAGE, used_capital, expected_loss, t2, t3, t4


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


def get_top_gainers(pairs):
    gainers = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_pair_stats, p) for p in pairs]
        for f in as_completed(futures):
            res = f.result()
            if not res:
                continue
            if res["change"] > 0:
                gainers.append(res)

    gainers = sorted(gainers, key=lambda x: x["change"], reverse=True)[:TOP_GAINERS_TO_SCAN]

    return [x["pair"] for x in gainers]


# ================= RSI =================
def rma(series, period):
    return series.ewm(alpha=1 / period, adjust=False).mean()


def calculate_rsi(df, length=RSI_LENGTH):
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = rma(gain, length)
    avg_loss = rma(loss, length)

    rs = avg_gain / avg_loss
    df["RSI"] = 100 - (100 / (1 + rs))
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
    if len(candles) < RSI_LENGTH + 5:
        return None

    df = pd.DataFrame(candles).sort_values("time").iloc[:-1]

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = calculate_rsi(df)

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
        prev = df.iloc[-2]

        if side == "SELL":
            if (
                prev["RSI"] > RSI_THRESHOLD and
                last["RSI"] < RSI_THRESHOLD  # RSI crossed downward through 40
            ):
                entry = float(last["low"])
                sl = float(prev["high"])

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

        return ("KEEP", pair)

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [executor.submit(process_pair, p) for p in watchlist]

        for f in as_completed(futures):
            res = f.result()
            if res[0] == "SIGNAL":
                # Signal fired -> coin is removed from watchlist (not re-added)
                alerts.append(res[2])
            else:
                updated_watchlist.append(res[1])

    return updated_watchlist, alerts


# ================= ADD TO WATCHLIST MODULE =================
def add_gainers_to_watchlist(top_gainers, sell_watch):
    for coin in top_gainers:
        if coin not in sell_watch:
            sell_watch.append(coin)

    return sell_watch


# ================= MAIN =================
def main():
    sell_watch = load_watchlist(SELL_FILE) if ENABLE_SELL else []

    alerts = []

    # Step 1: Scan watchlist for signals
    if ENABLE_SELL:
        sell_watch, sell_alerts = check_watchlist_for_signals(sell_watch, "SELL")
        alerts += sell_alerts

    if alerts:
        Send_Swing_Telegram_Message("\n\n".join(alerts))

    # Step 2: Fetch top 5 gainers and add to watchlist
    pairs = get_active_usdt_coins()
    top_gainers = get_top_gainers(pairs)

    if ENABLE_SELL:
        sell_watch = add_gainers_to_watchlist(top_gainers, sell_watch)
        save_watchlist(SELL_FILE, sell_watch)


if __name__ == "__main__":
    main()