import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from Telegram_Alert_EMA_Crossover import Telegram_Alert_EMA_Crossover

# =====================
# CONFIG
# =====================
MAX_WORKERS = 15
resolution = "60"  # 1-hour candles
limit_hours = 1000  # Enough history for prev day calculation

RISK_PER_TRADE = 200  # ₹500 fixed risk per trade

# ---------------------
def get_active_usdt_coins():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

def fetch_pair_stats(pair):
    try:
        url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        pc = data.get("price_change_percent", {}).get("1D")
        if pc is None:
            return None
        return {"pair": pair, "change": float(pc)}
    except Exception as e:
        # keep debug info
        print(f"[stats] {pair} error: {e}")
        return None

def fetch_candles(pair):
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        from_time = now - limit_hours * 3600
        url = "https://public.coindcx.com/market_data/candlesticks"
        params = {"pair": pair, "from": from_time, "to": now, "resolution": resolution, "pcode": "f"}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data or len(data) < 10:
            return None
        df = pd.DataFrame(data)
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        print(f"[candles] {pair} error: {e}")
        return None

def get_prev_day_hl(df):
    utc_now = datetime.now(timezone.utc)
    prev_day = (utc_now - timedelta(days=1)).date()
    prev_rows = df[df["time"].dt.date == prev_day]
    if prev_rows.empty:
        return None, None
    return prev_rows["high"].max(), prev_rows["low"].min()


# ---------------------
# Gainer: fresh breakout candle (-2 closed > PD High) + pullback candle (-1 is red)
# Use pullback candle close as ENTRY and pullback low as SL for RR/qty
def check_gainer(pair):
    df = fetch_candles(pair)
    if df is None or len(df) < 6:
        return None

    pd_high, _ = get_prev_day_hl(df)
    if pd_high is None:
        return None

    prev3 = df.iloc[-3]
    prev2 = df.iloc[-2]  # breakout candle (-2)
    prev1 = df.iloc[-1]  # pullback candle (-1)

    breakout = prev2["close"] > pd_high
    pullback_is_red = prev1["close"] < prev1["open"]
    no_previous_break = not (prev3["close"] > pd_high)

    if not (breakout and pullback_is_red and no_previous_break):
        return None

    entry = float(prev1["close"])
    sl = float(prev1["low"])
    risk_per_unit = entry - sl
    if risk_per_unit <= 0:
        return None

    qty = RISK_PER_TRADE / risk_per_unit
    # targets: 1:2 .. 1:5
    targets = [entry + risk_per_unit * r for r in (2, 3, 4, 5)]

    # Format message exactly as you requested
    msg = (
        f"{pair}-\n"
        f"Entry Price-{entry:.4f}\n"
        f"Stop Loss-{sl:.4f}\n"
        f"Qty-{qty:.4f}\n\n"
        f"Risk Per trade-₹{RISK_PER_TRADE}\n"
        f"----------------\n"
        f"Risk Reward\n"
        f"1:2- {targets[0]:.4f}\n"
        f"1:3- {targets[1]:.4f}\n"
        f"1:4- {targets[2]:.4f}\n"
        f"1:5- {targets[3]:.4f}\n"
    )
    return msg


# ---------------------
# Loser: fresh breakdown candle (-2 closed < PD Low) + pullback candle (-1 is green)
# Use pullback candle close as ENTRY and pullback high as SL for RR/qty
def check_loser(pair):
    df = fetch_candles(pair)
    if df is None or len(df) < 6:
        return None

    _, pd_low = get_prev_day_hl(df)
    if pd_low is None:
        return None

    prev3 = df.iloc[-3]
    prev2 = df.iloc[-2]  # breakdown candle (-2)
    prev1 = df.iloc[-1]  # pullback candle (-1)

    breakdown = prev2["close"] < pd_low
    pullback_is_green = prev1["close"] > prev1["open"]
    no_previous_break = not (prev3["close"] < pd_low)

    if not (breakdown and pullback_is_green and no_previous_break):
        return None

    entry = float(prev1["close"])
    sl = float(prev1["high"])
    risk_per_unit = sl - entry
    if risk_per_unit <= 0:
        return None

    qty = RISK_PER_TRADE / risk_per_unit
    # targets: 1:2 .. 1:5 (downside targets)
    targets = [entry - risk_per_unit * r for r in (2, 3, 4, 5)]

    msg = (
        f"{pair}-\n"
        f"Entry Price-{entry:.4f}\n"
        f"Stop Loss-{sl:.4f}\n"
        f"Qty-{qty:.4f}\n\n"
        f"Risk Per trade-₹{RISK_PER_TRADE}\n"
        f"----------------\n"
        f"Risk Reward\n"
        f"1:2- {targets[0]:.4f}\n"
        f"1:3- {targets[1]:.4f}\n"
        f"1:4- {targets[2]:.4f}\n"
        f"1:5- {targets[3]:.4f}\n"
       
    )
    return msg


# -----------------------------------------------------
def main():
    print("\n📊 Fetching active USDT futures...")
    pairs = get_active_usdt_coins()

    changes = []
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        for fut in as_completed([executor.submit(fetch_pair_stats, p) for p in pairs]):
            res = fut.result()
            if res:
                changes.append(res)

    df = pd.DataFrame(changes).dropna()
    if df.empty:
        print("⚠ No stats data received!")
        return

    top_gainers = df.sort_values("change", ascending=False).head(15)["pair"].tolist()
    top_losers = df.sort_values("change", ascending=True).head(15)["pair"].tolist()

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        gainers = [f.result() for f in as_completed([executor.submit(check_gainer, p) for p in top_gainers]) if f.result()]
        losers = [f.result() for f in as_completed([executor.submit(check_loser, p) for p in top_losers]) if f.result()]

    # Console print
    if gainers:
        print("\n✅ Bullish Pullback Setups:")
        print(*gainers, sep="\n\n")
    else:
        print("\n✅ Bullish Pullback Setups: None")

    if losers:
        print("\n✅ Bearish Pullback Setups:")
        print(*losers, sep="\n\n")
    else:
        print("\n✅ Bearish Pullback Setups: None")

    # Telegram: send only when any setup exists
    alerts = []
    if gainers:
        alerts.append("📈 Bullish Pullback Setups:\n\n" + "\n\n".join(gainers))
    if losers:
        alerts.append("📉 Bearish Pullback Setups:\n\n" + "\n\n".join(losers))

    if alerts:
        full_msg = "\n\n".join(alerts)
        print("\n📨 Sending Telegram alert...")
        Telegram_Alert_EMA_Crossover(full_msg)
    else:
        print("\n⏳ No setups found — no Telegram alert sent.")


if __name__ == "__main__":
    main()
