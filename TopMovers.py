import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from Telegram_Momentum import Send_Momentum_Telegram_Message


TOP_N = 15
MAX_WORKERS = 20
RESOLUTION = "60"
CANDLE_HOURS = 250
BB_LENGTH = 200
BB_STD = 2.2
PRE_BREAKOUT_CANDLES = 5
RISK_AMOUNT_RS = 70
RISK_REWARD_RATIOS = (2, 3, 4)
DEFAULT_LEVERAGE = 5


def safe_get(url, params=None, timeout=10):
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Exception:
        return None


def get_active_usdt_coins():
    url = (
        "https://api.coindcx.com/exchange/v1/derivatives/futures/data/"
        "active_instruments?margin_currency_short_name[]=USDT"
    )
    data = safe_get(url, timeout=30)
    if not data:
        return []
    return [item["pair"] if isinstance(item, dict) else item for item in data]


def fetch_pair_stats(pair):
    url = f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}"
    data = safe_get(url, timeout=8)
    if not data:
        return None

    change_percent = data.get("price_change_percent", {}).get("1D")
    if change_percent is None:
        return None

    return {
        "pair": pair,
        "change": float(change_percent),
    }


def get_top_movers(pairs):
    gainers = []
    losers = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_pair_stats, pair): pair for pair in pairs}
        for future in as_completed(futures):
            result = future.result()
            if not result:
                continue

            if result["change"] > 0:
                gainers.append(result)
            elif result["change"] < 0:
                losers.append(result)

    gainers = sorted(gainers, key=lambda item: item["change"], reverse=True)[:TOP_N]
    losers = sorted(losers, key=lambda item: item["change"])[:TOP_N]
    return gainers, losers

def fetch_bb_signal(pair):
    now = int(datetime.now(timezone.utc).timestamp())
    from_time = now - CANDLE_HOURS * 3600
    current_hour_ms = (now // 3600) * 3600 * 1000

    data = safe_get(
        "https://public.coindcx.com/market_data/candlesticks",
        params={
            "pair": pair,
            "from": from_time,
            "to": now,
            "resolution": RESOLUTION,
            "pcode": "f",
        },
        timeout=10,
    )
    if not data or "data" not in data:
        return None

    df = pd.DataFrame(data["data"]).sort_values("time")
    df = df[df["time"] < current_hour_ms].reset_index(drop=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df.dropna(subset=["close", "high", "low"], inplace=True)

    if len(df) < BB_LENGTH + PRE_BREAKOUT_CANDLES + 1:
        return None

    basis = df["close"].rolling(BB_LENGTH).mean()
    deviation = df["close"].rolling(BB_LENGTH).std(ddof=0)
    df["bb_upper"] = basis + (deviation * BB_STD)

    curr = df.iloc[-1]
    prior_window = df.iloc[-(PRE_BREAKOUT_CANDLES + 1):-1].copy()
    if pd.isna(curr["bb_upper"]) or prior_window["bb_upper"].isna().any():
        return None

    prior_below_upper = (prior_window["close"] < prior_window["bb_upper"]).all()
    close_price = float(curr["close"])
    upper_band = float(curr["bb_upper"])
    if not prior_below_upper or close_price <= upper_band:
        return None

    entry = float(curr["high"])
    sl = float(curr["low"])
    risk_per_unit = entry - sl
    if risk_per_unit <= 0:
        return None

    quantity = RISK_AMOUNT_RS / risk_per_unit
    position_value = quantity * entry
    capital_required = position_value / DEFAULT_LEVERAGE
    targets = {
        f"target_{ratio}r": entry + (risk_per_unit * ratio)
        for ratio in RISK_REWARD_RATIOS
    }
    profits = {
        f"profit_{ratio}r": RISK_AMOUNT_RS * ratio
        for ratio in RISK_REWARD_RATIOS
    }

    return {
        "prior_candles_checked": PRE_BREAKOUT_CANDLES,
        "last_close": close_price,
        "bb_upper": upper_band,
        "entry": entry,
        "sl": sl,
        "risk_per_unit": risk_per_unit,
        "quantity": quantity,
        "position_value": position_value,
        "capital_required": capital_required,
        **targets,
        **profits,
    }


def attach_bb_signals(rows):
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_bb_signal, row["pair"]): row for row in rows}
        for future in as_completed(futures):
            row = futures[future]
            signal = future.result()
            if not signal:
                continue
            results.append({**row, **signal})

    return sorted(results, key=lambda item: item["change"], reverse=True)


def build_trade_message(row):
    pair = row["pair"]
    entry = row["entry"]
    sl = row["sl"]
    capital = round(row["capital_required"], 2)
    leverage = DEFAULT_LEVERAGE
    risk = row["risk_per_unit"] * row["quantity"]
    link = f"https://coindcx.com/futures/{pair}"

    return (
        f"🟢 BUY {pair}\n"
        f"1D Change: {row['change']:.2f}%\n"
        f"Entry   : {round(entry, 4)}\n"
        f"SL      : {round(sl, 4)}\n"
        f"Capital : ₹{capital} ({leverage}x)\n\n"
        f"Risk    : ₹{round(risk, 2)}\n"
        f"Targets\n"
        f"2R -> {round(row['target_2r'], 4)} | ₹{round(row['profit_2r'], 2)}\n"
        f"3R -> {round(row['target_3r'], 4)} | ₹{round(row['profit_3r'], 2)}\n"
        f"4R -> {round(row['target_4r'], 4)} | ₹{round(row['profit_4r'], 2)}\n"
        f"{link}\n"
        f"------------------------------------------------"
    )


# def print_list(title, rows):
#     print(f"\n{title}")
#     for row in rows:
#         print(build_trade_message(row))


def send_telegram_alerts(title, rows):
    if not rows:
        return

    header = f"{title}\n\n"
    message = header + "\n".join(build_trade_message(row) for row in rows)
    Send_Momentum_Telegram_Message(message)


def main():
    pairs = get_active_usdt_coins()
    if not pairs:
        print("No active USDT pairs found.")
        return

    gainers, _ = get_top_movers(pairs)
    breakout_gainers = attach_bb_signals(gainers)
    title = (f"Momentum Alert")

    # print_list(title, breakout_gainers)
    send_telegram_alerts(title, breakout_gainers)


if __name__ == "__main__":
    main()
