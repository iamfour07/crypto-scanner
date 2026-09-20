import requests
import pandas as pd
import os
from datetime import datetime, timezone

# ============================================================
# TELEGRAM CONFIG
# ============================================================
try:
    from Telegram_Swing import Send_Swing_Telegram_Message
except ImportError:
    def Send_Swing_Telegram_Message(msg):
        print(f"\n--- TELEGRAM ALERT ---\n{msg}\n----------------------")


# ============================================================
# STRATEGY CONFIG
# ============================================================
PAIR = "B-BTC_USDT"

RESOLUTION = "15"
LIMIT_HOURS = 200

EMA_FAST = 9
EMA_SLOW = 30

SWING_LOOKBACK = 20

RISK_PER_TRADE = 100       # ₹100 risk per trade
LEVERAGE = 10              # 5x leverage

# Optional maximum margin you are willing to use
MAX_CAPITAL = 3000         # ₹3,000 maximum margin


# ============================================================
# FETCH CANDLES
# ============================================================
def fetch_candles(pair):

    url = "https://public.coindcx.com/market_data/candlesticks"

    now = int(datetime.now(timezone.utc).timestamp())

    params = {
        "pair": pair,
        "from": now - LIMIT_HOURS * 3600,
        "to": now,
        "resolution": RESOLUTION,
        "pcode": "f"
    }

    try:
        response = requests.get(
            url,
            params=params,
            timeout=10
        )

        response.raise_for_status()

        result = response.json()

        if (
            not isinstance(result, dict)
            or "data" not in result
            or not result["data"]
        ):
            print("❌ No candle data received")
            return None

        df = pd.DataFrame(result["data"])

        df = df.sort_values("time").reset_index(drop=True)

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        df = df.dropna(
            subset=["open", "high", "low", "close"]
        )

        # ----------------------------------------------------
        # IMPORTANT:
        # Remove current running candle.
        # Strategy must work only on CLOSED candles.
        # ----------------------------------------------------
        if len(df) > 1:
            df = df.iloc[:-1].copy()

        if len(df) < EMA_SLOW + SWING_LOOKBACK + 5:
            print("❌ Not enough candles")
            return None

        return df

    except Exception as e:

        print(f"❌ Candle fetch error: {e}")

        return None


# ============================================================
# CALCULATE EMA
# ============================================================
def calculate_ema(df):

    df = df.copy()

    df["EMA_9"] = df["close"].ewm(
        span=EMA_FAST,
        adjust=False
    ).mean()

    df["EMA_30"] = df["close"].ewm(
        span=EMA_SLOW,
        adjust=False
    ).mean()

    return df


# ============================================================
# DETECT EMA CROSS
# ============================================================
def detect_signal(df):

    last = df.iloc[-1]
    previous = df.iloc[-2]

    # --------------------------------------------------------
    # BUY
    # Previous candle EMA9 <= EMA30
    # Current candle EMA9 > EMA30
    # --------------------------------------------------------
    buy_cross = (
        previous["EMA_9"] <= previous["EMA_30"]
        and
        last["EMA_9"] > last["EMA_30"]
    )

    # --------------------------------------------------------
    # SELL
    # Previous candle EMA9 >= EMA30
    # Current candle EMA9 < EMA30
    # --------------------------------------------------------
    sell_cross = (
        previous["EMA_9"] >= previous["EMA_30"]
        and
        last["EMA_9"] < last["EMA_30"]
    )

    if buy_cross:
        return "BUY"

    if sell_cross:
        return "SELL"

    return None


# ============================================================
# FIND SWING LOW / HIGH
# ============================================================
def find_swing_levels(df):

    # Signal candle is excluded.
    # We use the PREVIOUS 10 candles.
    previous_10 = df.iloc[-11:-1]

    swing_low = previous_10["low"].min()
    swing_high = previous_10["high"].max()

    return swing_low, swing_high


# ============================================================
# CALCULATE POSITION SIZE
# ============================================================
def calculate_position(entry, sl):

    risk_distance = abs(entry - sl)

    if risk_distance <= 0:
        return None

    # --------------------------------------------------------
    # Quantity based on ₹100 maximum loss
    #
    # Quantity × price difference = ₹100
    # --------------------------------------------------------
    qty_by_risk = RISK_PER_TRADE / risk_distance

    # --------------------------------------------------------
    # Position notional
    # --------------------------------------------------------
    position_value = qty_by_risk * entry

    # --------------------------------------------------------
    # Margin required with 5x leverage
    # --------------------------------------------------------
    margin = position_value / LEVERAGE

    # --------------------------------------------------------
    # Maximum capital/margin protection
    # --------------------------------------------------------
    if margin > MAX_CAPITAL:

        qty_by_margin = (
            MAX_CAPITAL * LEVERAGE
        ) / entry

        qty = min(
            qty_by_risk,
            qty_by_margin
        )

    else:

        qty = qty_by_risk

    position_value = qty * entry

    margin = position_value / LEVERAGE

    actual_risk = qty * risk_distance

    return {
        "qty": qty,
        "position_value": position_value,
        "margin": margin,
        "risk_distance": risk_distance,
        "actual_risk": actual_risk
    }


# ============================================================
# BUILD TRADE SETUP
# ============================================================
def create_trade_setup(df, signal):

    last = df.iloc[-1]

    swing_low, swing_high = find_swing_levels(df)

    # ========================================================
    # BUY
    # ========================================================
    if signal == "BUY":

        entry = float(last["high"])
        sl = float(swing_low)

        # SL must be below entry
        if sl >= entry:
            print("⚠️ Invalid BUY setup: SL >= Entry")
            return None

        risk = entry - sl

        target_2 = entry + (risk * 2)
        target_3 = entry + (risk * 3)
        target_4 = entry + (risk * 4)

    # ========================================================
    # SELL
    # ========================================================
    elif signal == "SELL":

        entry = float(last["low"])
        sl = float(swing_high)

        # SL must be above entry
        if sl <= entry:
            print("⚠️ Invalid SELL setup: SL <= Entry")
            return None

        risk = sl - entry

        target_2 = entry - (risk * 2)
        target_3 = entry - (risk * 3)
        target_4 = entry - (risk * 4)

    else:

        return None

    # ========================================================
    # POSITION SIZE
    # ========================================================
    position = calculate_position(
        entry,
        sl
    )

    if position is None:
        return None

    sl_percentage = (
        abs(entry - sl) / entry
    ) * 100

    return {

        "signal": signal,

        "entry": entry,

        "sl": sl,

        "risk": risk,

        "sl_percentage": sl_percentage,

        "target_2": target_2,

        "target_3": target_3,

        "target_4": target_4,

        "qty": position["qty"],

        "position_value": position["position_value"],

        "margin": position["margin"],

        "actual_risk": position["actual_risk"],

        "ema_9": float(last["EMA_9"]),

        "ema_30": float(last["EMA_30"]),

        "candle_time": last["time"]
    }


# ============================================================
# TELEGRAM MESSAGE
# ============================================================
def create_telegram_message(trade):

    if trade["signal"] == "BUY":

        title = "🟢 BTC EMA CROSS — BUY"

    else:

        title = "🔴 BTC EMA CROSS — SELL"

    msg = f"""
{title}

━━━━━━━━━━━━━━━━━━
📊 Strategy
━━━━━━━━━━━━━━━━━━
Coin: BTC/USDT
Timeframe: 15 Min
EMA: 9 / 30

━━━━━━━━━━━━━━━━━━
📌 TRADE SETUP
━━━━━━━━━━━━━━━━━━
Entry: {trade["entry"]:.2f}
Stop Loss: {trade["sl"]:.2f}

SL Distance: {trade["risk"]:.2f}
SL %: {trade["sl_percentage"]:.2f}%

━━━━━━━━━━━━━━━━━━
💰 RISK MANAGEMENT
━━━━━━━━━━━━━━━━━━
Risk: ₹{RISK_PER_TRADE:.2f}
Leverage: {LEVERAGE}x

Quantity: {trade["qty"]:.8f}
Position Value: ₹{trade["position_value"]:.2f}
Required Margin: ₹{trade["margin"]:.2f}

Actual Risk: ₹{trade["actual_risk"]:.2f}

━━━━━━━━━━━━━━━━━━
🎯 TARGETS
━━━━━━━━━━━━━━━━━━
1:2 → {trade["target_2"]:.2f}
1:3 → {trade["target_3"]:.2f}
1:4 → {trade["target_4"]:.2f}

━━━━━━━━━━━━━━━━━━
📈 EMA
━━━━━━━━━━━━━━━━━━
EMA 9: {trade["ema_9"]:.2f}
EMA 30: {trade["ema_30"]:.2f}

⚠️ Signal generated only from
the last CLOSED 15-minute candle.

Entry is triggered only when
price reaches the signal candle
high/low.
"""

    return msg


# ============================================================
# MAIN
# ============================================================
def main():

    print("\n===================================")
    print("   BTC EMA 9/30 STRATEGY SCANNER")
    print("===================================\n")

    df = fetch_candles(PAIR)

    if df is None:
        return

    df = calculate_ema(df)

    signal = detect_signal(df)

    last = df.iloc[-1]

    print(f"BTC Price : {last['close']:.2f}")
    print(f"EMA 9     : {last['EMA_9']:.2f}")
    print(f"EMA 30    : {last['EMA_30']:.2f}")

    if signal is None:

        print("\n⏸️ No EMA crossover on last closed candle.")

        return

    print(f"\n🚨 SIGNAL: {signal}")

    trade = create_trade_setup(
        df,
        signal
    )

    if trade is None:

        print("❌ Invalid trade setup.")

        return

    message = create_telegram_message(
        trade
    )

    print(message)

    # ========================================================
    # SEND TELEGRAM
    # ========================================================
    Send_Swing_Telegram_Message(
        message
    )


# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    main()