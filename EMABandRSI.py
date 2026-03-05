import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_Momentum import Send_Momentum_Telegram_Message

IST = timezone(timedelta(hours=5, minutes=30))

# =========================
# CONFIGURATION
# =========================
RESOLUTION   = "15"     # 15-minute candles (CoinDCX format)
LIMIT_HOURS  = 500      # History to fetch
MAX_WORKERS  = 10

# ── EMA Band ──
EMA_PERIOD = 33         # EMA33 on High  → band top
                        # EMA33 on Low   → band bottom

# ── RSI ──
RSI_PERIOD = 33
RSI_UPPER  = 55         # BUY  : RSI above this
RSI_LOWER  = 45         # SELL : RSI below this

CoinList = ['B-SOL_USDT']

# ── Risk / Reward ──
RISK_RS  = 70       # Fixed risk per trade in ₹
LEVERAGE = 5         # Default leverage to calculate required capital


# =========================
# RSI CALCULATION
# =========================
def calc_rsi(series, period=RSI_PERIOD):
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs       = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


# =========================
# RISK / REWARD CALCULATION
# =========================
def calculate_trade_levels(entry, sl, side):
    risk_per_unit = abs(entry - sl)
    if risk_per_unit == 0:
        return None

    # Position Value = (Risk / risk_per_unit) * entry
    position_value = (RISK_RS / risk_per_unit) * entry
    used_capital   = round(position_value / LEVERAGE, 2)
    
    # Targets & Profit
    if side == "BUY":
        t2 = entry + risk_per_unit * 2
        t3 = entry + risk_per_unit * 3
        t4 = entry + risk_per_unit * 4
    else:
        t2 = entry - risk_per_unit * 2
        t3 = entry - risk_per_unit * 3
        t4 = entry - risk_per_unit * 4

    # Profit in ₹ for reaching each target
    p2 = RISK_RS * 2
    p3 = RISK_RS * 3
    p4 = RISK_RS * 4

    return used_capital, t2, t3, t4, p2, p3, p4


# =========================
# FETCH CANDLES + BUILD INDICATORS
# =========================
def fetch_indicators(symbol):
    """
    Fetches 15m candles from CoinDCX (same source as your chart).
    Computes:
      ema_high = EMA33 on High  → top of EMA band
      ema_low  = EMA33 on Low   → bottom of EMA band
      rsi      = RSI(33) on Close

    curr = df.iloc[-1]  last CLOSED candle  (running candle excluded)
    prev = df.iloc[-2]  second last closed  (cross reference)
    """
    now       = int(datetime.now(timezone.utc).timestamp())
    from_time = now - LIMIT_HOURS * 3600

    # CoinDCX timestamps are in MILLISECONDS; 15m = 900s = 900_000ms
    current_candle_ms = (now // 900) * 900 * 1000

    try:
        resp = requests.get(
            "https://public.coindcx.com/market_data/candlesticks",
            params={
                "pair":       symbol,
                "from":       from_time,
                "to":         now,
                "resolution": RESOLUTION,
                "pcode":      "f",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])
    except Exception as e:
        print(f"⚠️ {symbol}: API error — {e}")
        return None

    if not data:
        print(f"⚠️ {symbol}: No candles returned")
        return None

    df = pd.DataFrame(data)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.sort_values("time").reset_index(drop=True)
    df.dropna(subset=["close", "high", "low"], inplace=True)

    # ── Filter out the currently running (open) 15m candle ──
    df = df[df["time"] < current_candle_ms].reset_index(drop=True)

    if len(df) < EMA_PERIOD + 5:
        return None

    # ── EMA Band ──
    df["ema_high"] = df["high"].ewm(span=EMA_PERIOD, adjust=False).mean()
    df["ema_low"]  = df["low"].ewm(span=EMA_PERIOD,  adjust=False).mean()

    # ── RSI ──
    df["rsi"] = calc_rsi(df["close"])

    if len(df) < 2:
        return None

    curr = df.iloc[-1]   # last CLOSED candle → signal candle
    prev = df.iloc[-2]   # second last closed → cross reference

    candle_time_ist = datetime.fromtimestamp(
        int(curr["time"]) // 1000, tz=timezone.utc
    ).astimezone(IST).strftime("%H:%M %d-%b")

    return {
        "close":      float(curr["close"]),
        "high":       float(curr["high"]),
        "low":        float(curr["low"]),
        "volume":     float(curr["volume"]),
        "ema_high":   float(curr["ema_high"]),
        "ema_low":    float(curr["ema_low"]),
        "rsi":        float(curr["rsi"]),
        "candle_time": candle_time_ist,
        # Previous candle — for cross confirmation
        "prev_close":    float(prev["close"]),
        "prev_ema_high": float(prev["ema_high"]),
        "prev_ema_low":  float(prev["ema_low"]),
        "prev_rsi":      float(prev["rsi"]),
    }


# =========================
# SCANNER
# =========================
def scan():
    """
    BUY  : close > ema_high  AND  RSI crossed above 55 (prev < 55, curr > 55)
           Entry = candle HIGH | SL = candle LOW

    SELL : close < ema_low   AND  RSI crossed below 45 (prev > 45, curr < 45)
           Entry = candle LOW | SL = candle HIGH
    """
    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_indicators, coin): coin for coin in CoinList}
        for f in as_completed(futures):
            coin = futures[f]
            try:
                results[coin] = f.result()
            except Exception:
                results[coin] = None

    buy_signals, sell_signals = [], []

    sep = "─" * 45
    # print(f"\n{'═'*45}")
    # print(f"  DEBUG — EMA Band({EMA_PERIOD}) + RSI({RSI_PERIOD}) | {RESOLUTION}")
    # print(sep)

    for coin in CoinList:
        ind = results.get(coin)
        if not ind:
            print(f"  ⚠️  {coin:<12} — no data")
            continue

        # User explicit logic:
        # Buy: Current Close > EMA High AND Prev RSI < 55 AND Curr RSI > 55
        # Sell: Current Close < EMA Low AND Prev RSI > 45 AND Curr RSI < 45
        
        price_buy_ok = ind["close"] > ind["ema_high"]
        price_sell_ok = ind["close"] < ind["ema_low"]

        rsi_cross_up = ind["prev_rsi"] < RSI_UPPER and ind["rsi"] > RSI_UPPER
        rsi_cross_dn = ind["prev_rsi"] > RSI_LOWER and ind["rsi"] < RSI_LOWER

        buy_ok  = price_buy_ok and rsi_cross_up
        sell_ok = price_sell_ok and rsi_cross_dn

        tag = "🟢 BUY " if buy_ok else ("🔴 SELL" if sell_ok else "  ---- ")

        print(
            f"  {tag} {coin:<14} "
            f"[{ind['candle_time']}]  "
            f"Close={ind['close']:<8.2f} "
            f"EMAHigh={ind['ema_high']:<8.2f} "
            f"EMALow={ind['ema_low']:<8.2f} "
            f"RSI={ind['rsi']:<6.2f} "
            f"RSICrossUp={'✅' if rsi_cross_up else '❌'}  "
            f"RSICrossDn={'✅' if rsi_cross_dn else '❌'}  "
            f"Signal={'✅' if buy_ok or sell_ok else '❌'}"
        )

        # ── BUY ──
        if buy_ok:
            entry = ind["high"]
            sl    = ind["low"]
            levels = calculate_trade_levels(entry, sl, "BUY")
            if levels:
                cap, t2, t3, t4, p2, p3, p4 = levels
                buy_signals.append({
                    "pair": coin, **ind,
                    "entry": entry, "sl": sl,
                    "cap": cap, "t2": t2, "t3": t3, "t4": t4,
                    "p2": p2, "p3": p3, "p4": p4
                })

        # ── SELL ──
        if sell_ok:
            entry = ind["low"]
            sl    = ind["high"]
            levels = calculate_trade_levels(entry, sl, "SELL")
            if levels:
                cap, t2, t3, t4, p2, p3, p4 = levels
                sell_signals.append({
                    "pair": coin, **ind,
                    "entry": entry, "sl": sl,
                    "cap": cap, "t2": t2, "t3": t3, "t4": t4,
                    "p2": p2, "p3": p3, "p4": p4
                })

    print(sep)

    # Sort by volume descending
    buy_signals.sort(key=lambda x: x["volume"], reverse=True)
    sell_signals.sort(key=lambda x: x["volume"], reverse=True)
    return buy_signals, sell_signals


# =========================
# TELEGRAM ALERT
# =========================
def send_alerts(buy_signals, sell_signals):
    if not buy_signals and not sell_signals:
        return

    sep  = "─" * 35
    sep2 = "═" * 35
    parts = [f"📊 EMA Band({EMA_PERIOD}) + RSI({RSI_PERIOD}) | 15m\n{sep2}"]

    for s in buy_signals:
        parts.append(
            f"🟢 BUY  {s['pair']}\n"
            f"Close    : {s['close']}\n"
            f"EMA High : {round(s['ema_high'], 6)}\n"
            f"Entry    : {s['entry']}\n"
            f"SL       : {s['sl']}\n"
            f"RSI      : {s['rsi']:.1f} (above {RSI_UPPER})\n"
            f"Capital  : ₹{s['cap']} ({LEVERAGE}× leverage)\n"
            f"Risk     : ₹{RISK_RS}\n\n"
            f"🎯 Targets & Profit:\n"
            f"2R → {round(s['t2'], 4)}  (Profit: ₹{s['p2']})\n"
            f"3R → {round(s['t3'], 4)}  (Profit: ₹{s['p3']})\n"
            f"4R → {round(s['t4'], 4)}  (Profit: ₹{s['p4']})\n"
            f"{sep}"
        )

    for s in sell_signals:
        parts.append(
            f"🔴 SELL {s['pair']}\n"
            f"Close   : {s['close']}\n"
            f"EMA Low : {round(s['ema_low'], 6)}\n"
            f"Entry   : {s['entry']}\n"
            f"SL      : {s['sl']}\n"
            f"RSI     : {s['rsi']:.1f} (below {RSI_LOWER})\n"
            f"Capital : ₹{s['cap']} ({LEVERAGE}× leverage)\n"
            f"Risk    : ₹{RISK_RS}\n\n"
            f"🎯 Targets & Profit:\n"
            f"2R → {round(s['t2'], 4)}  (Profit: ₹{s['p2']})\n"
            f"3R → {round(s['t3'], 4)}  (Profit: ₹{s['p3']})\n"
            f"4R → {round(s['t4'], 4)}  (Profit: ₹{s['p4']})\n"
            f"{sep}"
        )
    print("\n".join(parts))
    Send_Momentum_Telegram_Message("\n".join(parts))


# =========================
# MAIN
# =========================
def main():
    buy_signals, sell_signals = scan()
    send_alerts(buy_signals, sell_signals)


if __name__ == "__main__":
    main()
