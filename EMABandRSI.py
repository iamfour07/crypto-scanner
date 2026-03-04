import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from Telegram_EMA import Send_EMA_Telegram_Message

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
    print(f"\n{'═'*45}")
    print(f"  DEBUG — EMA Band({EMA_PERIOD}) + RSI({RSI_PERIOD}) | {RESOLUTION}")
    print(sep)

    for coin in CoinList:
        ind = results.get(coin)
        if not ind:
            print(f"  ⚠️  {coin:<12} — no data")
            continue

        just_crossed_up = ind["prev_close"] < ind["prev_ema_high"] and ind["close"] > ind["ema_high"]
        just_crossed_dn = ind["prev_close"] > ind["prev_ema_low"]  and ind["close"] < ind["ema_low"]

        buy_ok  = just_crossed_up and ind["rsi"] > RSI_UPPER
        sell_ok = just_crossed_dn and ind["rsi"] < RSI_LOWER

        tag = "🟢 BUY " if buy_ok else ("🔴 SELL" if sell_ok else "  ----")

        print(
            f"  {tag}  {coin:<16} "
            f"[{ind['candle_time']}]  "
            f"Close={ind['close']:<12} "
            f"EMAHigh={round(ind['ema_high'],2):<12} "
            f"EMALow={round(ind['ema_low'],2):<12} "
            f"RSI={ind['rsi']:.2f}  "
            f"CrossUp={'✅' if just_crossed_up else '❌'}  "
            f"CrossDn={'✅' if just_crossed_dn else '❌'}"
        )

        # ── BUY ──
        if buy_ok:
            buy_signals.append({
                "pair": coin, **ind,
                "entry": ind["high"],
                "sl":    ind["low"],
            })

        # ── SELL ──
        if sell_ok:
            sell_signals.append({
                "pair": coin, **ind,
                "entry": ind["low"],
                "sl":    ind["high"],
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
            f"{sep}"
        )
    print("\n".join(parts))
    Send_EMA_Telegram_Message("\n".join(parts))


# =========================
# MAIN
# =========================
def main():
    buy_signals, sell_signals = scan()
    send_alerts(buy_signals, sell_signals)


if __name__ == "__main__":
    main()
