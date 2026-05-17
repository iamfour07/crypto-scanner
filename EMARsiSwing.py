import json, requests, pandas as pd, os
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from Telegram_Swing import Send_Swing_Telegram_Message
except ImportError:
    def Send_Swing_Telegram_Message(msg):
        print(f"\n--- TELEGRAM ALERT ---\n{msg}\n----------------------")

# ================================================================
#  CONFIG
# ================================================================
RESOLUTION         = "60"
BUY_FILE           = "BuyWatchlist.json"
SELL_FILE          = "SellWatchlist.json"
MAX_WORKERS        = 20
USE_VOLUME_FILTER  = False
MIN_VOLUME_USDT    = 10_000_000
MAX_PRICE_EMA20_GAP = 3    # % mein — cross ke waqt price 20 EMA se kitni door ho sakti hai
LEVERAGE           = 7
INR_TO_USDT_RATE   = None

# ================================================================
#  INDICATORS
# ================================================================
def calculate_indicators(df):
    df['ema20']  = df['close'].ewm(span=20,  adjust=False).mean()
    df['ema50']  = df['close'].ewm(span=50,  adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    delta        = df['close'].diff()
    gain         = delta.where(delta > 0, 0)
    loss         = -delta.where(delta < 0, 0)
    avg_gain     = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    avg_loss     = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    rs           = avg_gain / (avg_loss + 1e-9)
    df['rsi']    = 100 - (100 / (1 + rs))
    return df


def fetch_data(pair):
    url    = "https://public.coindcx.com/market_data/candlesticks"
    now    = int(datetime.now(timezone.utc).timestamp())
    params = {"pair": pair, "from": now - 500*3600, "to": now,
              "resolution": RESOLUTION, "pcode": "f"}
    try:
        r  = requests.get(url, params=params, timeout=10).json()
        df = pd.DataFrame(r["data"]).sort_values("time").iloc[:-1]  # running candle remove
        for col in ["open","high","low","close"]:
            df[col] = pd.to_numeric(df[col])
        return calculate_indicators(df).dropna()
    except:
        return None


# ================================================================
#  VOLUME
# ================================================================
def get_volume(pair):
    if not USE_VOLUME_FILTER:
        return float('inf')
    try:
        d = requests.get(
            f"https://api.coindcx.com/api/v1/derivatives/futures/data/stats?pair={pair}",
            timeout=5).json()
        return float(d.get("volume_24h", 0))
    except:
        return 0


# ================================================================
#  INR RATE
# ================================================================
def get_inr_rate():
    if INR_TO_USDT_RATE is not None:
        return INR_TO_USDT_RATE
    try:
        r = requests.get("https://api.coindcx.com/exchange/v1/markets_details", timeout=5).json()
        for m in r:
            if m.get("symbol") == "USDTINR":
                return float(m.get("last_price", 84.0))
    except:
        pass
    return 84.0


# ================================================================
#  POSITION SIZE
# ================================================================
def calc_position(entry, sl):
    sl_pct = abs(entry - sl) / entry * 100
    if sl_pct == 0:
        return None
    rate          = get_inr_rate()
    risk_usdt     = RISK_PER_TRADE_INR / rate
    position_usdt = round(risk_usdt / (sl_pct / 100), 2)
    capital_usdt  = round(position_usdt / LEVERAGE, 2)
    capital_inr   = round(capital_usdt * rate, 2)
    quantity      = round(position_usdt / entry, 4)
    return {"capital_inr": capital_inr, "capital_usdt": capital_usdt, "quantity": quantity}


# ================================================================
#  TRADE LEVELS
#  Buy:  Entry = last closed candle HIGH | SL = previous swing LOW
#  Sell: Entry = last closed candle LOW  | SL = previous swing HIGH
#
#  Swing LOW  = last 10 candles ka lowest LOW
#  Swing HIGH = last 10 candles ka highest HIGH
# ================================================================
SWING_CANDLES = 10   # Kitni candles mein swing dhundna hai

def trade_levels(df, side):
    last   = df.iloc[-1]             # last closed candle — entry ke liye
    swing  = df.iloc[-SWING_CANDLES:]  # last 10 candles — SL ke liye

    if side == "buy":
        entry = round(last['high'], 6)
        sl    = round(swing['low'].min(), 6)   # lowest LOW of last 10 candles
        risk  = entry - sl
        t2    = round(entry + 2 * risk, 6)
        t3    = round(entry + 3 * risk, 6)
    else:
        entry = round(last['low'],  6)
        sl    = round(swing['high'].max(), 6)  # highest HIGH of last 10 candles
        risk  = sl - entry
        t2    = round(entry - 2 * risk, 6)
        t3    = round(entry - 3 * risk, 6)

    return entry, sl, t2, t3


# ================================================================
#  ALERT MESSAGE
# ================================================================
def build_msg(side, pair, entry, sl, t2, t3, gap=None):
    pos   = calc_position(entry, sl)
    cap   = f"Rs.{pos['capital_inr']} (~${pos['capital_usdt']} USDT)" if pos else "N/A"
    qty   = pos['quantity'] if pos else "N/A"
    label = "BUY" if side == "buy" else "SELL"
    gap_line = f"\nEMA20 Gap : {round(gap, 2)}%" if gap is not None else ""
    return (
        f"{label} — {pair}\n\n"
        f"Entry    : {entry}\n"
        f"SL       : {sl}\n"
        f"Capital  : {cap}\n"
        f"Quantity : {qty}\n"
        f"{gap_line}\n"
        f"Targets:\n"
        f"1:2 → {t2}\n"
        f"1:3 → {t3}"
    )


# ================================================================
#  FRESH EMA CROSS CHECK
#
#  Sirf 2 candles compare:
#    prev = df.iloc[-2]  (iske pehle wali closed candle)
#    last = df.iloc[-1]  (last closed candle)
#
#  BUY cross:  prev mein 50 <= 200  AND  last mein 50 > 200
#  SELL cross: prev mein 50 >= 200  AND  last mein 50 < 200
# ================================================================
def scan_fresh_cross(pair, buy_pairs, sell_pairs):
    if pair in buy_pairs or pair in sell_pairs:
        return None

    if get_volume(pair) < MIN_VOLUME_USDT:
        return None

    df = fetch_data(pair)
    if df is None or len(df) < 2:
        return None

    last = df.iloc[-1]   # last closed candle
    prev = df.iloc[-2]   # uske pehle wali candle

    # Price aur 20 EMA ka gap check — price 20 EMA ke paas honi chahiye
    price_ema20_gap = abs(last['close'] - last['ema20']) / last['ema20'] * 100
    if price_ema20_gap > MAX_PRICE_EMA20_GAP:
        return None   # Price bahut door hai 20 EMA se — skip

    # BUY: pehle 50 neeche tha, ab 50 upar aa gaya
    if prev['ema50'] <= prev['ema200'] and last['ema50'] > last['ema200']:
        print(f"  ✅ BUY cross on last closed candle: {pair}")
        return ("ADD_BUY", pair)

    # SELL: pehle 50 upar tha, ab 50 neeche aa gaya
    if prev['ema50'] >= prev['ema200'] and last['ema50'] < last['ema200']:
        print(f"  🔴 SELL cross on last closed candle: {pair}")
        return ("ADD_SELL", pair)

    return None


# ================================================================
#  STATE MACHINE
#
#  BUY  flow: waiting_dip  → (RSI < 45) → dip_done  → (RSI > 55) → ALERT
#  SELL flow: waiting_bounce → (RSI > 55) → bounce_done → (RSI < 45) → ALERT
#
#  Sab kuch last closed candle ki RSI pe check hota hai
# ================================================================
def check_state(entry, buy_pairs, sell_pairs):
    pair  = entry["pair"]
    state = entry["state"]

    df = fetch_data(pair)
    if df is None or df.empty:
        return ("STAY", entry)

    last = df.iloc[-1]   # last closed candle
    rsi  = last['rsi']

    # ── BUY SIDE ──
    if state in ("waiting_dip", "dip_done"):

        # Trend break → remove
        if last['ema50'] < last['ema200']:
            print(f"  ❌ BUY Remove (trend break): {pair}")
            return ("REMOVE_BUY", entry)

        # Volume drop → remove
        if get_volume(pair) < MIN_VOLUME_USDT:
            print(f"  ❌ BUY Remove (low volume): {pair}")
            return ("REMOVE_BUY", entry)

        # waiting_dip: RSI < 45 aaya → dip_done
        if state == "waiting_dip" and rsi < 45:
            print(f"  🔄 BUY dip_done: {pair} | RSI: {round(rsi,1)}")
            return ("UPDATE", {**entry, "state": "dip_done"})

        # dip_done: RSI > 55 aaya → ALERT
        if state == "dip_done" and rsi > 55:
            e, sl, t2, t3 = trade_levels(df, "buy")
            gap = abs(last['close'] - last['ema20']) / last['ema20'] * 100
            return ("ALERT_BUY", entry, build_msg("buy", pair, e, sl, t2, t3, gap))

    # ── SELL SIDE ──
    elif state in ("waiting_bounce", "bounce_done"):

        # Trend break → remove
        if last['ema50'] > last['ema200']:
            print(f"  ❌ SELL Remove (trend break): {pair}")
            return ("REMOVE_SELL", entry)

        # Volume drop → remove
        if get_volume(pair) < MIN_VOLUME_USDT:
            print(f"  ❌ SELL Remove (low volume): {pair}")
            return ("REMOVE_SELL", entry)

        # waiting_bounce: RSI > 55 aaya → bounce_done
        if state == "waiting_bounce" and rsi > 55:
            print(f"  🔄 SELL bounce_done: {pair} | RSI: {round(rsi,1)}")
            return ("UPDATE", {**entry, "state": "bounce_done"})

        # bounce_done: RSI < 45 aaya → ALERT
        if state == "bounce_done" and rsi < 45:
            e, sl, t2, t3 = trade_levels(df, "sell")
            gap = abs(last['close'] - last['ema20']) / last['ema20'] * 100
            return ("ALERT_SELL", entry, build_msg("sell", pair, e, sl, t2, t3, gap))

    return ("STAY", entry)


# ================================================================
#  MAIN
# ================================================================
def main():
    buy_list  = json.load(open(BUY_FILE))  if os.path.exists(BUY_FILE)  else []
    sell_list = json.load(open(SELL_FILE)) if os.path.exists(SELL_FILE) else []

    buy_pairs  = {e["pair"] for e in buy_list}
    sell_pairs = {e["pair"] for e in sell_list}

    updated_buy  = list(buy_list)
    updated_sell = list(sell_list)
    alerts       = []

    # ── 1. Existing coins ka state check ──
    all_watched = buy_list + sell_list
    if all_watched:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(check_state, e, buy_pairs, sell_pairs)
                       for e in all_watched]
            for f in as_completed(futures):
                res = f.result()
                if not res: continue
                code = res[0]

                if code == "ALERT_BUY":
                    _, entry, msg = res
                    alerts.append(msg)
                    updated_buy = [e for e in updated_buy if e["pair"] != entry["pair"]]
                    print(f"  🚀 ALERT BUY: {entry['pair']}")

                elif code == "ALERT_SELL":
                    _, entry, msg = res
                    alerts.append(msg)
                    updated_sell = [e for e in updated_sell if e["pair"] != entry["pair"]]
                    print(f"  📉 ALERT SELL: {entry['pair']}")

                elif code == "UPDATE":
                    _, upd = res
                    pair = upd["pair"]
                    if pair in buy_pairs:
                        updated_buy  = [upd if e["pair"] == pair else e for e in updated_buy]
                    else:
                        updated_sell = [upd if e["pair"] == pair else e for e in updated_sell]

                elif code == "REMOVE_BUY":
                    _, entry = res
                    updated_buy = [e for e in updated_buy if e["pair"] != entry["pair"]]

                elif code == "REMOVE_SELL":
                    _, entry = res
                    updated_sell = [e for e in updated_sell if e["pair"] != entry["pair"]]

    if alerts:
        Send_Swing_Telegram_Message("\n\n---\n\n".join(alerts))

    # ── 2. Fresh cross scan — saare pairs ──
    try:
        raw = requests.get(
            "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments"
            "?margin_currency_short_name[]=USDT",
            timeout=10
        ).json()
        all_pairs = [p for p in raw if isinstance(p, str)]
    except Exception as e:
        print(f"Error fetching pairs: {e}")
        all_pairs = []

    buy_pairs_now  = {e["pair"] for e in updated_buy}
    sell_pairs_now = {e["pair"] for e in updated_sell}

    print(f"\nScanning {len(all_pairs)} pairs for fresh EMA cross on last closed candle...")
    new_buy = new_sell = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(scan_fresh_cross, p, buy_pairs_now, sell_pairs_now)
                   for p in all_pairs]
        for f in as_completed(futures):
            res = f.result()
            if not res: continue
            code, pair = res
            now_str = datetime.now().isoformat(timespec="seconds")

            if code == "ADD_BUY" and pair not in buy_pairs_now:
                updated_buy.append({"pair": pair, "state": "waiting_dip", "added": now_str})
                buy_pairs_now.add(pair)
                new_buy += 1

            elif code == "ADD_SELL" and pair not in sell_pairs_now:
                updated_sell.append({"pair": pair, "state": "waiting_bounce", "added": now_str})
                sell_pairs_now.add(pair)
                new_sell += 1

    with open(BUY_FILE,  "w") as f: json.dump(updated_buy,  f, indent=2)
    with open(SELL_FILE, "w") as f: json.dump(updated_sell, f, indent=2)

    print(f"\nDone. New → Buy: {new_buy} | Sell: {new_sell}")
    print(f"Watchlist → Buy: {len(updated_buy)} | Sell: {len(updated_sell)}")


if __name__ == "__main__":
    main()
