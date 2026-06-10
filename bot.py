import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

# ─── CONFIG ───────────────────────────────────────────────
ALPACA_API_KEY   = "PKOVEOWJ7IHAU4ZRT2W4RN6FCO"
ALPACA_SECRET    = "63kexZGqCue9eMii6Py6S13AwZks7y3mwTDi1TcmhKhK"
TELEGRAM_TOKEN   = "8855798705:AAFhs2RYnLUVxR-N2C2urTzl445NZn2fxv8"
TELEGRAM_CHAT_ID = "6903579390"

TICKERS            = ["RGTI", "RXT", "QUBT", "LUNR"]
SCAN_INTERVAL      = 60       # scan every 60 seconds
SIGNAL_COOLDOWN    = 900      # no duplicate signals for 15 min per ticker
MIN_SCORE          = 4        # minimum score to send signal
PRE_MARKET_HOUR    = 8        # start pre-market scan at 8 AM NY
MARKET_OPEN_HOUR   = 9
MARKET_OPEN_MIN    = 30
MARKET_CLOSE_HOUR  = 16

ALPACA_BASE    = "https://data.alpaca.markets/v2"
ALPACA_HEADERS = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET
}

# ─── STATE ────────────────────────────────────────────────
last_signal_time = {}   # ticker -> datetime of last signal
last_signal_type = {}   # ticker -> last signal BUY/SELL

# ─── MARKET HOURS ─────────────────────────────────────────
def get_ny_time():
    utc_now = datetime.now(timezone.utc)
    ny_offset = timedelta(hours=-4)  # EDT
    return utc_now + ny_offset

def market_status():
    ny = get_ny_time()
    if ny.weekday() >= 5:  # weekend
        return "CLOSED"
    hour, minute = ny.hour, ny.minute
    if hour == MARKET_OPEN_HOUR and minute < MARKET_OPEN_MIN:
        return "CLOSED"
    if hour < PRE_MARKET_HOUR:
        return "CLOSED"
    if hour < MARKET_OPEN_HOUR or (hour == MARKET_OPEN_HOUR and minute < MARKET_OPEN_MIN):
        return "PRE_MARKET"
    if hour >= MARKET_CLOSE_HOUR:
        return "CLOSED"
    return "OPEN"

def is_duplicate_signal(ticker, signal):
    if ticker not in last_signal_time:
        return False
    elapsed = (datetime.now() - last_signal_time[ticker]).total_seconds()
    if elapsed < SIGNAL_COOLDOWN and last_signal_type.get(ticker) == signal:
        return True
    return False

# ─── TELEGRAM ─────────────────────────────────────────────
async def send_telegram(session, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        async with session.post(url, json=payload) as resp:
            r = await resp.json()
            if not r.get("ok"):
                print(f"Telegram error: {r}")
    except Exception as e:
        print(f"Telegram error: {e}")

# ─── YAHOO PRICE ──────────────────────────────────────────
async def get_yahoo_price(session, ticker):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            data = await resp.json()
            meta       = data["chart"]["result"][0]["meta"]
            price      = meta["regularMarketPrice"]
            prev_close = meta["chartPreviousClose"]
            change_pct = ((price - prev_close) / prev_close) * 100
            return round(price, 4), round(change_pct, 2)
    except Exception as e:
        print(f"Yahoo error {ticker}: {e}")
        return None, None

# ─── ALPACA BARS (multi-timeframe) ────────────────────────
async def get_bars(session, ticker, timeframe="5Min", limit=50):
    url = f"{ALPACA_BASE}/stocks/{ticker}/bars"
    params = {"timeframe": timeframe, "limit": limit, "feed": "iex"}
    try:
        async with session.get(url, headers=ALPACA_HEADERS, params=params) as resp:
            data = await resp.json()
            return data.get("bars", [])
    except Exception as e:
        print(f"Bars error {ticker} {timeframe}: {e}")
        return []

# ─── INDICATORS ───────────────────────────────────────────
def calc_vwap(bars):
    cum_tp_vol = cum_vol = 0
    vals = []
    for b in bars:
        tp = (b['h'] + b['l'] + b['c']) / 3
        cum_tp_vol += tp * b['v']
        cum_vol += b['v']
        vals.append(round(cum_tp_vol / cum_vol if cum_vol > 0 else 0, 4))
    return vals

def calc_rsi(closes, period=14):
    if len(closes) < period + 1: return None
    gains  = [max(closes[i]-closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1]-closes[i], 0) for i in range(1, len(closes))]
    ag = sum(gains[-period:]) / period
    al = sum(losses[-period:]) / period
    if al == 0: return 100
    return round(100 - (100 / (1 + ag/al)), 2)

def calc_ema(values, period):
    if len(values) < period: return None
    k, ema = 2/(period+1), sum(values[:period])/period
    for v in values[period:]:
        ema = v*k + ema*(1-k)
    return round(ema, 4)

def calc_atr(bars, period=14):
    if len(bars) < period+1: return None
    trs = []
    for i in range(1, len(bars)):
        tr = max(
            bars[i]['h'] - bars[i]['l'],
            abs(bars[i]['h'] - bars[i-1]['c']),
            abs(bars[i]['l'] - bars[i-1]['c'])
        )
        trs.append(tr)
    return round(sum(trs[-period:]) / period, 4)

def calc_volume_ratio(bars):
    if len(bars) < 10: return None
    vols = [b['v'] for b in bars]
    avg  = sum(vols[:-1]) / len(vols[:-1])
    return round(vols[-1] / avg if avg > 0 else 0, 2)

def get_sr(bars, lookback=20):
    recent = bars[-lookback:]
    return (round(min(b['l'] for b in recent), 4),
            round(max(b['h'] for b in recent), 4))

# ─── MULTI-TIMEFRAME SIGNAL ───────────────────────────────
def compute_signal(bars_1m, bars_5m, bars_15m):
    # Need enough bars
    if len(bars_5m) < 20: return None

    closes_5m = [b['c'] for b in bars_5m]
    price     = closes_5m[-1]

    # 5m indicators (primary)
    vwap_5m   = calc_vwap(bars_5m)
    vwap_now  = vwap_5m[-1]
    vwap_prev = vwap_5m[-2]
    rsi       = calc_rsi(closes_5m)
    ema9      = calc_ema(closes_5m, 9)
    ema21     = calc_ema(closes_5m, 21)
    atr       = calc_atr(bars_5m)
    vol_ratio = calc_volume_ratio(bars_5m)
    support, resistance = get_sr(bars_5m)

    # 15m trend confirmation
    trend_15m = "NEUTRAL"
    if len(bars_15m) >= 21:
        closes_15m = [b['c'] for b in bars_15m]
        ema9_15m   = calc_ema(closes_15m, 9)
        ema21_15m  = calc_ema(closes_15m, 21)
        if ema9_15m and ema21_15m:
            trend_15m = "BULL" if ema9_15m > ema21_15m else "BEAR"

    # 1m momentum
    momentum_1m = "NEUTRAL"
    if len(bars_1m) >= 5:
        closes_1m = [b['c'] for b in bars_1m[-5:]]
        momentum_1m = "UP" if closes_1m[-1] > closes_1m[0] else "DOWN"

    # ── Scoring ──
    buy_score = sell_score = 0

    # VWAP cross (3pts)
    crossed_above = price > vwap_now and closes_5m[-2] <= vwap_prev
    crossed_below = price < vwap_now and closes_5m[-2] >= vwap_prev
    if crossed_above:      buy_score  += 3
    elif price > vwap_now: buy_score  += 1
    if crossed_below:      sell_score += 3
    elif price < vwap_now: sell_score += 1

    # Volume (2pts)
    if vol_ratio and vol_ratio >= 1.5:
        if price > vwap_now: buy_score  += 2
        else:                sell_score += 2
    elif vol_ratio and vol_ratio >= 1.2:
        if price > vwap_now: buy_score  += 1
        else:                sell_score += 1

    # RSI (2pts)
    if rsi:
        if rsi < 35:                             buy_score  += 2
        elif rsi < 50 and price > vwap_now:      buy_score  += 1
        if rsi > 65:                             sell_score += 2
        elif rsi > 50 and price < vwap_now:      sell_score += 1

    # EMA 9/21 (1pt)
    if ema9 and ema21:
        if ema9 > ema21: buy_score  += 1
        else:            sell_score += 1

    # 15m trend confirmation (2pts)
    if trend_15m == "BULL": buy_score  += 2
    if trend_15m == "BEAR": sell_score += 2

    # 1m momentum (1pt)
    if momentum_1m == "UP":   buy_score  += 1
    if momentum_1m == "DOWN": sell_score += 1

    # ── Decision ──
    if buy_score >= MIN_SCORE:
        signal     = "BUY"
        confidence = "HIGH" if buy_score >= 7 else "MEDIUM"
    elif sell_score >= MIN_SCORE:
        signal     = "SELL"
        confidence = "HIGH" if sell_score >= 7 else "MEDIUM"
    else:
        return None

    # ATR-based SL/TP (smarter levels)
    atr_mult = 1.5
    if atr:
        if signal == "BUY":
            sl = round(price - atr * atr_mult, 4)
            tp = round(price + atr * atr_mult * 2, 4)
        else:
            sl = round(price + atr * atr_mult, 4)
            tp = round(price - atr * atr_mult * 2, 4)
    else:
        if signal == "BUY":
            sl = round(min(vwap_now, support)   * 0.995, 4)
            tp = round(resistance * 0.998, 4)
        else:
            sl = round(max(vwap_now, resistance) * 1.005, 4)
            tp = round(support * 1.002, 4)

    risk   = abs(price - sl)
    reward = abs(tp - price)
    rr     = f"1:{round(reward/risk,1)}" if risk > 0 else "N/A"

    ema_bull = ema9 > ema21 if ema9 and ema21 else False
    trend    = "UPTREND" if ema_bull else "DOWNTREND"

    return {
        "signal": signal, "confidence": confidence,
        "entry": round(price, 4),
        "sl": sl, "tp": tp, "rr": rr,
        "vwap": vwap_now, "rsi": rsi,
        "ema9": ema9, "ema21": ema21,
        "atr": atr, "vol_ratio": vol_ratio,
        "trend": trend, "trend_15m": trend_15m,
        "momentum_1m": momentum_1m,
        "buy_score": buy_score, "sell_score": sell_score,
        "support": support, "resistance": resistance,
    }

# ─── FORMAT MESSAGE ───────────────────────────────────────
def format_message(ticker, result, yahoo_price, yahoo_change, status):
    signal = result["signal"]
    now    = get_ny_time().strftime("%H:%M:%S") + " NY"
    header = f"🟢 <b>BUY — {ticker}</b>" if signal == "BUY" else f"🔴 <b>SELL — {ticker}</b>"
    conf_e = "🔥" if result["confidence"] == "HIGH" else "⚡"
    t15_e  = "📈" if result["trend_15m"] == "BULL" else "📉" if result["trend_15m"] == "BEAR" else "➡️"
    m1_e   = "⬆️" if result["momentum_1m"] == "UP" else "⬇️"
    status_label = "🌅 PRE-MARKET" if status == "PRE_MARKET" else "🟢 MARKET OPEN"

    if yahoo_price:
        ch_e  = "📈" if yahoo_change and yahoo_change >= 0 else "📉"
        ch_str = f"+{yahoo_change}%" if yahoo_change and yahoo_change >= 0 else f"{yahoo_change}%"
        y_line = f"📱 <b>Live Price:</b>  <b>${yahoo_price}</b>  {ch_e} {ch_str} today"
    else:
        y_line = f"📱 <b>Signal Price:</b>  ${result['entry']}"

    score = result['buy_score'] if signal == "BUY" else result['sell_score']

    msg = f"""{header}  {conf_e} {result['confidence']}
{status_label}

{y_line}
━━━━━━━━━━━━━━━━━━━━━
🎯 <b>Entry:</b>  ${result['entry']}
🛑 <b>Stop Loss:</b>  ${result['sl']}
✅ <b>Take Profit:</b>  ${result['tp']}
⚖️ <b>Risk/Reward:</b>  {result['rr']}
📐 <b>ATR:</b>  ${result['atr']}
━━━━━━━━━━━━━━━━━━━━━
📊 <b>VWAP:</b>  ${result['vwap']}
📉 <b>RSI:</b>  {result['rsi']}
📈 <b>EMA 9/21:</b>  {result['ema9']} / {result['ema21']}
🔊 <b>Volume:</b>  {result['vol_ratio']}x avg
{t15_e} <b>15m Trend:</b>  {result['trend_15m']}
{m1_e} <b>1m Momentum:</b>  {result['momentum_1m']}
🏆 <b>Signal Score:</b>  {score}/11
━━━━━━━━━━━━━━━━━━━━━
🏷 S: ${result['support']}  R: ${result['resistance']}
🕐 {now}"""
    return msg

# ─── MAIN LOOP ────────────────────────────────────────────
async def main():
    print("🚀 AlphaSignal Ultimate Bot starting...")
    async with aiohttp.ClientSession() as session:
        await send_telegram(session,
            "🤖 <b>AlphaSignal Ultimate Bot Started!</b>\n\n"
            f"📊 Watching: {', '.join(TICKERS)}\n"
            "📐 Strategy: VWAP + Volume + RSI + EMA + ATR\n"
            "⏱ Multi-Timeframe: 1m + 5m + 15m\n"
            "🔄 Scanning every 60 seconds\n"
            "🚫 Duplicate signal filter: 15 min\n"
            "🌅 Pre-market scanning from 8 AM NY\n"
            "📱 Live prices from Yahoo Finance\n\n"
            "Only HIGH/MEDIUM confidence signals sent. 🚀"
        )

        while True:
            status = market_status()
            ny = get_ny_time()
            print(f"\n[{ny.strftime('%H:%M:%S')} NY] Market: {status}")

            if status == "CLOSED":
                next_check = 300  # check every 5 min when closed
                print(f"  Market closed. Next check in {next_check//60} min.")
                await asyncio.sleep(next_check)
                continue

            for ticker in TICKERS:
                try:
                    print(f"  📡 {ticker}...")

                    # Fetch all timeframes in parallel
                    bars_1m, bars_5m, bars_15m = await asyncio.gather(
                        get_bars(session, ticker, "1Min", 30),
                        get_bars(session, ticker, "5Min", 50),
                        get_bars(session, ticker, "15Min", 50),
                    )

                    if len(bars_5m) < 20:
                        print(f"    ⚠️ Not enough bars")
                        continue

                    result = compute_signal(bars_1m, bars_5m, bars_15m)

                    if result is None:
                        print(f"    ⏳ WAIT")
                        continue

                    # Duplicate filter
                    if is_duplicate_signal(ticker, result["signal"]):
                        print(f"    🔄 Duplicate {result['signal']} — skipped")
                        continue

                    # Get Yahoo live price
                    yahoo_price, yahoo_change = await get_yahoo_price(session, ticker)

                    print(f"    ✅ {result['signal']} | Score:{result['buy_score'] if result['signal']=='BUY' else result['sell_score']}/11 | Conf:{result['confidence']} | Yahoo:${yahoo_price}")

                    msg = format_message(ticker, result, yahoo_price, yahoo_change, status)
                    await send_telegram(session, msg)

                    # Update state
                    last_signal_time[ticker] = datetime.now()
                    last_signal_type[ticker] = result["signal"]

                    await asyncio.sleep(2)

                except Exception as e:
                    print(f"    ❌ {ticker}: {e}")
                    continue

            await asyncio.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
