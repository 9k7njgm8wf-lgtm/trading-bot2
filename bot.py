import asyncio
import aiohttp
import json
from datetime import datetime, timezone

# ─── CONFIG ───────────────────────────────────────────────
GROQ_API_KEY     = "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"
ALPACA_API_KEY   = "PKOVEOWJ7IHAU4ZRT2W4RN6FCO"
ALPACA_SECRET    = "63kexZGqCue9eMii6Py6S13AwZks7y3mwTDi1TcmhKhK"
TELEGRAM_TOKEN   = "8855798705:AAFhs2RYnLUVxR-N2C2urTzl445NZn2fxv8"
TELEGRAM_CHAT_ID = "6903579390"

TICKERS  = ["RGTI", "RXT", "QUBT", "LUNR"]
TIMEFRAME = "5Min"
ANALYSIS_INTERVAL = 300  # every 5 minutes

ALPACA_BASE    = "https://data.alpaca.markets/v2"
ALPACA_HEADERS = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET
}

# ─── TELEGRAM ─────────────────────────────────────────────
async def send_telegram(session, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        async with session.post(url, json=payload) as resp:
            result = await resp.json()
            if not result.get("ok"):
                print(f"Telegram error: {result}")
    except Exception as e:
        print(f"Telegram error: {e}")

# ─── ALPACA: GET BARS ──────────────────────────────────────
async def get_bars(session, ticker):
    url = f"{ALPACA_BASE}/stocks/{ticker}/bars"
    params = {"timeframe": TIMEFRAME, "limit": 50, "feed": "iex"}
    try:
        async with session.get(url, headers=ALPACA_HEADERS, params=params) as resp:
            data = await resp.json()
            return data.get("bars", [])
    except Exception as e:
        print(f"Bars error {ticker}: {e}")
        return []

# ─── VWAP CALCULATION ─────────────────────────────────────
def calculate_vwap(bars):
    """VWAP = cumulative(typical_price * volume) / cumulative(volume)"""
    cumulative_tp_vol = 0
    cumulative_vol = 0
    vwap_values = []
    for b in bars:
        typical_price = (b['h'] + b['l'] + b['c']) / 3
        cumulative_tp_vol += typical_price * b['v']
        cumulative_vol += b['v']
        vwap = cumulative_tp_vol / cumulative_vol if cumulative_vol > 0 else 0
        vwap_values.append(round(vwap, 4))
    return vwap_values

# ─── RSI CALCULATION ──────────────────────────────────────
def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

# ─── EMA CALCULATION ──────────────────────────────────────
def calculate_ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
    return round(ema, 4)

# ─── VOLUME ANALYSIS ──────────────────────────────────────
def analyze_volume(bars):
    if len(bars) < 10:
        return None, None
    volumes = [b['v'] for b in bars]
    avg_volume = sum(volumes[:-1]) / len(volumes[:-1])
    current_volume = volumes[-1]
    volume_ratio = round(current_volume / avg_volume, 2) if avg_volume > 0 else 0
    return round(avg_volume), volume_ratio

# ─── SUPPORT & RESISTANCE ─────────────────────────────────
def get_support_resistance(bars):
    if len(bars) < 5:
        return None, None
    highs = [b['h'] for b in bars[-20:]]
    lows  = [b['l'] for b in bars[-20:]]
    resistance = round(max(highs), 4)
    support    = round(min(lows), 4)
    return support, resistance

# ─── MAIN SIGNAL LOGIC ────────────────────────────────────
def compute_signal(bars):
    if len(bars) < 15:
        return None

    closes  = [b['c'] for b in bars]
    current_price = closes[-1]

    # Indicators
    vwap_values = calculate_vwap(bars)
    current_vwap = vwap_values[-1]
    prev_vwap    = vwap_values[-2]

    rsi = calculate_rsi(closes)
    ema9  = calculate_ema(closes, 9)
    ema21 = calculate_ema(closes, 21)

    avg_vol, vol_ratio = analyze_volume(bars)
    support, resistance = get_support_resistance(bars)

    # VWAP signals
    price_above_vwap = current_price > current_vwap
    price_crossed_above_vwap = current_price > current_vwap and closes[-2] <= prev_vwap
    price_crossed_below_vwap = current_price < current_vwap and closes[-2] >= prev_vwap

    # Volume confirmation (high volume = strong signal)
    high_volume = vol_ratio >= 1.5

    # EMA trend
    ema_bullish = ema9 > ema21 if ema9 and ema21 else False
    ema_bearish = ema9 < ema21 if ema9 and ema21 else False

    # Scoring system
    buy_score  = 0
    sell_score = 0

    # VWAP (most important - 3 points)
    if price_crossed_above_vwap: buy_score += 3
    elif price_above_vwap:       buy_score += 1
    if price_crossed_below_vwap: sell_score += 3
    elif not price_above_vwap:   sell_score += 1

    # Volume confirmation (2 points)
    if high_volume:
        if price_above_vwap: buy_score += 2
        else:                sell_score += 2

    # RSI (2 points)
    if rsi:
        if rsi < 35:  buy_score += 2
        elif rsi < 50 and price_above_vwap: buy_score += 1
        if rsi > 65:  sell_score += 2
        elif rsi > 50 and not price_above_vwap: sell_score += 1

    # EMA (1 point)
    if ema_bullish: buy_score += 1
    if ema_bearish: sell_score += 1

    # Determine signal
    if buy_score >= 4:
        signal     = "BUY"
        confidence = "HIGH" if buy_score >= 6 else "MEDIUM"
        # SL below recent support or VWAP, TP at resistance
        sl = round(min(current_vwap, support) * 0.995, 4) if support else round(current_price * 0.98, 4)
        tp = round(resistance * 0.998, 4) if resistance else round(current_price * 1.04, 4)
    elif sell_score >= 4:
        signal     = "SELL"
        confidence = "HIGH" if sell_score >= 6 else "MEDIUM"
        sl = round(max(current_vwap, resistance) * 1.005, 4) if resistance else round(current_price * 1.02, 4)
        tp = round(support * 1.002, 4) if support else round(current_price * 0.96, 4)
    else:
        return None  # No strong signal — stay quiet

    risk   = abs(current_price - sl)
    reward = abs(tp - current_price)
    rr     = f"1:{round(reward/risk, 1)}" if risk > 0 else "N/A"

    trend = "UPTREND" if ema_bullish else "DOWNTREND" if ema_bearish else "SIDEWAYS"

    return {
        "signal":       signal,
        "confidence":   confidence,
        "entry":        round(current_price, 4),
        "sl":           sl,
        "tp":           tp,
        "rr":           rr,
        "vwap":         current_vwap,
        "rsi":          rsi,
        "ema9":         ema9,
        "ema21":        ema21,
        "vol_ratio":    vol_ratio,
        "trend":        trend,
        "buy_score":    buy_score,
        "sell_score":   sell_score,
        "support":      support,
        "resistance":   resistance,
    }

# ─── FORMAT TELEGRAM MESSAGE ──────────────────────────────
def format_message(ticker, result):
    signal = result["signal"]
    now    = datetime.now().strftime("%H:%M:%S")

    if signal == "BUY":
        header = f"🟢 <b>BUY — {ticker}</b>"
    else:
        header = f"🔴 <b>SELL — {ticker}</b>"

    conf_emoji = "🔥" if result["confidence"] == "HIGH" else "⚡"
    trend_emoji = "📈" if result["trend"] == "UPTREND" else "📉" if result["trend"] == "DOWNTREND" else "➡️"

    vwap_pos = "Above VWAP ✅" if result["entry"] > result["vwap"] else "Below VWAP ⚠️"

    msg = f"""{header}

💰 <b>Entry:</b>  ${result['entry']}
🛑 <b>Stop Loss:</b>  ${result['sl']}
✅ <b>Take Profit:</b>  ${result['tp']}
⚖️ <b>Risk/Reward:</b>  {result['rr']}

📊 <b>VWAP:</b>  ${result['vwap']}  |  {vwap_pos}
📉 <b>RSI:</b>  {result['rsi']}
📈 <b>EMA 9/21:</b>  {result['ema9']} / {result['ema21']}
🔊 <b>Volume:</b>  {result['vol_ratio']}x average
{trend_emoji} <b>Trend:</b>  {result['trend']}
{conf_emoji} <b>Confidence:</b>  {result['confidence']}

🏷 S: ${result['support']}  |  R: ${result['resistance']}
🕐 {now}  |  {TIMEFRAME}
─────────────────────"""
    return msg

# ─── MAIN LOOP ────────────────────────────────────────────
async def main():
    print("🚀 AlphaSignal VWAP Bot starting...")
    async with aiohttp.ClientSession() as session:
        await send_telegram(session,
            "🤖 <b>AlphaSignal VWAP Bot Started!</b>\n\n"
            f"📊 Watching: {', '.join(TICKERS)}\n"
            f"📐 Strategy: VWAP + Volume + RSI + EMA\n"
            f"⏱ Timeframe: {TIMEFRAME}\n"
            f"🔄 Analyzing every {ANALYSIS_INTERVAL//60} minutes\n\n"
            "Only HIGH/MEDIUM confidence signals will be sent. 🚀"
        )

        while True:
            for ticker in TICKERS:
                try:
                    print(f"\n📡 Analyzing {ticker}...")
                    bars = await get_bars(session, ticker)

                    if len(bars) < 15:
                        print(f"  ⚠️ Not enough bars for {ticker}: {len(bars)}")
                        continue

                    result = compute_signal(bars)

                    if result is None:
                        print(f"  ⏳ {ticker}: No strong signal (WAIT)")
                        continue

                    print(f"  ✅ {ticker}: {result['signal']} | Conf: {result['confidence']} | R:R {result['rr']} | RSI: {result['rsi']} | Vol: {result['vol_ratio']}x")

                    msg = format_message(ticker, result)
                    await send_telegram(session, msg)
                    print(f"  📲 Alert sent!")

                    await asyncio.sleep(3)

                except Exception as e:
                    print(f"  ❌ Error {ticker}: {e}")
                    continue

            print(f"\n⏰ Waiting {ANALYSIS_INTERVAL//60} min for next cycle...")
            await asyncio.sleep(ANALYSIS_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
