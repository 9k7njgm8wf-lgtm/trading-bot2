import asyncio
import aiohttp
import json
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────
GROQ_API_KEY     = "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"
ALPACA_API_KEY   = "PKOVEOWJ7IHAU4ZRT2W4RN6FCO"
ALPACA_SECRET    = "63kexZGqCue9eMii6Py6S13AwZks7y3mwTDi1TcmhKhK"
TELEGRAM_TOKEN   = "8855798705:AAFhs2RYnLUVxR-N2C2urTzl445NZn2fxv8"
TELEGRAM_CHAT_ID = "6903579390"

TICKERS  = ["RGTI", "RXT", "QUBT", "LUNR"]
TIMEFRAME = "5Min"
ANALYSIS_INTERVAL = 300

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

# ─── YAHOO FINANCE PRICE ──────────────────────────────────
async def get_yahoo_price(session, ticker):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "1m", "range": "1d"}
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            data = await resp.json()
            price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
            prev_close = data["chart"]["result"][0]["meta"]["chartPreviousClose"]
            change_pct = ((price - prev_close) / prev_close) * 100
            return round(price, 4), round(change_pct, 2)
    except Exception as e:
        print(f"Yahoo price error {ticker}: {e}")
        return None, None

# ─── ALPACA BARS ──────────────────────────────────────────
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

# ─── INDICATORS ───────────────────────────────────────────
def calculate_vwap(bars):
    cum_tp_vol, cum_vol = 0, 0
    vwap_values = []
    for b in bars:
        tp = (b['h'] + b['l'] + b['c']) / 3
        cum_tp_vol += tp * b['v']
        cum_vol += b['v']
        vwap_values.append(round(cum_tp_vol / cum_vol if cum_vol > 0 else 0, 4))
    return vwap_values

def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains = [max(closes[i]-closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1]-closes[i], 0) for i in range(1, len(closes))]
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0: return 100
    return round(100 - (100 / (1 + avg_gain/avg_loss)), 2)

def calculate_ema(values, period):
    if len(values) < period: return None
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
    return round(ema, 4)

def analyze_volume(bars):
    if len(bars) < 10: return None, None
    volumes = [b['v'] for b in bars]
    avg_vol = sum(volumes[:-1]) / len(volumes[:-1])
    return round(avg_vol), round(volumes[-1] / avg_vol if avg_vol > 0 else 0, 2)

def get_support_resistance(bars):
    if len(bars) < 5: return None, None
    recent = bars[-20:]
    return round(min(b['l'] for b in recent), 4), round(max(b['h'] for b in recent), 4)

# ─── SIGNAL LOGIC ─────────────────────────────────────────
def compute_signal(bars):
    if len(bars) < 15: return None
    closes = [b['c'] for b in bars]
    current_price = closes[-1]

    vwap_values  = calculate_vwap(bars)
    current_vwap = vwap_values[-1]
    prev_vwap    = vwap_values[-2]

    rsi   = calculate_rsi(closes)
    ema9  = calculate_ema(closes, 9)
    ema21 = calculate_ema(closes, 21)
    _, vol_ratio = analyze_volume(bars)
    support, resistance = get_support_resistance(bars)

    price_above_vwap         = current_price > current_vwap
    price_crossed_above_vwap = current_price > current_vwap and closes[-2] <= prev_vwap
    price_crossed_below_vwap = current_price < current_vwap and closes[-2] >= prev_vwap
    high_volume  = vol_ratio >= 1.5
    ema_bullish  = ema9 > ema21 if ema9 and ema21 else False
    ema_bearish  = ema9 < ema21 if ema9 and ema21 else False

    buy_score = sell_score = 0
    if price_crossed_above_vwap: buy_score += 3
    elif price_above_vwap:       buy_score += 1
    if price_crossed_below_vwap: sell_score += 3
    elif not price_above_vwap:   sell_score += 1
    if high_volume:
        if price_above_vwap: buy_score += 2
        else:                sell_score += 2
    if rsi:
        if rsi < 35:  buy_score += 2
        elif rsi < 50 and price_above_vwap: buy_score += 1
        if rsi > 65:  sell_score += 2
        elif rsi > 50 and not price_above_vwap: sell_score += 1
    if ema_bullish: buy_score += 1
    if ema_bearish: sell_score += 1

    if buy_score >= 4:
        signal     = "BUY"
        confidence = "HIGH" if buy_score >= 6 else "MEDIUM"
        sl = round(min(current_vwap, support) * 0.995, 4) if support else round(current_price * 0.98, 4)
        tp = round(resistance * 0.998, 4) if resistance else round(current_price * 1.04, 4)
    elif sell_score >= 4:
        signal     = "SELL"
        confidence = "HIGH" if sell_score >= 6 else "MEDIUM"
        sl = round(max(current_vwap, resistance) * 1.005, 4) if resistance else round(current_price * 1.02, 4)
        tp = round(support * 1.002, 4) if support else round(current_price * 0.96, 4)
    else:
        return None

    risk   = abs(current_price - sl)
    reward = abs(tp - current_price)
    rr     = f"1:{round(reward/risk, 1)}" if risk > 0 else "N/A"
    trend  = "UPTREND" if ema_bullish else "DOWNTREND" if ema_bearish else "SIDEWAYS"

    return {
        "signal": signal, "confidence": confidence,
        "entry": round(current_price, 4),
        "sl": sl, "tp": tp, "rr": rr,
        "vwap": current_vwap, "rsi": rsi,
        "ema9": ema9, "ema21": ema21,
        "vol_ratio": vol_ratio, "trend": trend,
        "support": support, "resistance": resistance,
    }

# ─── FORMAT MESSAGE ───────────────────────────────────────
def format_message(ticker, result, yahoo_price, yahoo_change):
    signal = result["signal"]
    now    = datetime.now().strftime("%H:%M:%S")
    header = f"🟢 <b>BUY — {ticker}</b>" if signal == "BUY" else f"🔴 <b>SELL — {ticker}</b>"
    conf_emoji  = "🔥" if result["confidence"] == "HIGH" else "⚡"
    trend_emoji = "📈" if result["trend"] == "UPTREND" else "📉" if result["trend"] == "DOWNTREND" else "➡️"
    vwap_pos    = "Above VWAP ✅" if result["entry"] > result["vwap"] else "Below VWAP ⚠️"

    # Yahoo price line
    if yahoo_price:
        change_emoji = "📈" if yahoo_change and yahoo_change >= 0 else "📉"
        change_str   = f"+{yahoo_change}%" if yahoo_change and yahoo_change >= 0 else f"{yahoo_change}%"
        yahoo_line   = f"📱 <b>Current Price (Yahoo):</b>  <b>${yahoo_price}</b>  {change_emoji} {change_str} today"
    else:
        yahoo_line = "📱 <b>Current Price:</b>  unavailable"

    msg = f"""{header}

{yahoo_line}
━━━━━━━━━━━━━━━━━━━━━
🎯 <b>Entry:</b>  ${result['entry']}
🛑 <b>Stop Loss:</b>  ${result['sl']}
✅ <b>Take Profit:</b>  ${result['tp']}
⚖️ <b>Risk/Reward:</b>  {result['rr']}
━━━━━━━━━━━━━━━━━━━━━
📊 <b>VWAP:</b>  ${result['vwap']}  |  {vwap_pos}
📉 <b>RSI:</b>  {result['rsi']}
📈 <b>EMA 9/21:</b>  {result['ema9']} / {result['ema21']}
🔊 <b>Volume:</b>  {result['vol_ratio']}x average
{trend_emoji} <b>Trend:</b>  {result['trend']}
{conf_emoji} <b>Confidence:</b>  {result['confidence']}
━━━━━━━━━━━━━━━━━━━━━
🏷 Support: ${result['support']}  |  Resistance: ${result['resistance']}
🕐 {now}  |  {TIMEFRAME}"""
    return msg

# ─── MAIN LOOP ────────────────────────────────────────────
async def main():
    print("🚀 AlphaSignal VWAP Bot starting...")
    async with aiohttp.ClientSession() as session:
        await send_telegram(session,
            "🤖 <b>AlphaSignal VWAP Bot Started!</b>\n\n"
            f"📊 Watching: {', '.join(TICKERS)}\n"
            f"📐 Strategy: VWAP + Volume + RSI + EMA\n"
            f"📱 Live price from Yahoo Finance added!\n"
            f"⏱ Timeframe: {TIMEFRAME}\n\n"
            "Only HIGH/MEDIUM confidence signals sent. 🚀"
        )

        while True:
            for ticker in TICKERS:
                try:
                    print(f"\n📡 Analyzing {ticker}...")
                    bars = await get_bars(session, ticker)

                    if len(bars) < 15:
                        print(f"  ⚠️ Not enough bars: {len(bars)}")
                        continue

                    result = compute_signal(bars)
                    if result is None:
                        print(f"  ⏳ {ticker}: WAIT — no strong signal")
                        continue

                    # Get Yahoo live price
                    yahoo_price, yahoo_change = await get_yahoo_price(session, ticker)
                    print(f"  💰 Signal: {result['signal']} | Entry: ${result['entry']} | Yahoo: ${yahoo_price}")

                    msg = format_message(ticker, result, yahoo_price, yahoo_change)
                    await send_telegram(session, msg)
                    print(f"  📲 Alert sent!")
                    await asyncio.sleep(3)

                except Exception as e:
                    print(f"  ❌ Error {ticker}: {e}")
                    continue

            print(f"\n⏰ Waiting {ANALYSIS_INTERVAL//60} min...")
            await asyncio.sleep(ANALYSIS_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
