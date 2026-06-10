import asyncio
import aiohttp
import json
import time
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────
GROQ_API_KEY     = "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"
ALPACA_API_KEY   = "PKOVEOWJ7IHAU4ZRT2W4RN6FCO"
ALPACA_SECRET    = "63kexZGqCue9eMii6Py6S13AwZks7y3mwTDi1TcmhKhK"
TELEGRAM_TOKEN   = "8855798705:AAFhs2RYnLUVxR-N2C2urTzl445NZn2fxv8"
TELEGRAM_CHAT_ID = "6903579390"

TICKERS = ["RGTI", "RXT", "QUBT", "LUNR"]
TIMEFRAME = "5Min"
ANALYSIS_INTERVAL = 300  # analyze every 5 minutes per ticker

# ─── ALPACA REST ───────────────────────────────────────────
ALPACA_BASE = "https://data.alpaca.markets/v2"
ALPACA_HEADERS = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET
}

# ─── TELEGRAM ─────────────────────────────────────────────
async def send_telegram(session, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        async with session.post(url, json=payload) as resp:
            result = await resp.json()
            if not result.get("ok"):
                print(f"Telegram error: {result}")
    except Exception as e:
        print(f"Telegram send error: {e}")

# ─── ALPACA: GET BARS ──────────────────────────────────────
async def get_bars(session, ticker):
    url = f"{ALPACA_BASE}/stocks/{ticker}/bars"
    params = {
        "timeframe": TIMEFRAME,
        "limit": 20,
        "feed": "iex"
    }
    try:
        async with session.get(url, headers=ALPACA_HEADERS, params=params) as resp:
            data = await resp.json()
            bars = data.get("bars", [])
            return bars
    except Exception as e:
        print(f"Alpaca bars error for {ticker}: {e}")
        return []

# ─── ALPACA: GET LATEST PRICE ─────────────────────────────
async def get_latest_price(session, ticker):
    url = f"{ALPACA_BASE}/stocks/{ticker}/trades/latest"
    params = {"feed": "iex"}
    try:
        async with session.get(url, headers=ALPACA_HEADERS, params=params) as resp:
            data = await resp.json()
            return data.get("trade", {}).get("p", None)
    except Exception as e:
        print(f"Price fetch error for {ticker}: {e}")
        return None

# ─── GROQ AI ANALYSIS ─────────────────────────────────────
async def analyze_with_groq(session, ticker, price, bars):
    if not bars:
        bar_summary = "No bar data available."
    else:
        recent = bars[-10:]
        bar_summary = " | ".join([
            f"O:{b['o']} H:{b['h']} L:{b['l']} C:{b['c']} V:{b['v']}"
            for b in recent
        ])

    prompt = f"""You are an expert day trader. Analyze this stock for a day trade.

Stock: {ticker}
Current Price: ${price}
Timeframe: {TIMEFRAME}
Recent OHLCV bars (oldest to newest): {bar_summary}

Analyze trend, momentum, support/resistance, and volume.
Respond ONLY with a valid JSON object, no markdown, no explanation:
{{
  "signal": "BUY" or "SELL" or "WAIT",
  "entry": <number>,
  "sl": <number>,
  "tp": <number>,
  "rr": "<string like 1:2.5>",
  "confidence": "HIGH" or "MEDIUM" or "LOW",
  "trend": "UPTREND" or "DOWNTREND" or "SIDEWAYS",
  "reason": "<one sentence explaining the signal>"
}}"""

    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 300,
        "temperature": 0.2
    }

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        async with session.post(
            "https://api.groq.com/openai/v1/chat/completions",
            json=payload, headers=headers
        ) as resp:
            data = await resp.json()
            raw = data["choices"][0]["message"]["content"].strip()
            raw = raw.replace("```json", "").replace("```", "").strip()
            result = json.loads(raw)
            return result
    except Exception as e:
        print(f"Groq error for {ticker}: {e}")
        return None

# ─── FORMAT TELEGRAM MESSAGE ──────────────────────────────
def format_message(ticker, price, result):
    signal = result.get("signal", "WAIT")
    entry  = result.get("entry", price)
    sl     = result.get("sl", 0)
    tp     = result.get("tp", 0)
    rr     = result.get("rr", "N/A")
    conf   = result.get("confidence", "LOW")
    trend  = result.get("trend", "SIDEWAYS")
    reason = result.get("reason", "")
    now    = datetime.now().strftime("%H:%M:%S")

    if signal == "BUY":
        emoji = "🟢"
        signal_line = f"<b>🟢 BUY SIGNAL — {ticker}</b>"
    elif signal == "SELL":
        emoji = "🔴"
        signal_line = f"<b>🔴 SELL SIGNAL — {ticker}</b>"
    else:
        return None  # Don't send WAIT signals

    conf_emoji = "🔥" if conf == "HIGH" else "⚡" if conf == "MEDIUM" else "⚠️"
    trend_emoji = "📈" if trend == "UPTREND" else "📉" if trend == "DOWNTREND" else "➡️"

    msg = f"""{signal_line}

💰 <b>Price:</b> ${price:.2f}
🎯 <b>Entry:</b> ${entry:.2f}
🛑 <b>Stop Loss:</b> ${sl:.2f}
✅ <b>Take Profit:</b> ${tp:.2f}
⚖️ <b>Risk/Reward:</b> {rr}
{conf_emoji} <b>Confidence:</b> {conf}
{trend_emoji} <b>Trend:</b> {trend}

📝 {reason}

🕐 {now} | Timeframe: {TIMEFRAME}
─────────────────────"""

    return msg

# ─── MAIN LOOP ────────────────────────────────────────────
async def main():
    print("🚀 Trading bot starting...")

    async with aiohttp.ClientSession() as session:
        # Send startup message
        await send_telegram(session,
            "🤖 <b>AlphaSignal Bot Started!</b>\n\n"
            f"📊 Watching: {', '.join(TICKERS)}\n"
            f"⏱ Timeframe: {TIMEFRAME}\n"
            f"🔄 Analyzing every {ANALYSIS_INTERVAL//60} minutes\n\n"
            "Signals will appear here automatically. 🚀"
        )
        print("✅ Startup message sent to Telegram")

        while True:
            for ticker in TICKERS:
                try:
                    print(f"\n📡 Analyzing {ticker}...")

                    # Get price and bars
                    price = await get_latest_price(session, ticker)
                    bars  = await get_bars(session, ticker)

                    if not price:
                        print(f"  ⚠️ No price for {ticker}, skipping")
                        continue

                    print(f"  💰 Price: ${price}")

                    # AI analysis
                    result = await analyze_with_groq(session, ticker, price, bars)
                    if not result:
                        print(f"  ❌ Analysis failed for {ticker}")
                        continue

                    signal = result.get("signal", "WAIT")
                    print(f"  📊 Signal: {signal} | Conf: {result.get('confidence')} | R:R {result.get('rr')}")

                    # Send to Telegram only for BUY/SELL
                    if signal in ["BUY", "SELL"]:
                        msg = format_message(ticker, price, result)
                        if msg:
                            await send_telegram(session, msg)
                            print(f"  ✅ Alert sent to Telegram!")
                    else:
                        print(f"  ⏳ WAIT — no alert sent")

                    # Small delay between tickers
                    await asyncio.sleep(3)

                except Exception as e:
                    print(f"  ❌ Error processing {ticker}: {e}")
                    continue

            print(f"\n⏰ Waiting {ANALYSIS_INTERVAL//60} minutes for next cycle...")
            await asyncio.sleep(ANALYSIS_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
