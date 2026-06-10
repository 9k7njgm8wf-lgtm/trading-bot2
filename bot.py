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
SCAN_INTERVAL      = 60
SIGNAL_COOLDOWN    = 900
MIN_SCORE          = 4
ACCOUNT_SIZE       = 1000   # your account size in USD
RISK_PER_TRADE_PCT = 2      # risk 2% per trade

ALPACA_BASE    = "https://data.alpaca.markets/v2"
ALPACA_HEADERS = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET
}

# ─── STATE ────────────────────────────────────────────────
last_signal_time  = {}
last_signal_type  = {}
orb_levels        = {}       # ticker -> {high, low, set}
active_trades     = {}       # ticker -> {signal, entry, sl, tp, time}
performance_log   = []       # list of completed trades
watchlist         = list(TICKERS)
bot_paused        = False
trailing_stops    = {}       # ticker -> current trailing sl

# ─── MARKET HOURS ─────────────────────────────────────────
def get_ny_time():
    return datetime.now(timezone.utc) + timedelta(hours=-4)

def market_status():
    ny = get_ny_time()
    if ny.weekday() >= 5: return "CLOSED"
    h, m = ny.hour, ny.minute
    if h < 8: return "CLOSED"
    if h < 9 or (h == 9 and m < 30): return "PRE_MARKET"
    if h >= 16: return "CLOSED"
    return "OPEN"

def is_orb_time():
    ny = get_ny_time()
    h, m = ny.hour, ny.minute
    return h == 9 and 30 <= m <= 45

def is_duplicate(ticker, signal):
    if ticker not in last_signal_time: return False
    elapsed = (datetime.now() - last_signal_time[ticker]).total_seconds()
    return elapsed < SIGNAL_COOLDOWN and last_signal_type.get(ticker) == signal

# ─── TELEGRAM ─────────────────────────────────────────────
async def send_telegram(session, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        async with session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}) as r:
            res = await r.json()
            if not res.get("ok"): print(f"TG error: {res}")
    except Exception as e:
        print(f"TG error: {e}")

async def get_telegram_updates(session, offset=0):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    try:
        async with session.get(url, params={"offset": offset, "timeout": 1}) as r:
            data = await r.json()
            return data.get("result", [])
    except:
        return []

# ─── YAHOO PRICE ──────────────────────────────────────────
async def get_yahoo_price(session, ticker):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json()
            meta  = data["chart"]["result"][0]["meta"]
            price = meta["regularMarketPrice"]
            prev  = meta["chartPreviousClose"]
            return round(price, 4), round(((price-prev)/prev)*100, 2)
    except Exception as e:
        print(f"Yahoo error {ticker}: {e}")
        return None, None

# ─── NEWS SCANNER ─────────────────────────────────────────
async def get_news_sentiment(session, ticker):
    url = f"https://query1.finance.yahoo.com/v1/finance/search"
    params = {"q": ticker, "newsCount": 3, "enableFuzzyQuery": False}
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json()
            news = data.get("news", [])
            if not news: return "NEUTRAL", []
            headlines = [n.get("title", "") for n in news[:3]]
            # Simple sentiment keywords
            negative = ["downgrade", "loss", "miss", "decline", "drop", "fail", "lawsuit", "sec", "fraud", "warning", "cut", "lower"]
            positive = ["upgrade", "beat", "surge", "rally", "buy", "bullish", "growth", "profit", "deal", "partnership"]
            neg_count = sum(1 for h in headlines for w in negative if w in h.lower())
            pos_count = sum(1 for h in headlines for w in positive if w in h.lower())
            if neg_count > pos_count:   sentiment = "NEGATIVE"
            elif pos_count > neg_count: sentiment = "POSITIVE"
            else:                       sentiment = "NEUTRAL"
            return sentiment, headlines
    except Exception as e:
        print(f"News error {ticker}: {e}")
        return "NEUTRAL", []

# ─── ALPACA BARS ──────────────────────────────────────────
async def get_bars(session, ticker, timeframe="5Min", limit=50):
    url = f"{ALPACA_BASE}/stocks/{ticker}/bars"
    try:
        async with session.get(url, headers=ALPACA_HEADERS, params={"timeframe": timeframe, "limit": limit, "feed": "iex"}) as r:
            data = await r.json()
            return data.get("bars", [])
    except Exception as e:
        print(f"Bars error {ticker}: {e}")
        return []

# ─── INDICATORS ───────────────────────────────────────────
def calc_vwap(bars):
    cv = ct = 0
    vals = []
    for b in bars:
        tp = (b['h']+b['l']+b['c'])/3
        ct += tp*b['v']; cv += b['v']
        vals.append(round(ct/cv if cv>0 else 0, 4))
    return vals

def calc_rsi(closes, p=14):
    if len(closes) < p+1: return None
    g = [max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
    l = [max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
    ag, al = sum(g[-p:])/p, sum(l[-p:])/p
    return round(100-(100/(1+ag/al)),2) if al!=0 else 100

def calc_ema(vals, p):
    if len(vals)<p: return None
    k, e = 2/(p+1), sum(vals[:p])/p
    for v in vals[p:]: e = v*k+e*(1-k)
    return round(e, 4)

def calc_atr(bars, p=14):
    if len(bars)<p+1: return None
    trs = [max(bars[i]['h']-bars[i]['l'], abs(bars[i]['h']-bars[i-1]['c']), abs(bars[i]['l']-bars[i-1]['c'])) for i in range(1,len(bars))]
    return round(sum(trs[-p:])/p, 4)

def calc_bb(closes, p=20):
    if len(closes)<p: return None, None, None
    sma  = sum(closes[-p:])/p
    std  = (sum((c-sma)**2 for c in closes[-p:])/p)**0.5
    return round(sma+2*std,4), round(sma,4), round(sma-2*std,4)

def calc_vol_ratio(bars):
    if len(bars)<10: return None
    v = [b['v'] for b in bars]
    avg = sum(v[:-1])/len(v[:-1])
    return round(v[-1]/avg if avg>0 else 0, 2)

def get_sr(bars, n=20):
    r = bars[-n:]
    return round(min(b['l'] for b in r),4), round(max(b['h'] for b in r),4)

# ─── ORB (Opening Range Breakout) ─────────────────────────
def update_orb(ticker, bars_1m):
    ny = get_ny_time()
    # Reset ORB daily at 9:30
    if ny.hour == 9 and ny.minute == 30:
        orb_levels[ticker] = {"high": None, "low": None, "set": False}
    if not orb_levels.get(ticker): 
        orb_levels[ticker] = {"high": None, "low": None, "set": False}
    # Build ORB from 9:30-9:45 bars
    orb_bars = [b for b in bars_1m if '09:3' in b.get('t','') or '09:4' in b.get('t','')]
    if orb_bars:
        orb_levels[ticker]["high"] = round(max(b['h'] for b in orb_bars), 4)
        orb_levels[ticker]["low"]  = round(min(b['l'] for b in orb_bars), 4)
        orb_levels[ticker]["set"]  = True

def check_orb_signal(ticker, current_price):
    orb = orb_levels.get(ticker, {})
    if not orb.get("set"): return None
    orb_high, orb_low = orb.get("high"), orb.get("low")
    if not orb_high or not orb_low: return None
    if current_price > orb_high * 1.001: return "BUY_ORB"
    if current_price < orb_low  * 0.999: return "SELL_ORB"
    return None

# ─── POSITION SIZING ──────────────────────────────────────
def calc_position_size(entry, sl):
    risk_amount  = ACCOUNT_SIZE * (RISK_PER_TRADE_PCT / 100)
    risk_per_share = abs(entry - sl)
    if risk_per_share <= 0: return 0, 0
    shares     = int(risk_amount / risk_per_share)
    total_cost = round(shares * entry, 2)
    return shares, total_cost

# ─── TRAILING STOP ────────────────────────────────────────
async def check_trailing_stops(session):
    for ticker in list(active_trades.keys()):
        trade = active_trades[ticker]
        yahoo_price, _ = await get_yahoo_price(session, ticker)
        if not yahoo_price: continue
        entry  = trade["entry"]
        signal = trade["signal"]
        atr    = trade.get("atr", abs(entry - trade["sl"]))
        
        if signal == "BUY":
            new_trail_sl = round(yahoo_price - atr * 1.5, 4)
            current_trail = trailing_stops.get(ticker, trade["sl"])
            if new_trail_sl > current_trail:
                trailing_stops[ticker] = new_trail_sl
                print(f"  📈 Trailing SL updated {ticker}: ${new_trail_sl}")
            # Check if trailing SL hit
            if yahoo_price <= trailing_stops.get(ticker, trade["sl"]):
                pnl = round((yahoo_price - entry) / entry * 100, 2)
                await session.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                    json={"chat_id": TELEGRAM_CHAT_ID, "parse_mode": "HTML",
                          "text": f"🔔 <b>TRAILING STOP HIT — {ticker}</b>\n\n💰 Exit: ${yahoo_price}\n📊 Entry: ${entry}\n{'📈' if pnl>0 else '📉'} P&L: {pnl}%\n\nTrade closed by trailing stop."}
                )
                performance_log.append({"ticker": ticker, "signal": signal, "entry": entry, "exit": yahoo_price, "pnl": pnl, "time": get_ny_time().strftime("%H:%M")})
                del active_trades[ticker]
                if ticker in trailing_stops: del trailing_stops[ticker]

# ─── MAIN SIGNAL LOGIC ────────────────────────────────────
def compute_signal(bars_1m, bars_5m, bars_15m):
    if len(bars_5m) < 20: return None
    closes = [b['c'] for b in bars_5m]
    price  = closes[-1]

    vwap   = calc_vwap(bars_5m)
    vwap_n, vwap_p = vwap[-1], vwap[-2]
    rsi    = calc_rsi(closes)
    ema9   = calc_ema(closes, 9)
    ema21  = calc_ema(closes, 21)
    atr    = calc_atr(bars_5m)
    vol_r  = calc_vol_ratio(bars_5m)
    sup, res = get_sr(bars_5m)
    bb_upper, bb_mid, bb_lower = calc_bb(closes)

    trend_15m = "NEUTRAL"
    if len(bars_15m) >= 21:
        c15 = [b['c'] for b in bars_15m]
        e9, e21 = calc_ema(c15, 9), calc_ema(c15, 21)
        if e9 and e21: trend_15m = "BULL" if e9>e21 else "BEAR"

    mom_1m = "NEUTRAL"
    if len(bars_1m) >= 5:
        c1 = [b['c'] for b in bars_1m[-5:]]
        mom_1m = "UP" if c1[-1]>c1[0] else "DOWN"

    above_vwap   = price > vwap_n
    cross_above  = price > vwap_n and closes[-2] <= vwap_p
    cross_below  = price < vwap_n and closes[-2] >= vwap_p
    high_vol     = vol_r and vol_r >= 1.5
    ema_bull     = ema9 > ema21 if ema9 and ema21 else False
    bb_squeeze   = (bb_upper - bb_lower) < atr * 2 if bb_upper and bb_lower and atr else False

    buy_s = sell_s = 0
    # VWAP (3pts)
    if cross_above:    buy_s  += 3
    elif above_vwap:   buy_s  += 1
    if cross_below:    sell_s += 3
    elif not above_vwap: sell_s += 1
    # Volume (2pts)
    if high_vol:
        if above_vwap: buy_s  += 2
        else:          sell_s += 2
    elif vol_r and vol_r >= 1.2:
        if above_vwap: buy_s  += 1
        else:          sell_s += 1
    # RSI (2pts)
    if rsi:
        if rsi < 35:                        buy_s  += 2
        elif rsi < 50 and above_vwap:       buy_s  += 1
        if rsi > 65:                        sell_s += 2
        elif rsi > 50 and not above_vwap:   sell_s += 1
    # EMA (1pt)
    if ema_bull:  buy_s  += 1
    else:         sell_s += 1
    # 15m trend (2pts)
    if trend_15m == "BULL": buy_s  += 2
    if trend_15m == "BEAR": sell_s += 2
    # 1m momentum (1pt)
    if mom_1m == "UP":   buy_s  += 1
    if mom_1m == "DOWN": sell_s += 1
    # BB (1pt)
    if bb_lower and price <= bb_lower: buy_s  += 1
    if bb_upper and price >= bb_upper: sell_s += 1

    if buy_s >= MIN_SCORE:
        signal = "BUY"; conf = "HIGH" if buy_s >= 7 else "MEDIUM"
    elif sell_s >= MIN_SCORE:
        signal = "SELL"; conf = "HIGH" if sell_s >= 7 else "MEDIUM"
    else:
        return None

    if atr:
        sl = round(price - atr*1.5, 4) if signal=="BUY" else round(price + atr*1.5, 4)
        tp = round(price + atr*3.0, 4) if signal=="BUY" else round(price - atr*3.0, 4)
    else:
        sl = round(min(vwap_n,sup)*0.995,4) if signal=="BUY" else round(max(vwap_n,res)*1.005,4)
        tp = round(res*0.998,4)             if signal=="BUY" else round(sup*1.002,4)

    risk = abs(price-sl); reward = abs(tp-price)
    rr   = f"1:{round(reward/risk,1)}" if risk>0 else "N/A"

    return {
        "signal": signal, "confidence": conf,
        "entry": round(price,4), "sl": sl, "tp": tp, "rr": rr,
        "vwap": vwap_n, "rsi": rsi, "ema9": ema9, "ema21": ema21,
        "atr": atr, "vol_ratio": vol_r,
        "trend": "UPTREND" if ema_bull else "DOWNTREND",
        "trend_15m": trend_15m, "momentum_1m": mom_1m,
        "bb_upper": bb_upper, "bb_lower": bb_lower,
        "buy_score": buy_s, "sell_score": sell_s,
        "support": sup, "resistance": res, "bb_squeeze": bb_squeeze,
    }

# ─── FORMAT SIGNAL MESSAGE ────────────────────────────────
def format_signal(ticker, result, yahoo_price, yahoo_change, status, sentiment, headlines, orb_tag=""):
    sig    = result["signal"]
    now    = get_ny_time().strftime("%H:%M:%S") + " NY"
    header = f"🟢 <b>BUY — {ticker}</b>" if sig=="BUY" else f"🔴 <b>SELL — {ticker}</b>"
    if orb_tag: header += " 🚀 <b>ORB BREAKOUT</b>"
    ce     = "🔥" if result["confidence"]=="HIGH" else "⚡"
    t15e   = "📈" if result["trend_15m"]=="BULL" else "📉" if result["trend_15m"]=="BEAR" else "➡️"
    m1e    = "⬆️" if result["momentum_1m"]=="UP" else "⬇️"
    sl_lbl = "🟢 MARKET OPEN" if status=="OPEN" else "🌅 PRE-MARKET"
    sent_e = "✅" if sentiment=="POSITIVE" else "⚠️" if sentiment=="NEGATIVE" else "➖"

    yline  = f"📱 <b>Live Price:</b>  <b>${yahoo_price}</b>  {'📈' if yahoo_change>=0 else '📉'} {'+' if yahoo_change>=0 else ''}{yahoo_change}% today" if yahoo_price else f"📱 <b>Price:</b>  ${result['entry']}"

    shares, cost = calc_position_size(result['entry'], result['sl'])
    score  = result['buy_score'] if sig=="BUY" else result['sell_score']
    squeeze_line = "\n🔔 <b>BB Squeeze detected — big move incoming!</b>" if result.get("bb_squeeze") else ""

    news_line = ""
    if headlines:
        news_line = f"\n📰 <b>News:</b> {sent_e} {sentiment}\n<i>{headlines[0][:60]}...</i>" if headlines[0] else ""

    msg = f"""{header}  {ce} {result['confidence']}
{sl_lbl}{squeeze_line}

{yline}
━━━━━━━━━━━━━━━━━━━━━
🎯 <b>Entry:</b>  ${result['entry']}
🛑 <b>Stop Loss:</b>  ${result['sl']}
✅ <b>Take Profit:</b>  ${result['tp']}
⚖️ <b>Risk/Reward:</b>  {result['rr']}
📐 <b>ATR:</b>  ${result['atr']}
━━━━━━━━━━━━━━━━━━━━━
💼 <b>Position Size:</b>  {shares} shares  (${cost})
💰 <b>Risk Amount:</b>  ${round(ACCOUNT_SIZE*RISK_PER_TRADE_PCT/100,2)}  ({RISK_PER_TRADE_PCT}% of ${ACCOUNT_SIZE})
━━━━━━━━━━━━━━━━━━━━━
📊 <b>VWAP:</b>  ${result['vwap']}
📉 <b>RSI:</b>  {result['rsi']}
📈 <b>EMA 9/21:</b>  {result['ema9']} / {result['ema21']}
🔊 <b>Volume:</b>  {result['vol_ratio']}x avg
{t15e} <b>15m Trend:</b>  {result['trend_15m']}
{m1e} <b>1m Momentum:</b>  {result['momentum_1m']}
🏆 <b>Signal Score:</b>  {score}/12{news_line}
━━━━━━━━━━━━━━━━━━━━━
🏷 S: ${result['support']}  R: ${result['resistance']}
🕐 {now}"""
    return msg

# ─── DAILY REPORT ─────────────────────────────────────────
async def send_daily_report(session):
    if not performance_log:
        await send_telegram(session, "📊 <b>Daily Report</b>\n\nNo completed trades today.")
        return
    wins   = [t for t in performance_log if t['pnl'] > 0]
    losses = [t for t in performance_log if t['pnl'] <= 0]
    total_pnl = round(sum(t['pnl'] for t in performance_log), 2)
    win_rate  = round(len(wins)/len(performance_log)*100, 1)
    msg = f"""📊 <b>Daily Performance Report</b>

✅ Wins:    {len(wins)}
❌ Losses:  {len(losses)}
🎯 Win Rate: {win_rate}%
💰 Total P&L: {'+' if total_pnl>0 else ''}{total_pnl}%

<b>Trades:</b>
"""
    for t in performance_log[-10:]:
        e = "✅" if t['pnl']>0 else "❌"
        msg += f"{e} {t['ticker']} {t['signal']} → {'+' if t['pnl']>0 else ''}{t['pnl']}%\n"
    await send_telegram(session, msg)
    performance_log.clear()

# ─── TELEGRAM COMMAND HANDLER ─────────────────────────────
async def handle_commands(session, last_update_id):
    global bot_paused, watchlist, ACCOUNT_SIZE
    updates = await get_telegram_updates(session, last_update_id+1)
    for update in updates:
        last_update_id = update["update_id"]
        msg = update.get("message", {})
        text = msg.get("text", "").strip()
        if not text: continue

        print(f"  📩 Command: {text}")

        if text == "/status":
            status = market_status()
            ny = get_ny_time().strftime("%H:%M:%S")
            active_list = "\n".join([f"  {t}: {v['signal']} @ ${v['entry']}" for t,v in active_trades.items()]) or "  None"
            await send_telegram(session,
                f"📡 <b>Bot Status</b>\n\n"
                f"⏰ NY Time: {ny}\n"
                f"📊 Market: {status}\n"
                f"{'⏸ PAUSED' if bot_paused else '▶️ RUNNING'}\n"
                f"👀 Watching: {', '.join(watchlist)}\n"
                f"💼 Active Trades:\n{active_list}"
            )

        elif text.startswith("/add "):
            ticker = text.split()[1].upper()
            if ticker not in watchlist:
                watchlist.append(ticker)
                await send_telegram(session, f"✅ Added <b>{ticker}</b> to watchlist!\n📊 Watching: {', '.join(watchlist)}")
            else:
                await send_telegram(session, f"⚠️ {ticker} already in watchlist.")

        elif text.startswith("/remove "):
            ticker = text.split()[1].upper()
            if ticker in watchlist:
                watchlist.remove(ticker)
                await send_telegram(session, f"✅ Removed <b>{ticker}</b>\n📊 Watching: {', '.join(watchlist)}")
            else:
                await send_telegram(session, f"⚠️ {ticker} not in watchlist.")

        elif text == "/watchlist":
            await send_telegram(session, f"👀 <b>Watchlist:</b>\n{', '.join(watchlist)}\n\nUse /add TICKER or /remove TICKER")

        elif text == "/pause":
            bot_paused = True
            await send_telegram(session, "⏸ <b>Bot paused.</b> Send /resume to restart.")

        elif text == "/resume":
            bot_paused = False
            await send_telegram(session, "▶️ <b>Bot resumed!</b> Scanning for signals...")

        elif text == "/report":
            await send_daily_report(session)

        elif text.startswith("/risk "):
            try:
                ACCOUNT_SIZE = float(text.split()[1])
                await send_telegram(session, f"💰 Account size updated to <b>${ACCOUNT_SIZE}</b>\nRisk per trade: {RISK_PER_TRADE_PCT}% = ${round(ACCOUNT_SIZE*RISK_PER_TRADE_PCT/100,2)}")
            except:
                await send_telegram(session, "⚠️ Usage: /risk 5000")

        elif text == "/help":
            await send_telegram(session,
                "🤖 <b>AlphaSignal Commands</b>\n\n"
                "/status — bot status & active trades\n"
                "/watchlist — show current tickers\n"
                "/add AAPL — add ticker\n"
                "/remove RGTI — remove ticker\n"
                "/pause — pause signals\n"
                "/resume — resume signals\n"
                "/report — daily performance\n"
                "/risk 5000 — set account size\n"
                "/help — show this menu"
            )

    return last_update_id

# ─── MAIN LOOP ────────────────────────────────────────────
async def main():
    global bot_paused
    print("🚀 AlphaSignal ULTIMATE Bot starting...")
    last_update_id  = 0
    last_report_day = -1

    async with aiohttp.ClientSession() as session:
        await send_telegram(session,
            "🤖 <b>AlphaSignal ULTIMATE Bot Started!</b>\n\n"
            f"📊 Watching: {', '.join(watchlist)}\n"
            "📐 VWAP + Volume + RSI + EMA + ATR + BB\n"
            "🚀 Opening Range Breakout (ORB)\n"
            "📰 News & Sentiment Scanner\n"
            "💼 Smart Position Sizing\n"
            "📈 Trailing Stop Alerts\n"
            "⏱ Multi-Timeframe: 1m+5m+15m\n"
            "🔄 Scanning every 60 seconds\n\n"
            "📩 Commands: /help\n"
            "Only HIGH/MEDIUM confidence signals sent. 🚀"
        )

        while True:
            # Handle Telegram commands
            last_update_id = await handle_commands(session, last_update_id)

            status = market_status()
            ny     = get_ny_time()

            # Daily report at 4:05 PM
            if ny.hour == 16 and ny.minute == 5 and ny.day != last_report_day:
                await send_daily_report(session)
                last_report_day = ny.day

            if status == "CLOSED":
                await asyncio.sleep(300)
                continue

            if bot_paused:
                await asyncio.sleep(30)
                continue

            # Check trailing stops
            if active_trades:
                await check_trailing_stops(session)

            print(f"\n[{ny.strftime('%H:%M:%S')} NY] {status} | Watching: {watchlist}")

            for ticker in list(watchlist):
                try:
                    print(f"  📡 {ticker}...")

                    bars_1m, bars_5m, bars_15m = await asyncio.gather(
                        get_bars(session, ticker, "1Min", 30),
                        get_bars(session, ticker, "5Min", 50),
                        get_bars(session, ticker, "15Min", 50),
                    )

                    if len(bars_5m) < 20:
                        print(f"    ⚠️ Not enough bars")
                        continue

                    # Update ORB levels
                    update_orb(ticker, bars_1m)

                    result = compute_signal(bars_1m, bars_5m, bars_15m)
                    if result is None:
                        print(f"    ⏳ WAIT")
                        continue

                    if is_duplicate(ticker, result["signal"]):
                        print(f"    🔄 Duplicate — skip")
                        continue

                    # Check ORB
                    orb_tag = check_orb_signal(ticker, result["entry"]) or ""

                    # News sentiment
                    sentiment, headlines = await get_news_sentiment(session, ticker)

                    # Block signal on negative news for BUY
                    if sentiment == "NEGATIVE" and result["signal"] == "BUY":
                        await send_telegram(session, f"⚠️ <b>{ticker} BUY signal blocked</b> — negative news detected!\n📰 {headlines[0][:80] if headlines else ''}")
                        print(f"    🚫 Blocked by negative news")
                        continue

                    yahoo_price, yahoo_change = await get_yahoo_price(session, ticker)

                    score = result['buy_score'] if result['signal']=='BUY' else result['sell_score']
                    print(f"    ✅ {result['signal']} Score:{score}/12 Conf:{result['confidence']} News:{sentiment} Yahoo:${yahoo_price}")

                    msg = format_signal(ticker, result, yahoo_price, yahoo_change, status, sentiment, headlines, orb_tag)
                    await send_telegram(session, msg)

                    # Track trade
                    active_trades[ticker] = {
                        "signal": result["signal"], "entry": result["entry"],
                        "sl": result["sl"], "tp": result["tp"], "atr": result["atr"],
                        "time": ny.strftime("%H:%M")
                    }
                    trailing_stops[ticker] = result["sl"]
                    last_signal_time[ticker] = datetime.now()
                    last_signal_type[ticker] = result["signal"]

                    await asyncio.sleep(2)

                except Exception as e:
                    print(f"    ❌ {ticker}: {e}")
                    continue

            await asyncio.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
