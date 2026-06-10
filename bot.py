import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

# ─── CONFIG ───────────────────────────────────────────────
GROQ_API_KEY     = "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"
ALPACA_API_KEY   = "AKPYM4PLBJBOEBSD3NQQVEDALI"
ALPACA_SECRET    = "6Z73eeNG8Fpa64Tw7UBaEULCyFHJRCSh6r3kRgC7k2qo"
TELEGRAM_TOKEN   = "8855798705:AAFhs2RYnLUVxR-N2C2urTzl445NZn2fxv8"
TELEGRAM_CHAT_ID = "6903579390"

TICKERS            = ["RGTI", "RXT", "QUBT", "LUNR"]
SCAN_INTERVAL      = 60
SIGNAL_COOLDOWN    = 900
MIN_SCORE          = 4
ACCOUNT_SIZE       = 1000
RISK_PER_TRADE_PCT = 2

ALPACA_BASE    = "https://data.alpaca.markets/v2"
ALPACA_HEADERS = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET
}

# ─── STATE ────────────────────────────────────────────────
last_signal_time = {}
last_signal_type = {}
orb_levels       = {}
active_trades    = {}
performance_log  = []
watchlist        = list(TICKERS)
bot_paused       = False
trailing_stops   = {}
last_update_id   = 0
price_alerts     = {}   # ticker -> list of {price, direction, chat_id}
morning_brief_sent = False

# ─── TIME ─────────────────────────────────────────────────
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
            vol   = meta.get("regularMarketVolume", 0)
            return round(price,4), round(((price-prev)/prev)*100,2), vol
    except Exception as e:
        print(f"Yahoo error {ticker}: {e}")
        return None, None, None

# ─── NEWS SENTIMENT ───────────────────────────────────────
async def get_news_sentiment(session, ticker):
    url = f"https://query1.finance.yahoo.com/v1/finance/search"
    params = {"q": ticker, "newsCount": 3, "enableFuzzyQuery": False}
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json()
            news = data.get("news", [])
            if not news: return "NEUTRAL", []
            headlines = [n.get("title","") for n in news[:3]]
            neg = ["downgrade","loss","miss","decline","drop","fail","lawsuit","sec","fraud","warning","cut","lower","crash","bankrupt"]
            pos = ["upgrade","beat","surge","rally","buy","bullish","growth","profit","deal","partnership","launch","record"]
            nc = sum(1 for h in headlines for w in neg if w in h.lower())
            pc = sum(1 for h in headlines for w in pos if w in h.lower())
            return ("NEGATIVE" if nc>pc else "POSITIVE" if pc>nc else "NEUTRAL"), headlines
    except:
        return "NEUTRAL", []

# ─── ALPACA BARS ──────────────────────────────────────────
async def get_bars(session, ticker, timeframe="5Min", limit=50):
    url = f"{ALPACA_BASE}/stocks/{ticker}/bars"
    try:
        async with session.get(url, headers=ALPACA_HEADERS, params={"timeframe": timeframe, "limit": limit, "feed": "sip"}) as r:
            data = await r.json()
            return data.get("bars", [])
    except Exception as e:
        print(f"Bars error {ticker}: {e}")
        return []

# ─── INDICATORS ───────────────────────────────────────────
def calc_vwap(bars):
    cv=ct=0; vals=[]
    for b in bars:
        tp=(b['h']+b['l']+b['c'])/3; ct+=tp*b['v']; cv+=b['v']
        vals.append(round(ct/cv if cv>0 else 0,4))
    return vals

def calc_rsi(closes, p=14):
    if len(closes)<p+1: return None
    g=[max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
    l=[max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
    ag,al=sum(g[-p:])/p,sum(l[-p:])/p
    return round(100-(100/(1+ag/al)),2) if al!=0 else 100

def calc_ema(vals,p):
    if len(vals)<p: return None
    k,e=2/(p+1),sum(vals[:p])/p
    for v in vals[p:]: e=v*k+e*(1-k)
    return round(e,4)

def calc_atr(bars,p=14):
    if len(bars)<p+1: return None
    trs=[max(bars[i]['h']-bars[i]['l'],abs(bars[i]['h']-bars[i-1]['c']),abs(bars[i]['l']-bars[i-1]['c'])) for i in range(1,len(bars))]
    return round(sum(trs[-p:])/p,4)

def calc_bb(closes,p=20):
    if len(closes)<p: return None,None,None
    sma=sum(closes[-p:])/p; std=(sum((c-sma)**2 for c in closes[-p:])/p)**0.5
    return round(sma+2*std,4),round(sma,4),round(sma-2*std,4)

def calc_vol_ratio(bars):
    if len(bars)<10: return None
    v=[b['v'] for b in bars]; avg=sum(v[:-1])/len(v[:-1])
    return round(v[-1]/avg if avg>0 else 0,2)


def calc_rvol(bars_today, bars_yesterday):
    """Relative Volume vs same time yesterday"""
    if not bars_today or not bars_yesterday: return None
    today_vol = sum(b["v"] for b in bars_today)
    yest_vol  = sum(b["v"] for b in bars_yesterday[:len(bars_today)])
    if yest_vol == 0: return None
    return round(today_vol / yest_vol, 2)

def get_multiday_levels(bars_daily):
    if len(bars_daily) < 2: return None
    prev = bars_daily[-2]
    ph,pl,pc = round(prev["h"],4),round(prev["l"],4),round(prev["c"],4)
    pivot = round((ph+pl+pc)/3,4)
    return {"prev_high":ph,"prev_low":pl,"prev_close":pc,"pivot":pivot,"r1":round(2*pivot-pl,4),"s1":round(2*pivot-ph,4)}
def get_sr(bars,n=20):
    r=bars[-n:]
    return round(min(b['l'] for b in r),4),round(max(b['h'] for b in r),4)

# ─── CANDLESTICK PATTERNS ─────────────────────────────────
def detect_patterns(bars):
    patterns = []
    if len(bars) < 3: return patterns
    b0,b1,b2 = bars[-3],bars[-2],bars[-1]  # oldest to newest
    o2,h2,l2,c2 = b2['o'],b2['h'],b2['l'],b2['c']
    o1,h1,l1,c1 = b1['o'],b1['h'],b1['l'],b1['c']
    body2 = abs(c2-o2); range2 = h2-l2
    body1 = abs(c1-o1)

    # Doji (indecision)
    if range2 > 0 and body2/range2 < 0.1:
        patterns.append("⚪ Doji")

    # Hammer (bullish reversal)
    if body2 > 0 and (l2 < min(o2,c2)) and (min(o2,c2)-l2) > 2*body2 and c2 > o2:
        patterns.append("🔨 Hammer (Bullish)")

    # Shooting Star (bearish reversal)
    if body2 > 0 and (h2 > max(o2,c2)) and (h2-max(o2,c2)) > 2*body2 and c2 < o2:
        patterns.append("⭐ Shooting Star (Bearish)")

    # Bullish Engulfing
    if c1 < o1 and c2 > o2 and c2 > o1 and o2 < c1:
        patterns.append("🟢 Bullish Engulfing")

    # Bearish Engulfing
    if c1 > o1 and c2 < o2 and c2 < o1 and o2 > c1:
        patterns.append("🔴 Bearish Engulfing")

    # Morning Star (3-bar bullish reversal)
    o0,c0 = b0['o'],b0['c']
    if c0 < o0 and body1 < abs(c0-o0)*0.5 and c2 > o2 and c2 > (o0+c0)/2:
        patterns.append("🌟 Morning Star (Bullish)")

    # Evening Star (3-bar bearish reversal)
    if c0 > o0 and body1 < abs(c0-o0)*0.5 and c2 < o2 and c2 < (o0+c0)/2:
        patterns.append("🌆 Evening Star (Bearish)")

    # Marubozu (strong trend bar)
    if range2 > 0 and body2/range2 > 0.9:
        if c2 > o2: patterns.append("💚 Bullish Marubozu (Strong Buy)")
        else:       patterns.append("❤️ Bearish Marubozu (Strong Sell)")

    return patterns

# ─── ORB ──────────────────────────────────────────────────
def update_orb(ticker, bars_1m):
    ny = get_ny_time()
    if ny.hour == 9 and ny.minute == 30:
        orb_levels[ticker] = {"high":None,"low":None,"set":False}
    if not orb_levels.get(ticker):
        orb_levels[ticker] = {"high":None,"low":None,"set":False}
    orb_bars = [b for b in bars_1m if '09:3' in b.get('t','') or '09:4' in b.get('t','')]
    if orb_bars:
        orb_levels[ticker]["high"] = round(max(b['h'] for b in orb_bars),4)
        orb_levels[ticker]["low"]  = round(min(b['l'] for b in orb_bars),4)
        orb_levels[ticker]["set"]  = True

def check_orb_signal(ticker, price):
    orb = orb_levels.get(ticker,{})
    if not orb.get("set"): return None
    if price > orb["high"]*1.001: return "BUY_ORB"
    if price < orb["low"]*0.999:  return "SELL_ORB"
    return None

# ─── POSITION SIZING ──────────────────────────────────────
def calc_position_size(entry, sl):
    risk_amt = ACCOUNT_SIZE*(RISK_PER_TRADE_PCT/100)
    rps = abs(entry-sl)
    if rps<=0: return 0,0
    shares = int(risk_amt/rps)
    return shares, round(shares*entry,2)

# ─── TRAILING STOP ────────────────────────────────────────
async def check_trailing_stops(session):
    for ticker in list(active_trades.keys()):
        trade = active_trades[ticker]
        yahoo_price,_,_ = await get_yahoo_price(session, ticker)
        if not yahoo_price: continue
        entry  = trade["entry"]
        signal = trade["signal"]
        atr    = trade.get("atr", abs(entry-trade["sl"]))
        if signal == "BUY":
            new_trail = round(yahoo_price - atr*1.5, 4)
            cur_trail = trailing_stops.get(ticker, trade["sl"])
            if new_trail > cur_trail:
                trailing_stops[ticker] = new_trail
                await send_telegram(session, f"📈 <b>Trailing SL Updated — {ticker}</b>\n\n🛑 New SL: <b>${new_trail}</b>\n💰 Current Price: ${yahoo_price}\n🔒 Locking in profits!")
            if yahoo_price <= trailing_stops.get(ticker, trade["sl"]):
                pnl = round((yahoo_price-entry)/entry*100,2)
                await send_telegram(session, f"🔔 <b>TRAILING STOP HIT — {ticker}</b>\n\n💰 Exit: ${yahoo_price}\n📊 Entry: ${entry}\n{'📈' if pnl>0 else '📉'} P&L: {'+' if pnl>0 else ''}{pnl}%")
                performance_log.append({"ticker":ticker,"signal":signal,"entry":entry,"exit":yahoo_price,"pnl":pnl,"time":get_ny_time().strftime("%H:%M")})
                del active_trades[ticker]
                if ticker in trailing_stops: del trailing_stops[ticker]

# ─── PRICE ALERTS ─────────────────────────────────────────
async def check_price_alerts(session):
    for ticker in list(price_alerts.keys()):
        alerts = price_alerts[ticker]
        if not alerts: continue
        yahoo_price,_,_ = await get_yahoo_price(session, ticker)
        if not yahoo_price: continue
        remaining = []
        for alert in alerts:
            target = alert["price"]
            direction = alert["direction"]
            if direction == "above" and yahoo_price >= target:
                await send_telegram(session, f"🔔 <b>PRICE ALERT — {ticker}</b>\n\n📱 Current: <b>${yahoo_price}</b>\n🎯 Target: ${target}\n✅ Price crossed ABOVE your alert!")
            elif direction == "below" and yahoo_price <= target:
                await send_telegram(session, f"🔔 <b>PRICE ALERT — {ticker}</b>\n\n📱 Current: <b>${yahoo_price}</b>\n🎯 Target: ${target}\n✅ Price crossed BELOW your alert!")
            else:
                remaining.append(alert)
        price_alerts[ticker] = remaining

# ─── AI CONFIRMATION (GROQ) ───────────────────────────────
async def ai_confirm_signal(session, ticker, result, patterns, sentiment):
    pattern_str = ", ".join(patterns) if patterns else "None detected"
    prompt = f"""You are an expert day trader. Review this trade signal and give a final verdict.

Stock: {ticker}
Signal: {result['signal']}
Entry: ${result['entry']}
Stop Loss: ${result['sl']}
Take Profit: ${result['tp']}
Risk/Reward: {result['rr']}

Technical Indicators:
- VWAP: ${result['vwap']} (price is {'ABOVE' if result['entry'] > result['vwap'] else 'BELOW'})
- RSI: {result['rsi']}
- EMA 9/21: {result['ema9']} / {result['ema21']}
- Volume: {result['vol_ratio']}x average
- ATR: ${result['atr']}
- 15m Trend: {result['trend_15m']}
- 1m Momentum: {result['momentum_1m']}
- Signal Score: {result['buy_score'] if result['signal']=='BUY' else result['sell_score']}/12
- Candlestick Patterns: {pattern_str}
- News Sentiment: {sentiment}

Respond ONLY with JSON, no markdown:
{{"verdict": "CONFIRMED" or "REJECTED" or "CAUTION", "reason": "one clear sentence explaining why", "tip": "one actionable tip for this trade"}}"""

    try:
        async with session.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": "llama-3.3-70b-versatile", "messages": [{"role":"user","content":prompt}], "max_tokens":200, "temperature":0.1},
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            data = await r.json()
            raw = data["choices"][0]["message"]["content"].strip().replace("```json","").replace("```","")
            return json.loads(raw)
    except Exception as e:
        print(f"AI confirm error: {e}")
        return {"verdict": "CONFIRMED", "reason": "AI review unavailable", "tip": "Use your own judgment"}

# ─── MORNING MARKET BRIEF ─────────────────────────────────
async def send_morning_brief(session):
    ny = get_ny_time().strftime("%A, %B %d")
    brief = f"🌅 <b>Morning Market Brief — {ny}</b>\n\n"

    # SPY overview
    spy_price, spy_change, spy_vol = await get_yahoo_price(session, "SPY")
    qqq_price, qqq_change, _ = await get_yahoo_price(session, "QQQ")
    vix_price, vix_change, _ = await get_yahoo_price(session, "^VIX")

    brief += "📊 <b>Market Overview:</b>\n"
    if spy_price: brief += f"  SPY: ${spy_price}  {'📈' if spy_change>=0 else '📉'} {'+' if spy_change>=0 else ''}{spy_change}%\n"
    if qqq_price: brief += f"  QQQ: ${qqq_price}  {'📈' if qqq_change>=0 else '📉'} {'+' if qqq_change>=0 else ''}{qqq_change}%\n"
    if vix_price:
        vix_e = "😱 HIGH FEAR" if vix_price>25 else "⚠️ ELEVATED" if vix_price>18 else "😊 LOW"
        brief += f"  VIX: ${vix_price}  {vix_e}\n"

    brief += "\n👀 <b>Your Watchlist:</b>\n"
    for ticker in watchlist:
        price, change, vol = await get_yahoo_price(session, ticker)
        if price:
            e = "📈" if change and change>=0 else "📉"
            brief += f"  {ticker}: ${price}  {e} {'+' if change and change>=0 else ''}{change}%\n"
        await asyncio.sleep(0.5)

    brief += "\n⏰ Market opens in ~5 minutes!\n"
    brief += "🎯 Good luck today. Trade safe, manage risk! 💪"
    await send_telegram(session, brief)

# ─── MAIN SIGNAL ──────────────────────────────────────────
def compute_signal(bars_1m, bars_5m, bars_15m):
    if len(bars_5m)<20: return None
    closes=[b['c'] for b in bars_5m]; price=closes[-1]
    vwap=calc_vwap(bars_5m); vwap_n,vwap_p=vwap[-1],vwap[-2]
    rsi=calc_rsi(closes); ema9=calc_ema(closes,9); ema21=calc_ema(closes,21)
    atr=calc_atr(bars_5m); vol_r=calc_vol_ratio(bars_5m)
    sup,res=get_sr(bars_5m); bb_u,bb_m,bb_l=calc_bb(closes)

    trend_15m="NEUTRAL"
    if len(bars_15m)>=21:
        c15=[b['c'] for b in bars_15m]; e9,e21=calc_ema(c15,9),calc_ema(c15,21)
        if e9 and e21: trend_15m="BULL" if e9>e21 else "BEAR"

    mom_1m="NEUTRAL"
    if len(bars_1m)>=5:
        c1=[b['c'] for b in bars_1m[-5:]]
        mom_1m="UP" if c1[-1]>c1[0] else "DOWN"

    above=price>vwap_n; ca=price>vwap_n and closes[-2]<=vwap_p; cb=price<vwap_n and closes[-2]>=vwap_p
    hv=vol_r and vol_r>=1.5; eb=ema9>ema21 if ema9 and ema21 else False

    bs=ss=0
    if ca: bs+=3
    elif above: bs+=1
    if cb: ss+=3
    elif not above: ss+=1
    if hv:
        if above: bs+=2
        else: ss+=2
    elif vol_r and vol_r>=1.2:
        if above: bs+=1
        else: ss+=1
    if rsi:
        if rsi<35: bs+=2
        elif rsi<50 and above: bs+=1
        if rsi>65: ss+=2
        elif rsi>50 and not above: ss+=1
    if eb: bs+=1
    else: ss+=1
    if trend_15m=="BULL": bs+=2
    if trend_15m=="BEAR": ss+=2
    if mom_1m=="UP": bs+=1
    if mom_1m=="DOWN": ss+=1
    if bb_l and price<=bb_l: bs+=1
    if bb_u and price>=bb_u: ss+=1

    if bs>=MIN_SCORE: signal="BUY"; conf="HIGH" if bs>=7 else "MEDIUM"
    elif ss>=MIN_SCORE: signal="SELL"; conf="HIGH" if ss>=7 else "MEDIUM"
    else: return None

    if atr:
        sl=round(price-atr*1.5,4) if signal=="BUY" else round(price+atr*1.5,4)
        tp=round(price+atr*3.0,4) if signal=="BUY" else round(price-atr*3.0,4)
    else:
        sl=round(min(vwap_n,sup)*0.995,4) if signal=="BUY" else round(max(vwap_n,res)*1.005,4)
        tp=round(res*0.998,4) if signal=="BUY" else round(sup*1.002,4)

    risk=abs(price-sl); reward=abs(tp-price)
    rr=f"1:{round(reward/risk,1)}" if risk>0 else "N/A"

    return {"signal":signal,"confidence":conf,"entry":round(price,4),"sl":sl,"tp":tp,"rr":rr,
            "vwap":vwap_n,"rsi":rsi,"ema9":ema9,"ema21":ema21,"atr":atr,"vol_ratio":vol_r,
            "trend":"UPTREND" if eb else "DOWNTREND","trend_15m":trend_15m,"momentum_1m":mom_1m,
            "bb_upper":bb_u,"bb_lower":bb_l,"buy_score":bs,"sell_score":ss,
            "support":sup,"resistance":res,"bb_squeeze":(bb_u-bb_l)<atr*2 if bb_u and bb_l and atr else False}

# ─── FORMAT MESSAGE ───────────────────────────────────────
def format_signal(ticker, result, yahoo_price, yahoo_change, status, sentiment, headlines, patterns, ai, orb_tag=""):
    sig=result["signal"]; now=get_ny_time().strftime("%H:%M:%S")+" NY"
    header=f"🟢 <b>BUY — {ticker}</b>" if sig=="BUY" else f"🔴 <b>SELL — {ticker}</b>"
    if orb_tag: header+=" 🚀 <b>ORB!</b>"
    ce="🔥" if result["confidence"]=="HIGH" else "⚡"
    t15e="📈" if result["trend_15m"]=="BULL" else "📉" if result["trend_15m"]=="BEAR" else "➡️"
    m1e="⬆️" if result["momentum_1m"]=="UP" else "⬇️"
    se="✅" if sentiment=="POSITIVE" else "⚠️" if sentiment=="NEGATIVE" else "➖"

    yline=f"📱 <b>Live Price:</b>  <b>${yahoo_price}</b>  {'📈' if yahoo_change>=0 else '📉'} {'+' if yahoo_change>=0 else ''}{yahoo_change}% today" if yahoo_price else f"📱 <b>Price:</b>  ${result['entry']}"
    shares,cost=calc_position_size(result['entry'],result['sl'])
    score=result['buy_score'] if sig=="BUY" else result['sell_score']

    # AI verdict
    verdict=ai.get("verdict","CONFIRMED")
    ve="✅" if verdict=="CONFIRMED" else "🚫" if verdict=="REJECTED" else "⚠️"
    ai_line=f"\n{ve} <b>AI:</b> {verdict} — {ai.get('reason','')}\n💡 <b>Tip:</b> {ai.get('tip','')}"

    # Patterns
    pat_line="\n🕯 <b>Patterns:</b> "+", ".join(patterns) if patterns else ""
    sq_line="\n🔔 <b>BB Squeeze — big move incoming!</b>" if result.get("bb_squeeze") else ""
    news_line=f"\n📰 {se} <b>News:</b> {sentiment} — <i>{headlines[0][:55]}...</i>" if headlines else ""

    return f"""{header}  {ce} {result['confidence']}
{'🟢 MARKET OPEN' if status=='OPEN' else '🌅 PRE-MARKET'}{sq_line}{pat_line}{ai_line}{news_line}

{yline}
━━━━━━━━━━━━━━━━━━━━━
🎯 <b>Entry:</b>  ${result['entry']}
🛑 <b>Stop Loss:</b>  ${result['sl']}
✅ <b>Take Profit:</b>  ${result['tp']}
⚖️ <b>Risk/Reward:</b>  {result['rr']}
📐 <b>ATR:</b>  ${result['atr']}
━━━━━━━━━━━━━━━━━━━━━
💼 <b>Position:</b>  {shares} shares  (${cost})
💰 <b>Risk:</b>  ${round(ACCOUNT_SIZE*RISK_PER_TRADE_PCT/100,2)} ({RISK_PER_TRADE_PCT}% of ${ACCOUNT_SIZE})
━━━━━━━━━━━━━━━━━━━━━
📊 VWAP: ${result['vwap']}
📉 RSI: {result['rsi']}  |  📈 EMA: {result['ema9']}/{result['ema21']}
🔊 Vol: {result['vol_ratio']}x  |  {t15e} 15m: {result['trend_15m']}
{m1e} 1m: {result['momentum_1m']}  |  🏆 Score: {score}/12
━━━━━━━━━━━━━━━━━━━━━
📊 RVOL: {result.get('rvol','N/A')}x vs yesterday
🗷 S: ${result['support']}  R: ${result['resistance']}
{"\n📅 Pivot: $"+str(result['multiday']['pivot'])+"  PrevH: $"+str(result['multiday']['prev_high'])+"  PrevL: $"+str(result['multiday']['prev_low']) if result.get('multiday') else ""}
🕐 {now}"""

# ─── DAILY REPORT ─────────────────────────────────────────
async def send_daily_report(session):
    if not performance_log:
        await send_telegram(session,"📊 <b>Daily Report</b>\n\nNo completed trades today."); return
    wins=[t for t in performance_log if t['pnl']>0]
    total_pnl=round(sum(t['pnl'] for t in performance_log),2)
    wr=round(len(wins)/len(performance_log)*100,1)
    msg=f"📊 <b>Daily Report</b>\n\n✅ Wins: {len(wins)}\n❌ Losses: {len(performance_log)-len(wins)}\n🎯 Win Rate: {wr}%\n💰 P&L: {'+' if total_pnl>0 else ''}{total_pnl}%\n\n"
    for t in performance_log[-10:]:
        msg+=f"{'✅' if t['pnl']>0 else '❌'} {t['ticker']} {t['signal']} → {'+' if t['pnl']>0 else ''}{t['pnl']}%\n"
    await send_telegram(session,msg); performance_log.clear()

# ─── COMMAND HANDLER ──────────────────────────────────────
async def handle_commands(session, offset):
    global bot_paused, watchlist, ACCOUNT_SIZE
    updates = await get_telegram_updates(session, offset+1)
    for update in updates:
        offset = update["update_id"]
        text = update.get("message",{}).get("text","").strip()
        if not text: continue
        print(f"  📩 Cmd: {text}")

        if text=="/status":
            ny=get_ny_time().strftime("%H:%M:%S")
            al="\n".join([f"  {t}: {v['signal']} @ ${v['entry']}" for t,v in active_trades.items()]) or "  None"
            await send_telegram(session,f"📡 <b>Status</b>\n\n⏰ {ny} NY\n📊 Market: {market_status()}\n{'⏸ PAUSED' if bot_paused else '▶️ RUNNING'}\n👀 Watching: {', '.join(watchlist)}\n💼 Active:\n{al}")
        elif text.startswith("/add "):
            t=text.split()[1].upper()
            if t not in watchlist: watchlist.append(t); await send_telegram(session,f"✅ Added {t}!\n📊 {', '.join(watchlist)}")
            else: await send_telegram(session,f"⚠️ {t} already watching.")
        elif text.startswith("/remove "):
            t=text.split()[1].upper()
            if t in watchlist: watchlist.remove(t); await send_telegram(session,f"✅ Removed {t}\n📊 {', '.join(watchlist)}")
            else: await send_telegram(session,f"⚠️ {t} not found.")
        elif text=="/watchlist":
            await send_telegram(session,f"👀 <b>Watchlist:</b>\n{', '.join(watchlist)}\n\n/add TICKER or /remove TICKER")
        elif text=="/pause": bot_paused=True; await send_telegram(session,"⏸ Bot paused. /resume to restart.")
        elif text=="/resume": bot_paused=False; await send_telegram(session,"▶️ Bot resumed!")
        elif text=="/report": await send_daily_report(session)
        elif text.startswith("/risk "):
            try: ACCOUNT_SIZE=float(text.split()[1]); await send_telegram(session,f"💰 Account: ${ACCOUNT_SIZE}\nRisk/trade: ${round(ACCOUNT_SIZE*RISK_PER_TRADE_PCT/100,2)}")
            except: await send_telegram(session,"⚠️ Usage: /risk 5000")
        elif text.startswith("/alert "):
            try:
                parts=text.split(); ticker=parts[1].upper(); target=float(parts[2])
                p,_,_=await get_yahoo_price(session,ticker)
                direction="above" if p and target>p else "below"
                if ticker not in price_alerts: price_alerts[ticker]=[]
                price_alerts[ticker].append({"price":target,"direction":direction})
                await send_telegram(session,f"🔔 Alert set!\n{ticker} will notify when price goes {direction} ${target}\nCurrent: ${p}")
            except: await send_telegram(session,"⚠️ Usage: /alert RGTI 25.00")
        elif text=="/alerts":
            if not price_alerts or all(not v for v in price_alerts.values()):
                await send_telegram(session,"📭 No active price alerts.\n\n/alert TICKER PRICE to set one.")
            else:
                msg="🔔 <b>Active Price Alerts:</b>\n\n"
                for t,als in price_alerts.items():
                    for a in als: msg+=f"  {t}: {a['direction']} ${a['price']}\n"
                await send_telegram(session,msg)
        elif text=="/brief": await send_morning_brief(session)
        elif text=="/help":
            await send_telegram(session,
                "🤖 <b>AlphaSignal Commands</b>\n\n"
                "/status — bot status\n"
                "/watchlist — current tickers\n"
                "/add AAPL — add ticker\n"
                "/remove RGTI — remove ticker\n"
                "/alert RGTI 25.00 — price alert\n"
                "/alerts — show active alerts\n"
                "/pause — pause bot\n"
                "/resume — resume bot\n"
                "/report — daily P&L\n"
                "/risk 5000 — set account size\n"
                "/brief — morning market brief\n"
                "/help — this menu"
            )
    return offset

# ─── MAIN ─────────────────────────────────────────────────
async def main():
    global last_update_id, morning_brief_sent
    print("🚀 AlphaSignal ULTIMATE+ Bot starting...")
    last_report_day=-1

    async with aiohttp.ClientSession() as session:
        await send_telegram(session,
            "🤖 <b>AlphaSignal ULTIMATE+ Bot!</b>\n\n"
            f"📊 Watching: {', '.join(watchlist)}\n"
            "🧠 AI Signal Confirmation (Groq)\n"
            "🕯 Candlestick Pattern Recognition\n"
            "🌅 Morning Market Brief at 9:25 AM\n"
            "🔔 Custom Price Alerts (/alert)\n"
            "📐 VWAP+RSI+EMA+ATR+BB+ORB\n"
            "📈 Trailing Stops | 💼 Position Sizing\n"
            "📰 News Sentiment Filter\n"
            "🔄 Scanning every 60 seconds\n\n"
            "📩 Type /help for all commands 🚀"
        )

        while True:
            last_update_id = await handle_commands(session, last_update_id)
            status = market_status()
            ny = get_ny_time()

            # Morning brief at 9:25 AM
            if ny.hour==9 and ny.minute==25 and not morning_brief_sent and ny.weekday()<5:
                await send_morning_brief(session)
                morning_brief_sent=True
            if ny.hour==9 and ny.minute==26:
                morning_brief_sent=False

            # Daily report at 4:05 PM
            if ny.hour==16 and ny.minute==5 and ny.day!=last_report_day:
                await send_daily_report(session); last_report_day=ny.day

            if status=="CLOSED":
                await asyncio.sleep(300); continue
            if bot_paused:
                await asyncio.sleep(30); continue

            # Check trailing stops & price alerts
            if active_trades: await check_trailing_stops(session)
            if price_alerts:  await check_price_alerts(session)

            print(f"\n[{ny.strftime('%H:%M:%S')} NY] {status} | {watchlist}")

            for ticker in list(watchlist):
                try:
                    print(f"  📡 {ticker}...")
                    bars_1m,bars_5m,bars_15m,bars_daily,bars_yest = await asyncio.gather(
                        get_bars(session,ticker,"1Min",30),
                        get_bars(session,ticker,"5Min",50),
                        get_bars(session,ticker,"15Min",50),
                        get_bars(session,ticker,"1Day",5),
                        get_bars(session,ticker,"1Day",3),
                    )
                    if len(bars_5m)<20: print(f"    ⚠️ Not enough bars"); continue

                    update_orb(ticker,bars_1m)
                    rvol = calc_rvol(bars_1m, bars_yest)
                    multiday = get_multiday_levels(bars_daily)
                    if result: result["rvol"] = rvol
                    if result and multiday: result["multiday"] = multiday
                    result=compute_signal(bars_1m,bars_5m,bars_15m)
                    if result is None: print(f"    ⏳ WAIT"); continue
                    if is_duplicate(ticker,result["signal"]): print(f"    🔄 Duplicate"); continue

                    orb_tag=check_orb_signal(ticker,result["entry"]) or ""
                    patterns=detect_patterns(bars_5m)
                    sentiment,headlines=await get_news_sentiment(session,ticker)

                    # Block bad news on BUY
                    if sentiment=="NEGATIVE" and result["signal"]=="BUY":
                        await send_telegram(session,f"⚠️ <b>{ticker} BUY blocked</b> — negative news!\n📰 {headlines[0][:80] if headlines else ''}")
                        continue

                    # AI confirmation
                    ai=await ai_confirm_signal(session,ticker,result,patterns,sentiment)
                    if ai.get("verdict")=="REJECTED":
                        await send_telegram(session,f"🚫 <b>AI Rejected {ticker} {result['signal']}</b>\n\n❌ {ai.get('reason','')}\n\n⏳ Waiting for better setup...")
                        continue

                    yahoo_price,yahoo_change,_=await get_yahoo_price(session,ticker)
                    score=result['buy_score'] if result['signal']=='BUY' else result['sell_score']
                    print(f"    ✅ {result['signal']} Score:{score}/12 AI:{ai.get('verdict')} News:{sentiment}")

                    msg=format_signal(ticker,result,yahoo_price,yahoo_change,status,sentiment,headlines,patterns,ai,orb_tag)
                    await send_telegram(session,msg)

                    active_trades[ticker]={"signal":result["signal"],"entry":result["entry"],"sl":result["sl"],"tp":result["tp"],"atr":result["atr"],"time":ny.strftime("%H:%M")}
                    trailing_stops[ticker]=result["sl"]
                    last_signal_time[ticker]=datetime.now()
                    last_signal_type[ticker]=result["signal"]
                    await asyncio.sleep(2)

                except Exception as e:
                    print(f"    ❌ {ticker}: {e}"); continue

            await asyncio.sleep(SCAN_INTERVAL)

if __name__=="__main__":
    asyncio.run(main())
