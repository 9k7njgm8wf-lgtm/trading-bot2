import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

# CONFIG
GROQ_API_KEY     = "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"
ALPACA_API_KEY   = "AKPYM4PLBJBOEBSD3NQQVEDALI"
ALPACA_SECRET    = "6Z73eeNG8Fpa64Tw7UBaEULCyFHJRCSh6r3kRgC7k2qo"
TELEGRAM_TOKEN   = "8855798705:AAFhs2RYnLUVxR-N2C2urTzl445NZn2fxv8"
TELEGRAM_CHAT_ID = "6903579390"

TICKERS            = ["RGTI", "RXT", "QUBT", "LUNR"]
SCAN_INTERVAL      = 60
SIGNAL_COOLDOWN    = 900
MIN_SCORE          = 7
ACCOUNT_SIZE       = 1000
RISK_PER_TRADE_PCT = 2
BEST_HOURS         = [(9,30,11,0),(15,0,16,0)]
SPY_FILTER         = True
TIME_FILTER        = True

ALPACA_BASE    = "https://data.alpaca.markets/v2"
ALPACA_HEADERS = {"APCA-API-KEY-ID": ALPACA_API_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET}

# STATE
last_signal_time   = {}
last_signal_type   = {}
orb_levels         = {}
active_trades      = {}
performance_log    = []
watchlist          = list(TICKERS)
bot_paused         = False
trailing_stops     = {}
last_update_id     = 0
price_alerts       = {}
morning_brief_sent = False

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

def is_best_trading_hour():
    ny = get_ny_time()
    t = ny.hour*60 + ny.minute
    for sh,sm,eh,em in BEST_HOURS:
        if sh*60+sm <= t <= eh*60+em: return True
    return False

def get_time_session():
    ny = get_ny_time()
    t = ny.hour*60 + ny.minute
    if 570 <= t <= 660:  return "OPEN 9:30-11am BEST"
    if 660 <= t <= 840:  return "MIDDAY 11am-2pm CHOPPY"
    if 840 <= t <= 900:  return "AFTERNOON 2-3pm OK"
    if 900 <= t <= 960:  return "POWER HOUR 3-4pm BEST"
    return "PRE/AFTER MARKET"

def is_duplicate(ticker, signal):
    if ticker not in last_signal_time: return False
    elapsed = (datetime.now() - last_signal_time[ticker]).total_seconds()
    return elapsed < SIGNAL_COOLDOWN and last_signal_type.get(ticker) == signal

async def send_telegram(session, message):
    url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage"
    try:
        async with session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}) as r:
            res = await r.json()
            if not res.get("ok"): print("TG error:", res)
    except Exception as e:
        print("TG error:", e)

async def get_telegram_updates(session, offset=0):
    url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/getUpdates"
    try:
        async with session.get(url, params={"offset": offset, "timeout": 1}) as r:
            return (await r.json()).get("result", [])
    except: return []

async def get_yahoo_price(session, ticker):
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/" + ticker
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json()
            meta = data["chart"]["result"][0]["meta"]
            price = meta["regularMarketPrice"]
            prev  = meta["chartPreviousClose"]
            vol   = meta.get("regularMarketVolume", 0)
            return round(price,4), round(((price-prev)/prev)*100,2), vol
    except: return None, None, None

async def get_spy_trend(session):
    try:
        async with session.get("https://query1.finance.yahoo.com/v8/finance/chart/SPY",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json()
            meta = data["chart"]["result"][0]["meta"]
            price = meta["regularMarketPrice"]
            prev  = meta["chartPreviousClose"]
            chg   = round(((price-prev)/prev)*100, 2)
            return ("BULL" if chg>=0 else "BEAR"), chg, round(price,2)
    except: return "NEUTRAL", 0, 0

async def get_news_sentiment(session, ticker):
    try:
        async with session.get("https://query1.finance.yahoo.com/v1/finance/search",
            params={"q": ticker, "newsCount": 3}, headers={"User-Agent": "Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json()
            news = data.get("news", [])
            if not news: return "NEUTRAL", []
            headlines = [n.get("title","") for n in news[:3]]
            neg = ["downgrade","loss","miss","decline","drop","fail","lawsuit","sec","fraud","warning","cut","crash"]
            pos = ["upgrade","beat","surge","rally","buy","bullish","growth","profit","deal","launch","record"]
            nc = sum(1 for h in headlines for w in neg if w in h.lower())
            pc = sum(1 for h in headlines for w in pos if w in h.lower())
            return ("NEGATIVE" if nc>pc else "POSITIVE" if pc>nc else "NEUTRAL"), headlines
    except: return "NEUTRAL", []

async def get_bars(session, ticker, timeframe="5Min", limit=50):
    try:
        async with session.get(ALPACA_BASE+"/stocks/"+ticker+"/bars",
            headers=ALPACA_HEADERS,
            params={"timeframe": timeframe, "limit": limit, "feed": "sip"}) as r:
            return (await r.json()).get("bars", [])
    except: return []

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

def calc_ema(vals, p):
    if len(vals)<p: return None
    k,e=2/(p+1),sum(vals[:p])/p
    for v in vals[p:]: e=v*k+e*(1-k)
    return round(e,4)

def calc_atr(bars, p=14):
    if len(bars)<p+1: return None
    trs=[max(bars[i]['h']-bars[i]['l'],abs(bars[i]['h']-bars[i-1]['c']),abs(bars[i]['l']-bars[i-1]['c'])) for i in range(1,len(bars))]
    return round(sum(trs[-p:])/p,4)

def calc_bb(closes, p=20):
    if len(closes)<p: return None,None,None
    sma=sum(closes[-p:])/p; std=(sum((c-sma)**2 for c in closes[-p:])/p)**0.5
    return round(sma+2*std,4),round(sma,4),round(sma-2*std,4)

def calc_vol_ratio(bars):
    if len(bars)<10: return None
    v=[b['v'] for b in bars]; avg=sum(v[:-1])/len(v[:-1])
    return round(v[-1]/avg if avg>0 else 0,2)

def calc_rvol(bars_today, bars_yesterday):
    if not bars_today or not bars_yesterday: return None
    tv=sum(b['v'] for b in bars_today); yv=sum(b['v'] for b in bars_yesterday[:len(bars_today)])
    return round(tv/yv if yv>0 else 0,2)

def get_multiday_levels(bars_daily):
    if len(bars_daily)<2: return None
    p=bars_daily[-2]; ph,pl,pc=round(p['h'],4),round(p['l'],4),round(p['c'],4)
    pivot=round((ph+pl+pc)/3,4)
    return {"prev_high":ph,"prev_low":pl,"prev_close":pc,"pivot":pivot,"r1":round(2*pivot-pl,4),"s1":round(2*pivot-ph,4)}

def get_sr(bars, n=20):
    r=bars[-n:]
    return round(min(b['l'] for b in r),4),round(max(b['h'] for b in r),4)

def detect_patterns(bars):
    patterns=[]
    if len(bars)<3: return patterns
    b0,b1,b2=bars[-3],bars[-2],bars[-1]
    o2,h2,l2,c2=b2['o'],b2['h'],b2['l'],b2['c']
    o1,c1=b1['o'],b1['c']
    o0,c0=b0['o'],b0['c']
    body2=abs(c2-o2); range2=h2-l2; body1=abs(c1-o1)
    if range2>0 and body2/range2<0.1: patterns.append("Doji")
    if body2>0 and (l2<min(o2,c2)) and (min(o2,c2)-l2)>2*body2 and c2>o2: patterns.append("Hammer (Bullish)")
    if body2>0 and (h2>max(o2,c2)) and (h2-max(o2,c2))>2*body2 and c2<o2: patterns.append("Shooting Star (Bearish)")
    if c1<o1 and c2>o2 and c2>o1 and o2<c1: patterns.append("Bullish Engulfing")
    if c1>o1 and c2<o2 and c2<o1 and o2>c1: patterns.append("Bearish Engulfing")
    if c0<o0 and body1<abs(c0-o0)*0.5 and c2>o2 and c2>(o0+c0)/2: patterns.append("Morning Star (Bullish)")
    if c0>o0 and body1<abs(c0-o0)*0.5 and c2<o2 and c2<(o0+c0)/2: patterns.append("Evening Star (Bearish)")
    if range2>0 and body2/range2>0.9:
        patterns.append("Bullish Marubozu" if c2>o2 else "Bearish Marubozu")
    return patterns

def update_orb(ticker, bars_1m):
    ny=get_ny_time()
    if ny.hour==9 and ny.minute==30: orb_levels[ticker]={"high":None,"low":None,"set":False}
    if not orb_levels.get(ticker): orb_levels[ticker]={"high":None,"low":None,"set":False}
    ob=[b for b in bars_1m if '09:3' in b.get('t','') or '09:4' in b.get('t','')]
    if ob:
        orb_levels[ticker]["high"]=round(max(b['h'] for b in ob),4)
        orb_levels[ticker]["low"]=round(min(b['l'] for b in ob),4)
        orb_levels[ticker]["set"]=True

def check_orb(ticker, price):
    orb=orb_levels.get(ticker,{})
    if not orb.get("set"): return None
    if price>orb["high"]*1.001: return "BUY_ORB"
    if price<orb["low"]*0.999: return "SELL_ORB"
    return None

def calc_position_size(entry, sl):
    risk=ACCOUNT_SIZE*(RISK_PER_TRADE_PCT/100); rps=abs(entry-sl)
    if rps<=0: return 0,0
    shares=int(risk/rps)
    return shares, round(shares*entry,2)

async def check_trailing_stops(session):
    for ticker in list(active_trades.keys()):
        trade=active_trades[ticker]
        yp,_,_=await get_yahoo_price(session,ticker)
        if not yp: continue
        entry=trade["entry"]; sig=trade["signal"]; atr=trade.get("atr",abs(entry-trade["sl"]))
        if sig=="BUY":
            nt=round(yp-atr*1.5,4); ct=trailing_stops.get(ticker,trade["sl"])
            if nt>ct:
                trailing_stops[ticker]=nt
                await send_telegram(session,"Trailing SL updated "+ticker+": $"+str(nt))
            if yp<=trailing_stops.get(ticker,trade["sl"]):
                pnl=round((yp-entry)/entry*100,2)
                await send_telegram(session,"TRAILING STOP HIT "+ticker+" Exit:$"+str(yp)+" P&L:"+str(pnl)+"%")
                performance_log.append({"ticker":ticker,"signal":sig,"entry":entry,"exit":yp,"pnl":pnl,"time":get_ny_time().strftime("%H:%M")})
                del active_trades[ticker]
                if ticker in trailing_stops: del trailing_stops[ticker]

async def check_price_alerts(session):
    for ticker in list(price_alerts.keys()):
        alerts=price_alerts[ticker]
        if not alerts: continue
        yp,_,_=await get_yahoo_price(session,ticker)
        if not yp: continue
        remaining=[]
        for alert in alerts:
            target=alert["price"]; direction=alert["direction"]
            if direction=="above" and yp>=target:
                await send_telegram(session,"PRICE ALERT "+ticker+" hit $"+str(yp)+" (target "+direction+" $"+str(target)+")")
            elif direction=="below" and yp<=target:
                await send_telegram(session,"PRICE ALERT "+ticker+" hit $"+str(yp)+" (target "+direction+" $"+str(target)+")")
            else: remaining.append(alert)
        price_alerts[ticker]=remaining

async def ai_confirm_signal(session, ticker, result, patterns, sentiment):
    pattern_str=", ".join(patterns) if patterns else "None"
    prompt = ("Analyze trade signal for "+ticker+". Signal:"+result['signal']+
              " Entry:$"+str(result['entry'])+" SL:$"+str(result['sl'])+" TP:$"+str(result['tp'])+
              " RSI:"+str(result['rsi'])+" VWAP:$"+str(result['vwap'])+
              " Volume:"+str(result['vol_ratio'])+"x Score:"+str(result.get('buy_score',result.get('sell_score')))+"/12"+
              " Patterns:"+pattern_str+" News:"+sentiment+
              " 15mTrend:"+result['trend_15m']+" SPY:"+result.get('spy_trend','N/A')+
              " Respond ONLY JSON no markdown: {\"verdict\":\"CONFIRMED\" or \"REJECTED\" or \"CAUTION\",\"reason\":\"one sentence\",\"tip\":\"one tip\"}")
    try:
        async with session.post("https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization":"Bearer "+GROQ_API_KEY,"Content-Type":"application/json"},
            json={"model":"llama-3.3-70b-versatile","messages":[{"role":"user","content":prompt}],"max_tokens":150,"temperature":0.1},
            timeout=aiohttp.ClientTimeout(total=10)) as r:
            data=await r.json()
            raw=data["choices"][0]["message"]["content"].strip().replace("```json","").replace("```","")
            return json.loads(raw)
    except Exception as e:
        print("AI error:",e)
        return {"verdict":"CONFIRMED","reason":"AI unavailable","tip":"Use your judgment"}

async def run_backtest(session, ticker):
    bars=await get_bars(session,ticker,"1Day",35)
    if len(bars)<15: return None
    wins=losses=0; total_pnl=0; trades=[]
    for i in range(14,len(bars)-1):
        seg=bars[max(0,i-20):i+1]; closes=[b['c'] for b in seg]; price=closes[-1]
        ema9=calc_ema(closes,9); ema21=calc_ema(closes,21); rsi=calc_rsi(closes)
        vwap=calc_vwap(seg)[-1]; atr=calc_atr(seg)
        if not all([ema9,ema21,rsi,atr]): continue
        if ema9>ema21 and rsi<50 and price>vwap: signal="BUY"
        elif ema9<ema21 and rsi>50 and price<vwap: signal="SELL"
        else: continue
        nb=bars[i+1]
        pnl=round((nb['c']-price)/price*100,2) if signal=="BUY" else round((price-nb['c'])/price*100,2)
        if pnl>0: wins+=1
        else: losses+=1
        total_pnl+=pnl; trades.append(pnl)
    total=wins+losses
    if total==0: return None
    return {"ticker":ticker,"total_trades":total,"wins":wins,"losses":losses,
            "win_rate":round(wins/total*100,1),"avg_pnl":round(total_pnl/total,2),
            "total_pnl":round(total_pnl,2),"best":round(max(trades),2),"worst":round(min(trades),2)}

def compute_signal(bars_1m, bars_5m, bars_15m):
    if len(bars_5m)<20: return None
    closes=[b['c'] for b in bars_5m]; price=closes[-1]
    vwap=calc_vwap(bars_5m); vn,vp=vwap[-1],vwap[-2]
    rsi=calc_rsi(closes); ema9=calc_ema(closes,9); ema21=calc_ema(closes,21)
    atr=calc_atr(bars_5m); vol_r=calc_vol_ratio(bars_5m)
    sup,res=get_sr(bars_5m); bbu,bbm,bbl=calc_bb(closes)
    trend_15m="NEUTRAL"
    if len(bars_15m)>=21:
        c15=[b['c'] for b in bars_15m]; e9,e21=calc_ema(c15,9),calc_ema(c15,21)
        if e9 and e21: trend_15m="BULL" if e9>e21 else "BEAR"
    mom_1m="NEUTRAL"
    if len(bars_1m)>=5:
        c1=[b['c'] for b in bars_1m[-5:]]
        mom_1m="UP" if c1[-1]>c1[0] else "DOWN"
    above=price>vn; ca=price>vn and closes[-2]<=vp; cb=price<vn and closes[-2]>=vp
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
    if bbl and price<=bbl: bs+=1
    if bbu and price>=bbu: ss+=1
    if bs>=MIN_SCORE: signal="BUY"; conf="HIGH" if bs>=9 else "MEDIUM"
    elif ss>=MIN_SCORE: signal="SELL"; conf="HIGH" if ss>=9 else "MEDIUM"
    else: return None
    if atr:
        sl=round(price-atr*1.5,4) if signal=="BUY" else round(price+atr*1.5,4)
        tp=round(price+atr*3.0,4) if signal=="BUY" else round(price-atr*3.0,4)
    else:
        sl=round(min(vn,sup)*0.995,4) if signal=="BUY" else round(max(vn,res)*1.005,4)
        tp=round(res*0.998,4) if signal=="BUY" else round(sup*1.002,4)
    risk=abs(price-sl); reward=abs(tp-price)
    rr="1:"+str(round(reward/risk,1)) if risk>0 else "N/A"
    return {"signal":signal,"confidence":conf,"entry":round(price,4),"sl":sl,"tp":tp,"rr":rr,
            "vwap":vn,"rsi":rsi,"ema9":ema9,"ema21":ema21,"atr":atr,"vol_ratio":vol_r,
            "trend":"UPTREND" if eb else "DOWNTREND","trend_15m":trend_15m,"momentum_1m":mom_1m,
            "bb_upper":bbu,"bb_lower":bbl,"buy_score":bs,"sell_score":ss,
            "support":sup,"resistance":res,"bb_squeeze":(bbu-bbl)<atr*2 if bbu and bbl and atr else False}

def format_signal(ticker, result, yahoo_price, yahoo_change, status, sentiment, headlines, patterns, ai, rvol, multiday, spy_trend, spy_chg, orb_tag=""):
    sig=result["signal"]; now=get_ny_time().strftime("%H:%M:%S")+" NY"
    header=("BUY - "+ticker) if sig=="BUY" else ("SELL - "+ticker)
    if orb_tag: header+=" ORB BREAKOUT"
    conf=result["confidence"]; score=result['buy_score'] if sig=="BUY" else result['sell_score']
    verdict=ai.get("verdict","CONFIRMED")
    ve="AI-OK" if verdict=="CONFIRMED" else "AI-REJECTED" if verdict=="REJECTED" else "AI-CAUTION"
    pat_str=", ".join(patterns) if patterns else "None"
    shares,cost=calc_position_size(result['entry'],result['sl'])
    spy_str="SPY "+spy_trend+" "+("+" if spy_chg>=0 else "")+str(spy_chg)+"%"
    news_str="News "+sentiment+": "+(headlines[0][:50] if headlines else "")
    session_str=get_time_session()
    md_str=""
    if multiday:
        md_str="\nPivot:$"+str(multiday['pivot'])+" PrevH:$"+str(multiday['prev_high'])+" PrevL:$"+str(multiday['prev_low'])
    rvol_str=str(rvol)+"x vs yesterday" if rvol else "N/A"
    sq_str="\nBB SQUEEZE - big move incoming!" if result.get("bb_squeeze") else ""
    lines=[
        header+"  "+conf,
        ("MARKET OPEN" if status=="OPEN" else "PRE-MARKET")+" | "+session_str,
        spy_str+sq_str,
        "",
        "Live Price: $"+str(yahoo_price)+" "+("+"+str(yahoo_change) if yahoo_change and yahoo_change>=0 else str(yahoo_change))+"% today" if yahoo_price else "Price: $"+str(result['entry']),
        "",
        "Entry:  $"+str(result['entry']),
        "Stop Loss:  $"+str(result['sl']),
        "Take Profit:  $"+str(result['tp']),
        "Risk/Reward:  "+result['rr']+"  |  ATR: $"+str(result['atr']),
        "",
        "Position: "+str(shares)+" shares ($"+str(cost)+")",
        "Risk: $"+str(round(ACCOUNT_SIZE*RISK_PER_TRADE_PCT/100,2))+" ("+str(RISK_PER_TRADE_PCT)+"% of $"+str(ACCOUNT_SIZE)+")",
        "",
        "VWAP: $"+str(result['vwap'])+"  RSI: "+str(result['rsi']),
        "EMA 9/21: "+str(result['ema9'])+"/"+str(result['ema21']),
        "Volume: "+str(result['vol_ratio'])+"x  RVOL: "+rvol_str,
        "15m Trend: "+result['trend_15m']+"  1m Mom: "+result['momentum_1m'],
        "Score: "+str(score)+"/12"+md_str,
        "",
        ve+" - "+ai.get('reason',''),
        "Tip: "+ai.get('tip',''),
        "",
        "Patterns: "+pat_str,
        news_str,
        "",
        "S:$"+str(result['support'])+"  R:$"+str(result['resistance']),
        now
    ]
    return "\n".join(lines)

async def send_morning_brief(session):
    ny=get_ny_time().strftime("%A %B %d")
    lines=["Morning Brief - "+ny,""]
    spy_p,spy_c,_=await get_yahoo_price(session,"SPY")
    qqq_p,qqq_c,_=await get_yahoo_price(session,"QQQ")
    vix_p,vix_c,_=await get_yahoo_price(session,"^VIX")
    lines.append("Market Overview:")
    if spy_p: lines.append("  SPY: $"+str(spy_p)+" "+("+" if spy_c>=0 else "")+str(spy_c)+"%")
    if qqq_p: lines.append("  QQQ: $"+str(qqq_p)+" "+("+" if qqq_c>=0 else "")+str(qqq_c)+"%")
    if vix_p:
        vix_label="HIGH FEAR" if vix_p>25 else "ELEVATED" if vix_p>18 else "LOW/CALM"
        lines.append("  VIX: $"+str(vix_p)+" "+vix_label)
    lines.append("")
    lines.append("Your Watchlist:")
    for ticker in watchlist:
        p,c,_=await get_yahoo_price(session,ticker)
        if p: lines.append("  "+ticker+": $"+str(p)+" "+("+" if c and c>=0 else "")+str(c)+"%")
        await asyncio.sleep(0.5)
    lines.append("")
    lines.append("Market opens in ~5 minutes! Trade safe!")
    await send_telegram(session, "\n".join(lines))

async def send_daily_report(session):
    if not performance_log:
        await send_telegram(session,"Daily Report\n\nNo completed trades today."); return
    wins=[t for t in performance_log if t['pnl']>0]
    total_pnl=round(sum(t['pnl'] for t in performance_log),2)
    wr=round(len(wins)/len(performance_log)*100,1)
    lines=["Daily Report","","Wins: "+str(len(wins))+" Losses: "+str(len(performance_log)-len(wins)),
           "Win Rate: "+str(wr)+"%","Total P&L: "+str(total_pnl)+"%",""]
    for t in performance_log[-10:]:
        lines.append(("WIN" if t['pnl']>0 else "LOSS")+" "+t['ticker']+" "+t['signal']+" "+str(t['pnl'])+"%")
    await send_telegram(session, "\n".join(lines))
    performance_log.clear()

async def handle_commands(session, offset):
    global bot_paused, watchlist, ACCOUNT_SIZE
    updates=await get_telegram_updates(session, offset+1)
    for update in updates:
        offset=update["update_id"]
        text=update.get("message",{}).get("text","").strip()
        if not text: continue
        print("CMD:", text)
        if text=="/status":
            ny=get_ny_time().strftime("%H:%M:%S")
            al=", ".join([t+":"+v['signal']+"@$"+str(v['entry']) for t,v in active_trades.items()]) or "None"
            lines=["Bot Status","","Time: "+ny+" NY","Market: "+market_status(),
                   "Running" if not bot_paused else "PAUSED","Watching: "+", ".join(watchlist),
                   "Active Trades: "+al]
            await send_telegram(session, "\n".join(lines))
        elif text.startswith("/add "):
            t=text.split()[1].upper()
            if t not in watchlist: watchlist.append(t); await send_telegram(session,"Added "+t+"! Watching: "+", ".join(watchlist))
            else: await send_telegram(session,t+" already in watchlist")
        elif text.startswith("/remove "):
            t=text.split()[1].upper()
            if t in watchlist: watchlist.remove(t); await send_telegram(session,"Removed "+t+". Watching: "+", ".join(watchlist))
            else: await send_telegram(session,t+" not found")
        elif text=="/watchlist":
            await send_telegram(session,"Watchlist: "+", ".join(watchlist)+"\n\n/add TICKER or /remove TICKER")
        elif text=="/pause": bot_paused=True; await send_telegram(session,"Bot paused. /resume to restart.")
        elif text=="/resume": bot_paused=False; await send_telegram(session,"Bot resumed!")
        elif text=="/report": await send_daily_report(session)
        elif text.startswith("/risk "):
            try: ACCOUNT_SIZE=float(text.split()[1]); await send_telegram(session,"Account: $"+str(ACCOUNT_SIZE)+" Risk/trade: $"+str(round(ACCOUNT_SIZE*RISK_PER_TRADE_PCT/100,2)))
            except: await send_telegram(session,"Usage: /risk 5000")
        elif text.startswith("/alert "):
            try:
                parts=text.split(); ticker=parts[1].upper(); target=float(parts[2])
                p,_,_=await get_yahoo_price(session,ticker)
                direction="above" if p and target>p else "below"
                if ticker not in price_alerts: price_alerts[ticker]=[]
                price_alerts[ticker].append({"price":target,"direction":direction})
                await send_telegram(session,"Alert set! "+ticker+" will notify when "+direction+" $"+str(target)+" (now: $"+str(p)+")")
            except: await send_telegram(session,"Usage: /alert RGTI 25.00")
        elif text=="/alerts":
            if not any(price_alerts.values()): await send_telegram(session,"No active alerts.\n/alert TICKER PRICE to set one.")
            else:
                lines=["Active Alerts:"]
                for t,als in price_alerts.items():
                    for a in als: lines.append(t+": "+a['direction']+" $"+str(a['price']))
                await send_telegram(session, "\n".join(lines))
        elif text=="/brief": await send_morning_brief(session)
        elif text.startswith("/backtest"):
            parts=text.split()
            tickers_bt=[parts[1].upper()] if len(parts)>1 else list(watchlist)
            await send_telegram(session,"Running backtest on "+", ".join(tickers_bt)+"... (30 days)")
            for t in tickers_bt:
                bt=await run_backtest(session,t)
                if bt:
                    grade="GOOD" if bt['win_rate']>=55 else "OK" if bt['win_rate']>=45 else "POOR"
                    ps="+" if bt['total_pnl']>=0 else ""; avgs="+" if bt['avg_pnl']>=0 else ""
                    lines=["Backtest "+bt['ticker']+" - "+grade,
                           "Win Rate: "+str(bt['win_rate'])+"%",
                           "Trades: "+str(bt['total_trades'])+" | Wins: "+str(bt['wins'])+" | Losses: "+str(bt['losses']),
                           "Avg P&L: "+avgs+str(bt['avg_pnl'])+"%",
                           "Best: +"+str(bt['best'])+"% | Worst: "+str(bt['worst'])+"%",
                           "Total P&L: "+ps+str(bt['total_pnl'])+"%"]
                    await send_telegram(session, "\n".join(lines))
                else: await send_telegram(session,"Not enough data for "+t)
                await asyncio.sleep(1)
        elif text=="/filters":
            lines=["Active Filters","",
                   "Time Filter: "+("ON" if TIME_FILTER else "OFF")+" (9:30-11am, 3-4pm)",
                   "SPY Filter: "+("ON" if SPY_FILTER else "OFF"),
                   "Min Score: "+str(MIN_SCORE)+"/12","",
                   "Session: "+get_time_session(),
                   "Best time to trade!" if is_best_trading_hour() else "Outside best hours - waiting"]
            await send_telegram(session, "\n".join(lines))
        elif text=="/help":
            lines=["AlphaSignal Commands","",
                   "/status - bot status","/watchlist - tickers",
                   "/add AAPL - add ticker","/remove RGTI - remove",
                   "/alert RGTI 25.00 - price alert","/alerts - show alerts",
                   "/pause - pause bot","/resume - resume",
                   "/report - daily P&L","/risk 5000 - set account size",
                   "/brief - morning brief","/backtest - test 30 days",
                   "/backtest RGTI - test one stock","/filters - show filters","/help - this menu"]
            await send_telegram(session, "\n".join(lines))
    return offset

async def main():
    global last_update_id, morning_brief_sent
    print("AlphaSignal ULTIMATE+ starting...")
    last_report_day=-1

    async with aiohttp.ClientSession() as session:
        lines=["AlphaSignal ULTIMATE+ Bot Started!","",
               "Watching: "+", ".join(watchlist),
               "Strategy: VWAP+RSI+EMA+ATR+BB+ORB",
               "AI Confirmation: Groq LLaMA 3.3",
               "Candlestick Pattern Recognition",
               "SPY Trend Filter: ON",
               "Time Filter: Best hours only",
               "Min Score: "+str(MIN_SCORE)+"/12 (HIGH QUALITY ONLY)",
               "Backtest: /backtest",
               "Morning Brief: 9:25 AM NY",
               "Real-time SIP data feed",
               "",
               "Type /help for all commands"]
        await send_telegram(session, "\n".join(lines))

        while True:
            last_update_id=await handle_commands(session,last_update_id)
            status=market_status(); ny=get_ny_time()

            if ny.hour==9 and ny.minute==25 and not morning_brief_sent and ny.weekday()<5:
                await send_morning_brief(session); morning_brief_sent=True
            if ny.hour==9 and ny.minute==26: morning_brief_sent=False
            if ny.hour==16 and ny.minute==5 and ny.day!=last_report_day:
                await send_daily_report(session); last_report_day=ny.day

            if status=="CLOSED": await asyncio.sleep(300); continue
            if bot_paused: await asyncio.sleep(30); continue
            if active_trades: await check_trailing_stops(session)
            if price_alerts: await check_price_alerts(session)

            print("["+ny.strftime("%H:%M:%S")+" NY]", status, "|", watchlist)

            for ticker in list(watchlist):
                try:
                    print("  Analyzing", ticker, "...")
                    bars_1m,bars_5m,bars_15m,bars_daily,bars_yest=await asyncio.gather(
                        get_bars(session,ticker,"1Min",30),
                        get_bars(session,ticker,"5Min",50),
                        get_bars(session,ticker,"15Min",50),
                        get_bars(session,ticker,"1Day",5),
                        get_bars(session,ticker,"1Day",3),
                    )
                    if len(bars_5m)<20: print("    Not enough bars"); continue

                    if TIME_FILTER and not is_best_trading_hour():
                        print("    Outside best hours:", get_time_session()); continue

                    update_orb(ticker,bars_1m)
                    rvol=calc_rvol(bars_1m,bars_yest)
                    multiday=get_multiday_levels(bars_daily)
                    result=compute_signal(bars_1m,bars_5m,bars_15m)
                    if result is None: print("    WAIT"); continue
                    result["rvol"]=rvol
                    if multiday: result["multiday"]=multiday
                    if is_duplicate(ticker,result["signal"]): print("    Duplicate"); continue

                    spy_trend,spy_chg,spy_price=await get_spy_trend(session)
                    if SPY_FILTER:
                        if result["signal"]=="BUY" and spy_trend=="BEAR":
                            print("    BUY blocked - SPY red "+str(spy_chg)+"%"); continue
                        if result["signal"]=="SELL" and spy_trend=="BULL":
                            print("    SELL blocked - SPY green "+str(spy_chg)+"%"); continue
                    result["spy_trend"]=spy_trend; result["spy_change"]=spy_chg

                    orb_tag=check_orb(ticker,result["entry"]) or ""
                    patterns=detect_patterns(bars_5m)
                    sentiment,headlines=await get_news_sentiment(session,ticker)

                    if sentiment=="NEGATIVE" and result["signal"]=="BUY":
                        await send_telegram(session,"BUY blocked - negative news on "+ticker+": "+(headlines[0][:80] if headlines else "")); continue

                    ai=await ai_confirm_signal(session,ticker,result,patterns,sentiment)
                    if ai.get("verdict")=="REJECTED":
                        await send_telegram(session,"AI REJECTED "+ticker+" "+result["signal"]+": "+ai.get("reason","")); continue

                    yp,yc,_=await get_yahoo_price(session,ticker)
                    score=result['buy_score'] if result['signal']=='BUY' else result['sell_score']
                    print("    SIGNAL:",result['signal'],"Score:"+str(score)+"/12","AI:"+ai.get("verdict"),"SPY:"+spy_trend,"News:"+sentiment)

                    msg=format_signal(ticker,result,yp,yc,status,sentiment,headlines,patterns,ai,rvol,multiday,spy_trend,spy_chg,orb_tag)
                    await send_telegram(session,msg)
                    active_trades[ticker]={"signal":result["signal"],"entry":result["entry"],"sl":result["sl"],"tp":result["tp"],"atr":result["atr"],"time":ny.strftime("%H:%M")}
                    trailing_stops[ticker]=result["sl"]
                    last_signal_time[ticker]=datetime.now()
                    last_signal_type[ticker]=result["signal"]
                    await asyncio.sleep(2)

                except Exception as e:
                    print("    ERROR",ticker,":",e); continue

            await asyncio.sleep(SCAN_INTERVAL)

if __name__=="__main__":
    asyncio.run(main())
