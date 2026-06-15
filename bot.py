import asyncio
import aiohttp
import json
import time
from datetime import datetime, timezone, timedelta

# ── CONFIG ────────────────────────────────────────────────
FINNHUB_API_KEY  = "d8mpeg1r01qn3046mvtgd8mpeg1r01qn3046mvu0"
GROQ_API_KEY     = "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"
ALPACA_API_KEY   = "PKHY4LGTE2AF3PCURW4JJB423B"
ALPACA_SECRET    = "7NwLWDCxprL794BdsCheM9CB8D4VAi2GSqKTgfovv3Ws"
TELEGRAM_TOKEN   = "8855798705:AAHC0s0-zwlrALc9baA16tmGDk-VH8PZ8CQ"
TELEGRAM_CHAT_ID = "6903579390"

# ── TRADING CONFIG ────────────────────────────────────────
AUTO_TRADE         = True
PAPER_TRADING      = True
ACCOUNT_SIZE       = 10000
RISK_PCT           = 2
MAX_OPEN_TRADES    = 3
DAILY_LOSS_LIMIT   = 5.0
MAX_TRADES_PER_DAY = 6
SCAN_INTERVAL      = 60
SIGNAL_COOLDOWN    = 1800
MIN_SCORE          = 5  # lowered to get more signals
BEST_HOURS         = [(9,30,16,0)]  # full market hours 9:30 AM - 4:00 PM
SPY_FILTER         = True
TIME_FILTER        = False  # disabled - trade all market hours
REQUIRE_3TF_AGREE  = True
CLOSE_ALL_TIME     = (15, 45)  # Close all positions at 3:45 PM NY

# Leverage by confidence
LEVERAGE_MAP = {"HIGH": 2, "MEDIUM": 1}

# ── SCAN UNIVERSE (200+ stocks) ───────────────────────────
SMALL_CAPS = [
    "RGTI","RXT","QUBT","LUNR","BBAI","SOUN","KULR","MARA","RIOT","CLSK",
    "HIMS","RKLB","ASTS","ACHR","JOBY","NKLA","BLNK","PLUG","FCEL","SPCE",
    "NVAX","ACAD","ITCI","CLOV","WISH","WKHS","HYZN","GOEV","NKLA","RIDE",
    "MVIS","OCGN","AGEN","CTIC","CYTO","DARE","SINT","FREQ","SENS","CLVS",
    "AMPIO","OBSV","AVDL","AYALA","BFRI","BIMI","BTBT","BYFC","CANF","CEAD"
]

MID_CAPS = [
    "MSTR","COIN","HOOD","SOFI","AFRM","UPST","OPEN","CVNA","DKNG","PENN",
    "CHWY","RIVN","LCID","FSR","PTRA","ARVL","GOEV","XPEV","NIO","LI",
    "RBLX","DKNG","PENN","SKLZ","MTCH","BMBL","SNAP","PINS","TWTR","ZM",
    "PTON","ROKU","FUBO","SFIX","REAL","POSH","OZON","SE","GRAB","GOTU",
    "BARK","VUZI","KRTX","ACMR","AEHR","AIOT","AKBA","ALBT","ALDX","ALEC"
]

SCAN_UNIVERSE = list(set(SMALL_CAPS + MID_CAPS))

# ── ALPACA ENDPOINTS ──────────────────────────────────────
ALPACA_BASE          = "https://data.alpaca.markets/v2"
ALPACA_TRADE_BASE    = "https://paper-api.alpaca.markets"
ALPACA_HEADERS       = {"APCA-API-KEY-ID":ALPACA_API_KEY,"APCA-API-SECRET-KEY":ALPACA_SECRET}
ALPACA_TRADE_HEADERS = {"APCA-API-KEY-ID":ALPACA_API_KEY,"APCA-API-SECRET-KEY":ALPACA_SECRET,"Content-Type":"application/json"}

# ── DEFAULT WATCHLIST (always has stocks) ─────────────────
DEFAULT_WATCHLIST = ["MARA", "SOUN", "RGTI", "RIOT", "BBAI"]

# ── RESCAN STATE ─────────────────────────────────────────
last_signal_found   = {}   # ticker -> last time signal was found
no_signal_count     = {}   # ticker -> consecutive no-signal count
RESCAN_AFTER        = 3    # rescan after 3 consecutive no-signals (3 min)

# ── STATE ─────────────────────────────────────────────────
last_signal_time     = {}
last_signal_type     = {}
orb_levels           = {}
active_trades        = {}
performance_log      = []
all_time_log         = []
watchlist            = list(DEFAULT_WATCHLIST)  # always starts with defaults
daily_picks          = []           # today's top stocks
bot_paused           = False
trailing_stops       = {}
last_update_id       = 0
price_alerts         = {}
morning_brief_sent   = False
trades_today         = 0
daily_pnl            = 0.0
losing_streak        = 0
last_scan_day        = -1
weekly_report_sent   = False
alpaca_ws_prices     = {}
alpaca_ws_connected  = False
stocktwits_cooldown  = {}
positions_closed_today = False

# ── TIME ──────────────────────────────────────────────────
def get_ny():
    return datetime.now(timezone.utc) + timedelta(hours=-4)

def market_status():
    ny = get_ny()
    if ny.weekday() >= 5: return "CLOSED"
    h,m = ny.hour, ny.minute
    if h < 8: return "CLOSED"
    if h < 9 or (h==9 and m<30): return "PRE_MARKET"
    if h >= 16: return "CLOSED"
    return "OPEN"

def is_best_hour():
    ny = get_ny(); t = ny.hour*60+ny.minute
    return any(sh*60+sm <= t <= eh*60+em for sh,sm,eh,em in BEST_HOURS)

def get_session_name():
    ny = get_ny(); t = ny.hour*60+ny.minute
    if 570<=t<=660:  return "9:30-11am BEST"
    if 660<=t<=840:  return "11am-2pm CHOPPY"
    if 840<=t<=900:  return "2-3pm OK"
    if 900<=t<=960:  return "3-4pm BEST"
    return "PRE/AFTER"

def is_duplicate(ticker, signal):
    if ticker not in last_signal_time: return False
    return ((datetime.now()-last_signal_time[ticker]).total_seconds() < SIGNAL_COOLDOWN
            and last_signal_type.get(ticker) == signal)

def check_daily_limits():
    if trades_today >= MAX_TRADES_PER_DAY:
        return False, "Max trades ("+str(MAX_TRADES_PER_DAY)+"/day)"
    if daily_pnl <= -DAILY_LOSS_LIMIT:
        return False, "Daily loss limit (-"+str(DAILY_LOSS_LIMIT)+"%)"
    return True, ""

def reset_daily():
    global trades_today, daily_pnl, losing_streak, positions_closed_today, daily_picks, watchlist
    trades_today=0; daily_pnl=0.0; positions_closed_today=False
    daily_picks=[]
    watchlist=list(DEFAULT_WATCHLIST)  # reset to defaults not empty!
    print("Daily stats reset - watchlist:", watchlist)

# ── TELEGRAM ─────────────────────────────────────────────
async def tg(session, msg):
    url = "https://api.telegram.org/bot"+TELEGRAM_TOKEN+"/sendMessage"
    try:
        async with session.post(url, json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML"}) as r:
            res = await r.json()
            if not res.get("ok"): print("TG err:",res)
    except Exception as e: print("TG err:",e)

async def get_updates(session, offset=0):
    url = "https://api.telegram.org/bot"+TELEGRAM_TOKEN+"/getUpdates"
    try:
        async with session.get(url, params={"offset":offset,"timeout":1}) as r:
            return (await r.json()).get("result",[])
    except: return []

# ── PRICES ───────────────────────────────────────────────
async def yahoo_price(session, ticker):
    try:
        async with session.get("https://query1.finance.yahoo.com/v8/finance/chart/"+ticker,
            headers={"User-Agent":"Mozilla/5.0"}, timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json()
            meta = data["chart"]["result"][0]["meta"]
            price = meta["regularMarketPrice"]
            prev  = meta["chartPreviousClose"]
            vol   = meta.get("regularMarketVolume",0)
            return round(price,4), round(((price-prev)/prev)*100,2), vol
    except: return None,None,None

async def get_alpaca_price(session, ticker):
    try:
        async with session.get(ALPACA_BASE+"/stocks/"+ticker+"/trades/latest",
            headers=ALPACA_HEADERS, params={"feed":"sip"},
            timeout=aiohttp.ClientTimeout(total=5)) as r:
            data = await r.json()
            price = data.get("trade",{}).get("p")
            return round(price,4) if price else None
    except: return None

async def get_finnhub_price(session, ticker):
    """Get real-time price from Finnhub for trade accuracy confirmation"""
    try:
        async with session.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol":ticker,"token":FINNHUB_API_KEY},
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            data = await r.json()
            price = data.get("c",0)  # current price
            high  = data.get("h",0)  # day high
            low   = data.get("l",0)  # day low
            prev  = data.get("pc",0) # prev close
            if price and price>0:
                change_pct = round(((price-prev)/prev)*100,2) if prev else 0
                return round(price,4), change_pct, round(high,4), round(low,4)
            return None,None,None,None
    except Exception as e:
        print("Finnhub price error "+ticker+":",e)
        return None,None,None,None

async def get_realtime_price(session, ticker):
    """Get best available real-time price: Finnhub > Alpaca REST > Alpaca WS > Yahoo"""
    # 1. Try Finnhub (most accurate, real-time)
    fp,_,_,_ = await get_finnhub_price(session, ticker)
    if fp: return fp, "FINNHUB"

    # 2. Try Alpaca REST SIP
    p = await get_alpaca_price(session, ticker)
    if p: return p, "ALPACA-SIP"

    # 3. Try Alpaca WebSocket cache
    if ticker in alpaca_ws_prices and alpaca_ws_prices[ticker]>0:
        return alpaca_ws_prices[ticker], "ALPACA-WS"

    # 4. Yahoo fallback
    yp,_,_ = await yahoo_price(session, ticker)
    return yp, "YAHOO"

async def get_spy_trend(session):
    p,c,_ = await yahoo_price(session,"SPY")
    if p is None: return "NEUTRAL",0,0
    return ("BULL" if c>=0 else "BEAR"), c, p

async def get_bars(session, ticker, tf="5Min", limit=50):
    try:
        async with session.get(ALPACA_BASE+"/stocks/"+ticker+"/bars",
            headers=ALPACA_HEADERS,
            params={"timeframe":tf,"limit":limit,"feed":"sip"}) as r:
            return (await r.json()).get("bars",[])
    except: return []

async def get_yahoo_bars(session, ticker, days=90):
    try:
        end = int(time.time()); start = end-(days*24*60*60)
        async with session.get("https://query1.finance.yahoo.com/v8/finance/chart/"+ticker,
            params={"interval":"1d","period1":str(start),"period2":str(end)},
            headers={"User-Agent":"Mozilla/5.0"}, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
            result = data["chart"]["result"][0]
            ohlcv = result["indicators"]["quote"][0]
            bars = []
            for i in range(len(result.get("timestamp",[]))):
                try:
                    o,h,l,c,v = ohlcv["open"][i],ohlcv["high"][i],ohlcv["low"][i],ohlcv["close"][i],ohlcv["volume"][i]
                    if all([o,h,l,c,v]): bars.append({"o":round(o,4),"h":round(h,4),"l":round(l,4),"c":round(c,4),"v":int(v)})
                except: continue
            return bars
    except Exception as e:
        print("Yahoo bars error:",e); return []

# ── ALPACA WEBSOCKET ─────────────────────────────────────
async def alpaca_websocket():
    """Poll Alpaca REST every 5 seconds - reliable alternative to WebSocket"""
    global alpaca_ws_prices, alpaca_ws_connected
    print("Alpaca REST price poller starting...")
    alpaca_ws_connected = True

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                ny = get_ny()
                after_close = ny.hour > 16 or (ny.hour == 16 and ny.minute >= 30)
                before_open = ny.hour < 8
                if ny.weekday()>=5 or before_open or after_close:
                    await asyncio.sleep(1800); continue

                tickers = list(watchlist) if watchlist else list(DEFAULT_WATCHLIST)
                for ticker in tickers:
                    try:
                        async with session.get(
                            ALPACA_BASE+"/stocks/"+ticker+"/trades/latest",
                            headers=ALPACA_HEADERS,
                            params={"feed":"sip"},
                            timeout=aiohttp.ClientTimeout(total=3)
                        ) as r:
                            data = await r.json()
                            price = data.get("trade",{}).get("p")
                            if price:
                                alpaca_ws_prices[ticker] = round(price,4)
                    except: pass
                    await asyncio.sleep(0.5)

                await asyncio.sleep(5)  # poll every 5 seconds

            except Exception as e:
                print("Price poller error:",e)
                await asyncio.sleep(10)

# ── AUTO TRADING FUNCTIONS ────────────────────────────────
async def get_account_info(session):
    try:
        async with session.get(ALPACA_TRADE_BASE+"/v2/account",
            headers=ALPACA_TRADE_HEADERS, timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json()
            return {"equity":round(float(data.get("equity",0)),2),
                    "buying_power":round(float(data.get("buying_power",0)),2),
                    "cash":round(float(data.get("cash",0)),2)}
    except Exception as e: print("Account error:",e); return None

async def get_open_positions(session):
    try:
        async with session.get(ALPACA_TRADE_BASE+"/v2/positions",
            headers=ALPACA_TRADE_HEADERS, timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json()
            return {p["symbol"]:p for p in data} if isinstance(data,list) else {}
    except: return {}

async def place_order(session, ticker, side, qty, sl_price, tp_price):
    try:
        order = {"symbol":ticker,"qty":str(qty),"side":side,"type":"market",
                 "time_in_force":"day","order_class":"bracket",
                 "stop_loss":{"stop_price":str(round(sl_price,2))},
                 "take_profit":{"limit_price":str(round(tp_price,2))}}
        async with session.post(ALPACA_TRADE_BASE+"/v2/orders",
            headers=ALPACA_TRADE_HEADERS, json=order,
            timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            if data.get("id"): return {"success":True,"order_id":data["id"],"status":data.get("status")}
            return {"success":False,"error":str(data)}
    except Exception as e: return {"success":False,"error":str(e)}

async def close_all_positions(session):
    try:
        async with session.delete(ALPACA_TRADE_BASE+"/v2/positions",
            headers=ALPACA_TRADE_HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as r:
            return await r.json()
    except Exception as e: print("Close all error:",e); return None

async def close_position(session, ticker):
    try:
        async with session.delete(ALPACA_TRADE_BASE+"/v2/positions/"+ticker,
            headers=ALPACA_TRADE_HEADERS, timeout=aiohttp.ClientTimeout(total=8)) as r:
            return await r.json()
    except Exception as e: print("Close position error:",e); return None

def calc_position_size(entry, sl, confidence, equity):
    leverage = LEVERAGE_MAP.get(confidence, 1)
    risk_amt = equity * (RISK_PCT/100)
    rps = abs(entry-sl)
    if rps<=0: return 0,0,leverage

    # Calculate shares based on risk
    shares = int(risk_amt/rps) * leverage

    # If 0 shares (tight SL or expensive stock) - use minimum 1 share
    if shares == 0:
        if entry <= equity * 0.1:  # stock costs less than 10% of account
            shares = max(1, leverage)
        else:
            shares = 1  # at least 1 share

    # Safety check: never use more than 30% of account on one trade
    max_cost = equity * 0.30
    if shares * entry > max_cost:
        shares = int(max_cost / entry)

    # Minimum 1 share
    if shares < 1: shares = 1

    total_cost = round(shares * entry, 2)
    return shares, total_cost, leverage

# ── SMART DAILY SCANNER ───────────────────────────────────
async def smart_daily_scan(session):
    global watchlist, daily_picks
    await tg(session, "🔍 Smart Daily Scanner running...\nAnalyzing "+str(len(SCAN_UNIVERSE))+" stocks for best day trade picks...")

    spy_trend, spy_chg, _ = await get_spy_trend(session)
    candidates = []

    for ticker in SCAN_UNIVERSE:
        try:
            p,chg,vol = await yahoo_price(session, ticker)
            if not p or not vol or p < 1: continue

            bars = await get_bars(session, ticker, "1Day", 15)
            if len(bars) < 5: continue

            avg_vol = sum(b['v'] for b in bars[:-1]) / len(bars[:-1])
            rvol = vol/avg_vol if avg_vol>0 else 0
            atr = calc_atr(bars)
            if not atr: continue
            atr_pct = (atr/p)*100

            # Skip if price out of range (sweet spot $2-$100)
            if p < 2 or p > 100: continue
            # Skip if no volume
            if rvol < 1.0: continue
            # Skip if not moving
            if abs(chg) < 1.0: continue

            # Scoring
            score = 0

            # RVOL score (most important)
            if rvol >= 5:   score += 5
            elif rvol >= 3: score += 4
            elif rvol >= 2: score += 3
            elif rvol >= 1.5: score += 2
            else: score += 1

            # ATR% score (volatility = opportunity)
            if atr_pct >= 8:   score += 4
            elif atr_pct >= 5: score += 3
            elif atr_pct >= 3: score += 2
            elif atr_pct >= 2: score += 1

            # Price move score
            if abs(chg) >= 10: score += 4
            elif abs(chg) >= 5: score += 3
            elif abs(chg) >= 3: score += 2
            elif abs(chg) >= 1: score += 1

            # Direction alignment with SPY
            if spy_trend == "BULL" and chg > 0: score += 2
            if spy_trend == "BEAR" and chg < 0: score += 2

            # Price range bonus (sweet spot for day trading)
            if 5 <= p <= 50: score += 2
            elif 2 <= p < 5 or 50 < p <= 100: score += 1

            # Minimum score filter
            if score >= 3:
                candidates.append({
                    "ticker": ticker, "price": p, "change": chg,
                    "vol": vol, "rvol": round(rvol,1),
                    "atr_pct": round(atr_pct,1), "score": score,
                    "spy_aligned": (spy_trend=="BULL" and chg>0) or (spy_trend=="BEAR" and chg<0)
                })

            await asyncio.sleep(0.3)
        except: continue

    # Sort by score
    candidates.sort(key=lambda x: x['score'], reverse=True)
    top3 = candidates[:3]

    if not top3:
        # Use default watchlist if scanner finds nothing
        default = list(DEFAULT_WATCHLIST)
        await tg(session, "Scanner: No strong candidates found.\nUsing default watchlist: "+", ".join(default))
        return default

    daily_picks = top3
    new_watchlist = [c['ticker'] for c in top3]

    # Send detailed scan results
    lines = ["🔍 <b>Daily Smart Scan Complete!</b>","",
             "SPY: "+spy_trend+" "+("+" if spy_chg>=0 else "")+str(spy_chg)+"%","",
             "<b>Today's Top 3 Auto-Trade Picks:</b>",""]

    for i,c in enumerate(top3,1):
        sign = "+" if c['change']>=0 else ""
        aligned = "✅ SPY aligned" if c['spy_aligned'] else "⚠️ Against SPY"
        lines.append(str(i)+". <b>"+c['ticker']+"</b> $"+str(c['price'])+" "+sign+str(c['change'])+"%")
        lines.append("   RVOL: "+str(c['rvol'])+"x | ATR: "+str(c['atr_pct'])+"% | Score: "+str(c['score'])+" | "+aligned)
        lines.append("")

    lines.append("🤖 Auto-trading these stocks today!")
    lines.append("Max risk per trade: $"+str(round(ACCOUNT_SIZE*RISK_PCT/100,2)))
    await tg(session, "\n".join(lines))

    # Update Alpaca WebSocket subscriptions
    if alpaca_ws_connected:
        try:
            import websockets
            pass  # WS will resubscribe on next connection
        except: pass

    return new_watchlist

# ── INDICATORS ───────────────────────────────────────────
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

def get_multiday(bars_daily):
    if not bars_daily or len(bars_daily)<2: return None
    p=bars_daily[-2]; ph,pl,pc=round(p['h'],4),round(p['l'],4),round(p['c'],4)
    pivot=round((ph+pl+pc)/3,4)
    return {"prev_high":ph,"prev_low":pl,"prev_close":pc,"pivot":pivot,
            "r1":round(2*pivot-pl,4),"s1":round(2*pivot-ph,4)}

def get_sr(bars, n=20):
    r=bars[-n:]
    return round(min(b['l'] for b in r),4),round(max(b['h'] for b in r),4)

def check_3tf(bars_1m, bars_5m, bars_15m, signal):
    agrees=0
    for bars in [bars_1m, bars_5m, bars_15m]:
        if len(bars)<10: continue
        closes=[b['c'] for b in bars]
        e9=calc_ema(closes,9); e21=calc_ema(closes,min(21,len(closes)-1))
        if not e9 or not e21: continue
        if signal=="BUY" and e9>e21: agrees+=1
        if signal=="SELL" and e9<e21: agrees+=1
    return agrees

# ── SMC ENGINE ────────────────────────────────────────────
def find_swings(bars, lookback=5):
    highs,lows=[],[]
    for i in range(lookback, len(bars)-lookback):
        if bars[i]['h']==max(b['h'] for b in bars[i-lookback:i+lookback+1]):
            highs.append({"idx":i,"price":bars[i]['h']})
        if bars[i]['l']==min(b['l'] for b in bars[i-lookback:i+lookback+1]):
            lows.append({"idx":i,"price":bars[i]['l']})
    return highs,lows

def detect_bos_choch(bars, highs, lows):
    if not highs or not lows or len(bars)<10: return None,None
    price=bars[-1]['c']
    lh=highs[-1]['price'] if highs else None
    ll=lows[-1]['price'] if lows else None
    ph=highs[-2]['price'] if len(highs)>=2 else None
    pl=lows[-2]['price'] if len(lows)>=2 else None
    bos=choch=None
    if lh and price>lh: bos="BULLISH_BOS"
    if ll and price<ll: bos="BEARISH_BOS"
    if ph and lh and ph>lh and price>lh: choch="BULLISH_CHOCH"
    if pl and ll and pl<ll and price<ll: choch="BEARISH_CHOCH"
    return bos,choch

def find_order_blocks(bars, lookback=20):
    obs=[]; recent=bars[-lookback:] if len(bars)>=lookback else bars
    for i in range(1,len(recent)-2):
        b=recent[i]; body=abs(b['c']-b['o'])
        if body==0: continue
        if b['c']<b['o']:
            mu=sum(1 for j in range(i+1,min(i+4,len(recent))) if recent[j]['c']>recent[j]['o'])
            if mu>=2: obs.append({"type":"BULLISH_OB","high":b['h'],"low":b['l'],"mid":round((b['h']+b['l'])/2,4)})
        if b['c']>b['o']:
            md=sum(1 for j in range(i+1,min(i+4,len(recent))) if recent[j]['c']<recent[j]['o'])
            if md>=2: obs.append({"type":"BEARISH_OB","high":b['h'],"low":b['l'],"mid":round((b['h']+b['l'])/2,4)})
    return obs[-5:] if obs else []

def detect_liq_sweep(bars, highs, lows):
    if len(bars)<5 or not highs or not lows: return None
    cur,prev=bars[-1],bars[-2]
    lh=highs[-1]['price'] if highs else None
    ll=lows[-1]['price'] if lows else None
    if ll and prev['l']<ll and cur['c']>ll: return "BULLISH_SWEEP"
    if lh and prev['h']>lh and cur['c']<lh: return "BEARISH_SWEEP"
    return None

def detect_fvg(bars):
    fvgs=[]
    if len(bars)<3: return fvgs
    for i in range(len(bars)-3):
        b1,b2,b3=bars[i],bars[i+1],bars[i+2]
        if b3['l']>b1['h']: fvgs.append({"type":"BULLISH_FVG","top":b3['l'],"bottom":b1['h'],"mid":round((b3['l']+b1['h'])/2,4)})
        if b3['h']<b1['l']: fvgs.append({"type":"BEARISH_FVG","top":b1['l'],"bottom":b3['h'],"mid":round((b1['l']+b3['h'])/2,4)})
    return fvgs[-3:] if fvgs else []

def detect_smt(bars, highs, lows):
    if len(bars)<5 or not highs or not lows: return None
    cur,prev=bars[-1],bars[-2]
    lh=highs[-1]['price'] if highs else None
    ll=lows[-1]['price'] if lows else None
    if lh and prev['h']>lh and cur['c']<lh and cur['c']<cur['o']: return "BULL_TRAP"
    if ll and prev['l']<ll and cur['c']>ll and cur['c']>cur['o']: return "BEAR_TRAP"
    return None

def detect_po3(bars):
    if len(bars)<15: return "UNKNOWN"
    recent=bars[-15:]
    closes=[b['c'] for b in recent]
    h5=max(b['h'] for b in recent[:5]); l5=min(b['l'] for b in recent[:5])
    h10=max(b['h'] for b in recent[5:10]); l10=min(b['l'] for b in recent[5:10])
    ap=sum(closes)/len(closes)
    r1=(h5-l5)/ap*100; r2=(h10-l10)/ap*100
    if r1<2 and r2>r1*1.5: return "DISTRIBUTION_BULLISH" if closes[-1]>closes[-5] else "DISTRIBUTION_BEARISH"
    if r1<1.5: return "ACCUMULATION"
    return "TRENDING"

def get_zone(bars, highs, lows):
    if not highs or not lows: return None,None,None
    rh=highs[-1]['price']; rl=lows[-1]['price']
    eq=round((rh+rl)/2,4); price=bars[-1]['c']
    zone="PREMIUM" if price>eq else "DISCOUNT" if price<eq else "EQUILIBRIUM"
    return zone,eq,round((price-eq)/eq*100,2)

def detect_patterns(bars):
    patterns=[]
    if len(bars)<3: return patterns
    b0,b1,b2=bars[-3],bars[-2],bars[-1]
    o2,h2,l2,c2=b2['o'],b2['h'],b2['l'],b2['c']
    o1,c1=b1['o'],b1['c']; o0,c0=b0['o'],b0['c']
    body2=abs(c2-o2); range2=h2-l2; body1=abs(c1-o1)
    if range2>0 and body2/range2<0.1: patterns.append("Doji")
    if body2>0 and (l2<min(o2,c2)) and (min(o2,c2)-l2)>2*body2 and c2>o2: patterns.append("Hammer")
    if body2>0 and (h2>max(o2,c2)) and (h2-max(o2,c2))>2*body2 and c2<o2: patterns.append("Shooting Star")
    if c1<o1 and c2>o2 and c2>o1 and o2<c1: patterns.append("Bullish Engulfing")
    if c1>o1 and c2<o2 and c2<o1 and o2>c1: patterns.append("Bearish Engulfing")
    if c0<o0 and body1<abs(c0-o0)*0.5 and c2>o2 and c2>(o0+c0)/2: patterns.append("Morning Star")
    if range2>0 and body2/range2>0.9: patterns.append("Bullish Marubozu" if c2>o2 else "Bearish Marubozu")
    return patterns

def update_orb(ticker, bars_1m):
    ny=get_ny()
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

# ── MAIN SIGNAL ──────────────────────────────────────────
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
    highs,lows=find_swings(bars_5m)
    bos,choch=detect_bos_choch(bars_5m,highs,lows)
    obs=find_order_blocks(bars_5m)
    fvgs=detect_fvg(bars_5m)
    liq=detect_liq_sweep(bars_5m,highs,lows)
    smt=detect_smt(bars_5m,highs,lows)
    po3=detect_po3(bars_5m)
    zone,eq,zone_pct=get_zone(bars_5m,highs,lows)
    nearest_ob=next((ob for ob in reversed(obs) if
        (ob['type']=="BULLISH_OB" and ob['low']<=price<=ob['high']*1.02) or
        (ob['type']=="BEARISH_OB" and ob['low']*0.98<=price<=ob['high'])),None)
    nearest_fvg=next((f for f in reversed(fvgs) if f['bottom']<=price<=f['top']),None)
    above=price>vn; ca=price>vn and closes[-2]<=vp; cb=price<vn and closes[-2]>=vp
    hv=vol_r and vol_r>=1.5; eb=ema9>ema21 if ema9 and ema21 else False
    bs=ss=0
    if ca: bs+=2
    elif above: bs+=1
    if cb: ss+=2
    elif not above: ss+=1
    if hv:
        if above: bs+=2
        else: ss+=2
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
    if bos=="BULLISH_BOS": bs+=2
    if bos=="BEARISH_BOS": ss+=2
    if choch=="BULLISH_CHOCH": bs+=3
    if choch=="BEARISH_CHOCH": ss+=3
    if nearest_ob:
        if nearest_ob['type']=="BULLISH_OB": bs+=3
        if nearest_ob['type']=="BEARISH_OB": ss+=3
    if liq=="BULLISH_SWEEP": bs+=3
    if liq=="BEARISH_SWEEP": ss+=3
    if nearest_fvg:
        if nearest_fvg['type']=="BULLISH_FVG": bs+=2
        if nearest_fvg['type']=="BEARISH_FVG": ss+=2
    if po3=="DISTRIBUTION_BULLISH": bs+=2
    if po3=="DISTRIBUTION_BEARISH": ss+=2
    if zone=="DISCOUNT": bs+=2
    if zone=="PREMIUM": ss+=2
    if smt=="BULL_TRAP": bs-=5; ss+=2  # heavy penalty but don't block completely
    if smt=="BEAR_TRAP": ss-=5; bs+=2
    if bs>=MIN_SCORE: signal="BUY"; conf="HIGH" if bs>=10 else "MEDIUM"
    elif ss>=MIN_SCORE: signal="SELL"; conf="HIGH" if ss>=10 else "MEDIUM"
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
            "support":sup,"resistance":res,"bb_squeeze":(bbu-bbl)<atr*2 if bbu and bbl and atr else False,
            "bos":bos,"choch":choch,"nearest_ob":nearest_ob,"nearest_fvg":nearest_fvg,
            "liq_sweep":liq,"smt":smt,"po3":po3,"zone":zone,"equilibrium":eq,"zone_pct":zone_pct}

# ── NEWS & SENTIMENT ──────────────────────────────────────
async def get_news(session, ticker):
    try:
        async with session.get("https://query1.finance.yahoo.com/v1/finance/search",
            params={"q":ticker,"newsCount":3}, headers={"User-Agent":"Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            data=await r.json(); news=data.get("news",[])
            if not news: return "NEUTRAL",[]
            headlines=[n.get("title","") for n in news[:3]]
            neg=["downgrade","loss","miss","decline","drop","fail","lawsuit","fraud","warning","cut","crash"]
            pos=["upgrade","beat","surge","rally","bullish","profit","deal","launch","record","breakout"]
            nc=sum(1 for h in headlines for w in neg if w in h.lower())
            pc=sum(1 for h in headlines for w in pos if w in h.lower())
            return ("NEGATIVE" if nc>pc else "POSITIVE" if pc>nc else "NEUTRAL"),headlines
    except: return "NEUTRAL",[]

async def replace_ticker(removed_ticker):
    """Immediately find and add a replacement stock when one is removed"""
    print("Finding replacement for", removed_ticker)
    async with aiohttp.ClientSession() as session:
        try:
            candidates = []
            # Scan universe for best replacement
            for ticker in SCAN_UNIVERSE:
                if ticker in watchlist or ticker == removed_ticker: continue
                try:
                    p,chg,vol = await yahoo_price(session, ticker)
                    if not p or not vol or p < 1: continue
                    if abs(chg or 0) < 1: continue
                    bars = await get_bars(session, ticker, "5Min", 20)
                    if len(bars) < 10: continue
                    closes = [b['c'] for b in bars]
                    rsi = calc_rsi(closes)
                    vol_r = calc_vol_ratio(bars)
                    atr = calc_atr(bars)
                    if not all([rsi, vol_r, atr]): continue
                    atr_pct = (atr/p)*100
                    # Good candidate: affordable, good RSI, volume, moving up
                    if 30 < rsi < 60 and vol_r >= 1.3 and atr_pct >= 1.5 and (chg or 0) > 0 and p < 100:
                        score = 0
                        if vol_r >= 2: score += 3
                        elif vol_r >= 1.5: score += 2
                        else: score += 1
                        if atr_pct >= 4: score += 3
                        elif atr_pct >= 2: score += 2
                        else: score += 1
                        if 35 <= rsi <= 55: score += 2  # ideal RSI range
                        candidates.append({"ticker":ticker,"price":p,"change":chg,
                                          "rsi":rsi,"vol_r":round(vol_r,1),
                                          "atr_pct":round(atr_pct,1),"score":score})
                    await asyncio.sleep(0.3)
                except: continue

            if candidates:
                candidates.sort(key=lambda x: x['score'], reverse=True)
                best = candidates[0]
                watchlist.append(best['ticker'])
                sign = "+" if best['change'] >= 0 else ""
                print("Replaced "+removed_ticker+" with "+best['ticker']+" silently")
            else:
                # Re-add from defaults silently
                for t in DEFAULT_WATCHLIST:
                    if t not in watchlist:
                        watchlist.append(t)
                        print("Re-added "+t+" from defaults after removing "+removed_ticker)
                        break
        except Exception as e:
            print("Replace ticker error:", e)

async def stocktwits_premarket_scan(session):
    """Scan Stocktwits during pre/after market for trending stocks"""
    lines = ["Pre/After Market Stocktwits Scan",""]
    hot_stocks = []
    
    # Check all watchlist + popular stocks
    scan_list = list(set(list(watchlist) + list(DEFAULT_WATCHLIST)))
    
    for ticker in scan_list:
        try:
            st = await get_stocktwits(session, ticker)
            if st["total"] >= 5 and st["sentiment"] != "NEUTRAL":
                if st["bull_pct"] >= 70 or st["bear_pct"] >= 70:
                    hot_stocks.append({
                        "ticker": ticker,
                        "sentiment": st["sentiment"],
                        "bull_pct": st["bull_pct"],
                        "trending": st["trending"],
                        "top_post": st["top_post"]
                    })
            await asyncio.sleep(0.5)
        except: continue
    
    if hot_stocks:
        hot_stocks.sort(key=lambda x: x["bull_pct"] if x["sentiment"]=="BULLISH" else x["bear_pct"], reverse=True)
        lines.append("Hot stocks right now:")
        for h in hot_stocks[:5]:
            se = "BULLISH" if h["sentiment"]=="BULLISH" else "BEARISH"
            trend = " TRENDING" if h["trending"] else ""
            lines.append(h["ticker"]+" - "+se+trend+" | "+str(h["bull_pct"])+"% Bulls")
            if h["top_post"]: lines.append("  "+h["top_post"][:60])
        lines.append("")
        lines.append("Watch these at market open!")
        await tg(session, "\n".join(lines))
    else:
        print("No hot stocks on Stocktwits pre/after market")

async def get_stocktwits(session, ticker):
    try:
        now=get_ny(); last=stocktwits_cooldown.get(ticker)
        if last and (now-last).total_seconds()<3600: return {"sentiment":"NEUTRAL","bull_pct":50,"bear_pct":50,"total":0,"trending":False,"top_post":""}
        async with session.get("https://api.stocktwits.com/api/2/streams/symbol/"+ticker+".json",
            headers={"User-Agent":"Mozilla/5.0"}, timeout=aiohttp.ClientTimeout(total=8)) as r:
            data=await r.json(); messages=data.get("messages",[])
            if not messages: return {"sentiment":"NEUTRAL","bull_pct":50,"bear_pct":50,"total":0,"trending":False,"top_post":""}
            bull=sum(1 for m in messages if isinstance(m.get("entities",{}).get("sentiment"),dict) and m["entities"]["sentiment"].get("basic")=="Bullish")
            bear=sum(1 for m in messages if isinstance(m.get("entities",{}).get("sentiment"),dict) and m["entities"]["sentiment"].get("basic")=="Bearish")
            total=bull+bear
            if total<3: return {"sentiment":"NEUTRAL","bull_pct":50,"bear_pct":50,"total":total,"trending":len(messages)>=10,"top_post":""}
            bp=round(bull/total*100); bep=round(bear/total*100)
            sent="BULLISH" if bp>=60 else "BEARISH" if bep>=60 else "NEUTRAL"
            top=""
            for m in messages[:5]:
                body=m.get("body","").strip()
                if len(body)>15: top=body[:80]; break
            return {"sentiment":sent,"bull_pct":bp,"bear_pct":bep,"total":total,"trending":len(messages)>=15,"top_post":top}
    except: return {"sentiment":"NEUTRAL","bull_pct":50,"bear_pct":50,"total":0,"trending":False,"top_post":""}

async def ai_confirm(session, ticker, result, patterns, sentiment, tf_agrees):
    smc=("BOS="+str(result.get('bos'))+" CHoCH="+str(result.get('choch'))+
         " OB="+str(result.get('nearest_ob',{}).get('type') if result.get('nearest_ob') else 'None')+
         " Sweep="+str(result.get('liq_sweep'))+" Zone="+str(result.get('zone'))+
         " PO3="+str(result.get('po3'))+" SMT="+str(result.get('smt')))
    prompt=("Trade signal "+ticker+": "+result['signal']+" Score:"+
            str(result.get('buy_score' if result['signal']=='BUY' else 'sell_score',0))+"/20"+
            " RSI:"+str(result['rsi'])+" Vol:"+str(result['vol_ratio'])+"x"+
            " "+smc+" News:"+sentiment+" 3TF:"+str(tf_agrees)+"/3"+
            " Respond ONLY JSON: {\"verdict\":\"CONFIRMED\" or \"REJECTED\" or \"CAUTION\",\"reason\":\"one sentence\",\"tip\":\"one actionable tip\"}")
    try:
        async with session.post("https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization":"Bearer "+GROQ_API_KEY,"Content-Type":"application/json"},
            json={"model":"llama-3.3-70b-versatile","messages":[{"role":"user","content":prompt}],"max_tokens":200,"temperature":0.1},
            timeout=aiohttp.ClientTimeout(total=15)) as r:
            data=await r.json()
            if "choices" not in data:
                print("Groq API error:", data)
                return {"verdict":"CONFIRMED","reason":"AI rate limited","tip":"Check signal manually"}
            raw=data["choices"][0]["message"]["content"].strip()
            raw=raw.replace("```json","").replace("```","").strip()
            # Find JSON object
            start=raw.find("{"); end=raw.rfind("}")+1
            if start>=0 and end>start:
                raw=raw[start:end]
            # Fix common JSON issues
            raw=raw.replace("\n","").replace("\t","")
            try:
                return json.loads(raw)
            except:
                # Try to extract key fields manually
                verdict = "CONFIRMED"
                if '"REJECTED"' in raw: verdict = "REJECTED"
                elif '"CAUTION"' in raw: verdict = "CAUTION"
                return {"verdict":verdict,"reason":"AI parse error","tip":"Check manually"}
    except Exception as e:
        print("Groq error:",e)
        return {"verdict":"CONFIRMED","reason":"AI temporarily unavailable","tip":"Use your judgment"}

# ── BUILD MESSAGE ─────────────────────────────────────────
def build_msg(ticker, result, yp, yc, status, sentiment, headlines, patterns, ai, rvol, multiday, spy_trend, spy_chg, tf_agrees, orb_tag, shares, cost, leverage, st_data, price_source):
    sig=result["signal"]; now=get_ny().strftime("%H:%M:%S")+" NY"
    score=result['buy_score'] if sig=="BUY" else result['sell_score']
    verdict=ai.get("verdict","CONFIRMED")
    spy_sign="+" if spy_chg>=0 else ""; yc_sign="+" if yc and yc>=0 else ""
    header=("BUY" if sig=="BUY" else "SELL")+" - "+ticker
    if orb_tag: header+=" ORB!"
    header+="  "+result['confidence']+" [AUTO-TRADE]" if AUTO_TRADE else header+"  "+result['confidence']
    smc_lines=[]
    if result.get('choch'): smc_lines.append("CHoCH: "+result['choch'])
    elif result.get('bos'): smc_lines.append("BOS: "+result['bos'])
    if result.get('liq_sweep'): smc_lines.append("Liq Sweep: "+result['liq_sweep'])
    if result.get('nearest_ob'): smc_lines.append("Order Block: "+result['nearest_ob']['type']+" @ $"+str(result['nearest_ob']['mid']))
    if result.get('smt'): smc_lines.append("WARNING: "+result['smt'])
    if result.get('zone'): smc_lines.append("Zone: "+result['zone']+" ("+str(result.get('zone_pct',''))+"% from $"+str(result.get('equilibrium',''))+")")
    lines=[header,
           ("MARKET OPEN" if status=="OPEN" else "PRE-MARKET")+" | "+get_session_name(),
           "SPY: "+spy_trend+" "+spy_sign+str(spy_chg)+"%","",
           "LIVE $"+str(yp)+" "+yc_sign+str(yc)+"% ["+price_source+"]" if yp else "Price: $"+str(result['entry']),"",
           "Entry:       $"+str(result['entry']),
           "Stop Loss:   $"+str(result['sl']),
           "Take Profit: $"+str(result['tp']),
           "R/R: "+result['rr']+"  ATR: $"+str(result['atr']),"",
           "Auto Position: "+str(shares)+" shares ($"+str(cost)+")",
           "Leverage: 1:"+str(leverage)+"  Risk: $"+str(round(ACCOUNT_SIZE*RISK_PCT/100,2)),"",
           "SMC:"]
    lines.extend(smc_lines if smc_lines else ["No SMC setup"])
    lines+=["","TECHNICALS:",
            "VWAP: $"+str(result['vwap'])+"  RSI: "+str(result['rsi']),
            "EMA 9/21: "+str(result['ema9'])+"/"+str(result['ema21']),
            "Vol: "+str(result['vol_ratio'])+"x  RVOL: "+str(rvol)+"x",
            "15m: "+result['trend_15m']+"  1m: "+result['momentum_1m'],
            "3TF: "+str(tf_agrees)+"/3  Score: "+str(score)+"/20"]
    if result.get("bb_squeeze"): lines.append("BB SQUEEZE!")
    if multiday: lines.append("Pivot: $"+str(multiday['pivot'])+"  PrevH: $"+str(multiday['prev_high'])+"  PrevL: $"+str(multiday['prev_low']))
    lines+=["",verdict+" - "+ai.get('reason',''),"Tip: "+ai.get('tip',''),"",
            "Patterns: "+(", ".join(patterns) if patterns else "None"),
            "News: "+sentiment+(" - "+headlines[0][:50] if headlines else "")]
    if st_data and st_data.get("total",0)>0:
        se="BULLISH" if st_data["sentiment"]=="BULLISH" else "BEARISH" if st_data["sentiment"]=="BEARISH" else "NEUTRAL"
        lines+=["","Stocktwits: "+se+" | "+str(st_data["bull_pct"])+"% Bulls"+(" TRENDING" if st_data.get("trending") else "")]
    lines+=["","S: $"+str(result['support'])+"  R: $"+str(result['resistance']),now]
    return "\n".join(lines)

# ── TRAILING STOPS ───────────────────────────────────────
async def check_trailing(session):
    global daily_pnl, losing_streak
    for ticker in list(active_trades.keys()):
        try:
            trade=active_trades[ticker]
            yp,_=await get_realtime_price(session,ticker)
            if not yp: continue
            entry=trade["entry"]; sig=trade["signal"]; atr=trade.get("atr",abs(entry-trade["sl"]))
            if sig=="BUY":
                nt=round(yp-atr*1.5,4); ct=trailing_stops.get(ticker,trade["sl"])
                if nt>ct:
                    trailing_stops[ticker]=nt
                    print("Trailing SL updated "+ticker+": $"+str(nt))
                if yp<=trailing_stops.get(ticker,trade["sl"]):
                    pnl=round((yp-entry)/entry*100,2); daily_pnl+=pnl
                    if pnl<0: losing_streak+=1
                    else: losing_streak=0
                    sign="+" if pnl>=0 else ""
                    await tg(session,"TRADE CLOSED - "+ticker+"\nExit: $"+str(yp)+"\nP&L: "+sign+str(pnl)+"%\n"+("PROFIT!" if pnl>0 else "Stop loss hit."))
                    performance_log.append({"ticker":ticker,"signal":sig,"entry":entry,"exit":yp,"pnl":pnl,"time":get_ny().strftime("%H:%M")})
                    all_time_log.append({"ticker":ticker,"signal":sig,"entry":entry,"exit":yp,"pnl":pnl,"date":get_ny().strftime("%Y-%m-%d")})
                    del active_trades[ticker]
                    if ticker in trailing_stops: del trailing_stops[ticker]
        except Exception as e: print("Trailing err:",e)

async def check_alerts(session):
    for ticker in list(price_alerts.keys()):
        try:
            alerts=price_alerts[ticker]
            if not alerts: continue
            yp,_=await get_realtime_price(session,ticker)
            if not yp: continue
            remaining=[]
            for a in alerts:
                if (a["direction"]=="above" and yp>=a["price"]) or (a["direction"]=="below" and yp<=a["price"]):
                    await tg(session,"PRICE ALERT "+ticker+" hit $"+str(yp))
                else: remaining.append(a)
            price_alerts[ticker]=remaining
        except: pass

# ── REPORTS ──────────────────────────────────────────────
async def morning_brief(session):
    ny=get_ny().strftime("%A %B %d")
    lines=["Morning Brief - "+ny,""]
    sp,sc,_=await yahoo_price(session,"SPY")
    qp,qc,_=await yahoo_price(session,"QQQ")
    vp,vc,_=await yahoo_price(session,"^VIX")
    if sp: lines.append("SPY $"+str(sp)+" "+("+" if sc>=0 else "")+str(sc)+"%")
    if qp: lines.append("QQQ $"+str(qp)+" "+("+" if qc>=0 else "")+str(qc)+"%")
    if vp: lines.append("VIX $"+str(vp)+" "+("HIGH FEAR" if vp>25 else "ELEVATED" if vp>18 else "CALM"))
    if daily_picks:
        lines+=["","Today's picks:"]
        for c in daily_picks:
            lines.append(c['ticker']+" $"+str(c['price'])+" RVOL:"+str(c['rvol'])+"x Score:"+str(c['score']))
    lines+=["","Market opens in ~5 min! Auto-trading active!"]
    await tg(session,"\n".join(lines))

async def daily_report(session):
    acct = await get_account_info(session)
    if not performance_log and not acct:
        await tg(session,"Daily Report\n\nNo completed trades today."); return
    lines=["Daily Report",""]
    if acct: lines+=["Account: $"+str(acct['equity']),"Cash: $"+str(acct['cash']),""]
    if performance_log:
        wins=[t for t in performance_log if t['pnl']>0]
        tp=round(sum(t['pnl'] for t in performance_log),2)
        wr=round(len(wins)/len(performance_log)*100,1)
        lines+=["Trades: "+str(len(performance_log))+" | Wins: "+str(len(wins))+" | Losses: "+str(len(performance_log)-len(wins)),
                "Win Rate: "+str(wr)+"%","P&L: "+str(tp)+"%",""]
        for t in performance_log:
            lines.append(("WIN" if t['pnl']>0 else "LOSS")+" "+t['ticker']+" "+t['signal']+" "+str(t['pnl'])+"%")
    else: lines.append("No completed trades today")
    await tg(session,"\n".join(lines)); performance_log.clear()

async def weekly_report(session):
    if not all_time_log:
        await tg(session,"Weekly Report\n\nNo trades this week."); return
    wins=[t for t in all_time_log if t['pnl']>0]
    tp=round(sum(t['pnl'] for t in all_time_log),2)
    wr=round(len(wins)/len(all_time_log)*100,1) if all_time_log else 0
    best=max(all_time_log,key=lambda x:x['pnl'])
    worst=min(all_time_log,key=lambda x:x['pnl'])
    lines=["Weekly Report","","Trades: "+str(len(all_time_log)),
           "Win Rate: "+str(wr)+"%","Total P&L: "+str(tp)+"%",
           "Best: "+best['ticker']+" +"+str(best['pnl'])+"%",
           "Worst: "+worst['ticker']+" "+str(worst['pnl'])+"%"]
    await tg(session,"\n".join(lines))

async def run_backtest(session, ticker):
    bars=await get_yahoo_bars(session,ticker,90)
    if not bars or len(bars)<15: return None
    wins=losses=0; total_pnl=0; trades=[]
    for i in range(14,len(bars)-1):
        seg=bars[max(0,i-20):i+1]; closes=[b['c'] for b in seg]; price=closes[-1]
        ema9=calc_ema(closes,9); ema21=calc_ema(closes,21); rsi=calc_rsi(closes)
        if not all([ema9,ema21,rsi]): continue
        vwap=calc_vwap(seg)[-1]; atr=calc_atr(seg)
        highs,lows=find_swings(seg); zone,_,_=get_zone(seg,highs,lows)
        liq=detect_liq_sweep(seg,highs,lows); smt=detect_smt(seg,highs,lows)
        if smt: continue
        bull=ema9>ema21 and rsi<50 and price>vwap and zone=="DISCOUNT"
        bear=ema9<ema21 and rsi>50 and price<vwap and zone=="PREMIUM"
        bos,_=detect_bos_choch(seg,highs,lows)
        if bos=="BULLISH_BOS" or liq=="BULLISH_SWEEP": bull=True
        if bos=="BEARISH_BOS" or liq=="BEARISH_SWEEP": bear=True
        if bull: signal="BUY"
        elif bear: signal="SELL"
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

# ── COMMANDS ─────────────────────────────────────────────
async def handle_cmds(session, offset):
    global bot_paused, watchlist, ACCOUNT_SIZE, AUTO_TRADE
    updates=await get_updates(session, offset+1)
    for update in updates:
        try:
            offset=update["update_id"]
            text=update.get("message",{}).get("text","").strip()
            if not text: continue
            print("CMD:",text)
            if text=="/status":
                al=", ".join([t+":"+v['signal'] for t,v in active_trades.items()]) or "None"
                acct=await get_account_info(session)
                eq=str(acct['equity']) if acct else "N/A"
                lines=["Bot Status","",get_ny().strftime("%H:%M:%S")+" NY | "+market_status(),
                       "AUTO-TRADE: "+("ON [PAPER]" if AUTO_TRADE else "OFF"),
                       "Account: $"+eq,
                       "Trades today: "+str(trades_today)+"/"+str(MAX_TRADES_PER_DAY),
                       "Daily P&L: "+str(round(daily_pnl,2))+"%",
                       "Today's picks: "+", ".join([c['ticker'] for c in daily_picks]) if daily_picks else "Today's picks: Not scanned yet",
                       "Active: "+al]
                await tg(session,"\n".join(lines))
            elif text.startswith("/add "):
                try:
                    t=text.split()[1].upper()
                    if t not in watchlist:
                        watchlist.append(t)
                        await tg(session,"Added "+t+"! Watching: "+", ".join(watchlist))
                    else:
                        await tg(session,t+" already in watchlist: "+", ".join(watchlist))
                except: await tg(session,"Usage: /add RGTI")
            elif text.startswith("/remove "):
                try:
                    t=text.split()[1].upper()
                    if t in watchlist:
                        watchlist.remove(t)
                        await tg(session,"Removed "+t+". Watching: "+", ".join(watchlist))
                    else:
                        await tg(session,t+" not in watchlist")
                except: await tg(session,"Usage: /remove RGTI")
            elif text=="/autotrade on": AUTO_TRADE=True; await tg(session,"Auto-trading ENABLED [PAPER MODE]")
            elif text=="/autotrade off": AUTO_TRADE=False; await tg(session,"Auto-trading DISABLED - signals only")
            elif text=="/account":
                try:
                    acct=await get_account_info(session); pos=await get_open_positions(session)
                    if acct:
                        lines=["Alpaca PAPER Account","","Equity: $"+str(acct['equity']),"Cash: $"+str(acct['cash']),"Buying Power: $"+str(acct['buying_power']),"Open Positions: "+str(len(pos)),""]
                        for sym,p in pos.items():
                            pnl=round(float(p.get("unrealized_pl",0)),2)
                            lines.append(sym+": "+p.get("qty","")+" shares P&L: $"+str(pnl))
                        await tg(session,"\n".join(lines))
                except Exception as e: await tg(session,"Account error: "+str(e)[:100])
            elif text=="/positions":
                try:
                    pos=await get_open_positions(session)
                    if not pos: await tg(session,"No open positions")
                    else:
                        lines=["Open Positions:",""]
                        for sym,p in pos.items():
                            pnl=round(float(p.get("unrealized_pl",0)),2)
                            pnl_pct=round(float(p.get("unrealized_plpc",0))*100,2)
                            lines+=[sym+" | "+p.get("side","").upper(),"  Qty: "+p.get("qty","")+" Entry: $"+p.get("avg_entry_price",""),"  P&L: $"+str(pnl)+" ("+str(pnl_pct)+"%)" ,""]
                        await tg(session,"\n".join(lines))
                except Exception as e: await tg(session,"Positions error: "+str(e)[:100])
            elif text.startswith("/close "):
                t=text.split()[1].upper()
                await close_position(session,t); await tg(session,"Closed: "+t)
            elif text=="/closeall":
                await close_all_positions(session); await tg(session,"All positions closed!")
            elif text=="/scan":
                new_wl=await smart_daily_scan(session)
                watchlist.clear(); watchlist.extend(new_wl)
            elif text=="/watchlist": await tg(session,"Watching: "+", ".join(watchlist) if watchlist else "Empty - send /add TICKER")
            elif text=="/pause": bot_paused=True; await tg(session,"Bot paused.")
            elif text=="/resume": bot_paused=False; await tg(session,"Bot resumed!")
            elif text=="/report": await daily_report(session)
            elif text=="/weekly": await weekly_report(session)
            elif text.startswith("/risk "):
                try: ACCOUNT_SIZE=float(text.split()[1]); await tg(session,"Account size: $"+str(ACCOUNT_SIZE))
                except: await tg(session,"Usage: /risk 10000")
            elif text.startswith("/alert "):
                try:
                    parts=text.split(); t=parts[1].upper(); target=float(parts[2])
                    p,_=await get_realtime_price(session,t)
                    direction="above" if p and target>p else "below"
                    if t not in price_alerts: price_alerts[t]=[]
                    price_alerts[t].append({"price":target,"direction":direction})
                    await tg(session,"Alert set! "+t+" "+direction+" $"+str(target))
                except: await tg(session,"Usage: /alert RGTI 25.00")
            elif text=="/brief": await morning_brief(session)
            elif text.startswith("/backtest"):
                parts=text.split()
                tickers_bt=[parts[1].upper()] if len(parts)>1 else [c['ticker'] for c in daily_picks] if daily_picks else ["RGTI","MARA"]
                await tg(session,"Backtesting: "+", ".join(tickers_bt)+"...")
                for t in tickers_bt:
                    try:
                        bt=await run_backtest(session,t)
                        if bt:
                            grade="GOOD" if bt['win_rate']>=55 else "OK" if bt['win_rate']>=45 else "POOR"
                            await tg(session,"Backtest "+bt['ticker']+" - "+grade+"\nWin Rate: "+str(bt['win_rate'])+"%\nTrades: "+str(bt['total_trades'])+" | W:"+str(bt['wins'])+" L:"+str(bt['losses'])+"\nAvg P&L: "+str(bt['avg_pnl'])+"%\nTotal P&L: "+str(bt['total_pnl'])+"%")
                        else: await tg(session,"Not enough data for "+t)
                    except Exception as e: await tg(session,"Backtest error "+t+": "+str(e)[:80])
                    await asyncio.sleep(1)
            elif text=="/help":
                lines=["AlphaSignal SMC Auto-Trade Bot","",
                       "AUTO-TRADING:","/autotrade on/off","/account - balance & P&L",
                       "/positions - open trades","/close RGTI - close one","/closeall - close all",
                       "","SCANNING:","/scan - find today's best stocks","/watchlist - today's picks",
                       "/backtest - test strategy",
                       "","CONTROL:","/status","/pause","/resume","/report","/weekly",
                       "/risk 10000","/alert RGTI 25.00","/brief","/help"]
                await tg(session,"\n".join(lines))
        except Exception as e:
            print("CMD err:",e)
            try: await tg(session,"Command error: "+str(e)[:80])
            except: pass
    return offset

# ── MAIN ─────────────────────────────────────────────────
async def main():
    global last_update_id, morning_brief_sent, trades_today, watchlist
    global last_scan_day, weekly_report_sent, positions_closed_today
    print("AlphaSignal SMC Auto-Trader starting...")
    last_report_day=-1

    await asyncio.sleep(10)  # prevent duplicate startup messages

    async with aiohttp.ClientSession() as session:
        await tg(session,
            "AlphaSignal SMC AUTO-TRADER Started!\n\n"
            "PAPER TRADING MODE\n"
            "Account: $"+str(ACCOUNT_SIZE)+"\n\n"
            "Every morning at 9:20 AM NY (2:20 PM UK):\n"
            "Bot scans 100+ stocks\n"
            "Picks TOP 3 best for the day\n"
            "Auto-trades them with SMC strategy\n"
            "Closes all positions at 3:45 PM NY\n\n"
            "Leverage: HIGH=1:2, MEDIUM=1:1\n"
            "Risk per trade: "+str(RISK_PCT)+"% = $"+str(round(ACCOUNT_SIZE*RISK_PCT/100,2))+"\n\n"
            "/help for all commands")

        while True:
            try: last_update_id=await handle_cmds(session,last_update_id)
            except Exception as e: print("CMD err:",e)

            status=market_status(); ny=get_ny()

            # Reset daily at midnight
            if ny.hour==0 and ny.minute==0: reset_daily()

            # Heartbeat every 30 min during market hours
            if status=="OPEN" and ny.minute in [0,30] and ny.second < 65:
                score_summary = []
                for t in watchlist:
                    if t in no_signal_count:
                        score_summary.append(t+":"+str(no_signal_count.get(t,0))+"no-sig")
                print("Heartbeat - watching:",watchlist,"no-signal counts:",no_signal_count)

            # Pre-market Stocktwits scan at 8:00 AM NY (1:00 PM UK)
            if ny.hour==8 and ny.minute==0 and ny.weekday()<5:
                try: await stocktwits_premarket_scan(session)
                except Exception as e: print("Pre-market scan err:",e)

            # After-market Stocktwits scan at 4:30 PM NY (9:30 PM UK)
            if ny.hour==16 and ny.minute==30 and ny.weekday()<5:
                try: await stocktwits_premarket_scan(session)
                except Exception as e: print("After-market scan err:",e)

            # Smart daily scan at 9:20 AM NY (only once per day)
            if ny.hour==9 and ny.minute==20 and ny.weekday()<5 and ny.day!=last_scan_day:
                last_scan_day=ny.day  # set FIRST to prevent duplicate
                try:
                    new_wl=await smart_daily_scan(session)
                    watchlist.clear(); watchlist.extend(new_wl)
                except Exception as e: print("Scan err:",e)

            # Morning brief at 9:25 AM (only once per day)
            if ny.hour==9 and ny.minute==25 and not morning_brief_sent and ny.weekday()<5:
                morning_brief_sent=True  # set FIRST to prevent duplicate
                try: await morning_brief(session)
                except: pass
            if ny.hour==9 and ny.minute==26: morning_brief_sent=False

            # Close ALL positions at 3:45 PM
            if ny.hour==CLOSE_ALL_TIME[0] and ny.minute==CLOSE_ALL_TIME[1] and not positions_closed_today:
                try:
                    pos=await get_open_positions(session)
                    if pos:
                        await close_all_positions(session)
                        await tg(session,"3:45 PM - Closing all positions!\nTickers: "+", ".join(pos.keys()))
                    positions_closed_today=True
                    active_trades.clear(); trailing_stops.clear()
                except Exception as e: print("EOD close err:",e)

            # Daily report at 4:05 PM
            if ny.hour==16 and ny.minute==5 and ny.day!=last_report_day:
                try: await daily_report(session); last_report_day=ny.day
                except: pass

            # Weekly report Sunday 8 PM
            if ny.weekday()==6 and ny.hour==20 and ny.minute==0 and not weekly_report_sent:
                try: await weekly_report(session); weekly_report_sent=True
                except: pass
            if ny.weekday()==0: weekly_report_sent=False

            if status=="CLOSED": await asyncio.sleep(300); continue
            if bot_paused: await asyncio.sleep(30); continue

            try:
                if active_trades: await check_trailing(session)
                if price_alerts: await check_alerts(session)
            except Exception as e: print("Trailing err:",e)

            print("["+ny.strftime("%H:%M:%S")+" NY]",status,"|",watchlist)

            # Track no-signal count per ticker for rescan
            all_no_signal = True

            for ticker in list(watchlist):
                try:
                    can,reason=check_daily_limits()
                    if not can: print("  BLOCKED:",reason); break

                    bars_1m,bars_5m,bars_15m,bars_daily,bars_yest=await asyncio.gather(
                        get_bars(session,ticker,"1Min",30),
                        get_bars(session,ticker,"5Min",50),
                        get_bars(session,ticker,"15Min",50),
                        get_bars(session,ticker,"1Day",5),
                        get_bars(session,ticker,"1Day",3),
                    )
                    if len(bars_5m)<20: continue
                    update_orb(ticker,bars_1m)
                    result=compute_signal(bars_1m,bars_5m,bars_15m)
                    if result is None:
                        print("    WAIT - no signal")
                        no_signal_count[ticker] = no_signal_count.get(ticker,0) + 1
                        continue
                    score = result['buy_score'] if result.get('signal')=='BUY' else result.get('sell_score',0)
                    print("    "+ticker+" Signal:"+result.get('signal','')+" Score:"+str(score)+" RSI:"+str(result.get('rsi'))+" Zone:"+str(result.get('zone')))
                    no_signal_count[ticker] = 0  # reset on signal found
                    all_no_signal = False
                    if is_duplicate(ticker,result["signal"]): print("    Duplicate - skipping"); continue

                    tf_agrees=check_3tf(bars_1m,bars_5m,bars_15m,result["signal"])
                    if REQUIRE_3TF_AGREE and tf_agrees<2: continue

                    spy_trend,spy_chg,_=await get_spy_trend(session)
                    if SPY_FILTER:
                        if result["signal"]=="BUY" and spy_trend=="BEAR": continue
                        if result["signal"]=="SELL" and spy_trend=="BULL": continue

                    # SMT handled by score penalty in compute_signal
                    if result.get("smt"):
                        print("    "+ticker+" SMT: "+str(result['smt'])+" - score penalised, continuing")

                    rvol=calc_rvol(bars_1m,bars_yest)
                    multiday=get_multiday(bars_daily)
                    orb_tag=check_orb(ticker,result["entry"]) or ""
                    patterns=detect_patterns(bars_5m)
                    sentiment,headlines=await get_news(session,ticker)
                    if sentiment=="NEGATIVE" and result["signal"]=="BUY": continue

                    st_data=await get_stocktwits(session,ticker)
                    if st_data["sentiment"]=="BEARISH" and st_data["bear_pct"]>=70 and result["signal"]=="BUY": continue

                    # Block BUY when RSI overbought or SELL when oversold
                    if result["signal"]=="BUY" and result.get("rsi") and result["rsi"] > 70:
                        print("    "+ticker+" BUY blocked - RSI overbought: "+str(result['rsi']))
                        continue
                    if result["signal"]=="SELL" and result.get("rsi") and result["rsi"] < 30:
                        print("    "+ticker+" SELL blocked - RSI oversold: "+str(result['rsi']))
                        continue

                    # Block BUY when price in PREMIUM zone with high RSI
                    if result["signal"]=="BUY" and result.get("zone")=="PREMIUM" and result.get("rsi",0)>65:
                        print("    "+ticker+" BUY blocked - PREMIUM zone + high RSI")
                        continue

                    # Block BUY on strong bearish candlestick patterns
                    bearish_patterns = ["Bearish Engulfing","Bearish Marubozu","Shooting Star","Evening Star"]
                    bullish_patterns  = ["Bullish Engulfing","Bullish Marubozu","Hammer","Morning Star"]
                    if result["signal"]=="BUY" and any(p in patterns for p in bearish_patterns):
                        print("    BUY blocked - bearish pattern on "+ticker)
                        continue
                    if result["signal"]=="SELL" and any(p in patterns for p in bullish_patterns):
                        print("    SELL blocked - bullish pattern on "+ticker)
                        continue

                    ai=await ai_confirm(session,ticker,result,patterns,sentiment,tf_agrees)
                    if ai.get("verdict")=="REJECTED":
                        print("    AI REJECTED "+ticker+": "+ai.get("reason",""))
                        continue

                    yp,price_source=await get_realtime_price(session,ticker)
                    _,yc,_=await yahoo_price(session,ticker)
                    # Get Finnhub day high/low for context
                    _,fh_chg2,fh_day_high,fh_day_low=await get_finnhub_price(session,ticker)

                    # Price deviation filter - silent, just remove and replace
                    if yp and result['entry']:
                        dev=abs(yp-result['entry'])/result['entry']*100
                        if result['signal']=="BUY" and yp>result['entry']*1.01:
                            print("    EXPIRED "+ticker+" +"+str(round(dev,1))+"%")
                            if ticker in watchlist: watchlist.remove(ticker)
                            asyncio.ensure_future(replace_ticker(ticker))
                            continue
                        if result['signal']=="SELL" and yp<result['entry']*0.99:
                            print("    EXPIRED "+ticker+" -"+str(round(dev,1))+"%")
                            if ticker in watchlist: watchlist.remove(ticker)
                            asyncio.ensure_future(replace_ticker(ticker))
                            continue

                    # Calculate position with leverage
                    acct=await get_account_info(session)
                    equity=acct['equity'] if acct else ACCOUNT_SIZE
                    shares,cost,leverage=calc_position_size(result['entry'],result['sl'],result['confidence'],equity)

                    score=result['buy_score'] if result['signal']=='BUY' else result['sell_score']
                    print("    SIGNAL:",result['signal'],ticker,"Score:"+str(score),"Lev:1:"+str(leverage))

                    msg=build_msg(ticker,result,yp,yc,status,sentiment,headlines,patterns,ai,
                                  rvol,multiday,spy_trend,spy_chg,tf_agrees,orb_tag,shares,cost,leverage,st_data,price_source)
                    await tg(session,msg)

                    # ── Finnhub accuracy check before trading ─────────
                    # Get Finnhub price to confirm entry accuracy
                    fh_price,fh_chg,fh_high,fh_low = await get_finnhub_price(session,ticker)
                    if fh_price:
                        fh_dev = abs(fh_price - result['entry']) / result['entry'] * 100
                        print("    Finnhub confirm: $"+str(fh_price)+" vs entry $"+str(result['entry'])+" dev:"+str(round(fh_dev,1))+"%")
                        if fh_dev > 1.5:
                            print("    Finnhub: price moved too far - updating entry")
                            result['entry'] = fh_price  # use Finnhub price as entry
                        # Update SL/TP based on Finnhub price
                        if result['atr']:
                            if result['signal']=="BUY":
                                result['sl'] = round(fh_price - result['atr']*1.5, 4)
                                result['tp'] = round(fh_price + result['atr']*3.0, 4)
                            else:
                                result['sl'] = round(fh_price + result['atr']*1.5, 4)
                                result['tp'] = round(fh_price - result['atr']*3.0, 4)

                    # AUTO TRADE
                    if AUTO_TRADE and shares>0:
                        try:
                            pos=await get_open_positions(session)
                            if len(pos)>=MAX_OPEN_TRADES:
                                await tg(session,"Auto-trade skipped: max "+str(MAX_OPEN_TRADES)+" positions open")
                            elif ticker in pos:
                                await tg(session,"Auto-trade skipped: already in "+ticker)
                            else:
                                side="buy" if result["signal"]=="BUY" else "sell"
                                order=await place_order(session,ticker,side,shares,result['sl'],result['tp'])
                                if order["success"]:
                                    await tg(session,
                                        "AUTO-TRADE PLACED [PAPER]\n\n"
                                        +result["signal"]+" "+ticker+"\n"
                                        "Qty: "+str(shares)+" shares\n"
                                        "Entry: $"+str(result['entry'])+"\n"
                                        "SL: $"+str(result['sl'])+"\n"
                                        "TP: $"+str(result['tp'])+"\n"
                                        "Leverage: 1:"+str(leverage)+"\n"
                                        "Cost: $"+str(cost)+"\n"
                                        "Account: $"+str(equity))
                                else:
                                    await tg(session,"Auto-trade FAILED: "+str(order.get("error",""))[:100])
                        except Exception as e:
                            await tg(session,"Auto-trade error: "+str(e)[:100])

                    active_trades[ticker]={"signal":result["signal"],"entry":result["entry"],
                                           "sl":result["sl"],"tp":result["tp"],"atr":result["atr"]}
                    trailing_stops[ticker]=result["sl"]
                    last_signal_time[ticker]=datetime.now()
                    last_signal_type[ticker]=result["signal"]
                    trades_today+=1
                    await asyncio.sleep(2)

                except Exception as e: print("  ERROR",ticker,":",e); continue

            # ── Continuous rescan - replace any stock with no signal ──
            if status=="OPEN":
                for ticker in list(watchlist):
                    count = no_signal_count.get(ticker, 0)
                    if count >= 3:  # 3 consecutive no-signals = replace
                        print("  No signal on "+ticker+" for "+str(count)+" scans - replacing")
                        if ticker in watchlist:
                            watchlist.remove(ticker)
                        asyncio.ensure_future(replace_ticker(ticker))
                        no_signal_count[ticker] = 0

            await asyncio.sleep(SCAN_INTERVAL)

async def run_all():
    await asyncio.gather(main(), alpaca_websocket())

if __name__=="__main__":
    asyncio.run(run_all())
