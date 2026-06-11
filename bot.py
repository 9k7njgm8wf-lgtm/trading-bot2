import asyncio
import aiohttp
import json
import time
from datetime import datetime, timezone, timedelta

# ── CONFIG ────────────────────────────────────────────────
GROQ_API_KEY     = "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"
ALPACA_API_KEY   = "AKPYM4PLBJBOEBSD3NQQVEDALI"
ALPACA_SECRET    = "6Z73eeNG8Fpa64Tw7UBaEULCyFHJRCSh6r3kRgC7k2qo"
TELEGRAM_TOKEN   = "8855798705:AAFhs2RYnLUVxR-N2C2urTzl445NZn2fxv8"
TELEGRAM_CHAT_ID = "6903579390"

BASE_TICKERS       = ["RGTI", "RXT", "QUBT", "LUNR"]
SCAN_INTERVAL      = 60
SIGNAL_COOLDOWN    = 900
MIN_SCORE          = 7
ACCOUNT_SIZE       = 1000
RISK_PCT           = 2
MAX_TRADES_PER_DAY = 3
DAILY_LOSS_LIMIT   = 3.0
LOSING_STREAK_LIMIT= 3
BEST_HOURS         = [(9,30,11,0),(15,0,16,0)]
SPY_FILTER         = True
TIME_FILTER        = True
REQUIRE_3TF_AGREE  = True

ALPACA_BASE    = "https://data.alpaca.markets/v2"
ALPACA_HEADERS = {"APCA-API-KEY-ID":ALPACA_API_KEY,"APCA-API-SECRET-KEY":ALPACA_SECRET}

# ── STATE ─────────────────────────────────────────────────
last_signal_time   = {}
last_signal_type   = {}
orb_levels         = {}
active_trades      = {}
performance_log    = []
all_time_log       = []
watchlist          = list(BASE_TICKERS)
bot_paused         = False
trailing_stops     = {}
last_update_id     = 0
price_alerts       = {}
morning_brief_sent = False
trades_today       = 0
daily_pnl          = 0.0
daily_loss_hit     = False
losing_streak      = 0
last_scan_day      = -1
weekly_report_sent = False

# ── TIME ──────────────────────────────────────────────────
def get_ny():
    return datetime.now(timezone.utc) + timedelta(hours=-4)

def market_status():
    ny = get_ny()
    if ny.weekday() >= 5: return "CLOSED"
    h,m = ny.hour,ny.minute
    if h < 8: return "CLOSED"
    if h < 9 or (h==9 and m<30): return "PRE_MARKET"
    if h >= 16: return "CLOSED"
    return "OPEN"

def is_best_hour():
    ny = get_ny(); t = ny.hour*60+ny.minute
    return any(sh*60+sm <= t <= eh*60+em for sh,sm,eh,em in BEST_HOURS)

def get_session():
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
        end = int(time.time())
        start = end - (days * 24 * 60 * 60)
        url = "https://query1.finance.yahoo.com/v8/finance/chart/"+ticker
        params = {"interval":"1d","period1":str(start),"period2":str(end),"range":"3mo"}
        headers = {"User-Agent":"Mozilla/5.0","Accept":"application/json"}
        async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.json()
            result = data["chart"]["result"][0]
            timestamps = result.get("timestamp",[])
            ohlcv = result["indicators"]["quote"][0]
            bars = []
            for i in range(len(timestamps)):
                try:
                    o=ohlcv["open"][i]; h=ohlcv["high"][i]
                    l=ohlcv["low"][i];  c=ohlcv["close"][i]
                    v=ohlcv["volume"][i]
                    if all([o,h,l,c,v]):
                        bars.append({"o":round(o,4),"h":round(h,4),"l":round(l,4),"c":round(c,4),"v":int(v)})
                except: continue
            return bars
    except Exception as e:
        print("Yahoo bars error "+ticker+":", e)
        return []

# ════════════════════════════════════════════════════════════
# ══ SMART MONEY CONCEPT (SMC) ENGINE ════════════════════════
# ════════════════════════════════════════════════════════════

def find_swing_highs_lows(bars, lookback=5):
    """Find significant swing highs and lows"""
    highs = []
    lows  = []
    for i in range(lookback, len(bars)-lookback):
        # Swing high: highest point in lookback window
        if bars[i]['h'] == max(b['h'] for b in bars[i-lookback:i+lookback+1]):
            highs.append({"idx":i, "price":bars[i]['h']})
        # Swing low: lowest point in lookback window
        if bars[i]['l'] == min(b['l'] for b in bars[i-lookback:i+lookback+1]):
            lows.append({"idx":i, "price":bars[i]['l']})
    return highs, lows

def detect_bos_choch(bars, highs, lows):
    """
    BOS = Break of Structure (trend continuation)
    CHoCH = Change of Character (trend reversal)
    """
    if not highs or not lows or len(bars) < 10: return None, None
    current_price = bars[-1]['c']
    last_high = highs[-1]['price'] if highs else None
    last_low  = lows[-1]['price']  if lows  else None
    prev_high = highs[-2]['price'] if len(highs) >= 2 else None
    prev_low  = lows[-2]['price']  if len(lows)  >= 2 else None

    bos   = None
    choch = None

    # BOS Bullish: price breaks above last swing high
    if last_high and current_price > last_high:
        bos = "BULLISH_BOS"

    # BOS Bearish: price breaks below last swing low
    if last_low and current_price < last_low:
        bos = "BEARISH_BOS"

    # CHoCH: previous trend was down but now breaking above high (reversal)
    if prev_high and last_low and prev_high > last_high and current_price > last_high:
        choch = "BULLISH_CHOCH"

    # CHoCH: previous trend was up but now breaking below low (reversal)
    if prev_low and last_high and prev_low < last_low and current_price < last_low:
        choch = "BEARISH_CHOCH"

    return bos, choch

def find_order_blocks(bars, lookback=20):
    """
    Order Block = last bearish candle before bullish move (bullish OB)
                  last bullish candle before bearish move (bearish OB)
    """
    obs = []
    recent = bars[-lookback:] if len(bars) >= lookback else bars
    for i in range(1, len(recent)-2):
        b = recent[i]
        next_b = recent[i+1]
        prev_b = recent[i-1]
        body = abs(b['c']-b['o'])
        if body == 0: continue

        # Bullish OB: bearish candle followed by strong bullish move
        if b['c'] < b['o']:  # bearish candle
            move_up = sum(1 for j in range(i+1, min(i+4, len(recent))) if recent[j]['c'] > recent[j]['o'])
            if move_up >= 2:
                obs.append({
                    "type": "BULLISH_OB",
                    "high": b['h'], "low": b['l'],
                    "mid":  round((b['h']+b['l'])/2, 4),
                    "idx":  i
                })

        # Bearish OB: bullish candle followed by strong bearish move
        if b['c'] > b['o']:  # bullish candle
            move_down = sum(1 for j in range(i+1, min(i+4, len(recent))) if recent[j]['c'] < recent[j]['o'])
            if move_down >= 2:
                obs.append({
                    "type": "BEARISH_OB",
                    "high": b['h'], "low": b['l'],
                    "mid":  round((b['h']+b['l'])/2, 4),
                    "idx":  i
                })

    return obs[-5:] if obs else []  # return last 5 OBs

def detect_liquidity_zones(bars, highs, lows):
    """
    Liquidity zones = areas where stop losses cluster
    - Equal highs/lows (retail SL clusters)
    - Previous swing highs/lows
    """
    liquidity = {"buy_side": [], "sell_side": []}
    if not highs or not lows: return liquidity

    # Buy-side liquidity: above swing highs (retail sell stop clusters)
    for h in highs[-3:]:
        liquidity["buy_side"].append(round(h['price'] * 1.001, 4))

    # Sell-side liquidity: below swing lows (retail buy stop clusters)
    for l in lows[-3:]:
        liquidity["sell_side"].append(round(l['price'] * 0.999, 4))

    return liquidity

def detect_liquidity_sweep(bars, highs, lows):
    """
    Liquidity sweep = price breaks key high/low then immediately reverses
    This is where smart money grabs liquidity before the real move
    """
    if len(bars) < 5 or not highs or not lows: return None

    current  = bars[-1]
    prev     = bars[-2]
    last_high = highs[-1]['price'] if highs else None
    last_low  = lows[-1]['price']  if lows  else None

    # Bullish sweep: price dipped below swing low then closed above it
    if last_low and prev['l'] < last_low and current['c'] > last_low:
        return "BULLISH_SWEEP"  # Smart money grabbed sell-side liquidity → expect up

    # Bearish sweep: price spiked above swing high then closed below it
    if last_high and prev['h'] > last_high and current['c'] < last_high:
        return "BEARISH_SWEEP"  # Smart money grabbed buy-side liquidity → expect down

    return None

def detect_fvg(bars):
    """
    Fair Value Gap (FVG) = imbalance in price where no trading occurred
    3-candle pattern: gap between candle 1 high and candle 3 low (bullish)
    or candle 1 low and candle 3 high (bearish)
    """
    fvgs = []
    if len(bars) < 3: return fvgs

    for i in range(len(bars)-3):
        b1, b2, b3 = bars[i], bars[i+1], bars[i+2]

        # Bullish FVG: gap between b1 high and b3 low (price moved up too fast)
        if b3['l'] > b1['h']:
            fvgs.append({
                "type":  "BULLISH_FVG",
                "top":   b3['l'],
                "bottom": b1['h'],
                "mid":   round((b3['l']+b1['h'])/2, 4)
            })

        # Bearish FVG: gap between b1 low and b3 high (price moved down too fast)
        if b3['h'] < b1['l']:
            fvgs.append({
                "type":  "BEARISH_FVG",
                "top":   b1['l'],
                "bottom": b3['h'],
                "mid":   round((b1['l']+b3['h'])/2, 4)
            })

    return fvgs[-3:] if fvgs else []

def detect_power_of_3(bars):
    """
    Power of 3 = Accumulation → Manipulation → Distribution
    - Accumulation: sideways/tight range
    - Manipulation: fake move to grab liquidity
    - Distribution: real move in smart money direction
    """
    if len(bars) < 15: return "UNKNOWN"

    recent = bars[-15:]
    closes = [b['c'] for b in recent]
    highs_list  = [b['h'] for b in recent]
    lows_list   = [b['l'] for b in recent]

    # Measure range compression (accumulation = tight range)
    first_range = max(highs_list[:5]) - min(lows_list[:5])
    mid_range   = max(highs_list[5:10]) - min(lows_list[5:10])
    last_range  = max(highs_list[10:]) - min(lows_list[10:])

    avg_price = sum(closes) / len(closes)
    first_range_pct = (first_range / avg_price) * 100
    mid_range_pct   = (mid_range / avg_price) * 100
    last_range_pct  = (last_range / avg_price) * 100

    # Accumulation → Manipulation → Distribution pattern
    if first_range_pct < 2 and mid_range_pct > first_range_pct * 1.5:
        # Was tight, then expanded (manipulation phase)
        if closes[-1] > closes[-5]:
            return "DISTRIBUTION_BULLISH"  # Real move up
        else:
            return "DISTRIBUTION_BEARISH"  # Real move down
    elif first_range_pct < 1.5:
        return "ACCUMULATION"  # Still accumulating
    elif mid_range_pct > last_range_pct * 1.5:
        return "MANIPULATION"  # Possible fake move
    else:
        return "TRENDING"

def detect_smart_money_trap(bars, highs, lows):
    """
    Smart Money Trap = fake breakout to lure retail traders
    Price breaks key level, retail buys/sells, then SM reverses
    """
    if len(bars) < 5 or not highs or not lows: return None

    current = bars[-1]
    prev    = bars[-2]
    last_high = highs[-1]['price'] if highs else None
    last_low  = lows[-1]['price']  if lows  else None

    # Bull trap: broke above resistance but closed back below (fake breakout up)
    if last_high and prev['h'] > last_high and current['c'] < last_high:
        if current['c'] < current['o']:  # closed bearish
            return "BULL_TRAP"  # Retail bought breakout → will drop

    # Bear trap: broke below support but closed back above (fake breakdown)
    if last_low and prev['l'] < last_low and current['c'] > last_low:
        if current['c'] > current['o']:  # closed bullish
            return "BEAR_TRAP"  # Retail sold breakdown → will rise

    return None

def find_premium_discount_zones(bars, highs, lows):
    """
    Smart money buys in discount (below equilibrium) sells in premium (above)
    Equilibrium = 50% of the range
    """
    if not highs or not lows: return None, None, None
    range_high = highs[-1]['price']
    range_low  = lows[-1]['price']
    equilibrium = round((range_high + range_low) / 2, 4)
    current_price = bars[-1]['c']

    if current_price > equilibrium:
        zone = "PREMIUM"   # Price is expensive, look for sells
    elif current_price < equilibrium:
        zone = "DISCOUNT"  # Price is cheap, look for buys
    else:
        zone = "EQUILIBRIUM"

    return zone, equilibrium, round((current_price - equilibrium) / equilibrium * 100, 2)

# ════════════════════════════════════════════════════════════
# ══ STANDARD INDICATORS ═════════════════════════════════════
# ════════════════════════════════════════════════════════════

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
    if body2>0 and (l2<min(o2,c2)) and (min(o2,c2)-l2)>2*body2 and c2>o2: patterns.append("Hammer")
    if body2>0 and (h2>max(o2,c2)) and (h2-max(o2,c2))>2*body2 and c2<o2: patterns.append("Shooting Star")
    if c1<o1 and c2>o2 and c2>o1 and o2<c1: patterns.append("Bullish Engulfing")
    if c1>o1 and c2<o2 and c2<o1 and o2>c1: patterns.append("Bearish Engulfing")
    if c0<o0 and body1<abs(c0-o0)*0.5 and c2>o2 and c2>(o0+c0)/2: patterns.append("Morning Star")
    if c0>o0 and body1<abs(c0-o0)*0.5 and c2<o2 and c2<(o0+c0)/2: patterns.append("Evening Star")
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

def check_3tf_agreement(bars_1m, bars_5m, bars_15m, signal):
    agrees = 0
    for bars in [bars_1m, bars_5m, bars_15m]:
        if len(bars) < 10: continue
        closes = [b['c'] for b in bars]
        ema9  = calc_ema(closes, 9)
        ema21 = calc_ema(closes, min(21, len(closes)-1))
        if not ema9 or not ema21: continue
        if signal == "BUY"  and ema9 > ema21: agrees += 1
        if signal == "SELL" and ema9 < ema21: agrees += 1
    return agrees

def calc_position(entry, sl):
    risk_pct = RISK_PCT
    reduced  = False
    if losing_streak >= LOSING_STREAK_LIMIT:
        risk_pct = max(0.5, RISK_PCT/2); reduced = True
    risk_amt = ACCOUNT_SIZE*(risk_pct/100); rps=abs(entry-sl)
    if rps<=0: return 0,0,risk_pct,reduced
    shares=int(risk_amt/rps)
    return shares,round(shares*entry,2),risk_pct,reduced

def check_daily_limits():
    if trades_today >= MAX_TRADES_PER_DAY:
        return False, "Max trades ("+str(MAX_TRADES_PER_DAY)+"/day)"
    if daily_pnl <= -DAILY_LOSS_LIMIT:
        return False, "Daily loss limit (-"+str(DAILY_LOSS_LIMIT)+"%)"
    return True, ""

def reset_daily_stats():
    global trades_today, daily_pnl, daily_loss_hit
    trades_today=0; daily_pnl=0.0; daily_loss_hit=False

# ════════════════════════════════════════════════════════════
# ══ MAIN SMC + TRADITIONAL SIGNAL ═══════════════════════════
# ════════════════════════════════════════════════════════════

def compute_signal(bars_1m, bars_5m, bars_15m):
    if len(bars_5m) < 20: return None
    closes = [b['c'] for b in bars_5m]
    price  = closes[-1]

    # Standard indicators
    vwap  = calc_vwap(bars_5m); vn,vp = vwap[-1],vwap[-2]
    rsi   = calc_rsi(closes)
    ema9  = calc_ema(closes,9); ema21 = calc_ema(closes,21)
    atr   = calc_atr(bars_5m)
    vol_r = calc_vol_ratio(bars_5m)
    sup,res = get_sr(bars_5m)
    bbu,bbm,bbl = calc_bb(closes)

    # 15m trend
    trend_15m = "NEUTRAL"
    if len(bars_15m)>=21:
        c15=[b['c'] for b in bars_15m]; e9,e21=calc_ema(c15,9),calc_ema(c15,21)
        if e9 and e21: trend_15m="BULL" if e9>e21 else "BEAR"

    # 1m momentum
    mom_1m = "NEUTRAL"
    if len(bars_1m)>=5:
        c1=[b['c'] for b in bars_1m[-5:]]
        mom_1m="UP" if c1[-1]>c1[0] else "DOWN"

    # ── SMC Analysis ──────────────────────────────────────
    highs, lows = find_swing_highs_lows(bars_5m)
    bos, choch  = detect_bos_choch(bars_5m, highs, lows)
    order_blocks = find_order_blocks(bars_5m)
    fvgs         = detect_fvg(bars_5m)
    liq_sweep    = detect_liquidity_sweep(bars_5m, highs, lows)
    smt          = detect_smart_money_trap(bars_5m, highs, lows)
    po3          = detect_power_of_3(bars_5m)
    zone, equilibrium, zone_pct = find_premium_discount_zones(bars_5m, highs, lows)
    liq_zones    = detect_liquidity_zones(bars_5m, highs, lows)

    # Find relevant order block for current price
    nearest_ob = None
    for ob in reversed(order_blocks):
        if ob['type']=="BULLISH_OB" and ob['low'] <= price <= ob['high']*1.02:
            nearest_ob = ob; break
        if ob['type']=="BEARISH_OB" and ob['low']*0.98 <= price <= ob['high']:
            nearest_ob = ob; break

    # Find relevant FVG
    nearest_fvg = None
    for fvg in reversed(fvgs):
        if fvg['bottom'] <= price <= fvg['top']:
            nearest_fvg = fvg; break

    # ── Scoring System (Traditional + SMC) ───────────────
    bs = ss = 0
    above = price > vn
    ca = price>vn and closes[-2]<=vp
    cb = price<vn and closes[-2]>=vp
    hv = vol_r and vol_r>=1.5
    eb = ema9>ema21 if ema9 and ema21 else False

    # Traditional indicators
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

    # ── SMC Scoring (extra points) ────────────────────────
    # BOS/CHoCH
    if bos=="BULLISH_BOS":   bs+=2
    if bos=="BEARISH_BOS":   ss+=2
    if choch=="BULLISH_CHOCH": bs+=3  # reversal = strong signal
    if choch=="BEARISH_CHOCH": ss+=3

    # Order block
    if nearest_ob:
        if nearest_ob['type']=="BULLISH_OB": bs+=3
        if nearest_ob['type']=="BEARISH_OB": ss+=3

    # Liquidity sweep (very strong signal)
    if liq_sweep=="BULLISH_SWEEP": bs+=3
    if liq_sweep=="BEARISH_SWEEP": ss+=3

    # Fair Value Gap
    if nearest_fvg:
        if nearest_fvg['type']=="BULLISH_FVG": bs+=2
        if nearest_fvg['type']=="BEARISH_FVG": ss+=2

    # Power of 3
    if po3=="DISTRIBUTION_BULLISH": bs+=2
    if po3=="DISTRIBUTION_BEARISH": ss+=2
    if po3=="MANIPULATION": bs-=1; ss-=1  # avoid manipulation phase

    # Premium/Discount zones
    if zone=="DISCOUNT": bs+=2   # cheap price = buy opportunity
    if zone=="PREMIUM":  ss+=2   # expensive price = sell opportunity

    # Smart Money Trap (BLOCK opposite signal)
    if smt=="BULL_TRAP": bs-=3; ss+=2  # don't buy a bull trap
    if smt=="BEAR_TRAP": ss-=3; bs+=2  # don't sell a bear trap

    # ── Decision ──────────────────────────────────────────
    if bs>=MIN_SCORE: signal="BUY";  conf="HIGH" if bs>=10 else "MEDIUM"
    elif ss>=MIN_SCORE: signal="SELL"; conf="HIGH" if ss>=10 else "MEDIUM"
    else: return None

    # SMC-based SL/TP (smarter than ATR alone)
    if signal=="BUY":
        # SL below nearest order block or swing low
        if nearest_ob and nearest_ob['type']=="BULLISH_OB":
            sl = round(nearest_ob['low'] * 0.998, 4)
        elif lows:
            sl = round(lows[-1]['price'] * 0.998, 4)
        elif atr:
            sl = round(price - atr*1.5, 4)
        else:
            sl = round(price * 0.98, 4)
        # TP at next liquidity zone (buy side) or resistance
        tp_targets = liq_zones.get("buy_side", [])
        tp = round(tp_targets[0], 4) if tp_targets else (
             round(res * 0.998, 4) if res else round(price + atr*3, 4) if atr else round(price*1.04,4))
    else:
        if nearest_ob and nearest_ob['type']=="BEARISH_OB":
            sl = round(nearest_ob['high'] * 1.002, 4)
        elif highs:
            sl = round(highs[-1]['price'] * 1.002, 4)
        elif atr:
            sl = round(price + atr*1.5, 4)
        else:
            sl = round(price * 1.02, 4)
        tp_targets = liq_zones.get("sell_side", [])
        tp = round(tp_targets[0], 4) if tp_targets else (
             round(sup * 1.002, 4) if sup else round(price - atr*3, 4) if atr else round(price*0.96,4))

    risk=abs(price-sl); reward=abs(tp-price)
    rr="1:"+str(round(reward/risk,1)) if risk>0 else "N/A"

    return {
        "signal":signal,"confidence":conf,"entry":round(price,4),
        "sl":sl,"tp":tp,"rr":rr,
        "vwap":vn,"rsi":rsi,"ema9":ema9,"ema21":ema21,"atr":atr,
        "vol_ratio":vol_r,"trend":"UPTREND" if eb else "DOWNTREND",
        "trend_15m":trend_15m,"momentum_1m":mom_1m,
        "bb_upper":bbu,"bb_lower":bbl,
        "buy_score":bs,"sell_score":ss,
        "support":sup,"resistance":res,
        "bb_squeeze":(bbu-bbl)<atr*2 if bbu and bbl and atr else False,
        # SMC fields
        "bos":bos,"choch":choch,
        "nearest_ob":nearest_ob,
        "nearest_fvg":nearest_fvg,
        "liq_sweep":liq_sweep,
        "smt":smt,"po3":po3,
        "zone":zone,"equilibrium":equilibrium,"zone_pct":zone_pct,
        "liq_zones":liq_zones,
    }

# ── NEWS & AI ────────────────────────────────────────────
async def get_news(session, ticker):
    try:
        async with session.get("https://query1.finance.yahoo.com/v1/finance/search",
            params={"q":ticker,"newsCount":3}, headers={"User-Agent":"Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            data=await r.json()
            news=data.get("news",[])
            if not news: return "NEUTRAL",[]
            headlines=[n.get("title","") for n in news[:3]]
            neg=["downgrade","loss","miss","decline","drop","fail","lawsuit","sec","fraud","warning","cut","crash"]
            pos=["upgrade","beat","surge","rally","buy","bullish","growth","profit","deal","launch","record"]
            nc=sum(1 for h in headlines for w in neg if w in h.lower())
            pc=sum(1 for h in headlines for w in pos if w in h.lower())
            return ("NEGATIVE" if nc>pc else "POSITIVE" if pc>nc else "NEUTRAL"),headlines
    except: return "NEUTRAL",[]

async def ai_confirm(session, ticker, result, patterns, sentiment, tf_agrees):
    smc_context = (
        "SMC Analysis: BOS="+str(result.get('bos'))+" CHoCH="+str(result.get('choch'))+
        " OrderBlock="+str(result.get('nearest_ob',{}).get('type') if result.get('nearest_ob') else 'None')+
        " LiqSweep="+str(result.get('liq_sweep'))+
        " SmartMoneyTrap="+str(result.get('smt'))+
        " PowerOf3="+str(result.get('po3'))+
        " Zone="+str(result.get('zone'))+" ("+str(result.get('zone_pct'))+"% from equilibrium)"+
        " FVG="+str(result.get('nearest_fvg',{}).get('type') if result.get('nearest_fvg') else 'None')
    )
    prompt = ("Analyze trade for "+ticker+": "+result['signal']+
              " Entry:$"+str(result['entry'])+" SL:$"+str(result['sl'])+" TP:$"+str(result['tp'])+
              " Score:"+str(result.get('buy_score' if result['signal']=='BUY' else 'sell_score',0))+
              " RSI:"+str(result['rsi'])+" VWAP:$"+str(result['vwap'])+
              " Vol:"+str(result['vol_ratio'])+"x 15mTrend:"+result['trend_15m']+
              " "+smc_context+
              " Patterns:"+(",".join(patterns) if patterns else "None")+
              " News:"+sentiment+" 3TF:"+str(tf_agrees)+"/3"+
              " Respond ONLY JSON: {\"verdict\":\"CONFIRMED\" or \"REJECTED\" or \"CAUTION\",\"reason\":\"one sentence\",\"tip\":\"one tip\"}")
    try:
        async with session.post("https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization":"Bearer "+GROQ_API_KEY,"Content-Type":"application/json"},
            json={"model":"llama-3.3-70b-versatile","messages":[{"role":"user","content":prompt}],"max_tokens":150,"temperature":0.1},
            timeout=aiohttp.ClientTimeout(total=10)) as r:
            data=await r.json()
            raw=data["choices"][0]["message"]["content"].strip().replace("```json","").replace("```","")
            return json.loads(raw)
    except Exception as e:
        print("AI err:",e)
        return {"verdict":"CONFIRMED","reason":"AI unavailable","tip":"Use your judgment"}

# ── FORMAT MESSAGE ───────────────────────────────────────
def build_msg(ticker, result, yp, yc, status, sentiment, headlines, patterns, ai, rvol, multiday, spy_trend, spy_chg, tf_agrees, orb_tag, shares, cost, risk_pct, reduced):
    sig=result["signal"]; now=get_ny().strftime("%H:%M:%S")+" NY"
    score=result['buy_score'] if sig=="BUY" else result['sell_score']
    verdict=ai.get("verdict","CONFIRMED")
    spy_sign="+" if spy_chg>=0 else ""
    sign="+" if yc and yc>=0 else ""

    header = ("BUY" if sig=="BUY" else "SELL")+" - "+ticker
    if orb_tag: header+=" ORB!"
    header+="  "+result['confidence']

    # SMC summary
    smc_lines = []
    if result.get('choch'):    smc_lines.append("CHoCH: "+result['choch'])
    elif result.get('bos'):    smc_lines.append("BOS: "+result['bos'])
    if result.get('liq_sweep'): smc_lines.append("Liquidity Sweep: "+result['liq_sweep'])
    if result.get('nearest_ob'): smc_lines.append("Order Block: "+result['nearest_ob']['type']+" @ $"+str(result['nearest_ob']['mid']))
    if result.get('nearest_fvg'): smc_lines.append("FVG: "+result['nearest_fvg']['type'])
    if result.get('smt'):      smc_lines.append("WARNING: "+result['smt']+" detected!")
    if result.get('po3'):      smc_lines.append("Power of 3: "+result['po3'])
    if result.get('zone'):     smc_lines.append("Zone: "+result['zone']+" ("+str(result.get('zone_pct',''))+"% from equilibrium $"+str(result.get('equilibrium',''))+")")

    lines=[
        header,
        ("MARKET OPEN" if status=="OPEN" else "PRE-MARKET")+" | "+get_session(),
        "SPY: "+spy_trend+" "+spy_sign+str(spy_chg)+"%",
        "",
        "Live Price: $"+str(yp)+" "+sign+str(yc)+"% today" if yp else "Price: $"+str(result['entry']),
        "",
        "Entry:       $"+str(result['entry']),
        "Stop Loss:   $"+str(result['sl']),
        "Take Profit: $"+str(result['tp']),
        "R/R: "+result['rr']+"  ATR: $"+str(result['atr']),
        "",
        "Position: "+str(shares)+" shares ($"+str(cost)+")",
        "Risk: $"+str(round(ACCOUNT_SIZE*risk_pct/100,2))+" ("+str(risk_pct)+"%)"
        +(" REDUCED-losing streak" if reduced else ""),
        "Trades today: "+str(trades_today+1)+"/"+str(MAX_TRADES_PER_DAY),
        "",
        "SMC ANALYSIS:",
    ]
    lines.extend(smc_lines if smc_lines else ["No SMC setup detected"])
    lines+=[
        "",
        "TECHNICALS:",
        "VWAP: $"+str(result['vwap'])+"  RSI: "+str(result['rsi']),
        "EMA 9/21: "+str(result['ema9'])+"/"+str(result['ema21']),
        "Volume: "+str(result['vol_ratio'])+"x  RVOL: "+str(rvol)+"x",
        "15m: "+result['trend_15m']+"  1m: "+result['momentum_1m'],
        "3TF Agreement: "+str(tf_agrees)+"/3",
        "Score: "+str(score)+"/20",
    ]
    if result.get("bb_squeeze"): lines.append("BB SQUEEZE - big move coming!")
    if multiday:
        lines.append("Pivot: $"+str(multiday['pivot'])+" PrevH: $"+str(multiday['prev_high'])+" PrevL: $"+str(multiday['prev_low']))
    lines+=[
        "",
        verdict+" - "+ai.get('reason',''),
        "Tip: "+ai.get('tip',''),
        "",
        "Patterns: "+(", ".join(patterns) if patterns else "None"),
        "News: "+sentiment+(" - "+headlines[0][:50] if headlines else ""),
        "",
        "S: $"+str(result['support'])+"  R: $"+str(result['resistance']),
        now
    ]
    return "\n".join(lines)

# ── TRAILING & ALERTS ────────────────────────────────────
async def check_trailing(session):
    global daily_pnl, losing_streak
    for ticker in list(active_trades.keys()):
        try:
            trade=active_trades[ticker]
            yp,_,_=await yahoo_price(session,ticker)
            if not yp: continue
            entry=trade["entry"]; sig=trade["signal"]; atr=trade.get("atr",abs(entry-trade["sl"]))
            if sig=="BUY":
                nt=round(yp-atr*1.5,4); ct=trailing_stops.get(ticker,trade["sl"])
                if nt>ct:
                    trailing_stops[ticker]=nt
                    await tg(session,"Trailing SL updated "+ticker+": $"+str(nt))
                if yp<=trailing_stops.get(ticker,trade["sl"]):
                    pnl=round((yp-entry)/entry*100,2); daily_pnl+=pnl
                    if pnl<0: losing_streak+=1
                    else: losing_streak=0
                    await tg(session,"TRAILING STOP HIT "+ticker+" Exit:$"+str(yp)+" P&L:"+str(pnl)+"%")
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
            yp,_,_=await yahoo_price(session,ticker)
            if not yp: continue
            remaining=[]
            for a in alerts:
                if (a["direction"]=="above" and yp>=a["price"]) or (a["direction"]=="below" and yp<=a["price"]):
                    await tg(session,"PRICE ALERT "+ticker+" hit $"+str(yp)+" (target "+a['direction']+" $"+str(a['price'])+")")
                else: remaining.append(a)
            price_alerts[ticker]=remaining
        except: pass

# ── SCANNER ──────────────────────────────────────────────
SCAN_UNIVERSE = [
    "RGTI","RXT","QUBT","LUNR","BBAI","SOUN","KULR","MARA","RIOT","CLSK",
    "HIMS","RDDT","RKLB","ASTS","ACHR","JOBY","NKLA","BLNK","PLUG","FCEL",
    "NVAX","SRPT","BMRN","ACAD","ITCI","MSTR","COIN","HOOD","SOFI","AFRM",
    "UPST","OPEN","CVNA","DKNG","PENN","CHWY","WISH","CLOV","SPCE","WKHS"
]

async def run_morning_scan(session):
    await tg(session,"Morning Scanner running... checking "+str(len(SCAN_UNIVERSE))+" stocks")
    candidates=[]
    for ticker in SCAN_UNIVERSE:
        try:
            p,chg,vol=await yahoo_price(session,ticker)
            if not p or not vol: continue
            bars=await get_bars(session,ticker,"1Day",10)
            if len(bars)<5: continue
            avg_vol=sum(b['v'] for b in bars[:-1])/len(bars[:-1])
            rvol=vol/avg_vol if avg_vol>0 else 0
            atr=calc_atr(bars)
            if not atr: continue
            atr_pct=(atr/p)*100
            score=0
            if rvol>=2.0: score+=3
            elif rvol>=1.5: score+=2
            elif rvol>=1.2: score+=1
            if atr_pct>=5: score+=3
            elif atr_pct>=3: score+=2
            elif atr_pct>=2: score+=1
            if abs(chg)>=5: score+=3
            elif abs(chg)>=3: score+=2
            elif abs(chg)>=1: score+=1
            if score>=4: candidates.append({"ticker":ticker,"price":p,"change":chg,"rvol":round(rvol,1),"atr_pct":round(atr_pct,1),"score":score})
            await asyncio.sleep(0.3)
        except: continue
    candidates.sort(key=lambda x:x['score'],reverse=True)

    # If no high-score candidates, lower threshold and take best available
    if not candidates:
        await tg(session,"Scanner: No high-volatility stocks found today. Using base watchlist.")
        return list(BASE_TICKERS)

    top5 = candidates[:5]
    scanner_tickers = [c['ticker'] for c in top5]

    lines = ["Morning Scan Complete! Top picks today:",""]
    for i,c in enumerate(top5,1):
        sign = "+" if c['change']>=0 else ""
        lines.append(str(i)+". "+c['ticker'])
        lines.append("   Price: $"+str(c['price'])+" "+sign+str(c['change'])+"%")
        lines.append("   RVOL: "+str(c['rvol'])+"x  ATR: "+str(c['atr_pct'])+"%  Score: "+str(c['score']))
        lines.append("")

    full_list = list(set(BASE_TICKERS + scanner_tickers))
    lines.append("Watching today: "+", ".join(full_list))
    await tg(session,"\n".join(lines))
    return full_list

# ── REPORTS ──────────────────────────────────────────────
async def morning_brief(session):
    ny=get_ny().strftime("%A %B %d")
    lines=["Morning Brief - "+ny,""]
    sp,sc,_=await yahoo_price(session,"SPY")
    qp,qc,_=await yahoo_price(session,"QQQ")
    vp,vc,_=await yahoo_price(session,"^VIX")
    lines.append("Market:")
    if sp: lines.append("  SPY $"+str(sp)+" "+("+" if sc>=0 else "")+str(sc)+"%")
    if qp: lines.append("  QQQ $"+str(qp)+" "+("+" if qc>=0 else "")+str(qc)+"%")
    if vp: lines.append("  VIX $"+str(vp)+" "+("HIGH FEAR" if vp>25 else "ELEVATED" if vp>18 else "CALM"))
    lines+=["","Watchlist:"]
    for t in watchlist:
        p,c,_=await yahoo_price(session,t)
        if p: lines.append("  "+t+": $"+str(p)+" "+("+" if c>=0 else "")+str(c)+"%")
        await asyncio.sleep(0.3)
    lines+=["","SMC Strategy Active!","Market opens in ~5 min!"]
    await tg(session,"\n".join(lines))

async def daily_report(session):
    if not performance_log:
        await tg(session,"Daily Report\n\nNo completed trades today."); return
    wins=[t for t in performance_log if t['pnl']>0]
    tp=round(sum(t['pnl'] for t in performance_log),2)
    wr=round(len(wins)/len(performance_log)*100,1)
    lines=["Daily Report","","Wins: "+str(len(wins))+" Losses: "+str(len(performance_log)-len(wins)),
           "Win Rate: "+str(wr)+"%","P&L: "+str(tp)+"%",""]
    for t in performance_log: lines.append(("WIN" if t['pnl']>0 else "LOSS")+" "+t['ticker']+" "+t['signal']+" "+str(t['pnl'])+"%")
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
        # SMC-enhanced backtest signal
        highs,lows=find_swing_highs_lows(seg)
        bos,choch=detect_bos_choch(seg,highs,lows)
        zone,_,_=find_premium_discount_zones(seg,highs,lows)
        liq_sweep=detect_liquidity_sweep(seg,highs,lows)
        smt=detect_smart_money_trap(seg,highs,lows)
        if smt: continue  # skip traps
        bull = ema9>ema21 and rsi<50 and price>vwap and zone=="DISCOUNT"
        bear = ema9<ema21 and rsi>50 and price<vwap and zone=="PREMIUM"
        if bos=="BULLISH_BOS" or liq_sweep=="BULLISH_SWEEP": bull = True
        if bos=="BEARISH_BOS" or liq_sweep=="BEARISH_SWEEP": bear = True
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
    global bot_paused, watchlist, ACCOUNT_SIZE
    updates=await get_updates(session, offset+1)
    for update in updates:
        try:
            offset=update["update_id"]
            text=update.get("message",{}).get("text","").strip()
            if not text: continue
            print("CMD:",text)
            if text=="/status":
                al=", ".join([t+":"+v['signal']+"@$"+str(v['entry']) for t,v in active_trades.items()]) or "None"
                lines=["Bot Status","",get_ny().strftime("%H:%M:%S")+" NY | "+market_status(),
                       "Running" if not bot_paused else "PAUSED","Strategy: SMC + VWAP + RSI + EMA",
                       "Watching: "+", ".join(watchlist),
                       "Trades today: "+str(trades_today)+"/"+str(MAX_TRADES_PER_DAY),
                       "Daily P&L: "+str(round(daily_pnl,2))+"%",
                       "Losing streak: "+str(losing_streak),"Active: "+al]
                await tg(session,"\n".join(lines))
            elif text.startswith("/add "):
                t=text.split()[1].upper()
                if t not in watchlist: watchlist.append(t); await tg(session,"Added "+t+"!")
                else: await tg(session,t+" already watching")
            elif text.startswith("/remove "):
                t=text.split()[1].upper()
                if t in watchlist: watchlist.remove(t); await tg(session,"Removed "+t)
                else: await tg(session,t+" not found")
            elif text=="/watchlist": await tg(session,"Watching: "+", ".join(watchlist))
            elif text=="/pause": bot_paused=True; await tg(session,"Bot paused. /resume to restart.")
            elif text=="/resume": bot_paused=False; await tg(session,"Bot resumed!")
            elif text=="/report": await daily_report(session)
            elif text=="/weekly": await weekly_report(session)
            elif text.startswith("/risk "):
                try: ACCOUNT_SIZE=float(text.split()[1]); await tg(session,"Account: $"+str(ACCOUNT_SIZE))
                except: await tg(session,"Usage: /risk 5000")
            elif text.startswith("/alert "):
                try:
                    parts=text.split(); t=parts[1].upper(); target=float(parts[2])
                    p,_,_=await yahoo_price(session,t)
                    direction="above" if p and target>p else "below"
                    if t not in price_alerts: price_alerts[t]=[]
                    price_alerts[t].append({"price":target,"direction":direction})
                    await tg(session,"Alert set! "+t+" "+direction+" $"+str(target))
                except: await tg(session,"Usage: /alert RGTI 25.00")
            elif text=="/alerts":
                if not any(price_alerts.values()): await tg(session,"No alerts.")
                else:
                    lines=["Active Alerts:"]
                    for t,als in price_alerts.items():
                        for a in als: lines.append(t+": "+a['direction']+" $"+str(a['price']))
                    await tg(session,"\n".join(lines))
            elif text=="/brief": await morning_brief(session)
            elif text=="/scan":
                new_wl=await run_morning_scan(session)
                watchlist.clear(); watchlist.extend(new_wl)
            elif text.startswith("/backtest"):
                parts=text.split()
                tickers_bt=[parts[1].upper()] if len(parts)>1 else list(BASE_TICKERS)
                await tg(session,"Backtesting with SMC strategy: "+", ".join(tickers_bt)+"...")
                for t in tickers_bt:
                    try:
                        bt=await run_backtest(session,t)
                        if bt:
                            grade="GOOD" if bt['win_rate']>=55 else "OK" if bt['win_rate']>=45 else "POOR"
                            ps="+" if bt['total_pnl']>=0 else ""; avgs="+" if bt['avg_pnl']>=0 else ""
                            lines=["Backtest "+bt['ticker']+" (SMC) - "+grade,
                                   "Win Rate: "+str(bt['win_rate'])+"%",
                                   "Trades: "+str(bt['total_trades'])+" | Wins: "+str(bt['wins'])+" | Losses: "+str(bt['losses']),
                                   "Avg P&L: "+avgs+str(bt['avg_pnl'])+"%",
                                   "Best: +"+str(bt['best'])+"% | Worst: "+str(bt['worst'])+"%",
                                   "Total P&L: "+ps+str(bt['total_pnl'])+"%"]
                            await tg(session,"\n".join(lines))
                        else: await tg(session,"Not enough data for "+t)
                    except Exception as bt_err:
                        await tg(session,"Backtest error "+t+": "+str(bt_err)[:80])
                    await asyncio.sleep(1)
            elif text=="/smc":
                lines=["SMC Features Active:","",
                       "Break of Structure (BOS)",
                       "Change of Character (CHoCH)",
                       "Order Block Detection",
                       "Liquidity Sweep Detection",
                       "Fair Value Gap (FVG)",
                       "Power of 3 (Accumulation/Manipulation/Distribution)",
                       "Smart Money Trap Detection",
                       "Premium/Discount Zones",
                       "Liquidity Zone Mapping",
                       "","All combined with VWAP+RSI+EMA+ATR+BB"]
                await tg(session,"\n".join(lines))
            elif text=="/help":
                lines=["AlphaSignal SMC Bot Commands","",
                       "/status - bot status","/watchlist - tickers",
                       "/add AAPL - add ticker","/remove RGTI - remove",
                       "/scan - find best stocks today",
                       "/alert RGTI 25.00 - price alert","/alerts - show alerts",
                       "/pause - pause bot","/resume - resume",
                       "/report - daily P&L","/weekly - weekly report",
                       "/backtest - test SMC strategy","/backtest RGTI - one stock",
                       "/risk 5000 - set account size",
                       "/brief - morning brief","/smc - SMC features list","/help - this menu"]
                await tg(session,"\n".join(lines))
        except Exception as cmd_err:
            print("CMD err:",cmd_err)
            try: await tg(session,"Command error: "+str(cmd_err)[:80])
            except: pass
    return offset

# ── MAIN ─────────────────────────────────────────────────
async def main():
    global last_update_id, morning_brief_sent, trades_today, watchlist, last_scan_day, weekly_report_sent
    print("AlphaSignal SMC ULTIMATE starting...")
    last_report_day=-1

    async with aiohttp.ClientSession() as session:
        lines=["AlphaSignal SMC ULTIMATE Bot!","",
               "Watching: "+", ".join(BASE_TICKERS),"",
               "NEW SMART MONEY CONCEPT ENGINE:",
               "Break of Structure (BOS)",
               "Change of Character (CHoCH)",
               "Order Block Detection",
               "Liquidity Sweep Detection",
               "Fair Value Gap (FVG)",
               "Power of 3 Phases",
               "Smart Money Trap Filter",
               "Premium/Discount Zones","",
               "PLUS: VWAP+RSI+EMA+ATR+BB+ORB",
               "AI: Groq LLaMA 3.3",
               "Min Score: "+str(MIN_SCORE)+"/20",
               "Real-time SIP data","",
               "/help for all commands","/smc for SMC details"]
        await tg(session,"\n".join(lines))

        while True:
            try:
                last_update_id=await handle_cmds(session,last_update_id)
            except Exception as e:
                print("CMD handler err:",e)

            status=market_status(); ny=get_ny()

            if ny.hour==0 and ny.minute==0: reset_daily_stats()
            if ny.hour==9 and ny.minute==20 and ny.weekday()<5 and ny.day!=last_scan_day:
                try:
                    new_wl=await run_morning_scan(session)
                    watchlist.clear(); watchlist.extend(new_wl)
                    last_scan_day=ny.day
                except Exception as e: print("Scan err:",e)
            if ny.hour==9 and ny.minute==25 and not morning_brief_sent and ny.weekday()<5:
                try: await morning_brief(session); morning_brief_sent=True
                except: pass
            if ny.hour==9 and ny.minute==26: morning_brief_sent=False
            if ny.hour==16 and ny.minute==5 and ny.day!=last_report_day:
                try: await daily_report(session); last_report_day=ny.day
                except: pass
            if ny.weekday()==6 and ny.hour==20 and ny.minute==0 and not weekly_report_sent:
                try: await weekly_report(session); weekly_report_sent=True
                except: pass
            if ny.weekday()==0: weekly_report_sent=False

            if status=="CLOSED": await asyncio.sleep(300); continue
            if bot_paused: await asyncio.sleep(30); continue

            try:
                if active_trades: await check_trailing(session)
                if price_alerts:  await check_alerts(session)
            except Exception as e: print("Trailing/alerts err:",e)

            print("["+ny.strftime("%H:%M:%S")+" NY]",status,"|",watchlist)

            for ticker in list(watchlist):
                try:
                    can_trade,reason=check_daily_limits()
                    if not can_trade: print("  BLOCKED:",reason); continue
                    if TIME_FILTER and not is_best_hour(): continue

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
                    if result is None: print("    WAIT"); continue
                    if is_duplicate(ticker,result["signal"]): print("    Duplicate"); continue

                    tf_agrees=check_3tf_agreement(bars_1m,bars_5m,bars_15m,result["signal"])
                    if REQUIRE_3TF_AGREE and tf_agrees<2: print("    TF disagree"); continue

                    spy_trend,spy_chg,_=await get_spy_trend(session)
                    if SPY_FILTER:
                        if result["signal"]=="BUY" and spy_trend=="BEAR": continue
                        if result["signal"]=="SELL" and spy_trend=="BULL": continue

                    rvol=calc_rvol(bars_1m,bars_yest)
                    multiday=get_multiday(bars_daily)
                    orb_tag=check_orb(ticker,result["entry"]) or ""
                    patterns=detect_patterns(bars_5m)
                    sentiment,headlines=await get_news(session,ticker)

                    if sentiment=="NEGATIVE" and result["signal"]=="BUY":
                        await tg(session,"BUY blocked - negative news: "+ticker); continue

                    # Block smart money traps
                    if result.get("smt"):
                        await tg(session,"TRAP DETECTED on "+ticker+" - "+result['smt']+" - Signal blocked!"); continue

                    ai=await ai_confirm(session,ticker,result,patterns,sentiment,tf_agrees)
                    if ai.get("verdict")=="REJECTED":
                        await tg(session,"AI REJECTED "+ticker+" "+result["signal"]+": "+ai.get("reason","")); continue

                    yp,yc,_=await yahoo_price(session,ticker)
                    shares,cost,risk_pct,reduced=calc_position(result['entry'],result['sl'])
                    score=result['buy_score'] if result['signal']=='BUY' else result['sell_score']
                    print("    SIGNAL:",result['signal'],"Score:"+str(score)+"/20","TF:"+str(tf_agrees)+"/3",
                          "BOS:"+str(result.get('bos')),"CHoCH:"+str(result.get('choch')),
                          "OB:"+str(result.get('nearest_ob',{}).get('type') if result.get('nearest_ob') else 'None'),
                          "Sweep:"+str(result.get('liq_sweep')))

                    msg=build_msg(ticker,result,yp,yc,status,sentiment,headlines,patterns,ai,
                                  rvol,multiday,spy_trend,spy_chg,tf_agrees,orb_tag,shares,cost,risk_pct,reduced)
                    await tg(session,msg)

                    active_trades[ticker]={"signal":result["signal"],"entry":result["entry"],
                                           "sl":result["sl"],"tp":result["tp"],"atr":result["atr"],
                                           "time":ny.strftime("%H:%M")}
                    trailing_stops[ticker]=result["sl"]
                    last_signal_time[ticker]=datetime.now()
                    last_signal_type[ticker]=result["signal"]
                    trades_today+=1
                    await asyncio.sleep(2)

                except Exception as e:
                    print("  ERROR",ticker,":",e); continue

            await asyncio.sleep(SCAN_INTERVAL)

if __name__=="__main__":
    asyncio.run(main())
