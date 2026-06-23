import asyncio
import aiohttp
import json
import time
import traceback
from datetime import datetime, timezone, timedelta
import os

# ══════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════
FINNHUB_API_KEY  = os.environ.get("FINNHUB_API_KEY",  "d8mpeg1r01qn3046mvtgd8mpeg1r01qn3046mvu0")
GROQ_API_KEY     = os.environ.get("GROQ_API_KEY",     "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")

ALPACA_API_KEY   = os.environ.get("ALPACA_API_KEY",   "PKHY4LGTE2AF3PCURW4JJB423B")
ALPACA_SECRET    = os.environ.get("ALPACA_SECRET",    "7NwLWDCxprL794BdsCheM9CB8D4VAi2GSqKTgfovv3Ws")

ALPACA_LIVE_KEY    = os.environ.get("ALPACA_LIVE_KEY",    "AKPYM4PLBJBOEBSD3NQQVEDALI")
ALPACA_LIVE_SECRET = os.environ.get("ALPACA_LIVE_SECRET", "6Z73eeNG8Fpa64Tw7UBaEULCyFHJRCSh6r3kRgC7k2qo")

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN",   "8855798705:AAFuUv_mpafzcaSKwnsye1bgYHLomJS2SAU")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "6903579390")

# ── TRADING CONFIG ────────────────────────────────────────
AUTO_TRADE         = True
PAPER_TRADING      = True
ACCOUNT_SIZE       = 10000
RISK_PCT           = 2
MAX_OPEN_TRADES    = 10
DAILY_LOSS_LIMIT   = 5.0
MAX_TRADES_PER_DAY = 20
SCAN_INTERVAL      = 30
SIGNAL_COOLDOWN    = 1800
MIN_SCORE          = 8
SPY_FILTER         = True
TIME_FILTER        = False
REQUIRE_3TF_AGREE  = True
ALLOW_SHORT        = True
CLOSE_ALL_TIME     = (15, 45)
LEVERAGE_MAP       = {"HIGH": 2, "MEDIUM": 1}

# ── CRYPTO CONFIG ─────────────────────────────────────────
CRYPTO_ENABLED      = True
CRYPTO_UNIVERSE     = ["BTC/USD", "ETH/USD", "SOL/USD", "LTC/USD",
                       "AVAX/USD", "LINK/USD", "DOT/USD", "AAVE/USD"]
CRYPTO_MIN_SCORE    = 9          # slightly stricter than stocks (no SL safety net from broker)
CRYPTO_RISK_PCT     = 1.0        # risk less per crypto trade (more volatile)
CRYPTO_MAX_OPEN     = 4          # max simultaneous crypto positions
CRYPTO_SCAN_INTERVAL = 60        # seconds between crypto scans (24/7)
CRYPTO_SL_ATR       = 1.5        # stop-loss = entry - ATR*1.5
CRYPTO_TP_ATR       = 3.0        # take-profit = entry + ATR*3.0
CRYPTO_DATA_BASE    = "https://data.alpaca.markets/v1beta3/crypto/us"

# ── ENDPOINTS ─────────────────────────────────────────────
ALPACA_BASE          = "https://data.alpaca.markets/v2"
ALPACA_TRADE_BASE    = "https://paper-api.alpaca.markets"
ALPACA_LIVE_HEADERS  = {"APCA-API-KEY-ID": ALPACA_LIVE_KEY,  "APCA-API-SECRET-KEY": ALPACA_LIVE_SECRET}
ALPACA_TRADE_HEADERS = {"APCA-API-KEY-ID": ALPACA_API_KEY,   "APCA-API-SECRET-KEY": ALPACA_SECRET, "Content-Type": "application/json"}

# ── SCAN UNIVERSE ─────────────────────────────────────────
# Note: removed delisted/bankrupt tickers (NKLA, GOEV, FSR, RIDE, WKHS,
# HYZN, CLVS, PTRA) — they caused empty Yahoo responses. Replaced with
# currently-active, liquid small/mid caps.
SMALL_CAPS = [
    "RXT","QUBT","LUNR","BBAI","SOUN","KULR","MARA","RIOT","CLSK","HOOD",
    "HIMS","RKLB","ASTS","ACHR","JOBY","BLNK","PLUG","FCEL","NVAX","ACAD",
    "ITCI","CLOV","MVIS","OCGN","AGEN","CTIC","SENS","AMPIO","OBSV","AVDL",
    "BTBT","RGTI","IONQ","AISP","SMR","OKLO","CIFR","WULF","BITF","HUT"
]
MID_CAPS = [
    "MSTR","COIN","HOOD","SOFI","AFRM","UPST","OPEN","CVNA","DKNG","PENN",
    "CHWY","RIVN","LCID","XPEV","NIO","LI","RBLX","MTCH","BMBL","SNAP",
    "PINS","ZM","PTON","ROKU","FUBO","SE","GRAB","BARK","KRTX","ACMR",
    "AEHR","AKBA","ALDX","PLTR","SMCI","ARM","DELL","SNOW","NET","DDOG"
]
SCAN_UNIVERSE   = list(set(SMALL_CAPS + MID_CAPS))
DEFAULT_WATCHLIST = ["MARA", "SOUN", "RIOT", "BBAI", "MSTR"]

# ══════════════════════════════════════════════════════════
#  STATE
# ══════════════════════════════════════════════════════════
last_signal_time      = {}
last_signal_type      = {}
orb_levels            = {}
active_trades         = {}
performance_log       = []
all_time_log          = []
watchlist             = list(DEFAULT_WATCHLIST)
daily_picks           = []
bot_paused            = False
trailing_stops        = {}
last_update_id        = 0
price_alerts          = {}
morning_brief_sent    = False
trades_today          = 0
daily_pnl             = 0.0
losing_streak         = 0
last_scan_day         = -1
weekly_report_sent    = False
alpaca_ws_prices      = {}
alpaca_ws_connected   = False
stocktwits_cooldown   = {}
positions_closed_today = False
no_signal_count       = {}
last_heartbeat_minute = -1
last_report_day       = -1

# ══════════════════════════════════════════════════════════
#  LOGGING HELPERS
# ══════════════════════════════════════════════════════════
def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def log_err(context: str, e: Exception):
    log(f"ERROR [{context}]: {e}")
    traceback.print_exc()

# ══════════════════════════════════════════════════════════
#  TIME UTILITIES
# ══════════════════════════════════════════════════════════
def get_ny():
    return datetime.now(timezone.utc) + timedelta(hours=-4)

def market_status():
    ny = get_ny()
    if ny.weekday() >= 5:
        return "CLOSED"
    h, m = ny.hour, ny.minute
    if h < 8:
        return "CLOSED"
    if h < 9 or (h == 9 and m < 30):
        return "PRE_MARKET"
    if h >= 16:
        return "CLOSED"
    return "OPEN"

def get_session_name():
    ny = get_ny()
    t = ny.hour * 60 + ny.minute
    if 570 <= t <= 660:  return "9:30-11am BEST"
    if 660 <= t <= 840:  return "11am-2pm CHOPPY"
    if 840 <= t <= 900:  return "2-3pm OK"
    if 900 <= t <= 960:  return "3-4pm BEST"
    return "PRE/AFTER"

def is_duplicate(ticker, signal):
    if ticker not in last_signal_time:
        return False
    elapsed = (datetime.now(timezone.utc) - last_signal_time[ticker]).total_seconds()
    return elapsed < SIGNAL_COOLDOWN and last_signal_type.get(ticker) == signal

def check_daily_limits():
    if trades_today >= MAX_TRADES_PER_DAY:
        return False, f"Max trades ({MAX_TRADES_PER_DAY}/day)"
    if daily_pnl <= -DAILY_LOSS_LIMIT:
        return False, f"Daily loss limit (-{DAILY_LOSS_LIMIT}%)"
    return True, ""

def reset_daily():
    global trades_today, daily_pnl, losing_streak, positions_closed_today, daily_picks, watchlist
    trades_today = 0
    daily_pnl = 0.0
    losing_streak = 0
    positions_closed_today = False
    daily_picks = []
    watchlist = list(DEFAULT_WATCHLIST)
    log("Daily stats reset - watchlist: " + str(watchlist))

# ══════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════
async def tg(session: aiohttp.ClientSession, msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    # Telegram has a 4096-char limit
    chunks = [msg[i:i+4000] for i in range(0, len(msg), 4000)]
    for chunk in chunks:
        for attempt in range(3):
            try:
                async with session.post(
                    url,
                    json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "parse_mode": "HTML"},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    res = await r.json()
                    if not res.get("ok"):
                        log(f"TG error: {res}")
                break
            except Exception as e:
                log(f"TG attempt {attempt+1} failed: {e}")
                await asyncio.sleep(2)

async def get_updates(session: aiohttp.ClientSession, offset: int = 0):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    try:
        async with session.get(
            url,
            params={"offset": offset, "timeout": 1},
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            data = await r.json()
            return data.get("result", [])
    except Exception as e:
        log(f"get_updates error: {e}")
        return []

# ══════════════════════════════════════════════════════════
#  PRICE FEEDS
# ══════════════════════════════════════════════════════════
async def yahoo_price(session: aiohttp.ClientSession, ticker: str):
    try:
        async with session.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            if r.status == 429:
                log(f"yahoo_price {ticker}: rate limited (429)")
                return None, None, None
            try:
                data = await r.json()
            except Exception:
                return None, None, None

            # Defensive parsing — any missing layer returns None cleanly
            chart  = (data or {}).get("chart") or {}
            result = chart.get("result")
            if not result:
                # Yahoo returns {"chart":{"result":null,"error":{...}}} for bad/delisted tickers
                return None, None, None
            meta = (result[0] or {}).get("meta") or {}
            price = meta.get("regularMarketPrice")
            prev  = meta.get("chartPreviousClose")
            vol   = meta.get("regularMarketVolume", 0)
            if price is None or prev is None or prev == 0:
                return None, None, None
            return round(price, 4), round(((price - prev) / prev) * 100, 2), vol
    except Exception as e:
        log(f"yahoo_price {ticker}: {e}")
        return None, None, None

async def get_alpaca_price(session: aiohttp.ClientSession, ticker: str):
    try:
        async with session.get(
            f"{ALPACA_BASE}/stocks/{ticker}/trades/latest",
            headers=ALPACA_LIVE_HEADERS,
            params={"feed": "sip"},
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            data  = await r.json()
            price = data.get("trade", {}).get("p")
            return round(price, 4) if price else None
    except Exception as e:
        log(f"get_alpaca_price {ticker}: {e}")
        return None

async def get_finnhub_price(session: aiohttp.ClientSession, ticker: str):
    try:
        async with session.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": ticker, "token": FINNHUB_API_KEY},
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            data  = await r.json()
            price = data.get("c", 0)
            high  = data.get("h", 0)
            low   = data.get("l", 0)
            prev  = data.get("pc", 0)
            if price and price > 0:
                chg = round(((price - prev) / prev) * 100, 2) if prev else 0
                return round(price, 4), chg, round(high, 4), round(low, 4)
            return None, None, None, None
    except Exception as e:
        log(f"get_finnhub_price {ticker}: {e}")
        return None, None, None, None

async def get_realtime_price(session: aiohttp.ClientSession, ticker: str):
    """Priority: Finnhub → Alpaca SIP → Alpaca WS cache → Yahoo"""
    fp, _, _, _ = await get_finnhub_price(session, ticker)
    if fp:
        return fp, "FINNHUB"
    p = await get_alpaca_price(session, ticker)
    if p:
        return p, "ALPACA-SIP"
    if ticker in alpaca_ws_prices and alpaca_ws_prices[ticker] > 0:
        return alpaca_ws_prices[ticker], "ALPACA-WS"
    yp, _, _ = await yahoo_price(session, ticker)
    return yp, "YAHOO"

async def get_spy_trend(session: aiohttp.ClientSession):
    # Try Finnhub first (reliable from datacenter IPs), fall back to Yahoo
    p, c, _, _ = await get_finnhub_price(session, "SPY")
    if p is None:
        p, c, _ = await yahoo_price(session, "SPY")
    if p is None:
        return "NEUTRAL", 0, 0
    c = c or 0
    return ("BULL" if c >= 0 else "BEAR"), c, p

async def get_bars(session: aiohttp.ClientSession, ticker: str, tf: str = "5Min", limit: int = 50):
    try:
        async with session.get(
            f"{ALPACA_BASE}/stocks/{ticker}/bars",
            headers=ALPACA_LIVE_HEADERS,
            params={"timeframe": tf, "limit": limit, "feed": "sip"},
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            return (await r.json()).get("bars", [])
    except Exception as e:
        log(f"get_bars {ticker} {tf}: {e}")
        return []

async def check_sip_access(session: aiohttp.ClientSession):
    """
    Test whether the data keys can pull recent SIP data.
    - 200 with a trade  -> SIP active
    - 401/403 or 'subscription' message -> no SIP entitlement
    - 429 (rate limited) -> UNKNOWN, retry; never report as 'no SIP'
    Returns (status, detail) where status is 'yes', 'no', or 'unknown'.
    """
    for attempt in range(3):
        try:
            async with session.get(
                f"{ALPACA_BASE}/stocks/AAPL/trades/latest",
                headers=ALPACA_LIVE_HEADERS,
                params={"feed": "sip"},
                timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                text = await r.text()
                if r.status == 200:
                    try:
                        data = json.loads(text)
                        if data.get("trade", {}).get("p"):
                            return "yes", "SIP data OK"
                    except Exception:
                        pass
                    return "yes", "SIP responded (no trade yet — market may be closed)"
                if r.status in (401, 403) or "subscription" in text.lower():
                    return "no", f"No SIP entitlement (HTTP {r.status})"
                if r.status == 429:
                    # Rate limited — wait and retry, this says nothing about SIP
                    await asyncio.sleep(3 * (attempt + 1))
                    continue
                return "unknown", f"Unexpected response (HTTP {r.status})"
        except Exception as e:
            await asyncio.sleep(2)
            last_err = str(e)[:60]
    return "unknown", "Rate limited (HTTP 429) — could not confirm. Try /data later."

async def get_yahoo_bars(session: aiohttp.ClientSession, ticker: str, days: int = 90):
    try:
        end   = int(time.time())
        start = end - (days * 86400)
        async with session.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
            params={"interval": "1d", "period1": str(start), "period2": str(end)},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            data   = await r.json()
            result = data["chart"]["result"][0]
            ohlcv  = result["indicators"]["quote"][0]
            bars   = []
            for i in range(len(result.get("timestamp", []))):
                try:
                    o, h, l, c, v = (ohlcv["open"][i], ohlcv["high"][i],
                                     ohlcv["low"][i], ohlcv["close"][i], ohlcv["volume"][i])
                    if all([o, h, l, c, v]):
                        bars.append({"o": round(o,4), "h": round(h,4), "l": round(l,4),
                                     "c": round(c,4), "v": int(v)})
                except:
                    continue
            return bars
    except Exception as e:
        log(f"get_yahoo_bars {ticker}: {e}")
        return []

# ══════════════════════════════════════════════════════════
#  ALPACA PRICE POLLER (replaces WS)
# ══════════════════════════════════════════════════════════
async def alpaca_price_poller():
    global alpaca_ws_prices, alpaca_ws_connected
    log("Alpaca REST price poller starting...")
    alpaca_ws_connected = True

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                ny = get_ny()
                if ny.weekday() >= 5 or ny.hour < 8 or ny.hour >= 17:
                    await asyncio.sleep(300)
                    continue

                tickers = list(watchlist) if watchlist else list(DEFAULT_WATCHLIST)
                for ticker in tickers:
                    try:
                        async with session.get(
                            f"{ALPACA_BASE}/stocks/{ticker}/trades/latest",
                            headers=ALPACA_LIVE_HEADERS,
                            params={"feed": "sip"},
                            timeout=aiohttp.ClientTimeout(total=3)
                        ) as r:
                            data  = await r.json()
                            price = data.get("trade", {}).get("p")
                            if price:
                                alpaca_ws_prices[ticker] = round(price, 4)
                    except:
                        pass
                    await asyncio.sleep(0.3)

                await asyncio.sleep(5)

            except Exception as e:
                log_err("alpaca_price_poller", e)
                await asyncio.sleep(15)

# ══════════════════════════════════════════════════════════
#  ALPACA TRADING
# ══════════════════════════════════════════════════════════
async def get_account_info(session: aiohttp.ClientSession):
    try:
        async with session.get(
            f"{ALPACA_TRADE_BASE}/v2/account",
            headers=ALPACA_TRADE_HEADERS,
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            data = await r.json()
            return {
                "equity":       round(float(data.get("equity",       0)), 2),
                "buying_power": round(float(data.get("buying_power", 0)), 2),
                "cash":         round(float(data.get("cash",         0)), 2),
            }
    except Exception as e:
        log_err("get_account_info", e)
        return None

async def get_open_positions(session: aiohttp.ClientSession):
    try:
        async with session.get(
            f"{ALPACA_TRADE_BASE}/v2/positions",
            headers=ALPACA_TRADE_HEADERS,
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            data = await r.json()
            return {p["symbol"]: p for p in data} if isinstance(data, list) else {}
    except Exception as e:
        log_err("get_open_positions", e)
        return {}

# Cache asset shortability so we don't hit the API every signal
_shortable_cache = {}

async def is_shortable(session: aiohttp.ClientSession, ticker: str):
    """
    Check whether Alpaca allows shorting this asset.
    Returns True/False. Cached per ticker for the session.
    """
    if ticker in _shortable_cache:
        return _shortable_cache[ticker]
    try:
        async with session.get(
            f"{ALPACA_TRADE_BASE}/v2/assets/{ticker}",
            headers=ALPACA_TRADE_HEADERS,
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            data = await r.json()
            # Asset must be tradable, shortable, and easy-to-borrow
            ok = bool(data.get("tradable") and data.get("shortable")
                      and data.get("easy_to_borrow"))
            _shortable_cache[ticker] = ok
            log(f"  {ticker} shortable={ok} "
                f"(tradable={data.get('tradable')}, shortable={data.get('shortable')}, "
                f"etb={data.get('easy_to_borrow')})")
            return ok
    except Exception as e:
        log_err(f"is_shortable {ticker}", e)
        # On error, be safe and disallow shorting
        return False

async def place_order(session: aiohttp.ClientSession,
                      ticker: str, side: str, qty: int,
                      sl_price: float, tp_price: float):
    try:
        order = {
            "symbol":        ticker,
            "qty":           str(qty),
            "side":          side,
            "type":          "market",
            "time_in_force": "day",
            "order_class":   "bracket",
            "stop_loss":     {"stop_price":  str(round(sl_price, 2))},
            "take_profit":   {"limit_price": str(round(tp_price, 2))},
        }
        async with session.post(
            f"{ALPACA_TRADE_BASE}/v2/orders",
            headers=ALPACA_TRADE_HEADERS,
            json=order,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            data = await r.json()
            if data.get("id"):
                return {"success": True, "order_id": data["id"], "status": data.get("status")}
            return {"success": False, "error": str(data)}
    except Exception as e:
        log_err("place_order", e)
        return {"success": False, "error": str(e)}

async def close_all_positions(session: aiohttp.ClientSession):
    try:
        async with session.delete(
            f"{ALPACA_TRADE_BASE}/v2/positions",
            headers=ALPACA_TRADE_HEADERS,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            return await r.json()
    except Exception as e:
        log_err("close_all_positions", e)
        return None

async def close_position(session: aiohttp.ClientSession, ticker: str):
    try:
        async with session.delete(
            f"{ALPACA_TRADE_BASE}/v2/positions/{ticker}",
            headers=ALPACA_TRADE_HEADERS,
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            return await r.json()
    except Exception as e:
        log_err(f"close_position {ticker}", e)
        return None

def calc_position_size(entry: float, sl: float, confidence: str, equity: float):
    leverage  = LEVERAGE_MAP.get(confidence, 1)
    risk_amt  = equity * (RISK_PCT / 100)
    rps       = abs(entry - sl)
    if rps <= 0:
        return 0, 0, leverage

    shares = int(risk_amt / rps) * leverage
    if shares == 0:
        shares = max(1, leverage) if entry <= equity * 0.1 else 1

    max_cost = equity * 0.30
    if shares * entry > max_cost:
        shares = int(max_cost / entry)
    if shares < 1:
        shares = 1

    return shares, round(shares * entry, 2), leverage

def validate_bracket(signal: str, entry: float, sl: float, tp: float):
    """
    Ensure SL/TP are on the correct sides of entry with a safe minimum gap.
    Alpaca rejects bracket orders where, for a SHORT, stop_price <= entry,
    or for a LONG, stop_price >= entry. Returns (ok, sl, tp, reason).
    """
    entry = round(entry, 2)
    # Minimum gap: at least 1 cent, scaled a bit for higher-priced stocks
    min_gap = max(0.02, round(entry * 0.002, 2))

    if signal == "BUY":
        # LONG: SL below entry, TP above entry
        sl = round(sl, 2)
        tp = round(tp, 2)
        if sl >= entry - min_gap:
            sl = round(entry - min_gap, 2)
        if tp <= entry + min_gap:
            tp = round(entry + min_gap, 2)
        if sl >= entry or tp <= entry:
            return False, sl, tp, "LONG bracket invalid after correction"
    else:
        # SHORT: SL above entry, TP below entry
        sl = round(sl, 2)
        tp = round(tp, 2)
        if sl <= entry + min_gap:
            sl = round(entry + min_gap, 2)
        if tp >= entry - min_gap:
            tp = round(entry - min_gap, 2)
        if sl <= entry or tp >= entry:
            return False, sl, tp, "SHORT bracket invalid after correction"

    return True, sl, tp, ""

def can_place_bracket_order():
    """
    Alpaca bracket orders are only accepted during regular trading hours
    (9:30 AM - 3:55 PM NY). Returns (ok, reason).
    """
    ny = get_ny()
    if ny.weekday() >= 5:
        return False, "weekend"
    t = ny.hour * 60 + ny.minute
    if t < 9 * 60 + 30:
        return False, "pre-market (orders open at 9:30 NY)"
    if t > 15 * 60 + 55:
        return False, "after 3:55 PM NY (too close to close)"
    return True, ""

# ══════════════════════════════════════════════════════════
#  SMART DAILY SCANNER
# ══════════════════════════════════════════════════════════
async def smart_daily_scan(session: aiohttp.ClientSession):
    global watchlist, daily_picks
    log("Smart daily scan starting...")
    await tg(session, f"🔍 Smart Daily Scanner running...\nAnalysing {len(SCAN_UNIVERSE)} stocks...")

    spy_trend, spy_chg, _ = await get_spy_trend(session)
    candidates = []

    for ticker in SCAN_UNIVERSE:
        try:
            # Price & % change from Finnhub (reliable from datacenter IPs).
            # Volume/ATR/RVOL come from Alpaca daily bars below.
            p, chg, fh_high, fh_low = await get_finnhub_price(session, ticker)
            if not p or p < 1:
                continue

            bars = await get_bars(session, ticker, "1Day", 15)
            if len(bars) < 5:
                continue

            # Today's volume = most recent daily bar's volume
            vol = bars[-1]["v"]
            if not vol:
                continue

            avg_vol = sum(b["v"] for b in bars[:-1]) / len(bars[:-1])
            rvol    = vol / avg_vol if avg_vol > 0 else 0
            atr     = calc_atr(bars)
            if not atr:
                continue
            atr_pct = (atr / p) * 100

            # Hard exclusions only: bad price range (can't size / too illiquid)
            if p < 2 or p > 100:
                continue

            chg = chg or 0  # Finnhub may return None if prev close was 0

            score = 0
            if rvol >= 5:        score += 5
            elif rvol >= 3:      score += 4
            elif rvol >= 2:      score += 3
            elif rvol >= 1.5:    score += 2
            elif rvol >= 1.0:    score += 1

            if atr_pct >= 8:     score += 4
            elif atr_pct >= 5:   score += 3
            elif atr_pct >= 3:   score += 2
            elif atr_pct >= 2:   score += 1

            if abs(chg) >= 10:   score += 4
            elif abs(chg) >= 5:  score += 3
            elif abs(chg) >= 3:  score += 2
            elif abs(chg) >= 1:  score += 1

            if spy_trend == "BULL" and chg > 0: score += 2
            if spy_trend == "BEAR" and chg < 0: score += 2

            if 5 <= p <= 50:       score += 2
            elif 2 <= p < 5 or 50 < p <= 100: score += 1

            # Keep EVERY stock that returned valid data. We always want the
            # best-available picks, never a fallback to the same defaults.
            candidates.append({
                "ticker":      ticker,
                "price":       p,
                "change":      chg,
                "vol":         vol,
                "rvol":        round(rvol, 1),
                "atr_pct":     round(atr_pct, 1),
                "score":       score,
                "spy_aligned": (spy_trend == "BULL" and chg > 0) or (spy_trend == "BEAR" and chg < 0),
            })

            await asyncio.sleep(1.1)  # ~55 calls/min, under Finnhub's 60/min free-tier limit
        except Exception as e:
            log(f"scan {ticker}: {e}")
            continue

    # Sort by score, then by RVOL as a tiebreaker (most active first)
    candidates.sort(key=lambda x: (x["score"], x["rvol"]), reverse=True)
    top3 = candidates[:3]

    if not top3:
        # Only reached if data fetching totally failed for the whole universe.
        default = list(DEFAULT_WATCHLIST)
        await tg(session, "⚠️ Scanner: data unavailable for all stocks.\nUsing defaults: " + ", ".join(default))
        return default

    daily_picks = top3
    new_watchlist = [c["ticker"] for c in top3]

    # Flag whether today's picks are genuinely strong or just "best of a quiet day"
    strong = top3[0]["score"] >= 6
    quiet_note = "" if strong else " (quiet day — best available)"

    sign = lambda v: "+" if v >= 0 else ""
    lines = ["🔍 <b>Daily Smart Scan Complete!</b>" + quiet_note, "",
             f"SPY: {spy_trend} {sign(spy_chg)}{spy_chg}%", "",
             "<b>Today's Top 3 Picks:</b>", ""]
    for i, c in enumerate(top3, 1):
        al = "✅ SPY aligned" if c["spy_aligned"] else "⚠️ Against SPY"
        lines.append(f"{i}. <b>{c['ticker']}</b> ${c['price']} {sign(c['change'])}{c['change']}%")
        lines.append(f"   RVOL:{c['rvol']}x | ATR:{c['atr_pct']}% | Score:{c['score']} | {al}")
        lines.append("")

    lines.append("🤖 Auto-trading these stocks today!")
    lines.append(f"Max risk/trade: ${round(ACCOUNT_SIZE * RISK_PCT / 100, 2)}")
    await tg(session, "\n".join(lines))
    return new_watchlist

# ══════════════════════════════════════════════════════════
#  INDICATORS
# ══════════════════════════════════════════════════════════
def calc_vwap(bars):
    cv = ct = 0
    vals = []
    for b in bars:
        if b.get("vw"):
            vals.append(round(b["vw"], 4))
        else:
            tp = (b["h"] + b["l"] + b["c"]) / 3
            ct += tp * b["v"]
            cv += b["v"]
            vals.append(round(ct / cv if cv > 0 else 0, 4))
    return vals

def calc_rsi(closes, p=14):
    if len(closes) < p + 1:
        return None
    g = [max(closes[i] - closes[i-1], 0) for i in range(1, len(closes))]
    l = [max(closes[i-1] - closes[i], 0) for i in range(1, len(closes))]
    ag, al = sum(g[-p:]) / p, sum(l[-p:]) / p
    return round(100 - (100 / (1 + ag / al)), 2) if al != 0 else 100

def calc_ema(vals, p):
    if len(vals) < p:
        return None
    k = 2 / (p + 1)
    e = sum(vals[:p]) / p
    for v in vals[p:]:
        e = v * k + e * (1 - k)
    return round(e, 4)

def calc_atr(bars, p=14):
    if len(bars) < p + 1:
        return None
    trs = [
        max(bars[i]["h"] - bars[i]["l"],
            abs(bars[i]["h"] - bars[i-1]["c"]),
            abs(bars[i]["l"] - bars[i-1]["c"]))
        for i in range(1, len(bars))
    ]
    return round(sum(trs[-p:]) / p, 4)

def calc_bb(closes, p=20):
    if len(closes) < p:
        return None, None, None
    sma = sum(closes[-p:]) / p
    std = (sum((c - sma) ** 2 for c in closes[-p:]) / p) ** 0.5
    return round(sma + 2*std, 4), round(sma, 4), round(sma - 2*std, 4)

def calc_vol_ratio(bars):
    if len(bars) < 10:
        return None
    v   = [b["v"] for b in bars]
    avg = sum(v[:-1]) / len(v[:-1])
    return round(v[-1] / avg if avg > 0 else 0, 2)

def calc_rvol(bars_today, bars_yesterday):
    if not bars_today or not bars_yesterday:
        return None
    tv = sum(b["v"] for b in bars_today)
    yv = sum(b["v"] for b in bars_yesterday[:len(bars_today)])
    return round(tv / yv if yv > 0 else 0, 2)

def get_multiday(bars_daily):
    if not bars_daily or len(bars_daily) < 2:
        return None
    p  = bars_daily[-2]
    ph, pl, pc = round(p["h"], 4), round(p["l"], 4), round(p["c"], 4)
    pivot = round((ph + pl + pc) / 3, 4)
    return {
        "prev_high":  ph, "prev_low": pl, "prev_close": pc,
        "pivot":      pivot,
        "r1":         round(2 * pivot - pl, 4),
        "s1":         round(2 * pivot - ph, 4),
    }

def get_sr(bars, n=20):
    r = bars[-n:]
    return round(min(b["l"] for b in r), 4), round(max(b["h"] for b in r), 4)

def check_3tf(bars_1m, bars_5m, bars_15m, signal):
    agrees = 0
    for bars in [bars_1m, bars_5m, bars_15m]:
        if len(bars) < 10:
            continue
        closes = [b["c"] for b in bars]
        e9  = calc_ema(closes, 9)
        e21 = calc_ema(closes, min(21, len(closes) - 1))
        if not e9 or not e21:
            continue
        if signal == "BUY"  and e9 > e21: agrees += 1
        if signal == "SELL" and e9 < e21: agrees += 1
    return agrees

# ══════════════════════════════════════════════════════════
#  SMC ENGINE
# ══════════════════════════════════════════════════════════
def find_swings(bars, lookback=5):
    highs, lows = [], []
    for i in range(lookback, len(bars) - lookback):
        if bars[i]["h"] == max(b["h"] for b in bars[i-lookback:i+lookback+1]):
            highs.append({"idx": i, "price": bars[i]["h"]})
        if bars[i]["l"] == min(b["l"] for b in bars[i-lookback:i+lookback+1]):
            lows.append({"idx": i, "price": bars[i]["l"]})
    return highs, lows

def detect_bos_choch(bars, highs, lows):
    if not highs or not lows or len(bars) < 10:
        return None, None
    price = bars[-1]["c"]
    lh = highs[-1]["price"] if highs else None
    ll = lows[-1]["price"]  if lows  else None
    ph = highs[-2]["price"] if len(highs) >= 2 else None
    pl = lows[-2]["price"]  if len(lows)  >= 2 else None
    bos = choch = None
    if lh and price > lh: bos = "BULLISH_BOS"
    if ll and price < ll: bos = "BEARISH_BOS"
    if ph and lh and ph > lh and price > lh: choch = "BULLISH_CHOCH"
    if pl and ll and pl < ll and price < ll: choch = "BEARISH_CHOCH"
    return bos, choch

def find_order_blocks(bars, lookback=20):
    obs    = []
    recent = bars[-lookback:] if len(bars) >= lookback else bars
    for i in range(1, len(recent) - 2):
        b    = recent[i]
        body = abs(b["c"] - b["o"])
        if body == 0:
            continue
        if b["c"] < b["o"]:
            mu = sum(1 for j in range(i+1, min(i+4, len(recent)))
                     if recent[j]["c"] > recent[j]["o"])
            if mu >= 2:
                obs.append({"type": "BULLISH_OB", "high": b["h"], "low": b["l"],
                             "mid": round((b["h"] + b["l"]) / 2, 4)})
        if b["c"] > b["o"]:
            md = sum(1 for j in range(i+1, min(i+4, len(recent)))
                     if recent[j]["c"] < recent[j]["o"])
            if md >= 2:
                obs.append({"type": "BEARISH_OB", "high": b["h"], "low": b["l"],
                             "mid": round((b["h"] + b["l"]) / 2, 4)})
    return obs[-5:] if obs else []

def detect_liq_sweep(bars, highs, lows):
    if len(bars) < 5 or not highs or not lows:
        return None
    cur, prev = bars[-1], bars[-2]
    lh = highs[-1]["price"] if highs else None
    ll = lows[-1]["price"]  if lows  else None
    if ll and prev["l"] < ll and cur["c"] > ll: return "BULLISH_SWEEP"
    if lh and prev["h"] > lh and cur["c"] < lh: return "BEARISH_SWEEP"
    return None

def detect_fvg(bars):
    fvgs = []
    if len(bars) < 3:
        return fvgs
    for i in range(len(bars) - 3):
        b1, b2, b3 = bars[i], bars[i+1], bars[i+2]
        if b3["l"] > b1["h"]:
            fvgs.append({"type": "BULLISH_FVG", "top": b3["l"], "bottom": b1["h"],
                         "mid": round((b3["l"] + b1["h"]) / 2, 4)})
        if b3["h"] < b1["l"]:
            fvgs.append({"type": "BEARISH_FVG", "top": b1["l"], "bottom": b3["h"],
                         "mid": round((b1["l"] + b3["h"]) / 2, 4)})
    return fvgs[-3:] if fvgs else []

def detect_smt(bars, highs, lows):
    if len(bars) < 5 or not highs or not lows:
        return None
    cur, prev = bars[-1], bars[-2]
    lh = highs[-1]["price"] if highs else None
    ll = lows[-1]["price"]  if lows  else None
    if lh and prev["h"] > lh and cur["c"] < lh and cur["c"] < cur["o"]: return "BULL_TRAP"
    if ll and prev["l"] < ll and cur["c"] > ll and cur["c"] > cur["o"]: return "BEAR_TRAP"
    return None

def detect_po3(bars):
    if len(bars) < 15:
        return "UNKNOWN"
    recent = bars[-15:]
    closes = [b["c"] for b in recent]
    h5  = max(b["h"] for b in recent[:5])
    l5  = min(b["l"] for b in recent[:5])
    h10 = max(b["h"] for b in recent[5:10])
    l10 = min(b["l"] for b in recent[5:10])
    ap  = sum(closes) / len(closes)
    r1  = (h5 - l5)   / ap * 100
    r2  = (h10 - l10) / ap * 100
    if r1 < 2 and r2 > r1 * 1.5:
        return "DISTRIBUTION_BULLISH" if closes[-1] > closes[-5] else "DISTRIBUTION_BEARISH"
    if r1 < 1.5:
        return "ACCUMULATION"
    return "TRENDING"

def get_zone(bars, highs, lows):
    if not highs or not lows:
        return None, None, None
    rh    = highs[-1]["price"]
    rl    = lows[-1]["price"]
    eq    = round((rh + rl) / 2, 4)
    price = bars[-1]["c"]
    zone  = "PREMIUM" if price > eq else "DISCOUNT" if price < eq else "EQUILIBRIUM"
    return zone, eq, round((price - eq) / eq * 100, 2)

def detect_patterns(bars):
    patterns = []
    if len(bars) < 3:
        return patterns
    b0, b1, b2 = bars[-3], bars[-2], bars[-1]
    o2, h2, l2, c2 = b2["o"], b2["h"], b2["l"], b2["c"]
    o1, c1 = b1["o"], b1["c"]
    o0, c0 = b0["o"], b0["c"]
    body2  = abs(c2 - o2)
    range2 = h2 - l2
    body1  = abs(c1 - o1)
    if range2 > 0 and body2 / range2 < 0.1:
        patterns.append("Doji")
    if body2 > 0 and (l2 < min(o2, c2)) and (min(o2, c2) - l2) > 2 * body2 and c2 > o2:
        patterns.append("Hammer")
    if body2 > 0 and (h2 > max(o2, c2)) and (h2 - max(o2, c2)) > 2 * body2 and c2 < o2:
        patterns.append("Shooting Star")
    if c1 < o1 and c2 > o2 and c2 > o1 and o2 < c1:
        patterns.append("Bullish Engulfing")
    if c1 > o1 and c2 < o2 and c2 < o1 and o2 > c1:
        patterns.append("Bearish Engulfing")
    if c0 < o0 and body1 < abs(c0 - o0) * 0.5 and c2 > o2 and c2 > (o0 + c0) / 2:
        patterns.append("Morning Star")
    if range2 > 0 and body2 / range2 > 0.9:
        patterns.append("Bullish Marubozu" if c2 > o2 else "Bearish Marubozu")
    return patterns

def update_orb(ticker, bars_1m):
    ny = get_ny()
    if ny.hour == 9 and ny.minute == 30:
        orb_levels[ticker] = {"high": None, "low": None, "set": False}
    if not orb_levels.get(ticker):
        orb_levels[ticker] = {"high": None, "low": None, "set": False}
    ob = [b for b in bars_1m if "09:3" in b.get("t", "") or "09:4" in b.get("t", "")]
    if ob:
        orb_levels[ticker]["high"] = round(max(b["h"] for b in ob), 4)
        orb_levels[ticker]["low"]  = round(min(b["l"] for b in ob), 4)
        orb_levels[ticker]["set"]  = True

def check_orb(ticker, price):
    orb = orb_levels.get(ticker, {})
    if not orb.get("set"):
        return None
    if price > orb["high"] * 1.001: return "BUY_ORB"
    if price < orb["low"]  * 0.999: return "SELL_ORB"
    return None

# ══════════════════════════════════════════════════════════
#  SIGNAL ENGINE
# ══════════════════════════════════════════════════════════
def compute_signal(bars_1m, bars_5m, bars_15m):
    if len(bars_5m) < 20:
        return None

    closes   = [b["c"] for b in bars_5m]
    price    = closes[-1]
    vwap     = calc_vwap(bars_5m)
    vn, vp   = vwap[-1], vwap[-2]
    rsi      = calc_rsi(closes)
    ema9     = calc_ema(closes, 9)
    ema21    = calc_ema(closes, 21)
    atr      = calc_atr(bars_5m)
    vol_r    = calc_vol_ratio(bars_5m)
    sup, res = get_sr(bars_5m)
    bbu, bbm, bbl = calc_bb(closes)

    trend_15m = "NEUTRAL"
    if len(bars_15m) >= 21:
        c15 = [b["c"] for b in bars_15m]
        e9  = calc_ema(c15, 9)
        e21 = calc_ema(c15, 21)
        if e9 and e21:
            trend_15m = "BULL" if e9 > e21 else "BEAR"

    mom_1m = "NEUTRAL"
    if len(bars_1m) >= 5:
        c1 = [b["c"] for b in bars_1m[-5:]]
        mom_1m = "UP" if c1[-1] > c1[0] else "DOWN"

    highs, lows   = find_swings(bars_5m)
    bos, choch    = detect_bos_choch(bars_5m, highs, lows)
    obs           = find_order_blocks(bars_5m)
    fvgs          = detect_fvg(bars_5m)
    liq           = detect_liq_sweep(bars_5m, highs, lows)
    smt           = detect_smt(bars_5m, highs, lows)
    po3           = detect_po3(bars_5m)
    zone, eq, zp  = get_zone(bars_5m, highs, lows)

    nearest_ob = next((ob for ob in reversed(obs) if
        (ob["type"] == "BULLISH_OB" and ob["low"] <= price <= ob["high"] * 1.02) or
        (ob["type"] == "BEARISH_OB" and ob["low"] * 0.98 <= price <= ob["high"])), None)
    nearest_fvg = next((f for f in reversed(fvgs) if f["bottom"] <= price <= f["top"]), None)

    above = price > vn
    ca = price > vn and closes[-2] <= vp
    cb = price < vn and closes[-2] >= vp
    hv = vol_r and vol_r >= 1.5
    eb = ema9 > ema21 if ema9 and ema21 else False

    bs = ss = 0
    if ca:        bs += 2
    elif above:   bs += 1
    if cb:        ss += 2
    elif not above: ss += 1

    if hv:
        if above: bs += 2
        else:     ss += 2

    if rsi:
        if rsi < 35:            bs += 2
        elif rsi < 50 and above: bs += 1
        if rsi > 65:            ss += 2
        elif rsi > 50 and not above: ss += 1

    if eb:   bs += 1
    else:    ss += 1

    if trend_15m == "BULL": bs += 2
    if trend_15m == "BEAR": ss += 2
    if mom_1m == "UP":   bs += 1
    if mom_1m == "DOWN": ss += 1

    if bbl and price <= bbl: bs += 1
    if bbu and price >= bbu: ss += 1

    if bos == "BULLISH_BOS":   bs += 2
    if bos == "BEARISH_BOS":   ss += 2
    if choch == "BULLISH_CHOCH": bs += 3
    if choch == "BEARISH_CHOCH": ss += 3

    if nearest_ob:
        if nearest_ob["type"] == "BULLISH_OB": bs += 3
        if nearest_ob["type"] == "BEARISH_OB": ss += 3

    if liq == "BULLISH_SWEEP": bs += 3
    if liq == "BEARISH_SWEEP": ss += 3

    if nearest_fvg:
        if nearest_fvg["type"] == "BULLISH_FVG": bs += 2
        if nearest_fvg["type"] == "BEARISH_FVG": ss += 2

    if po3 == "DISTRIBUTION_BULLISH": bs += 2
    if po3 == "DISTRIBUTION_BEARISH": ss += 2

    if zone == "DISCOUNT": bs += 2
    if zone == "PREMIUM":  ss += 2

    if smt == "BULL_TRAP": bs -= 5; ss += 2
    if smt == "BEAR_TRAP": ss -= 5; bs += 2

    if bs >= MIN_SCORE:
        signal = "BUY"
        conf   = "HIGH" if bs >= 10 else "MEDIUM"
    elif ss >= MIN_SCORE:
        signal = "SELL"
        conf   = "HIGH" if ss >= 10 else "MEDIUM"
    else:
        return None

    if atr:
        sl = round(price - atr * 1.5, 4) if signal == "BUY" else round(price + atr * 1.5, 4)
        tp = round(price + atr * 3.0, 4) if signal == "BUY" else round(price - atr * 3.0, 4)
    else:
        sl = round(min(vn, sup) * 0.995, 4) if signal == "BUY" else round(max(vn, res) * 1.005, 4)
        tp = round(res * 0.998, 4)           if signal == "BUY" else round(sup * 1.002, 4)

    risk   = abs(price - sl)
    reward = abs(tp - price)
    rr     = "1:" + str(round(reward / risk, 1)) if risk > 0 else "N/A"

    return {
        "signal": signal, "confidence": conf, "entry": round(price, 4),
        "sl": sl, "tp": tp, "rr": rr,
        "vwap": vn, "rsi": rsi, "ema9": ema9, "ema21": ema21,
        "atr": atr, "vol_ratio": vol_r,
        "trend": "UPTREND" if eb else "DOWNTREND",
        "trend_15m": trend_15m, "momentum_1m": mom_1m,
        "bb_upper": bbu, "bb_lower": bbl,
        "buy_score": bs, "sell_score": ss,
        "support": sup, "resistance": res,
        "bb_squeeze": (bbu - bbl) < atr * 2 if bbu and bbl and atr else False,
        "bos": bos, "choch": choch,
        "nearest_ob": nearest_ob, "nearest_fvg": nearest_fvg,
        "liq_sweep": liq, "smt": smt, "po3": po3,
        "zone": zone, "equilibrium": eq, "zone_pct": zp,
    }

# ══════════════════════════════════════════════════════════
#  NEWS & SENTIMENT
# ══════════════════════════════════════════════════════════
async def get_news(session: aiohttp.ClientSession, ticker: str):
    try:
        async with session.get(
            "https://query1.finance.yahoo.com/v1/finance/search",
            params={"q": ticker, "newsCount": 3},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            data  = await r.json()
            news  = data.get("news", [])
            if not news:
                return "NEUTRAL", []
            headlines = [n.get("title", "") for n in news[:3]]
            neg = ["downgrade","loss","miss","decline","drop","fail","lawsuit",
                   "fraud","warning","cut","crash"]
            pos = ["upgrade","beat","surge","rally","bullish","profit","deal",
                   "launch","record","breakout"]
            nc  = sum(1 for h in headlines for w in neg if w in h.lower())
            pc  = sum(1 for h in headlines for w in pos if w in h.lower())
            return ("NEGATIVE" if nc > pc else "POSITIVE" if pc > nc else "NEUTRAL"), headlines
    except Exception as e:
        log(f"get_news {ticker}: {e}")
        return "NEUTRAL", []

async def get_stocktwits(session: aiohttp.ClientSession, ticker: str):
    empty = {"sentiment": "NEUTRAL", "bull_pct": 50, "bear_pct": 50,
             "total": 0, "trending": False, "top_post": ""}
    try:
        now  = get_ny()
        last = stocktwits_cooldown.get(ticker)
        if last and (now - last).total_seconds() < 3600:
            return empty
        async with session.get(
            f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            data     = await r.json()
            messages = data.get("messages", [])
            if not messages:
                return empty
            bull = sum(1 for m in messages
                       if isinstance(m.get("entities", {}).get("sentiment"), dict)
                       and m["entities"]["sentiment"].get("basic") == "Bullish")
            bear = sum(1 for m in messages
                       if isinstance(m.get("entities", {}).get("sentiment"), dict)
                       and m["entities"]["sentiment"].get("basic") == "Bearish")
            total = bull + bear
            if total < 3:
                return {**empty, "total": total, "trending": len(messages) >= 10}
            bp   = round(bull / total * 100)
            bep  = round(bear / total * 100)
            sent = "BULLISH" if bp >= 60 else "BEARISH" if bep >= 60 else "NEUTRAL"
            top  = ""
            for m in messages[:5]:
                body = m.get("body", "").strip()
                if len(body) > 15:
                    top = body[:80]
                    break
            stocktwits_cooldown[ticker] = now
            return {"sentiment": sent, "bull_pct": bp, "bear_pct": bep,
                    "total": total, "trending": len(messages) >= 15, "top_post": top}
    except Exception as e:
        log(f"get_stocktwits {ticker}: {e}")
        return empty

async def stocktwits_premarket_scan(session: aiohttp.ClientSession):
    lines     = ["📡 Pre/After Market Stocktwits Scan", ""]
    hot       = []
    scan_list = list(set(list(watchlist) + list(DEFAULT_WATCHLIST)))
    for ticker in scan_list:
        try:
            st = await get_stocktwits(session, ticker)
            if st["total"] >= 5 and st["sentiment"] != "NEUTRAL":
                if st["bull_pct"] >= 70 or st["bear_pct"] >= 70:
                    hot.append({**st, "ticker": ticker})
            await asyncio.sleep(0.5)
        except:
            continue
    if hot:
        hot.sort(key=lambda x: x["bull_pct"] if x["sentiment"] == "BULLISH" else x["bear_pct"],
                 reverse=True)
        lines.append("Hot stocks right now:")
        for h in hot[:5]:
            trend = " 🔥TRENDING" if h["trending"] else ""
            lines.append(f"{h['ticker']} - {h['sentiment']}{trend} | {h['bull_pct']}% Bulls")
            if h["top_post"]: lines.append("  " + h["top_post"][:60])
        lines.append("\nWatch these at market open!")
        await tg(session, "\n".join(lines))
    else:
        log("No hot Stocktwits stocks found")

# ══════════════════════════════════════════════════════════
#  AI CONFIRMATION (Groq)
# ══════════════════════════════════════════════════════════
async def ai_confirm(session: aiohttp.ClientSession,
                     ticker: str, result: dict, patterns, sentiment: str, tf_agrees: int):
    smc = (f"BOS={result.get('bos')} CHoCH={result.get('choch')} "
           f"OB={result.get('nearest_ob',{}).get('type') if result.get('nearest_ob') else 'None'} "
           f"Sweep={result.get('liq_sweep')} Zone={result.get('zone')} "
           f"PO3={result.get('po3')} SMT={result.get('smt')}")
    score = result.get("buy_score" if result["signal"] == "BUY" else "sell_score", 0)
    json_schema = '{"verdict":"CONFIRMED" or "REJECTED" or "CAUTION","reason":"one sentence","tip":"one actionable tip"}'
    prompt = (
        f"Trade signal {ticker}: {result['signal']} Score:{score}/20 "
        f"RSI:{result['rsi']} Vol:{result['vol_ratio']}x {smc} "
        f"News:{sentiment} 3TF:{tf_agrees}/3 "
        f"Respond ONLY JSON: {json_schema}"
    )
    try:
        async with session.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model":    "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 200, "temperature": 0.1,
            },
            timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            data = await r.json()
            if "choices" not in data:
                log(f"Groq API error: {data}")
                return {"verdict": "CONFIRMED", "reason": "AI rate limited", "tip": "Check manually"}
            raw = data["choices"][0]["message"]["content"].strip()
            raw = raw.replace("```json", "").replace("```", "").strip()
            s   = raw.find("{"); e = raw.rfind("}") + 1
            if s >= 0 and e > s:
                raw = raw[s:e]
            raw = raw.replace("\n", "").replace("\t", "")
            try:
                return json.loads(raw)
            except:
                verdict = "CONFIRMED"
                if '"REJECTED"' in raw: verdict = "REJECTED"
                elif '"CAUTION"' in raw: verdict = "CAUTION"
                return {"verdict": verdict, "reason": "AI parse error", "tip": "Check manually"}
    except Exception as e:
        log_err("ai_confirm", e)
        return {"verdict": "CONFIRMED", "reason": "AI unavailable", "tip": "Use your judgment"}

# ══════════════════════════════════════════════════════════
#  MESSAGE BUILDER
# ══════════════════════════════════════════════════════════
def build_msg(ticker, result, yp, yc, status, sentiment, headlines, patterns,
              ai, rvol, multiday, spy_trend, spy_chg, tf_agrees,
              orb_tag, shares, cost, leverage, st_data, price_source):
    sig    = result["signal"]
    now    = get_ny().strftime("%H:%M:%S") + " NY"
    score  = result["buy_score"] if sig == "BUY" else result["sell_score"]
    verdict = ai.get("verdict", "CONFIRMED")
    sp_sign = "+" if spy_chg >= 0 else ""
    yc_sign = "+" if yc and yc >= 0 else ""

    header  = ("📈 BUY" if sig == "BUY" else "📉 SELL") + f" - {ticker}"
    if orb_tag: header += " ORB!"
    header += f"  {result['confidence']}"
    if AUTO_TRADE: header += " [AUTO-TRADE]"

    smc_lines = []
    if result.get("choch"):     smc_lines.append(f"CHoCH: {result['choch']}")
    elif result.get("bos"):     smc_lines.append(f"BOS: {result['bos']}")
    if result.get("liq_sweep"): smc_lines.append(f"Liq Sweep: {result['liq_sweep']}")
    if result.get("nearest_ob"):
        ob = result["nearest_ob"]
        smc_lines.append(f"Order Block: {ob['type']} @ ${ob['mid']}")
    if result.get("smt"):       smc_lines.append(f"⚠️ WARNING: {result['smt']}")
    if result.get("zone"):
        smc_lines.append(f"Zone: {result['zone']} ({result.get('zone_pct','')}% from ${result.get('equilibrium','')})")

    verdict_emoji = "✅" if verdict == "CONFIRMED" else "❌" if verdict == "REJECTED" else "⚠️"

    lines = [
        header,
        ("MARKET OPEN" if status == "OPEN" else "PRE-MARKET") + " | " + get_session_name(),
        f"SPY: {spy_trend} {sp_sign}{spy_chg}%", "",
    ]
    if yp:
        lines.append(f"LIVE ${yp} {yc_sign}{yc}% [{price_source}]")
    lines += [
        "",
        f"Entry:       ${result['entry']}",
        f"Stop Loss:   ${result['sl']}",
        f"Take Profit: ${result['tp']}",
        f"R/R: {result['rr']}  ATR: ${result['atr']}", "",
        f"Auto Position: {shares} shares (${cost})",
        f"Leverage: 1:{leverage}  Risk: ${round(ACCOUNT_SIZE * RISK_PCT / 100, 2)}", "",
        "SMC:",
    ]
    lines.extend(smc_lines if smc_lines else ["No SMC setup"])
    lines += [
        "", "TECHNICALS:",
        f"VWAP: ${result['vwap']}  RSI: {result['rsi']}",
        f"EMA 9/21: {result['ema9']}/{result['ema21']}",
        f"Vol: {result['vol_ratio']}x  RVOL: {rvol}x",
        f"15m: {result['trend_15m']}  1m: {result['momentum_1m']}",
        f"3TF: {tf_agrees}/3  Score: {score}/20",
    ]
    if result.get("bb_squeeze"):
        lines.append("🔔 BB SQUEEZE!")
    if multiday:
        lines.append(f"Pivot: ${multiday['pivot']}  PH: ${multiday['prev_high']}  PL: ${multiday['prev_low']}")
    lines += [
        "",
        f"{verdict_emoji} {verdict} - {ai.get('reason','')}",
        f"💡 Tip: {ai.get('tip','')}",
        "",
        f"Patterns: {', '.join(patterns) if patterns else 'None'}",
        f"News: {sentiment}" + (f" - {headlines[0][:50]}" if headlines else ""),
    ]
    if st_data and st_data.get("total", 0) > 0:
        trend_tag = " 🔥TRENDING" if st_data.get("trending") else ""
        lines += [
            "",
            f"Stocktwits: {st_data['sentiment']}{trend_tag} | {st_data['bull_pct']}% Bulls",
        ]
    lines += [
        "",
        f"S: ${result['support']}  R: ${result['resistance']}",
        now,
    ]
    return "\n".join(lines)

# ══════════════════════════════════════════════════════════
#  TRAILING STOPS & ALERTS
# ══════════════════════════════════════════════════════════
async def check_trailing(session: aiohttp.ClientSession):
    global daily_pnl, losing_streak
    for ticker in list(active_trades.keys()):
        try:
            trade = active_trades[ticker]
            yp, _ = await get_realtime_price(session, ticker)
            if not yp:
                continue
            entry = trade["entry"]
            sig   = trade["signal"]
            atr   = trade.get("atr", abs(entry - trade["sl"]))

            if sig == "BUY":
                nt = round(yp - atr * 1.5, 4)
                ct = trailing_stops.get(ticker, trade["sl"])
                if nt > ct:
                    trailing_stops[ticker] = nt
                    log(f"Trailing SL updated {ticker}: ${nt}")
                if yp <= trailing_stops.get(ticker, trade["sl"]):
                    pnl = round((yp - entry) / entry * 100, 2)
                    daily_pnl += pnl
                    losing_streak = losing_streak + 1 if pnl < 0 else 0
                    sign = "+" if pnl >= 0 else ""
                    await tg(session,
                        f"{'✅ PROFIT' if pnl > 0 else '❌ STOP HIT'} - {ticker}\n"
                        f"Exit: ${yp}\nP&L: {sign}{pnl}%")
                    performance_log.append({
                        "ticker": ticker, "signal": sig, "entry": entry,
                        "exit": yp, "pnl": pnl, "time": get_ny().strftime("%H:%M")
                    })
                    all_time_log.append({
                        "ticker": ticker, "signal": sig, "entry": entry,
                        "exit": yp, "pnl": pnl, "date": get_ny().strftime("%Y-%m-%d")
                    })
                    del active_trades[ticker]
                    trailing_stops.pop(ticker, None)
        except Exception as e:
            log_err(f"check_trailing {ticker}", e)

async def check_alerts(session: aiohttp.ClientSession):
    for ticker in list(price_alerts.keys()):
        try:
            alerts = price_alerts[ticker]
            if not alerts:
                continue
            yp, _ = await get_realtime_price(session, ticker)
            if not yp:
                continue
            remaining = []
            for a in alerts:
                hit = (a["direction"] == "above" and yp >= a["price"]) or \
                      (a["direction"] == "below" and yp <= a["price"])
                if hit:
                    await tg(session, f"🔔 PRICE ALERT: {ticker} hit ${yp}")
                else:
                    remaining.append(a)
            price_alerts[ticker] = remaining
        except Exception as e:
            log(f"check_alerts {ticker}: {e}")

# ══════════════════════════════════════════════════════════
#  REPORTS
# ══════════════════════════════════════════════════════════
async def morning_brief(session: aiohttp.ClientSession):
    ny   = get_ny().strftime("%A %B %d")
    lines = [f"☀️ Morning Brief - {ny}", ""]
    sp, sc, _ = await yahoo_price(session, "SPY")
    qp, qc, _ = await yahoo_price(session, "QQQ")
    vp, vc, _ = await yahoo_price(session, "^VIX")
    sign = lambda v: "+" if v and v >= 0 else ""
    if sp: lines.append(f"SPY ${sp} {sign(sc)}{sc}%")
    if qp: lines.append(f"QQQ ${qp} {sign(qc)}{qc}%")
    if vp: lines.append(f"VIX ${vp} {'🔴 HIGH FEAR' if vp > 25 else '🟡 ELEVATED' if vp > 18 else '🟢 CALM'}")
    if daily_picks:
        lines += ["", "Today's picks:"]
        for c in daily_picks:
            lines.append(f"{c['ticker']} ${c['price']} RVOL:{c['rvol']}x Score:{c['score']}")
    lines += ["", "Market opens in ~5 min! Auto-trading active! 🚀"]
    await tg(session, "\n".join(lines))

async def daily_report(session: aiohttp.ClientSession):
    acct = await get_account_info(session)
    lines = ["📊 Daily Report", ""]
    if acct:
        lines += [f"Account: ${acct['equity']}", f"Cash: ${acct['cash']}", ""]
    if performance_log:
        wins = [t for t in performance_log if t["pnl"] > 0]
        tp   = round(sum(t["pnl"] for t in performance_log), 2)
        wr   = round(len(wins) / len(performance_log) * 100, 1)
        lines += [
            f"Trades: {len(performance_log)} | Wins: {len(wins)} | Losses: {len(performance_log)-len(wins)}",
            f"Win Rate: {wr}%", f"P&L: {tp}%", ""
        ]
        for t in performance_log:
            emoji = "✅" if t["pnl"] > 0 else "❌"
            lines.append(f"{emoji} {t['ticker']} {t['signal']} {t['pnl']}%")
    else:
        lines.append("No completed trades today.")
    await tg(session, "\n".join(lines))
    performance_log.clear()

async def weekly_report(session: aiohttp.ClientSession):
    if not all_time_log:
        await tg(session, "📅 Weekly Report\n\nNo trades this week.")
        return
    wins  = [t for t in all_time_log if t["pnl"] > 0]
    tp    = round(sum(t["pnl"] for t in all_time_log), 2)
    wr    = round(len(wins) / len(all_time_log) * 100, 1)
    best  = max(all_time_log, key=lambda x: x["pnl"])
    worst = min(all_time_log, key=lambda x: x["pnl"])
    await tg(session, "\n".join([
        "📅 Weekly Report", "",
        f"Trades: {len(all_time_log)}",
        f"Win Rate: {wr}%",
        f"Total P&L: {tp}%",
        f"Best: {best['ticker']} +{best['pnl']}%",
        f"Worst: {worst['ticker']} {worst['pnl']}%",
    ]))

async def run_backtest(session: aiohttp.ClientSession, ticker: str):
    bars = await get_yahoo_bars(session, ticker, 90)
    if not bars or len(bars) < 15:
        return None
    wins = losses = 0
    total_pnl = 0
    trades = []
    for i in range(14, len(bars) - 1):
        seg    = bars[max(0, i-20):i+1]
        closes = [b["c"] for b in seg]
        price  = closes[-1]
        ema9   = calc_ema(closes, 9)
        ema21  = calc_ema(closes, 21)
        rsi    = calc_rsi(closes)
        if not all([ema9, ema21, rsi]):
            continue
        vwap  = calc_vwap(seg)[-1]
        atr   = calc_atr(seg)
        highs, lows  = find_swings(seg)
        zone, _, _   = get_zone(seg, highs, lows)
        liq          = detect_liq_sweep(seg, highs, lows)
        smt          = detect_smt(seg, highs, lows)
        if smt:
            continue
        bull = ema9 > ema21 and rsi < 50 and price > vwap and zone == "DISCOUNT"
        bear = ema9 < ema21 and rsi > 50 and price < vwap and zone == "PREMIUM"
        bos, _ = detect_bos_choch(seg, highs, lows)
        if bos == "BULLISH_BOS" or liq == "BULLISH_SWEEP": bull = True
        if bos == "BEARISH_BOS" or liq == "BEARISH_SWEEP": bear = True
        if bull:       signal = "BUY"
        elif bear:     signal = "SELL"
        else:          continue
        nb  = bars[i + 1]
        pnl = round((nb["c"] - price) / price * 100, 2) if signal == "BUY" \
              else round((price - nb["c"]) / price * 100, 2)
        if pnl > 0: wins += 1
        else:       losses += 1
        total_pnl += pnl
        trades.append(pnl)
    total = wins + losses
    if total == 0:
        return None
    return {
        "ticker":       ticker,
        "total_trades": total,
        "wins":         wins,
        "losses":       losses,
        "win_rate":     round(wins / total * 100, 1),
        "avg_pnl":      round(total_pnl / total, 2),
        "total_pnl":    round(total_pnl, 2),
        "best":         round(max(trades), 2),
        "worst":        round(min(trades), 2),
    }

# ══════════════════════════════════════════════════════════
#  WATCHLIST REPLACEMENT
# ══════════════════════════════════════════════════════════
async def replace_ticker(removed_ticker: str):
    log(f"Finding replacement for {removed_ticker}...")
    async with aiohttp.ClientSession() as session:
        try:
            candidates = []
            for ticker in SCAN_UNIVERSE:
                if ticker in watchlist or ticker == removed_ticker:
                    continue
                try:
                    p, chg, vol = await yahoo_price(session, ticker)
                    if not p or not vol or p < 1 or abs(chg or 0) < 1:
                        continue
                    bars  = await get_bars(session, ticker, "5Min", 20)
                    if len(bars) < 10:
                        continue
                    closes = [b["c"] for b in bars]
                    rsi    = calc_rsi(closes)
                    vol_r  = calc_vol_ratio(bars)
                    atr    = calc_atr(bars)
                    if not all([rsi, vol_r, atr]):
                        continue
                    atr_pct = (atr / p) * 100
                    if 30 < rsi < 60 and vol_r >= 1.3 and atr_pct >= 1.5 and (chg or 0) > 0 and p < 100:
                        score = 0
                        if vol_r >= 2:          score += 3
                        elif vol_r >= 1.5:      score += 2
                        else:                   score += 1
                        if atr_pct >= 4:        score += 3
                        elif atr_pct >= 2:      score += 2
                        else:                   score += 1
                        if 35 <= rsi <= 55:     score += 2
                        candidates.append({"ticker": ticker, "score": score})
                    await asyncio.sleep(0.3)
                except:
                    continue

            if candidates:
                candidates.sort(key=lambda x: x["score"], reverse=True)
                best = candidates[0]["ticker"]
                watchlist.append(best)
                log(f"Replaced {removed_ticker} → {best}")
            else:
                for t in DEFAULT_WATCHLIST:
                    if t not in watchlist:
                        watchlist.append(t)
                        log(f"Restored default {t} after removing {removed_ticker}")
                        break
        except Exception as e:
            log_err("replace_ticker", e)

# ══════════════════════════════════════════════════════════
#  TELEGRAM COMMAND HANDLER
# ══════════════════════════════════════════════════════════
async def handle_cmds(session: aiohttp.ClientSession, offset: int):
    global bot_paused, watchlist, ACCOUNT_SIZE, AUTO_TRADE

    updates = await get_updates(session, offset + 1)
    for update in updates:
        try:
            offset = update["update_id"]
            text   = update.get("message", {}).get("text", "").strip()
            if not text:
                continue
            log(f"CMD: {text}")

            if text == "/status":
                al   = ", ".join([f"{t}:{v['signal']}" for t, v in active_trades.items()]) or "None"
                acct = await get_account_info(session)
                eq   = str(acct["equity"]) if acct else "N/A"
                picks = ", ".join([c["ticker"] for c in daily_picks]) if daily_picks else "Not scanned yet"
                await tg(session, "\n".join([
                    "🤖 Bot Status", "",
                    get_ny().strftime("%H:%M:%S") + " NY | " + market_status(),
                    "AUTO-TRADE: " + ("ON [PAPER]" if AUTO_TRADE else "OFF"),
                    f"Account: ${eq}",
                    f"Trades today: {trades_today}/{MAX_TRADES_PER_DAY}",
                    f"Daily P&L: {round(daily_pnl, 2)}%",
                    f"Today's picks: {picks}",
                    f"Active: {al}",
                    f"Watchlist: {', '.join(watchlist)}",
                ]))

            elif text.startswith("/add "):
                try:
                    t = text.split()[1].upper()
                    if t not in watchlist:
                        watchlist.append(t)
                        await tg(session, f"✅ Added {t}! Watching: {', '.join(watchlist)}")
                    else:
                        await tg(session, f"{t} already in watchlist: {', '.join(watchlist)}")
                except:
                    await tg(session, "Usage: /add RGTI")

            elif text.startswith("/remove "):
                try:
                    t = text.split()[1].upper()
                    if t in watchlist:
                        watchlist.remove(t)
                        await tg(session, f"Removed {t}. Watching: {', '.join(watchlist)}")
                    else:
                        await tg(session, f"{t} not in watchlist")
                except:
                    await tg(session, "Usage: /remove RGTI")

            elif text == "/autotrade on":
                AUTO_TRADE = True
                await tg(session, "✅ Auto-trading ENABLED [PAPER MODE]")

            elif text == "/autotrade off":
                AUTO_TRADE = False
                await tg(session, "⛔ Auto-trading DISABLED - signals only")

            elif text == "/account":
                acct = await get_account_info(session)
                pos  = await get_open_positions(session)
                if acct:
                    lines = ["💰 Alpaca PAPER Account", "",
                             f"Equity: ${acct['equity']}",
                             f"Cash: ${acct['cash']}",
                             f"Buying Power: ${acct['buying_power']}",
                             f"Open Positions: {len(pos)}", ""]
                    for sym, p in pos.items():
                        pnl = round(float(p.get("unrealized_pl", 0)), 2)
                        lines.append(f"{sym}: {p.get('qty','')} shares  P&L: ${pnl}")
                    await tg(session, "\n".join(lines))
                else:
                    await tg(session, "Could not fetch account info.")

            elif text == "/positions":
                pos = await get_open_positions(session)
                if not pos:
                    await tg(session, "No open positions.")
                else:
                    lines = ["📋 Open Positions:", ""]
                    for sym, p in pos.items():
                        pnl     = round(float(p.get("unrealized_pl",   0)), 2)
                        pnl_pct = round(float(p.get("unrealized_plpc", 0)) * 100, 2)
                        lines += [
                            f"{sym} | {p.get('side','').upper()}",
                            f"  Qty: {p.get('qty','')}  Entry: ${p.get('avg_entry_price','')}",
                            f"  P&L: ${pnl} ({pnl_pct}%)", ""
                        ]
                    await tg(session, "\n".join(lines))

            elif text.startswith("/close "):
                t = text.split()[1].upper()
                await close_position(session, t)
                await tg(session, f"Closed: {t}")

            elif text == "/closeall":
                await close_all_positions(session)
                await tg(session, "✅ All positions closed!")

            elif text == "/scan":
                new_wl = await smart_daily_scan(session)
                watchlist.clear()
                watchlist.extend(new_wl)

            elif text == "/watchlist":
                await tg(session, "👀 Watching: " + (", ".join(watchlist) if watchlist else "Empty"))

            elif text == "/crypto":
                if not CRYPTO_ENABLED:
                    await tg(session, "Crypto module is disabled.")
                else:
                    lines = ["₿ Crypto Status", "",
                             f"Trades today: {crypto_trades_today}",
                             f"Daily P&L: {round(crypto_daily_pnl, 2)}%",
                             f"Open positions: {len(crypto_active_trades)}/{CRYPTO_MAX_OPEN}", ""]
                    if crypto_active_trades:
                        for sym, t in crypto_active_trades.items():
                            lines.append(f"{sym}: {t['qty']} @ ${t['entry']}  SL:${t['sl']} TP:${t['tp']}")
                    else:
                        lines.append("No open crypto positions")
                    lines.append("")
                    lines.append("Watching: " + ", ".join(CRYPTO_UNIVERSE))
                    await tg(session, "\n".join(lines))

            elif text == "/data":
                sip_status, sip_detail = await check_sip_access(session)
                if sip_status == "yes":
                    await tg(session, f"📡 Data feed: ✅ SIP active\n{sip_detail}")
                elif sip_status == "no":
                    await tg(session,
                        f"📡 Data feed: ⚠️ SIP NOT active\n{sip_detail}\n\n"
                        "On free IEX data. Upgrade to Algo Trader Plus ($99/mo) "
                        "on the account tied to your DATA keys for accurate prices.")
                else:
                    await tg(session, f"📡 Data feed: ❓ Could not confirm\n{sip_detail}")

            elif text == "/pause":
                bot_paused = True
                await tg(session, "⏸ Bot paused.")

            elif text == "/resume":
                bot_paused = False
                await tg(session, "▶️ Bot resumed!")

            elif text == "/report":
                await daily_report(session)

            elif text == "/weekly":
                await weekly_report(session)

            elif text.startswith("/risk "):
                try:
                    ACCOUNT_SIZE = float(text.split()[1])
                    await tg(session, f"Account size: ${ACCOUNT_SIZE}")
                except:
                    await tg(session, "Usage: /risk 10000")

            elif text.startswith("/alert "):
                try:
                    parts     = text.split()
                    t         = parts[1].upper()
                    target    = float(parts[2])
                    p, _      = await get_realtime_price(session, t)
                    direction = "above" if p and target > p else "below"
                    if t not in price_alerts:
                        price_alerts[t] = []
                    price_alerts[t].append({"price": target, "direction": direction})
                    await tg(session, f"🔔 Alert set: {t} {direction} ${target}")
                except:
                    await tg(session, "Usage: /alert RGTI 25.00")

            elif text == "/brief":
                await morning_brief(session)

            elif text.startswith("/backtest"):
                parts      = text.split()
                tickers_bt = ([parts[1].upper()] if len(parts) > 1
                              else [c["ticker"] for c in daily_picks] if daily_picks
                              else ["RGTI", "MARA"])
                await tg(session, "🔬 Backtesting: " + ", ".join(tickers_bt) + "...")
                for t in tickers_bt:
                    try:
                        bt = await run_backtest(session, t)
                        if bt:
                            grade = "🟢 GOOD" if bt["win_rate"] >= 55 else "🟡 OK" if bt["win_rate"] >= 45 else "🔴 POOR"
                            await tg(session,
                                f"Backtest {bt['ticker']} - {grade}\n"
                                f"Win Rate: {bt['win_rate']}%\n"
                                f"Trades: {bt['total_trades']} | W:{bt['wins']} L:{bt['losses']}\n"
                                f"Avg P&L: {bt['avg_pnl']}%\n"
                                f"Total P&L: {bt['total_pnl']}%")
                        else:
                            await tg(session, f"Not enough data for {t}")
                    except Exception as e:
                        await tg(session, f"Backtest error {t}: {str(e)[:80]}")
                    await asyncio.sleep(1)

            elif text == "/help":
                await tg(session, "\n".join([
                    "🤖 AlphaSignal SMC Auto-Trade Bot", "",
                    "AUTO-TRADING:",
                    "/autotrade on/off",
                    "/account - balance & P&L",
                    "/positions - open trades",
                    "/close RGTI - close one position",
                    "/closeall - close all positions", "",
                    "SCANNING:",
                    "/scan - find today's best stocks",
                    "/watchlist - current watchlist",
                    "/data - check SIP data feed status",
                    "/crypto - crypto module status",
                    "/backtest [TICKER] - test strategy", "",
                    "CONTROL:",
                    "/status - full bot status",
                    "/pause / /resume",
                    "/report - daily summary",
                    "/weekly - weekly summary",
                    "/risk 10000 - set account size",
                    "/alert RGTI 25.00 - price alert",
                    "/brief - morning market brief",
                    "/help - this menu",
                ]))

        except Exception as e:
            log_err("handle_cmds", e)
            try:
                await tg(session, f"Command error: {str(e)[:80]}")
            except:
                pass
    return offset

# ══════════════════════════════════════════════════════════
#  MAIN TRADING LOOP
# ══════════════════════════════════════════════════════════
async def main():
    global last_update_id, morning_brief_sent, trades_today, watchlist
    global last_scan_day, weekly_report_sent, positions_closed_today
    global last_heartbeat_minute, last_report_day

    log("AlphaSignal SMC Auto-Trader starting...")

    # Brief startup pause — avoid duplicate messages on Railway redeploy
    await asyncio.sleep(5)

    async with aiohttp.ClientSession() as session:
        await tg(session,
            "🚀 AlphaSignal SMC AUTO-TRADER Online!\n\n"
            "Mode: PAPER TRADING\n"
            f"Account: ${ACCOUNT_SIZE}\n\n"
            "Schedule (NY time):\n"
            "8:00 AM - Pre-market Stocktwits scan\n"
            "9:20 AM - Smart daily scan (top 3 picks)\n"
            "9:25 AM - Morning brief\n"
            "9:30 AM - Trading begins\n"
            "3:45 PM - Close all positions\n"
            "4:05 PM - Daily report\n\n"
            f"Leverage: HIGH=1:2  MEDIUM=1:1\n"
            f"Risk/trade: {RISK_PCT}% = ${round(ACCOUNT_SIZE * RISK_PCT / 100, 2)}\n\n"
            "/help for all commands"
        )

        # Verify SIP data entitlement so the user knows if prices are accurate.
        # Brief pause first so we don't collide with the price poller's burst
        # and trip Alpaca's rate limit (which would give a misleading 429).
        await asyncio.sleep(8)
        try:
            sip_status, sip_detail = await check_sip_access(session)
            if sip_status == "yes":
                await tg(session, f"📡 Data feed: ✅ SIP active ({sip_detail})\nPrices are full-market accurate.")
            elif sip_status == "no":
                await tg(session,
                    f"📡 Data feed: ⚠️ SIP NOT active\n{sip_detail}\n\n"
                    "You're on free IEX data (~2% of market volume). Prices may be "
                    "thin/inaccurate and the scanner may show 'data unavailable'.\n\n"
                    "Fix: Alpaca dashboard → Plans & Features → Upgrade to Algo Trader Plus "
                    "($99/mo) on the account tied to your DATA keys.")
            else:  # unknown
                await tg(session,
                    f"📡 Data feed: ❓ Could not confirm SIP\n{sip_detail}\n"
                    "Send /data in a minute to re-check.")
            log(f"SIP check: {sip_status} ({sip_detail})")
        except Exception as e:
            log_err("SIP check", e)

        # If market is already open on startup, scan immediately
        if market_status() == "OPEN":
            log("Market open on startup — running immediate scan")
            try:
                new_wl = await smart_daily_scan(session)
                if new_wl:
                    watchlist.clear()
                    watchlist.extend(new_wl)
                    log("Startup watchlist: " + str(watchlist))
            except Exception as e:
                log_err("startup scan", e)

        # ── MAIN LOOP ─────────────────────────────────────
        while True:
            loop_start = time.monotonic()
            try:
                # Process Telegram commands
                try:
                    last_update_id = await handle_cmds(session, last_update_id)
                except Exception as e:
                    log_err("handle_cmds", e)

                ny     = get_ny()
                status = market_status()

                # ── Midnight reset ────────────────────────
                if ny.hour == 0 and ny.minute == 0:
                    reset_daily()
                    morning_brief_sent = False
                    last_scan_day      = -1
                    weekly_report_sent = False

                # ── Heartbeat every 30 min during market ─
                if status == "OPEN" and ny.minute in [0, 30] and ny.second < 35:
                    if ny.minute != last_heartbeat_minute:
                        last_heartbeat_minute = ny.minute
                        log(f"Heartbeat — watching: {watchlist}")
                        await tg(session, f"💓 Scanning: {' | '.join(watchlist)}")

                # ── Pre-market Stocktwits 8:00 AM ─────────
                if ny.hour == 8 and ny.minute == 0 and ny.weekday() < 5:
                    try:
                        await stocktwits_premarket_scan(session)
                    except Exception as e:
                        log_err("premarket stocktwits", e)

                # ── After-market Stocktwits 4:30 PM ───────
                if ny.hour == 16 and ny.minute == 30 and ny.weekday() < 5:
                    try:
                        await stocktwits_premarket_scan(session)
                    except Exception as e:
                        log_err("aftermarket stocktwits", e)

                # ── Daily scan 9:20 AM ────────────────────
                if (ny.hour == 9 and ny.minute == 20
                        and ny.weekday() < 5
                        and ny.day != last_scan_day):
                    last_scan_day = ny.day
                    try:
                        new_wl = await smart_daily_scan(session)
                        watchlist.clear()
                        watchlist.extend(new_wl)
                    except Exception as e:
                        log_err("daily scan", e)

                # ── Morning brief 9:25 AM ─────────────────
                if ny.hour == 9 and ny.minute == 25 and not morning_brief_sent and ny.weekday() < 5:
                    morning_brief_sent = True
                    try:
                        await morning_brief(session)
                    except Exception as e:
                        log_err("morning brief", e)
                if ny.hour == 9 and ny.minute == 26:
                    morning_brief_sent = False

                # ── EOD close 3:45 PM ─────────────────────
                if (ny.hour == CLOSE_ALL_TIME[0]
                        and ny.minute == CLOSE_ALL_TIME[1]
                        and not positions_closed_today):
                    try:
                        pos = await get_open_positions(session)
                        if pos:
                            await close_all_positions(session)
                            await tg(session,
                                "⏰ 3:45 PM — Closing all positions!\n"
                                "Tickers: " + ", ".join(pos.keys()))
                        positions_closed_today = True
                        active_trades.clear()
                        trailing_stops.clear()
                    except Exception as e:
                        log_err("EOD close", e)

                # ── Daily report 4:05 PM ──────────────────
                if ny.hour == 16 and ny.minute == 5 and ny.day != last_report_day:
                    last_report_day = ny.day
                    try:
                        await daily_report(session)
                    except Exception as e:
                        log_err("daily report", e)

                # ── Weekly report Sunday 8 PM ─────────────
                if ny.weekday() == 6 and ny.hour == 20 and ny.minute == 0 and not weekly_report_sent:
                    weekly_report_sent = True
                    try:
                        await weekly_report(session)
                    except Exception as e:
                        log_err("weekly report", e)
                if ny.weekday() == 0:
                    weekly_report_sent = False

                # ── Skip if closed / paused ───────────────
                if status == "CLOSED":
                    log("Market closed — sleeping 5 min")
                    await asyncio.sleep(300)
                    continue
                if bot_paused:
                    await asyncio.sleep(30)
                    continue

                # ── Trailing stops & alerts ───────────────
                try:
                    if active_trades:
                        await check_trailing(session)
                    if price_alerts:
                        await check_alerts(session)
                except Exception as e:
                    log_err("trailing/alerts", e)

                log(f"Scanning {watchlist} | {status} | {get_session_name()}")

                # ── Per-ticker signal loop ─────────────────
                for ticker in list(watchlist):
                    try:
                        can, reason = check_daily_limits()
                        if not can:
                            log(f"BLOCKED: {reason}")
                            break

                        (bars_1m, bars_5m, bars_15m,
                         bars_daily, bars_yest) = await asyncio.gather(
                            get_bars(session, ticker, "1Min",  30),
                            get_bars(session, ticker, "5Min",  50),
                            get_bars(session, ticker, "15Min", 50),
                            get_bars(session, ticker, "1Day",  5),
                            get_bars(session, ticker, "1Day",  3),
                        )

                        if len(bars_5m) < 20:
                            log(f"  {ticker}: not enough 5m bars")
                            continue

                        update_orb(ticker, bars_1m)
                        result = compute_signal(bars_1m, bars_5m, bars_15m)

                        if result is None:
                            log(f"  {ticker}: no signal")
                            no_signal_count[ticker] = no_signal_count.get(ticker, 0) + 1
                            continue

                        score = result["buy_score"] if result["signal"] == "BUY" else result["sell_score"]
                        log(f"  {ticker} {result['signal']} Score:{score} RSI:{result['rsi']} Zone:{result['zone']}")
                        no_signal_count[ticker] = 0

                        if is_duplicate(ticker, result["signal"]):
                            log(f"  {ticker}: duplicate signal — skipping")
                            continue

                        # Early exit: skip SELL signals on assets that can't be shorted.
                        # Done here (before AI/news/Stocktwits) to avoid wasted work and spam.
                        if result["signal"] == "SELL":
                            if not ALLOW_SHORT:
                                continue
                            if not await is_shortable(session, ticker):
                                log(f"  {ticker}: not shortable — skipping SELL early")
                                # Treat as a no-signal so stale-ticker replacement kicks in
                                no_signal_count[ticker] = no_signal_count.get(ticker, 0) + 1
                                # Suppress repeat SELL alerts via the normal cooldown
                                last_signal_time[ticker] = datetime.now(timezone.utc)
                                last_signal_type[ticker] = "SELL"
                                continue

                        tf_agrees = check_3tf(bars_1m, bars_5m, bars_15m, result["signal"])
                        if REQUIRE_3TF_AGREE and tf_agrees < 2:
                            log(f"  {ticker}: only {tf_agrees}/3 TF agree")
                            continue

                        spy_trend, spy_chg, _ = await get_spy_trend(session)
                        if SPY_FILTER and result["signal"] == "BUY" and spy_trend == "BEAR":
                            log(f"  {ticker}: BUY blocked by SPY filter")
                            continue

                        rvol     = calc_rvol(bars_1m, bars_yest)
                        multiday = get_multiday(bars_daily)
                        orb_tag  = check_orb(ticker, result["entry"]) or ""
                        patterns = detect_patterns(bars_5m)

                        sentiment, headlines = await get_news(session, ticker)
                        if sentiment == "NEGATIVE" and result["signal"] == "BUY":
                            log(f"  {ticker}: negative news — skip BUY")
                            continue

                        st_data = await get_stocktwits(session, ticker)
                        if (st_data["sentiment"] == "BEARISH"
                                and st_data["bear_pct"] >= 70
                                and result["signal"] == "BUY"):
                            log(f"  {ticker}: Stocktwits bearish — skip BUY")
                            continue

                        # RSI guards
                        rsi_val = result.get("rsi")
                        if result["signal"] == "BUY"  and rsi_val and rsi_val > 70:
                            log(f"  {ticker}: RSI overbought {rsi_val}")
                            continue
                        if result["signal"] == "SELL" and rsi_val and rsi_val < 30:
                            log(f"  {ticker}: RSI oversold {rsi_val}")
                            continue

                        # Zone + RSI guard
                        if (result["signal"] == "BUY"
                                and result.get("zone") == "PREMIUM"
                                and (rsi_val or 0) > 65):
                            log(f"  {ticker}: PREMIUM zone + high RSI")
                            continue

                        # Candlestick pattern guards
                        bearish_pats = {"Bearish Engulfing","Bearish Marubozu","Shooting Star","Evening Star"}
                        bullish_pats = {"Bullish Engulfing","Bullish Marubozu","Hammer","Morning Star"}
                        if result["signal"] == "BUY"  and any(p in patterns for p in bearish_pats):
                            log(f"  {ticker}: bearish candle pattern — skip BUY")
                            continue
                        if result["signal"] == "SELL" and any(p in patterns for p in bullish_pats):
                            log(f"  {ticker}: bullish candle pattern — skip SELL")
                            continue

                        ai = await ai_confirm(session, ticker, result, patterns, sentiment, tf_agrees)
                        if ai.get("verdict") in ["REJECTED", "CAUTION"]:
                            log(f"  {ticker}: AI {ai.get('verdict')} — {ai.get('reason','')}")
                            continue

                        # Get live price and recalculate SL/TP
                        yp, price_source = await get_realtime_price(session, ticker)
                        _, yc, _         = await yahoo_price(session, ticker)

                        if not yp or yp <= 0:
                            log(f"  {ticker}: no live price")
                            continue

                        result["entry"] = yp
                        atr_val = result.get("atr", 0)
                        if atr_val and atr_val > 0:
                            if result["signal"] == "BUY":
                                result["sl"] = round(yp - atr_val * 1.5, 4)
                                result["tp"] = round(yp + atr_val * 3.0, 4)
                            else:
                                result["sl"] = round(yp + atr_val * 1.5, 4)
                                result["tp"] = round(yp - atr_val * 3.0, 4)
                        risk   = abs(result["entry"] - result["sl"])
                        reward = abs(result["tp"] - result["entry"])
                        result["rr"] = "1:" + str(round(reward / risk, 1)) if risk > 0 else "N/A"
                        log(f"  Live entry: ${yp} [{price_source}]  SL:${result['sl']}  TP:${result['tp']}")

                        # Finnhub accuracy confirmation — always sync entry+SL+TP
                        # to ONE consistent price so the bracket can't desync.
                        fh_price, _, _, _ = await get_finnhub_price(session, ticker)
                        if fh_price:
                            dev = abs(fh_price - result["entry"]) / result["entry"] * 100
                            log(f"  Finnhub confirm: ${fh_price} dev:{round(dev,1)}%")
                            # Use Finnhub as the single source of truth for entry,
                            # then derive SL/TP from that same number.
                            result["entry"] = fh_price
                            if result["atr"]:
                                if result["signal"] == "BUY":
                                    result["sl"] = round(fh_price - result["atr"] * 1.5, 4)
                                    result["tp"] = round(fh_price + result["atr"] * 3.0, 4)
                                else:
                                    result["sl"] = round(fh_price + result["atr"] * 1.5, 4)
                                    result["tp"] = round(fh_price - result["atr"] * 3.0, 4)
                            # If price moved a lot since the signal, log it loudly
                            if dev > 1.5:
                                log(f"  {ticker}: price moved {round(dev,1)}% since signal — re-anchored to ${fh_price}")

                        acct = await get_account_info(session)
                        equity = acct["equity"] if acct else ACCOUNT_SIZE
                        shares, cost, leverage = calc_position_size(
                            result["entry"], result["sl"], result["confidence"], equity)

                        msg = build_msg(
                            ticker, result, yp, yc, status, sentiment, headlines,
                            patterns, ai, rvol, multiday, spy_trend, spy_chg,
                            tf_agrees, orb_tag, shares, cost, leverage, st_data, price_source)
                        await tg(session, msg)

                        # Place auto trade
                        if AUTO_TRADE and shares > 0:
                            try:
                                place_ok = True

                                # 1. Only place bracket orders during regular hours
                                hours_ok, hours_reason = can_place_bracket_order()
                                if not hours_ok:
                                    log(f"  {ticker}: order skipped — {hours_reason}")
                                    await tg(session, f"⏸ Signal only (no order): {hours_reason}")
                                    place_ok = False

                                # 2. Re-anchor to the freshest price right before
                                #    placing, then validate. This prevents Alpaca
                                #    rejecting the bracket because price moved since
                                #    the signal was computed.
                                if place_ok:
                                    fresh_price, _ = await get_realtime_price(session, ticker)
                                    if fresh_price and fresh_price > 0:
                                        result["entry"] = round(fresh_price, 2)
                                        if result["atr"]:
                                            if result["signal"] == "BUY":
                                                result["sl"] = round(fresh_price - result["atr"] * 1.5, 4)
                                                result["tp"] = round(fresh_price + result["atr"] * 3.0, 4)
                                            else:
                                                result["sl"] = round(fresh_price + result["atr"] * 1.5, 4)
                                                result["tp"] = round(fresh_price - result["atr"] * 3.0, 4)

                                    valid, fixed_sl, fixed_tp, vreason = validate_bracket(
                                        result["signal"], result["entry"], result["sl"], result["tp"])
                                    if not valid:
                                        log(f"  {ticker}: bracket invalid — {vreason}")
                                        await tg(session, f"❌ Order skipped: bad SL/TP ({vreason})")
                                        place_ok = False
                                    else:
                                        result["sl"], result["tp"] = fixed_sl, fixed_tp

                                # 3. For SHORT orders, verify the asset is shortable
                                if place_ok and result["signal"] == "SELL":
                                    if not await is_shortable(session, ticker):
                                        log(f"  {ticker}: not shortable — skipping SELL")
                                        await tg(session, f"⚠️ {ticker} cannot be sold short — signal only, no order")
                                        place_ok = False

                                if place_ok:
                                    pos = await get_open_positions(session)
                                    if len(pos) >= MAX_OPEN_TRADES:
                                        await tg(session, f"⚠️ Max {MAX_OPEN_TRADES} positions — skipping")
                                    elif ticker in pos:
                                        await tg(session, f"⚠️ Already in {ticker} — skipping")
                                    else:
                                        side  = "buy" if result["signal"] == "BUY" else "sell"
                                        order = await place_order(session, ticker, side, shares,
                                                                  result["sl"], result["tp"])
                                        if order["success"]:
                                            trade_type = "LONG" if side == "buy" else "SHORT"
                                            await tg(session,
                                                f"✅ AUTO-TRADE PLACED [PAPER] - {trade_type}\n\n"
                                                f"{'📈 BUY' if side=='buy' else '📉 SELL SHORT'} {ticker}\n"
                                                f"Qty: {shares} shares\n"
                                                f"Entry: ${result['entry']}\n"
                                                f"SL: ${result['sl']}\n"
                                                f"TP: ${result['tp']}\n"
                                                f"R/R: {result['rr']}\n"
                                                f"Leverage: 1:{leverage}\n"
                                                f"Cost: ${cost}\n"
                                                f"Account: ${equity}")
                                        else:
                                            await tg(session, f"❌ Auto-trade FAILED: {str(order.get('error',''))[:120]}")
                            except Exception as e:
                                log_err("auto-trade", e)
                                await tg(session, f"Auto-trade error: {str(e)[:100]}")

                        active_trades[ticker] = {
                            "signal": result["signal"], "entry": result["entry"],
                            "sl": result["sl"],         "tp":    result["tp"],
                            "atr": result["atr"],
                        }
                        trailing_stops[ticker]  = result["sl"]
                        last_signal_time[ticker] = datetime.now(timezone.utc)
                        last_signal_type[ticker] = result["signal"]
                        trades_today += 1
                        await asyncio.sleep(2)

                    except Exception as e:
                        log_err(f"signal loop {ticker}", e)
                        continue

                # ── Replace stale tickers ──────────────────
                if status == "OPEN":
                    for ticker in list(watchlist):
                        if no_signal_count.get(ticker, 0) >= 2:
                            log(f"Replacing stale ticker: {ticker}")
                            watchlist.remove(ticker)
                            asyncio.ensure_future(replace_ticker(ticker))
                            no_signal_count[ticker] = 0

                # ── Respect scan interval ──────────────────
                elapsed = time.monotonic() - loop_start
                sleep   = max(1.0, SCAN_INTERVAL - elapsed)
                await asyncio.sleep(sleep)

            except Exception as e:
                log_err("main loop", e)
                await asyncio.sleep(15)

# ══════════════════════════════════════════════════════════
#  CRYPTO MODULE  (separate loop, 24/7, bot-managed SL/TP)
# ══════════════════════════════════════════════════════════
# Crypto state
crypto_active_trades   = {}   # symbol -> {entry, sl, tp, qty, atr, time}
crypto_last_signal     = {}   # symbol -> datetime of last signal
crypto_trades_today    = 0
crypto_daily_pnl       = 0.0

def _crypto_enc(symbol: str) -> str:
    """URL-encode the slash in crypto symbols, e.g. BTC/USD -> BTC%2FUSD."""
    return symbol.replace("/", "%2F")

async def get_crypto_bars(session: aiohttp.ClientSession, symbol: str,
                          tf: str = "5Min", limit: int = 50):
    """Fetch crypto bars. Crypto data is FREE on Alpaca (no SIP needed)."""
    try:
        async with session.get(
            f"{CRYPTO_DATA_BASE}/bars",
            headers=ALPACA_LIVE_HEADERS,
            params={"symbols": symbol, "timeframe": tf, "limit": limit},
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            data = await r.json()
            bars = (data.get("bars") or {}).get(symbol, [])
            # Normalise to the same key shape the stock engine expects
            out = []
            for b in bars:
                out.append({"o": round(b["o"], 6), "h": round(b["h"], 6),
                            "l": round(b["l"], 6), "c": round(b["c"], 6),
                            "v": b.get("v", 0), "vw": b.get("vw")})
            return out
    except Exception as e:
        log(f"get_crypto_bars {symbol}: {e}")
        return []

async def get_crypto_price(session: aiohttp.ClientSession, symbol: str):
    """Latest crypto trade price (free feed)."""
    try:
        async with session.get(
            f"{CRYPTO_DATA_BASE}/latest/trades",
            headers=ALPACA_LIVE_HEADERS,
            params={"symbols": symbol},
            timeout=aiohttp.ClientTimeout(total=6)
        ) as r:
            data = await r.json()
            t = (data.get("trades") or {}).get(symbol, {})
            p = t.get("p")
            return round(p, 6) if p else None
    except Exception as e:
        log(f"get_crypto_price {symbol}: {e}")
        return None

async def place_crypto_order(session: aiohttp.ClientSession,
                             symbol: str, qty: float):
    """
    Place a simple market BUY for crypto (long-only, no brackets).
    SL/TP are managed by monitor_crypto_positions().
    """
    try:
        order = {"symbol": symbol, "qty": str(qty), "side": "buy",
                 "type": "market", "time_in_force": "gtc"}
        async with session.post(
            f"{ALPACA_TRADE_BASE}/v2/orders",
            headers=ALPACA_TRADE_HEADERS, json=order,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            data = await r.json()
            if data.get("id"):
                return {"success": True, "order_id": data["id"]}
            return {"success": False, "error": str(data)}
    except Exception as e:
        log_err("place_crypto_order", e)
        return {"success": False, "error": str(e)}

async def close_crypto_position(session: aiohttp.ClientSession, symbol: str):
    """Close a crypto position by selling. Symbol slash must be encoded."""
    try:
        async with session.delete(
            f"{ALPACA_TRADE_BASE}/v2/positions/{_crypto_enc(symbol)}",
            headers=ALPACA_TRADE_HEADERS,
            timeout=aiohttp.ClientTimeout(total=8)
        ) as r:
            return await r.json()
    except Exception as e:
        log_err(f"close_crypto_position {symbol}", e)
        return None

def calc_crypto_size(entry: float, sl: float, equity: float, cash: float):
    """
    Risk-based fractional sizing for crypto (long-only, no leverage).
    CRITICAL: crypto cannot be bought on margin — it must be sized against
    real USD cash, not buying power/equity. We cap by the smaller of:
      - risk-based size (CRYPTO_RISK_PCT of equity)
      - 20% of equity
      - 90% of available cash (leave a buffer for fees/slippage)
    """
    rps = abs(entry - sl)
    if rps <= 0 or cash <= 0:
        return 0.0

    risk_amt = equity * (CRYPTO_RISK_PCT / 100)
    qty = risk_amt / rps

    # Cap by 20% of equity
    max_cost_equity = equity * 0.20
    # Cap by available cash (crypto needs real USD), 90% to leave a buffer
    max_cost_cash = cash * 0.90
    max_cost = min(max_cost_equity, max_cost_cash)

    if qty * entry > max_cost:
        qty = max_cost / entry

    # If we can't afford even a tiny position, return 0
    if qty * entry < 1:
        return 0.0

    return round(qty, 6)

async def monitor_crypto_positions(session: aiohttp.ClientSession):
    """
    Bot-managed SL/TP for crypto, since Alpaca holds no bracket.
    Also trails the stop upward as price rises.
    """
    global crypto_daily_pnl
    for symbol in list(crypto_active_trades.keys()):
        try:
            trade = crypto_active_trades[symbol]
            price = await get_crypto_price(session, symbol)
            if not price:
                continue
            entry = trade["entry"]
            atr   = trade.get("atr", abs(entry - trade["sl"]))

            # Trail the stop up (long-only)
            new_sl = round(price - atr * CRYPTO_SL_ATR, 6)
            if new_sl > trade["sl"]:
                trade["sl"] = new_sl
                log(f"  CRYPTO trail {symbol}: SL -> ${new_sl}")

            hit_sl = price <= trade["sl"]
            hit_tp = price >= trade["tp"]
            if hit_sl or hit_tp:
                await close_crypto_position(session, symbol)
                pnl = round((price - entry) / entry * 100, 2)
                crypto_daily_pnl += pnl
                sign = "+" if pnl >= 0 else ""
                reason = "TP hit ✅" if hit_tp else "SL hit ❌"
                await tg(session,
                    f"{'✅ PROFIT' if pnl > 0 else '❌ LOSS'} - {symbol} (crypto)\n"
                    f"{reason}\nExit: ${price}\nP&L: {sign}{pnl}%")
                all_time_log.append({"ticker": symbol, "signal": "BUY",
                                     "entry": entry, "exit": price, "pnl": pnl,
                                     "date": get_ny().strftime("%Y-%m-%d")})
                del crypto_active_trades[symbol]
        except Exception as e:
            log_err(f"monitor_crypto {symbol}", e)

def crypto_is_duplicate(symbol: str) -> bool:
    last = crypto_last_signal.get(symbol)
    if not last:
        return False
    return (datetime.now(timezone.utc) - last).total_seconds() < SIGNAL_COOLDOWN

async def crypto_loop():
    """
    Independent 24/7 crypto trading loop. Long-only, bot-managed SL/TP.
    Reuses the same SMC signal engine as stocks.
    """
    global crypto_trades_today
    if not CRYPTO_ENABLED:
        return

    log("Crypto loop starting (24/7)...")
    await asyncio.sleep(20)  # let stock startup settle first

    async with aiohttp.ClientSession() as session:
        await tg(session,
            "₿ Crypto module ONLINE (paper)\n"
            f"Watching: {', '.join(CRYPTO_UNIVERSE)}\n"
            f"Risk/trade: {CRYPTO_RISK_PCT}% | Max open: {CRYPTO_MAX_OPEN}\n"
            "Long-only, 24/7, bot-managed SL/TP")

        while True:
            loop_start = time.monotonic()
            try:
                if bot_paused:
                    await asyncio.sleep(30)
                    continue

                # Manage existing positions first
                if crypto_active_trades:
                    await monitor_crypto_positions(session)

                # Daily reset at NY midnight
                ny = get_ny()
                if ny.hour == 0 and ny.minute == 0:
                    crypto_trades_today = 0

                for symbol in CRYPTO_UNIVERSE:
                    try:
                        if symbol in crypto_active_trades:
                            continue
                        if len(crypto_active_trades) >= CRYPTO_MAX_OPEN:
                            break
                        if crypto_is_duplicate(symbol):
                            continue

                        bars_1m, bars_5m, bars_15m = await asyncio.gather(
                            get_crypto_bars(session, symbol, "1Min",  30),
                            get_crypto_bars(session, symbol, "5Min",  50),
                            get_crypto_bars(session, symbol, "15Min", 50),
                        )
                        if len(bars_5m) < 20:
                            continue

                        result = compute_signal(bars_1m, bars_5m, bars_15m)
                        if result is None:
                            continue

                        # Crypto is long-only on Alpaca — ignore SELL signals
                        if result["signal"] != "BUY":
                            continue

                        score = result["buy_score"]
                        if score < CRYPTO_MIN_SCORE:
                            continue

                        # Require multi-timeframe agreement like stocks
                        tf_agrees = check_3tf(bars_1m, bars_5m, bars_15m, "BUY")
                        if tf_agrees < 2:
                            continue

                        # RSI overbought guard
                        if result.get("rsi") and result["rsi"] > 72:
                            log(f"  CRYPTO {symbol}: RSI overbought {result['rsi']}")
                            continue

                        # Don't buy into a PREMIUM zone with elevated RSI
                        # (that's buying the high, not a discount entry)
                        if result.get("zone") == "PREMIUM" and (result.get("rsi") or 0) > 55:
                            log(f"  CRYPTO {symbol}: PREMIUM zone + RSI {result.get('rsi')} — skip (buying high)")
                            continue

                        # Fresh price + SL/TP from ATR
                        price = await get_crypto_price(session, symbol)
                        if not price:
                            continue
                        atr = result.get("atr") or (price * 0.01)
                        entry = round(price, 6)
                        sl = round(entry - atr * CRYPTO_SL_ATR, 6)
                        tp = round(entry + atr * CRYPTO_TP_ATR, 6)

                        acct = await get_account_info(session)
                        equity = acct["equity"] if acct else ACCOUNT_SIZE
                        cash   = acct["cash"]   if acct else ACCOUNT_SIZE
                        qty = calc_crypto_size(entry, sl, equity, cash)
                        if qty <= 0:
                            log(f"  CRYPTO {symbol}: can't afford position (cash ${cash})")
                            continue

                        rr = round(abs(tp - entry) / abs(entry - sl), 1) if entry != sl else 0
                        await tg(session,
                            f"₿ CRYPTO BUY - {symbol}\n\n"
                            f"Score: {score}/20  3TF: {tf_agrees}/3\n"
                            f"RSI: {result['rsi']}  Zone: {result.get('zone')}\n\n"
                            f"Entry: ${entry}\nSL: ${sl}\nTP: ${tp}\nR/R: 1:{rr}\n"
                            f"Qty: {qty}  (~${round(qty*entry,2)})")

                        order = await place_crypto_order(session, symbol, qty)
                        if order["success"]:
                            crypto_active_trades[symbol] = {
                                "entry": entry, "sl": sl, "tp": tp,
                                "qty": qty, "atr": atr,
                                "time": get_ny().strftime("%H:%M")}
                            crypto_last_signal[symbol] = datetime.now(timezone.utc)
                            crypto_trades_today += 1
                            await tg(session,
                                f"✅ CRYPTO ORDER PLACED [PAPER]\n"
                                f"{symbol} {qty} @ ${entry}\n"
                                f"Bot will manage SL ${sl} / TP ${tp}")
                        else:
                            await tg(session, f"❌ Crypto order failed: {str(order.get('error',''))[:120]}")

                        await asyncio.sleep(2)
                    except Exception as e:
                        log_err(f"crypto signal {symbol}", e)
                        continue

                elapsed = time.monotonic() - loop_start
                await asyncio.sleep(max(5.0, CRYPTO_SCAN_INTERVAL - elapsed))

            except Exception as e:
                log_err("crypto_loop", e)
                await asyncio.sleep(20)

# ══════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════
async def run_all():
    await asyncio.gather(main(), alpaca_price_poller(), crypto_loop())

if __name__ == "__main__":
    asyncio.run(run_all())
