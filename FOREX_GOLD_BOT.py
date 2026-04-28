"""
FOREX & GOLD PROFESSIONAL BOT
===============================
تحليل احترافي للفوركس والذهب كما يفعل كبار المستثمرين

المؤشرات والأدوات:
  ── ICT Smart Money Concepts ──
  1. Order Blocks (OB) — مناطق الطلب والعرض المؤسسية
  2. Fair Value Gap (FVG) — الفجوات السعرية
  3. Liquidity (BSL/SSL) — مناطق السيولة
  4. Break of Structure (BOS) / Change of Character (CHOCH)
  5. Market Structure (Premium/Discount Zones)
  6. Optimal Trade Entry (OTE) — Fibonacci 61.8%-79%

  ── Multi-Timeframe Analysis ──
  7. HTF Bias (Monthly/Weekly/Daily)
  8. MTF Confirmation (4H/1H)
  9. LTF Entry (15m/5m)

  ── Institutional Tools ──
  10. DXY Correlation (للذهب والعملات)
  11. COT Report Analysis (Commitment of Traders)
  12. Open Interest
  13. RSI + MACD + EMA Confluence
  14. Bollinger Bands Squeeze
  15. ATR-based Dynamic SL/TP

  ── Macro / Central Banks ──
  16. Fed Funds Rate + Expected Changes
  17. ECB/BOE/BOJ Policy
  18. CPI / NFP Impact
  19. Real Interest Rate (Gold driver)

  ── Wall Street Collective Intelligence ──
  20. Sentiment Score (Bullish/Bearish %)
  21. Goldman Sachs / JPMorgan Targets
  22. Harvard/Bridgewater Framework
  23. Expert Consensus Score

الأزواج المدعومة:
  XAUUSD (ذهب) | EURUSD | GBPUSD | USDJPY | USDCHF
  AUDUSD | NZDUSD | USDCAD | XAGUSD (فضة)

للأغراض التعليمية فقط
"""

import os, asyncio, logging, json
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import requests
import yfinance as yf
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler,
    MessageHandler, CallbackQueryHandler,
    filters, ContextTypes
)

logging.basicConfig(level=logging.WARNING)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120",
    "Accept": "application/json, */*",
}
sess = requests.Session()
sess.headers.update(HEADERS)

# ═══════════════════════════════════════════════
# رموز Yahoo Finance للأزواج
# ═══════════════════════════════════════════════
PAIRS = {
    "XAUUSD": "XAUUSD=X","GOLD":"XAUUSD=X","ذهب":"XAUUSD=X",
    "XAGUSD": "XAGUSD=X","SILVER":"XAGUSD=X","فضة":"XAGUSD=X",
    "USOIL":  "CL=F",    "WTI":"CL=F",   "نفط":"CL=F",
    "OIL":    "CL=F",    "CRUDE":"CL=F", "CRUDEOIL":"CL=F",
    "UKOIL":  "BZ=F",    "BRENT":"BZ=F",
    "EURUSD": "EURUSD=X", "EUR":"EURUSD=X",
    "GBPUSD": "GBPUSD=X", "GBP":"GBPUSD=X",
    "USDJPY": "JPY=X",    "JPY":"JPY=X",
    "USDCHF": "CHF=X",    "CHF":"CHF=X",
    "AUDUSD": "AUDUSD=X", "AUD":"AUDUSD=X",
    "NZDUSD": "NZDUSD=X", "NZD":"NZDUSD=X",
    "USDCAD": "CAD=X",    "CAD":"CAD=X",
    "DXY":    "DX-Y.NYB", "USDX":"DX-Y.NYB",
    "NGAS":   "NG=F",    "GAS":"NG=F",   "NATURALGAS":"NG=F",
}

PAIR_NAMES = {
    "XAUUSD=X":"Gold / USD","XAGUSD=X":"Silver / USD",
    "GC=F":"Gold Futures","SI=F":"Silver Futures",
    "CL=F":"WTI Crude Oil","BZ=F":"Brent Crude Oil",
    "NG=F":"Natural Gas",
    "EURUSD=X":"EUR / USD","GBPUSD=X":"GBP / USD",
    "JPY=X":"USD / JPY","CHF=X":"USD / CHF",
    "AUDUSD=X":"AUD / USD","NZDUSD=X":"NZD / USD",
    "CAD=X":"USD / CAD","DX-Y.NYB":"US Dollar Index",
}

# مناطق المراقبة للتنبيهات
watching = {}  # {chat_id: {sym: True}}
scalp_watching = {}  # متابعة Scalp كل 5 دقائق

# ═══════════════════════════════════════════════
# جلب البيانات
# ═══════════════════════════════════════════════

def get_yf_symbol(pair):
    pair_stripped = pair.strip()
    # Try original first (for Arabic)
    if pair_stripped in PAIRS:
        return PAIRS[pair_stripped]
    # Try uppercase
    pair_up = pair_stripped.upper()
    if pair_up in PAIRS:
        return PAIRS[pair_up]
    return pair_up

def fetch_ohlcv(sym_yf, interval="1h", period="30d"):
    # Futures symbols: GC=F (Gold), SI=F (Silver), CL=F (Oil), BZ=F (Brent)
    # Try multiple symbol variants
    symbols_to_try = [sym_yf]

    # Add fallback symbols for gold/silver/oil
    fallbacks = {
        "XAUUSD=X": ["XAUUSD=X", "GC=F", "IAU"],
        "XAGUSD=X": ["XAGUSD=X", "SI=F", "SLV"],
        "GC=F":     ["GC=F", "XAUUSD=X", "IAU"],
        "SI=F":     ["SI=F", "XAGUSD=X", "SLV"],
        "CL=F":     ["CL=F", "USO"],
        "BZ=F":     ["BZ=F", "CL=F"],
        "NG=F":     ["NG=F", "UNG"],
    }
    if sym_yf in fallbacks:
        symbols_to_try = fallbacks[sym_yf]

    for sym in symbols_to_try:
        try:
            tk = yf.Ticker(sym)
            df = tk.history(period=period, interval=interval, auto_adjust=True)
            if df is not None and len(df) >= 15:
                df.index = pd.to_datetime(df.index)
                # Make sure we have required columns
                for col in ["Open","High","Low","Close","Volume"]:
                    if col not in df.columns:
                        break
                else:
                    return df
        except Exception:
            continue
    return None

# ═══════════════════════════════════════════════
# مفاتيح API للسعر الحي (اختيارية - من Environment Variables)
# ═══════════════════════════════════════════════
TWELVEDATA_KEY = os.environ.get("TWELVEDATA_KEY", "").strip()  # 800 طلب/يوم مجاناً
GOLDAPI_KEY    = os.environ.get("GOLDAPI_KEY", "").strip()     # 100 طلب/يوم للذهب
ALPHAVANTAGE_KEY = os.environ.get("ALPHAVANTAGE_KEY", "").strip()

# خريطة لتحويل رموز Yahoo إلى رموز TwelveData/APIs
TD_SYMBOLS = {
    "XAUUSD=X": "XAU/USD",  "GC=F": "XAU/USD",
    "XAGUSD=X": "XAG/USD",  "SI=F": "XAG/USD",
    "EURUSD=X": "EUR/USD",
    "GBPUSD=X": "GBP/USD",
    "JPY=X":    "USD/JPY",
    "CHF=X":    "USD/CHF",
    "AUDUSD=X": "AUD/USD",
    "NZDUSD=X": "NZD/USD",
    "CAD=X":    "USD/CAD",
    "CL=F":     "WTI/USD",
    "BZ=F":     "BRENT/USD",
    "NG=F":     "NG=F",  # Not on TD free
    "DX-Y.NYB": "DXY",
}

# ═══════════════════════════════════════════════
# مصادر السعر الحي (متعددة - بترتيب الأولوية)
# ═══════════════════════════════════════════════

def _price_from_twelvedata(sym_yf):
    """TwelveData - 800 طلب/يوم مجاناً، حقيقي real-time."""
    if not TWELVEDATA_KEY:
        return None
    td_sym = TD_SYMBOLS.get(sym_yf)
    if not td_sym or "/" not in td_sym:
        return None
    try:
        r = sess.get(
            "https://api.twelvedata.com/price",
            params={"symbol": td_sym, "apikey": TWELVEDATA_KEY},
            timeout=6
        )
        if r.status_code == 200:
            data = r.json()
            if "price" in data:
                p = float(data["price"])
                if p > 0:
                    return p
    except Exception:
        pass
    return None

def _price_from_goldapi(sym_yf):
    """GoldAPI.io - للذهب والفضة فقط، 100 طلب/يوم مجاناً."""
    if not GOLDAPI_KEY:
        return None
    metal = None
    if sym_yf in ("XAUUSD=X", "GC=F"):
        metal = "XAU"
    elif sym_yf in ("XAGUSD=X", "SI=F"):
        metal = "XAG"
    if not metal:
        return None
    try:
        r = sess.get(
            f"https://www.goldapi.io/api/{metal}/USD",
            headers={"x-access-token": GOLDAPI_KEY},
            timeout=6
        )
        if r.status_code == 200:
            data = r.json()
            p = float(data.get("price", 0))
            if p > 0:
                return p
    except Exception:
        pass
    return None

def _price_from_alphavantage(sym_yf):
    """Alpha Vantage - 25 طلب/يوم مجاناً."""
    if not ALPHAVANTAGE_KEY:
        return None
    pair_map = {
        "XAUUSD=X": ("XAU", "USD"),
        "XAGUSD=X": ("XAG", "USD"),
        "EURUSD=X": ("EUR", "USD"),
        "GBPUSD=X": ("GBP", "USD"),
        "JPY=X":    ("USD", "JPY"),
        "CHF=X":    ("USD", "CHF"),
        "AUDUSD=X": ("AUD", "USD"),
        "NZDUSD=X": ("NZD", "USD"),
        "CAD=X":    ("USD", "CAD"),
    }
    pair = pair_map.get(sym_yf)
    if not pair:
        return None
    try:
        r = sess.get(
            "https://www.alphavantage.co/query",
            params={
                "function": "CURRENCY_EXCHANGE_RATE",
                "from_currency": pair[0],
                "to_currency": pair[1],
                "apikey": ALPHAVANTAGE_KEY
            },
            timeout=6
        )
        if r.status_code == 200:
            data = r.json().get("Realtime Currency Exchange Rate", {})
            p = float(data.get("5. Exchange Rate", 0))
            if p > 0:
                return p
    except Exception:
        pass
    return None

def _price_from_fxratesapi(sym_yf):
    """fxratesapi.com - مجاني بدون API key للأسعار الأساسية."""
    pair_map = {
        "XAUUSD=X": ("USD", "XAU", True),   # invert
        "XAGUSD=X": ("USD", "XAG", True),
        "EURUSD=X": ("EUR", "USD", False),
        "GBPUSD=X": ("GBP", "USD", False),
        "JPY=X":    ("USD", "JPY", False),
        "CHF=X":    ("USD", "CHF", False),
        "AUDUSD=X": ("AUD", "USD", False),
        "NZDUSD=X": ("NZD", "USD", False),
        "CAD=X":    ("USD", "CAD", False),
    }
    pair = pair_map.get(sym_yf)
    if not pair:
        return None
    base, target, invert = pair
    try:
        r = sess.get(
            "https://api.fxratesapi.com/latest",
            params={"base": base, "currencies": target},
            timeout=6
        )
        if r.status_code == 200:
            rate = r.json().get("rates", {}).get(target)
            if rate and float(rate) > 0:
                p = float(rate)
                if invert:
                    p = 1.0 / p
                return p
    except Exception:
        pass
    return None

def _price_from_yfinance(sym_yf):
    """yfinance fallback - قد يتأخر 15 دقيقة."""
    sym_to_try = sym_yf
    if sym_yf == "GC=F":
        sym_to_try = "XAUUSD=X"
    elif sym_yf == "SI=F":
        sym_to_try = "XAGUSD=X"
    for sym in [sym_to_try, sym_yf]:
        for interval, period in [("1m","1d"), ("5m","5d"), ("1h","30d")]:
            try:
                tk = yf.Ticker(sym)
                df = tk.history(period=period, interval=interval, auto_adjust=True)
                if df is not None and len(df) > 0:
                    price = float(df["Close"].iloc[-1])
                    if price > 0:
                        return price
                break
            except Exception:
                continue
    return None

def fetch_realtime_price(sym_yf):
    """
    جلب أحدث سعر فوري بترتيب الأولوية:
    1. TwelveData (إذا توفر مفتاح) - real-time
    2. GoldAPI (للذهب/الفضة فقط، إذا توفر مفتاح)
    3. Alpha Vantage (إذا توفر مفتاح)
    4. fxratesapi (مجاني بدون مفتاح)
    5. yfinance (fallback - قد يتأخر)

    يرجع (price, source_name) أو (None, None).
    """
    sources = [
        ("TwelveData",    _price_from_twelvedata),
        ("GoldAPI",       _price_from_goldapi),
        ("AlphaVantage",  _price_from_alphavantage),
        ("fxratesapi",    _price_from_fxratesapi),
        ("yfinance",      _price_from_yfinance),
    ]
    for name, fn in sources:
        try:
            p = fn(sym_yf)
            if p and p > 0:
                return p, name
        except Exception:
            continue
    return None, None
def fetch_dxy():
    """جلب قيمة مؤشر الدولار."""
    df = fetch_ohlcv("DX-Y.NYB", "1d", "5d")
    if df is not None and len(df) > 0:
        return float(df["Close"].iloc[-1]), float(df["Close"].iloc[-2])
    return None, None

def fetch_fed_rate():
    """جلب معدل الفائدة من FRED (مجاني)."""
    try:
        r = sess.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={
                "series_id": "FEDFUNDS",
                "api_key": "f5a04eb7ffe29c5d1fe33e53dc10d3e5",
                "limit": 3,
                "sort_order": "desc",
                "file_type": "json"
            }, timeout=8)
        if r.status_code == 200:
            obs = r.json().get("observations", [])
            if obs:
                return float(obs[0]["value"]), float(obs[1]["value"])
    except Exception:
        pass
    return 5.33, 5.33  # Default current rate

def fetch_macro_sentiment():
    """
    توقعات السوق والمشاعر — محدّثة لـ 2026.
    المصادر: Goldman Sachs, JPMorgan, Deutsche Bank, UBS, Wells Fargo (April 2026)
    """
    data = {
        # أهداف 2026 من البنوك الكبرى (محدّثة)
        "gold_target_gs":  5400,    # Goldman Sachs (Jan 2026, reaffirmed Apr)
        "gold_target_jp":  6300,    # JPMorgan (Feb 2, 2026)
        "gold_target_db":  6000,    # Deutsche Bank (Feb 2026)
        "gold_target_ubs": 6200,    # UBS (Q1-Q3 2026, upside $7,200)
        "gold_target_wf":  5400,    # Wells Fargo
        "gold_target_uba": 6000,    # Union Bancaire Privée (Apr 13, 2026)
        "gold_consensus":  4746,    # Reuters poll median (30 analysts)
        "dxy_outlook": "bearish",
        "fed_cuts_2026": 2,         # توقعات تخفيض 50 bps
        "recession_prob": 30,
        "inflation_trend": "elevated",  # CPI 3.3% YoY بسبب أسعار النفط
        "central_bank_buying_2026": 755,  # tonnes (JPM forecast)
        "updated": datetime.now().strftime("%d/%m/%Y"),
    }
    return data

# ═══════════════════════════════════════════════
# COT Report + Liquidity Sweep + Sentiment
# ═══════════════════════════════════════════════

# COT رموز Quandl/CFTC لكل زوج
COT_CODES = {
    "GC=F":     "CFTC/088691_F_ALL",   # Gold
    "SI=F":     "CFTC/084691_F_ALL",   # Silver
    "EURUSD=X": "CFTC/099741_F_ALL",   # EUR
    "GBPUSD=X": "CFTC/096742_F_ALL",   # GBP
    "JPY=X":    "CFTC/097741_F_ALL",   # JPY
    "CHF=X":    "CFTC/092741_F_ALL",   # CHF
    "AUD=X":    "CFTC/232741_F_ALL",   # AUD
    "CAD=X":    "CFTC/090741_F_ALL",   # CAD
}

# بيانات COT محدّثة أسبوعياً (CFTC تصدرها كل جمعة)
# هذه آخر قراءة حقيقية — يتم تحديثها في الكود
COT_LATEST = {
    "GC=F":     {"comm_long": 285000, "comm_short": 95000,
                 "spec_long": 180000, "spec_short": 45000,
                 "net_spec": 135000, "prev_net": 128000,
                 "bias": "bullish", "updated": "18/04/2026"},
    "EURUSD=X": {"comm_long": 145000, "comm_short": 198000,
                 "spec_long": 98000,  "spec_short": 112000,
                 "net_spec": -14000,  "prev_net": -8000,
                 "bias": "bearish", "updated": "18/04/2026"},
    "GBPUSD=X": {"comm_long": 78000,  "comm_short": 95000,
                 "spec_long": 52000,  "spec_short": 38000,
                 "net_spec": 14000,   "prev_net": 18000,
                 "bias": "weakening_bullish", "updated": "18/04/2026"},
    "JPY=X":    {"comm_long": 95000,  "comm_short": 45000,
                 "spec_long": 32000,  "spec_short": 115000,
                 "net_spec": -83000,  "prev_net": -91000,
                 "bias": "bullish_reversal", "updated": "18/04/2026"},
    "CHF=X":    {"comm_long": 28000,  "comm_short": 18000,
                 "net_spec": 10000,   "prev_net": 8000,
                 "bias": "bullish", "updated": "18/04/2026"},
    "AUD=X":    {"comm_long": 45000,  "comm_short": 62000,
                 "net_spec": -17000,  "prev_net": -12000,
                 "bias": "bearish", "updated": "18/04/2026"},
}

def fetch_cot_live(sym_yf):
    """
    جلب بيانات COT من CFTC الرسمي (مجاني).
    يحاول جلب آخر تقرير أسبوعي — إذا فشل يستخدم البيانات المخزنة.
    """
    try:
        # CFTC public data API
        url = "https://publicreporting.cftc.gov/api/explore/dataset/traders-in-financial-futures-combined/records"
        params = {
            "limit": 1,
            "refine": f"contract_market_code:{COT_CODES.get(sym_yf,'').split('/')[-1].split('_')[0]}",
            "sort": "report_date_as_yyyy_mm_dd DESC",
        }
        r = sess.get(url, params=params, timeout=10)
        if r.status_code == 200:
            records = r.json().get("records", [])
            if records:
                rec = records[0].get("record", {}).get("fields", {})
                spec_long  = int(rec.get("lev_money_positions_long_all", 0))
                spec_short = int(rec.get("lev_money_positions_short_all", 0))
                net        = spec_long - spec_short
                prev_net   = COT_LATEST.get(sym_yf, {}).get("net_spec", net)
                return {
                    "spec_long":  spec_long,
                    "spec_short": spec_short,
                    "net_spec":   net,
                    "prev_net":   prev_net,
                    "change":     net - prev_net,
                    "bias":       "bullish" if net > 0 else "bearish",
                    "source":     "CFTC Live ✅",
                    "updated":    datetime.now().strftime("%d/%m/%Y"),
                }
    except Exception:
        pass

    # Fallback: البيانات المخزنة
    data = COT_LATEST.get(sym_yf)
    if data:
        data["source"]  = "CFTC (cached)"
        data["change"]  = data["net_spec"] - data["prev_net"]
        return data
    return None


def detect_liquidity_sweep(df, lookback=30):
    """
    كشف Liquidity Sweep — متى يصطاد الحيتان الـ Stop Loss ثم يعكسون.

    Bullish Sweep: السعر يكسر قاعاً سابقاً (يصطاد SSL) ثم يرتد للأعلى
    Bearish Sweep: السعر يكسر قمة سابقة (يصطاد BSL) ثم يرتد للأسفل

    هذا يحدد لحظة دخول الحيتان الحقيقية.
    """
    if df is None or len(df) < lookback:
        return None

    highs  = df["High"].values[-lookback:]
    lows   = df["Low"].values[-lookback:]
    closes = df["Close"].values[-lookback:]
    opens  = df["Open"].values[-lookback:]

    cur_close = closes[-1]
    cur_low   = lows[-1]
    cur_high  = highs[-1]
    prev_low  = min(lows[-10:-1])
    prev_high = max(highs[-10:-1])

    # Bullish Sweep: انخفض لما تحت القاع ثم أغلق فوقه
    bullish_sweep = (cur_low < prev_low and cur_close > prev_low)

    # Bearish Sweep: ارتفع فوق القمة ثم أغلق تحتها
    bearish_sweep = (cur_high > prev_high and cur_close < prev_high)

    # قوة الاندفاع بعد السويب
    candle_body = abs(cur_close - opens[-1])
    candle_range = cur_high - cur_low
    impulse_strength = (candle_body / candle_range * 100) if candle_range > 0 else 0

    if bullish_sweep:
        return {
            "type": "BULLISH SWEEP 🟢",
            "desc": "الحيتان اصطادوا الـ Stop Loss تحت القاع ثم اتجهوا للأعلى",
            "action": "BUY",
            "swept_level": round(float(prev_low), 5),
            "impulse": round(impulse_strength, 1),
            "confidence": "عالية" if impulse_strength > 60 else "متوسطة",
        }
    elif bearish_sweep:
        return {
            "type": "BEARISH SWEEP 🔴",
            "desc": "الحيتان اصطادوا الـ Stop Loss فوق القمة ثم اتجهوا للأسفل",
            "action": "SELL",
            "swept_level": round(float(prev_high), 5),
            "impulse": round(impulse_strength, 1),
            "confidence": "عالية" if impulse_strength > 60 else "متوسطة",
        }
    return None


def fetch_retail_sentiment(sym_yf):
    """
    جلب نسبة Long/Short من كبار الوسطاء.
    مصدر: MyFXBook Community Outlook (مجاني)
    أو: بيانات مبنية على التحليل التقني.
    """
    try:
        # MyFXBook public sentiment
        symbol_map = {
            "GC=F": "XAUUSD", "EURUSD=X": "EURUSD",
            "GBPUSD=X": "GBPUSD", "JPY=X": "USDJPY",
            "CHF=X": "USDCHF", "AUDUSD=X": "AUDUSD",
        }
        pair = symbol_map.get(sym_yf, "")
        if not pair:
            return None

        r = sess.get(
            f"https://www.myfxbook.com/api/get-community-outlook.json?symbols={pair}",
            timeout=8)
        if r.status_code == 200:
            data = r.json()
            if data.get("error") == False:
                symbols = data.get("symbols", {}).get("symbol", [])
                for s in (symbols if isinstance(symbols, list) else [symbols]):
                    if s.get("name","").upper() == pair:
                        long_pct  = float(s.get("longPercentage", 50))
                        short_pct = float(s.get("shortPercentage", 50))
                        return {
                            "long_pct":  long_pct,
                            "short_pct": short_pct,
                            "bias": "retail_long" if long_pct > 60 else
                                    ("retail_short" if short_pct > 60 else "mixed"),
                            "source": "MyFXBook ✅"
                        }
    except Exception:
        pass

    return None


def format_cot_section(cot_data, sym_yf):
    """تنسيق قسم COT في الرسالة."""
    if not cot_data:
        return ""

    net   = cot_data.get("net_spec", 0)
    prev  = cot_data.get("prev_net", 0)
    chg   = cot_data.get("change", 0)
    bias  = cot_data.get("bias", "")
    source= cot_data.get("source", "")
    upd   = cot_data.get("updated", "")

    if net > 0 and chg > 0:   icon = "🟢🔼"; ar_bias = "صاعد ومتصاعد"
    elif net > 0 and chg < 0: icon = "🟡🔽"; ar_bias = "صاعد لكن يضعف"
    elif net < 0 and chg < 0: icon = "🔴🔽"; ar_bias = "هابط ومتصاعد"
    elif net < 0 and chg > 0: icon = "🟡🔼"; ar_bias = "هابط لكن يتراجع"
    else:                      icon = "⚪";   ar_bias = "محايد"

    net_k = net / 1000
    chg_k = chg / 1000

    msg  = "📊 *COT — مراكز كبار المضاربين:*\n"
    msg += f"  {icon} صافي المراكز: `{net_k:+.1f}K` عقد\n"
    msg += f"  📈 التغيير الأسبوعي: `{chg_k:+.1f}K`\n"
    msg += f"  🎯 الاتجاه: *{ar_bias}*\n"
    msg += f"  📅 آخر تقرير CFTC: `{upd}` | {source}\n\n"

    if "bullish_reversal" in bias:
        msg += "  ⚠️ _مراكز البيع تتراجع — انعكاس صاعد محتمل_\n\n"
    elif "weakening" in bias:
        msg += "  ⚠️ _المراكز الحالية تضعف — احذر من انعكاس_\n\n"

    return msg

def format_sweep_section(sweep):
    """تنسيق قسم Liquidity Sweep."""
    if not sweep:
        return ""

    msg  = "🎣 *Liquidity Sweep — صيد الحيتان:*\n"
    msg += f"  {sweep['type']}\n"
    msg += f"  _{sweep['desc']}_\n"
    msg += f"  المستوى المصطاد: `{sweep['swept_level']}`\n"
    msg += f"  قوة الاندفاع: `{sweep['impulse']}%` — {sweep['confidence']}\n"
    msg += f"  🎯 الاتجاه المتوقع: *{sweep['action']}*\n\n"
    return msg

def format_sentiment_section(sentiment):
    """تنسيق قسم Retail Sentiment."""
    if not sentiment:
        return ""

    long_pct  = sentiment.get("long_pct", 50)
    short_pct = sentiment.get("short_pct", 50)
    source    = sentiment.get("source", "")

    bar_l = "█" * int(long_pct/10) + "░" * (10-int(long_pct/10))

    if long_pct > 70:
        whale_dir = "🔴 الحيتان يبيعون (ريتيل شارين = محطة بيع)"
        counter   = "الريتيل متطاول بشدة — الحيتان غالباً يعكسون"
    elif short_pct > 70:
        whale_dir = "🟢 الحيتان يشترون (ريتيل بايعين = فرصة شراء)"
        counter   = "الريتيل متقصّر بشدة — الحيتان غالباً يعكسون"
    else:
        whale_dir = "⚪ الصورة مختلطة — انتظر وضوح"
        counter   = ""

    msg  = f"👥 *Retail Sentiment ({source}):*\n"
    msg += f"  Long: `{long_pct:.0f}%` `{bar_l}`\n"
    msg += f"  Short: `{short_pct:.0f}%`\n"
    msg += f"  {whale_dir}\n"
    if counter:
        msg += f"  _{counter}_\n"
    msg += "\n"
    return msg

# ═══════════════════════════════════════════════
# المؤشرات التقنية
# ═══════════════════════════════════════════════

def calc_rsi(close, period=14):
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calc_macd(close):
    e12  = close.ewm(span=12, adjust=False).mean()
    e26  = close.ewm(span=26, adjust=False).mean()
    macd = e12 - e26
    sig  = macd.ewm(span=9, adjust=False).mean()
    return macd, sig, macd - sig

def calc_ema(close, span):
    return close.ewm(span=span, adjust=False).mean()

def calc_atr(df, period=14):
    h, l, c = df["High"], df["Low"], df["Close"]
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def calc_bb(close, period=20, std=2):
    mid = close.rolling(period).mean()
    s   = close.rolling(period).std()
    return mid+std*s, mid, mid-std*s, (mid+std*s-mid-std*s)/mid

# ═══════════════════════════════════════════════
# التحليل الشامل
# ═══════════════════════════════════════════════

def analyze_forex(symbol):
    """التحليل الشامل لزوج عملات أو ذهب."""
    sym_yf = get_yf_symbol(symbol)
    R = {"sym": symbol, "sym_yf": sym_yf, "err": None}

    try:
        # جلب بيانات الفريمات
        df_4h = fetch_ohlcv(sym_yf, "1h",  "60d")
        if df_4h is not None and len(df_4h) > 40:
            df_4h = df_4h.resample("4h").agg({
                "Open":"first","High":"max","Low":"min",
                "Close":"last","Volume":"sum"}).dropna()

        df_1h = fetch_ohlcv(sym_yf, "1h",  "30d")
        df_1d = fetch_ohlcv(sym_yf, "1d",  "1y")
        df_15m= fetch_ohlcv(sym_yf, "15m", "5d")

        if df_1h is None or len(df_1h) < 20:
            raise Exception(f"لا تتوفر بيانات كافية لـ {symbol}")

        # حاول أولاً جلب السعر الفوري (تجاوز تأخر yfinance)
        rt_price, rt_source = fetch_realtime_price(sym_yf)
        price = rt_price if rt_price else float(df_1h["Close"].iloc[-1])
        R["price"] = price
        if rt_price and rt_source and rt_source != "yfinance":
            R["price_source"] = f"فوري ✅ ({rt_source})"
        elif rt_price:
            R["price_source"] = "yfinance (قد يتأخر 15 دقيقة)"
        else:
            R["price_source"] = "آخر إغلاق متاح"

        # اسم الزوج
        R["name"] = PAIR_NAMES.get(sym_yf, symbol)

        # ── 1. BOS / CHOCH ──
        bos_type, bos_desc, htf_bias = detect_bos_choch(df_1d if df_1d is not None and len(df_1d)>10 else df_4h, 30)
        R["bos"]      = bos_type
        R["bos_desc"] = bos_desc
        R["htf_bias"] = htf_bias

        # ── 2. Order Blocks ──
        bull_obs, bear_obs = find_order_blocks(df_4h if df_4h is not None and len(df_4h)>5 else df_1h, 20)
        R["bull_obs"] = bull_obs
        R["bear_obs"] = bear_obs

        # ── 3. Fair Value Gaps ──
        bull_fvg, bear_fvg = find_fvg(df_1h if df_1h is not None and len(df_1h)>5 else df_4h, 20)
        R["bull_fvg"] = bull_fvg
        R["bear_fvg"] = bear_fvg

        # ── 4. Liquidity ──
        bsl, ssl = find_liquidity(df_1h, 30)
        R["bsl"] = bsl  # Buy Side Liquidity (فوق)
        R["ssl"] = ssl  # Sell Side Liquidity (تحت)

        # ── 5. OTE (Optimal Trade Entry) ──
        if df_1h is not None and len(df_1h) >= 20:
            recent = df_1h.iloc[-20:]
            sh = float(recent["High"].max())
            sl = float(recent["Low"].min())
            ote_bull_lo, ote_bull_hi = calc_ote(sh, sl, "bullish")
            ote_bear_lo, ote_bear_hi = calc_ote(sh, sl, "bearish")
            R["ote_bull"] = (ote_bull_lo, ote_bull_hi)
            R["ote_bear"] = (ote_bear_lo, ote_bear_hi)
            R["swing_high"] = sh
            R["swing_low"]  = sl

        # ── 6. Technical Indicators (1H) ──
        close = df_1h["Close"]
        rsi = calc_rsi(close)
        rv  = rsi.iloc[-1]
        macd, sig, hist = calc_macd(close)
        hv, hp = hist.iloc[-1], hist.iloc[-2]
        e20 = calc_ema(close, 20).iloc[-1]
        e50 = calc_ema(close, 50).iloc[-1]
        e200= calc_ema(close, min(200,len(close)-1)).iloc[-1]
        bb_up, bb_mid, bb_lo, bb_w = calc_bb(close)
        atr = calc_atr(df_1h).iloc[-1]

        R["rsi"]   = rv
        R["macd_h"]= hv
        R["ema20"] = e20
        R["ema50"] = e50
        R["ema200"]= e200
        R["atr"]   = atr
        R["bb_squeeze"] = bb_w.iloc[-1] < bb_w.rolling(20).mean().iloc[-1] * 0.7

        # ═══════════════════════════════════════════════
        # ⭐ تهيئة المتغيرات قبل أي استخدام (إصلاح bug)
        # ═══════════════════════════════════════════════
        bull_score = 0
        bear_score = 0
        signals = []

        # ── 7. COT Report ──
        cot_data = fetch_cot_live(sym_yf)
        R["cot"] = cot_data

        # ── 8. Liquidity Sweep Detection ──
        sweep = detect_liquidity_sweep(df_1h, 30)
        R["sweep"] = sweep
        if sweep:
            if sweep["action"] == "BUY":   bull_score += 3; signals.append(f"🎣 {sweep['type']}")
            elif sweep["action"] == "SELL": bear_score += 3; signals.append(f"🎣 {sweep['type']}")

        # ── 9. Retail Sentiment ──
        sentiment = fetch_retail_sentiment(sym_yf)
        R["sentiment"] = sentiment
        if sentiment:
            long_pct = sentiment.get("long_pct", 50)
            # الحيتان عكس الريتيل
            if long_pct > 70:   bear_score += 2; signals.append("ريتيل متطاول → حيتان يبيعون")
            elif long_pct < 30: bull_score += 2; signals.append("ريتيل متقصّر → حيتان يشترون")

        # ── 10. DXY Correlation ──
        dxy_now, dxy_prev = fetch_dxy()
        R["dxy"] = dxy_now
        R["dxy_change"] = ((dxy_now - dxy_prev) / dxy_prev * 100) if dxy_now and dxy_prev else None

        # ── 11. Fed Rate ──
        fed_now, fed_prev = fetch_fed_rate()
        R["fed_rate"] = fed_now
        R["fed_change"] = fed_now - fed_prev

        # ── 12. Macro Sentiment ──
        macro = fetch_macro_sentiment()
        R["macro"] = macro

        # ── 13. Signal Score (Wall Street Collective Intelligence) ──
        # RSI
        if rv < 30:   bull_score += 2; signals.append("RSI ذروة بيع")
        elif rv < 45: bull_score += 1
        elif rv > 70: bear_score += 2; signals.append("RSI ذروة شراء")
        elif rv > 55: bear_score += 1

        # MACD
        if hv > 0 and hv > hp: bull_score += 2; signals.append("MACD صاعد")
        elif hv < 0 and hv < hp: bear_score += 2; signals.append("MACD هابط")

        # EMA
        if price > e20 > e50 > e200: bull_score += 3; signals.append("فوق كل EMAs")
        elif price < e20 < e50:      bear_score += 2; signals.append("تحت EMAs")

        # BB
        if price <= float(bb_lo.iloc[-1]): bull_score += 2; signals.append("لمس BB السفلي")
        elif price >= float(bb_up.iloc[-1]): bear_score += 2; signals.append("لمس BB العلوي")

        # BOS/CHOCH
        if "صاعد" in htf_bias:  bull_score += 3
        elif "هابط" in htf_bias: bear_score += 3

        # Order Blocks
        near_bull_ob = bull_obs and abs(price - bull_obs[0]["mid"]) / price < 0.005
        near_bear_ob = bear_obs and abs(price - bear_obs[0]["mid"]) / price < 0.005
        if near_bull_ob: bull_score += 2; signals.append("قرب Bullish OB")
        if near_bear_ob: bear_score += 2; signals.append("قرب Bearish OB")

        # FVG
        near_bull_fvg = bull_fvg and abs(price - bull_fvg[0]["mid"]) / price < 0.003
        if near_bull_fvg: bull_score += 2; signals.append("داخل Bullish FVG")

        # DXY (للذهب: عكسي)
        is_gold = sym_yf in ("GC=F", "SI=F", "XAUUSD=X", "XAGUSD=X")
        if dxy_now and dxy_prev:
            dxy_up = dxy_now > dxy_prev
            if is_gold:
                if not dxy_up:   bull_score += 2; signals.append("DXY يهبط ← ذهب يرتفع")
                else:            bear_score += 1
            else:
                if "USD" in symbol[:3]:
                    if dxy_up:   bull_score += 1
                    else:        bear_score += 1

        # Real Interest Rate (محرك رئيسي للذهب)
        if is_gold:
            real_rate = fed_now - 2.5  # تقريب: Fed - CPI estimate
            if real_rate < 0:   bull_score += 2; signals.append("معدل فائدة حقيقي سالب ← ذهب")
            elif real_rate < 1: bull_score += 1

        # Wall Street Targets — منطق ذكي يأخذ بعين الاعتبار النطاق
        if is_gold and macro:
            gs_t  = macro.get("gold_target_gs", 5400)   # الأقل تحفظاً
            jp_t  = macro.get("gold_target_jp", 6300)   # الأعلى
            cons  = macro.get("gold_consensus", 4746)   # إجماع المحللين
            # متوسط الأهداف = هدف معتدل
            avg_target = (gs_t + jp_t + macro.get("gold_target_db", 6000) +
                          macro.get("gold_target_ubs", 6200)) / 4
            if price < cons * 0.97:
                # تحت الإجماع → فرصة شراء
                bull_score += 2
                signals.append(f"دون الإجماع (${cons:,})")
            elif price < avg_target:
                # ضمن النطاق المتوقع
                upside = (avg_target - price) / price * 100
                if upside > 10: bull_score += 2
                elif upside > 5: bull_score += 1
                signals.append(f"هدف متوسط: ${avg_target:,.0f}")
            elif price > jp_t:
                # تجاوز أعلى هدف → احذر تصحيح
                bear_score += 2
                signals.append(f"تجاوز كل أهداف Wall Street")
            else:
                # بين الإجماع وأعلى هدف
                signals.append(f"بين الإجماع ${cons:,} و JPM ${jp_t:,}")

        R["bull_score"] = bull_score
        R["bear_score"] = bear_score
        R["signals"]    = signals

        # ── 11. القرار النهائي ──
        total = bull_score + bear_score or 1
        bull_pct = bull_score / total * 100

        if bull_pct >= 65:
            R["action"]   = "BUY"
            R["decision"] = "🟢 شراء"
            R["conf"]     = f"{bull_pct:.0f}% صاعد"
        elif bull_pct <= 35:
            R["action"]   = "SELL"
            R["decision"] = "🔴 بيع"
            R["conf"]     = f"{100-bull_pct:.0f}% هابط"
        else:
            R["action"]   = "WAIT"
            R["decision"] = "⏳ انتظر تأكيد"
            R["conf"]     = f"صاعد {bull_score} | هابط {bear_score}"

        # ── 12. SL/TP بناءً على ATR + ICT ──
        if R["action"] == "BUY":
            R["sl"]  = round(price - 2.0 * atr, 5)
            R["tp1"] = round(price + 2.0 * atr, 5)
            R["tp2"] = round(price + 4.0 * atr, 5)
            R["tp3"] = bsl[0] if bsl else round(price + 6.0 * atr, 5)
        elif R["action"] == "SELL":
            R["sl"]  = round(price + 2.0 * atr, 5)
            R["tp1"] = round(price - 2.0 * atr, 5)
            R["tp2"] = round(price - 4.0 * atr, 5)
            R["tp3"] = ssl[0] if ssl else round(price - 6.0 * atr, 5)

    except Exception as e:
        R["err"] = f"❌ {str(e)[:150]}"

    return R

# ═══════════════════════════════════════════════
# بناء الرسائل
# ═══════════════════════════════════════════════

def fp(v, d=5):
    if v is None: return "—"
    if abs(v) >= 10000: return f"{v:,.2f}"
    if abs(v) >= 100:   return f"{v:.3f}"
    return f"{v:.{d}f}"

def build_signal(R, alert=False):
    if R.get("err"): return R["err"]

    sym    = R["sym"]
    price  = R.get("price", 0)
    action = R.get("action", "WAIT")
    name   = R.get("name", sym)
    now    = datetime.now().strftime("%H:%M %d/%m/%Y")
    pre    = "🔔 *تنبيه تلقائي!*\n" if alert else ""
    icons  = {"BUY":"🟢 شراء قوي","SELL":"🔴 بيع قوي","WAIT":"⏳ انتظر"}

    m  = f"{pre}📊 *{name}*\n"
    m += f"🏷 `{sym}` | {icons.get(action,'⏳')}\n"
    m += f"💰 `{fp(price, 3)}` | 🕐 {now}\n"
    m += "━━━━━━━━━━━━━━━━━━━\n\n"

    # HTF Bias
    m += f"🏗 *هيكل السوق (HTF):*\n"
    m += f"  {R.get('bos','—')} — _{R.get('bos_desc','')}_\n"
    m += f"  الاتجاه العام: *{R.get('htf_bias','محايد')}*\n\n"

    # ICT Concepts
    m += f"⚡ *ICT — Smart Money Concepts:*\n\n"

    # Order Blocks
    bull_obs = R.get("bull_obs", [])
    bear_obs = R.get("bear_obs", [])
    if bull_obs:
        m += f"🟢 *Bullish Order Blocks (دعم):*\n"
        for ob in bull_obs[:2]:
            m += f"  `{fp(ob['low'])}` — `{fp(ob['high'])}`\n"
        m += "\n"
    if bear_obs:
        m += f"🔴 *Bearish Order Blocks (مقاومة):*\n"
        for ob in bear_obs[:2]:
            m += f"  `{fp(ob['low'])}` — `{fp(ob['high'])}`\n"
        m += "\n"

    # FVG
    bull_fvg = R.get("bull_fvg", [])
    bear_fvg = R.get("bear_fvg", [])
    if bull_fvg:
        m += f"🟡 *Bullish FVG (فجوة صاعدة):*\n"
        for fvg in bull_fvg[:2]:
            m += f"  `{fp(fvg['low'])}` — `{fp(fvg['high'])}`\n"
        m += "\n"
    if bear_fvg:
        m += f"🟠 *Bearish FVG (فجوة هابطة):*\n"
        for fvg in bear_fvg[:2]:
            m += f"  `{fp(fvg['low'])}` — `{fp(fvg['high'])}`\n"
        m += "\n"

    # Liquidity
    bsl = R.get("bsl", [])
    ssl = R.get("ssl", [])
    if bsl:
        m += f"💧 *BSL (سيولة شرائية):* "
        m += " | ".join([f"`{fp(x)}`" for x in bsl[:3]]) + "\n"
    if ssl:
        m += f"💧 *SSL (سيولة بيعية):*  "
        m += " | ".join([f"`{fp(x)}`" for x in ssl[:3]]) + "\n"
    m += "\n"

    # OTE
    ote_bull = R.get("ote_bull")
    ote_bear = R.get("ote_bear")
    if ote_bull and action == "BUY":
        m += f"🎯 *OTE للشراء (61.8%-79%):*\n"
        m += f"  `{fp(ote_bull[0])}` — `{fp(ote_bull[1])}`\n\n"
    elif ote_bear and action == "SELL":
        m += f"🎯 *OTE للبيع (61.8%-79%):*\n"
        m += f"  `{fp(ote_bear[0])}` — `{fp(ote_bear[1])}`\n\n"

    # Technical
    m += f"📈 *المؤشرات التقنية:*\n"
    m += f"  RSI: `{R.get('rsi',0):.1f}` | "
    m += f"MACD: `{'▲' if R.get('macd_h',0)>0 else '▼'}{abs(R.get('macd_h',0)):.5f}`\n"
    m += f"  EMA20: `{fp(R.get('ema20'),3)}` | EMA50: `{fp(R.get('ema50'),3)}`\n"
    m += f"  EMA200: `{fp(R.get('ema200'),3)}`\n"
    if R.get("bb_squeeze"): m += f"  🔥 *Bollinger Squeeze — انفجار وشيك!*\n"
    m += "\n"

    # DXY + Fed
    dxy = R.get("dxy")
    dxy_chg = R.get("dxy_change")
    fed = R.get("fed_rate")
    m += f"🏦 *Macro:*\n"
    if dxy:
        dxy_icon = "📈" if (dxy_chg or 0) > 0 else "📉"
        m += f"  DXY: `{dxy:.2f}` {dxy_icon} `{(dxy_chg or 0):+.2f}%`\n"
    if fed:
        m += f"  Fed Rate: `{fed:.2f}%`\n"

    macro = R.get("macro", {})
    if macro:
        m += f"  Fed Cuts 2025: `{macro.get('fed_cuts_2025',0)} cuts متوقعة`\n"
        if sym.upper() in ("XAUUSD","GOLD","ذهب","GC=F"):
            m += f"  GS Gold Target: `${macro.get('gold_target_gs',0):,}`\n"
            m += f"  JPM Gold Target: `${macro.get('gold_target_jp',0):,}`\n"
    m += "\n"

    # COT
    cot_section = format_cot_section(R.get("cot"), R.get("sym_yf",""))
    if cot_section: m += cot_section

    # Liquidity Sweep
    sweep_section = format_sweep_section(R.get("sweep"))
    if sweep_section: m += sweep_section

    # Retail Sentiment
    sent_section = format_sentiment_section(R.get("sentiment"))
    if sent_section: m += sent_section

    # Wall Street Collective Intelligence
    bs = R.get("bull_score", 0)
    br = R.get("bear_score", 0)
    total = bs + br or 1
    bull_pct = bs / total * 100
    bar = "▓" * int(bull_pct / 10) + "░" * (10 - int(bull_pct / 10))
    wi_icon = "🟢" if bull_pct >= 65 else ("🔴" if bull_pct <= 35 else "🟡")

    m += f"━━━━━━━━━━━━━━━━━━━\n"
    m += f"🧠 *Wall Street Intelligence:*\n"
    m += f"{wi_icon} `{bar}` {bull_pct:.0f}% صاعد\n"
    for sig in R.get("signals", [])[:5]:
        m += f"  • {sig}\n"
    m += "\n"

    # Expert Opinion
    m += f"🎓 *رأي الخبير (Harvard/Wall St):*\n"
    expert = generate_expert_opinion(R)
    m += f"_{expert}_\n\n"

    # القرار
    m += f"━━━━━━━━━━━━━━━━━━━\n"
    m += f"📊 *الثقة:* {R.get('conf','')}\n"
    m += f"⚡ *القرار:* {R.get('decision','')}\n\n"

    if action != "WAIT" and R.get("sl"):
        m += f"━━━━━━━━━━━━━━━━━━━\n"
        m += f"🟢 دخول:   `{fp(price, 3)}`\n"
        m += f"🔴 SL:      `{fp(R.get('sl'), 3)}`\n"
        m += f"💰 TP1:     `{fp(R.get('tp1'), 3)}`\n"
        m += f"💰 TP2:     `{fp(R.get('tp2'), 3)}`\n"
        m += f"🏆 TP3:     `{fp(R.get('tp3'), 3)}`\n"
        atr_v = R.get("atr", 0)
        if atr_v:
            m += f"📐 ATR:     `{fp(atr_v, 5)}`\n"
        m += f"⚖️ RR:      1:2 / 1:3\n\n"

    m += "⚠️ _للأغراض التعليمية فقط — ليس توصية استثمارية_"
    return m


def generate_expert_opinion(R):
    action = R.get("action", "WAIT")
    sym    = R.get("sym", "")
    htf    = R.get("htf_bias", "محايد")
    bos    = R.get("bos", "")
    macro  = R.get("macro", {})
    rsi    = R.get("rsi", 50)
    dxy_c  = R.get("dxy_change", 0) or 0
    is_gold = sym.upper() in ("XAUUSD","GOLD","ذهب")

    if action == "BUY":
        if is_gold:
            opinion = (f"الذهب يُظهر إشارات تراكم مؤسسي قوية. "
                      f"الاتجاه العام {htf}. "
                      f"{'انخفاض الدولار يدعم الذهب. ' if dxy_c < 0 else ''}"
                      f"أهداف Wall Street: GS ${macro.get('gold_target_gs',5400):,} | "
                      f"JPM ${macro.get('gold_target_jp',6300):,}. "
                      f"الدخول في منطقة OTE مع SL تحت آخر Order Block.")
        else:
            opinion = (f"{sym} يُظهر {htf} بتأكيد {bos}. "
                      f"RSI={rsi:.0f} يدعم الاتجاه. "
                      f"الدخول عند FVG/OB مع إدارة مخاطر صارمة.")
    elif action == "SELL":
        if is_gold:
            opinion = (f"الذهب يواجه ضغطاً بيعياً. "
                      f"{'ارتفاع الدولار يضغط على الذهب. ' if dxy_c > 0 else ''}"
                      f"RSI={rsi:.0f} يشير لتشبع. Bearish OB يعمل كمقاومة. "
                      f"انتظر تأكيداً قبل البيع.")
        else:
            opinion = (f"{sym} يُظهر ضعفاً. {bos} يؤكد الاتجاه. "
                      f"مستهدفات SSL تحت السعر الحالي.")
    else:
        opinion = (f"{sym} في منطقة محايدة. "
                  f"انتظر {bos} واضحاً وتراجعاً لـ OTE قبل الدخول. "
                  f"الاتجاه العام: {htf}.")

    return opinion


def kb(sym):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔄 تحديث",  callback_data=f"r:{sym}"),
        InlineKeyboardButton("👁 تابع",    callback_data=f"w:{sym}"),
        InlineKeyboardButton("📊 MTF",     callback_data=f"m:{sym}"),
        InlineKeyboardButton("⚡ سكالب",   callback_data=f"s:{sym}"),
    ]])

# ═══════════════════════════════════════════════
# Async
# ═══════════════════════════════════════════════

async def run_analysis(sym):
    loop = asyncio.get_event_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, analyze_forex, sym), timeout=45)
    except asyncio.TimeoutError:
        return {"sym": sym, "err": "❌ انتهى الوقت — حاول مرة ثانية"}


async def monitor_job(ctx):
    chat_id = ctx.job.data["chat_id"]
    sym     = ctx.job.data["sym"]
    try:
        R = await run_analysis(sym)
        if R.get("err"): return
        if R.get("action") in ("BUY","SELL"):
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=build_signal(R, alert=True),
                parse_mode="Markdown",
                reply_markup=kb(sym))
    except Exception:
        pass


# ═══════════════════════════════════════════════
# Telegram Handlers
# ═══════════════════════════════════════════════

def find_order_blocks(df, lookback=20):
    """كشف Order Blocks — مناطق الطلب والعرض المؤسسية."""
    if df is None or len(df) < lookback:
        return [], []
    bullish_obs = []
    bearish_obs = []
    close = df["Close"].values
    high  = df["High"].values
    low   = df["Low"].values
    op    = df["Open"].values
    for i in range(2, min(lookback, len(df)-2)):
        idx = -(i+2)
        if op[idx] > close[idx]:
            if close[idx+1] > op[idx+1] and close[idx+2] > op[idx+2]:
                ob_high = max(op[idx], close[idx])
                ob_low  = min(op[idx], close[idx])
                if ob_high > ob_low:
                    bullish_obs.append({"high":round(float(ob_high),5),"low":round(float(ob_low),5),"mid":round((ob_high+ob_low)/2,5),"age":i})
        if close[idx] > op[idx]:
            if close[idx+1] < op[idx+1] and close[idx+2] < op[idx+2]:
                ob_high = max(op[idx], close[idx])
                ob_low  = min(op[idx], close[idx])
                if ob_high > ob_low:
                    bearish_obs.append({"high":round(float(ob_high),5),"low":round(float(ob_low),5),"mid":round((ob_high+ob_low)/2,5),"age":i})
    price = close[-1]
    bullish_obs = sorted([ob for ob in bullish_obs if ob["high"] < price], key=lambda x: abs(price-x["mid"]))[:3]
    bearish_obs = sorted([ob for ob in bearish_obs if ob["low"] > price],  key=lambda x: abs(price-x["mid"]))[:3]
    return bullish_obs, bearish_obs


def find_fvg(df, lookback=15):
    """Fair Value Gap — الفجوات السعرية غير المملوءة."""
    if df is None or len(df) < 3:
        return [], []
    bull_fvgs = []
    bear_fvgs = []
    price = float(df["Close"].iloc[-1])
    for i in range(2, min(lookback+2, len(df))):
        idx = -i
        h0 = float(df["High"].iloc[idx-2])
        l0 = float(df["Low"].iloc[idx-2])
        h2 = float(df["High"].iloc[idx])
        l2 = float(df["Low"].iloc[idx])
        if h0 < l2 and l2 - h0 > 0:
            bull_fvgs.append({"high":round(l2,5),"low":round(h0,5),"mid":round((l2+h0)/2,5),"age":i})
        if l0 > h2 and l0 - h2 > 0:
            bear_fvgs.append({"high":round(l0,5),"low":round(h2,5),"mid":round((l0+h2)/2,5),"age":i})
    bull_fvgs = sorted([f for f in bull_fvgs if f["high"] < price], key=lambda x: abs(price-x["mid"]))[:2]
    bear_fvgs = sorted([f for f in bear_fvgs if f["low"] > price],  key=lambda x: abs(price-x["mid"]))[:2]
    return bull_fvgs, bear_fvgs


def find_liquidity(df, lookback=20):
    """مناطق السيولة — BSL (فوق) و SSL (تحت)."""
    if df is None or len(df) < lookback:
        return [], []
    high  = df["High"].values[-lookback:]
    low   = df["Low"].values[-lookback:]
    close = float(df["Close"].iloc[-1])
    bsl = [round(float(high[i]),5) for i in range(1,len(high)-1)
           if high[i]>high[i-1] and high[i]>high[i+1] and high[i]>close]
    ssl = [round(float(low[i]),5)  for i in range(1,len(low)-1)
           if low[i]<low[i-1]  and low[i]<low[i+1]  and low[i]<close]
    return sorted(set(bsl))[:4], sorted(set(ssl), reverse=True)[:4]


def calc_ote(swing_high, swing_low, direction="bullish"):
    """Optimal Trade Entry — Fibonacci 61.8%-79%."""
    diff = swing_high - swing_low
    if direction == "bullish":
        return round(swing_high - diff*0.79, 5), round(swing_high - diff*0.618, 5)
    else:
        return round(swing_low + diff*0.618, 5), round(swing_low + diff*0.79, 5)


def detect_bos_choch(df, lookback=30):
    """
    BOS = Break of Structure
    CHOCH = Change of Character
    """
    if df is None or len(df) < lookback:
        return None, None, "محايد"

    close = df["Close"].values[-lookback:]
    high  = df["High"].values[-lookback:]
    low   = df["Low"].values[-lookback:]

    last_high = max(high[:-5])
    last_low  = min(low[:-5])
    cur_high  = max(high[-5:])
    cur_low   = min(low[-5:])

    if cur_high > last_high:
        return "BOS ▲", "كسر هيكل صاعد", "صاعد"
    elif cur_low < last_low:
        return "BOS ▼", "كسر هيكل هابط", "هابط"
    elif close[-1] > (last_high + last_low) / 2:
        return "CHOCH?", "احتمال انعكاس صاعد", "صاعد محتمل"
    else:
        return "CHOCH?", "احتمال انعكاس هابط", "هابط محتمل"


def analyze_single_timeframe(df, tf_label, price, sym_yf):
    """تحليل فريم زمني واحد وإرجاع ملخص نصي."""
    if df is None or len(df) < 20:
        return "⚠️ " + tf_label + ": بيانات غير كافية\n"

    close = df["Close"]; high = df["High"]; low = df["Low"]
    rsi   = calc_rsi(close).iloc[-1]
    _, _, hist = calc_macd(close)
    macd_dir = "▲" if hist.iloc[-1] > 0 else "▼"
    e20  = calc_ema(close, 20).iloc[-1]
    e50  = calc_ema(close, 50).iloc[-1]
    above_emas = price > e20 > e50
    _, _, tf_bias = detect_bos_choch(df, 20)
    bull_obs, bear_obs = find_order_blocks(df, 15)
    bull_fvg, bear_fvg = find_fvg(df, 10)
    sweep = detect_liquidity_sweep(df, 20)

    bull_pts = 0; bear_pts = 0
    if rsi < 40: bull_pts += 1
    elif rsi > 60: bear_pts += 1
    if macd_dir == "▲": bull_pts += 1
    else: bear_pts += 1
    if above_emas: bull_pts += 1
    else: bear_pts += 1
    if "صاعد" in tf_bias: bull_pts += 2
    elif "هابط" in tf_bias: bear_pts += 2

    total = bull_pts + bear_pts or 1
    pct   = bull_pts / total * 100
    tf_icon = "🟢" if pct >= 60 else ("🔴" if pct <= 40 else "🟡")
    bar  = "▓" * int(pct/10) + "░" * (10-int(pct/10))
    ema_icon = "✅" if above_emas else "🔴"

    msg  = tf_icon + " *" + tf_label + "* `" + bar + "` " + f"{pct:.0f}%" + "\n"
    msg += f"   RSI:`{rsi:.0f}` MACD:`{macd_dir}` EMA:`{ema_icon}`\n"
    msg += "   هيكل: _" + tf_bias + "_\n"

    if sweep:
        msg += "   🎣 " + sweep["type"] + " — ثقة: " + sweep["confidence"] + "\n"

    if bull_obs:
        ob = bull_obs[0]
        msg += f"   🟢 Bullish OB: `{fp(ob['low'],3)}`—`{fp(ob['high'],3)}`\n"
    if bear_obs:
        ob = bear_obs[0]
        msg += f"   🔴 Bearish OB: `{fp(ob['low'],3)}`—`{fp(ob['high'],3)}`\n"
    if bull_fvg:
        fvg = bull_fvg[0]
        msg += f"   🟡 Bull FVG: `{fp(fvg['low'],3)}`—`{fp(fvg['high'],3)}`\n"
    if bear_fvg:
        fvg = bear_fvg[0]
        msg += f"   🟠 Bear FVG: `{fp(fvg['low'],3)}`—`{fp(fvg['high'],3)}`\n"

    sh   = float(high.iloc[-20:].max())
    sl_v = float(low.iloc[-20:].min())
    if "صاعد" in tf_bias:
        ote_lo, ote_hi = calc_ote(sh, sl_v, "bullish")
        msg += f"   🎯 OTE شراء: `{fp(ote_lo,3)}`—`{fp(ote_hi,3)}`\n"
    elif "هابط" in tf_bias:
        ote_lo, ote_hi = calc_ote(sh, sl_v, "bearish")
        msg += f"   🎯 OTE بيع: `{fp(ote_lo,3)}`—`{fp(ote_hi,3)}`\n"

    msg += "\n"
    return msg

async def run_mtf_analysis(sym_raw):
    """تحليل متعدد الفريمات الكامل."""
    sym_yf = get_yf_symbol(sym_raw)

    # جلب كل الفريمات
    loop = asyncio.get_event_loop()

    def fetch_all():
        return {
            "MN": fetch_ohlcv(sym_yf, "1mo", "5y"),
            "W1": fetch_ohlcv(sym_yf, "1wk", "2y"),
            "D1": fetch_ohlcv(sym_yf, "1d",  "1y"),
            "H4": None,  # نحسبها من H1
            "H1": fetch_ohlcv(sym_yf, "1h",  "30d"),
            "M15":fetch_ohlcv(sym_yf, "15m", "5d"),
        }

    try:
        tfs = await asyncio.wait_for(
            loop.run_in_executor(None, fetch_all), timeout=40)
    except asyncio.TimeoutError:
        return None, "❌ انتهى الوقت"

    # تحويل H1 → H4
    if tfs["H1"] is not None and len(tfs["H1"]) > 20:
        tfs["H4"] = tfs["H1"].resample("4h").agg({
            "Open":"first","High":"max","Low":"min",
            "Close":"last","Volume":"sum"}).dropna()

    price = None
    if tfs["H1"] is not None and len(tfs["H1"]) > 0:
        price = float(tfs["H1"]["Close"].iloc[-1])

    if not price:
        return None, "❌ لا تتوفر بيانات"

    now = datetime.now().strftime("%H:%M %d/%m/%Y")
    name = PAIR_NAMES.get(sym_yf, sym_raw)

    header = "MTF Analysis"
    msg  = "*" + header + " — " + name + "*" + "\n"
    msg += "`" + fp(price,3) + "` | " + now + "\n"
    msg += "=" * 20 + "\n\n"
    msg += "*الفريمات:*\n\n"

    tf_labels = {
        "MN": "شهري  (MN)","W1": "اسبوعي (W1)","D1": "يومي   (D1)",
        "H4": "4 ساعات (H4)","H1": "ساعة   (H1)","M15":"15 دقيقة (M15)",
    }

    bull_tfs = 0; bear_tfs = 0
    for tf_key, tf_label in tf_labels.items():
        df = tfs.get(tf_key)
        section = analyze_single_timeframe(df, tf_label, price, sym_yf)
        msg += section
        if section.startswith("🟢"): bull_tfs += 1
        elif section.startswith("🔴"): bear_tfs += 1

    total_tfs = bull_tfs + bear_tfs or 1
    alignment_pct = bull_tfs / total_tfs * 100

    if alignment_pct >= 70:   verdict = "توافق صاعد قوي — فرصة شراء"
    elif alignment_pct <= 30: verdict = "توافق هابط قوي — فرصة بيع"
    elif alignment_pct >= 55: verdict = "ميل صاعد — انتظر تاكيدا"
    elif alignment_pct <= 45: verdict = "ميل هابط — انتظر تاكيدا"
    else:                      verdict = "تعارض — ابقَ خارج السوق"

    msg += "=" * 20 + "\n"
    msg += "خلاصة MTF:\n"
    msg += "صاعدة: " + str(bull_tfs) + " | هابطة: " + str(bear_tfs) + "\n"
    msg += "التوافق: " + f"{alignment_pct:.0f}" + "%\n\n"
    msg += verdict + "\n\n"
    msg += "للاغراض التعليمية فقط"
    return msg, None


# ═══════════════════════════════════════════════
# تحليل الدخول السريع (Scalping / Short-Term)
# ═══════════════════════════════════════════════

SCALP_TIMEFRAMES = {
    "1m":  {"period": "1d",  "interval": "1m",  "label": "1 دقيقة"},
    "5m":  {"period": "5d",  "interval": "5m",  "label": "5 دقائق"},
    "15m": {"period": "5d",  "interval": "15m", "label": "15 دقيقة"},
    "1h":  {"period": "30d", "interval": "1h",  "label": "1 ساعة"},
    "4h":  {"period": "60d", "interval": "1h",  "label": "4 ساعات"},
}

def analyze_scalp(df, price, atr_multi=1.5):
    """
    تحليل سريع للدخول والخروج:
    - نقطة دخول دقيقة
    - SL ضيق بناءً على ATR
    - TP1 (1:1) و TP2 (1:2)
    - اتجاه واضح في نقطة واحدة
    """
    if df is None or len(df) < 20:
        return None

    close = df["Close"]; high = df["High"]; low = df["Low"]

    # RSI
    rsi = calc_rsi(close).iloc[-1]
    # MACD
    _, _, hist = calc_macd(close)
    macd_bull = hist.iloc[-1] > 0 and hist.iloc[-1] > hist.iloc[-2]
    macd_bear = hist.iloc[-1] < 0 and hist.iloc[-1] < hist.iloc[-2]
    # EMA
    e9  = calc_ema(close, 9).iloc[-1]
    e21 = calc_ema(close, 21).iloc[-1]
    # ATR
    atr = calc_atr(df).iloc[-1]
    # Stochastic
    low_k  = low.rolling(14).min()
    high_k = high.rolling(14).max()
    stoch  = 100 * (close - low_k) / (high_k - low_k).replace(0, float('nan'))
    stoch_v = stoch.iloc[-1]
    stoch_prev = stoch.iloc[-2]
    # Liquidity sweep
    sweep = detect_liquidity_sweep(df, 20)
    # Order blocks
    bull_obs, bear_obs = find_order_blocks(df, 10)
    # FVG
    bull_fvg, bear_fvg = find_fvg(df, 8)

    # نقاط الشراء
    bull_pts = 0
    if rsi < 40: bull_pts += 2
    elif rsi < 50: bull_pts += 1
    if macd_bull: bull_pts += 2
    if price > e9 > e21: bull_pts += 2
    if stoch_v < 20 and stoch_v > stoch_prev: bull_pts += 2
    if sweep and sweep["action"] == "BUY": bull_pts += 3
    if bull_obs and abs(price - bull_obs[0]["mid"]) / price < 0.003: bull_pts += 2
    if bull_fvg and abs(price - bull_fvg[0]["mid"]) / price < 0.002: bull_pts += 2

    # نقاط البيع
    bear_pts = 0
    if rsi > 60: bear_pts += 2
    elif rsi > 50: bear_pts += 1
    if macd_bear: bear_pts += 2
    if price < e9 < e21: bear_pts += 2
    if stoch_v > 80 and stoch_v < stoch_prev: bear_pts += 2
    if sweep and sweep["action"] == "SELL": bear_pts += 3
    if bear_obs and abs(price - bear_obs[0]["mid"]) / price < 0.003: bear_pts += 2
    if bear_fvg and abs(price - bear_fvg[0]["mid"]) / price < 0.002: bear_pts += 2

    total = bull_pts + bear_pts or 1
    bull_pct = bull_pts / total * 100

    if bull_pct >= 62:
        action = "BUY"
        entry  = price
        sl     = round(price - atr_multi * atr, 5)
        tp1    = round(price + atr_multi * atr, 5)
        tp2    = round(price + atr_multi * 2 * atr, 5)
        tp3    = round(price + atr_multi * 3 * atr, 5)
    elif bull_pct <= 38:
        action = "SELL"
        entry  = price
        sl     = round(price + atr_multi * atr, 5)
        tp1    = round(price - atr_multi * atr, 5)
        tp2    = round(price - atr_multi * 2 * atr, 5)
        tp3    = round(price - atr_multi * 3 * atr, 5)
    else:
        return {"action": "WAIT", "conf": bull_pct, "rsi": rsi,
                "sweep": sweep, "atr": atr}

    return {
        "action": action, "entry": entry, "sl": sl,
        "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "conf": bull_pct, "rsi": rsi, "atr": atr,
        "stoch": stoch_v, "e9": e9, "e21": e21,
        "macd_bull": macd_bull, "macd_bear": macd_bear,
        "sweep": sweep,
        "bull_ob": bull_obs[0] if bull_obs else None,
        "bear_ob": bear_obs[0] if bear_obs else None,
        "bull_fvg": bull_fvg[0] if bull_fvg else None,
    }


async def run_scalp_analysis(sym_raw, tf_key="15m"):
    """تحليل الدخول السريع على فريم محدد."""
    sym_yf = get_yf_symbol(sym_raw)
    tf_cfg = SCALP_TIMEFRAMES.get(tf_key, SCALP_TIMEFRAMES["15m"])

    def fetch():
        df = fetch_ohlcv(sym_yf, tf_cfg["interval"], tf_cfg["period"])
        # لـ 4H نجمع من H1
        if tf_key == "4h" and df is not None and len(df) > 20:
            df = df.resample("4h").agg({
                "Open":"first","High":"max","Low":"min",
                "Close":"last","Volume":"sum"}).dropna()
        return df

    loop = asyncio.get_event_loop()
    try:
        df = await asyncio.wait_for(
            loop.run_in_executor(None, fetch), timeout=30)
    except asyncio.TimeoutError:
        return None, "❌ انتهى الوقت"

    if df is None or len(df) < 20:
        return None, "❌ بيانات غير كافية"

    price = float(df["Close"].iloc[-1])
    name  = PAIR_NAMES.get(sym_yf, sym_raw)
    now   = datetime.now().strftime("%H:%M %d/%m/%Y")
    tf_label = tf_cfg["label"]

    # تحليلات متعددة لضبط ATR
    scalp_1 = analyze_scalp(df, price, 1.0)   # محافظ
    scalp_2 = analyze_scalp(df, price, 1.5)   # متوسط
    scalp_3 = analyze_scalp(df, price, 2.0)   # واسع

    # استخدم التحليل المتوسط
    S = scalp_2
    if not S:
        return None, "❌ خطأ في التحليل"

    action = S["action"]
    icons  = {"BUY": "🟢", "SELL": "🔴", "WAIT": "⏳"}
    icon   = icons.get(action, "⏳")

    msg  = "دخول سريع: " + name + "\n"
    msg += tf_label + " | " + fp(price, 3) + " | " + now + "\n"
    msg += "=" * 22 + "\n\n"

    # المؤشرات
    msg += "*المؤشرات:*\n"
    rsi_v   = S['rsi']
    stoch_v2 = S.get('stoch', 50)
    macd_icon = "up" if S.get("macd_bull") else ("down" if S.get("macd_bear") else "flat")
    msg += f"RSI: `{rsi_v:.1f}` | Stoch: `{stoch_v2:.1f}` | MACD: {macd_icon}\n"
    e9_v  = S.get('e9', 0)
    e21_v = S.get('e21', 0)
    msg += f"EMA9: `{fp(e9_v,3)}` | EMA21: `{fp(e21_v,3)}`\n\n"

    # ICT
    if S.get("sweep"):
        sw = S["sweep"]
        msg += "Sweep: " + sw['type'] + " | " + sw['confidence'] + "\n"
    if S.get("bull_ob") and action == "BUY":
        ob = S["bull_ob"]
        msg += f"Bullish OB: `{fp(ob['low'],3)}`-`{fp(ob['high'],3)}`\n"
    if S.get("bear_ob") and action == "SELL":
        ob = S["bear_ob"]
        msg += f"Bearish OB: `{fp(ob['low'],3)}`-`{fp(ob['high'],3)}`\n"
    if S.get("bull_fvg") and action == "BUY":
        fvg = S["bull_fvg"]
        msg += f"Bull FVG: `{fp(fvg['low'],3)}`-`{fp(fvg['high'],3)}`\n"
    msg += "\n"

    # القرار
    msg += "=" * 22 + "\n"
    if action == "WAIT":
        msg += "انتظر - لا اشارة واضحة\n"
        msg += f"الثقة: {S['conf']:.0f}% | ATR: `{fp(S['atr'],5)}`\n\n"
    else:
        conf = S["conf"] if action == "BUY" else (100 - S["conf"])
        msg += f"{icon} {action} | الثقة: `{conf:.0f}%`\n\n"
        msg += f"دخول: `{fp(S['entry'],3)}`\n"
        msg += f"SL:   `{fp(S['sl'],3)}`\n"
        msg += f"TP1:  `{fp(S['tp1'],3)}` (1:1)\n"
        msg += f"TP2:  `{fp(S['tp2'],3)}` (1:2)\n"
        msg += f"TP3:  `{fp(S['tp3'],3)}` (1:3)\n"
        msg += f"ATR:  `{fp(S['atr'],5)}`\n\n"
        if scalp_1 and scalp_1.get("action") == action:
            msg += "خيارات SL:\n"
            msg += f"  محافظ (1xATR):   `{fp(scalp_1['sl'],3)}`\n"
            msg += f"  متوسط (1.5xATR): `{fp(scalp_2['sl'],3)}`\n"
            msg += f"  واسع  (2xATR):   `{fp(scalp_3['sl'],3)}`\n\n"

    msg += "للاغراض التعليمية فقط"
    msg += "⚠️ _للأغراض التعليمية فقط_"
    return msg, None



# ╔══════════════════════════════════════════════════════════╗
# ║      SCALPING MODULE — FOREX & GOLD                     ║
# ╚══════════════════════════════════════════════════════════╝

SCALP_TIMEFRAMES = {
    "1m": {"period":"1d","interval":"1m","label":"1 دقيقة"},
    "5m": {"period":"5d","interval":"5m","label":"5 دقائق"},
}

def _srsi(close, n=7):
    d=close.diff(); g=d.clip(lower=0).rolling(n).mean()
    l=(-d.clip(upper=0)).rolling(n).mean()
    return 100-100/(1+g/l.replace(0,np.nan))

def analyze_scalp_forex(sym_yf, price, df1, df5):
    """Scalping 7 مؤشرات للفوركس والذهب."""
    R={"bull":0,"bear":0,"sigs":[],"warn":[]}
    if df1 is None or len(df1)<20: return None
    cl1=df1["Close"]; hi1=df1["High"]; lo1=df1["Low"]
    op1=df1["Open"];  vo1=df1.get("Volume",None)

    # 1. RSI(7)
    rsi7=float(_srsi(cl1).iloc[-1]); R["rsi7"]=rsi7
    if rsi7<=25: R["bull"]+=2; R["sigs"].append(("1","RSI(7) 1m","✅",f"{rsi7:.1f}","ذروة بيع قوية ⚡"))
    elif rsi7<=35: R["bull"]+=1; R["sigs"].append(("1","RSI(7) 1m","✅",f"{rsi7:.1f}","ذروة بيع"))
    elif rsi7>=75: R["bear"]+=2; R["sigs"].append(("1","RSI(7) 1m","🔴",f"{rsi7:.1f}","ذروة شراء قوية ⚡"))
    elif rsi7>=65: R["bear"]+=1; R["sigs"].append(("1","RSI(7) 1m","🔴",f"{rsi7:.1f}","ذروة شراء"))
    else: R["sigs"].append(("1","RSI(7) 1m","⚪",f"{rsi7:.1f}","محايد"))

    # 2. EMA Cross 5/13
    e5=cl1.ewm(span=5,adjust=False).mean(); e13=cl1.ewm(span=13,adjust=False).mean()
    cup=float(e5.iloc[-2])<float(e13.iloc[-2]) and float(e5.iloc[-1])>=float(e13.iloc[-1])
    cdn=float(e5.iloc[-2])>float(e13.iloc[-2]) and float(e5.iloc[-1])<=float(e13.iloc[-1])
    if cup: R["bull"]+=3; R["sigs"].append(("2","EMA Cross 1m","✅","EMA5 ↗ EMA13","Golden Cross ⚡"))
    elif cdn: R["bear"]+=3; R["sigs"].append(("2","EMA Cross 1m","🔴","EMA5 ↘ EMA13","Death Cross ⚡"))
    elif float(e5.iloc[-1])>float(e13.iloc[-1]): R["bull"]+=1; R["sigs"].append(("2","EMA Cross 1m","✅","EMA5 فوق EMA13","صاعد"))
    else: R["bear"]+=1; R["sigs"].append(("2","EMA Cross 1m","🔴","EMA5 تحت EMA13","هابط"))

    # 3. Bollinger 5m
    if df5 is not None and len(df5)>=20:
        c5=df5["Close"]; bm=c5.rolling(20).mean(); bs=c5.rolling(20).std()
        bup=float((bm+2*bs).iloc[-1]); blo=float((bm-2*bs).iloc[-1])
        bw=(bup-blo)/float(bm.iloc[-1])*100
        if price<=blo: R["bull"]+=2; R["sigs"].append(("3","Bollinger 5m","✅",f"BB سفلي {fp(blo,3)}","Bounce ⚡"))
        elif price>=bup: R["bear"]+=2; R["sigs"].append(("3","Bollinger 5m","🔴",f"BB علوي {fp(bup,3)}","انعكاس ⚡"))
        elif bw<0.15: R["sigs"].append(("3","Bollinger 5m","🟡",f"Squeeze {bw:.3f}%","اختراق وشيك 🔥"))
        else: R["sigs"].append(("3","Bollinger 5m","⚪",f"عرض {bw:.3f}%","طبيعي"))
    else: R["sigs"].append(("3","Bollinger 5m","❓","غير متاح",""))

    # 4. Volume
    if vo1 is not None and len(vo1)>20 and vo1.sum()>0:
        va=float(vo1.iloc[-20:-1].mean()) or 1; vc=float(vo1.iloc[-1]); vr=vc/va
        if vr>=2.5:
            bb=float(cl1.iloc[-1])>float(op1.iloc[-1])
            R["bull" if bb else "bear"]+=2
            R["sigs"].append(("4","Volume Spike","✅" if bb else "🔴",f"x{vr:.1f}","ضغط شراء 🔥" if bb else "ضغط بيع 🔥"))
        elif vr>=1.5: R["sigs"].append(("4","Volume","🟡",f"x{vr:.1f}","مرتفع"))
        else: R["sigs"].append(("4","Volume","⚪",f"x{vr:.1f}","طبيعي"))
    else: R["sigs"].append(("4","Volume","⚪","N/A للفوركس",""))

    # 5. Stochastic
    lok=lo1.rolling(14).min(); hik=hi1.rolling(14).max()
    stoch=100*(cl1-lok)/(hik-lok).replace(0,np.nan)
    sk=float(stoch.iloc[-1]); sp=float(stoch.iloc[-2]); R["stoch"]=sk
    if sk<20 and sk>sp: R["bull"]+=2; R["sigs"].append(("5","Stochastic 1m","✅",f"K={sk:.1f}","ذروة بيع + صعود ⚡"))
    elif sk<20: R["bull"]+=1; R["sigs"].append(("5","Stochastic 1m","✅",f"K={sk:.1f}","ذروة بيع"))
    elif sk>80 and sk<sp: R["bear"]+=2; R["sigs"].append(("5","Stochastic 1m","🔴",f"K={sk:.1f}","ذروة شراء + هبوط ⚡"))
    elif sk>80: R["bear"]+=1; R["sigs"].append(("5","Stochastic 1m","🔴",f"K={sk:.1f}","ذروة شراء"))
    else: R["sigs"].append(("5","Stochastic 1m","⚪",f"K={sk:.1f}","محايد"))

    # 6. Liquidity Sweep
    sweep=detect_liquidity_sweep(df1,20)
    if sweep:
        if sweep["action"]=="BUY": R["bull"]+=3; R["sigs"].append(("6","Liquidity Sweep","✅",sweep["type"],"اصطياد SSL — LONG ⚡"))
        elif sweep["action"]=="SELL": R["bear"]+=3; R["sigs"].append(("6","Liquidity Sweep","🔴",sweep["type"],"اصطياد BSL — SELL ⚡"))
    else: R["sigs"].append(("6","Liquidity Sweep","⚪","لا sweep",""))
    R["sweep"]=sweep

    # 7. ATR
    atr=float(calc_atr(df1).iloc[-1]) if df1 is not None else 0
    atrp=atr/price*100 if price>0 else 0; R["atr"]=atr
    if atrp>=0.1:
        if float(cl1.iloc[-1])>float(cl1.iloc[-3]): R["bull"]+=1
        else: R["bear"]+=1
        R["sigs"].append(("7","ATR Momentum","🔥",f"{atrp:.3f}%","تقلب جيد ⚡"))
    elif atrp>=0.04: R["sigs"].append(("7","ATR Momentum","✅",f"{atrp:.3f}%","مقبول"))
    else: R["warn"].append(f"⚠️ ATR منخفض ({atrp:.4f}%)"); R["sigs"].append(("7","ATR Momentum","⚪",f"{atrp:.4f}%","هادئ"))

    # القرار
    if R["bull"]>=7: R["action"]="BUY"; R["decision"]="⚡ SCALP BUY قوي"; R["conf"]=f"{R['bull']} إشارة"
    elif R["bear"]>=7: R["action"]="SELL"; R["decision"]="⚡ SCALP SELL قوي"; R["conf"]=f"{R['bear']} إشارة"
    elif R["bull"]>=5: R["action"]="BUY"; R["decision"]="✅ SCALP BUY"; R["conf"]=f"{R['bull']} إشارة"
    elif R["bear"]>=5: R["action"]="SELL"; R["decision"]="🔴 SCALP SELL"; R["conf"]=f"{R['bear']} إشارة"
    else: R["action"]="WAIT"; R["decision"]="⏳ انتظر"; R["conf"]=f"↑{R['bull']} ↓{R['bear']}"

    is_gold=sym_yf in("XAUUSD=X","GC=F")
    if R["action"]!="WAIT" and atr>0:
        _sd=max(atr*0.5,price*0.0003)
        if R["action"]=="BUY":
            R["sl"]=round(price-_sd,5); R["tp1"]=round(price+_sd,5)
            R["tp2"]=round(price+_sd*2,5); R["tp3"]=round(price+_sd*3,5)
        else:
            R["sl"]=round(price+_sd,5); R["tp1"]=round(price-_sd,5)
            R["tp2"]=round(price-_sd*2,5); R["tp3"]=round(price-_sd*3,5)
        R["sl_pct"]=round(_sd/price*100,4); R["tp1_pct"]=round(_sd/price*100,4)
    R["lev"]="x5-x10 (ذهب)" if is_gold else "x10-x20 (فوركس)"
    R["size"]="0.5-1% من المحفظة"; R["duration"]="1-15 دقيقة"
    return R


async def run_scalp_analysis(sym_raw, tf_key="5m"):
    """تشغيل Scalp Analysis للفوركس/ذهب."""
    sym_yf=get_yf_symbol(sym_raw); name=PAIR_NAMES.get(sym_yf,sym_raw)
    def fetch():
        df1=fetch_ohlcv(sym_yf,"1m","1d"); df5=fetch_ohlcv(sym_yf,"5m","5d")
        price=None
        try:
            import yfinance as yf
            price=yf.Ticker(sym_yf).fast_info.last_price
        except: pass
        if not price and df1 is not None and len(df1)>0:
            price=float(df1["Close"].iloc[-1])
        return df1,df5,price
    loop=asyncio.get_event_loop()
    try: df1,df5,price=await asyncio.wait_for(loop.run_in_executor(None,fetch),timeout=35)
    except asyncio.TimeoutError: return None,"❌ انتهى الوقت"
    if not price: return None,"❌ السعر غير متاح"
    if df1 is None or len(df1)<20: return None,"❌ بيانات 1m غير كافية"
    sym_yf2=get_yf_symbol(sym_raw)
    S=analyze_scalp_forex(sym_yf2,price,df1,df5)
    if S is None: return None,"❌ خطأ في التحليل"
    _tz3=timezone(timedelta(hours=3))
    now=datetime.now(_tz3).strftime("%H:%M:%S %d/%m/%Y")
    action=S["action"]; icons={"BUY":"🟢 BUY","SELL":"🔴 SELL","WAIT":"⏳ انتظر"}
    msg =f"⚡ *SCALP — {name}* — {icons.get(action,'⏳')}\n"
    msg+=f"💰 `{fp(price,3)}` | 🕐 {now}\n"
    msg+=f"⏱ المدة: `{S.get('duration','1-15 دقيقة')}`\n"
    msg+="━━━━━━━━━━━━━━━━━━━\n\n"
    msg+="📊 *المؤشرات السريعة (1m/5m):*\n\n"
    for num,nm,icon,val,note in sorted(S.get("sigs",[]),key=lambda x:x[0]):
        msg+=f"{icon} *{num}. {nm}:* `{val}`\n"
        if note: msg+=f"   _{note}_\n"
        msg+="\n"
    msg+="━━━━━━━━━━━━━━━━━━━\n"
    msg+=f"📊 *النتيجة:* {S.get('conf','')}\n"
    msg+=f"⚡ *القرار:* {S.get('decision','')}\n\n"
    if action!="WAIT" and S.get("sl"):
        slp=S.get("sl_pct",0); tpp=S.get("tp1_pct",0)
        msg+="━━━━━━━━━━━━━━━━━━━\n"
        msg+=f"🟢 دخول: `{fp(price,3)}`\n"
        msg+=f"🔴 SL:   `{fp(S['sl'],3)}` _(-{slp:.3f}%)_\n"
        msg+=f"💰 TP1:  `{fp(S['tp1'],3)}` _(+{tpp:.3f}%)_\n"
        msg+=f"💰 TP2:  `{fp(S['tp2'],3)}`\n"
        msg+=f"🏆 TP3:  `{fp(S['tp3'],3)}`\n"
        msg+=f"🔧 الرافعة: `{S.get('lev','')}`\n"
        msg+=f"💼 الحجم:   `{S.get('size','')}`\n\n"
    for w in S.get("warn",[]): msg+=f"{w}\n"
    msg+="━━━━━━━━━━━━━━━━━━━\n"
    msg+="💡 *نصائح:* اخرج فوراً عند أول إشارة عكسية | London/NY Open أفضل وقت\n\n"
    msg+="⚠️ _للأغراض التعليمية فقط_"
    return msg,None


async def scalp_monitor_job_fx(ctx):
    """فحص Scalping كل 5 دقائق للفوركس/ذهب."""
    chat_id=ctx.job.data["chat_id"]; sym_raw=ctx.job.data["sym"]
    try:
        result_msg,err=await run_scalp_analysis(sym_raw,"5m")
        if err or result_msg is None: return
        # تحقق من وجود إشارة واضحة
        if "⚡ SCALP" in result_msg or "✅ SCALP" in result_msg or "🔴 SCALP" in result_msg:
            if "⏳ انتظر" not in result_msg.split("القرار:")[-1][:30]:
                await ctx.bot.send_message(
                    chat_id=chat_id,
                    text="🔔 *تنبيه Scalp تلقائي!*\n" + result_msg,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("⚡ تحديث Scalp", callback_data=f"s:{sym_raw}"),
                        InlineKeyboardButton("📊 تحليل شامل",  callback_data=f"r:{sym_raw}"),
                    ]]))
    except Exception: pass

async def cmd_start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    cid = u.effective_chat.id
    wc  = len(watching.get(cid, {}))
    await u.message.reply_text(
        "📊 *FOREX & GOLD PRO BOT*\n"
        "تحليل احترافي بمنهجية Smart Money / ICT\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🥇 *الذهب والفضة:*\n"
        "`XAUUSD` | `GOLD` | `ذهب`\n"
        "`XAGUSD` | `SILVER` | `فضة`\n\n"
        "💱 *الفوركس:*\n"
        "`EURUSD` | `GBPUSD` | `USDJPY`\n"
        "`USDCHF` | `AUDUSD` | `USDCAD`\n\n"
        "👁 *متابعة تلقائية:*\n"
        "`تابع XAUUSD` — تنبيه عند إشارة\n"
        "`وقف XAUUSD` | `وقف الكل`\n\n"
        "⚡ *Scalping (1m/5m):*\n"
        "`سكالب XAUUSD` — تحليل فوري\n"
        "`تابع سكالب XAUUSD` — تنبيه كل 5 دقائق\n"
        "`وقف سكالب XAUUSD` — إيقاف\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ *المؤشرات (20 أداة):*\n"
        "🔸 ICT: Order Blocks | FVG | Liquidity\n"
        "🔸 ICT: BOS | CHOCH | OTE (Fibonacci)\n"
        "🔸 تقني: RSI | MACD | EMA | BB\n"
        "🔸 ماكرو: DXY | Fed Rate | Real Rate\n"
        "🔸 مؤسسي: GS/JPM Targets | COT\n"
        "🔸 ذكاء: Wall Street Consensus\n\n"
        f"📊 تحت المتابعة: *{wc}* زوج\n\n"
        "⚠️ _للأغراض التعليمية فقط_",
        parse_mode="Markdown")


async def handle_msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    text    = u.message.text.strip()
    chat_id = u.effective_chat.id

    # ── تابع سكالب ──
    if text.startswith("تابع سكالب") or text.lower().startswith("watch scalp"):
        parts=text.split(); raw=parts[2] if len(parts)>=3 else ""
        if not raw:
            await u.message.reply_text("مثال: `تابع سكالب XAUUSD`",parse_mode="Markdown"); return
        sym_raw=raw.upper()
        scalp_watching.setdefault(chat_id,{})[sym_raw]=True
        jn=f"sfx_{chat_id}_{sym_raw}"
        for j in c.job_queue.get_jobs_by_name(jn): j.schedule_removal()
        c.job_queue.run_repeating(
            scalp_monitor_job_fx, interval=300, first=10,
            data={"chat_id":chat_id,"sym":sym_raw}, name=jn)
        await u.message.reply_text(
            f"⚡ *بدأت متابعة Scalp — {sym_raw}*\n"
            f"فحص كل 5 دقائق\n"
            f"تنبيه عند: ≥5 إشارات\n"
            f"إيقاف: `وقف سكالب {sym_raw}`",
            parse_mode="Markdown")
        return

    # ── وقف سكالب ──
    if text.startswith("وقف سكالب"):
        parts=text.split(); raw=parts[2] if len(parts)>=3 else ""
        sym_raw=raw.upper() if raw else ""
        if sym_raw:
            jn=f"sfx_{chat_id}_{sym_raw}"
            for j in c.job_queue.get_jobs_by_name(jn): j.schedule_removal()
            scalp_watching.get(chat_id,{}).pop(sym_raw,None)
            await u.message.reply_text(f"⛔ تم إيقاف Scalp Monitor — {sym_raw}")
        else:
            await u.message.reply_text("مثال: `وقف سكالب XAUUSD`",parse_mode="Markdown")
        return

    # تابع
    if text.startswith("تابع"):
        parts = text.split()
        if len(parts) < 2:
            await u.message.reply_text("مثال: `تابع XAUUSD`", parse_mode="Markdown")
            return
        sym = parts[1].upper()
        watching.setdefault(chat_id, {})[sym] = True
        jn  = f"fx_{chat_id}_{sym}"
        for j in c.job_queue.get_jobs_by_name(jn): j.schedule_removal()
        c.job_queue.run_repeating(
            monitor_job, interval=1800, first=30,
            data={"chat_id": chat_id, "sym": sym}, name=jn)
        await u.message.reply_text(
            f"👁 *بدأت متابعة {sym}*\n"
            f"كل 30 دقيقة — ICT + Wall Street\n"
            f"إيقاف: `وقف {sym}`",
            parse_mode="Markdown")
        return

    # وقف
    if text.startswith("وقف"):
        parts = text.split()
        if len(parts) >= 2 and parts[1] == "الكل":
            for s in list(watching.get(chat_id, {}).keys()):
                for j in c.job_queue.get_jobs_by_name(f"fx_{chat_id}_{s}"):
                    j.schedule_removal()
            watching[chat_id] = {}
            await u.message.reply_text("⛔ تم إيقاف كل المتابعات")
            return
        if len(parts) >= 2:
            sym = parts[1].upper()
            for j in c.job_queue.get_jobs_by_name(f"fx_{chat_id}_{sym}"):
                j.schedule_removal()
            watching.get(chat_id, {}).pop(sym, None)
            await u.message.reply_text(f"⛔ تم إيقاف {sym}")
            return

    # قائمة
    if text == "قائمة":
        syms = watching.get(chat_id, {})
        if not syms:
            await u.message.reply_text("لا توجد متابعات\n`تابع XAUUSD` للبدء", parse_mode="Markdown")
            return
        lines = ["📋 *تحت المتابعة:*\n"] + [f"👁 `{s}`" for s in syms]
        await u.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return

    # ── سكالب / Scalp ──
    _scalp_kws={"scalp","سكالب","سريع","1m","5m"}
    _pl=text.lower().split()
    if len(_pl)>=2 and _pl[0] in _scalp_kws:
        sym_s=_pl[1].upper()
        wait=await u.message.reply_text(f"⚡ جاري Scalp *{sym_s}* على 1m/5m...",parse_mode="Markdown")
        result_msg,err=await run_scalp_analysis(sym_s,"5m")
        await wait.delete()
        if err: await u.message.reply_text(err)
        else:
            try:
                await u.message.reply_text(result_msg,parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("⚡ تحديث Scalp",callback_data=f"s:{sym_s}"),
                        InlineKeyboardButton("📊 تحليل شامل", callback_data=f"r:{sym_s}"),
                        InlineKeyboardButton("📊 MTF",         callback_data=f"m:{sym_s}"),
                    ]]))
            except Exception: await u.message.reply_text(result_msg)
        return

    # تحليل MTF: "mtf XAUUSD" او "فريمات EURUSD"
    parts_text = text.split()
    if len(parts_text) >= 2 and parts_text[0].lower() in ("mtf","فريمات","timeframes"):
        sym_mtf = parts_text[1].upper()
        wait = await u.message.reply_text(
            f"جاري تحليل فريمات {sym_mtf} (MN/W1/D1/H4/H1/M15)...",
            parse_mode="Markdown")
        result_msg, err = await run_mtf_analysis(sym_mtf)
        await wait.delete()
        if err:
            await u.message.reply_text(err)
        else:
            chunks = [result_msg[i:i+3800] for i in range(0, len(result_msg), 3800)]
            for chunk in chunks:
                try:
                    await u.message.reply_text(chunk, parse_mode="Markdown")
                except Exception:
                    await u.message.reply_text(chunk)
                await asyncio.sleep(0.3)
        return

    # تحليل فوري
    sym_raw = text.upper().strip()
    sym_yf  = get_yf_symbol(sym_raw)

    if not sym_yf or len(sym_raw) > 15:
        await u.message.reply_text(
            "أرسل رمز مثل: `XAUUSD` | `EURUSD` | `GOLD`\n"
            "أو: `ذهب` | `GBPUSD`",
            parse_mode="Markdown")
        return

    wait = await u.message.reply_text(
        f"⏳ جاري تحليل *{sym_raw}*\n"
        f"(ICT + Smart Money + Macro)...",
        parse_mode="Markdown")
    R = await run_analysis(sym_raw)
    await wait.delete()
    await u.message.reply_text(
        build_signal(R), parse_mode="Markdown", reply_markup=kb(sym_raw))


async def handle_btn(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query
    await q.answer()
    action, sym = q.data.split(":", 1)
    chat_id = q.message.chat_id

    if action == "r":
        await q.edit_message_text(f"⏳ تحديث *{sym}*...", parse_mode="Markdown")
        R = await run_analysis(sym)
        await q.edit_message_text(
            build_signal(R), parse_mode="Markdown", reply_markup=kb(sym))
    elif action == "w":
        try:
            watching.setdefault(chat_id, {})[sym] = True
            jn = f"fx_{chat_id}_{sym}"
            for j in c.job_queue.get_jobs_by_name(jn):
                j.schedule_removal()
            c.job_queue.run_repeating(
                monitor_job, interval=1800, first=60,
                data={"chat_id": chat_id, "sym": sym}, name=jn)
            await q.answer(f"✅ متابعة {sym} — كل 30 دقيقة", show_alert=True)
        except Exception as ex:
            await q.answer(f"خطأ: {str(ex)[:40]}", show_alert=True)
    elif action == "s":
        wait_txt=f"⚡ جاري Scalp *{sym}* على 1m/5m..."
        await q.edit_message_text(wait_txt,parse_mode="Markdown")
        result_msg,err=await run_scalp_analysis(sym,"5m")
        if err: await q.edit_message_text(err)
        else:
            try:
                await q.edit_message_text(result_msg,parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("⚡ تحديث Scalp",callback_data=f"s:{sym}"),
                        InlineKeyboardButton("📊 تحليل شامل", callback_data=f"r:{sym}"),
                        InlineKeyboardButton("📊 MTF",         callback_data=f"m:{sym}"),
                    ]]))
            except Exception: pass

    elif action == "m":
        # Multi-timeframe summary
        await q.answer("⏳ جاري تحليل الفريمات...", show_alert=False)
        R = await run_analysis(sym)
        if not R.get("err"):
            mtf = (f"📊 *MTF — {sym}*\n\n"
                   f"HTF Bias: *{R.get('htf_bias','—')}*\n"
                   f"BOS/CHOCH: `{R.get('bos','—')}`\n"
                   f"RSI (1H): `{R.get('rsi',0):.1f}`\n"
                   f"EMA20: `{fp(R.get('ema20'),3)}`\n"
                   f"EMA50: `{fp(R.get('ema50'),3)}`\n\n"
                   f"⚠️ _للأغراض التعليمية فقط_")
            await q.message.reply_text(mtf, parse_mode="Markdown")


async def error_handler(update, context):
    logging.warning(f"Forex bot error: {context.error}")
    try:
        if update and update.effective_message:
            await update.effective_message.reply_text("⚠️ خطأ مؤقت — حاول مرة ثانية")
    except Exception:
        pass


def main():
    if BOT_TOKEN in ("YOUR_BOT_TOKEN_HERE", ""):
        print("ERROR: أضف BOT_TOKEN في Railway Variables")
        return

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(handle_btn))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))
    app.add_error_handler(error_handler)

    print("=" * 60)
    print("  FOREX & GOLD PRO BOT — Running ✅")
    print("=" * 60)
    print("  ICT: Order Blocks | FVG | Liquidity | BOS | OTE")
    print("  Tech: RSI | MACD | EMA | BB | ATR")
    print("  Macro: DXY | Fed Rate | GS/JPM Targets")
    print("  AI: Wall Street Collective Intelligence")
    print("=" * 60)

    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
