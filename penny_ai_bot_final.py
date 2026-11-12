# -*- coding: utf-8 -*-
"""
PENNY SIGNAL AI BOT  (FINAL, tuzatilgan versiya)
aiogram v3 + yfinance + Claude API + sqlite3

Tuzilma:
  * Raqamlar (kirish, stop, targetlar, R/R, ishonch) KOD tomonidan hisoblanadi -> AI uydirib bera olmaydi.
  * Claude faqat o'zbekcha matn yozadi: kompaniya nima ish qilishi, sabablar, xavflar.
  * Filtrlar: narx, RVOL, likvidlik ($ hajm), spread, pump&dump himoyasi, trend (VWAP/EMA).
  * Status: LONG / SHORT (pump exhaustion) / WATCHLIST (filtrdan o'tmaganlar, yig'ma xabarda).

O'rnatish:
    pip install -U aiogram yfinance pandas anthropic tzdata curl_cffi

Sozlash: shu papkada .env fayl yarating (namuna: .env.example):
    BOT_TOKEN=...
    ANTHROPIC_API_KEY=...        (ixtiyoriy; bo'lmasa lokal matn ishlatiladi)
    ALLOWED_CHATS=123456         (ixtiyoriy; faqat shu chat ID lar ishlata oladi)
Ishga tushirish:  python penny_ai_bot_final.py   yoki   run_bot.bat (avto-qayta ishga tushadi)

Buyruqlar:
    /start, /help        yordam
    /signal XYZ          bitta aksiya tahlili (ticker yozsangiz ham bo'ladi)
    /market              butun bozorni hozir skan qilish
    /auto on|off         bot o'zi topib signal yuborsin / to'xtatsin
    /risk 5000 [1.0]     depozit va bitta savdoga risk % (0.25-2) -> dona hisoblab beradi
    /status              sozlamalar
"""
import asyncio
import json
import logging
import math
import os
import re
import sqlite3
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from logging.handlers import RotatingFileHandler
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
from curl_cffi import requests as cffi_requests
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import Message

def _load_env(path: str = ".env") -> None:
    """Oddiy .env o'qish (qo'shimcha kutubxonasiz). Muhit o'zgaruvchisi ustun turadi."""
    full = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
    if not os.path.exists(full):
        return
    for line in open(full, encoding="utf-8-sig"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env()

# ======================= SOZLAMALAR =======================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
AI_MODEL = os.getenv("AI_MODEL", "claude-haiku-5-5")  # sifat kerak bo'lsa: claude-sonnet-5-5
ALLOWED = {int(x) for x in os.getenv("ALLOWED_CHATS", "").split(",") if x.strip().lstrip("-").isdigit()}
DB = "penny_ai_bot.db"

SCAN_INTERVAL = 300        # avto-skan oralig'i (soniya)
MAX_CANDIDATES = 40        # har skanda tekshiriladigan aksiyalar
CONCURRENCY = 5            # bir vaqtda nechta so'rov
ONLY_MARKET_HOURS = True   # faqat AQSh 04:00-20:00 ET (Du-Ju)
SEND_WATCHLIST_DIGEST = True   # filtrdan o'tmaganlar yig'ma WATCHLIST xabarida
WATCHLIST_MAX = 6          # yig'ma xabarda maksimal aksiya

# ---- Filtrlar (promptdagi qoidalar) ----
MIN_PRICE, MAX_PRICE = 0.50, 5.00
MIN_RVOL = 2.0             # 200%; kuchli: 3.0 (300%)
MIN_DOLLAR_VOL = 250_000   # kunlik $ aylanma (likvidlik)
MAX_SPREAD_PCT = 1.0
PUMP_DAY_PCT = 100.0       # kunlik +100%
PUMP_VWAP_PCT = 25.0       # VWAP dan +25% uzoqlik
EXHAUST_PULLBACK = 5.0     # kun cho'qqisidan 5% pastga qaytish = tükeniş alomati
DEFAULT_RISK_PCT = 1.0

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(),
              RotatingFileHandler("penny_bot.log", maxBytes=1_000_000, backupCount=3, encoding="utf-8")])
log = logging.getLogger("penny")
try:
    ET = ZoneInfo("America/New_York")
except Exception:  # Windows'da tzdata kerak
    raise SystemExit("Xato: 'pip install tzdata' ni bajaring va qayta ishga tushiring.")
CACHE_TTL = 90  # soniya: bir aksiya uchun Yahoo'ga qayta so'rov yubormaslik
_cache: dict = {}
TICKER_RE = re.compile(r"^[A-Z]{1,5}$")
DISCLAIMER = "Bu moliyaviy maslahat emas. Signalni kirishdan oldin brokerda tekshiring."

# Yahoo bloklamasligi uchun Chrome brauzer sessiyasini yaratamiz
session = cffi_requests.Session(impersonate="chrome")

# ======================= RENDER PORT HILYASI =======================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is running successfully!")
    def log_message(self, format, *args):
        pass

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    server.serve_forever()


# ======================= DB =======================
def db():
    con = sqlite3.connect(DB)
    con.execute("CREATE TABLE IF NOT EXISTS auto(chat_id INTEGER PRIMARY KEY, on_off INTEGER)")
    con.execute("CREATE TABLE IF NOT EXISTS sent(chat_id INTEGER, key TEXT, day TEXT, PRIMARY KEY(chat_id,key,day))")
    con.execute("CREATE TABLE IF NOT EXISTS cfg(chat_id INTEGER PRIMARY KEY, deposit REAL, risk_pct REAL)")
    return con


def get_cfg(chat_id: int):
    with db() as c:
        r = c.execute("SELECT deposit, risk_pct FROM cfg WHERE chat_id=?", (chat_id,)).fetchone()
    return r if r else (None, DEFAULT_RISK_PCT)


# ======================= MA'LUMOT VA HISOB =======================
def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()


def fetch_data(ticker: str) -> dict | None:
    """Keshlangan + qayta urinadigan o'ram (Yahoo vaqtincha xato/limit bersa)."""
    hit = _cache.get(ticker)
    if hit and time.time() - hit[0] < CACHE_TTL:
        return hit[1]
    for attempt in range(3):
        try:
            d = _fetch_data(ticker)
            if d:
                _cache[ticker] = (time.time(), d)
            return d
        except Exception as e:
            log.warning("%s: urinish %d xato: %s", ticker, attempt + 1, e)
            time.sleep(1.5 * (attempt + 1))
    return None


def _fetch_data(ticker: str) -> dict | None:
    """Ma'lumot yig'adi, filtrlaydi va signal darajalarini hisoblaydi (sinxron; to_thread ichida chaqiriladi)."""
    t = yf.Ticker(ticker, session=session)
    d5 = t.history(period="5d", interval="5m", prepost=True)
    d1 = t.history(period="3mo", interval="1d")
    if d5.empty or len(d1) < 10:
        return None

    last_ts = d5.index[-1]
    today = d5[d5.index.date == last_ts.date()]
    if len(today) < 3:
        return None
    price = float(today["Close"].iloc[-1])

    # VWAP / EMA
    tp = (today["High"] + today["Low"] + today["Close"]) / 3
    cumv = today["Volume"].cumsum()
    vwap = float((tp * today["Volume"]).cumsum().iloc[-1] / max(cumv.iloc[-1], 1))
    ema9 = float(_ema(d5["Close"], 9).iloc[-1])
    ema20 = float(_ema(d5["Close"], 20).iloc[-1])

    # Kunlik tarix: bugungi qator bor-yo'qligini aniqlash
    if d1.index[-1].date() == last_ts.date():
        hist, prev_close = d1.iloc[:-1], float(d1["Close"].iloc[-2])
        d1_today_vol = float(d1["Volume"].iloc[-1])
    else:
        hist, prev_close = d1, float(d1["Close"].iloc[-1])
        d1_today_vol = 0.0
    avg_vol = float(hist["Volume"].tail(20).mean())
    day_vol = max(float(today["Volume"].sum()), d1_today_vol)  # pre-market hajmi ham hisobda
    rvol = day_vol / avg_vol if avg_vol else 0.0
    dollar_vol = day_vol * price
    day_chg = (price / prev_close - 1) * 100

    # Kompaniya ma'lumoti
    info = {}
    try:
        info = t.info or {}
    except Exception:
        pass
    bid, ask = info.get("bid") or 0, info.get("ask") or 0
    spread_pct = ((ask - bid) / ask * 100) if (bid and ask and ask > 0) else None

    news = []
    try:
        for n in (t.news or [])[:3]:
            c = n.get("content", n)
            if c.get("title"):
                news.append(c["title"])
    except Exception:
        pass

    # Strukturaviy darajalar
    recent = today.tail(40)
    resistance = float(recent["High"].iloc[:-1].max()) if len(recent) > 3 else price
    support = float(recent["Low"].min())
    local_low = float(today["Low"].tail(12).min())
    local_high = float(today["High"].tail(12).max())
    day_high = float(today["High"].max())
    pullback = (1 - price / day_high) * 100 if day_high else 0.0
    vwap_dist = (price / vwap - 1) * 100 if vwap else 0.0

    pump = day_chg >= PUMP_DAY_PCT or vwap_dist >= PUMP_VWAP_PCT
    exhausted = pump and pullback >= EXHAUST_PULLBACK

    # Signal turi
    if price >= resistance and rvol >= MIN_RVOL:
        stype = "Breakout"
    elif price > vwap and abs(price / vwap - 1) < 0.02 and ema9 > ema20:
        stype = "VWAP Pullback"
    elif price <= support * 1.02:
        stype = "Support Bounce"
    elif ema9 > ema20 and price > vwap:
        stype = "Momentum Continuation"
    else:
        stype = "Aniq emas"

    d = {
        "ticker": ticker, "company": info.get("shortName") or ticker,
        "sector": info.get("sector") or "noma'lum", "industry": info.get("industry") or "noma'lum",
        "business_summary": (info.get("longBusinessSummary") or "ma'lumot yo'q")[:700],
        "market_cap": info.get("marketCap"), "float_shares": info.get("floatShares"),
        "price": round(price, 4), "day_change_pct": round(day_chg, 2), "rvol": round(rvol, 2),
        "avg_volume_20d": int(avg_vol), "day_volume": int(day_vol), "dollar_volume": int(dollar_vol),
        "vwap": round(vwap, 4), "ema9": round(ema9, 4), "ema20": round(ema20, 4),
        "spread_pct": None if spread_pct is None else round(spread_pct, 3),
        "support": round(support, 4), "resistance": round(resistance, 4),
        "pullback_from_high_pct": round(pullback, 2), "pump": pump, "exhausted": exhausted,
        "signal_type": stype, "news": news,
        "data_time": last_ts.astimezone(ET).strftime("%H:%M ET"),
        "delay_min": round((datetime.now(last_ts.tzinfo) - last_ts).total_seconds() / 60),
    }
    return evaluate(d, local_low, local_high)


def evaluate(d: dict, local_low: float, local_high: float) -> dict:
    """Filtrlar -> status -> darajalar -> ishonch reytingi."""
    p, rvol = d["price"], d["rvol"]
    hard = []
    if not (MIN_PRICE <= p <= MAX_PRICE):
        hard.append(f"narx ${MIN_PRICE}-${MAX_PRICE} oralig'idan tashqarida")
    if rvol < MIN_RVOL:
        hard.append(f"hajm yetarli emas (RVOL {rvol:.1f}x < {MIN_RVOL}x)")
    if d["dollar_volume"] < MIN_DOLLAR_VOL:
        hard.append(f"likvidlik past (${d['dollar_volume']:,} <${MIN_DOLLAR_VOL:,})")
    if d["spread_pct"] is not None and d["spread_pct"] > MAX_SPREAD_PCT:
        hard.append(f"spread keng ({d['spread_pct']}% > {MAX_SPREAD_PCT}%)")

    notes = []
    if d["spread_pct"] is None:
        notes.append("spread noma'lum (bozor yopiq yoki bid/ask yo'q) — limit order ishlating")
    if d["delay_min"] > 20 and market_open_now():
        notes.append(f"ma'lumot {d['delay_min']} daqiqa kechikkan")
    if not d["news"]:
        notes.append("yangilik topilmadi: harakat retail hype/texnik bo'lishi mumkin")

    status = "WATCHLIST"
    trend_ok = p > d["vwap"] and d["ema9"] > d["ema20"]
    if hard:
        pass
    elif d["pump"] and not d["news"]:
        if d["exhausted"]:
            status = "SHORT"
        else:
            hard.append("yangiliksiz pump davom etyapti: LONG taqiqlangan, tükeniş kutilmoqda")
    elif trend_ok:
        status = "LONG"
        if d["pump"]:
            notes.append("narx keskin o'sgan: yangilik bor, lekin pullback xavfi yuqori")
    else:
        hard.append("trend tasdiqlanmagan (VWAP/EMA 9-20 ostida)")

    d["failures"], d["notes"], d["status"] = hard, notes, status

    # Darajalar
    if status == "LONG":
        stop = min(local_low, d["vwap"], d["ema20"]) * 0.99
        stop = max(min(stop, p * 0.97), p * 0.88)   # kamida ~3%, ko'pi bilan 12%
        risk = p - stop
        d["entry"] = (round(p * 0.995, 4), round(p * 1.005, 4))
        d["stop"] = round(stop, 4)
        d["targets"] = [round(p + risk * m, 4) for m in (1.5, 2.5, 4.0)]
        d["rr"] = round(2.5, 1)
        d["risk_per_share"] = round(risk, 4)
    elif status == "SHORT":
        stop = min(max(local_high, p * 1.03) * 1.01, p * 1.12)
        risk = stop - p
        d["entry"] = (round(p * 0.995, 4), round(p * 1.005, 4))
        d["stop"] = round(stop, 4)
        d["targets"] = [round(max(p - risk * m, p * 0.2), 4) for m in (1.5, 2.5, 4.0)]
        d["rr"] = round(2.5, 1)
        d["risk_per_share"] = round(risk, 4)
        d["signal_type"] = "Pump Exhaustion / Pullback"

    # Ishonch reytingi (1-10)
    if status == "LONG":
        c = 3 + (2 if rvol >= 3 else 1) + (p > d["vwap"]) + (d["ema9"] > d["ema20"]) \
            + bool(d["news"]) + (d["signal_type"] == "Breakout")
        if d["spread_pct"] is not None and d["spread_pct"] > 0.5:
            c -= 1
        if d["pump"]:
            c -= 2
    elif status == "SHORT":
        c = 4 + (d["pullback_from_high_pct"] >= 8) + (rvol >= 3) + (p < d["vwap"])
    else:
        c = 2 + (rvol >= 2) + bool(d["news"])
    d["confidence"] = max(1, min(10, int(c)))
    return d


# ======================= AI MATNI =======================
AI_SYSTEM = """Siz professional kvant-treyder va penny aksiyalar (Small/Micro-Cap) bo'yicha texnik tahlil
va risk-menejment ekspertisiz. Sizga bozor ma'lumotlari JSON ko'rinishida beriladi. Raqamlar (kirish,
stop, targetlar) allaqachon hisoblangan: ularni O'ZGARTIRMANG va yangi narxlar o'ylab topmang.

Faqat quyidagi JSON ni qaytaring (boshqa matn yoki ``` belgilarisiz), hammasi sodda o'zbek tilida:
{
 "company": "Kompaniya nima ish qiladi, qaysi sektorda, qanday daromad topadi (2-3 qisqa gap)",
 "catalyst": "Yangilik bo'lsa uning kuchi (FDA, shartnoma, daromad...), bo'lmasa 'yangilik yo'q, hajm/retail hype'",
 "why": "Nima uchun signal shakllandi: price action, VWAP/EMA, hajm (2-3 qator)",
 "risks": "Asosiy xavflar: volatillik, dilution (yangi aksiya chiqarish), spread, halt (2-3 qator)",
 "conf_adj": -2 dan +1 gacha butun son (ishonch reytingiga tuzatish; shubha bo'lsa manfiy)
}
Status SHORT bo'lsa 'risks' ga penny aksiyani shortlash qiyinligini (borrow, squeeze, halt) yozing."""


def fallback_texts(d: dict) -> dict:
    return {
        "company": f"{d['sector']} / {d['industry']}. {d['business_summary'][:260]}",
        "catalyst": d["news"][0] if d["news"] else "yangilik yo'q, hajm/retail hype bo'lishi mumkin",
        "why": f"{d['signal_type']}: RVOL {d['rvol']}x, kun {d['day_change_pct']:+}%, "
               f"VWAP ${d['vwap']}, EMA9/20 {d['ema9']}/{d['ema20']}.",
        "risks": "Yuqori volatillik, dilution (yangi aksiya chiqarish) va halt xavfi, keng spread ehtimoli.",
        "conf_adj": 0,
    }


async def ai_texts(d: dict) -> dict:
    fb = fallback_texts(d)
    if not ANTHROPIC_KEY:
        return fb
    try:
        from anthropic import AsyncAnthropic
        payload = {k: d[k] for k in (
            "ticker", "company", "sector", "industry", "business_summary", "market_cap", "price",
            "day_change_pct", "rvol", "day_volume", "vwap", "ema9", "ema20", "spread_pct", "support",
            "resistance", "pullback_from_high_pct", "pump", "exhausted", "signal_type", "status",
            "failures", "news")}
        msg = await AsyncAnthropic(api_key=ANTHROPIC_KEY).messages.create(
            model=AI_MODEL, max_tokens=700, system=AI_SYSTEM,
            messages=[{"role": "user", "content": json.dumps(payload, ensure_ascii=False)}])
        raw = "".join(b.text for b in msg.content if b.type == "text").strip()
        raw = re.sub(r"^```(?:json)?|```$", "", raw, flags=re.M).strip()
        out = json.loads(raw)
        for k in ("company", "catalyst", "why", "risks"):
            if not isinstance(out.get(k), str) or not out[k].strip():
                out[k] = fb[k]
        out["conf_adj"] = max(-2, min(1, int(out.get("conf_adj", 0))))
        return out
    except Exception as e:
        log.warning("AI xato (%s): lokal matn ishlatildi", e)
        return fb


# ======================= FORMAT =======================
def money(x: float) -> str:
    return f"${x:,.2f}" if x >= 1 else f"${x:.4f}"


def pct(a: float, p: float) -> str:
    return f"{(a / p - 1) * 100:+.1f}%"


def format_signal(d: dict, ai: dict, deposit=None, risk_pct=DEFAULT_RISK_PCT) -> str:
    p, st = d["price"], d["status"]
    conf = max(1, min(10, d["confidence"] + ai.get("conf_adj", 0)))
    icon = {"LONG": "🟢", "SHORT": "🔴", "WATCHLIST": "🟡"}[st]
    head = (f"📊 SIGNAL STATUS: {icon} {st}\n"
            f"- Aksiya Ticker'i: {d['ticker']} ({d['company']})\n"
            f"- Joriy Narx: {money(p)}  ({d['day_change_pct']:+}% bugun)\n"
            f"- Signal Turi: {d['signal_type']}\n"
            f"- Ishonch Reytingi: {conf}/10\n")
    comp = (f"\n🏢 KOMPANIYA HAQIDA:\n- Sektor: {d['sector']} / {d['industry']}\n"
            f"- Nima ish qiladi: {ai['company']}\n- Katalizator: {ai['catalyst']}\n")
    data = (f"\n📈 BOZOR: RVOL {d['rvol']}x | Hajm {d['day_volume']:,} | VWAP {money(d['vwap'])} | "
            f"EMA9/20 {money(d['ema9'])}/{money(d['ema20'])}"
            + (f" | Spread {d['spread_pct']}%" if d["spread_pct"] is not None else "") + "\n")

    if st == "WATCHLIST":
        body = ("\n🚫 Hozircha kirish YO'Q. Sabablar:\n" + "".join(f"- {f}\n" for f in d["failures"]))
    else:
        e1, e2 = d["entry"]
        t1, t2, t3 = d["targets"]
        stop = d["stop"]
        sizing = "depozitning 1-2% dan oshmasin"
        if deposit and d["risk_per_share"] > 0:
            shares = math.floor(deposit * risk_pct / 100 / d["risk_per_share"])
            shares = max(0, min(shares, math.floor(deposit / p)))
            sizing = f"{risk_pct}% risk = ~{shares:,} dona (~${shares * p:,.0f})"
        body = (f"\n🎯 KIRISH VA MAQSADLAR:\n- Kirish Zonasi: {money(e1)} - {money(e2)}\n"
                f"- Target 1 (Konservativ): {money(t1)} ({pct(t1, p)})\n"
                f"- Target 2 (O'rta muddatli): {money(t2)} ({pct(t2, p)})\n"
                f"- Target 3 (Agressiv / Moonbag): {money(t3)} ({pct(t3, p)})\n"
                f"\n🛑 RISKNI BOSHQARISH:\n- Stop-Loss: {money(stop)} ({pct(stop, p)}) — qat'iy\n"
                f"- Risk/Reward Nisbati: 1:{d['rr']}\n- Tavsiya etilgan Kapital ulushi: {sizing}\n"
                f"- Reja: T1 da pozitsiyaning ~1/2 qismini yoping, stopni breakevenga ko'taring.\n")
    warn = (f"\n⚠️ SABABLAR VA OGOHLANTIRISHLAR:\n- {ai['why']}\n- Xavf: {ai['risks']}\n"
            + "".join(f"- {n}\n" for n in d["notes"])
            + f"\n🕒 Ma'lumot: {d['data_time']} (~{d['delay_min']} daq. oldin)\n{DISCLAIMER}")
    return (head + comp + data + body + warn)[:4000]


def format_digest(items: list[dict]) -> str:
    lines = ["🟡 WATCHLIST (filtrdan o'tmagan, kirish yo'q):"]
    for d in items:
        lines.append(f"• {d['ticker']} {money(d['price'])} | {d['day_change_pct']:+}% | RVOL {d['rvol']}x | "
                     f"{d['failures'][0] if d['failures'] else 'trend kutilmoqda'}")
    return "\n".join(lines)


# ======================= SKANER =======================
def market_open_now() -> bool:
    now = datetime.now(ET)
    return now.weekday() < 5 and 4 <= now.hour < 20


def discover_candidates() -> list[str]:
    """Bozordan penny aksiyalarni o'zi topadi (Yahoo screener)."""
    syms: list[str] = []
    try:
        from yfinance import EquityQuery
        q = EquityQuery("and", [
            EquityQuery("eq", ["region", "us"]),
            EquityQuery("is-in", ["exchange", "NMS", "NYQ", "ASE"]),
            EquityQuery("btwn", ["intradayprice", MIN_PRICE, MAX_PRICE]),
            EquityQuery("gt", ["percentchange", 5]),
            EquityQuery("gt", ["dayvolume", 500000]),
        ])
        res = yf.screen(q, sortField="percentchange", sortAsc=False, size=100)
        syms += [x["symbol"] for x in res.get("quotes", [])]
    except Exception as e:
        log.warning("custom screener xato: %s", e)
    for name in ("day_gainers", "most_actives", "small_cap_gainers"):
        try:
            for x in yf.screen(name, count=100).get("quotes", []):
                if MIN_PRICE <= (x.get("regularMarketPrice") or 0) <= MAX_PRICE:
                    syms.append(x["symbol"])
        except Exception as e:
            log.warning("screener %s xato: %s", name, e)
    out = []
    for s in syms:
        if not TICKER_RE.match(s) or (len(s) == 5 and s[-1] in "WRU"):  # warrant/right/unit emas
            continue
        if s not in out:
            out.append(s)
    return out[:MAX_CANDIDATES]


async def scan_market() -> list[dict]:
    cands = await asyncio.to_thread(discover_candidates)
    log.info("Nomzodlar (%d): %s", len(cands), cands)
    sem = asyncio.Semaphore(CONCURRENCY)

    async def one(tk):
        async with sem:
            try:
                return await asyncio.to_thread(fetch_data, tk)
            except Exception as e:
                log.warning("%s xato: %s", tk, e)

    res = [d for d in await asyncio.gather(*[one(t) for t in cands]) if d]
    res.sort(key=lambda d: (d["status"] == "WATCHLIST", -d["confidence"], -d["rvol"]))
    return res


# ======================= BOT =======================
bot = Bot(BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher()
if ALLOWED:
    dp.message.filter(F.chat.id.in_(ALLOWED))
_tasks: set = set()


@dp.message(Command("start", "help"))
async def start(m: Message):
    await m.answer(
        "👋 Penny Signal AI Bot\n\n"
        "/signal XYZ — bitta aksiya tahlili (yoki shunchaki ticker yozing)\n"
        "/market — butun bozorni hozir skan qilish\n"
        "/auto on|off — bot o'zi topib signal yuborsin\n"
        "/risk 5000 [1.0] — depozit va risk % (dona hisoblash uchun)\n"
        "/status — sozlamalar\n\n"
        f"Filtrlar: ${MIN_PRICE}-${MAX_PRICE}, RVOL≥{MIN_RVOL}x, $hajm≥{MIN_DOLLAR_VOL:,}, "
        f"spread≤{MAX_SPREAD_PCT}%, pump&dump himoyasi, VWAP/EMA trend.\n"
        "Status: 🟢 LONG, 🔴 SHORT (pump tükenişi), 🟡 WATCHLIST.")


@dp.message(Command("signal"))
async def signal(m: Message, command: CommandObject):
    tk = (command.args or "").strip().upper()
    if not TICKER_RE.match(tk):
        return await m.answer("Misol: /signal SNDL")
    await analyze_and_send(m, tk)


async def analyze_and_send(m: Message, tk: str):
    await m.answer(f"⏳ {tk} tahlil qilinmoqda...")
    d = await asyncio.to_thread(fetch_data, tk)
    if not d:
        return await m.answer(f"❌ {tk}: ma'lumot topilmadi.")
    dep, rp = get_cfg(m.chat.id)
    await m.answer(format_signal(d, await ai_texts(d), dep, rp))


@dp.message(Command("market"))
async def market_cmd(m: Message):
    await m.answer("🔍 Bozor skan qilinmoqda (1-2 daqiqa)...")
    res = await scan_market()
    dep, rp = get_cfg(m.chat.id)
    sig = [d for d in res if d["status"] != "WATCHLIST"]
    for d in sig:
        await m.answer(format_signal(d, await ai_texts(d), dep, rp))
    wl = [d for d in res if d["status"] == "WATCHLIST"][:WATCHLIST_MAX]
    if SEND_WATCHLIST_DIGEST and wl:
        await m.answer(format_digest(wl))
    if not res:
        await m.answer("Nomzod topilmadi (bozor yopiq yoki Yahoo javob bermadi).")
    elif not sig:
        await m.answer("Hozircha LONG/SHORT signal yo'q.")


@dp.message(Command("auto"))
async def auto(m: Message, command: CommandObject):
    v = 1 if (command.args or "").strip().lower() == "on" else 0
    with db() as c:
        c.execute("INSERT OR REPLACE INTO auto VALUES(?,?)", (m.chat.id, v))
    await m.answer(f"🔔 Avto-skan yoqildi (har {SCAN_INTERVAL // 60} daqiqada)." if v else "🔕 Avto-skan o'chirildi.")


@dp.message(Command("risk"))
async def risk(m: Message, command: CommandObject):
    parts = (command.args or "").replace(",", ".").split()
    try:
        dep = float(parts[0])
        rp = max(0.25, min(2.0, float(parts[1]))) if len(parts) > 1 else DEFAULT_RISK_PCT
        assert dep > 0
    except Exception:
        return await m.answer("Misol: /risk 5000 yoki /risk 5000 1.5  (risk 0.25–2% oralig'ida)")
    with db() as c:
        c.execute("INSERT OR REPLACE INTO cfg VALUES(?,?,?)", (m.chat.id, dep, rp))
    await m.answer(f"✅ Depozit ${dep:,.0f}, bitta savdoga risk {rp}%.")


@dp.message(Command("status"))
async def status(m: Message):
    with db() as c:
        a = c.execute("SELECT on_off FROM auto WHERE chat_id=?", (m.chat.id,)).fetchone()
    dep, rp = get_cfg(m.chat.id)
    await m.answer(f"Avto-skan: {'ON' if a and a[0] else 'OFF'}\n"
                   f"Depozit: {'$' + format(dep, ',.0f') if dep else 'kiritilmagan (/risk)'}\n"
                   f"Risk: {rp}%\nAI: {'Claude (' + AI_MODEL + ')' if ANTHROPIC_KEY else 'lokal matn'}\n"
                   f"Bozor ochiq (ET 04-20): {'ha' if market_open_now() else 'yo‘q'}")


@dp.message(F.text.regexp(r"^\$?[A-Za-z]{1,5}$"))
async def plain_ticker(m: Message):
    await analyze_and_send(m, m.text.upper().lstrip("$"))


def was_sent(cid: int, key: str, day: str) -> bool:
    with db() as c:
        return c.execute("SELECT 1 FROM sent WHERE chat_id=? AND key=? AND day=?", (cid, key, day)).fetchone() is not None


def mark_sent(cid: int, key: str, day: str) -> None:
    with db() as c:
        c.execute("INSERT OR IGNORE INTO sent VALUES(?,?,?)", (cid, key, day))
        c.execute("DELETE FROM sent WHERE day < date(?, '-3 day')", (day,))  # eski yozuvlarni tozalash


async def auto_loop():
    while True:
        await asyncio.sleep(SCAN_INTERVAL)
        try:
            if ONLY_MARKET_HOURS and not market_open_now():
                continue
            with db() as c:
                chats = [r[0] for r in c.execute("SELECT chat_id FROM auto WHERE on_off=1")]
            if not chats:
                continue
            day = datetime.now(ET).strftime("%Y-%m-%d")
            res = await scan_market()
            ai_cache: dict[str, dict] = {}
            for cid in chats:
                try:
                    dep, rp = get_cfg(cid)
                    for d in (x for x in res if x["status"] != "WATCHLIST"):
                        key = f"{d['ticker']}:{d['status']}"
                        if was_sent(cid, key, day):
                            continue
                        if d["ticker"] not in ai_cache:
                            ai_cache[d["ticker"]] = await ai_texts(d)
                        await bot.send_message(cid, "🚨 YANGI SIGNAL\n\n" + format_signal(d, ai_cache[d["ticker"]], dep, rp))
                        mark_sent(cid, key, day)  # faqat muvaffaqiyatli yuborilgach belgilanadi
                        await asyncio.sleep(0.3)
                    if SEND_WATCHLIST_DIGEST:
                        wl = [x for x in res if x["status"] == "WATCHLIST" and x["rvol"] >= 1.5
                              and not was_sent(cid, f"{x['ticker']}:W", day)][:WATCHLIST_MAX]
                        if wl:
                            await bot.send_message(cid, format_digest(wl))
                            for x in wl:
                                mark_sent(cid, f"{x['ticker']}:W", day)
                except Exception as e:
                    log.warning("chat %s ga yuborib bo'lmadi: %s", cid, e)
        except Exception as e:
            log.exception("auto_loop: %s", e)


async def main():
    if not bot:
        raise SystemExit("BOT_TOKEN topilmadi! .env fayliga BOT_TOKEN=... yozing.")
    db().close()
    
    # Render port talabini qondirish uchun veb-serverni alohida oqimda (thread) ishga tushiramiz
    threading.Thread(target=run_web_server, daemon=True).log = log
    
    task = asyncio.create_task(auto_loop())
    _tasks.add(task)
    log.info("Bot ishga tushdi. AI: %s", AI_MODEL if ANTHROPIC_KEY else "lokal")
    await dp.start_polling(bot, drop_pending_updates=True)


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
            break
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            log.exception("Bot qulab tushdi, 10 soniyadan keyin qayta ishga tushadi")
            time.sleep(10)
