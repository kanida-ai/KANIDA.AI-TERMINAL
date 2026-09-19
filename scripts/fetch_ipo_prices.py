"""
Fetch from-listing price history (daily -> 5min -> 1min) for the IPO cohort into kanida.db.
Runs AFTER the max-lookback backfill (sequential on the single Kite key -> no contention).
Per symbol START = its listing_date (forward fetch to today). Resumable (INSERT OR IGNORE +
resume from each symbol's latest bar), rate-limited, auto-refreshes the token across expiry.
Also upserts a minimal instrument_labels row (is_ipo=1, listing_date) so downstream tooling
recognises each IPO. Marks ipo_cohort.prices_fetched=1 when a symbol's daily is in.

Run: PYTHONIOENCODING=utf-8 python fetch_ipo_prices.py   (log: db/fetch_ipo_prices.log)
"""
import os, sys, sqlite3, time, asyncio
from datetime import datetime, date, timedelta
from pathlib import Path
from dotenv import load_dotenv

ENG = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
load_dotenv(ENG / "config" / ".env")
sys.path.insert(0, str(ENG / "universe_engine")); sys.path.insert(0, str(ENG / "backend"))
from engine.data_fetch import get_kite, RateLimiter  # noqa

KDB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
LOG = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\fetch_ipo_prices.log")
PLAN = [("day", "ohlc_daily", 2000), ("5minute", "ohlc_5min", 100), ("minute", "ohlc_1min", 60)]
_I64 = 9_223_372_036_854_775_807


def log(m):
    line = f"{datetime.now():%H:%M:%S} {m}"; print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f: f.write(line + "\n")


def safe_vol(v):
    try: return max(0, min(int(v or 0), _I64))
    except (ValueError, TypeError, OverflowError): return 0


def refresh_token():
    # step 1 (cheap, no thrash): re-read latest DB token (engine's auth worker may have refreshed it)
    try:
        k = get_kite(); k.profile(); log("  recovered via latest DB token (no mint)"); return k
    except Exception:
        pass
    try:                                                  # step 2 (last resort): mint fresh
        from services import zerodha_auto_auth as zaa
        log("  minting fresh token ...")
        res = asyncio.run(zaa.run_auth_attempt(attempt_of_day=0, trigger_kind="manual"))
        if getattr(res, "status", "") == "success":
            k = get_kite(); log(f"  minted OK: {k.profile().get('user_name')}"); return k
        log(f"  mint FAILED: {getattr(res,'error_code',None)}")
    except Exception as e:
        log(f"  mint EXC: {str(e)[:120]}")
    return None


def fetch_chunk(kite, token, cs, ce, interval, rl, retries=5):
    delay = 1.0; last = None
    for _ in range(retries):
        rl.wait()
        try:
            return kite.historical_data(token, datetime.combine(cs, datetime.min.time()),
                                        datetime.combine(ce, datetime.max.time()), interval)
        except Exception as e:
            msg = str(e)
            if "TokenException" in msg or "access_token" in msg.lower() or "api_key" in msg.lower():
                raise RuntimeError("AUTH")
            if "insufficient permission" in msg.lower():
                return []                      # permanent per-instrument restriction — skip, don't waste retries
            last = msg; time.sleep(delay); delay = min(delay * 2, 16)
    log(f"    chunk {cs}..{ce} failed: {last}"); return []


def chunks(s, e, maxd):
    cur = s
    while cur <= e:
        ce = min(cur + timedelta(days=maxd - 1), e); yield cur, ce; cur = ce + timedelta(days=1)


def main():
    LOG.write_text("", encoding="utf-8")
    kite = get_kite(); log(f"token OK: {kite.profile().get('user_name')}")
    nse = kite.instruments("NSE")
    eq = {i["tradingsymbol"]: i["instrument_token"] for i in nse if i.get("segment") == "NSE"}
    con = sqlite3.connect(KDB, timeout=120)
    have_cols = [c[1] for c in con.execute("PRAGMA table_info(instrument_labels)").fetchall()]
    if "is_ipo" not in have_cols: con.execute("ALTER TABLE instrument_labels ADD COLUMN is_ipo INTEGER DEFAULT 0")
    if "listing_date" not in have_cols: con.execute("ALTER TABLE instrument_labels ADD COLUMN listing_date TEXT")
    con.commit()
    sc = sqlite3.connect(SNR)
    cohort = sc.execute("SELECT symbol, listing_date FROM ipo_cohort WHERE in_kanida_db=0 ORDER BY listing_date").fetchall()
    sc.close()
    todo = [(s, date.fromisoformat(ld)) for s, ld in cohort if s in eq]
    unres = [s for s, _ in cohort if s not in eq]
    log(f"cohort needing fetch: {len(cohort)} | resolved on Kite: {len(todo)} | unresolved: {len(unres)}")
    rl = RateLimiter(rps=3.0); today = date.today()
    for interval, table, maxd in PLAN:
        log(f"=== {table} ({interval}) ===")
        for n, (s, ld) in enumerate(todo, 1):
            token = eq[s]
            row = con.execute(f"SELECT max(bar_time) FROM {table} WHERE symbol=?", (s,)).fetchone()[0]
            start = (date.fromisoformat(row[:10]) + timedelta(days=1)) if row else ld
            if start > today:
                continue
            for cs, ce in chunks(start, today, maxd):
                try:
                    bars = fetch_chunk(kite, token, cs, ce, interval, rl)
                except RuntimeError:
                    nk = refresh_token()
                    if nk is None: log("AUTH refresh failed — stopping (resumable)"); con.commit(); con.close(); return
                    kite = nk
                    try: bars = fetch_chunk(kite, token, cs, ce, interval, rl)
                    except RuntimeError: log("AUTH still failing — stopping"); con.commit(); con.close(); return
                if not bars: continue
                con.executemany(
                    f"INSERT OR IGNORE INTO {table} (symbol,instrument_token,bar_time,open,high,low,close,volume) "
                    f"VALUES (?,?,?,?,?,?,?,?)",
                    [(s, token, b["date"].strftime("%Y-%m-%d %H:%M:%S"), float(b["open"]), float(b["high"]),
                      float(b["low"]), float(b["close"]), safe_vol(b.get("volume"))) for b in bars])
                con.commit()
            if table == "ohlc_daily":
                con.execute("INSERT OR IGNORE INTO instrument_labels (symbol,exchange,instrument_type,is_ipo,listing_date) "
                            "VALUES (?,?,?,1,?)", (s, "NSE", "EQ", ld.isoformat()))
                con.execute("UPDATE instrument_labels SET is_ipo=1, listing_date=? WHERE symbol=?", (ld.isoformat(), s))
                sc2 = sqlite3.connect(SNR); sc2.execute("UPDATE ipo_cohort SET prices_fetched=1 WHERE symbol=?", (s,))
                sc2.commit(); sc2.close(); con.commit()
            if n % 25 == 0:
                log(f"  {table}: {n}/{len(todo)} (last {s})")
        r = con.execute(f"SELECT min(bar_time),max(bar_time),count(*) FROM {table}").fetchone()
        log(f"  {table} DONE: range {r[0]} .. {r[1]} | rows {r[2]:,}")
    con.close()
    log("IPO PRICE FETCH COMPLETE")


if __name__ == "__main__":
    main()
