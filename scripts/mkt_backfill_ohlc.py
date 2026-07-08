"""Historical 1-min OHLCV backfill -> mkt_ohlc_1min (mkt_ cluster). MAX lookback for
cash + index (Kite serves ~2018+); resumable + idempotent, so the same script does the
one-time deep backfill AND the daily EOD incremental (each instrument fetches from its
last-stored date). Run post-market to avoid contending with the live poller.

Usage: `python mkt_backfill_ohlc.py`            (deep/incremental to yesterday)
       `python mkt_backfill_ohlc.py --start 2024-01-01`  (override floor)
"""
import os, sys, sqlite3, time, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta
from pathlib import Path

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
sys.path.insert(0, str(ROOT / "universe_engine")); sys.path.insert(0, str(ROOT / "backend"))
from engine.data_fetch import RateLimiter, get_kite, get_latest_access_token
from mkt_poller import INDICES

DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
GLOBAL_START = "2018-01-01"
IDXN = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS mkt_ohlc_1min (
    symbol TEXT, segment TEXT, bar_time TEXT,
    open REAL, high REAL, low REAL, close REAL, volume INTEGER, oi INTEGER,
    PRIMARY KEY (symbol, bar_time)
);
CREATE INDEX IF NOT EXISTS idx_mktohlc_sym_dt ON mkt_ohlc_1min(symbol, bar_time);
"""


def resolve_tokens():
    """cash EQ tokens for our liquid universe + index tokens (segment INDICES)."""
    kite = get_kite()
    nse = kite.instruments("NSE")
    eq = {i["tradingsymbol"]: i["instrument_token"] for i in nse if i.get("segment") == "NSE"}
    idx = {i["tradingsymbol"]: i["instrument_token"] for i in nse if i.get("segment") == "INDICES"}
    con = sqlite3.connect(str(DB))
    cash = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM ohlc_1min").fetchall()]
    con.close()
    univ = []
    for s in cash:
        if s in IDXN:
            continue
        if s in eq:
            univ.append((s, "CASH", eq[s]))
    for s in INDICES:
        if s in idx:
            univ.append((s, "INDEX", idx[s]))
    return univ


def _chunks(s, e, maxd=60):
    cur = s
    while cur <= e:
        ce = min(cur + timedelta(days=maxd - 1), e)
        yield cur, ce
        cur = ce + timedelta(days=1)


def _existing_max(db, symbol):
    con = sqlite3.connect(db, timeout=60)
    r = con.execute("SELECT max(bar_time) FROM mkt_ohlc_1min WHERE symbol=?", (symbol,)).fetchone()
    con.close()
    return r[0][:10] if r and r[0] else None


def _fetch_one(args):
    sym, seg, token, start_iso, end_iso, db, rl = args
    out = {"sym": sym, "rows": 0, "status": "ok"}
    mx = _existing_max(db, sym)                       # resume from last stored date
    start = max(date.fromisoformat(start_iso), (date.fromisoformat(mx) + timedelta(days=1)) if mx else date.fromisoformat(start_iso))
    end = date.fromisoformat(end_iso)
    if start > end:
        out["status"] = "up-to-date"; return out
    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=os.environ["KITE_API_KEY"]); kite.set_access_token(get_latest_access_token())
    except Exception as e:
        out["status"] = "auth_error"; out["error"] = str(e); return out
    payload = []
    for cs, ce in _chunks(start, end):
        rl.wait()
        try:
            rows = kite.historical_data(token, datetime.combine(cs, datetime.min.time()),
                                        datetime.combine(ce, datetime.max.time()), "minute")
        except Exception as e:
            if "TokenException" in str(e) or "expired" in str(e).lower():
                out["status"] = "auth_error"; return out
            time.sleep(1.5); continue
        for r in rows:
            try:
                payload.append((sym, seg, r["date"].strftime("%Y-%m-%d %H:%M:%S"),
                                r.get("open"), r.get("high"), r.get("low"), r.get("close"),
                                int(r.get("volume") or 0), int(r.get("oi") or 0)))
            except Exception:
                continue
    if payload:
        con = sqlite3.connect(db, timeout=90)
        con.executemany("INSERT OR IGNORE INTO mkt_ohlc_1min VALUES (?,?,?,?,?,?,?,?,?)", payload)
        con.commit(); con.close()
    out["rows"] = len(payload)
    return out


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--start", default=GLOBAL_START)
    ap.add_argument("--workers", type=int, default=10); a = ap.parse_args()
    con = sqlite3.connect(str(DB)); con.executescript(SCHEMA); con.commit(); con.close()
    univ = resolve_tokens()
    end = (date.today() - timedelta(days=1)).isoformat()
    print(f"[backfill] {len(univ)} instruments ({sum(1 for u in univ if u[1]=='CASH')} cash + "
          f"{sum(1 for u in univ if u[1]=='INDEX')} index) | {a.start} -> {end} | resumable", flush=True)
    rl = RateLimiter(rps=3.0)
    args = [(s, seg, tok, a.start, end, str(DB), rl) for s, seg, tok in univ]
    done = rows = uptodate = autherr = 0; t0 = time.time()
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = {ex.submit(_fetch_one, x): x[0] for x in args}
        for f in as_completed(futs):
            r = f.result(); done += 1; rows += r["rows"]
            if r["status"] == "up-to-date": uptodate += 1
            if r["status"] == "auth_error": autherr += 1
            if done % 25 == 0:
                print(f"  [{done}/{len(args)}] rows={rows:,} uptodate={uptodate} elapsed={time.time()-t0:.0f}s", flush=True)
            if autherr >= 3:
                print("[backfill] FATAL 3 auth errors — refresh Kite token"); break
    print(f"[backfill] DONE {done} instruments | rows={rows:,} | up-to-date={uptodate} | {(time.time()-t0)/60:.1f} min")
    con = sqlite3.connect(str(DB))
    d1, d2, n, ns = con.execute("SELECT min(bar_time),max(bar_time),count(*),count(distinct symbol) FROM mkt_ohlc_1min").fetchone()
    print(f"[backfill] mkt_ohlc_1min: {d1} .. {d2} | {n:,} rows | {ns} instruments")
    con.close()


if __name__ == "__main__":
    main()
