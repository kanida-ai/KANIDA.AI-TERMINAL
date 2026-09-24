"""Fetch REAL 1-min OHLC + Volume + Open Interest for every ACTIVE NSE stock-futures
contract, into ohlc_futures_1min. Kite only exposes currently-active contracts (~3
monthly expiries/underlying, each with data from its introduction ~3 months before
expiry), so this yields the trailing ~3 months of real futures 1-min. Per-contract
(symbol=underlying, tradingsymbol=contract, expiry) so continuous/rolled series and
basis/roll analysis can be built later. Idempotent (INSERT OR IGNORE). Index futures
excluded (stocks only).
"""
import os, sqlite3, time, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta
from pathlib import Path

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
ENG = ROOT / "universe_engine"
sys.path.insert(0, str(ENG)); sys.path.insert(0, str(ROOT / "backend"))
from engine.data_fetch import RateLimiter, get_latest_access_token
from engine.oi_fetch import get_active_fut_contracts

DB = ENG / "data" / "db" / "kanida_universe.db"
START, END = "2026-04-01", "2026-07-04"
IDX = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS ohlc_futures_1min (
    symbol         TEXT NOT NULL,   -- underlying (e.g. RELIANCE)
    tradingsymbol  TEXT NOT NULL,   -- contract (e.g. RELIANCE26JULFUT)
    expiry         TEXT NOT NULL,
    bar_time       TEXT NOT NULL,
    open REAL, high REAL, low REAL, close REAL,
    volume INTEGER, oi INTEGER,
    PRIMARY KEY (tradingsymbol, bar_time)
);
CREATE INDEX IF NOT EXISTS idx_fut1m_sym_dt ON ohlc_futures_1min(symbol, bar_time);
"""


def _chunks(s, e, maxd=60):
    cur = s
    while cur <= e:
        ce = min(cur + timedelta(days=maxd - 1), e)
        yield cur, ce
        cur = ce + timedelta(days=1)


def _fetch_one(args):
    sym, c, start_iso, end_iso, db_path, rl = args
    out = {"symbol": sym, "tradingsymbol": c["tradingsymbol"], "rows": 0, "status": "ok"}
    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=os.environ["KITE_API_KEY"])
        kite.set_access_token(get_latest_access_token())
    except Exception as e:
        out["status"] = "auth_error"; out["error"] = str(e); return out
    payload = []
    for cs, ce in _chunks(date.fromisoformat(start_iso), date.fromisoformat(end_iso)):
        rl.wait()
        try:
            rows = kite.historical_data(
                c["instrument_token"],
                datetime.combine(cs, datetime.min.time()),
                datetime.combine(ce, datetime.max.time()),
                "minute", oi=True)
        except Exception as e:
            msg = str(e)
            if "TokenException" in msg or "expired" in msg.lower():
                out["status"] = "auth_error"; out["error"] = msg; return out
            time.sleep(1.5); continue
        for r in rows:
            try:
                payload.append((sym, c["tradingsymbol"], c["expiry"],
                                r["date"].strftime("%Y-%m-%d %H:%M:%S"),
                                float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]),
                                int(r["volume"]), int(r.get("oi", 0))))
            except (KeyError, TypeError, ValueError):
                continue
    if payload:
        con = sqlite3.connect(db_path, timeout=90.0)
        con.executemany(
            "INSERT OR IGNORE INTO ohlc_futures_1min "
            "(symbol,tradingsymbol,expiry,bar_time,open,high,low,close,volume,oi) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)", payload)
        con.commit(); con.close()
    else:
        out["status"] = "empty"
    out["rows"] = len(payload)
    return out


def main():
    con = sqlite3.connect(str(DB)); con.executescript(SCHEMA); con.commit()
    fo = [r[0] for r in con.execute("SELECT symbol FROM fo_stock_master").fetchall()]
    con.close()
    stocks = [s for s in fo if s not in IDX]
    print(f"[fut1m] discovering active contracts for {len(stocks)} F&O stocks...", flush=True)
    by = get_active_fut_contracts(stocks)
    rl = RateLimiter(rps=3.0)
    args = [(sym, c, START, END, str(DB), rl) for sym in stocks for c in by.get(sym, [])]
    print(f"[fut1m] {len(args)} contract-fetches queued | window {START}..{END} | 3 rps", flush=True)
    summ = {"done": 0, "rows": 0, "auth": 0, "empty": 0}
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_fetch_one, a): a[1]["tradingsymbol"] for a in args}
        for f in as_completed(futs):
            try:
                r = f.result()
            except Exception as e:
                r = {"status": "error", "rows": 0, "error": str(e)}
            summ["done"] += 1; summ["rows"] += r.get("rows", 0)
            if r["status"] == "auth_error": summ["auth"] += 1
            if r["status"] == "empty": summ["empty"] += 1
            if summ["done"] % 50 == 0:
                print(f"  [{summ['done']}/{len(args)}] rows={summ['rows']:,} elapsed={time.time()-t0:.0f}s", flush=True)
            if summ["auth"] >= 3:
                print("[fut1m] FATAL: 3+ auth errors — refresh Kite token."); break
    print(f"\n[fut1m] DONE contracts={summ['done']} rows={summ['rows']:,} empty={summ['empty']} "
          f"auth_err={summ['auth']} in {(time.time()-t0)/60:.1f}min")
    con = sqlite3.connect(str(DB))
    d1, d2, n, nc, ns = con.execute(
        "SELECT min(bar_time),max(bar_time),count(*),count(distinct tradingsymbol),count(distinct symbol) "
        "FROM ohlc_futures_1min").fetchone()
    print(f"[fut1m] ohlc_futures_1min: {d1}..{d2} | {n:,} rows | {nc} contracts | {ns} underlyings")
    con.close()


if __name__ == "__main__":
    main()
