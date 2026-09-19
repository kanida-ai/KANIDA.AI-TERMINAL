"""
Extend OHLC lookback to Kite's MAX floors, into kanida.db (the research cache):
  ohlc_daily -> 2013-01-01   |   ohlc_5min / ohlc_1min -> 2017-01-01
Fills [floor -> each symbol's current earliest bar), per symbol/table. Idempotent
(INSERT OR IGNORE) and RESUMABLE (window shrinks to what's still missing each run).
AUTO-REFRESHES the Kite token mid-job (Playwright run_auth_attempt) so it survives the
~7:30am IST daily expiry. Stocks listed later simply return nothing before listing.

Run: PYTHONIOENCODING=utf-8 python backfill_maxlookback.py   (log: db/backfill_maxlookback.log)
"""
import os, sys, sqlite3, time, asyncio
from datetime import datetime, date, timedelta
from pathlib import Path
from dotenv import load_dotenv

ENG = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
load_dotenv(ENG / "config" / ".env")
sys.path.insert(0, str(ENG / "universe_engine")); sys.path.insert(0, str(ENG / "backend"))
from engine.data_fetch import get_kite, get_latest_access_token, RateLimiter  # noqa

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
LOG = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\backfill_maxlookback.log")
FLOORS = {"ohlc_daily": date(2013, 1, 1), "ohlc_5min": date(2015, 2, 1), "ohlc_1min": date(2015, 2, 1)}
PLAN = [("day", "ohlc_daily", 2000), ("5minute", "ohlc_5min", 100), ("minute", "ohlc_1min", 60)]


def log(m):
    line = f"{datetime.now():%H:%M:%S} {m}"; print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f: f.write(line + "\n")


def refresh_token():
    """Recover a working Kite session. Step 1 (cheap, no thrash): re-read the latest DB token —
    another actor (the engine's auth worker) may have minted a fresh valid one. Step 2 (last
    resort): mint via the auto-auth bot. Returns a validated kite or None."""
    try:
        k = get_kite(); k.profile()
        log("  recovered via latest DB token (no new mint)"); return k
    except Exception:
        pass
    try:
        from services import zerodha_auto_auth as zaa
        log("  minting fresh token (auto-auth bot) ...")
        res = asyncio.run(zaa.run_auth_attempt(attempt_of_day=0, trigger_kind="manual"))
        if getattr(res, "status", "") == "success":
            k = get_kite(); log(f"  minted OK: {k.profile().get('user_name')}"); return k
        log(f"  mint FAILED: {getattr(res,'error_code',None)} {(getattr(res,'error_detail','') or '')[:80]}")
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
                raise RuntimeError("AUTH:" + msg)
            if "insufficient permission" in msg.lower():
                return []                      # permanent per-instrument restriction — skip, don't waste retries
            last = msg; time.sleep(delay); delay = min(delay * 2, 16)
    log(f"    chunk {cs}..{ce} failed: {last}"); return []


_I64MAX = 9_223_372_036_854_775_807


def safe_vol(v):                                          # some ETFs/indices report garbage volumes > int64
    try:
        return max(0, min(int(v or 0), _I64MAX))
    except (ValueError, TypeError, OverflowError):
        return 0


def chunks(s, e, maxd):
    cur = s
    while cur <= e:
        ce = min(cur + timedelta(days=maxd - 1), e); yield cur, ce; cur = ce + timedelta(days=1)


def main():
    LOG.write_text("", encoding="utf-8")
    kite = get_kite()
    log(f"token OK: {kite.profile().get('user_name')}")
    nse = kite.instruments("NSE")
    eq = {i["tradingsymbol"]: i["instrument_token"] for i in nse if i.get("segment") == "NSE"}
    idx = {i["tradingsymbol"]: i["instrument_token"] for i in nse if i.get("segment") == "INDICES"}
    con = sqlite3.connect(str(DB), timeout=120)
    syms = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM ohlc_daily ORDER BY symbol").fetchall()]
    tokmap = {s: (eq.get(s) or idx.get(s)) for s in syms if (eq.get(s) or idx.get(s))}
    log(f"symbols: {len(syms)} | tokens resolved: {len(tokmap)} | floors {FLOORS}")
    rl = RateLimiter(rps=3.0)
    for interval, table, maxd in PLAN:
        START = FLOORS[table]; log(f"=== {table} ({interval}) floor {START} ===")
        added0 = con.total_changes
        earliest = {r[0]: r[1] for r in
                    con.execute(f"SELECT symbol, min(bar_time) FROM {table} GROUP BY symbol").fetchall()}
        for n, s in enumerate(sorted(tokmap), 1):
            token = tokmap[s]
            row = earliest.get(s)
            end = (date.fromisoformat(row[:10]) - timedelta(days=1)) if row else date.today()
            if START > end:
                continue
            for cs, ce in chunks(START, end, maxd):
                try:
                    bars = fetch_chunk(kite, token, cs, ce, interval, rl)
                except RuntimeError:
                    nk = refresh_token()
                    if nk is None:
                        log("AUTH refresh failed — stopping (resumable next run)"); con.commit(); con.close(); return
                    kite = nk
                    try:
                        bars = fetch_chunk(kite, token, cs, ce, interval, rl)
                    except RuntimeError:
                        log("AUTH still failing after refresh — stopping"); con.commit(); con.close(); return
                if not bars:
                    continue
                payload = [(s, token, b["date"].strftime("%Y-%m-%d %H:%M:%S"),
                            float(b["open"]), float(b["high"]), float(b["low"]), float(b["close"]),
                            safe_vol(b.get("volume"))) for b in bars]
                con.executemany(
                    f"INSERT OR IGNORE INTO {table} (symbol,instrument_token,bar_time,open,high,low,close,volume) "
                    f"VALUES (?,?,?,?,?,?,?,?)", payload)
                con.commit()
            if n % 25 == 0:
                log(f"  {table}: {n}/{len(tokmap)} (last {s}) | +{con.total_changes - added0:,} rows so far")
        r = con.execute(f"SELECT min(bar_time), max(bar_time), count(*) FROM {table}").fetchone()
        log(f"  {table} DONE: added ~{con.total_changes - added0:,} | range {r[0]} .. {r[1]} | rows {r[2]:,}")
    con.close()
    log("MAX-LOOKBACK BACKFILL COMPLETE")


if __name__ == "__main__":
    main()
