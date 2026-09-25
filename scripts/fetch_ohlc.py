"""Fetch daily / 5-min / 1-min OHLC + volume from Kite (Zerodha) into
Kanida_Falcon/db/kanida.db, with SELF-HEALING coverage.

Hardened vs. the first version:
  * per-chunk retry with exponential backoff (transient errors no longer
    silently drop a 60/100-day window);
  * a gap-REPAIR pass that finds missing trading days by SET DIFFERENCE against
    each symbol's own daily bars (so it heals INTERIOR holes the max(bar_time)
    resume logic could never reach), then re-fetches only those day-ranges;
  * a coverage REPORT (rows / distinct days / range / missing / short days)
    printed per symbol x timeframe at the end.

Auth is reused from the engine project (config/.env KITE_API_KEY + freshest
token in the kite_tokens DB) — no credentials live here.

Usage:
    python fetch_ohlc.py SYM1 SYM2 ...      # fetch + repair + report these
    python fetch_ohlc.py                    # use DEFAULT_SYMBOLS
    python fetch_ohlc.py --report-only ...  # only cross-check coverage
    python fetch_ohlc.py --start 2022-01-01 # override history floor

Kite per-request day caps:  day <= 2000, 5minute <= 100, minute <= 60.
"""
import os, sys, sqlite3, time, argparse
from datetime import datetime, date, timedelta
from pathlib import Path

# Paths follow the machine (2026-09-25 Mac move): KANIDA_ENGINE_ROOT, else ~/Kanida/engine; the DB sits beside this repo.
ENGINE_ROOT = Path(os.environ.get("KANIDA_ENGINE_ROOT", str(Path.home() / "Kanida" / "engine")))
sys.path.insert(0, str(ENGINE_ROOT / "universe_engine"))
sys.path.insert(0, str(ENGINE_ROOT / "backend"))
from engine.data_fetch import get_latest_access_token, RateLimiter  # noqa: E402
from kiteconnect import KiteConnect  # noqa: E402

DB = Path(os.environ.get("KANIDA_DB", str(Path(__file__).resolve().parents[1] / "db" / "kanida.db")))
DEFAULT_SYMBOLS = ["ADANIENT", "NETWEB", "FORCEMOT"]
START = date(2022, 1, 1)
END = date.today()

# interval -> (kite_interval, table, max_days_per_request)
PLAN = [
    ("day",     "ohlc_daily", 2000),
    ("5minute", "ohlc_5min",  100),
    ("minute",  "ohlc_1min",  60),
]
INTRADAY = {"ohlc_5min", "ohlc_1min"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS {t} (
    symbol           TEXT    NOT NULL,
    instrument_token INTEGER,
    bar_time         TEXT    NOT NULL,
    open  REAL, high REAL, low REAL, close REAL,
    volume INTEGER,
    PRIMARY KEY (symbol, bar_time)
);
CREATE INDEX IF NOT EXISTS idx_{t}_sym_dt ON {t}(symbol, bar_time);
"""


class KiteAuthError(RuntimeError):
    """Fatal: token/api_key rejected — retrying won't help."""


# ── client / resolution ───────────────────────────────────────────────────────

def get_client():
    api_key = os.environ["KITE_API_KEY"]
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(get_latest_access_token())
    kite.profile()  # fail fast if the token is dead
    return kite


def resolve_tokens(kite, symbols):
    nse = {i["tradingsymbol"]: i for i in kite.instruments("NSE") if i.get("segment") == "NSE"}
    resolved, missing = {}, []
    for s in symbols:
        if s in nse:
            resolved[s] = (nse[s]["instrument_token"], nse[s].get("name", ""))
        else:
            missing.append(s)
    return resolved, missing


# ── fetch primitives (with retry) ──────────────────────────────────────────────

def fetch_chunk(kite, token, cs, ce, interval, rl, retries=5):
    """Fetch one window. Retries transient errors with backoff; raises
    KiteAuthError on token problems; returns [] only when Kite truly has none."""
    delay = 1.0
    last = None
    for attempt in range(retries):
        rl.wait()
        try:
            return kite.historical_data(
                token,
                datetime.combine(cs, datetime.min.time()),
                datetime.combine(ce, datetime.max.time()),
                interval)
        except Exception as e:
            msg = str(e)
            if "TokenException" in msg or "access_token" in msg.lower() or "api_key" in msg.lower():
                raise KiteAuthError(msg)
            last = msg
            time.sleep(delay)
            delay = min(delay * 2, 16)
    raise RuntimeError(f"chunk {cs}..{ce} failed after {retries} tries: {last}")


def _chunks(s, e, maxd):
    cur = s
    while cur <= e:
        ce = min(cur + timedelta(days=maxd - 1), e)
        yield cur, ce
        cur = ce + timedelta(days=1)


def _store(con, table, sym, token, bars):
    if not bars:
        return 0
    payload = [(sym, token, b["date"].strftime("%Y-%m-%d %H:%M:%S"),
                float(b["open"]), float(b["high"]), float(b["low"]),
                float(b["close"]), int(b["volume"])) for b in bars]
    con.executemany(
        f"INSERT OR IGNORE INTO {table} "
        f"(symbol, instrument_token, bar_time, open, high, low, close, volume) "
        f"VALUES (?,?,?,?,?,?,?,?)", payload)
    con.commit()
    return len(payload)


def _last_day(con, table, sym):
    r = con.execute(f"SELECT max(bar_time) FROM {table} WHERE symbol=?", (sym,)).fetchone()
    return date.fromisoformat(r[0][:10]) if r and r[0] else None


def _days_present(con, table, sym):
    return {r[0] for r in con.execute(
        f"SELECT DISTINCT substr(bar_time,1,10) FROM {table} WHERE symbol=?", (sym,)).fetchall()}


def _group_ranges(days_sorted):
    """List of ISO dates -> list of (start_date, end_date) contiguous-ish blocks
    (merging across weekend/holiday gaps so we issue few requests)."""
    if not days_sorted:
        return []
    ds = [date.fromisoformat(d) for d in days_sorted]
    ranges, s, prev = [], ds[0], ds[0]
    for d in ds[1:]:
        if (d - prev).days <= 5:            # bridge normal non-trading gaps
            prev = d
        else:
            ranges.append((s, prev)); s = prev = d
    ranges.append((s, prev))
    return ranges


# ── per-symbol forward fetch + repair ──────────────────────────────────────────

def forward_fetch(con, kite, sym, token, interval, table, maxd, start, end, rl):
    resume = _last_day(con, table, sym)
    fstart = max(start, resume + timedelta(days=1)) if resume else start
    if fstart > end:
        return
    for cs, ce in _chunks(fstart, end, maxd):
        _store(con, table, sym, token, fetch_chunk(kite, token, cs, ce, interval, rl))


def repair(con, kite, sym, token, interval, table, maxd, ref_days, rl, max_passes=3):
    """Heal interior holes: (ref_days present in the symbol's DAILY series) minus
    (days present in this table) -> re-fetch those ranges. Returns remaining set."""
    for _ in range(max_passes):
        present = _days_present(con, table, sym)
        missing = sorted(d for d in ref_days if d not in present)
        if not missing:
            return set()
        for cs, ce in _group_ranges(missing):
            for c2s, c2e in _chunks(cs, ce, maxd):
                _store(con, table, sym, token, fetch_chunk(kite, token, c2s, c2e, interval, rl))
    present = _days_present(con, table, sym)
    return {d for d in ref_days if d not in present}


# ── coverage report ────────────────────────────────────────────────────────────

def report(con, symbols):
    print("\n" + "=" * 78)
    print("COVERAGE REPORT  (cross-check: symbol x timeframe)")
    print("=" * 78)
    market_days = {r[0] for r in con.execute(
        "SELECT DISTINCT substr(bar_time,1,10) FROM ohlc_daily").fetchall()}
    ok = True
    for sym in symbols:
        daily_days = _days_present(con, "ohlc_daily", sym)
        if not daily_days:
            print(f"\n{sym}: NO DATA"); ok = False; continue
        lo, hi = min(daily_days), max(daily_days)
        # daily vs market calendar (flag suspensions/holes within active window)
        cal = {d for d in market_days if lo <= d <= hi}
        d_missing = sorted(cal - daily_days)
        print(f"\n{sym}   active {lo} .. {hi}")
        for _, table, _m in PLAN:
            n = con.execute(f"SELECT count(*) FROM {table} WHERE symbol=?", (sym,)).fetchone()[0]
            rng = con.execute(f"SELECT min(bar_time),max(bar_time) FROM {table} WHERE symbol=?",
                              (sym,)).fetchone()
            days = _days_present(con, table, sym)
            if table == "ohlc_daily":
                miss = d_missing
            else:
                miss = sorted(daily_days - days)      # intraday day missing vs own daily
            tag = "OK" if not miss else f"MISSING {len(miss)} day(s): {miss[:6]}"
            if miss:
                ok = False
            short = ""
            if table in INTRADAY and days:
                cnt = con.execute(
                    f"SELECT COUNT(*) FROM (SELECT substr(bar_time,1,10) d, COUNT(*) n "
                    f"FROM {table} WHERE symbol=? GROUP BY d HAVING n < "
                    f"{'370' if table=='ohlc_1min' else '74'})", (sym,)).fetchone()[0]
                short = f"  short_days={cnt}"
            print(f"   {table:11} rows={n:>8} days={len(days):>4} "
                  f"[{(rng[0] or '')[:16]} .. {(rng[1] or '')[:16]}]  {tag}{short}")
    print("\n" + ("ALL SYMBOLS x TIMEFRAMES: COMPLETE [OK]" if ok
                  else "INCOMPLETE - see MISSING lines above"))
    print("=" * 78)
    return ok


# ── main ────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("symbols", nargs="*", default=[])
    ap.add_argument("--start", default=None)
    ap.add_argument("--report-only", action="store_true")
    args = ap.parse_args()
    symbols = args.symbols or DEFAULT_SYMBOLS
    start = date.fromisoformat(args.start) if args.start else START

    DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DB), timeout=90.0)
    for _, table, _m in PLAN:
        con.executescript(SCHEMA.format(t=table))
    con.commit()

    if args.report_only:
        report(con, symbols); con.close(); return

    kite = get_client()
    resolved, missing = resolve_tokens(kite, symbols)
    if missing:
        print(f"!! UNRESOLVED tradingsymbols (skipped): {missing}")
    for s, (tok, name) in resolved.items():
        print(f"resolved {s:11} token={tok:<9} {name}")
    rl = RateLimiter(rps=3.0)

    for interval, table, maxd in PLAN:
        print(f"\n--- {table} ({interval}) ---")
        for sym, (token, _name) in resolved.items():
            forward_fetch(con, kite, sym, token, interval, table, maxd, start, END, rl)
            print(f"   fetched {sym}")

    # repair pass (needs daily as the per-symbol reference calendar)
    print("\n--- gap repair (set-difference vs each symbol's daily) ---")
    for interval, table, maxd in PLAN:
        if table not in INTRADAY:
            continue
        for sym, (token, _name) in resolved.items():
            ref = _days_present(con, "ohlc_daily", sym)
            left = repair(con, kite, sym, token, interval, table, maxd, ref, rl)
            if left:
                print(f"   !! {sym} {table}: {len(left)} day(s) unrecoverable (Kite has none): {sorted(left)[:6]}")
            else:
                print(f"   {sym} {table}: no gaps")

    report(con, list(resolved.keys()))
    con.close()


if __name__ == "__main__":
    main()
