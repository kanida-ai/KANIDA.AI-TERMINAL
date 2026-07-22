"""
Kite Connect 1-minute historical fetcher.

Multi-worker (thread pool, 10-48 workers). Kite caps the historical-data API at
~3-10 req/sec per account. The thread pool overlaps parsing/IO; the actual
throughput is rate-limited by Kite, not by us.

Usage:
    from engine.data_fetch import fetch_1m_for_symbols
    fetch_1m_for_symbols(con, symbols, months_back=6)
"""
from __future__ import annotations
import os, sqlite3, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional


# ── Env loader ────────────────────────────────────────────────────────────────

def _load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

_PROJ_ROOT = Path(__file__).resolve().parent.parent.parent
_load_env(_PROJ_ROOT / "config" / ".env")


# ── Token resolver: kite_tokens DB table is single source of truth ────────────

def get_latest_access_token(main_db: Path = None) -> str:
    """
    Resolve the freshest Kite access token. Order:
      1. kite_tokens table in main project DB (admin portal writes here)
      2. KITE_ACCESS_TOKEN env var (.env)
    Returns the token string. Raises if neither found.
    """
    import sqlite3
    from pathlib import Path as _Path
    # Respect KANIDA_DB_PATH (where services.kite_auth WRITES the fresh token).
    # The old hardcoded _PROJ_ROOT/data/db path is a laptop-relative path that
    # does NOT exist in the cloud container, so the token read silently fell back
    # to the stale KITE_ACCESS_TOKEN env placeholder -> TokenException on every
    # historical fetch. (cloud pipeline token-source fix)
    if main_db is None:
        _envdb = os.environ.get("KANIDA_DB_PATH")
        main_db = _Path(_envdb) if _envdb else (_PROJ_ROOT / "data" / "db" / "kanida_quant.db")
    else:
        main_db = _Path(main_db)
    if main_db.exists():
        try:
            con = sqlite3.connect(str(main_db))
            row = con.execute(
                "SELECT access_token FROM kite_tokens ORDER BY id DESC LIMIT 1"
            ).fetchone()
            con.close()
            if row and row[0]:
                return row[0]
        except Exception:
            pass
    tok = os.environ.get("KITE_ACCESS_TOKEN")
    if tok:
        return tok
    raise RuntimeError("No KITE_ACCESS_TOKEN found in kite_tokens table or env")


# ── Rate limiter (token bucket, thread-safe) ──────────────────────────────────

class RateLimiter:
    """Simple per-second rate limiter, thread-safe. Default: 5 req/sec."""
    def __init__(self, rps: float = 5.0):
        self.min_interval = 1.0 / rps
        self.lock = threading.Lock()
        self.next_ok = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            wait_for = self.next_ok - now
            if wait_for > 0:
                time.sleep(wait_for)
            self.next_ok = time.monotonic() + self.min_interval


# ── Kite client (lazy import) ─────────────────────────────────────────────────

def get_kite():
    from kiteconnect import KiteConnect
    api_key = os.environ.get("KITE_API_KEY")
    if not api_key:
        raise RuntimeError("Missing KITE_API_KEY in config/.env")
    token = get_latest_access_token()    # kite_tokens DB → .env fallback
    k = KiteConnect(api_key=api_key)
    k.set_access_token(token)
    return k


def get_instrument_tokens(kite, symbols: List[str]) -> Dict[str, int]:
    """Resolve NSE tradingsymbol → instrument_token for each requested symbol."""
    instruments = kite.instruments("NSE")
    by_sym = {i["tradingsymbol"]: i["instrument_token"]
              for i in instruments if i["segment"] == "NSE"}
    return {s: by_sym[s] for s in symbols if s in by_sym}


# ── Date pagination (Kite 1-min limit = 60 days per call) ─────────────────────

def _date_chunks(start: date, end: date, max_days: int = 60):
    """Yield (chunk_start, chunk_end) inclusive dates of at most max_days each."""
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=max_days - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


# ── Per-ticker fetch worker ───────────────────────────────────────────────────

def _fetch_one_symbol(args) -> Dict[str, int]:
    """Worker: fetch 1-min bars for one symbol over the requested window.
    Writes directly to SQLite under a process-local connection."""
    sym, token, start_iso, end_iso, db_path, rl = args
    if rl is None:
        rl = RateLimiter(rps=5.0)
    out = {"symbol": sym, "rows_fetched": 0, "rows_written": 0, "chunks": 0,
           "status": "ok", "error": None}

    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=os.environ["KITE_API_KEY"])
        # Resolve token: kite_tokens DB row first, .env fallback (matches main process)
        try:
            tok = get_latest_access_token()
        except Exception:
            tok = os.environ.get("KITE_ACCESS_TOKEN", "")
        kite.set_access_token(tok)
    except Exception as e:
        out["status"] = "auth_error"; out["error"] = str(e)
        return out

    con = sqlite3.connect(db_path, timeout=60.0)
    cur = con.cursor()

    rows_to_insert = []
    start_d = date.fromisoformat(start_iso)
    end_d   = date.fromisoformat(end_iso)

    for chunk_s, chunk_e in _date_chunks(start_d, end_d, max_days=60):
        rl.wait()
        try:
            raw = kite.historical_data(
                token,
                from_date=datetime.combine(chunk_s, datetime.min.time()),
                to_date=datetime.combine(chunk_e, datetime.max.time()),
                interval="minute",
            )
        except Exception as e:
            msg = str(e)
            if "TokenException" in msg or "InputException" in msg or "expired" in msg.lower():
                out["status"] = "auth_error"; out["error"] = msg
                con.close()
                return out
            # Transient error: small backoff and continue
            time.sleep(2.0)
            continue

        out["chunks"] += 1
        for r in raw:
            ts = r["date"].strftime("%Y-%m-%d %H:%M:%S")
            try:
                rows_to_insert.append((
                    sym, ts,
                    float(r["open"]), float(r["high"]),
                    float(r["low"]),  float(r["close"]),
                    int(r["volume"]),
                ))
            except (KeyError, TypeError, ValueError):
                continue
        out["rows_fetched"] += len(raw)

        # Flush periodically to avoid memory bloat
        if len(rows_to_insert) > 50_000:
            cur.executemany("""
                INSERT OR IGNORE INTO ohlc_1min
                    (symbol, bar_time, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, rows_to_insert)
            con.commit()
            out["rows_written"] += len(rows_to_insert)
            rows_to_insert = []

    if rows_to_insert:
        cur.executemany("""
            INSERT OR IGNORE INTO ohlc_1min
                (symbol, bar_time, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, rows_to_insert)
        con.commit()
        out["rows_written"] += len(rows_to_insert)

    con.close()
    return out


# ── Top-level driver ──────────────────────────────────────────────────────────

def fetch_1m_for_symbols(db_path: Path, symbols: List[str],
                          months_back: int = 6,
                          n_workers: int = 16,
                          rps: float = 5.0,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> Dict:
    """
    Fetch 1-min bars for each symbol.
    Window: explicit (start_date, end_date) ISO strings if given, else
    [today - months_back, today]. Writes to ohlc_1min in db_path.
    """
    n_workers = max(10, min(48, n_workers))
    print(f"[fetch1m] Resolving instrument tokens ...")
    kite = get_kite()
    tokens = get_instrument_tokens(kite, symbols)
    missing = [s for s in symbols if s not in tokens]
    if missing:
        print(f"[fetch1m] WARN: no token for {len(missing)} symbols: {missing[:10]}")

    if start_date and end_date:
        start_iso, end_iso = start_date, end_date
        print(f"[fetch1m] Window: {start_iso} -> {end_iso}  (explicit)")
    else:
        end_d   = date.today()
        start_d = end_d - timedelta(days=months_back * 31)
        start_iso, end_iso = start_d.isoformat(), end_d.isoformat()
        print(f"[fetch1m] Window: {start_iso} -> {end_iso}  ({months_back} months)")
    print(f"[fetch1m] Symbols to fetch: {len(tokens)}")
    print(f"[fetch1m] Workers: {n_workers}  ·  Rate: {rps} req/sec")

    rl = RateLimiter(rps=rps)
    args_list = [(sym, tok, start_iso, end_iso, str(db_path), rl)
                 for sym, tok in tokens.items()]

    summary = {"symbols_done": 0, "rows_total": 0, "auth_errors": 0,
               "details_per_symbol": {}}
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_fetch_one_symbol, a): a[0] for a in args_list}
        for f in as_completed(futs):
            sym = futs[f]
            try:
                r = f.result()
            except Exception as e:
                r = {"symbol": sym, "status": "error", "error": str(e),
                     "rows_fetched": 0, "rows_written": 0, "chunks": 0}
            summary["symbols_done"] += 1
            summary["rows_total"]   += r.get("rows_written", 0)
            if r.get("status") == "auth_error":
                summary["auth_errors"] += 1
            summary["details_per_symbol"][sym] = r
            elapsed = time.time() - t0
            print(f"  [{summary['symbols_done']:>3}/{len(tokens)}] {sym:<14} "
                  f"chunks={r.get('chunks',0):>2} rows={r.get('rows_written',0):>7,} "
                  f"status={r.get('status','?')} elapsed={elapsed:.0f}s",
                  flush=True)
            if r.get("status") == "auth_error" and summary["auth_errors"] >= 3:
                print(f"[fetch1m] FATAL: 3+ auth errors. Access token expired? Refresh KITE_ACCESS_TOKEN in config/.env")
                # Cancel remaining
                for f2 in futs:
                    f2.cancel()
                break

    elapsed = time.time() - t0
    print(f"\n[fetch1m] Done in {elapsed/60:.1f} min")
    print(f"  Symbols processed: {summary['symbols_done']}")
    print(f"  Total rows written: {summary['rows_total']:,}")
    if summary["auth_errors"]:
        print(f"  AUTH ERRORS: {summary['auth_errors']}  --  refresh KITE_ACCESS_TOKEN")
    return summary
