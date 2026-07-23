"""
Fetch daily OHLC + Open Interest for NSE F&O futures contracts.

Strategy: Kite's `instruments("NFO")` only returns CURRENTLY-ACTIVE contracts.
That gives us ~3 monthly contracts per underlying (current + next + far month).
For each underlying we fetch daily bars with OI for every active futures
contract — Kite's `historical_data` returns OI when called with `oi=True` flag.

Coverage: a freshly-introduced monthly contract has data from its introduction
date (~3 months before expiry). So at any given moment we get OI data for the
trailing ~3 months — sufficient for the Engine V1 5-day OI delta signal in the
back-half of our intraday Window A (Feb-Apr 2026).

Multi-worker thread pool (10-48), rate-limited 5 rps on Kite API.
"""
from __future__ import annotations
import os, sqlite3, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from engine.data_fetch import RateLimiter, get_kite, get_latest_access_token


# ─────────────────────────────────────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────────────────────────────────────

OI_SCHEMA = """
CREATE TABLE IF NOT EXISTS ohlc_futures_daily (
    symbol         TEXT NOT NULL,
    expiry         TEXT NOT NULL,
    trade_date     TEXT NOT NULL,
    open           REAL,
    high           REAL,
    low            REAL,
    close          REAL,
    volume         INTEGER,
    oi             INTEGER,
    PRIMARY KEY (symbol, expiry, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_fut_sym_dt ON ohlc_futures_daily(symbol, trade_date);

CREATE VIEW IF NOT EXISTS aggregate_oi_daily AS
    SELECT symbol, trade_date,
           SUM(oi) AS total_oi,
           SUM(volume) AS total_fut_volume,
           COUNT(*) AS n_contracts
    FROM ohlc_futures_daily
    GROUP BY symbol, trade_date;
"""


def ensure_oi_schema(con: sqlite3.Connection):
    con.executescript(OI_SCHEMA)
    con.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Contract discovery
# ─────────────────────────────────────────────────────────────────────────────

def get_active_fut_contracts(symbols: List[str]) -> Dict[str, List[Dict]]:
    """For each underlying symbol, return list of active monthly futures
    contracts {tradingsymbol, expiry, instrument_token, lot_size}."""
    kite = get_kite()
    nfo = kite.instruments("NFO")
    by_underlying: Dict[str, List[Dict]] = {s: [] for s in symbols}
    for i in nfo:
        if i["instrument_type"] != "FUT":
            continue
        u = i["name"]
        if u in by_underlying:
            by_underlying[u].append({
                "tradingsymbol":    i["tradingsymbol"],
                "expiry":           i["expiry"].isoformat() if hasattr(i["expiry"], "isoformat") else str(i["expiry"]),
                "instrument_token": i["instrument_token"],
                "lot_size":         i["lot_size"],
            })
    for u in by_underlying:
        by_underlying[u].sort(key=lambda c: c["expiry"])
    return by_underlying


# ─────────────────────────────────────────────────────────────────────────────
# Per-contract worker
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_one_contract(args) -> Dict:
    sym, contract, start_iso, end_iso, db_path, rl = args
    out = {"symbol": sym, "expiry": contract["expiry"],
           "tradingsymbol": contract["tradingsymbol"],
           "rows": 0, "status": "ok", "error": None}

    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=os.environ["KITE_API_KEY"])
        kite.set_access_token(get_latest_access_token())
    except Exception as e:
        out["status"] = "auth_error"; out["error"] = str(e)
        return out

    rl.wait()
    try:
        rows = kite.historical_data(
            contract["instrument_token"],
            from_date=datetime.fromisoformat(start_iso),
            to_date=datetime.fromisoformat(end_iso),
            interval="day",
            oi=True,
        )
    except Exception as e:
        msg = str(e)
        if "TokenException" in msg or "expired" in msg.lower():
            out["status"] = "auth_error"; out["error"] = msg
            return out
        out["status"] = "fetch_error"; out["error"] = msg
        return out

    if not rows:
        out["status"] = "empty"
        return out

    payload = []
    for r in rows:
        try:
            d = r["date"].strftime("%Y-%m-%d")
            payload.append((sym, contract["expiry"], d,
                             float(r["open"]), float(r["high"]),
                             float(r["low"]), float(r["close"]),
                             int(r["volume"]), int(r.get("oi", 0))))
        except (KeyError, TypeError, ValueError):
            continue

    con = sqlite3.connect(db_path, timeout=60.0)
    con.executemany("""
        INSERT OR REPLACE INTO ohlc_futures_daily
            (symbol, expiry, trade_date, open, high, low, close, volume, oi)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, payload)
    con.commit()
    con.close()
    out["rows"] = len(payload)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Top-level driver
# ─────────────────────────────────────────────────────────────────────────────

def fetch_oi_for_symbols(db_path: Path, symbols: List[str],
                          start_date: str, end_date: str,
                          n_workers: int = 16, rps: float = 5.0) -> Dict:
    """Fetch daily OHLC + OI for every active futures contract of each symbol
    over [start_date, end_date]. Returns summary dict."""
    n_workers = max(10, min(48, n_workers))
    print(f"[oi-fetch] Discovering active F&O contracts for {len(symbols)} symbols...")
    by_underlying = get_active_fut_contracts(symbols)
    no_fno = [s for s in symbols if not by_underlying.get(s)]
    if no_fno:
        print(f"[oi-fetch] WARN: no futures for {len(no_fno)} symbols (probably non-F&O): "
              f"{no_fno[:10]}{'...' if len(no_fno)>10 else ''}")

    args_list = []
    for sym in symbols:
        for c in by_underlying.get(sym, []):
            args_list.append((sym, c, start_date, end_date, str(db_path), None))

    print(f"[oi-fetch] {len(args_list)} contract-fetches queued | {n_workers} workers | {rps} rps")

    rl = RateLimiter(rps=rps)
    args_list = [(s, c, sd, ed, dp, rl) for s, c, sd, ed, dp, _ in args_list]

    summary = {"contracts_done": 0, "rows_total": 0, "auth_errors": 0,
               "empty": 0, "fetch_errors": 0, "by_symbol": {}}
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_fetch_one_contract, a): (a[0], a[1]["tradingsymbol"]) for a in args_list}
        for f in as_completed(futs):
            sym, ts = futs[f]
            try:
                r = f.result()
            except Exception as e:
                r = {"symbol": sym, "tradingsymbol": ts, "status": "error", "error": str(e), "rows": 0}
            summary["contracts_done"] += 1
            summary["rows_total"] += r.get("rows", 0)
            st = r.get("status")
            if st == "auth_error":  summary["auth_errors"] += 1
            if st == "empty":       summary["empty"] += 1
            if st == "fetch_error": summary["fetch_errors"] += 1
            summary["by_symbol"].setdefault(sym, []).append(r)
            if summary["contracts_done"] % 50 == 0:
                el = time.time() - t0
                print(f"  [{summary['contracts_done']:>4}/{len(args_list)}] "
                      f"rows={summary['rows_total']:,} elapsed={el:.0f}s", flush=True)
            if summary["auth_errors"] >= 3:
                print("[oi-fetch] FATAL: 3+ auth errors. Refresh KITE_ACCESS_TOKEN.")
                for f2 in futs: f2.cancel()
                break

    print(f"\n[oi-fetch] Done in {(time.time()-t0)/60:.1f} min")
    print(f"  Contracts processed: {summary['contracts_done']}")
    print(f"  Total rows written:  {summary['rows_total']:,}")
    print(f"  Empty:               {summary['empty']}")
    print(f"  Fetch errors:        {summary['fetch_errors']}")
    if summary["auth_errors"]:
        print(f"  AUTH ERRORS: {summary['auth_errors']}")
    return summary
