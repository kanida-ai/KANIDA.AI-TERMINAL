"""
Multi-worker daily-OHLC fetcher for NSE equity instruments.

Faster than 1m fetcher: Kite's `historical_data(interval="day")` returns
multi-year history in one call (no 60-day chunking). 5 rps × 16 workers handles
~500 symbols in ~2 minutes.
"""
from __future__ import annotations
import os, sqlite3, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from engine.data_fetch import RateLimiter, get_kite, get_latest_access_token


def get_eq_tokens(kite, symbols: List[str]) -> Dict[str, int]:
    """Map NSE EQ tradingsymbol -> instrument_token for the given symbols."""
    insts = kite.instruments("NSE")
    by_sym = {i["tradingsymbol"]: i["instrument_token"]
                for i in insts if i["segment"] == "NSE" and i.get("instrument_type") == "EQ"}
    return {s: by_sym[s] for s in symbols if s in by_sym}


def _fetch_one_eq_daily(args) -> Dict:
    sym, token, start_iso, end_iso, db_path, rl = args
    out = {"symbol": sym, "rows": 0, "status": "ok", "error": None}
    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=os.environ["KITE_API_KEY"])
        kite.set_access_token(get_latest_access_token())
    except Exception as e:
        out["status"] = "auth_error"; out["error"] = str(e); return out

    rl.wait()
    try:
        rows = kite.historical_data(token,
                                       from_date=datetime.fromisoformat(start_iso),
                                       to_date=datetime.fromisoformat(end_iso),
                                       interval="day")
    except Exception as e:
        msg = str(e)
        if "TokenException" in msg or "expired" in msg.lower():
            out["status"] = "auth_error"; out["error"] = msg; return out
        out["status"] = "fetch_error"; out["error"] = msg; return out

    if not rows:
        out["status"] = "empty"; return out

    payload = []
    for r in rows:
        try:
            d = r["date"].strftime("%Y-%m-%d")
            payload.append((sym, d,
                             float(r["open"]), float(r["high"]),
                             float(r["low"]),  float(r["close"]),
                             int(r["volume"]), "ok"))
        except (KeyError, TypeError, ValueError):
            continue

    con = sqlite3.connect(db_path, timeout=60.0)
    # OR REPLACE (not IGNORE): a re-fetched FINAL bar must overwrite a PARTIAL
    # bar that a mid-session run may have written for the same (symbol, trade_date).
    # With IGNORE, a partial intraday candle written before close would be frozen
    # forever (poisoning that day's features + signals). Relies on the UNIQUE
    # (symbol, trade_date) constraint that IGNORE already depended on.
    con.executemany("""
        INSERT OR REPLACE INTO ohlc_daily
            (symbol, trade_date, open, high, low, close, volume, quality_flag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, payload)
    con.commit()
    con.close()
    out["rows"] = len(payload)
    return out


def fetch_eq_daily(db_path: Path, symbols: List[str],
                     start_date: str, end_date: str,
                     n_workers: int = 16, rps: float = 5.0) -> Dict:
    n_workers = max(10, min(48, n_workers))
    print(f"[eq-daily] Resolving instrument tokens for {len(symbols)} symbols...")
    kite = get_kite()
    tokens = get_eq_tokens(kite, symbols)
    missing = [s for s in symbols if s not in tokens]
    if missing:
        print(f"[eq-daily] WARN: no token for {len(missing)} symbols (e.g., {missing[:8]})")

    rl = RateLimiter(rps=rps)
    args_list = [(s, t, start_date, end_date, str(db_path), rl) for s, t in tokens.items()]
    print(f"[eq-daily] Fetching daily bars for {len(args_list)} symbols  |  "
          f"{n_workers} workers  |  {rps} rps")

    summary = {"done": 0, "rows_total": 0, "auth_errors": 0, "empty": 0,
                "fetch_errors": 0, "details": {}}
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_fetch_one_eq_daily, a): a[0] for a in args_list}
        for f in as_completed(futs):
            sym = futs[f]
            try:
                r = f.result()
            except Exception as e:
                r = {"symbol": sym, "status": "error", "error": str(e), "rows": 0}
            summary["done"] += 1
            summary["rows_total"] += r.get("rows", 0)
            st = r.get("status")
            if st == "auth_error":  summary["auth_errors"] += 1
            if st == "empty":       summary["empty"] += 1
            if st == "fetch_error": summary["fetch_errors"] += 1
            summary["details"][sym] = r
            if summary["done"] % 50 == 0:
                print(f"  [{summary['done']:>4}/{len(args_list)}] "
                       f"rows={summary['rows_total']:,} elapsed={time.time()-t0:.0f}s",
                       flush=True)
            if summary["auth_errors"] >= 3:
                print("[eq-daily] FATAL: 3+ auth errors. Refresh token.")
                for f2 in futs: f2.cancel()
                break

    print(f"\n[eq-daily] Done in {(time.time()-t0)/60:.1f} min")
    print(f"  Symbols processed: {summary['done']}")
    print(f"  Total rows written: {summary['rows_total']:,}")
    print(f"  Empty: {summary['empty']}  ·  Fetch errors: {summary['fetch_errors']}")
    return summary
