"""
KANIDA — SNAPSHOT BUILDER (v2)
================================
Precomputes all stock signals and writes them to agent_signal_snapshots.
This is the ONLY place that calls the live scanner.
The screener, decision cards, and simulation ALL read from snapshots.

Improvements over v1:
  1. Parallel processing   — 5 workers process stocks simultaneously (5× faster)
  2. Retry logic           — auto-retries failed stocks up to 2 times
  3. snapshot_errors table — all failures stored in DB for debugging
  4. Timing metrics        — per-stock duration tracked for bottleneck visibility

Architecture rules (DO NOT VIOLATE):
  1. Only this script calls scan_one() or any live computation
  2. API endpoints only read from agent_signal_snapshots
  3. This runs on a schedule — never triggered by a user request
  4. DB writes are serialised through a lock — no concurrent write corruption

Usage:
  python snapshot_builder.py --market NSE
  python snapshot_builder.py --market NSE --bias bearish
  python snapshot_builder.py --all              (all markets + all biases)
  python snapshot_builder.py --status           (show snapshot freshness)
  python snapshot_builder.py --errors           (show recent errors)
  python snapshot_builder.py --workers 5        (override parallel workers)
"""

import argparse
import json
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.dirname(__file__))

import os as _os
DB_PATH = _os.environ.get(
    "KANIDA_DB_PATH",
    _os.path.normpath(_os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "..", "data", "db", "kanida_fingerprints.db"))
)

TODAY = datetime.today().strftime("%Y-%m-%d")
NOW   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Thread-safe DB write lock — prevents SQLite corruption under parallel writes
_DB_LOCK = threading.Lock()

# Read from env — safe upper bound is ~12 before yfinance rate-limits bite.
# Recommended: 5 (conservative/safe), 10 (fast on local), 12 (max safe).
MAX_WORKERS = int(os.environ.get("KANIDA_SB_WORKERS", "5"))
MAX_RETRIES = 2    # retry attempts per stock


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ══════════════════════════════════════════════════════════════════════════════
# NEW TABLES — snapshot_errors + snapshot_run_metrics
# ══════════════════════════════════════════════════════════════════════════════

CREATE_SNAPSHOT_ERRORS = """
CREATE TABLE IF NOT EXISTS snapshot_errors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    market          TEXT    NOT NULL,
    bias            TEXT    NOT NULL,
    snapshot_date   TEXT    NOT NULL,
    error_message   TEXT,
    attempt_count   INTEGER DEFAULT 1,
    created_at      TEXT    NOT NULL
)
"""

CREATE_SNAPSHOT_METRICS = """
CREATE TABLE IF NOT EXISTS snapshot_run_metrics (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    market          TEXT    NOT NULL,
    bias            TEXT    NOT NULL,
    snapshot_date   TEXT    NOT NULL,
    started_at      TEXT    NOT NULL,
    finished_at     TEXT    NOT NULL,
    duration_ms     INTEGER NOT NULL,
    status          TEXT    NOT NULL,   -- success / failed / retry_success
    attempt_count   INTEGER DEFAULT 1
)
"""

def init_new_tables(db_path: str = DB_PATH):
    """Create snapshot_errors and snapshot_run_metrics tables."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    with conn:
        conn.execute(CREATE_SNAPSHOT_ERRORS)
        conn.execute(CREATE_SNAPSHOT_METRICS)
    conn.close()


def log_error(ticker: str, market: str, bias: str,
              error_msg: str, attempts: int,
              db_path: str = DB_PATH):
    """Write a failed stock to snapshot_errors table."""
    import sqlite3
    with _DB_LOCK:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        with conn:
            conn.execute("""
                INSERT INTO snapshot_errors
                    (ticker, market, bias, snapshot_date,
                     error_message, attempt_count, created_at)
                VALUES (?,?,?,?,?,?,?)
            """, (ticker, market.upper(), bias, TODAY,
                  str(error_msg)[:500], attempts,
                  datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        conn.close()


def log_metric(ticker: str, market: str, bias: str,
               started_at: float, finished_at: float,
               status: str, attempts: int,
               db_path: str = DB_PATH):
    """Write per-stock timing to snapshot_run_metrics table."""
    import sqlite3
    duration_ms = int((finished_at - started_at) * 1000)
    with _DB_LOCK:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        with conn:
            conn.execute("""
                INSERT INTO snapshot_run_metrics
                    (ticker, market, bias, snapshot_date,
                     started_at, finished_at, duration_ms, status, attempt_count)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (ticker, market.upper(), bias, TODAY,
                  datetime.fromtimestamp(started_at).strftime("%Y-%m-%d %H:%M:%S"),
                  datetime.fromtimestamp(finished_at).strftime("%Y-%m-%d %H:%M:%S"),
                  duration_ms, status, attempts))
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# SINGLE STOCK PROCESSOR — with retry logic + timing
# ══════════════════════════════════════════════════════════════════════════════

def process_one_stock(ticker: str, bt_years: int,
                      market: str, bias: str, timeframe: str,
                      db_path: str) -> dict:
    """
    Process a single stock:
      1. Scan live signal (with up to MAX_RETRIES retries)
      2. Write snapshot to DB (thread-safe)
      3. Log timing metric
      4. Log error if all retries fail

    Returns result dict for aggregation.
    """
    from kanida_batch_scanner import scan_one
    from kanida_db import write_snapshot

    started_at = time.time()
    last_error = None
    result     = None

    for attempt in range(1, MAX_RETRIES + 2):  # attempt 1, 2, 3
        try:
            result = scan_one(
                ticker=ticker, market=market, bias=bias,
                timeframe=timeframe, backtest_years=bt_years,
                capital=0, max_risk=2.0, db_path=db_path,
            )

            if result.get("error"):
                raise ValueError(result["error"])

            # Build snapshot row
            snap = {
                "ticker":            ticker,
                "market":            market,
                "bias":              bias,
                "timeframe":         timeframe,
                "snapshot_date":     TODAY,
                "snapshot_time":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "score_pct":         result.get("score_pct", 0.0) or 0.0,
                "score_label":       result.get("score_label", "WATCHLIST"),
                "firing_count":      result.get("firing_count", 0) or 0,
                "qualified_total":   result.get("qualified_total", 0) or 0,
                "top_strategy":      result.get("top_strategy", "") or "",
                "top_win_rate":      result.get("top_win_rate", 0.0) or 0.0,
                "firing_strategies": result.get("firing_strategies", []),
                "regime":            result.get("regime", "") or "",
                "regime_score":      result.get("regime_score", 0.0) or 0.0,
                "bias_aligned":      bool(result.get("bias_aligned", False)),
                "sector":            result.get("sector", "") or "",
                "hist_win_pct":      result.get("hist_win_pct", 0.0) or 0.0,
                "hist_total":        result.get("hist_total", 0) or 0,
                "hist_avg_outcome":  result.get("hist_avg_outcome", 0.0) or 0.0,
                "live_price":        result.get("live_price", 0.0) or 0.0,
                "stop_pct":          result.get("stop_pct", 5.0) or 5.0,
                "stop_loss":         result.get("stop_loss", 0.0) or 0.0,
                "target_1":          result.get("target_1", 0.0) or 0.0,
                "target_2":          result.get("target_2", 0.0) or 0.0,
                "currency":          "₹" if market.upper() == "NSE" else "$",
            }

            # Thread-safe DB write
            with _DB_LOCK:
                write_snapshot(snap, db_path)

            finished_at = time.time()
            status      = "success" if attempt == 1 else "retry_success"
            log_metric(ticker, market, bias, started_at, finished_at, status, attempt, db_path)

            return {
                "ticker":    ticker,
                "success":   True,
                "score_pct": snap["score_pct"],
                "label":     snap["score_label"],
                "firing":    snap["firing_count"],
                "total":     snap["qualified_total"],
                "attempts":  attempt,
                "duration_ms": int((finished_at - started_at) * 1000),
                "raw":       result,
            }

        except Exception as e:
            last_error = str(e)
            if attempt <= MAX_RETRIES:
                time.sleep(1.5 * attempt)  # back-off: 1.5s, 3.0s
            continue

    # All retries exhausted — log failure
    finished_at = time.time()
    log_metric(ticker, market, bias, started_at, finished_at, "failed", MAX_RETRIES + 1, db_path)
    log_error(ticker, market, bias, last_error, MAX_RETRIES + 1, db_path)

    return {
        "ticker":   ticker,
        "success":  False,
        "error":    last_error,
        "attempts": MAX_RETRIES + 1,
        "duration_ms": int((finished_at - started_at) * 1000),
        "raw":      None,
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN BUILD FUNCTION — parallel processing
# ══════════════════════════════════════════════════════════════════════════════

def build_snapshots(market: str, bias: str,
                    timeframe: str = "1D",
                    workers: int = MAX_WORKERS,
                    db_path: str = DB_PATH) -> dict:
    """
    Build snapshots for all ready agents using parallel workers.

    Flow:
      1. Load all ready agents from DB
      2. Submit each stock to thread pool (workers=5 by default)
      3. As each finishes: log result, collect for screener cache
      4. Write screener cache (strong_buy / strong_sell / watchlist)
      5. Print timing summary
    """
    from kanida_db import get_conn, init_db, write_screener_cache
    from decision_engine import categorise_results

    init_db(db_path)
    init_new_tables(db_path)

    # Load agents
    conn = get_conn(db_path)
    agents = conn.execute("""
        SELECT ticker, backtest_years
        FROM agents
        WHERE status='ready' AND market=?
        ORDER BY ticker ASC
    """, (market.upper(),)).fetchall()
    conn.close()

    if not agents:
        log(f"  No ready agents for {market}")
        return {"market": market, "bias": bias, "built": 0, "errors": 0}

    total      = len(agents)
    build_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log(f"Building snapshots: {market} {bias} — {total} stocks — {workers} workers")
    log(f"Snapshot date: {TODAY}  |  Retries: {MAX_RETRIES}  |  Workers: {workers}")
    log("─" * 65)

    built       = 0
    errors      = 0
    raw_results = []
    timings     = []
    completed   = 0

    run_start = time.time()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                process_one_stock,
                agent["ticker"], agent["backtest_years"],
                market, bias, timeframe, db_path,
            ): agent["ticker"]
            for agent in agents
        }

        for future in as_completed(futures):
            ticker    = futures[future]
            completed += 1
            try:
                res = future.result()
            except Exception as e:
                res = {"ticker": ticker, "success": False, "error": str(e), "attempts": 1, "duration_ms": 0}

            timings.append((ticker, res.get("duration_ms", 0), res.get("success", False)))

            if res["success"]:
                built += 1
                raw_results.append(res["raw"])
                label = res.get("label", "WATCHLIST")
                icon  = "🟢" if label == "STRONG" else "🟡" if label == "DEVELOPING" else "⬜"
                retry = f" (retry {res['attempts']-1}×)" if res.get("attempts", 1) > 1 else ""
                log(f"  {icon} [{completed:3}/{total}] {ticker:<14} {label:<12} "
                    f"score:{res.get('score_pct',0):.0f}%  "
                    f"firing:{res.get('firing',0)}/{res.get('total',0)}  "
                    f"{res['duration_ms']}ms{retry}")
            else:
                errors += 1
                log(f"  ❌ [{completed:3}/{total}] {ticker:<14} FAILED ({res['attempts']} attempts): {str(res.get('error',''))[:60]}")

    # Build screener cache
    if raw_results:
        cats = categorise_results(raw_results)
        with _DB_LOCK:
            write_screener_cache(
                market=market, bias=bias,
                strong_buy=  [r["ticker"] for r in cats.get("strong_buy",  [])],
                strong_sell= [r["ticker"] for r in cats.get("strong_sell", [])],
                watchlist=   [r["ticker"] for r in cats.get("watchlist",   [])],
                total_scanned=len(raw_results),
                timeframe=timeframe,
                db_path=db_path,
            )
        sb = len(cats.get("strong_buy",  []))
        ss = len(cats.get("strong_sell", []))
        wl = len(cats.get("watchlist",   []))
        log(f"  Screener cache: strong_buy={sb}  strong_sell={ss}  watchlist={wl}")

    # Timing summary
    elapsed = time.time() - run_start
    if timings:
        slowest = sorted(timings, key=lambda x: x[1], reverse=True)[:5]
        log(f"\n  ── TIMING SUMMARY ──")
        log(f"  Total elapsed: {elapsed:.1f}s  |  Built: {built}  |  Errors: {errors}")
        log(f"  Slowest stocks:")
        for t in slowest:
            status = "✅" if t[2] else "❌"
            log(f"    {status} {t[0]:<14} {t[1]}ms")

    log(f"\n  ✅ Done: {built} built, {errors} errors, {elapsed:.1f}s total")

    return {
        "market":        market,
        "bias":          bias,
        "snapshot_date": TODAY,
        "built":         built,
        "errors":        errors,
        "total":         total,
        "elapsed_s":     round(elapsed, 1),
    }


def build_all(workers: int = MAX_WORKERS, db_path: str = DB_PATH):
    """Build snapshots for all markets and all biases."""
    log("=" * 65)
    log("KANIDA SNAPSHOT BUILDER v2 — FULL RUN")
    log("=" * 65)
    start = time.time()

    results = []
    for market in ["NSE", "US"]:
        for bias in ["bullish", "bearish", "neutral"]:
            log(f"\n── {market} {bias.upper()} ──")
            r = build_snapshots(market=market, bias=bias,
                                workers=workers, db_path=db_path)
            results.append(r)

    elapsed = time.time() - start
    log(f"\n{'='*65}")
    log(f"FULL RUN COMPLETE — {elapsed:.0f}s")
    log(f"{'Market':<6} {'Bias':<10} {'Built':>6} {'Errors':>7} {'Time':>8}")
    log(f"{'─'*45}")
    for r in results:
        log(f"{r['market']:<6} {r['bias']:<10} {r['built']:>6} {r['errors']:>7} {r['elapsed_s']:>7.1f}s")
    log(f"{'='*65}")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# STATUS + ERRORS VIEW
# ══════════════════════════════════════════════════════════════════════════════

def show_status(db_path: str = DB_PATH):
    """Show snapshot freshness and performance summary."""
    from kanida_db import init_db, get_conn
    init_db(db_path)
    init_new_tables(db_path)
    conn = get_conn(db_path)

    snaps = conn.execute("""
        SELECT market, bias, timeframe,
               MAX(snapshot_date) as latest_date,
               MAX(snapshot_time) as latest_time,
               COUNT(DISTINCT ticker) as stock_count
        FROM agent_signal_snapshots
        GROUP BY market, bias, timeframe
        ORDER BY market, bias
    """).fetchall()

    caches = conn.execute("""
        SELECT market, bias, cache_time,
               json_array_length(strong_buy)  as sb,
               json_array_length(strong_sell) as ss,
               json_array_length(watchlist)   as wl
        FROM screener_cache
        ORDER BY market, bias
    """).fetchall()

    # Performance stats from today
    metrics = conn.execute("""
        SELECT market, bias,
               COUNT(*) as total,
               AVG(duration_ms) as avg_ms,
               MAX(duration_ms) as max_ms,
               MIN(duration_ms) as min_ms,
               SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failures
        FROM snapshot_run_metrics
        WHERE snapshot_date=?
        GROUP BY market, bias
    """, (TODAY,)).fetchall()

    conn.close()

    print(f"\n{'='*80}")
    print("  SNAPSHOT STATUS")
    print(f"{'='*80}")
    if not snaps:
        print("  No snapshots found. Run: python snapshot_builder.py --all")
    else:
        print(f"  {'Market':<6} {'Bias':<10} {'TF':<5} {'Date':<12} {'Age':>10} {'Stocks':>7}")
        print(f"  {'─'*60}")
        for r in snaps:
            r = dict(r)
            try:
                from datetime import datetime as _dt
                latest = _dt.strptime(r["latest_time"], "%Y-%m-%d %H:%M:%S")
                age_m  = int((_dt.now() - latest).total_seconds() / 60)
                age    = f"{age_m}m ago" if age_m < 60 else f"{age_m//60}h {age_m%60}m ago"
            except Exception:
                age = "unknown"
            print(f"  {r['market']:<6} {r['bias']:<10} {r['timeframe']:<5} "
                  f"{r['latest_date']:<12} {age:>10} {r['stock_count']:>7}")

    print(f"\n  SCREENER CACHE")
    print(f"  {'─'*60}")
    if not caches:
        print("  No screener cache found.")
    else:
        print(f"  {'Market':<6} {'Bias':<10} {'Time':<22} {'Buy':>5} {'Sell':>5} {'Watch':>6}")
        for r in caches:
            r = dict(r)
            print(f"  {r['market']:<6} {r['bias']:<10} {r['cache_time']:<22} "
                  f"{r['sb']:>5} {r['ss']:>5} {r['wl']:>6}")

    if metrics:
        print(f"\n  PERFORMANCE — TODAY ({TODAY})")
        print(f"  {'─'*60}")
        print(f"  {'Market':<6} {'Bias':<10} {'Total':>6} {'AvgMs':>7} {'MaxMs':>7} {'Fails':>6}")
        for r in metrics:
            r = dict(r)
            print(f"  {r['market']:<6} {r['bias']:<10} {r['total']:>6} "
                  f"{int(r['avg_ms'] or 0):>7} {int(r['max_ms'] or 0):>7} {r['failures']:>6}")

    print(f"{'='*80}\n")


def show_errors(days: int = 7, db_path: str = DB_PATH):
    """Show recent snapshot errors from the DB."""
    from kanida_db import init_db, get_conn
    init_db(db_path)
    init_new_tables(db_path)
    conn = get_conn(db_path)

    errors = conn.execute("""
        SELECT ticker, market, bias, snapshot_date,
               error_message, attempt_count, created_at
        FROM snapshot_errors
        WHERE snapshot_date >= date('now', ?)
        ORDER BY created_at DESC
        LIMIT 50
    """, (f"-{days} days",)).fetchall()
    conn.close()

    print(f"\n  SNAPSHOT ERRORS — last {days} days")
    print(f"  {'─'*80}")
    if not errors:
        print("  No errors found.")
    else:
        print(f"  {'Date':<12} {'Ticker':<14} {'Market':<6} {'Bias':<10} {'Attempts':>8}  Error")
        for e in errors:
            e = dict(e)
            print(f"  {e['snapshot_date']:<12} {e['ticker']:<14} {e['market']:<6} "
                  f"{e['bias']:<10} {e['attempt_count']:>8}  {e['error_message'][:50]}")
    print()


def show_slowest(top_n: int = 10, db_path: str = DB_PATH):
    """Show slowest stocks from today's metrics."""
    from kanida_db import init_db, get_conn
    init_db(db_path)
    init_new_tables(db_path)
    conn = get_conn(db_path)

    rows = conn.execute("""
        SELECT ticker, market, bias, duration_ms, status, attempt_count
        FROM snapshot_run_metrics
        WHERE snapshot_date=?
        ORDER BY duration_ms DESC
        LIMIT ?
    """, (TODAY, top_n)).fetchall()
    conn.close()

    print(f"\n  SLOWEST STOCKS — {TODAY}")
    print(f"  {'─'*60}")
    if not rows:
        print("  No metrics found for today.")
    else:
        print(f"  {'Ticker':<14} {'Market':<6} {'Bias':<10} {'Ms':>7} {'Status':<15} {'Attempts':>8}")
        for r in rows:
            r = dict(r)
            print(f"  {r['ticker']:<14} {r['market']:<6} {r['bias']:<10} "
                  f"{r['duration_ms']:>7} {r['status']:<15} {r['attempt_count']:>8}")
    print()


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description="KANIDA Snapshot Builder v2 — parallel, retry, metrics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--all",       action="store_true", help="Build all markets + all biases")
    ap.add_argument("--market",    default="NSE",       choices=["NSE","US","ALL"])
    ap.add_argument("--bias",      default="ALL",       choices=["bullish","bearish","neutral","ALL"])
    ap.add_argument("--timeframe", default="1D",        choices=["1D","1W"])
    ap.add_argument("--status",    action="store_true", help="Show snapshot freshness + performance")
    ap.add_argument("--errors",    action="store_true", help="Show recent snapshot errors")
    ap.add_argument("--slowest",   action="store_true", help="Show slowest stocks from today")
    ap.add_argument("--workers",   type=int, default=MAX_WORKERS, help=f"Parallel workers (default {MAX_WORKERS})")
    ap.add_argument("--db",        default=DB_PATH)

    args = ap.parse_args()
    db   = args.db

    if args.status:
        show_status(db)
    elif args.errors:
        show_errors(db_path=db)
    elif args.slowest:
        show_slowest(db_path=db)
    elif args.all or (args.market == "ALL" and args.bias == "ALL"):
        build_all(workers=args.workers, db_path=db)
    else:
        markets = ["NSE","US"] if args.market == "ALL" else [args.market]
        biases  = ["bullish","bearish","neutral"] if args.bias == "ALL" else [args.bias]
        for market in markets:
            for bias in biases:
                build_snapshots(market=market, bias=bias,
                                timeframe=args.timeframe,
                                workers=args.workers,
                                db_path=db)


if __name__ == "__main__":
    main()
