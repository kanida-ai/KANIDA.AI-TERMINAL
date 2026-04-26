"""
KANIDA — DAILY SCHEDULER
=========================
Runs morning and evening simulation jobs automatically.

On Railway: configure two cron jobs in railway.toml or dashboard:
  Morning: 0 4 * * 1-5     (9:30am IST = 4:00am UTC, Mon-Fri)
  Evening: 30 10 * * 1-5   (4:00pm IST = 10:30am UTC, Mon-Fri)

Local usage (Anaconda — for testing):
  python kanida_scheduler.py --morning
  python kanida_scheduler.py --evening
  python kanida_scheduler.py --both       (runs both — for testing)
  python kanida_scheduler.py --status     (show all sim status)
  python kanida_scheduler.py --date 2026-04-01 --morning  (backfill a date)

How it works:
  Morning run (9:30am IST):
    1. Load all ACTIVE simulations from DB
    2. For each sim: run live scanner on that ticker
    3. For each firing strategy not already open: open a paper trade
    4. Log result to sim_trades table

  Evening run (4:00pm IST):
    1. Load all ACTIVE simulations from DB
    2. For each sim: fetch today's OHLC from yfinance
    3. For each open trade: check if stop or target was hit
    4. Close trades that hit exit levels
    5. Mark remaining open trades to today's close (unrealised P&L)
    6. Write daily snapshot to sim_daily_snapshots table
    7. This snapshot IS the chart data — portfolio value, drawdown, etc.

Railway cron configuration (add to railway.toml):
  [[cron]]
  schedule = "0 4 * * 1-5"
  command = "python kanida_scheduler.py --morning"

  [[cron]]
  schedule = "30 10 * * 1-5"
  command = "python kanida_scheduler.py --evening"
"""

import argparse
import sys
import os
from datetime import datetime, timedelta

# Ensure local modules are importable
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "agents"))  # find kanida_db, snapshot_builder, etc.

DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    os.path.normpath(os.path.join(_HERE, "..", "data", "db", "kanida_fingerprints.db"))
)
LOG_FILE = os.path.join(_HERE, "kanida_scheduler.log")


def log(msg: str):
    """Write to both stdout and log file."""
    ts  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out = f"[{ts}] {msg}"
    print(out)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(out + "\n")
    except Exception:
        pass


def run_snapshot_build(market: str = "ALL"):
    """
    Build precomputed signal snapshots for all stocks.
    Runs BEFORE morning simulation scan so snapshots are fresh.
    This is the ONLY place that calls live scanning logic.
    """
    from snapshot_builder import build_snapshots, build_all

    log(f"SNAPSHOT BUILD — market={market}")
    log("=" * 60)

    try:
        if market == "ALL":
            results = build_all(DB_PATH)
        else:
            results = []
            for bias in ["bullish", "bearish", "neutral"]:
                r = build_snapshots(market=market, bias=bias, db_path=DB_PATH)
                results.append(r)

        total_built  = sum(r.get("built", 0)  for r in results)
        total_errors = sum(r.get("errors", 0) for r in results)
        log(f"SNAPSHOT BUILD COMPLETE — {total_built} built, {total_errors} errors")
        return results

    except Exception as e:
        import traceback
        log(f"SNAPSHOT BUILD ERROR: {e}")
        traceback.print_exc()
        return []


def run_morning(run_date: str = None):
    """
    Morning scan — auto-execute signals for all active simulations.
    Runs at 9:30am IST (after NSE opening, before significant price moves).
    """
    from forward_sim import morning_run, init_sim_tables

    run_date = run_date or datetime.today().strftime("%Y-%m-%d")

    # Skip weekends
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    if dt.weekday() >= 5:
        log(f"MORNING — {run_date} is weekend, skipping")
        return

    log(f"MORNING RUN — {run_date}")
    log("=" * 60)

    init_sim_tables(DB_PATH)
    result = morning_run(run_date=run_date, db_path=DB_PATH)

    sims_done   = result.get("simulations_processed", 0)
    trades_done = result.get("trades_opened", 0)

    log(f"MORNING COMPLETE — {sims_done} sims · {trades_done} trades opened")

    # Per-sim detail
    for r in result.get("results", []):
        if r.get("error"):
            log(f"  ❌ {r['ticker']}: {r['error']}")
        else:
            log(f"  ✅ {r['ticker']} {r['bias']} — score:{r.get('score_pct',0):.0f}% "
                f"firing:{r.get('firing_count',0)} opened:{r.get('trades_opened',0)}")

    return result


def run_evening(run_date: str = None):
    """
    Evening mark-to-market — check stops/targets, record daily snapshot.
    Runs at 4:00pm IST (after NSE closing, 3:30pm).
    """
    from forward_sim import evening_run, init_sim_tables

    run_date = run_date or datetime.today().strftime("%Y-%m-%d")

    # Skip weekends
    dt = datetime.strptime(run_date, "%Y-%m-%d")
    if dt.weekday() >= 5:
        log(f"EVENING — {run_date} is weekend, skipping")
        return

    log(f"EVENING RUN — {run_date}")
    log("=" * 60)

    init_sim_tables(DB_PATH)
    result = evening_run(run_date=run_date, db_path=DB_PATH)

    sims_done    = result.get("simulations_processed", 0)
    trades_closed = result.get("trades_closed", 0)

    log(f"EVENING COMPLETE — {sims_done} sims · {trades_closed} trades closed")
    return result


def show_status():
    """Show current status of all simulations."""
    from forward_sim import init_sim_tables, get_conn

    init_sim_tables(DB_PATH)
    conn = get_conn(DB_PATH)

    sims = conn.execute("""
        SELECT s.*,
               COUNT(t.id) as total_trades,
               SUM(CASE WHEN t.status='OPEN' THEN 1 ELSE 0 END) as open_count,
               COALESCE(snap.portfolio_value, s.capital) as port_val,
               COALESCE(snap.cumulative_pct, 0) as ret_pct,
               COALESCE(snap.max_drawdown, 0) as max_dd,
               snap.snapshot_date as last_snap
        FROM simulations s
        LEFT JOIN sim_trades t ON t.sim_id=s.id
        LEFT JOIN (
            SELECT sim_id, portfolio_value, cumulative_pct,
                   max_drawdown, snapshot_date
            FROM sim_daily_snapshots
            WHERE (sim_id, snapshot_date) IN (
                SELECT sim_id, MAX(snapshot_date)
                FROM sim_daily_snapshots GROUP BY sim_id
            )
        ) snap ON snap.sim_id=s.id
        GROUP BY s.id ORDER BY s.created_at DESC
    """).fetchall()
    conn.close()

    if not sims:
        log("No simulations found")
        return

    log(f"\n{'═'*100}")
    log("  KANIDA SIMULATION STATUS")
    log(f"{'═'*100}")
    log(f"  {'ID':<4} {'Ticker':<12} {'Bias':<10} {'Start':<12} {'Capital':>10} "
        f"{'Portfolio':>12} {'Return':>8} {'MaxDD':>7} {'Trades':>7} {'LastMTM':<12} {'Status'}")
    log(f"  {'─'*98}")

    for s in sims:
        s   = dict(s)
        cur = "₹" if s["market"].upper() == "NSE" else "$"
        log(
            f"  {s['id']:<4} {s['ticker']:<12} {s['bias']:<10} "
            f"{s['sim_start']:<12} {cur}{s['capital']:>8,.0f} "
            f"{cur}{float(s['port_val'] or s['capital']):>10,.0f} "
            f"{float(s['ret_pct'] or 0):>+7.2f}% "
            f"{float(s['max_dd'] or 0):>6.2f}% "
            f"{s['total_trades'] or 0:>7} "
            f"{(s['last_snap'] or 'never'):<12} "
            f"{s['status']}"
        )
    log(f"{'═'*100}\n")


def backfill(from_date: str, to_date: str = None):
    """
    Backfill missing dates between from_date and to_date.
    Useful when the scheduler missed days due to outages.
    Runs morning + evening for each missing trading day.
    """
    to_date  = to_date or datetime.today().strftime("%Y-%m-%d")
    start_dt = datetime.strptime(from_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(to_date,   "%Y-%m-%d")

    current = start_dt
    days_processed = 0
    while current <= end_dt:
        date_str = current.strftime("%Y-%m-%d")
        if current.weekday() < 5:   # Mon-Fri only
            log(f"\n{'─'*60}")
            log(f"BACKFILL: {date_str}")
            run_morning(date_str)
            run_evening(date_str)
            days_processed += 1
        current += timedelta(days=1)

    log(f"\nBackfill complete: {days_processed} trading days processed")


def main():
    ap = argparse.ArgumentParser(
        description="KANIDA Daily Scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--morning",  action="store_true", help="Run morning scan")
    ap.add_argument("--evening",  action="store_true", help="Run evening mark-to-market")
    ap.add_argument("--both",     action="store_true", help="Run both (for testing)")
    ap.add_argument("--status",   action="store_true", help="Show all simulation status")
    ap.add_argument("--backfill", action="store_true", help="Backfill missing dates")
    ap.add_argument("--snapshot", action="store_true", help="Build signal snapshots for all stocks")
    ap.add_argument("--full",     action="store_true", help="Run snapshot + morning + evening (full daily cycle)")
    ap.add_argument("--date",     default=None,        help="Override date (YYYY-MM-DD)")
    ap.add_argument("--from-date",default=None,        help="Backfill start date")
    ap.add_argument("--to-date",  default=None,        help="Backfill end date")
    ap.add_argument("--db",       default=DB_PATH,     help="DB path")

    args = ap.parse_args()
    DB_PATH = args.db  # update module-level var

    log(f"KANIDA Scheduler — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.snapshot:
        run_snapshot_build()

    elif args.full:
        date = args.date or datetime.today().strftime("%Y-%m-%d")
        run_snapshot_build()
        run_morning(date)
        run_evening(date)

    elif args.status:
        show_status()

    elif args.morning:
        run_morning(args.date)

    elif args.evening:
        run_evening(args.date)

    elif args.both:
        date = args.date or datetime.today().strftime("%Y-%m-%d")
        run_morning(date)
        run_evening(date)

    elif args.backfill:
        if not args.from_date:
            print("--from-date required for backfill")
            sys.exit(1)
        backfill(args.from_date, args.to_date)

    else:
        ap.print_help()


if __name__ == "__main__":
    main()
