"""
KANIDA -- Sync OHLC data then compute trend states.

Step 1: Fetch OHLC for all tickers in signal_events (stocks)
Step 2: Optionally fetch OHLC for indices in index_metadata (--indices)
Step 3: Compute UPTREND / DOWNTREND / RANGE for every date
Step 4: Backfill trend_state into signal_events + signal_outcomes

Market separation is hard:
  --market NSE  fetches NSE stocks + NSE indices only
  --market US   fetches US stocks + US indices only
  No cross-market data is ever mixed.

Usage:
    python backend/scripts/run_ohlc_sync.py
    python backend/scripts/run_ohlc_sync.py --market NSE --workers 12
    python backend/scripts/run_ohlc_sync.py --market US --workers 8
    python backend/scripts/run_ohlc_sync.py --indices           # stocks + indices
    python backend/scripts/run_ohlc_sync.py --indices-only      # indices only, skip stocks
    python backend/scripts/run_ohlc_sync.py --start 2022-01-01
"""
import sys, os, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from backend.signals import ohlc as ohlc_mod
from backend.signals import trend_state as ts_mod
from backend.signals.db import SIGNALS_DB_PATH


def _run_one_market(market: str, args) -> None:
    print(f"\nOHLC Sync -- market={market}")

    # Step 1: Stock OHLC — authoritative source is sector_mapping
    # (matches run_nightly_worker and covers tickers that have not yet
    #  produced signal_events, e.g. US stocks in initial phase).
    if not args.indices_only:
        ohlc_mod.sync_all_stocks(
            market=market, db_path=args.db,
            workers=args.workers, start=args.start,
        )

    # Step 2: Index OHLC (opt-in)
    if args.indices or args.indices_only:
        ohlc_mod.sync_indices(
            market, args.db, workers=min(args.workers, 8), start=args.start,
        )

    # Step 3+4: Trend state + context backfill
    if not args.no_trend:
        ts_mod.run_all(market, args.db, workers=args.workers)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--market",       default="NSE",
                        help="Market partition: NSE | US | ALL (default: NSE)")
    parser.add_argument("--workers",      type=int, default=12)
    parser.add_argument("--start",        default="2020-01-01",
                        help="Earliest date to fetch (default 2020-01-01)")
    parser.add_argument("--db",           default=SIGNALS_DB_PATH)
    parser.add_argument("--indices",      action="store_true",
                        help="Also fetch OHLC for indices in index_metadata")
    parser.add_argument("--indices-only", action="store_true",
                        help="Skip stock OHLC; fetch indices only")
    parser.add_argument("--no-trend",     action="store_true",
                        help="Skip trend state computation after OHLC fetch")
    args = parser.parse_args()

    market = args.market.upper()
    markets = ["NSE", "US"] if market == "ALL" else [market]

    for m in markets:
        _run_one_market(m, args)

    print("\nDone.")
    print(f"Next: python backend/scripts/run_calibration.py --market {market}")


if __name__ == "__main__":
    main()
