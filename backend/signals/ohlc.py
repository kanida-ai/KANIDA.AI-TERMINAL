"""
KANIDA Signals — OHLC Management
==================================
Fetches daily OHLC from yfinance and stores in ohlc_daily.

Stock symbols:
  NSE stocks  : ticker + ".NS"  (e.g. PFC -> PFC.NS)
  US stocks   : plain ticker    (e.g. AAPL -> AAPL)
  Special NSE : see _SYMBOL_MAP overrides

Index symbols:
  Indices are never derived from the ticker name — they always use
  the yf_symbol column from index_metadata (e.g. NIFTY -> ^NSEI).
  Use sync_indices() for index OHLC, not sync_all().

Incremental: only fetches from the latest stored date onward.
"""

import sqlite3
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Optional

import yfinance as yf
import pandas as pd

from .db import get_conn, SIGNALS_DB_PATH


# ──────────────────────────────────────────────────────────────────
# SCALAR-SAFE EXTRACTION HELPERS
# ──────────────────────────────────────────────────────────────────
# yfinance occasionally returns DataFrames with duplicated column names
# (e.g. "Close" appearing twice when corporate-action data is merged),
# or MultiIndex columns that flatten badly. When that happens,
# `row.get("close")` returns a pandas Series, not a scalar — and the
# previous code's `float(x or 0)` breaks with
#     "The truth value of a Series is ambiguous."
# These helpers coerce any of (scalar / NaN / None / Series) into a
# clean float or date string.

def _as_scalar(v, default: float = 0.0) -> float:
    """Coerce v to a finite float. Handles Series, NaN, and None safely."""
    if v is None:
        return default
    # If we got a Series (duplicate column case), take the first element.
    if hasattr(v, "iloc"):
        try:
            v = v.iloc[0]
        except Exception:
            return default
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    if math.isnan(f) or math.isinf(f):
        return default
    return f


def _as_date_str(v) -> str:
    """Coerce v to 'YYYY-MM-DD'. Handles Series / Timestamp / str."""
    if hasattr(v, "iloc"):
        try:
            v = v.iloc[0]
        except Exception:
            return ""
    return str(v)[:10]


def _normalise_df(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase columns, flatten any MultiIndex, drop duplicates. Safe no-op
    for already-clean frames."""
    if df is None or df.empty:
        return df
    # Flatten MultiIndex columns (some yfinance versions return them even
    # when multi_level_index=False was passed).
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join(str(c) for c in col if str(c)) for col in df.columns
        ]
    df.columns = [str(c).lower().strip() for c in df.columns]
    # Drop duplicate column names — keep the first occurrence.
    df = df.loc[:, ~df.columns.duplicated()]
    return df

# Tickers that need a different yfinance symbol
_SYMBOL_MAP = {
    # yfinance symbol overrides — verified 2026-04
    ("M&M",    "NSE"): "M&M.NS",    # yfinance accepts ampersand
    ("L&T",    "NSE"): "LT.NS",     # L&T was renamed to LT on NSE; yfinance follows
    ("M&MFIN", "NSE"): "M&MFIN.NS", # ampersand accepted
}

# How far back to fetch on first pull (enough for 200-day SMA on earliest signal)
_DEFAULT_START = "2020-01-01"


def yf_symbol(ticker: str, market: str) -> str:
    if (ticker, market) in _SYMBOL_MAP:
        return _SYMBOL_MAP[(ticker, market)]
    if market == "NSE":
        return f"{ticker}.NS"
    return ticker


def latest_stored_date(ticker: str, market: str, conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute(
        "SELECT MAX(trade_date) FROM ohlc_daily WHERE ticker=? AND market=?",
        (ticker, market)
    ).fetchone()
    return row[0] if row else None


def fetch_and_store(
    ticker: str,
    market: str,
    conn: sqlite3.Connection,
    start: str = _DEFAULT_START,
) -> int:
    """Fetch OHLC for one ticker and upsert into ohlc_daily. Returns row count stored."""
    symbol = yf_symbol(ticker, market)
    last_date = latest_stored_date(ticker, market, conn)

    if last_date:
        # Fetch from day after last stored
        fetch_start = (
            datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")
    else:
        fetch_start = start

    today = datetime.now().strftime("%Y-%m-%d")
    if fetch_start > today:
        return 0  # already up to date

    try:
        df = yf.download(
            symbol,
            start=fetch_start,
            end=today,
            auto_adjust=True,
            progress=False,
            multi_level_index=False,
        )
    except Exception as exc:
        print(f"  [WARN] {ticker} ({symbol}): yfinance error — {exc}")
        return 0

    if df is None or df.empty:
        return 0

    df = df.reset_index()
    df = _normalise_df(df)

    if "date" not in df.columns or "close" not in df.columns:
        return 0

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for _, row in df.iterrows():
        trade_date = _as_date_str(row.get("date"))
        close_val  = _as_scalar(row.get("close"))
        if not trade_date or close_val == 0.0:
            # Skip rows without a real close (NaN / empty) — common on
            # the partial current-session bar yfinance sometimes returns.
            continue
        rows.append((
            ticker,
            market,
            trade_date,
            _as_scalar(row.get("open"),  close_val),
            _as_scalar(row.get("high"),  close_val),
            _as_scalar(row.get("low"),   close_val),
            close_val,
            _as_scalar(row.get("volume"), 0.0),
            "yfinance",
            now_str,
        ))

    if not rows:
        return 0

    with conn:
        conn.executemany(
            """INSERT OR REPLACE INTO ohlc_daily
               (ticker, market, trade_date, open, high, low, close, volume, source, fetched_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
    return len(rows)


def sync_all(
    tickers: list[str],
    market: str = "NSE",
    db_path: str = SIGNALS_DB_PATH,
    workers: int = 12,
    start: str = _DEFAULT_START,
) -> dict:
    """
    Fetch OHLC for all tickers using a thread pool.
    Each thread gets its own DB connection (SQLite WAL supports concurrent readers).
    All writes are single-row batches — no cross-thread write conflicts.
    Returns summary dict.
    """
    print(f"\nOHLC Sync — {market} — {len(tickers)} tickers — {workers} workers")
    print(f"  Fetching from {start} onward (incremental per ticker)\n")

    results = {"ok": 0, "skipped": 0, "failed": 0, "total_rows": 0}
    t_global = time.time()

    def _worker(ticker: str) -> tuple[str, int]:
        conn = get_conn(db_path)
        try:
            n = fetch_and_store(ticker, market, conn, start)
            return ticker, n
        except Exception as exc:
            print(f"  [FAIL] {ticker}: {exc}")
            return ticker, -1
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker, t): t for t in tickers}
        done = 0
        for fut in as_completed(futures):
            ticker, n = fut.result()
            done += 1
            if n < 0:
                results["failed"] += 1
            elif n == 0:
                results["skipped"] += 1
            else:
                results["ok"] += 1
                results["total_rows"] += n
            if done % 20 == 0 or done == len(tickers):
                pct = done / len(tickers) * 100
                print(f"  [{done}/{len(tickers)} {pct:.0f}%] ok={results['ok']} "
                      f"skip={results['skipped']} fail={results['failed']}")

    elapsed = time.time() - t_global
    print(f"\n  Finished in {elapsed:.1f}s — {results['total_rows']:,} new rows stored")
    return results


def get_tickers_from_signals(market: str = "NSE", db_path: str = SIGNALS_DB_PATH) -> list[str]:
    conn = get_conn(db_path)
    rows = conn.execute(
        "SELECT DISTINCT ticker FROM signal_events WHERE market = ? ORDER BY ticker",
        (market,)
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_tickers_from_sector_mapping(
    market: str = "NSE", db_path: str = SIGNALS_DB_PATH
) -> list[str]:
    """All non-index tickers the engine covers for `market`. This is the
    authoritative source for the nightly/intraday loops — it matches
    exactly what run_nightly_worker._tickers_for_market() picks up."""
    conn = get_conn(db_path)
    rows = conn.execute(
        """SELECT ticker FROM sector_mapping
           WHERE market=? AND (is_index IS NULL OR is_index=0)
           ORDER BY ticker""",
        (market,),
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def sync_all_stocks(
    market: str = "NSE",
    db_path: str = SIGNALS_DB_PATH,
    workers: int = 12,
    start: str = _DEFAULT_START,
) -> dict:
    """
    Nightly-worker entrypoint. Resolves all stock tickers for `market`
    from sector_mapping, then delegates to sync_all().

    Naming rationale: run_nightly_worker._run_ohlc_sync() calls
    `ohlc_mod.sync_all_stocks(market=...)`. Previously that attribute
    didn't exist, so the worker shelled out to run_ohlc_sync.py —
    noisy and duplicated the logic. This function closes that gap
    without changing the underlying fetch path.
    """
    tickers = get_tickers_from_sector_mapping(market, db_path)
    if not tickers:
        print(f"No stocks found in sector_mapping for market={market}")
        return {"ok": 0, "skipped": 0, "failed": 0, "total_rows": 0}
    return sync_all(
        tickers=tickers,
        market=market,
        db_path=db_path,
        workers=workers,
        start=start,
    )


def sync_indices(
    market: str = "NSE",
    db_path: str = SIGNALS_DB_PATH,
    workers: int = 8,
    start: str = _DEFAULT_START,
) -> dict:
    """
    Fetch OHLC for all entries in index_metadata using their yf_symbol directly.
    Stores rows in ohlc_daily with the canonical ticker (e.g. NIFTY, SPX).
    Completely separate from sync_all() — indices must never go through
    the stock yf_symbol() derivation.
    """
    conn = get_conn(db_path)
    index_rows = conn.execute(
        "SELECT ticker, yf_symbol FROM index_metadata WHERE market=? AND is_active=1",
        (market,)
    ).fetchall()
    conn.close()

    if not index_rows:
        print(f"No active indices found in index_metadata for market={market}")
        return {"ok": 0, "skipped": 0, "failed": 0, "total_rows": 0}

    print(f"\nIndex OHLC Sync -- {market} -- {len(index_rows)} indices")
    results = {"ok": 0, "skipped": 0, "failed": 0, "total_rows": 0}
    t0 = time.time()

    def _worker(ticker: str, yf_sym: str) -> tuple[str, int]:
        c = get_conn(db_path)
        try:
            last_date = latest_stored_date(ticker, market, c)
            if last_date:
                fetch_start = (
                    datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
                ).strftime("%Y-%m-%d")
            else:
                fetch_start = start

            today = datetime.now().strftime("%Y-%m-%d")
            if fetch_start > today:
                return ticker, 0

            df = yf.download(
                yf_sym,
                start=fetch_start,
                end=today,
                auto_adjust=True,
                progress=False,
                multi_level_index=False,
            )
            if df is None or df.empty:
                return ticker, 0

            df = df.reset_index()
            df = _normalise_df(df)
            if "date" not in df.columns or "close" not in df.columns:
                return ticker, 0

            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            rows = []
            for _, row in df.iterrows():
                trade_date = _as_date_str(row.get("date"))
                close_val  = _as_scalar(row.get("close"))
                if not trade_date or close_val == 0.0:
                    continue
                rows.append((
                    ticker, market,
                    trade_date,
                    _as_scalar(row.get("open"),  close_val),
                    _as_scalar(row.get("high"),  close_val),
                    _as_scalar(row.get("low"),   close_val),
                    close_val,
                    _as_scalar(row.get("volume"), 0.0),
                    "yfinance",
                    now_str,
                ))
            if rows:
                with c:
                    c.executemany(
                        """INSERT OR REPLACE INTO ohlc_daily
                           (ticker, market, trade_date, open, high, low, close, volume, source, fetched_at)
                           VALUES (?,?,?,?,?,?,?,?,?,?)""",
                        rows,
                    )
            return ticker, len(rows)
        except Exception as exc:
            print(f"  [FAIL] {ticker} ({yf_sym}): {exc}")
            return ticker, -1
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker, t, s): t for t, s in index_rows}
        for fut in as_completed(futures):
            ticker, n = fut.result()
            if n < 0:
                results["failed"] += 1
                print(f"  [FAIL] {ticker}")
            elif n == 0:
                results["skipped"] += 1
                print(f"  [SKIP] {ticker} (up to date)")
            else:
                results["ok"] += 1
                results["total_rows"] += n
                print(f"  [OK]   {ticker}  {n} rows")

    print(f"\n  Done in {time.time()-t0:.1f}s -- {results['total_rows']} new rows stored")
    return results
