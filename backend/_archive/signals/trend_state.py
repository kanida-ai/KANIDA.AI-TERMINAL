"""
KANIDA Signals — Stock Trend State Computation
===============================================
Classifies each trading day as UPTREND / DOWNTREND / RANGE using:
  - Price vs SMA-50, SMA-200
  - 52-week price position (0–1)

Classification rule (all three conditions must agree):
  UPTREND    : close > sma_200 AND sma_50 > sma_200 AND position_52w >= 0.50
  DOWNTREND  : close < sma_200 AND sma_50 < sma_200 AND position_52w < 0.50
  RANGE      : everything else (mixed signals)

Trend strength (0–100): continuous measure of conviction.
Low confidence flag: set when < 200 days of OHLC history available.

After computation, backfills trend_state and trend_strength into
signal_events and signal_outcomes for historical context.
"""

import sqlite3
import time
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import numpy as np

from .db import get_conn, SIGNALS_DB_PATH


# ──────────────────────────────────────────────────────────────────
# CLASSIFICATION
# ──────────────────────────────────────────────────────────────────

def _classify(
    close: float,
    sma_50: Optional[float],
    sma_200: Optional[float],
    position_52w: float,
    has_sma_50: bool,
    has_sma_200: bool,
) -> tuple[str, bool]:
    """Returns (trend_state, low_confidence)."""
    if not has_sma_50:
        return "RANGE", True

    low_conf = not has_sma_200

    if low_conf:
        # Only SMA-50 available
        if close > sma_50 and position_52w >= 0.5:
            return "UPTREND", True
        if close < sma_50 and position_52w < 0.5:
            return "DOWNTREND", True
        return "RANGE", True

    # Full formula
    if close > sma_200 and sma_50 > sma_200 and position_52w >= 0.5:
        return "UPTREND", False
    if close < sma_200 and sma_50 < sma_200 and position_52w < 0.5:
        return "DOWNTREND", False
    return "RANGE", False


def _strength(
    close: float,
    sma_50: Optional[float],
    sma_200: Optional[float],
    position_52w: float,
    trend_state: str,
    has_sma_200: bool,
) -> float:
    """
    Trend strength 0–100.
    Components:
      +25  price vs sma_200 (direction correct)
      +15  price vs sma_50  (direction correct)
      +15  sma_50 vs sma_200 (alignment)
      +20  position_52w scaled to 0–20 (distance from midpoint)
      +25  distance of price from sma_200 (conviction, capped at 15% gap)
    """
    if not has_sma_200 or sma_200 is None or sma_50 is None:
        # Simplified strength without sma_200
        base = 25.0 if trend_state == "UPTREND" else (25.0 if trend_state == "DOWNTREND" else 0.0)
        pos_score = abs(position_52w - 0.5) * 40  # 0–20
        return min(100.0, round(base + pos_score, 1))

    is_up = trend_state == "UPTREND"
    is_dn = trend_state == "DOWNTREND"

    s = 0.0
    # Price vs sma_200
    if (is_up and close > sma_200) or (is_dn and close < sma_200):
        s += 25
    # Price vs sma_50
    if (is_up and close > sma_50) or (is_dn and close < sma_50):
        s += 15
    # SMA alignment
    if (is_up and sma_50 > sma_200) or (is_dn and sma_50 < sma_200):
        s += 15
    # 52-week position (distance from 0.5 midpoint, max 20pts)
    s += abs(position_52w - 0.5) * 40
    # Distance from sma_200 (capped at 15% gap = full 25 pts)
    dist = abs(close - sma_200) / sma_200
    s += min(25.0, dist / 0.15 * 25.0)

    return min(100.0, round(s, 1))


# ──────────────────────────────────────────────────────────────────
# PER-TICKER COMPUTATION
# ──────────────────────────────────────────────────────────────────

def compute_for_ticker(
    ticker: str,
    market: str,
    conn: sqlite3.Connection,
) -> int:
    """
    Compute trend state for every date in ohlc_daily for this ticker.
    Inserts/replaces into stock_trend_state. Returns rows written.
    """
    rows = conn.execute(
        """SELECT trade_date, close FROM ohlc_daily
           WHERE ticker=? AND market=? ORDER BY trade_date""",
        (ticker, market)
    ).fetchall()

    if len(rows) < 10:
        return 0

    dates = [r[0] for r in rows]
    closes = np.array([r[1] for r in rows], dtype=float)
    n = len(closes)

    # Rolling SMAs via pandas (handles edges cleanly)
    s = pd.Series(closes)
    sma_50_arr  = s.rolling(50,  min_periods=1).mean().values
    sma_200_arr = s.rolling(200, min_periods=1).mean().values

    # Count of valid data points in each window (to check has_sma_50/200)
    cnt_50  = s.rolling(50,  min_periods=1).count().values
    cnt_200 = s.rolling(200, min_periods=1).count().values

    # 52-week high/low (252 trading days)
    high_52 = s.rolling(252, min_periods=1).max().values
    low_52  = s.rolling(252, min_periods=1).min().values
    spread = high_52 - low_52
    with np.errstate(divide="ignore", invalid="ignore"):
        position_52w = np.where(spread > 0, (closes - low_52) / spread, 0.5)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    out = []
    for i in range(n):
        has_50  = cnt_50[i]  >= 50
        has_200 = cnt_200[i] >= 200
        c   = float(closes[i])
        s50 = float(sma_50_arr[i])
        s200 = float(sma_200_arr[i])
        pos = float(position_52w[i])

        state, low_conf = _classify(c, s50, s200, pos, has_50, has_200)
        strength = _strength(c, s50, s200, pos, state, has_200)

        out.append((
            ticker, market, dates[i],
            state, strength,
            round(s50, 4), round(s200, 4) if has_200 else None,
            round(pos, 4), round(c, 4),
            1 if low_conf else 0,
            now_str,
        ))

    with conn:
        conn.executemany(
            """INSERT OR REPLACE INTO stock_trend_state
               (ticker, market, trade_date, trend_state, trend_strength,
                sma_50, sma_200, position_52w, close, low_confidence, computed_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            out,
        )
    return len(out)


# ──────────────────────────────────────────────────────────────────
# BACKFILL CONTEXT INTO signal_events + signal_outcomes
# ──────────────────────────────────────────────────────────────────

_UPDATE_SE_TREND = """
UPDATE signal_events
SET
    trend_state    = (
        SELECT ts.trend_state FROM stock_trend_state ts
        WHERE ts.ticker = signal_events.ticker
          AND ts.market = signal_events.market
          AND ts.trade_date = signal_events.signal_date
    ),
    trend_strength = (
        SELECT ts.trend_strength FROM stock_trend_state ts
        WHERE ts.ticker = signal_events.ticker
          AND ts.market = signal_events.market
          AND ts.trade_date = signal_events.signal_date
    )
WHERE trend_state IS NULL
  AND market = ?;
"""

_UPDATE_SO_TREND = """
UPDATE signal_outcomes
SET trend_state_at_signal = (
    SELECT ts.trend_state FROM stock_trend_state ts
    WHERE ts.ticker = signal_outcomes.ticker
      AND ts.market = signal_outcomes.market
      AND ts.trade_date = signal_outcomes.signal_date
)
WHERE trend_state_at_signal IS NULL
  AND market = ?;
"""


def backfill_trend_context(market: str, conn: sqlite3.Connection) -> None:
    print(f"  Backfilling trend_state into signal_events ({market})...")
    with conn:
        conn.execute(_UPDATE_SE_TREND, (market,))
    updated_se = conn.execute(
        "SELECT COUNT(*) FROM signal_events WHERE trend_state IS NOT NULL AND market=?",
        (market,)
    ).fetchone()[0]
    print(f"    signal_events with trend_state: {updated_se:,}")

    print(f"  Backfilling trend_state_at_signal into signal_outcomes ({market})...")
    with conn:
        conn.execute(_UPDATE_SO_TREND, (market,))
    updated_so = conn.execute(
        "SELECT COUNT(*) FROM signal_outcomes WHERE trend_state_at_signal IS NOT NULL AND market=?",
        (market,)
    ).fetchone()[0]
    print(f"    signal_outcomes with trend_state: {updated_so:,}")


# ──────────────────────────────────────────────────────────────────
# BATCH RUNNER
# ──────────────────────────────────────────────────────────────────

def run_all(
    market: str = "NSE",
    db_path: str = SIGNALS_DB_PATH,
    workers: int = 16,
) -> None:
    conn_main = get_conn(db_path)
    tickers = [r[0] for r in conn_main.execute(
        "SELECT DISTINCT ticker FROM ohlc_daily WHERE market=? ORDER BY ticker", (market,)
    ).fetchall()]
    conn_main.close()

    if not tickers:
        print("No OHLC data found. Run run_ohlc_sync.py first.")
        return

    print(f"\nTrend State Computation — {market} — {len(tickers)} tickers — {workers} workers")
    t0 = time.time()
    total_rows = 0
    done = 0

    def _worker(ticker: str) -> tuple[str, int]:
        c = get_conn(db_path)
        try:
            return ticker, compute_for_ticker(ticker, market, c)
        except Exception as exc:
            print(f"  [FAIL] {ticker}: {exc}")
            return ticker, -1
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker, t): t for t in tickers}
        for fut in as_completed(futures):
            _, n = fut.result()
            done += 1
            if n > 0:
                total_rows += n
            if done % 20 == 0 or done == len(tickers):
                print(f"  [{done}/{len(tickers)}] {total_rows:,} rows computed")

    print(f"\n  Done in {time.time()-t0:.1f}s — {total_rows:,} trend state rows")

    # Backfill context into signal tables
    conn_main = get_conn(db_path)
    backfill_trend_context(market, conn_main)
    conn_main.close()
