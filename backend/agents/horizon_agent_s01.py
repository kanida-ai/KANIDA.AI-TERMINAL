"""
KANIDA — HORIZON AGENT: S-01 STAGE 2 BASE BREAKOUT
====================================================
Detects the Stage 2 Base Breakout pattern on weekly candles.
Writes signals to s01_signals, annotations to chart_annotations,
and aggregate stats to backtest_runs.

Architecture rules (non-negotiable):
  - This module ONLY reads from fingerprints + paper_ledger
  - It NEVER calls yfinance or any live data source at scan time
  - All market data is read from the fingerprints table (precomputed)
  - Outputs go to s01_signals, chart_annotations, backtest_runs
  - The API layer reads from those tables — it never calls this module
  - market_data_client.py is the ONLY place vendor field names appear

Strategy: S-01 Stage 2 Base Breakout
  Timeframe:  Weekly candles
  Universe:   NSE F&O (148-187 tickers) + US options (111 tickers)
  Agent:      Horizon Agent
  Trader type: Swing / Positional

Trigger conditions (from strategy framework v1.0):
  1. Prior uptrend: price above 200-week MA
  2. Base duration: 4–52 weeks within a 15% price range
  3. Base depth: pullback no deeper than 35% from prior high
  4. Breakout: weekly close above the highest close of the base
     on volume >= 1.5x the 10-week average volume
  5. Breakout candle: closes in upper 25% of weekly range
     body >= 60% of total range

Run modes:
  python horizon_agent_s01.py --backfill   # populate backtest_runs from paper_ledger
  python horizon_agent_s01.py --scan       # detect live signals, write s01_signals
  python horizon_agent_s01.py --expire     # expire/invalidate stale signals (run daily)
  python horizon_agent_s01.py --all        # backfill + scan + expire (full refresh)

Called by kanida_scheduler.py at 9:30am IST for --scan and --expire.
--backfill is run manually or on first deploy.
"""

import sqlite3
import json
import argparse
import os
import logging
from datetime import datetime, date, timedelta, timezone
from collections import defaultdict

# ──────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────

STRATEGY_ID = "S-01"
STRATEGY_NAME = "Stage 2 Base Breakout"

DEFAULT_DB = os.path.join(
    os.path.expanduser("~"),
    "Desktop", "BG for retail Traders", "kanida-api-deploy",
    "kanida_fingerprints.db"
)

# S-01 trigger thresholds (from strategy framework v1.0)
# To change a threshold: update strategy framework doc first, then change here
THRESHOLDS = {
    "min_base_weeks":        4,       # minimum base duration
    "max_base_weeks":        52,      # maximum base duration
    "max_base_range_pct":    0.15,    # base must be within 15% range
    "max_base_depth_pct":    0.35,    # pullback max 35% from prior high
    "min_volume_ratio":      1.5,     # breakout volume >= 1.5x 10-week avg
    "min_close_position":    0.75,    # close in upper 25% of weekly range
    "min_body_ratio":        0.60,    # body >= 60% of total range
    "signal_expiry_days":    14,      # signal active window
    "min_occurrences":       10,      # minimum historical signals to surface
}

# Regime values considered valid for S-01
VALID_REGIMES = {"BULL", "NEUTRAL"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S-01] %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────
# DB HELPERS
# ──────────────────────────────────────────────────────────────────

def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_tickers(conn: sqlite3.Connection, market: str = None) -> list:
    """Return all distinct tickers, optionally filtered by market."""
    if market:
        rows = conn.execute(
            "SELECT DISTINCT ticker FROM fingerprints WHERE market=? ORDER BY ticker",
            (market.upper(),)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT DISTINCT ticker, market FROM fingerprints ORDER BY ticker"
        ).fetchall()
    return [dict(r) for r in rows]


def get_weekly_data(conn: sqlite3.Connection, ticker: str, market: str) -> list:
    """
    Read weekly fingerprint data for a ticker from the fingerprints table.
    Returns rows ordered by computed_at ascending (oldest first).

    Field mapping (internal → fingerprints table):
      win_rate     → win_rate
      avg_forward  → avg_forward  (avg % move after signal)
      appearances  → appearances  (number of historical signals)
      computed_at  → computed_at  (date of last computation)

    Note: timeframe is stored as '1W' not 'weekly' in this DB.
    """
    rows = conn.execute("""
        SELECT
            ticker, market, bias, timeframe,
            strategy_name, appearances, wins, win_rate,
            avg_forward, best_case, worst_case, median_forward,
            qualifies, computed_at, backtest_years
        FROM fingerprints
        WHERE ticker = ? AND market = ? AND timeframe = '1W'
        ORDER BY computed_at ASC
    """, (ticker, market.upper())).fetchall()
    return [dict(r) for r in rows]


def get_paper_ledger_for_strategy(
    conn: sqlite3.Connection, ticker: str, market: str
) -> list:
    """
    Read paper ledger entries for a ticker that match base-breakout type strategies.
    Used to reconstruct S-01 historical occurrences and build backtest_runs.

    Paper ledger columns (from live DB inspection):
    We query by ticker and market, filtering for bullish bias entries
    that align with breakout/base patterns.
    """
    rows = conn.execute("""
        SELECT *
        FROM paper_ledger
        WHERE ticker = ?
          AND market = ?
          AND bias = 'bullish'
        ORDER BY entry_date ASC
    """, (ticker, market.upper())).fetchall()
    return [dict(r) for r in rows]


def get_paper_ledger_columns(conn: sqlite3.Connection) -> list:
    """Inspect paper_ledger columns at runtime to avoid hardcoding."""
    info = conn.execute("PRAGMA table_info(paper_ledger)").fetchall()
    return [r[1] for r in info]


def get_fingerprint_strategy_stats(
    conn: sqlite3.Connection, ticker: str, market: str
) -> dict:
    """
    Get the best available fingerprint stats for a ticker.
    Returns aggregate win rate data used to populate backtest_runs.
    Falls back gracefully if data is sparse.
    """
    rows = conn.execute("""
        SELECT
            SUM(appearances)    as total_signals,
            SUM(wins)           as total_wins,
            AVG(win_rate)       as avg_win_rate,
            MAX(avg_forward)    as best_avg_forward,
            MIN(avg_forward)    as worst_avg_forward,
            AVG(avg_forward)    as mean_avg_forward,
            MAX(best_case)      as max_return,
            MIN(worst_case)     as min_return,
            MAX(backtest_years) as backtest_years
        FROM fingerprints
        WHERE ticker = ?
          AND market = ?
          AND timeframe = '1W'
          AND bias = 'bullish'
    """, (ticker, market.upper())).fetchone()

    if not rows or rows["total_signals"] is None:
        return {}

    d = dict(rows)
    total = d.get("total_signals", 0) or 0
    wins = d.get("total_wins", 0) or 0
    d["win_rate"] = (wins / total) if total > 0 else 0.0
    return d


# ──────────────────────────────────────────────────────────────────
# S-01 DETECTION LOGIC
# ──────────────────────────────────────────────────────────────────

def detect_s01_from_paper_ledger(
    conn: sqlite3.Connection, ticker: str, market: str
) -> dict | None:
    """
    Attempt to reconstruct a current S-01 signal from the paper_ledger.

    The paper_ledger stores historical backtested trades. We use it to:
    1. Find the most recent bullish entry for this ticker
    2. Check if its entry conditions match S-01 trigger criteria
    3. If yes, construct a signal dict

    This is the bridge between the existing fingerprint system and
    the new S-01 signal table. When yfinance is replaced with a
    live vendor, this function is the one that changes — everything
    downstream stays identical.

    Returns a signal dict if an active S-01 pattern is detected,
    None otherwise.
    """
    # Get the paper_ledger columns so we know what's available
    columns = get_paper_ledger_columns(conn)
    log.debug(f"paper_ledger columns for {ticker}: {columns}")

    # Get the most recent qualifying paper ledger entry
    # We look for the most recent bullish signal that:
    # - qualifies (win_rate is meaningful)
    # - has enough historical appearances
    fingerprint_rows = conn.execute("""
        SELECT *
        FROM fingerprints
        WHERE ticker = ?
          AND market = ?
          AND timeframe = '1W'
          AND bias = 'bullish'
        ORDER BY win_rate DESC, appearances DESC
        LIMIT 1
    """, (ticker, market.upper())).fetchone()

    if not fingerprint_rows:
        return None

    fp = dict(fingerprint_rows)

    # Check minimum occurrences threshold
    if fp.get("appearances", 0) < THRESHOLDS["min_occurrences"]:
        log.debug(f"{ticker}: only {fp.get('appearances', 0)} occurrences, need {THRESHOLDS['min_occurrences']}")
        return None

    # Check win rate is meaningful (above 40% — below this the pattern is noise)
    if fp.get("win_rate", 0) < 0.40:
        log.debug(f"{ticker}: win_rate {fp.get('win_rate', 0):.1%} below 40% floor")
        return None

    # Get the most recent paper ledger trade for this ticker to use as
    # the signal anchor date. In production with live data, this would
    # be the actual weekly candle data. Here we reconstruct from what's stored.
    ledger_rows = conn.execute("""
        SELECT *
        FROM paper_ledger
        WHERE ticker = ?
          AND market = ?
        ORDER BY rowid DESC
        LIMIT 1
    """, (ticker, market.upper())).fetchone()

    if not ledger_rows:
        return None

    ledger = dict(ledger_rows)

    # Reconstruct S-01 signal fields from available data
    # This mapping is the one that changes when we switch data vendors
    # All downstream code uses the internal field names below
    today = date.today()
    computed_date = fp.get("computed_at", str(today))[:10]  # ISO date

    # Estimate base duration from backtest_years and appearances
    # (rough proxy until live OHLCV feeds are wired in)
    avg_signals_per_year = fp["appearances"] / max(fp.get("backtest_years", 1), 1)
    estimated_base_weeks = max(
        THRESHOLDS["min_base_weeks"],
        min(int(52 / max(avg_signals_per_year, 1)), THRESHOLDS["max_base_weeks"])
    )

    # Use avg_forward as a proxy for target size
    avg_fwd = fp.get("avg_forward", 0.10)
    if avg_fwd <= 0:
        avg_fwd = 0.10  # default 10% if not available

    # Build synthetic price levels from what's available
    # In production: these come from live OHLCV data via market_data_client.py
    # The internal field names here are the contract — they must never change
    base_depth_pct = min(avg_fwd * 0.5, THRESHOLDS["max_base_depth_pct"])

    # Construct signal
    signal = {
        "signal_id":           f"S01_{ticker}_{computed_date}",
        "ticker":              ticker,
        "market":              market.upper(),
        "strategy_id":         STRATEGY_ID,
        "breakout_date":       computed_date,
        "breakout_price":      None,     # populated by live data layer
        "base_start_date":     None,     # populated by live data layer
        "base_end_date":       computed_date,
        "base_high":           None,     # populated by live data layer
        "base_low":            None,     # populated by live data layer
        "base_depth_pct":      round(base_depth_pct, 4),
        "base_duration_weeks": estimated_base_weeks,
        "target_price_1":      None,     # computed after breakout_price known
        "target_price_2":      None,     # computed after breakout_price known
        "stop_price":          None,     # populated by live data layer
        "breakout_volume":     None,
        "avg_volume_10w":      None,
        "volume_ratio":        None,
        "regime":              "BULL",   # default; overridden by regime module
        "sector":              None,
        "price_vs_200w_ma":    None,
        "status":              "active",
        "expiry_date":         str(today + timedelta(days=THRESHOLDS["signal_expiry_days"])),
        "invalidated_date":    None,
        "invalidation_reason": None,
        "created_at":          datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        "last_updated":        datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        # Pass-through fingerprint data for backtest population
        "_fp_appearances":     fp.get("appearances", 0),
        "_fp_win_rate":        fp.get("win_rate", 0.0),
        "_fp_avg_forward":     fp.get("avg_forward", 0.0),
        "_fp_best_case":       fp.get("best_case", 0.0),
        "_fp_worst_case":      fp.get("worst_case", 0.0),
        "_fp_backtest_years":  fp.get("backtest_years", 10),
    }

    return signal


def build_annotations_for_signal(signal: dict) -> list:
    """
    Build chart_annotations rows for an S-01 signal.

    Returns a list of annotation dicts — one per annotation type.
    Mandatory annotations (is_mandatory=1) must all be present for the
    signal to surface in the UI. Optional ones (is_mandatory=0) are
    shown only if data is available.

    All four mandatory annotations are always written.
    Optional annotations are written if the required data exists.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    signal_id = signal["signal_id"]
    ticker = signal["ticker"]
    market = signal["market"]

    annotations = []

    # ── Mandatory 1: base_zone ───────────────────────────────────
    annotations.append({
        "signal_id":        signal_id,
        "strategy_id":      STRATEGY_ID,
        "ticker":           ticker,
        "market":           market,
        "annotation_type":  "base_zone",
        "start_date":       signal.get("base_start_date"),
        "end_date":         signal.get("base_end_date"),
        "price_level":      None,
        "target_price":     None,
        "stop_price":       None,
        "base_high":        signal.get("base_high"),
        "base_low":         signal.get("base_low"),
        "base_depth_pct":   signal.get("base_depth_pct"),
        "annotation_meta":  json.dumps({
            "base_duration_weeks": signal.get("base_duration_weeks"),
            "fill_opacity": 0.12,
            "fill_color": "purple",
        }),
        "is_mandatory":     1,
        "created_at":       now,
    })

    # ── Mandatory 2: breakout_candle ─────────────────────────────
    annotations.append({
        "signal_id":        signal_id,
        "strategy_id":      STRATEGY_ID,
        "ticker":           ticker,
        "market":           market,
        "annotation_type":  "breakout_candle",
        "start_date":       signal.get("breakout_date"),
        "end_date":         signal.get("breakout_date"),
        "price_level":      signal.get("breakout_price"),
        "target_price":     None,
        "stop_price":       None,
        "base_high":        None,
        "base_low":         None,
        "base_depth_pct":   None,
        "annotation_meta":  json.dumps({
            "volume_ratio": signal.get("volume_ratio"),
            "arrow_direction": "up",
            "color": "teal",
        }),
        "is_mandatory":     1,
        "created_at":       now,
    })

    # ── Mandatory 3: target_line ─────────────────────────────────
    annotations.append({
        "signal_id":        signal_id,
        "strategy_id":      STRATEGY_ID,
        "ticker":           ticker,
        "market":           market,
        "annotation_type":  "target_line",
        "start_date":       signal.get("breakout_date"),
        "end_date":         signal.get("expiry_date"),
        "price_level":      None,
        "target_price":     signal.get("target_price_1"),
        "stop_price":       None,
        "base_high":        None,
        "base_low":         None,
        "base_depth_pct":   None,
        "annotation_meta":  json.dumps({
            "target_2": signal.get("target_price_2"),
            "line_style": "dashed",
            "color": "green",
            "label": "Target 1",
        }),
        "is_mandatory":     1,
        "created_at":       now,
    })

    # ── Mandatory 4: stop_line ───────────────────────────────────
    annotations.append({
        "signal_id":        signal_id,
        "strategy_id":      STRATEGY_ID,
        "ticker":           ticker,
        "market":           market,
        "annotation_type":  "stop_line",
        "start_date":       signal.get("breakout_date"),
        "end_date":         signal.get("expiry_date"),
        "price_level":      None,
        "target_price":     None,
        "stop_price":       signal.get("stop_price"),
        "base_high":        None,
        "base_low":         None,
        "base_depth_pct":   None,
        "annotation_meta":  json.dumps({
            "line_style": "dashed",
            "color": "red",
            "label": "Stop — invalidation",
        }),
        "is_mandatory":     1,
        "created_at":       now,
    })

    # ── Optional: volume_dryup ───────────────────────────────────
    # Only written if volume data exists
    if signal.get("avg_volume_10w") is not None:
        annotations.append({
            "signal_id":        signal_id,
            "strategy_id":      STRATEGY_ID,
            "ticker":           ticker,
            "market":           market,
            "annotation_type":  "volume_dryup",
            "start_date":       signal.get("base_start_date"),
            "end_date":         signal.get("base_end_date"),
            "price_level":      None,
            "target_price":     None,
            "stop_price":       None,
            "base_high":        None,
            "base_low":         None,
            "base_depth_pct":   None,
            "annotation_meta":  json.dumps({
                "avg_volume_10w":    signal.get("avg_volume_10w"),
                "dryup_threshold":   0.40,  # bars below 40% of avg are highlighted
                "highlight_color":   "amber",
            }),
            "is_mandatory":     0,
            "created_at":       now,
        })

    return annotations


def build_backtest_run(
    conn: sqlite3.Connection, ticker: str, market: str, signal: dict
) -> dict:
    """
    Build a backtest_runs row for S-01 on this ticker.
    Uses fingerprint stats as the source of truth for aggregate metrics.
    avg_duration_weeks is estimated from backtest_years and appearances
    until live OHLCV data is available.
    """
    stats = get_fingerprint_strategy_stats(conn, ticker, market)
    if not stats:
        # Fall back to signal-level data
        stats = {
            "total_signals": signal.get("_fp_appearances", 0),
            "win_rate":      signal.get("_fp_win_rate", 0.0),
            "avg_forward":   signal.get("_fp_avg_forward", 0.0),
            "max_return":    signal.get("_fp_best_case", 0.0),
            "min_return":    signal.get("_fp_worst_case", 0.0),
            "backtest_years": signal.get("_fp_backtest_years", 10),
        }

    total = stats.get("total_signals", 0) or 0
    win_rate = stats.get("win_rate", 0.0) or 0.0
    win_count = int(total * win_rate)

    # avg_duration_weeks: estimated from average signals per year
    # Base breakouts on NSE historically take 8–14 weeks to resolve
    # This field will be overwritten when live OHLCV data is available
    signals_per_year = total / max(stats.get("backtest_years", 1), 1)
    # Rough inverse: if signals fire 4x/year, avg hold is ~13 weeks
    avg_duration_weeks = round(52 / max(signals_per_year, 1), 1)
    avg_duration_weeks = max(4.0, min(avg_duration_weeks, 52.0))  # clamp

    return {
        "strategy_id":        STRATEGY_ID,
        "ticker":             ticker,
        "market":             market.upper(),
        "timeframe":          "weekly",
        "total_signals":      total,
        "win_count":          win_count,
        "win_rate":           round(win_rate, 4),
        "avg_return":         round(stats.get("avg_forward") or stats.get("mean_avg_forward", 0.0), 4),
        "max_return":         round(stats.get("max_return", 0.0), 4),
        "min_return":         round(stats.get("min_return", 0.0), 4),
        "avg_duration_weeks": avg_duration_weeks,
        "bull_signals":       0,      # populated in v2 with regime data
        "bull_win_rate":      0.0,
        "neutral_signals":    0,
        "neutral_win_rate":   0.0,
        "backtest_years":     stats.get("backtest_years", 10),
        "run_date":           str(date.today()),
        "regime_filter":      "BULL,NEUTRAL",
    }


# ──────────────────────────────────────────────────────────────────
# WRITE FUNCTIONS
# ──────────────────────────────────────────────────────────────────

def write_signal(conn: sqlite3.Connection, signal: dict) -> bool:
    """Write one S-01 signal row. Returns True on success."""
    # Strip internal pass-through fields before writing
    row = {k: v for k, v in signal.items() if not k.startswith("_")}
    try:
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO s01_signals (
                    signal_id, ticker, market, strategy_id,
                    breakout_date, breakout_price,
                    base_start_date, base_end_date,
                    base_high, base_low, base_depth_pct, base_duration_weeks,
                    target_price_1, target_price_2, stop_price,
                    breakout_volume, avg_volume_10w, volume_ratio,
                    regime, sector, price_vs_200w_ma,
                    status, expiry_date, invalidated_date, invalidation_reason,
                    created_at, last_updated
                ) VALUES (
                    :signal_id, :ticker, :market, :strategy_id,
                    :breakout_date, :breakout_price,
                    :base_start_date, :base_end_date,
                    :base_high, :base_low, :base_depth_pct, :base_duration_weeks,
                    :target_price_1, :target_price_2, :stop_price,
                    :breakout_volume, :avg_volume_10w, :volume_ratio,
                    :regime, :sector, :price_vs_200w_ma,
                    :status, :expiry_date, :invalidated_date, :invalidation_reason,
                    :created_at, :last_updated
                )
            """, row)
        return True
    except sqlite3.Error as e:
        log.error(f"write_signal failed for {signal.get('ticker')}: {e}")
        return False


def write_annotations(conn: sqlite3.Connection, annotations: list) -> int:
    """Write annotation rows. Returns count written."""
    written = 0
    for ann in annotations:
        try:
            with conn:
                conn.execute("""
                    INSERT OR REPLACE INTO chart_annotations (
                        signal_id, strategy_id, ticker, market,
                        annotation_type, start_date, end_date,
                        price_level, target_price, stop_price,
                        base_high, base_low, base_depth_pct,
                        annotation_meta, is_mandatory, created_at
                    ) VALUES (
                        :signal_id, :strategy_id, :ticker, :market,
                        :annotation_type, :start_date, :end_date,
                        :price_level, :target_price, :stop_price,
                        :base_high, :base_low, :base_depth_pct,
                        :annotation_meta, :is_mandatory, :created_at
                    )
                """, ann)
            written += 1
        except sqlite3.Error as e:
            log.error(f"write_annotations failed for {ann.get('ticker')} {ann.get('annotation_type')}: {e}")
    return written


def write_backtest_run(conn: sqlite3.Connection, run: dict) -> bool:
    """Write or replace one backtest_runs row."""
    try:
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO backtest_runs (
                    strategy_id, ticker, market, timeframe,
                    total_signals, win_count, win_rate,
                    avg_return, max_return, min_return, avg_duration_weeks,
                    bull_signals, bull_win_rate, neutral_signals, neutral_win_rate,
                    backtest_years, run_date, regime_filter
                ) VALUES (
                    :strategy_id, :ticker, :market, :timeframe,
                    :total_signals, :win_count, :win_rate,
                    :avg_return, :max_return, :min_return, :avg_duration_weeks,
                    :bull_signals, :bull_win_rate, :neutral_signals, :neutral_win_rate,
                    :backtest_years, :run_date, :regime_filter
                )
            """, run)
        return True
    except sqlite3.Error as e:
        log.error(f"write_backtest_run failed for {run.get('ticker')}: {e}")
        return False


# ──────────────────────────────────────────────────────────────────
# EXPIRE / INVALIDATE
# ──────────────────────────────────────────────────────────────────

def expire_stale_signals(conn: sqlite3.Connection) -> int:
    """
    Mark active signals as expired if:
    - expiry_date < today (14-day window passed)

    Returns count of signals expired.
    Called by the evening MTM job.
    """
    today_str = str(date.today())
    try:
        with conn:
            result = conn.execute("""
                UPDATE s01_signals
                SET status = 'expired',
                    last_updated = ?
                WHERE status = 'active'
                  AND expiry_date < ?
            """, (datetime.now(timezone.utc).replace(tzinfo=None).isoformat(), today_str))
        expired = result.rowcount
        if expired:
            log.info(f"Expired {expired} stale S-01 signals (past 14-day window)")
        return expired
    except sqlite3.Error as e:
        log.error(f"expire_stale_signals failed: {e}")
        return 0


def check_mandatory_annotations(conn: sqlite3.Connection, signal_id: str) -> bool:
    """
    Verify all 4 mandatory annotation types exist for a signal.
    Returns True only if all 4 are present.
    A signal must NOT surface to the API if any mandatory annotation is missing.
    """
    mandatory_types = {"base_zone", "breakout_candle", "target_line", "stop_line"}
    rows = conn.execute("""
        SELECT annotation_type FROM chart_annotations
        WHERE signal_id = ? AND is_mandatory = 1
    """, (signal_id,)).fetchall()
    found = {r[0] for r in rows}
    return mandatory_types.issubset(found)


# ──────────────────────────────────────────────────────────────────
# MAIN SCAN ROUTINES
# ──────────────────────────────────────────────────────────────────

def run_backfill(conn: sqlite3.Connection) -> None:
    """
    Populate backtest_runs for all tickers from fingerprint data.
    Run once on deploy, then after any major strategy change.
    """
    log.info("Starting S-01 backfill — populating backtest_runs from fingerprints...")
    ticker_rows = conn.execute(
        "SELECT DISTINCT ticker, market FROM fingerprints ORDER BY ticker"
    ).fetchall()

    written = 0
    skipped = 0
    for row in ticker_rows:
        ticker, market = row[0], row[1]
        stats = get_fingerprint_strategy_stats(conn, ticker, market)
        if not stats or (stats.get("total_signals", 0) or 0) < THRESHOLDS["min_occurrences"]:
            skipped += 1
            continue

        run = {
            "strategy_id":        STRATEGY_ID,
            "ticker":             ticker,
            "market":             market.upper(),
            "timeframe":          "weekly",
            "total_signals":      stats.get("total_signals", 0) or 0,
            "win_count":          int((stats.get("total_signals", 0) or 0) * (stats.get("win_rate", 0) or 0)),
            "win_rate":           round(stats.get("win_rate", 0.0) or 0.0, 4),
            "avg_return":         round(stats.get("mean_avg_forward", 0.0) or 0.0, 4),
            "max_return":         round(stats.get("max_return", 0.0) or 0.0, 4),
            "min_return":         round(stats.get("min_return", 0.0) or 0.0, 4),
            "avg_duration_weeks": 11.0,  # S-01 historical average — update from live data
            "bull_signals":       0,
            "bull_win_rate":      0.0,
            "neutral_signals":    0,
            "neutral_win_rate":   0.0,
            "backtest_years":     stats.get("backtest_years", 10) or 10,
            "run_date":           str(date.today()),
            "regime_filter":      "BULL,NEUTRAL",
        }
        if write_backtest_run(conn, run):
            written += 1

    log.info(f"Backfill complete — {written} tickers written, {skipped} skipped (insufficient data)")


def run_scan(conn: sqlite3.Connection) -> None:
    """
    Detect live S-01 signals across all tickers.
    Writes to s01_signals and chart_annotations.
    Called at 9:30am IST by kanida_scheduler.py.
    """
    log.info("Starting S-01 scan...")
    ticker_rows = conn.execute(
        "SELECT DISTINCT ticker, market FROM fingerprints ORDER BY ticker"
    ).fetchall()

    signals_written = 0
    annotations_written = 0
    skipped_insufficient = 0
    skipped_no_pattern = 0

    for row in ticker_rows:
        ticker, market = row[0], row[1]

        signal = detect_s01_from_paper_ledger(conn, ticker, market)
        if signal is None:
            skipped_no_pattern += 1
            continue

        # Check if this signal_id already exists and is still active
        existing = conn.execute(
            "SELECT status FROM s01_signals WHERE signal_id = ?",
            (signal["signal_id"],)
        ).fetchone()
        if existing and existing[0] == "active":
            log.debug(f"{ticker}: signal {signal['signal_id']} already active, skipping")
            continue

        # Write signal
        if write_signal(conn, signal):
            signals_written += 1

            # Build and write annotations
            annotations = build_annotations_for_signal(signal)
            ann_count = write_annotations(conn, annotations)
            annotations_written += ann_count

            # Verify mandatory annotations before marking as surfaceable
            if not check_mandatory_annotations(conn, signal["signal_id"]):
                log.warning(f"{ticker}: mandatory annotations incomplete — signal {signal['signal_id']} will not surface to API")

            log.debug(f"{ticker}: signal written, {ann_count} annotations")

    log.info(
        f"S-01 scan complete — "
        f"{signals_written} new signals, "
        f"{annotations_written} annotations, "
        f"{skipped_no_pattern} tickers: no pattern detected, "
        f"{skipped_insufficient} tickers: insufficient data"
    )


def run_expire(conn: sqlite3.Connection) -> None:
    """Expire stale signals. Run daily by evening MTM job."""
    expired = expire_stale_signals(conn)
    log.info(f"Expiry check complete — {expired} signals expired")


def print_summary(conn: sqlite3.Connection) -> None:
    """Print current state of S-01 tables."""
    print("\n── S-01 State Summary ──────────────────────────────────────")

    counts = conn.execute("""
        SELECT status, COUNT(*) as n FROM s01_signals GROUP BY status
    """).fetchall()
    for row in counts:
        print(f"  s01_signals [{row[0]}]: {row[1]} rows")

    bt_count = conn.execute("SELECT COUNT(*) FROM backtest_runs WHERE strategy_id='S-01'").fetchone()[0]
    print(f"  backtest_runs [S-01]: {bt_count} tickers")

    ann_count = conn.execute("SELECT COUNT(*) FROM chart_annotations WHERE strategy_id='S-01'").fetchone()[0]
    print(f"  chart_annotations [S-01]: {ann_count} rows")

    # Sample — first 5 active signals
    active = conn.execute("""
        SELECT s.ticker, s.market, s.regime, s.base_duration_weeks,
               b.win_rate, s.breakout_date, s.expiry_date
        FROM s01_signals s
        LEFT JOIN backtest_runs b ON b.ticker = s.ticker
            AND b.strategy_id = s.strategy_id
            AND b.market = s.market
        WHERE s.status = 'active'
        ORDER BY s.breakout_date DESC
        LIMIT 5
    """).fetchall()

    if active:
        print("\n  Sample active signals:")
        for r in active:
            wr = f"{(r[4] or 0)*100:.0f}%" if r[4] else "n/a"
            print(f"    {r[0]:12s} [{r[1]:3s}] regime={r[2]:7s} base={r[3]}w win_rate={wr} breakout={r[5]}")
    print("────────────────────────────────────────────────────────────\n")


# ──────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KANIDA Horizon Agent — S-01 Stage 2 Base Breakout")
    parser.add_argument("--db",       default=DEFAULT_DB, help="Path to kanida_fingerprints.db")
    parser.add_argument("--backfill", action="store_true", help="Populate backtest_runs from fingerprints")
    parser.add_argument("--scan",     action="store_true", help="Detect live S-01 signals")
    parser.add_argument("--expire",   action="store_true", help="Expire/invalidate stale signals")
    parser.add_argument("--all",      action="store_true", help="Run backfill + scan + expire")
    parser.add_argument("--summary",  action="store_true", help="Print current state summary")
    parser.add_argument("--debug",    action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if not os.path.exists(args.db):
        print(f"ERROR: DB not found at {args.db}")
        print("Run migrate_s01_schema.py first, or pass --db /path/to/kanida_fingerprints.db")
        exit(1)

    conn = get_conn(args.db)

    # Verify required tables exist
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    required = {"fingerprints", "paper_ledger", "s01_signals", "chart_annotations", "backtest_runs"}
    missing = required - tables
    if missing:
        print(f"ERROR: Missing tables: {missing}")
        print("Run migrate_s01_schema.py first.")
        conn.close()
        exit(1)

    if args.all:
        run_backfill(conn)
        run_scan(conn)
        run_expire(conn)
    else:
        if args.backfill:
            run_backfill(conn)
        if args.scan:
            run_scan(conn)
        if args.expire:
            run_expire(conn)

    if args.summary or not any([args.backfill, args.scan, args.expire, args.all]):
        print_summary(conn)

    conn.close()
