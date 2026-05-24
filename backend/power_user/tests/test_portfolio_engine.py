"""Tests for portfolio_engine — Co-Trader entry/exit/equity logic.

Strategy:
  - Run the engine against a tmp SQLite seeded with:
      * 5 days of synthetic OHLC for 3 symbols
      * 5 days of synthetic falcon_signals_live rows
    Then assert what the engine does with them.
  - Cover: entry budget capping, SL trigger, TARGET trigger, TIME exit,
    backfill idempotency, equity math.
  - We don't go through compute_top_n (that needs the full pattern DB).
    Instead we PRE-POPULATE falcon_signals_live so the engine takes the
    fast read path, and we keep the pattern table out of the way.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from datetime import date, timedelta

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.normpath(os.path.join(_HERE, "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from power_user.db_init import init_power_user_schema     # noqa: E402
from power_user.services import portfolio_engine          # noqa: E402
from power_user.services.portfolio_defs import (         # noqa: E402
    DAILY_TRADER,
    WEEKLY_TRADER,
)


# ──────────────────────────────────────────────────────────────────────────
# Fixture: tmp DB with the schema + tables the engine needs to read from
# ──────────────────────────────────────────────────────────────────────────

@pytest.fixture
def db_with_signals():
    """Create a tmp DB, init schema, then populate minimal ohlc_daily +
    falcon_signals_live so the engine has something to chew on."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_power_user_schema(path)

    con = sqlite3.connect(path)
    # Add the engine read-only tables the portfolio engine touches.
    # falcon_promoted_patterns is empty — the engine's fallback compute_top_n
    # path needs the table to exist (it returns [] when there are no patterns).
    con.executescript("""
        CREATE TABLE IF NOT EXISTS ohlc_daily (
            symbol     TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open       REAL, high REAL, low REAL, close REAL, volume REAL,
            PRIMARY KEY(symbol, trade_date)
        );
        CREATE TABLE IF NOT EXISTS falcon_signals_live (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_date TEXT NOT NULL, entry_date TEXT,
            rank INTEGER NOT NULL, symbol TEXT NOT NULL,
            sector TEXT, n_fires INTEGER DEFAULT 1, score REAL NOT NULL,
            close_at_signal REAL, avg_value_60d REAL,
            fired_pattern_ids TEXT, sample_rules TEXT,
            engine_version TEXT NOT NULL DEFAULT '7.1.0',
            emitted_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(signal_date, rank, symbol)
        );
        CREATE TABLE IF NOT EXISTS falcon_promoted_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_id TEXT NOT NULL UNIQUE,
            rule_json  TEXT NOT NULL,
            trader_phrase TEXT, hit_phrase TEXT,
            mined_year INTEGER, oos_lift_pp REAL,
            is_universal INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS falcon_features (
            symbol     TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            PRIMARY KEY(symbol, trade_date)
        );
    """)
    yield con, path
    con.close()
    try:
        os.unlink(path)
    except OSError:
        pass


def _seed_ohlc(con, symbol: str, prices: dict[str, tuple[float, float, float, float]]):
    """prices: {date_iso: (open, high, low, close)}."""
    for d, (o, h, l, c) in prices.items():
        con.execute("INSERT OR REPLACE INTO ohlc_daily(symbol,trade_date,open,high,low,close,volume) "
                    "VALUES(?,?,?,?,?,?,?)", (symbol, d, o, h, l, c, 1_000_000))
    con.commit()


def _seed_signal(con, signal_date: str, entry_date: str,
                  rank: int, symbol: str, score: float, close_at_signal: float,
                  sector: str = "Test"):
    con.execute("""
        INSERT OR REPLACE INTO falcon_signals_live
          (signal_date, entry_date, rank, symbol, sector, score, close_at_signal)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (signal_date, entry_date, rank, symbol, sector, score, close_at_signal))
    con.commit()


# ──────────────────────────────────────────────────────────────────────────
# Seed + identity tests
# ──────────────────────────────────────────────────────────────────────────

def test_seed_definitions_inserts_five(db_with_signals):
    con, _ = db_with_signals
    n = portfolio_engine.seed_portfolio_definitions(con)
    assert n == 5
    rows = con.execute(
        "SELECT slug FROM portfolio_definitions ORDER BY display_order"
    ).fetchall()
    assert [r[0] for r in rows] == [
        "daily-trader", "patient-trader", "weekly-trader", "monthly-trader", "btst-trader"
    ]


def test_seed_definitions_idempotent(db_with_signals):
    con, _ = db_with_signals
    portfolio_engine.seed_portfolio_definitions(con)
    portfolio_engine.seed_portfolio_definitions(con)
    n = con.execute("SELECT COUNT(*) FROM portfolio_definitions").fetchone()[0]
    assert n == 5


# ──────────────────────────────────────────────────────────────────────────
# Entry rule
# ──────────────────────────────────────────────────────────────────────────

def test_champion_opens_top14_on_entry_date(db_with_signals):
    con, _ = db_with_signals
    portfolio_engine.seed_portfolio_definitions(con)

    # Day 1: emit 3 signals for 3 stocks
    _seed_signal(con, "2026-04-13", "2026-04-14", 1, "AAA", score=100, close_at_signal=200.0)
    _seed_signal(con, "2026-04-13", "2026-04-14", 2, "BBB", score=95,  close_at_signal=400.0)
    _seed_signal(con, "2026-04-13", "2026-04-14", 3, "CCC", score=90,  close_at_signal=800.0)
    # OHLC for the entry date
    _seed_ohlc(con, "AAA", {"2026-04-14": (200.0, 210.0, 199.0, 205.0)})
    _seed_ohlc(con, "BBB", {"2026-04-14": (400.0, 405.0, 395.0, 402.0)})
    _seed_ohlc(con, "CCC", {"2026-04-14": (800.0, 815.0, 790.0, 810.0)})

    portfolio_engine.run_eod_for_portfolio(con, DAILY_TRADER, "2026-04-13")

    rows = con.execute("""
        SELECT symbol, qty, entry_price, sl_level FROM portfolio_positions
         WHERE exit_date IS NULL ORDER BY symbol
    """).fetchall()
    syms = {r[0] for r in rows}
    assert syms == {"AAA", "BBB", "CCC"}
    # Champion sl_pct = -7% → sl_level = entry × 0.93
    for sym, qty, entry, sl in rows:
        assert qty > 0
        assert abs(sl - entry * 0.93) < 0.01


def test_expiry_trader_skips_non_tuesday(db_with_signals):
    """Tuesday is weekday() == 1. Use a Wednesday signal date."""
    con, _ = db_with_signals
    portfolio_engine.seed_portfolio_definitions(con)

    # 2026-04-15 is Wednesday
    _seed_signal(con, "2026-04-15", "2026-04-16", 1, "AAA", score=100, close_at_signal=200.0)
    _seed_ohlc(con, "AAA", {"2026-04-16": (200, 210, 199, 205)})

    portfolio_engine.run_eod_for_portfolio(con, WEEKLY_TRADER, "2026-04-15")
    n_open = con.execute(
        "SELECT COUNT(*) FROM portfolio_positions WHERE exit_date IS NULL"
    ).fetchone()[0]
    assert n_open == 0, "Expiry Trader must not open on Wednesday"


def test_expiry_trader_opens_on_tuesday(db_with_signals):
    """2026-04-14 is Tuesday (Mon=0, Tue=1)."""
    con, _ = db_with_signals
    portfolio_engine.seed_portfolio_definitions(con)

    assert date(2026, 4, 14).weekday() == 1   # sanity
    _seed_signal(con, "2026-04-14", "2026-04-15", 1, "AAA", score=100, close_at_signal=200.0)
    _seed_ohlc(con, "AAA", {"2026-04-15": (200, 210, 199, 205)})

    portfolio_engine.run_eod_for_portfolio(con, WEEKLY_TRADER, "2026-04-14")
    rows = con.execute(
        "SELECT symbol FROM portfolio_positions WHERE exit_date IS NULL"
    ).fetchall()
    assert {r[0] for r in rows} == {"AAA"}


def test_no_duplicate_entries_same_symbol_same_day(db_with_signals):
    """Re-running EOD for the same date must NOT open the same position twice."""
    con, _ = db_with_signals
    portfolio_engine.seed_portfolio_definitions(con)

    _seed_signal(con, "2026-04-13", "2026-04-14", 1, "AAA", score=100, close_at_signal=200.0)
    _seed_ohlc(con, "AAA", {"2026-04-14": (200, 210, 199, 205)})

    portfolio_engine.run_eod_for_portfolio(con, DAILY_TRADER, "2026-04-13")
    portfolio_engine.run_eod_for_portfolio(con, DAILY_TRADER, "2026-04-13")     # re-run

    n = con.execute(
        "SELECT COUNT(*) FROM portfolio_positions WHERE symbol='AAA'"
    ).fetchone()[0]
    assert n == 1


# ──────────────────────────────────────────────────────────────────────────
# Exit rules
# ──────────────────────────────────────────────────────────────────────────

def test_sl_fires_when_low_pierces_stop(db_with_signals):
    con, _ = db_with_signals
    portfolio_engine.seed_portfolio_definitions(con)

    _seed_signal(con, "2026-04-13", "2026-04-14", 1, "AAA", score=100, close_at_signal=200.0)
    _seed_ohlc(con, "AAA", {
        "2026-04-14": (200, 210, 199, 205),
        # Day 2 low pierces SL (200 × 0.93 = 186)
        "2026-04-15": (203, 204, 185, 188),
    })
    portfolio_engine.run_eod_for_portfolio(con, DAILY_TRADER, "2026-04-13")
    # Now process day 2 — SL should fire
    portfolio_engine.run_eod_for_portfolio(con, DAILY_TRADER, "2026-04-15")

    row = con.execute(
        "SELECT exit_reason, exit_price FROM portfolio_positions WHERE symbol='AAA'"
    ).fetchone()
    assert row[0] == "SL"
    assert abs(row[1] - 200.0 * 0.93) < 0.01


def test_target_fires_when_high_reaches_target(db_with_signals):
    """Weekly Trader (V3) has fixed +12% target — seed OHLC to pierce it."""
    con, _ = db_with_signals
    portfolio_engine.seed_portfolio_definitions(con)

    # Tuesday signal so Weekly Trader engages
    assert date(2026, 4, 14).weekday() == 1
    _seed_signal(con, "2026-04-14", "2026-04-15", 1, "AAA", score=100, close_at_signal=100.0)
    _seed_ohlc(con, "AAA", {
        "2026-04-15": (100, 101, 99, 100),
        # Day 2 high reaches +12% target (100 × 1.12 = 112)
        "2026-04-16": (101, 113, 100, 110),
    })

    portfolio_engine.run_eod_for_portfolio(con, WEEKLY_TRADER, "2026-04-14")
    portfolio_engine.run_eod_for_portfolio(con, WEEKLY_TRADER, "2026-04-16")

    row = con.execute(
        "SELECT exit_reason, exit_price FROM portfolio_positions WHERE symbol='AAA'"
    ).fetchone()
    assert row[0] == "TARGET"
    assert abs(row[1] - 112.0) < 0.01     # Weekly Trader's +12% target


def test_time_exit_after_hold_window(db_with_signals):
    """Champion exits after 7 trading days max if nothing else triggers."""
    con, _ = db_with_signals
    portfolio_engine.seed_portfolio_definitions(con)

    _seed_signal(con, "2026-04-13", "2026-04-14", 1, "AAA", score=100, close_at_signal=100.0)

    # 8 days of flat-ish OHLC — neither SL nor target
    days = ["2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17",
            "2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23"]
    for d in days:
        _seed_ohlc(con, "AAA", {d: (100, 101, 99, 100.5)})

    portfolio_engine.run_eod_for_portfolio(con, DAILY_TRADER, "2026-04-13")
    for d in days[1:]:   # day 2..8
        portfolio_engine.run_eod_for_portfolio(con, DAILY_TRADER, d)

    row = con.execute(
        "SELECT exit_reason, holding_days FROM portfolio_positions WHERE symbol='AAA'"
    ).fetchone()
    assert row[0] == "TIME"
    assert row[1] >= 7


# ──────────────────────────────────────────────────────────────────────────
# Equity math
# ──────────────────────────────────────────────────────────────────────────

def test_equity_row_written_per_eod(db_with_signals):
    con, _ = db_with_signals
    portfolio_engine.seed_portfolio_definitions(con)

    _seed_signal(con, "2026-04-13", "2026-04-14", 1, "AAA", score=100, close_at_signal=100.0)
    _seed_ohlc(con, "AAA", {"2026-04-14": (100, 102, 99, 101)})

    portfolio_engine.run_eod_for_portfolio(con, DAILY_TRADER, "2026-04-13")

    rows = con.execute("""
        SELECT trade_date, total_equity, cumulative_return_pct, n_open_positions
          FROM portfolio_equity_history ORDER BY trade_date
    """).fetchall()
    assert len(rows) == 1
    _, eq, ret, n_open = rows[0]
    # V3 lock: ₹5 L starting capital — after opening a position, equity ≈ 5 L
    # still (capital just shifts from cash to deployed). Tiny variance from rounding.
    assert abs(eq - 500_000) < 5_000
    # cumulative return small (entry day, no MTM movement to speak of)
    assert -1.0 < ret < 2.0
    assert n_open == 1


def test_event_log_records_enter_and_exit(db_with_signals):
    con, _ = db_with_signals
    portfolio_engine.seed_portfolio_definitions(con)

    _seed_signal(con, "2026-04-13", "2026-04-14", 1, "AAA", score=100, close_at_signal=100.0)
    _seed_ohlc(con, "AAA", {
        "2026-04-14": (100, 102, 99, 101),
        "2026-04-15": (101, 102, 92, 93),   # pierces SL = 93
    })
    portfolio_engine.run_eod_for_portfolio(con, DAILY_TRADER, "2026-04-13")
    portfolio_engine.run_eod_for_portfolio(con, DAILY_TRADER, "2026-04-15")

    events = con.execute("""
        SELECT event_type, symbol FROM portfolio_event_log
         ORDER BY event_date, id
    """).fetchall()
    types = [e[0] for e in events]
    assert "ENTER"   in types
    assert "EXIT_SL" in types


# ──────────────────────────────────────────────────────────────────────────
# Backfill idempotency
# ──────────────────────────────────────────────────────────────────────────

def test_backfill_is_idempotent(db_with_signals):
    """Re-running backfill over the same range produces the same equity rows."""
    con, _ = db_with_signals
    portfolio_engine.seed_portfolio_definitions(con)

    _seed_signal(con, "2026-04-13", "2026-04-14", 1, "AAA", score=100, close_at_signal=100.0)
    _seed_ohlc(con, "AAA", {
        "2026-04-13": (100, 100, 100, 100),
        "2026-04-14": (100, 102, 99, 101),
    })

    # First pass
    portfolio_engine.run_eod_for_portfolio(con, DAILY_TRADER, "2026-04-13")
    portfolio_engine.run_eod_for_portfolio(con, DAILY_TRADER, "2026-04-14")
    rows_a = con.execute("""
        SELECT trade_date, total_equity FROM portfolio_equity_history ORDER BY trade_date
    """).fetchall()
    n_pos_a = con.execute(
        "SELECT COUNT(*) FROM portfolio_positions"
    ).fetchone()[0]

    # Re-run
    portfolio_engine.run_eod_for_portfolio(con, DAILY_TRADER, "2026-04-13")
    portfolio_engine.run_eod_for_portfolio(con, DAILY_TRADER, "2026-04-14")
    rows_b = con.execute("""
        SELECT trade_date, total_equity FROM portfolio_equity_history ORDER BY trade_date
    """).fetchall()
    n_pos_b = con.execute(
        "SELECT COUNT(*) FROM portfolio_positions"
    ).fetchone()[0]

    assert n_pos_a == n_pos_b      # no duplicate positions
    assert len(rows_a) == len(rows_b)
    # equity values must be identical (REPLACE on conflict)
    for a, b in zip(rows_a, rows_b):
        assert a[0] == b[0]
        assert abs(a[1] - b[1]) < 1.0
