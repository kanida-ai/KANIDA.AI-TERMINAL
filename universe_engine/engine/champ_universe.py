"""
Universe filtering for the championship-style backtest.

Two responsibilities:
  1. Build a corp-action / abnormal-move exclusion calendar — derived from any
     single-day move > 20% in ohlc_daily. Treats both real corp actions
     (splits/bonuses) and fundamental shocks as "do-not-trade" days.
  2. Provide an `is_eligible(symbol, date)` lookup that respects the exclusion
     windows AND the symbol's own first-bar date (no trading before it listed).

Why exclusion instead of full price adjustment: the DD memo flagged 59 such bars
in the daily DB. Building a proper corp-action overlay (split factors, dividend
adjustments) is a multi-day job. An exclusion filter is a tighter scope that
gets the same protection from fake winners — at the cost of skipping some
legitimate post-event continuation moves. Acceptable for MVP; upgradeable.
"""
from __future__ import annotations
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Set, Tuple


EXCLUSION_LOOKBACK_DAYS  = 5
EXCLUSION_LOOKAHEAD_DAYS = 30
ABNORMAL_MOVE_THRESHOLD  = 0.20    # >20% single-day move = treat as suspect


def ensure_corp_action_table(con: sqlite3.Connection):
    con.executescript("""
    CREATE TABLE IF NOT EXISTS corp_action_calendar (
        symbol     TEXT NOT NULL,
        event_date TEXT NOT NULL,
        move_pct   REAL,
        source     TEXT NOT NULL DEFAULT 'derived_abnormal',
        PRIMARY KEY (symbol, event_date)
    );
    CREATE INDEX IF NOT EXISTS idx_corp_sym ON corp_action_calendar(symbol);
    """)
    con.commit()


def build_calendar_from_abnormal_moves(con: sqlite3.Connection,
                                        threshold: float = ABNORMAL_MOVE_THRESHOLD,
                                        verbose: bool = True) -> int:
    """Find every single-day move > threshold and persist as a corp-action event.
    Returns count of events written."""
    ensure_corp_action_table(con)
    rows = con.execute("""
        SELECT symbol, trade_date, move_pct FROM (
            SELECT symbol, trade_date, close,
                   LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_close,
                   (close / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date), 0) - 1.0) AS move_pct
            FROM ohlc_daily
        ) WHERE prev_close > 0 AND ABS(move_pct) > ?
    """, (threshold,)).fetchall()

    if verbose:
        print(f"[corp-action] Found {len(rows)} abnormal moves (>{threshold*100:.0f}%)")
    payload = [(s, d, float(m), 'derived_abnormal') for s, d, m in rows]
    con.executemany("""
        INSERT OR REPLACE INTO corp_action_calendar
            (symbol, event_date, move_pct, source) VALUES (?, ?, ?, ?)
    """, payload)
    con.commit()
    return len(payload)


# ── Lookup ────────────────────────────────────────────────────────────────────

class UniverseFilter:
    """
    Cached lookup for whether (symbol, date) is eligible for trading.

    Pre-loads:
      - First-bar date per symbol (from ohlc_daily)
      - Set of (symbol, blocked_date) within exclusion windows of every event
    """
    def __init__(self, con: sqlite3.Connection, universe: List[str] = None):
        self.con = con
        self._first_bar: Dict[str, date] = {}
        self._blocked: Set[Tuple[str, date]] = set()
        self._load(universe)

    def _load(self, universe):
        # First-bar dates
        rows = self.con.execute(
            "SELECT symbol, MIN(trade_date) FROM ohlc_daily GROUP BY symbol"
        ).fetchall()
        self._first_bar = {s: date.fromisoformat(d) for s, d in rows if d}

        # Corp-action exclusion windows
        events = self.con.execute(
            "SELECT symbol, event_date FROM corp_action_calendar"
        ).fetchall()
        for sym, ev_d in events:
            if universe and sym not in universe:
                continue
            ev_d = date.fromisoformat(ev_d)
            for offset in range(-EXCLUSION_LOOKBACK_DAYS, EXCLUSION_LOOKAHEAD_DAYS + 1):
                self._blocked.add((sym, ev_d + timedelta(days=offset)))

    def is_eligible(self, symbol: str, on_date: date) -> bool:
        first = self._first_bar.get(symbol)
        if first is None or on_date < first:
            return False
        return (symbol, on_date) not in self._blocked

    def filter_universe(self, universe: List[str], on_date: date) -> List[str]:
        return [s for s in universe if self.is_eligible(s, on_date)]


def get_active_universe(con: sqlite3.Connection,
                         index_col: str = "in_nifty200") -> List[str]:
    rows = con.execute(f"""
        SELECT symbol FROM universe_master
        WHERE is_active = 1 AND {index_col} = 1 ORDER BY symbol
    """).fetchall()
    return [r[0] for r in rows]
