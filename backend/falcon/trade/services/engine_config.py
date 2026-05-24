"""Engine Playbook — operator-level rules for staging NEW trades.

This is the singleton config that the /falcon/trade panel reads to:
  * size each trade (per_trade_cash = total_capital × per_trade_pct/100)
  * cap the daily pick count (daily_picks_max)
  * filter out already-held positions (skip_already_held)

Distinct from `falcon_trail_config` (per-product MTF/CNC) which governs
already-placed positions. This one only affects NEW staging.

Singleton pattern: enforced at the DB layer with CHECK(id=1). One row, ever.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from ...db import falcon_conn


def get_config() -> Dict[str, Any]:
    """Return the current playbook. Always returns a dict (creates the seed row
    on first call if for some reason it's missing)."""
    with falcon_conn() as con:
        row = con.execute("SELECT * FROM falcon_engine_config WHERE id=1").fetchone()
        if row is None:
            con.execute(
                """INSERT INTO falcon_engine_config
                     (id, per_trade_pct, daily_picks_max, skip_already_held,
                      mining_window_years, updated_at)
                   VALUES (1, 6.0, 14, 1, 4, ?)""",
                (datetime.now().isoformat(),)
            )
            con.commit()
            row = con.execute("SELECT * FROM falcon_engine_config WHERE id=1").fetchone()
    return dict(row)


def upsert_config(*,
                  per_trade_pct:       float,
                  daily_picks_max:     int,
                  skip_already_held:   bool,
                  mining_window_years: int = 4) -> Dict[str, Any]:
    """Save the playbook. Validates ranges and returns the persisted row.

    `mining_window_years` controls the B4 publish_patterns.py cutoff:
    only patterns with `mined_year >= (current_calendar_year - this)` get
    published to PROD. Default 4 (R&D-validated 2026-05-10 sweep showed
    4-yr is the optimal cutoff in calm trending regimes; flip to 5 for
    stress-month defense).
    """
    if per_trade_pct <= 0 or per_trade_pct > 100:
        raise ValueError("per_trade_pct must be between 0 and 100 (exclusive of 0)")
    if daily_picks_max < 1 or daily_picks_max > 200:
        raise ValueError("daily_picks_max must be between 1 and 200")
    if mining_window_years < 1 or mining_window_years > 20:
        raise ValueError("mining_window_years must be between 1 and 20 (inclusive)")

    with falcon_conn() as con:
        con.execute(
            """INSERT INTO falcon_engine_config
                 (id, per_trade_pct, daily_picks_max, skip_already_held,
                  mining_window_years, updated_at)
               VALUES (1, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 per_trade_pct        = excluded.per_trade_pct,
                 daily_picks_max      = excluded.daily_picks_max,
                 skip_already_held    = excluded.skip_already_held,
                 mining_window_years  = excluded.mining_window_years,
                 updated_at           = excluded.updated_at""",
            (per_trade_pct, daily_picks_max,
             1 if skip_already_held else 0,
             mining_window_years,
             datetime.now().isoformat())
        )
        con.commit()
    return get_config()
