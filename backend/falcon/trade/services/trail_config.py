"""Per-product trail-stop config (MTF, CNC).

Stored in falcon_trail_config table (one row per product). Operator edits via
/api/falcon/trade/config endpoint. Monitor consumes these values when deciding
trail / auto-exit actions per position.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from ...db import falcon_conn

VALID_PRODUCTS = {"MTF", "CNC"}
VALID_METHODS = {"percentage", "10d_low"}


def get_all_configs() -> List[Dict[str, Any]]:
    with falcon_conn() as con:
        rows = con.execute("SELECT * FROM falcon_trail_config ORDER BY product").fetchall()
    return [dict(r) for r in rows]


def get_config(product: str) -> Dict[str, Any] | None:
    with falcon_conn() as con:
        row = con.execute("SELECT * FROM falcon_trail_config WHERE product=?",
                          (product.upper(),)).fetchone()
    return dict(row) if row else None


def upsert_config(*, product: str,
                  activate_pct: float, lock_pct: float,
                  trail_sl_pct: float, trail_profit_pct: float,
                  initial_sl_pct: float, hold_days_max: int,
                  auto_exit_enabled: bool,
                  trail_method: str = "percentage",
                  trail_lookback: int = 10) -> Dict[str, Any]:
    p = product.upper()
    if p not in VALID_PRODUCTS:
        raise ValueError(f"Invalid product '{product}'. Must be one of {sorted(VALID_PRODUCTS)}")
    method = (trail_method or "percentage").lower()
    if method not in VALID_METHODS:
        raise ValueError(f"Invalid trail_method '{trail_method}'. Must be one of {sorted(VALID_METHODS)}")
    if activate_pct <= 0 or lock_pct < 0:
        raise ValueError("activate_pct must be positive; lock_pct may be 0 to disable lock floor")
    # trail_sl_pct / trail_profit_pct only enforced under the percentage method.
    # Under 10d_low they're ignored — accept any non-negative value.
    if method == "percentage" and (trail_sl_pct <= 0 or trail_profit_pct <= 0):
        raise ValueError("Under percentage method, trail_sl_pct and trail_profit_pct must be positive")
    if trail_sl_pct < 0 or trail_profit_pct < 0:
        raise ValueError("trail_sl_pct and trail_profit_pct must be non-negative")
    if hold_days_max < 1:
        raise ValueError("hold_days_max must be >= 1")
    if trail_lookback < 1 or trail_lookback > 100:
        raise ValueError("trail_lookback must be between 1 and 100")
    now = datetime.now().isoformat()
    with falcon_conn() as con:
        con.execute("""
            INSERT INTO falcon_trail_config
              (product, activate_pct, lock_pct, trail_sl_pct, trail_profit_pct,
               auto_exit_enabled, initial_sl_pct, hold_days_max,
               trail_method, trail_lookback, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(product) DO UPDATE SET
              activate_pct      = excluded.activate_pct,
              lock_pct          = excluded.lock_pct,
              trail_sl_pct      = excluded.trail_sl_pct,
              trail_profit_pct  = excluded.trail_profit_pct,
              auto_exit_enabled = excluded.auto_exit_enabled,
              initial_sl_pct    = excluded.initial_sl_pct,
              hold_days_max     = excluded.hold_days_max,
              trail_method      = excluded.trail_method,
              trail_lookback    = excluded.trail_lookback,
              updated_at        = excluded.updated_at
        """, (p, activate_pct, lock_pct, trail_sl_pct, trail_profit_pct,
              1 if auto_exit_enabled else 0,
              initial_sl_pct, hold_days_max,
              method, trail_lookback, now))
        con.commit()
    return get_config(p)
