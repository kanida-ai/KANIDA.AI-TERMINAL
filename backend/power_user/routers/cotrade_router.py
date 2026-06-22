"""Co-Trading virtual-portfolio API — follow Falcon with virtual capital.

  POST /api/power/cotrade/simulate   body {style, capital, start_date, end_date?}
                                     → per-user-capital portfolio payload
  GET  /api/power/cotrade/portfolio  → last simulated result (in-memory) or {}

Read-only on market data. Reuses the validated falcon-top-10 walk-forward
engine via services/cotrade_sim.py — same rules as the falcon-top-10 persona,
just a single continuous cash pool scaled to the user's capital.

OUT OF SCOPE (by design): real broker execution / AutoTrade. This router
NEVER touches the auto-trade path. Per-user DB persistence of portfolios is
intentionally NOT built here (the GET endpoint serves the last in-memory
simulate result); when a logged-in per-user store is needed it can be added as
a thin write-through without changing this contract.

Public + read-only, matching the ask_router / persona_backtest_router policy:
the data is engine output, not a secret; the page gates access at the UI layer.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..services.cotrade_sim import (
    VALID_STYLES,
    get_last_portfolio,
    simulate_cotrade,
)

log = logging.getLogger("kanida.power_user.cotrade_router")
router = APIRouter(prefix="/api/power/cotrade", tags=["power_user_cotrade"])

# Bound capital so a fat-finger can't request an absurd run.
_MIN_CAPITAL = 10_000.0
_MAX_CAPITAL = 100_00_00_000.0   # ₹100 Cr ceiling


class SimulateBody(BaseModel):
    style: str = Field(..., description="Co-Trading style; currently 'falcon-top-10'")
    capital: float = Field(..., description="User's starting virtual capital (₹)")
    start_date: str = Field(..., description="YYYY-MM-DD start of the walk-forward")
    end_date: Optional[str] = Field(None, description="YYYY-MM-DD end (default: today IST)")


@router.post("/simulate")
def cotrade_simulate(body: SimulateBody) -> Dict[str, Any]:
    """Run the falcon-top-10 walk-forward for a (capital, start_date[, end_date]).

    Returns: { style, start_date, end_date, as_of, honesty,
               summary{ starting_rs, current_value_rs, total_pnl_rs, return_pct,
                        max_dd_pct, n_open, n_closed, cash_rs, win_rate_pct,
                        n_trades },
               positions[...], equity[...], actions[...], last_updated }.

    Replay = a past start_date; "live today" = start_date≈today, short window.
    Cached ~10 min by (style, capital, start, end).
    """
    if body.style not in VALID_STYLES:
        raise HTTPException(400, {"code": "BAD_STYLE",
                                  "message": f"Unknown style '{body.style}'. "
                                             f"Valid: {sorted(VALID_STYLES)}"})
    if body.capital is None or body.capital < _MIN_CAPITAL or body.capital > _MAX_CAPITAL:
        raise HTTPException(400, {"code": "BAD_CAPITAL",
                                  "message": f"capital must be between "
                                             f"₹{_MIN_CAPITAL:,.0f} and ₹{_MAX_CAPITAL:,.0f}"})
    try:
        result = simulate_cotrade(
            style=body.style,
            capital=float(body.capital),
            start_date=body.start_date,
            end_date=body.end_date,
        )
    except ValueError as e:
        raise HTTPException(400, {"code": "BAD_INPUT", "message": str(e)})
    except Exception as e:                                   # noqa: BLE001
        log.exception("cotrade simulate failed: %s", e)
        raise HTTPException(500, {"code": "SIM_FAILED", "message": str(e)[:200]})
    return result


@router.get("/portfolio")
def cotrade_portfolio() -> Dict[str, Any]:
    """Last simulated Co-Trading portfolio (in-memory), or {} if none yet.

    Deliberately simple: full per-user DB persistence is a non-goal for now —
    call POST /simulate first; this returns the most recent result. See the
    module docstring for the persistence note.
    """
    return get_last_portfolio()
