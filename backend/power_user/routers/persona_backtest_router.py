"""DRAFT — Persona backtest API endpoints.

Replaces the Excel-upload flow. Frontend reads from these endpoints; no
more static Excel files in the production pipeline.

ENDPOINTS:
  GET  /api/power/personas                              list (id, name, tagline, risk)
  GET  /api/power/personas/{slug}                       full backtest result (cached 1 hr)
  GET  /api/power/personas/{slug}/yearly                year-table only
  GET  /api/power/personas/{slug}/monthly/{year}        month-by-month drill for a year
  GET  /api/power/personas/{slug}/trades?limit=&offset= paginated trade log
  POST /api/power/personas/{slug}/refresh               admin-only: force cache invalidation

All endpoints public + read-only EXCEPT /refresh (admin secret required).
Numbers come from `persona_simulator.simulate_persona()` — single source of truth.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Header, Query

from ..services.persona_simulator import (
    PERSONA_CONFIGS,
    invalidate_cache,
    list_personas,
    simulate_persona,
)
from .. import config

log = logging.getLogger("kanida.power_user.persona_backtest_router")
router = APIRouter(prefix="/api/power/personas", tags=["power_user_personas"])


# ════════════════════════════════════════════════════════════════════════
# Public read endpoints
# ════════════════════════════════════════════════════════════════════════

@router.get("")
def get_persona_list() -> List[Dict[str, Optional[str]]]:
    """Lightweight list for the persona-selector UI on the landing page.
    Does NOT trigger a sim — just returns metadata from PERSONA_CONFIGS.
    `warning` is None for 4 of 5 personas (only P5 BTST carries one), so the
    return type must allow Optional[str], not str — FastAPI's ResponseValidator
    rejects None against a strict `str` annotation."""
    return list_personas()


@router.get("/{slug}")
def get_persona_backtest(slug: str) -> Dict[str, Any]:
    """Full backtest result for one persona. Cached 1 hour.

    Returns the same shape as `simulate_persona()`:
      - persona_id, name, tagline, risk_tier, warning
      - config (locked rules — entry, trade size, hold, SL/target)
      - yearly (list of {year, return_pct, max_dd, wr, n_closed, n_open})
      - monthly (list of {year, month, end_equity, return_pct})
      - trades (full trade log with sector, P&L, etc.)
      - summary (avg/median/best/worst yearly return, total P&L)
      - reconciliation (year-by-year P&L tie-out, should all be Re 0)
      - open_positions (empty for now — live data layer pending)
      - last_updated (ISO timestamp)
      - source_sim_script (which Excel this matches)
    """
    if slug not in PERSONA_CONFIGS:
        raise HTTPException(404, {"code": "PERSONA_NOT_FOUND",
                                   "message": f"Unknown persona: {slug}"})
    try:
        result = simulate_persona(slug)
    except Exception as e:
        log.exception("persona %s sim failed: %s", slug, e)
        raise HTTPException(500, {"code": "SIM_FAILED", "message": str(e)})
    return result


@router.get("/{slug}/yearly")
def get_persona_yearly(slug: str) -> Dict[str, Any]:
    """Just the year-by-year table — for the detail-page yearly grid."""
    full = get_persona_backtest(slug)
    return {
        "persona_id": slug,
        "yearly": full["yearly"],
        "summary": full["summary"],
        "last_updated": full["last_updated"],
    }


@router.get("/{slug}/monthly/{year}")
def get_persona_monthly_for_year(slug: str, year: int) -> Dict[str, Any]:
    """Month-by-month drill for a specific year. Used when user expands a year row."""
    full = get_persona_backtest(slug)
    months = [m for m in full["monthly"] if m["year"] == year]
    if not months:
        raise HTTPException(404, {"code": "YEAR_NOT_FOUND",
                                   "message": f"No monthly data for {year}"})
    return {
        "persona_id": slug,
        "year": year,
        "monthly": months,
        "last_updated": full["last_updated"],
    }


@router.get("/{slug}/trades")
def get_persona_trades(
    slug: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    year: Optional[int] = None,
) -> Dict[str, Any]:
    """Paginated trade log. Optionally filter by year."""
    full = get_persona_backtest(slug)
    trades = full["trades"]
    if year is not None:
        trades = [t for t in trades if t["year"] == year]
    total = len(trades)
    page = trades[offset:offset + limit]
    return {
        "persona_id": slug,
        "year_filter": year,
        "limit": limit,
        "offset": offset,
        "total": total,
        "trades": page,
        "last_updated": full["last_updated"],
    }


@router.get("/{slug}/reconciliation")
def get_persona_reconciliation(slug: str) -> Dict[str, Any]:
    """Year-by-year P&L reconciliation. For QA / audit pages — should always
    show 0 gap, otherwise there's a bug."""
    full = get_persona_backtest(slug)
    return {
        "persona_id": slug,
        "reconciliation": full["reconciliation"],
        "all_pass": all(abs(r["gap"]) < 1.0 for r in full["reconciliation"]),
        "max_gap_rs": max((abs(r["gap"]) for r in full["reconciliation"]), default=0),
        "last_updated": full["last_updated"],
    }


# ════════════════════════════════════════════════════════════════════════
# Admin-only: force cache refresh
# ════════════════════════════════════════════════════════════════════════

def _require_admin(x_admin_secret: Optional[str] = Header(None)):
    if not config.POWER_ADMIN_SECRET:
        raise HTTPException(500, {"code": "ADMIN_NOT_CONFIGURED",
                                   "message": "POWER_ADMIN_SECRET not set"})
    if x_admin_secret != config.POWER_ADMIN_SECRET:
        raise HTTPException(401, {"code": "UNAUTHORIZED",
                                   "message": "Invalid admin secret"})


@router.post("/{slug}/refresh")
def refresh_persona_cache(slug: str, _admin=Depends(_require_admin)) -> Dict[str, Any]:
    """Force re-run of a persona's sim (bypasses cache). Use after editing
    PERSONA_CONFIGS or when patterns/data change."""
    if slug not in PERSONA_CONFIGS:
        raise HTTPException(404, {"code": "PERSONA_NOT_FOUND",
                                   "message": f"Unknown persona: {slug}"})
    invalidate_cache(slug)
    result = simulate_persona(slug, force=True)
    return {
        "persona_id": slug,
        "refreshed": True,
        "elapsed_seconds": result["_elapsed_seconds"],
        "last_updated": result["last_updated"],
    }


@router.post("/refresh-all")
def refresh_all_personas(_admin=Depends(_require_admin)) -> Dict[str, Any]:
    """Force re-run for ALL personas. Use after a config-wide change."""
    invalidate_cache()  # clear everything
    results = {}
    for slug in PERSONA_CONFIGS.keys():
        try:
            r = simulate_persona(slug, force=True)
            results[slug] = {"ok": True, "elapsed": r["_elapsed_seconds"]}
        except Exception as e:
            log.exception("refresh failed for %s", slug)
            results[slug] = {"ok": False, "error": str(e)}
    return {"refreshed": True, "results": results}
