"""/api/power/picks/* — the three pick surfaces.

  GET  /api/power/picks/today/preview   — public, top 5
  GET  /api/power/picks/today           — authed, top 100
  GET  /api/power/picks/replay/{date}   — public if featured/random, else authed
  POST /api/power/picks/replay/random   — public, rate-limited
  GET  /api/power/replay/featured       — public, 3 featured cards
  GET  /api/power/picks/live            — authed, reads falcon_live_decisions

EVERY pick in EVERY response goes through build_pick_payload → validate_pick_payload.
Single chokepoint: no inline JSON construction anywhere in this router.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request

from ..config import FEATURED_REPLAYS
from ..services.explainer import (
    PICK_SCHEMA_VERSION,
    build_pick_payload,
    compute_top_n,
    load_patterns,
    validate_pick_payload,
)
from ..services.live_tier import get_decisions
from ..services.replay_cache import (
    get_or_compute,
    list_featured,
    random_replay,
)
from .dependencies import (
    check_anon_rate_limit,
    current_user_optional,
    current_user_required,
    get_db,
    hash_ip_ua,
    log_request,
)

log = logging.getLogger("kanida.power_user.picks_router")
router = APIRouter(prefix="/api/power", tags=["power_user_picks"])

RANDOM_REPLAY_LIMIT_PER_HOUR = 3   # Q4 anti-cherry-pick anon limit
PUBLIC_PREVIEW_TOP_N = 5
AUTHED_TOP_N         = 100
FEATURED_DATES = frozenset(e["date"] for e in FEATURED_REPLAYS)


# ──────────────────────────────────────────────────────────────────────────
# Helpers — single chokepoint to build today's picks
# ──────────────────────────────────────────────────────────────────────────

def _latest_signal_date(con: sqlite3.Connection) -> Optional[str]:
    row = con.execute("SELECT MAX(trade_date) FROM falcon_features").fetchone()
    return row[0] if row and row[0] else None


def _next_trading_day(con: sqlite3.Connection, sd: str) -> Optional[str]:
    row = con.execute(
        "SELECT MIN(trade_date) FROM ohlc_daily WHERE trade_date > ?", (sd,)
    ).fetchone()
    return row[0] if row and row[0] else None


def _build_today_picks(con: sqlite3.Connection, top_n: int) -> Dict[str, Any]:
    """Compute today's picks. Single function used by BOTH preview and authed
    endpoints — only top_n differs. Every pick validated via build_pick_payload."""
    signal_date = _latest_signal_date(con)
    if not signal_date:
        return {"signal_date": None, "entry_date": None, "picks_shown": 0,
                "total_available": 0, "picks": []}
    entry_date = _next_trading_day(con, signal_date)

    patterns = load_patterns(con)
    raw = compute_top_n(con, signal_date, top_n=AUTHED_TOP_N, patterns=patterns)
    total = len(raw)
    raw_slice = raw[:top_n]

    picks: List[Dict[str, Any]] = []
    for rank, p in enumerate(raw_slice, start=1):
        payload = build_pick_payload(
            rank=rank, pick=p, outcomes=None,        # no actual outcomes for today
            signal_date=signal_date, entry_date=entry_date,
        )
        validate_pick_payload(payload)   # belt-and-suspenders
        picks.append(payload)

    return {
        "_schema_version":  PICK_SCHEMA_VERSION,
        "signal_date":      signal_date,
        "entry_date":       entry_date,
        "picks_shown":      len(picks),
        "total_available":  total,
        "picks":            picks,
    }


# ──────────────────────────────────────────────────────────────────────────
# /picks/today  — public preview (top 5) + authed full (top 100)
# ──────────────────────────────────────────────────────────────────────────

@router.get("/picks/today/preview")
def today_preview(
    request: Request,
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Public top-5 with full stories. Conversion funnel:
    landing → see top 5 → 'See all 100 →' → /login + invite gate."""
    ip_hash = hash_ip_ua(request)
    result = _build_today_picks(con, top_n=PUBLIC_PREVIEW_TOP_N)
    log_request(con, user_id=None, route="/api/power/picks/today/preview",
                status_code=200, latency_ms=0, ip_hash=ip_hash)
    return result


@router.get("/picks/today")
def today_full(
    request: Request,
    user = Depends(current_user_required),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Authed: full top-100 with stories. The power-user core view."""
    ip_hash = hash_ip_ua(request)
    result = _build_today_picks(con, top_n=AUTHED_TOP_N)
    log_request(con, user_id=user.user_id, route="/api/power/picks/today",
                status_code=200, latency_ms=0, ip_hash=ip_hash)
    return result


# ──────────────────────────────────────────────────────────────────────────
# /replay/featured  — landing page 3-card payload (lightweight)
# ──────────────────────────────────────────────────────────────────────────

@router.get("/replay/featured")
def featured_replays(
    request: Request,
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """List the 3 featured replays (headline metrics only — no full picks)."""
    featured = list_featured(con)
    log_request(con, user_id=None, route="/api/power/replay/featured",
                status_code=200, latency_ms=0, ip_hash=hash_ip_ua(request))
    return {"featured": featured}


# ──────────────────────────────────────────────────────────────────────────
# /picks/replay/{date}  — featured (public) / random (rate-limited) / authed
# ──────────────────────────────────────────────────────────────────────────

SELECT_DATE_LIMIT_PER_HOUR = 20   # Sprint 5d Fix 8: rate-limit anon "Select Date"


@router.get("/picks/replay/{replay_date}")
def replay_for_date(
    replay_date: str,
    request: Request,
    user = Depends(current_user_optional),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Replay for any date.

    Sprint 5d Fix 8 (2026-05-16): "Select Date" is the operator's flagship
    transparency feature — anyone can audit any historical day end-to-end.
    Public, but rate-limited per IP-hash so anonymous users can't grind every
    date in the backtest window.

    Access policy:
      - Featured date          → public, unlimited
      - Any other date + anon  → public, rate-limited to 20/hr per IP-hash
      - Authed user            → any date, no rate limit
    """
    ip_hash = hash_ip_ua(request)
    is_featured = replay_date in FEATURED_DATES

    if not is_featured and user is None:
        check_anon_rate_limit(con, ip_hash,
                              "/api/power/picks/replay/select-date",
                              SELECT_DATE_LIMIT_PER_HOUR)

    payload = get_or_compute(con, replay_date)
    if payload.get("error"):
        raise HTTPException(404, {
            "code":    "REPLAY_UNAVAILABLE",
            "message": f"No replay available for {replay_date}: {payload['error']}",
        })

    # Re-validate every pick in the cached payload (catches drift if cache
    # was populated by an old version of build_pick_payload)
    for p in payload.get("picks", []):
        try:
            validate_pick_payload(p)
        except ValueError as ve:
            log.error("replay %s: cached pick failed validation — %s", replay_date, ve)
            raise HTTPException(500, {"code": "CACHE_DRIFT",
                                       "message": "Cached payload version mismatch; clearing."})

    log_request(con, user_id=user.user_id if user else None,
                route="/api/power/picks/replay/{date}",
                status_code=200, latency_ms=0, ip_hash=ip_hash)
    return payload


@router.post("/picks/replay/random")
def replay_random(
    request: Request,
    user = Depends(current_user_optional),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Pick a uniformly-random trading day from the last 2yr, return its replay.

    Anonymous users limited to 3 per hour per IP-hash (Q4 anti-cherry-pick).
    Authed users have no rate limit on this endpoint.
    """
    ip_hash = hash_ip_ua(request)
    if user is None:
        check_anon_rate_limit(con, ip_hash,
                              "/api/power/picks/replay/random",
                              RANDOM_REPLAY_LIMIT_PER_HOUR)

    payload = random_replay(con)
    if payload.get("error"):
        raise HTTPException(503, {"code": "RANDOM_UNAVAILABLE",
                                    "message": payload["error"]})
    # Validate every pick
    for p in payload.get("picks", []):
        validate_pick_payload(p)

    log_request(con, user_id=user.user_id if user else None,
                route="/api/power/picks/replay/random",
                status_code=200, latency_ms=0, ip_hash=ip_hash)
    return payload


# ──────────────────────────────────────────────────────────────────────────
# /picks/live  — sub-200ms read from falcon_live_decisions
# ──────────────────────────────────────────────────────────────────────────

@router.get("/picks/live")
def live_decisions(
    request: Request,
    cycle: str = "latest",
    entry_date: Optional[str] = None,
    user = Depends(current_user_required),
    con: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Read pre-computed ENTER/WAIT/SKIP decisions. No Kite call per request.

    Cycle: '0930' | '0945' | '1000' | 'latest' (default).
    """
    if cycle not in ("0930", "0945", "1000", "latest"):
        raise HTTPException(400, {"code": "BAD_CYCLE",
                                    "message": "cycle must be 0930|0945|1000|latest"})

    result = get_decisions(con, entry_date=entry_date, cycle=cycle)

    # Collapse the internal 6th tier 'DEEP-TAIL' → 'TAIL' for user-facing
    # display, matching the Pick v1 contract's 5-tier enum.
    for d in result.get("decisions", []):
        if d.get("tier") == "DEEP-TAIL":
            d["tier"] = "TAIL"

    # Layer 3 (graceful degradation): if Zerodha auto-auth has failed and we're
    # past 09:30 IST, the scheduler couldn't compute live decisions today. The
    # UI renders a yellow banner explaining that EOD picks are still valid but
    # the intraday ENTER/WAIT/SKIP overlay is unavailable.
    from ..services.auth_status import get_status as _auth_status
    from ..config import POWER_DB_PATH as _POWER_DB_PATH
    result["degraded"] = _auth_status(_POWER_DB_PATH).get("degraded", False)

    log_request(con, user_id=user.user_id, route="/api/power/picks/live",
                status_code=200, latency_ms=0, ip_hash=hash_ip_ua(request))
    return result
