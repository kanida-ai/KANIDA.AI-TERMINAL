"""Falcon Top 20 — the /power/today endpoint.

GET /api/power/today/falcon-top-20
    ?universe=all500|nifty50|nifty100|nifty200|fno
    ?sector=Healthcare              (free-form sector name)
    ?signal_date=YYYY-MM-DD         (default: latest available)

Returns the 3-bucket institutional-grade explainability payload — the
shape lives in lib/falcon-top20-types.ts on the frontend side.

In-process LRU cache: keyed by (signal_date, universe, sector). Once a
day is computed, repeats are sub-ms. The signal date itself rolls over
when the nightly engine emits a new row, so cache invalidates naturally.

Public endpoint (NOT auth-gated). The page that calls it (/power/today)
gates access at the UI layer per Phase 1b invite-only login. Direct API
access is fine — it's read-only data and the engine output isn't a
secret; the moat is the explainability rendering.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from .dependencies import get_db, hash_ip_ua
from ..config import POWER_RND_DB_PATH
from ..services.falcon_top20_explainer import build_falcon_top20

log = logging.getLogger("kanida.power_user.falcon_top20_router")
router = APIRouter(prefix="/api/power/today", tags=["power_user_top20"])


# ── In-process cache: (signal_date, universe, sector) → (payload, ts) ──
# Keep it small — at most ~40 entries (a few signal dates × a few filters).
_CACHE: Dict[tuple, Dict[str, Any]] = {}
_CACHE_TTL_SECONDS = 600   # 10 min. Sub-ms after first build.


def _cache_get(key: tuple) -> Optional[Dict[str, Any]]:
    entry = _CACHE.get(key)
    if entry is None:
        return None
    if time.time() - entry["_at"] > _CACHE_TTL_SECONDS:
        del _CACHE[key]
        return None
    return entry["payload"]


def _cache_put(key: tuple, payload: Dict[str, Any]) -> None:
    if len(_CACHE) > 50:
        _CACHE.clear()    # crude eviction; this is the operator's beta scale
    _CACHE[key] = {"payload": payload, "_at": time.time()}


@router.get("/falcon-top-20")
def falcon_top_20(
    request:      Request,
    universe:     str            = Query("all500", regex="^(all500|nifty50|nifty100|nifty200|fno)$"),
    sector:       Optional[str]  = Query(None,    max_length=64),
    signal_date:  Optional[str]  = Query(None,    regex=r"^\d{4}-\d{2}-\d{2}$"),
    prod_con:     sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """Falcon Top 20 with 3-bucket explainability for the chosen universe/sector.

    Reads PROD (via get_db) + RND (opened per-request below). Cached for
    10 min per (date, universe, sector) tuple.
    """
    key = (signal_date or "latest", universe, sector or "*")
    cached = _cache_get(key)
    if cached is not None:
        return cached

    # Open RND read-only for the duration of the call. SQLite shares fine
    # across processes; we don't hold the connection past this request.
    rnd_con = sqlite3.connect(
        f"file:{POWER_RND_DB_PATH}?mode=ro", uri=True, timeout=10.0
    )
    rnd_con.row_factory = sqlite3.Row

    try:
        t0 = time.time()
        payload = build_falcon_top20(
            prod_con=prod_con, rnd_con=rnd_con,
            signal_date=signal_date,
            universe=universe,
            sector=sector,
            # Falcon Top 10 — locked persona (2026-05-23 deployment note).
            # No watchlist; spec says "if the watchlist confuses users about
            # what's actually being traded, just show 10. Simpler is better."
            top_n=10,
        )
        elapsed_ms = int((time.time() - t0) * 1000)
        payload["_built_in_ms"] = elapsed_ms
        log.info("falcon_top20: universe=%s sector=%s n=%d built in %dms",
                  universe, sector, len(payload.get("picks") or []), elapsed_ms)
    except Exception as e:
        log.exception("falcon_top20 build failed: %s", e)
        raise HTTPException(500, {"code": "TOP20_BUILD_FAILED", "message": str(e)[:200]})
    finally:
        rnd_con.close()

    _cache_put(key, payload)
    return payload
