"""/api/falcon/admin/* — operational endpoints (status, manual rerun).

AUTH (2026-07-16): every endpoint on this router EXCEPT `GET /falcon/preflight`
requires the operator token (`X-Operator-Token`), reusing the exact dependency
that already gates /api/falcon/trade/* — see
`falcon.trade.routers.trade_router.require_operator_token`. Imported, not
re-implemented, so there is ONE source of truth for the secret compare.

Why per-endpoint and not router-level (as trade_router.py:59 does):
`GET /falcon/preflight` is fetched BROWSER-DIRECT to the backend by
`components/PreflightBanner.tsx` (`NEXT_PUBLIC_API_URL`, no token), NOT through
the token-injecting `/api/falcon-proxy`. Router-level auth would 403 the
preflight banner on /falcon/admin, /falcon/premarket and /falcon/trade. Every
OTHER endpoint here is either called through the proxy (which injects the token
server-side) or has no frontend caller at all, so all of them are gated.
Follow-up to close the last hole: move PreflightBanner onto /api/falcon-proxy,
then this router can go router-level.
"""
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ..config import FALCON_DB, FALCON_VERSION
from ..db import falcon_conn
from ..trade.routers.trade_router import require_operator_token

router = APIRouter()

# Per-endpoint operator gate (see module docstring for why not router-level).
_OPERATOR = [Depends(require_operator_token)]


class JobRun(BaseModel):
    id:            int
    job_name:      str
    started_at:    str
    finished_at:   str | None
    status:        str
    rows_affected: int | None
    notes:         str | None
    error:         str | None


class FalconStatus(BaseModel):
    engine_version:    str
    db_path:           str
    db_size_mb:        float
    n_promoted_patterns: int
    n_signals_emitted: int
    latest_signal_date: str | None
    n_features_rows:   int
    n_outcomes_rows:   int
    # Phase 2 — pattern sync visibility. Drift between R&D and PROD becomes loud.
    patterns_last_published:        str | None  # ISO timestamp from latest patterns_publish run
    patterns_last_published_status: str | None  # 'success' | 'failed' | None
    patterns_last_published_notes:  str | None  # e.g. "R&D->PROD: 846 promoted (was 0), 2283 candidates (was 0)"


@router.get("/falcon/preflight")
def preflight_endpoint(force: bool = False):
    """Run the 12-check preflight and return structured result.

    Hot path for the UI banner (polled every 30s) and for /admin inspection.
    Every auto-trade entry point (deployer cycle, /place) gates on this.

    Query params:
      force=true  → skip 60s cache, re-run all checks fresh
    """
    from .. import preflight
    result = preflight.run(force=force)
    return result.to_dict()


@router.get("/falcon/preflight/summary", dependencies=_OPERATOR)
def preflight_summary():
    """Lightweight summary for banner — just ok/red/yellow counts. Hits cache."""
    from .. import preflight
    r = preflight.get_cached()
    if r is None:
        r = preflight.run(force=False)
    return {
        "ok":           r.ok,
        "has_warnings": r.has_warnings,
        "n_red":        sum(1 for c in r.checks if c.status == preflight.RED),
        "n_yellow":     sum(1 for c in r.checks if c.status == preflight.YELLOW),
        "n_green":      sum(1 for c in r.checks if c.status == preflight.GREEN),
        "red_names":    [c.name for c in r.checks if c.status == preflight.RED],
        "ran_at":       r.ran_at,
    }


@router.post("/falcon/preflight/smoke", dependencies=_OPERATOR)
def preflight_smoke():
    """Run the daily 1-share integration smoke (places + cancels real Kite orders).
    Marks today's auto-deploy as SMOKE_PASSED on success. Operator-only."""
    from ..integration_smoke import run_smoke
    return run_smoke()


@router.get("/falcon/admin/status", response_model=FalconStatus, dependencies=_OPERATOR)
def status():
    """Admin/dashboard status. Each table query is wrapped in try/except so a
    missing table on a fresh deployment doesn't 500 the whole endpoint."""
    def _safe_count(con, table: str) -> int:
        try:
            return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except Exception:
            return 0

    with falcon_conn() as con:
        n_pats = _safe_count(con, "falcon_promoted_patterns")
        try:
            n_sigs = con.execute("SELECT COUNT(*) FROM falcon_signals_live").fetchone()[0]
            latest = con.execute("SELECT MAX(signal_date) FROM falcon_signals_live").fetchone()[0]
        except Exception:
            n_sigs, latest = 0, None
        n_feat = _safe_count(con, "falcon_features")
        n_out  = _safe_count(con, "falcon_outcomes")
        # Latest patterns_publish job (B4 audit trail)
        try:
            pub = con.execute(
                """SELECT finished_at, status, notes
                     FROM falcon_signal_runs
                    WHERE job_name = 'patterns_publish'
                    ORDER BY id DESC LIMIT 1"""
            ).fetchone()
        except Exception:
            pub = None
    size_mb = FALCON_DB.stat().st_size / 1e6 if FALCON_DB.exists() else 0
    return FalconStatus(
        engine_version=FALCON_VERSION, db_path=str(FALCON_DB),
        db_size_mb=round(size_mb, 1),
        n_promoted_patterns=n_pats, n_signals_emitted=n_sigs,
        latest_signal_date=latest, n_features_rows=n_feat,
        n_outcomes_rows=n_out,
        patterns_last_published       =(pub["finished_at"] if pub else None),
        patterns_last_published_status=(pub["status"]      if pub else None),
        patterns_last_published_notes =(pub["notes"]       if pub else None),
    )


@router.get("/falcon/admin/runs", response_model=List[JobRun], dependencies=_OPERATOR)
def list_runs(limit: int = 30):
    """Recent cron job runs — for the admin panel."""
    with falcon_conn() as con:
        try:
            rows = con.execute(
                """SELECT * FROM falcon_signal_runs ORDER BY started_at DESC LIMIT ?""",
                (limit,)
            ).fetchall()
        except Exception:
            rows = []
    return [JobRun(**dict(r)) for r in rows]


@router.get("/falcon/admin/inbox", dependencies=_OPERATOR)
def inbox(limit: int = 20, unread_only: bool = False):
    """In-app notification feed. Returns latest notifications (in-app channel)
    sorted by created_at desc. The frontend displays these in a bell icon."""
    where = "channel = 'inapp'"
    if unread_only:
        where += " AND status != 'read'"
    with falcon_conn() as con:
        rows = con.execute(f"""
            SELECT id, channel, subject, body_md, payload_json,
                   status, created_at, sent_at
            FROM falcon_notifications_out
            WHERE {where}
            ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
        unread = con.execute("""
            SELECT COUNT(*) FROM falcon_notifications_out
            WHERE channel='inapp' AND status != 'read'
        """).fetchone()[0]
    items = []
    import json
    for r in rows:
        try:
            payload = json.loads(r["payload_json"]) if r["payload_json"] else {}
        except Exception:
            payload = {}
        items.append({
            "id":         r["id"],
            "subject":    r["subject"],
            "body_md":    r["body_md"],
            "payload":    payload,
            "status":     r["status"],
            "created_at": r["created_at"],
        })
    return {"unread": unread, "items": items}


@router.post("/falcon/admin/inbox/mark-read", dependencies=_OPERATOR)
def mark_read(ids: List[int]):
    """Mark a list of notification IDs as read."""
    if not ids:
        return {"updated": 0}
    placeholders = ",".join("?" * len(ids))
    with falcon_conn() as con:
        cur = con.execute(
            f"UPDATE falcon_notifications_out SET status='read' "
            f"WHERE channel='inapp' AND id IN ({placeholders})",
            ids,
        )
        con.commit()
    return {"updated": cur.rowcount}


@router.post("/falcon/admin/rerun/{job_name}", dependencies=_OPERATOR)
def manual_rerun(job_name: str):
    """Trigger a job manually (for admin panel button).
    Job runs in background — UI should poll /falcon/admin/runs to see progress.
    """
    valid = {"daily_data_refresh", "daily_features", "daily_signals", "weekly_remine"}
    if job_name not in valid:
        raise HTTPException(400, f"Unknown job. Valid: {sorted(valid)}")

    # Lazy-import the job module so the API doesn't pull heavy deps at boot
    import importlib
    mod = importlib.import_module(f"falcon.jobs.{job_name}")

    import threading
    t = threading.Thread(target=mod.run, daemon=True)
    t.start()
    return {"status": "started", "job_name": job_name,
             "tip": "Poll /api/falcon/admin/runs for completion."}
