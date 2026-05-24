"""
Admin router — Zerodha token management.
All Kite credential logic is delegated to services.kite_auth.

GET  /api/admin/kite/login-url     — returns Zerodha OAuth login URL
POST /api/admin/kite/refresh-token — exchanges request_token, saves to DB
GET  /api/admin/kite/status        — token validity (safe fields only, no full token)
GET  /api/admin/kite/export        — returns full token for trusted local sync (requires secret)
"""
from __future__ import annotations

import os
import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

_RAILWAY_GQL = "https://backboard.railway.app/graphql/v2"


class TokenRequest(BaseModel):
    request_token: str
    secret: str


@router.get("/admin/kite/login-url")
def kite_login_url():
    from services.kite_auth import _load_env_file
    _load_env_file()
    api_key = os.environ.get("KITE_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=500, detail="KITE_API_KEY not configured")
    return {
        "login_url": f"https://kite.zerodha.com/connect/login?api_key={api_key}&v=3"
    }


@router.post("/admin/kite/refresh-token")
async def refresh_token(body: TokenRequest):
    # Verify admin secret
    admin_secret = os.getenv("ADMIN_SECRET", "")
    if not admin_secret or body.secret != admin_secret:
        raise HTTPException(status_code=403, detail="Invalid secret")

    from services.kite_auth import exchange_and_save, KiteAuthError
    try:
        access_token = exchange_and_save(body.request_token)
    except KiteAuthError as e:
        raise HTTPException(status_code=400, detail=f"{e.code}: {e.detail}")

    # Optionally persist to Railway env vars so it survives restarts
    railway_updated = await _push_to_railway(access_token)

    # Phase 3: KiteTicker can't swap creds mid-session, so a stale connection
    # from before the refresh will keep failing 403. Force-reconnect with the
    # new token now. Soft-fail — refresh itself stays successful regardless.
    ticker_restart = False
    try:
        from falcon.trade.services import kite_ticker
        ticker_restart = kite_ticker.start(force=True)
    except Exception:
        pass

    # Phase 2.2: margin lookups are bound to the previous session's auth state;
    # clear the day-cache so the next /preview re-fetches with fresh creds.
    try:
        from falcon.trade.services import margin_calc
        margin_calc.invalidate()
    except Exception:
        pass

    # 2026-05-10: Token refresh is the operator's "go" signal. If today's
    # daily_signals (V7 pipeline tail) hasn't completed yet, kick off the
    # full A2 -> A3 -> A4 pipeline async. This replaces the silent 16:05
    # cron abort-on-bad-token failure mode (Bug #1 from the Friday-skipped
    # diagnosis). Operator polls /falcon/admin/runs for progress.
    pipeline = {"kicked_off": False, "reason_skipped": "exception"}
    try:
        from falcon.jobs._pipeline import kick_off_v7_pipeline_if_stale
        pipeline = kick_off_v7_pipeline_if_stale(reason="token_refresh")
    except Exception:
        pass

    # GTM Phase: re-run preflight with the new token. The token check is the
    # most likely RED→GREEN flip after a refresh; updating the cache here means
    # the UI banner refreshes instantly and downstream auto-trade gates unblock.
    preflight_summary = {"ok": False, "red": ["unknown"], "ran": False}
    try:
        from falcon import preflight
        r = preflight.run(force=True)
        preflight_summary = {
            "ok":     r.ok,
            "red":    [c.name for c in r.checks if c.status == preflight.RED],
            "yellow": [c.name for c in r.checks if c.status == preflight.YELLOW],
            "ran":    True,
        }
    except Exception:
        pass

    return {
        "status":          "ok",
        "token_preview":   access_token[:8] + "...",
        "railway_updated": railway_updated,
        "ticker_restart":  ticker_restart,
        "pipeline":        pipeline,           # {kicked_off, reason_skipped}
        "preflight":       preflight_summary,  # {ok, red:[], yellow:[], ran}
        "message":         "Token refreshed and saved to DB. Railway env updated." if railway_updated
                           else "Token refreshed and saved to DB (Railway env not updated — set RAILWAY_TOKEN to enable).",
    }


@router.get("/admin/kite/status")
def token_status():
    from services.kite_auth import get_token_status
    return get_token_status()


# ── Keep old paths as aliases so existing frontend doesn't break ─────────────

@router.post("/admin/refresh-token")
async def refresh_token_legacy(body: TokenRequest):
    return await refresh_token(body)


@router.get("/admin/token-status")
def token_status_legacy():
    return token_status()


# ── Railway env var update (best-effort, non-blocking) ───────────────────────

async def _push_to_railway(access_token: str) -> bool:
    railway_token  = os.getenv("RAILWAY_TOKEN", "")
    project_id     = os.getenv("RAILWAY_PROJECT_ID", "")
    environment_id = os.getenv("RAILWAY_ENV_ID", "")
    service_id     = os.getenv("RAILWAY_SERVICE_ID", "")

    if not all([railway_token, project_id, environment_id, service_id]):
        return False

    mutation = """
    mutation($input: VariableUpsertInput!) {
        variableUpsert(input: $input)
    }
    """
    payload = {
        "query": mutation,
        "variables": {
            "input": {
                "projectId":     project_id,
                "environmentId": environment_id,
                "serviceId":     service_id,
                "name":          "KITE_ACCESS_TOKEN",
                "value":         access_token,
            }
        },
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                _RAILWAY_GQL,
                json=payload,
                headers={"Authorization": f"Bearer {railway_token}"},
            )
        return resp.status_code == 200 and "errors" not in resp.json()
    except Exception:
        return False
