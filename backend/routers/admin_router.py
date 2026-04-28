"""
Admin router — Zerodha token management.
All Kite credential logic is delegated to services.kite_auth.

GET  /api/admin/kite/login-url     — returns Zerodha OAuth login URL
POST /api/admin/kite/refresh-token — exchanges request_token, saves to DB
GET  /api/admin/kite/status        — token validity (safe fields only, no full token)
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

    return {
        "status":          "ok",
        "token_preview":   access_token[:8] + "...",
        "railway_updated": railway_updated,
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
