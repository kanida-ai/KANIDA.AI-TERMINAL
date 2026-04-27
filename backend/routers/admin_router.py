"""
Admin router — Zerodha token refresh endpoint.
POST /api/admin/refresh-token
  body: { "request_token": "...", "secret": "..." }
  - Exchanges Kite request_token → access_token
  - Updates KITE_ACCESS_TOKEN in Railway env via GraphQL API
  - Updates in-process os.environ so current instance works immediately
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


@router.post("/admin/refresh-token")
async def refresh_token(body: TokenRequest):
    admin_secret = os.getenv("ADMIN_SECRET", "")
    if not admin_secret or body.secret != admin_secret:
        raise HTTPException(status_code=403, detail="Invalid secret")

    api_key    = os.getenv("KITE_API_KEY", "")
    api_secret = os.getenv("KITE_API_SECRET", "")
    if not api_key or not api_secret:
        raise HTTPException(status_code=500, detail="KITE_API_KEY or KITE_API_SECRET not configured")

    # Exchange request_token → access_token
    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=api_key)
        session = kite.generate_session(body.request_token, api_secret=api_secret)
        access_token: str = session["access_token"]
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Kite session error: {e}")

    # Update in-process env immediately (current container works right away)
    os.environ["KITE_ACCESS_TOKEN"] = access_token

    # Persist to Railway env vars so it survives restarts
    railway_token   = os.getenv("RAILWAY_TOKEN", "")
    project_id      = os.getenv("RAILWAY_PROJECT_ID", "")
    environment_id  = os.getenv("RAILWAY_ENV_ID", "")
    service_id      = os.getenv("RAILWAY_SERVICE_ID", "")

    railway_updated = False
    if railway_token and project_id and environment_id and service_id:
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
            railway_updated = resp.status_code == 200 and "errors" not in resp.json()
        except Exception:
            pass

    return {
        "status":          "ok",
        "token_preview":   access_token[:10] + "...",
        "access_token":    access_token,
        "railway_updated": railway_updated,
        "message":         "Token refreshed. Railway env updated." if railway_updated
                           else "Token refreshed (in-process only — Railway env not updated, set RAILWAY_TOKEN).",
    }


@router.get("/admin/token-status")
def token_status():
    """Check if the current Kite token is valid."""
    api_key     = os.getenv("KITE_API_KEY", "")
    access_tok  = os.getenv("KITE_ACCESS_TOKEN", "")
    if not api_key or not access_tok:
        return {"valid": False, "reason": "credentials not set"}
    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_tok)
        profile = kite.profile()
        return {
            "valid":     True,
            "user":      profile.get("user_name", ""),
            "email":     profile.get("email", ""),
            "token_tip": access_tok[:10] + "...",
        }
    except Exception as e:
        return {"valid": False, "reason": str(e)}
