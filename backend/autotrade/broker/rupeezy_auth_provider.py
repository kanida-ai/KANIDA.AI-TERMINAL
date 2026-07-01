"""RupeezyAuth — the BrokerAuthProvider for Rupeezy (Vortex API), Stage 2.

Implements the partner-app / OAuth flow documented in
`docs/design/RUPEEZY_VORTEX_API_REFERENCE.md`:

  * login redirect → https://flow.rupeezy.in?applicationId=<application_id>
  * callback captures the `auth` query param on our registered redirect URL
  * exchange: checksum = SHA-256(application_id + auth + x-api-key), then
    POST {base}/user/session {checksum, applicationId, token: auth}
    → data.access_token (RS512 JWT)
  * subsequent calls: header Authorization: Bearer <access_token>

Vault mapping (see reference note): application_id → vault `api_key`;
x-api-key (app secret) → vault `api_secret`.

Refresh: NOT documented → has_refresh_token=False + refresh() raises
RefreshNotSupported until the lifecycle is CERTIFIED against a live account.
Expiry: session-scoped → expiry() returns None; the validate() health ping
(cheap authenticated GET) is what drives EXPIRED detection.

This provider is STATELESS and NEVER stores secrets — the vault owns storage.
It makes at most ONE HTTP call (the exchange POST or the validate GET); both
require creds and are only invoked by the account-lifecycle service. All
datetimes IST.

TODO(certify) items in this file (see RUPEEZY_VORTEX_API_REFERENCE.md §"Still to
CONFIRM"):
  * RUPEEZY_API_BASE default host (reference CONFIRM #1).
  * The validate() health-ping path (funds/positions — reference CONFIRM #2/#4).
"""
from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime
from typing import Optional

from .auth_base import (IST, BrokerAuthProvider, BrokerCapabilities,
                        STATUS_ACTIVE, STATUS_ERROR, STATUS_EXPIRED,
                        STATUS_REVOKED, TokenHealth, TokenSet)

log = logging.getLogger("kanida.autotrade.broker.rupeezy_auth_provider")

# TODO(certify): production host — reference CONFIRM #1 ("e.g. vortex-api.rupeezy.in").
# Override via env RUPEEZY_API_BASE until verified against the live docs/account.
_DEFAULT_API_BASE = "https://vortex-api.rupeezy.in/v2"  # TODO(certify)

# TODO(certify): the login-flow host is documented as flow.rupeezy.in — reference §Auth Step 1.
_LOGIN_FLOW_URL = "https://flow.rupeezy.in"

# The Vortex login flow returns the request token in an `auth` query param
# (reference §Auth Step 2). Documented default 10s HTTP timeout.
_HTTP_TIMEOUT = 10


def _api_base() -> str:
    """Resolve the Vortex API base URL (env override; default TODO(certify))."""
    return os.environ.get("RUPEEZY_API_BASE", _DEFAULT_API_BASE).rstrip("/")


def _proxies():
    """Reuse the shared proxy config (static-egress hook) if present, else None."""
    try:
        from services.kite_auth import _kite_proxies
        return _kite_proxies()
    except Exception:  # pragma: no cover - defensive
        return None


class RupeezyAuth(BrokerAuthProvider):
    broker_name = "rupeezy"
    capabilities = BrokerCapabilities(
        auth_kind="oauth2_flow",
        has_refresh_token=False,   # TODO(certify): reference CONFIRM — no refresh documented
        token_lifetime="session",
        supports_gtt=True,
        supports_mtf=True,
        fno=True,
    )

    # 1. CONNECT ────────────────────────────────────────────────────────────────
    def login_url(self, creds, *, redirect_uri: str = "", state: str = "") -> str:
        """Rupeezy login redirect for THIS account's application_id.

        application_id is stored in vault `api_key`. The reference documents the
        base redirect as `https://flow.rupeezy.in?applicationId=<application_id>`;
        the redirect_uri is configured on the Vortex app itself. `state` is
        threaded as an extra query param so the callback can correlate it (the
        Vortex flow echoes unknown params back on the redirect)."""
        application_id = getattr(creds, "api_key", "") or ""
        if not application_id:
            raise ValueError("rupeezy auth: account has no application_id (api_key)")
        url = f"{_LOGIN_FLOW_URL}?applicationId={application_id}"
        if state:
            # TODO(certify): confirm the flow echoes `state` back on the redirect.
            url += f"&state={state}"
        return url

    # 2. EXCHANGE ────────────────────────────────────────────────────────────────
    @staticmethod
    def compute_checksum(application_id: str, request_token: str,
                         x_api_key: str) -> str:
        """checksum = SHA-256(application_id + auth + x-api-key) hexdigest
        (reference §Auth Step 3). request_token here IS the `auth` query param."""
        raw = f"{application_id}{request_token}{x_api_key}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def exchange(self, creds, *, request_token: str) -> TokenSet:
        """Exchange the `auth` query param for an access token.

        POST {base}/user/session with {checksum, applicationId, token} and parse
        data.access_token. Uses THIS account's application_id (api_key) +
        x-api-key (api_secret). Session-scoped → no refresh token, expiry None."""
        import requests

        application_id = getattr(creds, "api_key", "") or ""
        x_api_key = getattr(creds, "api_secret", "") or ""
        if not application_id or not x_api_key:
            raise ValueError(
                "rupeezy auth: account missing application_id (api_key) / "
                "x-api-key (api_secret)")
        checksum = self.compute_checksum(application_id, request_token, x_api_key)
        body = {
            "checksum": checksum,
            "applicationId": application_id,
            "token": request_token,
        }
        url = f"{_api_base()}/user/session"
        r = requests.post(url, json=body, timeout=_HTTP_TIMEOUT,
                          proxies=_proxies() or None)
        r.raise_for_status()
        payload = r.json() or {}
        data = payload.get("data") or {}
        access_token = data.get("access_token")
        if not access_token:
            raise ValueError(
                f"rupeezy auth: no access_token in exchange response "
                f"(keys={list(payload.keys())})")
        return TokenSet(access_token=str(access_token), refresh_token=None,
                        expires_at=None)  # session-scoped; health-ping drives EXPIRED

    # 3. REFRESH — inherits RefreshNotSupported (has_refresh_token=False).

    # 4. VALIDATE / HEALTH ────────────────────────────────────────────────────────
    def validate(self, creds) -> TokenHealth:
        """Cheap authenticated liveness probe. Calls a GET (funds/positions) with
        Authorization: Bearer <access_token>. Maps 401 → EXPIRED, 403 → REVOKED,
        network/5xx → ERROR. NEVER raises — a failed probe returns a non-ok
        TokenHealth so the caller can record it and prompt re-auth."""
        import requests

        access_token = getattr(creds, "access_token", "") or ""
        if not access_token:
            return TokenHealth(ok=False, status=STATUS_EXPIRED,
                               detail="no access_token (re-connect the account)")
        # TODO(certify): confirm the funds path — reference CONFIRM #4
        # (`GET {base}/user/funds`). Positions (#2) is an equally valid cheap ping.
        url = f"{_api_base()}/user/funds"
        headers = {"Authorization": f"Bearer {access_token}"}
        try:
            r = requests.get(url, headers=headers, timeout=_HTTP_TIMEOUT,
                             proxies=_proxies() or None)
        except Exception as e:
            return TokenHealth(ok=False, status=STATUS_ERROR, detail=str(e))
        if r.status_code == 200:
            return TokenHealth(ok=True, status=STATUS_ACTIVE, detail="funds ok")
        if r.status_code == 401:
            return TokenHealth(ok=False, status=STATUS_EXPIRED,
                               detail=f"401 {r.text[:200]}")
        if r.status_code == 403:
            return TokenHealth(ok=False, status=STATUS_REVOKED,
                               detail=f"403 {r.text[:200]}")
        return TokenHealth(ok=False, status=STATUS_ERROR,
                           detail=f"HTTP {r.status_code} {r.text[:200]}")

    # 5. EXPIRY ────────────────────────────────────────────────────────────────
    def expiry(self, token_set: TokenSet) -> Optional[datetime]:
        """Session-scoped: no absolute expiry documented. Returns None so the
        health-ping (validate) is the source of truth for EXPIRED detection."""
        return None
