"""ZerodhaAuth — the BrokerAuthProvider for Kite (Stage 0).

BEHAVIOUR-PRESERVING WRAPPER. This class does NOT invent a new Kite auth flow; it
adapts the EXISTING per-account flow (broker/zerodha_auth.py) + the shared
proxy-aware constructor (services.kite_auth._new_kite) to the generic
`BrokerAuthProvider` interface so the account-lifecycle service can treat Kite
identically to any future broker.

Kite specifics (request-token, daily expiry, NO refresh token):
  * auth_kind         = "request_token"
  * has_refresh_token = False   → refresh() raises RefreshNotSupported
  * token_lifetime    = "daily_0600_ist"  (dies ~06:00 IST next morning)
  * login_url         → kite.login_url()  (built from the account's api_key)
  * exchange          → kite.generate_session(request_token, api_secret)
  * validate          → cheap kite.profile() ping
  * expiry            → the NEXT 06:00 IST

It NEVER touches the legacy kite_tokens table or the operator's global token —
each account's token stays isolated in broker_accounts (the vault owns storage).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

from .auth_base import (IST, BrokerAuthProvider, BrokerCapabilities,
                        STATUS_ACTIVE, STATUS_ERROR, STATUS_EXPIRED,
                        STATUS_REVOKED, TokenHealth, TokenSet)

log = logging.getLogger("kanida.autotrade.broker.zerodha_auth_provider")


def _next_0600_ist(now: Optional[datetime] = None) -> datetime:
    """The next 06:00 IST strictly after `now` — Kite's daily token death time."""
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = now.replace(tzinfo=IST)
    six = now.replace(hour=6, minute=0, second=0, microsecond=0)
    if now >= six:
        six = six + timedelta(days=1)
    return six


class ZerodhaAuth(BrokerAuthProvider):
    broker_name = "zerodha"
    capabilities = BrokerCapabilities(
        auth_kind="request_token",
        has_refresh_token=False,
        token_lifetime="daily_0600_ist",
        supports_gtt=True,
        supports_mtf=True,
        fno=True,
    )

    def login_url(self, creds, *, redirect_uri: str = "", state: str = "") -> str:
        """Kite's login URL for THIS account's api_key. Kite's redirect_uri +
        state are configured on the Kite app itself (not passed to login_url), so
        redirect_uri/state are accepted for interface parity but not injected —
        behaviour matches the existing broker/zerodha_auth.login_url."""
        api_key = getattr(creds, "api_key", "") or ""
        if not api_key:
            raise ValueError("zerodha auth: account has no api_key")
        from services.kite_auth import _new_kite
        kite = _new_kite(api_key)
        return kite.login_url()

    def exchange(self, creds, *, request_token: str) -> TokenSet:
        """Exchange a request_token for an access token via THIS account's
        api_key+secret. Reuses kite.generate_session (same call the existing
        broker/zerodha_auth.exchange_token makes). Returns a TokenSet with NO
        refresh token and expiry = next 06:00 IST."""
        api_key = getattr(creds, "api_key", "") or ""
        api_secret = getattr(creds, "api_secret", "") or ""
        if not api_key or not api_secret:
            raise ValueError("zerodha auth: account missing api_key/api_secret")
        from services.kite_auth import _new_kite
        kite = _new_kite(api_key)
        session = kite.generate_session(request_token, api_secret=api_secret)
        access_token = session["access_token"]
        now = datetime.now(IST)
        return TokenSet(access_token=access_token, refresh_token=None,
                        issued_at=now, expires_at=_next_0600_ist(now))

    # refresh() inherits the base RefreshNotSupported raise (has_refresh_token=False).

    def validate(self, creds) -> TokenHealth:
        """Cheap liveness probe: build a per-account Kite client and call
        profile(). Maps the outcome to the vault status vocabulary. NEVER raises
        — a failed probe returns a non-ok TokenHealth so the caller can record
        it and prompt re-auth."""
        api_key = getattr(creds, "api_key", "") or ""
        access_token = getattr(creds, "access_token", "") or ""
        if not api_key or not access_token:
            return TokenHealth(ok=False, status=STATUS_EXPIRED,
                               detail="no access_token (re-connect the account)")
        try:
            from services.kite_auth import _new_kite
            kite = _new_kite(api_key)
            kite.set_access_token(access_token)
            prof = kite.profile()
            user = (prof or {}).get("user_name", "") if isinstance(prof, dict) else ""
            return TokenHealth(ok=True, status=STATUS_ACTIVE,
                               detail=f"profile ok ({user})" if user else "profile ok")
        except Exception as e:
            err = str(e)
            low = err.lower()
            if "invalid" in low or "expired" in low or "token" in low:
                # A dead daily token — expected each morning, not an error.
                return TokenHealth(ok=False, status=STATUS_EXPIRED, detail=err)
            if "revoked" in low or "denied" in low or "forbidden" in low:
                return TokenHealth(ok=False, status=STATUS_REVOKED, detail=err)
            return TokenHealth(ok=False, status=STATUS_ERROR, detail=err)

    def expiry(self, token_set: TokenSet) -> Optional[datetime]:
        """Kite tokens die ~06:00 IST. Use the TokenSet's stamped expiry when set,
        else derive the next 06:00 IST from issued_at."""
        if token_set.expires_at is not None:
            return token_set.expires_at
        return _next_0600_ist(token_set.issued_at)
