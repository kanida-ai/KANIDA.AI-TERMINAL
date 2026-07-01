"""Generic, broker-AGNOSTIC account lifecycle service (Stage 1 core).

The API layer calls THESE functions; they resolve the auth provider via the
registry and the creds via the vault, and branch ONLY on `capabilities` — there
is NO `if broker == "zerodha"` anywhere. Adding a broker = registry line + two
adapter classes; this service is untouched.

Flow (§5 of the design):
  begin_connect       → vault.put_account (status PENDING)
  login_url           → auth.login_url(creds)
  complete_connect    → auth.exchange(request_token) → vault.store_tokens (ACTIVE)
  refresh_if_supported→ capabilities.has_refresh_token ? auth.refresh→store : re-auth signal
  health_check        → auth.validate(creds) → vault.record_health

Everything is user_id-scoped. Secrets never leave the server. Live-execution gate
(FALCON_AUTOTRADE_ENABLED) is unchanged — this layer only manages CREDENTIALS.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from .. import vault
from .auth_base import RefreshNotSupported, TokenHealth
from . import registry

log = logging.getLogger("kanida.autotrade.broker.account_lifecycle")


class AccountLifecycleError(RuntimeError):
    """Raised for connect/refresh/health failures the API can surface verbatim."""


class AccountNotTradeable(RuntimeError):
    """Raised by assert_account_tradeable when a bound account is not ACTIVE.
    A session must NEVER fire against a PENDING/EXPIRED/REVOKED/ERROR account."""


def _resolve_creds(user_id: Optional[str], broker_account_id: str):
    creds = vault.get_decrypted_creds(broker_account_id, user_id=user_id)
    if creds is None:
        raise AccountLifecycleError(
            "cannot resolve account creds: vault disabled or account not found "
            "(set FALCON_VAULT_KEY and connect the account first)")
    return creds


# ── 1. begin_connect ─────────────────────────────────────────────────────────

def begin_connect(user_id: str, broker: str, account_label: str,
                  api_key: str, api_secret: str) -> Dict[str, Any]:
    """Create (or rotate creds on) a broker account → status PENDING. Validates
    that the broker is registered before storing. Returns the PUBLIC dict."""
    broker = (broker or "").strip().lower()
    registry.get_entry(broker)  # raises ValueError on unknown broker
    return vault.put_account(user_id, broker, account_label, api_key, api_secret)


# ── 2. login_url ─────────────────────────────────────────────────────────────

def login_url(user_id: str, broker_account_id: str,
              redirect_uri: str = "", state: str = "") -> str:
    """Build the broker's login URL for this account (auth provider resolved via
    the registry). Raises AccountLifecycleError if the broker adapter is a
    placeholder (NotImplementedAuth) or creds can't be resolved."""
    creds = _resolve_creds(user_id, broker_account_id)
    auth = registry.get_auth(creds.broker)
    try:
        return auth.login_url(creds, redirect_uri=redirect_uri, state=state)
    except NotImplementedError as e:
        raise AccountLifecycleError(str(e))


# ── 3. complete_connect ──────────────────────────────────────────────────────

def complete_connect(user_id: str, broker_account_id: str,
                     request_token: str) -> Dict[str, Any]:
    """Exchange the broker's post-login artifact for tokens and store them
    (status ACTIVE). Broker-agnostic: auth.exchange returns a TokenSet whose
    fields (access, optional refresh, expires_at) feed vault.store_tokens as-is —
    a refresh-token broker persists its refresh token automatically, a daily
    broker (Kite) stores just the access token. Returns the PUBLIC dict."""
    creds = _resolve_creds(user_id, broker_account_id)
    auth = registry.get_auth(creds.broker)
    try:
        token_set = auth.exchange(creds, request_token=request_token)
    except NotImplementedError as e:
        raise AccountLifecycleError(str(e))
    except Exception as e:
        raise AccountLifecycleError(f"token exchange failed: {e}")
    expires_at_iso = (token_set.expires_at.isoformat()
                      if token_set.expires_at is not None else None)
    pub = vault.store_tokens(
        broker_account_id, token_set.access_token,
        refresh_token=token_set.refresh_token, expires_at=expires_at_iso,
        user_id=user_id)
    if pub is None:
        raise AccountLifecycleError("token exchanged but account vanished on store")
    log.info("account_lifecycle: connected account %s (%s)",
             broker_account_id, creds.broker)
    return pub


# ── 4. refresh_if_supported ──────────────────────────────────────────────────

def refresh_if_supported(user_id: str, broker_account_id: str) -> Dict[str, Any]:
    """Silently renew the access token IF the broker supports refresh tokens
    (capabilities.has_refresh_token). Branches ONLY on capabilities.

    * has_refresh_token=True  → auth.refresh → vault.store_tokens (silent renew).
    * has_refresh_token=False → returns the account with a 'reconnect_needed'
      hint (interactive re-auth required, e.g. Kite every morning). Never raises
      for the no-refresh case — that's the expected path for daily brokers."""
    creds = _resolve_creds(user_id, broker_account_id)
    auth = registry.get_auth(creds.broker)
    caps = registry.get_capabilities(creds.broker)
    if not caps.has_refresh_token:
        pub = vault.get_account_public(broker_account_id, user_id=user_id) or {}
        pub["reconnect_needed"] = True
        pub["refresh_supported"] = False
        return pub
    try:
        token_set = auth.refresh(creds)
    except RefreshNotSupported:
        # Capabilities said yes but the provider disagrees — fall back to re-auth.
        pub = vault.get_account_public(broker_account_id, user_id=user_id) or {}
        pub["reconnect_needed"] = True
        pub["refresh_supported"] = False
        return pub
    except Exception as e:
        vault.record_health(broker_account_id, vault.STATUS_ERROR, str(e),
                            user_id=user_id)
        raise AccountLifecycleError(f"refresh failed: {e}")
    expires_at_iso = (token_set.expires_at.isoformat()
                      if token_set.expires_at is not None else None)
    pub = vault.store_tokens(
        broker_account_id, token_set.access_token,
        refresh_token=token_set.refresh_token, expires_at=expires_at_iso,
        user_id=user_id)
    if pub is None:
        raise AccountLifecycleError("refreshed but account vanished on store")
    pub["refresh_supported"] = True
    return pub


# ── 5. health_check ──────────────────────────────────────────────────────────

def health_check(user_id: str, broker_account_id: str) -> TokenHealth:
    """Probe the account's token liveness (auth.validate) and record the result
    (vault.record_health). Returns the TokenHealth. Never raises on a probe
    failure — a dead token is a normal state, surfaced as a non-ok health."""
    creds = _resolve_creds(user_id, broker_account_id)
    auth = registry.get_auth(creds.broker)
    health = auth.validate(creds)
    vault.record_health(broker_account_id, health.status, health.detail,
                        user_id=user_id)
    return health


# ── 6. preflight gate ────────────────────────────────────────────────────────

def assert_account_tradeable(user_id: Optional[str],
                             broker_account_id: str) -> None:
    """Raise AccountNotTradeable unless the bound account's DERIVED status is
    ACTIVE. Called from the session start path so a session never fires against a
    PENDING / EXPIRED / REVOKED / ERROR account.

    Uses the vault's token-date-derived status (the same derivation the account
    card shows). NULL/absent account or a disabled vault is NOT this function's
    concern — the caller only invokes it for sessions that HAVE a
    broker_account_id; a disabled-vault bound account resolves to a global
    fallback upstream and is handled there."""
    pub = vault.get_account_public(broker_account_id, user_id=user_id)
    if pub is None:
        raise AccountNotTradeable(
            f"broker_account {broker_account_id} not found"
            + (f" for user {user_id}" if user_id else "")
            + " (or vault disabled)")
    status = pub.get("status")
    if status != vault.STATUS_ACTIVE:
        raise AccountNotTradeable(
            f"broker_account {broker_account_id} is {status} — reconnect the "
            "account before starting a session (a session never fires against a "
            "non-ACTIVE account)")
