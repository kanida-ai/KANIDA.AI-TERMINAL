"""Per-account Zerodha login/exchange — scoped to a VAULTED broker account.

PHASE-2 MULTI-TENANT. This is the per-account equivalent of services.kite_auth's
single-tenant login flow: it builds a Kite login URL for a SPECIFIC account's
api_key and exchanges the returned request_token using THAT account's
api_key+secret, then stores the resulting access_token (encrypted) back into the
vault. It REUSES services.kite_auth._new_kite (the proxy-aware constructor) so
the static-egress hooks apply identically: the PER-ACCOUNT map (BROKER_PROXY_MAP,
resolved here via resolve_account_proxy so the token exchange egresses from the
same static IP as that account's order client) and the global BROKER_PROXY_URL
fallback.

It NEVER touches the legacy kite_tokens table or the operator's global token —
each account's token is isolated in broker_accounts.access_token_enc.

DAILY EXPIRY: Kite access tokens die ~06:00 IST next day. This module implements
the USER-INITIATED connect/refresh: build URL → user logs in → exchange → store.
Unattended per-user refresh is a documented follow-up (the operator's OWN account
keeps the existing KanidaZerodhaAuth scheduled task).
"""
from __future__ import annotations

import logging
from typing import Optional

from .. import vault

log = logging.getLogger("kanida.autotrade.broker.zerodha_auth")


class AccountAuthError(RuntimeError):
    pass


def login_url(broker_account_id: str, user_id: Optional[str] = None) -> str:
    """Build the Kite login URL for THIS account's api_key. The user opens it,
    logs in at Zerodha, and is redirected back with a request_token which is
    passed to exchange_token().

    Resolves the api_key from the vault (decrypted creds). Raises AccountAuthError
    if the vault is disabled or the account is absent / not owned by user_id."""
    creds = vault.get_decrypted_creds(broker_account_id, user_id=user_id)
    if creds is None:
        raise AccountAuthError(
            "cannot build login URL: vault disabled or account not found "
            "(set FALCON_VAULT_KEY and connect the account first)")
    if creds.broker != "zerodha":
        raise AccountAuthError(
            f"account {broker_account_id} is broker {creds.broker!r}, "
            "not zerodha")
    if not creds.api_key:
        raise AccountAuthError("account has no api_key")
    from services.kite_auth import _new_kite, resolve_account_proxy
    # Per-account egress: keep this account's auth client on the SAME provisioned
    # static IP as its order client (SEBI one-IP-per-account). login_url() itself
    # makes no network call, but this keeps both auth entrypoints symmetric.
    proxy_url = resolve_account_proxy(broker_account_id)
    kite = _new_kite(creds.api_key, proxy_url=proxy_url)
    return kite.login_url()


def exchange_token(broker_account_id: str, request_token: str,
                   user_id: Optional[str] = None) -> dict:
    """Exchange a request_token for an access_token using THIS account's
    api_key+secret, then store the (encrypted) access_token in the vault and set
    status ACTIVE / token_date today.

    Returns the account's PUBLIC dict (no secrets). Raises AccountAuthError on
    any failure (vault disabled, account absent, bad request_token, broker
    rejection). NEVER writes the legacy kite_tokens table."""
    creds = vault.get_decrypted_creds(broker_account_id, user_id=user_id)
    if creds is None:
        raise AccountAuthError(
            "cannot exchange token: vault disabled or account not found")
    if creds.broker != "zerodha":
        raise AccountAuthError(
            f"account {broker_account_id} is broker {creds.broker!r}, "
            "not zerodha")
    if not creds.api_key or not creds.api_secret:
        raise AccountAuthError("account missing api_key/api_secret")
    from services.kite_auth import _new_kite, resolve_account_proxy
    # generate_session() is a REAL Kite REST egress (token exchange against this
    # account's api_key/secret) → it MUST originate from the SAME per-account
    # static IP as the order client, or the exchange itself 403s once that user's
    # Kite IP allowlist is set. Thread the per-account proxy through.
    proxy_url = resolve_account_proxy(broker_account_id)
    try:
        kite = _new_kite(creds.api_key, proxy_url=proxy_url)
        session = kite.generate_session(request_token,
                                        api_secret=creds.api_secret)
        access_token = session["access_token"]
    except Exception as e:
        raise AccountAuthError(f"Kite session exchange failed: {e}")
    pub = vault.store_access_token(broker_account_id, access_token,
                                  user_id=user_id)
    if pub is None:
        raise AccountAuthError("token exchanged but account vanished on store")
    log.info("zerodha_auth: refreshed token for account %s", broker_account_id)
    return pub
