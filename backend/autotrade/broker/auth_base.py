"""BrokerAuthProvider abstract base + auth value types (Stage 0 foundation).

The strategy engine is broker-INDEPENDENT. `BrokerClient` (base.py) owns EXECUTION
(orders / quotes / margins / GTT). This module introduces the ORTHOGONAL second
interface — `BrokerAuthProvider` — which owns the CREDENTIAL LIFECYCLE
(login-url → exchange → refresh → validate → expiry) per broker.

An adapter package therefore ships TWO classes: `XxxBroker(BrokerClient)` and
`XxxAuth(BrokerAuthProvider)`, registered together in `registry.py`. The generic
account-lifecycle service branches ONLY on `capabilities` — never on
`broker_name` — so adding/adjusting a broker never touches strategy code.

ADDITIVE + DEFAULT-OFF: nothing here changes existing behaviour. The Kite path is
wrapped behaviour-preservingly by `zerodha_auth_provider.ZerodhaAuth`; all
non-Kite providers are placeholders that raise clearly ("adapter not yet
certified") rather than making any live call.

All datetimes are IST (Asia/Kolkata) — `datetime.now(IST)`.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

# Reuse the vault's canonical status constants so TokenHealth.status uses the
# SAME vocabulary the persistence layer already speaks (ACTIVE|EXPIRED|REVOKED|
# ERROR + PENDING). No duplication of the state machine.
from .. import vault as _vault

IST = timezone(timedelta(hours=5, minutes=30))

# TokenHealth.status vocabulary — pinned to the vault status constants.
STATUS_ACTIVE = _vault.STATUS_ACTIVE     # "ACTIVE"
STATUS_EXPIRED = _vault.STATUS_EXPIRED   # "EXPIRED"
STATUS_REVOKED = _vault.STATUS_REVOKED   # "REVOKED"
STATUS_ERROR = _vault.STATUS_ERROR       # "ERROR"


def _now_ist() -> datetime:
    return datetime.now(IST)


# ── value types ──────────────────────────────────────────────────────────────

@dataclass
class BrokerCapabilities:
    """Declared, per-broker capability matrix. The generic engine branches on
    THESE flags, never on the broker name.

    auth_kind         "request_token" (Kite) | "oauth2_code" | "api_key_token" | ...
    has_refresh_token can the access token be renewed silently (no interactive
                      re-login)?  Drives the STAY-ALIVE loop.
    token_lifetime    "daily_0600_ist" | "seconds:<n>" | "until_revoked"
    supports_gtt      broker-held OCO backup available?
    supports_mtf      margin-trade-facility leverage available?
    fno               futures & options tradeable?
    """
    auth_kind: str
    has_refresh_token: bool
    token_lifetime: str
    supports_gtt: bool
    supports_mtf: bool
    fno: bool


@dataclass
class TokenSet:
    """The result of an exchange/refresh: the live access token (+ optional
    refresh token) and its lifetime. `expires_at=None` means broker-defined daily
    expiry (e.g. Kite dies ~06:00 IST next day — no absolute stamp)."""
    access_token: str
    refresh_token: Optional[str] = None
    issued_at: datetime = field(default_factory=_now_ist)
    expires_at: Optional[datetime] = None


@dataclass
class TokenHealth:
    """A cheap health-probe result. status ∈ ACTIVE|EXPIRED|REVOKED|ERROR
    (the vault status vocabulary)."""
    ok: bool
    status: str
    detail: str = ""


class RefreshNotSupported(Exception):
    """Raised by BrokerAuthProvider.refresh() when the broker has no refresh
    token (interactive re-auth required each expiry — e.g. Kite). The lifecycle
    service catches this and emits a 'reconnect needed' signal instead of
    crashing."""


# ── the interface ────────────────────────────────────────────────────────────

class BrokerAuthProvider(ABC):
    """Per-broker credential lifecycle. One instance per broker (stateless — all
    inputs are passed in). NEVER stores secrets; the vault owns persistence."""

    broker_name: str = "abstract"
    capabilities: BrokerCapabilities = BrokerCapabilities(
        auth_kind="abstract", has_refresh_token=False,
        token_lifetime="until_revoked", supports_gtt=False,
        supports_mtf=False, fno=False)

    # 1. CONNECT — where to send the user to authenticate.
    @abstractmethod
    def login_url(self, creds, *, redirect_uri: str, state: str) -> str:
        ...

    # 2. EXCHANGE — turn the broker's post-login artifact (request_token /
    #    auth_code / …) into an access token (+ optional refresh token).
    @abstractmethod
    def exchange(self, creds, *, request_token: str) -> TokenSet:
        ...

    # 3. REFRESH — silently renew via a refresh token. Default: not supported.
    def refresh(self, creds) -> TokenSet:
        raise RefreshNotSupported(
            f"{self.broker_name}: refresh tokens not supported "
            "(interactive re-auth required)")

    # 4. VALIDATE / HEALTH — is the current token live? (cheap profile ping).
    @abstractmethod
    def validate(self, creds) -> TokenHealth:
        ...

    # 5. EXPIRY — when does the current token die? (absolute IST, or None for a
    #    broker-defined daily expiry the caller derives).
    def expiry(self, token_set: TokenSet) -> Optional[datetime]:
        return token_set.expires_at
