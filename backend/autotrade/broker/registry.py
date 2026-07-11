"""Broker registry — the single source of truth mapping a broker name to its
EXECUTION client class, its AUTH provider class, and its declared capabilities.

Stage 0/1: zerodha is LIVE (real client + real auth). fyers/upstox/angel/dhan
are PLACEHOLDERS — they point at the existing stub BrokerClient subclasses for
execution, but their AUTH is a `NotImplementedAuth` that raises clearly
("adapter not yet certified") on ANY auth call. So the registry is COMPLETE (the
UI can enumerate every broker + its capability matrix) while non-Kite auth can
never silently succeed.

Adding a broker = one `_register(...)` line + the two adapter classes. Zero
changes to strategy code (kill switch / trail / sizing / monitor).

NOTE ON BROKER SET: LIVE adapters are zerodha + rupeezy (real client + real
auth). fyers/upstox/angel are placeholder stubs with NotImplementedAuth. dhan +
fivepaisa are COMING-SOON shells (live=False): dhan has a stub client, fivepaisa
reuses the dhan stub purely to satisfy the abstract client surface (its 4-field
creds don't map to the 2-field vault yet — display-only until certified). Every
registered broker also gets rich onboarding metadata via broker_metadata.py, so
list_supported() returns a full BrokerMeta card per broker.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Type

from .auth_base import (BrokerAuthProvider, BrokerCapabilities, RefreshNotSupported,
                        STATUS_ERROR, TokenHealth, TokenSet)
from .base import BrokerClient

log = logging.getLogger("kanida.autotrade.broker.registry")


class NotImplementedAuth(BrokerAuthProvider):
    """Placeholder auth provider for brokers whose adapter is NOT yet certified.

    Every method raises NotImplementedError with a clear "adapter not yet
    certified" message — so a session can be enumerated/validated but can NEVER
    obtain a token for an uncertified broker. validate() returns an ERROR
    TokenHealth (does not raise) so a health sweep degrades gracefully."""

    def __init__(self, broker_name: str, capabilities: BrokerCapabilities):
        self.broker_name = broker_name
        self.capabilities = capabilities

    def _nope(self, what: str):
        raise NotImplementedError(
            f"{self.broker_name}: {what} — adapter not yet certified "
            "(placeholder). Only zerodha auth is live.")

    def login_url(self, creds, *, redirect_uri: str = "", state: str = "") -> str:
        self._nope("login_url")

    def exchange(self, creds, *, request_token: str) -> TokenSet:
        self._nope("exchange")

    def refresh(self, creds) -> TokenSet:
        self._nope("refresh")

    def validate(self, creds) -> TokenHealth:
        return TokenHealth(ok=False, status=STATUS_ERROR,
                           detail=f"{self.broker_name} adapter not yet certified")


@dataclass
class BrokerEntry:
    broker: str
    client_cls: Type[BrokerClient]
    auth_cls: Type[BrokerAuthProvider]
    capabilities: BrokerCapabilities
    live: bool  # True = the UI enumerates it as a real broker (has a real adapter)
    # SPRINT CLUSTER 9b ITEM 9 — LIVE-ORDER CERTIFICATION GATE. `live` means the
    # adapter EXISTS and the UI shows it; `live_certified` means its order /
    # margin / GTT / status endpoints have been VERIFIED against a real account so
    # it may place REAL money orders. An uncertified LIVE broker (rupeezy today)
    # is BLOCKED from real order placement (paper always allowed) at the adapter —
    # see BrokerClient._certification_block. Default False (fail-closed): a broker
    # is only certified when explicitly registered as such.
    live_certified: bool = False


_REGISTRY: Dict[str, BrokerEntry] = {}


def _register(broker: str, client_cls, auth_cls, capabilities, *, live: bool,
              live_certified: bool = False):
    _REGISTRY[broker.lower()] = BrokerEntry(
        broker=broker.lower(), client_cls=client_cls, auth_cls=auth_cls,
        capabilities=capabilities, live=live, live_certified=live_certified)


# Placeholder capability matrices for the stub brokers. Conservative defaults —
# these are declarations for the UI, NOT verified facts, and are flagged live=False.
# (Do not hardcode verified per-broker facts from memory; certify at build time.)
_PLACEHOLDER_CAPS = {
    "fyers":  BrokerCapabilities("oauth2_code", True, "until_revoked", False, True, True),
    "upstox": BrokerCapabilities("oauth2_code", True, "until_revoked", False, True, True),
    "angel":  BrokerCapabilities("oauth2_code", True, "until_revoked", False, True, True),
    "dhan":   BrokerCapabilities("api_key_token", False, "until_revoked", False, True, True),
    # 5Paisa: coming-soon shell only. Multi-field creds don't map to the 2-field
    # vault yet, so its auth is a NotImplementedAuth placeholder (live=False).
    "fivepaisa": BrokerCapabilities("api_key_token", False, "session", False, False, True),
}


def _build_registry() -> None:
    if _REGISTRY:
        return
    # ── zerodha: LIVE ────────────────────────────────────────────────────────
    from .zerodha import ZerodhaBroker
    from .zerodha_auth_provider import ZerodhaAuth
    # zerodha is the ONE certified live adapter (orders/margin/GTT/status verified
    # against a real account) → may place REAL orders.
    _register("zerodha", ZerodhaBroker, ZerodhaAuth, ZerodhaAuth.capabilities,
              live=True, live_certified=True)

    # ── rupeezy (Vortex API): LIVE adapter, requires certification ────────────
    # Real execution client + real auth provider (Stage 2). Marked live=True so
    # the UI enumerates it as a real broker, but real orders STILL require:
    #   1. FALCON_AUTOTRADE_ENABLED=true  (master live gate),
    #   2. FALCON_VAULT_KEY               (to store/resolve creds),
    #   3. a resolved access_token        (via RupeezyAuth exchange), AND
    #   4. the instrument master configured (RUPEEZY_INSTRUMENT_MASTER / cache).
    # Until certified against a live account, treat the TODO(certify) endpoints
    # in rupeezy.py / rupeezy_auth_provider.py as UNVERIFIED.
    from .rupeezy import RupeezyBroker
    from .rupeezy_auth_provider import RupeezyAuth
    # rupeezy: LIVE adapter (UI-visible) but NOT yet certified — its positions/
    # margin/GTT/status are uncertified and it doesn't reliably transmit the client
    # tag. live_certified=False → BLOCKED from REAL order placement (paper allowed)
    # until certified against a live account. Flip to True only after certification.
    _register("rupeezy", RupeezyBroker, RupeezyAuth, RupeezyAuth.capabilities,
              live=True, live_certified=False)

    # ── placeholders: real execution stubs, NotImplementedAuth ───────────────
    from .fyers import FyersBroker
    from .upstox import UpstoxBroker
    from .angel import AngelBroker
    from .dhan import DhanBroker
    from .fivepaisa import FivePaisaBroker
    for name, client_cls in (("fyers", FyersBroker), ("upstox", UpstoxBroker),
                             ("angel", AngelBroker), ("dhan", DhanBroker),
                             ("fivepaisa", FivePaisaBroker)):
        caps = _PLACEHOLDER_CAPS[name]
        # Bind a NotImplementedAuth CLASS carrying this broker's name+caps.
        auth_cls = _make_placeholder_auth_cls(name, caps)
        _register(name, client_cls, auth_cls, caps, live=False)


def _make_placeholder_auth_cls(broker: str, caps: BrokerCapabilities):
    """Return a zero-arg-constructable NotImplementedAuth subclass for `broker`
    (so get_auth() can instantiate it like any other provider)."""
    def __init__(self, _b=broker, _c=caps):
        NotImplementedAuth.__init__(self, _b, _c)
    return type(f"{broker.capitalize()}AuthPlaceholder", (NotImplementedAuth,),
                {"__init__": __init__, "broker_name": broker, "capabilities": caps})


# ── public API ───────────────────────────────────────────────────────────────

def get_entry(name: str) -> BrokerEntry:
    _build_registry()
    entry = _REGISTRY.get((name or "").lower())
    if entry is None:
        raise ValueError(f"unknown broker: {name!r}")
    return entry


def get_client_cls(name: str) -> Type[BrokerClient]:
    return get_entry(name).client_cls


def get_auth(name: str) -> BrokerAuthProvider:
    """Instantiate the AUTH provider for `name` (zerodha → real; others →
    NotImplementedAuth placeholder that raises clearly)."""
    return get_entry(name).auth_cls()


def get_capabilities(name: str) -> BrokerCapabilities:
    return get_entry(name).capabilities


def is_live(name: str) -> bool:
    return get_entry(name).live


def is_certified(name: str) -> bool:
    """SPRINT CLUSTER 9b ITEM 9 — whether `name` is CERTIFIED to place REAL orders.
    An UNKNOWN broker is treated as NOT certified (fail-closed for real money)."""
    try:
        return bool(get_entry(name).live_certified)
    except Exception:
        return False


def list_supported() -> List[dict]:
    """Enumerate every registered broker as a full BrokerMeta for the guided
    "add a broker" UI — broker + live + capabilities (functional facts from this
    registry) MERGED with the declarative onboarding metadata (display_name,
    brand chip, exchanges, credential fields, setup steps) from
    broker_metadata.py. Data-driven: adding a broker surfaces its whole onboarding
    card here with no frontend redeploy."""
    from . import broker_metadata
    _build_registry()
    out: List[dict] = []
    for name in sorted(_REGISTRY):
        e = _REGISTRY[name]
        c = e.capabilities
        caps = {
            "auth_kind": c.auth_kind,
            "has_refresh_token": c.has_refresh_token,
            "token_lifetime": c.token_lifetime,
            "supports_gtt": c.supports_gtt,
            "supports_mtf": c.supports_mtf,
            "fno": c.fno,
        }
        entry = {"broker": e.broker, "live": e.live, "capabilities": caps}
        # Merge presentation + onboarding metadata (brand/display_name/exchanges/
        # fields/setup). Never overrides the functional broker/live/capabilities.
        entry.update(broker_metadata.meta_dict(
            e.broker, live=e.live, capabilities=caps))
        out.append(entry)
    return out
