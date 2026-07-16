"""PER-BROKER-ACCOUNT egress proxy (SEBI one-static-IP-per-Kite-account).

WHY THIS EXISTS: SEBI's retail-algo rules + Zerodha enforce ONE static IP per
Kite Connect account, so power user #2 must egress from HIS provisioned static
IP (a Lightsail proxy, 13.203.129.136) while the OPERATOR's own account keeps
egressing DIRECT from the home IP. The pre-existing global BROKER_PROXY_URL
reroutes EVERY account through ONE proxy → it cannot express that. So
BROKER_PROXY_MAP (JSON broker_account_id -> proxy URL) adds a PER-ACCOUNT
override on top, additive + DEFAULT-OFF.

THE CRITICAL REGRESSION GUARD in this file is
`test_operator_global_path_never_routed_through_a_users_proxy`: routing the
operator's own real-money account through another user's proxy would break his
orders (his IP is the one allowlisted on HIS Kite app). Default-off is asserted
explicitly: with nothing configured, NO proxies kwarg is passed at all.
"""
import logging

import pytest

from autotrade.config import BrokerProfile
from autotrade.broker.zerodha import ZerodhaBroker
import services.kite_auth as kite_auth


# A realistic map value — carries a PASSWORD, which must never be logged.
_PROXY_A = "http://kanida:s3cr3t-pw@13.203.129.136:8888"
_PROXY_GLOBAL = "http://kanida:global-pw@10.0.0.9:3128"


@pytest.fixture
def proxy_env(monkeypatch):
    """Hermetic proxy env.

    Clears BOTH hooks and neutralises _load_env_file so the operator's REAL
    config/.env can never leak a proxy value into (or out of) these tests, and
    resets the warn-once latch so the malformed-map test is order-independent.
    """
    monkeypatch.delenv("BROKER_PROXY_MAP", raising=False)
    monkeypatch.delenv("BROKER_PROXY_URL", raising=False)
    monkeypatch.setattr(kite_auth, "_load_env_file", lambda: None)
    monkeypatch.setattr(kite_auth, "_PROXY_MAP_WARNED", False)
    yield monkeypatch


@pytest.fixture
def kite_spy(monkeypatch):
    """Capture the kwargs each KiteConnect(...) construction receives.

    Spying on the CONSTRUCTOR (not the resulting object) is what lets us assert
    the default-off claim precisely: "no proxies kwarg AT ALL", which `.proxies
    == {}` alone could not distinguish from `proxies=None` being passed.
    """
    calls = []

    class _FakeKite:
        def __init__(self, **kw):
            calls.append(kw)
            self.proxies = kw.get("proxies", {}) or {}
            self.timeout = None
            self.access_token = None

        def set_access_token(self, tok):
            self.access_token = tok

    import kiteconnect
    monkeypatch.setattr(kiteconnect, "KiteConnect", _FakeKite)
    return calls


# ═══════════════════════════════════════════════════════════════════════════
# _new_kite — proxy precedence
# ═══════════════════════════════════════════════════════════════════════════

def test_new_kite_explicit_proxy_url_builds_from_it(proxy_env, kite_spy):
    """An explicit proxy_url builds the proxies dict from THAT url (both schemes).

    Revert: ignore the proxy_url param → no proxies kwarg → assert FAILS.
    """
    kite = kite_auth._new_kite("apikey", proxy_url=_PROXY_A)
    assert kite_spy == [{"api_key": "apikey",
                         "proxies": {"http": _PROXY_A, "https": _PROXY_A}}]
    assert kite.proxies == {"http": _PROXY_A, "https": _PROXY_A}


def test_new_kite_explicit_overrides_global(proxy_env, kite_spy):
    """Per-account proxy_url WINS over the global BROKER_PROXY_URL.

    This is the whole point: user #2 has a map entry, so he must NOT inherit a
    global hook meant for someone else.

    Revert: check the global first → global url in the dict → assert FAILS.
    """
    proxy_env.setenv("BROKER_PROXY_URL", _PROXY_GLOBAL)
    kite = kite_auth._new_kite("apikey", proxy_url=_PROXY_A)
    assert kite.proxies == {"http": _PROXY_A, "https": _PROXY_A}
    assert _PROXY_GLOBAL not in str(kite_spy)


def test_new_kite_none_falls_back_to_global(proxy_env, kite_spy):
    """proxy_url=None → fall back to the EXISTING global BROKER_PROXY_URL hook
    (the pre-existing behaviour must be preserved, not replaced).

    Revert: return early on proxy_url None → no proxies kwarg → assert FAILS.
    """
    proxy_env.setenv("BROKER_PROXY_URL", _PROXY_GLOBAL)
    kite = kite_auth._new_kite("apikey", proxy_url=None)
    assert kite.proxies == {"http": _PROXY_GLOBAL, "https": _PROXY_GLOBAL}


def test_new_kite_neither_set_is_direct_no_proxies_kwarg(proxy_env, kite_spy):
    """DEFAULT-OFF: neither hook set → NO proxies kwarg at all → byte-for-byte
    today's direct-egress construction. This is the operator's path.

    Revert: always pass proxies={...} → the kwarg appears → assert FAILS.
    """
    kite = kite_auth._new_kite("apikey")
    assert kite_spy == [{"api_key": "apikey"}]
    assert "proxies" not in kite_spy[0]
    assert kite.proxies == {}


def test_new_kite_default_arg_is_direct_when_called_positionally(proxy_env, kite_spy):
    """The added param is OPTIONAL — every existing 1-arg caller keeps working
    and stays direct (this is the additive-signature guarantee).

    Revert: make proxy_url required → TypeError → assert FAILS.
    """
    kite_auth._new_kite("apikey")
    assert "proxies" not in kite_spy[0]


@pytest.mark.parametrize("blank", ["", "   ", "\t\n"])
def test_new_kite_blank_proxy_url_falls_back(proxy_env, kite_spy, blank):
    """A blank/whitespace proxy_url == unset → falls back to the global hook
    (NOT a literal empty proxy, which would break egress entirely).

    Revert: treat any non-None as set → proxies={'http':'  '} → assert FAILS.
    """
    proxy_env.setenv("BROKER_PROXY_URL", _PROXY_GLOBAL)
    kite = kite_auth._new_kite("apikey", proxy_url=blank)
    assert kite.proxies == {"http": _PROXY_GLOBAL, "https": _PROXY_GLOBAL}


def test_new_kite_blank_proxy_url_and_no_global_is_direct(proxy_env, kite_spy):
    """Blank proxy_url + no global → direct, no proxies kwarg."""
    kite_auth._new_kite("apikey", proxy_url="   ")
    assert kite_spy == [{"api_key": "apikey"}]


def test_new_kite_applies_request_timeout(proxy_env, kite_spy):
    """REGRESSION: the proxy branch must NOT drop the 10s request timeout that
    _apply_request_timeout sets (the reference implementation on the divergent
    branch returned KiteConnect(...) raw and lost it).

    Revert: `return KiteConnect(...)` in the proxy branch → timeout is None →
    assert FAILS.
    """
    kite = kite_auth._new_kite("apikey", proxy_url=_PROXY_A)
    assert kite.timeout == 10.0
    direct = kite_auth._new_kite("apikey")
    assert direct.timeout == 10.0


# ═══════════════════════════════════════════════════════════════════════════
# resolve_account_proxy — BROKER_PROXY_MAP lookup
# ═══════════════════════════════════════════════════════════════════════════

def test_resolve_known_account_returns_its_proxy(proxy_env):
    """A mapped broker_account_id resolves to ITS url.

    Revert: return None unconditionally → assert FAILS.
    """
    proxy_env.setenv("BROKER_PROXY_MAP",
                     '{"acctA": "%s", "acctB": "http://x:y@1.2.3.4:8888"}' % _PROXY_A)
    assert kite_auth.resolve_account_proxy("acctA") == _PROXY_A
    assert kite_auth.resolve_account_proxy("acctB") == "http://x:y@1.2.3.4:8888"


def test_resolve_unknown_account_returns_none(proxy_env):
    """An account NOT in the map → None → caller falls back to global/direct.
    An unmapped account must never inherit another account's proxy.

    Revert: return the first/any value → assert FAILS.
    """
    proxy_env.setenv("BROKER_PROXY_MAP", '{"acctA": "%s"}' % _PROXY_A)
    assert kite_auth.resolve_account_proxy("acctZZZ") is None


def test_resolve_env_absent_returns_none(proxy_env):
    """DEFAULT-OFF: no BROKER_PROXY_MAP → None for everyone."""
    assert kite_auth.resolve_account_proxy("acctA") is None


def test_resolve_none_id_returns_none(proxy_env):
    """A None/empty broker_account_id (the operator's unbound global path) → None
    WITHOUT consulting the map at all."""
    proxy_env.setenv("BROKER_PROXY_MAP", '{"acctA": "%s"}' % _PROXY_A)
    assert kite_auth.resolve_account_proxy(None) is None
    assert kite_auth.resolve_account_proxy("") is None


def test_resolve_malformed_json_warns_once_and_never_raises(proxy_env, caplog):
    """A malformed map must DEGRADE (None → global/direct), never raise — a typo
    in .env must not take the order path down. Warned once, not per-call.

    Revert: let json.loads raise → the call raises → assert FAILS.
    """
    proxy_env.setenv("BROKER_PROXY_MAP", "{not valid json,,,")
    with caplog.at_level(logging.WARNING, logger="kanida.kite_auth"):
        assert kite_auth.resolve_account_proxy("acctA") is None
        assert kite_auth.resolve_account_proxy("acctA") is None
    warns = [r for r in caplog.records if r.levelno == logging.WARNING
             and "BROKER_PROXY_MAP" in r.getMessage()]
    assert len(warns) == 1, "malformed map must warn exactly ONCE, not per call"


@pytest.mark.parametrize("payload", ['["a","b"]', '"just-a-string"', "42", "null"])
def test_resolve_non_object_json_returns_none(proxy_env, payload):
    """Valid JSON that is not an OBJECT (list/string/number/null) → treated as an
    empty map, no raise.

    Revert: drop the isinstance(dict) check → .get() on a list raises → FAILS.
    """
    proxy_env.setenv("BROKER_PROXY_MAP", payload)
    assert kite_auth.resolve_account_proxy("acctA") is None


def test_resolve_never_logs_the_proxy_url(proxy_env, caplog):
    """SECURITY: the proxy URL embeds a password → it must NEVER reach the logs.
    Only the account id + a yes/no may be logged.

    Revert: log the url ("proxy=%s", url) → the password appears → assert FAILS.
    """
    proxy_env.setenv("BROKER_PROXY_MAP", '{"acctA": "%s"}' % _PROXY_A)
    with caplog.at_level(logging.DEBUG):
        assert kite_auth.resolve_account_proxy("acctA") == _PROXY_A
    blob = "\n".join(r.getMessage() for r in caplog.records)
    assert _PROXY_A not in blob
    assert "s3cr3t-pw" not in blob, "the proxy PASSWORD leaked into the logs"
    assert "13.203.129.136" not in blob
    assert "acctA" in blob, "the account id SHOULD be logged (it is not a secret)"


def test_resolve_non_string_map_value_returns_none(proxy_env):
    """A garbage (non-string) map value → None rather than a crash or a bogus
    proxies dict."""
    proxy_env.setenv("BROKER_PROXY_MAP", '{"acctA": 12345, "acctB": null}')
    assert kite_auth.resolve_account_proxy("acctA") is None
    assert kite_auth.resolve_account_proxy("acctB") is None


# ═══════════════════════════════════════════════════════════════════════════
# INTEGRATION — the dedicated per-account client build
# ═══════════════════════════════════════════════════════════════════════════

def _bound_profile(account_id):
    prof = BrokerProfile("z1", "zerodha", broker_account_id=account_id)
    prof.api_key = "ak-user2"
    prof.access_token = "at-user2"
    prof.owner_user_id = "user2"
    prof.owner_is_admin = False
    return prof


def test_mapped_account_dedicated_client_gets_its_proxy(proxy_env, kite_spy):
    """END-TO-END: a MAPPED broker_account_id → the dedicated KiteConnect is
    constructed with THAT account's proxy → its orders egress from its own
    allowlisted static IP.

    Revert: drop the resolve_account_proxy/proxy_url thread-through in
    zerodha._build_kite → no proxies kwarg → assert FAILS.
    """
    proxy_env.setenv("BROKER_PROXY_MAP", '{"acctA": "%s"}' % _PROXY_A)
    broker = ZerodhaBroker(_bound_profile("acctA"), dry_run=True)
    kite = broker._build_kite()
    assert kite.proxies == {"http": _PROXY_A, "https": _PROXY_A}
    assert kite.access_token == "at-user2"


def test_unmapped_account_dedicated_client_is_direct(proxy_env, kite_spy):
    """An UNMAPPED bound account → proxy None → NO proxies kwarg → direct.
    A second user's proxy must never be inherited by a third account.

    Revert: fall back to "any mapped proxy" → the kwarg appears → assert FAILS.
    """
    proxy_env.setenv("BROKER_PROXY_MAP", '{"acctA": "%s"}' % _PROXY_A)
    broker = ZerodhaBroker(_bound_profile("acctOTHER"), dry_run=True)
    kite = broker._build_kite()
    assert kite.proxies == {}
    assert "proxies" not in kite_spy[0]


def test_bound_account_with_no_map_at_all_is_direct(proxy_env, kite_spy):
    """DEFAULT-OFF end-to-end: no BROKER_PROXY_MAP → even a bound account builds
    exactly as it does today (no proxies kwarg)."""
    broker = ZerodhaBroker(_bound_profile("acctA"), dry_run=True)
    kite = broker._build_kite()
    assert kite_spy == [{"api_key": "ak-user2"}]
    assert kite.proxies == {}


def test_operator_global_path_never_routed_through_a_users_proxy(
        proxy_env, kite_spy, monkeypatch):
    """★ CRITICAL REGRESSION GUARD ★

    The OPERATOR's own account (broker_account_id None → the process-global
    get_kite_client path) must stay DIRECT from his home IP even while another
    user's account IS mapped. Routing his real-money orders through user #2's
    Lightsail proxy would egress from an IP that is NOT allowlisted on HIS Kite
    app → every order rejected ("IP is not allowed to place orders").

    Revert: resolve/apply a proxy on the `bound is None` branch, or make the map
    apply globally → get_kite_client is bypassed / a proxy is applied → FAILS.
    """
    proxy_env.setenv("BROKER_PROXY_MAP", '{"acctA": "%s"}' % _PROXY_A)
    sentinel = object()
    called = []
    monkeypatch.setattr(kite_auth, "get_kite_client",
                        lambda check=True: called.append(check) or sentinel)
    # The operator/global profile: NO bound account, no owner.
    prof = BrokerProfile("z1", "zerodha", broker_account_id=None)
    prof.owner_user_id = None
    prof.owner_is_admin = True
    broker = ZerodhaBroker(prof, dry_run=False)

    kite = broker._build_kite()

    # He goes through the UNTOUCHED global path...
    assert kite is sentinel, "operator must still use the process-global client"
    assert called == [False]
    # ...and no per-account KiteConnect (proxied or not) was constructed for him.
    assert kite_spy == [], "operator's build must not construct a per-account client"


def test_operator_global_path_untouched_when_no_map(proxy_env, monkeypatch):
    """Same operator path with NO map configured — the byte-for-byte baseline."""
    sentinel = object()
    monkeypatch.setattr(kite_auth, "get_kite_client", lambda check=True: sentinel)
    prof = BrokerProfile("z1", "zerodha", broker_account_id=None)
    prof.owner_user_id = None
    prof.owner_is_admin = True
    assert ZerodhaBroker(prof, dry_run=False)._build_kite() is sentinel


# ═══════════════════════════════════════════════════════════════════════════
# INTEGRATION — per-account AUTH egress (login_url / exchange_token)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture
def fake_creds(monkeypatch):
    """Stub the vault so the auth path resolves creds without a real vault key."""
    from autotrade import vault
    creds = vault.DecryptedCreds(
        broker_account_id="acctA", user_id="user2", broker="zerodha",
        account_label="u2", api_key="ak-user2", api_secret="as-user2")
    monkeypatch.setattr(vault, "get_decrypted_creds",
                        lambda bid, user_id=None, **kw: creds)
    return creds


def test_login_url_uses_the_accounts_proxy(proxy_env, fake_creds, monkeypatch):
    """login_url() builds its client on the SAME per-account egress as the order
    client (symmetry; keeps the auth/order IP identical).

    Revert: call _new_kite(api_key) with no proxy_url → captured None → FAILS.
    """
    from autotrade.broker import zerodha_auth
    proxy_env.setenv("BROKER_PROXY_MAP", '{"acctA": "%s"}' % _PROXY_A)
    seen = {}

    class _K:
        def login_url(self):
            return "https://kite.zerodha.com/connect/login?api_key=ak-user2"

    def _spy(api_key, proxy_url=None):
        seen["api_key"], seen["proxy_url"] = api_key, proxy_url
        return _K()

    monkeypatch.setattr(kite_auth, "_new_kite", _spy)
    url = zerodha_auth.login_url("acctA", user_id="user2")
    assert seen["proxy_url"] == _PROXY_A
    assert url.startswith("https://kite.zerodha.com/connect/login")


def test_exchange_token_uses_the_accounts_proxy(proxy_env, fake_creds, monkeypatch):
    """exchange_token()'s generate_session() is a REAL Kite REST egress → it MUST
    originate from the same per-account static IP as the order client, or the
    token exchange itself 403s once that user's IP allowlist is set.

    Revert: call _new_kite(api_key) with no proxy_url → captured None → FAILS.
    """
    from autotrade.broker import zerodha_auth
    proxy_env.setenv("BROKER_PROXY_MAP", '{"acctA": "%s"}' % _PROXY_A)
    seen = {}

    class _K:
        def generate_session(self, request_token, api_secret=None):
            return {"access_token": "new-at"}

    def _spy(api_key, proxy_url=None):
        seen["proxy_url"] = proxy_url
        return _K()

    monkeypatch.setattr(kite_auth, "_new_kite", _spy)
    from autotrade import vault
    monkeypatch.setattr(vault, "store_access_token",
                        lambda bid, tok, user_id=None: {"broker_account_id": bid})

    zerodha_auth.exchange_token("acctA", "req-tok", user_id="user2")
    assert seen["proxy_url"] == _PROXY_A


def test_exchange_token_unmapped_account_stays_direct(proxy_env, fake_creds,
                                                      monkeypatch):
    """An unmapped account's token exchange stays direct (proxy_url None) — the
    default-off guarantee on the auth path too."""
    from autotrade.broker import zerodha_auth
    seen = {}

    class _K:
        def generate_session(self, request_token, api_secret=None):
            return {"access_token": "new-at"}

    monkeypatch.setattr(kite_auth, "_new_kite",
                        lambda api_key, proxy_url=None: seen.setdefault(
                            "proxy_url", proxy_url) or _K())
    from autotrade import vault
    monkeypatch.setattr(vault, "store_access_token",
                        lambda bid, tok, user_id=None: {"broker_account_id": bid})

    zerodha_auth.exchange_token("acctA", "req-tok", user_id="user2")
    assert seen["proxy_url"] is None
