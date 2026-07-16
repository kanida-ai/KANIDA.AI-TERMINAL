"""SELF-SERVICE per-account static egress IP (DB-first, encrypted, pooled).

WHY THIS EXISTS: SEBI/Zerodha bind ONE static IP to ONE broker account, so every
power user needs their own egress IP. That mapping used to be hand-written into
config/.env (BROKER_PROXY_MAP) + a backend restart, PER USER — not self-service
and not scalable. This suite pins the replacement:

  * resolution is DB-FIRST (a DB write ⇒ no restart), env map SECOND (the
    already-live mapping for the first onboarded user must not break), global
    hook THIRD, else DIRECT;
  * the URL is a SECRET (it embeds a password) — encrypted at rest, never in a
    log line, never in an API response;
  * every failure mode falls through to DIRECT, NEVER to a wrong proxy — routing
    a user's real-money orders out of someone else's allowlisted IP is worse
    than no proxy at all;
  * the operator's own global path (broker_account_id None) is untouched.
"""
import logging
import uuid

import pytest

from autotrade import vault
from autotrade.broker import egress
import services.kite_auth as kite_auth

# Realistic values — every one embeds a PASSWORD that must never surface.
_DB_PROXY = "http://kanida:db-pw-9@13.203.129.136:8888"
_ENV_PROXY = "http://kanida:env-pw-9@13.203.129.137:8888"
_GLOBAL_PROXY = "http://kanida:global-pw@10.0.0.9:3128"
_POOL_1 = "http://kanida:pool-pw-1@13.203.129.140:8888"
_POOL_2 = "http://kanida:pool-pw-2@13.203.129.141:8888"


@pytest.fixture
def vault_key(monkeypatch):
    monkeypatch.setenv("FALCON_VAULT_KEY", vault.generate_key())
    monkeypatch.delenv("FALCON_VAULT_KEY_PREV", raising=False)
    return monkeypatch


@pytest.fixture
def clean_env(monkeypatch):
    """Hermetic egress env — the operator's REAL config/.env can never leak in."""
    monkeypatch.delenv("BROKER_PROXY_MAP", raising=False)
    monkeypatch.delenv("BROKER_PROXY_URL", raising=False)
    monkeypatch.delenv(egress.POOL_ENV, raising=False)
    monkeypatch.setattr(kite_auth, "_load_env_file", lambda: None)
    monkeypatch.setattr(kite_auth, "_PROXY_MAP_WARNED", False)
    monkeypatch.setattr(egress, "_POOL_WARNED", False)
    return monkeypatch


@pytest.fixture
def account(vault_key, clean_env):
    """A real broker_accounts row (isolated temp DB from conftest).

    The label MUST be uuid-unique: put_account UPDATEs in place on a repeated
    (user_id, broker, account_label), so a reused label would hand the next test
    a row still carrying the previous test's egress blob — encrypted under a
    different per-test vault key.
    """
    acct = vault.put_account(user_id="9", broker="zerodha",
                             account_label=f"kite-{uuid.uuid4().hex}",
                             api_key="ak", api_secret="as")
    return acct["broker_account_id"]


# ═══════════════════════════════════════════════════════════════════════════
# encryption round-trip + at-rest secrecy
# ═══════════════════════════════════════════════════════════════════════════

def test_set_then_get_round_trips(account):
    """The core contract: what we store decrypts back byte-identical."""
    assert egress.set_account_proxy(account, _DB_PROXY) is True
    assert egress.get_account_proxy(account) == _DB_PROXY


def test_stored_blob_is_encrypted_not_plaintext(account):
    """AT REST: the raw column must NOT contain the URL or its password — this is
    the whole reason the column is a Fernet BLOB and not TEXT.

    Revert: store the URL as plaintext → the password appears → assert FAILS.
    """
    from falcon.db import falcon_conn
    egress.set_account_proxy(account, _DB_PROXY)
    with falcon_conn() as con:
        blob = con.execute(
            "SELECT egress_proxy_url_enc FROM broker_accounts "
            "WHERE broker_account_id=?", (account,)).fetchone()[0]
    assert blob is not None
    raw = bytes(blob)
    assert b"db-pw-9" not in raw, "the proxy PASSWORD is stored in plaintext"
    assert b"13.203.129.136" not in raw
    assert raw.startswith(b"gAAAAA"), "not a Fernet token"


def test_set_requires_a_vault_key(clean_env, monkeypatch):
    """Refuse to store a password-bearing URL with no vault key, rather than
    silently writing it in the clear."""
    monkeypatch.delenv("FALCON_VAULT_KEY", raising=False)
    monkeypatch.setattr(vault, "_load_env_file", lambda: None)
    with pytest.raises(vault.VaultDisabledError):
        egress.set_account_proxy("whatever", _DB_PROXY)


@pytest.mark.parametrize("bad", ["", "   ", None])
def test_set_rejects_blank_url(account, bad):
    with pytest.raises(ValueError):
        egress.set_account_proxy(account, bad)


def test_set_rejects_unparseable_url(account):
    """A garbage URL must fail at CONFIG time, not at 09:15 on a live order."""
    with pytest.raises(ValueError):
        egress.set_account_proxy(account, "not-a-url")


# ═══════════════════════════════════════════════════════════════════════════
# RESOLUTION ORDER — (a) DB → (b) env map → (c) global → (d) direct
# ═══════════════════════════════════════════════════════════════════════════

def test_db_wins_over_env_map(account, clean_env):
    """★ THE SELF-SERVICE HINGE ★ DB-first is what removes the restart: env is
    read once per process, so an env-only mechanism can never be self-service.

    Revert: check the env map first → the env url wins → assert FAILS.
    """
    egress.set_account_proxy(account, _DB_PROXY)
    clean_env.setenv("BROKER_PROXY_MAP", '{"%s": "%s"}' % (account, _ENV_PROXY))
    assert kite_auth.resolve_account_proxy(account) == _DB_PROXY


def test_env_map_still_works_when_no_db_value(account, clean_env):
    """★ LIVE REGRESSION GUARD ★ user 9's hand-written .env mapping is in
    production RIGHT NOW and must keep resolving until it is migrated.

    Revert: delete the env fallback → None → assert FAILS.
    """
    clean_env.setenv("BROKER_PROXY_MAP", '{"%s": "%s"}' % (account, _ENV_PROXY))
    assert kite_auth.resolve_account_proxy(account) == _ENV_PROXY


def test_nothing_configured_is_direct(account, clean_env):
    """DEFAULT-OFF: no DB value, no map, no global → None → DIRECT egress."""
    assert kite_auth.resolve_account_proxy(account) is None


def test_unknown_account_never_inherits_another_accounts_proxy(account, clean_env):
    """An account with no egress of its own must NEVER pick up someone else's."""
    egress.set_account_proxy(account, _DB_PROXY)
    assert kite_auth.resolve_account_proxy("some-other-account-id") is None


def test_global_hook_still_applies_when_account_has_nothing(account, clean_env,
                                                            monkeypatch):
    """(c) the pre-existing global BROKER_PROXY_URL is applied by _new_kite when
    resolution returns None — the layer below must be preserved, not replaced."""
    calls = []

    class _FakeKite:
        def __init__(self, **kw):
            calls.append(kw)
            self.proxies = kw.get("proxies", {}) or {}
            self.timeout = None

    import kiteconnect
    monkeypatch.setattr(kiteconnect, "KiteConnect", _FakeKite)
    monkeypatch.setattr(kite_auth, "_load_env_file", lambda: None)
    monkeypatch.setenv("BROKER_PROXY_URL", _GLOBAL_PROXY)
    kite = kite_auth._new_kite("ak", proxy_url=kite_auth.resolve_account_proxy(account))
    assert kite.proxies == {"http": _GLOBAL_PROXY, "https": _GLOBAL_PROXY}


def test_operator_global_path_short_circuits_before_any_db_lookup(
        account, clean_env, monkeypatch):
    """★ CRITICAL ★ The operator trades on the process-global path
    (broker_account_id=None). It must resolve to None WITHOUT touching the DB —
    routing his real-money orders through a user's proxy would egress from an IP
    not allowlisted on HIS Kite app → every order rejected.

    Revert: drop the `if not broker_account_id: return None` guard → the DB is
    consulted → the boom sentinel raises → assert FAILS.
    """
    def _boom(*a, **kw):
        raise AssertionError("operator's global path must never hit the egress DB")

    monkeypatch.setattr(egress, "get_account_proxy", _boom)
    egress.set_account_proxy  # (unused; keep the import honest)
    assert kite_auth.resolve_account_proxy(None) is None
    assert kite_auth.resolve_account_proxy("") is None


# ═══════════════════════════════════════════════════════════════════════════
# FAIL-OPEN-TO-DIRECT (never to a wrong proxy)
# ═══════════════════════════════════════════════════════════════════════════

def test_decrypt_failure_falls_through_to_env_never_wrong_proxy(account, clean_env,
                                                                monkeypatch):
    """★ A corrupt/wrong-key blob must NOT crash and must NOT route anywhere
    wrong — it degrades to the next layer (env), exactly as if unset.

    Revert: let _decrypt's None propagate as a proxy value → TypeError/None-proxy
    → assert FAILS.
    """
    egress.set_account_proxy(account, _DB_PROXY)
    monkeypatch.setattr(vault, "_decrypt", lambda blob, provider: None)
    clean_env.setenv("BROKER_PROXY_MAP", '{"%s": "%s"}' % (account, _ENV_PROXY))
    assert egress.get_account_proxy(account) is None
    assert kite_auth.resolve_account_proxy(account) == _ENV_PROXY


def test_decrypt_failure_with_no_env_is_direct(account, clean_env, monkeypatch):
    """Same corruption, nothing else configured → DIRECT, not a crash."""
    egress.set_account_proxy(account, _DB_PROXY)
    monkeypatch.setattr(vault, "_decrypt", lambda blob, provider: None)
    assert kite_auth.resolve_account_proxy(account) is None


def test_db_error_falls_through_and_never_raises(account, clean_env, monkeypatch):
    """An unreadable DB must not take the shared real-money auth path down."""
    def _boom():
        raise RuntimeError("db is on fire")

    import falcon.db
    monkeypatch.setattr(falcon.db, "connect_falcon", _boom)
    assert egress.get_account_proxy(account) is None
    assert kite_auth.resolve_account_proxy(account) is None


def test_disabled_vault_resolves_to_direct(account, clean_env, monkeypatch):
    """No vault key → we cannot decrypt → DIRECT (never a guess)."""
    egress.set_account_proxy(account, _DB_PROXY)
    monkeypatch.delenv("FALCON_VAULT_KEY", raising=False)
    monkeypatch.setattr(vault, "_load_env_file", lambda: None)
    assert egress.get_account_proxy(account) is None


def test_egress_module_import_failure_degrades_to_env(account, clean_env,
                                                      monkeypatch):
    """If the autotrade package is broken/absent, services.kite_auth must still
    resolve via the env map rather than explode (it is the shared auth module)."""
    import builtins
    real_import = builtins.__import__

    def _fake(name, *a, **kw):
        if name == "autotrade.broker.egress":
            raise ImportError("boom")
        return real_import(name, *a, **kw)

    clean_env.setenv("BROKER_PROXY_MAP", '{"%s": "%s"}' % (account, _ENV_PROXY))
    monkeypatch.setattr(builtins, "__import__", _fake)
    assert kite_auth.resolve_account_proxy(account) == _ENV_PROXY


# ═══════════════════════════════════════════════════════════════════════════
# SECRECY — the URL must never reach a log or an API response
# ═══════════════════════════════════════════════════════════════════════════

def test_no_log_line_ever_contains_the_url_or_password(account, clean_env, caplog):
    """SECURITY: set + get + status, all at DEBUG — the password must not appear.

    Revert: log("proxy=%s", url) anywhere → assert FAILS.
    """
    with caplog.at_level(logging.DEBUG):
        egress.set_account_proxy(account, _DB_PROXY)
        egress.get_account_proxy(account)
        egress.egress_status(account)
        kite_auth.resolve_account_proxy(account)
    blob = "\n".join(r.getMessage() for r in caplog.records)
    assert _DB_PROXY not in blob
    assert "db-pw-9" not in blob, "the proxy PASSWORD leaked into the logs"
    assert "kanida:db-pw-9" not in blob


def test_egress_status_exposes_ip_only_never_the_url(account):
    """The connect-UI contract: the bare IP (the user must register it) and
    nothing else. No URL, no credentials, no port.

    Revert: return the url in the payload → assert FAILS.
    """
    egress.set_account_proxy(account, _DB_PROXY)
    st = egress.egress_status(account)
    assert st["configured"] is True
    assert st["egress_ip"] == "13.203.129.136"
    assert st["source"] == "account"
    blob = repr(st)
    assert "db-pw-9" not in blob and _DB_PROXY not in blob
    assert ":8888" not in blob, "the port (and thus URL shape) leaked"


def test_egress_status_direct_when_unassigned(account):
    st = egress.egress_status(account)
    assert st["configured"] is False
    assert st["egress_ip"] is None
    assert st["source"] == "direct"


def test_egress_status_reports_env_map_source(account, clean_env):
    """An account still on the legacy .env mapping must SHOW its real IP (so the
    UI never tells user 9 he is 'direct' while his orders go via the proxy)."""
    clean_env.setenv("BROKER_PROXY_MAP", '{"%s": "%s"}' % (account, _ENV_PROXY))
    st = egress.egress_status(account)
    assert st["egress_ip"] == "13.203.129.137"
    assert st["source"] == "env_map"


def test_public_account_dict_never_leaks_the_url(account):
    """vault._row_to_public feeds the connection cards — IP yes, URL never."""
    egress.set_account_proxy(account, _DB_PROXY)
    pub = vault.get_account_public(account)
    assert pub["has_egress_proxy"] is True
    assert pub["egress_ip"] == "13.203.129.136"
    assert "db-pw-9" not in repr(pub)


def test_public_account_dict_defaults_to_no_egress(account):
    pub = vault.get_account_public(account)
    assert pub["has_egress_proxy"] is False
    assert pub["egress_ip"] is None


@pytest.mark.parametrize("url,expected", [
    ("http://u:p@13.203.129.136:8888", "13.203.129.136"),
    ("http://13.203.129.136:8888", "13.203.129.136"),
    ("https://user:pw@proxy.example.com:3128", "proxy.example.com"),
    ("not-a-url", None), ("", None), (None, None), (123, None),
])
def test_egress_host_extraction(url, expected):
    assert egress.egress_host(url) == expected


# ═══════════════════════════════════════════════════════════════════════════
# POOL — assign / release
# ═══════════════════════════════════════════════════════════════════════════

def test_assign_takes_a_free_ip_and_persists_it(account, clean_env):
    clean_env.setenv(egress.POOL_ENV, '["%s","%s"]' % (_POOL_1, _POOL_2))
    st = egress.assign_from_pool(account)
    assert st["egress_ip"] == "13.203.129.140"
    assert egress.get_account_proxy(account) == _POOL_1


def test_assign_is_idempotent(account, clean_env):
    """Re-running onboarding must not burn a 2nd IP nor silently move the user
    off the IP he already registered with his broker."""
    clean_env.setenv(egress.POOL_ENV, '["%s","%s"]' % (_POOL_1, _POOL_2))
    first = egress.assign_from_pool(account)
    again = egress.assign_from_pool(account)
    assert again["egress_ip"] == first["egress_ip"]
    assert egress.pool_status()["available"] == 1


def test_two_accounts_get_distinct_ips(vault_key, clean_env):
    """★ THE SEBI RULE ★ one static IP → one broker account. Two accounts must
    never share an egress IP.

    Revert: always hand out pool[0] → both get .140 → assert FAILS.
    """
    clean_env.setenv(egress.POOL_ENV, '["%s","%s"]' % (_POOL_1, _POOL_2))
    a = vault.put_account("9", "zerodha", "acct-a", "ak", "as")["broker_account_id"]
    b = vault.put_account("10", "zerodha", "acct-b", "ak", "as")["broker_account_id"]
    ip_a = egress.assign_from_pool(a)["egress_ip"]
    ip_b = egress.assign_from_pool(b)["egress_ip"]
    assert ip_a != ip_b
    assert {ip_a, ip_b} == {"13.203.129.140", "13.203.129.141"}


def test_exhausted_pool_raises_rather_than_reusing_an_ip(vault_key, clean_env):
    """When the pool runs dry we FAIL LOUD — handing a second account the same
    IP would break BOTH users' order routing."""
    clean_env.setenv(egress.POOL_ENV, '["%s"]' % _POOL_1)
    a = vault.put_account("9", "zerodha", "x-a", "ak", "as")["broker_account_id"]
    b = vault.put_account("10", "zerodha", "x-b", "ak", "as")["broker_account_id"]
    egress.assign_from_pool(a)
    with pytest.raises(egress.NoEgressAvailableError):
        egress.assign_from_pool(b)
    assert egress.get_account_proxy(b) is None


def test_no_pool_configured_raises(account, clean_env):
    with pytest.raises(egress.NoEgressAvailableError):
        egress.assign_from_pool(account)


def test_clear_releases_the_ip_back_to_the_pool(account, clean_env):
    clean_env.setenv(egress.POOL_ENV, '["%s"]' % _POOL_1)
    egress.assign_from_pool(account)
    assert egress.pool_status()["available"] == 0
    assert egress.clear_account_proxy(account) is True
    assert egress.get_account_proxy(account) is None
    assert egress.pool_status()["available"] == 1


def test_deleting_the_account_releases_its_ip(vault_key, clean_env):
    """Release-on-delete: availability is DERIVED from live rows, so a deleted
    account cannot leak its IP out of the pool forever."""
    clean_env.setenv(egress.POOL_ENV, '["%s"]' % _POOL_1)
    a = vault.put_account("9", "zerodha", "del-me", "ak", "as")["broker_account_id"]
    egress.assign_from_pool(a)
    assert egress.pool_status()["available"] == 0
    assert vault.delete_account(a, user_id="9") is True
    assert egress.pool_status()["available"] == 1


def test_pool_status_never_exposes_urls(account, clean_env):
    clean_env.setenv(egress.POOL_ENV, '["%s","%s"]' % (_POOL_1, _POOL_2))
    egress.assign_from_pool(account)
    st = egress.pool_status()
    assert st["pool_size"] == 2 and st["assigned"] == 1 and st["available"] == 1
    blob = repr(st)
    assert "pool-pw-1" not in blob and "pool-pw-2" not in blob
    assert st["available_ips"] == ["13.203.129.141"]
    assert st["assigned_ips"] == ["13.203.129.140"]


@pytest.mark.parametrize("payload", ["{not json", '"a string"', "42", '{"a":1}'])
def test_malformed_pool_is_empty_not_a_crash(clean_env, payload):
    clean_env.setenv(egress.POOL_ENV, payload)
    assert egress.pool_urls() == []


def test_pool_skips_garbage_entries(clean_env):
    clean_env.setenv(egress.POOL_ENV, '["%s", "not-a-url", "", 42, null]' % _POOL_1)
    assert egress.pool_urls() == [_POOL_1]


def test_malformed_pool_warns_once(clean_env, caplog):
    clean_env.setenv(egress.POOL_ENV, "{not json")
    with caplog.at_level(logging.WARNING, logger="kanida.autotrade.egress"):
        egress.pool_urls()
        egress.pool_urls()
    warns = [r for r in caplog.records if egress.POOL_ENV in r.getMessage()]
    assert len(warns) == 1


# ═══════════════════════════════════════════════════════════════════════════
# BROKER-AGNOSTIC
# ═══════════════════════════════════════════════════════════════════════════

def test_egress_module_has_no_broker_specific_branching():
    """★ OPERATOR HARD REQUIREMENT ★ the mechanism must work for ANY broker.

    Revert: add `if broker == "zerodha"` → assert FAILS.
    """
    import inspect
    src = inspect.getsource(egress)
    for name in ("zerodha", "rupeezy", "upstox", "dhan", "fyers", "angel",
                 "fivepaisa"):
        assert f'== "{name}"' not in src and f"== '{name}'" not in src


def test_egress_works_for_a_non_zerodha_account(vault_key, clean_env):
    """A RUPEEZY account gets per-account egress through the identical path —
    nothing in the mechanism is Zerodha-specific (only the client that consumes
    the resolved URL is, today)."""
    clean_env.setenv(egress.POOL_ENV, '["%s"]' % _POOL_1)
    a = vault.put_account("11", "rupeezy", "vortex-1", "ak", "as")["broker_account_id"]
    assert egress.assign_from_pool(a)["egress_ip"] == "13.203.129.140"
    assert kite_auth.resolve_account_proxy(a) == _POOL_1
