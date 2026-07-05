"""BROKER-AGNOSTIC AUTH FOUNDATION tests (Stage 0 + Stage 1 core).

Covers:
  * auth_base value types + RefreshNotSupported default.
  * registry: zerodha LIVE + fyers/upstox/angel/dhan PLACEHOLDER; NotImplementedAuth raises.
  * vault: store_tokens / refresh-token round-trip / record_health (real Fernet key).
  * ZerodhaAuth: capabilities, expiry math (next 06:00 IST), RefreshNotSupported.
  * account_lifecycle: happy path via a MOCK auth provider injected into the registry.
  * assert_account_tradeable gating (ACTIVE ok; PENDING/EXPIRED raise).

Uses the shared conftest temp DB. A REAL Fernet key is enabled per-test via
FALCON_VAULT_KEY (monkeypatched) so the vault store/decrypt round-trips run. No
real broker / Kite / orders. All datetimes IST.
"""
from datetime import datetime, timedelta, timezone

import pytest

from autotrade import vault
from autotrade.broker import registry
from autotrade.broker import account_lifecycle as lifecycle
from autotrade.broker.account_lifecycle import (AccountNotTradeable,
                                                AccountLifecycleError,
                                                assert_account_tradeable)
from autotrade.broker.auth_base import (BrokerAuthProvider, BrokerCapabilities,
                                        RefreshNotSupported, TokenHealth, TokenSet,
                                        IST)
from autotrade.broker.zerodha_auth_provider import ZerodhaAuth, _next_0600_ist


# ── vault-enable helper ───────────────────────────────────────────────────────

@pytest.fixture
def vault_key(monkeypatch):
    """Enable the vault with a fresh real Fernet key for the test."""
    key = vault.generate_key()
    monkeypatch.setenv("FALCON_VAULT_KEY", key)
    monkeypatch.delenv("FALCON_VAULT_KEY_PREV", raising=False)
    assert vault.vault_enabled()
    yield key


@pytest.fixture
def wipe_accounts():
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute("DELETE FROM broker_accounts")
        con.commit()
    yield
    with falcon_conn() as con:
        con.execute("DELETE FROM broker_accounts")
        con.commit()


# ── 1. auth_base value types ──────────────────────────────────────────────────

def test_capabilities_shape():
    c = BrokerCapabilities(auth_kind="request_token", has_refresh_token=False,
                           token_lifetime="daily_0600_ist", supports_gtt=True,
                           supports_mtf=True, fno=True)
    assert c.auth_kind == "request_token"
    assert c.has_refresh_token is False
    assert c.supports_gtt and c.supports_mtf and c.fno


def test_tokenset_defaults():
    ts = TokenSet(access_token="abc")
    assert ts.access_token == "abc"
    assert ts.refresh_token is None
    assert ts.expires_at is None
    assert ts.issued_at.tzinfo is not None  # default_factory stamps IST


def test_tokenhealth_status_vocab():
    h = TokenHealth(ok=True, status=vault.STATUS_ACTIVE)
    assert h.status == "ACTIVE"
    assert h.detail == ""


def test_base_refresh_raises_not_supported():
    class _Bare(BrokerAuthProvider):
        broker_name = "bare"
        def login_url(self, creds, *, redirect_uri, state): return "x"
        def exchange(self, creds, *, request_token): return TokenSet("a")
        def validate(self, creds): return TokenHealth(True, "ACTIVE")
    with pytest.raises(RefreshNotSupported):
        _Bare().refresh(None)


# ── 2. registry ───────────────────────────────────────────────────────────────

def test_registry_zerodha_live():
    assert registry.is_live("zerodha") is True
    from autotrade.broker.zerodha import ZerodhaBroker
    assert registry.get_client_cls("zerodha") is ZerodhaBroker
    auth = registry.get_auth("zerodha")
    assert isinstance(auth, ZerodhaAuth)


@pytest.mark.parametrize("broker", ["fyers", "upstox", "angel", "dhan"])
def test_registry_placeholders(broker):
    assert registry.is_live(broker) is False
    # Execution client is the real stub class.
    cls = registry.get_client_cls(broker)
    assert cls.broker_name == broker
    # Auth is a placeholder that RAISES on login_url / exchange.
    auth = registry.get_auth(broker)
    with pytest.raises(NotImplementedError):
        auth.login_url(None, redirect_uri="", state="")
    with pytest.raises(NotImplementedError):
        auth.exchange(None, request_token="rt")
    # validate() degrades gracefully (no raise) → ERROR health.
    h = auth.validate(None)
    assert h.ok is False and h.status == "ERROR"


def test_registry_unknown_broker_raises():
    with pytest.raises(ValueError):
        registry.get_entry("nosuchbroker")


def test_list_supported_complete():
    rows = {r["broker"]: r for r in registry.list_supported()}
    assert set(rows) == {"zerodha", "fyers", "upstox", "angel", "dhan",
                         "rupeezy", "fivepaisa"}
    assert rows["zerodha"]["live"] is True
    assert rows["fyers"]["live"] is False
    # rupeezy is a real (Stage-2) adapter → live=True (certification pending).
    assert rows["rupeezy"]["live"] is True
    assert rows["zerodha"]["capabilities"]["auth_kind"] == "request_token"


# ── 3. vault store_tokens / refresh round-trip / record_health ────────────────

def test_store_tokens_access_only_round_trip(vault_key, wipe_accounts):
    pub = vault.put_account("u1", "zerodha", "Main", "apikey123", "secretXYZ")
    bid = pub["broker_account_id"]
    out = vault.store_tokens(bid, "acc-token-1", user_id="u1")
    assert out["status"] == "ACTIVE"
    creds = vault.get_decrypted_creds(bid, user_id="u1")
    assert creds.access_token == "acc-token-1"
    assert creds.refresh_token is None
    assert creds.api_secret == "secretXYZ"


def test_store_tokens_with_refresh_round_trip(vault_key, wipe_accounts):
    pub = vault.put_account("u1", "upstox", "Up1", "ukey", "usecret")
    bid = pub["broker_account_id"]
    exp = (datetime.now(IST) + timedelta(hours=12)).isoformat()
    vault.store_tokens(bid, "acc-tok", refresh_token="ref-tok",
                       expires_at=exp, user_id="u1")
    creds = vault.get_decrypted_creds(bid, user_id="u1")
    assert creds.access_token == "acc-tok"
    assert creds.refresh_token == "ref-tok"
    assert creds.token_expires_at == exp


def test_store_tokens_access_refresh_preserves_existing_refresh(vault_key, wipe_accounts):
    """A plain access-token refresh (no new refresh token) must NOT wipe the
    still-valid stored refresh token."""
    pub = vault.put_account("u1", "upstox", "Up1", "ukey", "usecret")
    bid = pub["broker_account_id"]
    vault.store_tokens(bid, "acc-1", refresh_token="ref-keep", user_id="u1")
    # Renew access token only.
    vault.store_tokens(bid, "acc-2", refresh_token=None, user_id="u1")
    creds = vault.get_decrypted_creds(bid, user_id="u1")
    assert creds.access_token == "acc-2"
    assert creds.refresh_token == "ref-keep"


def test_store_access_token_delegates(vault_key, wipe_accounts):
    """Back-compat shim still works and sets ACTIVE."""
    pub = vault.put_account("u1", "zerodha", "Main", "k", "s")
    bid = pub["broker_account_id"]
    out = vault.store_access_token(bid, "tok", user_id="u1")
    assert out["status"] == "ACTIVE"
    assert vault.get_decrypted_creds(bid, user_id="u1").access_token == "tok"


def test_record_health(vault_key, wipe_accounts):
    from falcon.db import falcon_conn
    pub = vault.put_account("u1", "zerodha", "Main", "k", "s")
    bid = pub["broker_account_id"]
    assert vault.record_health(bid, "ACTIVE", "profile ok", user_id="u1") is True
    with falcon_conn() as con:
        row = con.execute(
            "SELECT last_health_status, last_error, last_health_at "
            "FROM broker_accounts WHERE broker_account_id=?", (bid,)).fetchone()
    assert row["last_health_status"] == "ACTIVE"
    assert row["last_error"] == "profile ok"
    assert row["last_health_at"] is not None


def test_record_health_scoped_to_user(vault_key, wipe_accounts):
    pub = vault.put_account("u1", "zerodha", "Main", "k", "s")
    bid = pub["broker_account_id"]
    # Wrong user → no update.
    assert vault.record_health(bid, "ERROR", "x", user_id="uOTHER") is False


# ── 4. ZerodhaAuth ────────────────────────────────────────────────────────────

def test_zerodha_capabilities():
    c = ZerodhaAuth.capabilities
    assert c.auth_kind == "request_token"
    assert c.has_refresh_token is False
    assert c.token_lifetime == "daily_0600_ist"
    assert c.supports_gtt and c.supports_mtf and c.fno


def test_zerodha_refresh_not_supported():
    with pytest.raises(RefreshNotSupported):
        ZerodhaAuth().refresh(object())


def test_next_0600_ist_before_six():
    now = datetime(2026, 6, 25, 3, 0, tzinfo=IST)  # 03:00 → today 06:00
    nxt = _next_0600_ist(now)
    assert nxt == datetime(2026, 6, 25, 6, 0, tzinfo=IST)


def test_next_0600_ist_after_six():
    now = datetime(2026, 6, 25, 10, 0, tzinfo=IST)  # 10:00 → tomorrow 06:00
    nxt = _next_0600_ist(now)
    assert nxt == datetime(2026, 6, 26, 6, 0, tzinfo=IST)


def test_zerodha_expiry_derives_from_issued_at():
    ts = TokenSet(access_token="a",
                  issued_at=datetime(2026, 6, 25, 10, 0, tzinfo=IST),
                  expires_at=None)
    exp = ZerodhaAuth().expiry(ts)
    assert exp == datetime(2026, 6, 26, 6, 0, tzinfo=IST)


def test_zerodha_expiry_uses_stamped_when_set():
    stamped = datetime(2026, 6, 30, 6, 0, tzinfo=IST)
    ts = TokenSet(access_token="a", expires_at=stamped)
    assert ZerodhaAuth().expiry(ts) == stamped


def test_zerodha_validate_no_token_is_expired():
    class _C:
        api_key = "k"
        access_token = ""
    h = ZerodhaAuth().validate(_C())
    assert h.ok is False and h.status == "EXPIRED"


# ── 5. account_lifecycle happy path with a MOCK auth provider ─────────────────

class _MockAuth(BrokerAuthProvider):
    """Fake refresh-capable broker auth injected into the registry for tests."""
    broker_name = "zerodha"  # reuse a vault-VALID broker so put_account accepts it
    capabilities = BrokerCapabilities(
        auth_kind="oauth2_code", has_refresh_token=True,
        token_lifetime="seconds:86400", supports_gtt=False,
        supports_mtf=True, fno=True)

    def login_url(self, creds, *, redirect_uri, state):
        return f"https://mock/login?key={creds.api_key}&state={state}"

    def exchange(self, creds, *, request_token):
        return TokenSet(access_token=f"acc-for-{request_token}",
                        refresh_token="ref-1",
                        expires_at=datetime.now(IST) + timedelta(days=1))

    def refresh(self, creds):
        return TokenSet(access_token="acc-refreshed", refresh_token="ref-2",
                        expires_at=datetime.now(IST) + timedelta(days=1))

    def validate(self, creds):
        ok = bool(creds.access_token)
        return TokenHealth(ok=ok, status="ACTIVE" if ok else "EXPIRED")


@pytest.fixture
def mock_registry(monkeypatch):
    """Swap the zerodha entry's AUTH for _MockAuth + mark capabilities as
    refresh-capable, without touching the execution client. Restored after test."""
    registry._build_registry()
    orig = registry._REGISTRY["zerodha"]
    monkeypatch.setitem(
        registry._REGISTRY, "zerodha",
        registry.BrokerEntry(broker="zerodha", client_cls=orig.client_cls,
                             auth_cls=_MockAuth, capabilities=_MockAuth.capabilities,
                             live=True))
    yield


def test_lifecycle_begin_connect_pending(vault_key, wipe_accounts, mock_registry):
    pub = lifecycle.begin_connect("u1", "zerodha", "Main", "apik", "apis")
    assert pub["status"] == "PENDING"


def test_lifecycle_login_url(vault_key, wipe_accounts, mock_registry):
    pub = lifecycle.begin_connect("u1", "zerodha", "Main", "apik", "apis")
    url = lifecycle.login_url("u1", pub["broker_account_id"], state="s123")
    assert "state=s123" in url and "apik" in url


def test_lifecycle_complete_connect_active(vault_key, wipe_accounts, mock_registry):
    pub = lifecycle.begin_connect("u1", "zerodha", "Main", "apik", "apis")
    bid = pub["broker_account_id"]
    out = lifecycle.complete_connect("u1", bid, "req-tok-42")
    assert out["status"] == "ACTIVE"
    creds = vault.get_decrypted_creds(bid, user_id="u1")
    assert creds.access_token == "acc-for-req-tok-42"
    assert creds.refresh_token == "ref-1"


def test_lifecycle_refresh_supported(vault_key, wipe_accounts, mock_registry):
    pub = lifecycle.begin_connect("u1", "zerodha", "Main", "apik", "apis")
    bid = pub["broker_account_id"]
    lifecycle.complete_connect("u1", bid, "rt")
    out = lifecycle.refresh_if_supported("u1", bid)
    assert out.get("refresh_supported") is True
    creds = vault.get_decrypted_creds(bid, user_id="u1")
    assert creds.access_token == "acc-refreshed"
    assert creds.refresh_token == "ref-2"


def test_lifecycle_health_check_records(vault_key, wipe_accounts, mock_registry):
    from falcon.db import falcon_conn
    pub = lifecycle.begin_connect("u1", "zerodha", "Main", "apik", "apis")
    bid = pub["broker_account_id"]
    lifecycle.complete_connect("u1", bid, "rt")
    h = lifecycle.health_check("u1", bid)
    assert h.ok is True and h.status == "ACTIVE"
    with falcon_conn() as con:
        row = con.execute(
            "SELECT last_health_status FROM broker_accounts WHERE broker_account_id=?",
            (bid,)).fetchone()
    assert row["last_health_status"] == "ACTIVE"


def test_lifecycle_refresh_not_supported_daily_broker(vault_key, wipe_accounts):
    """Real zerodha (no refresh) → reconnect_needed, no raise."""
    pub = lifecycle.begin_connect("u1", "zerodha", "Main", "apik", "apis")
    bid = pub["broker_account_id"]
    out = lifecycle.refresh_if_supported("u1", bid)
    assert out.get("reconnect_needed") is True
    assert out.get("refresh_supported") is False


# ── 6. assert_account_tradeable gating ────────────────────────────────────────

def test_assert_tradeable_active_ok(vault_key, wipe_accounts, mock_registry):
    pub = lifecycle.begin_connect("u1", "zerodha", "Main", "apik", "apis")
    bid = pub["broker_account_id"]
    lifecycle.complete_connect("u1", bid, "rt")  # → ACTIVE
    assert_account_tradeable("u1", bid)  # no raise


def test_assert_tradeable_pending_raises(vault_key, wipe_accounts):
    pub = lifecycle.begin_connect("u1", "zerodha", "Main", "apik", "apis")
    bid = pub["broker_account_id"]  # PENDING (never exchanged)
    with pytest.raises(AccountNotTradeable):
        assert_account_tradeable("u1", bid)


def test_assert_tradeable_expired_raises(vault_key, wipe_accounts):
    from falcon.db import falcon_conn
    pub = vault.put_account("u1", "zerodha", "Main", "k", "s")
    bid = pub["broker_account_id"]
    vault.store_access_token(bid, "tok", user_id="u1")  # ACTIVE today
    # Force a stale token_date → derived EXPIRED.
    with falcon_conn() as con:
        con.execute("UPDATE broker_accounts SET token_date='2020-01-01' "
                    "WHERE broker_account_id=?", (bid,))
        con.commit()
    with pytest.raises(AccountNotTradeable):
        assert_account_tradeable("u1", bid)


def test_assert_tradeable_missing_raises():
    with pytest.raises(AccountNotTradeable):
        assert_account_tradeable("u1", "no-such-account")
