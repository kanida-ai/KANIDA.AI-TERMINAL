"""PHASE-2 MULTI-TENANT tests: credential vault + per-account broker wiring +
user/account scoping. ADDITIVE; the regression cases (NULL user/account ==
legacy path; vault disabled == today) are the critical ones — they prove the
currently-running operator sessions are byte-for-byte unaffected.

No real broker, no real Kite, no real orders. The vault key + per-account Kite
construction are exercised against an in-process Fernet key + monkeypatched
KiteConnect constructors.
"""
import os
import uuid

import pytest

from cryptography.fernet import Fernet

from autotrade import vault
from autotrade.config import BrokerProfile, TradingSessionConfig
from autotrade.broker.zerodha import ZerodhaBroker


# ── a key provider backed by a fresh in-memory Fernet key ────────────────────

@pytest.fixture
def test_key(monkeypatch):
    key = Fernet.generate_key().decode()
    monkeypatch.setenv("FALCON_VAULT_KEY", key)
    # Clear any stale prev key.
    monkeypatch.delenv("FALCON_VAULT_KEY_PREV", raising=False)
    # The default provider reads env lazily, so setting env enables the vault.
    return key


@pytest.fixture
def vault_off(monkeypatch):
    monkeypatch.delenv("FALCON_VAULT_KEY", raising=False)
    monkeypatch.delenv("FALCON_VAULT_KEY_PREV", raising=False)
    # Prevent config/.env from silently re-enabling the vault during the test.
    monkeypatch.setattr(vault, "_load_env_file", lambda: None)
    yield


def _wipe_accounts():
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute("DELETE FROM broker_accounts")
        con.commit()


@pytest.fixture(autouse=True)
def _clean_vault():
    _wipe_accounts()
    yield
    _wipe_accounts()


# ── 1. encrypt/decrypt round-trip ─────────────────────────────────────────────

def test_vault_round_trip(test_key):
    assert vault.vault_enabled() is True
    pub = vault.put_account(
        user_id="u1", broker="zerodha", account_label="Main Kite",
        api_key="apikeyAAAA", api_secret="topsecretAAAA")
    bid = pub["broker_account_id"]
    # Public dict NEVER carries the secret/token in plaintext.
    assert "api_secret" not in pub
    assert "access_token" not in pub
    assert pub["api_key_masked"].startswith("apik")
    assert pub["has_secret"] is True
    assert pub["has_token"] is False
    assert pub["status"] == "PENDING"

    creds = vault.get_decrypted_creds(bid, user_id="u1")
    assert creds is not None
    assert creds.api_key == "apikeyAAAA"
    assert creds.api_secret == "topsecretAAAA"
    assert creds.access_token is None  # no token yet

    # Store a token and round-trip it.
    vault.store_access_token(bid, "fresh-access-token-XYZ", user_id="u1")
    creds2 = vault.get_decrypted_creds(bid, user_id="u1")
    assert creds2.access_token == "fresh-access-token-XYZ"
    assert creds2.status == "ACTIVE"  # token minted today


def test_vault_blob_is_actually_encrypted(test_key):
    pub = vault.put_account(
        user_id="u1", broker="zerodha", account_label="Main",
        api_key="ak", api_secret="PLAINTEXT_SHOULD_NOT_APPEAR")
    bid = pub["broker_account_id"]
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        row = con.execute(
            "SELECT api_secret_enc FROM broker_accounts WHERE broker_account_id=?",
            (bid,)).fetchone()
    blob = row["api_secret_enc"]
    assert blob is not None
    # The plaintext must NOT appear in the stored ciphertext.
    assert b"PLAINTEXT_SHOULD_NOT_APPEAR" not in (
        blob if isinstance(blob, (bytes, bytearray)) else blob.encode())


# ── 2. per-user isolation + multiple accounts ─────────────────────────────────

def test_multiple_accounts_per_user_and_isolation(test_key):
    a = vault.put_account(user_id="u1", broker="zerodha",
                          account_label="Main Kite", api_key="k1", api_secret="s1")
    b = vault.put_account(user_id="u1", broker="zerodha",
                          account_label="F&O Kite", api_key="k2", api_secret="s2")
    c = vault.put_account(user_id="u2", broker="upstox",
                          account_label="Upstox", api_key="k3", api_secret="s3")
    assert a["broker_account_id"] != b["broker_account_id"]

    u1 = vault.list_accounts("u1")
    assert {x["account_label"] for x in u1} == {"Main Kite", "F&O Kite"}
    u2 = vault.list_accounts("u2")
    assert {x["account_label"] for x in u2} == {"Upstox"}

    # Cross-user read is impossible: u2 cannot resolve u1's account.
    assert vault.get_decrypted_creds(a["broker_account_id"], user_id="u2") is None
    # Owner can.
    assert vault.get_decrypted_creds(a["broker_account_id"], user_id="u1") is not None


def test_unique_label_updates_in_place(test_key):
    a = vault.put_account(user_id="u1", broker="zerodha",
                          account_label="Main", api_key="k1", api_secret="s1")
    b = vault.put_account(user_id="u1", broker="zerodha",
                          account_label="Main", api_key="k1b", api_secret="s1b")
    assert a["broker_account_id"] == b["broker_account_id"]  # same row
    creds = vault.get_decrypted_creds(a["broker_account_id"], user_id="u1")
    assert creds.api_key == "k1b" and creds.api_secret == "s1b"


# ── 3. vault DISABLED == today (the safety hinge) ─────────────────────────────

def test_vault_disabled_is_today(vault_off):
    assert vault.vault_enabled() is False
    # Read paths NEVER raise; they return None → caller falls back to global.
    assert vault.get_decrypted_creds("anything", user_id="u1") is None
    assert vault.list_accounts("u1") == []
    # Write paths raise a clear, catchable error.
    with pytest.raises(vault.VaultDisabledError):
        vault.put_account(user_id="u1", broker="zerodha", account_label="x",
                          api_key="k", api_secret="s")


# ── 4. per-account ZerodhaBroker uses the RIGHT creds (no global state) ────────

def test_zerodha_per_account_client_uses_account_creds(test_key, monkeypatch):
    """A profile bound to an account builds its OWN KiteConnect from the account's
    api_key+access_token via _new_kite — NEVER the process-global client."""
    import services.kite_auth as ka

    built = {}

    class FakeKite:
        def __init__(self, api_key=None, **kw):
            self.api_key = api_key
            self.access_token = None

        def set_access_token(self, tok):
            self.access_token = tok

    def fake_new_kite(api_key):
        k = FakeKite(api_key=api_key)
        built["api_key"] = api_key
        return k

    def fake_global(check=False):
        raise AssertionError("global get_kite_client must NOT be called for a "
                             "bound account")

    monkeypatch.setattr(ka, "_new_kite", fake_new_kite)
    monkeypatch.setattr(ka, "get_kite_client", fake_global)

    pub = vault.put_account(user_id="u1", broker="zerodha",
                            account_label="Main", api_key="ACCOUNT_KEY",
                            api_secret="sek")
    bid = pub["broker_account_id"]
    vault.store_access_token(bid, "ACCOUNT_TOKEN", user_id="u1")
    creds = vault.get_decrypted_creds(bid, user_id="u1")

    prof = BrokerProfile(profile_id="p1", broker_name="zerodha",
                         broker_account_id=bid,
                         api_key=creds.api_key, access_token=creds.access_token)
    broker = ZerodhaBroker(prof, dry_run=True)
    kite = broker.kite
    assert built["api_key"] == "ACCOUNT_KEY"
    assert kite.api_key == "ACCOUNT_KEY"
    assert kite.access_token == "ACCOUNT_TOKEN"


def test_zerodha_no_account_uses_global(monkeypatch):
    """A profile with NO bound account uses the process-global get_kite_client —
    today's operator path, byte-for-byte. _new_kite must NOT be hit."""
    import services.kite_auth as ka
    calls = {"global": 0, "new": 0}

    def fake_global(check=False):
        calls["global"] += 1
        return object()

    def fake_new_kite(api_key):
        calls["new"] += 1
        return object()

    monkeypatch.setattr(ka, "get_kite_client", fake_global)
    monkeypatch.setattr(ka, "_new_kite", fake_new_kite)

    prof = BrokerProfile(profile_id="p1", broker_name="zerodha")  # no account
    broker = ZerodhaBroker(prof, dry_run=True)
    _ = broker.kite
    assert calls["global"] == 1
    assert calls["new"] == 0


def test_two_concurrent_accounts_isolate(test_key, monkeypatch):
    """Two brokers on DIFFERENT accounts each build their OWN client with their
    OWN token — no cross-contamination through shared global state."""
    import services.kite_auth as ka

    class FakeKite:
        def __init__(self, api_key=None, **kw):
            self.api_key = api_key
            self.access_token = None

        def set_access_token(self, tok):
            self.access_token = tok

    monkeypatch.setattr(ka, "_new_kite", lambda api_key: FakeKite(api_key=api_key))
    monkeypatch.setattr(ka, "get_kite_client",
                        lambda check=False: (_ for _ in ()).throw(
                            AssertionError("global must not be used")))

    a = vault.put_account(user_id="u1", broker="zerodha", account_label="A",
                          api_key="KEY_A", api_secret="sa")
    b = vault.put_account(user_id="u2", broker="zerodha", account_label="B",
                          api_key="KEY_B", api_secret="sb")
    vault.store_access_token(a["broker_account_id"], "TOK_A", user_id="u1")
    vault.store_access_token(b["broker_account_id"], "TOK_B", user_id="u2")

    ca = vault.get_decrypted_creds(a["broker_account_id"], user_id="u1")
    cb = vault.get_decrypted_creds(b["broker_account_id"], user_id="u2")

    ba = ZerodhaBroker(BrokerProfile(
        profile_id="pa", broker_name="zerodha",
        broker_account_id=a["broker_account_id"],
        api_key=ca.api_key, access_token=ca.access_token), dry_run=True)
    bb = ZerodhaBroker(BrokerProfile(
        profile_id="pb", broker_name="zerodha",
        broker_account_id=b["broker_account_id"],
        api_key=cb.api_key, access_token=cb.access_token), dry_run=True)

    ka_kite, kb_kite = ba.kite, bb.kite
    assert (ka_kite.api_key, ka_kite.access_token) == ("KEY_A", "TOK_A")
    assert (kb_kite.api_key, kb_kite.access_token) == ("KEY_B", "TOK_B")
    assert ka_kite is not kb_kite


def test_bound_account_without_token_refuses(test_key, monkeypatch):
    """A bound account with NO token must NOT silently fall back to the global
    operator token (that would trade the WRONG account). It raises."""
    import services.kite_auth as ka
    monkeypatch.setattr(ka, "get_kite_client",
                        lambda check=False: (_ for _ in ()).throw(
                            AssertionError("must not use global for bound acct")))
    prof = BrokerProfile(profile_id="p1", broker_name="zerodha",
                         broker_account_id="acct123",
                         api_key="K", access_token="")  # token missing
    broker = ZerodhaBroker(prof, dry_run=True)
    with pytest.raises(ValueError):
        _ = broker.kite


# ── 5. session-level scoping (create / resolve / regression) ──────────────────

def test_session_create_with_account_resolves_creds(test_key, monkeypatch,
                                                     clean_positions):
    from autotrade.session import TradingSession
    pub = vault.put_account(user_id="u1", broker="zerodha", account_label="Main",
                            api_key="SKEY", api_secret="ssec")
    bid = pub["broker_account_id"]
    vault.store_access_token(bid, "STOK", user_id="u1")

    cfg = TradingSessionConfig(total_allocated_capital=100000.0)
    sess = TradingSession.create(cfg, mode="paper", user_id="u1",
                                 broker_account_id=bid)
    assert sess.user_id == "u1"
    assert sess.broker_account_id == bid

    # Avoid building a real Kite client: stub the constructor.
    import services.kite_auth as ka

    class FakeKite:
        def __init__(self, api_key=None, **kw):
            self.api_key = api_key
            self.access_token = None

        def set_access_token(self, tok):
            self.access_token = tok

    monkeypatch.setattr(ka, "_new_kite", lambda api_key: FakeKite(api_key=api_key))

    sess._build_brokers()
    prof = sess.config.broker_profiles[0]
    assert prof.broker_account_id == bid
    assert prof.api_key == "SKEY"
    assert prof.access_token == "STOK"
    kite = sess.brokers[prof.profile_id].kite
    assert kite.api_key == "SKEY" and kite.access_token == "STOK"


def test_session_create_rejects_unknown_account(test_key, clean_positions):
    from autotrade.session import TradingSession
    cfg = TradingSessionConfig(total_allocated_capital=100000.0)
    with pytest.raises(ValueError):
        TradingSession.create(cfg, mode="paper", user_id="u1",
                              broker_account_id="does-not-exist")


def test_session_create_rejects_cross_user_account(test_key, clean_positions):
    from autotrade.session import TradingSession
    pub = vault.put_account(user_id="owner", broker="zerodha",
                            account_label="Main", api_key="k", api_secret="s")
    cfg = TradingSessionConfig(total_allocated_capital=100000.0)
    with pytest.raises(ValueError):
        TradingSession.create(cfg, mode="paper", user_id="intruder",
                              broker_account_id=pub["broker_account_id"])


# ── 6. REGRESSION (CRITICAL): NULL user/account == legacy path ────────────────

def test_null_account_session_is_legacy(test_key, monkeypatch, clean_positions):
    """A session with NO user_id/broker_account_id must build the GLOBAL client
    (today's operator path) even when the vault is ENABLED. The vault must not
    touch a NULL-account session at all."""
    from autotrade.session import TradingSession
    import services.kite_auth as ka

    calls = {"global": 0, "new": 0}
    monkeypatch.setattr(ka, "get_kite_client",
                        lambda check=False: calls.__setitem__("global",
                                                              calls["global"] + 1)
                        or object())
    monkeypatch.setattr(ka, "_new_kite",
                        lambda api_key: calls.__setitem__("new",
                                                          calls["new"] + 1)
                        or object())

    cfg = TradingSessionConfig(total_allocated_capital=100000.0)
    sess = TradingSession.create(cfg, mode="paper")  # no user/account
    assert sess.user_id is None and sess.broker_account_id is None
    sess._build_brokers()
    prof = sess.config.broker_profiles[0]
    assert prof.broker_account_id is None
    assert prof.api_key == ""  # vault never populated it
    # The client is lazy — accessing .kite triggers the GLOBAL path (not _new_kite).
    _ = sess.brokers[prof.profile_id].kite
    assert calls["global"] == 1
    assert calls["new"] == 0


def test_bound_account_but_vault_disabled_falls_back(vault_off, monkeypatch,
                                                     clean_positions):
    """Defense in depth: if a session is bound to an account but the vault is
    DISABLED, _resolve_account_creds clears the binding and the adapter uses the
    global path (rather than erroring) — surfaced via a warning."""
    from autotrade.session import TradingSession, TradingSessionConfig as _C
    import services.kite_auth as ka
    monkeypatch.setattr(ka, "get_kite_client", lambda check=False: object())
    monkeypatch.setattr(ka, "_new_kite",
                        lambda api_key: (_ for _ in ()).throw(
                            AssertionError("must not build per-account when "
                                           "vault disabled")))

    # Insert a session row directly with a bound account (vault off → create()'s
    # account_exists check would pass on the row's presence, but we bypass the
    # vault-disabled put by inserting straight into the session table).
    cfg = TradingSessionConfig(total_allocated_capital=100000.0)
    sess = TradingSession(session_id=uuid.uuid4().hex, config=cfg, mode="paper",
                          user_id="u1", broker_account_id="acctX")
    # persist a minimal session row so _set_status/etc. have a home
    from falcon.db import falcon_conn
    from datetime import datetime, timezone, timedelta
    IST = timezone(timedelta(hours=5, minutes=30))
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                config_json, user_id, broker_account_id)
               VALUES (?,?,?,?,?,?,?,?)""",
            (sess.session_id, datetime.now(IST).isoformat(), "CREATED", "paper",
             100000.0, cfg.to_json(), "u1", "acctX"))
        con.commit()
    sess._build_brokers()
    prof = sess.config.broker_profiles[0]
    # Binding cleared → global fallback.
    assert prof.broker_account_id is None


# ── 7. secrets never serialised through the config layer ──────────────────────

def test_broker_profile_public_dict_has_no_secrets():
    prof = BrokerProfile(profile_id="p1", broker_name="zerodha",
                         broker_account_id="acct1",
                         api_key="SECRET_KEY", api_secret="SECRET",
                         access_token="SECRET_TOKEN")
    d = prof.to_public_dict()
    assert "api_secret" not in d
    assert "access_token" not in d
    assert "api_key" not in d  # only creds_configured boolean is exposed
    assert d["broker_account_id"] == "acct1"  # the id is NOT a secret
    assert d["creds_configured"] is True
    # The same is true through a full config round-trip (config_json persistence).
    cfg = TradingSessionConfig(total_allocated_capital=100000.0,
                               broker_profiles=[prof])
    js = cfg.to_json()
    assert "SECRET" not in js
    assert "acct1" in js  # the binding id survives serialisation (not a secret)


def test_delete_account_scoped(test_key):
    a = vault.put_account(user_id="u1", broker="zerodha", account_label="A",
                          api_key="k", api_secret="s")
    bid = a["broker_account_id"]
    # Wrong user cannot delete.
    assert vault.delete_account(bid, user_id="u2") is False
    assert vault.account_exists(bid) is True
    # Owner can.
    assert vault.delete_account(bid, user_id="u1") is True
    assert vault.account_exists(bid) is False
