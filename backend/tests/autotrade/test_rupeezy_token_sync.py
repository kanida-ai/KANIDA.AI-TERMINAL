"""RTS — Rupeezy (Vortex) daily access-token SYNC (laptop → cloud) tests.

Covers the cloud INGEST endpoint POST /api/falcon/publish/rupeezy-token
(falcon/routers/publish_router.py::publish_rupeezy_token), the EXACT parallel of
the Kite token-sync but PER broker_account + vault-encrypted:

  AUTH (reused _require_secret):
    * no secret configured on the server            → 503 PUBLISH_NOT_CONFIGURED
    * wrong / missing X-Publish-Secret              → 403 FORBIDDEN
    * right secret                                  → 200 + stored
  VALIDATION:
    * empty access_token                            → 400 EMPTY_TOKEN
    * empty broker_account_id                       → 400 EMPTY_ACCOUNT_ID
    * unknown broker_account_id                     → 404 ACCOUNT_NOT_FOUND
    * a NON-rupeezy account                         → 400 NOT_A_RUPEEZY_ACCOUNT
  HAPPY PATH:
    * stores the token vault-encrypted on the account; the vault (get_decrypted_
      creds) AND the RupeezyBroker adapter then read it back for that account
    * idempotent: a re-POST REPLACES the stored token for that account
    * the token VALUE is never returned in the response body

All in the isolated temp DB from conftest (FALCON_DB_PATH). No real network, no
real Vortex login, no real orders. The vault is enabled with a per-test key.
"""
import uuid

import pytest
from fastapi import HTTPException

from falcon.routers import publish_router
from falcon.routers.publish_router import RupeezyTokenPush, publish_rupeezy_token
from autotrade import vault
from autotrade.broker.rupeezy import RupeezyBroker


_SECRET = "rts-test-secret-xyz"


@pytest.fixture
def vault_key(monkeypatch):
    """Enable the vault with a fresh per-test key (so store/decrypt round-trips)."""
    monkeypatch.setenv("FALCON_VAULT_KEY", vault.generate_key())
    monkeypatch.delenv("FALCON_VAULT_KEY_PREV", raising=False)
    return monkeypatch


@pytest.fixture
def publish_secret(monkeypatch):
    monkeypatch.setenv("FALCON_PUBLISH_SECRET", _SECRET)
    return monkeypatch


def _mk_rupeezy_account(label_prefix="Vortex") -> str:
    """A real rupeezy broker_accounts row (isolated temp DB). uuid-unique label so
    put_account never UPDATEs a prior test's row."""
    acct = vault.put_account(
        user_id="rts-user",
        broker="rupeezy",
        account_label=f"{label_prefix}-{uuid.uuid4().hex}",
        api_key="dev_appid_123",       # application_id
        api_secret="xapikey_secret_9",  # x-api-key
    )
    return acct["broker_account_id"]


# ── AUTH ─────────────────────────────────────────────────────────────────────

def test_no_secret_configured_returns_503(monkeypatch):
    monkeypatch.delenv("FALCON_PUBLISH_SECRET", raising=False)
    payload = RupeezyTokenPush(broker_account_id="whatever", access_token="tok")
    with pytest.raises(HTTPException) as ei:
        publish_rupeezy_token(payload, x_publish_secret="anything")
    assert ei.value.status_code == 503
    assert ei.value.detail["code"] == "PUBLISH_NOT_CONFIGURED"


def test_wrong_secret_returns_403(publish_secret):
    payload = RupeezyTokenPush(broker_account_id="whatever", access_token="tok")
    with pytest.raises(HTTPException) as ei:
        publish_rupeezy_token(payload, x_publish_secret="WRONG")
    assert ei.value.status_code == 403
    assert ei.value.detail["code"] == "FORBIDDEN"


def test_missing_secret_header_returns_403(publish_secret):
    payload = RupeezyTokenPush(broker_account_id="whatever", access_token="tok")
    with pytest.raises(HTTPException) as ei:
        publish_rupeezy_token(payload, x_publish_secret=None)
    assert ei.value.status_code == 403


# ── VALIDATION ───────────────────────────────────────────────────────────────

def test_empty_token_returns_400(publish_secret, vault_key):
    bid = _mk_rupeezy_account()
    payload = RupeezyTokenPush(broker_account_id=bid, access_token="   ")
    with pytest.raises(HTTPException) as ei:
        publish_rupeezy_token(payload, x_publish_secret=_SECRET)
    assert ei.value.status_code == 400
    assert ei.value.detail["code"] == "EMPTY_TOKEN"


def test_empty_account_id_returns_400(publish_secret, vault_key):
    payload = RupeezyTokenPush(broker_account_id="  ", access_token="tok")
    with pytest.raises(HTTPException) as ei:
        publish_rupeezy_token(payload, x_publish_secret=_SECRET)
    assert ei.value.status_code == 400
    assert ei.value.detail["code"] == "EMPTY_ACCOUNT_ID"


def test_unknown_account_returns_404(publish_secret, vault_key):
    payload = RupeezyTokenPush(broker_account_id="does-not-exist-" + uuid.uuid4().hex,
                               access_token="tok")
    with pytest.raises(HTTPException) as ei:
        publish_rupeezy_token(payload, x_publish_secret=_SECRET)
    assert ei.value.status_code == 404
    assert ei.value.detail["code"] == "ACCOUNT_NOT_FOUND"


def test_non_rupeezy_account_returns_400(publish_secret, vault_key):
    # A zerodha account must be refused — never cross-store a Rupeezy token.
    acct = vault.put_account(user_id="rts-user", broker="zerodha",
                             account_label=f"kite-{uuid.uuid4().hex}",
                             api_key="ak", api_secret="as")
    bid = acct["broker_account_id"]
    payload = RupeezyTokenPush(broker_account_id=bid, access_token="tok")
    with pytest.raises(HTTPException) as ei:
        publish_rupeezy_token(payload, x_publish_secret=_SECRET)
    assert ei.value.status_code == 400
    assert ei.value.detail["code"] == "NOT_A_RUPEEZY_ACCOUNT"


# ── HAPPY PATH ───────────────────────────────────────────────────────────────

def test_happy_path_stores_and_reads_back(publish_secret, vault_key):
    bid = _mk_rupeezy_account()
    token = "eyJhbGciOi.rupeezy.jwt.token.value.ABC123"
    payload = RupeezyTokenPush(broker_account_id=bid, access_token=token,
                               token_date="2026-07-19")

    resp = publish_rupeezy_token(payload, x_publish_secret=_SECRET)
    assert resp["ok"] is True
    assert resp["broker_account_id"] == bid
    assert resp["token_date"] == "2026-07-19"
    assert resp["stored"] is True
    # The token VALUE is NEVER echoed back.
    assert token not in str(resp)

    # The vault decrypts it back for THIS account.
    creds = vault.get_decrypted_creds(bid)
    assert creds is not None
    assert creds.access_token == token
    assert creds.token_date == "2026-07-19"
    # The store stamps the raw status column ACTIVE (get_decrypted_creds re-derives
    # status vs today, which is clock-relative — assert the stored column directly).
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        raw_status = con.execute(
            "SELECT status FROM broker_accounts WHERE broker_account_id=?",
            (bid,)).fetchone()[0]
    assert raw_status == "ACTIVE"

    # And the RupeezyBroker adapter (built from that profile) serves it — this is
    # the exact path the live order code reads Authorization: Bearer <token> from.
    broker = RupeezyBroker(profile=creds, dry_run=True)
    assert broker._access_token() == token


def test_idempotent_replace(publish_secret, vault_key):
    bid = _mk_rupeezy_account()
    first = "first-token-AAA"
    second = "second-token-BBB"

    publish_rupeezy_token(
        RupeezyTokenPush(broker_account_id=bid, access_token=first),
        x_publish_secret=_SECRET)
    assert vault.get_decrypted_creds(bid).access_token == first

    # Re-POST replaces the stored token for that account.
    publish_rupeezy_token(
        RupeezyTokenPush(broker_account_id=bid, access_token=second),
        x_publish_secret=_SECRET)
    assert vault.get_decrypted_creds(bid).access_token == second


def test_vault_disabled_returns_503(publish_secret, monkeypatch):
    # An account created under a key, then the key removed → the write path must
    # fail CLOSED (never store a live-order token in plaintext).
    monkeypatch.setenv("FALCON_VAULT_KEY", vault.generate_key())
    monkeypatch.delenv("FALCON_VAULT_KEY_PREV", raising=False)
    bid = _mk_rupeezy_account()
    # Now disable the vault for the ingest write.
    monkeypatch.delenv("FALCON_VAULT_KEY", raising=False)
    monkeypatch.setattr(vault, "_load_env_file", lambda: None)
    payload = RupeezyTokenPush(broker_account_id=bid, access_token="tok")
    with pytest.raises(HTTPException) as ei:
        publish_rupeezy_token(payload, x_publish_secret=_SECRET)
    # Either the account is unreadable (get_account_public → None under a disabled
    # vault still returns the public row since it reads plaintext columns), so the
    # store step is what fails closed → 503 VAULT_DISABLED.
    assert ei.value.status_code == 503
    assert ei.value.detail["code"] == "VAULT_DISABLED"
