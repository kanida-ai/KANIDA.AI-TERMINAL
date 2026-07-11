"""CLUSTER 3 ITEM 5 — PROFILE-LEVEL account health gate before fire.

Session-level account validation is not enough: a multi-profile session can pair
an ACTIVE broker account with a REVOKED / EXPIRED one, and the revoked leg would
still fire. Every ENABLED profile's bound account must be validated before
placing; a non-tradeable profile is DEGRADED (not fired) with a clear reason.

MUTATION (verified): delete the CLUSTER-3-ITEM-5 profile-gate block in
session._fire_entries → the revoked profile stays enabled → its leg (B) is routed
and placed → a position for B appears under the revoked profile →
`_pos(... "B") is None` fails (and no degraded_profiles surfaces).
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.vault as vault_mod
import autotrade.broker.account_lifecycle as acct_mod
from autotrade.broker.account_lifecycle import AccountNotTradeable
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.session import TradingSession, set_fake_now
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)

ACTIVE_ACCT = "acct-active"
REVOKED_ACCT = "acct-revoked"


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _patch(monkeypatch):
    shared_ltps = {"A": 100.0, "B": 100.0}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps=shared_ltps)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    # Vault ON so the profile gate engages; creds resolve to None → the adapter
    # takes the (paper) global path cleanly.
    monkeypatch.setattr(vault_mod, "vault_enabled", lambda: True)
    monkeypatch.setattr(vault_mod, "get_decrypted_creds",
                        lambda *a, **k: None)

    def fake_assert(user_id, broker_account_id):
        if broker_account_id == REVOKED_ACCT:
            raise AccountNotTradeable(
                f"broker_account {broker_account_id} is EXPIRED — reconnect")
        return None

    monkeypatch.setattr(acct_mod, "assert_account_tradeable", fake_assert)


def _pos(session_id, sym):
    with falcon_conn() as con:
        r = con.execute("SELECT status FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?",
                        (session_id, sym)).fetchone()
    return r["status"] if r else None


def _mk_session():
    profiles = [
        BrokerProfile(profile_id="active", broker_name="zerodha",
                      allocated_capital=150000.0, symbols=["A"],
                      order_product="CNC", broker_account_id=ACTIVE_ACCT),
        BrokerProfile(profile_id="revoked", broker_name="zerodha",
                      allocated_capital=150000.0, symbols=["B"],
                      order_product="CNC", broker_account_id=REVOKED_ACCT),
    ]
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC", broker_profiles=profiles)
    return TradingSession.create(cfg, mode="paper")


def test_revoked_profile_leg_not_fired(clean_positions, monkeypatch):
    _patch(monkeypatch)
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 100.0)])
    sess = _mk_session()

    res = asyncio.run(sess.start(when="now"))

    # The ACTIVE leg fired; the REVOKED leg did NOT.
    assert _pos(sess.session_id, "A") == "OPEN"
    assert _pos(sess.session_id, "B") is None

    # A clear reason surfaces for the degraded profile.
    degraded = res.get("degraded_profiles") or []
    assert any(d.get("broker_account_id") == REVOKED_ACCT for d in degraded), res
    # The revoked profile object was degraded (disabled) in memory.
    revoked_prof = next(p for p in sess.config.broker_profiles
                        if p.profile_id == "revoked")
    assert revoked_prof.enabled is False


def test_all_profiles_revoked_refuses_fire(clean_positions, monkeypatch):
    """When NO enabled profile has a tradeable account the whole fire is refused."""
    _patch(monkeypatch)
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 100.0)])
    profiles = [
        BrokerProfile(profile_id="r1", broker_name="zerodha",
                      allocated_capital=150000.0, symbols=["A"],
                      order_product="CNC", broker_account_id=REVOKED_ACCT),
        BrokerProfile(profile_id="r2", broker_name="zerodha",
                      allocated_capital=150000.0, symbols=["B"],
                      order_product="CNC", broker_account_id=REVOKED_ACCT),
    ]
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC", broker_profiles=profiles)
    sess = TradingSession.create(cfg, mode="paper")

    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "FAILED"
    assert _pos(sess.session_id, "A") is None
    assert _pos(sess.session_id, "B") is None
