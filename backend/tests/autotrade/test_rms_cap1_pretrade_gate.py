"""RMS CAP 1 — pre-trade margin gate + per-user committed-capital ledger.

Proves:
  * committed_capital() sums a user's live sessions' allocated capital.
  * pre_trade_gate() refuses when planned deploy > free (available margin minus
    the user's already-committed capital), and is INERT when the broker can't
    report available margin (paper / stub → None) so paper is byte-identical.
  * A live-ish session whose planned deploy exceeds free margin FAILS and places
    NOTHING (integration through _fire_entries).

MUTATION-VERIFIED (test_gate_refuses_over_deploy_places_nothing):
  Revert = delete the `if leg_specs: … risk_manager.pre_trade_gate(…)` refusal
  block in session._fire_entries (let the over-deploy proceed). Then the session
  goes RUNNING and places 3 legs → this test's assert n_placed==0 / status FAILED
  fails.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade import risk_manager
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.session import TradingSession, set_fake_now
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _frozen_open_clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _make_session_row(sid, cap, status="RUNNING", user_id=None, config_json="{}"):
    from falcon.db import falcon_conn
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                config_json, user_id)
               VALUES (?,?,?,?,?,?,?)""",
            (sid, "2026-06-24T09:00:00", status, "live", cap, config_json,
             user_id))
        con.commit()


# ── committed-capital ledger ─────────────────────────────────────────────────

def test_committed_capital_sums_live_sessions(clean_positions):
    _make_session_row("s1", 500000.0, status="RUNNING", user_id="u1")
    _make_session_row("s2", 300000.0, status="SCHEDULED", user_id="u1")
    _make_session_row("s3", 900000.0, status="CLOSED", user_id="u1")   # not active
    _make_session_row("s4", 111111.0, status="RUNNING", user_id="u2")  # other user
    assert risk_manager.committed_capital("u1") == pytest.approx(800000.0)
    # excluding one session drops it from the sum.
    assert risk_manager.committed_capital(
        "u1", exclude_session_id="s1") == pytest.approx(300000.0)
    assert risk_manager.committed_capital("u2") == pytest.approx(111111.0)


# ── pre_trade_gate: unknown budget (paper) is inert ──────────────────────────

def test_gate_inert_when_margin_unknown(clean_positions):
    broker = MockBroker(profile=BrokerProfile("p", "mock"))  # available_margin None
    dec = risk_manager.pre_trade_gate(
        user_id="u1", session_id="new", planned_deployed=10_000_000.0,
        brokers={"p": broker})
    assert dec.allow is True
    assert dec.available_margin is None


# ── pre_trade_gate: funds < required → refuse (proof 1) ──────────────────────

def test_gate_refuses_when_funds_below_required(clean_positions):
    broker = MockBroker(profile=BrokerProfile("p", "mock"),
                        available_margin=100000.0)
    dec = risk_manager.pre_trade_gate(
        user_id="u1", session_id="new", planned_deployed=300000.0,
        brokers={"p": broker})
    assert dec.allow is False
    assert dec.free == pytest.approx(100000.0)
    assert "exceeds free margin" in dec.reason


# ── pre_trade_gate: 2nd session exceeds remaining ledger free (proof 2) ──────

def test_gate_refuses_second_session_over_ledger(clean_positions):
    # Session A already RUNNING committed ₹5L; the account shows ₹5L free.
    _make_session_row("A", 500000.0, status="RUNNING", user_id="u1")
    broker = MockBroker(profile=BrokerProfile("p", "mock"),
                        available_margin=500000.0)
    # Session B (new) wants ₹5L → free = 5L available − 5L committed = 0 → refuse.
    dec = risk_manager.pre_trade_gate(
        user_id="u1", session_id="B", planned_deployed=500000.0,
        brokers={"p": broker})
    assert dec.allow is False
    assert dec.committed_other == pytest.approx(500000.0)
    assert dec.free == pytest.approx(0.0)


def test_gate_allows_when_within_free(clean_positions):
    _make_session_row("A", 200000.0, status="RUNNING", user_id="u1")
    broker = MockBroker(profile=BrokerProfile("p", "mock"),
                        available_margin=500000.0)
    dec = risk_manager.pre_trade_gate(
        user_id="u1", session_id="B", planned_deployed=250000.0,
        brokers={"p": broker})
    assert dec.allow is True   # 250k <= 500k-200k=300k free


# ── integration: over-deploy is REFUSED at fire → places nothing (MUTATION) ──

@pytest.fixture
def patched_thin_margin(monkeypatch):
    """build_client → MockBrokers reporting only ₹100k free margin, so a ₹300k
    session's planned deploy exceeds free and the pre-trade gate refuses."""
    created = {}
    ltps = {"A": 100.0, "B": 200.0, "C": 50.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=ltps,
                        available_margin=100000.0)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def test_gate_refuses_over_deploy_places_nothing(clean_positions,
                                                 patched_thin_margin):
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper", user_id="u1")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "FAILED"
    assert res["n_placed"] == 0
    assert res.get("risk_refused") is True
    # NOTHING was placed at the broker.
    for mb in patched_thin_margin.values():
        assert mb.placed == []
    assert sess.status()["n_open_positions"] == 0


def test_gate_allows_when_margin_sufficient(clean_positions, monkeypatch):
    """Same session but ample margin → fires normally (gate does not over-refuse)."""
    ltps = {"A": 100.0, "B": 200.0, "C": 50.0}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps=ltps,
                          available_margin=10_000_000.0)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper", user_id="u1")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 3
