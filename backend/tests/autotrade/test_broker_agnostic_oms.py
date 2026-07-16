"""BROKER-AGNOSTIC OMS/EMS locks (2026-07-15 Rupeezy BTST incident).

The OMS/EMS must behave identically for EVERY broker (zerodha/rupeezy/upstox/
angel/dhan/fyers) — certification is a per-broker GATE, never a broker-specific
code path. These tests lock:

  1. A campaign bound to a Rupeezy account builds the RUPEEZY adapter (not
     zerodha) in BOTH the preview and the live-session build.
  2. A split-entry (Magnifier) whose adapter BLOCKS every leg → session FAILED +
     urgent alert, NOT mag_entry_complete, NOT RUNNING; the second leg is NOT
     scheduled (result-driven, broker-agnostic — the incident's exact failure).
  3. A split-entry (BTST) with leg-1 all-blocked → same FAIL, no leg-2.
  4. PARTIAL placement (some legs block, some fill) → the placed legs are KEPT +
     a LOUD partial alert (naked/partial), session still RUNNING.
  5. The broker-neutral 'default' profile label does NOT break reconciler
     matching (prof_scope = list(brokers.keys()) == the stored broker_profile).

Paper / dry-run throughout (patched MockBrokers + a real Fernet vault key for the
adapter-binding test). NO real network, NO real orders.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade import alerts as alerts_mod
from autotrade import vault
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.session import TradingSession, set_fake_now
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _frozen_open_clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


@pytest.fixture
def captured_alerts(monkeypatch):
    """Capture every urgent-deduped page the session fires (broker-agnostic wire)."""
    seen = []

    def _cap(*, kind, session_id, symbol, detail, **kw):
        seen.append({"kind": kind, "session_id": session_id,
                     "symbol": symbol, "detail": detail})
        return 1
    monkeypatch.setattr(alerts_mod, "send_urgent_deduped", _cap)
    return seen


@pytest.fixture
def all_high_tier(monkeypatch):
    def _all(picks, high_tier):
        return list(picks), {p.symbol: "GOLD" for p in picks}
    monkeypatch.setattr(sess_mod, "_magnifier_high_tier_filter", _all)


@pytest.fixture
def no_second_leg_timer(monkeypatch):
    """Record (instead of arm) the second-leg timers so a test can assert leg-2 was
    NOT scheduled after a leg-1 zero-placement FAIL."""
    calls = {"magnifier": 0, "btst": 0}
    monkeypatch.setattr(sess_mod, "_schedule_magnifier_second_leg",
                        lambda *a, **k: calls.__setitem__("magnifier",
                                                          calls["magnifier"] + 1))
    monkeypatch.setattr(sess_mod, "_schedule_btst_second_leg",
                        lambda *a, **k: calls.__setitem__("btst",
                                                          calls["btst"] + 1))
    return calls


def _brokers(monkeypatch, **mb_kwargs):
    """Patch build_client to hand back MockBrokers with the given kwargs."""
    created = {}
    ltps = {"A": 100.0, "B": 200.0, "C": 50.0}
    margins = {"A": 20.0, "B": 40.0, "C": 10.0}   # 5× MIS (margin = price / 5)

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=dict(ltps),
                        margins=dict(margins), margins_available=True, **mb_kwargs)
        created[profile.profile_id] = mb
        return mb
    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _mag_cfg():
    return TradingSessionConfig(
        total_allocated_capital=300000.0, strategy="intraday_magnifier",
        order_product="MIS", instrument_type="EQ", direction="long",
        top_n_stocks=15, sizing_mode="equal",
        arm_pct=0.06, floor_pct=0.02, trail_giveback_pct=0.05, stop_pct=0.03,
        magnifier_second_leg_offset_sec=600)


def _btst_cfg():
    return TradingSessionConfig(
        total_allocated_capital=300000.0, strategy="btst_oscillator",
        order_product="CNC", instrument_type="EQ", direction="long",
        top_n_stocks=15, sizing_mode="equal",
        arm_pct=0.5, floor_pct=0.01, trail_giveback_pct=0.04, stop_pct=0.06,
        square_off_enabled=False, max_hold_sessions=2,
        trail_step_lock_enabled=False,
        magnifier_second_leg_offset_sec=600)


def _positions(session_id):
    with falcon_conn() as con:
        return {r["symbol"]: dict(r) for r in con.execute(
            "SELECT * FROM autotrade_positions WHERE session_id=?",
            (session_id,)).fetchall()}


def _mag_flag(session_id):
    with falcon_conn() as con:
        r = con.execute(
            "SELECT mag_entry_complete FROM autotrade_sessions WHERE session_id=?",
            (session_id,)).fetchone()
    return r["mag_entry_complete"] if r else None


# ── vault helper ──────────────────────────────────────────────────────────────

@pytest.fixture
def vault_key(monkeypatch):
    key = vault.generate_key()
    monkeypatch.setenv("FALCON_VAULT_KEY", key)
    monkeypatch.delenv("FALCON_VAULT_KEY_PREV", raising=False)
    assert vault.vault_enabled()
    yield key


@pytest.fixture
def wipe_accounts():
    with falcon_conn() as con:
        con.execute("DELETE FROM broker_accounts")
        con.commit()
    yield
    with falcon_conn() as con:
        con.execute("DELETE FROM broker_accounts")
        con.commit()


# ── 1. A Rupeezy-bound campaign builds the RUPEEZY adapter (preview + live) ──────

def test_rupeezy_account_builds_rupeezy_adapter_preview_and_live(vault_key,
                                                                 wipe_accounts):
    from autotrade.broker.rupeezy import RupeezyBroker
    from autotrade.broker.router import build_client
    from autotrade.session import _preview_resolve_creds

    pub = vault.put_account("u9", "rupeezy", "Vortex", "appid", "xkey")
    bid = pub["broker_account_id"]
    vault.store_tokens(bid, "rz-token", user_id="u9")

    # PREVIEW build: the default profile is hardcoded zerodha; cred resolution must
    # rebind it to the account's broker BEFORE the adapter is built.
    prof = BrokerProfile(profile_id="default", broker_name="zerodha",
                         allocated_capital=100000.0, order_product="MIS",
                         instrument_type="EQ", broker_account_id=bid)
    _preview_resolve_creds(prof, user_id="u9")
    assert prof.broker_name == "rupeezy"
    preview_client = build_client(prof, dry_run=True)
    assert isinstance(preview_client, RupeezyBroker)
    assert preview_client.broker_name == "rupeezy"

    # LIVE build: _build_brokers resolves creds then build_client → same adapter.
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=5,
                               order_product="MIS", instrument_type="EQ")
    sess = TradingSession.create(cfg, mode="paper", user_id="u9",
                                 broker_account_id=bid)
    sess._build_brokers()
    built = sess.brokers["default"]
    assert isinstance(built, RupeezyBroker)
    assert built.broker_name == "rupeezy"
    # And NOT a Zerodha adapter anywhere.
    from autotrade.broker.zerodha import ZerodhaBroker
    assert not isinstance(built, ZerodhaBroker)


# ── 2. Split-entry (Magnifier): every leg blocked → FAILED + alert, no leg-2 ─────

def test_magnifier_all_legs_blocked_fails_loud_no_leg2(clean_positions, monkeypatch,
                                                       all_high_tier, captured_alerts,
                                                       no_second_leg_timer):
    _brokers(monkeypatch, block_orders="UNCERTIFIED adapter — cert block")
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = TradingSession.create(_mag_cfg(), mode="paper")
    res = asyncio.run(sess.start(when="now"))

    assert res["status"] == "FAILED"
    assert res["n_placed"] == 0
    assert "0/" in res["reason"] and "cert block" in res["reason"]
    # NOT complete, NOT running, holding nothing.
    assert _mag_flag(sess.session_id) == 0
    assert sess.status()["status"] == "FAILED"
    assert _positions(sess.session_id) == {}
    # The second leg was NEVER scheduled.
    assert no_second_leg_timer["magnifier"] == 0
    # A LOUD zero-placement page fired.
    kinds = [a["kind"] for a in captured_alerts]
    assert "ENTRY_ZERO_PLACEMENT" in kinds


# ── 3. Split-entry (BTST): leg-1 all-blocked → FAILED, no leg-2 ─────────────────

def test_btst_all_legs_blocked_fails_loud_no_leg2(clean_positions, monkeypatch,
                                                  all_high_tier, captured_alerts,
                                                  no_second_leg_timer):
    _brokers(monkeypatch, block_orders="RUPEEZY_LIVE_CERTIFIED unset — hard block")
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = TradingSession.create(_btst_cfg(), mode="paper")
    res = asyncio.run(sess.start(when="now"))

    assert res["status"] == "FAILED"
    assert res["strategy"] == "btst_oscillator"
    assert res["n_placed"] == 0
    assert _mag_flag(sess.session_id) == 0
    assert sess.status()["status"] == "FAILED"
    assert _positions(sess.session_id) == {}
    assert no_second_leg_timer["btst"] == 0
    assert "ENTRY_ZERO_PLACEMENT" in [a["kind"] for a in captured_alerts]


# ── 4. PARTIAL placement: placed legs KEPT + LOUD partial alert ─────────────────

def test_magnifier_partial_leg1_keeps_placed_and_pages(clean_positions, monkeypatch,
                                                       all_high_tier,
                                                       captured_alerts,
                                                       no_second_leg_timer):
    # A is blocked; B + C fill → 2/3 legs placed → partial (not a full FAIL).
    _brokers(monkeypatch, block_symbols={"A"})
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    sess = TradingSession.create(_mag_cfg(), mode="paper")
    res = asyncio.run(sess.start(when="now"))

    # Session lives (some legs placed), still stage-1 (leg-2 scheduled for B/C).
    assert res["status"] == "RUNNING"
    assert res["magnifier_entry_complete"] is False
    pos = _positions(sess.session_id)
    assert "A" not in pos          # the blocked leg was NOT registered (no phantom)
    assert "B" in pos and "C" in pos
    # The placed legs are kept AND the second leg IS scheduled for them.
    assert no_second_leg_timer["magnifier"] == 1
    # A LOUD partial page fired (naked/partial), NOT a zero-placement fail.
    kinds = [a["kind"] for a in captured_alerts]
    assert "ENTRY_PARTIAL" in kinds
    assert "ENTRY_ZERO_PLACEMENT" not in kinds
    assert sess.status()["status"] == "RUNNING"


# ── 5. The broker-neutral 'default' label does not break reconciler matching ────

def test_default_profile_label_is_broker_neutral_and_reconciler_matches(
        clean_positions, monkeypatch):
    _brokers(monkeypatch)
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    cfg = TradingSessionConfig(
        total_allocated_capital=300000.0, strategy="portfolio_kill_switch",
        order_product="MIS", instrument_type="EQ", top_n_stocks=3,
        sizing_mode="equal", kill_switch_enabled=True, kill_switch_pct=0.01)
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess.start(when="now"))

    # The default profile id is broker-NEUTRAL (no 'zerodha' in it).
    assert list(sess.brokers.keys()) == ["default"]
    pos = _positions(sess.session_id)
    assert pos and all(p["broker_profile"] == "default" for p in pos.values())
    assert not any("zerodha" in p["broker_profile"] for p in pos.values())

    # The reconciler scopes by prof_scope = list(brokers.keys()); it MATCHES the
    # stored broker_profile (write-side == read-side), and the OLD literal does NOT.
    from autotrade.monitoring.position_reconciler import _account_open_positions_for
    prof_scope = list(sess.brokers.keys())
    cache = {}
    found = _account_open_positions_for(
        "A", "MIS", prof_scope, cache, acct_scope=[None])
    assert [p["symbol"] for p in found] == ["A"]
    # Nothing matches the retired literal (proves nothing keys off it).
    none_found = _account_open_positions_for(
        "A", "MIS", ["zerodha_default"], {}, acct_scope=[None])
    assert none_found == []
