"""SPRINT CLUSTER 9c — the 6 code findings from a 4th audit (the symmetric edges
of C9/C9b).

Multi-user / multi-account safe, paper byte-identical, single-profile byte-
identical, real-money-safe. Every test is MUTATION-VERIFIED: it PASSES with the
fix and FAILS on the stated revert. All paper-safe (MockBroker, no real orders);
clock frozen to a mid-session NSE trading day so no square-off path interferes.

  F1 — mark_exit_failed RELEASES the exit lock scoped by broker_profile (so a
       sibling profile's in-flight exit lock is NOT cleared).
  F2 — recovery ADOPTS a partial-fill-then-CANCELLED/REJECTED orphan entry (real
       shares), not only a COMPLETE one.
  F3 — the RMS pre_trade_gate FAILS CLOSED in LIVE when it RAISES (paper inert).
  F4 — the RMS gate runs ONE decision PER broker-account group (over-budget acct
       caught); single-account byte-identical.
  F5 — a FOREIGN same-side pending order is detected: REFUSE at entry, PAGE at exit.
  F6 — the ladder config edit real UPDATE is conditional on config_version (409 on
       a stale edit; no clobber).
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade import order_ledger, recovery, risk_manager, alerts, exit_gate
from autotrade.api import config_edit_routes as cfgapi
from autotrade.api.autotrade_routes import Caller
from autotrade.config import BrokerProfile, TradingSessionConfig
from autotrade.ladder import LadderCampaign
from autotrade.monitoring.registry import PositionRegistry
from autotrade.session import (TradingSession, set_fake_now,
                               _exit_single_position, _foreign_same_side_pending)
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)
ADMIN = Caller(user_id="tester", is_admin=True, authenticated=True)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


@pytest.fixture(autouse=True)
def _clear_ledger(clean_positions):
    """Wipe the append-only ledger / config-edit audit between tests (their UNIQUE
    keys are not session-scoped)."""
    with falcon_conn() as con:
        con.execute("DELETE FROM autotrade_order_events")
        con.execute("DELETE FROM autotrade_config_edits")
        con.commit()
    yield


def _pos_row(session_id, symbol, broker_profile=None):
    with falcon_conn() as con:
        if broker_profile is not None:
            r = con.execute(
                "SELECT status, qty, avg_price FROM autotrade_positions "
                "WHERE session_id=? AND symbol=? AND "
                "COALESCE(broker_profile,'')=COALESCE(?,'')",
                (session_id, symbol, broker_profile)).fetchone()
        else:
            r = con.execute(
                "SELECT status, qty, avg_price FROM autotrade_positions "
                "WHERE session_id=? AND symbol=?", (session_id, symbol)).fetchone()
    return dict(r) if r else None


# ═══════════════════════════════════════════════════════════════════════════
# F1 — mark_exit_failed releases the exit lock SCOPED by broker_profile.
# ═══════════════════════════════════════════════════════════════════════════
def test_f1_mark_exit_failed_release_scoped_by_profile(clean_positions):
    """One session holds the SAME symbol on TWO profiles. p2's exit is IN FLIGHT
    (its lock held). p1's exit FAILS → mark_exit_failed must release ONLY p1's lock;
    p2's lock MUST remain held.

    Revert: change registry.mark_exit_failed's release to
    `release_exit_session(self.session_id, symbol)` (drop broker_profile) → the
    symbol-wide release clears p2's lock too → `is_locked p2 is True` FAILS.
    """
    sid = "sess-f1"
    reg = PositionRegistry(sid, 1_000_000.0)
    for prof in ("p1", "p2"):
        reg.register(symbol="X", broker_profile=prof, qty=100, avg_price=100.0,
                     product="CNC", instrument_type="EQ", exchange="NSE",
                     direction="long")
    # p2's exit is IN FLIGHT — hold its lock.
    assert exit_gate.claim_exit_session(sid, "X", "STOP_STOCK",
                                        broker_profile="p2") is True
    assert exit_gate.is_locked_session(sid, "X", broker_profile="p2") is True

    # p1's exit fails → releases ONLY p1's lock.
    reg.mark_exit_failed("X", "boom", broker_profile="p1")

    # p2's lock survives (its exit is still in flight — must not be re-claimable).
    assert exit_gate.is_locked_session(sid, "X", broker_profile="p2") is True
    # p1 released for retry (it was never locked → still 0).
    assert exit_gate.is_locked_session(sid, "X", broker_profile="p1") is False


# ═══════════════════════════════════════════════════════════════════════════
# F2 — recovery adopts a partial-fill-then-terminal (CANCELLED/REJECTED) orphan.
# ═══════════════════════════════════════════════════════════════════════════
def _live_session_with_intent(monkeypatch, *, mb, submitted=True,
                              broker_oid="BOID1", coid="FAL-ORPHAN-1",
                              qty=100, price=100.0, symbol="X",
                              prof="zerodha_default"):
    def fake_build_client(profile, dry_run=True):
        return mb
    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC", per_position_gtt_enabled=False)
    sess = TradingSession.create(cfg, mode="live")
    sid = sess.session_id
    order_ledger.record_intent(session_id=sid, symbol=symbol, client_order_id=coid,
                               qty=qty, side="BUY", product="CNC",
                               broker_profile=prof, instrument_type="EQ",
                               source="entry")
    if submitted:
        order_ledger.append_event(
            session_id=sid, symbol=symbol,
            event_type=order_ledger.EV_ORDER_SUBMITTED,
            broker_order_id=broker_oid, client_order_id=coid, qty=qty,
            price=price, product="CNC", broker_profile=prof, source="entry")
    return sid, coid


@pytest.mark.parametrize("terminal_status", ["CANCELLED", "REJECTED"])
def test_f2_adopts_partial_fill_terminal_orphan(clean_positions, monkeypatch,
                                                terminal_status):
    """A SUBMITTED entry that partially filled (6 shares @ a real avg) then went
    CANCELLED / REJECTED at the broker is a REAL unmanaged position → recovery must
    ADOPT the 6-share fill (not page 'nothing to adopt').

    Revert: restrict the adopt guard to `status == "COMPLETE"` only → the
    partial-terminal orphan stays unregistered → the 6-share position-row assert
    FAILS.
    """
    prof_obj = type("P", (), {"profile_id": "zerodha_default",
                              "broker_name": "mock"})()
    mb = MockBroker(profile=prof_obj, dry_run=False,
                    order_status={"BOID1": {"status": terminal_status,
                                            "filled_quantity": 6,
                                            "average_price": 100.5}})
    sid, coid = _live_session_with_intent(monkeypatch, mb=mb)

    actions = recovery._adopt_orphan_entry_intents(sid)

    assert any(a["outcome"] == "adopted_filled" for a in actions)
    row = _pos_row(sid, "X", "zerodha_default")
    assert row is not None and row["status"] == "OPEN"
    assert row["qty"] == 6                          # the REAL partial fill
    assert row["avg_price"] == pytest.approx(100.5)


def test_f2_zero_fill_terminal_still_paged_no_phantom(clean_positions, monkeypatch):
    """The no-fire complement: a CANCELLED orphan with ZERO fill (created-never-
    accepted / zero-fill reject) registers NO position and is paged — unchanged."""
    pages = []
    monkeypatch.setattr(alerts, "send_urgent_deduped",
                        lambda **kw: pages.append(kw))
    prof_obj = type("P", (), {"profile_id": "zerodha_default",
                              "broker_name": "mock"})()
    mb = MockBroker(profile=prof_obj, dry_run=False,
                    order_status={"BOID1": {"status": "CANCELLED",
                                            "filled_quantity": 0,
                                            "average_price": 0.0}})
    sid, coid = _live_session_with_intent(monkeypatch, mb=mb)

    actions = recovery._adopt_orphan_entry_intents(sid)

    assert _pos_row(sid, "X") is None               # NO phantom position
    assert any(a["outcome"] == "paged_rejected" for a in actions)
    assert pages


# ═══════════════════════════════════════════════════════════════════════════
# F3 — RMS pre_trade_gate EXCEPTION fails CLOSED in LIVE (inert in paper).
# ═══════════════════════════════════════════════════════════════════════════
def _boom_gate(**kw):
    raise RuntimeError("gate exploded")


def test_f3_live_gate_exception_fails_closed(clean_positions, monkeypatch):
    """LIVE + pre_trade_gate RAISES → the session is REFUSED (FAILED, risk_refused,
    reason RMS_GATE_ERROR) and places NOTHING.

    Revert: restore proceed-on-exception (`_rms = None; continue`) → the fire
    proceeds and places legs → the `status == FAILED` / `n_placed == 0` asserts FAIL.
    """
    created = {}
    ltps = {"A": 100.0, "B": 200.0, "C": 50.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=ltps,
                        available_margin=10_000_000.0)
        mb._live_allowed = lambda: True         # engage the RMS "live" path
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    monkeypatch.setattr(risk_manager, "pre_trade_gate", _boom_gate)
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0), ("C", 3, 7.0, 50.0)])
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="live", user_id="u1")

    res = asyncio.run(sess.start(when="now"))

    assert res["status"] == "FAILED"
    assert res["n_placed"] == 0
    assert res.get("risk_refused") is True
    assert "RMS_GATE_ERROR" in (res.get("reason") or "")
    for mb in created.values():
        assert mb.placed == []                  # NOTHING placed


def test_f3_paper_gate_exception_inert_proceeds(clean_positions, monkeypatch):
    """The complement: PAPER + pre_trade_gate RAISES → INERT (proceeds; the fire
    places its paper legs). A gate bug never fails a paper session."""
    ltps = {"A": 100.0, "B": 200.0, "C": 50.0}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps=ltps,
                          available_margin=10_000_000.0)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    monkeypatch.setattr(risk_manager, "pre_trade_gate", _boom_gate)
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0), ("C", 3, 7.0, 50.0)])
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper", user_id="u1")

    res = asyncio.run(sess.start(when="now"))

    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 3
    assert res.get("risk_refused") is not True


# ═══════════════════════════════════════════════════════════════════════════
# F4 — RMS gate runs ONE decision PER broker-account group.
# ═══════════════════════════════════════════════════════════════════════════
def test_f4_multi_account_over_budget_group_refuses_whole_fire(clean_positions,
                                                               monkeypatch):
    """Two profiles on TWO accounts (acctA ok, acctB over-budget). The gate must run
    PER account group → acctB's group is refused → the WHOLE fire is refused with
    acctB's reason.

    Revert: run ONE aggregate gate call (broker_account_id=self.broker_account_id,
    total planned) → the acctB-specific decision is never made → the fire proceeds →
    the `status == FAILED` / `acctB in reason` asserts FAIL.
    """
    created = {}
    ltps = {"A": 100.0, "B": 200.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=ltps,
                        available_margin=10_000_000.0)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    # Bypass the vault-backed account-tradeable gate (not under test here) so the
    # fire reaches the RMS gate with both accounts' legs sized.
    import autotrade.broker.account_lifecycle as _acctlc
    monkeypatch.setattr(_acctlc, "assert_account_tradeable",
                        lambda *a, **k: None)
    # Resolve vault creds so each profile RETAINS its distinct broker_account_id at
    # fire time (an unresolvable binding falls back to the global account → None,
    # which would collapse both legs into one group). The MockBroker ignores creds.
    import autotrade.vault as _vault
    monkeypatch.setattr(_vault, "vault_enabled", lambda: True)
    monkeypatch.setattr(_vault, "get_decrypted_creds",
                        lambda acct, user_id=None: type(
                            "C", (), {"api_key": "k", "api_secret": "s",
                                      "access_token": "t", "broker": None})())

    calls = []

    def fake_gate(**kw):
        calls.append(kw)
        acct = kw.get("broker_account_id")
        if acct == "acctB":
            return risk_manager.RiskDecision(
                allow=False,
                reason=f"pre-trade RMS REFUSED for account {acct}",
                available_margin=1000.0, committed_other=0.0,
                planned_deployed=float(kw.get("planned_deployed") or 0.0),
                free=1000.0)
        return risk_manager.RiskDecision(
            allow=True, reason="ok", available_margin=1e7, committed_other=0.0,
            planned_deployed=float(kw.get("planned_deployed") or 0.0), free=1e7)

    monkeypatch.setattr(risk_manager, "pre_trade_gate", fake_gate)
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0)])
    p1 = BrokerProfile("z1", "zerodha", broker_account_id="acctA", symbols=["A"],
                       allocated_capital=150000.0)
    p2 = BrokerProfile("z2", "zerodha", broker_account_id="acctB", symbols=["B"],
                       allocated_capital=150000.0)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               broker_profiles=[p1, p2])
    sess = TradingSession.create(cfg, mode="paper", user_id="u1")

    res = asyncio.run(sess.start(when="now"))

    assert res["status"] == "FAILED"
    assert res.get("risk_refused") is True
    assert "acctB" in (res.get("reason") or "")
    # The gate was consulted PER account (acctB's own decision was made).
    assert any(c.get("broker_account_id") == "acctB" for c in calls)
    for mb in created.values():
        assert mb.placed == []


def test_f4_single_account_one_group_byte_identical(clean_positions, monkeypatch):
    """A single-account session collapses to ONE group → exactly ONE gate call with
    the whole-session (planned, account) → byte-identical. Proves F4 doesn't perturb
    the common case."""
    ltps = {"A": 100.0, "B": 200.0, "C": 50.0}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps=ltps,
                          available_margin=10_000_000.0)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)

    calls = []

    def fake_gate(**kw):
        calls.append(kw)
        return risk_manager.RiskDecision(
            allow=True, reason="ok", available_margin=1e7,
            planned_deployed=float(kw.get("planned_deployed") or 0.0), free=1e7)

    monkeypatch.setattr(risk_manager, "pre_trade_gate", fake_gate)
    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0), ("C", 3, 7.0, 50.0)])
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="paper", user_id="u1")

    res = asyncio.run(sess.start(when="now"))

    assert res["status"] == "RUNNING"
    assert len(calls) == 1                          # exactly one aggregate call


# ═══════════════════════════════════════════════════════════════════════════
# F5 — foreign same-side pending: REFUSE at entry, PAGE at exit.
# ═══════════════════════════════════════════════════════════════════════════
def test_f5_entry_refused_on_foreign_same_side_pending(clean_positions,
                                                       monkeypatch):
    """A FOREIGN (manual, non-Falcon) BUY resting for the entry symbol in this
    account → the LIVE entry is REFUSED (SKIPPED, manual_conflict) + paged; NOTHING
    is placed for it.

    Revert: remove the entry conflict guard in _place_one → the entry proceeds and
    places (n_placed==1, status RUNNING) → the `status == FAILED` / no-place / page
    asserts FAIL.
    """
    pages = []
    monkeypatch.setattr(alerts, "send_urgent_deduped",
                        lambda **kw: pages.append(kw))
    created = {}
    foreign = [{"tradingsymbol": "A", "transaction_type": "BUY", "quantity": 5,
                "order_id": "MANUAL-1", "status": "OPEN"}]

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps={"A": 100.0},
                        available_margin=10_000_000.0, pending_orders=foreign)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="live", user_id="u1")

    res = asyncio.run(sess.start(when="now"))

    assert res["status"] == "FAILED"                # the only leg was refused
    assert res["n_placed"] == 0
    for mb in created.values():
        assert mb.placed == []                      # NO order placed
    assert any(p.get("kind") == "MANUAL_CONFLICT" for p in pages)


def test_f5_entry_places_when_pending_is_ours(clean_positions, monkeypatch):
    """The complement: a resting BUY that is OURS (carries our compact tag) is NOT a
    conflict → the entry proceeds. (A fresh entry has no position row, so we simulate
    ownership by a resting order tagged for a pre-existing row.)"""
    monkeypatch.setattr(alerts, "send_urgent_deduped", lambda **kw: None)
    # No pending at all → the clean common case proceeds and places.
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps={"A": 100.0},
                        available_margin=10_000_000.0, pending_orders=[])
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    seed_signals([("A", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="live", user_id="u1")

    res = asyncio.run(sess.start(when="now"))

    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 1


def _mk_live_exit(monkeypatch, net_positions, *, pending=None):
    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps={"A": 100.0},
                          net_positions=net_positions, pending_orders=pending)
    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def test_f5_exit_still_places_but_pages_on_foreign_same_side(clean_positions,
                                                             monkeypatch):
    """A FOREIGN (manual) SELL rests for the symbol while AutoTrade exits the same
    long leg → the exit STILL places (clamped to our qty) BUT an URGENT
    MANUAL_CONFLICT_ON_EXIT page fires.

    Revert: remove the exit conflict page block in _exit_single_position_inner → the
    exit still places but NO page → the `MANUAL_CONFLICT_ON_EXIT paged` assert FAILS.
    """
    pages = []
    monkeypatch.setattr(alerts, "send_urgent_deduped",
                        lambda **kw: pages.append(kw))
    foreign = [{"tradingsymbol": "A", "transaction_type": "SELL", "quantity": 10,
                "order_id": "MANUAL-SELL-1", "status": "OPEN"}]
    sess = _mk_live_exit(monkeypatch, {"A": 10}, pending=foreign)
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="A", broker_profile=prof, qty=10, avg_price=100.0,
                           product="CNC", instrument_type="EQ", exchange="NSE",
                           direction="long")
    sess.registry.update_ltp("A", 100.0, broker_profile=prof)
    pos = sess.registry.get_open_positions()[0]
    broker = next(iter(sess.brokers.values()))

    r = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))

    # The exit still went through (needing to exit beats the conflict).
    assert r["status"] in ("EXITED", "COMPLETE")
    assert broker.exits == [("A", 10)]
    # And the fungible-risk page fired.
    assert any(p.get("kind") == "MANUAL_CONFLICT_ON_EXIT" for p in pages)


def test_f5_detector_ignores_our_own_and_other_side(clean_positions, monkeypatch):
    """Unit: _foreign_same_side_pending returns [] for our OWN order (id in
    owned_ids) and for the OPPOSITE side; returns the order only for a foreign
    same-side one."""
    mb = MockBroker(profile=BrokerProfile("p", "mock"), dry_run=False,
                    pending_orders=[
                        {"tradingsymbol": "A", "transaction_type": "BUY",
                         "quantity": 5, "order_id": "OURS", "status": "OPEN"},
                        {"tradingsymbol": "A", "transaction_type": "SELL",
                         "quantity": 5, "order_id": "OTHER-SIDE", "status": "OPEN"},
                        {"tradingsymbol": "A", "transaction_type": "BUY",
                         "quantity": 7, "order_id": "MANUAL", "status": "OPEN"}])
    out = asyncio.run(_foreign_same_side_pending(
        mb, "A", "BUY", owned_ids={"OURS"}, owned_tags=set()))
    ids = {o["order_id"] for o in out}
    assert ids == {"MANUAL"}                         # not OURS, not the SELL


# ═══════════════════════════════════════════════════════════════════════════
# F6 — ladder config edit real UPDATE conditional on config_version (no TOCTOU).
# ═══════════════════════════════════════════════════════════════════════════
def _make_ladder_with_children(n=1):
    lad = LadderCampaign.create(total_capital=300_000.0, order_product="CNC",
                                mode="paper", user_id="tester")
    child_cfg = lad._build_child_config()
    child_ids = []
    for _ in range(n):
        sess = TradingSession.create(child_cfg, mode="paper", user_id="tester")
        with falcon_conn() as con:
            con.execute(
                "UPDATE autotrade_sessions SET status='RUNNING', ladder_id=? "
                "WHERE session_id=?", (lad.ladder_id, sess.session_id))
            con.commit()
        child_ids.append(sess.session_id)
    return lad, child_ids


def _ladder_version(ladder_id):
    with falcon_conn() as con:
        r = con.execute("SELECT config_version FROM autotrade_ladders "
                        "WHERE ladder_id=?", (ladder_id,)).fetchone()
    return int(r["config_version"]) if r else None


def test_f6_toctou_conditional_update_no_clobber(clean_positions, monkeypatch):
    """TRUE TOCTOU: the request reads the ladder at base v0 and PASSES
    _assert_version_or_409, but a CONCURRENT edit lands in the window BETWEEN the
    version check and the real UPDATE (simulated via a one-shot side effect in
    _now_iso, evaluated right before the UPDATE executes). The real UPDATE must be
    CONDITIONAL on config_version in the SAME statement → it finds the drifted DB
    (v1) → 0 rows → 409, and does NOT clobber the concurrent edit.

    (A plain sequential stale edit is caught earlier by _assert_version_or_409, so
    it cannot distinguish this fix — only a race in the probe→update window can.)

    Revert: restore the separate version PROBE + the UNCONDITIONAL real UPDATE → the
    probe passes (DB still v0 at probe time), then the concurrent bump lands, then the
    unconditional UPDATE clobbers it (→ v2, arm_pct overwritten) with NO 409 → the
    `pytest.raises(409)` / `version stays 1` asserts FAIL.
    """
    lad, _ = _make_ladder_with_children(1)          # DB v0
    assert _ladder_version(lad.ladder_id) == 0

    # A concurrent editor lands EXACTLY in the check→update window. _now_iso() is
    # evaluated for `updated_at` just before the real UPDATE runs — bump the DB there
    # (once), on a separate connection, mimicking that concurrent edit committing.
    real_now = cfgapi._now_iso
    fired = {"done": False}

    def racing_now():
        if not fired["done"]:
            fired["done"] = True
            with falcon_conn() as con:
                con.execute(
                    "UPDATE autotrade_ladders SET config_version=config_version+1, "
                    "child_config_json=? WHERE ladder_id=?",
                    ('{"arm_pct": 0.05}', lad.ladder_id))
                con.commit()
        return real_now()

    monkeypatch.setattr(cfgapi, "_now_iso", racing_now)

    with pytest.raises(HTTPException) as ei:
        cfgapi.patch_ladder_config(
            lad.ladder_id, None, {"arm_pct": 0.09, "expected_config_version": 0},
            False, ADMIN)
    assert ei.value.status_code == 409

    # NOT clobbered: the DB reflects ONLY the concurrent edit (v1, arm_pct=0.05),
    # never our stale 0.09.
    assert _ladder_version(lad.ladder_id) == 1
    lad2 = LadderCampaign.load(lad.ladder_id)
    assert lad2.child_config_overrides()["arm_pct"] == 0.05


def test_f6_no_version_edit_still_applies_unconditional(clean_positions):
    """The complement: an edit that carries NO expected_config_version still applies
    unconditionally (byte-identical to the pre-fix no-version path)."""
    lad, _ = _make_ladder_with_children(1)
    resp = cfgapi.patch_ladder_config(
        lad.ladder_id, None, {"arm_pct": 0.04}, False, ADMIN)
    assert resp["ok"] is True
    assert resp["config_version"] == 1
    assert _ladder_version(lad.ladder_id) == 1
