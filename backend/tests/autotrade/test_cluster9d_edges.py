"""SPRINT CLUSTER 9d — the remaining multi-account / multi-profile-per-session
edges the prior clusters left open, plus one live-edit hardening.

Theme: a SINGLE session can route to MULTIPLE broker accounts (per-profile
broker_account_id), and this is a MULTI-USER product. The earlier clusters scoped
most things per broker_account_id but missed these edges.

Every test is MUTATION-VERIFIED: it PASSES with the fix and FAILS on the stated
revert. All paper-safe (MockBroker / direct helpers, no real orders / Kite);
clock frozen to a mid-session NSE trading day.

Items:
  F1 — RMS single-group path uses the GROUP's account (not self.broker_account_id).
  F2 — committed-capital ledger attributes per broker_account_id for a multi-account
       session (autotrade_session_account_allocations); single-account byte-identical.
  F3 — the reconciler invariant buckets by (symbol, product, broker_account_id).
  F6 — a SPECIFIED-but-unresolvable bound account FAILS CLOSED (no silent global
       fallback) unless break_glass_global.
  F7 — expected_config_version is MANDATORY for a LIVE config edit (400
       VERSION_REQUIRED); a paper edit stays optional.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
import autotrade.risk_manager as risk_manager
import autotrade.vault as _vault
import autotrade.broker.account_lifecycle as _acctlc
from autotrade.config import BrokerProfile, TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from autotrade.risk_manager import committed_capital, record_session_account_allocations
from autotrade.monitoring.position_reconciler import reconcile_broker_positions
from autotrade.broker.zerodha import ZerodhaBroker
from autotrade.api import config_edit_routes as cfg_api
from autotrade.api.autotrade_routes import Caller
from tests.autotrade.mock_broker import MockBroker
from tests.autotrade.conftest import seed_signals
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)
ADMIN = Caller(user_id="tester", is_admin=True, authenticated=True)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _mock_vault(monkeypatch):
    """Make the vault resolve creds so a profile RETAINS its broker_account_id at
    fire time (an unresolvable binding would clear to None → collapse groups)."""
    monkeypatch.setattr(_vault, "vault_enabled", lambda: True)
    monkeypatch.setattr(
        _vault, "get_decrypted_creds",
        lambda acct, user_id=None: type(
            "C", (), {"api_key": "k", "api_secret": "s",
                      "access_token": "t", "broker": None})())
    monkeypatch.setattr(_acctlc, "assert_account_tradeable", lambda *a, **k: None)


# ═══════════════════════════════════════════════════════════════════════════
# F1 — RMS single-group path uses the GROUP's own broker_account_id.
# ═══════════════════════════════════════════════════════════════════════════
def test_f1_single_profile_gate_uses_group_account(clean_positions, monkeypatch):
    """ONE explicit profile bound to acctA, session-level broker_account_id=None.
    The pre-trade RMS gate must be called with broker_account_id='acctA' (the
    group's own account) — NOT None (the session/global account).

    Revert: restore the `if len(account_groups)<=1: broker_account_id=
    self.broker_account_id` single-group path → the gate is called with None →
    the `== 'acctA'` assert FAILS.
    """
    ltps = {"A": 100.0}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps=ltps,
                          available_margin=10_000_000.0)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    _mock_vault(monkeypatch)

    calls = []

    def fake_gate(**kw):
        calls.append(kw)
        return risk_manager.RiskDecision(
            allow=True, reason="ok", available_margin=1e7,
            planned_deployed=float(kw.get("planned_deployed") or 0.0), free=1e7)

    monkeypatch.setattr(risk_manager, "pre_trade_gate", fake_gate)
    seed_signals([("A", 1, 9.0, 100.0)])
    p1 = BrokerProfile("z1", "zerodha", broker_account_id="acctA", symbols=["A"],
                       allocated_capital=150000.0)
    cfg = TradingSessionConfig(total_allocated_capital=150000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               broker_profiles=[p1])
    # session-level account is NONE — the bug budgeted this against None (global).
    sess = TradingSession.create(cfg, mode="paper", user_id="u1",
                                 broker_account_id=None)

    res = asyncio.run(sess.start(when="now"))

    assert res["status"] == "RUNNING"
    assert len(calls) == 1
    assert calls[0].get("broker_account_id") == "acctA"


# ═══════════════════════════════════════════════════════════════════════════
# F2 — committed-capital ledger attributes per broker_account_id.
# ═══════════════════════════════════════════════════════════════════════════
def _insert_session(session_id, *, user_id, total, broker_account_id, status="RUNNING"):
    cfg = TradingSessionConfig(total_allocated_capital=total, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False)
    with falcon_conn() as con:
        con.execute(
            "INSERT INTO autotrade_sessions "
            "(session_id, created_at, status, mode, total_allocated_capital, "
            " config_json, user_id, broker_account_id) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (session_id, "2026-06-25T09:00:00", status, "live", total,
             cfg.to_json(), user_id, broker_account_id))
        con.commit()


def test_f2_committed_capital_per_account_multi(clean_positions):
    """A multi-account session (profile1@acctA ₹3L + profile2@acctB ₹2L) → its
    committed capital is attributed PER account: committed(acctA)=3L, committed(
    acctB)=2L — NOT 5L on one account and 0 on the other.

    Revert: restore the session-level-only committed_capital query (COALESCE the
    SESSION's broker_account_id) → this session's account is None → committed(acctA)
    and committed(acctB) are BOTH 0 → the `== 300000` assert FAILS.
    """
    _insert_session("S-multi", user_id="u1", total=500000.0,
                    broker_account_id=None, status="RUNNING")
    record_session_account_allocations(
        "S-multi", {"acctA": 300000.0, "acctB": 200000.0})

    assert committed_capital("u1", broker_account_id="acctA") == 300000.0
    assert committed_capital("u1", broker_account_id="acctB") == 200000.0
    # The un-scoped grand total across all accounts is unchanged (the whole 5L).
    assert committed_capital("u1") == 500000.0


def test_f2_single_account_fallback_byte_identical(clean_positions):
    """A single-account session (no allocation rows) attributes its whole
    total_allocated_capital to its OWN account via the session-level fallback —
    byte-identical to the pre-F2 behaviour."""
    _insert_session("S-single", user_id="u1", total=500000.0,
                    broker_account_id="acctS", status="RUNNING")
    # No allocation rows written → session-level fallback path.
    assert committed_capital("u1", broker_account_id="acctS") == 500000.0
    # A different account sees nothing from this session.
    assert committed_capital("u1", broker_account_id="acctA") == 0.0
    assert committed_capital("u1") == 500000.0


# ═══════════════════════════════════════════════════════════════════════════
# F3 — the reconciler invariant buckets by (symbol, product, broker_account_id).
# ═══════════════════════════════════════════════════════════════════════════
def test_f3_reconcile_buckets_symbol_by_account(clean_positions, monkeypatch):
    """A MULTI-ACCOUNT session S (X on acctA, Y on acctB) reconciles cleanly while
    ANOTHER session T holds X on acctB. Because S's session-wide account SET spans
    {acctA, acctB}, the OLD code let T's X@acctB leak into S's X invariant (which is
    physically on acctA) → a false deficit. F3 buckets X's invariant by acctA only.

    Revert: restore the session-wide `acct_scope` on the (symbol, product) group →
    S's X invariant sums X@acctA (S, 100) + X@acctB (T, 100) = 200 vs acctA broker
    book 100 → deficit → UNATTRIBUTED_CLOSE alert. The `actions == []` assert FAILS.
    """
    # acctA book: X net 100 (S's only X). acctB book: Y net 100 (S) + X net 100 (T).
    book_a = {"X": {"quantity": 100, "product": "MIS", "exchange": "NSE",
                    "buy_quantity": 100, "sell_quantity": 0, "average_price": 100.0}}
    book_b = {"Y": {"quantity": 100, "product": "MIS", "exchange": "NSE",
                    "buy_quantity": 100, "sell_quantity": 0, "average_price": 100.0},
              "X": {"quantity": 100, "product": "MIS", "exchange": "NSE",
                    "buy_quantity": 100, "sell_quantity": 0, "average_price": 100.0}}

    def _book_for(profile):
        return book_a if profile.broker_account_id == "acctA" else book_b

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False,
                          ltps={"X": 100.0, "Y": 100.0},
                          net_book=_book_for(profile))

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    _mock_vault(monkeypatch)

    pA = BrokerProfile("pA", "zerodha", broker_account_id="acctA", symbols=["X"],
                       allocated_capital=150000.0, order_product="MIS")
    pB = BrokerProfile("pB", "zerodha", broker_account_id="acctB", symbols=["Y"],
                       allocated_capital=150000.0, order_product="MIS")
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=2,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="MIS", broker_profiles=[pA, pB])
    sess = TradingSession.create(cfg, mode="live", user_id="u1")
    sess._build_brokers()
    # S: X on acctA (profile pA) and Y on acctB (profile pB).
    sess.registry.register(symbol="X", broker_profile="pA", qty=100,
                           avg_price=100.0, product="MIS", instrument_type="EQ",
                           exchange="NSE", direction="long",
                           broker_account_id="acctA")
    sess.registry.update_ltp("X", 100.0, broker_profile="pA")
    sess.registry.register(symbol="Y", broker_profile="pB", qty=100,
                           avg_price=100.0, product="MIS", instrument_type="EQ",
                           exchange="NSE", direction="long",
                           broker_account_id="acctB")
    sess.registry.update_ltp("Y", 100.0, broker_profile="pB")

    # T: another session holding X on acctB (same 'pB'/acctB account net as S's Y).
    from autotrade.monitoring.registry import PositionRegistry
    treg = PositionRegistry("T-sess", 1_000_000.0)
    treg.register(symbol="X", broker_profile="pB", qty=100, avg_price=100.0,
                  product="MIS", instrument_type="EQ", exchange="NSE",
                  direction="long", broker_account_id="acctB")
    treg.update_ltp("X", 100.0, broker_profile="pB")

    actions = reconcile_broker_positions(sess)
    assert actions == []                     # each symbol reconciled per its account


def test_f3_single_account_reconcile_in_sync(clean_positions, monkeypatch):
    """Single-account complement: one account, one symbol, broker net == db held →
    IN SYNC, no action (byte-identical to pre-F3)."""
    book = {"X": {"quantity": 100, "product": "MIS", "exchange": "NSE",
                  "buy_quantity": 100, "sell_quantity": 0, "average_price": 100.0}}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps={"X": 100.0},
                          net_book=book)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="MIS")
    sess = TradingSession.create(cfg, mode="live")
    sess.broker_account_id = "acctA"
    sess._build_brokers()
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="X", broker_profile=prof, qty=100, avg_price=100.0,
                           product="MIS", instrument_type="EQ", exchange="NSE",
                           direction="long", broker_account_id="acctA")
    sess.registry.update_ltp("X", 100.0, broker_profile=prof)
    assert reconcile_broker_positions(sess) == []


# ═══════════════════════════════════════════════════════════════════════════
# F6 — a SPECIFIED-but-unresolvable bound account FAILS CLOSED.
# ═══════════════════════════════════════════════════════════════════════════
def test_f6_resolve_marks_specified_unresolvable_fail_closed(clean_positions):
    """A profile bound to acctA + vault DISABLED (unresolvable) + break_glass_global
    False → _resolve_account_creds clears the binding AND stamps the fail-closed
    marker so the live build refuses.

    Revert: drop the `setattr(prof, '_account_specified_unresolvable', not
    _break_glass)` in the vault-disabled branch → the marker is False → the
    `is True` assert FAILS (the leg would silently go global).
    """
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False)
    sess = TradingSession.create(cfg, mode="live", user_id="u1")
    prof = BrokerProfile("z1", "zerodha", broker_account_id="acctA")
    sess._resolve_account_creds(prof)               # vault disabled in tests
    assert prof.broker_account_id is None
    assert getattr(prof, "_account_specified_unresolvable", False) is True


def test_f6_break_glass_allows_global_fallback(clean_positions):
    """The SAME unresolvable binding WITH break_glass_global=True → the marker is
    cleared → the historic silent global fallback is allowed (explicit override).

    Revert: hard-code the marker to True regardless of break_glass → the `is False`
    assert FAILS.
    """
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               break_glass_global=True)
    sess = TradingSession.create(cfg, mode="live", user_id="u1")
    prof = BrokerProfile("z1", "zerodha", broker_account_id="acctA")
    sess._resolve_account_creds(prof)
    assert prof.broker_account_id is None
    assert getattr(prof, "_account_specified_unresolvable", False) is False


def test_f6_build_kite_refuses_specified_unresolvable_even_admin(clean_positions):
    """_build_kite must FAIL CLOSED for a specified-but-unresolvable account even for
    an ADMIN/operator owner (the prior FIX-A guard only caught non-admin owners) —
    never silently trade the operator's global account in place of a specified one.

    Revert: drop the F6 guard block in _build_kite → an admin owner falls through to
    the global get_kite_client path → no SPECIFIED_ACCOUNT_UNRESOLVABLE raise → the
    `pytest.raises(match=...)` FAILS.
    """
    prof = BrokerProfile("z1", "zerodha", broker_account_id=None)
    setattr(prof, "_bound_account_id_original", "acctA")
    setattr(prof, "_account_specified_unresolvable", True)
    prof.owner_user_id = None          # operator/global
    prof.owner_is_admin = True         # admin — the prior guard would let this pass
    broker = ZerodhaBroker(prof, dry_run=False)
    with pytest.raises(ValueError, match="SPECIFIED_ACCOUNT_UNRESOLVABLE"):
        broker._build_kite()


# ═══════════════════════════════════════════════════════════════════════════
# F7 — expected_config_version MANDATORY for a LIVE config edit.
# ═══════════════════════════════════════════════════════════════════════════
def _running_session(mode):
    cfg = TradingSessionConfig(
        total_allocated_capital=300_000.0, top_n_stocks=3, sizing_mode="equal",
        strategy="intraday_basket", order_product="CNC",
        per_position_gtt_enabled=False, per_stock_stop_enabled=False,
        square_off_enabled=True, arm_pct=0.05, floor_pct=0.01,
        trail_giveback_pct=0.015, stop_pct=0.03,
        per_position_stop_pct=0.05, per_position_target_pct=0.06,
        square_off_time="15:29:00", mis_square_off_time="15:12:00")
    sess = TradingSession.create(cfg, mode=mode, user_id="tester")
    with falcon_conn() as con:
        con.execute(
            "UPDATE autotrade_sessions SET status='RUNNING', invested_basis=30000.0, "
            "started_at='2026-06-25T09:15:00' WHERE session_id=?",
            (sess.session_id,))
        con.commit()
    return sess.session_id


def test_f7_live_edit_without_version_rejected(clean_positions):
    """A LIVE running session edit with NO expected_config_version → 400
    VERSION_REQUIRED (a stale UI can't clobber live risk/exit config).

    Revert: remove the `_require_version_for_live(...)` call in patch_session_config
    → the versionless live edit applies (no raise) → the `pytest.raises` FAILS.
    """
    sid = _running_session("live")
    with pytest.raises(HTTPException) as ei:
        cfg_api.patch_session_config(sid, None, {"arm_pct": 0.06}, False, ADMIN)
    assert ei.value.status_code == 400
    assert ei.value.detail["code"] == "VERSION_REQUIRED"
    # Nothing applied.
    with falcon_conn() as con:
        v = con.execute("SELECT config_version FROM autotrade_sessions "
                        "WHERE session_id=?", (sid,)).fetchone()["config_version"]
    assert int(v or 0) == 0


def test_f7_live_edit_with_version_applies(clean_positions):
    """A LIVE edit that DOES carry expected_config_version (matching current) applies
    normally — the mandate only rejects the versionless case."""
    sid = _running_session("live")
    resp = cfg_api.patch_session_config(
        sid, None, {"arm_pct": 0.06, "expected_config_version": 0}, False, ADMIN)
    assert resp["ok"] is True
    assert resp["config_version"] == 1


def test_f7_paper_edit_without_version_still_applies(clean_positions):
    """A PAPER session edit WITHOUT expected_config_version stays OPTIONAL — it still
    applies (backward-compatible; only LIVE is mandated)."""
    sid = _running_session("paper")
    resp = cfg_api.patch_session_config(sid, None, {"arm_pct": 0.06}, False, ADMIN)
    assert resp["ok"] is True
    assert resp["config_version"] == 1
