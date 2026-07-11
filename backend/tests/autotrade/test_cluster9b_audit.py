"""SPRINT CLUSTER 9b — the 4 deferred audit findings (ITEMS 6, 8, 9, 10).

Multi-user / multi-account safe, paper byte-identical, real-money-safe. Every test
is MUTATION-VERIFIED: it PASSES with the fix and FAILS on the stated revert.

  ITEM 6  — recovery ADOPTS a broker-FILLED entry that has an order intent but NO
            position row (crash between broker-accept and the position insert).
  ITEM 8  — RMS pre_trade_gate FAILS CLOSED on unknown/errored margin in LIVE, and
            scopes the budget + committed-capital ledger PER broker account.
  ITEM 9  — an UNCERTIFIED live broker (rupeezy) is BLOCKED from REAL order
            placement (paper allowed); zerodha (certified) is unaffected.
  ITEM 10 — live config edit is optimistic-concurrency guarded (409 on a stale
            expected_config_version) + writes a durable audit row.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade import order_ledger, recovery, risk_manager, alerts
from autotrade.api import config_edit_routes as cfgapi
from autotrade.api.autotrade_routes import Caller
from autotrade.broker import registry as broker_registry
from autotrade.broker.rupeezy import RupeezyBroker
from autotrade.config import BrokerProfile, TradingSessionConfig
from autotrade.monitoring.registry import PositionRegistry
from autotrade.session import TradingSession, set_fake_now
from falcon.db import falcon_conn
from tests.autotrade.mock_broker import MockBroker

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
    """The shared clean_positions fixture doesn't wipe the append-only ledger /
    config-edit audit; clear them so a reused broker_order_id can't dedup-collide
    across tests (the order_events UNIQUE key is NOT session-scoped)."""
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


def _count_pos(session_id, symbol):
    with falcon_conn() as con:
        r = con.execute(
            "SELECT COUNT(*) AS c FROM autotrade_positions "
            "WHERE session_id=? AND symbol=?", (session_id, symbol)).fetchone()
    return int(r["c"])


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 6 — recovery adopts an orphan broker-FILLED entry intent.
# ═══════════════════════════════════════════════════════════════════════════
def _live_session_with_intent(monkeypatch, *, mb, submitted=True,
                              broker_oid="BOID1", coid="FAL-ORPHAN-1",
                              qty=100, price=100.0, symbol="X",
                              prof="zerodha_default"):
    """Create a LIVE session, write an ENTRY intent (ORDER_CREATED [+SUBMITTED])
    with NO position row, and monkeypatch build_client → the supplied MockBroker."""
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


def test_item6_adopts_filled_orphan(clean_positions, monkeypatch):
    """A SUBMITTED entry intent that FILLED at the broker but has NO position row →
    recovery REGISTERS the position at the real fill qty/price (adopt).

    Revert: make recovery._adopt_orphan_entry_intents return [] early (remove the
    orphan scan) → the filled orphan stays unregistered → the position-row assert
    FAILS.
    """
    prof_obj = type("P", (), {"profile_id": "zerodha_default",
                              "broker_name": "mock"})()
    mb = MockBroker(profile=prof_obj, dry_run=False,
                    order_status={"BOID1": {"status": "COMPLETE",
                                            "filled_quantity": 100,
                                            "average_price": 101.5}})
    sid, coid = _live_session_with_intent(monkeypatch, mb=mb)

    actions = recovery._adopt_orphan_entry_intents(sid)

    assert any(a["outcome"] == "adopted_filled" for a in actions)
    row = _pos_row(sid, "X", "zerodha_default")
    assert row is not None and row["status"] == "OPEN"
    assert row["qty"] == 100
    assert row["avg_price"] == pytest.approx(101.5)   # the REAL fill, not the mark


def test_item6_no_double_register(clean_positions, monkeypatch):
    """An already-positioned intent → the scan NEVER re-registers (idempotent). The
    pre-existing qty (50) must survive — it is NOT overwritten by the broker's 100.

    Revert: drop the `_position_exists_for_intent` idempotency skip → the scan
    re-registers → qty is overwritten to 100 → the `qty == 50` assert FAILS.
    """
    prof_obj = type("P", (), {"profile_id": "zerodha_default",
                              "broker_name": "mock"})()
    mb = MockBroker(profile=prof_obj, dry_run=False,
                    order_status={"BOID1": {"status": "COMPLETE",
                                            "filled_quantity": 100,
                                            "average_price": 101.5}})
    sid, coid = _live_session_with_intent(monkeypatch, mb=mb)
    # A position row already exists for this leg (the fire path DID insert it).
    reg = PositionRegistry(sid, 300000.0)
    reg.register(symbol="X", broker_profile="zerodha_default", qty=50,
                 avg_price=100.0, product="CNC", instrument_type="EQ",
                 exchange="NSE", direction="long", client_order_id=coid)

    actions = recovery._adopt_orphan_entry_intents(sid)

    assert _count_pos(sid, "X") == 1
    assert _pos_row(sid, "X", "zerodha_default")["qty"] == 50   # untouched
    assert all(a["outcome"] != "adopted_filled" for a in actions)


def test_item6_absent_order_paged_no_phantom(clean_positions, monkeypatch):
    """A SUBMITTED intent whose broker order is REJECTED at the broker → recovery
    pages and registers NO phantom position.

    Revert: remove the orphan scan → no page fires. This test asserts BOTH no
    position AND a page; the no-page revert fails the page assert (and the
    no-phantom guarantee still holds).
    """
    pages = []
    monkeypatch.setattr(alerts, "send_urgent_deduped",
                        lambda **kw: pages.append(kw))
    prof_obj = type("P", (), {"profile_id": "zerodha_default",
                              "broker_name": "mock"})()
    mb = MockBroker(profile=prof_obj, dry_run=False,
                    order_status={"BOID1": {"status": "REJECTED",
                                            "filled_quantity": 0,
                                            "average_price": 0.0}})
    sid, coid = _live_session_with_intent(monkeypatch, mb=mb)

    actions = recovery._adopt_orphan_entry_intents(sid)

    assert _pos_row(sid, "X") is None                 # NO phantom position
    assert any(a["outcome"] == "paged_rejected" for a in actions)
    assert pages, "an absent/rejected orphan must page"


def test_item6_created_only_never_accepted_silent(clean_positions, monkeypatch):
    """A pure ORDER_CREATED intent (never accepted at the broker — no SUBMITTED, no
    tag-matched order) is a rejected-at-fire leg, NOT an orphan → skipped SILENTLY
    (no position, no page)."""
    pages = []
    monkeypatch.setattr(alerts, "send_urgent_deduped",
                        lambda **kw: pages.append(kw))
    prof_obj = type("P", (), {"profile_id": "zerodha_default",
                              "broker_name": "mock"})()
    mb = MockBroker(profile=prof_obj, dry_run=False)      # get_orders() → None
    sid, coid = _live_session_with_intent(monkeypatch, mb=mb, submitted=False)

    actions = recovery._adopt_orphan_entry_intents(sid)

    assert _pos_row(sid, "X") is None
    assert all(a["outcome"] == "skip_never_accepted" for a in actions)
    assert not pages


def test_item6_paper_session_inert(clean_positions, monkeypatch):
    """A PAPER (dry_run) session is byte-identical — the orphan scan is a no-op
    (paper registers immediately; there is no orphan)."""
    prof_obj = type("P", (), {"profile_id": "zerodha_default",
                              "broker_name": "mock"})()
    mb = MockBroker(profile=prof_obj, dry_run=True)

    def fake_build_client(profile, dry_run=True):
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="paper")
    order_ledger.record_intent(session_id=sess.session_id, symbol="X",
                               client_order_id="C", qty=100, side="BUY",
                               broker_profile="zerodha_default", source="entry")
    assert recovery._adopt_orphan_entry_intents(sess.session_id) == []


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 8 — RMS pre-trade gate: fail-closed in LIVE + per-account scoping.
# ═══════════════════════════════════════════════════════════════════════════
class _RaisingMarginBroker(MockBroker):
    def available_margin(self):
        raise RuntimeError("kite.margins() blew up")


def _sess_row(sid, cap, *, status="RUNNING", user_id=None, acct=None):
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                config_json, user_id, broker_account_id)
               VALUES (?,?,?,?,?,?,?,?)""",
            (sid, "2026-06-24T09:00:00", status, "live", cap, "{}", user_id, acct))
        con.commit()


def test_item8_live_unknown_margin_refused(clean_positions):
    """LIVE + unknown margin (broker reports None) → REFUSED (fail-closed).

    Revert: drop the `if live and not allow_unknown_margin: refuse` branch (restore
    fail-open) → the gate ALLOWS on an unknown budget → the `allow is False` assert
    FAILS.
    """
    broker = MockBroker(profile=BrokerProfile("p", "mock"))  # available_margin None
    dec = risk_manager.pre_trade_gate(
        user_id="u1", session_id="new", planned_deployed=300000.0,
        brokers={"p": broker}, live=True)
    assert dec.allow is False
    assert risk_manager.MARGIN_UNKNOWN_REFUSED in (dec.reason or "")


def test_item8_paper_unknown_margin_inert(clean_positions):
    """PAPER (live=False) + unknown margin → INERT (allow), byte-for-byte
    unchanged. The complement no-fire case."""
    broker = MockBroker(profile=BrokerProfile("p", "mock"))
    dec = risk_manager.pre_trade_gate(
        user_id="u1", session_id="new", planned_deployed=1e12,
        brokers={"p": broker}, live=False)
    assert dec.allow is True
    assert dec.available_margin is None


def test_item8_live_override_allows(clean_positions):
    """LIVE + unknown margin + explicit operator override → allow (inert)."""
    broker = MockBroker(profile=BrokerProfile("p", "mock"))
    dec = risk_manager.pre_trade_gate(
        user_id="u1", session_id="new", planned_deployed=300000.0,
        brokers={"p": broker}, live=True, allow_unknown_margin=True)
    assert dec.allow is True


def test_item8_live_probe_raise_refused(clean_positions):
    """LIVE + the margin probe RAISES → REFUSED (fail-closed on the exception path,
    not just None).

    Revert: same fail-open revert as above → the raised probe is swallowed to
    'unknown' and ALLOWED → the `allow is False` assert FAILS.
    """
    broker = _RaisingMarginBroker(profile=BrokerProfile("p", "mock"))
    dec = risk_manager.pre_trade_gate(
        user_id="u1", session_id="new", planned_deployed=300000.0,
        brokers={"p": broker}, live=True)
    assert dec.allow is False
    assert risk_manager.MARGIN_UNKNOWN_REFUSED in (dec.reason or "")


def test_item8_committed_capital_scoped_per_account(clean_positions):
    """committed_capital scoped to a broker account IGNORES the user's OTHER
    account's committed capital.

    Revert: drop the broker_account_id filter in committed_capital → acctB's ₹5L is
    summed into acctA's budget → the `== 0.0` assert FAILS (leak).
    """
    _sess_row("a1", 500000.0, user_id="u1", acct="acctA")
    _sess_row("b1", 500000.0, user_id="u1", acct="acctB")
    # Scoped to acctA, excluding acctA's own session → 0 (acctB is invisible).
    assert risk_manager.committed_capital(
        "u1", exclude_session_id="a1", broker_account_id="acctA") == pytest.approx(0.0)
    # UNSCOPED (legacy) would see acctB's 500k — proving the isolation.
    assert risk_manager.committed_capital(
        "u1", exclude_session_id="a1") == pytest.approx(500000.0)


def test_item8_gate_per_account_no_cross_leak(clean_positions):
    """A pre_trade_gate for acctA (₹5L free) must NOT be reduced by acctB's ₹5L
    committed — so a ₹3L acctA deploy is ALLOWED.

    Revert: drop per-account scoping → committed_other = 5L (acctB) → free = 0 →
    the ₹3L deploy is REFUSED → the `allow is True` assert FAILS.
    """
    _sess_row("b1", 500000.0, user_id="u1", acct="acctB")   # other account committed
    # The broker's profile is bound to acctA (as _build_brokers binds it live).
    prof = BrokerProfile("p", "mock", broker_account_id="acctA")
    broker = MockBroker(profile=prof, available_margin=500000.0)
    dec = risk_manager.pre_trade_gate(
        user_id="u1", session_id="a1", planned_deployed=300000.0,
        brokers={"p": broker}, live=True, broker_account_id="acctA")
    assert dec.allow is True
    assert dec.committed_other == pytest.approx(0.0)


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 9 — uncertified rupeezy is blocked from LIVE order placement.
# ═══════════════════════════════════════════════════════════════════════════
def _rupeezy_profile():
    return type("P", (), {"profile_id": "rupeezy_default",
                          "broker_name": "rupeezy", "access_token": "tok",
                          "api_secret": "sec", "broker_account_id": None})()


def _order(symbol="X", qty=10):
    return type("O", (), {"symbol": symbol, "qty": qty, "order_type": "MARKET",
                          "price": None, "product": "CNC", "exchange": "NSE",
                          "client_order_id": "FAL-1", "tag": "AT-x"})()


def test_item9_registry_certification_flags():
    """The registry marks zerodha certified, rupeezy NOT (source of truth)."""
    assert broker_registry.is_certified("zerodha") is True
    assert broker_registry.is_certified("rupeezy") is False
    assert broker_registry.is_certified("upstox") is False


def test_item9_live_rupeezy_entry_blocked(clean_positions, monkeypatch):
    """A LIVE rupeezy place_order is REFUSED (UNCERTIFIED_BROKER_BLOCKED) + pages,
    with NO HTTP attempted.

    Revert: remove the `_certification_block()` gate in rupeezy.place_order → it
    proceeds to _order_body (raises 'instrument master not configured') → the error
    is NOT UNCERTIFIED_BROKER_BLOCKED → the substring assert FAILS.
    """
    pages = []
    monkeypatch.setattr(alerts, "send_urgent_deduped",
                        lambda **kw: pages.append(kw))
    b = RupeezyBroker(_rupeezy_profile(), dry_run=False)
    b._live_allowed = lambda: True     # force the live path (no env/token dance)
    # Prove no HTTP is attempted — the cert gate returns first.
    b._request = lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("HTTP must not be attempted for an uncertified broker"))

    res = asyncio.run(b.place_order(_order()))
    assert res.status == "FAILED"
    assert "UNCERTIFIED_BROKER_BLOCKED" in (res.error or "")
    assert pages, "the block must page"


def test_item9_live_rupeezy_exit_blocked(clean_positions, monkeypatch):
    """A LIVE rupeezy place_market_exit is likewise REFUSED (not a real exit)."""
    monkeypatch.setattr(alerts, "send_urgent_deduped", lambda **kw: None)
    b = RupeezyBroker(_rupeezy_profile(), dry_run=False)
    b._live_allowed = lambda: True
    b._request = lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("HTTP must not be attempted"))
    res = asyncio.run(b.place_market_exit("X", 10, "EQ", kite_product="CNC"))
    assert res.status == "FAILED"
    assert "UNCERTIFIED_BROKER_BLOCKED" in (res.error or "")


def test_item9_paper_rupeezy_ok(clean_positions):
    """PAPER (dry_run) rupeezy → DRY_RUN (cert gate never reached; paper works)."""
    b = RupeezyBroker(_rupeezy_profile(), dry_run=True)
    res = asyncio.run(b.place_order(_order()))
    assert res.status == "DRY_RUN"


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 10 — live config edit optimistic concurrency + audit row.
# ═══════════════════════════════════════════════════════════════════════════
def _running_session():
    cfg = TradingSessionConfig(
        total_allocated_capital=300000.0, top_n_stocks=3, sizing_mode="equal",
        strategy="intraday_basket", order_product="CNC",
        per_position_gtt_enabled=False, arm_pct=0.05, floor_pct=0.01,
        trail_giveback_pct=0.015, stop_pct=0.03, per_position_stop_pct=0.05,
        per_position_target_pct=0.06, square_off_time="15:29:00")
    sess = TradingSession.create(cfg, mode="paper", user_id="tester")
    with falcon_conn() as con:
        con.execute("UPDATE autotrade_sessions SET status='RUNNING', "
                    "invested_basis=30000.0 WHERE session_id=?",
                    (sess.session_id,))
        con.commit()
    return sess.session_id


def _cfg_version(sid):
    with falcon_conn() as con:
        r = con.execute("SELECT config_version, config_json FROM "
                        "autotrade_sessions WHERE session_id=?", (sid,)).fetchone()
    return dict(r)


def test_item10_stale_expected_version_409(clean_positions):
    """Two edits from the SAME base version: the first applies; the second (stale
    expected_config_version) is 409-rejected and NOT applied.

    Revert: remove `_assert_version_or_409` + the conditional UPDATE → the stale
    edit clobbers (applies, bumps to v2) → the 409/`config_version==1` asserts FAIL.
    """
    sid = _running_session()
    r1 = cfgapi.patch_session_config(
        sid, None, {"arm_pct": 0.06, "expected_config_version": 0}, False, ADMIN)
    assert r1["config_version"] == 1

    with pytest.raises(HTTPException) as ei:
        cfgapi.patch_session_config(
            sid, None, {"arm_pct": 0.09, "expected_config_version": 0}, False, ADMIN)
    assert ei.value.status_code == 409

    row = _cfg_version(sid)
    assert row["config_version"] == 1                  # NOT clobbered to 2
    assert '"arm_pct": 0.06' in row["config_json"]     # first edit stuck


def test_item10_audit_row_written(clean_positions):
    """A SUCCESSFUL edit writes ONE audit row (who/from/to/field-changes).

    Revert: remove the `_write_config_edit_audit(...)` call → no audit row → the
    count assert FAILS.
    """
    sid = _running_session()
    cfgapi.patch_session_config(
        sid, None, {"stop_pct": 0.025, "expected_config_version": 0}, False, ADMIN)
    with falcon_conn() as con:
        rows = con.execute(
            "SELECT * FROM autotrade_config_edits WHERE target_type='session' "
            "AND target_id=?", (sid,)).fetchall()
    assert len(rows) == 1
    a = dict(rows[0])
    assert a["from_version"] == 0 and a["to_version"] == 1
    assert a["user_id"] == "tester"
    assert "stop_pct" in a["field_changes"]


def test_item10_backward_compat_no_version(clean_positions):
    """An edit WITHOUT expected_config_version still applies (backward-compatible)
    and writes an audit row — the optimistic guard is opt-in per request."""
    sid = _running_session()
    r = cfgapi.patch_session_config(sid, None, {"arm_pct": 0.07}, False, ADMIN)
    assert r["config_version"] == 1
    with falcon_conn() as con:
        n = con.execute("SELECT COUNT(*) AS c FROM autotrade_config_edits "
                        "WHERE target_id=?", (sid,)).fetchone()["c"]
    assert n == 1
