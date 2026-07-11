"""SPRINT CLUSTER 9 — multi-tenant / multi-account ISOLATION + audit fixes.

This is a MULTI-USER, MULTI-BROKER-ACCOUNT product: each user logs in
(owner_user_id) and may connect MULTIPLE broker accounts (broker_account_id).
There must be ZERO trade/state clash between users, and zero clash between one
user's different broker accounts — even trading the SAME symbol at the same time.

Every test is MUTATION-VERIFIED: it PASSES with the fix and FAILS on the stated
revert. All paper-safe (MockBroker, no real orders); clock frozen to a mid-session
NSE trading day so no square-off/market-hours path interferes.

Items covered here:
  ITEM 1 — exit single-flight + DB exit-lock keyed by (session, symbol, profile).
  ITEM 2 — the FIRST kill single-order exit mints+persists a stable exit
           client_order_id and ADOPTS a tagged order on resume (zero new orders).
  ITEM 3 — sibling subtraction scoped by broker_account_id (THE core isolation fix).
  ITEM 4 — _our_working_exit_qty counts ONLY Falcon-owned resting orders.
  ITEM 5 — order_events dedup key includes broker_profile.
  ITEM 7 — confirm_exit books the filled portion on CANCELLED/REJECTED/TIMEOUT.
  CROSS-CUTTING — the reconciler invariant is bucketed by broker_account_id.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import (TradingSession, set_fake_now,
                               _exit_single_position, _our_working_exit_qty,
                               _falcon_owned_exit_ids_tags)
from autotrade.monitoring.registry import (PositionRegistry, sibling_open_qty,
                                           our_held_at_broker)
from autotrade.monitoring.position_reconciler import reconcile_broker_positions
from autotrade.monitoring.exit_poller import confirm_exit
from autotrade import exit_gate, order_ledger
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _reg_row(session_id, symbol, *, qty=100, avg=100.0, ltp=100.0,
             broker_profile="zerodha_default", broker_account_id=None,
             product="CNC", instrument_type="EQ", direction="long"):
    """Insert an OPEN autotrade_positions row for an arbitrary (fake) session."""
    reg = PositionRegistry(session_id, 1_000_000.0)
    reg.register(symbol=symbol, broker_profile=broker_profile, qty=qty,
                 avg_price=avg, product=product, instrument_type=instrument_type,
                 exchange="NSE", direction=direction,
                 broker_account_id=broker_account_id)
    reg.update_ltp(symbol, ltp, broker_profile=broker_profile)
    return reg


def _pos_row(session_id, symbol, broker_profile=None):
    with falcon_conn() as con:
        if broker_profile is not None:
            r = con.execute(
                "SELECT status, qty, exit_lock, exit_initiated_by FROM "
                "autotrade_positions WHERE session_id=? AND symbol=? AND "
                "COALESCE(broker_profile,'')=COALESCE(?,'')",
                (session_id, symbol, broker_profile)).fetchone()
        else:
            r = con.execute(
                "SELECT status, qty FROM autotrade_positions "
                "WHERE session_id=? AND symbol=?", (session_id, symbol)).fetchone()
    return dict(r) if r else None


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 1 — exit single-flight + DB exit-lock keyed by (session, symbol, profile).
# ═══════════════════════════════════════════════════════════════════════════
def test_item1_claim_exit_two_profiles_independent(clean_positions):
    """One session, SAME symbol on TWO broker profiles → each profile's exit-lock
    is INDEPENDENT: claiming profile p1 does NOT block claiming p2.

    Revert: drop the broker_profile scoping in claim_exit_session (symbol-wide
    UPDATE) → claiming p1 locks BOTH rows → the p2 claim (different reason) is
    blocked → the `p2 True` assert FAILS.
    """
    sid = "sess-item1"
    _reg_row(sid, "X", broker_profile="p1")
    _reg_row(sid, "X", broker_profile="p2")

    assert exit_gate.claim_exit_session(sid, "X", "KILL_SWITCH",
                                        broker_profile="p1") is True
    # A DIFFERENT reason on the OTHER profile must still succeed (independent lock).
    assert exit_gate.claim_exit_session(sid, "X", "STOP_STOCK",
                                        broker_profile="p2") is True
    assert _pos_row(sid, "X", "p1")["exit_lock"] == 1
    assert _pos_row(sid, "X", "p2")["exit_lock"] == 1
    assert _pos_row(sid, "X", "p1")["exit_initiated_by"] == "KILL_SWITCH"
    assert _pos_row(sid, "X", "p2")["exit_initiated_by"] == "STOP_STOCK"
    # And a same-reason re-claim on p1 is re-entrant TRUE; p2 stays owned by STOP.
    assert exit_gate.claim_exit_session(sid, "X", "KILL_SWITCH",
                                        broker_profile="p1") is True
    assert exit_gate.claim_exit_session(sid, "X", "KILL_SWITCH",
                                        broker_profile="p2") is False


def test_item1_flight_two_profiles_independent(clean_positions):
    """The in-flight single-flight slot is keyed by (session, symbol, profile):
    p1 in flight does NOT block p2; a second p1 IS blocked.

    Revert: key the flight on (session, symbol) → begin_exit_flight(p2) returns
    False (blocked by p1's flight) → the `p2 True` assert FAILS.
    """
    sid = "sess-item1f"
    try:
        assert exit_gate.begin_exit_flight(sid, "X", broker_profile="p1") is True
        # Second p1 flight blocked (already in flight).
        assert exit_gate.begin_exit_flight(sid, "X", broker_profile="p1") is False
        # p2 flies independently.
        assert exit_gate.begin_exit_flight(sid, "X", broker_profile="p2") is True
        assert exit_gate.is_exit_in_flight(sid, "X", broker_profile="p1") is True
        assert exit_gate.is_exit_in_flight(sid, "X", broker_profile="p2") is True
    finally:
        exit_gate.end_exit_flight(sid, "X", broker_profile="p1")
        exit_gate.end_exit_flight(sid, "X", broker_profile="p2")
    assert exit_gate.is_exit_in_flight(sid, "X", broker_profile="p1") is False


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 3 — sibling subtraction scoped by broker_account_id. THE CORE FIX.
# ═══════════════════════════════════════════════════════════════════════════
def test_item3_sibling_scoped_by_account_multi_user(clean_positions):
    """TWO USERS, SAME symbol, DIFFERENT broker accounts (same 'zerodha_default'
    profile string). User A's our_held must IGNORE user B's lot entirely — B's
    shares are in B's account net, not A's.

    Revert: drop the broker_account_id scoping in sibling_open_qty → B's 100 is
    subtracted from A's per-account net (100) → our_held(A)=0 → A misreads its own
    100 held shares as FLAT (would skip a real exit). The `== 100` assert FAILS.
    """
    _reg_row("userA-sess", "X", qty=100, broker_account_id="acctA")
    _reg_row("userB-sess", "X", qty=100, broker_account_id="acctB")

    # A's siblings ON A's ACCOUNT = 0 (B is a different account).
    assert sibling_open_qty("userA-sess", "X", "EQ",
                            broker_account_id="acctA") == 0
    # Un-scoped (the OLD behaviour) would have counted B's 100 — proving the leak.
    assert sibling_open_qty("userA-sess", "X", "EQ") == 100
    # A's per-account broker net is 100 (A's own lot). our_held must be 100, not 0.
    assert our_held_at_broker("userA-sess", "X", "EQ", 100, 100,
                              broker_account_id="acctA") == 100
    # Symmetric for B.
    assert our_held_at_broker("userB-sess", "X", "EQ", 100, 100,
                              broker_account_id="acctB") == 100


def test_item3_same_account_sibling_still_subtracted(clean_positions):
    """One user, ONE account, TWO sessions, SAME symbol → the account net IS shared,
    so a sibling on the SAME account is STILL subtracted (byte-identical to the
    pre-Cluster-9 single-account behaviour). This is the no-fire complement."""
    _reg_row("sessA", "X", qty=100, broker_account_id="acctA")
    _reg_row("sessB", "X", qty=100, broker_account_id="acctA")
    # Same account → sibling subtracted → account net 100 covers only one → 0.
    assert sibling_open_qty("sessA", "X", "EQ", broker_account_id="acctA") == 100
    assert our_held_at_broker("sessA", "X", "EQ", 100, 100,
                              broker_account_id="acctA") == 0


def test_item3_two_accounts_kill_does_not_misread_flat(clean_positions,
                                                        monkeypatch):
    """End-to-end: user A (acctA) fires a kill on X while user B (acctB) also holds
    X. A's broker net (its OWN account) shows 100; B is invisible to A's net. A must
    place its real exit — NOT reconcile-flat by miscounting B as a sibling.

    Revert: drop broker_account_id scoping in our_held_at_broker → A subtracts B's
    100 → our_held(A)=0 → A treats itself as already flat → places NO exit and
    reconciles a phantom close. `broker.exits == [("X", 100)]` FAILS.
    """
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps={"X": 99.0},
                        net_positions={"X": 100})
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)

    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess_a = TradingSession.create(cfg, mode="live", broker_account_id=None)
    # Force the session's account binding to acctA in-memory (vault disabled here).
    sess_a.broker_account_id = "acctA"
    sess_a._build_brokers()
    prof = sess_a.config.broker_profiles[0].profile_id
    sess_a.registry.register(symbol="X", broker_profile=prof, qty=100,
                             avg_price=100.0, product="CNC", instrument_type="EQ",
                             exchange="NSE", direction="long",
                             broker_account_id="acctA")
    sess_a.registry.update_ltp("X", 99.0, broker_profile=prof)
    # User B's lot on a DIFFERENT account (a different session row).
    _reg_row("userB-sess", "X", qty=100, broker_account_id="acctB")

    asyncio.run(sess_a.kill_switch.fire("TEST", gross_return=-0.05))

    broker = created[prof]
    assert broker.exits == [("X", 100)]                 # A DID exit its real lot
    assert _pos_row("userB-sess", "X") == {"status": "OPEN", "qty": 100}  # B untouched


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 4 — _our_working_exit_qty counts ONLY Falcon-owned resting orders.
# ═══════════════════════════════════════════════════════════════════════════
def test_item4_foreign_working_exit_not_counted(clean_positions):
    """A FOREIGN resting same-side SELL (not ours) must NOT be counted as working —
    it must never block our legit exit. Our OWN tagged order IS counted.

    Revert: drop the ownership gate in _our_working_exit_qty → the foreign SELL is
    counted → working=10 → the `== 0` assert FAILS.
    """
    coid = "FAL-OWN-COID"
    tag = order_ledger.compact_tag(coid)
    foreign = [{"tradingsymbol": "X", "transaction_type": "SELL",
                "quantity": 10, "order_id": "FOREIGN-1", "status": "OPEN"}]
    ours = [{"tradingsymbol": "X", "transaction_type": "SELL",
             "quantity": 10, "order_id": "OURS-1", "status": "OPEN", "tag": tag}]

    class _PB(MockBroker):
        pass

    prof_obj = type("P", (), {"profile_id": "zerodha_default",
                              "broker_name": "mock"})()

    fb = MockBroker(profile=prof_obj, dry_run=False, pending_orders=foreign)
    # No owned ids/tags → the foreign order is NOT ours → 0.
    assert asyncio.run(_our_working_exit_qty(fb, "X", "long",
                                             owned_ids=set(), owned_tags=set())) == 0
    ob = MockBroker(profile=prof_obj, dry_run=False, pending_orders=ours)
    # Our tag IS owned → counted.
    assert asyncio.run(_our_working_exit_qty(ob, "X", "long", owned_ids=set(),
                                             owned_tags={tag})) == 10


def test_item4_owned_ids_from_row(clean_positions):
    """_falcon_owned_exit_ids_tags derives our ids + compact tags from the position
    row (entry/exit/gtt ids + client/exit client-order-id tags), scoped to the
    (session, symbol, profile) leg — never another session's leg."""
    sid = "sess-item4"
    reg = _reg_row(sid, "X", broker_profile="p1")
    reg.set_exit_client_order_id("X", "FAL-EXIT-COID", broker_profile="p1")
    # A DIFFERENT session/leg must not leak into our owned set.
    _reg_row("other-sess", "X", broker_profile="p1")

    ids, tags = _falcon_owned_exit_ids_tags(sid, "X", broker_profile="p1")
    assert order_ledger.compact_tag("FAL-EXIT-COID") in tags
    # The other session's row is out of scope.
    ids2, tags2 = _falcon_owned_exit_ids_tags("other-sess", "X",
                                              broker_profile="p1")
    assert order_ledger.compact_tag("FAL-EXIT-COID") not in tags2


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 5 — order_events dedup key includes broker_profile.
# ═══════════════════════════════════════════════════════════════════════════
def _count_events(broker_order_id, event_type):
    with falcon_conn() as con:
        r = con.execute(
            "SELECT COUNT(*) AS c FROM autotrade_order_events "
            "WHERE broker_order_id=? AND event_type=?",
            (broker_order_id, event_type)).fetchone()
    return int(r["c"])


def test_item5_same_order_id_two_profiles_two_rows(clean_positions):
    """The SAME broker_order_id + event_type on TWO different broker profiles must
    produce TWO distinct ledger rows (each broker mints its own order-id space; ids
    CAN collide across accounts). A same-profile replay still dedups to ONE.

    Revert: restore the 2-col UNIQUE(broker_order_id, event_type) index (drop the
    profile from the key) → the second profile's row is deduped away → count is 1 →
    the `== 2` assert FAILS.
    """
    order_ledger.append_event(session_id="s1", symbol="X",
                              event_type=order_ledger.EV_EXIT_PLACED,
                              broker_order_id="SHARED-OID", broker_profile="pA")
    order_ledger.append_event(session_id="s2", symbol="X",
                              event_type=order_ledger.EV_EXIT_PLACED,
                              broker_order_id="SHARED-OID", broker_profile="pB")
    assert _count_events("SHARED-OID", order_ledger.EV_EXIT_PLACED) == 2
    # A replay of pA's event is deduped (still 2 total).
    order_ledger.append_event(session_id="s1", symbol="X",
                              event_type=order_ledger.EV_EXIT_PLACED,
                              broker_order_id="SHARED-OID", broker_profile="pA")
    assert _count_events("SHARED-OID", order_ledger.EV_EXIT_PLACED) == 2


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 7 — confirm_exit books the filled portion on CANCELLED / TIMEOUT.
# ═══════════════════════════════════════════════════════════════════════════
def test_item7_partial_then_cancelled_books_filled(clean_positions):
    """A partial fill (4 of 10) then CANCELLED must reduce the DB qty by the filled
    portion (to 6) BEFORE EXIT_FAILED, so the retry exits only the remainder — never
    the whole 10 again (which would oversell).

    Revert: remove the `if filled_qty > 0: update_partial_exit(...)` block in the
    CANCELLED/REJECTED branch → the DB qty stays 10 → the `qty == 6` assert FAILS.
    """
    sid = "sess-item7c"
    prof = "zerodha_default"
    reg = _reg_row(sid, "X", qty=10, avg=100.0, ltp=98.0, broker_profile=prof)
    prof_obj = type("P", (), {"profile_id": prof, "broker_name": "mock"})()
    broker = MockBroker(profile=prof_obj, dry_run=False,
                        order_status={"OID": {"status": "CANCELLED",
                                              "filled_quantity": 4,
                                              "average_price": 98.0}})
    res = asyncio.run(confirm_exit(session_id=sid, symbol="X", order_id="OID",
                                   qty=10, broker=broker, registry=reg,
                                   close_reason="KILL_SWITCH", max_wait_sec=2,
                                   poll_interval_sec=0.1, broker_profile=prof))
    assert res["status"] == "CANCELLED"
    row = _pos_row(sid, "X", prof)
    assert row["qty"] == 6                       # 10 - 4 filled
    assert row["status"] == "EXIT_FAILED"


def test_item7_partial_then_timeout_books_filled(clean_positions):
    """A partial fill (3 of 10) that then TIMES OUT (order still pending) must book
    the 3 so the DB qty is 7 before the caller cancels + retries.

    Revert: remove the `if filled_qty > 0: update_partial_exit(...)` block in the
    TIMEOUT tail → the DB qty stays 10 → the `qty == 7` assert FAILS.
    """
    sid = "sess-item7t"
    prof = "zerodha_default"
    reg = _reg_row(sid, "X", qty=10, avg=100.0, ltp=98.0, broker_profile=prof)
    prof_obj = type("P", (), {"profile_id": prof, "broker_name": "mock"})()
    # Status stays OPEN (pending) with 3 filled → confirm_exit loops to TIMEOUT.
    broker = MockBroker(profile=prof_obj, dry_run=False,
                        order_status={"OID": {"status": "OPEN",
                                              "filled_quantity": 3,
                                              "average_price": 98.0}})
    res = asyncio.run(confirm_exit(session_id=sid, symbol="X", order_id="OID",
                                   qty=10, broker=broker, registry=reg,
                                   close_reason="KILL_SWITCH", max_wait_sec=1,
                                   poll_interval_sec=0.2, broker_profile=prof))
    assert res["status"] == "TIMEOUT"
    row = _pos_row(sid, "X", prof)
    assert row["qty"] == 7                       # 10 - 3 filled, still OPEN
    assert row["status"] == "OPEN"


# ═══════════════════════════════════════════════════════════════════════════
# ITEM 2 — the FIRST kill single-order exit mints+persists a stable exit
#          client_order_id and ADOPTS a tagged order on resume (zero new orders).
# ═══════════════════════════════════════════════════════════════════════════
def test_item2_kill_mints_and_threads_exit_coid(clean_positions, monkeypatch):
    """The FIRST kill single-order exit now carries OUR client_order_id (persisted
    before the broker call) so a crash-then-resume can adopt-by-tag.

    Revert: drop the ITEM 2 mint/persist block (place with no client_order_id) →
    the placed exit's recorded client_order_id is None → the `is not None` assert
    FAILS.
    """
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps={"X": 99.0},
                        net_positions={"X": 100})
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="X", broker_profile=prof, qty=100,
                           avg_price=100.0, product="CNC", instrument_type="EQ",
                           exchange="NSE", direction="long")
    sess.registry.update_ltp("X", 99.0, broker_profile=prof)

    asyncio.run(sess.kill_switch.fire("TEST", gross_return=-0.05))

    broker = created[prof]
    assert broker.exit_calls, "the kill must have placed the exit"
    assert broker.exit_calls[0]["client_order_id"] is not None
    # And it was PERSISTED on the row (stable across a retry / resume).
    with falcon_conn() as con:
        r = con.execute("SELECT exit_client_order_id FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?",
                        (sess.session_id, "X")).fetchone()
    assert r["exit_client_order_id"] is not None


def test_item2_kill_adopts_tagged_exit_places_zero(clean_positions, monkeypatch):
    """CRASH-THEN-RESUME on the kill path: the position already has a persisted exit
    client_order_id and OUR tagged exit is already COMPLETE at the broker. The kill
    must ADOPT it (mark closed at its fill) and place ZERO new orders.

    Revert: drop the ITEM 2 query-before-place adopt block → the kill places a
    SECOND market exit → `broker.exits == []` FAILS (a duplicate exit → oversell).
    """
    coid = "FAL-RESUME-COID"
    tag = order_ledger.compact_tag(coid)
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(
            profile=profile, dry_run=False, ltps={"X": 99.0},
            net_positions={"X": 100},
            orders=[{"order_id": "OID1", "tradingsymbol": "X",
                     "transaction_type": "SELL", "status": "COMPLETE",
                     "filled_quantity": 100, "average_price": 99.0, "tag": tag}],
            order_status={"OID1": {"status": "COMPLETE", "filled_quantity": 100,
                                   "average_price": 99.0}})
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="X", broker_profile=prof, qty=100,
                           avg_price=100.0, product="CNC", instrument_type="EQ",
                           exchange="NSE", direction="long")
    sess.registry.update_ltp("X", 99.0, broker_profile=prof)
    # A PRIOR attempt persisted the exit client_order_id (the crash-before-close).
    sess.registry.set_exit_client_order_id("X", coid, broker_profile=prof)

    asyncio.run(sess.kill_switch.fire("TEST", gross_return=-0.05))

    broker = created[prof]
    assert broker.exits == []                    # placed ZERO new orders (adopted)
    assert _pos_row(sess.session_id, "X")["status"] == "CLOSED"


# ═══════════════════════════════════════════════════════════════════════════
# CROSS-CUTTING — the reconciler invariant is bucketed by broker_account_id.
# ═══════════════════════════════════════════════════════════════════════════
def test_crosscutting_reconcile_ignores_other_account(clean_positions,
                                                       monkeypatch):
    """User A reconciles X (MIS): A's own account net = 100 and A tracks 100 → IN
    SYNC → no action. User B (a DIFFERENT account) also tracks 100 of X on the same
    'zerodha_default' profile — that must NOT be summed into A's invariant.

    Revert: drop the acct_scope filter in _account_open_positions_for → A's
    db_held_all becomes 200 (A + B) vs broker_held 100 → a DEFICIT with >1 session →
    an UNATTRIBUTED_CLOSE alert. `actions == []` FAILS.
    """
    net_book = {"X": {"quantity": 100, "product": "MIS", "exchange": "NSE",
                      "buy_quantity": 100, "sell_quantity": 0,
                      "average_price": 100.0}}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps={"X": 100.0},
                          net_book=net_book)

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="MIS")
    sess_a = TradingSession.create(cfg, mode="live")
    sess_a.broker_account_id = "acctA"
    sess_a._build_brokers()
    prof = sess_a.config.broker_profiles[0].profile_id
    sess_a.registry.register(symbol="X", broker_profile=prof, qty=100,
                             avg_price=100.0, product="MIS", instrument_type="EQ",
                             exchange="NSE", direction="long",
                             broker_account_id="acctA")
    sess_a.registry.update_ltp("X", 100.0, broker_profile=prof)
    # User B's lot on acctB (same profile string, different account).
    _reg_row("userB-sess", "X", qty=100, broker_account_id="acctB",
             product="MIS")

    actions = reconcile_broker_positions(sess_a)
    assert actions == []                         # A in sync; B invisible to A
    assert _pos_row("userB-sess", "X") == {"status": "OPEN", "qty": 100}
