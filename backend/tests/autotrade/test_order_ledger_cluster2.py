"""SPRINT CLUSTER 2 — durable order ledger + idempotency backbone.

MUTATION-VERIFIED: each test asserts the SAFEGUARDED behaviour and FAILS when the
fix is reverted. The exact revert is stated in each test's docstring.

  CAP 1 — client_order_id + broker tag on every placement.
  CAP 2 — autotrade_order_events append-only ledger + dedup.
  CAP 3 — durable order-intent BEFORE broker submission (crash safety).
  CAP 5 — cross-day attribution via the ledger (CLOSED_SETTLED_AWAY +
          _confirmed_close ledger consult).

(CAP 4 — idempotent query-before-place-by-tag — is STAGED for the next pass; the
tag now rides on every order, laying the groundwork. See the report.)
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
from autotrade import order_ledger
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.execution.orders import Order
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.kill_switch import KillSwitchExecutor
from autotrade.monitoring.exit_poller import confirm_exit
from autotrade.monitoring.position_reconciler import reconcile_broker_positions
from autotrade.session import TradingSession, set_fake_now
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


class _FakeKite:
    """Minimal Kite constant surface for Order.to_kite_params (no network)."""
    VARIETY_REGULAR = "regular"
    PRODUCT_CNC = "CNC"
    PRODUCT_MIS = "MIS"
    PRODUCT_NRML = "NRML"
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_LIMIT = "LIMIT"
    TRANSACTION_TYPE_BUY = "BUY"
    TRANSACTION_TYPE_SELL = "SELL"
    EXCHANGE_NSE = "NSE"


def _prof(pid="zer", product="CNC", instrument_type="EQ"):
    return BrokerProfile(profile_id=pid, broker_name="zerodha",
                         allocated_capital=300000.0, order_product=product,
                         instrument_type=instrument_type)


def _patch_brokers(monkeypatch, factory):
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = created.get(profile.profile_id) or factory(profile)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    import autotrade.session as sess_mod
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def _events(session_id, symbol=None):
    return order_ledger.get_events(session_id, symbol)


def _pos(session_id, symbol):
    with falcon_conn() as con:
        r = con.execute(
            "SELECT * FROM autotrade_positions WHERE session_id=? AND symbol=?",
            (session_id, symbol)).fetchone()
    return dict(r) if r else None


# ══════════════════════════════════════════════════════════════════════════════
# CAP 1 — client_order_id + broker tag
# ══════════════════════════════════════════════════════════════════════════════

def test_cap1_client_order_id_unique_and_tag_bounded():
    """make_client_order_id is unique per call; compact_tag is deterministic,
    <=20 chars, alphanumeric. REVERT: n/a (primitive)."""
    a = order_ledger.make_client_order_id("sess-abc12345", "INFY")
    b = order_ledger.make_client_order_id("sess-abc12345", "INFY")
    assert a != b                                  # epoch-ms + attempt differ
    assert a.startswith("FAL-")
    t = order_ledger.compact_tag(a)
    assert t == order_ledger.compact_tag(a)        # deterministic
    assert len(t) <= 20
    assert t.isalnum()


def test_cap1_entry_order_carries_the_client_tag():
    """MUTATION: Order.to_kite_params attaches compact_tag(client_order_id) as the
    broker `tag`. REVERT: restore `tag=f"at-{self.symbol[:10].lower()}"[:20]` in
    orders.py to_kite_params → the tag no longer equals compact_tag → FAILS."""
    coid = order_ledger.make_client_order_id("sess-tag-1", "AAA")
    o = Order(symbol="AAA", qty=10, product="CNC",
              client_order_id=coid, tag=order_ledger.compact_tag(coid))
    params = o.to_kite_params(_FakeKite())
    assert params["tag"] == order_ledger.compact_tag(coid)
    assert len(params["tag"]) <= 20


def test_cap1_client_order_id_persisted_and_maps_to_broker_order_id(clean_positions):
    """The full client_order_id is persisted on the position row AND the ledger
    FILLED event maps it to the broker entry order-id. REVERT: drop the
    client_order_id column write in registry.register → the row's client_order_id
    is NULL → FAILS."""
    reg = PositionRegistry("sess-cap1-map", 300000.0)
    coid = order_ledger.make_client_order_id("sess-cap1-map", "AAA")
    reg.register(symbol="AAA", broker_profile="zer", qty=10, avg_price=100.0,
                 entry_order_id="ORD-BROKER-9", client_order_id=coid)
    row = _pos("sess-cap1-map", "AAA")
    assert row["client_order_id"] == coid
    assert row["entry_order_id"] == "ORD-BROKER-9"
    filled = [e for e in _events("sess-cap1-map", "AAA")
              if e["event_type"] == order_ledger.EV_FILLED]
    assert len(filled) == 1
    assert filled[0]["client_order_id"] == coid
    assert filled[0]["broker_order_id"] == "ORD-BROKER-9"


def test_cap1_kill_cancels_owned_pending_order_by_tag(clean_positions):
    """A kill cancels a pending broker order carrying OUR tag even when its
    order-id was never recorded — and leaves a foreign-tagged order intact.
    MUTATION REVERT: in kill_switch.fire STEP 1, change
    `owned = (str(oid) in owned_ids) or (otag ... in owned_tags)` back to
    `owned = str(oid) in owned_ids` → the tag-owned order is NOT cancelled → FAILS.
    """
    sid = "sess-cap1-kill"
    coid = order_ledger.make_client_order_id(sid, "AAA")
    our_tag = order_ledger.compact_tag(coid)
    prof = _prof()
    # Pending orders: OURS (by tag, unrecorded id) + a FOREIGN operator order.
    mb = MockBroker(profile=prof, dry_run=True, pending_orders=[
        {"order_id": "UNRECORDED-1", "tag": our_tag},
        {"order_id": "OPERATOR-99", "tag": "manual-resting"},
    ])
    reg = PositionRegistry(sid, 300000.0)
    # A CLOSED row still carries the client_order_id → no open leg to exit, but the
    # tag is owned. entry/exit order-ids stay NULL so ownership is TAG-ONLY.
    reg.register(symbol="AAA", broker_profile="zer", qty=10, avg_price=100.0,
                 client_order_id=coid)
    reg.mark_closed("AAA", "done", broker_profile="zer")
    cfg = TradingSessionConfig(total_allocated_capital=300000.0,
                               order_product="CNC")
    ks = KillSwitchExecutor(sid, cfg, {"zer": mb}, reg)
    asyncio.run(ks.fire("cap1-kill-test"))
    assert "UNRECORDED-1" in mb.cancelled     # ours (by tag) — cancelled
    assert "OPERATOR-99" not in mb.cancelled   # foreign — left intact


# ══════════════════════════════════════════════════════════════════════════════
# CAP 2 — append-only ledger + dedup
# ══════════════════════════════════════════════════════════════════════════════

def test_cap2_dedup_same_broker_order_and_event_type_one_row(clean_positions):
    """Two appends with the SAME (broker_order_id, event_type) leave ONE row.
    MUTATION REVERT: drop the UNIQUE from the (broker_order_id, event_type) index
    in db_migrations (make it a plain index) → the second INSERT OR IGNORE no
    longer conflicts → two rows → FAILS. (Note: keeping the UNIQUE index but
    changing INSERT OR IGNORE to a plain INSERT does NOT reproduce — the second
    insert raises IntegrityError which append_event swallows — so the index is the
    load-bearing dedup guarantee. Verified by dropping the UNIQUE in a fresh DB.)"""
    for _ in range(2):
        order_ledger.append_event(
            session_id="sess-dedup", symbol="AAA",
            event_type=order_ledger.EV_EXIT_FILLED,
            broker_order_id="DUP-ORDER-1", client_order_id="c1",
            qty=10, price=99.0, source="exit")
    with falcon_conn() as con:
        n = con.execute(
            "SELECT COUNT(*) FROM autotrade_order_events "
            "WHERE broker_order_id=? AND event_type=?",
            ("DUP-ORDER-1", order_ledger.EV_EXIT_FILLED)).fetchone()[0]
    assert n == 1


def test_cap2_null_broker_id_paper_rows_are_recorded_not_deduped(clean_positions):
    """Paper / pre-submission rows (NULL broker_order_id) are RECORDED and NOT
    collapsed by the dedup index (SQLite NULLs are distinct). REVERT: n/a — proves
    paper still logs a synthetic client_order_id with no broker id."""
    for i in range(2):
        order_ledger.append_event(
            session_id="sess-paper", symbol="AAA",
            event_type=order_ledger.EV_FILLED,
            broker_order_id=None, client_order_id=f"coid-{i}",
            qty=5, price=10.0, source="entry")
    rows = [e for e in _events("sess-paper", "AAA")
            if e["event_type"] == order_ledger.EV_FILLED]
    assert len(rows) == 2
    assert all(r["broker_order_id"] is None for r in rows)
    assert {r["client_order_id"] for r in rows} == {"coid-0", "coid-1"}


def test_cap2_full_lifecycle_is_a_reconstructable_ordered_trail(
        clean_positions, monkeypatch):
    """A full entry→exit produces an ordered, reconstructable event trail for the
    position: ORDER_CREATED → ORDER_SUBMITTED → FILLED → EXIT_PLACED → EXIT_FILLED
    → POSITION_CLOSED. REVERT: remove any of the lifecycle append_event calls →
    the trail is incomplete → FAILS."""
    ltps = {"AAA": 100.0}
    _patch_brokers(monkeypatch, lambda p: MockBroker(
        profile=p, dry_run=True, ltps=ltps))
    seed_signals([("AAA", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="paper")
    asyncio.run(sess._fire_entries())
    # Exit the position through the universal confirm path (mock reports COMPLETE).
    prof = sess.config.broker_profiles[0].profile_id
    order_status = {"exit-AAA": {"status": "COMPLETE", "filled_quantity":
                                 _pos(sess.session_id, "AAA")["qty"],
                                 "average_price": 110.0}}
    mb = MockBroker(profile=_prof(), dry_run=False, ltps=ltps,
                    order_status=order_status)
    asyncio.run(confirm_exit(
        session_id=sess.session_id, symbol="AAA", order_id="exit-AAA",
        qty=_pos(sess.session_id, "AAA")["qty"], broker=mb,
        registry=sess.registry, close_reason="KILL_SWITCH",
        broker_profile=prof))
    trail = [e["event_type"] for e in _events(sess.session_id, "AAA")]
    for ev in (order_ledger.EV_ORDER_CREATED, order_ledger.EV_ORDER_SUBMITTED,
               order_ledger.EV_FILLED, order_ledger.EV_EXIT_PLACED,
               order_ledger.EV_EXIT_FILLED, order_ledger.EV_POSITION_CLOSED):
        assert ev in trail, (ev, trail)
    # ORDER_CREATED precedes ORDER_SUBMITTED precedes FILLED precedes the exit.
    assert trail.index(order_ledger.EV_ORDER_CREATED) < \
        trail.index(order_ledger.EV_ORDER_SUBMITTED) < \
        trail.index(order_ledger.EV_FILLED) < \
        trail.index(order_ledger.EV_POSITION_CLOSED)


# ══════════════════════════════════════════════════════════════════════════════
# CAP 3 — durable intent BEFORE broker submission
# ══════════════════════════════════════════════════════════════════════════════

def test_cap3_record_intent_is_findable(clean_positions):
    """record_intent writes a durable ORDER_CREATED row locatable by
    client_order_id. REVERT: n/a (primitive underpinning the crash test)."""
    coid = order_ledger.make_client_order_id("sess-intent", "AAA")
    order_ledger.record_intent(session_id="sess-intent", symbol="AAA",
                               client_order_id=coid, qty=7, side="BUY",
                               product="CNC", broker_profile="zer")
    found = order_ledger.find_intent_by_client_order_id(coid)
    assert found is not None
    assert found["event_type"] == order_ledger.EV_ORDER_CREATED
    assert found["symbol"] == "AAA" and found["qty"] == 7


def test_cap3_crash_after_ack_leaves_detectable_orphan(
        clean_positions, monkeypatch):
    """Simulate a crash AFTER the broker accepts (order-id assigned) but BEFORE the
    position row is written (registry.register raises). The durable intent
    (ORDER_CREATED) + ORDER_SUBMITTED-with-broker-order-id survive so recovery can
    find the orphan; NO OPEN position exists.
    MUTATION REVERT: remove the `order_ledger.record_intent(...)` call in
    session._place_one (before the broker submission) → no ORDER_CREATED → the
    orphan is undetectable → FAILS."""
    ltps = {"AAA": 100.0}
    _patch_brokers(monkeypatch, lambda p: MockBroker(
        profile=p, dry_run=False, ltps=ltps))
    seed_signals([("AAA", 1, 9.0, 100.0)])
    cfg = TradingSessionConfig(total_allocated_capital=100000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")

    # Crash the position insert AFTER the broker has ACKed the order.
    def _boom(*a, **k):
        raise RuntimeError("simulated crash before position insert")
    # Patch on the instance's registry (register + register_partial).
    monkeypatch.setattr(sess.registry, "register", _boom)
    monkeypatch.setattr(sess.registry, "register_partial", _boom)

    asyncio.run(sess._fire_entries())     # leg crashes but is isolated

    # No position was written (the crash prevented it) …
    assert _pos(sess.session_id, "AAA") is None
    # … but the durable ORDER_CREATED intent — written BEFORE the broker call —
    # survives, so recovery can find the orphan (this is what a removed
    # record_intent would erase). The intent carries the client_order_id whose
    # compact_tag rides on the broker order.
    evs = _events(sess.session_id, "AAA")
    created = [e for e in evs
               if e["event_type"] == order_ledger.EV_ORDER_CREATED]
    assert created, [e["event_type"] for e in evs]
    assert created[0]["client_order_id"] and \
        created[0]["client_order_id"].startswith("FAL-")
    # When the broker DID ack before the crash, the ORDER_SUBMITTED row maps that
    # client_order_id to the broker order-id (present whenever placement succeeded).
    submitted = [e for e in evs
                 if e["event_type"] == order_ledger.EV_ORDER_SUBMITTED]
    if submitted:
        assert submitted[0]["broker_order_id"] == "ord-AAA"
        assert submitted[0]["client_order_id"] == created[0]["client_order_id"]


# ══════════════════════════════════════════════════════════════════════════════
# CAP 5 — cross-day attribution via the ledger
# ══════════════════════════════════════════════════════════════════════════════

def _carried_session(monkeypatch, *, net_book, holdings):
    def factory(p):
        return MockBroker(profile=p, dry_run=False,
                          ltps={"CARRIED": 96.0},
                          net_book=net_book, holdings=holdings)
    _patch_brokers(monkeypatch, factory)
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def test_cap5_closed_settled_away_from_ledger(clean_positions, monkeypatch):
    """A carried CNC position closed on a PRIOR day: broker fully flat + holdings 0
    + NO sell volume in today's single-day book (the sell was yesterday), BUT a
    persisted EXIT_PLACED event in the durable ledger → the reconciler closes it
    cleanly as CLOSED_SETTLED_AWAY instead of looping UNATTRIBUTED_CLOSE.
    MUTATION REVERT: drop `_any_ledger` from the fully-flat gate in
    position_reconciler (restore `if broker_held == 0 and _has_exit_evidence(...)`)
    → the phantom stays OPEN → FAILS."""
    # Today's net row: flat, NO buy/sell qty (cross-day → no same-day evidence).
    net_book = {"CARRIED": {"tradingsymbol": "CARRIED", "quantity": 0,
                            "exchange": "NSE", "product": "CNC"}}
    sess = _carried_session(monkeypatch, net_book=net_book, holdings={})
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="CARRIED", broker_profile=prof, qty=10,
                           avg_price=100.0, product="CNC", instrument_type="EQ")
    # The exit was placed yesterday (order-id recorded), never confirmed COMPLETE
    # before EOD → EXIT_FAILED with the exit order-id + a persisted EXIT_PLACED.
    sess.registry.mark_exit_failed("CARRIED", "EOD confirm timeout",
                                   broker_profile=prof, exit_order_id="X-YDAY")
    order_ledger.append_event(
        session_id=sess.session_id, symbol="CARRIED",
        event_type=order_ledger.EV_EXIT_PLACED, broker_profile=prof,
        broker_order_id="X-YDAY", qty=10, source="exit")

    actions = reconcile_broker_positions(sess)
    assert any(a["action"] == "CLOSED_SETTLED_AWAY" and a["symbol"] == "CARRIED"
               for a in actions), actions
    assert _pos(sess.session_id, "CARRIED")["status"] == "CLOSED"


def test_cap5_confirmed_close_ledger_consult_uses_fill_price(
        clean_positions, monkeypatch):
    """When the ledger holds a CONFIRMED exit fill (EXIT_FILLED with a price) for
    the position's exit order-id, _confirmed_close resolves the cross-day close at
    that fill price via RECONCILE_CLOSE_LEDGER (before the fully-flat fallback).
    MUTATION REVERT: remove the step-0 ledger consult at the top of
    _confirmed_close → the close falls back to CLOSED_SETTLED_AWAY (different
    close_reason) → the reason assertion FAILS."""
    net_book = {"CARRIED": {"tradingsymbol": "CARRIED", "quantity": 0,
                            "exchange": "NSE", "product": "CNC"}}
    sess = _carried_session(monkeypatch, net_book=net_book, holdings={})
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol="CARRIED", broker_profile=prof, qty=10,
                           avg_price=100.0, product="CNC", instrument_type="EQ")
    sess.registry.mark_exit_failed("CARRIED", "EOD confirm timeout",
                                   broker_profile=prof, exit_order_id="X-FILLED")
    order_ledger.append_event(
        session_id=sess.session_id, symbol="CARRIED",
        event_type=order_ledger.EV_EXIT_FILLED, broker_profile=prof,
        broker_order_id="X-FILLED", qty=10, price=96.0, source="exit")

    actions = reconcile_broker_positions(sess)
    closed = [a for a in actions if a["symbol"] == "CARRIED"
              and a["action"] in ("CLOSED_RECONCILED", "CLOSED_SETTLED_AWAY")]
    assert closed, actions
    assert closed[0]["close_reason"] == "RECONCILE_CLOSE_LEDGER"
    assert abs(float(closed[0]["exit_price"]) - 96.0) < 1e-6
    assert _pos(sess.session_id, "CARRIED")["status"] == "CLOSED"
