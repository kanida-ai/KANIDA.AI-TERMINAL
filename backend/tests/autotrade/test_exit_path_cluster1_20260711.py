"""SPRINT CLUSTER 1 — exit-path P0s (2026-07-11). Five mutation-verified fixes for
retry/timeout paths that route around the B1 single-flight guard (fb3f6e2) and
could place a DUPLICATE exit → a NAKED reverse position (real money).

Each test PASSES with the fix and FAILS if the fix is reverted — the revert is
stated in the docstring. All paper-safe (MockBroker, no real orders).

FIX 1 — cancel_and_retry_exit re-probes the broker net BEFORE every placement, so
        a filled-but-unconfirmed (TIMEOUT) first exit is not double-covered.
FIX 2 — a software-stop exit that TIMES OUT cancels the resting order + routes to
        the guarded retry, and the pre-exit reconcile nets out our own working exit
        so a resting exit blocks a second placement.
FIX 3 — the exit-qty clamp to our_held is UNCONDITIONAL (was gated on a GTT-cancel
        timeout), so a partial external fill can never overshoot into a reverse.
FIX 4 — mark_exit_failed / update_partial_exit are scoped by broker_profile, so a
        same-symbol-two-profile session mutates only the firing leg.
FIX 5 — a Falcon kill cancels ONLY Falcon's own recorded pending orders, never the
        operator's manual resting orders.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
import autotrade.monitoring.exit_poller as ep
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.exit_poller import confirm_exit, cancel_and_retry_exit
from autotrade.session import TradingSession, set_fake_now, _exit_single_position
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


# ── shared helpers ────────────────────────────────────────────────────────────
def _row_pf(sid, symbol, prof):
    with falcon_conn() as con:
        r = con.execute(
            """SELECT status, qty FROM autotrade_positions
               WHERE session_id=? AND symbol=?
                 AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
            (sid, symbol, prof)).fetchone()
    return dict(r) if r else None


def _row(sess, symbol):
    with falcon_conn() as con:
        r = con.execute("SELECT status, qty FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?",
                        (sess.session_id, symbol)).fetchone()
    return dict(r) if r else None


def _mk_live(monkeypatch, net_positions, *, ltps=None, pending=None,
             broker_cls=MockBroker):
    """A LIVE-mode single-broker session whose MockBroker answers the net-position
    probe (net_positions) and optionally a pending orderbook."""
    def fake_build_client(profile, dry_run=True):
        return broker_cls(profile=profile, dry_run=False, ltps=ltps or {"A": 100.0},
                          net_positions=net_positions, pending_orders=pending)
    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    cfg = TradingSessionConfig(total_allocated_capital=200000.0, top_n_stocks=1,
                               sizing_mode="equal", kill_switch_enabled=False,
                               order_product="CNC")
    sess = TradingSession.create(cfg, mode="live")
    sess._build_brokers()
    return sess


def _register(sess, symbol, qty, *, avg=100.0, ltp=100.0, gtt=False,
              direction="long"):
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol=symbol, broker_profile=prof, qty=qty,
                           avg_price=avg, product="CNC", instrument_type="EQ",
                           exchange="NSE", direction=direction)
    sess.registry.update_ltp(symbol, ltp, broker_profile=prof)
    if gtt:
        sess.registry.set_gtt(symbol, "G-" + symbol, broker_profile=prof)
    return prof


# ═══════════════════════════════════════════════════════════════════════════
# FIX 1 — cancel_and_retry_exit re-probes; a filled-but-unconfirmed exit is not
#         re-covered into a naked reverse.
# ═══════════════════════════════════════════════════════════════════════════
class _StatusRaiseFlatBroker(MockBroker):
    """get_order_status RAISES (status read times out) — models the first exit that
    ACTUALLY FILLED but whose confirm read never returned. net shows FLAT (0)
    because that first exit did fill at the broker."""
    def get_order_status(self, order_id):
        raise ConnectionResetError(10054, "status read timed out")


def test_fix1_retry_reprobes_flat_places_zero_orders(clean_positions):
    """place -> COMPLETE-but-status-read-raises -> TIMEOUT -> retry re-probes, sees
    the broker FLAT (net 0) -> places ZERO orders. Exactly ONE closing order total
    (the first, which filled).

    Revert verified: delete the pre-place re-probe guard in cancel_and_retry_exit
    (let it fall straight through to place_market_exit) -> the retry places a
    SECOND exit -> broker.exit_calls == 2 -> `len == 1` FAILS.
    """
    sid = "fix1-sess"
    reg = PositionRegistry(sid, 200000.0)
    reg.register(symbol="A", broker_profile="p1", qty=10, avg_price=100.0,
                 direction="long")
    reg.update_ltp("A", 100.0, broker_profile="p1")
    broker = _StatusRaiseFlatBroker(profile=BrokerProfile("p1", "mock"),
                                    dry_run=False, ltps={"A": 100.0},
                                    net_positions={"A": 0})

    # 1. FIRST exit — the one real closing order (it actually fills at the broker).
    res1 = asyncio.run(broker.place_market_exit("A", 10, "EQ", direction="long"))
    oid = res1.broker_order_id
    assert len(broker.exit_calls) == 1

    # 2. its confirm TIMES OUT (the status read keeps raising to the deadline).
    c = asyncio.run(confirm_exit(sid, "A", oid, 10, broker, reg,
                                 max_wait_sec=0.2, poll_interval_sec=0.05,
                                 broker_profile="p1"))
    assert c["status"] == "TIMEOUT"

    # 3. the retry RE-PROBES, sees the broker flat (net 0) → places ZERO orders.
    r = asyncio.run(cancel_and_retry_exit(sid, "A", oid, 10, broker, reg,
                                          max_wait_sec=0.2, poll_interval_sec=0.05,
                                          instrument_type="EQ", broker_profile="p1"))
    assert r.get("reconciled_flat") is True
    assert len(broker.exit_calls) == 1        # STILL one — the retry placed none.
    assert _row_pf(sid, "A", "p1")["status"] == "CLOSED"


def test_fix1_retry_still_places_when_broker_still_holds(clean_positions):
    """No-fire complement: if the re-probe shows the broker STILL holds our full
    qty (the first exit did NOT fill), the retry MUST place a fresh exit — the
    guard never suppresses a genuinely-needed retry."""
    sid = "fix1-sess-hold"
    reg = PositionRegistry(sid, 200000.0)
    reg.register(symbol="A", broker_profile="p1", qty=10, avg_price=100.0)
    reg.update_ltp("A", 100.0, broker_profile="p1")
    # Broker still holds 10 (first exit never filled); status read is fine → the
    # fresh exit confirms COMPLETE.
    broker = MockBroker(profile=BrokerProfile("p1", "mock"), dry_run=False,
                        ltps={"A": 100.0}, net_positions={"A": 10})
    r = asyncio.run(cancel_and_retry_exit(sid, "A", "exit-A", 10, broker, reg,
                                          max_wait_sec=1, poll_interval_sec=0.05,
                                          instrument_type="EQ", broker_profile="p1"))
    assert r.get("reconciled_flat") is not True
    # A fresh exit WAS placed (the retry is not suppressed when we still hold).
    assert any(e["symbol"] == "A" for e in broker.exit_calls)


# ═══════════════════════════════════════════════════════════════════════════
# FIX 2 — software-stop retry: resting order cancelled + working-exit netting →
#         no next-tick double.
# ═══════════════════════════════════════════════════════════════════════════
def test_fix2_timeout_cancels_resting_order_and_marks_failed(clean_positions,
                                                             monkeypatch):
    """A software-stop exit whose confirm TIMES OUT must CANCEL the still-resting
    order and mark the row EXIT_FAILED (routed to the guarded retry) — so the
    resting order can never fill alongside a next-tick re-fire (oversell).

    Revert verified: remove the `broker.cancel_order_sync(order_id)` call in the
    PARTIAL/TIMEOUT branch of _exit_single_position_inner (leave cancelled_ok
    False) -> `resting_order_cancelled` is False AND 'exit-A' is not in
    broker.cancelled -> both asserts FAIL.
    """
    sess = _mk_live(monkeypatch, {"A": 10})    # broker net still full (unfilled)
    _register(sess, "A", 10)
    pos = sess.registry.get_open_positions()[0]
    broker = next(iter(sess.brokers.values()))

    # Force confirm_exit → TIMEOUT fast (no 60s poll wait).
    def _stub_confirm(**kw):
        async def _c():
            return {"status": "TIMEOUT", "filled_qty": 0, "exit_price": None}
        return _c()
    monkeypatch.setattr(ep, "confirm_exit", _stub_confirm)

    r1 = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))

    assert r1["status"] == "EXIT_FAILED"
    assert r1["resting_order_cancelled"] is True
    assert "exit-A" in broker.sync_cancelled       # the resting order WAS cancelled
    assert _row(sess, "A")["status"] == "EXIT_FAILED"
    # INVARIANT: every exit order placed for A has been cancelled → 0 live now.
    a_placed = [e for e in broker.exit_calls if e["symbol"] == "A"]
    a_cancelled = [c for c in broker.sync_cancelled if c == "exit-A"]
    assert len(a_cancelled) >= len(a_placed)


def test_fix2_working_exit_netting_blocks_second_placement(clean_positions,
                                                           monkeypatch):
    """A resting exit we already placed does NOT reduce the broker net, so the
    net-based clamp can't see it. The pre-exit reconcile must net out our own
    working exit-side qty: a resting SELL for the full 10 → a re-fire places ZERO.

    Revert verified: delete the WORKING-EXIT NETTING block in
    _exit_single_position_inner -> our_held (10) passes unclamped -> a SECOND exit
    is placed -> broker.exit_calls is non-empty & status EXITED -> asserts FAIL.

    CLUSTER 9 ITEM 4: the resting order is OUR own exit, so it carries our compact
    tag (recognised as Falcon-owned); a foreign resting SELL would NOT block (see
    test_item4_foreign_working_exit_not_counted).
    """
    from autotrade.order_ledger import compact_tag
    _coid = "FAL-COID-A-1"
    pending = [{"tradingsymbol": "A", "transaction_type": "SELL",
                "quantity": 10, "order_id": "exit-A", "status": "OPEN",
                "tag": compact_tag(_coid)}]
    sess = _mk_live(monkeypatch, {"A": 10}, pending=pending)
    _register(sess, "A", 10)
    # Persist the exit client_order_id so its compact tag marks the resting order ours.
    sess.registry.set_exit_client_order_id(
        "A", _coid, broker_profile=sess.config.broker_profiles[0].profile_id)
    pos = sess.registry.get_open_positions()[0]
    broker = next(iter(sess.brokers.values()))

    r = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="EXIT_RETRY",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))

    assert r["status"] == "EXIT_PENDING_WORKING"
    assert r["working_qty"] == 10
    assert broker.exit_calls == []                 # NO second exit placed


def test_fix2_working_netting_ignores_foreign_and_entry_orders(clean_positions,
                                                              monkeypatch):
    """The netting counts ONLY our symbol's CLOSING side: a resting BUY (an entry)
    on A and a resting SELL on a DIFFERENT symbol must NOT be netted → the exit for
    A still fires normally."""
    pending = [{"tradingsymbol": "A", "transaction_type": "BUY",   # entry, not exit
                "quantity": 10, "order_id": "e1", "status": "OPEN"},
               {"tradingsymbol": "B", "transaction_type": "SELL",  # other symbol
                "quantity": 10, "order_id": "e2", "status": "OPEN"}]
    sess = _mk_live(monkeypatch, {"A": 10}, pending=pending)
    _register(sess, "A", 10)
    pos = sess.registry.get_open_positions()[0]
    broker = next(iter(sess.brokers.values()))

    r = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="EXIT_RETRY",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))

    assert r["status"] in ("EXITED", "COMPLETE")
    assert broker.exits == [("A", 10)]             # full exit, nothing netted


# ═══════════════════════════════════════════════════════════════════════════
# FIX 3 — the exit-qty clamp to our_held is UNCONDITIONAL (no GTT-timeout gate).
# ═══════════════════════════════════════════════════════════════════════════
def test_fix3_clamp_unconditional_long_no_gtt_timeout(clean_positions, monkeypatch):
    """our_held=7, intended qty=10, the GTT cancel SUCCEEDS (no timeout) → the exit
    is STILL clamped to 7 (long: SELL 7). A partial external fill can shrink held
    without any GTT-cancel timeout; the clamp must not depend on that timeout.

    Revert verified: restore the `gtt_cancel_timed_out and` conjunct on the clamp
    in _exit_single_position_inner -> the cancel succeeded so the flag is False ->
    clamp skipped -> SELL 10 -> `broker.exits == [("A", 7)]` FAILS.
    """
    sess = _mk_live(monkeypatch, {"A": 7})
    _register(sess, "A", 10, gtt=True)             # gtt set → cancel is attempted
    pos = sess.registry.get_open_positions()[0]
    broker = next(iter(sess.brokers.values()))

    asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))

    assert broker.exits == [("A", 7)]              # clamped to live-held, never 10


def test_fix3_clamp_unconditional_short_buy_to_cover(clean_positions, monkeypatch):
    """Same for a SHORT: our_held=7 (net -7), intended 10 → BUY-to-cover 7, never
    10 (a 10-cover would leave a +3 naked long)."""
    sess = _mk_live(monkeypatch, {"A": -7})
    _register(sess, "A", 10, gtt=True, direction="short")
    pos = sess.registry.get_open_positions()[0]
    broker = next(iter(sess.brokers.values()))

    asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))

    assert broker.exit_calls[0]["qty"] == 7
    assert broker.exit_calls[0]["direction"] == "short"


def test_fix3_no_clamp_when_broker_fully_covers(clean_positions, monkeypatch):
    """No-fire complement: our_held == qty → no clamp, the full 10 exits (unchanged
    byte-for-byte for the healthy case)."""
    sess = _mk_live(monkeypatch, {"A": 10})
    _register(sess, "A", 10, gtt=True)
    pos = sess.registry.get_open_positions()[0]
    broker = next(iter(sess.brokers.values()))

    asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC"))

    assert broker.exits == [("A", 10)]


# ═══════════════════════════════════════════════════════════════════════════
# FIX 4 — mark_exit_failed / update_partial_exit scoped by broker_profile.
# ═══════════════════════════════════════════════════════════════════════════
def _two_profile_rows(sid):
    reg = PositionRegistry(sid, 200000.0)
    reg.register(symbol="A", broker_profile="p1", qty=10, avg_price=100.0)
    reg.register(symbol="A", broker_profile="p2", qty=20, avg_price=100.0)
    return reg


def test_fix4_mark_exit_failed_scoped_to_one_profile(clean_positions):
    """Two OPEN rows, same symbol A, different broker_profile. mark_exit_failed on
    p1 leaves p2 OPEN.

    Revert verified: drop the `AND COALESCE(broker_profile,...)` predicate from
    mark_exit_failed (WHERE session_id AND symbol only) -> BOTH rows go EXIT_FAILED
    -> `p2 == OPEN` FAILS.
    """
    sid = "fix4-mef"
    reg = _two_profile_rows(sid)
    reg.mark_exit_failed("A", "boom", broker_profile="p1")
    assert _row_pf(sid, "A", "p1")["status"] == "EXIT_FAILED"
    assert _row_pf(sid, "A", "p2")["status"] == "OPEN"        # UNTOUCHED
    assert _row_pf(sid, "A", "p2")["qty"] == 20


def test_fix4_update_partial_exit_scoped_to_one_profile(clean_positions):
    """A partial exit (4 of 10) on p1 reduces only p1 to 6; p2 stays 20 OPEN.

    Revert verified: drop the broker_profile predicate from update_partial_exit
    (read + write WHERE session_id AND symbol only) -> the write hits BOTH rows
    (and the read picks an arbitrary row) -> `p2 qty == 20` FAILS.
    """
    sid = "fix4-upe"
    reg = _two_profile_rows(sid)
    reg.update_partial_exit("A", 4, 101.0, broker_profile="p1")
    assert _row_pf(sid, "A", "p1")["qty"] == 6
    assert _row_pf(sid, "A", "p2")["qty"] == 20               # UNTOUCHED
    assert _row_pf(sid, "A", "p2")["status"] == "OPEN"


# ═══════════════════════════════════════════════════════════════════════════
# FIX 5 — a Falcon kill cancels ONLY Falcon's own recorded pending orders.
# ═══════════════════════════════════════════════════════════════════════════
def test_fix5_kill_cancels_only_falcon_owned_pending(clean_positions):
    """The broker pending book has a Falcon order-id (Z's recorded entry order) and
    a FOREIGN order-id (the operator's manual resting limit). The kill cancels ONLY
    the Falcon one; the foreign order is NEVER in the cancel calls.

    Revert verified: remove the `if str(oid) not in owned_ids: skip` ownership
    filter in kill_switch STEP 1 -> the foreign 'MANUAL-1' is cancelled -> the
    `'MANUAL-1' not in broker.cancelled` assert FAILS.
    """
    from autotrade.monitoring.kill_switch import KillSwitchExecutor
    sid = "fix5-sess"
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                config_json) VALUES (?,?,?,?,?,?)""",
            (sid, "2026-06-24T09:00:00", "RUNNING", "paper", 500000.0, "{}"))
        con.commit()
    reg = PositionRegistry(sid, 500000.0)
    reg.register(symbol="Z", broker_profile="zer", qty=10, avg_price=100.0,
                 entry_order_id="FAL-ENTRY-Z")
    reg.update_ltp("Z", 100.0, broker_profile="zer")
    broker = MockBroker(
        profile=BrokerProfile("zer", "mock"), dry_run=False, ltps={"Z": 100.0},
        pending_orders=[{"order_id": "FAL-ENTRY-Z"},   # Falcon-owned (recorded)
                        {"order_id": "MANUAL-1"}])       # operator's manual order
    cfg = TradingSessionConfig(total_allocated_capital=500000.0,
                               kill_switch_enabled=True)
    ks = KillSwitchExecutor(sid, cfg, {"zer": broker}, reg)
    asyncio.run(ks.fire("TEST"))

    assert "FAL-ENTRY-Z" in broker.cancelled          # our own order cancelled
    assert "MANUAL-1" not in broker.cancelled          # foreign order LEFT INTACT
