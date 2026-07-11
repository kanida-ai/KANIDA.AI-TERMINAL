"""SPRINT CLUSTER 8 ITEM 1 — EXIT ICEBERG (completes the C7 feature).

A large EXIT (per-stock stop / kill-switch / square-off) of a position whose qty
exceeds the effective slice/freeze cap is SLICED into child exits <= slice, placed
SEQUENTIALLY, each confirmed, aggregated; the position CLOSES only at FULL cover.
A child reject/partial leaves the REMAINDER as EXIT_FAILED (guarded retry), never a
mis-stated close. The C1 unconditional clamp (never exit more than our_held) runs
BEFORE the slice (clamp the TOTAL, then slice the clamped qty). Inert when iceberg
is off or qty <= slice (a single order, byte-for-byte unchanged).

Each test PASSES with the fix and FAILS on the stated revert. Paper-safe (MockBroker).
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.kill_switch import KillSwitchExecutor
from autotrade.monitoring.exit_poller import slice_and_confirm_exit
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


def _row(sid, symbol):
    with falcon_conn() as con:
        r = con.execute("SELECT status, qty FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?", (sid, symbol)).fetchone()
    return dict(r) if r else None


def _ice_cfg(**kw):
    base = dict(total_allocated_capital=2_000_000.0, top_n_stocks=1,
                sizing_mode="equal", kill_switch_enabled=False, order_product="CNC",
                iceberg_enabled=True, iceberg_slice_qty=300)
    base.update(kw)
    return TradingSessionConfig(**base)


def _mk_sess(monkeypatch, net_positions, *, cfg=None, ltps=None):
    """A LIVE single-broker session whose MockBroker answers the net-position probe."""
    _ltps = ltps or {"A": 100.0}

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps=dict(_ltps),
                          net_positions=net_positions)
    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    sess = TradingSession.create(cfg or _ice_cfg(), mode="live")
    sess._build_brokers()
    return sess


def _register(sess, symbol, qty, *, avg=100.0, ltp=100.0, direction="long"):
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol=symbol, broker_profile=prof, qty=qty,
                           avg_price=avg, product="CNC", instrument_type="EQ",
                           exchange="NSE", direction=direction)
    sess.registry.update_ltp(symbol, ltp, broker_profile=prof)
    return prof


# ══════════════════════════════════════════════════════════════════════════════
# PROOF 1 — a large exit slices into N children; CLOSED only at full cover.
# ══════════════════════════════════════════════════════════════════════════════
def test_exit_iceberg_slices_into_children_full_cover_closes(clean_positions,
                                                             monkeypatch):
    """qty 1000, slice 300, our_held 1000 → 4 child exits [300,300,300,100] each
    <= slice summing to 1000; the row CLOSES only after the FULL 1000 is covered.

    MUTATION REVERT: delete the `if getattr(exec_cfg,'iceberg_enabled',...)` exit-
    iceberg branch in session._exit_single_position_inner → ONE exit of 1000 is
    placed → `len(broker.exits) == 4` and the per-leg `<= 300` asserts FAIL."""
    sess = _mk_sess(monkeypatch, {"A": 1000})
    _register(sess, "A", 1000)
    pos = sess.registry.get_open_positions()[0]
    broker = next(iter(sess.brokers.values()))

    res = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC", exec_cfg=sess.config))

    assert res["status"] == "COMPLETE"
    assert res.get("iceberg") is True
    assert res["n_children"] == 4
    assert [q for _s, q in broker.exits] == [300, 300, 300, 100]
    assert all(q <= 300 for _s, q in broker.exits)
    assert sum(q for _s, q in broker.exits) == 1000        # full cover
    assert _row(sess.session_id, "A")["status"] == "CLOSED"


# ══════════════════════════════════════════════════════════════════════════════
# PROOF 2 — a mid-exit child reject → remainder EXIT_FAILED (guarded retry).
# ══════════════════════════════════════════════════════════════════════════════
def test_exit_iceberg_mid_child_reject_leaves_remainder_exit_failed(clean_positions,
                                                                    monkeypatch):
    """child0 (300) + child1 (300) fill, child2 REJECTS → 600 covered, the row is
    left OPEN-at-remainder then flagged EXIT_FAILED at qty 400 (NEVER a full close).

    MUTATION REVERT: in exit_poller.slice_and_confirm_exit change the final close
    gate `if filled_total >= total:` to `if filled_total > 0:` (close on ANY fill) →
    the row is marked CLOSED at 600 → `status == 'EXIT_FAILED'` and `qty == 400`
    FAIL (the mis-stated close this fix prevents)."""

    def fake_build_client(profile, dry_run=True):
        return MockBroker(profile=profile, dry_run=False, ltps={"A": 100.0},
                          net_positions={"A": 1000},
                          exit_reject_after={"A": 2})     # child2 rejects
    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    sess = TradingSession.create(_ice_cfg(), mode="live")
    sess._build_brokers()
    _register(sess, "A", 1000)
    pos = sess.registry.get_open_positions()[0]
    broker = next(iter(sess.brokers.values()))

    res = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC", exec_cfg=sess.config))

    assert res["status"] == "EXIT_FAILED"
    assert res["filled_qty"] == 600
    assert res["remaining_qty"] == 400
    assert [q for _s, q in broker.exits] == [300, 300]     # child2 never recorded
    row = _row(sess.session_id, "A")
    assert row["status"] == "EXIT_FAILED"
    assert row["qty"] == 400                                # remainder, NOT closed


# ══════════════════════════════════════════════════════════════════════════════
# PROOF 3 — clamp-BEFORE-slice: the sliced total never exceeds our_held.
# ══════════════════════════════════════════════════════════════════════════════
def test_exit_iceberg_clamp_before_slice_never_oversells(clean_positions,
                                                         monkeypatch):
    """DB qty 1000 but the broker holds only 400 for this session (net 400). The C1
    clamp reduces the TOTAL to 400 FIRST, then slices the clamped 400 → [300,100];
    the sliced total is 400, NEVER 1000.

    MUTATION REVERT: move the exit-iceberg branch ABOVE the `if our_held ... qty =
    int(our_held)` clamp in _exit_single_position_inner (slice the UN-clamped 1000)
    → legs [300,300,300,100] summing 1000 → `sum == 400` FAILS (the oversell)."""
    sess = _mk_sess(monkeypatch, {"A": 400})
    _register(sess, "A", 1000)
    pos = sess.registry.get_open_positions()[0]
    broker = next(iter(sess.brokers.values()))

    asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC", exec_cfg=sess.config))

    assert [q for _s, q in broker.exits] == [300, 100]     # clamped THEN sliced
    assert sum(q for _s, q in broker.exits) == 400          # never 1000


# ══════════════════════════════════════════════════════════════════════════════
# PROOF 4 — INERT when disabled: a large exit is ONE order (byte-for-byte).
# ══════════════════════════════════════════════════════════════════════════════
def test_exit_iceberg_inert_when_disabled_single_order(clean_positions, monkeypatch):
    """iceberg OFF → a 1000-share exit is placed as ONE order (control; proves the
    slicing is opt-in and paper/existing behaviour is unchanged)."""
    cfg = _ice_cfg(iceberg_enabled=False, iceberg_slice_qty=None)
    sess = _mk_sess(monkeypatch, {"A": 1000}, cfg=cfg)
    _register(sess, "A", 1000)
    pos = sess.registry.get_open_positions()[0]
    broker = next(iter(sess.brokers.values()))

    res = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP_STOCK",
        brokers=sess.brokers, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="CNC", exec_cfg=sess.config))

    assert res["status"] == "EXITED"
    assert "iceberg" not in res
    assert broker.exits == [("A", 1000)]                    # ONE order
    assert _row(sess.session_id, "A")["status"] == "CLOSED"


# ══════════════════════════════════════════════════════════════════════════════
# PROOF 5 — the KILL SWITCH path slices a large exit + CLOSES at full cover.
# ══════════════════════════════════════════════════════════════════════════════
def test_kill_switch_exit_iceberg_slices_and_closes(clean_positions):
    """kill_switch.fire() of a 1000-share position with iceberg on slices into 4
    child exits and CLOSES only at full cover; n_exited_ok counts it once.

    MUTATION REVERT: delete the exit-iceberg `sliced_jobs` branch in
    kill_switch.fire STEP 2 (fall through to the single parallel place_market_exit)
    → ONE exit of 1000 → `len(broker.exits) == 4` FAILS."""
    sid = "ks-ice-sess"
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_sessions
               (session_id, created_at, status, mode, total_allocated_capital,
                config_json) VALUES (?,?,?,?,?,?)""",
            (sid, "2026-06-24T09:00:00", "RUNNING", "live", 2_000_000.0, "{}"))
        con.commit()
    reg = PositionRegistry(sid, 2_000_000.0)
    reg.register(symbol="A", broker_profile="zer", qty=1000, avg_price=100.0,
                 product="CNC", instrument_type="EQ")
    reg.update_ltp("A", 100.0, broker_profile="zer")
    broker = MockBroker(profile=BrokerProfile("zer", "mock"), dry_run=False,
                        ltps={"A": 100.0}, net_positions={"A": 1000})
    cfg = TradingSessionConfig(total_allocated_capital=2_000_000.0,
                               kill_switch_enabled=True, order_product="CNC",
                               iceberg_enabled=True, iceberg_slice_qty=300)
    ks = KillSwitchExecutor(sid, cfg, {"zer": broker}, reg)
    summary = asyncio.run(ks.fire("TEST"))

    assert [q for _s, q in broker.exits] == [300, 300, 300, 100]
    assert sum(q for _s, q in broker.exits) == 1000
    assert summary["n_exit_failed"] == 0
    assert summary["n_exited_ok"] == 1
    assert _row(sid, "A")["status"] == "CLOSED"


# ══════════════════════════════════════════════════════════════════════════════
# PROOF 6 — helper unit: a PARTIAL child books the filled portion + EXIT_FAILED
#           remainder (never a full close).
# ══════════════════════════════════════════════════════════════════════════════
class _PartialChildBroker(MockBroker):
    """The 2nd exit placement of a symbol under-fills (COMPLETE but filled < ordered)
    so poll_order_fill returns PARTIAL for that child."""
    def __init__(self, *a, partial_leg_index=1, partial_fill=100, **kw):
        super().__init__(*a, **kw)
        self._partial_leg_index = partial_leg_index
        self._partial_fill = partial_fill
        self._exit_seq = 0

    async def place_market_exit(self, symbol, qty, instrument_type, **kw):
        idx = self._exit_seq
        self._exit_seq += 1
        res = await super().place_market_exit(symbol, qty, instrument_type, **kw)
        # tag the order-id so get_order_status can report a per-child partial
        if idx == self._partial_leg_index:
            res.broker_order_id = "part-" + symbol
        return res

    def get_order_status(self, order_id):
        if str(order_id).startswith("part-"):
            return {"status": "COMPLETE", "filled_quantity": self._partial_fill,
                    "average_price": 100.0}
        return super().get_order_status(order_id)


def test_exit_iceberg_helper_partial_child_books_remainder_failed(clean_positions):
    """A child that under-fills (200 ordered, 100 filled) STOPS the iceberg, books
    the 500 (300+100+100) covered via update_partial_exit, and flags the row
    EXIT_FAILED — never CLOSED.

    MUTATION REVERT: in slice_and_confirm_exit remove the PARTIAL `break` (keep
    slicing after an under-filled child) → more children fill and the row can reach
    full cover / CLOSE → `status == 'EXIT_FAILED'` FAILS."""
    sid = "ice-partial"
    reg = PositionRegistry(sid, 2_000_000.0)
    reg.register(symbol="A", broker_profile="p1", qty=1000, avg_price=100.0,
                 product="CNC", instrument_type="EQ")
    reg.update_ltp("A", 100.0, broker_profile="p1")
    broker = _PartialChildBroker(profile=BrokerProfile("p1", "mock"), dry_run=False,
                                 ltps={"A": 100.0}, partial_leg_index=2,
                                 partial_fill=100)
    # legs [300,300,300,100]; child2 under-fills 100/300 → stop after 3 placements.
    res = asyncio.run(slice_and_confirm_exit(
        session_id=sid, symbol="A", total_qty=1000, legs=[300, 300, 300, 100],
        broker=broker, registry=reg, close_reason="STOP_STOCK",
        broker_profile="p1", instrument_type="EQ", kite_product="CNC"))

    assert res["status"] == "EXIT_FAILED"
    assert res["filled_qty"] == 700            # 300 + 300 + 100 (partial)
    row = _row(sid, "A")
    assert row["status"] == "EXIT_FAILED"
    assert row["qty"] == 300                    # 1000 - 700 remainder OPEN-then-failed
