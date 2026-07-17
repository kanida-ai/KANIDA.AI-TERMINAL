"""WORKED-ORDER (participation / TWAP) execution engine — v1.

Works a large entry BUILD / exit UNWIND into many child slices PACED over time
(POV = participation_pct × recent-interval volume, a TWAP floor guaranteeing
progress, a hard per-child freeze cap), instead of one market shot. Required for
₹cr-per-name deployment where one order moves the price / breaches the freeze qty.

Each test PASSES with the fix and FAILS on the stated MUTATION REVERT. All paper /
mock — NO real Kite, NO real orders. The pacing clock is INJECTED (FakeClock) so
the async loop tests are deterministic (no wall-clock / no real sleeps).
"""
from __future__ import annotations

import asyncio
import time

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.broker.base import Pick
from autotrade.execution import worked_order as wo
from autotrade.execution.orders import place_order_with_retry, Order
from autotrade.monitoring.exit_poller import work_and_confirm_exit
from autotrade.monitoring.kill_switch import KillSwitchExecutor
from autotrade.session import TradingSession, set_fake_now, _exit_single_position
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)


@pytest.fixture(autouse=True)
def _clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


class FakeClock:
    """Injected pacing clock: now() returns the current epoch, sleep(s) ADVANCES
    it by s (no real delay) so the async loop paces deterministically."""

    def __init__(self, start: float = 1000.0):
        self.t = float(start)

    def now(self) -> float:
        return self.t

    async def sleep(self, s: float) -> None:
        self.t += float(s)


def _row(sid, symbol):
    with falcon_conn() as con:
        r = con.execute("SELECT status, qty, avg_price FROM autotrade_positions "
                        "WHERE session_id=? AND symbol=?", (sid, symbol)).fetchone()
    return dict(r) if r else None


# ══════════════════════════════════════════════════════════════════════════════
# PURE SIZING — POV cap, TWAP floor, hard freeze cap
# ══════════════════════════════════════════════════════════════════════════════

def test_next_child_pov_governs_when_below_freeze():
    """child = min(pov, remaining, freeze), floored by the TWAP floor. pov (2000) <
    freeze (5000) and > twap floor → child == pov.
    MUTATION REVERT: in next_child_qty drop the `min(pov, remaining)` primary (use
    just the twap floor) → child becomes ~81 (remaining/intervals) not 2000 → FAIL."""
    q = wo.next_child_qty(remaining=8000, n_intervals=99, recent_volume=20000,
                          participation_pct=0.10, freeze_cap=5000, min_child_qty=1)
    assert q == 2000                         # 10% of 20000, POV-paced
    assert q <= 5000                         # never exceeds the freeze cap


def test_next_child_freeze_cap_is_a_hard_cap():
    """pov (10000) > freeze (1500) → the freeze cap HARD-limits the child to 1500.
    MUTATION REVERT: remove the final `qty = min(qty, fc)` in next_child_qty → the
    child becomes ~10000 (or the twap floor) > 1500 → the `<= 1500` assert FAILS."""
    q = wo.next_child_qty(remaining=10000, n_intervals=99, recent_volume=100000,
                          participation_pct=0.10, freeze_cap=1500, min_child_qty=1)
    assert q == 1500
    assert q <= 1500


def test_twap_floor_guarantees_progress_when_volume_zero():
    """volume read 0 → pov 0 → the TWAP floor drives progress (ceil(remaining/il)),
    NEVER stalls at 0.
    MUTATION REVERT: in next_child_qty drop `qty = max(primary, floor_q)` (use just
    the POV primary) → with pov 0 the child is 0 → `q > 0` FAILS (a stalled order)."""
    q = wo.next_child_qty(remaining=1000, n_intervals=5, recent_volume=0,
                          participation_pct=0.10, freeze_cap=None, min_child_qty=1)
    assert q == 200                          # ceil(1000/5)
    assert q > 0                             # progress guaranteed


def test_intervals_left_and_twap_floor_edges():
    assert wo.intervals_left(1000.0, 1100.0, 20.0) == 5
    assert wo.intervals_left(1100.0, 1100.0, 20.0) == 1     # at/past deadline → 1
    assert wo.intervals_left(1000.0, 1030.0, 20.0) == 2     # ceil(30/20)
    assert wo.twap_floor(1000, 5) == 200
    assert wo.twap_floor(0, 5) == 0
    assert wo.participation_qty(20000, 0.10) == 2000
    assert wo.participation_qty(0, 0.10) == 0               # no volume → 0


def test_remaining_from_fills_restart_recompute():
    """Restart-durable remaining = target - already, never negative.
    MUTATION REVERT: in remaining_from_fills drop the max(0, ...) → (500,600)
    returns -100 → the `== 0` assert FAILS (a negative remaining re-fires)."""
    assert wo.remaining_from_fills(1000, 300) == 700
    assert wo.remaining_from_fills(1000, 1000) == 0
    assert wo.remaining_from_fills(500, 600) == 0           # over-filled → 0, not -100


# ══════════════════════════════════════════════════════════════════════════════
# ASYNC LOOP — children sum to target, POV pacing, partial/shortfall, fail-closed
# ══════════════════════════════════════════════════════════════════════════════

def _parent(**kw):
    base = dict(symbol="AAA", side="BUY", target_qty=10000, kind="entry",
                interval_sec=20.0, participation_pct=0.10, freeze_cap=5000,
                min_child_qty=1, max_children=500, deadline_ts=1000.0 + 100 * 20)
    base.update(kw)
    return wo.WorkedParent(**base)


def test_work_order_children_sum_to_target_pov_paced():
    """A parent works into N children summing to the TARGET, each <= the freeze cap,
    sized by POV (2000 = 10% of 20000 volume).
    MUTATION REVERT: in work_order remove `remaining -= cf` (don't subtract the
    confirmed fill) → the loop never converges → it runs to max_children with a huge
    over-fill → `sum(placed) == target` and `n_children == 5` FAIL."""
    clock = FakeClock(1000.0)
    placed = []

    async def pc(*, idx, qty, recent_volume):
        placed.append(qty)
        return {"filled_qty": qty, "avg_price": 100.0, "status": "OK",
                "broker_order_id": f"o{idx}"}

    res = asyncio.run(wo.work_order(
        _parent(target_qty=10000, freeze_cap=5000),
        place_child=pc, volume_fn=lambda s: 20000,
        now_fn=clock.now, sleep_fn=clock.sleep))

    assert placed == [2000, 2000, 2000, 2000, 2000]     # POV-paced, 5 children
    assert sum(placed) == 10000
    assert all(q <= 5000 for q in placed)               # freeze cap respected
    assert res.filled_qty == 10000 and res.shortfall == 0
    assert res.n_children == 5 and res.complete is True
    assert res.avg_fill_price == pytest.approx(100.0)


def test_work_order_twap_floor_progresses_when_volume_zero():
    """volume_fn returns 0 every interval → the POV is 0 but the TWAP floor still
    fills the whole target by the deadline.
    MUTATION REVERT: same as test_twap_floor_guarantees_progress_when_volume_zero
    (drop the twap-floor max) → 0-qty children → no progress → filled 0 → FAIL."""
    clock = FakeClock(1000.0)
    placed = []

    async def pc(*, idx, qty, recent_volume):
        placed.append(qty)
        return {"filled_qty": qty, "avg_price": 50.0, "status": "OK"}

    res = asyncio.run(wo.work_order(
        _parent(target_qty=1000, freeze_cap=None,
                deadline_ts=1000.0 + 5 * 20),
        place_child=pc, volume_fn=lambda s: 0,
        now_fn=clock.now, sleep_fn=clock.sleep))
    assert sum(placed) == 1000 and res.filled_qty == 1000
    assert all(q > 0 for q in placed)                   # never a stalled 0-child


def test_work_order_partial_at_deadline_surfaces_shortfall():
    """A THIN name absorbs only 100/child; at the deadline filled < target, the
    SHORTFALL is surfaced, the name is NOT dropped, and the filled qty IS the result.
    MUTATION REVERT: in work_order remove the `now >= deadline_ts` break (never stop
    at the deadline) → it keeps placing past the window and eventually fills 1000 →
    `filled_qty == 400` and `shortfall == 600` FAIL (impact-blind over-run)."""
    clock = FakeClock(1000.0)
    placed = []

    async def pc(*, idx, qty, recent_volume):
        placed.append(qty)
        return {"filled_qty": min(qty, 100), "avg_price": 100.0}   # thin book

    res = asyncio.run(wo.work_order(
        _parent(target_qty=1000, freeze_cap=None,
                deadline_ts=1000.0 + 4 * 20),      # only ~4 intervals
        place_child=pc, volume_fn=None,
        now_fn=clock.now, sleep_fn=clock.sleep))
    assert res.filled_qty == 400 and res.shortfall == 600      # 4 × 100 filled
    assert res.filled_qty < res.target_qty                     # name NOT skipped
    assert res.stopped_reason == "deadline" and res.deadline_hit is True
    assert res.n_children == 4


def test_work_order_child_reject_fails_closed_stops_the_name():
    """A child REJECT stops working the name and surfaces it (fail-closed) — no
    further children are placed.
    MUTATION REVERT: in work_order delete the `if res.get('rejected'): break` branch
    → it keeps placing after a reject → `len(placed) == 2` and stopped_reason FAIL."""
    clock = FakeClock(1000.0)
    placed = []

    async def pc(*, idx, qty, recent_volume):
        placed.append(qty)
        if idx == 1:
            return {"filled_qty": 0, "avg_price": 0.0, "rejected": True,
                    "error": "circuit"}
        return {"filled_qty": qty, "avg_price": 100.0}

    res = asyncio.run(wo.work_order(
        _parent(target_qty=1000, freeze_cap=None, deadline_ts=1000.0 + 100 * 20),
        place_child=pc, volume_fn=None, now_fn=clock.now, sleep_fn=clock.sleep))
    assert len(placed) == 2                       # stopped after the reject
    assert "rejected" in (res.stopped_reason or "")
    assert res.filled_qty == placed[0]            # only the first child filled
    assert res.shortfall > 0


def test_work_order_child_raise_fails_closed_no_further_place():
    """A place_child that RAISES (e.g. an inconclusive query-before-retry
    OrderTimeoutError) is FAIL-CLOSED: the loop stops and places NO more children —
    the exact double-fill direction to avoid.
    MUTATION REVERT: in work_order remove the `try/except` around `await
    place_child(...)` (let the exception propagate / continue) → the assertions on a
    clean stop (len(calls)==2, ERROR child recorded) FAIL."""
    clock = FakeClock(1000.0)
    calls = []

    async def pc(*, idx, qty, recent_volume):
        calls.append(qty)
        if idx == 1:
            raise RuntimeError("order timeout inconclusive")
        return {"filled_qty": qty, "avg_price": 100.0}

    res = asyncio.run(wo.work_order(
        _parent(target_qty=1000, freeze_cap=None, deadline_ts=1000.0 + 100 * 20),
        place_child=pc, volume_fn=None, now_fn=clock.now, sleep_fn=clock.sleep))
    assert len(calls) == 2                         # no third placement (fail-closed)
    assert "place error" in (res.stopped_reason or "")
    assert res.children[-1].status == "ERROR"      # the raising child is audited
    assert res.filled_qty == calls[0]


def test_work_order_stop_fn_halts_immediately():
    """A stop_fn (manual stop / kill / breaker) halts working NOW — whatever filled
    is the result."""
    clock = FakeClock(1000.0)
    placed = []
    _stop = {"v": False}

    async def pc(*, idx, qty, recent_volume):
        placed.append(qty)
        _stop["v"] = True                          # stop after the first child
        return {"filled_qty": qty, "avg_price": 100.0}

    res = asyncio.run(wo.work_order(
        _parent(target_qty=1000, freeze_cap=None, deadline_ts=1000.0 + 100 * 20),
        place_child=pc, volume_fn=None, now_fn=clock.now, sleep_fn=clock.sleep,
        stop_fn=lambda: _stop["v"]))
    assert len(placed) == 1 and res.stopped_reason == "stopped"


# ══════════════════════════════════════════════════════════════════════════════
# IDEMPOTENCY — a child confirm-timeout does NOT double-place (query-before-retry)
# ══════════════════════════════════════════════════════════════════════════════

def test_worked_child_timeout_adopts_no_duplicate():
    """When a worked child goes through the real place path (place_order_with_retry)
    and the broker confirmation TIMES OUT but the order already reached the broker,
    find_recent_order ADOPTS it — place_order is called EXACTLY ONCE per child, no
    duplicate.
    MUTATION REVERT: in place_order_with_retry delete the `if existing is not None:`
    adopt branch → each timed-out child re-places → broker.calls == 2 × n_children →
    the `broker.calls == res.n_children` assert FAILS (the ZENSARTECH double-fill)."""

    class AdoptBroker(MockBroker):
        def __init__(self, **kw):
            super().__init__(**kw)
            self.calls = 0

        async def place_order(self, order):
            self.calls += 1
            await asyncio.sleep(0.3)               # exceeds the 50ms wait below
            return await super().place_order(order)

        def find_recent_order(self, order):
            return {"order_id": f"real-{self.calls}", "status": "COMPLETE",
                    "tag": order.tag, "tradingsymbol": order.symbol,
                    "transaction_type": "BUY", "quantity": order.qty}

    broker = AdoptBroker(profile=None, ltps={"ZEN": 100.0})
    clock = FakeClock(1000.0)

    async def pc(*, idx, qty, recent_volume):
        o = Order(symbol="ZEN", qty=qty, transaction_type="BUY")
        o.tag = f"ATtag{idx}"
        r = await place_order_with_retry(o, broker, max_retries=3, timeout_ms=50)
        return {"filled_qty": qty, "avg_price": 100.0,
                "broker_order_id": r.broker_order_id}

    res = asyncio.run(wo.work_order(
        _parent(target_qty=600, freeze_cap=None, deadline_ts=1000.0 + 2 * 20),
        place_child=pc, volume_fn=None, now_fn=clock.now, sleep_fn=clock.sleep))
    assert res.filled_qty == 600
    assert broker.calls == res.n_children          # ONE place per child, no dup


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG — validation + round-trip; default-off
# ══════════════════════════════════════════════════════════════════════════════

def test_config_worked_execution_mode_validation_and_roundtrip():
    """execution_mode 'worked' is valid; the worked_* knobs validate WHEN SET and
    round-trip through to_dict/from_dict.
    MUTATION REVERT: in config.validate() remove the worked_participation_pct range
    check → the `worked_participation_pct=1.5` case stops raising → FAIL."""
    cfg = TradingSessionConfig(
        total_allocated_capital=250_000_000.0, top_n_stocks=10,
        execution_mode="worked", worked_participation_pct=0.10,
        worked_interval_sec=20, worked_min_child_qty=100)
    cfg.validate()
    rt = TradingSessionConfig.from_dict(cfg.to_dict())
    assert rt.execution_mode == "worked"
    assert rt.worked_participation_pct == 0.10
    assert rt.worked_interval_sec == 20
    assert rt.worked_min_child_qty == 100
    with pytest.raises(ValueError):
        TradingSessionConfig(total_allocated_capital=1e6,
                             execution_mode="worked",
                             worked_participation_pct=1.5).validate()
    with pytest.raises(ValueError):
        TradingSessionConfig(total_allocated_capital=1e6,
                             execution_mode="worked",
                             worked_interval_sec=0).validate()


def test_config_default_execution_mode_is_not_worked():
    """DEFAULT-OFF: a fresh config is 'marketable_limit' (the deployed one-shot
    default), NEVER 'worked'. Worked must be opted into explicitly."""
    cfg = TradingSessionConfig(total_allocated_capital=1e6)
    assert cfg.execution_mode in ("market", "marketable_limit")
    assert cfg.execution_mode != "worked"


# ══════════════════════════════════════════════════════════════════════════════
# SESSION ENTRY — worked build grows ONE position; partial surfaces shortfall
# ══════════════════════════════════════════════════════════════════════════════

def _bp():
    return BrokerProfile(profile_id="zerodha_default", broker_name="zerodha",
                         allocated_capital=5_000_000.0, order_product="MIS",
                         instrument_type="EQ")


def _worked_cfg(**kw):
    base = dict(total_allocated_capital=5_000_000.0, top_n_stocks=1,
                sizing_mode="equal", kill_switch_enabled=False,
                order_product="MIS", execution_mode="worked",
                per_position_gtt_enabled=False, worked_participation_pct=0.10,
                worked_interval_sec=20, worked_max_children=500,
                broker_profiles=[_bp()])
    base.update(kw)
    return TradingSessionConfig(**base)


def test_work_entry_leg_builds_one_position_incrementally(clean_positions,
                                                          monkeypatch):
    """A worked ENTRY leg works `target_qty` into paced children (POV-sized, freeze-
    capped) and grows ONE aggregate position; the final row carries the full qty.
    MUTATION REVERT: in _work_entry_leg's _place_child drop the
    `self._register_worked_position(...)` call → the position row is never grown →
    `_row(...).qty == 1000` FAILS (no live-monitored position during the build)."""
    cfg = _worked_cfg(iceberg_freeze_qty_default=300)
    sess = TradingSession.create(cfg, mode="live")
    prof = sess.config.broker_profiles[0]
    broker = MockBroker(profile=prof, dry_run=False, ltps={"AAA": 100.0})
    clock = FakeClock(1000.0)
    res = asyncio.run(sess._work_entry_leg(
        broker, prof, "AAA", 1000, 100.0,
        now_fn=clock.now, sleep_fn=clock.sleep,
        volume_fn=lambda s: 100000,                # pov huge → freeze (300) governs
        deadline_ts=1000.0 + 100 * 20))
    assert res["status"] == "PLACED"
    assert res["qty"] == 1000 and res["shortfall"] == 0
    assert all(o.qty <= 300 for o in broker.placed)         # freeze cap per child
    assert sum(o.qty for o in broker.placed) == 1000
    row = _row(sess.session_id, "AAA")
    assert row is not None and row["status"] == "OPEN" and row["qty"] == 1000
    assert row["avg_price"] == pytest.approx(100.0, abs=0.5)


def test_work_entry_leg_partial_at_deadline_surfaces_shortfall(clean_positions):
    """A tight deadline stops the build partway: the position IS the filled qty and
    the SHORTFALL is surfaced (the pick is never dropped)."""
    cfg = _worked_cfg(iceberg_freeze_qty_default=300)
    sess = TradingSession.create(cfg, mode="live")
    prof = sess.config.broker_profiles[0]
    broker = MockBroker(profile=prof, dry_run=False, ltps={"BBB": 100.0})
    clock = FakeClock(1000.0)
    res = asyncio.run(sess._work_entry_leg(
        broker, prof, "BBB", 5000, 100.0,
        now_fn=clock.now, sleep_fn=clock.sleep,
        volume_fn=lambda s: 100000,
        deadline_ts=1000.0 + 3 * 20))               # only ~3 intervals of 300
    assert res["status"] == "PARTIAL"
    assert res["shortfall"] > 0 and res["qty"] < 5000
    row = _row(sess.session_id, "BBB")
    assert row["qty"] == res["qty"]                # position == what actually filled


# ══════════════════════════════════════════════════════════════════════════════
# DEFAULT-OFF BYTE-IDENTITY — execution_mode != 'worked' never routes to worked
# ══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def patched_brokers(monkeypatch):
    created = {}
    ltps = {"A": 100.0, "B": 200.0, "C": 50.0}

    def fake_build_client(profile, dry_run=True):
        mb = MockBroker(profile=profile, dry_run=False, ltps=dict(ltps))
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


def test_fire_entries_market_mode_is_one_shot_not_worked(clean_positions,
                                                         patched_brokers,
                                                         monkeypatch):
    """execution_mode="market" fires the UNCHANGED one-shot gather — the worked path
    is NEVER entered.
    MUTATION REVERT: change the `if execution_mode == 'worked'` guard in
    _fire_entries to always-true → a market session routes to _fire_entries_worked →
    the monkeypatched sentinel raises → this test FAILS (proves the guard gates it)."""
    def _boom(*a, **k):
        raise AssertionError("worked path taken for a non-worked session")
    monkeypatch.setattr(TradingSession, "_fire_entries_worked", _boom)

    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               execution_mode="market")
    sess = TradingSession.create(cfg, mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == 3                     # one-shot, all 3 placed at once
    assert "worked_building" not in res             # NOT the worked path


def test_fire_entries_worked_mode_spawns_background_build(clean_positions,
                                                          patched_brokers,
                                                          monkeypatch):
    """execution_mode="worked" routes _fire_entries to the worked path: it returns
    RUNNING + worked_building and spawns one _work_entry_leg task per leg (the build
    runs over the pacing window, not inline).
    MUTATION REVERT: remove the worked branch in _fire_entries → a worked session
    falls into the one-shot gather → `worked_building` absent → this test FAILS."""
    called = []

    async def _stub(self, broker, prof, symbol, target_qty, ref_price, **kw):
        called.append((symbol, target_qty))
        return {"symbol": symbol, "status": "PLACED", "qty": target_qty,
                "worked": True}
    monkeypatch.setattr(TradingSession, "_work_entry_leg", _stub)

    seed_signals([("A", 1, 9.0, 100.0), ("B", 2, 8.0, 200.0),
                  ("C", 3, 7.0, 50.0)])
    cfg = TradingSessionConfig(total_allocated_capital=300000.0, top_n_stocks=3,
                               sizing_mode="equal", kill_switch_enabled=False,
                               execution_mode="worked", order_product="MIS",
                               per_position_gtt_enabled=False)
    sess = TradingSession.create(cfg, mode="paper")

    async def _go():
        res = await sess.start(when="now")
        tasks = list(getattr(sess, "_worked_entry_tasks", []))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        return res

    res = asyncio.run(_go())
    assert res["status"] == "RUNNING"
    assert res.get("worked_building") is True
    assert res.get("worked_legs") == 3
    assert sorted(s for s, _q in called) == ["A", "B", "C"]   # one leg-task each


# ══════════════════════════════════════════════════════════════════════════════
# SESSION EXIT — worked (paced) exit closes at full cover; routes only for worked
# ══════════════════════════════════════════════════════════════════════════════

def _register(sess, symbol, qty, *, avg=100.0, ltp=100.0):
    prof = sess.config.broker_profiles[0].profile_id
    sess.registry.register(symbol=symbol, broker_profile=prof, qty=qty,
                           avg_price=avg, product="MIS", instrument_type="EQ",
                           exchange="NSE", direction="long")
    sess.registry.update_ltp(symbol, ltp, broker_profile=prof)
    return prof


def test_work_and_confirm_exit_paces_children_full_cover_closes(clean_positions):
    """A large exit is PACED into freeze-capped children; the position CLOSES only
    at FULL cover (Σ children == total).
    MUTATION REVERT: in work_and_confirm_exit change the final close gate
    `if filled_total >= total:` to `if filled_total > 0:` → a partial books a CLOSED
    → with a tight deadline the row would close early; here (full cover) the
    n_children>1 pacing proof still stands, but the paired partial test below fails.
    Direct proof here: delete the worked branch entirely → NameError / no children."""
    cfg = _worked_cfg(iceberg_freeze_qty_default=300)
    sess = TradingSession.create(cfg, mode="live")
    prof = _register(sess, "A", 1000)
    broker = MockBroker(profile=sess.config.broker_profiles[0], dry_run=False,
                        ltps={"A": 100.0})
    clock = FakeClock(1000.0)
    res = asyncio.run(work_and_confirm_exit(
        session_id=sess.session_id, symbol="A", total_qty=1000, broker=broker,
        registry=sess.registry, close_reason="KILL_SWITCH", exec_cfg=cfg,
        broker_profile=prof, direction="long", instrument_type="EQ",
        kite_product="MIS", volume_fn=lambda s: 100000,
        now_fn=clock.now, sleep_fn=clock.sleep, deadline_ts=1000.0 + 100 * 20))
    assert res["status"] == "COMPLETE" and res["filled_qty"] == 1000
    assert all(q <= 300 for _s, q in broker.exits)          # freeze-capped children
    assert sum(q for _s, q in broker.exits) == 1000
    assert res["n_children"] >= 4                            # PACED, not one shot
    assert _row(sess.session_id, "A")["status"] == "CLOSED"


def test_work_and_confirm_exit_partial_at_deadline_leaves_remainder(clean_positions):
    """Deadline before full cover → the filled portion is booked, the REMAINDER is
    left OPEN + flagged EXIT_FAILED (guarded retry), NEVER a mis-stated full close.
    MUTATION REVERT: change the close gate `if filled_total >= total:` to
    `> 0:` → the row is CLOSED at the partial → `status == 'EXIT_FAILED'` FAILS."""
    cfg = _worked_cfg(iceberg_freeze_qty_default=300)
    sess = TradingSession.create(cfg, mode="live")
    prof = _register(sess, "B", 2000)
    broker = MockBroker(profile=sess.config.broker_profiles[0], dry_run=False,
                        ltps={"B": 100.0})
    clock = FakeClock(1000.0)
    res = asyncio.run(work_and_confirm_exit(
        session_id=sess.session_id, symbol="B", total_qty=2000, broker=broker,
        registry=sess.registry, close_reason="KILL_SWITCH", exec_cfg=cfg,
        broker_profile=prof, direction="long", instrument_type="EQ",
        kite_product="MIS", volume_fn=lambda s: 100000,
        now_fn=clock.now, sleep_fn=clock.sleep,
        deadline_ts=1000.0 + 2 * 20))               # only ~2 × 300 fit
    assert res["status"] == "EXIT_FAILED" and res.get("partial") is True
    assert res["filled_qty"] < 2000 and res["remaining_qty"] > 0
    row = _row(sess.session_id, "B")
    assert row["status"] == "EXIT_FAILED"           # remainder OPEN, not closed
    assert row["qty"] == res["remaining_qty"]


def test_exit_single_position_routes_to_worked_only_for_worked_mode(clean_positions,
                                                                    monkeypatch):
    """_exit_single_position paces (work_and_confirm_exit) for a worked-mode session
    but uses the UNCHANGED one-shot / iceberg path for any other mode.

    REASON UPDATED 2026-07-16: this test originally used "STOP_STOCK" as an ARBITRARY
    reason — it predates the pacing bypass and is about worked-vs-non-worked ROUTING,
    not about that reason. STOP_STOCK is now a capital-protecting BYPASS reason (it is
    the trail "STOP" relabelled for per-stock scope), so it no longer paces and would
    make this test assert the opposite of its own intent. Switched to "TARGET_HIT" —
    a reason that deliberately STILL paces — which preserves the original intent
    exactly. (The bypass reasons get their own fire/no-fire tests below.)

    MUTATION REVERT: remove the `if execution_mode == 'worked'` branch in
    _exit_single_position_inner → the worked session takes the one-shot path →
    the sentinel is never called → `called['n'] == 1` FAILS."""
    called = {"n": 0, "total": None}

    async def _stub(**kw):
        called["n"] += 1
        called["total"] = kw.get("total_qty")
        kw["registry"].mark_closed(kw["symbol"], kw["close_reason"],
                                   exit_price=100.0,
                                   broker_profile=kw.get("broker_profile"))
        return {"status": "COMPLETE", "filled_qty": kw["total_qty"],
                "symbol": kw["symbol"], "worked": True}

    import autotrade.monitoring.exit_poller as _ep
    monkeypatch.setattr(_ep, "work_and_confirm_exit", _stub)

    cfg = _worked_cfg(iceberg_freeze_qty_default=300)
    sess = TradingSession.create(cfg, mode="live")
    prof = _register(sess, "A", 1000)
    broker = MockBroker(profile=sess.config.broker_profiles[0], dry_run=False,
                        ltps={"A": 100.0}, net_positions={"A": 1000})
    sess.brokers = {prof: broker}
    pos = sess.registry.get_open_positions()[0]
    res = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="TARGET_HIT",
        brokers={prof: broker}, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="MIS", exec_cfg=cfg))
    assert called["n"] == 1                          # routed to the worked exit
    assert called["total"] == 1000                   # the clamped our_held qty
    assert res.get("worked") is True


# ══════════════════════════════════════════════════════════════════════════════
# PACING BYPASS — a capital-protecting exit (STOP / KILL_SWITCH) is NEVER paced
# ══════════════════════════════════════════════════════════════════════════════
#
# REAL INCIDENT (2026-07-16): LIVE session 1aeb11b8 took 213 SECONDS to exit a STOP
# under execution_mode=="worked" — 11 paced children at the 20s cadence (ledger tag
# "STOP:worked-child-0 .. -10") while the market moved against the book. Worked mode
# minimises impact on a large ENTRY; on a stop it merely prolongs the loss.


def _child_details(sid):
    """Every order-ledger `detail` for a session. The worked pacer writes
    "{close_reason}:worked-child-{idx}" per child (exit_poller), so an EMPTY
    worked-child list is POSITIVE EVIDENCE that nothing was paced."""
    with falcon_conn() as con:
        rows = con.execute(
            "SELECT detail FROM autotrade_order_events WHERE session_id=?",
            (sid,)).fetchall()
    return [r[0] for r in rows if r[0]]


def _worked_children(sid):
    return [d for d in _child_details(sid) if "worked-child-" in d]


def test_bypass_predicate_fires_for_urgent_and_deadline_bound_exits_only():
    """The pure predicate encodes THE PRINCIPLE: pacing is for ENTRIES; every URGENT
    (capital-protecting) or DEADLINE-BOUND exit fires as market. Everything with time
    to spend still paces.
    MUTATION REVERT: widen the match to a prefix/substring test (r.startswith)
    → "TIME_STOP" starts matching "STOP" → the no-fire asserts FAIL."""
    # FIRE — capital-protecting (urgent). STOP / STOP_STOCK / STOP_SEAT are ONE trail
    # decision under three labels; all three must bypass or "stops don't pace" is a
    # lie for 2 of 3.
    for r in ("STOP", "STOP_STOCK", "STOP_SEAT", "KILL_SWITCH",
              "stop_seat", " kill_switch "):
        assert wo.bypass_pacing_for_exit(r) is True, r
    # FIRE — deadline-bound: pacing a hard deadline is guaranteed failure.
    for r in ("MIS_SQUARE_OFF", "SQUARE_OFF"):
        assert wo.bypass_pacing_for_exit(r) is True, r
    # NO-FIRE — genuinely has time to spend, so impact control is worth having.
    # This is the LINE we are deliberately drawing.
    for r in ("TARGET_HIT", "MAX_HOLD_EXIT", "TRAIL_EXIT", "STEP_LOCK_EXIT",
              "FLOOR_EXIT", "EXIT_RETRY", "TIME_STOP", "GTT", "OPERATOR",
              "", None):
        assert wo.bypass_pacing_for_exit(r) is False, r


def test_stop_seat_is_in_the_exit_gate_vocabulary():
    """STOP_SEAT (session.py's per-seat relabel of the trail "STOP") must be a
    RECOGNISED gate reason. It was missing — it worked only because an unknown reason
    fails OPEN, i.e. it was correct BY LUCK while logging a spurious warning on every
    per-seat stop.
    The fail-OPEN default is deliberately UNCHANGED: an exit must never be blocked by
    vocabulary. This asserts the vocabulary is honest, not that it gates anything.
    MUTATION REVERT: remove "STOP_SEAT" from exit_gate.VALID_REASONS → FAILS."""
    from autotrade.exit_gate import VALID_REASONS
    for r in ("STOP", "STOP_STOCK", "STOP_SEAT", "KILL_SWITCH", "MIS_SQUARE_OFF",
              "SQUARE_OFF"):
        assert r in VALID_REASONS, r
    # The fail-open contract itself is untouched: an unknown reason still ALLOWS.
    assert "A_TOTALLY_UNKNOWN_REASON" not in VALID_REASONS


def test_stop_exit_under_worked_fires_one_market_exit_not_paced(clean_positions):
    """FIRE: a STOP under execution_mode=="worked" bypasses the pacer — ONE market
    exit for the FULL qty, and NO worked-child rows in the ledger.
    MUTATION REVERT: drop `and not _bypass_pacing` from the worked branch in
    _exit_single_position_inner → the STOP paces again → len(broker.exits) > 1 and
    the `_worked_children == []` assert FAILS (the 213s incident shape)."""
    cfg = _worked_cfg(iceberg_freeze_qty_default=300)
    sess = TradingSession.create(cfg, mode="live")
    prof = _register(sess, "A", 1000)
    broker = MockBroker(profile=sess.config.broker_profiles[0], dry_run=False,
                        ltps={"A": 100.0}, net_positions={"A": 1000})
    sess.brokers = {prof: broker}
    pos = sess.registry.get_open_positions()[0]
    res = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP",
        brokers={prof: broker}, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="MIS", exec_cfg=cfg))
    assert res.get("worked") is not True              # NOT the paced path
    assert len(broker.exits) == 1                     # ONE shot, no slicing
    assert broker.exits[0] == ("A", 1000)             # the FULL clamped qty
    assert _worked_children(sess.session_id) == []    # nothing was paced
    assert _row(sess.session_id, "A")["status"] == "CLOSED"


def test_kill_switch_exit_under_worked_is_not_paced(clean_positions):
    """FIRE: the portfolio kill switch under worked mode bypasses the pacer.
    close_reason="KILL_SWITCH" is the tag the MANUAL kill, LADDER_KILL and the
    PORTFOLIO_DAILY_LOSS_BREAKER all reach the exit path with (fire()'s default),
    so this covers every kill flavour.
    MUTATION REVERT: drop `and not _bypass_pacing` from the worked branch in
    kill_switch.fire → the kill paces → `_worked_children == []` FAILS."""
    cfg = _worked_cfg(iceberg_freeze_qty_default=300, kill_switch_enabled=True,
                      kill_switch_pct=0.01)
    sess = TradingSession.create(cfg, mode="live")
    prof = _register(sess, "K", 1000)
    broker = MockBroker(profile=sess.config.broker_profiles[0], dry_run=False,
                        ltps={"K": 100.0}, net_positions={"K": 1000})
    sess.brokers = {prof: broker}
    ks = KillSwitchExecutor(sess.session_id, cfg, {prof: broker}, sess.registry)
    out = asyncio.run(ks.fire("LOSS_LIMIT gross_return=-0.0200",
                              gross_return=-0.02))
    assert out["n_exited_ok"] == 1
    assert len(broker.exits) == 1                     # ONE market shot per name
    assert broker.exits[0] == ("K", 1000)
    assert _worked_children(sess.session_id) == []    # nothing was paced
    assert _row(sess.session_id, "K")["status"] == "CLOSED"


def test_worked_entry_still_paces_under_worked_mode(clean_positions):
    """NO-FIRE / REGRESSION GUARD: the bypass is EXIT-ONLY. A worked ENTRY still
    paces into POV/freeze-capped children — the validated worked v2 build engine is
    untouched (impact control on a BUILD is the whole point of worked mode).
    MUTATION REVERT: apply bypass_pacing_for_exit anywhere on the entry path →
    the build becomes one shot → `n_children >= 4` FAILS."""
    cfg = _worked_cfg(iceberg_freeze_qty_default=300)
    sess = TradingSession.create(cfg, mode="live")
    prof = sess.config.broker_profiles[0]
    broker = MockBroker(profile=prof, dry_run=False, ltps={"AAA": 100.0})
    clock = FakeClock(1000.0)
    res = asyncio.run(sess._work_entry_leg(
        broker, prof, "AAA", 1000, 100.0,
        now_fn=clock.now, sleep_fn=clock.sleep,
        volume_fn=lambda s: 100000,
        deadline_ts=1000.0 + 100 * 20))
    assert res["status"] == "PLACED"
    assert res["qty"] == 1000 and res["shortfall"] == 0
    assert res["n_children"] >= 4                     # STILL PACED (unregressed)
    assert clock.t > 1000.0                           # pacing time actually spent


def test_non_worked_session_stop_exit_is_byte_identical(clean_positions):
    """NO-FIRE: a DEFAULT (non-worked) session is untouched by the bypass — a STOP
    still takes the same one-shot market path it always did. The bypass only ever
    changes behaviour INSIDE execution_mode=="worked"."""
    cfg = _worked_cfg(execution_mode="market")
    sess = TradingSession.create(cfg, mode="live")
    prof = _register(sess, "M", 500)
    broker = MockBroker(profile=sess.config.broker_profiles[0], dry_run=False,
                        ltps={"M": 100.0}, net_positions={"M": 500})
    sess.brokers = {prof: broker}
    pos = sess.registry.get_open_positions()[0]
    res = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason="STOP",
        brokers={prof: broker}, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="MIS", exec_cfg=cfg))
    assert res.get("worked") is not True
    assert broker.exits == [("M", 500)]
    assert _worked_children(sess.session_id) == []
    assert _row(sess.session_id, "M")["status"] == "CLOSED"


def test_stop_kill_across_many_positions_is_one_round_not_n_cadence(clean_positions):
    """THE 213s SHAPE, pinned: N positions under worked+STOP flatten in ONE round of
    market orders (one per name) — NOT N × the 20s pacing cadence. The live incident
    was 11 children × ~20.5s = 213s for a SINGLE name; this asserts the whole basket
    now costs exactly one order each and ZERO paced children.
    MUTATION REVERT: restore pacing on the kill path → each name works into multiple
    freeze-capped children → `len(broker.exits) == 5` FAILS (becomes ~20)."""
    cfg = _worked_cfg(iceberg_freeze_qty_default=300, kill_switch_enabled=True,
                      kill_switch_pct=0.01, top_n_stocks=5)
    sess = TradingSession.create(cfg, mode="live")
    syms = ["S1", "S2", "S3", "S4", "S5"]
    for s in syms:
        prof = _register(sess, s, 1000)           # 1000 > freeze 300 → would pace
    broker = MockBroker(profile=sess.config.broker_profiles[0], dry_run=False,
                        ltps={s: 100.0 for s in syms},
                        net_positions={s: 1000 for s in syms})
    sess.brokers = {prof: broker}
    ks = KillSwitchExecutor(sess.session_id, cfg, {prof: broker}, sess.registry)
    out = asyncio.run(ks.fire("LOSS_LIMIT gross_return=-0.0200",
                              gross_return=-0.02, close_reason="STOP"))
    assert out["n_exited_ok"] == 5
    # ONE market order per name — a single round, no cadence, no slicing.
    assert len(broker.exits) == 5
    assert sorted(broker.exits) == sorted([(s, 1000) for s in syms])
    assert _worked_children(sess.session_id) == []
    for s in syms:
        assert _row(sess.session_id, s)["status"] == "CLOSED"


# ── EXPANDED SCOPE (operator-approved): every URGENT / DEADLINE-BOUND exit ─────


@pytest.mark.parametrize("reason", ["STOP_STOCK", "STOP_SEAT", "MIS_SQUARE_OFF",
                                    "SQUARE_OFF"])
def test_newly_bypassed_reason_fires_one_market_exit_not_paced(clean_positions,
                                                               reason):
    """FIRE: each newly-approved reason bypasses the pacer under worked mode — ONE
    market exit for the FULL qty, ZERO worked-child ledger rows.
      STOP_STOCK / STOP_SEAT — the SAME trail "STOP" relabelled (capital-protecting).
      MIS_SQUARE_OFF         — deadline-bound; paced it overruns the broker's ~15:20.
      SQUARE_OFF             — deadline-bound; paced its window is 0s → zero orders.
    MUTATION REVERT: drop any of these from PACING_BYPASS_EXIT_REASONS → that param
    paces → len(broker.exits) > 1 and `_worked_children == []` FAIL."""
    cfg = _worked_cfg(iceberg_freeze_qty_default=300)
    sess = TradingSession.create(cfg, mode="live")
    prof = _register(sess, "A", 1000)
    broker = MockBroker(profile=sess.config.broker_profiles[0], dry_run=False,
                        ltps={"A": 100.0}, net_positions={"A": 1000})
    sess.brokers = {prof: broker}
    pos = sess.registry.get_open_positions()[0]
    res = asyncio.run(_exit_single_position(
        session_id=sess.session_id, position=pos, reason=reason,
        brokers={prof: broker}, registry=sess.registry,
        gtt_manager=sess.gtt_manager, kite_product="MIS", exec_cfg=cfg))
    assert res.get("worked") is not True              # NOT the paced path
    assert len(broker.exits) == 1                     # ONE shot, no slicing
    assert broker.exits[0] == ("A", 1000)             # the FULL clamped qty
    assert _worked_children(sess.session_id) == []    # nothing was paced
    assert _row(sess.session_id, "A")["status"] == "CLOSED"


@pytest.mark.parametrize("reason", ["TARGET_HIT", "MAX_HOLD_EXIT"])
def test_target_and_max_hold_STILL_pace_under_worked(clean_positions, reason):
    """NO-FIRE / THE LINE WE ARE DRAWING: a TARGET_HIT / MAX_HOLD_EXIT is neither
    urgent nor deadline-bound — the position is fine and there IS time to spend, so
    impact control is genuinely worth having. These MUST still pace.
    This is the explicit regression guard on the bypass NOT over-reaching.
    MUTATION REVERT: add "TARGET_HIT"/"MAX_HOLD_EXIT" to PACING_BYPASS_EXIT_REASONS
    → the paced sentinel is never called → `called["n"] == 1` FAILS."""
    called = {"n": 0, "reason": None}

    async def _stub(**kw):
        called["n"] += 1
        called["reason"] = kw.get("close_reason")
        kw["registry"].mark_closed(kw["symbol"], kw["close_reason"],
                                   exit_price=100.0,
                                   broker_profile=kw.get("broker_profile"))
        return {"status": "COMPLETE", "filled_qty": kw["total_qty"],
                "symbol": kw["symbol"], "worked": True}

    import autotrade.monitoring.exit_poller as _ep
    _orig = _ep.work_and_confirm_exit
    _ep.work_and_confirm_exit = _stub
    try:
        cfg = _worked_cfg(iceberg_freeze_qty_default=300)
        sess = TradingSession.create(cfg, mode="live")
        prof = _register(sess, "A", 1000)
        broker = MockBroker(profile=sess.config.broker_profiles[0], dry_run=False,
                            ltps={"A": 100.0}, net_positions={"A": 1000})
        sess.brokers = {prof: broker}
        pos = sess.registry.get_open_positions()[0]
        res = asyncio.run(_exit_single_position(
            session_id=sess.session_id, position=pos, reason=reason,
            brokers={prof: broker}, registry=sess.registry,
            gtt_manager=sess.gtt_manager, kite_product="MIS", exec_cfg=cfg))
    finally:
        _ep.work_and_confirm_exit = _orig
    assert called["n"] == 1                 # STILL routed to the PACED worked exit
    assert called["reason"] == reason
    assert res.get("worked") is True


def test_exit_retry_of_a_stop_STILL_PACES_known_gap(clean_positions):
    """KNOWN GAP, PINNED (reported to the operator, deliberately NOT fixed here).

    The EXIT_FAILED retry sweep (session.py) passes reason="EXIT_RETRY", which ERASES
    the ORIGINAL reason — so the retry of a failed STOP is STILL PACED, at exactly the
    moment we are already in trouble. The original is destroyed TWICE by the failure
    path: registry.mark_exit_failed overwrites close_reason with "EXIT_FAILED:
    {error}", and the gate release NULLs exit_initiated_by (which claim_exit_session
    had set to the reason). A clean fix needs a persisted original-reason field
    threaded through ~18 mark_exit_failed call sites whose only natural chokepoint
    (registry.mark_exit_failed) is outside this change's scope.

    This test documents the CURRENT (wrong) behaviour so the gap is visible and
    cannot be forgotten. WHEN THE GAP IS FIXED THIS TEST SHOULD FAIL — that is
    intended: flip it to assert the STOP is NOT paced."""
    assert wo.bypass_pacing_for_exit("EXIT_RETRY") is False   # the gap, pinned
    called = {"n": 0}

    async def _stub(**kw):
        called["n"] += 1
        kw["registry"].mark_closed(kw["symbol"], kw["close_reason"],
                                   exit_price=100.0,
                                   broker_profile=kw.get("broker_profile"))
        return {"status": "COMPLETE", "filled_qty": kw["total_qty"],
                "symbol": kw["symbol"], "worked": True}

    import autotrade.monitoring.exit_poller as _ep
    _orig = _ep.work_and_confirm_exit
    _ep.work_and_confirm_exit = _stub
    try:
        cfg = _worked_cfg(iceberg_freeze_qty_default=300)
        sess = TradingSession.create(cfg, mode="live")
        prof = _register(sess, "A", 1000)
        broker = MockBroker(profile=sess.config.broker_profiles[0], dry_run=False,
                            ltps={"A": 100.0}, net_positions={"A": 1000})
        sess.brokers = {prof: broker}
        pos = sess.registry.get_open_positions()[0]
        asyncio.run(_exit_single_position(
            session_id=sess.session_id, position=pos, reason="EXIT_RETRY",
            brokers={prof: broker}, registry=sess.registry,
            gtt_manager=sess.gtt_manager, kite_product="MIS", exec_cfg=cfg))
    finally:
        _ep.work_and_confirm_exit = _orig
    assert called["n"] == 1        # PACED — the known gap (see docstring)


def test_kill_switch_mis_square_off_across_basket_is_one_round(clean_positions):
    """The MIS_SQUARE_OFF flatten through kill_switch.fire (the real square-off path)
    is ONE market order per name — not N x the 20s cadence. Paced, this exit's
    deadline computes to ~15:20:30 vs the broker's ~15:20 auto-square (a ~30s
    overrun), which would end in a broker force-close at an arbitrary price.
    MUTATION REVERT: drop MIS_SQUARE_OFF from PACING_BYPASS_EXIT_REASONS → each name
    works into freeze-capped children → `len(broker.exits) == 4` FAILS."""
    cfg = _worked_cfg(iceberg_freeze_qty_default=300, top_n_stocks=4)
    sess = TradingSession.create(cfg, mode="live")
    syms = ["Q1", "Q2", "Q3", "Q4"]
    for s in syms:
        prof = _register(sess, s, 1000)           # 1000 > freeze 300 -> would pace
    broker = MockBroker(profile=sess.config.broker_profiles[0], dry_run=False,
                        ltps={s: 100.0 for s in syms},
                        net_positions={s: 1000 for s in syms})
    sess.brokers = {prof: broker}
    ks = KillSwitchExecutor(sess.session_id, cfg, {prof: broker}, sess.registry)
    out = asyncio.run(ks.fire("MIS defensive square-off",
                              close_reason="MIS_SQUARE_OFF"))
    assert out["n_exited_ok"] == 4
    assert len(broker.exits) == 4                 # ONE market order per name
    assert sorted(broker.exits) == sorted([(s, 1000) for s in syms])
    assert _worked_children(sess.session_id) == []
    for s in syms:
        assert _row(sess.session_id, s)["status"] == "CLOSED"
