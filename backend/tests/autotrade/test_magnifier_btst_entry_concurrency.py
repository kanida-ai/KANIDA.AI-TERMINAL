"""ENTRY-LATENCY fix lock (2026-07-20): the Magnifier + BTST split-entry fire
methods must place their leg-1 and leg-2 orders CONCURRENTLY (asyncio.gather under
the shared entry-rate semaphore), NOT in a sequential await-loop.

Root cause of the live bug: `_fire_magnifier_initial` / `_fire_btst_initial` (and
the leg-2 completions) placed orders in `for pick: await self._place_one(...)` —
each order awaited before the next started, so N legs cost ~N× the broker round-
trip (measured live: Magnifier 31.8s / BTST 61.3s). The standard `_fire_entries`
path already gathers; these split-entry methods did not.

The TIMING tests are MUTATION / REVERT PROOF: a fake broker whose `place_order`
sleeps `DELAY` is placed for N picks. Concurrent placement finishes in ~1×DELAY;
a sequential await-loop would take ~N×DELAY. The assertions bound the wall clock
BELOW the sequential time, so re-introducing the `for pick: await` loop makes them
FAIL (proven not vacuous by `test_timing_threshold_would_catch_sequential`).

Safety locks alongside: concurrent placement preserves per-pick 50/50 sizing, the
per-name leg-2 plan, the blended-cost leg-2 average, the ENTRY_PARTIAL / zero-
placement FAIL decisions, and the anti-double-fill guarantee (each pick placed
EXACTLY once — the gather never double-places a name).

Paper / dry-run throughout (patched MockBrokers; no real Kite).
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

import autotrade.broker.router as router_mod
import autotrade.session as sess_mod
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession, set_fake_now
from autotrade.ladder import BTST_TOP_N, BTST_MAX_HOLD, BTST_STOP_PCT
from tests.autotrade.conftest import seed_signals
from tests.autotrade.mock_broker import MockBroker
from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_NOW = datetime(2026, 6, 25, 10, 0, 0, tzinfo=IST)

# Six names → a wide sequential-vs-concurrent gap. Priced so equal 1× CNC and 5×
# MIS both size an even, splittable qty (margin = price/5 for MIS; == price for
# CNC — the MockBroker margin path is used for MIS, the cash path for CNC).
SYMS = ["A", "B", "C", "D", "E", "F"]
LTPS = {s: 100.0 for s in SYMS}
MIS_MARGINS = {s: 20.0 for s in SYMS}     # 5×
CNC_MARGINS = {s: 100.0 for s in SYMS}    # 1×
DELAY = 0.4                                # per-order place_order sleep
N = len(SYMS)


class SlowBroker(MockBroker):
    """MockBroker whose place_order sleeps DELAY before returning — so a
    sequential await-loop takes ~N×DELAY and a gather takes ~1×DELAY."""

    def __init__(self, *args, place_delay: float = DELAY, **kwargs):
        super().__init__(*args, **kwargs)
        self._place_delay = place_delay

    async def place_order(self, order):
        if self._place_delay:
            await asyncio.sleep(self._place_delay)
        return await super().place_order(order)


@pytest.fixture(autouse=True)
def _frozen_open_clock():
    set_fake_now(OPEN_NOW)
    yield
    set_fake_now(None)


def _make_broker_fixture(monkeypatch, margins, block_orders=None,
                         block_symbols=None):
    created = {}

    def fake_build_client(profile, dry_run=True):
        mb = SlowBroker(profile=profile, dry_run=False, ltps=dict(LTPS),
                        margins=dict(margins), margins_available=True,
                        block_orders=block_orders,
                        block_symbols=block_symbols)
        created[profile.profile_id] = mb
        return mb

    monkeypatch.setattr(router_mod, "build_client", fake_build_client)
    monkeypatch.setattr(sess_mod, "build_client", fake_build_client)
    return created


@pytest.fixture
def all_high_tier(monkeypatch):
    def _all(picks, high_tier):
        return list(picks), {p.symbol: "GOLD" for p in picks}
    monkeypatch.setattr(sess_mod, "_magnifier_high_tier_filter", _all)


def _seed():
    seed_signals([(s, i + 1, 9.0 - i, 100.0) for i, s in enumerate(SYMS)])


def _mag_cfg(capital=600000.0):
    return TradingSessionConfig(
        total_allocated_capital=capital, strategy="intraday_magnifier",
        order_product="MIS", instrument_type="EQ", direction="long",
        top_n_stocks=15, sizing_mode="equal",
        arm_pct=0.06, floor_pct=0.02, trail_giveback_pct=0.05, stop_pct=0.03,
        magnifier_second_leg_offset_sec=600)


def _btst_cfg(capital=600000.0):
    return TradingSessionConfig(
        total_allocated_capital=capital, strategy="btst_oscillator",
        order_product="CNC", instrument_type="EQ", direction="long",
        top_n_stocks=BTST_TOP_N, sizing_mode="equal",
        stop_pct=BTST_STOP_PCT, square_off_enabled=False,
        max_hold_sessions=BTST_MAX_HOLD, trail_step_lock_enabled=False,
        magnifier_second_leg_offset_sec=600)


def _positions(session_id):
    with falcon_conn() as con:
        return {r["symbol"]: dict(r) for r in con.execute(
            "SELECT * FROM autotrade_positions WHERE session_id=?",
            (session_id,)).fetchall()}


def _placed_symbol_counts(brokers):
    counts = {}
    for mb in brokers.values():
        for o in mb.placed:
            counts[o.symbol] = counts.get(o.symbol, 0) + 1
    return counts


# The concurrency budget: wall clock must be BELOW the sequential time. With N
# legs each sleeping DELAY: sequential ~= N*DELAY, concurrent ~= DELAY. The
# threshold sits well under N*DELAY so a revert to the await-loop fails, and well
# over 1*DELAY so a healthy concurrent run passes even under CI jitter.
_SEQ_TIME = N * DELAY
_THRESHOLD = _SEQ_TIME * 0.5      # 1.2s for N=6, DELAY=0.4 (seq=2.4s, conc~0.4-0.85s)


# ── 1. MAGNIFIER leg-1 placement is CONCURRENT ──────────────────────────────────

def test_magnifier_leg1_concurrent(clean_positions, monkeypatch, all_high_tier):
    brokers = _make_broker_fixture(monkeypatch, MIS_MARGINS)
    _seed()
    sess = TradingSession.create(_mag_cfg(), mode="paper")
    t0 = time.monotonic()
    res = asyncio.run(sess.start(when="now"))
    elapsed = time.monotonic() - t0
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == N
    assert elapsed < _THRESHOLD, (
        f"leg-1 fire took {elapsed:.3f}s (>= {_THRESHOLD:.3f}s) — looks SEQUENTIAL "
        f"(sequential ~= {_SEQ_TIME:.2f}s for {N} legs @ {DELAY}s)")
    # Each name placed EXACTLY once (anti-double-fill under gather).
    assert _placed_symbol_counts(brokers) == {s: 1 for s in SYMS}


# ── 2. MAGNIFIER leg-2 completion is CONCURRENT ─────────────────────────────────

def test_magnifier_leg2_concurrent(clean_positions, monkeypatch, all_high_tier):
    brokers = _make_broker_fixture(monkeypatch, MIS_MARGINS)
    _seed()
    sess = TradingSession.create(_mag_cfg(), mode="paper")
    asyncio.run(sess.start(when="now"))
    t0 = time.monotonic()
    res2 = asyncio.run(sess.complete_magnifier_entry())
    elapsed = time.monotonic() - t0
    assert res2["magnifier_entry_complete"] is True
    assert res2["n_placed"] == N
    assert elapsed < _THRESHOLD, (
        f"leg-2 completion took {elapsed:.3f}s — looks SEQUENTIAL "
        f"(sequential ~= {_SEQ_TIME:.2f}s)")
    # Two placements per name total (leg-1 + leg-2), none tripled.
    assert _placed_symbol_counts(brokers) == {s: 2 for s in SYMS}


# ── 3. BTST leg-1 placement is CONCURRENT ───────────────────────────────────────

def test_btst_leg1_concurrent(clean_positions, monkeypatch, all_high_tier):
    brokers = _make_broker_fixture(monkeypatch, CNC_MARGINS)
    _seed()
    sess = TradingSession.create(_btst_cfg(), mode="paper")
    t0 = time.monotonic()
    res = asyncio.run(sess.start(when="now"))
    elapsed = time.monotonic() - t0
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == N
    assert elapsed < _THRESHOLD, (
        f"BTST leg-1 fire took {elapsed:.3f}s — looks SEQUENTIAL "
        f"(sequential ~= {_SEQ_TIME:.2f}s)")
    assert _placed_symbol_counts(brokers) == {s: 1 for s in SYMS}


# ── 4. BTST leg-2 completion is CONCURRENT ──────────────────────────────────────

def test_btst_leg2_concurrent(clean_positions, monkeypatch, all_high_tier):
    brokers = _make_broker_fixture(monkeypatch, CNC_MARGINS)
    _seed()
    sess = TradingSession.create(_btst_cfg(), mode="paper")
    asyncio.run(sess.start(when="now"))
    t0 = time.monotonic()
    res2 = asyncio.run(sess.complete_btst_entry())
    elapsed = time.monotonic() - t0
    assert res2["magnifier_entry_complete"] is True
    assert res2["n_placed"] == N
    assert elapsed < _THRESHOLD, (
        f"BTST leg-2 completion took {elapsed:.3f}s — looks SEQUENTIAL "
        f"(sequential ~= {_SEQ_TIME:.2f}s)")
    assert _placed_symbol_counts(brokers) == {s: 2 for s in SYMS}


# ── 5. META: the timing threshold WOULD catch a sequential run (not vacuous) ────

def test_timing_threshold_would_catch_sequential():
    """Prove the timing assertion is meaningful: a genuine SEQUENTIAL placement of
    N legs each sleeping DELAY exceeds the threshold. This is what a revert to the
    `for pick: await self._place_one(...)` loop would produce."""
    async def _sequential():
        t0 = time.monotonic()
        for _ in range(N):
            await asyncio.sleep(DELAY)
        return time.monotonic() - t0

    seq_elapsed = asyncio.run(_sequential())
    assert seq_elapsed >= _THRESHOLD, (
        f"sequential {N}×{DELAY}s = {seq_elapsed:.3f}s should exceed the "
        f"{_THRESHOLD:.3f}s threshold the concurrency tests assert under")


# ── 6. SAFETY: concurrent split entry preserves sizing + leg-2 plan + blend ─────

def test_concurrent_split_entry_correct_sizing_and_blend(clean_positions,
                                                         monkeypatch,
                                                         all_high_tier):
    """Under the gather, EVERY name is sized 50% at leg-1 and blended at leg-2 —
    no name dropped, mis-sized, or double-counted."""
    brokers = _make_broker_fixture(monkeypatch, MIS_MARGINS)
    _seed()
    sess = TradingSession.create(_mag_cfg(), mode="paper")
    asyncio.run(sess.start(when="now"))
    # leg-1: half the 5× target per name (600000/6 = 100000 slice; 100000/20 =
    # 5000 target; leg-1 = 2500).
    pos1 = _positions(sess.session_id)
    assert set(pos1) == set(SYMS)
    assert all(pos1[s]["qty"] == 2500 for s in SYMS)
    # Move the market up so leg-2 fills at a different price → the blend is provable.
    for mb in brokers.values():
        for s in SYMS:
            mb.set_ltp(s, 120.0)
    res2 = asyncio.run(sess.complete_magnifier_entry())
    assert res2["magnifier_entry_complete"] is True
    pos2 = _positions(sess.session_id)
    assert set(pos2) == set(SYMS)
    # Full target qty (both legs averaged into ONE row) + blended avg (2500@100 +
    # 2500@120)/5000 = 110 for every name.
    for s in SYMS:
        assert pos2[s]["qty"] == 5000
        assert abs(pos2[s]["avg_price"] - 110.0) < 1e-6
    # Exactly ONE position row per name (no double-registration under gather).
    assert len(pos2) == N


# ── 7. SAFETY: a PARTIAL leg-1 under gather → correct n_filled, no double-place ──

def test_concurrent_partial_leg1_no_double_place(clean_positions, monkeypatch,
                                                 all_high_tier):
    """One name is broker-blocked; the gather must still place the other N-1 (each
    exactly once), yield the correct n_placed, and keep the session RUNNING
    (ENTRY_PARTIAL), never marking FAILED or double-placing a sibling."""
    brokers = _make_broker_fixture(monkeypatch, CNC_MARGINS,
                                   block_symbols={"C"})
    _seed()
    sess = TradingSession.create(_mag_cfg(), mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "RUNNING"
    assert res["n_placed"] == N - 1          # C blocked
    pos = _positions(sess.session_id)
    assert set(pos) == set(SYMS) - {"C"}     # C not registered
    # Every name (incl. the blocked C) was ATTEMPTED exactly once — no blind retry
    # / double-place of any sibling in the gather.
    assert _placed_symbol_counts(brokers) == {s: 1 for s in SYMS}


# ── 8. SAFETY: a fully-blocked leg-1 under gather → FAIL LOUDLY, no second leg ───

def test_concurrent_zero_placement_fails_loud(clean_positions, monkeypatch,
                                              all_high_tier):
    brokers = _make_broker_fixture(monkeypatch, CNC_MARGINS,
                                   block_orders="cert blocked (uncertified)")
    _seed()
    sess = TradingSession.create(_mag_cfg(), mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "FAILED"
    assert res["n_placed"] == 0
    assert _positions(sess.session_id) == {}
    # Each name attempted exactly once even on the all-fail path.
    assert _placed_symbol_counts(brokers) == {s: 1 for s in SYMS}


def test_concurrent_btst_zero_placement_fails_loud(clean_positions, monkeypatch,
                                                   all_high_tier):
    brokers = _make_broker_fixture(monkeypatch, CNC_MARGINS,
                                   block_orders="cert blocked (uncertified)")
    _seed()
    sess = TradingSession.create(_btst_cfg(), mode="paper")
    res = asyncio.run(sess.start(when="now"))
    assert res["status"] == "FAILED"
    assert res["n_placed"] == 0
    assert _positions(sess.session_id) == {}
    assert _placed_symbol_counts(brokers) == {s: 1 for s in SYMS}
