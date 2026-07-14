"""WORKED-ORDER v2 — VWAP-CURVE PACING + ADAPTIVE PARTICIPATION (paper / mock).

v1 paces FLAT; real intraday volume is U-SHAPED (heavy open + close, thin midday).
v2 paces to a per-symbol intraday VOLUME PROFILE (normalized 5-min buckets, built
from recent-N days of ohlc_1min) and ADAPTS to realized fills — filling MORE when
the curve is deep, LESS when thin, while NEVER exceeding a HARD participation
ceiling and preserving EVERY v1 safety guard (freeze cap, TWAP progress floor,
fail-closed, partial accounting). When v2 is OFF (or a symbol has no profile) the
child sizer is EXACTLY v1's flat POV — byte-identical.

Each test PASSES with the code and FAILS on the stated MUTATION REVERT. All paper /
pure — NO real Kite, NO real orders. The pacing clock is INJECTED (a FakeClock and
an identity clock_fn) so the profile's time-of-day coordinate lines up with the
test window deterministically (profile open_sec == the window start, so loop
timestamps ARE the profile coordinate).
"""
from __future__ import annotations

import asyncio

import pytest

from autotrade.config import TradingSessionConfig
from autotrade.execution import worked_order as wo


# A U-shaped 10-bucket profile over the TEST window [1000, 2000] (bucket_sec=100 so
# the 10 buckets exactly span the window). clock_fn=identity → a loop timestamp IS
# the profile's seconds-of-day coordinate. Deterministic, no wall clock.
U_BUCKETS = [10, 6, 3, 2, 1, 1, 2, 3, 6, 10]
WIN_START, WIN_END, BUCKET = 1000.0, 2000.0, 100.0


def _profile(buckets):
    return wo.IntradayVolumeProfile(buckets=list(buckets), open_sec=WIN_START,
                                    bucket_sec=BUCKET)


def _sizer(buckets, max_participation_pct=0.5):
    return wo.VwapScheduleSizer(profile=_profile(buckets),
                                max_participation_pct=max_participation_pct,
                                clock_fn=lambda t: t)


class FakeClock:
    """Injected pacing clock: now() returns the current epoch, sleep(s) ADVANCES it
    by s (no real delay)."""

    def __init__(self, start: float = WIN_START):
        self.t = float(start)

    def now(self) -> float:
        return self.t

    async def sleep(self, s: float) -> None:
        self.t += float(s)


def _run(child_sizer, *, target=10000, interval=100.0, vol=1e9,
         participation_pct=0.10, freeze_cap=None, deadline=WIN_END):
    """Run work_order over the window and return the per-child requested qtys."""
    clock = FakeClock(WIN_START)
    placed = []

    async def pc(*, idx, qty, recent_volume):
        placed.append(qty)
        return {"filled_qty": qty, "avg_price": 100.0, "status": "OK"}

    parent = wo.WorkedParent(
        symbol="X", side="BUY", target_qty=target, interval_sec=interval,
        participation_pct=participation_pct, freeze_cap=freeze_cap,
        min_child_qty=1, max_children=999, deadline_ts=deadline)
    asyncio.run(wo.work_order(
        parent, place_child=pc, volume_fn=lambda s: vol,
        now_fn=clock.now, sleep_fn=clock.sleep, child_sizer=child_sizer))
    return placed


def _child_on_schedule(profile, t_now, *, target=10000, vol=1e9, interval=100.0,
                       participation_pct=0.10, max_participation_pct=0.5):
    """The v2 child at t_now assuming we are EXACTLY on schedule so far (isolates the
    profile's per-interval pacing signal from catch-up)."""
    frac = profile.cum_fraction
    denom = frac(WIN_END) - frac(WIN_START)
    filled = int(round((frac(t_now) - frac(WIN_START)) / denom * target))
    il = wo.intervals_left(t_now, WIN_END, interval)
    return wo._vwap_child_qty(
        profile_frac=frac, target_qty=target, filled=filled,
        remaining=target - filled, n_intervals=il, recent_volume=vol,
        now_ts=t_now, interval_sec=interval, window_start_ts=WIN_START,
        deadline_ts=WIN_END, participation_pct=participation_pct,
        max_participation_pct=max_participation_pct, freeze_cap=None,
        min_child_qty=1)


# ══════════════════════════════════════════════════════════════════════════════
# PROFILE — normalization + cumulative interpolation
# ══════════════════════════════════════════════════════════════════════════════

def test_profile_normalizes_and_cum_fraction_is_monotone_0_to_1():
    """A profile normalizes its buckets to Σ==1 and cum_fraction is a monotone 0→1
    curve, interpolated within a bucket. A flat profile's cum is linear.
    MUTATION REVERT: in IntradayVolumeProfile.cum_fraction drop the in-bucket
    `+ self.buckets[b]*within` term → the curve becomes a step function → the
    midpoint interpolation assert (cum(1050) == 0.5*share0) FAILS."""
    p = _profile(U_BUCKETS)
    assert p.valid and abs(sum(p.buckets) - 1.0) < 1e-9
    assert p.cum_fraction(WIN_START) == 0.0            # at/before open → 0
    assert p.cum_fraction(WIN_END) == pytest.approx(1.0)
    assert p.cum_fraction(WIN_END + 5000) == 1.0       # past close clamps to 1
    # first bucket share = 10/44; the midpoint of bucket 0 is half of it
    assert p.cum_fraction(1050.0) == pytest.approx(0.5 * (10 / 44))
    # strictly monotone non-decreasing across the window
    prev = -1.0
    for t in range(int(WIN_START), int(WIN_END) + 1, 25):
        c = p.cum_fraction(float(t))
        assert c >= prev - 1e-12
        prev = c
    flat = _profile([1] * 10)
    assert flat.cum_fraction(1500.0) == pytest.approx(0.5)   # flat → linear


# ══════════════════════════════════════════════════════════════════════════════
# VWAP PACING — front/back-loaded to the curve vs flat v1
# ══════════════════════════════════════════════════════════════════════════════

def test_vwap_pacing_front_and_back_loaded_vs_flat():
    """On-schedule, the v2 per-interval child TRACES the volume curve: the heavy
    OPEN and heavy CLOSE buckets get MORE than the thin MIDDAY bucket (U-shape),
    whereas a FLAT profile gives uniform children == v1 flat pacing.
    MUTATION REVERT: in _vwap_child_qty replace the profile-driven
    `frac_next = (profile_frac(look) - c_start)/denom` with a flat linear schedule
    `frac_next = (look - window_start)/(deadline - window_start)` (ignore the
    profile) → open == mid == close → the `open > mid` / `close > mid` U-shape
    asserts FAIL (schedule collapses to v1 flat)."""
    p = _profile(U_BUCKETS)
    open_c = _child_on_schedule(p, WIN_START)          # heavy open bucket
    mid_c = _child_on_schedule(p, 1400.0)              # thin midday bucket
    close_c = _child_on_schedule(p, 1900.0)            # heavy close bucket
    assert open_c > mid_c                              # front-loaded to the open
    assert close_c > mid_c                             # back-loaded to the close
    assert open_c == 2273 and close_c == 2273          # 10/44 of the order per end
    # FLAT profile → uniform children (matches v1 flat POV)
    fp = _profile([1] * 10)
    assert (_child_on_schedule(fp, WIN_START)
            == _child_on_schedule(fp, 1400.0)
            == _child_on_schedule(fp, 1900.0) == 1000)


def test_vwap_full_run_front_loads_and_sums_to_target():
    """End-to-end: a U-profile work_order front-loads the FIRST (heavy-open) child
    far above the flat-v1 child and the children still sum EXACTLY to the target
    (never over/under the target).
    MUTATION REVERT: in work_order ignore `child_sizer` (always call
    next_child_qty) → the U front-load disappears (child[0] == 1000, the flat POV) →
    the `placed_vwap[0] > placed_flat[0]` assert FAILS."""
    # vol=10000 so v1's POV cap (0.10×vol=1000) binds at ~1000/interval → v1 paces
    # FLAT [1000]×10; the U sizer's adaptive participation lifts the heavy-open child.
    placed_vwap = _run(_sizer(U_BUCKETS), vol=10000)
    placed_flat = _run(None, vol=10000)                # pure v1 flat POV
    assert sum(placed_vwap) == 10000                   # never over/under the target
    assert placed_vwap[0] > placed_flat[0]             # front-loaded into open depth
    assert placed_vwap[0] == 2273 and placed_flat[0] == 1000
    assert max(placed_vwap) == placed_vwap[0]          # the open child is the largest
    # a FLATTENED profile through the SAME sizer path collapses the U back to flat
    # (uniform ~1000/child, the heavy-open front-load GONE) — matching v1's shape (a
    # ±1 share drift is the schedule's ceil vs v1's floor; byte-identity to v1 is
    # covered by test_no_profile_sizer_declines, which takes the literal v1 path).
    placed_flatprof = _run(_sizer([1] * 10), vol=10000)
    assert sum(placed_flatprof) == 10000
    assert all(abs(q - 1000) <= 1 for q in placed_flatprof)   # uniform, no front-load
    assert placed_flatprof[0] < placed_vwap[0]                 # U collapsed to flat


# ══════════════════════════════════════════════════════════════════════════════
# ADAPTIVE PARTICIPATION — catch up when behind, NEVER exceed the hard ceiling
# ══════════════════════════════════════════════════════════════════════════════

def test_vwap_adaptive_catchup_leans_up_but_respects_hard_ceiling():
    """Seeded WAY behind schedule, the adaptive POV cap rises ABOVE the normal
    participation cap toward the ceiling to catch up — but is HARD-CLAMPED at
    worked_max_participation_pct × recent_volume and never exceeds it (the TWAP
    floor is set below the ceiling here so the ceiling is the binding constraint).
    MUTATION REVERT: in _vwap_child_qty remove the ceiling clamp
    `eff_pct = min(max(needed_pct, base_pct), ceil_pct)` → `= max(needed_pct,
    base_pct)` → the POV cap explodes to needed_pct×vol → child == 500000 > 25000 →
    the `child <= ceiling` assert FAILS (participation blown past the safety cap)."""
    p = _profile(U_BUCKETS)
    frac = p.cum_fraction
    vol = 100_000.0
    base_cap = int(0.10 * vol)                          # normal impact cap = 10000
    ceiling = int(0.25 * vol)                           # hard ceiling      = 25000
    # WAY behind: filled 0 near the deadline; target huge; MANY intervals so the
    # TWAP floor (ceil(remaining/n) = 10000) sits BELOW the ceiling (25000).
    child = wo._vwap_child_qty(
        profile_frac=frac, target_qty=1_000_000, filled=0, remaining=1_000_000,
        n_intervals=100, recent_volume=vol, now_ts=1900.0, interval_sec=100.0,
        window_start_ts=WIN_START, deadline_ts=WIN_END, participation_pct=0.10,
        max_participation_pct=0.25, freeze_cap=None, min_child_qty=1)
    assert child == ceiling                             # leaned all the way to 0.25
    assert child > base_cap                             # rose above the normal cap
    assert child <= ceiling                             # NEVER past the hard ceiling
    # A LESS-behind state (needed_pct between base and ceiling) lands strictly
    # inside the band — the adaptation is graded, not a hard on/off jump. Here the
    # catch-up needs ~15% participation → child == 15000 (between 10000 and 25000).
    graded = wo._vwap_child_qty(
        profile_frac=frac, target_qty=1_000_000, filled=0, remaining=1_000_000,
        n_intervals=100, recent_volume=vol, now_ts=1000.0, interval_sec=100.0,
        window_start_ts=WIN_START, deadline_ts=WIN_END, participation_pct=0.10,
        max_participation_pct=0.25, freeze_cap=None, min_child_qty=1)
    # at now=open, schedule for interval 0 = 10/44 ≈ 0.227 of the order = 227272,
    # needed_pct = 2.27 → clamped to the 0.25 ceiling as well; assert within band.
    assert base_cap <= graded <= ceiling


def test_vwap_ahead_of_schedule_eases_off():
    """AHEAD of schedule (filled already past the scheduled cumulative) → the child
    target eases off to ~0, so only the TWAP progress floor keeps it moving (never
    negative, no oscillation). The ease-off comes from subtracting realized `filled`
    from the scheduled cumulative.
    MUTATION REVERT: in _vwap_child_qty drop the `- int(filled or 0)` fills-adaptation
    (`child_target = ceil(sched_cum_qty)`) → the sizer ignores that we are ahead and
    asks for the full scheduled slice (1000) instead of easing to the floor (167) →
    the `child == twap_floor(1000, 6)` assert FAILS."""
    p = _profile(U_BUCKETS)
    frac = p.cum_fraction
    # At t=1400 the schedule says ~50% done; seed filled=90% (far ahead).
    child = wo._vwap_child_qty(
        profile_frac=frac, target_qty=10000, filled=9000, remaining=1000,
        n_intervals=6, recent_volume=1e9, now_ts=1400.0, interval_sec=100.0,
        window_start_ts=WIN_START, deadline_ts=WIN_END, participation_pct=0.10,
        max_participation_pct=0.25, freeze_cap=None, min_child_qty=1)
    assert child >= 0
    # ease-off: the schedule gap is ~0, so only the TWAP floor (ceil(1000/6)=167)
    # keeps progress — far below what the heavy-curve would ask if behind.
    assert child == wo.twap_floor(1000, 6)


# ══════════════════════════════════════════════════════════════════════════════
# NO-PROFILE / OFF — byte-identical fallback to v1 flat POV
# ══════════════════════════════════════════════════════════════════════════════

def test_no_profile_sizer_declines_and_falls_back_to_v1():
    """A VwapScheduleSizer with NO profile (thin history) DECLINES (returns None)
    every interval → work_order runs EXACTLY v1 flat POV (byte-identical child
    sequence to child_sizer=None).
    MUTATION REVERT: in VwapScheduleSizer.__call__ delete the
    `if self.profile is None or not self.profile.valid: return None` guard → it
    dereferences profile.cum_fraction on None → AttributeError → the run RAISES →
    this test FAILS (the decline guard is what makes no-profile safe)."""
    no_prof = wo.VwapScheduleSizer(profile=None, max_participation_pct=0.25,
                                   clock_fn=lambda t: t)
    placed_decline = _run(no_prof)
    placed_v1 = _run(None)
    assert placed_decline == placed_v1                  # byte-identical to v1


def test_sizer_declines_when_no_time_deadline():
    """No time deadline (paced by max_children) → the VWAP sizer has no window to
    schedule against → it DECLINES → v1 flat POV governs.
    MUTATION REVERT: in VwapScheduleSizer.__call__ remove the
    `if ctx.deadline_ts is None: return None` guard → it calls _vwap_child_qty with
    deadline_ts=None → float(None) TypeError → the decline assert FAILS."""
    s = _sizer(U_BUCKETS)
    ctx = wo.SizerContext(remaining=100, n_intervals=5, recent_volume=1000,
                          now_ts=WIN_START, deadline_ts=None, filled=0,
                          target_qty=100, n_children=0)
    parent = wo.WorkedParent(symbol="X", side="BUY", target_qty=100)
    assert s(parent, ctx) is None


def test_make_vwap_sizer_off_by_default_and_when_disabled(monkeypatch):
    """make_vwap_sizer returns None unless BOTH execution_mode=='worked' AND
    worked_vwap_enabled — so v2 is INERT by default (→ v1 flat POV, byte-identical).
    A VALID profile is STUBBED so the OFF gate (not the absence of DB data) is what
    is under test: an inert config must short-circuit BEFORE the profile load.
    MUTATION REVERT: in make_vwap_sizer delete the
    `if not worked_vwap_enabled: return None` guard → the worked-but-vwap-OFF config
    reaches the stubbed profile and builds a sizer → the `is None` assert FAILS (v2
    would leak on by default)."""
    monkeypatch.setattr(wo, "load_intraday_profile",
                        lambda *a, **k: _profile(U_BUCKETS))
    # default config: execution_mode='marketable_limit' → None (mode gate)
    cfg_default = TradingSessionConfig(total_allocated_capital=1e6)
    assert wo.make_vwap_sizer(cfg_default, "AAA") is None
    # worked but vwap disabled (the default) → None (the enabled gate)
    cfg_worked_off = TradingSessionConfig(
        total_allocated_capital=1e6, execution_mode="worked",
        worked_vwap_enabled=False)
    assert wo.make_vwap_sizer(cfg_worked_off, "AAA") is None
    # vwap enabled but execution_mode not worked → still None (gate is AND)
    cfg_mode_off = TradingSessionConfig(
        total_allocated_capital=1e6, execution_mode="marketable_limit",
        worked_vwap_enabled=True)
    assert wo.make_vwap_sizer(cfg_mode_off, "AAA") is None
    # BOTH on + a valid profile → a sizer IS built (proves the stub is reachable and
    # the gate is the ONLY thing suppressing it above).
    cfg_on = TradingSessionConfig(
        total_allocated_capital=1e6, execution_mode="worked",
        worked_vwap_enabled=True)
    assert isinstance(wo.make_vwap_sizer(cfg_on, "AAA"), wo.VwapScheduleSizer)


def test_make_vwap_sizer_thin_history_returns_none_fallback():
    """A symbol with NO ohlc_1min history (thin) → load_intraday_profile returns
    None → make_vwap_sizer returns None → the caller falls back to v1 flat POV
    (never fails). Uses the real read-only profile DB path.
    MUTATION REVERT: in load_intraday_profile drop the `if len(days) < min_days`
    thin-history guard → a symbol with 0 days builds an all-zero profile → invalid
    but the guard's None short-circuit is gone → behavior diverges (a bogus profile
    could be cached) → this None assert FAILS."""
    cfg = TradingSessionConfig(
        total_allocated_capital=1e6, execution_mode="worked",
        worked_vwap_enabled=True)
    bogus = "___NO_SUCH_SYMBOL_ZZZ___"
    assert wo.make_vwap_sizer(cfg, bogus) is None
    assert wo.load_intraday_profile(bogus, min_days=5) is None


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG — v2 validation + round-trip; default-off
# ══════════════════════════════════════════════════════════════════════════════

def test_config_vwap_validation_and_roundtrip():
    """worked_vwap_enabled + worked_max_participation_pct validate and round-trip;
    a ceiling BELOW the participation cap, or above 1.0, is rejected; the default is
    OFF with a 0.25 ceiling.
    MUTATION REVERT: in config.validate() remove the
    `worked_max_participation_pct < worked_participation_pct` check → the
    ceiling-below-floor case stops raising → the `pytest.raises` FAILS."""
    cfg = TradingSessionConfig(
        total_allocated_capital=1e6, execution_mode="worked",
        worked_vwap_enabled=True, worked_participation_pct=0.10,
        worked_max_participation_pct=0.25, worked_vwap_profile_days=15)
    cfg.validate()
    rt = TradingSessionConfig.from_dict(cfg.to_dict())
    assert rt.worked_vwap_enabled is True
    assert rt.worked_max_participation_pct == 0.25
    assert rt.worked_vwap_profile_days == 15
    # default OFF
    d = TradingSessionConfig(total_allocated_capital=1e6)
    assert d.worked_vwap_enabled is False
    assert d.worked_max_participation_pct == 0.25
    # ceiling BELOW the participation cap → reject
    with pytest.raises(ValueError):
        TradingSessionConfig(
            total_allocated_capital=1e6, execution_mode="worked",
            worked_participation_pct=0.20,
            worked_max_participation_pct=0.10).validate()
    # ceiling above 1.0 → reject
    with pytest.raises(ValueError):
        TradingSessionConfig(
            total_allocated_capital=1e6, execution_mode="worked",
            worked_max_participation_pct=1.5).validate()
