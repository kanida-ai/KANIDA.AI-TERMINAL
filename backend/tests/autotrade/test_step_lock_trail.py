"""PROFIT STEP-LOCKING (ratcheting profit floor) for the intraday_basket trail.

Replaces the single fixed floor_pct with a STEP-LOCK LADDER floor(peak) that
ratchets UP in steps as the peak G climbs, while STILL taking the give-back level
and exiting at the MAX of both. All values are FRACTIONS OF ALLOCATED CAPITAL
(the intraday_basket trail decides on compute_gross_return = uPnL/deployed
capital), so a ₹5L / 5x setup reads directly in rupees:
  ladder [[0.03,0.02],[0.05,0.035],[0.08,0.06],[0.12,0.095],[0.16,0.13]]
  → ₹15K→lock₹10K, ₹25K→₹17.5K, ₹40K→₹30K, ₹60K→₹47.5K, ₹80K→₹65K.

These are PURE decide() unit tests: a fixed `now` (10:00 IST) is passed so the
time-based SQUARE_OFF branch never fires and the tests are wall-clock independent.
The operator's exact worked example numbers are asserted (incl. the ₹33,750 §4
headline and the ₹82.5K large-day protection on ₹5L).
"""
from datetime import datetime, timedelta, timezone

import pytest

from autotrade.config import (
    TradingSessionConfig, DEFAULT_STEP_LOCK_LADDER, validate_step_lock_ladder)
from autotrade.monitoring import trail_engine
from autotrade.monitoring.trail_engine import (
    TrailParams, TrailState, decide, step_lock_floor, compute_trigger)
from autotrade.session import LIVE_EDITABLE_SESSION_FIELDS

IST = timezone(timedelta(hours=5, minutes=30))

# 10:00 IST today — before any intraday square-off, so SQUARE_OFF never fires.
NOW = datetime.now(IST).replace(hour=10, minute=0, second=0, microsecond=0)

# The operator's default ladder (peak_threshold, lock_floor), fractions of capital.
LADDER = [(0.03, 0.02), (0.05, 0.035), (0.08, 0.06), (0.12, 0.095), (0.16, 0.13)]
CAP = 500_000.0   # ₹5L, to prove the rupee numbers.


def P(*, giveback=0.0125, large_peak=0.20, large_rel=0.175, ladder=LADDER,
      stop=0.03, enabled=True):
    """A step-lock TrailParams. giveback defaults to the 0.0125 the operator used
    in the worked examples (config default trail_giveback_pct is 0.015)."""
    return TrailParams(
        arm_pct=0.05, floor_pct=0.01, trail_giveback_pct=giveback, stop_pct=stop,
        square_off_time="15:29:00",
        step_lock_enabled=enabled, step_lock_ladder=list(ladder),
        large_peak_pct=large_peak, large_giveback_rel=large_rel)


def armed(peak):
    return TrailState(armed=True, peak=peak)


# ── step_lock_floor() helper ──────────────────────────────────────────────────

def test_step_lock_floor_ladder():
    f = lambda p: step_lock_floor(p, LADDER)
    assert f(0.0) == 0.0
    assert f(0.016) == 0.0            # ₹8K — below the first rung → no floor
    assert f(0.029) == 0.0
    assert f(0.03) == 0.02            # first rung crossed
    assert f(0.049) == 0.02
    assert f(0.05) == 0.035
    assert f(0.08) == 0.06
    assert f(0.12) == 0.095
    assert f(0.16) == 0.13
    assert f(0.20) == 0.13            # above the top rung → stays at the top lock
    assert f(0.99) == 0.13


# ── ARM at the first rung's peak_threshold ────────────────────────────────────

def test_arm_below_first_rung_holds():
    # g = 0.016 (₹8K) is below the first rung 0.03 → HOLD, not armed.
    d = decide(0.016, TrailState(), P(), now=NOW)
    assert d.action == "HOLD"
    assert d.state.armed is False
    assert d.trigger is None


def test_arm_at_first_rung():
    # g = 0.03 (₹15K) → ARM at the first rung (supersedes arm_pct=0.05). Immediate
    # trigger = max(step 0.02, give-back 0.03-0.0125=0.0175) = 0.02 (lock ₹10K).
    d = decide(0.03, TrailState(), P(giveback=0.0125), now=NOW)
    assert d.action == "ARM"
    assert d.state.armed is True
    assert d.state.peak == pytest.approx(0.03)
    assert d.trigger == pytest.approx(0.02)          # step binds over give-back
    assert d.trigger * CAP == pytest.approx(10_000.0)


# ── RATCHET: floor steps up and never steps down on a dip ─────────────────────

def test_ratchet_floor_steps_up_and_sticks():
    p = P(giveback=0.0125)
    # Start armed at peak 0.03 (floor 0.02).
    st = armed(0.03)
    assert step_lock_floor(st.peak, LADDER) == 0.02

    # Drive peak up to 0.05 → floor 0.035.
    d = decide(0.05, st, p, now=NOW)
    assert d.action == "HOLD"
    assert d.state.peak == pytest.approx(0.05)
    assert step_lock_floor(d.state.peak, LADDER) == 0.035
    st = d.state

    # Drive peak up to 0.08 → floor 0.06.
    d = decide(0.08, st, p, now=NOW)
    assert d.state.peak == pytest.approx(0.08)
    assert step_lock_floor(d.state.peak, LADDER) == 0.06
    st = d.state

    # DIP: g falls to 0.07 (still above the 0.0675 trigger) → HOLD, peak STICKS at
    # 0.08 and the floor stays 0.06 (monotonic-up — the ratchet never drops).
    d = decide(0.07, st, p, now=NOW)
    assert d.action == "HOLD"
    assert d.state.peak == pytest.approx(0.08)       # peak did not fall with g
    assert step_lock_floor(d.state.peak, LADDER) == 0.06
    assert d.state_changed is False


# ── §4 HEADLINE example: peak 0.08, giveback 0.0125 → TRAIL_EXIT at ₹33,750 ────

def test_section4_headline_trail_exit_at_33750():
    p = P(giveback=0.0125)
    # peak 0.08 (₹40K): step floor 0.06, give-back level 0.08-0.0125=0.0675.
    # trigger = max(0.06, 0.0675) = 0.0675 (give-back binds → TRAIL_EXIT).
    d_hold = decide(0.07, armed(0.08), p, now=NOW)
    assert d_hold.action == "HOLD"                   # 0.07 > 0.0675 → hold
    assert d_hold.trigger == pytest.approx(0.0675)

    d_exit = decide(0.0675, armed(0.08), p, now=NOW)
    assert d_exit.action == "EXIT"
    assert d_exit.reason == "TRAIL_EXIT"             # give-back is the higher side
    assert d_exit.trigger == pytest.approx(0.0675)
    assert d_exit.trigger * CAP == pytest.approx(33_750.0)   # the ₹33,750 number


def test_section4_headline_step_floor_value():
    # Prove the step floor at peak 0.08 is ₹30K (0.06) — it is BELOW the give-back
    # side here, so give-back binds, but the floor is genuinely locked at ₹30K.
    assert step_lock_floor(0.08, LADDER) * CAP == pytest.approx(30_000.0)


# ── STEP-LOCK binds: choose a giveback where the step floor is higher ──────────

def test_step_lock_binds_reason():
    # peak 0.08, giveback 0.03 → give-back level 0.05 < step floor 0.06.
    # trigger = max(0.06, 0.05) = 0.06 (step binds → STEP_LOCK_EXIT).
    p = P(giveback=0.03)
    d = decide(0.06, armed(0.08), p, now=NOW)
    assert d.action == "EXIT"
    assert d.reason == "STEP_LOCK_EXIT"
    assert d.trigger == pytest.approx(0.06)
    assert d.trigger * CAP == pytest.approx(30_000.0)
    # And just above the step floor → HOLD.
    assert decide(0.061, armed(0.08), p, now=NOW).action == "HOLD"


# ── LARGE DAY: relative give-back protects ₹82.5K on ₹1L peak ──────────────────

def test_large_day_relative_giveback():
    # peak 0.20 (₹1L), large_peak 0.20, rel 0.175 → give-back level =
    # 0.20*(1-0.175) = 0.165 (protect ₹82.5K). step floor at 0.20 = 0.13.
    # trigger = max(0.13, 0.165) = 0.165 (relative give-back binds → TRAIL_EXIT).
    p = P(giveback=0.0125)   # fixed giveback is IGNORED at/above large_peak
    d = decide(0.165, armed(0.20), p, now=NOW)
    assert d.action == "EXIT"
    assert d.reason == "TRAIL_EXIT"
    assert d.trigger == pytest.approx(0.165)
    assert d.trigger * CAP == pytest.approx(82_500.0)
    # The RELATIVE branch is used, NOT the fixed 0.20-0.0125 = 0.1875.
    assert d.trigger != pytest.approx(0.1875)
    # Just below large_peak (0.19) → the FIXED give-back applies (0.19-0.0125);
    # at/above large_peak (0.20) → the RELATIVE give-back (0.165).
    assert trail_engine._giveback_level(0.19, p) == pytest.approx(0.1775)
    assert trail_engine._giveback_level(0.20, p) == pytest.approx(0.165)


# ── MONOTONIC: peak 0.16 then g falls to 0.10 → floor stays 0.13, exit fires ───

def test_monotonic_floor_holds_and_exits():
    # peak 0.16 → step floor 0.13 (₹65K). With giveback 0.05 the give-back level
    # (0.16-0.05=0.11) is BELOW the step floor → trigger = 0.13 (step binds).
    p = P(giveback=0.05)
    assert step_lock_floor(0.16, LADDER) == 0.13
    d = decide(0.10, armed(0.16), p, now=NOW)
    assert d.action == "EXIT"                        # 0.10 <= 0.13
    assert d.reason == "STEP_LOCK_EXIT"
    assert d.trigger == pytest.approx(0.13)
    assert d.trigger * CAP == pytest.approx(65_000.0)


# ── BACKWARD-COMPAT: step-locking OFF → legacy fixed-floor decide, byte-identical ─

def test_disabled_is_legacy_fixed_floor():
    # enabled=False → arm at arm_pct (0.05), NOT the ladder's 0.03.
    p = P(enabled=False, giveback=0.0075)
    assert decide(0.03, TrailState(), p, now=NOW).action == "HOLD"   # 0.03 < 0.05
    assert decide(0.05, TrailState(), p, now=NOW).action == "ARM"

    # Armed peak 0.02, giveback 0.0075, floor 0.01 → trigger max(0.0125, 0.01)=0.0125.
    # G drops to 0.010 → TRAIL_EXIT (matches the pre-step-lock behaviour exactly).
    lp = TrailParams(arm_pct=0.01, floor_pct=0.01, trail_giveback_pct=0.0075,
                     square_off_time="15:29:00")   # step_lock_enabled defaults False
    d = decide(0.010, armed(0.02), lp, now=NOW)
    assert d.action == "EXIT"
    assert d.reason == "TRAIL_EXIT"
    assert d.trigger == pytest.approx(0.0125)

    # Floor clamps → FLOOR_EXIT (legacy reason still emitted when step-lock off).
    lp2 = TrailParams(arm_pct=0.01, floor_pct=0.01, trail_giveback_pct=0.02,
                      square_off_time="15:29:00")
    d2 = decide(0.009, armed(0.012), lp2, now=NOW)
    assert d2.action == "EXIT"
    assert d2.reason == "FLOOR_EXIT"


def test_empty_ladder_falls_back_to_fixed_floor():
    # enabled=True but an EMPTY ladder → the fixed-floor path runs (opt-out).
    p = P(enabled=True, ladder=[], giveback=0.0075)
    # arm at arm_pct (0.05), not a ladder rung (there is none).
    assert decide(0.03, TrailState(), p, now=NOW).action == "HOLD"
    d = decide(0.010, armed(0.02), p, now=NOW)
    assert d.reason == "TRAIL_EXIT"                  # legacy reason set


# ── compute_trigger reflects the step-lock ────────────────────────────────────

def test_compute_trigger_step_lock():
    p = P(giveback=0.0125)
    assert compute_trigger(TrailState(armed=False), p) is None
    assert compute_trigger(armed(0.08), p) == pytest.approx(0.0675)   # give-back
    assert compute_trigger(armed(0.08), P(giveback=0.03)) == pytest.approx(0.06)  # step
    assert compute_trigger(armed(0.20), p) == pytest.approx(0.165)    # large-day


# ── config: default ON, round-trips, threads to params ────────────────────────

def test_config_default_step_lock_on_and_roundtrips():
    cfg = TradingSessionConfig(
        total_allocated_capital=CAP, strategy="intraday_basket", top_n_stocks=5)
    assert cfg.trail_step_lock_enabled is True
    assert [list(r) for r in cfg.trail_step_lock_ladder] == \
        [list(r) for r in DEFAULT_STEP_LOCK_LADDER]
    assert cfg.trail_large_peak_pct == pytest.approx(0.20)
    assert cfg.trail_large_giveback_rel == pytest.approx(0.175)
    cfg.validate()

    # Round-trip; a dict WITHOUT the keys loads with the defaults (an existing
    # session with no step-lock keys picks up the default-on ratchet).
    d = cfg.to_dict()
    for k in ("trail_step_lock_enabled", "trail_step_lock_ladder",
              "trail_large_peak_pct", "trail_large_giveback_rel"):
        assert k in d
    del d["trail_step_lock_enabled"]
    del d["trail_step_lock_ladder"]
    c2 = TradingSessionConfig.from_dict(d)
    assert c2.trail_step_lock_enabled is True
    assert [list(r) for r in c2.trail_step_lock_ladder] == \
        [list(r) for r in DEFAULT_STEP_LOCK_LADDER]

    # Explicit custom ladder round-trips.
    d3 = TradingSessionConfig(
        total_allocated_capital=CAP, strategy="intraday_basket", top_n_stocks=5,
        trail_step_lock_ladder=[[0.02, 0.01], [0.04, 0.03]]).to_dict()
    c3 = TradingSessionConfig.from_dict(d3)
    assert c3.trail_step_lock_ladder == [[0.02, 0.01], [0.04, 0.03]]


def test_config_params_from_config_threads_step_lock():
    cfg = TradingSessionConfig(
        total_allocated_capital=CAP, strategy="intraday_basket", top_n_stocks=5)
    p = trail_engine.params_from_config(cfg)
    assert p.step_lock_enabled is True
    assert p.step_lock_ladder == [(0.03, 0.02), (0.05, 0.035), (0.08, 0.06),
                                  (0.12, 0.095), (0.16, 0.13)]
    assert p.large_peak_pct == pytest.approx(0.20)
    assert p.large_giveback_rel == pytest.approx(0.175)

    # Disabled config → step_lock_enabled False threads through (legacy path).
    cfg2 = TradingSessionConfig(
        total_allocated_capital=CAP, strategy="intraday_basket", top_n_stocks=5,
        trail_step_lock_enabled=False)
    assert trail_engine.params_from_config(cfg2).step_lock_enabled is False


# ── config validation of the ladder ───────────────────────────────────────────

def test_validate_step_lock_ladder_ok():
    validate_step_lock_ladder(DEFAULT_STEP_LOCK_LADDER)   # must not raise
    validate_step_lock_ladder([[0.02, 0.01]])


@pytest.mark.parametrize("bad", [
    [],                                        # empty
    [[0.03]],                                  # not a pair
    [[0.03, 0.03]],                            # lock == peak
    [[0.03, 0.04]],                            # lock > peak
    [[0.03, 0.02], [0.03, 0.025]],             # peak not strictly ascending
    [[0.03, 0.02], [0.05, 0.02]],              # lock not strictly ascending
    [[0.0, 0.0]],                              # zero
    [[1.0, 0.5]],                              # peak == 1.0
])
def test_validate_step_lock_ladder_rejects(bad):
    with pytest.raises(ValueError):
        validate_step_lock_ladder(bad)


def test_config_validate_rejects_bad_ladder_when_enabled():
    cfg = TradingSessionConfig(
        total_allocated_capital=CAP, strategy="intraday_basket", top_n_stocks=5,
        trail_step_lock_ladder=[[0.05, 0.02], [0.03, 0.01]])   # descending
    with pytest.raises(ValueError, match="ascending"):
        cfg.validate()


def test_config_empty_ladder_opts_out_and_validates():
    # enabled=True + empty ladder is the fixed-floor opt-out → validate() passes.
    cfg = TradingSessionConfig(
        total_allocated_capital=CAP, strategy="intraday_basket", top_n_stocks=5,
        trail_step_lock_enabled=True, trail_step_lock_ladder=[])
    cfg.validate()
    assert trail_engine.params_from_config(cfg).step_lock_ladder == []


def test_config_rejects_bad_large_params():
    for field, val in (("trail_large_peak_pct", 0.6),
                       ("trail_large_peak_pct", 0.0),
                       ("trail_large_giveback_rel", 1.0),
                       ("trail_large_giveback_rel", 0.0)):
        cfg = TradingSessionConfig(
            total_allocated_capital=CAP, strategy="intraday_basket",
            top_n_stocks=5, **{field: val})
        with pytest.raises(ValueError):
            cfg.validate()


# ── live-edit whitelist ───────────────────────────────────────────────────────

def test_step_lock_fields_are_live_editable():
    for f in ("trail_step_lock_enabled", "trail_step_lock_ladder",
              "trail_large_peak_pct", "trail_large_giveback_rel"):
        assert f in LIVE_EDITABLE_SESSION_FIELDS


def test_config_edit_coerce_and_validate():
    from autotrade.api.config_edit_routes import _coerce_and_validate
    from fastapi import HTTPException

    # bool coercion
    assert _coerce_and_validate("trail_step_lock_enabled", True) is True
    assert _coerce_and_validate("trail_step_lock_enabled", "false") is False
    # rel range
    assert _coerce_and_validate("trail_large_giveback_rel", 0.2) == pytest.approx(0.2)
    with pytest.raises(HTTPException):
        _coerce_and_validate("trail_large_giveback_rel", 1.5)
    # large_peak is a normal (0,0.5) pct field
    assert _coerce_and_validate("trail_large_peak_pct", 0.25) == pytest.approx(0.25)
    with pytest.raises(HTTPException):
        _coerce_and_validate("trail_large_peak_pct", 0.9)
    # ladder shape validated + normalised to float pairs
    out = _coerce_and_validate("trail_step_lock_ladder", [[0.03, 0.02], [0.05, 0.035]])
    assert out == [[0.03, 0.02], [0.05, 0.035]]
    with pytest.raises(HTTPException):
        _coerce_and_validate("trail_step_lock_ladder", [[0.05, 0.02], [0.03, 0.01]])
    with pytest.raises(HTTPException):
        _coerce_and_validate("trail_step_lock_ladder", [])
