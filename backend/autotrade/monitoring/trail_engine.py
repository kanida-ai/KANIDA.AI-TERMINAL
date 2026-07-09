"""Intraday-basket TRAILING-PROFIT decision engine (strategy=="intraday_basket").

PURE decision function over the NOTIONAL / invested-basis basket gross return G
(= PortfolioMonitor.compute_gross_return_invested) + the persisted trail state
(armed, peak). It DECIDES only — it never places or cancels an order. The caller
(session.tick / ws_driver / square-off scheduler) executes the flatten by
REUSING the existing kill-switch flatten path (KillSwitchExecutor.fire), passing
the returned reason through as the close_reason. The denominator is the FROZEN
invested basis, identical to the portfolio kill switch — never the fund.

DECISION TABLE (evaluated top to bottom, each tick):

  precedence  condition                                   → action / result
  ----------  ------------------------------------------  ---------------------
  1 SQUARE    now_IST >= square_off_time                  → EXIT "SQUARE_OFF"
  2 STOP      G <= -stop_pct                              → EXIT "STOP"
              (always active; in practice only reachable
               pre-arm because once armed the floor is
               positive and triggers first)
  3 PRE-ARM   not armed and G >= arm_threshold            → ARM (armed=True,
              (arm_threshold = ladder[0] peak when          peak=G); state change
               step-locking, else arm_pct; sets the
               lock; NO exit this tick)
  4 ARMED     armed:                                      → maybe EXIT
                peak = max(peak, G)  (ratchet up)
                STEP-LOCK (default):
                  step  = step_lock_floor(peak)   (ratchets up)
                  gback = peak - trail_giveback_pct  (peak < large_peak_pct)
                        = peak*(1 - large_giveback_rel) (peak >= large_peak_pct)
                  trigger = max(step, gback)
                  if G <= trigger:
                    EXIT "STEP_LOCK_EXIT" if step > gback (step binds)
                    EXIT "TRAIL_EXIT"     otherwise
                LEGACY (step-locking off / empty ladder):
                  trigger = max(peak - trail_giveback_pct, floor_pct)
                  if G <= trigger:
                    EXIT "TRAIL_EXIT"  if trigger == peak-giveback
                    EXIT "FLOOR_EXIT"  if trigger == floor_pct
  -           otherwise                                   → HOLD (state persisted
                                                            if peak ratcheted)

EXIT REASON SET: {"SQUARE_OFF", "STOP", "TRAIL_EXIT", "FLOOR_EXIT",
                  "STEP_LOCK_EXIT"}.

The function is side-effect-free and fully unit-testable. State persistence and
order execution are the caller's responsibility (see session.tick()).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

IST = timezone(timedelta(hours=5, minutes=30))

# Exit reasons the engine can emit. Kept as a frozenset so callers can assert.
# STEP_LOCK_EXIT is emitted when the ratcheting step-lock floor is the binding
# (higher) side of the armed trigger (else TRAIL_EXIT); FLOOR_EXIT is the legacy
# fixed-floor reason (step-locking disabled).
EXIT_REASONS = frozenset(
    {"SQUARE_OFF", "STOP", "TRAIL_EXIT", "FLOOR_EXIT", "STEP_LOCK_EXIT"})


@dataclass
class TrailState:
    """Persisted trail state for an intraday_basket session.

    armed: has the trail armed (G crossed +arm_pct at least once)?
    peak:  the highest G observed since arming (the ratchet high-water mark).
    """
    armed: bool = False
    peak: float = 0.0


@dataclass
class TrailParams:
    """The fractional knobs from TradingSessionConfig (intraday_basket only)."""
    arm_pct: float = 0.01
    floor_pct: float = 0.01
    trail_giveback_pct: float = 0.0075
    stop_pct: float = 0.015
    square_off_time: str = "15:29:00"
    # ── PROFIT STEP-LOCK (ratcheting profit floor) ────────────────────────────
    # step_lock_enabled DEFAULTS FALSE HERE (the low-level pure struct) so any
    # directly-constructed TrailParams keeps the EXACT legacy fixed-floor
    # behaviour byte-for-byte. params_from_config() threads the CONFIG default
    # (True) so a real intraday_basket session ratchets. When active
    # (step_lock_enabled AND a non-empty step_lock_ladder):
    #   * ARM at the first rung's peak_threshold (supersedes arm_pct).
    #   * the armed trigger = max(step_lock_floor(peak), give-back level), where
    #     step_lock_floor(peak) is the lock of the highest rung whose threshold
    #     <= peak (monotonic-up because peak only ratchets up), and the give-back
    #     level is (peak - trail_giveback_pct) below large_peak_pct, else the
    #     RELATIVE peak * (1 - large_giveback_rel).
    # All fractions of ALLOCATED CAPITAL (the trail's g basis).
    step_lock_enabled: bool = False
    step_lock_ladder: List[Tuple[float, float]] = field(default_factory=list)
    large_peak_pct: float = 0.20
    large_giveback_rel: float = 0.175
    # True (DEFAULT) = INTRADAY: the time-based SQUARE_OFF branch is active
    #   (today's behaviour, byte-for-byte). False = POSITIONAL: the SQUARE_OFF
    #   branch is SKIPPED entirely so the ratchet/floor/hard-stop carry across
    #   days. ALL other branches (STOP, ARM, peak-ratchet, TRAIL/FLOOR) are
    #   UNCHANGED regardless of this flag.
    square_off_enabled: bool = True


@dataclass
class TrailDecision:
    """The engine's verdict for one tick.

    action: "EXIT" | "ARM" | "HOLD".
    reason: an EXIT_REASONS member when action=="EXIT", else None.
    state:  the (possibly mutated) TrailState the caller must persist if changed.
    state_changed: True when armed/peak differ from the input state (persist it).
    trigger: the live exit-trigger level (the G at which an armed basket would
             exit) for status display; None when not armed.
    """
    action: str
    reason: Optional[str]
    state: TrailState
    state_changed: bool
    trigger: Optional[float] = None


def _parse_square_off_today_ist(square_off_time: str,
                                now: Optional[datetime] = None) -> Optional[datetime]:
    """Parse square_off_time ("HH:MM"/"HH:MM:SS") as TODAY in IST. None if bad."""
    s = (square_off_time or "").strip()
    parsed = None
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            parsed = datetime.strptime(s, fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        return None
    base = now or datetime.now(IST)
    return base.replace(hour=parsed.hour, minute=parsed.minute,
                        second=parsed.second, microsecond=0)


def _step_lock_active(params: TrailParams) -> bool:
    """True when profit step-locking is switched on AND has a ladder to use.
    When False the engine runs the legacy fixed-floor path byte-for-byte."""
    return bool(params.step_lock_enabled and params.step_lock_ladder)


def step_lock_floor(peak: float, ladder: List[Tuple[float, float]]) -> float:
    """The ratcheting profit floor for a given peak: the lock_floor of the HIGHEST
    rung whose peak_threshold <= peak, or 0.0 if peak is below the first rung.

    The ladder is ascending by peak_threshold (validated at config time). Because
    the caller only ever feeds a MONOTONIC-UP peak (peak = max(peak, g)), the
    returned floor is itself monotonic-up — it never steps down."""
    floor = 0.0
    for thr, lock in ladder:
        if peak >= thr:
            floor = lock
        else:
            break  # ascending ladder → no higher rung can qualify
    return floor


def _giveback_level(peak: float, params: TrailParams) -> float:
    """The give-back trigger level for an armed peak. Below large_peak_pct it is
    the FIXED giveback (peak - trail_giveback_pct); at/above large_peak_pct it is
    the RELATIVE giveback (peak * (1 - large_giveback_rel)) so a big-trend day
    trails a proportional distance and lets the winner run."""
    if peak >= params.large_peak_pct:
        return peak * (1.0 - params.large_giveback_rel)
    return peak - params.trail_giveback_pct


def compute_trigger(state: TrailState, params: TrailParams) -> Optional[float]:
    """The live exit-trigger level for an ARMED basket. None when not armed.

    STEP-LOCK ACTIVE : max(step_lock_floor(peak), give-back level) — the give-back
        level is the fixed peak-giveback below large_peak_pct, else the relative
        peak*(1-large_giveback_rel).
    LEGACY (off)     : max(peak - trail_giveback_pct, floor_pct) — byte-for-byte."""
    if not state.armed:
        return None
    if _step_lock_active(params):
        return max(step_lock_floor(state.peak, params.step_lock_ladder),
                   _giveback_level(state.peak, params))
    return max(state.peak - params.trail_giveback_pct, params.floor_pct)


def decide(g: float, state: TrailState, params: TrailParams,
           now: Optional[datetime] = None) -> TrailDecision:
    """Pure trail decision for one tick.

    g:     the NOTIONAL / invested-basis basket gross return (a fraction).
    state: the persisted TrailState (armed, peak).
    params: the fractional knobs + square_off_time.
    now:   IST-aware datetime override for tests; defaults to now in IST.

    Returns a TrailDecision; the caller persists state when state_changed and
    executes the flatten (reusing KillSwitchExecutor.fire) when action=="EXIT".
    """
    now_ist = now or datetime.now(IST)

    # 1. SQUARE-OFF — time-based flatten takes precedence over everything.
    # POSITIONAL (square_off_enabled False): skip this branch entirely so the
    # ratchet/floor/hard-stop carry across days. All other branches unchanged.
    if params.square_off_enabled:
        sq = _parse_square_off_today_ist(params.square_off_time, now_ist)
        if sq is not None and now_ist >= sq:
            return TrailDecision(action="EXIT", reason="SQUARE_OFF", state=state,
                                 state_changed=False,
                                 trigger=compute_trigger(state, params))

    # 2. STOP — downside hard stop, always active (in practice pre-arm only).
    if g <= -abs(params.stop_pct):
        return TrailDecision(action="EXIT", reason="STOP", state=state,
                             state_changed=False,
                             trigger=compute_trigger(state, params))

    step_active = _step_lock_active(params)
    # The arm threshold: with step-locking active it is the FIRST rung's
    # peak_threshold (that IS the arm, superseding arm_pct); else the legacy
    # arm_pct. Below it → HOLD (no trail yet).
    arm_threshold = params.step_lock_ladder[0][0] if step_active else params.arm_pct

    # 3. PRE-ARM — arm when G first reaches the arm threshold. No exit this tick.
    if not state.armed:
        if g >= arm_threshold:
            new_state = TrailState(armed=True, peak=g)
            return TrailDecision(action="ARM", reason=None, state=new_state,
                                 state_changed=True,
                                 trigger=compute_trigger(new_state, params))
        return TrailDecision(action="HOLD", reason=None, state=state,
                             state_changed=False, trigger=None)

    # 4. ARMED — ratchet the peak up, then test the trigger.
    changed = False
    peak = state.peak
    if g > peak:
        peak = g
        changed = True
    cur_state = TrailState(armed=True, peak=peak)

    if step_active:
        # STEP-LOCK: trigger = max(ratcheting step floor, give-back level). The
        # step floor is monotonic-up (peak only ratchets up); the give-back level
        # is fixed below large_peak_pct, else relative. STEP_LOCK_EXIT when the
        # step floor is the binding (strictly higher) side, else TRAIL_EXIT.
        step_floor = step_lock_floor(peak, params.step_lock_ladder)
        giveback_level = _giveback_level(peak, params)
        trigger = max(step_floor, giveback_level)
        if g <= trigger:
            reason = "STEP_LOCK_EXIT" if step_floor > giveback_level else "TRAIL_EXIT"
            return TrailDecision(action="EXIT", reason=reason, state=cur_state,
                                 state_changed=changed, trigger=trigger)
        return TrailDecision(action="HOLD", reason=None, state=cur_state,
                             state_changed=changed, trigger=trigger)

    # LEGACY fixed-floor path — byte-for-byte the pre-step-lock behaviour.
    giveback_level = peak - params.trail_giveback_pct
    trigger = max(giveback_level, params.floor_pct)
    if g <= trigger:
        # TRAIL_EXIT when the giveback level is the binding trigger; FLOOR_EXIT
        # when the floor clamps it (giveback level fell below the floor). Per the
        # spec: reason = TRAIL_EXIT if trigger == peak-giveback else FLOOR_EXIT.
        reason = "TRAIL_EXIT" if trigger == giveback_level else "FLOOR_EXIT"
        return TrailDecision(action="EXIT", reason=reason, state=cur_state,
                             state_changed=changed, trigger=trigger)
    return TrailDecision(action="HOLD", reason=None, state=cur_state,
                         state_changed=changed, trigger=trigger)


def params_from_config(config) -> TrailParams:
    """Build TrailParams from a TradingSessionConfig.

    Threads the CONFIG-level step-lock default (True) through so a real
    intraday_basket session ratchets; the ladder is coerced to plain float pairs.
    A directly-constructed TrailParams still defaults step_lock_enabled=False
    (legacy fixed-floor)."""
    ladder = getattr(config, "trail_step_lock_ladder", None) or []
    return TrailParams(
        arm_pct=float(config.arm_pct),
        floor_pct=float(config.floor_pct),
        trail_giveback_pct=float(config.trail_giveback_pct),
        stop_pct=float(config.stop_pct),
        square_off_time=config.square_off_time,
        square_off_enabled=bool(getattr(config, "square_off_enabled", True)),
        step_lock_enabled=bool(getattr(config, "trail_step_lock_enabled", False)),
        step_lock_ladder=[(float(r[0]), float(r[1])) for r in ladder],
        large_peak_pct=float(getattr(config, "trail_large_peak_pct", 0.20)),
        large_giveback_rel=float(getattr(config, "trail_large_giveback_rel", 0.175)),
    )


def seconds_to_square_off(square_off_time: str,
                          now: Optional[datetime] = None) -> Optional[int]:
    """Seconds remaining until today's square-off (>=0), or None if unparseable.
    0 once the time has passed."""
    now_ist = now or datetime.now(IST)
    sq = _parse_square_off_today_ist(square_off_time, now_ist)
    if sq is None:
        return None
    return int(max(0.0, (sq - now_ist).total_seconds()))
