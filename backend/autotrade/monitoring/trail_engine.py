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
  3 PRE-ARM   not armed and G >= arm_pct                  → ARM (armed=True,
              (sets the lock; NO exit this tick)            peak=G); state change
  4 ARMED     armed:                                      → maybe EXIT
                peak = max(peak, G)  (ratchet up)
                trigger = max(peak - trail_giveback_pct,
                              floor_pct)
                if G <= trigger:
                  EXIT "TRAIL_EXIT"  if trigger == peak-giveback
                  EXIT "FLOOR_EXIT"  if trigger == floor_pct
  -           otherwise                                   → HOLD (state persisted
                                                            if peak ratcheted)

EXIT REASON SET: {"SQUARE_OFF", "STOP", "TRAIL_EXIT", "FLOOR_EXIT"}.

The function is side-effect-free and fully unit-testable. State persistence and
order execution are the caller's responsibility (see session.tick()).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

IST = timezone(timedelta(hours=5, minutes=30))

# Exit reasons the engine can emit. Kept as a frozenset so callers can assert.
EXIT_REASONS = frozenset({"SQUARE_OFF", "STOP", "TRAIL_EXIT", "FLOOR_EXIT"})


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


def compute_trigger(state: TrailState, params: TrailParams) -> Optional[float]:
    """The live exit-trigger level for an ARMED basket:
        max(peak - trail_giveback_pct, floor_pct).
    None when not armed (no trigger yet)."""
    if not state.armed:
        return None
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

    # 3. PRE-ARM — arm when G first reaches +arm_pct. No exit this tick.
    if not state.armed:
        if g >= params.arm_pct:
            new_state = TrailState(armed=True, peak=g)
            return TrailDecision(action="ARM", reason=None, state=new_state,
                                 state_changed=True,
                                 trigger=compute_trigger(new_state, params))
        return TrailDecision(action="HOLD", reason=None, state=state,
                             state_changed=False, trigger=None)

    # 4. ARMED — ratchet the peak up, then test the giveback / floor trigger.
    changed = False
    peak = state.peak
    if g > peak:
        peak = g
        changed = True
    cur_state = TrailState(armed=True, peak=peak)
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
    """Build TrailParams from a TradingSessionConfig."""
    return TrailParams(
        arm_pct=float(config.arm_pct),
        floor_pct=float(config.floor_pct),
        trail_giveback_pct=float(config.trail_giveback_pct),
        stop_pct=float(config.stop_pct),
        square_off_time=config.square_off_time,
        square_off_enabled=bool(getattr(config, "square_off_enabled", True)),
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
