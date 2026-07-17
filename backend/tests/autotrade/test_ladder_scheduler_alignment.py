"""LADDER SCHEDULER — ALIGNED WAKE (09:15 entry-latency fix).

The 2026-07-17 defect: the loop was a pure 60s BACKSTOP POLL that never aligned
to the open. The poll PHASE was whatever second the backend last booted at — a
07:44:15 restart made the day's tick fire at 09:15:15; a restart 45s later would
have fired at 09:15:50. Entry time was a lottery decided by the last restart,
and the measured cost of being 37s late on one live Rs1.32cr basket was Rs36,337.

These tests drive the PURE wake-time rule (_next_wait_seconds) with an injected
clock — the simulation below reproduces the loop's phase behaviour exactly (it
is the same rule the daemon applies), proving the wake lands ON the target from
ANY boot phase, that the 60s backstop still fires a MISSED target, and that the
loop cannot busy-spin once the target has passed.
"""
from datetime import datetime, timedelta, timezone

import autotrade.monitoring.ladder_scheduler as sched

IST = timezone(timedelta(hours=5, minutes=30))


def _target_for(now):
    return now.replace(hour=9, minute=15, second=0, microsecond=0)


def _simulate_wakes(boot: datetime, horizon_s: float = 3 * 3600):
    """Replay the loop's wake sequence from `boot` using the REAL rule under
    test. Returns the wake times. (Pure clock arithmetic — no threads.)"""
    wakes = []
    now = boot
    end = boot + timedelta(seconds=horizon_s)
    while now < end:
        wait = sched._next_wait_seconds(now, _target_for(now))
        now = now + timedelta(seconds=wait)
        wakes.append(now)
    return wakes


def _first_wake_at_or_after_target(boot):
    target = _target_for(boot)
    horizon = (target - boot).total_seconds() + 120  # always reach the target
    for w in _simulate_wakes(boot, horizon_s=horizon):
        if w >= target:
            return w
    raise AssertionError("never reached the target")


# ── the headline: wake lands ON the target regardless of boot phase ───────────

def test_wake_lands_on_target_regardless_of_boot_phase():
    """Boot at :15 and boot at :50 BOTH fire at 09:15:00 — not :15 / :50."""
    target = datetime(2026, 7, 17, 9, 15, 0, tzinfo=IST)

    boot_15 = datetime(2026, 7, 17, 7, 44, 15, tzinfo=IST)   # the real 07-17 boot
    boot_50 = datetime(2026, 7, 17, 7, 44, 50, tzinfo=IST)   # 35s later → :50
    boot_odd = datetime(2026, 7, 17, 6, 3, 37, 500000, tzinfo=IST)

    for boot in (boot_15, boot_50, boot_odd):
        fire = _first_wake_at_or_after_target(boot)
        assert fire == target, (
            f"boot {boot.time()} → fired {fire.time()}, expected {target.time()}")


def test_pre_fix_poll_phase_was_the_boot_second_mutation_control():
    """MUTATION CONTROL — the PRE-FIX rule (always sleep the full poll) is what
    made entry time a lottery. Proves these tests measure a real change."""
    def old_rule(now, target):
        return sched._POLL_SECONDS  # the pre-fix unconditional backstop sleep

    def simulate_old(boot):
        now, target = boot, _target_for(boot)
        while now < target:
            now = now + timedelta(seconds=old_rule(now, target))
        return now

    fire_15 = simulate_old(datetime(2026, 7, 17, 7, 44, 15, tzinfo=IST))
    fire_50 = simulate_old(datetime(2026, 7, 17, 7, 44, 50, tzinfo=IST))
    # This reproduces the MEASURED live behaviour: the 07:44:15 boot fired the
    # day's tick at 09:15:15 (BTST entry_latency then pushed Magnifier to :37).
    assert (fire_15.hour, fire_15.minute, fire_15.second) == (9, 15, 15), \
        "pre-fix boot-phase :15 should fire at 09:15:15 (the real 07-17 case)"
    assert (fire_50.hour, fire_50.minute, fire_50.second) == (9, 15, 50), \
        "pre-fix boot-phase :50 should fire at 09:15:50"
    # Two boots 35s apart → entry times 35s apart. THAT is the lottery.
    assert fire_15 != fire_50
    # ...and the fixed rule collapses both onto the target.
    assert _first_wake_at_or_after_target(
        datetime(2026, 7, 17, 7, 44, 15, tzinfo=IST)) == \
        _first_wake_at_or_after_target(
            datetime(2026, 7, 17, 7, 44, 50, tzinfo=IST))


def test_wake_is_never_late_by_more_than_a_hair():
    """From every boot second in a minute, the fire is exactly ON the target."""
    target = datetime(2026, 7, 17, 9, 15, 0, tzinfo=IST)
    for sec in range(60):
        boot = datetime(2026, 7, 17, 8, 30, sec, tzinfo=IST)
        assert _first_wake_at_or_after_target(boot) == target, \
            f"boot second {sec} missed the aligned target"


# ── the 60s backstop is RETAINED ──────────────────────────────────────────────

def test_backstop_poll_retained_after_target():
    """After the target, sleep the full 60s backstop — no busy-loop."""
    now = datetime(2026, 7, 17, 9, 15, 1, tzinfo=IST)
    assert sched._next_wait_seconds(now, _target_for(now)) == sched._POLL_SECONDS
    later = datetime(2026, 7, 17, 14, 0, 0, tzinfo=IST)
    assert sched._next_wait_seconds(later, _target_for(later)) == sched._POLL_SECONDS


def test_no_busy_loop_after_target_bounded_wake_count():
    """`now >= target` stays true all day; the loop must NOT spin. Over 6h past
    the target the wake count must be the poll cadence (~360), not thousands."""
    boot = datetime(2026, 7, 17, 9, 16, 0, tzinfo=IST)
    wakes = _simulate_wakes(boot, horizon_s=6 * 3600)
    assert len(wakes) <= (6 * 3600 / sched._POLL_SECONDS) + 2, \
        f"busy-loop: {len(wakes)} wakes in 6h past the target"


def test_missed_target_still_fires_backstop():
    """Process asleep ACROSS 09:15 → the first wake after resume is at/after the
    target, so the tick fires immediately (the reason the backstop exists)."""
    resume = datetime(2026, 7, 17, 9, 41, 12, tzinfo=IST)  # woke up late
    target = _target_for(resume)
    assert resume >= target                      # the loop's fire condition
    assert sched._next_wait_seconds(resume, target) == sched._POLL_SECONDS


def test_far_before_target_still_wakes_at_poll_cadence():
    """Long before the open the loop still wakes every 60s (holiday refresh,
    stop responsiveness) — it does not sleep for hours."""
    now = datetime(2026, 7, 17, 3, 0, 0, tzinfo=IST)
    assert sched._next_wait_seconds(now, _target_for(now)) == sched._POLL_SECONDS


def test_wait_is_never_zero_or_negative():
    """A wake landing a hair before the target must not produce a 0s spin."""
    target = datetime(2026, 7, 17, 9, 15, 0, tzinfo=IST)
    for micros in (1, 100, 10_000, 999_999):
        now = target - timedelta(microseconds=micros)
        w = sched._next_wait_seconds(now, target)
        assert w >= sched._MIN_WAIT_SECONDS > 0, f"spin risk: wait={w}"


def test_clock_jump_backwards_recovers_and_does_not_spin():
    """Clock jumps back from 09:20 to 09:10 → the loop simply sleeps to the
    (re-reached) target and fires again; the ATOMIC day-claim — not the
    scheduler — is what prevents a second basket."""
    jumped = datetime(2026, 7, 17, 9, 10, 0, tzinfo=IST)
    w = sched._next_wait_seconds(jumped, _target_for(jumped))
    assert w == sched._POLL_SECONDS  # 300s remaining, capped at the backstop
    fire = _first_wake_at_or_after_target(jumped)
    assert fire == datetime(2026, 7, 17, 9, 15, 0, tzinfo=IST)


def test_clock_jump_forwards_past_target_does_not_spin():
    jumped = datetime(2026, 7, 17, 11, 0, 0, tzinfo=IST)
    assert sched._next_wait_seconds(jumped, _target_for(jumped)) == \
        sched._POLL_SECONDS


# ── the REAL daemon loop (threads + an injected clock) ────────────────────────

def _run_loop_briefly(monkeypatch, fake_now, calls, is_trading=True):
    """Run the ACTUAL _run() loop against an injected clock for a moment."""
    import threading
    import autotrade.ladder as ladder_mod
    import autotrade.trading_calendar as cal_mod
    import autotrade.nse_holiday_source as nse_mod

    monkeypatch.setattr(sched, "_now", lambda: fake_now)
    monkeypatch.setattr(sched, "_POLL_SECONDS", 0.05)
    monkeypatch.setattr(cal_mod, "is_trading_day", lambda d: is_trading)
    monkeypatch.setattr(nse_mod, "refresh_if_stale", lambda **k: None)
    monkeypatch.setattr(nse_mod, "ensure_years_covered", lambda *a, **k: set())
    monkeypatch.setattr(ladder_mod, "tick_all_running",
                        lambda ref_now=None: calls.append(ref_now) or
                        {"ticked": 0, "opened": 0, "errors": 0})
    sched._stop.clear()
    t = threading.Thread(target=sched._run, daemon=True)
    t.start()
    import time as _t
    _t.sleep(0.3)
    sched._stop.set()
    t.join(5)
    assert not t.is_alive(), "the scheduler thread did not stop"


def test_daemon_fires_at_or_after_target_on_a_trading_day(monkeypatch):
    calls = []
    _run_loop_briefly(monkeypatch, datetime(2026, 7, 17, 9, 15, 30, tzinfo=IST),
                      calls, is_trading=True)
    assert calls, "the scheduler never ticked at/after the open on a trading day"


def test_daemon_fires_nothing_on_a_non_trading_day(monkeypatch):
    calls = []
    _run_loop_briefly(monkeypatch, datetime(2026, 7, 18, 9, 15, 30, tzinfo=IST),
                      calls, is_trading=False)
    assert calls == [], "the scheduler ticked on a NON-trading day"


def test_daemon_fires_nothing_before_the_target(monkeypatch):
    calls = []
    _run_loop_briefly(monkeypatch, datetime(2026, 7, 17, 8, 0, 0, tzinfo=IST),
                      calls, is_trading=True)
    assert calls == [], "the scheduler ticked BEFORE the open target"
