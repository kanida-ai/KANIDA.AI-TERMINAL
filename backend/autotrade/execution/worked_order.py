"""WORKED-ORDER (participation / TWAP) execution engine — v1.

Works ONE logical order (an entry BUILD or an exit UNWIND) into many child slices
PACED OVER TIME, instead of a single market shot, so very large size can be
deployed / flattened while LIMITING market impact. Built for the ₹25cr-actual /
~₹125cr-MIS book (~₹12.5cr per name across a ~10-name basket): at that size a
single order moves the price and/or exceeds the exchange FREEZE quantity.

OPERATOR DIRECTIVE (hard): fill AS MUCH AS EACH NAME WILL ABSORB — pace to limit
impact, but NEVER cap the TARGET size and NEVER drop a pick. A thin name simply
partial-fills; the SHORTFALL is surfaced (target - filled), never hidden.

DESIGN — this module is the PURE PACING + SIZING + ACCOUNTING core. It does NO
broker I/O of its own: the caller injects a `place_child` coroutine that actually
places+confirms ONE child slice (in production that delegates to the EXISTING safe
place path — durable ledger, unique client_order_id per child, query-before-retry
so a confirm timeout never double-places, and fill reconciliation). The engine
only decides HOW BIG each child is and WHEN to place the next one, subtracts the
CONFIRMED fill, and stops at the deadline / a kill. This keeps the whole engine
deterministically unit-testable (inject a fake clock + a mock place_child) AND
inherits every real-money safety guard from the delegated place path.

CHILD SIZING (per interval):
    participation_qty = floor(participation_pct × recent_interval_volume)   (POV)
    child = min(freeze_cap, participation_qty, remaining)                    (POV cap)
    child = max(child, twap_floor)   twap_floor = ceil(remaining/intervals_left)
                                     — GUARANTEES progress even when the volume
                                       read is stale / zero (POV would be 0).
    child = min(child, freeze_cap, remaining)   — the freeze cap is a HARD
                                     exchange limit; a child NEVER exceeds it.
So a long window (many intervals → small twap_floor) is the impact control; POV
lets us take MORE opportunistically when liquidity is ample (still ≤ freeze); the
TWAP floor guarantees we finish by the deadline even on a thin name.

PAPER-SAFE: `simulate_child_fill` models a paper fill off the live LTP + a simple
participation/slippage model, so a caller can run the WHOLE engine end-to-end in
dry-run with NO live orders. (The session's real worked path reuses the existing
dry-run-safe place path, which is byte-identical to today for paper.)

RESTART-DURABLE: `remaining_from_fills` recomputes REMAINING from the target minus
what is already filled/held (the reconciled position row or the ledger), so a
worked order interrupted by a restart resumes without double-filling.

DATA ISOLATION: reads ONLY the poller's mkt_orderflow_1min (read-only) for the
volume signal. Never touches falcon_position_state or any legacy table.
"""
from __future__ import annotations

import asyncio
import logging
import math
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

log = logging.getLogger("kanida.autotrade.execution.worked_order")
IST = timezone(timedelta(hours=5, minutes=30))

# Repo root = backend/autotrade/execution/worked_order.py → parents[3].
_DEFAULT_UNIVERSE_DB = (
    Path(__file__).resolve().parents[3]
    / "universe_engine" / "data" / "db" / "kanida_universe.db")


def _now_iso() -> str:
    return datetime.now(IST).isoformat()


# ══════════════════════════════════════════════════════════════════════════════
# PURE SIZING
# ══════════════════════════════════════════════════════════════════════════════

def intervals_left(now_ts: float, deadline_ts: float, interval_sec: float) -> int:
    """The number of pacing intervals remaining until the deadline, >= 1.

    ceil((deadline - now) / interval); clamped to >= 1 so the TWAP floor always
    has a positive denominator (the LAST interval works whatever remains). A
    past/at deadline → 1 (one final push before the caller stops)."""
    if interval_sec is None or interval_sec <= 0:
        return 1
    remaining_sec = float(deadline_ts) - float(now_ts)
    if remaining_sec <= 0:
        return 1
    return max(1, int(math.ceil(remaining_sec / float(interval_sec))))


def twap_floor(remaining: int, n_intervals: int) -> int:
    """The minimum child that finishes `remaining` by the deadline: ceil(remaining
    / intervals_left). This is the PROGRESS GUARANTEE — it is applied as a FLOOR on
    the child qty so the order still advances even when the POV read is 0/stale."""
    remaining = int(remaining or 0)
    if remaining <= 0:
        return 0
    n = max(1, int(n_intervals or 1))
    return int(math.ceil(remaining / n))


def participation_qty(recent_volume: Optional[float],
                      participation_pct: float) -> int:
    """POV size = floor(participation_pct × recent_interval_volume). 0 when the
    volume read is missing/zero or the pct is non-positive (→ the TWAP floor then
    governs progress)."""
    if not recent_volume or float(recent_volume) <= 0:
        return 0
    if not participation_pct or float(participation_pct) <= 0:
        return 0
    return int(math.floor(float(recent_volume) * float(participation_pct)))


def next_child_qty(remaining: int, n_intervals: int,
                   recent_volume: Optional[float], participation_pct: float,
                   freeze_cap: Optional[int], min_child_qty: int = 1) -> int:
    """The next child slice qty.

        pov     = participation_qty(recent_volume, participation_pct)  (0 if none)
        primary = min(pov, remaining[, freeze_cap])   — POV cap
        qty     = max(primary, twap_floor)            — TWAP progress floor
        qty     = min(qty, freeze_cap, remaining)     — freeze is a HARD cap
        qty     = max(qty, min(min_child_qty, remaining[, freeze_cap]))

    Guarantees: 0 < qty <= remaining, qty <= freeze_cap (when set). When the
    volume read is 0/None, pov=0 → primary=0 → the twap_floor drives progress
    (never stalls). NEVER caps the TARGET — this only sizes ONE child of it."""
    remaining = int(remaining or 0)
    if remaining <= 0:
        return 0
    fc = int(freeze_cap) if (freeze_cap and int(freeze_cap) > 0) else None
    pov = participation_qty(recent_volume, participation_pct)
    primary = min(pov, remaining) if pov > 0 else 0
    if fc is not None and primary > 0:
        primary = min(primary, fc)
    floor_q = twap_floor(remaining, n_intervals)
    qty = max(primary, floor_q)
    if fc is not None:
        qty = min(qty, fc)                 # freeze is a HARD exchange cap
    qty = min(qty, remaining)
    # Minimum child (avoid dust), bounded by remaining and the freeze cap.
    mcq = min(int(min_child_qty or 1), remaining)
    if fc is not None:
        mcq = min(mcq, fc)
    qty = max(qty, mcq)
    return max(0, int(qty))


# ══════════════════════════════════════════════════════════════════════════════
# v2 — VWAP-CURVE PACING + ADAPTIVE PARTICIPATION (additive; v1 stays the default)
# ══════════════════════════════════════════════════════════════════════════════
#
# v1 paces FLAT. Real intraday volume is U-SHAPED (heavy at the open + close, thin
# midday), so flat pacing UNDER-fills a deep book and OVER-participates a thin one.
# v2 paces to a per-symbol intraday VOLUME PROFILE and ADAPTS to realized fills:
#
#   1. PROFILE  — a normalized volume curve by 5-min time-of-day bucket over
#      [09:15,15:30], averaged over recent-N days of `ohlc_1min` (deep 1-min
#      history; the poller's mkt_orderflow_1min has only a few days of uptime so it
#      is NOT used for the profile — only v1's real-time POV read uses it).
#   2. SCHEDULE — over the ACTUAL work window [window_start, deadline] the profile
#      gives the cumulative fraction of the order that "should" be done by any time
#      t:  frac(t) = (cum(t) - cum(start)) / (cum(deadline) - cum(start)).  The
#      child target this interval brings cumulative-filled up to the scheduled
#      cumulative by the END of the interval:  target = frac(now+interval)*qty -
#      filled.  BEHIND → a big gap (catch up); AHEAD → a small/zero gap (ease off).
#      Monotone in (schedule - filled); no oscillation.
#   3. ADAPTIVE POV — the impact cap is the pct needed to fill that target, clamped
#      to [participation_pct, worked_max_participation_pct]: on schedule ≈ the
#      normal cap; BEHIND it leans up toward the HARD ceiling; it NEVER exceeds the
#      ceiling regardless of schedule. The v1 TWAP floor + freeze cap are preserved
#      exactly (the deadline/exchange guarantees; the floor may still exceed POV to
#      finish, identical to v1). No profile / no window → returns None → v1 flat POV.


@dataclass
class SizerContext:
    """The per-interval state a pluggable child-target strategy sees. All fields are
    read-only inputs; the strategy returns an int child target (or None to decline
    → v1 flat POV fallback for that interval)."""
    remaining: int
    n_intervals: int
    recent_volume: Optional[float]
    now_ts: float
    deadline_ts: Optional[float]
    filled: int
    target_qty: int
    n_children: int


# A pluggable child-target strategy: (parent, ctx) -> child qty | None (decline).
ChildSizer = Callable[["WorkedParent", SizerContext], Optional[int]]

# The NSE cash session in seconds-of-day (IST): 09:15:00 .. 15:30:00.
_SESSION_OPEN_SEC = 9 * 3600 + 15 * 60      # 33300
_SESSION_CLOSE_SEC = 15 * 3600 + 30 * 60    # 55800
_PROFILE_BUCKET_SEC = 300                    # 5-min buckets


def epoch_to_sec_of_day(epoch_ts: float) -> float:
    """An epoch timestamp → IST seconds-of-day (0..86400). Production maps the real
    clock onto the profile's time-of-day curve; deterministic (no hidden now())."""
    dt = datetime.fromtimestamp(float(epoch_ts), IST)
    return dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6


@dataclass
class IntradayVolumeProfile:
    """A per-symbol normalized intraday volume curve. `buckets` are the NORMALIZED
    volume shares (Σ == 1.0) of consecutive `bucket_sec`-second buckets starting at
    `open_sec` seconds-of-day. `cum_fraction(sod)` is the cumulative share from the
    open to seconds-of-day `sod`, LINEARLY interpolated within a bucket — a smooth,
    monotone 0→1 schedule curve."""
    buckets: List[float]
    open_sec: float = float(_SESSION_OPEN_SEC)
    bucket_sec: float = float(_PROFILE_BUCKET_SEC)
    symbol: Optional[str] = None
    n_days: int = 0

    def __post_init__(self):
        tot = sum(float(b) for b in (self.buckets or []) if b and float(b) > 0)
        if self.buckets and tot > 0:
            self.buckets = [max(0.0, float(b)) / tot for b in self.buckets]
            # Prefix sums (cum[i] = cumulative share through the END of bucket i).
            acc = 0.0
            self._cum: List[float] = []
            for b in self.buckets:
                acc += b
                self._cum.append(acc)
        else:
            self.buckets = []
            self._cum = []

    @property
    def valid(self) -> bool:
        return bool(self.buckets)

    def cum_fraction(self, sec_of_day: float) -> float:
        """Cumulative volume share [0,1] from the session open to `sec_of_day`,
        interpolated within the containing bucket. Clamped: <= open → 0, >= close
        → 1."""
        if not self.buckets:
            return 0.0
        rel = float(sec_of_day) - self.open_sec
        if rel <= 0.0:
            return 0.0
        span = len(self.buckets) * self.bucket_sec
        if rel >= span:
            return 1.0
        b = int(rel // self.bucket_sec)
        within = (rel - b * self.bucket_sec) / self.bucket_sec   # 0..1 in-bucket
        cum_before = self._cum[b - 1] if b > 0 else 0.0
        return cum_before + self.buckets[b] * within


def _vwap_child_qty(*, profile_frac: Callable[[float], float], target_qty: int,
                    filled: int, remaining: int, n_intervals: int,
                    recent_volume: Optional[float], now_ts: float,
                    interval_sec: float, window_start_ts: float,
                    deadline_ts: float, participation_pct: float,
                    max_participation_pct: float, freeze_cap: Optional[int],
                    min_child_qty: int) -> Optional[int]:
    """The v2 child target (PURE). `profile_frac(loop_ts)` returns the cumulative
    volume share [0,1] of the trading session at loop timestamp `loop_ts` (monotone
    non-decreasing). Returns None to DECLINE (degenerate window → v1 fallback).

        c_start = profile_frac(window_start);  c_end = profile_frac(deadline)
        denom   = c_end - c_start                    (<= 0 → None → v1 fallback)
        # scheduled cumulative fraction of the ORDER due by the END of this interval
        frac_next   = (profile_frac(min(now+interval, deadline)) - c_start) / denom
        sched_cum   = frac_next * target_qty
        child_target= ceil(max(0, sched_cum - filled))   # gap to the schedule
        # ADAPTIVE POV: the pct needed to fill child_target, clamped to the band
        # [participation_pct, max_participation_pct] — BEHIND leans to the ceiling,
        # AHEAD (small target) rides the normal cap; NEVER exceeds the ceiling.
        eff_pct = clamp(child_target / recent_volume, participation, max_participation)
        pov_cap = floor(eff_pct * recent_volume)
        child   = min(child_target, pov_cap)
        # v1 guarantees preserved EXACTLY: freeze cap (hard), TWAP floor (deadline
        # progress — may exceed POV to finish, as in v1), remaining, min-child.
    """
    remaining = int(remaining or 0)
    if remaining <= 0:
        return 0
    tq = int(target_qty or 0)
    c_start = profile_frac(float(window_start_ts))
    c_end = profile_frac(float(deadline_ts))
    denom = c_end - c_start
    if tq <= 0 or denom <= 0.0:
        return None                               # degenerate → v1 flat POV
    look = min(float(now_ts) + float(interval_sec), float(deadline_ts))
    frac_next = (profile_frac(look) - c_start) / denom
    frac_next = min(1.0, max(0.0, frac_next))
    sched_cum_qty = frac_next * tq
    child_target = int(math.ceil(max(0.0, sched_cum_qty - int(filled or 0))))

    fc = int(freeze_cap) if (freeze_cap and int(freeze_cap) > 0) else None
    rv = float(recent_volume) if (recent_volume and float(recent_volume) > 0) else 0.0
    if rv > 0.0 and child_target > 0:
        # ADAPTIVE participation, bounded to [participation_pct, ceiling].
        base_pct = float(participation_pct) if participation_pct and \
            float(participation_pct) > 0 else 0.0
        ceil_pct = float(max_participation_pct) if max_participation_pct and \
            float(max_participation_pct) > 0 else base_pct
        if ceil_pct < base_pct:
            ceil_pct = base_pct
        needed_pct = child_target / rv
        eff_pct = min(max(needed_pct, base_pct), ceil_pct)  # HARD ceiling clamp
        pov_cap = int(math.floor(eff_pct * rv))
        child = min(child_target, pov_cap) if pov_cap > 0 else 0
    else:
        # No volume read → POV governs nothing; the TWAP floor drives progress
        # (identical to v1 when the read is 0/None).
        child = 0

    if fc is not None and child > 0:
        child = min(child, fc)
    child = min(child, remaining)
    # v1 TWAP PROGRESS FLOOR — the deadline-completion guarantee (may exceed POV to
    # finish a thin name by the deadline, exactly as v1 does; the adaptive ceiling
    # bounds only the SCHEDULE-driven catch-up, never this hard deadline floor).
    floor_q = twap_floor(remaining, n_intervals)
    child = max(child, floor_q)
    if fc is not None:
        child = min(child, fc)                    # freeze is a HARD exchange cap
    child = min(child, remaining)
    mcq = min(int(min_child_qty or 1), remaining)
    if fc is not None:
        mcq = min(mcq, fc)
    child = max(child, mcq)
    return max(0, int(child))


@dataclass
class VwapScheduleSizer:
    """A pluggable `ChildSizer` (v2) that paces to `profile` over the work window
    and adapts to fills. `clock_fn` maps a loop timestamp → IST seconds-of-day (the
    profile's coordinate); production uses `epoch_to_sec_of_day`, tests inject a
    deterministic map. `window_start_ts` is captured on the FIRST call (the moment
    the work actually begins). Declines (returns None → v1 fallback) when there is
    no time deadline or no valid profile."""
    profile: Optional[IntradayVolumeProfile]
    max_participation_pct: float
    clock_fn: Callable[[float], float] = epoch_to_sec_of_day
    window_start_ts: Optional[float] = None

    def _frac(self, loop_ts: float) -> float:
        return self.profile.cum_fraction(self.clock_fn(loop_ts))  # type: ignore

    def __call__(self, parent: "WorkedParent", ctx: SizerContext) -> Optional[int]:
        if self.profile is None or not self.profile.valid:
            return None                           # no profile → v1 flat POV
        if ctx.deadline_ts is None:
            return None                           # no time window → v1 flat POV
        if self.window_start_ts is None:
            self.window_start_ts = ctx.now_ts     # work begins now
        return _vwap_child_qty(
            profile_frac=self._frac, target_qty=ctx.target_qty, filled=ctx.filled,
            remaining=ctx.remaining, n_intervals=ctx.n_intervals,
            recent_volume=ctx.recent_volume, now_ts=ctx.now_ts,
            interval_sec=parent.interval_sec,
            window_start_ts=float(self.window_start_ts),
            deadline_ts=float(ctx.deadline_ts),
            participation_pct=parent.participation_pct,
            max_participation_pct=self.max_participation_pct,
            freeze_cap=parent.freeze_cap, min_child_qty=parent.min_child_qty)


# Per-(symbol, day-roll) profile cache — a profile is stable within a trading day;
# rebuilt on the day roll (the IST date changes) so a long-running process refreshes.
_PROFILE_CACHE: Dict[str, IntradayVolumeProfile] = {}


def load_intraday_profile(symbol: str, *, db_path: Optional[str] = None,
                          lookback_days: int = 20, min_days: int = 5,
                          day_key: Optional[str] = None
                          ) -> Optional[IntradayVolumeProfile]:
    """Build (and cache per symbol+day) the normalized intraday volume profile for
    `symbol` from the last `lookback_days` DISTINCT trading days of `ohlc_1min`. 5-min
    buckets over [09:15,15:30]. Returns None on ANY error / a THIN history
    (< `min_days` days) / a missing DB / no data — the caller then FALLS BACK to v1
    flat POV. Read-only (mode=ro, query_only), never raises."""
    try:
        dk = day_key or datetime.now(IST).strftime("%Y-%m-%d")
        ck = f"{symbol}|{dk}|{int(lookback_days)}"
        cached = _PROFILE_CACHE.get(ck)
        if cached is not None:
            return cached if cached.valid else None
        p = Path(db_path) if db_path else _DEFAULT_UNIVERSE_DB
        if not p.exists():
            return None
        n_buckets = int((_SESSION_CLOSE_SEC - _SESSION_OPEN_SEC)
                        // _PROFILE_BUCKET_SEC)          # 75
        sums = [0.0] * n_buckets
        uri = f"file:{p.as_posix()}?mode=ro"
        con = sqlite3.connect(uri, uri=True, timeout=5)
        try:
            con.execute("PRAGMA query_only=ON")
            days = [r[0] for r in con.execute(
                "SELECT DISTINCT substr(bar_time,1,10) d FROM ohlc_1min "
                "WHERE symbol=? ORDER BY d DESC LIMIT ?",
                (symbol, int(max(1, lookback_days)))).fetchall()]
            if len(days) < int(min_days):
                _PROFILE_CACHE[ck] = IntradayVolumeProfile(buckets=[], symbol=symbol)
                return None
            ph = ",".join("?" * len(days))
            rows = con.execute(
                "SELECT bar_time, volume FROM ohlc_1min "
                f"WHERE symbol=? AND substr(bar_time,1,10) IN ({ph})",
                [symbol] + days).fetchall()
        finally:
            con.close()
        for bt, v in rows:
            try:
                sod = int(bt[11:13]) * 3600 + int(bt[14:16]) * 60
            except Exception:
                continue
            rel = sod - _SESSION_OPEN_SEC
            if rel < 0:
                continue
            b = rel // _PROFILE_BUCKET_SEC
            if 0 <= b < n_buckets:
                sums[b] += float(v or 0)
        prof = IntradayVolumeProfile(buckets=sums, symbol=symbol, n_days=len(days))
        _PROFILE_CACHE[ck] = prof
        return prof if prof.valid else None
    except Exception as e:  # pragma: no cover - defensive; profile is optional
        log.debug("load_intraday_profile(%s) failed: %s", symbol, e)
        return None


def make_vwap_sizer(cfg: Any, symbol: str, *, db_path: Optional[str] = None,
                    clock_fn: Optional[Callable[[float], float]] = None
                    ) -> Optional[VwapScheduleSizer]:
    """Build the v2 sizer for `symbol` from `cfg`, or None when v2 is OFF / there is
    no profile (→ the caller passes no child_sizer → v1 flat POV). Gated on
    execution_mode=='worked' AND cfg.worked_vwap_enabled so it is INERT by default."""
    if str(getattr(cfg, "execution_mode", "")) != "worked":
        return None
    if not bool(getattr(cfg, "worked_vwap_enabled", False)):
        return None
    prof = load_intraday_profile(
        symbol, db_path=db_path,
        lookback_days=int(getattr(cfg, "worked_vwap_profile_days", 20)))
    if prof is None or not prof.valid:
        return None
    return VwapScheduleSizer(
        profile=prof,
        max_participation_pct=float(getattr(cfg, "worked_max_participation_pct",
                                            0.25)),
        clock_fn=clock_fn or epoch_to_sec_of_day)


def _clock_to_today_epoch(clock: str) -> Optional[float]:
    """An IST "HH:MM[:SS]" clock → today's epoch seconds (same scale as time.time).
    None on an unparseable clock. Pure-ish (reads the wall clock for today's date)."""
    s = (clock or "").strip()
    parsed = None
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            parsed = datetime.strptime(s, fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        return None
    now = datetime.now(IST)
    return now.replace(hour=parsed.hour, minute=parsed.minute,
                       second=parsed.second, microsecond=0).timestamp()


def resolve_deadline_ts(cfg, *, tighten_exit: bool = False,
                        now_ts: Optional[float] = None) -> Optional[float]:
    """The epoch-seconds deadline the worked engine paces toward:
    cfg.worked_deadline if set, else cfg.square_off_time (today, IST). An EXIT
    (tighten_exit=True) gets HALF the remaining runway, floored ~2 min out (a
    flatten cannot dawdle to 15:29). None → no time deadline (pace by
    max_children)."""
    dl = (getattr(cfg, "worked_deadline", None)
          or getattr(cfg, "square_off_time", None))
    if not dl:
        return None
    base = _clock_to_today_epoch(dl)
    if base is None:
        return None
    if not tighten_exit:
        return base
    now = now_ts if now_ts is not None else time.time()
    if base <= now:
        return base
    return min(base, now + max(120.0, (base - now) / 2.0))


# ══════════════════════════════════════════════════════════════════════════════
# PACING BYPASS — urgent / deadline-bound exits are NEVER paced
# ══════════════════════════════════════════════════════════════════════════════
#
# THE PRINCIPLE (operator, 2026-07-16) — use this to resolve any reason NOT yet
# enumerated below:
#
#     WORKED-MODE PACING IS FOR ENTRIES. Every URGENT or DEADLINE-BOUND exit
#     fires as MARKET. Impact control is only worth having when there is TIME to
#     spend; a capital-protecting or deadline-bound exit has none.
#
# WHY (real money): LIVE session 1aeb11b8 took 213 SECONDS to exit a STOP under
# execution_mode=="worked" — 11 paced child slices at the 20s worked_interval_sec
# cadence (ledger: "STOP:worked-child-0 .. -10"). Worked mode exists to minimise
# MARKET IMPACT when BUILDING a large position: trading slowly is the whole point
# and time is an acceptable cost. On an urgent EXIT that reasoning INVERTS — the
# position bleeds for the entire window while the exit trickles out, converting an
# impact saving into a strictly larger loss.
#
# ── BYPASSED (fire ONE market exit) ───────────────────────────────────────────
# CAPITAL-PROTECTING (urgent — the position is losing money right now):
#   * STOP        — the BASKET trail engine's downside hard stop (trail_engine emits
#                   reason="STOP" → kill_switch.fire(close_reason="STOP")). The exact
#                   tag of the 213s incident.
#   * STOP_STOCK  — the SAME trail "STOP" decision, RELABELLED for per-stock scope
#                   (session.py). per_stock_stop_enabled=False by default.
#   * STOP_SEAT   — the SAME trail "STOP" decision, RELABELLED for per-seat (Tesla)
#                   scope (session.py).
#                   STOP/STOP_STOCK/STOP_SEAT are ONE decision with three labels:
#                   bypassing only "STOP" would make "stops don't pace" TRUE for 1 of
#                   3 and manufacture false confidence — worse than not fixing it.
#   * KILL_SWITCH — the portfolio kill switch. VERIFIED: this one tag ALSO covers the
#                   MANUAL/OPERATOR kill, LADDER_KILL and the
#                   PORTFOLIO_DAILY_LOSS_BREAKER — every one reaches the exit path via
#                   TradingSession.kill() → KillSwitchExecutor.fire() WITHOUT a
#                   close_reason argument, so they all take fire()'s DEFAULT
#                   close_reason="KILL_SWITCH". Their distinct wording ("MANUAL
#                   LADDER_KILL ...", "LOSS_LIMIT ...") rides on `trigger_reason`,
#                   which the pacing decision never sees. So "OPERATOR" /
#                   "LADDER_KILL" / "LOSS_LIMIT" never appear here as a close_reason.
#
# DEADLINE-BOUND (pacing a hard deadline is guaranteed failure):
#   * MIS_SQUARE_OFF — the ~15:12 defensive flatten. QUANTIFIED: its paced deadline
#                   (resolve_deadline_ts(tighten_exit=True) = now + HALF the runway to
#                   square_off_time 15:29) computes to 15:20:30 — ~30s BEYOND the
#                   broker's own ~15:20 intraday auto-square. A paced MIS_SQUARE_OFF
#                   can therefore hand the book to the broker to force-close at
#                   whatever price it likes — strictly WORSE than the 213s stop.
#   * SQUARE_OFF  — the 15:29 flatten. Its paced window is ZERO seconds (the deadline
#                   IS square_off_time, so now >= deadline on the first loop pass) →
#                   work_order breaks at the top → filled=0 → ZERO orders placed →
#                   EXIT_FAILED. Pacing a hard deadline is guaranteed failure; the
#                   bypass incidentally immunizes it from that trap.
#
# ── STILL PACED (deliberately — this is the line we are drawing) ──────────────
#   TARGET_HIT, MAX_HOLD_EXIT  — NOT urgent and NOT deadline-bound: the position is
#       fine and there is genuinely time to spend, so impact control is worth having.
#   TRAIL_EXIT, STEP_LOCK_EXIT, FLOOR_EXIT — profit-taking trail exits (the trail is
#       ABOVE entry by construction); same reasoning as TARGET_HIT.
#   EXIT_RETRY  — KNOWN GAP, NOT fixed here (reported to the operator). The retry
#       sweep in session.py passes reason="EXIT_RETRY", which ERASES the original
#       reason, so a retried STOP is still paced. The original is destroyed TWICE by
#       the failure path: registry.mark_exit_failed overwrites close_reason with
#       "EXIT_FAILED: {error}", and the gate release NULLs exit_initiated_by (which
#       claim_exit_session had set to the reason). Preserving it cleanly needs a new
#       persisted field threaded through ~18 mark_exit_failed call sites — whose only
#       natural chokepoint (registry.mark_exit_failed) is out of this change's scope.
#       ('GTT' is only a reconciler mark_closed tag — it never reaches this decision.)
#
# ENTRIES ARE NEVER CONSULTED HERE — the worked ENTRY engine (incl. v2 VWAP) is
# untouched and still paces. This predicate is only ever called on the exit path.
#
# ONE explicit set, not scattered conditionals: adding a future reason is a ONE-LINE
# decision here and cannot be silently forgotten at one of the two decision sites.
PACING_BYPASS_EXIT_REASONS = frozenset({
    # Capital-protecting (urgent).
    "STOP", "STOP_STOCK", "STOP_SEAT", "KILL_SWITCH",
    # Deadline-bound.
    "MIS_SQUARE_OFF", "SQUARE_OFF",
})


def bypass_pacing_for_exit(close_reason: Optional[str]) -> bool:
    """True when this EXIT reason must fire as ONE immediate market exit instead of
    being worked/paced into child slices. PURE (no I/O, no clock).

    Matches the close_reason tag EXACTLY (case/space-normalised) against
    PACING_BYPASS_EXIT_REASONS — the tags at both decision points are plain
    vocabulary tokens (see exit_gate.VALID_REASONS), never free text. A prefix/
    substring match is deliberately NOT used: it would make "STOP" swallow
    "STOP_STOCK" and "TIME_STOP", silently widening the approved scope.

    An unknown/None reason returns False → PACED, i.e. today's behaviour. This
    fails toward the UNCHANGED path, so a vocabulary drift can never turn a paced
    exit into an unreviewed market blast."""
    if not close_reason:
        return False
    return str(close_reason).strip().upper() in PACING_BYPASS_EXIT_REASONS


def remaining_from_fills(target_qty: int, already_filled: int) -> int:
    """RESTART-DURABLE remaining = target - already_filled, never negative. On
    resume `already_filled` is read from the reconciled position row (entry: the
    current position qty; exit: original_qty - current_open_qty)."""
    return max(0, int(target_qty or 0) - int(already_filled or 0))


# ══════════════════════════════════════════════════════════════════════════════
# DATACLASSES
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class WorkedParent:
    """One logical worked order. side is the ORDER side of every child:
    BUY for a long entry / short cover; SELL for a long exit / short entry."""
    symbol: str
    side: str
    target_qty: int
    kind: str = "entry"                     # "entry" | "exit"
    product: str = "MIS"
    instrument_type: str = "EQ"
    session_id: str = ""
    broker_profile: Optional[str] = None
    deadline_ts: Optional[float] = None     # epoch seconds; None → schedule by max_children
    interval_sec: float = 20.0
    participation_pct: float = 0.10
    freeze_cap: Optional[int] = None
    min_child_qty: int = 1
    max_children: int = 500                  # hard safety cap on child count


@dataclass
class WorkedChild:
    idx: int
    requested_qty: int
    filled_qty: int
    avg_price: float
    status: str
    broker_order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    reason: Optional[str] = None
    recent_volume: Optional[float] = None
    ts: str = ""


@dataclass
class WorkedResult:
    symbol: str
    side: str
    kind: str
    target_qty: int
    filled_qty: int
    avg_fill_price: float
    n_children: int
    shortfall: int
    remaining: int
    stopped_reason: Optional[str]
    deadline_hit: bool
    children: List[WorkedChild] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return self.remaining <= 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol, "side": self.side, "kind": self.kind,
            "target_qty": self.target_qty, "filled_qty": self.filled_qty,
            "avg_fill_price": self.avg_fill_price, "n_children": self.n_children,
            "shortfall": self.shortfall, "remaining": self.remaining,
            "stopped_reason": self.stopped_reason, "deadline_hit": self.deadline_hit,
            "complete": self.complete,
            "children": [
                {"idx": c.idx, "requested_qty": c.requested_qty,
                 "filled_qty": c.filled_qty, "avg_price": c.avg_price,
                 "status": c.status, "broker_order_id": c.broker_order_id,
                 "client_order_id": c.client_order_id, "reason": c.reason,
                 "recent_volume": c.recent_volume, "ts": c.ts}
                for c in self.children],
        }


# The place_child contract: an async callable
#   place_child(*, idx: int, qty: int, recent_volume: Optional[float]) -> dict
# returning at least {filled_qty, avg_price, status}. Optional keys:
#   broker_order_id, client_order_id, reason;
#   rejected=True + error → FAIL-CLOSED: stop working this name (place nothing more);
#   skip=True   + reason  → no fill THIS interval (e.g. a stale-quote skip), keep
#                           working the name on the next interval.
PlaceChild = Callable[..., Awaitable[Dict[str, Any]]]
VolumeFn = Callable[[str], Optional[float]]


# ══════════════════════════════════════════════════════════════════════════════
# THE LOOP
# ══════════════════════════════════════════════════════════════════════════════

async def work_order(parent: WorkedParent, *, place_child: PlaceChild,
                     volume_fn: Optional[VolumeFn] = None,
                     now_fn: Optional[Callable[[], float]] = None,
                     sleep_fn: Optional[Callable[[float], Awaitable[None]]] = None,
                     stop_fn: Optional[Callable[[], bool]] = None,
                     child_sizer: Optional["ChildSizer"] = None) -> WorkedResult:
    """Work `parent` into child slices until remaining == 0, the deadline, or a
    stop/kill. Returns the WorkedResult (filled_qty, avg_fill_price, n_children,
    shortfall = target - filled, per-child audit).

    Injection points (production uses the defaults; tests inject):
      * place_child : places + CONFIRMS one child through the safe place path.
      * volume_fn   : recent-interval volume for the POV size (None → TWAP floor
                      only).
      * now_fn      : the clock (default time.time epoch); deadline_ts is on the
                      SAME scale.
      * sleep_fn    : the pacing sleep (default asyncio.sleep).
      * stop_fn     : returns True to STOP working NOW (a manual stop / kill / a
                      portfolio breaker) — whatever filled IS the position.
      * child_sizer : an OPTIONAL pluggable child-target strategy (v2). None (the
                      DEFAULT) → EXACTLY v1's flat POV `next_child_qty(...)` — the
                      byte-identical fallback. A sizer that returns None for a given
                      interval ALSO falls back to v1 for that interval (e.g. the
                      VWAP sizer with no time window). Every safety guard below is
                      applied to the sizer's qty identically to v1's.

    FAIL-CLOSED: a child that REJECTS or whose place_child RAISES (e.g. an
    inconclusive query-before-retry → OrderTimeoutError) STOPS working this name
    and surfaces the reason. We NEVER blindly place another child when a child
    could not be confirmed (the exact double-fill direction to avoid)."""
    now_fn = now_fn or time.time
    sleep_fn = sleep_fn or asyncio.sleep
    remaining = int(parent.target_qty or 0)
    filled = 0
    weighted = 0.0
    children: List[WorkedChild] = []
    stopped: Optional[str] = None

    while remaining > 0:
        if len(children) >= int(parent.max_children):
            stopped = f"max_children ({parent.max_children}) reached"
            break
        now = now_fn()
        if parent.deadline_ts is not None and now >= float(parent.deadline_ts):
            stopped = "deadline"
            break
        if stop_fn is not None:
            try:
                if stop_fn():
                    stopped = "stopped"
                    break
            except Exception as e:  # never let a stop-probe error strand the loop
                log.debug("worked %s stop_fn raised: %s", parent.symbol, e)

        if parent.deadline_ts is not None:
            il = intervals_left(now, float(parent.deadline_ts), parent.interval_sec)
        else:
            il = max(1, int(parent.max_children) - len(children))

        vol: Optional[float] = None
        if volume_fn is not None:
            try:
                vol = volume_fn(parent.symbol)
            except Exception as e:  # a stale/failed read → TWAP floor governs
                log.debug("worked %s volume_fn raised: %s", parent.symbol, e)
                vol = None

        if child_sizer is None:
            qty = next_child_qty(remaining, il, vol, parent.participation_pct,
                                 parent.freeze_cap, parent.min_child_qty)
        else:
            # v2: a pluggable child-target strategy (VWAP-curve pacing). It shares
            # this loop + EVERY safety guard below. A None return = the strategy
            # declined this interval (no profile / no window) → v1 flat POV fallback.
            ctx = SizerContext(
                remaining=remaining, n_intervals=il, recent_volume=vol,
                now_ts=float(now), deadline_ts=parent.deadline_ts, filled=filled,
                target_qty=int(parent.target_qty or 0), n_children=len(children))
            qty = child_sizer(parent, ctx)
            if qty is None:
                qty = next_child_qty(remaining, il, vol, parent.participation_pct,
                                     parent.freeze_cap, parent.min_child_qty)
            else:
                qty = int(qty)
        if qty <= 0:
            stopped = "no_progress"
            break

        idx = len(children)
        try:
            res = await place_child(idx=idx, qty=qty, recent_volume=vol)
        except Exception as e:
            # FAIL-CLOSED: the placement could not be confirmed (e.g. an
            # inconclusive query-before-retry raised OrderTimeoutError). Do NOT
            # place another child — record the error and stop working this name.
            log.error("worked %s child %d place RAISED: %s — stopping (fail-closed)",
                      parent.symbol, idx, e)
            children.append(WorkedChild(
                idx=idx, requested_qty=qty, filled_qty=0, avg_price=0.0,
                status="ERROR", reason=str(e), recent_volume=vol, ts=_now_iso()))
            stopped = f"child {idx} place error: {e}"
            break

        res = res or {}
        cf = int(res.get("filled_qty") or 0)
        px = float(res.get("avg_price") or 0.0)
        status = str(res.get("status")
                     or ("REJECTED" if res.get("rejected")
                         else ("SKIP" if res.get("skip") else "OK")))
        children.append(WorkedChild(
            idx=idx, requested_qty=qty, filled_qty=cf, avg_price=px,
            status=status, broker_order_id=res.get("broker_order_id"),
            client_order_id=res.get("client_order_id"),
            reason=res.get("reason") or res.get("error"),
            recent_volume=vol, ts=_now_iso()))

        if res.get("rejected"):
            # FAIL-CLOSED — a broker/exchange rejection stops this name.
            stopped = f"child {idx} rejected: {res.get('error') or res.get('reason')}"
            break

        if cf > 0:
            filled += cf
            weighted += cf * px
            remaining -= cf
        if res.get("stop_after"):
            # The child asked to STOP after booking its (possibly partial) fill —
            # e.g. an exit child that PARTIAL/TIMED-OUT and cancelled its resting
            # remainder. Count the fill above, then stop working this name.
            stopped = res.get("reason") or f"child {idx} stop_after"
            break
        if remaining <= 0:
            break
        # PACE: wait the interval before the next child (skipped once done above).
        await sleep_fn(float(parent.interval_sec))

    avg = (weighted / filled) if filled > 0 else 0.0
    return WorkedResult(
        symbol=parent.symbol, side=parent.side, kind=parent.kind,
        target_qty=int(parent.target_qty or 0), filled_qty=filled,
        avg_fill_price=avg, n_children=len(children),
        shortfall=max(0, int(parent.target_qty or 0) - filled),
        remaining=max(0, remaining), stopped_reason=stopped,
        deadline_hit=(stopped == "deadline"), children=children)


# ══════════════════════════════════════════════════════════════════════════════
# PAPER FILL SIMULATION
# ══════════════════════════════════════════════════════════════════════════════

def simulate_child_fill(symbol: str, side: str, qty: int, ltp: Optional[float], *,
                        sim_volume: Optional[float] = None,
                        participation_pct: float = 0.10,
                        slippage_bps: float = 3.0, idx: int = 0
                        ) -> Dict[str, Any]:
    """PAPER child fill: fill up to `participation_pct × sim_volume` of `qty` at
    LTP ± a few bps of slippage (BUY pays up, SELL gives up). When `sim_volume` is
    None the paper model fills the whole `qty` (ample-liquidity default). Returns a
    place_child-shaped dict. NO real order is ever placed."""
    ltp = float(ltp or 0.0)
    if sim_volume is None:
        fillable = int(qty)
    else:
        cap = participation_qty(sim_volume, participation_pct)
        fillable = min(int(qty), max(0, cap))
    bps = float(slippage_bps) / 10000.0
    px = round(ltp * (1.0 + bps), 2) if str(side).upper() == "BUY" \
        else round(ltp * (1.0 - bps), 2)
    return {"filled_qty": int(fillable),
            "avg_price": px if fillable > 0 else 0.0,
            "status": "SIM_FILL" if fillable > 0 else "SIM_NOFILL",
            "broker_order_id": f"paper-{symbol}-{idx}", "client_order_id": None}


# ══════════════════════════════════════════════════════════════════════════════
# RECENT-INTERVAL VOLUME (the POV signal) — best-effort, read-only, never raises
# ══════════════════════════════════════════════════════════════════════════════

def recent_interval_volume(symbol: str, *, db_path: Optional[str] = None,
                           n_bars: int = 1, segment: str = "CASH"
                           ) -> Optional[float]:
    """Summed per-minute volume over the last `n_bars` minute-bars for `symbol`
    from the poller's mkt_orderflow_1min. Returns None on ANY error / no data /
    missing DB (the engine then relies on the TWAP floor). Read-only (mode=ro,
    query_only), never raises — a missing poller must never break the order path."""
    try:
        p = Path(db_path) if db_path else _DEFAULT_UNIVERSE_DB
        if not p.exists():
            return None
        uri = f"file:{p.as_posix()}?mode=ro"
        con = sqlite3.connect(uri, uri=True, timeout=5)
        try:
            con.execute("PRAGMA query_only=ON")
            rows = con.execute(
                "SELECT volume FROM mkt_orderflow_1min "
                "WHERE symbol=? AND segment=? ORDER BY bar_time DESC LIMIT ?",
                (symbol, segment, int(max(1, n_bars)))).fetchall()
        finally:
            con.close()
        if not rows:
            return None
        tot = sum(int(r[0] or 0) for r in rows)
        return float(tot) if tot > 0 else None
    except Exception as e:  # pragma: no cover - defensive; poller is optional
        log.debug("recent_interval_volume(%s) failed: %s", symbol, e)
        return None
