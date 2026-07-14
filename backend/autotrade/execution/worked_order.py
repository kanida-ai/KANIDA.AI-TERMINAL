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
                     stop_fn: Optional[Callable[[], bool]] = None) -> WorkedResult:
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

        qty = next_child_qty(remaining, il, vol, parent.participation_pct,
                             parent.freeze_cap, parent.min_child_qty)
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
