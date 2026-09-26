"""Derivatives signal layer -- the seven definitions of DERIVATIVES_SPEC.md section 3.

Ownership (build split, 2026-09-18)
-----------------------------------
This module and ``metrics_cli.py`` are worker **D2**'s only files.  Worker D1
owns ``instruments.py``, ``store.py``, ``schema.sql``, ``capture.py``,
``backfill.py`` and ``cli.py`` in this package; nothing here edits them.  The
loaders below read D1's tables (``contracts``, ``snapshots``, ``candles_15m``,
``underlying_snapshots``) by the names the spec fixes, and the writer adapts
itself to whatever columns D1's ``metrics`` table actually has (see
``write_metric_rows``), so the two halves can land in either order.

Shape of the module
-------------------
1. **Pure functions** (no database, no clock, no network) implement every one of
   the seven signals.  They take numbers and return dataclasses.  Every one of
   them returns an explicit ``status`` string when an input is missing --
   *never* a default number.  ``STATUS_OK`` means the number is real.
2. **A thin layer** on top opens ``db/derivatives.db``, loads the inputs,
   calls the pure functions, writes rows, and serves the five read shapes the
   API/UI needs.

Honesty rules that are enforced in code, not in comments (spec section 5):

* a ratio with fewer than ``MIN_BASELINE_SESSIONS`` sessions behind it returns
  ``STATUS_NO_BASELINE`` and ``ratio=None`` -- the UI must print "no baseline";
* the 15-minute build-up label and the day-on-day build-up label are separate
  fields and are never merged into one "the" label;
* every row carries ``days_to_expiry`` so expiry-day churn cannot masquerade as
  unusual activity;
* the liquidity floors are constants here and are exported for display.

Volume convention: Kite's NFO volume is in **units** (contracts x lot size), so
premium in rupees is ``volume * average_price`` with no lot multiplier.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import sqlite3
import statistics
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

LOG = logging.getLogger("market_data.derivatives.metrics")

DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "db" / "derivatives.db"

# ---------------------------------------------------------------------------
# Statuses.  A signal either has a number, or it says why it has not.
# ---------------------------------------------------------------------------
STATUS_OK = "ok"
STATUS_NO_BASELINE = "no baseline"
STATUS_NO_PRIOR = "no prior session"
STATUS_NO_DATA = "no data"
STATUS_NO_SPOT = "no spot"
STATUS_NO_OI = "no oi"
#: Some inputs were missing (None).  A missing value is never read as zero, so
#: a ratio over an incomplete side is not computed; the raw sums and missing
#: counts travel with the result.
STATUS_INCOMPLETE = "incomplete"

# ---------------------------------------------------------------------------
# Section 3.1 labels.  Exactly the four of the spec table, plus two honest
# non-labels: FLAT (a change of zero is none of the four) and "no data".
# ---------------------------------------------------------------------------
BUILDUP_LONG = "Long build-up"          # price up,   OI up
BUILDUP_SHORT = "Short build-up"        # price down, OI up
BUILDUP_SHORT_COVER = "Short covering"  # price up,   OI down
BUILDUP_LONG_UNWIND = "Long unwinding"  # price down, OI down
BUILDUP_FLAT = "Flat"                   # a zero change on either axis
BUILDUP_NO_DATA = STATUS_NO_DATA

BUILDUP_LABELS = (
    BUILDUP_LONG,
    BUILDUP_SHORT,
    BUILDUP_SHORT_COVER,
    BUILDUP_LONG_UNWIND,
)

WINDOW_15M = "15m"
WINDOW_DAY = "day"

# ---------------------------------------------------------------------------
# Liquidity floors (spec section 3, "a screen without floors is a junk list").
# These are shown in the UI; ``LiquidityFloors.as_dict()`` is the payload.
# ---------------------------------------------------------------------------

#: Premium traded floor: Rs 2 crore.
MIN_PREMIUM_TRADED_RS = 2_00_00_000.0

#: OI floor, lot-normalised: at least this many *lots* outstanding.  The spec
#: writes "OI >= 1 lot-normalised threshold" and does not pin a bigger number,
#: so the default is the literal one lot.  It is a constant, not a magic
#: number, precisely so it can be raised once we have seen a week of lists.
MIN_OI_LOTS = 1.0

#: Last-traded-price floor: Rs 1 (keeps near-worthless far strikes out).
MIN_LAST_PRICE = 1.0

#: Sessions of history a time-of-day baseline needs before it is a number.
#: Spec 3.2: "fewer than 3 sessions of history => report 'no baseline'".
MIN_BASELINE_SESSIONS = 3

#: Sessions the baseline looks back over (spec 3.2: "the last 10 sessions").
BASELINE_SESSIONS = 10

#: Sessions in the futures OI average (spec 3.7: "its own 20-day average").
FUT_OI_AVG_SESSIONS = 20

#: Spec 3.3 flags a volume-to-OI ratio "above 1.0".
VOL_OI_SPIKE_RATIO = 1.0

#: How many times its own time-of-day median counts as "unusually active".
#: The spec pins the *denominator* (3.2) but not this trigger; 2x is the
#: constant we screen at and it is displayed with the floors.
UNUSUAL_VOL_TOD_RATIO = 2.0

RS_PER_CRORE = 1_00_00_000.0


@dataclass(frozen=True)
class LiquidityFloors:
    """The floors any "unusual" list is filtered by.  Shown in the UI."""

    min_premium_rs: float = MIN_PREMIUM_TRADED_RS
    min_oi_lots: float = MIN_OI_LOTS
    min_last_price: float = MIN_LAST_PRICE
    unusual_vol_tod_ratio: float = UNUSUAL_VOL_TOD_RATIO
    vol_oi_spike_ratio: float = VOL_OI_SPIKE_RATIO

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["min_premium_cr"] = self.min_premium_rs / RS_PER_CRORE
        d["description"] = (
            f"premium traded >= Rs {self.min_premium_rs / RS_PER_CRORE:.3g} crore"
            f" and OI >= {self.min_oi_lots:g} lot(s)"
            f" and last price >= Rs {self.min_last_price:g}"
        )
        return d

    def check(
        self,
        *,
        premium_rs: float | None,
        oi: float | None,
        lot_size: int | None,
        last_price: float | None,
    ) -> tuple[bool, list[str]]:
        """Return ``(passes, failed_floor_names)``.

        A missing input fails its floor by name (``"premium:unknown"``) rather
        than passing on a default.
        """
        failed: list[str] = []
        if premium_rs is None:
            failed.append("premium:unknown")
        elif premium_rs < self.min_premium_rs:
            failed.append("premium")
        if oi is None or lot_size in (None, 0):
            failed.append("oi:unknown")
        elif (oi / float(lot_size)) < self.min_oi_lots:
            failed.append("oi")
        if last_price is None:
            failed.append("price:unknown")
        elif last_price < self.min_last_price:
            failed.append("price")
        return (not failed), failed


DEFAULT_FLOORS = LiquidityFloors()


# ---------------------------------------------------------------------------
# Input value objects.  These mirror D1's rows but are plain dataclasses, so
# every pure function below is testable with no database at all.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ContractRef:
    """One row of D1's ``contracts`` table."""

    instrument_token: int
    tradingsymbol: str
    underlying: str
    instrument_type: str            # 'CE' | 'PE' | 'FUT'
    expiry: date
    strike: float | None = None
    lot_size: int | None = None

    @property
    def is_option(self) -> bool:
        return self.instrument_type in ("CE", "PE")


@dataclass(frozen=True)
class Snapshot:
    """One row of D1's ``snapshots`` table (a 15-minute mark)."""

    instrument_token: int
    captured_at: datetime
    last_price: float | None = None
    average_price: float | None = None
    volume: float | None = None      # cumulative day volume, in units
    oi: float | None = None
    buy_quantity: float | None = None
    sell_quantity: float | None = None
    bid: float | None = None
    ask: float | None = None


@dataclass(frozen=True)
class DailyBar:
    """A session's closing state for one contract, from ``candles_15m``."""

    session: date
    close: float | None = None
    oi: float | None = None
    volume: float | None = None      # whole-session volume


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _num(value: Any) -> float | None:
    """Coerce to float, or ``None`` -- never to 0.0."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _pct(change: float | None, base: float | None) -> float | None:
    base = _num(base)
    change = _num(change)
    if change is None or base is None or base == 0:
        return None
    return 100.0 * change / abs(base)


def _lots(oi: float | None, lot_size: int | None) -> float | None:
    """OI in lots -- the unit the liquidity floor is stated in.

    Derived on the way out rather than stored: it is `oi / lot_size` and both
    are already on the row, so a column for it would only be a second copy that
    could disagree."""
    oi = _num(oi)
    if oi is None or not lot_size:
        return None
    return oi / float(lot_size)


def days_to_expiry(expiry: date | datetime | str | None, as_of: date | datetime) -> int | None:
    """Calendar days from ``as_of`` to ``expiry`` (0 on expiry day, negative after).

    Calendar days, not trading days: the spec asks for "days-to-expiry" on every
    row as an expiry-awareness guard, and a calendar count cannot silently
    depend on a holiday calendar that this module does not own.
    """
    if expiry is None:
        return None
    if isinstance(expiry, str):
        try:
            expiry = datetime.fromisoformat(expiry)
        except ValueError:
            return None
    if isinstance(expiry, datetime):
        expiry = expiry.date()
    if isinstance(as_of, datetime):
        as_of = as_of.date()
    return (expiry - as_of).days


def to_crore(rupees: float | None) -> float | None:
    v = _num(rupees)
    return None if v is None else v / RS_PER_CRORE


# ===========================================================================
# 3.1  Open-interest build-up, per strike
# ===========================================================================


@dataclass(frozen=True)
class BuildUp:
    """One window's build-up classification.  Never merged with another window."""

    window: str                      # '15m' | 'day'
    label: str
    status: str
    price_change: float | None = None
    price_change_pct: float | None = None
    oi_change: float | None = None
    oi_change_pct: float | None = None


def classify_buildup(price_change: float | None, oi_change: float | None) -> str:
    """The four labels of spec 3.1, by the sign of the two changes.

    | price | oi   | label          |
    |-------|------|----------------|
    | up    | up   | Long build-up  |
    | down  | up   | Short build-up |
    | up    | down | Short covering |
    | down  | down | Long unwinding |

    A zero change on either axis is none of the four, so it returns ``Flat``; a
    missing input returns ``"no data"``.  Neither is ever dressed up as one of
    the four.
    """
    p = _num(price_change)
    o = _num(oi_change)
    if p is None or o is None:
        return BUILDUP_NO_DATA
    if p == 0 or o == 0:
        return BUILDUP_FLAT
    if p > 0 and o > 0:
        return BUILDUP_LONG
    if p < 0 and o > 0:
        return BUILDUP_SHORT
    if p > 0 and o < 0:
        return BUILDUP_SHORT_COVER
    return BUILDUP_LONG_UNWIND


def build_up(
    *,
    window: str,
    price_now: float | None,
    price_before: float | None,
    oi_now: float | None,
    oi_before: float | None,
) -> BuildUp:
    """Classify one window from the two endpoint readings."""
    pn, pb = _num(price_now), _num(price_before)
    on, ob = _num(oi_now), _num(oi_before)
    price_change = None if (pn is None or pb is None) else pn - pb
    oi_change = None if (on is None or ob is None) else on - ob
    label = classify_buildup(price_change, oi_change)
    status = STATUS_OK if label != BUILDUP_NO_DATA else STATUS_NO_DATA
    return BuildUp(
        window=window,
        label=label,
        status=status,
        price_change=price_change,
        price_change_pct=_pct(price_change, pb),
        oi_change=oi_change,
        oi_change_pct=_pct(oi_change, ob),
    )


def buildup_15m(current: Snapshot, previous: Snapshot | None) -> BuildUp:
    """Build-up over the last 15-minute mark (spec 3.1, intraday window)."""
    if previous is None:
        return BuildUp(window=WINDOW_15M, label=BUILDUP_NO_DATA, status=STATUS_NO_DATA)
    return build_up(
        window=WINDOW_15M,
        price_now=current.last_price,
        price_before=previous.last_price,
        oi_now=current.oi,
        oi_before=previous.oi,
    )


def buildup_day(current: Snapshot, previous_close: DailyBar | None) -> BuildUp:
    """Build-up since the previous close (spec 3.1, day-on-day window)."""
    if previous_close is None:
        return BuildUp(window=WINDOW_DAY, label=BUILDUP_NO_DATA, status=STATUS_NO_PRIOR)
    return build_up(
        window=WINDOW_DAY,
        price_now=current.last_price,
        price_before=previous_close.close,
        oi_now=current.oi,
        oi_before=previous_close.oi,
    )


# ===========================================================================
# 3.2  Volume versus its own time-of-day average
# ===========================================================================


@dataclass(frozen=True)
class VolumeVsTimeOfDay:
    """Today's cumulative volume by this time / the median of the same clock time."""

    status: str
    ratio: float | None = None
    today_cumulative: float | None = None
    median_cumulative: float | None = None
    sessions_used: int = 0
    min_sessions: int = MIN_BASELINE_SESSIONS


#: The capture's candle interval. ``candles_15m`` holds 15-minute bars; the
#: python reader below infers a finer interval if the bars themselves show one.
DEFAULT_BAR_MINUTES = 15


def _bar_interval(starts: Sequence[datetime]) -> timedelta:
    """The bar length the data itself shows, never longer than the default.

    The smallest positive gap between consecutive bars of one session is the
    interval (5-minute bars show a 5-minute gap).  A gap LONGER than the
    default proves nothing -- bars go missing -- so it never stretches a bar
    past ``DEFAULT_BAR_MINUTES``.
    """
    default = timedelta(minutes=DEFAULT_BAR_MINUTES)
    by_day: dict[date, list[datetime]] = {}
    for s in starts:
        by_day.setdefault(s.date(), []).append(s)
    best: timedelta | None = None
    for day_starts in by_day.values():
        ordered = sorted(set(day_starts))
        for a, b in zip(ordered, ordered[1:]):
            gap = b - a
            if gap > timedelta(0) and (best is None or gap < best):
                best = gap
    return min(best, default) if best is not None else default


def cumulative_by_time_of_day(
    bars: Iterable[Mapping[str, Any]],
    *,
    cutoff: datetime,
    sessions: int = BASELINE_SESSIONS,
    interval: timedelta | None = None,
) -> list[float]:
    """Per-session cumulative volume up to the same clock time as ``cutoff``.

    ``bars`` are candles for **one contract** as mappings with ``bar_start``
    (``datetime`` or ISO string) and ``volume``.  A bar counts only when it has
    ENDED by the cutoff's clock time (``bar_start + interval <= cutoff``): the
    09:30 mark's cumulative volume holds the 09:15-09:30 bar, so the baseline
    must not borrow the 09:30-09:45 bar from prior days (E04).  ``interval``
    defaults to what the bars show, capped at 15 minutes.

    Sessions on or after ``cutoff``'s own date are excluded -- point-in-time:
    today's partial session can never be part of its own baseline.  The most
    recent ``sessions`` prior sessions are returned, oldest first.
    """
    tod = cutoff.time()
    today = cutoff.date()
    parsed: list[tuple[datetime, Any]] = []
    for bar in bars:
        raw = bar.get("bar_start")
        if isinstance(raw, str):
            try:
                raw = datetime.fromisoformat(raw)
            except ValueError:
                continue
        if not isinstance(raw, datetime):
            continue
        parsed.append((raw, bar.get("volume")))
    step = interval if interval is not None else _bar_interval([p[0] for p in parsed])
    per_session: dict[date, float] = {}
    for raw, volume in parsed:
        if raw.date() >= today:
            continue
        end = raw + step
        if end.date() != raw.date() or end.time() > tod:
            continue
        vol = _num(volume)
        if vol is None:
            continue
        per_session[raw.date()] = per_session.get(raw.date(), 0.0) + vol
    ordered = [per_session[d] for d in sorted(per_session)]
    return ordered[-sessions:] if sessions else ordered


def volume_vs_time_of_day(
    today_cumulative: float | None,
    baseline_cumulatives: Sequence[float] | None,
    *,
    min_sessions: int = MIN_BASELINE_SESSIONS,
) -> VolumeVsTimeOfDay:
    """Spec 3.2.

    ``today's cumulative volume by this time of day / median of the cumulative
    volume by the same time of day over the last 10 sessions, for that exact
    contract``.  Fewer than ``min_sessions`` sessions of history returns
    ``"no baseline"`` and ``ratio=None`` -- never a ratio.
    """
    today = _num(today_cumulative)
    clean = [v for v in (_num(x) for x in (baseline_cumulatives or [])) if v is not None]
    if len(clean) < min_sessions:
        return VolumeVsTimeOfDay(
            status=STATUS_NO_BASELINE,
            today_cumulative=today,
            sessions_used=len(clean),
            min_sessions=min_sessions,
        )
    median = statistics.median(clean)
    if today is None:
        return VolumeVsTimeOfDay(
            status=STATUS_NO_DATA,
            median_cumulative=median,
            sessions_used=len(clean),
            min_sessions=min_sessions,
        )
    if median <= 0:
        # A zero median is not a divisor; it is an absent baseline.
        return VolumeVsTimeOfDay(
            status=STATUS_NO_BASELINE,
            today_cumulative=today,
            median_cumulative=median,
            sessions_used=len(clean),
            min_sessions=min_sessions,
        )
    return VolumeVsTimeOfDay(
        status=STATUS_OK,
        ratio=today / median,
        today_cumulative=today,
        median_cumulative=median,
        sessions_used=len(clean),
        min_sessions=min_sessions,
    )


# ===========================================================================
# 3.3  Volume-to-OI spike
# ===========================================================================


@dataclass(frozen=True)
class VolumeToOi:
    status: str
    ratio: float | None = None
    day_volume: float | None = None
    prev_day_oi: float | None = None
    is_spike: bool = False
    threshold: float = VOL_OI_SPIKE_RATIO


def volume_to_oi(
    day_volume: float | None,
    prev_day_close_oi: float | None,
    *,
    threshold: float = VOL_OI_SPIKE_RATIO,
) -> VolumeToOi:
    """Spec 3.3: ``day volume / previous-day closing OI``, flagged above 1.0.

    The raw numbers travel beside the ratio, as the spec asks.  No previous
    session (or a zero previous OI) is ``"no prior session"``, not a ratio.
    """
    vol = _num(day_volume)
    oi = _num(prev_day_close_oi)
    if oi is None:
        return VolumeToOi(status=STATUS_NO_PRIOR, day_volume=vol, threshold=threshold)
    if oi <= 0:
        return VolumeToOi(
            status=STATUS_NO_OI, day_volume=vol, prev_day_oi=oi, threshold=threshold
        )
    if vol is None:
        return VolumeToOi(status=STATUS_NO_DATA, prev_day_oi=oi, threshold=threshold)
    ratio = vol / oi
    return VolumeToOi(
        status=STATUS_OK,
        ratio=ratio,
        day_volume=vol,
        prev_day_oi=oi,
        is_spike=ratio > threshold,
        threshold=threshold,
    )


# ===========================================================================
# 3.4  Premium traded
# ===========================================================================


@dataclass(frozen=True)
class PremiumTraded:
    status: str
    rupees: float | None = None
    crore: float | None = None
    volume: float | None = None
    average_price: float | None = None


def premium_traded(volume: float | None, average_price: float | None) -> PremiumTraded:
    """Spec 3.4: ``volume x average_price`` in rupees, reported in Rs crore.

    NFO volume is already in units (contracts x lot size), so there is no lot
    multiplier here.  A missing leg is a status, not a zero.
    """
    vol = _num(volume)
    avg = _num(average_price)
    if vol is None or avg is None:
        return PremiumTraded(status=STATUS_NO_DATA, volume=vol, average_price=avg)
    rupees = vol * avg
    return PremiumTraded(
        status=STATUS_OK,
        rupees=rupees,
        crore=rupees / RS_PER_CRORE,
        volume=vol,
        average_price=avg,
    )


def premium_rollup(premiums: Iterable[PremiumTraded]) -> PremiumTraded:
    """Sum the contracts that have a real number; say so if none do."""
    total = 0.0
    seen = 0
    for p in premiums:
        if p.status == STATUS_OK and p.rupees is not None:
            total += p.rupees
            seen += 1
    if not seen:
        return PremiumTraded(status=STATUS_NO_DATA)
    return PremiumTraded(status=STATUS_OK, rupees=total, crore=total / RS_PER_CRORE)


# ===========================================================================
# 3.5  Put-call ratio
# ===========================================================================


@dataclass(frozen=True)
class PutCallRatio:
    status: str
    pcr_oi: float | None = None
    pcr_volume: float | None = None
    ce_oi: float = 0.0
    pe_oi: float = 0.0
    ce_volume: float = 0.0
    pe_volume: float = 0.0
    strikes: int = 0
    trend: str | None = None         # filled by pcr_trend()
    trend_change: float | None = None
    # E06 completeness: how many legs of each side carried NO value (None).  The
    # sums above are over KNOWN values only; a ratio is computed only when both
    # of its sides are complete.
    ce_oi_missing: int = 0
    pe_oi_missing: int = 0
    ce_volume_missing: int = 0
    pe_volume_missing: int = 0
    ce_legs: int = 0
    pe_legs: int = 0
    oi_complete: bool = True
    volume_complete: bool = True
    pcr_oi_status: str | None = None
    pcr_volume_status: str | None = None


def _ratio(numerator: float, denominator: float) -> float | None:
    if denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def put_call_ratio(legs: Iterable[Mapping[str, Any]]) -> PutCallRatio:
    """Spec 3.5, for one underlying and one expiry.

    ``OI PCR = sum(PE OI) / sum(CE OI)``; ``volume PCR = sum(PE volume) /
    sum(CE volume)``.  ``legs`` are mappings with ``instrument_type`` (CE/PE),
    ``oi`` and ``volume``.  A zero call side means the ratio is undefined, and
    it is reported as ``None`` with the raw sums beside it -- not as infinity
    and not as zero.

    E06: a leg whose OI (or volume) is missing (None) is NOT a zero.  Either
    side with a missing value makes that ratio undefined (``None``) and the
    status ``"incomplete"``; the missing counts and completeness flags travel
    with the result, so a PCR of 0 can only come from a real zero put side.
    """
    ce_oi = pe_oi = ce_vol = pe_vol = 0.0
    miss = {"ce_oi": 0, "pe_oi": 0, "ce_vol": 0, "pe_vol": 0}
    n_ce = n_pe = 0
    strikes: set[Any] = set()
    for leg in legs:
        typ = str(leg.get("instrument_type", "")).upper()
        if typ not in ("CE", "PE"):
            continue
        strikes.add(leg.get("strike"))
        oi = _num(leg.get("oi"))
        vol = _num(leg.get("volume"))
        side = "ce" if typ == "CE" else "pe"
        if side == "ce":
            n_ce += 1
        else:
            n_pe += 1
        if oi is None:
            miss[f"{side}_oi"] += 1
        elif side == "ce":
            ce_oi += oi
        else:
            pe_oi += oi
        if vol is None:
            miss[f"{side}_vol"] += 1
        elif side == "ce":
            ce_vol += vol
        else:
            pe_vol += vol
    if not strikes:
        return PutCallRatio(status=STATUS_NO_DATA)
    oi_complete = n_ce > 0 and n_pe > 0 and miss["ce_oi"] == 0 and miss["pe_oi"] == 0
    vol_complete = n_ce > 0 and n_pe > 0 and miss["ce_vol"] == 0 and miss["pe_vol"] == 0
    pcr_oi = _ratio(pe_oi, ce_oi) if oi_complete else None
    pcr_vol = _ratio(pe_vol, ce_vol) if vol_complete else None

    def _one(value: float | None, complete: bool) -> str:
        if not complete:
            return STATUS_INCOMPLETE
        return STATUS_OK if value is not None else STATUS_NO_DATA

    oi_status = _one(pcr_oi, oi_complete)
    vol_status = _one(pcr_vol, vol_complete)
    if not (oi_complete and vol_complete):
        status = STATUS_INCOMPLETE
    else:
        status = STATUS_OK if (pcr_oi is not None or pcr_vol is not None) else STATUS_NO_DATA
    return PutCallRatio(
        status=status,
        pcr_oi=pcr_oi,
        pcr_volume=pcr_vol,
        ce_oi=ce_oi,
        pe_oi=pe_oi,
        ce_volume=ce_vol,
        pe_volume=pe_vol,
        strikes=len(strikes),
        ce_oi_missing=miss["ce_oi"],
        pe_oi_missing=miss["pe_oi"],
        ce_volume_missing=miss["ce_vol"],
        pe_volume_missing=miss["pe_vol"],
        ce_legs=n_ce,
        pe_legs=n_pe,
        oi_complete=oi_complete,
        volume_complete=vol_complete,
        pcr_oi_status=oi_status,
        pcr_volume_status=vol_status,
    )


def pcr_trend(series: Sequence[float | None]) -> tuple[str, float | None]:
    """The day's PCR trend: last reading versus the first of the day.

    Returns ``(label, change)`` where label is ``rising`` / ``falling`` /
    ``flat``, or ``"no baseline"`` with ``None`` when there are fewer than two
    real readings to compare.
    """
    clean = [v for v in (_num(x) for x in series) if v is not None]
    if len(clean) < 2:
        return STATUS_NO_BASELINE, None
    change = clean[-1] - clean[0]
    if change > 0:
        return "rising", change
    if change < 0:
        return "falling", change
    return "flat", 0.0


# ===========================================================================
# 3.6  Max pain
# ===========================================================================


@dataclass(frozen=True)
class MaxPain:
    status: str
    strike: float | None = None
    total_payout: float | None = None
    distance_from_spot: float | None = None
    distance_pct: float | None = None
    total_oi: float = 0.0
    strikes_used: int = 0
    payout_by_strike: dict[float, float] = field(default_factory=dict)
    # E06: every strike whose payout equals the minimum (a flat bottom is a
    # RANGE, not a point), its low/high ends, and how complete the OI was.
    tied_strikes: tuple[float, ...] = ()
    tie_low: float | None = None
    tie_high: float | None = None
    oi_missing: int = 0
    legs: int = 0


def max_pain(
    oi_by_strike: Mapping[float, Mapping[str, float]] | Iterable[Mapping[str, Any]],
    *,
    spot: float | None = None,
) -> MaxPain:
    """Spec 3.6: the settlement strike that MINIMISES the total intrinsic payout
    owed to option holders of this expiry.

    At an expiry settlement price ``S`` the holders of the calls are paid
    ``ce_oi_K * max(0, S - K)`` and the holders of the puts ``pe_oi_K *
    max(0, K - S)``, summed over every strike ``K`` of that expiry.  The
    candidate settlement prices are the listed strikes themselves.  It is a
    payout-minimisation arithmetic over the OI supplied -- NOT "the strike where
    most OI expires worthless", not a pin, and not a target or forecast.

    Ties (a genuinely flat payout curve) are returned as ``tied_strikes`` with
    ``tie_low``/``tie_high``; the single ``strike`` resolves to the tied strike
    nearest the spot, or the lowest when there is no spot.  The tie rule is
    stated rather than hidden.

    Missing OI (None) is not zero: it is counted in ``oi_missing`` and the
    status is ``"incomplete"`` (the strike is still computed over the known OI,
    so a reader can see what the known part says).  Payouts use sorted prefix
    sums: O(n log n) rather than O(n^2).

    ``oi_by_strike`` is either ``{strike: {"ce_oi": .., "pe_oi": ..}}`` or an
    iterable of leg mappings with ``strike``/``instrument_type``/``oi``.
    """
    table: dict[float, dict[str, float]] = {}
    missing = 0
    legs = 0
    if isinstance(oi_by_strike, Mapping):
        for raw_k, vals in oi_by_strike.items():
            k = _num(raw_k)
            if k is None:
                continue
            row = table.setdefault(k, {"ce_oi": 0.0, "pe_oi": 0.0})
            for key in ("ce_oi", "pe_oi"):
                if key not in vals:
                    continue
                legs += 1
                v = _num(vals.get(key))
                if v is None:
                    missing += 1
                else:
                    row[key] += v
    else:
        for leg in oi_by_strike:
            k = _num(leg.get("strike"))
            typ = str(leg.get("instrument_type", "")).upper()
            if k is None or typ not in ("CE", "PE"):
                continue
            legs += 1
            row = table.setdefault(k, {"ce_oi": 0.0, "pe_oi": 0.0})
            v = _num(leg.get("oi"))
            if v is None:
                missing += 1
            else:
                row["ce_oi" if typ == "CE" else "pe_oi"] += v

    if not table:
        return MaxPain(status=STATUS_NO_DATA, oi_missing=missing, legs=legs)

    total_oi = sum(r["ce_oi"] + r["pe_oi"] for r in table.values())
    if total_oi <= 0:
        return MaxPain(
            status=STATUS_INCOMPLETE if missing else STATUS_NO_OI,
            strikes_used=len(table),
            oi_missing=missing,
            legs=legs,
        )

    ks = sorted(table)
    n = len(ks)
    # calls strictly below the settle pay (S-K): S*sum(ce) - sum(ce*K) over K<S
    # puts strictly above the settle pay (K-S): sum(pe*K) - S*sum(pe) over K>S
    ce_cum = ce_k_cum = 0.0
    below: list[tuple[float, float]] = []
    for k in ks:
        below.append((ce_cum, ce_k_cum))
        ce_cum += table[k]["ce_oi"]
        ce_k_cum += table[k]["ce_oi"] * k
    pe_cum = pe_k_cum = 0.0
    above: list[tuple[float, float]] = [(0.0, 0.0)] * n
    for i in range(n - 1, -1, -1):
        above[i] = (pe_cum, pe_k_cum)
        pe_cum += table[ks[i]]["pe_oi"]
        pe_k_cum += table[ks[i]]["pe_oi"] * ks[i]
    payouts: dict[float, float] = {}
    for i, settle in enumerate(ks):
        c, ck = below[i]
        p, pk = above[i]
        payouts[settle] = max(0.0, (settle * c - ck) + (pk - settle * p))

    best = min(payouts.values())
    # prefix sums can differ from the direct sum in the last bits; a tie is a
    # payout within a relative 1e-12 of the minimum.
    tol = 1e-12 * max(1.0, abs(best))
    candidates = [k for k in ks if payouts[k] - best <= tol]
    spot_v = _num(spot)
    if len(candidates) > 1 and spot_v is not None:
        chosen = min(candidates, key=lambda k: (abs(k - spot_v), k))
    else:
        chosen = min(candidates)

    distance = None if spot_v is None else chosen - spot_v
    return MaxPain(
        status=STATUS_INCOMPLETE if missing else STATUS_OK,
        strike=chosen,
        total_payout=payouts[chosen],
        distance_from_spot=distance,
        distance_pct=None if (spot_v is None or spot_v == 0) else 100.0 * (chosen - spot_v) / spot_v,
        total_oi=total_oi,
        strikes_used=len(table),
        payout_by_strike=payouts,
        tied_strikes=tuple(candidates),
        tie_low=min(candidates),
        tie_high=max(candidates),
        oi_missing=missing,
        legs=legs,
    )


@dataclass(frozen=True)
class FuturesBuildUp:
    tradingsymbol: str
    underlying: str
    expiry: date | None
    days_to_expiry: int | None
    buildup_15m: BuildUp
    buildup_day: BuildUp
    oi: float | None = None
    oi_avg_sessions: int = 0
    oi_avg: float | None = None
    oi_vs_avg: float | None = None       # OI as a share of its own 20-day average
    oi_vs_avg_status: str = STATUS_NO_BASELINE
    last_price: float | None = None
    spot: float | None = None
    basis: float | None = None           # futures - spot
    basis_pct: float | None = None
    basis_status: str = STATUS_NO_SPOT
    premium: PremiumTraded | None = None


def oi_vs_average(
    oi_now: float | None,
    history: Sequence[float] | None,
    *,
    sessions: int = FUT_OI_AVG_SESSIONS,
    min_sessions: int = MIN_BASELINE_SESSIONS,
) -> tuple[float | None, float | None, int, str]:
    """OI as a share of its own N-day average closing OI.

    Returns ``(share, average, sessions_used, status)``.  Fewer than
    ``min_sessions`` prior sessions is ``"no baseline"`` -- the same floor spec
    3.2 sets for the volume baseline, applied here because the spec fixes the
    window (20 days) but not the minimum, and a 1-session "20-day average"
    would be a fabricated number.
    """
    clean = [v for v in (_num(x) for x in (history or [])) if v is not None][-sessions:]
    if len(clean) < min_sessions:
        return None, None, len(clean), STATUS_NO_BASELINE
    avg = statistics.fmean(clean)
    now = _num(oi_now)
    if now is None:
        return None, avg, len(clean), STATUS_NO_DATA
    if avg <= 0:
        return None, avg, len(clean), STATUS_NO_BASELINE
    return now / avg, avg, len(clean), STATUS_OK


def futures_basis(futures_price: float | None, spot: float | None) -> tuple[float | None, float | None, str]:
    """Spec 3.7 basis: ``futures - spot``.  Returns ``(basis, basis_pct, status)``."""
    f = _num(futures_price)
    s = _num(spot)
    if f is None:
        return None, None, STATUS_NO_DATA
    if s is None:
        return None, None, STATUS_NO_SPOT
    basis = f - s
    return basis, (None if s == 0 else 100.0 * basis / s), STATUS_OK


def compute_futures_buildup(
    contract: ContractRef,
    current: Snapshot,
    *,
    previous: Snapshot | None = None,
    previous_close: DailyBar | None = None,
    oi_history: Sequence[float] | None = None,
    spot: float | None = None,
    as_of: date | datetime | None = None,
) -> FuturesBuildUp:
    """Spec 3.7 on one futures contract: the four labels, OI vs its own
    20-day average, and the basis."""
    as_of = as_of or current.captured_at
    share, avg, used, status = oi_vs_average(current.oi, oi_history)
    basis, basis_pct, basis_status = futures_basis(current.last_price, spot)
    return FuturesBuildUp(
        tradingsymbol=contract.tradingsymbol,
        underlying=contract.underlying,
        expiry=contract.expiry,
        days_to_expiry=days_to_expiry(contract.expiry, as_of),
        buildup_15m=buildup_15m(current, previous),
        buildup_day=buildup_day(current, previous_close),
        oi=_num(current.oi),
        oi_avg_sessions=used,
        oi_avg=avg,
        oi_vs_avg=share,
        oi_vs_avg_status=status,
        last_price=_num(current.last_price),
        spot=_num(spot),
        basis=basis,
        basis_pct=basis_pct,
        basis_status=basis_status,
        premium=premium_traded(current.volume, current.average_price),
    )


# ===========================================================================
# What "unusual" IS: a small, closed registry of RULES -- not a bag of prose
# ===========================================================================
#
# Two rules flag a contract as unusual, and only two: 3.2 (volume against this
# contract's own time-of-day median) and 3.3 (the day's volume against what was
# standing at yesterday's close).  Each one fires at many different NUMBERS, so
# the human sentence it produces is different every time -- "volume 206.0x its
# own time-of-day median" and "volume 781.9x its own time-of-day median" are the
# SAME rule at two readings.
#
# Counting those sentences is what produced "124 distinct conditions" on NIFTY
# at the 11:30 reading of 18 Sep 2026, when the truth is TWO.  A reader is owed
# the rule, not the rendering of it, so a trigger is carried as:
#
#     rule id, rule version, the measured value, the comparator, the threshold
#     it was compared against, the baseline it was measured against, and how
#     many observations that baseline stands on.
#
# The prose is DERIVED from the trigger and is kept byte-for-byte what it always
# was, so the stored `unusual_reasons` text does not change and no row anywhere
# needs rewriting.  Everything in this section is additive.

#: Bumped when a rule's measurement, comparator or threshold changes, so a
#: stored flag can be told apart from a flag this version would produce.
UNUSUAL_RULES_VERSION = 1

#: 3.2 -- this contract's cumulative volume against the median of its own
#: cumulative volume at the same clock time over the baseline sessions.
RULE_VOL_TOD = "vol_tod_median"
#: 3.3 -- the day's volume against the open interest standing at yesterday's
#: close.  One prior session is the whole baseline, so the sample count is 1.
RULE_DAY_VOL_VS_PREV_OI = "day_vol_vs_prev_oi"
#: A stored reason string this registry does not recognise.  It is carried as
#: itself rather than dropped or silently folded into one of the two above.
RULE_UNCLASSIFIED = "unclassified"


@dataclass(frozen=True)
class UnusualRule:
    """One condition, named once.  Numbers live on the trigger, never here."""

    rule_id: str
    version: int
    #: the rule's full name, for a drawer or an explanation
    label: str
    #: the same rule in a table cell's width, in the tab's existing column words
    short_label: str
    #: what is measured, in a sentence
    measure: str
    #: what it is measured against
    baseline_label: str
    #: what one observation of the baseline IS, so "3 sessions" is not read as
    #: "3 contracts"
    sample_label: str
    #: '>=' or '>' -- the comparison the code actually performs
    comparator: str
    #: the multiple the comparator is applied to at this version
    threshold: float
    #: the unit of `value` and `threshold`; both are multiples
    unit: str = "x"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


UNUSUAL_RULES: tuple[UnusualRule, ...] = (
    UnusualRule(
        rule_id=RULE_VOL_TOD,
        version=UNUSUAL_RULES_VERSION,
        label="Volume vs its own median",
        short_label="Vol vs median",
        measure="cumulative volume so far today",
        baseline_label="the median of this contract's own cumulative volume at "
        "the same clock time",
        sample_label="session",
        comparator=">=",
        threshold=UNUSUAL_VOL_TOD_RATIO,
    ),
    UnusualRule(
        rule_id=RULE_DAY_VOL_VS_PREV_OI,
        version=UNUSUAL_RULES_VERSION,
        label="Day volume vs previous-close OI",
        short_label="Day vol vs prev OI",
        measure="the day's volume",
        baseline_label="the open interest standing at the previous close",
        sample_label="prior session",
        comparator=">",
        threshold=VOL_OI_SPIKE_RATIO,
    ),
)
UNUSUAL_RULES_BY_ID: dict[str, UnusualRule] = {r.rule_id: r for r in UNUSUAL_RULES}

#: How each rule's own sentence is written, and how it is read back.  Both
#: directions live beside the rule so they cannot drift apart: the writer below
#: produces the text and the reader classifies it with the same pattern.
_REASON_TEMPLATE: dict[str, str] = {
    RULE_VOL_TOD: "volume {value:.1f}x its own time-of-day median",
    RULE_DAY_VOL_VS_PREV_OI: "day volume {value:.1f}x yesterday's OI",
}
_REASON_PATTERN: dict[str, "re.Pattern[str]"] = {
    RULE_VOL_TOD: re.compile(r"^volume\s+([0-9.]+)x its own time-of-day median$", re.I),
    RULE_DAY_VOL_VS_PREV_OI: re.compile(
        r"^day volume\s+([0-9.]+)x yesterday's OI$", re.I
    ),
}


@dataclass(frozen=True)
class UnusualTrigger:
    """One rule firing on one contract at one 15-minute reading."""

    rule_id: str
    rule_version: int
    #: the measured multiple.  None only when a stored row recorded the
    #: condition but no longer carries the number behind it.
    value: float | None
    comparator: str
    threshold: float
    #: the denominator the value was measured against, in the measure's own
    #: units (median cumulative volume; previous-day closing OI)
    baseline: float | None
    #: how many observations that baseline stands on -- sessions for 3.2, and
    #: the single prior session for 3.3
    sample_count: int | None
    unit: str = "x"
    #: the store's own words, kept for a reason this registry could not classify
    text: str = ""

    @property
    def rule(self) -> "UnusualRule | None":
        return UNUSUAL_RULES_BY_ID.get(self.rule_id)

    @property
    def reason(self) -> str:
        """The human sentence this trigger writes into ``unusual_reasons``."""
        template = _REASON_TEMPLATE.get(self.rule_id)
        if template is None or self.value is None:
            return self.text
        return template.format(value=self.value)

    def as_dict(self) -> dict[str, Any]:
        rule = self.rule
        out = asdict(self)
        out["rule_label"] = rule.label if rule else ""
        out["rule_short_label"] = rule.short_label if rule else ""
        out["measure"] = rule.measure if rule else ""
        out["baseline_label"] = rule.baseline_label if rule else ""
        out["sample_label"] = rule.sample_label if rule else ""
        return out


def unusual_triggers(
    volume_vs_tod: VolumeVsTimeOfDay,
    volume_to_oi_result: VolumeToOi,
    *,
    floors: LiquidityFloors = DEFAULT_FLOORS,
) -> list[UnusualTrigger]:
    """Every rule that fires on one contract, as structured triggers.

    The comparisons are exactly the ones this module has always made; only the
    shape of the answer is new.
    """
    out: list[UnusualTrigger] = []
    if (
        volume_vs_tod.status == STATUS_OK
        and volume_vs_tod.ratio is not None
        and volume_vs_tod.ratio >= floors.unusual_vol_tod_ratio
    ):
        out.append(
            UnusualTrigger(
                rule_id=RULE_VOL_TOD,
                rule_version=UNUSUAL_RULES_VERSION,
                value=volume_vs_tod.ratio,
                comparator=">=",
                threshold=floors.unusual_vol_tod_ratio,
                baseline=volume_vs_tod.median_cumulative,
                sample_count=volume_vs_tod.sessions_used,
            )
        )
    if volume_to_oi_result.is_spike and volume_to_oi_result.ratio is not None:
        out.append(
            UnusualTrigger(
                rule_id=RULE_DAY_VOL_VS_PREV_OI,
                rule_version=UNUSUAL_RULES_VERSION,
                value=volume_to_oi_result.ratio,
                comparator=">",
                threshold=volume_to_oi_result.threshold,
                baseline=volume_to_oi_result.prev_day_oi,
                sample_count=1,
            )
        )
    return out


def classify_reason(text: str) -> "tuple[str, float | None]":
    """Read one stored reason sentence back into (rule id, stated value).

    Rows written before this registry existed carry only the sentence.  This is
    the boundary that turns them back into the rule that wrote them -- no stored
    row is rewritten, and a sentence no rule claims stays itself under
    ``RULE_UNCLASSIFIED`` rather than being folded into one that did not fire.
    """
    clean = str(text or "").strip()
    for rule_id, pattern in _REASON_PATTERN.items():
        found = pattern.match(clean)
        if found:
            try:
                return rule_id, float(found.group(1))
            except ValueError:  # shaped right, with a number that is not one
                return rule_id, None
    return RULE_UNCLASSIFIED, None


def triggers_from_metric_row(row: Mapping[str, Any]) -> list[UnusualTrigger]:
    """Structured triggers for one STORED ``metrics`` row.

    The store already holds every number a trigger needs -- ``vol_tod_ratio``,
    ``vol_tod_median``, ``vol_tod_sessions``, ``vol_oi_ratio``,
    ``vol_oi_prev_oi`` -- so nothing is recomputed and nothing is invented: WHICH
    rules fired is the row's own ``unusual_reasons``, and the numbers beside them
    are the row's own columns.  A reason whose column is gone keeps the value its
    own sentence states, and says nothing more.
    """
    out: list[UnusualTrigger] = []
    for piece in str(row.get("unusual_reasons") or "").split(","):
        piece = piece.strip()
        if not piece:
            continue
        rule_id, stated = classify_reason(piece)
        rule = UNUSUAL_RULES_BY_ID.get(rule_id)
        if rule_id == RULE_VOL_TOD:
            value = _num(row.get("vol_tod_ratio"))
            baseline = _num(row.get("vol_tod_median"))
            sessions = row.get("vol_tod_sessions")
            sample = int(sessions) if sessions is not None else None
        elif rule_id == RULE_DAY_VOL_VS_PREV_OI:
            value = _num(row.get("vol_oi_ratio"))
            baseline = _num(row.get("vol_oi_prev_oi"))
            sample = 1
        else:
            value, baseline, sample = None, None, None
        out.append(
            UnusualTrigger(
                rule_id=rule_id,
                rule_version=UNUSUAL_RULES_VERSION,
                value=value if value is not None else stated,
                comparator=rule.comparator if rule else "",
                threshold=rule.threshold if rule else 0.0,
                baseline=baseline,
                sample_count=sample,
                text=piece,
            )
        )
    return out


# ===========================================================================
# Per-contract assembly and the per-underlying roll-up
# ===========================================================================


@dataclass
class ContractInputs:
    """Everything one contract's metrics need, with no database attached."""

    contract: ContractRef
    current: Snapshot
    previous: Snapshot | None = None
    previous_close: DailyBar | None = None
    tod_baseline: Sequence[float] = ()
    spot: float | None = None


@dataclass
class ContractMetrics:
    contract: ContractRef
    captured_at: datetime
    days_to_expiry: int | None
    last_price: float | None
    average_price: float | None
    volume: float | None
    oi: float | None
    buildup_15m: BuildUp
    buildup_day: BuildUp
    volume_vs_tod: VolumeVsTimeOfDay
    volume_to_oi: VolumeToOi
    premium: PremiumTraded
    floors_passed: bool
    floors_failed: list[str]
    unusual: bool
    unusual_reasons: list[str]
    spot: float | None = None
    #: The SAME flags as `unusual_reasons`, carried as rule id + version +
    #: value + comparator + threshold + baseline + sample count instead of as
    #: prose.  Additive: `to_row()` below is unchanged, so no stored row moves.
    triggers: tuple["UnusualTrigger", ...] = ()

    def to_row(self) -> dict[str, Any]:
        """The ``metrics``-table row for this contract (scope='contract')."""
        c = self.contract
        return {
            "scope": "contract",
            "metric_key": c.tradingsymbol,
            "instrument_token": c.instrument_token,
            "tradingsymbol": c.tradingsymbol,
            "underlying": c.underlying,
            "instrument_type": c.instrument_type,
            "strike": c.strike,
            "expiry": c.expiry.isoformat() if isinstance(c.expiry, (date, datetime)) else c.expiry,
            "lot_size": c.lot_size,
            "captured_at": self.captured_at.isoformat(sep=" ", timespec="seconds"),
            "days_to_expiry": self.days_to_expiry,
            "last_price": self.last_price,
            "average_price": self.average_price,
            "volume": self.volume,
            "oi": self.oi,
            "spot": self.spot,
            # 3.1 -- the two windows stay apart
            "price_change_15m": self.buildup_15m.price_change,
            "price_change_pct_15m": self.buildup_15m.price_change_pct,
            "oi_change_15m": self.buildup_15m.oi_change,
            "oi_change_pct_15m": self.buildup_15m.oi_change_pct,
            "buildup_15m": self.buildup_15m.label,
            "price_change_day": self.buildup_day.price_change,
            "price_change_pct_day": self.buildup_day.price_change_pct,
            "oi_change_day": self.buildup_day.oi_change,
            "oi_change_pct_day": self.buildup_day.oi_change_pct,
            "buildup_day": self.buildup_day.label,
            # 3.2
            "vol_tod_ratio": self.volume_vs_tod.ratio,
            "vol_tod_median": self.volume_vs_tod.median_cumulative,
            "vol_tod_sessions": self.volume_vs_tod.sessions_used,
            "vol_tod_status": self.volume_vs_tod.status,
            # 3.3
            "vol_oi_ratio": self.volume_to_oi.ratio,
            "vol_oi_prev_oi": self.volume_to_oi.prev_day_oi,
            "vol_oi_spike": int(self.volume_to_oi.is_spike),
            "vol_oi_status": self.volume_to_oi.status,
            # 3.4
            "premium_rs": self.premium.rupees,
            "premium_cr": self.premium.crore,
            "premium_status": self.premium.status,
            # screen
            "floors_passed": int(self.floors_passed),
            "floors_failed": ",".join(self.floors_failed),
            "unusual": int(self.unusual),
            "unusual_reasons": ",".join(self.unusual_reasons),
        }


def compute_contract_metrics(
    inputs: ContractInputs,
    *,
    floors: LiquidityFloors = DEFAULT_FLOORS,
) -> ContractMetrics:
    """Signals 3.1-3.4 for one contract at one 15-minute mark, plus the floors."""
    cur = inputs.current
    c = inputs.contract
    prem = premium_traded(cur.volume, cur.average_price)
    vtod = volume_vs_time_of_day(cur.volume, inputs.tod_baseline)
    prev_oi = inputs.previous_close.oi if inputs.previous_close else None
    voi = volume_to_oi(cur.volume, prev_oi, threshold=floors.vol_oi_spike_ratio)

    passed, failed = floors.check(
        premium_rs=prem.rupees,
        oi=cur.oi,
        lot_size=c.lot_size,
        last_price=cur.last_price,
    )

    # ONE source of truth for what fired.  The prose is DERIVED from the
    # structured trigger rather than written beside it, so the sentence a row
    # stores and the rule a reader is shown can never drift apart -- and the
    # text itself is byte-for-byte what it has always been.
    triggers = unusual_triggers(vtod, voi, floors=floors)
    reasons: list[str] = [t.reason for t in triggers]

    return ContractMetrics(
        contract=c,
        captured_at=cur.captured_at,
        days_to_expiry=days_to_expiry(c.expiry, cur.captured_at),
        last_price=_num(cur.last_price),
        average_price=_num(cur.average_price),
        volume=_num(cur.volume),
        oi=_num(cur.oi),
        buildup_15m=buildup_15m(cur, inputs.previous),
        buildup_day=buildup_day(cur, inputs.previous_close),
        volume_vs_tod=vtod,
        volume_to_oi=voi,
        premium=prem,
        floors_passed=passed,
        floors_failed=failed,
        unusual=bool(passed and reasons),
        unusual_reasons=reasons,
        spot=_num(inputs.spot),
        triggers=tuple(triggers),
    )


@dataclass
class UnderlyingRollup:
    """The default list row: one underlying, expanding to its strikes."""

    underlying: str
    captured_at: datetime
    expiry: date | None
    days_to_expiry: int | None
    spot: float | None
    pcr: PutCallRatio
    max_pain: MaxPain
    premium: PremiumTraded
    contracts: int
    unusual_ce: int
    unusual_pe: int
    oi_change_pct_day: float | None
    oi_change_day_status: str
    headline: str
    # E04 matched cohort: `oi_change_pct_day` compares the SAME legs now and at
    # the prior close.  Legs that exist now with no prior close (births) and
    # legs with a prior close but no current OI (deaths) are reported apart,
    # never folded into the change as growth or decline.
    oi_matched_now: float | None = None
    oi_matched_before: float | None = None
    oi_matched_legs: int = 0
    oi_births: int = 0
    oi_births_oi: float = 0.0
    oi_deaths: int = 0
    oi_deaths_prior_oi: float = 0.0

    def to_row(self) -> dict[str, Any]:
        return {
            "scope": "underlying",
            "metric_key": f"{self.underlying}|{self.expiry.isoformat() if self.expiry else 'all'}",
            "instrument_token": None,
            "tradingsymbol": None,
            "underlying": self.underlying,
            "instrument_type": None,
            "strike": None,
            "expiry": self.expiry.isoformat() if self.expiry else None,
            "lot_size": None,
            "captured_at": self.captured_at.isoformat(sep=" ", timespec="seconds"),
            "days_to_expiry": self.days_to_expiry,
            "spot": self.spot,
            "pcr_oi": self.pcr.pcr_oi,
            "pcr_volume": self.pcr.pcr_volume,
            "pcr_trend": self.pcr.trend,
            "pcr_trend_change": self.pcr.trend_change,
            "total_ce_oi": self.pcr.ce_oi,
            "total_pe_oi": self.pcr.pe_oi,
            "total_ce_volume": self.pcr.ce_volume,
            "total_pe_volume": self.pcr.pe_volume,
            "max_pain_strike": self.max_pain.strike,
            "max_pain_distance": self.max_pain.distance_from_spot,
            "max_pain_total_oi": self.max_pain.total_oi,
            "max_pain_status": self.max_pain.status,
            "premium_rs": self.premium.rupees,
            "premium_cr": self.premium.crore,
            "premium_status": self.premium.status,
            "oi_change_pct_day": self.oi_change_pct_day,
            "oi_change_day_status": self.oi_change_day_status,
            "contracts": self.contracts,
            "unusual_ce_strikes": self.unusual_ce,
            "unusual_pe_strikes": self.unusual_pe,
            "unusual": int(bool(self.unusual_ce or self.unusual_pe)),
            "headline": self.headline,
        }


def _headline(
    underlying: str,
    unusual_ce: int,
    unusual_pe: int,
    oi_change_pct: float | None,
    oi_status: str,
    premium: PremiumTraded,
) -> str:
    """"RELIANCE - 3 call strikes unusually active, OI +12%, Rs 48 cr traded"."""
    bits: list[str] = []
    if unusual_ce:
        bits.append(f"{unusual_ce} call strike{'s' if unusual_ce != 1 else ''} unusually active")
    if unusual_pe:
        bits.append(f"{unusual_pe} put strike{'s' if unusual_pe != 1 else ''} unusually active")
    if not bits:
        bits.append("nothing above the floors")
    if oi_status == STATUS_OK and oi_change_pct is not None:
        bits.append(f"OI {oi_change_pct:+.0f}%")
    else:
        bits.append(f"OI {oi_status}")
    if premium.status == STATUS_OK and premium.crore is not None:
        bits.append(f"Rs {premium.crore:.0f} cr traded")
    else:
        bits.append("premium no data")
    return f"{underlying} - " + ", ".join(bits)


def roll_up_underlying(
    underlying: str,
    metrics: Sequence[ContractMetrics],
    *,
    captured_at: datetime | None = None,
    expiry: date | None = None,
    spot: float | None = None,
    pcr_series: Sequence[float | None] = (),
) -> UnderlyingRollup:
    """The per-underlying roll-up of spec 3 ("Roll-up"), over option legs.

    PCR (3.5) and max pain (3.6) are computed from the same ``metrics`` list, so
    the card and the strikes beneath it can never disagree.
    """
    options = [m for m in metrics if m.contract.is_option]
    captured_at = captured_at or (metrics[0].captured_at if metrics else datetime.now())
    spot = spot if spot is not None else next((m.spot for m in metrics if m.spot is not None), None)

    legs = [
        {
            "instrument_type": m.contract.instrument_type,
            "strike": m.contract.strike,
            "oi": m.oi,
            "volume": m.volume,
        }
        for m in options
    ]
    pcr = put_call_ratio(legs)
    trend_label, trend_change = pcr_trend(list(pcr_series) + [pcr.pcr_oi])
    pcr = PutCallRatio(**{**asdict(pcr), "trend": trend_label, "trend_change": trend_change})
    mp = max_pain(legs, spot=spot)
    prem = premium_rollup([m.premium for m in metrics])

    # Matched cohort (E04): only legs with BOTH a current OI and a prior-close
    # OI enter the change.  Summing every current leg against only the legs
    # that had a prior would make a newly listed strike look like OI growth.
    matched = [m for m in options if m.oi is not None and m.buildup_day.oi_change is not None]
    births = [m for m in options if m.oi is not None and m.buildup_day.oi_change is None]
    deaths = [
        m for m in options
        if m.oi is None and m.volume_to_oi.prev_day_oi is not None
    ]
    oi_now = sum(m.oi for m in matched) if matched else None
    oi_before = sum(m.oi - m.buildup_day.oi_change for m in matched) if matched else None
    if oi_before is not None and oi_before > 0:
        oi_change_pct = 100.0 * (oi_now - oi_before) / oi_before
        oi_status = STATUS_OK
    else:
        oi_change_pct, oi_status = None, STATUS_NO_PRIOR

    unusual_ce = sum(1 for m in options if m.unusual and m.contract.instrument_type == "CE")
    unusual_pe = sum(1 for m in options if m.unusual and m.contract.instrument_type == "PE")

    return UnderlyingRollup(
        underlying=underlying,
        captured_at=captured_at,
        expiry=expiry,
        days_to_expiry=days_to_expiry(expiry, captured_at) if expiry else None,
        spot=spot,
        pcr=pcr,
        max_pain=mp,
        premium=prem,
        contracts=len(metrics),
        unusual_ce=unusual_ce,
        unusual_pe=unusual_pe,
        oi_change_pct_day=oi_change_pct,
        oi_change_day_status=oi_status,
        headline=_headline(underlying, unusual_ce, unusual_pe, oi_change_pct, oi_status, prem),
        oi_matched_now=oi_now,
        oi_matched_before=oi_before,
        oi_matched_legs=len(matched),
        oi_births=len(births),
        oi_births_oi=sum(m.oi for m in births),
        oi_deaths=len(deaths),
        oi_deaths_prior_oi=sum(m.volume_to_oi.prev_day_oi for m in deaths),
    )


# ===========================================================================
# The thin database layer
# ===========================================================================

#: The columns this module writes.  D1 owns ``schema.sql``; if its ``metrics``
#: table has a different (or smaller) set of columns, ``write_metric_rows``
#: writes only the intersection and logs the dropped keys.  This DDL is used
#: **only** when the table does not exist at all, so a D2-only run can be
#: tested end to end before D1's store lands.
PROVISIONAL_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS metrics (
    scope                 TEXT    NOT NULL,   -- 'contract' | 'underlying'
    metric_key            TEXT    NOT NULL,   -- tradingsymbol, or 'UNDERLYING|expiry'
    captured_at           TEXT    NOT NULL,
    instrument_token      INTEGER,
    tradingsymbol         TEXT,
    underlying            TEXT    NOT NULL,
    instrument_type       TEXT,
    strike                REAL,
    expiry                TEXT,
    lot_size              INTEGER,
    days_to_expiry        INTEGER,
    last_price            REAL,
    average_price         REAL,
    volume                REAL,
    oi                    REAL,
    spot                  REAL,
    spot_source           TEXT,
    price_change_pct_15m  REAL,
    oi_change_15m         REAL,
    oi_change_pct_15m     REAL,
    buildup_15m           TEXT,
    price_change_pct_day  REAL,
    oi_change_day         REAL,
    oi_change_pct_day     REAL,
    buildup_day           TEXT,
    vol_tod_ratio         REAL,
    vol_tod_sessions      INTEGER,
    vol_tod_status        TEXT,
    vol_oi_ratio          REAL,
    vol_oi_prev_oi        REAL,
    vol_oi_status         TEXT,
    premium_rs            REAL,
    premium_cr            REAL,
    pcr_oi                REAL,
    pcr_volume            REAL,
    pcr_trend             TEXT,
    pcr_trend_change      REAL,
    total_ce_oi           REAL,
    total_pe_oi           REAL,
    max_pain_strike       REAL,
    max_pain_distance     REAL,
    max_pain_total_oi     REAL,
    max_pain_status       TEXT,
    fut_oi_avg            REAL,
    fut_oi_vs_avg         REAL,
    fut_oi_vs_avg_status  TEXT,
    basis                 REAL,
    basis_pct             REAL,
    basis_status          TEXT,
    contracts             INTEGER,
    floors_passed         INTEGER,
    unusual               INTEGER NOT NULL DEFAULT 0,
    unusual_reasons       TEXT,
    headline              TEXT,
    vendor_id             TEXT,
    fetched_at            TEXT,
    snapshot_id           TEXT,
    PRIMARY KEY (scope, metric_key, captured_at)
);
CREATE INDEX IF NOT EXISTS idx_metrics_und_at   ON metrics (underlying, captured_at);
CREATE INDEX IF NOT EXISTS idx_metrics_unusual  ON metrics (captured_at, unusual);
CREATE INDEX IF NOT EXISTS idx_metrics_expiry   ON metrics (underlying, expiry, captured_at);
"""


def connect(db_path: str | Path = DEFAULT_DB_PATH, *, readonly: bool = False) -> sqlite3.Connection:
    """Open ``db/derivatives.db``.  ``db/kanida.db`` and ``db/market15.db`` are
    never opened by this module at all."""
    path = Path(db_path)
    if readonly:
        if not path.exists():
            raise LookupError(f"{path} does not exist yet (D1's capture has not run)")
        conn = sqlite3.connect(
            f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
        )
        conn.execute("PRAGMA query_only=ON")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), timeout=60.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
    conn.row_factory = sqlite3.Row
    return conn


# SQLite binds a limited number of "?" variables per statement (the full F&O scope is ~27,000
# contracts, well past it). Anything longer than this goes through a TEMP table instead.
MAX_INLINE_TOKENS = 800

# The capture worker writes to the same file every 15 minutes; a cycle takes well under a minute.
WRITE_LOCK_RETRIES = 6
WRITE_LOCK_WAIT_SECONDS = 15.0

#: Retired from `schema.sql` on 2026-09-19 after every reader was traced.  They
#: are still COMPUTED — the analysis is unchanged and the CLI still prints them
#: — they are simply no longer stored, because nothing ever read them back.
#:
#:   price_change_15m / price_change_day
#:       the Derivative tab refuses rupee moves and says so in a comment at both
#:       of the places it builds its field list.
#:   premium_status / floors_failed / headline
#:       pre-rendered text.  Written, never read by anything.
#:   vol_tod_median / vol_oi_spike / oi_change_day_status / oi_change_pct_day_agg
#:   / unusual_ce_strikes / unusual_pe_strikes / pcr_trend / pcr_trend_change
#:   / total_ce_volume / total_pe_volume
#:       reach a `SELECT *` envelope and are never subscripted out of it.
#:
#: Naming them here is what keeps `write_metric_rows`' drift warning meaningful:
#: a column that vanishes from the live table WITHOUT being on this list is real
#: drift and must still shout.
RETIRED_METRIC_COLUMNS = frozenset({
    "price_change_15m", "price_change_day",
    "premium_status", "floors_failed",
    "vol_tod_median", "vol_oi_spike",
    "oi_change_day_status", "oi_change_pct_day_agg",
    "unusual_ce_strikes", "unusual_pe_strikes",
    "total_ce_volume", "total_pe_volume",
})
_TOKEN_TABLE = "_metrics_token_filter"


def _token_clause(
    conn: sqlite3.Connection, tokens: "Sequence[int] | None", column: str = "instrument_token"
) -> tuple[str, list[Any]]:
    """An ``AND <column> IN (...)`` fragment restricting a query to ``tokens``.

    A short list is bound inline. A long one is written to a TEMP table and joined against, because
    SQLite refuses a statement with more variables than its compiled-in limit. TEMP tables live in
    the connection's own temp database, never in ``db/derivatives.db``, so a read-only connection
    can still use this.
    """
    if not tokens:
        return "", []
    if len(tokens) <= MAX_INLINE_TOKENS:
        return f" AND {column} IN ({','.join('?' * len(tokens))})", list(tokens)
    conn.execute(f"CREATE TEMP TABLE IF NOT EXISTS {_TOKEN_TABLE} (token INTEGER PRIMARY KEY)")
    conn.execute(f"DELETE FROM {_TOKEN_TABLE}")
    conn.executemany(f"INSERT OR IGNORE INTO {_TOKEN_TABLE} (token) VALUES (?)",
                     [(int(x),) for x in tokens])
    return f" AND {column} IN (SELECT token FROM {_TOKEN_TABLE})", []


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=?", (name,)
    ).fetchone()
    return row is not None


def ensure_metrics_table(conn: sqlite3.Connection) -> None:
    """Create ``metrics`` only if D1's schema has not already created it."""
    if not table_exists(conn, "metrics"):
        conn.executescript(PROVISIONAL_METRICS_DDL)
        conn.commit()


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def write_metric_rows(
    conn: sqlite3.Connection,
    rows: Sequence[Mapping[str, Any]],
    *,
    vendor_id: str | None = None,
    fetched_at: str | None = None,
    snapshot_id: str | None = None,
) -> int:
    """Upsert metric rows, writing only the columns the live table actually has.

    D1 owns the schema; this keeps the two halves independent.  Keys that the
    table does not carry are dropped with one log line, never silently.
    """
    if not rows:
        return 0
    # The loads above left a read transaction open.
    # (retired columns are filtered further down, once the live shape is known) In WAL a read snapshot cannot be upgraded to a
    # write while the capture worker holds the writer -- SQLite answers BUSY at once and
    # busy_timeout never gets a chance. Every row is already in memory, so end the read first and
    # take the write lock cleanly, waiting out one capture cycle if it is mid-write.
    conn.rollback()
    for attempt in range(WRITE_LOCK_RETRIES):
        try:
            conn.execute("BEGIN IMMEDIATE")
            break
        except sqlite3.OperationalError as error:
            if "locked" not in str(error) and "busy" not in str(error).lower():
                raise
            if attempt == WRITE_LOCK_RETRIES - 1:
                raise
            LOG.info("metrics: waiting for the capture writer (%s)", error)
            time.sleep(WRITE_LOCK_WAIT_SECONDS)
    ensure_metrics_table(conn)
    have = set(_columns(conn, "metrics"))
    prov = {"vendor_id": vendor_id, "fetched_at": fetched_at, "snapshot_id": snapshot_id}
    written = 0
    dropped: set[str] = set()
    for row in rows:
        payload = {**dict(row), **{k: v for k, v in prov.items() if v is not None}}
        # A retired column is expected to be missing: it is not schema drift and
        # must not be reported as a lost signal every fifteen minutes.
        dropped |= set(payload) - have - RETIRED_METRIC_COLUMNS
        payload = {k: v for k, v in payload.items() if k in have}
        if not payload:
            continue
        cols = ", ".join(payload)
        marks = ", ".join("?" * len(payload))
        conn.execute(
            f"INSERT OR REPLACE INTO metrics ({cols}) VALUES ({marks})", list(payload.values())
        )
        written += 1
    conn.commit()
    if dropped:
        LOG.warning(
            "metrics table has no columns %s -- those signals were not stored "
            "(D1 owns schema.sql)", sorted(dropped)
        )
    return written


# --- loaders -------------------------------------------------------------


def _as_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def load_contracts(
    conn: sqlite3.Connection,
    *,
    underlyings: Sequence[str] | None = None,
    expiry: date | None = None,
) -> dict[int, ContractRef]:
    """Read D1's ``contracts`` table."""
    if not table_exists(conn, "contracts"):
        raise LookupError("db/derivatives.db has no 'contracts' table yet (D1's store)")
    sql = ["SELECT instrument_token, tradingsymbol, underlying, instrument_type, strike,",
           "expiry, lot_size FROM contracts WHERE 1=1"]
    args: list[Any] = []
    if underlyings:
        sql.append(f"AND underlying IN ({','.join('?' * len(underlyings))})")
        args += list(underlyings)
    if expiry:
        sql.append("AND date(expiry) = ?")
        args.append(expiry.isoformat())
    out: dict[int, ContractRef] = {}
    for r in conn.execute(" ".join(sql), args):
        exp = _as_dt(r["expiry"])
        out[int(r["instrument_token"])] = ContractRef(
            instrument_token=int(r["instrument_token"]),
            tradingsymbol=r["tradingsymbol"],
            underlying=r["underlying"],
            instrument_type=(r["instrument_type"] or "").upper(),
            expiry=exp.date() if exp else None,
            strike=_num(r["strike"]),
            lot_size=int(r["lot_size"]) if r["lot_size"] else None,
        )
    return out


def load_snapshots_at(
    conn: sqlite3.Connection, captured_at: datetime, tokens: Sequence[int] | None = None
) -> dict[int, Snapshot]:
    """Read D1's ``snapshots`` rows for one 15-minute mark."""
    if not table_exists(conn, "snapshots"):
        raise LookupError("db/derivatives.db has no 'snapshots' table yet (D1's store)")
    stamp = captured_at.isoformat(sep=" ", timespec="seconds")
    sql = "SELECT * FROM snapshots WHERE (captured_at = ? OR captured_at = ?)"
    args: list[Any] = [stamp, captured_at.isoformat(timespec="seconds")]
    clause, clause_args = _token_clause(conn, tokens)
    sql += clause
    args += clause_args
    out: dict[int, Snapshot] = {}
    for r in conn.execute(sql, args):
        keys = r.keys()
        out[int(r["instrument_token"])] = Snapshot(
            instrument_token=int(r["instrument_token"]),
            captured_at=_as_dt(r["captured_at"]) or captured_at,
            last_price=_num(r["last_price"]) if "last_price" in keys else None,
            average_price=_num(r["average_price"]) if "average_price" in keys else None,
            volume=_num(r["volume"]) if "volume" in keys else None,
            oi=_num(r["oi"]) if "oi" in keys else None,
            buy_quantity=_num(r["buy_quantity"]) if "buy_quantity" in keys else None,
            sell_quantity=_num(r["sell_quantity"]) if "sell_quantity" in keys else None,
            bid=_num(r["bid"]) if "bid" in keys else None,
            ask=_num(r["ask"]) if "ask" in keys else None,
        )
    return out


def load_previous_closes(
    conn: sqlite3.Connection, session: date, tokens: Sequence[int] | None = None
) -> dict[int, DailyBar]:
    """The last ``candles_15m`` bar of the last session strictly before ``session``.

    Point-in-time: nothing from ``session`` itself can be a "previous" close.
    """
    if not table_exists(conn, "candles_15m"):
        raise LookupError("db/derivatives.db has no 'candles_15m' table yet (D1's store)")
    args: list[Any] = [session.isoformat()]
    token_clause, clause_args = _token_clause(conn, tokens)
    args += clause_args
    sql = f"""
        WITH prior AS (
            SELECT instrument_token, bar_start, close, oi, volume,
                   date(bar_start) AS d
            FROM candles_15m
            WHERE date(bar_start) < ?{token_clause}
        ), last_day AS (
            SELECT instrument_token, MAX(d) AS d FROM prior GROUP BY instrument_token
        ), last_bar AS (
            SELECT p.instrument_token, MAX(p.bar_start) AS bar_start
            FROM prior p JOIN last_day l
              ON l.instrument_token = p.instrument_token AND l.d = p.d
            GROUP BY p.instrument_token
        )
        SELECT p.instrument_token, p.d, p.close, p.oi,
               (SELECT SUM(volume) FROM prior q
                 WHERE q.instrument_token = p.instrument_token AND q.d = p.d) AS day_volume
        FROM prior p JOIN last_bar b
          ON b.instrument_token = p.instrument_token AND b.bar_start = p.bar_start
    """
    out: dict[int, DailyBar] = {}
    for r in conn.execute(sql, args):
        out[int(r["instrument_token"])] = DailyBar(
            session=date.fromisoformat(r["d"]),
            close=_num(r["close"]),
            oi=_num(r["oi"]),
            volume=_num(r["day_volume"]),
        )
    return out


def load_tod_baselines(
    conn: sqlite3.Connection,
    captured_at: datetime,
    tokens: Sequence[int] | None = None,
    *,
    sessions: int = BASELINE_SESSIONS,
) -> dict[int, list[float]]:
    """Per contract, the cumulative volume by this clock time in prior sessions.

    A 15-minute bar counts when it has ENDED by the mark (bar end <= mark), the
    same rule as :func:`cumulative_by_time_of_day` (E04).
    """
    if not table_exists(conn, "candles_15m"):
        raise LookupError("db/derivatives.db has no 'candles_15m' table yet (D1's store)")
    args: list[Any] = [captured_at.date().isoformat(), captured_at.strftime("%H:%M:%S")]
    token_clause, clause_args = _token_clause(conn, tokens)
    args += clause_args
    sql = f"""
        SELECT instrument_token, date(bar_start) AS d, SUM(volume) AS cum
        FROM candles_15m
        WHERE date(bar_start) < ? AND time(bar_start, '+{DEFAULT_BAR_MINUTES} minutes') <= ?{token_clause}
        GROUP BY instrument_token, d
        ORDER BY instrument_token, d
    """
    out: dict[int, list[float]] = {}
    for r in conn.execute(sql, args):
        out.setdefault(int(r["instrument_token"]), []).append(_num(r["cum"]) or 0.0)
    return {k: v[-sessions:] for k, v in out.items()}


def load_daily_oi_history(
    conn: sqlite3.Connection,
    session: date,
    tokens: Sequence[int] | None = None,
    *,
    sessions: int = FUT_OI_AVG_SESSIONS,
) -> dict[int, list[float]]:
    """Closing OI per prior session, oldest first (for spec 3.7's 20-day average)."""
    if not table_exists(conn, "candles_15m"):
        raise LookupError("db/derivatives.db has no 'candles_15m' table yet (D1's store)")
    args: list[Any] = [session.isoformat()]
    token_clause, clause_args = _token_clause(conn, tokens)
    args += clause_args
    sql = f"""
        WITH prior AS (
            SELECT instrument_token, bar_start, oi, date(bar_start) AS d
            FROM candles_15m WHERE date(bar_start) < ?{token_clause}
        ), last_bar AS (
            SELECT instrument_token, d, MAX(bar_start) AS bar_start
            FROM prior GROUP BY instrument_token, d
        )
        SELECT p.instrument_token, p.d, p.oi
        FROM prior p JOIN last_bar b
          ON b.instrument_token = p.instrument_token AND b.bar_start = p.bar_start
        ORDER BY p.instrument_token, p.d
    """
    out: dict[int, list[float]] = {}
    for r in conn.execute(sql, args):
        v = _num(r["oi"])
        if v is not None:
            out.setdefault(int(r["instrument_token"]), []).append(v)
    return {k: v[-sessions:] for k, v in out.items()}


def load_spots(conn: sqlite3.Connection, captured_at: datetime) -> dict[str, float]:
    """Spot per underlying at this mark, from D1's ``underlying_snapshots``.

    Absent table or absent row => the underlying simply has no spot, and every
    number that needs one (basis, max-pain distance) carries ``"no spot"``.
    """
    return {k: v for k, (v, _src) in load_spot_rows(conn, captured_at).items()}


def load_spot_sources(conn: sqlite3.Connection, captured_at: datetime) -> dict[str, str]:
    """Where each of those spots came from — ``underlying_snapshots.spot_source``.

    Carried onto every ``metrics`` row so the tab can tell a spot captured at the
    mark from one rebuilt afterwards without joining back to the roll-up table.
    A spot with no recorded source yields no entry: "not recorded" is not a claim
    that it was captured.
    """
    return {k: src for k, (_v, src) in load_spot_rows(conn, captured_at).items()
            if src}


def load_spot_rows(
    conn: sqlite3.Connection, captured_at: datetime
) -> dict[str, tuple[float, str | None]]:
    """``underlying -> (spot, spot_source)`` at this mark.  One read for both."""
    if not table_exists(conn, "underlying_snapshots"):
        return {}
    has_source = "spot_source" in set(_columns(conn, "underlying_snapshots"))
    stamp = captured_at.isoformat(sep=" ", timespec="seconds")
    column = "spot_source" if has_source else "NULL AS spot_source"
    out: dict[str, tuple[float, str | None]] = {}
    for r in conn.execute(
        f"SELECT underlying, spot, {column} FROM underlying_snapshots"
        " WHERE captured_at IN (?, ?)",
        (stamp, captured_at.isoformat(timespec="seconds")),
    ):
        v = _num(r["spot"])
        if v is not None:
            out[r["underlying"]] = (v, r["spot_source"])
    return out


def load_pcr_series(
    conn: sqlite3.Connection, underlying: str, session: date, *, expiry: date | None = None
) -> list[float]:
    """Today's earlier OI-PCR readings for this underlying, for the day's trend."""
    if not table_exists(conn, "metrics"):
        return []
    cols = set(_columns(conn, "metrics"))
    if not {"pcr_oi", "captured_at", "underlying"} <= cols:
        return []
    sql = ("SELECT pcr_oi FROM metrics WHERE underlying=? AND date(captured_at)=?"
           " AND pcr_oi IS NOT NULL")
    args: list[Any] = [underlying, session.isoformat()]
    if expiry and "expiry" in cols:
        sql += " AND expiry = ?"
        args.append(expiry.isoformat())
    sql += " ORDER BY captured_at"
    return [float(r[0]) for r in conn.execute(sql, args)]


# --- orchestration -------------------------------------------------------


@dataclass
class ComputeResult:
    captured_at: datetime
    contract_rows: list[dict[str, Any]]
    underlying_rows: list[dict[str, Any]]
    futures: list[FuturesBuildUp]
    written: int = 0
    skipped_no_snapshot: int = 0


def compute_for_mark(
    conn: sqlite3.Connection,
    captured_at: datetime,
    *,
    underlyings: Sequence[str] | None = None,
    floors: LiquidityFloors = DEFAULT_FLOORS,
    write: bool = True,
) -> ComputeResult:
    """Compute every signal of section 3 for one 15-minute mark and store it.

    Reads D1's ``contracts`` / ``snapshots`` / ``candles_15m`` /
    ``underlying_snapshots``; writes the ``metrics`` table.  Contracts without a
    snapshot at this mark are counted, not invented.
    """
    contracts = load_contracts(conn, underlyings=underlyings)
    tokens = list(contracts)
    snaps = load_snapshots_at(conn, captured_at, tokens)
    prev_mark = captured_at - timedelta(minutes=15)
    prev_snaps = load_snapshots_at(conn, prev_mark, tokens)
    prev_closes = load_previous_closes(conn, captured_at.date(), tokens)
    baselines = load_tod_baselines(conn, captured_at, tokens)
    spot_rows = load_spot_rows(conn, captured_at)
    spots = {k: v for k, (v, _s) in spot_rows.items()}
    spot_sources = {k: s for k, (_v, s) in spot_rows.items() if s}

    by_underlying: dict[str, list[ContractMetrics]] = {}
    fut_tokens: list[int] = []
    contract_rows: list[dict[str, Any]] = []
    skipped = 0

    for token, c in contracts.items():
        snap = snaps.get(token)
        if snap is None:
            skipped += 1
            continue
        if c.instrument_type == "FUT":
            fut_tokens.append(token)
            continue
        m = compute_contract_metrics(
            ContractInputs(
                contract=c,
                current=snap,
                previous=prev_snaps.get(token),
                previous_close=prev_closes.get(token),
                tod_baseline=baselines.get(token, []),
                spot=spots.get(c.underlying),
            ),
            floors=floors,
        )
        by_underlying.setdefault(c.underlying, []).append(m)
        row = m.to_row()
        # The spot's provenance travels with the spot.  A row whose underlying
        # has no recorded source carries None, which reads as "not recorded" and
        # never as "captured at this mark".
        row["spot_source"] = spot_sources.get(c.underlying)
        contract_rows.append(row)

    # futures (3.7)
    oi_hist = load_daily_oi_history(conn, captured_at.date(), fut_tokens) if fut_tokens else {}
    futures: list[FuturesBuildUp] = []
    for token in fut_tokens:
        c = contracts[token]
        fb = compute_futures_buildup(
            c,
            snaps[token],
            previous=prev_snaps.get(token),
            previous_close=prev_closes.get(token),
            oi_history=oi_hist.get(token, []),
            spot=spots.get(c.underlying),
            as_of=captured_at,
        )
        futures.append(fb)
        fut = futures_row(fb, snaps[token], c, captured_at, floors)
        fut["spot_source"] = spot_sources.get(c.underlying)
        contract_rows.append(fut)

    underlying_rows: list[dict[str, Any]] = []
    for und, metrics in by_underlying.items():
        expiries = {m.contract.expiry for m in metrics if m.contract.expiry}
        for exp in sorted(expiries):
            legs = [m for m in metrics if m.contract.expiry == exp]
            series = load_pcr_series(conn, und, captured_at.date(), expiry=exp)
            roll = roll_up_underlying(
                und, legs, captured_at=captured_at, expiry=exp,
                spot=spots.get(und), pcr_series=series,
            )
            roll_row = roll.to_row()
            roll_row["spot_source"] = spot_sources.get(und)
            underlying_rows.append(roll_row)

    written = 0
    if write:
        written = write_metric_rows(conn, contract_rows + underlying_rows)
    return ComputeResult(
        captured_at=captured_at,
        contract_rows=contract_rows,
        underlying_rows=underlying_rows,
        futures=futures,
        written=written,
        skipped_no_snapshot=skipped,
    )


def futures_row(
    fb: FuturesBuildUp,
    snap: Snapshot,
    contract: ContractRef,
    captured_at: datetime,
    floors: LiquidityFloors = DEFAULT_FLOORS,
) -> dict[str, Any]:
    """The ``metrics`` row for a futures contract (scope='contract', type FUT)."""
    prem = fb.premium or premium_traded(snap.volume, snap.average_price)
    passed, failed = floors.check(
        premium_rs=prem.rupees, oi=snap.oi, lot_size=contract.lot_size,
        last_price=snap.last_price,
    )
    return {
        "scope": "contract",
        "metric_key": contract.tradingsymbol,
        "instrument_token": contract.instrument_token,
        "tradingsymbol": contract.tradingsymbol,
        "underlying": contract.underlying,
        "instrument_type": "FUT",
        "strike": None,
        "expiry": fb.expiry.isoformat() if fb.expiry else None,
        "lot_size": contract.lot_size,
        "captured_at": captured_at.isoformat(sep=" ", timespec="seconds"),
        "days_to_expiry": fb.days_to_expiry,
        "last_price": fb.last_price,
        "average_price": _num(snap.average_price),
        "volume": _num(snap.volume),
        "oi": fb.oi,
        "spot": fb.spot,
        "price_change_15m": fb.buildup_15m.price_change,
        "oi_change_15m": fb.buildup_15m.oi_change,
        "oi_change_pct_15m": fb.buildup_15m.oi_change_pct,
        "buildup_15m": fb.buildup_15m.label,
        "price_change_day": fb.buildup_day.price_change,
        "oi_change_day": fb.buildup_day.oi_change,
        "oi_change_pct_day": fb.buildup_day.oi_change_pct,
        "buildup_day": fb.buildup_day.label,
        "premium_rs": prem.rupees,
        "premium_cr": prem.crore,
        "premium_status": prem.status,
        "fut_oi_avg": fb.oi_avg,
        "fut_oi_vs_avg": fb.oi_vs_avg,
        "fut_oi_vs_avg_status": fb.oi_vs_avg_status,
        "basis": fb.basis,
        "basis_pct": fb.basis_pct,
        "basis_status": fb.basis_status,
        "floors_passed": int(passed),
        "floors_failed": ",".join(failed),
        "unusual": 0,
        "unusual_reasons": "",
    }


# ===========================================================================
# Read functions -- the five shapes the API/UI layer (D3) calls
# ===========================================================================


def _latest_mark(conn: sqlite3.Connection, on: date | None = None) -> str | None:
    if not table_exists(conn, "metrics"):
        return None
    if on:
        row = conn.execute(
            "SELECT MAX(captured_at) FROM metrics WHERE date(captured_at)=?", (on.isoformat(),)
        ).fetchone()
    else:
        row = conn.execute("SELECT MAX(captured_at) FROM metrics").fetchone()
    return row[0] if row and row[0] else None


def _rows(conn: sqlite3.Connection, sql: str, args: Sequence[Any]) -> list[dict[str, Any]]:
    return [dict(r) for r in conn.execute(sql, list(args))]


def _envelope(as_of: str | None, floors: LiquidityFloors, **extra: Any) -> dict[str, Any]:
    """Every card states its own as-of time and the floors in force (spec 4)."""
    return {"as_of": as_of, "floors": floors.as_dict(), **extra}


def read_unusual_activity(
    conn: sqlite3.Connection,
    *,
    as_of: datetime | None = None,
    floors: LiquidityFloors = DEFAULT_FLOORS,
    underlyings: Sequence[str] | None = None,
    min_days_to_expiry: int | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """Card 1 -- the unusual-activity list, per underlying, expandable to strikes.

    Shape::

        {"as_of": str|None, "floors": {...}, "rows": [
            {"underlying": str, "headline": str, "premium_cr": float|None,
             "pcr_oi": float|None, "max_pain_strike": float|None,
             "unusual_ce_strikes": int, "unusual_pe_strikes": int,
             "expiry": str|None, "days_to_expiry": int|None,
             "strikes": [ <contract row>, ... ]}]}

    ``strikes`` carries only the legs that passed the floors *and* tripped a
    trigger, each with its own ``unusual_reasons``.
    """
    stamp = as_of.isoformat(sep=" ", timespec="seconds") if as_of else _latest_mark(conn)
    if stamp is None:
        return _envelope(None, floors, rows=[], status="no metrics yet")

    args: list[Any] = [stamp]
    where = ""
    if underlyings:
        where += f" AND underlying IN ({','.join('?' * len(underlyings))})"
        args += list(underlyings)
    if min_days_to_expiry is not None:
        where += " AND days_to_expiry >= ?"
        args.append(min_days_to_expiry)

    heads = _rows(
        conn,
        "SELECT * FROM metrics WHERE captured_at=? AND scope='underlying'"
        f"{where} ORDER BY COALESCE(premium_rs,0) DESC LIMIT ?",
        args + [limit],
    )
    out = []
    for h in heads:
        legs = _rows(
            conn,
            "SELECT * FROM metrics WHERE captured_at=? AND scope='contract'"
            " AND underlying=? AND unusual=1"
            + (" AND expiry=?" if h.get("expiry") else "")
            + " ORDER BY COALESCE(premium_rs,0) DESC",
            [stamp, h["underlying"]] + ([h["expiry"]] if h.get("expiry") else []),
        )
        h["strikes"] = legs
        out.append(h)
    return _envelope(stamp, floors, rows=out)


def read_option_chain(
    conn: sqlite3.Connection,
    underlying: str,
    expiry: date | str,
    *,
    as_of: datetime | None = None,
    floors: LiquidityFloors = DEFAULT_FLOORS,
) -> dict[str, Any]:
    """Card 2 -- the full option chain for one underlying + expiry.

    Shape::

        {"as_of", "floors", "underlying", "expiry", "spot", "days_to_expiry",
         "summary": <underlying row or None>,
         "rows": [{"strike": float, "ce": <row|None>, "pe": <row|None>}, ...]}

    Each leg row carries OI, OI change (both windows, separately), volume,
    premium and the build-up labels.
    """
    stamp = as_of.isoformat(sep=" ", timespec="seconds") if as_of else _latest_mark(conn)
    exp = expiry.isoformat() if isinstance(expiry, date) else str(expiry)
    if stamp is None:
        return _envelope(None, floors, underlying=underlying, expiry=exp, rows=[],
                         status="no metrics yet")
    legs = _rows(
        conn,
        "SELECT * FROM metrics WHERE captured_at=? AND scope='contract' AND underlying=?"
        " AND expiry=? AND instrument_type IN ('CE','PE') ORDER BY strike",
        [stamp, underlying, exp],
    )
    by_strike: dict[float, dict[str, Any]] = {}
    for leg in legs:
        k = _num(leg.get("strike"))
        if k is None:
            continue
        slot = by_strike.setdefault(k, {"strike": k, "ce": None, "pe": None})
        slot["ce" if leg["instrument_type"] == "CE" else "pe"] = leg
    summary = next(
        iter(_rows(
            conn,
            "SELECT * FROM metrics WHERE captured_at=? AND scope='underlying'"
            " AND underlying=? AND expiry=?",
            [stamp, underlying, exp],
        )),
        None,
    )
    return _envelope(
        stamp, floors,
        underlying=underlying,
        expiry=exp,
        spot=(summary or {}).get("spot"),
        days_to_expiry=(summary or {}).get("days_to_expiry"),
        summary=summary,
        rows=[by_strike[k] for k in sorted(by_strike)],
    )


def read_oi_by_strike(
    conn: sqlite3.Connection,
    underlying: str,
    expiry: date | str,
    *,
    as_of: datetime | None = None,
    floors: LiquidityFloors = DEFAULT_FLOORS,
) -> dict[str, Any]:
    """Card 3 -- CE vs PE OI by strike, with max pain and spot marked.

    Shape::

        {"as_of", "floors", "underlying", "expiry", "spot",
         "max_pain": {"strike", "distance_from_spot", "total_oi", "status"},
         "rows": [{"strike", "ce_oi", "pe_oi", "ce_oi_change_day",
                   "pe_oi_change_day", "ce_volume", "pe_volume"}, ...]}
    """
    chain = read_option_chain(conn, underlying, expiry, as_of=as_of, floors=floors)
    rows = []
    for item in chain["rows"]:
        ce, pe = item.get("ce") or {}, item.get("pe") or {}
        rows.append({
            "strike": item["strike"],
            "ce_oi": ce.get("oi"),
            "pe_oi": pe.get("oi"),
            "ce_oi_change_day": ce.get("oi_change_day"),
            "pe_oi_change_day": pe.get("oi_change_day"),
            "ce_volume": ce.get("volume"),
            "pe_volume": pe.get("volume"),
        })
    s = chain.get("summary") or {}
    return _envelope(
        chain["as_of"], floors,
        underlying=underlying,
        expiry=chain["expiry"],
        spot=chain.get("spot"),
        max_pain={
            "strike": s.get("max_pain_strike"),
            "distance_from_spot": s.get("max_pain_distance"),
            "total_oi": s.get("max_pain_total_oi"),
            "status": s.get("max_pain_status", STATUS_NO_DATA),
        },
        rows=rows,
    )


INDEX_UNDERLYINGS = ("NIFTY", "BANKNIFTY", "FINNIFTY")


def read_index_summary(
    conn: sqlite3.Connection,
    *,
    underlyings: Sequence[str] = INDEX_UNDERLYINGS,
    as_of: datetime | None = None,
    floors: LiquidityFloors = DEFAULT_FLOORS,
) -> dict[str, Any]:
    """Card 4 -- PCR, max pain and the day's OI shift for the index underlyings.

    Shape::

        {"as_of", "floors", "rows": [
            {"underlying", "expiry", "days_to_expiry", "spot",
             "pcr_oi", "pcr_volume", "pcr_trend", "pcr_trend_change",
             "max_pain_strike", "max_pain_distance", "max_pain_total_oi",
             "oi_change_pct_day", "oi_change_day_status", "premium_cr",
             "pcr_series": [{"captured_at", "pcr_oi"}, ...]}]}

    ``pcr_series`` is the day's readings so far -- the "OI shift through the
    day" line on the card.
    """
    stamp = as_of.isoformat(sep=" ", timespec="seconds") if as_of else _latest_mark(conn)
    if stamp is None:
        return _envelope(None, floors, rows=[], status="no metrics yet")
    session = stamp[:10]
    rows = _rows(
        conn,
        "SELECT * FROM metrics WHERE captured_at=? AND scope='underlying'"
        f" AND underlying IN ({','.join('?' * len(underlyings))})"
        " ORDER BY underlying, expiry",
        [stamp, *underlyings],
    )
    for r in rows:
        r["pcr_series"] = _rows(
            conn,
            "SELECT captured_at, pcr_oi, total_ce_oi, total_pe_oi FROM metrics"
            " WHERE scope='underlying' AND underlying=? AND expiry IS ? "
            " AND date(captured_at)=? ORDER BY captured_at",
            [r["underlying"], r.get("expiry"), session],
        )
    return _envelope(stamp, floors, rows=rows)


def read_futures_buildup(
    conn: sqlite3.Connection,
    *,
    as_of: datetime | None = None,
    floors: LiquidityFloors = DEFAULT_FLOORS,
    underlyings: Sequence[str] | None = None,
    only_floor_passing: bool = True,
    limit: int = 200,
) -> dict[str, Any]:
    """Card 5 -- the futures OI build-up table across underlyings.

    Shape::

        {"as_of", "floors", "rows": [
            {"underlying", "tradingsymbol", "expiry", "days_to_expiry",
             "last_price", "spot", "oi", "buildup_15m", "buildup_day",
             "oi_change_day", "oi_change_pct_day",
             "fut_oi_vs_avg", "fut_oi_vs_avg_status",
             "basis", "basis_pct", "basis_status", "premium_cr"}]}
    """
    stamp = as_of.isoformat(sep=" ", timespec="seconds") if as_of else _latest_mark(conn)
    if stamp is None:
        return _envelope(None, floors, rows=[], status="no metrics yet")
    args: list[Any] = [stamp]
    sql = ("SELECT * FROM metrics WHERE captured_at=? AND scope='contract'"
           " AND instrument_type='FUT'")
    if underlyings:
        sql += f" AND underlying IN ({','.join('?' * len(underlyings))})"
        args += list(underlyings)
    if only_floor_passing:
        sql += " AND floors_passed=1"
    sql += " ORDER BY COALESCE(premium_rs,0) DESC LIMIT ?"
    args.append(limit)
    return _envelope(stamp, floors, rows=_rows(conn, sql, args))


def describe_floors(floors: LiquidityFloors = DEFAULT_FLOORS) -> dict[str, Any]:
    """The floors payload the UI prints on every card."""
    return floors.as_dict()


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(repr(obj))


def dumps(payload: Any) -> str:
    """JSON with dates handled -- used by ``metrics_cli`` and safe for the API."""
    return json.dumps(payload, default=_json_default, indent=2)


# ===========================================================================
# Public aliases for the app layer (worker D3)
# ===========================================================================
#
# D3's reader (`kanida-app/server/kanida_pilot/derivatives.py`) calls this
# module by a shorter set of names, with keyword arguments only, no connection,
# and it expects a **flat list of row dicts** -- it does its own grouping,
# CE/PE pairing, floor re-check and sorting, and states `source:
# metrics_module` when we answered.  The `read_*` functions above return the
# card envelope instead (as-of + floors + nested rows), which is the shape the
# HTTP API wants.  Both shapes are real and both are kept; these aliases are
# the flat-row spelling.
#
# One more thing the aliases do: D3's shaper reads canonical field names
# (`volume_ratio`, `previous_oi`, `volume_to_oi`).  Our stored column names are
# the same everywhere except three, so `_canonical_row` adds those three keys
# beside the originals rather than renaming anything.

#: stored column -> the key name D3's shaper reads.  Our column names and D1's
#: live `metrics` table agree (D1 built the table from this module's DDL); the
#: app simply spells several of them the other way round (`price_change_15m_pct`
#: where we store `price_change_pct_15m`).  Nothing is renamed on either side:
#: the alias key is added *beside* the stored one on the way out.
_D3_ALIASES = {
    "vol_tod_ratio": "volume_ratio",
    "vol_tod_sessions": "volume_baseline_sessions",
    "vol_oi_ratio": "volume_to_oi",
    "vol_oi_prev_oi": "previous_oi",
    "price_change_pct_15m": "price_change_15m_pct",
    "price_change_pct_day": "price_change_day_pct",
    "oi_change_pct_15m": "oi_change_15m_pct",
    "oi_change_pct_day": "oi_change_day_pct",
    "fut_oi_vs_avg": "oi_vs_20d_avg",
    "premium_rs": "premium_inr",
}


def _canonical_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """A stored metrics row plus the key spellings D3's shaper reads.

    ``oi_lots`` is derived here rather than stored: it is OI in the unit the
    liquidity floor is stated in, and D1's table has no column for it.
    """
    out = dict(row)
    for stored, alias in _D3_ALIASES.items():
        if stored in out and alias not in out:
            out[alias] = out[stored]
    if out.get("oi_lots") is None:
        out["oi_lots"] = _lots(out.get("oi"), out.get("lot_size"))
    return out


#: Env override for the store the aliases open when the caller passes no path.
#: D3's `_delegate` calls these readers with its card keywords only, so this is
#: the one way a differently-placed `derivatives.db` can be pointed at without
#: the app changing.  Both sides resolve to `<repo>/db/derivatives.db` today.
DB_PATH_ENV = ("KANIDA_DERIVATIVES_DB", "DERIVATIVES_DB")


def resolve_db_path(db_path: str | Path | None = None) -> Path:
    """Explicit path, else the env override, else `<repo>/db/derivatives.db`."""
    if db_path:
        return Path(db_path)
    for name in DB_PATH_ENV:
        value = os.environ.get(name)
        if value:
            return Path(value)
    return DEFAULT_DB_PATH


def _alias_conn(conn: sqlite3.Connection | None, db_path: str | Path | None):
    """Return ``(connection, must_close)``.  The app calls with no connection,
    so open the store read-only and close it again."""
    if conn is not None:
        return conn, False
    return connect(resolve_db_path(db_path), readonly=True), True


def _flat(payload: dict[str, Any], *, key: str = "rows") -> list[dict[str, Any]]:
    """Flatten a card envelope to its row list, carrying the as-of onto each row."""
    rows = [_canonical_row(r) for r in (payload.get(key) or [])]
    if payload.get("as_of"):
        for r in rows:
            r.setdefault("captured_at", payload["as_of"])
    return rows


def unusual_activity(
    *,
    underlying: str | None = None,
    expiry: str | date | None = None,
    max_days_to_expiry: int | None = None,
    option_type: str | None = None,
    min_premium_cr: float | None = None,
    limit: int | None = None,
    as_of: datetime | None = None,
    conn: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Card 1 as flat strike rows -- the contracts that cleared the floors and
    tripped a section 3.2/3.3 trigger.  ``read_unusual_activity`` is the
    enveloped, per-underlying spelling of the same numbers.

    ``min_premium_cr`` can only *raise* the premium floor, never lower it.
    """
    connection, close = _alias_conn(conn, db_path)
    try:
        floors = DEFAULT_FLOORS
        floor_cr = DEFAULT_FLOORS.min_premium_rs / RS_PER_CRORE
        if min_premium_cr is not None:
            floor_cr = max(floor_cr, float(min_premium_cr))
            floors = LiquidityFloors(
                min_premium_rs=floor_cr * RS_PER_CRORE,
                min_oi_lots=DEFAULT_FLOORS.min_oi_lots,
                min_last_price=DEFAULT_FLOORS.min_last_price,
            )
        payload = read_unusual_activity(
            connection,
            as_of=as_of,
            floors=floors,
            underlyings=[underlying] if underlying else None,
            limit=limit or 200,
        )
        rows: list[dict[str, Any]] = []
        for group in payload.get("rows") or []:
            for leg in group.get("strikes") or []:
                rows.append(_canonical_row(leg))
        exp = expiry.isoformat() if isinstance(expiry, date) else (expiry or None)
        if exp:
            rows = [r for r in rows if r.get("expiry") == exp]
        if option_type:
            rows = [r for r in rows if r.get("instrument_type") == option_type.upper()]
        if max_days_to_expiry is not None:
            rows = [
                r for r in rows
                if r.get("days_to_expiry") is not None
                and r["days_to_expiry"] <= max_days_to_expiry
            ]
        if min_premium_cr is not None:
            rows = [r for r in rows if (_num(r.get("premium_cr")) or 0.0) >= floor_cr]
        return rows
    finally:
        if close:
            connection.close()


def option_chain(
    *,
    underlying: str,
    expiry: str | date | None = None,
    as_of: datetime | None = None,
    conn: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Card 2 as flat CE/PE legs for one underlying + expiry (D3 pairs them by
    strike itself).  ``read_option_chain`` is the enveloped, pre-paired
    spelling.  No expiry given => the nearest expiry that has metrics."""
    connection, close = _alias_conn(conn, db_path)
    try:
        exp = expiry.isoformat() if isinstance(expiry, date) else (expiry or None)
        if not exp:
            exp = _front_expiry_with_metrics(connection, underlying, as_of=as_of)
        if not exp:
            return []
        payload = read_option_chain(connection, underlying, exp, as_of=as_of)
        rows: list[dict[str, Any]] = []
        for pair in payload.get("rows") or []:
            for side in ("ce", "pe"):
                leg = pair.get(side)
                if leg:
                    rows.append(_canonical_row(leg))
        rows.sort(key=lambda r: (_num(r.get("strike")) or 0.0, r.get("instrument_type") or ""))
        return rows
    finally:
        if close:
            connection.close()


def oi_by_strike(
    *,
    underlying: str,
    expiry: str | date | None = None,
    as_of: datetime | None = None,
    conn: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Card 3 as flat per-strike rows (``strike``, ``ce_oi``, ``pe_oi``, the two
    day OI changes, the volumes).  Max pain and spot live on the envelope, so
    use ``read_oi_by_strike`` when the card needs them."""
    connection, close = _alias_conn(conn, db_path)
    try:
        exp = expiry.isoformat() if isinstance(expiry, date) else (expiry or None)
        if not exp:
            exp = _front_expiry_with_metrics(connection, underlying, as_of=as_of)
        if not exp:
            return []
        return _flat(read_oi_by_strike(connection, underlying, exp, as_of=as_of))
    finally:
        if close:
            connection.close()


def index_summary(
    *,
    underlyings: Sequence[str] | None = None,
    underlying: str | None = None,
    as_of: datetime | None = None,
    conn: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Card 4 as flat per-underlying-per-expiry rows (PCR, its day trend, max
    pain, the day's OI shift).  ``read_index_summary`` is the enveloped
    spelling; each row here still carries its own ``pcr_series``."""
    connection, close = _alias_conn(conn, db_path)
    try:
        names = list(underlyings) if underlyings else ([underlying] if underlying else None)
        return _flat(read_index_summary(
            connection, underlyings=names or list(INDEX_UNDERLYINGS), as_of=as_of,
        ))
    finally:
        if close:
            connection.close()


def futures_buildup_rows(
    *,
    underlying: str | None = None,
    limit: int | None = None,
    include_below_floors: bool = False,
    as_of: datetime | None = None,
    conn: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Card 5 as flat futures rows: both build-up labels, ``oi_vs_20d_avg``,
    ``basis`` and the premium, one row per futures contract."""
    connection, close = _alias_conn(conn, db_path)
    try:
        return _flat(read_futures_buildup(
            connection,
            as_of=as_of,
            underlyings=[underlying] if underlying else None,
            only_floor_passing=not include_below_floors,
            limit=limit or 200,
        ))
    finally:
        if close:
            connection.close()


def futures_buildup(*args: Any, **kwargs: Any):
    """Two callers, one name -- nothing was renamed to make room.

    * ``futures_buildup(contract, snapshot, ...)`` -- the pure section 3.7
      computation, returning a :class:`FuturesBuildUp`.  This is what
      ``compute_for_mark`` and the unit tests call; it is also exported under
      the unambiguous name ``compute_futures_buildup``.
    * ``futures_buildup(underlying=..., limit=...)`` -- D3's card-5 reader,
      returning a flat list of stored row dicts (``futures_buildup_rows``).

    The two are told apart by the first positional argument: a
    :class:`ContractRef` means the computation, no positional argument at all
    means the reader.  Anything else raises rather than guessing.
    """
    if args:
        if isinstance(args[0], ContractRef):
            return compute_futures_buildup(*args, **kwargs)
        raise TypeError(
            "futures_buildup() takes a ContractRef positionally (the section 3.7 "
            "computation) or keyword arguments only (the card-5 reader); got "
            f"{type(args[0]).__name__}"
        )
    return futures_buildup_rows(**kwargs)


def _front_expiry_with_metrics(
    conn: sqlite3.Connection, underlying: str, *, as_of: datetime | None = None
) -> str | None:
    """The nearest expiry this underlying actually has option metrics for."""
    if not table_exists(conn, "metrics"):
        return None
    stamp = as_of.isoformat(sep=" ", timespec="seconds") if as_of else _latest_mark(conn)
    if stamp is None:
        return None
    row = conn.execute(
        "SELECT MIN(expiry) FROM metrics WHERE captured_at=? AND underlying=?"
        " AND expiry IS NOT NULL AND instrument_type IN ('CE','PE')",
        (stamp, underlying),
    ).fetchone()
    return row[0] if row and row[0] else None
