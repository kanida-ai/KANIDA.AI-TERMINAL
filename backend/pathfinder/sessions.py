"""
Exchange-calendar PROJECTION for dates the data cannot know yet.

The S1 engine stamps `GradingState.due_session` from the warehouse's own session list
(`MarketData.session_after`) — a fact. For a pending finding the horizon lies PAST the data
seal, so that fact does not exist yet and the engine leaves the field null. The feed still
wants to say a date; the honest way is a labelled projection: weekdays, net of the NSE
equity closures the API knows about, and a `due_session_basis` that says so. The GRADE never
uses this date — it lands on the first session the data actually reaches (research/scan.py).

Self-contained on purpose: the product's Pathfinder surface does not import the live OMS
(`backend/autotrade/`), which is walled off the customer path.
"""
from __future__ import annotations

from datetime import date, timedelta

#: NSE equity (CM) trading holidays, 2025–2026, reconciled against the official circulars
#: (the same list the OMS calendar carries). Muhurat / special sessions are extra OPEN windows,
#: not closures, and are not modelled. Years outside this table are weekday-only projections.
NSE_CLOSURES: frozenset[str] = frozenset({
    # 2025
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10", "2025-04-14", "2025-04-18", "2025-05-01",
    "2025-08-15", "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22", "2025-11-05", "2025-12-25",
    # 2026
    "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31", "2026-04-03", "2026-04-14", "2026-05-01",
    "2026-05-28", "2026-06-26", "2026-09-14", "2026-10-02", "2026-10-20", "2026-11-10", "2026-11-24",
    "2026-12-25",
})

CLOSURES_COVER_YEARS = (2025, 2026)

#: What the engine wrote when the date came from the warehouse's session list.
BASIS_SESSION_CALENDAR = "session_calendar: the n-th session after the edition in the sealed data"
#: What the API writes when it projected the date because the data seal had not reached it.
BASIS_PROJECTED = (
    "projected: weekdays after the edition net of NSE closures (circulars 2025–2026); the grade lands on the "
    "first session the data actually reaches, not on this date"
)


def is_projected_session(d: date) -> bool:
    return d.weekday() < 5 and d.isoformat() not in NSE_CLOSURES


def project_session_after(d: date, n: int) -> date:
    """The n-th projected session strictly after `d` (n >= 1)."""
    if n < 1:
        raise ValueError("n must be >= 1")
    cur = d
    left = n
    while left > 0:
        cur = cur + timedelta(days=1)
        if is_projected_session(cur):
            left -= 1
    return cur
