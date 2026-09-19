"""The session calendar extended forward past the end of the observed history,
plus the NIFTY 500 universe.  Both read-only on ``db/kanida.db``.

Why this exists
---------------
``market_data.calendar.SessionCalendar`` derives sessions *from observed bars*,
which is exactly right for history and useless for today: the newest session in
``db/market15_calendar.json`` is the last day ``db/kanida.db`` contains
(2026-07-29 at the time of writing).  Live ingest and the live scanner need to
know that *today* is a session before any bar for today exists.

So we extend the observed calendar forward with regular 09:15-15:30 sessions on
weekdays that are not NSE holidays, using the holiday list the scanner already
ships in ``market_scanner/config.json`` (one source, not a second copy).  This
is a *forward* statement about the exchange timetable, never a backfill of a
day we have no data for: ``extend_calendar`` refuses to touch any day at or
before the observed calendar's last day.

Per-symbol CAS handling (contract 2A) is not done here -- it belongs to
``SessionCalendar.for_symbol(symbol, regime_book)``, which this module's output
feeds unchanged.
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from market_data.calendar import (
    KIND_SPECIAL_WEEKEND,
    Session,
    SessionCalendar,
    regular_session,
)

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CALENDAR_PATH = ROOT / "db" / "market15_calendar.json"
DEFAULT_KANIDA_DB = ROOT / "db" / "kanida.db"
SCANNER_CONFIG = ROOT / "market_scanner" / "config.json"

#: How far past today the calendar is projected, so `next_close` style lookups
#: never fall off the end.
DEFAULT_LOOKAHEAD_DAYS = 30


@dataclass(frozen=True)
class SymbolInfo:
    """One universe member, as ``instrument_labels`` knows it."""

    symbol: str
    kite_token: int | None
    is_fno: bool
    company: str = ""

    @property
    def instrument_id(self) -> int:
        from market_data.seed_from_legacy import instrument_id_for

        return instrument_id_for(self.symbol, self.kite_token)


# ---------------------------------------------------------------------------
# exchange timetable
# ---------------------------------------------------------------------------
def scanner_calendar_config(path: str | os.PathLike = SCANNER_CONFIG) -> dict:
    """Holiday/special-session config from the scanner, or empty if absent."""
    try:
        cfg = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"holidays": [], "special_sessions": {}, "closed_special_dates": []}
    return {
        "holidays": list(cfg.get("holidays", [])),
        "special_sessions": dict(cfg.get("special_sessions", {})),
        "closed_special_dates": list(cfg.get("closed_special_dates", [])),
    }


def _parse_times(pair: Sequence[str]) -> tuple[time, time]:
    return time.fromisoformat(pair[0]), time.fromisoformat(pair[1])


def extend_calendar(calendar: SessionCalendar, through: date, *,
                    holidays: Iterable[str | date] = (),
                    closed: Iterable[str | date] = (),
                    special: Mapping[str, Sequence[str]] | None = None
                    ) -> SessionCalendar:
    """`calendar` plus projected sessions for every day after its last observed
    day up to and including `through`.

    Days already in `calendar` are never rewritten -- what the data observed
    wins over what a config file predicts.
    """
    sessions = list(calendar.sessions())
    last = calendar.last_day
    if last is None:
        return calendar
    holiday_days = {date.fromisoformat(h) if isinstance(h, str) else h for h in holidays}
    closed_days = {date.fromisoformat(d) if isinstance(d, str) else d for d in closed}
    special_map = {date.fromisoformat(k): _parse_times(v)
                   for k, v in (special or {}).items()}

    day = last + timedelta(days=1)
    while day <= through:
        if day in closed_days:
            day += timedelta(days=1)
            continue
        if day in special_map:
            start_t, end_t = special_map[day]
            start = datetime.combine(day, start_t)
            end = datetime.combine(day, end_t)
            expected = max(int((end - start).total_seconds() // 900), 1)
            kind = KIND_SPECIAL_WEEKEND if day.weekday() >= 5 else "regular"
            sessions.append(Session(day, start, end, expected, kind,
                                    "projected special session"))
        elif day.weekday() < 5 and day not in holiday_days:
            sessions.append(regular_session(day))
        day += timedelta(days=1)
    return SessionCalendar(sessions)


def live_calendar(through: date | None = None, *,
                  path: str | os.PathLike = DEFAULT_CALENDAR_PATH,
                  kanida_db: str | os.PathLike = DEFAULT_KANIDA_DB,
                  config_path: str | os.PathLike = SCANNER_CONFIG,
                  lookahead_days: int = DEFAULT_LOOKAHEAD_DAYS) -> SessionCalendar:
    """The observed calendar, projected forward to `through` (default: today +
    `lookahead_days`).

    Loads the cached JSON calendar when present; otherwise derives it from
    ``db/kanida.db`` read-only and caches it.
    """
    if os.path.exists(path):
        base = SessionCalendar.load(path)
    else:
        base = SessionCalendar.from_kanida_db(kanida_db, cache_path=path)
    through = through or (date.today() + timedelta(days=lookahead_days))
    cfg = scanner_calendar_config(config_path)
    return extend_calendar(base, through, holidays=cfg["holidays"],
                           closed=cfg["closed_special_dates"],
                           special=cfg["special_sessions"])


# ---------------------------------------------------------------------------
# universe
# ---------------------------------------------------------------------------
def nifty500(db_path: str | os.PathLike = DEFAULT_KANIDA_DB, *,
             index_column: str = "in_nifty500",
             symbols: Sequence[str] | None = None) -> list[SymbolInfo]:
    """The live universe from ``instrument_labels``, **read-only**.

    Same predicate as ``seed_from_legacy.load_universe`` so the live loop and
    the seed cover exactly the same names.
    """
    if index_column not in ("in_nifty50", "in_nifty100", "in_nifty200",
                            "in_nifty500"):
        raise ValueError(f"unsupported index column {index_column!r}")
    uri = f"file:{Path(db_path).as_posix()}?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=60)
    try:
        con.execute("PRAGMA query_only=ON")
        rows = con.execute(
            f"SELECT symbol, kite_token, COALESCE(is_fno,0), "
            f"COALESCE(company, company_name, symbol) FROM instrument_labels "
            f"WHERE {index_column}=1 AND is_active=1 AND exchange='NSE' "
            f"ORDER BY symbol").fetchall()
    finally:
        con.close()
    out = [SymbolInfo(r[0], int(r[1]) if r[1] else None, bool(r[2]), r[3])
           for r in rows]
    if symbols is not None:
        wanted = {s.upper() for s in symbols}
        out = [s for s in out if s.symbol.upper() in wanted]
    return out
