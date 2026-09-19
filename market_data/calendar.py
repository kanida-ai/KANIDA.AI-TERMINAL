"""NSE session calendar for the 15-minute base store.

Contract: docs/DATA_PIPELINE_CONTRACT.md section 2.

Design rules
------------
* A regular NSE equity session is 09:15 -> 15:30 IST, which divides exactly into
  **25** fifteen-minute bars (09:15, 09:30, ... 15:15).
* Holidays are **derived from the data itself**: a weekday inside the observed
  range for which the whole reference universe has no bars is a holiday, not a
  gap in one symbol's history.
* **Special sessions are handled explicitly, never treated as corruption**:
    - Muhurat trading (Diwali) -- a short evening session on a day that is
      otherwise a holiday, often a Saturday/Sunday. Its bounds are *derived
      from the observed bars* because NSE changes the timing every year.
    - Budget Saturdays / special live (DR) Saturdays -- regular 09:15-15:30
      hours on a weekend.
* Nothing here invents a session. If the reference universe has no bars for a
  date, that date has no session.

All datetimes are naive IST (matching how `db/kanida.db` stores `bar_time`).
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Iterable, Mapping, Sequence

BAR_MINUTES = 15
REGULAR_OPEN = time(9, 15)
REGULAR_CLOSE = time(15, 30)
REGULAR_BAR_COUNT = 25  # (15:30 - 09:15) / 15 minutes

KIND_REGULAR = "regular"
KIND_MUHURAT = "muhurat"
KIND_SPECIAL_WEEKEND = "special_weekend"

# ---------------------------------------------------------------------------
# Session regimes -- contract section 2A.
#
# NSE introduced a Closing Auction Session on 2026-08-03. Phase 1 covers
# cash-segment stocks that also have F&O contracts: continuous trading for
# those stocks ends at 15:15 (24 fifteen-minute bars) and the official close is
# the auction equilibrium price struck at 15:30-15:35, which is NOT a bar in
# the intraday series.
#
# So the expected bar count is per symbol AND per date. A CAS stock that runs
# to 15:15 is COMPLETE, not truncated.
# ---------------------------------------------------------------------------
REGIME_REGULAR = "regular"
REGIME_CAS = "cas"

CAS_START = date(2026, 8, 3)
CAS_CLOSE = time(15, 15)
CAS_BAR_COUNT = 24  # (15:15 - 09:15) / 15 minutes

# ---------------------------------------------------------------------------
# Explicit special sessions. Dates only -- the *times* for Muhurat are derived
# from the observed bars, because NSE publishes a different window each year.
# ---------------------------------------------------------------------------
MUHURAT_SESSIONS: dict[str, str] = {
    "2015-11-11": "Muhurat 2015 (Samvat 2072)",
    "2016-10-30": "Muhurat 2016 (Samvat 2073)",
    "2017-10-19": "Muhurat 2017 (Samvat 2074)",
    "2018-11-07": "Muhurat 2018 (Samvat 2075)",
    "2019-10-27": "Muhurat 2019 (Samvat 2076)",
    "2020-11-14": "Muhurat 2020 (Samvat 2077)",
    "2021-11-04": "Muhurat 2021 (Samvat 2078)",
    "2022-10-24": "Muhurat 2022 (Samvat 2079)",
    "2023-11-12": "Muhurat 2023 (Samvat 2080)",
    "2024-11-01": "Muhurat 2024 (Samvat 2081)",
    "2025-10-21": "Muhurat 2025 (Samvat 2082)",
}

# Weekend sessions with *regular* hours: Union Budget Saturdays and the
# exchange's special live-trading (disaster-recovery) Saturdays.
SPECIAL_WEEKEND_SESSIONS: dict[str, str] = {
    "2015-02-28": "Union Budget Saturday 2015",
    "2020-02-01": "Union Budget Saturday 2020",
    "2024-01-20": "Special live trading session (DR site)",
    "2024-03-02": "Special live trading session (DR site)",
    "2024-05-18": "Special live trading session (DR site)",
    "2025-02-01": "Union Budget Saturday 2025",
    "2026-02-01": "Union Budget weekend session 2026",
}


def _floor_15m(stamp: datetime) -> datetime:
    return stamp.replace(minute=(stamp.minute // BAR_MINUTES) * BAR_MINUTES,
                         second=0, microsecond=0)


@dataclass(frozen=True)
class Session:
    """One tradable session."""

    day: date
    start: datetime
    end: datetime
    expected_bars: int
    kind: str = KIND_REGULAR
    label: str = ""
    regime: str = REGIME_REGULAR

    @property
    def is_regular(self) -> bool:
        return self.kind == KIND_REGULAR

    @property
    def official_close_in_series(self) -> bool:
        """False for a CAS session: its official close is an auction price
        struck after the last intraday bar, so it is only in the daily bar."""
        return self.regime != REGIME_CAS

    def bar_starts(self) -> list[datetime]:
        step = timedelta(minutes=BAR_MINUTES)
        out, cur = [], self.start
        while cur < self.end:
            out.append(cur)
            cur += step
        return out

    def contains(self, stamp: datetime) -> bool:
        return self.start <= stamp < self.end

    def to_dict(self) -> dict:
        return {
            "day": self.day.isoformat(),
            "start": self.start.isoformat(sep=" "),
            "end": self.end.isoformat(sep=" "),
            "expected_bars": self.expected_bars,
            "kind": self.kind,
            "label": self.label,
            "regime": self.regime,
        }

    @staticmethod
    def from_dict(d: Mapping) -> "Session":
        return Session(
            day=date.fromisoformat(d["day"]),
            start=datetime.fromisoformat(d["start"]),
            end=datetime.fromisoformat(d["end"]),
            expected_bars=int(d["expected_bars"]),
            kind=d.get("kind", KIND_REGULAR),
            label=d.get("label", ""),
            regime=d.get("regime", REGIME_REGULAR),
        )

    def under_regime(self, regime: str) -> "Session":
        """This session as a symbol on `regime` experiences it.

        Only a full 09:15-15:30 session is shortened; Muhurat and any other
        non-standard window is left exactly as observed.
        """
        if regime != REGIME_CAS or self.regime == REGIME_CAS:
            return self
        if self.day < CAS_START or self.end.time() != REGULAR_CLOSE:
            return self
        end = datetime.combine(self.day, CAS_CLOSE)
        return Session(self.day, self.start, end, CAS_BAR_COUNT, self.kind,
                       self.label, REGIME_CAS)


def regular_session(day: date) -> Session:
    key = day.isoformat()
    kind = KIND_REGULAR
    label = ""
    if key in SPECIAL_WEEKEND_SESSIONS:
        kind = KIND_SPECIAL_WEEKEND
        label = SPECIAL_WEEKEND_SESSIONS[key]
    return Session(
        day=day,
        start=datetime.combine(day, REGULAR_OPEN),
        end=datetime.combine(day, REGULAR_CLOSE),
        expected_bars=REGULAR_BAR_COUNT,
        kind=kind,
        label=label,
    )


class SessionCalendar:
    """Sessions observed in the data, plus explicitly handled special days."""

    def __init__(self, sessions: Iterable[Session]):
        self._sessions: dict[date, Session] = {s.day: s for s in sessions}
        self._days: list[date] = sorted(self._sessions)

    # -- construction -------------------------------------------------------
    @classmethod
    def from_bar_times(cls, stamps: Iterable[datetime]) -> "SessionCalendar":
        """Build from observed *reference universe* bar starts.

        A date is a session iff the reference universe traded that date. The
        session window is the regular 09:15-15:30 grid unless the date is a
        known Muhurat session, in which case the window is derived from the
        observed bars (rounded out to the 15-minute grid).
        """
        seen: dict[date, list[datetime]] = {}
        for stamp in stamps:
            if isinstance(stamp, str):
                stamp = datetime.fromisoformat(stamp)
            seen.setdefault(stamp.date(), []).append(stamp)

        sessions: list[Session] = []
        for day, stamps_for_day in seen.items():
            key = day.isoformat()
            if key in MUHURAT_SESSIONS:
                first = _floor_15m(min(stamps_for_day))
                last = _floor_15m(max(stamps_for_day)) + timedelta(minutes=BAR_MINUTES)
                expected = int((last - first).total_seconds() // (BAR_MINUTES * 60))
                sessions.append(Session(day, first, last, max(expected, 1),
                                        KIND_MUHURAT, MUHURAT_SESSIONS[key]))
            else:
                sessions.append(regular_session(day))
        return cls(sessions)

    @classmethod
    def from_kanida_db(
        cls,
        db_path: str | os.PathLike,
        reference_symbols: Sequence[str] = ("RELIANCE", "INFY", "TCS",
                                            "HDFCBANK", "ICICIBANK"),
        table: str = "ohlc_5min",
        cache_path: str | os.PathLike | None = None,
        refresh: bool = False,
    ) -> "SessionCalendar":
        """Derive the calendar from the legacy DB, **read-only**.

        Uses a handful of always-traded reference symbols so that a single
        suspended/delisted stock cannot manufacture a 'holiday'.
        """
        if cache_path and not refresh and os.path.exists(cache_path):
            return cls.load(cache_path)

        uri = f"file:{os.fspath(db_path)}?mode=ro"
        con = sqlite3.connect(uri, uri=True, timeout=60)
        try:
            con.execute("PRAGMA query_only=ON")
            stamps: set[datetime] = set()
            for sym in reference_symbols:
                rows = con.execute(
                    f"SELECT bar_time FROM {table} WHERE symbol=? ORDER BY bar_time",
                    (sym,),
                ).fetchall()
                for (bt,) in rows:
                    stamps.add(datetime.fromisoformat(bt))
        finally:
            con.close()

        cal = cls.from_bar_times(stamps)
        if cache_path:
            cal.save(cache_path)
        return cal

    # -- persistence --------------------------------------------------------
    def to_json(self) -> str:
        return json.dumps({"version": 1,
                           "sessions": [s.to_dict() for s in self.sessions()]})

    def save(self, path: str | os.PathLike) -> None:
        tmp = f"{os.fspath(path)}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(self.to_json())
        os.replace(tmp, path)

    @classmethod
    def load(cls, path: str | os.PathLike) -> "SessionCalendar":
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        return cls(Session.from_dict(d) for d in payload["sessions"])

    # -- queries ------------------------------------------------------------
    def __len__(self) -> int:
        return len(self._sessions)

    def sessions(self) -> list[Session]:
        return [self._sessions[d] for d in self._days]

    @property
    def first_day(self) -> date | None:
        return self._days[0] if self._days else None

    @property
    def last_day(self) -> date | None:
        return self._days[-1] if self._days else None

    def session(self, day: date | datetime) -> Session | None:
        if isinstance(day, datetime):
            day = day.date()
        return self._sessions.get(day)

    def session_for(self, stamp: datetime) -> Session | None:
        """Session that contains `stamp` (None outside session hours)."""
        s = self._sessions.get(stamp.date())
        if s and s.contains(stamp):
            return s
        return None

    def expected_bars(self, day: date | datetime) -> int:
        s = self.session(day)
        return s.expected_bars if s else 0

    def is_session(self, day: date | datetime) -> bool:
        return self.session(day) is not None

    def is_holiday(self, day: date) -> bool:
        """A weekday inside the observed range with no session at all."""
        if self._in_range(day) is False:
            return False
        return day.weekday() < 5 and day not in self._sessions

    def is_special(self, day: date) -> bool:
        s = self.session(day)
        return bool(s) and s.kind != KIND_REGULAR

    def holidays(self) -> list[date]:
        """Weekday holidays derived from the observed session set."""
        if not self._days:
            return []
        out, cur, last = [], self._days[0], self._days[-1]
        while cur <= last:
            if cur.weekday() < 5 and cur not in self._sessions:
                out.append(cur)
            cur += timedelta(days=1)
        return out

    def trading_days(self, start: date, end: date) -> list[date]:
        return [d for d in self._days if start <= d <= end]

    def week_sessions(self, day: date | datetime) -> list[Session]:
        """Mon-Fri sessions of the ISO week containing `day`.

        Weekend special sessions (budget Saturday) are *excluded* from the
        weekly bucket: the contract says 1W is Mon-Fri.
        """
        if isinstance(day, datetime):
            day = day.date()
        monday = day - timedelta(days=day.weekday())
        out = []
        for i in range(5):
            s = self._sessions.get(monday + timedelta(days=i))
            if s:
                out.append(s)
        return out

    def next_session(self, day: date) -> Session | None:
        for d in self._days:
            if d > day:
                return self._sessions[d]
        return None

    def _in_range(self, day: date) -> bool:
        return bool(self._days) and self._days[0] <= day <= self._days[-1]

    def for_symbol(self, symbol: str,
                   regimes: "RegimeBook | None" = None) -> "SymbolCalendar":
        """A view of this calendar as `symbol` experiences it (contract 2A)."""
        return SymbolCalendar(self, symbol, regimes)

    # -- diagnostics --------------------------------------------------------
    def summary(self) -> dict:
        specials = [s for s in self.sessions() if s.kind != KIND_REGULAR]
        return {
            "sessions": len(self._sessions),
            "first_day": self._days[0].isoformat() if self._days else None,
            "last_day": self._days[-1].isoformat() if self._days else None,
            "weekday_holidays": len(self.holidays()),
            "special_sessions": [
                {"day": s.day.isoformat(), "kind": s.kind, "label": s.label,
                 "start": s.start.strftime("%H:%M"), "end": s.end.strftime("%H:%M"),
                 "expected_bars": s.expected_bars}
                for s in specials
            ],
        }


# ---------------------------------------------------------------------------
# Session regimes (contract section 2A)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SessionRegime:
    """A persisted "from this date, this symbol trades under this regime" record.

    Derived empirically, never from a static list: `source` records how we
    decided, so the record can be argued with.
    """

    symbol: str
    effective_from: date
    regime: str
    source: str = "observed"      # observed | is_fno | manual
    evidence: str = ""

    def to_dict(self) -> dict:
        return {"symbol": self.symbol,
                "effective_from": self.effective_from.isoformat(),
                "regime": self.regime, "source": self.source,
                "evidence": self.evidence}

    @staticmethod
    def from_dict(d: Mapping) -> "SessionRegime":
        return SessionRegime(d["symbol"], date.fromisoformat(d["effective_from"]),
                             d["regime"], d.get("source", "observed"),
                             d.get("evidence", ""))


class RegimeBook:
    """symbol -> the regime timeline. Empty book == everything regular."""

    def __init__(self, entries: Iterable[SessionRegime] = ()):
        self._by_symbol: dict[str, list[SessionRegime]] = {}
        for e in entries:
            self.add(e)

    def add(self, entry: SessionRegime) -> None:
        bucket = self._by_symbol.setdefault(entry.symbol, [])
        bucket.append(entry)
        bucket.sort(key=lambda e: e.effective_from)

    def __len__(self) -> int:
        return sum(len(v) for v in self._by_symbol.values())

    def symbols(self) -> list[str]:
        return sorted(self._by_symbol)

    def entries(self) -> list[SessionRegime]:
        return [e for sym in sorted(self._by_symbol) for e in self._by_symbol[sym]]

    def regime(self, symbol: str, day: date) -> str:
        current = REGIME_REGULAR
        for e in self._by_symbol.get(symbol, ()):
            if e.effective_from <= day:
                current = e.regime
            else:
                break
        return current

    @classmethod
    def from_observed_last_bars(cls, symbol: str,
                                last_bar_start_by_day: Mapping[date, datetime],
                                calendar: "SessionCalendar | None" = None,
                                is_fno: bool | None = None,
                                min_sessions: int = 2) -> "RegimeBook":
        """Decide CAS empirically: on/after 2026-08-03, full sessions whose last
        15m bar starts at 15:00 instead of 15:15.

        `is_fno` (from `instrument_labels.is_fno`) is only a **cross-check**: it
        is recorded in the evidence, never used to override what the bars say.
        """
        days = sorted(d for d in last_bar_start_by_day if d >= CAS_START)
        if calendar is not None:
            days = [d for d in days
                    if (s := calendar.session(d)) is not None
                    and s.end.time() == REGULAR_CLOSE]
        if not days:
            return cls()
        cas_days = [d for d in days
                    if last_bar_start_by_day[d].time() == time(15, 0)]
        full_days = [d for d in days
                     if last_bar_start_by_day[d].time() == time(15, 15)]
        if len(cas_days) < min_sessions or len(cas_days) <= len(full_days):
            return cls()
        first = min(cas_days)
        evidence = (f"cas_sessions={len(cas_days)} full_sessions={len(full_days)} "
                    f"first_cas={first.isoformat()} is_fno={is_fno}")
        return cls([SessionRegime(symbol, first, REGIME_CAS, "observed", evidence)])

    # -- persistence --------------------------------------------------------
    def to_rows(self) -> list[dict]:
        return [e.to_dict() for e in self.entries()]

    @classmethod
    def from_rows(cls, rows: Iterable[Mapping]) -> "RegimeBook":
        return cls(SessionRegime.from_dict(r) for r in rows)

    def save(self, path: str | os.PathLike) -> None:
        tmp = f"{os.fspath(path)}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"version": 1, "regimes": self.to_rows()}, fh)
        os.replace(tmp, path)

    @classmethod
    def load(cls, path: str | os.PathLike) -> "RegimeBook":
        with open(path, "r", encoding="utf-8") as fh:
            return cls.from_rows(json.load(fh)["regimes"])


class SymbolCalendar:
    """The market calendar as one symbol experiences it.

    Exposes the same surface `aggregate.py` / `validate.py` use, so those
    modules never need to know that regimes exist -- they just get the right
    session end and expected bar count for the symbol and date.
    """

    def __init__(self, calendar: SessionCalendar, symbol: str,
                 regimes: RegimeBook | None = None):
        self.calendar = calendar
        self.symbol = symbol
        self.regimes = regimes or RegimeBook()

    def regime(self, day: date | datetime) -> str:
        if isinstance(day, datetime):
            day = day.date()
        return self.regimes.regime(self.symbol, day)

    def session(self, day: date | datetime) -> Session | None:
        base = self.calendar.session(day)
        if base is None:
            return None
        return base.under_regime(self.regime(base.day))

    def session_for(self, stamp: datetime) -> Session | None:
        s = self.session(stamp.date())
        return s if s and s.contains(stamp) else None

    def expected_bars(self, day: date | datetime) -> int:
        s = self.session(day)
        return s.expected_bars if s else 0

    def is_session(self, day: date | datetime) -> bool:
        return self.calendar.is_session(day)

    def is_holiday(self, day: date) -> bool:
        return self.calendar.is_holiday(day)

    def is_special(self, day: date) -> bool:
        return self.calendar.is_special(day)

    def holidays(self) -> list[date]:
        return self.calendar.holidays()

    def sessions(self) -> list[Session]:
        return [s.under_regime(self.regime(s.day)) for s in self.calendar.sessions()]

    def week_sessions(self, day: date | datetime) -> list[Session]:
        return [s.under_regime(self.regime(s.day))
                for s in self.calendar.week_sessions(day)]

    def trading_days(self, start: date, end: date) -> list[date]:
        return self.calendar.trading_days(start, end)

    @property
    def first_day(self) -> date | None:
        return self.calendar.first_day

    @property
    def last_day(self) -> date | None:
        return self.calendar.last_day

    def __len__(self) -> int:
        return len(self.calendar)
