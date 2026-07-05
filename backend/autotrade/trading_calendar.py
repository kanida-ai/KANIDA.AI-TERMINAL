"""NSE trading-day / market-open calendar for the AutoTrade Sessions scheduler.

WHY THIS EXISTS (real-money safety):
  Before this module the scheduler had ZERO market-day awareness. entry_time was
  only a clock applied to TODAY, and a past target "fired immediately" — so a
  session created on a Sunday or an NSE holiday would fire entries into a CLOSED
  market (rejected/garbage fills, or worse on the live path). Every fire path now
  asks this module two questions before placing anything:
      1. is the FIRE DATE a real NSE trading day?  → is_trading_day(date)
      2. is the market OPEN right now?              → is_market_open(now_ist)

DESIGN (pragmatic + robust, in priority order):
  * WEEKDAY check        — Saturday/Sunday are never trading days. (Always on.)
  * NSE HOLIDAY SET      — a module-level, hand-maintained set of NSE full-day
                           holidays (seeded with 2026). ⚠️ MAINTENANCE ITEM: NSE
                           publishes the holiday list annually; this set MUST be
                           updated each year (and for any ad-hoc holiday). See
                           NSE_HOLIDAYS below. An optional override file
                           (data/config/nse_holidays.txt or env
                           FALCON_NSE_HOLIDAYS_FILE, one YYYY-MM-DD per line)
                           is merged in if present, so the operator can patch a
                           newly-announced holiday WITHOUT a code deploy.
  * ohlc_daily CONFIRM   — for a PAST/today date that falls within ohlc_daily's
                           coverage, the presence (or absence) of a row for that
                           trade_date is AUTHORITATIVE and overrides the heuristic
                           (it captures real exchange behaviour incl. special
                           sessions / unlisted holidays). ohlc_daily lives in the
                           LEGACY db (kanida_quant.db) and its coverage lags
                           (~T-1 at best), so it can only confirm the PAST — it is
                           never consulted for future dates. Best-effort: any DB
                           error falls back silently to the heuristic.

  Net rule for is_trading_day(d):
      not_weekend AND not_in_holiday_set, THEN if d is within ohlc coverage,
      defer to "did ohlc have a bar that day?" as the final word.

MARKET-OPEN window: NSE equity continuous session 09:15–15:30 IST (configurable
  via env). A fire at/after 15:30 or before 09:15 is "closed".

DETERMINISM: every public function takes the reference time/date as a parameter
  (default = real IST now) so tests can freeze "now" and never depend on the wall
  clock. Do NOT add hidden datetime.now() calls in the decision logic.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Set, Union

log = logging.getLogger("kanida.autotrade.calendar")

IST = timezone(timedelta(hours=5, minutes=30))

# ── Market-open window (IST). Configurable via env; defaults = NSE equity. ──────
def _clock_env(name: str, default: str) -> tuple[int, int]:
    raw = (os.environ.get(name, "") or default).strip()
    try:
        h, m = raw.split(":")[:2]
        return int(h), int(m)
    except Exception:  # pragma: no cover - defensive, fall back to default
        h, m = default.split(":")
        return int(h), int(m)


def market_open_window() -> tuple[tuple[int, int], tuple[int, int]]:
    """(open_hm, close_hm) for the NSE continuous session, IST. Env-overridable:
    FALCON_MARKET_OPEN="09:15", FALCON_MARKET_CLOSE="15:30"."""
    return (_clock_env("FALCON_MARKET_OPEN", "09:15"),
            _clock_env("FALCON_MARKET_CLOSE", "15:30"))


# ── NSE HOLIDAY SET ─────────────────────────────────────────────────────────────
# ⚠️ MAINTENANCE ITEM — KEEP THIS CURRENT. NSE trading holidays (full-day market
# closure) for the equity segment. Source: NSE annual holiday circular. This list
# MUST be refreshed every year and whenever NSE announces an ad-hoc holiday.
# Seeded with the 2026 NSE trading holidays (the year in play). 2025 included for
# back-testing/ohlc-confirmation of recent past dates. Muhurat/special sessions
# are NOT modelled (they are extra OPEN windows, not closures — and ohlc_daily
# confirmation will catch them for past dates).
NSE_HOLIDAYS: Set[str] = {
    # ── 2026 NSE equity trading holidays ──
    # Reconciled 2026-07-05 against the official NSE circular via three agreeing
    # public sources (cleartax / kotakneo / groww); 06-26 also operator-confirmed.
    # Weekend-only festivals are intentionally OMITTED (they are already
    # non-trading): Mahashivratri, Id-Ul-Fitr, and Independence Day (2026-08-15,
    # Sat) all fall on Sat/Sun in 2026. The Diwali Laxmi-Pujan Muhurat special
    # session (Sun 2026-11-08) is a trading session, NOT a full closure, so it is
    # not listed. Prior list had several wrong dates (Holi/Bakri/Ganesh/Dussehra/
    # Diwali off by days→weeks, a bogus 04-01, missing Ram Navami) — corrected here.
    # (2026-01-15 "Municipal Election" was in some public lists but the market
    #  actually traded that day per ohlc_daily — NOT a holiday; omitted.)
    "2026-01-26",  # Republic Day
    "2026-03-03",  # Holi
    "2026-03-26",  # Shri Ram Navami
    "2026-03-31",  # Shri Mahavir Jayanti
    "2026-04-03",  # Good Friday
    "2026-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
    "2026-05-01",  # Maharashtra Day
    "2026-05-28",  # Bakri Id
    "2026-06-26",  # Muharram (operator-confirmed 2026-06-29)
    "2026-09-14",  # Ganesh Chaturthi
    "2026-10-02",  # Mahatma Gandhi Jayanti
    "2026-10-20",  # Dussehra
    "2026-11-10",  # Diwali - Balipratipada
    "2026-11-24",  # Prakash Gurpurb Sri Guru Nanak Dev
    "2026-12-25",  # Christmas
    # ── 2025 (kept for past-date confirmation / back-tests) ──
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10", "2025-04-14",
    "2025-04-18", "2025-05-01", "2025-08-15", "2025-08-27", "2025-10-02",
    "2025-10-21", "2025-10-22", "2025-11-05", "2025-12-25",
}

# NOTE: dates flagged "[verify]" are best-effort placeholders pending the exact
# NSE 2026 circular. The WEEKDAY guard + ohlc_daily confirmation cover the common
# cases regardless; the operator should reconcile NSE_HOLIDAYS against the
# official 2026 NSE holiday circular (and the optional override file below lets a
# correction ship without a code deploy).


def _override_holidays() -> Set[str]:
    """Merge in any operator-supplied holiday overrides (one YYYY-MM-DD per line)
    from FALCON_NSE_HOLIDAYS_FILE or data/config/nse_holidays.txt. Lets a
    newly-announced holiday be patched WITHOUT a code deploy. Best-effort: a
    missing/garbled file is ignored (returns the seeded set unchanged)."""
    out: Set[str] = set(NSE_HOLIDAYS)
    candidates = []
    env_path = os.environ.get("FALCON_NSE_HOLIDAYS_FILE", "").strip()
    if env_path:
        candidates.append(Path(env_path))
    # repo-relative default: data/config/nse_holidays.txt under the backend tree.
    try:
        candidates.append(Path(__file__).resolve().parents[2]
                          / "data" / "config" / "nse_holidays.txt")
    except Exception:  # pragma: no cover
        pass
    for p in candidates:
        try:
            if p and p.exists():
                for line in p.read_text(encoding="utf-8").splitlines():
                    s = line.strip()
                    if not s or s.startswith("#"):
                        continue
                    # validate shape; ignore garbage lines
                    try:
                        datetime.strptime(s, "%Y-%m-%d")
                        out.add(s)
                    except ValueError:
                        continue
        except Exception:  # pragma: no cover - never let a file break the gate
            continue
    return out


# ── date coercion ────────────────────────────────────────────────────────────
def _to_date(d: Union[str, date, datetime]) -> date:
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    s = str(d).strip()
    return datetime.strptime(s, "%Y-%m-%d").date()


def _iso(d: Union[str, date, datetime]) -> str:
    return _to_date(d).isoformat()


# ── ohlc_daily confirmation (PAST dates only) ──────────────────────────────────
_OHLC_COVERAGE: Optional[tuple[Optional[str], Optional[str]]] = None


def _ohlc_coverage() -> tuple[Optional[str], Optional[str]]:
    """(min_trade_date, max_trade_date) in ohlc_daily, cached. ohlc_daily is in
    the LEGACY db. Returns (None, None) if unavailable. Best-effort — a missing
    table / DB error disables ohlc confirmation gracefully."""
    global _OHLC_COVERAGE
    if _OHLC_COVERAGE is not None:
        return _OHLC_COVERAGE
    try:
        from falcon.db import legacy_conn
        with legacy_conn() as con:
            row = con.execute(
                "SELECT MIN(trade_date), MAX(trade_date) FROM ohlc_daily"
            ).fetchone()
        _OHLC_COVERAGE = (row[0], row[1]) if row else (None, None)
    except Exception as e:  # pragma: no cover - DB/table absent in some envs
        log.debug("ohlc_daily coverage unavailable (%s) — heuristic only", e)
        _OHLC_COVERAGE = (None, None)
    return _OHLC_COVERAGE


def _ohlc_has_trading_day(iso_day: str) -> Optional[bool]:
    """Authoritative confirmation for a date WITHIN ohlc_daily coverage:
      True  → a bar exists for that trade_date (it WAS a trading day),
      False → no bar within coverage (it was NOT a trading day),
      None  → out of coverage / DB unavailable → caller uses the heuristic.
    Never raises."""
    lo, hi = _ohlc_coverage()
    if not lo or not hi:
        return None
    if iso_day < lo or iso_day > hi:
        return None  # out of coverage — heuristic only
    try:
        from falcon.db import legacy_conn
        with legacy_conn() as con:
            row = con.execute(
                "SELECT 1 FROM ohlc_daily WHERE trade_date=? LIMIT 1",
                (iso_day,),
            ).fetchone()
        return bool(row)
    except Exception as e:  # pragma: no cover - never let confirmation crash
        log.debug("ohlc_daily confirm failed for %s (%s)", iso_day, e)
        return None


# ── PUBLIC API ─────────────────────────────────────────────────────────────────
def is_holiday(d: Union[str, date, datetime]) -> bool:
    """True if the date is in the NSE holiday set (+ operator overrides)."""
    return _iso(d) in _override_holidays()


def is_trading_day(d: Union[str, date, datetime]) -> bool:
    """Is `d` an NSE trading day?

    weekday AND not-holiday — then, if within ohlc_daily coverage, defer to
    whether the exchange actually had a session that day (authoritative for the
    past). Future/out-of-coverage dates use the weekday+holiday heuristic only.
    """
    dd = _to_date(d)
    if dd.weekday() >= 5:                       # Sat/Sun
        return False
    iso = dd.isoformat()
    heuristic = iso not in _override_holidays()
    confirmed = _ohlc_has_trading_day(iso)
    if confirmed is not None:
        # Authoritative for in-coverage (past) dates. A weekend already returned
        # False above; here confirmed reflects real exchange behaviour.
        return confirmed
    return heuristic


def next_trading_day(d: Union[str, date, datetime],
                     inclusive: bool = False) -> date:
    """The next NSE trading day strictly AFTER `d` (or `d` itself if inclusive
    and `d` is a trading day). Skips weekends + holidays. Bounded scan (≤ 31
    days) so a mis-seeded holiday set can never loop forever."""
    dd = _to_date(d)
    if inclusive and is_trading_day(dd):
        return dd
    cur = dd + timedelta(days=1)
    for _ in range(31):
        if is_trading_day(cur):
            return cur
        cur = cur + timedelta(days=1)
    # Fallback (should be unreachable): the weekday-only next day.
    log.warning("next_trading_day: no trading day within 31d of %s — "
                "returning weekday fallback (check NSE_HOLIDAYS)", dd)
    cur = dd + timedelta(days=1)
    while cur.weekday() >= 5:
        cur = cur + timedelta(days=1)
    return cur


def is_market_open(now_ist: Optional[datetime] = None) -> bool:
    """Is the NSE continuous session open at `now_ist` (default: real IST now)?

    True iff `now_ist` is a trading day AND the IST clock is within
    [open, close) (default 09:15–15:30). Determinism: pass now_ist in tests.
    """
    n = now_ist or datetime.now(IST)
    if not is_trading_day(n.date()):
        return False
    (oh, om), (ch, cm) = market_open_window()
    cur = (n.hour, n.minute)
    if cur < (oh, om):
        return False
    if cur >= (ch, cm):
        return False
    return True


def market_open_for_date(d: Union[str, date, datetime]) -> tuple[datetime, datetime]:
    """The (open_dt, close_dt) IST-aware datetimes for a given date's session."""
    dd = _to_date(d)
    (oh, om), (ch, cm) = market_open_window()
    base = datetime(dd.year, dd.month, dd.day, tzinfo=IST)
    return (base.replace(hour=oh, minute=om),
            base.replace(hour=ch, minute=cm))
