"""Tests for power_user.services.auth_status — the single source of truth for
'is the Zerodha token healthy right now'.

The key invariant we test: the `degraded` flag MUST only be True when:
  - the token is invalid, AND
  - it's a weekday, AND
  - we're past 09:30 IST

Anything else (weekend, before market open, valid token) must be False —
that flag drives the yellow banner on /power/live that hides intraday
overlays, and we don't want it firing on Saturdays.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.normpath(os.path.join(_HERE, "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from power_user.services import auth_status     # noqa: E402
from power_user.db_init import init_power_user_schema    # noqa: E402

# Force these into sys.modules early so monkeypatch can resolve them by
# dotted path — the production code imports them lazily inside get_status().
import services        # noqa: E402, F401
try:
    import services.kite_auth     # noqa: E402, F401
except Exception:
    # Some test environments lack the real module; install a shim with the
    # attribute auth_status reaches for. Tests will overwrite it via monkeypatch.
    import types as _t, sys as _sys
    _m = _t.ModuleType("services.kite_auth")
    _m.get_token_status = lambda: {"valid": False}
    _sys.modules["services.kite_auth"] = _m
    setattr(services, "kite_auth", _m)
try:
    import services.auth_scheduler    # noqa: E402, F401
except Exception:
    import types as _t, sys as _sys
    _m = _t.ModuleType("services.auth_scheduler")
    _m.status = lambda: {"next_attempt_at": None}
    _sys.modules["services.auth_scheduler"] = _m
    setattr(services, "auth_scheduler", _m)

IST = timezone(timedelta(hours=5, minutes=30))


@pytest.fixture
def tmp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_power_user_schema(path)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


def _set_fake_kite_status(monkeypatch, *, valid: bool, **extra):
    """Patch services.kite_auth.get_token_status with a deterministic value."""
    import services.kite_auth as _ka
    fake = {"valid": valid, **extra}
    monkeypatch.setattr(_ka, "get_token_status", lambda: fake, raising=False)


def _set_fake_scheduler(monkeypatch, next_at: str | None = None):
    import services.auth_scheduler as _sched
    monkeypatch.setattr(_sched, "status",
                          lambda: {"next_attempt_at": next_at}, raising=False)


def _set_fake_now(monkeypatch, fake_dt: datetime):
    """Force auth_status._now to return our fixed datetime."""
    real_dt = auth_status.datetime
    class _FakeDT(real_dt):
        @classmethod
        def now(cls, tz=None):     # noqa: ARG003
            return fake_dt
    monkeypatch.setattr(auth_status, "datetime", _FakeDT)


# ──────────────────────────────────────────────────────────────────────────
# degraded flag — the high-stakes invariant
# ──────────────────────────────────────────────────────────────────────────

def test_degraded_false_when_token_valid(tmp_db, monkeypatch):
    _set_fake_kite_status(monkeypatch, valid=True, user="X")
    _set_fake_scheduler(monkeypatch)
    # Weekday morning, past 09:30 — but valid token means NOT degraded
    _set_fake_now(monkeypatch, datetime(2026, 5, 14, 10, 0, tzinfo=IST))   # Thursday 10:00 IST
    s = auth_status.get_status(tmp_db)
    assert s["token_valid"] is True
    assert s["degraded"] is False


def test_degraded_true_when_invalid_and_past_0930_on_weekday(tmp_db, monkeypatch):
    _set_fake_kite_status(monkeypatch, valid=False)
    _set_fake_scheduler(monkeypatch)
    _set_fake_now(monkeypatch, datetime(2026, 5, 14, 10, 15, tzinfo=IST))   # Thursday 10:15 IST
    s = auth_status.get_status(tmp_db)
    assert s["token_valid"] is False
    assert s["degraded"] is True


def test_degraded_false_when_invalid_but_before_0930(tmp_db, monkeypatch):
    _set_fake_kite_status(monkeypatch, valid=False)
    _set_fake_scheduler(monkeypatch)
    _set_fake_now(monkeypatch, datetime(2026, 5, 14, 8, 0, tzinfo=IST))    # Thursday 08:00 IST
    s = auth_status.get_status(tmp_db)
    assert s["degraded"] is False, "Before 9:30 IST — the morning bot still has runway"


def test_degraded_false_on_weekend_even_if_invalid(tmp_db, monkeypatch):
    _set_fake_kite_status(monkeypatch, valid=False)
    _set_fake_scheduler(monkeypatch)
    _set_fake_now(monkeypatch, datetime(2026, 5, 16, 12, 0, tzinfo=IST))   # Saturday noon IST
    s = auth_status.get_status(tmp_db)
    assert s["degraded"] is False, "No market = no degradation banner"


def test_degraded_false_on_sunday_even_if_invalid(tmp_db, monkeypatch):
    _set_fake_kite_status(monkeypatch, valid=False)
    _set_fake_scheduler(monkeypatch)
    _set_fake_now(monkeypatch, datetime(2026, 5, 17, 14, 0, tzinfo=IST))   # Sunday afternoon
    s = auth_status.get_status(tmp_db)
    assert s["degraded"] is False


def test_degraded_true_exactly_at_0930_on_weekday(tmp_db, monkeypatch):
    """Boundary case: at the stroke of 09:30, banner should fire."""
    _set_fake_kite_status(monkeypatch, valid=False)
    _set_fake_scheduler(monkeypatch)
    _set_fake_now(monkeypatch, datetime(2026, 5, 14, 9, 30, tzinfo=IST))
    s = auth_status.get_status(tmp_db)
    assert s["degraded"] is True


# ──────────────────────────────────────────────────────────────────────────
# last_auto_attempt + today_attempts — driven by falcon_auth_log
# ──────────────────────────────────────────────────────────────────────────

def _seed_attempt(db_path: str, *, attempt_at: str, status: str,
                  attempt_of_day: int = 1, error_code: str | None = None) -> None:
    con = sqlite3.connect(db_path)
    con.execute("""
        INSERT INTO falcon_auth_log
          (attempt_at, attempt_of_day, trigger_kind, status, elapsed_ms,
           stage, error_code, error_detail, token_preview)
        VALUES (?, ?, 'scheduled', ?, 0, NULL, ?, NULL, NULL)
    """, (attempt_at, attempt_of_day, status, error_code))
    con.commit()
    con.close()


def test_last_auto_attempt_is_most_recent(tmp_db, monkeypatch):
    _set_fake_kite_status(monkeypatch, valid=True)
    _set_fake_scheduler(monkeypatch)
    today = datetime.now(IST).date().isoformat()
    _seed_attempt(tmp_db, attempt_at=f"{today}T06:30:00+05:30",
                  status="failed", attempt_of_day=1, error_code="BAD_CREDS")
    _seed_attempt(tmp_db, attempt_at=f"{today}T07:30:00+05:30",
                  status="success", attempt_of_day=2)

    s = auth_status.get_status(tmp_db)
    assert s["last_auto_attempt"]["status"] == "success"
    assert s["last_auto_attempt"]["attempt_of_day"] == 2
    assert s["today_attempts"] == 2


def test_today_attempts_only_counts_today(tmp_db, monkeypatch):
    _set_fake_kite_status(monkeypatch, valid=False)
    _set_fake_scheduler(monkeypatch)
    today = datetime.now(IST).date().isoformat()
    yesterday = (datetime.now(IST) - timedelta(days=1)).date().isoformat()
    _seed_attempt(tmp_db, attempt_at=f"{yesterday}T06:30:00+05:30",
                  status="failed", attempt_of_day=1)
    _seed_attempt(tmp_db, attempt_at=f"{today}T07:30:00+05:30",
                  status="failed", attempt_of_day=2)
    s = auth_status.get_status(tmp_db)
    assert s["today_attempts"] == 1
    assert s["last_auto_attempt"]["attempt_at"].startswith(today)


def test_get_status_survives_corrupt_db(monkeypatch, tmp_path):
    """When the DB is missing/corrupt, get_status returns sensible defaults
    rather than raising — the admin widget must keep rendering."""
    _set_fake_kite_status(monkeypatch, valid=False)
    _set_fake_scheduler(monkeypatch)
    bogus = str(tmp_path / "no_such.db")
    s = auth_status.get_status(bogus)
    # Doesn't crash; flags reflect best-effort state
    assert s["token_valid"] is False
    assert s["today_attempts"] == 0
    assert s["last_auto_attempt"] is None


# ──────────────────────────────────────────────────────────────────────────
# recent_log — pagination + ordering
# ──────────────────────────────────────────────────────────────────────────

def test_recent_log_orders_newest_first_and_limits(tmp_db):
    for i in range(12):
        _seed_attempt(tmp_db,
                      attempt_at=f"2026-05-14T{6+i:02d}:00:00+05:30",
                      status="failed", attempt_of_day=(i % 4) + 1)
    rows = auth_status.recent_log(tmp_db, limit=5)
    assert len(rows) == 5
    # Newest first → highest hour first
    hours = [int(r["attempt_at"][11:13]) for r in rows]
    assert hours == sorted(hours, reverse=True)


def test_recent_log_empty_when_no_attempts(tmp_db):
    assert auth_status.recent_log(tmp_db) == []
