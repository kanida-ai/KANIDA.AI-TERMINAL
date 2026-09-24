"""Focused tests for the Workflow Health Watchdog (scripts/workflow_watchdog.py).

These cover the pure / DB logic without touching Task Scheduler, the network,
the live backend, or any broker. The end-to-end self-heal is proven separately
(against a throwaway dummy task) in the build session.
"""
from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import pytest

IST = timezone(timedelta(hours=5, minutes=30))

# ── Import the watchdog module by path (it lives in scripts/, not a package) ──
_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS = os.path.abspath(os.path.join(_HERE, "..", "..", "scripts"))
_WD_PATH = os.path.join(_SCRIPTS, "workflow_watchdog.py")


@pytest.fixture(scope="module")
def wd():
    spec = importlib.util.spec_from_file_location("workflow_watchdog", _WD_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── window gating ────────────────────────────────────────────────────────────
def test_active_window_weekday_inside(wd):
    d = datetime(2026, 6, 29, 9, 0, tzinfo=IST)  # Mon 09:00
    assert wd._in_active_window(d) is True


def test_active_window_weekday_outside(wd):
    assert wd._in_active_window(datetime(2026, 6, 29, 4, 0, tzinfo=IST)) is False
    assert wd._in_active_window(datetime(2026, 6, 29, 17, 0, tzinfo=IST)) is False


def test_active_window_weekend(wd):
    assert wd._in_active_window(datetime(2026, 6, 27, 9, 0, tzinfo=IST)) is False  # Sat


# ── Check result + safe wrapper ──────────────────────────────────────────────
def test_safe_never_raises(wd):
    def boom():
        raise ValueError("kaboom")
    c = wd._safe("x", boom)
    assert c.status == wd.WARN and "kaboom" in c.detail


def test_dry_run_heal_does_not_register(wd, monkeypatch):
    """dry-run on a MISSING task must report FAIL + 'would re-register', never heal."""
    monkeypatch.setattr(wd, "_AUTH_TASK", "KanidaTaskThatDoesNotExist_Test")
    c = wd.check_auth_task_present(dry_run=True)
    assert c.status == wd.FAIL
    assert "DRY-RUN" in c.detail and "would re-register" in c.detail
    assert c.healed is False


# ── snapshot table write + read ──────────────────────────────────────────────
def test_table_write_and_read(wd, tmp_path, monkeypatch):
    db = tmp_path / "wh.db"
    # Point POWER_DB_PATH used inside the module's lazy imports.
    import power_user.config as pc
    monkeypatch.setattr(pc, "POWER_DB_PATH", str(db))

    ran_at = datetime.now(IST).isoformat()
    checks = [
        wd.Check("a", wd.OK, "ok detail"),
        wd.Check("b", wd.WARN, "warn detail"),
        wd.Check("c", wd.FAIL, "fail detail"),
        wd.Check("d", wd.OK, "healed detail", healed=True),
    ]
    wd._ensure_table_and_write(checks, ran_at)

    con = sqlite3.connect(str(db))
    rows = con.execute(
        'SELECT "check", status, detail, healed FROM workflow_health ORDER BY id'
    ).fetchall()
    con.close()
    assert len(rows) == 4
    names = {r[0]: (r[1], r[3]) for r in rows}
    assert names["a"] == (wd.OK, 0)
    assert names["c"] == (wd.FAIL, 0)
    assert names["d"] == (wd.OK, 1)


def test_idempotent_create(wd, tmp_path, monkeypatch):
    """Calling _ensure_table_and_write twice must not raise (CREATE IF NOT EXISTS)."""
    db = tmp_path / "wh2.db"
    import power_user.config as pc
    monkeypatch.setattr(pc, "POWER_DB_PATH", str(db))
    wd._ensure_table_and_write([wd.Check("a", wd.OK, "x")], datetime.now(IST).isoformat())
    wd._ensure_table_and_write([wd.Check("a", wd.OK, "y")], datetime.now(IST).isoformat())
    con = sqlite3.connect(str(db))
    n = con.execute("SELECT COUNT(*) FROM workflow_health").fetchone()[0]
    con.close()
    assert n == 2


# ── alert de-dupe ────────────────────────────────────────────────────────────
def test_alert_dedupe(wd, tmp_path, monkeypatch):
    db = tmp_path / "wh3.db"
    import power_user.config as pc
    monkeypatch.setattr(pc, "POWER_DB_PATH", str(db))
    # Create the table first.
    wd._ensure_table_and_write([wd.Check("a", wd.OK, "x")], datetime.now(IST).isoformat())
    ran_at = datetime.now(IST).isoformat()
    day = ran_at[:10]
    assert wd._already_alerted_today("token_healthy", day) is False
    wd._record_alert_sent("token_healthy", ran_at, "dead token")
    assert wd._already_alerted_today("token_healthy", day) is True
    # A different check is independent.
    assert wd._already_alerted_today("backend_up", day) is False
