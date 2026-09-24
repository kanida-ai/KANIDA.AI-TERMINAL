"""Workflow Health Watchdog — continuous self-healing + alerting monitor.

WHY THIS EXISTS (the 2026-06-29 incident):
  The KanidaZerodhaAuth scheduled task silently VANISHED from Task Scheduler.
  With no task, the fresh-process auth worker never ran, the Kite token went
  stale, and we discovered it only because a live trade was about to fire. The
  root cause of the vanish was a `-WakeToRun $true` switch bug in
  register_auth_task.ps1 (a switch takes no value; the bad form could leave the
  task in a corrupt/unregistered state). That bug is now fixed.

  This watchdog is the structural defence: a fresh, short-lived process that
  runs every ~15 min, evaluates a fixed set of CHECKS about the *workflows*
  the trading system depends on, SELF-HEALS the small, safe, idempotent things
  (re-registering a vanished auth task), and ALERTS (Web Push, de-duped) for
  everything else. It writes a queryable health snapshot to `workflow_health`.

HARD SAFETY CONTRACT (do not weaken):
  * ADDITIVE ONLY. Never touches the order-execution path, falcon_position_state,
    or any broker order. Never restarts the backend. Never disturbs a SCHEDULED
    live autotrade session.
  * Self-heal is limited to: re-registering the KanidaZerodhaAuth scheduled task
    (idempotent — register_auth_task.ps1 removes+recreates). EVERYTHING ELSE is
    alert-only.
  * A watchdog must NEVER crash. Every check is wrapped; a missing dependency
    degrades to a SKIP/WARN, never an exception that aborts the run.
  * Fresh short-lived process every run (mirrors auth_worker.py) — avoids the
    aged-process Playwright/CIM pathology.
  * All times IST.

USAGE:
    python scripts/workflow_watchdog.py            # normal run (may self-heal)
    python scripts/workflow_watchdog.py --dry-run  # log what it WOULD heal; no changes
    python scripts/workflow_watchdog.py --force    # ignore the IST active-window gate

EXIT CODE: 0 if no FAIL checks, 1 if any FAIL (Task Scheduler shows last result).
"""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

# ── Make the backend package importable (mirror auth_worker.py) ──────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT = os.path.dirname(_HERE)
_BACKEND = os.path.join(_PROJECT, "backend")
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

IST = timezone(timedelta(hours=5, minutes=30))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("kanida.workflow_watchdog")

# ── Status constants ─────────────────────────────────────────────────────────
OK = "OK"
WARN = "WARN"
FAIL = "FAIL"

# ── IST active window (weekday ~05:30-16:30). Mirrors auth_worker self-gate but
# starts a touch earlier so we catch a missing auth task BEFORE the 06:00 token
# expiry + the EOD/pre-market workflow. Outside the window, --force overrides. ─
_WINDOW_START = (5, 30)
_WINDOW_END = (16, 30)

# Backend base URL — local loopback only (the watchdog never reaches the public
# tunnel; it checks the process on THIS machine).
_BACKEND_BASE = os.environ.get("KANIDA_BACKEND_BASE", "http://127.0.0.1:8001")

# Auth-task name we monitor + self-heal.
_AUTH_TASK = "KanidaZerodhaAuth"
_REGISTER_AUTH_PS1 = os.path.join(_HERE, "register_auth_task.ps1")

# Token freshness threshold for "auth ran recently" (minutes).
_AUTH_RECENT_MIN = 90
# Pre-live-trade gate hour: a dead token AT/AFTER this hour on a trading day is FAIL.
_LIVE_GATE_HOUR = 8


def _now_ist() -> datetime:
    return datetime.now(IST)


def _in_active_window(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    hm = (now.hour, now.minute)
    return _WINDOW_START <= hm <= _WINDOW_END


# ── Result type ──────────────────────────────────────────────────────────────
class Check:
    """One health check result + an optional self-heal that already ran."""

    __slots__ = ("name", "status", "detail", "healed")

    def __init__(self, name: str, status: str, detail: str, healed: bool = False):
        self.name = name
        self.status = status
        self.detail = detail
        self.healed = healed

    def __repr__(self) -> str:
        h = " (SELF-HEALED)" if self.healed else ""
        return f"[{self.status}] {self.name}: {self.detail}{h}"


def _safe(name: str, fn: Callable[[], "Check"]) -> "Check":
    """Run a check, never let it crash the watchdog."""
    try:
        return fn()
    except Exception as e:  # noqa: BLE001 — a watchdog must never crash
        log.exception("check %s raised", name)
        return Check(name, WARN, f"check raised (treated as WARN): {e}")


# ── PowerShell helper ────────────────────────────────────────────────────────
def _run_powershell(ps: str, timeout: int = 60) -> Tuple[int, str, str]:
    """Run a PowerShell snippet, return (rc, stdout, stderr). Never raises."""
    try:
        proc = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", ps],
            capture_output=True, text=True, timeout=timeout,
        )
        return proc.returncode, (proc.stdout or "").strip(), (proc.stderr or "").strip()
    except Exception as e:  # noqa: BLE001
        return 1, "", str(e)


# ── CHECK 1: AUTH TASK PRESENT (self-heals) ──────────────────────────────────
def check_auth_task_present(dry_run: bool) -> Check:
    name = "auth_task_present"
    rc, out, err = _run_powershell(
        f"$t = Get-ScheduledTask -TaskName '{_AUTH_TASK}' -ErrorAction SilentlyContinue; "
        "if ($null -eq $t) { 'MISSING' } else { $t.State }"
    )
    state = out.strip()
    if state and state != "MISSING":
        if state == "Ready":
            return Check(name, OK, f"{_AUTH_TASK} present, State=Ready")
        if state == "Running":
            return Check(name, OK, f"{_AUTH_TASK} present, State=Running")
        if state == "Disabled":
            return Check(name, FAIL, f"{_AUTH_TASK} present but State=Disabled — auth will not run")
        return Check(name, WARN, f"{_AUTH_TASK} present, State={state}")

    # MISSING (or query failed). The query failing is itself suspicious — but
    # only self-heal on an explicit MISSING to avoid spurious re-registers.
    if state != "MISSING":
        return Check(name, WARN,
                     f"could not query {_AUTH_TASK} (rc={rc}, err={err[:120]}) — not self-healing")

    # ── SELF-HEAL: re-register the vanished auth task ──
    if dry_run:
        return Check(name, FAIL,
                     f"{_AUTH_TASK} MISSING — [DRY-RUN] would re-register via {_REGISTER_AUTH_PS1}",
                     healed=False)

    log.warning("SELF-HEAL: %s is MISSING — re-registering via %s", _AUTH_TASK, _REGISTER_AUTH_PS1)
    hrc, hout, herr = _run_powershell(
        f"& '{_REGISTER_AUTH_PS1}'", timeout=90
    )
    # Verify it came back.
    vrc, vout, _ = _run_powershell(
        f"$t = Get-ScheduledTask -TaskName '{_AUTH_TASK}' -ErrorAction SilentlyContinue; "
        "if ($null -eq $t) { 'MISSING' } else { $t.State }"
    )
    if vout.strip() and vout.strip() != "MISSING":
        return Check(name, OK,
                     f"{_AUTH_TASK} was MISSING — SELF-HEALED (re-registered, now State={vout.strip()})",
                     healed=True)
    return Check(name, FAIL,
                 f"{_AUTH_TASK} MISSING and self-heal FAILED (rc={hrc}, err={(herr or hout)[:160]})")


# ── CHECK 2: AUTH RAN RECENTLY (alert only) ──────────────────────────────────
def check_auth_ran_recently() -> Check:
    """Within the IST window, the latest successful auth/token write should be
    fresh. We read BOTH the kite_tokens table (legacy DB; the token write itself)
    and falcon_auth_log (power DB; the attempt timeline). FAIL → alert. We do NOT
    run Playwright auth from here (the dedicated auth task owns that)."""
    name = "auth_ran_recently"
    now = _now_ist()
    today = now.date().isoformat()

    # Token-date check (cheap, authoritative): kite_tokens.token_date == today.
    token_date = None
    token_created = None
    try:
        from falcon.db import legacy_conn  # noqa: WPS433
        with legacy_conn() as con:
            row = con.execute(
                "SELECT token_date, created_at FROM kite_tokens ORDER BY id DESC LIMIT 1"
            ).fetchone()
        if row:
            token_date = row[0]
            token_created = row[1]
    except Exception as e:  # noqa: BLE001
        return Check(name, WARN, f"kite_tokens unreadable ({e}) — skipping")

    if token_date is None:
        return Check(name, FAIL, "no rows in kite_tokens — auth has never written a token")

    # Outside the active window we only verify a token EXISTS for today/yesterday.
    if not _in_active_window(now):
        return Check(name, OK, f"outside window; latest token_date={token_date}")

    # MORNING GRACE: Zerodha tokens expire at 06:00 IST and the auth task
    # refreshes around then. Before ~06:45 IST a token still dated YESTERDAY is
    # entirely normal (the morning refresh hasn't run/completed yet). The live
    # token check (check_token_healthy) is the real pre-trade gate; do not FAIL
    # on a stale-by-date token during the refresh window.
    if token_date != today:
        if (now.hour, now.minute) < (6, 45):
            return Check(name, OK,
                         f"token_date={token_date} (pre-06:45 refresh window — yesterday's token is normal)")
        return Check(name, FAIL,
                     f"latest token_date={token_date} != today {today} — auth has not refreshed today")

    # Token is for today. Optionally confirm the write/attempt is < threshold old.
    age_min = None
    try:
        from power_user.config import POWER_DB_PATH  # noqa: WPS433
        import sqlite3
        con = sqlite3.connect(POWER_DB_PATH, timeout=10.0)
        try:
            r = con.execute(
                "SELECT attempt_at FROM falcon_auth_log WHERE status='success' "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            con.close()
        if r and r[0]:
            last = datetime.fromisoformat(r[0])
            if last.tzinfo is None:
                last = last.replace(tzinfo=IST)
            age_min = (now - last).total_seconds() / 60.0
    except Exception:  # noqa: BLE001 — auth_log is best-effort
        age_min = None

    if age_min is not None and age_min > _AUTH_RECENT_MIN and now.hour >= _LIVE_GATE_HOUR:
        # Token is today's but the last success is stale AND we're in the trading
        # window — soft FAIL: the token file is fresh enough (today) so this is a
        # WARN, not a block (the live token check below is the real gate).
        return Check(name, WARN,
                     f"token_date=today but last success {age_min:.0f}m ago (>{_AUTH_RECENT_MIN}m)")
    detail = f"token_date=today ({token_created})"
    if age_min is not None:
        detail += f", last success {age_min:.0f}m ago"
    return Check(name, OK, detail)


# ── CHECK 3: TOKEN HEALTHY (live kite.profile) (alert only) ──────────────────
def check_token_healthy() -> Check:
    """Live Kite profile() via the same path preflight/auth use. FAIL after the
    live-gate hour on a trading day → alert (the pre-live-trade gate)."""
    name = "token_healthy"
    now = _now_ist()
    # Need the env loaded for the kite client to read the token + api key.
    healthy = None
    detail = ""
    try:
        from services.kite_auth import _load_env_file  # noqa: WPS433
        _load_env_file()
    except Exception:  # noqa: BLE001
        pass
    try:
        from services.auth_scheduler import _live_token_is_healthy  # noqa: WPS433
        healthy = bool(_live_token_is_healthy())
    except Exception as e:  # noqa: BLE001
        return Check(name, WARN, f"live token check unavailable ({e}) — skipping")

    if healthy:
        return Check(name, OK, "kite.profile() OK (live token valid)")

    # Unhealthy. Severity depends on time + trading day.
    try:
        from autotrade import trading_calendar  # noqa: WPS433
        is_td = trading_calendar.is_trading_day(now.date())
    except Exception:  # noqa: BLE001
        is_td = now.weekday() < 5
    if is_td and now.hour >= _LIVE_GATE_HOUR:
        return Check(name, FAIL,
                     f"live token INVALID at {now:%H:%M} IST on a trading day — pre-trade gate")
    return Check(name, WARN, "live token invalid (outside live-gate window)")


# ── CHECK 4: IP ALLOWED (reuse preflight) (alert only) ───────────────────────
def check_ip_allowed() -> Check:
    name = "ip_allowed"
    try:
        from services.kite_auth import _load_env_file, get_kite_client  # noqa: WPS433
        _load_env_file()
    except Exception:  # noqa: BLE001
        pass
    try:
        from falcon import preflight  # noqa: WPS433
        from services.kite_auth import get_kite_client  # noqa: WPS433
        kite = get_kite_client(check=False)
        if kite is None:
            return Check(name, WARN, "kite client unavailable — skipping IP check")
        res = preflight.check_kite_ip_allowed(kite, {})
        if res.status == preflight.GREEN:
            return Check(name, OK, res.detail)
        if res.status == preflight.RED:
            return Check(name, FAIL, res.detail)
        return Check(name, WARN, res.detail)
    except Exception as e:  # noqa: BLE001
        return Check(name, WARN, f"IP check unavailable ({e}) — skipping")


# ── CHECK 5: BACKEND UP (alert only — never restart) ─────────────────────────
def check_backend_up() -> Check:
    name = "backend_up"
    url = f"{_BACKEND_BASE}/openapi.json"
    try:
        import urllib.request
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=8) as resp:  # noqa: S310 — loopback only
            code = resp.getcode()
        if code == 200:
            return Check(name, OK, f"GET {url} -> 200")
        return Check(name, FAIL, f"GET {url} -> {code}")
    except Exception as e:  # noqa: BLE001
        return Check(name, FAIL, f"backend not reachable at {url} ({e}) — do NOT restart from here; alert operator")


# ── CHECK 6: SCHEDULED SESSIONS ARMED (alert only) ───────────────────────────
def check_scheduled_sessions_armed() -> Check:
    """For each autotrade_sessions row status=SCHEDULED, the backend's live
    status() must report scheduler_armed=True. The arming lives in the backend's
    in-memory entry_scheduler, so we MUST ask the running backend (operator-token
    gated) rather than infer from the DB. FAIL → alert (never re-arm from here)."""
    name = "scheduled_sessions_armed"
    # 1. Find SCHEDULED sessions from the DB.
    try:
        from falcon.db import falcon_conn  # noqa: WPS433
        with falcon_conn() as con:
            rows = con.execute(
                "SELECT session_id, mode FROM autotrade_sessions WHERE status='SCHEDULED'"
            ).fetchall()
        scheduled = [(r[0], r[1]) for r in rows]
    except Exception as e:  # noqa: BLE001
        return Check(name, WARN, f"autotrade_sessions unreadable ({e}) — skipping")

    if not scheduled:
        return Check(name, OK, "no SCHEDULED sessions")

    # 2. Ask the backend for each session's live armed state.
    token = os.environ.get("FALCON_OPERATOR_TOKEN", "").strip()
    if not token:
        # Try loading .env in case it wasn't loaded yet.
        try:
            from services.kite_auth import _load_env_file  # noqa: WPS433
            _load_env_file()
            token = os.environ.get("FALCON_OPERATOR_TOKEN", "").strip()
        except Exception:  # noqa: BLE001
            pass
    if not token:
        return Check(name, WARN,
                     f"{len(scheduled)} SCHEDULED session(s) but FALCON_OPERATOR_TOKEN unset — cannot verify armed")

    import json
    import urllib.request
    not_armed: List[str] = []
    unknown: List[str] = []
    for sid, mode in scheduled:
        url = f"{_BACKEND_BASE}/api/autotrade/session/{sid}/status"
        try:
            req = urllib.request.Request(url, method="GET",
                                         headers={"X-Operator-Token": token})
            with urllib.request.urlopen(req, timeout=8) as resp:  # noqa: S310
                data = json.loads(resp.read().decode("utf-8"))
            armed = data.get("scheduler_armed")
            if armed is True:
                continue
            not_armed.append(f"{sid[:8]}({mode},armed={armed})")
        except Exception as e:  # noqa: BLE001
            unknown.append(f"{sid[:8]}({e.__class__.__name__})")

    if not_armed:
        return Check(name, FAIL,
                     f"{len(not_armed)} SCHEDULED session(s) NOT armed: {', '.join(not_armed)} "
                     "— backend timer lost (restart?); operator must re-arm")
    if unknown:
        return Check(name, WARN,
                     f"could not verify {len(unknown)} session(s): {', '.join(unknown)}")
    return Check(name, OK, f"all {len(scheduled)} SCHEDULED session(s) armed")


# ── CHECK 7: EOD SIGNALS FRESH (alert only) ──────────────────────────────────
def check_eod_signals_fresh() -> Check:
    """Latest signal_date should equal the expected last trading day. Mirrors the
    eod-pipeline-signal-date-gate: NEVER flag weekends/holidays as stale. The
    'expected last trading day' is the most recent trading day STRICTLY BEFORE
    today if today's EOD hasn't run yet, else today. We use a tolerant rule:
    the latest signal_date must be >= the previous trading day."""
    name = "eod_signals_fresh"
    try:
        from falcon.db import falcon_conn  # noqa: WPS433
        with falcon_conn() as con:
            row = con.execute("SELECT MAX(signal_date) FROM falcon_signals_live").fetchone()
        latest = row[0] if row else None
    except Exception as e:  # noqa: BLE001
        return Check(name, WARN, f"falcon_signals_live unreadable ({e}) — skipping")

    if not latest:
        return Check(name, FAIL, "no rows in falcon_signals_live")

    now = _now_ist()
    try:
        from autotrade import trading_calendar  # noqa: WPS433
        # Expected: the last trading day that should already have produced
        # signals. Signals are generated EOD (~16:05 IST) for THAT day's bar, so:
        #   - before ~16:30 today: the previous trading day is the expectation
        #   - after: today (if a trading day) is the expectation
        prev_td = trading_calendar.next_trading_day(now.date(), inclusive=False)  # placeholder, replaced below
    except Exception as e:  # noqa: BLE001
        return Check(name, WARN, f"trading_calendar unavailable ({e}) — skipping")

    # Compute the previous trading day (strictly before today) robustly.
    try:
        from datetime import timedelta as _td
        d = now.date() - _td(days=1)
        guard = 0
        while not trading_calendar.is_trading_day(d) and guard < 15:
            d = d - _td(days=1)
            guard += 1
        prev_trading_day = d
        today_is_td = trading_calendar.is_trading_day(now.date())
        # Expectation: if today is a trading day AND we're past EOD (16:30),
        # expect today; otherwise expect the previous trading day.
        if today_is_td and (now.hour, now.minute) >= (16, 30):
            expected = now.date()
        else:
            expected = prev_trading_day
    except Exception as e:  # noqa: BLE001
        return Check(name, WARN, f"calendar math failed ({e}) — skipping")

    latest_d = datetime.strptime(latest, "%Y-%m-%d").date()
    if latest_d >= expected:
        return Check(name, OK, f"latest signal_date={latest} >= expected {expected.isoformat()}")
    return Check(name, WARN,
                 f"signals STALE: latest={latest} < expected last trading day {expected.isoformat()}")


# ── CHECK 8: TUNNEL UP (best-effort) (alert only) ────────────────────────────
def check_tunnel_up() -> Check:
    name = "tunnel_up"
    rc, out, err = _run_powershell(
        "$p = Get-Process -Name 'cloudflared' -ErrorAction SilentlyContinue; "
        "if ($p) { ($p | Measure-Object).Count } else { '0' }"
    )
    try:
        n = int((out or "0").strip().splitlines()[-1])
    except Exception:  # noqa: BLE001
        n = 0
    if n > 0:
        return Check(name, OK, f"cloudflared running ({n} process)")
    return Check(name, WARN, "cloudflared.exe not found — public tunnel may be down (api.kanida.ai)")


# ── Snapshot table ───────────────────────────────────────────────────────────
def _ensure_table_and_write(checks: List["Check"], ran_at: str) -> None:
    """Idempotent CREATE + INSERT one row per check into workflow_health in the
    PROD power DB (kanida_universe.db — same DB as falcon_auth_log/sessions)."""
    try:
        from power_user.config import POWER_DB_PATH  # noqa: WPS433
        import sqlite3
        con = sqlite3.connect(POWER_DB_PATH, timeout=15.0)
        try:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute(
                """CREATE TABLE IF NOT EXISTS workflow_health (
                    id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts       TEXT NOT NULL,      -- IST ISO when the run completed
                    "check"  TEXT NOT NULL,      -- check name (snake_case; reserved word, quoted)
                    status   TEXT NOT NULL,      -- OK | WARN | FAIL | ALERT_SENT
                    detail   TEXT,               -- 1-line observation
                    healed   INTEGER DEFAULT 0   -- 1 if this check self-healed this run
                )"""
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS ix_workflow_health_check_ts "
                "ON workflow_health(\"check\", ts)"
            )
            con.executemany(
                'INSERT INTO workflow_health (ts, "check", status, detail, healed) '
                "VALUES (?,?,?,?,?)",
                [(ran_at, c.name, c.status, c.detail, 1 if c.healed else 0) for c in checks],
            )
            con.commit()
        finally:
            con.close()
    except Exception as e:  # noqa: BLE001 — snapshot write must never crash the run
        log.warning("could not write workflow_health snapshot: %s", e)


# ── Alerting (de-duped per check per day) ────────────────────────────────────
def _already_alerted_today(check_name: str, day: str) -> bool:
    """Have we already pushed an alert for this (check, day)? We record alerts as
    rows in workflow_health with status 'ALERT_SENT' so the de-dupe is queryable
    and survives across fresh processes."""
    try:
        from power_user.config import POWER_DB_PATH  # noqa: WPS433
        import sqlite3
        con = sqlite3.connect(POWER_DB_PATH, timeout=10.0)
        try:
            row = con.execute(
                'SELECT 1 FROM workflow_health WHERE "check"=? AND status=? '
                "AND substr(ts,1,10)=? LIMIT 1",
                (check_name, "ALERT_SENT", day),
            ).fetchone()
            return bool(row)
        finally:
            con.close()
    except Exception:  # noqa: BLE001
        return False  # fail-open: better a duplicate alert than a missed one


def _record_alert_sent(check_name: str, ran_at: str, detail: str) -> None:
    try:
        from power_user.config import POWER_DB_PATH  # noqa: WPS433
        import sqlite3
        con = sqlite3.connect(POWER_DB_PATH, timeout=10.0)
        try:
            con.execute(
                'INSERT INTO workflow_health (ts, "check", status, detail, healed) '
                "VALUES (?,?,?,?,0)",
                (ran_at, check_name, "ALERT_SENT", detail[:300]),
            )
            con.commit()
        finally:
            con.close()
    except Exception:  # noqa: BLE001
        pass


def _fire_alerts(checks: List["Check"], ran_at: str, dry_run: bool) -> None:
    """Web Push for any FAIL, de-duped per check per day. Reuses the same
    web_push path auth_worker uses. Self-healed checks (status OK, healed=True)
    DO get an informational alert so the operator knows a heal happened."""
    day = ran_at[:10]
    to_alert = [c for c in checks if c.status == FAIL]
    healed = [c for c in checks if c.healed]
    if not to_alert and not healed:
        return

    if dry_run:
        for c in to_alert:
            log.info("[DRY-RUN] would ALERT (FAIL): %s — %s", c.name, c.detail)
        for c in healed:
            log.info("[DRY-RUN] would ALERT (SELF-HEALED): %s — %s", c.name, c.detail)
        return

    try:
        from power_user.services.web_push import notify_playwright_broken  # noqa: WPS433
    except Exception as e:  # noqa: BLE001
        log.warning("web_push unavailable (%s) — alerts not sent (snapshot still written)", e)
        return

    for c in to_alert:
        if _already_alerted_today(c.name, day):
            log.info("alert for %s already sent today — de-duped", c.name)
            continue
        try:
            # Reuse the generic Layer-2 push. We pass a workflow-specific class +
            # hint so the operator copy makes sense; the magic link routes to the
            # admin refresh page (the most common remediation).
            res = notify_playwright_broken(
                failure_class=f"WORKFLOW_{c.name.upper()}",
                hint=c.detail,
            )
            log.warning("ALERT fired for %s -> %s", c.name, res)
            _record_alert_sent(c.name, ran_at, c.detail)
        except Exception as e:  # noqa: BLE001
            log.warning("alert push failed for %s (non-fatal): %s", c.name, e)

    for c in healed:
        # Informational: a self-heal happened. De-duped same as FAILs.
        key = f"{c.name}_healed"
        if _already_alerted_today(key, day):
            continue
        try:
            res = notify_playwright_broken(
                failure_class=f"WORKFLOW_SELFHEAL_{c.name.upper()}",
                hint=f"Watchdog self-healed: {c.detail}",
            )
            log.warning("SELF-HEAL alert fired for %s -> %s", c.name, res)
            _record_alert_sent(key, ran_at, c.detail)
        except Exception as e:  # noqa: BLE001
            log.warning("self-heal alert push failed for %s (non-fatal): %s", c.name, e)


# ── Runner ───────────────────────────────────────────────────────────────────
def run(dry_run: bool = False, force: bool = False) -> int:
    now = _now_ist()
    log.info("workflow_watchdog START (pid=%s, fresh process, dry_run=%s, %s IST)",
             os.getpid(), dry_run, now.strftime("%a %H:%M"))

    if not force and not _in_active_window(now):
        log.info("outside active window (weekday 05:30-16:30 IST) — exiting without checks")
        return 0

    # Load .env once up front so kite/operator-token checks have what they need.
    try:
        from services.kite_auth import _load_env_file  # noqa: WPS433
        _load_env_file()
    except Exception as e:  # noqa: BLE001
        log.warning("could not load .env (%s) — kite-dependent checks may degrade", e)

    checks: List[Check] = [
        _safe("auth_task_present", lambda: check_auth_task_present(dry_run)),
        _safe("auth_ran_recently", check_auth_ran_recently),
        _safe("token_healthy", check_token_healthy),
        _safe("ip_allowed", check_ip_allowed),
        _safe("backend_up", check_backend_up),
        _safe("scheduled_sessions_armed", check_scheduled_sessions_armed),
        _safe("eod_signals_fresh", check_eod_signals_fresh),
        _safe("tunnel_up", check_tunnel_up),
    ]

    ran_at = _now_ist().isoformat()
    for c in checks:
        lvl = logging.ERROR if c.status == FAIL else (
            logging.WARNING if c.status == WARN else logging.INFO)
        log.log(lvl, "%s", c)

    # Always write the snapshot (queryable state) — even in dry-run, so the
    # endpoint reflects reality; dry-run only suppresses heals + real alerts.
    _ensure_table_and_write(checks, ran_at)

    _fire_alerts(checks, ran_at, dry_run)

    n_fail = sum(1 for c in checks if c.status == FAIL)
    n_warn = sum(1 for c in checks if c.status == WARN)
    n_heal = sum(1 for c in checks if c.healed)
    log.info("workflow_watchdog DONE: %s — fail=%d warn=%d ok=%d healed=%d",
             "ALL OK" if n_fail == 0 else "FAILURES PRESENT",
             n_fail, n_warn, len(checks) - n_fail - n_warn, n_heal)
    return 1 if n_fail else 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Kanida workflow health watchdog")
    ap.add_argument("--dry-run", action="store_true",
                    help="log what would be healed/alerted; make no changes")
    ap.add_argument("--force", action="store_true",
                    help="ignore the IST active-window gate")
    args = ap.parse_args()
    return run(dry_run=args.dry_run, force=args.force)


if __name__ == "__main__":
    sys.exit(main())
