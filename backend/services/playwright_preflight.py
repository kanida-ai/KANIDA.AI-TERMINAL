"""Playwright browser preflight — detect "browser unlaunchable" state EARLY.

Why this exists (incident 2026-05-29):
  21 consecutive auth_scheduler cycles burned 30 sec each (≈ 10 min wasted)
  trying to launch a Chromium browser that couldn't start. Each cycle hit
  the same misleading Playwright error:

      BrowserType.launch: Executable doesn't exist at <path>
      Looks like Playwright was just installed or updated.
      Please run: playwright install

  Forensics found the binary IS on disk (199 MB, correct revision, correct
  ACL) — but the running uvicorn process can't launch it. Cause is usually
  one of: env-var carryover from a conda-activated shell, DLL load conflict,
  PLAYWRIGHT_BROWSERS_PATH set to a wrong path, or a permissions edge case.

  Diagnosing it took 30 minutes of post-incident forensics. Worse, the
  admin panel only showed "UNEXPECTED" for the error code — no actionable
  signal. Worst of all, users saw stale picks for 24+ hours.

This module fixes all three problems:
  1. PRE-FLIGHT: at backend boot AND once per auth cycle, run a tiny
     chromium.launch() in a SUBPROCESS (so a hung/crashed launch doesn't
     poison the main event loop). If it fails:
       - Classify the failure (see PlaywrightHealth.failure_class)
       - Capture a structured diagnostic (env vars, paths, sys.executable,
         playwright.__file__, expected binary path + exists check)
       - Set a global BROKEN flag the scheduler reads
       - Trigger Web Push IMMEDIATELY (don't wait for 21 cycle failures)
  2. NO WASTED CYCLES: auth_scheduler reads is_broken() and refuses to
     schedule a Playwright attempt while broken. Health check itself still
     runs hourly (might self-heal — Windows AV unblock, disk reappear).
  3. ACTIONABLE DIAGNOSTIC: the full env dump goes to falcon_auth_log AND
     is surfaced to /power/admin via auth_status.browser_health.

Subprocess isolation rationale:
  Playwright launches a Node driver subprocess + a Chromium subprocess.
  If either hangs (we've seen 30-sec timeouts), running the test IN-PROCESS
  blocks uvicorn's event loop and breaks the rest of FastAPI. Using
  subprocess.run with a 20-sec hard timeout contains the blast radius.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger("kanida.playwright_preflight")
IST = timezone(timedelta(hours=5, minutes=30))

# Hard cap on the subprocess launch test. Healthy local launches finish in
# 1.5–3 sec; a hang past this is itself a failure signal.
_LAUNCH_TIMEOUT_S = 20


# ──────────────────────────────────────────────────────────────────────────
# Health state — global singleton consulted by auth_scheduler before each
# cycle. Updated by check_now(). Thread-safe via _state_lock.
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class PlaywrightHealth:
    """Snapshot of the last preflight result."""
    checked_at:        str    = ""        # ISO IST
    is_healthy:        bool   = False
    failure_class:     str    = "UNKNOWN" # see classify_failure()
    error_summary:     str    = ""        # short human-readable
    expected_binary:   str    = ""        # what Playwright thinks the path is
    binary_exists:     Optional[bool] = None
    env:               Dict[str, Optional[str]] = field(default_factory=dict)
    elapsed_ms:        int    = 0
    next_recheck_at:   str    = ""        # ISO IST

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


_state: PlaywrightHealth = PlaywrightHealth(
    checked_at="",
    is_healthy=False,        # PESSIMISTIC default — assume broken until proven healthy
    failure_class="UNCHECKED",
    error_summary="No preflight has run yet",
)
_state_lock = threading.Lock()


def get_health() -> PlaywrightHealth:
    """Latest cached health snapshot. Thread-safe."""
    with _state_lock:
        return PlaywrightHealth(**asdict(_state))


def is_broken() -> bool:
    """Convenience: True iff the last preflight said unhealthy.
    Pre-first-check this returns True (pessimistic) so the scheduler waits
    for the boot preflight before trying Playwright."""
    with _state_lock:
        return not _state.is_healthy


# ──────────────────────────────────────────────────────────────────────────
# Failure classification — actionable strings, not stack traces
# ──────────────────────────────────────────────────────────────────────────

def classify_failure(stderr: str, stdout: str) -> str:
    """Map raw Playwright error text to one of:
      BROWSER_NOT_INSTALLED  — Playwright says executable doesn't exist
                               (which covers BOTH "actually missing" AND
                               the misleading DLL-load-failure case)
      VERSION_MISMATCH       — installed Playwright expects a different
                               Chromium revision than what's on disk
      PERMISSION_DENIED      — file ACL blocks read/exec
      TIMEOUT                — subprocess exceeded _LAUNCH_TIMEOUT_S
      IMPORT_FAILED          — couldn't even import playwright (pip issue)
      UNKNOWN                — none of the above
    """
    blob = (stderr + "\n" + stdout).lower()
    if "executable doesn't exist" in blob or "playwright install" in blob:
        return "BROWSER_NOT_INSTALLED"
    if "no module named 'playwright'" in blob or "modulenotfounderror" in blob:
        return "IMPORT_FAILED"
    if "permission denied" in blob or "access is denied" in blob:
        return "PERMISSION_DENIED"
    if "_launch_timeout_marker" in blob:
        return "TIMEOUT"
    if "unsupported" in blob and "version" in blob:
        return "VERSION_MISMATCH"
    return "UNKNOWN"


def remediation_hint(failure_class: str) -> str:
    """One-sentence guide to surface in the admin panel."""
    return {
        "BROWSER_NOT_INSTALLED": (
            "Run `scripts\\repair_playwright.bat` from a fresh cmd prompt "
            "(NOT from an Anaconda Prompt or VS Code terminal — those pollute "
            "the DLL search path)."
        ),
        "VERSION_MISMATCH": (
            "Playwright python version changed but browsers weren't redownloaded. "
            "Run `scripts\\repair_playwright.bat`."
        ),
        "PERMISSION_DENIED": (
            "Antivirus or Windows Defender is blocking the Chromium binary. "
            "Whitelist C:\\Users\\SPS\\AppData\\Local\\ms-playwright."
        ),
        "TIMEOUT": (
            "Chromium subprocess hung. Check for stale chrome-headless-shell.exe "
            "processes (Task Manager) and kill them."
        ),
        "IMPORT_FAILED": (
            "Playwright python package missing or broken. Run "
            "`C:\\Users\\SPS\\anaconda3\\Scripts\\pip install playwright==1.59.0`."
        ),
        "UNKNOWN": (
            "Manual recovery: refresh the Kite token via the admin panel's "
            "'Refresh token now' button — it triggers the same Playwright bot, "
            "but if THAT button works the env is fine, just retry the auto-cycle."
        ),
        "UNCHECKED": "Boot preflight hasn't run yet.",
    }.get(failure_class, "")


# ──────────────────────────────────────────────────────────────────────────
# The actual check — runs Playwright launch in a subprocess
# ──────────────────────────────────────────────────────────────────────────

_LAUNCH_PROBE = r"""
import asyncio, json, os, sys, traceback
async def go():
    try:
        from playwright.async_api import async_playwright
    except Exception as e:
        print('IMPORT_FAILED:', repr(e), file=sys.stderr)
        sys.exit(2)
    async with async_playwright() as p:
        try:
            b = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage'],
                timeout=15000,
            )
            await b.close()
            print(json.dumps({'ok': True}))
        except Exception as e:
            print(json.dumps({'ok': False, 'err': repr(e)[:500]}))
            sys.exit(3)
asyncio.run(go())
"""


def _collect_env_diag() -> Dict[str, Optional[str]]:
    """Capture every env var likely to cause Playwright weirdness.
    These get logged so the next incident takes 2 min, not 30."""
    keys = [
        "PLAYWRIGHT_BROWSERS_PATH",
        "PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD",
        "PLAYWRIGHT_BROWSERS_DOWNLOAD_HOST",
        "USERPROFILE",
        "APPDATA",
        "LOCALAPPDATA",
        "CONDA_PREFIX",
        "CONDA_DEFAULT_ENV",
        "VIRTUAL_ENV",
        "TEMP",
    ]
    diag: Dict[str, Optional[str]] = {k: os.environ.get(k) for k in keys}
    # PATH is too long to dump in full — just record whether it contains the
    # canonical conda mingw-w64 directory (root cause of the 2026-05-24 issue).
    path = os.environ.get("PATH", "")
    diag["__PATH_HAS_CONDA_MINGW"] = (
        "yes" if "anaconda3\\Library\\mingw-w64\\bin" in path.lower().replace("/", "\\")
        else "no"
    )
    diag["__SYS_EXECUTABLE"] = sys.executable
    # Where is the playwright PACKAGE coming from? If two installs exist
    # this is the smoking gun.
    try:
        import playwright   # noqa: WPS433
        diag["__PLAYWRIGHT_FILE"] = playwright.__file__
    except Exception as e:
        diag["__PLAYWRIGHT_FILE"] = f"IMPORT_ERR: {e}"
    return diag


def _expected_binary_path() -> str:
    """The path Playwright 1.59 expects for chrome-headless-shell on Windows.
    Used for the binary_exists check in the diagnostic. Hardcoded because
    importing playwright._impl pulls in async machinery we don't want here."""
    base = os.environ.get(
        "PLAYWRIGHT_BROWSERS_PATH",
        os.path.join(os.environ.get("LOCALAPPDATA", ""), "ms-playwright"),
    )
    # Revision 1217 matches Playwright 1.59.x. If we bump Playwright, update
    # this AND scripts/repair_playwright.bat AND requirements.txt together.
    return os.path.join(
        base,
        "chromium_headless_shell-1217",
        "chrome-headless-shell-win64",
        "chrome-headless-shell.exe",
    )


def check_now(*, fire_push_on_break: bool = True) -> PlaywrightHealth:
    """Run the preflight test, update global state, optionally fire Web Push
    on a state transition healthy→broken. Returns the new health snapshot.

    This is safe to call from any thread (FastAPI startup hook, scheduler
    loop, request handler). The subprocess timeout caps blocking time at
    _LAUNCH_TIMEOUT_S + small overhead.
    """
    t0 = time.time()
    now = datetime.now(IST)
    env_diag = _collect_env_diag()
    expected = _expected_binary_path()
    exists = os.path.exists(expected) if expected else None

    try:
        proc = subprocess.run(
            [sys.executable, "-c", _LAUNCH_PROBE],
            capture_output=True,
            text=True,
            timeout=_LAUNCH_TIMEOUT_S,
            # Inherit env from current process — that's what auth bot sees.
        )
        elapsed_ms = int((time.time() - t0) * 1000)
        if proc.returncode == 0 and '"ok": true' in (proc.stdout or "").lower():
            new_health = PlaywrightHealth(
                checked_at=now.isoformat(),
                is_healthy=True,
                failure_class="NONE",
                error_summary="",
                expected_binary=expected,
                binary_exists=exists,
                env=env_diag,
                elapsed_ms=elapsed_ms,
                next_recheck_at=(now + timedelta(hours=1)).isoformat(),
            )
        else:
            cls = classify_failure(proc.stderr or "", proc.stdout or "")
            new_health = PlaywrightHealth(
                checked_at=now.isoformat(),
                is_healthy=False,
                failure_class=cls,
                error_summary=(proc.stderr or proc.stdout or "")[:400],
                expected_binary=expected,
                binary_exists=exists,
                env=env_diag,
                elapsed_ms=elapsed_ms,
                # Re-check sooner if broken — maybe the user runs repair.bat.
                next_recheck_at=(now + timedelta(minutes=30)).isoformat(),
            )
    except subprocess.TimeoutExpired:
        elapsed_ms = int((time.time() - t0) * 1000)
        new_health = PlaywrightHealth(
            checked_at=now.isoformat(),
            is_healthy=False,
            failure_class="TIMEOUT",
            error_summary=f"Probe subprocess exceeded {_LAUNCH_TIMEOUT_S}s — "
                          f"Chromium likely hanging at launch.",
            expected_binary=expected,
            binary_exists=exists,
            env=env_diag,
            elapsed_ms=elapsed_ms,
            next_recheck_at=(now + timedelta(minutes=30)).isoformat(),
        )

    # Atomic state swap + detect transition
    with _state_lock:
        was_healthy = _state.is_healthy
        globals()["_state"] = new_health
        transitioned_to_broken = was_healthy and not new_health.is_healthy
        first_check_broken = (_state.checked_at == new_health.checked_at
                                 and not new_health.is_healthy
                                 and was_healthy is False)

    log.info(
        "playwright_preflight: healthy=%s class=%s elapsed=%dms",
        new_health.is_healthy, new_health.failure_class, new_health.elapsed_ms,
    )
    if not new_health.is_healthy:
        log.warning(
            "playwright_preflight: BROKEN — class=%s binary_exists=%s expected=%s\n"
            "                       env=%s\n"
            "                       err=%s",
            new_health.failure_class, new_health.binary_exists,
            new_health.expected_binary, json.dumps(new_health.env),
            new_health.error_summary[:200],
        )

    if fire_push_on_break and not new_health.is_healthy and (transitioned_to_broken or first_check_broken):
        _try_fire_push(new_health)

    return new_health


def _try_fire_push(health: PlaywrightHealth) -> None:
    """Best-effort Web Push fire on healthy→broken transition. Never raises."""
    try:
        from power_user.services.web_push import notify_playwright_broken
        notify_playwright_broken(
            failure_class=health.failure_class,
            hint=remediation_hint(health.failure_class),
        )
    except Exception as e:
        log.warning("playwright_preflight: push notification failed (non-fatal): %s", e)
