"""Zerodha automated daily authentication — Layer 1 of the 3-layer system.

Production target: 99% of trading days the daily Zerodha access_token is
refreshed without operator intervention.

How Zerodha auth works (one-shot):
  1. GET  https://kite.zerodha.com/connect/login?api_key=<KEY>&v=3
  2. Submit username + password → server issues 2FA challenge
  3. Submit TOTP (6-digit, from pyotp.TOTP(SECRET).now())
  4. Server redirects to <redirect_uri>?request_token=<RT>&action=login
  5. POST to KiteConnect.generate_session(request_token, api_secret)
     → returns {access_token, ...}
  6. Write access_token + today's date to kanida_quant.db.kite_tokens

This module drives steps 1-5 via Playwright (headless Chromium) and step 6
via services.kite_auth helpers we already trust.

Security:
  - Credentials read from env vars only (ZERODHA_USERNAME, ZERODHA_PASSWORD,
    ZERODHA_TOTP_SECRET). Never stored in DB, never logged.
  - Token preview in falcon_auth_log is only the first 8 chars (audit trail,
    not a leak vector).
  - Error_detail strings are redacted by `_redact_for_log` before insertion.
  - Browser context is ephemeral — no session persistence across runs.

Failure modes (all auditable in falcon_auth_log):
  BAD_CREDS         → username/password rejected
  TOTP_FAILED       → 2FA code rejected (rare — pyotp clock drift)
  TIMEOUT           → page load / element wait exceeded 30s
  KITE_API_ERROR    → KiteConnect.generate_session raised
  BROWSER_CRASHED   → Playwright crashed (resource issue)
  REDIRECT_MALFORMED→ couldn't parse request_token from redirect
  CONFIG_MISSING    → env var absent (operator forgot to set)
  REQUEST_BLOCKED   → Zerodha returned an unexpected page (captcha, IP block)
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import sqlite3
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Callable, Dict, Optional
from urllib.parse import parse_qs, urlparse

log = logging.getLogger("kanida.zerodha_auto_auth")
IST = timezone(timedelta(hours=5, minutes=30))

LOGIN_URL_TEMPLATE = "https://kite.zerodha.com/connect/login?api_key={api_key}&v=3"
LOGIN_TIMEOUT_MS   = 30_000
TOTP_TIMEOUT_MS    = 15_000
REDIRECT_TIMEOUT_MS = 30_000


# ──────────────────────────────────────────────────────────────────────────
# Result types
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class AuthAttemptResult:
    """One attempt outcome. All fields are JSON-serialisable for falcon_auth_log."""
    status:         str        # success | failed | skipped | partial
    stage:          Optional[str]   # login_page | credentials | totp | request_token | access_token | None
    error_code:     Optional[str]
    error_detail:   Optional[str]
    token_preview:  Optional[str]
    elapsed_ms:     int


# ──────────────────────────────────────────────────────────────────────────
# Env-var ingest with strict validation
# ──────────────────────────────────────────────────────────────────────────

def _load_zerodha_creds() -> Optional[Dict[str, str]]:
    """Return creds dict or None if config incomplete. Never raises."""
    api_key      = os.environ.get("KITE_API_KEY", "").strip()
    api_secret   = os.environ.get("KITE_API_SECRET", "").strip()
    username     = os.environ.get("ZERODHA_USERNAME", "").strip()
    password     = os.environ.get("ZERODHA_PASSWORD", "").strip()
    totp_secret  = os.environ.get("ZERODHA_TOTP_SECRET", "").strip()
    if not all([api_key, api_secret, username, password, totp_secret]):
        return None
    return {
        "api_key":     api_key,
        "api_secret":  api_secret,
        "username":    username,
        "password":    password,
        "totp_secret": totp_secret,
    }


_REDACT_PATTERNS = [
    re.compile(r"password=[^&\s]*", re.I),
    re.compile(r"totp=\d+", re.I),
    re.compile(r"pin=\d+", re.I),
    re.compile(r"api_secret=[a-zA-Z0-9]+", re.I),
]


def _redact_for_log(text: str) -> str:
    """Strip anything sensitive from error messages before they hit the DB."""
    if not text:
        return text
    redacted = text
    for pat in _REDACT_PATTERNS:
        redacted = pat.sub("[REDACTED]", redacted)
    # Truncate to bounded length so a giant stack trace doesn't bloat the table
    return redacted[:800]


# ──────────────────────────────────────────────────────────────────────────
# Playwright driver — async because Playwright's sync API can't run inside
# an async FastAPI event loop. Scheduler will call via asyncio.run().
# ──────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def _open_browser(headless: bool = True) -> AsyncIterator[Any]:
    """Yield a Playwright BrowserContext. Caller closes via 'async with'."""
    from playwright.async_api import async_playwright    # noqa: WPS433
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless,
                                            args=["--no-sandbox", "--disable-dev-shm-usage"])
        try:
            context = await browser.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/120.0.0.0 Safari/537.36"),
                viewport={"width": 1280, "height": 800},
            )
            yield context
        finally:
            await browser.close()


async def _capture_request_token(context: Any, login_url: str,
                                  creds: Dict[str, str]) -> str:
    """Drive the Zerodha login form via Playwright and return request_token.

    Raises:
        BadCredentialsError  — username/password rejected
        TotpFailedError      — TOTP rejected
        TimeoutError         — page slow or stuck
        RedirectMalformedError — request_token not found in redirect URL
    """
    import pyotp    # noqa: WPS433
    page = await context.new_page()

    # Intercept the post-auth redirect — that's where ?request_token=... lands.
    # We listen for BOTH `request` and `response` events because the configured
    # Kite Connect redirect_uri may be unreachable from the headless browser
    # (e.g. http://localhost:3000/admin?... — only the operator's dev frontend
    # would normally answer that). In that case the browser blows up with
    # `chrome-error://chromewebdata/` and `response` never fires, but the
    # `request` event captures the URL BEFORE the navigation is attempted,
    # which is all we need — the request_token is already in the URL.
    redirect_event = asyncio.Event()
    captured_url:   Dict[str, str] = {}

    def _on_response(resp):
        url = resp.url
        if "request_token=" in url and "url" not in captured_url:
            captured_url["url"] = url
            redirect_event.set()
    def _on_request(req):
        url = req.url
        if "request_token=" in url and "url" not in captured_url:
            captured_url["url"] = url
            redirect_event.set()
    page.on("response", _on_response)
    page.on("request", _on_request)

    await page.goto(login_url, wait_until="domcontentloaded", timeout=LOGIN_TIMEOUT_MS)

    # ── Step 1: username + password ────────────────────────────────────
    await page.wait_for_selector('input[type="text"]', timeout=LOGIN_TIMEOUT_MS)
    await page.fill('input[type="text"]', creds["username"])
    await page.fill('input[type="password"]', creds["password"])
    await page.click('button[type="submit"]')

    # Wait for either: TOTP field appears, OR an error toast (bad creds).
    try:
        await page.wait_for_selector(
            'input[type="number"], input[label="External TOTP"], '
            '.error-msg, .text-danger, [class*="error"]',
            timeout=TOTP_TIMEOUT_MS,
        )
    except Exception as e:
        raise TimeoutError(f"username/password step stalled: {e}")

    # Did we land on an error? Some Zerodha responses show a toast without changing URL.
    err = await page.query_selector('.error-msg, .text-danger, [class*="error-msg"]')
    if err is not None:
        text = (await err.inner_text() or "").strip()
        if text:
            raise BadCredentialsError(text[:200])

    # ── Step 2: TOTP ──────────────────────────────────────────────────
    totp = pyotp.TOTP(creds["totp_secret"]).now()
    # Zerodha's TOTP input is typically input[type="number"] or label "External TOTP"
    totp_input = await page.query_selector('input[type="number"]')
    if totp_input is None:
        totp_input = await page.query_selector('input[label="External TOTP"]')
    if totp_input is None:
        raise TimeoutError("TOTP input not found on page")
    await totp_input.fill(totp)
    # Zerodha sometimes auto-submits on 6-digit completion; if not, click submit
    submit = await page.query_selector('button[type="submit"]')
    if submit is not None:
        try:
            await submit.click(timeout=2_000)
        except Exception:
            pass  # auto-submit already fired

    # ── Step 3: wait for redirect with request_token ──────────────────
    try:
        await asyncio.wait_for(redirect_event.wait(), timeout=REDIRECT_TIMEOUT_MS / 1000)
    except asyncio.TimeoutError:
        # Could be TOTP failed (would show error) or page didn't redirect
        err = await page.query_selector('.error-msg, .text-danger')
        if err is not None:
            text = (await err.inner_text() or "").strip()
            if text and ("totp" in text.lower() or "code" in text.lower()):
                raise TotpFailedError(text[:200])
        raise TimeoutError("redirect with request_token never arrived")

    captured = captured_url.get("url", "")
    parsed = urlparse(captured)
    qs = parse_qs(parsed.query)
    rt = qs.get("request_token", [None])[0]
    if not rt:
        raise RedirectMalformedError(f"no request_token in {captured[:120]}")
    return rt


# ──────────────────────────────────────────────────────────────────────────
# Domain errors — typed so the audit log gets accurate error_code values
# ──────────────────────────────────────────────────────────────────────────

class AutoAuthError(Exception):
    code = "UNKNOWN"

class BadCredentialsError(AutoAuthError):
    code = "BAD_CREDS"

class TotpFailedError(AutoAuthError):
    code = "TOTP_FAILED"

class RedirectMalformedError(AutoAuthError):
    code = "REDIRECT_MALFORMED"

class KiteApiError(AutoAuthError):
    code = "KITE_API_ERROR"

class BrowserCrashedError(AutoAuthError):
    code = "BROWSER_CRASHED"


# ──────────────────────────────────────────────────────────────────────────
# Public entry point — async, called by the scheduler
# ──────────────────────────────────────────────────────────────────────────

# Pluggable browser driver: tests inject a fake. In prod, this is
# _capture_request_token + _open_browser.
BrowserDriver = Callable[[Dict[str, str]], "asyncio.Future[str]"]


async def _default_driver(creds: Dict[str, str]) -> str:
    """Real Playwright driver. Returns request_token or raises."""
    login_url = LOGIN_URL_TEMPLATE.format(api_key=creds["api_key"])
    async with _open_browser(headless=True) as ctx:
        try:
            return await _capture_request_token(ctx, login_url, creds)
        except (BadCredentialsError, TotpFailedError, RedirectMalformedError):
            raise
        except Exception as e:
            # Anything else from Playwright → BrowserCrashed
            raise BrowserCrashedError(str(e))


async def run_auth_attempt(
    *,
    attempt_of_day: int = 1,
    trigger_kind:   str = "scheduled",
    driver:         Optional[Callable[[Dict[str, str]], Any]] = None,
) -> AuthAttemptResult:
    """One full auth attempt. Drives the bot, exchanges the request_token,
    writes the new access_token, returns the structured outcome.

    Args:
        attempt_of_day — 1..4 for scheduled, 0 for manual/magic
        trigger_kind   — 'scheduled' | 'manual' | 'magic_link'
        driver         — alternative request_token producer (tests inject)
    """
    t0 = time.time()
    creds = _load_zerodha_creds()
    if creds is None:
        return AuthAttemptResult(
            status="failed", stage=None,
            error_code="CONFIG_MISSING",
            error_detail=("Missing one or more env vars: KITE_API_KEY, KITE_API_SECRET, "
                          "ZERODHA_USERNAME, ZERODHA_PASSWORD, ZERODHA_TOTP_SECRET."),
            token_preview=None,
            elapsed_ms=int((time.time() - t0) * 1000),
        )

    drive = driver or _default_driver
    stage = "login_page"
    try:
        # ── Stages 1-3 (login + TOTP + capture) ──
        stage = "credentials"
        request_token = await drive(creds)
        stage = "request_token"

        # ── Stage 4: exchange request_token → access_token ──
        stage = "access_token"
        access_token = await asyncio.to_thread(
            _exchange_request_token, creds, request_token
        )

        return AuthAttemptResult(
            status="success", stage="access_token",
            error_code=None, error_detail=None,
            token_preview=(access_token[:8] + "..."),
            elapsed_ms=int((time.time() - t0) * 1000),
        )
    except AutoAuthError as e:
        return AuthAttemptResult(
            status="failed", stage=stage,
            error_code=e.code,
            error_detail=_redact_for_log(str(e)),
            token_preview=None,
            elapsed_ms=int((time.time() - t0) * 1000),
        )
    except asyncio.TimeoutError as e:
        return AuthAttemptResult(
            status="failed", stage=stage,
            error_code="TIMEOUT",
            error_detail=_redact_for_log(str(e)),
            token_preview=None,
            elapsed_ms=int((time.time() - t0) * 1000),
        )
    except Exception as e:
        # 2026-05-29 fix: classify the "Playwright can't launch browser"
        # pattern as BROWSER_LAUNCH_FAILED instead of the catch-all UNEXPECTED.
        # The auth_scheduler keys off this code to skip future cycles and
        # trigger an immediate Web Push (instead of running 21 doomed cycles
        # and pushing only after the last one). Pattern matches both the
        # genuinely-missing-binary case AND the misleading "doesn't exist"
        # error that Playwright shows on DLL-load failures.
        err_str = str(e)
        is_browser_launch_failure = (
            "BrowserType.launch" in err_str and "doesn't exist" in err_str
        ) or "Looks like Playwright was just installed" in err_str
        if is_browser_launch_failure:
            log.warning(
                "zerodha_auto_auth: BROWSER_LAUNCH_FAILED at stage=%s — "
                "Playwright can't start Chromium. Diagnostics in playwright_"
                "preflight.get_health(). Skipping further auto-cycles until "
                "preflight self-heals or operator runs scripts\\repair_"
                "playwright.bat.", stage,
            )
            # Trigger an immediate preflight + push (best-effort, non-fatal).
            try:
                from services.playwright_preflight import check_now    # noqa: WPS433
                await asyncio.to_thread(check_now, fire_push_on_break=True)
            except Exception as _e:
                log.warning("zerodha_auto_auth: preflight refresh failed: %s", _e)
            return AuthAttemptResult(
                status="failed", stage=stage,
                error_code="BROWSER_LAUNCH_FAILED",
                error_detail=_redact_for_log(f"{type(e).__name__}: {err_str}"),
                token_preview=None,
                elapsed_ms=int((time.time() - t0) * 1000),
            )
        log.exception("zerodha_auto_auth: unexpected failure at stage=%s", stage)
        return AuthAttemptResult(
            status="failed", stage=stage,
            error_code="UNEXPECTED",
            error_detail=_redact_for_log(f"{type(e).__name__}: {e}"),
            token_preview=None,
            elapsed_ms=int((time.time() - t0) * 1000),
        )


def _exchange_request_token(creds: Dict[str, str], request_token: str) -> str:
    """KiteConnect.generate_session — sync, run on thread from the async caller."""
    try:
        from kiteconnect import KiteConnect    # noqa: WPS433
        from services.kite_auth import _save_token_to_db    # noqa: WPS433
    except Exception as e:
        raise KiteApiError(f"import_failed: {e}")

    kite = KiteConnect(api_key=creds["api_key"])
    try:
        data = kite.generate_session(request_token, api_secret=creds["api_secret"])
    except Exception as e:
        raise KiteApiError(_redact_for_log(str(e)))

    access_token = data.get("access_token", "")
    if not access_token:
        raise KiteApiError("generate_session returned empty access_token")

    # Persist via the existing kite_auth helper (same write path
    # /api/admin/kite/refresh-token uses for manual operator refreshes).
    try:
        _save_token_to_db(access_token, set_by="auto_auth_bot")
    except Exception as e:
        raise KiteApiError(f"db_write_failed: {e}")
    return access_token


# ──────────────────────────────────────────────────────────────────────────
# Audit log helpers
# ──────────────────────────────────────────────────────────────────────────

def log_attempt(db_path: str, attempt_of_day: int, trigger_kind: str,
                  result: AuthAttemptResult) -> None:
    """Insert one row into falcon_auth_log. Never raises."""
    try:
        con = sqlite3.connect(db_path, timeout=10.0, check_same_thread=False)
        try:
            con.execute("""
                INSERT INTO falcon_auth_log
                  (attempt_at, attempt_of_day, trigger_kind, status, elapsed_ms,
                   stage, error_code, error_detail, token_preview)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now(IST).isoformat(),
                attempt_of_day, trigger_kind,
                result.status, result.elapsed_ms,
                result.stage, result.error_code, result.error_detail,
                result.token_preview,
            ))
            con.commit()
        finally:
            con.close()
    except sqlite3.Error as e:
        log.warning("falcon_auth_log: insert failed (non-fatal): %s", e)


def today_already_succeeded(db_path: str) -> bool:
    """True if a successful attempt has been logged in the CURRENT Kite-token
    lifecycle window. Used to skip later attempts in the 4-cycle morning
    sequence.

    Bug #2 fix (2026-05-25): the window boundary must be 06:00 IST, NOT
    midnight. Kite invalidates ALL access tokens at 06:00 IST daily, so any
    success BEFORE 06:00 IST is already expired by the time the morning
    sequence (06:30 / 07:30 / 08:30 / 09:00 IST) fires. Previously a 04:55
    IST manual refresh caused all 4 scheduled attempts to skip with
    ALREADY_OK, leaving the system tokenless from 06:00 IST onward — which
    is exactly what happened on 2026-05-25 and silently broke the V7
    pipeline for the day (Monday EOD signals never generated).

    Window definition:
      - If now >= 06:00 IST today → window_start = today 06:00 IST
      - If now <  06:00 IST today → window_start = yesterday 06:00 IST
    window_end is always +24h (the next 06:00 IST cutoff).

    Implementation note: we compare ISO timestamp strings (`attempt_at` is
    stored as `datetime.now(IST).isoformat()`). SQLite's `date()` function
    interprets these as UTC when the string carries a ±HH:MM offset, which
    broke an earlier version of this check. Direct ISO string comparison
    with < / >= sidesteps that.
    """
    now_ist      = datetime.now(IST)
    six_am_today = now_ist.replace(hour=6, minute=0, second=0, microsecond=0)
    if now_ist >= six_am_today:
        window_start = six_am_today
    else:
        window_start = six_am_today - timedelta(days=1)
    window_end = window_start + timedelta(days=1)
    # Variable names kept compatible with the SQL below.
    day_start, day_end = window_start, window_end
    try:
        con = sqlite3.connect(db_path, timeout=5.0, check_same_thread=False)
        try:
            row = con.execute("""
                SELECT 1 FROM falcon_auth_log
                 WHERE status = 'success'
                   AND attempt_at >= ?
                   AND attempt_at <  ?
                 LIMIT 1
            """, (day_start.isoformat(), day_end.isoformat())).fetchone()
            return row is not None
        finally:
            con.close()
    except sqlite3.Error:
        return False    # fail open — if we can't read, run the attempt
