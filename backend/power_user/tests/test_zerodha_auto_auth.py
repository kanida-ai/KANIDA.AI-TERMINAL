"""Tests for services.zerodha_auto_auth — Layer 1 Playwright bot.

Strategy:
  * Playwright is NEVER invoked. Every test injects a fake `driver` callable
    that either returns a fake request_token or raises the typed errors.
    The single point that hits Playwright in production (`_default_driver`)
    is deliberately not covered here — that's a manual smoke test against a
    throwaway Zerodha account during 5d.
  * `_exchange_request_token` is monkey-patched to skip the real KiteConnect
    call and the real DB write so we can test the success path too.
  * `log_attempt` + `today_already_succeeded` run against the real schema in
    a tmp SQLite — that's our actual production schema, no fixtures.
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
import tempfile

import pytest

# Make sure backend/ is importable when running from repo root
_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.normpath(os.path.join(_HERE, "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from services import zerodha_auto_auth as zaa     # noqa: E402
from power_user.db_init import init_power_user_schema    # noqa: E402


# ──────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────

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


@pytest.fixture
def env_creds(monkeypatch):
    """Set all 5 env vars to dummy values so _load_zerodha_creds() returns a dict."""
    monkeypatch.setenv("KITE_API_KEY",       "test_key")
    monkeypatch.setenv("KITE_API_SECRET",    "test_secret")
    monkeypatch.setenv("ZERODHA_USERNAME",   "test_user")
    monkeypatch.setenv("ZERODHA_PASSWORD",   "test_password")
    monkeypatch.setenv("ZERODHA_TOTP_SECRET", "JBSWY3DPEHPK3PXP")   # base32 dummy


# ──────────────────────────────────────────────────────────────────────────
# _load_zerodha_creds
# ──────────────────────────────────────────────────────────────────────────

def test_load_creds_returns_none_when_unset(monkeypatch):
    for k in ("KITE_API_KEY", "KITE_API_SECRET", "ZERODHA_USERNAME",
              "ZERODHA_PASSWORD", "ZERODHA_TOTP_SECRET"):
        monkeypatch.delenv(k, raising=False)
    assert zaa._load_zerodha_creds() is None


def test_load_creds_returns_none_when_partial(monkeypatch):
    monkeypatch.setenv("KITE_API_KEY", "x")
    monkeypatch.delenv("KITE_API_SECRET", raising=False)
    monkeypatch.setenv("ZERODHA_USERNAME", "u")
    monkeypatch.setenv("ZERODHA_PASSWORD", "p")
    monkeypatch.setenv("ZERODHA_TOTP_SECRET", "t")
    assert zaa._load_zerodha_creds() is None


def test_load_creds_strips_whitespace(monkeypatch):
    monkeypatch.setenv("KITE_API_KEY",       "  k  ")
    monkeypatch.setenv("KITE_API_SECRET",    "  s  ")
    monkeypatch.setenv("ZERODHA_USERNAME",   "  u  ")
    monkeypatch.setenv("ZERODHA_PASSWORD",   "  p  ")
    monkeypatch.setenv("ZERODHA_TOTP_SECRET", "  t  ")
    c = zaa._load_zerodha_creds()
    assert c == {"api_key": "k", "api_secret": "s", "username": "u",
                 "password": "p", "totp_secret": "t"}


# ──────────────────────────────────────────────────────────────────────────
# _redact_for_log — must scrub sensitive fields, must NOT bloat the column
# ──────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("dirty,leaked_value", [
    ("login failed: password=hunter2 wrong",   "hunter2"),
    ("invalid totp=123456",                     "123456"),
    ("pin=4321 rejected",                       "4321"),
    ("call to api_secret=abc123XYZ failed",     "abc123XYZ"),
])
def test_redact_scrubs_sensitive_fields(dirty, leaked_value):
    out = zaa._redact_for_log(dirty)
    assert "[REDACTED]" in out
    assert leaked_value not in out, f"{leaked_value!r} was NOT redacted in {out!r}"


def test_redact_truncates_huge_input():
    huge = "x" * 5_000
    out = zaa._redact_for_log(huge)
    assert len(out) <= 800


def test_redact_handles_empty_string():
    assert zaa._redact_for_log("") == ""


# ──────────────────────────────────────────────────────────────────────────
# run_auth_attempt — every error path the audit log must capture
# ──────────────────────────────────────────────────────────────────────────

def _async_run(coro):
    """Run an async coroutine in a fresh loop (safe for pytest)."""
    return asyncio.new_event_loop().run_until_complete(coro)


def test_attempt_fails_with_CONFIG_MISSING_when_no_env(monkeypatch):
    for k in ("KITE_API_KEY", "KITE_API_SECRET", "ZERODHA_USERNAME",
              "ZERODHA_PASSWORD", "ZERODHA_TOTP_SECRET"):
        monkeypatch.delenv(k, raising=False)
    result = _async_run(zaa.run_auth_attempt(attempt_of_day=1))
    assert result.status == "failed"
    assert result.error_code == "CONFIG_MISSING"
    assert result.token_preview is None
    assert "Missing" in result.error_detail


def test_attempt_success_writes_token_preview(env_creds, monkeypatch):
    """Driver returns a fake request_token; _exchange_request_token returns a
    fake access_token; result should be SUCCESS with the first 8 chars."""

    async def fake_driver(creds):
        return "fake_request_token_xyz"

    def fake_exchange(creds, rt):
        return "abcdef1234567890_access_token"

    monkeypatch.setattr(zaa, "_exchange_request_token", fake_exchange)

    result = _async_run(zaa.run_auth_attempt(
        attempt_of_day=1, trigger_kind="scheduled", driver=fake_driver
    ))
    assert result.status == "success"
    assert result.error_code is None
    assert result.token_preview == "abcdef12..."
    assert result.stage == "access_token"
    assert result.elapsed_ms >= 0


def test_attempt_BAD_CREDS_path(env_creds):
    async def fake_driver(creds):
        raise zaa.BadCredentialsError("invalid_username_or_password")
    result = _async_run(zaa.run_auth_attempt(driver=fake_driver))
    assert result.status == "failed"
    assert result.error_code == "BAD_CREDS"
    assert result.stage == "credentials"
    assert result.token_preview is None


def test_attempt_TOTP_FAILED_path(env_creds):
    async def fake_driver(creds):
        raise zaa.TotpFailedError("totp code rejected by server")
    result = _async_run(zaa.run_auth_attempt(driver=fake_driver))
    assert result.status == "failed"
    assert result.error_code == "TOTP_FAILED"


def test_attempt_REDIRECT_MALFORMED_path(env_creds):
    async def fake_driver(creds):
        raise zaa.RedirectMalformedError("no request_token in https://x.com/")
    result = _async_run(zaa.run_auth_attempt(driver=fake_driver))
    assert result.status == "failed"
    assert result.error_code == "REDIRECT_MALFORMED"


def test_attempt_BROWSER_CRASHED_path(env_creds):
    async def fake_driver(creds):
        raise zaa.BrowserCrashedError("Playwright: Target page crashed")
    result = _async_run(zaa.run_auth_attempt(driver=fake_driver))
    assert result.status == "failed"
    assert result.error_code == "BROWSER_CRASHED"


def test_attempt_TIMEOUT_path(env_creds):
    async def fake_driver(creds):
        raise asyncio.TimeoutError("page load timed out")
    result = _async_run(zaa.run_auth_attempt(driver=fake_driver))
    assert result.status == "failed"
    assert result.error_code == "TIMEOUT"


def test_attempt_KITE_API_ERROR_path(env_creds, monkeypatch):
    async def fake_driver(creds):
        return "valid_rt"

    def boom(creds, rt):
        raise zaa.KiteApiError("Token is invalid or has expired")

    monkeypatch.setattr(zaa, "_exchange_request_token", boom)
    result = _async_run(zaa.run_auth_attempt(driver=fake_driver))
    assert result.status == "failed"
    assert result.error_code == "KITE_API_ERROR"
    assert result.stage == "access_token"


def test_attempt_UNEXPECTED_path(env_creds):
    """Anything not an AutoAuthError subclass falls into UNEXPECTED so we don't
    silently swallow a new failure mode (the audit log still gets the row)."""
    async def fake_driver(creds):
        raise RuntimeError("something nobody anticipated")
    result = _async_run(zaa.run_auth_attempt(driver=fake_driver))
    assert result.status == "failed"
    assert result.error_code == "UNEXPECTED"
    assert "RuntimeError" in result.error_detail


def test_attempt_redacts_secrets_in_error_detail(env_creds):
    async def fake_driver(creds):
        # Simulate a driver that accidentally puts sensitive data in the message
        raise zaa.BadCredentialsError("submit failed with password=leakybadpwd")
    result = _async_run(zaa.run_auth_attempt(driver=fake_driver))
    assert "leakybadpwd" not in (result.error_detail or "")
    assert "[REDACTED]" in (result.error_detail or "")


# ──────────────────────────────────────────────────────────────────────────
# log_attempt + today_already_succeeded — DB audit trail
# ──────────────────────────────────────────────────────────────────────────

def test_log_attempt_inserts_row(tmp_db):
    result = zaa.AuthAttemptResult(
        status="success", stage="access_token",
        error_code=None, error_detail=None,
        token_preview="abcd1234...", elapsed_ms=512,
    )
    zaa.log_attempt(tmp_db, attempt_of_day=1, trigger_kind="scheduled", result=result)

    con = sqlite3.connect(tmp_db)
    rows = con.execute("SELECT status, attempt_of_day, trigger_kind, "
                       "       error_code, token_preview, elapsed_ms "
                       "  FROM falcon_auth_log").fetchall()
    con.close()
    assert len(rows) == 1
    assert rows[0] == ("success", 1, "scheduled", None, "abcd1234...", 512)


def test_log_attempt_swallows_db_error_silently():
    """Insert against a missing DB must not raise — auth attempts shouldn't
    crash because the log table got corrupted."""
    bogus = os.path.join(tempfile.gettempdir(), "definitely_not_a_db_xyz.db")
    if os.path.exists(bogus):
        os.unlink(bogus)
    result = zaa.AuthAttemptResult(
        status="failed", stage=None, error_code="X",
        error_detail="x", token_preview=None, elapsed_ms=0,
    )
    # Should not raise (function logs at WARNING and returns)
    zaa.log_attempt(bogus, 1, "scheduled", result)


def test_today_already_succeeded_true_after_success(tmp_db):
    ok = zaa.AuthAttemptResult(status="success", stage="access_token",
                                error_code=None, error_detail=None,
                                token_preview="a...", elapsed_ms=1)
    zaa.log_attempt(tmp_db, 1, "scheduled", ok)
    assert zaa.today_already_succeeded(tmp_db) is True


def test_today_already_succeeded_false_after_only_failures(tmp_db):
    fail = zaa.AuthAttemptResult(status="failed", stage="credentials",
                                  error_code="BAD_CREDS", error_detail="nope",
                                  token_preview=None, elapsed_ms=1)
    zaa.log_attempt(tmp_db, 1, "scheduled", fail)
    zaa.log_attempt(tmp_db, 2, "scheduled", fail)
    assert zaa.today_already_succeeded(tmp_db) is False


def test_today_already_succeeded_false_when_empty(tmp_db):
    assert zaa.today_already_succeeded(tmp_db) is False
