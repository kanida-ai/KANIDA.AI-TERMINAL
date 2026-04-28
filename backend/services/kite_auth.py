"""
Central Zerodha Kite authentication service.

Single source of truth for all Kite credentials.
Token is stored in the DB (kite_tokens table) so every process — API,
scheduler, ingestion scripts — reads the same refreshed token.

Usage everywhere:
    from services.kite_auth import get_kite_client, KiteAuthError
    kite = get_kite_client()          # raises KiteAuthError if token missing/expired
    kite = get_kite_client(check=False)  # skip profile validation (faster, for bulk ops)
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

log = logging.getLogger("kanida.kite_auth")

_HERE   = Path(__file__).parent
DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    str(_HERE.parent.parent / "data" / "db" / "kanida_quant.db"),
)

IST = timezone(timedelta(hours=5, minutes=30))

# ── Error types ───────────────────────────────────────────────────────────────

class KiteAuthError(Exception):
    """Raised when Kite credentials are missing or the token is invalid."""
    def __init__(self, code: str, detail: str = ""):
        self.code   = code     # TOKEN_MISSING | TOKEN_EXPIRED | KITE_AUTH_FAILED | CONFIG_MISSING
        self.detail = detail
        super().__init__(f"{code}: {detail}")


# ── DB helpers ────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def _ensure_table() -> None:
    with _conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS kite_tokens (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                access_token TEXT NOT NULL,
                token_date   TEXT NOT NULL,
                set_by       TEXT DEFAULT 'admin_api',
                created_at   TEXT DEFAULT (datetime('now'))
            )
        """)


def _save_token_to_db(access_token: str, set_by: str = "admin_api") -> None:
    _ensure_table()
    today = date.today().isoformat()
    with _conn() as conn:
        # Upsert: replace today's row if it exists
        conn.execute(
            "DELETE FROM kite_tokens WHERE token_date = ?", (today,)
        )
        conn.execute(
            "INSERT INTO kite_tokens (access_token, token_date, set_by) VALUES (?,?,?)",
            (access_token, today, set_by),
        )
    log.info("Token saved to DB for %s (set_by=%s)", today, set_by)


def _load_token_from_db() -> Optional[str]:
    """Return today's token from DB, or None if not present."""
    try:
        _ensure_table()
        today = date.today().isoformat()
        with _conn() as conn:
            row = conn.execute(
                "SELECT access_token FROM kite_tokens WHERE token_date = ? ORDER BY id DESC LIMIT 1",
                (today,),
            ).fetchone()
        return row["access_token"] if row else None
    except Exception:
        return None


def _get_credentials() -> tuple[str, str]:
    """Return (api_key, api_secret). Raises KiteAuthError if not configured."""
    _load_env_file()
    api_key    = os.environ.get("KITE_API_KEY", "")
    api_secret = os.environ.get("KITE_API_SECRET", "")
    if not api_key or not api_secret:
        raise KiteAuthError("CONFIG_MISSING", "KITE_API_KEY or KITE_API_SECRET not set")
    return api_key, api_secret


def _load_env_file() -> None:
    """Load config/.env once if env vars are not already set."""
    if os.environ.get("KITE_API_KEY"):
        return
    env_paths = [
        Path(DB_PATH).parent.parent.parent / "config" / ".env",
        Path(__file__).parent.parent.parent / "config" / ".env",
    ]
    for p in env_paths:
        if p.exists():
            for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and v:
                    os.environ[k] = v  # override so refreshed token in env always wins
            break


# ── Public API ────────────────────────────────────────────────────────────────

def get_access_token() -> str:
    """
    Return the current access token.
    Priority: DB (today's row) → KITE_ACCESS_TOKEN env var.
    Raises KiteAuthError(TOKEN_MISSING) if neither is available.
    """
    token = _load_token_from_db()
    if token:
        return token

    _load_env_file()
    token = os.environ.get("KITE_ACCESS_TOKEN", "")
    if token:
        return token

    raise KiteAuthError("TOKEN_MISSING", "No Kite access token found. Refresh at /admin.")


def get_kite_client(check: bool = False):
    """
    Return an authenticated KiteConnect instance.

    Args:
        check: if True, validates token with kite.profile() (slower, use for status checks)

    Raises:
        KiteAuthError: TOKEN_MISSING | TOKEN_EXPIRED | CONFIG_MISSING | KITE_AUTH_FAILED
    """
    from kiteconnect import KiteConnect

    api_key, _  = _get_credentials()
    access_token = get_access_token()

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    if check:
        try:
            kite.profile()
        except Exception as e:
            err = str(e)
            if "Invalid" in err or "expired" in err.lower() or "token" in err.lower():
                raise KiteAuthError("TOKEN_EXPIRED", err)
            raise KiteAuthError("KITE_AUTH_FAILED", err)

    return kite


def get_token_status() -> dict:
    """
    Check current token validity.
    Returns safe dict (no full token).
    """
    try:
        api_key, _ = _get_credentials()
    except KiteAuthError as e:
        return {"valid": False, "code": e.code, "reason": e.detail}

    try:
        token = get_access_token()
    except KiteAuthError as e:
        return {"valid": False, "code": e.code, "reason": e.detail}

    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(token)
        profile = kite.profile()
        return {
            "valid":         True,
            "user":          profile.get("user_name", ""),
            "email":         profile.get("email", ""),
            "token_preview": token[:8] + "...",
            "token_source":  "db" if _load_token_from_db() else "env",
            "token_date":    date.today().isoformat(),
        }
    except Exception as e:
        return {"valid": False, "code": "TOKEN_EXPIRED", "reason": str(e)}


def exchange_and_save(request_token: str) -> str:
    """
    Exchange a request_token for an access_token and persist it.
    Returns the new access_token.
    Raises KiteAuthError on failure.
    """
    api_key, api_secret = _get_credentials()

    try:
        from kiteconnect import KiteConnect
        kite    = KiteConnect(api_key=api_key)
        session = kite.generate_session(request_token, api_secret=api_secret)
        token: str = session["access_token"]
    except Exception as e:
        raise KiteAuthError("KITE_AUTH_FAILED", str(e))

    # Save to DB (available to all processes immediately)
    _save_token_to_db(token, set_by="admin_api")

    # Also update in-process env so this process works immediately
    os.environ["KITE_ACCESS_TOKEN"] = token

    return token
