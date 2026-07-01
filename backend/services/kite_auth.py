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
import socket
import sqlite3
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

log = logging.getLogger("kanida.kite_auth")


# ── IPv4-only override for Kite egress ───────────────────────────────────────
# 2026-05-13: Windows IPv6 privacy extensions rotate the last 64 bits of the
# laptop's egress IPv6 every ~24h. If the Kite app's "Allowed IPs" allowlist
# contains a specific IPv6, it stops matching after rotation → every
# kite.place_order() rejects with "IP is not allowed to place orders".
#
# Fix: force getaddrinfo() to return ONLY IPv4 records for any kite.zerodha.com
# / kite.trade hostname. The IPv4 (Comcast in the operator's case) is stable
# for months; whitelist it once and never break again. Token auth is the real
# security boundary anyway — IP allowlist is optional on Kite Connect.
_KITE_HOST_SUFFIXES = ("zerodha.com", "kite.trade", "kiteconnect.com")
_orig_getaddrinfo = socket.getaddrinfo

def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    """For Kite hostnames, force AF_INET. For everything else, pass through.

    Signature must match socket.getaddrinfo exactly — both positional and keyword
    callers use this same arg layout (host, port, family, type, proto, flags)."""
    if isinstance(host, str) and any(host.endswith(s) for s in _KITE_HOST_SUFFIXES):
        return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
    return _orig_getaddrinfo(host, port, family, type, proto, flags)

if socket.getaddrinfo is not _ipv4_only_getaddrinfo:
    socket.getaddrinfo = _ipv4_only_getaddrinfo
    log.info("kite_auth: forcing IPv4-only DNS resolution for Kite endpoints")

_HERE         = Path(__file__).parent
_PROJECT_ROOT = _HERE.parent.parent
DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    str(_PROJECT_ROOT / "data" / "db" / "kanida_quant.db"),
)
# Make the path CWD-independent. If the env var (or default) is relative,
# anchor it at the project root — NOT the caller's current directory —
# because SQLite happily auto-creates missing DBs which silently produces
# orphan empty DBs in whatever subdirectory a one-off script was run from.
if not os.path.isabs(DB_PATH):
    DB_PATH = str(_PROJECT_ROOT / DB_PATH)

IST = timezone(timedelta(hours=5, minutes=30))


# ── Static-egress proxy for broker REST (SEBI registered-IP rule) ────────────
# 2026-06-25: SEBI now requires a registered STATIC IP for live ORDER APIs. The
# laptop server's egress IP is dynamic (Comcast), so we route the KiteConnect
# REST client through a static-IP proxy. kiteconnect 5.2.0 has native support:
# KiteConnect(..., proxies={...}) is stored as self.proxies and passed verbatim
# to reqsession.request(..., proxies=self.proxies) on EVERY REST call (orders,
# GTT, quotes) — verified in site-packages/kiteconnect/connect.py.
#
# ADDITIVE + DEFAULT-OFF: when BROKER_PROXY_URL is unset, _kite_proxies() returns
# None and NO proxies kwarg is passed → behaviour is byte-identical to before.
# Value format (set ONCE the static-IP proxy is up):
#     BROKER_PROXY_URL=http://user:pass@HOST:PORT
# The same URL is used for both http and https (broker REST is https; http kept
# for completeness / any redirect). The KiteTicker WebSocket (market data) stays
# DIRECT — the SEBI rule is for order APIs, and WS proxying is non-trivial.

def _kite_proxies() -> Optional[dict]:
    """Return a requests-style proxies dict if BROKER_PROXY_URL is set, else None.

    None → caller passes NO proxies kwarg → unchanged behaviour (direct egress)."""
    _load_env_file()
    url = os.environ.get("BROKER_PROXY_URL", "").strip()
    if not url:
        return None
    return {"http": url, "https": url}


def _apply_request_timeout(kite, timeout_sec: float = 10.0):
    """Set kiteconnect's per-request timeout.

    kiteconnect passes timeout=self.timeout explicitly to reqsession.request(),
    so the correct fix is to set self.timeout directly — NOT to wrap reqsession.
    Default kiteconnect timeout is 7s; we raise it to 10s for margin on slow links.
    All blocking calls (place_order, get_gtt, orders) must additionally run inside
    asyncio.to_thread so the event loop stays responsive during the HTTP wait.
    """
    kite.timeout = timeout_sec
    return kite


def _new_kite(api_key: str):
    """Construct a KiteConnect client, applying BROKER_PROXY_URL if set.

    Single construction helper so all sites share the default-off proxy logic."""
    from kiteconnect import KiteConnect

    proxies = _kite_proxies()
    if proxies is not None:
        kite = KiteConnect(api_key=api_key, proxies=proxies)
    else:
        kite = KiteConnect(api_key=api_key)
    return _apply_request_timeout(kite)


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
    api_key, _  = _get_credentials()
    access_token = get_access_token()

    kite = _new_kite(api_key)
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
        kite = _new_kite(api_key)
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
        kite    = _new_kite(api_key)
        session = kite.generate_session(request_token, api_secret=api_secret)
        token: str = session["access_token"]
    except Exception as e:
        raise KiteAuthError("KITE_AUTH_FAILED", str(e))

    # Save to DB (available to all processes immediately)
    _save_token_to_db(token, set_by="admin_api")

    # Also update in-process env so this process works immediately
    os.environ["KITE_ACCESS_TOKEN"] = token

    return token
