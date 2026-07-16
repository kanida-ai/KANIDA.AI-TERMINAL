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

import json
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


# ── PER-ACCOUNT static egress (SEBI one-IP-per-broker-account) ───────────────
# 2026-07-16: onboarding a SECOND power user. SEBI's registered-IP rule is
# enforced PER BROKER ACCOUNT — Zerodha allows exactly ONE static IP per Kite
# Connect account. The global BROKER_PROXY_URL above reroutes EVERY account
# through ONE proxy, which would break the operator's own account (which must
# keep egressing DIRECT from the home IP 174.61.231.194). So we add an OPTIONAL
# per-broker_account override: BROKER_PROXY_MAP is a JSON object mapping
# broker_account_id -> proxy URL. An account IN the map gets its dedicated Kite
# client routed through THAT account's proxy; accounts NOT in the map fall back
# to the global BROKER_PROXY_URL (normally unset → direct, unchanged).
#
# ADDITIVE + DEFAULT-OFF: BROKER_PROXY_MAP unset (or malformed) → every lookup
# returns None → callers fall back to global/direct → byte-identical to today.
# Format (one line, JSON object; set once per-account static proxies exist):
#     BROKER_PROXY_MAP={"<broker_account_id>":"http://user:pass@13.203.129.136:8888"}
# The value contains a PASSWORD → NEVER log the URL; log only the account id
# and whether a mapping was found.
_PROXY_MAP_WARNED = False


def _resolve_account_proxy_env(broker_account_id: Optional[str]) -> Optional[str]:
    """LEGACY/FALLBACK layer: resolve a per-account egress proxy URL from the
    hand-written BROKER_PROXY_MAP env.

    Kept as a DOCUMENTED fallback beneath the DB (see resolve_account_proxy) so
    the mapping the operator hand-wrote for the first onboarded power user keeps
    working unchanged until it is migrated into the vault.

    Returns the mapped proxy URL for `broker_account_id`, or None when the id is
    None / unmapped / the map is unset, malformed, or not a JSON object. NEVER
    raises — a bad map degrades to "no per-account override" so the caller falls
    back to the global hook / direct egress (fail-open to TODAY's behaviour, not
    to someone else's proxy).

    Malformed JSON is logged ONCE at WARNING. The proxy URL VALUE is never
    logged (it carries a password) — only the account id + found yes/no.
    """
    global _PROXY_MAP_WARNED
    if not broker_account_id:
        return None
    _load_env_file()
    raw = os.environ.get("BROKER_PROXY_MAP", "").strip()
    if not raw:
        return None
    try:
        mapping = json.loads(raw)
        if not isinstance(mapping, dict):
            raise ValueError("BROKER_PROXY_MAP is not a JSON object")
    except Exception as e:
        if not _PROXY_MAP_WARNED:
            log.warning("BROKER_PROXY_MAP malformed (%s) — ignoring per-account "
                        "proxy overrides (falling back to global/direct)", e)
            _PROXY_MAP_WARNED = True
        return None
    url = mapping.get(broker_account_id)
    url = url.strip() if isinstance(url, str) else None
    log.info("resolve_account_proxy(env): account=%s per-account-proxy=%s",
             broker_account_id, "yes" if url else "no")
    return url or None


def resolve_account_proxy(broker_account_id: Optional[str]) -> Optional[str]:
    """Resolve a broker account's dedicated egress proxy URL.

    RESOLUTION ORDER (first hit wins):
      (a) the account's DB `broker_accounts.egress_proxy_url_enc` (Fernet-
          decrypted) — the SELF-SERVICE path. DB-first is what makes onboarding
          a new user a DB write with **no backend restart**: config/.env is only
          read once per process (see _load_env_file), so an env-only mechanism
          can never be self-service.
      (b) the legacy hand-written BROKER_PROXY_MAP env — kept as a documented
          fallback so the already-live mapping for the first onboarded power
          user does not break.
      (c) / (d) global BROKER_PROXY_URL, else None = DIRECT — both applied by
          _new_kite() when this returns None (not duplicated here).

    BROKER-AGNOSTIC: keyed only on broker_account_id; nothing here inspects the
    broker. (a) is implemented in autotrade.broker.egress, which any adapter can
    call.

    NEVER raises and NEVER logs the URL. A None/empty id (the operator's own
    process-global path) short-circuits BEFORE any lookup — his account must
    never be routed through a user's proxy.
    """
    if not broker_account_id:
        return None
    # (a) DB — imported lazily: services.* must not hard-depend on autotrade.*,
    # and a missing/broken autotrade package must degrade to the env path rather
    # than break the shared real-money auth module.
    try:
        from autotrade.broker.egress import get_account_proxy
        url = get_account_proxy(broker_account_id)
        if url:
            return url
    except Exception as e:  # pragma: no cover - defensive
        log.warning("resolve_account_proxy: DB egress lookup unavailable (%s) — "
                    "falling back to BROKER_PROXY_MAP/global", e)
    # (b) legacy env map
    return _resolve_account_proxy_env(broker_account_id)


def _new_kite(api_key: str, proxy_url: Optional[str] = None):
    """Construct a KiteConnect client, applying a proxy if configured.

    Single construction helper so all sites share the default-off proxy logic.
      - proxy_url non-empty → build the proxies dict from IT (per-account egress,
        overrides the global hook)
      - proxy_url None/blank → fall back to the global BROKER_PROXY_URL hook
      - neither set → NO proxies kwarg → direct egress (unchanged).
    """
    from kiteconnect import KiteConnect

    if isinstance(proxy_url, str) and proxy_url.strip():
        u = proxy_url.strip()
        return _apply_request_timeout(
            KiteConnect(api_key=api_key, proxies={"http": u, "https": u}))
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


# ── Token-status cache (for the O(1) order-path pre-check, AutoTrade GUARD G2) ─
# get_token_status() does a kite.profile() network round-trip. The AutoTrade
# order path (entry/exit) must NOT pay that on every order. So every successful/
# failed get_token_status() result is cached with a monotonic timestamp, and
# get_cached_token_status() lets a hot path REUSE a recent result WITHOUT ever
# triggering a fresh probe. This changes NO existing behaviour of
# get_token_status (it still probes + returns the same dict) — it only adds a
# side cache other callers can read.
import time as _time  # local alias; avoids touching the module import block above

_TOKEN_STATUS_CACHE: dict = {"status": None, "at": 0.0}


def get_token_status() -> dict:
    """
    Check current token validity.
    Returns safe dict (no full token).
    """
    try:
        api_key, _ = _get_credentials()
    except KiteAuthError as e:
        st = {"valid": False, "code": e.code, "reason": e.detail}
        _TOKEN_STATUS_CACHE.update(status=st, at=_time.monotonic())
        return st

    try:
        token = get_access_token()
    except KiteAuthError as e:
        st = {"valid": False, "code": e.code, "reason": e.detail}
        _TOKEN_STATUS_CACHE.update(status=st, at=_time.monotonic())
        return st

    try:
        kite = _new_kite(api_key)
        kite.set_access_token(token)
        profile = kite.profile()
        st = {
            "valid":         True,
            "user":          profile.get("user_name", ""),
            "email":         profile.get("email", ""),
            "token_preview": token[:8] + "...",
            "token_source":  "db" if _load_token_from_db() else "env",
            "token_date":    date.today().isoformat(),
        }
        _TOKEN_STATUS_CACHE.update(status=st, at=_time.monotonic())
        return st
    except Exception as e:
        st = {"valid": False, "code": "TOKEN_EXPIRED", "reason": str(e)}
        _TOKEN_STATUS_CACHE.update(status=st, at=_time.monotonic())
        return st


def get_cached_token_status(max_age_sec: float = 60.0) -> Optional[dict]:
    """Return the most recent get_token_status() result IF it is younger than
    max_age_sec, else None — WITHOUT triggering a fresh network probe.

    O(1), no network. Used by the AutoTrade order path (GUARD G2) so the happy
    path (valid token) costs nothing extra: any recent status computed by the
    admin widget / auth scheduler / a prior order is reused; a stale/absent cache
    returns None and the caller falls back to the O(1) token-PRESENCE check
    (get_access_token) rather than a profile() round-trip."""
    st = _TOKEN_STATUS_CACHE.get("status")
    if st is None:
        return None
    age = _time.monotonic() - float(_TOKEN_STATUS_CACHE.get("at") or 0.0)
    if age > max_age_sec:
        return None
    return dict(st)


def token_present() -> Optional[str]:
    """O(1) token-PRESENCE probe (no network): the current access token if one
    exists in DB/env, else None. A None here is the E5 'dead session' signal —
    there is NO token to place into. This never validates against the broker (a
    present-but-server-expired token still returns a string); it only answers
    'is there a token at all'. Used by GUARD G2 as the cheap always-available
    fallback when no cached status is fresh."""
    try:
        return get_access_token()
    except KiteAuthError:
        return None


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
