"""Per-broker-account STATIC EGRESS IP — self-service assignment + resolution.

WHY: SEBI's registered-IP rule (as enforced by Zerodha/Kite Connect and, by the
same logic, every other broker app) binds ONE static IP to ONE broker account.
A multi-tenant backend therefore needs a DISTINCT egress IP per power user's
broker account. Until now that mapping was hand-written into config/.env as
BROKER_PROXY_MAP + a backend restart, per user — which does not scale and
contradicts the self-service onboarding design.

THIS MODULE makes the mapping DATA, not configuration:
  * `broker_accounts.egress_proxy_url_enc` — the account's proxy URL, FERNET-
    ENCRYPTED at rest exactly like api_secret_enc (the URL embeds a password).
  * Onboarding assigns a free URL from a pool → a DB write → **no restart**.
  * Deleting the account releases the URL back to the pool (availability is
    derived from live rows, so the release is automatic and cannot leak).

BROKER-AGNOSTIC BY CONSTRUCTION: nothing here knows or asks what broker an
account belongs to. It is keyed purely on broker_account_id. Today only the
Zerodha client threads the resolved URL into its REST client, but any future
adapter gets per-account egress by calling `get_account_proxy(...)` — there is
no `if broker == "..."` anywhere in this file, and there must never be.

SAFETY / DEFAULT-OFF (the hinges):
  * An account with NO row value and no env mapping → None → the caller egresses
    DIRECT. Nothing configured == today's behaviour, byte for byte.
  * The operator's own global path (broker_account_id None) never reaches this
    module — `resolve_account_proxy(None)` short-circuits before any lookup.
  * EVERY read path fails OPEN TO DIRECT, never to a wrong proxy: a missing
    column, an unreadable DB, a disabled vault or a decrypt failure all return
    None (logged) so the caller falls back to env/global/direct. Routing an
    account through SOMEONE ELSE'S proxy is the one outcome worse than no proxy
    at all (their orders would egress from an IP allowlisted to another user).

SECRET HANDLING: the proxy URL carries credentials. It is NEVER returned by any
API and NEVER logged — callers get `egress_host()` (bare host/IP, no creds, no
port) or a boolean. Same contract as api_secret.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

log = logging.getLogger("kanida.autotrade.egress")

# Env name for the operator's pool of provisioned static-IP proxies. JSON array
# of proxy URLs, one per hand-created Lightsail box (see
# docs/ops/AWS_LIGHTSAIL_STATIC_IP_SETUP.md). Deliberately NOT an AWS API
# integration — the operator creates boxes by hand and pastes the URLs here.
#     BROKER_EGRESS_POOL=["http://user:pw@13.203.129.136:8888", "..."]
POOL_ENV = "BROKER_EGRESS_POOL"

_POOL_WARNED = False


# ── URL helpers ──────────────────────────────────────────────────────────────

def egress_host(proxy_url: Optional[str]) -> Optional[str]:
    """Extract the bare host/IP from a proxy URL — the ONLY part safe to surface.

    "http://user:s3cr3t@13.203.129.136:8888" -> "13.203.129.136"

    Credentials and port are dropped: the user needs the IP to register on their
    broker app, and nothing else. Returns None for anything unparseable (never
    raises, never echoes the input back)."""
    if not isinstance(proxy_url, str) or not proxy_url.strip():
        return None
    try:
        host = urlsplit(proxy_url.strip()).hostname
    except Exception:
        return None
    return host or None


# ── DB read (the resolution source of truth) ─────────────────────────────────

def _column_exists(con) -> bool:
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(broker_accounts)")}
    except Exception:
        return False
    return "egress_proxy_url_enc" in cols


def get_account_proxy(broker_account_id: Optional[str],
                      provider: Optional[Any] = None) -> Optional[str]:
    """Return the account's DECRYPTED egress proxy URL, or None.

    NEVER raises and NEVER logs the URL. None means "no per-account egress
    configured in the DB" → the caller falls back to the env map / global hook /
    direct. Returns None when: the id is empty, the vault is disabled, the
    column/table/DB is missing, the row has no value, or decryption fails
    (fail-open to DIRECT — never to another account's proxy)."""
    if not broker_account_id:
        return None
    try:
        from .. import vault
        from oltp_db import oltp_conn as falcon_conn  # OLTP: SQLite(flag off)/Postgres(KANIDA_PG_ENABLED). pure-OLTP.

        p = provider or vault._DEFAULT_PROVIDER
        if not p.available():
            return None
        with falcon_conn() as con:
            if not _column_exists(con):
                return None
            row = con.execute(
                "SELECT egress_proxy_url_enc FROM broker_accounts "
                "WHERE broker_account_id=?", (broker_account_id,),
            ).fetchone()
        if not row:
            return None
        blob = row["egress_proxy_url_enc"] if not isinstance(row, tuple) else row[0]
        if not blob:
            return None
        url = vault._decrypt(blob, p)
    except Exception as e:
        # A broken DB/vault must degrade to DIRECT, never take the order path
        # down and never route to a wrong proxy.
        log.warning("egress: DB lookup failed for account=%s (%s) — "
                    "falling back to env/global/direct", broker_account_id, e)
        return None
    if url is None:
        # _decrypt already logged the failure reason (without the value).
        log.warning("egress: could not decrypt egress proxy for account=%s — "
                    "falling back to env/global/direct", broker_account_id)
        return None
    url = url.strip()
    log.info("egress: account=%s db-egress=%s", broker_account_id,
             "yes" if url else "no")
    return url or None


# ── DB write (set / clear) ───────────────────────────────────────────────────

def set_account_proxy(broker_account_id: str, proxy_url: str,
                      user_id: Optional[str] = None,
                      provider: Optional[Any] = None) -> bool:
    """Encrypt + store an account's egress proxy URL. Returns True if a row was
    updated (False = no such account / not owned by user_id).

    Raises vault.VaultDisabledError when no vault key is configured — we refuse
    to store a password-bearing URL in plaintext, ever. Raises ValueError on a
    blank or unparseable URL (a bad URL silently stored would break the user's
    orders at 09:15, not at configuration time)."""
    from .. import vault
    from oltp_db import oltp_conn as falcon_conn  # OLTP: SQLite(flag off)/Postgres(KANIDA_PG_ENABLED). pure-OLTP.

    p = provider or vault._DEFAULT_PROVIDER
    if not p.available():
        raise vault.VaultDisabledError(
            "vault disabled — set FALCON_VAULT_KEY to store an egress proxy")
    if not isinstance(proxy_url, str) or not proxy_url.strip():
        raise ValueError("proxy_url required")
    proxy_url = proxy_url.strip()
    if not egress_host(proxy_url):
        raise ValueError("proxy_url is not a valid URL (expected "
                         "http://user:pass@HOST:PORT)")
    enc = vault._encrypt(proxy_url, p)
    now = vault._now_ist_iso()
    with falcon_conn() as con:
        if user_id is not None:
            cur = con.execute(
                "UPDATE broker_accounts SET egress_proxy_url_enc=?, updated_at=? "
                "WHERE broker_account_id=? AND user_id=?",
                (enc, now, broker_account_id, user_id))
        else:
            cur = con.execute(
                "UPDATE broker_accounts SET egress_proxy_url_enc=?, updated_at=? "
                "WHERE broker_account_id=?", (enc, now, broker_account_id))
        con.commit()
        ok = cur.rowcount > 0
    if ok:
        # Host only — the URL carries a password.
        log.info("egress: assigned static egress host=%s to account=%s",
                 egress_host(proxy_url), broker_account_id)
    return ok


def clear_account_proxy(broker_account_id: str,
                        user_id: Optional[str] = None) -> bool:
    """Release an account's egress proxy (back to the pool). Returns True if a
    row was updated. Needs no vault key — clearing must always be possible."""
    from .. import vault
    from oltp_db import oltp_conn as falcon_conn  # OLTP: SQLite(flag off)/Postgres(KANIDA_PG_ENABLED). pure-OLTP.

    now = vault._now_ist_iso()
    with falcon_conn() as con:
        if user_id is not None:
            cur = con.execute(
                "UPDATE broker_accounts SET egress_proxy_url_enc=NULL, updated_at=? "
                "WHERE broker_account_id=? AND user_id=?",
                (now, broker_account_id, user_id))
        else:
            cur = con.execute(
                "UPDATE broker_accounts SET egress_proxy_url_enc=NULL, updated_at=? "
                "WHERE broker_account_id=?", (now, broker_account_id))
        con.commit()
        ok = cur.rowcount > 0
    if ok:
        log.info("egress: released egress proxy for account=%s", broker_account_id)
    return ok


# ── Public status (what the connect UI renders) ──────────────────────────────

def egress_status(broker_account_id: str, user_id: Optional[str] = None,
                  provider: Optional[Any] = None) -> Dict[str, Any]:
    """The SAFE public view of an account's egress, for the connect flow.

    NEVER contains the proxy URL or its credentials — only the bare IP/host the
    user must register on their broker app, plus where it came from.

    source: "account" (dedicated DB-assigned static IP — the self-service path)
          | "env_map"  (legacy hand-written BROKER_PROXY_MAP fallback)
          | "direct"   (no proxy → egresses from the server's own IP)
    """
    url = get_account_proxy(broker_account_id, provider=provider)
    source = "account"
    if not url:
        # Legacy fallback so an account mapped in .env still SHOWS its real IP.
        try:
            import services.kite_auth as kite_auth
            url = kite_auth._resolve_account_proxy_env(broker_account_id)
        except Exception:
            url = None
        source = "env_map" if url else "direct"
    host = egress_host(url)
    return {
        "broker_account_id": broker_account_id,
        "configured": bool(host),
        "egress_ip": host,
        "source": source if host else "direct",
        "instructions": (
            f"Register {host} as the Allowed IP on your broker app AND on your "
            f"broker profile (SEBI static-IP requirement). Orders for this "
            f"account will originate from this IP only."
        ) if host else (
            "No dedicated static IP is assigned to this account yet. Orders "
            "egress from the server's shared IP."
        ),
    }


# ── Pool ─────────────────────────────────────────────────────────────────────

def pool_urls() -> List[str]:
    """The operator's configured pool of provisioned static-IP proxy URLs.

    Deliberately minimal: a JSON array in env BROKER_EGRESS_POOL, one entry per
    hand-created Lightsail box. Unset/malformed → empty pool (assignment then
    fails loudly at onboarding rather than silently mis-routing). Malformed is
    warned ONCE; the URLs themselves are never logged."""
    global _POOL_WARNED
    try:
        import services.kite_auth as kite_auth
        kite_auth._load_env_file()
    except Exception:
        pass
    raw = os.environ.get(POOL_ENV, "").strip()
    if not raw:
        return []
    try:
        pool = json.loads(raw)
        if not isinstance(pool, list):
            raise ValueError(f"{POOL_ENV} is not a JSON array")
    except Exception as e:
        if not _POOL_WARNED:
            log.warning("%s malformed (%s) — treating the egress pool as EMPTY",
                        POOL_ENV, e)
            _POOL_WARNED = True
        return []
    out = []
    for u in pool:
        if isinstance(u, str) and u.strip() and egress_host(u.strip()):
            out.append(u.strip())
    return out


def _assigned_urls(provider: Optional[Any] = None) -> List[str]:
    """Every proxy URL currently held by a broker account (decrypted, in memory).

    Availability is DERIVED from live rows — so deleting an account releases its
    IP automatically and a released IP can never be double-booked."""
    from .. import vault
    from oltp_db import oltp_conn as falcon_conn  # OLTP: SQLite(flag off)/Postgres(KANIDA_PG_ENABLED). pure-OLTP.

    p = provider or vault._DEFAULT_PROVIDER
    if not p.available():
        return []
    out: List[str] = []
    try:
        with falcon_conn() as con:
            if not _column_exists(con):
                return []
            rows = con.execute(
                "SELECT egress_proxy_url_enc FROM broker_accounts "
                "WHERE egress_proxy_url_enc IS NOT NULL").fetchall()
        for r in rows:
            blob = r["egress_proxy_url_enc"] if not isinstance(r, tuple) else r[0]
            u = vault._decrypt(blob, p)
            if u and u.strip():
                out.append(u.strip())
    except Exception as e:
        log.warning("egress: could not read assigned proxies (%s)", e)
        return out
    return out


def pool_status(provider: Optional[Any] = None) -> Dict[str, Any]:
    """Operator view of the pool — COUNTS + bare IPs only, never the URLs."""
    pool = pool_urls()
    assigned = set(_assigned_urls(provider=provider))
    free = [u for u in pool if u not in assigned]
    return {
        "pool_size": len(pool),
        "assigned": len(pool) - len(free),
        "available": len(free),
        "available_ips": [egress_host(u) for u in free],
        "assigned_ips": [egress_host(u) for u in pool if u in assigned],
    }


class NoEgressAvailableError(RuntimeError):
    """The pool is empty / fully assigned — the operator must provision another
    static-IP box before this user can be onboarded for live orders."""


def assign_from_pool(broker_account_id: str, user_id: Optional[str] = None,
                     provider: Optional[Any] = None) -> Dict[str, Any]:
    """Assign the first FREE pool URL to an account and persist it (encrypted).

    IDEMPOTENT: an account that already has an egress keeps it (returns its
    current status) — re-running onboarding must never burn a second IP or
    silently move a user to a different IP than the one they registered.

    Raises NoEgressAvailableError when nothing is free."""
    existing = get_account_proxy(broker_account_id, provider=provider)
    if existing:
        return egress_status(broker_account_id, user_id=user_id,
                             provider=provider)
    pool = pool_urls()
    if not pool:
        raise NoEgressAvailableError(
            f"no egress pool configured (set {POOL_ENV})")
    assigned = set(_assigned_urls(provider=provider))
    free = [u for u in pool if u not in assigned]
    if not free:
        raise NoEgressAvailableError(
            f"egress pool exhausted ({len(pool)} IPs, all assigned) — "
            "provision another static-IP box and add it to " + POOL_ENV)
    ok = set_account_proxy(broker_account_id, free[0], user_id=user_id,
                           provider=provider)
    if not ok:
        raise NoEgressAvailableError("broker account not found")
    return egress_status(broker_account_id, user_id=user_id, provider=provider)
