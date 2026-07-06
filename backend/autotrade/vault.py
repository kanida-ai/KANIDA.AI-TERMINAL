"""Credential vault (L3) — per (user, broker, account) broker credentials at rest.

PHASE-2 MULTI-TENANT. ADDITIVE + DEFAULT-OFF. This module owns the
`broker_accounts` table: it encrypts each account's api_secret + access_token at
rest with Fernet (symmetric AES-128-CBC + HMAC), keyed by a key from a small
KeyProvider interface (KMS-ready). Secrets are NEVER returned to the API/browser
— only status + masked previews. Decrypted creds are handed (in memory only) to
the broker layer at session-build / order time.

DEFAULT-OFF CONTRACT (the safety hinge):
  * The vault is ENABLED only when a key is available (env FALCON_VAULT_KEY, via
    EnvKeyProvider). If NO key is configured, `vault_enabled()` is False and
    every cred-resolution path returns None → the session layer falls back to the
    PROCESS-GLOBAL get_kite_client() = the EXACTLY-AS-TODAY operator path.
  * A NULL broker_account_id on a session ALSO means "use the global creds path"
    regardless of whether the vault is enabled. So existing sessions (no
    broker_account_id) behave byte-for-byte as before.

KEY PROVIDER (KMS-ready):
  * `KeyProvider` is a tiny interface (one method: get_fernet()). Today the only
    impl is `EnvKeyProvider` (reads FALCON_VAULT_KEY, supports key rotation via
    FALCON_VAULT_KEY_PREV using Fernet's MultiFernet). When the backend moves to
    a cloud VM, swap in a KmsKeyProvider with NO change to callers.

THREAT MODEL: anyone with BOTH the DB file AND the env key can decrypt — the same
trust boundary the operator already has on the laptop. The DB at rest (backups,
accidental commit) is protected; the running process must decrypt to trade.
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.vault")

IST = timezone(timedelta(hours=5, minutes=30))

VALID_BROKERS = ("zerodha", "upstox", "angel", "dhan", "fyers", "rupeezy",
                 "fivepaisa")
# Status state machine for a broker account.
STATUS_PENDING = "PENDING"   # account stored, no access token yet
STATUS_ACTIVE = "ACTIVE"     # has a fresh access token (today)
STATUS_EXPIRED = "EXPIRED"   # token from a prior day (Kite expires daily)
STATUS_REVOKED = "REVOKED"
STATUS_ERROR = "ERROR"


def _now_ist() -> datetime:
    return datetime.now(IST)


def _now_ist_iso() -> str:
    return _now_ist().isoformat()


def _today_ist() -> str:
    return _now_ist().date().isoformat()


# ── env loading (reuse the same config/.env discovery as kite_auth) ──────────

def _load_env_file() -> None:
    """Populate os.environ from config/.env if FALCON_VAULT_KEY is not already
    set, mirroring kite_auth._load_env_file's discovery (project-root config)."""
    if os.environ.get("FALCON_VAULT_KEY"):
        return
    here = Path(__file__).resolve()
    # backend/autotrade/vault.py → project root is parents[2] (backend/.. = root)
    candidates = [
        here.parents[2] / "config" / ".env",          # <root>/config/.env
        here.parents[3] / "config" / ".env" if len(here.parents) > 3 else None,
    ]
    for p in candidates:
        if p and p.exists():
            try:
                for line in p.read_text(encoding="utf-8",
                                        errors="replace").splitlines():
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    # Only set vault keys we care about here; don't clobber others.
                    if k in ("FALCON_VAULT_KEY", "FALCON_VAULT_KEY_PREV") and v:
                        os.environ.setdefault(k, v)
            except Exception:  # pragma: no cover - never crash on env read
                pass
            break


# ── Key provider interface (KMS-ready) ───────────────────────────────────────

class KeyProvider:
    """Supplies the Fernet (or MultiFernet) used to encrypt/decrypt at rest.

    The ONLY method callers use is get_fernet(); returns None when no key is
    available (→ vault disabled). Swap EnvKeyProvider for a KmsKeyProvider in the
    cloud end-state with zero change to the vault/session code."""

    def get_fernet(self):  # -> Optional[Fernet|MultiFernet]
        raise NotImplementedError

    def available(self) -> bool:
        return self.get_fernet() is not None


class EnvKeyProvider(KeyProvider):
    """Reads the Fernet key from env FALCON_VAULT_KEY (urlsafe-base64 32 bytes).

    Supports key rotation: if FALCON_VAULT_KEY_PREV is also set, a MultiFernet is
    built [current, prev] so values encrypted under the previous key still
    decrypt (re-encrypt-on-write rotates them forward). Returns None if no key is
    configured → the vault is DISABLED and the system runs exactly as today."""

    def __init__(self, env_var: str = "FALCON_VAULT_KEY",
                 prev_env_var: str = "FALCON_VAULT_KEY_PREV"):
        self._env_var = env_var
        self._prev_env_var = prev_env_var

    def _raw_keys(self) -> List[str]:
        _load_env_file()
        keys: List[str] = []
        cur = os.environ.get(self._env_var, "").strip()
        if cur:
            keys.append(cur)
        prev = os.environ.get(self._prev_env_var, "").strip()
        if prev:
            keys.append(prev)
        return keys

    def get_fernet(self):
        keys = self._raw_keys()
        if not keys:
            return None
        try:
            from cryptography.fernet import Fernet, MultiFernet
        except Exception as e:  # pragma: no cover - cryptography always present
            log.error("vault: cryptography not available: %s", e)
            return None
        try:
            fernets = [Fernet(k.encode() if isinstance(k, str) else k)
                       for k in keys]
        except Exception as e:
            log.error("vault: FALCON_VAULT_KEY is not a valid Fernet key: %s", e)
            return None
        if len(fernets) == 1:
            return fernets[0]
        return MultiFernet(fernets)


# Module-default provider (env-based). Tests may inject their own provider via
# the optional `provider=` argument on the public functions.
_DEFAULT_PROVIDER = EnvKeyProvider()


def generate_key() -> str:
    """Generate a fresh urlsafe-base64 Fernet key (operator helper for setup)."""
    from cryptography.fernet import Fernet
    return Fernet.generate_key().decode()


def vault_enabled(provider: Optional[KeyProvider] = None) -> bool:
    """True iff a vault key is available (vault ON). When False, every cred path
    falls back to the global operator client → byte-for-byte today's behaviour."""
    p = provider or _DEFAULT_PROVIDER
    return p.available()


class VaultDisabledError(RuntimeError):
    """Raised by put/refresh paths when no vault key is configured (so the API
    can return a clear 'configure FALCON_VAULT_KEY first' message). Read paths
    that resolve creds NEVER raise this — they return None and fall back."""


# ── encrypt / decrypt primitives ─────────────────────────────────────────────

def _encrypt(plaintext: str, provider: KeyProvider) -> bytes:
    f = provider.get_fernet()
    if f is None:
        raise VaultDisabledError(
            "vault key not configured (set FALCON_VAULT_KEY)")
    return f.encrypt((plaintext or "").encode("utf-8"))


def _decrypt(token: Optional[bytes], provider: KeyProvider) -> Optional[str]:
    if token is None:
        return None
    f = provider.get_fernet()
    if f is None:
        return None
    try:
        if isinstance(token, str):
            token = token.encode("utf-8")
        return f.decrypt(token).decode("utf-8")
    except Exception as e:
        log.error("vault: decrypt failed (wrong key / corrupt blob): %s", e)
        return None


def _mask(value: Optional[str]) -> Optional[str]:
    """Mask a secret for display: keep the first 4 chars, never the rest."""
    if not value:
        return None
    v = str(value)
    if len(v) <= 4:
        return "****"
    return v[:4] + "…" + ("*" * 4)


# ── public dict (NEVER includes secrets) ─────────────────────────────────────

def _row_to_public(row, provider: KeyProvider) -> Dict[str, Any]:
    """Serialise a broker_accounts row WITHOUT secrets. api_key is masked
    (it identifies the broker app but is not a credential on its own; we still
    mask it). api_secret / access_token are NEVER returned in any form except a
    boolean 'has_secret' / 'has_token'."""
    d = dict(row)
    token_date = d.get("token_date")
    status = _derive_status(d.get("status"), token_date,
                            d.get("token_expires_at"))
    return {
        "broker_account_id": d.get("broker_account_id"),
        "user_id": d.get("user_id"),
        "broker": d.get("broker"),
        "account_label": d.get("account_label"),
        "api_key_masked": _mask(d.get("api_key")),
        "has_secret": bool(d.get("api_secret_enc")),
        "has_token": bool(d.get("access_token_enc")),
        "status": status,
        "token_date": token_date,
        "token_expiry": d.get("token_expiry"),
        # Absolute expiry (e.g. Vortex JWT exp) — powers the real "expires" line on
        # the connection card. None for daily-cycle brokers (Kite).
        "token_expires_at": d.get("token_expires_at"),
        "created_at": d.get("created_at"),
        "updated_at": d.get("updated_at"),
        "last_login_at": d.get("last_login_at"),
        # Health observability — powers the "Last verified" line on the connection
        # card + the Admin all-users monitoring table. No secrets.
        "last_health_at": d.get("last_health_at"),
        "last_health_status": d.get("last_health_status"),
    }


def _derive_status(stored_status: Optional[str], token_date: Optional[str],
                   token_expires_at: Optional[str] = None) -> str:
    """ACTIVE only if the token is still live. PENDING/REVOKED/ERROR pass through.

    Two expiry rules, in order:
      1. Absolute expiry (token_expires_at, e.g. a Vortex JWT `exp`): if it has
         PASSED → EXPIRED, regardless of token_date. This catches short-lived
         tokens that die intraday (a Vortex token can lapse before market open),
         which the date-only rule would wrongly keep 'ACTIVE' all day.
      2. Otherwise (brokers with no absolute stamp, e.g. Kite's daily cycle) →
         ACTIVE only if token_date is TODAY (IST); a stale date → EXPIRED."""
    s = (stored_status or STATUS_PENDING).upper()
    if s in (STATUS_REVOKED, STATUS_ERROR, STATUS_PENDING):
        return s
    # ACTIVE / EXPIRED → re-derive.
    if token_expires_at:
        try:
            exp = datetime.fromisoformat(str(token_expires_at))
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=IST)
            if exp < _now_ist():
                return STATUS_EXPIRED
        except Exception:  # unparseable stamp → fall through to the date rule
            pass
    if token_date and str(token_date) == _today_ist():
        return STATUS_ACTIVE
    return STATUS_EXPIRED


# ── CRUD ─────────────────────────────────────────────────────────────────────

def put_account(user_id: str, broker: str, account_label: str,
                api_key: str, api_secret: str,
                provider: Optional[KeyProvider] = None) -> Dict[str, Any]:
    """Create (or update creds for) a broker account. Encrypts api_secret at rest.

    UNIQUE(user_id, broker, account_label): re-putting the same triple UPDATES
    api_key + api_secret_enc in place (a re-enter / rotate flow) and keeps the
    existing broker_account_id. Returns the public dict (no secrets).

    Raises VaultDisabledError if no vault key is configured."""
    provider = provider or _DEFAULT_PROVIDER
    if not provider.available():
        raise VaultDisabledError(
            "vault disabled — set FALCON_VAULT_KEY to store broker creds")
    if not user_id or not str(user_id).strip():
        raise ValueError("user_id required")
    broker = (broker or "").strip().lower()
    if broker not in VALID_BROKERS:
        raise ValueError(f"invalid broker {broker!r} (one of {VALID_BROKERS})")
    account_label = (account_label or "").strip()
    if not account_label:
        raise ValueError("account_label required")
    if not api_key or not api_secret:
        raise ValueError("api_key and api_secret required")

    secret_enc = _encrypt(api_secret, provider)
    now = _now_ist_iso()
    with falcon_conn() as con:
        existing = con.execute(
            """SELECT broker_account_id FROM broker_accounts
               WHERE user_id=? AND broker=? AND account_label=?""",
            (user_id, broker, account_label),
        ).fetchone()
        if existing:
            bid = existing["broker_account_id"]
            con.execute(
                """UPDATE broker_accounts
                   SET api_key=?, api_secret_enc=?, updated_at=?
                   WHERE broker_account_id=?""",
                (api_key, secret_enc, now, bid),
            )
        else:
            bid = uuid.uuid4().hex
            con.execute(
                """INSERT INTO broker_accounts
                   (broker_account_id, user_id, broker, account_label,
                    api_key, api_secret_enc, access_token_enc, token_date,
                    token_expiry, status, created_at, updated_at, last_login_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (bid, user_id, broker, account_label, api_key, secret_enc,
                 None, None, None, STATUS_PENDING, now, now, None),
            )
        con.commit()
        row = con.execute(
            "SELECT * FROM broker_accounts WHERE broker_account_id=?", (bid,),
        ).fetchone()
    log.info("vault: stored broker account %s (%s/%s) for user %s",
             bid, broker, account_label, user_id)
    return _row_to_public(row, provider)


def list_accounts(user_id: str,
                  provider: Optional[KeyProvider] = None) -> List[Dict[str, Any]]:
    """List a user's broker accounts (public dicts, NO secrets). Scoped strictly
    to user_id — there is no cross-user read path."""
    provider = provider or _DEFAULT_PROVIDER
    with falcon_conn() as con:
        rows = con.execute(
            """SELECT * FROM broker_accounts WHERE user_id=?
               ORDER BY created_at ASC""",
            (user_id,),
        ).fetchall()
    return [_row_to_public(r, provider) for r in rows]


def get_account_public(broker_account_id: str, user_id: Optional[str] = None,
                       provider: Optional[KeyProvider] = None
                       ) -> Optional[Dict[str, Any]]:
    """Fetch one account's PUBLIC dict (no secrets). If user_id is given it is
    enforced (defense-in-depth: a user can only read their own account)."""
    provider = provider or _DEFAULT_PROVIDER
    with falcon_conn() as con:
        if user_id is not None:
            row = con.execute(
                "SELECT * FROM broker_accounts WHERE broker_account_id=? AND user_id=?",
                (broker_account_id, user_id),
            ).fetchone()
        else:
            row = con.execute(
                "SELECT * FROM broker_accounts WHERE broker_account_id=?",
                (broker_account_id,),
            ).fetchone()
    if not row:
        return None
    return _row_to_public(row, provider)


def delete_account(broker_account_id: str, user_id: str,
                   provider: Optional[KeyProvider] = None) -> bool:
    """Delete a broker account, scoped to (broker_account_id, user_id). Returns
    True if a row was deleted. Does NOT touch sessions/positions."""
    with falcon_conn() as con:
        cur = con.execute(
            "DELETE FROM broker_accounts WHERE broker_account_id=? AND user_id=?",
            (broker_account_id, user_id),
        )
        con.commit()
        deleted = cur.rowcount > 0
    if deleted:
        log.info("vault: deleted broker account %s for user %s",
                 broker_account_id, user_id)
    return deleted


# ── token storage / refresh ──────────────────────────────────────────────────

def store_tokens(broker_account_id: str, access_token: str,
                 refresh_token: Optional[str] = None,
                 expires_at: Optional[str] = None,
                 user_id: Optional[str] = None,
                 token_date: Optional[str] = None,
                 provider: Optional[KeyProvider] = None,
                 token_expiry: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """BROKER-AGNOSTIC token store. Encrypt + persist BOTH the access token AND
    (when the broker supplies one) the refresh token, stamp status ACTIVE +
    token_date (defaults to today IST), and record the absolute expiry.

    Args:
      access_token   the freshly-minted access token (encrypted at rest).
      refresh_token  optional silent-renewal token (encrypted into
                     refresh_token_enc); None leaves the column untouched-null.
      expires_at     absolute ISO-IST expiry for non-daily brokers → written to
                     token_expires_at. None → daily/broker-defined expiry.
      token_date     IST date the token was minted (default today).
      token_expiry   legacy per-broker expiry hint (kept for back-compat with
                     store_access_token's signature; written to token_expiry).

    Returns the account's PUBLIC dict (no secrets), or None if the account is
    absent / not owned by user_id. Raises VaultDisabledError if no key."""
    provider = provider or _DEFAULT_PROVIDER
    if not provider.available():
        raise VaultDisabledError(
            "vault disabled — set FALCON_VAULT_KEY to store tokens")
    token_date = token_date or _today_ist()
    access_enc = _encrypt(access_token, provider)
    refresh_enc = _encrypt(refresh_token, provider) if refresh_token else None
    now = _now_ist_iso()
    with falcon_conn() as con:
        if user_id is not None:
            row = con.execute(
                "SELECT broker_account_id FROM broker_accounts "
                "WHERE broker_account_id=? AND user_id=?",
                (broker_account_id, user_id),
            ).fetchone()
        else:
            row = con.execute(
                "SELECT broker_account_id FROM broker_accounts WHERE broker_account_id=?",
                (broker_account_id,),
            ).fetchone()
        if not row:
            return None
        # Only overwrite refresh_token_enc when a new refresh token is supplied;
        # a plain access-token refresh must NOT wipe a still-valid refresh token.
        if refresh_enc is not None:
            con.execute(
                """UPDATE broker_accounts
                   SET access_token_enc=?, refresh_token_enc=?, token_date=?,
                       token_expiry=?, token_expires_at=?, status=?,
                       updated_at=?, last_login_at=?
                   WHERE broker_account_id=?""",
                (access_enc, refresh_enc, token_date, token_expiry, expires_at,
                 STATUS_ACTIVE, now, now, broker_account_id),
            )
        else:
            con.execute(
                """UPDATE broker_accounts
                   SET access_token_enc=?, token_date=?, token_expiry=?,
                       token_expires_at=?, status=?, updated_at=?, last_login_at=?
                   WHERE broker_account_id=?""",
                (access_enc, token_date, token_expiry, expires_at,
                 STATUS_ACTIVE, now, now, broker_account_id),
            )
        con.commit()
        pub = con.execute(
            "SELECT * FROM broker_accounts WHERE broker_account_id=?",
            (broker_account_id,),
        ).fetchone()
    log.info("vault: stored tokens for account %s (token_date=%s, refresh=%s, "
             "expires_at=%s)", broker_account_id, token_date,
             bool(refresh_token), expires_at)
    return _row_to_public(pub, provider)


def store_access_token(broker_account_id: str, access_token: str,
                       user_id: Optional[str] = None,
                       token_date: Optional[str] = None,
                       token_expiry: Optional[str] = None,
                       provider: Optional[KeyProvider] = None
                       ) -> Optional[Dict[str, Any]]:
    """BACK-COMPAT SHIM. Encrypt + store a freshly-minted access_token, set
    status ACTIVE and stamp token_date. Delegates to store_tokens (no refresh
    token, no absolute expiry) so the existing Kite exchange path is unchanged.

    Raises VaultDisabledError if no vault key is configured."""
    return store_tokens(
        broker_account_id, access_token, refresh_token=None, expires_at=None,
        user_id=user_id, token_date=token_date, provider=provider,
        token_expiry=token_expiry)


def record_health(broker_account_id: str, status: str, detail: str = "",
                  user_id: Optional[str] = None,
                  provider: Optional[KeyProvider] = None) -> bool:
    """Record the result of a health probe (validate()) onto the account:
    last_health_at (now IST), last_health_status, last_error. Does NOT change the
    account's primary `status` column (that stays token-date-derived); this is
    connection-health observability only.

    Scoped to user_id when given. Returns True if a row was updated. Never
    raises on a disabled vault (health recording is best-effort metadata)."""
    now = _now_ist_iso()
    with falcon_conn() as con:
        if user_id is not None:
            cur = con.execute(
                """UPDATE broker_accounts
                   SET last_health_at=?, last_health_status=?, last_error=?,
                       updated_at=?
                   WHERE broker_account_id=? AND user_id=?""",
                (now, status, detail or None, now, broker_account_id, user_id),
            )
        else:
            cur = con.execute(
                """UPDATE broker_accounts
                   SET last_health_at=?, last_health_status=?, last_error=?,
                       updated_at=?
                   WHERE broker_account_id=?""",
                (now, status, detail or None, now, broker_account_id),
            )
        con.commit()
        updated = cur.rowcount > 0
    if updated:
        log.info("vault: recorded health for account %s: %s%s",
                 broker_account_id, status,
                 f" ({detail})" if detail else "")
    return updated


# ── decrypted-cred resolution (in memory only; for the broker layer) ─────────

class DecryptedCreds:
    """In-memory-only decrypted credentials for one account. NEVER serialised,
    NEVER logged (repr hides the secrets)."""
    __slots__ = ("broker_account_id", "user_id", "broker", "account_label",
                 "api_key", "api_secret", "access_token", "refresh_token",
                 "token_date", "token_expires_at", "status")

    def __init__(self, **kw):
        for k in self.__slots__:
            setattr(self, k, kw.get(k))

    def __repr__(self) -> str:  # never leak secrets in a repr
        return (f"DecryptedCreds(broker_account_id={self.broker_account_id!r}, "
                f"broker={self.broker!r}, account_label={self.account_label!r}, "
                f"api_key=<set:{bool(self.api_key)}>, "
                f"api_secret=<redacted>, access_token=<redacted>, "
                f"refresh_token=<set:{bool(self.refresh_token)}>, "
                f"status={self.status!r})")


def get_decrypted_creds(broker_account_id: str, user_id: Optional[str] = None,
                        provider: Optional[KeyProvider] = None
                        ) -> Optional[DecryptedCreds]:
    """Resolve an account's DECRYPTED creds for the broker layer (in memory).

    Returns None — NEVER raises — when:
      * the vault is disabled (no key), OR
      * the account is absent / not owned by user_id, OR
      * decryption fails.
    A None result tells the caller to FALL BACK to the global creds path
    (today's operator behaviour). This is the safety hinge.

    When user_id is provided it is enforced (cross-user reads impossible)."""
    provider = provider or _DEFAULT_PROVIDER
    if not provider.available():
        return None
    with falcon_conn() as con:
        if user_id is not None:
            row = con.execute(
                "SELECT * FROM broker_accounts WHERE broker_account_id=? AND user_id=?",
                (broker_account_id, user_id),
            ).fetchone()
        else:
            row = con.execute(
                "SELECT * FROM broker_accounts WHERE broker_account_id=?",
                (broker_account_id,),
            ).fetchone()
    if not row:
        return None
    d = dict(row)
    api_secret = _decrypt(d.get("api_secret_enc"), provider)
    access_token = _decrypt(d.get("access_token_enc"), provider)
    # refresh_token_enc is an additive column; older rows / DBs may not have it.
    refresh_token = _decrypt(d.get("refresh_token_enc"), provider)
    return DecryptedCreds(
        broker_account_id=d.get("broker_account_id"),
        user_id=d.get("user_id"),
        broker=d.get("broker"),
        account_label=d.get("account_label"),
        api_key=d.get("api_key"),
        api_secret=api_secret,
        access_token=access_token,
        refresh_token=refresh_token,
        token_date=d.get("token_date"),
        token_expires_at=d.get("token_expires_at"),
        status=_derive_status(d.get("status"), d.get("token_date"),
                              d.get("token_expires_at")),
    )


def account_exists(broker_account_id: str,
                   user_id: Optional[str] = None) -> bool:
    with falcon_conn() as con:
        if user_id is not None:
            row = con.execute(
                "SELECT 1 FROM broker_accounts WHERE broker_account_id=? AND user_id=?",
                (broker_account_id, user_id),
            ).fetchone()
        else:
            row = con.execute(
                "SELECT 1 FROM broker_accounts WHERE broker_account_id=?",
                (broker_account_id,),
            ).fetchone()
    return row is not None
