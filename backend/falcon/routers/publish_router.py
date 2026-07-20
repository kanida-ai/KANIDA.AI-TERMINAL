"""/api/falcon/publish/* — laptop → cloud "publish intelligence" ingest.

Phase 1 of the Kanida.AI cloud migration (see docs/launch/CLOUD_ARCHITECTURE.md
§6). With the production DB on a cloud volume, the research laptop can no longer
`sqlite3.connect` it remotely. The weekly publish therefore travels over HTTP:

    laptop (scripts/publish_to_cloud.py)
        --POST /api/falcon/publish/intelligence + X-Publish-Secret-->
    cloud (this router)  →  full-replace into the cloud PROD DB (one transaction)

This is the cloud counterpart of the LOCAL R&D→PROD publish
(`scripts/publish_patterns.py`). It mirrors that script's safety guards:
  - refuse to wipe PROD patterns with an EMPTY bundle (REFUSING_EMPTY_PUBLISH)
  - atomic full-replace inside ONE transaction (rollback on any error)
  - an audit row into falcon_signal_runs (job_name='publish_intelligence')

SECURITY (self-contained — INV5 no secrets in code):
  The cloud FastAPI backend is reached server-to-server (api host), NOT through
  the Next.js Basic-Auth middleware that gates the operator UI. So this endpoint
  protects ITSELF with its own secret (X-Publish-Secret == env
  FALCON_PUBLISH_SECRET), checked in constant time. It does NOT rely on any
  upstream gate.

INV2: this module only ADDS a router. It never touches Auto-Trade execution
(backend/falcon/trade/). INV4: all timestamps are IST.
"""
from __future__ import annotations

import hmac
import json
import logging
import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from ..db import falcon_conn

router = APIRouter()

log = logging.getLogger("kanida.falcon.publish")

IST = timezone(timedelta(hours=5, minutes=30))

# ── Supported bundle protocol version ────────────────────────────────────────
SUPPORTED_BUNDLE_VERSION = 1

# ── Table allowlist (Phase 1) ────────────────────────────────────────────────
# ONLY these tables may be written by a publish bundle. Designed as a constant
# so Phase 2 can extend it (evidence / persona summary tables) without touching
# the import logic below. Any other table name → 400 TABLE_NOT_ALLOWED.
ALLOWED_TABLES: tuple[str, ...] = (
    "falcon_promoted_patterns",
    "falcon_pattern_candidates",
    "falcon_pattern_taxonomy",
    # Phase 2 (do NOT enable until the cloud DB has these tables + a publisher
    # ships them): "pattern_stock_evidence", "stock_signal_evidence",
    # "persona_backtest_summary", "persona_backtest_yearly", ...
)

# The one table whose emptiness means "do not wipe PROD" (mirrors
# publish_patterns: refuse to wipe prod with empty data). If the bundle includes
# this key but its row list is empty → REFUSING_EMPTY_PUBLISH.
EMPTY_GUARD_TABLE = "falcon_promoted_patterns"


def _now_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


# ── Request / response models ────────────────────────────────────────────────
class PublishBundle(BaseModel):
    bundle_version: int
    generated_at: str
    source: str
    cutoff_year: int | None = None
    # tables: { <table_name>: [ {col: val, ...}, ... ] }
    tables: Dict[str, List[Dict[str, Any]]]


def _require_secret(x_publish_secret: str | None) -> None:
    """Self-contained auth. Fail CLOSED if the secret is not configured."""
    expected = os.environ.get("FALCON_PUBLISH_SECRET")
    if not expected:
        # Fail closed: an unconfigured server must never accept a publish.
        raise HTTPException(
            status_code=503,
            detail={"code": "PUBLISH_NOT_CONFIGURED",
                    "message": "FALCON_PUBLISH_SECRET is not set on the server."},
        )
    if not x_publish_secret or not hmac.compare_digest(str(x_publish_secret), str(expected)):
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN",
                    "message": "Invalid or missing X-Publish-Secret."},
        )


def _table_columns(con: sqlite3.Connection, table: str) -> List[str]:
    """Introspect the LIVE table's column names. Empty list → table missing."""
    return [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]


def _count(con: sqlite3.Connection, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _audit_row(con: sqlite3.Connection, status: str, notes: str,
               rows: int | None, error: str | None, started_at: str) -> int | None:
    """Append an audit row to falcon_signal_runs. Returns the new row id."""
    cur = con.execute(
        """INSERT INTO falcon_signal_runs
             (job_name, started_at, finished_at, status, rows_affected, notes, error)
           VALUES ('publish_intelligence', ?, ?, ?, ?, ?, ?)""",
        (started_at, _now_ist(), status, rows, notes, error),
    )
    return cur.lastrowid


@router.post("/falcon/publish/intelligence")
def publish_intelligence(
    bundle: PublishBundle,
    x_publish_secret: str | None = Header(default=None, alias="X-Publish-Secret"),
):
    """Ingest a compact intelligence bundle from the research laptop and
    full-replace the allowlisted tables in the cloud PROD DB, atomically.

    See module docstring for the security + atomicity contract.
    """
    # ── 1. Auth (self-contained, constant-time, fail-closed) ─────────────────
    _require_secret(x_publish_secret)

    # ── 2. Protocol version ──────────────────────────────────────────────────
    if bundle.bundle_version != SUPPORTED_BUNDLE_VERSION:
        raise HTTPException(
            status_code=400,
            detail={"code": "UNSUPPORTED_BUNDLE_VERSION",
                    "message": f"bundle_version {bundle.bundle_version} not supported "
                               f"(server supports {SUPPORTED_BUNDLE_VERSION})."},
        )

    # ── 3. Table allowlist ───────────────────────────────────────────────────
    for tname in bundle.tables.keys():
        if tname not in ALLOWED_TABLES:
            raise HTTPException(
                status_code=400,
                detail={"code": "TABLE_NOT_ALLOWED",
                        "message": f"Table '{tname}' is not in the Phase-1 allowlist.",
                        "allowed": list(ALLOWED_TABLES)},
            )

    # ── 4. Empty-publish guard (mirror publish_patterns) ─────────────────────
    # Never wipe PROD patterns with an empty bundle.
    if EMPTY_GUARD_TABLE in bundle.tables and len(bundle.tables[EMPTY_GUARD_TABLE]) == 0:
        raise HTTPException(
            status_code=400,
            detail={"code": "REFUSING_EMPTY_PUBLISH",
                    "message": f"Bundle includes '{EMPTY_GUARD_TABLE}' but it is EMPTY. "
                               "Refusing to wipe PROD patterns with empty data."},
        )

    started_at = _now_ist()

    with falcon_conn() as con:
        con.execute("PRAGMA foreign_keys = OFF")  # full-replace; no FK trips

        # ── 5. Validate columns against the LIVE schema BEFORE any write ──────
        # For each table: it must exist, and every row's columns must be a
        # SUBSET of the live table's columns (reject unknown columns).
        live_cols: Dict[str, List[str]] = {}
        before_counts: Dict[str, int] = {}
        for tname, rows in bundle.tables.items():
            cols = _table_columns(con, tname)
            if not cols:
                raise HTTPException(
                    status_code=400,
                    detail={"code": "TABLE_MISSING",
                            "message": f"Allowlisted table '{tname}' does not exist in "
                                       "the cloud DB. Run schema init first."},
                )
            live_cols[tname] = cols
            before_counts[tname] = _count(con, tname)
            live_set = set(cols)
            for i, row in enumerate(rows):
                unknown = [c for c in row.keys() if c not in live_set]
                if unknown:
                    raise HTTPException(
                        status_code=400,
                        detail={"code": "UNKNOWN_COLUMN",
                                "message": f"Row {i} of '{tname}' has columns not present "
                                           f"in the live table: {sorted(unknown)}.",
                                "table": tname,
                                "live_columns": cols},
                    )

        # ── 6. Atomic full-replace (ONE transaction; rollback on any error) ──
        # full-replace semantics matching publish_patterns: DELETE then INSERT.
        after_counts: Dict[str, int] = {}
        total_rows = 0
        con.execute("BEGIN IMMEDIATE")
        try:
            for tname, rows in bundle.tables.items():
                con.execute(f"DELETE FROM {tname}")
                if rows:
                    # Insert each row using only the keys present on that row,
                    # parameterized. (Columns already validated as a subset.)
                    for row in rows:
                        row_cols = list(row.keys())
                        col_list = ", ".join(row_cols)
                        placeholders = ", ".join("?" * len(row_cols))
                        values = [row[c] for c in row_cols]
                        con.execute(
                            f"INSERT INTO {tname} ({col_list}) VALUES ({placeholders})",
                            values,
                        )
                total_rows += len(rows)

            after_counts = {t: _count(con, t) for t in bundle.tables.keys()}

            # ── 7. Audit row inside the SAME transaction (IST) ───────────────
            per_table = ", ".join(
                f"{t}: {before_counts[t]}->{after_counts[t]}" for t in bundle.tables.keys()
            )
            notes = (
                f"source={bundle.source} cutoff_year={bundle.cutoff_year} "
                f"bundle_generated_at={bundle.generated_at} "
                f"bundle_version={bundle.bundle_version} | {per_table}"
            )
            audit_run_id = _audit_row(
                con, status="success", notes=notes,
                rows=total_rows, error=None, started_at=started_at,
            )
            con.execute("COMMIT")
        except HTTPException:
            con.execute("ROLLBACK")
            raise
        except Exception as e:
            con.execute("ROLLBACK")
            # Best-effort failure audit OUTSIDE the rolled-back transaction.
            try:
                _audit_row(
                    con, status="failed",
                    notes=(f"rollback source={bundle.source} "
                           f"cutoff_year={bundle.cutoff_year}"),
                    rows=None, error=str(e), started_at=started_at,
                )
                con.commit()
            except Exception:
                pass
            raise HTTPException(
                status_code=500,
                detail={"code": "PUBLISH_FAILED",
                        "message": f"Import rolled back; cloud DB unchanged. {e}"},
            )

    # ── 8. Idempotent full-replace → re-POST yields the same state ───────────
    return {
        "status": "published",
        "source": bundle.source,
        "cutoff_year": bundle.cutoff_year,
        "tables": {
            t: {"before": before_counts[t], "after": after_counts[t]}
            for t in bundle.tables.keys()
        },
        "audit_run_id": audit_run_id,
    }


# ══════════════════════════════════════════════════════════════════════════════
# KTS (2026-07-17): Kite (Zerodha) daily access-token SYNC — laptop → cloud.
#
# The Zerodha access token is minted ONLY on the laptop (Playwright login runs
# there, see scripts/auth_worker.py). A cloud-hosted copy of this backend has no
# way to mint it, yet needs it to place LIVE orders. This endpoint is the cloud
# INGEST: the laptop POSTs the token it already minted and the cloud persists it
# through kite_auth's own save path, so get_access_token() serves it unchanged.
#
# SECURITY: reuses the SAME self-contained, constant-time, fail-CLOSED auth as
# /falcon/publish/intelligence (X-Publish-Secret == env FALCON_PUBLISH_SECRET).
# REAL-MONEY-ADJACENT: the token authorizes live orders. The token VALUE is
# NEVER logged or echoed — only its length, the date, and set_by.
#
# ADDITIVE: this only adds a route. It does NOT change get_access_token()'s
# priority logic, the minting worker, or any execution path. The persist uses
# kite_auth._save_token_to_db (no schema/insert duplication), which upserts
# today's row keyed on date.today() — exactly the row get_access_token() reads.
# ══════════════════════════════════════════════════════════════════════════════
class KiteTokenPush(BaseModel):
    access_token: str
    # Optional IST YYYY-MM-DD; advisory/echoed only. The authoritative storage
    # key is the server's date.today() (what get_access_token reads), so the
    # write and the read stay internally consistent on the cloud box.
    token_date: str | None = None
    set_by: str = "cloud-sync"


@router.post("/falcon/publish/kite-token")
def publish_kite_token(
    payload: KiteTokenPush,
    x_publish_secret: str | None = Header(default=None, alias="X-Publish-Secret"),
):
    """Ingest the daily Kite access token minted on the laptop and persist it so
    the cloud's get_access_token() returns it. See the KTS block above.

    Body: { access_token: str, token_date?: str (IST YYYY-MM-DD), set_by?: str }
    Returns: { ok: true, token_date: <stored>, requested_token_date: <echo>,
               stored: true }
    """
    # ── 1. Auth (self-contained, constant-time, fail-closed) — same as above ──
    _require_secret(x_publish_secret)

    # ── 2. Validate — reject empty/whitespace token (never store a blank) ─────
    token = (payload.access_token or "").strip()
    if not token:
        raise HTTPException(
            status_code=400,
            detail={"code": "EMPTY_TOKEN",
                    "message": "access_token is empty or whitespace."},
        )

    set_by = (payload.set_by or "cloud-sync").strip() or "cloud-sync"
    requested_date = (payload.token_date or "").strip() or datetime.now(IST).date().isoformat()

    # ── 3. Persist via kite_auth's OWN save path (no schema/insert dup) ───────
    # _save_token_to_db upserts today's row (DELETE today + INSERT) keyed on
    # date.today() — the exact row get_access_token() serves. Lazy import mirrors
    # every other falcon→services call site (services/ on sys.path at runtime)
    # and keeps this module importable in isolation for tests.
    from services.kite_auth import _save_token_to_db

    _save_token_to_db(token, set_by=set_by)
    stored_date = date.today().isoformat()  # authoritative key get_access_token reads

    # NEVER log the token value — only its length + the metadata.
    log.info("kite-token ingest: stored for %s (requested=%s, set_by=%s, token_len=%d)",
             stored_date, requested_date, set_by, len(token))

    return {
        "ok": True,
        "token_date": stored_date,
        "requested_token_date": requested_date,
        "stored": True,
    }


# ══════════════════════════════════════════════════════════════════════════════
# RTS (2026-07-19): Rupeezy (Vortex) daily access-token SYNC — laptop → cloud.
#
# The EXACT parallel of the Kite token-sync above, with ONE structural difference:
# the Rupeezy token is NOT a global daily row — it lives PER broker_account,
# VAULT-ENCRYPTED. The headless Vortex bot (services/vortex_auto_auth.py) mints it
# on the laptop and stores it on the ACTIVE rupeezy broker_account via
# autotrade.vault.store_access_token(). A cloud copy of this backend cannot mint
# (no Playwright / no login creds there), so this endpoint is the cloud INGEST:
# the laptop POSTs {broker_account_id, access_token} and the cloud persists it
# through the SAME vault store path, so the RupeezyBroker adapter's
# self.profile.access_token resolves it unchanged.
#
# SECURITY: reuses the SAME self-contained, constant-time, fail-CLOSED auth as
# every other publish route (X-Publish-Secret == env FALCON_PUBLISH_SECRET).
# REAL-MONEY-ADJACENT: the token authorises live orders. The token VALUE is NEVER
# logged or echoed — only its length, the account id, and the token_date.
#
# ADDITIVE: this ONLY adds a route. It does NOT change the adapter's request
# logic, the mint bot, or any execution path. The persist uses
# vault.store_access_token (no schema/insert duplication), which encrypts + writes
# access_token_enc + stamps status=ACTIVE + token_date on THAT account — exactly
# the row get_decrypted_creds() reads. Idempotent: a re-POST replaces the stored
# token for that account.
# ══════════════════════════════════════════════════════════════════════════════
class RupeezyTokenPush(BaseModel):
    broker_account_id: str
    access_token: str
    # Optional IST YYYY-MM-DD the token was minted; defaults to today IST on the
    # server. Stamped onto the account so _derive_status()'s date rule agrees.
    token_date: str | None = None
    # Advisory; the vault store always stamps ACTIVE (a freshly-minted token IS
    # active). Accepted for contract parity / forward-compat; not a downgrade path.
    status: str = "ACTIVE"


@router.post("/falcon/publish/rupeezy-token")
def publish_rupeezy_token(
    payload: RupeezyTokenPush,
    x_publish_secret: str | None = Header(default=None, alias="X-Publish-Secret"),
):
    """Ingest the daily Rupeezy (Vortex) access token minted on the laptop and
    persist it — VAULT-ENCRYPTED — on the given broker_account, so the cloud's
    RupeezyBroker adapter serves it. See the RTS block above.

    Body: { broker_account_id: str, access_token: str, token_date?: str (IST
            YYYY-MM-DD), status?: str }
    Returns: { ok: true, broker_account_id, token_date, stored: true }
    """
    # ── 1. Auth (self-contained, constant-time, fail-closed) — same as above ──
    _require_secret(x_publish_secret)

    # ── 2. Validate — reject empty ids/tokens (never store a blank) ───────────
    bid = (payload.broker_account_id or "").strip()
    if not bid:
        raise HTTPException(
            status_code=400,
            detail={"code": "EMPTY_ACCOUNT_ID",
                    "message": "broker_account_id is required."},
        )
    token = (payload.access_token or "").strip()
    if not token:
        raise HTTPException(
            status_code=400,
            detail={"code": "EMPTY_TOKEN",
                    "message": "access_token is empty or whitespace."},
        )

    token_date = (payload.token_date or "").strip() or datetime.now(IST).date().isoformat()

    # ── 3. Resolve the account + assert it is a RUPEEZY account ───────────────
    # Lazy import mirrors the kite path (autotrade/ on sys.path at runtime; keeps
    # this module importable in isolation). NEVER cross-store: a token for the
    # wrong broker on the wrong account is a real-money mis-route.
    from autotrade import vault

    acct = vault.get_account_public(bid)  # public dict, NO secrets; None if absent
    if acct is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "ACCOUNT_NOT_FOUND",
                    "message": f"No broker account {bid!r} exists on this server."},
        )
    if str(acct.get("broker") or "").lower() != "rupeezy":
        raise HTTPException(
            status_code=400,
            detail={"code": "NOT_A_RUPEEZY_ACCOUNT",
                    "message": f"Account {bid!r} is broker "
                               f"{acct.get('broker')!r}, not rupeezy — refusing to "
                               "store a Rupeezy token on it."},
        )

    # ── 4. Persist via the vault's OWN store path (the SAME fn the mint bot uses:
    # vortex_auto_auth._exchange_and_store → vault.store_access_token). Encrypts
    # access_token_enc, stamps status=ACTIVE + token_date on THIS account. ───────
    try:
        stored = vault.store_access_token(bid, token, token_date=token_date)
    except vault.VaultDisabledError:
        # Real-money-adjacent: with no vault key we CANNOT encrypt the token.
        # Fail closed (503) — never store a live-order token in plaintext.
        raise HTTPException(
            status_code=503,
            detail={"code": "VAULT_DISABLED",
                    "message": "FALCON_VAULT_KEY is not set on the server — "
                               "cannot store an encrypted Rupeezy token."},
        )
    if stored is None:
        # Race: the account vanished between the check and the write.
        raise HTTPException(
            status_code=404,
            detail={"code": "ACCOUNT_NOT_FOUND",
                    "message": f"Broker account {bid!r} not found at write time."},
        )

    # NEVER log the token value — only its length + the metadata.
    log.info("rupeezy-token ingest: stored for account=%s (token_date=%s, "
             "token_len=%d)", bid, token_date, len(token))

    return {
        "ok": True,
        "broker_account_id": bid,
        "token_date": token_date,
        "stored": True,
    }
