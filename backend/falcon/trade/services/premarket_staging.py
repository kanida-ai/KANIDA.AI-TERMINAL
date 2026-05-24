"""Pre-market staging table CRUD.

EOD orchestrator stages new-entry + bulk-adopt items here at 16:05 IST.
User reviews + confirms in /falcon/premarket; deployer fires at 9:15 next day.

Lifecycle:
    STAGED   ← EOD orchestrator inserted it
    QUEUED   ← user clicked Confirm & Schedule
    DEPLOYED ← deployer placed it on Kite (terminal)
    FAILED   ← deployer tried, Kite/exchange rejected (terminal)
    CANCELLED ← user removed it before deployment (terminal)

Idempotency: UNIQUE (target_date, kind, symbol). Re-staging the same symbol on
a re-run uses INSERT OR REPLACE for STAGED rows; never overwrites a terminal row.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ...db import falcon_conn

log = logging.getLogger("kanida.falcon.trade.premarket")
IST = timezone(timedelta(hours=5, minutes=30))

VALID_KINDS = {"NEW_ENTRY", "BULK_ADOPT"}
# DEPLOYED + FAILED are terminal Kite outcomes (broker has acted) — never overwrite.
# CANCELLED is user-pulled-back — re-stage IS allowed (user can bring it back).
TERMINAL_STATUSES = {"DEPLOYED", "FAILED"}
PENDING_STATUSES = {"STAGED", "QUEUED"}


def _now_ist() -> str:
    return datetime.now(IST).isoformat()


def stage_item(*, target_date: str, kind: str, symbol: str,
                payload: Dict[str, Any]) -> Dict[str, Any]:
    """Insert (or replace if a non-terminal row exists) a staging item.

    If a terminal row already exists for (target_date, kind, symbol), this is
    a no-op (returns the existing row) — we never overwrite history."""
    if kind not in VALID_KINDS:
        raise ValueError(f"Invalid kind '{kind}'. Must be one of {sorted(VALID_KINDS)}")
    payload_json = json.dumps(payload, default=str, sort_keys=True)
    now = _now_ist()
    with falcon_conn() as con:
        existing = con.execute("""
            SELECT id, status FROM falcon_premarket_staging
             WHERE target_date=? AND kind=? AND symbol=?
        """, (target_date, kind, symbol)).fetchone()
        if existing and existing["status"] in TERMINAL_STATUSES:
            return _row_to_dict(con.execute(
                "SELECT * FROM falcon_premarket_staging WHERE id=?",
                (existing["id"],)).fetchone())
        if existing:
            con.execute("""
                UPDATE falcon_premarket_staging
                   SET payload_json=?, staged_at=?, status='STAGED',
                       confirmed_at=NULL, deployed_at=NULL, deploy_error=NULL
                 WHERE id=?
            """, (payload_json, now, existing["id"]))
            row_id = existing["id"]
        else:
            cur = con.execute("""
                INSERT INTO falcon_premarket_staging
                  (staged_at, target_date, kind, symbol, payload_json, status)
                VALUES (?, ?, ?, ?, ?, 'STAGED')
            """, (now, target_date, kind, symbol, payload_json))
            row_id = cur.lastrowid
        con.commit()
        return _row_to_dict(con.execute(
            "SELECT * FROM falcon_premarket_staging WHERE id=?", (row_id,)).fetchone())


def list_for_date(target_date: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
    """All staging items for a target_date, optionally filtered by status.
    Sorted: NEW_ENTRY first, then BULK_ADOPT, both alphabetical by symbol."""
    sql = "SELECT * FROM falcon_premarket_staging WHERE target_date=?"
    params: List[Any] = [target_date]
    if status:
        sql += " AND status=?"
        params.append(status)
    sql += " ORDER BY CASE kind WHEN 'NEW_ENTRY' THEN 0 ELSE 1 END, symbol"
    with falcon_conn() as con:
        rows = con.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


def list_pending(target_date: Optional[str] = None) -> List[Dict[str, Any]]:
    """STAGED + QUEUED items. If target_date is None, returns ALL pending
    across all dates (used by deployer to find work to do today)."""
    sql = "SELECT * FROM falcon_premarket_staging WHERE status IN ('STAGED','QUEUED')"
    params: List[Any] = []
    if target_date:
        sql += " AND target_date=?"
        params.append(target_date)
    sql += " ORDER BY target_date, CASE kind WHEN 'NEW_ENTRY' THEN 0 ELSE 1 END, symbol"
    with falcon_conn() as con:
        rows = con.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_item(item_id: int) -> Optional[Dict[str, Any]]:
    with falcon_conn() as con:
        row = con.execute("SELECT * FROM falcon_premarket_staging WHERE id=?",
                          (item_id,)).fetchone()
    return _row_to_dict(row) if row else None


def confirm_items(item_ids: List[int]) -> int:
    """STAGED → QUEUED. Returns count actually transitioned."""
    if not item_ids:
        return 0
    placeholders = ",".join("?" * len(item_ids))
    now = _now_ist()
    with falcon_conn() as con:
        cur = con.execute(f"""
            UPDATE falcon_premarket_staging
               SET status='QUEUED', confirmed_at=?
             WHERE id IN ({placeholders}) AND status='STAGED'
        """, [now, *item_ids])
        con.commit()
    n = cur.rowcount or 0
    log.info("premarket: confirmed %d items (requested %d)", n, len(item_ids))
    return n


def confirm_all_for_date(target_date: str) -> int:
    """STAGED → QUEUED for every STAGED item on this date. Returns count."""
    now = _now_ist()
    with falcon_conn() as con:
        cur = con.execute("""
            UPDATE falcon_premarket_staging
               SET status='QUEUED', confirmed_at=?
             WHERE target_date=? AND status='STAGED'
        """, (now, target_date))
        con.commit()
    n = cur.rowcount or 0
    log.info("premarket: confirm_all_for_date(%s) → %d items queued", target_date, n)
    return n


def cancel_items(item_ids: List[int]) -> int:
    """Hard DELETE rows that are still in STAGED|QUEUED.

    Per operator preference (2026-05-08): cancel = remove from list.
    Leaving soft-CANCELLED rows around clutters the queue view and serves
    no operational purpose — actual deploy attempts are logged in
    falcon_trade_events for audit. Terminal rows (DEPLOYED/FAILED) are
    preserved here as the deploy history."""
    if not item_ids:
        return 0
    placeholders = ",".join("?" * len(item_ids))
    with falcon_conn() as con:
        cur = con.execute(f"""
            DELETE FROM falcon_premarket_staging
             WHERE id IN ({placeholders}) AND status IN ('STAGED','QUEUED')
        """, item_ids)
        con.commit()
    return cur.rowcount or 0


def purge_cancelled_legacy() -> int:
    """One-shot cleanup: delete all rows with status='CANCELLED' (legacy soft-marks
    from before the cancel-is-hard-delete change). Safe to call repeatedly."""
    with falcon_conn() as con:
        cur = con.execute("DELETE FROM falcon_premarket_staging WHERE status='CANCELLED'")
        con.commit()
    return cur.rowcount or 0


def update_payload(item_id: int, payload_patch: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Edit the payload of a STAGED item — e.g. operator overrides SL price.
    Forbidden once status moves past STAGED (Confirm freezes the payload)."""
    with falcon_conn() as con:
        row = con.execute("SELECT * FROM falcon_premarket_staging WHERE id=?",
                          (item_id,)).fetchone()
        if not row:
            return None
        if row["status"] != "STAGED":
            raise ValueError(f"Cannot edit item {item_id}: status={row['status']} (only STAGED is editable)")
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        payload.update(payload_patch)
        con.execute("UPDATE falcon_premarket_staging SET payload_json=? WHERE id=?",
                    (json.dumps(payload, default=str, sort_keys=True), item_id))
        con.commit()
        updated = con.execute("SELECT * FROM falcon_premarket_staging WHERE id=?",
                              (item_id,)).fetchone()
    return _row_to_dict(updated)


def mark_deployed(item_id: int, result: Dict[str, Any]) -> None:
    with falcon_conn() as con:
        con.execute("""
            UPDATE falcon_premarket_staging
               SET status='DEPLOYED', deployed_at=?, deploy_result=?, deploy_error=NULL
             WHERE id=?
        """, (_now_ist(), json.dumps(result, default=str), item_id))
        con.commit()


def mark_failed(item_id: int, error: str) -> None:
    with falcon_conn() as con:
        con.execute("""
            UPDATE falcon_premarket_staging
               SET status='FAILED', deployed_at=?, deploy_error=?
             WHERE id=?
        """, (_now_ist(), error[:1000], item_id))
        con.commit()


def purge_old(days: int = 14) -> int:
    """Delete terminal rows older than `days` days. Keeps audit short."""
    cutoff = (datetime.now(IST) - timedelta(days=days)).date().isoformat()
    with falcon_conn() as con:
        cur = con.execute("""
            DELETE FROM falcon_premarket_staging
             WHERE status IN ('DEPLOYED','FAILED','CANCELLED')
               AND target_date < ?
        """, (cutoff,))
        con.commit()
    return cur.rowcount or 0


def summary_for_date(target_date: str) -> Dict[str, Any]:
    """Counts by (kind, status). Used by the page header."""
    with falcon_conn() as con:
        rows = con.execute("""
            SELECT kind, status, COUNT(*) AS n
              FROM falcon_premarket_staging
             WHERE target_date=?
          GROUP BY kind, status
        """, (target_date,)).fetchall()
    out: Dict[str, Dict[str, int]] = {}
    for r in rows:
        out.setdefault(r["kind"], {})[r["status"]] = r["n"]
    return out


def _row_to_dict(row) -> Dict[str, Any]:
    if row is None:
        return {}
    d = dict(row)
    payload_json = d.pop("payload_json", None)
    try:
        d["payload"] = json.loads(payload_json) if payload_json else {}
    except Exception:
        d["payload"] = {}
    deploy_result = d.pop("deploy_result", None)
    if deploy_result:
        try:
            d["deploy_result"] = json.loads(deploy_result)
        except Exception:
            d["deploy_result"] = deploy_result
    return d
