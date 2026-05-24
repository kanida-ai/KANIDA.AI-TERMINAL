"""Reconciles falcon_trade_orders.filled_qty against kite.orders() truth.

Called once per monitor poll. Single kite.orders() call covers the whole book —
cheap. Updates filled_qty for every still-active order row and emits a
PARTIAL_FILL event the first time a partial gets detected (so it lands in the
trigger feed and the operator sees it).

Why care about partial fills:
  - ENTRY (MARKET) on liquid NSE EQ — partials are rare but possible on illiquid.
  - STOP (SL-L) — partials happen when SL fires into a thin book (post-circuit).
  - EXIT (MARKET) — same as ENTRY.

When STOP partially fills, the remaining qty is left without protection. Today
we only TRACK + ALERT; auto-replacement of the unprotected stub is a follow-up
(noted in events as PARTIAL_FILL, severity=warn so the trigger feed surfaces it).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from ...db import falcon_conn

log = logging.getLogger("kanida.falcon.trade.fill_reconciler")
IST = timezone(timedelta(hours=5, minutes=30))


# Kite order statuses that imply some quantity may have moved.
_FILL_STATUSES = {"COMPLETE", "OPEN", "TRIGGER PENDING", "MODIFIED"}


def reconcile(kite) -> Dict[str, Any]:
    """Pull latest kite.orders() and update filled_qty for every Falcon order
    we still track as PLACED/PARTIAL. Returns a summary dict."""
    # Grab all rows we care about — cheap, this table is small.
    with falcon_conn() as con:
        rows = con.execute("""
            SELECT id, symbol, role, kite_order_id, qty, filled_qty, status
            FROM falcon_trade_orders
            WHERE kite_order_id IS NOT NULL
              AND status IN ('PLACED', 'PARTIAL', 'PLACING')
        """).fetchall()
    if not rows:
        return {"checked": 0, "updated": 0, "partials": 0, "completes": 0}

    by_kite_id: Dict[str, Dict[str, Any]] = {str(r["kite_order_id"]): dict(r) for r in rows}

    try:
        kite_orders = kite.orders()
    except Exception as e:
        log.warning("fill_reconciler: kite.orders() failed: %s", e)
        return {"checked": 0, "updated": 0, "partials": 0, "completes": 0,
                "error": str(e)}

    updates: List[Dict[str, Any]] = []
    new_partials: List[Dict[str, Any]] = []
    new_completes: List[Dict[str, Any]] = []
    now_iso = datetime.now(IST).isoformat()

    for ko in kite_orders:
        kid = str(ko.get("order_id") or "")
        if kid not in by_kite_id:
            continue
        row = by_kite_id[kid]
        kite_status = (ko.get("status") or "").upper()
        filled = int(ko.get("filled_quantity") or 0)
        ordered = int(row["qty"] or 0)
        prior_filled = int(row["filled_qty"] or 0)
        prior_status = (row["status"] or "").upper()

        # Decide our internal status.
        if kite_status == "COMPLETE" and filled >= ordered:
            new_status = "PLACED"   # fully complete — keep PLACED for compat with downstream
        elif filled > 0 and filled < ordered:
            new_status = "PARTIAL"
        elif kite_status in ("REJECTED", "CANCELLED"):
            new_status = kite_status
        elif kite_status not in _FILL_STATUSES:
            # Don't downgrade — leave the row as-is
            continue
        else:
            new_status = prior_status

        # Anything to update?
        if filled == prior_filled and new_status == prior_status:
            continue

        updates.append({
            "id":         row["id"],
            "filled_qty": filled,
            "status":     new_status,
            "now_iso":    now_iso,
        })
        # First-time partial detection
        if new_status == "PARTIAL" and prior_status != "PARTIAL":
            new_partials.append({
                "symbol":   row["symbol"],
                "role":     row["role"],
                "filled":   filled,
                "ordered":  ordered,
                "kite_id":  kid,
            })
        # First-time full completion
        if new_status == "PLACED" and prior_status != "PLACED" and filled >= ordered and ordered > 0:
            # PLACED is also the "just got placed by us" status, so guard on prior_filled too.
            if prior_filled < ordered:
                new_completes.append({
                    "symbol":   row["symbol"],
                    "role":     row["role"],
                    "filled":   filled,
                    "kite_id":  kid,
                })

    if updates:
        with falcon_conn() as con:
            con.executemany("""
                UPDATE falcon_trade_orders
                SET filled_qty = ?, status = ?, last_reconciled_at = ?
                WHERE id = ?
            """, [(u["filled_qty"], u["status"], u["now_iso"], u["id"]) for u in updates])
            con.commit()

    # Emit events for partials so the operator sees them in the trigger feed.
    # Imported lazily to avoid circular imports at module load.
    if new_partials or new_completes:
        from . import position_monitor
        for p in new_partials:
            position_monitor.log_event(
                p["symbol"], "PARTIAL_FILL", "warn",
                f"{p['role']} partial fill: {p['filled']}/{p['ordered']} (kite_id={p['kite_id']})",
                auto_action_taken=False, related_kite_id=p["kite_id"],
            )
        for c in new_completes:
            # Severity info — completion is good news but worth surfacing once.
            position_monitor.log_event(
                c["symbol"], "ORDER_FILLED", "info",
                f"{c['role']} fully filled: {c['filled']} (kite_id={c['kite_id']})",
                auto_action_taken=False, related_kite_id=c["kite_id"],
            )

    return {
        "checked":   len(by_kite_id),
        "updated":   len(updates),
        "partials":  len(new_partials),
        "completes": len(new_completes),
    }
