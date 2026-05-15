"""Kite ↔ Falcon DB reconciliation.

Fact of life on Zerodha: Day-validity SL/SL-M orders are auto-cancelled at
15:30 IST. So Falcon's `falcon_position_state.sl_kite_order_id` and the
matching `falcon_trade_orders` row become stale every evening — the order
no longer exists at Kite even though our DB still says PLACED / managed_by='falcon'.

Without reconciliation, three problems:
  1. /premarket modal hides re-adopt rows ("already adopted") that aren't
     actually adopted anymore.
  2. trail_manager.execute_replace_sl tries to modify_order on a dead ID
     and fails on every poll.
  3. positions look "protected" in /falcon/positions UI but have no actual SL.

This service runs at EOD (and on demand) to:
  * Pull live kite.orders() → set of order_ids that still exist
  * For every falcon_position_state row with managed_by='falcon' and a
    sl_kite_order_id NOT in that set: flip to managed_by='external',
    clear sl_kite_order_id, clear current_sl_price.
  * Mark the corresponding falcon_trade_orders row as CANCELLED (audit sync).

Failure mode: if kite.orders() fails, the function aborts cleanly without
touching DB state — never mark positions external on a transient API error.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Set

from ...db import falcon_conn

log = logging.getLogger("kanida.falcon.trade.reconcile")
IST = timezone(timedelta(hours=5, minutes=30))

# Kite statuses where the order is still "alive" — ours to manage.
ALIVE_STATUSES = {"OPEN", "TRIGGER PENDING"}


def _live_kite_order_ids(kite) -> Set[str]:
    """Set of Kite order_ids that are currently alive (not cancelled/filled).
    Returns empty set on API failure (caller treats as "could not verify"
    and aborts to avoid false positives)."""
    try:
        all_orders = kite.orders() or []
    except Exception as e:
        log.warning("reconcile: kite.orders() failed: %s — aborting reconcile", e)
        return set()
    alive: Set[str] = set()
    for o in all_orders:
        oid = o.get("order_id")
        st  = o.get("status")
        if oid and st in ALIVE_STATUSES:
            alive.add(str(oid))
    return alive


def reconcile_managed_positions(kite, dry_run: bool = False) -> Dict[str, Any]:
    """Scan falcon_position_state, flip stale managed_by='falcon' rows to
    'external'. Returns {n_managed, n_alive, n_stale, stale_symbols, ...}.

    `dry_run=True` reports what would change without mutating.
    """
    started_at = datetime.now(IST).isoformat()
    live_ids = _live_kite_order_ids(kite)

    if not live_ids:
        # Either no alive orders OR API failed. We can't distinguish here, so
        # abort to be safe — never flip everything to external on a fluke.
        log.warning("reconcile: kite.orders() returned 0 alive orders — refusing to mass-flip. "
                    "If Zerodha really has no orders (e.g. post-EOD), set explicit_empty=True.")
        return {
            "started_at":  started_at,
            "n_managed":   None,
            "n_alive":     0,
            "n_stale":     0,
            "stale_symbols": [],
            "skipped":     "ABORTED_NO_ALIVE_KITE_ORDERS",
        }

    return _do_reconcile(live_ids, dry_run=dry_run, started_at=started_at)


def reconcile_force_all_stale(kite, dry_run: bool = False) -> Dict[str, Any]:
    """Explicit operator-confirmed reconcile when Kite has zero alive orders
    (typical post-EOD state). Flips ALL managed_by='falcon' rows whose
    sl_kite_order_id is non-null to external.

    Use this after Zerodha's 15:30 wipe when you KNOW nothing is alive."""
    started_at = datetime.now(IST).isoformat()
    return _do_reconcile(set(), dry_run=dry_run, started_at=started_at,
                          force_empty=True)


def _do_reconcile(live_ids: Set[str], *, dry_run: bool, started_at: str,
                   force_empty: bool = False) -> Dict[str, Any]:
    """Core reconcile logic — extracted so reconcile_managed_positions and
    reconcile_force_all_stale share it."""
    with falcon_conn() as con:
        rows = con.execute("""
            SELECT symbol, sl_kite_order_id, current_sl_price
              FROM falcon_position_state
             WHERE managed_by='falcon' AND sl_kite_order_id IS NOT NULL
        """).fetchall()
        managed = [dict(r) for r in rows]

        stale: List[Dict[str, Any]] = []
        alive: List[str] = []
        for r in managed:
            oid = str(r.get("sl_kite_order_id") or "")
            if not oid:
                continue
            if force_empty:
                stale.append(r)
            elif oid not in live_ids:
                stale.append(r)
            else:
                alive.append(r["symbol"])

        if dry_run or not stale:
            return {
                "started_at":    started_at,
                "n_managed":     len(managed),
                "n_alive":       len(alive),
                "n_stale":       len(stale),
                "stale_symbols": [r["symbol"] for r in stale],
                "alive_symbols": alive,
                "dry_run":       dry_run,
            }

        stale_syms = [r["symbol"] for r in stale]
        stale_oids = [str(r["sl_kite_order_id"]) for r in stale if r.get("sl_kite_order_id")]

        # Flip falcon_position_state: managed_by='external', clear SL pointers
        ph_syms = ",".join("?" * len(stale_syms))
        con.execute(f"""
            UPDATE falcon_position_state
               SET managed_by='external',
                   sl_kite_order_id=NULL,
                   trail_active=0
             WHERE symbol IN ({ph_syms})
        """, stale_syms)

        # Sync falcon_trade_orders: PLACED rows pointing at dead IDs → CANCELLED
        n_audit = 0
        if stale_oids:
            ph_oids = ",".join("?" * len(stale_oids))
            cur = con.execute(f"""
                UPDATE falcon_trade_orders
                   SET status='CANCELLED', error='kite_eod_cancelled_or_stale'
                 WHERE role='STOP' AND status IN ('PLACED','PENDING')
                   AND kite_order_id IN ({ph_oids})
            """, stale_oids)
            n_audit = cur.rowcount or 0
        con.commit()

        log.info("reconcile: flipped %d positions to external (audit-cancelled %d trade_orders rows). "
                  "Stale symbols: %s",
                  len(stale_syms), n_audit, stale_syms)

        return {
            "started_at":      started_at,
            "n_managed":       len(managed),
            "n_alive":         len(alive),
            "n_stale":         len(stale_syms),
            "stale_symbols":   stale_syms,
            "alive_symbols":   alive,
            "n_audit_cancelled": n_audit,
            "force_empty":     force_empty,
            "dry_run":         False,
        }
