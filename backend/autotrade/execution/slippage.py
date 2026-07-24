"""Slippage recording + report (spec 5.3).

record_slippage writes a row to autotrade_slippage and fires an alert when
|slippage| exceeds the high-slippage threshold. Pure DB + alert; no order side
effects.

ALERT AUTO-TRIAGE — TIER 1(c) (default-OFF behind FALCON_AUTOTRADE_AUTO_TRIAGE).
The legacy alert here was pure noise by construction:
  * It fired at |slippage| > 0.5%, which is EXACTLY p90 of observed fills
    (median 0.111%, p90 0.487%) → it fired on 10.4% of every fill. A threshold
    sitting on top of the distribution's shoulder is not an anomaly detector.
  * It had kind="GENERIC" and NO session_id, so the resulting row was
    untriageable: you could not tell what fired it or which session it belonged
    to, and it could not be deduped per session.
Under the flag it becomes a real, attributable, INFO-severity signal:
  kind="HIGH_SLIPPAGE" + session_id + symbol, at a retuned threshold far above
  p90 (default 1.5%, env FALCON_AUTOTRADE_HIGH_SLIPPAGE_PCT).

⚠️ ASSUMPTION (flagged): 1.5% is chosen as ~3× p90 / ~13× median from the fill
distribution quoted above — it is a REASONED default, not a fitted one (I did not
re-derive the distribution from the live table). It is env-tunable precisely so
the operator can move it on real data without a deploy.

FLAG-OFF is byte-identical to the pre-triage behaviour: threshold 0.5,
kind GENERIC, severity warn, no session_id, same message string.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from oltp_db import oltp_conn as falcon_conn  # OLTP: SQLite(flag off)/Postgres(KANIDA_PG_ENABLED). pure-OLTP.

log = logging.getLogger("kanida.autotrade.execution.slippage")
IST = timezone(timedelta(hours=5, minutes=30))

# LEGACY threshold (%) — kept as the module constant so any existing importer /
# test reads exactly what it read before. Used verbatim when the triage flag is
# off. This is the p90-of-fills value that made the alert fire on 10.4% of fills.
HIGH_SLIPPAGE_PCT = 0.5

# RETUNED threshold (%) used ONLY under the triage flag. Well above p90 (0.487)
# so it flags a genuine execution anomaly instead of the top decile of normal.
HIGH_SLIPPAGE_PCT_RETUNED = 1.5


def _high_slippage_threshold() -> float:
    """The active |slippage| % threshold. Flag OFF → the legacy 0.5 (byte-for-byte
    unchanged). Flag ON → FALCON_AUTOTRADE_HIGH_SLIPPAGE_PCT, default 1.5.
    A non-numeric / non-positive env value falls back to the retuned default."""
    try:
        from ..alerts import auto_triage_enabled
        if not auto_triage_enabled():
            return HIGH_SLIPPAGE_PCT
    except Exception:  # pragma: no cover - defensive: never break the fill path
        return HIGH_SLIPPAGE_PCT
    try:
        v = float(os.environ.get("FALCON_AUTOTRADE_HIGH_SLIPPAGE_PCT",
                                 str(HIGH_SLIPPAGE_PCT_RETUNED)).strip())
        return v if v > 0 else HIGH_SLIPPAGE_PCT_RETUNED
    except (ValueError, AttributeError, TypeError):
        return HIGH_SLIPPAGE_PCT_RETUNED


def record_slippage(symbol: str, expected_price: float, actual_price: float,
                    qty: int, session_id: Optional[str] = None,
                    broker_profile: Optional[str] = None) -> float:
    if expected_price <= 0:
        slippage_pct = 0.0
    else:
        slippage_pct = (actual_price - expected_price) / expected_price * 100
    with falcon_conn() as con:
        con.execute(
            """INSERT INTO autotrade_slippage
               (session_id, broker_profile, symbol, expected_price, actual_price,
                slippage_pct, qty, recorded_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (session_id, broker_profile, symbol, expected_price, actual_price,
             slippage_pct, qty, datetime.now(IST).isoformat()),
        )
        con.commit()
    _threshold = _high_slippage_threshold()
    if abs(slippage_pct) > _threshold:
        from ..alerts import send, auto_triage_enabled
        if auto_triage_enabled():
            # TIER 1(c): a REAL kind + a session_id + INFO severity. The kind
            # makes it triageable and dedupable; the session_id makes it
            # attributable; info severity keeps it off the urgent page path
            # (send() only pushes for critical/urgent) — high slippage is
            # execution telemetry, not a manual-intervention event.
            send(f"High slippage on {symbol}: {slippage_pct:.2f}% "
                 f"(threshold {_threshold:.2f}%, qty={qty}, "
                 f"expected={expected_price}, actual={actual_price})",
                 severity="info", kind="HIGH_SLIPPAGE", session_id=session_id,
                 symbol=symbol)
        else:
            # Byte-for-byte the pre-triage call: GENERIC / warn / no session.
            send(f"High slippage on {symbol}: {slippage_pct:.2f}%",
                 severity="warn")
    return slippage_pct


def slippage_report(session_id: str) -> List[Dict[str, Any]]:
    with falcon_conn() as con:
        rows = con.execute(
            "SELECT * FROM autotrade_slippage WHERE session_id=? "
            "ORDER BY recorded_at DESC", (session_id,)
        ).fetchall()
    return [dict(r) for r in rows]
