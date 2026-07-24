"""Monitor 3 — Execution-quality: slippage, rejects, EXIT_FAILED, stop overshoot.

DATA SOURCES (read-only DB; the durable order ledger + slippage table):
  * autotrade_slippage      — recent |slippage| distribution (p95, max).
  * autotrade_order_events  — recent REJECTED events (partial/rejected legs).
  * autotrade_positions     — current EXIT_FAILED rows (a failed exit = a stranded
    leg still held = the highest-value execution-quality signal).

These are the SAME tables execution/slippage.py and the reconciler already write;
this monitor only READS them. It never places, cancels, or inspects a live order
via the broker (no money-path touch). Signal→fill latency LEGS are not separately
timestamped per leg today (only entry/exit_latency_ms on the session), so
per-leg latency is reported UNKNOWN.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from oltp_db import oltp_conn as falcon_conn  # OLTP: SQLite(flag off)/Postgres(KANIDA_PG_ENABLED). pure-OLTP module.

from ..base import MonitorAgent
from ..signals import HealthSignal, Status, worse

IST = timezone(timedelta(hours=5, minutes=30))

_LOOKBACK_SEC = 24 * 3600
_SLIP_P95_WARN = 1.0     # % — retuned high-slippage shoulder
_SLIP_P95_ALERT = 2.0    # %
_REJECT_WARN = 3
_REJECT_ALERT = 10


def _p95(vals):
    if not vals:
        return None
    s = sorted(vals)
    idx = min(len(s) - 1, int(round(0.95 * (len(s) - 1))))
    return s[idx]


class ExecutionQualityMonitor(MonitorAgent):
    subsystem = "execution-quality"

    def observe(self, context=None) -> HealthSignal:
        cutoff = (datetime.now(IST) - timedelta(seconds=_LOOKBACK_SEC)).isoformat()
        metrics = {
            "signal_to_fill_ms": None,   # not timestamped per leg today
            "slippage_p95_pct": None,
            "slippage_max_pct": None,
            "n_fills_24h": 0,
            "n_rejected_24h": 0,
            "n_exit_failed_open": 0,
        }
        with falcon_conn() as con:
            slips = [abs(float(r["slippage_pct"])) for r in con.execute(
                "SELECT slippage_pct FROM autotrade_slippage WHERE recorded_at>=?",
                (cutoff,)).fetchall() if r["slippage_pct"] is not None]
            n_rejected = con.execute(
                "SELECT COUNT(*) AS n FROM autotrade_order_events "
                "WHERE event_type='REJECTED' AND ts>=?", (cutoff,)).fetchone()
            n_exit_failed = con.execute(
                "SELECT COUNT(*) AS n FROM autotrade_positions "
                "WHERE status='EXIT_FAILED'").fetchone()

        metrics["n_fills_24h"] = len(slips)
        metrics["slippage_p95_pct"] = round(_p95(slips), 4) if slips else None
        metrics["slippage_max_pct"] = round(max(slips), 4) if slips else None
        n_rej = int(dict(n_rejected)["n"]) if n_rejected else 0
        n_ef = int(dict(n_exit_failed)["n"]) if n_exit_failed else 0
        metrics["n_rejected_24h"] = n_rej
        metrics["n_exit_failed_open"] = n_ef

        status = Status.OK
        reasons = []
        # A currently-open EXIT_FAILED leg is money exposed → ALERT (the reconciler
        # + alert_monitor already page it as EXIT_FAILED; this is the aggregated
        # execution-quality view, deduped one incident up at the orchestrator).
        if n_ef > 0:
            status = worse(status, Status.ALERT)
            reasons.append(f"{n_ef} EXIT_FAILED open")
        if n_rej >= _REJECT_ALERT:
            status = worse(status, Status.ALERT)
            reasons.append(f"{n_rej} rejects/24h")
        elif n_rej >= _REJECT_WARN:
            status = worse(status, Status.WARN)
            reasons.append(f"{n_rej} rejects/24h")
        p95 = metrics["slippage_p95_pct"]
        if p95 is not None and p95 >= _SLIP_P95_ALERT:
            status = worse(status, Status.ALERT)
            reasons.append(f"slippage p95 {p95:.2f}%")
        elif p95 is not None and p95 >= _SLIP_P95_WARN:
            status = worse(status, Status.WARN)
            reasons.append(f"slippage p95 {p95:.2f}%")

        summary = ("execution nominal (no rejects/EXIT_FAILED; per-leg latency "
                   "not timestamped)" if status == Status.OK
                   else "; ".join(reasons))
        return self._signal(status, summary, metrics)
