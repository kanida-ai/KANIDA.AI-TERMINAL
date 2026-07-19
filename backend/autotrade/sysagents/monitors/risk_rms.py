"""Monitor 7 — Risk/RMS: panel↔broker divergence, stuck EXIT_FAILED, naked.

DATA SOURCE (read-only): the RECONCILIATION FRAMEWORK's OWN observability outputs
+ the alert ledger — the tables the reconciler / alert_monitor already write:
  * autotrade_recon_alerts — UNATTRIBUTED_CLOSE / ORPHAN_AT_BROKER /
    CORP_ACTION_SUSPECTED divergences it flagged (never auto-corrected) TODAY.
  * autotrade_positions    — stuck EXIT_FAILED rows (a leg the exit could not
    flatten — still held at the broker).
  * autotrade_alerts       — UNACKED Tier-3 money-path pages (NAKED_POSITION,
    KILLING_INCOMPLETE, RECONCILED_FLAT_NO_EXIT_ORDER, DOUBLE_FILL, ...).

This monitor RE-READS those durable findings — it does NOT run the reconciler,
fetch a broker book, or place/inspect any order. SLO (§8): any unresolved
broker/DB divergence in-hours = page. We surface it as an ALERT and let the
orchestrator collapse it into one incident.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from falcon.db import falcon_conn

from ..base import MonitorAgent
from ..signals import HealthSignal, Status, worse

IST = timezone(timedelta(hours=5, minutes=30))

# Tier-3 money-path alert kinds (mirror of alert_monitor._NEVER_AUTO_ACK).
_TIER3_KINDS = (
    "EXIT_FAILED", "KILLING_INCOMPLETE", "NAKED_POSITION", "MANUAL_CONFLICT",
    "UNATTRIBUTED_CLOSE", "ORPHAN_AT_BROKER", "DOUBLE_FILL",
    "RECONCILED_FLAT_NO_EXIT_ORDER", "DAILY_LOSS_BREAKER",
)


class RiskRmsMonitor(MonitorAgent):
    subsystem = "risk-rms"

    def observe(self, context=None) -> HealthSignal:
        today = datetime.now(IST).date().isoformat()
        metrics = {
            "recon_divergences_today": 0,
            "exit_failed_open": 0,
            "unacked_tier3_alerts": 0,
        }
        with falcon_conn() as con:
            div = con.execute(
                "SELECT COUNT(*) AS n FROM autotrade_recon_alerts "
                "WHERE substr(ts,1,10)=?", (today,)).fetchone()
            ef = con.execute(
                "SELECT COUNT(*) AS n FROM autotrade_positions "
                "WHERE status='EXIT_FAILED'").fetchone()
            t3 = con.execute(
                f"""SELECT COUNT(*) AS n FROM autotrade_alerts
                    WHERE acknowledged=0 AND kind IN
                    ({','.join('?' * len(_TIER3_KINDS))})""",
                _TIER3_KINDS).fetchone()

        n_div = int(dict(div)["n"]) if div else 0
        n_ef = int(dict(ef)["n"]) if ef else 0
        n_t3 = int(dict(t3)["n"]) if t3 else 0
        metrics.update({
            "recon_divergences_today": n_div,
            "exit_failed_open": n_ef,
            "unacked_tier3_alerts": n_t3,
        })

        status = Status.OK
        reasons = []
        if n_t3 > 0:
            status = worse(status, Status.CRITICAL)
            reasons.append(f"{n_t3} unacked Tier-3 alert(s)")
        if n_ef > 0:
            status = worse(status, Status.ALERT)
            reasons.append(f"{n_ef} EXIT_FAILED")
        if n_div > 0:
            status = worse(status, Status.ALERT)
            reasons.append(f"{n_div} recon divergence(s) today")

        summary = ("no divergence / stuck exits / unacked money-path alerts"
                   if status == Status.OK else "; ".join(reasons))
        return self._signal(status, summary, metrics)
