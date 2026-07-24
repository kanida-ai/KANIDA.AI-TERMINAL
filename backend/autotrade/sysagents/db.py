"""Persistence for the health layer — self-contained, additive, idempotent.

Two NEW tables (never a trading table). Created via the same falcon_conn +
CREATE TABLE IF NOT EXISTS pattern the AutoTrade migrations use, but kept in THIS
package so the whole layer is self-contained: the tables come into existence only
when the layer first runs (persistence / the endpoint call ensure_tables()), and
nothing outside sysagents ever writes them.

  * sysagent_health_snapshots — one row per orchestrator run: the correlated
    overall status + the per-subsystem signal JSON (history / charts / audit).
  * sysagent_incidents        — one row per correlated incident the orchestrator
    raised (root cause, severity, impacted subsystems, whether it paged, the
    LLM-or-deterministic provenance).

DATA-ISOLATION: writes ONLY these two tables. Never reads/writes
falcon_position_state or any autotrade_* / order / position table.
NO SECRETS: only sanitized signal metrics are ever persisted (the orchestrator
sanitizes before it reaches here).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from oltp_db import oltp_conn as falcon_conn  # OLTP: SQLite(flag off)/Postgres(KANIDA_PG_ENABLED). pure-OLTP.

log = logging.getLogger("kanida.sysagents.db")
IST = timezone(timedelta(hours=5, minutes=30))

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sysagent_health_snapshots (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    ts             TEXT NOT NULL,            -- ISO IST of the run
    overall_status TEXT NOT NULL,           -- OK|WARN|ALERT|CRITICAL|UNKNOWN
    n_signals      INTEGER NOT NULL DEFAULT 0,
    n_unknown      INTEGER NOT NULL DEFAULT 0,
    incident_id    TEXT,                     -- FK-ish into sysagent_incidents (may be NULL)
    signals_json   TEXT NOT NULL             -- JSON list of sanitized HealthSignal dicts
);
CREATE INDEX IF NOT EXISTS idx_sysagent_snapshots_ts
    ON sysagent_health_snapshots(ts DESC);

CREATE TABLE IF NOT EXISTS sysagent_incidents (
    incident_id   TEXT PRIMARY KEY,          -- stable signature hash
    ts            TEXT NOT NULL,             -- ISO IST first-seen
    severity      TEXT NOT NULL,             -- OK|WARN|ALERT|CRITICAL
    root_cause    TEXT,                      -- one-line root cause
    summary       TEXT,                      -- human-readable page body
    impacted      TEXT,                      -- JSON list of subsystem names
    source        TEXT NOT NULL DEFAULT 'deterministic',  -- 'llm' | 'deterministic'
    paged         INTEGER NOT NULL DEFAULT 0,
    alert_id      INTEGER,                   -- autotrade_alerts.id if it paged
    detail_json   TEXT
);
CREATE INDEX IF NOT EXISTS idx_sysagent_incidents_ts
    ON sysagent_incidents(ts DESC);
"""

_ENSURED = False


def ensure_tables() -> None:
    """Create the two tables if absent (idempotent). Cheap; called before any
    read/write. Never raises into the caller."""
    global _ENSURED
    if _ENSURED:
        return
    try:
        with falcon_conn() as con:
            con.executescript(_SCHEMA_SQL)
            con.commit()
        _ENSURED = True
    except Exception as e:  # noqa: BLE001
        log.warning("sysagents.db: ensure_tables failed: %s", e)


def persist_snapshot(*, overall_status: str, signals: List[Dict[str, Any]],
                     incident_id: Optional[str], now: Optional[datetime] = None
                     ) -> Optional[int]:
    """Insert one health-snapshot row. Returns the row id, or None on failure."""
    ensure_tables()
    n_unknown = sum(1 for s in signals if s.get("status") == "UNKNOWN")
    ts = (now or datetime.now(IST)).isoformat()
    try:
        with falcon_conn() as con:
            cur = con.execute(
                """INSERT INTO sysagent_health_snapshots
                   (ts, overall_status, n_signals, n_unknown, incident_id,
                    signals_json)
                   VALUES (?,?,?,?,?,?)""",
                (ts, overall_status, len(signals), n_unknown, incident_id,
                 json.dumps(signals)[:200000]))
            con.commit()
            return cur.lastrowid
    except Exception as e:  # noqa: BLE001
        log.warning("sysagents.db: persist_snapshot failed: %s", e)
        return None


def persist_incident(*, incident_id: str, severity: str, root_cause: str,
                     summary: str, impacted: List[str], source: str,
                     paged: bool, alert_id: Optional[int],
                     detail: Optional[Dict[str, Any]] = None,
                     now: Optional[datetime] = None) -> None:
    """Upsert one incident row keyed by its stable signature. A repeat of the same
    incident refreshes ts/severity/paged rather than duplicating. Never raises."""
    ensure_tables()
    ts = (now or datetime.now(IST)).isoformat()
    try:
        with falcon_conn() as con:
            con.execute(
                """INSERT INTO sysagent_incidents
                   (incident_id, ts, severity, root_cause, summary, impacted,
                    source, paged, alert_id, detail_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(incident_id) DO UPDATE SET
                     ts=excluded.ts, severity=excluded.severity,
                     root_cause=excluded.root_cause, summary=excluded.summary,
                     impacted=excluded.impacted, source=excluded.source,
                     paged=MAX(sysagent_incidents.paged, excluded.paged),
                     alert_id=COALESCE(excluded.alert_id, sysagent_incidents.alert_id),
                     detail_json=excluded.detail_json""",
                (incident_id, ts, severity, root_cause, (summary or "")[:2000],
                 json.dumps(impacted), source, 1 if paged else 0, alert_id,
                 json.dumps(detail or {})[:20000]))
            con.commit()
    except Exception as e:  # noqa: BLE001
        log.warning("sysagents.db: persist_incident failed: %s", e)


def latest_snapshot() -> Optional[Dict[str, Any]]:
    """The most recent snapshot row (+ its incident, if any), or None. Read-only."""
    ensure_tables()
    try:
        with falcon_conn() as con:
            snap = con.execute(
                "SELECT * FROM sysagent_health_snapshots ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if snap is None:
                return None
            snap = dict(snap)
            inc = None
            if snap.get("incident_id"):
                r = con.execute(
                    "SELECT * FROM sysagent_incidents WHERE incident_id=?",
                    (snap["incident_id"],)).fetchone()
                inc = dict(r) if r else None
    except Exception as e:  # noqa: BLE001
        log.warning("sysagents.db: latest_snapshot failed: %s", e)
        return None
    try:
        snap["signals"] = json.loads(snap.pop("signals_json") or "[]")
    except Exception:  # noqa: BLE001
        snap["signals"] = []
    if inc is not None:
        for k in ("impacted", "detail_json"):
            if inc.get(k):
                try:
                    inc[k] = json.loads(inc[k])
                except Exception:  # noqa: BLE001
                    pass
    snap["incident"] = inc
    return snap
