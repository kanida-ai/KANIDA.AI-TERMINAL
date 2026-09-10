"""
The experiment registry — APPEND-ONLY, HASH-CHAINED (spec addendum 3: memory, versions, trials).

A second SQLite file next to S1's research store (`KANIDA_PATHFINDER_EXPERIMENTS_DB`), so the
S1 store stays byte-for-byte what S1 built. Every table rejects UPDATE and DELETE by trigger
(S1 store.py); the tables that ARE the record — experiments, versions, periods, outcomes,
post-mortems, proposals — are additionally hash-chained the way P1's `engine/repository.py`
chains its published rows: every row carries the previous row's hash and a SHA-256 over its
own content, so rewriting history means rewriting every row after it, and `verify_chain()`
says so. Timestamps are kept OUT of the hashed payload — the timestamp columns and the
timestamp keys inside JSON columns (a frozen rule's `frozen_at`, a fact's `computed_at`) — so
two rebuilds of the same seals produce identical chains: determinism is a property a reader
can check with `verify_chain()` and a table diff, not a promise.

What is recorded, and why it cannot be quietly optimised:
  * every VARIANT ever evaluated is a `pfx_trials` row — adopted or not, at opening or after a
    period — so `trials_total` on a card is the count the reader should divide the p-value by;
  * every S1 finding the gate declined is a `pfx_candidates` row with its reason;
  * a version's EXPECTATION and GRADING RULE are frozen in the version row before its first
    period opens; a period's OUTCOME is a new row, once, ever;
  * constituent names live only in `pfx_trades`, which no public surface reads.

A store under a superseded schema is refused on open — archived and rebuilt, never migrated.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

from ..engine.repository import canonical
from ..schemas import (
    BACKFILL_LABEL, FORWARD_LABEL, ExperimentScoreboard, ExperimentState, ScoreCounts, Verdict,
)
from .config import DEFAULT_EXPERIMENTS_DB

#: 2 = after the S2 audit: `pfx_trades.resolved` / `unresolved_reason` (finding 7), `pfx_outcomes.grader_version`
#: (finding 6), content-only chain hashes (finding 2). A registry under 1 is refused on open — archive and rebuild.
SCHEMA_VERSION = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS pfx_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pfx_experiments (
    experiment_id       TEXT PRIMARY KEY,
    family_id           TEXT NOT NULL,
    source_finding_id   TEXT NOT NULL,
    opened_edition      TEXT NOT NULL,
    theme               TEXT NOT NULL,
    question            TEXT NOT NULL,
    direction           TEXT NOT NULL,
    threshold_pct       REAL NOT NULL,
    capital_inr         REAL NOT NULL,
    engine_version      TEXT NOT NULL,
    constitution_version TEXT NOT NULL,
    params_json         TEXT NOT NULL,
    evidence_json       TEXT NOT NULL,
    gates_json          TEXT NOT NULL,
    backfilled          INTEGER NOT NULL,
    created_at          TEXT NOT NULL,
    prev_hash           TEXT,
    row_hash            TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pfx_candidates (
    candidate_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    edition_date        TEXT NOT NULL,
    finding_id          TEXT NOT NULL,
    template_id         TEXT NOT NULL,
    family_id           TEXT,
    trials_evaluated    INTEGER NOT NULL,
    opened_experiment_id TEXT,
    reason              TEXT NOT NULL,
    best_signature      TEXT,
    best_rule_text      TEXT,
    best_expectancy_net REAL,
    best_failed_gates_json TEXT NOT NULL,
    created_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pfx_trials (
    trial_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_kind          TEXT NOT NULL CHECK (owner_kind IN ('finding','experiment')),
    owner_id            TEXT NOT NULL,
    edition_date        TEXT NOT NULL,
    trial_no            INTEGER NOT NULL,
    context             TEXT NOT NULL,
    signature           TEXT NOT NULL,
    variant_json        TEXT NOT NULL,
    stats_json          TEXT NOT NULL,
    gates_json          TEXT NOT NULL,
    passed              INTEGER NOT NULL,
    adopted             INTEGER NOT NULL,
    reason              TEXT NOT NULL,
    created_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pfx_versions (
    experiment_id       TEXT NOT NULL REFERENCES pfx_experiments(experiment_id),
    version             INTEGER NOT NULL,
    created_edition     TEXT NOT NULL,
    variant_json        TEXT NOT NULL,
    change              TEXT NOT NULL,
    why                 TEXT NOT NULL,
    level               TEXT NOT NULL CHECK (level IN ('L1','L2','L3')),
    validation          TEXT,
    expectation_json    TEXT NOT NULL,
    expectation_facts_json TEXT NOT NULL,
    grading_rule_json   TEXT NOT NULL,
    trials_for_version  INTEGER NOT NULL,
    backfilled          INTEGER NOT NULL,
    created_at          TEXT NOT NULL,
    prev_hash           TEXT,
    row_hash            TEXT NOT NULL,
    PRIMARY KEY (experiment_id, version)
);
CREATE TABLE IF NOT EXISTS pfx_periods (
    experiment_id       TEXT NOT NULL,
    version             INTEGER NOT NULL,
    period_no           INTEGER NOT NULL,
    start_after         TEXT NOT NULL,
    sessions            INTEGER NOT NULL,
    opened_edition      TEXT NOT NULL,
    backfilled          INTEGER NOT NULL,
    created_at          TEXT NOT NULL,
    prev_hash           TEXT,
    row_hash            TEXT NOT NULL,
    PRIMARY KEY (experiment_id, version, period_no)
);
CREATE TABLE IF NOT EXISTS pfx_marks (
    experiment_id       TEXT NOT NULL,
    version             INTEGER NOT NULL,
    period_no           INTEGER NOT NULL,
    session             TEXT NOT NULL,
    equity_inr          REAL NOT NULL,
    open_positions      INTEGER NOT NULL,
    closed_cum          INTEGER NOT NULL,
    recorded_edition    TEXT NOT NULL,
    PRIMARY KEY (experiment_id, version, period_no, session)
);
CREATE TABLE IF NOT EXISTS pfx_trades (
    experiment_id       TEXT NOT NULL,
    version             INTEGER NOT NULL,
    period_no           INTEGER NOT NULL,
    symbol              TEXT NOT NULL,
    signal_date         TEXT NOT NULL,
    entry_date          TEXT NOT NULL,
    entry_price         REAL NOT NULL,
    exit_date           TEXT NOT NULL,
    exit_price          REAL NOT NULL,
    holding_sessions    INTEGER NOT NULL,
    pnl_pct_gross       REAL NOT NULL,
    pnl_pct_net         REAL NOT NULL,
    costs_pct           REAL NOT NULL,
    notional_inr        REAL NOT NULL,
    resolved            INTEGER NOT NULL,
    unresolved_reason   TEXT,
    recorded_edition    TEXT NOT NULL,
    PRIMARY KEY (experiment_id, version, period_no, symbol, signal_date)
);
CREATE TABLE IF NOT EXISTS pfx_outcomes (
    experiment_id       TEXT NOT NULL,
    version             INTEGER NOT NULL,
    period_no           INTEGER NOT NULL,
    graded_edition      TEXT NOT NULL,
    data_as_of          TEXT NOT NULL,
    period_start        TEXT NOT NULL,
    period_end          TEXT NOT NULL,
    due_session         TEXT NOT NULL,
    verdict             TEXT NOT NULL CHECK (verdict IN ('right','wrong','inconclusive','void')),
    rule_version        TEXT NOT NULL,
    grader_version      TEXT NOT NULL,
    forward_json        TEXT NOT NULL,
    comparison_json     TEXT NOT NULL,
    realized_facts_json TEXT NOT NULL,
    learning_json       TEXT NOT NULL,
    next_action         TEXT NOT NULL,
    backfilled          INTEGER NOT NULL,
    graded_at           TEXT NOT NULL,
    prev_hash           TEXT,
    row_hash            TEXT NOT NULL,
    PRIMARY KEY (experiment_id, version, period_no)
);
CREATE TABLE IF NOT EXISTS pfx_post_mortems (
    experiment_id       TEXT PRIMARY KEY,
    buried_edition      TEXT NOT NULL,
    cause               TEXT NOT NULL,
    retired_version     INTEGER NOT NULL,
    what_we_kept        TEXT NOT NULL,
    summary             TEXT NOT NULL,
    fact_refs_json      TEXT NOT NULL,
    backfilled          INTEGER NOT NULL,
    created_at          TEXT NOT NULL,
    prev_hash           TEXT,
    row_hash            TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pfx_proposals (
    proposal_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    experiment_id       TEXT NOT NULL,
    version             INTEGER NOT NULL,
    edition_date        TEXT NOT NULL,
    target_agent        TEXT NOT NULL,
    status              TEXT NOT NULL,
    gates_json          TEXT NOT NULL,
    incumbent           TEXT NOT NULL,
    backfilled          INTEGER NOT NULL,
    created_at          TEXT NOT NULL,
    prev_hash           TEXT,
    row_hash            TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pfx_gate_checks (
    experiment_id       TEXT NOT NULL,
    version             INTEGER NOT NULL,
    edition_date        TEXT NOT NULL,
    gates_json          TEXT NOT NULL,
    proposable          INTEGER NOT NULL,
    PRIMARY KEY (experiment_id, version, edition_date)
);
CREATE TABLE IF NOT EXISTS pfx_narratives (
    experiment_id       TEXT NOT NULL,
    edition_date        TEXT NOT NULL,
    story_json          TEXT NOT NULL,
    facts_json          TEXT NOT NULL,
    produced_by         TEXT NOT NULL,
    llm_provider        TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    PRIMARY KEY (experiment_id, edition_date)
);
CREATE TABLE IF NOT EXISTS pfx_editions (
    edition_date        TEXT PRIMARY KEY,
    data_as_of          TEXT NOT NULL,
    engine_version      TEXT NOT NULL,
    constitution_version TEXT NOT NULL,
    params_json         TEXT NOT NULL,
    backfilled          INTEGER NOT NULL,
    generated_at        TEXT NOT NULL
);
"""

_APPEND_ONLY = ("pfx_experiments", "pfx_candidates", "pfx_trials", "pfx_versions", "pfx_periods", "pfx_marks",
                "pfx_trades", "pfx_outcomes", "pfx_post_mortems", "pfx_proposals", "pfx_gate_checks",
                "pfx_narratives", "pfx_editions")
CHAINED = ("pfx_experiments", "pfx_versions", "pfx_periods", "pfx_outcomes", "pfx_post_mortems", "pfx_proposals")
#: Columns outside the hashed payload: timestamps and database-generated ids.
UNHASHED = {"created_at", "graded_at", "generated_at", "prev_hash", "row_hash", "proposal_id"}
#: Timestamp keys INSIDE JSON columns (a frozen rule's `frozen_at`, a fact's `computed_at`, a story
#: line's `at`), likewise outside the hashed content: the chain is over what was computed, not when.
JSON_TIMESTAMP_KEYS = {"frozen_at", "computed_at", "at", "graded_at", "created_at", "generated_at"}


class StoreSchemaError(RuntimeError):
    """The registry on disk was written under a superseded schema. Archive and rebuild."""


def _triggers() -> str:
    out = []
    for t in _APPEND_ONLY:
        for op in ("UPDATE", "DELETE"):
            out.append(f"CREATE TRIGGER IF NOT EXISTS {t}_no_{op.lower()} BEFORE {op} ON {t} "
                       f"BEGIN SELECT RAISE(ABORT, '{t} is append-only'); END;")
    return "\n".join(out)


def strip_timestamps(value: Any) -> Any:
    """Recursively drop timestamp keys from a decoded JSON value."""
    if isinstance(value, dict):
        return {k: strip_timestamps(v) for k, v in value.items() if k not in JSON_TIMESTAMP_KEYS}
    if isinstance(value, list):
        return [strip_timestamps(v) for v in value]
    return value


def content_payload(row: dict[str, Any]) -> dict[str, Any]:
    """The hashed content of a row: no timestamp columns, no timestamps inside its JSON columns."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        if k in UNHASHED:
            continue
        if k.endswith("_json") and isinstance(v, str):
            try:
                out[k] = strip_timestamps(json.loads(v))
                continue
            except ValueError:
                pass
        out[k] = v
    return out


def _row_hash(payload: dict[str, Any], prev: Optional[str]) -> str:
    return hashlib.sha256(f"{prev or ''}|{canonical(payload)}".encode()).hexdigest()


def _j(x: Any) -> str:
    return json.dumps(x, sort_keys=True, default=str)


class ExperimentStore:
    """SQLite, one connection per thread (FastAPI runs sync endpoints in a threadpool)."""

    def __init__(self, db_path: str) -> None:
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self.source_name = f"pathfinder experiment registry (S2) — {db_path}"
        if self.path.exists() and self.path.stat().st_size > 0:
            self._check_schema()
        self.con.executescript(SCHEMA + _triggers())
        self.con.execute("INSERT OR IGNORE INTO pfx_meta (key, value) VALUES ('schema_version', ?)", [str(SCHEMA_VERSION)])
        self.con.commit()

    def _check_schema(self) -> None:
        row = self.con.execute("SELECT value FROM pfx_meta WHERE key = 'schema_version'").fetchone() \
            if self.con.execute("SELECT 1 FROM sqlite_master WHERE name = 'pfx_meta'").fetchone() else None
        have = int(row[0]) if row else 0
        if have < SCHEMA_VERSION:
            raise StoreSchemaError(f"{self.path}: schema_version {have} < {SCHEMA_VERSION} — archive the registry "
                                   "(run_pathfinder_experiments.py --archive-store --i-understand-this-archives-the-store) and rebuild")

    @property
    def con(self) -> sqlite3.Connection:
        con = getattr(self._local, "con", None)
        if con is None:
            # WAL + a long timeout: the API reads while the after-close loop writes; a reader must
            # never see "database is locked" because a step's transaction is open.
            con = sqlite3.connect(str(self.path), timeout=30.0)
            con.row_factory = sqlite3.Row
            con.execute("PRAGMA foreign_keys = ON")
            con.execute("PRAGMA journal_mode = WAL")
            self._local.con = con
        return con

    def close(self) -> None:
        con = getattr(self._local, "con", None)
        if con is not None:
            con.close()
            self._local.con = None

    def commit(self) -> None:
        self.con.commit()

    # ── low level ────────────────────────────────────────────────────────────

    def _insert(self, table: str, row: dict[str, Any]) -> None:
        cols = ", ".join(row)
        self.con.execute(f"INSERT INTO {table} ({cols}) VALUES ({', '.join('?' * len(row))})", list(row.values()))

    def _chain_prev(self, table: str) -> Optional[str]:
        r = self.con.execute(f"SELECT row_hash FROM {table} ORDER BY rowid DESC LIMIT 1").fetchone()
        return None if r is None else str(r[0])

    def _insert_chained(self, table: str, row: dict[str, Any]) -> str:
        prev = self._chain_prev(table)
        h = _row_hash(content_payload(row), prev)
        self._insert(table, {**row, "prev_hash": prev, "row_hash": h})
        return h

    def verify_chain(self, table: str) -> tuple[bool, str]:
        rows = list(self.con.execute(f"SELECT * FROM {table} ORDER BY rowid"))
        prev: Optional[str] = None
        for r in rows:
            d = content_payload({k: r[k] for k in r.keys()})
            if r["prev_hash"] != prev or r["row_hash"] != _row_hash(d, prev):
                return False, f"{table}: chain broken at row_hash={r['row_hash']}"
            prev = str(r["row_hash"])
        return True, f"{table}: {len(rows)} rows, chain intact"

    def verify_all_chains(self) -> list[tuple[str, bool, str]]:
        return [(t, *self.verify_chain(t)) for t in CHAINED]

    # ── writes ───────────────────────────────────────────────────────────────

    def has_edition(self, d: str) -> bool:
        return self.con.execute("SELECT 1 FROM pfx_editions WHERE edition_date = ?", [d]).fetchone() is not None

    def put_edition(self, **row: Any) -> None:
        self._insert("pfx_editions", row)

    def put_experiment(self, **row: Any) -> None:
        self._insert_chained("pfx_experiments", row)

    def put_candidate(self, **row: Any) -> None:
        self._insert("pfx_candidates", row)

    def put_trial(self, **row: Any) -> None:
        self._insert("pfx_trials", row)

    def put_version(self, **row: Any) -> None:
        self._insert_chained("pfx_versions", row)

    def put_period(self, **row: Any) -> None:
        self._insert_chained("pfx_periods", row)

    def put_mark(self, **row: Any) -> None:
        self._insert("pfx_marks", row)

    def put_trade(self, **row: Any) -> None:
        self._insert("pfx_trades", row)

    def put_outcome(self, **row: Any) -> None:
        self._insert_chained("pfx_outcomes", row)

    def put_post_mortem(self, **row: Any) -> None:
        self._insert_chained("pfx_post_mortems", row)

    def put_proposal(self, **row: Any) -> None:
        self._insert_chained("pfx_proposals", row)

    def put_gate_check(self, **row: Any) -> None:
        self._insert("pfx_gate_checks", row)

    def put_narrative(self, **row: Any) -> None:
        self._insert("pfx_narratives", row)

    # ── reads ────────────────────────────────────────────────────────────────

    def q(self, sql: str, params: list[Any] | tuple = ()) -> list[sqlite3.Row]:
        return list(self.con.execute(sql, list(params)))

    def one(self, sql: str, params: list[Any] | tuple = ()) -> Optional[sqlite3.Row]:
        return self.con.execute(sql, list(params)).fetchone()

    def editions(self) -> list[str]:
        return [r[0] for r in self.q("SELECT edition_date FROM pfx_editions ORDER BY edition_date")]

    def latest_edition(self) -> Optional[str]:
        r = self.one("SELECT MAX(edition_date) FROM pfx_editions")
        return r[0] if r and r[0] else None

    def experiments(self) -> list[sqlite3.Row]:
        return self.q("SELECT * FROM pfx_experiments ORDER BY opened_edition, experiment_id")

    def experiment(self, eid: str) -> Optional[sqlite3.Row]:
        return self.one("SELECT * FROM pfx_experiments WHERE experiment_id = ?", [eid])

    def experiment_for_family(self, family_id: str) -> Optional[sqlite3.Row]:
        return self.one("SELECT * FROM pfx_experiments WHERE family_id = ? ORDER BY opened_edition DESC LIMIT 1", [family_id])

    def versions(self, eid: str) -> list[sqlite3.Row]:
        return self.q("SELECT * FROM pfx_versions WHERE experiment_id = ? ORDER BY version", [eid])

    def latest_version(self, eid: str) -> Optional[sqlite3.Row]:
        return self.one("SELECT * FROM pfx_versions WHERE experiment_id = ? ORDER BY version DESC LIMIT 1", [eid])

    def periods(self, eid: str, version: int) -> list[sqlite3.Row]:
        return self.q("SELECT * FROM pfx_periods WHERE experiment_id = ? AND version = ? ORDER BY period_no", [eid, version])

    def outcome(self, eid: str, version: int, period_no: int) -> Optional[sqlite3.Row]:
        return self.one("SELECT * FROM pfx_outcomes WHERE experiment_id = ? AND version = ? AND period_no = ?",
                        [eid, version, period_no])

    def outcomes(self, eid: str) -> list[sqlite3.Row]:
        return self.q("SELECT * FROM pfx_outcomes WHERE experiment_id = ? ORDER BY version, period_no", [eid])

    def marks(self, eid: str, version: int, period_no: int) -> list[sqlite3.Row]:
        return self.q("SELECT * FROM pfx_marks WHERE experiment_id = ? AND version = ? AND period_no = ? ORDER BY session",
                      [eid, version, period_no])

    def trades(self, eid: str, version: Optional[int] = None, period_no: Optional[int] = None,
               *, resolved_only: bool = False) -> list[sqlite3.Row]:
        sql = "SELECT * FROM pfx_trades WHERE experiment_id = ?" + (" AND resolved = 1" if resolved_only else "")
        p: list[Any] = [eid]
        if version is not None:
            sql += " AND version = ?"
            p.append(version)
        if period_no is not None:
            sql += " AND period_no = ?"
            p.append(period_no)
        return self.q(sql + " ORDER BY exit_date, signal_date, symbol", p)

    def trials(self, owner_kind: str, owner_id: str) -> list[sqlite3.Row]:
        return self.q("SELECT * FROM pfx_trials WHERE owner_kind = ? AND owner_id = ? ORDER BY trial_no", [owner_kind, owner_id])

    def trials_for_experiment(self, eid: str, source_finding_id: str) -> list[sqlite3.Row]:
        return self.q("SELECT * FROM pfx_trials WHERE (owner_kind = 'finding' AND owner_id = ?) OR "
                      "(owner_kind = 'experiment' AND owner_id = ?) ORDER BY trial_no", [source_finding_id, eid])

    def post_mortem(self, eid: str) -> Optional[sqlite3.Row]:
        return self.one("SELECT * FROM pfx_post_mortems WHERE experiment_id = ?", [eid])

    def proposal(self, eid: str) -> Optional[sqlite3.Row]:
        return self.one("SELECT * FROM pfx_proposals WHERE experiment_id = ? ORDER BY proposal_id DESC LIMIT 1", [eid])

    def gate_check(self, eid: str, version: int) -> Optional[sqlite3.Row]:
        return self.one("SELECT * FROM pfx_gate_checks WHERE experiment_id = ? AND version = ? ORDER BY edition_date DESC LIMIT 1",
                        [eid, version])

    def narrative(self, eid: str, edition: Optional[str] = None) -> Optional[sqlite3.Row]:
        if edition is None:
            return self.one("SELECT * FROM pfx_narratives WHERE experiment_id = ? ORDER BY edition_date DESC LIMIT 1", [eid])
        return self.one("SELECT * FROM pfx_narratives WHERE experiment_id = ? AND edition_date = ?", [eid, edition])

    def narratives_on(self, edition: str) -> list[sqlite3.Row]:
        return self.q("SELECT * FROM pfx_narratives WHERE edition_date = ? ORDER BY experiment_id", [edition])

    def candidates(self, edition: Optional[str] = None) -> list[sqlite3.Row]:
        if edition is None:
            return self.q("SELECT * FROM pfx_candidates ORDER BY edition_date, candidate_id")
        return self.q("SELECT * FROM pfx_candidates WHERE edition_date = ? ORDER BY candidate_id", [edition])

    def candidate_for_finding(self, finding_id: str) -> Optional[sqlite3.Row]:
        return self.one("SELECT * FROM pfx_candidates WHERE finding_id = ?", [finding_id])

    def state_of(self, eid: str) -> ExperimentState:
        if self.post_mortem(eid) is not None:
            return ExperimentState.buried
        if self.proposal(eid) is not None:
            return ExperimentState.proposed
        return ExperimentState.testing

    def edition_backfilled(self, edition: str) -> Optional[bool]:
        r = self.one("SELECT backfilled FROM pfx_editions WHERE edition_date = ?", [edition])
        return None if r is None else bool(r[0])

    def consecutive_void(self, eid: str, version: int) -> int:
        """How many of the version's most recent graded periods, counting back, closed no trade."""
        n = 0
        for o in reversed(self.q("SELECT verdict FROM pfx_outcomes WHERE experiment_id = ? AND version = ? ORDER BY period_no",
                                 [eid, version])):
            if o["verdict"] != Verdict.void.value:
                break
            n += 1
        return n

    def version_status(self, eid: str, version: int) -> str:
        if self.post_mortem(eid) is not None:
            latest = self.latest_version(eid)
            return "buried" if latest is not None and int(latest["version"]) == version else "superseded"
        latest = self.latest_version(eid)
        return "open" if latest is not None and int(latest["version"]) == version else "superseded"

    def open_periods(self) -> list[sqlite3.Row]:
        """Periods of open versions of unburied experiments that have no outcome yet."""
        return self.q(
            "SELECT p.* FROM pfx_periods p JOIN pfx_experiments e ON e.experiment_id = p.experiment_id "
            "LEFT JOIN pfx_outcomes o ON o.experiment_id = p.experiment_id AND o.version = p.version AND o.period_no = p.period_no "
            "LEFT JOIN pfx_post_mortems pm ON pm.experiment_id = p.experiment_id "
            "WHERE o.experiment_id IS NULL AND pm.experiment_id IS NULL "
            "AND p.version = (SELECT MAX(version) FROM pfx_versions v WHERE v.experiment_id = p.experiment_id) "
            "ORDER BY p.experiment_id, p.version, p.period_no")

    def scoreboard(self, as_of: str) -> ExperimentScoreboard:
        rows = self.q("SELECT o.verdict, o.backfilled, e.family_id FROM pfx_outcomes o "
                      "JOIN pfx_experiments e ON e.experiment_id = o.experiment_id WHERE o.data_as_of <= ?", [as_of])
        counted = [r for r in rows if r["verdict"] != Verdict.void.value]
        void = len(rows) - len(counted)

        def sc(rs) -> ScoreCounts:
            c = {"right": 0, "wrong": 0, "inconclusive": 0}
            for r in rs:
                c[r["verdict"]] += 1
            return ScoreCounts(**c, n=sum(c.values()))

        tot = sc(counted)
        fwd = sc([r for r in counted if not r["backfilled"]])
        bf = sc([r for r in counted if r["backfilled"]])
        by = {f: sc([r for r in counted if r["family_id"] == f]) for f in sorted({r["family_id"] for r in counted})}
        pending = self.one(
            "SELECT COUNT(*) FROM pfx_periods p LEFT JOIN pfx_outcomes o ON o.experiment_id = p.experiment_id "
            "AND o.version = p.version AND o.period_no = p.period_no WHERE p.opened_edition <= ? "
            "AND (o.experiment_id IS NULL OR o.data_as_of > ?)", [as_of, as_of])[0]
        exps = [r for r in self.experiments() if r["opened_edition"] <= as_of]
        buried = sum(1 for r in exps if (pm := self.post_mortem(r["experiment_id"])) is not None and pm["buried_edition"] <= as_of)
        proposed = sum(1 for r in exps if (pr := self.proposal(r["experiment_id"])) is not None and pr["edition_date"] <= as_of
                       and self.post_mortem(r["experiment_id"]) is None)
        testing = len(exps) - buried - proposed
        cands = self.one("SELECT COUNT(*) FROM pfx_candidates WHERE edition_date <= ? AND opened_experiment_id IS NULL", [as_of])[0]
        trials = self.one("SELECT COUNT(*) FROM pfx_trials WHERE edition_date <= ?", [as_of])[0]
        if fwd.n and bf.n:
            label = f"mixed: {fwd.n} forward, {bf.n} {BACKFILL_LABEL}"
        elif bf.n or not fwd.n:
            label = BACKFILL_LABEL
        else:
            label = FORWARD_LABEL
        return ExperimentScoreboard(
            right=tot.right, wrong=tot.wrong, inconclusive=tot.inconclusive, n=tot.n, void=int(void), pending=int(pending),
            forward=fwd, backfilled=bf, by_family=by, experiments_testing=int(testing), experiments_buried=int(buried),
            experiments_proposed=int(proposed), candidates_not_opened=int(cands), trials_total=int(trials),
            as_of=date.fromisoformat(as_of), record_label=label)


_store: Optional[ExperimentStore] = None
_lock = threading.Lock()


def experiments_db_path() -> str:
    return os.environ.get("KANIDA_PATHFINDER_EXPERIMENTS_DB", DEFAULT_EXPERIMENTS_DB)


def get_experiment_store() -> Optional[ExperimentStore]:
    """The process-wide registry, or None when no loop has ever run (never fixtures)."""
    global _store
    with _lock:
        if _store is not None:
            return _store
        p = experiments_db_path()
        if not Path(p).exists():
            return None
        _store = ExperimentStore(p)
        return _store


def set_experiment_store(store: Optional[ExperimentStore]) -> None:
    global _store
    with _lock:
        _store = store
