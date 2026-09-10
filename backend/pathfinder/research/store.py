"""
The research store — editions, findings, grades and scoreboard snapshots. APPEND-ONLY.

Every published row is protected by database triggers that reject UPDATE and DELETE. A
finding's grading rule is stored with it at publication (`grading_rule_json`, `frozen_at`);
the grade, when it lands, is a NEW row in `pf_grades` (one per finding, ever). The
scoreboard is derived from `pf_grades` and also snapshotted after every grading pass, so
"what did the scoreboard say on date X" is answerable.

Unpublished candidates are kept too (`pf_candidates`) with their scores, because a feed
that hides what it chose NOT to publish is not auditable.

S1 second audit:
  * A1 — every edition and every finding carries `backfilled` (1 = generated after its
    session date: a simulated backfill). The scoreboard is split forward / backfilled and
    the feed says which it is. A store cannot be re-labelled: it is archived and rebuilt.
  * A2 — `novelty_key` = `<template>|<subject>|<evidence signature>`; a finding that
    continues an OPEN call carries `continues = <root finding id>`, is never graded on its
    own (`pending()` skips it) and is not counted as a publication. The scoreboard's `n`
    is the independent count; `n_total` is every grade row.
  * A store written under a superseded schema is refused on open (`StoreSchemaError`) —
    it cannot be migrated in place, only archived and rebuilt (append-only).

Reads are served through `feed()`; the router never touches SQL.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Optional

from ..schemas import (
    BACKFILL_LABEL, FORWARD_LABEL, FeedResponse, Finding, GradingState, GradingStatus, Fact, ScoreCounts,
    Scoreboard, Tier, Verdict,
)
from .config import DEFAULT_RESEARCH_DB

SCHEMA_VERSION = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS pf_editions (
    edition_date        TEXT PRIMARY KEY,
    data_as_of          TEXT NOT NULL,
    generated_at        TEXT NOT NULL,
    engine_version      TEXT NOT NULL,
    llm_provider        TEXT NOT NULL,
    regime              TEXT NOT NULL,
    universe_scanned    INTEGER NOT NULL,
    candidates          INTEGER NOT NULL,
    threshold           REAL NOT NULL,
    params_json         TEXT NOT NULL,
    backfilled          INTEGER NOT NULL,
    data_through        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pf_findings (
    finding_id          TEXT PRIMARY KEY,
    edition_date        TEXT NOT NULL REFERENCES pf_editions(edition_date),
    rank                INTEGER NOT NULL,
    tier                TEXT NOT NULL,
    template_id         TEXT NOT NULL,
    subject             TEXT NOT NULL,
    decision            TEXT NOT NULL,
    novelty_key         TEXT NOT NULL,
    usefulness          REAL NOT NULL,
    horizon_sessions    INTEGER NOT NULL,
    grading_rule_json   TEXT NOT NULL,
    frozen_at           TEXT NOT NULL,
    card_json           TEXT NOT NULL,
    selected_by         TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    backfilled          INTEGER NOT NULL,
    continues           TEXT REFERENCES pf_findings(finding_id)
);
CREATE TABLE IF NOT EXISTS pf_candidates (
    candidate_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    edition_date        TEXT NOT NULL REFERENCES pf_editions(edition_date),
    template_id         TEXT NOT NULL,
    subject             TEXT NOT NULL,
    decision            TEXT NOT NULL,
    novelty_key         TEXT NOT NULL,
    usefulness          REAL NOT NULL,
    score_json          TEXT NOT NULL,
    published           INTEGER NOT NULL,
    reason              TEXT NOT NULL,
    finding_id          TEXT
);
CREATE TABLE IF NOT EXISTS pf_grades (
    finding_id          TEXT PRIMARY KEY REFERENCES pf_findings(finding_id),
    graded_at           TEXT NOT NULL,
    data_as_of          TEXT NOT NULL,
    due_session         TEXT NOT NULL,
    verdict             TEXT NOT NULL CHECK (verdict IN ('right','wrong','inconclusive')),
    rule_version        TEXT NOT NULL,
    realized_json       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pf_scoreboard (
    snapshot_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of               TEXT NOT NULL,
    right               INTEGER NOT NULL,
    wrong               INTEGER NOT NULL,
    inconclusive        INTEGER NOT NULL,
    n                   INTEGER NOT NULL,
    pending             INTEGER NOT NULL,
    by_template_json    TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    forward_json        TEXT NOT NULL,
    backfilled_json     TEXT NOT NULL,
    n_total             INTEGER NOT NULL,
    continued           INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS pf_meta (
    key                 TEXT PRIMARY KEY,
    value               TEXT NOT NULL
);
"""

_APPEND_ONLY = ("pf_editions", "pf_findings", "pf_candidates", "pf_grades", "pf_scoreboard")
_REQUIRED = {
    "pf_editions": ("backfilled", "data_through"),
    "pf_findings": ("backfilled", "continues"),
    "pf_scoreboard": ("forward_json", "backfilled_json", "n_total", "continued"),
}


class StoreSchemaError(RuntimeError):
    """The store on disk was written under a superseded schema. Archive it and rebuild."""


def _triggers() -> str:
    out = []
    for t in _APPEND_ONLY:
        for op in ("UPDATE", "DELETE"):
            out.append(
                f"CREATE TRIGGER IF NOT EXISTS {t}_no_{op.lower()} BEFORE {op} ON {t} "
                f"BEGIN SELECT RAISE(ABORT, '{t} is append-only'); END;"
            )
    return "\n".join(out)


def _empty_counts() -> ScoreCounts:
    return ScoreCounts(right=0, wrong=0, inconclusive=0, n=0)


class ResearchStore:
    """SQLite, one connection per thread (FastAPI runs sync endpoints in a threadpool)."""

    def __init__(self, db_path: str) -> None:
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self.source_name = f"pathfinder research store (S1) — {db_path}"
        existing = self.path.exists() and self.path.stat().st_size > 0
        if existing:
            self._check_schema()
        self.con.executescript(SCHEMA + _triggers())
        self.con.execute("INSERT OR IGNORE INTO pf_meta (key, value) VALUES ('schema_version', ?)", [str(SCHEMA_VERSION)])
        self.con.commit()

    def _check_schema(self) -> None:
        """Refuse a store written by a superseded schema — it cannot be corrected in place."""
        tables = {r[0] for r in self.con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        for t, cols in _REQUIRED.items():
            if t not in tables:
                continue
            have = {r[1] for r in self.con.execute(f"PRAGMA table_info({t})")}
            missing = [c for c in cols if c not in have]
            if missing:
                raise StoreSchemaError(
                    f"{self.path}: table {t} lacks {missing} — written under a superseded schema. "
                    "An append-only store is never migrated: archive it "
                    "(run_pathfinder_scan.py --archive-store --i-understand-this-archives-the-store) and rebuild.")

    @property
    def con(self) -> sqlite3.Connection:
        con = getattr(self._local, "con", None)
        if con is None:
            con = sqlite3.connect(str(self.path))
            con.row_factory = sqlite3.Row
            con.execute("PRAGMA foreign_keys = ON")
            self._local.con = con
        return con

    def close(self) -> None:
        con = getattr(self._local, "con", None)
        if con is not None:
            con.close()
            self._local.con = None

    def commit(self) -> None:
        self.con.commit()

    # ── writes ──────────────────────────────────────────────────────────────

    def has_edition(self, edition_date: str) -> bool:
        return self.con.execute("SELECT 1 FROM pf_editions WHERE edition_date = ?", [edition_date]).fetchone() is not None

    def put_edition(self, **row: Any) -> None:
        cols = ", ".join(row)
        self.con.execute(f"INSERT INTO pf_editions ({cols}) VALUES ({', '.join('?' * len(row))})", list(row.values()))

    def put_finding(self, finding: Finding, *, novelty_key: str, selected_by: str) -> None:
        self.con.execute(
            "INSERT INTO pf_findings (finding_id, edition_date, rank, tier, template_id, subject, decision, "
            "novelty_key, usefulness, horizon_sessions, grading_rule_json, frozen_at, card_json, selected_by, "
            "created_at, backfilled, continues) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [finding.id, finding.edition_date.isoformat(), finding.rank, finding.tier.value,
             finding.template_id, finding.subject, finding.decision.value, novelty_key,
             finding.usefulness.total, finding.grading_rule.horizon_sessions,
             finding.grading_rule.model_dump_json(), finding.grading_rule.frozen_at.isoformat(),
             finding.model_dump_json(), selected_by, datetime.now().isoformat(),
             int(finding.backfilled), finding.continues])

    def put_candidate(self, *, edition_date: str, template_id: str, subject: str, decision: str,
                      novelty_key: str, usefulness: float, score: dict[str, Any], published: bool,
                      reason: str, finding_id: Optional[str]) -> None:
        self.con.execute(
            "INSERT INTO pf_candidates (edition_date, template_id, subject, decision, novelty_key, usefulness, "
            "score_json, published, reason, finding_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
            [edition_date, template_id, subject, decision, novelty_key, usefulness,
             json.dumps(score, sort_keys=True), int(published), reason, finding_id])

    def put_grade(self, *, finding_id: str, graded_at: datetime, data_as_of: str, due_session: str,
                  verdict: Verdict, rule_version: str, realized: list[Fact]) -> None:
        self.con.execute(
            "INSERT INTO pf_grades (finding_id, graded_at, data_as_of, due_session, verdict, rule_version, realized_json) "
            "VALUES (?,?,?,?,?,?,?)",
            [finding_id, graded_at.isoformat(), data_as_of, due_session, verdict.value, rule_version,
             json.dumps([f.model_dump(mode="json") for f in realized])])

    def put_scoreboard_snapshot(self, sb: Scoreboard) -> None:
        self.con.execute(
            "INSERT INTO pf_scoreboard (as_of, right, wrong, inconclusive, n, pending, by_template_json, created_at, "
            "forward_json, backfilled_json, n_total, continued) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            [sb.as_of.isoformat(), sb.right, sb.wrong, sb.inconclusive, sb.n, sb.pending,
             json.dumps({k: v.model_dump(mode="json") for k, v in sb.by_template.items()}, sort_keys=True),
             datetime.now().isoformat(), sb.forward.model_dump_json(), sb.backfilled.model_dump_json(),
             sb.n_total, sb.continued])

    # ── reads ───────────────────────────────────────────────────────────────

    def editions(self) -> list[str]:
        return [r[0] for r in self.con.execute("SELECT edition_date FROM pf_editions ORDER BY edition_date")]

    def latest_edition(self) -> Optional[str]:
        r = self.con.execute("SELECT MAX(edition_date) FROM pf_editions").fetchone()
        return r[0] if r and r[0] else None

    def edition(self, edition_date: str) -> Optional[sqlite3.Row]:
        return self.con.execute("SELECT * FROM pf_editions WHERE edition_date = ?", [edition_date]).fetchone()

    def _lookback(self, before: str, lookback_editions: int) -> list[str]:
        return [r[0] for r in self.con.execute(
            "SELECT edition_date FROM pf_editions WHERE edition_date < ? ORDER BY edition_date DESC LIMIT ?",
            [before, lookback_editions])]

    @staticmethod
    def _split_key(key: str) -> tuple[str, str, str]:
        tmpl, subj, sig = key.split("|", 2)
        return tmpl, subj, sig

    def novelty(self, key: str, *, before: str, lookback_editions: int) -> float:
        """
        1.0 never seen in the lookback; 0.6 same template and subject on different evidence;
        0.25 the same claim (identical evidence signature) — whatever the subject (A2).
        """
        eds = self._lookback(before, lookback_editions)
        if not eds:
            return 1.0
        ph = ",".join("?" * len(eds))
        rows = self.con.execute(
            f"SELECT novelty_key FROM pf_findings WHERE edition_date IN ({ph})", eds).fetchall()
        keys = [self._split_key(r[0]) for r in rows]
        tmpl, subj, sig = self._split_key(key)
        if any(k[0] == tmpl and k[2] == sig for k in keys):
            return 0.25
        if any(k[0] == tmpl and k[1] == subj for k in keys):
            return 0.6
        return 1.0

    def open_root(self, key: str, *, before: str, lookback_editions: int,
                  is_open: Callable[[str, int], bool]) -> Optional[sqlite3.Row]:
        """
        The ROOT finding (one that is not itself a continuation) carrying the same claim whose
        horizon is still running at `before` — `is_open(edition_date, horizon_sessions)` is the
        caller's session arithmetic. A repeat of an open claim is a continuation, not a new
        publication (A2). Once the root's horizon completes, the next repeat is a new,
        independently graded publication.
        """
        eds = self._lookback(before, lookback_editions)
        if not eds:
            return None
        ph = ",".join("?" * len(eds))
        tmpl, _, sig = self._split_key(key)
        rows = self.con.execute(
            f"SELECT * FROM pf_findings WHERE edition_date IN ({ph}) AND continues IS NULL "
            "ORDER BY edition_date DESC, rank", eds).fetchall()
        for r in rows:
            t, _, s = self._split_key(r["novelty_key"])
            if t == tmpl and s == sig and is_open(r["edition_date"], int(r["horizon_sessions"])):
                return r
        return None

    def _grade_row(self, finding_id: str) -> Optional[sqlite3.Row]:
        return self.con.execute("SELECT * FROM pf_grades WHERE finding_id = ?", [finding_id]).fetchone()

    def _finding(self, row: sqlite3.Row) -> Finding:
        f = Finding.model_validate_json(row["card_json"])
        g = self._grade_row(row["finding_id"])
        if g is not None:
            realized = [Fact.model_validate(x) for x in json.loads(g["realized_json"])]
            f = f.model_copy(update={"grading": GradingState(
                status=GradingStatus.graded, due_session=date.fromisoformat(g["due_session"]),
                verdict=Verdict(g["verdict"]), graded_at=datetime.fromisoformat(g["graded_at"]),
                data_as_of=date.fromisoformat(g["data_as_of"]), realized_facts=realized,
                backfilled=bool(row["backfilled"]))})
        return f

    def findings_for(self, edition_date: str) -> list[Finding]:
        rows = self.con.execute(
            "SELECT * FROM pf_findings WHERE edition_date = ? ORDER BY rank", [edition_date]).fetchall()
        return [self._finding(r) for r in rows]

    def pending(self) -> list[sqlite3.Row]:
        """Published ROOT findings with no grade yet (all editions). Continuations are graded through their root."""
        return self.con.execute(
            "SELECT f.* FROM pf_findings f LEFT JOIN pf_grades g ON g.finding_id = f.finding_id "
            "WHERE g.finding_id IS NULL AND f.continues IS NULL ORDER BY f.edition_date, f.rank").fetchall()

    def scoreboard(self, as_of: str) -> Scoreboard:
        def counts(rows) -> dict[str, int]:
            c = {"right": 0, "wrong": 0, "inconclusive": 0}
            for r in rows:
                c[r["verdict"]] += 1
            return c

        def sc(rows) -> ScoreCounts:
            c = counts(rows)
            return ScoreCounts(**c, n=sum(c.values()))

        all_rows = self.con.execute(
            "SELECT g.verdict, f.template_id, f.backfilled, f.continues FROM pf_grades g "
            "JOIN pf_findings f ON f.finding_id = g.finding_id WHERE g.data_as_of <= ?", [as_of]).fetchall()
        # Independent grades: on root findings only. A continuation is never graded on its own
        # (pending() skips it), so in a store built by this code n_total == n; a store that
        # carried re-grades of the same claim would show them in n_total and not in n.
        indep = [r for r in all_rows if r["continues"] is None]
        tot = counts(indep)
        by: dict[str, ScoreCounts] = {}
        for t in sorted({r["template_id"] for r in indep}):
            by[t] = sc([r for r in indep if r["template_id"] == t])
        fwd = sc([r for r in indep if not r["backfilled"]])
        bf = sc([r for r in indep if r["backfilled"]])
        # Pending AS OF `as_of` (S1 audit P4): published by then and not yet graded by then —
        # a finding graded on a later seal was still pending on this date.
        pending = self.con.execute(
            "SELECT COUNT(*) FROM pf_findings f LEFT JOIN pf_grades g ON g.finding_id = f.finding_id "
            "WHERE f.edition_date <= ? AND f.continues IS NULL AND (g.finding_id IS NULL OR g.data_as_of > ?)",
            [as_of, as_of]).fetchone()[0]
        continued = self.con.execute(
            "SELECT COUNT(*) FROM pf_findings WHERE edition_date <= ? AND continues IS NOT NULL", [as_of]).fetchone()[0]
        if fwd.n and bf.n:
            label = f"mixed: {fwd.n} forward, {bf.n} {BACKFILL_LABEL}"
        elif bf.n or not fwd.n:
            label = BACKFILL_LABEL
        else:
            label = FORWARD_LABEL
        return Scoreboard(**tot, n=sum(tot.values()), pending=int(pending), by_template=by,
                          as_of=date.fromisoformat(as_of), forward=fwd, backfilled=bf,
                          n_total=len(all_rows), n_independent=sum(tot.values()), continued=int(continued),
                          record_label=label)

    def feed(self, edition_date: Optional[str] = None) -> Optional[FeedResponse]:
        ed = edition_date or self.latest_edition()
        if ed is None:
            return None
        row = self.edition(ed)
        if row is None:
            return None
        items = self.findings_for(ed)
        backfilled = bool(row["backfilled"])
        return FeedResponse(
            edition_date=date.fromisoformat(ed), data_as_of=date.fromisoformat(row["data_as_of"]),
            generated_at=datetime.fromisoformat(row["generated_at"]), regime=row["regime"],
            universe_scanned=row["universe_scanned"], candidates_considered=row["candidates"],
            published_count=sum(1 for f in items if f.continues is None),
            continued_count=sum(1 for f in items if f.continues is not None),
            usefulness_threshold=row["threshold"],
            what_matters_now=[f for f in items if f.tier == Tier.what_matters_now],
            discoveries=[f for f in items if f.tier == Tier.discovery],
            scoreboard=self.scoreboard(ed), llm_provider=row["llm_provider"],
            backfilled=backfilled, record_label=BACKFILL_LABEL if backfilled else FORWARD_LABEL,
        )


_store: Optional[ResearchStore] = None
_lock = threading.Lock()


def research_db_path() -> str:
    return os.environ.get("KANIDA_PATHFINDER_RESEARCH_DB", DEFAULT_RESEARCH_DB)


def get_research_store() -> Optional[ResearchStore]:
    """The process-wide store, or None when no scan has ever been run (never fixtures)."""
    global _store
    with _lock:
        if _store is not None:
            return _store
        p = research_db_path()
        if not Path(p).exists():
            return None
        _store = ResearchStore(p)
        return _store


def set_research_store(store: Optional[ResearchStore]) -> None:
    global _store
    with _lock:
        _store = store
