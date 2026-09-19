from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any

from .market import MarketData
from .models import Edition, EvaluationPlan, Finding, Policy


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hypothesis_id(finding: Finding) -> str:
    identity = (
        finding.template_id,
        finding.public_subject,
        finding.evaluation.direction,
        finding.evaluation.horizon_sessions,
        finding.evaluation.kind,
    )
    return "HYP-" + hashlib.sha256(_json(identity).encode()).hexdigest()[:12].upper()


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS editions (
    as_of TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    scanned_symbols INTEGER NOT NULL,
    candidate_count INTEGER NOT NULL,
    published_count INTEGER NOT NULL,
    suppressed_count INTEGER NOT NULL,
    usefulness_threshold REAL NOT NULL,
    market_regime TEXT NOT NULL,
    data_quality_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS findings (
    finding_id TEXT PRIMARY KEY,
    edition_date TEXT NOT NULL REFERENCES editions(as_of),
    template_id TEXT NOT NULL,
    public_subject TEXT NOT NULL,
    publication_score REAL NOT NULL,
    payload_json TEXT NOT NULL,
    evaluation_json TEXT NOT NULL,
    due_date TEXT,
    outcome_status TEXT NOT NULL DEFAULT 'PENDING',
    realized_return REAL,
    grade TEXT,
    graded_at TEXT
);

CREATE TABLE IF NOT EXISTS hypotheses (
    hypothesis_id TEXT PRIMARY KEY,
    template_id TEXT NOT NULL,
    public_subject TEXT NOT NULL,
    direction TEXT NOT NULL,
    horizon_sessions INTEGER NOT NULL,
    status TEXT NOT NULL,
    current_version INTEGER NOT NULL,
    max_versions INTEGER NOT NULL,
    created_date TEXT NOT NULL,
    retirement_rule TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS experiment_versions (
    hypothesis_id TEXT NOT NULL REFERENCES hypotheses(hypothesis_id),
    version INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_date TEXT NOT NULL,
    change_note TEXT NOT NULL,
    why_changed TEXT NOT NULL,
    rules_json TEXT NOT NULL,
    PRIMARY KEY (hypothesis_id, version)
);

CREATE TABLE IF NOT EXISTS experiment_runs (
    run_id TEXT PRIMARY KEY,
    hypothesis_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    trial_number INTEGER NOT NULL,
    finding_id TEXT NOT NULL REFERENCES findings(finding_id),
    signal_date TEXT NOT NULL,
    due_date TEXT,
    capital REAL NOT NULL,
    expected_return REAL NOT NULL,
    evaluation_json TEXT NOT NULL,
    status TEXT NOT NULL,
    actual_return REAL,
    pnl REAL,
    grade TEXT,
    completed_date TEXT,
    FOREIGN KEY (hypothesis_id, version) REFERENCES experiment_versions(hypothesis_id, version),
    UNIQUE (hypothesis_id, trial_number)
);
"""


class Registry:
    def __init__(self, path: str | Path, policy: Policy):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.policy = policy
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(SCHEMA)
        self.connection.execute(
            "INSERT OR IGNORE INTO meta(key,value) VALUES('engine_identity',?)",
            ("pathfinder_fresh_v1",),
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def save_edition(self, edition: Edition, market: MarketData) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connection:
            self.connection.execute(
                """INSERT OR REPLACE INTO editions
                   (as_of,created_at,scanned_symbols,candidate_count,published_count,suppressed_count,
                    usefulness_threshold,market_regime,data_quality_json)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    edition.as_of, now, edition.scanned_symbols, edition.candidate_count,
                    len(edition.published), edition.suppressed_count, edition.usefulness_threshold,
                    edition.market_regime, _json(edition.data_quality),
                ),
            )
            for finding in edition.published:
                due = market.session_offset(finding.edition_date, finding.evaluation.horizon_sessions)
                self.connection.execute(
                    """INSERT OR IGNORE INTO findings
                       (finding_id,edition_date,template_id,public_subject,publication_score,payload_json,
                        evaluation_json,due_date,outcome_status)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (
                        finding.finding_id, finding.edition_date, finding.template_id,
                        finding.public_subject, finding.publication_score,
                        _json(finding.as_dict(public=False)), _json(asdict(finding.evaluation)),
                        due.strftime("%Y-%m-%d") if due is not None else None, "PENDING",
                    ),
                )

    def launch_experiment(self, finding: Finding, market: MarketData) -> dict[str, Any] | None:
        if not finding.experiment_eligible:
            return None
        hypothesis_id = _hypothesis_id(finding)
        existing = self.connection.execute(
            "SELECT * FROM hypotheses WHERE hypothesis_id=?", (hypothesis_id,)
        ).fetchone()
        if existing is None:
            retirement = (
                f"Evaluate each version after at least {self.policy.minimum_trials_per_version} forward trials; "
                f"retire after {self.policy.max_hypothesis_versions} failed rule versions."
            )
            with self.connection:
                self.connection.execute(
                    """INSERT INTO hypotheses
                       (hypothesis_id,template_id,public_subject,direction,horizon_sessions,status,
                        current_version,max_versions,created_date,retirement_rule)
                       VALUES(?,?,?,?,?,'ACTIVE',1,?,?,?)""",
                    (
                        hypothesis_id, finding.template_id, finding.public_subject,
                        finding.evaluation.direction, finding.evaluation.horizon_sessions,
                        self.policy.max_hypothesis_versions, finding.edition_date, retirement,
                    ),
                )
                self.connection.execute(
                    """INSERT INTO experiment_versions
                       (hypothesis_id,version,status,created_date,change_note,why_changed,rules_json)
                       VALUES(?,1,'ACTIVE',?,'Initial frozen version','Historical evidence cleared the experiment gate.',?)""",
                    (hypothesis_id, finding.edition_date, _json(asdict(finding.grading_rule))),
                )
            version = 1
        else:
            if existing["status"] == "RETIRED":
                return None
            version = int(existing["current_version"])
            open_run = self.connection.execute(
                "SELECT 1 FROM experiment_runs WHERE hypothesis_id=? AND status='OPEN'", (hypothesis_id,)
            ).fetchone()
            same_signal = self.connection.execute(
                "SELECT 1 FROM experiment_runs WHERE hypothesis_id=? AND signal_date=?",
                (hypothesis_id, finding.edition_date),
            ).fetchone()
            if open_run or same_signal:
                return None
            if version > 1:
                breadth = float(finding.internal.get("breadth", 0.5))
                aligned = breadth >= 0.50 if finding.evaluation.direction == "LONG" else breadth <= 0.50
                if not aligned:
                    return None

        trial_number = int(self.connection.execute(
            "SELECT COUNT(*) FROM experiment_runs WHERE hypothesis_id=?", (hypothesis_id,)
        ).fetchone()[0]) + 1
        run_id = f"{hypothesis_id}-V{version}-T{trial_number}"
        due = market.session_offset(finding.edition_date, finding.evaluation.horizon_sessions)
        with self.connection:
            self.connection.execute(
                """INSERT INTO experiment_runs
                   (run_id,hypothesis_id,version,trial_number,finding_id,signal_date,due_date,capital,
                    expected_return,evaluation_json,status)
                   VALUES(?,?,?,?,?,?,?,?,?,?,'OPEN')""",
                (
                    run_id, hypothesis_id, version, trial_number, finding.finding_id,
                    finding.edition_date, due.strftime("%Y-%m-%d") if due is not None else None,
                    self.policy.virtual_capital_per_experiment, finding.evidence.net_edge,
                    _json(asdict(finding.evaluation)),
                ),
            )
        return {"run_id": run_id, "hypothesis_id": hypothesis_id, "version": version, "trial_number": trial_number}

    @staticmethod
    def _plan(payload: str) -> EvaluationPlan:
        return EvaluationPlan(**json.loads(payload))

    def settle_due(self, market: MarketData, as_of: str) -> dict[str, int]:
        finding_rows = self.connection.execute(
            "SELECT * FROM findings WHERE outcome_status='PENDING' AND due_date IS NOT NULL AND due_date<=?",
            (as_of,),
        ).fetchall()
        run_rows = self.connection.execute(
            "SELECT * FROM experiment_runs WHERE status='OPEN' AND due_date IS NOT NULL AND due_date<=?",
            (as_of,),
        ).fetchall()
        settled_findings = 0
        settled_runs = 0
        with self.connection:
            for row in finding_rows:
                outcome = market.realize(self._plan(row["evaluation_json"]), row["edition_date"])
                if not outcome.available:
                    continue
                self.connection.execute(
                    """UPDATE findings SET outcome_status='GRADED',realized_return=?,grade=?,graded_at=?
                       WHERE finding_id=?""",
                    (outcome.scored_return, outcome.grade, as_of, row["finding_id"]),
                )
                settled_findings += 1
            for row in run_rows:
                outcome = market.realize(self._plan(row["evaluation_json"]), row["signal_date"])
                if not outcome.available:
                    continue
                actual = float(outcome.scored_return or 0.0)
                pnl = float(row["capital"]) * actual
                self.connection.execute(
                    """UPDATE experiment_runs SET status='COMPLETED',actual_return=?,pnl=?,grade=?,completed_date=?
                       WHERE run_id=?""",
                    (actual, pnl, outcome.grade, as_of, row["run_id"]),
                )
                settled_runs += 1
                self._evaluate_version(row["hypothesis_id"], int(row["version"]), as_of)
        return {"findings": settled_findings, "experiments": settled_runs}

    def _evaluate_version(self, hypothesis_id: str, evaluated_version: int, as_of: str) -> None:
        hypothesis = self.connection.execute(
            "SELECT * FROM hypotheses WHERE hypothesis_id=?", (hypothesis_id,)
        ).fetchone()
        if hypothesis is None or int(hypothesis["current_version"]) != evaluated_version:
            return
        returns = [
            float(row[0]) for row in self.connection.execute(
                """SELECT actual_return FROM experiment_runs
                     WHERE hypothesis_id=? AND version=? AND status='COMPLETED'
                     ORDER BY trial_number""",
                (hypothesis_id, evaluated_version),
            ).fetchall()
        ]
        if len(returns) < self.policy.minimum_trials_per_version or sum(returns) / len(returns) > 0:
            return
        maximum = int(hypothesis["max_versions"])
        if evaluated_version >= maximum:
            self.connection.execute("UPDATE hypotheses SET status='RETIRED' WHERE hypothesis_id=?", (hypothesis_id,))
            self.connection.execute(
                "UPDATE experiment_versions SET status='RETIRED' WHERE hypothesis_id=? AND version=?",
                (hypothesis_id, evaluated_version),
            )
            return
        next_version = evaluated_version + 1
        previous_rules = self.connection.execute(
            "SELECT rules_json FROM experiment_versions WHERE hypothesis_id=? AND version=?",
            (hypothesis_id, evaluated_version),
        ).fetchone()[0]
        self.connection.execute(
            "UPDATE experiment_versions SET status='SUPERSEDED' WHERE hypothesis_id=? AND version=?",
            (hypothesis_id, evaluated_version),
        )
        self.connection.execute(
            "UPDATE hypotheses SET current_version=? WHERE hypothesis_id=?",
            (next_version, hypothesis_id),
        )
        self.connection.execute(
            """INSERT INTO experiment_versions
               (hypothesis_id,version,status,created_date,change_note,why_changed,rules_json)
               VALUES(?,?,'ACTIVE',?,'Added breadth-direction confirmation',
                      'The prior version failed its minimum-trial checkpoint; the next version may launch only when market breadth agrees with the direction.',?)""",
            (hypothesis_id, next_version, as_of, previous_rules),
        )

    def scoreboard(self) -> dict[str, int]:
        rows = self.connection.execute(
            "SELECT COALESCE(grade,'PENDING') grade, COUNT(*) n FROM findings GROUP BY COALESCE(grade,'PENDING')"
        ).fetchall()
        board = {"RIGHT": 0, "WRONG": 0, "INCONCLUSIVE": 0, "PENDING": 0}
        board.update({row["grade"]: int(row["n"]) for row in rows})
        board["n"] = board["RIGHT"] + board["WRONG"] + board["INCONCLUSIVE"]
        return board

    def experiment_registry(self) -> list[dict[str, Any]]:
        hypotheses = self.connection.execute(
            "SELECT * FROM hypotheses ORDER BY created_date DESC, hypothesis_id"
        ).fetchall()
        result: list[dict[str, Any]] = []
        for hypothesis in hypotheses:
            versions = self.connection.execute(
                "SELECT * FROM experiment_versions WHERE hypothesis_id=? ORDER BY version",
                (hypothesis["hypothesis_id"],),
            ).fetchall()
            runs = self.connection.execute(
                "SELECT * FROM experiment_runs WHERE hypothesis_id=? ORDER BY trial_number",
                (hypothesis["hypothesis_id"],),
            ).fetchall()
            completed = [row for row in runs if row["status"] == "COMPLETED"]
            returns = [float(row["actual_return"]) for row in completed]
            cumulative = 1.0
            peak = 1.0
            max_drawdown = 0.0
            for value in returns:
                cumulative *= 1.0 + value
                peak = max(peak, cumulative)
                max_drawdown = max(max_drawdown, 1.0 - cumulative / peak)
            mean_return = sum(returns) / len(returns) if returns else None
            tried_versions = len({int(row["version"]) for row in runs})
            promotion = {
                "eligible": bool(
                    len(completed) >= self.policy.minimum_oos_trials_for_promotion
                    and mean_return is not None and mean_return > 0
                    and mean_return > self.policy.incumbent_net_return
                    and max_drawdown < self.policy.incumbent_max_drawdown
                ),
                "positive_oos_edge": mean_return is not None and mean_return > 0,
                "beats_incumbent_net_return": mean_return is not None and mean_return > self.policy.incumbent_net_return,
                "lower_drawdown_than_incumbent": max_drawdown < self.policy.incumbent_max_drawdown,
                "oos_trials": len(completed),
                "minimum_oos_trials": self.policy.minimum_oos_trials_for_promotion,
                "mean_net_return": mean_return,
                "max_drawdown": max_drawdown,
            }
            result.append({
                "hypothesis_id": hypothesis["hypothesis_id"],
                "template_id": hypothesis["template_id"],
                "public_subject": hypothesis["public_subject"],
                "direction": hypothesis["direction"],
                "horizon_sessions": hypothesis["horizon_sessions"],
                "status": hypothesis["status"],
                "current_version": hypothesis["current_version"],
                "versions_registered": len(versions),
                "variants_tried": tried_versions,
                "max_versions": hypothesis["max_versions"],
                "trial_count": len(runs),
                "retirement_rule": hypothesis["retirement_rule"],
                "versions": [dict(row) | {"rules": json.loads(row["rules_json"])} for row in versions],
                "runs": [dict(row) | {"evaluation": json.loads(row["evaluation_json"])} for row in runs],
                "promotion_gate": promotion,
            })
        return result

    def recent_outcomes(self, limit: int = 10) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """SELECT finding_id,edition_date,template_id,public_subject,realized_return,grade,graded_at
                 FROM findings WHERE outcome_status='GRADED'
                ORDER BY graded_at DESC, edition_date DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def latest_edition(self) -> dict[str, Any] | None:
        edition = self.connection.execute("SELECT * FROM editions ORDER BY as_of DESC LIMIT 1").fetchone()
        if edition is None:
            return None
        findings = self.connection.execute(
            "SELECT payload_json,grade,realized_return FROM findings WHERE edition_date=? ORDER BY publication_score DESC",
            (edition["as_of"],),
        ).fetchall()
        payloads = []
        for row in findings:
            payload = json.loads(row["payload_json"])
            payload["outcome"] = {"grade": row["grade"] or "PENDING", "realized_return": row["realized_return"]}
            payloads.append(payload)
        return dict(edition) | {"data_quality": json.loads(edition["data_quality_json"]), "findings": payloads}
