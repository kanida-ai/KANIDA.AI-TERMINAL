"""
The authoritative store. Append-only, hash-chained, dialect-portable.

`Repository` writes the tables specified in `docs/DATA_MODEL.md`. The SQL is written
once and executed against either backend:

    sqlite    (migrations/0002_pathfinder_sqlite.sql)  — verified, what P1 ran on
    postgres  (migrations/0001_pathfinder.sql)         — the target; needs a driver
                                                         and credentials P1 did not have

Only three things differ between them: the placeholder style, the array encoding, and
the two law-enforcing functions. `_Dialect` holds all three, and there is nothing else
backend-specific in this file.

The hash chain is the thing to understand: every published row on a chained table
carries `prev_hash` (the previous row's hash for that chain) and `row_hash` (a SHA-256
over its own canonical content plus `prev_hash`). Rewriting history therefore requires
rewriting every row after it, and `verify_chain()` will say so.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

MODEL_NAME_RE = re.compile(r"(claude|gpt|gemini|sonnet|haiku|opus|llm)", re.IGNORECASE)
REF_TOKEN_RE = re.compile(r"\{\{(?:fact|exp|evd|ver):[A-Za-z0-9_.\-]+\}\}")

#: Tables whose rows form a hash chain (the published record).
CHAINED = (
    "constitution_versions", "experiment_state", "outcomes",
    "decisions", "learning_events", "post_mortems",
)

#: Columns the DATABASE generates, so they are not part of what was hashed at insert.
#: Including them would make every chain verify as broken, which is a worse failure
#: mode than not having a chain at all: an alarm that is always on is not an alarm.
GENERATED_COLUMNS = {"state_id", "parameter_id", "recall_id"}


def pf_names_a_model(value: Optional[str]) -> int:
    """DB-registered. `computed_by` may never name a model — the LLM does not compute."""
    return 1 if (value and MODEL_NAME_RE.search(value)) else 0


def pf_has_bare_numeral(value: Optional[str]) -> int:
    """DB-registered. LLM prose, with `{{fact:…}}` stripped, may not contain a digit."""
    if not value:
        return 0
    return 1 if any(ch.isdigit() for ch in REF_TOKEN_RE.sub("", value)) else 0


def canonical(payload: dict[str, Any]) -> str:
    """Stable serialisation for hashing. Sorted keys; dates as ISO strings."""
    def norm(v: Any) -> Any:
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, dict):
            return {k: norm(x) for k, x in sorted(v.items())}
        if isinstance(v, (list, tuple)):
            return [norm(x) for x in v]
        return v
    return json.dumps(norm(payload), sort_keys=True, separators=(",", ":"), default=str)


def row_hash(payload: dict[str, Any], prev_hash: Optional[str]) -> str:
    return hashlib.sha256(f"{prev_hash or ''}|{canonical(payload)}".encode()).hexdigest()


class _Dialect:
    """Everything that differs between SQLite and Postgres, in one place."""

    def __init__(self, kind: str) -> None:
        if kind not in ("sqlite", "postgres"):
            raise ValueError(f"unknown dialect {kind!r}")
        self.kind = kind

    @property
    def ph(self) -> str:
        return "?" if self.kind == "sqlite" else "%s"

    def array(self, values: Sequence[str]) -> Any:
        """`text[]` on Postgres; a JSON array in TEXT on SQLite."""
        return json.dumps(list(values)) if self.kind == "sqlite" else list(values)

    def unarray(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            return list(value)
        return list(json.loads(value))

    def jsonb(self, value: dict[str, Any]) -> Any:
        return json.dumps(value, sort_keys=True)

    def unjson(self, value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        return value if isinstance(value, dict) else json.loads(value)

    def boolean(self, value: Optional[bool]) -> Any:
        if value is None:
            return None
        return int(value) if self.kind == "sqlite" else bool(value)


class AppendOnlyViolation(RuntimeError):
    """Someone tried to edit history. It is the only bug class this schema cannot forgive."""


class Repository:
    """
    Reads and writes the Pathfinder authoritative tables.

    Nothing in this class interprets a number. It stores what the engine computed and
    what the gateway returned, with the provenance of each.
    """

    def __init__(self, db_path: str, *, dialect: str = "sqlite") -> None:
        self.dialect = _Dialect(dialect)
        if dialect != "sqlite":  # pragma: no cover — needs a driver P1 did not have
            raise NotImplementedError(
                "the Postgres backend needs psycopg and credentials; see docs/handbacks/P1.md. "
                "The SQL below is dialect-portable — only `_Dialect` changes."
            )
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # A SQLite connection belongs to the thread that made it, and FastAPI runs sync
        # endpoints in a threadpool — so a process-wide store handed the same connection
        # to whichever worker picked up the request, and every request after the first
        # died. One connection per thread, created on first use.
        self._local = threading.local()

    @property
    def con(self) -> sqlite3.Connection:
        con = getattr(self._local, "con", None)
        if con is None:
            con = sqlite3.connect(str(self.path))
            con.row_factory = sqlite3.Row
            # The two laws the DDL delegates to the application, registered on EVERY
            # connection — an unregistered one cannot insert at all, which is the
            # correct direction to fail in.
            con.create_function("pf_names_a_model", 1, pf_names_a_model, deterministic=True)
            con.create_function("pf_has_bare_numeral", 1, pf_has_bare_numeral, deterministic=True)
            con.execute("PRAGMA foreign_keys = ON")
            self._local.con = con
        return con

    # ── schema ──────────────────────────────────────────────────────────────

    def migrate(self, sql_path: str | Path) -> None:
        self.con.executescript(Path(sql_path).read_text(encoding="utf-8"))
        self.con.commit()

    def close(self) -> None:
        con = getattr(self._local, "con", None)
        if con is not None:
            con.close()
            self._local.con = None

    # ── low-level ───────────────────────────────────────────────────────────

    def _insert(self, table: str, row: dict[str, Any]) -> None:
        cols = ", ".join(row)
        marks = ", ".join(self.dialect.ph for _ in row)
        self.con.execute(f"INSERT INTO {table} ({cols}) VALUES ({marks})", list(row.values()))

    def _chain_prev(self, table: str) -> Optional[str]:
        cur = self.con.execute(f"SELECT row_hash FROM {table} ORDER BY rowid DESC LIMIT 1")
        r = cur.fetchone()
        return None if r is None else str(r[0])

    def _insert_chained(self, table: str, row: dict[str, Any]) -> str:
        prev = self._chain_prev(table)
        h = row_hash(row, prev)
        self._insert(table, {**row, "prev_hash": prev, "row_hash": h})
        return h

    def commit(self) -> None:
        self.con.commit()

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        return list(self.con.execute(sql, list(params)))

    def one(self, sql: str, params: Sequence[Any] = ()) -> Optional[sqlite3.Row]:
        cur = self.con.execute(sql, list(params))
        return cur.fetchone()

    # ── writes ──────────────────────────────────────────────────────────────

    def put_constitution(self, *, version: str, document: dict[str, Any], approved_by: str,
                         effective_from: str, previous_version: Optional[str],
                         created_at: datetime) -> None:
        if self.one("SELECT 1 FROM constitution_versions WHERE version = ?", [version]):
            return                                    # idempotent; never rewritten
        self._insert_chained("constitution_versions", {
            "version": version, "previous_version": previous_version,
            "effective_from": effective_from, "document": self.dialect.jsonb(document),
            "authored_by": "human", "approved_by": approved_by,
            "created_at": created_at.isoformat(),
        })

    def put_llm_call(self, **row: Any) -> None:
        row["batched"] = self.dialect.boolean(row.get("batched", False))
        row["ok"] = self.dialect.boolean(row.get("ok", True))
        self._insert("llm_calls", row)

    def put_hypothesis(self, **row: Any) -> None:
        row["novelty_recall_ids"] = self.dialect.array(row.get("novelty_recall_ids") or [])
        self._insert("hypotheses", row)

    def put_experiment(self, **row: Any) -> None:
        self._insert("experiments", row)

    def put_decision(self, **row: Any) -> str:
        row["evidence_ids"] = self.dialect.array(row.get("evidence_ids") or [])
        return self._insert_chained("decisions", row)

    def put_state(self, **row: Any) -> str:
        return self._insert_chained("experiment_state", row)

    def put_strategy_version(self, **row: Any) -> None:
        row["rulebook"] = self.dialect.jsonb(row["rulebook"])
        self._insert("strategy_versions", row)

    def put_parameter(self, **row: Any) -> None:
        self._insert("parameters", row)

    def put_fact(self, **row: Any) -> None:
        self._insert("facts", row)

    def put_outcome(self, **row: Any) -> str:
        return self._insert_chained("outcomes", row)

    def put_trade(self, **row: Any) -> None:
        self._insert("trades", row)

    def put_evidence(self, **row: Any) -> None:
        row["fact_ids"] = self.dialect.array(row.get("fact_ids") or [])
        self._insert("evidence", row)

    def put_trigger(self, **row: Any) -> None:
        row["fact_ids"] = self.dialect.array(row.get("fact_ids") or [])
        row["detail"] = self.dialect.jsonb(row.get("detail") or {})
        self._insert("triggers", row)

    def put_story_line(self, **row: Any) -> None:
        row["fact_ids"] = self.dialect.array(row.get("fact_ids") or [])
        self._insert("story_lines", row)

    def put_learning_event(self, **row: Any) -> str:
        row["evidence_ids"] = self.dialect.array(row.get("evidence_ids") or [])
        row["improved"] = self.dialect.boolean(row.get("improved"))
        return self._insert_chained("learning_events", row)

    def put_post_mortem(self, **row: Any) -> str:
        row["evidence_ids"] = self.dialect.array(row.get("evidence_ids") or [])
        return self._insert_chained("post_mortems", row)

    def put_cycle(self, **row: Any) -> None:
        self._insert("research_cycles", row)

    def put_recall(self, **row: Any) -> None:
        """
        Recall is a POINTER, never truth. `embedding` is a cheap deterministic
        bag-of-tokens hash vector, not a learned one: P1 needs "have I explored
        something like this before?" to work offline, and nothing is ever read out of
        here as evidence — only as a pointer back to an authoritative row.
        """
        row["embedding"] = json.dumps(row["embedding"])
        self.con.execute(
            "INSERT OR IGNORE INTO recall_embeddings "
            "(kind, source_table, source_id, snippet, embedding, embed_model, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [row["kind"], row["source_table"], row["source_id"], row["snippet"],
             row["embedding"], row["embed_model"], row["created_at"]],
        )

    # ── integrity ───────────────────────────────────────────────────────────

    def verify_chain(self, table: str) -> tuple[bool, str]:
        """Recompute every hash in order. Returns `(ok, message)`."""
        if table not in CHAINED:
            return True, f"{table} is not a chained table"
        rows = self.query(f"SELECT * FROM {table} ORDER BY rowid")
        prev: Optional[str] = None
        for r in rows:
            d = {k: r[k] for k in r.keys()
                 if k not in ("prev_hash", "row_hash") and k not in GENERATED_COLUMNS}
            expect = row_hash(d, prev)
            if r["prev_hash"] != prev:
                return False, f"{table}: broken link at row_hash={r['row_hash']}"
            if r["row_hash"] != expect:
                return False, f"{table}: content hash mismatch at prev_hash={prev}"
            prev = str(r["row_hash"])
        return True, f"{table}: {len(rows)} rows, chain intact"

    def verify_all_chains(self) -> list[tuple[str, bool, str]]:
        return [(t, *self.verify_chain(t)) for t in CHAINED]
