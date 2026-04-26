from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any


SNAPSHOT_DB_NAME = "kanida_prototype_snapshots.db"


def snapshot_db_path(outputs: Path) -> Path:
    path = outputs / "snapshots" / SNAPSHOT_DB_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def connect(outputs: Path) -> sqlite3.Connection:
    con = sqlite3.connect(snapshot_db_path(outputs))
    con.row_factory = sqlite3.Row
    init_schema(con)
    return con


def init_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        create table if not exists snapshot_runs (
            id integer primary key autoincrement,
            run_type text not null,
            status text not null,
            started_at text not null,
            finished_at text,
            learned_patterns integer default 0,
            live_opportunities integer default 0,
            message text
        );

        create table if not exists outcome_first_patterns (
            snapshot_run_id integer,
            market text,
            ticker text,
            direction text,
            target_move real,
            forward_window integer,
            pattern_size integer,
            behavior_pattern text,
            occurrences integer,
            hits integer,
            baseline_probability real,
            target_probability real,
            raw_probability real,
            probability_ci_low real,
            probability_ci_high real,
            trusted_probability real,
            display_probability real,
            credibility text,
            lift real,
            avg_forward_return real,
            recent_probability real,
            stability real,
            opportunity_score real,
            decay_flag integer,
            tier text
        );

        create table if not exists outcome_first_live_opportunities (
            snapshot_run_id integer,
            market text,
            ticker text,
            direction text,
            target_move real,
            forward_window integer,
            pattern_size integer,
            behavior_pattern text,
            occurrences integer,
            hits integer,
            baseline_probability real,
            target_probability real,
            raw_probability real,
            probability_ci_low real,
            probability_ci_high real,
            trusted_probability real,
            display_probability real,
            credibility text,
            lift real,
            avg_forward_return real,
            recent_probability real,
            stability real,
            opportunity_score real,
            decay_flag integer,
            tier text,
            latest_date text,
            current_close real,
            current_behavior text,
            current_atoms text,
            similarity real,
            decision_score real,
            setup_summary text
        );

        create index if not exists idx_outcome_live_market
          on outcome_first_live_opportunities(snapshot_run_id, market, direction, decision_score desc);
        create index if not exists idx_outcome_patterns_stock
          on outcome_first_patterns(snapshot_run_id, market, ticker, direction, target_move, forward_window);
        """
    )
    con.commit()


def start_run(outputs: Path, run_type: str) -> int:
    con = connect(outputs)
    try:
        cur = con.execute(
            "insert into snapshot_runs(run_type, status, started_at) values (?, ?, ?)",
            [run_type, "running", datetime.now().isoformat(timespec="seconds")],
        )
        con.commit()
        return int(cur.lastrowid)
    finally:
        con.close()


def finish_run(
    outputs: Path,
    run_id: int,
    status: str,
    learned_patterns: int,
    live_opportunities: int,
    message: str = "",
) -> None:
    con = connect(outputs)
    try:
        con.execute(
            """
            update snapshot_runs
               set status=?, finished_at=?, learned_patterns=?, live_opportunities=?, message=?
             where id=?
            """,
            [
                status,
                datetime.now().isoformat(timespec="seconds"),
                learned_patterns,
                live_opportunities,
                message,
                run_id,
            ],
        )
        con.commit()
    finally:
        con.close()


def write_snapshot(outputs: Path, run_id: int, learned: list[dict[str, Any]], live: list[dict[str, Any]]) -> None:
    con = connect(outputs)
    try:
        con.execute("delete from outcome_first_patterns")
        con.execute("delete from outcome_first_live_opportunities")
        _insert_rows(con, "outcome_first_patterns", run_id, learned)
        _insert_rows(con, "outcome_first_live_opportunities", run_id, live)
        con.commit()
    finally:
        con.close()


def latest_run(outputs: Path) -> dict[str, Any] | None:
    con = connect(outputs)
    try:
        row = con.execute(
            "select * from snapshot_runs order by id desc limit 1"
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def latest_rows(outputs: Path, table: str) -> list[dict[str, Any]]:
    con = connect(outputs)
    try:
        run = con.execute(
            "select id from snapshot_runs where status='success' order by id desc limit 1"
        ).fetchone()
        if not run:
            return []
        order_col = "decision_score" if table.endswith("live_opportunities") else "opportunity_score"
        rows = con.execute(f"select * from {table} where snapshot_run_id=? order by {order_col} desc", [run["id"]]).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def _insert_rows(con: sqlite3.Connection, table: str, run_id: int, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    cols = [r[1] for r in con.execute(f"pragma table_info({table})").fetchall()]
    insert_cols = cols
    placeholders = ",".join("?" for _ in insert_cols)
    sql = f"insert into {table}({','.join(insert_cols)}) values ({placeholders})"
    payload = []
    for row in rows:
        values = []
        for col in insert_cols:
            if col == "snapshot_run_id":
                values.append(run_id)
            else:
                value = row.get(col)
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, ensure_ascii=True)
                values.append(value)
        payload.append(values)
    con.executemany(sql, payload)
