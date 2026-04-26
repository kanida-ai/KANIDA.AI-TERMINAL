from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable


class ReadOnlySQLite:
    """Tiny read-only SQLite wrapper.

    Uses URI `mode=ro` and query-only pragma. This prototype never writes to the
    production database.
    """

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        uri_path = self.db_path.as_posix()
        self.conn = sqlite3.connect(f"file:{uri_path}?mode=ro", uri=True)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA query_only=ON")

    def close(self) -> None:
        self.conn.close()

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        cur = self.conn.execute(sql, tuple(params))
        return [dict(row) for row in cur.fetchall()]

    def scalar(self, sql: str, params: Iterable[Any] = ()) -> Any:
        row = self.conn.execute(sql, tuple(params)).fetchone()
        return None if row is None else row[0]

    def table_names(self) -> list[str]:
        rows = self.query(
            "select name from sqlite_master where type='table' order by name"
        )
        return [row["name"] for row in rows]

    def table_info(self, table: str) -> list[dict[str, Any]]:
        return self.query(f"PRAGMA table_info({table})")

    def has_table(self, table: str) -> bool:
        return (
            self.scalar(
                "select count(*) from sqlite_master where type='table' and name=?",
                [table],
            )
            or 0
        ) > 0

    def __enter__(self) -> "ReadOnlySQLite":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
