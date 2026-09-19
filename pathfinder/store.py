"""SQLite checkpoint plus append-only audit events. All cycle mutations are atomic."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

from .models import canonical


class Store:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript("""
          CREATE TABLE IF NOT EXISTS checkpoint (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, at TEXT NOT NULL, cycle_date TEXT NOT NULL, kind TEXT NOT NULL, body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS model_calls (id INTEGER PRIMARY KEY, day TEXT NOT NULL);
          CREATE TRIGGER IF NOT EXISTS no_event_update BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT,'Events are append-only'); END;
          CREATE TRIGGER IF NOT EXISTS no_event_delete BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT,'Events are append-only'); END;
        """)

    @contextmanager
    def transaction(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    def read(self):
        row = self.conn.execute("SELECT body FROM checkpoint WHERE id=1").fetchone()
        return json.loads(row[0]) if row else None

    def save(self, state):
        self.conn.execute("INSERT INTO checkpoint VALUES(1,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body", (canonical(state),))

    def event(self, day, kind, body):
        self.conn.execute("INSERT INTO events(at,cycle_date,kind,body) VALUES(?,?,?,?)",
                          (datetime.now(timezone.utc).isoformat(), day, kind, canonical(body)))

    def reserve_model_call(self, limit):
        # Committed independently: failed calls consume the call budget too.
        day = datetime.now(timezone.utc).date().isoformat()
        with self.transaction():
            n = self.conn.execute("SELECT COUNT(*) FROM model_calls WHERE day=?", (day,)).fetchone()[0]
            if n >= limit:
                raise ValueError("Daily model-call budget exhausted")
            self.conn.execute("INSERT INTO model_calls(day) VALUES(?)", (day,))

    def events(self, limit=100):
        return [{"id": r[0], "date": r[1], "kind": r[2], "body": json.loads(r[3])} for r in self.conn.execute(
            "SELECT id,cycle_date,kind,body FROM events ORDER BY id DESC LIMIT ?", (limit,))]

    def close(self):
        self.conn.close()
