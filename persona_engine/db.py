"""
RND DB connection factory for the persona engine.

The persona engine reads from and writes ADDITIVELY to the same R&D SQLite DB the
Falcon research engine uses (the ~14 GB ``kanida_universe.db``). We never create a
new DB file (spec: "resolve via existing config; do not create a new DB file").

Resolution order:
  1. ``PERSONA_DB_PATH`` env var (explicit override)
  2. ``FALCON_DB_PATH`` env var (the canonical Falcon override)
  3. first existing of a list of known locations, preferring the big research DB
     in the prod tree (the worktree itself does not carry the gitignored DB).
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import List

# Known absolute / relative candidates, richest first. The research DB lives only
# in the prod working tree (it is gitignored), so we hardcode that absolute path
# as the primary fallback for use from any worktree.
_PROD_TREE = Path(
    r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
)

_CANDIDATES: List[Path] = [
    _PROD_TREE / "universe_engine" / "data" / "db" / "kanida_universe.db",
    _PROD_TREE / "data" / "db" / "kanida_universe.db",
    Path(__file__).resolve().parents[1] / "universe_engine" / "data" / "db" / "kanida_universe.db",
    Path(__file__).resolve().parents[1] / "data" / "db" / "kanida_universe.db",
]


def resolve_db_path() -> Path:
    env = os.environ.get("PERSONA_DB_PATH") or os.environ.get("FALCON_DB_PATH")
    if env:
        return Path(env)
    for c in _CANDIDATES:
        if c.exists():
            return c
    # Last resort: return the primary candidate even if missing, so the error
    # message is actionable.
    return _CANDIDATES[0]


DB_PATH = resolve_db_path()


def connect(read_only: bool = False) -> sqlite3.Connection:
    """Open the RND DB. WAL + relaxed sync for fast bulk writes (research DB)."""
    path = resolve_db_path()
    if not path.exists():
        raise FileNotFoundError(
            f"RND DB not found at {path}. Set PERSONA_DB_PATH or FALCON_DB_PATH."
        )
    if read_only:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=60)
    else:
        con = sqlite3.connect(str(path), timeout=120)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=120000")
    return con


def table_exists(con: sqlite3.Connection, name: str) -> bool:
    return (
        con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        is not None
    )
