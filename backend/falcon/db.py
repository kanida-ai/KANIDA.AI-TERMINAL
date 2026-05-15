"""Falcon DB connection helpers.

Single entry point for SQLite connections. Both kanida_quant.db (legacy auth /
universe) and kanida_universe.db (Falcon) are accessed through here so paths
+ pragmas are consistent.
"""
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import FALCON_DB, LEGACY_DB


def connect_falcon(timeout: float = 60.0) -> sqlite3.Connection:
    """Open the Falcon (universe) DB. Read-mostly in API requests."""
    if not FALCON_DB.exists():
        raise FileNotFoundError(
            f"Falcon DB not found at {FALCON_DB}. "
            "Did you bundle kanida_universe.db into the Docker image?"
        )
    con = sqlite3.connect(str(FALCON_DB), timeout=timeout)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA synchronous = NORMAL")
    return con


def connect_legacy(timeout: float = 60.0) -> sqlite3.Connection:
    """Open the legacy kanida_quant.db — used for kite_tokens + auth."""
    con = sqlite3.connect(str(LEGACY_DB), timeout=timeout)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode = WAL")
    return con


@contextmanager
def falcon_conn() -> Iterator[sqlite3.Connection]:
    """Context-managed connection."""
    con = connect_falcon()
    try:
        yield con
    finally:
        con.close()


@contextmanager
def legacy_conn() -> Iterator[sqlite3.Connection]:
    con = connect_legacy()
    try:
        yield con
    finally:
        con.close()
