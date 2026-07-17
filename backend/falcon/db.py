"""Falcon DB connection helpers.

Single entry point for SQLite connections. Both kanida_quant.db (legacy auth /
universe) and kanida_universe.db (Falcon) are accessed through here so paths
+ pragmas are consistent.
"""
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import FALCON_DB, LEGACY_DB, verify_falcon_db


def connect_falcon(timeout: float = 60.0) -> sqlite3.Connection:
    """Open the Falcon (universe) DB. Read-mostly in API requests."""
    # A2: same fail-loud contract as startup — names the expected path and the
    # env var that sets it, and never silently substitutes the R&D DB.
    # ProductionDBMissingError subclasses FileNotFoundError, so this raises the
    # same exception type as before; existing handlers are unaffected.
    verify_falcon_db(FALCON_DB)
    con = sqlite3.connect(str(FALCON_DB), timeout=timeout)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode = WAL")
    # SPRINT CLUSTER 8 ITEM 6(b) — WAL durability for the AutoTrade write path.
    # The AutoTrade session/position store lives in this DB; synchronous=FULL (was
    # NORMAL) fsyncs the WAL on every commit so a live order-book write survives an
    # OS crash / power loss, not just an app crash (NORMAL's guarantee). Strictly
    # safer; the only cost is a marginally slower commit. Reads are unaffected.
    con.execute("PRAGMA synchronous = FULL")
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
