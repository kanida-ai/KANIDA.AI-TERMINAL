"""
Pathfinder data source seam.

The router NEVER touches a data store directly — it goes through `get_store()`.
That is the whole point of P0: P2 (frontend) builds against `FixtureStore`, P1
(engine) lands `PostgresStore`, and swapping is an env var, not a code change.

    KANIDA_PATHFINDER_SOURCE = mock      (default — the honest fixtures)
                             = engine    (P1 — the real loop's authoritative tables,
                                          SQLite mirror at KANIDA_PATHFINDER_DB)
                             = postgres  (the same tables on the target Postgres; needs a
                                          driver and credentials P1 did not have)
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Protocol

from . import fixture_data as fx
from .schemas import (
    ExperimentDetail,
    ExperimentListResponse,
    ExperimentStatus,
    LearningsResponse,
    LoopResponse,
)


class PathfinderStore(Protocol):
    """The four reads the P0 contract needs. P1 implements this over Postgres."""

    #: Human-readable provenance of the data being served — surfaced by the mock
    #: server's root so nobody ever mistakes fixtures for engine output.
    source_name: str

    def loop(self) -> LoopResponse: ...

    def experiments(self, status: Optional[ExperimentStatus] = None) -> ExperimentListResponse: ...

    def experiment(self, experiment_id: str) -> Optional[ExperimentDetail]: ...

    def learnings(self) -> LearningsResponse: ...


class FixtureStore:
    """P0 store: hand-authored honest fixtures. NOT engine output, NOT real results."""

    source_name = "mock-fixtures (P0) — hand-authored, not engine output"

    def loop(self) -> LoopResponse:
        return fx.LOOP

    def experiments(self, status: Optional[ExperimentStatus] = None) -> ExperimentListResponse:
        return fx.experiment_list(status)

    def experiment(self, experiment_id: str) -> Optional[ExperimentDetail]:
        return fx.experiment_detail(experiment_id)

    def learnings(self) -> LearningsResponse:
        return fx.LEARNINGS


_store: Optional[PathfinderStore] = None


def get_store() -> PathfinderStore:
    """Resolve the configured store once per process."""
    global _store
    if _store is not None:
        return _store
    source = os.environ.get("KANIDA_PATHFINDER_SOURCE", "mock").strip().lower()
    if source == "mock":
        _store = FixtureStore()
    elif source == "engine":
        from .store_engine import EngineStore
        db = os.environ.get(
            "KANIDA_PATHFINDER_DB",
            str(Path(__file__).resolve().parents[2] / "var" / "pathfinder.db"),
        )
        if not Path(db).exists():
            # Never silently serve fixtures as if they were engine output. That would
            # be the worst bug this product could ship.
            raise RuntimeError(
                f"KANIDA_PATHFINDER_SOURCE=engine but there is no run database at {db}. "
                "Run `python scripts/run_pathfinder_loop.py` first. Refusing to fall back "
                "to fixtures."
            )
        _store = EngineStore(db)
    elif source == "postgres":
        raise RuntimeError(
            "KANIDA_PATHFINDER_SOURCE=postgres: the engine writes the same tables, but the "
            "Postgres backend needs psycopg and credentials that session P1 did not have "
            "(see docs/handbacks/P1.md). Use `engine` for the verified SQLite mirror. "
            "Refusing to fall back to fixtures."
        )
    else:
        raise RuntimeError(f"unknown KANIDA_PATHFINDER_SOURCE: {source!r}")
    return _store


def set_store(store: Optional[PathfinderStore]) -> None:
    """Test/DI hook. Pass None to reset."""
    global _store
    _store = store
