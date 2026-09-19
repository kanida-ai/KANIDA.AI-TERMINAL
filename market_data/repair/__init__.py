"""Data repair workflow (contract section 3) -- W3.

Pipeline::

    diagnose  ->  plan  ->  refresh  ->  reconcile  ->  report

Hard rules this package obeys (contract sections 3 and 6):

* ``db/kanida.db`` is opened **read-only** (``mode=ro`` + ``query_only=ON``).
  Nothing in this package ever writes to it.
* Every write to ``db/market15.db`` goes through W2's store API.  There is no
  fallback path that writes candles with raw SQL -- if the store API is
  missing, we fail loudly rather than inventing a second writer.
* Nothing is ever silently patched.  A suspicious low is never replaced with a
  daily low; unfavourable trades are never deleted; unresolved rows are
  quarantined with a label, not invented.

Only the repair package lives here.  ``market_data.provider`` /
``kite_provider`` (W1) and ``market_data.store`` / ``aggregate`` / ``validate``
/ ``calendar`` (W2) are imported, never modified.
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

# ---------------------------------------------------------------------------
# paths
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
KANIDA_DB = Path(os.environ.get("KANIDA_OHLC_DB", ROOT / "db" / "kanida.db"))
MARKET15_DB = Path(os.environ.get("MARKET15_DB", ROOT / "db" / "market15.db"))
RAW_ARCHIVE_DIR = Path(os.environ.get("RAW_ARCHIVE_DIR", ROOT / "db" / "raw_archive"))
ARTIFACTS = Path(__file__).resolve().parent / "artifacts"
REPORT_MD = ROOT / "docs" / "pattern_research" / "DATA_REPAIR_REPORT.md"
REPORT_JSON = ROOT / "docs" / "pattern_research" / "DATA_REPAIR_REPORT.json"

#: The withheld source-quality screen that named the 37 flagged stocks.
SOURCE_QUALITY_SCREEN = (
    ROOT / "market_scanner" / "output" / "expanded_research"
    / "8ae6ddc251e80668239e" / "SOURCE_QUALITY_SCREEN.json"
)

BASE_TIMEFRAME = "15minute"
DAILY_TIMEFRAME = "day"

# ---------------------------------------------------------------------------
# known-unresolvable cases (contract section 3.5) -- labelled, never invented
# ---------------------------------------------------------------------------

#: Symbols this Kite account cannot see at all.  Not a data error we can fix.
INVISIBLE_TO_ACCOUNT = ("LTIM", "GSPL", "GUJGASLTD", "JBCHEPHARM")

#: A genuine trading suspension, not a gap to be filled.
KNOWN_SUSPENSIONS = {
    "FORCEMOT": [("2023-10-26", "2024-02-13", "genuine trading suspension")],
}

# ---------------------------------------------------------------------------
# read-only access to the legacy 158 GB store
# ---------------------------------------------------------------------------


def read_only_conn(path: Path | str = KANIDA_DB, *, timeout: float = 60.0) -> sqlite3.Connection:
    """Open ``path`` read-only.  Two locks, belt and braces: ``mode=ro`` in the
    URI (the OS-level guarantee) and ``query_only=ON`` (the SQLite-level one).
    """
    uri = "file:" + Path(path).as_posix() + "?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=timeout)
    con.execute("PRAGMA query_only=ON")
    con.execute("PRAGMA temp_store=MEMORY")
    con.row_factory = sqlite3.Row
    return con


@dataclass(frozen=True)
class Symbol:
    symbol: str
    exchange: str
    kite_token: Optional[int]
    in_nifty500: bool


def universe(conn: sqlite3.Connection, *, nifty500_only: bool = True) -> list[Symbol]:
    """The NIFTY 500 universe as ``instrument_labels`` records it."""
    where = "WHERE in_nifty500=1 AND is_active=1" if nifty500_only else "WHERE is_active=1"
    rows = conn.execute(
        f"SELECT symbol, COALESCE(exchange,'NSE') AS exchange, kite_token, in_nifty500 "
        f"FROM instrument_labels {where} ORDER BY symbol"
    ).fetchall()
    return [
        Symbol(r["symbol"], r["exchange"], r["kite_token"], bool(r["in_nifty500"]))
        for r in rows
    ]


def symbol_lookup(conn: sqlite3.Connection) -> dict[str, Symbol]:
    return {s.symbol: s for s in universe(conn, nifty500_only=False)}


def flagged_symbols(path: Path = SOURCE_QUALITY_SCREEN) -> list[str]:
    """The 37 stocks the withheld source-quality screen flagged.

    Read from the screen's own JSON so this never drifts from the evidence.
    """
    if not Path(path).exists():
        return []
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return sorted({r["symbol"] for r in data.get("flagged", [])})


def flagged_detail(path: Path = SOURCE_QUALITY_SCREEN) -> list[dict]:
    if not Path(path).exists():
        return []
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return [
        {"symbol": r["symbol"], "timeframe": r["timeframe"], "flag_counts": r["flag_counts"]}
        for r in data.get("flagged", [])
    ]


# ---------------------------------------------------------------------------
# adapters onto W1 / W2 (imported, never edited)
# ---------------------------------------------------------------------------


class MissingDependency(RuntimeError):
    """A W1/W2 module or callable this step needs is not available yet."""


def _first_callable(obj: Any, names: Iterable[str]) -> Optional[Callable]:
    for n in names:
        fn = getattr(obj, n, None)
        if callable(fn):
            return fn
    return None


def resolve_provider(provider_id: Optional[str] = None, **kwargs):
    """Return W1's configured provider instance."""
    try:
        from market_data.provider import get_provider  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on W1
        raise MissingDependency(f"market_data.provider is not importable yet: {exc}") from exc
    return get_provider(provider_id, **kwargs)


class StoreAdapter:
    """A thin, defensive shim over W2's ``market_data.store``.

    W2 owns the schema and the writes.  This adapter only *finds* the callable
    W2 exposes for each operation, so a naming difference between workers turns
    into one clear error instead of a second, divergent writer.
    """

    #: operation -> candidate names, best first
    _NAMES = {
        "write_candles": ("write_candles", "upsert_candles", "put_candles",
                          "write_bars", "insert_candles", "save_candles"),
        "archive_raw": ("archive_raw", "write_raw_archive", "archive_payload",
                        "record_raw", "save_raw"),
        "record_correction": ("record_correction", "write_correction",
                              "add_correction", "log_correction"),
        "start_run": ("start_run", "begin_run", "open_run", "start_ingest_run"),
        "finish_run": ("finish_run", "end_run", "close_run", "finish_ingest_run"),
        "freeze_snapshot": ("freeze_snapshot", "create_snapshot", "snapshot",
                            "make_snapshot"),
        "record_finding": ("record_findings", "record_finding", "write_finding",
                           "add_finding", "record_quality_finding"),
        "read_candles": ("read_candles", "get_candles", "load_candles",
                         "candles", "fetch_candles"),
        "last_bar": ("last_bar", "latest_bar", "max_bar_start", "last_bar_start"),
        "close": ("close",),
    }

    def __init__(self, store: Any):
        self.store = store
        self.resolved: dict[str, Optional[Callable]] = {
            op: _first_callable(store, names) for op, names in self._NAMES.items()
        }

    def require(self, op: str) -> Callable:
        fn = self.resolved.get(op)
        if fn is None:
            raise MissingDependency(
                f"market_data.store exposes none of {self._NAMES[op]} for operation {op!r}. "
                f"Available: {sorted(n for n in dir(self.store) if not n.startswith('_'))}"
            )
        return fn

    def has(self, op: str) -> bool:
        return self.resolved.get(op) is not None

    def __getattr__(self, item):  # pass-through for anything W2 exposes directly
        return getattr(self.store, item)


def resolve_store(path: Path | str = MARKET15_DB, **kwargs) -> StoreAdapter:
    """Open W2's store on ``db/market15.db``.  Never opens kanida.db."""
    p = Path(path)
    if p.resolve() == KANIDA_DB.resolve():
        raise RuntimeError("refusing to open kanida.db for writing (contract section 2)")
    try:
        import market_data.store as store_mod  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on W2
        raise MissingDependency(f"market_data.store is not importable yet: {exc}") from exc
    opener = _first_callable(store_mod, ("open_store", "Store", "MarketStore",
                                         "connect", "get_store", "open"))
    if opener is None:
        raise MissingDependency(
            "market_data.store has no open_store/Store/connect entry point; "
            f"found: {sorted(n for n in dir(store_mod) if not n.startswith('_'))}"
        )
    return StoreAdapter(opener(str(p), **kwargs))


def resolve_validate():
    """W2's validation checks.  Returns the module; callers probe for checks."""
    try:
        import market_data.validate as validate_mod  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on W2
        raise MissingDependency(f"market_data.validate is not importable yet: {exc}") from exc
    return validate_mod


def dependencies_ready() -> dict[str, bool]:
    """Which W1/W2 pieces import cleanly right now (used by ``cli.py --wait``)."""
    out: dict[str, bool] = {}
    for name in ("market_data.types", "market_data.provider", "market_data.kite_provider",
                 "market_data.calendar", "market_data.aggregate", "market_data.store",
                 "market_data.validate"):
        try:
            __import__(name)
            out[name] = True
        except Exception:
            out[name] = False
    return out


__all__ = [
    "ROOT", "KANIDA_DB", "MARKET15_DB", "RAW_ARCHIVE_DIR", "ARTIFACTS",
    "REPORT_MD", "REPORT_JSON", "SOURCE_QUALITY_SCREEN",
    "BASE_TIMEFRAME", "DAILY_TIMEFRAME",
    "INVISIBLE_TO_ACCOUNT", "KNOWN_SUSPENSIONS",
    "read_only_conn", "Symbol", "universe", "symbol_lookup",
    "flagged_symbols", "flagged_detail",
    "MissingDependency", "StoreAdapter",
    "resolve_provider", "resolve_store", "resolve_validate", "dependencies_ready",
]
