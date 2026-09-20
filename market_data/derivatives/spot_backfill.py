"""Fill in the **spot** a closed session's 15-minute readings were never given.

Why this exists
---------------
On Friday 18 Sep 2026 the F&O capture died at 11:30 when a session restarted.
The readings from 11:45 to 15:45 were rebuilt afterwards from Kite's 15-minute
candles, and that rebuild ran with an expired Kite token — so the spot leg came
back empty for every one of them.  Measured on the store: 216 underlying rows at
each of the sixteen afternoon readings, **zero** with a spot, against 216 of 216
at every reading up to 11:30.

Two different things were lost in that outage and only one of them is lost for
good:

* ``average_price`` — the exchange's own traded average.  A candle does not
  carry one, and it exists nowhere but in a live quote, so it **cannot** be
  recovered after the fact.  Nothing here invents one.
* ``spot`` — the underlying's price.  That one we already hold: ``db/market15.db``
  is our own 15-minute equity store, and it has Friday's bars for 210 of the 216
  F&O underlyings.  This module puts them where they belong.

The rules this module is built to
---------------------------------
**Same reading, never a neighbour.**  The spot written against the 11:45 reading
is the underlying's own bar that CLOSES at 11:45 (``bar_end = '… 11:45:00'``).
If that bar is not in the store, nothing is written — no carry-forward, no
interpolation, no nearest-bar.  A gap stays a gap.

**The provenance is recorded, not implied.**  Everything written here is stamped
``spot_source = 'market15.candles_15m'``: our own equity store's bar close, after
the fact, not the F&O vendor's quote at that instant.  A reader can tell the two
apart because the column says which.  The same run also stamps the session's
genuinely live readings ``kite.quote`` — and only where the store PROVES it, ie.
where every snapshot row at that mark carries ``source='kite.quote'``.  A mark
that mixes sources is left unstamped rather than guessed at.

**CAS is not a failure.**  F&O underlyings run a closing auction, so our equity
store's last continuous bar for them starts at 15:00 and closes at 15:15.  The
15:30 and 15:45 readings therefore get no stock spot, and that is correct.  The
count of readings left empty for this reason is reported, not hidden.

**The six indices need a live token.**  NIFTY, NIFTYFPI, BANKNIFTY, FINNIFTY,
MIDCPNIFTY and NIFTYNXT50 are index levels; ``db/market15.db`` holds equities and
has no bars for them.  They are reported as unresolved and left alone.

``db/market15.db`` is opened **read-only** here, exactly as everywhere else in
this package.  Only ``db/derivatives.db`` is written.

Running it::

    py -m market_data.derivatives.spot_backfill --session 2026-09-18 --dry-run
    py -m market_data.derivatives.spot_backfill --session 2026-09-18 --recompute

and, once a Kite token is alive again, the six indices in the same run:

    py -m market_data.derivatives.spot_backfill --session 2026-09-18 --from-vendor --recompute-session
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_data.derivatives import config  # noqa: E402
from market_data.derivatives import metrics as M  # noqa: E402
from market_data.derivatives.store import apply_migrations  # noqa: E402

LOG = logging.getLogger("market_data.derivatives.spot_backfill")

#: Our own 15-minute equity store.  READ-ONLY from this package, always.
MARKET15_PATH = ROOT / "db" / "market15.db"

#: Why a reading was left without a spot.  Each one is a different fact and they
#: are never collapsed into one number: "the exchange was in its closing auction"
#: and "we hold no bars for this instrument at all" are not the same gap.
REASON_NO_BAR = "no_bar_at_this_reading"
REASON_NO_SYMBOL = "no_equity_symbol_known"
REASON_NOT_IN_STORE = "instrument_not_in_market15"
REASONS = (REASON_NO_BAR, REASON_NO_SYMBOL, REASON_NOT_IN_STORE)
REASON_TEXT = {
    REASON_NO_BAR: ("db/market15.db holds bars for this instrument on this session but none that closes "
                    "at this reading. Nothing was written: the spot at a reading is that reading's own "
                    "bar close or nothing at all. For an F&O underlying at 15:30 and 15:45 this is the "
                    "closing auction, which is a correct absence and not a gap in capture."),
    REASON_NO_SYMBOL: ("no equity symbol is recorded for this underlying anywhere in the session, so there "
                       "is nothing to look up."),
    REASON_NOT_IN_STORE: ("db/market15.db holds no bars at all for this instrument on this session. The six "
                          "index underlyings are index levels rather than equities and are always here; "
                          "they need a live Kite token."),
}


def open_market15(path: str | Path = MARKET15_PATH) -> sqlite3.Connection:
    """``db/market15.db``, read-only and refusing to be anything else."""
    p = Path(path)
    if not p.exists():
        raise LookupError(f"{p} does not exist")
    conn = sqlite3.connect(f"file:{p.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0)
    conn.execute("PRAGMA query_only=ON")
    conn.row_factory = sqlite3.Row
    return conn


def _day_bounds(session: date) -> tuple[str, str]:
    return session.isoformat() + " 00:00:00", (session + timedelta(days=1)).isoformat() + " 00:00:00"


def spot_symbols(conn: sqlite3.Connection, session: date) -> dict[str, str]:
    """``underlying -> NSE equity symbol``, taken from the session's own rows.

    The live capture writes ``spot_symbol`` beside every spot it takes, so the
    readings that DID capture already hold the mapping this session used — no
    vendor call, and no guess that an underlying's symbol equals its own name
    (it does not for the six indices: NIFTY's is "NIFTY 50").
    """
    out: dict[str, str] = {}
    for r in conn.execute(
        "SELECT underlying, spot_symbol FROM underlying_snapshots"
        " WHERE substr(captured_at,1,10)=? AND spot_symbol IS NOT NULL AND spot_symbol<>''",
        (session.isoformat(),),
    ):
        out.setdefault(r["underlying"], r["spot_symbol"])
    return out


def bars_at_marks(m15: sqlite3.Connection, session: date, symbols: Iterable[str]
                  ) -> dict[tuple[str, str], float]:
    """``(symbol, mark) -> close`` for one session, keyed by the bar's CLOSE time.

    ``bar_end`` is the mark: the reading at 11:45 is the bar that ran 11:30-11:45.
    Only complete, unflagged bars qualify — a partial bar has no close to speak
    of, and a flagged one is a bar the equity store itself does not vouch for.
    """
    wanted = sorted({s for s in symbols if s})
    if not wanted:
        return {}
    lo, hi = _day_bounds(session)
    out: dict[tuple[str, str], float] = {}
    best: dict[tuple[str, str], int] = {}
    chunk = 500
    for i in range(0, len(wanted), chunk):
        part = wanted[i:i + chunk]
        sql = ("SELECT symbol, bar_end, close, revision FROM candles_15m"
               " WHERE bar_start>=? AND bar_start<? AND candle_complete=1"
               " AND (quality_flags IS NULL OR quality_flags='')"
               f" AND symbol IN ({','.join('?' * len(part))})")
        for r in m15.execute(sql, [lo, hi, *part]):
            if r["close"] is None or not r["bar_end"]:
                continue
            key = (r["symbol"], str(r["bar_end"]))
            rev = int(r["revision"] or 0)
            # A revisioned store can in principle hold more than one revision of
            # the same bar.  The newest one is the one it stands behind.
            if key in best and best[key] >= rev:
                continue
            best[key] = rev
            out[key] = float(r["close"])
    return out


def symbols_present(m15: sqlite3.Connection, session: date, symbols: Iterable[str]) -> set[str]:
    """Which of these symbols the equity store holds ANY bar for on this session."""
    wanted = sorted({s for s in symbols if s})
    if not wanted:
        return set()
    lo, hi = _day_bounds(session)
    found: set[str] = set()
    chunk = 500
    for i in range(0, len(wanted), chunk):
        part = wanted[i:i + chunk]
        sql = ("SELECT DISTINCT symbol FROM candles_15m WHERE bar_start>=? AND bar_start<?"
               f" AND symbol IN ({','.join('?' * len(part))})")
        found |= {r[0] for r in m15.execute(sql, [lo, hi, *part])}
    return found


def missing_spot_rows(conn: sqlite3.Connection, session: date,
                      underlyings: Sequence[str] | None = None) -> list[tuple[str, str]]:
    """``(underlying, captured_at)`` for every reading of the session with no spot."""
    sql = ("SELECT underlying, captured_at FROM underlying_snapshots"
           " WHERE substr(captured_at,1,10)=? AND spot IS NULL")
    args: list[Any] = [session.isoformat()]
    if underlyings:
        sql += f" AND underlying IN ({','.join('?' * len(underlyings))})"
        args += [u.upper() for u in underlyings]
    sql += " ORDER BY captured_at, underlying"
    return [(r[0], r[1]) for r in conn.execute(sql, args)]


def stamp_captured_sources(conn: sqlite3.Connection, session: date, *, write: bool = True) -> dict:
    """Record ``kite.quote`` on the session's spots that the store PROVES were live.

    A spot with no recorded source is ambiguous, and after this module runs the
    ambiguity matters: an unstamped row sitting beside a ``market15.candles_15m``
    row would read as "the other kind" without saying so.

    The evidence is in the store and is not inferred from the clock: the
    ``underlying_snapshots`` row for a mark is written in the same pass as that
    mark's ``snapshots`` rows, and ``snapshots.source`` says how each of those was
    obtained.  A mark whose snapshot rows are ALL ``kite.quote`` was a live
    capture.  A mark that mixes sources, or has none, is left unstamped —
    "not recorded" is an honest answer and a wrong one is not.
    """
    day = session.isoformat()
    live, mixed = [], []
    for r in conn.execute(
        "SELECT captured_at, COUNT(*) AS n, SUM(source=?) AS live FROM snapshots"
        " WHERE substr(captured_at,1,10)=? GROUP BY captured_at ORDER BY captured_at",
        (config.SPOT_SOURCE_QUOTE, day),
    ):
        if r["n"] and r["n"] == (r["live"] or 0):
            live.append(r["captured_at"])
        else:
            mixed.append(r["captured_at"])
    stamped = 0
    if live:
        sql = ("SELECT COUNT(*) FROM underlying_snapshots WHERE spot IS NOT NULL"
               " AND spot_source IS NULL AND captured_at IN"
               f" ({','.join('?' * len(live))})")
        stamped = int(conn.execute(sql, live).fetchone()[0] or 0)
        if write and stamped:
            conn.execute(
                "UPDATE underlying_snapshots SET spot_source=? WHERE spot IS NOT NULL"
                " AND spot_source IS NULL AND captured_at IN"
                f" ({','.join('?' * len(live))})",
                [config.SPOT_SOURCE_QUOTE, *live],
            )
    return {"live_marks": live, "unstamped_marks": mixed, "rows_stamped": stamped}


def propagate_spot_source(conn: sqlite3.Connection, marks: Sequence[str], *,
                          write: bool = True) -> int:
    """Copy ``underlying_snapshots.spot_source`` onto the ``metrics`` rows of these marks.

    The Derivative tab reads ``metrics``.  A mark whose metric rows were computed
    before this column existed carries the spot but not where it came from, and
    an unlabelled spot beside a labelled one reads as "the other kind" without
    saying so.

    This is a PROVENANCE-ONLY write and deliberately not a recompute: it changes
    no number, it is guarded by ``metrics.spot = underlying_snapshots.spot`` so it
    can only stamp a row whose spot is the very one being described, and it never
    touches a row that already carries a source.
    """
    if not marks:
        return 0
    slots = ",".join("?" * len(marks))
    match = ("SELECT u.spot_source FROM underlying_snapshots u"
             " WHERE u.underlying=metrics.underlying AND u.captured_at=metrics.captured_at"
             " AND u.spot=metrics.spot AND u.spot_source IS NOT NULL")
    where = (f" WHERE captured_at IN ({slots}) AND spot IS NOT NULL AND spot_source IS NULL"
             f" AND EXISTS ({match})")
    n = int(conn.execute("SELECT COUNT(*) FROM metrics" + where, list(marks)).fetchone()[0] or 0)
    if write and n:
        conn.rollback()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(f"UPDATE metrics SET spot_source=({match})" + where, list(marks))
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
    return n


def backfill_session(
    conn: sqlite3.Connection,
    session: date,
    *,
    market15_path: str | Path = MARKET15_PATH,
    underlyings: Sequence[str] | None = None,
    write: bool = True,
) -> dict:
    """Fill the session's missing spots from our own 15-minute equity bars.

    Returns a summary that names every reading it could not fill and why.  The
    write is one UPDATE per filled row inside a single transaction, and it is
    guarded by ``spot IS NULL`` so it can never overwrite a captured spot — re-
    running it is a no-op once the gap is closed.
    """
    pending = missing_spot_rows(conn, session, underlyings)
    mapping = spot_symbols(conn, session)
    summary: dict[str, Any] = {
        "session": session.isoformat(),
        "source": config.SPOT_SOURCE_MARKET15,
        "market15": str(Path(market15_path)),
        "rows_missing_spot": len(pending),
        "rows_filled": 0,
        "written": bool(write),
        "per_reading": {},
        "unresolved": {r: [] for r in REASONS},
        "reason_text": dict(REASON_TEXT),
    }
    if not pending:
        return summary

    names = sorted({u for u, _ in pending})
    symbols = {u: mapping.get(u) for u in names}
    m15 = open_market15(market15_path)
    try:
        present = symbols_present(m15, session, [s for s in symbols.values() if s])
        bars = bars_at_marks(m15, session, [s for s in symbols.values() if s and s in present])
    finally:
        m15.close()

    updates: list[tuple[float, str, str, str, str]] = []
    per_reading: dict[str, dict[str, int]] = {}
    unresolved: dict[str, set[str]] = {r: set() for r in REASONS}
    for underlying, at in pending:
        slot = per_reading.setdefault(at, {"missing": 0, "filled": 0, REASON_NO_BAR: 0,
                                           REASON_NO_SYMBOL: 0, REASON_NOT_IN_STORE: 0})
        slot["missing"] += 1
        symbol = symbols.get(underlying)
        if not symbol:
            slot[REASON_NO_SYMBOL] += 1
            unresolved[REASON_NO_SYMBOL].add(underlying)
            continue
        if symbol not in present:
            slot[REASON_NOT_IN_STORE] += 1
            unresolved[REASON_NOT_IN_STORE].add(underlying)
            continue
        close = bars.get((symbol, at))
        if close is None:
            # SAME READING, NEVER A NEIGHBOUR.  There is a bar half an hour
            # either side of this one and it is not this reading's bar.
            slot[REASON_NO_BAR] += 1
            unresolved[REASON_NO_BAR].add(underlying)
            continue
        updates.append((close, symbol, config.SPOT_SOURCE_MARKET15, underlying, at))
        slot["filled"] += 1

    if write and updates:
        # The reads above may have left a transaction open; a BEGIN inside one
        # is an error, and in WAL a read snapshot cannot be upgraded to a write.
        conn.rollback()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executemany(
                "UPDATE underlying_snapshots SET spot=?, spot_symbol=?, spot_source=?"
                " WHERE underlying=? AND captured_at=? AND spot IS NULL",
                updates,
            )
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")

    summary["rows_filled"] = len(updates)
    summary["per_reading"] = {k: per_reading[k] for k in sorted(per_reading)}
    summary["unresolved"] = {k: sorted(v) for k, v in unresolved.items()}
    return summary


def backfill_from_vendor(
    conn: sqlite3.Connection,
    session: date,
    underlyings: Sequence[str],
    *,
    provider=None,
    write: bool = True,
) -> dict:
    """Fill the spots ``db/market15.db`` cannot hold, from the vendor's own bars.

    This is the six INDEX underlyings.  ``db/market15.db`` is an equity store and
    has no NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, NIFTYNXT50 or NIFTYFPI in it,
    so an index level at a past reading can only come from the vendor — and that
    needs a live Kite token, which is why this is a separate, explicit pass and
    not something the ordinary run attempts and fails at.

    It reuses ``seed_from_candles.fetch_spot_series``: the same request, the same
    mark convention (a bar belongs to the reading it CLOSES at), the same source
    label.  Nothing but ``spot``/``spot_symbol``/``spot_source`` is written — the
    snapshot rows this session already holds are left exactly as they are.

    Same reading, never a neighbour: a mark the vendor returns no bar for gets
    nothing, exactly as the equity pass does.
    """
    from .seed_from_candles import fetch_spot_series

    names = sorted({u.upper() for u in underlyings})
    if not names:
        return {"underlyings": [], "rows_filled": 0, "written": bool(write), "per_underlying": {}}
    if provider is None:
        from market_data import get_provider
        provider = get_provider(config.VENDOR_ID)
    series = fetch_spot_series(provider, names, session)
    pending = missing_spot_rows(conn, session, names)
    updates, per_name = [], {name: {"missing": 0, "filled": 0} for name in names}
    for underlying, at in pending:
        slot = per_name.setdefault(underlying, {"missing": 0, "filled": 0})
        slot["missing"] += 1
        symbol, marks = series.get(underlying, (None, {}))
        close = marks.get(at) if marks else None
        if symbol is None or close is None:
            continue
        updates.append((float(close), symbol, config.SPOT_SOURCE_CANDLES, underlying, at))
        slot["filled"] += 1
    if write and updates:
        conn.rollback()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executemany(
                "UPDATE underlying_snapshots SET spot=?, spot_symbol=?, spot_source=?"
                " WHERE underlying=? AND captured_at=? AND spot IS NULL",
                updates,
            )
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
    return {"underlyings": names, "source": config.SPOT_SOURCE_CANDLES,
            "rows_filled": len(updates), "written": bool(write),
            "per_underlying": per_name,
            "marks_filled": sorted({u[4] for u in updates})}


def rebuilt_marks(conn: sqlite3.Connection, session: date) -> list[str]:
    """Every reading of the session that carries at least one RECONSTRUCTED spot.

    The set worth recomputing after any backfill pass: these are the readings
    whose ``metrics.spot`` is behind what ``underlying_snapshots`` now holds.
    """
    return [r[0] for r in conn.execute(
        "SELECT DISTINCT captured_at FROM underlying_snapshots"
        " WHERE substr(captured_at,1,10)=? AND spot_source IN (?, ?) ORDER BY captured_at",
        (session.isoformat(), *config.SPOT_SOURCES_REBUILT))]


def affected_marks(summary: Mapping[str, Any]) -> list[str]:
    """The readings a backfill actually changed — the ones worth recomputing."""
    return [at for at, slot in (summary.get("per_reading") or {}).items() if slot.get("filled")]


def recompute_marks(conn: sqlite3.Connection, marks: Sequence[str]) -> list[dict]:
    """Re-run the metrics worker over these readings so ``metrics.spot`` follows.

    The Derivative tab reads ``metrics``, not ``underlying_snapshots``: a spot on
    the roll-up row that never reaches the metric row is a spot no reader sees.
    Recomputing is how it gets there, and it also refreshes every figure that
    rests on a spot (the futures basis, the distance from max pain) instead of
    leaving them stale beside a spot they were not computed against.
    """
    out = []
    for at in marks:
        res = M.compute_for_mark(conn, datetime.strptime(at, "%Y-%m-%d %H:%M:%S"), write=True)
        out.append({"captured_at": res.captured_at.isoformat(sep=" ", timespec="seconds"),
                    "contract_rows": len(res.contract_rows),
                    "underlying_rows": len(res.underlying_rows),
                    "written": res.written,
                    "skipped_no_snapshot": res.skipped_no_snapshot})
        LOG.info("recomputed %s: %d rows", at, res.written)
    return out


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="spot_backfill", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", default=str(config.DEFAULT_DB_PATH))
    p.add_argument("--market15", default=str(MARKET15_PATH))
    p.add_argument("--session", required=True, help="YYYY-MM-DD, a session that has closed")
    p.add_argument("--underlying", action="append", dest="underlyings")
    p.add_argument("--dry-run", action="store_true", help="measure and report; write nothing")
    p.add_argument("--recompute", action="store_true",
                   help="re-run the metrics worker over the readings this run filled")
    p.add_argument("--recompute-session", action="store_true",
                   help="re-run it over EVERY reading of the session that carries a rebuilt spot")
    p.add_argument("--from-vendor", action="store_true",
                   help="also fill the underlyings db/market15.db cannot hold (the indices) from the "
                        "vendor's own 15-minute bars. NEEDS A LIVE KITE TOKEN.")
    p.add_argument("--log-level", default="INFO")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")
    session = date.fromisoformat(args.session)
    write = not args.dry_run
    conn = M.connect(args.db, readonly=not write)
    try:
        # `spot_source` is newer than this database. Adding it is idempotent and
        # touches no row; without it there is nowhere to record provenance, and
        # writing a rebuilt spot with nothing saying so is the one thing this
        # module must never do.
        if write:
            added = apply_migrations(conn)
            if added:
                LOG.info("schema: added %s", ", ".join(added))
        summary = backfill_session(conn, session, market15_path=args.market15,
                                   underlyings=args.underlyings, write=write)
        stamped = stamp_captured_sources(conn, session, write=write)
        if write:
            conn.commit()
        # The live marks' metric rows were computed before this column existed.
        # Stamping them is a provenance write, not a recompute: no figure moves.
        stamped["metric_rows_stamped"] = propagate_spot_source(
            conn, stamped["live_marks"], write=write)
        summary["stamped_captured"] = stamped
        if args.from_vendor:
            # The indices, which an equity store can never hold. Explicit, because it is the one part of this
            # that reaches a vendor at all, and it fails loudly rather than quietly when the token is dead.
            summary["from_vendor"] = backfill_from_vendor(
                conn, session, summary["unresolved"][REASON_NOT_IN_STORE], write=write)
        if write and (args.recompute or args.recompute_session):
            marks = (rebuilt_marks(conn, session) if args.recompute_session
                     else sorted(set(affected_marks(summary))
                                 | set((summary.get("from_vendor") or {}).get("marks_filled") or [])))
            summary["recomputed"] = recompute_marks(conn, marks)
        print(json.dumps(summary, indent=2, default=str))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["backfill_session", "backfill_from_vendor", "rebuilt_marks",
           "stamp_captured_sources", "propagate_spot_source",
           "recompute_marks",
           "affected_marks", "spot_symbols", "bars_at_marks", "open_market15",
           "MARKET15_PATH", "REASONS", "REASON_TEXT"]
