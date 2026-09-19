"""Freeze the snapshot the research rerun reads (contract section 3.6).

What makes this snapshot different from the one the repair pass froze:

* the **universe** is NIFTY 500 minus the symbols the provider cannot serve at
  all (``market_data.quarantine``), instead of every symbol the store happens
  to hold;
* every row the repair labelled unusable is **excluded by label**
  (``vendor_zero_print``, ``vendor_bad_print``, ``wrong_instrument``,
  ``unresolved``) rather than left in for the rerun to trip over.  The rows
  stay in ``candles_15m`` -- deleting them would destroy the evidence for the
  label -- so the exclusion is recorded in ``snapshot_exclusions``, which is
  what stops a deliberate exclusion from looking like a coverage hole.

The labels are read from the reconcile step's own artifacts, not re-derived, so
this cannot drift from the report that justified them.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, Sequence

from market_data.repair import ARTIFACTS, MARKET15_DB

log = logging.getLogger("market_data.repair.freeze_rerun")

RECONCILE_DIR = ARTIFACTS / "reconcile"
VERDICTS = RECONCILE_DIR / "verdicts.jsonl"
SUMMARY = RECONCILE_DIR / "summary.json"
OUT_DIR = ARTIFACTS / "freeze"

#: Per-bar labels, read from `verdicts.jsonl`.
BAR_LABELS = ("vendor_zero_print", "vendor_bad_print", "unresolved")
#: A span label: the reused-token window, read from `summary.json`.
SPAN_LABEL = "wrong_instrument"


def load_bar_labels(path: Path = VERDICTS,
                    labels: Sequence[str] = BAR_LABELS) -> dict:
    """``{label: [(symbol, bar_start), ...]}`` from the reconcile verdicts."""
    wanted = set(labels)
    out: dict[str, list] = {k: [] for k in labels}
    if not Path(path).exists():
        return out
    with Path(path).open(encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("klass") in wanted:
                out[row["klass"]].append((row["symbol"], row["bar_start"]))
    return out


def load_wrong_instrument_spans(path: Path = SUMMARY) -> list:
    """``[(symbol, first_bar, vendor_first_daily)]`` -- the reused-token window.

    The *inferred* span is used, not just the sessions the repair happened to
    probe: the rows between the last probed session and the vendor's own first
    daily bar belong to the same earlier life of the token.  Excluding the
    narrower window would leave most of the bad history in.
    """
    if not Path(path).exists():
        return []
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    spans = []
    for d in (data.get("wrong_instrument") or {}).get("detail", []):
        first, last = d.get("inferred_quarantine_span") or [
            d["held_first_session"], d["vendor_first_daily_session"]]
        spans.append((d["symbol"], f"{first} 00:00:00", f"{last} 00:00:00"))
    return spans


def universe_symbols(store, *, index_column: str = "in_nifty500",
                     kanida_db=None) -> tuple[list, list]:
    """``(symbols_in, quarantined_out)`` -- the rerun's universe."""
    from market_data.live.calendar_ext import DEFAULT_KANIDA_DB, nifty500

    members = [i.symbol for i in nifty500(kanida_db or DEFAULT_KANIDA_DB,
                                          index_column=index_column)]
    blocked = set(store.quarantined())
    held = set(store.symbols())
    keep = sorted(s for s in members if s not in blocked and s in held)
    return keep, sorted(blocked & set(members))


def _reusable(store, snapshot_id: Optional[str]) -> int:
    """Members already pinned into `snapshot_id`, if it is open and non-empty."""
    if not snapshot_id:
        return 0
    row = store.con.execute("SELECT status FROM snapshots WHERE snapshot_id=?",
                            (snapshot_id,)).fetchone()
    if row is None or row["status"] != "open":
        return 0
    return int(store.con.execute(
        "SELECT COUNT(*) FROM snapshot_members WHERE snapshot_id=?",
        (snapshot_id,)).fetchone()[0])


def run(*, db: Path = MARKET15_DB, snapshot_id: Optional[str] = None,
        universe: str = "NIFTY500", provider: str = "kite",
        adjustment_basis_id: str = "kite-eod-adjusted",
        start=None, end=None, notes: str = "",
        allow_concurrent: bool = False, verify: bool = True,
        out_dir: Path = OUT_DIR) -> dict:
    from market_data.store import MarketStore, WriterLock

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    store = MarketStore(db)
    lock = None
    if not allow_concurrent:
        lock = WriterLock(db).acquire()
    try:
        symbols, blocked = universe_symbols(store)
        log.info("universe: %d symbols (%d quarantined and excluded: %s)",
                 len(symbols), len(blocked), ", ".join(blocked) or "none")
        reused = _reusable(store, snapshot_id)
        if reused:
            # Pinning 28 M members takes minutes and holds the write lock the
            # whole time; a resumed run must not pay it twice.
            log.info("reusing the open snapshot %s (%d members already pinned)",
                     snapshot_id, reused)
        else:
            snapshot_id = store.create_snapshot(
                snapshot_id, universe=universe, provider=provider,
                adjustment_basis_id=adjustment_basis_id, symbols=symbols,
                start=start, end=end,
                notes=notes or ("NIFTY 500 minus quarantined symbols; rows labelled "
                                "vendor_zero_print / vendor_bad_print / "
                                "wrong_instrument / unresolved excluded by label"))
        pinned = store._snapshot_counts(snapshot_id)
        log.info("snapshot %s pinned %d rows / %d symbols before exclusions",
                 snapshot_id, pinned["rows"], pinned["symbols"])

        exclusions = []
        for label, bars in load_bar_labels().items():
            removed = store.exclude_snapshot_rows(
                snapshot_id, label=label, bars=bars,
                source=str(VERDICTS),
                note=f"{len(bars)} rows carry this label in the reconcile verdicts")
            removed["labelled_rows"] = len(bars)
            exclusions.append(removed)
            log.info("excluded %s: %d rows", label, removed["rows"])
        spans = load_wrong_instrument_spans()
        removed = store.exclude_snapshot_rows(
            snapshot_id, label=SPAN_LABEL, spans=spans, source=str(SUMMARY),
            note=(f"{len(spans)} reused-token windows, each from the symbol's first "
                  f"held bar to the vendor's own first daily bar"))
        removed["labelled_spans"] = len(spans)
        exclusions.append(removed)
        log.info("excluded %s: %d rows", SPAN_LABEL, removed["rows"])

        frozen = store.freeze_snapshot(snapshot_id)
        # A second full pass over the members. It proves the checksum
        # reproduces, which is the whole claim the snapshot makes -- and on the
        # real store it costs as much again as computing it, so it is skippable
        # for a throwaway run rather than silently cheap.
        verified = store.verify_snapshot(snapshot_id) if verify else None
        if verify and not verified:
            raise RuntimeError(f"snapshot {snapshot_id}: checksum did not reproduce")
        summary = {
            "snapshot_id": snapshot_id,
            "db": str(db),
            "universe": universe,
            "reused_open_snapshot": bool(reused),
            "symbols_requested": len(symbols),
            "quarantined_excluded": blocked,
            "pinned_before_exclusions": pinned,
            "exclusions": exclusions,
            "rows_excluded_total": sum(e["rows"] for e in exclusions),
            "snapshot": {k: frozen[k] for k in (
                "snapshot_id", "created_at", "universe", "adjustment_basis_id",
                "provider", "first_bar", "last_bar", "symbol_count", "row_count",
                "checksum", "status", "frozen_at", "notes")},
            "verified": verified,
            "generated_at": datetime.now().astimezone().isoformat(),
        }
        path = out_dir / f"freeze_{snapshot_id}.json"
        path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        print(f"[freeze_rerun] wrote {path}", flush=True)
        return summary
    finally:
        store.close()
        if lock is not None:
            lock.release()


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", default=str(MARKET15_DB))
    ap.add_argument("--snapshot-id")
    ap.add_argument("--universe", default="NIFTY500")
    ap.add_argument("--start", default=None, help="first bar_start (inclusive)")
    ap.add_argument("--end", default=None,
                    help="first bar_start to EXCLUDE -- set it to the start of "
                         "today's session so a half-finished session never lands "
                         "in a frozen research input")
    ap.add_argument("--allow-concurrent", action="store_true")
    ap.add_argument("--no-verify", action="store_true",
                    help="skip the second checksum pass (it costs as much as "
                         "the first over ~28 M rows)")
    a = ap.parse_args(argv)
    summary = run(db=Path(a.db), snapshot_id=a.snapshot_id, universe=a.universe,
                  start=a.start, end=a.end, verify=not a.no_verify,
                  allow_concurrent=a.allow_concurrent)
    print(json.dumps({k: v for k, v in summary.items()
                      if k != "quarantined_excluded"} |
                     {"quarantined_excluded": summary["quarantined_excluded"]},
                     indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
