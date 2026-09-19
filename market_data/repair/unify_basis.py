"""Put every symbol's 15-minute history on ONE adjustment basis.

Why a second pass was needed
----------------------------
The repair (``market_data/repair/``) re-fetched only the windows it disputed.
Every re-fetched row carries the vendor's current basis; every untouched row
keeps the legacy seed's ``legacy_unknown``.  For most symbols that is only
bookkeeping -- the values agree bar for bar.  For a handful the two labels are
genuinely different corporate-action bases, and a return computed across the
boundary picks up the ratio between them as a move that never happened.

This module:

1. **derives** the affected symbols from the store (``market_data.basis``),
   never from the ten names the repair report happened to print;
2. **re-fetches the full 15-minute history** of each one through the provider,
   archiving every raw payload to ``raw_archive`` before any candle is derived
   from it, resumable, and inside the one global rate limiter;
3. writes the result as **new revisions** -- the legacy rows stay where they
   are, they simply stop being the latest revision;
4. records one ``corrections`` row per changed **segment** (reason
   ``basis_unification``) carrying the ``raw_archive`` request id that
   justifies it -- a row per bar would bury the handful of real price
   corrections the earlier pass wrote;
5. **verifies**: zero bars left on the old basis, and the price discontinuity
   that used to sit at each old label boundary measured before and after.

Nothing is deleted and nothing is re-scaled by arithmetic: the unified series
is what the vendor actually serves today, not the legacy series multiplied by a
ratio we inferred.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Sequence

from market_data import basis as basis_mod
from market_data.repair import (
    ARTIFACTS,
    BASE_TIMEFRAME,
    KANIDA_DB,
    MARKET15_DB,
    read_only_conn,
    resolve_provider,
    resolve_store,
)
from market_data.repair.plan import FetchPlan, chunk
from market_data.repair.refresh import request_id_for
from market_data.repair.refresh import run as run_refresh

log = logging.getLogger("market_data.repair.unify_basis")

PURPOSE = "basis_unification"
REASON = "basis_unification"
HISTORY_START = date(2015, 1, 1)
OUT_DIR = ARTIFACTS / "unify_basis"


# ---------------------------------------------------------------------------
# before-state (measured, not assumed)
# ---------------------------------------------------------------------------
@dataclass
class SymbolBefore:
    """Exactly which bars were on the legacy basis, and by how much they differ."""

    symbol: str
    instrument_id: int
    evidence: dict
    legacy_bars: list = field(default_factory=list)   # bar_start strings, ordered
    segments: list = field(default_factory=list)      # (first, last, bars)
    boundaries: list = field(default_factory=list)    # dicts, see _boundaries()
    first_bar: str | None = None
    last_bar: str | None = None


def _latest_rows(store, symbol: str) -> list[tuple]:
    """``(bar_start, adjustment_basis_id, close, revision, instrument_id)`` at
    latest-wins, in bar order."""
    return [tuple(r) for r in store.con.execute(
        "SELECT bar_start, adjustment_basis_id, close, revision, instrument_id FROM ("
        "  SELECT bar_start, adjustment_basis_id, close, revision, instrument_id,"
        "         ROW_NUMBER() OVER (PARTITION BY instrument_id, bar_start"
        "                            ORDER BY revision DESC) AS rn"
        "  FROM candles_15m WHERE symbol=?"
        ") WHERE rn=1 ORDER BY bar_start", (symbol,))]


def _segments(bars: Sequence[str], all_bars: Sequence[str]) -> list[tuple]:
    """Contiguous runs of `bars` inside the symbol's own bar sequence."""
    index = {b: i for i, b in enumerate(all_bars)}
    runs: list[list] = []
    for b in bars:
        i = index[b]
        if runs and i == runs[-1][2] + 1:
            runs[-1][1] = b
            runs[-1][2] = i
            runs[-1][3] += 1
        else:
            runs.append([b, b, i, 1])
    return [(r[0], r[1], r[3]) for r in runs]


def _boundaries(rows: Sequence[tuple], legacy_basis: str, limit: int = 3) -> list[dict]:
    """Adjacent bars that sat on different bases -- where the fake move lived.

    The 15-minute return across such a pair is the real move multiplied by the
    ratio between the two bases.  Recomputing it after unification is the
    cleanest proof the discontinuity is gone.
    """
    out: list[dict] = []
    for (b0, basis0, close0, *_), (b1, basis1, close1, *_) in zip(rows, rows[1:]):
        if basis0 == basis1 or close0 <= 0 or close1 <= 0:
            continue
        out.append({
            "from_bar": b0, "to_bar": b1,
            "from_basis": basis0, "to_basis": basis1,
            "from_close_before": close0, "to_close_before": close1,
            "return_before": close1 / close0 - 1.0,
            "crosses_into_legacy": basis1 == legacy_basis,
        })
    if len(out) <= limit:
        return out
    step = max(1, len(out) // limit)
    return out[::step][:limit]


def measure_before(store, evidence: basis_mod.BasisEvidence) -> SymbolBefore:
    rows = _latest_rows(store, evidence.symbol)
    all_bars = [r[0] for r in rows]
    legacy = [r[0] for r in rows if r[1] == basis_mod.LEGACY_BASIS]
    return SymbolBefore(
        symbol=evidence.symbol,
        instrument_id=int(rows[0][4]) if rows else 0,
        evidence=evidence.to_dict(),
        legacy_bars=legacy,
        segments=_segments(legacy, all_bars),
        boundaries=_boundaries(rows, basis_mod.LEGACY_BASIS),
        first_bar=all_bars[0] if all_bars else None,
        last_bar=all_bars[-1] if all_bars else None,
    )


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------
def build_plan(befores: Sequence[SymbolBefore], *, provider, tokens: dict,
               through: date) -> FetchPlan:
    caps = dict(getattr(provider, "max_days_per_request", {"15minute": 200}))
    cap15 = int(caps.get(BASE_TIMEFRAME, 200))
    requests = []
    for b in befores:
        first = date.fromisoformat(b.first_bar[:10]) if b.first_bar else HISTORY_START
        start = min(first, HISTORY_START)
        requests += chunk(b.symbol, int(tokens.get(b.symbol) or 0), BASE_TIMEFRAME,
                          start, through, PURPOSE, cap15)
    return FetchPlan(
        created_at=datetime.now().astimezone().isoformat(),
        provider=getattr(provider, "provider_id", "unknown"),
        rate_limit_per_second=float(getattr(provider, "rate_limit_per_second", 3.0)),
        caps=caps, requests=requests,
        notes=[f"full 15-minute history for {len(befores)} symbols measured to hold "
               f"two adjustment bases; one basis afterwards"])


def _request_windows(plan: FetchPlan) -> dict:
    """``symbol -> [(start_date, end_date, request_id)]``, in order."""
    out: dict[str, list] = {}
    for r in plan.requests:
        out.setdefault(r.symbol, []).append(
            (date.fromisoformat(r.start), date.fromisoformat(r.end),
             request_id_for(r)))
    for v in out.values():
        v.sort()
    return out


# ---------------------------------------------------------------------------
# corrections (one row per changed segment, with its evidence)
# ---------------------------------------------------------------------------
def write_corrections(store, before: SymbolBefore, windows: Sequence[tuple],
                      run_id: str) -> int:
    """One ``corrections`` row per (segment x covering request)."""
    ratio = before.evidence.get("median_ratio")
    written = 0
    for seg_first, seg_last, seg_bars in before.segments:
        a, b = date.fromisoformat(seg_first[:10]), date.fromisoformat(seg_last[:10])
        for w_start, w_end, request_id in windows:
            if w_end < a or w_start > b:
                continue
            lo, hi = max(a, w_start), min(b, w_end)
            store.record_correction(
                symbol=before.symbol, timeframe=BASE_TIMEFRAME,
                bar_start=f"{lo.isoformat()} 09:15:00",
                field="adjustment_basis_id",
                old_value=basis_mod.LEGACY_BASIS,
                new_value="kite-eod-adjusted",
                reason=(f"{REASON}: {lo} .. {hi} (part of the {seg_bars}-bar segment "
                        f"{seg_first[:10]} .. {seg_last[:10]}) was held on "
                        f"{basis_mod.LEGACY_BASIS} while other windows of the same "
                        f"symbol were on the vendor's current basis; the measured "
                        f"level ratio between the two was "
                        f"{ratio:.6f} on {before.evidence.get('overlap_bars')} bars "
                        f"held on both. Replaced by a full-history re-fetch as a new "
                        f"revision; the legacy rows are kept."
                        if ratio else f"{REASON}: {lo} .. {hi}"),
                evidence_request_id=request_id, run_id=run_id)
            written += 1
    return written


# ---------------------------------------------------------------------------
# verification
# ---------------------------------------------------------------------------
def verify(store, before: SymbolBefore) -> dict:
    """Zero bars on the old basis, and the old boundary jumps re-measured."""
    after = basis_mod.evidence_for(store, before.symbol)
    rows = {r[0]: r for r in _latest_rows(store, before.symbol)}
    checks = []
    for b in before.boundaries:
        r0, r1 = rows.get(b["from_bar"]), rows.get(b["to_bar"])
        entry = dict(b)
        if r0 and r1 and r0[2] > 0:
            entry.update({"from_basis_after": r0[1], "to_basis_after": r1[1],
                          "from_close_after": r0[2], "to_close_after": r1[2],
                          "return_after": r1[2] / r0[2] - 1.0})
            entry["return_distortion_removed"] = (
                entry["return_before"] - entry["return_after"])
        else:
            entry["note"] = "bar not present at the latest revision after the re-fetch"
        checks.append(entry)
    return {
        "symbol": before.symbol,
        "legacy_bars_before": len(before.legacy_bars),
        "legacy_bars_after": after.legacy_rows,
        "verdict_before": before.evidence.get("verdict"),
        "verdict_after": after.verdict,
        "level_ratio_before": before.evidence.get("median_ratio"),
        "level_ratio_after": after.median_ratio,
        "shifted_share_before": before.evidence.get("shifted_share"),
        "rows_by_basis_after": after.rows_by_basis,
        "unified": after.legacy_rows == 0,
        "boundary_checks": checks,
    }


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------
def run(*, db: Path = MARKET15_DB, kanida_db: Path = KANIDA_DB,
        symbols: Optional[Sequence[str]] = None, provider=None,
        threads: int = 4, run_id: Optional[str] = None, dry_run: bool = False,
        allow_concurrent: bool = False, out_dir: Path = OUT_DIR) -> dict:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    provider = provider or resolve_provider()
    run_id = run_id or f"basis-unify-{datetime.now():%Y%m%d-%H%M%S}"

    store = resolve_store(db)
    lock = None
    if not (allow_concurrent or dry_run):
        from market_data.store import WriterLock

        lock = WriterLock(db).acquire()
    try:
        log.info("scanning %s for mixed adjustment bases", db)
        scanned = basis_mod.scan(store.store, symbols)
        mixed = [e for e in scanned if e.mixed]
        log.info("%d symbols scanned, %d measured mixed", len(scanned), len(mixed))
        befores = [measure_before(store.store, e) for e in mixed]

        con = read_only_conn(kanida_db)
        try:
            tokens = {r["symbol"]: r["kite_token"] for r in con.execute(
                "SELECT symbol, kite_token FROM instrument_labels "
                "WHERE kite_token IS NOT NULL")}
        finally:
            con.close()

        through = _through(provider)
        plan = build_plan(befores, provider=provider, tokens=tokens, through=through)
        summary = {
            "run_id": run_id, "db": str(db), "through": through.isoformat(),
            "symbols_scanned": len(scanned),
            "symbols_mixed": [b.symbol for b in befores],
            "requests_planned": plan.n_requests,
            "legacy_bars_before": {b.symbol: len(b.legacy_bars) for b in befores},
            "by_verdict": _by_verdict(scanned),
            "evidence": [b.evidence for b in befores],
        }
        if dry_run or not befores:
            summary["dry_run"] = True
            _write(out_dir / f"unify_{run_id}.json", summary)
            return summary

        stats = run_refresh(plan, store=store, provider=provider, threads=threads,
                            run_id=run_id, out_dir=out_dir)
        windows = _request_windows(plan)
        corrections = {b.symbol: write_corrections(store.store, b,
                                                   windows.get(b.symbol, []), run_id)
                       for b in befores}
        checks = [verify(store.store, b) for b in befores]
        summary.update({
            "refresh": {k: v for k, v in stats.as_dict().items() if k != "errors"},
            "refresh_errors": dict(list(stats.errors.items())[:20]),
            "corrections_written": corrections,
            "corrections_total": sum(corrections.values()),
            "verification": checks,
            "all_unified": all(c["unified"] for c in checks),
            "elapsed_seconds": round(time.perf_counter() - t0, 1),
        })
        _write(out_dir / f"unify_{run_id}.json", summary)
        return summary
    finally:
        store.close()
        if lock is not None:
            lock.release()


def _through(provider) -> date:
    """Last session this pass re-fetches -- never the session in progress.

    Today's bars belong to the live loop, which writes them at revision 1.  A
    re-fetch mid-session would return the *current, incomplete* bar and store it
    at a higher revision, so the completed bar the live loop writes a few
    minutes later would lose to it.  Stopping at the previous session avoids
    that entirely, and costs nothing: today is already on the vendor's basis
    because the live loop fetched it.
    """
    from market_data.live.ingest import now_ist

    now = now_ist()
    latest = provider.latest_completed_bar(BASE_TIMEFRAME, now)
    through = latest.date()
    if through >= now.date():
        through = now.date() - timedelta(days=1)
    return through


def _by_verdict(scanned: Sequence[basis_mod.BasisEvidence]) -> dict:
    out: dict[str, int] = {}
    for e in scanned:
        out[e.verdict] = out.get(e.verdict, 0) + 1
    return out


def _write(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"[unify_basis] wrote {path}", flush=True)


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--db", default=str(MARKET15_DB))
    ap.add_argument("--symbols", default=None,
                    help="restrict the scan (comma separated); the default scans "
                         "every symbol in the store")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--run-id")
    ap.add_argument("--dry-run", action="store_true",
                    help="measure and plan; fetch nothing")
    ap.add_argument("--allow-concurrent", action="store_true",
                    help="do not take the store's advisory writer lock")
    a = ap.parse_args(argv)
    summary = run(db=Path(a.db),
                  symbols=[s.strip().upper() for s in a.symbols.split(",")]
                  if a.symbols else None,
                  threads=a.threads, run_id=a.run_id, dry_run=a.dry_run,
                  allow_concurrent=a.allow_concurrent)
    print(json.dumps({k: v for k, v in summary.items()
                      if k not in ("evidence", "verification")},
                     indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
