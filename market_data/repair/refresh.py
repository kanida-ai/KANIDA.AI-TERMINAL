"""Step 3 -- execute the fetch plan -- contract section 3.2.

Guarantees:

* **Archive first, write second.**  Every payload is gzipped to disk and
  recorded in ``raw_archive`` *before* a candle derived from it is stored, so
  every row in ``candles_15m`` can be traced back to the bytes it came from.
  This is the provenance the PIIND audit went looking for and could not find.
* **One writer.**  All database writes happen on the calling thread through a
  single ``MarketStore``; fetch threads only produce payloads.
* **The global rate limit is never exceeded.**  ``KiteProvider`` acquires from
  the process-wide token bucket ``ratelimit.get_limiter("kite", rate)`` inside
  every call, so adding fetch threads increases concurrency but not throughput.
  This module deliberately does **not** acquire a second time -- that would
  halve the real rate while looking correct.
* **Resumable.**  A request's id is a deterministic hash of its plan key, so a
  rerun skips whatever ``raw_archive`` already holds.
* **Nothing is invented.**  An empty response is archived as an empty payload
  and recorded as zero rows; it never becomes a forward-filled bar.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional, Sequence

from market_data.repair import (
    ARTIFACTS,
    MARKET15_DB,
    RAW_ARCHIVE_DIR,
    MissingDependency,
    resolve_provider,
    resolve_store,
)
from market_data.repair.plan import FetchPlan, FetchRequest, load as load_plan

log = logging.getLogger("market_data.repair.refresh")


def request_id_for(req: FetchRequest) -> str:
    return hashlib.sha256(req.key.encode("utf-8")).hexdigest()[:32]


def payload_path_for(req: FetchRequest, request_id: str,
                     root: Path = RAW_ARCHIVE_DIR) -> Path:
    return (Path(root) / req.timeframe / req.symbol[:1].upper() / req.symbol
            / f"{req.start}_{req.end}_{request_id}.json.gz")


@dataclass
class FetchResult:
    req: FetchRequest
    request_id: str
    candles: list = field(default_factory=list)
    payload_path: Optional[str] = None
    sha256: Optional[str] = None
    error: Optional[str] = None
    elapsed: float = 0.0
    skipped: bool = False


@dataclass
class RefreshStats:
    run_id: str = ""
    requests_planned: int = 0
    requests_sent: int = 0
    requests_skipped: int = 0
    requests_failed: int = 0
    rows_fetched: int = 0
    rows_written: int = 0
    symbols_written: int = 0
    empty_responses: int = 0
    started_at: str = ""
    finished_at: str = ""
    elapsed_seconds: float = 0.0
    per_request_seconds: float = 0.0
    errors: dict = field(default_factory=dict)
    visibility: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        d = dict(self.__dict__)
        d["errors"] = {k: v for k, v in list(self.errors.items())[:200]}
        return d


# ---------------------------------------------------------------------------
# fetch side (threads)
# ---------------------------------------------------------------------------


def _fetch_one(provider, req: FetchRequest, archive_root: Path) -> FetchResult:
    rid = request_id_for(req)
    t0 = time.perf_counter()
    try:
        start = datetime.fromisoformat(req.start + "T00:00:00")
        end = datetime.fromisoformat(req.end + "T00:00:00")
        candles = provider.candles(req.symbol, req.timeframe, start, end)
    except Exception as exc:
        return FetchResult(req, rid, error=f"{type(exc).__name__}: {exc}",
                           elapsed=time.perf_counter() - t0)

    rows = [c.as_row() if hasattr(c, "as_row") else dict(c) for c in candles]
    payload = json.dumps({
        "request_id": rid,
        "provider": getattr(provider, "provider_id", "unknown"),
        "symbol": req.symbol,
        "timeframe": req.timeframe,
        "start": req.start,
        "end": req.end,
        "purpose": req.purpose,
        "fetched_at": datetime.now().astimezone().isoformat(),
        "row_count": len(rows),
        "content": "provider-normalized candles (market_data.types.RawCandle.as_row)",
        "rows": rows,
    }, sort_keys=True, default=str).encode("utf-8")
    sha = hashlib.sha256(payload).hexdigest()
    path = payload_path_for(req, rid, archive_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with gzip.open(tmp, "wb") as fh:
        fh.write(payload)
    tmp.replace(path)
    return FetchResult(req, rid, candles=candles, payload_path=str(path),
                       sha256=sha, elapsed=time.perf_counter() - t0)


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def already_done(store, request_ids: Iterable[str]) -> set[str]:
    """Which request ids ``raw_archive`` already holds (resume support)."""
    ids = list(request_ids)
    done: set[str] = set()
    con = getattr(store, "con", None) or getattr(getattr(store, "store", None), "con", None)
    if con is None:
        return done
    for i in range(0, len(ids), 500):
        chunk = ids[i:i + 500]
        q = ",".join("?" * len(chunk))
        for (rid,) in con.execute(
                f"SELECT request_id FROM raw_archive WHERE request_id IN ({q})", chunk):
            done.add(rid)
    return done


def run(plan: FetchPlan, *, store=None, provider=None, db: Path = MARKET15_DB,
        archive_root: Path = RAW_ARCHIVE_DIR, threads: int = 6,
        resume: bool = True, limit: Optional[int] = None,
        run_id: Optional[str] = None, progress_every: int = 50,
        out_dir: Path = ARTIFACTS / "refresh") -> RefreshStats:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    provider = provider or resolve_provider()
    owns_store = store is None
    lock = None
    if owns_store:
        # Honour W2's advisory single-writer lock.  If the legacy seed is still
        # running this raises immediately and names the holding PID, instead of
        # quietly becoming a second writer.
        try:
            from market_data.store import WriterLock  # type: ignore

            lock = WriterLock(db).acquire()
        except ImportError:
            lock = None
    store = store or resolve_store(db)

    reqs = list(plan.requests)[:limit] if limit else list(plan.requests)
    stats = RefreshStats(requests_planned=len(reqs),
                         started_at=datetime.now().astimezone().isoformat())

    ids = {r.key: request_id_for(r) for r in reqs}
    done = already_done(store, ids.values()) if resume else set()
    todo = [r for r in reqs if ids[r.key] not in done]
    stats.requests_skipped = len(reqs) - len(todo)

    run_id = run_id or f"repair-{datetime.now():%Y%m%d-%H%M%S}"
    stats.run_id = run_id
    start_run = store.resolved.get("start_run") if hasattr(store, "resolved") else None
    if start_run:
        start_run(run_id, provider=getattr(provider, "provider_id", "unknown"),
                  plan=json.dumps({"total_requests": len(reqs),
                                   "by_purpose": plan.by_purpose()}, default=str))

    log.info("refresh %s: %d planned, %d already archived, %d to fetch",
             run_id, len(reqs), stats.requests_skipped, len(todo))
    print(f"[refresh] run_id={run_id} planned={len(reqs)} "
          f"already_archived={stats.requests_skipped} to_fetch={len(todo)}", flush=True)

    t0 = time.perf_counter()
    written_symbols: set[str] = set()
    basis_ids: set[str] = set()
    # Fetch threads produce; this thread is the ONLY writer.
    with ThreadPoolExecutor(max_workers=max(1, threads)) as pool:
        for n, res in enumerate(pool.map(lambda r: _fetch_one(provider, r, archive_root), todo), 1):
            if res.error:
                stats.requests_failed += 1
                stats.errors[res.req.key] = res.error
                if res.req.purpose == "visibility_probe":
                    stats.visibility[res.req.symbol] = f"error: {res.error}"
            else:
                stats.requests_sent += 1
                stats.rows_fetched += len(res.candles)
                if not res.candles:
                    stats.empty_responses += 1
                if res.req.purpose == "visibility_probe":
                    stats.visibility[res.req.symbol] = (
                        f"{len(res.candles)} bars returned for "
                        f"{res.req.start}..{res.req.end} (request {res.request_id})")
                _write(store, res, run_id, written_symbols, basis_ids, stats)
            if n % progress_every == 0 or n == len(todo):
                rate = n / max(time.perf_counter() - t0, 1e-6)
                print(f"[refresh] {n}/{len(todo)} req  {rate:.2f} req/s  "
                      f"rows={stats.rows_written}  fail={stats.requests_failed}",
                      flush=True)

    stats.elapsed_seconds = round(time.perf_counter() - t0, 1)
    stats.per_request_seconds = round(
        stats.elapsed_seconds / max(stats.requests_sent + stats.requests_failed, 1), 4)
    stats.symbols_written = len(written_symbols)
    stats.finished_at = datetime.now().astimezone().isoformat()

    finish = store.resolved.get("finish_run") if hasattr(store, "resolved") else None
    if finish:
        # A resumed run writes the SAME ingest_runs row again, so the totals
        # have to accumulate across passes -- otherwise the row would claim the
        # whole run did only what the last pass did.
        prior = (0, 0, 0)
        con = getattr(store, "con", None)
        if con is not None:
            row = con.execute("SELECT requests, rows, errors FROM ingest_runs "
                              "WHERE run_id=?", (run_id,)).fetchone()
            if row:
                prior = (row[0] or 0, row[1] or 0, row[2] or 0)
        finish(run_id,
               requests=prior[0] + stats.requests_sent,
               rows=prior[1] + stats.rows_written,
               errors=stats.requests_failed,
               status="ok" if not stats.requests_failed else "partial")

    # One file per pass: a resumed run must not overwrite the pass that did the
    # bulk of the work, or the report would under-count it by thousands of
    # requests.
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    (out_dir / f"refresh_{run_id}__{stamp}.json").write_text(
        json.dumps({**stats.as_dict(), "adjustment_basis_ids": sorted(basis_ids)},
                   indent=2, default=str), encoding="utf-8")
    if owns_store and hasattr(store, "close"):
        store.close()
    if lock is not None:
        lock.release()
    print(json.dumps({k: v for k, v in stats.as_dict().items() if k != "errors"},
                     indent=2, default=str), flush=True)
    return stats


def _write(store, res: FetchResult, run_id: str, written_symbols: set,
           basis_ids: set, stats: RefreshStats) -> None:
    """Archive row first, then the candles as a NEW revision.  Single writer."""
    store.archive_raw(provider=res.candles[0].vendor_id if res.candles else "kite",
                      symbol=res.req.symbol, timeframe=res.req.timeframe,
                      start=res.req.start, end=res.req.end,
                      payload_path=res.payload_path, sha256=res.sha256,
                      request_id=res.request_id, rows=len(res.candles),
                      run_id=run_id)
    if not res.candles:
        return
    if res.req.timeframe != "15minute":
        # Daily bars are an independent cross-check; they are archived for
        # reconcile.py and deliberately NOT written into candles_15m, which is
        # a 15-minute table.
        return
    first = res.candles[0]
    iid = res.req.instrument_id or int(first.instrument_id)
    basis_ids.add(first.adjustment_basis_id)
    rows, rev = store.upsert_candles(
        res.req.symbol, iid, res.candles,
        vendor_id=first.vendor_id or "kite",
        adjustment_basis_id=first.adjustment_basis_id,
        exchange=first.exchange or "NSE",
        source_request_id=res.request_id, run_id=run_id)
    stats.rows_written += rows
    written_symbols.add(res.req.symbol)


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description="execute the fetch plan")
    ap.add_argument("--plan", default=str(ARTIFACTS / "plan" / "fetch_plan.json"))
    ap.add_argument("--db", default=str(MARKET15_DB))
    ap.add_argument("--threads", type=int, default=6)
    ap.add_argument("--limit", type=int, default=None, help="first N requests only")
    ap.add_argument("--no-resume", action="store_true")
    ap.add_argument("--run-id")
    a = ap.parse_args(argv)
    plan = load_plan(Path(a.plan))
    run(plan, db=Path(a.db), threads=a.threads, limit=a.limit,
        resume=not a.no_resume, run_id=a.run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
