"""Step 2 -- build the fetch plan -- contract section 3.2.

Five jobs, in priority order:

1. ``visibility_probe``       one small request per known-invisible symbol, so
                              "this Kite account cannot see it" is a *measured*
                              claim and not an inherited assumption.
2. ``gap_fill``               every NIFTY 500 symbol, 15-minute, from its last
                              stored bar to the latest completed bar.
3. ``flagged_full_history``   full 15-minute history for the symbols the
                              withheld source-quality screen flagged.
4. ``disputed_window``        the windows ``diagnose`` marked as contradicted,
                              for symbols not already getting full history.
5. ``daily_crosscheck``       daily bars for all 500 -- an *independent* series
                              to reconcile the intraday against.
6. ``audit_sample``           100 random non-flagged symbols, 15-minute, recent
                              6 months, to test whether the problem is wider
                              than the 37 the screen named.
7. ``listing_probe``          one 200-day request at each symbol's *earliest*
                              stored bar.  The legacy DB has symbols whose 15m
                              history starts years before they listed
                              (DELHIVERY, SONACOMS, AWL, STARHEALTH list in
                              2021-22 with a first bar in 2015), which is what a
                              reused instrument token looks like.  Asking the
                              vendor for that exact window turns "we think
                              those rows are wrong" into "we asked and the
                              vendor has nothing there".

The plan respects the provider's real per-request day caps and the **global**
rate limit for the key, and is printed -- requests and estimated minutes --
before anything is executed.
"""

from __future__ import annotations

import argparse
import json
import random
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional, Sequence

from market_data.repair import (
    ARTIFACTS,
    BASE_TIMEFRAME,
    DAILY_TIMEFRAME,
    INVISIBLE_TO_ACCOUNT,
    KANIDA_DB,
    MissingDependency,
    flagged_symbols,
    read_only_conn,
    resolve_provider,
    universe,
)

#: Kite's documented per-request caps; used only when a live provider is not
#: available (``--offline``).  A real run reads them off the provider.
FALLBACK_CAPS = {"15minute": 200, "30minute": 200, "60minute": 400, "day": 2000}
FALLBACK_RATE = 3.0

AUDIT_SAMPLE_SIZE = 100
AUDIT_MONTHS = 6
AUDIT_SEED = 20260915          # fixed so the sample is reproducible
PROBE_DAYS = 5

PRIORITY = {
    "visibility_probe": 0,
    "listing_probe": 0,
    "gap_fill": 1,
    "flagged_full_history": 2,
    "disputed_window": 3,
    "daily_crosscheck": 4,
    "audit_sample": 5,
}


@dataclass(frozen=True)
class FetchRequest:
    symbol: str
    instrument_id: int
    timeframe: str
    start: str            # 'YYYY-MM-DD' inclusive
    end: str              # 'YYYY-MM-DD' inclusive
    purpose: str

    @property
    def key(self) -> str:
        """Deterministic id -- this is what makes ``refresh`` resumable."""
        return f"{self.symbol}|{self.timeframe}|{self.start}|{self.end}|{self.purpose}"

    @property
    def days(self) -> int:
        return (date.fromisoformat(self.end) - date.fromisoformat(self.start)).days + 1


@dataclass
class FetchPlan:
    created_at: str
    provider: str
    rate_limit_per_second: float
    caps: dict
    requests: list = field(default_factory=list)
    quarantined: dict = field(default_factory=dict)
    notes: list = field(default_factory=list)

    # -- estimates -----------------------------------------------------------
    @property
    def n_requests(self) -> int:
        return len(self.requests)

    def by_purpose(self) -> dict:
        out: dict[str, dict] = {}
        for r in self.requests:
            d = out.setdefault(r.purpose, {"requests": 0, "symbols": set(), "days": 0})
            d["requests"] += 1
            d["symbols"].add(r.symbol)
            d["days"] += r.days
        return {k: {"requests": v["requests"], "symbols": len(v["symbols"]),
                    "calendar_days": v["days"]}
                for k, v in sorted(out.items(), key=lambda kv: PRIORITY.get(kv[0], 9))}

    def estimated_seconds(self, per_request_overhead: float = 0.0) -> float:
        """Floor set by the global rate limit, plus any measured overhead.

        With N workers sharing one token bucket at R requests/second the wall
        clock cannot beat ``N_requests / R``; latency only adds to it.
        """
        return self.n_requests / max(self.rate_limit_per_second, 0.001) \
            + self.n_requests * per_request_overhead

    def to_dict(self) -> dict:
        return {
            "created_at": self.created_at,
            "provider": self.provider,
            "rate_limit_per_second": self.rate_limit_per_second,
            "caps": self.caps,
            "total_requests": self.n_requests,
            "estimated_minutes_at_rate_limit": round(self.estimated_seconds() / 60, 1),
            "by_purpose": self.by_purpose(),
            "quarantined": self.quarantined,
            "notes": self.notes,
            "requests": [asdict(r) for r in self.requests],
        }

    def render(self) -> str:
        d = self.to_dict()
        lines = [
            "FETCH PLAN",
            f"  provider              : {d['provider']}",
            f"  global rate limit     : {d['rate_limit_per_second']:g} req/s (shared by all workers)",
            f"  per-request day caps  : {d['caps']}",
            "",
            f"  {'purpose':<22}{'requests':>10}{'symbols':>10}{'cal.days':>12}",
            f"  {'-' * 54}",
        ]
        for purpose, v in d["by_purpose"].items():
            lines.append(f"  {purpose:<22}{v['requests']:>10}{v['symbols']:>10}"
                         f"{v['calendar_days']:>12}")
        lines += [
            f"  {'-' * 54}",
            f"  {'TOTAL':<22}{d['total_requests']:>10}",
            "",
            f"  estimated wall clock  : {d['estimated_minutes_at_rate_limit']} min "
            f"(rate-limit floor; network latency adds to this)",
        ]
        if self.quarantined:
            lines.append("")
            lines.append("  quarantined (not fetched as history):")
            for sym, why in sorted(self.quarantined.items()):
                lines.append(f"    {sym:<14} {why}")
        for n in self.notes:
            lines.append(f"  note: {n}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# chunking
# ---------------------------------------------------------------------------


def chunk(symbol: str, instrument_id: int, timeframe: str, start: date, end: date,
          purpose: str, cap_days: int) -> list[FetchRequest]:
    out: list[FetchRequest] = []
    cur = start
    while cur <= end:
        stop = min(cur + timedelta(days=cap_days - 1), end)
        out.append(FetchRequest(symbol, instrument_id, timeframe,
                                cur.isoformat(), stop.isoformat(), purpose))
        cur = stop + timedelta(days=1)
    return out


def _merge_spans(spans: Iterable[tuple[date, date]]) -> list[tuple[date, date]]:
    out: list[list[date]] = []
    for a, b in sorted(spans):
        if out and a <= out[-1][1] + timedelta(days=1):
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return [(a, b) for a, b in out]


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------


def _listing_probe(sym: str, iid: int, cov: dict, cap15: int,
                   full_history: bool) -> list:
    """One request at the symbol's earliest stored bar (skipped when the full
    history is already being fetched -- that covers it)."""
    first = (cov.get("first_bar") or "")[:10]
    if not first or full_history:
        return []
    a = date.fromisoformat(first)
    return chunk(sym, iid, BASE_TIMEFRAME, a,
                 a + timedelta(days=cap15 - 1), "listing_probe", cap15)


def build_plan(*, diagnose_dir: Path = ARTIFACTS / "diagnose",
               db: Path = KANIDA_DB,
               provider=None, offline: bool = False,
               now: Optional[datetime] = None,
               audit_sample_size: int = AUDIT_SAMPLE_SIZE,
               history_start: str = "2015-01-01",
               daily_history_start: str = "2013-01-01") -> FetchPlan:
    now = now or datetime.now()

    if provider is None and not offline:
        provider = resolve_provider()
    if provider is not None:
        caps = dict(getattr(provider, "max_days_per_request", FALLBACK_CAPS))
        rate = float(getattr(provider, "rate_limit_per_second", FALLBACK_RATE))
        provider_id = getattr(provider, "provider_id", "unknown")
        try:
            last_complete = provider.latest_completed_bar(BASE_TIMEFRAME, now).date()
        except Exception:
            last_complete = now.date() - timedelta(days=1)
    else:
        caps, rate, provider_id = dict(FALLBACK_CAPS), FALLBACK_RATE, "offline"
        last_complete = now.date() - timedelta(days=1)

    cap15 = int(caps.get(BASE_TIMEFRAME, 200))
    cap1d = int(caps.get(DAILY_TIMEFRAME, 2000))

    con = read_only_conn(db)
    try:
        syms = universe(con)
        tokens = {r["symbol"]: r["kite_token"] for r in con.execute(
            "SELECT symbol, kite_token FROM instrument_labels WHERE kite_token IS NOT NULL")}
    finally:
        con.close()

    cov_path = Path(diagnose_dir) / "coverage.json"
    coverage = json.loads(cov_path.read_text(encoding="utf-8")) if cov_path.exists() else {}
    disp_path = Path(diagnose_dir) / "disputed_windows.json"
    disputed = json.loads(disp_path.read_text(encoding="utf-8")) if disp_path.exists() else {}

    flagged = set(flagged_symbols())
    universe_symbols = [s.symbol for s in syms]
    plan_syms = {s.symbol: s for s in syms}
    # flagged stocks are not all in the NIFTY 500 -- fetch them anyway, they are
    # the ones the withheld screen named.
    for f in flagged:
        if f not in plan_syms:
            plan_syms[f] = None  # type: ignore[assignment]

    requests: list[FetchRequest] = []
    quarantined: dict[str, str] = {}
    notes: list[str] = []
    if not coverage:
        notes.append(f"no coverage.json under {diagnose_dir}; gap fill spans the "
                     f"full history for every symbol")

    hist_start = date.fromisoformat(history_start)
    daily_start = date.fromisoformat(daily_history_start)
    full_history_syms = set(flagged)

    for sym in sorted(plan_syms):
        # 0 means "resolve the instrument from the provider's own dump at fetch
        # time".  A symbol missing from instrument_labels is exactly the case we
        # must not assume away -- the provider may well know it.
        iid = int(tokens.get(sym) or 0)
        if iid == 0:
            notes.append(f"{sym}: no kite_token in instrument_labels; the provider "
                         f"will be asked to resolve the symbol at fetch time")

        if sym in INVISIBLE_TO_ACCOUNT:
            # One probe, not a history fetch: we need evidence, not an assumption.
            requests += chunk(sym, iid, BASE_TIMEFRAME,
                              last_complete - timedelta(days=PROBE_DAYS - 1),
                              last_complete, "visibility_probe", cap15)
            quarantined[sym] = ("reported invisible to this Kite account; one "
                                "probe request will confirm or refute it")
            continue

        cov = coverage.get(sym) or {}
        last_bar = (cov.get("last_bar") or "")[:10]

        # 1. gap fill ------------------------------------------------------
        gap_from = (date.fromisoformat(last_bar) + timedelta(days=1)) if last_bar else hist_start
        if gap_from <= last_complete and sym not in full_history_syms:
            requests += chunk(sym, iid, BASE_TIMEFRAME, gap_from, last_complete,
                              "gap_fill", cap15)

        # 2. full 15m history for the flagged ------------------------------
        if sym in full_history_syms:
            first = (cov.get("first_bar") or "")[:10]
            start = min(date.fromisoformat(first), hist_start) if first else hist_start
            requests += chunk(sym, iid, BASE_TIMEFRAME, start, last_complete,
                              "flagged_full_history", cap15)

        # 3. disputed windows ---------------------------------------------
        if sym in disputed and sym not in full_history_syms:
            spans = _merge_spans(
                (date.fromisoformat(a), date.fromisoformat(b)) for a, b in disputed[sym])
            windowed: list[FetchRequest] = []
            for a, b in spans:
                if b >= gap_from:          # already inside the gap-fill window
                    b = min(b, gap_from - timedelta(days=1))
                if a > b:
                    continue
                windowed += chunk(sym, iid, BASE_TIMEFRAME, a, b,
                                  "disputed_window", cap15)
            # A 200-day request costs exactly what a 5-day one costs.  If the
            # scattered windows need as many requests as the whole history
            # would, fetch the whole history -- same price, strictly more
            # verification.
            first = (cov.get("first_bar") or "")[:10]
            hstart = min(date.fromisoformat(first), hist_start) if first else hist_start
            full = chunk(sym, iid, BASE_TIMEFRAME, hstart, last_complete,
                         "flagged_full_history", cap15)
            requests += full if len(windowed) >= len(full) else windowed

        # 3b. listing probe ------------------------------------------------
        requests += _listing_probe(sym, iid, cov, cap15, sym in full_history_syms)

        # 4. daily cross-check --------------------------------------------
        if sym in universe_symbols or sym in flagged:
            requests += chunk(sym, iid, DAILY_TIMEFRAME, daily_start, last_complete,
                              "daily_crosscheck", cap1d)

    # 5. audit sample ------------------------------------------------------
    pool = [s for s in universe_symbols
            if s not in flagged and s not in INVISIBLE_TO_ACCOUNT and s in tokens]
    rng = random.Random(AUDIT_SEED)
    sample = sorted(rng.sample(pool, min(audit_sample_size, len(pool))))
    audit_start = last_complete - timedelta(days=AUDIT_MONTHS * 31)
    for sym in sample:
        cov = coverage.get(sym) or {}
        last_bar = (cov.get("last_bar") or "")[:10]
        stop = date.fromisoformat(last_bar) if last_bar else last_complete
        start = max(audit_start, date.fromisoformat((cov.get("first_bar") or "2015-01-01")[:10]))
        if start <= stop:
            requests += chunk(sym, int(tokens.get(sym) or 0), BASE_TIMEFRAME, start, stop,
                              "audit_sample", cap15)
    notes.append(f"audit sample: {len(sample)} non-flagged symbols, seed={AUDIT_SEED} "
                 f"(reproducible), window {audit_start} .. {last_complete}")
    notes.append(f"latest completed {BASE_TIMEFRAME} bar according to the provider: "
                 f"{last_complete}")

    requests.sort(key=lambda r: (PRIORITY.get(r.purpose, 9), r.symbol, r.start))
    return FetchPlan(created_at=now.isoformat(), provider=provider_id,
                     rate_limit_per_second=rate, caps=caps, requests=requests,
                     quarantined=quarantined, notes=notes)


def save(plan: FetchPlan, path: Path = ARTIFACTS / "plan" / "fetch_plan.json") -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plan.to_dict(), indent=2), encoding="utf-8")
    return path


def load(path: Path = ARTIFACTS / "plan" / "fetch_plan.json") -> FetchPlan:
    d = json.loads(Path(path).read_text(encoding="utf-8"))
    return FetchPlan(created_at=d["created_at"], provider=d["provider"],
                     rate_limit_per_second=d["rate_limit_per_second"],
                     caps=d["caps"],
                     requests=[FetchRequest(**r) for r in d["requests"]],
                     quarantined=d.get("quarantined", {}), notes=d.get("notes", []))


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="build and print the fetch plan")
    ap.add_argument("--diagnose-dir", default=str(ARTIFACTS / "diagnose"))
    ap.add_argument("--offline", action="store_true",
                    help="do not contact the provider; use documented Kite caps")
    ap.add_argument("--audit-sample", type=int, default=AUDIT_SAMPLE_SIZE)
    ap.add_argument("--out", default=str(ARTIFACTS / "plan" / "fetch_plan.json"))
    a = ap.parse_args(argv)
    plan = build_plan(diagnose_dir=Path(a.diagnose_dir), offline=a.offline,
                      audit_sample_size=a.audit_sample)
    print(plan.render())
    print(f"\nwritten: {save(plan, Path(a.out))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
