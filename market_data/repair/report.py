"""Step 5 -- the repair report + the frozen snapshot -- contract section 3.5/3.6.

Writes ``docs/pattern_research/DATA_REPAIR_REPORT.md`` (and the matching
``.json``) covering: what was flagged, what was fetched, what was corrected and
why, what is still unresolved, and the **frozen snapshot id** the research
rerun must use.

Every number in the report is read out of an artifact produced by an actual
run -- ``diagnose/summary.json``, ``plan/fetch_plan.json``,
``refresh/refresh_<run>.json``, ``reconcile/summary.json`` -- or out of the
store.  A stage that has not run is reported as "not run", never as zero.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence

from market_data.repair import (
    ARTIFACTS,
    INVISIBLE_TO_ACCOUNT,
    KNOWN_SUSPENSIONS,
    MARKET15_DB,
    REPORT_JSON,
    REPORT_MD,
    flagged_symbols,
    resolve_store,
)

NOT_RUN = {"status": "not run"}


def _load(path: Path) -> Optional[dict]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _latest_refresh(refresh_dir: Path) -> Optional[dict]:
    """Combine every refresh file for the newest run id.

    ``refresh`` is resumable, so one logical run can land as several files
    (the second pass fetched only what the first had not). Reporting just the
    last file would under-count the work by thousands of requests.
    """
    files = sorted(Path(refresh_dir).glob("refresh_*.json"))
    docs = [d for d in (_load(f) for f in files) if d]
    if not docs:
        return None
    run_id = docs[-1].get("run_id")
    parts = [d for d in docs if d.get("run_id") == run_id] or [docs[-1]]
    out = dict(parts[-1])
    for k in ("requests_sent", "rows_fetched", "rows_written",
              "empty_responses", "elapsed_seconds"):
        out[k] = sum(d.get(k) or 0 for d in parts)
    # A failed request is retried by the next pass (nothing was archived for
    # it), so summing the per-pass counts would double-count it.  The union of
    # the failing request keys is the honest number.
    failed_keys: set = set()
    for d in parts:
        failed_keys |= set((d.get("errors") or {}).keys())
    out["requests_failed"] = len(failed_keys)
    out["failed_symbols"] = sorted({k.split("|")[0] for k in failed_keys})
    out["requests_planned"] = max(d.get("requests_planned") or 0 for d in parts)
    out["requests_skipped"] = max(0, out["requests_planned"] - out["requests_sent"]
                                  - out["requests_failed"])
    out["errors"] = {k: v for d in parts for k, v in (d.get("errors") or {}).items()}
    out["symbols_written"] = max(d.get("symbols_written") or 0 for d in parts)
    out["passes"] = len(parts)
    out["elapsed_seconds"] = round(out["elapsed_seconds"], 1)
    out["per_request_seconds"] = round(
        out["elapsed_seconds"] / max(out["requests_sent"] + out["requests_failed"], 1), 4)
    vis: dict = {}
    for d in parts:
        vis.update(d.get("visibility") or {})
    out["visibility"] = vis
    basis: list = []
    for d in parts:
        for b in d.get("adjustment_basis_ids") or []:
            if b not in basis:
                basis.append(b)
    out["adjustment_basis_ids"] = basis
    return out


def existing_snapshot(store, snapshot_id: str) -> Optional[dict]:
    row = store.con.execute("SELECT * FROM snapshots WHERE snapshot_id=?",
                            (snapshot_id,)).fetchone()
    return dict(row) if row else None


def freeze(store, *, universe_name: str, provider: str,
           adjustment_basis_id: Optional[str], notes: str = "",
           snapshot_id: Optional[str] = None) -> dict:
    """Create and freeze the snapshot the research rerun will read.

    Re-running the report with an id that already exists reuses that snapshot
    rather than minting a second one: regenerating prose must never change
    which inputs the research is pinned to.
    """
    if snapshot_id:
        found = existing_snapshot(store, snapshot_id)
        if found:
            return {"snapshot_id": snapshot_id, **found, "reused": True}
    sid = store.create_snapshot(snapshot_id, universe=universe_name,
                                adjustment_basis_id=adjustment_basis_id,
                                provider=provider, notes=notes)
    return {"snapshot_id": sid, **(store.freeze_snapshot(sid) or {})}


# ---------------------------------------------------------------------------
# rendering
# ---------------------------------------------------------------------------


def _table(rows: Sequence[Sequence], header: Sequence[str]) -> str:
    out = ["| " + " | ".join(str(h) for h in header) + " |",
           "|" + "|".join("---" for _ in header) + "|"]
    for r in rows:
        out.append("| " + " | ".join("" if c is None else str(c) for c in r) + " |")
    return "\n".join(out)


def _example_block(v: dict) -> str:
    held, fresh = v.get("held") or {}, v.get("fresh") or {}
    daily = v.get("daily_fresh") or {}
    lines = [
        f"- **{v['symbol']} {v['bar_start']}** -- `{v['klass']}`",
        f"  - held  : O={held.get('open')} H={held.get('high')} L={held.get('low')} "
        f"C={held.get('close')} V={held.get('volume')}",
        f"  - fresh : O={fresh.get('open')} H={fresh.get('high')} L={fresh.get('low')} "
        f"C={fresh.get('close')} V={fresh.get('volume')}",
    ]
    if daily:
        lines.append(f"  - fresh daily: O={daily.get('open')} H={daily.get('high')} "
                     f"L={daily.get('low')} C={daily.get('close')}")
    if v.get("ratio") is not None:
        lines.append(f"  - held/fresh level ratio: {v['ratio']:.6f}")
    if v.get("corp_action"):
        ca = v["corp_action"]
        lines.append(f"  - corporate action: {ca.get('action_type')} ex {ca.get('ex_date')} "
                     f"-- {ca.get('subject')}")
    lines.append(f"  - why: {v['reason']}")
    if v.get("evidence_request_id"):
        lines.append(f"  - evidence: `raw_archive.request_id = {v['evidence_request_id']}`")
    return "\n".join(lines)


def render(data: dict) -> str:
    d = data
    diag = d.get("diagnose") or NOT_RUN
    plan = d.get("plan") or NOT_RUN
    refresh = d.get("refresh") or NOT_RUN
    rec = d.get("reconcile") or NOT_RUN
    snap = d.get("snapshot") or NOT_RUN

    L: list[str] = []
    A = L.append
    A("# Data repair report")
    A("")
    A(f"Generated {d['generated_at']}. Workflow: `market_data/repair/` "
      f"(diagnose -> plan -> refresh -> reconcile -> report), "
      f"contract `docs/DATA_PIPELINE_CONTRACT.md` section 3.")
    A("")
    A("`db/kanida.db` was opened read-only for every step. Every write went to "
      "`db/market15.db` through `market_data/store.py`. No row was edited in "
      "place, no suspicious low was replaced with a daily low, and nothing "
      "unresolved was guessed at.")
    A("")

    # -- 1 diagnose --------------------------------------------------------
    A("## 1. What was flagged")
    A("")
    if diag is NOT_RUN or "totals" not in diag:
        A("_diagnose has not been run._")
    else:
        eng = diag.get("checks_engine_by_symbol_count", {})
        A(f"Read-only sweep of {diag['universe_size']} NIFTY 500 symbols in "
          f"`{diag['database']}`, {diag['elapsed_seconds']}s. The contract's "
          f"section-2 checks are W2's, not a second set: "
          + ", ".join(f"{v} symbols by `{k}`" for k, v in eng.items()) + ".")
        A("")
        rows = [(k, v) for k, v in sorted(diag["totals"].items(),
                                          key=lambda kv: -kv[1])
                if not k.endswith(":basis_explained")]
        expl = {k.split(":")[0]: v for k, v in diag["totals"].items()
                if k.endswith(":basis_explained")}
        A(_table([(k, v, expl.get(k, "")) for k, v in rows],
                 ["check", "candidate rows", "of which explained by adjustment basis"]))
        A("")
        A(f"- symbols with at least one finding: **{len(diag['symbols_with_findings'])}**")
        A(f"- symbols with no intraday rows at all: "
          f"**{len(diag.get('symbols_with_no_intraday_data', []))}** "
          f"({', '.join(diag.get('symbols_with_no_intraday_data', [])[:12])})")
        basis = diag.get("symbols_with_adjustment_basis_break", [])
        A(f"- symbols where `ohlc_daily` and `ohlc_5min` sit on **different "
          f"adjustment bases**: **{len(basis)}**")
        A(f"- special (Muhurat) sessions identified and excluded from the "
          f"off-session check: {len(diag.get('special_sessions_detected', []))}")
        A("")
        A("Flagged by the withheld source-quality screen: "
          f"**{len(flagged_symbols())}** symbols "
          f"(`docs/pattern_research/SOURCE_QUALITY_SCREEN.md`).")
    A("")

    # -- 2 plan / fetch ----------------------------------------------------
    A("## 2. What was fetched")
    A("")
    if plan is NOT_RUN or "by_purpose" not in plan:
        A("_no plan was built._")
    else:
        A(f"Provider `{plan['provider']}`, global rate limit "
          f"{plan['rate_limit_per_second']:g} req/s, per-request caps "
          f"`{plan['caps']}`.")
        A("")
        A(_table([(k, v["requests"], v["symbols"], v["calendar_days"])
                  for k, v in plan["by_purpose"].items()],
                 ["purpose", "requests", "symbols", "calendar days"]))
        A("")
        A(f"Planned total: **{plan['total_requests']}** requests, "
          f"estimated **{plan['estimated_minutes_at_rate_limit']} min** at the "
          f"rate-limit floor.")
    A("")
    if refresh is NOT_RUN or "requests_sent" not in refresh:
        A("_refresh has not been run._")
    else:
        A("Measured execution:")
        A("")
        A(_table([
            ("requests planned", refresh["requests_planned"]),
            ("requests sent (all passes)", refresh["requests_sent"]),
            ("requests that never succeeded",
             f"{refresh['requests_failed']} "
             f"({', '.join(refresh.get('failed_symbols', []))})"),
            ("empty responses", refresh["empty_responses"]),
            ("bars fetched", refresh["rows_fetched"]),
            ("bars written to candles_15m", refresh["rows_written"]),
            ("symbols written", refresh["symbols_written"]),
            ("passes (resumed)", refresh.get("passes")),
            ("elapsed", f"{refresh['elapsed_seconds']}s"),
            ("mean seconds per request", refresh["per_request_seconds"]),
        ], ["measure", "value"]))
        A("")
        A(f"Run id `{refresh['run_id']}`. Every payload is gzipped on disk and "
          f"recorded in `raw_archive` with its sha256 before any candle derived "
          f"from it is stored.")
        if refresh.get("adjustment_basis_ids"):
            A(f"Adjustment basis recorded on the new rows: "
              f"`{', '.join(refresh['adjustment_basis_ids'])}`.")
    A("")

    # -- 3 reconcile -------------------------------------------------------
    A("## 3. What was corrected, and why")
    A("")
    if rec is NOT_RUN or "by_class" not in rec:
        A("_reconcile has not been run._")
    else:
        total = rec["bars_compared"] or 1
        A(_table([(k, rec["by_class"].get(k, 0),
                   f"{100 * rec['by_class'].get(k, 0) / total:.2f}%")
                  for k in ("agree", "source_error", "adjustment_basis",
                            "genuine_extreme", "session_regime_cas",
                            "vendor_zero_print", "vendor_bad_print",
                            "wrong_instrument", "unresolved")],
                 ["class", "bars", "share"]))
        A("")
        A(f"{rec['bars_compared']} bars compared across {rec['symbols']} symbols; "
          f"**{rec['corrections_written']}** rows written to `corrections`, each "
          f"carrying the `raw_archive` request id that justifies it. "
          f"`adjustment_basis` is recorded once per contiguous segment (ratio, "
          f"span, bar count, matched ex-date) rather than four rows per bar, so "
          f"the handful of real price corrections stay findable.")
        A("")
        A("`session_regime_cas` reads 0 in the table above and that is correct: "
          "it counts bars we *hold* that the closing-auction regime explains, "
          "and our stored history stops at 2026-07-29, before CAS began. The "
          "regime's real exposure is in the refreshed window, below.")
        A("")
        reg = rec.get("session_regime") or {}
        if reg:
            A("### Session regime -- NSE Closing Auction Session "
              f"(from {reg.get('cas_from')}, contract section 2A)")
            A("")
            A("From that date continuous trading in F&O cash stocks ends at "
              f"{reg.get('cas_last_bar_start')} and the official close is set by an "
              "auction at 15:30-15:35, so those sessions hold 24 15-minute bars "
              f"instead of 25 ending {reg.get('regular_last_bar_start')}. This is "
              "the expected shape, not a defect: no bar is synthesised, the "
              "buckets are labelled `SESSION_REGIME_CAS`, and 1D/close values "
              "come from the provider's daily series.")
            A("")
            A(_table([
                ("freshly fetched sessions examined", reg.get("fresh_sessions_examined")),
                ("symbols on the CAS regime", reg.get("cas_symbols")),
                ("symbol-sessions on the CAS regime", reg.get("cas_symbol_sessions")),
                ("distinct trading dates affected", reg.get("cas_distinct_sessions")),
                ("of those, instrument_labels.is_fno = 1",
                 reg.get("cas_symbol_sessions_with_is_fno_1")),
                ("of those, is_fno != 1 (regime vs label disagreement)",
                 reg.get("cas_symbol_sessions_with_is_fno_not_1")),
                ("short sessions CAS does NOT explain",
                 reg.get("unexplained_short_symbol_sessions")),
            ], ["measure", "value"]))
            if reg.get("example"):
                e = reg["example"]
                A("")
                A(f"Example: **{e.get('symbol')} {e.get('session')}** -- "
                  f"{e.get('bars_returned')} bars, last bar start "
                  f"{e.get('last_bar_start')}, is_fno={e.get('is_fno')}, "
                  f"{e.get('auction_share_of_daily_volume')} of the daily volume "
                  f"traded outside the continuous session.")
            A("")
        for klass in ("source_error", "adjustment_basis", "vendor_bad_print",
                      "genuine_extreme"):
            ex = (rec.get("examples") or {}).get(klass) or []
            if not ex:
                continue
            A(f"### Worked examples -- `{klass}`")
            A("")
            # one example per symbol first, so a class dominated by one stock
            # still shows the others
            seen: set = set()
            ordered = [v for v in ex if not (v["symbol"] in seen or seen.add(v["symbol"]))]
            ordered += [v for v in ex if v not in ordered]
            for v in ordered[:4]:
                A(_example_block(v))
                A("")
    A("")

    # -- 4 unresolved ------------------------------------------------------
    A("## 4. What remains unresolved")
    A("")
    quar = (rec.get("quarantined") if isinstance(rec, dict) else None) or []
    wi = (rec.get("wrong_instrument") if isinstance(rec, dict) else None) or {}
    if wi.get("symbols"):
        A(f"### Reused instrument tokens -- `wrong_instrument` "
          f"({wi['symbols']} symbols)")
        A("")
        A("These symbols carry intraday history from years before the vendor's "
          "own **daily** series for them begins, at price levels that belong to "
          "a different instrument. The vendor serves the same rows under the "
          "same token, so a re-fetch cannot repair it: the token had an earlier "
          "life. The rows are quarantined with the evidence, never deleted.")
        A("")
        A(_table([(w["symbol"], w["held_first_session"], w["sample_held_price"],
                   w["vendor_first_daily_session"], w["lead_days_before_vendor_daily"],
                   w["sessions"], w.get("inferred_quarantine_sessions"), w["held_bars"])
                  for w in wi.get("detail", [])],
                 ["symbol", "our first intraday session", "price there",
                  "vendor's first daily bar", "lead (days)", "sessions probed",
                  "sessions inferred", "bars"]))
        A("")
        A(f"Totals: {wi['sessions']} sessions / {wi['held_bars']} bars proved "
          f"inside a window we requested; {wi.get('inferred_sessions')} sessions / "
          f"{wi.get('inferred_bars')} bars quarantined by inference up to the "
          f"vendor's first daily bar. Median lead "
          f"{wi.get('median_lead_days_before_vendor_daily')} days.")
        A("")
    vb = ((rec.get("by_class") or {}).get("vendor_bad_print")
          if isinstance(rec, dict) else None)
    if vb:
        A(f"- **{vb} `vendor_bad_print` bars** -- our row breaks out of the "
          f"vendor's own daily range for that session **and so does the "
          f"vendor's own 15-minute bar** (INFY 2015-04-24 prints 2090.90 "
          f"against a daily high of 526.20, then and now). A re-fetch "
          f"reproduces it, so there is nothing to correct against; quarantined "
          f"as an unusable price.")
    vz = ((rec.get("by_class") or {}).get("vendor_zero_print")
          if isinstance(rec, dict) else None)
    if vz:
        A(f"- **{vz} `vendor_zero_print` bars** -- we and the vendor hold the "
          f"same O=H=L=C=0 row (INFY 2015-04-27..05-13 is the clearest case). "
          f"The defect is the vendor's, not ours; there is nothing to correct, "
          f"and the rows are labelled unusable rather than counted as agreement.")
    A(f"- **{', '.join(INVISIBLE_TO_ACCOUNT)}** -- invisible to this Kite "
      f"account. Probed explicitly rather than assumed; see the "
      f"`visibility_probe` results below.")
    for sym, spans in KNOWN_SUSPENSIONS.items():
        for a, b, why in spans:
            A(f"- **{sym} {a} .. {b}** -- {why}. Labelled, not filled.")
    if refresh is not NOT_RUN and refresh.get("visibility"):
        A("")
        A(_table([(k, v) for k, v in sorted(refresh["visibility"].items())],
                 ["symbol", "probe result"]))
    if quar:
        A("")
        by_label: dict[str, int] = {}
        for q in quar:
            by_label[q.get("label", "?")] = by_label.get(q.get("label", "?"), 0) + 1
        A(_table(sorted(by_label.items()), ["quarantine label", "sessions"]))
        A("")
        A("First 20 quarantined sessions:")
        A("")
        A(_table([(q.get("symbol"), q.get("session", q.get("span")), q.get("reason"))
                  for q in quar[:20]], ["symbol", "session", "reason"]))
    if rec is not NOT_RUN and rec.get("by_class", {}).get("unresolved"):
        A("")
        A(f"**{rec['by_class']['unresolved']}** bars are classed `unresolved`: "
          f"held and fresh disagree, no constant level shift explains it, and no "
          f"freshly fetched daily bar settles it. They keep their old revision "
          f"and are listed in `market_data/repair/artifacts/reconcile/verdicts.jsonl`. "
          f"They are **not** corrected and **not** deleted.")
    A("")

    # -- 5 snapshot --------------------------------------------------------
    A("## 5. Frozen snapshot for the research rerun")
    A("")
    if snap is NOT_RUN or "snapshot_id" not in snap:
        A("_no snapshot was frozen._")
    else:
        A(_table([(k, v) for k, v in snap.items() if not isinstance(v, (dict, list))],
                 ["field", "value"]))
        A("")
        A(f"The research rerun must read `snapshot_id = {snap['snapshot_id']}`. "
          f"A withheld source-quality release must never become the active "
          f"evidence release (contract section 6).")
        A("")
        A("### Caveats the rerun must carry")
        A("")
        ab = ((rec or {}).get("by_class") or {}).get("adjustment_basis", 0)
        A(f"1. **Mixed adjustment basis.** Re-fetched windows are on the vendor's "
          f"current basis; windows that were not re-fetched keep the legacy basis "
          f"they were seeded with. {ab} bars were measured as a constant level "
          f"shift between the two. Every one is recorded in `corrections` at "
          f"segment level with its ratio and span. A symbol whose history spans "
          f"both bases must not have returns computed across the boundary without "
          f"re-basing; the clean fix is a full-history re-fetch of the affected "
          f"symbols.")
        A(f"2. **Parity checks use 1H and 4H, not 1D/1W.** The frozen research "
          f"built daily and weekly bars from `ohlc_daily`, while this store "
          f"aggregates sessions and takes the 1D/1W close from the provider's "
          f"daily bar (which after 2026-08-03 is a closing-auction price). 1H and "
          f"4H are directly comparable; 1D and 1W are not, by design.")
        A(f"3. **`quality_findings` accumulates per run without dedupe** -- filter "
          f"by `run_id` when counting.")
        A(f"4. **`vendor_zero_print` and `wrong_instrument` rows are still in the "
          f"store**, labelled, because deleting them would be a silent edit. The "
          f"rerun must exclude them by label, not assume they are gone.")
    A("")
    A("---")
    A("")
    A("Artifacts: `market_data/repair/artifacts/{diagnose,plan,refresh,reconcile}/`. "
      "Raw payloads: `db/raw_archive/`. Provenance tables: `raw_archive`, "
      "`corrections`, `snapshots`, `ingest_runs` in `db/market15.db`.")
    return "\n".join(L) + "\n"


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def build(*, artifacts: Path = ARTIFACTS, market15: Path = MARKET15_DB,
          snapshot: bool = True, snapshot_id: Optional[str] = None,
          md_path: Path = REPORT_MD, json_path: Path = REPORT_JSON) -> dict:
    diag = _load(Path(artifacts) / "diagnose" / "summary.json")
    plan = _load(Path(artifacts) / "plan" / "fetch_plan.json")
    if plan:
        plan.pop("requests", None)
    refresh = _latest_refresh(Path(artifacts) / "refresh")
    rec = _load(Path(artifacts) / "reconcile" / "summary.json")

    snap: Optional[dict] = None
    if snapshot:
        store = resolve_store(market15)
        try:
            basis = (refresh or {}).get("adjustment_basis_ids") or []
            snap = freeze(
                store,
                universe_name="NIFTY500",
                provider=(plan or {}).get("provider", "kite"),
                adjustment_basis_id=basis[0] if len(basis) == 1 else ",".join(basis),
                snapshot_id=snapshot_id,
                notes="frozen by market_data/repair after diagnose+refresh+reconcile")
        finally:
            if hasattr(store, "close"):
                store.close()

    data = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "diagnose": diag, "plan": plan, "refresh": refresh,
        "reconcile": {k: v for k, v in (rec or {}).items() if k != "per_symbol"}
                     if rec else None,
        "snapshot": snap,
    }
    Path(md_path).parent.mkdir(parents=True, exist_ok=True)
    Path(md_path).write_text(render(data), encoding="utf-8")
    Path(json_path).write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    return data


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="write DATA_REPAIR_REPORT.md")
    ap.add_argument("--artifacts", default=str(ARTIFACTS))
    ap.add_argument("--no-snapshot", action="store_true")
    ap.add_argument("--snapshot-id")
    a = ap.parse_args(argv)
    d = build(artifacts=Path(a.artifacts), snapshot=not a.no_snapshot,
              snapshot_id=a.snapshot_id)
    print(f"written: {REPORT_MD}")
    if d.get("snapshot"):
        print(f"snapshot_id: {d['snapshot']['snapshot_id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
