"""
FastAPI surface for the Agent Platform. Mount in main.py exactly like agent_builder:

    from agents.router import router as agents_router
    app.include_router(agents_router, prefix="/api", tags=["Agents"])

Read-only + paper by default. NO execution happens here — an agent's intents flow to
backend/autotrade/ downstream (paper-default, cert-gated, operator-armed).

The chart/* endpoints render the REAL Chart Agent output (honest WATCH verdicts + evidence).
Every one is GUARDED (try/except -> honest JSON, never a 500 crash) and strictly read-only /
point-in-time (as_of = the requested date; only data <= that bar is used).
"""
from __future__ import annotations
import hmac
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Annotated, Optional

from fastapi import APIRouter, Header, HTTPException, Query, Response
from . import registry

log = logging.getLogger("agents.router")
router = APIRouter()
registry.load_builtin()

CHART_AGENT_ID = "chart-v1"


@router.get("/agents/health")
def agents_health():
    return {"ok": True, "agents": [a.manifest.agent_id for a in registry.all_agents()]}


@router.get("/agents")
def list_agents():
    return {"agents": [a.manifest.to_dict() for a in registry.all_agents()]}


@router.get("/agents/{agent_id}")
def get_agent(agent_id: str):
    a = registry.get(agent_id)
    if not a:
        raise HTTPException(404, f"no agent '{agent_id}'")
    return a.manifest.to_dict()


# --------------------------------------------------------------------------- chart helpers
def _chart_agent():
    a = registry.get(CHART_AGENT_ID)
    if a is None:
        raise RuntimeError("chart-v1 agent is not registered")
    return a


def _pattern_forward(evidence: Optional[dict]) -> Optional[dict]:
    """Compact the ported pattern-forward evidence to the headline horizons the UI shows
    (T+1/3/5/10): win% / ETV(mean) / MFE / MAE. Returns None if evidence is absent (honest)."""
    if not evidence or "horizons" not in evidence:
        return None
    hz = evidence["horizons"]
    out = {"n": (evidence.get("summary") or {}).get("n"), "horizons": {}}
    for h in (1, 3, 5, 10):
        row = hz.get(h) or hz.get(str(h))
        if row is None:
            continue
        out["horizons"][str(h)] = {
            "win": row.get("win"),
            "etv": row.get("mean"),   # pattern-forward ETV = mean fwd return at that horizon
            "mfe": row.get("mfe"),
            "mae": row.get("mae"),
        }
    return out


def _strategy_head(strategy: Optional[dict]) -> Optional[dict]:
    """The strategy-replay headline the UI shows — the stats the §9 gates actually read."""
    if not strategy:
        return None
    return {
        "version": strategy.get("version"),
        "etv": strategy.get("etv"),
        "win": strategy.get("win"),
        "payoff": strategy.get("payoff"),
        "n": strategy.get("n"),
        "avg_win": strategy.get("avg_win"),
        "avg_loss": strategy.get("avg_loss"),
        "mae": strategy.get("mae"),
        "ci_low": strategy.get("ci_low"),
        "exits": strategy.get("exits"),
        "avg_holding": strategy.get("avg_holding"),
    }


_STAGE_RANK = {"BREAKOUT": 0, "RETEST": 1, "APPROACHING": 2, "FAILED": 3}


def _recent_occurrence(sym: str, date: Optional[str], lookback: int = 90,
                       pattern: Optional[str] = None):
    """Most recent LIVE-stage occurrence for SYM whose signal bar is <= date (point-in-time).
    Walks back from the as-of bar; at each bar it runs the requested pattern's detector, or — when
    ``pattern`` is None — EVERY registered detector (NOT hardcoded horizontal; fixes the drill-down
    bug) and returns the most actionable stage found on the most recent bar. Returns
    (occurrence_dict, df, k_asof) or (None, df, k)."""
    from .chart import data as cdata
    from .chart.patterns import registry as patterns
    from .chart.agent import _as_of_idx  # reuse the exact point-in-time bar resolver

    if pattern:
        det = patterns.get(pattern)
        if det is None:
            raise RuntimeError(f"pattern '{pattern}' detector not available")
        dets = [det]
    else:
        dets = patterns.all_patterns()
    df = cdata.load_daily(sym)
    df.attrs["symbol"] = sym
    k = _as_of_idx(df, date)
    if k < 0:
        return None, df, k
    lo = max(0, k - lookback)
    for j in range(k, lo - 1, -1):
        best = None
        for det in dets:
            try:
                occ = det.detect(df, as_of_idx=j)
            except Exception as e:  # noqa: BLE001 — one bad detector must not sink the walk
                log.debug("detect %s failed at %s[%d]: %s",
                          getattr(det, "pattern_id", "?"), sym, j, e)
                continue
            if occ:
                cand = occ[0].to_dict()
                if best is None or _STAGE_RANK.get(cand.get("stage"), 9) < _STAGE_RANK.get(best.get("stage"), 9):
                    best = cand
        if best is not None:
            return best, df, k
    return None, df, k


# --------------------------------------------------------------------------- chart endpoints
def _setup_to_row(s: dict) -> dict:
    """Normalise a screener setup (or a precomputed artifact row) to the endpoint's occurrence shape.
    Accepts both the screener's ``symbol`` key and the legacy ``stock`` key."""
    row = {
        "stock": s.get("symbol") or s.get("stock"),
        "pattern": s.get("pattern"),
        "stage": s.get("stage"),
        "level": s.get("level"),
        "distance_pct": s.get("distance_pct") if "distance_pct" in s else (s.get("context") or {}).get("distance_to_level_pct"),
        "volume_x": s.get("volume_x") if "volume_x" in s else (s.get("context") or {}).get("volume_x"),
        "direction": s.get("direction", "long"),
        "sector": s.get("sector"),
        "touches": s.get("touches") if isinstance(s.get("touches"), int) else len(s.get("touches") or []),
        "as_of_date": s.get("as_of_date") or (s.get("context") or {}).get("as_of_date"),
    }
    # storyline enrichment (present when the screen was built with enrich=True) — carried through
    for f in ("tier", "quality_score", "evidence_summary", "hook"):
        if f in s:
            row[f] = s.get(f)
    return row


@router.get("/agents/chart/scan")
def chart_scan(date: Optional[str] = Query(None, description="as-of date YYYY-MM-DD (point-in-time)"),
               limit: int = Query(40, ge=1, le=500),
               full: Annotated[bool, Query(description="return the entire screen (ignore limit)")] = False,
               live: Annotated[bool, Query(description="force an on-request full-universe compute "
                                           "(OFF-gateway only — can exceed a 60s ALB timeout)")] = False):
    """Full-universe live-stage screen for the as-of date. Point-in-time: only bars <= date.

    Serves the POST-MARKET precomputed screen (target <1 s). The full-universe compute (~9 patterns ×
    universe over S3 Parquet) takes tens of seconds and WOULD 504 behind a 60s gateway, so it does NOT
    run on the default request path — build_screen (via the post-market agents.chart.eod job) is the
    serving path. If no precomputed screen exists this returns fast + honest (served="pending"); pass
    ?live=1 only from OFF-gateway contexts (an ECS RunTask / local) to force the compute. Guarded."""
    try:
        from .chart import data as cdata
        from .chart import screener as scr
        if not cdata.db_available():
            return {"ok": False, "date": date, "occurrences": [], "count": 0,
                    "note": f"data source unavailable ({cdata.db_path()}); SPEC: cloud feeds wiring."}

        cached = scr.load_screen(date)
        if cached is not None:
            res = cached
            served = "precompute"
            serve_note = f"served precomputed screen (built_at {cached.get('built_at')})"
        elif live:
            res = scr.scan_universe_detailed(date)
            served = "live"
            serve_note = ("live-computed on the request path (explicit ?live=1) — slow; can exceed a "
                          "60s gateway timeout on the full universe. Use the post-market precompute.")
        else:
            # Never run the ~minute-long full-universe compute behind the request gateway (ALB 60s) —
            # it times out. Serve precomputed only; if absent, return fast + honest and let the
            # post-market EOD job (agents.chart.eod) build the screen.
            return {"ok": True, "date": date, "served": "pending", "count": 0, "occurrences": [],
                    "note": ("no precomputed screen for this date yet — the post-market job "
                             "(agents.chart.eod) builds it; pass ?live=1 from an off-gateway context "
                             "to force an on-request compute.")}

        setups = res.get("setups", [])
        rows = [_setup_to_row(s) for s in setups]
        rows.sort(key=lambda r: {"BREAKOUT": 0, "RETEST": 1, "APPROACHING": 2, "FAILED": 3}.get(r["stage"], 9))
        out_rows = rows if full else rows[:limit]
        # coverage accounting is surfaced so the universe-vs-classified gap is labelled, not hidden
        return {"ok": True, "date": date, "served": served, "note": serve_note,
                "screen_note": res.get("note"), "trading_day": res.get("trading_day"),
                "universe_size": res.get("universe_size"),
                "scanned": res.get("scanned"),
                "skipped_min_bars": res.get("skipped_min_bars"),
                "skipped_stale": res.get("skipped_stale"),
                "skipped_no_window": res.get("skipped_no_window"),
                "enriched": res.get("enriched"),
                "statistically_meaningful": res.get("statistically_meaningful"),
                "qualified": res.get("qualified"),
                "market_story": res.get("market_story"),
                "count": len(rows), "occurrences": out_rows}
    except Exception as e:  # noqa: BLE001 — never 500-crash
        log.warning("chart_scan failed: %s", e)
        return {"ok": False, "date": date, "occurrences": [], "count": 0, "error": str(e)}


@router.get("/agents/chart/bars")
def chart_bars(symbol: str = Query(..., description="stock symbol, e.g. TITAN"),
               date: Optional[str] = Query(None, description="as-of date YYYY-MM-DD (point-in-time)"),
               lookback: int = Query(180, ge=1, le=2000,
                                     description="number of daily bars up to & including date"),
               pattern: Optional[str] = Query(None, description="pattern_id — when set, serves bars "
                                              "instantly from the precomputed per-setup bundle")):
    """Point-in-time daily candles for drawing the chart: only bars dated <= date, the last
    ``lookback`` of them. {symbol, date, lookback, bars:[{date,o,h,l,c,v}]}. Guarded (never 500).

    FAST PATH: when ``pattern`` is supplied and a precomputed per-setup bundle exists for
    (symbol,pattern,date) with >= ``lookback`` embedded bars, they are sliced + served in ms
    (served="precompute"). Otherwise the live point-in-time read runs (served="live", honest note)."""
    sym = (symbol or "").strip().upper()
    try:
        from .chart import data as cdata
        from .chart.agent import _as_of_idx
        if not cdata.db_available():
            return {"ok": False, "symbol": sym, "date": date, "bars": [],
                    "note": f"data source unavailable ({cdata.db_path()}); SPEC: cloud feeds wiring."}
        # FAST PATH — one object read serves the bars the same click already loaded for /setup.
        if pattern:
            try:
                from .chart import screener as scr
                bundle = scr.load_setup_bundle(date, sym, pattern)
                bars_b = bundle.get("bars") if isinstance(bundle, dict) else None
                if isinstance(bars_b, list) and len(bars_b) >= lookback:
                    bars = bars_b[-lookback:]
                    return {"ok": True, "symbol": sym,
                            "date": bundle.get("as_of_date") or date, "lookback": lookback,
                            "count": len(bars), "bars": bars, "served": "precompute",
                            "note": f"served from precomputed bundle (precomputed_at "
                                    f"{bundle.get('precomputed_at')})"}
            except Exception as e:  # noqa: BLE001 — fast path must never sink the live fallback
                log.debug("chart_bars fast-path %s/%s miss: %s", sym, pattern, e)
        df = cdata.load_daily(sym)
        k = _as_of_idx(df, date)
        if k < 0:
            return {"ok": True, "symbol": sym, "date": date, "lookback": lookback, "bars": [],
                    "note": f"no bar on/before {date or 'latest'} for {sym}."}
        lo = max(0, k - lookback + 1)
        w = df.iloc[lo:k + 1]
        bars = [{"date": str(idx.date()) if hasattr(idx, "date") else str(idx),
                 "o": round(float(r.open), 4), "h": round(float(r.high), 4),
                 "l": round(float(r.low), 4), "c": round(float(r.close), 4),
                 "v": int(r.volume) if r.volume == r.volume else None}
                for idx, r in w.iterrows()]
        return {"ok": True, "symbol": sym, "date": str(df.index[k].date()),
                "lookback": lookback, "count": len(bars), "bars": bars, "served": "live",
                "note": ("live point-in-time read (no precomputed bundle for this key/lookback) — "
                         "correct but slower; the post-market EOD job precomputes these.")}
    except Exception as e:  # noqa: BLE001 — never 500-crash
        log.warning("chart_bars(%s) failed: %s", sym, e)
        return {"ok": False, "symbol": sym, "date": date, "bars": [], "error": str(e)}


@router.get("/agents/chart/setup")
def chart_setup(symbol: str = Query(..., description="stock symbol, e.g. TITAN"),
                pattern: str = Query(..., description="pattern_id, e.g. falling_wedge"),
                date: Optional[str] = Query(None, description="as-of date YYYY-MM-DD (point-in-time)")):
    """FULL per-setup detail for column-3: runs THAT pattern's detector (resolved via patterns.get —
    NOT hardcoded horizontal) at the as-of date and returns geometry (real anchors for drawing),
    quality (0-100), pattern-forward evidence, winner/loser paths, decision (§9 gates) and the watch
    plan. Point-in-time; guarded (never 500). Small-N setups honestly return WATCH.

    FAST PATH: first tries the precomputed per-setup bundle for (symbol,pattern,date) — an instant
    single-object read (served="precompute", bars embedded for the same click's /bars). Only if absent
    does it live-compute build_setup (served="live", honest note). Both are point-in-time + correct."""
    sym = (symbol or "").strip().upper()
    try:
        from .chart import data as cdata
        if not cdata.db_available():
            return {"ok": False, "symbol": sym, "pattern": pattern, "date": date,
                    "note": f"data source unavailable ({cdata.db_path()}); SPEC: cloud feeds wiring."}
        # FAST PATH — precomputed bundle (ms). Guarded so a store miss/error falls through to live.
        try:
            from .chart import screener as scr
            bundle = scr.load_setup_bundle(date, sym, pattern)
            if isinstance(bundle, dict):
                out = dict(bundle)
                out["served"] = "precompute"
                out["serve_note"] = (f"served precomputed bundle (precomputed_at "
                                     f"{bundle.get('precomputed_at')})")
                return out
        except Exception as e:  # noqa: BLE001 — fast path must never sink the live compute
            log.debug("chart_setup fast-path %s/%s miss: %s", sym, pattern, e)
        from .chart import setup as csetup
        res = csetup.build_setup(sym, pattern, date)
        if isinstance(res, dict):
            res["served"] = "live"
            res["serve_note"] = ("live-computed on the request path (no precomputed bundle for this "
                                 "key) — correct but slower; the post-market EOD job precomputes these.")
        return res
    except Exception as e:  # noqa: BLE001 — never 500-crash
        log.warning("chart_setup(%s/%s) failed: %s", sym, pattern, e)
        return {"ok": False, "symbol": sym, "pattern": pattern, "date": date, "error": str(e)}


@router.get("/agents/chart/decision")
def chart_decision(symbol: str = Query(..., description="stock symbol, e.g. TITAN"),
                   date: Optional[str] = Query(None, description="as-of date YYYY-MM-DD"),
                   pattern: Annotated[Optional[str], Query(description="restrict to one pattern_id "
                                      "(default: best across all patterns)")] = None):
    """Most recent occurrence for SYM with signal <= date, run through decide() (honest WATCH at
    current N). Returns decision/reason/basis, the strategy-replay head, the pattern-forward numbers
    (T+1/3/5/10), gates, policy and the occurrence. Point-in-time throughout."""
    sym = (symbol or "").strip().upper()
    try:
        agent = _chart_agent()
        from .chart import data as cdata
        if not cdata.db_available():
            return {"ok": False, "symbol": sym, "date": date, "decision": None,
                    "note": f"data source unavailable ({cdata.db_path()}); SPEC: cloud feeds wiring."}
        occ, _df, k = _recent_occurrence(sym, date, pattern=pattern)
        if occ is None:
            return {"ok": True, "symbol": sym, "date": date, "decision": "WATCH",
                    "reason": f"no clean chart stage for {sym} on/around {date or 'latest'} "
                              f"(no breakout/retest/approach in the recent window).",
                    "occurrence": None, "strategy": None, "pattern_forward": None,
                    "gates": [], "policy": None, "basis": None}
        res = agent.decide(occ)
        return {
            "ok": True,
            "symbol": sym,
            "date": date,
            "decision": res.get("decision"),
            "reason": res.get("reason"),
            "basis": res.get("basis"),
            "spec_note": res.get("spec_note"),
            "strategy": _strategy_head(res.get("strategy")),
            "pattern_forward": _pattern_forward(res.get("evidence")),
            "gates": res.get("gates", []),
            "policy": res.get("policy"),
            "occurrence": res.get("occurrence"),
        }
    except Exception as e:  # noqa: BLE001
        log.warning("chart_decision(%s) failed: %s", sym, e)
        return {"ok": False, "symbol": sym, "date": date, "decision": None, "error": str(e)}


@router.get("/agents/chart/storyline")
def chart_storyline(symbol: str = Query(..., description="stock symbol, e.g. TITAN"),
                    date: Optional[str] = Query(None, description="as-of date YYYY-MM-DD")):
    """A small ordered list of human-readable events building to the decision. Derived from the
    occurrence + evidence + decision (not a new engine). Honest: shows the WATCH and its reason."""
    sym = (symbol or "").strip().upper()
    try:
        agent = _chart_agent()
        from .chart import data as cdata
        if not cdata.db_available():
            return {"ok": False, "symbol": sym, "date": date, "events": [],
                    "note": f"data source unavailable ({cdata.db_path()}); SPEC: cloud feeds wiring."}
        occ, _df, k = _recent_occurrence(sym, date)
        if occ is None:
            return {"ok": True, "symbol": sym, "date": date, "decision": "WATCH", "events": [
                {"kind": "watch", "title": "No active setup",
                 "detail": f"No clean chart stage for {sym} on/around {date or 'latest'}."}]}
        res = agent.decide(occ)
        ctx = occ.get("context", {}) or {}
        stage = occ.get("stage")
        level = occ.get("level")
        touches = len(occ.get("touches") or [])
        volx = ctx.get("volume_x")
        dist = ctx.get("distance_to_level_pct")
        as_of_date = ctx.get("as_of_date") or date
        strat = res.get("strategy") or {}
        ev_sum = (res.get("evidence") or {}).get("summary") or {}

        events = []
        # 1) the level
        events.append({
            "kind": "level", "title": f"Resistance identified at ₹{level}",
            "detail": f"Horizontal flat-top touched {touches}× over the prior window "
                      f"(min-touches gate met)."})
        # 2) the live stage
        if stage == "BREAKOUT":
            events.append({"kind": "breakout", "title": f"Breakout: closed above ₹{level}",
                           "detail": f"First daily close above the level on {volx}× average volume "
                                     f"({dist:+.2f}% past it) on {as_of_date}."})
        elif stage == "RETEST":
            events.append({"kind": "retest", "title": f"Retest held ₹{level}",
                           "detail": f"Price returned to the level and turned up on {volx}× volume "
                                     f"({dist:+.2f}%) on {as_of_date}."})
        elif stage == "APPROACHING":
            events.append({"kind": "approaching", "title": f"Approaching ₹{level}",
                           "detail": f"Price is {dist:+.2f}% from the level ({volx}× volume) on "
                                     f"{as_of_date} — not broken out yet."})
        else:
            events.append({"kind": "stage", "title": f"{stage} at ₹{level}",
                           "detail": f"{dist:+.2f}% from level, {volx}× volume on {as_of_date}."})
        # 3) evidence
        if strat and strat.get("n") is not None:
            events.append({
                "kind": "evidence",
                "title": f"Evidence: {strat.get('n')} resolved precedents",
                "detail": f"Strategy-replay ETV {strat.get('etv')}% (win {strat.get('win')}%, "
                          f"payoff {strat.get('payoff')}, avg-MAE {strat.get('mae')}%) under governed "
                          f"policy {strat.get('version')}. Pattern-forward T+10 up-rate "
                          f"{ev_sum.get('pct_up')}%."})
        else:
            events.append({"kind": "evidence", "title": "Evidence: no resolved precedents yet",
                           "detail": "Not enough history has fully printed (T+10) to score this "
                                     "setup — honest insufficient-evidence state."})
        # 4) the decision
        events.append({
            "kind": "decision", "title": f"Decision: {res.get('decision')}",
            "detail": res.get("reason", ""),
            "basis": res.get("basis"), "spec_note": res.get("spec_note")})

        return {"ok": True, "symbol": sym, "date": date, "decision": res.get("decision"),
                "reason": res.get("reason"), "stage": stage, "level": level, "events": events}
    except Exception as e:  # noqa: BLE001
        log.warning("chart_storyline(%s) failed: %s", sym, e)
        return {"ok": False, "symbol": sym, "date": date, "events": [], "error": str(e)}


# ------------------------------------------------------------- ADMIN: current-day refresh (guarded)
# This is the ONE agent endpoint that triggers a PROD-DATA WRITE (+ a Kite fetch), so it is
# AUTH-GATED: every call must carry header ``X-Agent-Admin-Token`` == env ``AGENT_ADMIN_TOKEN``.
#   * AGENT_ADMIN_TOKEN unset -> 503 (fail-closed; the endpoint is "not configured", like an
#     unarmed middleware — we never run a prod write when no admin secret is configured).
#   * header missing/mismatched -> 401 (constant-time compare via hmac.compare_digest).
ADMIN_TOKEN_HEADER = "X-Agent-Admin-Token"

# In-process job registry. PER-TASK ONLY: on multi-task ECS each task has its OWN dict, so a job_id
# started on task A is unknown to task B. The AUTHORITATIVE cross-task completion signal is the S3
# screen artifact — poll GET /api/agents/chart/scan?date=D until it reports served="precompute".
_REFRESH_JOBS: dict = {}
_REFRESH_LOCK = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_admin(token: Optional[str]) -> None:
    """Fail-closed admin gate. Raises 503 if AGENT_ADMIN_TOKEN is unset, 401 on a missing/wrong
    header. Constant-time compare so the token is not leakable by timing."""
    expected = os.environ.get("AGENT_ADMIN_TOKEN")
    if not expected:
        raise HTTPException(503, "admin refresh not configured (AGENT_ADMIN_TOKEN unset)")
    if not token or not hmac.compare_digest(str(token), str(expected)):
        raise HTTPException(401, "invalid or missing admin token")


def _run_refresh_job(job_id: str, date: Optional[str]) -> None:
    """Background worker: run the EOD refresh+scan+publish for DATE and record the outcome on the job.
    Wrapped end-to-end so a failure records status='error' and never escapes the daemon thread. The
    fetcher is resolved by run_eod from env AGENT_CHART_FETCH_FN (not hardcoded here)."""
    try:
        from .chart import eod  # lazy: import problems here must not affect module import / boot
        res = eod.run_eod(date, fetch=True)
        summary = {"count": res.get("count"), "scanned": res.get("scanned"),
                   "by_pattern": res.get("by_pattern"), "stored": res.get("stored"),
                   "fetched": res.get("fetched")}
        with _REFRESH_LOCK:
            job = _REFRESH_JOBS.get(job_id)
            if job is not None:
                job.update(status="done", finished_at=_now_iso(), result=summary)
    except Exception as e:  # noqa: BLE001 — never let a background failure escape the thread
        log.warning("chart refresh job %s failed: %s", job_id, e)
        with _REFRESH_LOCK:
            job = _REFRESH_JOBS.get(job_id)
            if job is not None:
                job.update(status="error", finished_at=_now_iso(), error=f"{type(e).__name__}: {e}")


@router.post("/agents/chart/refresh")
def chart_refresh(response: Response,
                  date: Annotated[Optional[str], Query(description="as-of date YYYY-MM-DD "
                                                       "(default: today)")] = None,
                  x_agent_admin_token: Annotated[Optional[str], Header()] = None):
    """ADMIN: kick off a current-day Chart Agent refresh (Kite fetch + full-universe scan + publish).

    Auth: header ``X-Agent-Admin-Token`` == env ``AGENT_ADMIN_TOKEN`` (503 if unset, 401 if wrong).
    Starts ``agents.chart.eod.run_eod(date, fetch=True)`` in a DAEMON thread and returns 202 with a
    job_id immediately. SINGLE-FLIGHT: if a refresh is already running, returns 409 busy with the
    running job_id (a second refresh is never started). Poll ``/agents/chart/refresh/status`` for the
    per-task job record; the cross-task truth is the S3 screen artifact (see the status docstring)."""
    _require_admin(x_agent_admin_token)
    try:
        with _REFRESH_LOCK:
            running = next((jid for jid, j in _REFRESH_JOBS.items()
                            if j.get("status") == "running"), None)
            if running is not None:
                response.status_code = 409
                return {"ok": False, "status": "busy", "job_id": running}
            job_id = uuid.uuid4().hex
            started = _now_iso()
            _REFRESH_JOBS[job_id] = {"status": "running", "date": date, "started_at": started,
                                     "finished_at": None, "result": None, "error": None}
        threading.Thread(target=_run_refresh_job, args=(job_id, date), daemon=True).start()
        response.status_code = 202
        return {"ok": True, "job_id": job_id, "status": "running", "date": date,
                "started_at": started}
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001 — never leak a 500; the intentional codes are raised above
        log.warning("chart_refresh failed to start: %s", e)
        response.status_code = 200
        return {"ok": False, "error": str(e)}


@router.get("/agents/chart/refresh/status")
def chart_refresh_status(response: Response,
                         job_id: Annotated[str, Query(description="job_id from POST refresh")],
                         x_agent_admin_token: Annotated[Optional[str], Header()] = None):
    """ADMIN: status of a refresh job started on THIS task. Returns the job record, or 404 if unknown.

    IMPORTANT — the job registry is PER-TASK (in-process). On multi-task ECS a job_id is only known to
    the task that started it; another task returns 404 for it. The AUTHORITATIVE, cross-task
    completion signal is the S3 screen artifact: poll GET /api/agents/chart/scan?date=D until it
    reports served="precompute"."""
    _require_admin(x_agent_admin_token)
    try:
        with _REFRESH_LOCK:
            job = _REFRESH_JOBS.get(job_id)
            if job is None:
                raise HTTPException(404, f"no refresh job {job_id!r} on this task")
            return {"ok": True, "job_id": job_id, **job}
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        log.warning("chart_refresh_status(%s) failed: %s", job_id, e)
        response.status_code = 200
        return {"ok": False, "job_id": job_id, "error": str(e)}
