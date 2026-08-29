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
import logging
from typing import Annotated, Optional

from fastapi import APIRouter, HTTPException, Query
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


def _recent_occurrence(sym: str, date: Optional[str], lookback: int = 90):
    """Most recent LIVE-stage occurrence for SYM whose signal bar is <= date (point-in-time).
    Walks back from the as-of bar calling the detector until a clean stage appears; returns a
    (occurrence_dict, df, k_asof) triple, or (None, df, k) if there is no stage in the window."""
    from .chart import data as cdata
    from .chart.patterns import registry as patterns
    from .chart.agent import _as_of_idx  # reuse the exact point-in-time bar resolver

    det = patterns.get("horizontal_trendline")
    if det is None:
        raise RuntimeError("horizontal_trendline detector not available")
    df = cdata.load_daily(sym)
    df.attrs["symbol"] = sym
    k = _as_of_idx(df, date)
    if k < 0:
        return None, df, k
    lo = max(0, k - lookback)
    for j in range(k, lo - 1, -1):
        try:
            occ = det.detect(df, as_of_idx=j)
        except Exception as e:  # noqa: BLE001
            log.debug("detect failed at %s[%d]: %s", sym, j, e)
            continue
        if occ:
            return occ[0].to_dict(), df, k
    return None, df, k


# --------------------------------------------------------------------------- chart endpoints
def _setup_to_row(s: dict) -> dict:
    """Normalise a screener setup (or a precomputed artifact row) to the endpoint's occurrence shape.
    Accepts both the screener's ``symbol`` key and the legacy ``stock`` key."""
    return {
        "stock": s.get("symbol") or s.get("stock"),
        "pattern": s.get("pattern"),
        "stage": s.get("stage"),
        "level": s.get("level"),
        "distance_pct": s.get("distance_pct") if "distance_pct" in s else (s.get("context") or {}).get("distance_to_level_pct"),
        "volume_x": s.get("volume_x") if "volume_x" in s else (s.get("context") or {}).get("volume_x"),
        "direction": s.get("direction", "long"),
        "touches": s.get("touches") if isinstance(s.get("touches"), int) else len(s.get("touches") or []),
        "as_of_date": s.get("as_of_date") or (s.get("context") or {}).get("as_of_date"),
    }


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
                "count": len(rows), "occurrences": out_rows}
    except Exception as e:  # noqa: BLE001 — never 500-crash
        log.warning("chart_scan failed: %s", e)
        return {"ok": False, "date": date, "occurrences": [], "count": 0, "error": str(e)}


@router.get("/agents/chart/decision")
def chart_decision(symbol: str = Query(..., description="stock symbol, e.g. TITAN"),
                   date: Optional[str] = Query(None, description="as-of date YYYY-MM-DD")):
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
        occ, _df, k = _recent_occurrence(sym, date)
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
