"""
Chart Agent · PER-SETUP DETAIL builder (column-3 of the agent UX).

Given (symbol, pattern, date) this runs THAT pattern's detector point-in-time and returns everything
the drill-down needs, ALL from the detector's REAL outputs — never an approximation:

  geometry     REAL anchor points (dates + prices) to DRAW the pattern on the candles. Horizontal =
               a flat level line + touches; the 7 sloped = upper+lower lines from the _geometry_dict
               anchors; cup&handle = rim line + base points + handle. Fields the pattern lacks are null.
  quality      quality.compute_quality on measurements pulled from the occurrence/geometry (0-100).
  evidence     the ported pattern-forward family (T+1..T+10 win/ETV/MFE/MAE) on RESOLVED precedents.
  paths        the per-cohort mean forward trajectory (winners vs losers) from the SAME precedents.
  decision     evidence.decide() — the §9 gate stack (verdict + per-gate pass/fail + strategy head).
  watch_plan   confirmation / warning / invalidation levels, direction-aware, from level + policy.

POINT-IN-TIME IS LAW: only bars <= date are ever read. The detector slices to as_of internally; the
evidence/paths use the as-of slice and resolved-only precedents (entry+9 <= k). Fully guarded — the
builder never raises; on any problem it returns an honest ok:false / null-fields payload.
"""
from __future__ import annotations
from typing import Optional
import logging
import numpy as np

from . import data
from . import evidence as ev
from . import strategy as strat
from . import quality as q
from .patterns import registry as patterns
from .agent import _as_of_idx

log = logging.getLogger("agents.chart.setup")

# Watch-plan thresholds (frozen, governed). invalidation buffer MATCHES strategy.py's structural stop
# (StrategyPolicy.buffer = 0.002 = detector PARAMS.buffer): close beyond level*(1∓buffer) breaks the
# thesis. The warning band = detector retest_tol (0.012): inside it the reclaim is still "in play";
# past it toward invalidation the setup is deteriorating.
INVAL_BUFFER = strat.DEFAULT_POLICY.buffer     # 0.002
WARN_BUFFER = 0.012                            # = horizontal_trendline.PARAMS["retest_tol"]


# --------------------------------------------------------------------------- geometry helpers
def _pt(dates, series, idx):
    """A {date, price} point at bar ``idx`` from a price series, or None if idx is out of range."""
    try:
        i = int(round(idx))
    except (TypeError, ValueError):
        return None
    if i < 0 or i >= len(series):
        return None
    d = dates[i]
    return {"date": str(d.date()) if hasattr(d, "date") else str(d),
            "price": round(float(series[i]), 4)}


def _line_from_anchors(anchors, prices, k):
    """Reconstruct the deterministic two-anchor line (val(x)=m*(x-a)+prices[a]) from anchor indices.
    Returns (a, b, val_fn) or (None, None, None)."""
    if not anchors or anchors[0] is None or anchors[1] is None:
        return None, None, None
    a, b = int(anchors[0]), int(anchors[1])
    if b == a:
        return None, None, None
    pa, pb = float(prices[a]), float(prices[b])
    m = (pb - pa) / (b - a)
    return a, b, (lambda x: m * (x - a) + pa)


def _sloped_geometry(occ, geom, dates, h, l, k):
    """Draw-geometry for a sloped pattern from the REAL _geometry_dict anchors. Upper line from the
    swing-HIGH anchors (high prices), lower from the swing-LOW anchors (low prices); touches + breakout
    on the BROKEN line; level_line = the broken line extended to k; apex the projected intersection."""
    anchors = (geom or {}).get("anchors") or {}
    ua, ub, uval = _line_from_anchors(anchors.get("upper"), h, k)
    la, lb, lval = _line_from_anchors(anchors.get("lower"), l, k)

    upper = None
    if uval is not None:
        upper = {"a": _pt(dates, h, ua), "b": _pt(dates, h, ub),
                 "extend_to": {**(_pt(dates, h, k) or {}), "price": round(float(uval(k)), 4)}}
    lower = None
    if lval is not None:
        lower = {"a": _pt(dates, l, la), "b": _pt(dates, l, lb),
                 "extend_to": {**(_pt(dates, l, k) or {}), "price": round(float(lval(k)), 4)}}

    direction = occ.get("direction", "long")
    broke_upper = direction == "long"
    brk_series = h if broke_upper else l
    brk_val = uval if broke_upper else lval
    touches = [_pt(dates, brk_series, t) for t in (occ.get("touches") or [])]
    touches = [t for t in touches if t]

    level = occ.get("level")
    level_line = None
    if brk_val is not None:
        ax = (ua if broke_upper else la)
        level_line = {"from": _pt(dates, brk_series, ax),
                      "to": {**(_pt(dates, brk_series, k) or {}), "price": round(float(level), 4)}}

    apex = None
    apex_idx = (geom or {}).get("apex_idx")
    if apex_idx is not None and uval is not None:
        d = None
        if apex_idx <= k:
            pt = _pt(dates, h, apex_idx)
            d = pt["date"] if pt else None
        # The apex price is the (linear) intersection value. When the apex is projected FAR beyond the
        # base (a very flat convergence) the linear extension is not meaningful to draw — report the
        # index + projected flag but null the price rather than surfacing a spurious/negative value.
        base_start = min(int(anchors["upper"][0]), int(anchors["lower"][0])) if (
            anchors.get("upper") and anchors.get("lower")) else k
        far = (apex_idx - k) > 2 * max(1, (k - base_start))
        price = round(float(uval(apex_idx)), 4)
        if far or price <= 0:
            price = None
        apex = {"index": float(apex_idx), "date": d, "price": price,
                "projected": bool(apex_idx > k),
                "note": ("projected far beyond the base — price not drawable" if price is None else None)}

    return {"upper": upper, "lower": lower, "touches": touches,
            "breakout": {**(_pt(dates, brk_series, k) or {}), "price": round(float(level), 4)} if level is not None else None,
            "level_line": level_line, "apex": apex, "cup": None, "handle": None}


def _horizontal_geometry(occ, dates, h, k):
    """Draw-geometry for the horizontal trendline: a flat level line + the touching pivot highs."""
    level = occ.get("level")
    touches = [_pt(dates, h, t) for t in (occ.get("touches") or [])]
    touches = [t for t in touches if t]
    first = min((int(t) for t in (occ.get("touches") or [])), default=k)
    level_line = None
    if level is not None:
        fp = _pt(dates, h, first)
        tp = _pt(dates, h, k)
        level_line = {"from": {"date": fp["date"] if fp else None, "price": round(float(level), 4)},
                      "to": {"date": tp["date"] if tp else None, "price": round(float(level), 4)}}
    return {"upper": None, "lower": None, "touches": touches,
            "breakout": {**(_pt(dates, h, k) or {}), "price": round(float(level), 4)} if level is not None else None,
            "level_line": level_line, "apex": None, "cup": None, "handle": None}


def _cup_geometry(occ, geom, dates, h, c, k):
    """Draw-geometry for cup & handle: the flat rim line, the two rims + base bottom, and the handle."""
    rim = occ.get("level")
    Lr = (geom or {}).get("left_rim")
    Rr = (geom or {}).get("right_rim")
    bottom_idx = (geom or {}).get("cup_bottom_idx")
    left = _pt(dates, h, Lr) if Lr is not None else None
    right = _pt(dates, h, Rr) if Rr is not None else None
    bottom = _pt(dates, c, bottom_idx) if bottom_idx is not None else None
    touches = [t for t in (left, right) if t]
    level_line = None
    if rim is not None and left is not None:
        tp = _pt(dates, h, k)
        level_line = {"from": {"date": left["date"], "price": round(float(rim), 4)},
                      "to": {"date": tp["date"] if tp else None, "price": round(float(rim), 4)}}
    handle = {"bars": (geom or {}).get("handle_bars"),
              "depth_pct": (geom or {}).get("handle_depth_pct")}
    cup = {"left_rim": left, "bottom": bottom, "right_rim": right,
           "depth_pct": (geom or {}).get("cup_depth_pct"), "r2": (geom or {}).get("r2")}
    return {"upper": None, "lower": None, "touches": touches,
            "breakout": {**(_pt(dates, h, k) or {}), "price": round(float(rim), 4)} if rim is not None else None,
            "level_line": level_line, "apex": None, "cup": cup, "handle": handle}


def _contraction(geom, h, l, k):
    """Apex-pattern tightness: 1 - width(k)/width(base_start), where width(x)=upper(x)-lower(x). A
    tighter coil (lines converged) -> closer to 1. None when either line is missing."""
    anchors = (geom or {}).get("anchors") or {}
    _, _, uval = _line_from_anchors(anchors.get("upper"), h, k)
    _, _, lval = _line_from_anchors(anchors.get("lower"), l, k)
    if uval is None or lval is None:
        return None
    base_start = min(int(anchors["upper"][0]), int(anchors["lower"][0]))
    w0 = uval(base_start) - lval(base_start)
    wk = uval(k) - lval(k)
    if w0 <= 0:
        return None
    return float(max(0.0, min(1.0, 1.0 - wk / w0)))


def _flatness(occ, h, k):
    """Horizontal flat-cleanliness in [0,1]: 1 - dispersion(touch highs)/tol. Tight touches -> ~1."""
    ti = [int(t) for t in (occ.get("touches") or []) if 0 <= int(t) < len(h)]
    level = occ.get("level")
    if len(ti) < 2 or not level:
        return None
    prices = np.array([float(h[t]) for t in ti])
    disp = float(prices.std()) / float(level)
    return float(max(0.0, 1.0 - min(disp / q.FLAT_TOL, 1.0)))


# --------------------------------------------------------------------------- quality wiring
def _quality_for(occ, geom, pattern_id, h, l, k):
    ctx = occ.get("context") or {}
    r2 = None
    if pattern_id == "cup_and_handle":
        r2 = (geom or {}).get("r2")
    elif geom and "r2s" in geom:
        vals = [v for v in ((geom["r2s"] or {}).get("upper"), (geom["r2s"] or {}).get("lower")) if v is not None]
        r2 = float(np.mean(vals)) if vals else None
    measurements = {
        "r2": r2,
        "flatness": _flatness(occ, h, k) if pattern_id == "horizontal_trendline" else None,
        "n_touches": len(occ.get("touches") or []),
        "distance_pct": ctx.get("distance_to_level_pct"),
        "volume_x": ctx.get("volume_x"),
        "contraction": _contraction(geom, h, l, k) if pattern_id in q.APEX_PATTERNS else None,
    }
    return q.compute_quality(pattern_id, measurements), measurements


# --------------------------------------------------------------------------- paths (winners/losers)
def win_loss_paths(evidence: Optional[dict], small_n: int = 20) -> dict:
    """Split the REAL per-precedent forward paths (evidence['paths'], each [0,t1..t10]) into winners
    (T+10 > 0) and losers (<=0) and return the MEAN trajectory per cohort. Small-N is flagged honestly.
    Percent units (paths are cumulative net-of-cost returns from the pattern-forward family)."""
    if not evidence or not evidence.get("paths"):
        return {"winners": None, "losers": None, "n_win": 0, "n_loss": 0, "n_total": 0,
                "small_n": True, "note": "no resolved precedents — no forward paths to average."}
    P = np.array(evidence["paths"], dtype=float)      # (n, 11): col 0 == 0.0, cols 1..10 == T+1..T+10
    ref = P[:, -1]
    win = P[ref > 0]
    loss = P[ref <= 0]
    def mean_traj(M):
        if M.shape[0] == 0:
            return None
        return [round(float(x) * 100, 2) for x in M[:, 1:].mean(axis=0)]   # T+1..T+10 in percent
    n_total = int(P.shape[0])
    return {"winners": mean_traj(win), "losers": mean_traj(loss),
            "n_win": int(win.shape[0]), "n_loss": int(loss.shape[0]), "n_total": n_total,
            "small_n": bool(n_total < small_n),
            "note": (f"small sample (n={n_total} < {small_n}) — cohort means are indicative only."
                     if n_total < small_n else f"n={n_total} resolved precedents.")}


# --------------------------------------------------------------------------- watch plan
def watch_plan(level: Optional[float], direction: str = "long") -> dict:
    """Confirmation / warning / invalidation levels from the setup's level + governed buffers.
    LONG: confirm = level (close above); warning = level*(1-warn) (reclaim slipping); invalidation =
    level*(1-inval) (thesis broken). SHORT mirrors above the level."""
    if level is None:
        return {"confirmation": None, "warning": None, "invalidation": None, "direction": direction,
                "note": "no level — watch plan unavailable."}
    lvl = float(level)
    if direction == "short":
        confirmation = round(lvl, 4)
        warning = round(lvl * (1 + WARN_BUFFER), 4)
        invalidation = round(lvl * (1 + INVAL_BUFFER), 4)
        note = (f"short: confirm on a close BELOW {confirmation}; warning if it climbs back to "
                f"{warning} (+{WARN_BUFFER*100:.1f}%); invalidation (thesis broken) on a close above "
                f"{invalidation} (+{INVAL_BUFFER*100:.1f}%, matches the strategy-replay structural stop).")
    else:
        confirmation = round(lvl, 4)
        warning = round(lvl * (1 - WARN_BUFFER), 4)
        invalidation = round(lvl * (1 - INVAL_BUFFER), 4)
        note = (f"long: confirm on a close ABOVE {confirmation}; warning if it slips to {warning} "
                f"(-{WARN_BUFFER*100:.1f}%); invalidation (thesis broken) on a close below "
                f"{invalidation} (-{INVAL_BUFFER*100:.1f}%, matches the strategy-replay structural stop).")
    return {"confirmation": confirmation, "warning": warning, "invalidation": invalidation,
            "direction": direction, "note": note}


# --------------------------------------------------------------------------- tier (storyline feed)
def tier_of(stage: Optional[str], decision: Optional[str], evidence: Optional[dict],
            edge_positive: Optional[bool], n: int) -> str:
    """Derive the middle-column feed tier from stage + decision + evidence (REAL, no fabrication).
      qualified : decision == TRADE.
      strong    : a breakout/retest with >= ~10 resolved precedents AND positive expectancy.
      watch     : approaching / forming (no break yet), or breakout with thin/immature evidence.
      weak      : evidence weak / negative expectancy (decision NO_TRADE)."""
    if decision == "TRADE":
        return "qualified"
    if decision == "NO_TRADE":
        return "weak"
    broke = stage in ("BREAKOUT", "RETEST")
    if broke and n >= 10 and edge_positive:
        return "strong"
    return "watch"


# --------------------------------------------------------------------------- orchestrator
def _decode_geometry(pattern_id, occ, geom, dates, h, l, c, k):
    if pattern_id == "horizontal_trendline":
        return _horizontal_geometry(occ, dates, h, k)
    if pattern_id == "cup_and_handle":
        return _cup_geometry(occ, geom, dates, h, c, k)
    return _sloped_geometry(occ, geom, dates, h, l, k)


def build_setup(symbol: str, pattern_id: str, date: Optional[str] = None) -> dict:
    """Full per-setup detail for (symbol, pattern, date). Guarded, point-in-time. Returns a dict with
    ok / stage / direction / level / geometry / quality / evidence / paths / decision / watch_plan."""
    sym = (symbol or "").strip().upper()
    base = {"ok": False, "symbol": sym, "pattern": pattern_id, "date": date}
    det = patterns.get(pattern_id)
    if det is None:
        return {**base, "note": f"unknown pattern '{pattern_id}'."}
    try:
        df = data.load_daily(sym)
    except Exception as e:  # noqa: BLE001
        return {**base, "note": f"data unavailable for {sym} ({e})."}
    df.attrs["symbol"] = sym
    k = _as_of_idx(df, date)
    if k < 0 or k >= len(df):
        return {**base, "ok": True, "stage": None, "note": f"no bar on/before {date} for {sym}.",
                "geometry": None, "quality": None, "evidence": None, "paths": None,
                "decision": None, "watch_plan": None}

    # 1) run THIS pattern's detector point-in-time (resolves pattern_id -> detector; NOT hardcoded).
    try:
        occs = det.detect(df, as_of_idx=k)
    except Exception as e:  # noqa: BLE001
        return {**base, "ok": True, "stage": None, "note": f"detector error: {e}",
                "geometry": None, "quality": None, "evidence": None, "paths": None,
                "decision": None, "watch_plan": None}
    as_of_date = str(df.index[k].date())
    if not occs:
        return {**base, "ok": True, "stage": None, "as_of_date": as_of_date,
                "note": f"no live {pattern_id} stage for {sym} on {as_of_date}.",
                "geometry": None, "quality": None, "evidence": None, "paths": None,
                "decision": None, "watch_plan": None}
    occ = occs[0].to_dict()
    geom = occ.get("geometry") or {}
    direction = occ.get("direction", "long") or "long"
    level = occ.get("level")

    dates = df.index
    h, l, c = df["high"].values, df["low"].values, df["close"].values

    # 2) geometry for DRAWING (real anchors).
    try:
        geometry = _decode_geometry(pattern_id, occ, geom, dates, h, l, c, k)
    except Exception as e:  # noqa: BLE001
        log.warning("geometry build failed %s/%s: %s", sym, pattern_id, e)
        geometry = None

    # 3) quality (real measurements).
    try:
        quality, _meas = _quality_for(occ, geom, pattern_id, h, l, k)
    except Exception as e:  # noqa: BLE001
        quality = {"score": None, "note": f"quality unavailable: {e}"}

    # 4) evidence + decision (point-in-time; resolved-only precedents).
    policy = (strat.DEFAULT_POLICY if pattern_id == "horizontal_trendline"
              else strat.TRENDLINE_SHORT_POLICY if direction == "short"
              else strat.TRENDLINE_POLICY)
    evidence = decision = None
    paths = None
    try:
        events = det.historical_events(df, as_of_idx=k, direction=direction)
        df_asof = df.iloc[:k + 1]
        evidence = ev.pattern_evidence(df_asof, events, max_h=10, direction=direction)
        decision = ev.decide(df_asof, events, evidence, as_of_idx=k, policy=policy, direction=direction)
        paths = win_loss_paths(evidence)
    except Exception as e:  # noqa: BLE001
        log.warning("evidence/decision failed %s/%s: %s", sym, pattern_id, e)

    plan = watch_plan(level, direction)

    # REAL sector from instrument_labels (data.sector_map). None when no sector source exists (e.g. a
    # Parquet-only cloud source) — honest "—" in the UI, never fabricated. No real market-cap column
    # exists in kanida.db, so marketCapCr is intentionally omitted (not fabricated).
    try:
        sector = data.sector_map().get(sym)
    except Exception:  # noqa: BLE001
        sector = None

    return {
        "ok": True, "symbol": sym, "pattern": pattern_id, "date": date, "as_of_date": as_of_date,
        "stage": occ.get("stage"), "direction": direction, "level": level,
        "sector": sector,
        "context": occ.get("context"),
        "geometry": geometry,
        "quality": quality,
        "evidence": evidence,
        "paths": paths,
        "decision": _decision_head(decision),
        "watch_plan": plan,
    }


# --------------------------------------------------------------------------- bars (candles)
def build_bars(symbol: str, date: Optional[str] = None, lookback: int = 250) -> list:
    """Point-in-time daily candles [{date,o,h,l,c,v}] for DRAWING — only bars dated <= ``date``, the
    last ``lookback`` of them. Shaped BYTE-IDENTICALLY to router.chart_bars so a precomputed bundle and
    the live /bars endpoint agree exactly. Guarded: returns [] on any problem (never raises)."""
    sym = (symbol or "").strip().upper()
    try:
        df = data.load_daily(sym)
    except Exception as e:  # noqa: BLE001
        log.debug("build_bars(%s) load failed: %s", sym, e)
        return []
    k = _as_of_idx(df, date)
    if k < 0 or k >= len(df):
        return []
    lo = max(0, k - lookback + 1)
    w = df.iloc[lo:k + 1]
    return [{"date": str(idx.date()) if hasattr(idx, "date") else str(idx),
             "o": round(float(r.open), 4), "h": round(float(r.high), 4),
             "l": round(float(r.low), 4), "c": round(float(r.close), 4),
             "v": int(r.volume) if r.volume == r.volume else None}
            for idx, r in w.iterrows()]


# --------------------------------------------------------------------------- scan enrichment
MEANINGFUL_N = ev.N_MIN     # 20 — same bar as the §9 G1 sample gate ("statistically meaningful")


def _hook(pattern_id: str, stage: Optional[str], direction: str, n: int,
          win_t5: Optional[float]) -> str:
    """A one-line, REAL agent hook for the middle-column feed. Honest at small N."""
    name = pattern_id.replace("_", " ").title()
    verb = {"BREAKOUT": "breakout", "RETEST": "retest", "APPROACHING": "approaching",
            "FAILED": "failed break"}.get(stage or "", (stage or "").lower())
    side = "" if direction == "long" else " (short)"
    head = f"{name} {verb}{side}"
    if n >= MEANINGFUL_N and win_t5 is not None:
        return f"{head}; T+5 historical win {win_t5:.0f}% (n={n})"
    if n > 0 and win_t5 is not None:
        return f"{head}; T+5 win {win_t5:.0f}% but insufficient precedents (n={n})"
    return f"{head}; insufficient precedents (n={n})"


def enrich(symbol: str, pattern_id: str, date: Optional[str], occ_dict: Optional[dict] = None) -> dict:
    """Lightweight REAL summary for the storyline feed: {tier, quality_score, evidence_summary:
    {n, win_t5, etv_t5}, hook}. Point-in-time, guarded. Reuses the full-history precedents so n is
    the same evidence the drill-down shows. ``occ_dict`` (from the scan) is used when supplied so the
    detector is not re-run."""
    out = {"tier": "watch", "quality_score": None,
           "evidence_summary": {"n": 0, "win_t5": None, "etv_t5": None}, "hook": None}
    det = patterns.get(pattern_id)
    if det is None:
        return out
    try:
        df = data.load_daily(symbol)
        df.attrs["symbol"] = symbol
        k = _as_of_idx(df, date)
        if k < 0:
            return out
        if occ_dict is None:
            occs = det.detect(df, as_of_idx=k)
            if not occs:
                return out
            occ_dict = occs[0].to_dict()
        geom = occ_dict.get("geometry") or {}
        direction = occ_dict.get("direction", "long") or "long"
        stage = occ_dict.get("stage")
        h, l = df["high"].values, df["low"].values
        try:
            quality, _ = _quality_for(occ_dict, geom, pattern_id, h, l, k)
            out["quality_score"] = quality.get("score")
        except Exception:  # noqa: BLE001
            pass
        policy = (strat.DEFAULT_POLICY if pattern_id == "horizontal_trendline"
                  else strat.TRENDLINE_SHORT_POLICY if direction == "short"
                  else strat.TRENDLINE_POLICY)
        events = det.historical_events(df, as_of_idx=k, direction=direction)
        df_asof = df.iloc[:k + 1]
        evidence = ev.pattern_evidence(df_asof, events, max_h=10, direction=direction)
        n = 0
        win_t5 = etv_t5 = None
        if evidence:
            n = (evidence.get("summary") or {}).get("n", 0)
            hz = (evidence.get("horizons") or {}).get(5) or (evidence.get("horizons") or {}).get("5")
            if hz:
                win_t5, etv_t5 = hz.get("win"), hz.get("mean")
        out["evidence_summary"] = {"n": n, "win_t5": win_t5, "etv_t5": etv_t5}
        out["hook"] = _hook(pattern_id, stage, direction, n, win_t5)

        # Tier — SHORT-CIRCUIT for speed (enrichment runs on hundreds of setups in build_screen). The
        # expensive part of ev.decide() is strategy_baseline_etv (a full-history replay). Below N_MIN
        # the §9 G1 gate fails FIRST -> decision is WATCH regardless of the baseline, so we skip decide
        # and derive the tier cheaply. Only at n >= N_MIN (a TRADE is even possible) do we run the full
        # gate stack, so ``qualified`` (== a real TRADE verdict) stays exact.
        if n < MEANINGFUL_N:
            strat_ev = strat.strategy_evidence(df_asof, events, policy, as_of_idx=k)
            etv = (strat_ev or {}).get("etv")
            out["tier"] = tier_of(stage, "WATCH", evidence, (etv is not None and etv > 0), n)
        else:
            decision = ev.decide(df_asof, events, evidence, as_of_idx=k, policy=policy,
                                 direction=direction)
            edge = decision.get("edge")
            out["tier"] = tier_of(stage, decision.get("decision"), evidence,
                                  (edge is not None and edge > 0), n)
    except Exception as e:  # noqa: BLE001 — enrichment must never sink the scan
        log.debug("enrich(%s/%s) failed: %s", symbol, pattern_id, e)
        out["hook"] = _hook(pattern_id, (occ_dict or {}).get("stage"),
                            (occ_dict or {}).get("direction", "long") or "long", 0, None)
    return out


def _decision_head(decision: Optional[dict]) -> Optional[dict]:
    """Surface decide() cleanly: verdict + reason + basis + per-gate pass/fail + strategy head."""
    if not decision:
        return {"decision": "WATCH", "reason": "no resolved precedents yet — insufficient evidence.",
                "gates": [], "basis": "strategy_replay", "strategy": None}
    strat_ = decision.get("strategy") or {}
    return {
        "decision": decision.get("decision"),
        "reason": decision.get("reason"),
        "basis": decision.get("basis"),
        "spec_note": decision.get("spec_note"),
        "etv": decision.get("etv"),
        "edge": decision.get("edge"),
        "gates": decision.get("gates", []),
        "policy": decision.get("policy"),
        "strategy": {k: strat_.get(k) for k in
                     ("version", "n", "etv", "win", "payoff", "avg_win", "avg_loss",
                      "mae", "ci_low", "exits", "avg_holding")} if strat_ else None,
    }
