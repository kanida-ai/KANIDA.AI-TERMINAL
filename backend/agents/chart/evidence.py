"""
Chart Agent · Evidence + Decision gates.

PART A — Pattern-Forward evidence (v3 §8.1)  [BUILT]
    ``pattern_evidence`` is PORTED VERBATIM from R&D chart_agent.pattern_evidence: forward
    cumulative-return PATHS T+0..T+max_h for every occurrence, per-horizon win%/mean/median/
    quartiles, MFE/MAE, up/down split, target hit-rates. ``_baseline_edges`` adds the stock's own
    baseline (enter-every-day) ETV per horizon so we can report edge-vs-baseline (also in R&D).

PART B — Decision gate stack (v3 §9)  [BUILT on pattern-forward; reads SPEC input]
    ⚠️ HONESTY: v3 §9 says the gates must read the **STRATEGY-REPLAY ETV** (§8.2) — the stats of the
    exact stop/target/trail policy the agent will trade. That replay engine is **[SPEC — NOT BUILT]**.
    Until it exists, the gate reads the **PATTERN-FORWARD ETV** (§8.1) as an honest stand-in and
    stamps ``basis="pattern_forward"`` + a SPEC note on every decision. We do NOT fabricate strategy
    statistics. Gates that need populations we don't have (G4 nested coherence) and the strategy
    replay (G6 strategy-recency) are reported as ``skipped`` with the reason, never silently passed.
"""
from __future__ import annotations
from typing import Optional
import numpy as np

# Cost + horizon conventions — copied from R&D chart_agent (v3 §3/§8.1).
COST = 0.003                      # 30 bps round-trip
HORIZONS = [1, 3, 5]

# Governed decision constants (v3 §9). Pre-declared per pattern; NOT fit on the deciding data.
DECISION_HORIZON = 3              # headline horizon for the horizontal detector (R&D used T+3)
N_MIN = 20                        # G1 minimum precedents
MAE_CAP = 0.08                    # G5 tail: avg MAE must be >= -8%
PAYOFF_MIN = 1.0                  # G5 payoff floor (avg_win / |avg_loss|)


# ------------------------------------------------------- PART A · pattern-forward evidence (ported)
def pattern_evidence(df, events, max_h: int = 10) -> Optional[dict]:
    """PORTED from chart_agent.pattern_evidence. Forward paths + per-horizon stats for every
    occurrence. ``events`` need only expose ``.entry_idx``. Point-in-time is the caller's job
    (pass resolved events / an as-of-sliced df)."""
    o, hi, lo, c = (df[k].values for k in ["open", "high", "low", "close"])
    n = len(c)
    paths, mfe, mae = [], {h: [] for h in range(1, max_h + 1)}, {h: [] for h in range(1, max_h + 1)}
    for e in events:
        s = e.entry_idx
        if s + max_h - 1 >= n or o[s] <= 0:
            continue
        path = [0.0] + [c[s + d - 1] / o[s] - 1 - COST for d in range(1, max_h + 1)]
        paths.append(path)
        for h in range(1, max_h + 1):
            mfe[h].append(hi[s:s + h].max() / o[s] - 1)
            mae[h].append(lo[s:s + h].min() / o[s] - 1)
    if not paths:
        return None
    P = np.array(paths)
    horizons = {}
    for h in range(1, max_h + 1):
        col = P[:, h]
        horizons[h] = {"win": round(float((col > 0).mean()) * 100, 1),
                       "mean": round(float(col.mean()) * 100, 2),
                       "median": round(float(np.median(col)) * 100, 2),
                       "p25": round(float(np.percentile(col, 25)) * 100, 2),
                       "p75": round(float(np.percentile(col, 75)) * 100, 2),
                       "mfe": round(float(np.mean(mfe[h])) * 100, 2),
                       "mae": round(float(np.mean(mae[h])) * 100, 2)}
    ref = P[:, max_h]; ups = ref[ref > 0]; downs = ref[ref <= 0]
    mfeR, maeR = np.array(mfe[max_h]), np.array(mae[max_h])
    summary = {"n": len(P), "ref_h": max_h,
               "pct_up": round(float((ref > 0).mean()) * 100, 1),
               "pct_down": round(float((ref <= 0).mean()) * 100, 1),
               "avg_up": round(float(ups.mean()) * 100, 2) if len(ups) else 0.0,
               "avg_down": round(float(downs.mean()) * 100, 2) if len(downs) else 0.0,
               "hit_up2": round(float((mfeR >= 0.02).mean()) * 100, 1),
               "hit_up5": round(float((mfeR >= 0.05).mean()) * 100, 1),
               "hit_dn2": round(float((maeR <= -0.02).mean()) * 100, 1),
               "hit_dn5": round(float((maeR <= -0.05).mean()) * 100, 1)}
    return {"paths": P.tolist(), "horizons": horizons, "summary": summary, "ref_h": max_h}


def _baseline_edges(df, evidence: dict, as_of_idx: Optional[int] = None) -> dict:
    """The stock's own baseline ETV (enter every day's next open, same cost) per horizon, and the
    pattern's edge over it — mirrors R&D run_experiment. Point-in-time: baseline uses bars <= as_of."""
    o, c = df["open"].values, df["close"].values
    n = len(c) if as_of_idx is None else min(len(c), int(as_of_idx) + 1)
    max_h = evidence["ref_h"]
    edges = {}
    for h in range(1, max_h + 1):
        base = np.array([c[i + h - 1] / o[i] - 1 - COST for i in range(1, n - h + 1) if o[i] > 0], float)
        base_mean = float(np.nanmean(base)) * 100 if base.size else 0.0
        etv = evidence["horizons"][h]["mean"]
        edges[h] = {"baseline_ev": round(base_mean, 3), "edge": round(etv - base_mean, 3)}
    return edges


# --------------------------------------------------------------------- PART B · decision gates (§9)
def _ci_low_mean(paths: list, h: int) -> Optional[float]:
    """Lower 95% CI of the pattern-forward ETV at horizon h (normal approx: mean-1.96*se)."""
    P = np.array(paths)
    if P.shape[0] < 2:
        return None
    col = P[:, h]
    se = float(col.std(ddof=1)) / np.sqrt(len(col))
    return (float(col.mean()) - 1.96 * se) * 100     # percent


def _payoff_at(paths: list, h: int):
    """avg win / avg loss / payoff at horizon h — keeps G5's tail on the SAME (decision) horizon
    as the edge/MAE, instead of mixing a T+10 payoff/up-down split with a T+3 edge (auditor #3b)."""
    P = np.array(paths)
    if P.shape[0] == 0:
        return 0.0, 0.0, None
    col = P[:, h]
    ups, downs = col[col > 0], col[col <= 0]
    au = round(float(ups.mean()) * 100, 2) if len(ups) else 0.0
    ad = round(float(downs.mean()) * 100, 2) if len(downs) else 0.0
    po = round(au / abs(ad), 2) if ad < 0 else None
    return au, ad, po


def decide(df, events, evidence: Optional[dict], as_of_idx: Optional[int] = None) -> dict:
    """Run the v3 §9 gate stack and return {decision, reason, gates, basis, evidence_ref_horizon}.

    HONEST BASIS: gates read the PATTERN-FORWARD ETV (§8.1); the strategy-replay ETV (§8.2) is
    [SPEC]. Recorded on the result so no reader mistakes this for the final strategy-gated decision."""
    H = DECISION_HORIZON
    spec_note = ("gate reads pattern-forward ETV (v3 §8.1); strategy-replay ETV (§8.2) is SPEC — "
                 "not yet built, so the final strategy-gated verdict may differ.")
    if not evidence:
        return {"decision": "WATCH", "reason": "no resolved precedents yet — insufficient evidence.",
                "gates": [], "basis": "pattern_forward", "spec_note": spec_note,
                "evidence_ref_horizon": H}

    edges = _baseline_edges(df, evidence, as_of_idx=as_of_idx)
    hz = evidence["horizons"][H]
    n = evidence["summary"]["n"]
    etv = hz["mean"]
    edge = edges[H]["edge"]
    ci_low = _ci_low_mean(evidence["paths"], H)
    avg_up, avg_down, payoff = _payoff_at(evidence["paths"], H)   # G5 on the DECISION horizon (auditor #3b)
    mae = hz["mae"]

    gates: list = []

    def add(name, passed, reason, **extra):
        gates.append({"gate": name, "pass": passed, "reason": reason, **extra})

    # G1 · Sample
    g1 = n >= N_MIN
    add("G1_sample", g1, f"n={n} (>= {N_MIN})" if g1 else f"too few precedents (n={n} < {N_MIN})")
    if not g1:
        return _verdict("WATCH", f"too few precedents (n={n} < {N_MIN}).", gates, H, spec_note, etv, edge)

    # G2 · Edge (ETV>0 AND edge>baseline)
    g2 = (etv > 0) and (edge > 0)
    if etv <= 0:
        add("G2_edge", False, f"no edge after costs (ETV T+{H} {etv:+.2f}%)")
        return _verdict("NO_TRADE", f"no edge after costs (ETV T+{H} {etv:+.2f}%).", gates, H, spec_note, etv, edge)
    if edge <= 0:
        add("G2_edge", False, f"profitable ({etv:+.2f}%) but no edge vs baseline ({edge:+.2f}%)")
        return _verdict("NO_TRADE", f"no edge vs the stock's own baseline (edge {edge:+.2f}%).", gates, H, spec_note, etv, edge)
    add("G2_edge", True, f"ETV {etv:+.2f}% and edge {edge:+.2f}% vs baseline")

    # G3 · Significance (CI low > 0)
    g3 = ci_low is not None and ci_low > 0
    add("G3_significance", g3, (f"CI_low(ETV T+{H}) = {ci_low:+.2f}% (normal-approx SE; bootstrap CI is SPEC §7.3)"
                                if ci_low is not None else "CI unavailable"))
    if not g3:
        return _verdict("WATCH", f"edge inside the noise (95% CI_low {ci_low:+.2f}%).", gates, H, spec_note, etv, edge)

    # G4 · Consistency across nested populations — [SPEC] (look-alike/sector engine not built, §7.2/§7.3)
    add("G4_consistency", None, "skipped — nested-population coherence engine is SPEC (§7.2/§7.3); "
                                 "only the Stock tier exists today.", skipped=True)

    # G5 · Tail (MAE cap AND payoff floor)
    g5 = (mae >= -MAE_CAP * 100) and (payoff is not None and payoff >= PAYOFF_MIN)
    add("G5_tail", g5, f"avg MAE T+{H} {mae:+.2f}% (cap {-MAE_CAP*100:.0f}%), payoff {payoff}")
    if not g5:
        return _verdict("NO_TRADE", f"risk/tail too large (MAE {mae:+.2f}%, payoff {payoff}).", gates, H, spec_note, etv, edge)

    # G6 · Recency (strategy-recency) — [SPEC] needs strategy replay (§8.2); reported, not silently passed
    add("G6_recency", None, "skipped — strategy-recency needs the strategy-replay engine (§8.2, SPEC).",
        skipped=True)

    return _verdict("TRADE", f"positive expectancy (+{etv:.2f}% T+{H}, +{edge:.2f}% edge, "
                             f"CI_low {ci_low:+.2f}%) across {n} precedents.", gates, H, spec_note, etv, edge)


def _verdict(decision, reason, gates, H, spec_note, etv, edge):
    return {"decision": decision, "reason": reason, "gates": gates,
            "basis": "pattern_forward", "spec_note": spec_note,
            "evidence_ref_horizon": H, "etv": etv, "edge": edge}
