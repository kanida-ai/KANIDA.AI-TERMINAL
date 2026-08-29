"""
Chart Agent · Evidence + Decision gates.

PART A — Pattern-Forward evidence (v3 §8.1)  [BUILT]
    ``pattern_evidence`` is PORTED VERBATIM from R&D chart_agent.pattern_evidence: forward
    cumulative-return PATHS T+0..T+max_h for every occurrence, per-horizon win%/mean/median/
    quartiles, MFE/MAE, up/down split, target hit-rates. ``_baseline_edges`` adds the stock's own
    baseline (enter-every-day) ETV per horizon so we can report edge-vs-baseline (also in R&D).

PART B — Decision gate stack (v3 §9)  [BUILT on STRATEGY-REPLAY stats]
    The gates now read the **STRATEGY-REPLAY ETV** (§8.2, strategy.py) — the stats of the exact
    governed stop/target/trail policy the agent will trade — as v3 §9 mandates. ``basis`` is stamped
    ``"strategy_replay"``. The PATTERN-FORWARD family (§8.1) is still computed and returned ALONGSIDE
    for research; it is NEVER overwritten by the strategy exit (two outcome families, §5/§8).
    Still honestly labelled SPEC: G4 nested-population coherence (look-alike/sector engine, §7.2/§7.3)
    is ``skipped`` with reason; CI_low is a normal-approx SE (bootstrap CI is §7.3 SPEC).
"""
from __future__ import annotations
from typing import Optional
import numpy as np

from . import strategy as strat

# Cost + horizon conventions — copied from R&D chart_agent (v3 §3/§8.1).
COST = 0.003                      # 30 bps round-trip
HORIZONS = [1, 3, 5]

# Governed decision constants (v3 §9). Pre-declared per pattern; NOT fit on the deciding data.
DECISION_HORIZON = 3              # headline horizon for the horizontal detector (R&D used T+3)
N_MIN = 20                        # G1 minimum precedents
MAE_CAP = 0.08                    # G5 tail: avg strategy-MAE must be >= -8%
PAYOFF_MIN = 1.0                  # G5 payoff floor (avg_win / |avg_loss|)
MIN_RECENT = 3                    # G6 min recent precedents before decay can be judged
DECAY_FRAC = 0.5                  # G6 decaying if recent Strategy-ETV < DECAY_FRAC * prior


# ------------------------------------------------------- PART A · pattern-forward evidence (ported)
def pattern_evidence(df, events, max_h: int = 10, direction: str = "long") -> Optional[dict]:
    """PORTED from chart_agent.pattern_evidence. Forward paths + per-horizon stats for every
    occurrence. ``events`` need only expose ``.entry_idx``. Point-in-time is the caller's job
    (pass resolved events / an as-of-sliced df). The long path is BYTE-IDENTICAL to the original;
    ``direction='short'`` negates the forward return and swaps MFE/MAE (favourable = price down)."""
    o, hi, lo, c = (df[k].values for k in ["open", "high", "low", "close"])
    n = len(c)
    short = direction == "short"
    paths, mfe, mae = [], {h: [] for h in range(1, max_h + 1)}, {h: [] for h in range(1, max_h + 1)}
    for e in events:
        s = e.entry_idx
        if s + max_h - 1 >= n or o[s] <= 0:
            continue
        if short:
            path = [0.0] + [-(c[s + d - 1] / o[s] - 1) - COST for d in range(1, max_h + 1)]
        else:
            path = [0.0] + [c[s + d - 1] / o[s] - 1 - COST for d in range(1, max_h + 1)]
        paths.append(path)
        for h in range(1, max_h + 1):
            if short:
                mfe[h].append(o[s] / lo[s:s + h].min() - 1)     # favourable = lowest low
                mae[h].append(o[s] / hi[s:s + h].max() - 1)     # adverse = highest high
            else:
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


def _baseline_edges(df, evidence: dict, as_of_idx: Optional[int] = None, direction: str = "long") -> dict:
    """The stock's own baseline ETV (enter every day's next open, same cost) per horizon, and the
    pattern's edge over it — mirrors R&D run_experiment. Point-in-time: baseline uses bars <= as_of.
    ``direction='short'`` negates the baseline forward return (same-direction comparison)."""
    o, c = df["open"].values, df["close"].values
    n = len(c) if as_of_idx is None else min(len(c), int(as_of_idx) + 1)
    max_h = evidence["ref_h"]
    short = direction == "short"
    edges = {}
    for h in range(1, max_h + 1):
        if short:
            base = np.array([-(c[i + h - 1] / o[i] - 1) - COST for i in range(1, n - h + 1) if o[i] > 0], float)
        else:
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


def decide(df, events, evidence: Optional[dict], as_of_idx: Optional[int] = None,
           policy: Optional["strat.StrategyPolicy"] = None, direction: str = "long") -> dict:
    """Run the v3 §9 gate stack on STRATEGY-REPLAY stats (§8.2) and return
    {decision, reason, gates, basis, strategy, policy, ...}.

    The gates read the strategy outcome under the frozen governed policy `S` — the same strategy the
    agent trades. The pattern-forward family (``evidence``) is passed through untouched for research.
    ``spec_note`` records only what remains SPEC (G4 coherence; bootstrap CI)."""
    H = DECISION_HORIZON
    policy = policy or strat.DEFAULT_POLICY
    spec_note = ("G4 nested-population coherence (§7.2/§7.3) and bootstrap CI (§7.3) remain SPEC; "
                 "CI_low is a normal-approx SE. G1-G3,G5,G6 read the strategy-replay stats (§8.2).")

    strategy = strat.strategy_evidence(df, events, policy, as_of_idx=as_of_idx)
    if not strategy:
        return {"decision": "WATCH", "reason": "no resolved precedents yet — insufficient evidence.",
                "gates": [], "basis": "strategy_replay", "spec_note": spec_note,
                "evidence_ref_horizon": H, "strategy": None, "policy": policy.to_dict()}

    n = strategy["n"]
    etv = strategy["etv"]
    ci_low = strategy["ci_low"]
    payoff = strategy["payoff"]
    mae = strategy["mae"]
    base_etv = strat.strategy_baseline_etv(df, policy, as_of_idx=as_of_idx, direction=direction)
    edge = round(etv - base_etv, 3) if base_etv is not None else None
    rec = strategy["recency"]

    gates: list = []

    def add(name, passed, reason, **extra):
        gates.append({"gate": name, "pass": passed, "reason": reason, **extra})

    def out(decision, reason):
        return _verdict(decision, reason, gates, H, spec_note, etv, edge, strategy, policy)

    # G1 · Sample (strategy N = resolved occurrences replayed)
    g1 = n >= N_MIN
    add("G1_sample", g1, f"n={n} (>= {N_MIN})" if g1 else f"too few precedents (n={n} < {N_MIN})")
    if not g1:
        return out("WATCH", f"too few precedents (n={n} < {N_MIN}).")

    # G2 · Edge — Strategy-ETV > 0 AND edge over the same-policy enter-every-day baseline > 0
    if etv <= 0:
        add("G2_edge", False, f"no edge after costs (Strategy-ETV {etv:+.2f}%)")
        return out("NO_TRADE", f"no edge after costs (Strategy-ETV {etv:+.2f}% under {policy.version}).")
    if edge is None or edge <= 0:
        add("G2_edge", False, f"profitable ({etv:+.2f}%) but no edge vs same-policy baseline ({edge})")
        return out("NO_TRADE", f"no edge vs the enter-every-day baseline under the same policy (edge {edge}).")
    add("G2_edge", True, f"Strategy-ETV {etv:+.2f}% and edge {edge:+.2f}% vs same-policy baseline")

    # G3 · Significance — CI_low(Strategy-ETV) > 0
    g3 = ci_low is not None and ci_low > 0
    add("G3_significance", g3, (f"CI_low(Strategy-ETV) = {ci_low:+.2f}% (normal-approx SE; bootstrap CI is SPEC §7.3)"
                                if ci_low is not None else "CI unavailable"))
    if not g3:
        return out("WATCH", f"edge inside the noise (95% CI_low {ci_low}%).")

    # G4 · Consistency across nested populations — [SPEC] (look-alike/sector engine not built)
    add("G4_consistency", None, "skipped — nested-population coherence engine is SPEC (§7.2/§7.3); "
                                 "only the Stock tier exists today.", skipped=True)

    # G5 · Tail — strategy avg-MAE cap AND payoff floor
    g5 = (mae >= -MAE_CAP * 100) and (payoff is not None and payoff >= PAYOFF_MIN)
    add("G5_tail", g5, f"avg strat-MAE {mae:+.2f}% (cap {-MAE_CAP*100:.0f}%), payoff {payoff}")
    if not g5:
        return out("NO_TRADE", f"risk/tail too large (strat-MAE {mae:+.2f}%, payoff {payoff}).")

    # G6 · Recency — recent-window Strategy-ETV not decaying (BUILT on strategy replay)
    decaying = (rec["recent_n"] >= MIN_RECENT and rec["prior_etv"] is not None and rec["prior_etv"] > 0
                and rec["recent_etv"] is not None and rec["recent_etv"] < DECAY_FRAC * rec["prior_etv"])
    if rec["recent_n"] < MIN_RECENT:
        add("G6_recency", True, f"recent n={rec['recent_n']} < {MIN_RECENT} — too few to judge decay; not blocked")
    else:
        add("G6_recency", not decaying,
            f"recent Strategy-ETV {rec['recent_etv']}% vs prior {rec['prior_etv']}%")
        if decaying:
            return out("WATCH", f"edge fading — recent Strategy-ETV {rec['recent_etv']}% vs prior {rec['prior_etv']}%.")

    return out("TRADE", f"positive strategy expectancy (+{etv:.2f}% under {policy.version}, "
                        f"+{edge:.2f}% edge, CI_low {ci_low:+.2f}%) across {n} precedents.")


def _verdict(decision, reason, gates, H, spec_note, etv, edge, strategy, policy):
    return {"decision": decision, "reason": reason, "gates": gates,
            "basis": "strategy_replay", "spec_note": spec_note,
            "evidence_ref_horizon": H, "etv": etv, "edge": edge,
            "strategy": strategy, "policy": policy.to_dict()}
