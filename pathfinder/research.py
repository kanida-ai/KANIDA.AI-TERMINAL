"""Historical screening: next-open entries, date-clustered evidence, explicit limits."""
from collections import defaultdict
from statistics import mean, stdev, NormalDist

from .models import matches


def selections(proposal, market, day):
    rows = market.features.get(day, {})
    return sorted((s for s, r in rows.items() if matches(proposal, r)),
                  key=lambda s: (-rows[s]["return_1"], s))[:proposal["top_k"]]


def screen(proposal, market, policy):
    # Discovery history is reusable and therefore NEVER described as untouched validation.
    sessions = market.days
    clusters, lifts = defaultdict(list), defaultdict(list)
    costs = policy.cost_bps_per_side / 10000
    for i, day in enumerate(sessions):
        exit_i = i + proposal["hold_sessions"]
        if exit_i >= len(sessions):
            break
        entry_day, exit_day = sessions[i+1], sessions[exit_i]
        entry, end = market.by_day[entry_day], market.by_day[exit_day]
        available = [s for s in market.features.get(day, {}) if s in entry and s in end and entry[s]["volume"] > 0 and end[s]["volume"] > 0]
        if not available:
            continue
        outcomes = {s: end[s]["close"]*(1-costs)/(entry[s]["open"]*(1+costs))-1 for s in available}
        baseline = mean(outcomes.values())
        for symbol in selections(proposal, market, day):
            if symbol in outcomes:
                clusters[day].append(outcomes[symbol])
                lifts[day].append(outcomes[symbol]-baseline)
    days = sorted(clusters)
    # Nonoverlapping signal windows reduce overlapping-horizon dependence.
    sampled, last_i = [], -100
    for day in days:
        idx = sessions.index(day)
        if idx-last_i >= proposal["hold_sessions"]:
            sampled.append(day)
            last_i = idx
    values = [mean(clusters[d]) for d in sampled]
    excess = [mean(lifts[d]) for d in sampled]
    folds = []
    for k in range(3):
        a, b = k*len(values)//3, (k+1)*len(values)//3
        folds.append(mean(values[a:b]) if values[a:b] else None)
    enough = len(values) >= policy.min_screen_days
    positive = bool(values) and mean(values) > 0 and mean(excess) > 0
    stable = all(x is not None and x > 0 for x in folds)
    eligible = enough and positive and stable
    return {"signal_count": sum(map(len, clusters.values())), "independent_windows": len(values),
            "mean_net_return": mean(values) if values else None,
            "mean_excess_return": mean(excess) if excess else None,
            "chronological_block_returns": folds, "paper_eligible": eligible,
            "status": "paper_eligible" if eligible else "inconclusive" if not enough else "screen_failed",
            "reason": "Positive net return and sample-relative lift across three historical blocks; small paper trial only."
            if eligible else "Insufficient independent windows." if not enough else "Net return, baseline lift or chronological stability failed.",
            "evidence_kind": "adaptive_historical_screen_not_confirmation",
            "data_through": sessions[-1],
            "limitations": ["History reused for discovery; not an untouched test.",
                            "Fixed observed universe can have survivorship bias.",
                            "Daily bars cannot test 10 a.m. entries or intraday stop ordering.",
                            "Fixed cost model; corporate-action adjustment and universe history need external verification."]}


def prospective_evidence(trades, policy, trial_number):
    grouped = defaultdict(list)
    for t in trades:
        grouped[t["entry_date"]].append(t["pnl"] / t["cost_basis"])
    values = [mean(grouped[d]) for d in sorted(grouped)]
    n = len(values)
    average = mean(values) if values else None
    # Lifetime alpha spending; repeated checkpoints share an experiment's allocation.
    # Approximate normal bound, explicitly not a formal proof for dependent market returns.
    checkpoint = max(1, n // policy.min_paper_days)
    alpha = .05 / (trial_number * (trial_number+1) * checkpoint * (checkpoint+1))
    z = NormalDist().inv_cdf(1-alpha)
    lower = average-z*stdev(values)/(n**.5) if n > 1 else None
    status = "collecting"
    if n >= policy.min_paper_days:
        status = "retired" if average <= 0 else "supported_provisionally" if lower is not None and lower > 0 else "collecting"
    return {"entry_days": n, "mean_net_return": average, "approx_lower_bound": lower,
            "alpha_budget": alpha, "status": status,
            "qualification": "Date-clustered normal approximation; dependence and regime changes can invalidate confidence. No production promotion."}
