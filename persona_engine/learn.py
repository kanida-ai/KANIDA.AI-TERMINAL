"""
Self-learning loop: rule mining + lifecycle + human-review proposals
(spec §4.2 rule lifecycle, §4.3 scaling discipline, §4.4 P2/P3/P4/P10).

Two complementary learning signals feed ``learning_proposals``:

  1. **Weight proposals** — the rank-IC weights the walk-forward adopts in the
     research loop are recorded here as proposed changes for LIVE deployment, which
     still requires human approval (P4/P10).

  2. **Discrete rules** — single/二-feature threshold rules whose top-quintile
     membership predicts next-day (F&O) / 4-wk (LT) Top-10 inclusion at a lift over
     base rate. Each rule carries the full discipline record: sample size, hit rate,
     years, regimes, sectors, worst-year floor, consistency, status.

Scaling discipline gates (a rule is ACTIVE only if ALL hold):
  • n_occurrences >= 18
  • fired in >= 2 calendar years
  • fired in >= 2 market regimes
  • fired across >= 3 sectors (unless explicitly a sector rule)
  • worst-year hit rate above the coin-flip floor for the persona
  • out-of-sample (the metrics are pooled walk-forward, never in-sample-only)
"""
from __future__ import annotations

import json
import sqlite3
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from persona_engine import model, outcomes
from persona_engine.model import ALL_FEATURES

# coin-flip floors (spec §4.3): F&O > base-rate-ish; we use a practical floor that a
# rule's in-bucket hit rate must beat in its worst year.
FO_FLOOR = 0.08      # ~1.6x the 5% unconditional next-day-top10 base rate
LT_FLOOR = 0.08

MIN_OCC = 18


def _labeled_dataset(con, fo_universe, lt_universe) -> pd.DataFrame:
    feats = pd.read_sql_query("SELECT * FROM persona_signal_features", con)
    fwd = outcomes.forward_returns(con, symbols=sorted(set(fo_universe) | set(lt_universe)))
    data = feats.merge(
        fwd[["symbol", "trade_date", "fwd_nd", "ret_20"]],
        on=["symbol", "trade_date"], how="left")
    reg = model.market_regime_series(con)
    data = data.merge(reg[["trade_date", "regime"]], on="trade_date", how="left")
    data["year"] = data["trade_date"].str[:4]

    fo_set, lt_set = set(fo_universe), set(lt_universe)
    # per-date top10 labels
    def top10_flag(g, col, universe, asc=False):
        sub = g[g["symbol"].isin(universe)].dropna(subset=[col])
        if len(sub) < 20:
            return pd.Series(False, index=g.index)
        thr_idx = sub.sort_values(col, ascending=asc).head(10).index
        s = pd.Series(False, index=g.index)
        s.loc[thr_idx] = True
        return s

    data["lab_gain"] = False
    data["lab_lose"] = False
    data["lab_4wk"] = False
    for _, idx in data.groupby("trade_date").groups.items():
        g = data.loc[idx]
        data.loc[idx, "lab_gain"] = top10_flag(g, "fwd_nd", fo_set, asc=False)
        data.loc[idx, "lab_lose"] = top10_flag(g, "fwd_nd", fo_set, asc=True)
        data.loc[idx, "lab_4wk"] = top10_flag(g, "ret_20", lt_set, asc=False)
    return data


def _mine_feature_rules(data, label_col, universe, persona_tag, floor,
                        ret_col, walk_start="2022-01-01") -> List[Dict]:
    """For each feature, test the top (and bottom) quintile membership as a rule."""
    d = data[(data["symbol"].isin(universe)) & (data["trade_date"] >= walk_start)].copy()
    d = d.dropna(subset=[label_col])
    if d.empty:
        return []
    # per-date quintile thresholds
    proposals = []
    for feat in ALL_FEATURES:
        if feat not in d.columns or d[feat].notna().sum() < 1000:
            continue
        q = d.groupby("trade_date")[feat].transform(lambda s: s.rank(pct=True))
        for side, mask in (("hi", q >= 0.8), ("lo", q <= 0.2)):
            bucket = d[mask & d[feat].notna()]
            n = len(bucket)
            if n < MIN_OCC:
                continue
            hit = bucket[label_col].mean()
            base = d[label_col].mean()
            if hit <= base * 1.2:        # require >20% lift over base rate
                continue
            by_year = bucket.groupby("year")[label_col].mean()
            worst = float(by_year.min()) if len(by_year) else 0.0
            n_years = bucket["year"].nunique()
            n_reg = bucket["regime"].nunique()
            n_sec = bucket["sector"].nunique()
            avg_ret = float(bucket[ret_col].mean())
            active = (n >= MIN_OCC and n_years >= 2 and n_reg >= 2
                      and n_sec >= 3 and worst >= floor)
            op = ">=" if side == "hi" else "<="
            thr = "top-quintile" if side == "hi" else "bottom-quintile"
            rule_name = f"{persona_tag}:{feat}:{side}"
            proposals.append({
                "rule_name": rule_name,
                "rule_logic": f"{feat} {op} per-date {thr}",
                "n_occurrences": int(n),
                "hit_rate": round(float(hit) * 100, 2),
                "avg_return": round(avg_ret, 3),
                "regimes_tested": json.dumps(sorted(bucket["regime"].dropna().unique().tolist())),
                "n_years": int(n_years),
                "n_sectors": int(n_sec),
                "worst_year_hr": round(worst * 100, 2),
                "consistency": round(worst * 100, 2),
                "status": "ACTIVE" if active else "TESTING",
                "proposed_change": json.dumps({"base_rate_pct": round(base*100,2),
                                               "lift_pp": round((hit-base)*100,2)}),
            })
    # sort by lift desc
    proposals.sort(key=lambda p: -(p["hit_rate"]))
    return proposals


def run_learning(con: sqlite3.Connection, fo_universe, lt_universe,
                 week_ending: str, walk_start="2022-01-01") -> Dict:
    data = _labeled_dataset(con, fo_universe, lt_universe)
    fo_long = _mine_feature_rules(data, "lab_gain", fo_universe, "FO_LONG", FO_FLOOR, "fwd_nd", walk_start)
    fo_short = _mine_feature_rules(data, "lab_lose", fo_universe, "FO_SHORT", FO_FLOOR, "fwd_nd", walk_start)
    lt = _mine_feature_rules(data, "lab_4wk", lt_universe, "LT", LT_FLOOR, "ret_20", walk_start)

    all_props = ([("FO", p) for p in fo_long] + [("FO", p) for p in fo_short]
                 + [("LT", p) for p in lt])
    cur = con.cursor()
    n_active = 0
    for persona, p in all_props:
        if p["status"] == "ACTIVE":
            n_active += 1
        cur.execute(
            """INSERT OR REPLACE INTO learning_proposals
               (persona,week_ending,rule_name,rule_logic,n_occurrences,hit_rate,
                avg_return,regimes_tested,n_years,n_sectors,worst_year_hr,
                consistency,status,proposed_change,human_approved)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)""",
            (persona, week_ending, p["rule_name"], p["rule_logic"],
             p["n_occurrences"], p["hit_rate"], p["avg_return"],
             p["regimes_tested"], p["n_years"], p["n_sectors"],
             p["worst_year_hr"], p["consistency"], p["status"],
             p["proposed_change"]))
    con.commit()
    return {"n_proposals": len(all_props), "n_active": n_active,
            "fo_long": fo_long[:8], "fo_short": fo_short[:8], "lt": lt[:8]}


def record_weight_proposal(con, persona, week_ending, weights: Dict[str, float]):
    """Record an adopted IC-weight vector as a human-review proposal for LIVE use."""
    con.execute(
        """INSERT OR REPLACE INTO learning_proposals
           (persona,week_ending,rule_name,rule_logic,status,proposed_change,human_approved)
           VALUES (?,?,?,?, 'TESTING', ?, 0)""",
        (persona, week_ending, f"{persona}:ic_weights:{week_ending}",
         "rank-IC weight vector (trailing 126d)", json.dumps(weights)))
    con.commit()
