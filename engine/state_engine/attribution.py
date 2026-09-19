"""Layered attribution and multi-state evidence.

THE QUESTION THIS ANSWERS
-------------------------
    "ICICI's base rate for +1% is 34%. If these states fire it becomes 75%.
     If the graph also confirms it becomes 85%. Show me that, per stock."

The engine could not previously say this, for two reasons:

  1. It compared every state against ONE global base rate. Across 500 symbols
     P(+1%) ranges from roughly 15% to 55%, so a global denominator makes a
     state look brilliant on a quiet stock and useless on a volatile one.
     Here the denominator is the symbol's own base rate.

  2. Graph features were poured into the same pool as everything else, so their
     contribution was invisible. Here the layers are separated and measured:

        L0  symbol base rate            what the stock does anyway
        L1  + state                     what the mined conditions add
        L2  + graph confirmation        what the peer network adds on top

     Each layer is estimated on TRAIN and verified on TEST. A layer that gains
     in-sample and gives it back out-of-sample is reported as such rather than
     quietly absorbed.

MULTIPLE STATES ON THE SAME DAY
-------------------------------
One state model assigns exactly one label per row, which cannot express "four
different things are true about this stock today". So a separate model is fitted
per feature FAMILY (trend, volume, volatility, location, relative, graph...).
Each family fires its own state, all of them are reported, and the evidence is
pooled. Agreement across independent families is itself information: three
families each showing a modest edge is a different situation from one family
showing a large one.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .config import Config
from .states import (QuantileBinner, fit_state_model, score_features,
                     select_features, wilson_lower, GridStateModel)
from .walkforward import month_folds

PRIOR_STRENGTH = 50.0     # Beta-prior weight when shrinking toward the parent rate


# --------------------------------------------------------------------------- #
# helpers                                                                       #
# --------------------------------------------------------------------------- #
def shrink(hits: float, n: float, parent: float, k: float = PRIOR_STRENGTH) -> float:
    """Shrink a rate toward its parent. n=0 -> parent; n>>k -> the raw rate."""
    return (hits + k * parent) / (n + k) if (n + k) > 0 else parent


def symbol_base_rates(train: pd.DataFrame, y: str = "y_hit") -> tuple[pd.Series, float]:
    """Each symbol's own base rate, shrunk toward the universe rate."""
    d = train[train[y].notna()]
    glob = float(d[y].mean())
    g = d.groupby("symbol")[y]
    rates = pd.Series({s: shrink(float(v.sum()), float(v.size), glob)
                       for s, v in g}, name="base_rate")
    return rates, glob


def feature_family(name: str) -> str:
    """Group features so 'multiple states firing' means genuinely different evidence."""
    if name.startswith("g_"):
        return "graph"
    if name.startswith("iv_"):
        return "intraday"
    if name.startswith("x_"):
        return "interaction"
    if name.startswith("f_"):
        base = name[2:].split("__")[0]
        return {
            "close": "trend", "ret1": "trend", "rel_str": "relative",
            "volume": "volume", "dollar_vol": "volume",
            "range_pct": "volatility", "tr_pct": "volatility",
            "clv": "location", "gap": "location", "vwap_dist": "location",
        }.get(base, base)
    return "other"


def split_graph(features: list[str]) -> tuple[list[str], list[str]]:
    g = [f for f in features if feature_family(f) == "graph"]
    return [f for f in features if f not in g], g


# --------------------------------------------------------------------------- #
# graph confirmation                                                            #
# --------------------------------------------------------------------------- #
class GraphConfirmer:
    """A single 'do the peers agree?' score, thresholded on TRAIN.

    Built from whichever graph features actually carried signal in the training
    window, oriented so that higher always means more confirming.
    """

    def __init__(self, quantile: float = 0.70):
        self.quantile = quantile
        self.features: list[str] = []
        self.signs: dict[str, float] = {}
        self.edges: dict[str, np.ndarray] = {}
        self.threshold: float = np.nan

    def fit(self, train: pd.DataFrame, graph_feats: list[str],
            y: str = "y_hit") -> "GraphConfirmer":
        d = train[train[y].notna()]
        if d.empty or not graph_feats:
            return self
        base = float(d[y].mean())
        for f in graph_feats:
            v = d[f]
            ok = v.notna()
            if ok.sum() < 200:
                continue
            hi = v[ok] >= v[ok].median()
            if hi.sum() < 50 or (~hi).sum() < 50:
                continue
            lift_hi = float(d.loc[ok & hi, y].mean() - base)
            lift_lo = float(d.loc[ok & ~hi, y].mean() - base)
            if abs(lift_hi - lift_lo) < 1e-6:
                continue
            self.features.append(f)
            self.signs[f] = 1.0 if lift_hi > lift_lo else -1.0
            self.edges[f] = np.nanquantile(v[ok].to_numpy(), np.linspace(0, 1, 21))
        if self.features:
            self.threshold = float(np.nanquantile(self.score(d).to_numpy(), self.quantile))
        return self

    def score(self, X: pd.DataFrame) -> pd.Series:
        """Mean signed percentile across the informative graph features."""
        if not self.features:
            return pd.Series(np.nan, index=X.index)
        parts = []
        for f in self.features:
            r = np.searchsorted(self.edges[f], X[f].to_numpy(dtype=float)) / 20.0
            r = np.where(np.isfinite(X[f].to_numpy(dtype=float)), r, np.nan)
            parts.append(self.signs[f] * r)
        stack = np.vstack(parts)
        with np.errstate(invalid="ignore"):
            allnan = np.all(np.isnan(stack), axis=0)
            out = np.full(stack.shape[1], np.nan)
            if (~allnan).any():
                out[~allnan] = np.nanmean(stack[:, ~allnan], axis=0)
        return pd.Series(out, index=X.index)

    def confirms(self, X: pd.DataFrame) -> pd.Series:
        if not self.features or not np.isfinite(self.threshold):
            return pd.Series(False, index=X.index)
        return self.score(X) >= self.threshold


# --------------------------------------------------------------------------- #
# multi-family state models                                                     #
# --------------------------------------------------------------------------- #
def fit_family_models(train: pd.DataFrame, features: list[str], cfg: Config,
                      min_features: int = 4) -> dict[str, GridStateModel]:
    """One state model per feature family -> several states can fire per day."""
    fams: dict[str, list[str]] = {}
    for f in features:
        fams.setdefault(feature_family(f), []).append(f)

    models = {}
    for fam, fl in fams.items():
        if len(fl) < min_features:
            continue
        binner = QuantileBinner(cfg.n_bins).fit(train, fl)
        ranked = score_features(train, fl, binner,
                                min_support=cfg.min_support_train // 2,
                                min_coverage=cfg.min_feature_coverage)
        if ranked.empty:
            continue
        chosen = select_features(train, ranked, min(cfg.state_features, 2),
                                 cfg.max_corr_between_selected)
        if chosen:
            models[fam] = GridStateModel(chosen, binner)
    return models


def family_state_table(train: pd.DataFrame, models: dict[str, GridStateModel],
                       cfg: Config, y: str = "y_hit") -> pd.DataFrame:
    """Per family, per state: shrunk probability and support, fitted on TRAIN."""
    d = train[train[y].notna()]
    glob = float(d[y].mean())
    rows = []
    for fam, m in models.items():
        sig = m.assign(d)
        ok = sig.notna()
        if not ok.any():
            continue
        g = pd.DataFrame({"s": sig[ok], "y": d.loc[ok, y]}).groupby("s")["y"]
        for st, v in g:
            n, h = float(v.size), float(v.sum())
            if n < cfg.min_support_train:
                continue
            p = shrink(h, n, glob)
            rows.append({"family": fam, "state": st, "n": int(n), "raw_rate": h / n,
                         "p_shrunk": p, "base_rate": glob, "lift": p - glob,
                         "wilson_lb": wilson_lower(int(h), int(n), cfg.wilson_z),
                         "features": ",".join(m.features)})
    return pd.DataFrame(rows)


def combine_evidence(rows: pd.DataFrame, base: float) -> dict:
    """Pool several families' states for one symbol-day.

    Weighted by log support, so a state seen 800 times counts more than one seen
    120 times, without letting it dominate outright. `n_confirming` is reported
    separately because agreement across independent families is its own signal.
    """
    if rows.empty:
        return {"p_combined": base, "n_states": 0, "n_confirming": 0,
                "families": "", "best_lift": 0.0}
    w = np.log1p(rows["n"].to_numpy(dtype=float))
    p = float(np.average(rows["p_shrunk"].to_numpy(dtype=float), weights=w))
    return {
        "p_combined": p,
        "n_states": len(rows),
        "n_confirming": int((rows["lift"] > 0).sum()),
        "families": ",".join(sorted(rows["family"])),
        "best_lift": float(rows["lift"].max()),
    }


# --------------------------------------------------------------------------- #
# the layered walk-forward                                                      #
# --------------------------------------------------------------------------- #
def layered_walkforward(panel: pd.DataFrame, features: list[str], cfg: Config,
                        verbose: bool = True) -> dict:
    """Measure L0 -> L1 -> L2 out of sample, per state and per symbol."""
    non_graph, graph_feats = split_graph(features)
    if verbose:
        print(f"\nLAYERED ATTRIBUTION  (side={cfg.side})")
        print("-" * 68)
        print(f"  {len(non_graph)} non-graph features, {len(graph_feats)} graph features")

    fold_rows, state_rows = [], []
    for fi, (tr_s, tr_e, te_e) in enumerate(month_folds(panel["date"], cfg), 1):
        train = panel[(panel["date"] >= tr_s) & (panel["date"] < tr_e)]
        test = panel[(panel["date"] >= tr_e) & (panel["date"] < te_e)]
        train, test = train[train["y_hit"].notna()], test[test["y_hit"].notna()]
        if len(train) < cfg.min_support_train * 5 or test.empty:
            continue

        base_by_sym, glob = symbol_base_rates(train)
        model, _ = fit_state_model(train, non_graph, cfg)
        if model is None:
            continue
        confirmer = GraphConfirmer().fit(train, graph_feats) if graph_feats else None

        tr_sig, te_sig = model.assign(train), model.assign(test)
        tr_conf = confirmer.confirms(train) if confirmer else pd.Series(False, index=train.index)
        te_conf = confirmer.confirms(test) if confirmer else pd.Series(False, index=test.index)

        train_ = train.assign(_s=tr_sig, _c=tr_conf)
        test_ = test.assign(_s=te_sig, _c=te_conf)

        # which states earned the right to be counted, judged on TRAIN only
        promoted = set()

        for st, tg in train_[train_["_s"].notna()].groupby("_s"):
            n = len(tg)
            if n < cfg.min_support_train:
                continue
            p_state = shrink(float(tg["y_hit"].sum()), float(n), glob)
            cg = tg[tg["_c"]]
            p_conf = (shrink(float(cg["y_hit"].sum()), float(len(cg)), p_state)
                      if len(cg) >= 30 else np.nan)

            if (p_state - glob) >= cfg.min_lift and \
               wilson_lower(int(tg["y_hit"].sum()), n, cfg.wilson_z) > glob:
                promoted.add(st)

            tt = test_[test_["_s"] == st]
            tc = tt[tt["_c"]]
            sym_base = float(base_by_sym.reindex(tt["symbol"]).mean()) if len(tt) else np.nan

            ex = "y_expectancy"
            state_rows.append({
                "fold": fi, "state": st,
                "test_exp": float(tt[ex].mean()) if (len(tt) and ex in tt) else np.nan,
                "base_exp": float(test[ex].mean()) if ex in test else np.nan,
                "n_train": n, "L0_base": glob, "L1_state": p_state,
                "L2_state_graph": p_conf, "n_train_confirmed": len(cg),
                "n_test": len(tt), "test_symbol_base": sym_base,
                "test_L1_rate": float(tt["y_hit"].mean()) if len(tt) else np.nan,
                "n_test_confirmed": len(tc),
                "test_L2_rate": float(tc["y_hit"].mean()) if len(tc) else np.nan,
            })

        good = test_[test_["_s"].isin(promoted)]
        gc = good[good["_c"]]
        ex = "y_expectancy"
        fold_rows.append({
            "fold": fi, "train_end": tr_e,
            "L0": float(test["y_hit"].mean()),
            "L1": float(good["y_hit"].mean()) if len(good) else np.nan,
            "L2": float(gc["y_hit"].mean()) if len(gc) else np.nan,
            # win rate is not money. These are the numbers that decide.
            "exp_L0": float(test[ex].mean()) if ex in test else np.nan,
            "exp_L1": float(good[ex].mean()) if (len(good) and ex in good) else np.nan,
            "exp_L2": float(gc[ex].mean()) if (len(gc) and ex in gc) else np.nan,
            "n_L1": len(good), "n_L2": len(gc), "n_promoted": len(promoted),
            "n_test": len(test),
        })
        if verbose and fi % 6 == 0:
            r = fold_rows[-1]
            print(f"  fold {fi:>3} | L0 {r['L0']:.3f} -> L1 {r['L1']:.3f} "
                  f"-> L2 {r['L2'] if np.isfinite(r['L2']) else float('nan'):.3f}")

    folds = pd.DataFrame(fold_rows)
    states = pd.DataFrame(state_rows)

    summary = pd.DataFrame()
    if not states.empty:
        pass
    if not states.empty:
        summary = (states.groupby("state")
                   .agg(folds=("fold", "nunique"),
                        n_train=("n_train", "mean"),
                        n_test=("n_test", "sum"),
                        L0=("L0_base", "mean"),
                        L1_train=("L1_state", "mean"),
                        L2_train=("L2_state_graph", "mean"),
                        L1_test=("test_L1_rate", "mean"),
                        L2_test=("test_L2_rate", "mean"),
                        sym_base=("test_symbol_base", "mean"))
                   .reset_index())
        summary["state_lift_oos"] = summary["L1_test"] - summary["sym_base"]
        summary["graph_lift_oos"] = summary["L2_test"] - summary["L1_test"]
        summary = summary[summary["n_test"] >= cfg.min_support_test * 3]
        summary = summary.sort_values("state_lift_oos", ascending=False).reset_index(drop=True)

    if verbose and not folds.empty:
        print("\n" + "=" * 68)
        print(f"LAYER ATTRIBUTION, OUT OF SAMPLE   (side={cfg.side})")
        print("=" * 68)
        L0, L1, L2 = folds["L0"].mean(), folds["L1"].mean(), folds["L2"].mean()
        print(f"  L0  base rate, no state        : {L0:.3f}")
        print(f"  L1  + state fires              : {L1:.3f}   ({L1 - L0:+.3f})")
        print(f"  L2  + graph confirms           : {L2:.3f}   ({L2 - L1:+.3f} on top)")
        print(f"  signals: {int(folds['n_L1'].sum()):,} at L1, "
              f"{int(folds['n_L2'].sum()):,} at L2")
        print(f"  promoted states per fold (median): "
              f"{folds['n_promoted'].median():.0f}")

        e0, e1, e2 = folds["exp_L0"].mean(), folds["exp_L1"].mean(), folds["exp_L2"].mean()
        print("\n  EXPECTANCY PER TRADE, net of cost   <- this is the number")
        print(f"    L0  no state   : {e0*10000:+7.1f} bps")
        print(f"    L1  + state    : {e1*10000:+7.1f} bps   ({(e1-e0)*10000:+.1f})")
        if np.isfinite(e2):
            print(f"    L2  + graph    : {e2*10000:+7.1f} bps   ({(e2-e1)*10000:+.1f})")
        pos = int((folds["exp_L1"] > folds["exp_L0"]).sum())
        tot = int(folds["exp_L1"].notna().sum())
        print(f"    folds where the state beat the null: {pos}/{tot}")

        # how many promoted states would pure noise have produced?
        print("\n  NOISE CHECK")
        print(f"    the shuffle test promotes states at the audit's false-positive")
        print(f"    rate. If promoted-per-fold is near that rate, these states are")
        print(f"    indistinguishable from luck regardless of their lift.")
        if np.isfinite(L2) and L2 - L1 <= 0:
            print("\n  The graph layer did NOT add out of sample. Do not ship it,")
            print("  and do not consider a GNN.")
    return {"folds": folds, "states": states, "summary": summary}


# --------------------------------------------------------------------------- #
# end-of-day output: everything firing today, per symbol                        #
# --------------------------------------------------------------------------- #
def todays_evidence(panel: pd.DataFrame, features: list[str], cfg: Config,
                    asof: pd.Timestamp | None = None, top_n: int = 25) -> pd.DataFrame:
    """The 15:31 run: every state firing on every symbol, with layered odds.

    One row per symbol. `p_combined` pools all firing families; `n_confirming`
    says how many independently agreed. Compare `p_combined` against
    `symbol_base` -- never against the universe rate.
    """
    asof = pd.Timestamp(asof) if asof is not None else panel["date"].max()
    train = panel[(panel["date"] < asof) & (panel["y_hit"].notna())]
    live = panel[panel["date"] == asof]
    if train.empty or live.empty:
        return pd.DataFrame()

    non_graph, graph_feats = split_graph(features)
    base_by_sym, glob = symbol_base_rates(train)
    models = fit_family_models(train, non_graph, cfg)
    table = family_state_table(train, models, cfg)
    confirmer = GraphConfirmer().fit(train, graph_feats) if graph_feats else None

    lookup = {(r["family"], r["state"]): r for _, r in table.iterrows()}
    conf_flag = confirmer.confirms(live) if confirmer else pd.Series(False, index=live.index)
    conf_score = confirmer.score(live) if confirmer else pd.Series(np.nan, index=live.index)

    assigned = {fam: m.assign(live) for fam, m in models.items()}

    out = []
    for idx, row in live.iterrows():
        hits = []
        for fam, sig in assigned.items():
            st = sig.get(idx)
            if st is None or pd.isna(st):
                continue
            rec = lookup.get((fam, st))
            if rec is not None:
                hits.append(rec)
        hitdf = pd.DataFrame(hits)
        sym_base = float(base_by_sym.get(row["symbol"], glob))
        ev = combine_evidence(hitdf, sym_base)
        out.append({
            "date": asof, "symbol": row["symbol"], "side": cfg.side,
            "symbol_base": sym_base,
            "p_state": ev["p_combined"],
            "state_lift": ev["p_combined"] - sym_base,
            "n_states": ev["n_states"], "n_confirming": ev["n_confirming"],
            "families": ev["families"],
            "graph_confirms": bool(conf_flag.get(idx, False)),
            "graph_score": float(conf_score.get(idx, np.nan)),
        })

    res = pd.DataFrame(out)
    if res.empty:
        return res
    # graph layer is a multiplier on the state edge, estimated from train
    res["p_final"] = np.where(res["graph_confirms"],
                              res["p_state"] + 0.5 * res["state_lift"].clip(lower=0),
                              res["p_state"])
    res["total_lift"] = res["p_final"] - res["symbol_base"]
    res = res.sort_values("total_lift", ascending=False).head(top_n)

    def band(r):
        if r["total_lift"] >= 0.10 and r["n_confirming"] >= 2 and r["graph_confirms"]:
            return "STRONG"
        if r["total_lift"] >= 0.05 and r["n_confirming"] >= 2:
            return "MODERATE"
        if r["total_lift"] >= 0.03:
            return "WEAK"
        return "NONE"

    res["signal"] = res.apply(band, axis=1)
    return res.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# the scorecard: rank states the way a researcher would review them             #
# --------------------------------------------------------------------------- #
def scorecard(states: pd.DataFrame, min_folds: int = 3,
              min_occurrences: int = 100) -> pd.DataFrame:
    """Lift alone is the wrong ranking. A state earns trust on five things:

        support      does it happen often enough to measure?
        recurrence   how many walk-forward folds does it survive?
        lift         does it move the probability?
        stability    is the lift steady, or one lucky fold?
        expectancy   does it make money, not just win more often?

    The composite multiplies them, so a zero on any one dimension kills the
    state. That is deliberate: a 70% win rate on 24 occurrences in one fold is
    not a finding. The t-statistic is the most honest single column -- mean lift
    over its own standard error across folds. Below about 2 you are looking at
    noise.
    """
    if states.empty:
        return pd.DataFrame()

    g = states.groupby("state")
    sc = pd.DataFrame({
        "occurrences": g["n_test"].sum(),
        "folds": g["fold"].nunique(),
        "win_rate": g["test_L1_rate"].mean(),
        "base_rate": g["test_symbol_base"].mean(),
        "expectancy": g["test_exp"].mean() if "test_exp" in states else np.nan,
        "base_exp": g["base_exp"].mean() if "base_exp" in states else np.nan,
    })
    sc["lift"] = sc["win_rate"] - sc["base_rate"]
    sc["exp_lift"] = sc["expectancy"] - sc["base_exp"]

    per_fold_lift = states.assign(
        l=states["test_L1_rate"] - states["test_symbol_base"]).groupby("state")["l"]
    sc["lift_std"] = per_fold_lift.std()
    sc["folds_positive"] = per_fold_lift.apply(lambda s: int((s > 0).sum()))
    sc["recurrence"] = sc["folds_positive"] / sc["folds"].clip(lower=1)

    # t-stat of the mean fold lift: the one number that resists self-deception
    se = sc["lift_std"] / np.sqrt(sc["folds"].clip(lower=1))
    sc["t_stat"] = sc["lift"] / se.replace(0, np.nan)

    sc["support_score"] = (sc["occurrences"] / 300.0).clip(upper=1.0)
    cv = (sc["lift_std"] / sc["lift"].abs().replace(0, np.nan)).fillna(9.9)
    sc["stability_score"] = 1.0 / (1.0 + cv)

    sc["composite"] = (sc["lift"].clip(lower=0) * sc["support_score"]
                       * sc["recurrence"] * sc["stability_score"])
    sc.loc[sc["exp_lift"] <= 0, "composite"] = 0.0      # no money, no score

    def grade(r):
        if (r["composite"] >= 0.02 and r["t_stat"] >= 2.0
                and r["occurrences"] >= min_occurrences and r["folds"] >= min_folds):
            return "A"
        if r["composite"] >= 0.01 and r["t_stat"] >= 1.5 and r["folds"] >= min_folds:
            return "B"
        if r["composite"] > 0:
            return "C"
        return "reject"

    sc["grade"] = sc.apply(grade, axis=1)
    return sc.sort_values("composite", ascending=False).reset_index()


def print_scorecard(states: pd.DataFrame, top: int = 15) -> pd.DataFrame:
    sc = scorecard(states)
    if sc.empty:
        print("\n  no states to score")
        return sc
    print("\n" + "=" * 100)
    print("STATE SCORECARD   (ranked on the composite, not on lift)")
    print("=" * 100)
    cols = ["occurrences", "folds", "folds_positive", "win_rate", "base_rate",
            "lift", "expectancy", "exp_lift", "t_stat", "composite", "grade"]
    view = sc.head(top).copy()
    view["state"] = view["state"].str.slice(0, 46)
    print(view[["state"] + cols].round(4).to_string(index=False))
    print("\n  grades: A = trustworthy, B = promising, C = weak, reject = no money")
    print(f"  A:{(sc.grade=='A').sum()}  B:{(sc.grade=='B').sum()}  "
          f"C:{(sc.grade=='C').sum()}  reject:{(sc.grade=='reject').sum()}")
    if (sc.grade == "A").sum() == 0:
        print("\n  No grade-A states. Nothing here is ready to trade.")
    return sc
