"""State discovery.

A *state* is a reproducible description of a symbol-day that measurably shifts
the outcome distribution. It is discovered, not named.

Two methods are provided, because how you carve the feature space determines
everything downstream (including what the state-transition graph looks like):

  grid -- select k weakly-correlated informative features, quantile-bin each on
          TRAIN ONLY, and let the bin tuple be the state. Interpretable,
          stable, and the bin edges are an explicit fitted artefact.
  tree -- fit a shallow decision tree on TRAIN; each leaf is a state. Finds
          interactions the grid misses, at the cost of less stable signatures
          across folds.

Everything fitted here (bin edges, selected features, tree structure) is fitted
on the training window and then applied unchanged to the test window.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from .config import Config


# --------------------------------------------------------------------------- #
# statistics helpers                                                            #
# --------------------------------------------------------------------------- #
def wilson_lower(k: int, n: int, z: float = 1.64) -> float:
    """One-sided lower confidence bound on a proportion. Small n -> low bound."""
    if n == 0:
        return 0.0
    p = k / n
    denom = 1.0 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt(max(p * (1 - p) / n + z * z / (4 * n * n), 0.0))
    return max((centre - margin) / denom, 0.0)


# --------------------------------------------------------------------------- #
# binning                                                                       #
# --------------------------------------------------------------------------- #
@dataclass
class QuantileBinner:
    n_bins: int
    edges: dict[str, np.ndarray] = field(default_factory=dict)

    def fit(self, X: pd.DataFrame, features: list[str]) -> "QuantileBinner":
        qs = np.linspace(0, 1, self.n_bins + 1)[1:-1]
        for f in features:
            v = X[f].to_numpy(dtype=float)
            v = v[np.isfinite(v)]
            if v.size < self.n_bins * 10:
                continue
            e = np.unique(np.quantile(v, qs))
            if e.size >= 1:
                self.edges[f] = e
        return self

    def transform_col(self, s: pd.Series, f: str) -> pd.Series:
        if f not in self.edges:
            return pd.Series(np.nan, index=s.index)
        b = np.digitize(s.to_numpy(dtype=float), self.edges[f], right=False).astype(float)
        b[~np.isfinite(s.to_numpy(dtype=float))] = np.nan
        return pd.Series(b, index=s.index)

    @property
    def fitted(self) -> list[str]:
        return list(self.edges.keys())


# --------------------------------------------------------------------------- #
# feature scoring and selection                                                 #
# --------------------------------------------------------------------------- #
def score_features(train: pd.DataFrame, features: list[str], binner: QuantileBinner,
                   y_col: str = "y_hit", min_support: int = 60,
                   min_coverage: float = 0.0) -> pd.DataFrame:
    """Discrete 'eta squared': how much does knowing the bin move the hit rate?

    score = support-weighted variance of per-bin hit rates around the base rate.
    Cheap, monotone-agnostic, and directly aligned with what a state is for.
    """
    y = train[y_col]
    ok = y.notna()
    y = y[ok]
    base = float(y.mean())
    rows = []
    n_rows = int(ok.sum())
    for f in binner.fitted:
        b = binner.transform_col(train.loc[ok, f], f)
        m = b.notna()
        if n_rows and (m.sum() / n_rows) < min_coverage:
            continue        # low-coverage feature would shrink the usable sample
        if m.sum() < min_support * 2:
            continue
        grp = pd.DataFrame({"b": b[m], "y": y[m]}).groupby("b", observed=True)["y"]
        cnt, mean = grp.count(), grp.mean()
        keep = cnt >= min_support
        if keep.sum() < 2:
            continue
        wgt = cnt[keep] / cnt[keep].sum()
        var = float((wgt * (mean[keep] - base) ** 2).sum())
        rows.append({"feature": f, "score": var, "n_bins": int(keep.sum()),
                     "coverage": float(m.sum() / n_rows) if n_rows else 0.0,
                     "max_abs_lift": float((mean[keep] - base).abs().max())})
    return (pd.DataFrame(rows).sort_values("score", ascending=False)
            .reset_index(drop=True) if rows else pd.DataFrame(columns=["feature", "score"]))


def select_features(train: pd.DataFrame, ranked: pd.DataFrame, k: int,
                    max_corr: float) -> list[str]:
    """Greedy: take the best-scoring feature, then skip anything too correlated with it."""
    chosen: list[str] = []
    for f in ranked["feature"]:
        if len(chosen) >= k:
            break
        if not chosen:
            chosen.append(f)
            continue
        sub = train[chosen + [f]].dropna()
        if len(sub) < 50:
            continue
        c = sub.corr().loc[f, chosen].abs().max()
        if c <= max_corr:
            chosen.append(f)
    return chosen


# --------------------------------------------------------------------------- #
# state models                                                                  #
# --------------------------------------------------------------------------- #
@dataclass
class GridStateModel:
    features: list[str]
    binner: QuantileBinner

    def assign(self, X: pd.DataFrame) -> pd.Series:
        bins = {f: self.binner.transform_col(X[f], f) for f in self.features}
        B = pd.DataFrame(bins, index=X.index)
        ok = B.notna().all(axis=1)
        sig = pd.Series(pd.NA, index=X.index, dtype="object")
        if ok.any():
            # element-wise concatenation. Do NOT use an f-string here: it would
            # render the whole Series into a single string and collapse every
            # row into one state.
            parts = [f + "=" + B.loc[ok, f].astype(int).astype(str)
                     for f in self.features]
            joined = parts[0]
            for p in parts[1:]:
                joined = joined + "|" + p
            sig.loc[ok] = joined.to_numpy()
        return sig

    def describe(self) -> dict:
        return {"method": "grid", "features": self.features,
                "edges": {f: self.binner.edges[f].tolist() for f in self.features
                          if f in self.binner.edges}}


@dataclass
class TreeStateModel:
    features: list[str]
    tree: object

    def assign(self, X: pd.DataFrame) -> pd.Series:
        M = X[self.features]
        ok = M.notna().all(axis=1)
        sig = pd.Series(pd.NA, index=X.index, dtype="object")
        if ok.any():
            leaf = self.tree.apply(M.loc[ok].to_numpy(dtype=float))
            sig.loc[ok] = ["leaf=" + str(v) for v in leaf]
        return sig

    def describe(self) -> dict:
        return {"method": "tree", "features": self.features}


def fit_state_model(train: pd.DataFrame, features: list[str], cfg: Config):
    """Fit the chosen state model on the TRAINING window only."""
    binner = QuantileBinner(cfg.n_bins).fit(train, features)
    ranked = score_features(train, features, binner, min_support=cfg.min_support_train // 2,
                            min_coverage=cfg.min_feature_coverage)
    if ranked.empty:
        return None, ranked

    if cfg.state_method == "tree":
        try:
            from sklearn.tree import DecisionTreeClassifier
        except ImportError:
            raise RuntimeError("state_method='tree' needs scikit-learn: pip install scikit-learn")
        chosen = list(ranked["feature"].head(max(cfg.state_features * 4, 12)))
        sub = train[chosen + ["y_hit"]].dropna()
        if len(sub) < cfg.min_support_train * 4:
            return None, ranked
        clf = DecisionTreeClassifier(
            max_leaf_nodes=cfg.tree_max_leaves,
            min_samples_leaf=cfg.tree_min_samples_leaf,
            random_state=cfg.seed,
        ).fit(sub[chosen].to_numpy(dtype=float), sub["y_hit"].to_numpy())
        return TreeStateModel(chosen, clf), ranked

    chosen = select_features(train, ranked, cfg.state_features, cfg.max_corr_between_selected)
    if len(chosen) < 1:
        return None, ranked
    return GridStateModel(chosen, binner), ranked


# --------------------------------------------------------------------------- #
# state statistics                                                              #
# --------------------------------------------------------------------------- #
def state_stats(df: pd.DataFrame, sig: pd.Series, cfg: Config,
                min_support: int) -> pd.DataFrame:
    """Outcome distribution per state: the probability engine."""
    d = df.assign(_sig=sig)
    d = d[d["_sig"].notna() & d["y_hit"].notna()]
    if d.empty:
        return pd.DataFrame()
    base = float(d["y_hit"].mean())

    g = d.groupby("_sig", observed=True)
    out = pd.DataFrame({
        "n": g["y_hit"].size(),
        "hits": g["y_hit"].sum(),
        "hit_rate": g["y_hit"].mean(),
        "mfe": g["y_mfe"].mean(),
        "mae": g["y_mae"].mean(),
        "ret": g["y_ret"].mean(),
        "expectancy": g["y_expectancy"].mean(),
        "hit_rate_down": g["y_hit_down"].mean(),
    }).reset_index().rename(columns={"_sig": "state"})

    out["base_rate"] = base
    out["lift"] = out["hit_rate"] - base
    out["wilson_lb"] = [wilson_lower(int(h), int(n), cfg.wilson_z)
                        for h, n in zip(out["hits"], out["n"])]
    out["edge_score"] = (out["wilson_lb"] - base) * np.log1p(out["n"])
    out = out[out["n"] >= min_support]
    return out.sort_values("edge_score", ascending=False).reset_index(drop=True)


def promote_states(stats: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """A state only leaves the training window if it clears both bars."""
    if stats.empty:
        return stats
    ok = (stats["lift"] >= cfg.min_lift) & (stats["wilson_lb"] > stats["base_rate"])
    return stats[ok].reset_index(drop=True)
