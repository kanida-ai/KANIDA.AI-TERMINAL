"""
MUTABLE CAPABILITY LAYER  —  the agent MAY author & evolve components here.
Every component still passes immutable_core.gauntlet before admission.
Thresholds are sampled from TRAIN quantiles ONLY (never val/vault) — leak-free.
"""
import numpy as np
import immutable_core as core
from immutable_core import load_min, load_atr, day_pack, TRAIN, features, orb_late, FEATURES


def make_predicate(feature, lo, hi):
    def pred(f, side):
        x = f[feature]; return lo <= x <= hi
    pred.desc = f"{lo:.4g}<={feature}<={hi:.4g}"
    pred.feature = feature
    return pred


def make_filter(preds):
    def filt(f, side):
        return all(p(f, side) for p in preds)
    filt.desc = " AND ".join(p.desc for p in preds) if preds else "(no filter)"
    filt.preds = list(preds)
    return filt


def train_feature_stats(sym):
    """Point-in-time feature distribution on TRAIN only (for threshold authoring)."""
    atrmap = load_atr(sym); tr = day_pack(load_min(sym, *TRAIN))
    vals = {k: [] for k in FEATURES}
    for d, day in tr.items():
        ap, pc = atrmap.get(d, (np.nan, np.nan))
        if pc is None or (isinstance(pc, float) and np.isnan(pc)):
            continue
        r = orb_late(day)
        if r is None:
            continue
        e, side, _ = r
        f = features(day, ap, pc, e)
        for k in FEATURES:
            vals[k].append(f[k])
    return {k: np.array(v) for k, v in vals.items()}


# quantile bands the agent explores when authoring a predicate over a feature
_BANDS = [(0.0, 0.5), (0.5, 1.0), (0.25, 0.75), (0.0, 0.33), (0.67, 1.0), (0.33, 0.67)]


def author_predicates(stats):
    """Author candidate one-feature predicates from TRAIN quantiles (self-written components)."""
    out = []
    for fe in FEATURES:
        s = stats[fe]
        if len(s) < 50 or np.nanstd(s) == 0:
            continue
        for ql, qh in _BANDS:
            lo = float(np.quantile(s, ql)); hi = float(np.quantile(s, qh))
            if hi > lo:
                out.append(make_predicate(fe, lo, hi))
    return out
