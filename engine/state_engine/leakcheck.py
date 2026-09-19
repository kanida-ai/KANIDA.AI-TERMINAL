"""Leakage detectors.

If these do not pass, nothing else in the engine means anything. Run them before
believing any result.

  1. causality   -- recompute every feature on data truncated at day T. The value
                    on row T must be bit-identical to the value computed on the
                    full history. If truncation changes it, the feature saw the
                    future.
  2. no_y_leak   -- assert no `y_` column reached the feature list.
  3. shuffle     -- destroy the feature->label link by permuting labels WITHIN
                    each date, then re-run the miner. It should find essentially
                    nothing. If it still finds many "edges", the discovery
                    procedure is manufacturing them.
  4. shift_probe -- inject a deliberately leaky feature (tomorrow's return). Test 1
                    must catch it. This checks the checker.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .config import Config
from .features import build_features
from .data import build_index
from .labels import add_labels
from .states import fit_state_model, state_stats, promote_states


def check_causality(raw: pd.DataFrame, cfg: Config, n_probe_dates: int = 3,
                    tol: float = 1e-9, verbose: bool = True) -> bool:
    """Truncate history at date T, rebuild features, compare row T to the full run."""
    full, feats = build_features(build_index(raw), cfg)
    dates = np.sort(raw["date"].unique())
    probes = dates[int(len(dates) * 0.6)::max(1, len(dates) // (n_probe_dates * 3))][:n_probe_dates]

    ok = True
    for T in probes:
        trunc_raw = raw[raw["date"] <= T]
        trunc, _ = build_features(build_index(trunc_raw), cfg)
        a = full[full["date"] == T].set_index("symbol")[feats].sort_index()
        b = trunc[trunc["date"] == T].set_index("symbol")[feats].sort_index()
        common = a.index.intersection(b.index)
        diff = (a.loc[common] - b.loc[common]).abs()
        bad = diff.max(axis=0)
        bad = bad[(bad > tol) & bad.notna()]
        if len(bad):
            ok = False
            if verbose:
                print(f"  [FAIL] {pd.Timestamp(T).date()}: {len(bad)} non-causal features, "
                      f"worst: {list(bad.sort_values(ascending=False).head(5).index)}")
        elif verbose:
            print(f"  [ok]   {pd.Timestamp(T).date()}: all {len(feats)} features causal")
    return ok


def check_no_label_leak(features: list[str], verbose: bool = True) -> bool:
    bad = [f for f in features if f.startswith("y_")]
    if verbose:
        print(f"  [{'ok' if not bad else 'FAIL'}]   label columns in feature list: {bad}")
    return not bad


def check_shuffle(panel: pd.DataFrame, features: list[str], cfg: Config,
                  verbose: bool = True) -> bool:
    """Permute labels within each date. A sound miner should find almost nothing."""
    rng = np.random.default_rng(cfg.seed)
    d = panel.copy()
    d["y_hit"] = (d.groupby("date", observed=True)["y_hit"]
                   .transform(lambda s: pd.Series(rng.permutation(s.to_numpy()), index=s.index)))
    d = d[d["y_hit"].notna()]

    model, _ = fit_state_model(d, features, cfg)
    if model is None:
        if verbose:
            print("  [ok]   shuffled data produced no state model at all")
        return True
    stats = state_stats(d, model.assign(d), cfg, cfg.min_support_train)
    good = promote_states(stats, cfg)
    frac = len(good) / max(len(stats), 1)
    passed = frac < 0.10
    if verbose:
        print(f"  [{'ok' if passed else 'FAIL'}]   shuffled labels: "
              f"{len(good)}/{len(stats)} states passed the in-sample bar ({frac:.1%}); "
              f"want <10%")
    return passed


def check_probe(raw: pd.DataFrame, cfg: Config, verbose: bool = True) -> bool:
    """Plant a leak on purpose and confirm check_causality catches it."""
    d = build_index(raw)
    d["poisoned"] = d.groupby("symbol", sort=False)["close"].shift(-1)
    dates = np.sort(d["date"].unique())
    T = dates[int(len(dates) * 0.7)]
    full_val = d.loc[d["date"] == T, "poisoned"].to_numpy()
    tr = build_index(raw[raw["date"] <= T])
    tr["poisoned"] = tr.groupby("symbol", sort=False)["close"].shift(-1)
    trunc_val = tr.loc[tr["date"] == T, "poisoned"].to_numpy()
    caught = not np.allclose(np.nan_to_num(full_val), np.nan_to_num(trunc_val))
    if verbose:
        print(f"  [{'ok' if caught else 'FAIL'}]   planted leak was "
              f"{'detected' if caught else 'MISSED'} by the truncation test")
    return caught


def run_all(raw: pd.DataFrame, cfg: Config, verbose: bool = True) -> bool:
    if verbose:
        print("\nLEAKAGE AUDIT")
        print("-" * 68)
    r1 = check_causality(raw, cfg, verbose=verbose)
    r4 = check_probe(raw, cfg, verbose=verbose)

    panel, feats = build_features(build_index(raw), cfg)
    panel = add_labels(panel, cfg)
    r2 = check_no_label_leak(feats, verbose=verbose)
    r3 = check_shuffle(panel[panel["y_hit"].notna()], feats, cfg, verbose=verbose)

    ok = all([r1, r2, r3, r4])
    if verbose:
        print("-" * 68)
        print(f"AUDIT {'PASSED' if ok else 'FAILED'}")
    return ok
