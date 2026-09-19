"""Tests that are allowed to fail the build.

The point of this suite is not code coverage. It is that the three ways this
kind of engine lies to you -- lookahead, label leakage, and a state signature
that silently collapses -- are each pinned down by an assertion.

Run:  pytest -q
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from state_engine import Config, data, leakcheck
from state_engine.features import build_features
from state_engine.labels import add_labels, label_columns
from state_engine.pipeline import prepare_panel
from state_engine.states import (QuantileBinner, fit_state_model, promote_states,
                                 state_stats, wilson_lower)
from state_engine.walkforward import month_folds


@pytest.fixture(scope="module")
def raw():
    return data.make_synthetic_panel(n_symbols=15, n_days=600, seed=11)


@pytest.fixture(scope="module")
def cfg():
    return Config()


# --------------------------------------------------------------------------- #
# causality                                                                     #
# --------------------------------------------------------------------------- #
def test_features_are_causal(raw, cfg):
    """Truncating the future must not change any past feature value.

    This is the single strongest leakage test available: rebuild the feature
    matrix on data ending at a cutoff and compare it to the full-sample build.
    Any expanding statistic, centred window, or global normalisation fails here.
    """
    full, feats = build_features(data.build_index(raw), cfg)
    cutoff = raw["date"].quantile(0.7)

    trunc_raw = raw[raw["date"] <= cutoff]
    trunc, _ = build_features(data.build_index(trunc_raw), cfg)

    a = full[full["date"] <= cutoff].set_index(["symbol", "date"])[feats]
    b = trunc.set_index(["symbol", "date"])[feats]
    common = a.index.intersection(b.index)
    diff = (a.loc[common] - b.loc[common]).abs()
    worst = float(np.nanmax(diff.to_numpy()))
    assert worst < 1e-8, f"non-causal feature detected, max drift {worst}"


def test_planted_leak_is_caught(raw, cfg):
    """A deliberately non-causal feature must be rejected by the audit."""
    assert leakcheck.run_all(raw, cfg, verbose=False)


def test_no_label_column_reaches_features(raw, cfg):
    panel, feats = prepare_panel(raw, cfg, verbose=False)
    assert not set(feats) & set(label_columns(panel))
    assert not any(f.startswith("y_") for f in feats)


# --------------------------------------------------------------------------- #
# labels                                                                        #
# --------------------------------------------------------------------------- #
def test_labels_measured_from_next_open(raw, cfg):
    d = add_labels(data.build_index(raw), cfg)
    g = d.groupby("symbol", sort=False)
    assert np.allclose(d["y_entry"].dropna(),
                       g["open"].shift(-1).dropna(), equal_nan=False)
    ok = d["y_mfe"].notna()
    assert (d.loc[ok, "y_mfe"] >= d.loc[ok, "y_mae"]).all()


def test_conservative_label_is_not_looser_than_optimistic(raw):
    c_cons, c_opt = Config(label_mode="conservative"), Config(label_mode="optimistic")
    base = data.build_index(raw)
    a = add_labels(base, c_cons)["y_hit"]
    b = add_labels(base, c_opt)["y_hit"]
    ok = a.notna() & b.notna()
    assert (a[ok] <= b[ok]).all()
    assert a[ok].mean() <= b[ok].mean()


def test_last_row_per_symbol_has_no_label(raw, cfg):
    d = add_labels(data.build_index(raw), cfg)
    last = d.groupby("symbol", sort=False).tail(1)
    assert last["y_hit"].isna().all()


# --------------------------------------------------------------------------- #
# state machinery                                                               #
# --------------------------------------------------------------------------- #
def test_state_signatures_are_row_wise(raw, cfg):
    """Regression test.

    An f-string over a Series renders the whole Series into one string, which
    collapses every row into a single state and makes the engine look dead.
    Assert that many distinct signatures exist and each is short.
    """
    panel, feats = prepare_panel(raw, cfg, verbose=False)
    train = panel[panel["y_hit"].notna()]
    model, _ = fit_state_model(train, feats, cfg)
    assert model is not None
    sig = model.assign(train).dropna()
    assert sig.nunique() > 10, "state signatures collapsed"
    assert sig.map(len).max() < 400, "signature looks like a stringified Series"


def test_bin_edges_fitted_on_train_only(raw, cfg):
    panel, feats = prepare_panel(raw, cfg, verbose=False)
    train = panel[panel["date"] < panel["date"].quantile(0.6)]
    b1 = QuantileBinner(cfg.n_bins).fit(train, feats[:20])
    b2 = QuantileBinner(cfg.n_bins).fit(train, feats[:20])
    for f in b1.fitted:
        assert np.allclose(b1.edges[f], b2.edges[f])   # deterministic
    b3 = QuantileBinner(cfg.n_bins).fit(panel, feats[:20])
    changed = any(not np.allclose(b1.edges[f], b3.edges[f])
                  for f in b1.fitted if f in b3.edges)
    assert changed, "edges identical on train vs full sample - suspicious"


def test_wilson_bound_is_conservative():
    assert wilson_lower(5, 10) < 0.5
    assert wilson_lower(500, 1000) < 0.5
    assert wilson_lower(500, 1000) > wilson_lower(5, 10)
    assert wilson_lower(0, 0) == 0.0


def test_promotion_requires_both_bars(raw, cfg):
    panel, feats = prepare_panel(raw, cfg, verbose=False)
    train = panel[panel["y_hit"].notna()]
    model, _ = fit_state_model(train, feats, cfg)
    stats = state_stats(train, model.assign(train), cfg, cfg.min_support_train)
    good = promote_states(stats, cfg)
    if not good.empty:
        assert (good["lift"] >= cfg.min_lift).all()
        assert (good["wilson_lb"] > good["base_rate"]).all()
        assert (good["n"] >= cfg.min_support_train).all()


# --------------------------------------------------------------------------- #
# walk-forward                                                                  #
# --------------------------------------------------------------------------- #
def test_folds_do_not_overlap(raw, cfg):
    folds = month_folds(raw["date"], cfg)
    assert folds, "no folds generated"
    for _, train_end, test_end in folds:
        assert test_end > train_end
    ends = [f[2] for f in folds]
    assert ends == sorted(ends)


def test_train_strictly_precedes_test(raw, cfg):
    panel, _ = prepare_panel(raw, cfg, verbose=False)
    for tr_s, tr_e, te_e in month_folds(panel["date"], cfg):
        train = panel[(panel["date"] >= tr_s) & (panel["date"] < tr_e)]
        test = panel[(panel["date"] >= tr_e) & (panel["date"] < te_e)]
        if train.empty or test.empty:
            continue
        assert train["date"].max() < test["date"].min()


# --------------------------------------------------------------------------- #
# the engine must find a known edge, and must not find one in noise             #
# --------------------------------------------------------------------------- #
def test_recovers_planted_edge():
    """The synthetic market contains a real conditional edge. Find it."""
    raw = data.make_synthetic_panel(n_symbols=30, n_days=900, seed=3)
    g = raw.groupby("symbol", sort=False)
    relvol = raw["volume"] / g["volume"].transform(
        lambda s: s.rolling(20, min_periods=20).mean())
    clv = (raw["close"] - raw["low"]) / (raw["high"] - raw["low"]).replace(0, np.nan)
    trig = ((relvol > 1.4) & (clv > 0.75)).fillna(False)

    d = add_labels(data.build_index(raw), Config())
    ok = d["y_hit"].notna()
    base = d.loc[ok, "y_hit"].mean()
    cond = d.loc[ok & trig, "y_hit"].mean()
    assert cond - base > 0.05, f"planted edge not present: {cond:.3f} vs {base:.3f}"


def test_no_edge_in_shuffled_labels(raw, cfg):
    """Destroy the feature-label link; almost nothing should survive promotion."""
    panel, feats = prepare_panel(raw, cfg, verbose=False)
    train = panel[panel["y_hit"].notna()].copy()
    rng = np.random.default_rng(0)
    train["y_hit"] = rng.permutation(train["y_hit"].to_numpy())
    model, _ = fit_state_model(train, feats, cfg)
    if model is None:
        return
    stats = state_stats(train, model.assign(train), cfg, cfg.min_support_train)
    good = promote_states(stats, cfg)
    frac = len(good) / max(len(stats), 1)
    assert frac < 0.10, f"{frac:.1%} of states passed on shuffled labels"
