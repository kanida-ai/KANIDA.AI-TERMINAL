"""Tests for the baseline profiler and the layered attribution."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from state_engine import Config, data
from state_engine.baseline import (BaselineSpec, add_excursions, continuation_curve,
                                   persistence_profile, profitability_profile,
                                   symbol_baseline)
from state_engine.attribution import (GraphConfirmer, combine_evidence,
                                      feature_family, shrink, split_graph,
                                      symbol_base_rates)
from state_engine.labels import add_labels
from state_engine.data import build_index


@pytest.fixture(scope="module")
def raw():
    return data.make_synthetic_panel(n_symbols=12, n_days=700, seed=21)


# --------------------------------------------------------------------------- #
# baseline                                                                      #
# --------------------------------------------------------------------------- #
def test_excursions_bracket_the_close(raw):
    d = add_excursions(raw)
    assert (d["ex_up"] >= d["ex_close"] - 1e-9).all()
    assert (d["ex_dn"] <= d["ex_close"] + 1e-9).all()
    assert (d["ex_range"] >= 0).all()


def test_reach_probabilities_are_monotone(raw):
    b = symbol_baseline(raw).iloc[0]
    for a, c in [("0.5", "0.7"), ("0.7", "1"), ("1", "1.5"), ("1.5", "2")]:
        assert b[f"p_up_{a}"] >= b[f"p_up_{c}"] - 1e-12
        assert b[f"p_dn_{a}"] >= b[f"p_dn_{c}"] - 1e-12


def test_continuation_is_a_conditional_probability(raw):
    """Reaching a bigger milestone implies reaching the smaller one, so every
    continuation figure must lie in [0, 1] and decay as the target grows."""
    cc = continuation_curve(raw, side="long")
    sub = cc[cc["reached"] == 0.005].iloc[0]
    vals = [sub["to_0.7"], sub["to_1"], sub["to_1.5"], sub["to_2"]]
    assert all(0.0 <= v <= 1.0 for v in vals)
    assert vals == sorted(vals, reverse=True)


def test_both_sides_are_profiled(raw):
    for side in ("long", "short"):
        cc = continuation_curve(raw, side=side)
        pp = persistence_profile(raw, side=side)
        pr = profitability_profile(raw, side=side)
        assert not cc.empty and not pp.empty and not pr.empty
        assert set(cc["side"]) == {side}
        assert set(pp["side"]) == {side}


def test_persistence_reports_conditional_and_unconditional(raw):
    pp = persistence_profile(raw, side="long")
    assert {"p_uncond", "p_given_moved", "lift"} <= set(pp.columns)
    assert np.allclose(pp["lift"], pp["p_given_moved"] - pp["p_uncond"], equal_nan=True)
    # a random-walk panel should show no strong persistence either way
    assert pp["lift"].abs().median() < 0.15


def test_persistence_probability_rises_with_horizon(raw):
    pp = persistence_profile(raw, side="long")
    one = pp[pp["horizon_days"] == 1].set_index("symbol")["p_uncond"]
    five = pp[pp["horizon_days"] == 5].set_index("symbol")["p_uncond"]
    common = one.index.intersection(five.index)
    assert (five[common] >= one[common] - 1e-9).all()


def test_breakeven_matches_the_payoff_ratio(raw):
    pr = profitability_profile(raw, target=0.007, stop=0.005).iloc[0]
    assert pr["breakeven_hit_rate"] == pytest.approx(0.005 / 0.012)
    assert pr["edge_vs_breakeven"] == pytest.approx(
        pr["p_target"] - pr["breakeven_hit_rate"])


def test_character_classification(raw):
    b = symbol_baseline(raw)
    assert set(b["character"]) <= {"momentum", "mean-reverting", "mixed"}


# --------------------------------------------------------------------------- #
# long vs short are genuinely separate                                          #
# --------------------------------------------------------------------------- #
def test_short_is_not_just_negated_long(raw):
    base = build_index(raw)
    lo = add_labels(base, Config(side="long"))
    sh = add_labels(base, Config(side="short"))
    ok = lo["y_hit"].notna() & sh["y_hit"].notna()
    # both can be 0 on the same day (neither barrier reached), so they are not
    # complements; and both can be 1 only if the ordering allowed it
    assert ((lo.loc[ok, "y_hit"] == 0) & (sh.loc[ok, "y_hit"] == 0)).any()
    assert lo.loc[ok, "y_hit"].mean() != pytest.approx(1 - sh.loc[ok, "y_hit"].mean())


def test_short_favourable_excursion_is_downward(raw):
    sh = add_labels(build_index(raw), Config(side="short"))
    ok = sh["y_mfe"].notna()
    assert np.allclose(sh.loc[ok, "y_mfe"], -sh.loc[ok, "y_dn_excursion"])
    assert np.allclose(sh.loc[ok, "y_mae"], -sh.loc[ok, "y_up_excursion"])
    assert (sh.loc[ok, "y_mfe"] >= sh.loc[ok, "y_mae"]).all()


def test_invalid_side_rejected(raw):
    with pytest.raises(ValueError):
        add_labels(build_index(raw), Config(side="sideways"))


# --------------------------------------------------------------------------- #
# attribution                                                                   #
# --------------------------------------------------------------------------- #
def test_shrinkage_moves_small_samples_toward_the_parent():
    assert shrink(1, 2, 0.2) == pytest.approx((1 + 50 * 0.2) / 52)
    assert abs(shrink(1, 2, 0.2) - 0.2) < abs(0.5 - 0.2)          # pulled in
    assert shrink(600, 1000, 0.2) > 0.55                          # large n dominates
    assert shrink(0, 0, 0.37) == pytest.approx(0.37)              # no data -> parent


def test_symbol_base_rates_differ_from_global(raw):
    d = add_labels(build_index(raw), Config())
    rates, glob = symbol_base_rates(d[d["y_hit"].notna()])
    assert len(rates) == raw["symbol"].nunique()
    assert rates.std() > 0
    assert 0.0 < glob < 1.0


def test_feature_families_separate_evidence():
    assert feature_family("g_peer_bull_frac") == "graph"
    assert feature_family("iv_or_ret") == "intraday"
    assert feature_family("x_f_clv__z5__X__f_volume__z5") == "interaction"
    assert feature_family("f_volume__z20") == "volume"
    assert feature_family("f_clv__pctrank20") == "location"
    assert feature_family("f_close__slope10") == "trend"


def test_split_graph_is_exhaustive():
    feats = ["f_close__z5", "g_degree", "g_peer_ret_w", "iv_or_ret"]
    non_g, g = split_graph(feats)
    assert g == ["g_degree", "g_peer_ret_w"]
    assert set(non_g) | set(g) == set(feats)
    assert not set(non_g) & set(g)


def test_combine_evidence_pools_states():
    rows = pd.DataFrame({"family": ["volume", "trend", "location"],
                         "n": [800, 120, 300],
                         "p_shrunk": [0.30, 0.40, 0.25],
                         "lift": [0.10, 0.20, 0.05]})
    ev = combine_evidence(rows, base=0.20)
    assert ev["n_states"] == 3
    assert ev["n_confirming"] == 3
    assert 0.25 < ev["p_combined"] < 0.40      # weighted, not the max
    assert ev["best_lift"] == pytest.approx(0.20)


def test_combine_evidence_with_nothing_firing():
    ev = combine_evidence(pd.DataFrame(), base=0.34)
    assert ev["p_combined"] == 0.34            # falls back to the base rate
    assert ev["n_states"] == 0


def test_graph_confirmer_is_fitted_on_train_only(raw):
    from state_engine.pipeline import prepare_panel
    cfg = Config()
    panel, feats = prepare_panel(raw, cfg, verbose=False)
    _, gfeats = split_graph(feats)
    train = panel[panel["date"] < panel["date"].quantile(0.6)]
    train = train[train["y_hit"].notna()]
    c = GraphConfirmer().fit(train, gfeats)
    if c.features:
        assert np.isfinite(c.threshold)
        conf = c.confirms(panel)
        assert conf.dtype == bool
        # threshold is the train 70th percentile, so roughly 30% confirm in train
        assert 0.10 < c.confirms(train).mean() < 0.55
