"""Orchestration: raw panel in, ranked surviving states out."""
from __future__ import annotations

import time

import numpy as np
import pandas as pd

from .config import Config
from .data import build_index
from .features import build_features, add_interactions
from .graph import build_graph_features
from .intraday import attach_intraday
from .labels import add_labels
from .timeframes import add_timeframe_features
from .states import fit_state_model, state_stats, promote_states, QuantileBinner, score_features
from .walkforward import run_walkforward, summarise


def prepare_panel(raw: pd.DataFrame, cfg: Config, verbose: bool = True,
                  intraday_cache: pd.DataFrame | None = None,
                  exact_labels: bool = False):
    """Build the full modelling panel. Returns (panel, feature_names).

    If `intraday_cache` is supplied, intraday FEATURES of day D are merged in as
    ordinary (causal) features and the exact first-touch OUTCOME of day D+1 is
    merged in as y_ft_*. With `exact_labels=True` the exact label replaces the
    daily proxy everywhere -- at the cost of restricting the sample to the period
    your intraday table covers.
    """
    t0 = time.time()
    d = build_index(raw)
    d, feats = build_features(d, cfg)
    if verbose:
        print(f"  feature factory   : {len(feats)} candidates  ({time.time()-t0:.1f}s)")

    if cfg.use_graph:
        t1 = time.time()
        d, gfeats = build_graph_features(d, cfg)
        feats += gfeats
        if verbose:
            print(f"  graph layer       : +{len(gfeats)} features   ({time.time()-t1:.1f}s)")

    if getattr(cfg, "use_timeframes", False):
        t0 = time.time()
        d, tf_cols = add_timeframe_features(d, verbose=verbose)
        feats += tf_cols
        if verbose:
            print(f"  higher timeframes : +{len(tf_cols)} features  ({time.time()-t0:.1f}s)")

    d = add_labels(d, cfg)

    if intraday_cache is not None:
        t3 = time.time()
        d, iv_feats = attach_intraday(d, intraday_cache)
        feats += iv_feats
        cov = d["y_ft_hit"].notna().mean()
        if verbose:
            print(f"  intraday layer    : +{len(iv_feats)} features, "
                  f"exact labels on {cov:.1%} of rows  ({time.time()-t3:.1f}s)")
        if exact_labels:
            d["y_hit_proxy"] = d["y_hit"]
            for a, b in [("y_hit", "y_ft_hit"), ("y_mfe", "y_ft_mfe"),
                         ("y_mae", "y_ft_mae"), ("y_ret", "y_ft_ret")]:
                d[a] = d[b]
            d["y_expectancy"] = np.where(
                d["y_ft_outcome"] == "target", cfg.target_pct,
                np.where(d["y_ft_outcome"] == "stop", -cfg.stop_pct, d["y_ft_ret"]))
            d.loc[d["y_hit"].isna(), "y_expectancy"] = np.nan
            if verbose:
                print("  labels            : EXACT first-touch (sample restricted)")

    # 633 symbols x ~1,130 days x ~390 features is ~2.2 GB in float64 before any
    # copies. float32 halves it and costs nothing: these are z-scores and ratios,
    # not currency amounts.
    if feats:
        others = [c for c in d.columns if c not in feats]
        d = pd.concat([d[others], d[feats].astype("float32")], axis=1)
        d = d.copy()          # single contiguous block, no fragmentation

    # liquidity / price filter, applied after labelling so the base rate is honest
    if cfg.min_price > 0:
        d = d[d["close"] >= cfg.min_price]
    if cfg.min_dollar_vol > 0:
        d = d[d["dollar_vol"] >= cfg.min_dollar_vol]

    if cfg.use_interactions:
        t2 = time.time()
        warm = d[d["y_hit"].notna()]
        cut = warm["date"].quantile(0.5)
        seed_train = warm[warm["date"] <= cut]
        binner = QuantileBinner(cfg.n_bins).fit(seed_train, feats)
        ranked = score_features(seed_train, feats, binner,
                                min_support=cfg.min_support_train // 2)
        top = list(ranked["feature"].head(cfg.interaction_top_k))
        d, feats = add_interactions(d, feats, top)
        if verbose:
            print(f"  interactions      : {len(feats)} total     ({time.time()-t2:.1f}s)")
        # NOTE: interaction seeding uses the first half of the sample only, so the
        # final folds remain clean. For maximum strictness set use_interactions=False.

    d = d.sort_values(["symbol", "date"]).reset_index(drop=True)
    if verbose:
        print(f"  panel             : {len(d):,} rows, {d['symbol'].nunique()} symbols, "
              f"{d['date'].min().date()} -> {d['date'].max().date()}")
    return d, feats


def run_pipeline(raw: pd.DataFrame, cfg: Config, verbose: bool = True,
                 intraday_cache: pd.DataFrame | None = None,
                 exact_labels: bool = False) -> dict:
    print("\nBUILDING PANEL")
    print("-" * 68)
    panel, feats = prepare_panel(raw, cfg, verbose, intraday_cache, exact_labels)

    labelled = panel[panel["y_hit"].notna()]
    print(f"  base rate P(hit)  : {labelled['y_hit'].mean():.4f} "
          f"(target +{cfg.target_pct:.2%}, stop -{cfg.stop_pct:.2%}, "
          f"mode={cfg.label_mode})")

    print("\nWALK-FORWARD")
    print("-" * 68)
    res = run_walkforward(panel, feats, cfg, verbose)
    res["panel"] = panel
    res["features"] = feats
    print(summarise(res, cfg))
    return res


def todays_signals(panel: pd.DataFrame, feats: list[str], cfg: Config,
                   asof: pd.Timestamp | None = None, top_n: int = 20) -> pd.DataFrame:
    """Production-style call: fit on everything up to `asof`, score that day's rows.

    This is what you would run at 15:31. The output is the frozen state vector's
    verdict, to be executed at the next open with no recomputation.
    """
    asof = pd.Timestamp(asof) if asof is not None else panel["date"].max()
    train = panel[(panel["date"] < asof) & (panel["y_hit"].notna())]
    live = panel[panel["date"] == asof]
    if train.empty or live.empty:
        return pd.DataFrame()

    model, _ = fit_state_model(train, feats, cfg)
    if model is None:
        return pd.DataFrame()
    stats = promote_states(state_stats(train, model.assign(train), cfg,
                                       cfg.min_support_train), cfg)
    if stats.empty:
        return pd.DataFrame()

    out = live.assign(state=model.assign(live)).merge(stats, on="state", how="inner")
    cols = ["date", "symbol", "close", "state", "n", "hit_rate", "wilson_lb",
            "base_rate", "lift", "mfe", "mae", "expectancy", "edge_score"]
    out = out[cols].sort_values("edge_score", ascending=False).head(top_n)

    def band(r):
        if r["wilson_lb"] - r["base_rate"] >= 0.10:
            return "STRONG BUY"
        if r["wilson_lb"] - r["base_rate"] >= 0.05:
            return "BUY"
        return "NEUTRAL"

    out["signal"] = out.apply(band, axis=1)
    return out.reset_index(drop=True)
