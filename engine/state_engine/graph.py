"""Relationship graph layer.

Two rules make this leak-proof:

  1. Edges for date D are estimated from returns strictly BEFORE D, over a
     trailing window of `graph_lookback` days.
  2. The graph is rebuilt on a cadence (`graph_rebuild_days`) and then FROZEN
     until the next rebuild. It is never estimated once over the full sample.

Node features use peers' SAME-DAY (day T) returns, which are available at the
day-T close, so they remain causal.

Edge types implemented:
  contemp  -- trailing Pearson correlation of daily returns
  leadlag  -- corr(peer_ret[t-1], self_ret[t]); positive = peer leads self
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .config import Config

GRAPH_FEATURES = [
    "g_peer_bull_frac",
    "g_peer_ret_w",
    "g_self_minus_peer",
    "g_leader_ret_w",
    "g_peer_relvol",
    "g_peer_breadth20",
    "g_degree",
]


def _corr_matrix(mat: np.ndarray) -> np.ndarray:
    """Pearson correlation across columns, NaN-safe enough for our use."""
    x = mat - np.nanmean(mat, axis=0, keepdims=True)
    sd = np.nanstd(x, axis=0, keepdims=True)
    sd[sd == 0] = np.nan
    x = x / sd
    n = np.sum(~np.isnan(x), axis=0)
    x = np.nan_to_num(x, nan=0.0)
    c = (x.T @ x) / np.maximum(n[:, None], 1)
    np.fill_diagonal(c, 0.0)
    return np.clip(c, -1.0, 1.0)


def build_graph_features(d: pd.DataFrame, cfg: Config) -> tuple[pd.DataFrame, list[str]]:
    """Attach graph features to the long panel. Returns (panel, feature_names)."""
    if not cfg.use_graph:
        return d, []

    relvol_src = d["volume"] / d.groupby("symbol", sort=False)["volume"].transform(
        lambda s: s.rolling(20, min_periods=20).mean())
    d = d.assign(_relvol=relvol_src)

    dates = pd.DatetimeIndex(np.sort(d["date"].unique()))
    syms = sorted(d["symbol"].unique())

    def wide(col: str) -> pd.DataFrame:
        return (d.pivot(index="date", columns="symbol", values=col)
                 .reindex(index=dates, columns=syms))

    ret, relvol, close = wide("ret1"), wide("_relvol"), wide("close")
    above20 = (close > close.rolling(20, min_periods=20).mean()).astype(float)

    R = ret.to_numpy(dtype=float)
    n_dates, n_syms = R.shape

    out = {name: np.full((n_dates, n_syms), np.nan) for name in GRAPH_FEATURES}
    RV = relvol.to_numpy(dtype=float)
    A20 = above20.to_numpy(dtype=float)

    lb, cad = cfg.graph_lookback, cfg.graph_rebuild_days
    W_contemp = np.zeros((n_syms, n_syms))
    W_lead = np.zeros((n_syms, n_syms))
    have_graph = False

    for i in range(n_dates):
        # ---- rebuild on cadence, using history strictly before day i ----
        if i >= lb and (i - lb) % cad == 0:
            hist = R[i - lb:i, :]                       # rows i-lb .. i-1
            c = _corr_matrix(hist)
            lead = _corr_matrix_lagged(hist)
            W_contemp = _sparsify(c, cfg)
            W_lead = _sparsify(lead, cfg, cfg.graph_min_lead_corr)
            have_graph = True

        if not have_graph:
            continue

        r_t, rv_t, a_t = R[i, :], RV[i, :], A20[i, :]
        valid = (~np.isnan(r_t)).astype(float)
        pr = np.nan_to_num(r_t, nan=0.0)

        W = W_contemp * valid[None, :]          # zero out peers with no data today
        M = (W != 0).astype(float)              # adjacency mask
        deg = M.sum(axis=1)
        tot = np.abs(W).sum(axis=1)
        safe_deg = np.where(deg > 0, deg, np.nan)
        safe_tot = np.where(tot > 0, tot, np.nan)

        out["g_degree"][i, :] = deg
        out["g_peer_bull_frac"][i, :] = (M @ (pr > 0).astype(float)) / safe_deg
        peer_ret = (W @ pr) / safe_tot
        out["g_peer_ret_w"][i, :] = peer_ret
        out["g_self_minus_peer"][i, :] = np.where(valid > 0, r_t, np.nan) - peer_ret

        rv_ok = (~np.isnan(rv_t)).astype(float)
        out["g_peer_relvol"][i, :] = (M @ np.nan_to_num(rv_t)) / np.where(
            (M @ rv_ok) > 0, M @ rv_ok, np.nan)
        a_ok = (~np.isnan(a_t)).astype(float)
        out["g_peer_breadth20"][i, :] = (M @ np.nan_to_num(a_t)) / np.where(
            (M @ a_ok) > 0, M @ a_ok, np.nan)

        WL = W_lead * valid[None, :]
        totl = np.abs(WL).sum(axis=1)
        out["g_leader_ret_w"][i, :] = (WL @ pr) / np.where(totl > 0, totl, np.nan)

    frames = []
    for name, arr in out.items():
        f = pd.DataFrame(arr, index=dates, columns=syms).stack(future_stack=True).rename(name)
        frames.append(f)
    gdf = pd.concat(frames, axis=1).reset_index()
    gdf.columns = ["date", "symbol"] + GRAPH_FEATURES

    d = d.drop(columns=["_relvol"]).merge(gdf, on=["date", "symbol"], how="left")
    return d, list(GRAPH_FEATURES)


def _corr_matrix_lagged(hist: np.ndarray) -> np.ndarray:
    """corr(peer[t-1], self[t]). Entry [j, k] = how well peer k leads symbol j."""
    a = hist[:-1, :]     # peers at t-1
    b = hist[1:, :]      # self  at t
    a = np.nan_to_num(a - np.nanmean(a, axis=0, keepdims=True), nan=0.0)
    b = np.nan_to_num(b - np.nanmean(b, axis=0, keepdims=True), nan=0.0)
    sa = np.linalg.norm(a, axis=0)
    sb = np.linalg.norm(b, axis=0)
    sa[sa == 0] = np.nan
    sb[sb == 0] = np.nan
    c = (b.T @ a) / np.outer(sb, sa)
    np.fill_diagonal(c, 0.0)
    return np.nan_to_num(np.clip(c, -1, 1), nan=0.0)


def _sparsify(c: np.ndarray, cfg: Config, thresh: float | None = None) -> np.ndarray:
    """Keep only the strongest `graph_max_neighbours` edges above the threshold."""
    t = cfg.graph_min_corr if thresh is None else thresh
    w = np.where(np.abs(c) >= t, c, 0.0)
    k = cfg.graph_max_neighbours
    if k < w.shape[1]:
        for j in range(w.shape[0]):
            row = np.abs(w[j])
            if (row > 0).sum() > k:
                cut = np.partition(row, -k)[-k]
                w[j][row < cut] = 0.0
    return w
