"""V4 FEATURE BUILDER — a representation the model can learn from.

WHY V3 COULD NOT GET THERE
--------------------------
V3 searched combinations of twenty booleans that I hand-wrote. That is a
vocabulary problem, not a search problem: if the edge lives in a shape I did not
think to encode, no amount of searching finds it. Twenty-four thousand tests
per stock only established that those twenty shapes, in pairs and triples, do
not contain much.

V4 does not enumerate rules. It builds a dense numeric description of every
decision point and lets a gradient booster find the structure -- including
interactions and thresholds nobody specified.

WHAT IS NEW HERE, CONCRETELY
----------------------------
1. SEQUENCE, not a point. The last 60 minutes are described as a shape: the
   slope of price, where volume sat in that window, how the range evolved, how
   many bars closed in the upper third. V3 saw only "is this bar's CLV low".

2. CROSS-SECTIONAL context. What the index did in the same minutes, and what
   this stock did RELATIVE to it. You have 131 indices sitting unused. A stock
   falling while the market rises is a completely different state from one
   falling with it, and V3 could not tell them apart.

3. MULTI-SCALE. The same descriptors at 5, 15, 30 and 60 minute lookbacks, so
   the model can pick the horizon that matters instead of me guessing.

4. SESSION POSITION. Minutes since the open, minutes to the close, and where
   the day sits relative to its own opening range and prior close.

CAUSALITY, UNCHANGED
--------------------
Every feature at decision minute t uses bars up to and including t. The trade
enters at the OPEN of t+1. The index features use the index's own bars up to t.
Nothing reads forward. This is verified by the truncation test in the agent:
rebuild on truncated data, demand identical values.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

EPS = 1e-12
LOOKBACKS = (5, 15, 30, 60)


def _win_stats(x: np.ndarray, w: int) -> dict:
    """Shape of the last w observations, not just their mean."""
    if len(x) < 2:
        return {"slope": 0.0, "std": 0.0, "rng": 0.0, "last_z": 0.0}
    a = x[-w:] if len(x) >= w else x
    n = len(a)
    t = np.arange(n) - (n - 1) / 2.0
    denom = (t ** 2).sum()
    slope = float((t * (a - a.mean())).sum() / denom) if denom > 0 else 0.0
    sd = float(a.std())
    return {"slope": slope, "std": sd,
            "rng": float(a.max() - a.min()),
            "last_z": float((a[-1] - a.mean()) / (sd + EPS))}


def session_matrix(g: pd.DataFrame, idx: pd.DataFrame | None,
                   step: int = 5) -> pd.DataFrame:
    """One row per decision minute. Dense numeric description, no booleans."""
    d = g.sort_values("ts").reset_index(drop=True)
    n = len(d)
    if n < 40:
        return pd.DataFrame()
    # warm-up scales with the session so this also works on coarser bars
    warm = min(60, max(n // 5, 12))

    o = d["open"].to_numpy(float)
    h = d["high"].to_numpy(float)
    lo = d["low"].to_numpy(float)
    c = d["close"].to_numpy(float)
    v = d["volume"].to_numpy(float)

    sess_open = o[0]
    ret = np.log(c / sess_open)
    bar_ret = np.diff(np.log(c), prepend=np.log(c[0]))
    tp = (h + lo + c) / 3.0
    cum_v = np.cumsum(v) + EPS
    vwap = np.cumsum(tp * v) / cum_v
    rng = (h - lo) / np.maximum(c, EPS)
    clv = np.where(h > lo, (c - lo) / np.maximum(h - lo, EPS), 0.5)

    ir = None
    if idx is not None and len(idx) >= n:
        ic = idx["close"].to_numpy(float)[:n]
        ir = np.log(ic / ic[0])

    rows = []
    for t in range(warm, n - 1, step):
        r = {"bar": t, "minute": t,
             "mins_from_open": t, "mins_to_close": n - 1 - t,
             "ret_from_open": float(ret[t]),
             "vwap_dist": float(c[t] / vwap[t] - 1.0),
             "clv": float(clv[t]),
             "range_now": float(rng[t]),
             "vol_share": float(v[t] / (cum_v[t] / (t + 1) + EPS)),
             "pos_in_day_range": float((c[t] - lo[:t + 1].min())
                                       / (h[:t + 1].max() - lo[:t + 1].min() + EPS)),
             "dist_day_high": float(c[t] / h[:t + 1].max() - 1.0),
             "dist_day_low": float(c[t] / lo[:t + 1].min() - 1.0),
             }
        for w in LOOKBACKS:
            s = _win_stats(ret[:t + 1], w)
            r[f"ret_slope_{w}"] = s["slope"] * 1e4
            r[f"ret_std_{w}"] = s["std"] * 1e2
            r[f"ret_rng_{w}"] = s["rng"] * 1e2
            r[f"ret_z_{w}"] = s["last_z"]
            sv = _win_stats(np.log1p(v[:t + 1]), w)
            r[f"vol_slope_{w}"] = sv["slope"]
            r[f"vol_z_{w}"] = sv["last_z"]
            sr = _win_stats(rng[:t + 1], w)
            r[f"rng_slope_{w}"] = sr["slope"] * 1e4
            r[f"rng_z_{w}"] = sr["last_z"]
            a = slice(max(0, t + 1 - w), t + 1)
            r[f"clv_mean_{w}"] = float(clv[a].mean())
            r[f"clv_hi_frac_{w}"] = float((clv[a] > 0.66).mean())
            r[f"clv_lo_frac_{w}"] = float((clv[a] < 0.33).mean())
            r[f"up_bar_frac_{w}"] = float((bar_ret[a] > 0).mean())
            r[f"vol_conc_{w}"] = float(v[a].max() / (v[a].sum() + EPS))
            if ir is not None:
                si = _win_stats(ir[:t + 1], w)
                r[f"idx_slope_{w}"] = si["slope"] * 1e4
                r[f"rel_slope_{w}"] = (s["slope"] - si["slope"]) * 1e4
                r[f"rel_ret_{w}"] = float(
                    (ret[t] - ret[max(0, t - w)]) - (ir[t] - ir[max(0, t - w)])) * 1e2
        # opening range position
        k_or = max(min(15, n // 10), 3)
        orh, orl = h[:k_or].max(), lo[:k_or].min()
        r["or_pos"] = float((c[t] - orl) / (orh - orl + EPS))
        r["or_width"] = float((orh - orl) / sess_open)
        r["entry_px"] = float(o[t + 1])
        r["close_px"] = float(c[-1])
        rows.append(r)
    out = pd.DataFrame(rows)
    out["session"] = d["session"].iloc[0]
    return out


def build_panel(bars: pd.DataFrame, index_bars: pd.DataFrame | None = None,
                step: int = 5, verbose: bool = True) -> pd.DataFrame:
    """Decision-point panel across all sessions."""
    idx_by_sess = ({s: g.sort_values("ts").reset_index(drop=True)
                    for s, g in index_bars.groupby("session")}
                   if index_bars is not None else {})
    out = []
    for sess, g in bars.groupby("session", sort=True):
        m = session_matrix(g, idx_by_sess.get(sess), step=step)
        if not m.empty:
            out.append(m)
    if not out:
        raise RuntimeError("no usable sessions")
    p = pd.concat(out, ignore_index=True)
    if verbose:
        feats = feature_names(p)
        print(f"    decision panel: {len(p):,} points across "
              f"{p['session'].nunique()} sessions, {len(feats)} features"
              f"{' (with index context)' if idx_by_sess else ' (NO index data)'}",
              flush=True)
    return p


NON_FEATURES = {"bar", "minute", "session", "entry_px", "close_px",
                "y_close", "y_30", "y_60", "y_d1", "y_d3", "fold"}


def feature_names(p: pd.DataFrame) -> list[str]:
    return [c for c in p.columns
            if c not in NON_FEATURES and p[c].dtype != object
            and not c.startswith("y_")]


def add_targets(p: pd.DataFrame, bars: pd.DataFrame, daily_close: dict,
                cost: float) -> pd.DataFrame:
    """Forward returns from the next bar's open, at several horizons."""
    d = p.copy()
    close_by_sess = bars.groupby("session")["close"]
    px = {s: g.to_numpy(float) for s, g in close_by_sess}

    d["y_close"] = d["close_px"] / d["entry_px"] - 1.0 - cost
    for k, tag in ((30, "y_30"), (60, "y_60")):
        vals = []
        for sess, b, e in zip(d["session"], d["bar"], d["entry_px"]):
            arr = px[sess]
            j = min(b + k, len(arr) - 1)
            vals.append(arr[j] / e - 1.0 - cost)
        d[tag] = vals
    for n, tag in ((1, "y_d1"), (3, "y_d3")):
        d[tag] = [daily_close.get((s, n), np.nan) / e - 1.0 - cost
                  for s, e in zip(d["session"], d["entry_px"])]
    return d
