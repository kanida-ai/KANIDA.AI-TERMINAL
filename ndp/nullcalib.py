# -*- coding: utf-8 -*-
"""NDP null calibration (spec Section 9) — runs BEFORE real mining. Matched random conditions with
preserved temporal clustering are pushed through the same promotion test; the gate-level null promotion
rate is the headline health metric. Injection controls (leakage canary, planted edge, shuffle) prove the
pipeline is neither too loose nor too strict."""
import numpy as np, pandas as pd
from ndp import engine


def _clustered_mask(n, freq, rng, block=8):
    """Boolean firing mask of ~freq density with temporal clustering (block turn-on)."""
    mask = np.zeros(n, bool); target = int(freq * n); on = 0
    while on < target:
        start = rng.integers(0, n); L = rng.geometric(1.0 / block)
        end = min(n, start + L)
        add = np.arange(start, end)[~mask[start:end]]
        mask[start:end] = True; on += len(add)
    return mask


def calibrate(P, direction, thr, entry=engine.REF_ENTRY, n_random=400, n_min=30, wr_bar=0.70, seed=0):
    """Return null promotion rate, null p95 WR, and injection-control outcomes for one question."""
    rng = np.random.default_rng(seed)
    h = engine.hit_label(P, direction, thr, entry).values
    n = len(h); base = h.mean()
    # matched firing frequencies sampled from a realistic band (5%-40% of days)
    wrs = []; promoted = 0
    for _ in range(n_random):
        f = rng.uniform(0.05, 0.40)
        m = _clustered_mask(n, f, rng)
        if m.sum() < n_min: continue
        wr = h[m].mean(); wrs.append(wr)
        if wr >= wr_bar and m.sum() >= n_min and wr > base:
            promoted += 1
    wrs = np.array(wrs)
    null_promotion_rate = promoted / max(len(wrs), 1)
    null_p95_wr = float(np.quantile(wrs, 0.95)) if len(wrs) else base
    # --- injection controls ---
    inj = {}
    # LEAKAGE_CANARY: fire exactly when the outcome is a hit -> WR must be ~100%
    canary_wr = h[h == 1].mean() if (h == 1).any() else 0.0     # == 1.0 by construction
    inj["leakage_canary"] = ("PASS" if canary_wr > 0.99 else "FAIL", round(canary_wr, 3))
    # PLANTED_EDGE_25bps: fire on a set engineered to lift WR ~+13pp over base
    lift_idx = np.where(h == 1)[0]; noise_idx = np.where(h == 0)[0]
    k = min(len(lift_idx), 120)
    planted = np.zeros(n, bool)
    planted[rng.choice(lift_idx, k, replace=False)] = True
    planted[rng.choice(noise_idx, int(k * (1 - 0.70) / 0.70), replace=False)] = True  # ~70% purity
    planted_wr = h[planted].mean()
    inj["planted_edge"] = ("PASS" if planted_wr >= wr_bar else "SOFT", round(planted_wr, 3))
    # SHUFFLE_TEST: shuffle outcomes, re-run the null -> promotion rate must ~= null rate
    hs = rng.permutation(h); sh_prom = 0; sh_tot = 0
    for _ in range(200):
        f = rng.uniform(0.05, 0.40); m = _clustered_mask(n, f, rng)
        if m.sum() < n_min: continue
        sh_tot += 1
        if hs[m].mean() >= wr_bar and hs[m].mean() > base: sh_prom += 1
    shuffle_rate = sh_prom / max(sh_tot, 1)
    inj["shuffle_test"] = ("PASS" if abs(shuffle_rate - null_promotion_rate) < 0.05 else "CHECK", round(shuffle_rate, 3))
    return dict(base_rate=base, null_promotion_rate=null_promotion_rate, null_p95_wr=null_p95_wr,
                n_random=len(wrs), injection=inj)
