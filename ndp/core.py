# -*- coding: utf-8 -*-
"""NDP core — the ONE implementation of returns/MFE/MAE (REQ-SCI-002), the cost model (CostModelV1),
the stationary block bootstrap (REQ-SCI-006), MDE/power (REQ-POW-002), and the three-part + WR gate
(REQ-GATE-001 / Addendum A). Pure functions, no I/O — unit-testable.
"""
import numpy as np

# ------------------------------------------------------------------ returns
def r_gross(direction, p_entry, p_exit):
    """Signed gross return; entry price is the denominator for BOTH long and short (REQ-SCI-001)."""
    d = 1.0 if direction == "LONG" else -1.0
    return d * (p_exit - p_entry) / p_entry

def mfe_mae(direction, p_entry, path_high, path_low):
    """Max favourable / adverse excursion on the entry denominator."""
    d = 1.0 if direction == "LONG" else -1.0
    fav = d * (path_high - p_entry) / p_entry if d > 0 else d * (path_low - p_entry) / p_entry
    adv = d * (path_low - p_entry) / p_entry if d > 0 else d * (path_high - p_entry) / p_entry
    return fav, adv

# ------------------------------------------------------------------ costs (CostModelV1)
COST_MODEL_V1 = {"version": "CostModelV1", "last_verified": "2026-07-01",
                 "mis": {"brokerage_pct": 0.03, "stt_sell_pct": 0.025, "exch_pct": 0.00297,
                         "stamp_buy_pct": 0.003, "sebi_per_cr": 10.0, "gst_pct": 18.0},
                 "cnc": {"brokerage_pct": 0.0, "stt_both_pct": 0.10, "exch_pct": 0.00297,
                         "stamp_buy_pct": 0.015, "sebi_per_cr": 10.0, "gst_pct": 18.0},
                 "fut": {"brokerage_pct": 0.0, "stt_sell_pct": 0.02, "exch_pct": 0.00173,
                         "stamp_buy_pct": 0.002, "sebi_per_cr": 10.0, "gst_pct": 18.0}}

def cost_pct(product, entry_time, first_min_range_pct, slip_mult=1.0):
    """Round-trip cost as % of turnover, INCLUDING a time-of-day-dependent slippage (REQ-COST-003/004).
    Slippage is a function of entry time and the first-minute range (spread proxy) — never a constant."""
    m = COST_MODEL_V1[product.lower()]
    fixed = m.get("brokerage_pct", 0.0) * 2 + m["exch_pct"] * 2 + m["stamp_buy_pct"]
    stt = m["stt_both_pct"] * 2 if "stt_both_pct" in m else m.get("stt_sell_pct", 0.0)  # CNC STT on BOTH sides
    gst = (m.get("brokerage_pct", 0.0) * 2 + m["exch_pct"] * 2) * m["gst_pct"] / 100.0
    # slippage: base + half-spread proxy, worse the earlier the entry
    tod = {"09:15": 1.6, "09:20": 1.0, "09:30": 0.8, "09:45": 0.7, "10:00": 0.65}.get(entry_time, 1.0)
    slip = (0.010 + 0.25 * max(first_min_range_pct, 0.0)) * tod
    return (fixed + stt + gst + slip * slip_mult)

# ------------------------------------------------------------------ block bootstrap (Politis-Romano)
def block_bootstrap_lcb(x, alpha=0.05, n_boot=2000, block=None, seed=0):
    """One-sided lower confidence bound on the mean via stationary block bootstrap (REQ-SCI-006).
    Geometric block lengths (expected length `block`). IID bootstrap is forbidden in the gate path."""
    x = np.asarray(x, float); x = x[~np.isnan(x)]
    n = len(x)
    if n < 3: return np.nan
    if block is None: block = max(2, int(round(n ** (1/3))))
    rng = np.random.default_rng(seed)
    p = 1.0 / block
    means = np.empty(n_boot)
    for b in range(n_boot):
        idx = np.empty(n, int); i = 0
        while i < n:
            start = rng.integers(0, n); L = rng.geometric(p)
            for k in range(L):
                if i >= n: break
                idx[i] = (start + k) % n; i += 1
        means[b] = x[idx].mean()
    return float(np.quantile(means, alpha))

def n_effective(n_total, avg_overlap):
    return n_total / max(1.0, avg_overlap)

# ------------------------------------------------------------------ power / MDE
def mde_80(return_sd, n_eff, alpha=0.05):
    """Minimum detectable |Delta_mu| at 80% power (bps)."""
    z = 1.2816 + 1.6449   # z_0.8 + z_(1-alpha)
    return z * return_sd / np.sqrt(max(n_eff, 1e-9)) * 1e4

def wr_p_chance(wins, n, base_rate):
    """One-sided binomial tail: P(observe >= wins successes | p = base_rate)."""
    from math import comb
    if n == 0: return 1.0
    p = base_rate
    return float(sum(comb(n, k) * p**k * (1-p)**(n-k) for k in range(wins, n+1)))

def wr_detectable_n(base_rate, target=0.70, power=0.80, alpha=0.05):
    """Trades needed to distinguish target WR from base at given power (proportion test)."""
    from math import sqrt
    za, zb = 1.6449, 0.8416
    p0, p1 = base_rate, target
    if p1 <= p0: return np.inf
    return ((za*sqrt(p0*(1-p0)) + zb*sqrt(p1*(1-p1)))**2) / (p1 - p0)**2

# ------------------------------------------------------------------ the gate
def gate(g, cfg):
    """Three-part scientific gate (REQ-GATE-001) fused with Addendum A's WR>=70% primary bar.
    `g` is a dict of computed components. Returns (tier, gate_result, reasons)."""
    reasons = []
    # --- Addendum A primary: win rate ---
    wr = g["wr_oos"]; base = g["base_rate"]; neff = g["n_eff"]
    deploy_wr = wr >= cfg["wr_bar"] and neff >= cfg["n_min"] and wr > base and wr > g.get("null_p95_wr", 0.0)
    # --- scientific validity guards (subordinate; they qualify, never replace, the WR) ---
    guards_ok = (g["mu_c_lcb"] > 0 and g["delta_mu_lcb"] > 0 and g["delta_p_lcb"] > 0)
    if wr < base: reasons.append("WR_below_base")
    if neff < cfg["n_min"]: reasons.append("insufficient_n")
    if wr <= g.get("null_p95_wr", 0.0): reasons.append("not_above_null")
    if g["trades_per_year"] < cfg["f_min"]: reasons.append("too_infrequent")
    if not g.get("frozen_before_eval", True): reasons.append("not_frozen")
    if not g.get("stress_2x_passed", True): reasons.append("fragile_to_costs")
    # --- tiering (Addendum A A3.1) ---
    if deploy_wr and neff >= cfg["n_min"] and g["trades_per_year"] >= cfg["f_min"] \
            and g.get("frozen_before_eval", True) and g.get("stress_2x_passed", True):
        tier = "DEPLOY"; result = "PROMOTED"
        if not guards_ok: reasons.append("WR_ok_but_expectancy_guard_soft")
    elif wr > base and wr > g.get("null_p95_wr", 0.0):
        tier = "TRACK"; result = "INSUFFICIENT_EVIDENCE"
    else:
        tier = "DISCARD"; result = "NO_EDGE"
    return tier, result, reasons
