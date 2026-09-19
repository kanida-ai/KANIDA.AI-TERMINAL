"""AUTO-GENERATED. The agent rewrites this file. Do not hand-edit.

Every function below was composed, written to disk, tested against the immutable
core, and kept because it improved validated performance. Functions that failed
were deleted. The docstring on each records when it was born and what it scored.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

EPS = 1e-12


def _w(a, w):
    return a[-w:] if len(a) >= w else a


MODEL_CFG = {
    "max_iter": 140,
    "max_depth": 7,
    "learning_rate": 0.06,
    "min_samples_leaf": 117,
    "l2_regularization": 1.6
}


def g_idx_streak_60(series: dict) -> float:
    """born gen 1"""
    a = _w(series["idx"], 60)
    if len(a) < 2:
        return 0.0
    return float(np.sum(np.sign(np.diff(a)) == np.sign(np.diff(a))[-1]) ) if len(a) > 2 else 0.0


def g_rng_z_5(series: dict) -> float:
    """born gen 1"""
    a = _w(series["rng"], 5)
    if len(a) < 2:
        return 0.0
    return float((a[-1] - a.mean()) / (a.std() + EPS))


def g_pos_pctile_15(series: dict) -> float:
    """born gen 1"""
    a = _w(series["pos"], 15)
    if len(a) < 2:
        return 0.0
    return float((a[:-1] < a[-1]).mean()) if len(a) > 1 else 0.5


def g_rel_z_60(series: dict) -> float:
    """born gen 1"""
    a = _w(series["rel"], 60)
    if len(a) < 2:
        return 0.0
    return float((a[-1] - a.mean()) / (a.std() + EPS))


def g_rng_z_30(series: dict) -> float:
    """born gen 1"""
    a = _w(series["rng"], 30)
    if len(a) < 2:
        return 0.0
    return float((a[-1] - a.mean()) / (a.std() + EPS))


def g_rel_frac_lo_30(series: dict) -> float:
    """born gen 1"""
    a = _w(series["rel"], 30)
    if len(a) < 2:
        return 0.0
    return float((a < np.median(a)).mean())


def g_pos_accel_3(series: dict) -> float:
    """born gen 1"""
    a = _w(series["pos"], 3)
    if len(a) < 2:
        return 0.0
    return float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0


def g_vwap_d_conc_10(series: dict) -> float:
    """born gen 1"""
    a = _w(series["vwap_d"], 10)
    if len(a) < 2:
        return 0.0
    return float(np.abs(a).max() / (np.abs(a).sum() + EPS))


def g_clv_streak_3(series: dict) -> float:
    """born gen 1"""
    a = _w(series["clv"], 3)
    if len(a) < 2:
        return 0.0
    return float(np.sum(np.sign(np.diff(a)) == np.sign(np.diff(a))[-1]) ) if len(a) > 2 else 0.0


def g_clv_frac_hi_90(series: dict) -> float:
    """born gen 1"""
    a = _w(series["clv"], 90)
    if len(a) < 2:
        return 0.0
    return float((a > np.median(a)).mean())


def g_vwap_d_frac_lo_3(series: dict) -> float:
    """born gen 1"""
    a = _w(series["vwap_d"], 3)
    if len(a) < 2:
        return 0.0
    return float((a < np.median(a)).mean())


def g_pos_frac_hi_5(series: dict) -> float:
    """born gen 1"""
    a = _w(series["pos"], 5)
    if len(a) < 2:
        return 0.0
    return float((a > np.median(a)).mean())


def g_rng_pctile_10(series: dict) -> float:
    """born gen 1"""
    a = _w(series["rng"], 10)
    if len(a) < 2:
        return 0.0
    return float((a[:-1] < a[-1]).mean()) if len(a) > 1 else 0.5


def g_rel_slope_5(series: dict) -> float:
    """born gen 1"""
    a = _w(series["rel"], 5)
    if len(a) < 2:
        return 0.0
    return float(np.polyfit(np.arange(len(a)), a, 1)[0]) if len(a) > 2 else 0.0


def g_ret_frac_lo_90(series: dict) -> float:
    """born gen 1"""
    a = _w(series["ret"], 90)
    if len(a) < 2:
        return 0.0
    return float((a < np.median(a)).mean())


def g_idx_slope_5(series: dict) -> float:
    """born gen 1"""
    a = _w(series["idx"], 5)
    if len(a) < 2:
        return 0.0
    return float(np.polyfit(np.arange(len(a)), a, 1)[0]) if len(a) > 2 else 0.0


def g_rng_frac_lo_60(series: dict) -> float:
    """born gen 1"""
    a = _w(series["rng"], 60)
    if len(a) < 2:
        return 0.0
    return float((a < np.median(a)).mean())


def g_vol_frac_hi_10(series: dict) -> float:
    """born gen 1"""
    a = _w(series["vol"], 10)
    if len(a) < 2:
        return 0.0
    return float((a > np.median(a)).mean())


def g_rel_accel_5(series: dict) -> float:
    """born gen 1"""
    a = _w(series["rel"], 5)
    if len(a) < 2:
        return 0.0
    return float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0


def g_vwap_d_std_15(series: dict) -> float:
    """born gen 1"""
    a = _w(series["vwap_d"], 15)
    if len(a) < 2:
        return 0.0
    return float(a.std())


def g_rel_streak_10(series: dict) -> float:
    """born gen 1"""
    a = _w(series["rel"], 10)
    if len(a) < 2:
        return 0.0
    return float(np.sum(np.sign(np.diff(a)) == np.sign(np.diff(a))[-1]) ) if len(a) > 2 else 0.0


def g_rel_frac_lo_90(series: dict) -> float:
    """born gen 1"""
    a = _w(series["rel"], 90)
    if len(a) < 2:
        return 0.0
    return float((a < np.median(a)).mean())


def g_vol_slope_60(series: dict) -> float:
    """born gen 1"""
    a = _w(series["vol"], 60)
    if len(a) < 2:
        return 0.0
    return float(np.polyfit(np.arange(len(a)), a, 1)[0]) if len(a) > 2 else 0.0


def g_vol_z_5(series: dict) -> float:
    """born gen 1"""
    a = _w(series["vol"], 5)
    if len(a) < 2:
        return 0.0
    return float((a[-1] - a.mean()) / (a.std() + EPS))


def g_vwap_d_conc_60(series: dict) -> float:
    """born gen 2"""
    a = _w(series["vwap_d"], 60)
    if len(a) < 2:
        return 0.0
    return float(np.abs(a).max() / (np.abs(a).sum() + EPS))


def g_pos_slope_90(series: dict) -> float:
    """born gen 2"""
    a = _w(series["pos"], 90)
    if len(a) < 2:
        return 0.0
    return float(np.polyfit(np.arange(len(a)), a, 1)[0]) if len(a) > 2 else 0.0


def g_pos_z_45(series: dict) -> float:
    """born gen 2"""
    a = _w(series["pos"], 45)
    if len(a) < 2:
        return 0.0
    return float((a[-1] - a.mean()) / (a.std() + EPS))


def g_clv_accel_45(series: dict) -> float:
    """born gen 2"""
    a = _w(series["clv"], 45)
    if len(a) < 2:
        return 0.0
    return float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0


def g_rng_z_10(series: dict) -> float:
    """born gen 2"""
    a = _w(series["rng"], 10)
    if len(a) < 2:
        return 0.0
    return float((a[-1] - a.mean()) / (a.std() + EPS))


def g_rng_pctile_60(series: dict) -> float:
    """born gen 2"""
    a = _w(series["rng"], 60)
    if len(a) < 2:
        return 0.0
    return float((a[:-1] < a[-1]).mean()) if len(a) > 1 else 0.5


def g_ret_ratio_15(series: dict) -> float:
    """born gen 2"""
    a = _w(series["ret"], 15)
    if len(a) < 2:
        return 0.0
    return float(a[-1] / (np.abs(a).mean() + EPS))


def g_ret_frac_hi_15(series: dict) -> float:
    """born gen 2"""
    a = _w(series["ret"], 15)
    if len(a) < 2:
        return 0.0
    return float((a > np.median(a)).mean())


def g_ret_conc_10(series: dict) -> float:
    """born gen 2"""
    a = _w(series["ret"], 10)
    if len(a) < 2:
        return 0.0
    return float(np.abs(a).max() / (np.abs(a).sum() + EPS))


def g_rng_pctile_45(series: dict) -> float:
    """born gen 2"""
    a = _w(series["rng"], 45)
    if len(a) < 2:
        return 0.0
    return float((a[:-1] < a[-1]).mean()) if len(a) > 1 else 0.5


def g_idx_conc_30(series: dict) -> float:
    """born gen 2"""
    a = _w(series["idx"], 30)
    if len(a) < 2:
        return 0.0
    return float(np.abs(a).max() / (np.abs(a).sum() + EPS))


def g_vol_z_45(series: dict) -> float:
    """born gen 2"""
    a = _w(series["vol"], 45)
    if len(a) < 2:
        return 0.0
    return float((a[-1] - a.mean()) / (a.std() + EPS))


def g_atp_ratio_5(series: dict) -> float:
    """born gen 2"""
    a = _w(series["atp"], 5)
    if len(a) < 2:
        return 0.0
    return float(a[-1] / (np.abs(a).mean() + EPS))


def g_vol_frac_lo_3(series: dict) -> float:
    """born gen 2"""
    a = _w(series["vol"], 3)
    if len(a) < 2:
        return 0.0
    return float((a < np.median(a)).mean())


def g_rng_frac_hi_3(series: dict) -> float:
    """born gen 2"""
    a = _w(series["rng"], 3)
    if len(a) < 2:
        return 0.0
    return float((a > np.median(a)).mean())


def g_vwap_d_accel_3(series: dict) -> float:
    """born gen 2"""
    a = _w(series["vwap_d"], 3)
    if len(a) < 2:
        return 0.0
    return float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0


def g_pos_conc_3(series: dict) -> float:
    """born gen 2"""
    a = _w(series["pos"], 3)
    if len(a) < 2:
        return 0.0
    return float(np.abs(a).max() / (np.abs(a).sum() + EPS))


def g_vwap_d_frac_hi_15(series: dict) -> float:
    """born gen 2"""
    a = _w(series["vwap_d"], 15)
    if len(a) < 2:
        return 0.0
    return float((a > np.median(a)).mean())


def g_rng_conc_3(series: dict) -> float:
    """born gen 2"""
    a = _w(series["rng"], 3)
    if len(a) < 2:
        return 0.0
    return float(np.abs(a).max() / (np.abs(a).sum() + EPS))


def g_vol_conc_45(series: dict) -> float:
    """born gen 2"""
    a = _w(series["vol"], 45)
    if len(a) < 2:
        return 0.0
    return float(np.abs(a).max() / (np.abs(a).sum() + EPS))


def g_atp_streak_45(series: dict) -> float:
    """born gen 2"""
    a = _w(series["atp"], 45)
    if len(a) < 2:
        return 0.0
    return float(np.sum(np.sign(np.diff(a)) == np.sign(np.diff(a))[-1]) ) if len(a) > 2 else 0.0


def g_idx_z_10(series: dict) -> float:
    """born gen 2"""
    a = _w(series["idx"], 10)
    if len(a) < 2:
        return 0.0
    return float((a[-1] - a.mean()) / (a.std() + EPS))


def g_clv_frac_lo_10(series: dict) -> float:
    """born gen 2"""
    a = _w(series["clv"], 10)
    if len(a) < 2:
        return 0.0
    return float((a < np.median(a)).mean())


def g_idx_accel_60(series: dict) -> float:
    """born gen 2"""
    a = _w(series["idx"], 60)
    if len(a) < 2:
        return 0.0
    return float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0


def g_idx_accel_45(series: dict) -> float:
    """born gen 9"""
    a = _w(series["idx"], 45)
    if len(a) < 2:
        return 0.0
    return float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0


def g_atp_frac_lo_90(series: dict) -> float:
    """born gen 9"""
    a = _w(series["atp"], 90)
    if len(a) < 2:
        return 0.0
    return float((a < np.median(a)).mean())


def g_atp_std_30(series: dict) -> float:
    """born gen 9"""
    a = _w(series["atp"], 30)
    if len(a) < 2:
        return 0.0
    return float(a.std())


def g_rel_streak_3(series: dict) -> float:
    """born gen 9"""
    a = _w(series["rel"], 3)
    if len(a) < 2:
        return 0.0
    return float(np.sum(np.sign(np.diff(a)) == np.sign(np.diff(a))[-1]) ) if len(a) > 2 else 0.0


def g_rng_accel_3(series: dict) -> float:
    """born gen 9"""
    a = _w(series["rng"], 3)
    if len(a) < 2:
        return 0.0
    return float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0


def g_vol_z_3(series: dict) -> float:
    """born gen 9"""
    a = _w(series["vol"], 3)
    if len(a) < 2:
        return 0.0
    return float((a[-1] - a.mean()) / (a.std() + EPS))


def g_ret_conc_3(series: dict) -> float:
    """born gen 9"""
    a = _w(series["ret"], 3)
    if len(a) < 2:
        return 0.0
    return float(np.abs(a).max() / (np.abs(a).sum() + EPS))


def g_vol_ratio_3(series: dict) -> float:
    """born gen 9"""
    a = _w(series["vol"], 3)
    if len(a) < 2:
        return 0.0
    return float(a[-1] / (np.abs(a).mean() + EPS))


def g_vol_ratio_60(series: dict) -> float:
    """born gen 9"""
    a = _w(series["vol"], 60)
    if len(a) < 2:
        return 0.0
    return float(a[-1] / (np.abs(a).mean() + EPS))


def g_rel_frac_lo_10(series: dict) -> float:
    """born gen 9"""
    a = _w(series["rel"], 10)
    if len(a) < 2:
        return 0.0
    return float((a < np.median(a)).mean())


def g_idx_accel_3(series: dict) -> float:
    """born gen 9"""
    a = _w(series["idx"], 3)
    if len(a) < 2:
        return 0.0
    return float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0


def g_pos_frac_lo_60(series: dict) -> float:
    """born gen 9"""
    a = _w(series["pos"], 60)
    if len(a) < 2:
        return 0.0
    return float((a < np.median(a)).mean())


def g_pos_frac_hi_45(series: dict) -> float:
    """born gen 9"""
    a = _w(series["pos"], 45)
    if len(a) < 2:
        return 0.0
    return float((a > np.median(a)).mean())


def g_rel_frac_hi_15(series: dict) -> float:
    """born gen 9"""
    a = _w(series["rel"], 15)
    if len(a) < 2:
        return 0.0
    return float((a > np.median(a)).mean())


def g_atp_slope_45(series: dict) -> float:
    """born gen 9"""
    a = _w(series["atp"], 45)
    if len(a) < 2:
        return 0.0
    return float(np.polyfit(np.arange(len(a)), a, 1)[0]) if len(a) > 2 else 0.0


def g_atp_std_3(series: dict) -> float:
    """born gen 9"""
    a = _w(series["atp"], 3)
    if len(a) < 2:
        return 0.0
    return float(a.std())


def g_pos_z_30(series: dict) -> float:
    """born gen 9"""
    a = _w(series["pos"], 30)
    if len(a) < 2:
        return 0.0
    return float((a[-1] - a.mean()) / (a.std() + EPS))


def g_vwap_d_conc_3(series: dict) -> float:
    """born gen 9"""
    a = _w(series["vwap_d"], 3)
    if len(a) < 2:
        return 0.0
    return float(np.abs(a).max() / (np.abs(a).sum() + EPS))


def g_vwap_d_slope_5(series: dict) -> float:
    """born gen 9"""
    a = _w(series["vwap_d"], 5)
    if len(a) < 2:
        return 0.0
    return float(np.polyfit(np.arange(len(a)), a, 1)[0]) if len(a) > 2 else 0.0


def g_vwap_d_accel_45(series: dict) -> float:
    """born gen 9"""
    a = _w(series["vwap_d"], 45)
    if len(a) < 2:
        return 0.0
    return float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0


def g_vwap_d_std_90(series: dict) -> float:
    """born gen 9"""
    a = _w(series["vwap_d"], 90)
    if len(a) < 2:
        return 0.0
    return float(a.std())


def g_idx_std_90(series: dict) -> float:
    """born gen 9"""
    a = _w(series["idx"], 90)
    if len(a) < 2:
        return 0.0
    return float(a.std())


def g_rng_streak_90(series: dict) -> float:
    """born gen 9"""
    a = _w(series["rng"], 90)
    if len(a) < 2:
        return 0.0
    return float(np.sum(np.sign(np.diff(a)) == np.sign(np.diff(a))[-1]) ) if len(a) > 2 else 0.0


def g_vwap_d_accel_30(series: dict) -> float:
    """born gen 9"""
    a = _w(series["vwap_d"], 30)
    if len(a) < 2:
        return 0.0
    return float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0


def g_idx_std_3(series: dict) -> float:
    """born gen 27"""
    a = _w(series["idx"], 3)
    if len(a) < 2:
        return 0.0
    return float(a.std())


def g_rel_ratio_3(series: dict) -> float:
    """born gen 27"""
    a = _w(series["rel"], 3)
    if len(a) < 2:
        return 0.0
    return float(a[-1] / (np.abs(a).mean() + EPS))


def g_pos_accel_45(series: dict) -> float:
    """born gen 27"""
    a = _w(series["pos"], 45)
    if len(a) < 2:
        return 0.0
    return float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0


def g_atp_slope_30(series: dict) -> float:
    """born gen 27"""
    a = _w(series["atp"], 30)
    if len(a) < 2:
        return 0.0
    return float(np.polyfit(np.arange(len(a)), a, 1)[0]) if len(a) > 2 else 0.0


def g_ret_frac_hi_10(series: dict) -> float:
    """born gen 27"""
    a = _w(series["ret"], 10)
    if len(a) < 2:
        return 0.0
    return float((a > np.median(a)).mean())


def g_clv_pctile_3(series: dict) -> float:
    """born gen 27"""
    a = _w(series["clv"], 3)
    if len(a) < 2:
        return 0.0
    return float((a[:-1] < a[-1]).mean()) if len(a) > 1 else 0.5


def g_vwap_d_frac_hi_90(series: dict) -> float:
    """born gen 27"""
    a = _w(series["vwap_d"], 90)
    if len(a) < 2:
        return 0.0
    return float((a > np.median(a)).mean())


def g_vwap_d_conc_45(series: dict) -> float:
    """born gen 27"""
    a = _w(series["vwap_d"], 45)
    if len(a) < 2:
        return 0.0
    return float(np.abs(a).max() / (np.abs(a).sum() + EPS))


def g_clv_conc_45(series: dict) -> float:
    """born gen 27"""
    a = _w(series["clv"], 45)
    if len(a) < 2:
        return 0.0
    return float(np.abs(a).max() / (np.abs(a).sum() + EPS))


def g_clv_pctile_30(series: dict) -> float:
    """born gen 27"""
    a = _w(series["clv"], 30)
    if len(a) < 2:
        return 0.0
    return float((a[:-1] < a[-1]).mean()) if len(a) > 1 else 0.5


def g_pos_pctile_5(series: dict) -> float:
    """born gen 27"""
    a = _w(series["pos"], 5)
    if len(a) < 2:
        return 0.0
    return float((a[:-1] < a[-1]).mean()) if len(a) > 1 else 0.5


def g_pos_z_5(series: dict) -> float:
    """born gen 27"""
    a = _w(series["pos"], 5)
    if len(a) < 2:
        return 0.0
    return float((a[-1] - a.mean()) / (a.std() + EPS))


def g_atp_z_3(series: dict) -> float:
    """born gen 27"""
    a = _w(series["atp"], 3)
    if len(a) < 2:
        return 0.0
    return float((a[-1] - a.mean()) / (a.std() + EPS))


def g_pos_accel_10(series: dict) -> float:
    """born gen 27"""
    a = _w(series["pos"], 10)
    if len(a) < 2:
        return 0.0
    return float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0


def g_rng_ratio_15(series: dict) -> float:
    """born gen 27"""
    a = _w(series["rng"], 15)
    if len(a) < 2:
        return 0.0
    return float(a[-1] / (np.abs(a).mean() + EPS))


def g_rng_frac_hi_5(series: dict) -> float:
    """born gen 27"""
    a = _w(series["rng"], 5)
    if len(a) < 2:
        return 0.0
    return float((a > np.median(a)).mean())


def g_ret_std_30(series: dict) -> float:
    """born gen 27"""
    a = _w(series["ret"], 30)
    if len(a) < 2:
        return 0.0
    return float(a.std())


def g_vol_frac_hi_5(series: dict) -> float:
    """born gen 27"""
    a = _w(series["vol"], 5)
    if len(a) < 2:
        return 0.0
    return float((a > np.median(a)).mean())


def g_clv_streak_90(series: dict) -> float:
    """born gen 27"""
    a = _w(series["clv"], 90)
    if len(a) < 2:
        return 0.0
    return float(np.sum(np.sign(np.diff(a)) == np.sign(np.diff(a))[-1]) ) if len(a) > 2 else 0.0


def g_rng_std_90(series: dict) -> float:
    """born gen 27"""
    a = _w(series["rng"], 90)
    if len(a) < 2:
        return 0.0
    return float(a.std())


def g_pos_accel_15(series: dict) -> float:
    """born gen 27"""
    a = _w(series["pos"], 15)
    if len(a) < 2:
        return 0.0
    return float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0


def g_ret_pctile_5(series: dict) -> float:
    """born gen 27"""
    a = _w(series["ret"], 5)
    if len(a) < 2:
        return 0.0
    return float((a[:-1] < a[-1]).mean()) if len(a) > 1 else 0.5


def g_rng_accel_45(series: dict) -> float:
    """born gen 27"""
    a = _w(series["rng"], 45)
    if len(a) < 2:
        return 0.0
    return float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0


def g_vol_streak_3(series: dict) -> float:
    """born gen 27"""
    a = _w(series["vol"], 3)
    if len(a) < 2:
        return 0.0
    return float(np.sum(np.sign(np.diff(a)) == np.sign(np.diff(a))[-1]) ) if len(a) > 2 else 0.0


GENES = [
    ("g_idx_streak_60", g_idx_streak_60, "idx", "streak", 60),
    ("g_rng_z_5", g_rng_z_5, "rng", "z", 5),
    ("g_pos_pctile_15", g_pos_pctile_15, "pos", "pctile", 15),
    ("g_rel_z_60", g_rel_z_60, "rel", "z", 60),
    ("g_rng_z_30", g_rng_z_30, "rng", "z", 30),
    ("g_rel_frac_lo_30", g_rel_frac_lo_30, "rel", "frac_lo", 30),
    ("g_pos_accel_3", g_pos_accel_3, "pos", "accel", 3),
    ("g_vwap_d_conc_10", g_vwap_d_conc_10, "vwap_d", "conc", 10),
    ("g_clv_streak_3", g_clv_streak_3, "clv", "streak", 3),
    ("g_clv_frac_hi_90", g_clv_frac_hi_90, "clv", "frac_hi", 90),
    ("g_vwap_d_frac_lo_3", g_vwap_d_frac_lo_3, "vwap_d", "frac_lo", 3),
    ("g_pos_frac_hi_5", g_pos_frac_hi_5, "pos", "frac_hi", 5),
    ("g_rng_pctile_10", g_rng_pctile_10, "rng", "pctile", 10),
    ("g_rel_slope_5", g_rel_slope_5, "rel", "slope", 5),
    ("g_ret_frac_lo_90", g_ret_frac_lo_90, "ret", "frac_lo", 90),
    ("g_idx_slope_5", g_idx_slope_5, "idx", "slope", 5),
    ("g_rng_frac_lo_60", g_rng_frac_lo_60, "rng", "frac_lo", 60),
    ("g_vol_frac_hi_10", g_vol_frac_hi_10, "vol", "frac_hi", 10),
    ("g_rel_accel_5", g_rel_accel_5, "rel", "accel", 5),
    ("g_vwap_d_std_15", g_vwap_d_std_15, "vwap_d", "std", 15),
    ("g_rel_streak_10", g_rel_streak_10, "rel", "streak", 10),
    ("g_rel_frac_lo_90", g_rel_frac_lo_90, "rel", "frac_lo", 90),
    ("g_vol_slope_60", g_vol_slope_60, "vol", "slope", 60),
    ("g_vol_z_5", g_vol_z_5, "vol", "z", 5),
    ("g_vwap_d_conc_60", g_vwap_d_conc_60, "vwap_d", "conc", 60),
    ("g_pos_slope_90", g_pos_slope_90, "pos", "slope", 90),
    ("g_pos_z_45", g_pos_z_45, "pos", "z", 45),
    ("g_clv_accel_45", g_clv_accel_45, "clv", "accel", 45),
    ("g_rng_z_10", g_rng_z_10, "rng", "z", 10),
    ("g_rng_pctile_60", g_rng_pctile_60, "rng", "pctile", 60),
    ("g_ret_ratio_15", g_ret_ratio_15, "ret", "ratio", 15),
    ("g_ret_frac_hi_15", g_ret_frac_hi_15, "ret", "frac_hi", 15),
    ("g_ret_conc_10", g_ret_conc_10, "ret", "conc", 10),
    ("g_rng_pctile_45", g_rng_pctile_45, "rng", "pctile", 45),
    ("g_idx_conc_30", g_idx_conc_30, "idx", "conc", 30),
    ("g_vol_z_45", g_vol_z_45, "vol", "z", 45),
    ("g_atp_ratio_5", g_atp_ratio_5, "atp", "ratio", 5),
    ("g_vol_frac_lo_3", g_vol_frac_lo_3, "vol", "frac_lo", 3),
    ("g_rng_frac_hi_3", g_rng_frac_hi_3, "rng", "frac_hi", 3),
    ("g_vwap_d_accel_3", g_vwap_d_accel_3, "vwap_d", "accel", 3),
    ("g_pos_conc_3", g_pos_conc_3, "pos", "conc", 3),
    ("g_vwap_d_frac_hi_15", g_vwap_d_frac_hi_15, "vwap_d", "frac_hi", 15),
    ("g_rng_conc_3", g_rng_conc_3, "rng", "conc", 3),
    ("g_vol_conc_45", g_vol_conc_45, "vol", "conc", 45),
    ("g_atp_streak_45", g_atp_streak_45, "atp", "streak", 45),
    ("g_idx_z_10", g_idx_z_10, "idx", "z", 10),
    ("g_clv_frac_lo_10", g_clv_frac_lo_10, "clv", "frac_lo", 10),
    ("g_idx_accel_60", g_idx_accel_60, "idx", "accel", 60),
    ("g_idx_accel_45", g_idx_accel_45, "idx", "accel", 45),
    ("g_atp_frac_lo_90", g_atp_frac_lo_90, "atp", "frac_lo", 90),
    ("g_atp_std_30", g_atp_std_30, "atp", "std", 30),
    ("g_rel_streak_3", g_rel_streak_3, "rel", "streak", 3),
    ("g_rng_accel_3", g_rng_accel_3, "rng", "accel", 3),
    ("g_vol_z_3", g_vol_z_3, "vol", "z", 3),
    ("g_ret_conc_3", g_ret_conc_3, "ret", "conc", 3),
    ("g_vol_ratio_3", g_vol_ratio_3, "vol", "ratio", 3),
    ("g_vol_ratio_60", g_vol_ratio_60, "vol", "ratio", 60),
    ("g_rel_frac_lo_10", g_rel_frac_lo_10, "rel", "frac_lo", 10),
    ("g_idx_accel_3", g_idx_accel_3, "idx", "accel", 3),
    ("g_pos_frac_lo_60", g_pos_frac_lo_60, "pos", "frac_lo", 60),
    ("g_pos_frac_hi_45", g_pos_frac_hi_45, "pos", "frac_hi", 45),
    ("g_rel_frac_hi_15", g_rel_frac_hi_15, "rel", "frac_hi", 15),
    ("g_atp_slope_45", g_atp_slope_45, "atp", "slope", 45),
    ("g_atp_std_3", g_atp_std_3, "atp", "std", 3),
    ("g_pos_z_30", g_pos_z_30, "pos", "z", 30),
    ("g_vwap_d_conc_3", g_vwap_d_conc_3, "vwap_d", "conc", 3),
    ("g_vwap_d_slope_5", g_vwap_d_slope_5, "vwap_d", "slope", 5),
    ("g_vwap_d_accel_45", g_vwap_d_accel_45, "vwap_d", "accel", 45),
    ("g_vwap_d_std_90", g_vwap_d_std_90, "vwap_d", "std", 90),
    ("g_idx_std_90", g_idx_std_90, "idx", "std", 90),
    ("g_rng_streak_90", g_rng_streak_90, "rng", "streak", 90),
    ("g_vwap_d_accel_30", g_vwap_d_accel_30, "vwap_d", "accel", 30),
    ("g_idx_std_3", g_idx_std_3, "idx", "std", 3),
    ("g_rel_ratio_3", g_rel_ratio_3, "rel", "ratio", 3),
    ("g_pos_accel_45", g_pos_accel_45, "pos", "accel", 45),
    ("g_atp_slope_30", g_atp_slope_30, "atp", "slope", 30),
    ("g_ret_frac_hi_10", g_ret_frac_hi_10, "ret", "frac_hi", 10),
    ("g_clv_pctile_3", g_clv_pctile_3, "clv", "pctile", 3),
    ("g_vwap_d_frac_hi_90", g_vwap_d_frac_hi_90, "vwap_d", "frac_hi", 90),
    ("g_vwap_d_conc_45", g_vwap_d_conc_45, "vwap_d", "conc", 45),
    ("g_clv_conc_45", g_clv_conc_45, "clv", "conc", 45),
    ("g_clv_pctile_30", g_clv_pctile_30, "clv", "pctile", 30),
    ("g_pos_pctile_5", g_pos_pctile_5, "pos", "pctile", 5),
    ("g_pos_z_5", g_pos_z_5, "pos", "z", 5),
    ("g_atp_z_3", g_atp_z_3, "atp", "z", 3),
    ("g_pos_accel_10", g_pos_accel_10, "pos", "accel", 10),
    ("g_rng_ratio_15", g_rng_ratio_15, "rng", "ratio", 15),
    ("g_rng_frac_hi_5", g_rng_frac_hi_5, "rng", "frac_hi", 5),
    ("g_ret_std_30", g_ret_std_30, "ret", "std", 30),
    ("g_vol_frac_hi_5", g_vol_frac_hi_5, "vol", "frac_hi", 5),
    ("g_clv_streak_90", g_clv_streak_90, "clv", "streak", 90),
    ("g_rng_std_90", g_rng_std_90, "rng", "std", 90),
    ("g_pos_accel_15", g_pos_accel_15, "pos", "accel", 15),
    ("g_ret_pctile_5", g_ret_pctile_5, "ret", "pctile", 5),
    ("g_rng_accel_45", g_rng_accel_45, "rng", "accel", 45),
    ("g_vol_streak_3", g_vol_streak_3, "vol", "streak", 3),
]
