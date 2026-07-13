"""Falcon Tesla — v3 Difference-in-Differences (DiD) entry-selection layer.

VENDORED, byte-for-byte port of the DiD logic in the operator's batch reference:

    C:\\Users\\SPS\\Documents\\Kanida Ai\\outputs\\falcon_tesla\\falcons_tesla_short_engine_v3_did.py
        (regular_session_mask, add_regular_session_pre_mean, add_did_features,
         grade_v3)

WHAT THIS IS: a pure POST-PROCESSING layer on the v2 scored+graded frame. v3 does
NOT replace v2 — it reads the v2 score universe (every column it consumes is
already produced by tesla_short_engine.grade_scored_frame) and adds:
  * a 1-minute DiD feature block (stock minute state minus sector/market/NIFTY
    state at the same minute + a pre/post 5-minute DiD vs the same benchmarks),
  * `did_score` / `did_exhaustion_risk`,
  * a stricter short gate `v3_did_gate` → regrades to A++/A+++ (`v3_grade`),
  * `v3_setup` and `v3_rank_score` (the seat rank key when the layer is on).

DELIBERATELY NOT PORTED: the batch `add_pnl` / `summarize` — they use
forward-looking `fwd_Nm_bps` and are RESEARCH-ONLY. The live engine never touches
forward bars for entry selection.

Do NOT "improve" the math here — its whole purpose is to reproduce the batch DiD
frame EXACTLY (see tests/autotrade/test_tesla_did_v3.py, which asserts byte-for-
byte parity against the batch's own output CSVs). If the research math changes,
re-port it here deliberately and re-run parity.

The `pre5_*` rolling means are computed on the FULL intraday minute series per
(instrument, day) — the live scored/graded frame carries that full series before
the latest-minute collapse in compute_live_signals, so this layer is computable
at signal time (see tesla_short_engine.grade_scored_frame `did_layer_enabled`).

Attribution: falcons_tesla_short_engine_v3_did.py (Falcon's Tesla DiD prototype).
"""
from __future__ import annotations

import numpy as np
import pandas as pd


# ── pre/post 5-minute rolling means (verbatim from the batch) ────────────────

def regular_session_mask(df: pd.DataFrame) -> pd.Series:
    return df["time"].between("09:15", "15:30")


def add_regular_session_pre_mean(
    df: pd.DataFrame,
    source_col: str,
    out_col: str,
    window: int = 5,
    min_periods: int = 3,
) -> pd.DataFrame:
    out = df.copy()
    out[out_col] = np.nan
    mask = regular_session_mask(out)
    out.loc[mask, out_col] = (
        out.loc[mask]
        .groupby(["instrument", "day"], sort=False)[source_col]
        .transform(lambda s: s.shift(1).rolling(window, min_periods=min_periods).mean())
    )
    return out


# ── DiD feature block (verbatim from the batch) ──────────────────────────────

def add_did_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["did_move_vs_sector_pct"] = out["move_from_first_close%"] - out["sector_move_open_pct"]
    out["did_move_vs_market_pct"] = out["move_from_first_close%"] - out["market_median_move_open_pct"]
    out["did_move_vs_nifty_pct"] = out["move_from_first_close%"] - out["nifty_move_open_pct"]

    out["did_ret_vs_sector_bps"] = out["ret_bps"] - out["sector_median_ret_bps"]
    out["did_ret_vs_market_bps"] = out["ret_bps"] - out["market_median_ret_bps"]
    out["did_ret_vs_nifty_bps"] = out["ret_bps"] - out["nifty_ret_bps"]

    out["did_short_vs_sector"] = out["short_drive"] - out["sector_short_pressure"]
    out["did_short_vs_market"] = out["short_drive"] - out["market_short_pressure"]
    out["did_long_vs_sector"] = out["long_drive"] - out["sector_long_pressure"]
    out["did_long_vs_market"] = out["long_drive"] - out["market_long_pressure"]
    out["did_atp_vs_nifty"] = out["atp_torque"] - out["nifty_atp_torque"]
    out["did_volume_vs_personality"] = out["volume_ratio"] / out["personality_vol_gate"].replace(0, np.nan)
    out["short_minus_long"] = out["short_drive"] - out["long_drive"]

    rolling_inputs = [
        ("move_from_first_close%", "stock_move"),
        ("sector_move_open_pct", "sector_move"),
        ("market_median_move_open_pct", "market_move"),
        ("nifty_move_open_pct", "nifty_move"),
        ("ret_bps", "stock_ret"),
        ("sector_median_ret_bps", "sector_ret"),
        ("market_median_ret_bps", "market_ret"),
        ("nifty_ret_bps", "nifty_ret"),
        ("short_drive", "stock_short"),
        ("sector_short_pressure", "sector_short"),
        ("market_short_pressure", "market_short"),
        ("long_drive", "stock_long"),
        ("sector_long_pressure", "sector_long"),
        ("market_long_pressure", "market_long"),
        ("atp_torque", "stock_atp"),
        ("nifty_atp_torque", "nifty_atp"),
    ]
    for source_col, name in rolling_inputs:
        out = add_regular_session_pre_mean(out, source_col, f"pre5_{name}")

    out["did5_move_vs_sector_pct"] = (
        out["move_from_first_close%"] - out["pre5_stock_move"]
    ) - (out["sector_move_open_pct"] - out["pre5_sector_move"])
    out["did5_move_vs_market_pct"] = (
        out["move_from_first_close%"] - out["pre5_stock_move"]
    ) - (out["market_median_move_open_pct"] - out["pre5_market_move"])
    out["did5_move_vs_nifty_pct"] = (
        out["move_from_first_close%"] - out["pre5_stock_move"]
    ) - (out["nifty_move_open_pct"] - out["pre5_nifty_move"])

    out["did5_ret_vs_sector_bps"] = (out["ret_bps"] - out["pre5_stock_ret"]) - (
        out["sector_median_ret_bps"] - out["pre5_sector_ret"]
    )
    out["did5_ret_vs_market_bps"] = (out["ret_bps"] - out["pre5_stock_ret"]) - (
        out["market_median_ret_bps"] - out["pre5_market_ret"]
    )
    out["did5_ret_vs_nifty_bps"] = (out["ret_bps"] - out["pre5_stock_ret"]) - (
        out["nifty_ret_bps"] - out["pre5_nifty_ret"]
    )

    out["did5_short_vs_sector"] = (out["short_drive"] - out["pre5_stock_short"]) - (
        out["sector_short_pressure"] - out["pre5_sector_short"]
    )
    out["did5_short_vs_market"] = (out["short_drive"] - out["pre5_stock_short"]) - (
        out["market_short_pressure"] - out["pre5_market_short"]
    )
    out["did5_long_vs_sector"] = (out["long_drive"] - out["pre5_stock_long"]) - (
        out["sector_long_pressure"] - out["pre5_sector_long"]
    )
    out["did5_atp_vs_nifty"] = (out["atp_torque"] - out["pre5_stock_atp"]) - (
        out["nifty_atp_torque"] - out["pre5_nifty_atp"]
    )
    out["short_minus_long_d5"] = (out["short_drive"] - out["pre5_stock_short"]) - (
        out["long_drive"] - out["pre5_stock_long"]
    )

    clip = lambda x: np.clip(x, 0, 1)
    out["did_score"] = 100.0 * (
        0.20 * clip((-out["did_move_vs_sector_pct"]) / 0.50)
        + 0.12 * clip((-out["did_move_vs_market_pct"]) / 0.50)
        + 0.12 * clip((-out["did_ret_vs_sector_bps"]) / 20.0)
        + 0.18 * clip(out["did_short_vs_sector"] / 0.35)
        + 0.14 * clip((-out["did5_move_vs_sector_pct"]) / 0.25)
        + 0.10 * clip(out["did5_short_vs_sector"] / 0.20)
        + 0.08 * clip((-out["did5_atp_vs_nifty"]) / 1.25)
        + 0.06 * clip((out["did_volume_vs_personality"] - 0.85) / 1.50)
    )

    out["did_exhaustion_risk"] = (
        (out["did5_short_vs_sector"] > 0.26)
        | (out["did5_move_vs_sector_pct"] > 0.05)
        | (out["close_atp_gap_bps"] > -50)
    )
    return out


# ── v3 grade (verbatim from the batch) ───────────────────────────────────────

def grade_v3(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    base_short_gate = (
        out["base_gate"]
        & out["personality_quality_ok"]
        & out["falcon_phase"].isin(["SHORT_DRIVE", "ATP_ASK_REVERSAL_DISTRIBUTION"])
    )

    continuation_gap_ok = out["close_atp_gap_bps"] <= -50.0
    abnormal_selling_ok = out["did_short_vs_sector"] >= 0.10
    controlled_pressure_ok = out["did5_short_vs_sector"] <= 0.26
    controlled_price_pulse_ok = out["did5_move_vs_sector_pct"] <= 0.05

    out["v3_did_gate"] = (
        base_short_gate
        & continuation_gap_ok
        & abnormal_selling_ok
        & controlled_pressure_ok
        & controlled_price_pulse_ok
    )

    out["v3_grade"] = "IGNORE"
    out.loc[
        out["falcon_phase"].isin(["SHORT_DRIVE", "ATP_ASK_REVERSAL_DISTRIBUTION"]),
        "v3_grade",
    ] = "WATCH"
    out.loc[out["v3_did_gate"], "v3_grade"] = "A++"

    a3_gate = (
        out["v3_did_gate"]
        & (out["close_atp_gap_bps"] <= -60.0)
        & (out["did_volume_vs_personality"] >= 5.0)
        & (out["did_short_vs_sector"] >= 0.10)
        & (out["did_score"] >= 50.0)
    )
    out.loc[a3_gate, "v3_grade"] = "A+++"

    out["v3_setup"] = np.select(
        [
            out["reload_after_relief"].fillna(False),
            out["new_breakdown"].fillna(False),
            out["falcon_phase"].eq("ATP_ASK_REVERSAL_DISTRIBUTION"),
        ],
        [
            "DID_SHORT_RELOAD_AFTER_RELIEF",
            "DID_SHORT_BREAKDOWN_CONTINUATION",
            "DID_ASK_DISTRIBUTION_CONTINUATION",
        ],
        default="DID_SHORT_CONTINUATION",
    )

    out["v3_rank_score"] = (
        out["did_score"].fillna(0)
        + np.clip((-out["close_atp_gap_bps"].fillna(0)) / 2.0, 0, 80)
        + np.clip(out["did_volume_vs_personality"].fillna(0) * 4.0, 0, 60)
        + np.clip(out["did_short_vs_sector"].fillna(0) * 80.0, 0, 40)
    )
    return out


# ── convenience wrapper (live post-processing entry point) ───────────────────

def apply_did_layer(df: pd.DataFrame) -> pd.DataFrame:
    """Add the DiD feature block + v3 grade to an already-v2-graded frame.

    Input `df` MUST carry the full intraday minute series per (instrument, day)
    (so the pre5 rolling means are correct) and every v2 column the DiD layer
    consumes — exactly the frame tesla_short_engine.grade_scored_frame produces
    right after grade(). Returns a NEW frame; never mutates the input.
    """
    return grade_v3(add_did_features(df))
