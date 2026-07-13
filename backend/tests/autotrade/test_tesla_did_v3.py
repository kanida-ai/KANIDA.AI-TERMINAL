"""Falcon Tesla v3 DiD layer — port fidelity + flag-off byte-identity.

Three layers:
  1. DiD-LAYER PARITY (auto-skips if the batch CSVs are absent): load the v3
     script's INPUT (short_engine_v2_all_scores.csv), run the VENDORED
     tesla_did.add_did_features + grade_v3, and assert every DiD feature column,
     v3_grade / v3_setup / v3_did_gate, and the A++/A+++ cooldown trade selection
     match the v3 script's OWN OUTPUT CSVs (short_engine_v3_did_all_scores.csv /
     short_engine_v3_did_trades.csv) byte-for-byte (numeric within 1e-9). This
     isolates DiD-port correctness from the live-engine v2 parity.
  2. FLAG-OFF BYTE-IDENTITY (always runs, synthetic): with the DiD layer OFF,
     grade_scored_frame + the signal-build path are identical to today's v2 path
     (no v3 columns, no mutated v2 column, rank_score=None → legacy rotation
     order).
  3. ROTATION RANK ROUTING (always runs): rank_signals prefers v3_rank_score when
     present and is byte-identical to the short_drive order when it is None.

MUTATION-VERIFY notes are inline at each assert (the exact revert that FAILS it).
"""
import os

import numpy as np
import pandas as pd
import pytest

from autotrade.strategies import tesla_features as tf
from autotrade.strategies import tesla_short_engine as tse
from autotrade.strategies import tesla_did as tdid
from autotrade.strategies import tesla_rotation as trot


# ── batch CSV locations (the v3 script's own INPUT + OUTPUT) ──────────────────

_OUT = r"C:\Users\SPS\Documents\Kanida Ai\outputs\falcon_tesla"
_V2_SCORES = os.path.join(_OUT, "short_engine_v2_all_scores.csv")
_V3_SCORES = os.path.join(_OUT, "short_engine_v3_did_all_scores.csv")
_V3_TRADES = os.path.join(_OUT, "short_engine_v3_did_trades.csv")

_CSVS_PRESENT = all(os.path.exists(p) for p in (_V2_SCORES, _V3_SCORES, _V3_TRADES))
_SKIP_REASON = (
    "batch CSVs absent — need "
    + ", ".join(p for p in (_V2_SCORES, _V3_SCORES, _V3_TRADES)
                if not os.path.exists(p))
)

# The DiD feature columns the port computes — compared numerically (within 1e-9).
_DID_NUMERIC_COLS = [
    "did_move_vs_sector_pct", "did_move_vs_market_pct", "did_move_vs_nifty_pct",
    "did_ret_vs_sector_bps", "did_ret_vs_market_bps", "did_ret_vs_nifty_bps",
    "did_short_vs_sector", "did_short_vs_market", "did_long_vs_sector",
    "did_long_vs_market", "did_atp_vs_nifty", "did_volume_vs_personality",
    "short_minus_long",
    "pre5_stock_move", "pre5_sector_move", "pre5_market_move", "pre5_nifty_move",
    "pre5_stock_ret", "pre5_sector_ret", "pre5_market_ret", "pre5_nifty_ret",
    "pre5_stock_short", "pre5_sector_short", "pre5_market_short",
    "pre5_stock_long", "pre5_sector_long", "pre5_market_long",
    "pre5_stock_atp", "pre5_nifty_atp",
    "did5_move_vs_sector_pct", "did5_move_vs_market_pct", "did5_move_vs_nifty_pct",
    "did5_ret_vs_sector_bps", "did5_ret_vs_market_bps", "did5_ret_vs_nifty_bps",
    "did5_short_vs_sector", "did5_short_vs_market", "did5_long_vs_sector",
    "did5_atp_vs_nifty", "short_minus_long_d5", "did_score", "v3_rank_score",
]
_DID_BOOL_COLS = ["did_exhaustion_risk", "v3_did_gate"]
_DID_STR_COLS = ["v3_grade", "v3_setup"]


def _as_bool(s: pd.Series) -> pd.Series:
    """Verbatim from the v3 batch script (falcons_tesla_short_engine_v3_did.py)
    — used so the loaded frame matches the batch's own load_v2_scores bit-for-bit."""
    if s.dtype == bool:
        return s.fillna(False)
    if np.issubdtype(s.dtype, np.number):
        return s.fillna(0).astype(bool)
    return s.astype(str).str.lower().isin(["true", "1", "yes"])


def _load_v2_scores(path: str) -> pd.DataFrame:
    """Verbatim replica of the v3 batch load_v2_scores so the DiD input is
    identical to the batch's."""
    df = pd.read_csv(path, low_memory=False)
    df["bar_time"] = pd.to_datetime(df["bar_time"])
    for col in ["base_gate", "personality_quality_ok", "phase_transition_ok",
                "new_breakdown"]:
        if col in df.columns:
            df[col] = _as_bool(df[col])
    return df.sort_values(["instrument", "day", "bar_time"]).reset_index(drop=True)


# ── layer 1: DiD-layer parity vs the batch's own output CSVs ──────────────────

@pytest.mark.skipif(not _CSVS_PRESENT, reason=_SKIP_REASON)
def test_did_features_and_grade_match_batch_all_scores():
    """The vendored add_did_features + grade_v3 on the v2 input reproduce the
    batch v3_did_all_scores.csv exactly.

    MUTATION-VERIFY: change ANY constant/weight in tesla_did (e.g. did_score
    weight 0.20→0.25, gate -50.0→-40.0, exhaustion 0.26→0.30, v3_rank_score clip
    80→100) → the corresponding numeric/bool/str column diverges > 1e-9 and the
    assert FAILS.
    """
    src = _load_v2_scores(_V2_SCORES)
    mine = tdid.grade_v3(tdid.add_did_features(src))

    # Load only the columns we assert on from the (large) batch output.
    keys = ["instrument", "day", "bar_time"]
    want = keys + _DID_NUMERIC_COLS + _DID_BOOL_COLS + _DID_STR_COLS
    ref = pd.read_csv(_V3_SCORES, low_memory=False, usecols=want)
    ref["bar_time"] = pd.to_datetime(ref["bar_time"])

    assert len(mine) == len(ref), "row count diverged"
    # Both frames are in the SAME sorted order (batch load_v2_scores sort ==
    # ours), so compare positionally after a key-alignment sanity check.
    mine = mine.reset_index(drop=True)
    ref = ref.reset_index(drop=True)
    for k in keys:
        a = mine[k].astype(str).to_numpy()
        b = ref[k].astype(str).to_numpy()
        assert (a == b).all(), f"row-order/key mismatch on {k}"

    for col in _DID_NUMERIC_COLS:
        a = pd.to_numeric(mine[col], errors="coerce").to_numpy(dtype="float64")
        b = pd.to_numeric(ref[col], errors="coerce").to_numpy(dtype="float64")
        assert np.allclose(a, b, rtol=0.0, atol=1e-9, equal_nan=True), (
            f"DiD numeric column {col} diverged > 1e-9")

    for col in _DID_BOOL_COLS:
        a = _as_bool(mine[col]).to_numpy()
        b = _as_bool(ref[col]).to_numpy()
        assert (a == b).all(), f"DiD bool column {col} diverged"

    for col in _DID_STR_COLS:
        a = mine[col].astype(str).to_numpy()
        b = ref[col].astype(str).to_numpy()
        assert (a == b).all(), f"DiD string column {col} diverged"


@pytest.mark.skipif(not _CSVS_PRESENT, reason=_SKIP_REASON)
def test_v3_trade_selection_matches_batch_trades():
    """The A++/A+++ cooldown lifecycle over the v3 regrade reproduces the batch
    v3_did_trades.csv trade set exactly (instrument, day, bar_time, v3_grade).

    MUTATION-VERIFY: change the select cooldown 30→20 (extra entries) or the v3
    A+++ gate (did_score 50.0→40.0 → some A++ become A+++) → the selected keys or
    grades diverge and the set-equality assert FAILS.
    """
    src = _load_v2_scores(_V2_SCORES)
    graded = tdid.grade_v3(tdid.add_did_features(src))
    mine = tse.select_trade_lifecycle(
        graded, cooldown_minutes=30, grades=("A++", "A+++"),
        grade_col="v3_grade")

    ref = pd.read_csv(_V3_TRADES, low_memory=False,
                      usecols=["instrument", "day", "bar_time", "v3_grade"])
    ref["bar_time"] = pd.to_datetime(ref["bar_time"])

    mine_set = set(zip(mine["instrument"].astype(str), mine["day"].astype(str),
                       pd.to_datetime(mine["bar_time"]).astype(str),
                       mine["v3_grade"].astype(str)))
    ref_set = set(zip(ref["instrument"].astype(str), ref["day"].astype(str),
                      pd.to_datetime(ref["bar_time"]).astype(str),
                      ref["v3_grade"].astype(str)))
    assert mine_set == ref_set, (
        f"v3 trade set diverged: only-mine={len(mine_set - ref_set)} "
        f"only-ref={len(ref_set - mine_set)}")


# ── synthetic scored frame (mirrors score_universe_from_db's per-group tail) ──

def _raw_bars(symbol: str, day: str, sector: str, drop: float) -> pd.DataFrame:
    n = 25
    bt = pd.date_range(f"{day} 09:15", periods=n, freq="1min")
    close = np.linspace(100.0, 100.0 - drop, n)
    df = pd.DataFrame({
        "symbol": symbol, "segment": "CASH", "instrument_key": symbol,
        "bar_time": bt, "instrument": symbol, "root_symbol": symbol,
        "time": bt.strftime("%H:%M"), "day": day, "sector": sector,
        "open": close + 0.05, "high": close + 0.1, "low": close - 0.1,
        "close": close, "volume": np.linspace(1000, 3000, n),
        "atp": close + 0.02, "oi": np.linspace(50000, 51000, n),
        "total_buy_qty": [400] * n, "total_sell_qty": [600] * n,
        "buy_imb%": [40.0] * n, "depth_bid%": [45.0] * n,
        "b1q": [100] * n, "a1q": [200] * n, "b1o": [5] * n, "a1o": [5] * n,
        "b1p": close - 0.05, "a1p": close + 0.05,
        "tick_buy%": [30.0] * n, "last_qty": [10] * n, "block_x": [3.0] * n,
    })
    return df


def _synth_scored() -> pd.DataFrame:
    days = ["2026-07-08", "2026-07-09", "2026-07-10"]
    syms = [("AAA", "BANK"), ("BBB", "BANK"), ("CCC", "IT")]
    parts = []
    for day in days:
        for sym, sector in syms:
            raw = _raw_bars(sym, day, sector, drop=1.5 if sector == "BANK" else 0.5)
            parts.append(tf.add_microstructure_features(raw))
    scored = pd.concat(parts, ignore_index=True)
    # Tail mirrors tesla_short_engine.score_universe_from_db.
    scored["short_drive"] = tf.clamp01(scored["short_drive_core"])
    scored["long_drive"] = tf.clamp01(scored["long_drive_core"])
    scored["bid_absorption"] = tf.clamp01(scored["bid_absorption_core"])
    scored["ask_absorption"] = tf.clamp01(scored["ask_absorption_core"])
    scored["falcon_tesla_score"] = (scored["long_drive"] - scored["short_drive"]) * 100.0
    scored["absorption_bias"] = (scored["bid_absorption"] - scored["ask_absorption"]) * 100.0
    scored = tf.assign_phase(scored)
    scored = tse._add_forward_15m(scored)
    scored["sector"] = scored["sector"].fillna("UNKNOWN")
    scored["bar_time"] = pd.to_datetime(scored["bar_time"])
    scored["close_atp_gap_bps"] = ((scored["close"] - scored["atp"]) /
                                   scored["close"].replace(0, np.nan)) * 10000.0
    scored["tick_sell%"] = 100.0 - scored["tick_buy%"]
    return scored


def _synth_nifty_ctx(scored: pd.DataFrame) -> pd.DataFrame:
    dt = scored[["day", "time"]].drop_duplicates().reset_index(drop=True)
    dt["nifty_close"] = 22000.0
    dt["nifty_move_open_pct"] = -0.6
    dt["nifty_ret_bps"] = -5.0
    dt["nifty_atp_torque"] = -0.8
    dt["nifty_short_pressure"] = 0.7
    dt["nifty_long_pressure"] = 0.1
    dt["nifty_net_aggression"] = -0.4
    return dt


_V2_ONLY_COLS = ["v2_grade", "v2_setup", "base_gate", "personality_quality_ok",
                 "falcon_phase", "short_drive", "close_atp_gap_bps",
                 "reload_after_relief", "new_breakdown", "phase_transition_ok"]
_V3_ADDED_COLS = ["v3_grade", "v3_setup", "v3_did_gate", "v3_rank_score",
                  "did_score", "did_short_vs_sector", "did5_short_vs_sector"]


# ── layer 2: flag-off byte-identity ──────────────────────────────────────────

def test_grade_scored_frame_flag_off_is_v2_identical():
    """did_layer_enabled=False → the graded frame is EXACTLY the v2 frame: no v3
    columns, and every v2 column is bit-identical to the DiD-on frame's v2
    columns (the DiD layer never mutates a v2 column).

    MUTATION-VERIFY: make grade_scored_frame call apply_did_layer UNCONDITIONALLY
    (drop the `if did_layer_enabled` guard) → v3_grade appears in the off-frame →
    the 'v3 col not present' assert FAILS. Make the DiD port overwrite v2_grade →
    the v2-columns-equal assert FAILS.
    """
    scored = _synth_scored()
    nctx = _synth_nifty_ctx(scored)
    off = tse.grade_scored_frame(scored, nctx, ["2026-07-08", "2026-07-09"],
                                 did_layer_enabled=False)
    on = tse.grade_scored_frame(scored, nctx, ["2026-07-08", "2026-07-09"],
                                did_layer_enabled=True)

    for c in _V3_ADDED_COLS:
        assert c not in off.columns, f"flag-off frame leaked v3 column {c}"
        assert c in on.columns, f"flag-on frame missing v3 column {c}"

    # every v2 column present in the off frame is bit-identical in the on frame
    for c in _V2_ONLY_COLS:
        assert c in off.columns
        a = off[c].to_numpy()
        b = on[c].to_numpy()
        if off[c].dtype.kind in "fc":
            assert np.allclose(a.astype("float64"), b.astype("float64"),
                               equal_nan=True), f"v2 col {c} mutated by DiD layer"
        else:
            assert (pd.Series(a).astype(str).to_numpy()
                    == pd.Series(b).astype(str).to_numpy()).all(), (
                f"v2 col {c} mutated by DiD layer")


def test_signal_from_row_flag_off_matches_v2_shape():
    """_signal_from_row(did=False) reads v2_grade/v2_setup and leaves
    rank_score=None (legacy rotation order); did=True reads v3_* and carries
    v3_rank_score.

    MUTATION-VERIFY: have _signal_from_row always read v3_grade → the v2-shape
    assert (grade=='A++' from v2_grade) FAILS; always set rank_score → the
    'rank_score is None' assert FAILS.
    """
    row = pd.Series({
        "instrument": "AAA", "day": "2026-07-10", "time": "09:30",
        "bar_time": pd.Timestamp("2026-07-10 09:30"), "close": 100.0,
        "short_drive": 0.7, "sector": "BANK",
        "v2_grade": "A++", "v2_setup": "SHORT_RELOAD_OR_BREAKDOWN",
        "v3_grade": "A+++", "v3_setup": "DID_SHORT_CONTINUATION",
        "v3_rank_score": 123.45,
    })
    s_off = tse._signal_from_row(row, did_layer_enabled=False)
    assert s_off.grade == "A++" and s_off.setup == "SHORT_RELOAD_OR_BREAKDOWN"
    assert s_off.rank_score is None

    s_on = tse._signal_from_row(row, did_layer_enabled=True)
    assert s_on.grade == "A+++" and s_on.setup == "DID_SHORT_CONTINUATION"
    assert s_on.rank_score == pytest.approx(123.45)


# ── layer 3: rotation rank routing ───────────────────────────────────────────

class _Sig:
    def __init__(self, instrument, grade, short_drive, rank_score=None):
        self.instrument = instrument
        self.grade = grade
        self.short_drive = short_drive
        self.rank_score = rank_score


def test_rank_signals_v2_order_unchanged_when_rank_score_none():
    """rank_score=None (the v2 path) → order is byte-identical to the legacy
    (grade, short_drive) order.

    MUTATION-VERIFY: change _rank_key to always use rank_score (0.0 when None) →
    the short_drive tiebreak is lost and this order assert FAILS.
    """
    sigs = [_Sig("A", "A++", 0.60), _Sig("B", "A++", 0.90),
            _Sig("C", "A+++", 0.10)]
    order = [s.instrument for s in trot.rank_signals(sigs)]
    # A+++ first, then A++ by descending short_drive.
    assert order == ["C", "B", "A"]


def test_rank_signals_prefers_v3_rank_score_when_present():
    """rank_score set (the v3 path) → the secondary key is v3_rank_score, NOT
    short_drive.

    MUTATION-VERIFY: make _rank_key ignore rank_score (use short_drive) → order
    flips to ['B','A'] and this assert FAILS.
    """
    # Same grade; short_drive would order B>A, but rank_score orders A>B.
    sigs = [_Sig("A", "A++", 0.10, rank_score=200.0),
            _Sig("B", "A++", 0.90, rank_score=100.0)]
    order = [s.instrument for s in trot.rank_signals(sigs)]
    assert order == ["A", "B"]
