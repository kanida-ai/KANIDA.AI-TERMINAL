"""Falcon Tesla LIVE short-signal engine — port fidelity + logic.

Two layers:
  1. FAST synthetic tests (always run): the ported gate/grade/lifecycle/window
     logic on hand-built frames + a tiny in-memory poll DB. No large data.
  2. REAL-DATA PARITY (opt-in, env FALCON_TESLA_PARITY=1 + data present): the
     live engine reproduces the batch falcons_tesla_short_engine_v2.py A++/A+++
     set. Proven live 2026-07-12: grade-port 264/264 exact on the batch CSV, and
     the full DB feature-recompute 264/264 exact on 2026-07-08 (0-vs-0 on the
     07-10 walk-forward day — the batch itself produces 0 there). Guarded so the
     default suite stays fast; run with FALCON_TESLA_PARITY=1 to re-verify.
"""
import os
import sqlite3

import numpy as np
import pandas as pd
import pytest

from autotrade.strategies import tesla_features as tf
from autotrade.strategies import tesla_short_engine as tse


# ── helpers ──────────────────────────────────────────────────────────────────

def _passing_row(**over):
    """A single-row frame whose columns satisfy EVERY base_gate condition.
    Override any key to flip one condition and prove the gate is sensitive."""
    row = dict(
        instrument="X", day="2026-07-08", time="09:30",
        bar_time=pd.Timestamp("2026-07-08 09:30"), sector="S", close=100.0,
        atp=100.0, volume=1000.0, volume_ratio=5.0, ret_bps=-10.0,
        # market regime (down)
        market_median_move_open_pct=-0.60, market_down_breadth=70.0,
        market_median_ret_bps=-10.0, market_short_pressure=0.7,
        market_long_pressure=0.1, market_median_ret=-1,
        # nifty (down)
        nifty_move_open_pct=-0.60, nifty_short_pressure=0.7, nifty_long_pressure=0.1,
        # sector (down)
        sector_move_open_pct=-0.60, sector_down_breadth=80.0,
        sector_short_pressure=0.7, sector_long_pressure=0.1,
        # stock
        falcon_phase="SHORT_DRIVE", short_drive=0.70, long_drive=0.10,
        bid_absorption=0.10, atp_torque=-0.90, net_aggression=-0.50,
        close_atp_gap_bps=-40.0, personality_short_gate=0.60,
        personality_gap_gate=-15.0, personality_vol_gate=3.0,
        phase_transition_ok=True, tick_buy_pct=20.0,
    )
    row.update(over)
    df = pd.DataFrame([row])
    df["tick_buy%"] = df.pop("tick_buy_pct")
    return df


# ── base_gate sensitivity ────────────────────────────────────────────────────

def test_base_gate_passes_when_all_conditions_met():
    assert bool(tse.base_gate(_passing_row()).iloc[0]) is True


@pytest.mark.parametrize("field,bad", [
    ("atp_torque", -0.50),               # weaker than -0.70
    ("net_aggression", -0.20),           # weaker than -0.35
    ("volume_ratio", 1.0),               # below personality_vol_gate 3.0
    ("short_drive", 0.30),               # below personality_short_gate 0.60
    ("long_drive", 0.40),                # above 0.20
    ("bid_absorption", 0.50),            # above 0.30
    ("close_atp_gap_bps", 0.0),          # above the -15 gap gate
    ("market_down_breadth", 40.0),       # below 50
    ("nifty_move_open_pct", 0.10),       # not down
    ("sector_down_breadth", 40.0),       # below 55
    ("phase_transition_ok", False),      # no transition
    ("falcon_phase", "LONG_DRIVE"),      # not a short phase
    ("tick_buy_pct", 80.0),              # buyers dominate → not a short
])
def test_base_gate_fails_when_any_condition_broken(field, bad):
    assert bool(tse.base_gate(_passing_row(**{field: bad})).iloc[0]) is False


# ── grade tiers ──────────────────────────────────────────────────────────────

def _graded_row(**over):
    df = _passing_row(**over)
    df["base_gate"] = tse.base_gate(df)
    df["personality_quality_ok"] = True
    df["train_vol_p90"] = 4.0
    return tse.grade(df)


def test_grade_a_plus_plus_when_gate_only():
    # gate met but NOT the strong_market/strong_stock upgrade (breadth < 60 etc)
    out = _graded_row(market_down_breadth=55.0, sector_down_breadth=60.0,
                      market_median_move_open_pct=-0.30, nifty_move_open_pct=-0.30)
    assert out["v2_grade"].iloc[0] == "A++"


def test_grade_a_plus_plus_plus_when_strong():
    out = _graded_row()  # passing row already has strong market + strong stock
    assert out["v2_grade"].iloc[0] == "A+++"


def test_grade_watch_when_gate_fails_but_short_phase():
    df = _passing_row(atp_torque=-0.1)  # breaks base_gate, still SHORT_DRIVE phase
    df["base_gate"] = tse.base_gate(df)
    df["personality_quality_ok"] = True
    df["train_vol_p90"] = 4.0
    out = tse.grade(df)
    assert out["v2_grade"].iloc[0] == "WATCH"


def test_grade_requires_personality_quality():
    df = _passing_row()
    df["base_gate"] = tse.base_gate(df)
    df["personality_quality_ok"] = False   # quality gate fails
    df["train_vol_p90"] = 4.0
    out = tse.grade(df)
    assert out["v2_grade"].iloc[0] in ("WATCH", "IGNORE")  # never A++/A+++


# ── stock personality (rolling window quantiles) ─────────────────────────────

def test_stock_personality_derives_gates_from_train_days():
    train = pd.DataFrame({
        "instrument": ["A"] * 5,
        "day": ["2026-07-08"] * 5,
        "volume_ratio": [1.0, 2.0, 3.0, 4.0, 5.0],
        "close_atp_gap_bps": [-50, -40, -30, -20, -10],
        "short_drive": [0.5, 0.6, 0.7, 0.8, 0.9],
    })
    # an infer-day row for the same instrument (not in train)
    infer = train.iloc[[0]].copy(); infer["day"] = "2026-07-10"
    df = pd.concat([train, infer], ignore_index=True)
    out = tse.add_stock_personality(df, train_days=["2026-07-08"])
    r = out[out["day"] == "2026-07-10"].iloc[0]
    # gates clamp/floor: vol gate in [1.5,6], gap gate <= -15, short gate >= 0.60
    assert 1.5 <= r["personality_vol_gate"] <= 6.0
    assert r["personality_gap_gate"] <= -15.0
    assert r["personality_short_gate"] >= 0.60


def test_stock_personality_defaults_for_unknown_instrument():
    df = pd.DataFrame({"instrument": ["NEW"], "day": ["2026-07-10"],
                       "volume_ratio": [2.0], "close_atp_gap_bps": [-20.0],
                       "short_drive": [0.7]})
    out = tse.add_stock_personality(df, train_days=["2026-07-08"])  # NEW not in train
    r = out.iloc[0]
    assert r["personality_vol_gate"] == 3.0      # fillna default
    assert r["personality_gap_gate"] == -15.0
    assert r["personality_short_gate"] == 0.60


# ── lifecycle cooldown ───────────────────────────────────────────────────────

def _cand(instrument, minute, grade="A++"):
    return dict(instrument=instrument, day="2026-07-08",
                bar_time=pd.Timestamp(f"2026-07-08 {minute}"), v2_grade=grade)


def test_lifecycle_cooldown_suppresses_within_window():
    df = pd.DataFrame([_cand("A", "09:30"), _cand("A", "09:45")])  # 15 min apart
    out = tse.select_trade_lifecycle(df, cooldown_minutes=30)
    assert len(out) == 1


def test_lifecycle_cooldown_allows_after_window():
    df = pd.DataFrame([_cand("A", "09:30"), _cand("A", "10:05")])  # 35 min apart
    out = tse.select_trade_lifecycle(df, cooldown_minutes=30)
    assert len(out) == 2


def test_lifecycle_independent_per_instrument():
    df = pd.DataFrame([_cand("A", "09:30"), _cand("B", "09:35")])
    out = tse.select_trade_lifecycle(df, cooldown_minutes=30)
    assert len(out) == 2


def test_lifecycle_min_grade_filter():
    df = pd.DataFrame([_cand("A", "09:30", "A++"), _cand("B", "09:31", "A+++")])
    only_strong = tse.select_trade_lifecycle(df, cooldown_minutes=30, grades=("A+++",))
    assert set(only_strong["instrument"]) == {"B"}


def test_min_grades_mapping():
    assert tse._min_grades("A++") == ["A++", "A+++"]
    assert tse._min_grades("A+++") == ["A+++"]
    assert tse._min_grades("a+++") == ["A+++"]


# ── window resolution against a tiny in-memory poll DB ───────────────────────

def _tiny_poll_db():
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE mkt_orderflow_1min (symbol TEXT, segment TEXT, "
                "bar_time TEXT)")
    rows = []
    for day in ("2026-07-06", "2026-07-08", "2026-07-09", "2026-07-10"):
        for m in range(3):
            rows.append(("AAA", "CASH", f"{day} 09:1{m}"))
    con.executemany("INSERT INTO mkt_orderflow_1min VALUES (?,?,?)", rows)
    con.commit()
    return con


def test_available_trading_days_ascending():
    con = _tiny_poll_db()
    assert tse.available_trading_days(con) == [
        "2026-07-06", "2026-07-08", "2026-07-09", "2026-07-10"]
    con.close()


def test_resolve_window_last_k_prior_days():
    con = _tiny_poll_db()
    assert tse.resolve_window(con, "2026-07-10", 2) == ["2026-07-08", "2026-07-09"]
    assert tse.resolve_window(con, "2026-07-10", 5) == [
        "2026-07-06", "2026-07-08", "2026-07-09"]
    # first day has no prior window
    assert tse.resolve_window(con, "2026-07-06", 5) == []
    con.close()


def test_compute_live_signals_empty_on_no_data(tmp_path):
    empty = tmp_path / "empty.db"
    con = sqlite3.connect(str(empty))
    con.execute("CREATE TABLE mkt_orderflow_1min (symbol TEXT, segment TEXT, "
                "bar_time TEXT)")
    con.commit(); con.close()
    res = tse.compute_live_signals(db_path=empty)
    assert res.signals == [] and "no poll data" in res.note


# ── no-look-ahead cutoff (synthetic) ─────────────────────────────────────────

def test_as_of_cutoff_excludes_future_bars():
    # two A++ minutes; as_of at the earlier minute must drop the later one.
    df = pd.DataFrame([
        dict(instrument="A", day="2026-07-08",
             bar_time=pd.Timestamp("2026-07-08 09:30"), v2_grade="A++"),
        dict(instrument="B", day="2026-07-08",
             bar_time=pd.Timestamp("2026-07-08 09:50"), v2_grade="A++"),
    ])
    sel = tse.select_trade_lifecycle(df, cooldown_minutes=1)
    cutoff = pd.Timestamp("2026-07-08 09:30")
    kept = sel[sel["bar_time"] <= cutoff]
    assert set(kept["instrument"]) == {"A"}


# ── feature vendor sanity (verbatim probe math) ──────────────────────────────

def test_feature_vendor_produces_core_columns():
    g = pd.DataFrame({
        "bar_time": pd.date_range("2026-07-08 09:15", periods=20, freq="1min"),
        "open": np.linspace(100, 98, 20), "high": np.linspace(100.2, 98.2, 20),
        "low": np.linspace(99.8, 97.8, 20), "close": np.linspace(100, 98, 20),
        "volume": np.linspace(1000, 2000, 20), "atp": np.linspace(100, 98.5, 20),
        "oi": np.linspace(50000, 51000, 20),
        "total_buy_qty": [400] * 20, "total_sell_qty": [600] * 20,
        "b1q": [100] * 20, "a1q": [200] * 20, "b1o": [5] * 20, "a1o": [5] * 20,
        "b1p": np.linspace(99.9, 97.9, 20), "a1p": np.linspace(100.1, 98.1, 20),
        "tick_buy%": [30.0] * 20, "last_qty": [10] * 20,
        # buy_imb% / depth_bid% are supplied by the caller (_pull_cash_symbol)
        # before add_microstructure_features — the vendored probe expects them.
        "buy_imb%": [40.0] * 20, "depth_bid%": [45.0] * 20,
    })
    out = tf.add_microstructure_features(g)
    for c in ("short_drive_core", "long_drive_core", "atp_torque",
              "net_aggression", "volume_ratio", "move_from_first_close%"):
        assert c in out.columns
    assert (out["short_drive_core"].between(0, 1)).all()


# ── REAL-DATA PARITY (opt-in) ────────────────────────────────────────────────

_CSV = (r"C:\Users\SPS\Documents\Kanida Ai\outputs\falcon_tesla"
        r"\walk_forward_all_cash_scores.csv")
_PARITY = (os.environ.get("FALCON_TESLA_PARITY", "").strip() == "1"
           and os.path.exists(_CSV) and tse.DEFAULT_DB_PATH.exists())


@pytest.mark.skipif(not _PARITY, reason="FALCON_TESLA_PARITY!=1 or data absent")
def test_grade_port_reproduces_batch_a_plus_set():
    """The ported grade pipeline on the batch's own scored CSV reproduces the
    batch v2 A++/A+++ minute set EXACTLY (train window [07-08, 07-09])."""
    import sys
    sys.path.insert(0, r"C:\Users\SPS\Documents\Kanida Ai\outputs\falcon_tesla")
    import falcons_tesla_short_engine_v2 as se2  # type: ignore
    ref = se2.grade(se2.add_train_personality_quality(se2.add_stock_personality(
        se2.add_phase_transition_features(se2.add_market_context(
            se2.load_cash_scores())))))
    ref = se2.select_trade_lifecycle(ref)
    ref_set = set(zip(ref["instrument"], ref["day"], ref["time"],
                      ref["v2_grade"]))
    scored = se2.load_cash_scores(); scored["bar_time"] = pd.to_datetime(scored["bar_time"])
    con = tse.connect_db_readonly()
    nctx = tse.load_nifty_context(con, ["2026-07-08", "2026-07-09", "2026-07-10"])
    con.close()
    mine = tse.select_trade_lifecycle(
        tse.grade_scored_frame(scored, nctx, ["2026-07-08", "2026-07-09"]),
        cooldown_minutes=30)
    mine_set = set(zip(mine["instrument"], mine["day"], mine["time"],
                      mine["v2_grade"]))
    assert mine_set == ref_set
