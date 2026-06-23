"""
Smoke tests for the persona engine — fast, no network, read-mostly.

Run:  PYTHONPATH=. C:/Users/SPS/Anaconda3/python.exe -m pytest persona_engine/tests -q
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from persona_engine import db, model, personas, outcomes
from persona_engine.model import ALL_FEATURES


def test_db_resolves_and_tables_exist():
    con = db.connect(read_only=True)
    try:
        for t in ["persona_signal_features", "fo_daily_predictions",
                  "lt_daily_predictions", "learning_proposals"]:
            assert db.table_exists(con, t), f"missing table {t}"
    finally:
        con.close()


def test_centered_rank_bounds_and_monotonic():
    s = pd.Series([1, 2, 3, 4, 5], dtype=float)
    cr = model._centered_rank(s)
    assert cr.min() >= -1.0 and cr.max() <= 1.0
    # strictly monotonic in the input (all that matters for ranking)
    assert list(cr.argsort()) == [0, 1, 2, 3, 4]
    assert cr.is_monotonic_increasing


def test_score_cross_section_orders_by_weight_sign():
    df = pd.DataFrame(
        {"roc_5": [10, 5, 1, -2, -8]},
        index=["A", "B", "C", "D", "E"])
    score = model.score_cross_section(df, {"roc_5": 1.0})
    # highest roc_5 -> highest score
    assert score.idxmax() == "A" and score.idxmin() == "E"
    score2 = model.score_cross_section(df, {"roc_5": -1.0})
    assert score2.idxmax() == "E" and score2.idxmin() == "A"


def test_predict_fo_shapes():
    rng = np.random.default_rng(0)
    n = 60
    df = pd.DataFrame(
        {f: rng.normal(size=n) for f in ALL_FEATURES},
        index=[f"S{i}" for i in range(n)])
    df["sector"] = (["Banks", "IT", "Auto"] * n)[:n]
    long_top, short_top = personas.predict_fo(
        df, None, model.BASELINE_WEIGHTS["FO_LONG"],
        model.BASELINE_WEIGHTS["FO_SHORT"])
    assert len(long_top) == 10 and len(short_top) == 10
    assert list(long_top["rank"]) == list(range(1, 11))


def test_forward_returns_no_lookahead_columns():
    con = db.connect(read_only=True)
    try:
        fwd = outcomes.forward_returns(con, symbols=["RELIANCE", "INFY"])
        # the last row per symbol must have NaN next-day return (no future bar)
        last = fwd.sort_values("trade_date").groupby("symbol").tail(1)
        assert last["fwd_nd"].isna().all()
    finally:
        con.close()
