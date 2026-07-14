"""Falcon Tesla — VECTORISED once-per-minute feature pipeline PARITY PROOF.

The sub-10s path (config `tesla_vectorized_features=true`, engine kwarg
`vectorized=True`) replaces the per-(instrument,day) Python loops with multi-group
(C-level) pandas ops:
  * tesla_features.add_microstructure_features_vectorized   (vs the per-group loop)
  * tesla_short_engine.add_phase_transition_features_vectorized  (vs the loop)
  * tesla_did.add_regular_session_pre_mean_fast (fast DiD pre5 means, vs the loop)

This drives a REAL-MONEY intraday SHORT signal, so parity is SACRED: the fast path
must be BYTE-IDENTICAL to the committed loop baseline. This module proves it end to
end, on a full trading day, for EVERY column, EVERY symbol, EVERY minute:

  (1) primitive equivalence — each vectorised op == concat(per-group loop), exact.
  (2) SCORED-FRAME parity — score_universe_for_symbols(vectorized=True) equals the
      per-symbol oracle score_universe_from_db on EVERY shared column. The scored
      frame IS every minute of every symbol, so this is the all-columns/all-symbols/
      all-minutes proof of the feature layer.
  (3) GRADED-FRAME parity — grade_infer_only(vectorized=True) equals
      grade_scored_frame(train+infer)[infer] on every column (v2 AND v3/DiD).
  (4) END-TO-END minute-by-minute — for a sweep of as_of minutes spanning the whole
      day (incl. the last-15-min provisional tail), compute_live_signals_fast(
      vectorized=True) yields the IDENTICAL signal set as the full-recompute oracle
      compute_live_signals (v2 AND v3/DiD).

MUTATION (verified live during the build): reverting cum_volume in
add_microstructure_features_vectorized to `groupby(...).cumsum()` (instead of the
byte-exact _segmented_cumsum) reintroduces a ~1e-11 ULP drift → test (2)/(3)/(4)
FAIL. Restored → all green. groupby.cumsum is the ONLY grouped op that is not
byte-identical to the per-group form; every other op (shift/diff/pct_change/
rolling median|mean|max|min|sum / expanding median / transform first) is exact.
"""
import sqlite3
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from autotrade.strategies import tesla_features as tf
from autotrade.strategies import tesla_short_engine as tse
from autotrade.strategies import tesla_did as tdid

# A full trading day is 375 one-minute bars (09:15–15:29). Use the full length so
# every rolling window (45/30/20/15/10/8) and the provisional 15-min tail are
# exercised, across MULTIPLE symbols (group boundaries) and MULTIPLE days.
_DAYS = ["2026-07-06", "2026-07-07", "2026-07-08", "2026-07-09"]
_CASH = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]
_NBARS = 375

_OF_COLS = [
    "symbol", "segment", "instrument_key", "bar_time", "open", "high", "low",
    "close", "volume", "atp", "oi", "total_buy_qty", "total_sell_qty",
    "b1p", "b1q", "b2p", "b2q", "b3p", "b3q", "b4p", "b4q", "b5p", "b5q",
    "a1p", "a1q", "a2p", "a2q", "a3p", "a3q", "a4p", "a4q", "a5p", "a5q",
    "last_qty", "b1o", "b2o", "b3o", "b4o", "b5o", "a1o", "a2o", "a3o", "a4o", "a5o",
]
_TR_COLS = ["instrument_key", "bar_time", "n_ticks", "buy_vol", "sell_vol",
            "buy_vol_pct", "avg_tick_vol", "max_tick_vol", "max_trade_qty"]


def _bars(symbol, segment, day, n=_NBARS, seed=0):
    rng = np.random.default_rng(abs(hash((symbol, day, seed))) % (2**32))
    ikey = f"{symbol}|{segment}"
    base = datetime(int(day[:4]), int(day[5:7]), int(day[8:10]), 9, 15, 0)
    of_rows, tr_rows = [], []
    price = 100.0 + rng.uniform(-5, 5)
    atp = price
    # Deliberately bias some sessions DOWN so a few A++/A+++ SHORT signals fire —
    # exercising select_trade_lifecycle + the latest-minute collapse end to end.
    drift = rng.uniform(-0.02, 0.005)
    for i in range(n):
        t = (base + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S")
        price = max(1.0, price + rng.normal(drift, 0.08))
        atp = atp + (price - atp) * 0.1
        hi, lo = price + abs(rng.normal(0, 0.06)), price - abs(rng.normal(0, 0.06))
        vol = float(max(1.0, rng.normal(1500, 500)))
        tbq, tsq = float(rng.integers(200, 800)), float(rng.integers(200, 800))
        of_rows.append((
            symbol, segment, ikey, t, price, hi, lo, price, vol, atp,
            float(rng.integers(40000, 60000)), tbq, tsq,
            lo, float(rng.integers(50, 300)), lo - 0.05, float(rng.integers(50, 300)),
            lo - 0.10, float(rng.integers(50, 300)), lo - 0.15, float(rng.integers(50, 300)),
            lo - 0.20, float(rng.integers(50, 300)),
            hi, float(rng.integers(50, 300)), hi + 0.05, float(rng.integers(50, 300)),
            hi + 0.10, float(rng.integers(50, 300)), hi + 0.15, float(rng.integers(50, 300)),
            hi + 0.20, float(rng.integers(50, 300)),
            float(rng.integers(1, 50)),
            float(rng.integers(1, 10)), float(rng.integers(1, 10)),
            float(rng.integers(1, 10)), float(rng.integers(1, 10)), float(rng.integers(1, 10)),
            float(rng.integers(1, 10)), float(rng.integers(1, 10)), float(rng.integers(1, 10)),
            float(rng.integers(1, 10)), float(rng.integers(1, 10)),
        ))
        if segment == "CASH":
            tr_rows.append((
                ikey, t, int(rng.integers(5, 50)), float(rng.integers(100, 500)),
                float(rng.integers(100, 500)), float(rng.uniform(20, 60)),
                float(rng.uniform(5, 30)), float(rng.uniform(30, 120)),
                float(rng.integers(20, 200))))
    return of_rows, tr_rows


@pytest.fixture(scope="module")
def synth_db(tmp_path_factory):
    path = tmp_path_factory.mktemp("teslavec") / "poll.db"
    con = sqlite3.connect(str(path))
    con.execute(f"CREATE TABLE mkt_orderflow_1min ({', '.join(_OF_COLS)})")
    con.execute(f"CREATE TABLE mkt_trades_1min ({', '.join(_TR_COLS)})")
    con.execute("CREATE TABLE mkt_reference (symbol TEXT, segment TEXT, "
                "instrument_key TEXT, sector TEXT, company TEXT, lot_size INTEGER)")
    of_all, tr_all = [], []
    for day in _DAYS:
        for sym in _CASH:
            of, tr = _bars(sym, "CASH", day)
            of_all += of
            tr_all += tr
        of, _ = _bars("NIFTY", "FUT", day)
        of_all += of
    con.executemany(
        f"INSERT INTO mkt_orderflow_1min VALUES ({','.join(['?'] * len(_OF_COLS))})",
        of_all)
    con.executemany(
        f"INSERT INTO mkt_trades_1min VALUES ({','.join(['?'] * len(_TR_COLS))})",
        tr_all)
    # Two sectors so the DiD sector-DiD block is non-degenerate.
    sectors = {"AAA": "TECH", "BBB": "TECH", "CCC": "TECH",
               "DDD": "BANK", "EEE": "BANK", "FFF": "BANK"}
    con.executemany("INSERT INTO mkt_reference VALUES (?,?,?,?,?,?)",
                    [(s, "CASH", f"{s}|CASH", sectors[s], s + " Ltd", 1)
                     for s in _CASH])
    con.commit()
    con.close()
    return path


# ── (1) primitive equivalence — each vectorised op == concat(per-group loop) ──

def _loop_microstructure(raw: pd.DataFrame) -> pd.DataFrame:
    parts = [tf.add_microstructure_features(g)
             for _, g in raw.groupby(["instrument", "day"], sort=False)]
    return pd.concat(parts, ignore_index=True)


def test_microstructure_vectorized_equals_loop(synth_db):
    con = tse.connect_db_readonly(synth_db)
    raw = tse._pull_cash_symbols_batch(con, _CASH, _DAYS)
    con.close()
    assert not raw.empty
    loop = _loop_microstructure(raw)
    vec = tf.add_microstructure_features_vectorized(raw)
    key = ["instrument", "day", "bar_time"]
    a = loop.sort_values(key).reset_index(drop=True)
    b = vec.sort_values(key).reset_index(drop=True)
    num = [c for c in a.columns
           if c in b.columns and pd.api.types.is_numeric_dtype(a[c])]
    assert len(num) > 40
    pd.testing.assert_frame_equal(a[num], b[num], check_dtype=False,
                                  check_exact=True)
    # MUTATION: revert cum_volume to groupby.cumsum → cum_volume + every field
    # downstream of it (minute_vwap_from_atp/vwap_gap_bps/atp_torque/…) diverges.


def test_phase_transition_vectorized_equals_loop(synth_db):
    con = tse.connect_db_readonly(synth_db)
    scored = tse.score_universe_from_db(con, _DAYS)
    nifty = tse.load_nifty_context(con, _DAYS)
    con.close()
    df = tse.add_market_context(scored, nifty)
    loop = tse.add_phase_transition_features(df)
    vec = tse.add_phase_transition_features_vectorized(df)
    key = ["instrument", "day", "bar_time"]
    cols = ["prev_low_10", "prev_high_10", "prev_short_count_20",
            "prev_relief_count_8", "is_short_phase", "is_relief_phase",
            "new_breakdown", "first_short_after_pause", "reload_after_relief",
            "ask_distribution_turn", "phase_transition_ok"]
    a = loop.sort_values(key).reset_index(drop=True)[cols]
    b = vec.sort_values(key).reset_index(drop=True)[cols]
    pd.testing.assert_frame_equal(a, b, check_dtype=False, check_exact=True)


def test_did_pre_mean_fast_equals_loop(synth_db):
    con = tse.connect_db_readonly(synth_db)
    scored = tse.score_universe_from_db(con, _DAYS)
    nifty = tse.load_nifty_context(con, _DAYS)
    con.close()
    df = tse.grade_scored_frame(scored, nifty, _DAYS[:-1], did_layer_enabled=False)
    slow = tdid.apply_did_layer(df, fast=False)
    fast = tdid.apply_did_layer(df, fast=True)
    key = ["instrument", "day", "bar_time"]
    num = [c for c in slow.columns
           if c in fast.columns and pd.api.types.is_numeric_dtype(slow[c])]
    a = slow.sort_values(key).reset_index(drop=True)[num]
    b = fast.sort_values(key).reset_index(drop=True)[num]
    pd.testing.assert_frame_equal(a, b, check_dtype=False, check_exact=True)


# ── (2) SCORED-FRAME parity: all columns, all symbols, all minutes ────────────

def test_scored_frame_vectorized_equals_oracle_all_columns(synth_db):
    """score_universe_for_symbols(vectorized=True) == the per-symbol oracle
    score_universe_from_db on EVERY shared column — this frame is every minute of
    every symbol, so it is the full-day/all-symbol/all-minute feature-layer proof."""
    con = tse.connect_db_readonly(synth_db)
    oracle = tse.score_universe_from_db(con, _DAYS)                     # per-symbol
    vec = tse.score_universe_for_symbols(con, _DAYS, _CASH, vectorized=True)
    con.close()
    key = ["instrument", "day", "bar_time"]
    num = [c for c in oracle.columns
           if c in vec.columns and pd.api.types.is_numeric_dtype(oracle[c])]
    a = oracle.sort_values(key).reset_index(drop=True)
    b = vec.sort_values(key).reset_index(drop=True)
    assert len(a) == len(b) and len(a) == len(_CASH) * len(_DAYS) * _NBARS
    assert len(num) > 40
    pd.testing.assert_frame_equal(a[num], b[num], check_dtype=False,
                                  check_exact=True)


# ── (3) GRADED-FRAME parity: v2 gates + v3/DiD, all columns ──────────────────

@pytest.mark.parametrize("did", [False, True])
def test_graded_frame_vectorized_equals_oracle(synth_db, did):
    con = tse.connect_db_readonly(synth_db)
    train_days = _DAYS[:-1]
    infer_day = _DAYS[-1]
    syms = _CASH
    train_scored = tse.score_universe_for_symbols(con, train_days, syms,
                                                  vectorized=True)
    infer_scored = tse.score_universe_for_symbols(con, [infer_day], syms,
                                                  vectorized=True)
    train_nifty = tse.load_nifty_context(con, train_days)
    infer_nifty = tse.load_nifty_context(con, [infer_day])
    con.close()

    scored = pd.concat([train_scored, infer_scored], ignore_index=True)
    nctx = [f for f in (train_nifty, infer_nifty) if not f.empty]
    nifty = pd.concat(nctx, ignore_index=True) if nctx else pd.DataFrame()
    # ORACLE: full-frame grade (per-group loops), restricted to infer rows.
    oracle = tse.grade_scored_frame(scored, nifty, train_days, did_layer_enabled=did)
    oracle_infer = oracle[oracle["day"] == infer_day].copy()
    # FAST: infer-only grade with cached train stats + vectorised primitives.
    profile, quality = tse.compute_train_stats(train_scored, train_nifty, train_days)
    fast = tse.grade_infer_only(infer_scored, infer_nifty, train_days,
                                profile, quality, did_layer_enabled=did,
                                vectorized=True)
    key = ["instrument", "day", "bar_time"]
    num = [c for c in oracle_infer.columns
           if c in fast.columns and pd.api.types.is_numeric_dtype(oracle_infer[c])]
    a = oracle_infer.sort_values(key).reset_index(drop=True)
    b = fast.sort_values(key).reset_index(drop=True)
    assert len(a) == len(b) and len(a) > 0 and len(num) > 30
    pd.testing.assert_frame_equal(a[num], b[num], check_dtype=False,
                                  check_exact=True)


# ── (4) END-TO-END minute-by-minute: fast(vectorized) == full recompute ───────

def _sig_set(res):
    return {(s.instrument, s.day, s.time, s.grade, s.setup,
             round(float(s.entry_ref_price), 10)) for s in res.signals}


@pytest.mark.parametrize("did", [False, True])
def test_minute_by_minute_vectorized_matches_full_recompute(synth_db, did):
    """For a sweep of as_of minutes spanning the whole infer day (incl. the last-15
    provisional tail), the vectorised fast path yields the byte-identical signal set
    as the full-recompute oracle — for BOTH v2 and v3/DiD."""
    infer_day = _DAYS[-1]
    base = datetime(2026, 7, 9, 9, 15, 0)
    # Every 15th minute across the day + the final provisional-tail minutes.
    sweep = list(range(20, _NBARS, 15)) + [_NBARS - 12, _NBARS - 5, _NBARS - 1]
    checked = 0
    for i in sorted(set(sweep)):
        as_of = (base + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M")
        kw = dict(infer_day=infer_day, as_of=as_of, db_path=synth_db,
                  personality_window_days=3, min_grade="A++", cooldown_minutes=15,
                  latest_only=True, did_layer_enabled=did)
        tse.reset_train_cache()
        oracle = tse.compute_live_signals(**kw)
        tse.reset_train_cache()
        fast = tse.compute_live_signals_fast(vectorized=True, **kw)
        assert _sig_set(oracle) == _sig_set(fast), (
            f"signal-set divergence at as_of={as_of} did={did}: "
            f"oracle={_sig_set(oracle)} fast={_sig_set(fast)}")
        assert oracle.train_days == fast.train_days
        checked += 1
    assert checked >= 20
    # MUTATION: break cum_volume (groupby.cumsum) or grade the infer WITHOUT the
    # full minute series → a feature/gate diverges → some minute's set differs.
