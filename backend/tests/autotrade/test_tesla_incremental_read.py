"""Falcon Tesla — INCREMENTAL infer-day read PARITY PROOF (round-2 warm-tick opt).

The sub-10s round-2 path (config `tesla_incremental_read=true`, engine kwarg
`incremental=True`) stops re-reading the whole infer day from the poll DB every
tick. It caches the FINALIZED infer-day bars in-process and each tick reads only
the NEW minute(s) (WHERE bar_time > cached_max), then runs the SAME vectorised
`_finalize_scored_from_raw` on the reassembled frame.

This is BYTE-SAFE by construction, not an approximation: scripts/mkt_poller.py
writes each 1-min bar EXACTLY ONCE, AFTER the minute closes, via INSERT OR IGNORE,
and never rewrites an older bar — so the stored bars are immutable + monotonic and
a cached copy is provably identical to a fresh full read. This drives a REAL-MONEY
intraday SHORT signal, so parity is SACRED. This module proves it end to end, with
the DB actually GROWING minute-by-minute (the real poller behaviour) and a fresh
symbol crossing the candidate threshold mid-day (the new-symbol history-read path):

  (1) RAW reassembly parity — after incremental accumulation across appends,
      _incremental_infer_raw(...) equals _pull_cash_symbols_batch(full read) on
      EVERY column, EVERY symbol, EVERY minute.
  (2) CANDIDATE parity — _incremental_infer_candidates == candidate_symbols at
      every DB state.
  (3) END-TO-END minute-by-minute — as the DB grows, compute_live_signals_fast(
      incremental=True, vectorized=True) yields the IDENTICAL signal set as the
      full-recompute oracle compute_live_signals, for BOTH v2 and v3/DiD — while
      the counters confirm the reads were genuinely INCREMENTAL (one cold build,
      the rest tail reads).

MUTATION (verified live during the build): changing the tail read predicate from
`bar_time > cached_max` to `>=` (double-counting the boundary minute), or dropping
the new-symbol history read, makes the reassembled raw diverge from a full read →
tests (1) and (3) FAIL. Restored → all green.
"""
import sqlite3
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from autotrade.strategies import tesla_short_engine as tse

_DAYS = ["2026-07-06", "2026-07-07", "2026-07-08", "2026-07-09"]
_TRAIN = _DAYS[:-1]
_INFER = _DAYS[-1]
# AAA..FFF trade every day (train candidates). GGG appears ONLY on the infer day
# and crosses the >=300-row candidate threshold mid-day → exercises the
# new-symbol history-read branch of the incremental raw cache.
_CASH = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]
_INFER_ONLY = "GGG"
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


def _bars(symbol, segment, day, n=_NBARS):
    """Deterministic synthetic minute bars for one (symbol, day). Returns
    (of_rows, tr_rows); of_rows[i][3] and tr_rows[i][1] are the bar_time text."""
    rng = np.random.default_rng(abs(hash((symbol, day))) % (2**32))
    ikey = f"{symbol}|{segment}"
    base = datetime(int(day[:4]), int(day[5:7]), int(day[8:10]), 9, 15, 0)
    of_rows, tr_rows = [], []
    price = 100.0 + rng.uniform(-5, 5)
    atp = price
    drift = rng.uniform(-0.02, 0.005)   # bias some sessions DOWN → SHORT fires
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


def _new_db(path):
    con = sqlite3.connect(str(path))
    con.execute(f"CREATE TABLE mkt_orderflow_1min ({', '.join(_OF_COLS)}, "
                "PRIMARY KEY (instrument_key, bar_time))")
    con.execute(f"CREATE TABLE mkt_trades_1min ({', '.join(_TR_COLS)}, "
                "PRIMARY KEY (instrument_key, bar_time))")
    con.execute("CREATE INDEX idx_mktof_sym_dt ON mkt_orderflow_1min(symbol, bar_time)")
    con.execute("CREATE INDEX idx_mkttr_ik_dt ON mkt_trades_1min(instrument_key, bar_time)")
    con.execute("CREATE TABLE mkt_reference (symbol TEXT, segment TEXT, "
                "instrument_key TEXT, sector TEXT, company TEXT, lot_size INTEGER)")
    sectors = {"AAA": "TECH", "BBB": "TECH", "CCC": "TECH",
               "DDD": "BANK", "EEE": "BANK", "FFF": "BANK", "GGG": "TECH"}
    con.executemany("INSERT INTO mkt_reference VALUES (?,?,?,?,?,?)",
                    [(s, "CASH", f"{s}|CASH", sectors[s], s + " Ltd", 1)
                     for s in _CASH + [_INFER_ONLY]])
    con.commit()
    return con


def _insert(con, of_rows, tr_rows):
    if of_rows:
        con.executemany(
            f"INSERT OR IGNORE INTO mkt_orderflow_1min VALUES "
            f"({','.join(['?'] * len(_OF_COLS))})", of_rows)
    if tr_rows:
        con.executemany(
            f"INSERT OR IGNORE INTO mkt_trades_1min VALUES "
            f"({','.join(['?'] * len(_TR_COLS))})", tr_rows)
    con.commit()


def _gen_all():
    """All rows, grouped. Returns (train_of, train_tr, infer_of, infer_tr) where
    the infer lists carry (bar_time, of_row) / (bar_time, tr_row) for phased
    inserts (append the day minute-by-minute like the live poller)."""
    train_of, train_tr = [], []
    for day in _TRAIN:
        for sym in _CASH:
            of, tr = _bars(sym, "CASH", day)
            train_of += of
            train_tr += tr
        of, _ = _bars("NIFTY", "FUT", day)
        train_of += of
    infer_of, infer_tr = [], []
    for sym in _CASH + [_INFER_ONLY]:
        of, tr = _bars(sym, "CASH", _INFER)
        infer_of += [(r[3], r) for r in of]
        infer_tr += [(r[1], r) for r in tr]
    nof, _ = _bars("NIFTY", "FUT", _INFER)
    infer_of += [(r[3], r) for r in nof]
    return train_of, train_tr, infer_of, infer_tr


def _slice(rows_bt, lo_excl, hi_incl):
    """Rows with lo_excl < bar_time <= hi_incl (bar_time text is sortable)."""
    return [r for (bt, r) in rows_bt
            if (lo_excl is None or bt > lo_excl) and bt <= hi_incl]


# ── (1) RAW reassembly parity: incremental cache == full read, all columns ────

def test_incremental_raw_equals_full_read(tmp_path):
    path = tmp_path / "poll.db"
    con = _new_db(path)
    train_of, train_tr, infer_of, infer_tr = _gen_all()
    _insert(con, train_of, train_tr)
    tse.reset_train_cache()
    base = datetime(2026, 7, 9, 9, 15, 0)
    cutoffs = [(base + timedelta(minutes=m)).strftime("%Y-%m-%d %H:%M:%S")
               for m in (149, 259, 374)]
    db_key = path.as_posix()
    prev = None
    saw_new_symbol_phase = False
    for c in cutoffs:
        _insert(con, _slice(infer_of, prev, c), _slice(infer_tr, prev, c))
        prev = c
        ro = tse.connect_db_readonly(path)
        train_cands = tse.candidate_symbols(ro, _TRAIN)
        infer_cands = tse._incremental_infer_candidates(ro, db_key, _INFER)
        all_syms = sorted(set(train_cands) | set(infer_cands))
        if _INFER_ONLY in all_syms:
            saw_new_symbol_phase = True
        inc = tse._incremental_infer_raw(ro, db_key, _INFER, all_syms)
        full = tse._pull_cash_symbols_batch(ro, all_syms, [_INFER])
        ro.close()
        key = ["instrument", "bar_time"]
        a = inc.sort_values(key).reset_index(drop=True)
        b = full.sort_values(key).reset_index(drop=True)
        assert len(a) == len(b) and len(a) > 0
        shared = [col for col in b.columns if col in a.columns]
        assert len(shared) > 40
        pd.testing.assert_frame_equal(a[shared], b[shared], check_dtype=False,
                                      check_exact=True)
    con.close()
    assert saw_new_symbol_phase, "GGG should have entered candidacy mid-day"
    assert tse._infer_raw_build_count == 1      # exactly one cold build
    assert tse._infer_raw_incr_count >= 2       # the rest were incremental


# ── (2) CANDIDATE parity: incremental counts == full candidate_symbols ────────

def test_incremental_candidates_equal_full(tmp_path):
    path = tmp_path / "poll.db"
    con = _new_db(path)
    train_of, train_tr, infer_of, infer_tr = _gen_all()
    _insert(con, train_of, train_tr)
    tse.reset_train_cache()
    base = datetime(2026, 7, 9, 9, 15, 0)
    cutoffs = [(base + timedelta(minutes=m)).strftime("%Y-%m-%d %H:%M:%S")
               for m in (149, 259, 320, 374)]
    db_key = path.as_posix()
    prev = None
    for c in cutoffs:
        _insert(con, _slice(infer_of, prev, c), _slice(infer_tr, prev, c))
        prev = c
        ro = tse.connect_db_readonly(path)
        inc = set(tse._incremental_infer_candidates(ro, db_key, _INFER))
        full = set(tse.candidate_symbols(ro, [_INFER]))
        ro.close()
        assert inc == full, f"candidate divergence at {c}: inc-only={inc-full} full-only={full-inc}"
    con.close()


# ── (3) END-TO-END minute-by-minute: incremental == full recompute ────────────

def _sig_set(res):
    return {(s.instrument, s.day, s.time, s.grade, s.setup,
             round(float(s.entry_ref_price), 10)) for s in res.signals}


@pytest.mark.parametrize("did", [False, True])
def test_end_to_end_incremental_matches_full_recompute(tmp_path, did):
    """As the infer day grows phase by phase (the poller's monotonic append), the
    incremental fast path yields the byte-identical signal set as the full-recompute
    oracle — v2 AND v3/DiD — while the read counters confirm genuinely INCREMENTAL
    reads (one cold build, the rest tail reads)."""
    path = tmp_path / "poll.db"
    con = _new_db(path)
    train_of, train_tr, infer_of, infer_tr = _gen_all()
    _insert(con, train_of, train_tr)
    tse.reset_train_cache()                        # clean slate (+resets infer caches)
    base = datetime(2026, 7, 9, 9, 15, 0)
    cutoffs = [(base + timedelta(minutes=m)).strftime("%Y-%m-%d %H:%M:%S")
               for m in (149, 259, 374)]           # GGG crosses 300 by the last
    prev = None
    checked = 0
    saw_candidates = False
    for c in cutoffs:
        _insert(con, _slice(infer_of, prev, c), _slice(infer_tr, prev, c))
        prev = c
        as_of = c[:16]                             # "YYYY-MM-DD HH:MM"
        kw = dict(infer_day=_INFER, as_of=as_of, db_path=path,
                  personality_window_days=3, min_grade="A++", cooldown_minutes=15,
                  latest_only=True, did_layer_enabled=did)
        oracle = tse.compute_live_signals(**kw)               # full re-read, no cache
        fast = tse.compute_live_signals_fast(                 # incremental + vectorised
            vectorized=True, incremental=True, **kw)
        assert oracle.train_days == fast.train_days
        assert oracle.n_candidates == fast.n_candidates
        assert _sig_set(oracle) == _sig_set(fast), (
            f"signal-set divergence at as_of={as_of} did={did}: "
            f"oracle={_sig_set(oracle)} fast={_sig_set(fast)}")
        saw_candidates = saw_candidates or fast.n_candidates >= len(_CASH)
        checked += 1
    con.close()
    assert checked == 3
    assert saw_candidates                          # machinery non-trivially exercised
    assert tse._infer_raw_build_count == 1         # exactly one cold build
    assert tse._infer_raw_incr_count >= 2          # the rest were incremental tail reads
