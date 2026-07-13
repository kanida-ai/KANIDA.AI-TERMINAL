"""Falcon Tesla — signal-cache + once-per-minute refresher + staleness gate.

Makes the tesla_short strategy RUN-READY: the ~120s full-universe recompute must
NEVER run inline on the 5s tick. Proves:
  (a) the tesla tick READS the cache and does NOT call the heavy full recompute
      inline (call-count on compute_live_signals / compute_live_signals_fast == 0),
  (b) the refresher recomputes AT MOST once per 1-min bar (a 2nd call in the same
      minute returns the cache — no recompute),
  (c) a STALE cache (older than the bound) → the tick abstains from NEW seat
      entries (returns []); exits are unaffected,
  (d) the DAILY (train-day) profile cache rebuilds on a DAY-ROLL, not per minute,
  + PARITY: the optimized fast path produces the SAME scored rows / the SAME
      latest-minute signals as the full recompute.

Each proof has a MUTATION note (what to break to make it fail).
"""
import sqlite3
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from autotrade import compute_offload as co
from autotrade.compute_offload import run_offloaded
from autotrade.config import TradingSessionConfig
from autotrade.session import TradingSession
from autotrade.strategies import tesla_short_engine as tse
from autotrade.strategies import tesla_signal_cache as sc
from autotrade.strategies.tesla_short_engine import TeslaSignal, TeslaSignalResult

IST = timezone(timedelta(hours=5, minutes=30))


# ── helpers ──────────────────────────────────────────────────────────────────

def _sig(sym, grade="A++"):
    return TeslaSignal(instrument=sym, day="2026-07-10", time="09:30",
                       bar_time="2026-07-10 09:30:00", grade=grade,
                       setup="SHORT_RELOAD_OR_BREAKDOWN", entry_ref_price=100.0,
                       short_drive=0.7, sector="S")


def _result(sigs):
    return TeslaSignalResult(as_of=None, infer_day="2026-07-10", train_days=[],
                             signals=list(sigs))


class _RecomputeSpy:
    """Stub for tesla_signal_cache._recompute — counts calls, returns fixed sigs."""
    def __init__(self, sigs):
        self.calls = 0
        self.sigs = sigs

    def __call__(self, **kw):
        self.calls += 1
        return _result(self.sigs)


@pytest.fixture(autouse=True)
def _clean_caches():
    sc.reset_cache()
    tse.reset_train_cache()
    yield
    sc.reset_cache()
    tse.reset_train_cache()


# ── (b) once-per-minute guard ────────────────────────────────────────────────

def test_refresher_recomputes_at_most_once_per_minute(monkeypatch):
    spy = _RecomputeSpy([_sig("AAA")])
    monkeypatch.setattr(sc, "_recompute", spy)
    t0 = datetime(2026, 7, 10, 10, 0, 0, tzinfo=IST)
    assert sc.refresh_if_needed(now=t0, block=True) is True
    assert spy.calls == 1
    # 2nd call SAME minute → cache hit, NO recompute.
    assert sc.refresh_if_needed(now=t0.replace(second=45), block=True) is False
    assert spy.calls == 1
    # next minute → a fresh recompute.
    assert sc.refresh_if_needed(now=t0.replace(minute=1, second=2),
                                block=True) is True
    assert spy.calls == 2
    # MUTATION: drop the `entry.minute_key == minute_key` guard in
    # refresh_if_needed → the 2nd same-minute call recomputes → spy.calls==2 here.


def test_inflight_guard_blocks_concurrent_refresh(monkeypatch):
    spy = _RecomputeSpy([_sig("AAA")])
    monkeypatch.setattr(sc, "_recompute", spy)
    key = sc._cache_key(None, 5, "A++")
    # Simulate a refresh already in flight for THIS minute-key params.
    sc._INFLIGHT[key] = True
    t0 = datetime(2026, 7, 10, 10, 0, 0, tzinfo=IST)
    assert sc.refresh_if_needed(now=t0, block=True) is False
    assert spy.calls == 0


# ── cache read ───────────────────────────────────────────────────────────────

def test_get_signals_returns_cached_after_refresh(monkeypatch):
    spy = _RecomputeSpy([_sig("AAA"), _sig("BBB")])
    monkeypatch.setattr(sc, "_recompute", spy)
    t0 = datetime(2026, 7, 10, 10, 0, 0, tzinfo=IST)
    sc.refresh_if_needed(now=t0, block=True)
    sigs, stale = sc.get_signals(now=t0.replace(second=5), staleness_bound_sec=90)
    assert stale is False
    assert {s.instrument for s in sigs} == {"AAA", "BBB"}


# ── (c) staleness gate ───────────────────────────────────────────────────────

def test_stale_cache_is_flagged_stale(monkeypatch):
    spy = _RecomputeSpy([_sig("AAA")])
    monkeypatch.setattr(sc, "_recompute", spy)
    t0 = datetime(2026, 7, 10, 10, 0, 0, tzinfo=IST)
    sc.refresh_if_needed(now=t0, block=True)
    # within bound → fresh
    _, fresh = sc.get_signals(now=t0 + timedelta(seconds=60),
                              staleness_bound_sec=90)
    assert fresh is False
    # beyond bound → stale
    sigs, stale = sc.get_signals(now=t0 + timedelta(seconds=200),
                                 staleness_bound_sec=90)
    assert stale is True
    # MUTATION: `age > bound` → `False` in get_signals → stale never True here.


def test_cold_cache_is_stale_and_empty():
    sigs, stale = sc.get_signals(now=datetime(2026, 7, 10, 10, 0, tzinfo=IST),
                                 staleness_bound_sec=90)
    assert sigs == [] and stale is True


def test_disabled_staleness_bound_never_stale(monkeypatch):
    spy = _RecomputeSpy([_sig("AAA")])
    monkeypatch.setattr(sc, "_recompute", spy)
    t0 = datetime(2026, 7, 10, 10, 0, 0, tzinfo=IST)
    sc.refresh_if_needed(now=t0, block=True)
    _, stale = sc.get_signals(now=t0 + timedelta(hours=5), staleness_bound_sec=0)
    assert stale is False


def test_recompute_error_never_raises_and_leaves_cache_stale(monkeypatch):
    def _boom(**kw):
        raise RuntimeError("poll db down")
    monkeypatch.setattr(sc, "_recompute", _boom)
    t0 = datetime(2026, 7, 10, 10, 0, 0, tzinfo=IST)
    # must NOT raise
    sc.refresh_if_needed(now=t0, block=True)
    sigs, stale = sc.get_signals(now=t0, staleness_bound_sec=90)
    assert sigs == [] and stale is True


# ── (a) the tesla tick reads the cache, never the heavy path inline ──────────

def _tesla_cfg(**kw):
    base = dict(total_allocated_capital=900000.0, strategy="tesla_short",
                direction="short", instrument_type="EQ", order_product="MIS",
                n_seats=3, per_position_gtt_enabled=False,
                per_stock_stop_pct=0.03,
                mis_square_off_time="23:58:00", square_off_time="23:59:00")
    base.update(kw)
    return TradingSessionConfig(**base)


def test_tick_signal_source_uses_cache_not_inline_heavy(monkeypatch):
    """_tesla_live_signals reads the cache (get_signals) and NEVER calls the heavy
    full recompute inline. Proves the 5s tick can't stall on the ~120s path."""
    heavy = _RecomputeSpy([_sig("Z")])       # spy: full recompute must be 0 calls
    fast = _RecomputeSpy([_sig("Z")])        # spy: fast recompute must be 0 inline
    monkeypatch.setattr(tse, "compute_live_signals", heavy)
    monkeypatch.setattr(tse, "compute_live_signals_fast", fast)

    consulted = {"refresh": 0, "get": 0}

    def _fake_refresh(**kw):
        consulted["refresh"] += 1
        return False                         # no thread; simulate cache warm

    def _fake_get(**kw):
        consulted["get"] += 1
        return [_sig("AAA"), _sig("BBB")], False

    monkeypatch.setattr(sc, "refresh_if_needed", _fake_refresh)
    monkeypatch.setattr(sc, "get_signals", _fake_get)

    sess = TradingSession.create(_tesla_cfg(), mode="paper")
    out = sess._tesla_live_signals()
    assert {s.instrument for s in out} == {"AAA", "BBB"}
    assert consulted["refresh"] == 1 and consulted["get"] == 1   # cache consulted
    assert heavy.calls == 0 and fast.calls == 0                  # no inline recompute
    # MUTATION: inline `tse.compute_live_signals(...)` in _tesla_live_signals →
    # heavy.calls == 1 → fails.


def test_tick_signal_source_abstains_when_cache_stale(monkeypatch):
    """A STALE cache → _tesla_live_signals returns [] (no new seat entries)."""
    monkeypatch.setattr(sc, "refresh_if_needed", lambda **kw: True)
    monkeypatch.setattr(sc, "get_signals", lambda **kw: ([_sig("AAA")], True))
    sess = TradingSession.create(_tesla_cfg(), mode="paper")
    assert sess._tesla_live_signals() == []
    # MUTATION: ignore the stale flag (return signals regardless) → returns
    # [AAA] here → fails.


def test_tick_signal_source_returns_signals_when_fresh(monkeypatch):
    monkeypatch.setattr(sc, "refresh_if_needed", lambda **kw: False)
    monkeypatch.setattr(sc, "get_signals", lambda **kw: ([_sig("AAA")], False))
    sess = TradingSession.create(_tesla_cfg(), mode="paper")
    out = sess._tesla_live_signals()
    assert [s.instrument for s in out] == ["AAA"]


# ── synthetic poll DB (real order-flow columns) ──────────────────────────────

_DAYS = ["2026-07-06", "2026-07-08", "2026-07-09", "2026-07-10"]
_CASH = ["AAA", "BBB", "CCC"]
_OF_COLS = [
    "symbol", "segment", "instrument_key", "bar_time", "open", "high", "low",
    "close", "volume", "atp", "oi", "total_buy_qty", "total_sell_qty",
    "b1p", "b1q", "b2p", "b2q", "b3p", "b3q", "b4p", "b4q", "b5p", "b5q",
    "a1p", "a1q", "a2p", "a2q", "a3p", "a3q", "a4p", "a4q", "a5p", "a5q",
    "last_qty", "b1o", "b2o", "b3o", "b4o", "b5o", "a1o", "a2o", "a3o", "a4o", "a5o",
]
_TR_COLS = ["instrument_key", "bar_time", "n_ticks", "buy_vol", "sell_vol",
            "buy_vol_pct", "avg_tick_vol", "max_tick_vol", "max_trade_qty"]


def _bars(symbol, segment, day, n=305, seed=0):
    rng = np.random.default_rng(abs(hash((symbol, day, seed))) % (2**32))
    ikey = f"{symbol}|{segment}"
    base = datetime(int(day[:4]), int(day[5:7]), int(day[8:10]), 9, 15, 0)
    of_rows, tr_rows = [], []
    price = 100.0 + rng.uniform(-5, 5)
    atp = price
    for i in range(n):
        t = (base + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S")
        price = max(1.0, price + rng.normal(0, 0.05))
        atp = atp + (price - atp) * 0.1
        hi, lo = price + abs(rng.normal(0, 0.05)), price - abs(rng.normal(0, 0.05))
        vol = float(max(1.0, rng.normal(1500, 400)))
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
    path = tmp_path_factory.mktemp("teslapoll") / "poll.db"
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
    con.executemany("INSERT INTO mkt_reference VALUES (?,?,?,?,?,?)",
                    [(s, "CASH", f"{s}|CASH", "TECH", s + " Ltd", 1) for s in _CASH])
    con.commit()
    con.close()
    return path


# ── scorer parity: score_universe_for_symbols == score_universe_from_db ──────

def test_scorer_for_symbols_matches_from_db(synth_db):
    con = tse.connect_db_readonly(synth_db)
    days = ["2026-07-08", "2026-07-09"]
    full = tse.score_universe_from_db(con, days)
    syms = tse.candidate_symbols(con, days)
    fast = tse.score_universe_for_symbols(con, days, syms)
    con.close()
    assert not full.empty and set(syms) == set(_CASH)
    cols = ["instrument", "day", "time", "short_drive", "long_drive",
            "atp_torque", "net_aggression", "volume_ratio", "close_atp_gap_bps",
            "falcon_phase", "sector"]
    a = full[cols].sort_values(["instrument", "day", "time"]).reset_index(drop=True)
    b = fast[cols].sort_values(["instrument", "day", "time"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(a, b, check_dtype=False)


def test_batched_read_matches_per_symbol(synth_db):
    """The BATCHED DB read (_pull_cash_symbols_batch, one IN(...) query) returns
    exactly the same raw rows as looping the per-symbol _pull_cash_symbol — this
    is the opt-1 (N+1 → 1 query) parity floor: same rows, same values."""
    con = tse.connect_db_readonly(synth_db)
    days = ["2026-07-08", "2026-07-09"]
    syms = tse.candidate_symbols(con, days)
    parts = []
    for s in syms:
        r = tse._pull_cash_symbol(con, s, days)
        if not r.empty:
            parts.append(r)
    ref = pd.concat(parts, ignore_index=True)
    batch = tse._pull_cash_symbols_batch(con, syms, days)
    con.close()
    key = ["instrument", "day", "bar_time"]
    cols = [c for c in ref.columns if c in batch.columns]
    a = ref[cols].sort_values(key).reset_index(drop=True)
    b = batch[cols].sort_values(key).reset_index(drop=True)
    assert len(a) == len(b) and len(a) > 0
    pd.testing.assert_frame_equal(a, b, check_dtype=False, check_exact=True)
    # MUTATION: drop the `dayset` day-filter in _pull_cash_symbols_batch (so the
    # range pulls extra days) or change the WHERE symbol IN clause → rows differ.


def test_batched_read_drops_unlisted_days(synth_db):
    """The batched range read [min(days), max(days)+1) must return ONLY the listed
    days — a non-contiguous window (skipping 07-08, which HAS data inside the range)
    proves the dayset filter drops the unlisted intermediate day."""
    con = tse.connect_db_readonly(synth_db)
    days = ["2026-07-06", "2026-07-09"]      # 07-08 is inside the range but unlisted
    syms = tse.candidate_symbols(con, days)
    batch = tse._pull_cash_symbols_batch(con, syms, days)
    con.close()
    assert set(batch["day"].unique()) == {"2026-07-06", "2026-07-09"}
    # MUTATION: drop the `dayset` filter in _pull_cash_symbols_batch → 07-08 rows
    # leak in → the day set becomes {06,08,09}.


def test_batched_scored_frame_matches_per_symbol_all_columns(synth_db):
    """The full BATCHED scored frame (score_universe_for_symbols) equals the
    per-symbol reference (score_universe_from_db) on EVERY shared column, exactly
    — every microstructure/drive/absorption feature is byte-identical."""
    con = tse.connect_db_readonly(synth_db)
    days = ["2026-07-08", "2026-07-09"]
    full = tse.score_universe_from_db(con, days)          # per-symbol reference
    syms = tse.candidate_symbols(con, days)
    fast = tse.score_universe_for_symbols(con, days, syms)  # batched
    con.close()
    key = ["instrument", "day", "bar_time"]
    num = [c for c in full.columns
           if c in fast.columns and pd.api.types.is_numeric_dtype(full[c])]
    a = full.sort_values(key).reset_index(drop=True)
    b = fast.sort_values(key).reset_index(drop=True)
    assert len(a) == len(b) and len(num) > 30
    pd.testing.assert_frame_equal(a[num], b[num], check_dtype=False,
                                  check_exact=True)
    # MUTATION: batch a different symbol/day set → a feature column diverges.


def _grade_pair(con, train_days, infer_day, did=False):
    tc = tse.candidate_symbols(con, train_days)
    ic = tse.candidate_symbols(con, [infer_day])
    syms = sorted(set(tc) | set(ic))
    train_scored = tse.score_universe_for_symbols(con, train_days, syms)
    infer_scored = tse.score_universe_for_symbols(con, [infer_day], syms)
    train_nifty = tse.load_nifty_context(con, train_days)
    infer_nifty = tse.load_nifty_context(con, [infer_day])
    scored = pd.concat([train_scored, infer_scored], ignore_index=True)
    nctx = ([f for f in (train_nifty, infer_nifty) if not f.empty])
    nifty = pd.concat(nctx, ignore_index=True) if nctx else pd.DataFrame()
    full = tse.grade_scored_frame(scored, nifty, train_days, did_layer_enabled=did)
    full_infer = full[full["day"] == infer_day].copy()
    profile, quality = tse.compute_train_stats(train_scored, train_nifty, train_days)
    fast = tse.grade_infer_only(infer_scored, infer_nifty, train_days,
                                profile, quality, did_layer_enabled=did)
    return full_infer, fast


def test_grade_infer_only_matches_full_frame_grade(synth_db):
    """opt-2 parity: grading the INFER day alone with cached train stats reproduces
    grade_scored_frame(train+infer) restricted to the infer rows — byte-for-byte on
    the v2 gate/grade columns and the personality gates (which prove the cached
    profile + quality are identical to the full-frame derivation)."""
    con = tse.connect_db_readonly(synth_db)
    full_infer, fast = _grade_pair(con, ["2026-07-08", "2026-07-09"], "2026-07-10")
    con.close()
    key = ["instrument", "day", "bar_time"]
    cols = ["instrument", "day", "time", "v2_grade", "v2_setup", "base_gate",
            "personality_quality_ok", "personality_vol_gate",
            "personality_gap_gate", "personality_short_gate",
            "train_vol_p90", "short_drive", "close_atp_gap_bps", "volume_ratio"]
    a = full_infer.sort_values(key)[cols].reset_index(drop=True)
    b = fast.sort_values(key)[cols].reset_index(drop=True)
    assert len(a) == len(b) and len(a) > 0
    pd.testing.assert_frame_equal(a, b, check_dtype=False, check_exact=True)
    # MUTATION: pass the WRONG train_days to compute_train_stats (or drop the
    # quality merge in _apply_cached_quality) → personality_quality_ok / base_gate
    # diverge from the full-frame grade.


def test_grade_infer_only_matches_full_frame_grade_did(synth_db):
    """opt-2 parity WITH the v3 DiD layer on: the infer-only DiD block + v3 regrade
    equal the full-frame DiD for the infer rows (did_score / v3_grade / v3_rank_score
    byte-identical — the pre5 rolling means are per-(instrument,day) infer-local)."""
    con = tse.connect_db_readonly(synth_db)
    full_infer, fast = _grade_pair(con, ["2026-07-08", "2026-07-09"], "2026-07-10",
                                   did=True)
    con.close()
    key = ["instrument", "day", "bar_time"]
    cols = ["instrument", "day", "time", "v3_grade", "v3_setup", "v3_did_gate",
            "did_score", "v3_rank_score", "did_short_vs_sector",
            "did5_short_vs_sector", "did5_move_vs_sector_pct"]
    a = full_infer.sort_values(key)[cols].reset_index(drop=True)
    b = fast.sort_values(key)[cols].reset_index(drop=True)
    assert len(a) == len(b) and len(a) > 0
    pd.testing.assert_frame_equal(a, b, check_dtype=False, check_exact=True)
    # MUTATION: grade the infer frame WITHOUT the full intraday minute series (e.g.
    # collapse to the latest bar before add_did_features) → the pre5 means and thus
    # did_score / v3_rank_score diverge.


# ── full-path parity: fast latest-minute signals == full recompute ──────────

def _mark_aaa(out):
    out = out.copy()
    out["v2_grade"] = "IGNORE"
    out["v2_setup"] = "SHORT_RELOAD_OR_BREAKDOWN"
    out.loc[out["instrument"] == "AAA", "v2_grade"] = "A++"
    return out


def _grade_stub(scored, nifty_ctx, train_days, did_layer_enabled=False):
    """Deterministic non-empty grade applied to the FULL-recompute path: mark every
    AAA row A++. select_trade_lifecycle + the infer-day + latest-minute filters then
    pick the same single latest AAA bar as the fast path, so any assembly divergence
    would surface.

    Accepts (and ignores) did_layer_enabled so the stub matches the real
    grade_scored_frame signature — this v2-path test always runs with it False."""
    return _mark_aaa(scored)


def _grade_infer_stub(infer_scored, infer_nifty, train_days, profile, quality,
                      did_layer_enabled=False):
    """The fast-path counterpart stub (grade_infer_only signature): same AAA-A++
    marking on the infer-only frame, so both paths select the identical bar."""
    return _mark_aaa(infer_scored)


def test_fast_signals_match_full_recompute(synth_db, monkeypatch):
    monkeypatch.setattr(tse, "grade_scored_frame", _grade_stub)
    monkeypatch.setattr(tse, "grade_infer_only", _grade_infer_stub)
    kw = dict(infer_day="2026-07-10", db_path=synth_db,
              personality_window_days=2, min_grade="A++", cooldown_minutes=1,
              latest_only=True)
    full = tse.compute_live_signals(**kw)
    tse.reset_train_cache()
    fast = tse.compute_live_signals_fast(**kw)
    fk = {(s.instrument, s.day, s.time, s.grade) for s in full.signals}
    ffk = {(s.instrument, s.day, s.time, s.grade) for s in fast.signals}
    assert fk and fk == ffk        # non-empty AND identical latest-minute set
    # MUTATION: score the infer day for the WRONG symbol set (e.g. infer-only
    # candidates) in compute_live_signals_fast → the assembled frame diverges →
    # fk != ffk.


# ── (d) daily (train-day) cache rebuilds on a DAY-ROLL, not per minute ───────

def test_daily_cache_rebuilds_on_day_roll_not_per_minute(synth_db, monkeypatch):
    tse.reset_train_cache()
    kw = dict(db_path=synth_db, personality_window_days=2, min_grade="A++",
              cooldown_minutes=30, latest_only=True)
    # Same infer day, two minutes → the train-day cache builds ONCE.
    tse.compute_live_signals_fast(infer_day="2026-07-10",
                                  as_of="2026-07-10 10:00", **kw)
    assert tse._train_cache_build_count == 1
    tse.compute_live_signals_fast(infer_day="2026-07-10",
                                  as_of="2026-07-10 10:05", **kw)
    assert tse._train_cache_build_count == 1        # NO rebuild within the day
    # Day roll (train window shifts) → exactly one rebuild.
    tse.compute_live_signals_fast(infer_day="2026-07-09",
                                  as_of="2026-07-09 10:00", **kw)
    assert tse._train_cache_build_count == 2
    # MUTATION: rebuild the train cache every call (drop the train_days-equality
    # short-circuit in _get_train_cache) → build_count == 2 after the 2nd call.


# ── OFF-PROCESS refresh: the heavy recompute runs in a SEPARATE process ───────
#
# WHY: the backend is a single-worker uvicorn (one async loop). The recompute is
# heavy pandas — it HOLDS the GIL — so running it on a daemon *thread* still
# stalls the event loop. The non-blocking refresh path therefore runs the
# recompute OFF-PROCESS (own GIL) via autotrade.compute_offload.run_offloaded.
# The block=True (test) path stays INLINE (no pool) so the swappable-_recompute
# spy pattern above remains hermetic.

def test_nonblock_refresh_routes_through_the_offprocess_pool(monkeypatch):
    """_do_refresh(use_pool=True) submits _recompute_worker to run_offloaded and
    does NOT run _recompute inline. Proves the daemon-thread path offloads the
    GIL-holding compute to a separate process."""
    inline_spy = _RecomputeSpy([_sig("INLINE")])
    monkeypatch.setattr(sc, "_recompute", inline_spy)   # must stay 0 in pool path

    captured = {}

    def _fake_offloaded(fn, /, timeout=None, **kwargs):
        captured["fn"] = fn
        captured["timeout"] = timeout
        captured["kwargs"] = kwargs
        return _result([_sig("OFFPROC")])

    # _do_refresh does `from ..compute_offload import run_offloaded` at CALL time,
    # so patching the attribute on the module is picked up.
    monkeypatch.setattr(co, "run_offloaded", _fake_offloaded)

    t0 = datetime(2026, 7, 10, 10, 0, 0, tzinfo=IST)
    key = sc._cache_key(None, 5, "A++")
    sc._INFLIGHT[key] = True                             # mimic refresh_if_needed
    sc._do_refresh(key, sc._minute_bucket(t0), t0, db_path=None,
                   personality_window_days=5, min_grade="A++",
                   cooldown_minutes=30, use_pool=True)

    assert captured.get("fn") is sc._recompute_worker    # off-process entry point
    assert captured["kwargs"]["as_of"] == "2026-07-10 10:00"
    assert inline_spy.calls == 0                          # NEVER ran inline
    sigs, _ = sc.get_signals(now=t0, staleness_bound_sec=90)
    assert [s.instrument for s in sigs] == ["OFFPROC"]    # off-process result cached
    assert sc._INFLIGHT[key] is False                     # in-flight cleared
    # MUTATION: make _do_refresh call _recompute inline in the pool path →
    # inline_spy.calls == 1 and run_offloaded never called → fails.


def test_block_path_runs_inline_with_no_pool(monkeypatch):
    """block=True → _do_refresh(use_pool=False) runs _recompute INLINE and NEVER
    touches the process pool. Keeps the test path hermetic (spy still works)."""
    inline_spy = _RecomputeSpy([_sig("INLINE")])
    monkeypatch.setattr(sc, "_recompute", inline_spy)

    def _must_not_run(*a, **k):
        raise AssertionError("run_offloaded must NOT be called on the block path")
    monkeypatch.setattr(co, "run_offloaded", _must_not_run)

    t0 = datetime(2026, 7, 10, 10, 0, 0, tzinfo=IST)
    assert sc.refresh_if_needed(now=t0, block=True) is True
    assert inline_spy.calls == 1
    sigs, _ = sc.get_signals(now=t0, staleness_bound_sec=90)
    assert [s.instrument for s in sigs] == ["INLINE"]
    # MUTATION: route the block path through the pool (use_pool=True on block) →
    # _must_not_run raises → the recompute is caught best-effort → cache empty →
    # the instrument assertion fails.


def test_offloaded_recompute_leaves_last_good_cache_on_failure(monkeypatch):
    """A broken/failed offload (run_offloaded raises OffloadError) is caught
    best-effort: the last-good cache is left in place and _do_refresh NEVER
    raises. get_signals still returns (last, stale) exactly as designed."""
    # Seed a last-good cache via the inline block path.
    monkeypatch.setattr(sc, "_recompute", _RecomputeSpy([_sig("GOOD")]))
    t0 = datetime(2026, 7, 10, 10, 0, 0, tzinfo=IST)
    sc.refresh_if_needed(now=t0, block=True)

    # Next minute: the off-process refresh blows up.
    def _boom(fn, /, timeout=None, **kwargs):
        raise co.OffloadError("worker crashed")
    monkeypatch.setattr(co, "run_offloaded", _boom)
    t1 = t0.replace(minute=1)
    key = sc._cache_key(None, 5, "A++")
    sc._INFLIGHT[key] = True
    sc._do_refresh(key, sc._minute_bucket(t1), t1, db_path=None,
                   personality_window_days=5, min_grade="A++",
                   cooldown_minutes=30, use_pool=True)   # must NOT raise

    # Within the bound of t0 → last-good still fresh; beyond → stale but present.
    sigs, stale = sc.get_signals(now=t0.replace(second=30), staleness_bound_sec=90)
    assert [s.instrument for s in sigs] == ["GOOD"] and stale is False
    sigs2, stale2 = sc.get_signals(now=t0 + timedelta(seconds=200),
                                   staleness_bound_sec=90)
    assert [s.instrument for s in sigs2] == ["GOOD"] and stale2 is True
    assert sc._INFLIGHT[key] is False
    # MUTATION: let _do_refresh propagate the OffloadError (drop the except) → the
    # call raises here → fails.


# ── PARITY: off-process recompute == in-process recompute (byte-for-byte) ─────

def _sig_tuple(s):
    return (s.instrument, s.day, s.time, s.bar_time, s.grade, s.setup,
            s.entry_ref_price, s.short_drive, s.sector, s.rank_score)


def test_offprocess_recompute_parity_synth(synth_db):
    """The OFF-PROCESS _recompute_worker returns a TeslaSignalResult with a
    signals list IDENTICAL to the SAME in-process _recompute for the same inputs
    (synthetic poll DB). This is the REVERT safety: the process boundary did NOT
    change the math (per-signal instrument/grade/short_drive/rank_score + the
    result note/train_days/n_candidates all match)."""
    kw = dict(db_path=str(synth_db), personality_window_days=2, min_grade="A++",
              cooldown_minutes=1, as_of="2026-07-10 10:00", did_layer_enabled=False)

    tse.reset_train_cache()
    inproc = sc._recompute(**kw)                     # in THIS process
    offproc = run_offloaded(sc._recompute_worker, timeout=120, **kw)  # spawned child

    assert isinstance(offproc, TeslaSignalResult)
    assert [_sig_tuple(s) for s in offproc.signals] == \
           [_sig_tuple(s) for s in inproc.signals]
    assert offproc.note == inproc.note
    assert offproc.infer_day == inproc.infer_day
    assert offproc.train_days == inproc.train_days
    assert offproc.n_candidates == inproc.n_candidates
    # MUTATION: have _recompute_worker score a different symbol set / day (or
    # drop a field on the boundary) → the per-signal tuples or note diverge.
