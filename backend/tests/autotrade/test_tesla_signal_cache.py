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


# ── full-path parity: fast latest-minute signals == full recompute ──────────

def _grade_stub(scored, nifty_ctx, train_days, did_layer_enabled=False):
    """Deterministic non-empty grade applied to BOTH paths: mark every AAA row
    A++. select_trade_lifecycle + the infer-day + latest-minute filters then pick
    the same single latest AAA bar in each path (the scored frames are equal per
    the scorer-parity test), so any assembly divergence would surface.

    Accepts (and ignores) did_layer_enabled so the stub matches the real
    grade_scored_frame signature — this v2-path test always runs with it False."""
    out = scored.copy()
    out["v2_grade"] = "IGNORE"
    out["v2_setup"] = "SHORT_RELOAD_OR_BREAKDOWN"
    out.loc[out["instrument"] == "AAA", "v2_grade"] = "A++"
    return out


def test_fast_signals_match_full_recompute(synth_db, monkeypatch):
    monkeypatch.setattr(tse, "grade_scored_frame", _grade_stub)
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
