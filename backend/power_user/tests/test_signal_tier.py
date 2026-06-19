"""Tests for signal-time tier classification (services/signal_tier.py)."""
import sqlite3

from backend.power_user.services.signal_tier import (
    classify_signal_tier,
    enrich_picks,
    SIGNAL_TIER_ENUM,
)


# ── classifier parity (mirrors scripts/export_tier_excel.classify) ──────────

def test_premium_pullback():
    # flat/down + 2-day pullback >5% + strong pattern
    assert classify_signal_tier(sret=-0.5, twoday=-6.0, rng=3.0,
                                avg_lift=16.0, trend3_20=1.1, turn_pct=0.8) == "PREMIUM-Pullback"

def test_premium_compression():
    # flat/down + narrow range + strong pattern (no big pullback)
    assert classify_signal_tier(sret=1.0, twoday=-1.0, rng=1.5,
                                avg_lift=16.0, trend3_20=1.1, turn_pct=0.8) == "PREMIUM-Compression"

def test_enterprise_dryup():
    assert classify_signal_tier(sret=0.0, twoday=-1.0, rng=3.0,
                                avg_lift=14.0, trend3_20=0.8, turn_pct=0.9) == "ENTERPRISE-Dryup"

def test_gold():
    assert classify_signal_tier(sret=-1.0, twoday=0.0, rng=3.0,
                                avg_lift=14.0, trend3_20=1.2, turn_pct=0.5) == "GOLD"

def test_gold_baseline():
    assert classify_signal_tier(sret=-1.0, twoday=0.0, rng=3.0,
                                avg_lift=14.0, trend3_20=1.2, turn_pct=0.9) == "GOLD-baseline"

def test_standard():
    assert classify_signal_tier(sret=3.5, twoday=2.0, rng=3.0,
                                avg_lift=14.0, trend3_20=1.2, turn_pct=0.5) == "STANDARD"

def test_avoid_extended():
    assert classify_signal_tier(sret=12.0, twoday=15.0, rng=8.0,
                                avg_lift=16.0, trend3_20=1.0, turn_pct=0.9) == "AVOID"

def test_avoid_froth():
    # up >7% on heavy turnover = froth
    assert classify_signal_tier(sret=8.0, twoday=10.0, rng=5.0,
                                avg_lift=16.0, trend3_20=1.0, turn_pct=0.9) == "AVOID"

def test_5to7_high_turnover_not_avoided():
    # 5<sret<=7 on heavy turnover is a POSITIVE-edge band (55.8% WR) -> NOT AVOID.
    # Falls through to STANDARD-weak (5<sret<=10). [boundary fix 2026-06-19]
    assert classify_signal_tier(sret=6.0, twoday=8.0, rng=4.0,
                                avg_lift=16.0, trend3_20=1.0, turn_pct=0.9) == "STANDARD-weak"

def test_all_tiers_in_enum():
    for t in ("PREMIUM-Pullback", "GOLD", "AVOID", "STANDARD"):
        assert t in SIGNAL_TIER_ENUM


# ── enrich_picks against a tiny in-memory ohlc fixture ──────────────────────

def _mk_db():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute("CREATE TABLE ohlc_daily (symbol TEXT, trade_date TEXT, "
                "close REAL, high REAL, low REAL, volume REAL)")
    # 70 sessions of flat history, then a flat-down signal day after a 2-day fall
    rows = []
    for i in range(70):
        d = f"2026-01-{i+1:02d}" if i < 31 else f"2026-02-{i-30:02d}"
        rows.append(("ACME", d, 100.0, 101.0, 99.0, 1000.0))
    # overwrite last 3 days: fall then flat (pullback into signal)
    rows[-3] = ("ACME", rows[-3][1], 100.0, 100.5, 99.5, 1000.0)
    rows[-2] = ("ACME", rows[-2][1],  96.0,  97.0, 95.0, 1000.0)   # -4%
    rows[-1] = ("ACME", rows[-1][1],  94.0,  94.3, 93.9,  300.0)   # -2% day, tight, low vol
    con.executemany("INSERT INTO ohlc_daily VALUES (?,?,?,?,?,?)", rows)
    con.commit()
    return con, rows[-1][1]


def test_enrich_attaches_fields():
    con, sd = _mk_db()
    picks = [{"symbol": "ACME", "score": 160.0, "n_fires": 10}]  # avg_lift=16
    enrich_picks(con, picks, sd)
    p = picks[0]
    assert p["signal_tier"] in SIGNAL_TIER_ENUM
    assert p["signal_day_ret_pct"] is not None
    assert p["two_day_ret_pct"] is not None
    # close 94 vs prev_prev 100 ≈ -6% two-day pullback, avg_lift 16 → PREMIUM-Pullback
    assert p["two_day_ret_pct"] < -5
    assert p["signal_tier"] == "PREMIUM-Pullback"


def test_enrich_missing_symbol_is_safe():
    con, sd = _mk_db()
    picks = [{"symbol": "NOPE", "score": 160.0, "n_fires": 10}]
    enrich_picks(con, picks, sd)              # must not raise
    assert picks[0]["signal_tier"] in SIGNAL_TIER_ENUM
    assert picks[0]["signal_day_ret_pct"] is None
