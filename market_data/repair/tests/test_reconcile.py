"""Unit tests for the fresh-vs-held classifier.  No network, no real DB."""

from __future__ import annotations

import pytest

from market_data.repair import reconcile as R


def bar(start, o, h, l, c, v=1000, **kw):
    return {"bar_start": start, "open": o, "high": h, "low": l, "close": c,
            "volume": v, **kw}


PIIND_ACTIONS: list[dict] = []


# ---------------------------------------------------------------------------
# source_error -- the PIIND case
# ---------------------------------------------------------------------------

def test_piind_low_is_a_source_error():
    held = [bar("2019-09-05 11:15:00", 1192.15, 1192.95, 65.35, 1192.15)]
    fresh = [bar("2019-09-05 11:15:00", 1192.15, 1192.95, 1191.60, 1192.15,
                 source_request_id="req-piind")]
    daily = bar("2019-09-05 09:15:00", 1190.0, 1218.0, 1183.10, 1199.10)
    [v] = R.classify_session("PIIND", "2019-09-05", held, fresh, daily, PIIND_ACTIONS)
    assert v.klass == "source_error"
    assert "low" in v.fields_changed
    assert "1183" in v.reason
    assert v.evidence_request_id == "req-piind"


def test_close_only_mismatch_is_never_a_source_error():
    """NSE's daily close is a 15:00-15:30 VWAP, not the last trade.  RELIANCE
    2026-07-31: intraday last close 1305.00, daily close 1307.80, same high."""
    held = [bar("2026-07-31 15:15:00", 1304.0, 1306.0, 1303.0, 1305.00)]
    fresh = [bar("2026-07-31 15:15:00", 1304.0, 1306.0, 1303.0, 1305.00,
                 source_request_id="r1")]
    daily = bar("2026-07-31 09:15:00", 1300.0, 1310.0, 1298.0, 1307.80)
    [v] = R.classify_session("RELIANCE", "2026-07-31", held, fresh, daily, [])
    assert v.klass == "agree"


def test_daily_close_drift_alone_does_not_make_a_disagreement_a_source_error():
    """The bars differ, but only inside the daily range: not two-witness proof."""
    held = [bar("2026-07-31 15:15:00", 1304.0, 1306.0, 1303.0, 1305.00)]
    fresh = [bar("2026-07-31 15:15:00", 1304.0, 1306.0, 1303.0, 1301.00,
                 source_request_id="r1")]
    daily = bar("2026-07-31 09:15:00", 1300.0, 1310.0, 1298.0, 1307.80)
    [v] = R.classify_session("RELIANCE", "2026-07-31", held, fresh, daily, [])
    assert v.klass == "unresolved"


# ---------------------------------------------------------------------------
# adjustment_basis
# ---------------------------------------------------------------------------

def test_constant_level_shift_is_an_adjustment_basis_difference():
    held = [bar("2015-04-28 09:15:00", 15.2, 15.25, 15.10, 15.20),
            bar("2015-04-28 09:30:00", 15.2, 15.30, 15.15, 15.25)]
    fresh = [bar("2015-04-28 09:15:00", 3.8, 3.8125, 3.775, 3.80,
                 source_request_id="r-fb"),
             bar("2015-04-28 09:30:00", 3.8, 3.825, 3.7875, 3.8125,
                 source_request_id="r-fb")]
    daily = bar("2015-04-28 09:15:00", 60.7, 64.25, 60.55, 63.6)
    actions = [{"symbol": "FEDERALBNK", "ex_date": "2015-07-20",
                "action_type": "bonus", "subject": "Bonus 1:1"}]
    vs = R.classify_session("FEDERALBNK", "2015-04-28", held, fresh, daily, actions)
    assert {v.klass for v in vs} == {"adjustment_basis"}
    assert vs[0].ratio == pytest.approx(4.0)
    assert vs[0].corp_action["ex_date"] == "2015-07-20"


def test_a_dividend_is_not_offered_as_the_cause_of_a_4x_shift():
    actions = [{"symbol": "X", "ex_date": "2015-08-01", "action_type": "dividend",
                "subject": "Dividend - Rs 1"}]
    assert R.match_corp_action(actions, "2015-04-28", 4.0) is None
    # a sub-5% shift may legitimately be a dividend adjustment
    assert R.match_corp_action(actions, "2015-04-28", 1.01)["action_type"] == "dividend"


def test_a_spike_inside_a_shifted_session_is_not_scaled_away():
    """The session has a genuine 4x basis shift AND one corrupt low.  The low
    must not inherit the basis excuse -- but the daily comparison must be made
    on the vendor's basis, not across the shift."""
    held = [bar("2015-04-28 09:15:00", 15.2, 15.25, 15.10, 15.20),
            bar("2015-04-28 09:30:00", 15.2, 15.30, 0.85, 15.25)]
    fresh = [bar("2015-04-28 09:15:00", 3.8, 3.8125, 3.775, 3.80, source_request_id="r"),
             bar("2015-04-28 09:30:00", 3.8, 3.825, 3.7875, 3.8125, source_request_id="r")]
    daily = bar("2015-04-28 09:15:00", 3.79, 3.83, 3.77, 3.81)   # vendor basis
    vs = R.classify_session("X", "2015-04-28", held, fresh, daily, [])
    assert vs[0].klass == "adjustment_basis"
    # 0.85 / 4.0 = 0.2125, far below 0.8 x the vendor daily low of 3.77
    assert vs[1].klass == "source_error"
    assert "re-based by the session ratio" in vs[1].reason


def test_a_vendor_print_outside_its_own_daily_range_is_not_our_error():
    """INFY 2015-04-24: 2090.90 against a daily high of 526.20, in the legacy DB
    and in a fetch made today. Not a source_error -- a vendor defect."""
    held = [bar("2015-04-24 09:15:00", 523.9, 2090.9, 520.2, 2090.9)]
    fresh = [bar("2015-04-24 09:15:00", 2090.9, 2090.9, 2090.9, 2090.9,
                 source_request_id="r-infy")]
    daily = bar("2015-04-24 09:15:00", 522.7, 526.2, 484.5, 488.2)
    [v] = R.classify_session("INFY", "2015-04-24", held, fresh, daily, [])
    assert v.klass == "vendor_bad_print"
    assert "a re-fetch reproduces it" in v.reason


# ---------------------------------------------------------------------------
# genuine_extreme
# ---------------------------------------------------------------------------

def test_vendor_confirmed_extreme_is_not_an_error():
    held = [bar("2020-03-23 09:15:00", 100.0, 101.0, 40.0, 99.0)]
    fresh = [bar("2020-03-23 09:15:00", 100.0, 101.0, 40.0, 99.5,
                 source_request_id="r-ext")]
    daily = bar("2020-03-23 09:15:00", 100.0, 101.0, 40.0, 99.5)
    [v] = R.classify_session("X", "2020-03-23", held, fresh, daily, [])
    assert v.klass == "genuine_extreme"


# ---------------------------------------------------------------------------
# session_regime_cas  (NSE Closing Auction Session, from 2026-08-03)
# ---------------------------------------------------------------------------

def test_absent_1515_bar_after_cas_is_the_expected_regime_not_an_error():
    held = [bar("2026-08-04 15:00:00", 100.0, 101.0, 99.0, 100.5),
            bar("2026-08-04 15:15:00", 100.5, 101.5, 100.0, 101.0)]
    fresh = [bar("2026-08-04 15:00:00", 100.0, 101.0, 99.0, 100.5,
                 source_request_id="r-cas")]
    daily = bar("2026-08-04 09:15:00", 99.0, 101.5, 98.5, 101.0)
    vs = R.classify_session("X", "2026-08-04", held, fresh, daily, [], regime="cas")
    klasses = {v.bar_start: v.klass for v in vs}
    assert klasses["2026-08-04 15:00:00"] == "agree"
    assert klasses["2026-08-04 15:15:00"] == "session_regime_cas"
    cas = next(v for v in vs if v.klass == "session_regime_cas")
    assert "Closing Auction Session" in cas.reason
    assert "nothing is synthesised" in cas.reason
    assert cas.fields_changed == []               # nothing is corrected


def test_the_same_shape_before_the_cas_start_date_is_not_excused():
    held = [bar("2026-07-31 15:00:00", 100.0, 101.0, 99.0, 100.5),
            bar("2026-07-31 15:15:00", 100.5, 101.5, 100.0, 101.0)]
    fresh = [bar("2026-07-31 15:00:00", 100.0, 101.0, 99.0, 100.5,
                 source_request_id="r")]
    vs = R.classify_session("X", "2026-07-31", held, fresh, None, [], regime="cas")
    assert next(v.klass for v in vs if v.bar_start.endswith("15:15:00")) == "unresolved"


def test_a_symbol_that_is_not_on_the_cas_regime_gets_no_excuse():
    held = [bar("2026-08-04 15:00:00", 100.0, 101.0, 99.0, 100.5),
            bar("2026-08-04 15:15:00", 100.5, 101.5, 100.0, 101.0)]
    fresh = [bar("2026-08-04 15:00:00", 100.0, 101.0, 99.0, 100.5,
                 source_request_id="r")]
    vs = R.classify_session("X", "2026-08-04", held, fresh, None, [], regime="regular")
    assert next(v.klass for v in vs if v.bar_start.endswith("15:15:00")) == "unresolved"


def test_regime_is_derived_from_the_bars_by_w2s_regimebook():
    """Two 24-bar sessions after 2026-08-03 and none before => CAS."""
    fresh_by_day = {
        "2026-07-31": [bar(f"2026-07-31 15:15:00", 1, 1, 1, 1)],
        "2026-08-04": [bar(f"2026-08-04 15:00:00", 1, 1, 1, 1)],
        "2026-09-15": [bar(f"2026-09-15 15:00:00", 1, 1, 1, 1)],
    }
    regime_of = R.regime_for("RELIANCE", fresh_by_day, is_fno_flag=1)
    assert regime_of("2026-08-04") == "cas"
    assert regime_of("2026-09-15") == "cas"
    assert regime_of("2026-07-31") == "regular"    # before the first CAS session


def test_regime_stays_regular_for_a_symbol_whose_sessions_are_all_full():
    fresh_by_day = {d: [bar(f"{d} 15:15:00", 1, 1, 1, 1)]
                    for d in ("2026-08-04", "2026-08-05", "2026-09-15")}
    regime_of = R.regime_for("AARTIIND", fresh_by_day, is_fno_flag=0)
    assert regime_of("2026-09-15") == "regular"


def test_a_missing_midsession_bar_is_unresolved_not_cas():
    held = [bar("2026-08-04 11:00:00", 100.0, 101.0, 99.0, 100.5),
            bar("2026-08-04 15:00:00", 100.5, 101.5, 100.0, 101.0)]
    fresh = [bar("2026-08-04 15:00:00", 100.5, 101.5, 100.0, 101.0,
                 source_request_id="r")]
    vs = R.classify_session("X", "2026-08-04", held, fresh, None, [])
    assert next(v.klass for v in vs if v.bar_start.endswith("11:00:00")) == "unresolved"


def test_regime_summary_separates_cas_from_unexplained_short_sessions():
    cas = {
        "RELIANCE": [{"session": "2026-08-04", "regime": "cas", "is_fno": 1,
                      "bars_returned": 24, "last_bar_start": "15:00:00"},
                     {"session": "2026-09-15", "regime": "cas", "is_fno": 1,
                      "bars_returned": 24, "last_bar_start": "15:00:00"}],
        "SOMETHING": [{"session": "2021-03-01", "regime": "short_session_other",
                       "is_fno": 0, "bars_returned": 12,
                       "last_bar_start": "12:00:00"}],
    }
    out = R._regime_summary(cas, fresh_sessions_total=1000)
    assert out["cas_symbols"] == 1
    assert out["cas_symbol_sessions"] == 2
    assert out["cas_distinct_sessions"] == 2
    assert out["cas_symbol_sessions_with_is_fno_1"] == 2
    assert out["unexplained_short_symbol_sessions"] == 1
    assert out["fresh_sessions_examined"] == 1000


# ---------------------------------------------------------------------------
# corrections
# ---------------------------------------------------------------------------

class _RecordingStore:
    def __init__(self):
        self.rows = []

    def record_correction(self, **kw):
        self.rows.append(kw)
        return len(self.rows)


def test_corrections_carry_reason_and_evidence_and_never_touch_agree():
    held = [bar("2019-09-05 11:15:00", 1192.15, 1192.95, 65.35, 1192.15)]
    fresh = [bar("2019-09-05 11:15:00", 1192.15, 1192.95, 1191.60, 1192.15,
                 source_request_id="req-piind", revision=2)]
    daily = bar("2019-09-05 09:15:00", 1190.0, 1218.0, 1183.10, 1199.10)
    vs = R.classify_session("PIIND", "2019-09-05", held, fresh, daily, [])
    store = _RecordingStore()
    assert R.write_corrections(store, vs, "run-1") == 1
    row = store.rows[0]
    assert row["field"] == "low"
    assert row["old_value"] == 65.35 and row["new_value"] == 1191.60
    assert row["evidence_request_id"] == "req-piind"
    assert row["reason"].startswith("source_error:")
    assert row["new_revision"] == 2


def test_unresolved_is_never_corrected():
    held = [bar("2026-07-31 15:15:00", 1304.0, 1306.0, 1303.0, 1305.0)]
    fresh = [bar("2026-07-31 15:15:00", 1304.0, 1306.0, 1303.0, 1301.0,
                 source_request_id="r1")]
    daily = bar("2026-07-31 09:15:00", 1300.0, 1310.0, 1298.0, 1307.8)
    vs = R.classify_session("X", "2026-07-31", held, fresh, daily, [])
    store = _RecordingStore()
    assert R.write_corrections(store, vs, "run-1") == 0


def test_close_enough_tolerances():
    assert R.close_enough(100.00, 100.005)
    assert R.close_enough(1000.0, 1000.4)          # 0.04% rel
    assert not R.close_enough(100.0, 101.0)
    assert not R.close_enough(None, 100.0)


def test_adjustment_basis_is_recorded_once_per_segment_not_per_bar():
    """A corporate action can re-level a decade. One segment row, not four rows
    per 15-minute bar, so real price corrections stay findable."""
    vs = [R.BarVerdict("X", f"2015-04-{d:02d} 09:15:00", "adjustment_basis",
                       "constant 4x level shift", {"open": 15.2}, {"open": 3.8,
                       "revision": 2}, None, 4.0, None, "r-seg",
                       ["open", "high", "low", "close"])
          for d in range(1, 21)]
    store = _RecordingStore()
    assert R.write_corrections(store, vs, "run-1") == 1
    row = store.rows[0]
    assert row["field"] == "adjustment_basis_id"
    assert "20 bars" in row["reason"]
    assert "2015-04-01 .. 2015-04-20" in row["reason"]
    assert row["evidence_request_id"] == "r-seg"


def test_a_new_ratio_starts_a_new_basis_segment_row():
    vs = ([R.BarVerdict("X", f"2015-04-{d:02d} 09:15:00", "adjustment_basis", "r",
                        {}, {"revision": 2}, None, 4.0, None, "r1", ["open"])
           for d in range(1, 6)]
          + [R.BarVerdict("X", f"2015-05-{d:02d} 09:15:00", "adjustment_basis", "r",
                          {}, {"revision": 2}, None, 2.0, None, "r1", ["open"])
             for d in range(1, 6)])
    store = _RecordingStore()
    assert R.write_corrections(store, vs, "run-1") == 2


def test_source_errors_are_still_recorded_per_field_around_a_basis_run():
    vs = ([R.BarVerdict("X", "2015-04-01 09:15:00", "adjustment_basis", "r",
                        {}, {"revision": 2}, None, 4.0, None, "r1", ["open"])]
          + [R.BarVerdict("X", "2015-04-02 09:15:00", "source_error", "bad low",
                          {"low": 1.0}, {"low": 100.0, "revision": 2}, None, None,
                          None, "r2", ["low"])]
          + [R.BarVerdict("X", "2015-04-03 09:15:00", "adjustment_basis", "r",
                          {}, {"revision": 2}, None, 4.0, None, "r1", ["open"])])
    store = _RecordingStore()
    assert R.write_corrections(store, vs, "run-1") == 3
    assert [r["field"] for r in store.rows] == [
        "adjustment_basis_id", "low", "adjustment_basis_id"]
