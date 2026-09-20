"""The seven derivatives signals of DERIVATIVES_SPEC.md section 3, pinned.

Every fixture here is hand-built and the expected numbers are arithmetic a
reader can redo on paper -- that is the point.  No network, no vendor, no
`db/kanida.db`, no `db/market15.db`.  The database tests build a throwaway
`derivatives.db` in tmp_path with the tables of spec section 2, because worker
D1's `store.py` / `schema.sql` are being written in parallel; when they land,
these tests still pass unchanged as long as the column names match the spec.

Coverage, one block per definition:
  3.1  the four build-up labels, both windows, and the "never mixed" rule
  3.2  volume vs its own time-of-day median, and the "no baseline" path
  3.3  volume-to-OI spike above 1.0, raw numbers carried
  3.4  premium traded in Rs crore, per contract and rolled up
  3.5  OI PCR and volume PCR per expiry, plus the day's trend
  3.6  max pain on a three-strike book verified by hand
  3.7  futures build-up, OI vs its own average, basis
  plus the liquidity floors, days-to-expiry, the roll-up, and the read shapes.
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_data.derivatives import metrics as M  # noqa: E402

EXPIRY = date(2026, 9, 24)
MARK = datetime(2026, 9, 18, 11, 15, 0)


def contract(token=1, typ="CE", strike=2500.0, lot=250, symbol=None, underlying="RELIANCE"):
    return M.ContractRef(
        instrument_token=token,
        tradingsymbol=symbol or f"{underlying}26SEP{int(strike)}{typ}",
        underlying=underlying,
        instrument_type=typ,
        expiry=EXPIRY,
        strike=strike,
        lot_size=lot,
    )


def snap(token=1, at=MARK, price=100.0, avg=99.0, volume=1_000_000, oi=500_000):
    return M.Snapshot(
        instrument_token=token,
        captured_at=at,
        last_price=price,
        average_price=avg,
        volume=volume,
        oi=oi,
    )


# ===========================================================================
# 3.1  Open-interest build-up
# ===========================================================================


class TestBuildUp:
    @pytest.mark.parametrize(
        "price_change, oi_change, expected",
        [
            (+5.0, +1000, M.BUILDUP_LONG),          # price up,   OI up
            (-5.0, +1000, M.BUILDUP_SHORT),         # price down, OI up
            (+5.0, -1000, M.BUILDUP_SHORT_COVER),   # price up,   OI down
            (-5.0, -1000, M.BUILDUP_LONG_UNWIND),   # price down, OI down
        ],
    )
    def test_the_four_labels(self, price_change, oi_change, expected):
        assert M.classify_buildup(price_change, oi_change) == expected

    def test_zero_change_is_not_one_of_the_four(self):
        assert M.classify_buildup(0.0, 1000) == M.BUILDUP_FLAT
        assert M.classify_buildup(5.0, 0) == M.BUILDUP_FLAT
        assert M.BUILDUP_FLAT not in M.BUILDUP_LABELS

    def test_missing_input_is_no_data_not_a_label(self):
        assert M.classify_buildup(None, 1000) == M.BUILDUP_NO_DATA
        assert M.classify_buildup(5.0, None) == M.BUILDUP_NO_DATA

    def test_15m_and_day_are_computed_separately_and_can_disagree(self):
        """The whole point of "never mix them in one label"."""
        cur = snap(price=105.0, oi=520_000)
        prev = snap(at=MARK - timedelta(minutes=15), price=100.0, oi=500_000)
        prev_close = M.DailyBar(session=date(2026, 9, 17), close=110.0, oi=560_000)

        b15 = M.buildup_15m(cur, prev)
        bday = M.buildup_day(cur, prev_close)

        assert b15.window == M.WINDOW_15M
        assert b15.label == M.BUILDUP_LONG          # +5 price, +20k OI
        assert b15.price_change == 5.0 and b15.oi_change == 20_000
        assert bday.window == M.WINDOW_DAY
        assert bday.label == M.BUILDUP_LONG_UNWIND  # -5 price, -40k OI
        assert bday.price_change == -5.0 and bday.oi_change == -40_000
        assert b15.label != bday.label

    def test_percentages(self):
        b = M.build_up(window="day", price_now=110.0, price_before=100.0,
                       oi_now=110_000, oi_before=100_000)
        assert b.price_change_pct == pytest.approx(10.0)
        assert b.oi_change_pct == pytest.approx(10.0)

    def test_no_previous_snapshot_is_no_data(self):
        assert M.buildup_15m(snap(), None).status == M.STATUS_NO_DATA
        assert M.buildup_day(snap(), None).status == M.STATUS_NO_PRIOR


# ===========================================================================
# 3.2  Volume versus its own time-of-day average
# ===========================================================================


class TestVolumeVsTimeOfDay:
    def test_ratio_is_today_over_the_median_of_the_same_clock_time(self):
        # median of [100, 200, 300, 400, 500] = 300; today 900 => 3.0x
        r = M.volume_vs_time_of_day(900, [100, 200, 300, 400, 500])
        assert r.status == M.STATUS_OK
        assert r.median_cumulative == 300
        assert r.ratio == pytest.approx(3.0)
        assert r.sessions_used == 5

    def test_even_number_of_sessions_uses_the_median_not_the_mean(self):
        # [10, 20, 30, 1000]: median 25, mean 265. A mean would hide the spike.
        r = M.volume_vs_time_of_day(50, [10, 20, 30, 1000])
        assert r.median_cumulative == 25
        assert r.ratio == pytest.approx(2.0)

    def test_fewer_than_three_sessions_is_no_baseline_never_a_number(self):
        for history in ([], [100], [100, 200]):
            r = M.volume_vs_time_of_day(900, history)
            assert r.status == M.STATUS_NO_BASELINE
            assert r.ratio is None
            assert r.sessions_used == len(history)

    def test_three_sessions_is_exactly_enough(self):
        r = M.volume_vs_time_of_day(900, [100, 300, 500])
        assert r.status == M.STATUS_OK and r.ratio == pytest.approx(3.0)

    def test_nones_in_history_do_not_count_as_sessions(self):
        r = M.volume_vs_time_of_day(900, [100, None, None])
        assert r.status == M.STATUS_NO_BASELINE and r.sessions_used == 1

    def test_zero_median_is_no_baseline_not_a_division(self):
        r = M.volume_vs_time_of_day(900, [0, 0, 0])
        assert r.status == M.STATUS_NO_BASELINE and r.ratio is None

    def test_cumulative_by_time_of_day_is_point_in_time(self):
        """Bars after the clock time are excluded, and today never feeds its own
        baseline."""
        bars = []
        for day in (16, 17, 18):                       # 18th is 'today'
            for hour, vol in ((9, 100), (11, 200), (14, 900)):
                bars.append({"bar_start": datetime(2026, 9, day, hour, 15), "volume": vol})
        out = M.cumulative_by_time_of_day(bars, cutoff=datetime(2026, 9, 18, 11, 15))
        assert out == [300.0, 300.0]       # 16th and 17th, 09:15 + 11:15 only


# ===========================================================================
# 3.3  Volume-to-OI spike
# ===========================================================================


class TestVolumeToOi:
    def test_above_one_is_flagged_and_raw_numbers_travel_with_it(self):
        r = M.volume_to_oi(1_500_000, 1_000_000)
        assert r.status == M.STATUS_OK
        assert r.ratio == pytest.approx(1.5)
        assert r.is_spike is True
        assert r.day_volume == 1_500_000 and r.prev_day_oi == 1_000_000

    def test_exactly_one_is_not_above_one(self):
        assert M.volume_to_oi(1_000, 1_000).is_spike is False

    def test_below_one_is_not_a_spike(self):
        r = M.volume_to_oi(400, 1_000)
        assert r.ratio == pytest.approx(0.4) and r.is_spike is False

    def test_no_previous_session_is_a_status_not_a_ratio(self):
        r = M.volume_to_oi(1_000, None)
        assert r.status == M.STATUS_NO_PRIOR and r.ratio is None

    def test_zero_previous_oi_is_no_oi_not_infinity(self):
        r = M.volume_to_oi(1_000, 0)
        assert r.status == M.STATUS_NO_OI and r.ratio is None


# ===========================================================================
# 3.4  Premium traded
# ===========================================================================


class TestPremiumTraded:
    def test_volume_times_average_price_in_rupees_and_crore(self):
        # NFO volume is in units already: 1,200,000 x Rs 40 = Rs 4.8 crore
        p = M.premium_traded(1_200_000, 40.0)
        assert p.status == M.STATUS_OK
        assert p.rupees == pytest.approx(4_80_00_000.0)
        assert p.crore == pytest.approx(4.8)

    def test_missing_leg_is_no_data_not_zero(self):
        assert M.premium_traded(None, 40.0).status == M.STATUS_NO_DATA
        assert M.premium_traded(1_000, None).rupees is None

    def test_rollup_sums_only_real_numbers(self):
        total = M.premium_rollup([
            M.premium_traded(1_000_000, 10.0),   # Rs 1 crore
            M.premium_traded(2_000_000, 20.0),   # Rs 4 crore
            M.premium_traded(None, 20.0),        # no data -- skipped, not zeroed
        ])
        assert total.crore == pytest.approx(5.0)

    def test_rollup_of_nothing_real_is_no_data(self):
        assert M.premium_rollup([M.premium_traded(None, None)]).status == M.STATUS_NO_DATA


# ===========================================================================
# 3.5  Put-call ratio
# ===========================================================================


def pcr_legs():
    return [
        {"instrument_type": "CE", "strike": 100.0, "oi": 1000, "volume": 4000},
        {"instrument_type": "CE", "strike": 110.0, "oi": 3000, "volume": 6000},
        {"instrument_type": "PE", "strike": 100.0, "oi": 2000, "volume": 3000},
        {"instrument_type": "PE", "strike": 110.0, "oi": 4000, "volume": 2000},
    ]


class TestPutCallRatio:
    def test_oi_and_volume_pcr(self):
        r = M.put_call_ratio(pcr_legs())
        assert r.ce_oi == 4000 and r.pe_oi == 6000
        assert r.pcr_oi == pytest.approx(1.5)          # 6000 / 4000
        assert r.ce_volume == 10000 and r.pe_volume == 5000
        assert r.pcr_volume == pytest.approx(0.5)      # 5000 / 10000
        assert r.strikes == 2

    def test_futures_legs_are_ignored(self):
        legs = pcr_legs() + [{"instrument_type": "FUT", "oi": 999999, "volume": 999999}]
        assert M.put_call_ratio(legs).pcr_oi == pytest.approx(1.5)

    def test_zero_call_side_is_none_not_infinity(self):
        r = M.put_call_ratio([{"instrument_type": "PE", "strike": 100.0, "oi": 10, "volume": 5}])
        assert r.pcr_oi is None and r.pcr_volume is None
        assert r.pe_oi == 10

    def test_no_option_legs_at_all_is_no_data(self):
        assert M.put_call_ratio([]).status == M.STATUS_NO_DATA

    def test_day_trend_needs_two_readings(self):
        assert M.pcr_trend([1.2]) == (M.STATUS_NO_BASELINE, None)
        label, change = M.pcr_trend([1.0, 1.1, 1.3])
        assert label == "rising" and change == pytest.approx(0.3)
        label, change = M.pcr_trend([1.3, 1.0])
        assert label == "falling" and change == pytest.approx(-0.3)
        assert M.pcr_trend([1.0, 1.0])[0] == "flat"


# ===========================================================================
# 3.6  Max pain
# ===========================================================================


class TestMaxPain:
    """Hand-verifiable book.

        strike | CE OI | PE OI
          100  |  100  |  200
          110  |  100  |  100
          120  |  200  |  100

    Payout to buyers at settlement S = sum over strikes of
        CE OI x max(0, S - K)  +  PE OI x max(0, K - S)

      S = 100 -> calls 0                          + puts 100x10 + 100x20 = 3000
      S = 110 -> calls 100x10 = 1000              + puts 100x10          = 2000
      S = 120 -> calls 100x20 + 100x10 = 3000     + puts 0               = 3000

    So max pain = 110, payout 2000, total OI 800.
    """

    BOOK = {
        100.0: {"ce_oi": 100, "pe_oi": 200},
        110.0: {"ce_oi": 100, "pe_oi": 100},
        120.0: {"ce_oi": 200, "pe_oi": 100},
    }

    def test_strike_payout_and_total_oi(self):
        mp = M.max_pain(self.BOOK, spot=105.0)
        assert mp.status == M.STATUS_OK
        assert mp.strike == 110.0
        assert mp.total_payout == pytest.approx(2000.0)
        assert mp.total_oi == 800
        assert mp.strikes_used == 3
        assert mp.payout_by_strike[100.0] == pytest.approx(3000.0)
        assert mp.payout_by_strike[120.0] == pytest.approx(3000.0)

    def test_distance_from_spot(self):
        mp = M.max_pain(self.BOOK, spot=105.0)
        assert mp.distance_from_spot == pytest.approx(5.0)
        assert mp.distance_pct == pytest.approx(100.0 * 5.0 / 105.0)

    def test_no_spot_means_no_distance_not_a_zero(self):
        mp = M.max_pain(self.BOOK)
        assert mp.strike == 110.0
        assert mp.distance_from_spot is None and mp.distance_pct is None

    def test_accepts_leg_rows_too(self):
        legs = []
        for k, v in self.BOOK.items():
            legs.append({"strike": k, "instrument_type": "CE", "oi": v["ce_oi"]})
            legs.append({"strike": k, "instrument_type": "PE", "oi": v["pe_oi"]})
        assert M.max_pain(legs, spot=105.0).strike == 110.0

    def test_empty_and_zero_oi_books_are_statuses(self):
        assert M.max_pain({}).status == M.STATUS_NO_DATA
        assert M.max_pain({100.0: {"ce_oi": 0, "pe_oi": 0}}).status == M.STATUS_NO_OI


# ===========================================================================
# 3.7  Futures OI build-up
# ===========================================================================


class TestFuturesBuildUp:
    def fut(self):
        return M.ContractRef(
            instrument_token=99, tradingsymbol="RELIANCE26SEPFUT", underlying="RELIANCE",
            instrument_type="FUT", expiry=EXPIRY, strike=None, lot_size=250,
        )

    def test_labels_oi_share_and_basis(self):
        cur = M.Snapshot(99, MARK, last_price=2520.0, average_price=2515.0,
                         volume=4_000_000, oi=12_000_000)
        prev = M.Snapshot(99, MARK - timedelta(minutes=15), last_price=2500.0, oi=11_000_000)
        prev_close = M.DailyBar(session=date(2026, 9, 17), close=2530.0, oi=11_500_000)
        history = [10_000_000] * 20          # its own 20-day average

        fb = M.futures_buildup(
            self.fut(), cur, previous=prev, previous_close=prev_close,
            oi_history=history, spot=2500.0, as_of=MARK,
        )
        assert fb.buildup_15m.label == M.BUILDUP_LONG          # +20 price, +1m OI
        assert fb.buildup_day.label == M.BUILDUP_SHORT         # -10 price, +0.5m OI
        assert fb.oi_avg == pytest.approx(10_000_000)
        assert fb.oi_vs_avg == pytest.approx(1.2)              # 12m / 10m
        assert fb.oi_vs_avg_status == M.STATUS_OK
        assert fb.basis == pytest.approx(20.0)                 # 2520 - 2500
        assert fb.basis_pct == pytest.approx(0.8)
        assert fb.days_to_expiry == 6

    def test_short_history_is_no_baseline(self):
        share, avg, used, status = M.oi_vs_average(12_000_000, [10_000_000, 11_000_000])
        assert status == M.STATUS_NO_BASELINE and share is None and used == 2

    def test_only_the_last_20_sessions_count(self):
        share, avg, used, status = M.oi_vs_average(100, [1] * 30 + [10] * 20)
        assert used == 20 and avg == pytest.approx(10.0) and share == pytest.approx(10.0)

    def test_no_spot_means_no_basis(self):
        b, pct, status = M.futures_basis(2520.0, None)
        assert status == M.STATUS_NO_SPOT and b is None and pct is None

    def test_no_futures_price_means_no_basis(self):
        assert M.futures_basis(None, 2500.0)[2] == M.STATUS_NO_DATA


# ===========================================================================
# Liquidity floors and days-to-expiry
# ===========================================================================


class TestFloors:
    def test_the_three_floors_are_the_spec_numbers(self):
        f = M.LiquidityFloors()
        assert f.min_premium_rs == 2_00_00_000.0      # Rs 2 crore
        assert f.min_last_price == 1.0
        assert f.min_oi_lots == 1.0
        assert f.as_dict()["min_premium_cr"] == pytest.approx(2.0)
        assert "crore" in f.as_dict()["description"]

    def test_a_contract_that_clears_everything_passes(self):
        ok, failed = M.DEFAULT_FLOORS.check(
            premium_rs=5_00_00_000.0, oi=500_000, lot_size=250, last_price=40.0)
        assert ok is True and failed == []

    @pytest.mark.parametrize(
        "kwargs, expected",
        [
            (dict(premium_rs=1_00_00_000.0, oi=500_000, lot_size=250, last_price=40.0), "premium"),
            (dict(premium_rs=5_00_00_000.0, oi=100, lot_size=250, last_price=40.0), "oi"),
            (dict(premium_rs=5_00_00_000.0, oi=500_000, lot_size=250, last_price=0.5), "price"),
        ],
    )
    def test_each_floor_rejects_by_name(self, kwargs, expected):
        ok, failed = M.DEFAULT_FLOORS.check(**kwargs)
        assert ok is False and failed == [expected]

    def test_missing_inputs_fail_the_floor_they_cannot_prove(self):
        ok, failed = M.DEFAULT_FLOORS.check(
            premium_rs=None, oi=None, lot_size=None, last_price=None)
        assert ok is False
        assert failed == ["premium:unknown", "oi:unknown", "price:unknown"]


def test_days_to_expiry_on_every_row():
    assert M.days_to_expiry(date(2026, 9, 24), datetime(2026, 9, 18, 11, 15)) == 6
    assert M.days_to_expiry(date(2026, 9, 18), date(2026, 9, 18)) == 0      # expiry day
    assert M.days_to_expiry(date(2026, 9, 17), date(2026, 9, 18)) == -1     # already gone
    assert M.days_to_expiry(None, date(2026, 9, 18)) is None
    assert M.days_to_expiry("2026-09-24", date(2026, 9, 18)) == 6


# ===========================================================================
# Per-contract assembly and the per-underlying roll-up
# ===========================================================================


def build_metrics(**over):
    """A liquid 2500 CE with a real baseline, unless overridden."""
    cfg = dict(
        token=1, typ="CE", strike=2500.0,
        price=40.0, avg=40.0, volume=1_200_000, oi=500_000,
        prev_price=35.0, prev_oi=480_000,
        close=30.0, close_oi=450_000, close_volume=400_000,
        baseline=[300_000, 400_000, 500_000, 600_000],
        spot=2505.0,
    )
    cfg.update(over)
    c = contract(token=cfg["token"], typ=cfg["typ"], strike=cfg["strike"])
    return M.compute_contract_metrics(M.ContractInputs(
        contract=c,
        current=snap(cfg["token"], MARK, cfg["price"], cfg["avg"], cfg["volume"], cfg["oi"]),
        previous=snap(cfg["token"], MARK - timedelta(minutes=15),
                      cfg["prev_price"], cfg["avg"], 0, cfg["prev_oi"]),
        previous_close=M.DailyBar(date(2026, 9, 17), cfg["close"], cfg["close_oi"],
                                  cfg["close_volume"]),
        tod_baseline=cfg["baseline"],
        spot=cfg["spot"],
    ))


class TestContractMetrics:
    def test_every_signal_is_present_on_one_row(self):
        m = build_metrics()
        assert m.days_to_expiry == 6
        assert m.buildup_15m.label == M.BUILDUP_LONG
        assert m.buildup_day.label == M.BUILDUP_LONG
        # premium 1.2m x Rs 40 = Rs 4.8 crore
        assert m.premium.crore == pytest.approx(4.8)
        # volume 1.2m vs median(300k,400k,500k,600k) = 450k -> 2.67x
        assert m.volume_vs_tod.ratio == pytest.approx(1_200_000 / 450_000)
        # day volume 1.2m vs yesterday's closing OI 450k -> 2.67x, a spike
        assert m.volume_to_oi.is_spike is True
        assert m.floors_passed is True
        assert m.unusual is True
        assert len(m.unusual_reasons) == 2

    def test_row_keeps_the_two_windows_in_separate_columns(self):
        row = build_metrics().to_row()
        assert row["buildup_15m"] and row["buildup_day"]
        assert "buildup" not in row          # there is no merged label column
        assert row["scope"] == "contract"
        assert row["days_to_expiry"] == 6

    def test_a_row_below_the_floors_is_never_unusual(self):
        m = build_metrics(price=0.5, avg=0.5, volume=100)     # 50 rupees traded
        assert m.floors_passed is False
        assert m.unusual is False
        assert "premium" in m.floors_failed and "price" in m.floors_failed

    def test_no_baseline_contract_reports_the_status_on_the_row(self):
        m = build_metrics(baseline=[100_000])
        assert m.volume_vs_tod.status == M.STATUS_NO_BASELINE
        assert m.to_row()["vol_tod_ratio"] is None
        assert m.to_row()["vol_tod_status"] == "no baseline"

    def test_a_quiet_contract_that_clears_the_floors_is_still_not_unusual(self):
        m = build_metrics(volume=500_000, avg=100.0, baseline=[400_000, 500_000, 600_000],
                          close_oi=5_000_000)
        assert m.floors_passed is True
        assert m.volume_vs_tod.ratio == pytest.approx(1.0)
        assert m.unusual is False and m.unusual_reasons == []

class TestUnusualRules:
    """A TRIGGER IS A RULE, NOT A SENTENCE.

    The store writes "volume 206.0x its own time-of-day median", and that sentence differs at every multiple.
    Counting sentences is what let one underlying's 80 flagged contracts read as "124 conditions" downstream.
    Everything here is additive: the stored text is byte-for-byte what it always was.
    """

    def test_a_flagged_contract_carries_its_rules_and_their_numbers(self):
        m = build_metrics()
        assert [t.rule_id for t in m.triggers] == [M.RULE_VOL_TOD, M.RULE_DAY_VOL_VS_PREV_OI]
        tod, voi = m.triggers
        assert tod.comparator == ">=" and tod.threshold == M.UNUSUAL_VOL_TOD_RATIO
        assert tod.value == pytest.approx(1_200_000 / 450_000)
        assert tod.baseline == pytest.approx(450_000)     # the median it was measured against
        assert tod.sample_count == 4                      # over four sessions, not four contracts
        assert voi.comparator == ">" and voi.threshold == M.VOL_OI_SPIKE_RATIO
        assert voi.baseline == pytest.approx(450_000)     # yesterday's closing OI
        assert voi.sample_count == 1                      # one prior session IS the baseline
        assert all(t.rule_version == M.UNUSUAL_RULES_VERSION for t in m.triggers)

    def test_the_stored_sentence_is_derived_from_the_trigger_and_is_unchanged(self):
        m = build_metrics()
        assert m.unusual_reasons == [t.reason for t in m.triggers]
        assert m.to_row()["unusual_reasons"] == (
            "volume 2.7x its own time-of-day median,day volume 2.7x yesterday's OI"
        )
        # NO COLUMN IS ADDED. A store written before this registry existed keeps every column it had.
        assert "unusual_triggers" not in m.to_row()
        assert "rule_id" not in m.to_row()

    def test_a_quiet_contract_has_no_triggers_at_all(self):
        m = build_metrics(volume=500_000, avg=100.0, baseline=[400_000, 500_000, 600_000],
                          close_oi=5_000_000)
        assert m.triggers == () and m.unusual_reasons == []

    @pytest.mark.parametrize("text,rule_id,value", [
        ("volume 206.0x its own time-of-day median", M.RULE_VOL_TOD, 206.0),
        ("volume 781.9x its own time-of-day median", M.RULE_VOL_TOD, 781.9),
        ("day volume 28.3x yesterday's OI", M.RULE_DAY_VOL_VS_PREV_OI, 28.3),
        ("day volume 1.2x yesterday's OI", M.RULE_DAY_VOL_VS_PREV_OI, 1.2),
    ])
    def test_every_multiple_of_one_rule_reads_back_to_that_one_rule(self, text, rule_id, value):
        assert M.classify_reason(text) == (rule_id, value)

    def test_a_sentence_no_rule_claims_stays_itself(self):
        assert M.classify_reason("something this build never wrote") == (M.RULE_UNCLASSIFIED, None)
        assert M.classify_reason("") == (M.RULE_UNCLASSIFIED, None)

    def test_a_stored_row_is_read_back_into_triggers_without_recomputing_anything(self):
        row = {
            "unusual_reasons": "volume 206.0x its own time-of-day median,day volume 28.3x yesterday's OI",
            "vol_tod_ratio": 206.04, "vol_tod_median": 1200.0, "vol_tod_sessions": 7,
            "vol_oi_ratio": 28.31, "vol_oi_prev_oi": 999.0,
        }
        tod, voi = M.triggers_from_metric_row(row)
        # the COLUMN's exact number, not the rounded one the sentence prints
        assert tod.value == 206.04 and tod.baseline == 1200.0 and tod.sample_count == 7
        assert voi.value == 28.31 and voi.baseline == 999.0 and voi.sample_count == 1
        # a row whose columns are gone keeps the number its own sentence states, and invents nothing else
        thin = M.triggers_from_metric_row({"unusual_reasons": "volume 206.0x its own time-of-day median"})
        assert thin[0].value == 206.0 and thin[0].baseline is None and thin[0].sample_count is None
        assert M.triggers_from_metric_row({"unusual_reasons": None}) == []

    def test_the_registry_is_closed_and_every_rule_states_its_comparison(self):
        assert [r.rule_id for r in M.UNUSUAL_RULES] == [M.RULE_VOL_TOD, M.RULE_DAY_VOL_VS_PREV_OI]
        assert len(M.UNUSUAL_RULES) == 2, "there is no third rule, so no name can trip three"
        for rule in M.UNUSUAL_RULES:
            assert rule.comparator in (">=", ">") and rule.threshold > 0
            assert rule.label and rule.measure and rule.baseline_label and rule.sample_label
            assert rule.version == M.UNUSUAL_RULES_VERSION


class TestRollup:
    def test_headline_pcr_max_pain_and_premium_agree_with_the_strikes(self):
        legs = [
            build_metrics(token=1, typ="CE", strike=2500.0, oi=100_000, close_oi=80_000,
                          volume=1_200_000, avg=40.0),
            build_metrics(token=2, typ="CE", strike=2600.0, oi=100_000, close_oi=100_000,
                          volume=200_000, avg=50.0, baseline=[300_000, 400_000, 500_000]),
            build_metrics(token=3, typ="PE", strike=2500.0, oi=200_000, close_oi=150_000,
                          volume=300_000, avg=45.0, baseline=[300_000, 400_000, 500_000]),
            build_metrics(token=4, typ="PE", strike=2600.0, oi=100_000, close_oi=90_000,
                          volume=100_000, avg=45.0, baseline=[300_000, 400_000, 500_000]),
        ]
        roll = M.roll_up_underlying("RELIANCE", legs, captured_at=MARK, expiry=EXPIRY,
                                    spot=2505.0)
        assert roll.pcr.pcr_oi == pytest.approx(300_000 / 200_000)
        assert roll.max_pain.status == M.STATUS_OK
        assert roll.premium.crore == pytest.approx(
            (1_200_000 * 40 + 200_000 * 50 + 300_000 * 45 + 100_000 * 45) / 1_00_00_000
        )
        # OI 500k now vs 420k at yesterday's close => +19%
        assert roll.oi_change_pct_day == pytest.approx(100.0 * 80 / 420)
        assert roll.unusual_ce >= 1
        assert roll.headline.startswith("RELIANCE - ")
        assert "cr traded" in roll.headline
        assert roll.days_to_expiry == 6

    def test_trend_uses_earlier_readings_of_the_same_day(self):
        legs = [build_metrics(token=1, typ="CE", oi=100),
                build_metrics(token=3, typ="PE", oi=200)]
        roll = M.roll_up_underlying("RELIANCE", legs, captured_at=MARK, expiry=EXPIRY,
                                    pcr_series=[1.0, 1.5])
        assert roll.pcr.pcr_oi == pytest.approx(2.0)
        assert roll.pcr.trend == "rising"

    def test_no_earlier_reading_means_no_baseline_trend(self):
        legs = [build_metrics(token=1, typ="CE", oi=100),
                build_metrics(token=3, typ="PE", oi=200)]
        roll = M.roll_up_underlying("RELIANCE", legs, captured_at=MARK, expiry=EXPIRY)
        assert roll.pcr.trend == M.STATUS_NO_BASELINE

    def test_headline_says_so_when_nothing_clears_the_floors(self):
        legs = [build_metrics(token=1, typ="CE", price=0.5, avg=0.5, volume=100)]
        roll = M.roll_up_underlying("RELIANCE", legs, captured_at=MARK, expiry=EXPIRY)
        assert "nothing above the floors" in roll.headline


# ===========================================================================
# The thin database layer: spec-section-2 tables in tmp_path, no vendor
# ===========================================================================

SPEC_TABLES = """
CREATE TABLE contracts (
    instrument_token INTEGER PRIMARY KEY, tradingsymbol TEXT, underlying TEXT,
    instrument_type TEXT, strike REAL, expiry TEXT, lot_size INTEGER,
    first_seen TEXT, last_seen TEXT);
CREATE TABLE snapshots (
    instrument_token INTEGER, captured_at TEXT, last_price REAL, average_price REAL,
    volume REAL, oi REAL, buy_quantity REAL, sell_quantity REAL, bid REAL, ask REAL,
    source TEXT, PRIMARY KEY (instrument_token, captured_at));
CREATE TABLE candles_15m (
    instrument_token INTEGER, bar_start TEXT, open REAL, high REAL, low REAL,
    close REAL, volume REAL, oi REAL, PRIMARY KEY (instrument_token, bar_start));
CREATE TABLE underlying_snapshots (
    underlying TEXT, captured_at TEXT, spot REAL, fut_price REAL,
    total_ce_oi REAL, total_pe_oi REAL, total_ce_volume REAL, total_pe_volume REAL,
    pcr_oi REAL, pcr_volume REAL, max_pain_strike REAL,
    PRIMARY KEY (underlying, captured_at));
"""

TOKENS = {
    "CE2500": (1, "CE", 2500.0),
    "CE2600": (2, "CE", 2600.0),
    "PE2500": (3, "PE", 2500.0),
    "PE2600": (4, "PE", 2600.0),
    "FUT": (9, "FUT", None),
}


@pytest.fixture()
def db(tmp_path):
    """A small RELIANCE book: four strikes + the front future, four prior
    sessions of 15-minute candles, and two marks today."""
    path = tmp_path / "derivatives.db"
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.executescript(SPEC_TABLES)

    for name, (token, typ, strike) in TOKENS.items():
        conn.execute(
            "INSERT INTO contracts VALUES (?,?,?,?,?,?,?,?,?)",
            (token, f"RELIANCE26SEP{name}", "RELIANCE", typ, strike,
             EXPIRY.isoformat(), 250, "2026-08-01", "2026-09-18"),
        )

    # Four prior sessions of candles at 09:30 and 11:15, plus a 15:15 close.
    for day in (14, 15, 16, 17):
        for token, _, _ in TOKENS.values():
            for hh, mm, vol, close, oi in (
                (9, 30, 100_000, 30.0, 440_000),
                (11, 15, 300_000, 31.0, 445_000),
                (15, 15, 200_000, 30.0, 450_000),
            ):
                conn.execute(
                    "INSERT INTO candles_15m VALUES (?,?,?,?,?,?,?,?)",
                    (token, f"2026-09-{day} {hh:02d}:{mm:02d}:00",
                     close, close, close, close, vol, oi),
                )

    # Today: 11:00 and 11:15 marks.
    rows = {
        1: (40.0, 40.0, 1_200_000, 500_000),      # CE 2500 -- loud
        2: (20.0, 20.0, 200_000, 300_000),        # CE 2600 -- quiet
        3: (45.0, 45.0, 300_000, 600_000),        # PE 2500
        4: (15.0, 15.0, 100_000, 200_000),        # PE 2600
        9: (2520.0, 2515.0, 4_000_000, 1_000_000),  # the future
    }
    for token, (lp, ap, vol, oi) in rows.items():
        conn.execute("INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                     (token, "2026-09-18 11:00:00", lp - 5, ap, vol * 0.8, oi * 0.95,
                      0, 0, None, None, "kite"))
        conn.execute("INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                     (token, "2026-09-18 11:15:00", lp, ap, vol, oi,
                      0, 0, None, None, "kite"))
    conn.execute(
        "INSERT INTO underlying_snapshots (underlying, captured_at, spot) VALUES (?,?,?)",
        ("RELIANCE", "2026-09-18 11:15:00", 2505.0),
    )
    conn.commit()
    return conn


class TestLoaders:
    def test_contracts(self, db):
        cs = M.load_contracts(db)
        assert len(cs) == 5
        assert cs[1].instrument_type == "CE" and cs[1].strike == 2500.0
        assert cs[9].instrument_type == "FUT" and cs[9].strike is None
        assert cs[1].expiry == EXPIRY and cs[1].lot_size == 250

    def test_snapshots_at_one_mark(self, db):
        snaps = M.load_snapshots_at(db, MARK)
        assert set(snaps) == {1, 2, 3, 4, 9}
        assert snaps[1].last_price == 40.0 and snaps[1].oi == 500_000

    def test_previous_close_is_the_last_bar_of_the_last_prior_session(self, db):
        closes = M.load_previous_closes(db, date(2026, 9, 18))
        assert closes[1].session == date(2026, 9, 17)
        assert closes[1].oi == 450_000
        assert closes[1].volume == 600_000          # 100k + 300k + 200k

    def test_previous_close_never_uses_today(self, db):
        db.execute("INSERT INTO candles_15m VALUES (?,?,?,?,?,?,?,?)",
                   (1, "2026-09-18 09:30:00", 1, 1, 1, 1, 99, 99))
        db.commit()
        assert M.load_previous_closes(db, date(2026, 9, 18))[1].session == date(2026, 9, 17)

    def test_time_of_day_baseline_stops_at_the_clock_time(self, db):
        base = M.load_tod_baselines(db, MARK)
        # 09:30 (100k) + 11:15 (300k) on each of four prior sessions; 15:15 excluded
        assert base[1] == [400_000.0] * 4

    def test_oi_history_is_the_closing_oi_per_prior_session(self, db):
        hist = M.load_daily_oi_history(db, date(2026, 9, 18))
        assert hist[9] == [450_000.0] * 4

    def test_spot(self, db):
        assert M.load_spots(db, MARK) == {"RELIANCE": 2505.0}

    def test_missing_table_is_a_clear_error_not_a_crash(self, tmp_path):
        conn = sqlite3.connect(str(tmp_path / "empty.db"))
        conn.row_factory = sqlite3.Row
        with pytest.raises(LookupError, match="contracts"):
            M.load_contracts(conn)


class TestComputeAndRead:
    def test_compute_writes_contract_and_underlying_rows(self, db):
        res = M.compute_for_mark(db, MARK)
        assert res.skipped_no_snapshot == 0
        assert len(res.contract_rows) == 5          # 4 options + 1 future
        assert len(res.underlying_rows) == 1        # one underlying, one expiry
        assert len(res.futures) == 1
        assert res.written == 6
        stored = db.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
        assert stored == 6

    def test_recomputing_the_same_mark_does_not_duplicate(self, db):
        M.compute_for_mark(db, MARK)
        M.compute_for_mark(db, MARK)
        assert db.execute("SELECT COUNT(*) FROM metrics").fetchone()[0] == 6

    def test_unusual_list_is_per_underlying_and_expands_to_strikes(self, db):
        M.compute_for_mark(db, MARK)
        out = M.read_unusual_activity(db)
        assert out["as_of"] == "2026-09-18 11:15:00"
        assert out["floors"]["min_premium_cr"] == pytest.approx(2.0)
        assert len(out["rows"]) == 1
        row = out["rows"][0]
        assert row["underlying"] == "RELIANCE"
        assert row["headline"].startswith("RELIANCE - ")
        # CE 2500 is the only leg over Rs 2 crore with a 2x+ trigger
        syms = [s["tradingsymbol"] for s in row["strikes"]]
        assert syms == ["RELIANCE26SEPCE2500"]
        assert row["strikes"][0]["unusual_reasons"]

    def test_option_chain_pairs_ce_and_pe_by_strike(self, db):
        M.compute_for_mark(db, MARK)
        chain = M.read_option_chain(db, "RELIANCE", EXPIRY)
        assert [r["strike"] for r in chain["rows"]] == [2500.0, 2600.0]
        first = chain["rows"][0]
        assert first["ce"]["instrument_type"] == "CE"
        assert first["pe"]["instrument_type"] == "PE"
        assert first["ce"]["buildup_15m"] and first["ce"]["buildup_day"]
        assert chain["spot"] == 2505.0
        assert chain["days_to_expiry"] == 6

    def test_oi_by_strike_carries_max_pain_and_spot(self, db):
        M.compute_for_mark(db, MARK)
        out = M.read_oi_by_strike(db, "RELIANCE", EXPIRY)
        assert out["spot"] == 2505.0
        assert out["max_pain"]["status"] == M.STATUS_OK
        assert out["max_pain"]["strike"] in (2500.0, 2600.0)
        assert out["max_pain"]["total_oi"] == 1_600_000
        assert out["rows"][0]["ce_oi"] == 500_000 and out["rows"][0]["pe_oi"] == 600_000

    def test_index_summary_shape(self, db):
        M.compute_for_mark(db, MARK)
        out = M.read_index_summary(db, underlyings=["RELIANCE"])
        assert len(out["rows"]) == 1
        r = out["rows"][0]
        assert r["pcr_oi"] == pytest.approx(800_000 / 800_000)
        assert r["pcr_trend"] == M.STATUS_NO_BASELINE      # only one mark so far
        assert r["max_pain_strike"] is not None
        assert len(r["pcr_series"]) == 1

    def test_pcr_trend_appears_once_there_are_two_marks(self, db):
        M.compute_for_mark(db, datetime(2026, 9, 18, 11, 0, 0))
        M.compute_for_mark(db, MARK)
        r = M.read_index_summary(db, underlyings=["RELIANCE"])["rows"][0]
        assert r["pcr_trend"] in ("rising", "falling", "flat")
        assert len(r["pcr_series"]) == 2

    def test_futures_table(self, db):
        M.compute_for_mark(db, MARK)
        out = M.read_futures_buildup(db)
        assert len(out["rows"]) == 1
        f = out["rows"][0]
        assert f["instrument_type"] == "FUT"
        assert f["basis"] == pytest.approx(15.0)            # 2520 - 2505
        assert f["basis_status"] == M.STATUS_OK
        assert f["fut_oi_vs_avg"] == pytest.approx(1_000_000 / 450_000)
        assert f["buildup_15m"] in M.BUILDUP_LABELS
        assert f["days_to_expiry"] == 6

    def test_read_functions_are_empty_but_honest_before_any_compute(self, db):
        M.ensure_metrics_table(db)
        out = M.read_unusual_activity(db)
        assert out["rows"] == [] and out["as_of"] is None
        assert out["status"] == "no metrics yet"
        assert out["floors"]["min_last_price"] == 1.0

    def test_writer_drops_columns_the_live_schema_lacks_instead_of_failing(self, tmp_path, caplog):
        """D1 owns schema.sql; a narrower metrics table must not crash the run."""
        conn = sqlite3.connect(str(tmp_path / "narrow.db"))
        conn.row_factory = sqlite3.Row
        conn.execute(
            "CREATE TABLE metrics (scope TEXT, metric_key TEXT, captured_at TEXT,"
            " underlying TEXT, unusual INTEGER, PRIMARY KEY (scope, metric_key, captured_at))"
        )
        written = M.write_metric_rows(conn, [build_metrics().to_row()])
        assert written == 1
        assert conn.execute("SELECT underlying FROM metrics").fetchone()[0] == "RELIANCE"

    def test_provenance_columns_are_filled_when_given(self, tmp_path):
        conn = M.connect(tmp_path / "prov.db")
        M.write_metric_rows(conn, [build_metrics().to_row()],
                            vendor_id="kite", fetched_at="2026-09-18T11:15:10",
                            snapshot_id="snap-1")
        row = conn.execute("SELECT vendor_id, snapshot_id FROM metrics").fetchone()
        assert row["vendor_id"] == "kite" and row["snapshot_id"] == "snap-1"


def test_cli_floors_runs_without_a_database(capsys):
    from market_data.derivatives import metrics_cli

    assert metrics_cli.main(["floors"]) == 0
    assert "min_premium_cr" in capsys.readouterr().out


# ===========================================================================
# The public aliases D3's Derivative tab calls (flat row lists, keywords only)
# ===========================================================================


class TestAppAliases:
    """D3 calls `reader(**kwargs)` with no connection and expects a flat list of
    row dicts; the `read_*` functions return the enveloped shape.  Both spellings
    must work, off the same numbers."""

    def test_every_name_d3_looks_for_exists_and_is_callable(self):
        for name in ("unusual_activity", "option_chain", "oi_by_strike",
                     "index_summary", "futures_buildup"):
            assert callable(getattr(M, name)), name

    def test_unusual_activity_returns_flat_strike_rows(self, db):
        M.compute_for_mark(db, MARK)
        rows = M.unusual_activity(conn=db)
        assert isinstance(rows, list) and rows
        assert all(isinstance(r, dict) for r in rows)
        assert [r["tradingsymbol"] for r in rows] == ["RELIANCE26SEPCE2500"]
        # the same legs the enveloped spelling nests under its underlying row
        nested = M.read_unusual_activity(db)["rows"][0]["strikes"]
        assert [r["tradingsymbol"] for r in nested] == [r["tradingsymbol"] for r in rows]

    def test_unusual_activity_honours_its_filters(self, db):
        M.compute_for_mark(db, MARK)
        assert M.unusual_activity(conn=db, underlying="NOSUCH") == []
        assert M.unusual_activity(conn=db, option_type="PE") == []
        assert M.unusual_activity(conn=db, option_type="CE")
        assert M.unusual_activity(conn=db, expiry=EXPIRY)
        assert M.unusual_activity(conn=db, expiry="2026-12-31") == []
        assert M.unusual_activity(conn=db, max_days_to_expiry=1) == []
        assert M.unusual_activity(conn=db, max_days_to_expiry=6)

    def test_min_premium_cr_can_only_raise_the_floor(self, db):
        M.compute_for_mark(db, MARK)
        # the one unusual leg traded Rs 4.8 crore
        assert M.unusual_activity(conn=db, min_premium_cr=4.0)
        assert M.unusual_activity(conn=db, min_premium_cr=10.0) == []
        # a floor below the spec's Rs 2 crore must not widen the list
        assert len(M.unusual_activity(conn=db, min_premium_cr=0.0)) == \
            len(M.unusual_activity(conn=db))

    def test_option_chain_returns_flat_legs_not_pairs(self, db):
        M.compute_for_mark(db, MARK)
        rows = M.option_chain(underlying="RELIANCE", expiry=EXPIRY, conn=db)
        assert len(rows) == 4
        assert [(r["strike"], r["instrument_type"]) for r in rows] == [
            (2500.0, "CE"), (2500.0, "PE"), (2600.0, "CE"), (2600.0, "PE")]

    def test_option_chain_finds_the_front_expiry_when_none_is_given(self, db):
        M.compute_for_mark(db, MARK)
        assert M.option_chain(underlying="RELIANCE", conn=db) == \
            M.option_chain(underlying="RELIANCE", expiry=EXPIRY, conn=db)
        assert M.option_chain(underlying="NOSUCH", conn=db) == []

    def test_oi_by_strike_and_index_summary_are_flat(self, db):
        M.compute_for_mark(db, MARK)
        strikes = M.oi_by_strike(underlying="RELIANCE", expiry=EXPIRY, conn=db)
        assert [r["strike"] for r in strikes] == [2500.0, 2600.0]
        assert strikes[0]["ce_oi"] == 500_000
        index = M.index_summary(underlyings=["RELIANCE"], conn=db)
        assert len(index) == 1 and index[0]["underlying"] == "RELIANCE"
        assert index[0]["pcr_oi"] is not None

    def test_futures_buildup_dispatches_on_its_first_argument(self, db):
        """The pure section 3.7 computation and D3's card-5 reader share the name."""
        M.compute_for_mark(db, MARK)
        rows = M.futures_buildup(underlying="RELIANCE", limit=10, conn=db)
        assert isinstance(rows, list) and len(rows) == 1
        assert rows[0]["instrument_type"] == "FUT"

        computed = M.futures_buildup(
            M.ContractRef(9, "RELIANCE26SEPFUT", "RELIANCE", "FUT", EXPIRY, None, 250),
            M.Snapshot(9, MARK, last_price=2520.0, average_price=2515.0,
                       volume=4_000_000, oi=1_000_000),
            spot=2505.0,
        )
        assert isinstance(computed, M.FuturesBuildUp)
        assert computed.basis == pytest.approx(15.0)
        assert computed.basis == rows[0]["basis"]

    def test_futures_buildup_refuses_to_guess(self, db):
        with pytest.raises(TypeError, match="ContractRef"):
            M.futures_buildup("RELIANCE")

    def test_rows_carry_the_key_spellings_the_app_shaper_reads(self, db):
        """D3's `_shape`/`_passes` read canonical names off the delegated row."""
        M.compute_for_mark(db, MARK)
        row = M.unusual_activity(conn=db)[0]
        for key in ("captured_at", "tradingsymbol", "underlying", "instrument_type",
                    "strike", "expiry", "lot_size", "days_to_expiry", "last_price",
                    "oi", "oi_lots", "volume", "average_price", "premium_cr",
                    "premium_inr", "price_change_15m_pct", "oi_change_15m",
                    "oi_change_15m_pct", "buildup_15m", "price_change_day_pct",
                    "oi_change_day", "oi_change_day_pct", "buildup_day",
                    "volume_ratio", "volume_baseline_sessions", "volume_to_oi",
                    "previous_oi"):
            assert key in row, f"D3's shaper reads {key!r} and the row has no such key"
        assert row["oi_lots"] == pytest.approx(500_000 / 250)
        assert row["volume_ratio"] == row["vol_tod_ratio"]
        assert row["previous_oi"] == row["vol_oi_prev_oi"]
        assert row["volume_to_oi"] == row["vol_oi_ratio"]

    def test_futures_rows_carry_the_futures_field_spellings(self, db):
        M.compute_for_mark(db, MARK)
        row = M.futures_buildup(conn=db)[0]
        for key in ("basis", "oi_vs_20d_avg", "buildup_15m", "buildup_day",
                    "days_to_expiry", "oi_lots", "captured_at"):
            assert key in row, key

    def test_a_percentage_column_is_never_a_rupee_change(self, db):
        """`price_change_15m_pct` must be the percentage, not the absolute move.

        The app labels it as a percentage and would print a wrong number if the
        rupee move ever arrived under that name.  Since 2026-09-19 the rupee
        columns (`price_change_15m`, `price_change_day`) are not stored at all —
        the Derivative tab refuses rupee moves outright, and they were 12.4 of
        the metric record's 322 measured bytes.  That makes the confusion
        impossible rather than merely wrong, so the guard is now: the percentage
        is the percentage, and the rupee column is absent, not NULL.
        """
        M.compute_for_mark(db, MARK)
        row = M.unusual_activity(conn=db)[0]
        # CE 2500 went 35 -> 40 in the 15 minutes: +5 rupees, +14.3%
        assert row["price_change_15m_pct"] == pytest.approx(100.0 * 5.0 / 35.0)
        assert "price_change_15m" not in row
        assert "price_change_day" not in row
        stored = {r[1] for r in db.execute("PRAGMA table_info(metrics)")}
        assert stored & M.RETIRED_METRIC_COLUMNS == set()

    def test_aliases_open_their_own_store_when_given_no_connection(self, db, tmp_path,
                                                                   monkeypatch):
        """D3 passes no connection at all, so the path must resolve on its own."""
        M.compute_for_mark(db, MARK)
        db.commit()
        monkeypatch.setenv("KANIDA_DERIVATIVES_DB", str(tmp_path / "derivatives.db"))
        assert M.resolve_db_path() == tmp_path / "derivatives.db"
        rows = M.unusual_activity()
        assert [r["tradingsymbol"] for r in rows] == ["RELIANCE26SEPCE2500"]

    def test_a_store_that_does_not_exist_yet_raises_a_clear_lookup_error(self, tmp_path,
                                                                        monkeypatch):
        monkeypatch.setenv("KANIDA_DERIVATIVES_DB", str(tmp_path / "missing.db"))
        with pytest.raises(LookupError, match="does not exist yet"):
            M.unusual_activity()


class TestAppReaderIntegration:
    """Drive worker D3's own reader against this module, end to end.

    Skipped when the app tree is not on this machine -- `market_data` must not
    depend on `kanida-app` to be testable."""

    @pytest.fixture()
    def app_reader(self, db, tmp_path, monkeypatch):
        # D3's `_delegate` passes its card keywords only -- no connection and no
        # path -- so the aliases resolve the store themselves.  Point both sides
        # at this fixture's store instead of the real `db/derivatives.db`.
        monkeypatch.setenv("KANIDA_DERIVATIVES_DB", str(tmp_path / "derivatives.db"))
        server = ROOT / "kanida-app" / "server"
        if not (server / "kanida_pilot" / "derivatives.py").is_file():
            pytest.skip("kanida-app server tree not present")
        if str(server) not in sys.path:
            sys.path.insert(0, str(server))
        derivatives = pytest.importorskip("kanida_pilot.derivatives")
        M.compute_for_mark(db, MARK)
        db.commit()
        return derivatives.Derivatives(str(tmp_path / "derivatives.db"), metrics_module=M)

    def test_the_tab_serves_our_numbers_and_says_so(self, app_reader):
        card = app_reader.unusual()
        assert card["source"] == "metrics_module"      # not the raw store fallback
        assert card["rows"], "the unusual card fell through to an empty list"
        group = card["rows"][0]
        assert group["underlying"] == "RELIANCE"
        leg = group["strikes"][0]
        assert leg["tradingsymbol"] == "RELIANCE26SEPCE2500"
        assert leg["premium_cr"] == pytest.approx(4.8)
        assert leg["buildup_15m"] == M.BUILDUP_LONG
        # the app rounds for display; the ratio underneath is ours
        assert leg["volume_to_oi"] == round(1_200_000 / 450_000, 2)

    def test_the_volume_ratio_survives_the_app_baseline_gate(self, app_reader):
        """§3.2 is re-checked app-side: without our sessions count it prints
        "no baseline" even though we computed a ratio."""
        leg = app_reader.unusual()["rows"][0]["strikes"][0]
        assert leg["volume_baseline_sessions"] == 4
        assert leg["volume_baseline"] == "ok"
        # 1.2m units today vs a 400k median at 11:15 over four sessions
        assert leg["volume_ratio"] == round(1_200_000 / 400_000, 2)

    def test_chain_and_futures_cards_are_served_by_us_too(self, app_reader):
        chain = app_reader.chain("RELIANCE", EXPIRY.isoformat())
        assert chain["source"] == "metrics_module"
        assert [r["strike"] for r in chain["rows"]] == [2500.0, 2600.0]
        assert chain["rows"][0]["ce"]["buildup_day"]
        futures = app_reader.futures()
        assert futures["source"] == "metrics_module"
        assert futures["rows"], "the futures card fell through"
        assert futures["rows"][0]["basis"] == pytest.approx(15.0)
        assert futures["rows"][0]["oi_vs_20d_avg"] == round(1_000_000 / 450_000, 2)
