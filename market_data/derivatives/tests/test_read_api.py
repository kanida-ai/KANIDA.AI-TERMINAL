"""The per-strike ΔOI series: baseline, gaps, and the direction rule."""
import re
from datetime import date

import pytest

from market_data.derivatives import config
from market_data.derivatives.read_api import (
    DIRECTION_LOOKBACK_MARKS,
    FLAT_FRACTION,
    FLOW_FLAT_MEANING,
    FLOW_FLAT_WHAT,
    FLOW_LABELS,
    FLOW_NOT_ENOUGH,
    PRICE_FLAT_FRACTION,
    _flow,
    _price_direction,
    atm_strike,
    strike_oi_series,
)
from market_data.derivatives.store import (
    CANDLE_COLUMNS,
    SNAPSHOT_COLUMNS,
    UNDERLYING_COLUMNS,
    DerivativesStore,
)

SESSION = "2026-09-18"
PREV = "2026-09-17"
STRIKES = [23100.0, 23150.0, 23200.0, 23250.0, 23300.0, 23350.0, 23400.0,
           23450.0, 23500.0, 23550.0]
MARKS = [f"{SESSION} {t}" for t in
         ("09:30:00", "09:45:00", "10:00:00", "10:15:00", "10:30:00", "10:45:00")]


def _token(strike, side):
    return int(strike) * 10 + (1 if side == "CE" else 2)


@pytest.fixture
def store(tmp_path):
    st = DerivativesStore(tmp_path / "d.db")
    rows = []
    for strike in STRIKES:
        for side in ("CE", "PE"):
            rows.append((_token(strike, side),
                         f"NIFTY26922{int(strike)}{side}", "NIFTY", side, strike,
                         "2026-09-22", 65, 0.05, "NFO", "NFO-OPT", "seen", "seen",
                         1, "kite", "2026-09-18T04:00:00", None))
    st.con.executemany(
        "INSERT INTO contracts (instrument_token, tradingsymbol, underlying, "
        "instrument_type, strike, expiry, lot_size, tick_size, exchange, segment, "
        "first_seen, last_seen, in_scope, vendor_id, fetched_at, snapshot_id) "
        "VALUES (" + ",".join("?" * 16) + ")", rows)
    return st


def _snapshot(store, token, at, oi, mark_kind="bar_close", last_price=100.0):
    values = dict(zip(SNAPSHOT_COLUMNS, [None] * len(SNAPSHOT_COLUMNS)))
    values.update(instrument_token=token, captured_at=at, mark_kind=mark_kind, oi=oi,
                  last_price=last_price, volume=10, source="kite.quote", vendor_id="kite",
                  fetched_at="2026-09-18T05:00:00", snapshot_id="cap_x")
    store.write_snapshots([tuple(values[c] for c in SNAPSHOT_COLUMNS)])


def _prev_close(store, token, oi, bar_start=f"{PREV} 15:30:00"):
    values = dict(zip(CANDLE_COLUMNS, [None] * len(CANDLE_COLUMNS)))
    values.update(instrument_token=token, bar_start=bar_start, open=1.0, high=1.0,
                  low=1.0, close=1.0, volume=1, oi=oi, vendor_id="kite",
                  fetched_at="2026-09-18T04:00:00")
    store.write_candles([tuple(values[c] for c in CANDLE_COLUMNS)])


def _spot(store, at, spot, mark_kind="bar_close"):
    values = dict(zip(UNDERLYING_COLUMNS, [None] * len(UNDERLYING_COLUMNS)))
    values.update(underlying="NIFTY", captured_at=at, mark_kind=mark_kind, spot=spot,
                  spot_symbol="NIFTY 50", vendor_id="kite",
                  fetched_at="2026-09-18T05:00:00", snapshot_id="cap_x")
    store.write_underlying_snapshots([tuple(values[c] for c in UNDERLYING_COLUMNS)])


def _populate(store, *, oi_by_mark=None, spot=23315.0, price_by_mark=None):
    for at in MARKS:
        _spot(store, at, spot)
    for strike in STRIKES:
        for side in ("CE", "PE"):
            token = _token(strike, side)
            _prev_close(store, token, 1000)
            for i, at in enumerate(MARKS):
                oi = (oi_by_mark or (lambda i: 1000 + i * 100))(i)
                price = (price_by_mark or (lambda i: 100.0))(i)
                _snapshot(store, token, at, oi, last_price=price)


# ── the ATM ──────────────────────────────────────────────────────────────────

def test_atm_is_the_listed_strike_nearest_spot():
    assert atm_strike(STRIKES, 23315.0) == 23300.0
    assert atm_strike(STRIKES, 23340.0) == 23350.0
    assert atm_strike(STRIKES, 0.0) == 23100.0          # below the ladder
    assert atm_strike(STRIKES, 99999.0) == 23550.0      # above it
    assert atm_strike([], 100.0) is None


def test_the_ten_contracts_are_atm_ce_up_and_atm_pe_down(store):
    _populate(store)
    out = strike_oi_series(store, "NIFTY")
    assert out["atm_strike"] == 23300.0
    assert out["as_of"] == MARKS[-1]
    got = {(c["option_type"], c["atm_offset"]): c["strike"] for c in out["contracts"]}
    assert got == {
        ("CE", 0): 23300.0, ("CE", 1): 23350.0, ("CE", 2): 23400.0,
        ("CE", 3): 23450.0, ("CE", 4): 23500.0,
        ("PE", 0): 23300.0, ("PE", -1): 23250.0, ("PE", -2): 23200.0,
        ("PE", -3): 23150.0, ("PE", -4): 23100.0}
    assert len(out["contracts"]) == 10


def test_the_atm_basis_mark_is_reported(store):
    _populate(store)
    out = strike_oi_series(store, "NIFTY")
    assert out["atm_basis"]["mark"] == MARKS[-1]
    assert out["atm_basis"]["spot"] == 23315.0


# ── the baseline ─────────────────────────────────────────────────────────────

def test_delta_is_measured_against_the_previous_session_close(store):
    _populate(store)
    ce = next(c for c in strike_oi_series(store, "NIFTY")["contracts"]
              if c["atm_offset"] == 0 and c["option_type"] == "CE")
    assert ce["previous_close_oi"] == 1000
    assert [p["delta_oi"] for p in ce["points"]] == [0, 100, 200, 300, 400, 500]
    assert [p["oi"] for p in ce["points"]] == [1000, 1100, 1200, 1300, 1400, 1500]


def test_without_a_previous_close_the_delta_is_none_and_the_direction_has_no_baseline(store):
    _populate(store)
    orphan = _token(23300.0, "CE")
    store.con.execute("DELETE FROM candles_15m WHERE instrument_token=?", (orphan,))
    ce = next(c for c in strike_oi_series(store, "NIFTY")["contracts"]
              if c["instrument_token"] == orphan)
    assert ce["previous_close_oi"] is None
    assert all(p["delta_oi"] is None for p in ce["points"])
    assert all(p["oi"] is not None for p in ce["points"])   # the OI itself is real
    assert ce["direction"] == "no baseline"


def test_a_missing_mark_is_a_gap_and_is_never_interpolated(store):
    _populate(store)
    token = _token(23300.0, "CE")
    store.con.execute("DELETE FROM snapshots WHERE instrument_token=? AND captured_at=?",
                      (token, MARKS[2]))
    ce = next(c for c in strike_oi_series(store, "NIFTY")["contracts"]
              if c["instrument_token"] == token)
    stamps = [p["at"] for p in ce["points"]]
    assert MARKS[2] not in stamps
    assert len(stamps) == len(MARKS) - 1
    assert stamps == sorted(stamps)


def test_a_single_mark_cannot_have_a_direction(store):
    for at in MARKS[:1]:
        _spot(store, at, 23315.0)
    token = _token(23300.0, "CE")
    _prev_close(store, token, 1000)
    _snapshot(store, token, MARKS[0], 1200)
    ce = next(c for c in strike_oi_series(store, "NIFTY")["contracts"]
              if c["instrument_token"] == token)
    assert len(ce["points"]) == 1
    assert ce["direction"] == "no baseline"


# ── the direction rule ───────────────────────────────────────────────────────

def test_direction_compares_now_with_four_marks_ago(store):
    _populate(store, oi_by_mark=lambda i: [1000, 5000, 5000, 5000, 5000, 5100][i])
    ce = next(c for c in strike_oi_series(store, "NIFTY")["contracts"]
              if c["atm_offset"] == 0 and c["option_type"] == "CE")
    # deltas: 0, 4000, 4000, 4000, 4000, 4100 — against 4 marks ago (4000) the
    # move is +100 on a scale of 4100, i.e. under the 5% flat band
    assert ce["direction_detail"]["from"] == MARKS[1]
    assert ce["direction_detail"]["change"] == 100
    assert ce["direction"] == "flat"


def test_building_and_unwinding(store):
    _populate(store, oi_by_mark=lambda i: 1000 + i * 1000)
    up = next(c for c in strike_oi_series(store, "NIFTY")["contracts"]
              if c["atm_offset"] == 0 and c["option_type"] == "CE")
    assert up["direction"] == "building"

    _populate(store, oi_by_mark=lambda i: [1000, 9000, 8000, 7000, 6000, 5000][i])
    down = next(c for c in strike_oi_series(store, "NIFTY")["contracts"]
                if c["atm_offset"] == 0 and c["option_type"] == "CE")
    assert down["direction"] == "unwinding"


def test_the_flat_band_is_five_percent_of_the_contracts_own_largest_move(store):
    _populate(store, oi_by_mark=lambda i: [1000, 21000, 21000, 21000, 21000, 21900][i])
    ce = next(c for c in strike_oi_series(store, "NIFTY")["contracts"]
              if c["atm_offset"] == 0 and c["option_type"] == "CE")
    # largest |delta| = 20900 -> band 1045; the move of +900 sits inside it
    assert ce["direction_detail"]["flat_threshold"] == pytest.approx(
        FLAT_FRACTION * 20900)
    assert ce["direction"] == "flat"

    _populate(store, oi_by_mark=lambda i: [1000, 21000, 21000, 21000, 21000, 23000][i])
    ce2 = next(c for c in strike_oi_series(store, "NIFTY")["contracts"]
               if c["atm_offset"] == 0 and c["option_type"] == "CE")
    assert ce2["direction"] == "building"


# ── what it refuses to do ────────────────────────────────────────────────────

def test_the_post_close_mark_is_not_used_as_the_as_of(store):
    _populate(store)
    _spot(store, f"{SESSION} 15:45:00", 23315.0, mark_kind="post_close")
    _snapshot(store, _token(23300.0, "CE"), f"{SESSION} 15:45:00", 9999,
              mark_kind="post_close")
    out = strike_oi_series(store, "NIFTY")
    assert out["as_of"] == MARKS[-1]
    ce = next(c for c in out["contracts"] if c["atm_offset"] == 0
              and c["option_type"] == "CE")
    assert all(p["at"] != f"{SESSION} 15:45:00" for p in ce["points"])


def test_no_spot_means_no_atm_and_it_says_so(store):
    for at in MARKS:
        _spot(store, at, None)
    _snapshot(store, _token(23300.0, "CE"), MARKS[0], 1000)
    out = strike_oi_series(store, "NIFTY")
    assert out["contracts"] == [] and out["atm_strike"] is None
    assert "no spot" in out["note"]


def test_an_unknown_underlying_returns_a_note_not_an_exception(store):
    _populate(store)
    out = strike_oi_series(store, "NOTLISTED")
    assert out["contracts"] == []
    assert out["note"]


# ── the price line ───────────────────────────────────────────────────────────

def test_every_point_carries_the_contracts_own_price(store):
    _populate(store, price_by_mark=lambda i: 100.0 + i * 10)
    ce = next(c for c in strike_oi_series(store, "NIFTY")["contracts"]
              if c["atm_offset"] == 0 and c["option_type"] == "CE")
    assert [p["price"] for p in ce["points"]] == [100.0, 110.0, 120.0, 130.0, 140.0, 150.0]
    assert sorted(ce["points"][0]) == ["at", "delta_oi", "oi", "price"]


def test_a_mark_with_no_price_is_a_gap_on_the_price_line_only(store):
    _populate(store, price_by_mark=lambda i: 100.0 + i * 10)
    token = _token(23300.0, "CE")
    store.con.execute("UPDATE snapshots SET last_price=NULL WHERE instrument_token=? "
                      "AND captured_at=?", (token, MARKS[2]))
    ce = next(c for c in strike_oi_series(store, "NIFTY")["contracts"]
              if c["instrument_token"] == token)
    assert [p["price"] for p in ce["points"]] == [100.0, 110.0, None, 130.0, 140.0, 150.0]
    # the ΔOI beside it is untouched: the two gaps are independent, and neither is filled in
    assert all(p["delta_oi"] is not None for p in ce["points"])


def test_a_contract_with_no_price_at_all_has_no_price_direction(store):
    _populate(store)
    token = _token(23300.0, "CE")
    store.con.execute("UPDATE snapshots SET last_price=NULL WHERE instrument_token=?", (token,))
    ce = next(c for c in strike_oi_series(store, "NIFTY")["contracts"]
              if c["instrument_token"] == token)
    assert all(p["price"] is None for p in ce["points"])
    assert ce["flow"]["price_direction"] == "no baseline"
    assert ce["flow"]["what_label"] == FLOW_NOT_ENOUGH
    assert ce["flow"]["meaning"] is None
    assert ce["direction"] == "building", "the ΔOI reading is unaffected by a missing price"


# ── price and OI read together ───────────────────────────────────────────────

def _points(deltas, prices):
    return [{"at": f"{SESSION} 10:{i:02d}", "oi": 1000 + d, "delta_oi": d, "price": p}
            for i, (d, p) in enumerate(zip(deltas, prices))]


OI_SERIES = {
    "building": [0, 20000, 40000, 60000, 80000, 100000],
    "unwinding": [0, -20000, -40000, -60000, -80000, -100000],
    "flat": [0] * 6,
}
PRICE_SERIES = {
    "up": [100.0, 104.0, 109.0, 115.0, 122.0, 130.0],
    "down": [130.0, 122.0, 115.0, 109.0, 104.0, 100.0],
    "flat": [100.0] * 6,
}
BUILD = OI_SERIES["building"]
UNWIND = OI_SERIES["unwinding"]
UP = PRICE_SERIES["up"]
DOWN = PRICE_SERIES["down"]
#: every one of the nine (price x OI) combinations, per option type.  A flat axis has its OWN row: collapsing
#: the mixed cases into the both-flat wording put a tile at war with its own chip on screen.
TABLE = {
    ("CE", "down", "building"): ("Call writing increasing", "Sellers are building resistance"),
    ("CE", "up", "unwinding"): ("Call short covering", "Call sellers are exiting"),
    ("CE", "up", "building"): ("Call buying increasing", "Traders are buying upside"),
    ("CE", "down", "unwinding"): ("Call buyers exiting", "Call buyers are closing out"),
    ("CE", "flat", "building"): ("New positions added", "Premium barely moved"),
    ("CE", "flat", "unwinding"): ("Positions closing out", "Premium barely moved"),
    ("CE", "up", "flat"): ("Premium rose", "Open interest barely moved"),
    ("CE", "down", "flat"): ("Premium fell", "Open interest barely moved"),
    ("CE", "flat", "flat"): ("Very little change", "Positioning is unchanged"),
    ("PE", "down", "building"): ("Put writing increasing", "Sellers are building support"),
    ("PE", "up", "unwinding"): ("Put short covering", "Put sellers are exiting"),
    ("PE", "up", "building"): ("Put buying increasing", "Traders are buying downside protection"),
    ("PE", "down", "unwinding"): ("Put buyers exiting", "Put buyers are closing out"),
    ("PE", "flat", "building"): ("New positions added", "Premium barely moved"),
    ("PE", "flat", "unwinding"): ("Positions closing out", "Premium barely moved"),
    ("PE", "up", "flat"): ("Premium rose", "Open interest barely moved"),
    ("PE", "down", "flat"): ("Premium fell", "Open interest barely moved"),
    ("PE", "flat", "flat"): ("Very little change", "Positioning is unchanged"),
}


@pytest.mark.parametrize("key,expected", sorted(TABLE.items()))
def test_every_row_of_the_price_and_oi_table(key, expected):
    kind, price_way, oi_way = key
    flow = _flow(kind, _points(OI_SERIES[oi_way], PRICE_SERIES[price_way]))
    assert flow["price_direction"] == price_way
    assert flow["oi_direction"] == oi_way
    assert (flow["what_label"], flow["meaning"]) == expected


def test_the_table_is_the_one_the_pilot_serves():
    assert FLOW_LABELS == {f"{k[0]}|{k[1]}|{k[2]}": v for k, v in TABLE.items()}
    assert len(FLOW_LABELS) == 18
    for kind in ("CE", "PE"):
        for price_way in ("up", "down", "flat"):
            for oi_way in ("building", "unwinding", "flat"):
                assert f"{kind}|{price_way}|{oi_way}" in FLOW_LABELS


def test_the_sentence_never_contradicts_the_chip():
    """A property over the whole table, not a list of cases.

    When OI has a direction the sentence must say something happened to open
    interest; when OI is flat it must say open interest barely moved.  A tile
    reading "unwinding" over "positioning is unchanged" is the bug this blocks.
    """
    moved = re.compile(r"\b(writing|covering|buying|exiting|added|closing out)\b", re.I)
    still = re.compile(r"\b(open interest barely moved|positioning is unchanged)\b", re.I)
    for (kind, price_way, oi_way), (what, meaning) in TABLE.items():
        sentence = f"{what}. {meaning}"
        if oi_way == "flat":
            assert still.search(sentence), (kind, price_way, oi_way)
            assert not moved.search(what), (kind, price_way, oi_way)
        else:
            assert moved.search(what), (kind, price_way, oi_way)
            assert not still.search(sentence), (kind, price_way, oi_way)
            assert (what, meaning) != (FLOW_FLAT_WHAT, FLOW_FLAT_MEANING)
        flow = _flow(kind, _points(OI_SERIES[oi_way], PRICE_SERIES[price_way]))
        assert (flow["what_label"], flow["meaning"]) == (what, meaning)


@pytest.mark.parametrize("kind", ["CE", "PE"])
def test_only_both_axes_still_reads_as_very_little_change(kind):
    flow = _flow(kind, _points(OI_SERIES["flat"], PRICE_SERIES["flat"]))
    assert flow["what_label"] == FLOW_FLAT_WHAT
    assert flow["meaning"] == FLOW_FLAT_MEANING
    # a flat premium over an hour in which open interest plainly moved is NOT "nothing happened"
    for oi_way, expected in (("building", "New positions added"),
                             ("unwinding", "Positions closing out")):
        moving = _flow(kind, _points(OI_SERIES[oi_way], PRICE_SERIES["flat"]))
        assert moving["what_label"] == expected
        assert moving["what_label"] != FLOW_FLAT_WHAT
    # and the other way round
    for price_way, expected in (("up", "Premium rose"), ("down", "Premium fell")):
        moving = _flow(kind, _points(OI_SERIES["flat"], PRICE_SERIES[price_way]))
        assert moving["what_label"] == expected


@pytest.mark.parametrize("kind", ["CE", "PE"])
def test_the_bug_the_owner_saw_on_the_atm_put(kind):
    """Unwinding open interest beside a barely-moved premium is never "unchanged"."""
    # the premium jumped early and has barely moved since, so it is flat against its OWN largest move
    barely = [120.0, 140.0, 140.0, 140.0, 140.0, 140.05]
    flow = _flow(kind, _points(OI_SERIES["unwinding"], barely))
    assert flow["price_direction"] == "flat" and flow["oi_direction"] == "unwinding"
    assert flow["what_label"] == "Positions closing out"
    assert flow["meaning"] == "Premium barely moved"


@pytest.mark.parametrize("kind", ["CE", "PE"])
def test_no_baseline_on_either_side_is_never_guessed(kind):
    assert _flow(kind, [])["what_label"] == FLOW_NOT_ENOUGH
    one = _points([0], [100.0])
    assert _flow(kind, one)["what_label"] == FLOW_NOT_ENOUGH
    assert _flow(kind, one)["meaning"] is None
    unpriced = _points(BUILD, [None] * 6)
    assert _flow(kind, unpriced)["price_direction"] == "no baseline"
    assert _flow(kind, unpriced)["what_label"] == FLOW_NOT_ENOUGH
    no_delta = [{"at": f"{SESSION} 10:{i:02d}", "oi": 1, "delta_oi": None, "price": 100.0 + i}
                for i in range(6)]
    assert _flow(kind, no_delta)["oi_direction"] == "no baseline"
    assert _flow(kind, no_delta)["what_label"] == FLOW_NOT_ENOUGH


def test_an_unknown_instrument_type_is_never_read_as_a_call():
    assert _flow("FUT", _points(BUILD, UP))["what_label"] == FLOW_NOT_ENOUGH


def test_the_price_flat_band_is_five_percent_of_the_contracts_own_move():
    at = lambda prices: [{"at": f"{SESSION} 10:{i:02d}", "oi": 1, "delta_oi": i, "price": p}
                         for i, p in enumerate(prices)]
    assert PRICE_FLAT_FRACTION == FLAT_FRACTION == 0.05
    # first 100, peak 200 -> the band is 5; +4 over the hour is inside it, +6 is outside
    assert _price_direction(at([100, 200, 200, 200, 200, 204]))[0] == "flat"
    assert _price_direction(at([100, 200, 200, 200, 200, 206]))[0] == "up"
    assert _price_direction(at([100, 200, 200, 200, 200, 194]))[0] == "down"
    assert _price_direction(at([120, 120]))[0] == "flat"
    label, detail = _price_direction(at([100, 120, 140, 160, 180, 200]))
    assert label == "up" and detail["readings_back"] == DIRECTION_LOOKBACK_MARKS
    assert detail["price_change"] == 80
    assert detail["price_flat_threshold"] == pytest.approx(PRICE_FLAT_FRACTION * 100)
    # a mark with no price is skipped by the rule, exactly as it is skipped by the line
    assert _price_direction(at([100, 110, None, None, 160]))[0] == "up"
    assert _price_direction(at([100]))[0] == "no baseline"
    assert _price_direction([])[0] == "no baseline"


def test_the_served_contracts_carry_their_flow(store):
    _populate(store, oi_by_mark=lambda i: 1000 + i * 1000,
              price_by_mark=lambda i: 100.0 + i * 10)
    out = strike_oi_series(store, "NIFTY")
    ce = next(c for c in out["contracts"] if c["atm_offset"] == 0 and c["option_type"] == "CE")
    pe = next(c for c in out["contracts"] if c["atm_offset"] == 0 and c["option_type"] == "PE")
    assert ce["flow"]["what_label"] == "Call buying increasing"
    assert pe["flow"]["what_label"] == "Put buying increasing"
    detail = ce["flow"]["detail"]
    assert detail["readings_back"] == DIRECTION_LOOKBACK_MARKS
    assert detail["oi_change"] == 4000 and detail["price_change"] == 40.0
    assert detail["from"] == MARKS[1] and detail["to"] == MARKS[-1]
    # nothing in any served sentence predicts, and none of them says "mark" to the reader
    for c in out["contracts"]:
        for text in (c["flow"]["what_label"], c["flow"]["meaning"] or ""):
            assert "mark" not in text.lower()
            for word in ("bullish", "bearish", "will ", "forecast", "predict", "target"):
                assert word not in text.lower()
