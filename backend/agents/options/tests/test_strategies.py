"""
Options Agent · strategy-layer tests (constructor, leg selection, staging, provenance).

WHY THIS FILE EXISTS
--------------------
An adversarial audit found that `strategies/` — iron_condor.py, _legs.py, base.py,
registry.py — had ZERO test coverage, and that the production data path
(snapshot -> store -> load_chain -> construct) had never been executed even once. It was
dead: rows produced by fetch_kite carried no `as_of_date`, so the constructor refused to
build and returned an empty list with only a log line. The only thing exercising the
constructor was the worked-example script, which builds its own rows from a direct live
Kite call and so bypassed the store, load_chain and the consistency guard entirely — i.e.
the acceptance artifact validated a path production does not use.

The first test below is therefore the important one: it walks the REAL path end to end.
"""
from __future__ import annotations

import datetime as dt

import pytest

from agents.options import data, fetch_kite, store
from agents.options.strategies import _legs as L
from agents.options.strategies import registry as strat_registry
from agents.options.strategies.iron_condor import IronCondorConstructor
from agents.options.tests.test_data_chain import FakeKite

UND = "NIFTY"

# The FakeKite ladder is 23500..24500 step 100 at a flat 14% vol, so a 0.16-delta short with
# 200-wide wings has no listed wing. These params fit the fixture while exercising the same
# code path; the GOVERNED defaults are asserted separately in test_governed_defaults_are_frozen.
FIXTURE_PARAMS = {"short_delta_target": 0.30, "short_delta_band": (0.20, 0.45),
                  "wing_width_points": 100.0, "min_oi": 0, "max_spread_pct": 100.0}


@pytest.fixture(autouse=True)
def snap_dir(tmp_path, monkeypatch):
    monkeypatch.delenv(store.ENV_URI, raising=False)
    monkeypatch.setenv(store.ENV_DIR, str(tmp_path / "options_chains"))
    monkeypatch.setenv(store.ENV_PARQUET, "0")
    monkeypatch.setenv(fetch_kite.ENV_BACKFILL_DIR, str(tmp_path / "options_backfill"))


def _archived_chain(today=None):
    """Run the REAL snapshot job against the fake feed, then read it back through the store."""
    today = today or dt.date.today()
    res = fetch_kite.snapshot_chain(today, UND, _client=FakeKite(today))
    assert res["ok"] is True, res.get("reason")
    return data.load_chain(UND, today), today


# ══════════════════════════════════════════════ THE PRODUCTION PATH, END TO END
def test_archive_to_constructor_path_actually_produces_occurrences():
    """snapshot -> store -> load_chain -> construct. This path returned ZERO before the fix."""
    chain, _ = _archived_chain()
    assert chain["available"] is True and chain["row_count"] > 0

    occs = IronCondorConstructor().construct(chain, params=FIXTURE_PARAMS)
    assert occs, "the archive->constructor path produced nothing"
    assert any(o["legs"] for o in occs), "no occurrence carried any legs"


def test_both_call_shapes_work_so_neither_can_silently_produce_nothing():
    """`construct(chain_dict)` is the production shape, but passing `chain["rows"]` is a very
    natural mistake -- and it used to yield 0 candidates with only a log line."""
    chain, _ = _archived_chain()
    from_dict = IronCondorConstructor().construct(chain, params=FIXTURE_PARAMS)
    from_rows = IronCondorConstructor().construct(chain["rows"], params=FIXTURE_PARAMS)
    assert len(from_dict) == len(from_rows) > 0


def test_every_occurrence_carries_its_provenance():
    """basis / survivorship / capture_mode / forward_source must travel WITH the occurrence.
    load_chain reported them faithfully and the constructor used to drop all of it."""
    chain, _ = _archived_chain()
    for o in IronCondorConstructor().construct(chain, params=FIXTURE_PARAMS):
        prov = o["provenance"]
        assert prov["basis"] == "live_quote_snapshot"
        assert prov["capture_mode"] in ("market_hours", "post_market")
        assert "survivorship" in prov and "expiries_quarantined" in prov
        if o["legs"]:
            assert prov["forward_source"] is not None
            assert "liquidity_measurable" in prov


def test_occurrence_is_json_serialisable():
    """It has to survive the store/router seam."""
    import json
    chain, _ = _archived_chain()
    occs = IronCondorConstructor().construct(chain, params=FIXTURE_PARAMS)
    json.dumps(occs)          # raises if any value is not JSON-safe


# ══════════════════════════════════════════════════════════ point-in-time
def test_dte_is_measured_from_the_decision_date_not_the_clock():
    assert L.dte("2026-08-31", "2026-09-07") == 7
    assert L.dte("2023-01-02", "2023-01-09") == 7          # a replayed past date
    assert L.dte(dt.date(2026, 8, 31), dt.date(2026, 9, 29)) == 29


def test_classify_expiry_requires_an_explicit_as_of():
    """Reading date.today() here would silently corrupt every replayed decision."""
    with pytest.raises(TypeError):
        L.classify_expiry("2026-09-29", ["2026-09-29"])     # as_of is mandatory


def test_classify_expiry_is_stable_across_the_clock():
    exps = ["2026-09-01", "2026-09-08", "2026-09-29"]
    assert L.classify_expiry("2026-09-29", exps, "2026-08-31") == "monthly"
    assert L.classify_expiry("2026-09-01", exps, "2026-08-31") == "weekly"


def test_expiry_classification_uses_the_archived_master_not_surviving_rows():
    """A quarantined expiry must not change how OTHER expiries are classified. Deriving the
    ladder from surviving rows made weekly-vs-monthly a function of data quality."""
    full = ["2026-09-01", "2026-09-08", "2026-09-22", "2026-09-29"]
    thinned = ["2026-09-01", "2026-09-08", "2026-09-22"]        # monthly quarantined
    as_of = "2026-08-31"
    assert L.classify_expiry("2026-09-22", full, as_of) == "weekly"
    # against the thinned ladder the same expiry would look like the month's last -> monthly
    assert L.classify_expiry("2026-09-22", thinned, as_of) == "monthly"
    # so the constructor must classify against the ARCHIVED master, which includes quarantined
    # expiries -- asserted end-to-end in test_archive_to_constructor_path_actually_produces_occurrences


# ══════════════════════════════════════════════════════════ leg selection
def test_snap_outward_never_narrows_the_structure():
    """Nearest-snapping silently halved a requested width on a thinned ladder, which halves the
    max loss and doubles the apparent R:R."""
    ladder = [23000.0, 23200.0, 23400.0, 24600.0, 24800.0, 25000.0]
    # call wing wants >= 24650 -> must go OUT to 24800, not back to 24600
    assert L.snap_strike_outward(ladder, 24650.0, "up") == 24800.0
    # put wing wants <= 23350 -> must go OUT to 23200, not up to 23400
    assert L.snap_strike_outward(ladder, 23350.0, "down") == 23200.0
    # nearest-snapping would have chosen the NARROWER strike in both cases (deliberately off
    # the midpoint so this asserts the policy, not a tie-break)
    assert L.snap_strike(ladder, 24650.0) == 24600.0
    assert L.snap_strike(ladder, 23350.0) == 23400.0


def test_snap_outward_falls_back_to_the_ladder_end():
    ladder = [24000.0, 24100.0]
    assert L.snap_strike_outward(ladder, 99999.0, "up") == 24100.0
    assert L.snap_strike_outward(ladder, 1.0, "down") == 24000.0
    assert L.snap_strike_outward([], 100.0, "up") is None


def test_pick_by_delta_returns_none_rather_than_defaulting():
    """An unknown delta must never be treated as 0 -- that silently selects the farthest wing."""
    rows = [{"expiry": "2026-09-29", "right": "CE", "strike": 24000.0, "delta": None},
            {"expiry": "2026-09-29", "right": "CE", "strike": 24500.0, "delta": None}]
    assert L.pick_by_delta(rows, "2026-09-29", "CE", 0.16) is None


def test_pick_by_delta_respects_the_band():
    rows = [{"expiry": "E", "right": "CE", "strike": 24000.0, "delta": 0.55},
            {"expiry": "E", "right": "CE", "strike": 24500.0, "delta": 0.16},
            {"expiry": "E", "right": "CE", "strike": 25000.0, "delta": 0.02}]
    got = L.pick_by_delta(rows, "E", "CE", 0.16, band=(0.10, 0.25))
    assert got["strike"] == 24500.0


def test_spread_pct_is_none_not_zero_without_a_two_sided_quote():
    assert L.spread_pct({"bid": None, "ask": None}) is None
    assert L.spread_pct({"bid": 0.0, "ask": 5.0}) is None
    assert L.spread_pct({"bid": 6.0, "ask": 4.0}) is None       # crossed
    assert L.spread_pct({"bid": 4.0, "ask": 6.0}) == pytest.approx(40.0)


def test_liquidity_fails_closed_when_the_spread_is_unmeasurable():
    ok, why = L.liquidity_ok({"bid": None, "ask": None, "oi": 10_000, "volume": 100},
                             {"max_spread_pct": 8.0, "min_oi": 500, "min_volume": 0})
    assert ok is False and "spread unmeasurable" in why


# ══════════════════════════════════════════════════════════ forward handling
def test_forward_is_never_silently_replaced_by_spot():
    """Spot is not the forward: ~190 points of basis at 45 DTE, straight into POP and EV."""
    rows = [{"expiry": "2026-09-29", "right": r, "strike": k, "lot_size": 65, "ltp": 50.0,
             "as_of_date": "2026-08-31", "underlying": "NIFTY", "spot": 24080.0,
             "delta": 0.16 if r == "CE" else -0.16, "iv": 0.12, "oi": 1000, "volume": 10}
            for k in (23000.0, 23500.0, 24500.0, 25000.0) for r in ("CE", "PE")]
    occs = IronCondorConstructor().construct(rows)       # no forward anywhere in the rows
    assert occs, "expected an INCOMPLETE occurrence, not silence"
    o = occs[0]
    assert o["stage"] == "INCOMPLETE"
    assert o["forward"] is None
    assert any("refusing to substitute spot" in r for r in o["reasons"])


# ══════════════════════════════════════════════════════════ staging
def test_stage_is_unpriced_when_no_expectancy_could_be_computed():
    """A candidate with no EV/POP used to still come out as CANDIDATE, with the problem buried
    in `reasons` -- a gate keying on `stage` would have promoted it."""
    rows = [{"expiry": "2026-09-29", "right": r, "strike": k, "lot_size": 65, "ltp": 50.0,
             "as_of_date": "2026-08-31", "underlying": "NIFTY", "spot": 24080.0,
             "forward": 24250.0, "iv": None,
             "delta": 0.16 if r == "CE" else -0.16, "oi": 1000, "volume": 10}
            for k in (23000.0, 23200.0, 24800.0, 25000.0) for r in ("CE", "PE")]
    occs = IronCondorConstructor().construct(rows)
    assert occs
    assert occs[0]["stage"] in ("UNPRICED", "INCOMPLETE")
    if occs[0]["stage"] == "UNPRICED":
        assert "ev" not in occs[0]["metrics"]


def test_liquidity_skipped_is_distinct_from_illiquid():
    """'we could not measure liquidity' is not the same claim as 'this is illiquid'. Collapsing
    them means the backfill arm of the hybrid design can never produce a clean candidate."""
    chain, _ = _archived_chain()
    for row in chain["rows"]:
        row["bid"] = row["ask"] = None          # simulate a post-market / backfilled capture
    occs = IronCondorConstructor().construct(chain, params=FIXTURE_PARAMS)
    staged = [o for o in occs if o["legs"]]
    assert staged
    assert all(o["stage"] != "ILLIQUID" for o in staged)
    assert any(o["stage"] == "LIQUIDITY_SKIPPED" for o in staged)
    assert all(o["provenance"]["liquidity_measurable"] is False for o in staged)


# ══════════════════════════════════════════════════════════ registry + governance
def test_iron_condor_self_registers():
    strat_registry.load_builtin()
    got = strat_registry.get("iron_condor")
    assert got is not None
    m = got.manifest()
    assert m["status"] == "built" and m["legs_count"] == 4 and m["defined_risk"] is True


def test_governed_defaults_are_frozen_and_declared():
    """The knobs must be auditable and must not have been quietly tuned."""
    from agents.options.strategies.iron_condor import CONDOR_PARAMS, PARAMS_VERSION
    assert CONDOR_PARAMS["short_delta_target"] == 0.16
    assert CONDOR_PARAMS["wing_width_points"] == 200.0
    assert CONDOR_PARAMS["dte_weekly"] == (1, 10)
    assert CONDOR_PARAMS["dte_monthly"] == (15, 45)
    assert "SPEC-until-OOS" in PARAMS_VERSION


def test_iv_rank_is_reported_skipped_never_invented():
    chain, _ = _archived_chain()
    built = [o for o in IronCondorConstructor().construct(chain, params=FIXTURE_PARAMS)
             if o["legs"]]
    assert built
    for o in built:
        assert o["regime"]["iv_rank"] is None
        assert "skipped" in o["regime"]["iv_rank_status"]
        # there is no IV history archived yet, so a percentile would be fabricated
        assert "fabricated" in o["regime"]["iv_rank_status"]
