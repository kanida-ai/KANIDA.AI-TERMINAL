"""Adjustment-basis mixing: the detector must not fire on a label difference.

After the repair pass almost every symbol in `db/market15.db` carries both
`legacy_unknown` and `kite-eod-adjusted` rows, because the repair re-fetched
disputed windows and stamped them with the vendor's current basis id.  For 463
of 513 symbols the re-fetched values agreed with the legacy ones bar for bar,
so those labels describe the same basis and there is nothing to repair.  A
detector that keyed on the label would have ordered a full-history re-fetch of
the whole universe; one that keys on the measured level orders seven.

These tests pin that distinction, plus the two ways the measurement can fail
honestly (no overlap to measure, and a difference that is not one ratio).
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_data import basis  # noqa: E402
from market_data.aggregate import Bar  # noqa: E402
from market_data.store import MarketStore  # noqa: E402

INSTRUMENT = 700123
SYMBOL = "TESTCO"
VENDOR_BASIS = "kite-eod-adjusted"


def bars(n=40, start="2024-01-02 09:15:00", base=100.0, scale=1.0):
    t0 = datetime.fromisoformat(start)
    out = []
    for i in range(n):
        s = t0 + timedelta(minutes=15 * i)
        p = (base + i) * scale
        out.append(Bar(s, s + timedelta(minutes=15), p, p * 1.01, p * 0.99,
                       p * 1.005, 1000 + i))
    return out


@pytest.fixture()
def store(tmp_path):
    st = MarketStore(tmp_path / "market15.db")
    yield st
    st.close()


def seed_legacy(store, n=40, scale=1.0):
    store.upsert_candles(SYMBOL, INSTRUMENT, bars(n, scale=scale), vendor_id="legacy",
                         adjustment_basis_id=basis.LEGACY_BASIS, revision=1)


def refetch(store, window, *, scale=1.0, revision=2):
    """Re-fetch `window` (a slice of the same bars) on the vendor's basis."""
    store.upsert_candles(SYMBOL, INSTRUMENT, [bars(40, scale=scale)[i] for i in window],
                         vendor_id="kite", adjustment_basis_id=VENDOR_BASIS,
                         revision=revision)


# ---------------------------------------------------------------------------
# the distinction the whole module exists for
# ---------------------------------------------------------------------------
def test_same_prices_under_two_labels_is_not_a_mixed_basis(store):
    """463 of 513 real symbols look like this.  None of them needs a re-fetch."""
    seed_legacy(store)
    refetch(store, range(10, 20))            # identical values, new label
    e = basis.evidence_for(store, SYMBOL)
    assert e.verdict == basis.VERDICT_LABEL_ONLY
    assert not e.mixed
    assert e.legacy_rows == 30 and e.vendor_rows == 10
    assert e.overlap_bars == 10 and e.shifted_bars == 0


def test_a_constant_level_shift_in_the_overlap_is_a_mixed_basis(store):
    seed_legacy(store)
    refetch(store, range(10, 20), scale=1 / 1.0343)   # vendor sits 3.43% lower
    e = basis.evidence_for(store, SYMBOL)
    assert e.verdict == basis.VERDICT_MIXED and e.mixed
    assert e.shifted_share == 1.0
    assert e.median_ratio == pytest.approx(1.0343, rel=1e-4)
    # the 30 bars still resolving to the legacy revision are the landmine
    assert e.legacy_rows == 30
    assert "return computed across that boundary is wrong" in e.reason


def test_a_single_bad_print_does_not_veto_a_real_shift(store):
    """ZEEL: 5004 of 5005 shifted bars agree on 1.023892, one prints 1.156.

    A min/max spread rule called that "no single ratio" and refused to repair a
    symbol whose whole history is demonstrably off by 2.39%.
    """
    seed_legacy(store, n=400)
    shifted = bars(400, scale=1 / 1.0239)[100:300]
    odd = shifted[42]
    shifted[42] = Bar(odd.bar_start, odd.bar_end, odd.open / 1.13, odd.high / 1.13,
                      odd.low / 1.13, odd.close / 1.13, odd.volume)
    store.upsert_candles(SYMBOL, INSTRUMENT, shifted, vendor_id="kite",
                         adjustment_basis_id=VENDOR_BASIS, revision=2)
    e = basis.evidence_for(store, SYMBOL)
    assert e.verdict == basis.VERDICT_MIXED
    assert e.off_ratio_bars == 1
    assert e.ratio_concentration == pytest.approx(199 / 200)
    assert e.median_ratio == pytest.approx(1.0239, rel=1e-4)


def test_scattered_disagreement_is_inconclusive_not_a_rebasing_job(store):
    """EMAMILTD-shaped: bars differ, but by no one ratio.  We do not guess."""
    seed_legacy(store)
    noisy = []
    for i, b in enumerate(bars(40)[10:20]):
        k = 1 + 0.05 * (1 if i % 2 else -1) * (1 + i / 10)
        noisy.append(Bar(b.bar_start, b.bar_end, b.open * k, b.high * k,
                         b.low * k, b.close * k, b.volume))
    store.upsert_candles(SYMBOL, INSTRUMENT, noisy, vendor_id="kite",
                         adjustment_basis_id=VENDOR_BASIS, revision=2)
    e = basis.evidence_for(store, SYMBOL)
    assert e.verdict == basis.VERDICT_INCONCLUSIVE
    assert not e.mixed


# ---------------------------------------------------------------------------
# the two single-basis outcomes
# ---------------------------------------------------------------------------
def test_a_fully_refetched_symbol_reads_unified(store):
    """GUJENERGY/JMA: named in the repair report, already fixed by its own
    full-history fetch.  Re-fetching them again would be work for nothing."""
    seed_legacy(store)
    refetch(store, range(40), scale=1 / 1.0348)
    e = basis.evidence_for(store, SYMBOL)
    assert e.verdict == basis.VERDICT_UNIFIED and not e.mixed
    assert e.legacy_rows == 0


def test_a_symbol_nothing_was_refetched_for_is_legacy_only(store):
    """HEG/HFCL/JBCHEPHARM: quarantined, so no vendor rows exist to compare."""
    seed_legacy(store)
    e = basis.evidence_for(store, SYMBOL)
    assert e.verdict == basis.VERDICT_LEGACY_ONLY and not e.mixed
    assert e.overlap_bars == 0
    assert "consistent" in e.reason


def test_two_labels_with_nothing_held_on_both_is_inconclusive(store):
    """Disjoint windows: the store cannot say how the levels relate."""
    store.upsert_candles(SYMBOL, INSTRUMENT, bars(40)[:20], vendor_id="legacy",
                         adjustment_basis_id=basis.LEGACY_BASIS, revision=1)
    store.upsert_candles(SYMBOL, INSTRUMENT, bars(40)[20:], vendor_id="kite",
                         adjustment_basis_id=VENDOR_BASIS, revision=1)
    e = basis.evidence_for(store, SYMBOL)
    assert e.verdict == basis.VERDICT_INCONCLUSIVE
    assert e.overlap_bars == 0 and e.legacy_rows == 20 and e.vendor_rows == 20


# ---------------------------------------------------------------------------
# latest-revision semantics
# ---------------------------------------------------------------------------
def test_only_the_latest_revision_counts(store):
    """An old revision is history, not what a reader of the store gets."""
    seed_legacy(store)
    refetch(store, range(40), scale=1 / 1.05, revision=2)
    refetch(store, range(40), scale=1 / 1.05, revision=3)
    assert basis.latest_revision_bases(store, SYMBOL) == {
        VENDOR_BASIS: {"rows": 40, "first_bar": "2024-01-02 09:15:00",
                       "last_bar": "2024-01-02 19:00:00"}}


def test_scan_and_mixed_symbols_agree(store):
    seed_legacy(store)
    refetch(store, range(10, 20), scale=1 / 1.04)
    assert [e.symbol for e in basis.mixed_symbols(store)] == [SYMBOL]
    assert [e.verdict for e in basis.scan(store)] == [basis.VERDICT_MIXED]


# ---------------------------------------------------------------------------
# the pure classifier
# ---------------------------------------------------------------------------
def test_classify_is_pure_and_needs_no_store():
    bases = {basis.LEGACY_BASIS: {"rows": 100, "first_bar": "a", "last_bar": "b"},
             VENDOR_BASIS: {"rows": 20, "first_bar": "c", "last_bar": "d"}}
    ratios = [(f"bar{i}", 1.0343) for i in range(20)]
    assert basis.classify("X", bases, ratios).verdict == basis.VERDICT_MIXED
    assert basis.classify("X", bases, [(f"bar{i}", 1.0) for i in range(20)]
                          ).verdict == basis.VERDICT_LABEL_ONLY
