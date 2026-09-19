"""Quarantine: skipping a symbol the provider cannot serve, without hiding it.

Six NIFTY 500 names are in `instrument_labels` but not in the Kite NSE
instrument list, so every live cycle raised one `ProviderError` each and
finished with `errors=6`.  `/api/state.data_status.last_run.errors` publishes
that number and the app turns it into "some symbols may be behind" -- a warning
about the loop, not about the data.

The tests below pin the four properties that make the skip safe:

* it is driven by what the **provider** answers, never by a constant list;
* the decision is persisted with `first_seen` / `last_checked` and is also
  written to `corrections` as `field='quarantine_status'`, the same mechanism
  the repair pass used -- one audit trail, not two;
* a quarantined symbol is re-probed **once a day** and released automatically;
* a failed re-probe is recorded, never counted as a cycle error.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_data import quarantine as q  # noqa: E402
from market_data.store import MarketStore  # noqa: E402
from market_data.types import Instrument, ProviderError  # noqa: E402

GONE = "LTIM"
HERE = "RELIANCE"


class StubProvider:
    """Answers the only question :func:`quarantine.probe` asks."""

    provider_id = "stub"

    def __init__(self, known=(HERE,)):
        self.known = set(known)
        self.resolved = []

    def resolve(self, symbol):
        self.resolved.append(symbol)
        if symbol not in self.known:
            raise ProviderError(
                f"symbol {symbol!r} is not in the Kite NSE instrument list "
                f"(invisible to this account, delisted, or renamed)",
                provider_id="stub")
        return Instrument(instrument_id="700001", symbol=symbol, exchange="NSE",
                          vendor_id="stub")


@pytest.fixture()
def store(tmp_path):
    st = MarketStore(tmp_path / "market15.db")
    yield st
    st.close()


# ---------------------------------------------------------------------------
# probing
# ---------------------------------------------------------------------------
def test_probe_reports_what_the_provider_answered():
    p = StubProvider()
    assert q.probe(p, HERE).available is True
    bad = q.probe(p, GONE)
    assert bad.available is False
    assert "not in the Kite NSE instrument list" in bad.error


def test_probe_uses_the_instrument_list_not_a_candle_request():
    """It costs no historical-data quota and cannot be confused with
    "listed, but no bars in the window you asked for"."""
    p = StubProvider()
    q.probe(p, GONE)
    assert p.resolved == [GONE]


def test_a_provider_without_an_instrument_list_quarantines_nothing():
    class NoList:
        provider_id = "nolist"

    assert q.probe(NoList(), GONE).available is True


# ---------------------------------------------------------------------------
# persistence + the single audit trail
# ---------------------------------------------------------------------------
def test_sync_quarantines_only_what_the_provider_cannot_serve(store):
    out = q.sync(store, StubProvider(), [HERE, GONE])
    assert out["quarantined"] == [GONE] and out["ok"] == [HERE]
    assert set(store.quarantined()) == {GONE}
    row = store.get_quarantine(GONE)
    assert row["reason"] == q.NOT_IN_INSTRUMENT_LIST
    assert row["first_seen"] and row["last_checked"] and row["provider"] == "stub"


def test_the_decision_is_also_a_corrections_row(store):
    """`field='quarantine_status'` is exactly what
    `market_data/repair/reconcile.py::flag_wrong_instrument` wrote."""
    q.sync(store, StubProvider(), [GONE])
    rows = store.con.execute(
        "SELECT symbol,old_value,new_value FROM corrections "
        "WHERE field='quarantine_status'").fetchall()
    assert [tuple(r) for r in rows] == [
        (GONE, "active", f"quarantined:{q.NOT_IN_INSTRUMENT_LIST}")]


def test_requarantining_keeps_first_seen_and_advances_last_checked(store):
    store.quarantine(GONE, reason=q.NOT_IN_INSTRUMENT_LIST,
                     when="2026-09-01 00:00:00")
    store.quarantine(GONE, reason=q.NOT_IN_INSTRUMENT_LIST,
                     when="2026-09-05 00:00:00")
    row = store.get_quarantine(GONE)
    assert row["first_seen"] == "2026-09-01 00:00:00"
    assert row["last_checked"] == "2026-09-05 00:00:00"
    assert row["checks"] == 2
    # the second call is a re-check, not a second decision
    assert store.con.execute(
        "SELECT COUNT(*) FROM corrections WHERE field='quarantine_status'"
    ).fetchone()[0] == 1


def test_release_is_recorded_and_lets_the_symbol_back_in(store):
    p = StubProvider()
    q.sync(store, p, [GONE])
    p.known.add(GONE)                      # the vendor lists it again
    out = q.sync(store, p, [GONE])
    assert out["released"] == [GONE]
    assert store.quarantined() == {}
    assert store.get_quarantine(GONE)["status"] == "released"
    assert store.con.execute(
        "SELECT new_value FROM corrections WHERE field='quarantine_status' "
        "ORDER BY id DESC LIMIT 1").fetchone()[0] == "active"


def test_a_quarantined_symbol_keeps_every_bar_it_already_had(store):
    """Quarantine excludes a symbol from *fetching*, never from existing."""
    from market_data.aggregate import Bar

    t = datetime(2026, 7, 29, 9, 15)
    store.upsert_candles(GONE, 700009, [Bar(t, t + timedelta(minutes=15),
                                            100, 101, 99, 100.5, 10)],
                         vendor_id="legacy", adjustment_basis_id="legacy_unknown")
    q.sync(store, StubProvider(), [GONE])
    assert len(store.read_window(GONE)) == 1


# ---------------------------------------------------------------------------
# the daily re-check
# ---------------------------------------------------------------------------
def test_recheck_is_due_only_after_the_window(store):
    now = datetime(2026, 9, 16, 12, 0)
    row = {"status": "quarantined", "last_checked": "2026-09-16 01:00:00"}
    assert q.due_for_recheck(row, now=now, hours=24) is False
    assert q.due_for_recheck(row, now=now, hours=4) is True
    assert q.due_for_recheck({**row, "last_checked": None}, now=now) is True


def test_a_released_row_is_never_due(store):
    assert q.due_for_recheck({"status": "released",
                              "last_checked": "2020-01-01 00:00:00"}) is False


def test_due_symbols_selects_the_stale_ones():
    now = datetime(2026, 9, 16, 12, 0)
    rows = {"A": {"status": "quarantined", "last_checked": "2026-09-15 11:00:00"},
            "B": {"status": "quarantined", "last_checked": "2026-09-16 11:00:00"}}
    assert q.due_symbols(rows, now=now, hours=24) == {"A"}


def test_summarise_gives_the_operator_the_four_facts(store):
    q.sync(store, StubProvider(), [GONE])
    (row,) = q.summarise(store)
    assert set(row) >= {"symbol", "reason", "first_seen", "last_checked"}
    assert row["symbol"] == GONE
