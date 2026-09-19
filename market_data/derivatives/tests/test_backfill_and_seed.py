"""Backfill (10 sessions of 15-minute candles with OI) and the candle-seeded marks."""
from datetime import date, datetime, timedelta

import pytest

from market_data.derivatives import config
from market_data.derivatives.backfill import (
    Backfiller,
    Candidate,
    run_backfill,
    select_candidates,
)
from market_data.derivatives.capture import DerivativesCapture, SessionDays
from market_data.derivatives.fake_nfo import FakeNFOClient, FakeNFOProvider
from market_data.derivatives.seed_from_candles import SOURCE, seed_session
from market_data.derivatives.store import DerivativesStore

TODAY = date(2026, 9, 18)


@pytest.fixture
def cap(tmp_path):
    store = DerivativesStore(tmp_path / "derivatives.db")
    provider = FakeNFOProvider(FakeNFOClient(today=TODAY))
    capture = DerivativesCapture(store, provider, cache_dir=tmp_path / "cache")
    capture.ensure_scope(TODAY)
    return capture


# ── the liquidity screen ─────────────────────────────────────────────────────

def test_every_future_is_backfilled_and_illiquid_options_are_not(cap):
    scope = cap.scope
    liquidity = {c.instrument_token: (0, 0.0) for c in scope.contracts}
    liquid = next(c for c in scope.contracts if c.instrument_type == "CE")
    liquidity[liquid.instrument_token] = (10_000, 55.0)
    picked = select_candidates(scope, liquidity)
    tokens = {c.token for c in picked}
    assert all(f.instrument_token in tokens
               for f in scope.contracts if f.instrument_type == "FUT")
    assert liquid.instrument_token in tokens
    assert len(tokens) == len([c for c in scope.contracts if c.instrument_type == "FUT"]) + 1


def test_a_budget_keeps_the_most_liquid(cap):
    scope = cap.scope
    liquidity = {c.instrument_token: (i * 100, 10.0)
                 for i, c in enumerate(scope.contracts)}
    picked = select_candidates(scope, liquidity, max_contracts=5)
    assert len(picked) == 5
    options = [c for c in picked if c.contract.instrument_type != "FUT"]
    assert options == sorted(options, key=lambda c: -c.oi)


# ── the fetch ────────────────────────────────────────────────────────────────

def test_backfill_asks_for_open_interest_and_stores_per_bar_volume(cap):
    res = run_backfill(cap, workers=2)
    assert res.rows > 0 and res.errors == 0
    assert cap.provider.kite.calls["historical_data"] == res.attempted
    row = cap.store.con.execute(
        "SELECT * FROM candles_15m ORDER BY instrument_token, bar_start").fetchone()
    assert row["oi"] is not None
    assert row["vendor_id"] == "kite" and row["fetched_at"]
    # per-bar volume, not the running day total (contract with D2)
    vols = [r[0] for r in cap.store.con.execute(
        "SELECT volume FROM candles_15m WHERE instrument_token=? "
        "AND substr(bar_start,1,10)=substr((SELECT MIN(bar_start) FROM candles_15m),1,10)"
        " ORDER BY bar_start", (row["instrument_token"],))]
    assert len(vols) > 1 and vols != sorted(vols)  # a cumulative series would be sorted


def test_backfill_is_resumable(cap):
    first = run_backfill(cap, workers=2)
    calls = cap.provider.kite.calls["historical_data"]
    second = run_backfill(cap, workers=2)
    assert second.skipped_done == first.candidates
    assert second.attempted == 0
    assert cap.provider.kite.calls["historical_data"] == calls   # nothing re-fetched


def test_a_failed_contract_is_recorded_and_retried_next_run(cap):
    bad = cap.scope.contracts[0].instrument_token
    real = cap.provider.kite.historical_data

    def flaky(token, *a, **kw):
        if int(token) == bad:
            raise RuntimeError("vendor said no")
        return real(token, *a, **kw)

    cap.provider.kite.historical_data = flaky
    res = run_backfill(cap, workers=1)
    assert res.errors == 1
    row = cap.store.con.execute(
        "SELECT * FROM backfill_progress WHERE instrument_token=?", (bad,)).fetchone()
    assert row["status"] == "error" and "vendor said no" in row["error"]

    cap.provider.kite.historical_data = real
    again = run_backfill(cap, workers=1)
    assert again.attempted == 1 and again.errors == 0


def test_the_fetch_thread_never_touches_the_database(cap):
    """The first backfill died writing SQLite from worker threads — never again."""
    import inspect

    src = inspect.getsource(Backfiller.fetch_rows)
    assert "self.store" not in src


# ── seeding a past session from candles ──────────────────────────────────────

def test_seeded_marks_are_labelled_as_candles_not_quotes(cap):
    run_backfill(cap, workers=2)
    session = TODAY - timedelta(days=1)
    out = seed_session(cap.store, cap.provider, session=session)
    assert out["snapshot_rows"] > 0
    sources = {r[0] for r in cap.store.con.execute("SELECT DISTINCT source FROM snapshots")}
    assert sources == {SOURCE}
    row = cap.store.read_snapshots()[0]
    # a candle carries no book and no average price — and we do not invent them
    assert row["average_price"] is None and row["bid"] is None and row["ask"] is None
    assert row["last_price"] is not None and row["oi"] is not None


def test_seeded_volume_is_cumulative_through_the_session(cap):
    run_backfill(cap, workers=2)
    session = TODAY - timedelta(days=1)
    seed_session(cap.store, cap.provider, session=session)
    token = cap.store.read_snapshots()[0]["instrument_token"]
    vols = [r["volume"] for r in cap.store.read_snapshots(token=token)]
    assert vols == sorted(vols)
    bars = [r[0] for r in cap.store.con.execute(
        "SELECT volume FROM candles_15m WHERE instrument_token=? "
        "AND substr(bar_start,1,10)=? ORDER BY bar_start", (token, session.isoformat()))]
    assert vols[-1] == sum(bars)


def test_a_live_capture_overwrites_a_seeded_mark(cap):
    run_backfill(cap, workers=2)
    session = TODAY - timedelta(days=1)
    seed_session(cap.store, cap.provider, session=session)
    mark = datetime.combine(session, config.FIRST_MARK)
    res = cap.run_once(mark, now=mark + timedelta(seconds=30), force=True)
    assert res.status == "ok"
    sources = {r[0] for r in cap.store.con.execute(
        "SELECT DISTINCT source FROM snapshots WHERE captured_at=?",
        (mark.strftime("%Y-%m-%d %H:%M:%S"),))}
    assert sources == {"kite.quote"}
