"""The spot a rebuilt session was never given, and the provenance that says where it came from.

What is under test is one rule and one refusal.

The rule: the spot written against a 15-minute reading is that reading's OWN bar
close, taken from ``db/market15.db`` by ``bar_end``.  The refusal: when that bar
is not there, nothing is written.  No carry-forward, no interpolation, no
nearest-bar — the closing auction of an F&O underlying is exactly the case where
a neighbouring bar would look plausible and be wrong.

And everything written says where it came from, because a spot rebuilt from our
own equity store after the fact is not the same fact as a spot the F&O vendor
quoted at that instant, and a reader has to be able to tell them apart.
"""
from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from market_data.derivatives import config, spot_backfill as SB
from market_data.derivatives.store import DerivativesStore

SESSION = date(2026, 9, 18)
DAY = SESSION.isoformat()
#: two readings our equity store has a bar for, and one it does not (the auction)
LIVE_MARK = f"{DAY} 11:30:00"
REBUILT_MARKS = [f"{DAY} 11:45:00", f"{DAY} 12:00:00"]
AUCTION_MARK = f"{DAY} 15:30:00"


@pytest.fixture
def store(tmp_path):
    """A derivatives store shaped like the real one after the 18 Sep outage."""
    s = DerivativesStore(tmp_path / "derivatives.db")
    con = s.con
    con.execute(
        "INSERT INTO contracts(instrument_token,tradingsymbol,underlying,instrument_type,strike,"
        "expiry,lot_size,first_seen,last_seen,in_scope,vendor_id,fetched_at)"
        " VALUES(1,'RELIANCE26SEP1400CE','RELIANCE','CE',1400.0,'2026-09-29',500,?,?,1,'kite',?)",
        (DAY, DAY, DAY),
    )
    # the live reading: a real quote, spot and all
    con.execute(
        "INSERT INTO underlying_snapshots(underlying,captured_at,mark_kind,spot,spot_symbol,"
        "vendor_id,fetched_at,snapshot_id) VALUES('RELIANCE',?,'bar_close',1408.0,'RELIANCE','kite',?,'s1')",
        (LIVE_MARK, DAY),
    )
    con.execute(
        "INSERT INTO snapshots(instrument_token,captured_at,mark_kind,last_price,average_price,"
        "volume,oi,source,vendor_id) VALUES(1,?,'bar_close',26.0,25.5,700000,1500000,'kite.quote','kite')",
        (LIVE_MARK,),
    )
    # an index, which our EQUITY store can never hold a bar for
    con.execute(
        "INSERT INTO underlying_snapshots(underlying,captured_at,mark_kind,spot,spot_symbol,"
        "vendor_id,fetched_at,snapshot_id) VALUES('NIFTY',?,'bar_close',25120.0,'NIFTY 50','kite',?,'s1')",
        (LIVE_MARK, DAY),
    )
    # the rebuilt readings: rows are there, the spot is not
    for mark in [*REBUILT_MARKS, AUCTION_MARK]:
        for name, symbol in (("RELIANCE", None), ("NIFTY", None)):
            con.execute(
                "INSERT INTO underlying_snapshots(underlying,captured_at,mark_kind,spot,spot_symbol,"
                "vendor_id,fetched_at,snapshot_id) VALUES(?,?,'bar_close',NULL,?,'kite',?,'s2')",
                (name, mark, symbol, DAY),
            )
        con.execute(
            "INSERT INTO snapshots(instrument_token,captured_at,mark_kind,last_price,average_price,"
            "volume,oi,source,vendor_id)"
            " VALUES(1,?,'bar_close',25.0,NULL,700000,1500000,'kite.candles_15m','kite')",
            (mark,),
        )
    con.commit()
    yield s
    s.close()


@pytest.fixture
def market15(tmp_path):
    """Our own equity store: bars for the two rebuilt readings, none for the auction."""
    path = tmp_path / "market15.db"
    con = sqlite3.connect(path)
    con.execute(
        "CREATE TABLE candles_15m(instrument_id INTEGER, symbol TEXT, exchange TEXT, bar_start TEXT,"
        " bar_end TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER,"
        " candle_complete INTEGER, quality_flags TEXT, revision INTEGER)"
    )
    rows = [
        # (bar_start, bar_end, close) -- the reading is the bar's CLOSE time
        (f"{DAY} 11:15:00", LIVE_MARK, 1407.0, 1, ""),
        (f"{DAY} 11:30:00", REBUILT_MARKS[0], 1411.5, 1, ""),
        (f"{DAY} 11:45:00", REBUILT_MARKS[1], 1409.25, 1, ""),
        # 15:00-15:15 exists; 15:15-15:30 does NOT -- the closing auction
        (f"{DAY} 15:00:00", f"{DAY} 15:15:00", 1415.0, 1, ""),
    ]
    con.executemany(
        "INSERT INTO candles_15m(instrument_id,symbol,exchange,bar_start,bar_end,open,high,low,close,"
        "volume,candle_complete,quality_flags,revision) VALUES(1,'RELIANCE','NSE',?,?,?,?,?,?,1000,?,?,1)",
        [(a, b, c, c, c, c, complete, flags) for a, b, c, complete, flags in rows],
    )
    con.commit()
    con.close()
    return path


# ── the rule ────────────────────────────────────────────────────────────────

def test_the_spot_written_is_that_readings_own_bar_close(store, market15):
    out = SB.backfill_session(store.con, SESSION, market15_path=market15)
    assert out["rows_filled"] == 2
    got = dict(store.con.execute(
        "SELECT captured_at, spot FROM underlying_snapshots WHERE underlying='RELIANCE'"
        " AND spot IS NOT NULL").fetchall())
    assert got == {LIVE_MARK: 1408.0, REBUILT_MARKS[0]: 1411.5, REBUILT_MARKS[1]: 1409.25}


def test_a_reading_with_no_bar_of_its_own_gets_nothing_at_all(store, market15):
    """THE CLOSING AUCTION. There is a bar half an hour either side of 15:30 and neither is 15:30's."""
    out = SB.backfill_session(store.con, SESSION, market15_path=market15)
    row = store.con.execute(
        "SELECT spot, spot_symbol, spot_source FROM underlying_snapshots"
        " WHERE underlying='RELIANCE' AND captured_at=?", (AUCTION_MARK,)).fetchone()
    assert tuple(row) == (None, None, None), "no carry-forward, no interpolation, no nearest bar"
    assert out["per_reading"][AUCTION_MARK]["filled"] == 0
    assert out["per_reading"][AUCTION_MARK][SB.REASON_NO_BAR] == 1
    assert "RELIANCE" in out["unresolved"][SB.REASON_NO_BAR]


def test_an_index_is_reported_rather_than_invented(store, market15):
    """`db/market15.db` holds equities. An index level is not in it and is not guessed at."""
    out = SB.backfill_session(store.con, SESSION, market15_path=market15)
    assert out["unresolved"][SB.REASON_NOT_IN_STORE] == ["NIFTY"]
    left = store.con.execute(
        "SELECT COUNT(*) FROM underlying_snapshots WHERE underlying='NIFTY'"
        " AND captured_at<>? AND spot IS NOT NULL", (LIVE_MARK,)).fetchone()[0]
    assert left == 0


def test_a_captured_spot_is_never_overwritten_and_a_rerun_changes_nothing(store, market15):
    first = SB.backfill_session(store.con, SESSION, market15_path=market15)
    before = store.con.execute(
        "SELECT spot FROM underlying_snapshots WHERE underlying='RELIANCE' AND captured_at=?",
        (LIVE_MARK,)).fetchone()[0]
    second = SB.backfill_session(store.con, SESSION, market15_path=market15)
    after = store.con.execute(
        "SELECT spot FROM underlying_snapshots WHERE underlying='RELIANCE' AND captured_at=?",
        (LIVE_MARK,)).fetchone()[0]
    assert first["rows_filled"] == 2 and second["rows_filled"] == 0
    assert before == after == 1408.0


def test_a_dry_run_measures_and_writes_nothing(store, market15):
    out = SB.backfill_session(store.con, SESSION, market15_path=market15, write=False)
    assert out["rows_filled"] == 2 and out["written"] is False
    assert store.con.execute(
        "SELECT COUNT(*) FROM underlying_snapshots WHERE captured_at IN (?,?) AND spot IS NOT NULL",
        tuple(REBUILT_MARKS)).fetchone()[0] == 0


def test_the_equity_symbol_comes_from_the_sessions_own_capture(store, market15):
    """NIFTY's spot instrument is "NIFTY 50", not "NIFTY". The mapping is read, never assumed."""
    assert SB.spot_symbols(store.con, SESSION) == {"RELIANCE": "RELIANCE", "NIFTY": "NIFTY 50"}


def test_an_incomplete_or_flagged_bar_is_not_a_close(store, tmp_path):
    """A bar the equity store does not vouch for is not a number to write a spot from."""
    path = tmp_path / "flagged.db"
    con = sqlite3.connect(path)
    con.execute(
        "CREATE TABLE candles_15m(symbol TEXT, bar_start TEXT, bar_end TEXT, close REAL,"
        " candle_complete INTEGER, quality_flags TEXT, revision INTEGER)")
    con.executemany(
        "INSERT INTO candles_15m VALUES('RELIANCE',?,?,?,?,?,1)",
        [(f"{DAY} 11:30:00", REBUILT_MARKS[0], 1411.5, 0, ""),          # not complete
         (f"{DAY} 11:45:00", REBUILT_MARKS[1], 1409.25, 1, "gap")])     # flagged
    con.commit()
    con.close()
    out = SB.backfill_session(store.con, SESSION, market15_path=path)
    assert out["rows_filled"] == 0
    assert out["per_reading"][REBUILT_MARKS[0]][SB.REASON_NO_BAR] == 1


# ── the provenance ──────────────────────────────────────────────────────────

def test_every_rebuilt_spot_says_where_it_came_from(store, market15):
    SB.backfill_session(store.con, SESSION, market15_path=market15)
    sources = dict(store.con.execute(
        "SELECT captured_at, spot_source FROM underlying_snapshots WHERE underlying='RELIANCE'"
        " AND spot IS NOT NULL").fetchall())
    assert sources[REBUILT_MARKS[0]] == config.SPOT_SOURCE_MARKET15
    assert sources[REBUILT_MARKS[1]] == config.SPOT_SOURCE_MARKET15
    assert config.SPOT_SOURCE_MARKET15 in config.SPOT_SOURCES_REBUILT


def test_a_live_reading_is_stamped_only_where_the_store_proves_it(store, market15):
    """The evidence is `snapshots.source` at the same mark, not the clock and not an assumption."""
    out = SB.stamp_captured_sources(store.con, SESSION)
    assert out["live_marks"] == [LIVE_MARK]
    assert sorted(out["unstamped_marks"]) == sorted([*REBUILT_MARKS, AUCTION_MARK])
    assert out["rows_stamped"] == 2  # RELIANCE and NIFTY at the live mark
    got = dict(store.con.execute(
        "SELECT underlying, spot_source FROM underlying_snapshots WHERE captured_at=?",
        (LIVE_MARK,)).fetchall())
    assert got == {"RELIANCE": config.SPOT_SOURCE_QUOTE, "NIFTY": config.SPOT_SOURCE_QUOTE}


def test_a_mark_whose_sources_are_mixed_is_left_unstamped(store, market15):
    """"Not recorded" is an honest answer. A guess dressed as provenance is not."""
    store.con.execute(
        "INSERT INTO snapshots(instrument_token,captured_at,mark_kind,last_price,volume,oi,source,vendor_id)"
        " VALUES(2,?,'bar_close',5.0,1,1,'kite.candles_15m','kite')", (LIVE_MARK,))
    store.con.commit()
    out = SB.stamp_captured_sources(store.con, SESSION)
    assert out["live_marks"] == [] and LIVE_MARK in out["unstamped_marks"]
    assert out["rows_stamped"] == 0


def test_the_metric_rows_take_the_same_source_and_no_other_number_moves(store, market15):
    """A provenance write is a provenance write: it stamps the source and touches nothing else."""
    store.con.execute(
        "INSERT INTO metrics(scope,metric_key,captured_at,underlying,spot,last_price,premium_cr)"
        " VALUES('contract','RELIANCE26SEP1400CE',?,'RELIANCE',1408.0,26.0,17.85)", (LIVE_MARK,))
    # a row whose spot DISAGREES with the roll-up is not described by the roll-up's source
    store.con.execute(
        "INSERT INTO metrics(scope,metric_key,captured_at,underlying,spot,last_price,premium_cr)"
        " VALUES('contract','OTHER',?,'RELIANCE',999.0,26.0,17.85)", (LIVE_MARK,))
    store.con.commit()
    SB.stamp_captured_sources(store.con, SESSION)
    stamped = SB.propagate_spot_source(store.con, [LIVE_MARK])
    assert stamped == 1
    rows = dict(store.con.execute(
        "SELECT metric_key, spot_source FROM metrics WHERE captured_at=?", (LIVE_MARK,)).fetchall())
    assert rows["RELIANCE26SEP1400CE"] == config.SPOT_SOURCE_QUOTE
    assert rows["OTHER"] is None
    kept = store.con.execute(
        "SELECT spot, last_price, premium_cr FROM metrics WHERE metric_key='RELIANCE26SEP1400CE'"
    ).fetchone()
    assert tuple(kept) == (1408.0, 26.0, 17.85)


def test_the_provenance_column_is_added_to_a_store_that_predates_it(tmp_path):
    """`CREATE TABLE IF NOT EXISTS` cannot add a column, so the migration is explicit and idempotent."""
    from market_data.derivatives.store import apply_migrations
    path = tmp_path / "old.db"
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE underlying_snapshots(underlying TEXT, captured_at TEXT, spot REAL)")
    con.execute("CREATE TABLE metrics(scope TEXT, captured_at TEXT, spot REAL)")
    con.commit()
    assert apply_migrations(con) == ["underlying_snapshots.spot_source", "metrics.spot_source"]
    assert apply_migrations(con) == [], "running it twice adds nothing"
    for table in ("underlying_snapshots", "metrics"):
        assert "spot_source" in {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
    con.close()


def test_market15_is_opened_read_only(tmp_path, market15):
    """This package never writes the equity store, and the connection itself refuses to."""
    con = SB.open_market15(market15)
    try:
        with pytest.raises(sqlite3.OperationalError):
            con.execute("DELETE FROM candles_15m")
    finally:
        con.close()


# ── the indices, which an equity store can never hold ───────────────────────

class _FakeProvider:
    """Stands in for Kite: returns 15-minute bars for the index tokens only."""

    def __init__(self, bars):
        self.bars = bars


def test_the_vendor_pass_fills_only_the_readings_the_vendor_returned(store, market15, monkeypatch):
    """Same reading, never a neighbour - the rule does not soften because the source changed."""
    from market_data.derivatives import seed_from_candles

    SB.backfill_session(store.con, SESSION, market15_path=market15)
    # the vendor answers for the first rebuilt reading and NOT for the second
    monkeypatch.setattr(
        seed_from_candles, "fetch_spot_series",
        lambda provider, names, session: {"NIFTY": ("NIFTY 50", {REBUILT_MARKS[0]: 25118.4})})
    out = SB.backfill_from_vendor(store.con, SESSION, ["NIFTY"], provider=object())
    assert out["rows_filled"] == 1
    assert out["marks_filled"] == [REBUILT_MARKS[0]]
    got = dict(store.con.execute(
        "SELECT captured_at, spot FROM underlying_snapshots WHERE underlying='NIFTY'"
        " AND spot IS NOT NULL").fetchall())
    assert got == {LIVE_MARK: 25120.0, REBUILT_MARKS[0]: 25118.4}
    source = store.con.execute(
        "SELECT spot_symbol, spot_source FROM underlying_snapshots WHERE underlying='NIFTY'"
        " AND captured_at=?", (REBUILT_MARKS[0],)).fetchone()
    assert tuple(source) == ("NIFTY 50", config.SPOT_SOURCE_CANDLES)


def test_the_readings_worth_recomputing_are_the_ones_carrying_a_rebuilt_spot(store, market15):
    assert SB.rebuilt_marks(store.con, SESSION) == []
    SB.backfill_session(store.con, SESSION, market15_path=market15)
    assert SB.rebuilt_marks(store.con, SESSION) == REBUILT_MARKS
    SB.stamp_captured_sources(store.con, SESSION)
    assert LIVE_MARK not in SB.rebuilt_marks(store.con, SESSION), \
        "a spot the vendor quoted at the mark is not a rebuilt one"
