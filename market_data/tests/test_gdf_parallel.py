"""Parallel capture — the store keeps first-seen values and counts revisions; the comparison pairs bar T-15 with Kite T."""
import sqlite3
from datetime import datetime

from market_data.gdf_parallel import SCHEMA, compare, upsert_bars
from market_data.types import IST


def bar(h, m, close, oi):
    return {'bar_start': datetime(2026, 9, 22, h, m, tzinfo=IST), 'Open': close, 'High': close, 'Low': close,
            'Close': close, 'TradedQty': 5, 'OpenInterest': oi}


def test_revisions_keep_the_first_values_and_drop_out_of_session_bars():
    db = sqlite3.connect(':memory:')
    db.executescript(SCHEMA)
    upsert_bars(db, 'X', [bar(9, 15, 100, 10), bar(15, 30, 1, 1)], '2026-09-22 09:31:00')
    upsert_bars(db, 'X', [bar(9, 15, 101, 12)], '2026-09-22 09:46:00')
    upsert_bars(db, 'X', [bar(9, 15, 101, 12)], '2026-09-22 10:01:00')
    rows = db.execute('select bar_start, close, first_close, first_oi, first_seen, last_seen, revisions from vendor_bars').fetchall()
    assert rows == [('2026-09-22 09:15:00', 101, 100, 10, '2026-09-22 09:31:00', '2026-09-22 10:01:00', 1)]


def test_compare_pairs_the_bar_starting_fifteen_minutes_before_the_reading(tmp_path):
    db = sqlite3.connect(tmp_path / 'vendor_shadow.db')
    db.executescript(SCHEMA)
    db.execute("insert into universe values('FUTIDX_NIFTY_29SEP2026_XX_0','NFO','NIFTY','FUT',null,'2026-09-29',1,'probe','',0)")
    db.execute("insert into kite_rows values('FUTIDX_NIFTY_29SEP2026_XX_0','2026-09-22 09:30:00',200,50,0,'')")
    upsert_bars(db, 'FUTIDX_NIFTY_29SEP2026_XX_0', [bar(9, 15, 200.5, 50), bar(9, 30, 999, 1)], '2026-09-22 09:46:00')
    db.commit()
    db.close()
    text = compare(tmp_path, '2026-09-22')
    row = next(l for l in text.splitlines() if l.startswith('| 09:30 | Futures'))
    assert '| 1/1 | 0 | 0.250% | 1/1 | 1/1 | +16.0 |' in row
