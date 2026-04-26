from __future__ import annotations

from collections import defaultdict
from typing import Any

from engine.readers.sqlite_reader import ReadOnlySQLite


def load_ohlcv_by_stock(db: ReadOnlySQLite, markets: list[str]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    placeholders = ",".join("?" for _ in markets)
    rows = db.query(
        f"""
        select ticker, market, trade_date, open, high, low, close, volume
        from ohlc_daily
        where market in ({placeholders})
          and open is not null and high is not null and low is not null
          and close is not null and volume is not null
        order by market, ticker, trade_date
        """,
        markets,
    )
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["market"], row["ticker"])].append(row)
    return grouped
