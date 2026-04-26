"""
Polygon.io adapter — US equities + options data ingestion.

Fetches clean OHLCV data and writes to kanida_quant.db.
Free tier: 2 years history, 15-min delayed quotes, unlimited API calls.
Starter ($29/mo): real-time, full options chain.

Setup:
    pip install polygon-api-client
    Set POLYGON_API_KEY in .env
"""

from __future__ import annotations

import os
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

try:
    from polygon import RESTClient
    from polygon.rest.models import Agg
except ImportError:
    RESTClient = None  # type: ignore
    Agg = None  # type: ignore

from data.validators.data_validator import DataValidator, OHLCVRow


DB_PATH = Path(__file__).parent.parent / "db" / "kanida_quant.db"

POLYGON_MAX_RESULTS = 50_000


class PolygonAdapter:
    """
    Wraps Polygon.io REST API to deliver normalized OHLCV records.
    Handles pagination and rate limiting automatically.
    """

    MARKET = "US"

    def __init__(self) -> None:
        if RESTClient is None:
            raise ImportError("polygon-api-client not installed. Run: pip install polygon-api-client")
        api_key = os.environ.get("POLYGON_API_KEY", "")
        if not api_key:
            raise EnvironmentError("POLYGON_API_KEY not set in environment")
        self.client = RESTClient(api_key)

    # ── OHLCV Fetch ───────────────────────────────────────────

    def fetch_ohlcv(
        self,
        ticker: str,
        from_date: str,
        to_date: str,
        adjusted: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Fetch daily OHLCV for a US ticker.
        Returns list of dicts normalized to kanida_quant.db schema.
        adjusted=True returns split/dividend-adjusted prices.
        """
        aggs = self.client.get_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="day",
            from_=from_date,
            to=to_date,
            adjusted=adjusted,
            sort="asc",
            limit=POLYGON_MAX_RESULTS,
        )
        return self._normalize(ticker, list(aggs))

    def _normalize(self, ticker: str, aggs: list) -> list[dict[str, Any]]:
        out = []
        for a in aggs:
            ts_ms = getattr(a, "timestamp", None)
            if ts_ms is None:
                continue
            trade_date = datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d")
            row = OHLCVRow(
                market=self.MARKET,
                ticker=ticker,
                trade_date=trade_date,
                open=float(a.open),
                high=float(a.high),
                low=float(a.low),
                close=float(a.close),
                volume=int(a.volume),
                source="polygon",
            )
            out.append(row.__dict__)
        return out

    # ── DB Write ──────────────────────────────────────────────

    def sync_to_db(
        self,
        ticker: str,
        from_date: str,
        to_date: str,
        db_path: Path = DB_PATH,
    ) -> dict[str, int]:
        """Fetch, validate, and upsert OHLCV rows into ohlc_daily."""
        rows = self.fetch_ohlcv(ticker, from_date, to_date)
        validator = DataValidator(self.MARKET, ticker)
        ok, suspect, rejected = validator.validate_series(rows)

        con = sqlite3.connect(db_path)
        try:
            written = _upsert_ohlcv(con, ok + suspect)
            con.commit()
        finally:
            con.close()

        return {"fetched": len(rows), "written": written, "rejected": len(rejected)}

    def batch_sync(
        self,
        tickers: list[str],
        from_date: str,
        to_date: str,
        db_path: Path = DB_PATH,
        delay_seconds: float = 0.12,
    ) -> dict[str, dict[str, int]]:
        """Batch sync multiple US tickers. Respects Polygon free tier rate limits."""
        results = {}
        for ticker in tickers:
            try:
                result = self.sync_to_db(ticker, from_date, to_date, db_path)
                results[ticker] = result
                print(f"  {ticker}: fetched={result['fetched']} written={result['written']} rejected={result['rejected']}")
            except Exception as e:
                results[ticker] = {"error": str(e)}
                print(f"  {ticker}: ERROR — {e}")
            time.sleep(delay_seconds)
        return results


# ── Helpers ────────────────────────────────────────────────────────────────────

def _upsert_ohlcv(con: sqlite3.Connection, rows: list[dict]) -> int:
    sql = """
        INSERT INTO ohlc_daily
            (market, ticker, trade_date, open, high, low, close, volume, source, quality_flag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(market, ticker, trade_date) DO UPDATE SET
            open=excluded.open, high=excluded.high, low=excluded.low,
            close=excluded.close, volume=excluded.volume,
            source=excluded.source, quality_flag=excluded.quality_flag,
            ingested_at=datetime('now')
    """
    payload = [
        (r["market"], r["ticker"], r["trade_date"],
         r["open"], r["high"], r["low"], r["close"],
         r["volume"], r["source"], r.get("quality_flag", "ok"))
        for r in rows
    ]
    con.executemany(sql, payload)
    return len(payload)
