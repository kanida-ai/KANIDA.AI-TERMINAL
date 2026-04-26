"""
Zerodha Kite Connect adapter — NSE + F&O data ingestion.

Fetches clean OHLCV data and writes to kanida_quant.db.
Handles Kite's daily access token refresh requirement.

Setup:
    pip install kiteconnect
    Set KITE_API_KEY, KITE_API_SECRET in .env
    Run auth flow once per day to get KITE_ACCESS_TOKEN
"""

from __future__ import annotations

import os
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

try:
    from kiteconnect import KiteConnect
except ImportError:
    KiteConnect = None  # type: ignore

from data.validators.data_validator import validate_ohlcv_row, OHLCVRow


DB_PATH = Path(__file__).parent.parent / "db" / "kanida_quant.db"

# Kite delivers max 2000 candles per request for day interval
KITE_MAX_DAYS_PER_CALL = 365


class KiteAdapter:
    """
    Wraps Kite Connect API to deliver normalized OHLCV records
    ready for insertion into ohlc_daily.

    Daily access token must be refreshed each morning via:
        adapter.refresh_access_token(request_token)
    or set KITE_ACCESS_TOKEN directly in env after manual login.
    """

    MARKET = "NSE"

    def __init__(self) -> None:
        if KiteConnect is None:
            raise ImportError("kiteconnect not installed. Run: pip install kiteconnect")
        self.api_key = os.environ["KITE_API_KEY"]
        self.api_secret = os.environ["KITE_API_SECRET"]
        self.kite = KiteConnect(api_key=self.api_key)
        access_token = os.environ.get("KITE_ACCESS_TOKEN", "")
        if access_token:
            self.kite.set_access_token(access_token)
        self._instrument_cache: dict[str, int] = {}

    # ── Auth ──────────────────────────────────────────────────

    def get_login_url(self) -> str:
        """Returns URL user must visit to get request_token."""
        return self.kite.login_url()

    def refresh_access_token(self, request_token: str) -> str:
        """Exchange request_token for access_token. Call once per day."""
        data = self.kite.generate_session(request_token, api_secret=self.api_secret)
        access_token = data["access_token"]
        self.kite.set_access_token(access_token)
        os.environ["KITE_ACCESS_TOKEN"] = access_token
        print(f"Kite access token refreshed. Set KITE_ACCESS_TOKEN={access_token}")
        return access_token

    # ── Instruments ───────────────────────────────────────────

    def load_instruments(self) -> dict[str, int]:
        """Fetch full NSE instrument list and build ticker→token map."""
        instruments = self.kite.instruments("NSE")
        self._instrument_cache = {
            inst["tradingsymbol"]: inst["instrument_token"]
            for inst in instruments
            if inst["segment"] == "NSE"
        }
        print(f"Loaded {len(self._instrument_cache)} NSE instruments")
        return self._instrument_cache

    def get_token(self, ticker: str) -> int:
        if not self._instrument_cache:
            self.load_instruments()
        token = self._instrument_cache.get(ticker)
        if not token:
            raise ValueError(f"Instrument not found: {ticker}. Check ticker symbol format (e.g. 'RELIANCE' not 'RELIANCE.NS')")
        return token

    # ── OHLCV Fetch ───────────────────────────────────────────

    def fetch_ohlcv(
        self,
        ticker: str,
        from_date: str,
        to_date: str,
        interval: str = "day",
    ) -> list[dict[str, Any]]:
        """
        Fetch OHLCV for a single NSE ticker.
        Returns list of dicts normalized to kanida_quant.db ohlc_daily schema.
        """
        token = self.get_token(ticker)
        raw = self.kite.historical_data(
            instrument_token=token,
            from_date=from_date,
            to_date=to_date,
            interval=interval,
        )
        return self._normalize(ticker, raw)

    def fetch_ohlcv_chunked(
        self,
        ticker: str,
        from_date: str,
        to_date: str,
    ) -> list[dict[str, Any]]:
        """Fetch multi-year OHLCV in chunks to respect Kite's candle limit."""
        start = datetime.strptime(from_date, "%Y-%m-%d").date()
        end = datetime.strptime(to_date, "%Y-%m-%d").date()
        all_rows: list[dict[str, Any]] = []
        cursor = start
        while cursor <= end:
            chunk_end = min(cursor + timedelta(days=KITE_MAX_DAYS_PER_CALL), end)
            rows = self.fetch_ohlcv(ticker, cursor.isoformat(), chunk_end.isoformat())
            all_rows.extend(rows)
            cursor = chunk_end + timedelta(days=1)
            time.sleep(0.2)  # respect rate limits
        return all_rows

    def _normalize(self, ticker: str, raw: list[dict]) -> list[dict[str, Any]]:
        out = []
        for r in raw:
            row = OHLCVRow(
                market=self.MARKET,
                ticker=ticker,
                trade_date=str(r["date"])[:10],
                open=float(r["open"]),
                high=float(r["high"]),
                low=float(r["low"]),
                close=float(r["close"]),
                volume=int(r["volume"]),
                source="kite",
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
        """Fetch and write validated OHLCV rows to ohlc_daily."""
        from data.validators.data_validator import DataValidator
        rows = self.fetch_ohlcv_chunked(ticker, from_date, to_date)
        validator = DataValidator(self.MARKET, ticker)
        ok, suspect, rejected = validator.validate_series(rows)

        con = sqlite3.connect(db_path)
        try:
            written = _upsert_ohlcv(con, ok + suspect)
            con.commit()
        finally:
            con.close()

        return {"fetched": len(rows), "written": written, "rejected": len(rejected)}


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
