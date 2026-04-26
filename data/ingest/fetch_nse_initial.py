"""
Initial NSE data fetch — 3 years daily + weekly OHLCV.
Writes to kanida_quant.db with quality validation.

Usage:
    python data/ingest/fetch_nse_initial.py
"""
from __future__ import annotations

import sqlite3
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from kiteconnect import KiteConnect

DB_PATH = Path(__file__).parent.parent / "db" / "kanida_quant.db"

API_KEY    = "8h8pm171qawrjgdn"
API_SECRET = "vcdbx0kp4062dht2qya9ar25hdi3lp2m"
ACCESS_TOKEN = "drfa4uSDpebxrCyhXEWAKeACVrPikJWj"

TICKERS = ["SBIN", "PNB", "ABB", "OFSS", "TATAPOWER"]
FROM_DATE = (date.today() - timedelta(days=365 * 3)).isoformat()
TO_DATE   = date.today().isoformat()
MARKET    = "NSE"

# Volume floors for quality gate (NSE large/mid cap)
VOL_FLOOR_DAILY  = 50_000
VOL_FLOOR_WEEKLY = 200_000
MAX_SINGLE_DAY_MOVE = 0.25   # 25% — reject anything above this


def connect_kite() -> KiteConnect:
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)
    return kite


def get_instrument_tokens(kite: KiteConnect, tickers: list[str]) -> dict[str, int]:
    print("Loading NSE instruments...")
    instruments = kite.instruments("NSE")
    token_map = {
        i["tradingsymbol"]: i["instrument_token"]
        for i in instruments
        if i["segment"] == "NSE" and i["tradingsymbol"] in tickers
    }
    for t in tickers:
        if t not in token_map:
            print(f"  WARNING: {t} not found in NSE instruments")
        else:
            print(f"  {t} -> token {token_map[t]}")
    return token_map


def validate_row(row: dict, prev_close: float | None, vol_floor: int) -> str:
    """Returns 'ok', 'suspect', or 'rejected'."""
    if row["close"] <= 0 or row["volume"] <= 0:
        return "rejected"
    if row["high"] < row["low"]:
        return "rejected"
    if row["volume"] < vol_floor:
        return "suspect"
    if prev_close and prev_close > 0:
        move = abs(row["close"] - prev_close) / prev_close
        if move > MAX_SINGLE_DAY_MOVE:
            return "rejected"
        # Large gap on very low volume = suspect
        gap = abs(row["open"] - prev_close) / prev_close
        if gap > 0.12 and row["volume"] < vol_floor * 3:
            return "suspect"
    return "ok"


def fetch_and_store(
    kite: KiteConnect,
    con: sqlite3.Connection,
    ticker: str,
    token: int,
    interval: str,
) -> dict:
    table   = "ohlc_daily"   if interval == "day"  else "ohlc_weekly"
    date_col = "trade_date"  if interval == "day"  else "week_start"
    vol_floor = VOL_FLOOR_DAILY if interval == "day" else VOL_FLOOR_WEEKLY

    print(f"\n  [{ticker}] Fetching {interval} bars {FROM_DATE} → {TO_DATE}...")
    try:
        raw = kite.historical_data(token, FROM_DATE, TO_DATE, interval)
    except Exception as e:
        print(f"  ERROR fetching {ticker} {interval}: {e}")
        return {"fetched": 0, "written": 0, "rejected": 0}

    ok = suspect = rejected = 0
    prev_close = None
    rows_to_insert = []

    for r in raw:
        flag = validate_row(r, prev_close, vol_floor)
        if flag == "rejected":
            rejected += 1
            print(f"    REJECTED {str(r['date'])[:10]} close={r['close']} vol={r['volume']}")
            continue

        rows_to_insert.append((
            MARKET, ticker,
            str(r["date"])[:10],
            float(r["open"]), float(r["high"]),
            float(r["low"]),  float(r["close"]),
            int(r["volume"]),
            "kite", flag
        ))
        prev_close = float(r["close"])
        if flag == "suspect":
            suspect += 1
            print(f"    SUSPECT  {str(r['date'])[:10]} close={r['close']} vol={r['volume']}")
        else:
            ok += 1

    if rows_to_insert:
        sql = f"""
            INSERT INTO {table}
                (market, ticker, {date_col}, open, high, low, close, volume, source, quality_flag)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(market, ticker, {date_col}) DO UPDATE SET
                open=excluded.open, high=excluded.high, low=excluded.low,
                close=excluded.close, volume=excluded.volume,
                quality_flag=excluded.quality_flag,
                ingested_at=datetime('now')
        """
        con.executemany(sql, rows_to_insert)
        con.commit()

    print(f"    -> ok={ok}  suspect={suspect}  rejected={rejected}  total={len(raw)}")
    return {"fetched": len(raw), "written": len(rows_to_insert), "rejected": rejected}


def register_instruments(con: sqlite3.Connection, tickers: list[str]) -> None:
    for ticker in tickers:
        con.execute("""
            INSERT INTO instruments(market, ticker, is_active)
            VALUES (?, ?, 1)
            ON CONFLICT(market, ticker) DO NOTHING
        """, [MARKET, ticker])
    con.commit()


def main() -> None:
    print("=" * 55)
    print("KANIDA.AI — NSE Initial Data Fetch")
    print(f"Stocks : {', '.join(TICKERS)}")
    print(f"Period : {FROM_DATE}  to  {TO_DATE}")
    print(f"DB     : {DB_PATH}")
    print("=" * 55)

    kite = connect_kite()
    token_map = get_instrument_tokens(kite, TICKERS)

    con = sqlite3.connect(DB_PATH)
    register_instruments(con, TICKERS)

    summary = {}
    for ticker in TICKERS:
        token = token_map.get(ticker)
        if not token:
            print(f"\nSkipping {ticker} — token not found")
            continue
        print(f"\n{'-'*40}")
        print(f"  {ticker}")
        print(f"{'-'*40}")
        d = fetch_and_store(kite, con, ticker, token, "day")
        time.sleep(0.3)
        w = fetch_and_store(kite, con, ticker, token, "week")
        time.sleep(0.3)
        summary[ticker] = {"daily": d, "weekly": w}

    con.close()

    print("\n" + "=" * 55)
    print("SUMMARY")
    print("=" * 55)
    for ticker, s in summary.items():
        d, w = s["daily"], s["weekly"]
        print(f"  {ticker:<12} daily={d['written']:>4} bars   weekly={w['written']:>3} bars   rejected={d['rejected']+w['rejected']}")
    print("\nDone. Ready for pattern learning.")


if __name__ == "__main__":
    main()
