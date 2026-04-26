"""
KANIDA.AI — NSE F&O Universe Data Fetch (yfinance)
====================================================
Fetches 3-year daily OHLCV for all 188 NSE F&O stocks.
Writes to kanida_quant.db / ohlc_daily table.

Usage:
    python data/ingest/fetch_fno_yfinance.py

Estimated time: 15-25 minutes (188 stocks, threaded, with rate-limit pauses).
"""
from __future__ import annotations

import math
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

import yfinance as yf
import pandas as pd

ROOT    = Path(__file__).parent.parent.parent
DB_PATH = ROOT / "data" / "db" / "kanida_quant.db"

MARKET     = "NSE"
START_DATE = "2020-01-01"
WORKERS    = 8    # yfinance is network-bound; 8 parallel is fast without triggering rate-limits

# ── NSE F&O universe (188 stocks) ────────────────────────────────────────────
FNO_TICKERS = [
    "AARTIIND","ABB","ABBOTINDIA","ABCAPITAL","ABFRL","ACC","ADANIENT",
    "ADANIPORTS","ALKEM","AMBUJACEM","APOLLOHOSP","APOLLOTYRE","ASHOKLEY",
    "ASIANPAINT","ASTRAL","AUBANK","AUROPHARMA","AXISBANK",
    "BAJAJ-AUTO","BAJAJFINSV","BAJFINANCE","BALKRISIND","BANDHANBNK",
    "BANKBARODA","BATAINDIA","BEL","BERGEPAINT","BHARATFORG","BHARTIARTL",
    "BHEL","BIOCON","BPCL","BRITANNIA","CANBK","CANFINHOME",
    "CHOLAFIN","CIPLA","COALINDIA","COFORGE","COLPAL","CONCOR",
    "CROMPTON","DABUR","DEEPAKNTR","DIVISLAB","DIXON","DLF","DRREDDY",
    "EICHERMOT","ESCORTS","EXIDEIND","FEDERALBNK","GAIL","GLENMARK",
    "GODREJCP","GODREJPROP","GRANULES","GRASIM","GUJGASLTD","HAL",
    "HAVELLS","HCLTECH","HDFCAMC","HDFCBANK","HDFCLIFE","HEROMOTOCO",
    "HINDALCO","HINDPETRO","HINDUNILVR","ICICIBANK","ICICIGI","ICICIPRULI",
    "IDEA","IDFCFIRSTB","IEX","IGL","INDHOTEL","INDUSINDBK","INFY",
    "IOC","IRCTC","IRFC","ITC","JINDALSTEL","JSWSTEEL","JUBLFOOD",
    "KOTAKBANK","LAURUSLABS","LICHSGFIN","LICI","LT","LTIM","LTTS",
    "LUPIN","M&M","MANAPPURAM","MARICO","MARUTI","MCX","MPHASIS",
    "MRF","MUTHOOTFIN","NAUKRI","NESTLEIND","NMDC","NTPC","OFSS",
    "ONGC","PAGEIND","PERSISTENT","PETRONET","PFC","PIDILITIND","PNB",
    "POLYCAB","POWERGRID","PVRINOX","RECLTD","RELIANCE","SAIL",
    "SBICARD","SBILIFE","SBIN","SHREECEM","SIEMENS","SRF","SUNPHARMA",
    "TATACHEM","TATACONSUM","TATAMOTORS","TATAPOWER","TATASTEEL",
    "TCS","TECHM","TITAN","TORNTPHARM","TORNTPOWER","TRENT",
    "TVSMOTOR","UBL","ULTRACEMCO","UPL","VEDL","VOLTAS","WIPRO",
    "ZOMATO","ZYDUSLIFE","MCDOWELL-N",
]

# Tickers that need a non-standard yfinance symbol
_SYMBOL_MAP: dict[str, str] = {
    "M&M":        "M&M.NS",
    "MCDOWELL-N": "MCDOWELL-N.NS",
    "BAJAJ-AUTO": "BAJAJ-AUTO.NS",
}

SECTOR_MAP = {
    "HDFCBANK":"Banks","ICICIBANK":"Banks","SBIN":"Banks","AXISBANK":"Banks",
    "KOTAKBANK":"Banks","BAJFINANCE":"NBFC","INDUSINDBK":"Banks","IDFCFIRSTB":"Banks",
    "TCS":"IT","INFY":"IT","WIPRO":"IT","HCLTECH":"IT","TECHM":"IT",
    "MPHASIS":"IT","PERSISTENT":"IT","LTIM":"IT","LTTS":"IT","COFORGE":"IT",
    "SUNPHARMA":"Pharma","CIPLA":"Pharma","DRREDDY":"Pharma","LUPIN":"Pharma",
    "AUROPHARMA":"Pharma","DIVISLAB":"Pharma","BIOCON":"Pharma",
    "MARUTI":"Auto","TATAMOTORS":"Auto","M&M":"Auto","HEROMOTOCO":"Auto",
    "BAJAJ-AUTO":"Auto","EICHERMOT":"Auto","TVSMOTOR":"Auto","ESCORTS":"Auto",
    "RELIANCE":"Energy","BPCL":"Energy","IOC":"Energy","HINDPETRO":"Energy",
    "ONGC":"Energy","GAIL":"Energy","PETRONET":"Energy",
    "TATASTEEL":"Metals","JSWSTEEL":"Metals","HINDALCO":"Metals",
    "VEDL":"Metals","NMDC":"Metals","SAIL":"Metals","JINDALSTEL":"Metals",
    "HINDUNILVR":"FMCG","ITC":"FMCG","BRITANNIA":"FMCG","NESTLEIND":"FMCG",
    "MARICO":"FMCG","DABUR":"FMCG","COLPAL":"FMCG","GODREJCP":"FMCG",
    "LT":"Capital Goods","HAL":"Capital Goods","SIEMENS":"Capital Goods",
    "HAVELLS":"Capital Goods","POLYCAB":"Capital Goods","ABB":"Capital Goods",
    "ASIANPAINT":"Paints","BERGEPAINT":"Paints","PIDILITIND":"Chemicals",
    "DEEPAKNTR":"Chemicals","TITAN":"Consumer","TRENT":"Retail",
    "IRCTC":"Travel","INDHOTEL":"Hotels","BHARTIARTL":"Telecom",
    "NTPC":"Power","POWERGRID":"Power","TATAPOWER":"Power",
    "TORNTPOWER":"Power","ZOMATO":"Internet","NAUKRI":"Internet",
    "MCDOWELL-N":"Consumer",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _as_scalar(v, default: float = 0.0) -> float:
    if v is None:
        return default
    if hasattr(v, "iloc"):
        try: v = v.iloc[0]
        except: return default
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    if math.isnan(f) or math.isinf(f):
        return default
    return f


def _as_date_str(v) -> str:
    if hasattr(v, "iloc"):
        try: v = v.iloc[0]
        except: return ""
    return str(v)[:10]


def _normalise_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(str(c) for c in col if str(c)) for col in df.columns]
    df.columns = [str(c).lower().strip() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def yf_symbol(ticker: str) -> str:
    return _SYMBOL_MAP.get(ticker, f"{ticker}.NS")


def latest_stored_date(ticker: str, conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT MAX(trade_date) FROM ohlc_daily WHERE ticker=? AND market=?",
        (ticker, MARKET)
    ).fetchone()
    return row[0] if row and row[0] else None


# ── Per-ticker fetch ──────────────────────────────────────────────────────────

def fetch_ticker(ticker: str) -> tuple[str, int, str]:
    """Returns (ticker, rows_written, status_message)."""
    conn = sqlite3.connect(DB_PATH)
    try:
        last = latest_stored_date(ticker, conn)
        if last:
            fetch_start = (
                datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")
        else:
            fetch_start = START_DATE

        today = datetime.now().strftime("%Y-%m-%d")
        if fetch_start > today:
            return ticker, 0, "already_up_to_date"

        symbol = yf_symbol(ticker)
        df = yf.download(
            symbol,
            start=fetch_start,
            end=today,
            auto_adjust=True,
            progress=False,
            multi_level_index=False,
        )

        if df is None or df.empty:
            return ticker, 0, f"no_data ({symbol})"

        df = df.reset_index()
        df = _normalise_df(df)

        if "date" not in df.columns or "close" not in df.columns:
            return ticker, 0, "bad_columns"

        rows = []
        for _, row in df.iterrows():
            trade_date = _as_date_str(row.get("date"))
            close_val  = _as_scalar(row.get("close"))
            if not trade_date or close_val == 0.0:
                continue
            rows.append((
                MARKET, ticker, trade_date,
                _as_scalar(row.get("open"),   close_val),
                _as_scalar(row.get("high"),   close_val),
                _as_scalar(row.get("low"),    close_val),
                close_val,
                int(_as_scalar(row.get("volume"), 0.0)),
                "yfinance", "ok",
            ))

        if rows:
            conn.executemany("""
                INSERT INTO ohlc_daily
                    (market, ticker, trade_date, open, high, low, close, volume, source, quality_flag)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(market, ticker, trade_date) DO UPDATE SET
                    open=excluded.open, high=excluded.high, low=excluded.low,
                    close=excluded.close, volume=excluded.volume,
                    source=excluded.source, quality_flag=excluded.quality_flag,
                    ingested_at=datetime('now')
            """, rows)
            conn.commit()

        return ticker, len(rows), "ok"

    except Exception as exc:
        return ticker, 0, f"error: {exc}"
    finally:
        conn.close()


# ── Instruments registration ──────────────────────────────────────────────────

def register_instruments() -> None:
    conn = sqlite3.connect(DB_PATH)
    for ticker in FNO_TICKERS:
        conn.execute("""
            INSERT INTO instruments(market, ticker, sector, is_active)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(market, ticker) DO UPDATE SET
                sector=excluded.sector, is_active=1
        """, (MARKET, ticker, SECTOR_MAP.get(ticker, "Other")))
    conn.commit()
    conn.close()
    print(f"  Registered {len(FNO_TICKERS)} F&O instruments")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    t0 = time.time()
    print("=" * 65)
    print("KANIDA.AI — NSE F&O Universe Data Fetch")
    print(f"  Stocks : {len(FNO_TICKERS)} F&O tickers")
    print(f"  From   : {START_DATE}  to  today")
    print(f"  DB     : {DB_PATH}")
    print(f"  Workers: {WORKERS} parallel threads")
    print("=" * 65)

    print("\nStep 1/2 — Registering instruments...")
    register_instruments()

    print(f"\nStep 2/2 — Fetching OHLCV (this takes ~15-25 min)...")
    results: dict[str, tuple[int, str]] = {}
    done = 0
    failed: list[str] = []
    skipped: list[str] = []

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(fetch_ticker, t): t for t in FNO_TICKERS}
        for fut in as_completed(futures):
            ticker, n, status = fut.result()
            results[ticker] = (n, status)
            done += 1

            if status.startswith("error"):
                failed.append(ticker)
                marker = "FAIL"
            elif status == "already_up_to_date":
                skipped.append(ticker)
                marker = "SKIP"
            elif n == 0:
                failed.append(ticker)
                marker = "NONE"
            else:
                marker = "OK  "

            elapsed = time.time() - t0
            pct = done / len(FNO_TICKERS) * 100
            print(f"  [{done:>3}/{len(FNO_TICKERS)}  {pct:.0f}%  {elapsed:.0f}s]  "
                  f"{marker} {ticker}: {n} bars ({status})", flush=True)

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed = time.time() - t0
    total_rows = sum(n for n, _ in results.values())
    ok_count   = sum(1 for n, s in results.values() if n > 0)

    print("\n" + "=" * 65)
    print("FETCH COMPLETE")
    print("=" * 65)
    print(f"  Elapsed      : {elapsed:.0f}s  ({elapsed/60:.1f} min)")
    print(f"  Stocks OK    : {ok_count} / {len(FNO_TICKERS)}")
    print(f"  Skipped (UTD): {len(skipped)}")
    print(f"  Failed/Empty : {len(failed)}")
    print(f"  Total rows   : {total_rows:,}")

    if failed:
        print(f"\n  Failed tickers ({len(failed)}):")
        for t in sorted(failed):
            _, s = results[t]
            print(f"    {t:<15}  {s}")

    print("\n  Next steps:")
    print("  1. python engine/jobs/run_learning.py")
    print("  2. python engine/backtest/run_backtest.py")
    print("  3. python engine/backtest/run_execution_analysis.py")
    print("\nDone.")


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    main()
