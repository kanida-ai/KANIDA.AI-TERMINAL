"""
KANIDA.AI -- NSE F&O Universe Data Fetch (Zerodha Kite Connect)
================================================================
Exact same workflow as fetch_nse_initial.py but expanded to all 188 F&O stocks.
Reads credentials from config/.env. Writes to kanida_quant.db / ohlc_daily.

Usage:
    python data/ingest/fetch_fno_kite.py

If you get a token-expired error, generate a fresh access token:
  1. Visit: https://kite.trade/connect/login?api_key=<KITE_API_KEY>&v=3
  2. Authorise -> copy request_token from redirect URL
  3. Update KITE_ACCESS_TOKEN in config/.env
"""
from __future__ import annotations

import os
import sqlite3
import sys
import time
from datetime import date, timedelta
from pathlib import Path

ROOT    = Path(__file__).parent.parent.parent
DB_PATH = ROOT / "data" / "db" / "kanida_quant.db"
ENV     = ROOT / "config" / ".env"

sys.path.insert(0, str(ROOT))

# -- Load .env -----------------------------------------------------------------
def _load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

_load_env(ENV)

API_KEY      = os.environ["KITE_API_KEY"]
API_SECRET   = os.environ["KITE_API_SECRET"]
ACCESS_TOKEN = os.environ["KITE_ACCESS_TOKEN"]

from kiteconnect import KiteConnect

MARKET    = "NSE"
FROM_DATE = (date.today() - timedelta(days=365 * 3)).isoformat()
TO_DATE   = date.today().isoformat()

VOL_FLOOR_DAILY  = 50_000
VOL_FLOOR_WEEKLY = 200_000
MAX_SINGLE_DAY_MOVE = 0.25

# -- Full NSE F&O universe (188 stocks) ----------------------------------------
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


# -- Identical helpers from fetch_nse_initial.py --------------------------------

def connect_kite() -> KiteConnect:
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)
    return kite


def get_instrument_tokens(kite: KiteConnect, tickers: list[str]) -> dict[str, int]:
    print("Loading NSE instruments from Kite...")
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
    table    = "ohlc_daily"  if interval == "day"  else "ohlc_weekly"
    date_col = "trade_date"  if interval == "day"  else "week_start"
    vol_floor = VOL_FLOOR_DAILY if interval == "day" else VOL_FLOOR_WEEKLY

    # Incremental: only fetch from the day after what we already have
    last = con.execute(
        f"SELECT MAX({date_col}) FROM {table} WHERE ticker=? AND market=?",
        (ticker, MARKET)
    ).fetchone()[0]
    fetch_from = FROM_DATE
    if last:
        from datetime import datetime
        fetch_from = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        if fetch_from > TO_DATE:
            return {"fetched": 0, "written": 0, "rejected": 0, "status": "up_to_date"}

    print(f"    [{ticker}] {interval} {fetch_from} -> {TO_DATE} ...", end=" ", flush=True)
    try:
        raw = kite.historical_data(token, fetch_from, TO_DATE, interval)
    except Exception as e:
        print(f"ERROR: {e}")
        return {"fetched": 0, "written": 0, "rejected": 0, "status": f"error: {e}"}

    ok = suspect = rejected = 0
    prev_close = None
    rows_to_insert = []

    for r in raw:
        flag = validate_row(r, prev_close, vol_floor)
        if flag == "rejected":
            rejected += 1
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
        else:
            ok += 1

    if rows_to_insert:
        con.executemany(f"""
            INSERT INTO {table}
                (market, ticker, {date_col}, open, high, low, close, volume, source, quality_flag)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(market, ticker, {date_col}) DO UPDATE SET
                open=excluded.open, high=excluded.high, low=excluded.low,
                close=excluded.close, volume=excluded.volume,
                quality_flag=excluded.quality_flag,
                ingested_at=datetime('now')
        """, rows_to_insert)
        con.commit()

    print(f"ok={ok} suspect={suspect} rejected={rejected}")
    return {"fetched": len(raw), "written": len(rows_to_insert),
            "rejected": rejected, "status": "ok"}


def register_instruments(con: sqlite3.Connection) -> None:
    for ticker in FNO_TICKERS:
        con.execute("""
            INSERT INTO instruments(market, ticker, sector, is_active)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(market, ticker) DO UPDATE SET
                sector=excluded.sector, is_active=1
        """, (MARKET, ticker, SECTOR_MAP.get(ticker, "Other")))
    con.commit()
    print(f"  Registered {len(FNO_TICKERS)} instruments")


# -- Universe table bootstrap --------------------------------------------------

def _ensure_universe_table(con: sqlite3.Connection) -> None:
    """Create universe table if it doesn't exist yet."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS universe (
            symbol          TEXT NOT NULL,
            exchange        TEXT NOT NULL DEFAULT 'NSE',
            asset_class     TEXT NOT NULL DEFAULT 'EQUITY',
            company_name    TEXT,
            sector          TEXT,
            industry        TEXT,
            universe_sets   TEXT NOT NULL DEFAULT '["FNO"]',
            is_active       INTEGER NOT NULL DEFAULT 1,
            added_date      TEXT NOT NULL,
            added_by        TEXT NOT NULL DEFAULT 'system',
            notes           TEXT,
            PRIMARY KEY (symbol, exchange)
        )
    """)
    con.commit()


def _seed_universe_if_empty(con: sqlite3.Connection) -> int:
    """
    If the universe table has no NSE rows, seed it from the built-in FNO list.
    Returns number of rows seeded (0 means table already had data).
    """
    count = con.execute(
        "SELECT COUNT(*) FROM universe WHERE exchange='NSE'"
    ).fetchone()[0]
    if count > 0:
        return 0

    today = date.today().isoformat()
    seeded = 0
    for ticker in FNO_TICKERS:
        try:
            con.execute("""
                INSERT OR IGNORE INTO universe
                    (symbol, exchange, asset_class, sector, universe_sets,
                     is_active, added_date, added_by)
                VALUES (?, 'NSE', 'EQUITY', ?, '["FNO"]', 1, ?, 'fetch_bootstrap')
            """, (ticker, SECTOR_MAP.get(ticker, "Other"), today))
            seeded += 1
        except Exception:
            pass
    con.commit()
    print(f"  [universe] Bootstrapped {seeded} stocks from built-in F&O list.")
    print("  [universe] Going forward, manage your stock universe at /admin → Universe tab.")
    return seeded


def _get_active_nse_tickers(con: sqlite3.Connection) -> list[str]:
    """
    Return active NSE tickers from the universe table.
    This is the authoritative ticker list — managed via the admin portal.
    """
    rows = con.execute(
        "SELECT symbol FROM universe WHERE exchange='NSE' AND is_active=1 ORDER BY symbol"
    ).fetchall()
    return [r[0] for r in rows]


# -- Main ----------------------------------------------------------------------

def main() -> None:
    con = sqlite3.connect(DB_PATH)

    # Bootstrap universe table on first run
    _ensure_universe_table(con)
    _seed_universe_if_empty(con)

    # Read authoritative ticker list from DB (managed via admin portal)
    active_tickers = _get_active_nse_tickers(con)

    print("=" * 60)
    print("KANIDA.AI -- NSE F&O Data Fetch (Zerodha Kite)")
    print(f"  Stocks : {len(active_tickers)} active in universe")
    print(f"  Period : {FROM_DATE}  to  {TO_DATE}")
    print(f"  DB     : {DB_PATH}")
    print("=" * 60)

    kite = connect_kite()
    print(f"  Kite connected (api_key={API_KEY})")

    token_map = get_instrument_tokens(kite, active_tickers)
    print(f"  Resolved {len(token_map)}/{len(active_tickers)} tokens\n")

    register_instruments(con)

    summary = {}
    for i, ticker in enumerate(active_tickers, 1):
        token = token_map.get(ticker)
        print(f"\n[{i:>3}/{len(active_tickers)}] {ticker}")
        if not token:
            print(f"  SKIP -- token not found")
            summary[ticker] = {"status": "no_token"}
            continue
        d = fetch_and_store(kite, con, ticker, token, "day")
        time.sleep(0.35)   # Kite rate-limit: stay under 3 req/sec per instrument
        w = fetch_and_store(kite, con, ticker, token, "week")
        time.sleep(0.35)
        summary[ticker] = {"daily": d, "weekly": w}

    con.close()

    # -- Summary ---------------------------------------------------------------
    print("\n" + "=" * 60)
    print("FETCH SUMMARY")
    print("=" * 60)
    ok_list   = []
    fail_list = []
    for ticker, s in summary.items():
        if "daily" in s:
            d = s["daily"]
            if d["status"] == "up_to_date":
                print(f"  {ticker:<15}  already up to date")
            elif d["status"] == "ok":
                print(f"  {ticker:<15}  daily={d['written']:>4} bars   "
                      f"weekly={s['weekly']['written']:>3} bars   "
                      f"rejected={d['rejected']+s['weekly']['rejected']}")
                ok_list.append(ticker)
            else:
                print(f"  {ticker:<15}  FAILED: {d['status']}")
                fail_list.append(ticker)
        else:
            print(f"  {ticker:<15}  SKIPPED: {s['status']}")
            fail_list.append(ticker)

    print(f"\n  OK      : {len(ok_list)}")
    print(f"  Failed  : {len(fail_list)}")
    if fail_list:
        print(f"  Failed tickers: {', '.join(fail_list)}")

    print("""
  Next steps:
    python engine/jobs/run_learning.py
    python engine/backtest/run_backtest.py
    python engine/backtest/run_execution_analysis.py
""")
    print("Done.")


if __name__ == "__main__":
    main()
