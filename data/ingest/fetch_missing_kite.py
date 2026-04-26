"""
Fetch the 3 tickers whose Kite symbols differ from their NSE F&O canonical names.
Stores data in the DB under the canonical name so the rest of the pipeline is consistent.

  TATAMOTORS  <- Kite: TMCV     (Tata Motors CV, post-demerger)
  ZOMATO      <- Kite: ETERNAL  (Zomato rebranded to Eternal, Jan 2025)
  MCDOWELL-N  <- Kite: UNITDSPR (United Spirits)

LTIM (LTIMindtree) is genuinely absent from this Kite account -- skip for now.
"""
from __future__ import annotations

import os, sqlite3, sys, time
from datetime import date, timedelta, datetime
from pathlib import Path

ROOT    = Path(__file__).parent.parent.parent
DB_PATH = ROOT / "data" / "db" / "kanida_quant.db"
sys.path.insert(0, str(ROOT))

env = ROOT / "config" / ".env"
for line in env.read_text().splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line: continue
    k, v = line.split("=", 1)
    os.environ[k.strip()] = v.strip().strip('"').strip("'")

from kiteconnect import KiteConnect

API_KEY      = os.environ["KITE_API_KEY"]
ACCESS_TOKEN = os.environ["KITE_ACCESS_TOKEN"]
MARKET       = "NSE"
FROM_DATE    = (date.today() - timedelta(days=365 * 3)).isoformat()
TO_DATE      = date.today().isoformat()
VOL_FLOOR_DAILY  = 50_000
VOL_FLOOR_WEEKLY = 200_000
MAX_MOVE = 0.25

# canonical_name -> kite_symbol
SYMBOL_OVERRIDES = {
    "TATAMOTORS": "TMCV",
    "ZOMATO":     "ETERNAL",
    "MCDOWELL-N": "UNITDSPR",
}


def validate_row(row, prev_close, vol_floor):
    if row["close"] <= 0 or row["volume"] <= 0: return "rejected"
    if row["high"] < row["low"]:                return "rejected"
    if row["volume"] < vol_floor:               return "suspect"
    if prev_close and prev_close > 0:
        if abs(row["close"] - prev_close) / prev_close > MAX_MOVE: return "rejected"
        if abs(row["open"] - prev_close) / prev_close > 0.12 and row["volume"] < vol_floor * 3:
            return "suspect"
    return "ok"


def fetch_and_store(kite, con, canonical, kite_sym, token, interval):
    table    = "ohlc_daily"  if interval == "day"  else "ohlc_weekly"
    date_col = "trade_date"  if interval == "day"  else "week_start"
    vol_floor = VOL_FLOOR_DAILY if interval == "day" else VOL_FLOOR_WEEKLY

    last = con.execute(
        f"SELECT MAX({date_col}) FROM {table} WHERE ticker=? AND market=?",
        (canonical, MARKET)
    ).fetchone()[0]
    fetch_from = FROM_DATE
    if last:
        fetch_from = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        if fetch_from > TO_DATE:
            print(f"    [{canonical}/{kite_sym}] {interval}: already up to date")
            return 0

    print(f"    [{canonical}/{kite_sym}] {interval} {fetch_from} -> {TO_DATE} ...", end=" ", flush=True)
    try:
        raw = kite.historical_data(token, fetch_from, TO_DATE, interval)
    except Exception as e:
        print(f"ERROR: {e}")
        return 0

    prev_close = None
    rows = []
    for r in raw:
        flag = validate_row(r, prev_close, vol_floor)
        if flag == "rejected": continue
        rows.append((
            MARKET, canonical,          # store under canonical name
            str(r["date"])[:10],
            float(r["open"]), float(r["high"]),
            float(r["low"]),  float(r["close"]),
            int(r["volume"]), "kite", flag
        ))
        prev_close = float(r["close"])

    if rows:
        con.executemany(f"""
            INSERT INTO {table}
                (market, ticker, {date_col}, open, high, low, close, volume, source, quality_flag)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(market, ticker, {date_col}) DO UPDATE SET
                open=excluded.open, high=excluded.high, low=excluded.low,
                close=excluded.close, volume=excluded.volume,
                quality_flag=excluded.quality_flag,
                ingested_at=datetime('now')
        """, rows)
        con.commit()

    print(f"{len(rows)} bars written")
    return len(rows)


def main():
    print("=" * 60)
    print("KANIDA.AI -- Fetching 3 symbol-mapped tickers")
    for c, k in SYMBOL_OVERRIDES.items():
        print(f"  {c:<15} <- Kite: {k}")
    print("=" * 60)

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)

    instruments = kite.instruments("NSE")
    token_map = {i["tradingsymbol"]: i["instrument_token"] for i in instruments if i["segment"] == "NSE"}

    con = sqlite3.connect(DB_PATH)

    for canonical, kite_sym in SYMBOL_OVERRIDES.items():
        token = token_map.get(kite_sym)
        print(f"\n[{canonical}] Kite symbol: {kite_sym} -> token: {token}")
        if not token:
            print(f"  SKIP: {kite_sym} not found in NSE instruments")
            continue
        # Register under canonical name
        con.execute("""
            INSERT INTO instruments(market, ticker, is_active)
            VALUES (?,?,1)
            ON CONFLICT(market, ticker) DO UPDATE SET is_active=1
        """, (MARKET, canonical))
        con.commit()

        fetch_and_store(kite, con, canonical, kite_sym, token, "day")
        time.sleep(0.35)
        fetch_and_store(kite, con, canonical, kite_sym, token, "week")
        time.sleep(0.35)

    con.close()
    print("\nDone. Re-run the pipeline steps to include these tickers.")


if __name__ == "__main__":
    main()
