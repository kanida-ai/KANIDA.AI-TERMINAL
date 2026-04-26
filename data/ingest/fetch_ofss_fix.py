"""
Re-fetch OFSS with corrected thresholds.
OFSS is a high-priced IT stock (~Rs 5000-10000) with naturally low share volume.
Normal daily volume: 10K-50K shares. The 50K floor was too aggressive.
Also fixes validator cascade bug: rejected rows still advance prev_close.
"""
from __future__ import annotations
import sqlite3, sys, time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from kiteconnect import KiteConnect

DB_PATH    = Path(__file__).parent.parent / "db" / "kanida_quant.db"
API_KEY    = "8h8pm171qawrjgdn"
ACCESS_TOKEN = "drfa4uSDpebxrCyhXEWAKeACVrPikJWj"

FROM_DATE  = (date.today() - timedelta(days=365 * 3)).isoformat()
TO_DATE    = date.today().isoformat()
OFSS_TOKEN = 2748929

# OFSS-specific thresholds
VOL_FLOOR_DAILY  = 5_000    # high-priced stock, low share count is normal
VOL_FLOOR_WEEKLY = 25_000
MAX_MOVE         = 0.40     # 40% — allow genuine large result-day moves
# High-volume exception: if vol > this multiplier * floor, always accept
HIGH_VOL_MULTIPLIER = 20


def validate(row, prev_close, vol_floor):
    if row["close"] <= 0 or row["volume"] <= 0: return "rejected"
    if row["high"] < row["low"]: return "rejected"
    flag = "ok"
    if row["volume"] < vol_floor:
        flag = "suspect"
    if prev_close and prev_close > 0:
        move = abs(row["close"] - prev_close) / prev_close
        # High-volume institutional event — always accept, just flag
        if row["volume"] > vol_floor * HIGH_VOL_MULTIPLIER:
            if move > MAX_MOVE:
                flag = "suspect"   # large move but huge volume — real event
        elif move > MAX_MOVE:
            return "rejected"
    return flag


def fetch_store(kite, con, interval):
    table    = "ohlc_daily"  if interval == "day"  else "ohlc_weekly"
    date_col = "trade_date"  if interval == "day"  else "week_start"
    vol_floor = VOL_FLOOR_DAILY if interval == "day" else VOL_FLOOR_WEEKLY

    print(f"\n  [OFSS] Fetching {interval} {FROM_DATE} -> {TO_DATE}...")
    raw = kite.historical_data(OFSS_TOKEN, FROM_DATE, TO_DATE, interval)

    rows, rejected = [], 0
    prev_close = None
    for r in raw:
        flag = validate(r, prev_close, vol_floor)
        # Always advance prev_close so cascade doesn't happen
        prev_close = float(r["close"])
        if flag == "rejected":
            rejected += 1
            continue
        rows.append((
            "NSE", "OFSS", str(r["date"])[:10],
            float(r["open"]), float(r["high"]),
            float(r["low"]),  float(r["close"]),
            int(r["volume"]), "kite", flag
        ))

    if rows:
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
        con.executemany(sql, rows)
        con.commit()

    ok_ct = sum(1 for r in rows if r[-1] == "ok")
    sus_ct = sum(1 for r in rows if r[-1] == "suspect")
    print(f"    -> ok={ok_ct}  suspect={sus_ct}  rejected={rejected}  total={len(raw)}")
    return len(rows)


kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)
con  = sqlite3.connect(DB_PATH)

fetch_store(kite, con, "day")
time.sleep(0.3)
fetch_store(kite, con, "week")

con.close()
print("\nOFSS fix complete.")
