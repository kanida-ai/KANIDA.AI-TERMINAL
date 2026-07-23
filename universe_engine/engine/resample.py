"""
Resample 1-min bars → 5m / 15m / 30m. Multi-worker (process pool).
"""
from __future__ import annotations
import os, sqlite3
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple


SUPPORTED_TFS = ("5min", "15min", "30min")


def _floor_to_minute(dt: datetime, mins: int) -> datetime:
    """Floor a datetime to a multiple of `mins` minutes from midnight."""
    total_min = dt.hour * 60 + dt.minute
    floor_min = (total_min // mins) * mins
    return dt.replace(hour=floor_min // 60, minute=floor_min % 60, second=0, microsecond=0)


def _resample_one_symbol(args) -> dict:
    sym, db_path, target_tf_min = args
    tf_table = f"ohlc_{target_tf_min}min"
    con = sqlite3.connect(db_path, timeout=60.0)
    cur = con.cursor()

    # Load all 1m bars for this symbol
    rows = list(cur.execute("""
        SELECT bar_time, open, high, low, close, volume
        FROM ohlc_1min WHERE symbol = ? ORDER BY bar_time
    """, (sym,)))
    if not rows:
        con.close()
        return {"symbol": sym, "tf_min": target_tf_min, "in": 0, "out": 0}

    # Bucket
    buckets = defaultdict(list)
    for ts_str, o, h, l, c, v in rows:
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        floor_dt = _floor_to_minute(dt, target_tf_min)
        buckets[floor_dt].append((dt, o, h, l, c, v))

    # Aggregate
    out_rows = []
    for floor_dt in sorted(buckets):
        b = sorted(buckets[floor_dt], key=lambda x: x[0])
        op = b[0][1]
        cl = b[-1][4]
        hi = max(r[2] for r in b)
        lo = min(r[3] for r in b)
        vol = sum(r[5] for r in b)
        out_rows.append((sym, floor_dt.strftime("%Y-%m-%d %H:%M:%S"),
                         op, hi, lo, cl, vol))

    cur.executemany(f"""
        INSERT OR REPLACE INTO {tf_table}
            (symbol, bar_time, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, out_rows)
    con.commit()
    con.close()
    return {"symbol": sym, "tf_min": target_tf_min,
            "in": len(rows), "out": len(out_rows)}


def resample_all_symbols(db_path: Path, symbols: List[str],
                         tfs_min: Tuple[int, ...] = (5, 15, 30),
                         n_workers: int = 16) -> dict:
    """Resample 1m bars to 5m, 15m, 30m for every symbol. Multi-worker."""
    n_workers = max(10, min(48, n_workers))
    summary = {"by_tf": {}, "errors": []}
    for tf_min in tfs_min:
        print(f"[resample] -> ohlc_{tf_min}min  | {len(symbols)} symbols, {n_workers} workers")
        args_list = [(s, str(db_path), tf_min) for s in symbols]
        in_total = out_total = done = 0
        with ProcessPoolExecutor(max_workers=n_workers) as ex:
            futs = [ex.submit(_resample_one_symbol, a) for a in args_list]
            for f in as_completed(futs):
                try:
                    r = f.result()
                except Exception as e:
                    summary["errors"].append(str(e)); continue
                in_total += r["in"]; out_total += r["out"]; done += 1
        summary["by_tf"][f"{tf_min}min"] = {
            "in": in_total, "out": out_total, "symbols": done
        }
        print(f"  done: {done} symbols, {in_total:,} 1-min in -> {out_total:,} {tf_min}-min out")
    return summary


def ensure_intraday_schema(con: sqlite3.Connection):
    """Create intraday OHLC tables if missing."""
    schema = """
    CREATE TABLE IF NOT EXISTS ohlc_5min (
        symbol TEXT NOT NULL, bar_time TEXT NOT NULL,
        open REAL, high REAL, low REAL, close REAL, volume INTEGER,
        PRIMARY KEY (symbol, bar_time)
    );
    CREATE INDEX IF NOT EXISTS idx_ohlc5_t ON ohlc_5min(bar_time);
    CREATE TABLE IF NOT EXISTS ohlc_15min (
        symbol TEXT NOT NULL, bar_time TEXT NOT NULL,
        open REAL, high REAL, low REAL, close REAL, volume INTEGER,
        PRIMARY KEY (symbol, bar_time)
    );
    CREATE INDEX IF NOT EXISTS idx_ohlc15_t ON ohlc_15min(bar_time);
    CREATE TABLE IF NOT EXISTS ohlc_30min (
        symbol TEXT NOT NULL, bar_time TEXT NOT NULL,
        open REAL, high REAL, low REAL, close REAL, volume INTEGER,
        PRIMARY KEY (symbol, bar_time)
    );
    CREATE INDEX IF NOT EXISTS idx_ohlc30_t ON ohlc_30min(bar_time);
    """
    con.executescript(schema)
    con.commit()
