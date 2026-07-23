"""
Falcon Engine — outcome labeler.

For every (symbol, trade_date), compute forward returns and hit labels over
multiple horizons. These outcomes are the supervised signal that mining
operates against.

Horizons:
  5d, 10d, 20d, 30d, 40d (trading days)

For each horizon h:
  ret_h    = close[t+h] / open[t+1] - 1   (entry next-day open, exit at close[t+h])
  mfe_h    = max(high[t+1..t+h]) / open[t+1] - 1
  mae_h    = min(low[t+1..t+h])  / open[t+1] - 1

Hit labels (boolean — did MFE within h days reach threshold?):
  hit_5pc_10d, hit_7pc_10d, hit_10pc_20d, hit_15pc_20d, hit_25pc_30d, hit_40pc_40d

Failure label:
  failure_10d = MAE within 10 days hit -7%

This separates "the move happened" (hit_*) from "the trade was profitable at exit"
(ret_*). They're related but not identical — a stock can spike then revert.
"""
from __future__ import annotations
import sqlite3
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List


SCHEMA = """
CREATE TABLE IF NOT EXISTS falcon_outcomes (
    symbol      TEXT NOT NULL,
    trade_date  TEXT NOT NULL,
    -- forward returns
    ret_5d  REAL,  ret_10d REAL,  ret_20d REAL,  ret_30d REAL,  ret_40d REAL,
    -- max favorable excursion (MFE) within window
    mfe_5d  REAL,  mfe_10d REAL,  mfe_20d REAL,  mfe_30d REAL,  mfe_40d REAL,
    -- max adverse excursion (MAE) within window
    mae_5d  REAL,  mae_10d REAL,  mae_20d REAL,  mae_30d REAL,  mae_40d REAL,
    -- hit labels (did MFE reach threshold within horizon?)
    hit_5pc_10d  INTEGER,
    hit_7pc_10d  INTEGER,
    hit_10pc_20d INTEGER,
    hit_15pc_20d INTEGER,
    hit_25pc_30d INTEGER,
    hit_40pc_40d INTEGER,
    -- failure label (MAE hit -7% within 10 days)
    failure_10d  INTEGER,
    PRIMARY KEY (symbol, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_falcon_outcomes_date ON falcon_outcomes(trade_date);
CREATE INDEX IF NOT EXISTS idx_falcon_outcomes_hit_10pc ON falcon_outcomes(hit_10pc_20d);
"""


def ensure_schema(con: sqlite3.Connection):
    con.executescript(SCHEMA)
    con.commit()


HORIZONS = (5, 10, 20, 30, 40)


def _label_one_symbol(args) -> int:
    """Compute and store outcomes for one symbol. Returns rows written."""
    symbol, db_path = args
    con = sqlite3.connect(db_path, timeout=60.0)
    rows = con.execute("""
        SELECT trade_date, open, high, low, close
        FROM ohlc_daily WHERE symbol = ? ORDER BY trade_date
    """, (symbol,)).fetchall()
    if len(rows) < max(HORIZONS) + 2:
        con.close(); return 0

    bars = [{"date": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4]}
              for r in rows]

    payload = []
    n = len(bars)
    for i in range(n - max(HORIZONS) - 1):
        # Entry: next bar's open
        entry = bars[i + 1]
        if entry["open"] <= 0: continue
        ep = entry["open"]

        outcomes = {}
        # Pre-compute running MFE/MAE/ret for each horizon
        # Walk forward day-by-day so we can update MFE/MAE incrementally
        running_high = entry["high"]
        running_low  = entry["low"]
        for h in HORIZONS:
            if i + h >= n: continue
            # Bars [i+1 .. i+h] inclusive (h bars)
            window = bars[i + 1: i + 1 + h]
            high_max = max(b["high"] for b in window)
            low_min  = min(b["low"]  for b in window)
            close_h  = window[-1]["close"]
            outcomes[f"ret_{h}d"] = round((close_h / ep - 1) * 100, 4)
            outcomes[f"mfe_{h}d"] = round((high_max / ep - 1) * 100, 4)
            outcomes[f"mae_{h}d"] = round((low_min  / ep - 1) * 100, 4)

        # Skip bar if any horizon couldn't be computed
        if any(f"ret_{h}d" not in outcomes for h in HORIZONS):
            continue

        # Hit labels
        hit_5_10  = 1 if outcomes["mfe_10d"] >=  5 else 0
        hit_7_10  = 1 if outcomes["mfe_10d"] >=  7 else 0
        hit_10_20 = 1 if outcomes["mfe_20d"] >= 10 else 0
        hit_15_20 = 1 if outcomes["mfe_20d"] >= 15 else 0
        hit_25_30 = 1 if outcomes["mfe_30d"] >= 25 else 0
        hit_40_40 = 1 if outcomes["mfe_40d"] >= 40 else 0
        fail_10   = 1 if outcomes["mae_10d"] <= -7 else 0

        payload.append((
            symbol, bars[i]["date"],
            outcomes["ret_5d"],  outcomes["ret_10d"],  outcomes["ret_20d"],  outcomes["ret_30d"],  outcomes["ret_40d"],
            outcomes["mfe_5d"],  outcomes["mfe_10d"],  outcomes["mfe_20d"],  outcomes["mfe_30d"],  outcomes["mfe_40d"],
            outcomes["mae_5d"],  outcomes["mae_10d"],  outcomes["mae_20d"],  outcomes["mae_30d"],  outcomes["mae_40d"],
            hit_5_10, hit_7_10, hit_10_20, hit_15_20, hit_25_30, hit_40_40,
            fail_10,
        ))

    if payload:
        con.executemany("""
            INSERT OR REPLACE INTO falcon_outcomes
              (symbol, trade_date,
               ret_5d, ret_10d, ret_20d, ret_30d, ret_40d,
               mfe_5d, mfe_10d, mfe_20d, mfe_30d, mfe_40d,
               mae_5d, mae_10d, mae_20d, mae_30d, mae_40d,
               hit_5pc_10d, hit_7pc_10d, hit_10pc_20d, hit_15pc_20d,
               hit_25pc_30d, hit_40pc_40d, failure_10d)
            VALUES (?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?,?, ?)
        """, payload)
        con.commit()
    con.close()
    return len(payload)


def label_universe(db_path: Path, symbols: List[str], n_workers: int = 16) -> Dict:
    n_workers = max(10, min(48, n_workers))
    con = sqlite3.connect(db_path, timeout=60.0)
    ensure_schema(con)
    con.close()

    print(f"[outcomes] Labeling {len(symbols)} symbols with {n_workers} workers ...")
    summary = {"done": 0, "rows_total": 0}
    args_list = [(s, str(db_path)) for s in symbols]
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_label_one_symbol, a): a[0] for a in args_list}
        for f in as_completed(futs):
            sym = futs[f]
            try:
                rows = f.result()
            except Exception as e:
                print(f"  [{sym}] ERROR: {e}", flush=True); rows = 0
            summary["done"] += 1
            summary["rows_total"] += rows
            if summary["done"] % 50 == 0:
                print(f"  [{summary['done']}/{len(symbols)}] rows so far={summary['rows_total']:,}",
                       flush=True)
    return summary
