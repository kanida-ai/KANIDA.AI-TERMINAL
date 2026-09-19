"""Convert kanida.db (SQLite) -> Parquet partitioned by symbol, ready to upload to S3.
   Produces the canonical schema data.py expects: symbol,date,open,high,low,close,volume.
Run:  python convert_to_parquet.py --db ../db/kanida.db --out ./parquet/daily --table ohlc_daily
Then: aws s3 sync ./parquet/daily s3://your-bucket/kanida/daily/
Set:  AGENT_DATA_URI=s3://your-bucket/kanida/daily/   (or the local ./parquet/daily dir)
"""
import argparse, sqlite3
from pathlib import Path
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True); ap.add_argument("--out", required=True)
    ap.add_argument("--table", default="ohlc_daily")
    a = ap.parse_args()
    con = sqlite3.connect("file:" + a.db.replace("\\", "/") + "?mode=ro", uri=True)
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    syms = [r[0] for r in con.execute(f"SELECT DISTINCT symbol FROM {a.table}").fetchall()]
    for i, s in enumerate(syms, 1):
        df = pd.read_sql_query(
            f"SELECT symbol, substr(bar_time,1,10) date, open,high,low,close,volume "
            f"FROM {a.table} WHERE symbol=? ORDER BY bar_time", con, params=(s,))
        if df.empty: continue
        d = out / f"symbol={s.replace('/', '_')}"; d.mkdir(exist_ok=True)
        df.to_parquet(d / "data.parquet", index=False)
        if i % 100 == 0: print(f"  {i}/{len(syms)}")
    con.close(); print(f"done -> {out}  ({len(syms)} symbols)")

if __name__ == "__main__":
    main()
