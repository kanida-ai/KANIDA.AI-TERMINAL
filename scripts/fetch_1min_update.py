"""Refresh ohlc_1min: fetch 1-min bars for the full pickable universe from
2026-05-01 to today, filling the post-2026-05-11 gap. Idempotent (INSERT OR IGNORE)."""
import sys, sqlite3
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
ENG = ROOT / "universe_engine"
sys.path.insert(0, str(ENG))
from engine.data_fetch import fetch_1m_for_symbols

DB = ENG / "data" / "db" / "kanida_universe.db"
ALIAS = {"ZOMATO": "ETERNAL"}
START, END = "2026-05-01", "2026-06-28"

con = sqlite3.connect(str(DB))
univ = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM ohlc_1min")]
study = [r[0] for r in con.execute(
    "SELECT DISTINCT symbol FROM falcon_signal_day_study "
    "WHERE persona='falcon_top10_daily' AND engine_rank<=10")]
con.close()
symbols = sorted(set(univ) | set(ALIAS.get(s, s) for s in study))
print(f"[*] universe {len(univ)}  study {len(study)}  union-to-fetch {len(symbols)}", flush=True)

summary = fetch_1m_for_symbols(DB, symbols, n_workers=16, rps=4.0,
                               start_date=START, end_date=END)
print(f"\n[*] FETCH DONE: symbols {summary['symbols_done']}, rows written {summary['rows_total']:,}, "
      f"auth_errors {summary['auth_errors']}")

# verify new coverage
con = sqlite3.connect(str(DB))
import pandas as pd
df = pd.read_sql_query("SELECT substr(max(bar_time),1,10) maxd, count(distinct symbol) n FROM ohlc_1min", con)
cov = pd.read_sql_query("SELECT symbol, substr(max(bar_time),1,10) m FROM ohlc_1min GROUP BY symbol", con)
con.close()
print(f"[*] ohlc_1min now: max date {df['maxd'].iloc[0]}, symbols {df['n'].iloc[0]}")
print(f"[*] symbols ending >= 2026-06-20: {(cov['m']>='2026-06-20').sum()} / {len(cov)}")
print(f"[*] symbols still ending <= 2026-05-12: {(cov['m']<='2026-05-12').sum()}")
