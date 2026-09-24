"""Refresh ohlc_1min to the latest trading day (idempotent). Fetches the union of:
  - the existing ohlc_1min universe (~500),
  - research signal stocks (falcon_signal_day_study Top-10),
  - recent LIVE signal stocks (falcon_signals_live Top-10, so the extension to current date
    has their 1-min too).
Window 2026-06-20 -> 2026-07-04 (covers the June-25 gap; INSERT OR IGNORE de-dupes overlap)."""
import sys, sqlite3
from pathlib import Path
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
ENG = ROOT / "universe_engine"
sys.path.insert(0, str(ENG)); sys.path.insert(0, str(ROOT / "backend"))
from engine.data_fetch import fetch_1m_for_symbols

DB = ENG / "data" / "db" / "kanida_universe.db"
ALIAS = {"ZOMATO": "ETERNAL"}
START, END = "2026-07-01", "2026-07-11"

con = sqlite3.connect(str(DB))
before = con.execute("SELECT max(substr(bar_time,1,10)) FROM ohlc_1min").fetchone()[0]
univ = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM ohlc_1min")]
study = [r[0] for r in con.execute(
    "SELECT DISTINCT symbol FROM falcon_signal_day_study "
    "WHERE persona='falcon_top10_daily' AND engine_rank<=10")]
con.close()
from falcon.db import falcon_conn
with falcon_conn() as pc:
    live = [r[0] for r in pc.execute(
        "SELECT DISTINCT symbol FROM falcon_signals_live WHERE rank<=10 AND signal_date>='2026-05-01'")]
    # durable Top-10 audit archive (retains history the live table has rotated out)
    audit = [r[0] for r in pc.execute(
        "SELECT DISTINCT symbol FROM falcon_top10_audit WHERE rank<=10 AND entry_date>='2026-06-20'")]

symbols = sorted(set(univ) | {ALIAS.get(s, s) for s in study}
                 | {ALIAS.get(s, s) for s in live} | {ALIAS.get(s, s) for s in audit})
print(f"[*] ohlc_1min currently ends {before}", flush=True)
print(f"[*] universe {len(univ)}  study {len(study)}  live {len(live)}  -> union to fetch {len(symbols)}", flush=True)
print(f"[*] fetching {START} -> {END} (idempotent) ...", flush=True)

s = fetch_1m_for_symbols(str(DB), symbols, n_workers=16, rps=4.0, start_date=START, end_date=END)
print(f"\n[*] DONE symbols_done={s['symbols_done']} rows_written={s['rows_total']:,} auth_errors={s['auth_errors']}")

con = sqlite3.connect(str(DB))
after = con.execute("SELECT max(substr(bar_time,1,10)) FROM ohlc_1min").fetchone()[0]
newdays = [r[0] for r in con.execute(
    "SELECT DISTINCT substr(bar_time,1,10) d FROM ohlc_1min WHERE d > ? ORDER BY d", (before,))]
cur = con.execute("SELECT COUNT(DISTINCT symbol) FROM ohlc_1min WHERE substr(bar_time,1,10)=?", (after,)).fetchone()[0]
con.close()
print(f"[*] ohlc_1min now ends {after}  (was {before})")
print(f"[*] new trading days added: {newdays}")
print(f"[*] symbols present on {after}: {cur}")
