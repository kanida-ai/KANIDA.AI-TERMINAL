"""Deep probe of the SLIM (live) DB for the June-25 Falcon audit. Read-only."""
import sqlite3
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "db" / "kanida_universe.db"
c = sqlite3.connect(str(DB))


def cols(t):
    return [r[1] for r in c.execute(f"PRAGMA table_info({t})")]


print("=== falcon_signals_live cols ===")
print(cols("falcon_signals_live"))
print("\n=== 2026-06-25 Top-12 by rank ===")
rows = c.execute("""SELECT rank, symbol, sector, n_fires, score, close_at_signal
                    FROM falcon_signals_live WHERE signal_date='2026-06-25'
                    ORDER BY rank LIMIT 12""").fetchall()
for r in rows:
    print("  ", r)
print(f"  (total rows on 2026-06-25: "
      f"{c.execute(chr(39).join(['SELECT count(*) FROM falcon_signals_live WHERE signal_date=','2026-06-25',''])).fetchone()[0]})")

print("\n=== falcon_top10_audit cols ===")
print(cols("falcon_top10_audit"))
print("\n=== falcon_top10_audit rows for 2026-06-25 (AEGISLOG, MSUMI) ===")
for sym in ("AEGISLOG", "MSUMI"):
    r = c.execute("SELECT * FROM falcon_top10_audit WHERE signal_date='2026-06-25' AND symbol=?",
                  (sym,)).fetchone()
    print(f"\n  --- {sym} ---")
    if r:
        for k, v in zip(cols("falcon_top10_audit"), r):
            sv = str(v)
            print(f"     {k:28}: {sv[:200]}")
    else:
        print("     (no row)")

print("\n=== falcon_features in slim: coverage + AEGISLOG/MSUMI 06-25 ===")
try:
    print("  max trade_date:", c.execute("SELECT max(trade_date) FROM falcon_features").fetchone()[0])
    for sym in ("AEGISLOG", "MSUMI"):
        n = c.execute("SELECT count(*) FROM falcon_features WHERE symbol=? AND trade_date='2026-06-25'", (sym,)).fetchone()[0]
        print(f"  {sym} 06-25 feature row: {n}")
except Exception as e:
    print("  no falcon_features:", e)

print("\n=== pattern tables present? ===")
for t in ("falcon_pattern_taxonomy", "falcon_promoted_patterns", "falcon_pattern_candidates"):
    try:
        n = c.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        print(f"  {t}: {n} rows")
    except Exception as e:
        print(f"  {t}: MISSING ({e})")

print("\n=== ohlc_daily slim: AEGISLOG/MSUMI last 3 days ===")
for sym in ("AEGISLOG", "MSUMI"):
    rr = c.execute("SELECT trade_date,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? "
                   "ORDER BY trade_date DESC LIMIT 3", (sym,)).fetchall()
    print(f"  {sym}: {rr}")
c.close()
