"""Full coverage cross-check across daily / 5-min / 1-min for every instrument in
kanida.db. Classifies each instrument x timeframe as COMPLETE / PARTIAL / MISSING
by set-difference of intraday trading-days vs the instrument's own daily series.
Separates STOCKS from INDICES (many NSE strategy indices have no intraday feed).
Writes a detailed report to db/coverage_report.txt and prints a summary.
"""
import sqlite3
from pathlib import Path

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
OUT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\coverage_report.txt")
con = sqlite3.connect(str(DB))

# labels: type per symbol
labels = {r[0]: r[1] for r in con.execute("SELECT symbol, instrument_type FROM instrument_labels").fetchall()}

def per_symbol(table):
    """symbol -> (rows, distinct_days, min_bar, max_bar)"""
    q = (f"SELECT symbol, COUNT(*), COUNT(DISTINCT substr(bar_time,1,10)), "
         f"MIN(bar_time), MAX(bar_time) FROM {table} GROUP BY symbol")
    return {r[0]: (r[1], r[2], r[3], r[4]) for r in con.execute(q).fetchall()}

print("scanning daily...");  D = per_symbol("ohlc_daily")
print("scanning 5min...");   F = per_symbol("ohlc_5min")
print("scanning 1min...");   M = per_symbol("ohlc_1min")

# distinct day SETS for gap detection (only where needed)
def day_set(table, sym):
    return {r[0] for r in con.execute(
        f"SELECT DISTINCT substr(bar_time,1,10) FROM {table} WHERE symbol=?", (sym,)).fetchall()}

all_syms = sorted(set(D) | set(F) | set(M) | set(labels))
lines = []
def w(s): lines.append(s);

w("=" * 90)
w("KANIDA.DB COVERAGE CROSS-CHECK  —  daily / 5-min / 1-min")
w("=" * 90)

# headline
for tf, mp in [("daily", D), ("5-min", F), ("1-min", M)]:
    rows = sum(v[0] for v in mp.values())
    spans = [v[2][:10] for v in mp.values() if v[2]] + [v[3][:10] for v in mp.values() if v[3]]
    w(f"{tf:6}: {len(mp):>4} instruments | {rows:>13,} rows | span {min(spans)} .. {max(spans)}")

# classify
cats = {"stock": {"complete": [], "partial": [], "no_intraday": [], "no_daily": []},
        "index": {"complete": [], "partial": [], "no_intraday": [], "no_daily": []}}
detail = []
for s in all_syms:
    typ = labels.get(s, "STOCK").lower()
    typ = "index" if typ == "index" else "stock"
    if s not in D:
        cats[typ]["no_daily"].append(s); detail.append((s, typ, "NO_DAILY", "", "")); continue
    daily_days = day_set("ohlc_daily", s)
    stat = {}
    for tf, table in [("5min", "ohlc_5min"), ("1min", "ohlc_1min")]:
        if s not in (F if tf == "5min" else M):
            stat[tf] = ("no_intraday", len(daily_days)); continue
        idays = day_set(table, s)
        miss = daily_days - idays
        stat[tf] = ("complete" if not miss else "partial", len(miss))
    # overall classification by the stricter of the two intraday tfs
    s5, m5 = stat["5min"]; s1, m1 = stat["1min"]
    if s5 == "complete" and s1 == "complete":
        cats[typ]["complete"].append(s)
    elif s5 == "no_intraday" and s1 == "no_intraday":
        cats[typ]["no_intraday"].append(s)
    else:
        cats[typ]["partial"].append((s, m5, m1))
    detail.append((s, typ, f"5min:{s5}({m5})", f"1min:{s1}({m1})", ""))

for typ in ("stock", "index"):
    c = cats[typ]
    w("")
    w(f"--- {typ.upper()}S ---")
    w(f"  fully COMPLETE (5min & 1min, 0 missing days): {len(c['complete'])}")
    w(f"  PARTIAL (some intraday days missing):         {len(c['partial'])}")
    w(f"  daily-only (NO intraday from Kite):           {len(c['no_intraday'])}")
    w(f"  missing daily entirely:                       {len(c['no_daily'])}")
    if c["no_daily"]:
        w(f"    no_daily: {c['no_daily']}")
    if c["partial"]:
        w(f"    PARTIAL detail (symbol, 5min_missing_days, 1min_missing_days):")
        for s, m5, m1 in sorted(c["partial"], key=lambda x: -(x[2])):
            w(f"      {s:22} 5min_miss={m5:<5} 1min_miss={m1}")
    if typ == "index" and c["no_intraday"]:
        w(f"    daily-only indices ({len(c['no_intraday'])}): {c['no_intraday']}")

OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("\n".join(lines))
print(f"\n[written to {OUT}]")
con.close()
