"""RUN MANIFEST — every number the arena emits carries the method that produced it.
Root cause of "the numbers changed and I don't know which to trust": several builders drifted to different
standards with nothing recording which. This stamps each output table with method version, the leakage rule,
the out-of-sample rule, costs, window, the data snapshot, and a hash of the code that produced it.
Any two numbers become instantly comparable, and a table built under an old method identifies itself as STALE.
All timestamps are IST (Asia/Kolkata), computed explicitly — the host clock is not IST."""
import os, sqlite3, hashlib
from datetime import datetime, timezone, timedelta
import pandas as pd

# ---- THE CURRENT STANDARD. Bump METHOD_VERSION whenever the methodology changes. ----
METHOD_VERSION = "2.0"
METHOD_NAME    = "leakfree-trueoos"
LEAK_FIX       = "weekly_* features fire ONLY on the last session of the ISO week"
OOS_RULE       = "scored only on dates AFTER each pattern's mined_year (true OOS)"
COST_PCT       = 0.15
WINDOW         = "2025-01-01..2026-07-31"

AP  = os.path.dirname(os.path.abspath(__file__))
MDB = os.path.join(AP, "arena_metrics.db")
UDB = os.path.join(os.path.dirname(os.path.dirname(AP)), "data", "db", "kanida_universe.db")
IST = timezone(timedelta(hours=5, minutes=30))


def ist_now() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def code_hash(path: str) -> str:
    try:
        return hashlib.sha256(open(path, "rb").read()).hexdigest()[:12]
    except Exception:
        return "unknown"


def data_snapshot() -> str:
    """Latest bar in the source price data — pins WHICH data produced the number."""
    try:
        c = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
        d = c.execute("SELECT MAX(trade_date) FROM ohlc_daily").fetchone()[0]
        c.close(); return d or "unknown"
    except Exception:
        return "unknown"


def stamp(script_file, tables, rows=None, oos=OOS_RULE, leak=LEAK_FIX,
          cost=COST_PCT, window=WINDOW, notes="", quiet=False):
    """Record how `tables` were produced. Call at the END of a builder, after the writes."""
    if isinstance(tables, str): tables = [tables]
    now, snap = ist_now(), data_snapshot()
    ch, script = code_hash(script_file), os.path.basename(script_file)
    recs = [dict(table_name=t, script=script, method_version=METHOD_VERSION, method_name=METHOD_NAME,
                 leak_fix=leak, oos_rule=oos, cost_pct=cost, window=window, data_snapshot=snap,
                 code_hash=ch, rows=(int(rows) if rows is not None else None), notes=notes,
                 built_at_ist=now) for t in tables]
    con = sqlite3.connect(MDB)
    try:
        ex = pd.read_sql_query("SELECT * FROM run_manifest", con)
        ex = ex[~ex.table_name.isin(tables)]
    except Exception:
        ex = pd.DataFrame()
    new = pd.DataFrame(recs)
    pd.concat([ex, new], ignore_index=True).to_sql("run_manifest", con, if_exists="replace", index=False)
    con.close()
    if not quiet:
        print(f"\n🧾 MANIFEST v{METHOD_VERSION} ({METHOD_NAME}) · {script} @ {ch}")
        print(f"   tables      : {', '.join(tables)}" + (f"  ({rows} rows)" if rows is not None else ""))
        print(f"   leak rule   : {leak}")
        print(f"   OOS rule    : {oos}")
        print(f"   cost/window : {cost}%/trade · {window}")
        print(f"   data through: {snap} · built {now} IST")


def read_manifest():
    try:
        con = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
        m = pd.read_sql_query("SELECT * FROM run_manifest", con); con.close()
        m["stale"] = (m.method_version != METHOD_VERSION).astype(int)
        return m
    except Exception:
        return pd.DataFrame()


if __name__ == "__main__":
    m = read_manifest()
    if m.empty:
        print("No manifest yet — run the builders."); raise SystemExit
    print(f"CURRENT STANDARD: v{METHOD_VERSION} ({METHOD_NAME})")
    print(f"  leak: {LEAK_FIX}\n  oos : {OOS_RULE}\n  cost: {COST_PCT}%/trade · window {WINDOW}\n")
    cols = ["table_name", "script", "method_version", "rows", "data_snapshot", "built_at_ist", "stale"]
    print(m[cols].sort_values("table_name").to_string(index=False))
    n = int(m.stale.sum())
    print(f"\n{'⚠ ' if n else '✅ '}{n} stale table(s)" + (" — rebuild them" if n else " — all tables on the current standard"))
