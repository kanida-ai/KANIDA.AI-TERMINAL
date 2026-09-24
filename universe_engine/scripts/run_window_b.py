"""
Window-B replication of the IAS multi-TF analysis.

Pulls 1m bars for an explicit historical window (default 2024-11-01 → 2025-04-30)
into a SEPARATE DB so it cannot contaminate the existing Window-A run, then
resamples and runs the same multi-TF IAS analyzer.

Goal: test whether the inverted-IAS finding from Window A (Nov 2025-Apr 2026)
holds in a non-overlapping period (Nov 2024-Apr 2025). If sign holds → finding
is robust. If sign flips → regime-conditional.

Usage:
    python scripts/run_window_b.py                       # default Nov 2024 - Apr 2025
    python scripts/run_window_b.py --start 2024-11-01 --end 2025-04-30
    python scripts/run_window_b.py --skip-fetch          # only resample / analyze
"""
from __future__ import annotations
import argparse, json, sqlite3, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.data_fetch import fetch_1m_for_symbols
from engine.resample   import ensure_intraday_schema, resample_all_symbols
from engine.ias_multi_tf import run_multi_tf

MAIN_DB   = ROOT / "data" / "db" / "kanida_universe.db"
WINB_DB   = ROOT / "data" / "db" / "kanida_universe_winB.db"
REPORT    = ROOT / "reports" / "IAS_MULTI_TF_WINB_REPORT.md"
COMPARE   = ROOT / "reports" / "IAS_REPLICATION_COMPARE.md"
RESULTS_A = ROOT / "scripts" / "_ias_multi_tf_results.json"
RESULTS_B = ROOT / "scripts" / "_ias_multi_tf_results_winB.json"


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS universe_master (
    symbol           TEXT PRIMARY KEY,
    exchange         TEXT NOT NULL DEFAULT 'NSE',
    sector           TEXT,
    in_nifty50       INTEGER NOT NULL DEFAULT 0,
    in_nifty100      INTEGER NOT NULL DEFAULT 0,
    in_nifty200      INTEGER NOT NULL DEFAULT 0,
    in_nifty500      INTEGER NOT NULL DEFAULT 0,
    is_active        INTEGER NOT NULL DEFAULT 1,
    effective_from   TEXT,
    effective_to     TEXT,
    notes            TEXT
);
CREATE TABLE IF NOT EXISTS ohlc_1min (
    symbol TEXT NOT NULL, bar_time TEXT NOT NULL,
    open REAL, high REAL, low REAL, close REAL, volume INTEGER,
    PRIMARY KEY (symbol, bar_time)
);
CREATE INDEX IF NOT EXISTS idx_ohlc1_t ON ohlc_1min(bar_time);
"""


def init_winb_db():
    WINB_DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(WINB_DB, timeout=60.0)
    con.executescript(SCHEMA_SQL)
    ensure_intraday_schema(con)   # adds 5/15/30min tables

    # Mirror universe_master from main DB so analyzer sees the same symbols
    n = con.execute("SELECT COUNT(*) FROM universe_master").fetchone()[0]
    if n == 0:
        src = sqlite3.connect(MAIN_DB)
        src.row_factory = sqlite3.Row
        rows = src.execute("SELECT * FROM universe_master").fetchall()
        cols = [d[0] for d in src.execute("SELECT * FROM universe_master LIMIT 1").description]
        src.close()
        placeholders = ",".join("?" for _ in cols)
        con.executemany(f"INSERT INTO universe_master ({','.join(cols)}) VALUES ({placeholders})",
                        [tuple(r[c] for c in cols) for r in rows])
        con.commit()
        print(f"  Mirrored {len(rows)} symbols from {MAIN_DB.name} -> {WINB_DB.name}")
    con.close()


def get_universe(index_col: str = "in_nifty200"):
    con = sqlite3.connect(WINB_DB)
    rows = con.execute(f"""
        SELECT symbol FROM universe_master
        WHERE is_active = 1 AND {index_col} = 1 ORDER BY symbol
    """).fetchall()
    con.close()
    return [r[0] for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-11-01")
    ap.add_argument("--end",   default="2025-04-30")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--rps", type=float, default=5.0)
    ap.add_argument("--index", default="in_nifty200")
    ap.add_argument("--skip-fetch", action="store_true")
    ap.add_argument("--skip-resample", action="store_true")
    ap.add_argument("--skip-analyze", action="store_true")
    args = ap.parse_args()

    print("=" * 70)
    print(f"Window-B IAS replication  |  {args.start}  →  {args.end}")
    print("=" * 70)

    print("\n[1/4] Init Window-B DB ...")
    init_winb_db()
    symbols = get_universe(args.index)
    print(f"  Universe: {len(symbols)} symbols ({args.index})")

    if not args.skip_fetch:
        print("\n[2/4] Fetching 1m bars from Kite ...")
        fetch_1m_for_symbols(WINB_DB, symbols,
                             n_workers=args.workers, rps=args.rps,
                             start_date=args.start, end_date=args.end)
    else:
        print("\n[2/4] Skipping fetch (--skip-fetch)")

    if not args.skip_resample:
        print("\n[3/4] Resampling 1m → 5m / 15m / 30m ...")
        resample_all_symbols(WINB_DB, symbols, tfs_min=(5, 15, 30),
                             n_workers=args.workers)
    else:
        print("\n[3/4] Skipping resample (--skip-resample)")

    # Quick coverage sanity
    con = sqlite3.connect(WINB_DB)
    print("\n[Coverage in Window-B DB]")
    for tbl in ("ohlc_1min", "ohlc_5min", "ohlc_15min", "ohlc_30min"):
        cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        rng = con.execute(f"SELECT MIN(bar_time), MAX(bar_time) FROM {tbl}").fetchone()
        nsym = con.execute(f"SELECT COUNT(DISTINCT symbol) FROM {tbl}").fetchone()[0]
        print(f"  {tbl:<12} {cnt:>10,} bars · {nsym} sym · {rng[0]} → {rng[1]}")
    con.close()

    if args.skip_analyze:
        print("\n[4/4] Skipping analyze (--skip-analyze)")
        return

    print("\n[4/4] Running multi-TF IAS analysis on Window-B DB ...")
    tfs = ["1min", "5min", "15min", "30min"]
    results_b = run_multi_tf(WINB_DB, tfs, n_workers=args.workers)

    RESULTS_B.write_text(json.dumps(results_b, indent=1, default=str))
    print(f"\n  Window-B results JSON → {RESULTS_B}")

    # Re-use the report builder from run_ias_multi_tf (scripts/ has no __init__.py)
    sys.path.insert(0, str(ROOT / "scripts"))
    from run_ias_multi_tf import build_md
    md = build_md(results_b)
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(md, encoding="utf-8")
    print(f"  Window-B report      → {REPORT}")

    # Side-by-side comparison
    if RESULTS_A.exists():
        results_a = json.loads(RESULTS_A.read_text())
        write_compare(results_a, results_b, args.start, args.end)
    else:
        print(f"  (No Window-A results JSON at {RESULTS_A} — skipping compare)")


def fmt_p(p):
    if p is None: return "—"
    if p < 0.001: return f"{p:.2e}"
    return f"{p:.4f}"


def write_compare(A: dict, B: dict, b_start: str, b_end: str):
    L = ["# IAS Replication — Window A vs Window B", ""]
    L.append("**Window A** (original): 2025-10-28 → 2026-04-30")
    L.append(f"**Window B** (replication): {b_start} → {b_end}")
    L.append("")
    L.append("Identical universe (143 NSE F&O stocks), identical 13:45–15:29 IST window, "
             "identical IAS formula, identical signal threshold (IAS ≥ 5.5), "
             "identical 'up day' threshold (next-day close-to-close ≥ 1.0%).")
    L.append("")
    L.append("## Headline — does the IAS sign hold across windows?")
    L.append("")
    L.append("| TF | Window | Panel rows | Hit (sig) | Hit (no-sig) | Lift | t-stat | p | Cohen's d |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for tf in ("1min", "5min", "15min", "30min"):
        for label, R in (("A", A), ("B", B)):
            if tf not in R: continue
            r = R[tf]; t1 = r["test1_base_rates"]
            L.append(f"| {tf} | {label} | {r['panel_size']:,} | "
                     f"{t1['hit_signal']*100:.1f}% | {t1['hit_nosignal']*100:.1f}% | "
                     f"{t1['lift']:.2f} | {t1['t']:.2f} | {fmt_p(t1['p'])} | {t1['d']:.2f} |")
    L.append("")
    L.append("## Sign of Granger IAS coefficient")
    L.append("")
    L.append("| TF | A coef | A p | B coef | B p |")
    L.append("|---|---|---|---|---|")
    for tf in ("1min", "5min", "15min", "30min"):
        ga = A.get(tf, {}).get("test4_granger", {})
        gb = B.get(tf, {}).get("test4_granger", {})
        L.append(f"| {tf} | {ga.get('ias_coef','—')} | {fmt_p(ga.get('p'))} | "
                 f"{gb.get('ias_coef','—')} | {fmt_p(gb.get('p'))} |")
    L.append("")
    L.append("## Verdict guide")
    L.append("")
    L.append("- **Lift < 1 in BOTH windows AND Granger coef negative in BOTH** → finding is "
             "robust across regimes; high IAS systematically predicts weakness on this universe.")
    L.append("- **Lift flips above 1 in Window B** → regime-conditional; the inversion only "
             "applies in 2025-26 conditions, not a fundamental property.")
    L.append("- **Mixed (some TFs hold, others flip)** → effect is real but fragile; not safe "
             "to use as a standalone gate without further conditioning.")

    out = COMPARE
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(L), encoding="utf-8")
    print(f"  Compare report       → {out}")


if __name__ == "__main__":
    sys.exit(main() or 0)
