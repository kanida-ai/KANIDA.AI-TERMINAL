"""ADDENDUM F — DATA GENERATION (the gating dependency for Agents 1-5).
Materialise Falcon's FULL daily ranking (rank 1 -> ~500), 2022-2026, via the bit-exact read-only replay
(scripts/falcon_signal_replay.rank_for_date at top_n=600, min_fires=0). Enrich each (date,symbol) with metadata
(rank, n_fires, score, avg_lift, tier, entry_context, sector, regime) + next-day intraday return (09:15->EOD) and
forward 1/3/5/10-day returns. Write to a NEW table in a NEW DB (data/db/falcon_research.db) — the Falcon engine and
its prod tables are NEVER touched. Correction-2 compliant: full depth, not Top-10."""
import os, sqlite3, importlib.util
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
FEAT_DB = os.path.join(ROOT, "data", "db", "kanida_universe.db")           # falcon_features, patterns, sectors (read-only)
OHLC_DB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")  # ohlc_daily (read-only)
OUT_DB = os.path.join(ROOT, "data", "db", "falcon_research.db")            # NEW research DB (writes here only)
W0, W1 = "2022-01-01", "2026-07-31"
spec = importlib.util.spec_from_file_location("fsr", os.path.join(ROOT, "scripts", "falcon_signal_replay.py"))
fsr = importlib.util.module_from_spec(spec); spec.loader.exec_module(fsr)

def entry_context(gap, dist_hi20, roc20, rng, cloc):
    if gap == gap and abs(gap) >= 4.5: return "circuit" if abs(gap) >= 9 else "gap"
    if dist_hi20 == dist_hi20 and dist_hi20 >= -0.01 and (cloc or 0) > 0.6: return "breakout"
    if roc20 == roc20 and roc20 >= 15: return "extended_move"
    return "normal"

def tier_of(avg_lift, n_fires):
    if avg_lift >= 15 and n_fires >= 20: return "PREMIUM"
    if avg_lift >= 10 and n_fires >= 10: return "GOLD"
    if n_fires >= 5: return "STANDARD"
    return "TAIL"

if __name__ == "__main__":
    fc = sqlite3.connect("file:" + FEAT_DB.replace("\\", "/") + "?mode=ro", uri=True)
    oc = sqlite3.connect("file:" + OHLC_DB.replace("\\", "/") + "?mode=ro", uri=True)
    pats = fsr.load_patterns(fc); print(f"patterns: {len(pats)}", flush=True)
    sectors = {r[0]: r[1] for r in fc.execute("SELECT symbol, sector FROM falcon_sectors")}
    days = [r[0] for r in fc.execute("SELECT DISTINCT trade_date FROM falcon_features WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date", (W0, W1))]
    # trading calendar + per-symbol open/close from ohlc_daily (for next-day & forward returns)
    od = pd.read_sql_query("SELECT symbol, trade_date, open, close FROM ohlc_daily WHERE trade_date BETWEEN ? AND ?", oc, params=(W0, "2026-12-31"))
    cal = sorted(od.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
    opx = od.pivot_table(index="trade_date", columns="symbol", values="open")
    cpx = od.pivot_table(index="trade_date", columns="symbol", values="close")
    print(f"days to rank: {len(days)} | calendar {len(cal)} days | symbols {cpx.shape[1]}", flush=True)

    fcols = ["gap_pct", "dist_high_20", "roc_20", "range_pct", "close_loc", "dist_sma_20"]
    rows = []
    for k, sd in enumerate(days):
        rk = fsr.rank_for_date(fc, pats, sd, min_fires=0, top_n=600)
        if not rk: continue
        feats = {r[0]: r[1:] for r in fc.execute(f"SELECT symbol,{','.join(fcols)} FROM falcon_features WHERE trade_date=?", (sd,))}
        breadth = np.mean([1 if (v[5] is not None and v[5] > 0) else 0 for v in feats.values()]) if feats else 0.5
        regime = "risk_on" if breadth > 0.55 else "risk_off" if breadth < 0.45 else "neutral"
        ei = cidx.get(sd, -99) + 1
        entry_date = cal[ei] if 0 <= ei < len(cal) else None
        for c in rk:
            s = c["symbol"]; f = feats.get(s, (None,) * 6)
            ec = entry_context(f[0], f[1], f[2], f[3], f[4])
            nd_o = opx.at[entry_date, s] if entry_date in opx.index and s in opx.columns else np.nan
            nd_c = cpx.at[entry_date, s] if entry_date in cpx.index and s in cpx.columns else np.nan
            nd_ret = (nd_c / nd_o - 1) * 100 if nd_o and nd_o == nd_o and nd_c == nd_c else np.nan
            fwd = {}
            if entry_date and s in cpx.columns:
                ej = cidx.get(entry_date, -99)
                for h in (1, 3, 5, 10):
                    d2 = cal[ej + h] if 0 <= ej + h < len(cal) else None
                    fwd[h] = (cpx.at[d2, s] / nd_c - 1) * 100 if d2 in cpx.index and nd_c == nd_c else np.nan
            rows.append((sd, entry_date, c["rank"], s, sectors.get(s, "NA"), c["n_fires"], round(c["score"], 3),
                         c["avg_lift"], tier_of(c["avg_lift"], c["n_fires"]), ec, regime,
                         round(nd_ret, 3) if nd_ret == nd_ret else None,
                         *[round(fwd.get(h, np.nan), 3) if fwd.get(h, np.nan) == fwd.get(h, np.nan) else None for h in (1, 3, 5, 10)]))
        if (k + 1) % 100 == 0: print(f"  {k+1}/{len(days)} days ranked, {len(rows):,} rows", flush=True)
    fc.close(); oc.close()

    out = sqlite3.connect(OUT_DB)
    out.execute("DROP TABLE IF EXISTS falcon_full_ranking")
    out.execute("""CREATE TABLE falcon_full_ranking (signal_date TEXT, entry_date TEXT, rank INT, symbol TEXT,
        sector TEXT, n_fires INT, score REAL, avg_lift REAL, tier TEXT, entry_context TEXT, regime TEXT,
        nd_intraday_ret REAL, fwd1 REAL, fwd3 REAL, fwd5 REAL, fwd10 REAL)""")
    out.executemany("INSERT INTO falcon_full_ranking VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    out.execute("CREATE INDEX ix_sd ON falcon_full_ranking(signal_date)"); out.execute("CREATE INDEX ix_rk ON falcon_full_ranking(rank)")
    out.commit()
    n = out.execute("SELECT COUNT(*) FROM falcon_full_ranking").fetchone()[0]
    sp = out.execute("SELECT MIN(signal_date),MAX(signal_date),COUNT(DISTINCT signal_date),MAX(rank) FROM falcon_full_ranking").fetchone()
    print(f"\nWROTE {n:,} rows -> {OUT_DB} :: falcon_full_ranking")
    print(f"  span {sp[0]}..{sp[1]} | {sp[2]} days | max depth {sp[3]}")
    print("  by tier:", dict(out.execute("SELECT tier,COUNT(*) FROM falcon_full_ranking GROUP BY tier").fetchall()))
    print("  by entry_context:", dict(out.execute("SELECT entry_context,COUNT(*) FROM falcon_full_ranking GROUP BY entry_context").fetchall()))
    print("  sample Top-3 rows 2025-03-17:")
    for r in out.execute("SELECT rank,symbol,tier,entry_context,regime,nd_intraday_ret,fwd5 FROM falcon_full_ranking WHERE signal_date='2025-03-17' ORDER BY rank LIMIT 3"): print("   ", r)
    out.close()
