"""Extract fired patterns + feature values for AEGISLOG/MSUMI on 2026-06-25. Read-only."""
import sqlite3, json
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "db" / "kanida_universe.db"
c = sqlite3.connect(str(DB))

for sym in ("AEGISLOG", "MSUMI"):
    print(f"\n############### {sym} ###############")
    row = c.execute("""SELECT rank, score, n_fires, fired_pattern_ids, sample_rules, engine_version
                       FROM falcon_signals_live WHERE signal_date='2026-06-25' AND symbol=?""",
                    (sym,)).fetchone()
    rank, score, nf, fpi, srules, ev = row
    print(f"rank={rank} score={score:.2f} n_fires={nf} avg_lift={score/nf:.3f} engine={ev}")
    print(f"sample_rules (raw): {str(srules)[:500]}")
    ids = []
    if fpi:
        try:
            ids = json.loads(fpi) if fpi.strip().startswith("[") else [int(x) for x in fpi.split(",") if x.strip()]
        except Exception:
            ids = [int(x) for x in str(fpi).replace("[", "").replace("]", "").split(",") if x.strip().isdigit()]
    print(f"fired_pattern_ids: count={len(ids)} first15={ids[:15]}")

    # full parsed sample_rules (the stored top-5 fired patterns)
    print("\n  STORED TOP-5 FIRED PATTERNS (sample_rules):")
    try:
        for p in json.loads(srules):
            print(f"   #{p.get('pattern_id')} target={p.get('target')} oos_lift={p.get('oos_lift')}pp")
            print(f"       rule: {p.get('rule')}")
    except Exception as e:
        print("   parse err:", e)
    # taxonomy enrichment (only columns that exist in slim)
    taxcols = [r[1] for r in c.execute("PRAGMA table_info(falcon_pattern_taxonomy)")]
    want = [x for x in ("pattern_id", "lift_pp", "oos_hit_rate", "regime", "mined_year",
                        "english", "rule_text", "intraday_suitable", "swing_suitable") if x in taxcols]
    if ids:
        ph = ",".join("?" * len(ids))
        tax = c.execute(f"SELECT {','.join(want)} FROM falcon_pattern_taxonomy "
                        f"WHERE pattern_id IN ({ph})", ids).fetchall()
        print(f"\n  TAXONOMY for the 5 sample patterns (cols={want}):")
        for t in tax:
            print("   ", dict(zip(want, t)))

    # feature values used (the engine inputs)
    fcols = [r[1] for r in c.execute("PRAGMA table_info(falcon_features)")]
    fr = c.execute("SELECT * FROM falcon_features WHERE symbol=? AND trade_date='2026-06-25'",
                   (sym,)).fetchone()
    print("\n  KEY falcon_features (2026-06-25):")
    keymap = dict(zip(fcols, fr))
    for k in ("range_pct", "close_loc", "gap_pct", "body_pct", "rsi_14", "roc_5", "roc_20", "roc_60",
              "vol_vs_20d", "vol_5d_vs_20d", "dist_high_20", "dist_high_60", "dist_high_252",
              "weekly_close_loc", "weekly_breakout_20w", "rs_sector_20d", "rs_market_20d",
              "dist_sma_20", "dist_sma_50", "dist_sma_200", "atr_20_pct"):
        if k in keymap:
            print(f"     {k:22}: {keymap[k]}")
c.close()
