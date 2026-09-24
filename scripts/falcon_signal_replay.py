"""
Faithful replay of the PRODUCTION Falcon signal engine (backend signal_runner.py)
over the historical feature panel in the LIVE/slim DB, to regenerate the true
live-equivalent Top-10 for any date. Read-only. Reproduces falcon_signals_live exactly.

Logic mirrors backend/falcon/services/{signal_runner,pattern_loader}.py.
"""
import json, sqlite3, argparse
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "db" / "kanida_universe.db"          # FALCON_DB (live/slim)

FEATURE_COLS = [
    "range_pct","close_loc","gap_pct","body_pct","upper_wick_pct","lower_wick_pct",
    "dist_sma_20","dist_sma_50","dist_sma_200","slope_sma_20","slope_sma_50",
    "rsi_14","roc_5","roc_20","roc_60",
    "vol_vs_20d","vol_5d_vs_20d","n_sub_75v_7d","n_sub_75v_20d",
    "atr_20_pct","atr_5_vs_20","n_sub_2_5_range_7d","n_sub_3_range_7d",
    "n_higher_lows_5d","n_higher_highs_5d",
    "dist_high_10","dist_high_20","dist_high_60","dist_high_120","dist_high_252",
    "weekly_close_vs_sma20","weekly_breakout_20w","weekly_range_pct","weekly_close_loc",
    "rs_sector_20d","rs_sector_60d","rs_market_20d","rs_market_60d",
]
FIDX = {c: i for i, c in enumerate(FEATURE_COLS)}
DROPPED_FAMILIES = {"drawdown_bounce"}


def classify_families(rule):
    fams = set()
    for f, op, th in rule:
        if f == "weekly_close_loc" and op == ">": fams.add("weekly_close")
        if f == "atr_20_pct" and op == ">": fams.add("high_atr")
        if f in ("dist_high_252", "dist_high_120") and op == "<=" and th < -10:
            fams.add("drawdown_bounce")
        if f == "weekly_range_pct" and op == ">": fams.add("weekly_range")
    return fams


def load_patterns(con):
    rows = con.execute("""
        SELECT c.pattern_id, c.mined_year, c.outcome_target, c.rule_json,
               p.classification, p.avg_oos_year_lift_pp
        FROM falcon_promoted_patterns p
        INNER JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
        WHERE p.classification IN ('universal','regime_dependent')
        ORDER BY p.avg_oos_year_lift_pp DESC""").fetchall()
    out = []
    for pid, my, tgt, rj, cls, lift in rows:
        rule = [(f, op, th) for f, op, th in json.loads(rj)]
        if DROPPED_FAMILIES & classify_families(rule):
            continue
        out.append({"pattern_id": pid, "mined_year": my, "target": tgt,
                    "rule": rule, "oos_lift": lift})
    return out


def rule_mask(rule, X):
    m = np.ones(X.shape[0], dtype=bool)
    for f, op, th in rule:
        idx = FIDX.get(f)
        if idx is None:
            return np.zeros(X.shape[0], dtype=bool)
        col = X[:, idx]
        if op == "<=":
            m &= (col <= th) & ~np.isnan(col)
        else:
            m &= (col > th) & ~np.isnan(col)
    return m


def rank_for_date(con, patterns, signal_date, min_fires=10, top_n=100):
    sel = ", ".join(FEATURE_COLS)
    rows = con.execute(f"SELECT symbol, {sel} FROM falcon_features WHERE trade_date=?",
                       (signal_date,)).fetchall()
    if not rows:
        return None
    syms = [r[0] for r in rows]
    X = np.full((len(syms), len(FEATURE_COLS)), np.nan)
    for i, r in enumerate(rows):
        X[i] = [v if v is not None else np.nan for v in r[1:]]
    yr = int(signal_date[:4])
    elig = [p for p in patterns if int(p["mined_year"]) < yr]
    fire = np.zeros(len(syms), dtype=np.int32)
    score = np.zeros(len(syms))
    for p in elig:
        m = rule_mask(p["rule"], X)
        if not m.any():
            continue
        fire += m.astype(np.int32)
        score += m.astype(np.float64) * p["oos_lift"]
    cands = [{"symbol": syms[i], "n_fires": int(fire[i]), "score": float(score[i])}
             for i in range(len(syms)) if fire[i] >= min_fires]
    cands.sort(key=lambda c: -c["score"])
    top = cands[:top_n]
    ranked = sorted(top, key=lambda c: -(c["score"] / max(c["n_fires"], 1)))
    for pos, c in enumerate(ranked, 1):
        c["rank"] = pos
        c["avg_lift"] = round(c["score"] / max(c["n_fires"], 1), 4)
    return ranked


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-06-25")
    ap.add_argument("--min-fires", type=int, default=10)
    args = ap.parse_args()
    con = sqlite3.connect(str(DB))
    pats = load_patterns(con)
    print(f"[*] patterns loaded (after drawdown_bounce drop): {len(pats)}")
    rk = rank_for_date(con, pats, args.date, args.min_fires)
    print(f"\n=== REPLAY top-10 for {args.date} (min_fires={args.min_fires}) ===")
    for c in rk[:10]:
        print(f"  rank {c['rank']:>2}  {c['symbol']:12} n_fires={c['n_fires']:>3} "
              f"score={c['score']:.2f} avg_lift={c['avg_lift']}")

    # compare to stored falcon_signals_live
    stored = {sym: (rank, nf, sc) for rank, sym, nf, sc in con.execute(
        "SELECT rank, symbol, n_fires, score FROM falcon_signals_live WHERE signal_date=?",
        (args.date,))}
    con.close()
    print(f"\n=== MATCH vs stored falcon_signals_live ===")
    repl_top10 = [c["symbol"] for c in rk[:10]]
    stored_top10 = [s for s, (r, _, _) in sorted(stored.items(), key=lambda kv: kv[1][0])][:10]
    exact = sum(1 for a, b in zip(repl_top10, stored_top10) if a == b)
    print(f"  replay top-10 : {repl_top10}")
    print(f"  stored top-10 : {stored_top10}")
    print(f"  exact position matches: {exact}/10   set overlap: {len(set(repl_top10)&set(stored_top10))}/10")
    for c in rk[:10]:
        st = stored.get(c["symbol"])
        if st:
            print(f"    {c['symbol']:12} replay(rank={c['rank']},nf={c['n_fires']},score={c['score']:.1f}) "
                  f"vs stored(rank={st[0]},nf={st[1]},score={st[2]:.1f})")


if __name__ == "__main__":
    main()
