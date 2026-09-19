"""
Export the Chart Agent's evidence as plain TABLES (CSV + console) for independent verification.
Produces three tables:
  1) per-occurrence trades  — every historical signal with dates, level, and T+1..T+10 net returns + MFE/MAE
  2) per-horizon summary    — win%/mean/median/quartiles/MFE/MAE for T+1..T+10
  3) pooled per-stock       — the same pattern across the basket (n, %up, mean T+3/T+10)
Run:  python scripts/chart_agent_table.py RELIANCE
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from chart_agent import load_daily, detect_horizontal_breakout_retest, pattern_evidence, PARAMS, BASKET, COST, OUT

pd.set_option("display.width", 200); pd.set_option("display.max_columns", 40)


def occurrence_table(df, events, max_h=10) -> pd.DataFrame:
    o, hi, lo, c = (df[k].values for k in ["open", "high", "low", "close"]); n = len(c); idx = df.index
    rows = []
    for e in events:
        s = e.entry_idx
        if s + max_h - 1 >= n:
            continue
        row = {"signal_date": idx[e.signal_idx].strftime("%Y-%m-%d"),
               "entry_date": idx[s].strftime("%Y-%m-%d"),
               "breakout_date": idx[e.breakout_idx].strftime("%Y-%m-%d"),
               "level": round(e.level, 2), "entry_open": round(float(o[s]), 2)}
        for h in range(1, max_h + 1):
            row[f"T+{h}"] = round((c[s + h - 1] / o[s] - 1 - COST) * 100, 2)
        row["MFE10"] = round((hi[s:s + max_h].max() / o[s] - 1) * 100, 2)
        row["MAE10"] = round((lo[s:s + max_h].min() / o[s] - 1) * 100, 2)
        rows.append(row)
    return pd.DataFrame(rows)


def horizon_summary(ev) -> pd.DataFrame:
    hz = ev["horizons"]
    return pd.DataFrame([{"horizon": f"T+{h}", "win%": hz[h]["win"], "mean%": hz[h]["mean"],
                          "median%": hz[h]["median"], "p25%": hz[h]["p25"], "p75%": hz[h]["p75"],
                          "avgMFE%": hz[h]["mfe"], "avgMAE%": hz[h]["mae"]} for h in range(1, ev["ref_h"] + 1)])


def per_stock_table(symbols) -> pd.DataFrame:
    rows = []
    for sym in symbols:
        try:
            df = load_daily(sym)
        except Exception:
            continue
        if len(df) < 400:
            continue
        evs = detect_horizontal_breakout_retest(df, **PARAMS)
        if not evs:
            continue
        ev = pattern_evidence(df, evs, 10)
        if not ev:
            continue
        s, hz = ev["summary"], ev["horizons"]
        rows.append({"symbol": sym, "n": s["n"], "pct_up_T10": s["pct_up"],
                     "mean_T3%": hz[3]["mean"], "mean_T10%": hz[10]["mean"],
                     "avg_up%": s["avg_up"], "avg_down%": s["avg_down"]})
    return pd.DataFrame(rows).sort_values("mean_T3%", ascending=False).reset_index(drop=True)


if __name__ == "__main__":
    symbol = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"
    df = load_daily(symbol)
    events = detect_horizontal_breakout_retest(df, **PARAMS)
    occ = occurrence_table(df, events)
    ev = pattern_evidence(df, events, 10)
    summ = horizon_summary(ev)
    ps = per_stock_table(BASKET)

    os.makedirs(OUT, exist_ok=True)
    occ.to_csv(os.path.join(OUT, f"{symbol}_occurrences.csv"), index=False)
    summ.to_csv(os.path.join(OUT, f"{symbol}_horizon_summary.csv"), index=False)
    ps.to_csv(os.path.join(OUT, "POOLED_per_stock.csv"), index=False)

    print(f"\n### 1) {symbol} — every historical occurrence (net %, incl. {COST*10000:.0f}bps cost)\n")
    print(occ.to_string(index=False))
    print(f"\n### 2) {symbol} — per-horizon summary T+1..T+10\n")
    print(summ.to_string(index=False))
    print(f"\n### 3) Pooled — same pattern across {len(ps)} stocks (sorted by mean T+3)\n")
    print(ps.to_string(index=False))
    print(f"\nCSVs written to {OUT}\\  ({symbol}_occurrences.csv, {symbol}_horizon_summary.csv, POOLED_per_stock.csv)")
