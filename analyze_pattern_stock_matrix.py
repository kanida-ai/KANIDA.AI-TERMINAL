import json
import pathlib
import re

import numpy as np
import pandas as pd


SOURCE = pathlib.Path(r"C:\Users\SPS\Downloads\PATTERN_STOCK_CONSISTENCY_MATRIX.xlsx")
OUTDIR = pathlib.Path(
    r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\research_outputs\pattern_stock_consistency_matrix_analysis"
)


def family_for_name(name: str) -> str:
    labels = [
        ("fresh_breakout", r"break|new.?high|high|bo_|bo\b"),
        ("pullback", r"pullback|pb|dip|retrac|red|weak|down"),
        ("dryup_compression", r"dry|compress|squeeze|tight|narrow|low.?vol|vol.*dry"),
        ("continuation", r"continu|momentum|trend|rs|strength|green|up"),
        ("volume", r"volume|vol_|vol\b"),
        ("gap", r"gap"),
        ("vwap_intraday", r"vwap|intraday|open|close|9:|15m|minute"),
    ]
    s = str(name).lower()
    hits = [label for label, rx in labels if re.search(rx, s)]
    return "|".join(hits) if hits else "unclassified"


def weighted_avg(group: pd.DataFrame) -> float:
    total = group["n"].sum()
    return float(np.average(group["avg_d1"], weights=group["n"])) if total > 0 else np.nan


def main() -> None:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    cp = pd.read_excel(SOURCE, sheet_name="consistent_pairs")
    bp = pd.read_excel(SOURCE, sheet_name="by_pattern")

    cp["pattern"] = cp["pattern"].astype(str).str.strip()
    cp["symbol"] = cp["symbol"].astype(str).str.strip().str.upper()
    cp["n"] = pd.to_numeric(cp["n"], errors="coerce")
    cp["avg_d1"] = pd.to_numeric(cp["avg_d1"], errors="coerce")
    bp["pattern"] = bp["pattern"].astype(str).str.strip()
    bp["n_consistent"] = pd.to_numeric(bp["n_consistent"], errors="coerce")
    bp["stocks"] = bp["stocks"].astype(str)

    qc = {
        "source": str(SOURCE),
        "consistent_pairs_rows": len(cp),
        "by_pattern_rows": len(bp),
        "unique_patterns_pairs": int(cp.pattern.nunique()),
        "unique_patterns_by_pattern": int(bp.pattern.nunique()),
        "unique_symbols": int(cp.symbol.nunique()),
        "null_counts_pairs": cp.isna().sum().to_dict(),
        "null_counts_by_pattern": bp.isna().sum().to_dict(),
        "duplicate_pair_rows": int(cp.duplicated(["pattern", "symbol"]).sum()),
        "duplicate_pattern_rows": int(bp.duplicated(["pattern"]).sum()),
        "n_min": float(cp.n.min()),
        "n_max": float(cp.n.max()),
        "n_mean": float(cp.n.mean()),
        "n_median": float(cp.n.median()),
        "avg_d1_min": float(cp.avg_d1.min()),
        "avg_d1_max": float(cp.avg_d1.max()),
        "avg_d1_mean": float(cp.avg_d1.mean()),
        "avg_d1_median": float(cp.avg_d1.median()),
    }

    calc = cp.groupby("pattern").agg(
        calc_n_consistent=("symbol", "nunique"),
        pair_rows=("symbol", "size"),
        total_occurrences=("n", "sum"),
    ).reset_index()
    calc["weighted_avg_d1"] = cp.groupby("pattern").apply(weighted_avg).values
    rec = bp.merge(calc, on="pattern", how="outer", indicator=True)
    rec["n_diff"] = rec["n_consistent"] - rec["calc_n_consistent"]
    qc["patterns_only_in_pairs"] = int((rec["_merge"] == "right_only").sum())
    qc["patterns_only_in_by_pattern"] = int((rec["_merge"] == "left_only").sum())
    qc["n_consistent_mismatches"] = int((rec["n_diff"].fillna(0) != 0).sum())

    cp["family"] = cp.pattern.map(family_for_name)
    bp["family"] = bp.pattern.map(family_for_name)

    pattern_summary = cp.groupby("pattern").agg(
        n_symbols=("symbol", "nunique"),
        total_n=("n", "sum"),
        avg_d1_mean=("avg_d1", "mean"),
        avg_d1_median=("avg_d1", "median"),
        max_pair_avg_d1=("avg_d1", "max"),
        min_pair_avg_d1=("avg_d1", "min"),
    ).reset_index()
    pattern_summary["avg_d1_weighted"] = cp.groupby("pattern").apply(weighted_avg).values
    pattern_summary["family"] = pattern_summary.pattern.map(family_for_name)
    pattern_summary = pattern_summary.sort_values(
        ["n_symbols", "avg_d1_weighted", "total_n"], ascending=[False, False, False]
    )

    symbol_summary = cp.groupby("symbol").agg(
        n_patterns=("pattern", "nunique"),
        total_n=("n", "sum"),
        avg_d1_mean=("avg_d1", "mean"),
        avg_d1_median=("avg_d1", "median"),
        best_avg_d1=("avg_d1", "max"),
        worst_avg_d1=("avg_d1", "min"),
    ).reset_index()
    symbol_summary["avg_d1_weighted"] = cp.groupby("symbol").apply(weighted_avg).values
    symbol_summary = symbol_summary.sort_values(
        ["n_patterns", "avg_d1_weighted", "total_n"], ascending=[False, False, False]
    )

    pairs = cp.copy()
    pairs["score_n_x_avg"] = pairs.n * pairs.avg_d1
    strong = pairs[(pairs.n >= 5) & (pairs.avg_d1 > 0)].sort_values(["avg_d1", "n"], ascending=[False, False])
    strong10 = pairs[(pairs.n >= 10) & (pairs.avg_d1 > 0)].sort_values(["avg_d1", "n"], ascending=[False, False])
    negative = pairs[(pairs.n >= 5) & (pairs.avg_d1 < 0)].sort_values(["avg_d1", "n"], ascending=[True, False])

    n_bins = pd.cut(
        cp.n,
        bins=[0, 1, 2, 3, 5, 10, 20, 50, 100, 10**9],
        labels=["1", "2", "3", "4-5", "6-10", "11-20", "21-50", "51-100", "100+"],
    ).value_counts().sort_index().reset_index()
    n_bins.columns = ["n_bucket", "pairs"]
    avg_bins = pd.cut(
        cp.avg_d1,
        bins=[-999, -5, -2, -1, 0, 1, 2, 5, 10, 999],
        labels=["<-5", "-5:-2", "-2:-1", "-1:0", "0:1", "1:2", "2:5", "5:10", "10+"],
    ).value_counts().sort_index().reset_index()
    avg_bins.columns = ["avg_d1_bucket", "pairs"]

    fam_summary = cp.groupby("family").agg(
        pairs=("symbol", "size"),
        patterns=("pattern", "nunique"),
        symbols=("symbol", "nunique"),
        total_n=("n", "sum"),
    ).reset_index()
    fam_summary["avg_d1_weighted"] = cp.groupby("family").apply(weighted_avg).values
    fam_summary = fam_summary.sort_values("pairs", ascending=False)

    outputs = {
        "reconciliation": rec,
        "pattern_summary": pattern_summary,
        "symbol_summary": symbol_summary,
        "strong_pairs_n5": strong,
        "strong_pairs_n10": strong10,
        "negative_pairs_n5": negative,
        "n_distribution": n_bins,
        "avg_d1_distribution": avg_bins,
        "family_summary": fam_summary,
    }
    for name, df in outputs.items():
        df.to_csv(OUTDIR / f"{name}.csv", index=False)
    (OUTDIR / "qc_summary.json").write_text(json.dumps(qc, indent=2, default=str), encoding="utf-8")

    print("QC")
    print(json.dumps(qc, indent=2))
    print("\nTOP_PATTERNS_BY_SYMBOL_COUNT")
    print(pattern_summary.head(20).round(4).to_string(index=False))
    print("\nTOP_SYMBOLS_BY_PATTERN_COUNT")
    print(symbol_summary.head(25).round(4).to_string(index=False))
    print("\nSTRONG_PAIRS_N10")
    print(strong10.head(25).round(4).to_string(index=False))
    print("\nNEGATIVE_PAIRS_N5")
    print(negative.head(15).round(4).to_string(index=False))
    print("\nN_DISTRIBUTION")
    print(n_bins.to_string(index=False))
    print("\nAVG_D1_DISTRIBUTION")
    print(avg_bins.to_string(index=False))
    print("\nFAMILY_SUMMARY")
    print(fam_summary.round(4).to_string(index=False))
    print(f"\nOUTDIR {OUTDIR}")


if __name__ == "__main__":
    main()
