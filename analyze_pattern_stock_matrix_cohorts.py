import pathlib
import re

import numpy as np
import pandas as pd


ROOT = pathlib.Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
ANALYSIS_DIR = ROOT / "research_outputs" / "pattern_stock_consistency_matrix_analysis"
SOURCE_XLSX = pathlib.Path(r"C:\Users\SPS\Downloads\PATTERN_STOCK_CONSISTENCY_MATRIX.xlsx")
AGENTS = ROOT / "arena" / "agent_population" / "arena_agents_814.csv"


def family(rule: object) -> str:
    s = str(rule or "").lower()
    hits = []
    if "rsi" in s and "<=" in s:
        hits.append("oversold")
    if re.search(r"distance from (10|20|60|120)-day high <=", s) or "year-high <=" in s:
        hits.append("pullback_from_high")
    if "atr" in s or "daily range % >=" in s or "weekly range % >=" in s:
        hits.append("volatility_expansion")
    if "weekly range % <=" in s or "sub-2.5% range" in s or "sub-3% range" in s:
        hits.append("compression")
    if "5-day return <=" in s or "20-day return <=" in s or "60-day return <=" in s:
        hits.append("recent_weakness")
    if "close position in weekly range >=" in s:
        hits.append("weekly_strength")
    if "close position in weekly range <=" in s:
        hits.append("weekly_weak_close")
    if "relative strength" in s:
        hits.append("relative_strength")
    return "|".join(hits) if hits else "other"


def bucket(x: float) -> str:
    if x >= 10:
        return "10%+"
    if x >= 5:
        return "5-10%"
    if x >= 2:
        return "2-5%"
    if x >= 1:
        return "1-2%"
    return "<1%"


def weighted_avg(g: pd.DataFrame) -> float:
    return float(np.average(g["avg_d1"], weights=g["n"])) if g["n"].sum() > 0 else np.nan


def main() -> None:
    out = ANALYSIS_DIR / "cohort_deep_dive"
    out.mkdir(parents=True, exist_ok=True)

    cp = pd.read_excel(SOURCE_XLSX, sheet_name="consistent_pairs")
    cp["pattern"] = cp["pattern"].astype(str).str.strip()
    cp["symbol"] = cp["symbol"].astype(str).str.strip().str.upper()
    cp["n"] = pd.to_numeric(cp["n"], errors="coerce")
    cp["avg_d1"] = pd.to_numeric(cp["avg_d1"], errors="coerce")
    cp["bucket"] = cp["avg_d1"].map(bucket)
    cp["edge_score"] = cp["n"] * cp["avg_d1"]
    cp["stability_score"] = np.sqrt(cp["n"]) * cp["avg_d1"]

    defs = pd.read_csv(AGENTS)
    defs["pattern"] = "FALCPAT_" + defs["pattern_id"].astype(str)
    keep_cols = ["pattern", "target", "regime", "hit_pct", "base_pct", "real_lift_pp", "status", "rule", "mandate"]
    cp = cp.merge(defs[keep_cols], on="pattern", how="left")
    cp["rule_family"] = cp["rule"].map(family)

    pos = cp[cp["bucket"].isin(["1-2%", "2-5%", "5-10%", "10%+"])].copy()

    cohort_summary = pos.groupby("bucket").agg(
        pairs=("symbol", "size"),
        patterns=("pattern", "nunique"),
        symbols=("symbol", "nunique"),
        total_n=("n", "sum"),
        median_n=("n", "median"),
        mean_n=("n", "mean"),
        avg_d1_mean=("avg_d1", "mean"),
        avg_d1_median=("avg_d1", "median"),
        edge_score_sum=("edge_score", "sum"),
    ).reset_index()

    pattern_cohort = pos.groupby(["bucket", "pattern"]).agg(
        symbols=("symbol", "nunique"),
        total_n=("n", "sum"),
        avg_d1_mean=("avg_d1", "mean"),
        max_avg_d1=("avg_d1", "max"),
        edge_score=("edge_score", "sum"),
        stability_score=("stability_score", "sum"),
        rule_family=("rule_family", lambda s: s.dropna().iloc[0] if len(s.dropna()) else "unknown"),
        rule=("rule", lambda s: s.dropna().iloc[0] if len(s.dropna()) else ""),
    ).reset_index()
    pattern_cohort["weighted_avg_d1"] = pos.groupby(["bucket", "pattern"]).apply(weighted_avg).values

    symbol_cohort = pos.groupby(["bucket", "symbol"]).agg(
        patterns=("pattern", "nunique"),
        total_n=("n", "sum"),
        avg_d1_mean=("avg_d1", "mean"),
        max_avg_d1=("avg_d1", "max"),
        edge_score=("edge_score", "sum"),
        stability_score=("stability_score", "sum"),
    ).reset_index()
    symbol_cohort["weighted_avg_d1"] = pos.groupby(["bucket", "symbol"]).apply(weighted_avg).values

    family_cohort = pos.groupby(["bucket", "rule_family"]).agg(
        pairs=("symbol", "size"),
        patterns=("pattern", "nunique"),
        symbols=("symbol", "nunique"),
        total_n=("n", "sum"),
        edge_score=("edge_score", "sum"),
    ).reset_index()
    family_cohort["weighted_avg_d1"] = pos.groupby(["bucket", "rule_family"]).apply(weighted_avg).values

    # Screener tiers:
    # A: high edge and enough sample. B: broad moderate edge. C: explosive but thin, cap its weight.
    pos["screener_tier"] = np.select(
        [
            (pos["avg_d1"] >= 2.0) & (pos["n"] >= 10),
            (pos["avg_d1"].between(1.0, 2.0, inclusive="left")) & (pos["n"] >= 20),
            (pos["avg_d1"] >= 5.0) & (pos["n"] >= 5),
            (pos["avg_d1"] >= 10.0) & (pos["n"] >= 3),
        ],
        ["A_high_edge", "B_broad_edge", "C_explosive_thin", "D_extreme_thin"],
        default="ignore",
    )
    screener_pairs = pos[pos["screener_tier"] != "ignore"].copy()
    screener_pairs["prior_score"] = np.log1p(screener_pairs["n"]) * screener_pairs["avg_d1"]
    screener_pairs.loc[screener_pairs["screener_tier"].eq("C_explosive_thin"), "prior_score"] *= 0.75
    screener_pairs.loc[screener_pairs["screener_tier"].eq("D_extreme_thin"), "prior_score"] *= 0.50

    files = {
        "cohort_summary": cohort_summary,
        "pattern_cohort": pattern_cohort.sort_values(["bucket", "edge_score"], ascending=[True, False]),
        "symbol_cohort": symbol_cohort.sort_values(["bucket", "edge_score"], ascending=[True, False]),
        "family_cohort": family_cohort.sort_values(["bucket", "edge_score"], ascending=[True, False]),
        "screener_pairs": screener_pairs.sort_values(["screener_tier", "prior_score"], ascending=[True, False]),
        "top_pairs_by_edge": pos.sort_values("edge_score", ascending=False).head(500),
        "top_pairs_by_avg_d1": pos.sort_values(["avg_d1", "n"], ascending=[False, False]).head(500),
    }
    for name, df in files.items():
        df.to_csv(out / f"{name}.csv", index=False)

    print("COHORT SUMMARY")
    print(cohort_summary.round(3).to_string(index=False))
    for b in ["1-2%", "2-5%", "5-10%", "10%+"]:
        print(f"\n===== {b} TOP RULE FAMILIES =====")
        print(family_cohort[family_cohort.bucket.eq(b)].sort_values("edge_score", ascending=False).head(12).round(3).to_string(index=False))
        print(f"\n===== {b} TOP PATTERNS BY EDGE =====")
        cols = ["bucket", "pattern", "symbols", "total_n", "weighted_avg_d1", "edge_score", "rule_family", "rule"]
        print(pattern_cohort[pattern_cohort.bucket.eq(b)].sort_values("edge_score", ascending=False)[cols].head(12).round(3).to_string(index=False))
        print(f"\n===== {b} TOP SYMBOLS BY EDGE =====")
        cols2 = ["bucket", "symbol", "patterns", "total_n", "weighted_avg_d1", "edge_score"]
        print(symbol_cohort[symbol_cohort.bucket.eq(b)].sort_values("edge_score", ascending=False)[cols2].head(15).round(3).to_string(index=False))
    print("\nSCREENER PAIR TIERS")
    print(screener_pairs.groupby("screener_tier").agg(pairs=("symbol", "size"), symbols=("symbol", "nunique"), patterns=("pattern", "nunique"), total_n=("n", "sum"), avg_prior_score=("prior_score", "mean")).round(3).to_string())
    print("\nOUTDIR", out)


if __name__ == "__main__":
    main()
