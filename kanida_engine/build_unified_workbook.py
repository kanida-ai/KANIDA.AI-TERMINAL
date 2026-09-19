"""
Human-friendly deep-dive of the UNIFIED engine's mined patterns (KANIDA_SNR.db.unified_patterns).
Plain-English rules + a glossary of the micro-structure + institutional features, occurrences, hits,
false positives, precision, edge/lift, mined year, and sealed-2026 OOS stats. Excel + full CSVs.
Reads KANIDA_SNR.db (idle for reads) — safe alongside the running jobs.

Run: python build_unified_workbook.py
"""
import json, sqlite3, re
from pathlib import Path
import pandas as pd

SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
OUT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\reports"); OUT.mkdir(exist_ok=True)

GLOSS = {
    "m_roc_5": "5-day price return %", "m_roc_20": "20-day price return %",
    "m_atr20_pct": "20-day volatility (ATR) as % of price",
    "m_dist_high_20": "% below the 20-day high (0 = at the high)", "m_dist_high_60": "% below the 60-day high",
    "m_dist_sma_20": "% distance from the 20-day moving average", "m_slope_sma20": "slope of the 20-day MA (trend)",
    "m_rsi14": "14-day RSI momentum (0-100; >70 hot, <30 oversold)",
    "m_vol_vs_20": "daily volume vs its 20-day average", "m_cmf20": "Chaikin Money Flow — accumulation/distribution (>0 buying)",
    "m_mfi14": "Money Flow Index — volume-weighted RSI (0-100)", "m_obv_slope20": "On-Balance-Volume 20-day slope (pressure trend)",
    "m_wtd_ret": "week-to-date return %", "m_mtd_ret": "month-to-date return %",
    "m_wtd_range": "this week's price range so far %", "m_rs_20d": "20-day outperformance vs Nifty %",
    "eod_ret": "the day's return (open→close) %", "eod_range_pct": "the day's high-low range %",
    "eod_n_hivol": "count of unusually high-volume 1-min bars that day",
    "eod_n_iceberg": "count of iceberg bars (big volume, flat price = silent absorption)",
    "eod_n_absorb": "count of absorption bars (heavy down-volume that held)",
    "eod_buy_sell_imbalance": "up-minute vs down-minute volume balance (−1..+1)",
    "eod_max_vol_z": "biggest 1-min volume spike that day (z-score)",
    "eod_atp_crossings": "times price crossed its VWAP intraday (chop)",
    "eod_cvd_norm": "cumulative volume delta / volume — net buy vs sell pressure",
    "eod_vol_compression": "intraday volatility compression (<1 = squeeze)",
    "eod_deepest_pullback": "deepest intraday pullback from the day's high %",
    "eod_dist_poc": "close vs the Point-of-Control (most-traded price) %",
}
SHORT = {
    "m_roc_5": "5d-return", "m_roc_20": "20d-return", "m_atr20_pct": "volatility", "m_dist_high_20": "below-20d-high",
    "m_dist_high_60": "below-60d-high", "m_dist_sma_20": "vs-20d-MA", "m_slope_sma20": "20d-MA-slope", "m_rsi14": "RSI",
    "m_vol_vs_20": "volume-vs-avg", "m_cmf20": "money-flow(CMF)", "m_mfi14": "money-flow-index(MFI)",
    "m_obv_slope20": "OBV-slope", "m_wtd_ret": "week-to-date-ret", "m_mtd_ret": "month-to-date-ret",
    "m_wtd_range": "week-range", "m_rs_20d": "vs-Nifty(20d)", "eod_ret": "day-return", "eod_range_pct": "day-range",
    "eod_n_hivol": "#hi-vol-bars", "eod_n_iceberg": "#iceberg-bars", "eod_n_absorb": "#absorption-bars",
    "eod_buy_sell_imbalance": "buy/sell-imbalance", "eod_max_vol_z": "max-vol-spike", "eod_atp_crossings": "VWAP-crossings",
    "eod_cvd_norm": "net-buy-pressure(CVD)", "eod_vol_compression": "vol-squeeze", "eod_deepest_pullback": "deepest-pullback",
    "eod_dist_poc": "vs-POC",
}
TGT = {"up_1pct_1d": ("Long", "rises ≥1% within 1 day"), "up_2pct_2d": ("Long", "rises ≥2% within 2 days"),
       "up_5pct_5d": ("Long", "rises ≥5% within 5 days"), "dn_1pct_1d": ("Short", "falls ≥1% within 1 day"),
       "dn_2pct_2d": ("Short", "falls ≥2% within 2 days"), "dn_5pct_5d": ("Short", "falls ≥5% within 5 days")}


def short_lbl(f):
    m = re.match(r"(.+)_T(\d+)$", f)
    if m:
        base, n = m.group(1), m.group(2)
        return f"{SHORT.get(base, base)}[{n}d-ago]"
    return SHORT.get(f, f)


def plain_rule(rule_json):
    conds = json.loads(rule_json); parts = []
    for f, op, thr in conds:
        parts.append(f"{short_lbl(f)} {'≤' if op == '<=' else '>'} {round(thr, 3)}")
    return " AND ".join(parts)


def main():
    con = sqlite3.connect(SNR)
    df = pd.read_sql("SELECT * FROM unified_patterns", con); con.close()
    print(f"loaded {len(df):,} patterns across {df.symbol.nunique()} stocks")
    df["Direction"] = df["target"].map(lambda t: TGT.get(t, ("?", "?"))[0])
    df["Predicts"] = df["target"].map(lambda t: TGT.get(t, ("?", t))[1])
    df["Family"] = df["has_micro"].map({1: "micro+macro", 0: "macro-only"})
    df["Rule_plain"] = df["rule_json"].map(plain_rule)
    df["Mined_year"] = "train ≤2024"
    df["hits_tr"] = (df.n_tr * df.prec_tr / 100).round().astype(int)
    df["fp_tr"] = df.n_tr - df.hits_tr
    df["hits_te"] = (df.n_te * df.prec_te / 100).round().astype(int)

    friendly = {"symbol": "Stock", "Direction": "Direction", "Predicts": "Predicts (plain)", "Family": "Family",
                "Rule_plain": "Rule (plain English)", "rule_text": "Rule (technical)", "Mined_year": "Mined on",
                "n_tr": "Occurrences (train)", "hits_tr": "Hits (train)", "fp_tr": "False Positives (train)",
                "prec_tr": "Precision % (train)", "base_tr": "Base rate %", "lift_tr": "Edge/Lift pp (train)",
                "promoted": "Held 2025?", "n_te": "Occurrences (2026)", "hits_te": "Hits (2026)",
                "fp_te": "False Positives (2026)", "prec_te": "Precision % (2026 SEALED)", "base_te": "Base % (2026)",
                "lift_te": "Edge/Lift pp (2026 SEALED)", "status": "Status"}
    cols = list(friendly.keys())
    df["promoted"] = df["promoted"].map({1: "Yes", 0: "No"})

    def view(d): return d[cols].rename(columns=friendly)
    keep = df[df.status == "Keep"].sort_values(["symbol", "lift_te"], ascending=[True, False])
    view(df.sort_values(["symbol", "lift_tr"], ascending=[True, False])).to_csv(OUT / "unified_patterns_ALL.csv", index=False)
    view(keep).to_csv(OUT / "unified_patterns_keep.csv", index=False)

    g = df.groupby("symbol")
    summ = pd.DataFrame({
        "Total_mined": g.size(), "Held_2025": g.apply(lambda x: int((x.promoted == "Yes").sum()), include_groups=False),
        "Keep_survived_2026": g.apply(lambda x: int((x.status == "Keep").sum()), include_groups=False),
        "micro_macro_Keep": g.apply(lambda x: int(((x.status == "Keep") & (x.Family == "micro+macro")).sum()), include_groups=False),
        "Best_Edge_2026_pp": g["lift_te"].max().round(1)}).reset_index().sort_values("Best_Edge_2026_pp", ascending=False)

    gl = pd.DataFrame({"Feature (in rules)": ["--- HOW TO READ ---", "Precision % (2026 SEALED)",
        "Edge/Lift pp (2026 SEALED)", "False Positives", "Held 2025?", "Status", "Family", ""] + list(GLOSS.keys()),
        "Meaning": ["Trust the 2026-SEALED columns (that year was never used to build/select).",
        "How often the pattern was RIGHT on the unseen 2026 year.",
        "Precision minus base rate = the real EDGE. THIS is the number to trust.",
        "Times it fired but was WRONG (occurrences − hits) = likely losing signals.",
        "Yes = it also worked on the 2025 validation year.",
        "Keep = survived sealed 2026 | Watch/Retire/Test = weaker.",
        "micro+macro = uses 1-min structure (POC/CVD/squeeze/footprints); macro-only = daily only.", ""]
        + list(GLOSS.values())})

    with pd.ExcelWriter(OUT / "KANIDA_unified_patterns.xlsx", engine="openpyxl") as xl:
        gl.to_excel(xl, "README_and_Glossary", index=False)
        summ.to_excel(xl, "Summary_by_Stock", index=False)
        view(keep).to_excel(xl, "Keep_Patterns", index=False)
        view(keep.sort_values("lift_te", ascending=False).head(1500)).to_excel(xl, "Top_Patterns", index=False)
    print(f"WROTE reports/KANIDA_unified_patterns.xlsx (Keep {len(keep):,}) + unified_patterns_ALL.csv ({len(df):,})")


if __name__ == "__main__":
    main()
