"""
Build a human-friendly Excel deep-dive of all mined patterns (from KANIDA_SNR.db.mined_patterns).
Plain-English direction/target, a feature glossary so rules are readable, per-stock summary, and the
'Keep' (survived sealed-2026) gold set. Also dumps full CSVs for unrestricted deep-dive.
Reads KANIDA_SNR.db only (idle) — zero impact on the running kanida.db fetch.

Run: PYTHONIOENCODING=utf-8 python build_pattern_workbook.py
"""
import re, sqlite3
from pathlib import Path
import pandas as pd

SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
OUT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\reports"); OUT.mkdir(exist_ok=True)

GLOSSARY = {
    "atr_20_pct": "20-day volatility (Average True Range) as % of price",
    "atr_5_vs_20": "short vs long volatility ratio (>1 = volatility rising)",
    "close_loc": "where the close sat in the day's range (0=low, 1=high)",
    "gap_pct": "opening gap: today's open vs yesterday's close (%)",
    "body_pct": "candle body size as a fraction of the day's range",
    "vol_vs_20d": "today's volume vs its 20-day average (1 = average)",
    "vol_5d_vs_20d": "5-day avg volume vs 20-day avg volume",
    "roc_2": "2-day price return (%)", "roc_5": "5-day price return (%)",
    "roc_10": "10-day price return (%)", "roc_20": "20-day price return (%)",
    "roc_60": "60-day price return (%)",
    "dist_high_10": "% below the 10-day high (0 = at the high)",
    "dist_high_20": "% below the 20-day high", "dist_high_60": "% below the 60-day high",
    "dist_high_120": "% below the 120-day high", "dist_high_252": "% below the 1-year high",
    "dist_sma_20": "% distance from the 20-day moving average",
    "dist_sma_50": "% distance from the 50-day moving average",
    "dist_sma_200": "% distance from the 200-day moving average",
    "slope_sma_20": "slope of the 20-day average (trend up/down)",
    "slope_sma_50": "slope of the 50-day average",
    "rsi_14": "14-day RSI momentum (0-100; >70 hot, <30 oversold)",
    "rs_market_20d": "20-day outperformance vs Nifty (%)",
    "rs_market_60d": "60-day outperformance vs Nifty (%)",
    "n_sub_3_range_7d": "# of last 7 days with a tight (<3%) range (compression)",
    "n_higher_highs_5d": "# of last 5 days making a higher high",
    "n_higher_lows_5d": "# of last 5 days making a higher low",
    "n_sub_75v_7d": "# of last 7 days with quiet volume (<75% of avg)",
    "weekly_close_loc": "where the close sits in this week's range so far (0-1)",
    "weekly_range_pct": "this week's range so far as % of price",
    "id_close_vs_vwap": "day's close vs day's VWAP (%) — above/below avg traded price",
    "id_first30_ret": "first 30-min return (open -> 09:44) (%)",
    "id_volfh_pct": "% of the day's volume done in the first hour",
}


def translate_target(t):
    m = re.match(r"(up|dn)_(\d+)pct_(\d+)d$", t)
    if m:
        d, p, w = m.groups()
        return ("Long" if d == "up" else "Short", "Reversion",
                f"{'rises' if d=='up' else 'falls'} >= {p}% within {w} day(s)")
    m = re.match(r"(up|dn)_tb(\d+)x(\d+)_(\d+)d$", t)
    if m:
        d, a, b, w = m.groups()
        return ("Long (trend)" if d == "up" else "Short (trend)", "Trend",
                (f"+{a}% before -{b}%" if d == "up" else f"-{a}% before +{b}%") + f" within {w} day(s)")
    return ("?", "?", t)


def main():
    con = sqlite3.connect(SNR)
    df = pd.read_sql("SELECT * FROM mined_patterns", con)
    try:
        verd = pd.read_sql("SELECT Stock,verdict FROM stock_verdict", con)
    except Exception:
        verd = pd.DataFrame(columns=["Stock", "verdict"])
    con.close()
    print(f"loaded {len(df):,} patterns across {df.Stock.nunique()} stocks")

    tr = df["outcome_target"].map(translate_target)
    df["Direction"] = [x[0] for x in tr]; df["Family"] = [x[1] for x in tr]; df["Predicts"] = [x[2] for x in tr]
    df["Held_2025"] = df["promoted"].map({1: "Yes", 0: "No"})
    # explicit false positives = fired but target did NOT happen (wrong signal / likely losing trade)
    df["fp_train"] = (df["n_obs"] - df["n_hits"]).clip(lower=0)
    df["fp_rate_train"] = (100 - df["precision_pct"]).round(2)
    df["fp_2026"] = (df["oos2026_n"].fillna(0) - df["oos2026_hits"].fillna(0)).clip(lower=0).astype("Int64")
    df["fp_rate_2026"] = (100 - df["oos2026_precision_pct"]).round(2)

    friendly = {"Stock": "Stock", "Direction": "Direction", "Predicts": "Predicts (plain English)",
                "Family": "Family", "rule_text": "Rule (conditions, ALL must hold)",
                "n_obs": "Occurrences (train)", "n_hits": "Hits/True-Pos (train)",
                "fp_train": "False Positives (train)", "precision_pct": "Precision % (train)",
                "fp_rate_train": "False Positive % (train)", "base_rate_pct": "Base rate %",
                "lift_pct": "Edge/Lift pp (train)", "depth": "# Conditions",
                "Held_2025": "Held on 2025?", "status": "Status",
                "oos2026_n": "Occurrences (2026)", "oos2026_hits": "Hits/True-Pos (2026)",
                "fp_2026": "False Positives (2026 SEALED)", "oos2026_precision_pct": "Precision % (2026 SEALED)",
                "fp_rate_2026": "False Positive % (2026 SEALED)", "oos2026_base_pct": "Base rate % (2026)",
                "oos2026_lift_pct": "Edge/Lift pp (2026 SEALED)"}
    cols = list(friendly.keys())

    def view(d):
        return d[cols].rename(columns=friendly)

    keep = df[df["status"] == "Keep"].sort_values(["Stock", "oos2026_lift_pct"], ascending=[True, False])
    promoted = df[df["promoted"] == 1].sort_values(["Stock", "lift_pct"], ascending=[True, False])

    # CSVs (full raw for unrestricted deep-dive)
    view(df.sort_values(["Stock", "lift_pct"], ascending=[True, False])).to_csv(OUT / "mined_patterns_ALL.csv", index=False)
    view(promoted).to_csv(OUT / "mined_patterns_promoted.csv", index=False)
    view(keep).to_csv(OUT / "mined_patterns_keep.csv", index=False)

    # per-stock summary
    g = df.groupby("Stock")
    summ = pd.DataFrame({
        "Total_mined": g.size(),
        "Held_2025": g.apply(lambda x: int((x.promoted == 1).sum()), include_groups=False),
        "Keep_survived_2026": g.apply(lambda x: int((x.status == "Keep").sum()), include_groups=False),
        "Watch": g.apply(lambda x: int((x.status == "Watch").sum()), include_groups=False),
        "Retire": g.apply(lambda x: int((x.status == "Retire").sum()), include_groups=False),
        "Best_Edge_2026_pp": g["oos2026_lift_pct"].max().round(1),
    }).reset_index()
    if len(verd):
        summ = summ.merge(verd, on="Stock", how="left").rename(columns={"verdict": "Verdict_2026"})
    summ = summ.sort_values("Best_Edge_2026_pp", ascending=False)

    # README / glossary
    readme = pd.DataFrame({"Column / Term": [
        "HOW TO USE", "Sheet: Summary_by_Stock", "Sheet: Keep_Patterns", "Sheet: Top_Patterns", "",
        "Direction", "Predicts (plain English)", "Family", "Rule (conditions)",
        "Occurrences (train)", "Hits/True-Pos (train)", "False Positives (train)", "Precision % (train)", "False Positive % (train)",
        "Base rate %", "Edge/Lift pp", "# Conditions", "Held on 2025?", "Status",
        "Occurrences (2026)", "Hits/True-Pos (2026)", "False Positives (2026 SEALED)", "Precision % (2026 SEALED)", "False Positive % (2026 SEALED)", "Edge/Lift pp (2026 SEALED)",
        "", "--- FEATURE GLOSSARY (used in rules) ---"] + list(GLOSSARY.keys()),
        "Meaning": [
        "Start at Summary_by_Stock, then filter Keep_Patterns for a stock to see its surviving edges.",
        "One row per stock: how many patterns, how many held 2025, how many survived sealed 2026, best edge, 2026 verdict.",
        "Every pattern that SURVIVED the sealed 2026 year (the trustworthy set). Filter by Stock/Direction.",
        "The single strongest surviving patterns across all stocks, by 2026 edge.", "",
        "Long = bet price rises; Short = bet price falls.",
        "What the pattern predicts, in words.",
        "Reversion (bounce/fade to a target) or Trend (target hit before a stop).",
        "The exact conditions — ALL must be true on the day for the pattern to fire. See glossary below.",
        "How many times this setup occurred in the training years (<=2024).",
        "Of those, how many HIT the target (correct signals).",
        "Of those, how many did NOT hit = WRONG signals / likely losing trades (Occurrences - Hits).",
        "Hits / Occurrences. How reliable it looked in training.",
        "Share of signals that were WRONG = 100 - Precision. Lower is better.",
        "How often the target happens ANYWAY (no pattern). The bar to beat.",
        "Precision minus Base rate = the real EDGE in percentage points. Higher = better.",
        "How many conditions the rule has (fewer = simpler/robust).",
        "Yes = the pattern still worked on the 2025 validation year (out-of-sample).",
        "Keep = survived sealed 2026 | Watch = marginal | Retire = failed 2026 | Test = didn't hold 2025.",
        "How many times it fired in the sealed 2026 test year.",
        "Correct signals on 2026.",
        "WRONG signals on the honest 2026 test (Occurrences - Hits).",
        "Precision on the 2026 year that was NEVER used to build/select — the honest test.",
        "Wrong-signal rate on 2026 = 100 - Precision. Compare to (100 - Base rate) to judge real skill.",
        "The honest EDGE on sealed 2026. THIS is the number to trust most.",
        "", ""] + list(GLOSSARY.values())})

    def safe_write(path_xlsx):
        try:
            with pd.ExcelWriter(path_xlsx, engine="openpyxl") as xl:
                readme.to_excel(xl, sheet_name="README_and_Glossary", index=False)
                summ.to_excel(xl, sheet_name="Summary_by_Stock", index=False)
                view(keep).to_excel(xl, sheet_name="Keep_Patterns", index=False)
                view(keep.sort_values("oos2026_lift_pct", ascending=False).head(1000)).to_excel(
                    xl, sheet_name="Top_Patterns", index=False)
            return path_xlsx
        except PermissionError:
            return None
    out_x = safe_write(OUT / "KANIDA_mined_patterns.xlsx") or safe_write(OUT / "KANIDA_mined_patterns_v2.xlsx")
    print("WROTE:")
    print("  %s  (README+Glossary, Summary_by_Stock, Keep_Patterns %d, Top_Patterns) [+ False-Positive columns]"
          % (out_x.name, len(keep)))
    print("  reports/mined_patterns_ALL.csv      (%d rows)" % len(df))
    print("  reports/mined_patterns_promoted.csv (%d rows)" % len(promoted))
    print("  reports/mined_patterns_keep.csv     (%d rows)" % len(keep))


if __name__ == "__main__":
    main()
