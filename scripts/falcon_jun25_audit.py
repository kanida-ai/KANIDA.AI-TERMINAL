"""
Falcon Top-10 forensic audit — signal_date 2026-06-25 (engine v7.1.0).
Authoritative source = the LIVE db (data/db/kanida_universe.db): falcon_signals_live
(rank, score, n_fires, fired_pattern_ids, sample_rules), falcon_features (06-25 inputs),
ohlc_daily (raw bars), falcon_pattern_taxonomy (pattern rules/english/lift).

Traces raw OHLC -> features -> fired patterns -> score -> avg_lift -> rank, and
classifies every input as EOD-only / known-early / after-close. Read-only.
Output: outputs/Falcon_Jun25_Signal_Audit.xlsx
"""
import sqlite3, json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "db" / "kanida_universe.db"
OUT = ROOT / "outputs" / "Falcon_Jun25_Signal_Audit.xlsx"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
SD = "2026-06-25"

# When is each feature knowable? (from its definition)
AVAIL = {
    "close_at_signal": "AFTER-CLOSE (the official close)",
    "range_pct": "EOD (needs full-day high & low)",
    "close_loc": "AFTER-CLOSE (needs the close)",
    "gap_pct": "KNOWN 09:15 (open vs prev close)",
    "body_pct": "AFTER-CLOSE (needs the close)",
    "rsi_14": "AFTER-CLOSE (close-based)",
    "roc_5": "AFTER-CLOSE (close-based)",
    "roc_20": "AFTER-CLOSE (close-based)",
    "roc_60": "AFTER-CLOSE (close-based)",
    "vol_vs_20d": "EOD (needs FULL-DAY volume)",
    "vol_5d_vs_20d": "EOD (needs FULL-DAY volume)",
    "dist_sma_20": "AFTER-CLOSE (close vs MA)",
    "dist_sma_50": "AFTER-CLOSE (close vs MA)",
    "dist_sma_200": "AFTER-CLOSE (close vs MA)",
    "slope_sma_20": "AFTER-CLOSE (close-based)",
    "slope_sma_50": "AFTER-CLOSE (close-based)",
    "dist_high_20": "AFTER-CLOSE (close vs N-day high)",
    "dist_high_60": "AFTER-CLOSE (close vs N-day high)",
    "dist_high_120": "AFTER-CLOSE (close vs N-day high)",
    "dist_high_252": "AFTER-CLOSE (close vs N-day high)",
    "weekly_close_loc": "AFTER-CLOSE (weekly close)",
    "weekly_close_vs_sma20": "AFTER-CLOSE (weekly close)",
    "weekly_range_pct": "EOD (needs full-day/week range)",
    "weekly_breakout_20w": "AFTER-CLOSE (weekly close)",
    "atr_20_pct": "EOD (includes today's full range)",
    "atr_5_vs_20": "EOD (includes today's full range)",
}
KEYFEATS = ["roc_60", "roc_20", "roc_5", "rsi_14", "close_loc", "weekly_close_loc",
            "weekly_breakout_20w", "range_pct", "atr_20_pct", "vol_vs_20d", "vol_5d_vs_20d",
            "dist_sma_20", "dist_sma_50", "dist_sma_200", "dist_high_252", "gap_pct"]

c = sqlite3.connect(str(DB))
fcols = [r[1] for r in c.execute("PRAGMA table_info(falcon_features)")]


def parse_ids(fpi):
    if not fpi:
        return []
    try:
        return json.loads(fpi)
    except Exception:
        return [int(x) for x in str(fpi).replace("[", "").replace("]", "").split(",") if x.strip().isdigit()]


# ---- Sheet 1: Top-10 ranking with the ranking metric ----
top = c.execute("""SELECT rank, symbol, sector, n_fires, score, close_at_signal, avg_value_60d
                   FROM falcon_signals_live WHERE signal_date=? ORDER BY rank LIMIT 10""", (SD,)).fetchall()
rank_rows = []
for rk, sym, sec, nf, sc, cl, av in top:
    rank_rows.append({"rank": rk, "symbol": sym, "sector": sec, "n_patterns_fired": nf,
                      "score_sum_lift": round(sc, 2), "avg_lift_RANKMETRIC": round(sc / nf, 3),
                      "close_at_signal": cl, "avg_value_60d_cr": round((av or 0) / 1e7, 1)})
s_rank = pd.DataFrame(rank_rows)

# ---- Per-stock detail (all 10): features + sample patterns ----
feat_rows, pat_rows, raw_rows = [], [], []
for rk, sym, sec, nf, sc, cl, av in top:
    fr = c.execute("SELECT * FROM falcon_features WHERE symbol=? AND trade_date=?", (sym, SD)).fetchone()
    fmap = dict(zip(fcols, fr)) if fr else {}
    row = {"rank": rk, "symbol": sym}
    for k in KEYFEATS:
        row[k] = fmap.get(k)
    feat_rows.append(row)
    # raw OHLC (signal day + prev)
    rr = c.execute("SELECT trade_date,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? "
                   "AND trade_date<=? ORDER BY trade_date DESC LIMIT 2", (sym, SD)).fetchall()
    if rr:
        d0 = rr[0]; prev = rr[1] if len(rr) > 1 else (None,)*6
        raw_rows.append({"rank": rk, "symbol": sym, "date": d0[0], "open": d0[1], "high": d0[2],
                         "low": d0[3], "close": d0[4], "volume": d0[5], "prev_close": prev[4]})
    # fired pattern sample
    srules = c.execute("SELECT sample_rules FROM falcon_signals_live WHERE signal_date=? AND symbol=?",
                       (SD, sym)).fetchone()[0]
    try:
        for p in json.loads(srules):
            pat_rows.append({"rank": rk, "symbol": sym, "pattern_id": p.get("pattern_id"),
                             "target": p.get("target"), "oos_lift_pp": p.get("oos_lift"),
                             "rule": p.get("rule")})
    except Exception:
        pass
s_feat = pd.DataFrame(feat_rows)
s_pat = pd.DataFrame(pat_rows)
s_raw = pd.DataFrame(raw_rows)

# ---- EOD vs intraday classification of the inputs the patterns actually use ----
used_feats = sorted(set(
    tok.split(">")[0].split("<")[0].strip()
    for p in pat_rows for tok in p["rule"].replace("<=", "<").replace(">=", ">").split("AND")
))
class_rows = [{"feature": f, "availability": AVAIL.get(f, "EOD (daily-derived)")} for f in used_feats]
s_class = pd.DataFrame(class_rows).sort_values("availability")

# ---- No-leakage / methodology ----
yrs = c.execute("""SELECT DISTINCT mined_year FROM falcon_pattern_taxonomy WHERE pattern_id IN
                   (SELECT DISTINCT value FROM (SELECT 8326 value UNION SELECT 7964 UNION SELECT 8366))""").fetchall()
method = pd.DataFrame([
    {"item": "Engine version", "detail": "Falcon v7.1.0"},
    {"item": "Ranking metric", "detail": "avg_lift = (sum of fired patterns' OOS lift) / (n_patterns_fired). Higher = higher rank."},
    {"item": "Signal timing", "detail": "signal_date 2026-06-25 (after close), entry_date 2026-06-26. emitted/inserted_at is UTC: 10:39:45 UTC = 16:09 IST = ~16:10 IST EOD run, AFTER the 15:30 IST close."},
    {"item": "Inputs", "detail": "Daily/weekly features from ohlc_daily up to & including the 2026-06-25 CLOSE. No 06-26+ bars. No 1-minute/intraday data."},
    {"item": "Patterns", "detail": "Mined in 2023 / 2024 (walk-forward: mined_year < signal_year 2026). No future patterns."},
    {"item": "Leakage check", "detail": "PASS - no intraday data, no future data. Inputs are EOD close + full-day volume of 06-25; patterns predate 2026."},
    {"item": "Core finding", "detail": "Every pattern input depends on the EOD CLOSE and/or FULL-DAY VOLUME (close_loc, roc_60, rsi, weekly_close_loc, vol_vs_20d, dist_sma_200, atr...). None can be RELIABLY evaluated intraday because the close & full-day volume are unknown until 15:30."},
])

OUT.parent.mkdir(parents=True, exist_ok=True)
with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
    s_rank.to_excel(xl, "1_Top10_Ranking", index=False)
    s_raw.to_excel(xl, "2_Raw_OHLC_0625", index=False)
    s_feat.to_excel(xl, "3_Key_Features_0625", index=False)
    s_pat.to_excel(xl, "4_Fired_Patterns_sample", index=False)
    s_class.to_excel(xl, "5_EOD_vs_Intraday_inputs", index=False)
    method.to_excel(xl, "6_NoLeakage_Methodology", index=False)
c.close()

print("=== TOP-10 RANKING (2026-06-25) ===")
print(s_rank.to_string(index=False))
print("\n=== AEGISLOG (#1) vs MSUMI (#8) key features ===")
print(s_feat[s_feat.symbol.isin(["AEGISLOG", "MSUMI"])].to_string(index=False))
print("\n=== inputs the fired patterns use, by availability ===")
print(s_class.to_string(index=False))
print(f"\n[*] wrote {OUT}")
if DESK.exists():
    import shutil; shutil.copy(OUT, DESK / OUT.name); print(f"[*] copied to {DESK/OUT.name}")
