"""
Export Falcon tier classification to Excel.

Sheets:
  1. 6_18_top10      - today's (2026-06-18) Falcon Top 10, classified under the
                       DERIVED tiers (v1 dry-up + v2 pullback/compression edges).
  2. full_trade_log  - all 10,063 resolved historical signals, each row labeled
                       with its tier + signal-day features + outcome.
  3. tier_summary    - per-tier rollup (N / WR / avg ret / stop), split IS/OOS.
  4. readme          - rule definitions + honesty notes.

Tiers are SIGNAL-TIME-SAFE (entry_gap excluded = look-ahead). Read-only on DBs.
Run: "C:\\Users\\SPS\\anaconda3\\python.exe" scripts/export_tier_excel.py
"""
import os, sqlite3
import numpy as np, pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RND_DB  = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
PROD_DB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT_DIR = os.path.join(ROOT, "outputs"); os.makedirs(OUT_DIR, exist_ok=True)
OUT_XLSX = os.path.join(OUT_DIR, "tier_classification.xlsx")
TODAY = "2026-06-18"
OOS_CUTOFF = "2025-12-31"

# ----------------------------------------------------------------------------
# shared volume-feature engineering (signal-time-safe, backward windows)
# ----------------------------------------------------------------------------
def vol_features(db, symbols):
    con = sqlite3.connect(db)
    oh = pd.read_sql_query(
        "SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE symbol IN (%s)"
        % ",".join("?"*len(symbols)), con, params=tuple(symbols))
    con.close()
    oh = oh.sort_values(["symbol","trade_date"]).reset_index(drop=True)
    g = oh.groupby("symbol", group_keys=False)
    oh["avg20"] = g["volume"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    oh["std20"] = g["volume"].transform(lambda s: s.rolling(20, min_periods=10).std())
    oh["avg3"]  = g["volume"].transform(lambda s: s.rolling(3,  min_periods=2).mean())
    oh["turn"]  = oh["close"]*oh["volume"]
    oh["turn_pct"] = g["turn"].transform(
        lambda s: s.rolling(252, min_periods=60).apply(lambda w:(w<=w[-1]).mean(), raw=True))
    oh["prev_close"]      = g["close"].shift(1)
    oh["prev_prev_close"] = g["close"].shift(2)
    oh["rvol20"]    = oh["volume"]/oh["avg20"]
    oh["volz"]      = (oh["volume"]-oh["avg20"])/oh["std20"]
    oh["trend3_20"] = oh["avg3"]/oh["avg20"]
    oh["sret"]   = (oh["close"]/oh["prev_close"]-1)*100
    oh["twoday"] = (oh["close"]/oh["prev_prev_close"]-1)*100
    oh["rng"]    = (oh["high"]-oh["low"])/oh["prev_close"]*100
    return oh

# ----------------------------------------------------------------------------
# the DERIVED tier classifier (layered best -> worst, signal-time-safe)
# ----------------------------------------------------------------------------
def classify(sret, twoday, rng, avg_lift, trend3_20, turn_pct):
    def ok(v): return v is not None and not (isinstance(v,float) and np.isnan(v))
    if ok(sret) and sret > 10: return "AVOID"
    # froth from >+7% only (5-7% high-turn is +edge; fix 2026-06-19)
    if ok(sret) and sret > 7 and ok(turn_pct) and turn_pct >= 0.75: return "AVOID"
    if ok(sret) and sret <= 2 and ok(twoday) and twoday < -5 and ok(avg_lift) and avg_lift > 15:
        return "PREMIUM-Pullback"
    if ok(sret) and sret <= 2 and ok(rng) and rng < 2 and ok(avg_lift) and avg_lift > 15:
        return "PREMIUM-Compression"
    if ok(sret) and sret <= 2 and ok(trend3_20) and trend3_20 < 0.9:
        return "ENTERPRISE-Dryup"
    if ok(sret) and sret <= 2 and ok(turn_pct) and turn_pct < 0.75:
        return "GOLD"
    if ok(sret) and sret <= 2: return "GOLD-baseline"
    if ok(sret) and sret <= 5: return "STANDARD"
    return "STANDARD-weak"

TIER_ORDER = ["PREMIUM-Pullback","PREMIUM-Compression","ENTERPRISE-Dryup",
              "GOLD","GOLD-baseline","STANDARD","STANDARD-weak","AVOID"]

# ----------------------------------------------------------------------------
# 1. 6/18 top-10
# ----------------------------------------------------------------------------
def build_today():
    con = sqlite3.connect(PROD_DB)
    live = pd.read_sql_query(
        "SELECT rank,symbol,sector,n_fires,score,close_at_signal "
        "FROM falcon_signals_live WHERE signal_date=? AND n_fires>=10", con, params=(TODAY,))
    con.close()
    live["avg_lift"] = live["score"]/live["n_fires"]
    live = live.sort_values("avg_lift", ascending=False).head(10).reset_index(drop=True)
    vf = vol_features(PROD_DB, live["symbol"].tolist())
    day = vf[vf["trade_date"]==TODAY].set_index("symbol")
    rows = []
    for i, r in live.iterrows():
        d = day.loc[r["symbol"]] if r["symbol"] in day.index else None
        sret  = d["sret"] if d is not None else None
        twod  = d["twoday"] if d is not None else None
        rng   = d["rng"] if d is not None else None
        t320  = d["trend3_20"] if d is not None else None
        tpct  = d["turn_pct"] if d is not None else None
        rv20  = d["rvol20"] if d is not None else None
        tier  = classify(sret, twod, rng, r["avg_lift"], t320, tpct)
        rows.append(dict(rank=i+1, symbol=r["symbol"], sector=r["sector"],
                         score=round(r["score"],1), avg_lift=round(r["avg_lift"],2),
                         n_fires=int(r["n_fires"]),
                         signal_day_ret_pct=round(sret,2) if sret==sret else None,
                         two_day_ret_pct=round(twod,2) if twod==twod else None,
                         range_pct=round(rng,2) if rng==rng else None,
                         rvol20=round(rv20,2) if rv20==rv20 else None,
                         trend3_20=round(t320,2) if t320==t320 else None,
                         turn_pct=round(tpct,2) if tpct==tpct else None,
                         TIER=tier))
    return pd.DataFrame(rows)

# ----------------------------------------------------------------------------
# 2. full trade log (10,063)
# ----------------------------------------------------------------------------
def build_full_log():
    con = sqlite3.connect(RND_DB)
    df = pd.read_sql_query(
        """SELECT s.signal_date, s.symbol, s.sector, s.avg_lift, s.engine_rank, s.n_fires,
                  x.signal_day_ret_pct AS sret, x.two_day_ret_pct AS twoday,
                  x.signal_day_range_pct AS rng, s.entry_price, s.exit_price,
                  s.net_ret_pct AS net_ret, s.exit_reason, s.hold_days_trading
           FROM falcon_signal_day_study s
           JOIN falcon_signal_day_context x
             ON s.signal_date=x.signal_date AND s.symbol=x.symbol
           WHERE s.net_ret_pct IS NOT NULL AND x.is_extension=0""", con)
    con.close()
    vf = vol_features(PROD_DB, df["symbol"].unique().tolist())
    key = vf.set_index(["symbol","trade_date"])
    def lookup(row, col):
        try: return key.loc[(row["symbol"], row["signal_date"]), col]
        except Exception: return np.nan
    for col in ["trend3_20","turn_pct","rvol20","volz"]:
        df[col] = df.apply(lambda r: lookup(r, col), axis=1)
    df["TIER"] = df.apply(lambda r: classify(r["sret"], r["twoday"], r["rng"],
                          r["avg_lift"], r["trend3_20"], r["turn_pct"]), axis=1)
    df["win"]  = (df["net_ret"]>0).astype(int)
    df["stop"] = (df["exit_reason"]=="INIT_STOP").astype(int)
    df["window"] = np.where(df["signal_date"]<=OOS_CUTOFF, "IS(train<=2025)", "OOS(2026)")
    return df

# ----------------------------------------------------------------------------
# 3. tier summary
# ----------------------------------------------------------------------------
def build_summary(full):
    def agg(g):
        return pd.Series(dict(N=len(g), WR_pct=round(100*g["win"].mean(),1),
                              avg_ret_pct=round(g["net_ret"].mean(),2),
                              stop_pct=round(100*g["stop"].mean(),1)))
    overall = full.groupby("TIER").apply(agg).reindex(TIER_ORDER).reset_index()
    piv_n  = full.pivot_table(index="TIER", columns="window", values="win", aggfunc="size", fill_value=0)
    piv_wr = full.pivot_table(index="TIER", columns="window", values="win", aggfunc="mean")*100
    piv_wr = piv_wr.round(1)
    piv_n.columns  = [f"N_{c}" for c in piv_n.columns]
    piv_wr.columns = [f"WR_{c}" for c in piv_wr.columns]
    out = overall.merge(piv_n, on="TIER", how="left").merge(piv_wr, on="TIER", how="left")
    return out.reindex([overall.index[overall["TIER"]==t][0] for t in TIER_ORDER
                        if t in set(overall["TIER"])]).reset_index(drop=True)

def build_readme():
    rows = [
        ("DERIVED tiers (signal-time-safe; entry_gap EXCLUDED = look-ahead)", ""),
        ("PREMIUM-Pullback",   "sret<=2 & two_day_ret<-5 & avg_lift>15  | bounce-from-drawdown; 83-85% WR every year, ~0-6% stop (v2)"),
        ("PREMIUM-Compression","sret<=2 & range<2 & avg_lift>15          | narrow-candle coil; ~83% IS"),
        ("ENTERPRISE-Dryup",   "sret<=2 & 3d/20d-vol<0.9                 | volume dry-up; ~74% OOS (v1)"),
        ("GOLD",               "sret<=2 & turnover-%ile<0.75             | ~71% OOS"),
        ("GOLD-baseline",      "sret<=2                                  | ~66-70%; operator's flat/down floor"),
        ("STANDARD",           "2<sret<=5                                | ~58% OOS"),
        ("STANDARD-weak",      "5<sret<=10 (not froth)                   | weak"),
        ("AVOID",              "sret>10, or sret>5 & turnover-%ile>=0.75 | overextended/froth"),
        ("", ""),
        ("HONESTY NOTE", "PREMIUM-Pullback is ~83-85% in-sample and >=80% in 4/5 years, but the"),
        ("", "strict 2026 walk-forward holdout is thin (N~34) so 80% is NOT yet CERTIFIED."),
        ("", "Real edge; needs leave-one-year-out CV or more 2026 data to certify."),
        ("DBs", "RND=universe_engine/data/db/kanida_universe.db ; PROD=data/db/kanida_universe.db (read-only)"),
    ]
    return pd.DataFrame(rows, columns=["item","definition / note"])

def main():
    print("Building 6/18 top-10 ..."); today = build_today()
    print(today[["rank","symbol","signal_day_ret_pct","two_day_ret_pct","avg_lift","TIER"]].to_string(index=False))
    print("\nBuilding full trade log (10,063) ..."); full = build_full_log()
    print("  tier counts:\n", full["TIER"].value_counts().reindex(TIER_ORDER).fillna(0).astype(int).to_string())
    print("Building summary ..."); summ = build_summary(full)

    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as xl:
        today.to_excel(xl, sheet_name="6_18_top10", index=False)
        summ.to_excel(xl, sheet_name="tier_summary", index=False)
        full_out = full[["signal_date","symbol","sector","TIER","window","engine_rank",
                         "avg_lift","n_fires","sret","twoday","rng","rvol20","trend3_20",
                         "turn_pct","entry_price","exit_price","net_ret","exit_reason",
                         "hold_days_trading","win","stop"]].copy()
        full_out.to_excel(xl, sheet_name="full_trade_log", index=False)
        build_readme().to_excel(xl, sheet_name="readme", index=False)
    print(f"\nWROTE: {OUT_XLSX}")
    print("rows:", len(full), "| 6/18 picks:", len(today))

if __name__ == "__main__":
    main()
