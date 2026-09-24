"""Quality-filter test (full OOS history). Does gating 7164's flow with 7619's conditions lift the edge?
Variants (all target hit_10pc_20d: buy next open, +10% within 20d else 20d close, 0.15% cost):
  plain_7164              : ATR%>2.2284 & wkloc>0.5467 & wkrange>13.298
  7164_x_7619 (confirm)   : plain_7164 AND 7619's full rule
  7164_+roc5gate          : plain_7164 AND roc_5<=3.643  (just 7619's key 'not-overextended' lever)
  plain_7619              : roc_5<=3.643 & wkloc>0.5098 & wkrange>14.1572
OOS windows: 2025-2026 (post both mined_years) and 2026-only (cleanest). Rs5L one-at-a-time (20d cooldown). Read-only."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
CAP = 5e5; PCT = 10; H = 20; COST = 0.15
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT symbol,trade_date,atr_20_pct,weekly_close_loc,weekly_range_pct,roc_5 FROM falcon_features WHERE trade_date>='2018-01-01'", uc)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,close FROM ohlc_daily WHERE trade_date>='2018-01-01' ORDER BY symbol,trade_date", uc)
uc.close()
# precompute target-hit outcome per (symbol,date)
outs = []
for s, g in oh.groupby("symbol", sort=False):
    O = g.open.values; Hh = g.high.values; C = g.close.values
    entry = np.roll(O, -1).astype(float); entry[-1] = np.nan
    fmax = pd.Series(Hh).rolling(H).max().shift(-H).values; cH = pd.Series(C).shift(-H).values
    hit = (fmax >= entry * (1 + PCT / 100)).astype(float)
    ret = np.where(hit > 0, float(PCT), (cH - entry) / entry * 100) - COST
    bad = np.isnan(entry) | np.isnan(cH); hit[bad] = np.nan; ret[bad] = np.nan
    outs.append(pd.DataFrame({"symbol": s, "trade_date": g.trade_date.values, "hit": hit, "ret": ret}))
M = feat.merge(pd.concat(outs, ignore_index=True), on=["symbol", "trade_date"], how="inner")
M["yr"] = M.trade_date.str[:4].astype(int)

VARIANTS = {
 "plain_7164":      (M.atr_20_pct > 2.2284) & (M.weekly_close_loc > 0.5467) & (M.weekly_range_pct > 13.298),
 "7164_x_7619":     (M.atr_20_pct > 2.2284) & (M.weekly_close_loc > 0.5467) & (M.weekly_range_pct > 13.298) & (M.roc_5 <= 3.643) & (M.weekly_close_loc > 0.5098) & (M.weekly_range_pct > 14.1572),
 "7164_+roc5gate":  (M.atr_20_pct > 2.2284) & (M.weekly_close_loc > 0.5467) & (M.weekly_range_pct > 13.298) & (M.roc_5 <= 3.643),
 "plain_7619":      (M.roc_5 <= 3.643) & (M.weekly_close_loc > 0.5098) & (M.weekly_range_pct > 14.1572),
}

def acct(sub):
    sub = sub.dropna(subset=["ret"]).sort_values("trade_date"); eq = CAP; peak = CAP; mdd = 0; free = None; taken = 0
    for d, r in zip(sub.trade_date.values, sub.ret.values):
        if free is not None and d < free: continue
        eq = max(eq * (1 + r / 100), 1.0); peak = max(peak, eq); mdd = max(mdd, (peak - eq) / peak); taken += 1
        free = (pd.Timestamp(d) + pd.Timedelta(days=30)).strftime("%Y-%m-%d")
    span = max(1, sub.yr.max() - sub.yr.min() + 1) if len(sub) else 1
    return eq / CAP * 100 - 100, ((eq / CAP) ** (1 / span) - 1) * 100 if eq > 0 else np.nan, mdd * 100, taken

for wlabel, wmask in [("OOS 2025-2026", M.yr >= 2025), ("CLEAN 2026-only", M.yr == 2026)]:
    base = M.loc[wmask, "hit"].mean() * 100
    print("\n" + "=" * 104 + f"\n{wlabel}   (base hit-rate for +10%/20d = {base:.1f}%)\n" + "=" * 104)
    print(f"{'variant':<18}{'fires':>7}{'hit%':>7}{'lift_pp':>9}{'avg_ret%':>10}{'win%':>7}{'med_ret':>9}{'5L_ROC%':>9}{'5L_CAGR%':>10}{'maxDD%':>8}{'acct_trd':>9}")
    for name, mask in VARIANTS.items():
        sub = M[mask & wmask]
        if len(sub) == 0: print(f"{name:<18}  no fires"); continue
        roc, cagr, mdd, taken = acct(sub[["trade_date", "ret", "yr"]])
        print(f"{name:<18}{len(sub):>7}{sub.hit.mean()*100:>7.1f}{(sub.hit.mean()*100-base):>+9.1f}{sub.ret.mean():>+10.2f}{(sub.ret>0).mean()*100:>7.0f}{sub.ret.median():>+9.2f}{roc:>+9.0f}{cagr:>+10.1f}{mdd:>8.0f}{taken:>9}")
