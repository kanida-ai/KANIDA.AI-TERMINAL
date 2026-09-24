"""LEAK-FREE re-validation of the combined agent FALCPAT_7164 ∩ 7619 (and its two constituents).
Applies the SAME standard the podium now uses:
  (1) both rules use weekly_* features -> may fire ONLY on the last session of the ISO week (no Monday-knows-Friday);
  (2) TRUE OUT-OF-SAMPLE -> only dates AFTER max(mined_year) = 2024, i.e. 2025-01..2026-07;
  (3) native target model (hit_10pc_20d: +10% within 20d else exit at 20d close), net 0.15%/trade;
  (4) profitable gate -> expectancy>0 AND PF>1.5 to be 'sellable'.
Combined rule = AND of both, de-duplicated on shared features by taking the TIGHTER threshold.
Read-only on all sources. Writes nothing unless --register is passed (then -> combo_agents table)."""
import os, sys, sqlite3, json
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
CAP = 5e5; COST = 0.15; PCT, HOR = 10, 20            # hit_10pc_20d
OOS_AFTER = 2024                                      # max(mined_year of 7164=2023, 7619=2024)
WINLO, WINHI = "2025-01-01", "2026-07-31"

uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01'", uc)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,close FROM ohlc_daily WHERE trade_date>='2024-06-01' ORDER BY symbol,trade_date", uc)
uc.close()

outs = []
for s, g in oh.groupby("symbol", sort=False):
    O = g.open.values.astype(float); H = g.high.values; C = g.close.values
    entry = np.roll(O, -1); entry[-1] = np.nan
    fmax = pd.Series(H).rolling(HOR).max().shift(-HOR).values; cH = pd.Series(C).shift(-HOR).values
    hit = fmax >= entry*(1+PCT/100)
    ret = np.where(hit, float(PCT), (cH-entry)/entry*100) - COST
    bad = np.isnan(entry) | np.isnan(cH); ret = ret.astype(float); ret[bad] = np.nan
    hh = hit.astype(float); hh[bad] = np.nan
    outs.append(pd.DataFrame({"symbol": s, "trade_date": g.trade_date.values, "r": ret, "hit": hh}))
M = feat.merge(pd.concat(outs, ignore_index=True), on=["symbol", "trade_date"], how="inner").reset_index(drop=True)
M["yr"] = M.trade_date.str[:4].astype(int); M["ym"] = M.trade_date.str[:7]
cal = pd.DataFrame({"d": sorted(M.trade_date.unique())}); dd = pd.to_datetime(cal.d)
cal["wk"] = dd.dt.isocalendar().year.astype(str)+"-"+dd.dt.isocalendar().week.astype(str)
cal["we"] = cal.wk != cal.wk.shift(-1); M["we"] = M.trade_date.map(dict(zip(cal.d, cal.we)))

OOS = (M.trade_date >= WINLO) & (M.trade_date <= WINHI) & (M.yr > OOS_AFTER)
BASE = M.loc[OOS, "hit"].mean()*100      # base rate of the target over the same OOS window

VARIANTS = {
 "7164 alone  (Rapidro)": (M.atr_20_pct > 2.2284) & (M.weekly_close_loc > 0.5467) & (M.weekly_range_pct > 13.298),
 "7619 alone  (Rapidus)": (M.roc_5 <= 3.643) & (M.weekly_close_loc > 0.5098) & (M.weekly_range_pct > 14.1572),
 "7164 ∩ 7619 (COMBO)":   (M.atr_20_pct > 2.2284) & (M.roc_5 <= 3.643) & (M.weekly_close_loc > 0.5467) & (M.weekly_range_pct > 14.1572),
}

def stats(mask, weekend_only=True):
    idx = mask.values & OOS.values & (M.we.values if weekend_only else True)
    sub = M.loc[idx, ["trade_date", "ym", "r", "hit"]].dropna().sort_values("trade_date")
    if len(sub) < 5: return None
    r = sub.r.values; wins = r[r > 0]; losses = r[r <= 0]
    pf = wins.sum()/-losses.sum() if losses.sum() < 0 else 99.0
    eq = CAP; peak = CAP; mdd = 0.0; free = None
    for dt, rr in zip(sub.trade_date.values, r):
        if free is not None and dt < free: continue
        eq = max(eq*(1+rr/100), 1.0); peak = max(peak, eq); mdd = max(mdd, (peak-eq)/peak)
        free = (pd.Timestamp(dt)+pd.Timedelta(days=int(HOR*1.5))).strftime("%Y-%m-%d")
    mos = sub.ym.nunique(); cagr = ((eq/CAP)**(1/max(mos/12, 1e-6))-1)*100
    hitr = sub.hit.mean()*100
    return dict(trades=len(r), win_pct=(r > 0).mean()*100, expectancy=r.mean(), pf=round(float(pf), 2),
                avg_win=wins.mean() if len(wins) else 0, avg_loss=losses.mean() if len(losses) else 0,
                hit_rate=hitr, lift_pp=hitr-BASE, maxdd=mdd*100, cagr=cagr, months=mos,
                sellable=bool(r.mean() > 0 and pf > 1.5))

print(f"LEAK-FREE RE-VALIDATION · target +{PCT}% in {HOR}d · net {COST}%/trade")
print(f"TRUE OOS: {WINLO}..{WINHI} AND year>{OOS_AFTER} (after both mining years) · weekly agents fire WEEK-END ONLY")
print(f"Base rate of target over this OOS window: {BASE:.1f}%\n")
print(f"{'variant':<24}{'trades':>7}{'win%':>7}{'exp%':>8}{'PF':>6}{'hit%':>7}{'lift':>8}{'maxDD':>7}{'CAGR':>8}  sellable")
res = {}
for name, mask in VARIANTS.items():
    s = stats(mask); res[name] = s
    if not s: print(f"{name:<24}  (too few signals)"); continue
    print(f"{name:<24}{s['trades']:>7}{s['win_pct']:>7.0f}{s['expectancy']:>8.2f}{s['pf']:>6.2f}{s['hit_rate']:>7.1f}{s['lift_pp']:>+8.1f}{s['maxdd']:>7.0f}{s['cagr']:>+8.0f}   {'YES' if s['sellable'] else 'no'}")

# leaky-vs-clean delta (what the old, pre-fix number looked like)
print("\nLeak check — same combo WITHOUT the week-end fix (the OLD, inflated way):")
old = stats(VARIANTS["7164 ∩ 7619 (COMBO)"], weekend_only=False)
new = res["7164 ∩ 7619 (COMBO)"]
if old and new:
    print(f"  leaky : {old['trades']:>5} trades · win {old['win_pct']:.0f}% · exp {old['expectancy']:+.2f}% · PF {old['pf']:.2f} · lift {old['lift_pp']:+.1f}pp")
    print(f"  clean : {new['trades']:>5} trades · win {new['win_pct']:.0f}% · exp {new['expectancy']:+.2f}% · PF {new['pf']:.2f} · lift {new['lift_pp']:+.1f}pp")
    print(f"  the leak was worth {old['expectancy']-new['expectancy']:+.2f}pp of expectancy and {old['lift_pp']-new['lift_pp']:+.1f}pp of lift")

# verdict
c = res["7164 ∩ 7619 (COMBO)"]; a = res["7164 alone  (Rapidro)"]; b = res["7619 alone  (Rapidus)"]
print("\nVERDICT:")
if c and a and b:
    best_alone = max(a["expectancy"], b["expectancy"])
    print(f"  combo expectancy {c['expectancy']:+.2f}%/trade vs best constituent {best_alone:+.2f}%/trade "
          f"-> {'ADDS' if c['expectancy']>best_alone else 'does NOT add'} edge over either alone")
    print(f"  profitable gate (exp>0 & PF>1.5): {'PASS' if c['sellable'] else 'FAIL'}")
    print(f"  frequency cost: {a['trades']} -> {c['trades']} trades ({(1-c['trades']/max(a['trades'],1))*100:.0f}% fewer signals)")

if "--register" in sys.argv and c and c["sellable"]:
    NAME = sys.argv[sys.argv.index("--register")+1] if len(sys.argv) > sys.argv.index("--register")+1 else "Concord"
    rule = [["atr_20_pct", ">", 2.2284], ["roc_5", "<=", 3.643], ["weekly_close_loc", ">", 0.5467], ["weekly_range_pct", ">", 14.1572]]
    row = dict(agent_id="FALCOMB_7164x7619", codename=NAME, members="FALCPAT_7164 ∩ FALCPAT_7619",
               kind="intersection", target="hit_10pc_20d", regime="breakout", rule_json=json.dumps(rule),
               uses_weekly=1, oos_after=OOS_AFTER, trades=c["trades"], win_pct=c["win_pct"], expectancy=c["expectancy"],
               profit_factor=c["pf"], hit_rate=c["hit_rate"], lift_pp=c["lift_pp"], maxdd=c["maxdd"], cagr=c["cagr"],
               verified=int(c["sellable"]))
    con = sqlite3.connect(os.path.join(AP, "arena_metrics.db"))
    try: ex = pd.read_sql_query("SELECT * FROM combo_agents", con)
    except Exception: ex = pd.DataFrame()
    ex = ex[ex.agent_id != row["agent_id"]] if len(ex) else ex
    pd.concat([ex, pd.DataFrame([row])], ignore_index=True).to_sql("combo_agents", con, if_exists="replace", index=False)
    con.close()
    print(f"\nREGISTERED as '{NAME}' (FALCOMB_7164x7619) in combo_agents.")
