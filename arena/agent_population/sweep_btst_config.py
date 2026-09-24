"""SWEEP the campaign config for profitability, leak-free, across Jan-Jul 2026 (7 months, point-in-time).

Rebuild Top-15 each signal day (production engine, week-to-date features -> leak-free every month).
For each firing position precompute long returns for holds {1,2,3,5,10,20} and an intraday-short return,
with/without the -6% disaster stop. Then aggregate every (filter x hold x direction x stop) config.

FAIR COMPARATOR = capital-normalized monthly return:
  daily entry + h-session hold => h overlapping baskets => pool = h * Rs5L.
  monthly return on capital = (sum of basket% that month) / h.
Rank by MEAN monthly return on capital AND positive-month count (7 = every month green = robust).
Cost: intraday (hold=1) 0.15% ; positional (hold>=2) 0.30%. Read-only.
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
HOLDS = [1, 2, 3, 5, 10, 20]; STOPPX = -6.0
GOLD = {"GOLD", "GOLD-baseline"}; ENTPREM = {"ENTERPRISE-Dryup", "PREMIUM-Pullback", "PREMIUM-Compression"}
HIGH = GOLD | ENTPREM


def classify(sret, twoday, rng, avg_lift, trend3_20, turn_pct):
    if sret is None or not np.isfinite(sret): return "UNKNOWN"
    if sret > 10: return "AVOID"
    if sret > 7 and np.isfinite(turn_pct or np.nan) and turn_pct >= 0.75: return "AVOID"
    if sret <= 2 and np.isfinite(twoday or np.nan) and twoday < -5 and avg_lift and avg_lift > 15: return "PREMIUM-Pullback"
    if sret <= 2 and np.isfinite(rng or np.nan) and rng < 2 and avg_lift and avg_lift > 15: return "PREMIUM-Compression"
    if sret <= 2 and np.isfinite(trend3_20 or np.nan) and trend3_20 < 0.9: return "ENTERPRISE-Dryup"
    if sret <= 2 and np.isfinite(turn_pct or np.nan) and turn_pct < 0.75: return "GOLD"
    if sret <= 2: return "GOLD-baseline"
    if sret <= 5: return "STANDARD"
    return "STANDARD-weak"


con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2025-06-01' AND trade_date<='2026-07-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2024-06-01' AND trade_date<='2026-08-31' ORDER BY symbol,trade_date", con)
con.close()

o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date)
o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []; TF = {}; SYM = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    hi = g.groupby("wk").high.cummax().values; lo = g.groupby("wk").low.cummin().values
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float); v = g.volume.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index()
    wb["sm"] = wb.wc.rolling(20).mean().shift(1); wb["ph"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values; ph = g.wk.map(dict(zip(wb.wk, wb.ph))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan),
        weekly_range_pct=np.where(c > 0, (hi - lo) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan),
        weekly_breakout_20w=np.where(ph == ph, (c > ph).astype(float), np.nan))))
    pc = np.roll(c, 1); pc[0] = np.nan; c2 = np.roll(c, 2); c2[:2] = np.nan
    sret = (c / pc - 1) * 100; rng = (h - l) / pc * 100; twoday = (c / c2 - 1) * 100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values
    tr3 = np.where(av20 > 0, av3 / av20, np.nan)
    tp = pd.Series(c * v).rolling(252, min_periods=60).apply(lambda w: (w <= w[-1]).mean(), raw=True).values
    for i, d in enumerate(g.trade_date.values): TF[(s, d)] = (sret[i], rng[i], twoday[i], tr3[i], tp[i])
    SYM[s] = dict(o=g.open.values.astype(float), h=h, l=l, c=c, idx={d: i for i, d in enumerate(g.trade_date)}, n=len(g))
FCpit = feat.drop(columns=WEEKLY).merge(pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")


def pos_returns(sym, sigday):
    """returns dict: long[h] (stop & nostop), short1 (stop & nostop). Net of cost. None entries where no data."""
    S = SYM.get(sym); i = S["idx"].get(sigday) if S else None
    if i is None or i + 1 >= S["n"]: return None
    e = S["o"][i + 1]
    if e <= 0: return None
    out = {"L": {}, "Lns": {}}
    for hh in HOLDS:
        xi = i + hh
        if xi >= S["n"]: out["L"][hh] = None; out["Lns"][hh] = None; continue
        cost = 0.15 if hh == 1 else 0.30
        gross = (S["c"][xi] / e - 1) * 100
        out["Lns"][hh] = gross - cost
        lowmin = S["l"][i + 1:xi + 1].min()
        out["L"][hh] = (STOPPX - cost) if lowmin <= e * (1 + STOPPX / 100) else gross - cost
    # intraday short (hold=1): profit if close<entry ; +6% disaster stop on the upside
    xi = i + 1
    gshort = (e / S["c"][xi] - 1) * 100
    out["S"] = (STOPPX - 0.15) if S["h"][xi] >= e * (1 - STOPPX / 100) else gshort - 0.15
    out["Sns"] = gshort - 0.15
    return out


def basket(day):
    fd = FCpit[FCpit.trade_date == day]
    if fd.empty: return []
    syms = fd.symbol.values
    X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
    for j, col in enumerate(FR.FEATURE_COLS):
        if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
    yr = int(day[:4]); elig = [p for p in pats if int(p["mined_year"]) < yr]
    fire = np.zeros(len(syms), np.int32); score = np.zeros(len(syms))
    for p in elig:
        m = FR.rule_mask(p["rule"], X)
        if not m.any(): continue
        fire += m.astype(np.int32); score += m.astype(np.float64) * p["oos_lift"]
    cands = [{"symbol": syms[i], "n_fires": int(fire[i]), "score": float(score[i])}
             for i in range(len(syms)) if fire[i] >= 10]
    cands.sort(key=lambda c: -c["score"])
    ranked = sorted(cands[:100], key=lambda c: -(c["score"] / max(c["n_fires"], 1)))[:15]
    out = []
    for rk, c in enumerate(ranked, 1):
        al = c["score"] / max(c["n_fires"], 1); tf = TF.get((c["symbol"], day), (np.nan,) * 5)
        out.append((c["symbol"], rk, classify(tf[0], tf[2], tf[1], al, tf[3], tf[4])))
    return out


FILTERS = {
    "TOP15_all": lambda rk, ti: True,
    "high_tier": lambda rk, ti: ti in HIGH,
    "GOLD_only": lambda rk, ti: ti in GOLD,
    "ENT_PREM":  lambda rk, ti: ti in ENTPREM,
    "TOP5_rank": lambda rk, ti: rk <= 5,
    "TOP3_rank": lambda rk, ti: rk <= 3,
}
sigdays = sorted(FCpit[(FCpit.trade_date >= "2026-01-01") & (FCpit.trade_date <= "2026-07-31")].trade_date.unique())
print(f"sweeping {len(sigdays)} signal days x {len(FILTERS)} filters x {len(HOLDS)} holds x long/short x stop/nostop ...", flush=True)

# accumulate: config -> month -> list of basket returns
from collections import defaultdict
acc = defaultdict(lambda: defaultdict(list))
for d in sigdays:
    mo = d[:7]; bk = basket(d)
    if not bk: continue
    pr = {s: pos_returns(s, d) for s, _, _ in bk}
    for fname, fn in FILTERS.items():
        names = [(s, rk, ti) for (s, rk, ti) in bk if fn(rk, ti)]
        if not names: continue
        for hh in HOLDS:
            for stkey, dkey in [("stop", "L"), ("nostop", "Lns")]:
                vals = [pr[s][dkey][hh] for s, _, _ in names if pr[s] and pr[s][dkey][hh] is not None]
                if vals: acc[(fname, "long", hh, stkey)][mo].append(float(np.mean(vals)))
        # intraday short (hold=1 only)
        for stkey, dkey in [("stop", "S"), ("nostop", "Sns")]:
            vals = [pr[s][dkey] for s, _, _ in names if pr[s] and pr[s].get(dkey) is not None]
            if vals: acc[(fname, "short", 1, stkey)][mo].append(float(np.mean(vals)))

rows = []
for (fname, dirn, hh, stop), bymo in acc.items():
    monthly_roc = []; nb = 0; allb = []
    for mo, lst in bymo.items():
        monthly_roc.append(sum(lst) / hh)   # capital-normalized monthly return %
        nb += len(lst); allb += lst
    monthly_roc = np.array(monthly_roc)
    rows.append(dict(filter=fname, dir=dirn, hold=hh, stop=stop, n_baskets=nb,
                     avg_basket_pct=round(float(np.mean(allb)), 3),
                     win_pct=round(float((np.array(allb) > 0).mean() * 100), 0),
                     mean_monthly_roc=round(float(np.mean(monthly_roc)), 3),
                     pos_months=int((monthly_roc > 0).sum()), tot_months=len(monthly_roc),
                     total_roc_7mo=round(float(np.sum(monthly_roc)), 2)))
R = pd.DataFrame(rows)

def show(title, df):
    print("\n" + "=" * 104); print("  " + title); print("=" * 104)
    print(f"  {'filter':<11}{'dir':<6}{'hold':>5}{'stop':>8}{'baskets':>8}{'avgBskt%':>10}{'win%':>6}{'mMoROC%':>9}{'posMo':>7}{'7moROC%':>9}")
    for _, x in df.iterrows():
        print(f"  {x['filter']:<11}{x['dir']:<6}{x.hold:>5}{x.stop:>8}{x.n_baskets:>8}{x.avg_basket_pct:>+10.2f}{x.win_pct:>5.0f}%{x.mean_monthly_roc:>+9.2f}{x.pos_months:>4}/{x.tot_months}{x.total_roc_7mo:>+9.2f}")

# robust ranking: require positive mean monthly ROC, then sort by pos_months then mean ROC
robust = R[(R.mean_monthly_roc > 0)].sort_values(["pos_months", "mean_monthly_roc"], ascending=False)
show("TOP 20 CONFIGS by robustness (positive months, then capital-normalized monthly return)", robust.head(20))
show("TOP 12 by raw mean-monthly return on capital", R.sort_values("mean_monthly_roc", ascending=False).head(12))
base = R[(R['filter'] == "high_tier") & (R.dir == "long") & (R.hold == 2) & (R.stop == "stop")]
show("BASELINE (your config: high_tier long hold-2 stop)", base)

out = os.path.join(os.path.expanduser("~"), "Downloads", "FALCON_CONFIG_SWEEP_2026.xlsx")
R.sort_values("mean_monthly_roc", ascending=False).to_excel(out, index=False)
print(f"\nfull sweep ({len(R)} configs) -> {out}")
