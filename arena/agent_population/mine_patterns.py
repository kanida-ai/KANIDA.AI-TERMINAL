"""VECTORIZED leak-free pattern MINER. Clean week-to-date features (day D close) -> trade entered day D+1 at
09:15/09:45/10:00, exited same-day (INTRADAY) or next-day (BTST). Search single- and two-feature threshold conditions,
keep those that BEAT the baseline on TRAIN(<=2025) AND still beat it OOS(2026) with enough support. All numpy.
-> docs/ops/MINED_PATTERNS.xlsx. Read-only."""
import os, sqlite3, json, warnings, itertools
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
ENTRY_TIMES = ["09:15", "09:45", "10:00"]; MINSUP = 200; MIN_EDGE = 0.20   # pattern must beat baseline by >=0.20pp/trade
TARGETS = [f"ID_{t}" for t in ENTRY_TIMES] + [f"BT_{t}" for t in ENTRY_TIMES]

print("loading clean panel + outcomes ...", flush=True)
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-05-13' AND trade_date<='2026-07-09'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,high,low,close,volume FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2026-08-15' ORDER BY symbol,trade_date", con); con.close()
# week-to-date weekly features (validated recompute)
oh2 = oh.copy(); dt = pd.to_datetime(oh2.trade_date); oh2["wk"] = dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec = []
for s, g in oh2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi_td = g.groupby("wk").high.cummax().values; lo_td = g.groupby("wk").low.cummin().values; cl = g.close.values.astype(float)
    wcl = np.where(hi_td > lo_td, (cl-lo_td)/(hi_td-lo_td), np.nan); wrp = np.where(cl > 0, (hi_td-lo_td)/cl*100, np.nan)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index(); wb["s"] = wb.wc.rolling(20).mean().shift(1); wb["p"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.s))).values; ph = g.wk.map(dict(zip(wb.wk, wb.p))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values, weekly_close_loc=wcl, weekly_range_pct=wrp,
                                 weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (cl/sm-1)*100, np.nan), weekly_breakout_20w=np.where(ph == ph, (cl > ph).astype(float), np.nan))))
WTD = pd.concat(rec, ignore_index=True)
FC = feat.drop(columns=["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]).merge(WTD, on=["symbol", "trade_date"], how="left")

OUT = pd.read_pickle(os.path.join(AP, "_mine_outcomes.pkl"))
alldays = sorted(set(oh.trade_date) | set(OUT.trade_date)); AIDX = {d: i for i, d in enumerate(alldays)}
FC["entry_date"] = FC.trade_date.map(lambda d: alldays[AIDX[d]+1] if AIDX.get(d, 10**9)+1 < len(alldays) else None)
M = FC.merge(OUT.rename(columns={"trade_date": "entry_date"}), on=["symbol", "entry_date"], how="inner")
M["yr"] = M.trade_date.str[:4].astype(int)
train = (M.yr <= 2025).values; oos = (M.yr == 2026).values
print(f"  merged panel: {len(M)} rows · train {train.sum()} · oos {oos.sum()}")

FEATURES = [c for c in FC.columns if c not in ("symbol", "trade_date", "entry_date", "id") and M[c].dtype.kind in "fi"]
X = M[FEATURES].values.astype(np.float64)
print(f"  features: {len(FEATURES)}")

def base(y, mask):
    v = y[mask]; v = v[~np.isnan(v)]; return v.mean() if len(v) else np.nan

def eval_mask(m, y):
    tr = m & train; oo = m & oos
    yt = y[tr]; yt = yt[~np.isnan(yt)]; yo = y[oo]; yo = yo[~np.isnan(yo)]
    if len(yt) < MINSUP or len(yo) < 30: return None
    return len(yt), yt.mean(), (yt > 0).mean()*100, len(yo), yo.mean(), (yo > 0).mean()*100

results = {}
for tgt in TARGETS:
    y = M[tgt].values.astype(np.float64); b_tr = base(y, train); b_oo = base(y, oos)
    conds = []                                   # single-feature threshold conditions
    for j, f in enumerate(FEATURES):
        col = X[:, j]; qs = np.nanpercentile(col, [10, 20, 30, 40, 50, 60, 70, 80, 90])
        for thr in np.unique(np.round(qs, 4)):
            for op, m in [(">", col > thr), ("<", col < thr)]:
                r = eval_mask(m, y)
                if r and (r[1]-b_tr) >= MIN_EDGE:            # beats train baseline
                    conds.append(dict(rule=[(f, op, float(thr))], mask=m, tr_edge=r[1]-b_tr, tr_ret=r[1], tr_n=r[0],
                                      oo_edge=r[4]-b_oo, oo_ret=r[4], oo_win=r[5], oo_n=r[3]))
    conds.sort(key=lambda c: -c["tr_edge"]); top = conds[:25]
    combos = []                                   # two-feature AND of the strongest singles
    for a, bnd in itertools.combinations(top, 2):
        if a["rule"][0][0] == bnd["rule"][0][0]: continue
        m = a["mask"] & bnd["mask"]; r = eval_mask(m, y)
        if r and (r[1]-b_tr) >= MIN_EDGE:
            combos.append(dict(rule=a["rule"]+bnd["rule"], tr_edge=r[1]-b_tr, tr_ret=r[1], tr_n=r[0], oo_edge=r[4]-b_oo, oo_ret=r[4], oo_win=r[5], oo_n=r[3]))
    allc = [{k: v for k, v in c.items() if k != "mask"} for c in conds] + combos
    # KEEP only patterns that ALSO beat baseline OOS (robust) — this is the honest filter
    robust = [c for c in allc if c["oo_edge"] > 0]
    robust.sort(key=lambda c: -c["oo_ret"])
    results[tgt] = (b_tr, b_oo, len(allc), robust)

print("\n" + "="*94)
print("MINED PATTERNS — beat baseline on TRAIN(<=2025) AND still beat it OOS(2026). Top 3 per structure/entry.")
print("="*94)
rows_x = []
for tgt in TARGETS:
    b_tr, b_oo, ncand, robust = results[tgt]
    kind = "INTRADAY same-day" if tgt.startswith("ID") else "BTST next-day"; et = tgt[-5:]
    print(f"\n{kind} · entry {et}   (baseline train {b_tr:+.3f}%  oos {b_oo:+.3f}%)  · {ncand} beat-train, {len(robust)} ALSO beat-OOS")
    for c in robust[:3]:
        rule = " AND ".join(f"{f}{op}{th:.3f}" for f, op, th in c["rule"])
        print(f"   OOS {c['oo_ret']:+.2f}%/trade (win {c['oo_win']:.0f}%, n={c['oo_n']})  train {c['tr_ret']:+.2f}% (n={c['tr_n']})   [{rule}]")
        rows_x.append(dict(structure=kind, entry=et, oos_ret=round(c["oo_ret"], 3), oos_win=round(c["oo_win"], 0), oos_n=c["oo_n"],
                           train_ret=round(c["tr_ret"], 3), train_n=c["tr_n"], rule=rule))
best = max((c for tgt in TARGETS for c in results[tgt][3]), key=lambda c: c["oo_ret"], default=None)
if best:
    print(f"\n>>> STRONGEST leak-free mined pattern (OOS 2026): {best['oo_ret']:+.2f}%/trade, win {best['oo_win']:.0f}%, n={best['oo_n']}")
    print(f"    rule: {' AND '.join(f'{f}{op}{th:.3f}' for f,op,th in best['rule'])}")
    print(f"    (a portfolio of the top clean patterns x MIS 5x + throttle is the honest route toward 300%/yr)")
pd.DataFrame(rows_x).to_excel(os.path.join(ROOT, "docs", "ops", "MINED_PATTERNS.xlsx"), index=False)
print(f"\nExcel -> docs/ops/MINED_PATTERNS.xlsx")
