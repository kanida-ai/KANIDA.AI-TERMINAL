"""CLEAN BTST PORTFOLIO — built from freshly-mined leak-free patterns. Patterns selected on TRAIN(<=2025) ONLY;
2026 is a pure out-of-sample test. Clean week-to-date features -> signal day D -> enter day D+1 @ 09:45 -> exit D+2 close
(2-session BTST), net 0.30%. Daily basket = stocks where >=2 selected patterns agree, top 15 by agreement.
Rs10L = 2 x Rs5L sleeves, continuous roll, compounded. Reports the honest 2026 monthly. Vectorized. Read-only."""
import os, sqlite3, itertools, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
OPS = {">": np.greater, "<": np.less}; MINSUP = 200; MIN_EDGE = 0.20; N_PATTERNS = 40; BASKET = 15; CONSENSUS = 2
POOL = 1_000_000.0; SLEEVE = POOL/2; TGT = "BT_09:45"

print("building clean panel + outcomes ...", flush=True)
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-05-13' AND trade_date<='2026-07-09'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,high,low,close FROM ohlc_daily WHERE trade_date>='2023-06-01' AND trade_date<='2026-08-15' ORDER BY symbol,trade_date", con); con.close()
oh2 = oh.copy(); dt = pd.to_datetime(oh2.trade_date); oh2["wk"] = dt.dt.isocalendar().year.astype(int)*100+dt.dt.isocalendar().week.astype(int)
rec = []
for s, g in oh2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True); hi_td = g.groupby("wk").high.cummax().values; lo_td = g.groupby("wk").low.cummin().values; cl = g.close.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last"), wh=("high", "max")).reset_index(); wb["s"] = wb.wc.rolling(20).mean().shift(1); wb["p"] = wb.wh.rolling(20).max().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.s))).values; ph = g.wk.map(dict(zip(wb.wk, wb.p))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi_td > lo_td, (cl-lo_td)/(hi_td-lo_td), np.nan), weekly_range_pct=np.where(cl > 0, (hi_td-lo_td)/cl*100, np.nan),
        weekly_close_vs_sma20=np.where((sm == sm) & (sm > 0), (cl/sm-1)*100, np.nan), weekly_breakout_20w=np.where(ph == ph, (cl > ph).astype(float), np.nan))))
FC = feat.drop(columns=["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]).merge(pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")
OUT = pd.read_pickle(os.path.join(AP, "_mine_outcomes.pkl"))
alldays = sorted(set(oh.trade_date) | set(OUT.trade_date)); AIDX = {d: i for i, d in enumerate(alldays)}
FC["entry_date"] = FC.trade_date.map(lambda d: alldays[AIDX[d]+1] if AIDX.get(d, 10**9)+1 < len(alldays) else None)
M = FC.merge(OUT[["symbol", "trade_date", TGT]].rename(columns={"trade_date": "entry_date"}), on=["symbol", "entry_date"], how="inner")
M["yr"] = M.trade_date.str[:4].astype(int); train = (M.yr <= 2025).values; oos = (M.yr == 2026).values
y = M[TGT].values.astype(np.float64); b_tr = np.nanmean(y[train])
FEATURES = [c for c in FC.columns if c not in ("symbol", "trade_date", "entry_date", "id") and M[c].dtype.kind in "fi"]
X = M[FEATURES].values.astype(np.float64)
print(f"  panel {len(M)} · train {train.sum()} · oos {oos.sum()} · features {len(FEATURES)} · baseline train {b_tr:+.3f}%")

# --- select patterns on TRAIN ONLY (2026 untouched) ---
def edge(m):
    v = y[m & train]; v = v[~np.isnan(v)]; return (len(v), v.mean()) if len(v) >= MINSUP else (0, np.nan)
singles = []
for j, f in enumerate(FEATURES):
    col = X[:, j]
    for thr in np.unique(np.round(np.nanpercentile(col, [10, 20, 30, 40, 50, 60, 70, 80, 90]), 4)):
        for op in (">", "<"):
            m = OPS[op](col, thr); n, r = edge(m)
            if n and (r-b_tr) >= MIN_EDGE: singles.append(dict(rule=[(f, op, float(thr))], mask=m, tr_edge=r-b_tr))
singles.sort(key=lambda c: -c["tr_edge"]); topS = singles[:25]
pats = list(singles)
for a, bnd in itertools.combinations(topS, 2):
    if a["rule"][0][0] == bnd["rule"][0][0]: continue
    m = a["mask"] & bnd["mask"]; n, r = edge(m)
    if n and (r-b_tr) >= MIN_EDGE: pats.append(dict(rule=a["rule"]+bnd["rule"], mask=m, tr_edge=r-b_tr))
pats.sort(key=lambda c: -c["tr_edge"]); pats = pats[:N_PATTERNS]
print(f"  selected {len(pats)} train-only patterns (top by train edge). Sample:")
for c in pats[:4]: print(f"    train +{c['tr_edge']:.2f}pp  [{' AND '.join(f'{f}{op}{th:.2f}' for f,op,th in c['rule'])}]")

# --- daily basket = consensus of selected patterns, top-BASKET by agreement ---
consensus = np.sum([c["mask"] for c in pats], axis=0)          # how many selected patterns fire per row (vectorized)
M2 = M[["symbol", "trade_date", "entry_date", "yr", TGT]].copy(); M2["cons"] = consensus
def portfolio(mask_years, label):
    d = M2[mask_years & (M2.cons >= CONSENSUS)].dropna(subset=[TGT])
    rows = []
    for ed, g in d.groupby("entry_date"):
        gg = g.sort_values("cons", ascending=False).head(BASKET)
        rows.append((ed, float(gg[TGT].mean()), len(gg)))
    B = pd.DataFrame(rows, columns=["entry_date", "ret", "n"]).sort_values("entry_date").reset_index(drop=True)
    sl = [SLEEVE, SLEEVE]; eq = []
    for i, r in B.iterrows(): sl[i % 2] *= (1+r.ret/100); eq.append((r.entry_date, sl[0]+sl[1]))
    E = pd.DataFrame(eq, columns=["date", "equity"]); E["m"] = E.date.str[:7]
    fin = E.equity.iloc[-1]; dd = ((E.equity.cummax()-E.equity)/E.equity.cummax()*100).max()
    mo = E.groupby("m").equity.last(); mo = pd.concat([pd.Series({E.m.iloc[0][:5]+"00": POOL}), mo]).pct_change().dropna()*100
    print(f"\n  {label}: Rs{POOL:,.0f} -> Rs{fin:,.0f}  = {(fin/POOL-1)*100:+.1f}%  maxDD {dd:.1f}%  win {(B.ret>0).mean()*100:.0f}%  avg {B.ret.mean():+.2f}%  ({len(B)} baskets, {B.n.mean():.1f} names)")
    return E, mo, B

print("\n" + "="*64)
print("CLEAN BTST PORTFOLIO (mined leak-free, 9:45 entry, 2-session)")
print("="*64)
_, mo_tr, _ = portfolio(M2.yr.values <= 2025, "TRAIN 2024-25 (in-sample, context only)")
E, mo, B = portfolio(M2.yr.values == 2026, "2026 OUT-OF-SAMPLE (the honest number)")
print(f"\n  2026 MONTHLY (honest, compounded, net 0.30%):")
run = POOL
for m, rr in mo.items(): run *= (1+rr/100); print(f"    {m:<9}{rr:>+8.1f}%   Rs{run:>12,.0f}")
print(f"    {'TOTAL':<9}{(E.equity.iloc[-1]/POOL-1)*100:>+8.1f}%   Rs{E.equity.iloc[-1]:>12,.0f}")
print(f"\n  annualised ~{((E.equity.iloc[-1]/POOL)**(12/len(mo))-1)*100:+.0f}%/yr  (BTST = CNC 1x, no leverage possible)")
out = os.path.join(ROOT, "docs", "ops", "CLEAN_BTST_PORTFOLIO.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w:
    pd.DataFrame({"month": mo.index, "return_pct": mo.values}).to_excel(w, "Monthly_2026", index=False)
    B.to_excel(w, "Baskets", index=False)
    pd.DataFrame([{"rule": " AND ".join(f"{f}{op}{th:.3f}" for f, op, th in c["rule"]), "train_edge_pp": round(c["tr_edge"], 3)} for c in pats]).to_excel(w, "Patterns", index=False)
print(f"\nExcel -> {out}")
