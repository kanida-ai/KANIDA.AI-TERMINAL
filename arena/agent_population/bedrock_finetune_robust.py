"""WHY did 'max return under 20% DD' pick a WORSE config than Swing-1x? Because it maximised IN-SAMPLE return —
the classic overfitting trap. FIX: nested selection. Train on 2025-H1, VALIDATE on 2025-H2, and pick the config that
is best on its WORST half (max of min(H1,H2)) while BOTH halves keep DD<20%. 2026 stays the untouched true test.
Also prints the specific Swing top-3/WAIT30/D15/1x config so we can see it directly."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; AP = os.path.dirname(os.path.abspath(__file__)); CAP0 = 500000.0
uc = sqlite3.connect("file:" + os.path.join(ROOT, "data", "db", "kanida_universe.db").replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,close FROM ohlc_daily WHERE trade_date>='2025-01-01' AND trade_date<='2026-08-15'", uc); uc.close()
CLOSE = {(r.symbol, r.trade_date): r.close for r in oh.itertuples()}; alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}
GS = pd.read_pickle(os.path.join(AP, "_bedrock_grid_full.pkl")); GS["uni_all"] = 1
GI = pd.read_pickle(os.path.join(AP, "_bedrock_intraday_grid.pkl")); GI["uni_all"] = 1
UCOL = {"ALL": "uni_all", "FO": "uni_fo", "N50": "uni_n50"}; MINTR = 30

def swing_1x(lo, hi, em, tg, sp, tm, cap, uni):
    k = f"{em}|{tg}|{sp}|{tm}"
    if k not in GS.columns: return None
    sub = GS[(GS.entry_date >= lo) & (GS.entry_date <= hi) & (GS[UCOL[uni]] == 1) & (GS["rank"] <= cap)]; sub = sub[np.isfinite(sub[k].values)]
    if len(sub) < MINTR: return None
    byday = {d: g for d, g in sub.groupby("entry_date")}; cash = CAP0; openp = []; eqs = []; nt = 0; wins = 0; slots = (cap if cap != 999 else 20) * tm
    for d in [x for x in alldays if lo <= x <= hi]:
        eq = cash + sum(p["tk"] * (CLOSE.get((p["s"], d), p["px"]) / p["px"]) for p in openp)
        if d in byday:
            tk = max(eq / slots, 0); i0 = AIDX[d]
            for r in byday[d].itertuples():
                ep = r.ep_map.get(em)
                if ep is None: continue
                openp.append(dict(s=r.symbol, px=ep, tk=tk, ret=GS.loc[r.Index, k], xd=alldays[min(i0+tm-1, len(alldays)-1)])); cash -= tk
        keep = []
        for p in openp:
            if p["xd"] <= d: cash += p["tk"]+p["tk"]*p["ret"]/100.0; nt += 1; wins += p["ret"] > 0
            else: keep.append(p)
        openp = keep
        eqs.append(cash + sum(p["tk"]*(CLOSE.get((p["s"], d), p["px"])/p["px"]) for p in openp))
    for p in openp: nt += 1; wins += p["ret"] > 0
    return pd.Series(eqs), nt, (wins/nt*100 if nt else 0)

def intraday_1x(lo, hi, em, tg, sp, cap, uni):
    k = f"{em}|{tg}|{sp}"
    if k not in GI.columns: return None
    sub = GI[(GI.entry_date >= lo) & (GI.entry_date <= hi) & (GI[UCOL[uni]] == 1) & (GI["rank"] <= cap)]; sub = sub[np.isfinite(sub[k].values)]
    if len(sub) < MINTR: return None
    byday = {d: g for d, g in sub.groupby("entry_date")}; eq = CAP0; eqs = []; nt = 0; wins = 0
    for d in [x for x in sorted(byday) if lo <= x <= hi]:
        r = byday[d].nsmallest(min(len(byday[d]), cap), "rank")[k].values
        eq *= (1 + np.mean(r)/100.0); eqs.append(eq); nt += len(r); wins += (r > 0).sum()
    return pd.Series(eqs), nt, (wins/nt*100 if nt else 0)

def lever(eq1x, L):
    r = eq1x.pct_change().fillna(eq1x.iloc[0]/CAP0 - 1); eqL = CAP0*(1+L*r).cumprod()
    return (eqL.iloc[-1]/CAP0-1)*100, ((eqL.cummax()-eqL)/eqL.cummax()*100).max()

_v = lambda x: None if (x is None or (isinstance(x, float) and pd.isna(x))) else x
def run(row, lo, hi):
    tg, sp = _v(row["tg"]), _v(row["sp"])
    r = swing_1x(lo, hi, row["em"], tg, sp, int(row["tm"]), int(row["cap"]), row["uni"]) if row["grid"] == "swing" \
        else intraday_1x(lo, hi, row["em"], tg, sp, int(row["cap"]), row["uni"])
    if r is None: return None
    return lever(r[0], int(row["lev"]))
def desc(row):
    return (f"{row['persona']} · {row['uni']} · top-{'all' if row['cap']==999 else int(row['cap'])} · {row['em']} · "
            f"tgt {row['tg'] if pd.notna(row['tg']) else '-'} · stop {row['sp'] if pd.notna(row['sp']) else '-'} · "
            f"{'same-day' if row['grid']=='intraday' else 'D'+str(int(row['tm']))} · {int(row['lev'])}x")

SW = pd.read_pickle(os.path.join(AP, "_finetune_sweep.pkl"))
H1, H2, Y26 = ("2025-01-01", "2025-06-30"), ("2025-07-01", "2025-12-31"), ("2026-01-01", "2026-07-10")

# 1) show the OLD pick (max in-sample return under cap) vs the SWING-1x robust config
old = SW[(SW.dd < 20) & (SW.nt >= MINTR)].sort_values("ret", ascending=False).iloc[0]
swing1x = SW[(SW.grid == "swing") & (SW.em == "WAIT30") & (SW.tg.isna()) & (SW.sp.isna()) & (SW.tm == 15) & (SW.cap == 3) & (SW.uni == "ALL") & (SW.lev == 1)]
print("WHY THE OPTIMISER MISSED THE BETTER CONFIG\n" + "="*70)
for lab, row in [("OLD PICK (max in-sample return)", old), ("SWING top-3 WAIT30 D15 1x", swing1x.iloc[0] if len(swing1x) else None)]:
    if row is None: continue
    h1, h2, y26 = run(row, *H1), run(row, *H2), run(row, *Y26)
    print(f"\n{lab}\n  {desc(row)}")
    print(f"   in-sample H1 : {h1[0]:>+7.1f}%  DD {h1[1]:>4.1f}%")
    print(f"   validate  H2 : {h2[0]:>+7.1f}%  DD {h2[1]:>4.1f}%   <- the OLD pick chased H1 and ignored this")
    print(f"   TRUE OOS 2026: {y26[0]:>+7.1f}%  DD {y26[1]:>4.1f}%")

# 2) ROBUST re-selection: train H1, validate H2, keep DD<20 in BOTH, maximise the WORSE half
print("\n\nROBUST RE-SELECTION (train H1 + validate H2, both DD<20%, maximise worst half)\n" + "="*70)
cands = SW[(SW.dd < 20) & (SW.nt >= MINTR)].sort_values("ret", ascending=False).head(200).copy()
rec = []
for _, row in cands.iterrows():
    h2 = run(row, *H2)
    if h2 is None: continue
    h1ret, h1dd = row["ret"], row["dd"]
    if h2[1] >= 20 or h1dd >= 20: continue          # DD cap must hold in BOTH halves
    rec.append((row, min(h1ret, h2[0]), h1ret, h1dd, h2[0], h2[1]))
rec.sort(key=lambda x: x[1], reverse=True)
print(f"  {len(rec)} configs keep DD<20% in BOTH 2025 halves. Best by worst-half return:\n")
print(f"  {'worst-half':>11}{'H1 ret':>9}{'H2 ret':>9}   config")
for row, worst, h1r, h1d, h2r, h2d in rec[:6]:
    print(f"  {worst:>+10.1f}%{h1r:>+8.1f}%{h2r:>+8.1f}%   {desc(row)}")
best = rec[0][0]
y = run(best, *Y26)
print(f"\n  >>> ROBUST PROFILE A: {desc(best)}")
print(f"      TRUE OOS 2026: {y[0]:+.1f}%   maxDD {y[1]:.1f}%   (20% cap {'HELD' if y[1]<20 else 'BREACHED'})")
