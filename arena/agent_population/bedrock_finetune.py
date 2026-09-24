"""BEDROCK — FINE-TUNE to two risk profiles, honestly (4-stage discipline).
Search space = persona/hold (Weekly-5d, Swing-15d via the daily grid; Intraday via the 1-min grid) x universe x
crowding cap x entry timing x target x stop x LEVERAGE {1,2,3,5}. For speed & honesty, each config is simulated ONCE at
1x to get its daily equity; leverage L is then applied as constant-leverage rebalancing (eq_t = eq_{t-1}*(1+L*r_t)).
SELECT on 2025-H1 (in-sample), FREEZE, DEPLOY true-OOS on 2026:
   PROFILE A (Controlled) : max in-sample return SUBJECT TO in-sample maxDD < 20%
   PROFILE B (Aggressive) : max in-sample return, no risk limit
Report OOS 2026 return + realised maxDD (did the 20% cap hold OOS?) + monthly + worst day/month. Rs5L, net 0.15%."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; AP = os.path.dirname(os.path.abspath(__file__)); CAP0 = 500000.0
SWEEP_LO, SWEEP_HI = "2025-01-01", "2025-06-30"; MINTR = 30; DD_CAP = 20.0; LEVS = [1, 2, 3, 5]
uc = sqlite3.connect("file:" + os.path.join(ROOT, "data", "db", "kanida_universe.db").replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,close FROM ohlc_daily WHERE trade_date>='2025-01-01' AND trade_date<='2026-08-15'", uc); uc.close()
CLOSE = {(r.symbol, r.trade_date): r.close for r in oh.itertuples()}; alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}
GS = pd.read_pickle(os.path.join(AP, "_bedrock_grid_full.pkl")); GS["uni_all"] = 1
GI = pd.read_pickle(os.path.join(AP, "_bedrock_intraday_grid.pkl")); GI["uni_all"] = 1
UCOL = {"ALL": "uni_all", "FO": "uni_fo", "N50": "uni_n50"}

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
    r = eq1x.pct_change().fillna(eq1x.iloc[0]/CAP0 - 1)
    eqL = CAP0 * (1 + L*r).cumprod()
    dd = ((eqL.cummax()-eqL)/eqL.cummax()*100).max()
    return (eqL.iloc[-1]/CAP0-1)*100, dd, eqL

# ---- STAGE 2 sweep on 2025-H1 (1x), then apply leverage ----
CACHE = os.path.join(AP, "_finetune_sweep.pkl")
if os.path.exists(CACHE):
    SW = pd.read_pickle(CACHE); print("(loaded cached sweep)")
else:
    print("sweeping configs on 2025-H1 ...", flush=True)
    rows = []
    UNIS = ["ALL", "FO", "N50"]; CAPS = [3, 5, 10, 20, 999]
    # swing/weekly personas
    for tm, pers in [(5, "Weekly"), (15, "Swing")]:
        for em in ["OPEN", "WAIT30", "DIP", "BRK"]:
            for tg in [None, 10.0, 15.0]:
                for sp in [None, 6.0, 10.0]:
                    for uni in UNIS:
                        for cap in CAPS:
                            r = swing_1x(SWEEP_LO, SWEEP_HI, em, tg, sp, tm, cap, uni)
                            if r is None: continue
                            eq, nt, win = r
                            for L in LEVS:
                                ret, dd, _ = lever(eq, L)
                                rows.append(dict(persona=pers, grid="swing", em=em, tg=tg, sp=sp, tm=tm, uni=uni, cap=cap, lev=L, ret=ret, dd=dd, nt=nt, win=win))
    # intraday persona
    for em in ["OPEN", "SPLIT", "WAIT30", "DIP", "BRK"]:
        for tg in [None, 1.0, 2.0, 3.0]:
            for sp in [None, 1.0, 2.0]:
                for uni in UNIS:
                    for cap in CAPS:
                        r = intraday_1x(SWEEP_LO, SWEEP_HI, em, tg, sp, cap, uni)
                        if r is None: continue
                        eq, nt, win = r
                        for L in LEVS:
                            ret, dd, _ = lever(eq, L)
                            rows.append(dict(persona="Intraday", grid="intraday", em=em, tg=tg, sp=sp, tm=None, uni=uni, cap=cap, lev=L, ret=ret, dd=dd, nt=nt, win=win))
    SW = pd.DataFrame(rows); SW.to_pickle(CACHE)
print(f"  swept {len(SW)} (config x leverage) combos in-sample\n")

def deploy(row, lo, hi):
    _v = lambda x: None if pd.isna(x) else x          # pandas turned None -> NaN; restore for the key
    tg, sp = _v(row["tg"]), _v(row["sp"])
    if row["grid"] == "swing":
        r = swing_1x(lo, hi, row["em"], tg, sp, int(row["tm"]), int(row["cap"]), row["uni"])
    else:
        r = intraday_1x(lo, hi, row["em"], tg, sp, int(row["cap"]), row["uni"])
    if r is None: return None
    eq, nt, win = r; ret, dd, eqL = lever(eq, int(row["lev"]))
    return ret, dd, nt, win, eqL

def describe(row):
    ent = row["em"]; hold = "same-day" if row["grid"] == "intraday" else f"D{int(row['tm'])}"
    return (f"{row['persona']} · {row['uni']} · top-{'all' if row['cap']==999 else int(row['cap'])}/day · {ent} entry · "
            f"target {row['tg'] if pd.notna(row['tg']) else 'close/none'} · stop {row['sp'] if pd.notna(row['sp']) else 'none'} · "
            f"exit {hold} · {int(row['lev'])}x")

A = SW[(SW.dd < DD_CAP) & (SW.nt >= MINTR)].sort_values("ret", ascending=False).iloc[0]
B = SW[SW.nt >= MINTR].sort_values("ret", ascending=False).iloc[0]
print("="*90)
for tag, sel in [("PROFILE A — CONTROLLED (in-sample maxDD < 20%, max return)", A),
                 ("PROFILE B — AGGRESSIVE (max return, any risk)", B)]:
    print(f"\n{tag}")
    print(f"  FROZEN: {describe(sel)}")
    print(f"  in-sample 2025-H1: {sel.ret:+.1f}% · maxDD {sel.dd:.1f}% · {int(sel.nt)} trades · win {sel.win:.0f}%")
    for lab, lo, hi in [("A) 2025-H1 [IN/IN] not evid", SWEEP_LO, SWEEP_HI),
                        ("B) 2025-H2 [OUT/IN] partial", "2025-07-01", "2025-12-31"),
                        ("C) 2026    [OUT/OUT] REAL ", "2026-01-01", "2026-07-10")]:
        d = deploy(sel, lo, hi)
        if d: print(f"    {lab}: return {d[0]:>+8.1f}%   maxDD {d[1]:>5.1f}%   trades {d[2]:>4}   win {d[3]:>3.0f}%"
                     + ("   <-- 20% cap " + ("HELD" if d[1] < 20 else "BREACHED") if 'A —' in tag else ""))
    dC = deploy(sel, "2026-01-01", "2026-07-10")
    if dC:
        eqL = dC[4]; E = pd.DataFrame({"equity": eqL.values})
        # monthly needs dates: rebuild with dates
        if sel["grid"] == "swing":
            days = [x for x in alldays if "2026-01-01" <= x <= "2026-07-10"]
        else:
            sub = GI[(GI.entry_date >= "2026-01-01") & (GI.entry_date <= "2026-07-10")]; days = sorted(sub.entry_date.unique())
        E["date"] = days[:len(E)]; E["m"] = E.date.str[:7]
        mo = E.groupby("m").equity.last(); mo = pd.concat([pd.Series({"2026-00": CAP0}), mo]).pct_change().dropna()*100
        print(f"    2026 monthly: " + " ".join(f"{m[-2:]}:{v:+.0f}%" for m, v in mo.items())
              + f"  | worst month {mo.min():+.1f}% · worst day "
              + f"{(pd.Series(eqL.values).pct_change().min()*100):+.2f}%")
print("\n" + "="*90)
print(f"Cap held OOS? Profile A must keep 2026 maxDD < 20% to be trustworthy (in-sample fit can still break OOS).")
out = os.path.join(ROOT, "docs", "ops", "BEDROCK_FINETUNE.xlsx")
with pd.ExcelWriter(out, engine="openpyxl") as w:
    SW.sort_values("ret", ascending=False).to_excel(w, "All configs x leverage", index=False)
    SW[(SW.dd < DD_CAP) & (SW.nt >= MINTR)].sort_values("ret", ascending=False).head(30).to_excel(w, "Profile A candidates", index=False)
print(f"Excel -> {out}")
