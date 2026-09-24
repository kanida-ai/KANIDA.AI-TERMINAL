"""COMBINATION ENGINE — union of agents, done meaningfully.
Cluster promoted patterns by (target x regime). Within a cluster: consensus composite = fires when >=K member patterns
agree (K = conviction knob; K=1 union / K>=2 consensus). Across clusters: union (diversified frequency).
Validate each composite OOS (2025-26 + clean 2026) + measure cross-cluster correlation. Compare portfolio vs raw union.
Nothing discarded — every agent feeds its (target,regime) cluster. Read-only; Falcon untouched."""
import os, sqlite3, json
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
REG = os.path.join(ROOT, "arena", "agent_population", "falcon_pattern_registry.db")
CAP = 5e5; COST = 0.15
TARGETS = {"hit_10pc_20d": (10, 20), "hit_15pc_20d": (15, 20), "hit_25pc_30d": (25, 30), "hit_40pc_40d": (40, 40)}
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2018-01-01'", uc)
tax = pd.read_sql_query("SELECT pattern_id,target,regime,rule_json FROM falcon_pattern_taxonomy", uc)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,close FROM ohlc_daily WHERE trade_date>='2018-01-01' ORDER BY symbol,trade_date", uc)
uc.close()
rc = sqlite3.connect("file:" + REG.replace("\\", "/") + "?mode=ro", uri=True)
promoted = set(pid for (pid,) in rc.execute("SELECT pattern_id FROM agents WHERE status='PROMOTED'")); rc.close()
tax = tax[tax.pattern_id.isin(promoted)]
print(f"promoted patterns: {len(tax)} | clusters (target x regime): {tax.groupby(['target','regime']).ngroups}", flush=True)

# outcomes per (symbol,date) for each target
outs = []
for s, g in oh.groupby("symbol", sort=False):
    O = g.open.values; H = g.high.values; C = g.close.values; entry = np.roll(O, -1).astype(float); entry[-1] = np.nan
    d = {"symbol": s, "trade_date": g.trade_date.values}
    for tn, (pct, Hh) in TARGETS.items():
        fmax = pd.Series(H).rolling(Hh).max().shift(-Hh).values; cH = pd.Series(C).shift(-Hh).values
        hit = (fmax >= entry*(1+pct/100)).astype(float); ret = np.where(hit > 0, float(pct), (cH-entry)/entry*100) - COST
        bad = np.isnan(entry) | np.isnan(cH); hit[bad] = np.nan; ret[bad] = np.nan
        d[f"hit_{tn}"] = hit; d[f"ret_{tn}"] = ret
    outs.append(pd.DataFrame(d))
M = feat.merge(pd.concat(outs, ignore_index=True), on=["symbol", "trade_date"], how="inner").reset_index(drop=True)
M["yr"] = M.trade_date.str[:4].astype(int)

def mask(rj):
    m = np.ones(len(M), dtype=bool)
    for f, op, thr in json.loads(rj): m &= OPS[op](M[f].values, thr)
    return m

def acct(idx, retcol, Hh):
    sub = M.loc[idx, ["trade_date", retcol, "yr"]].dropna().sort_values("trade_date")
    eq = CAP; peak = CAP; mdd = 0; free = None; taken = 0
    for dt, r in zip(sub.trade_date.values, sub[retcol].values):
        if free is not None and dt < free: continue
        eq = max(eq*(1+r/100), 1.0); peak = max(peak, eq); mdd = max(mdd, (peak-eq)/peak); taken += 1
        free = (pd.Timestamp(dt)+pd.Timedelta(days=int(Hh*1.5))).strftime("%Y-%m-%d")
    span = max(1, sub.yr.max()-sub.yr.min()+1) if len(sub) else 1
    return (eq/CAP)**(1/span)*100-100 if eq > 0 else np.nan, mdd*100, taken

# base rates per target/window
def base(tn, wmask): return M.loc[wmask, f"hit_{tn}"].mean()*100
clusters = {}; monthly = {}
print(f"\n{'cluster (target/regime)':<34}{'pats':>5}{'K':>3}{'fires25-26':>11}{'hit%':>7}{'lift':>7}{'avgRet':>8}{'WR%':>6}{'CAGR%':>8}{'DD%':>6}{'clean26_lift':>13}")
for (tn, rg), grp in tax.groupby(["target", "regime"]):
    if len(grp) < 2: continue
    count = np.zeros(len(M), dtype=np.int16)
    for rj in grp.rule_json: count += mask(rj).astype(np.int16)
    for K in [1, 2]:
        comp = count >= K
        for wl, wm in [("25", M.yr.values >= 2025)]:
            idx = np.where(comp & wm)[0]
            if len(idx) < 30: continue
            hit = M.iloc[idx][f"hit_{tn}"].mean()*100; ret = M.iloc[idx][f"ret_{tn}"]
            b25 = base(tn, M.yr.values >= 2025); cl = base(tn, M.yr.values == 2026)
            cidx26 = np.where(comp & (M.yr.values == 2026))[0]
            cl26 = (M.iloc[cidx26][f"hit_{tn}"].mean()*100 - cl) if len(cidx26) else np.nan
            cagr, dd, taken = acct(idx, f"ret_{tn}", TARGETS[tn][1])
            print(f"{(tn[:12]+'/'+rg)[:33]:<34}{len(grp):>5}{K:>3}{len(idx):>11}{hit:>7.1f}{hit-b25:>+7.1f}{ret.mean():>+8.2f}{(ret>0).mean()*100:>6.0f}{cagr:>+8.1f}{dd:>6.0f}{cl26:>+13.1f}")
            if K == 2:
                clusters[(tn, rg)] = dict(comp=comp, tn=tn, avg=ret.mean(), wr=(ret > 0).mean()*100, cl26=cl26, fires=len(idx))
                mr = M.loc[comp & (M.yr.values >= 2025), ["trade_date", f"ret_{tn}"]].dropna()
                monthly[(tn, rg)] = mr.assign(ym=mr.trade_date.str[:7]).groupby("ym")[f"ret_{tn}"].mean()

def build_portfolio(keys):
    rows = []
    for k in keys:
        c = clusters[k]; comp = c["comp"]; tn = c["tn"]
        idx = np.where(comp & (M.yr.values >= 2025))[0]
        sub = M.iloc[idx][["symbol", "trade_date", f"ret_{tn}"]].rename(columns={f"ret_{tn}": "ret"}).dropna()
        sub["H"] = TARGETS[tn][1]; rows.append(sub)
    P = pd.concat(rows, ignore_index=True).sort_values("trade_date")
    held = {}; keep = []
    for _, t in P.iterrows():                                          # dedup: one position per stock
        if t.symbol in held and t.trade_date < held[t.symbol]: continue
        keep.append(t); held[t.symbol] = (pd.Timestamp(t.trade_date)+pd.Timedelta(days=int(t.H*1.5))).strftime("%Y-%m-%d")
    PF = pd.DataFrame(keep); PF["ym"] = PF.trade_date.str[:7]
    mret = PF.groupby("ym").ret.mean()                                 # equal-weight monthly portfolio return
    eq = 1.0; peak = 1.0; mdd = 0
    for r in mret: eq *= (1+r/100); peak = max(peak, eq); mdd = max(mdd, (peak-eq)/peak)
    mos = PF.ym.nunique()
    return PF, (eq**(12/mos)-1)*100, mdd*100, (PF.ret > 0).mean()*100, PF.ret.mean(), len(PF)/mos

print("\n" + "="*100 + "\nPORTFOLIO — cross-family union, one position per stock (OOS 2025-26, monthly-compounded)\n" + "="*100)
ALLK = list(clusters.keys())
SEL = [k for k, c in clusters.items() if c["avg"] > 1.5 and c["cl26"] > 5]   # SELECTIVE: strong composites only
for name, keys in [("BLIND union (all 20 families)", ALLK), (f"SELECTIVE union ({len(SEL)} strong families)", SEL)]:
    PF, cagr, mdd, wr, avg, freq = build_portfolio(keys)
    print(f"  {name:<38} trades {len(PF):>5} (~{freq:>3.0f}/mo) | WR {wr:>3.0f}% | avg {avg:+.2f}% | monthly-comp CAGR {cagr:+.0f}% | maxDD {mdd:.0f}%")
mdf = pd.DataFrame({f"{k[0][:7]}/{k[1][:4]}": v for k, v in monthly.items() if k in SEL}).dropna(how="all")
if len(mdf.columns) > 1:
    corr = mdf.corr().values; off = corr[np.triu_indices(len(corr), 1)]; off = off[~np.isnan(off)]
    print(f"\n  cross-family monthly-return corr (selective set): avg {np.mean(off):+.2f} | median {np.median(off):+.2f}  (low = real diversification)")
print("\n  SELECTED strong families (avg>1.5%, clean-2026 lift>5pp):")
for k in sorted(SEL, key=lambda x: -clusters[x]["avg"]):
    c = clusters[k]; print(f"    {c['tn'][:12]}/{k[1]:<14} avg {c['avg']:+.2f}% | WR {c['wr']:.0f}% | clean26 lift {c['cl26']:+.1f}pp | {c['fires']} fires")
