"""Operator-Winner Screen v5 — add THEME/CO-MOVEMENT AFFINITY lane.
Hypothesis: many 'fresh' winners (never traded before) are same-THEME as the operator's recent picks
(railway day, Adani day, retail day). A stock that co-moves with the last ~10 sessions' picks is in that
theme even if its own symbol has no memory. affinity(sym,S) = mean 20d-return correlation of sym with the
operator's picks in sessions [S-10, S-1]. Leak-free. Tests whether affinity lifts FRESH-name recall.
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
LOG = os.path.join(ROOT, "arena", "agent_population", "operator_ranked_log_dec24_jan25.tsv")
WIN_THR = 0.3; MEM_LOOK, MEM_DECAY = 20, 0.90; AFF_LOOK = 10; RWIN = 20

picks = pd.read_csv(LOG, sep="\t", dtype={"trade_date": str})
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily "
                       "WHERE trade_date>='2024-08-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date", con)
con.close()
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
prev = lambda d: cal[cidx[d] - 1] if (d in cidx and cidx[d] - 1 >= 0) else None
nxt = lambda d: cal[cidx[d] + 1] if (d in cidx and cidx[d] + 1 < len(cal)) else None

# per-symbol return series aligned to calendar index; RET_OC for basket
RET = {}; RET_OC = {}
for s, g in oh.groupby("symbol", sort=False):
    g = g.sort_values("trade_date"); c = g.close.values.astype(float); op = g.open.values.astype(float)
    idx = [cidx[d] for d in g.trade_date.values]
    r = np.full(len(cal), np.nan)
    ret = np.concatenate([[np.nan], np.diff(c) / c[:-1]])
    for k, ii in enumerate(idx):
        r[ii] = ret[k]; RET_OC[(s, cal[ii])] = ((c[k] - op[k]) / op[k] * 100) if op[k] > 0 else np.nan
    RET[s] = r
allsyms = list(RET.keys())
RMAT = np.vstack([RET[s] for s in allsyms])            # symbols x calendar
symrow = {s: i for i, s in enumerate(allsyms)}

# labels + memory
pick_sd = {}
for _, p in picks.iterrows():
    s = prev(p.trade_date)
    if s: pick_sd.setdefault(s, []).append(p.symbol)
win_key, lose_key = set(), set()
for _, p in picks.iterrows():
    s = prev(p.trade_date)
    if s: (win_key if p.stock_ret_pct > WIN_THR else lose_key).add((s, p.symbol))
sig_days = sorted({prev(t) for t in picks.trade_date.unique() if prev(t)})
sym_pickidx = {}
for sd, syms in pick_sd.items():
    for sym in syms: sym_pickidx.setdefault(sym, []).append(cidx[sd])
for k in sym_pickidx: sym_pickidx[k] = sorted(sym_pickidx[k])
def memory(sym, i): return sum(MEM_DECAY ** (i - j) for j in sym_pickidx.get(sym, []) if 0 < i - j <= MEM_LOOK)
def ever_before(sym, i): return any(j < i for j in sym_pickidx.get(sym, []))

def zrows(M):
    mu = np.nanmean(M, axis=1, keepdims=True); sd = np.nanstd(M, axis=1, keepdims=True)
    Z = (M - mu) / np.where(sd > 0, sd, np.nan); return np.nan_to_num(Z)

rows = []
for S in sig_days:
    i = cidx[S]
    if i - RWIN < 0: continue
    win = RMAT[:, i - RWIN + 1:i + 1]                  # symbols x RWIN (up to & incl S) leak-free
    Zw = zrows(win)                                    # standardized returns
    # recent picks (sessions [i-AFF_LOOK, i-1]) that have valid windows
    recent = set()
    for sd in sig_days:
        j = cidx[sd]
        if i - AFF_LOOK <= j < i: recent |= set(pick_sd.get(sd, []))
    pr = [symrow[p] for p in recent if p in symrow]
    pr = [k for k in pr if not np.all(win[k] != win[k])]
    if pr:
        Pz = Zw[pr]                                    # P x RWIN
        aff = (Zw @ Pz.T) / RWIN                        # U x P correlation-ish
        affinity = np.nanmean(aff, axis=1)
    else:
        affinity = np.zeros(len(allsyms))
    T = nxt(S)
    for k, sym in enumerate(allsyms):
        if np.isnan(RMAT[k, i]): continue              # symbol not trading on S
        lab = 1 if (S, sym) in win_key else (-1 if (S, sym) in lose_key else 0)
        rows.append(dict(sd=S, symbol=sym, label=lab, mem=memory(sym, i), aff=affinity[k],
                         recur=ever_before(sym, i), ret_oc=RET_OC.get((sym, T), np.nan)))
P = pd.DataFrame(rows); P["mo"] = P.sd.str[:7]
w = P[P.label == 1]
print(f"panel {len(P):,} · WIN {len(w)} · recurring {int(w.recur.sum())}/{len(w)}")

# univariate: do winners have higher affinity than neutrals? split by recur/fresh
for grp, mask in [("ALL win", P.label == 1), ("FRESH win", (P.label == 1) & (~P.recur)),
                  ("RECUR win", (P.label == 1) & (P.recur)), ("neutral", P.label == 0)]:
    print(f"  {grp:<10} affinity mean {P.loc[mask,'aff'].mean():+.3f}  mem mean {P.loc[mask,'mem'].mean():.3f}")

def z(s):
    s = pd.Series(s).astype(float); mu, sd = s.mean(), s.std()
    return ((s - mu) / sd).fillna(0).values if sd and sd == sd else np.zeros(len(s))
def eval_screen(test, col):
    hit = hitR = hitF = tot = totR = totF = 0; bask = []
    for s, g in test.groupby("sd"):
        g = g.sort_values(col, ascending=False); bask.append(np.nanmean(g.head(15).ret_oc.values))
        ranks = {sym: r + 1 for r, sym in enumerate(g.symbol)}
        for _, rr in g[g.label == 1].iterrows():
            r = ranks[rr.symbol]; tot += 1; totR += rr.recur; totF += (not rr.recur)
            if r <= 15: hit += 1; hitR += rr.recur; hitF += (not rr.recur)
    R = lambda a, b: (a / b * 100 if b else 0)
    return dict(r15=R(hit, tot), rr=R(hitR, totR), rf=R(hitF, totF), totR=totR, totF=totF, bask=float(np.nanmean(bask)) * 5)

print("\n================= v5 THEME-AFFINITY =================")
for tag, mask in [("test=all", P.index.notna()), ("test=Jan", P.mo == "2025-01"), ("test=Dec", P.mo == "2024-12")]:
    t = P[mask].copy()
    lanes = {"affinity-only": z(t.aff), "memory-only": z(t.mem),
             "mem+aff": z(t.mem) + z(t.aff), "mem+aff (aff-weighted)": z(t.mem) + 1.5 * z(t.aff)}
    print(f"\n[{tag}]")
    for name, sc in lanes.items():
        t["_s"] = sc; e = eval_screen(t, "_s")
        print(f"    {name:<24} recall@15 {e['r15']:4.0f}% (recur {e['rr']:3.0f}%/{e['totR']} | fresh {e['rf']:3.0f}%/{e['totF']}) top15 5x {e['bask']:+.2f}%/day")
