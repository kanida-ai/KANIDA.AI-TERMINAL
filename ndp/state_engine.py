# -*- coding: utf-8 -*-
"""STATE SEPARATION ENGINE. Transition: price hits +0.5% from open (mirror: -0.5%). Two futures:
CONTINUATION (reaches +1.0%) vs REVERSION (doesn't). Freeze ALL info at the +0.5% instant (leak-free).
Primitives -> Feature Engine (transforms/interactions) -> State Engine (which features separate the two
futures) -> Probability Engine (walk-forward P(continuation)) -> decision. Measures the OOS separation
(AUC) and the odds-lift of the high-confidence subset vs the base rate. Promote features by OOS evidence."""
import os, sqlite3, warnings
import numpy as np, pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
warnings.filterwarnings("ignore")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
DDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")


def _rsi(x, n=14):
    d = np.diff(x, prepend=x[0]); u = pd.Series(np.clip(d, 0, None)).ewm(alpha=1/n, adjust=False).mean().values
    dn = pd.Series(-np.clip(d, None, 0)).ewm(alpha=1/n, adjust=False).mean().values
    return 100-100/(1+u/(dn+1e-9))


def dctx(sym):
    con = sqlite3.connect("file:" + DDB.replace("\\", "/") + "?mode=ro", uri=True)
    d = pd.read_sql_query("SELECT trade_date,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? ORDER BY trade_date", con, params=[sym]); con.close()
    d["pc"] = d.close.shift(1); d["ph"] = d.high.shift(1); d["pl"] = d.low.shift(1)
    d["prev_ret"] = (d.close.shift(1)/d.close.shift(2)-1)*100
    d["prsi"] = pd.Series(_rsi(d.close.values)).shift(1).values
    d["sma50"] = d.close.rolling(50).mean().shift(1); d["sma200"] = d.close.rolling(200).mean().shift(1)
    d["prange"] = ((d.high-d.low)/d.open*100).shift(1)
    d["pclose_pos"] = ((d.close-d.low)/(d.high-d.low+1e-9)).shift(1)
    d["pvol"] = d.volume.shift(1)
    tr = np.maximum(d.high-d.low, np.maximum((d.high-d.pc).abs(), (d.low-d.pc).abs()))
    d["atrp"] = (pd.Series(tr).ewm(alpha=1/14, adjust=False).mean()/d.close*100).shift(1)
    return d.set_index("trade_date")


def build(sym, side):
    dc = dctx(sym)
    con = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
    b = pd.read_sql_query("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min WHERE symbol=? ORDER BY bar_time", con, params=[sym]); con.close()
    b["date"] = b.bar_time.str[:10]; b["hm"] = b.bar_time.str[11:16]; b = b[(b.hm >= "09:15") & (b.hm <= "15:29")]
    rows = []
    for dt, g in b.groupby("date"):
        if dt not in dc.index: continue
        g = g.sort_values("bar_time").reset_index(drop=True); o = g.open.iloc[0]
        H, L, C, V = g.high.values, g.low.values, g.close.values, g.volume.values
        lvl = o*(1+side*0.5/100); tgt = o*(1+side*1.0/100)
        hit = np.where(H >= lvl)[0] if side > 0 else np.where(L <= lvl)[0]
        if len(hit) == 0: continue
        t = hit[0]
        if t < 3 or t > len(g)-8: continue
        c = dc.loc[dt]
        seg = slice(0, t+1); dret = np.diff(C[:t+1], prepend=C[0])
        tp = (H[seg]+L[seg]+C[seg])/3; vwap = (tp*V[seg]).sum()/max(V[seg].sum(), 1)
        speed = 0.5/(t+1)
        straight = abs(C[t]-o)/(np.abs(dret).sum()+1e-9)
        maxdip = (o-L[:t+1].min())/o*100 if side > 0 else (H[:t+1].max()-o)/o*100
        rvol = np.std(dret/C[:t+1]*100) if t > 3 else 0.1
        volpace = V[:t+1].mean()/(c.pvol/375+1e-9) if c.pvol == c.pvol else 1
        vwapd = (C[t]/vwap-1)*100*side
        gap = (o/c.pc-1)*100 if c.pc == c.pc else 0
        pra = c.prange if c.prange == c.prange else 3.0
        f = dict(symbol=sym, date=dt,
                 mins=t, speed=speed, straight=straight, maxdip=maxdip, rvol=rvol, volpace=volpace,
                 vwapd=vwapd, gap=gap*side, gapmag=abs(gap), prev_ret=c.prev_ret*side, prsi=c.prsi,
                 trend=(int(c.sma50 > c.sma200) if c.sma200 == c.sma200 else 0)*side + (0 if side > 0 else 1),
                 prange=pra, pclose_pos=c.pclose_pos, atrp=c.atrp, range_used=0.5/(pra+1e-9),
                 pdh_break=int(o > c.ph)*1.0 if side > 0 else int(o < c.pl)*1.0,
                 # engineered interactions
                 speed_vol=speed*volpace, straight_speed=straight*speed, vwapd_vol=vwapd*volpace,
                 gap_mom=gap*side*(c.prev_ret if c.prev_ret == c.prev_ret else 0), move_vs_noise=0.5/(rvol+1e-9),
                 mom_align=int(np.sign(c.prev_ret if c.prev_ret == c.prev_ret else 0) == side))
        # label: continuation = reached +/-1.0% after t
        aft_ext = (H[t:].max() if side > 0 else L[t:].min())
        f["y"] = int(aft_ext >= tgt) if side > 0 else int(aft_ext <= tgt)
        rows.append(f)
    return pd.DataFrame(rows)


def run_side(sym, side, label):
    P = build(sym, side).dropna()
    feats = [c for c in P.columns if c not in ("symbol", "date", "y")]
    P = P.sort_values("date").reset_index(drop=True)
    base = P.y.mean()*100
    # walk-forward: expanding, retrain each ~monthly block, pool OOS predictions
    P["mo"] = P.date.str[:7]; months = sorted(P.mo.unique())
    oos_p = []; oos_y = []
    for i in range(6, len(months)):
        tr = P[P.mo < months[i]]; te = P[P.mo == months[i]]
        if len(tr) < 80 or te.empty: continue
        mu = tr[feats].mean(); sd = tr[feats].std().replace(0, 1)
        clf = LogisticRegression(max_iter=300, C=0.3).fit(((tr[feats]-mu)/sd).values, tr.y.values)
        p = clf.predict_proba(((te[feats]-mu)/sd).values)[:, 1]
        oos_p += list(p); oos_y += list(te.y.values)
    oos_p = np.array(oos_p); oos_y = np.array(oos_y)
    auc = roc_auc_score(oos_y, oos_p) if len(set(oos_y)) > 1 else np.nan
    # tercile separation
    q1, q2 = np.quantile(oos_p, [1/3, 2/3])
    hi = oos_y[oos_p >= q2]; lo = oos_y[oos_p <= q1]
    # per-feature univariate OOS AUC (which single states separate)
    fa = []
    for f in feats:
        try:
            a = roc_auc_score(P.y.values, P[f].values)
            fa.append((f, max(a, 1-a), "+" if a >= 0.5 else "-"))
        except Exception: pass
    fa.sort(key=lambda x: -x[1])
    print(f"\n{'='*80}\n{label}  ({sym})  ·  transition +{('0.5% long' if side>0 else '')}{('0.5% short' if side<0 else '')} -> continuation(reach {'+' if side>0 else '-'}1.0%) vs reversion\n{'='*80}")
    print(f"  touches: {len(P)}   ·   BASE continuation rate: {base:.0f}%   ·   OOS pooled n={len(oos_y)}")
    print(f"  --- SEPARATION (walk-forward OOS) ---")
    print(f"  AUC = {auc:.3f}   (0.50 = no separation; >0.55 = real)")
    print(f"  HIGH-confidence third: continuation {hi.mean()*100:.0f}%  (n={len(hi)})")
    print(f"  LOW-confidence third : continuation {lo.mean()*100:.0f}%  (n={len(lo)})")
    print(f"  SEPARATION SPREAD (high - low): {(hi.mean()-lo.mean())*100:+.0f} pts   ·   lift over base: {(hi.mean()*100-base):+.0f} pts")
    print(f"  --- top separating states (univariate |AUC|) ---")
    for f, a, s in fa[:8]: print(f"    {f:<16} AUC {a:.3f} ({s})")
    return dict(auc=auc, base=base, hi=hi.mean()*100 if len(hi) else np.nan, lo=lo.mean()*100 if len(lo) else np.nan)


def main(sym="ADANIENT"):
    print(f"STATE SEPARATION ENGINE — {sym}  (1-min 2024-05+, leak-free walk-forward)")
    run_side(sym, +1, "LONG: reached +0.5%")
    run_side(sym, -1, "SHORT: reached -0.5%")


if __name__ == "__main__":
    import sys; main(sys.argv[1].upper() if len(sys.argv) > 1 else "ADANIENT")
