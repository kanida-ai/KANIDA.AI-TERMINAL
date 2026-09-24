# -*- coding: utf-8 -*-
"""At the moment ICICI reaches +0.5% from open, which indicators separate CONTINUERS (run to +0.7/1.0/1.5%)
from REVERSERS (fall back to open)? Compute state AT the touch minute (leak-free, only bars up to t), label
the outcome AFTER t, and rank each feature by how much it lifts continuation top-tercile vs bottom-tercile.
Validated TRAIN->TEST so we don't overfit. 1-min 2024-05+."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
DDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")


def _rsi(x, n=14):
    d = np.diff(x, prepend=x[0]); up = pd.Series(np.clip(d, 0, None)).ewm(alpha=1/n, adjust=False).mean().values
    dn = pd.Series(-np.clip(d, None, 0)).ewm(alpha=1/n, adjust=False).mean().values
    return 100 - 100/(1 + up/(dn+1e-9))


def build(sym, side=+1, X=0.5):
    con = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
    b = pd.read_sql_query("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min WHERE symbol=? ORDER BY bar_time", con, params=[sym]); con.close()
    b["date"] = b.bar_time.str[:10]; b["hm"] = b.bar_time.str[11:16]; b = b[(b.hm >= "09:15") & (b.hm <= "15:29")]
    con = sqlite3.connect("file:" + DDB.replace("\\", "/") + "?mode=ro", uri=True)
    d = pd.read_sql_query("SELECT trade_date,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? ORDER BY trade_date", con, params=[sym]); con.close()
    d["pc"] = d.close.shift(1); d["ph"] = d.high.shift(1); d["pret"] = d.close.pct_change()*100
    d["sma50"] = d.close.rolling(50).mean(); d["sma200"] = d.close.rolling(200).mean()
    d["prsi"] = _rsi(d.close.values); d["pv"] = d.volume.shift(1)
    dmap = d.set_index("trade_date")
    rows = []
    for dt, g in b.groupby("date"):
        if dt not in dmap.index: continue
        g = g.sort_values("bar_time").reset_index(drop=True); o = g.open.iloc[0]
        H = g.high.values; L = g.low.values; C = g.close.values; V = g.volume.values
        lvl = o*(1+side*X/100)
        hit = np.where(H >= lvl)[0] if side > 0 else np.where(L <= lvl)[0]
        if len(hit) == 0: continue
        t = hit[0]
        if t < 3 or t > len(g)-10: continue                      # need some history + room after
        dd = dmap.loc[dt]
        # ---- features AT the touch (only bars 0..t) ----
        seg = slice(0, t+1)
        tp = (H[seg]+L[seg]+C[seg])/3; vwap = (tp*V[seg]).sum()/max(V[seg].sum(), 1)
        rets = np.diff(C[:t+1], prepend=C[0])
        straight = abs(C[t]-o)/(np.abs(rets).sum()+1e-9)
        maxdip = (o - L[:t+1].min())/o*100 if side > 0 else (H[:t+1].max()-o)/o*100
        idrsi = _rsi(C[:t+1])[-1] if t >= 5 else 50
        volpace = (V[:t+1].mean()) / (dd.pv/375 + 1e-9) if dd.pv == dd.pv else np.nan
        feat = dict(
            mins_to_touch=t,
            straightness=round(straight, 3),
            maxdip_before=round(maxdip, 3),
            gap_pct=round((o/dd.pc-1)*100, 3) if dd.pc == dd.pc else np.nan,
            prev_ret=round(dd.pret, 3) if dd.pret == dd.pret else np.nan,
            trend_up=int(dd.sma50 > dd.sma200) if dd.sma200 == dd.sma200 else 0,
            prev_rsi=round(dd.prsi, 1) if dd.prsi == dd.prsi else np.nan,
            vwap_dist=round((C[t]/vwap-1)*100*side, 3),
            intraday_rsi=round(idrsi, 1),
            vol_pace=round(volpace, 2) if volpace == volpace else np.nan,
            above_pdh=int(o > dd.ph) if dd.ph == dd.ph else 0,
        )
        # ---- labels: after t, reach next milestones (from open) ----
        Ha = H[t:]; La = L[t:]
        if side > 0:
            m07 = Ha.max() >= o*1.007; m10 = Ha.max() >= o*1.010; m15 = Ha.max() >= o*1.015
        else:
            m07 = La.min() <= o*0.993; m10 = La.min() <= o*0.990; m15 = La.min() <= o*0.985
        rows.append(dict(date=dt, **feat, cont07=int(m07), cont10=int(m10), cont15=int(m15)))
    return pd.DataFrame(rows)


def analyse(df, label):
    feats = [c for c in df.columns if c not in ("date", "cont07", "cont10", "cont15")]
    ntr = int(len(df)*0.6); TR, TE = df.iloc[:ntr], df.iloc[ntr:]
    base_tr = TR[label].mean()*100; base_te = TE[label].mean()*100
    out = []
    for f in feats:
        x = TR[f].dropna()
        if x.nunique() < 3: 
            # binary feature
            hi_tr = TR[TR[f] == 1]; lo_tr = TR[TR[f] == 0]
            hi_te = TE[TE[f] == 1]; lo_te = TE[TE[f] == 0]
            if len(hi_tr) < 15 or len(lo_tr) < 15: continue
            spread_tr = (hi_tr[label].mean()-lo_tr[label].mean())*100
            spread_te = (hi_te[label].mean()-lo_te[label].mean())*100 if len(hi_te) and len(lo_te) else np.nan
            out.append((f, "=1 vs =0", spread_tr, spread_te, hi_te[label].mean()*100 if len(hi_te) else np.nan, len(hi_te)))
        else:
            q = TR[f].quantile([1/3, 2/3]).values
            def grp(D): return D[D[f] <= q[0]], D[D[f] >= q[1]]
            lo_tr, hi_tr = grp(TR); lo_te, hi_te = grp(TE)
            if len(hi_tr) < 15 or len(lo_tr) < 15: continue
            spread_tr = (hi_tr[label].mean()-lo_tr[label].mean())*100
            spread_te = (hi_te[label].mean()-lo_te[label].mean())*100 if len(hi_te) and len(lo_te) else np.nan
            out.append((f, f"top vs bot 3rd (@{q[1]:.2f}/{q[0]:.2f})", spread_tr, spread_te, hi_te[label].mean()*100 if len(hi_te) else np.nan, len(hi_te)))
    R = pd.DataFrame(out, columns=["feature", "split", "train_spread", "test_spread", "test_top_cont%", "n_te"])
    R["robust"] = np.sign(R.train_spread) == np.sign(R.test_spread)
    return R.reindex(R.test_spread.abs().sort_values(ascending=False).index), base_tr, base_te


def run(sym="ICICIBANK"):
    for side, name in [(+1, "UPSIDE +0.5% (long): continuer vs reverser"),
                       (-1, "DOWNSIDE -0.5% (short): continuer vs reverser")]:
        df = build(sym, side)
        print("="*88); print(f"{name}   ·   {len(df)} touch-days  ·  1-min 2024-05+"); print("="*88)
        for lab, desc in [("cont07", "reach +0.7% after entry"), ("cont10", "reach +1.0%"), ("cont15", "reach +1.5%")]:
            R, btr, bte = analyse(df, lab)
            print(f"\n--- LABEL: {desc}   (base continuation: train {btr:.0f}% / test {bte:.0f}%) ---")
            print(f"  {'feature':<16}{'split':<28}{'trn_d':>8}{'tst_d':>8}{'test_top':>9}{'robust':>8}")
            for _, r in R.head(6).iterrows():
                print(f"  {r.feature:<16}{r.split:<28}{r.train_spread:>+7.0f}{r.test_spread:>+8.0f}{r['test_top_cont%']:>8.0f}%{'YES' if r.robust else 'no':>8}")
        df.to_csv(os.path.expanduser("~")+f"/Downloads/SELECTOR_{sym}.csv", index=False)
        print(f"\nsaved -> ~/Downloads/SELECTOR_{sym}.csv  ({len(df)} touch-days x features + labels)")


if __name__ == "__main__":
    run()
