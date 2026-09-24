# -*- coding: utf-8 -*-
"""Finalize the ICICI intraday trade setup. Entry: price reaches +-X% from open -> trade (momentum=with
the break, or fade=against). Manage with arm/floor/trail-giveback/hard-stop on CAPITAL basis at 5x MIS,
square off 15:29. Sweep entry X and the config; also test a simple GTT trailing stop. Optimize on a TRAIN
half, validate on a TEST half (guards overfitting). Rs 1,00,000 capital, net of cost.
"""
import os, sqlite3, itertools
import numpy as np, pandas as pd
MDB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "universe_engine", "data", "db", "kanida_universe.db")
LEV = 5.0; CAP = 100000; COST_NOTIONAL = 0.06                 # % round-trip on notional
COST_CAP = COST_NOTIONAL * LEV                                 # 0.30% of capital at 5x


def load_sessions(sym):
    con = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
    b = pd.read_sql_query("SELECT bar_time,open,high,low,close FROM ohlc_1min WHERE symbol=? ORDER BY bar_time", con, params=[sym]); con.close()
    b["date"] = b.bar_time.str[:10]; b["hm"] = b.bar_time.str[11:16]
    b = b[(b.hm >= "09:15") & (b.hm <= "15:29")]
    S = []
    for d, g in b.groupby("date"):
        g = g.sort_values("bar_time")
        if len(g) < 60: continue
        S.append((d, g.open.iloc[0], g.high.values, g.low.values, g.close.iloc[-1]))
    return S


def sim_session(sess, X, mode, arm, floor, give, hs):
    """Return capital-basis P&L% for one session, or None if no trade."""
    d, o, H, L, Cl = sess
    bl = o*(1+X/100); sl = o*(1-X/100)
    ei = -1; direction = 0; entry = None
    for i in range(len(H)):
        hb = H[i] >= bl; hs_ = L[i] <= sl
        if hb or hs_:
            up = hb if not (hb and hs_) else True
            brk_dir = 1 if up else -1                             # up-break = +1
            direction = brk_dir if mode == "momentum" else -brk_dir
            entry = (bl if up else sl); ei = i; break
    if ei == -1: return None
    armed = False; peak = None
    for j in range(ei, len(H)):
        fav_px = H[j] if direction == 1 else L[j]                 # favourable extreme
        adv_px = L[j] if direction == 1 else H[j]                 # adverse extreme
        fav = direction*(fav_px-entry)/entry*100*LEV
        adv = direction*(adv_px-entry)/entry*100*LEV
        if not armed:
            if adv <= -hs: return -hs - COST_CAP
            if fav >= arm:
                armed = True; peak = fav; trail = max(floor, peak-give)
                if adv <= trail: return trail - COST_CAP
        else:
            peak = max(peak, fav); trail = max(floor, peak-give)
            if adv <= trail: return trail - COST_CAP
    exitp = direction*(Cl-entry)/entry*100*LEV
    return exitp - COST_CAP


def evaluate(S, X, mode, arm, floor, give, hs):
    pnls = [p for p in (sim_session(s, X, mode, arm, floor, give, hs) for s in S) if p is not None]
    if len(pnls) < 20: return None
    a = np.array(pnls); eq = np.cumsum(a); peak = np.maximum.accumulate(eq); dd = (peak-eq)
    return dict(n=len(a), wr=round((a > 0).mean()*100, 1), avg=round(a.mean(), 3),
                total_pct=round(a.sum(), 1), total_inr=round(a.sum()/100*CAP),
                maxdd_pct=round(dd.max(), 1), ret_dd=round(a.sum()/max(dd.max(), 1e-9), 2))


def main(sym="ICICIBANK"):
    S = load_sessions(sym); ntr = int(len(S)*0.6); TR, TE = S[:ntr], S[ntr:]
    print(f"{sym}: {len(S)} sessions  ·  train {len(TR)} ({TR[0][0]}->{TR[-1][0]})  ·  test {len(TE)} ({TE[0][0]}->{TE[-1][0]})")
    print(f"5x MIS · capital-basis trail · cost {COST_CAP:.2f}%/trade capital\n")
    # TIGHT trail configs (capital basis @5x; 1% cap = 0.2% stock). Testing "trail every 0.1-0.2%".
    Xs = [0.4, 0.5, 0.6]; modes = ["momentum"]
    arms = [0.5, 1.0, 1.5]        # 0.1 / 0.2 / 0.3 % stock before trailing arms
    floors = [0, 0.5]
    gives = [0.5, 0.75, 1.0]      # 0.1 / 0.15 / 0.2 % stock trailing give-back  <-- the tight trail
    hss = [1.0, 1.5, 2.5]         # 0.2 / 0.3 / 0.5 % stock hard stop
    grid = list(itertools.product(Xs, modes, arms, floors, gives, hss))
    rows = []
    for (X, mode, arm, fl, gv, hsv) in grid:
        tr = evaluate(TR, X, mode, arm, fl, gv, hsv)
        if tr is None: continue
        rows.append(dict(X=X, mode=mode, arm=arm, floor=fl, give=gv, hard=hsv, **{f"tr_{k}": v for k, v in tr.items()}))
    G = pd.DataFrame(rows)
    print("===== TRAIN: top 8 configs by return/max-DD =====")
    top = G.sort_values("tr_ret_dd", ascending=False).head(8)
    print(top[["X", "mode", "arm", "floor", "give", "hard", "tr_n", "tr_wr", "tr_avg", "tr_total_inr", "tr_maxdd_pct", "tr_ret_dd"]].to_string(index=False))
    # validate top-5 on test
    print("\n===== TEST (out-of-sample) validation of the train-best configs =====")
    print(f"  {'X':>4}{'mode':>10}{'arm':>4}{'flr':>4}{'giv':>4}{'hrd':>4}  |  {'train Rs':>10}{'trDD%':>7}  ||  {'TEST Rs':>10}{'teWR':>6}{'teDD%':>7}{'te_ret/DD':>10}")
    for _, r in G.sort_values("tr_ret_dd", ascending=False).head(6).iterrows():
        te = evaluate(TE, r.X, r["mode"], r.arm, r.floor, r.give, r.hard)
        if te is None: continue
        print(f"  {r.X:>4}{r['mode']:>10}{int(r.arm):>4}{r.floor:>4}{r.give:>4}{int(r.hard):>4}  |  {r.tr_total_inr:>10,.0f}{r.tr_maxdd_pct:>7.1f}  ||  {te['total_inr']:>10,.0f}{te['wr']:>6.1f}{te['maxdd_pct']:>7.1f}{te['ret_dd']:>10.2f}")
    # simple GTT trailing stop comparison (pure trail: arm=0, floor=0, give=G, hard=give)
    print("\n===== SIMPLE GTT TRAILING STOP (pure trail, no arm/floor) =====")
    for X in Xs:
        for mode in modes:
            for gv in [2, 3, 4]:
                te = evaluate(TE, X, mode, 0.01, 0, gv, gv)
                tr = evaluate(TR, X, mode, 0.01, 0, gv, gv)
                if te and tr: print(f"  X={X} {mode:<9} trail {gv}%cap  ·  train Rs {tr['total_inr']:>9,.0f}  test Rs {te['total_inr']:>9,.0f} (WR {te['wr']}%, DD {te['maxdd_pct']}%)")
    G.to_csv(os.path.expanduser("~")+f"/Downloads/TRAIL_SWEEP_{sym}.csv", index=False)
    print(f"\nsaved full grid -> ~/Downloads/TRAIL_SWEEP_{sym}.csv")


if __name__ == "__main__":
    main()
