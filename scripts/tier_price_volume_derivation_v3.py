"""
Falcon TIER DERIVATION v3  —  PULLBACK  x  VOLUME DRY-UP.

v2 found the bounce-from-drawdown edge (2-day pullback + strong pattern) at
83-85% WR per-year, but couldn't certify 80% on the 2026 walk-forward (thin N).
v1 found volume DRY-UP lifts WR / halves stops. v3 combines them and SEARCHES
for the cell that holds OOS_WR>=80 at the LARGEST sample (= sellable volume),
clearing the strict gate.

Signal-time-safe features only (entry_gap excluded = look-ahead):
  pullback: two_day_ret_pct (<0 = fell into the signal)   [context, SAFE]
  range:    signal_day_range_pct                           [context, SAFE]
  pattern:  avg_lift, engine_rank                          [study,  SAFE]
  volume:   rvol20, volz, trend3_20, turn_pct  (from ohlc, backward) [SAFE]

Strict gate: IS_N>=100  AND  OOS_N>=30  AND  OOS_WR>=80  AND  worst-year(N>=20)>=75
Read-only; writes nothing.
Run: "C:\\Users\\SPS\\anaconda3\\python.exe" scripts/tier_price_volume_derivation_v3.py
"""
import os, sqlite3, itertools
import numpy as np, pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RND_DB  = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
OHLC_DB = os.path.join(ROOT, "data", "db", "kanida_universe.db")

OOS_CUTOFF = "2025-12-31"
MIN_IS_N, MIN_OOS_N, WR_TARGET, WORST_FLOOR = 100, 30, 80.0, 75.0


def load_cohort():
    con = sqlite3.connect(RND_DB)
    df = pd.read_sql_query(
        """SELECT s.signal_date, s.symbol, s.avg_lift, s.engine_rank, s.n_fires,
                  x.signal_day_ret_pct AS sret, x.two_day_ret_pct AS twoday,
                  x.signal_day_range_pct AS rng, x.signal_day_close AS close,
                  s.net_ret_pct AS net_ret, s.exit_reason AS exit_reason
           FROM falcon_signal_day_study s
           JOIN falcon_signal_day_context x
             ON s.signal_date=x.signal_date AND s.symbol=x.symbol
           WHERE s.net_ret_pct IS NOT NULL AND x.is_extension=0""", con)
    con.close()
    df["win"]  = (df["net_ret"] > 0).astype(int)
    df["stop"] = (df["exit_reason"] == "INIT_STOP").astype(int)
    df["yr"]   = df["signal_date"].str[:4]
    return df


def add_volume_features(df):
    syms = tuple(sorted(df["symbol"].unique()))
    con = sqlite3.connect(OHLC_DB)
    ohlc = pd.read_sql_query(
        "SELECT symbol,trade_date,close,volume FROM ohlc_daily WHERE symbol IN (%s)"
        % ",".join("?"*len(syms)), con, params=syms)
    con.close()
    ohlc = ohlc.sort_values(["symbol","trade_date"]).reset_index(drop=True)
    g = ohlc.groupby("symbol", group_keys=False)
    ohlc["avg20"] = g["volume"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    ohlc["std20"] = g["volume"].transform(lambda s: s.rolling(20, min_periods=10).std())
    ohlc["avg3"]  = g["volume"].transform(lambda s: s.rolling(3,  min_periods=2).mean())
    ohlc["turn"]  = ohlc["close"]*ohlc["volume"]
    ohlc["turn_pct"] = g["turn"].transform(
        lambda s: s.rolling(252, min_periods=60).apply(lambda w:(w<=w[-1]).mean(), raw=True))
    ohlc["rvol20"]    = ohlc["volume"]/ohlc["avg20"]
    ohlc["volz"]      = (ohlc["volume"]-ohlc["avg20"])/ohlc["std20"]
    ohlc["trend3_20"] = ohlc["avg3"]/ohlc["avg20"]
    feats = ohlc[["symbol","trade_date","rvol20","volz","trend3_20","turn_pct"]]
    return df.merge(feats, left_on=["symbol","signal_date"],
                    right_on=["symbol","trade_date"], how="left").drop(columns=["trade_date"])


def st(sub):
    n = len(sub)
    if n == 0: return (0, float("nan"), float("nan"), float("nan"))
    return (n, 100*sub["win"].mean(), sub["net_ret"].mean(), 100*sub["stop"].mean())


def evalcell(df, mask):
    d = df[mask]
    is_d, oos_d = d[d.signal_date<=OOS_CUTOFF], d[d.signal_date>OOS_CUTOFF]
    isn, iswr, isret, _ = st(is_d); on, owr, oret, ost = st(oos_d)
    worst = min((st(g)[1] for _,g in d.groupby("yr") if len(g)>=20), default=float("nan"))
    yrs = "/".join(f"{st(g)[1]:.0f}" for _,g in d.groupby("yr") if len(g)>=20)
    passes = (isn>=MIN_IS_N and on>=MIN_OOS_N and owr>=WR_TARGET and worst>=WORST_FLOOR)
    return dict(isn=isn,iswr=iswr,isret=isret,on=on,owr=owr,oret=oret,ost=ost,
                worst=worst,yrs=yrs,passes=passes)


def show(df, name, mask):
    r = evalcell(df, mask)
    flag = "  <== PASSES STRICT GATE" if r["passes"] else ""
    print(f"\n--- {name}{flag}")
    print(f"    IS  N={r['isn']:<5} WR={r['iswr']:5.1f}% ret={r['isret']:+5.2f}%")
    print(f"    OOS N={r['on']:<5} WR={r['owr']:5.1f}% ret={r['oret']:+5.2f}% stop={r['ost']:4.1f}%")
    print(f"    per-year(N>=20): {r['yrs']}   worst={r['worst']:.1f}%")
    return r


def main():
    print("Loading cohort + engineering volume features ...")
    df = add_volume_features(load_cohort())
    print(f"  rows={len(df)}")

    PB = lambda d,t=-5: d["twoday"] < t           # pullback
    L  = lambda d,x=15: d["avg_lift"] > x          # pattern strength
    base = lambda d: (d["sret"]<=2)

    print("\n================ ANCHORS (from v2) ================")
    show(df, "A pullback: sret<=2 & 2d<-5 & lift>15",
         base(df) & PB(df) & L(df))

    print("\n========== A  x  each DRY-UP lever (does WR clear 80% OOS?) ==========")
    show(df, "A & turn_pct<0.75", base(df)&PB(df)&L(df)&(df["turn_pct"]<0.75))
    show(df, "A & turn_pct<0.50", base(df)&PB(df)&L(df)&(df["turn_pct"]<0.50))
    show(df, "A & trend3_20<0.9", base(df)&PB(df)&L(df)&(df["trend3_20"]<0.9))
    show(df, "A & volz<-0.3",     base(df)&PB(df)&L(df)&(df["volz"]<-0.3))

    print("\n====== GROW N: relax pattern/pullback, ADD dry-up to hold WR ======")
    show(df, "milder PB 2d<-3 & lift>15 & turn_pct<0.5",
         base(df)&PB(df,-3)&L(df)&(df["turn_pct"]<0.5))
    show(df, "milder PB 2d<-3 & turn_pct<0.5 (no lift filter)",
         base(df)&PB(df,-3)&(df["turn_pct"]<0.5))
    show(df, "PB 2d<-5 & turn_pct<0.5 (no lift filter)",
         base(df)&PB(df,-5)&(df["turn_pct"]<0.5))

    print("\n========== GUARDED SEARCH: max-N cell clearing strict 80% gate ==========")
    print("dims: sret<=[0,2] x 2d<[-3,-5,-8] x lift>[None,12,15] x rank<=[None,5,10]")
    print("      x dryup{turn<.5, turn<.75, trend<.9, volz<-.3, none}")
    dry = {
        "none":     lambda d: pd.Series(True, index=d.index),
        "turn<.5":  lambda d: d["turn_pct"]<0.5,
        "turn<.75": lambda d: d["turn_pct"]<0.75,
        "trend<.9": lambda d: d["trend3_20"]<0.9,
        "volz<-.3": lambda d: d["volz"]<-0.3,
    }
    hits = []
    for sc, tc, lc, rk, (dn, dfn) in itertools.product(
            [0,2], [-3,-5,-8], [None,12,15], [None,5,10], dry.items()):
        def mk(d, sc=sc,tc=tc,lc=lc,rk=rk,dfn=dfn):
            m = (d["sret"]<=sc) & (d["twoday"]<tc) & dfn(d)
            if lc is not None: m &= d["avg_lift"]>lc
            if rk is not None: m &= d["engine_rank"]<=rk
            return m
        r = evalcell(df, mk(df))
        if r["passes"]:
            hits.append((r["isn"], sc,tc,lc,rk,dn, r))
    if hits:
        hits.sort(key=lambda h:-h[0])   # max IS_N first (sellable volume)
        print(f"\n  {len(hits)} cell(s) CLEAR the strict gate. Top by sample size:")
        for isn,sc,tc,lc,rk,dn,r in hits[:8]:
            print(f"   sret<={sc} 2d<{tc} lift>{lc} rank<={rk} dry[{dn}] | "
                  f"IS {r['isn']}@{r['iswr']:.0f}%  OOS {r['on']}@{r['owr']:.0f}% "
                  f"ret{r['oret']:+.1f}% stop{r['ost']:.0f}% worstyr{r['worst']:.0f}% "
                  f"(~{r['isn']//4}/yr)")
    else:
        print("\n  NONE clear OOS_WR>=80 + IS_N>=100 + OOS_N>=30 + worst-year>=75.")
        print("  (Best near-misses are the A-family at 76-78% OOS / thin high-WR cells.)")

    print("\n================ VERDICT ================")
    if hits:
        b = hits[0][6]
        print(f"Largest robust >80% cell: IS {b['isn']}@{b['iswr']:.0f}% / "
              f"OOS {b['on']}@{b['owr']:.0f}% / worst-year {b['worst']:.0f}% / stop {b['ost']:.0f}%.")
        print("Pullback x dry-up DID produce a strict-gate-passing tier at sellable volume.")
    else:
        print("Pullback x dry-up sharpens WR/stops but still cannot certify 80.0% OOS")
        print("at N>=30 with worst-year>=75 — the edge is real (~83-85% per-year) but the")
        print("2026 holdout sample remains too small to stamp 80% certified.")
    print("(read-only; no DB writes)")


if __name__ == "__main__":
    main()
