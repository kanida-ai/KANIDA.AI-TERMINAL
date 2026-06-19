"""
Falcon TIER DERIVATION v2  —  hunt for a ROBUST >80% WR signal-time cell.

v1 tested price x volume and topped out ~74-76% OOS. v2 adds the signal-day
dimensions surfaced by a second analysis and tests them HONESTLY:
    - two_day_ret_pct      backward 2-day pullback   (VERIFIED signal-time safe)
    - signal_day_range_pct narrow-candle compression (VERIFIED signal-time safe)
    - avg_lift / engine_rank / n_fires  pattern strength (known at signal time)
    - entry_gap_pct        NEXT-DAY open gap          (VERIFIED LOOK-AHEAD ->
                           reported only as an ENTRY OVERLAY, never a tier)

Honesty gates for any cell to count as a real >80% tier:
    IS_N >= 100  AND  OOS_N >= 30  AND  OOS_WR >= 80
    AND worst single year (years with N>=20) WR >= 75   <- kills 1-year artifacts

Read-only. Writes nothing to any DB.
Run: "C:\\Users\\SPS\\anaconda3\\python.exe" scripts/tier_price_volume_derivation_v2.py
"""
import os, sqlite3, itertools
import numpy as np, pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RND_DB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")

OOS_CUTOFF = "2025-12-31"
MIN_IS_N, MIN_OOS_N, WR_TARGET, WORST_YEAR_FLOOR = 100, 30, 80.0, 75.0


def load_cohort():
    con = sqlite3.connect(RND_DB)
    df = pd.read_sql_query(
        """
        SELECT s.signal_date, s.symbol,
               s.avg_lift, s.engine_rank, s.n_fires,
               x.signal_day_ret_pct   AS sret,
               x.two_day_ret_pct      AS twoday,
               x.signal_day_range_pct AS rng,
               x.entry_gap_pct        AS entry_gap,
               s.net_ret_pct          AS net_ret,
               s.exit_reason          AS exit_reason
        FROM falcon_signal_day_study   s
        JOIN falcon_signal_day_context x
          ON s.signal_date = x.signal_date AND s.symbol = x.symbol
        WHERE s.net_ret_pct IS NOT NULL AND x.is_extension = 0
        """, con)
    con.close()
    df["win"]  = (df["net_ret"] > 0).astype(int)
    df["stop"] = (df["exit_reason"] == "INIT_STOP").astype(int)
    df["yr"]   = df["signal_date"].str[:4]
    return df


def st(sub):
    n = len(sub)
    if n == 0: return (0, float("nan"), float("nan"), float("nan"))
    return (n, 100*sub["win"].mean(), sub["net_ret"].mean(), 100*sub["stop"].mean())


def report(df, name, mask, safe=True):
    """Print IS/OOS + per-year for one rule, and return a verdict dict."""
    d = df[mask(df)]
    is_d  = d[d["signal_date"] <= OOS_CUTOFF]
    oos_d = d[d["signal_date"] >  OOS_CUTOFF]
    isn, iswr, isret, _   = st(is_d)
    on, owr, oret, ost    = st(oos_d)
    tag = "" if safe else "  [LOOK-AHEAD: entry overlay only, NOT a signal-time tier]"
    print(f"\n--- {name}{tag}")
    print(f"    IS : N={isn:<5} WR={iswr:5.1f}%  ret={isret:+5.2f}%")
    print(f"    OOS: N={on:<5} WR={owr:5.1f}%  ret={oret:+5.2f}%  stop={ost:4.1f}%")
    # per-year
    yrs = []
    for yr, g in d.groupby("yr"):
        n, wr, ret, _ = st(g)
        yrs.append((yr, n, wr))
    print("    per-year: " + "  ".join(f"{yr}:{n}@{wr:.0f}%" for yr, n, wr in yrs))
    worst = min((wr for yr, n, wr in yrs if n >= 20), default=float("nan"))
    passes = (safe and isn >= MIN_IS_N and on >= MIN_OOS_N
              and owr >= WR_TARGET and worst >= WORST_YEAR_FLOOR)
    verdict = ("PASS >80% ROBUST" if passes else
               "fails gate" + ("" if safe else " (look-ahead)"))
    print(f"    worst-year WR (N>=20) = {worst:.1f}%   ->  {verdict}")
    return dict(name=name, isn=isn, iswr=iswr, on=on, owr=owr, oret=oret,
                worst=worst, passes=passes, safe=safe)


def main():
    df = load_cohort()
    print(f"cohort: {len(df)} rows | avg_lift quantiles: "
          + ", ".join(f"{q}:{df['avg_lift'].quantile(q):.1f}" for q in (.25,.5,.75,.9)))

    print("\n================ BASELINES ================")
    report(df, "GOLD baseline: sret<=2", lambda d: d["sret"] <= 2)
    report(df, "flat/down only: sret<=0", lambda d: d["sret"] <= 0)

    print("\n========== CLAIMED >80% COMBOS (verify) ==========")
    res = []
    res.append(report(df, "A  pullback: sret<=2 & twoday<-5 & avg_lift>15",
        lambda d: (d["sret"]<=2) & (d["twoday"]<-5) & (d["avg_lift"]>15)))
    res.append(report(df, "A0 pullback(flat/down): sret<=0 & twoday<-5 & avg_lift>15",
        lambda d: (d["sret"]<=0) & (d["twoday"]<-5) & (d["avg_lift"]>15)))
    res.append(report(df, "B  range-compress: sret<=2 & rng<2 & avg_lift>15",
        lambda d: (d["sret"]<=2) & (d["rng"]<2) & (d["avg_lift"]>15)))
    res.append(report(df, "E-E rank+pullback: engine_rank<=5 & twoday<-5 & avg_lift>15",
        lambda d: (d["engine_rank"]<=5) & (d["twoday"]<-5) & (d["avg_lift"]>15)))
    res.append(report(df, "A+B pullback & compression: sret<=2 & twoday<-5 & rng<2 & avg_lift>15",
        lambda d: (d["sret"]<=2) & (d["twoday"]<-5) & (d["rng"]<2) & (d["avg_lift"]>15)))
    # look-ahead, reported separately:
    res.append(report(df, "C  GAP (look-ahead): sret<=2 & entry_gap<-2 & avg_lift>15",
        lambda d: (d["sret"]<=2) & (d["entry_gap"]<-2) & (d["avg_lift"]>15), safe=False))

    print("\n========== GUARDED SEARCH for robust >80% (signal-time-safe only) ==========")
    print("gates: IS_N>=100, OOS_N>=30, OOS_WR>=80, worst-year(N>=20)>=75")
    hits = []
    sret_cuts   = [0, 2]
    twoday_cuts = [None, -3, -5, -8]
    rng_cuts    = [None, 1.5, 2, 3]
    lift_cuts   = [None, 12, 15, 18]
    for sc, tc, rc, lc in itertools.product(sret_cuts, twoday_cuts, rng_cuts, lift_cuts):
        def mk(d, sc=sc, tc=tc, rc=rc, lc=lc):
            m = d["sret"] <= sc
            if tc is not None: m &= d["twoday"] < tc
            if rc is not None: m &= d["rng"]   < rc
            if lc is not None: m &= d["avg_lift"] > lc
            return m
        d = df[mk(df)]
        is_d, oos_d = d[d.signal_date<=OOS_CUTOFF], d[d.signal_date>OOS_CUTOFF]
        isn, iswr, _, _ = st(is_d); on, owr, oret, ost = st(oos_d)
        if isn < MIN_IS_N or on < MIN_OOS_N or owr < WR_TARGET:
            continue
        worst = min((st(g)[1] for _, g in d.groupby("yr") if len(g) >= 20),
                    default=float("nan"))
        if worst >= WORST_YEAR_FLOOR:
            hits.append((sc, tc, rc, lc, isn, iswr, on, owr, oret, ost, worst))
    if hits:
        print("  ROBUST >80% CELLS FOUND:")
        for sc,tc,rc,lc,isn,iswr,on,owr,oret,ost,worst in sorted(hits,key=lambda h:-h[7]):
            print(f"   sret<={sc} twoday<{tc} rng<{rc} lift>{lc} | "
                  f"IS {isn}@{iswr:.0f}%  OOS {on}@{owr:.0f}% ret{oret:+.1f}% stop{ost:.0f}%  worstyr {worst:.0f}%")
    else:
        print("  NONE. No signal-time-safe cell clears OOS_WR>=80 with N>=100/30 and worst-year>=75.")

    print("\n================ VERDICT ================")
    robust = [r for r in res if r["passes"]]
    if robust:
        print("Robust >80% signal-time tiers (survive OOS + per-year):")
        for r in robust:
            print(f"  - {r['name']}: OOS {r['on']}@{r['owr']:.0f}% worstyr {r['worst']:.0f}%")
    else:
        print("No CLAIMED combo passes the full robustness gate as a signal-time tier.")
    print("(read-only; no DB writes)")


if __name__ == "__main__":
    main()
