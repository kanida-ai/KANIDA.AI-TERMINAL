"""Layer-2 v2 — context-gated sweep-reversal at SHORT horizons. Faster detector (numpy
group slices), saves events to pickle for instant re-analysis. Adds 5/10/15/30-min exits.

Context-gated logic (from v1: edge runs WITH the broad tape, not against the sweep):
  broad-STRONG tape  -> BUY  swept-low reversals  (dip-buy, aligned with up-tape)
  broad-WEAK   tape  -> SHORT swept-high reversals (fade the pop, aligned with down-tape)
Reports each leg by breadth bucket x horizon, the combined gated strategy net of cost, and
the short-side breadth mirror.
"""
import sqlite3, pickle
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
EVCACHE = ROOT / "docs" / "ops" / "_open_sweep_events.pkl"
IDX = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}
DET = ("09:16", "09:45"); RECLAIM_K = 3; VPM_DIV = 375.0
COST = 0.06   # % round-trip intraday (futures brokerage + slippage), for net figures


def load():
    con = sqlite3.connect(str(DB))
    fo = [r[0] for r in con.execute("SELECT symbol FROM fo_stock_master").fetchall() if r[0] not in IDX]
    ph = ",".join("?" * len(fo))
    daily = pd.read_sql_query(
        f"SELECT symbol, substr(bar_time,1,10) d, min(low) lo, max(high) hi, sum(volume) dvol "
        f"FROM ohlc_1min WHERE symbol IN ({ph}) GROUP BY symbol, substr(bar_time,1,10)", con, params=fo)
    dclose = pd.read_sql_query(
        f"SELECT symbol, substr(bar_time,1,10) d, close dclose FROM ohlc_1min "
        f"WHERE symbol IN ({ph}) AND substr(bar_time,12,5)='15:29'", con, params=fo)
    mb = pd.read_sql_query(
        f"SELECT symbol, substr(bar_time,1,10) d, substr(bar_time,12,5) hm, open, high, low, close, volume "
        f"FROM ohlc_1min WHERE symbol IN ({ph}) AND substr(bar_time,12,5)>='09:15' AND substr(bar_time,12,5)<='10:30' "
        f"ORDER BY symbol, bar_time", con, params=fo)
    con.close()
    daily = daily.merge(dclose, on=["symbol", "d"], how="left").sort_values(["symbol", "d"])
    for c in ["lo", "hi", "dclose", "dvol"]:
        daily["p_" + c] = daily.groupby("symbol")[c].shift(1)
    return fo, daily, mb


def detect(daily, mb):
    lvl = daily.set_index(["symbol", "d"])[["p_lo", "p_hi", "p_dvol", "dclose"]].to_dict("index")
    O = mb.open.values; H = mb.high.values; Lw = mb.low.values; C = mb.close.values; V = mb.volume.values; HM = mb.hm.values.astype("U5")
    idxmap = mb.groupby(["symbol", "d"], sort=False).indices
    ev = []; breadth = {}
    for (sym, d), pos in idxmap.items():
        o = O[pos]; h = H[pos]; l = Lw[pos]; c = C[pos]; v = V[pos]; hm = HM[pos]
        if len(pos) < 20 or hm[0] != "09:15":
            continue
        Lm = lvl.get((sym, d))
        if not Lm or not Lm.get("p_dvol") or Lm["p_dvol"] <= 0:
            continue
        opn = o[0]; orm = hm <= "09:18"
        or_lo = l[orm].min(); or_hi = h[orm].max()
        rvol = v / (Lm["p_dvol"] / VPM_DIV); dayclose = Lm["dclose"]
        i918 = np.where(hm == "09:18")[0]
        if len(i918): breadth.setdefault(d, []).append(c[i918[0]] / opn - 1)
        det_idx = np.where((hm >= DET[0]) & (hm <= DET[1]))[0]
        if not len(det_idx): continue
        n = len(c)

        def fwd(idx, entry, sign):
            f = lambda k: (c[idx + k] / entry - 1) * sign * 100 if idx + k < n else np.nan
            eod = (dayclose / entry - 1) * sign * 100 if (dayclose and entry) else np.nan
            return f(5), f(10), f(15), f(30), eod

        for side, levels in [("long", {"open": opn, "pd_lo": Lm["p_lo"], "or_lo": or_lo}),
                             ("short", {"open": opn, "pd_hi": Lm["p_hi"], "or_hi": or_hi})]:
            for lname, Lv in levels.items():
                if Lv is None or not np.isfinite(Lv): continue
                sw = det_idx[(l[det_idx] < Lv)] if side == "long" else det_idx[(h[det_idx] > Lv)]
                if not len(sw): continue
                i = int(sw[0]); recl = None
                for j in range(i, min(i + RECLAIM_K + 1, n)):
                    if (side == "long" and c[j] > Lv) or (side == "short" and c[j] < Lv):
                        recl = j; break
                sgn = +1 if side == "long" else -1
                if recl is not None:
                    r5, r10, r15, r30, eod = fwd(recl, c[recl], sgn); rrv = float(rvol[recl])
                else:
                    r5 = r10 = r15 = r30 = eod = np.nan; rrv = np.nan
                ch5, ch10, ch15, ch30, cheod = fwd(i, c[i], -sgn)   # chase = trade the break
                ev.append((sym, d, side, lname, recl is not None,
                           float(np.nanmean(rvol[max(0, i - 3):i])) if i >= 1 else np.nan,
                           float(rvol[i]), rrv, r5, r10, r15, r30, eod, ch5, ch15, cheod))
    cols = ["sym", "d", "side", "level", "reclaimed", "dry", "sweep_rvol", "reclaim_rvol",
            "r5", "r10", "r15", "r30", "eod", "ch5", "ch15", "cheod"]
    ev = pd.DataFrame(ev, columns=cols)
    bdf = pd.DataFrame([(d, np.mean(np.array(x) < 0) * 100) for d, x in breadth.items()], columns=["d", "pct_down"])
    return ev, bdf


def stat(x):
    x = np.asarray(x, float); x = x[np.isfinite(x)]
    return (round(x.mean(), 3), round((x > 0).mean() * 100, 0), len(x)) if len(x) else (np.nan, np.nan, 0)


def main():
    if EVCACHE.exists():
        ev, bdf = pickle.load(open(EVCACHE, "rb")); print(f"[cache] {len(ev):,} events")
    else:
        fo, daily, mb = load(); print(f"loaded {len(mb):,} morning bars"); ev, bdf = detect(daily, mb)
        pickle.dump((ev, bdf), open(EVCACHE, "wb")); print(f"detected + saved {len(ev):,} events")
    ev = ev.merge(bdf, on="d", how="left")
    ev["ctx"] = pd.cut(ev.pct_down, [-1, 33, 66, 101], labels=["strong", "neutral", "weak"])
    R = ev[ev.reclaimed]

    print("\n=== Reversal edge by CONTEXT x HORIZON (mean % / %pos / n), net-of-cost in [] ===")
    for side, lv in [("long", "open"), ("long", "pd_lo"), ("short", "open"), ("short", "pd_hi")]:
        print(f"\n{side.upper()} reversal, level={lv}:")
        g = R[(R.side == side) & (R.level == lv)]
        print(f"  {'context':<9}{'+5m':>16}{'+15m':>16}{'EOD':>16}")
        for ctx in ["strong", "neutral", "weak"]:
            gg = g[g.ctx == ctx]
            def cell(col):
                m, pp, nn = stat(gg[col]);
                return f"{m:+.3f}/{pp:.0f}%" if nn else "—"
            print(f"  {ctx:<9}{cell('r5'):>16}{cell('r15'):>16}{cell('eod'):>16}   n={len(gg)}")

    print("\n=== CONTEXT-GATED STRATEGY: strong->buy swept-low(open) rev ; weak->short swept-high(pd_hi) rev ===")
    longs = R[(R.side == "long") & (R.level == "open") & (R.ctx == "strong")]
    shorts = R[(R.side == "short") & (R.level == "pd_hi") & (R.ctx == "weak")]
    for lab, horizon in [("+5 min", "r5"), ("+15 min", "r15"), ("+30 min", "r30"), ("EOD", "eod")]:
        pool = pd.concat([longs[horizon], shorts[horizon]])
        m, pp, nn = stat(pool)
        print(f"  {lab:<8} gross {m:+.3f}%  net {m-COST:+.3f}%  %pos {pp:.0f}%  n={nn}")
    print(f"  (legs: {len(longs)} long-in-strong + {len(shorts)} short-in-weak; cost {COST}%/round-trip)")

    print("\n=== + volume-confirmed (reclaim RVOL>1.5) on the gated strategy ===")
    lv2 = longs[longs.reclaim_rvol > 1.5]; sv2 = shorts[shorts.reclaim_rvol > 1.5]
    for lab, horizon in [("+5 min", "r5"), ("+15 min", "r15"), ("EOD", "eod")]:
        pool = pd.concat([lv2[horizon], sv2[horizon]]); m, pp, nn = stat(pool)
        print(f"  {lab:<8} gross {m:+.3f}%  net {m-COST:+.3f}%  %pos {pp:.0f}%  n={nn}")


if __name__ == "__main__":
    main()
