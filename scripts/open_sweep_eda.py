"""Layer 2 — opening-window LIQUIDITY SWEEP -> REVERSAL detector, F&O cash tape, 2 yrs.

For each stock-morning we look in 09:16-09:45 for a SWEEP of a key level (price pierces it
making a new extreme) and ask whether it RECLAIMS (closes back through within 3 min = reversal)
or CONTINUES. We test all three levels:
   open      = 09:15 open
   pd_lo/hi  = prior-day low / high
   or_lo/hi  = opening-range (09:15-09:18) low / high
Volume via RVOL = bar volume / (prior-day volume / 375). dry-up = quiet before the sweep;
expansion = volume surge on the reclaim.

Core question: does BUYING the reversal (reclaim) beat CHASING the fall (shorting the break)?
Long side = swept lows; short side = swept highs (mirror). Reported gross; cost noted after.
"""
import sqlite3
from pathlib import Path
import numpy as np
import pandas as pd

DB = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine") / "universe_engine" / "data" / "db" / "kanida_universe.db"
IDX = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}
DET = ("09:16", "09:45")     # sweep must start in this window
RECLAIM_K = 3                # bars to reclaim
VPM_DIV = 375.0


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
        f"FROM ohlc_1min WHERE symbol IN ({ph}) AND substr(bar_time,12,5)>='09:15' AND substr(bar_time,12,5)<='10:30'",
        con, params=fo)
    con.close()
    daily = daily.merge(dclose, on=["symbol", "d"], how="left").sort_values(["symbol", "d"])
    for c in ["lo", "hi", "dclose", "dvol"]:
        daily["p_" + c] = daily.groupby("symbol")[c].shift(1)
    return fo, daily, mb


def process(daily, mb):
    lvl = daily.set_index(["symbol", "d"])[["p_lo", "p_hi", "p_dclose", "p_dvol", "dclose"]].to_dict("index")
    ev = []
    breadth = {}
    for (sym, d), g in mb.groupby(["symbol", "d"], sort=False):
        g = g.sort_values("hm")
        hm = g.hm.values; o = g.open.values; h = g.high.values; l = g.low.values; c = g.close.values; v = g.volume.values
        if len(g) < 20 or hm[0] != "09:15":
            continue
        L = lvl.get((sym, d))
        if not L or not np.isfinite(L.get("p_dvol") or np.nan) or (L["p_dvol"] or 0) <= 0:
            continue
        opn = o[0]
        orm = (hm >= "09:15") & (hm <= "09:18")
        or_lo = l[orm].min(); or_hi = h[orm].max()
        vpm = L["p_dvol"] / VPM_DIV
        rvol = v / vpm
        dayclose = L["dclose"]
        # breadth: early return to 09:18
        i918 = np.where(hm == "09:18")[0]
        if len(i918):
            breadth.setdefault(d, []).append(c[i918[0]] / opn - 1)
        detmask = (hm >= DET[0]) & (hm <= DET[1])
        det_idx = np.where(detmask)[0]
        if not len(det_idx):
            continue

        def fwd(idx, entry, sign):
            def at(k):
                j = idx + k
                return (c[j] / entry - 1) * sign if j < len(c) else np.nan
            eod = (dayclose / entry - 1) * sign if (dayclose and entry) else np.nan
            return at(15), at(30), eod

        for side, levels in [("long", {"open": opn, "pd_lo": L["p_lo"], "or_lo": or_lo}),
                             ("short", {"open": opn, "pd_hi": L["p_hi"], "or_hi": or_hi})]:
            for lname, Lv in levels.items():
                if Lv is None or not np.isfinite(Lv):
                    continue
                # first sweep bar in det window
                if side == "long":
                    sw = [k for k in det_idx if l[k] < Lv]
                else:
                    sw = [k for k in det_idx if h[k] > Lv]
                if not sw:
                    continue
                i = sw[0]
                # reclaim within K bars
                recl = None
                for j in range(i, min(i + RECLAIM_K + 1, len(c))):
                    if (side == "long" and c[j] > Lv) or (side == "short" and c[j] < Lv):
                        recl = j; break
                dry = float(np.nanmean(rvol[max(0, i - 3):i])) if i >= 1 else np.nan
                sweep_rvol = float(rvol[i])
                if recl is not None:
                    entry = c[recl]
                    r15, r30, eod = fwd(recl, entry, +1 if side == "long" else -1)
                    reclaim_rvol = float(rvol[recl])
                else:
                    r15 = r30 = eod = np.nan; reclaim_rvol = np.nan; entry = np.nan
                # chase = trade the break (long side: short the fall; short side: long the pop)
                ce = c[i]
                if side == "long":
                    ch15, ch30, cheod = fwd(i, ce, -1)   # short the break
                else:
                    ch15, ch30, cheod = fwd(i, ce, +1)   # long the break
                ev.append(dict(sym=sym, d=d, side=side, level=lname, reclaimed=recl is not None,
                               dry=dry, sweep_rvol=sweep_rvol, reclaim_rvol=reclaim_rvol,
                               r15=r15, r30=r30, eod=eod, ch15=ch15, ch30=ch30, cheod=cheod))
    bdf = pd.DataFrame([(d, np.mean(np.array(x) < 0) * 100) for d, x in breadth.items()], columns=["d", "pct_down"])
    return pd.DataFrame(ev), bdf


def pct(x):
    x = np.asarray(x, float); x = x[np.isfinite(x)]
    return (x.mean() * 100, (x > 0).mean() * 100, len(x)) if len(x) else (np.nan, np.nan, 0)


def main():
    fo, daily, mb = load()
    print(f"loaded: {len(fo)} F&O stocks | daily rows {len(daily):,} | morning bars {len(mb):,}")
    ev, bdf = process(daily, mb)
    print(f"sweep events: {len(ev):,}\n")

    for side in ["long", "short"]:
        s = ev[ev.side == side]
        print(f"===== {side.upper()} side (swept {'lows->buy reversal' if side=='long' else 'highs->short reversal'}) =====")
        print(f"{'level':<8}{'sweeps':>8}{'reclaim%':>10}{'REV eod%':>10}{'REV %pos':>10}{'REV +30m%':>11}{'CHASE eod%':>12}")
        for lv in (["open", "pd_lo", "or_lo"] if side == "long" else ["open", "pd_hi", "or_hi"]):
            g = s[s.level == lv]
            rec = g.reclaimed.mean() * 100
            rm, rp, rn = pct(g[g.reclaimed].eod)
            r30m, _, _ = pct(g[g.reclaimed].r30)
            cm, cp, cn = pct(g[~g.reclaimed].cheod)   # chase eod for the continuation (non-reclaim) breaks
            print(f"{lv:<8}{len(g):>8}{rec:>10.1f}{rm:>10.3f}{rp:>10.1f}{r30m:>11.3f}{cm:>12.3f}")
        print()

    # ---- does WAIT (reversal) beat CHASE (break) on the SAME events? (open level, long) ----
    print("===== WAIT vs CHASE, same swept-low events (level=open, long) =====")
    g = ev[(ev.side == "long") & (ev.level == "open")]
    wm, wp, wn = pct(g[g.reclaimed].eod)            # reversal buy, reclaimed
    cm, cp, cn = pct(g.cheod)                        # short the break, all
    print(f"  WAIT (buy reclaim, reclaimed only):  eod {wm:+.3f}%  %pos {wp:.0f}%  n={wn}")
    print(f"  CHASE (short the break, all sweeps):  eod {cm:+.3f}%  %pos {cp:.0f}%  n={cn}")

    # ---- volume confirmation + dry-up on the reversal edge (long, open) ----
    print("\n===== Volume conditioning on reversal edge (long, open, reclaimed) =====")
    r = g[g.reclaimed]
    for lab, sub in [("reclaim RVOL>1.5 (volume-confirmed)", r[r.reclaim_rvol > 1.5]),
                     ("reclaim RVOL<=1.5", r[r.reclaim_rvol <= 1.5]),
                     ("dry-up before (pre-RVOL<0.7)", r[r.dry < 0.7]),
                     ("no dry-up (pre-RVOL>=0.7)", r[r.dry >= 0.7])]:
        m, pp, n = pct(sub.eod); print(f"  {lab:<38} eod {m:+.3f}%  %pos {pp:.0f}%  n={n}")

    # ---- breadth conditioning (long reversal works better when market broadly weak?) ----
    print("\n===== Breadth conditioning (long reversal, open) =====")
    r = r.merge(bdf, on="d", how="left")
    for lab, sub in [("broad-weak morning (pct_down>=66)", r[r.pct_down >= 66]),
                     ("neutral (33-66)", r[(r.pct_down > 33) & (r.pct_down < 66)]),
                     ("broad-strong (pct_down<=33)", r[r.pct_down <= 33])]:
        m, pp, n = pct(sub.eod); print(f"  {lab:<34} eod {m:+.3f}%  %pos {pp:.0f}%  n={n}")
    print("\n(costs ~0.05-0.10% round-trip intraday; judge edges vs that.)")


if __name__ == "__main__":
    main()
