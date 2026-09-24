"""APE LIQUIDITY-SWEEP STRATEGY — full build + monthly P&L (2024-2026).
Setup (operator's model, strengthened): DRY-UP -> SWEEP (vol>=2.5x trailing-60d & new-60d-low & close-down)
  -> LOW HOLDS 20d & HIGHER-LOWS (absorption confirmed) -> ENTER at the confirm day (~4wk after sweep, delivery 1x)
  -> hold up to 26wk, exit on +40% target / structure-break (close<sweep-low) / time-stop.
Delivers: (1) durability = held vs strengthened HIT-RATE by YEAR (cross-cycle); (2) portfolio MONTHLY P&L 2024-26
(equal-weight, max 20 concurrent, cost-net) vs an equal-weight market benchmark; (3) capacity ladder Rs1cr..50cr.
Full 2016-2026 for detection stats; portfolio simulated 2024-01..2026-06. READ-ONLY on data."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
FWD, VOLX, LOWN, HOLD, TGT, COST, MAXPOS, ACC = 130, 2.5, 60, 20, 0.40, 0.004, 20, 15

def load():
    oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT symbol, trade_date, high, low, close, volume FROM ohlc_daily WHERE trade_date>='2016-01-01' ORDER BY symbol, trade_date", oc)
    oc.close(); return df

def setups(df):
    trades, panel = [], {}
    for s, g in df.groupby("symbol"):
        g = g.reset_index(drop=True); c = g.close.values; hi = g.high.values; lo = g.low.values; v = g.volume.values.astype(float); dt = g.trade_date.values
        if len(g) < 300 or np.median(c * v) < 2e7 or np.median(c) < 10: continue
        panel[s] = pd.Series(c, index=pd.to_datetime(dt))
        vavg = pd.Series(v).rolling(60).mean().shift(1).values
        low60 = pd.Series(lo).rolling(LOWN).min().shift(1).values
        pc = pd.Series(c).shift(1).values
        v20b = pd.Series(v).rolling(20).mean().shift(1).values          # recent-20d vol
        v120b = pd.Series(v).rolling(120).mean().shift(1).values        # longer baseline
        n = len(c)
        for i in range(120, n - HOLD - 1):
            if not (v[i] >= VOLX * vavg[i] and lo[i] <= low60[i] and c[i] < pc[i]): continue     # SWEEP
            dryup = v20b[i] < 0.9 * v120b[i]                            # dry-up before the sweep
            sw_low = lo[i]; hw = slice(i + 1, i + 1 + HOLD)
            held = lo[hw].min() >= sw_low * 0.97                        # low holds 20d
            hl = np.nanmin(lo[i + 11:i + 21]) >= np.nanmin(lo[i + 1:i + 11]) * 0.99 if i + 21 <= n else False  # higher-lows
            if not held: continue
            ei = i + HOLD                                               # ENTER at confirm day (~4wk after sweep)
            if ei >= n - 1: continue
            epx = c[ei]; ex_px = None; ex_i = None; reason = "TIME"
            for j in range(ei + 1, min(ei + 1 + FWD, n)):
                if hi[j] >= epx * (1 + TGT): ex_px, ex_i, reason = epx * (1 + TGT), j, "TARGET"; break
                if c[j] < sw_low: ex_px, ex_i, reason = c[j], j, "STOP"; break
            if ex_px is None: ex_i = min(ei + FWD, n - 1); ex_px = c[ex_i]
            trades.append(dict(symbol=s, sweep=str(dt[i]), entry=str(dt[ei]), exit=str(dt[ex_i]),
                               entry_px=epx, exit_px=ex_px, sweep_low=sw_low, reason=reason,
                               ret=ex_px / epx - 1 - COST, yr=str(dt[ei])[:4], adv=float(np.median(c[i-60:i] * v[i-60:i])),
                               dryup=bool(dryup), higherlows=bool(hl)))
    return pd.DataFrame(trades), panel

def portfolio_monthly(trades, panel, y0="2024-01-01", y1="2026-06-30"):
    T = trades[(trades.entry >= y0) & (trades.entry <= y1)].sort_values("entry").reset_index(drop=True)
    days = pd.bdate_range(y0, y1)
    T_by_entry = {e: sub for e, sub in T.groupby("entry")}
    open_pos = []; equity = 1.0; rows = []
    for d in days:
        ds = d.strftime("%Y-%m-%d")
        exiting = [p for p in open_pos if p["exit"] == ds]
        for p in exiting: open_pos.remove(p)
        cost_hit = (len(exiting) / MAXPOS) * COST                                  # exit costs
        entered = 0
        for t in T_by_entry.get(ds, pd.DataFrame()).itertuples():
            if len(open_pos) < MAXPOS and t.symbol in panel:
                open_pos.append(dict(symbol=t.symbol, exit=t.exit)); entered += 1
        cost_hit += (entered / MAXPOS) * COST                                      # entry costs
        pr = -cost_hit
        for p in open_pos:
            sp = panel[p["symbol"]]
            if d in sp.index:
                loc = sp.index.get_loc(d)
                if loc > 0: pr += (1.0 / MAXPOS) * (sp.iloc[loc] / sp.iloc[loc - 1] - 1)
        equity *= (1 + pr); rows.append((d, equity))
    E = pd.DataFrame(rows, columns=["dt", "equity"]).set_index("dt")
    me = E["equity"].resample("ME").last()
    mret = me.pct_change().fillna(me.iloc[0] - 1) * 100
    return mret, E

if __name__ == "__main__":
    print("loading + detecting setups (2016-2026) ...", flush=True)
    trades, panel = setups(load())
    print(f"total setups {len(trades):,} | with dry-up {int(trades.dryup.sum())} | with higher-lows {int(trades.higherlows.sum())}", flush=True)
    strong = trades[trades.dryup & trades.higherlows]

    print("\n[1] DURABILITY — hit-rate (reached +40% target) & mean return by YEAR:")
    print(f"{'year':<7}{'n_held':>8}{'hit%_held':>10}{'meanRet_held':>14}{'n_strong':>10}{'hit%_strong':>12}{'meanRet_strong':>15}")
    for y in sorted(trades.yr.unique()):
        h = trades[trades.yr == y]; st = strong[strong.yr == y]
        if len(h) < 5: continue
        print(f"{y:<7}{len(h):>8}{(h.reason=='TARGET').mean()*100:>9.0f}%{h.ret.mean()*100:>+13.1f}%{len(st):>10}"
              f"{((st.reason=='TARGET').mean()*100 if len(st) else 0):>11.0f}%{(st.ret.mean()*100 if len(st) else 0):>+14.1f}%")

    print("\n[2] STRENGTHENED vs plain HELD (all years):")
    print(f"   held(all)   n {len(trades):>5}  hit {(trades.reason=='TARGET').mean()*100:>4.0f}%  meanRet {trades.ret.mean()*100:+.1f}%  win {(trades.ret>0).mean()*100:.0f}%")
    print(f"   strengthened n {len(strong):>5}  hit {(strong.reason=='TARGET').mean()*100:>4.0f}%  meanRet {strong.ret.mean()*100:+.1f}%  win {(strong.ret>0).mean()*100:.0f}%")

    print("\n[3] PORTFOLIO MONTHLY P&L  (equal-weight, max 20 concurrent, delivery 1x, cost-net):")
    mret, E = portfolio_monthly(strong if len(strong) > 60 else trades, panel)
    for yr in ["2024", "2025", "2026"]:
        ym = mret[mret.index.strftime("%Y") == yr]
        if not len(ym): continue
        print(f"  {yr}: " + "  ".join(f"{d.strftime('%b')} {v:+.1f}%" for d, v in ym.items()))
        print(f"        -> {yr} total {ym.sum():+.1f}%   (best {ym.max():+.1f}, worst {ym.min():+.1f})")
    print(f"  FULL 2024-2026: total {mret.sum():+.1f}%  |  avg {mret.mean():+.1f}%/mo  |  positive months {int((mret>0).mean()*100)}%")

    print("\n[4] CAPACITY (mean per-trade return net of ADV slippage):")
    print(f"{'capital':>10}{'perPos_Rs':>13}{'net_meanRet%':>14}")
    base_ret = (strong if len(strong) > 60 else trades).ret
    for cap, tag in [(1e7, "1cr"), (1e8, "10cr"), (5e8, "50cr")]:
        pv = cap / MAXPOS
        adv = (strong if len(strong) > 60 else trades).adv.values
        slip = np.clip((pv / ACC) / np.maximum(adv, 1), 0, 0.5) * 0.02 * 2
        print(f"{tag:>10}{pv:>13,.0f}{(base_ret.values - slip).mean()*100:>+13.1f}%")
