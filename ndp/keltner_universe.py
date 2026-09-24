# -*- coding: utf-8 -*-
"""Universe-wide rich-Keltner backtest (Path 2). Buy-the-dip mean-reversion (close stretched >= z ATRs
below EMA), enter next-day OPEN, exit next-day CLOSE (intraday MIS), net of cost. Two versions per stock:
  (a) UNIVERSAL fixed setting (ma=20, atr=10, z=2.5)   — robust, hardest to overfit
  (b) PER-STOCK DNA tuning via leak-free expanding weekly walk-forward — every stock its own best config
Leak-free. Full daily history. Outputs per-stock scorecard + daily P&L + yearly performance.
"""
import os, sqlite3, multiprocessing as mp
import numpy as np, pandas as pd
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
COST = 0.06                                   # MIS intraday round-trip %, net
UNIVERSAL = dict(ma=20, atr=10, z=2.5, mode="breach")
GRID = [dict(ma=ma, atr=at, z=z, mode=md) for ma in (10, 20, 34) for at in (10, 14, 20)
        for z in (1.5, 2.0, 2.5, 3.0) for md in ("breach", "reentry")]
MIN_TRAIN_WK = 26; MIN_SIG = 8


def _ema(s, n): return pd.Series(s).ewm(span=n, adjust=False).mean().values
def _rma(s, n): return pd.Series(s).ewm(alpha=1/n, adjust=False).mean().values
def _atr(h, l, c, n):
    pc = np.roll(c, 1); pc[0] = c[0]
    tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc))); return _rma(tr, n)


def buy_mask(D, p):
    m = _ema(D["c"], p["ma"]); a = _atr(D["h"], D["l"], D["c"], p["atr"]); st = (D["c"] - m) / (a + 1e-9)
    if p["mode"] == "breach":
        b = st <= -p["z"]
    else:
        pv = np.roll(st, 1); b = (pv <= -p["z"]) & (st > -p["z"])
    b[np.isnan(st)] = False; return b


def one_stock(sym):
    try:
        con = sqlite3.connect("file:" + DB.replace("\\", "/") + "?mode=ro", uri=True)
        g = pd.read_sql_query("SELECT trade_date,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? ORDER BY trade_date",
                              con, params=[sym]); con.close()
        if len(g) < 400: return None
        D = dict(o=g.open.values.astype(float), h=g.high.values.astype(float), l=g.low.values.astype(float),
                 c=g.close.values.astype(float))
        o1 = np.roll(D["o"], -1); c1 = np.roll(D["c"], -1); oc = (c1 - o1) / o1 * 100.0; oc[-1] = np.nan
        dates = g.trade_date.values; valid = ~np.isnan(oc)
        # (a) universal
        ub = buy_mask(D, UNIVERSAL) & valid
        u_ret = oc[ub] - COST; u_n = int(ub.sum())
        u_rows = list(zip(dates[ub], np.round(u_ret, 3)))                    # for daily P&L
        u_wr = float((oc[ub] >= 0.5).mean()) if u_n else np.nan; u_E = float(u_ret.mean()) if u_n else np.nan
        # (b) per-stock DNA walk-forward
        wk = pd.to_datetime(g.trade_date).dt.strftime("%G%V").values
        masks = {i: buy_mask(D, p) & valid for i, p in enumerate(GRID)}
        cum = {i: (np.cumsum(masks[i]), np.cumsum(np.where(masks[i], oc, 0.0)), np.cumsum(masks[i] & (oc >= 0.5)))
               for i in masks}
        weeks = sorted(set(wk)); widx = {w: np.where(wk == w)[0] for w in weeks}
        t_hits = 0; t_n = 0; t_ret = 0.0; cfg_count = {}
        for wi in range(MIN_TRAIN_WK, len(weeks)):
            a = widx[weeks[wi]][0]
            if a == 0: continue
            pe = a - 1; best = None
            for i in masks:
                cn, cr, ch = cum[i]; n = cn[pe]
                if n < MIN_SIG: continue
                E = cr[pe] / n
                if best is None or E > best[0]: best = (E, i)
            if best is None: continue
            i = best[1]; cfg_count[i] = cfg_count.get(i, 0) + 1
            for j in widx[weeks[wi]]:
                if masks[i][j]:
                    t_n += 1; t_ret += oc[j] - COST; t_hits += 1 if oc[j] >= 0.5 else 0
        t_wr = t_hits / t_n if t_n else np.nan; t_E = t_ret / t_n if t_n else np.nan
        best_cfg = GRID[max(cfg_count, key=cfg_count.get)] if cfg_count else None
        return dict(symbol=sym, u_n=u_n, u_wr=u_wr, u_E=u_E, t_n=t_n, t_wr=t_wr, t_E=t_E,
                    best=(f"ma={best_cfg['ma']},atr={best_cfg['atr']},z={best_cfg['z']},{best_cfg['mode']}" if best_cfg else ""),
                    yr_first=dates[0][:4], yr_last=dates[-2][:4]), u_rows
    except Exception as e:
        return ("ERR", sym, str(e)[:60]), None


def main():
    con = sqlite3.connect("file:" + DB.replace("\\", "/") + "?mode=ro", uri=True)
    syms = pd.read_sql_query("SELECT symbol, COUNT(*) n FROM ohlc_daily GROUP BY symbol HAVING n>=400 ORDER BY n DESC",
                             con).symbol.tolist(); con.close()
    print(f"universe-wide rich-Keltner buy-the-dip  ·  {len(syms)} stocks  ·  universal={UNIVERSAL}  ·  net cost {COST}%", flush=True)
    rows = []; daily = []; errs = 0
    with mp.Pool(max(2, (os.cpu_count() or 4) - 2)) as pool:
        for res in pool.imap_unordered(one_stock, syms, chunksize=4):
            if res is None: continue
            meta, urows = res
            if isinstance(meta, tuple) and meta and meta[0] == "ERR": errs += 1; continue
            if meta is None: continue
            rows.append(meta)
            if urows: daily.extend(urows)
            if len(rows) % 50 == 0: print(f"  ...{len(rows)} stocks done", flush=True)
    SC = pd.DataFrame(rows)
    DPL = pd.DataFrame(daily, columns=["date", "net"])
    print(f"\nDONE: {len(SC)} stocks, {errs} errors, {len(DPL):,} universal buy signals total")

    # ---- universal-setting universe summary ----
    valid = SC[SC.u_n >= 10]
    print("\n===== (a) UNIVERSAL setting  ma=20/atr=10/z=2.5 across the universe =====")
    print(f"  stocks w/ >=10 signals: {len(valid)}   ·   total signals: {int(SC.u_n.sum()):,}")
    print(f"  mean per-stock WR: {valid.u_wr.mean()*100:.1f}%   ·   mean per-stock E[net]: {valid.u_E.mean():+.3f}%")
    print(f"  stocks with positive E[net]: {int((valid.u_E>0).sum())}/{len(valid)} ({(valid.u_E>0).mean()*100:.0f}%)")
    print(f"  stocks with WR>=55%: {int((valid.u_wr>=0.55).sum())}   ·   WR>=60%: {int((valid.u_wr>=0.60).sum())}")

    # ---- per-stock DNA tuning summary ----
    vt = SC[SC.t_n >= 10]
    print("\n===== (b) PER-STOCK DNA tuning (leak-free walk-forward) =====")
    print(f"  mean per-stock OOS WR: {vt.t_wr.mean()*100:.1f}%   ·   mean OOS E[net]: {vt.t_E.mean():+.3f}%")
    print(f"  stocks with positive OOS E: {int((vt.t_E>0).sum())}/{len(vt)} ({(vt.t_E>0).mean()*100:.0f}%)")

    # ---- daily P&L (universal, Rs 1,00,000/signal) ----
    DPL["pnl"] = DPL.net / 100 * 100000
    day = DPL.groupby("date").agg(n=("net", "size"), pnl=("pnl", "sum"), wr=("net", lambda x: (x > 0).mean())).reset_index()
    day["yr"] = day.date.str[:4]; day["cum"] = day.pnl.cumsum()
    total = day.pnl.sum(); avg_day = day.pnl.mean()
    print("\n===== DAILY P&L — universal buy-the-dip, Rs 1,00,000/signal, net =====")
    print(f"  total signals {len(DPL):,}  ·  overall WR {(DPL.net>0).mean()*100:.1f}%  ·  avg net/trade Rs {DPL.net.mean()/100*100000:+,.0f} ({DPL.net.mean():+.3f}%)")
    print(f"  total P&L Rs {total:,.0f}  ·  trading days {len(day)}  ·  avg/day Rs {avg_day:+,.0f}")
    print("\n  yearly:")
    yr = day.groupby("yr").agg(signals=("n", "sum"), pnl=("pnl", "sum"), days=("date", "size")).reset_index()
    for _, r in yr.iterrows(): print(f"    {r.yr}: signals {int(r.signals):>6}  P&L Rs {r.pnl:>+14,.0f}")

    print("\n----- top 12 stocks by UNIVERSAL Keltner buy WR (>=15 signals) -----")
    top = valid[valid.u_n >= 15].sort_values("u_wr", ascending=False).head(12)
    print(f"  {'symbol':<12}{'n':>5}{'WR':>7}{'E_net':>8}{'DNA best config':>32}")
    for _, r in top.iterrows(): print(f"  {r.symbol:<12}{r.u_n:>5}{r.u_wr*100:>6.1f}%{r.u_E:>+8.2f}   {r.best}")

    xls = os.path.join(os.path.expanduser("~"), "Downloads", "KELTNER_UNIVERSE.xlsx")
    with pd.ExcelWriter(xls, engine="openpyxl") as w:
        SC.sort_values("u_wr", ascending=False).to_excel(w, "per_stock_scorecard", index=False)
        day.to_excel(w, "daily_pnl", index=False); yr.to_excel(w, "yearly", index=False)
    print(f"\nsaved -> {xls}")


if __name__ == "__main__":
    mp.freeze_support(); main()
