# -*- coding: utf-8 -*-
"""Trend-matched rich-Keltner across the REAL Nifty-500 (universe_master.in_nifty500=1).
The insight from ICICI (uptrend->buy dip) + JIOFIN (downtrend->short rip): match the mean-reversion
DIRECTION to the primary trend. Compare head-to-head on the SAME universe:
  (A) BLIND buy-the-dip  — long on lower-band breach regardless of trend (the failing approach)
  (B) TREND-MATCHED      — buy dip only in UPtrend, short rip only in DOWNtrend, skip neutral
Trend (PIT, leak-free): UP = close>SMA200 & SMA50>SMA200 ; DOWN = close<SMA200 & SMA50<SMA200.
Keltner ma=20 / atr=10 / z=2.5 breach. Outcome = next-day OPEN->CLOSE, MIS intraday, net of cost.
"""
import os, sqlite3, multiprocessing as mp
import numpy as np, pandas as pd
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
COST = 0.06; MA, ATR_N, Z = 20, 10, 2.5


def _ema(s, n): return pd.Series(s).ewm(span=n, adjust=False).mean().values
def _sma(s, n): return pd.Series(s).rolling(n).mean().values
def _rma(s, n): return pd.Series(s).ewm(alpha=1/n, adjust=False).mean().values
def _atr(h, l, c, n):
    pc = np.roll(c, 1); pc[0] = c[0]
    return _rma(np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc))), n)


def one_stock(sym):
    try:
        con = sqlite3.connect("file:" + DB.replace("\\", "/") + "?mode=ro", uri=True)
        g = pd.read_sql_query("SELECT trade_date,open,high,low,close FROM ohlc_daily WHERE symbol=? ORDER BY trade_date",
                              con, params=[sym]); con.close()
        if len(g) < 300: return None
        o, h, l, c = (g[x].values.astype(float) for x in ("open", "high", "low", "close"))
        o1 = np.roll(o, -1); c1 = np.roll(c, -1); oc = (c1 - o1) / o1 * 100.0; oc[-1] = np.nan
        dates = g.trade_date.values; valid = ~np.isnan(oc)
        sma50, sma200 = _sma(c, 50), _sma(c, 200)
        # structural trend (SMA50 vs SMA200) — a single deep dip must NOT flip the regime
        up = sma50 > sma200; down = sma50 < sma200
        ema = _ema(c, MA); a = _atr(h, l, c, ATR_N); st = (c - ema) / (a + 1e-9)
        buy = (st <= -Z) & valid & ~np.isnan(st); sell = (st >= Z) & valid & ~np.isnan(st)
        # (A) blind buy-the-dip
        A = buy
        A_ret = oc[A] - COST
        # (B) trend-matched: long buy-dip in uptrend; short sell-rip in downtrend
        long_m = buy & up; short_m = sell & down
        B_long_ret = oc[long_m] - COST; B_short_ret = -oc[short_m] - COST
        B_rows = list(zip(dates[long_m], np.round(B_long_ret, 3), ["L"] * int(long_m.sum()))) + \
                 list(zip(dates[short_m], np.round(B_short_ret, 3), ["S"] * int(short_m.sum())))
        def stat(ret, hitfrac):
            n = len(ret); return (n, float(hitfrac) if n else np.nan, float(ret.mean()) if n else np.nan)
        return dict(
            symbol=sym,
            A_n=int(A.sum()), A_wr=float((oc[A] >= 0.5).mean()) if A.sum() else np.nan, A_E=float(A_ret.mean()) if A.sum() else np.nan,
            Bn_long=int(long_m.sum()), Bn_short=int(short_m.sum()),
            B_n=int(long_m.sum() + short_m.sum()),
            B_wr=float((np.concatenate([oc[long_m] >= 0.5, -oc[short_m] >= 0.5])).mean()) if (long_m.sum() + short_m.sum()) else np.nan,
            B_E=float(np.concatenate([B_long_ret, B_short_ret]).mean()) if (long_m.sum() + short_m.sum()) else np.nan,
            trend_up_days=int(up.sum()), trend_dn_days=int(down.sum())), B_rows
    except Exception as e:
        return ("ERR", sym, str(e)[:50]), None


def main():
    con = sqlite3.connect("file:" + DB.replace("\\", "/") + "?mode=ro", uri=True)
    syms = pd.read_sql_query("SELECT DISTINCT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1",
                             con).symbol.tolist(); con.close()
    print(f"REAL Nifty-500 trend-matched Keltner  ·  {len(syms)} symbols  ·  ma={MA}/atr={ATR_N}/z={Z}  ·  net {COST}%", flush=True)
    rows = []; brows = []; errs = 0
    with mp.Pool(max(2, (os.cpu_count() or 4) - 2)) as pool:
        for res in pool.imap_unordered(one_stock, syms, chunksize=4):
            if res is None: continue
            meta, br = res
            if isinstance(meta, tuple) and meta and meta[0] == "ERR": errs += 1; continue
            rows.append(meta)
            if br: brows.extend(br)
            if len(rows) % 50 == 0: print(f"  ...{len(rows)} done", flush=True)
    SC = pd.DataFrame(rows)
    print(f"\nDONE: {len(SC)} stocks with history, {errs} errors")

    A = SC[SC.A_n >= 10]; B = SC[SC.B_n >= 10]
    print("\n===== (A) BLIND buy-the-dip (long only, ignore trend) — the failing approach, on REAL Nifty-500 =====")
    print(f"  stocks {len(A)}  ·  total signals {int(SC.A_n.sum()):,}  ·  mean WR {A.A_wr.mean()*100:.1f}%  ·  mean E[net] {A.A_E.mean():+.3f}%  ·  positive {int((A.A_E>0).sum())}/{len(A)} ({(A.A_E>0).mean()*100:.0f}%)")
    print("\n===== (B) TREND-MATCHED (buy dip in uptrend, short rip in downtrend) =====")
    print(f"  stocks {len(B)}  ·  total signals {int(SC.B_n.sum()):,}  (long {int(SC.Bn_long.sum()):,} / short {int(SC.Bn_short.sum()):,})")
    print(f"  mean WR {B.B_wr.mean()*100:.1f}%  ·  mean E[net] {B.B_E.mean():+.3f}%  ·  positive {int((B.B_E>0).sum())}/{len(B)} ({(B.B_E>0).mean()*100:.0f}%)")
    print(f"  stocks WR>=55%: {int((B.B_wr>=0.55).sum())}  ·  WR>=60%: {int((B.B_wr>=0.60).sum())}")

    # daily P&L for (B), Rs 1,00,000/signal
    DP = pd.DataFrame(brows, columns=["date", "net", "side"]); DP["pnl"] = DP.net / 100 * 100000
    day = DP.groupby("date").agg(n=("net", "size"), pnl=("pnl", "sum")).reset_index(); day["yr"] = day.date.str[:4]
    print("\n===== (B) DAILY P&L — trend-matched, Rs 1,00,000/signal, net =====")
    print(f"  total signals {len(DP):,}  ·  overall WR {(DP.net>0).mean()*100:.1f}%  ·  avg net/trade {DP.net.mean():+.3f}% (Rs {DP.net.mean()/100*100000:+,.0f})")
    print(f"  TOTAL P&L Rs {DP.pnl.sum():,.0f}  ·  trading days {len(day)}  ·  avg/day Rs {day.pnl.mean():+,.0f}")
    print("\n  yearly (trend-matched):")
    yr = day.groupby("yr").agg(signals=("n", "sum"), pnl=("pnl", "sum")).reset_index()
    for _, r in yr.iterrows(): print(f"    {r.yr}: signals {int(r.signals):>6}  P&L Rs {r.pnl:>+13,.0f}")
    # side split
    print(f"\n  by side: LONG (buy-dip in uptrend) {(DP.side=='L').sum():,} trades WR {(DP[DP.side=='L'].net>0).mean()*100:.1f}% E {DP[DP.side=='L'].net.mean():+.3f}%  |  "
          f"SHORT (sell-rip in downtrend) {(DP.side=='S').sum():,} trades WR {(DP[DP.side=='S'].net>0).mean()*100:.1f}% E {DP[DP.side=='S'].net.mean():+.3f}%")

    xls = os.path.join(os.path.expanduser("~"), "Downloads", "KELTNER_NIFTY500_TREND.xlsx")
    with pd.ExcelWriter(xls, engine="openpyxl") as w:
        SC.sort_values("B_wr", ascending=False).to_excel(w, "scorecard", index=False); day.to_excel(w, "daily_pnl", index=False); yr.to_excel(w, "yearly", index=False)
    print(f"\nsaved -> {xls}")


if __name__ == "__main__":
    mp.freeze_support(); main()
