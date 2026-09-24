"""Custom arena agent: 'BB drift' (bullish continuation). F&O universe, daily bars, signals Jan-2024 -> current.
Setup (all daily): lower-BB liquidity sweep that recovers in one candle within the last ~6-8 bars, then TWO consecutive
candles CLOSE >= upper BB (walk) with upper shadow < 50% of body, vol(walk2)>=vol(walk1), rolling-VWAP rising, ATR rising
-> BUY next day 09:15 open. Exit: +4% target (via day High) OR daily close < the 2nd walk candle's close (fixed stop).
Reports per-trade edge + a Rs5L one-position-at-a-time account. Cost 0.15% round-trip. Read-only."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
UEDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
CAP = 5e5; COST = 0.0015; TARGET = 0.04
fc = sqlite3.connect("file:" + UEDB.replace("\\", "/") + "?mode=ro", uri=True)
FO = [s for (s,) in fc.execute("SELECT DISTINCT symbol FROM fo_stock_master WHERE fo_eligible=1")
      if s not in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50")]; fc.close()
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)

def indicators(d):
    c = d.Close; d["sma"] = c.rolling(20).mean(); sd = c.rolling(20).std()
    d["upper"] = d.sma + 2*sd; d["lower"] = d.sma - 2*sd
    tp = (d.High + d.Low + d.Close) / 3
    d["vwap"] = (tp*d.Volume).rolling(20).sum() / d.Volume.rolling(20).sum()
    pc = c.shift(1); tr = pd.concat([d.High-d.Low, (d.High-pc).abs(), (d.Low-pc).abs()], axis=1).max(axis=1)
    d["atr"] = tr.rolling(14).mean()
    d["body"] = (d.Close - d.Open).abs()
    d["ushadow"] = d.High - d[["Open", "Close"]].max(axis=1)
    return d

trades = []
for sym in FO:
    d = pd.read_sql_query("SELECT trade_date,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? AND trade_date BETWEEN '2023-06-01' AND '2026-07-31' ORDER BY trade_date", uc, params=(sym,))
    if len(d) < 60: continue
    d = d.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}).reset_index(drop=True)
    d = indicators(d)
    O, H, L, C = d.Open.values, d.High.values, d.Low.values, d.Close.values
    up, lo, vw, at, bd, us, vol = d.upper.values, d.lower.values, d.vwap.values, d.atr.values, d.body.values, d.ushadow.values, d.Volume.values
    dt = d.trade_date.values; n = len(d)
    def small(i): return bd[i] > 0 and us[i] < 0.5 * bd[i]
    for t in range(25, n - 1):
        if not (C[t] >= up[t] and C[t-1] >= up[t-1]): continue            # two upper-band walks
        if not (small(t) and small(t-1)): continue                        # small upper shadows
        if not (vol[t] >= vol[t-1]): continue                             # volume rises
        if not (vw[t] > vw[t-1] and at[t] > at[t-1]): continue            # VWAP + ATR rising
        if not any(L[k] < lo[k] and C[k] >= lo[k] for k in range(max(0, t-8), t-1)): continue  # sweep+recover
        ed = t + 1
        if dt[ed] < "2024-01-01": continue
        entry = O[ed]; stop = C[t]; tgt = entry * (1 + TARGET)
        ret = reason = exit_i = None
        for j in range(ed, n):
            if H[j] >= tgt: ret = TARGET; reason = "target"; exit_i = j; break
            if C[j] < stop: ret = (C[j]-entry)/entry; reason = "stop"; exit_i = j; break
        if ret is None: ret = (C[n-1]-entry)/entry; reason = "open_end"; exit_i = n-1
        trades.append(dict(stock=sym, signal_date=dt[t], entry_date=dt[ed], entry=entry, exit_date=dt[exit_i],
                           gross=ret*100, net=(ret-COST)*100, reason=reason, hold=exit_i-ed+1, year=dt[ed][:4]))
uc.close()
T = pd.DataFrame(trades)
T.to_csv(os.path.join(ROOT, "docs", "reports", "BB_DRIFT_trades.csv"), index=False)
print(f"BB drift — F&O universe, signals Jan-2024..Jul-2026, 1D bars\nTOTAL SIGNALS/TRADES: {len(T)}  across {T.stock.nunique()} stocks\n")
def stats(g, tag):
    w = (g.net > 0).mean()*100; pf = g.net[g.net > 0].sum() / -g.net[g.net < 0].sum() if (g.net < 0).any() else np.inf
    print(f"  {tag:<10} n={len(g):<4} win%={w:5.1f}  avg={g.net.mean():+.2f}%  med={g.net.median():+.2f}  avgWin={g.net[g.net>0].mean():+.2f}  avgLoss={g.net[g.net<0].mean():+.2f}  PF={pf:.2f}  avgHold={g.hold.mean():.1f}d")
stats(T, "ALL")
for y in ["2024", "2025", "2026"]:
    if (T.year == y).any(): stats(T[T.year == y], y)
print(f"\n  exit mix: " + ", ".join(f"{k} {v} ({v/len(T)*100:.0f}%)" for k, v in T.reason.value_counts().items()))
# Rs5L one-position-at-a-time account (chronological)
T2 = T.sort_values("entry_date").reset_index(drop=True); eq = CAP; peak = CAP; mdd = 0; free = "0000"; taken = 0; path = []
for _, r in T2.iterrows():
    if r.entry_date >= free:
        eq *= (1 + r.net/100); peak = max(peak, eq); mdd = max(mdd, (peak-eq)/peak); free = r.exit_date; taken += 1; path.append((r.exit_date, eq))
print(f"\n  Rs5L account (one position at a time): took {taken}/{len(T)} signals | final Rs{eq:,.0f} | ROC {eq/CAP*100-100:+.1f}% | maxDD {mdd*100:.0f}%")
print(f"  Rs5L-per-signal (all trades, non-compounding): total P&L Rs{(T.net/100*CAP).sum():,.0f} | avg Rs{(T.net/100*CAP).mean():,.0f}/trade")
top = T.groupby("stock").net.agg(["count", "sum", "mean"]).sort_values("sum", ascending=False)
print("\n  Top 8 stocks by total net%:")
for s, r in top.head(8).iterrows(): print(f"    {s:<12} trades={int(r['count']):<3} totNet={r['sum']:+.1f}%  avg={r['mean']:+.2f}%")
print(f"\n  -> trades saved: docs/reports/BB_DRIFT_trades.csv")
