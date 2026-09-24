"""BB drift — base + 3 variants. F&O universe, daily, signals Jan-2024..current.
Base : 2 upper-band walk closes -> long next open; target +4%; stop = close < 2nd-walk close.
A    : enter on FIRST upper-band walk close (less extended).
B    : 2 walks, but stop = entry - 2*ATR (wider) instead of confirmation-candle close.
C    : MIRROR SHORT — sweep UPPER band + recover, 2 lower-band walk closes (small lower shadows, vol up, VWAP down,
       ATR up) -> short next open; target +4% (price -4%); stop = close > 2nd-walk close.
Cost 0.15% round-trip. Read-only."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); UEDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
CAP = 5e5; COST = 0.0015; TGT = 0.04
fc = sqlite3.connect("file:" + UEDB.replace("\\", "/") + "?mode=ro", uri=True)
FO = [s for (s,) in fc.execute("SELECT DISTINCT symbol FROM fo_stock_master WHERE fo_eligible=1")
      if s not in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50")]; fc.close()
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
DATA = {}
for sym in FO:
    d = pd.read_sql_query("SELECT trade_date,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? AND trade_date BETWEEN '2023-06-01' AND '2026-07-31' ORDER BY trade_date", uc, params=(sym,))
    if len(d) < 60: continue
    d = d.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}).reset_index(drop=True)
    c = d.Close; sd = c.rolling(20).std(); d["sma"] = c.rolling(20).mean(); d["upper"] = d.sma+2*sd; d["lower"] = d.sma-2*sd
    tp = (d.High+d.Low+d.Close)/3; d["vwap"] = (tp*d.Volume).rolling(20).sum()/d.Volume.rolling(20).sum()
    pc = c.shift(1); tr = pd.concat([d.High-d.Low, (d.High-pc).abs(), (d.Low-pc).abs()], axis=1).max(axis=1); d["atr"] = tr.rolling(14).mean()
    DATA[sym] = d
uc.close()

def run(direction, first_walk, atr_mult):
    trades = []
    for sym, d in DATA.items():
        O, H, L, C = d.Open.values, d.High.values, d.Low.values, d.Close.values
        up, lo, vw, at, vol = d.upper.values, d.lower.values, d.vwap.values, d.atr.values, d.Volume.values
        dt = d.trade_date.values; n = len(d)
        body = np.abs(C - O); ush = H - np.maximum(O, C); lsh = np.minimum(O, C) - L
        def small_u(i): return body[i] > 0 and ush[i] < 0.5*body[i]
        def small_l(i): return body[i] > 0 and lsh[i] < 0.5*body[i]
        for t in range(25, n-1):
            if direction == 1:
                walk = C[t] >= up[t] and (first_walk or C[t-1] >= up[t-1]); shad = small_u(t) and (first_walk or small_u(t-1))
                sweep = any(L[k] < lo[k] and C[k] >= lo[k] for k in range(max(0, t-8), t-1)); vwok = vw[t] > vw[t-1]
            else:
                walk = C[t] <= lo[t] and (first_walk or C[t-1] <= lo[t-1]); shad = small_l(t) and (first_walk or small_l(t-1))
                sweep = any(H[k] > up[k] and C[k] <= up[k] for k in range(max(0, t-8), t-1)); vwok = vw[t] < vw[t-1]
            volok = first_walk or (vol[t] >= vol[t-1])
            if not (walk and shad and volok and vwok and at[t] > at[t-1] and sweep): continue
            ed = t+1
            if dt[ed] < "2024-01-01": continue
            entry = O[ed]
            if direction == 1:
                tgt = entry*(1+TGT); stop = C[t] if atr_mult is None else entry - atr_mult*at[t]
            else:
                tgt = entry*(1-TGT); stop = C[t] if atr_mult is None else entry + atr_mult*at[t]
            ret = reason = xi = None
            for j in range(ed, n):
                if (direction == 1 and H[j] >= tgt) or (direction == -1 and L[j] <= tgt): ret = TGT; reason = "target"; xi = j; break
                if (direction == 1 and C[j] < stop) or (direction == -1 and C[j] > stop): ret = direction*(C[j]-entry)/entry; reason = "stop"; xi = j; break
            if ret is None: ret = direction*(C[n-1]-entry)/entry; reason = "open_end"; xi = n-1
            trades.append(dict(stock=sym, entry_date=dt[ed], net=(ret-COST)*100, reason=reason, hold=xi-ed+1, year=dt[ed][:4]))
    return pd.DataFrame(trades)

def report(name, T):
    if T.empty: print(f"{name:<26} no signals"); return
    w = (T.net > 0).mean()*100; pf = T.net[T.net > 0].sum()/-T.net[T.net < 0].sum() if (T.net < 0).any() else np.inf
    T2 = T.sort_values("entry_date"); eq = CAP; peak = CAP; mdd = 0; free = "0000"; took = 0
    for _, r in T2.iterrows():
        if r.entry_date >= free: eq *= (1+r.net/100); peak = max(peak, eq); mdd = max(mdd, (peak-eq)/peak); free = r.entry_date; took += 1
    tgt = (T.reason == "target").mean()*100
    print(f"{name:<26} n={len(T):<4} win%={w:5.1f}  avg={T.net.mean():+.2f}%  PF={pf:4.2f}  tgt%={tgt:3.0f}  avgHold={T.hold.mean():4.1f}d  |  Rs5L ROC={eq/CAP*100-100:+6.1f}%  maxDD={mdd*100:3.0f}%")

print("BB drift — base + 3 variants (F&O, 1D, Jan-2024..Jul-2026, Rs5L one-at-a-time)\n" + "="*118)
report("BASE (2 walks, tight stop)", run(1, False, None))
report("A: enter on 1st walk", run(1, True, None))
report("B: 2 walks, 2xATR stop", run(1, False, 2.0))
report("C: MIRROR SHORT", run(-1, False, None))
print("="*118)
