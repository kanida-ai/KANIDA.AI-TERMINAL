# -*- coding: utf-8 -*-
"""THE RING — read Nifty-50 as one weighted ecosystem. weight x return = contribution; classify each
stock's live STATE vs its own baseline+continuation curve; produce Buy/Short candidates; FORWARD-VALIDATE
on a real day (do candidates flagged at time T outperform for the rest of the day?). 5-min replay."""
import os, sqlite3, sys
import numpy as np, pandas as pd
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
DDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
# approximate published free-float Nifty-50 weights (%), early 2026 — heavyweight ordering is the point
W = {"HDFCBANK":13.3,"ICICIBANK":8.7,"RELIANCE":8.0,"INFY":4.8,"BHARTIARTL":4.4,"LT":4.0,"ITC":3.8,"TCS":3.6,
 "AXISBANK":3.1,"KOTAKBANK":2.8,"SBIN":2.8,"HINDUNILVR":2.2,"BAJFINANCE":2.2,"M&M":2.1,"MARUTI":1.9,"SUNPHARMA":1.8,
 "NTPC":1.7,"HCLTECH":1.7,"TITAN":1.5,"ULTRACEMCO":1.4,"POWERGRID":1.4,"ASIANPAINT":1.3,"ADANIENT":1.2,"COALINDIA":1.2,
 "BAJAJFINSV":1.2,"ONGC":1.1,"NESTLEIND":1.1,"WIPRO":1.0,"JSWSTEEL":1.0,"TATASTEEL":1.0,"ADANIPORTS":1.0,"TRENT":0.9,
 "GRASIM":0.9,"HDFCLIFE":0.8,"SBILIFE":0.8,"TECHM":0.8,"CIPLA":0.8,"DRREDDY":0.8,"BRITANNIA":0.7,"EICHERMOT":0.7,
 "HINDALCO":0.7,"BPCL":0.6,"TATACONSUM":0.6,"HEROMOTOCO":0.6,"APOLLOHOSP":0.6,"BAJAJ-AUTO":0.6,"INDUSINDBK":0.6,"LICI":0.4}


def baselines(syms):
    con = sqlite3.connect("file:" + DDB.replace("\\", "/") + "?mode=ro", uri=True)
    out = {}
    for s in syms:
        g = pd.read_sql_query("SELECT open,high,low,close FROM ohlc_daily WHERE symbol=? AND trade_date>='2022-01-01' ORDER BY trade_date", con, params=[s])
        if len(g) < 250: continue
        o, h, l, c = g.open.values, g.high.values, g.low.values, g.close.values
        oh = (h/o-1)*100; ol = (l/o-1)*100; up05 = oh >= 0.5; dn05 = ol <= -0.5
        out[s] = dict(rng=((h-l)/o*100).mean(),
                      cu10=(oh >= 1.0).sum()/max(up05.sum(), 1), cd10=(ol <= -1.0).sum()/max(dn05.sum(), 1),
                      reten=((c[oh >= 1] > o[oh >= 1]).mean()+(c[ol <= -1] < o[ol <= -1]).mean())/2)
    con.close(); return out


def load_day(syms, day):
    con = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
    q = "SELECT symbol,bar_time,open,high,low,close FROM ohlc_1min WHERE bar_time LIKE ? AND symbol IN (%s) ORDER BY bar_time" % ",".join("?"*len(syms))
    b = pd.read_sql_query(q, con, params=[day+"%"]+syms); con.close()
    b["hm"] = b.bar_time.str[11:16]; return b[(b.hm >= "09:15") & (b.hm <= "15:29")]


def state(mv, accel, ruse, direction):
    if abs(mv) < 0.25: return "NEUTRAL", "⚪"
    if direction*accel > 0.02:
        return ("IGNITING", "🟢") if abs(mv) < 0.75 else ("TRENDING", "🟢")
    if direction*accel < -0.03:
        return ("EXHAUSTING", "🟡") if ruse > 0.65 else ("REVERSING", "🔴")
    return ("TRENDING", "🟢") if ruse < 0.7 else ("EXHAUSTING", "🟡")


def board(b, base, ts):
    rows = []
    for s, g in b.groupby("symbol"):
        g = g[g.hm <= ts]
        if len(g) < 3 or s not in base: continue
        o = g.open.iloc[0]; px = g.close.iloc[-1]; mv = (px/o-1)*100
        prev = (g.close.iloc[-3]/o-1)*100; accel = mv-prev
        bs = base[s]; ruse = abs(mv)/max(bs["rng"], 0.1); d = np.sign(mv)
        cont = (bs["cu10"] if d > 0 else bs["cd10"])*100
        st, ic = state(mv, accel, ruse, d)
        rows.append(dict(stock=s, wt=W.get(s, 0.3), mv=round(mv, 3), contrib=round(mv*W.get(s, 0.3)/100, 4),
                         accel=round(accel, 3), state=st, icon=ic, cont=round(cont), room=round(max(0, 1-ruse), 2), px=px))
    return pd.DataFrame(rows)


def run(day="2026-01-20", checkpoints=("09:45", "10:30", "11:30", "13:00")):
    syms = [s for s in W]
    base = baselines(syms); b = load_day([s for s in syms if s in base], day)
    close_px = b.sort_values("bar_time").groupby("symbol").close.last().to_dict()
    print("="*96); print(f"THE RING — NIFTY-50 index-ecosystem replay  ·  {day}  ·  (weights approximate)"); print("="*96)
    valrows = []
    for ts in checkpoints:
        B = board(b, base, ts)
        idx = B.contrib.sum()
        up = B[B.mv > 0].sort_values("contrib", ascending=False); dn = B[B.mv < 0].sort_values("contrib")
        # candidates: aligned with index direction, igniting/trending, room, high continuation
        buys = B[(B.mv > 0) & (B.state.isin(["IGNITING", "TRENDING"])) & (B.room > 0.25) & (B.cont >= 60)].sort_values(["cont", "wt"], ascending=False)
        shorts = B[(B.mv < 0) & (B.state.isin(["IGNITING", "TRENDING"])) & (B.room > 0.25) & (B.cont >= 60)].sort_values(["cont", "wt"], ascending=False)
        print(f"\n{'─'*96}\n⏱  {ts}   reconstructed index move (Σ contributions): {idx*100:+.2f}%")
        print("  TOP DRIVERS ↑: " + " · ".join(f"{r.stock}{r.icon}{r.mv:+.1f}%(w{r.wt})" for _, r in up.head(4).iterrows()))
        print("  TOP DRAGS   ↓: " + " · ".join(f"{r.stock}{r.icon}{r.mv:+.1f}%(w{r.wt})" for _, r in dn.head(4).iterrows()))
        print(f"  🟢 BUY candidates ({len(buys)}): " + " · ".join(f"{r.stock}(cont{r.cont}%,room{r.room})" for _, r in buys.head(5).iterrows()))
        print(f"  🔴 SHORT candidates ({len(shorts)}): " + " · ".join(f"{r.stock}(cont{r.cont}%,room{r.room})" for _, r in shorts.head(5).iterrows()))
        # forward validation: from ts to close
        def fwd(df):
            r = [(close_px[x.stock]/x.px-1)*100*np.sign(x.mv) for _, x in df.iterrows() if x.stock in close_px]
            return np.mean(r) if r else np.nan
        fb, fs = fwd(buys), fwd(shorts); fall = fwd(B.assign(mv=1))  # all long-benchmark
        valrows.append(dict(ts=ts, n_buy=len(buys), n_short=len(shorts), buy_fwd=fb, short_fwd=fs))
        print(f"  ▶ FORWARD (ts→close, direction-adjusted):  BUY cands {fb:+.2f}%   SHORT cands {fs:+.2f}%   (edge = did they continue?)")
    V = pd.DataFrame(valrows)
    print(f"\n{'='*96}\nVALIDATION SUMMARY ({day}) — avg forward move of flagged candidates, direction-adjusted:")
    print(f"  BUY candidates:   {V.buy_fwd.mean():+.2f}%   ·   SHORT candidates: {V.short_fwd.mean():+.2f}%")
    print(f"  (positive = the state-board correctly predicted continuation for the rest of the day)")
    return V


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "2026-01-20")
