"""FALCON VIRTUAL TRADING ARENA — v1. Adds: Nifty-500 universe (universe_master.in_nifty500), margin/ruin
floor (Spec §6.2), and Falcon basket-agents (§8.4/§9): Magnifier (MIS 5x, basket trail) + BTST (CNC, 2-session).
Per-stock agents (Momentum/MeanRev/ORB) + basket agents compete on the SAME point-in-time replay + T+1 fill +
cost/slippage/participation. Falcon daily baskets are precomputed (Top-15 high-tier) — read-only. """
import os, sqlite3, importlib.util, math
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
SP = os.path.join(ROOT, "..", "..", "AppData", "Local", "Temp", "claude")  # placeholder; picks loaded below
PICKS = r"C:\Users\SPS\AppData\Local\Temp\claude\C--Users-SPS-Desktop-Kanida-ai-Terminal-Quant-Intelligence-Engine\c73fe1ef-c928-428e-a17b-d7f23047b24b\scratchpad\pos_picks.pkl"
spec = importlib.util.spec_from_file_location("fpe", os.path.join(ROOT, "scripts", "flow_paper_engine.py")); fpe = importlib.util.module_from_spec(spec); spec.loader.exec_module(fpe)
HIT = ["ENTERPRISE-Dryup", "GOLD", "GOLD-baseline", "PREMIUM-Compression", "PREMIUM-Pullback"]
SLICE0, SLICE1 = "2026-05-01", "2026-05-29"
CAPITAL, PARTICIPATION, BASE_SLIP_BPS, IMPACT_K = 1_000_000.0, 0.02, 2.0, 800.0
MIS_CUT, SESS_END, RUIN = "15:10", "15:29", 0.30      # ruin floor = 30% of start (Spec §A10)
MAX_POS, POS_FRAC = 5, 0.20
CACHE = os.path.join(ROOT, "arena", "bars_n500_may26.pkl")
def load_universe_and_bars():
    if os.path.exists(CACHE):
        import pickle
        with open(CACHE, "rb") as f: return pickle.load(f)
    oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    n500 = [r[0] for r in oc.execute("SELECT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1")]
    have = set(r[0] for r in oc.execute("SELECT DISTINCT symbol FROM ohlc_1min WHERE substr(bar_time,1,10)='2026-05-15'"))
    universe = sorted(set(n500) & have)
    bars = {}
    for s in universe:
        df = pd.read_sql_query("SELECT substr(bar_time,1,10) d, substr(bar_time,12,5) hm, open,high,low,close,volume FROM ohlc_1min WHERE symbol=? AND substr(bar_time,1,10) BETWEEN ? AND ? ORDER BY bar_time", oc, params=(s, SLICE0, SLICE1))
        for d, g in df.groupby("d"):
            g = g.reset_index(drop=True); g["cumv"] = g.volume.cumsum(); g["cumpv"] = (g.close * g.volume).cumsum()
            g["vwap"] = g.cumpv / g.cumv.replace(0, np.nan); g["mom5"] = g.close.pct_change(5) * 100
            g["vavg20"] = g.volume.rolling(20, 5).mean(); g["orh15"] = g[g.hm <= "09:30"].high.max()
            bars[(s, d)] = g.set_index("hm")
    oc.close()
    import pickle
    with open(CACHE, "wb") as f: pickle.dump((universe, bars), f)
    return universe, bars
def falcon_baskets():
    P = pd.read_pickle(PICKS)
    B = P[(P["rank"] <= 15) & P.tier.isin(HIT) & (P.entry_date >= SLICE0) & (P.entry_date <= SLICE1)]
    return {d: list(sub.symbol) for d, sub in B.groupby("entry_date")}
def fillpx(side, ref, oval, mval):
    slip = (BASE_SLIP_BPS + IMPACT_K * min(oval, PARTICIPATION * mval) / max(mval, 1)) / 1e4
    return ref * (1 + slip) if side == "BUY" else ref * (1 - slip)
def rt_cost(e, x, q):
    try: return fpe.cost("CASH", e, x, q)
    except Exception: return 0.0006 * (e + x) * q
# ---------- agents ----------
class Agent:
    is_basket = False
    def __init__(self, aid, product="MIS", lev=5):
        self.id = aid; self.product = product; self.lev = lev; self.cash = CAPITAL
        self.pos = {}; self.trades = []; self.eq = []; self.dead = False
    def equity(self, active, hm):
        un = 0.0
        for s, p in self.pos.items():
            g = active.get(s)
            if g is not None and hm in g.index: un += (g.loc[hm].close - p["entry"]) * p["qty"]
        return self.cash + un
class PerStock(Agent):
    def sizeq(self, price): return max(int(POS_FRAC * CAPITAL * self.lev / price), 0)
    def decide(self, sym, row, prev): return None
    def manage(self, sym, row):
        p = self.pos.get(sym)
        if not p: return None
        if row.name >= MIS_CUT: return ("EXIT", row.close, "EOD")
        if row.low <= p["stop"]: return ("EXIT", p["stop"], "STOP")
        if row.high >= p["target"]: return ("EXIT", p["target"], "TARGET")
        return None
    def tgtstop(self, px): return px * 1.010, px * 0.994
class RetailMomentum(PerStock):
    def decide(self, s, r, pv):
        if s in self.pos or len(self.pos) >= MAX_POS: return None
        if pd.notna(r.vwap) and r.close > r.vwap and (r.mom5 or 0) > 0.3 and (r.vavg20 or 0) > 0 and r.volume > 1.2 * r.vavg20: return "BUY"
class MeanReversion(PerStock):
    def tgtstop(self, px): return px * 1.012, px * 0.990
    def decide(self, s, r, pv):
        if s in self.pos or len(self.pos) >= MAX_POS: return None
        if pd.notna(r.vwap) and (r.close / r.vwap - 1) * 100 < -1.5 and pv is not None and r.close >= pv.close: return "BUY"
class ORB(PerStock):
    def tgtstop(self, px): return px * 1.015, px * 0.990
    def decide(self, s, r, pv):
        if s in self.pos or len(self.pos) >= MAX_POS: return None
        if r.name > "09:30" and pd.notna(r.orh15) and r.close > r.orh15 and (r.vavg20 or 0) > 0 and r.volume > 1.5 * r.vavg20: return "BUY"
class BasketAgent(Agent):
    is_basket = True
    def __init__(self, aid, baskets, product, lev, trail=None, hold_days=1, hard_stop=6):
        super().__init__(aid, product, lev); self.baskets = baskets; self.trail = trail
        self.hold_days = hold_days; self.hard = hard_stop; self.entry = {}; self.peak = 0.0; self.entered_di = None
    def enter(self, active, hm, day):
        names = [s for s in self.baskets.get(day, []) if (s, day) in active and hm in active[(s)].index] if False else self.baskets.get(day, [])
        alloc = CAPITAL * self.lev / max(len([s for s in names if s in active]), 1)  # split basket over n filled names (s-keyed active)
        for s in names:
            g = active.get(s)
            if g is None or hm not in g.index: continue
            op = g.loc[hm].open; mval = g.loc[hm].close * g.loc[hm].volume; q = int(alloc / op)
            if q <= 0: continue
            fp = fillpx("BUY", op, q * op, mval); self.pos[s] = dict(qty=q, entry=fp); self.entry[s] = fp
        # GUARD: a basket may never deploy more than CAPITAL*lev of gross notional (catches the n-count sizing bug)
        gross = sum(p["qty"] * p["entry"] for p in self.pos.values())
        assert gross <= CAPITAL * self.lev * 1.01, f"{self.id} over-leveraged: gross {gross:,.0f} > {CAPITAL*self.lev:,.0f} ({gross/CAPITAL:.1f}x cap {self.lev}x)"
        self.peak = 0.0
    def basket_pnl(self, active, hm):
        rs = [(active[s].loc[hm].close / p["entry"] - 1) for s, p in self.pos.items() if (s in active and hm in active[s].index)]
        return np.mean(rs) * 100 * self.lev if rs else 0.0
    def exit_all(self, active, hm, why):
        for s, p in list(self.pos.items()):
            g = active.get(s); px = g.loc[hm].close if (g is not None and hm in g.index) else p["entry"]
            gross = (px - p["entry"]) * p["qty"]; c = rt_cost(p["entry"], px, p["qty"]); notl = p["qty"] * p["entry"]
            self.cash += gross - c
            self.trades.append(dict(sym=s, pnl=gross - c, gross=gross, cost=c, why=why, notl=notl, day=getattr(self, "_cur_day", None), exit_hm=hm,
                                    gross_pct=gross / notl * 100 if notl else 0, cost_pct=c / notl * 100 if notl else 0))
        self.pos.clear(); self.entry.clear()
# ---------- replay ----------
def run(universe, bars, agents):
    days = sorted(set(d for (_, d) in bars)); di = {d: i for i, d in enumerate(days)}
    mins = [f"{9 + (m // 60):02d}:{m % 60:02d}" for m in range(15, 15 + 6 * 60 + 15)]; mins = [x for x in mins if "09:15" <= x <= SESS_END]
    for d in days:
        print(f"[replay] day {di[d]+1}/{len(days)} {d} ({(di[d]+1)/len(days)*100:.0f}%)", flush=True)
        active = {s: bars[(s, d)] for s in universe if (s, d) in bars}
        for ag in agents:
            if not ag.is_basket or ag.product == "MIS": ag.pos.clear()   # MIS/per-stock reset daily; CNC carries
        prev = {}
        for i, hm in enumerate(mins):
            nxt = mins[i + 1] if i + 1 < len(mins) else None
            for ag in agents:
                if ag.dead: continue
                if ag.is_basket:
                    if not ag.pos and hm == "09:16" and d in ag.baskets:
                        ag.enter(active, hm, d); ag.entered_di = di[d]
                    elif ag.pos:
                        pnl = ag.basket_pnl(active, hm)
                        if ag.trail:
                            A, F, G, S = ag.trail; armed = ag.peak >= A; stop = max(F, ag.peak - G) if armed else -S
                            if pnl <= stop: ag.exit_all(active, hm, "TRAIL")
                            elif ag.product == "MIS" and hm >= SESS_END: ag.exit_all(active, hm, "EOD")
                            else: ag.peak = max(ag.peak, pnl)
                        else:  # BTST: hold hold_days sessions, -hard% stop
                            if pnl <= -ag.hard: ag.exit_all(active, hm, "STOP")
                            elif di[d] >= ag.entered_di + (ag.hold_days - 1) and hm >= SESS_END: ag.exit_all(active, hm, "EOD")
                    continue
                # per-stock
                for s in list(ag.pos):
                    g = active.get(s)
                    if g is None or hm not in g.index: continue
                    act = ag.manage(s, g.loc[hm])
                    if act:
                        _, px, why = act; p = ag.pos.pop(s); c = rt_cost(p["entry"], px, p["qty"])
                        ag.cash += (px - p["entry"]) * p["qty"] - c; ag.trades.append(dict(sym=s, pnl=(px - p["entry"]) * p["qty"] - c, why=why))
                if nxt is None: continue
                for s, g in active.items():
                    if hm not in g.index or nxt not in g.index: continue
                    if ag.decide(s, g.loc[hm], prev.get(s)) == "BUY":
                        nb = g.loc[nxt]; mval = nb.close * nb.volume; q = ag.sizeq(nb.open)
                        if q <= 0: continue
                        if q * nb.open > PARTICIPATION * mval: q = int(PARTICIPATION * mval / nb.open)
                        if q <= 0: continue
                        fp = fillpx("BUY", nb.open, q * nb.open, mval); tgt, stp = ag.tgtstop(fp)
                        ag.pos[s] = dict(qty=q, entry=fp, stop=stp, target=tgt)
            for s, g in active.items():
                if hm in g.index: prev[s] = g.loc[hm]
        for ag in agents:
            eq = ag.equity(active, SESS_END); ag.eq.append((d, eq))
            if eq <= RUIN * CAPITAL and not ag.dead: ag.dead = True   # margin ruin: stop trading
def metrics(ag):
    eq = pd.Series([e for _, e in ag.eq]); ret = (eq.iloc[-1] / CAPITAL - 1) * 100
    peak = eq.cummax(); dd = ((peak - eq) / peak * 100).max(); dret = eq.pct_change().dropna()
    sh = dret.mean() / (dret.std() + 1e-9) * math.sqrt(252) if len(dret) > 1 else 0
    t = pd.DataFrame(ag.trades); wr = (t.pnl > 0).mean() * 100 if len(t) else 0
    return dict(agent=ag.id, ret=ret, dd=dd, sharpe=sh, wr=wr, trades=len(t), net=eq.iloc[-1] - CAPITAL, dead="RUIN" if ag.dead else "")
if __name__ == "__main__":
    print(f"loading Nifty-500 universe + bars ({SLICE0}..{SLICE1})...")
    universe, bars = load_universe_and_bars(); fb = falcon_baskets()
    print(f"universe: {len(universe)} Nifty-500 names | day-bars: {len(bars)} | Falcon basket-days: {len(fb)} (avg {np.mean([len(v) for v in fb.values()]):.1f} names)")
    agents = [RetailMomentum("Retail-Momentum"), MeanReversion("Mean-Reversion"), ORB("ORB-Breakout"),
              BasketAgent("Falcon-Magnifier", fb, "MIS", 5, trail=(6, 2, 5, 3)),
              BasketAgent("Falcon-BTST", fb, "CNC", 1, trail=None, hold_days=2, hard_stop=6)]
    run(universe, bars, agents)
    lb = pd.DataFrame([metrics(a) for a in agents]).sort_values("ret", ascending=False)
    print("\n===== ARENA v1 LEADERBOARD (Nifty-500, ₹10L, T+1 fill + full friction + margin floor) =====")
    print(f"{'rank':<5}{'agent':<20}{'net_ret%':>9}{'maxDD%':>8}{'sharpe':>8}{'win%':>7}{'trades':>8}{'net_pnl':>12}{'  flag'}")
    for i, (_, r) in enumerate(lb.iterrows(), 1):
        print(f"{i:<5}{r.agent:<20}{r.ret:>+9.1f}{r.dd:>8.1f}{r.sharpe:>8.2f}{r.wr:>7.1f}{r.trades:>8.0f}{r.net:>+12,.0f}  {r.dead}")
