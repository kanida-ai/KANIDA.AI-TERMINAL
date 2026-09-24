"""FALCON VIRTUAL TRADING ARENA — v0 correctness core (Spec §3,4,5,6,7,10,11,14).
Single-clock 1-min replay, point-in-time state, T+1-open execution with the Indian cost stack + slippage +
participation cap, MIS 5x margin/EOD square-off, per-agent portfolio, composite leaderboard.
Ships 3 real competing agents (Retail Momentum, Mean-Reversion, Opening-Range Breakout) to prove the engine
end-to-end. Falcon personas + more families plug into the SAME Agent interface next. Anti-lookahead is structural:
agents receive state built ONLY from bars <= current minute; fills use the NEXT minute's open. READ-ONLY on data."""
import os, sqlite3, importlib.util, math
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
spec = importlib.util.spec_from_file_location("fpe", os.path.join(ROOT, "scripts", "flow_paper_engine.py")); fpe = importlib.util.module_from_spec(spec); spec.loader.exec_module(fpe)
# ---------- config ----------
SLICE0, SLICE1 = "2026-05-01", "2026-05-29"
CAPITAL = 1_000_000.0            # ₹10L per agent
LEVERAGE = 5                     # MIS
PARTICIPATION = 0.02            # max 2% of a minute's traded value
BASE_SLIP_BPS, IMPACT_K = 2.0, 800.0
MIS_SQUAREOFF = "15:10"; SESS_END = "15:29"
MAX_POS = 5                     # concurrent positions per agent
POS_FRAC = 0.20                # fraction of capital notional per position (pre-leverage cash)
# ---------- data (point-in-time bars per symbol/day) ----------
def load():
    oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    fno = [x[0] for x in oc.execute("SELECT symbol FROM fo_stock_master WHERE fo_eligible=1")]
    # pick ~40 liquid names by traded value on a sample day
    liq = []
    for s in fno:
        r = oc.execute("SELECT SUM(close*volume) FROM ohlc_1min WHERE symbol=? AND substr(bar_time,1,10)='2026-05-15'", (s,)).fetchone()[0]
        if r: liq.append((s, r))
    liq.sort(key=lambda x: -x[1]); universe = [s for s, _ in liq[:40]]
    bars = {}
    for s in universe:
        df = pd.read_sql_query("SELECT substr(bar_time,1,10) d, substr(bar_time,12,5) hm, open,high,low,close,volume FROM ohlc_1min WHERE symbol=? AND substr(bar_time,1,10) BETWEEN ? AND ? ORDER BY bar_time", oc, params=(s, SLICE0, SLICE1))
        for d, g in df.groupby("d"):
            g = g.reset_index(drop=True)
            g["cumv"] = g.volume.cumsum(); g["cumpv"] = (g.close * g.volume).cumsum()
            g["vwap"] = g.cumpv / g.cumv.replace(0, np.nan)
            g["mom5"] = g.close.pct_change(5) * 100
            g["vavg20"] = g.volume.rolling(20, 5).mean()
            g["orh15"] = g[g.hm <= "09:30"].high.max()
            bars[(s, d)] = g.set_index("hm")
    oc.close()
    return universe, bars
# ---------- execution simulator (Spec §5) ----------
def fill(side, ref_open, order_val, minute_val):
    slip = (BASE_SLIP_BPS + IMPACT_K * min(order_val, PARTICIPATION * minute_val) / max(minute_val, 1)) / 1e4
    return ref_open * (1 + slip) if side == "BUY" else ref_open * (1 - slip)
def rt_cost(entry, exitp, qty):
    try: return fpe.cost("CASH", entry, exitp, qty)
    except Exception: return 0.0006 * (entry + exitp) * qty
# ---------- agent interface (Spec §7) ----------
class Agent:
    def __init__(self, aid, product="MIS"):
        self.id = aid; self.product = product; self.cash = CAPITAL
        self.pos = {}            # symbol -> dict(qty, entry, stop, target)
        self.trades = []; self.eq = []
    def n_pos(self): return len(self.pos)
    def size_qty(self, price):
        notional = POS_FRAC * CAPITAL * (LEVERAGE if self.product == "MIS" else 1)
        return max(int(notional / price), 0)
    def decide(self, sym, row, prev_row): return None   # override -> 'BUY' or None
    def manage(self, sym, row):                          # stop/target/EOD per open pos
        p = self.pos.get(sym);
        if not p: return None
        hm = row.name
        if hm >= MIS_SQUAREOFF and self.product == "MIS": return ("EXIT", row.close, "EOD")
        # conservative: stop checked before target (Spec §5.2)
        if row.low <= p["stop"]: return ("EXIT", p["stop"], "STOP")
        if row.high >= p["target"]: return ("EXIT", p["target"], "TARGET")
        return None
class RetailMomentum(Agent):
    def decide(self, sym, row, prev):
        if sym in self.pos or self.n_pos() >= MAX_POS: return None
        if pd.notna(row.vwap) and row.close > row.vwap and (row.mom5 or 0) > 0.3 and (row.vavg20 or 0) > 0 and row.volume > 1.2 * row.vavg20:
            return "BUY"
class MeanReversion(Agent):
    def decide(self, sym, row, prev):
        if sym in self.pos or self.n_pos() >= MAX_POS: return None
        if pd.notna(row.vwap) and (row.close / row.vwap - 1) * 100 < -1.5 and prev is not None and row.close >= prev.close:
            return "BUY"
class OpeningRangeBreakout(Agent):
    def decide(self, sym, row, prev):
        if sym in self.pos or self.n_pos() >= MAX_POS: return None
        if row.name > "09:30" and pd.notna(row.orh15) and row.close > row.orh15 and (row.vavg20 or 0) > 0 and row.volume > 1.5 * row.vavg20:
            return "BUY"
def target_stop(agent, price):
    if isinstance(agent, RetailMomentum): return price * 1.010, price * 0.994
    if isinstance(agent, MeanReversion):  return price * 1.012, price * 0.990
    return price * 1.015, price * 0.990   # ORB
# ---------- replay loop (Spec §4) ----------
def run(universe, bars, agents):
    days = sorted(set(d for (_, d) in bars))
    mins = [f"{9 + (m // 60):02d}:{m % 60:02d}" for m in range(15, 15 + (15 - 9) * 60 + 15)]  # 09:15..15:29
    mins = [x for x in mins if "09:15" <= x <= SESS_END]
    for d in days:
        active = {s: bars[(s, d)] for s in universe if (s, d) in bars}
        for ag in agents: ag.pos.clear()
        prev = {}
        for i, hm in enumerate(mins):
            nxt = mins[i + 1] if i + 1 < len(mins) else None
            for ag in agents:
                # manage open positions
                for s in list(ag.pos):
                    g = active.get(s)
                    if g is None or hm not in g.index: continue
                    act = ag.manage(s, g.loc[hm])
                    if act:
                        _, px, why = act; p = ag.pos.pop(s)
                        c = rt_cost(p["entry"], px, p["qty"]); pnl = (px - p["entry"]) * p["qty"] - c
                        ag.cash += pnl; ag.trades.append(dict(sym=s, day=d, entry=p["entry"], exitp=px, qty=p["qty"], pnl=pnl, why=why))
                # new entries (fill at NEXT minute open — anti-lookahead)
                if nxt is None: continue
                for s, g in active.items():
                    if hm not in g.index or nxt not in g.index: continue
                    sig = ag.decide(s, g.loc[hm], prev.get(s))
                    if sig == "BUY":
                        nb = g.loc[nxt]; mval = nb.close * nb.volume; qty = ag.size_qty(nb.open)
                        if qty <= 0: continue
                        oval = qty * nb.open
                        if oval > PARTICIPATION * mval: qty = int(PARTICIPATION * mval / nb.open)
                        if qty <= 0: continue
                        fp = fill("BUY", nb.open, qty * nb.open, mval); tgt, stp = target_stop(ag, fp)
                        ag.pos[s] = dict(qty=qty, entry=fp, stop=stp, target=tgt)
            for s, g in active.items():
                if hm in g.index: prev[s] = g.loc[hm]
        # EOD mark equity
        for ag in agents:
            unreal = 0.0
            ag.eq.append((d, ag.cash + unreal))
# ---------- analytics / leaderboard (Spec §14) ----------
def metrics(ag):
    eq = pd.Series([e for _, e in ag.eq]); ret = (eq.iloc[-1] / CAPITAL - 1) * 100 if len(eq) else 0
    peak = eq.cummax(); dd = ((peak - eq) / peak * 100).max() if len(eq) else 0
    dret = eq.pct_change().dropna(); sharpe = (dret.mean() / (dret.std() + 1e-9) * math.sqrt(252)) if len(dret) > 1 else 0
    t = pd.DataFrame(ag.trades); wr = (t.pnl > 0).mean() * 100 if len(t) else 0
    return dict(agent=ag.id, ret=ret, dd=dd, sharpe=sharpe, wr=wr, trades=len(t), net_pnl=eq.iloc[-1] - CAPITAL if len(eq) else 0)
if __name__ == "__main__":
    print(f"loading universe + bars ({SLICE0}..{SLICE1})...")
    universe, bars = load()
    print(f"universe: {len(universe)} names | day-bars: {len(bars)}")
    agents = [RetailMomentum("Retail-Momentum"), MeanReversion("Mean-Reversion"), OpeningRangeBreakout("ORB-Breakout")]
    run(universe, bars, agents)
    lb = pd.DataFrame([metrics(a) for a in agents]).sort_values("ret", ascending=False)
    print("\n===== ARENA v0 LEADERBOARD (MIS 5x, ₹10L, cost+slippage+participation, T+1 fill) =====")
    print(f"{'rank':<5}{'agent':<20}{'net_ret%':>9}{'maxDD%':>8}{'sharpe':>8}{'win%':>7}{'trades':>8}{'net_pnl':>12}")
    for i, (_, r) in enumerate(lb.iterrows(), 1):
        print(f"{i:<5}{r.agent:<20}{r.ret:>+9.1f}{r.dd:>8.1f}{r.sharpe:>8.2f}{r.wr:>7.1f}{r.trades:>8.0f}{r.net_pnl:>+12,.0f}")
