"""ADDENDUM B — PART 2: the autonomous minute-level agent "Apex".
A single self-directed agent that acts on every 1-min candle: it screens its own instruments, detects regime, scores
long/short signals across the Nifty-500, chooses direction + horizon + product + adaptive size, and risk-manages —
all bounded by FIXED market physics (T+1 fill, cost stack, slippage, participation cap, margin/ruin). "Unconstrained"
= strategy freedom; the physics never move (B7). Apex never rewrites itself in the loop — it only trades; it improves
as a CLIENT of the Part-1 pipeline (pipeline.py). This module also runs B11: the GROWTH-vs-RUIN FRONTIER and the B12
capacity sweep — the real deliverable is the frontier (max growth, max SURVIVABLE growth, ruin curve), not a headline
multiple. READ-ONLY on data; anti-lookahead inherited from the arena engine."""
import os, sqlite3, importlib.util
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
wf = importlib.util.spec_from_file_location("wf", os.path.join(os.path.dirname(__file__), "..", "..", "..", "AppData", "Local", "Temp", "claude", "C--Users-SPS-Desktop-Kanida-ai-Terminal-Quant-Intelligence-Engine", "c73fe1ef-c928-428e-a17b-d7f23047b24b", "scratchpad", "walk_forward.py"))
WF = importlib.util.module_from_spec(wf); wf.loader.exec_module(WF)
V = WF.V; CAP, RUIN = WF.CAP, WF.RUIN
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06"]

def _z(s):
    s = s.dropna(); sd = s.std()
    return (s - s.mean()) / sd if sd and sd == sd else s * 0.0

class Apex(V.Agent):
    """Self-directed intraday long/short. Skills (B8) composed each minute: regime detect, dynamic screen,
    multi-factor signal, direction + adaptive size + risk manager. Aggression profile sets its risk appetite."""
    fam = "apex"; product = "MIS"; can_short = True; uses_a3 = False; learn = False
    def __init__(self, aid, lev=5, base_frac=0.15, max_pos=8, stop=0.015, kill_dd=0.15, allow_short=True, cap0=CAP):
        super().__init__(aid); self.lev = lev; self.base_frac = base_frac; self.max_pos = max_pos
        self.stop = stop; self.kill_dd = kill_dd; self.allow_short = allow_short; self.cap0 = cap0
        self._peak_eq = cap0; self._killed_today = False
    def reset(self):
        self.cash = self.cap0; self.pos = {}; self.eq = []; self.dead = False; self.trades = []
        self.day_realized = 0.0; self._peak_eq = self.cap0; self._killed_today = False
    def day_reset(self, ctx):
        super().day_reset(ctx); self._killed_today = False; self._peak_eq = self.equity(ctx.close) if hasattr(ctx, "close") else self.cap0
    def stop_target(self, side, fp):                       # risk manager: fixed stop, ride winners to EOD
        return (fp * (1 - self.stop), fp * 5) if side == 1 else (fp * (1 + self.stop), fp * 0.2)
    # ---- skills composed per minute ----
    def _regime(self, ctx):                                # breadth-based regime detector
        gt = ctx.close > ctx.vwap
        return gt.sum() / max(gt.count(), 1)
    def _score(self, ctx):                                 # multi-factor signal generator (momentum+RS+flow+MTF proxy)
        return _z(ctx.mom5).add(_z(ctx.vwgap), fill_value=0).add(0.5 * _z(ctx.volsurge), fill_value=0).add(0.5 * _z(ctx.ret2), fill_value=0)
    def step(self, ctx):                                   # portfolio kill-switch wraps the base per-minute loop
        eqnow = self.equity(ctx.close)
        self._peak_eq = max(self._peak_eq, eqnow)
        if not self._killed_today and eqnow <= self._peak_eq * (1 - self.kill_dd):
            for s in list(self.pos):                       # hard risk breach -> flatten + sit out rest of day
                px = ctx.close.get(s, np.nan); px = self.pos[s]["entry"] if px != px else px
                self._close(s, px, "KILL")
            self._killed_today = True; return
        if self._killed_today: return
        super().step(ctx)
    def entries(self, ctx):
        regime = self._regime(ctx); sc = self._score(ctx).dropna()
        if sc.empty: return []
        k = self.max_pos; longs, shorts = [], []
        long_bias = int(round(k * min(max(regime, 0.2), 0.8)))          # regime tilts long/short mix
        nlong = long_bias if self.allow_short else k
        top = sc.nlargest(nlong).index
        longs = [(s, 1, None) for s in top if s not in self.pos and (ctx.close.get(s, 0) > ctx.vwap.get(s, 1e9))]
        if self.allow_short:
            bot = sc.nsmallest(k - nlong).index
            shorts = [(s, -1, None) for s in bot if s not in self.pos and (ctx.close.get(s, 1e9) < ctx.vwap.get(s, 0))]
        # adaptive size: conviction from |score|, bounded
        out = []
        for s, side, _ in longs + shorts:
            conv = min(abs(sc.get(s, 0)) / 2.0, 1.5)
            out.append((s, side, self.base_frac * (0.5 + conv)))
        return out

def run_frontier(profiles, months, oc, univ, frank):
    agents = {tag: Apex(tag, **prof) for tag, prof in profiles}      # one instance per profile, all run together
    rows = []
    for ym in months:
        bars = WF.load_month(oc, univ, ym); panels, days = V.build_panels(univ, bars)
        for ag in agents.values(): ag.reset()
        V.run(panels, days, {}, list(agents.values()), list(univ), bars, falcon_rank=frank)   # ONE pass serves all
        for tag, ag in agents.items():
            eqs = [e for _, e in ag.eq]
            if not eqs: continue
            s = pd.Series(eqs); dd = float(((s.cummax() - s) / s.cummax() * 100).max())
            rows.append(dict(profile=tag, month=ym, ret=(eqs[-1] / ag.cap0 - 1) * 100, maxdd=dd,
                             ruin=bool(ag.dead or min(eqs) <= RUIN * ag.cap0), trades=len(ag.trades)))
        print(f"  {ym} done", flush=True); del bars, panels
    return pd.DataFrame(rows)

if __name__ == "__main__":
    oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    univ = set(r[0] for r in oc.execute("SELECT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1"))
    frank = WF.falcon_ranks_window()
    print(f"universe {len(univ)} | months {len(MONTHS)}  — running Apex growth-vs-ruin frontier ...", flush=True)
    # aggression ladder: timid -> extreme (leverage x concentration x looser risk)
    profiles = [
        ("A1-timid",   dict(lev=2, base_frac=0.06, max_pos=10, stop=0.012, kill_dd=0.08, allow_short=False)),
        ("A2-modest",  dict(lev=3, base_frac=0.10, max_pos=8,  stop=0.015, kill_dd=0.10, allow_short=True)),
        ("A3-balanced",dict(lev=5, base_frac=0.15, max_pos=8,  stop=0.015, kill_dd=0.15, allow_short=True)),
        ("A4-agg",     dict(lev=5, base_frac=0.25, max_pos=5,  stop=0.020, kill_dd=0.25, allow_short=True)),
        ("A5-extreme", dict(lev=5, base_frac=0.45, max_pos=3,  stop=0.030, kill_dd=0.99, allow_short=True)),
    ]
    D = run_frontier(profiles, MONTHS, oc, univ, frank)
    oc.close()
    front = []
    for tag, g in D.groupby("profile"):
        front.append(dict(profile=tag, mean=g.ret.mean(), best=g.ret.max(), worst=g.ret.min(),
                          avgdd=g.maxdd.mean(), ruin_rate=g.ruin.mean() * 100, surv=(1 - g.ruin.mean()) * 100, trades=int(g.trades.sum())))
    order = [t for t, _ in profiles]
    F = pd.DataFrame(front).set_index("profile").loc[order].reset_index()
    print("\n" + "=" * 92); print("APEX — GROWTH-vs-RUIN FRONTIER (Jan-Jun 2026, monthly-reset Rs10L, honest physics)"); print("=" * 92)
    print(f"{'profile':<12}{'meanRet%':>9}{'bestMo%':>9}{'worstMo%':>9}{'avgDD%':>8}{'ruinRate%':>10}{'surv%':>7}{'trades':>8}")
    for _, r in F.iterrows():
        print(f"{r.profile:<12}{r['mean']:>+9.1f}{r['best']:>+9.1f}{r['worst']:>+9.1f}{r['avgdd']:>8.1f}{r['ruin_rate']:>10.0f}{r['surv']:>7.0f}{r['trades']:>8d}")
    survivable = F[F.ruin_rate <= 10]
    print("\n[B11] max ACHIEVED growth (any profile):   ", f"{F['mean'].max():+.1f}%/mo  ({F.loc[F['mean'].idxmax(),'profile']})")
    if len(survivable):
        print("[B11] max SURVIVABLE growth (ruin<=10%):    ", f"{survivable['mean'].max():+.1f}%/mo  ({survivable.loc[survivable['mean'].idxmax(),'profile']})")
    print("[B11] ruin appears at aggression:            ", ", ".join(f"{r.profile}={r.ruin_rate:.0f}%" for _, r in F.iterrows()))
    print("\nNOTE: futures/MTF are cash-bar proxies (leverage+short on cash); swing/positional horizons not yet enabled (intraday MIS only).")
