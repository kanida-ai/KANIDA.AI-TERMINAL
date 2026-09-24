"""FALCON VIRTUAL TRADING ARENA — v2 (Addendum A roster + A3 self-learning). 21 agents on ONE point-in-time replay.
Families: conservative(5) + aggressive E(6) + scalpers F(4) + screeners G(4) + research-lab A4(2).
Every non-basket agent inherits the A3 PER-AGENT SELF-LEARNING LAYER (online, private, causal):
  - setup_id = (time-of-day bucket, breadth-regime bucket); learning is per-setup.
  - on each CLOSED trade: update running expectancy + realized MFE/MAE for that setup.
  - adapt NEXT decision only:  size_mult (bounded, expectancy-driven) · stop/target (from realized MFE/MAE)
                               · enabled (sit out a setup whose recent expectancy went negative, with periodic probe).
Anti-lookahead (A3.4): updates fire ONLY on closed trades; stats use exits<=now; adapted params affect the NEXT
decision, never open positions; in walk-forward the learner accumulates through train and continues online into test.
Shared structural guarantees: T+1-open fills, full ₹ cost stack, participation cap, HARD margin cap (gross<=equity*lev).
READ-ONLY on data."""
import os, sys, importlib.util, math
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
spec = importlib.util.spec_from_file_location("arena1", os.path.join(ROOT, "arena", "arena_v1.py"))
A1 = importlib.util.module_from_spec(spec); spec.loader.exec_module(A1)
CAPITAL, PARTICIPATION, SESS_END, MIS_CUT, RUIN = A1.CAPITAL, A1.PARTICIPATION, A1.SESS_END, A1.MIS_CUT, A1.RUIN
fillpx, rt_cost = A1.fillpx, A1.rt_cost
# ---- A3 learning hyperparameters ----
LEARN_MIN = 8          # closed trades in a setup before its stats are trusted
EMA_DECAY = 0.85       # recency weight for per-setup expectancy / MFE / MAE
SIZE_BETA = 3.0        # expectancy(%) -> size multiplier slope
SIZE_LO, SIZE_HI = 0.30, 2.00
MAX_BET = 0.50         # cap on any single per-name notional fraction (pre-margin)
NEG_TOL = -0.05        # disable a setup when recent expectancy(%) falls below this
PROBE_EVERY = 50       # while disabled, allow one probe trade every N visits (keeps learning alive)

def build_panels(universe, bars):
    days = sorted(set(d for (_, d) in bars)); panels = {}
    for d in days:
        recs = {s: bars[(s, d)] for s in universe if (s, d) in bars}
        M = {f: pd.DataFrame({s: recs[s][f] for s in recs}) for f in ["open", "high", "low", "close", "volume", "vwap", "mom5", "vavg20", "orh15"]}
        c, v = M["close"], M["volume"]
        M["mval"] = c * v
        M["ret1"] = c.pct_change(fill_method=None) * 100
        M["ret2"] = c.pct_change(2, fill_method=None) * 100
        M["vwgap"] = (c / M["vwap"] - 1) * 100
        M["volsurge"] = v / M["vavg20"].replace(0, np.nan)
        M["rng"] = (M["high"] - M["low"]) / c * 100
        M["cmin"] = M["low"].cummin(); M["cmax"] = M["high"].cummax()
        panels[d] = M
    return panels, days

def _z(s):
    s = s.dropna(); sd = s.std()
    return (s - s.mean()) / sd if sd and sd == sd else s * 0.0

def setup_id(hm, breadth):
    tb = "open" if hm < "10:00" else "mid" if hm < "13:00" else "aft" if hm < "14:30" else "close"
    rb = "on" if breadth > 0.55 else "off" if breadth < 0.45 else "neu"
    return tb + "|" + rb

# ============================ base agent: margin-capped, T+1 fill, A3 self-learning ============================
class Agent:
    fam = "consv"; product = "MIS"; lev = 5; max_pos = 5; frac = 0.20; can_short = False; pyramid = False
    sq = SESS_END; base_st = (0.010, 0.015); learn = True; uses_a3 = True   # uses_a3=False => opt out of the A3 toggle
    def __init__(self, aid, **kw):
        self.id = aid
        for k, val in kw.items(): setattr(self, k, val)
        self.cash = CAPITAL; self.pos = {}; self.trades = []; self.eq = []; self.dead = False
        self.day_realized = 0.0; self.cur_tag = None; self._cur_setup = None; self._mi = 0
        self.stats = {}; self.enabled = {}; self.size_mult = {}; self.st_adapt = {}; self._probe = {}
    # ---- portfolio math ----
    def equity(self, close):
        un = 0.0
        for s, p in self.pos.items():
            px = close.get(s, np.nan); px = p["entry"] if px != px else px
            un += p["side"] * (px - p["entry"]) * p["qty"]
        return self.cash + un
    def gross(self, close):
        return sum(p["qty"] * (close.get(s, p["entry"]) if close.get(s, p["entry"]) == close.get(s, p["entry"]) else p["entry"]) for s, p in self.pos.items())
    # ---- A3 learning ----
    def _learn_update(self, sid, net_pct, mfe, mae):
        st = self.stats.setdefault(sid, dict(n=0, en=0.0, ef=0.0, ea=0.0))
        first = st["n"] == 0
        st["en"] = net_pct if first else EMA_DECAY * st["en"] + (1 - EMA_DECAY) * net_pct
        st["ef"] = mfe if first else EMA_DECAY * st["ef"] + (1 - EMA_DECAY) * mfe
        st["ea"] = mae if first else EMA_DECAY * st["ea"] + (1 - EMA_DECAY) * mae
        st["n"] += 1
        if st["n"] >= LEARN_MIN:
            self.enabled[sid] = st["en"] >= NEG_TOL
            self.size_mult[sid] = float(np.clip(1 + SIZE_BETA * st["en"], SIZE_LO, SIZE_HI))
            sd = float(np.clip(abs(st["ea"]) * 0.008, 0.003, 0.02))    # stop  ~ 0.8x typical adverse excursion
            td = float(np.clip(st["ef"] * 0.008, 0.004, 0.05))         # target~ 0.8x typical favourable excursion
            self.st_adapt[sid] = (sd, td)
    def _sit_out(self, sid):
        if not self.learn or self.enabled.get(sid, True): return False
        self._probe[sid] = self._probe.get(sid, 0) + 1
        return self._probe[sid] % PROBE_EVERY != 0      # sit out except a periodic probe
    # ---- lifecycle ----
    def day_reset(self, ctx):
        if self.product == "MIS": self.pos.clear()
        self.day_realized = 0.0
    def stop_target(self, side, fp):
        sd, td = (self.st_adapt.get(self._cur_setup, self.base_st) if self.learn else self.base_st)
        return (fp * (1 - sd), fp * (1 + td)) if side == 1 else (fp * (1 + sd), fp * (1 - td))
    def want_exit(self, s, p, ctx): return None
    def entries(self, ctx): return []
    def step(self, ctx):
        self._mi = ctx.mi; self._cur_setup = ctx.setup
        self._manage(ctx)
        if ctx.nopen is None: return
        if self._sit_out(ctx.setup): return                          # A3: skip bad setup (learned negative)
        for (s, side, fr) in self.entries(ctx): self._open(ctx, s, side, fr)
    def _manage(self, ctx):
        for s, p in list(self.pos.items()):
            px = ctx.close.get(s, np.nan)
            if px != px: continue
            hi, lo, side = ctx.high.get(s, px), ctx.low.get(s, px), p["side"]
            fav = (hi - p["entry"]) / p["entry"] * 100 if side == 1 else (p["entry"] - lo) / p["entry"] * 100
            adv = (lo - p["entry"]) / p["entry"] * 100 if side == 1 else (p["entry"] - hi) / p["entry"] * 100
            p["mfe"] = max(p["mfe"], fav); p["mae"] = min(p["mae"], adv)      # realized excursions (past only)
            why, xpx = None, px
            if self.product == "MIS" and ctx.hm >= self.sq: why = "EOD"
            elif side == 1 and lo <= p["stop"]: why, xpx = "STOP", p["stop"]
            elif side == -1 and hi >= p["stop"]: why, xpx = "STOP", p["stop"]
            elif side == 1 and hi >= p["target"]: why, xpx = "TGT", p["target"]
            elif side == -1 and lo <= p["target"]: why, xpx = "TGT", p["target"]
            else:
                w = self.want_exit(s, p, ctx)
                if w: why = w
            if why: self._close(s, xpx, why)
    def _close(self, s, px, why):
        p = self.pos.pop(s); c = rt_cost(p["entry"], px, p["qty"]); gross = p["side"] * (px - p["entry"]) * p["qty"]; pnl = gross - c
        self.cash += pnl; self.day_realized += pnl
        notl = p["qty"] * p["entry"]; net_pct = pnl / notl * 100 if notl else 0.0
        self.trades.append(dict(sym=s, pnl=pnl, gross=gross, cost=c, why=why, tag=p.get("tag"), side=p["side"],
                                entry=p["entry"], exit=px, notl=notl, hold=self._mi - p["bar"],
                                setup=p["setup"], gross_pct=gross / notl * 100 if notl else 0, cost_pct=c / notl * 100 if notl else 0))
        if self.learn: self._learn_update(p["setup"], net_pct, p["mfe"], p["mae"])
    def _open(self, ctx, s, side, fr):
        held = s in self.pos
        if held and not (self.pyramid and self.pos[s]["side"] == side): return
        if not held and len(self.pos) >= self.max_pos: return
        no = ctx.nopen.get(s, np.nan)
        if no != no or no <= 0: return
        eq = self.equity(ctx.close)
        fr = min((fr or self.frac) * (self.size_mult.get(ctx.setup, 1.0) if self.learn else 1.0), MAX_BET)   # A3 sizing
        qty = int(fr * eq * self.lev / no)
        mv = ctx.mval.get(s, 0.0); mv = 0.0 if mv != mv else mv
        if qty * no > PARTICIPATION * mv: qty = int(PARTICIPATION * mv / no)
        if qty <= 0: return
        if self.gross(ctx.close) + qty * no > eq * self.lev + 1: return
        fp = fillpx("BUY" if side == 1 else "SELL", no, qty * no, mv)
        if held:
            p = self.pos[s]; nq = p["qty"] + qty; p["entry"] = (p["entry"] * p["qty"] + fp * qty) / nq; p["qty"] = nq
            p["stop"], p["target"] = self.stop_target(side, p["entry"])
        else:
            st, tg = self.stop_target(side, fp)
            self.pos[s] = dict(qty=qty, entry=fp, side=side, stop=st, target=tg, bar=ctx.mi, tag=self.cur_tag, setup=ctx.setup, mfe=0.0, mae=0.0)

# ============================ conservative per-stock ============================
class RetailMomentum(Agent):
    fam = "consv"; sq = MIS_CUT; base_st = (0.006, 0.010)
    def entries(self, ctx):
        m = (ctx.close > ctx.vwap) & (ctx.mom5 > 0.3) & (ctx.volsurge > 1.2)
        return [(s, 1, None) for s in ctx.mom5[m].dropna().index if s not in self.pos][:self.max_pos]
class MeanReversion(Agent):
    fam = "consv"; sq = MIS_CUT; base_st = (0.010, 0.012)
    def entries(self, ctx):
        m = (ctx.vwgap < -1.5) & (ctx.close >= ctx.prev_close)
        return [(s, 1, None) for s in ctx.vwgap[m].dropna().index if s not in self.pos][:self.max_pos]
class ORB(Agent):
    fam = "consv"; sq = MIS_CUT; base_st = (0.010, 0.015)
    def entries(self, ctx):
        if ctx.hm <= "09:30": return []
        m = (ctx.close > ctx.orh) & (ctx.volsurge > 1.5)
        return [(s, 1, None) for s in ctx.volsurge[m].dropna().index if s not in self.pos][:self.max_pos]

# ============================ Family E — aggressive ============================
class LeverageMomentumAggressor(Agent):
    fam = "aggr"; max_pos = 4; frac = 0.22; pyramid = True; base_st = (0.015, 0.060)
    def entries(self, ctx):
        m = (ctx.close > ctx.vwap) & (ctx.mom5 > 0.5) & (ctx.volsurge > 1.3)
        top = ctx.mom5[m].dropna().nlargest(self.max_pos).index
        res = [(s, 1, None) for s in top if s not in self.pos]
        res += [(s, 1, 0.08) for s, p in self.pos.items() if ctx.mom5.get(s, 0) > 0.8]
        return res
class ConcentratedConviction(Agent):
    fam = "aggr"; max_pos = 3; frac = 0.33; base_st = (0.020, 0.050)
    def entries(self, ctx):
        m = (ctx.close > ctx.vwap) & (ctx.mom5 > 0.4)
        return [(s, 1, None) for s in ctx.mom5[m].dropna().nlargest(self.max_pos).index if s not in self.pos]
class FuturesTrendAggressor(Agent):            # futures-proxy: cash bars, long/short, high lev
    fam = "aggr"; max_pos = 4; frac = 0.22; can_short = True; pyramid = True; base_st = (0.015, 0.050)
    def entries(self, ctx):
        lng = (ctx.mom5 > 0.5) & (ctx.close > ctx.vwap) & (ctx.volsurge > 1.2)
        sht = (ctx.mom5 < -0.5) & (ctx.close < ctx.vwap) & (ctx.volsurge > 1.2)
        res = [(s, 1, None) for s in ctx.mom5[lng].dropna().nlargest(2).index if s not in self.pos]
        res += [(s, -1, None) for s in ctx.mom5[sht].dropna().nsmallest(2).index if s not in self.pos]
        return res
class VolatilityHarvester(Agent):
    fam = "aggr"; max_pos = 5; base_st = (0.015, 0.030)
    def entries(self, ctx):
        fr = 0.28 if ctx.rng.dropna().mean() > 0.9 else 0.10
        m = (ctx.close > ctx.vwap) & (ctx.mom5 > 0.4) & (ctx.volsurge > 1.2)
        return [(s, 1, fr) for s in ctx.mom5[m].dropna().nlargest(self.max_pos).index if s not in self.pos]
class CompoundingScalperStacker(Agent):
    fam = "aggr"; max_pos = 5; base_st = (0.004, 0.006)
    def entries(self, ctx):
        fr = 0.15 * (1 + max(0.0, self.day_realized) / CAPITAL)
        m = (ctx.ret2 > 0.3) & (ctx.volsurge > 1.2) & (ctx.close > ctx.vwap)
        return [(s, 1, fr) for s in ctx.ret2[m].dropna().nlargest(self.max_pos).index if s not in self.pos]
class BreakoutBlitz(Agent):
    fam = "aggr"; max_pos = 6; frac = 0.16; base_st = (0.010, 0.020)
    def entries(self, ctx):
        if ctx.hm <= "09:30": return []
        m = (ctx.close > ctx.orh) & (ctx.volsurge > 1.5)
        return [(s, 1, None) for s in ctx.volsurge[m].dropna().nlargest(self.max_pos).index if s not in self.pos]

# ============================ Family F — scalpers ============================
class VWAPReversionScalper(Agent):
    fam = "scalp"; max_pos = 6; frac = 0.15; base_st = (0.005, 0.030)
    def entries(self, ctx):
        m = (ctx.vwgap < -0.8) & (ctx.ret1 >= 0)
        return [(s, 1, None) for s in ctx.vwgap[m].dropna().nsmallest(self.max_pos).index if s not in self.pos]
    def want_exit(self, s, p, ctx): return "SNAP" if (ctx.vwgap.get(s, -1) >= -0.1 or ctx.mi - p["bar"] >= 10) else None
class MicroMomentumScalper(Agent):
    fam = "scalp"; max_pos = 6; frac = 0.15; base_st = (0.004, 0.006)
    def entries(self, ctx):
        m = (ctx.ret2 > 0.4) & (ctx.volsurge > 1.3)
        return [(s, 1, None) for s in ctx.ret2[m].dropna().nlargest(self.max_pos).index if s not in self.pos]
    def want_exit(self, s, p, ctx): return "FADE" if (ctx.ret1.get(s, 0) < 0 or ctx.mi - p["bar"] >= 5) else None
class OpeningDriveScalper(Agent):
    fam = "scalp"; max_pos = 6; frac = 0.15; base_st = (0.005, 0.008)
    def entries(self, ctx):
        if ctx.hm > "10:00": return []
        m = (ctx.mom5 > 0.4) & (ctx.close > ctx.vwap)
        return [(s, 1, None) for s in ctx.mom5[m].dropna().nlargest(self.max_pos).index if s not in self.pos]
    def want_exit(self, s, p, ctx): return "TIME" if (ctx.hm > "10:00" or ctx.mi - p["bar"] >= 8) else None
class RangeEdgeScalper(Agent):
    fam = "scalp"; max_pos = 6; frac = 0.15; base_st = (0.006, 0.020)
    def entries(self, ctx):
        span = (ctx.cmax - ctx.cmin); pos = (ctx.close - ctx.cmin) / span.replace(0, np.nan)
        m = (pos < 0.15) & (ctx.ret1 >= 0)
        return [(s, 1, None) for s in pos[m].dropna().index if s not in self.pos][:self.max_pos]
    def want_exit(self, s, p, ctx):
        span = ctx.cmax.get(s, np.nan) - ctx.cmin.get(s, np.nan)
        loc = (ctx.close.get(s, np.nan) - ctx.cmin.get(s, np.nan)) / span if span else 1
        return "EDGE" if (loc > 0.6 or ctx.mi - p["bar"] >= 15) else None

# ============================ Family G — dynamic screeners ============================
class Screener(Agent):
    fam = "screen"; max_pos = 8; frac = 0.11; screen_every = 5; base_st = (0.015, 0.030)
    def day_reset(self, ctx): super().day_reset(ctx); self.target = set()
    def rank(self, ctx): return []
    def entries(self, ctx):
        if ctx.mi % self.screen_every == 0: self.target = set(self.rank(ctx))
        return [(s, 1, None) for s in self.target if s not in self.pos]
    def want_exit(self, s, p, ctx):
        return "ROTATE" if (getattr(self, "target", set()) and s not in self.target and ctx.mi % self.screen_every == 0) else None
class MomentumScreener(Screener):
    def rank(self, ctx): return list(ctx.mom5[ctx.volsurge > 1.0].dropna().nlargest(self.max_pos).index)
class BreakoutScreener(Screener):
    def rank(self, ctx): return list(ctx.volsurge[(ctx.close > ctx.orh)].dropna().nlargest(self.max_pos).index)
class RelativeStrengthScreener(Screener):
    def rank(self, ctx): return list(ctx.vwgap.dropna().nlargest(self.max_pos).index)
class MultiFactorScreener(Screener):
    def rank(self, ctx):
        sc = _z(ctx.mom5).add(_z(ctx.volsurge), fill_value=0).add(_z(ctx.vwgap), fill_value=0)
        return list(sc.dropna().nlargest(self.max_pos).index)

# ============================ A4 — research-lab meta-agents (hypothesis selection ON TOP of A3) ============================
class ResearchLab(Agent):
    fam = "lab"; max_pos = 5; frac = 0.18; base_st = (0.010, 0.015)
    def __init__(self, aid, pool, **kw):
        super().__init__(aid, **kw); self.pool = pool; self.hstats = {h["id"]: dict(n=0, pnl=0.0) for h in pool}; self.active = pool[0]
    def day_reset(self, ctx):
        super().day_reset(ctx)
        ready = [h for h in self.pool if self.hstats[h["id"]]["n"] >= LEARN_MIN]
        if ready: self.active = max(ready, key=lambda h: self.hstats[h["id"]]["pnl"] / max(self.hstats[h["id"]]["n"], 1))
        self.cur_tag = self.active["id"]
    def _close(self, s, px, why):
        p = self.pos.get(s); tag = p.get("tag") if p else None
        super()._close(s, px, why)
        if tag in self.hstats: self.hstats[tag]["n"] += 1; self.hstats[tag]["pnl"] += self.trades[-1]["pnl"]
    def entries(self, ctx):
        return [(s, 1, None) for s in self.active["signal"](ctx) if s not in self.pos][:self.max_pos]
def _lab_mom(th): return lambda ctx: list(ctx.mom5[(ctx.close > ctx.vwap) & (ctx.mom5 > th) & (ctx.volsurge > 1.2)].dropna().nlargest(5).index)
def _lab_mr(th): return lambda ctx: list(ctx.vwgap[(ctx.vwgap < th) & (ctx.close >= ctx.prev_close)].dropna().nsmallest(5).index)

# ---- signal-fed research lab: trades the REAL Falcon daily picks; DISCOVERS the best harvest recipe (A4) ----
SIGNAL_RECIPES = [   # execution hypotheses over the Falcon daily picks. sd/td=0.99 => no intraday stop (hold to EOD).
                     # trail=(arm,floor,giveback,disaster) => Magnifier-style BASKET trail on aggregate 5x P&L.
    dict(id="hold_top10",  n=10, sd=0.99, td=0.99),        # DEFAULT (cold-start): diversified, hold to EOD
    dict(id="hold_top5",   n=5,  sd=0.99, td=0.99),        # concentrated hold to EOD
    dict(id="trail_top10", n=10, trail=(6, 2, 5, 3)),      # diversified + basket trail (Magnifier knobs)
    dict(id="trail_top5",  n=5,  trail=(6, 2, 5, 3)),      # concentrated + basket trail
    dict(id="trail_top15", n=15, trail=(6, 2, 5, 3)),      # Magnifier-equivalent (breadth + trail)
    dict(id="top10_stop3", n=10, sd=0.030, td=0.99),       # diversified + ride, -3% per-name disaster stop
    dict(id="top5_tight",  n=5,  sd=0.020, td=0.030),      # active scalp of the pick (+3% / -2%)
]
class SignalLab(ResearchLab):
    """A4 meta-agent fed the real Falcon signal (today's high-tier ranked picks). It does NOT invent entries — it
    picks WHICH execution recipe (concentration + stop/target) to harvest the signal with, learning from its own
    closed trades. Enters the active recipe's top-N picks once daily (09:16 -> T+1 fill), MIS 5x, per-name stops."""
    fam = "lab"; product = "MIS"; lev = 5; max_pos = 15; learn = False; uses_a3 = False   # A4 recipe selection, NOT A3 buckets
    def __init__(self, aid, recipes=SIGNAL_RECIPES):
        super().__init__(aid, recipes); self.learn = False; self._bpeak = 0.0
    def day_reset(self, ctx):
        super().day_reset(ctx); self._bpeak = 0.0                # reset basket-trail peak each day
    def stop_target(self, side, fp):
        if self.active.get("trail"): return (fp * 0.001, fp * 1000.0)   # no per-name stop; basket trail manages exits
        sd, td = self.active["sd"], self.active["td"]
        return (fp * (1 - sd), fp * (1 + td)) if side == 1 else (fp * (1 + sd), fp * (1 - td))
    def _exit_all(self, ctx, why):
        for s in list(self.pos):
            px = ctx.close.get(s, np.nan); px = self.pos[s]["entry"] if px != px else px
            self._close(s, px, why)
    def _manage(self, ctx):
        tr = self.active.get("trail")
        if not tr: return super()._manage(ctx)                   # per-name stop/target/EOD
        if not self.pos: return
        rets = [ctx.close.get(s, np.nan) / p["entry"] - 1 for s, p in self.pos.items() if ctx.close.get(s, np.nan) == ctx.close.get(s, np.nan)]
        pnl = np.mean(rets) * 100 * self.lev if rets else None
        A, F, G, S = tr; armed = self._bpeak >= A; stop = max(F, self._bpeak - G) if armed else -S
        if pnl is not None and pnl <= stop: self._exit_all(ctx, "TRAIL")
        elif ctx.hm >= self.sq: self._exit_all(ctx, "EOD")
        elif pnl is not None: self._bpeak = max(self._bpeak, pnl)
    def entries(self, ctx):
        if ctx.hm != "09:16" or not getattr(ctx, "falcon", None): return []
        n = self.active["n"]; picks = sorted(ctx.falcon.items(), key=lambda kv: kv[1])[:n]   # lowest rank = best
        fr = 0.95 / n
        return [(s, 1, fr) for s, _ in picks if s not in self.pos]

def falcon_ranks(slice0=A1.SLICE0, slice1=A1.SLICE1):
    P = pd.read_pickle(A1.PICKS)
    B = P[(P["rank"] <= 15) & P.tier.isin(A1.HIT) & (P.entry_date >= slice0) & (P.entry_date <= slice1)]
    return {d: dict(zip(sub.symbol, sub["rank"])) for d, sub in B.groupby("entry_date")}

# ============================ replay ============================
class Ctx: pass
def run(panels, days, fb, agents, univ, raw_bars, falcon_rank=None, xs_signal=None):
    falcon_rank = falcon_rank or {}; xs_signal = xs_signal or {}
    di = {d: i for i, d in enumerate(days)}
    baskets = [a for a in agents if getattr(a, "is_basket", False)]
    others = [a for a in agents if not getattr(a, "is_basket", False)]
    for d in days:
        M = panels[d]; idx = [h for h in M["close"].index if "09:15" <= h <= SESS_END]
        print(f"[replay] {d} ({di[d]+1}/{len(days)})", flush=True)
        active = {s: raw_bars[(s, d)] for s in univ if (s, d) in raw_bars}
        ctx = Ctx(); ctx.falcon = falcon_rank.get(d, {}); ctx.xs = xs_signal.get(d, {})
        for ag in others: ag.day_reset(ctx)
        for ag in baskets:
            if ag.product == "MIS": ag.pos.clear()
        prev_close = None
        for k, hm in enumerate(idx):
            nxt = idx[k + 1] if k + 1 < len(idx) else None
            ctx.hm, ctx.mi, ctx.di = hm, k, di[d]
            for f in ["open", "high", "low", "close", "vwap", "mom5", "vavg20", "volume", "orh15", "mval", "ret1", "ret2", "vwgap", "volsurge", "rng", "cmin", "cmax"]:
                setattr(ctx, {"vavg20": "vavg", "orh15": "orh", "volume": "vol"}.get(f, f), M[f].loc[hm])
            ctx.prev_close = prev_close if prev_close is not None else ctx.close
            ctx.nopen = M["open"].loc[nxt] if nxt else None
            gt = ctx.close > ctx.vwap; ctx.setup = setup_id(hm, gt.sum() / max(gt.count(), 1))
            for ag in others:
                if not ag.dead: ag.step(ctx)
            for ag in baskets:
                if ag.dead: continue
                ag._cur_day = d
                if not ag.pos and hm == "09:16" and d in ag.baskets:
                    ag.enter(active, hm, d); ag.entered_di = di[d]
                elif ag.pos:
                    pnl = ag.basket_pnl(active, hm)
                    if ag.trail:
                        Aa, F, G, S = ag.trail; armed = ag.peak >= Aa; stop = max(F, ag.peak - G) if armed else -S
                        if pnl <= stop: ag.exit_all(active, hm, "TRAIL")
                        elif ag.product == "MIS" and hm >= SESS_END: ag.exit_all(active, hm, "EOD")
                        else: ag.peak = max(ag.peak, pnl)
                    else:
                        if pnl <= -ag.hard: ag.exit_all(active, hm, "STOP")
                        elif di[d] >= ag.entered_di + (ag.hold_days - 1) and hm >= SESS_END: ag.exit_all(active, hm, "EOD")
            prev_close = ctx.close
        cN = M["close"].loc[idx[-1]]
        for ag in others:
            eq = ag.equity(cN); ag.eq.append((d, eq))
            if eq <= RUIN * CAPITAL: ag.dead = True
        for ag in baskets:
            eq = ag.equity(active, SESS_END); ag.eq.append((d, eq))
            if eq <= RUIN * CAPITAL: ag.dead = True

def metrics(ag):
    eq = pd.Series([e for _, e in ag.eq]); ret = (eq.iloc[-1] / CAPITAL - 1) * 100
    peak = eq.cummax(); ddser = (peak - eq); dd = (ddser / peak * 100).max(); dd_rs = ddser.max()
    dret = eq.pct_change().dropna(); sh = dret.mean() / (dret.std() + 1e-9) * math.sqrt(252) if len(dret) > 1 else 0
    de = eq.diff().fillna(eq.iloc[0] - CAPITAL); mcl = 0; run = 0
    for x in de:
        run = run + 1 if x < 0 else 0; mcl = max(mcl, run)
    net = eq.iloc[-1] - CAPITAL; rec = net / dd_rs if dd_rs > 0 else float("inf")
    t = pd.DataFrame(ag.trades); wr = (t.pnl > 0).mean() * 100 if len(t) else 0
    off = sum(1 for v in getattr(ag, "enabled", {}).values() if not v)
    return dict(agent=ag.id, fam=ag.fam, ret=ret, dd=dd, sharpe=sh, wr=wr, trades=len(t), off=off,
                mcl=mcl, rec=rec, net=net, flag="RUIN" if ag.dead else "")

def make_agents(fb, learn=True):
    lab_mom = ResearchLab("Lab-Momentum", [dict(id="mom0.3", signal=_lab_mom(0.3)), dict(id="mom0.6", signal=_lab_mom(0.6)), dict(id="mom1.0", signal=_lab_mom(1.0))])
    lab_mr = ResearchLab("Lab-MeanRev", [dict(id="mr1.0", signal=_lab_mr(-1.0)), dict(id="mr1.5", signal=_lab_mr(-1.5)), dict(id="mr2.0", signal=_lab_mr(-2.0))])
    ag = [RetailMomentum("Retail-Momentum"), MeanReversion("Mean-Reversion"), ORB("ORB-Breakout"),
          A1.BasketAgent("Falcon-Magnifier", fb, "MIS", 5, trail=(6, 2, 5, 3)),
          A1.BasketAgent("Falcon-BTST", fb, "CNC", 1, trail=None, hold_days=2, hard_stop=6),
          LeverageMomentumAggressor("Lev-Momentum-Aggressor"), ConcentratedConviction("Concentrated-Conviction"),
          FuturesTrendAggressor("Futures-Trend-Aggressor"), VolatilityHarvester("Volatility-Harvester"),
          CompoundingScalperStacker("Compounding-Stacker"), BreakoutBlitz("Breakout-Blitz"),
          VWAPReversionScalper("VWAP-Reversion-Scalper"), MicroMomentumScalper("Micro-Momentum-Scalper"),
          OpeningDriveScalper("Opening-Drive-Scalper"), RangeEdgeScalper("Range-Edge-Scalper"),
          MomentumScreener("Momentum-Screener"), BreakoutScreener("Breakout-Screener"),
          RelativeStrengthScreener("RelStrength-Screener"), MultiFactorScreener("MultiFactor-Screener"),
          lab_mom, lab_mr, SignalLab("Falcon-SignalLab")]
    A1.BasketAgent.fam = "basket"
    for a in ag:
        if not hasattr(a, "fam"): a.fam = "basket"
        if not getattr(a, "is_basket", False) and getattr(a, "uses_a3", True): a.learn = learn   # respect A4-only agents
    return ag

def leaderboard(agents, title):
    lb = pd.DataFrame([metrics(a) for a in agents]).sort_values("ret", ascending=False)
    print(f"\n===== {title} =====")
    print(f"{'#':<3}{'agent':<26}{'fam':<7}{'ret%':>9}{'maxDD%':>8}{'sharpe':>8}{'win%':>7}{'trades':>8}{'offBkts':>8}{'recov':>7}{'  flag'}")
    for i, (_, r) in enumerate(lb.iterrows(), 1):
        rec = "inf" if r.rec == float("inf") else f"{r.rec:.1f}"
        print(f"{i:<3}{r.agent:<26}{r.fam:<7}{r.ret:>+9.1f}{r.dd:>8.1f}{r.sharpe:>8.2f}{r.wr:>7.1f}{r.trades:>8.0f}{r.off:>8.0f}{rec:>7}  {r.flag}")
    return lb

if __name__ == "__main__":
    learn = "off" not in sys.argv
    print(f"loading Nifty-500 universe + bars... (A3 learning {'ON' if learn else 'OFF'})")
    UNIV, RAW = A1.load_universe_and_bars(); fb = A1.falcon_baskets()
    panels, days = build_panels(UNIV, RAW)
    print(f"universe {len(UNIV)} | day-bars {len(RAW)} | basket-days {len(fb)} | trading days {len(days)}")
    agents = make_agents(fb, learn=learn)
    run(panels, days, fb, agents, UNIV, RAW, falcon_rank=falcon_ranks())
    leaderboard(agents, f"ARENA v2 — {len(agents)} agents (Nifty-500, May-2026, A3 learning {'ON' if learn else 'OFF'})")
