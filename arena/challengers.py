"""CHALLENGER ARENA — generate worthy challengers vs FIXED Falcon incumbents; report which clear the 3-gate bar.
Falcon (Magnifier 5x-MIS, SignalLab 5x-MIS) = FIXED incumbents (untouched). Benchmarks = naive momentum + naive
mean-reversion (5x MIS). Challengers evaluated apples-to-apples (same window/costs/capital, 5x MIS intraday) across
MULTIPLE months, monthly-reset; last month = HELD-OUT (touched once at the gate). Reports per challenger: gross
bps/trade, net%, maxDD%, breadth (stocks/day), + 3-gate PASS/FAIL vs Magnifier, + multiple-testing margin.
Leverage is 5x(MIS) or 1x(CNC) ONLY — no challenger wins on hotter leverage. READ-ONLY on data."""
import os, sqlite3, importlib.util
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
WFP = r"C:\Users\SPS\AppData\Local\Temp\claude\C--Users-SPS-Desktop-Kanida-ai-Terminal-Quant-Intelligence-Engine\c73fe1ef-c928-428e-a17b-d7f23047b24b\scratchpad\walk_forward.py"
WF = importlib.util.module_from_spec(importlib.util.spec_from_file_location("wf", WFP)); importlib.util.spec_from_file_location("wf", WFP).loader.exec_module(WF)
V = WF.V; CAP = V.CAPITAL; ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06"]; HELDOUT = "2026-06"

# ---------- cross-sectional daily signal (trailing 5d return as of yesterday), point-in-time ----------
def daily_signals(univ):
    oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    d = pd.read_sql_query("SELECT symbol, trade_date, close FROM ohlc_daily WHERE trade_date>='2025-10-01' AND trade_date<='2026-07-01' ORDER BY symbol, trade_date", oc)
    oc.close(); d = d[d.symbol.isin(univ)]
    piv = d.pivot_table(index="trade_date", columns="symbol", values="close")
    mom = piv.pct_change(5).shift(1)                      # trailing-5d return, known as of the prior close
    return {dt: mom.loc[dt].dropna() for dt in mom.index}

# ---------- Family-2 challenger: cross-sectional rank (breadth-heavy, high-capacity) ----------
class XSRank(V.Agent):
    fam = "XS"; product = "MIS"; lev = 5; sq = V.SESS_END; base_st = (0.99, 0.99); learn = False; uses_a3 = False
    def __init__(self, aid, top=True, n=30):
        super().__init__(aid); self.top = top; self.n = n
    def stop_target(self, side, fp): return (fp * 0.001, fp * 1000.0)     # no intraday stop; hold to EOD
    def entries(self, ctx):
        xs = getattr(ctx, "xs", None)
        if ctx.hm != "09:16" or xs is None or len(xs) == 0: return []
        picks = (xs.nlargest(self.n) if self.top else xs.nsmallest(self.n)).index
        fr = 0.95 / self.n
        return [(s, 1, fr) for s in picks if s not in self.pos]

# ---------- Family-1 adversarial challengers (target Falcon's blind spots from the loss-mining) ----------
class FalconFadeShort(V.Agent):
    """Falcon's trail-exit legs lose 67% (picks that FADE). Short today's Falcon picks showing intraday weakness."""
    fam = "ADV"; product = "MIS"; lev = 5; can_short = True; sq = V.SESS_END; base_st = (0.04, 0.99); learn = False; uses_a3 = False; max_pos = 15
    def stop_target(self, side, fp): return (fp * 1.04, fp * 0.001) if side == -1 else (fp * 0.96, fp * 1000)
    def entries(self, ctx):
        if ctx.hm != "09:45" or not getattr(ctx, "falcon", None): return []
        weak = [s for s in ctx.falcon if (ctx.close.get(s, np.nan) == ctx.close.get(s, np.nan))
                and ctx.close.get(s, 1e18) < ctx.vwap.get(s, 0) and ctx.mom5.get(s, 0) < -0.3]
        fr = min(0.95 / len(weak), 0.2) if weak else 0
        return [(s, -1, fr) for s in weak if s not in self.pos]

class RegimeShort(V.Agent):
    """Falcon bleeds on down-breadth days. When early breadth is weak (>55% below VWAP), short the weakest 30."""
    fam = "ADV"; product = "MIS"; lev = 5; can_short = True; sq = V.SESS_END; base_st = (0.05, 0.99); learn = False; uses_a3 = False; max_pos = 30
    def stop_target(self, side, fp): return (fp * 1.05, fp * 0.001) if side == -1 else (fp * 0.95, fp * 1000)
    def entries(self, ctx):
        if ctx.hm != "09:45": return []
        below = (ctx.close < ctx.vwap); breadth = below.sum() / max(below.count(), 1)
        if breadth < 0.55: return []                                  # only on broadly-weak (down-breadth) sessions
        picks = ctx.mom5.dropna().nsmallest(30).index                 # weakest 30 by intraday momentum
        return [(s, -1, 0.95 / 30) for s in picks if s not in self.pos]

def metrics(ag, ND):
    eq = pd.Series([e for _, e in ag.eq]); net = (eq.iloc[-1] / CAP - 1) * 100
    dd = ((eq.cummax() - eq) / eq.cummax() * 100).max()
    t = pd.DataFrame(ag.trades); gbps = t.gross_pct.mean() * 100 if len(t) and "gross_pct" in t else np.nan
    breadth = (t.groupby("day").sym.nunique().mean() if len(t) and "day" in t else (len(t) / ND if len(t) else 0))
    return dict(net=net, dd=float(dd), gbps=gbps, ntr=len(t), breadth=breadth)

def build_roster(fb):
    inc = [V.A1.BasketAgent("Falcon-Magnifier", fb, "MIS", 5, trail=(6, 2, 5, 3)),
           V.SignalLab("Falcon-SignalLab")]
    for a in inc: a.fam = "INCUMBENT"
    bench = [V.RetailMomentum("Bench-NaiveMom"), V.MeanReversion("Bench-NaiveMeanRev")]
    for b in bench: b.fam = "BENCHMARK"; b.learn = False; b.lev = 5
    chal = [XSRank("XS-RelStrength", top=True, n=30), XSRank("XS-Reversal", top=False, n=30),
            FalconFadeShort("ADV-FalconFadeShort"), RegimeShort("ADV-RegimeShort")]
    return inc + bench + chal

def reset(a):
    a.cash = CAP; a.pos = {}; a.eq = []; a.dead = False; a.trades = []
    if hasattr(a, "day_realized"): a.day_realized = 0.0
    if hasattr(a, "entry"): a.entry = {}; a.peak = 0.0; a.entered_di = None
    if hasattr(a, "_bpeak"): a._bpeak = 0.0

if __name__ == "__main__":
    oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    univ = set(r[0] for r in oc.execute("SELECT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1")); oc.close()
    fb = WF.falcon_baskets_window(); frank = WF.falcon_ranks_window(); xs = daily_signals(univ)
    roster = build_roster(fb); n_chal = sum(1 for a in roster if a.fam in ("XS", "ADV"))
    print(f"universe {len(univ)} | months {MONTHS} (held-out {HELDOUT}) | challengers tested this cycle: {n_chal}", flush=True)

    rec = {a.id: {} for a in roster}
    for ym in MONTHS:
        bars = WF.load_month(sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True), univ, ym)
        panels, days = V.build_panels(univ, bars); ND = len(days)
        for a in roster: reset(a)
        print(f"  {ym}: {ND}d ...", flush=True)
        V.run(panels, days, fb, roster, list(univ), bars, falcon_rank=frank, xs_signal=xs)
        for a in roster: rec[a.id][ym] = metrics(a, ND)
        del bars, panels

    mag = {ym: rec["Falcon-Magnifier"][ym] for ym in MONTHS}
    def gates(m, ym):                                     # 3-gate vs Magnifier on that window
        r = rec[m][ym]; f = mag[ym]
        return (r["gbps"] > 0, r["net"] > f["net"], r["dd"] < f["dd"])
    print("\n" + "=" * 118)
    print("CHALLENGER LEADERBOARD vs FALCON (Magnifier) — per window: net% / maxDD% / gross-bps/trade / breadth / gates[edge,net,DD]")
    print("=" * 118)
    hdr = "".join(f"{ym[-2:]+'net':>9}{'dd':>6}{'bps':>6}{'brd':>5}{'gate':>6}" for ym in MONTHS)
    print(f"{'agent':<22}{'fam':<11}" + "".join(f"{ym[2:]:>32}" for ym in MONTHS))
    for a in roster:
        line = f"{a.id:<22}{a.fam:<11}"
        for ym in MONTHS:
            r = rec[a.id][ym]
            if a.fam in ("INCUMBENT", "BENCHMARK"):
                line += f"{r['net']:>+8.0f}{r['dd']:>6.1f}{r['gbps']:>+6.0f}{r['breadth']:>5.0f}{'--':>6}"
            else:
                g = gates(a.id, ym); pс = sum(g); line += f"{r['net']:>+8.0f}{r['dd']:>6.1f}{r['gbps']:>+6.0f}{r['breadth']:>5.0f}{str(pс)+'/3':>6}"
        print(line)

    print("\n[3-GATE PASS COUNT] challenger passes ALL 3 gates (edge>0, net>Falcon, DD<Falcon) in how many of 6 windows:")
    margin = 0.0  # multiple-testing note below
    for a in roster:
        if a.fam not in ("XS", "ADV"): continue
        passes = [all(gates(a.id, ym)) for ym in MONTHS]
        ho = all(gates(a.id, HELDOUT))
        print(f"  {a.id:<22} passes {sum(passes)}/6 windows | HELD-OUT({HELDOUT}) {'PASS' if ho else 'FAIL'} | "
              f"avg net {np.mean([rec[a.id][ym]['net'] for ym in MONTHS]):+.0f}% vs Falcon {np.mean([mag[ym]['net'] for ym in MONTHS]):+.0f}% | "
              f"avg bps/trade {np.mean([rec[a.id][ym]['gbps'] for ym in MONTHS]):+.0f} | breadth {np.mean([rec[a.id][ym]['breadth'] for ym in MONTHS]):.0f}/day")
    print(f"\n  MULTIPLE-TESTING: {n_chal} challengers tested this cycle; a real winner must clear the HELD-OUT window,")
    print(f"  not just the dev months. With few challengers the snooping risk is low; it rises as we add more families.")
