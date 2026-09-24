"""SHORT Falcon — rank the sell list (falcon_short_promoted) + intraday SHORT backtest.
Ranking mirrors falcon_signal_replay (mined_year<signal_year, sum short-lift, rank by avg_lift).
Backtest: SHORT top-N at 09:15, basket trail on capital, 5x, sq-off 15:29, Jan-Jul 2026. READ-ONLY."""
import os, sys, json, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts"))
from falcon_signal_replay import FEATURE_COLS, rule_mask
import flow_paper_engine as fpe
PROD = os.path.join(ROOT, "data", "db", "kanida_universe.db"); RND = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
CAP, LEV, EOD, MINF = 100000, 5, "15:29", 3
# load SHORT promoted patterns
rc = sqlite3.connect("file:" + RND.replace("\\", "/") + "?mode=ro", uri=True)
PATS = [dict(mined_year=int(my), lift=lift, rule=[(f, op, th) for f, op, th in json.loads(rj)])
        for my, lift, rj in rc.execute("SELECT mined_year,avg_oos_year_lift_pp,rule_json FROM falcon_short_promoted")]
rc.close()
pc = sqlite3.connect("file:" + PROD.replace("\\", "/") + "?mode=ro", uri=True)
def rank_short(date, topn=15):
    yr = int(date[:4]); elig = [p for p in PATS if p["mined_year"] < yr]
    rows = pc.execute(f"SELECT symbol,{','.join(FEATURE_COLS)} FROM falcon_features WHERE trade_date=?", (date,)).fetchall()
    if not rows: return []
    syms = [r[0] for r in rows]; X = np.array([[v if v is not None else np.nan for v in r[1:]] for r in rows])
    fire = np.zeros(len(syms), np.int32); score = np.zeros(len(syms))
    for p in elig:
        m = rule_mask(p["rule"], X)
        if m.any(): fire += m.astype(int); score += m * p["lift"]
    c = [(syms[i], int(fire[i]), score[i] / max(fire[i], 1)) for i in range(len(syms)) if fire[i] >= MINF]
    c.sort(key=lambda x: -x[2])
    return [(i + 1, s, nf, round(al, 2)) for i, (s, nf, al) in enumerate(c[:topn])]
# trading calendar
tdates = [r[0] for r in pc.execute("SELECT DISTINCT trade_date FROM ohlc_daily WHERE trade_date BETWEEN '2025-12-15' AND '2026-07-31' ORDER BY trade_date")]
nextday = {tdates[i]: tdates[i + 1] for i in range(len(tdates) - 1)}
# 07-13 sell list
print("===== SHORT (SELL) list — signal 2026-07-13 -> trade 07-14 (top-15) =====")
print(f"{'rk':>3}  {'symbol':<12}{'n_fires':>8}{'avg_lift':>9}")
for rk, s, nf, al in rank_short("2026-07-13", 15): print(f"{rk:>3}. {s:<12}{nf:>8}{al:>9}")
# build Jan-Jul short picks
picks = {}
for d in [x for x in tdates if "2025-12-30" <= x <= "2026-07-09"]:
    r = rank_short(d, 15)
    if r: picks[d] = [(rk, s) for rk, s, nf, al in r]
# 1-min bars for shorted names
ocon = sqlite3.connect("file:" + RND.replace("\\", "/") + "?mode=ro", uri=True); bars = {}
need = set()
for d, ps in picks.items():
    ed = nextday.get(d)
    if ed and ed[:7] >= "2026-01":
        for rk, s in ps: need.add((s, ed))
for (s, ed) in need:
    b = ocon.execute("SELECT substr(bar_time,12,5),open,high,low,close FROM ohlc_1min WHERE symbol=? AND substr(bar_time,1,10)=? AND bar_time>=? ORDER BY bar_time", (s, ed, f"{ed} 09:15")).fetchall()
    if len(b) >= 30 and b[0][1]: bars[(s, ed)] = b
ocon.close(); pc.close()
def costp(entry, ex): return fpe.cost("CASH", entry, ex, CAP * LEV / entry) / CAP * 100
def short_day(symbols, ed, ARM, FLOOR, GIVE, STOP=3):
    stk = [(bars[(s, ed)][0][1], {x[0]: x[1:] for x in bars[(s, ed)]}) for s in symbols if (s, ed) in bars]
    if not stk: return None
    E = [x[0] for x in stk]; M = [x[1] for x in stk]; mins = sorted(set().union(*[set(m) for m in M])); last = list(E); peak = 0.0
    for hm in mins:
        for i, m in enumerate(M):
            if hm in m and m[hm][3]: last[i] = m[hm][3]
        pnl = np.mean([E[i] / last[i] - 1 for i in range(len(E))]) * 100 * LEV   # SHORT: profit when price falls
        armed = peak >= ARM; stop = max(FLOOR, peak - GIVE) if armed else -STOP; mc = np.mean([costp(E[i], last[i]) for i in range(len(E))])
        if hm >= EOD: return pnl - mc
        if pnl <= stop: return stop - mc
        peak = max(peak, pnl)
    return pnl - mc
def basket_days(topn):
    out = {}
    for d, ps in picks.items():
        ed = nextday.get(d)
        if ed and ed[:7] >= "2026-01" and ed <= "2026-07-31":
            out.setdefault(ed, [s for rk, s in ps if rk <= topn])
    return out
def total(top, cfg, L=None):
    L0 = LEV
    mret = {}
    for ed, sy in basket_days(top).items():
        r = short_day(sy, ed, *cfg)
        if r is not None: mret.setdefault(ed[:7], []).append(r)
    t5 = sum(sum(v) for v in mret.values()); pm = sum(1 for v in mret.values() if sum(v) > 0)
    return t5, pm, len(mret), mret
print("\n===== SHORT backtest config sweep (Jan-Jul, 5x) — Top-15 short =====")
best = None
for a in (4, 5, 6):
    for fl in (1, 2):
        t5, pm, nm, _ = total(15, (a, fl, 5));
        if best is None or t5 > best[0]: best = (t5, a, fl, pm, nm)
print(f"  best Top-15 short: arm{best[1]}/floor{best[2]}/give5/stop3 -> {best[0]:+.0f}% ({best[3]}/{best[4]} months)")
cfg = (best[1], best[2], 5)
print(f"\n===== MONTHLY: Top-3 short & Top-15 short (arm{best[1]}/floor{best[2]}/give5/stop3, 5x) =====")
for top in (3, 15):
    _, _, _, mret = total(top, cfg)
    tt = sum(sum(v) for v in mret.values()); pm = sum(1 for v in mret.values() if sum(v) > 0)
    print(f"  --- Top-{top} short ---")
    for m in sorted(mret): print(f"    {m}: {sum(mret[m]):+7.1f}%  ({sum(1 for x in mret[m] if x>0)}/{len(mret[m])} days+)")
    print(f"    TOTAL 5x {tt:+.1f}%   +months {pm}/{len(mret)}")
