"""V9 LEAK TEST — port the operator's V9 pattern-union multi-lane scoring formula EXACTLY, then run it two ways:
  (A) LEAKED weekly  = full Mon-Fri week aggregation merged onto every day (V9 as written: to_period('W-FRI'))
  (B) LEAK-FREE weekly = week-to-date (cummax/cummin within iso-week up to the signal day) — no future days
Same patterns, same weights, same family lanes, same final_score. Isolates exactly what the weekly leak buys.
Reports Dec-fit + Jan-validation recall@15 of profitable teacher picks + open->close 5x replay for each variant.
"""
import os, sys, sqlite3, warnings, math
from collections import Counter, defaultdict
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
LOG = os.path.join(ROOT, "arena", "agent_population", "operator_ranked_log_dec24_jan25.tsv")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
WIN_THR = 0.0; MIN_WINNER_FIRES = 3; TOP_N = 15

picks = pd.read_csv(LOG, sep="\t", dtype={"trade_date": str})
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
pats = FR.load_patterns(con); PM = {p["pattern_id"]: p for p in pats}
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily "
                       "WHERE trade_date>='2023-05-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date", con)
con.close()
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
prev = lambda d: cal[cidx[d] - 1] if (d in cidx and cidx[d] - 1 >= 0) else None
nxt = lambda d: cal[cidx[d] + 1] if (d in cidx and cidx[d] + 1 < len(cal)) else None
RET_OC = {(r.symbol, r.trade_date): ((r.close - r.open) / r.open * 100 if r.open > 0 else np.nan) for r in oh.itertuples()}

# ---- weekly features: two variants ----
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date)
o2["iso"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
o2["wfri"] = dt.dt.to_period("W-FRI").astype(str)
pit_rows = []; leak_rows = []
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    c = g.close.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float)
    # (B) week-to-date within iso week
    hi = g.groupby("iso").high.cummax().values; lo = g.groupby("iso").low.cummin().values
    wbp = g.groupby("iso").agg(wc=("close", "last"), wh=("high", "max")).reset_index()
    wbp["sm"] = wbp.wc.rolling(20).mean().shift(1); wbp["ph"] = wbp.wh.rolling(20).max().shift(1)
    smp = g.iso.map(dict(zip(wbp.iso, wbp.sm))).values; php = g.iso.map(dict(zip(wbp.iso, wbp.ph))).values
    pit_rows.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(hi > lo, (c - lo) / (hi - lo), np.nan),
        weekly_range_pct=np.where(c > 0, (hi - lo) / c * 100, np.nan),
        weekly_close_vs_sma20=np.where((smp == smp) & (smp > 0), (c / smp - 1) * 100, np.nan),
        weekly_breakout_20w=np.where(php == php, (c > php).astype(float), np.nan))))
    # (A) full W-FRI week merged onto every day (LEAK)
    wg = g.groupby("wfri").agg(whigh=("high", "max"), wlow=("low", "min"), wclose=("close", "last")).reset_index()
    wg["wsma20"] = wg.wclose.rolling(20, min_periods=6).mean()
    wg["ph"] = wg.whigh.rolling(20, min_periods=6).max().shift(1)
    mh = dict(zip(wg.wfri, wg.whigh)); ml = dict(zip(wg.wfri, wg.wlow)); mc = dict(zip(wg.wfri, wg.wclose))
    ms = dict(zip(wg.wfri, wg.wsma20)); mp = dict(zip(wg.wfri, wg.ph))
    wh_ = g.wfri.map(mh).values; wl_ = g.wfri.map(ml).values; wc_ = g.wfri.map(mc).values
    ws_ = g.wfri.map(ms).values; wp_ = g.wfri.map(mp).values
    leak_rows.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        weekly_close_loc=np.where(wh_ > wl_, (wc_ - wl_) / (wh_ - wl_), np.nan),
        weekly_range_pct=np.where(wc_ > 0, (wh_ - wl_) / wc_ * 100, np.nan),
        weekly_close_vs_sma20=np.where((ws_ == ws_) & (ws_ > 0), (wc_ / ws_ - 1) * 100, np.nan),
        weekly_breakout_20w=np.where(wp_ == wp_, (wc_ > wp_).astype(float), np.nan))))
WK = {"LEAK-FREE (week-to-date)": pd.concat(pit_rows, ignore_index=True),
      "LEAKED (V9 full W-FRI)": pd.concat(leak_rows, ignore_index=True)}
base_feat = feat.drop(columns=[w for w in WEEKLY if w in feat.columns])

def family(rule):
    feats = [f for f, _, _ in rule]; ops = {f: op for f, op, _ in rule}
    txt = set(feats)
    if any(f.startswith("dist_high_252") or f.startswith("dist_high_120") for f in feats) or ("weekly_breakout_20w" in txt) or (ops.get("weekly_close_vs_sma20") == ">"): return "fresh_breakout"
    if "n_sub_3_range_7d" in txt or "n_sub_2_5_range_7d" in txt: return "compression"
    if "n_sub_75v_7d" in txt or "n_sub_75v_20d" in txt or "vol_5d_vs_20d" in txt: return "dryup"
    if ops.get("rsi_14") == "<=" or ops.get("dist_high_10") == "<=": return "pullback_reversal"
    if ops.get("roc_60") == ">" or ops.get("roc_20") == ">" or any(f.startswith("slope_sma") for f in feats): return "continuation"
    return "other"
FAM = {p["pattern_id"]: family(p["rule"]) for p in pats}

def run_variant(name, wk):
    FCpit = base_feat.merge(wk, on=["symbol", "trade_date"], how="left")
    # panel: entry_date in Dec+Jan
    rows = []
    tp = {}  # (entry_date, symbol) -> profitable(1/0), selected(1/0)
    for _, p in picks.iterrows():
        tp[(p.trade_date, p.symbol)] = (1 if p.stock_ret_pct > WIN_THR else 0)
    sig_days = sorted({prev(t) for t in picks.trade_date.unique() if prev(t)})
    fire = {}  # (sd) -> (syms, F matrix over pats, X)
    panel = []
    for sd in sig_days:
        fd = FCpit[FCpit.trade_date == sd]
        if fd.empty: continue
        syms = fd.symbol.values; X = np.full((len(syms), len(FR.FEATURE_COLS)), np.nan)
        for j, col in enumerate(FR.FEATURE_COLS):
            if col in fd.columns: X[:, j] = pd.to_numeric(fd[col], errors="coerce").values
        yr = int(sd[:4]); ed = nxt(sd)
        Fm = np.zeros((len(syms), len(pats)))
        for k, pp in enumerate(pats):
            if int(pp["mined_year"]) < yr: Fm[:, k] = FR.rule_mask(pp["rule"], X)
        fire[sd] = (syms, Fm)
        for ii, sym in enumerate(syms):
            prof = tp.get((ed, sym), 0)
            panel.append(dict(sd=sd, ed=ed, symbol=sym, i=ii, profitable=prof,
                              selected=1 if (ed, sym) in tp else 0))
    P = pd.DataFrame(panel); base_rate = P.profitable.mean()
    # learn_union on Dec winners
    decmask = (P.ed.str[:7] == "2024-12")
    winners = P[decmask & (P.profitable == 1)]
    weights = {}; fam_of = {}
    for k, pp in enumerate(pats):
        wf = sum(fire[r.sd][1][r.i, k] for r in winners.itertuples())
        if wf < MIN_WINNER_FIRES: continue
        decP = P[decmask]
        fired = np.array([fire[r.sd][1][r.i, k] for r in decP.itertuples()], bool)
        if fired.sum() == 0: continue
        winprec = decP.profitable.values[fired].mean()
        lift = max(0.1, float(pp["oos_lift"] or 0))
        weights[k] = math.log1p(wf) * lift * max(0.1, winprec / max(1e-6, base_rate)); fam_of[k] = FAM[pp["pattern_id"]]
    fams = sorted(set(fam_of.values()))
    # score candidates
    P["pscore"] = 0.0
    for f in fams: P["lane_" + f] = 0.0
    pv = np.zeros(len(P)); lanev = {f: np.zeros(len(P)) for f in fams}
    for ri, r in enumerate(P.itertuples()):
        Fr = fire[r.sd][1][r.i]
        for k, w in weights.items():
            if Fr[k]:
                pv[ri] += w; lanev[fam_of[k]][ri] += w
    P["pscore"] = pv
    for f in fams: P["lane_" + f] = lanev[f]
    lane_cols = ["lane_" + f for f in fams]
    for c in ["pscore"] + lane_cols:
        P[c + "_r"] = P.groupby("ed")[c].rank(pct=True)
    P["mlb"] = (P[lane_cols] > 0).sum(axis=1)
    P["final"] = P["pscore_r"] * 2.0 + P["mlb"] * 0.25
    for c in lane_cols: P["final"] += P[c + "_r"] * 0.35
    # select + recall + replay
    def rep(month):
        sub = P[P.ed.str[:7] == month]; hit = tot = 0; bask = []
        for ed, g in sub.groupby("ed"):
            g = g.sort_values("final", ascending=False); top = g.head(TOP_N)
            wins = g[g.profitable == 1].symbol; inset = set(top.symbol)
            tot += len(wins); hit += sum(s in inset for s in wins)
            bask.append(np.nanmean([RET_OC.get((s, ed), np.nan) for s in top.symbol]))
        return (hit / tot * 100 if tot else 0), np.nanmean(bask) * 5, len(weights)
    dr, db, nw = rep("2024-12"); jr, jb, _ = rep("2025-01")
    print(f"\n[{name}]  union patterns={nw}")
    print(f"    DEC (fit)        recall@15 {dr:4.0f}%   replay 5x {db:+.2f}%/day")
    print(f"    JAN (validation) recall@15 {jr:4.0f}%   replay 5x {jb:+.2f}%/day")

print("================ V9 SCORING FORMULA — LEAKED vs LEAK-FREE ================")
for name, wk in WK.items():
    run_variant(name, wk)
