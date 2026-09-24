"""BEDROCK-STYLE DISCOVERY v2 — mine on the STRUCTURED intraday outcome (9:45 entry + 1% stop, same-day close),
the structure that actually gives Bedrock its edge. Non-weekly features, day-weighted tradeable basket, nested
walk-forward (mine 2024-05..2025-06, validate 2025-07..12, test 2026). 1% stop approximated by the day's low
(conservative). Vectorized. Read-only."""
import os, sqlite3, itertools, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
OPS = {">": np.greater, "<": np.less}; COST = 0.15; STOPS = [1.0, 2.0]; MINDAYS = 25; MIN_BASKET_RET = 0.05; CAP0 = 5e5
MINE = ("2024-05-13", "2025-06-30"); VAL = ("2025-07-01", "2025-12-31"); TEST = ("2026-01-01", "2026-07-10")

print("loading features + 9:45 entry + daily OHLC ...", flush=True)
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-05-13' AND trade_date<='2026-07-09'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,low,close FROM ohlc_daily WHERE trade_date>='2024-05-13' AND trade_date<='2026-07-10'", con); con.close()
mc = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
e = pd.read_sql_query("SELECT symbol, substr(bar_time,1,10) d, open e945 FROM ohlc_1min WHERE substr(bar_time,12,5)='09:45' AND substr(bar_time,1,10) BETWEEN '2024-05-13' AND '2026-07-10'", mc); mc.close()
E = e.merge(oh.rename(columns={"trade_date": "d"}), on=["symbol", "d"], how="inner")
# structured outcomes: 9:45 entry + X% stop (day-low proxy) else close, net cost
for sp in STOPS:
    hit = E.low.values <= E.e945.values*(1-sp/100)
    E[f"ID945s{int(sp)}"] = np.where(E.e945.values > 0, np.where(hit, -sp, (E.close.values/E.e945.values-1)*100) - COST, np.nan)
E = E.rename(columns={"d": "entry_date"})
ad = sorted(set(oh.trade_date)); AIDX = {d: i for i, d in enumerate(ad)}
feat["entry_date"] = feat.trade_date.map(lambda d: ad[AIDX[d]+1] if AIDX.get(d, 10**9)+1 < len(ad) else None)
FEATURES = [c for c in feat.columns if c not in ("symbol", "trade_date", "entry_date", "id") and not c.startswith("weekly_") and feat[c].dtype.kind in "fi"]

def run_for(target):
    M = feat.merge(E[["symbol", "entry_date", target]], on=["symbol", "entry_date"], how="inner").dropna(subset=[target])
    X = M[FEATURES].values.astype(np.float64); y = M[target].values.astype(np.float64); ed = M.entry_date.values
    wm = lambda w: (ed >= w[0]) & (ed <= w[1]); mMine, mVal, mTest = wm(MINE), wm(VAL), wm(TEST)
    def basket(mask, wmask):
        sel = mask & wmask
        if sel.sum() < MINDAYS: return None
        daily = pd.DataFrame({"d": ed[sel], "r": y[sel]}).groupby("d").r.mean()
        return (daily.mean(), (daily > 0).mean()*100, len(daily)) if len(daily) >= MINDAYS else None
    bT = pd.DataFrame({"d": ed[mTest], "r": y[mTest]}).groupby("d").r.mean().mean()
    singles = []
    for j, f in enumerate(FEATURES):
        col = X[:, j]
        for thr in np.unique(np.round(np.nanpercentile(col, [5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95]), 4)):
            for op in (">", "<"):
                m = OPS[op](col, thr); bm = basket(m, mMine)
                if bm and bm[0] >= MIN_BASKET_RET:
                    bv = basket(m, mVal)
                    if bv and bv[0] > 0: singles.append(dict(rule=[(f, op, float(thr))], mask=m, mine=bm[0], val=bv[0]))
    surv = list(singles)
    singles.sort(key=lambda c: -min(c["mine"], c["val"]))
    for a, b in itertools.combinations(singles[:30], 2):
        if a["rule"][0][0] == b["rule"][0][0]: continue
        m = a["mask"] & b["mask"]; bm = basket(m, mMine)
        if bm and bm[0] >= MIN_BASKET_RET:
            bv = basket(m, mVal)
            if bv and bv[0] > 0: surv.append(dict(rule=a["rule"]+b["rule"], mask=m, mine=bm[0], val=bv[0]))
    surv.sort(key=lambda c: -min(c["mine"], c["val"]))
    return M, y, ed, mTest, surv, bT

for sp in STOPS:
    tgt = f"ID945s{int(sp)}"; M, y, ed, mTest, surv, bT = run_for(tgt)
    print(f"\n{'='*70}\nSTRUCTURED intraday · 9:45 entry · {int(sp)}% stop · same-day  (test26 baseline {bT:+.3f}%)")
    print(f"  survivors (clear cost on mine AND validate): {len(surv)}")
    if not surv: print("  -> none survive."); continue
    print(f"  {'#':<3}{'mine%':>8}{'val%':>8}{'TEST26%':>9}{'win':>6}  rule")
    oos_beat = 0
    for i, c in enumerate(surv[:12]):
        sel = c["mask"] & mTest
        daily = pd.DataFrame({"d": ed[sel], "r": y[sel]}).groupby("d").r.mean() if sel.sum() >= MINDAYS else None
        if daily is None: continue
        t = daily.mean(); oos_beat += (t > bT)
        r = " AND ".join(f"{f}{op}{th:.2f}" for f, op, th in c["rule"])
        print(f"  {i+1:<3}{c['mine']:>+8.2f}{c['val']:>+8.2f}{t:>+9.2f}{(daily>0).mean()*100:>5.0f}%  {r}")
    # survivor portfolio 2026 @1x/5x
    U = np.any([c["mask"] for c in surv], axis=0); sel = U & mTest
    db = pd.DataFrame({"d": ed[sel], "r": y[sel]}).groupby("d").r.mean().reset_index(); db["m"] = db.d.str[:7]
    for lev in [1, 5]:
        eq = CAP0; eqs = []
        for _, r in db.iterrows(): eq *= (1+r.r*lev/100); eqs.append((r.d, eq))
        Eq = pd.DataFrame(eqs, columns=["d", "eq"]); Eq["m"] = Eq.d.str[:7]
        dd = ((Eq.eq.cummax()-Eq.eq)/Eq.eq.cummax()*100).max(); mo = Eq.groupby("m").eq.last()
        mo = pd.concat([pd.Series({"2026-00": CAP0}), mo]).pct_change().dropna()*100
        print(f"  PORTFOLIO {lev}x: {(Eq.eq.iloc[-1]/CAP0-1)*100:+.1f}%  maxDD {dd:.1f}%  ({db.r.mean():+.3f}%/day)  monthly " + " ".join(f"{m[-2:]}:{v:+.0f}%" for m, v in mo.items()))
