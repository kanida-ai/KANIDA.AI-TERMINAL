"""BEDROCK-STYLE DISCOVERY — mine NON-WEEKLY intraday patterns on the clean panel, with the discipline the BTST
failure taught us:
  * NON-WEEKLY features only (leak-free, Bedrock family).
  * Score each pattern by its TRADEABLE DAILY-BASKET return (day-weighted mean of the day's basket), NOT the
    trade-weighted per-signal edge (that was the illusion that made BTST look good and lose).
  * NESTED walk-forward: MINE 2024-05..2025-06, VALIDATE 2025-07..2025-12, TEST 2026. Never select on 2026.
  * A pattern survives only if its daily-basket clears cost on BOTH mine AND validate. Then we report 2026.
Structure: INTRADAY same-day, 09:45 entry (ID_09:45), net 0.15%. Then a survivor PORTFOLIO -> 2026 monthly @1x/5x.
Vectorized. Read-only."""
import os, sqlite3, itertools, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"; UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
OPS = {">": np.greater, "<": np.less}; TGT = "ID_09:45"; MINDAYS = 25; MIN_BASKET_RET = 0.05; CAP0 = 5e5
MINE = ("2024-05-13", "2025-06-30"); VAL = ("2025-07-01", "2025-12-31"); TEST = ("2026-01-01", "2026-07-10")

print("building clean NON-WEEKLY panel + intraday outcome ...", flush=True)
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-05-13' AND trade_date<='2026-07-09'", con); con.close()
OUT = pd.read_pickle(os.path.join(AP, "_mine_outcomes.pkl"))
ad = sorted(OUT.trade_date.unique()); AIDX = {d: i for i, d in enumerate(ad)}
feat["entry_date"] = feat.trade_date.map(lambda d: ad[AIDX[d]+1] if AIDX.get(d, 10**9)+1 < len(ad) else None)
M = feat.merge(OUT[["symbol", "trade_date", TGT]].rename(columns={"trade_date": "entry_date"}), on=["symbol", "entry_date"], how="inner").dropna(subset=[TGT])
# NON-WEEKLY numeric features only
FEATURES = [c for c in feat.columns if c not in ("symbol", "trade_date", "entry_date", "id") and not c.startswith("weekly_") and M[c].dtype.kind in "fi"]
X = M[FEATURES].values.astype(np.float64); y = M[TGT].values.astype(np.float64); ed = M.entry_date.values
def win_mask(w): return (ed >= w[0]) & (ed <= w[1])
mMine, mVal, mTest = win_mask(MINE), win_mask(VAL), win_mask(TEST)
print(f"  rows {len(M)} · features(non-weekly) {len(FEATURES)} · mine {mMine.sum()} val {mVal.sum()} test {mTest.sum()}")

# --- tradeable DAILY-BASKET return (day-weighted) for a boolean pattern mask within a window ---
edser = pd.Series(ed); yser = pd.Series(y)
def basket(mask, wmask):
    sel = mask & wmask
    if sel.sum() < MINDAYS: return None
    df = pd.DataFrame({"d": ed[sel], "r": y[sel]})
    daily = df.groupby("d").r.mean()
    if len(daily) < MINDAYS: return None
    return daily.mean(), (daily > 0).mean()*100, len(daily), int(sel.sum())     # day-weighted basket return, day-win%, #days, #trades
base_mine = pd.DataFrame({"d": ed[mMine], "r": y[mMine]}).groupby("d").r.mean().mean()
base_test = pd.DataFrame({"d": ed[mTest], "r": y[mTest]}).groupby("d").r.mean().mean()
print(f"  baseline daily-basket: mine {base_mine:+.3f}%  test(2026) {base_test:+.3f}%\n  mining ...", flush=True)

# --- mine single + 2-feature conditions; keep those clearing cost on MINE AND VAL ---
singles = []
for j, f in enumerate(FEATURES):
    col = X[:, j]
    for thr in np.unique(np.round(np.nanpercentile(col, [5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95]), 4)):
        for op in (">", "<"):
            m = OPS[op](col, thr)
            bm = basket(m, mMine)
            if bm and bm[0] >= MIN_BASKET_RET:
                bv = basket(m, mVal)
                if bv and bv[0] > 0:                       # survives validation too
                    singles.append(dict(rule=[(f, op, float(thr))], mask=m, mine=bm[0], val=bv[0]))
singles.sort(key=lambda c: -min(c["mine"], c["val"]))
survivors = list(singles)
for a, b in itertools.combinations(singles[:30], 2):
    if a["rule"][0][0] == b["rule"][0][0]: continue
    m = a["mask"] & b["mask"]; bm = basket(m, mMine)
    if bm and bm[0] >= MIN_BASKET_RET:
        bv = basket(m, mVal)
        if bv and bv[0] > 0: survivors.append(dict(rule=a["rule"]+b["rule"], mask=m, mine=bm[0], val=bv[0]))
survivors.sort(key=lambda c: -min(c["mine"], c["val"]))
print(f"  survivors (clear cost on BOTH mine & validate): {len(survivors)}")
if not survivors:
    print("\n  >>> HONEST RESULT: no non-weekly intraday pattern survives the nested test. Bedrock stays the sole edge.")
    raise SystemExit

# --- report survivors' TRUE OOS (2026) daily-basket — the honest test ---
print(f"\n  {'#':<3}{'mine%':>8}{'val%':>8}{'TEST26%':>9}{'test_win':>9}{'days':>6}  rule")
rows = []
for i, c in enumerate(survivors[:15]):
    bt = basket(c["mask"], mTest)
    if not bt: continue
    r = " AND ".join(f"{f}{op}{th:.2f}" for f, op, th in c["rule"])
    print(f"  {i+1:<3}{c['mine']:>+8.2f}{c['val']:>+8.2f}{bt[0]:>+9.2f}{bt[1]:>8.0f}%{bt[2]:>6}  {r}")
    rows.append(dict(rule=r, mine=round(c["mine"], 3), val=round(c["val"], 3), test26=round(bt[0], 3), test_win=round(bt[1], 0), test_days=bt[2]))
oos_pos = sum(1 for c in survivors if (basket(c["mask"], mTest) or (0,))[0] and basket(c["mask"], mTest)[0] > base_test)
print(f"\n  of {len(survivors)} survivors, {oos_pos} also beat the 2026 baseline OOS (honest generalisation rate)")

# --- survivor PORTFOLIO: union daily basket, compounded Rs5L, 2026 monthly @1x/5x ---
U = np.any([c["mask"] for c in survivors], axis=0)          # union of all survivor patterns
d2026 = pd.DataFrame({"d": ed[U & mTest], "r": y[U & mTest]})
db = d2026.groupby("d").r.mean().reset_index(); db["m"] = db.d.str[:7]
def compound(lev):
    eq = CAP0; eqs = []
    for _, r in db.iterrows(): eq *= (1 + r.r*lev/100.0); eqs.append((r.d, eq))
    E = pd.DataFrame(eqs, columns=["d", "eq"]); E["m"] = E.d.str[:7]
    dd = ((E.eq.cummax()-E.eq)/E.eq.cummax()*100).max(); mo = E.groupby("m").eq.last()
    mo = pd.concat([pd.Series({"2026-00": CAP0}), mo]).pct_change().dropna()*100
    return (E.eq.iloc[-1]/CAP0-1)*100, dd, mo
print("\n" + "="*60 + "\n  SURVIVOR PORTFOLIO — INTRADAY 9:45, 2026 OOS (Rs5L)\n" + "="*60)
for lev in [1, 5]:
    tot, dd, mo = compound(lev)
    print(f"\n  {lev}x MIS:  {tot:+.1f}%   maxDD {dd:.1f}%   ({db.r.mean():+.3f}%/day basket, {(db.r>0).mean()*100:.0f}% up-days)")
    print("     monthly: " + "  ".join(f"{m[-2:]}:{v:+.0f}%" for m, v in mo.items()))
out = os.path.join(ROOT, "docs", "ops", "BEDROCK_STYLE_DISCOVERY.xlsx")
pd.DataFrame(rows).to_excel(out, index=False)
print(f"\nExcel -> {out}")
