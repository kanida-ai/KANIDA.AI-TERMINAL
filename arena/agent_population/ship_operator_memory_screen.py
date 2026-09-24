"""SHIPPABLE: Operator Memory Screen — a daily screen that surfaces the operator's RECURRING watchlist names
currently sitting in a buyable pullback, ranked. Leak-free (memory uses only STRICTLY-PRIOR operator picks).

Honest scope: this is a RECALL / decision-support tool for the ~64% of the operator's winners that are recurring
names. It is NOT a standalone profit engine (its top-15 open->close basket is flat-to-slightly-negative OOS);
fresh-name discovery is a separate channel. It reproduces the operator's working watchlist, in pullback context.

For each signal_date S it ranks symbols by a recency-decayed count of the operator's own prior picks, restricted
to names in a buyable state today (sret<=2, not extended), tie-broken by pullback depth. Outputs an Excel:
  'screen'  : daily top-15 (signal_date, trade_date, rank, symbol, mem_score, sret, wcvs20, dist_high_20,
              rsi_14, tier, hit_win = was actually an operator profit next day [backtest column])
  'summary' : per-day recall@15 of operator profit-picks + the top-15 open->close 5x basket.
Usage: python ship_operator_memory_screen.py [--start 2024-12-01 --end 2025-01-31]
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "scripts")); import falcon_signal_replay as FR
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
LOG = os.path.join(ROOT, "arena", "agent_population", "operator_ranked_log_dec24_jan25.tsv")
WEEKLY = ["weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"]
WIN_THR = 0.3; MEM_LOOK, MEM_DECAY, TOPN = 20, 0.90, 15
START = sys.argv[sys.argv.index("--start") + 1] if "--start" in sys.argv else "2024-12-01"
END = sys.argv[sys.argv.index("--end") + 1] if "--end" in sys.argv else "2025-01-31"
OUT = os.path.join(os.path.expanduser("~"), "Downloads", "OPERATOR_MEMORY_SCREEN.xlsx")


def _ok(v): return v is not None and not (isinstance(v, float) and v != v)
def tier_of(sr, tr):
    if _ok(sr) and sr > 10: return "AVOID"
    if _ok(sr) and sr <= 2 and _ok(tr) and tr < 0.9: return "ENTERPRISE-Dryup"
    if _ok(sr) and sr <= 2: return "GOLD"
    if _ok(sr) and sr <= 5: return "STANDARD"
    return "STANDARD-weak"

picks = pd.read_csv(LOG, sep="\t", dtype={"trade_date": str})
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT symbol,trade_date,weekly_close_vs_sma20,dist_high_20,rsi_14 FROM falcon_features "
                         "WHERE trade_date>='2024-11-15' AND trade_date<='2025-01-31'", con)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,close,volume,high,low FROM ohlc_daily "
                       "WHERE trade_date>='2024-06-01' AND trade_date<='2025-01-31' ORDER BY symbol,trade_date", con)
con.close()
# PIT weekly_close_vs_sma20 overrides the stored (leaky) col
o2 = oh.copy(); dt = pd.to_datetime(o2.trade_date)
o2["wk"] = dt.dt.isocalendar().year.astype(int) * 100 + dt.dt.isocalendar().week.astype(int)
rec = []; TF = {}; RET_OC = {}
for s, g in o2.groupby("symbol", sort=False):
    g = g.sort_values("trade_date").reset_index(drop=True)
    c = g.close.values.astype(float); v = g.volume.values.astype(float); op = g.open.values.astype(float)
    wb = g.groupby("wk").agg(wc=("close", "last")).reset_index(); wb["sm"] = wb.wc.rolling(20).mean().shift(1)
    sm = g.wk.map(dict(zip(wb.wk, wb.sm))).values
    rec.append(pd.DataFrame(dict(symbol=s, trade_date=g.trade_date.values,
        wcvs_pit=np.where((sm == sm) & (sm > 0), (c / sm - 1) * 100, np.nan))))
    pc = np.roll(c, 1); pc[0] = np.nan; sret = (c / pc - 1) * 100
    av20 = pd.Series(v).rolling(20).mean().values; av3 = pd.Series(v).rolling(3).mean().values
    tr3 = np.where(av20 > 0, av3 / av20, np.nan)
    for i, d in enumerate(g.trade_date.values):
        TF[(s, d)] = (sret[i], tr3[i]); RET_OC[(s, d)] = ((c[i] - op[i]) / op[i] * 100) if op[i] > 0 else np.nan
FC = feat.merge(pd.concat(rec, ignore_index=True), on=["symbol", "trade_date"], how="left")
cal = sorted(oh.trade_date.unique()); cidx = {d: i for i, d in enumerate(cal)}
prev = lambda d: cal[cidx[d] - 1] if (d in cidx and cidx[d] - 1 >= 0) else None
nxt = lambda d: cal[cidx[d] + 1] if (d in cidx and cidx[d] + 1 < len(cal)) else None
# operator pick history -> per-symbol prior pick indices (leak-free memory)
sym_pickidx = {}; win_key = set()
for _, p in picks.iterrows():
    s = prev(p.trade_date)
    if s is None: continue
    sym_pickidx.setdefault(p.symbol, []).append(cidx[s])
    if p.stock_ret_pct > WIN_THR: win_key.add((s, p.symbol))
for k in sym_pickidx: sym_pickidx[k] = sorted(sym_pickidx[k])
def memory(sym, i):
    return sum(MEM_DECAY ** (i - j) for j in sym_pickidx.get(sym, []) if 0 < i - j <= MEM_LOOK)

screen_days = [d for d in cal if START <= d <= END]
rows = []
for S in screen_days:
    i = cidx[S]; fd = FC[FC.trade_date == S]
    if fd.empty: continue
    T = nxt(S); cand = []
    for _, r in fd.iterrows():
        sym = r.symbol; m = memory(sym, i)
        if m <= 0: continue                       # only recurring watchlist names
        tf = TF.get((sym, S), (np.nan, np.nan)); sr = tf[0]
        if not (_ok(sr) and sr <= 2): continue    # buyable pullback state (not extended today)
        cand.append(dict(symbol=sym, mem=m, sret=sr, wcvs=r.wcvs_pit,
                         dist_high_20=pd.to_numeric(r.dist_high_20, errors="coerce"),
                         rsi_14=pd.to_numeric(r.rsi_14, errors="coerce"), tier=tier_of(sr, tf[1])))
    cand.sort(key=lambda c: (-c["mem"], c["wcvs"] if _ok(c["wcvs"]) else 1e9))
    for rank, c in enumerate(cand[:TOPN], 1):
        hit = int((S, c["symbol"]) in win_key)
        rows.append(dict(signal_date=S, trade_date=T, rank=rank, symbol=c["symbol"],
                         mem_score=round(c["mem"], 3), sret=round(c["sret"], 2),
                         weekly_close_vs_sma20=(round(c["wcvs"], 2) if _ok(c["wcvs"]) else np.nan),
                         dist_high_20=(round(c["dist_high_20"], 2) if _ok(c["dist_high_20"]) else np.nan),
                         rsi_14=(round(c["rsi_14"], 1) if _ok(c["rsi_14"]) else np.nan),
                         tier=c["tier"], hit_win=hit, ret_oc_next=RET_OC.get((c["symbol"], T), np.nan)))
SC = pd.DataFrame(rows)
# summary: recall of profit-picks + basket
wins_per_day = {}
for (s, sym) in win_key: wins_per_day.setdefault(s, set()).add(sym)
srows = []
for S in screen_days:
    day = SC[SC.signal_date == S]
    if day.empty: continue
    ws = wins_per_day.get(S, set()); got = len(set(day.symbol) & ws)
    srows.append(dict(signal_date=S, trade_date=nxt(S), screened=len(day),
                      operator_profit_picks=len(ws), captured_in_top15=got,
                      recall_pct=(round(got / len(ws) * 100, 0) if ws else np.nan),
                      top15_ret_oc_5x=round(np.nanmean(day.ret_oc_next.values) * 5, 2)))
SUM = pd.DataFrame(srows)
os.makedirs(os.path.dirname(OUT), exist_ok=True)
with pd.ExcelWriter(OUT, engine="openpyxl") as xw:
    SC.to_excel(xw, "screen", index=False); SUM.to_excel(xw, "summary", index=False)
tot_w = sum(len(v) for v in wins_per_day.values() if list(wins_per_day)[0])
capt = sum(r["captured_in_top15"] for r in srows); totw = sum(r["operator_profit_picks"] for r in srows)
print(f"days {len(SUM)} · overall recall@15 of profit-picks {capt}/{totw} = {capt/totw*100:.0f}%")
print(f"top15 open->close 5x basket: mean {SUM.top15_ret_oc_5x.mean():+.2f}%/day  median {SUM.top15_ret_oc_5x.median():+.2f}%/day")
print(f"saved -> {OUT}")
