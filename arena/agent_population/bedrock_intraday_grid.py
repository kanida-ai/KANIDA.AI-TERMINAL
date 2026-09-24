"""BEDROCK — INTRADAY shadow grid. Same mined entry signal; enter and EXIT THE SAME SESSION (no overnight).
For every signal, precompute the outcome under each config = ENTRY timing x intraday TARGET x intraday STOP,
using the FULL-DAY 1-minute path (stop checked before target, intrabar; else exit at that day's close). Net 0.15%.
ENTRY : OPEN(9:15) / WAIT30(9:45 close) / DIP(0.5% pullback limit in first 30m) / BRK(first-15m breakout)
TARGET: none(close) / +1% / +2% / +3%     STOP: none / -1% / -2%
-> _bedrock_intraday_grid.pkl . Read-only. Window 2025-01..2026-07-10 (1-min data limit)."""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
AP = os.path.dirname(os.path.abspath(__file__)); PID = 8787; COST = 0.15
LO, HI = "2025-01-01", "2026-07-10"
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
ENTRY_MODES = ["OPEN", "SPLIT", "WAIT30", "DIP", "BRK"]; TARGETS = [None, 1.0, 2.0, 3.0]; STOPS = [None, 1.0, 2.0]

print("loading ...", flush=True)
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
rule = json.loads(pd.read_sql_query("SELECT rule_json FROM falcon_pattern_taxonomy WHERE pattern_id=?", uc, params=(PID,)).rule_json.iloc[0])
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>=? AND trade_date<=?", uc, params=(LO, HI))
oh = pd.read_sql_query("SELECT symbol,trade_date,open FROM ohlc_daily WHERE trade_date>=? AND trade_date<=? ORDER BY trade_date", uc, params=(LO, "2026-08-15"))
um = pd.read_sql_query("SELECT symbol,in_nifty50,in_nifty200 FROM universe_master WHERE is_active=1", uc); uc.close()
FO = set(um[um.in_nifty200 == 1].symbol); N50 = set(um[um.in_nifty50 == 1].symbol)
alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}
mask = np.ones(len(feat), bool)
for c, o, t in rule: mask &= OPS[o](feat[c].values, t)
SIG = feat.loc[mask, ["trade_date", "symbol", "dist_high_120"]].copy()
SIG["entry_date"] = SIG.trade_date.map(lambda d: alldays[AIDX[d]+1] if AIDX.get(d, 10**9)+1 < len(alldays) else None)
SIG = SIG.dropna(subset=["entry_date"])
SIG["rank"] = SIG.groupby("entry_date")["dist_high_120"].rank(method="first", ascending=False).astype(int)
print(f"  signals: {len(SIG)}  ({SIG.entry_date.nunique()} entry days)")

COMBOS = [(em, tg, sp) for em in ENTRY_MODES for tg in TARGETS for sp in STOPS]
mc = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
rows = []
groups = list(SIG.groupby("entry_date"))
print(f"  walking {len(groups)} entry days of full 1-min bars ...", flush=True)
for gi, (d, g) in enumerate(groups):
    syms = g.symbol.tolist()
    q = ("SELECT symbol,bar_time,open,high,low,close FROM ohlc_1min WHERE substr(bar_time,1,10)=? AND symbol IN (%s)"
         % ",".join("?"*len(syms)))
    try: bars = pd.read_sql_query(q, mc, params=[d]+syms)
    except Exception: bars = pd.DataFrame()
    bars["hm"] = bars.bar_time.str[11:16] if len(bars) else pd.Series(dtype=str)
    bybar = {s: b.sort_values("bar_time") for s, b in bars.groupby("symbol")} if len(bars) else {}
    for r in g.itertuples():
        b = bybar.get(r.symbol)
        rec = dict(entry_date=d, signal_date=r.trade_date, symbol=r.symbol, rank=r.rank,
                   uni_fo=int(r.symbol in FO), uni_n50=int(r.symbol in N50), ep_map={})
        if b is None or b.empty:
            for (em, tg, sp) in COMBOS: rec[f"{em}|{tg}|{sp}"] = np.nan
            rows.append(rec); continue
        o915 = float(b.open.iloc[0])
        first = b[b.hm <= "09:29"]; win30 = b[b.hm <= "09:45"]
        hi15 = float(first.high.max()) if len(first) else np.nan
        lo30 = float(win30.low.min()) if len(win30) else np.nan
        hi30 = float(win30.high.max()) if len(win30) else np.nan
        c30 = float(win30.close.iloc[-1]) if len(win30) else np.nan
        # entry price + the post-entry intraday window per mode
        eps = {}; postwin = {}
        eps["OPEN"] = o915; postwin["OPEN"] = b[b.hm > "09:15"]
        _b916 = b[b.hm == "09:16"]
        o916 = float(_b916.open.iloc[0]) if len(_b916) else np.nan
        eps["SPLIT"] = (0.5*o915 + 0.5*o916) if np.isfinite(o916) else o915   # 50:50 @9:15 + 9:16 blended cost
        postwin["SPLIT"] = b[b.hm > "09:16"]
        eps["WAIT30"] = c30 if np.isfinite(c30) else o915; postwin["WAIT30"] = b[b.hm > "09:45"]
        eps["DIP"] = (o915*0.995) if (np.isfinite(lo30) and lo30 <= o915*0.995) else None; postwin["DIP"] = b[b.hm > "09:45"]
        eps["BRK"] = hi15 if (np.isfinite(hi15) and np.isfinite(hi30) and hi30 > hi15) else None; postwin["BRK"] = b[b.hm > "09:45"]
        rec["ep_map"] = eps
        for em in ENTRY_MODES:
            ep = eps[em]; pw = postwin[em]
            if ep is None or ep <= 0 or pw.empty:
                for tg in TARGETS:
                    for sp in STOPS: rec[f"{em}|{tg}|{sp}"] = np.nan
                continue
            pe_hi = float(pw.high.max()); pe_lo = float(pw.low.min()); pe_cl = float(pw.close.iloc[-1])
            for tg in TARGETS:
                for sp in STOPS:
                    if sp is not None and pe_lo <= ep*(1-sp/100): ret = -sp                      # stop first (conservative)
                    elif tg is not None and pe_hi >= ep*(1+tg/100): ret = tg                      # target
                    else: ret = (pe_cl/ep - 1)*100                                                 # same-day close
                    rec[f"{em}|{tg}|{sp}"] = ret - COST
        rows.append(rec)
    if (gi+1) % 40 == 0: print(f"    {gi+1}/{len(groups)} days", flush=True)
mc.close()
G = pd.DataFrame(rows)
G.to_pickle(os.path.join(AP, "_bedrock_intraday_grid.pkl"))
print(f"done. intraday grid: {len(G)} signals x {len(COMBOS)} configs -> _bedrock_intraday_grid.pkl")
# quick sanity: same-day-close, OPEN entry, distribution
k = "OPEN|None|None"; v = G[k].dropna()
print(f"  sanity (OPEN, same-day close): {len(v)} trades · avg {v.mean():+.2f}% · win {(v>0).mean()*100:.0f}%")
