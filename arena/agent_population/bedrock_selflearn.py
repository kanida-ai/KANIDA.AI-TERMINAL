"""BEDROCK — self-correcting SWING agent. TRUE WALK-FORWARD (adapts daily, never optimised over the whole period).
Entry SIGNAL is Bedrock's mined rule and is NEVER changed (IP). Everything else it learns each morning using ONLY
trades that CLOSED BEFORE that day:
   1 ENTRY TIMING  (1-min): OPEN / WAIT30 / DIP (0.5% pullback limit) / BRK (first-15min breakout)
   2 CROWDING CAP  : max names to take per day (3/5/10/20/all), best names first by dist_high_120
   3 EXIT RULE     : profit target (none/10/15%) x stop (none/6/10%) x time exit (5/10/15 sessions)
   4 UNIVERSE      : ALL / F&O(N200) / NIFTY50
   5 CAPITAL       : starts Rs5L cash. EARNS the right to borrow up to 2x - only after trailing edge is
                     positive and drawdown is contained; de-levers instantly on a losing streak; repays from profits.
Each trade carries SHADOW outcomes for every configuration, so the daily choice is made purely on realised past
results - no future information is ever consulted. Produces a daily JOURNAL + full TRADE LOG -> Excel.
Window 2026-01..2026-07-10 (limit of 1-minute data). Read-only sources."""
import os, sqlite3, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")

ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
AP = os.path.dirname(os.path.abspath(__file__))
PID = 8787; HOLD_MAX = 15; COST = 0.0015
CAP0 = 500000.0; MAX_LEV = 2.0; MIN_TICKET = 1000.0
LO, HI = "2025-01-01", "2026-07-10"
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
ENTRY_MODES = ["OPEN", "WAIT30", "DIP", "BRK"]
TARGETS = [None, 10.0, 15.0]; STOPS = [None, 6.0, 10.0]; TIMES = [5, 10, 15]
CAPS = [3, 5, 10, 20, 999]; UNIS = ["ALL", "FO", "N50"]
LOOKBACK = 40          # trailing closed trades used to judge a configuration
WARMUP = 15            # closed trades needed before it may deviate from defaults
DEFAULT = ("OPEN", None, None, 15)

print("loading data ...", flush=True)
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
rule = json.loads(pd.read_sql_query("SELECT rule_json FROM falcon_pattern_taxonomy WHERE pattern_id=?", uc, params=(PID,)).rule_json.iloc[0])
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>=? AND trade_date<=?", uc, params=(LO, HI))
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>=? AND trade_date<=? ORDER BY symbol,trade_date",
                       uc, params=(LO, "2026-08-15"))
um = pd.read_sql_query("SELECT symbol,in_nifty50,in_nifty200 FROM universe_master WHERE is_active=1", uc); uc.close()
FO = set(um[um.in_nifty200 == 1].symbol); N50 = set(um[um.in_nifty50 == 1].symbol)

days = sorted(feat.trade_date.unique()); IDX = {d: i for i, d in enumerate(days)}
alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}
BAR = {(r.symbol, r.trade_date): (r.open, r.high, r.low, r.close) for r in oh.itertuples()}

mask = np.ones(len(feat), bool)
for c, o, t in rule: mask &= OPS[o](feat[c].values, t)
SIG = feat.loc[mask, ["trade_date", "symbol", "dist_high_120"]].copy()
print(f"  signals: {len(SIG)}  ({SIG.trade_date.nunique()} signal days)")

# entry day = next session after the signal
SIG["entry_date"] = SIG.trade_date.map(lambda d: alldays[AIDX[d]+1] if AIDX.get(d, 10**9)+1 < len(alldays) else None)
SIG = SIG.dropna(subset=["entry_date"])
# rank within the entry day: closest to its 120d high first
SIG["rank"] = SIG.groupby("entry_date")["dist_high_120"].rank(method="first", ascending=False).astype(int)

# ---- 1-minute bars for the first 30 minutes of each entry day (for entry-timing modes) ----
pairs = SIG[["symbol", "entry_date"]].drop_duplicates()
print(f"  fetching 1-min opening ranges for {len(pairs)} symbol-days ...", flush=True)
mc = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
mins = []
for d, g in pairs.groupby("entry_date"):
    syms = g.symbol.tolist()
    q = ("SELECT symbol,bar_time,open,high,low,close FROM ohlc_1min "
         "WHERE substr(bar_time,1,10)=? AND substr(bar_time,12,5)<='09:45' AND symbol IN (%s)" % ",".join("?"*len(syms)))
    try: mins.append(pd.read_sql_query(q, mc, params=[d]+syms))
    except Exception: pass
mc.close()
M1 = pd.concat(mins, ignore_index=True) if mins else pd.DataFrame(columns=["symbol", "bar_time", "open", "high", "low", "close"])
M1["d"] = M1.bar_time.str[:10]; M1["hm"] = M1.bar_time.str[11:16]
OPENRANGE = {}
for (s, d), g in M1.groupby(["symbol", "d"]):
    g = g.sort_values("bar_time")
    first = g[g.hm <= "09:29"]; win = g[g.hm <= "09:45"]
    if g.empty: continue
    OPENRANGE[(s, d)] = dict(o=float(g.open.iloc[0]), hi15=float(first.high.max()) if len(first) else np.nan,
                             lo30=float(win.low.min()) if len(win) else np.nan,
                             c30=float(win.close.iloc[-1]) if len(win) else np.nan,
                             hi30=float(win.high.max()) if len(win) else np.nan)
print(f"  1-min opening ranges available: {len(OPENRANGE)}")


def entry_price(sym, ed, mode):
    """Entry price under each timing rule; None = no fill that day (a real, honest outcome)."""
    bar = BAR.get((sym, ed))
    if not bar: return None
    o = bar[0]
    r = OPENRANGE.get((sym, ed))
    if mode == "OPEN": return o
    if r is None or not np.isfinite(r.get("c30", np.nan)): return o if mode == "WAIT30" else None
    if mode == "WAIT30": return r["c30"]
    if mode == "DIP":
        lim = r["o"] * 0.995
        return lim if (np.isfinite(r["lo30"]) and r["lo30"] <= lim) else None      # no pullback -> no trade
    if mode == "BRK":
        trig = r["hi15"]
        return trig if (np.isfinite(trig) and np.isfinite(r["hi30"]) and r["hi30"] > trig) else None
    return o


def outcome(sym, ed, ep, target, stop, tmax):
    """Walk daily bars forward from the entry day; stop checked before target (conservative)."""
    i0 = AIDX.get(ed)
    if i0 is None or ep is None or ep <= 0: return None
    for k in range(0, tmax):
        j = i0 + k
        if j >= len(alldays): break
        b = BAR.get((sym, alldays[j]))
        if not b: continue
        _, hi, lo, cl = b
        if stop is not None and lo <= ep*(1-stop/100):
            return (ep*(1-stop/100)/ep - 1)*100 - COST*100, alldays[j], "STOP"
        if target is not None and hi >= ep*(1+target/100):
            return (ep*(1+target/100)/ep - 1)*100 - COST*100, alldays[j], "TARGET"
    j = min(i0+tmax-1, len(alldays)-1)
    b = BAR.get((sym, alldays[j]))
    while b is None and j > i0:
        j -= 1; b = BAR.get((sym, alldays[j]))
    if b is None: return None
    return (b[3]/ep - 1)*100 - COST*100, alldays[j], "TIME"


# ---- shadow grid: every signal, every configuration (computed once; all are PAST facts when used) ----
print("  building shadow outcome grid ...", flush=True)
COMBOS = [(em, tg, sp, tm) for em in ENTRY_MODES for tg in TARGETS for sp in STOPS for tm in TIMES]
rows = []
for r in SIG.itertuples():
    ed = r.entry_date
    eps = {em: entry_price(r.symbol, ed, em) for em in ENTRY_MODES}
    rec = dict(signal_date=r.trade_date, entry_date=ed, symbol=r.symbol, rank=r.rank,
               uni_fo=int(r.symbol in FO), uni_n50=int(r.symbol in N50))
    for (em, tg, sp, tm) in COMBOS:
        ep = eps.get(em)
        out = outcome(r.symbol, ed, ep, tg, sp, tm) if ep else None
        key = f"{em}|{tg}|{sp}|{tm}"
        rec[key] = out[0] if out else np.nan
        if (em, tg, sp, tm) == DEFAULT and out: rec["def_exit"] = out[1]
    rec["ep_map"] = eps
    rows.append(rec)
G = pd.DataFrame(rows)
print(f"  grid: {len(G)} signals x {len(COMBOS)} configs")
G.to_pickle(os.path.join(AP, "_bedrock_grid_full.pkl"))
print("done. grid cached -> _bedrock_grid_full.pkl")
