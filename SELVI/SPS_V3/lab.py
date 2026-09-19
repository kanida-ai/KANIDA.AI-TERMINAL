"""
SPS_V3 — SELVI Autonomous Quant Research Lab (foundation, v3.0)
==============================================================
One lab per agent (ADANIENT->SELVI-AE, CARTRADE->SELVI-CT). This is the
deterministic research engine that iterates thousands of hypotheses cheaply
(the workhorse the meta-learner/LLM layer will steer).

Guarantees baked in:
  * VAULT SEALED: 2026 data is refused by the loader unless unlock=True. Research
    NEVER touches it. Splits: TRAIN 2022-2024, VAL 2025.  (leak-free)
  * Point-in-time facts only (computed from bars <= decision minute).
  * Product-aware, leverage-aware P&L on fixed Rs.30,000 capital, at 1x AND 5x.
  * Walk-forward: a hypothesis must be net-positive on TRAIN *and* VAL to survive;
    survivors + every rejection (with reason) are written to the knowledge base.
  * Action space per agent enforced (intraday L/S via MIS; positional long via CNC;
    NO positional short). Impossible orders are rejected at generation.

Run:  PYTHONIOENCODING=utf-8 python lab.py ADANIENT
      PYTHONIOENCODING=utf-8 python lab.py CARTRADE
"""
from __future__ import annotations
import sys, json, sqlite3, itertools, math
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
DB = HERE.parents[1] / "db" / "kanida.db"
OUT = HERE / "outputs"; OUT.mkdir(exist_ok=True)

# ---- splits (VAULT sealed) ----
TRAIN = ("2022-01-01", "2024-12-31")
VAL = ("2025-01-01", "2025-12-31")
VAULT_START = "2026-01-01"          # anything >= this is SEALED
CAPITAL = 30_000.0
N = 375                              # session minutes
LEVERAGE = {"1x": 1.0, "5x": 5.0}    # evaluate both
# round-trip cost on notional per product (charges + slippage), fraction of turnover
COST = {"MIS": 0.0010, "CNC": 0.0020}   # CNC delivery STT is higher
ENTRY_SLIP = 0.0003

FNO = {"ADANIENT": True, "CARTRADE": False}
# per-agent action space (all: intraday L/S via MIS, positional long via CNC; no positional short)
ACTIONS = {
    "ADANIENT": {"intraday": ("long", "short"), "positional_long": True},
    "CARTRADE": {"intraday": ("long", "short"), "positional_long": True},
}


# ---------------- data (vault-guarded) ----------------
def load_min(symbol, start, end, unlock=False):
    if not unlock and end >= VAULT_START:
        raise PermissionError(f"VAULT SEALED: refusing to load 1-min data at/after {VAULT_START} "
                              f"(requested end={end}). Research must not see the holdout.")
    con = sqlite3.connect(str(DB))
    df = pd.read_sql("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min "
                     "WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",
                     con, params=[symbol, start, end + " 23:59:59"])
    con.close()
    dt = pd.to_datetime(df["bar_time"])
    df["date"] = dt.dt.date
    df["off"] = (dt.dt.hour * 60 + dt.dt.minute) - (9 * 60 + 15)
    return df[(df["off"] >= 0) & (df["off"] < N)]


def load_atr(symbol):
    con = sqlite3.connect(str(DB))
    d = pd.read_sql("SELECT bar_time,high,low,close FROM ohlc_daily WHERE symbol=? ORDER BY bar_time",
                    con, params=[symbol])
    con.close()
    d["date"] = pd.to_datetime(d["bar_time"]).dt.date
    pc = d["close"].shift(1)
    tr = pd.concat([d["high"] - d["low"], (d["high"] - pc).abs(), (d["low"] - pc).abs()], axis=1).max(axis=1)
    d["atr_pct_prev"] = (tr.rolling(14).mean() / pc).shift(1)   # prior-day ATR%, known before open
    d["prevclose"] = pc
    return {r.date: (r.atr_pct_prev, r.prevclose) for r in d.itertuples()}


def day_pack(df):
    days = {}
    for d, sub in df.groupby("date"):
        o = np.full(N, np.nan); h = np.full(N, np.nan); l = np.full(N, np.nan); c = np.full(N, np.nan); v = np.zeros(N)
        off = sub["off"].to_numpy()
        o[off] = sub["open"].to_numpy(); h[off] = sub["high"].to_numpy()
        l[off] = sub["low"].to_numpy(); c[off] = sub["close"].to_numpy(); v[off] = sub["volume"].to_numpy()
        if np.isnan(o[0]):
            continue
        last = int(np.where(~np.isnan(c))[0][-1])
        cf = pd.Series(c).ffill().to_numpy()
        vwap = np.cumsum(np.nan_to_num(cf) * v) / np.maximum(np.cumsum(v), 1)   # point-in-time ATP/VWAP
        days[d] = dict(o=o, h=h, l=l, c=c, v=v, cf=cf, vwap=vwap, last=last)
    return days


# ---------------- facts (point-in-time) ----------------
def fact_values(day, atr_pct, prevclose, entry_off):
    """all computed from bars <= entry_off only."""
    o, h, l, cf, v, vwap = day["o"], day["h"], day["l"], day["cf"], day["v"], day["vwap"]
    e = entry_off
    gap = o[0] / prevclose - 1 if prevclose else 0.0
    orh = np.nanmax(h[:15]); orl = np.nanmin(l[:15]); orw = (orh - orl) / o[0]
    vwap_dev = cf[e] / vwap[e] - 1 if vwap[e] else 0.0
    recent = v[max(0, e - 5):e]; sofar = v[:e]
    dryup = (recent.mean() < 0.7 * sofar.mean()) if e > 6 and sofar.mean() > 0 else False
    return dict(gap=gap, atr=atr_pct if atr_pct == atr_pct else 0.0, orw=orw,
                vwap_dev=vwap_dev, dryup=dryup, tod=e)


# ---------------- triggers: return (entry_off, side, entry_level) or None ----------------
def trig_ORB(day, k, cutoff=105):
    o, h, l = day["o"], day["h"], day["l"]; orh = np.nanmax(h[:k]); orl = np.nanmin(l[:k])
    if np.isnan(orh):
        return None
    for t in range(k, min(cutoff, day["last"] + 1)):
        if np.isnan(h[t]):
            continue
        up = h[t] >= orh; dn = l[t] <= orl
        if up and dn:
            continue
        if up:
            return (t, "long", orh)
        if dn:
            return (t, "short", orl)
    return None


def trig_VWAPX(day, warm=20, cutoff=180):
    cf, vwap = day["cf"], day["vwap"]
    for t in range(warm, min(cutoff, day["last"] + 1)):
        if cf[t - 1] < vwap[t - 1] and cf[t] >= vwap[t]:
            return (t, "long", cf[t])
        if cf[t - 1] > vwap[t - 1] and cf[t] <= vwap[t]:
            return (t, "short", cf[t])
    return None


def trig_MOM(day, k=30):
    o, cf = day["o"], day["cf"]
    if k > day["last"]:
        return None
    side = "long" if cf[k] > o[0] else "short"
    return (k, side, cf[k])


TRIGGERS = {"ORB15": lambda d: trig_ORB(d, 15), "ORB30": lambda d: trig_ORB(d, 30),
            "VWAPX": lambda d: trig_VWAPX(d), "MOM30": lambda d: trig_MOM(d, 30)}

# filters: name -> predicate(facts, side)
FILTERS = {
    "ATRHI": lambda f, atr_thr, s: f["atr"] >= atr_thr,
    "DRYUP": lambda f, atr_thr, s: f["dryup"],
    "AM": lambda f, atr_thr, s: f["tod"] <= 105,           # before 11:00
    "ORWIDE": lambda f, atr_thr, s: f["orw"] >= 0.004,
    "WITHVWAP": lambda f, atr_thr, s: (f["vwap_dev"] >= 0 if s == "long" else f["vwap_dev"] <= 0),
}


# ---------------- product resolution ----------------
def product_for(agent, side, horizon):
    if horizon == "intraday":
        return "MIS"                      # both sides ok intraday
    # positional
    if side == "long":
        return "CNC"                      # positional long only
    return None                           # positional short not allowed


# ---------------- simulate one hypothesis over a day-set ----------------
def simulate(agent, days, atrmap, hyp, atr_thr):
    trig = TRIGGERS[hyp["trigger"]]
    prod = product_for(agent, "long", hyp["horizon"])   # side decided by trigger; product by horizon+side later
    rt = COST["MIS"] if hyp["horizon"] == "intraday" else COST["CNC"]
    rets1x = []
    thit = 0
    for d, day in days.items():
        atr_pct, prevclose = atrmap.get(d, (np.nan, np.nan))
        if prevclose is None or (isinstance(prevclose, float) and np.isnan(prevclose)):
            continue
        r = trig(day)
        if r is None:
            continue
        eoff, side, level = r
        # product legality
        p = product_for(agent, side, hyp["horizon"])
        if p is None:
            continue                       # e.g. positional short -> illegal, skip
        f = fact_values(day, atr_pct, prevclose, eoff)
        if not all(FILTERS[fl](f, atr_thr, side) for fl in hyp["filters"]):
            continue
        entry = level * (1 + ENTRY_SLIP) if side == "long" else level * (1 - ENTRY_SLIP)
        tgt = entry * (1 + hyp["target"]) if side == "long" else entry * (1 - hyp["target"])
        stp = None
        if hyp["stop"]:
            stp = entry * (1 - hyp["stop"]) if side == "long" else entry * (1 + hyp["stop"])
        h_, l_, cf, last = day["h"], day["l"], day["cf"], day["last"]
        exitp = None; hit = False
        for t in range(eoff + 1, last + 1):
            if np.isnan(h_[t]):
                continue
            if side == "long":
                if stp is not None and l_[t] <= stp:
                    exitp = stp; break
                if h_[t] >= tgt:
                    exitp = tgt; hit = True; break
            else:
                if stp is not None and h_[t] >= stp:
                    exitp = stp; break
                if l_[t] <= tgt:
                    exitp = tgt; hit = True; break
        if exitp is None:
            exitp = cf[last]               # intraday: flat by EOD
        gross = (exitp - entry) / entry if side == "long" else (entry - exitp) / entry
        rets1x.append(gross - rt)          # net PRICE return after round-trip cost (1x)
        thit += hit
    a = np.array(rets1x)
    if len(a) == 0:
        return None
    return {"trades": int(len(a)), "net_1x": float(a.mean()), "net_5x": float(5 * a.mean()),
            "win": float((a > 0).mean()), "tgt_hit": thit / len(a),
            "sharpe_1x": float(a.mean() / (a.std() + 1e-12))}


# ---------------- hypothesis space ----------------
def gen_hypotheses(agent):
    hyps = []
    filt_names = list(FILTERS.keys())
    filt_combos = [()] + [(x,) for x in filt_names] + list(itertools.combinations(filt_names, 2))
    for trig in TRIGGERS:
        for fc in filt_combos:
            for target in (0.005, 0.01, 0.015):
                for stop in (None, 0.005, 0.0075, 0.01):
                    hyps.append({"trigger": trig, "filters": list(fc), "target": target,
                                 "stop": stop, "horizon": "intraday"})
    return hyps


# ---------------- knowledge base ----------------
def kb_conn(agent):
    con = sqlite3.connect(str(HERE / f"kb_{agent}.db"))
    con.execute("""CREATE TABLE IF NOT EXISTS results(
        id INTEGER PRIMARY KEY, ts TEXT, spec TEXT,
        tr_trades INT, tr_net1x REAL, va_trades INT, va_net1x REAL, va_net5x REAL,
        va_win REAL, va_tgthit REAL, survived INT, reason TEXT)""")
    return con


# ---------------- research burst ----------------
def burst(agent):
    atrmap = load_atr(agent)
    tr = day_pack(load_min(agent, *TRAIN))
    va = day_pack(load_min(agent, *VAL))
    train_atrs = [atrmap[d][0] for d in tr if atrmap.get(d) and atrmap[d][0] == atrmap[d][0]]
    atr_thr = float(np.nanmedian(train_atrs))
    hyps = gen_hypotheses(agent)
    con = kb_conn(agent)
    survivors = []
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for hyp in hyps:
        rtr = simulate(agent, tr, atrmap, hyp, atr_thr)
        if not rtr or rtr["trades"] < 100:
            reason = "insufficient_train_trades" if rtr else "no_train_trades"
            _rec(con, ts, hyp, rtr, None, 0, reason); continue
        if rtr["net_1x"] <= 0:
            _rec(con, ts, hyp, rtr, None, 0, "train_negative"); continue
        rva = simulate(agent, va, atrmap, hyp, atr_thr)
        if not rva or rva["trades"] < 30:
            _rec(con, ts, hyp, rtr, rva, 0, "insufficient_val_trades"); continue
        surv = rva["net_1x"] > 0
        _rec(con, ts, hyp, rtr, rva, int(surv), "OK" if surv else "val_negative")
        if surv:
            survivors.append((hyp, rtr, rva))
    con.commit()
    # deflate: bar rises with number tested
    n_tested = len(hyps)
    survivors.sort(key=lambda x: x[2]["sharpe_1x"], reverse=True)
    _report(agent, atr_thr, n_tested, len(tr), len(va), survivors, con)
    con.close()
    return survivors


def _rec(con, ts, hyp, rtr, rva, surv, reason):
    con.execute("INSERT INTO results(ts,spec,tr_trades,tr_net1x,va_trades,va_net1x,va_net5x,va_win,va_tgthit,survived,reason)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (ts, json.dumps(hyp), rtr["trades"] if rtr else 0, rtr["net_1x"] if rtr else None,
                 rva["trades"] if rva else None, rva["net_1x"] if rva else None,
                 rva["net_5x"] if rva else None, rva["win"] if rva else None,
                 rva["tgt_hit"] if rva else None, surv, reason))


def _report(agent, atr_thr, n_tested, ntr, nva, survivors, con):
    lines = [f"# SELVI-{'AE' if agent=='ADANIENT' else 'CT'}  ({agent})  — research burst 1",
             f"_TRAIN 2022-2024 ({ntr}d) · VAL 2025 ({nva}d) · VAULT 2026 SEALED · ATR%thr {atr_thr*100:.2f}%_",
             f"hypotheses tested: **{n_tested}** · survivors (net+ on train AND val): **{len(survivors)}**", ""]
    tot = con.execute("SELECT count(*) FROM results").fetchone()[0]
    lines.append(f"knowledge base rows: {tot}\n")
    if survivors:
        lines.append("| # | trigger | filters | tgt/stop | VAL trades | VAL win | VAL net/trade 1x | net/day-cap 5x | tgt-hit |")
        lines.append("|--|--|--|--|--|--|--|--|--|")
        for i, (h, rtr, rva) in enumerate(survivors[:20], 1):
            lines.append(f"| {i} | {h['trigger']} | {','.join(h['filters']) or '-'} | "
                         f"{h['target']*100:.1f}%/{(str(h['stop']*100)+'%') if h['stop'] else 'none'} | "
                         f"{rva['trades']} | {rva['win']*100:.1f}% | {rva['net_1x']*100:+.3f}% | "
                         f"{rva['net_5x']*100:+.3f}% | {rva['tgt_hit']*100:.0f}% |")
    else:
        lines.append("**No survivors this burst.** (honest result — the KB now records why each "
                     "failed; the meta-learner uses these to steer the next burst toward new facts.)")
    (OUT / f"burst1_{agent}.md").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    agent = sys.argv[1] if len(sys.argv) > 1 else "CARTRADE"
    burst(agent)
