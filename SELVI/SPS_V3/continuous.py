"""
SPS_V3 continuous research runner (time-boxed chunk, resumable via KB + champion.json).
Expands the intraday hypothesis space, DEDUPES by realised trade-set, applies a
multiple-testing / deflated bar (t-stat >= 2.5 on validation), and tracks the champion.
Each invocation runs for ~budget seconds then EXITS (so the outer loop reports & relaunches).

Run:  python continuous.py <AGENT> <budget_seconds>
"""
import sys, time, json, hashlib, itertools
from pathlib import Path
from datetime import datetime
import numpy as np, sqlite3
import lab
from lab import (load_min, load_atr, day_pack, fact_values, product_for,
                 COST, ENTRY_SLIP, TRAIN, VAL, N, HERE, OUT)

# ---- expanded triggers (leak-free) ----
def _orb(day, k, wstart, cutoff):
    o, h, l = day["o"], day["h"], day["l"]; orh = np.nanmax(h[:k]); orl = np.nanmin(l[:k])
    if np.isnan(orh):
        return None
    for t in range(max(k, wstart), min(cutoff, day["last"] + 1)):
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

def _dryrev(day, warm=20, cutoff=200):
    cf, v = day["cf"], day["v"]
    for t in range(warm, min(cutoff, day["last"] + 1)):
        recent = v[max(0, t - 5):t]; sofar = v[:t]
        if sofar.mean() <= 0 or recent.mean() >= 0.7 * sofar.mean():
            continue
        lo = np.nanmin(cf[:t]); hi = np.nanmax(cf[:t])
        if cf[t] <= lo * 1.002:
            return (t, "long", cf[t])        # dry-up at the lows -> accumulation
        if cf[t] >= hi * 0.998:
            return (t, "short", cf[t])       # dry-up at the highs -> distribution
    return None

def _vwaprev(day, ext=0.008, warm=20, cutoff=210):
    cf, vwap = day["cf"], day["vwap"]
    for t in range(warm, min(cutoff, day["last"] + 1)):
        dev = cf[t] / vwap[t] - 1 if vwap[t] else 0
        if dev >= ext:
            return (t, "short", cf[t])        # stretched above VWAP -> fade
        if dev <= -ext:
            return (t, "long", cf[t])
    return None

TRIGGERS = dict(lab.TRIGGERS)
TRIGGERS.update({
    "ORB45": lambda d: _orb(d, 45, 45, 150),
    "ORBlate": lambda d: _orb(d, 30, 60, 210),
    "DRYREV": lambda d: _dryrev(d),
    "VWAPREV": lambda d: _vwaprev(d),
})
FILTERS = lab.FILTERS

TARGETS = (0.005, 0.0075, 0.01, 0.015, 0.02)
STOPS = (None, 0.003, 0.005, 0.0075, 0.01)


def simulate(agent, days, atrmap, hyp, atr_thr):
    trig = TRIGGERS[hyp["trigger"]]; rt = COST["MIS"]
    rets, sig, thit = [], [], 0
    for d, day in days.items():
        atr_pct, prevclose = atrmap.get(d, (np.nan, np.nan))
        if prevclose is None or (isinstance(prevclose, float) and np.isnan(prevclose)):
            continue
        r = trig(day)
        if r is None:
            continue
        eoff, side, level = r
        if product_for(agent, side, "intraday") is None:
            continue
        f = fact_values(day, atr_pct, prevclose, eoff)
        if not all(FILTERS[fl](f, atr_thr, side) for fl in hyp["filters"]):
            continue
        entry = level * (1 + ENTRY_SLIP) if side == "long" else level * (1 - ENTRY_SLIP)
        tgt = entry * (1 + hyp["target"]) if side == "long" else entry * (1 - hyp["target"])
        stp = (entry * (1 - hyp["stop"]) if side == "long" else entry * (1 + hyp["stop"])) if hyp["stop"] else None
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
            exitp = cf[last]
        g = (exitp - entry) / entry if side == "long" else (entry - exitp) / entry
        net = g - rt
        rets.append(net); sig.append((str(d), int(eoff), side, round(net, 5))); thit += hit  # sig includes EXIT via return
    a = np.array(rets)
    if len(a) == 0:
        return None
    tstat = float(a.mean() / (a.std() + 1e-12) * np.sqrt(len(a)))
    return {"trades": int(len(a)), "net_1x": float(a.mean()), "net_5x": float(5 * a.mean()),
            "win": float((a > 0).mean()), "tgt_hit": thit / len(a), "tstat": tstat,
            "sig": hashlib.md5(json.dumps(sorted(sig)).encode()).hexdigest()}


def space():
    fnames = list(FILTERS.keys())
    combos = [()] + [(x,) for x in fnames] + list(itertools.combinations(fnames, 2))
    for trig in TRIGGERS:
        for fc in combos:
            for tg in TARGETS:
                for st in STOPS:
                    yield {"trigger": trig, "filters": list(fc), "target": tg, "stop": st, "horizon": "intraday"}


def run(agent, budget):
    atrmap = load_atr(agent)
    tr = day_pack(load_min(agent, *TRAIN)); va = day_pack(load_min(agent, *VAL))
    atrs = [atrmap[d][0] for d in tr if atrmap.get(d) and atrmap[d][0] == atrmap[d][0]]
    atr_thr = float(np.nanmedian(atrs))
    con = sqlite3.connect(str(HERE / f"kb_cont_{agent}.db"))
    con.execute("""CREATE TABLE IF NOT EXISTS results(
        id INTEGER PRIMARY KEY, ts TEXT, spec TEXT, tr_trades INT, tr_net1x REAL,
        va_trades INT, va_net1x REAL, va_net5x REAL, va_win REAL, va_tgthit REAL,
        survived INT, reason TEXT)""")
    con.execute("CREATE TABLE IF NOT EXISTS seen(sig TEXT PRIMARY KEY)")
    seen = {r[0] for r in con.execute("SELECT sig FROM seen").fetchall()}
    cpath = HERE / f"champion_{agent}.json"
    champ = json.load(open(cpath)) if cpath.exists() else None
    # skip hypotheses already tested (resume): count existing kb rows
    done = con.execute("SELECT count(*) FROM results").fetchone()[0]
    t0 = time.time(); tested = 0; new_surv = 0; idx = 0
    for hyp in space():
        idx += 1
        if idx <= done:                       # resume: skip already-tested prefix
            continue
        if time.time() - t0 > budget:
            break
        tested += 1
        rtr = simulate(agent, tr, atrmap, hyp, atr_thr)
        if not rtr or rtr["trades"] < 100 or rtr["net_1x"] <= 0:
            lab._rec(con, "", hyp, rtr, None, 0, "train_fail"); continue
        rva = simulate(agent, va, atrmap, hyp, atr_thr)
        if not rva or rva["trades"] < 40:
            lab._rec(con, "", hyp, rtr, rva, 0, "val_thin"); continue
        # dedup by realised validation trade-set
        if rva["sig"] in seen:
            lab._rec(con, "", hyp, rtr, rva, 0, "duplicate"); continue
        seen.add(rva["sig"]); con.execute("INSERT OR IGNORE INTO seen(sig) VALUES(?)", (rva["sig"],))
        # multiple-testing / deflated bar: val positive AND t-stat >= 2.5
        surv = rva["net_1x"] > 0 and rva["tstat"] >= 2.5
        lab._rec(con, "", hyp, rtr, rva, int(surv), "OK" if surv else "below_tstat")
        if surv:
            new_surv += 1
            score = rva["tstat"]
            if champ is None or score > champ["val"]["tstat"]:
                champ = {"agent": agent, "spec": hyp, "train": rtr, "val": rva,
                         "found": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                json.dump(champ, open(cpath, "w"), indent=2)
    con.commit()
    total = con.execute("SELECT count(*) FROM results").fetchone()[0]
    surv_total = con.execute("SELECT count(*) FROM results WHERE survived=1").fetchone()[0]
    status = {
        "agent": agent, "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "kb_rows": total, "tested_this_chunk": tested, "new_survivors_this_chunk": new_surv,
        "distinct_survivors_total": surv_total, "space_covered": f"{min(idx,total_space())}/{total_space()}",
        "champion": None if not champ else {
            "trigger": champ["spec"]["trigger"], "filters": champ["spec"]["filters"],
            "target": champ["spec"]["target"], "stop": champ["spec"]["stop"],
            "val_trades": champ["val"]["trades"], "val_win": champ["val"]["win"],
            "val_net_1x": champ["val"]["net_1x"], "val_net_5x": champ["val"]["net_5x"],
            "val_tstat": champ["val"]["tstat"], "val_tgt_hit": champ["val"]["tgt_hit"]},
    }
    json.dump(status, open(HERE / f"status_{agent}.json", "w"), indent=2)
    con.close()
    print(json.dumps(status, indent=2))


def total_space():
    fnames = list(FILTERS.keys())
    n_combos = 1 + len(fnames) + len(fnames) * (len(fnames) - 1) // 2
    return len(TRIGGERS) * n_combos * len(TARGETS) * len(STOPS)


if __name__ == "__main__":
    run(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 300)
