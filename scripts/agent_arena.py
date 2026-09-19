"""
KANIDA ALGORITHM 1 — keystone: the CANONICAL HYPOTHESIS engine + persistent AGENT IDENTITY + MARKET WORLDS.

An AGENT = an identity (name, personality, specialty) wrapping ONE canonical hypothesis:
   WHEN [trigger] happens to [scope] under [context], WHAT is [outcome] over [horizon], vs [baseline], after [costs].
The engine evaluates any such hypothesis LEAK-FREE (signal uses only info <= t; outcome measured forward),
produces an EVIDENCE CARD, runs the agent across 5 MARKET WORLDS for robustness, writes a human-voiced
JOURNAL, and persists the agent's public profile + history to agents_arena/<id>.json.

Run: python scripts/agent_arena.py
"""
from __future__ import annotations
import sys, json, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import daily_core as DC
KDB = str(ROOT / "db" / "kanida.db")
AGDIR = ROOT / "agents_arena"; AGDIR.mkdir(exist_ok=True)
COST = 0.30 / 100

# ---------------- data + market regime ----------------
_F = {}
def data():
    if not _F:
        f, _ = DC.wide_all()
        o, c = f["o"], f["c"]
        _F["o"], _F["c"], _F["v"] = o, c, f["v"]
        con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
        nf = pd.read_sql_query("SELECT bar_time,close FROM ohlc_daily WHERE symbol='NIFTY 50' ORDER BY bar_time", con); con.close()
        nf["d"] = pd.to_datetime(nf["bar_time"].str[:10]); nf = nf.set_index("d")["close"].reindex(c.index).ffill()
        ma200 = nf.rolling(200).mean(); rv20 = nf.pct_change().rolling(20).std() * np.sqrt(252)
        breadth = (c > c.rolling(50).mean()).mean(axis=1)                 # % of stocks above their 50DMA
        mvol = _F["v"].sum(axis=1)                                        # aggregate market volume
        _F["nf"] = nf; _F["above200"] = nf > ma200; _F["rv"] = rv20; _F["breadth"] = breadth; _F["mvol"] = mvol
    return _F

# ---------------- MARKET WORLDS (date masks over the same history) ----------------
def worlds():
    F = data(); idx = F["c"].index
    rv = F["rv"]; above = F["above200"]; mvol = F["mvol"]
    hv = rv > rv.quantile(0.75)                                          # high-volatility days
    lowliq = mvol < mvol.quantile(0.25)                                  # low-liquidity days
    unseen = (idx >= "2018-01-01") & (idx < "2020-01-01")               # a fixed held-out slice
    return {
        "Normal":       (above.values & ~hv.values & pd.Series(~unseen, index=idx).values),
        "High-Vol":     hv.values,
        "Bear":         (~above.values),
        "Low-Liquidity": lowliq.values,
        "Unseen 18-19": unseen,
    }

# ---------------- trigger primitives (date x symbol boolean, point-in-time at close t) ----------------
def trig(name, **p):
    F = data(); o, c = F["o"], F["c"]
    if name == "gap_up":     return (o / c.shift(1) - 1) >= p["thr"]
    if name == "gap_down":   return (o / c.shift(1) - 1) <= -p["thr"]
    if name == "streak_up":  return (c.diff() > 0).rolling(p["n"]).sum() == p["n"]
    if name == "breakout":   return c > c.shift(1).rolling(p["n"]).max()
    if name == "oversold":   return (c / c.shift(p["n"]) - 1) <= -p["thr"]
    raise ValueError(name)

# ---------------- outcome + evaluation ----------------
def fwd_net(direction, horizon):
    F = data(); o, c = F["o"], F["c"]
    raw = c.shift(-horizon) / o.shift(-1) - 1                            # enter next open, exit close t+h
    dr = raw if direction == "long" else -raw
    return dr - COST

def evaluate(agent, world_mask=None):
    ev = trig(agent["trigger"]["type"], **agent["trigger"]["params"])
    net = fwd_net(agent["outcome"]["direction"], agent["outcome"]["horizon"])
    ev, net = ev.align(net, join="inner")
    m = ev.values & np.isfinite(net.values)
    if world_mask is not None:
        m = m & world_mask[:, None]
    r = net.values[m]
    if len(r) < 20:
        return None
    base = net.values[(np.isfinite(net.values)) & (world_mask[:, None] if world_mask is not None else True)]
    base = base[np.isfinite(base)]
    wins = r[r > 0]; loss = r[r <= 0]
    return {"n": int(len(r)), "win": round(float((r > 0).mean()) * 100, 1),
            "avg": round(float(r.mean()) * 100, 2), "med": round(float(np.median(r)) * 100, 2),
            "pf": round(float(wins.sum() / -loss.sum()), 2) if loss.sum() < 0 else None,
            "edge": round(float(r.mean() - base.mean()) * 100, 2),      # vs same-world universe baseline
            "expct": round(float(r.mean()) * 100, 2)}

def today_events(agent):
    ev = trig(agent["trigger"]["type"], **agent["trigger"]["params"])
    last = ev.iloc[-1]; return sorted(last[last.fillna(False)].index.tolist())

# ---------------- AGENTS (identity + canonical hypothesis) ----------------
AGENTS = [
    {"id": "falconfade", "name": "FalconFade", "personality": "contrarian", "specialty": "gap-fade short",
     "voice": "sharp, sceptical of hype",
     "trigger": {"type": "gap_up", "params": {"thr": 0.03}}, "scope": "all",
     "outcome": {"direction": "short", "horizon": 1}, "baseline": "universe-same-world"},
    {"id": "breakoutbull", "name": "BreakoutBull", "personality": "aggressive", "specialty": "breakout continuation",
     "voice": "bold, momentum-hungry",
     "trigger": {"type": "breakout", "params": {"n": 20}}, "scope": "all",
     "outcome": {"direction": "long", "horizon": 3}, "baseline": "universe-same-world"},
    {"id": "diphunter", "name": "DipHunter", "personality": "patient contrarian", "specialty": "oversold reversal",
     "voice": "calm, buys fear",
     "trigger": {"type": "oversold", "params": {"n": 3, "thr": 0.08}}, "scope": "all",
     "outcome": {"direction": "long", "horizon": 3}, "baseline": "universe-same-world"},
]


# ---------------- BUILDER GRID: precompute every user-composable agent, offline & leak-free ----------------
TRIGGER_OPTIONS = [
    ("gap_up", "2", {"thr": 0.02}, "Gap up ≥2%"), ("gap_up", "3", {"thr": 0.03}, "Gap up ≥3%"),
    ("gap_up", "5", {"thr": 0.05}, "Gap up ≥5%"),
    ("gap_down", "2", {"thr": 0.02}, "Gap down ≥2%"), ("gap_down", "3", {"thr": 0.03}, "Gap down ≥3%"),
    ("breakout", "20", {"n": 20}, "New 20-day high"), ("breakout", "60", {"n": 60}, "New 60-day high"),
    ("breakout", "120", {"n": 120}, "New 120-day high"),
    ("oversold", "3-8", {"n": 3, "thr": 0.08}, "3-day −8% dip"), ("oversold", "5-10", {"n": 5, "thr": 0.10}, "5-day −10% dip"),
    ("streak_up", "3", {"n": 3}, "3 green days"), ("streak_up", "5", {"n": 5}, "5 green days"),
]


def _card(evv, netv, mask):
    fin = np.isfinite(netv); m = evv & fin
    if mask is not None: m = m & mask[:, None]
    r = netv[m]
    if len(r) < 20: return None
    base = netv[fin & (mask[:, None] if mask is not None else np.ones_like(fin))]
    wins = r[r > 0]; loss = r[r <= 0]
    return {"n": int(len(r)), "win": round(float((r > 0).mean()) * 100, 1),
            "expct": round(float(r.mean()) * 100, 2), "med": round(float(np.median(r)) * 100, 2),
            "pf": round(float(wins.sum() / -loss.sum()), 2) if loss.sum() < 0 else None,
            "edge": round(float(r.mean() - base.mean()) * 100, 2)}


def build_grid():
    F = data(); W = worlds(); o, c = F["o"], F["c"]
    fwd = {h: (c.shift(-h) / o.shift(-1) - 1).values for h in [1, 2, 3, 5]}
    grid = {}
    for tname, pkey, params, label in TRIGGER_OPTIONS:
        evv = trig(tname, **params).values
        for direction in ["long", "short"]:
            for h in [1, 2, 3, 5]:
                netv = (fwd[h] if direction == "long" else -fwd[h]) - COST
                grid[f"{tname}|{pkey}|{direction}|{h}"] = {
                    "label": label, "overall": _card(evv, netv, None),
                    "worlds": {w: _card(evv, netv, mask) for w, mask in W.items()}}
    (AGDIR / "_grid.json").write_text(json.dumps(grid), encoding="utf-8")
    opts = [{"v": f"{t}|{p}", "label": lbl} for t, p, _, lbl in TRIGGER_OPTIONS]
    (AGDIR / "_grid_opts.json").write_text(json.dumps(opts), encoding="utf-8")
    print(f"  built builder grid: {len(grid)} composable agents -> agents_arena/_grid.json")


def journal(agent, overall, wmap):
    picks = today_events(agent); d = data()["c"].index[-1].date()
    verb = "shorted" if agent["outcome"]["direction"] == "short" else "bought"
    setup = agent["specialty"]
    best = max(wmap.items(), key=lambda kv: (kv[1]["edge"] if kv[1] else -9))
    worst = min(wmap.items(), key=lambda kv: (kv[1]["edge"] if kv[1] else 9))
    lines = [f"[{agent['name']} · {agent['personality']}]  {d}",
             f"  Today I {verb} {len(picks)} {setup} setups" + (f": {', '.join(picks[:8])}{'…' if len(picks)>8 else ''}." if picks else " — nothing met my rules; I sit out."),
             f"  My edge (all history): {overall['n']} trades, {overall['win']}% win, {overall['expct']:+.2f}%/trade, {overall['edge']:+.2f}% vs the market.",
             f"  I'm strongest in the '{best[0]}' world (+{best[1]['edge'] if best[1] else 0:.2f}% edge) and weakest in '{worst[0]}' ({worst[1]['edge'] if worst[1] else 0:+.2f}%)."]
    return "\n".join(lines)


def main():
    W = worlds()
    print("=" * 78)
    print("KANIDA A1 — Agent Arena: canonical hypothesis + identity + Market Worlds")
    print("=" * 78)
    ranks = {w: [] for w in W}
    profiles = {}
    for a in AGENTS:
        overall = evaluate(a)
        wmap = {w: evaluate(a, mask) for w, mask in W.items()}
        profiles[a["id"]] = (a, overall, wmap)
        for w in W:
            if wmap[w]: ranks[w].append((a["name"], wmap[w]["edge"]))
        print(f"\n### {a['name']} — {a['specialty']}  (WHEN {a['trigger']['type']}{a['trigger']['params']} "
              f"-> {a['outcome']['direction']} T+{a['outcome']['horizon']})")
        print(f"  overall: n={overall['n']} win={overall['win']}% exp={overall['expct']:+.2f}%/trade edge={overall['edge']:+.2f}% PF={overall['pf']}")
        print(f"  {'world':<15}{'n':>7}{'win%':>7}{'exp%':>8}{'edge%':>8}")
        for w in W:
            c = wmap[w]
            if c: print(f"  {w:<15}{c['n']:>7}{c['win']:>6}%{c['expct']:>7}%{c['edge']:>7}%")
    print("\n" + "=" * 78 + "\nMARKET-WORLD RANKINGS (by edge vs universe)\n" + "=" * 78)
    for w in W:
        r = sorted(ranks[w], key=lambda x: -x[1])
        print(f"  {w:<15} " + "  ".join(f"#{i+1} {n}({e:+.2f}%)" for i, (n, e) in enumerate(r)))
    print("\n" + "=" * 78 + "\nAGENT JOURNALS (auto-narrated from real evidence)\n" + "=" * 78)
    # world rank lookup per agent
    wrank = {w: {n: i + 1 for i, (n, _) in enumerate(sorted(ranks[w], key=lambda x: -x[1]))} for w in W}
    allprof = []
    for aid, (a, overall, wmap) in profiles.items():
        jt = journal(a, overall, wmap)
        print("\n" + jt)
        prof = {"id": aid, "name": a["name"], "personality": a["personality"], "specialty": a["specialty"],
                "voice": a["voice"], "direction": a["outcome"]["direction"], "horizon": a["outcome"]["horizon"],
                "hypothesis": {"trigger": a["trigger"], "scope": a["scope"], "outcome": a["outcome"], "baseline": a["baseline"], "cost_bps": 30},
                "overall_evidence": overall,
                "market_worlds": {w: (dict(wmap[w], rank=wrank[w].get(a["name"])) if wmap[w] else None) for w in wmap},
                "today_picks": today_events(a), "journal": jt, "as_of": str(data()["c"].index[-1].date())}
        (AGDIR / f"{aid}.json").write_text(json.dumps(prof, indent=2), encoding="utf-8")
        allprof.append(prof)
    (AGDIR / "_all.json").write_text(json.dumps(allprof), encoding="utf-8")
    print(f"\n  persisted {len(allprof)} agent profiles -> agents_arena/*.json (+ _all.json)")
    build_grid()


if __name__ == "__main__":
    main()
