"""AUTONOMOUS PATTERN AGENT — self-configuring, self-coding, continuous.

    python pat_evolve.py --stock ADANIENT --db kanida.db --xlsx pats.xlsx

WHAT WAS WRONG WITH THE PREVIOUS VERSION
----------------------------------------
It ran once, with constants I chose. When it failed I inspected the output,
found the flaw, hard-coded a different constant and re-ran. Four times. I was
the iteration loop, which is the opposite of what an autonomous agent is for.

And every "bug" I fixed by hand was a PARAMETER THAT SHOULD HAVE BEEN SEARCHED:

    top_patterns   I set 300, it covered 90% of sessions, I changed it to 8
    min_n          I picked 25
    edge/strength  I picked > 0
    consistency    I required it
    adaptation     I fixed the percentile method
    combos         I fixed pair-depth at 2

All of that is now genome. The agent mutates it, tests it against the immutable
scorer, keeps what improves the fitness and reverts what does not. Nobody
watches. It does not stop.

IT ALSO WRITES ITS OWN PATTERNS
-------------------------------
Beyond tuning, it composes NEW rules -- taking variables and comparison
directions from the workbook's vocabulary and generating conjunctions that were
never in the 4,794. Those are written to pat_genome.py as real Python, compiled,
and tested exactly like the originals. Rules that improve the fitness survive
into the next generation; the rest are deleted.

WHAT IT MAY NOT TOUCH
---------------------
pat_core.py: the coverage cap, the skipped-days check, the forward protocol,
the shuffle control, the point-in-time rule, and the fitness definition. It is
hash-checked every generation and the agent halts if it changed. The coverage
cap alone is what caught a permanent long masquerading as a -0.25%/day
directional finding, and an agent free to remove it would do so immediately.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
import time
import warnings
from datetime import datetime

import numpy as np
import pandas as pd

warnings.simplefilter("ignore", FutureWarning)

import pat_core as core
from pattern_lib import (ALL_VARS, ThresholdAdapter, compute_vars,
                         evaluate_rule, parse_rules)

GENOME = "pat_genome.py"


def log(m=""):
    print(f"[{datetime.now():%H:%M:%S}] {m}", flush=True)


def load_daily(db, table, sym, start):
    con = sqlite3.connect(db)
    d = pd.read_sql_query(
        f'SELECT bar_time,open,high,low,close,volume FROM "{table}" '
        f'WHERE symbol=? AND bar_time>=? AND bar_time<? ORDER BY bar_time',
        con, params=[sym, start, core.SEAL_FROM])
    con.close()
    if d.empty:
        return d
    d["date"] = pd.to_datetime(d["bar_time"]).dt.normalize()
    return d.groupby("date", as_index=False).agg(
        {"open": "first", "high": "max", "low": "min",
         "close": "last", "volume": "sum"})


def forward_frame(d):
    f = pd.DataFrame({"date": d["date"]})
    e = d["open"].shift(-1)
    for n in (1, 2, 3):
        ex = d["close"].shift(-n)
        hi = d["high"].shift(-1).rolling(n, min_periods=1).max().shift(-(n-1))
        lo = d["low"].shift(-1).rolling(n, min_periods=1).min().shift(-(n-1))
        f[f"r{n}"] = (ex/e - 1)*100 - (core.COST + core.SLIP)*100
        f[f"mfe{n}"] = (hi/e - 1)*100
        f[f"mae{n}"] = (lo/e - 1)*100
    return f


def ref_universe(db, table, n, start, exclude=None):
    con = sqlite3.connect(db)
    rows = con.execute(
        f'SELECT symbol, AVG(close*volume) t, COUNT(*) c FROM "{table}" '
        f'WHERE bar_time>=? AND bar_time<? GROUP BY symbol '
        f'HAVING c>300 AND t>0 ORDER BY t DESC LIMIT ?',
        [start, core.SEAL_FROM, n*4]).fetchall()
    con.close()
    out = []
    for s, t, c in rows:
        u = s.strip().upper()
        if u.startswith("NIFTY") or "BHARATBOND" in u or u == "INDIA VIX":
            continue
        if exclude and s == exclude:
            continue
        out.append(s)
        if len(out) >= n:
            break
    return out


# --------------------------------------------------------------------------- #
DEFAULT_GENOME = {
    "min_n": 25, "top_patterns": 8, "min_edge1": 0.0, "min_strength1": 0.0,
    "require_consistency": 1, "combo_depth": 1, "side": "long",
    "rank_by": "strength1", "min_hist": 150, "max_conditions": 4,
    "recency_weight": 0.0, "horizon": 1,
}


class Evolver:
    def __init__(self, stock, db, xlsx, table, index, start, burnin,
                 ref_stocks, out, max_rules):
        self.stock, self.out = stock, out
        os.makedirs(out, exist_ok=True)
        self.g = dict(DEFAULT_GENOME)
        self.best_fit, self.gen = -9.0, 0
        self.stale = 0
        self.last_reason = ""
        self.invented = []          # rules the agent wrote itself
        self.audit = open(os.path.join(out, f"audit_{stock}.log"), "a")
        self._load()

        log(f"core {core.CORE} (locked)  coverage cap {core.MAX_COVERAGE:.0%}")
        self.rules = parse_rules(xlsx).head(max_rules)
        d = load_daily(db, table, stock, start)
        if d.empty:
            raise RuntimeError(f"no bars for {stock}")
        idx = load_daily(db, table, index, start)
        mkt = idx.set_index("date")["close"] if not idx.empty else None
        self.f = compute_vars(d, mkt, None)
        self.fw = forward_frame(d)
        self.dates = self.f["date"].to_numpy()
        log(f"{len(d)} sessions {d['date'].min().date()} -> {d['date'].max().date()}")

        frames = []
        for rs in ref_universe(db, table, ref_stocks, start, exclude=stock):
            rd = load_daily(db, table, rs, start)
            if len(rd) >= 150:
                rf = compute_vars(rd, mkt, None)
                frames.append(rf[rf["date"] <= burnin])
        frames.append(self.f[self.f["date"] <= burnin])
        self.adapter = ThresholdAdapter(frames, min_hist=self.g["min_hist"])
        log(f"reference: {self.adapter.n_ref:,} pooled rows, "
            f"{len(self.adapter.ref)} variables usable")
        self.vocab = sorted(self.adapter.ref)
        self.cache = {}
        self.masks = {}
        self._build_masks(self.rules)
        log(f"SEEDED with {len(self.masks)} of {len(self.rules)} workbook "
            f"patterns, adapted to {stock}'s own percentiles. The agent starts "
            f"from these and invents more each generation.")
        self.years = [y for y in sorted({int(str(x)[:4]) for x in self.dates})
                      if y >= 2023]

    # ------------------------------------------------------- masks ----------
    def _build_masks(self, rules):
        for r in rules.itertuples():
            m = evaluate_rule(self.f, r.conds, self.adapter, self.cache)
            if m is not None and m.sum() >= 15:
                self.masks[str(r.pattern_id)] = m

    # ------------------------------------------------- self-written rules ---
    def invent(self, n=12):
        """Compose rules that were never in the workbook, and write them out."""
        new = []
        for _ in range(n * 6):
            if len(new) >= n:
                break
            k = random.randint(2, self.g["max_conditions"])
            vs = random.sample(self.vocab, min(k, len(self.vocab)))
            conds = []
            for v in vs:
                a = self.adapter.ref[v]
                q = random.choice([0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8])
                thr = float(np.quantile(a, q))
                conds.append([v, random.choice(["<=", ">"]), thr])
            pid = "INV_" + "_".join(f"{c[0][:6]}{c[1][0]}" for c in conds)[:40] \
                  + f"_{self.gen}_{len(new)}"
            if pid in self.masks:
                continue
            m = evaluate_rule(self.f, conds, self.adapter, self.cache)
            if m is None or m.sum() < self.g["min_n"]:
                continue
            new.append((pid, conds, m))
        return new

    def write_genome(self, invented):
        with open(GENOME + ".tmp", "w") as fh:
            fh.write('"""AUTO-GENERATED by the agent. Do not hand-edit.\n\n'
                     'Rules the agent composed itself and kept because they\n'
                     'improved the forward-test fitness.\n"""\n\n')
            fh.write(f"CONFIG = {json.dumps(self.g, indent=4)}\n\n")
            fh.write("INVENTED_RULES = [\n")
            for pid, conds, _ in invented:
                fh.write(f'    ({pid!r}, {json.dumps(conds)}),\n')
            fh.write("]\n")
        src = open(GENOME + ".tmp").read()
        compile(src, GENOME, "exec")
        os.replace(GENOME + ".tmp", GENOME)

    # ------------------------------------------------------ mutation --------
    def mutate(self, reason: str = ""):
        """Mutate, steered by WHY the last attempt failed.

        Blind mutation over ten knobs is slow when the binding constraint is
        known. If coverage was the failure, tighten the things that reduce it;
        if nothing qualified, loosen the gates. This is the agent using its own
        diagnosis to direct the search rather than sampling at random.
        """
        g = dict(self.g)
        if "cover" in reason:
            # BUG FIXED: this used to apply a FIXED shrink to the same best
            # config every generation, so it produced the identical mutation
            # over and over -- CARTRADE sat at 55% coverage for nine straight
            # generations proposing the same thing. The step now compounds with
            # how long it has been stuck, and is randomised, so the search
            # actually walks instead of standing still.
            hard = 1 + self.stale
            g["top_patterns"] = max(1, int(g["top_patterns"]
                                           / (1.6 ** min(hard, 4))))
            g["min_edge1"] = round(min(g["min_edge1"]
                                       + 0.04 * hard * random.uniform(0.5, 1.5),
                                       0.8), 3)
            g["min_n"] = int(np.clip(g["min_n"] * random.uniform(1.1, 1.9),
                                     12, 250))
            if self.stale >= 2:
                g["combo_depth"] = 2
            if self.stale >= 4:
                g["require_consistency"] = 1
                g["min_strength1"] = round(min(g["min_strength1"] + 0.03, 0.4), 3)
            if self.stale >= 6:
                g["side"] = random.choice(["long", "short"])
                g["horizon"] = random.choice([1, 2, 3])
            return g
        if "nothing qualified" in reason or "too few signals" in reason:
            hard = 1 + self.stale
            g["top_patterns"] = min(40, int(g["top_patterns"]
                                            * (1.5 ** min(hard, 4))) + 1)
            g["min_edge1"] = round(max(g["min_edge1"] - 0.04 * hard, -0.2), 3)
            g["min_strength1"] = round(max(g["min_strength1"] - 0.03 * hard,
                                           -0.1), 3)
            g["require_consistency"] = 0
            g["min_n"] = max(12, int(g["min_n"] / random.uniform(1.1, 1.7)))
            if self.stale >= 4:
                g["combo_depth"] = 1
                g["horizon"] = random.choice([1, 2, 3])
            return g
        keys = random.sample(list(g), random.randint(1, 3))
        for k in keys:
            if k == "min_n":
                g[k] = int(np.clip(g[k] * random.choice([0.6, 1.6]), 12, 200))
            elif k == "top_patterns":
                g[k] = int(np.clip(g[k] * random.choice([0.5, 2.0]), 1, 40))
            elif k in ("min_edge1", "min_strength1"):
                g[k] = round(float(np.clip(g[k] + random.choice(
                    [-0.05, -0.02, 0.02, 0.05, 0.1]), -0.1, 0.6)), 3)
            elif k == "require_consistency":
                g[k] = 1 - g[k]
            elif k == "combo_depth":
                g[k] = random.choice([1, 2])
            elif k == "side":
                g[k] = random.choice(["long", "short"])
            elif k == "rank_by":
                g[k] = random.choice(["strength1", "edge1", "recency",
                                      "edge3", "conf1"])
            elif k == "max_conditions":
                g[k] = random.choice([2, 3, 4, 5])
            elif k == "horizon":
                g[k] = random.choice([1, 2, 3])
            elif k == "recency_weight":
                g[k] = round(random.choice([0.0, 0.25, 0.5, 1.0]), 2)
        return g

    # -------------------------------------------------------- fitness -------
    def run_config(self, g, masks) -> tuple[float, str, list]:
        rng = np.random.default_rng(11)
        years = []
        for ty in self.years:
            tr = self.dates < np.datetime64(f"{ty}-01-01")
            te = ((self.dates >= np.datetime64(f"{ty}-01-01")) &
                  (self.dates < np.datetime64(f"{ty+1}-01-01")))
            if tr.sum() < 200 or te.sum() < 60:
                continue
            btr = core.base_rates(self.fw, tr)
            cands = []
            for pid, m in masks.items():
                s = core.score_mask(m & tr, self.fw, btr, g["min_n"])
                if not s:
                    continue
                if s["edge1"] < g["min_edge1"] or s["strength1"] < g["min_strength1"]:
                    continue
                if g["require_consistency"] and s["consistency"] <= 0:
                    continue
                key = s.get(g["rank_by"], s["strength1"]) \
                    + g["recency_weight"] * s.get("recency", 0.0)
                cands.append((key, pid))
            cands.sort(reverse=True)
            picked = [p for _, p in cands[:g["top_patterns"]]]
            if not picked:
                years.append({"valid": False, "why": "nothing qualified",
                              "signals": 0, "coverage": 0.0})
                continue
            if g["combo_depth"] == 2 and len(picked) >= 2:
                sel = np.zeros(len(self.dates), bool)
                for i in range(len(picked)):
                    for j in range(i+1, len(picked)):
                        sel |= (masks[picked[i]] & masks[picked[j]])
            else:
                sel = np.zeros(len(self.dates), bool)
                for p in picked:
                    sel |= masks[p]
            years.append(core.evaluate_year(sel, te, self.fw, g["side"], rng))
        fit, why = core.fitness(years, g.get("horizon", 1))
        return fit, why, years

    # ----------------------------------------------------- generation -------
    def generation(self):
        self.gen += 1
        t0 = time.time()
        g = self.mutate(self.last_reason)
        inv = self.invent(12)
        masks = dict(self.masks)
        for pid, conds, m in inv:
            masks[pid] = m
        fit, why, years = self.run_config(g, masks)
        bad = [y.get("why", "") for y in years if not y.get("valid")]
        self.last_reason = (bad[0] if bad else "")
        improved = fit > self.best_fit + 1e-9
        if improved:
            self.best_fit, self.g = fit, g
            for pid, conds, m in inv:
                self.masks[pid] = m
                self.invented.append((pid, conds))
            self.invented = self.invented[-300:]
            self.write_genome(inv)
            self.stale = 0
            tag = "KEPT"
        else:
            self.stale += 1
            tag = "reverted"
        if improved:
            self.last_reason = ""
        if self.stale >= 12:
            self.g = dict(DEFAULT_GENOME)
            self.g["top_patterns"] = random.choice([2, 3, 5, 8])
            self.stale = 0
            tag += " + config reset"

        valid = [y for y in years if y.get("valid")]
        cov = np.mean([y["coverage"] for y in years]) if years else 0
        log(f"gen {self.gen} | fit {fit:+.4f} ({why}) | best {self.best_fit:+.4f} "
            f"| {tag} | cover {cov:.0%} | {len(inv)} invented | "
            f"{(time.time()-t0)/60:.1f}m")
        if improved and valid:
            log(f"    config: horizon={self.g['horizon']}d "
                f"top={self.g['top_patterns']} min_n={self.g['min_n']} "
                f"side={self.g['side']} rank={self.g['rank_by']} "
                f"combo={self.g['combo_depth']} edge>={self.g['min_edge1']}")
            h = self.g["horizon"]
            tot = 0.0
            for y in valid:
                tot += y.get(f"total{h}", 0.0)
                log(f"      {y['signals']:>4} sig cover {y['coverage']:.0%} | "
                    f"{h}d avg {y.get(f'avg{h}', 0):+.3f}% "
                    f"hit {y.get(f'hitrate{h}', 0):.0%} "
                    f"sharpe {y.get(f'sharpe{h}', 0):+.2f} | "
                    f"1d vs shuffle {y['avg1']:+.3f}/{y['shuffled']:+.3f}%")
            log(f"      TOTAL across test years {tot:+.1f}% "
                f"= Rs {30000*tot/100:,.0f} on Rs 30,000 per signal")
        self.audit.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} gen={self.gen} "
                         f"fit={fit:+.5f} best={self.best_fit:+.5f} {tag} "
                         f"core={core.CORE} cfg={json.dumps(self.g)}\n")
        self.audit.flush()
        self._save()

    def _save(self):
        p = os.path.join(self.out, f"state_{self.stock}.json")
        json.dump({"gen": self.gen, "best_fit": self.best_fit, "g": self.g,
                   "invented": self.invented[-300:], "stale": self.stale},
                  open(p + ".tmp", "w"))
        os.replace(p + ".tmp", p)

    def _load(self):
        p = os.path.join(self.out, f"state_{self.stock}.json")
        if not os.path.exists(p):
            return
        try:
            d = json.load(open(p))
            self.gen, self.best_fit = d["gen"], d["best_fit"]
            self.g, self.stale = d["g"], d.get("stale", 0)
            self.invented = [tuple(x) for x in d.get("invented", [])]
            log(f"resumed gen {self.gen}, best fitness {self.best_fit:+.4f}")
        except Exception as e:
            log(f"state unreadable ({e}); starting fresh")

    def run(self, once=False):
        # restore any rules it invented in earlier runs
        for pid, conds in self.invented:
            m = evaluate_rule(self.f, conds, self.adapter, self.cache)
            if m is not None:
                self.masks[pid] = m
        log(f"running. {len(self.masks)} patterns in play "
            f"({len(self.invented)} self-written). No stopping condition.")
        while True:
            if core.fingerprint() != core.CORE:
                log("CORE MODIFIED -- halting. The scorer must not change.")
                return
            try:
                self.generation()
            except KeyboardInterrupt:
                self._save()
                raise
            except Exception as e:
                log(f"  generation error: {type(e).__name__}: {e} -- continuing")
                time.sleep(2)
            if once:
                return


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stock", required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--xlsx", required=True)
    ap.add_argument("--table", default="ohlc_daily")
    ap.add_argument("--index", default="NIFTY 50")
    ap.add_argument("--start", default="2022-01-01")
    ap.add_argument("--burnin-end", default="2022-12-31")
    ap.add_argument("--ref-stocks", type=int, default=40)
    ap.add_argument("--max-rules", type=int, default=4794)
    ap.add_argument("--out", default="evolve_out")
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args()
    print("=" * 78, flush=True)
    print(f"AUTONOMOUS PATTERN AGENT  |  {a.stock}", flush=True)
    print("tunes its own config | writes its own rules | never stops", flush=True)
    print("pat_core.py is hash-locked: it cannot edit what judges it", flush=True)
    print("=" * 78, flush=True)
    Evolver(a.stock, a.db, a.xlsx, a.table, a.index, a.start, a.burnin_end,
            a.ref_stocks, a.out, a.max_rules).run(a.once)


if __name__ == "__main__":
    main()
