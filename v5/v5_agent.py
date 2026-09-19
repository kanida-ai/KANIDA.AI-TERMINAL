"""V5 — a self-modifying research agent.

    python v5_agent.py --stock ADANIENT --db path\\to\\kanida.db

WHAT IT ACTUALLY DOES TO ITSELF
-------------------------------
1. WRITES ITS OWN FEATURE CODE. Each generation it composes new feature
   expressions from primitives and operators, writes them as real Python into
   v5_genome.py, imports that file, and tests them. Features that improve
   validated performance are kept and become parents of the next generation.
   Features that do not are deleted from the genome. The file on disk changes.

2. ACQUIRES ITS OWN RESOURCES. It scans the database for indices, sector peers
   and correlated symbols, and adds whichever it finds useful as cross-sectional
   inputs. It was not told which ones exist.

3. IMPROVES ITS OWN SEARCH. It tracks which primitive families and which model
   configurations produce survivors, and reallocates the next generation's
   budget toward them. A family that has never produced a survivor is sampled
   less; one that has is sampled more.

4. TUNES ITS OWN MODELS. Depth, learning rate, leaf size and regularisation are
   part of the genome and mutate alongside the features.

WHAT IT MAY NOT TOUCH, AND WHY
------------------------------
v5_core.py -- the purge gap, the shuffle control, the seal, the cost model, the
fixed-capital rule. It is hash-locked and the agent halts if it changes.

An optimiser told to reach +1%/day has two routes: find an edge, or weaken the
test. The second is far easier and instantly effective. In this project six
corrections were needed and every one moved results DOWN, because each
unexamined assumption had been flattering the search. An agent able to edit its
own scorer would have moved all six the other way and reported success within
the hour.

So it may rewrite anything that GENERATES a candidate, and nothing that JUDGES
one. That is what makes the self-improvement worth having.
"""
from __future__ import annotations

import argparse
import hashlib
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

import v5_core as core

GENOME_PATH = "v5_genome.py"

PRIMITIVES = ["ret", "vol", "rng", "clv", "vwap_d", "rel", "idx", "atp", "pos"]
OPERATORS = ["slope", "z", "std", "frac_hi", "frac_lo", "accel", "ratio",
             "pctile", "streak", "conc"]
WINDOWS = [3, 5, 10, 15, 30, 45, 60, 90]

GENOME_HEADER = '''"""AUTO-GENERATED. The agent rewrites this file. Do not hand-edit.

Every function below was composed, written to disk, tested against the immutable
core, and kept because it improved validated performance. Functions that failed
were deleted. The docstring on each records when it was born and what it scored.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

EPS = 1e-12


def _w(a, w):
    return a[-w:] if len(a) >= w else a


'''


def log(m: str = "") -> None:
    print(f"[{datetime.now():%H:%M:%S}] {m}", flush=True)


# --------------------------------------------------------------------------- #
class Gene:
    """One generated feature: primitive x operator x window."""

    def __init__(self, prim, op, win, born=0):
        self.prim, self.op, self.win, self.born = prim, op, win, born
        self.score = None

    @property
    def name(self):
        return f"g_{self.prim}_{self.op}_{self.win}"

    def valid(self) -> bool:
        """The agent writes real Python, so a malformed name is a syntax error
        in its own source. Anything not a legal identifier is rejected before
        it can be written."""
        return (self.prim in PRIMITIVES and self.op in OPERATORS
                and self.name.isidentifier())

    def code(self) -> str:
        body = {
            "slope": "float(np.polyfit(np.arange(len(a)), a, 1)[0]) if len(a) > 2 else 0.0",
            "z": "float((a[-1] - a.mean()) / (a.std() + EPS))",
            "std": "float(a.std())",
            "frac_hi": "float((a > np.median(a)).mean())",
            "frac_lo": "float((a < np.median(a)).mean())",
            "accel": "float(a[-1] - 2*a[len(a)//2] + a[0]) if len(a) > 2 else 0.0",
            "ratio": "float(a[-1] / (np.abs(a).mean() + EPS))",
            "pctile": "float((a[:-1] < a[-1]).mean()) if len(a) > 1 else 0.5",
            "streak": "float(np.sum(np.sign(np.diff(a)) == np.sign(np.diff(a))[-1]) ) if len(a) > 2 else 0.0",
            "conc": "float(np.abs(a).max() / (np.abs(a).sum() + EPS))",
        }[self.op]
        return (f'def {self.name}(series: dict) -> float:\n'
                f'    """born gen {self.born}"""\n'
                f'    a = _w(series["{self.prim}"], {self.win})\n'
                f'    if len(a) < 2:\n'
                f'        return 0.0\n'
                f'    return {body}\n\n\n')


def write_genome(genes: list[Gene], model_cfg: dict, path: str = GENOME_PATH):
    """The agent writing its own source. This file changes on disk.

    Written to a temp file and compiled first: if the agent ever generates code
    that will not parse, the previous genome survives untouched rather than the
    process dying on an import of its own broken output.
    """
    genes = [g for g in genes if g.valid()]
    with open(path + ".tmp", "w") as fh:
        fh.write(GENOME_HEADER)
        fh.write(f"MODEL_CFG = {json.dumps(model_cfg, indent=4)}\n\n\n")
        for g in genes:
            fh.write(g.code())
        fh.write("GENES = [\n")
        for g in genes:
            fh.write(f'    ("{g.name}", {g.name}, "{g.prim}", "{g.op}", {g.win}),\n')
        fh.write("]\n")
    src = open(path + ".tmp").read()
    try:
        compile(src, path, "exec")
    except SyntaxError as e:
        os.remove(path + ".tmp")
        raise RuntimeError(f"agent generated invalid code: {e}") from None
    os.replace(path + ".tmp", path)


# --------------------------------------------------------------------------- #
def build_series(g: pd.DataFrame, idx: pd.DataFrame | None) -> list[dict]:
    """Raw series the generated genes operate on, per decision minute."""
    d = g.sort_values("ts").reset_index(drop=True)
    n = len(d)
    if n < 40:
        return []
    c = d["close"].to_numpy(float)
    h = d["high"].to_numpy(float)
    lo = d["low"].to_numpy(float)
    v = d["volume"].to_numpy(float)
    o = d["open"].to_numpy(float)
    ret = np.log(c / c[0])
    tp = (h + lo + c) / 3.0
    vwap = np.cumsum(tp * v) / (np.cumsum(v) + 1e-12)
    rng = (h - lo) / np.maximum(c, 1e-12)
    clv = np.where(h > lo, (c - lo) / np.maximum(h - lo, 1e-12), 0.5)
    atp = np.cumsum(tp * v) / (np.cumsum(v) + 1e-12)
    ir = None
    if idx is not None and len(idx) >= n:
        ic = idx["close"].to_numpy(float)[:n]
        ir = np.log(ic / ic[0])
    out = []
    warm = min(60, max(n // 5, 12))
    for t in range(warm, n - 1, 5):
        s = {"ret": ret[:t+1], "vol": np.log1p(v[:t+1]), "rng": rng[:t+1],
             "clv": clv[:t+1], "vwap_d": (c[:t+1] / vwap[:t+1] - 1.0),
             "atp": np.diff(atp[:t+1], prepend=atp[0]),
             "pos": np.array([(c[i] - lo[:i+1].min())
                              / (h[:i+1].max() - lo[:i+1].min() + 1e-12)
                              for i in range(t+1)]),
             "rel": (ret[:t+1] - ir[:t+1]) if ir is not None else ret[:t+1],
             "idx": ir[:t+1] if ir is not None else np.zeros(t+1)}
        out.append({"t": t, "series": s, "entry_px": float(o[t+1]),
                    "session": d["session"].iloc[0], "bar": t})
    return out


# --------------------------------------------------------------------------- #
class V5:
    def __init__(self, stock, db, table, capital, out, start):
        self.stock, self.capital, self.out, self.db = stock, capital, out, db
        self.table = table
        os.makedirs(out, exist_ok=True)
        self.audit = open(os.path.join(out, f"audit_{stock}.log"), "a")
        self.gen = 0
        self.genes: list[Gene] = []
        self.family_wins: dict[str, list] = {}
        self.model_cfg = {"max_iter": 200, "max_depth": 5,
                          "learning_rate": 0.06, "min_samples_leaf": 60,
                          "l2_regularization": 1.0}
        self.best_edge = -9.0
        self._cache: dict[str, list] = {}
        self.max_genes = 96
        self.mut_size = 24
        self.stale = 0
        self._load()

        log(f"core fingerprint {core.CORE_HASH} (locked)")
        self._acquire(start)

    # ------------------------------------------------------- resources ------
    def _acquire(self, start):
        """Find its own inputs. Nobody told it what is in the database."""
        con = sqlite3.connect(self.db)
        names = [r[0] for r in con.execute(
            f'SELECT DISTINCT symbol FROM "{self.table}"')]
        con.close()
        idx_candidates = [n for n in names
                          if n.strip().upper().startswith(("NIFTY", "INDIA VIX"))]
        pick = None
        for want in ("NIFTY 50", "NIFTY50", "NIFTY 500", "NIFTY MIDCAP 100"):
            for n in idx_candidates:
                if n.strip().upper() == want:
                    pick = n
                    break
            if pick:
                break
        if pick is None and idx_candidates:
            pick = idx_candidates[0]
        log(f"  resources found: {len(names)} symbols, {len(idx_candidates)} "
            f"index series -> using '{pick}'")
        self.bars = self._load_bars(self.stock, start)
        self.idx = self._load_bars(pick, start) if pick else None
        core.assert_sealed(self.bars)
        self.points = []
        for sess, g in self.bars.groupby("session", sort=True):
            ib = (self.idx[self.idx["session"] == sess]
                  if self.idx is not None else None)
            self.points += build_series(g, ib)
        self.n_sessions = len({p["session"] for p in self.points})
        log(f"  {len(self.points):,} decision points across {self.n_sessions} "
            f"sessions")
        self._targets()

    def _load_bars(self, sym, start):
        con = sqlite3.connect(self.db)
        d = pd.read_sql_query(
            f'SELECT symbol,bar_time,open,high,low,close,volume FROM "{self.table}" '
            f'WHERE symbol=? AND bar_time>=? AND bar_time<?', con,
            params=[sym, start, core.SEAL_FROM])
        con.close()
        if d.empty:
            return d
        d["ts"] = pd.to_datetime(d["bar_time"])
        d["session"] = d["ts"].dt.normalize()
        return d.sort_values("ts").reset_index(drop=True)

    def _targets(self):
        cl = self.bars.groupby("session")["close"].last()
        ss = list(cl.index)
        dmap = {(s, n): (float(cl.iloc[i+n]) if i+n < len(ss) else np.nan)
                for i, s in enumerate(ss) for n in (1, 3)}
        closes = {s: g.to_numpy(float)
                  for s, g in self.bars.groupby("session")["close"]}
        for p in self.points:
            e = p["entry_px"]
            arr = closes[p["session"]]
            p["y_close"] = core.net(arr[-1] / e - 1.0)
            p["y_60"] = core.net(arr[min(p["bar"]+60, len(arr)-1)] / e - 1.0)
            p["y_d1"] = core.net(dmap.get((p["session"], 1), np.nan) / e - 1.0)
            p["y_d3"] = core.net(dmap.get((p["session"], 3), np.nan) / e - 1.0)

    # ------------------------------------------------------- self-edit ------
    def prune(self, genes, X=None, protected=None):
        """Drop redundant and dead genes so the genome stays searchable.

        Without this the genome only grows, each generation costs more than the
        last, and the agent slows to a halt long before it runs out of ideas.
        Two things go: genes whose column barely varies (they carry nothing),
        and genes almost perfectly correlated with an older gene (they carry
        nothing NEW).
        """
        if X is None or len(genes) <= self.max_genes:
            return genes
        names = [g.name for g in genes if g.name in X.columns]
        M = X[names]
        keep, dropped = [], []
        sd = M.std()
        corr = M.corr().abs()
        for n in names:
            if sd.get(n, 0) < 1e-9:
                dropped.append(n)
                continue
            if any(corr.loc[n, k] > 0.97 for k in keep):
                dropped.append(n)
                continue
            keep.append(n)
        if len(keep) > self.max_genes:
            # BUG FIXED: this kept the LAST n genes, which are the newest random
            # ones, so every generation evicted the proven genes that earned
            # their place. The best genome was being thrown away one generation
            # at a time, which is why the edge decayed from +0.364% to +0.000%.
            prot = set(protected or [])
            first = [n for n in keep if n in prot]
            rest = [n for n in keep if n not in prot]
            keep = (first + rest)[:self.max_genes]
        if dropped:
            log(f"    pruned {len(dropped)} genes "
                f"(flat or >0.97 correlated), {len(keep)} remain")
        return [g for g in genes if g.name in keep]

    def mutate(self, n_new: int | None = None):
        n_new = n_new or self.mut_size
        """Compose new feature code, weighted toward families that have won."""
        hot = [f for f, v in self.family_wins.items()
               if f in PRIMITIVES and np.mean(v) > 0] or list(PRIMITIVES)
        newg = []
        have = {g.name for g in self.genes}
        for _ in range(n_new * 8):
            if len(newg) >= n_new:
                break
            prim = random.choice(hot) if random.random() < 0.6 \
                else random.choice(PRIMITIVES)
            g = Gene(prim, random.choice(OPERATORS), random.choice(WINDOWS),
                     born=self.gen)
            if g.name in have or not g.valid():
                continue
            have.add(g.name)
            newg.append(g)
        # occasionally mutate the model too
        if random.random() < 0.4:
            k = random.choice(list(self.model_cfg))
            cur = self.model_cfg[k]
            self.model_cfg[k] = (max(2, int(cur * random.choice([0.7, 1.4])))
                                 if isinstance(cur, int)
                                 else round(cur * random.choice([0.6, 1.6]), 4))
        return newg

    def matrix(self, genes):
        """Only compute genes that are new. Everything else comes from cache.

        Recomputing every gene each generation is why generation time went from
        5 minutes to 15: the cost grew with the genome even though most genes
        were unchanged. Caching makes each generation cost only what it added.
        """
        cols = {}
        for g in genes:
            if g.name in self._cache:
                cols[g.name] = self._cache[g.name]
                continue
            fn = getattr(self.genome_mod, g.name)
            v = [fn(p["series"]) for p in self.points]
            self._cache[g.name] = v
            cols[g.name] = v
        for k in list(self._cache):
            if k not in cols and len(self._cache) > 400:
                del self._cache[k]
        X = pd.DataFrame(cols)
        X["session"] = [p["session"] for p in self.points]
        for t in ("y_close", "y_60", "y_d1", "y_d3"):
            X[t] = [p[t] for p in self.points]
        return X

    def score(self, X, genes, target, side, top_frac=0.35, shuffle=False):
        from sklearn.ensemble import HistGradientBoostingRegressor
        p = X[X[target].notna()].copy()
        if len(p) < core.MIN_TRAIN + core.MIN_TEST:
            return None
        y = p[target].to_numpy(float) * (1.0 if side == "long" else -1.0)
        if shuffle:
            y = np.random.default_rng(5).permutation(y)
        F = [g.name for g in genes]
        M = p[F].to_numpy(np.float32)
        sess = p["session"].to_numpy()
        picks = []
        for tr_end, te_a, te_b in core.purged_folds(sess):
            tr = sess < tr_end
            te = (sess >= te_a) & (sess < te_b)
            if tr.sum() < core.MIN_TRAIN or te.sum() < core.MIN_TEST:
                continue
            m = HistGradientBoostingRegressor(random_state=7, **self.model_cfg)
            m.fit(M[tr], y[tr])
            pr = m.predict(M[te])
            sub = pd.DataFrame({"session": sess[te], "pred": pr, "real": y[te]})
            best = (sub.sort_values(["session", "pred"], ascending=[True, False])
                       .groupby("session").head(1))
            picks.append(best.nlargest(max(int(len(best)*top_frac), 1), "pred"))
        if not picks:
            return None
        pk = pd.concat(picks, ignore_index=True)
        r = core.evaluate_leg(pk["real"].to_numpy(), self.n_sessions)
        r["picks"] = pk
        return r

    # ---------------------------------------------------------- generation --
    def generation(self):
        self.gen += 1
        t0 = time.time()
        cand = [g for g in self.genes + self.mutate() if g.valid()]
        write_genome(cand, self.model_cfg)
        import importlib
        import v5_genome
        importlib.reload(v5_genome)
        self.genome_mod = v5_genome

        X = self.matrix(cand)
        cand = self.prune(cand, X, protected=[g.name for g in self.genes])
        X = X[[g.name for g in cand] + ["session", "y_close", "y_60",
                                        "y_d1", "y_d3"]]
        legs, sh_legs, rows = {}, {}, []
        for target in ("y_60", "y_close", "y_d1", "y_d3"):
            for side in ("long", "short"):
                r = self.score(X, cand, target, side)
                if r is None:
                    continue
                s = self.score(X, cand, target, side, shuffle=True)
                ok, why = core.verdict(r["per_session"],
                                       s["per_session"] if s else 0.0, r["n"])
                rows.append({"leg": f"{target}|{side}", "n": r["n"],
                             "per_session": r["per_session"],
                             "shuffled": s["per_session"] if s else 0.0,
                             "keep": ok, "why": why})
                if ok:
                    legs[f"{target}|{side}"] = r["picks"]
                    sh_legs[f"{target}|{side}"] = s["picks"] if s else None

        kept_legs = [r["leg"] for r in rows if r["keep"]]
        raw = core.book_per_session(legs, self.n_sessions)
        shb = core.book_per_session(sh_legs, self.n_sessions)
        edge = raw - shb

        # ---- selection: keep the genome only if it genuinely improved --------
        improved = edge > self.best_edge + 1e-6
        if improved:
            self.best_edge = edge
            self.genes = cand
            self.stale = 0
            write_genome(self.genes, self.model_cfg)
            tag = "KEPT"
        else:
            self.stale += 1
            write_genome(self.genes, self.model_cfg)   # roll back the file
            tag = "reverted"
        # A search that has not improved in a long time is not going to improve
        # by drawing more of the same. Widen the step: mutate harder, and drop
        # the weakest third of the genome so new material has room.
        if self.stale >= 8:
            self.mut_size = min(self.mut_size + 12, 60)
            if self.genes:
                self.genes = self.genes[len(self.genes) // 3:]
            self.stale = 0
            log(f"    stagnant {8} generations -- widening search "
                f"(mutation {self.mut_size}, genome trimmed to "
                f"{len(self.genes)})")

        # credit the PRIMITIVES that were in play, not the leg names -- the
        # sampler draws primitives, so it must be scored on primitives
        delta = edge - (self.best_edge if not improved else -9.0)
        signal = 1.0 if improved else -0.25
        for g in cand:
            self.family_wins.setdefault(g.prim, []).append(signal)
        for k in list(self.family_wins):
            if k not in PRIMITIVES:
                del self.family_wins[k]

        self.audit.write(core.audit_line(
            "GEN", gen=self.gen, genes=len(cand), edge=f"{edge:+.4f}",
            best=f"{self.best_edge:+.4f}", action=tag,
            core=core.CORE_HASH) + "\n")
        self.audit.flush()

        if not kept_legs:
            log(f"gen {self.gen} | {len(cand)} genes | no leg beat its shuffled "
                f"control | best {self.best_edge:+.3f}% | {tag} | "
                f"{(time.time()-t0)/60:.1f}m")
            self._save()
            return
        log(f"gen {self.gen} | {len(cand)} genes | "
            f"book raw {raw:+.3f}% shuffled {shb:+.3f}% EDGE {edge:+.3f}%/session "
            f"| best {self.best_edge:+.3f}% | {tag} | "
            f"{(time.time()-t0)/60:.1f}m")
        for r in rows:
            if r["keep"]:
                log(f"    {r['leg']:<14} n={r['n']:<5} "
                    f"{r['per_session']:+.3f}% vs shuffled {r['shuffled']:+.3f}%")
        sides = {l.split("|")[1] for l in kept_legs}
        if len(kept_legs) >= 3 and len(sides) == 1:
            log(f"    NOTE: all {len(kept_legs)} legs are {sides.pop().upper()} "
                f"at different exits -- one bet held four ways, not four bets. "
                f"The book is far less diversified than the leg count implies.")
        log(f"    Rs {self.capital*self.best_edge/100:,.0f}/day  |  "
            f"target Rs {self.capital/100:,.0f}/day  |  "
            f"gap {1.0-self.best_edge:+.3f}")
        self._save()

    # ------------------------------------------------------------- state ----
    def _save(self):
        """Atomic write. A Ctrl+C part-way through json.dump left a truncated
        file, and _load silently started fresh on the parse error -- which is
        how a best_edge of +0.532% came back as +0.337%. Write to a temp file,
        then rename, so the old state survives any interruption."""
        path = os.path.join(self.out, f"state_{self.stock}.json")
        tmp = path + ".tmp"
        json.dump({"gen": self.gen, "best_edge": self.best_edge,
                   "model_cfg": self.model_cfg, "mut_size": self.mut_size,
                   "stale": self.stale,
                   "genes": [[g.prim, g.op, g.win, g.born] for g in self.genes],
                   "family_wins": {k: v[-40:] for k, v in self.family_wins.items()}},
                  open(tmp, "w"))
        os.replace(tmp, path)
        # a dated snapshot too, so a bad state is never the only copy
        if self.gen % 10 == 0:
            import shutil as _sh
            _sh.copy(path, path.replace(".json", f"_gen{self.gen}.json"))

    def _load(self):
        p = os.path.join(self.out, f"state_{self.stock}.json")
        if not os.path.exists(p):
            return
        try:
            d = json.load(open(p))
            self.gen = d["gen"]
            self.best_edge = d["best_edge"]
            self.model_cfg = d["model_cfg"]
            self.mut_size = d.get("mut_size", 24)
            self.stale = d.get("stale", 0)
            self.genes = [Gene(*g[:3], born=g[3]) for g in d["genes"]]
            self.family_wins = d.get("family_wins", {})
            log(f"  resumed at generation {self.gen}, {len(self.genes)} genes, "
                f"best edge {self.best_edge:+.3f}%")
        except Exception as e:
            log(f"  !! STATE UNREADABLE ({e})")
            snaps = sorted(f for f in os.listdir(self.out)
                           if f.startswith(f"state_{self.stock}_gen"))
            if snaps:
                log(f"  !! recovering from snapshot {snaps[-1]}")
                try:
                    d = json.load(open(os.path.join(self.out, snaps[-1])))
                    self.gen = d["gen"]; self.best_edge = d["best_edge"]
                    self.model_cfg = d["model_cfg"]
                    self.genes = [Gene(*g[:3], born=g[3]) for g in d["genes"]]
                    self.family_wins = d.get("family_wins", {})
                    log(f"  !! recovered generation {self.gen}, "
                        f"best edge {self.best_edge:+.3f}%")
                    return
                except Exception:
                    pass
            log("  !! NO SNAPSHOT -- starting fresh. Prior progress is lost.")

    def run(self, once=False):
        while True:
            if core.core_fingerprint() != core.CORE_HASH:
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
    ap.add_argument("--table", default="ohlc_1min")
    ap.add_argument("--capital", type=float, default=30000.0)
    ap.add_argument("--start", default="2022-01-01")
    ap.add_argument("--out", default="v5_out")
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args()
    print("=" * 74, flush=True)
    print(f"V5 SELF-MODIFYING AGENT  |  {a.stock}  |  Rs {a.capital:,.0f} fixed",
          flush=True)
    print("writes its own feature code | acquires its own inputs | "
          "tunes its own models", flush=True)
    print("v5_core.py is hash-locked: the agent cannot edit what judges it",
          flush=True)
    print("=" * 74, flush=True)
    V5(a.stock, a.db, a.table, a.capital, a.out, a.start).run(a.once)


if __name__ == "__main__":
    main()
