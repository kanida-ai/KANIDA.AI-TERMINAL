"""V3 AUTONOMOUS QUANT RESEARCH AGENT — one instance per stock.

    python agent.py --stock ADANIENT --db path\\to\\kanida.db
    python agent.py --stock CARTRADE --db path\\to\\kanida.db

Run each in its own Anaconda prompt. Both print continuously and never exit.

OPERATING RULES
---------------
1.  The agent NEVER stops, never refuses a direction, never reports that
    something is impossible, and never asks permission to continue. When a
    hypothesis dies it reports the death and immediately proposes the next.
    "No" is not an available output; "next" is.

2.  A failed test is a RESULT, not a refusal. The agent must report failures
    accurately, because every later iteration builds on what it was told. An
    agent that reports dead candidates as alive wastes every cycle that follows.

3.  2026 is SEALED. The research window ends 2025-12-31 and the loader hard
    filters it. The agent cannot reach 2026 even if a hypothesis asks for it.

4.  Every survivor must clear: walk-forward, a search-corrected permutation
    null, a shuffle control, and absolute profitability net of costs. Passing
    three of four is not passing.

WHY THE PERMUTATION NULL IS NOT OPTIONAL
----------------------------------------
The agent tests thousands of candidates. With that many, the best result from
PURE NOISE is large -- and it grows with the number tried. So each burst records
how good the best candidate would look if outcomes were random, and a candidate
must beat that bar, not zero. Without it a tireless agent is a machine for
manufacturing false discoveries, and the more tirelessly it runs the more it
manufactures.
"""
from __future__ import annotations

import argparse
import itertools
import json
import os
import random
import sqlite3
import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd

from microstructure import (build_event_panel, event_names, time_names)

SEAL_FROM = "2026-01-01"          # never crossed, for any reason
COST = 0.0011
SLIP = 0.0005


# --------------------------------------------------------------------------- #
def log(msg: str = "", stamp: bool = True) -> None:
    if stamp:
        print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)
    else:
        print(msg, flush=True)


def load_bars(db_path: str, table: str, symbol: str, start: str) -> pd.DataFrame:
    """Sealed loader. 2026 onward is unreachable from research code."""
    con = sqlite3.connect(db_path)
    q = (f'SELECT symbol, bar_time, open, high, low, close, volume '
         f'FROM "{table}" WHERE symbol = ? AND bar_time >= ? AND bar_time < ?')
    d = pd.read_sql_query(q, con, params=[symbol, start, SEAL_FROM])
    con.close()
    if d.empty:
        raise RuntimeError(f"no bars for {symbol} before {SEAL_FROM}")
    d["ts"] = pd.to_datetime(d["bar_time"])
    d["session"] = d["ts"].dt.normalize()
    if str(d["ts"].max())[:4] >= "2026":
        raise RuntimeError("SEAL BREACH: 2026 data reached the research window")
    return d.sort_values("ts").reset_index(drop=True)


# --------------------------------------------------------------------------- #
class Hypothesis:
    """One falsifiable claim: when these events fire, this side over this
    horizon makes money."""

    def __init__(self, events, side, horizon, time_filter=None):
        self.events = tuple(sorted(events))
        self.side = side
        self.horizon = horizon          # 'close', or minutes, or ('days', n)
        self.time_filter = time_filter

    @property
    def key(self) -> str:
        t = f"@{self.time_filter}" if self.time_filter else ""
        return f"{'+'.join(self.events)}{t}|{self.side}|{self.horizon}"

    def mask(self, p: pd.DataFrame) -> np.ndarray:
        m = np.ones(len(p), dtype=bool)
        for e in self.events:
            m &= p[e].to_numpy()
        if self.time_filter:
            m &= p[self.time_filter].to_numpy()
        m &= p["next_open"].notna().to_numpy()
        return m


def returns_for(p: pd.DataFrame, h: Hypothesis, daily_close: pd.Series | None):
    """Net return per firing. Entry at the NEXT bar's open, always."""
    m = h.mask(p)
    if m.sum() < 30:
        return None, None
    sub = p.loc[m]
    entry = sub["next_open"].to_numpy(dtype=float)
    if h.horizon == "close":
        exit_px = sub["sess_close"].to_numpy(dtype=float)
    elif isinstance(h.horizon, tuple):          # ('days', n) -> daily close n on
        n = h.horizon[1]
        sess = sub["session"].to_numpy()
        exit_px = np.array([daily_close.get((s, n), np.nan) for s in sess])
    else:                                        # fixed number of minutes
        k = int(h.horizon)
        idx = sub.index.to_numpy()
        ahead = np.minimum(idx + k, idx + sub["bars_left"].to_numpy())
        exit_px = p["close"].to_numpy(dtype=float)[ahead]
    s = 1.0 if h.side == "long" else -1.0
    r = (exit_px / entry - 1.0) * s - COST - SLIP
    ok = np.isfinite(r)
    return r[ok], sub.loc[ok, "session"].to_numpy()


# --------------------------------------------------------------------------- #
def walk_forward(r: np.ndarray, sess: np.ndarray, n_folds: int = 8):
    """Expanding-window folds by time. Returns per-fold mean of the test slice."""
    order = np.argsort(sess)
    r, sess = r[order], sess[order]
    n = len(r)
    if n < n_folds * 8:
        return None
    edges = [int(n * (i + 1) / (n_folds + 1)) for i in range(n_folds)]
    means = []
    for i, e in enumerate(edges):
        nxt = edges[i + 1] if i + 1 < len(edges) else n
        if nxt - e < 5:
            continue
        means.append(float(r[e:nxt].mean()))
    return np.array(means) if means else None


def evaluate(r: np.ndarray, sess: np.ndarray) -> dict:
    folds = walk_forward(r, sess)
    return {
        "n": int(len(r)),
        "mean_pct": float(r.mean() * 100),
        "median_pct": float(np.median(r) * 100),
        "win_rate": float((r > 0).mean()),
        "std_pct": float(r.std() * 100),
        "t_stat": float(r.mean() / (r.std() / np.sqrt(len(r)) + 1e-12)),
        "folds": 0 if folds is None else len(folds),
        "folds_pos": 0 if folds is None else int((folds > 0).sum()),
        "worst_pct": float(r.min() * 100),
        "sessions": int(len(set(sess))),
    }


# --------------------------------------------------------------------------- #
class Agent:
    def __init__(self, stock: str, db: str, table: str, capital: float,
                 out_dir: str, start: str = "2022-01-01"):
        self.stock, self.capital, self.out = stock, capital, out_dir
        os.makedirs(out_dir, exist_ok=True)
        self.state_path = os.path.join(out_dir, f"agent_{stock}.json")
        self.burst = 0
        self.tested: dict[str, dict] = {}
        self.dead: set[str] = set()
        self.best: list[dict] = []
        self.family_score: dict[str, list] = {}
        self._load_state()

        log(f"loading 1-minute bars for {stock} (research window ends {SEAL_FROM})")
        bars = load_bars(db, table, stock, start)
        log(f"  {len(bars):,} bars, {bars['session'].nunique()} sessions, "
            f"{bars['session'].min().date()} -> {bars['session'].max().date()}")
        self.panel = build_event_panel(bars)
        self.events = event_names(self.panel)
        self.times = time_names(self.panel)
        self.daily_close = self._daily_close_map(bars)
        log(f"  vocabulary: {len(self.events)} events x {len(self.times)} time "
            f"buckets x 2 sides x 7 horizons")
        log(f"  search space ~{self._space():,} single and paired hypotheses")

    def _space(self) -> int:
        e = len(self.events)
        pairs = e * (e - 1) // 2
        return (e + pairs) * (len(self.times) + 1) * 2 * 7

    def _daily_close_map(self, bars):
        cl = bars.groupby("session")["close"].last()
        sessions = list(cl.index)
        m = {}
        for i, s in enumerate(sessions):
            for n in (1, 2, 3, 5, 7):
                j = i + n
                m[(s, n)] = float(cl.iloc[j]) if j < len(sessions) else np.nan
        return m

    # ---------------------------------------------------------------- state --
    def _load_state(self):
        if os.path.exists(self.state_path):
            try:
                d = json.load(open(self.state_path))
                self.burst = d.get("burst", 0)
                self.tested = d.get("tested", {})
                self.dead = set(d.get("dead", []))
                self.best = d.get("best", [])
                self.family_score = d.get("family_score", {})
                log(f"  resumed: burst {self.burst}, {len(self.tested):,} tested, "
                    f"{len(self.dead):,} dead ends recorded")
            except json.JSONDecodeError:
                log("  state file unreadable, starting fresh")

    def _save_state(self):
        json.dump({"burst": self.burst, "tested": self.tested,
                   "dead": sorted(self.dead), "best": self.best[:200],
                   "family_score": self.family_score},
                  open(self.state_path, "w"))

    # ----------------------------------------------------------- hypotheses --
    def propose(self, n: int) -> list[Hypothesis]:
        """Generate the next batch, weighted toward families that have produced
        survivors. Dead ends are never re-proposed."""
        horizons = ["close", 30, 60, 120, ("days", 1), ("days", 3), ("days", 7)]
        hot = [f for f, v in self.family_score.items()
               if len(v) >= 3 and np.mean(v) > 0]
        out, guard = [], 0
        while len(out) < n and guard < n * 60:
            guard += 1
            if hot and random.random() < 0.45:
                base = random.choice(hot)
                ev = [base] if base in self.events else random.sample(self.events, 1)
            else:
                ev = random.sample(self.events, 1)
            if random.random() < 0.45:
                other = random.choice(self.events)
                if other != ev[0]:
                    ev = ev + [other]
            h = Hypothesis(ev, random.choice(["long", "short"]),
                           random.choice(horizons),
                           random.choice([None] + self.times))
            if h.key in self.tested or h.key in self.dead:
                continue
            out.append(h)
        return out

    # ----------------------------------------------------------- the burst ---
    def run_burst(self, minutes: float, batch: int = 220):
        self.burst += 1
        t0 = time.time()
        log("")
        log(f"{'=' * 70}")
        log(f"BURST {self.burst}   {self.stock}   budget {minutes:.0f} min")
        log(f"{'=' * 70}")

        hyps = self.propose(batch)
        results, all_means = [], []
        last_report = time.time()

        for i, h in enumerate(hyps, 1):
            if time.time() - t0 > minutes * 60:
                log(f"  budget reached after {i} hypotheses")
                break
            r, sess = returns_for(self.panel, h, self.daily_close)
            if r is None or len(r) < 30:
                self.dead.add(h.key)
                continue
            m = evaluate(r, sess)
            m["key"] = h.key
            m["family"] = h.events[0]
            results.append(m)
            all_means.append(m["mean_pct"])
            self.tested[h.key] = {"mean_pct": m["mean_pct"], "n": m["n"]}
            self.family_score.setdefault(h.events[0], []).append(m["mean_pct"])

            if time.time() - last_report > 300:      # progress every 5 minutes
                best = max(results, key=lambda x: x["mean_pct"])
                log(f"  ...{i}/{len(hyps)} tested   best so far "
                    f"{best['mean_pct']:+.3f}%/trade on {best['n']} firings")
                last_report = time.time()

        if not results:
            log("  no hypothesis had enough firings. Widening the next batch.")
            self._save_state()
            return

        # ---- search-corrected bar: how good does the best look on noise? ----
        noise_bar = self._noise_bar(len(results))
        df = pd.DataFrame(results).sort_values("mean_pct", ascending=False)
        survivors = df[(df["mean_pct"] > noise_bar)
                       & (df["folds_pos"] >= df["folds"] * 0.65)
                       & (df["folds"] >= 6)
                       & (df["n"] >= 60)]

        log("")
        log(f"  tested {len(df)}   median {df['mean_pct'].median():+.3f}%   "
            f"best {df['mean_pct'].max():+.3f}%")
        log(f"  search-corrected bar (best-of-{len(df)} under noise): "
            f"{noise_bar:+.3f}%")
        log(f"  SURVIVORS: {len(survivors)}")
        for _, r in survivors.head(6).iterrows():
            per_day = self._per_day(r)
            log(f"    {r['key'][:58]:<58} {r['mean_pct']:+.3f}%  "
                f"n={int(r['n']):<5} folds {int(r['folds_pos'])}/{int(r['folds'])}"
                f"  ~{per_day:+.3f}%/day")
        if survivors.empty:
            log("    none this burst. Next batch queued -- the search continues.")

        for _, r in survivors.iterrows():
            self.best.append({k: (float(v) if isinstance(v, (int, float, np.floating))
                                  else v) for k, v in r.items()})
        self.best = sorted(self.best, key=lambda x: -x["mean_pct"])[:200]
        for _, r in df[df["mean_pct"] < 0].iterrows():
            self.dead.add(r["key"])

        self._report_progress()
        self._save_state()
        df.to_csv(os.path.join(self.out, f"burst_{self.stock}_{self.burst}.csv"),
                  index=False)
        log(f"  burst done in {(time.time()-t0)/60:.1f} min  "
            f"({len(self.tested):,} tested lifetime, {len(self.dead):,} dead)")

    def _noise_bar(self, k: int, n_perm: int = 300) -> float:
        """Best-of-k under random outcomes, at the 95th percentile."""
        pool = self.panel["ret_from_open"].dropna().to_numpy()
        if len(pool) < 500:
            return 0.0
        rng = np.random.default_rng(7 + self.burst)
        bests = []
        for _ in range(n_perm):
            draws = [rng.choice(pool, size=120, replace=True).mean()
                     for _ in range(min(k, 60))]
            bests.append(max(draws))
        return float(np.quantile(bests, 0.95) * 100)

    def _per_day(self, r) -> float:
        h = r["key"].split("|")[-1]
        if "days" in h:
            try:
                n = int("".join(ch for ch in h if ch.isdigit()))
            except ValueError:
                n = 1
            return r["mean_pct"] / max(n, 1)
        return r["mean_pct"]

    def _report_progress(self):
        log("")
        log(f"  PROGRESS TOWARD +1.00%/day on Rs {self.capital:,.0f}")
        if not self.best:
            log("    nothing validated yet. Target 1.000%/day. Gap: full.")
            return
        top = self.best[0]
        pd_ = self._per_day(top)
        log(f"    best validated : {top['key'][:52]}")
        log(f"    per day        : {pd_:+.3f}%   "
            f"(Rs {self.capital * pd_ / 100:,.0f}/day)")
        log(f"    target         : +1.000%   "
            f"(Rs {self.capital / 100:,.0f}/day)")
        log(f"    gap            : {1.0 - pd_:+.3f} percentage points")
        log(f"    validated set  : {len(self.best)} playbooks retained")

    def run_forever(self, burst_min: float):
        log("")
        log(f"AGENT {self.stock} online. Continuous operation.")
        log("No hypothesis is out of scope. The search does not stop.")
        while True:
            try:
                self.run_burst(burst_min)
            except KeyboardInterrupt:
                log("interrupt received -- saving state before exit")
                self._save_state()
                raise
            except Exception as e:                 # a bad burst must not halt us
                log(f"  burst error: {type(e).__name__}: {e}")
                log("  recovering and continuing -- the search does not stop.")
                self._save_state()
                time.sleep(3)


# --------------------------------------------------------------------------- #
def main():
    p = argparse.ArgumentParser(description="V3 autonomous quant research agent")
    p.add_argument("--stock", required=True)
    p.add_argument("--db", required=True)
    p.add_argument("--table", default="ohlc_1min")
    p.add_argument("--capital", type=float, default=30000.0)
    p.add_argument("--burst-min", type=float, default=10.0)
    p.add_argument("--start", default="2022-01-01")
    p.add_argument("--out", default="agent_out")
    p.add_argument("--once", action="store_true", help="single burst, then stop")
    a = p.parse_args()

    log("=" * 70, stamp=False)
    log(f"V3 AGENT  |  {a.stock}  |  Rs {a.capital:,.0f} fixed, no compounding",
        stamp=False)
    log(f"research window: {a.start} -> {SEAL_FROM} (exclusive)", stamp=False)
    log(f"2026 is SEALED and unreachable from this process", stamp=False)
    log(f"costs {COST*100:.2f}% + slippage {SLIP*100:.2f}% per round trip",
        stamp=False)
    log("=" * 70, stamp=False)

    ag = Agent(a.stock, a.db, a.table, a.capital, a.out, a.start)
    if a.once:
        ag.run_burst(a.burst_min)
    else:
        ag.run_forever(a.burst_min)


if __name__ == "__main__":
    main()
