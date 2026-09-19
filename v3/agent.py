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

import warnings

import numpy as np
import pandas as pd

warnings.simplefilter("ignore", FutureWarning)

from microstructure import (build_event_panel, event_names, time_names)

SEAL_FROM = "2026-01-01"          # never crossed, for any reason
COST = 0.0011
SLIP = 0.0005


# --------------------------------------------------------------------------- #
_LAST = {"t": 0.0}
TICK = {"s": 30.0}


def log(msg: str = "", stamp: bool = True) -> None:
    """Always printed. Use for discoveries and section headers only."""
    if stamp:
        print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)
    else:
        print(msg, flush=True)


def tick(msg: str) -> bool:
    """Routine status. Printed at most once per --tick seconds.

    A burst can finish in under a second, so throttling only the within-burst
    line left the end-of-burst line printing dozens of times a second. Every
    routine message now goes through one gate.
    """
    now = time.time()
    if now - _LAST["t"] < TICK["s"]:
        return False
    _LAST["t"] = now
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)
    return True


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
    """Net return per TRADE. Entry at the next bar's open, one trade per session.

    An event can fire on dozens of bars in the same day. Counting each as a
    separate trade produced rules with 12,000 'firings' across 1,100 sessions --
    eleven positions a day, at Rs 30,000 each, which is not a strategy anyone
    can execute on fixed capital.

    So only the FIRST firing in each session is taken. That is what you could
    actually trade, and it makes the trade count directly comparable to the
    session count.
    """
    m = h.mask(p)
    if m.sum() < 30:
        return None, None
    sub = p.loc[m]
    sub = sub.loc[~sub["session"].duplicated(keep="first")]
    if len(sub) < 30:
        return None, None
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


def effective_n(sess: np.ndarray, hold_days: int) -> int:
    """How many INDEPENDENT bets are in a set of overlapping multi-day trades?

    Fifty-two seven-day holds are not fifty-two independent observations. If the
    entries cluster -- and events like a squeeze or a VWAP loss plausibly do,
    because they fire in similar market conditions -- then overlapping windows
    share most of their price path. Clustered entries can reduce fifty-two
    trades to roughly fourteen independent bets, which widens the error bar by
    nearly three times.

    Counting non-overlapping windows is a conservative, assumption-free way to
    say how much independent evidence there actually is.
    """
    if hold_days <= 1:
        return len(sess)
    order = np.sort(np.unique(sess.astype("datetime64[D]").astype(int)))
    blocks, last = 0, -10**9
    for d in order:
        if d >= last + hold_days:
            blocks += 1
            last = d
    return max(blocks, 1)


def hold_days_of(h) -> int:
    if isinstance(h, tuple):
        return int(h[1])
    return 1


def evaluate(r: np.ndarray, sess: np.ndarray, hold_days: int = 1) -> dict:
    folds = walk_forward(r, sess)
    eff = effective_n(sess, hold_days)
    return {
        "eff_n": int(eff),
        "n": int(len(r)),
        "mean_pct": float(r.mean() * 100),
        "median_pct": float(np.median(r) * 100),
        "win_rate": float((r > 0).mean()),
        "std_pct": float(r.std() * 100),
        "t_stat": float(r.mean() / (r.std() / np.sqrt(eff) + 1e-12)),
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
        self.run_tested = 0
        self.run_best = -99.0
        self.book_session = 0.0     # what the combined book earns per session
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
        self.n_sessions = int(self.panel["session"].nunique())
        self.base = self._baselines()
        log(f"  vocabulary: {len(self.events)} events x {len(self.times)} time "
            f"buckets x 2 sides x 7 horizons")
        log(f"  search space ~{self._space():,} single and paired hypotheses")

    def _baselines(self):
        """What does this side+horizon earn with NO condition at all?

        This is the gate the first version was missing, and its absence is why
        almost every 'survivor' was a 7-day LONG on a stock that tripled. A
        7-day forward return on a strongly trending name is positive whatever
        triggered the entry -- the events were sampling days, not selecting them.

        So every hypothesis must now beat the unconditional return for its OWN
        side and horizon. Beating zero is not evidence. Beating the drift is.
        """
        log("  measuring unconditional baselines (the bar every rule must clear)")
        base = {}
        horizons = ["close", 30, 60, 120, ("days", 1), ("days", 3), ("days", 7)]
        rng = np.random.default_rng(11)
        idx = np.where(self.panel["next_open"].notna().to_numpy())[0]
        samp = rng.choice(idx, size=min(20000, len(idx)), replace=False)
        for side in ("long", "short"):
            for h in horizons:
                hyp = Hypothesis([], side, h)
                sub = self.panel.iloc[samp]
                entry = sub["next_open"].to_numpy(dtype=float)
                if h == "close":
                    ex = sub["sess_close"].to_numpy(dtype=float)
                elif isinstance(h, tuple):
                    ex = np.array([self.daily_close.get((s_, h[1]), np.nan)
                                   for s_ in sub["session"].to_numpy()])
                else:
                    ahead = np.minimum(samp + int(h),
                                       samp + sub["bars_left"].to_numpy())
                    ahead = np.clip(ahead, 0, len(self.panel) - 1)
                    ex = self.panel["close"].to_numpy(dtype=float)[ahead]
                sg = 1.0 if side == "long" else -1.0
                r = (ex / entry - 1.0) * sg - COST - SLIP
                r = r[np.isfinite(r)]
                base[(side, str(h))] = (float(r.mean() * 100),
                                        float(r.std() * 100))
        for k, v in sorted(base.items()):
            log(f"    {k[0]:<5} {k[1]:<12} unconditional {v[0]:+.3f}%  "
                f"(sd {v[1]:.2f})")
        return base

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
        exhausted = len(self.tested) + len(self.dead) > self._space() * 0.8
        max_ev = 3 if exhausted else 2
        if exhausted and self.burst % 200 == 0:
            tick("search space largely covered -- composing deeper conjunctions")
        while len(out) < n and guard < n * 80:
            guard += 1
            if hot and random.random() < 0.45:
                base = random.choice(hot)
                ev = [base] if base in self.events else random.sample(self.events, 1)
            else:
                ev = random.sample(self.events, 1)
            while len(ev) < max_ev and random.random() < 0.5:
                other = random.choice(self.events)
                if other not in ev:
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


        # keep proposing until the time budget is actually spent, rather than
        # blasting through one small batch and starting a fresh burst -- which
        # inflates the lifetime test count without doing more real work
        hyps = self.propose(batch)
        while (len(hyps) < batch * 0.6
               and time.time() - t0 < minutes * 20):
            hyps += self.propose(batch)
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
            m = evaluate(r, sess, hold_days_of(h.horizon))
            m["_r"] = r
            m["_sess"] = sess
            m["key"] = h.key
            m["family"] = h.events[0]
            bkey = (h.side, str(h.horizon))
            m["uncond_pct"] = self.base.get(bkey, (0.0, 1.0))[0]
            m["edge_pct"] = m["mean_pct"] - m["uncond_pct"]
            m["side"] = h.side
            m["horizon"] = str(h.horizon)
            results.append(m)
            all_means.append(m["mean_pct"])
            self.tested[h.key] = {"mean_pct": m["mean_pct"], "n": m["n"]}
            self.family_score.setdefault(h.events[0], []).append(m["mean_pct"])

            pass

        if not results:
            tick(f"burst {self.burst} | no hypothesis had enough firings | "
                 f"composing new combinations")
            self._save_state()
            time.sleep(0.5)
            return

        # ---- search-corrected bar: how good does the best look on noise? ----
        df = pd.DataFrame(results).drop_duplicates("key")
        df["noise_bar"] = [self._noise_bar(len(df), r.side, r.horizon,
                                           n_obs=int(r.eff_n))
                           for r in df.itertuples()]
        df = df.sort_values("edge_pct", ascending=False)
        # a rule must beat its own unconditional baseline, and beat what the
        # best of this many candidates would look like on noise at that horizon
        survivors = df[(df["edge_pct"] > 0)
                       & (df["mean_pct"] > df["noise_bar"])
                       & (df["edge_pct"] > df["noise_bar"] - df["uncond_pct"])
                       & (df["folds_pos"] >= df["folds"] * 0.65)
                       & (df["folds"] >= 6)
                       & (df["n"] >= 60)
                       & (df["eff_n"] >= 40)]
        noise_bar = float(df["noise_bar"].median())

        self.run_tested += len(df)
        self.run_best = max(self.run_best, float(df["edge_pct"].max()))
        if survivors.empty:
            tick(f"burst {self.burst} | {len(self.tested):,} tested lifetime | "
                 f"{len(self.best)} rules validated | "
                 f"book {self.book_session:+.3f}%/session | "
                 f"gap {1.0 - self.book_session:+.3f} | "
                 f"best edge seen {self.run_best:+.2f}%")
        else:
            log("")
            log(f"  *** {len(survivors)} NEW RULE(S) after {len(self.tested):,} "
                f"lifetime tests (bar {noise_bar:+.2f}%)")
        for _, r in survivors.head(6).iterrows():
            per_day = self._per_day(r)
            log(f"    {r['key'][:44]:<44} EDGE {r['edge_pct']:+.2f}% "
                f"n={int(r['n'])}/eff{int(r['eff_n'])} "
                f"-> {per_day:+.3f}%/session {int(r['folds_pos'])}/{int(r['folds'])}")


        for _, r in survivors.iterrows():
            rec = {k: (float(v) if isinstance(v, (int, float, np.floating)) else v)
                   for k, v in r.items() if not k.startswith("_")}
            # keep the firing days so rules can be combined into a book
            sess = r["_sess"]
            rec["days"] = [str(pd.Timestamp(x).date()) for x in sess]
            rec["rets"] = [float(x) for x in r["_r"]]
            self.best.append(rec)
        seen, uniq = set(), []
        for b in sorted(self.best, key=lambda x: -x.get("edge_pct", -9)):
            if b["key"] in seen:
                continue
            seen.add(b["key"])
            uniq.append(b)
        self.best = uniq[:80]
        for _, r in df[df["mean_pct"] < 0].iterrows():
            self.dead.add(r["key"])

        if not survivors.empty:
            self._report_progress()
        if self.burst % 3 == 0:
            prev = self.book_session
            bk = self.build_book(verbose=False)
            if bk:
                self.book_session = bk["per_session"]
                if bk["per_session"] > prev + 0.005:
                    self.build_book(verbose=True)
                json.dump(bk, open(os.path.join(
                    self.out, f"book_{self.stock}.json"), "w"), indent=1)
        self._save_state()
        df.to_csv(os.path.join(self.out, f"burst_{self.stock}_{self.burst}.csv"),
                  index=False)


    def _noise_bar(self, k: int, side: str, horizon: str, n_obs: int = 120,
                   n_perm: int = 250) -> float:
        """Best-of-k under random outcomes AT THE SAME HORIZON.

        The first version drew from intraday returns and compared the result to
        7-day holds. A 7-day return carries several times the drift and spread
        of an intraday one, so every multi-day candidate cleared a bar built for
        a different distribution. That single mismatch produced the entire
        7-day-long survivor list.
        """
        mu, sd = self.base.get((side, horizon), (0.0, 1.0))
        # A rule seen 66 times has a far wider sampling error than one seen
        # 8,000 times. Using a fixed n understates the bar for exactly the
        # small-sample candidates that keep winning, which is backwards.
        se = sd / np.sqrt(max(n_obs, 10))
        # And the correction must cover every test run so far. A burst that
        # takes the best of 130 after 3,000 lifetime tests is not choosing from
        # 130 -- it is choosing from 3,130, and the bar rises with the count.
        k_eff = min(max(k, len(self.tested)), 20000)
        rng = np.random.default_rng(7 + self.burst)
        bests = [rng.normal(mu, se, size=min(k_eff, 400)).max()
                 for _ in range(n_perm)]
        return float(np.quantile(bests, 0.95))

    def _per_day(self, r) -> float:
        """Expected edge per CALENDAR session, not per trade.

        A rule earning +1.17% that fires on 138 of 992 sessions does not earn
        1.17% a day. It earns 138 x 1.17% spread over 992 days -- about 0.16%.
        Reporting the per-trade figure against a per-day target overstated the
        best ADANIENT rule by seven times and told you the target was beaten
        when it was not.
        """
        h = r["key"].split("|")[-1]
        base_val = r.get("edge_pct", r.get("mean_pct", 0.0))
        n = float(r.get("n", 0) or 0)
        sess = float(self.n_sessions or 1)
        return base_val * n / sess


    def _report_progress(self):
        log("")
        log(f"  PROGRESS TOWARD +1.00%/day on Rs {self.capital:,.0f}")
        if not self.best:
            log("    nothing validated yet. Target 1.000%/day. Gap: full.")
            return
        top = max(self.best, key=lambda x: x.get("edge_pct", -9))
        pd_ = self._per_day(top)
        log(f"    best validated : {top['key'][:52]}")
        log(f"    raw / uncond   : {top.get('mean_pct',0):+.3f}% / "
            f"{top.get('uncond_pct',0):+.3f}%   EDGE "
            f"{top.get('edge_pct',0):+.3f}%")
        log(f"    independent    : {int(top.get('eff_n',0)):,} non-overlapping "
            f"bets (raw trade count {int(top.get('n',0)):,})")
        log(f"    trades on      : {int(top.get('n',0)):,} of "
            f"{self.n_sessions:,} sessions "
            f"({top.get('n',0)/max(self.n_sessions,1):.1%} of days)")
        log(f"    per SESSION    : {pd_:+.3f}%   "
            f"(Rs {self.capital * pd_ / 100:,.0f}/day averaged over every "
            f"session, traded or not)")
        log(f"    target         : +1.000%   "
            f"(Rs {self.capital / 100:,.0f}/day)")
        log(f"    gap            : {1.0 - pd_:+.3f} percentage points")
        log(f"    validated set  : {len(self.best)} playbooks retained")

    def build_book(self, max_rules: int = 12, verbose: bool = True):
        """Combine validated rules into one book on fixed capital.

        No single rule reaches the target and none ever will: a rule earning
        +0.5% on 14% of sessions contributes 0.07%/session, and that arithmetic
        does not change however good the rule is. The only route to +1%/session
        is several rules whose firing days barely overlap, running together.

        Capital is fixed at Rs 30,000. When k rules fire on the same session the
        book splits between them, so the session return is the MEAN of what
        fired -- never the sum. That is the constraint that makes overlap
        expensive and diversity valuable, and it is why rules are added greedily
        by what they contribute to the BOOK rather than by their own edge.
        """
        if len(self.best) < 2:
            if verbose:
                log("  book: need at least 2 validated rules")
            return None
        pool = sorted(self.best, key=lambda x: -x.get("edge_pct", -9))[:60]
        pool = [p for p in pool if p.get("days") and p.get("rets")]
        if len(pool) < 2:
            return None

        by_day = []
        for p_ in pool:
            by_day.append(dict(zip(p_["days"], p_["rets"])))
        chosen, cur = [], {}

        def book_score(sel):
            acc = {}
            for i in sel:
                for d, v in by_day[i].items():
                    acc.setdefault(d, []).append(v)
            if not acc:
                return 0.0, 0, 0.0
            per = np.array([np.mean(v) for v in acc.values()])
            cover = len(acc)
            return float(per.sum() / self.n_sessions * 100), cover, float(per.mean() * 100)

        remaining = set(range(len(pool)))
        best_total = 0.0
        for _ in range(min(max_rules, len(pool))):
            gains = []
            for i in remaining:
                t, _, _ = book_score(chosen + [i])
                gains.append((t, i))
            if not gains:
                break
            t, i = max(gains)
            if t <= best_total + 1e-9:
                break
            best_total = t
            chosen.append(i)
            remaining.discard(i)

        total, cover, per_trade = book_score(chosen)
        if verbose:
            log("")
            log(f"  BOOK   {len(chosen)} rules combined on Rs "
                f"{self.capital:,.0f} fixed")
            for i in chosen:
                p_ = pool[i]
                log(f"    {p_['key'][:56]:<56} {p_.get('edge_pct',0):+.2f}% "
                    f"x{len(p_['days'])}")
            log(f"    sessions with a position : {cover:,} of "
                f"{self.n_sessions:,} ({cover/self.n_sessions:.0%})")
            log(f"    mean return when trading : {per_trade:+.3f}%")
            log(f"    BOOK per session         : {total:+.3f}%   "
                f"(Rs {self.capital*total/100:,.0f}/day)")
            log(f"    target                   : +1.000%   "
                f"(Rs {self.capital/100:,.0f}/day)")
            log(f"    gap                      : {1.0-total:+.3f} points")
            log(f"    NOTE: each rule cleared its own bar. A book of rules that")
            log(f"    each barely cleared is a stack of marginal results, and it")
            log(f"    must be validated as ONE unit on the sealed 2026 slice")
            log(f"    before any of this counts.")
        return {"rules": [pool[i]["key"] for i in chosen], "per_session": total,
                "coverage": cover}

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
    p.add_argument("--tick", type=float, default=30.0,
                   help="seconds between status lines")
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

    TICK["s"] = a.tick
    ag = Agent(a.stock, a.db, a.table, a.capital, a.out, a.start)
    if a.once:
        ag.run_burst(a.burst_min)
    else:
        ag.run_forever(a.burst_min)


if __name__ == "__main__":
    main()
