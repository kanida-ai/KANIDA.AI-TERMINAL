"""V4 AGENT — learns the tape instead of enumerating rules I wrote.

    python v4_agent.py --stock ADANIENT --db path\\to\\kanida.db

WHAT CHANGED FROM V3, AND WHY IT MATTERS
----------------------------------------
V3 searched combinations of twenty booleans. Twenty-four thousand tests
established that those twenty shapes contain little -- which is a statement
about my vocabulary, not about the tape.

V4 predicts the forward return directly from ~120 numeric descriptors of the
last hour, including what the INDEX did in the same minutes. It finds its own
thresholds and interactions. Nothing is enumerated.

WHAT DELIBERATELY DID NOT CHANGE
--------------------------------
The statistical discipline. Every guard that pushed V3's numbers down stays,
because each one was correct:

  purged walk-forward   train strictly before test, with a gap so a 3-day
                        target cannot straddle the boundary
  shuffle control       the same pipeline on permuted targets must produce
                        nothing; if it does not, the pipeline is broken
  capital constraint    Rs 30,000 fixed. k positions on one day split it, so
                        the book earns the MEAN of what fired, never the sum
  cost and slippage     charged on every trade, both directions
  sealed holdout        2026 is unreachable from this process

A model that reports a large edge after dropping those guards has discovered
nothing. The guards are why the number means something.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
import warnings
from datetime import datetime

import numpy as np
import pandas as pd

warnings.simplefilter("ignore", FutureWarning)

from v4_features import (add_targets, build_panel, feature_names)

SEAL_FROM = "2026-01-01"
COST = 0.0011
SLIP = 0.0005
TARGETS = ["y_30", "y_60", "y_close", "y_d1", "y_d3"]
HOLD_DAYS = {"y_30": 1, "y_60": 1, "y_close": 1, "y_d1": 1, "y_d3": 3}


def log(m: str = "") -> None:
    print(f"[{datetime.now():%H:%M:%S}] {m}", flush=True)


def load(db: str, table: str, symbol: str, start: str) -> pd.DataFrame:
    con = sqlite3.connect(db)
    d = pd.read_sql_query(
        f'SELECT symbol,bar_time,open,high,low,close,volume FROM "{table}" '
        f'WHERE symbol=? AND bar_time>=? AND bar_time<?',
        con, params=[symbol, start, SEAL_FROM])
    con.close()
    if d.empty:
        return d
    d["ts"] = pd.to_datetime(d["bar_time"])
    d["session"] = d["ts"].dt.normalize()
    return d.sort_values("ts").reset_index(drop=True)


def pick_index(db: str, table: str) -> str | None:
    """Prefer a broad market index if one is present in the intraday table."""
    con = sqlite3.connect(db)
    try:
        names = [r[0] for r in con.execute(
            f'SELECT DISTINCT symbol FROM "{table}" LIMIT 2000')]
    except sqlite3.Error:
        return None
    finally:
        con.close()
    for want in ("NIFTY 50", "NIFTY50", "NIFTY BANK", "NIFTY 500",
                 "NIFTY MIDCAP 100"):
        for n in names:
            if n.strip().upper() == want:
                return n
    for n in names:
        if n.strip().upper().startswith("NIFTY"):
            return n
    return None


# --------------------------------------------------------------------------- #
class V4:
    def __init__(self, stock, db, table, capital, out, start, step):
        self.stock, self.capital, self.out = stock, capital, out
        os.makedirs(out, exist_ok=True)
        log(f"loading {stock} 1-minute bars (research ends {SEAL_FROM})")
        bars = load(db, table, stock, start)
        if bars.empty:
            raise RuntimeError(f"no bars for {stock}")
        log(f"  {len(bars):,} bars, {bars['session'].nunique()} sessions")

        idx_name = pick_index(db, table)
        idx = load(db, table, idx_name, start) if idx_name else None
        log(f"  index context: {idx_name if idx_name is not None else 'NONE FOUND'}")

        self.panel = build_panel(bars, idx, step=step)
        cl = bars.groupby("session")["close"].last()
        ss = list(cl.index)
        dmap = {}
        for i, s in enumerate(ss):
            for n in (1, 3):
                dmap[(s, n)] = float(cl.iloc[i + n]) if i + n < len(ss) else np.nan
        self.panel = add_targets(self.panel, bars, dmap, COST + SLIP)
        self.feats = feature_names(self.panel)
        self.n_sessions = int(self.panel["session"].nunique())
        log(f"  {len(self.panel):,} decision points, {len(self.feats)} features, "
            f"{self.n_sessions} sessions")

    # ------------------------------------------------------------ modelling --
    def _folds(self, n_folds=8, purge_days=5):
        s = np.sort(self.panel["session"].unique())
        cuts = [s[int(len(s) * (i + 1) / (n_folds + 1))] for i in range(n_folds)]
        for i, c in enumerate(cuts):
            end = cuts[i + 1] if i + 1 < len(cuts) else s[-1] + np.timedelta64(1, "D")
            # purge: drop training rows whose target could straddle the boundary
            tr_end = c - np.timedelta64(purge_days, "D")
            yield tr_end, c, end

    def run(self, target: str, side: str, top_frac: float, shuffle: bool = False):
        from sklearn.ensemble import HistGradientBoostingRegressor
        p = self.panel[self.panel[target].notna()].copy()
        if shuffle:
            rng = np.random.default_rng(3)
            p[target] = rng.permutation(p[target].to_numpy())
        sgn = 1.0 if side == "long" else -1.0
        X_all = p[self.feats].to_numpy(np.float32)
        y_all = (p[target].to_numpy(float)) * sgn
        picks = []
        for tr_end, te_start, te_end in self._folds():
            tr = (p["session"] < tr_end).to_numpy()
            te = ((p["session"] >= te_start) & (p["session"] < te_end)).to_numpy()
            if tr.sum() < 3000 or te.sum() < 200:
                continue
            m = HistGradientBoostingRegressor(
                max_iter=220, max_depth=5, learning_rate=0.06,
                min_samples_leaf=60, l2_regularization=1.0, random_state=7)
            m.fit(X_all[tr], y_all[tr])
            pr = m.predict(X_all[te])
            sub = p.loc[te].assign(pred=pr, real=y_all[te])
            # one position per session: the highest-conviction minute
            best = (sub.sort_values(["session", "pred"], ascending=[True, False])
                       .groupby("session").head(1))
            k = max(int(len(best) * top_frac), 1)
            picks.append(best.nlargest(k, "pred")[["session", "pred", "real"]])
        if not picks:
            return None
        pk = pd.concat(picks, ignore_index=True)
        return {"n": len(pk), "mean_pct": float(pk["real"].mean() * 100),
                "win": float((pk["real"] > 0).mean()),
                "per_session": float(pk["real"].sum() / self.n_sessions * 100),
                "picks": pk}

    # ------------------------------------------------------------- the book --
    def sweep(self, top_frac: float):
        rows, books = [], {}
        for target in TARGETS:
            for side in ("long", "short"):
                r = self.run(target, side, top_frac)
                if r is None:
                    continue
                sh = self.run(target, side, top_frac, shuffle=True)
                r["shuffle_pct"] = sh["mean_pct"] if sh else 0.0
                r["target"], r["side"] = target, side
                books[(target, side)] = r.pop("picks")
                rows.append(r)
        if not rows:
            return None, None
        df = pd.DataFrame(rows).sort_values("per_session", ascending=False)
        return df, books

    def build_book(self, df, books, max_legs=5):
        """Combine legs on fixed capital: same-day positions SPLIT the money."""
        cand = df[(df["mean_pct"] > 0)
                  & (df["mean_pct"] > df["shuffle_pct"] + 0.05)]
        if cand.empty:
            return None
        chosen, acc = [], {}
        for _ in range(min(max_legs, len(cand))):
            best, best_key, best_acc = None, None, None
            for _, r in cand.iterrows():
                key = (r["target"], r["side"])
                if key in [c[0] for c in chosen]:
                    continue
                trial = {k: list(v) for k, v in acc.items()}
                for s, v in zip(books[key]["session"], books[key]["real"]):
                    trial.setdefault(s, []).append(v)
                tot = sum(np.mean(v) for v in trial.values()) / self.n_sessions * 100
                if best is None or tot > best:
                    best, best_key, best_acc = tot, key, trial
            if best_key is None or (chosen and best <= chosen[-1][1] + 1e-9):
                break
            chosen.append((best_key, best))
            acc = best_acc
        if not chosen:
            return None
        per = np.array([np.mean(v) for v in acc.values()])
        return {"legs": [f"{k[0]}|{k[1]}" for k, _ in chosen],
                "per_session_pct": float(per.sum() / self.n_sessions * 100),
                "sessions_traded": int(len(acc)),
                "mean_when_trading_pct": float(per.mean() * 100)}


# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stock", required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--table", default="ohlc_1min")
    ap.add_argument("--capital", type=float, default=30000.0)
    ap.add_argument("--start", default="2022-01-01")
    ap.add_argument("--step", type=int, default=5)
    ap.add_argument("--out", default="v4_out")
    ap.add_argument("--top-frac", type=float, default=0.35,
                    help="share of sessions to actually trade")
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args()

    print("=" * 72, flush=True)
    print(f"V4 AGENT  |  {a.stock}  |  Rs {a.capital:,.0f} fixed", flush=True)
    print(f"learns forward returns from the tape; does not enumerate rules",
          flush=True)
    print(f"2026 SEALED  |  costs {(COST+SLIP)*100:.2f}% round trip", flush=True)
    print("=" * 72, flush=True)

    ag = V4(a.stock, a.db, a.table, a.capital, a.out, a.start, a.step)
    round_no = 0
    fracs = [a.top_frac, 0.2, 0.5, 0.1, 0.35]
    while True:
        round_no += 1
        tf = fracs[(round_no - 1) % len(fracs)]
        log("")
        log(f"ROUND {round_no}   trading the top {tf:.0%} of sessions")
        t0 = time.time()
        df, books = ag.sweep(tf)
        if df is None:
            log("  no usable model this round")
            if a.once:
                break
            continue
        show = df[["target", "side", "n", "mean_pct", "win",
                   "shuffle_pct", "per_session"]].head(6)
        for _, r in show.iterrows():
            flag = "" if r["mean_pct"] > r["shuffle_pct"] + 0.05 else "  <- no better than shuffled"
            log(f"    {r['target']:<8} {r['side']:<5} n={int(r['n']):<5} "
                f"{r['mean_pct']:+.3f}%/trade  win {r['win']:.0%}  "
                f"shuffled {r['shuffle_pct']:+.3f}%  "
                f"-> {r['per_session']:+.3f}%/session{flag}")
        bk = ag.build_book(df, books)
        if bk:
            log("")
            log(f"  BOOK  {' + '.join(bk['legs'])}")
            log(f"    trades on {bk['sessions_traded']:,} of {ag.n_sessions:,} "
                f"sessions, mean {bk['mean_when_trading_pct']:+.3f}% when trading")
            log(f"    PER SESSION {bk['per_session_pct']:+.3f}%   "
                f"(Rs {a.capital*bk['per_session_pct']/100:,.0f}/day)")
            log(f"    target +1.000% (Rs {a.capital/100:,.0f}/day)   "
                f"gap {1.0-bk['per_session_pct']:+.3f}")
            json.dump(bk, open(os.path.join(a.out, f"book_{a.stock}.json"), "w"),
                      indent=1)
        else:
            log("  no leg beat its own shuffled control. Nothing to combine.")
        df.drop(columns=[c for c in df.columns if c == "picks"], errors="ignore") \
          .to_csv(os.path.join(a.out, f"sweep_{a.stock}_{round_no}.csv"), index=False)
        log(f"  round done in {(time.time()-t0)/60:.1f} min")
        if a.once:
            break


if __name__ == "__main__":
    main()
