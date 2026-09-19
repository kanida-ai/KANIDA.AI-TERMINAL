"""A persistent registry of discovered states, tracked across research runs.

WHY THIS AND NOT JUST A REPORT
------------------------------
Walk-forward tells you a state survived unseen TIME. It says nothing about
whether the state survives a different UNIVERSE, a different feature set, or a
different set of choices you made while searching. Those are separate failure
modes, and a single run cannot test them.

Rediscovery can. Run the engine again next month with more symbols, a different
bin count, a different training window. If the same state keeps surfacing near
the top, that is evidence a single backtest cannot produce -- because each run
is a fresh chance for the state to fail.

WHAT THE CONFIDENCE NUMBER IS
-----------------------------
Every term is measured. There is no judgement in it and no free parameters
chosen to make the output look good:

    rediscovery   runs where the state appeared / runs where it COULD have
    consistency   walk-forward folds positive / folds tested, pooled
    support       total occurrences, log-scaled and capped
    steadiness    1 - (spread of its mean return across runs / |mean|)

    confidence = rediscovery x consistency x support x steadiness

Multiplicative, so a zero on any dimension is a zero overall. A state that looks
brilliant once and never returns scores near zero, which is the correct answer.

Do not hand-write a confidence score. A number without a method behind it is
worse than no number, because it looks like evidence.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

SCHEMA = 2


# --------------------------------------------------------------------------- #
class StateRegistry:
    def __init__(self, path: str = "outputs/state_registry.json"):
        self.path = path
        self.data = {"schema": SCHEMA, "runs": [], "states": {}}
        if os.path.exists(path):
            try:
                with open(path) as fh:
                    loaded = json.load(fh)
                if loaded.get("schema") == SCHEMA:
                    self.data = loaded
                else:
                    print(f"  registry schema changed; starting fresh at {path}")
            except json.JSONDecodeError:
                print(f"  registry at {path} unreadable; starting fresh")

    # ---------------------------------------------------------------- record #
    def record_run(self, run_id: str, settings: dict, states: pd.DataFrame,
                   key: str = "state") -> None:
        """Log one research run and everything it discovered.

        `states` needs a `key` column plus any of: n, win_rate, ret, folds,
        folds_positive, holdout_ret.
        """
        stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.data["runs"].append({"run_id": run_id, "when": stamp,
                                  "settings": settings,
                                  "n_states": int(len(states))})
        for _, r in states.iterrows():
            sig = str(r[key])
            rec = self.data["states"].setdefault(sig, {
                "signature": sig, "first_seen": stamp, "observations": []})
            rec["last_seen"] = stamp
            rec["observations"].append({
                "run_id": run_id,
                "n": int(r.get("n", 0) or 0),
                "win_rate": _f(r.get("win_rate")),
                "ret": _f(r.get("ret")),
                "folds": int(r.get("folds", 0) or 0),
                "folds_positive": int(r.get("folds_positive", 0) or 0),
                "holdout_ret": _f(r.get("holdout_ret")),
            })

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        with open(self.path, "w") as fh:
            json.dump(self.data, fh, indent=1)

    # ------------------------------------------------------------ evaluation #
    def table(self) -> pd.DataFrame:
        n_runs = len(self.data["runs"])
        if not self.data["states"]:
            return pd.DataFrame()
        rows = []
        for sig, rec in self.data["states"].items():
            obs = pd.DataFrame(rec["observations"])
            runs_seen = obs["run_id"].nunique()
            # a state could only be found in runs at or after its first sighting
            first_idx = next((i for i, r in enumerate(self.data["runs"])
                              if r["run_id"] == obs["run_id"].iloc[0]), 0)
            eligible = max(n_runs - first_idx, 1)

            rets = obs["ret"].dropna()
            mean_ret = float(rets.mean()) if len(rets) else np.nan
            spread = float(rets.std()) if len(rets) > 1 else 0.0
            steadiness = (1.0 / (1.0 + spread / abs(mean_ret))
                          if mean_ret and abs(mean_ret) > 1e-9 else 0.0)

            folds = float(obs["folds"].sum())
            fpos = float(obs["folds_positive"].sum())
            consistency = fpos / folds if folds else 0.0
            n_total = float(obs["n"].sum())
            support = min(1.0, np.log1p(n_total) / np.log1p(1000.0))
            rediscovery = runs_seen / eligible

            conf = rediscovery * consistency * support * max(steadiness, 0.0)
            if mean_ret is not np.nan and mean_ret <= 0:
                conf = 0.0

            rows.append({
                "signature": sig[:70], "runs_seen": runs_seen,
                "runs_eligible": eligible, "rediscovery": rediscovery,
                "n_total": int(n_total), "mean_ret_%": (mean_ret or 0) * 100,
                "ret_spread_%": spread * 100,
                "folds": int(folds), "folds_positive": int(fpos),
                "consistency": consistency, "steadiness": steadiness,
                "support": support, "confidence": conf,
                "status": _status(runs_seen, rediscovery, consistency, n_total, conf),
            })
        return (pd.DataFrame(rows)
                .sort_values("confidence", ascending=False)
                .reset_index(drop=True))

    def report(self, top: int = 20) -> pd.DataFrame:
        t = self.table()
        n_runs = len(self.data["runs"])
        print(f"\n{'=' * 100}\nSTATE REGISTRY   {len(self.data['states'])} states "
              f"across {n_runs} run(s)\n{'=' * 100}")
        if t.empty:
            print("  empty")
            return t
        if n_runs < 3:
            print(f"  Only {n_runs} run(s) recorded. Rediscovery cannot mean anything")
            print("  yet -- every state has been found in every run it could be. Re-run")
            print("  with a different universe, bin count or training window, and the")
            print("  confidence column starts carrying information.\n")
        cols = ["signature", "runs_seen", "rediscovery", "n_total", "mean_ret_%",
                "ret_spread_%", "folds", "folds_positive", "consistency",
                "confidence", "status"]
        print(t.head(top)[cols].round(3).to_string(index=False))
        print(f"\n  {t['status'].value_counts().to_dict()}")
        print("\n  confidence = rediscovery x consistency x support x steadiness.")
        print("  Every term is measured; none is a judgement call.")
        return t


def _f(v):
    try:
        f = float(v)
        return None if not np.isfinite(f) else f
    except (TypeError, ValueError):
        return None


def _status(runs_seen, rediscovery, consistency, n_total, conf) -> str:
    if conf <= 0:
        return "rejected"
    if runs_seen >= 4 and rediscovery >= 0.75 and consistency >= 0.65 and n_total >= 500:
        return "production"
    if runs_seen >= 3 and rediscovery >= 0.60 and consistency >= 0.60:
        return "validated"
    if runs_seen >= 2:
        return "recurring"
    return "candidate"
