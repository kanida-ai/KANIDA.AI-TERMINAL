"""
KANIDA unified engine — MINER. Outcome-first discovery over the combined micro+macro feature frame.
Builds the frame (features.build_from_db → atoms self-compute ATP etc.), labels with outcomes, mines
combination rules (shallow RF leaf-rules), gates on occurrence+lift, promotes on val 2025, confirms on
sealed 2026 → Keep/Watch/Test/Retire. Discovers arbitrary per-stock behaviours over the neutral basis
(micro-structure + institutional + macro) — nothing hand-coded.

Run: PYTHONIOENCODING=utf-8 python miner.py SYMBOL [SYMBOL ...]
"""
import sys, json, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
from sklearn.ensemble import RandomForestClassifier
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\scripts")))
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\kanida_engine")))
from mine_phase1 import leaf_rules, apply_rule
import features as FE

TRAIN_MAX, VAL, VAULT = 2024, 2025, 2026
MIN_TR, LIFT_TR, MIN_VA, LIFT_VA = 40, 5.0, 12, 2.0
SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"


def mine_stock(symbol, lookback_N=5):
    frame = FE.build_from_db(symbol, lookback_N=lookback_N)
    if frame.empty or len(frame) < 300:
        return frame, []
    feats = [c for c in frame.columns if c not in ("_o", "_h", "_l", "_c", "year")]
    X = frame[feats].replace([np.inf, -np.inf], np.nan)
    yr = frame["year"]; out = []
    for name, d, pct, w in FE.DAILY_TARGETS:
        y = FE.label_daily(frame, d, pct, w)
        valid = y.notna() & X.notna().all(axis=1)
        tr = (yr <= TRAIN_MAX) & valid; va = (yr == VAL) & valid; te = (yr == VAULT) & valid
        ytr = y[tr]
        if len(ytr) < 200 or ytr.nunique() < 2:
            continue
        base_tr = ytr.mean() * 100
        import os as _os
        rf = RandomForestClassifier(n_estimators=60, max_depth=3, min_samples_leaf=25,
                                    max_features=0.5, random_state=int(_os.environ.get("RF_SEED", "7")),
                                    n_jobs=int(_os.environ.get("RF_NJOBS", "-1")))
        rf.fit(X[tr].fillna(0).values, ytr.values)
        seen = set()
        for conds in leaf_rules(rf, feats):
            key = tuple(sorted(conds))
            if key in seen:
                continue
            seen.add(key)
            mtr = apply_rule(frame[tr], conds); ntr = int(mtr.sum())
            if ntr < MIN_TR:
                continue
            ptr = y[tr][mtr].mean() * 100
            if ptr - base_tr < LIFT_TR:
                continue
            mva = apply_rule(frame[va], conds); nva = int(mva.sum()); base_va = y[va].mean() * 100
            if nva < MIN_VA:
                continue
            pva = y[va][mva].mean() * 100
            promoted = 1 if (pva - base_va >= LIFT_VA and pva > base_va) else 0
            mte = apply_rule(frame[te], conds); nte = int(mte.sum()); base_te = y[te].mean() * 100
            pte = y[te][mte].mean() * 100 if nte else 0.0
            status = ("Keep" if (promoted and nte >= 10 and pte - base_te >= 3 and pte > base_te)
                      else "Watch" if (promoted and nte >= 3 and pte > base_te)
                      else "Retire" if promoted else "Test")
            out.append(dict(symbol=symbol, target=name, direction=d, conds=conds, depth=len(conds),
                            n_tr=ntr, prec_tr=round(ptr, 1), base_tr=round(base_tr, 1), lift_tr=round(ptr - base_tr, 1),
                            n_va=nva, prec_va=round(pva, 1), promoted=promoted,
                            n_te=nte, prec_te=round(pte, 1), base_te=round(base_te, 1),
                            fp_te=nte - int(round(pte / 100 * nte)), lift_te=round(pte - base_te, 1), status=status))
    return frame, out


def rule_text(conds):
    return " AND ".join(f"{f}{op}{round(t, 4)}" for f, op, t in conds)


def main():
    syms = sys.argv[1:] or ["ADANIENT"]
    for s in syms:
        print(f"\n===== MINING {s} (unified micro+macro engine) =====", flush=True)
        frame, pats = mine_stock(s)
        if not pats:
            print("  (insufficient data / no patterns)"); continue
        print(f"  frame: {frame.shape[0]} days x {frame.shape[1]} cols | mined {len(pats)} | "
              f"promoted {sum(p['promoted'] for p in pats)} | Keep {sum(p['status']=='Keep' for p in pats)}")
        keep = sorted([p for p in pats if p["status"] == "Keep"], key=lambda x: -x["lift_te"])
        micro = [p for p in keep if any(c[0].startswith(("id_", "eod_")) for c in p["conds"])]
        print(f"\n  --- top KEEP patterns (survived sealed 2026), micro-structure ones first ---")
        for p in (micro[:6] + [k for k in keep if k not in micro][:4])[:10]:
            has_micro = "MICRO+MACRO" if any(c[0].startswith(("id_", "eod_")) for c in p["conds"]) else "macro"
            print(f"   [{p['target']:11} {has_micro:11}] 2026: prec {p['prec_te']}% vs base {p['base_te']}% "
                  f"= +{p['lift_te']}pp | n={p['n_te']} FP={p['fp_te']} | {rule_text(p['conds'])}")


if __name__ == "__main__":
    main()
