"""
SPS_V4 batch self-improvement over the tradeable list.
For each stock (with its V3 champion target/stop) the agent authors filter
components, runs the immutable leak-free gauntlet (train&val t>=2.5, beat baseline),
composes survivors (recursion), then VAULT-confirms the champion vs the baseline.
Optimised: each stock's train/val/vault packs are loaded once.

Run:  PYTHONIOENCODING=utf-8 python v4_batch.py
"""
import json, sqlite3, itertools
from pathlib import Path
from datetime import datetime
import numpy as np
import immutable_core as core
import capability_layer as cap
from immutable_core import load_min, load_atr, day_pack, TRAIN, VAL, VAULT

HERE = Path(__file__).resolve().parent
SPECS = json.load(open(HERE / "tradeable_specs.json"))


def metrics(days, atrmap, filt, target, stop):
    return core.simulate(days, atrmap, filt, target, stop)


def improve(sym, target, stop):
    atrmap = load_atr(sym)
    tr = day_pack(load_min(sym, *TRAIN)); va = day_pack(load_min(sym, *VAL))
    vault = day_pack(load_min(sym, *VAULT, unlock=True))
    stats = {}
    # feature stats on train (point-in-time)
    fv = {k: [] for k in core.FEATURES}
    for d, day in tr.items():
        ap, pc = atrmap.get(d, (np.nan, np.nan))
        if pc is None or (isinstance(pc, float) and np.isnan(pc)):
            continue
        r = core.orb_late(day)
        if r is None:
            continue
        e, side, _ = r; f = core.features(day, ap, pc, e)
        for k in core.FEATURES:
            fv[k].append(f[k])
    stats = {k: np.array(v) for k, v in fv.items()}

    NO = cap.make_filter([])
    brtr = metrics(tr, atrmap, NO, target, stop); brva = metrics(va, atrmap, NO, target, stop)
    bvc = metrics(vault, atrmap, NO, target, stop)
    if not brva:
        return {"sym": sym, "improved": False}
    base_t = brva["tstat"]

    def admit(preds):
        filt = cap.make_filter(preds)
        rtr = metrics(tr, atrmap, filt, target, stop); rva = metrics(va, atrmap, filt, target, stop)
        if not rtr or not rva:
            return None
        ok = (rtr["trades"] >= 100 and rva["trades"] >= 40 and rtr["net"] > 0 and rva["net"] > 0
              and rtr["tstat"] >= 2.5 and rva["tstat"] >= 2.5)
        return (preds, rtr, rva) if ok else None

    admitted = []
    for p in cap.author_predicates(stats):
        a = admit([p])
        if a and a[2]["tstat"] > base_t:
            admitted.append(a)
    admitted.sort(key=lambda x: -x[2]["tstat"])
    best = admitted[0][2]["tstat"] if admitted else base_t
    composed = []
    for (pa, _, _), (pb, _, _) in itertools.combinations(admitted[:6], 2):
        if pa[0].feature == pb[0].feature:
            continue
        a = admit(pa + pb)
        if a and a[2]["tstat"] > best:
            composed.append(a)
    pool = admitted + composed
    pool.sort(key=lambda x: -x[2]["tstat"])
    if not pool:
        return {"sym": sym, "improved": False, "base_val_t": base_t,
                "base_vault_net": bvc["net"] if bvc else None}
    cp, crtr, crva = pool[0]
    cvc = metrics(vault, atrmap, cap.make_filter(cp), target, stop)
    genuine = bool(cvc and bvc and cvc["net"] > bvc["net"] and cvc["net"] > 0)
    return {"sym": sym, "improved": True, "desc": cap.make_filter(cp).desc,
            "base_val_t": base_t, "champ_val_t": crva["tstat"],
            "base_vault_net": bvc["net"] if bvc else None,
            "champ_vault_net": cvc["net"] if cvc else None,
            "champ_vault_trades": cvc["trades"] if cvc else 0,
            "champ_vault_win": cvc["win"] if cvc else None,
            "genuine": genuine, "n_admitted": len(admitted), "n_composed": len(composed)}


def main():
    print(f"immutable-core SEAL: {core.SEAL[:12]}... (locked) | self-improving {len(SPECS)} tradeable stocks", flush=True)
    results = []
    for sym, target, stop in SPECS:
        try:
            r = improve(sym, target, stop)
        except Exception as e:
            r = {"sym": sym, "improved": False, "error": str(e)[:80]}
        results.append(r)
        if r.get("improved"):
            g = "GENUINE" if r["genuine"] else "val-overfit(vault rejects)"
            print(f"{sym:12} vt {r['base_val_t']:.2f}->{r['champ_val_t']:.2f} | vault {r['base_vault_net']*100:+.3f}%->"
                  f"{r['champ_vault_net']*100:+.3f}% ({r['champ_vault_trades']}t) [{g}] {r['desc']}", flush=True)
        else:
            print(f"{sym:12} no improvement beat baseline {r.get('error','')}", flush=True)
    json.dump(results, open(HERE / "v4_batch_results.json", "w"), indent=2, default=str)

    genuine = [r for r in results if r.get("improved") and r.get("genuine")]
    base = [r["base_vault_net"] for r in results if r.get("base_vault_net") is not None]
    impr = [(r["champ_vault_net"] if (r.get("improved") and r.get("genuine")) else r.get("base_vault_net"))
            for r in results if r.get("base_vault_net") is not None]
    print(f"\n=== {len(genuine)}/{len(SPECS)} stocks got a GENUINE vault-confirmed self-improvement ===", flush=True)
    print(f"portfolio avg vault net/trade: baseline {np.mean(base)*100:+.3f}%  ->  self-improved {np.mean(impr)*100:+.3f}%", flush=True)
    print(f"                        (5x): baseline {np.mean(base)*5*100:+.3f}%  ->  self-improved {np.mean(impr)*5*100:+.3f}%", flush=True)


if __name__ == "__main__":
    main()
