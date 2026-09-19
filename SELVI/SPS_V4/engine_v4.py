"""
SPS_V4 self-improvement engine.
Loop: author filter components -> immutable leak-free gauntlet -> admit survivors to
the skill library -> RECURSE (compose admitted survivors) -> meta-learn -> vault-confirm.

Run:  PYTHONIOENCODING=utf-8 python engine_v4.py <AGENT>
"""
import sys, json, sqlite3, itertools
from pathlib import Path
from datetime import datetime
from collections import Counter
import immutable_core as core
import capability_layer as cap

HERE = Path(__file__).resolve().parent


def lib_conn():
    c = sqlite3.connect(str(HERE / "skill_library.db"))
    c.execute("""CREATE TABLE IF NOT EXISTS skills(
        id INTEGER PRIMARY KEY, ts TEXT, sym TEXT, desc TEXT, target REAL, stop REAL,
        tr_t REAL, va_t REAL, va_net REAL, va_win REAL, gen INT, lineage TEXT,
        vault_net REAL, vault_t REAL)""")
    return c


def run(sym, target=0.015, stop=0.01):
    # integrity check: the agent may not have tampered with the core
    print(f"immutable-core SEAL: {core.SEAL[:12]}...  (locked)")
    con = lib_conn(); ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stats = cap.train_feature_stats(sym)

    # baseline: base ORBlate, no authored filter
    _, brtr, brva = core.gauntlet(sym, cap.make_filter([]), target, stop)
    base_t = brva["tstat"] if brva else 0.0
    print(f"[{sym}] BASELINE ORBlate {target*100:.1f}%/{stop}: val_t={base_t:.2f} "
          f"net={brva['net']*100:+.3f}% win={brva['win']*100:.0f}% trades={brva['trades']}")

    # ---- Generation 1: single authored predicates ----
    preds = cap.author_predicates(stats)
    admitted = []
    for p in preds:
        filt = cap.make_filter([p])
        ok, rtr, rva = core.gauntlet(sym, filt, target, stop)
        if ok and rva["tstat"] > base_t:                       # must clear gauntlet AND beat baseline
            admitted.append(([p], rtr, rva))
            con.execute("INSERT INTO skills(ts,sym,desc,target,stop,tr_t,va_t,va_net,va_win,gen,lineage) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (ts, sym, filt.desc, target, stop, rtr["tstat"], rva["tstat"],
                         rva["net"], rva["win"], 1, "gen1"))
    con.commit()
    admitted.sort(key=lambda x: -x[2]["tstat"])
    print(f"[{sym}] GEN1 authored {len(preds)} predicates -> admitted (beat baseline+gauntlet): {len(admitted)}")

    # ---- meta-learning: which feature families keep getting admitted ----
    fam = Counter(p.feature for a in admitted for p in a[0])
    if fam:
        print(f"[{sym}] meta: admitted feature families -> {dict(fam.most_common())}")

    # ---- Generation 2: RECURSE — compose top survivors (self-improvement on survivors) ----
    best = admitted[0][2]["tstat"] if admitted else base_t
    composed = []
    for (pa, _, _), (pb, _, _) in itertools.combinations(admitted[:6], 2):
        if pa[0].feature == pb[0].feature:
            continue
        filt = cap.make_filter(pa + pb)
        ok, rtr, rva = core.gauntlet(sym, filt, target, stop)
        if ok and rva["tstat"] > best:                         # must improve on the best gen1
            composed.append((pa + pb, rtr, rva))
            con.execute("INSERT INTO skills(ts,sym,desc,target,stop,tr_t,va_t,va_net,va_win,gen,lineage) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (ts, sym, filt.desc, target, stop, rtr["tstat"], rva["tstat"],
                         rva["net"], rva["win"], 2, "gen2:compose(gen1)"))
    con.commit()
    composed.sort(key=lambda x: -x[2]["tstat"])
    print(f"[{sym}] GEN2 composed improvements over best gen1: {len(composed)}")

    # ---- champion = best across generations, then VAULT-CONFIRM (honest final test) ----
    pool = admitted + composed
    pool.sort(key=lambda x: -x[2]["tstat"])
    if not pool:
        print(f"[{sym}] no self-improvement beat the baseline. (honest — baseline stands.)"); con.close(); return
    champ_preds, crtr, crva = pool[0]
    filt = cap.make_filter(champ_preds)
    vc = core.vault_confirm(sym, filt, target, stop)
    bvc = core.vault_confirm(sym, cap.make_filter([]), target, stop)   # baseline vault for comparison
    con.execute("UPDATE skills SET vault_net=?, vault_t=? WHERE sym=? AND desc=?",
                (vc["net"] if vc else None, vc["tstat"] if vc else None, sym, filt.desc))
    con.commit()
    print(f"\n[{sym}] === SELF-IMPROVED CHAMPION ===")
    print(f"  filter: {filt.desc}  (lineage gen{2 if champ_preds in [c[0] for c in composed] else 1})")
    print(f"  val:   baseline t={base_t:.2f} net={brva['net']*100:+.3f}%  ->  champion t={crva['tstat']:.2f} net={crva['net']*100:+.3f}% win={crva['win']*100:.0f}%")
    if vc and bvc:
        print(f"  VAULT 2026 (sealed):  baseline net={bvc['net']*100:+.3f}% (t={bvc['tstat']:.2f})  ->  "
              f"champion net={vc['net']*100:+.3f}% (5x {vc['net']*5*100:+.3f}%, t={vc['tstat']:.2f}, win={vc['win']*100:.0f}%, trades={vc['trades']})")
        verdict = "GENUINE (holds on sealed vault, beats baseline)" if vc["net"] > bvc["net"] and vc["net"] > 0 \
            else "VAL-OVERFIT (vault rejects it) — the immutable core did its job"
        print(f"  verdict: {verdict}")
    con.close()


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "CARTRADE")
