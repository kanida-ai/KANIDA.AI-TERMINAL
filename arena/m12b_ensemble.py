"""
STABILITY ENSEMBLE — the honest "truly sellable" test. A worker's READY status is seed-sensitive
(proved by the seed control), so a single-mine READY is not trustworthy. Here we mine the SAME 99-feature
engine under 5 independent seeds, grade each on the same 6-test gauntlet, and score every worker by HOW
MANY seeds it is READY in. The robust, sellable cohort = READY in most/all seeds — immune to modeling luck.

Seeds: 7 (=unified_patterns_event), 13 (=unified_patterns_seed13) already done; 23/37/51 mined here.
Outputs: reports/worker_stability.csv (per-worker READY-count 0..5) + reports/m12b_stability_summary.txt.
Run: PYTHONIOENCODING=utf-8 python arena/m12b_ensemble.py   (log: db/m12b_ensemble.log)
"""
import os
os.environ["OMP_NUM_THREADS"] = "1"; os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"; os.environ["RF_NJOBS"] = "1"
import sys, time, sqlite3, subprocess, multiprocessing as mp
from datetime import datetime
from pathlib import Path
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "arena")); sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "kanida_engine"))
from m12b_pipeline import mine_one

KDB = str(ROOT / "db" / "kanida.db"); SNR = str(ROOT / "db" / "KANIDA_SNR.db"); REP = ROOT / "reports"
LOG = ROOT / "db" / "m12b_ensemble.log"; NWORK = 8
# seed -> (pattern table, readiness csv). 7 and 13 already exist.
SEEDS = {7:  ("unified_patterns_event",  REP / "worker_readiness_event.csv"),
         13: ("unified_patterns_seed13", REP / "worker_readiness_seed13.csv"),
         23: ("unified_patterns_seed23", REP / "worker_readiness_seed23.csv"),
         37: ("unified_patterns_seed37", REP / "worker_readiness_seed37.csv"),
         51: ("unified_patterns_seed51", REP / "worker_readiness_seed51.csv")}
NEW = [23, 37, 51]


def log(m):
    line = f"{datetime.now():%H:%M:%S} {m}"; print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f: f.write(line + "\n")


def mine_seed(seed, table):
    os.environ["RF_SEED"] = str(seed)
    kc = sqlite3.connect(KDB)
    universe = sorted(set(r[0] for r in kc.execute(
        "SELECT symbol FROM instrument_labels WHERE in_nifty500=1 OR is_fno=1").fetchall())); kc.close()
    con = sqlite3.connect(SNR, timeout=180)
    con.execute(f"""CREATE TABLE IF NOT EXISTS {table}(
        symbol TEXT, target TEXT, direction TEXT, rule_text TEXT, rule_json TEXT, depth INTEGER,
        has_micro INTEGER, n_tr INTEGER, prec_tr REAL, base_tr REAL, lift_tr REAL,
        n_va INTEGER, prec_va REAL, promoted INTEGER, n_te INTEGER, prec_te REAL, base_te REAL,
        lift_te REAL, fp_te INTEGER, status TEXT)""")
    done = set(r[0] for r in con.execute(f"SELECT DISTINCT symbol FROM {table}").fetchall())
    todo = [s for s in universe if s not in done]
    log(f"  seed {seed}: mining {len(todo)} symbols -> {table}")
    n = 0
    with mp.get_context("spawn").Pool(NWORK, maxtasksperchild=15) as pool:
        for sym, res in pool.imap_unordered(mine_one, todo):
            n += 1
            if isinstance(res, list) and res:
                con.execute(f"DELETE FROM {table} WHERE symbol=?", (sym,))
                con.executemany(f"INSERT INTO {table} VALUES (" + ",".join("?" * 20) + ")", res); con.commit()
            if n % 60 == 0: log(f"    seed {seed}: {n}/{len(todo)}")
    con.close()


def grade(table, csv):
    env = dict(os.environ, UNIFIED_TABLE=table, READINESS_OUT=str(csv), PYTHONIOENCODING="utf-8")
    subprocess.run([sys.executable, str(ROOT / "arena" / "worker_readiness.py")], env=env, capture_output=True, text=True)


def main():
    LOG.write_text("", encoding="utf-8"); T0 = time.time()
    log("===== STABILITY ENSEMBLE (5 seeds) =====")
    for seed in NEW:
        table, csv = SEEDS[seed]
        mine_seed(seed, table)
        log(f"  seed {seed}: grading"); grade(table, csv)
        log(f"  seed {seed}: done | {time.time()-T0:.0f}s")

    import pandas as pd
    # per-worker READY count across the 5 seeds
    ready_sets = {}
    for seed, (table, csv) in SEEDS.items():
        d = pd.read_csv(csv); ready_sets[seed] = set(d.query("tier=='READY'").symbol)
    allsyms = set().union(*[set(pd.read_csv(c).symbol) for _, c in SEEDS.values()])
    rows = []
    for s in allsyms:
        cnt = sum(1 for seed in SEEDS if s in ready_sets[seed])
        rows.append({"symbol": s, "ready_in_seeds": cnt, "of_seeds": len(SEEDS),
                     **{f"ready_s{seed}": int(s in ready_sets[seed]) for seed in SEEDS}})
    df = pd.DataFrame(rows).sort_values("ready_in_seeds", ascending=False)
    df.to_csv(REP / "worker_stability.csv", index=False)
    dist = {k: int((df.ready_in_seeds == k).sum()) for k in range(len(SEEDS), -1, -1)}
    rock = df[df.ready_in_seeds == len(SEEDS)]; robust = df[df.ready_in_seeds >= 4]
    base54 = ready_sets[7]  # note seed7=event book here
    out = ["===== WORKER STABILITY ACROSS 5 SEEDS (same 99-feat engine, 6-test gauntlet) =====",
           f"  READY-in-N-seeds distribution: " + "  ".join(f"{k}/5:{dist[k]}" for k in range(5, -1, -1)),
           "",
           f"  ROCK-SOLID (READY in all 5 seeds): {len(rock)}  <- the genuinely seed-proof, sellable core",
           f"  ROBUST     (READY in >=4 of 5)   : {len(robust)}",
           f"  per-seed READY counts            : " + ", ".join(f"s{seed}={len(ready_sets[seed])}" for seed in SEEDS),
           "",
           f"  ROCK-SOLID members: {sorted(rock.symbol)}",
           "",
           "  TAKEAWAY: single-mine 'READY' overstates the sellable set; the robust cohort is the honest product.",
           f"  total: {time.time()-T0:.0f}s"]
    txt = "\n".join(out); (REP / "m12b_stability_summary.txt").write_text(txt, encoding="utf-8")
    log("\n" + txt); log("===== ENSEMBLE COMPLETE =====")


if __name__ == "__main__":
    main()
