"""
M12b CONTROL — is the event "+10 READY" real, or just re-mine variance?
Re-mine the SAME features with a DIFFERENT random seed (13) into unified_patterns_seed13, grade it on the
same 6-test gauntlet, and measure how much the READY set moves from seed alone. If seed-only churn ~ the
event churn (33/54) and READY count wobbles ~±10, the event gain is noise, not an event-feature effect.
Cache already has the event columns (unchanged features) so NO cache rebuild is needed.
Run: PYTHONIOENCODING=utf-8 python arena/m12b_seedctl.py   (log: db/m12b_seedctl.log)
"""
import os
os.environ["OMP_NUM_THREADS"] = "1"; os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"; os.environ["RF_NJOBS"] = "1"; os.environ["RF_SEED"] = "13"
import sys, time, sqlite3, subprocess, multiprocessing as mp
from datetime import datetime
from pathlib import Path
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "arena")); sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "kanida_engine"))
from m12b_pipeline import mine_one            # same worker; reads RF_SEED from env

KDB = str(ROOT / "db" / "kanida.db"); SNR = str(ROOT / "db" / "KANIDA_SNR.db"); REP = ROOT / "reports"
LOG = ROOT / "db" / "m12b_seedctl.log"; TABLE = "unified_patterns_seed13"; NWORK = 8


def log(m):
    line = f"{datetime.now():%H:%M:%S} {m}"; print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f: f.write(line + "\n")


def main():
    LOG.write_text("", encoding="utf-8"); T0 = time.time()
    log("===== M12b SEED-VARIANCE CONTROL (seed=13, same features) =====")
    kc = sqlite3.connect(KDB)
    universe = sorted(set(r[0] for r in kc.execute(
        "SELECT symbol FROM instrument_labels WHERE in_nifty500=1 OR is_fno=1").fetchall())); kc.close()
    con = sqlite3.connect(SNR, timeout=180)
    con.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE}(
        symbol TEXT, target TEXT, direction TEXT, rule_text TEXT, rule_json TEXT, depth INTEGER,
        has_micro INTEGER, n_tr INTEGER, prec_tr REAL, base_tr REAL, lift_tr REAL,
        n_va INTEGER, prec_va REAL, promoted INTEGER, n_te INTEGER, prec_te REAL, base_te REAL,
        lift_te REAL, fp_te INTEGER, status TEXT)""")
    done = set(r[0] for r in con.execute(f"SELECT DISTINCT symbol FROM {TABLE}").fetchall())
    todo = [s for s in universe if s not in done]
    log(f"mining {len(todo)} symbols (seed 13) -> {TABLE}")
    n = 0
    with mp.get_context("spawn").Pool(NWORK, maxtasksperchild=15) as pool:
        for sym, res in pool.imap_unordered(mine_one, todo):
            n += 1
            if isinstance(res, list) and res:
                con.execute(f"DELETE FROM {TABLE} WHERE symbol=?", (sym,))
                con.executemany(f"INSERT INTO {TABLE} VALUES (" + ",".join("?" * 20) + ")", res); con.commit()
            if n % 40 == 0: log(f"  mined {n}/{len(todo)} | {time.time()-T0:.0f}s")
    con.close()
    log("grading seed13 book")
    env = dict(os.environ, UNIFIED_TABLE=TABLE, READINESS_OUT=str(REP / "worker_readiness_seed13.csv"))
    subprocess.run([sys.executable, str(ROOT / "arena" / "worker_readiness.py")], env=env,
                   capture_output=True, text=True)

    import pandas as pd
    def ready(csv): return set(pd.read_csv(csv).query("tier=='READY'").symbol)
    base = ready(REP / "worker_readiness.csv"); event = ready(REP / "worker_readiness_event.csv")
    seed = ready(REP / "worker_readiness_seed13.csv")
    out = ["===== SEED-VARIANCE CONTROL RESULT =====",
           f"  baseline (seed7, 93 feat)  READY: {len(base)}",
           f"  event    (seed7, 99 feat)  READY: {len(event)}   vs baseline {len(event)-len(base):+d}",
           f"  seed13   (seed13, 99 feat) READY: {len(seed)}   vs baseline {len(seed)-len(base):+d}",
           "",
           f"  churn baseline->event : {len(base-event)} lost, {len(event-base)} new (retained {len(base&event)})",
           f"  churn baseline->seed13: {len(base-seed)} lost, {len(seed-base)} new (retained {len(base&seed)})",
           f"  STABLE CORE (READY in all 3 mines): {len(base & event & seed)}",
           f"  stable-core members: {sorted(base & event & seed)}",
           "",
           "  READ: if seed-only churn ~ event churn, the event '+READY' is variance, not an event effect.",
           f"  total: {time.time()-T0:.0f}s"]
    txt = "\n".join(out); (REP / "m12b_seedctl_result.txt").write_text(txt, encoding="utf-8")
    log("\n" + txt); log("===== CONTROL COMPLETE =====")


if __name__ == "__main__":
    main()
