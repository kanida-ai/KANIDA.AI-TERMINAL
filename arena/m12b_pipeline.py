"""
M12b AUTONOMOUS PIPELINE — runs the whole event-feature evaluation unattended, in parallel, then reports.
Steps (single background job):
  1. PARALLEL re-mine remaining symbols -> KANIDA_SNR.db.unified_patterns_event  (resumes; skips done)
  2. Force-rebuild the frame cache WITH the new event features (delete pickles, parallel rebuild)
  3. Grade BASELINE (unified_patterns) on the new cache        -> reports/worker_readiness.csv  (sanity: ~54)
  4. Grade EVENT   (unified_patterns_event)                    -> reports/worker_readiness_event.csv
  5. Compare READY/NEAR and write reports/m12b_compare.txt + print the verdict

Quality is identical to the sequential mine (same per-stock logic); we only fan stocks across workers.
RF is pinned to 1 thread/worker so 8 processes don't oversubscribe 12 cores.
Run: PYTHONIOENCODING=utf-8 python arena/m12b_pipeline.py   (log: db/m12b_pipeline.log)
"""
import os
os.environ["OMP_NUM_THREADS"] = "1"; os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"; os.environ["RF_NJOBS"] = "1"
import sys, json, glob, time, sqlite3, subprocess, multiprocessing as mp
from datetime import datetime
from pathlib import Path
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "kanida_engine"))
import features as FE
import miner

KDB = str(ROOT / "db" / "kanida.db"); SNR = str(ROOT / "db" / "KANIDA_SNR.db")
CACHE = str(ROOT / "db" / "frame_cache"); REP = ROOT / "reports"
LOG = ROOT / "db" / "m12b_pipeline.log"
EVENT_TABLE = "unified_patterns_event"; NWORK = 8


def log(m):
    line = f"{datetime.now():%H:%M:%S} {m}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f: f.write(line + "\n")


def mine_one(sym):
    try:
        frame, pats = miner.mine_stock(sym)
        if not pats: return (sym, None)
        rows = []
        for p in pats:
            hm = 1 if any(c[0].startswith(("id_", "eod_")) for c in p["conds"]) else 0
            rows.append((sym, p["target"], p["direction"], miner.rule_text(p["conds"]),
                         json.dumps([[a, b, c] for a, b, c in p["conds"]]), p["depth"], hm,
                         p["n_tr"], p["prec_tr"], p["base_tr"], p["lift_tr"], p["n_va"], p["prec_va"],
                         p["promoted"], p["n_te"], p["prec_te"], p["base_te"], p["lift_te"],
                         p["fp_te"], p["status"]))
        return (sym, rows)
    except Exception as e:
        return (sym, f"ERR {str(e)[:80]}")


def cache_one(sym):
    try:
        f = FE.load_frame(sym, lookback_N=5)
        return (sym, 0 if f is None or f.empty else len(f))
    except Exception:
        return (sym, -1)


def step1_mine():
    kc = sqlite3.connect(KDB)
    universe = sorted(set(r[0] for r in kc.execute(
        "SELECT symbol FROM instrument_labels WHERE in_nifty500=1 OR is_fno=1").fetchall()))
    kc.close()
    con = sqlite3.connect(SNR, timeout=180)
    con.execute(f"""CREATE TABLE IF NOT EXISTS {EVENT_TABLE}(
        symbol TEXT, target TEXT, direction TEXT, rule_text TEXT, rule_json TEXT, depth INTEGER,
        has_micro INTEGER, n_tr INTEGER, prec_tr REAL, base_tr REAL, lift_tr REAL,
        n_va INTEGER, prec_va REAL, promoted INTEGER, n_te INTEGER, prec_te REAL, base_te REAL,
        lift_te REAL, fp_te INTEGER, status TEXT)""")
    done = set(r[0] for r in con.execute(f"SELECT DISTINCT symbol FROM {EVENT_TABLE}").fetchall())
    todo = [s for s in universe if s not in done]
    log(f"STEP 1 mine: {len(universe)} universe, {len(done)} already done, {len(todo)} to mine ({NWORK} workers)")
    t0 = time.time(); n = 0
    with mp.get_context("spawn").Pool(NWORK, maxtasksperchild=15) as pool:
        for sym, res in pool.imap_unordered(mine_one, todo):
            n += 1
            if isinstance(res, list) and res:
                con.execute(f"DELETE FROM {EVENT_TABLE} WHERE symbol=?", (sym,))
                con.executemany(f"INSERT INTO {EVENT_TABLE} VALUES (" + ",".join("?" * 20) + ")", res)
                con.commit()
            if n % 20 == 0:
                tot = con.execute(f"SELECT count(DISTINCT symbol),count(*) FROM {EVENT_TABLE}").fetchone()
                log(f"  mined {n}/{len(todo)} | table: {tot[0]} syms, {tot[1]} patterns | {time.time()-t0:.0f}s")
    tot = con.execute(f"SELECT count(DISTINCT symbol),count(*),sum(promoted) FROM {EVENT_TABLE}").fetchone()
    con.close()
    log(f"STEP 1 done: {tot[0]} syms, {tot[1]} patterns, {tot[2]} promoted in {time.time()-t0:.0f}s")


def step2_cache():
    old = glob.glob(os.path.join(CACHE, "*.pkl"))
    for p in old:
        try: os.remove(p)
        except Exception: pass
    con = sqlite3.connect(SNR)
    syms = sorted(set(r[0] for r in con.execute(
        f"SELECT symbol FROM unified_patterns UNION SELECT symbol FROM {EVENT_TABLE}").fetchall()))
    con.close()
    log(f"STEP 2 cache: deleted {len(old)} old pickles, rebuilding {len(syms)} frames with event features")
    t0 = time.time(); n = ok = 0
    with mp.get_context("spawn").Pool(NWORK, maxtasksperchild=15) as pool:
        for sym, sz in pool.imap_unordered(cache_one, syms):
            n += 1; ok += (sz > 0)
            if n % 50 == 0: log(f"  cached {n}/{len(syms)} (ok {ok}) | {time.time()-t0:.0f}s")
    log(f"STEP 2 done: {ok}/{len(syms)} frames cached in {time.time()-t0:.0f}s")


def grade(table, out):
    env = dict(os.environ, UNIFIED_TABLE=table, READINESS_OUT=str(out), PYTHONIOENCODING="utf-8")
    log(f"  grading {table} -> {Path(out).name}")
    r = subprocess.run([sys.executable, str(ROOT / "arena" / "worker_readiness.py")],
                       env=env, capture_output=True, text=True)
    tail = [l for l in r.stdout.splitlines() if "READY (all" in l or "gate pass" in l]
    for l in tail: log("    " + l.strip())
    if r.returncode != 0: log(f"  GRADE ERROR: {r.stderr[-300:]}")


def counts(csv):
    import pandas as pd
    d = pd.read_csv(csv)
    return {k: int((d.tier == k).sum()) for k in ["READY", "NEAR", "NOT READY"]}, d


def main():
    LOG.write_text("", encoding="utf-8"); T0 = time.time()
    log("===== M12b AUTONOMOUS PIPELINE START =====")
    step1_mine()
    step2_cache()
    log("STEP 3-4 grade baseline + event")
    grade("unified_patterns", REP / "worker_readiness.csv")
    grade(EVENT_TABLE, REP / "worker_readiness_event.csv")
    import pandas as pd
    b, bd = counts(REP / "worker_readiness.csv")
    e, ed = counts(REP / "worker_readiness_event.csv")
    lines = ["===== M12b RESULT: EVENT PATTERNS vs BASELINE (same 6-test gauntlet) =====",
             f"  {'tier':<12}{'baseline':>10}{'event':>8}{'delta':>8}"]
    for k in ["READY", "NEAR", "NOT READY"]:
        lines.append(f"  {k:<12}{b[k]:>10}{e[k]:>8}{e[k]-b[k]:>+8}")
    er = ed[ed.tier == "READY"]; verdict = "ADOPT" if e["READY"] > b["READY"] else "KEEP BASELINE"
    lines.append(f"  event READY cohort profit(2026): Rs{er.profit_2026.sum():,.0f}  median ret/DD {er.retdd_2026.median():.2f}")
    lines.append(f"  VERDICT: {verdict}  (event {e['READY']} vs baseline {b['READY']} READY)")
    lines.append(f"  total pipeline: {time.time()-T0:.0f}s")
    txt = "\n".join(lines); (REP / "m12b_compare.txt").write_text(txt, encoding="utf-8")
    log("\n" + txt)
    log("===== M12b PIPELINE COMPLETE =====")


if __name__ == "__main__":
    main()
