"""Isolated nightly EOD runner — executed as a one-off ECS task with the main
app STOPPED (desired_count=0), so NO live monitor is writing kanida_universe.db
while the >=10-worker feature job writes falcon_features to it.

Background (2026-07-28 incident, see memory cloud_universe_db_corruption_recovery):
  The in-app 16:05 IST pipeline ran daily_features in-process. Its ProcessPool
  (clamped to >=10 workers in universe_engine/engine/falcon_features.py) has each
  worker open its OWN sqlite connection and INSERT OR REPLACE into falcon_features
  — while the live autotrade monitors write autotrade_*/portfolio_* rows to the
  SAME kanida_universe.db file. The concurrent multi-writer load (plus the 300s
  backup-sync) corrupts the DB ("database disk image is malformed"). Running the
  chain with the app OFF removes the live writers and the corruption with them.

This script bypasses entrypoint.sh (the ECS RunTask override sets the command),
so it must reproduce the local-disk bridge itself: copy EFS -> /localdb, point the
job config at /localdb, run the chain, then online-backup /localdb -> EFS.

Orchestration (stop app -> run this -> start app) is done by the caller
(.github/workflows/nightly-eod.yml). Exit code: 0 = today's signals emitted,
2 = ran but signals not fresh, 1 = hard failure. The workflow ALWAYS restarts the
app afterwards regardless of exit code.
"""
import os, sys, glob, shutil, sqlite3, time
from datetime import datetime, timezone, timedelta

EFS = os.environ.get("KANIDA_EFS_DB_DIR", "/data/db")
LOCAL = os.environ.get("KANIDA_LOCAL_DB_DIR", "/localdb")
IST = timezone(timedelta(hours=5, minutes=30))
TODAY = datetime.now(IST).date().isoformat()

# Reproduce entrypoint.sh's env (it does not run under a command override).
os.makedirs(LOCAL, exist_ok=True)
os.environ["FALCON_DB_PATH"] = f"{LOCAL}/kanida_universe.db"
os.environ["POWER_DB_PATH"] = f"{LOCAL}/kanida_universe.db"
os.environ["POWER_RND_DB_PATH"] = f"{LOCAL}/kanida_universe.db"
os.environ["KANIDA_DB_PATH"] = f"{LOCAL}/kanida_quant.db"
os.environ["FALCON_OUTCOMES_ARTIFACT"] = f"{LOCAL}/falcon_serve_evidence.db"
os.environ["FALCON_SIM_PATTERNS_ARTIFACT"] = f"{LOCAL}/falcon_sim_patterns.db"
os.environ["RUPEEZY_INSTRUMENT_MASTER"] = f"{LOCAL}/rupeezy_instruments.json"
# Never let a batch task try to mint broker tokens.
os.environ.pop("FALCON_INPROCESS_AUTH", None)
sys.path.insert(0, "/app/backend")

UDB = f"{LOCAL}/kanida_universe.db"


def log(m): print(f"[nightly_eod {datetime.now(IST):%H:%M:%S} IST] {m}", flush=True)


def quick_check(db):
    c = sqlite3.connect(db)
    v = c.execute("PRAGMA quick_check").fetchone()[0]
    c.close()
    return v.decode() if isinstance(v, bytes) else v


def rebuild(src):
    """Salvage-rebuild that DECODES bytes->str per value (keeps TEXT columns TEXT,
    never BLOB — the type bug that broke fromisoformat on 2026-07-28)."""
    new = src + ".rebuilt"
    if os.path.exists(new):
        os.remove(new)
    s = sqlite3.connect(src); s.text_factory = bytes
    rows = s.execute("SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL "
                     "AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'").fetchall()
    dec = lambda x: x.decode() if isinstance(x, bytes) else x
    tabs = [(dec(n), dec(sq)) for (t, n, sq) in rows if dec(t) == "table"]
    idxs = [(dec(n), dec(sq)) for (t, n, sq) in rows if dec(t) == "index"]
    conv = lambda v: (v.decode("utf-8", "replace") if isinstance(v, bytes) else v)
    d = sqlite3.connect(new); d.execute("PRAGMA journal_mode=OFF"); d.execute("PRAGMA synchronous=OFF")
    for n, sq in tabs:
        try: d.execute(sq)
        except Exception as e: log(f"  create fail {n}: {str(e)[:50]}")
    d.commit()
    for n, _ in tabs:
        nc = len(s.execute(f'PRAGMA table_info("{n}")').fetchall())
        ph = ",".join("?" * nc); got = err = 0
        try:
            cur = s.execute(f'SELECT * FROM "{n}"')
            while True:
                try: batch = cur.fetchmany(2000)
                except Exception: err += 1; break
                if not batch: break
                cb = [tuple(conv(v) for v in r) for r in batch]
                try: d.executemany(f'INSERT INTO "{n}" VALUES ({ph})', cb); got += len(cb)
                except Exception:
                    for r in cb:
                        try: d.execute(f'INSERT INTO "{n}" VALUES ({ph})', r); got += 1
                        except Exception: err += 1
        except Exception: err += 1
        d.commit()
        if got or err: log(f"  {n}: {got} rows, {err} err")
    for n, sq in idxs:
        try: d.execute(sq)
        except Exception: pass
    d.commit(); d.close(); s.close()
    if quick_check(new) == "ok":
        shutil.move(src, src + ".corrupt-bak"); shutil.move(new, src)
        log("rebuild OK, swapped"); return True
    log("rebuild FAILED verify"); return False


def main():
    log(f"start; expected signal_date={TODAY}")
    log("copy EFS -> LOCAL")
    for f in glob.glob(f"{EFS}/*"):
        if os.path.isfile(f) and not f.endswith(".tmp"):
            shutil.copy(f, LOCAL + "/")

    v = quick_check(UDB)
    log(f"universe quick_check: {v}")
    if v != "ok":
        if not rebuild(UDB):
            log("ABORT: rebuild failed"); return 1
        log(f"post-rebuild quick_check: {quick_check(UDB)}")

    import importlib
    for job in ["daily_data_refresh", "daily_features", "daily_signals"]:
        t0 = time.time()
        try:
            res = importlib.import_module(f"falcon.jobs.{job}").run()
            st = res.get("status") if isinstance(res, dict) else "?"
            log(f"JOB {job}: status={st} ({time.time()-t0:.1f}s)")
            if st == "failed":
                log(f"{job} failed -> halt"); break
        except Exception as e:
            import traceback; log(f"JOB {job} CRASHED: {e}"); traceback.print_exc(); break

    c = sqlite3.connect(UDB)
    max_sd = c.execute("SELECT MAX(signal_date) FROM falcon_signals_live").fetchone()[0]
    c.close()
    log(f"MAX signal_date = {max_sd}")

    log("sync LOCAL -> EFS (online backup)")
    for db in ["kanida_universe.db", "kanida_quant.db"]:
        lp = f"{LOCAL}/{db}"
        if not os.path.exists(lp):
            continue
        s = sqlite3.connect(lp); d = sqlite3.connect(f"{EFS}/{db}.tmp"); s.backup(d); d.close(); s.close()
        os.replace(f"{EFS}/{db}.tmp", f"{EFS}/{db}"); log(f"synced {db}")

    if max_sd == TODAY:
        log("DONE — signals fresh for today"); return 0
    log(f"DONE — but signal_date={max_sd} != {TODAY} (holiday? or failure)"); return 2


if __name__ == "__main__":
    sys.exit(main())
