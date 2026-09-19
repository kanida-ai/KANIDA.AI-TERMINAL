"""
KANIDA — PARALLEL playbook executor. Runs the exact same per-stock playbook as run_playbook.py,
but across N worker processes for speed. QUALITY-NEUTRAL: each stock is mined in isolation, RF is
deterministic (fixed random_state; n_jobs does not change results), gates/features/sealed-vault
logic are identical. Only scheduling changes. Each worker writes to its OWN sqlite file (no write
contention); the parent merges into KANIDA_SNR.db at the end.

Default universe: the REST of the Nifty-500 (in_nifty500=1, has 2026 data, not already in stock_verdict).
Run: PYTHONIOENCODING=utf-8 python run_playbook_parallel.py [N_WORKERS]   (logs: db/run_parallel.out + db/wk_*.log)
"""
import os
os.environ["KANIDA_RF_NJOBS"] = "1"                       # single-thread RF per worker (result-identical)
os.environ["OMP_NUM_THREADS"] = "1"; os.environ["OPENBLAS_NUM_THREADS"] = "1"; os.environ["MKL_NUM_THREADS"] = "1"

import sys, sqlite3, time, multiprocessing as mp
from pathlib import Path
from datetime import datetime
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\scripts")))
from mine_phase1 import features, mine_stock
from confirm_and_trade import load_cash_1min, load_fut_1min_frontmonth
import run_playbook as rp

KDB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
WK_DIR = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\wk_dbs")
MINED_DDL = """CREATE TABLE IF NOT EXISTS mined_patterns(
  Stock TEXT, pattern_id INTEGER, mined_year INTEGER, scope TEXT, outcome_target TEXT,
  n_obs INTEGER, n_hits INTEGER, precision_pct REAL, base_rate_pct REAL, lift_pct REAL,
  depth INTEGER, rule_text TEXT, rule_json TEXT, promoted INTEGER,
  status TEXT, oos2026_n INTEGER, oos2026_hits INTEGER, oos2026_precision_pct REAL,
  oos2026_base_pct REAL, oos2026_lift_pct REAL,
  PRIMARY KEY(Stock,pattern_id,outcome_target))"""


def process_chunk(arg):
    wid, stocks = arg
    WK_DIR.mkdir(exist_ok=True)
    wpath = WK_DIR / f"wk_{wid}.db"
    if wpath.exists(): wpath.unlink()
    rp.LOG = WK_DIR / f"wk_{wid}.log"; rp.LOG.write_text("", encoding="utf-8")
    con = sqlite3.connect(str(wpath), timeout=120)
    rp.ensure(con, fresh=True); con.execute(MINED_DDL); con.commit()
    kc = sqlite3.connect(KDB)
    fno = {s: (kc.execute("SELECT is_fno FROM instrument_labels WHERE symbol=?", (s,)).fetchone() or [0])[0] for s in stocks}
    kc.close()
    results = []
    for s in stocks:
        try:
            F = features(s)
            con.execute("DELETE FROM mined_patterns WHERE Stock=? AND scope='stock_specific'", (s,)); con.commit()
            mine_stock(s, con)
            keep = rp.confirm(con, s, F)
            cash = load_cash_1min(s); fut = load_fut_1min_frontmonth(s) if fno[s] else {}
            tr, dropped = rp.trade(con, s, F, bool(fno[s]), cash, fut)
            rp.journal(con, s, fno[s], tr, dropped, keep)
            results.append((s, "ok"))
        except Exception as e:
            results.append((s, f"ERR {str(e)[:120]}"))
            with open(rp.LOG, "a", encoding="utf-8") as f: f.write(f"  [{s}] ERROR: {str(e)[:160]}\n")
    con.close()
    return (str(wpath), results)


def compute_universe():
    con = sqlite3.connect(KDB)
    cov = {r[0]: r[1][:10] for r in con.execute("SELECT symbol,max(bar_time) FROM ohlc_daily GROUP BY symbol")}
    n500 = [r[0] for r in con.execute("SELECT symbol FROM instrument_labels WHERE in_nifty500=1")]
    con.close()
    snr = sqlite3.connect(SNR)
    done = set(r[0] for r in snr.execute("SELECT Stock FROM stock_verdict").fetchall()); snr.close()
    todo = sorted([s for s in set(n500) - {"NIFTY 50"} if s in cov and cov[s] >= "2026-07-25" and s not in done])
    return todo


def merge(outs, todo):
    main = sqlite3.connect(SNR, timeout=180)
    rp.ensure(main, fresh=False); main.execute(MINED_DDL); main.commit()
    qmarks = ",".join("?" * len(todo))
    main.execute(f"DELETE FROM mined_patterns WHERE scope='stock_specific' AND Stock IN ({qmarks})", todo)
    for t in ("trade_log_rev", "trade_journal_rev", "stock_verdict"):
        main.execute(f"DELETE FROM {t} WHERE Stock IN ({qmarks})", todo)
    main.commit()

    def cols(tbl):
        return [c[1] for c in main.execute(f"PRAGMA table_info({tbl})").fetchall()]
    for (wpath, _res) in outs:
        posix = wpath.replace("\\", "/")
        main.execute(f"ATTACH DATABASE '{posix}' AS wk")
        for tbl, mode in [("mined_patterns", "OR REPLACE"), ("stock_verdict", "OR REPLACE"),
                          ("trade_journal_rev", "OR REPLACE"), ("trade_log_rev", "")]:
            c = ",".join(cols(tbl))
            main.execute(f"INSERT {mode} INTO {tbl} ({c}) SELECT {c} FROM wk.{tbl}")
        main.commit(); main.execute("DETACH DATABASE wk")
    main.close()


def main():
    WK_DIR.mkdir(exist_ok=True)
    OUT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\run_parallel.out")

    def log(m):
        line = f"{datetime.now():%H:%M:%S} {m}"; print(line, flush=True)
        with open(OUT, "a", encoding="utf-8") as f: f.write(line + "\n")

    OUT.write_text("", encoding="utf-8")
    N = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    todo = compute_universe()
    if not todo:
        log("Nothing to do — all Nifty-500 stocks already processed."); return
    chunks = [[] for _ in range(N)]
    for i, s in enumerate(todo): chunks[i % N].append(s)          # round-robin balance
    args = [(i, ch) for i, ch in enumerate(chunks) if ch]
    log(f"=== PARALLEL playbook | {len(todo)} stocks | {len(args)} workers | RF n_jobs=1 (quality-identical) ===")
    t0 = time.time()
    ctx = mp.get_context("spawn")
    with ctx.Pool(len(args)) as pool:
        outs = pool.map(process_chunk, args)
    ok = sum(1 for _, res in outs for _, st in res if st == "ok")
    err = [(s, st) for _, res in outs for s, st in res if st != "ok"]
    log(f"workers done in {time.time()-t0:.0f}s | ok={ok} err={len(err)}")
    for s, st in err[:20]: log(f"  ERR {s}: {st}")
    log("merging worker DBs into KANIDA_SNR.db ...")
    merge(outs, todo)
    snr = sqlite3.connect(SNR)
    v = snr.execute("SELECT verdict,count(*) FROM stock_verdict WHERE Stock IN (%s) GROUP BY verdict"
                    % ",".join("?" * len(todo)), todo).fetchall()
    tot = snr.execute("SELECT verdict,count(*) FROM stock_verdict GROUP BY verdict").fetchall(); snr.close()
    log("THIS BATCH verdicts: " + " ".join(f"{a}={b}" for a, b in v))
    log("WHOLE UNIVERSE now: " + " ".join(f"{a}={b}" for a, b in tot))
    log(f"PARALLEL RUN COMPLETE in {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
