"""
Build the daily-frame CACHE once, in parallel. After this, engine_trade / arena / analytics read each
stock's frame instantly (a small pickle) instead of re-reading ~1M 1-min bars every run.
Run: PYTHONIOENCODING=utf-8 python build_frame_cache.py [N_WORKERS]   (log: db/frame_cache.log)
"""
import os
os.environ["OMP_NUM_THREADS"] = "1"; os.environ["OPENBLAS_NUM_THREADS"] = "1"; os.environ["MKL_NUM_THREADS"] = "1"
import sys, sqlite3, time, multiprocessing as mp
from datetime import datetime
from pathlib import Path
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\kanida_engine")))
import features as FE

SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
LOG = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\frame_cache.log")


def one(sym):
    try:
        f = FE.load_frame(sym, lookback_N=5)          # builds + caches if absent
        return (sym, 0 if f is None or f.empty else len(f))
    except Exception as e:
        return (sym, -1)


def main():
    LOG.write_text("", encoding="utf-8")
    N = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    con = sqlite3.connect(SNR)
    syms = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]
    con.close()
    t0 = time.time()
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%H:%M:%S} caching {len(syms)} frames | {N} workers\n")
    done = ok = 0
    with mp.get_context("spawn").Pool(N) as pool:
        for sym, n in pool.imap_unordered(one, syms):
            done += 1; ok += (n > 0)
            if done % 25 == 0:
                with open(LOG, "a", encoding="utf-8") as f:
                    f.write(f"{datetime.now():%H:%M:%S} {done}/{len(syms)} cached (ok {ok})\n")
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%H:%M:%S} FRAME CACHE COMPLETE: {ok}/{len(syms)} in {time.time()-t0:.0f}s\n")


if __name__ == "__main__":
    main()
