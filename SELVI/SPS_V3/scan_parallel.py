"""Parallel cross-sectional scan — runs scan.scan_stock across all remaining
universe stocks on a process pool. Resumable (skips stocks already in scan_results.db).
Run:  python scan_parallel.py
"""
import json, sqlite3
from datetime import datetime
from multiprocessing import Pool, cpu_count
import scan
HERE = scan.HERE


def work(sym):
    try:
        best, thr = scan.scan_stock(sym)
    except Exception as e:
        return (sym, None, 0.0, None, None, -1, str(e)[:80])
    if best:
        h, rtr, rva = best
        return (sym, json.dumps(h), thr, json.dumps(rtr), json.dumps(rva), 1, "")
    return (sym, None, thr, None, None, 0, "")


def main():
    con = sqlite3.connect(str(HERE / "scan_results.db"))
    con.execute("""CREATE TABLE IF NOT EXISTS champ(
        sym TEXT PRIMARY KEY, spec TEXT, thr REAL, tr_json TEXT, va_json TEXT, has_edge INT, ts TEXT)""")
    done = {r[0] for r in con.execute("SELECT sym FROM champ").fetchall()}
    todo = [s for s in scan.UNIV if s not in done]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nproc = max(1, cpu_count() - 2)
    print(f"scanning {len(todo)} stocks (of {len(scan.UNIV)}) on {nproc} workers...", flush=True)
    with Pool(nproc) as p:
        for r in p.imap_unordered(work, todo):
            con.execute("INSERT OR REPLACE INTO champ VALUES(?,?,?,?,?,?,?)",
                        (r[0], r[1], r[2], r[3], r[4], max(r[5], 0), now))
            con.commit()
            tag = "EDGE" if r[5] == 1 else ("ERR:" + r[6] if r[5] == -1 else "no edge")
            print(f"{r[0]:12} {tag}", flush=True)
    tot = con.execute("SELECT count(*) FROM champ").fetchone()[0]
    ed = con.execute("SELECT count(*) FROM champ WHERE has_edge=1").fetchone()[0]
    print(f"\nDONE — scanned {tot}/{len(scan.UNIV)} · validated edges (train&val t>=2.5): {ed}", flush=True)
    con.close()


if __name__ == "__main__":
    main()
