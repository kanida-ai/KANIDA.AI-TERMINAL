"""
Concurrency benchmark: SQLite (single-writer) vs Postgres, on the LIVE box.

Answers the real question — "how many users can place orders at the same time
before the DB is the bottleneck?" — with measured numbers instead of estimates.

WHAT IT SIMULATES
  Each "order" = one transaction of INSERT + UPDATE (the shape of an order write:
  append an order event, update a position). N worker threads each run K such
  transactions as fast as they can, at concurrency levels [1,5,10,25,50]. We
  report throughput (orders/sec) and error/lock-timeout counts per level.

SAFETY
  * SQLite runs against a SEPARATE scratch file on /localdb (bench_sqlite.db),
    NOT the serving DB — so it measures the same disk + WAL single-writer
    behaviour without contending with live traffic or touching real data.
  * Postgres uses a scratch table (bench_scratch) it creates and DROPS; direct
    short-lived connections (not the app pool) so we measure real DB concurrency,
    not the pool's max=8.
  * Reads/writes ONLY the scratch objects. No real order/position/user data is
    touched.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List

log = logging.getLogger(__name__)

LEVELS = [1, 5, 10, 25, 50]
OPS_PER_WORKER = 40


def _scratch_sqlite_path() -> str:
    local = os.environ.get("KANIDA_LOCAL_DB_DIR", "/localdb")
    base = local if os.path.isdir(local) else "."
    return os.path.join(base, "bench_sqlite.db")


# ── SQLite ───────────────────────────────────────────────────────────────────

def _sqlite_setup(path: str) -> None:
    con = sqlite3.connect(path, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("DROP TABLE IF EXISTS bench_scratch")
    con.execute("CREATE TABLE bench_scratch (worker INT, seq INT, val INT, ts REAL)")
    con.commit()
    con.close()


def _sqlite_worker(path: str, ops: int, out: List, idx: int) -> None:
    con = sqlite3.connect(path, timeout=60)
    con.execute("PRAGMA busy_timeout=60000")
    errs = 0
    t0 = time.time()
    for i in range(ops):
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute("INSERT INTO bench_scratch(worker,seq,val,ts) VALUES(?,?,?,?)",
                        (idx, i, i, time.time()))
            con.execute("UPDATE bench_scratch SET val=val+1 WHERE worker=? AND seq=?",
                        (idx, i))
            con.execute("COMMIT")
        except Exception:
            errs += 1
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
    con.close()
    out[idx] = (ops - errs, errs, time.time() - t0)


# ── Postgres ─────────────────────────────────────────────────────────────────

def _pg_connect():
    import psycopg2
    import pgdb
    return psycopg2.connect(pgdb.dsn(), connect_timeout=10)


def _pg_setup() -> None:
    con = _pg_connect()
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS bench_scratch")
    cur.execute("CREATE TABLE bench_scratch (worker INT, seq INT, val INT, ts DOUBLE PRECISION)")
    con.commit()
    con.close()


def _pg_teardown() -> None:
    try:
        con = _pg_connect()
        con.cursor().execute("DROP TABLE IF EXISTS bench_scratch")
        con.commit()
        con.close()
    except Exception:
        pass


def _pg_worker(ops: int, out: List, idx: int) -> None:
    try:
        con = _pg_connect()
    except Exception as e:
        out[idx] = (0, ops, 0.0, str(e)[:100])
        return
    cur = con.cursor()
    errs = 0
    t0 = time.time()
    for i in range(ops):
        try:
            cur.execute("INSERT INTO bench_scratch(worker,seq,val,ts) VALUES(%s,%s,%s,%s)",
                        (idx, i, i, time.time()))
            cur.execute("UPDATE bench_scratch SET val=val+1 WHERE worker=%s AND seq=%s",
                        (idx, i))
            con.commit()
        except Exception:
            errs += 1
            try:
                con.rollback()
            except Exception:
                pass
    con.close()
    out[idx] = (ops - errs, errs, time.time() - t0)


# ── Driver ───────────────────────────────────────────────────────────────────

def _run_level(kind: str, workers: int, ops: int, sqlite_path: str) -> Dict[str, Any]:
    out: List = [None] * workers
    threads = []
    wall0 = time.time()
    for idx in range(workers):
        if kind == "sqlite":
            t = threading.Thread(target=_sqlite_worker, args=(sqlite_path, ops, out, idx))
        else:
            t = threading.Thread(target=_pg_worker, args=(ops, out, idx))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    wall = time.time() - wall0
    ok = sum(r[0] for r in out if r)
    errs = sum(r[1] for r in out if r)
    return {
        "workers": workers,
        "total_ops": ok + errs,
        "ok": ok,
        "errors": errs,
        "wall_sec": round(wall, 3),
        "orders_per_sec": round(ok / wall, 1) if wall > 0 else 0,
    }


def run(levels: List[int] = None, ops: int = OPS_PER_WORKER) -> Dict[str, Any]:
    levels = levels or LEVELS
    result: Dict[str, Any] = {"ops_per_worker": ops, "levels": levels,
                              "sqlite": [], "postgres": []}

    # SQLite
    sqlite_path = _scratch_sqlite_path()
    try:
        _sqlite_setup(sqlite_path)
        for w in levels:
            _sqlite_setup(sqlite_path)  # fresh table each level
            result["sqlite"].append(_run_level("sqlite", w, ops, sqlite_path))
    except Exception as e:
        result["sqlite_error"] = f"{type(e).__name__}: {str(e)[:200]}"
    finally:
        try:
            os.remove(sqlite_path)
        except Exception:
            pass

    # Postgres
    try:
        import pgdb
        if not pgdb.dsn():
            result["postgres_error"] = "no DATABASE_URL configured"
        else:
            for w in levels:
                _pg_setup()
                result["postgres"].append(_run_level("postgres", w, ops, sqlite_path))
            _pg_teardown()
    except Exception as e:
        result["postgres_error"] = f"{type(e).__name__}: {str(e)[:200]}"

    # crossover summary
    def peak(rows):
        return max((r["orders_per_sec"] for r in rows), default=0)
    result["summary"] = {
        "sqlite_peak_orders_per_sec": peak(result["sqlite"]),
        "postgres_peak_orders_per_sec": peak(result["postgres"]),
    }
    return result


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    print(json.dumps(run(), indent=2))
