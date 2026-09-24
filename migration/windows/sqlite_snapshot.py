"""Consistent copies of every SQLite file under a root, for the machine move.

A plain file copy of a database that a live service is writing (derivatives.db,
market15.db, intelligence.db) can tear mid-transaction. The online-backup API
reads one consistent snapshot while the writer keeps going.

    python sqlite_snapshot.py <src_root> <dst_root> <manifest.jsonl> <root-label>

Resumable and incremental: a finished copy leaves ``<dst>.snapshot-ok``; the next run
skips it unless the source (or its -wal) changed since — so re-running at cutover
re-copies only the databases the live services wrote to.
Each manifest line records size and, per table, ``max(rowid)`` — cheap even on
the 150 GB kanida.db — so the Mac side can prove the copy matches.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path

EXTS = {".db", ".sqlite", ".sqlite3"}
SKIP_DIRS = {"node_modules", ".venv", ".pilot-venv", "venv", "__pycache__", ".git", ".pytest_cache", "worktrees"}
HEADER = b"SQLite format 3\x00"


def is_sqlite(p: Path) -> bool:
    try:
        with p.open("rb") as f:
            return f.read(16) == HEADER
    except OSError:
        return False


def table_marks(con: sqlite3.Connection) -> dict:
    marks = {}
    for (name,) in con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"):
        try:
            marks[name] = con.execute(f'SELECT max(rowid) FROM "{name}"').fetchone()[0]
        except sqlite3.Error:            # WITHOUT ROWID tables
            marks[name] = None
    return marks


def snapshot(src: Path, dst: Path) -> dict:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".partial")
    if tmp.exists():
        tmp.unlink()
    if not is_sqlite(src):               # empty or not really SQLite: plain copy
        shutil.copy2(src, dst)
        return {"kind": "file", "bytes": dst.stat().st_size}
    s = sqlite3.connect(f"file:{src.as_posix()}?mode=ro", uri=True, timeout=60)
    d = sqlite3.connect(tmp)
    try:
        s.backup(d)                      # one step: a single consistent read snapshot
        d.execute("PRAGMA journal_mode=DELETE")
        marks = table_marks(d)
    finally:
        d.close()
        s.close()
    os.replace(tmp, dst)
    return {"kind": "sqlite", "bytes": dst.stat().st_size, "tables": marks}


def main() -> int:
    src_root, dst_root, manifest = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    label = sys.argv[4].replace("\\", "/")
    manifest.parent.mkdir(parents=True, exist_ok=True)
    found = []
    for dirpath, dirnames, filenames in os.walk(src_root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fn in filenames:
            if Path(fn).suffix.lower() in EXTS:
                found.append(Path(dirpath) / fn)
    found.sort(key=lambda p: p.stat().st_size)     # small first: early failures surface fast
    failures = 0
    with manifest.open("a", encoding="utf-8") as out:
        for src in found:
            rel = src.relative_to(src_root)
            dst = dst_root / rel
            ok = dst.with_name(dst.name + ".snapshot-ok")
            # changed since the last snapshot? (a WAL database's new data sits in -wal)
            wal = src.with_name(src.name + "-wal")
            src_m = max(src.stat().st_mtime, wal.stat().st_mtime if wal.exists() else 0)
            if ok.exists() and src_m <= ok.stat().st_mtime:
                continue
            gb = src.stat().st_size / 1e9
            t0 = time.time()
            print(f"  {gb:8.2f} GB  {rel} ...", end="", flush=True)
            try:
                rec = snapshot(src, dst)
            except Exception as e:           # report and keep going; the summary fails the run
                failures += 1
                print(f" FAILED ({type(e).__name__}: {e})")
                continue
            rec.update(root=label, path=rel.as_posix(), seconds=round(time.time() - t0, 1))
            out.write(json.dumps(rec) + "\n")
            out.flush()
            ok.write_text("ok", encoding="utf-8")
            print(f" ok ({rec['seconds']}s)")
    print(f"  {len(found)} database files, {failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
