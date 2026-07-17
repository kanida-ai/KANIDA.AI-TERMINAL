"""LEG 3.a publisher — the ohlc_1min intraday-volume-PROFILE artifact.

WHY: `autotrade/execution/worked_order.load_intraday_profile` scans the ~95.7 M-row
`ohlc_1min` table (in the 38 GB R&D DB) at execution time to build a per-symbol
normalized 75-bucket intraday volume-profile SHAPE. The cloud replica will not carry
that 38 GB file, so this script PRECOMPUTES that shape into a tiny artifact
(`mkt_intraday_profile`), letting `load_intraday_profile` read a per-symbol vector
instead of copying 95.7 M rows.

WHAT (A1-style, mirrors publish_outcomes_evidence.py):
  * reads the R&D DB READ-ONLY (mode=ro, query_only) — a publish can NEVER mutate R&D,
  * replicates load_intraday_profile's EXACT computation (same SQL, same bucket math,
    same OPEN/CLOSE/BUCKET constants, imported from worked_order so they cannot drift),
  * stores the RAW per-bucket volume SUMS (not normalized) + n_days per symbol, so that
    reconstructing IntradayVolumeProfile(buckets=sums, n_days=...) re-runs the identical
    normalization → BYTE-IDENTICAL buckets (exact parity, proven by --verify-parity),
  * writes `mkt_intraday_profile` + `mkt_intraday_profile_manifest` (version / checksum /
    rowcounts / lookback / source commit / build time).

The artifact is default-off: nothing reads it until FALCON_MKT_SINK_DB is set (leg 3.a
resolver). This script does NOT flip anything, restart anything, or touch the live path.

Usage:
  python publish_volume_profile.py                       # publish all symbols
  python publish_volume_profile.py --limit 20            # sample (size estimate)
  python publish_volume_profile.py --verify-parity       # per-symbol exact-equality gate
  python publish_volume_profile.py --rnd-db <path> --out <path> --lookback-days 20 --min-days 5
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

# Import the EXACT constants + dataclass the runtime read uses, so the artifact can
# never drift from load_intraday_profile's bucketing.
from autotrade.execution.worked_order import (  # noqa: E402
    IntradayVolumeProfile, _PROFILE_BUCKET_SEC, _SESSION_CLOSE_SEC,
    _SESSION_OPEN_SEC)
from autotrade.mkt_sink import PROFILE_MANIFEST_TABLE, PROFILE_TABLE  # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))

_DEFAULT_RND_DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
# Under data/artifacts/ (gitignored .db, tracked .manifest.json) — matches the
# legs 1 & 2 publishers and keeps the multi-KB .db OUT of git. Note: data/db/*.db
# is NOT gitignored (Railway-baked), so never default the artifact into data/db/.
_DEFAULT_OUT = ROOT / "data" / "artifacts" / "kanida_mkt_profile.db"

N_BUCKETS = int((_SESSION_CLOSE_SEC - _SESSION_OPEN_SEC) // _PROFILE_BUCKET_SEC)  # 75


def _now_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def _source_commit() -> str:
    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(ROOT),
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip()[:12] if out.returncode == 0 else "unknown"
    except Exception:
        return "unknown"


def _connect_ro(path: Path) -> sqlite3.Connection:
    """Read-only, query_only — a publish must never be able to mutate R&D."""
    if not path.exists():
        raise SystemExit(f"FATAL: R&D DB not found at {path}")
    con = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=120.0)
    con.execute("PRAGMA query_only=ON")
    return con


def _symbol_buckets(con: sqlite3.Connection, symbol: str, *, lookback_days: int,
                    min_days: int):
    """Replicates load_intraday_profile EXACTLY: last `lookback_days` DISTINCT days
    of ohlc_1min for `symbol`, summed into `N_BUCKETS` `_PROFILE_BUCKET_SEC` buckets
    from `_SESSION_OPEN_SEC`. Returns (sums, n_days) or None on a THIN history."""
    days = [r[0] for r in con.execute(
        "SELECT DISTINCT substr(bar_time,1,10) d FROM ohlc_1min "
        "WHERE symbol=? ORDER BY d DESC LIMIT ?",
        (symbol, int(max(1, lookback_days)))).fetchall()]
    if len(days) < int(min_days):
        return None
    ph = ",".join("?" * len(days))
    rows = con.execute(
        "SELECT bar_time, volume FROM ohlc_1min "
        f"WHERE symbol=? AND substr(bar_time,1,10) IN ({ph})",
        [symbol] + days).fetchall()
    sums = [0.0] * N_BUCKETS
    for bt, v in rows:
        try:
            sod = int(bt[11:13]) * 3600 + int(bt[14:16]) * 60
        except Exception:
            continue
        rel = sod - _SESSION_OPEN_SEC
        if rel < 0:
            continue
        b = rel // _PROFILE_BUCKET_SEC
        if 0 <= b < N_BUCKETS:
            sums[b] += float(v or 0)
    return sums, len(days)


def _all_symbols(con: sqlite3.Connection, limit: int = 0):
    q = "SELECT DISTINCT symbol FROM ohlc_1min ORDER BY symbol"
    if limit and limit > 0:
        q += f" LIMIT {int(limit)}"
    return [r[0] for r in con.execute(q).fetchall()]


def _checksum(rows) -> str:
    """Order-stable checksum over (symbol, buckets_json, n_days)."""
    h = hashlib.sha256()
    for sym, bj, nd in sorted(rows, key=lambda r: r[0]):
        h.update(sym.encode()); h.update(b"\x00")
        h.update(bj.encode()); h.update(b"\x00")
        h.update(str(nd).encode()); h.update(b"\x00")
    return h.hexdigest()[:32]


def build_artifact(rnd_db: Path, out: Path, *, lookback_days: int, min_days: int,
                   limit: int = 0) -> dict:
    con = _connect_ro(rnd_db)
    try:
        symbols = _all_symbols(con, limit=limit)
        published, skipped_thin = [], 0
        for sym in symbols:
            res = _symbol_buckets(con, sym, lookback_days=lookback_days,
                                  min_days=min_days)
            if res is None:
                skipped_thin += 1
                continue
            sums, n_days = res
            published.append((sym, json.dumps(sums), int(n_days)))
    finally:
        con.close()

    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    art = sqlite3.connect(str(tmp))
    try:
        art.execute(
            f"CREATE TABLE {PROFILE_TABLE} ("
            "  symbol TEXT PRIMARY KEY,"
            "  buckets_json TEXT NOT NULL,"   # raw per-bucket volume SUMS (N_BUCKETS)
            "  n_days INTEGER NOT NULL,"
            "  as_of TEXT NOT NULL,"
            "  source_lookback_days INTEGER NOT NULL)")
        art.execute(
            f"CREATE TABLE {PROFILE_MANIFEST_TABLE} (key TEXT PRIMARY KEY, value TEXT)")
        as_of = _now_ist()
        art.executemany(
            f"INSERT INTO {PROFILE_TABLE} "
            "(symbol, buckets_json, n_days, as_of, source_lookback_days) "
            "VALUES (?,?,?,?,?)",
            [(s, bj, nd, as_of, int(lookback_days)) for (s, bj, nd) in published])
        checksum = _checksum(published)
        manifest = {
            "artifact": PROFILE_TABLE,
            "version": "leg3a-1",
            "built_at_ist": as_of,
            "source_db": str(rnd_db),
            "source_commit": _source_commit(),
            "n_symbols_published": str(len(published)),
            "n_symbols_skipped_thin": str(skipped_thin),
            "lookback_days": str(int(lookback_days)),
            "min_days": str(int(min_days)),
            "n_buckets": str(N_BUCKETS),
            "bucket_sec": str(int(_PROFILE_BUCKET_SEC)),
            "session_open_sec": str(int(_SESSION_OPEN_SEC)),
            "session_close_sec": str(int(_SESSION_CLOSE_SEC)),
            "checksum_sha256_32": checksum,
            "sampled_limit": str(int(limit)) if limit else "",
        }
        art.executemany(
            f"INSERT INTO {PROFILE_MANIFEST_TABLE} (key, value) VALUES (?,?)",
            list(manifest.items()))
        art.commit()
    finally:
        art.close()
    if out.exists():
        out.unlink()
    tmp.rename(out)

    size_mb = out.stat().st_size / (1024 * 1024)
    manifest["artifact_path"] = str(out)
    manifest["artifact_size_mb"] = round(size_mb, 3)
    return manifest


def verify_parity(rnd_db: Path, out: Path, *, lookback_days: int, min_days: int,
                  sample: int = 0) -> int:
    """Reconstruct each published symbol's IntradayVolumeProfile from the artifact
    and compare its normalized buckets to a fresh R&D scan (the runtime read).
    Exact float equality required. Returns 0 on full parity, 1 on any mismatch."""
    con = _connect_ro(rnd_db)
    a = sqlite3.connect(f"file:{out.as_posix()}?mode=ro", uri=True)
    try:
        a.execute("PRAGMA query_only=ON")
        rows = a.execute(
            f"SELECT symbol, buckets_json, n_days FROM {PROFILE_TABLE} "
            "ORDER BY symbol").fetchall()
        if sample and sample > 0:
            rows = rows[:sample]
        mism = 0
        for sym, bj, nd in rows:
            art_prof = IntradayVolumeProfile(
                buckets=[float(x) for x in json.loads(bj)], symbol=sym, n_days=int(nd))
            res = _symbol_buckets(con, sym, lookback_days=lookback_days,
                                  min_days=min_days)
            if res is None:
                print(f"  PARITY MISS {sym}: R&D now thin but artifact has it")
                mism += 1
                continue
            sums, n_days = res
            live_prof = IntradayVolumeProfile(buckets=list(sums), symbol=sym,
                                              n_days=n_days)
            if art_prof.buckets != live_prof.buckets or art_prof.n_days != n_days:
                print(f"  PARITY MISS {sym}: buckets/n_days differ")
                mism += 1
        print(f"verify-parity: {len(rows) - mism}/{len(rows)} symbols EXACT"
              f"{' (sampled)' if sample else ''}")
        return 0 if mism == 0 else 1
    finally:
        con.close(); a.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--rnd-db", type=Path, default=_DEFAULT_RND_DB)
    ap.add_argument("--out", type=Path, default=_DEFAULT_OUT)
    ap.add_argument("--lookback-days", type=int, default=20)
    ap.add_argument("--min-days", type=int, default=5)
    ap.add_argument("--limit", type=int, default=0,
                    help="publish only the first N symbols (size estimate/sample)")
    ap.add_argument("--verify-parity", action="store_true")
    ap.add_argument("--parity-sample", type=int, default=0,
                    help="verify only the first N published symbols")
    args = ap.parse_args()

    manifest = build_artifact(args.rnd_db, args.out, lookback_days=args.lookback_days,
                              min_days=args.min_days, limit=args.limit)
    print("PUBLISHED volume-profile artifact:")
    for k in ("artifact_path", "artifact_size_mb", "n_symbols_published",
              "n_symbols_skipped_thin", "lookback_days", "min_days", "n_buckets",
              "checksum_sha256_32", "built_at_ist", "source_commit"):
        print(f"  {k:24s}: {manifest[k]}")
    (args.out.parent / "kanida_mkt_profile.manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    if args.verify_parity:
        rc = verify_parity(args.rnd_db, args.out, lookback_days=args.lookback_days,
                           min_days=args.min_days, sample=args.parity_sample)
        return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
