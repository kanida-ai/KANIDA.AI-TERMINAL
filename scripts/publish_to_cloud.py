"""P1PUB — publish R&D-mined intelligence UP to the CLOUD production DB over HTTP.

This is the CLOUD counterpart of scripts/publish_patterns.py. The two differ
only in the TRANSPORT:

  publish_patterns.py   R&D DB  --(local sqlite3 file copy)-->  local PROD DB
  publish_to_cloud.py   R&D DB  --(HTTPS POST + secret)------->  cloud PROD DB

Why a separate script: once the production DB lives on a cloud volume
(Phase 1, see docs/launch/CLOUD_ARCHITECTURE.md §6) the laptop can NO LONGER
`sqlite3.connect` it remotely. The weekly publish must travel over an
authenticated HTTP endpoint. **For cloud production, THIS script is the path.**
`publish_patterns.py` stays for the local R&D→PROD case (dev / single box).

What it ships (compact — KB-MB, never the 14 GB warehouse):
  - falcon_pattern_candidates   (filtered by mining-window cutoff)
  - falcon_promoted_patterns    (linked to the eligible candidates)
  - falcon_pattern_taxonomy     (plain-English + regime tags for the above)

Selection logic is REPLICATED from publish_patterns.py (same cutoff via
`mining_window_years` from falcon_engine_config; same "promoted linked to
eligible candidate ids" rule). We replicate rather than import because
publish_patterns writes to a local PROD sqlite file — its public function is
file-to-file, not "give me the rows". The cutoff math here is kept identical;
if publish_patterns' filter changes, mirror it here.

Safety: the EMPTY-publish guard and atomic full-replace live on the SERVER
(backend/falcon/routers/publish_router.py). This client refuses to POST a
bundle with 0 promoted patterns too (fail fast, same message as
publish_patterns), so the round-trip isn't wasted.

Usage:
    python scripts/publish_to_cloud.py --dry-run
    python scripts/publish_to_cloud.py --cloud-url https://api.kanida.ai
    FALCON_PUBLISH_URL=https://api.kanida.ai \
        FALCON_PUBLISH_SECRET=...  python scripts/publish_to_cloud.py

Env:
    FALCON_PUBLISH_URL     base URL of the cloud backend (or --cloud-url)
    FALCON_PUBLISH_SECRET  shared secret → sent as X-Publish-Secret header
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

try:
    import requests
except ImportError:  # pragma: no cover - dependency check
    print("[FAIL] The 'requests' package is required. pip install requests",
          file=sys.stderr)
    sys.exit(1)


IST = timezone(timedelta(hours=5, minutes=30))
ROOT = Path(__file__).resolve().parent.parent

# R&D source DB — same default + env override convention as publish_patterns.
RND_DB = Path(
    os.environ.get("POWER_RND_DB_PATH")
    or (ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db")
)

BUNDLE_VERSION = 1
PUBLISH_PATH = "/api/falcon/publish/intelligence"

# Tables shipped, in dependency order (candidates is the parent referenced by
# the promoted JOIN). Order is preserved in the bundle dict (py3.7+).
SYNC_TABLES = ("falcon_pattern_candidates", "falcon_promoted_patterns",
               "falcon_pattern_taxonomy")


def _now_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def _columns(con: sqlite3.Connection, table: str) -> List[str]:
    return [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]


def _read_mining_window_years(con: sqlite3.Connection) -> int:
    """Mirror publish_patterns._read_mining_window_years. Defaults to 4."""
    try:
        row = con.execute(
            "SELECT mining_window_years FROM falcon_engine_config WHERE id=1"
        ).fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        pass
    return 4


def _rows_to_dicts(cols: List[str], rows: List[tuple]) -> List[Dict[str, Any]]:
    return [dict(zip(cols, r)) for r in rows]


def build_bundle(rnd_path: Path = RND_DB,
                 *, mining_window_years: int | None = None) -> Dict[str, Any]:
    """Read the three tables from the R&D DB, applying the SAME cutoff filter
    publish_patterns uses, and return a bundle_version=1 dict ready to POST.

    Raises FileNotFoundError / RuntimeError on fatal config errors (same shape
    as publish_patterns).
    """
    if not rnd_path.exists():
        raise FileNotFoundError(f"R&D DB not found: {rnd_path}")

    con = sqlite3.connect(str(rnd_path))
    try:
        if mining_window_years is None:
            mining_window_years = _read_mining_window_years(con)
        current_year = datetime.now(IST).year
        cutoff_year = current_year - int(mining_window_years)

        # ── falcon_pattern_candidates: WHERE mined_year >= cutoff_year ───────
        cand_cols = _columns(con, "falcon_pattern_candidates")
        if not cand_cols:
            raise RuntimeError("R&D DB missing table: falcon_pattern_candidates")
        cand_rows = con.execute(
            f"SELECT {', '.join(cand_cols)} FROM falcon_pattern_candidates "
            "WHERE mined_year >= ?",
            (cutoff_year,),
        ).fetchall()

        # ── falcon_promoted_patterns: linked to eligible candidate ids ──────
        prom_cols = _columns(con, "falcon_promoted_patterns")
        if not prom_cols:
            raise RuntimeError("R&D DB missing table: falcon_promoted_patterns")
        prom_rows = con.execute(
            f"""SELECT {', '.join(prom_cols)} FROM falcon_promoted_patterns p
                WHERE p.pattern_id IN (
                  SELECT pattern_id FROM falcon_pattern_candidates
                  WHERE mined_year >= ?
                )""",
            (cutoff_year,),
        ).fetchall()

        # Refuse to ship an empty promoted set (mirror publish_patterns guard;
        # the server enforces it too, this just fails fast).
        if len(prom_rows) == 0:
            raise RuntimeError(
                "R&D DB has 0 eligible promoted patterns after the cutoff. "
                "Refusing to publish empty data (would wipe cloud PROD). "
                "Run the weekly re-mine + validate first."
            )

        # ── falcon_pattern_taxonomy: ship in full (small, descriptive) ──────
        # Only the rows whose pattern_id is in the eligible promoted set so the
        # cloud taxonomy stays aligned with the published patterns.
        tax_cols = _columns(con, "falcon_pattern_taxonomy")
        tax_rows: List[tuple] = []
        if tax_cols:
            if "pattern_id" in tax_cols:
                tax_rows = con.execute(
                    f"""SELECT {', '.join(tax_cols)} FROM falcon_pattern_taxonomy
                        WHERE pattern_id IN (
                          SELECT pattern_id FROM falcon_pattern_candidates
                          WHERE mined_year >= ?
                        )""",
                    (cutoff_year,),
                ).fetchall()
            else:
                tax_rows = con.execute(
                    f"SELECT {', '.join(tax_cols)} FROM falcon_pattern_taxonomy"
                ).fetchall()

        tables: Dict[str, List[Dict[str, Any]]] = {
            "falcon_pattern_candidates": _rows_to_dicts(cand_cols, cand_rows),
            "falcon_promoted_patterns":  _rows_to_dicts(prom_cols, prom_rows),
        }
        if tax_cols:
            tables["falcon_pattern_taxonomy"] = _rows_to_dicts(tax_cols, tax_rows)

        return {
            "bundle_version": BUNDLE_VERSION,
            "generated_at":   _now_ist(),
            "source":         "research-laptop",
            "cutoff_year":    cutoff_year,
            "_mining_window_years": mining_window_years,  # local-only meta (server ignores extra top-level keys via pydantic)
            "tables":         tables,
        }
    finally:
        con.close()


def _bundle_summary(bundle: Dict[str, Any]) -> str:
    parts = [f"{t}: {len(rows)} rows" for t, rows in bundle["tables"].items()]
    return (f"bundle_version={bundle['bundle_version']} "
            f"source={bundle['source']} cutoff_year={bundle['cutoff_year']} "
            f"window={bundle.get('_mining_window_years')}yr generated_at={bundle['generated_at']}\n"
            "  " + "\n  ".join(parts))


def post_bundle(bundle: Dict[str, Any], cloud_url: str, secret: str,
                timeout: float = 120.0) -> Dict[str, Any]:
    """POST the bundle to the cloud ingest endpoint. Returns the parsed JSON."""
    url = cloud_url.rstrip("/") + PUBLISH_PATH
    # Strip local-only meta keys the server doesn't need.
    payload = {k: v for k, v in bundle.items() if not k.startswith("_")}
    resp = requests.post(
        url, json=payload,
        headers={"X-Publish-Secret": secret, "Content-Type": "application/json"},
        timeout=timeout,
    )
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}
    if resp.status_code != 200:
        raise RuntimeError(f"Server returned {resp.status_code}: {body}")
    return body


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Publish R&D intelligence to the cloud PROD DB.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Build + print the bundle summary; do NOT POST.")
    ap.add_argument("--cloud-url", default=None,
                    help="Base URL of the cloud backend (overrides FALCON_PUBLISH_URL).")
    ap.add_argument("--mining-window-years", type=int, default=None,
                    help="Override the cutoff window (default: falcon_engine_config).")
    args = ap.parse_args(argv)

    print(f"[{_now_ist()}] publish_to_cloud: building bundle from R&D")
    print(f"  R&D: {RND_DB}")
    try:
        bundle = build_bundle(mining_window_years=args.mining_window_years)
    except Exception as e:
        print(f"\n[FAIL] {e}", file=sys.stderr)
        return 1

    print("  Bundle:")
    print("  " + _bundle_summary(bundle).replace("\n", "\n  "))

    if args.dry_run:
        print("\n[dry-run] not POSTing. Bundle is ready.")
        return 0

    cloud_url = args.cloud_url or os.environ.get("FALCON_PUBLISH_URL")
    secret = os.environ.get("FALCON_PUBLISH_SECRET")
    if not cloud_url:
        print("\n[FAIL] No cloud URL. Set FALCON_PUBLISH_URL or pass --cloud-url.",
              file=sys.stderr)
        return 1
    if not secret:
        print("\n[FAIL] FALCON_PUBLISH_SECRET not set in env.", file=sys.stderr)
        return 1

    print(f"\n  POST {cloud_url.rstrip('/') + PUBLISH_PATH}")
    try:
        result = post_bundle(bundle, cloud_url, secret)
    except Exception as e:
        print(f"\n[FAIL] {e}", file=sys.stderr)
        return 1

    print("\n  Server response:")
    print(f"    status     : {result.get('status')}")
    print(f"    source     : {result.get('source')}")
    print(f"    cutoff_year: {result.get('cutoff_year')}")
    for t, c in (result.get("tables") or {}).items():
        print(f"    {t}: before={c.get('before')} after={c.get('after')}")
    print(f"    audit_run_id: {result.get('audit_run_id')}")
    print("\n[OK] Published.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
