"""
Fetch NSE index constituent lists and refresh stock_index_membership table.

Source: NSE archives (https://nsearchives.nseindia.com/content/indices/*.csv)
Why not Zerodha: Kite Connect does not expose index membership.

Usage:
    python data/ingest/fetch_nse_indices.py            # refresh all configured indices
    python data/ingest/fetch_nse_indices.py --dry-run  # fetch + parse, don't write

Returns a summary dict when called as a function (used by the admin endpoint).
"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "backend"))

log = logging.getLogger("kanida.fetch_nse_indices")
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")

CONFIG_PATH = ROOT / "config" / "nse_indices.json"

# NSE blocks default user-agents. A vanilla browser UA is enough for the static CSVs.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/csv,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


def _ensure_table(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_index_membership (
            ticker      TEXT NOT NULL,
            index_name  TEXT NOT NULL,
            added_on    TEXT NOT NULL DEFAULT (CURRENT_DATE),
            PRIMARY KEY (ticker, index_name)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_membership_index  ON stock_index_membership (index_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_membership_ticker ON stock_index_membership (ticker)")


def _load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _parse_csv(text: str) -> list[str]:
    """NSE index CSVs have headers like 'Company Name,Industry,Symbol,...'. We pull Symbol."""
    reader = csv.DictReader(io.StringIO(text))
    tickers: list[str] = []
    for row in reader:
        sym = (row.get("Symbol") or row.get("SYMBOL") or row.get("symbol") or "").strip().upper()
        if sym:
            tickers.append(sym)
    return tickers


def _fetch_one(client: httpx.Client, url: str) -> str:
    r = client.get(url, headers=HEADERS, timeout=30.0, follow_redirects=True)
    r.raise_for_status()
    return r.text


def refresh_indices(dry_run: bool = False) -> dict[str, Any]:
    """
    Fetch all configured indices from NSE and replace memberships in the DB.
    Returns a per-index summary suitable for the admin UI.
    """
    cfg = _load_config()
    base = cfg["base_url"]
    indices = cfg["indices"]

    results: list[dict[str, Any]] = []
    total_tickers = 0
    failed = 0

    fetched: dict[str, list[str]] = {}
    with httpx.Client() as client:
        for entry in indices:
            name = entry["name"]
            url = base + entry["file"]
            try:
                text = _fetch_one(client, url)
                tickers = _parse_csv(text)
                fetched[name] = tickers
                total_tickers += len(tickers)
                results.append({"index": name, "members": len(tickers), "status": "ok"})
                log.info("Fetched %s — %d members", name, len(tickers))
            except Exception as exc:
                failed += 1
                results.append({"index": name, "members": 0, "status": f"error: {exc}"})
                log.warning("Failed %s — %s", name, exc)

    if dry_run:
        return {
            "dry_run": True,
            "indices_attempted": len(indices),
            "indices_failed": failed,
            "total_tickers": total_tickers,
            "results": results,
        }

    from db import get_conn
    inserted = deleted = 0
    with get_conn() as conn:
        _ensure_table(conn)
        for name, tickers in fetched.items():
            # Replace strategy: delete existing rows for this index, then insert fresh.
            d = conn.execute(
                "DELETE FROM stock_index_membership WHERE index_name = ?", (name,)
            ).rowcount
            deleted += d
            for sym in tickers:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO stock_index_membership (ticker, index_name) VALUES (?, ?)",
                        (sym, name),
                    )
                    inserted += 1
                except Exception as exc:
                    log.warning("Insert failed %s/%s: %s", sym, name, exc)
        conn.commit()

    return {
        "dry_run": False,
        "indices_attempted": len(indices),
        "indices_failed": failed,
        "rows_inserted": inserted,
        "rows_deleted": deleted,
        "total_tickers": total_tickers,
        "results": results,
    }


def main() -> int:
    dry = "--dry-run" in sys.argv
    summary = refresh_indices(dry_run=dry)
    print(json.dumps(summary, indent=2))
    return 0 if summary["indices_failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
