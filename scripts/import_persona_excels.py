"""Import the operator's V3 audit-ready persona Excel files into prod DB.

Source files (operator's Desktop):
  persona1_V3_fixed35k_2021_2026.xlsx           → daily-trader
  persona2_V3_fixed35k_2024_2026.xlsx           → patient-trader
  persona3_V3_variantA2_fixed_50k_2021_2026.xlsx → weekly-trader
  persona4_V3_locked_top14_sl10_tgt30.xlsx      → monthly-trader
  persona5_V3_locked_top15_sl5_tgt7.xlsx        → btst-trader

What it extracts:
  • Summary sheet rows 4-9 (or wherever the year rows live) → year-by-year
    return %, drawdown %, win-rate %, closed trades. Stored in
    portfolio_yearly_performance.
  • Monthly Equity sheet → 12 month-end equity values per year. We compute
    monthly return = (end_eq[m] - end_eq[m-1]) / end_eq[m-1]; January uses
    ₹5 L as the previous-month baseline (yearly reset). Stored in
    portfolio_monthly_performance.
  • Winning months per year = count(months where return_pct > 0). Backfilled
    into portfolio_yearly_performance.winning_months.

Idempotent: wipes both tables on each run, then re-inserts. Safe to call
on every backend boot (cost ≈ 1 sec) or via CLI.

Usage:
  python scripts/import_persona_excels.py
  python scripts/import_persona_excels.py --excel-dir C:\path\to\excels
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas + openpyxl required. pip install pandas openpyxl", file=sys.stderr)
    sys.exit(1)


# Default Excel locations (operator's Desktop) + slug mapping.
DEFAULT_FILES: List[Tuple[str, str]] = [
    ("daily-trader",   "persona1_V3_fixed35k_2021_2026.xlsx"),
    ("patient-trader", "persona2_V3_fixed35k_2024_2026.xlsx"),
    ("weekly-trader",  "persona3_V3_variantA2_fixed_50k_2021_2026.xlsx"),
    ("monthly-trader", "persona4_V3_locked_top14_sl10_tgt30.xlsx"),
    ("btst-trader",    "persona5_V3_locked_top15_sl5_tgt7.xlsx"),
]

DEFAULT_EXCEL_DIR = Path(os.environ.get("USERPROFILE", str(Path.home()))) / "Desktop"
YEARLY_RESET_CAPITAL_RS = 500_000.0      # ₹5 L per V3 audit


def _portfolio_id(con: sqlite3.Connection, slug: str) -> Optional[int]:
    row = con.execute("SELECT id FROM portfolio_definitions WHERE slug = ?", (slug,)).fetchone()
    return int(row[0]) if row else None


def _extract_yearly(xlsx_path: Path) -> List[Dict]:
    """Read the `Summary` sheet, find the year-rows table (header at row with
    'Year' in column 0), return rows as dicts."""
    df = pd.read_excel(xlsx_path, sheet_name="Summary", header=None)
    # Locate the header row — first row where column 0 == 'Year'
    header_row = None
    for i in range(min(20, len(df))):
        v = df.iat[i, 0]
        if isinstance(v, str) and v.strip().lower() == "year":
            header_row = i
            break
    if header_row is None:
        raise RuntimeError(f"{xlsx_path.name}: couldn't find 'Year' header in Summary sheet")

    cols = [str(c).strip() for c in df.iloc[header_row].tolist()]
    out: List[Dict] = []
    for i in range(header_row + 1, len(df)):
        v = df.iat[i, 0]
        if v is None or (isinstance(v, float) and pd.isna(v)):
            break    # blank row separates the data block from AGGREGATE
        try:
            yr = int(v)
        except (TypeError, ValueError):
            break    # 'AGGREGATE' or similar — end of data
        row = dict(zip(cols, df.iloc[i].tolist()))
        # Normalise keys
        out.append({
            "year":               yr,
            "mining_window":      str(row.get("Window", "")),
            "n_closed_trades":    int(row.get("Closed", 0) or 0),
            "start_cap_rs":       float(row.get("Start Cap", YEARLY_RESET_CAPITAL_RS) or 0),
            "end_equity_rs":      float(row.get("End Equity", 0) or 0),
            "pnl_rs":             float(row.get("P&L", 0) or 0),
            "return_pct":         float(row.get("Return %", 0) or 0),
            "max_drawdown_pct":   float(row.get("Max DD %", 0) or 0),
            "win_rate_pct":       float(row.get("Win Rate %", 0) or 0),
        })
    return out


def _extract_monthly(xlsx_path: Path) -> List[Dict]:
    """Read the `Monthly Equity` sheet — a pivot of {month_label: {year: end_equity}}.
    Returns one row per (year, month) with computed return%.
    """
    if "Monthly Equity" not in pd.ExcelFile(xlsx_path).sheet_names:
        return []
    df = pd.read_excel(xlsx_path, sheet_name="Monthly Equity", header=None)
    # Find the header row: first row whose col 0 == 'Month'
    header_row = None
    for i in range(min(10, len(df))):
        v = df.iat[i, 0]
        if isinstance(v, str) and v.strip().lower() == "month":
            header_row = i
            break
    if header_row is None:
        return []
    year_cols = []
    for c in range(1, df.shape[1]):
        v = df.iat[header_row, c]
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        try:
            year_cols.append((c, int(v)))
        except (TypeError, ValueError):
            continue
    months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    out: List[Dict] = []
    for year_col_idx, year in year_cols:
        # Walk down from header_row+1 — each row is one month
        prev_equity = YEARLY_RESET_CAPITAL_RS
        for m_idx, month_name in enumerate(months, start=1):
            row_idx = header_row + 1 + (m_idx - 1)
            if row_idx >= len(df):
                break
            label = df.iat[row_idx, 0]
            if not isinstance(label, str) or label.strip()[:3] != month_name:
                continue   # row layout drift — skip
            val = df.iat[row_idx, year_col_idx]
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue   # partial year (e.g. 2026 stops mid-year)
            end_eq = float(val)
            ret_pct = (end_eq - prev_equity) / prev_equity * 100.0 if prev_equity else 0.0
            out.append({
                "year":          year,
                "month":         m_idx,
                "end_equity_rs": end_eq,
                "return_pct":    ret_pct,
            })
            prev_equity = end_eq
    return out


def _winning_months_per_year(monthly: List[Dict]) -> Dict[int, int]:
    """Tally months where return_pct > 0, keyed by year."""
    out: Dict[int, int] = {}
    for r in monthly:
        if r["return_pct"] > 0:
            out[r["year"]] = out.get(r["year"], 0) + 1
    return out


def import_persona(con: sqlite3.Connection, slug: str, xlsx_path: Path) -> Dict:
    """Wipe + import one persona's yearly + monthly rows. Returns a summary."""
    pid = _portfolio_id(con, slug)
    if pid is None:
        return {"slug": slug, "skipped": True, "reason": "portfolio_definitions row missing"}
    if not xlsx_path.exists():
        return {"slug": slug, "skipped": True, "reason": f"file not found: {xlsx_path}"}

    yearly  = _extract_yearly(xlsx_path)
    monthly = _extract_monthly(xlsx_path)
    win_per_year = _winning_months_per_year(monthly)

    con.execute("DELETE FROM portfolio_yearly_performance  WHERE portfolio_id = ?", (pid,))
    con.execute("DELETE FROM portfolio_monthly_performance WHERE portfolio_id = ?", (pid,))

    # We treat the LAST year in the file as partial (e.g. 2026 mid-year).
    last_year = max((r["year"] for r in yearly), default=None)

    for r in yearly:
        is_partial = 1 if r["year"] == last_year and r["year"] >= 2026 else 0
        con.execute("""
            INSERT INTO portfolio_yearly_performance
              (portfolio_id, year, start_cap_rs, end_equity_rs, pnl_rs, return_pct,
               max_drawdown_pct, win_rate_pct, n_closed_trades, winning_months,
               is_partial, mining_window)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pid, r["year"], r["start_cap_rs"], r["end_equity_rs"], r["pnl_rs"],
            r["return_pct"], r["max_drawdown_pct"], r["win_rate_pct"],
            r["n_closed_trades"], win_per_year.get(r["year"]),
            is_partial, r.get("mining_window", ""),
        ))

    for r in monthly:
        is_partial = 1 if r["year"] == last_year and r["year"] >= 2026 else 0
        con.execute("""
            INSERT INTO portfolio_monthly_performance
              (portfolio_id, year, month, end_equity_rs, return_pct, is_partial)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (pid, r["year"], r["month"], r["end_equity_rs"], r["return_pct"], is_partial))

    con.commit()
    return {"slug": slug, "skipped": False,
            "yearly_rows": len(yearly), "monthly_rows": len(monthly),
            "winning_months_by_year": win_per_year}


def import_all(db_path: str, excel_dir: Optional[Path] = None) -> Dict:
    excel_dir = Path(excel_dir or DEFAULT_EXCEL_DIR)
    con = sqlite3.connect(db_path, timeout=60.0)
    summary = {"db_path": db_path, "excel_dir": str(excel_dir), "personas": []}
    try:
        for slug, fname in DEFAULT_FILES:
            r = import_persona(con, slug, excel_dir / fname)
            summary["personas"].append(r)
    finally:
        con.close()
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db",  default=None, help="path to kanida_universe.db")
    parser.add_argument("--excel-dir", default=None,
                        help="folder holding persona*.xlsx (default: ~/Desktop)")
    args = parser.parse_args()
    db = args.db
    if not db:
        ROOT = Path(__file__).resolve().parent.parent
        db = str(ROOT / "data" / "db" / "kanida_universe.db")
    import json
    print(json.dumps(import_all(db, args.excel_dir), indent=2, default=str))
