from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from engine.readers.sqlite_reader import ReadOnlySQLite


DATE_COLUMNS = {
    "ohlc_daily": "trade_date",
    "stock_trend_state": "trade_date",
    "signal_events": "signal_date",
    "signal_outcomes": "signal_date",
    "signal_roster": "last_calibrated_at",
    "stock_signal_fitness": "last_signal_date",
    "paper_trades": "entry_date",
}


def discover_sources(db: ReadOnlySQLite, outputs: Path) -> dict[str, Any]:
    tables = db.table_names()
    summary: list[dict[str, Any]] = []
    columns: dict[str, list[dict[str, Any]]] = {}
    market_coverage: dict[str, list[dict[str, Any]]] = {}

    for table in tables:
        info = db.table_info(table)
        columns[table] = info
        col_names = {c["name"] for c in info}
        row_count = db.scalar(f"select count(*) from {table}") or 0
        date_col = DATE_COLUMNS.get(table)
        min_date = max_date = None
        if date_col and date_col in col_names:
            min_date, max_date = db.conn.execute(
                f"select min({date_col}), max({date_col}) from {table}"
            ).fetchone()
        summary.append(
            {
                "table": table,
                "rows": row_count,
                "date_column": date_col or "",
                "min_date": min_date or "",
                "max_date": max_date or "",
                "columns": len(info),
            }
        )

        if "market" in col_names:
            parts = ["market", "count(*) as rows"]
            if date_col and date_col in col_names:
                parts.extend([f"min({date_col}) as min_date", f"max({date_col}) as max_date"])
            sql = f"select {', '.join(parts)} from {table} group by market order by market"
            market_coverage[table] = db.query(sql)

    write_csv(outputs / "discovery" / "source_tables.csv", summary)
    write_markdown(outputs / "discovery" / "source_discovery.md", summary, columns, market_coverage)
    return {
        "tables": summary,
        "columns": columns,
        "market_coverage": market_coverage,
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(
    path: Path,
    summary: list[dict[str, Any]],
    columns: dict[str, list[dict[str, Any]]],
    market_coverage: dict[str, list[dict[str, Any]]],
) -> None:
    lines: list[str] = []
    lines.append("# Source discovery")
    lines.append("")
    lines.append("SQLite opened read-only with query-only mode.")
    lines.append("")
    lines.append("## Tables")
    lines.append("")
    lines.append("| Table | Rows | Date column | Min date | Max date | Columns |")
    lines.append("|---|---:|---|---|---|---:|")
    for row in summary:
        lines.append(
            f"| {row['table']} | {row['rows']} | {row['date_column']} | "
            f"{row['min_date']} | {row['max_date']} | {row['columns']} |"
        )
    lines.append("")
    lines.append("## Market coverage")
    for table, rows in market_coverage.items():
        lines.append("")
        lines.append(f"### {table}")
        lines.append("")
        headers = list(rows[0].keys()) if rows else ["market", "rows"]
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("|" + "|".join("---" for _ in headers) + "|")
        for row in rows:
            lines.append("| " + " | ".join(str(row.get(h, "")) for h in headers) + " |")
    lines.append("")
    lines.append("## Assumed key fields")
    lines.append("")
    for table, info in columns.items():
        field_list = ", ".join(c["name"] for c in info)
        lines.append(f"- `{table}`: {field_list}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
