"""
Universe router — single source of truth for every instrument KANIDA tracks.

GET  /api/universe                  list instruments (filter by sector, set, active)
POST /api/universe                  add one instrument
PUT  /api/universe/{symbol}         update sector / notes / active / universe_sets
DELETE /api/universe/{symbol}       soft-deactivate (never hard-delete)
POST /api/universe/seed             one-time seed from built-in F&O list (admin only)
POST /api/universe/bulk-import      import JSON array or CSV text (admin only)
GET  /api/universe/stats            counts by sector / exchange / universe_set
GET  /api/universe/data-audit       OHLCV source breakdown — exposes yfinance contamination
"""
from __future__ import annotations

import csv
import io
import json
import os
import sys
import sqlite3
from datetime import date
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()

_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE.parent))
from db import get_conn

DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    str(_HERE.parent.parent / "data" / "db" / "kanida_quant.db"),
)

# ── Built-in F&O seed list (used ONLY for /universe/seed) ─────────────────────
# After seeding, the DB is the authoritative source. Edit via the admin portal.

_FNO_TICKERS_WITH_SECTORS: list[tuple[str, str]] = [
    ("AARTIIND","Chemicals"),("ABB","Capital Goods"),("ABBOTINDIA","Pharma"),
    ("ABCAPITAL","NBFC"),("ABFRL","Retail"),("ACC","Cement"),
    ("ADANIENT","Conglomerate"),("ADANIPORTS","Infrastructure"),
    ("ALKEM","Pharma"),("AMBUJACEM","Cement"),("APOLLOHOSP","Healthcare"),
    ("APOLLOTYRE","Auto Ancillary"),("ASHOKLEY","Auto"),("ASIANPAINT","Paints"),
    ("ASTRAL","Pipes"),("AUBANK","Banks"),("AUROPHARMA","Pharma"),
    ("AXISBANK","Banks"),("BAJAJ-AUTO","Auto"),("BAJAJFINSV","NBFC"),
    ("BAJFINANCE","NBFC"),("BALKRISIND","Auto Ancillary"),("BANDHANBNK","Banks"),
    ("BANKBARODA","Banks"),("BATAINDIA","Consumer"),("BEL","Defence"),
    ("BERGEPAINT","Paints"),("BHARATFORG","Capital Goods"),("BHARTIARTL","Telecom"),
    ("BHEL","Capital Goods"),("BIOCON","Pharma"),("BPCL","Energy"),
    ("BRITANNIA","FMCG"),("CANBK","Banks"),("CANFINHOME","NBFC"),
    ("CHOLAFIN","NBFC"),("CIPLA","Pharma"),("COALINDIA","Mining"),
    ("COFORGE","IT"),("COLPAL","FMCG"),("CONCOR","Logistics"),
    ("CROMPTON","Consumer"),("DABUR","FMCG"),("DEEPAKNTR","Chemicals"),
    ("DIVISLAB","Pharma"),("DIXON","Consumer Electronics"),("DLF","Real Estate"),
    ("DRREDDY","Pharma"),("EICHERMOT","Auto"),("ESCORTS","Auto"),
    ("EXIDEIND","Auto Ancillary"),("FEDERALBNK","Banks"),("GAIL","Energy"),
    ("GLENMARK","Pharma"),("GODREJCP","FMCG"),("GODREJPROP","Real Estate"),
    ("GRANULES","Pharma"),("GRASIM","Cement"),("GUJGASLTD","Energy"),
    ("HAL","Defence"),("HAVELLS","Capital Goods"),("HCLTECH","IT"),
    ("HDFCAMC","Asset Management"),("HDFCBANK","Banks"),("HDFCLIFE","Insurance"),
    ("HEROMOTOCO","Auto"),("HINDALCO","Metals"),("HINDPETRO","Energy"),
    ("HINDUNILVR","FMCG"),("ICICIBANK","Banks"),("ICICIGI","Insurance"),
    ("ICICIPRULI","Insurance"),("IDEA","Telecom"),("IDFCFIRSTB","Banks"),
    ("IEX","Energy"),("IGL","Energy"),("INDHOTEL","Hotels"),
    ("INDUSINDBK","Banks"),("INFY","IT"),("IOC","Energy"),
    ("IRCTC","Travel"),("IRFC","NBFC"),("ITC","FMCG"),
    ("JINDALSTEL","Metals"),("JSWSTEEL","Metals"),("JUBLFOOD","Consumer"),
    ("KOTAKBANK","Banks"),("LAURUSLABS","Pharma"),("LICHSGFIN","NBFC"),
    ("LICI","Insurance"),("LT","Capital Goods"),("LTIM","IT"),
    ("LTTS","IT"),("LUPIN","Pharma"),("M&M","Auto"),
    ("MANAPPURAM","NBFC"),("MARICO","FMCG"),("MARUTI","Auto"),
    ("MCX","Exchange"),("MPHASIS","IT"),("MRF","Auto Ancillary"),
    ("MUTHOOTFIN","NBFC"),("NAUKRI","Internet"),("NESTLEIND","FMCG"),
    ("NMDC","Mining"),("NTPC","Power"),("OFSS","IT"),
    ("ONGC","Energy"),("PAGEIND","Consumer"),("PERSISTENT","IT"),
    ("PETRONET","Energy"),("PFC","NBFC"),("PIDILITIND","Chemicals"),
    ("PNB","Banks"),("POLYCAB","Capital Goods"),("POWERGRID","Power"),
    ("PVRINOX","Entertainment"),("RECLTD","NBFC"),("RELIANCE","Energy"),
    ("SAIL","Metals"),("SBICARD","NBFC"),("SBILIFE","Insurance"),
    ("SBIN","Banks"),("SHREECEM","Cement"),("SIEMENS","Capital Goods"),
    ("SRF","Chemicals"),("SUNPHARMA","Pharma"),("TATACHEM","Chemicals"),
    ("TATACONSUM","FMCG"),("TATAMOTORS","Auto"),("TATAPOWER","Power"),
    ("TATASTEEL","Metals"),("TCS","IT"),("TECHM","IT"),
    ("TITAN","Consumer"),("TORNTPHARM","Pharma"),("TORNTPOWER","Power"),
    ("TRENT","Retail"),("TVSMOTOR","Auto"),("UBL","FMCG"),
    ("ULTRACEMCO","Cement"),("UPL","Chemicals"),("VEDL","Metals"),
    ("VOLTAS","Consumer"),("WIPRO","IT"),("ZOMATO","Internet"),
    ("ZYDUSLIFE","Pharma"),("MCDOWELL-N","Consumer"),
]


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _conn():
    return get_conn()


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS universe (
            symbol          TEXT NOT NULL,
            exchange        TEXT NOT NULL DEFAULT 'NSE',
            asset_class     TEXT NOT NULL DEFAULT 'EQUITY',
            company_name    TEXT,
            sector          TEXT,
            industry        TEXT,
            universe_sets   TEXT NOT NULL DEFAULT '["FNO"]',
            is_active       INTEGER NOT NULL DEFAULT 1,
            added_date      TEXT NOT NULL,
            added_by        TEXT NOT NULL DEFAULT 'system',
            notes           TEXT,
            PRIMARY KEY (symbol, exchange)
        )
    """)
    conn.commit()


def _row_to_dict(row) -> dict:
    d = dict(row)
    try:
        d["universe_sets"] = json.loads(d.get("universe_sets") or "[]")
    except Exception:
        d["universe_sets"] = []
    d["is_active"] = bool(d.get("is_active", 1))
    return d


# ── Request / Response models ──────────────────────────────────────────────────

class AddInstrumentRequest(BaseModel):
    symbol:         str
    exchange:       str = "NSE"
    asset_class:    str = "EQUITY"
    company_name:   Optional[str] = None
    sector:         Optional[str] = None
    industry:       Optional[str] = None
    universe_sets:  List[str] = ["FNO"]
    notes:          Optional[str] = None
    added_by:       str = "admin"


class UpdateInstrumentRequest(BaseModel):
    sector:         Optional[str] = None
    industry:       Optional[str] = None
    company_name:   Optional[str] = None
    universe_sets:  Optional[List[str]] = None
    is_active:      Optional[bool] = None
    notes:          Optional[str] = None


class BulkImportRequest(BaseModel):
    # Accept either a JSON array of objects or CSV text
    data:       Optional[List[dict]] = None   # JSON path
    csv_text:   Optional[str]        = None   # CSV path: symbol,sector,exchange,industry
    exchange:   str = "NSE"                   # default exchange for CSV imports
    universe_sets: List[str] = ["FNO"]        # default sets for CSV imports
    added_by:   str = "admin"


class SeedRequest(BaseModel):
    secret: str


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/universe/stats")
def universe_stats():
    """Counts by sector, exchange, universe_set, active status."""
    with _conn() as conn:
        _ensure_table(conn)

        total = conn.execute("SELECT COUNT(*) FROM universe").fetchone()[0]
        active = conn.execute("SELECT COUNT(*) FROM universe WHERE is_active=1").fetchone()[0]

        by_sector = conn.execute("""
            SELECT COALESCE(sector,'Unknown') as sector, COUNT(*) as count
            FROM universe WHERE is_active=1
            GROUP BY sector ORDER BY count DESC
        """).fetchall()

        by_exchange = conn.execute("""
            SELECT exchange, COUNT(*) as count
            FROM universe WHERE is_active=1
            GROUP BY exchange ORDER BY count DESC
        """).fetchall()

    return {
        "total":       total,
        "active":      active,
        "inactive":    total - active,
        "by_sector":   [dict(r) for r in by_sector],
        "by_exchange":  [dict(r) for r in by_exchange],
    }


@router.get("/universe/data-audit")
def data_audit():
    """
    OHLCV source breakdown — shows yfinance contamination, staleness per ticker.
    Returns actionable warnings the admin can act on.
    """
    with _conn() as conn:
        _ensure_table(conn)

        try:
            source_rows = conn.execute("""
                SELECT source, COUNT(*) as rows, COUNT(DISTINCT ticker) as tickers,
                       MAX(trade_date) as latest_date
                FROM ohlc_daily GROUP BY source ORDER BY rows DESC
            """).fetchall()
        except Exception:
            source_rows = []

        try:
            yf_count = conn.execute(
                "SELECT COUNT(*) FROM ohlc_daily WHERE source='yfinance' OR source IS NULL"
            ).fetchone()[0]
        except Exception:
            yf_count = 0

        try:
            total_rows = conn.execute("SELECT COUNT(*) FROM ohlc_daily").fetchone()[0]
        except Exception:
            total_rows = 0

        try:
            stale_tickers = conn.execute("""
                SELECT ticker,
                       MAX(trade_date) as latest,
                       COUNT(*) as rows
                FROM ohlc_daily
                WHERE market='NSE'
                GROUP BY ticker
                HAVING latest < date('now', '-5 days')
                ORDER BY latest ASC
                LIMIT 20
            """).fetchall()
        except Exception:
            stale_tickers = []

        try:
            universe_missing = conn.execute("""
                SELECT u.symbol
                FROM universe u
                LEFT JOIN (
                    SELECT DISTINCT ticker FROM ohlc_daily WHERE market='NSE'
                ) o ON u.symbol = o.ticker
                WHERE u.is_active=1 AND u.exchange='NSE' AND o.ticker IS NULL
            """).fetchall()
        except Exception:
            universe_missing = []

    sources = [dict(r) for r in source_rows]
    contaminated = yf_count > 0

    warnings = []
    if contaminated:
        warnings.append({
            "level": "critical",
            "message": f"{yf_count:,} rows have source='yfinance' — these are contaminated and must be purged.",
            "action": "Run the pipeline with Kite fetch after purging yfinance rows.",
        })
    if stale_tickers:
        warnings.append({
            "level": "warning",
            "message": f"{len(stale_tickers)} tickers haven't had data in 5+ days.",
            "action": "Check Kite token validity and re-run the OHLCV fetch step.",
        })
    if universe_missing:
        warnings.append({
            "level": "info",
            "message": f"{len(universe_missing)} active universe stocks have zero OHLCV rows.",
            "action": "Run the OHLCV fetch pipeline to ingest their history.",
        })

    return {
        "total_ohlcv_rows":    total_rows,
        "yfinance_rows":       yf_count,
        "contamination_pct":   round(yf_count / total_rows * 100, 2) if total_rows else 0,
        "is_clean":            not contaminated,
        "sources":             sources,
        "stale_tickers":       [dict(r) for r in stale_tickers],
        "universe_missing_data": [r[0] for r in universe_missing],
        "warnings":            warnings,
    }


@router.get("/universe")
def list_universe(
    sector:   Optional[str] = Query(None),
    exchange: Optional[str] = Query(None),
    active:   Optional[bool] = Query(None),
    set_name: Optional[str] = Query(None),   # filter by universe_set membership
    search:   Optional[str] = Query(None),   # partial symbol or company_name match
    limit:    int = Query(500, le=2000),
    offset:   int = Query(0, ge=0),
):
    """List instruments with optional filters."""
    with _conn() as conn:
        _ensure_table(conn)

        clauses = []
        params: list = []

        if sector:
            clauses.append("sector = ?")
            params.append(sector)
        if exchange:
            clauses.append("exchange = ?")
            params.append(exchange)
        if active is not None:
            clauses.append("is_active = ?")
            params.append(1 if active else 0)
        if set_name:
            clauses.append("universe_sets LIKE ?")
            params.append(f'%"{set_name}"%')
        if search:
            q = f"%{search.upper()}%"
            clauses.append("(UPPER(symbol) LIKE ? OR UPPER(company_name) LIKE ?)")
            params.extend([q, q])

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = conn.execute(
            f"SELECT * FROM universe {where} ORDER BY symbol LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        total = conn.execute(
            f"SELECT COUNT(*) FROM universe {where}", params
        ).fetchone()[0]

    return {"total": total, "results": [_row_to_dict(r) for r in rows]}


@router.post("/universe")
def add_instrument(body: AddInstrumentRequest):
    """Add a single instrument to the universe."""
    with _conn() as conn:
        _ensure_table(conn)
        try:
            conn.execute("""
                INSERT INTO universe
                    (symbol, exchange, asset_class, company_name, sector,
                     industry, universe_sets, is_active, added_date, added_by, notes)
                VALUES (?,?,?,?,?,?,?,1,?,?,?)
            """, (
                body.symbol.upper().strip(),
                body.exchange.upper(),
                body.asset_class.upper(),
                body.company_name,
                body.sector,
                body.industry,
                json.dumps(body.universe_sets),
                str(date.today()),
                body.added_by,
                body.notes,
            ))
            conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(
                status_code=409,
                detail=f"{body.symbol.upper()} already exists on {body.exchange}. Use PUT to update it.",
            )

    return {"status": "added", "symbol": body.symbol.upper(), "exchange": body.exchange.upper()}


@router.put("/universe/{symbol}")
def update_instrument(symbol: str, body: UpdateInstrumentRequest, exchange: str = "NSE"):
    """Update sector, notes, active status, or universe sets for an instrument."""
    sym = symbol.upper().strip()
    with _conn() as conn:
        _ensure_table(conn)

        existing = conn.execute(
            "SELECT * FROM universe WHERE symbol=? AND exchange=?", (sym, exchange.upper())
        ).fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail=f"{sym} not found in universe.")

        updates: list[str] = []
        params: list = []

        if body.sector is not None:
            updates.append("sector=?"); params.append(body.sector)
        if body.industry is not None:
            updates.append("industry=?"); params.append(body.industry)
        if body.company_name is not None:
            updates.append("company_name=?"); params.append(body.company_name)
        if body.universe_sets is not None:
            updates.append("universe_sets=?"); params.append(json.dumps(body.universe_sets))
        if body.is_active is not None:
            updates.append("is_active=?"); params.append(1 if body.is_active else 0)
        if body.notes is not None:
            updates.append("notes=?"); params.append(body.notes)

        if not updates:
            return {"status": "no_changes"}

        params.extend([sym, exchange.upper()])
        conn.execute(
            f"UPDATE universe SET {', '.join(updates)} WHERE symbol=? AND exchange=?",
            params,
        )
        conn.commit()

    return {"status": "updated", "symbol": sym}


@router.delete("/universe/{symbol}")
def deactivate_instrument(symbol: str, exchange: str = "NSE"):
    """Soft-deactivate: marks is_active=0. Does not delete data or OHLCV rows."""
    sym = symbol.upper().strip()
    with _conn() as conn:
        _ensure_table(conn)
        n = conn.execute(
            "UPDATE universe SET is_active=0 WHERE symbol=? AND exchange=?",
            (sym, exchange.upper()),
        ).rowcount
        conn.commit()

    if n == 0:
        raise HTTPException(status_code=404, detail=f"{sym} not found.")
    return {"status": "deactivated", "symbol": sym, "note": "OHLCV history preserved. Re-activate via PUT."}


@router.post("/universe/seed")
def seed_universe(body: SeedRequest):
    """
    One-time seed: inserts the built-in 188 F&O stocks into the universe table.
    Safe to re-run — uses INSERT OR IGNORE so existing rows are untouched.
    Requires ADMIN_SECRET.
    """
    admin_secret = os.getenv("ADMIN_SECRET", "")
    if not admin_secret or body.secret != admin_secret:
        raise HTTPException(status_code=403, detail="Invalid admin secret.")

    today = str(date.today())
    inserted = skipped = 0

    with _conn() as conn:
        _ensure_table(conn)
        for sym, sector in _FNO_TICKERS_WITH_SECTORS:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO universe
                        (symbol, exchange, asset_class, sector, universe_sets,
                         is_active, added_date, added_by)
                    VALUES (?, 'NSE', 'EQUITY', ?, '["FNO"]', 1, ?, 'seed')
                """, (sym, sector, today))
                if conn.total_changes > 0:
                    inserted += 1
                else:
                    skipped += 1
            except Exception:
                skipped += 1
        conn.commit()

    return {
        "status":   "seeded",
        "inserted": inserted,
        "skipped":  skipped,
        "total_seed_size": len(_FNO_TICKERS_WITH_SECTORS),
        "message":  f"Inserted {inserted} new stocks. {skipped} already existed.",
    }


@router.post("/universe/purge-yfinance")
def purge_yfinance(body: SeedRequest):
    """
    Hard-delete all ohlc_daily rows with source='yfinance' or source IS NULL.
    These are contaminated rows that must be removed before relying on Kite data.
    Requires ADMIN_SECRET. IRREVERSIBLE — run the Kite fetch pipeline after this.
    """
    admin_secret = os.getenv("ADMIN_SECRET", "")
    if not admin_secret or body.secret != admin_secret:
        raise HTTPException(status_code=403, detail="Invalid admin secret.")

    with _conn() as conn:
        try:
            n = conn.execute(
                "DELETE FROM ohlc_daily WHERE source='yfinance' OR source IS NULL"
            ).rowcount
            conn.commit()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Purge failed: {e}")

    return {
        "status":       "purged",
        "rows_deleted": n,
        "message":      (
            f"Deleted {n:,} contaminated rows from ohlc_daily. "
            "Run the Kite OHLCV fetch pipeline to repopulate clean data."
        ),
    }


@router.post("/universe/bulk-import")
def bulk_import(body: BulkImportRequest):
    """
    Import multiple instruments at once.

    JSON path: send { "data": [ {"symbol":"SBIN","sector":"Banks"}, ... ] }
    CSV path:  send { "csv_text": "SBIN,Banks\\nRELIANCE,Energy" }
               Columns (in order): symbol, sector, exchange (opt), industry (opt)
    """
    admin_secret = os.getenv("ADMIN_SECRET", "")

    rows_to_insert: list[dict] = []
    today = str(date.today())

    if body.data:
        for item in body.data:
            sym = str(item.get("symbol", "")).upper().strip()
            if not sym:
                continue
            rows_to_insert.append({
                "symbol":        sym,
                "exchange":      str(item.get("exchange", body.exchange)).upper(),
                "sector":        item.get("sector"),
                "industry":      item.get("industry"),
                "company_name":  item.get("company_name"),
                "universe_sets": json.dumps(item.get("universe_sets", body.universe_sets)),
                "added_by":      body.added_by,
            })

    elif body.csv_text:
        reader = csv.DictReader(
            io.StringIO(body.csv_text.strip()),
            fieldnames=["symbol", "sector", "exchange", "industry"],
        )
        for row in reader:
            sym = (row.get("symbol") or "").upper().strip()
            if not sym or sym == "SYMBOL":   # skip header row if pasted with header
                continue
            rows_to_insert.append({
                "symbol":        sym,
                "exchange":      (row.get("exchange") or body.exchange).upper(),
                "sector":        row.get("sector") or None,
                "industry":      row.get("industry") or None,
                "company_name":  None,
                "universe_sets": json.dumps(body.universe_sets),
                "added_by":      body.added_by,
            })

    if not rows_to_insert:
        raise HTTPException(status_code=400, detail="No valid rows found in import data.")

    inserted = updated = errors = 0
    error_detail: list[str] = []

    with _conn() as conn:
        _ensure_table(conn)
        for r in rows_to_insert:
            try:
                # Upsert: if symbol+exchange exists, update sector/industry/sets
                conn.execute("""
                    INSERT INTO universe
                        (symbol, exchange, asset_class, sector, industry, company_name,
                         universe_sets, is_active, added_date, added_by)
                    VALUES (?, ?, 'EQUITY', ?, ?, ?, ?, 1, ?, ?)
                    ON CONFLICT(symbol, exchange) DO UPDATE SET
                        sector        = COALESCE(excluded.sector, sector),
                        industry      = COALESCE(excluded.industry, industry),
                        company_name  = COALESCE(excluded.company_name, company_name),
                        universe_sets = excluded.universe_sets,
                        is_active     = 1
                """, (
                    r["symbol"], r["exchange"],
                    r["sector"], r["industry"], r["company_name"],
                    r["universe_sets"], today, r["added_by"],
                ))
                inserted += 1
            except Exception as e:
                errors += 1
                error_detail.append(f"{r['symbol']}: {e}")
        conn.commit()

    return {
        "status":        "ok" if errors == 0 else "partial",
        "processed":     len(rows_to_insert),
        "inserted":      inserted,
        "errors":        errors,
        "error_detail":  error_detail[:10],   # cap at 10 for readability
    }
