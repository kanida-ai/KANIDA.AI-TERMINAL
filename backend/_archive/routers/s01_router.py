"""
KANIDA — S-01 API ROUTER
=========================
FastAPI router for the three Priority A read endpoints needed for the
S-01 vertical slice. All reads are from pre-computed tables — no live
scanning at request time.

Endpoints:
  GET /s01/feed              → headline feed (Level 0)
  GET /s01/list              → filtered stock list (Level 1)
  GET /s01/{ticker}/card     → stock card data + annotations (Level 2 default tab)
  GET /s01/{ticker}/history  → trade history (Level 2 detail tab — async, paginated)
  POST /s01/{ticker}/paper-trade  → initiate paper trade (Level 3)

Performance targets (from product contract):
  /feed    → <500ms
  /list    → <800ms
  /card    → <500ms
  /history → <2s (async, paginated 25 rows)
  /paper-trade (POST) → <300ms

Add to main.py:
  from routers.s01_router import router as s01_router
  app.include_router(s01_router, prefix="/s01", tags=["S-01"])

Architecture rules:
  - No live scan calls in any endpoint
  - All reads from: s01_signals, backtest_runs, chart_annotations
  - Paper trade writes to: paper_trade_summary, paper_trade_detail
  - Signal must have all 4 mandatory annotations or it is not returned
"""

import sqlite3
import json
import os
import logging
from datetime import datetime, date, timedelta
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()
log = logging.getLogger(__name__)

DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    "/app/data/kanida_fingerprints.db"   # Railway volume path
)

STRATEGY_ID = "S-01"
MANDATORY_ANNOTATION_TYPES = {"base_zone", "breakout_candle", "target_line", "stop_line"}


# ──────────────────────────────────────────────────────────────────
# DB HELPER
# ──────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def signal_is_complete(conn: sqlite3.Connection, signal_id: str) -> bool:
    """
    Returns True only if all 4 mandatory annotation types exist.
    Incomplete signals must never be returned by any endpoint.
    """
    rows = conn.execute("""
        SELECT annotation_type FROM chart_annotations
        WHERE signal_id = ? AND is_mandatory = 1
    """, (signal_id,)).fetchall()
    found = {r[0] for r in rows}
    return MANDATORY_ANNOTATION_TYPES.issubset(found)


# ──────────────────────────────────────────────────────────────────
# RESPONSE MODELS
# ──────────────────────────────────────────────────────────────────

class FeedResponse(BaseModel):
    strategy_id:    str
    headline:       str
    signal_count:   int
    agent_tag:      str
    timeframe_tag:  str
    markets:        List[str]
    last_updated:   str


class StockListItem(BaseModel):
    ticker:          str
    market:          str
    sector:          Optional[str]
    breakout_date:   str
    days_since_breakout: int
    regime:          str
    win_rate_pct:    Optional[float]
    win_rate_label:  Optional[str]
    total_signals:   Optional[int]
    signal_id:       str
    status:          str


class AggregateStats(BaseModel):
    win_rate_pct:        Optional[float]
    avg_return_pct:      Optional[float]
    max_return_pct:      Optional[float]
    min_return_pct:      Optional[float]
    total_signals:       Optional[int]
    avg_duration_weeks:  Optional[float]
    sebi_narrative:      str


class AnnotationItem(BaseModel):
    annotation_type: str
    start_date:      Optional[str]
    end_date:        Optional[str]
    price_level:     Optional[float]
    target_price:    Optional[float]
    stop_price:      Optional[float]
    base_high:       Optional[float]
    base_low:        Optional[float]
    base_depth_pct:  Optional[float]
    annotation_meta: dict
    is_mandatory:    bool


class StockCardResponse(BaseModel):
    signal_id:          str
    ticker:             str
    market:             str
    breakout_date:      str
    expiry_date:        str
    status:             str
    regime:             str
    sector:             Optional[str]
    base_duration_weeks: Optional[int]
    aggregate_stats:    AggregateStats
    annotations:        list[AnnotationItem]
    paper_trade_active: bool   # True = no open trade, button is active
    paper_trade_status: Optional[str]  # "open" | "closed" | None


class TradeHistoryRow(BaseModel):
    trade_id:    int
    entry_date:  str
    exit_date:   Optional[str]
    entry_price: Optional[float]
    exit_price:  Optional[float]
    target:      Optional[float]
    stop:        Optional[float]
    outcome:     Optional[str]
    pnl_pct:     Optional[float]
    regime:      Optional[str]
    source:      str
    status:      str


class PaperTradeRequest(BaseModel):
    user_id:       str
    ticker:        str
    market:        str
    signal_id:     str
    capital:       Optional[float] = None   # INR or USD
    quantity:      Optional[int]   = None   # units
    # One of capital or quantity must be provided


class PaperTradeResponse(BaseModel):
    trade_id:    int
    ticker:      str
    signal_id:   str
    entry_price: Optional[float]
    target:      Optional[float]
    stop:        Optional[float]
    status:      str
    message:     str


# ──────────────────────────────────────────────────────────────────
# SEBI NARRATIVE BUILDER
# ──────────────────────────────────────────────────────────────────

def build_sebi_narrative(ticker: str, market: str, stats: dict) -> str:
    """
    Build the SEBI-safe narrative sentence for a stock card.
    Uses X% and total_signals from backtest_runs.
    Never uses directive language.

    Template (from strategy framework v1.0):
    "Historically, X% of similar weekly base breakout setups on [market]
     reached the first target within Y weeks. This is a data observation,
     not a buy recommendation."
    """
    win_pct = stats.get("win_rate_pct")
    total = stats.get("total_signals")
    duration = stats.get("avg_duration_weeks")
    market_label = "NSE" if market == "NSE" else "US options"

    if win_pct and total and duration:
        return (
            f"Historically, {win_pct:.0f}% of similar weekly base breakout setups on {market_label} "
            f"reached the first target within {duration:.0f} weeks across {total} occurrences. "
            f"This is a data observation, not a buy recommendation."
        )
    elif win_pct and total:
        return (
            f"Historically, {win_pct:.0f}% of similar weekly base breakout setups on {market_label} "
            f"reached the first target across {total} occurrences. "
            f"This is a data observation, not a buy recommendation."
        )
    else:
        return (
            f"Insufficient historical data to compute a reliable pattern frequency for {ticker}. "
            f"This signal is in watchlist mode only."
        )


# ──────────────────────────────────────────────────────────────────
# ENDPOINTS
# ──────────────────────────────────────────────────────────────────

@router.get("/feed", response_model=FeedResponse)
def get_headline_feed(market: Optional[str] = None):
    """
    Level 0 — headline feed.
    Returns the autonomous headline for S-01 with signal count.
    Target: <500ms. No live scan. Reads only from s01_signals.
    """
    conn = get_conn()
    try:
        query = """
            SELECT COUNT(*) as n, MAX(last_updated) as last_updated,
                   GROUP_CONCAT(DISTINCT market) as markets
            FROM s01_signals
            WHERE status = 'active'
        """
        params = []
        if market:
            query = """
                SELECT COUNT(*) as n, MAX(last_updated) as last_updated,
                       GROUP_CONCAT(DISTINCT market) as markets
                FROM s01_signals
                WHERE status = 'active' AND market = ?
            """
            params = [market.upper()]

        row = conn.execute(query, params).fetchone()
        signal_count = row["n"] or 0
        last_updated = row["last_updated"] or datetime.utcnow().isoformat()
        markets = (row["markets"] or "").split(",") if row["markets"] else []

        # Headline is generated from signal count — never hardcoded
        if signal_count == 0:
            headline = "No active base breakout signals at this time"
        elif signal_count == 1:
            headline = "1 stock forming a strong weekly base — breakout in progress"
        else:
            headline = f"{signal_count} stocks forming strong weekly bases — breakouts in progress"

        return FeedResponse(
            strategy_id=STRATEGY_ID,
            headline=headline,
            signal_count=signal_count,
            agent_tag="Horizon Agent",
            timeframe_tag="Weekly",
            markets=markets,
            last_updated=last_updated,
        )
    finally:
        conn.close()


@router.get("/list", response_model=List[StockListItem])
def get_stock_list(
    market: Optional[str] = None,
    limit: int = Query(default=50, le=200),
):
    """
    Level 1 — filtered stock list.
    Returns all active S-01 signals sorted by breakout recency.
    Joined with backtest_runs for win_rate display.
    Target: <800ms. Pre-indexed on (status, market) and (strategy_id, ticker).
    """
    conn = get_conn()
    try:
        params = [STRATEGY_ID]
        market_clause = ""
        if market:
            market_clause = "AND s.market = ?"
            params.append(market.upper())

        rows = conn.execute(f"""
            SELECT
                s.signal_id, s.ticker, s.market, s.sector,
                s.breakout_date, s.regime, s.status,
                b.win_rate, b.total_signals
            FROM s01_signals s
            LEFT JOIN backtest_runs b
                ON b.ticker = s.ticker
               AND b.strategy_id = ?
               AND b.market = s.market
            WHERE s.status = 'active'
              {market_clause}
            ORDER BY s.breakout_date DESC
            LIMIT ?
        """, params + [limit]).fetchall()

        today = date.today()
        result = []
        for r in rows:
            # Only surface signals with complete mandatory annotations
            if not signal_is_complete(conn, r["signal_id"]):
                continue

            breakout_dt = date.fromisoformat(r["breakout_date"])
            days_since = (today - breakout_dt).days

            win_rate = r["win_rate"]
            win_rate_pct = round(win_rate * 100, 1) if win_rate else None
            total = r["total_signals"]
            win_rate_label = None
            if win_rate_pct and total:
                win_rate_label = f"Historically: {win_rate_pct:.0f}% reached target across {total} signals"

            result.append(StockListItem(
                ticker=r["ticker"],
                market=r["market"],
                sector=r["sector"],
                breakout_date=r["breakout_date"],
                days_since_breakout=days_since,
                regime=r["regime"],
                win_rate_pct=win_rate_pct,
                win_rate_label=win_rate_label,
                total_signals=total,
                signal_id=r["signal_id"],
                status=r["status"],
            ))

        return result
    finally:
        conn.close()


@router.get("/{ticker}/card", response_model=StockCardResponse)
def get_stock_card(ticker: str, market: str = "NSE"):
    """
    Level 2 — stock card default view.
    Returns signal data + aggregate backtest stats + all annotations.
    Target: <500ms. Three reads: s01_signals (1 row) + backtest_runs (1 row)
    + chart_annotations (4-7 rows). All indexed by ticker + strategy_id.
    """
    ticker = ticker.upper()
    market = market.upper()
    conn = get_conn()
    try:
        # ── Read signal ───────────────────────────────────────────
        signal = conn.execute("""
            SELECT * FROM s01_signals
            WHERE ticker = ? AND market = ? AND status = 'active'
            ORDER BY breakout_date DESC LIMIT 1
        """, (ticker, market)).fetchone()

        if not signal:
            raise HTTPException(
                status_code=404,
                detail=f"No active S-01 signal for {ticker} on {market}. Signal may have expired."
            )

        signal = dict(signal)
        signal_id = signal["signal_id"]

        # ── Verify mandatory annotations ──────────────────────────
        if not signal_is_complete(conn, signal_id):
            raise HTTPException(
                status_code=503,
                detail=f"Signal {signal_id} has incomplete chart annotations. Not ready to display."
            )

        # ── Read aggregate stats ──────────────────────────────────
        bt = conn.execute("""
            SELECT win_rate, avg_return, max_return, min_return,
                   total_signals, avg_duration_weeks
            FROM backtest_runs
            WHERE strategy_id = ? AND ticker = ? AND market = ?
        """, (STRATEGY_ID, ticker, market)).fetchone()

        stats_dict = {}
        if bt:
            stats_dict = {
                "win_rate_pct":       round((bt["win_rate"] or 0) * 100, 1),
                "avg_return_pct":     round((bt["avg_return"] or 0) * 100, 1),
                "max_return_pct":     round((bt["max_return"] or 0) * 100, 1),
                "min_return_pct":     round((bt["min_return"] or 0) * 100, 1),
                "total_signals":      bt["total_signals"],
                "avg_duration_weeks": bt["avg_duration_weeks"],
            }

        aggregate_stats = AggregateStats(
            **stats_dict,
            sebi_narrative=build_sebi_narrative(ticker, market, stats_dict),
        )

        # ── Read annotations ──────────────────────────────────────
        ann_rows = conn.execute("""
            SELECT * FROM chart_annotations
            WHERE signal_id = ?
            ORDER BY is_mandatory DESC, annotation_type ASC
        """, (signal_id,)).fetchall()

        annotations = []
        for a in ann_rows:
            try:
                meta = json.loads(a["annotation_meta"] or "{}")
            except Exception:
                meta = {}
            annotations.append(AnnotationItem(
                annotation_type=a["annotation_type"],
                start_date=a["start_date"],
                end_date=a["end_date"],
                price_level=a["price_level"],
                target_price=a["target_price"],
                stop_price=a["stop_price"],
                base_high=a["base_high"],
                base_low=a["base_low"],
                base_depth_pct=a["base_depth_pct"],
                annotation_meta=meta,
                is_mandatory=bool(a["is_mandatory"]),
            ))

        # ── Check paper trade status for this user/ticker ─────────
        # For now: check if any open paper trade exists for this signal
        # In production: filter by user_id from auth token
        try:
            open_trade = conn.execute("""
                SELECT id, status FROM paper_trade_summary
                WHERE signal_snapshot_id = ? AND status = 'open'
                LIMIT 1
            """, (signal_id,)).fetchone()
        except sqlite3.OperationalError:
            open_trade = None

        paper_trade_active = open_trade is None  # True = button is active
        paper_trade_status = open_trade["status"] if open_trade else None

        return StockCardResponse(
            signal_id=signal_id,
            ticker=ticker,
            market=market,
            breakout_date=signal["breakout_date"],
            expiry_date=signal["expiry_date"],
            status=signal["status"],
            regime=signal["regime"],
            sector=signal["sector"],
            base_duration_weeks=signal["base_duration_weeks"],
            aggregate_stats=aggregate_stats,
            annotations=annotations,
            paper_trade_active=paper_trade_active,
            paper_trade_status=paper_trade_status,
        )
    finally:
        conn.close()


@router.get("/{ticker}/history", response_model=List[TradeHistoryRow])
def get_trade_history(
    ticker: str,
    market: str = "NSE",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, le=100),
):
    """
    Level 2 detail tab — trade history (async, paginated).
    Loaded only when user clicks the Trade History tab.
    Target: <2s. Paginated 25 rows. Indexed by (strategy_id, ticker).
    """
    ticker = ticker.upper()
    market = market.upper()
    offset = (page - 1) * page_size

    conn = get_conn()
    try:
        # Check if paper_trade_summary exists with expected columns
        # Gracefully returns empty if table not yet populated
        try:
            rows = conn.execute("""
                SELECT
                    id as trade_id,
                    entry_date, exit_date,
                    entry_price, exit_price,
                    target, stop,
                    outcome, pnl,
                    regime_at_entry as regime,
                    source, status
                FROM paper_trade_summary
                WHERE ticker = ?
                  AND signal_snapshot_id LIKE 'S01_%'
                ORDER BY entry_date DESC
                LIMIT ? OFFSET ?
            """, (ticker, page_size, offset)).fetchall()
        except sqlite3.OperationalError:
            rows = []

        result = []
        for r in rows:
            pnl = r["pnl"]
            entry_price = r["entry_price"]
            pnl_pct = None
            if pnl is not None and entry_price and entry_price > 0:
                pnl_pct = round((pnl / entry_price) * 100, 2)

            result.append(TradeHistoryRow(
                trade_id=r["trade_id"],
                entry_date=r["entry_date"] or "",
                exit_date=r["exit_date"],
                entry_price=r["entry_price"],
                exit_price=r["exit_price"],
                target=r["target"],
                stop=r["stop"],
                outcome=r["outcome"],
                pnl_pct=pnl_pct,
                regime=r["regime"],
                source=r["source"] or "system_auto",
                status=r["status"] or "unknown",
            ))

        return result
    finally:
        conn.close()


@router.post("/{ticker}/paper-trade", response_model=PaperTradeResponse)
def initiate_paper_trade(ticker: str, body: PaperTradeRequest):
    """
    Level 3 — paper trade initiation.
    Target: <300ms. Two inserts: paper_trade_summary + first paper_trade_detail event.
    Signal must be active and have no existing open paper trade.
    """
    ticker = ticker.upper()
    market = body.market.upper()

    if body.capital is None and body.quantity is None:
        raise HTTPException(status_code=400, detail="Provide either capital or quantity, not neither.")
    if body.capital is not None and body.quantity is not None:
        raise HTTPException(status_code=400, detail="Provide either capital or quantity, not both.")

    conn = get_conn()
    try:
        # Verify signal is active
        signal = conn.execute("""
            SELECT * FROM s01_signals
            WHERE signal_id = ? AND ticker = ? AND market = ? AND status = 'active'
        """, (body.signal_id, ticker, market)).fetchone()

        if not signal:
            raise HTTPException(
                status_code=404,
                detail=f"Signal {body.signal_id} is not active. It may have expired or been invalidated."
            )
        signal = dict(signal)

        # Check no existing open paper trade for this signal
        existing = conn.execute("""
            SELECT id FROM paper_trade_summary
            WHERE signal_snapshot_id = ? AND user_id = ? AND status = 'open'
            LIMIT 1
        """, (body.signal_id, body.user_id)).fetchone()

        if existing:
            raise HTTPException(
                status_code=409,
                detail="You already have an open paper trade on this signal. View your trade instead."
            )

        # Get aggregate stats for target/stop (use signal values if available)
        target = signal.get("target_price_1")
        stop = signal.get("stop_price")
        entry_price = signal.get("breakout_price")

        now = datetime.utcnow().isoformat()
        today_str = str(date.today())

        # Write paper_trade_summary
        with conn:
            cursor = conn.execute("""
                INSERT INTO paper_trade_summary (
                    user_id, strategy_id, ticker, signal_snapshot_id,
                    entry_date, entry_price,
                    target, stop,
                    regime_at_entry, source, status,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                body.user_id, STRATEGY_ID, ticker, body.signal_id,
                today_str, entry_price,
                target, stop,
                signal.get("regime"), "user_triggered", "open",
                now
            ))
            trade_id = cursor.lastrowid

            # Write first paper_trade_detail event (entry event)
            conn.execute("""
                INSERT INTO paper_trade_detail (
                    trade_id, event_type, event_timestamp,
                    price_at_event, pnl_at_event, regime_at_event, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                trade_id, "entry", now,
                entry_price, 0.0,
                signal.get("regime"),
                f"User-initiated paper trade on S-01 signal {body.signal_id}"
            ))

        return PaperTradeResponse(
            trade_id=trade_id,
            ticker=ticker,
            signal_id=body.signal_id,
            entry_price=entry_price,
            target=target,
            stop=stop,
            status="open",
            message=f"Paper trade opened for {ticker}. Tracking from {today_str}.",
        )
    finally:
        conn.close()
