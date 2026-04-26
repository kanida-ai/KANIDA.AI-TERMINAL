"""
KANIDA — DATABASE ENGINE
=========================
SQLite layer for fingerprints, paper ledger, and scan results.
Single file: kanida_fingerprints.db

Tables:
  fingerprints  — per-stock × per-strategy × per-bias × per-timeframe win rates
  paper_ledger  — historical + live paper trades with outcomes
  scan_results  — daily batch scan output history
  agents        — registered stock agents with their config

All reads/writes go through this module.
Both NSE and US stocks use identical schema.
"""

import sqlite3
import os
from datetime import datetime
from typing import List, Optional, Dict, Any

import os as _os
DB_PATH = _os.environ.get(
    "KANIDA_DB_PATH",
    _os.path.normpath(_os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "..", "data", "db", "kanida_fingerprints.db"))
)


# ══════════════════════════════════════════════════════════════════
# CONNECTION
# ══════════════════════════════════════════════════════════════════

def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row          # rows behave like dicts
    conn.execute("PRAGMA journal_mode=WAL") # safe concurrent reads
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ══════════════════════════════════════════════════════════════════
# SCHEMA — CREATE TABLES
# ══════════════════════════════════════════════════════════════════

CREATE_AGENTS = """
CREATE TABLE IF NOT EXISTS agents (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    market          TEXT    NOT NULL,           -- NSE / US
    agent_name      TEXT    NOT NULL,
    capital         REAL    DEFAULT 500000,
    max_risk_pct    REAL    DEFAULT 2.0,
    backtest_years  INTEGER DEFAULT 5,
    created_at      TEXT    NOT NULL,
    last_built      TEXT,                       -- when fingerprint was last computed
    status          TEXT    DEFAULT 'pending',  -- pending / ready / building
    UNIQUE(ticker, market)
)
"""

CREATE_FINGERPRINTS = """
CREATE TABLE IF NOT EXISTS fingerprints (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    market          TEXT    NOT NULL,
    bias            TEXT    NOT NULL,           -- bullish / bearish / neutral
    timeframe       TEXT    NOT NULL DEFAULT '1D',
    strategy_name   TEXT    NOT NULL,
    appearances     INTEGER DEFAULT 0,
    wins            INTEGER DEFAULT 0,
    win_rate        REAL    DEFAULT 0.0,
    avg_forward     REAL    DEFAULT 0.0,
    best_case       REAL    DEFAULT 0.0,
    worst_case      REAL    DEFAULT 0.0,
    median_forward  REAL    DEFAULT 0.0,
    qualifies       INTEGER DEFAULT 0,          -- 1 = qualifies, 0 = does not
    computed_at     TEXT    NOT NULL,
    backtest_years  INTEGER DEFAULT 5,
    UNIQUE(ticker, market, bias, timeframe, strategy_name, backtest_years)
)
"""

CREATE_PAPER_LEDGER = """
CREATE TABLE IF NOT EXISTS paper_ledger (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    market          TEXT    NOT NULL,
    bias            TEXT    NOT NULL,
    timeframe       TEXT    NOT NULL DEFAULT '1D',
    strategy_name   TEXT    NOT NULL,
    source          TEXT    NOT NULL,           -- historical / live
    signal_date     TEXT    NOT NULL,           -- date strategy fired
    entry_price     REAL    NOT NULL,
    stop_loss       REAL,
    stop_pct        REAL,
    target_1        REAL,
    target_2        REAL,
    exit_date       TEXT,                       -- NULL = still open
    exit_price      REAL,
    outcome_pct     REAL,                       -- actual % gain/loss at exit
    forward_15d_ret REAL,                       -- close[+15] vs entry
    status          TEXT    DEFAULT 'OPEN',     -- OPEN / T1_HIT / T2_HIT / STOPPED / EXPIRED
    win             INTEGER,                    -- 1 = win, 0 = loss, NULL = open
    agent_name      TEXT,
    logged_at       TEXT    NOT NULL
)
"""

CREATE_SCAN_RESULTS = """
CREATE TABLE IF NOT EXISTS scan_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_date       TEXT    NOT NULL,
    ticker          TEXT    NOT NULL,
    market          TEXT    NOT NULL,
    bias            TEXT    NOT NULL,
    timeframe       TEXT    NOT NULL DEFAULT '1D',
    qualified_total INTEGER DEFAULT 0,
    firing_count    INTEGER DEFAULT 0,
    score_pct       REAL    DEFAULT 0.0,
    score_label     TEXT,
    top_strategy    TEXT,
    top_win_rate    REAL,
    regime          TEXT,
    regime_score    REAL,
    bias_aligned    INTEGER,
    sector          TEXT,
    live_price      REAL,
    stop_loss       REAL,
    target_1        REAL,
    target_2        REAL,
    kelly_fraction  REAL,
    position_size   REAL
)
"""

INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_fp_ticker    ON fingerprints(ticker, market, bias, timeframe)",
    "CREATE INDEX IF NOT EXISTS idx_fp_qualifies ON fingerprints(qualifies, win_rate)",
    "CREATE INDEX IF NOT EXISTS idx_pl_ticker    ON paper_ledger(ticker, market, bias)",
    "CREATE INDEX IF NOT EXISTS idx_pl_date      ON paper_ledger(signal_date)",
    "CREATE INDEX IF NOT EXISTS idx_pl_status    ON paper_ledger(status)",
    "CREATE INDEX IF NOT EXISTS idx_sr_date      ON scan_results(scan_date, ticker)",
    "CREATE INDEX IF NOT EXISTS idx_agents       ON agents(ticker, market)",
]




CREATE_SIGNAL_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS agent_signal_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    market          TEXT    NOT NULL,
    bias            TEXT    NOT NULL,
    timeframe       TEXT    NOT NULL DEFAULT '1D',
    snapshot_date   TEXT    NOT NULL,   -- date of snapshot (YYYY-MM-DD)
    snapshot_time   TEXT    NOT NULL,   -- full timestamp
    score_pct       REAL    DEFAULT 0.0,
    score_label     TEXT    DEFAULT 'WATCHLIST',
    firing_count    INTEGER DEFAULT 0,
    qualified_total INTEGER DEFAULT 0,
    top_strategy    TEXT,
    top_win_rate    REAL    DEFAULT 0.0,
    firing_strategies TEXT  DEFAULT '[]',  -- JSON list
    regime          TEXT    DEFAULT '',
    regime_score    REAL    DEFAULT 0.0,
    bias_aligned    INTEGER DEFAULT 0,
    sector          TEXT    DEFAULT '',
    hist_win_pct    REAL    DEFAULT 0.0,
    hist_total      INTEGER DEFAULT 0,
    hist_avg_outcome REAL   DEFAULT 0.0,
    live_price      REAL    DEFAULT 0.0,
    stop_pct        REAL    DEFAULT 5.0,
    stop_loss       REAL    DEFAULT 0.0,
    target_1        REAL    DEFAULT 0.0,
    target_2        REAL    DEFAULT 0.0,
    currency        TEXT    DEFAULT 'Rs',
    UNIQUE(ticker, market, bias, timeframe, snapshot_date)
    ON CONFLICT REPLACE
)
"""

CREATE_SCREENER_CACHE = """
CREATE TABLE IF NOT EXISTS screener_cache (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market          TEXT    NOT NULL,
    bias            TEXT    NOT NULL,
    timeframe       TEXT    NOT NULL DEFAULT '1D',
    cache_date      TEXT    NOT NULL,
    cache_time      TEXT    NOT NULL,
    strong_buy      TEXT    DEFAULT '[]',   -- JSON: list of ticker strings
    strong_sell     TEXT    DEFAULT '[]',
    watchlist       TEXT    DEFAULT '[]',
    total_scanned   INTEGER DEFAULT 0,
    UNIQUE(market, bias, timeframe, cache_date)
    ON CONFLICT REPLACE
)
"""

SNAPSHOT_INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_snap_ticker  ON agent_signal_snapshots(ticker, market, bias, snapshot_date)",
    "CREATE INDEX IF NOT EXISTS idx_snap_date    ON agent_signal_snapshots(snapshot_date, market)",
    "CREATE INDEX IF NOT EXISTS idx_snap_score   ON agent_signal_snapshots(score_pct, bias_aligned)",
    "CREATE INDEX IF NOT EXISTS idx_cache_market ON screener_cache(market, bias, cache_date)",
]

def init_db(db_path: str = DB_PATH) -> None:
    """Create all tables and indices. Safe to call multiple times."""
    conn = get_conn(db_path)
    with conn:
        conn.execute(CREATE_AGENTS)
        conn.execute(CREATE_FINGERPRINTS)
        conn.execute(CREATE_PAPER_LEDGER)
        conn.execute(CREATE_SCAN_RESULTS)
        conn.execute(CREATE_SIGNAL_SNAPSHOTS)
        conn.execute(CREATE_SCREENER_CACHE)
        for idx in INDICES + SNAPSHOT_INDICES:
            conn.execute(idx)
    conn.close()
    print(f"  ✅  Database ready → {db_path}")


# ══════════════════════════════════════════════════════════════════
# AGENTS
# ══════════════════════════════════════════════════════════════════

def register_agent(ticker: str, market: str, agent_name: str,
                   capital: float = 500_000, max_risk_pct: float = 2.0,
                   backtest_years: int = 5, db_path: str = DB_PATH) -> int:
    """Register a stock agent. Returns agent id."""
    conn = get_conn(db_path)
    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with conn:
        cur = conn.execute("""
            INSERT INTO agents (ticker, market, agent_name, capital, max_risk_pct,
                                backtest_years, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
            ON CONFLICT(ticker, market) DO UPDATE SET
                agent_name     = excluded.agent_name,
                capital        = excluded.capital,
                max_risk_pct   = excluded.max_risk_pct,
                backtest_years = excluded.backtest_years,
                status         = 'pending'
        """, (ticker.upper(), market.upper(), agent_name,
              capital, max_risk_pct, backtest_years, now))
        agent_id = cur.lastrowid
    conn.close()
    return agent_id


def mark_agent_ready(ticker: str, market: str, db_path: str = DB_PATH) -> None:
    conn = get_conn(db_path)
    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with conn:
        conn.execute("""
            UPDATE agents SET status='ready', last_built=?
            WHERE ticker=? AND market=?
        """, (now, ticker.upper(), market.upper()))
    conn.close()


def get_agent(ticker: str, market: str, db_path: str = DB_PATH) -> Optional[Dict]:
    conn = get_conn(db_path)
    row  = conn.execute(
        "SELECT * FROM agents WHERE ticker=? AND market=?",
        (ticker.upper(), market.upper())
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def list_agents(db_path: str = DB_PATH) -> List[Dict]:
    conn = get_conn(db_path)
    rows = conn.execute(
        "SELECT * FROM agents ORDER BY market, ticker"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════
# FINGERPRINTS — WRITE
# ══════════════════════════════════════════════════════════════════

def save_fingerprint_batch(ticker: str, market: str, bias: str,
                           timeframe: str, backtest_years: int,
                           records: List[Dict], db_path: str = DB_PATH) -> None:
    """
    Save all strategy records for one ticker × bias × timeframe in one transaction.
    records: list of dicts with keys matching fingerprints table columns.
    """
    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn(db_path)
    with conn:
        for r in records:
            conn.execute("""
                INSERT INTO fingerprints
                    (ticker, market, bias, timeframe, strategy_name,
                     appearances, wins, win_rate, avg_forward,
                     best_case, worst_case, median_forward,
                     qualifies, computed_at, backtest_years)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(ticker, market, bias, timeframe, strategy_name, backtest_years)
                DO UPDATE SET
                    appearances    = excluded.appearances,
                    wins           = excluded.wins,
                    win_rate       = excluded.win_rate,
                    avg_forward    = excluded.avg_forward,
                    best_case      = excluded.best_case,
                    worst_case     = excluded.worst_case,
                    median_forward = excluded.median_forward,
                    qualifies      = excluded.qualifies,
                    computed_at    = excluded.computed_at
            """, (
                ticker.upper(), market.upper(), bias, timeframe,
                r["strategy_name"],
                r.get("appearances", 0), r.get("wins", 0),
                r.get("win_rate", 0.0), r.get("avg_forward", 0.0),
                r.get("best_case", 0.0), r.get("worst_case", 0.0),
                r.get("median_forward", 0.0),
                1 if r.get("qualifies") else 0,
                now, backtest_years,
            ))
    conn.close()


# ══════════════════════════════════════════════════════════════════
# FINGERPRINTS — READ
# ══════════════════════════════════════════════════════════════════

def get_fingerprint(ticker: str, market: str, bias: str,
                    timeframe: str = "1D", backtest_years: int = 5,
                    qualified_only: bool = False,
                    db_path: str = DB_PATH) -> List[Dict]:
    """Return all strategy records for a ticker × bias × timeframe."""
    conn  = get_conn(db_path)
    where = "WHERE ticker=? AND market=? AND bias=? AND timeframe=? AND backtest_years=?"
    params: list = [ticker.upper(), market.upper(), bias, timeframe, backtest_years]
    if qualified_only:
        where += " AND qualifies=1"
    rows = conn.execute(
        f"SELECT * FROM fingerprints {where} ORDER BY win_rate DESC",
        params
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def fingerprint_exists(ticker: str, market: str, db_path: str = DB_PATH) -> bool:
    """Check whether any fingerprint rows exist for this stock."""
    conn = get_conn(db_path)
    row  = conn.execute(
        "SELECT 1 FROM fingerprints WHERE ticker=? AND market=? LIMIT 1",
        (ticker.upper(), market.upper())
    ).fetchone()
    conn.close()
    return row is not None


def get_best_strategies(ticker: str, market: str,
                        bias: Optional[str] = None,
                        timeframe: str = "1D",
                        backtest_years: int = 5,
                        min_win_rate: float = 0.55,
                        limit: int = 20,
                        db_path: str = DB_PATH) -> List[Dict]:
    """
    Return top strategies for a stock across all biases (or filtered by bias),
    sorted by win_rate. Used for the stock-specific agent query.
    """
    conn   = get_conn(db_path)
    params: list = [ticker.upper(), market.upper(), timeframe,
                    backtest_years, min_win_rate]
    bias_clause = ""
    if bias:
        bias_clause = "AND bias=?"
        params.append(bias)
    rows = conn.execute(f"""
        SELECT * FROM fingerprints
        WHERE ticker=? AND market=? AND timeframe=? AND backtest_years=?
          AND win_rate >= ? AND qualifies=1
          {bias_clause}
        ORDER BY win_rate DESC, appearances DESC
        LIMIT ?
    """, params + [limit]).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_strategy_summary(ticker: str, market: str,
                         backtest_years: int = 5,
                         db_path: str = DB_PATH) -> Dict:
    """
    Returns a count summary across all biases for a stock.
    e.g. {bullish_qualified: 17, bearish_qualified: 10, neutral_qualified: 8}
    """
    conn = get_conn(db_path)
    rows = conn.execute("""
        SELECT bias,
               COUNT(*) as total,
               SUM(qualifies) as qualified,
               AVG(CASE WHEN qualifies=1 THEN win_rate END) as avg_win_rate
        FROM fingerprints
        WHERE ticker=? AND market=? AND backtest_years=?
        GROUP BY bias
    """, (ticker.upper(), market.upper(), backtest_years)).fetchall()
    conn.close()
    summary: Dict[str, Any] = {}
    for r in rows:
        summary[r["bias"]] = {
            "total":        r["total"],
            "qualified":    r["qualified"] or 0,
            "avg_win_rate": round(r["avg_win_rate"] or 0, 3),
        }
    return summary


# ══════════════════════════════════════════════════════════════════
# PAPER LEDGER — WRITE
# ══════════════════════════════════════════════════════════════════

def save_paper_trade(ticker: str, market: str, bias: str,
                     timeframe: str, strategy_name: str,
                     source: str,              # "historical" or "live"
                     signal_date: str,
                     entry_price: float,
                     forward_15d_ret: float,
                     stop_loss: float = 0.0,
                     stop_pct: float  = 0.0,
                     target_1: float  = 0.0,
                     target_2: float  = 0.0,
                     agent_name: str  = "",
                     db_path: str     = DB_PATH) -> None:
    """
    Save one paper trade entry.
    For historical trades: outcome is known immediately (forward_15d_ret).
    For live trades: outcome is None until updated.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Determine outcome for historical trades
    if source == "historical" and forward_15d_ret is not None:
        if bias == "bullish":
            win    = 1 if forward_15d_ret > 0 else 0
            status = "T2_HIT" if forward_15d_ret > stop_pct * 3 else \
                     "T1_HIT" if forward_15d_ret > stop_pct * 1.5 else \
                     "STOPPED" if forward_15d_ret < -stop_pct else "EXPIRED"
        elif bias == "bearish":
            win    = 1 if forward_15d_ret < 0 else 0
            actual = -forward_15d_ret
            status = "T2_HIT" if actual > stop_pct * 3 else \
                     "T1_HIT" if actual > stop_pct * 1.5 else \
                     "STOPPED" if actual < -stop_pct else "EXPIRED"
        else:
            win    = 1 if abs(forward_15d_ret) < 3.0 else 0
            status = "T1_HIT" if win else "EXPIRED"
        outcome_pct = round(forward_15d_ret, 3)
    else:
        win = status = outcome_pct = None

    conn = get_conn(db_path)
    with conn:
        conn.execute("""
            INSERT OR IGNORE INTO paper_ledger
                (ticker, market, bias, timeframe, strategy_name, source,
                 signal_date, entry_price, stop_loss, stop_pct,
                 target_1, target_2, forward_15d_ret, outcome_pct,
                 status, win, agent_name, logged_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            ticker.upper(), market.upper(), bias, timeframe, strategy_name,
            source, signal_date,
            round(entry_price, 4),
            round(stop_loss, 4) if stop_loss else None,
            round(stop_pct, 4) if stop_pct else None,
            round(target_1, 4) if target_1 else None,
            round(target_2, 4) if target_2 else None,
            round(forward_15d_ret, 4) if forward_15d_ret is not None else None,
            outcome_pct,
            status or "OPEN",
            win, agent_name, now,
        ))
    conn.close()


def update_live_trade_outcome(trade_id: int, exit_price: float,
                               exit_date: str, outcome_pct: float,
                               status: str, db_path: str = DB_PATH) -> None:
    """Update a live paper trade with its actual outcome."""
    conn = get_conn(db_path)
    win  = 1 if outcome_pct > 0 else 0
    with conn:
        conn.execute("""
            UPDATE paper_ledger
            SET exit_price=?, exit_date=?, outcome_pct=?, status=?, win=?
            WHERE id=?
        """, (round(exit_price, 4), exit_date,
              round(outcome_pct, 3), status, win, trade_id))
    conn.close()


# ══════════════════════════════════════════════════════════════════
# PAPER LEDGER — READ
# ══════════════════════════════════════════════════════════════════

def get_paper_ledger(ticker: str, market: str,
                     bias: Optional[str]      = None,
                     timeframe: Optional[str] = None,
                     source: Optional[str]    = None,   # historical / live
                     strategy: Optional[str]  = None,
                     status: Optional[str]    = None,
                     limit: int               = 100,
                     db_path: str             = DB_PATH) -> List[Dict]:
    """Flexible query on paper ledger."""
    conn   = get_conn(db_path)
    where  = ["ticker=?", "market=?"]
    params: list = [ticker.upper(), market.upper()]

    if bias:
        where.append("bias=?");        params.append(bias)
    if timeframe:
        where.append("timeframe=?");   params.append(timeframe)
    if source:
        where.append("source=?");      params.append(source)
    if strategy:
        where.append("strategy_name=?"); params.append(strategy)
    if status:
        where.append("status=?");      params.append(status)

    sql  = f"SELECT * FROM paper_ledger WHERE {' AND '.join(where)} ORDER BY signal_date DESC LIMIT ?"
    rows = conn.execute(sql, params + [limit]).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_ledger_stats(ticker: str, market: str,
                     bias: Optional[str] = None,
                     source: str = "historical",
                     db_path: str = DB_PATH) -> Dict:
    """
    Aggregate stats for the paper ledger:
    total trades, wins, losses, win%, avg outcome, best, worst.
    """
    conn   = get_conn(db_path)
    params: list = [ticker.upper(), market.upper(), source]
    bias_clause = ""
    if bias:
        bias_clause = "AND bias=?"
        params.append(bias)

    row = conn.execute(f"""
        SELECT
            COUNT(*)                           as total,
            SUM(win)                           as wins,
            SUM(CASE WHEN win=0 THEN 1 END)    as losses,
            AVG(outcome_pct)                   as avg_outcome,
            MAX(outcome_pct)                   as best,
            MIN(outcome_pct)                   as worst,
            AVG(CASE WHEN win=1 THEN outcome_pct END) as avg_win,
            AVG(CASE WHEN win=0 THEN outcome_pct END) as avg_loss
        FROM paper_ledger
        WHERE ticker=? AND market=? AND source=? AND win IS NOT NULL
        {bias_clause}
    """, params).fetchone()
    conn.close()

    if not row or not row["total"]:
        return {}

    total = row["total"] or 0
    wins  = row["wins"] or 0
    return {
        "total":      total,
        "wins":       wins,
        "losses":     row["losses"] or 0,
        "win_pct":    round(wins / total * 100, 1) if total else 0,
        "avg_outcome": round(row["avg_outcome"] or 0, 2),
        "best":       round(row["best"] or 0, 2),
        "worst":      round(row["worst"] or 0, 2),
        "avg_win":    round(row["avg_win"] or 0, 2),
        "avg_loss":   round(row["avg_loss"] or 0, 2),
    }


def get_strategy_ledger_stats(ticker: str, market: str,
                               bias: Optional[str] = None,
                               db_path: str = DB_PATH) -> List[Dict]:
    """Per-strategy win rate from the paper ledger."""
    conn   = get_conn(db_path)
    params: list = [ticker.upper(), market.upper()]
    bias_clause  = ""
    if bias:
        bias_clause = "AND bias=?"
        params.append(bias)

    rows = conn.execute(f"""
        SELECT strategy_name, bias,
               COUNT(*)                        as appearances,
               SUM(win)                        as wins,
               ROUND(AVG(outcome_pct),2)       as avg_outcome,
               ROUND(MAX(outcome_pct),2)       as best,
               ROUND(MIN(outcome_pct),2)       as worst
        FROM paper_ledger
        WHERE ticker=? AND market=? AND win IS NOT NULL
        {bias_clause}
        GROUP BY strategy_name, bias
        ORDER BY CAST(wins AS REAL)/COUNT(*) DESC
    """, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════
# SCAN RESULTS — WRITE / READ
# ══════════════════════════════════════════════════════════════════

def save_scan_result(result: Dict, db_path: str = DB_PATH) -> None:
    conn = get_conn(db_path)
    now  = datetime.now().strftime("%Y-%m-%d")
    with conn:
        conn.execute("""
            INSERT INTO scan_results
                (scan_date, ticker, market, bias, timeframe,
                 qualified_total, firing_count, score_pct, score_label,
                 top_strategy, top_win_rate, regime, regime_score,
                 bias_aligned, sector, live_price, stop_loss,
                 target_1, target_2, kelly_fraction, position_size)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now,
            result.get("ticker",""), result.get("market",""),
            result.get("bias",""),   result.get("timeframe","1D"),
            result.get("qualified_total",0), result.get("firing_count",0),
            result.get("score_pct",0.0),     result.get("score_label",""),
            result.get("top_strategy",""),   result.get("top_win_rate",0.0),
            result.get("regime",""),         result.get("regime_score",0.0),
            1 if result.get("bias_aligned") else 0,
            result.get("sector",""),
            result.get("live_price",0.0),    result.get("stop_loss",0.0),
            result.get("target_1",0.0),      result.get("target_2",0.0),
            result.get("kelly_fraction",0.0),result.get("position_size",0.0),
        ))
    conn.close()


def get_scan_history(ticker: str, market: str,
                     bias: Optional[str] = None,
                     days: int = 30,
                     db_path: str = DB_PATH) -> List[Dict]:
    """Recent scan history for a stock."""
    conn   = get_conn(db_path)
    params: list = [ticker.upper(), market.upper()]
    bias_clause  = ""
    if bias:
        bias_clause = "AND bias=?"
        params.append(bias)
    rows = conn.execute(f"""
        SELECT * FROM scan_results
        WHERE ticker=? AND market=? {bias_clause}
        ORDER BY scan_date DESC LIMIT ?
    """, params + [days]).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════
# UTILITY — PRINT HELPERS
# ══════════════════════════════════════════════════════════════════

def print_db_summary(db_path: str = DB_PATH) -> None:
    if not os.path.exists(db_path):
        print(f"  ❌  Database not found: {db_path}")
        return

    conn = get_conn(db_path)
    agents = conn.execute("SELECT COUNT(*) as n FROM agents WHERE status='ready'").fetchone()["n"]
    fp     = conn.execute("SELECT COUNT(*) as n FROM fingerprints").fetchone()["n"]
    pl     = conn.execute("SELECT COUNT(*) as n FROM paper_ledger").fetchone()["n"]
    sr     = conn.execute("SELECT COUNT(*) as n FROM scan_results").fetchone()["n"]
    fp_q   = conn.execute("SELECT COUNT(*) as n FROM fingerprints WHERE qualifies=1").fetchone()["n"]
    pl_h   = conn.execute("SELECT COUNT(*) as n FROM paper_ledger WHERE source='historical'").fetchone()["n"]
    pl_l   = conn.execute("SELECT COUNT(*) as n FROM paper_ledger WHERE source='live'").fetchone()["n"]

    print(f"\n  KANIDA DATABASE — {db_path}")
    print(f"  {'─'*50}")
    print(f"  Agents ready:          {agents}")
    print(f"  Fingerprint rows:      {fp:,}  ({fp_q:,} qualified)")
    print(f"  Paper trades:          {pl:,}  ({pl_h:,} historical · {pl_l:,} live)")
    print(f"  Scan result rows:      {sr:,}")

    # Per-agent summary
    rows = conn.execute("""
        SELECT a.ticker, a.market, a.last_built,
               COUNT(DISTINCT f.bias) as biases_built,
               SUM(f.qualifies) as qualified_strategies
        FROM agents a
        LEFT JOIN fingerprints f ON f.ticker=a.ticker AND f.market=a.market
        WHERE a.status='ready'
        GROUP BY a.ticker, a.market
        ORDER BY a.market, a.ticker
    """).fetchall()

    if rows:
        print(f"\n  {'TICKER':<12} {'MARKET':<6} {'BIASES':>6} {'QUALIFIED':>10} {'LAST BUILT'}")
        print(f"  {'─'*55}")
        for r in rows:
            print(f"  {r['ticker']:<12} {r['market']:<6} {r['biases_built']:>6} "
                  f"{r['qualified_strategies'] or 0:>10} {r['last_built'] or '—'}")
    conn.close()
    print()


if __name__ == "__main__":
    init_db()
    print_db_summary()


# ══════════════════════════════════════════════════════════════════
# SNAPSHOT READ / WRITE — agent_signal_snapshots
# ══════════════════════════════════════════════════════════════════

def write_snapshot(snap: dict, db_path: str = DB_PATH) -> bool:
    """
    Write one stock snapshot to agent_signal_snapshots.
    Called by the scheduler every refresh cycle.
    snap keys must match the table columns.
    """
    import json as _json
    conn = get_conn(db_path)
    try:
        firing = snap.get("firing_strategies", [])
        if isinstance(firing, list):
            firing = _json.dumps(firing)
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO agent_signal_snapshots
                    (ticker, market, bias, timeframe,
                     snapshot_date, snapshot_time,
                     score_pct, score_label, firing_count, qualified_total,
                     top_strategy, top_win_rate, firing_strategies,
                     regime, regime_score, bias_aligned, sector,
                     hist_win_pct, hist_total, hist_avg_outcome,
                     live_price, stop_pct, stop_loss, target_1, target_2,
                     currency)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                snap["ticker"].upper(), snap["market"].upper(),
                snap["bias"], snap.get("timeframe","1D"),
                snap["snapshot_date"], snap["snapshot_time"],
                snap.get("score_pct", 0.0),
                snap.get("score_label","WATCHLIST"),
                snap.get("firing_count", 0),
                snap.get("qualified_total", 0),
                snap.get("top_strategy",""),
                snap.get("top_win_rate", 0.0),
                firing,
                snap.get("regime",""),
                snap.get("regime_score", 0.0),
                1 if snap.get("bias_aligned") else 0,
                snap.get("sector",""),
                snap.get("hist_win_pct", 0.0),
                snap.get("hist_total", 0),
                snap.get("hist_avg_outcome", 0.0),
                snap.get("live_price", 0.0),
                snap.get("stop_pct", 5.0),
                snap.get("stop_loss", 0.0),
                snap.get("target_1", 0.0),
                snap.get("target_2", 0.0),
                snap.get("currency","Rs"),
            ))
        return True
    except Exception as e:
        print(f"  write_snapshot error {snap.get('ticker')}: {e}")
        return False
    finally:
        conn.close()


def read_snapshots(market: str, bias: str,
                   timeframe: str = "1D",
                   snapshot_date: str = None,
                   db_path: str = DB_PATH) -> List[dict]:
    """
    Read latest snapshots for all stocks matching market + bias.
    Returns one row per ticker (the most recent snapshot date).
    UI calls this — never calls the scanner directly.
    """
    import json as _json
    conn = get_conn(db_path)

    if snapshot_date is None:
        # Get most recent date available
        row = conn.execute("""
            SELECT MAX(snapshot_date) as latest
            FROM agent_signal_snapshots
            WHERE market=? AND bias=? AND timeframe=?
        """, (market.upper(), bias, timeframe)).fetchone()
        snapshot_date = row["latest"] if row and row["latest"] else None

    if not snapshot_date:
        conn.close()
        return []

    rows = conn.execute("""
        SELECT * FROM agent_signal_snapshots
        WHERE market=? AND bias=? AND timeframe=? AND snapshot_date=?
        ORDER BY score_pct DESC
    """, (market.upper(), bias, timeframe, snapshot_date)).fetchall()
    conn.close()

    result = []
    for r in rows:
        d = dict(r)
        try:
            d["firing_strategies"] = _json.loads(d.get("firing_strategies") or "[]")
        except Exception:
            d["firing_strategies"] = []
        d["bias_aligned"] = bool(d.get("bias_aligned", 0))
        result.append(d)
    return result


def read_snapshot_one(ticker: str, market: str, bias: str,
                      timeframe: str = "1D",
                      db_path: str = DB_PATH) -> Optional[dict]:
    """Read latest snapshot for a single ticker."""
    import json as _json
    conn = get_conn(db_path)
    row = conn.execute("""
        SELECT * FROM agent_signal_snapshots
        WHERE ticker=? AND market=? AND bias=? AND timeframe=?
        ORDER BY snapshot_date DESC, snapshot_time DESC
        LIMIT 1
    """, (ticker.upper(), market.upper(), bias, timeframe)).fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    try:
        d["firing_strategies"] = _json.loads(d.get("firing_strategies") or "[]")
    except Exception:
        d["firing_strategies"] = []
    d["bias_aligned"] = bool(d.get("bias_aligned", 0))
    return d


def get_snapshot_age(market: str, bias: str,
                     timeframe: str = "1D",
                     db_path: str = DB_PATH) -> dict:
    """
    Returns the age of the latest snapshot in minutes.
    Used by the API to show staleness warnings.
    """
    from datetime import datetime as _dt
    conn = get_conn(db_path)
    row = conn.execute("""
        SELECT MAX(snapshot_time) as latest_time,
               MAX(snapshot_date) as latest_date,
               COUNT(*) as total_stocks
        FROM agent_signal_snapshots
        WHERE market=? AND bias=? AND timeframe=?
    """, (market.upper(), bias, timeframe)).fetchone()
    conn.close()

    if not row or not row["latest_time"]:
        return {"age_minutes": None, "latest_date": None, "total_stocks": 0, "stale": True}

    try:
        latest = _dt.strptime(row["latest_time"], "%Y-%m-%d %H:%M:%S")
        age_min = ((_dt.now() - latest).total_seconds()) / 60
    except Exception:
        age_min = 9999

    return {
        "age_minutes":  round(age_min, 1),
        "latest_date":  row["latest_date"],
        "total_stocks": row["total_stocks"],
        "stale":        age_min > 60,   # stale if older than 60 minutes
    }


# ══════════════════════════════════════════════════════════════════
# SCREENER CACHE READ / WRITE
# ══════════════════════════════════════════════════════════════════

def write_screener_cache(market: str, bias: str,
                         strong_buy: list, strong_sell: list, watchlist: list,
                         total_scanned: int,
                         timeframe: str = "1D",
                         db_path: str = DB_PATH) -> bool:
    """Write pre-categorised screener results to DB cache."""
    import json as _json
    from datetime import datetime as _dt
    now = _dt.now()
    conn = get_conn(db_path)
    try:
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO screener_cache
                    (market, bias, timeframe, cache_date, cache_time,
                     strong_buy, strong_sell, watchlist, total_scanned)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                market.upper(), bias, timeframe,
                now.strftime("%Y-%m-%d"),
                now.strftime("%Y-%m-%d %H:%M:%S"),
                _json.dumps(strong_buy),
                _json.dumps(strong_sell),
                _json.dumps(watchlist),
                total_scanned,
            ))
        return True
    except Exception as e:
        print(f"  write_screener_cache error: {e}")
        return False
    finally:
        conn.close()


def read_screener_cache(market: str, bias: str,
                        timeframe: str = "1D",
                        db_path: str = DB_PATH) -> Optional[dict]:
    """Read latest screener cache row."""
    import json as _json
    conn = get_conn(db_path)
    row = conn.execute("""
        SELECT * FROM screener_cache
        WHERE market=? AND bias=? AND timeframe=?
        ORDER BY cache_date DESC, cache_time DESC
        LIMIT 1
    """, (market.upper(), bias, timeframe)).fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    try:
        d["strong_buy"]  = _json.loads(d.get("strong_buy")  or "[]")
        d["strong_sell"] = _json.loads(d.get("strong_sell") or "[]")
        d["watchlist"]   = _json.loads(d.get("watchlist")   or "[]")
    except Exception:
        d["strong_buy"] = d["strong_sell"] = d["watchlist"] = []
    return d

