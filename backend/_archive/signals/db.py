"""
KANIDA Signals DB
=================
Schema and connection for kanida_signals.db — the autonomous signal
learning engine. Completely separate from kanida_fingerprints.db.

All new signal intelligence writes here. The legacy DB is read-only
from this system's perspective.
"""

import sqlite3
import os

SIGNALS_DB_PATH = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "data", "db", "kanida_signals.db"
))

LEGACY_DB_PATH = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "data", "db", "kanida_fingerprints.db"
))


def get_conn(db_path: str = SIGNALS_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA cache_size=-32000")   # ~32 MB page cache
    conn.execute("PRAGMA synchronous=NORMAL")  # safe with WAL, faster than FULL
    return conn


# ──────────────────────────────────────────────────────────────────
# TABLE DEFINITIONS
# ──────────────────────────────────────────────────────────────────

_OHLC_DAILY = """
CREATE TABLE IF NOT EXISTS ohlc_daily (
    ticker          TEXT    NOT NULL,
    market          TEXT    NOT NULL,
    trade_date      TEXT    NOT NULL,
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL    NOT NULL,
    volume          REAL,
    source          TEXT    DEFAULT 'yfinance',
    fetched_at      TEXT    NOT NULL,
    UNIQUE(ticker, market, trade_date)
);
"""

_STOCK_TREND_STATE = """
CREATE TABLE IF NOT EXISTS stock_trend_state (
    ticker              TEXT    NOT NULL,
    market              TEXT    NOT NULL,
    trade_date          TEXT    NOT NULL,
    trend_state         TEXT    NOT NULL,   -- UPTREND | DOWNTREND | RANGE
    trend_strength      REAL    NOT NULL,   -- 0–100
    sma_50              REAL,
    sma_200             REAL,
    position_52w        REAL,               -- 0.0–1.0, price in 52-week range
    close               REAL    NOT NULL,
    low_confidence      INTEGER DEFAULT 0,  -- 1 if < 200 days of history
    computed_at         TEXT    NOT NULL,
    UNIQUE(ticker, market, trade_date)
);
"""

_SIGNAL_EVENTS = """
CREATE TABLE IF NOT EXISTS signal_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    market          TEXT    NOT NULL,
    timeframe       TEXT    NOT NULL,       -- 1D | 1W
    signal_date     TEXT    NOT NULL,
    strategy_name   TEXT    NOT NULL,
    bias            TEXT    NOT NULL,       -- bullish | bearish | neutral
    entry_price     REAL    NOT NULL,
    trend_state     TEXT,                   -- UPTREND | DOWNTREND | RANGE at signal time
    trend_strength  REAL,                   -- 0–100, NULL until OHLC synced
    source          TEXT    NOT NULL,       -- backfill_paper_ledger | live_detection
    detected_at     TEXT    NOT NULL,
    UNIQUE(ticker, market, timeframe, signal_date, strategy_name, bias)
);
"""

_SIGNAL_OUTCOMES = """
CREATE TABLE IF NOT EXISTS signal_outcomes (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_event_id         INTEGER,        -- FK → signal_events.id
    ticker                  TEXT    NOT NULL,
    market                  TEXT    NOT NULL,
    timeframe               TEXT    NOT NULL,
    strategy_name           TEXT    NOT NULL,
    bias                    TEXT    NOT NULL,
    signal_date             TEXT    NOT NULL,
    entry_price             REAL    NOT NULL,
    trend_state_at_signal   TEXT,           -- context at fire time, filled post OHLC sync

    -- Forward returns (% change from entry_price, price-direction: positive = price up)
    ret_1d                  REAL,
    ret_3d                  REAL,
    ret_5d                  REAL,
    ret_10d                 REAL,
    ret_15d                 REAL,
    ret_30d                 REAL,

    -- Directional wins (True = signal direction was correct)
    -- bullish: win if ret > 0 | bearish: win if ret < 0
    win_5d                  INTEGER,        -- 1/0/NULL
    win_15d                 INTEGER,
    win_30d                 INTEGER,

    -- Excursion over the 30-day window
    mfe_pct                 REAL,           -- Maximum Favorable Excursion %
    mae_pct                 REAL,           -- Maximum Adverse Excursion %
    mfe_day                 INTEGER,        -- day (1-30) MFE was reached
    mae_day                 INTEGER,        -- day (1-30) MAE was reached

    is_complete             INTEGER DEFAULT 0,  -- 1 when 30 trading days elapsed
    source                  TEXT    NOT NULL,   -- backfill | live_measured
    measured_at             TEXT    NOT NULL,

    UNIQUE(ticker, market, timeframe, signal_date, strategy_name, bias)
);
"""

_STOCK_SIGNAL_FITNESS = """
CREATE TABLE IF NOT EXISTS stock_signal_fitness (
    ticker                      TEXT    NOT NULL,
    market                      TEXT    NOT NULL,
    timeframe                   TEXT    NOT NULL,
    strategy_name               TEXT    NOT NULL,
    bias                        TEXT    NOT NULL,

    -- Sample base
    total_appearances           INTEGER DEFAULT 0,
    recent_appearances          INTEGER DEFAULT 0,  -- last 90 days

    -- Full-history accuracy
    win_rate_15d                REAL    DEFAULT 0,
    wilson_lower_15d            REAL    DEFAULT 0,  -- Wilson lower bound (90% CI)
    avg_ret_15d                 REAL    DEFAULT 0,
    median_ret_15d              REAL    DEFAULT 0,
    stddev_ret_15d              REAL    DEFAULT 1,

    -- Recent accuracy (last 90 days)
    recent_win_rate_15d         REAL    DEFAULT 0,
    recent_avg_ret_15d          REAL    DEFAULT 0,

    -- Trend-state conditioned breakdown
    win_rate_in_uptrend         REAL,
    win_rate_in_downtrend       REAL,
    win_rate_in_range           REAL,
    appearances_in_uptrend      INTEGER DEFAULT 0,
    appearances_in_downtrend    INTEGER DEFAULT 0,
    appearances_in_range        INTEGER DEFAULT 0,

    -- Excursion profile
    avg_mfe_pct                 REAL,
    avg_mae_pct                 REAL,
    mfe_mae_ratio               REAL,               -- reward/risk profile

    -- Composite scores (all 0–1 before final multiplication)
    accuracy_score              REAL    DEFAULT 0,  -- Wilson-adjusted
    return_score                REAL    DEFAULT 0,  -- sigmoid of avg/stddev
    frequency_score             REAL    DEFAULT 0,  -- appearances/yr capped at 24
    recency_score               REAL    DEFAULT 0.5, -- 0=degrading, 1=improving
    confidence_multiplier       REAL    DEFAULT 0,  -- sqrt(n/30) capped at 1
    raw_fitness                 REAL    DEFAULT 0,  -- 0–100 before confidence
    fitness_score               REAL    DEFAULT 0,  -- final 0–100

    -- Context
    best_trend_state            TEXT,               -- trend state with highest win rate
    first_signal_date           TEXT,
    last_signal_date            TEXT,
    last_calibrated_at          TEXT,

    UNIQUE(ticker, market, timeframe, strategy_name, bias)
);
"""

_SIGNAL_ROSTER = """
CREATE TABLE IF NOT EXISTS signal_roster (
    ticker                      TEXT    NOT NULL,
    market                      TEXT    NOT NULL,
    timeframe                   TEXT    NOT NULL,
    strategy_name               TEXT    NOT NULL,
    bias                        TEXT    NOT NULL,

    -- Current status
    status                      TEXT    NOT NULL DEFAULT 'test',
    -- test | watchlist | active | retired

    -- Scores at current status
    fitness_score               REAL    DEFAULT 0,
    fitness_at_promotion        REAL    DEFAULT 0,

    -- Track record since entering current status
    appearances_since_promotion INTEGER DEFAULT 0,
    win_rate_since_promotion    REAL,

    -- Trend gate: NULL = no restriction, else comma-sep allowed states
    -- e.g. 'UPTREND' or 'UPTREND,RANGE'
    trend_gate                  TEXT    DEFAULT NULL,

    -- History
    previous_status             TEXT,
    status_changed_at           TEXT    NOT NULL,
    demotion_reason             TEXT,
    last_calibrated_at          TEXT    NOT NULL,

    UNIQUE(ticker, market, timeframe, strategy_name, bias)
);
"""

_SIGNAL_GLOBAL_PATTERNS = """
CREATE TABLE IF NOT EXISTS signal_global_patterns (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    market           TEXT    NOT NULL,
    timeframe        TEXT    NOT NULL,
    strategy_name    TEXT    NOT NULL,
    bias             TEXT    NOT NULL,
    stocks_tested    INTEGER DEFAULT 0,   -- tickers with fitness data
    stocks_active    INTEGER DEFAULT 0,   -- tickers where status=active
    median_fitness   REAL,
    p25_fitness      REAL,
    p75_fitness      REAL,
    best_trend_state TEXT,               -- most common best_trend_state among active tickers
    last_updated_at  TEXT,
    UNIQUE(market, timeframe, strategy_name, bias)
);
"""

_PAPER_TRADES = """
CREATE TABLE IF NOT EXISTS paper_trades (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_event_id         INTEGER,        -- FK → signal_events.id
    ticker                  TEXT    NOT NULL,
    market                  TEXT    NOT NULL,
    timeframe               TEXT    NOT NULL,
    strategy_name           TEXT    NOT NULL,
    bias                    TEXT    NOT NULL,
    trend_state_entry       TEXT,           -- trend state when trade was logged
    trend_strength_entry    REAL,

    entry_date              TEXT    NOT NULL,
    entry_price             REAL    NOT NULL,
    stop_price              REAL,
    target_1                REAL,
    target_2                REAL,

    -- Exit tracking (updated daily until closed)
    status                  TEXT    NOT NULL DEFAULT 'open',
    -- open | hit_target_1 | hit_target_2 | stopped_out | expired
    exit_date               TEXT,
    exit_price              REAL,
    outcome_pct             REAL,
    win                     INTEGER,        -- 1/0/NULL while open
    days_held               INTEGER,

    logged_at               TEXT    NOT NULL,
    last_checked_at         TEXT
);
"""

_CALIBRATION_RUNS = """
CREATE TABLE IF NOT EXISTS calibration_runs (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker                  TEXT    NOT NULL,
    market                  TEXT    NOT NULL,
    run_date                TEXT    NOT NULL,
    signals_evaluated       INTEGER DEFAULT 0,
    promoted_to_active      INTEGER DEFAULT 0,
    demoted_from_active     INTEGER DEFAULT 0,
    promoted_to_watchlist   INTEGER DEFAULT 0,
    newly_retired           INTEGER DEFAULT 0,
    unchanged               INTEGER DEFAULT 0,
    fitness_p25             REAL,
    fitness_p50             REAL,
    fitness_p75             REAL,
    top_signal              TEXT,
    top_signal_fitness      REAL,
    run_duration_ms         INTEGER,
    notes                   TEXT,
    UNIQUE(ticker, market, run_date)
);
"""

_TICKER_ALIASES = """
CREATE TABLE IF NOT EXISTS ticker_aliases (
    canonical_ticker    TEXT    NOT NULL,
    alias_ticker        TEXT    NOT NULL,
    market              TEXT    NOT NULL,
    reason              TEXT,
    created_at          TEXT    NOT NULL,
    UNIQUE(alias_ticker, market)
);
"""

_SECTOR_MAPPING = """
CREATE TABLE IF NOT EXISTS sector_mapping (
    ticker          TEXT    NOT NULL,
    market          TEXT    NOT NULL,
    company_name    TEXT,
    sector          TEXT    NOT NULL,
    is_index        INTEGER DEFAULT 0,  -- 1 for index symbols (NIFTY etc.)
    yf_symbol       TEXT,               -- yfinance symbol override if different from ticker
    added_at        TEXT    NOT NULL,
    UNIQUE(ticker, market)
);
"""

_INDEX_METADATA = """
CREATE TABLE IF NOT EXISTS index_metadata (
    ticker          TEXT    NOT NULL,
    market          TEXT    NOT NULL,
    index_name      TEXT    NOT NULL,   -- e.g. NIFTY 50, BANK NIFTY
    yf_symbol       TEXT    NOT NULL,   -- e.g. ^NSEI, ^NSEBANK
    sector          TEXT,               -- e.g. Broad Market, Financials
    is_active       INTEGER DEFAULT 1,
    added_at        TEXT    NOT NULL,
    UNIQUE(ticker, market)
);
"""

_INDICES = [
    # sector_mapping
    "CREATE INDEX IF NOT EXISTS idx_sector_market     ON sector_mapping(market, sector)",

    # index_metadata
    "CREATE INDEX IF NOT EXISTS idx_index_market      ON index_metadata(market)",

    # ohlc_daily
    "CREATE INDEX IF NOT EXISTS idx_ohlc_ticker_date  ON ohlc_daily(ticker, market, trade_date DESC)",

    # stock_trend_state
    "CREATE INDEX IF NOT EXISTS idx_trend_ticker_date ON stock_trend_state(ticker, market, trade_date DESC)",

    # signal_events
    "CREATE INDEX IF NOT EXISTS idx_se_ticker_date    ON signal_events(ticker, market, timeframe, signal_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_se_strategy       ON signal_events(strategy_name, bias, market)",
    "CREATE INDEX IF NOT EXISTS idx_se_trend_state    ON signal_events(trend_state, market, signal_date DESC)",

    # signal_outcomes
    "CREATE INDEX IF NOT EXISTS idx_so_ticker_date    ON signal_outcomes(ticker, market, timeframe, signal_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_so_event_id       ON signal_outcomes(signal_event_id)",
    "CREATE INDEX IF NOT EXISTS idx_so_complete       ON signal_outcomes(is_complete, measured_at)",

    # stock_signal_fitness
    "CREATE INDEX IF NOT EXISTS idx_ssf_ticker        ON stock_signal_fitness(ticker, market, timeframe)",
    "CREATE INDEX IF NOT EXISTS idx_ssf_fitness       ON stock_signal_fitness(fitness_score DESC, market)",

    # signal_roster
    "CREATE INDEX IF NOT EXISTS idx_roster_ticker     ON signal_roster(ticker, market, timeframe, status)",
    "CREATE INDEX IF NOT EXISTS idx_roster_active     ON signal_roster(status, market, timeframe)",

    # paper_trades
    "CREATE INDEX IF NOT EXISTS idx_pt_ticker         ON paper_trades(ticker, market, status)",
    "CREATE INDEX IF NOT EXISTS idx_pt_open           ON paper_trades(status, entry_date DESC)",

    # signal_global_patterns
    "CREATE INDEX IF NOT EXISTS idx_sgp_market        ON signal_global_patterns(market, timeframe)",
    "CREATE INDEX IF NOT EXISTS idx_sgp_active        ON signal_global_patterns(stocks_active DESC, market)",
]


# ──────────────────────────────────────────────────────────────────
# INIT
# ──────────────────────────────────────────────────────────────────

def init_db(db_path: str = SIGNALS_DB_PATH) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    with conn:
        for ddl in [
            _TICKER_ALIASES, _SECTOR_MAPPING, _INDEX_METADATA,
            _OHLC_DAILY, _STOCK_TREND_STATE, _SIGNAL_EVENTS,
            _SIGNAL_OUTCOMES, _STOCK_SIGNAL_FITNESS, _SIGNAL_ROSTER,
            _PAPER_TRADES, _CALIBRATION_RUNS, _SIGNAL_GLOBAL_PATTERNS,
        ]:
            conn.execute(ddl)
        for idx in _INDICES:
            conn.execute(idx)

    conn.close()
