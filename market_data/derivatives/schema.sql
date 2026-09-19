-- db/derivatives.db — the F&O capture store (docs/DERIVATIVES_SPEC.md §2).
--
-- Rules this schema exists to enforce:
--   * every row carries provenance: vendor_id, fetched_at, snapshot_id;
--   * a snapshot is keyed by the 15-minute **mark** it belongs to, so a rerun
--     of the same mark replaces like with like and can never double-count;
--   * raw stays raw — derived numbers live in `metrics`, never written over a
--     captured column;
--   * nothing here is ever written to db/kanida.db or db/market15.db.

PRAGMA foreign_keys = ON;

-- ── the instrument universe ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS contracts (
    instrument_token INTEGER PRIMARY KEY,
    tradingsymbol    TEXT    NOT NULL,
    underlying       TEXT    NOT NULL,
    instrument_type  TEXT    NOT NULL,   -- CE | PE | FUT
    strike           REAL    NOT NULL DEFAULT 0,
    expiry           TEXT    NOT NULL,   -- YYYY-MM-DD
    lot_size         INTEGER NOT NULL,
    tick_size        REAL,
    exchange         TEXT    NOT NULL DEFAULT 'NFO',
    segment          TEXT,
    first_seen       TEXT    NOT NULL,   -- first session this contract was listed for us
    last_seen        TEXT    NOT NULL,   -- last instrument-list refresh that still had it
    in_scope         INTEGER NOT NULL DEFAULT 0,  -- front-two-expiry scope at last refresh
    vendor_id        TEXT    NOT NULL,
    fetched_at       TEXT    NOT NULL,
    snapshot_id      TEXT
);
CREATE INDEX IF NOT EXISTS ix_contracts_underlying ON contracts(underlying, expiry, instrument_type);
CREATE INDEX IF NOT EXISTS ix_contracts_scope      ON contracts(in_scope, expiry);
CREATE INDEX IF NOT EXISTS ix_contracts_symbol     ON contracts(tradingsymbol);

-- ── 15-minute quote snapshots (raw) ─────────────────────────────────────────
-- captured_at is the MARK (09:30, 09:45 … 15:30, plus the post-close mark),
-- naive IST.  fetched_at is the real wall clock of the request, so the lag
-- between the two is always visible.
CREATE TABLE IF NOT EXISTS snapshots (
    instrument_token INTEGER NOT NULL,
    captured_at      TEXT    NOT NULL,
    mark_kind        TEXT    NOT NULL DEFAULT 'bar_close',
    last_price       REAL,
    average_price    REAL,      -- the vendor's own day VWAP (quote only)
    average_price_est REAL,     -- ESTIMATE, marks rebuilt from candles only:
                                -- volume-weighted (high+low+close)/3 of the
                                -- session's bars so far.  NEVER the vendor's
                                -- number; anything shown from it must say
                                -- "estimated".
    volume           INTEGER,
    oi               INTEGER,
    oi_day_high      INTEGER,
    oi_day_low       INTEGER,
    buy_quantity     INTEGER,
    sell_quantity    INTEGER,
    bid              REAL,
    ask              REAL,
    bid_quantity     INTEGER,
    ask_quantity     INTEGER,
    day_open         REAL,
    day_high         REAL,
    day_low          REAL,
    prev_close       REAL,      -- quote.ohlc.close = previous session's close
    last_trade_time  TEXT,
    exchange_time    TEXT,      -- quote.timestamp as the exchange stamped it
    source           TEXT    NOT NULL DEFAULT 'kite.quote',
    vendor_id        TEXT    NOT NULL,
    fetched_at       TEXT    NOT NULL,
    snapshot_id      TEXT    NOT NULL,
    PRIMARY KEY (instrument_token, captured_at)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_snapshots_time ON snapshots(captured_at);
CREATE INDEX IF NOT EXISTS ix_snapshots_snap ON snapshots(snapshot_id);

-- ── 15-minute candles with OI (backfill + daily top-up) ─────────────────────
CREATE TABLE IF NOT EXISTS candles_15m (
    instrument_token INTEGER NOT NULL,
    bar_start        TEXT    NOT NULL,   -- naive IST
    open             REAL,
    high             REAL,
    low              REAL,
    close            REAL,
    volume           INTEGER,
    oi               INTEGER,
    vendor_id        TEXT    NOT NULL,
    fetched_at       TEXT    NOT NULL,
    snapshot_id      TEXT,
    PRIMARY KEY (instrument_token, bar_start)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_candles_time ON candles_15m(bar_start);

-- ── daily candles with OI, straight from the vendor ─────────────────────────
-- The SAME contract's own daily history (a future lives about three months, so
-- ~60 sessions), NOT a stitched continuous series and NOT the underlying.  It
-- is the vendor's own daily bar, which is why it lives here and not in
-- `daily_rollups`: that table is OUR roll-up of `candles_15m`/`snapshots`, and
-- rebuilding it must never overwrite what the exchange itself published.
--
-- `oi` is NULL when the vendor sent no open interest for that session.  Never
-- zero, never carried forward from the session before it.
CREATE TABLE IF NOT EXISTS candles_day (
    instrument_token INTEGER NOT NULL,
    session_date     TEXT    NOT NULL,   -- YYYY-MM-DD, the trading day (IST)
    open             REAL,
    high             REAL,
    low              REAL,
    close            REAL,
    volume           INTEGER,
    oi               INTEGER,
    vendor_id        TEXT    NOT NULL,
    fetched_at       TEXT    NOT NULL,
    snapshot_id      TEXT,
    PRIMARY KEY (instrument_token, session_date)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_candles_day_date ON candles_day(session_date);

-- ── per-underlying roll-up of the same mark ─────────────────────────────────
-- Only sums of what was actually captured are written here by the capture loop.
-- pcr_oi / pcr_volume / max_pain_strike are §3 signals and are left NULL for
-- the metrics worker; a NULL means "not computed", never "zero".
CREATE TABLE IF NOT EXISTS underlying_snapshots (
    underlying       TEXT    NOT NULL,
    captured_at      TEXT    NOT NULL,
    mark_kind        TEXT    NOT NULL DEFAULT 'bar_close',
    spot             REAL,
    spot_symbol      TEXT,               -- which NSE instrument the spot came from
    fut_price        REAL,
    fut_token        INTEGER,
    total_ce_oi      INTEGER,
    total_pe_oi      INTEGER,
    total_ce_volume  INTEGER,
    total_pe_volume  INTEGER,
    ce_contracts     INTEGER,
    pe_contracts     INTEGER,
    pcr_oi           REAL,
    pcr_volume       REAL,
    max_pain_strike  REAL,
    vendor_id        TEXT    NOT NULL,
    fetched_at       TEXT    NOT NULL,
    snapshot_id      TEXT    NOT NULL,
    PRIMARY KEY (underlying, captured_at)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_underlying_time ON underlying_snapshots(captured_at);

-- ── computed signals (spec §3) — written by the metrics worker, not by me ─
-- Column list owned jointly: the DDL below is D2's PROVISIONAL_METRICS_DDL,
-- adopted verbatim so its writer (which introspects this table and writes
-- only the intersection) can never silently drop a signal.  Retention: 1 year.
CREATE TABLE IF NOT EXISTS metrics (
    scope                 TEXT    NOT NULL,   -- 'contract' | 'underlying'
    metric_key            TEXT    NOT NULL,   -- tradingsymbol, or 'UNDERLYING|expiry'
    captured_at           TEXT    NOT NULL,
    instrument_token      INTEGER,
    tradingsymbol         TEXT,
    underlying            TEXT    NOT NULL,
    instrument_type       TEXT,
    strike                REAL,
    expiry                TEXT,
    lot_size              INTEGER,
    days_to_expiry        INTEGER,
    last_price            REAL,
    average_price         REAL,
    volume                REAL,
    oi                    REAL,
    spot                  REAL,
    price_change_15m      REAL,
    price_change_pct_15m  REAL,
    oi_change_15m         REAL,
    oi_change_pct_15m     REAL,
    buildup_15m           TEXT,
    price_change_day      REAL,
    price_change_pct_day  REAL,
    oi_change_day         REAL,
    oi_change_pct_day     REAL,
    buildup_day           TEXT,
    vol_tod_ratio         REAL,
    vol_tod_median        REAL,
    vol_tod_sessions      INTEGER,
    vol_tod_status        TEXT,
    vol_oi_ratio          REAL,
    vol_oi_prev_oi        REAL,
    vol_oi_spike          INTEGER,
    vol_oi_status         TEXT,
    premium_rs            REAL,
    premium_cr            REAL,
    premium_status        TEXT,
    pcr_oi                REAL,
    pcr_volume            REAL,
    pcr_trend             TEXT,
    pcr_trend_change      REAL,
    total_ce_oi           REAL,
    total_pe_oi           REAL,
    total_ce_volume       REAL,
    total_pe_volume       REAL,
    max_pain_strike       REAL,
    max_pain_distance     REAL,
    max_pain_total_oi     REAL,
    max_pain_status       TEXT,
    oi_change_pct_day_agg REAL,
    oi_change_day_status  TEXT,
    fut_oi_avg            REAL,
    fut_oi_vs_avg         REAL,
    fut_oi_vs_avg_status  TEXT,
    basis                 REAL,
    basis_pct             REAL,
    basis_status          TEXT,
    contracts             INTEGER,
    unusual_ce_strikes    INTEGER,
    unusual_pe_strikes    INTEGER,
    floors_passed         INTEGER,
    floors_failed         TEXT,
    unusual               INTEGER NOT NULL DEFAULT 0,
    unusual_reasons       TEXT,
    headline              TEXT,
    vendor_id             TEXT,
    fetched_at            TEXT,
    snapshot_id           TEXT,
    PRIMARY KEY (scope, metric_key, captured_at)
);
CREATE INDEX IF NOT EXISTS idx_metrics_und_at   ON metrics (underlying, captured_at);
CREATE INDEX IF NOT EXISTS idx_metrics_unusual  ON metrics (captured_at, unusual);
CREATE INDEX IF NOT EXISTS idx_metrics_expiry   ON metrics (underlying, expiry, captured_at);

-- ── daily roll-ups, kept for good (they survive the 90-day raw prune) ───────
CREATE TABLE IF NOT EXISTS daily_rollups (
    instrument_token INTEGER NOT NULL,
    session_date     TEXT    NOT NULL,
    underlying       TEXT,
    instrument_type  TEXT,
    expiry           TEXT,
    strike           REAL,
    open             REAL,
    high             REAL,
    low              REAL,
    close            REAL,
    volume           INTEGER,
    oi_open          INTEGER,
    oi_close         INTEGER,
    premium_rs       REAL,
    bars             INTEGER,
    marks            INTEGER,
    built_from       TEXT    NOT NULL,   -- candles_15m | snapshots
    vendor_id        TEXT    NOT NULL,
    fetched_at       TEXT    NOT NULL,
    PRIMARY KEY (instrument_token, session_date)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_rollup_date ON daily_rollups(session_date);

-- ── run bookkeeping: resumability and the real cost of a cycle ──────────────
CREATE TABLE IF NOT EXISTS captures (
    snapshot_id        TEXT PRIMARY KEY,
    mark_at            TEXT NOT NULL,
    mark_kind          TEXT NOT NULL,
    session_date       TEXT NOT NULL,
    started_at         TEXT,
    finished_at        TEXT,
    status             TEXT NOT NULL,      -- running | ok | partial | missed | error
    contracts_planned  INTEGER DEFAULT 0,
    rows_written       INTEGER DEFAULT 0,
    rows_skipped       INTEGER DEFAULT 0,
    underlyings_written INTEGER DEFAULT 0,
    requests           INTEGER DEFAULT 0,
    wall_seconds       REAL,
    lag_seconds        REAL,
    vendor_id          TEXT,
    error              TEXT
);
CREATE INDEX IF NOT EXISTS ix_captures_mark ON captures(mark_at);
CREATE INDEX IF NOT EXISTS ix_captures_day  ON captures(session_date, status);

CREATE TABLE IF NOT EXISTS backfill_progress (
    instrument_token INTEGER NOT NULL,
    timeframe        TEXT    NOT NULL DEFAULT '15minute',
    from_date        TEXT,
    through_date     TEXT,
    rows             INTEGER DEFAULT 0,
    status           TEXT    NOT NULL,     -- ok | empty | error
    error            TEXT,
    run_id           TEXT,
    updated_at       TEXT    NOT NULL,
    PRIMARY KEY (instrument_token, timeframe, through_date)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_backfill_run ON backfill_progress(run_id);

CREATE TABLE IF NOT EXISTS runs (
    run_id      TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,             -- capture | backfill | prune | rollup
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    status      TEXT,
    requests    INTEGER DEFAULT 0,
    rows        INTEGER DEFAULT 0,
    detail      TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
