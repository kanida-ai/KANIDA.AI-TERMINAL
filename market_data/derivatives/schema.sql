-- db/derivatives.db — the F&O capture store (docs/DERIVATIVES_SPEC.md §2).
--
-- Rules this schema exists to enforce:
--   * every row carries provenance.  On `contracts`, `candles_15m`,
--     `candles_day` and `underlying_snapshots` that is vendor_id + fetched_at +
--     snapshot_id on the row itself.  On `snapshots` — 27,238 rows per mark, the
--     one table where 38 bytes of provenance per row is 27 MB a day — it is
--     vendor_id on the row and the rest in `captures`, one row per mark, joined
--     on captured_at = mark_at.  Provenance is never absent, only normalised;
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

-- ── vendor-token identity history (slice 14 / audit E01) ────────────────────
-- Kite warns an instrument_token may be REUSED for a different contract after
-- the first one expires.  `contracts` is keyed by that token, so a reuse used to
-- overwrite the old contract's symbol/strike/expiry/lot and every historical
-- bar silently changed identity.  Now, when a sync sees a known token carrying a
-- different economic contract (symbol, underlying, type, strike or expiry):
--   * the OLD identity is kept in `contracts` under a SURROGATE token (negative,
--     never a vendor token), copied verbatim, in_scope = 0;
--   * this table records that vendor token V meant contract C (surrogate) for
--     sessions up to `valid_through` (the old contract's expiry), and the vendor
--     token's own row from `valid_from` on;
--   * a same-contract change of lot or tick size is a METADATA version: the old
--     values are recorded here with reason 'metadata_change'.
-- Raw rows (snapshots, candles) are never rewritten; a point-in-time reader
-- resolves (token, session) through `DerivativesStore.contract_at`.
-- Additive: CREATE IF NOT EXISTS on every open; nothing existing is altered.
CREATE TABLE IF NOT EXISTS contract_token_history (
    vendor_token     INTEGER NOT NULL,
    contract_token   INTEGER NOT NULL,   -- the contracts row holding that identity
    reason           TEXT    NOT NULL,   -- token_reuse | metadata_change
    tradingsymbol    TEXT    NOT NULL,
    underlying       TEXT,
    instrument_type  TEXT,
    strike           REAL,
    expiry           TEXT,
    lot_size         INTEGER,
    tick_size        REAL,
    valid_from       TEXT,               -- first session this identity/metadata applied (YYYY-MM-DD, or first_seen)
    valid_through    TEXT,               -- last session it applied (YYYY-MM-DD)
    detected_at      TEXT    NOT NULL,   -- UTC, the sync that noticed the change
    snapshot_id      TEXT,
    PRIMARY KEY (vendor_token, contract_token, reason, detected_at)
);
CREATE INDEX IF NOT EXISTS ix_token_history_vendor ON contract_token_history(vendor_token, valid_through);

-- ── 15-minute quote snapshots (raw) ─────────────────────────────────────────
-- captured_at is the MARK (09:30, 09:45 … 15:30, plus the post-close mark),
-- naive IST.
--
-- Eleven columns were retired on 2026-09-19.  Each one was traced to its
-- readers first; the list and the evidence are in `store.RETIRED_SNAPSHOT_COLUMNS`.
-- Measured over the 265,601 rows on disk at the time, they cost 90.99 of the
-- record's 214.86 bytes and bought nothing.
--
--   never read anywhere : exchange_time, oi_day_high, oi_day_low, ask_quantity
--   loaded, never used  : bid, ask, buy_quantity, sell_quantity (they reach
--                         `metrics.Snapshot` and are not consumed), bid_quantity
--   moved to `captures` : fetched_at, snapshot_id
--
-- Provenance is NOT lost by that last pair: `captures` is one row per mark and
-- holds `snapshot_id` (its primary key), `mark_at`, `vendor_id`, `started_at`,
-- `finished_at` and `lag_seconds`.  A snapshot row's provenance is
--     SELECT * FROM captures WHERE mark_at = snapshots.captured_at
-- which is exact, because a mark has exactly one capture.  `vendor_id` stays on
-- the row: 5 bytes, and it is the one field that must be true per row if a
-- second vendor is ever mixed in.
--
-- NOTE FOR AN EXISTING DATABASE: `CREATE TABLE IF NOT EXISTS` does not alter a
-- table that already exists, and nothing here rewrites one.  A store created
-- before this date keeps all 26 columns and all its rows untouched; the capture
-- path simply stops filling the retired ones (`store.write_snapshots` writes
-- only the columns the live table has, and still fills any the live table
-- declares NOT NULL).  Reclaiming the bytes already on disk is a table rebuild,
-- which is a separate, owner-gated step.
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
                                -- "estimated".  Kept although nothing reads it
                                -- yet: it costs 1 byte on a captured row and it
                                -- is the only thing keeping a seeded estimate
                                -- out of `average_price`.
    volume           INTEGER,
    oi               INTEGER,
    day_open         REAL,
    day_high         REAL,
    day_low          REAL,
    prev_close       REAL,      -- quote.ohlc.close = previous session's close
    last_trade_time  TEXT,      -- KEPT: the Derivative tab's IV staleness gate
                                -- reads it (kanida_pilot/derivatives.py
                                -- `_last_trade_times`).  STORAGE_PLAN.md §5.3
                                -- lists it as droppable; that is wrong.
    source           TEXT    NOT NULL DEFAULT 'kite.quote',
    vendor_id        TEXT    NOT NULL,
    PRIMARY KEY (instrument_token, captured_at)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_snapshots_time ON snapshots(captured_at);
-- `ix_snapshots_snap` is gone with the column it indexed.  An existing database
-- still carries it (14.07 MB, measured); dropping it there is part of the same
-- owner-gated reclaim as the table rebuild.

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
    -- HOW the spot was obtained, because a reconstruction is not a measurement:
    --   kite.quote            the live capture's own NSE quote AT this mark;
    --   kite.candles_15m      the vendor's 15-minute bar close, fetched while
    --                         rebuilding a closed session from candles;
    --   market15.candles_15m  the same 15-minute bar close taken from OUR equity
    --                         store after the fact (db/market15.db).
    -- NULL means the source was not recorded.  It never means "captured".
    spot_source      TEXT,
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
-- only the intersection) can never silently drop a signal.
-- Retention: config.METRICS_DAYS (90 days since 2026-09-19; it was 365).
--
-- Twelve columns were retired on 2026-09-19 after every reader was traced:
--   price_change_15m, price_change_day  the tab refuses rupee moves outright
--                                       (kanida_pilot/derivatives.py:335, 339)
--   premium_status, floors_failed       written, never read
--   vol_tod_median, vol_oi_spike, oi_change_day_status, oi_change_pct_day_agg,
--   unusual_ce_strikes, unusual_pe_strikes, total_ce_volume, total_pe_volume
--                                       reach a `SELECT *` envelope and are
--                                       never subscripted out of it
-- They are still computed; they are simply not stored.  `headline`, `pcr_trend`
-- and `pcr_trend_change` were on that list and were put back: they cost 1.94,
-- 1.10 and 1.11 bytes a row and `market_data/tests/test_metrics.py` asserts
-- their values, so retiring them would have bought ~4 bytes for a contract
-- the suite says is real.
-- The list, and the reason each one qualified, is `metrics.RETIRED_METRIC_COLUMNS`.
--
-- What was NOT retired, against STORAGE_PLAN.md §5.4, because the readers are
-- there and the plan missed them:
--   tradingsymbol, underlying, instrument_type, strike, expiry, lot_size — the
--     plan calls these duplicates of `contracts` that the tab joins for.  The
--     join is only one of three query paths; the screener and the per-contract
--     session reader take all six straight from `metrics`.
--   vol_oi_status, vol_tod_status, unusual_reasons — served on every screener row.
--   floors_passed, unusual, basis_status, fut_oi_vs_avg_status, max_pain_status —
--     read; `floors_passed` is a WHERE clause, so dropping it would change a
--     result set rather than shrink a row.
--   pcr_*, total_ce_oi, total_pe_oi, max_pain_* — the plan calls these
--     "structurally unreachable".  They are reached, on the underlying-scope
--     rows, by the chain query that does no join at all.  Measured, they also
--     cost ~1.1 bytes a row because they are NULL on every contract row, so
--     dropping them would have saved almost nothing even if it were safe.
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
    -- carried straight off `underlying_snapshots.spot_source` for this mark, so
    -- a reader of the Derivative tab can tell a captured spot from a rebuilt one
    -- without joining back to the roll-up table.
    spot_source           TEXT,
    price_change_pct_15m  REAL,
    oi_change_15m         REAL,
    oi_change_pct_15m     REAL,
    buildup_15m           TEXT,
    price_change_pct_day  REAL,
    oi_change_day         REAL,
    oi_change_pct_day     REAL,
    buildup_day           TEXT,
    vol_tod_ratio         REAL,
    vol_tod_sessions      INTEGER,
    vol_tod_status        TEXT,
    vol_oi_ratio          REAL,
    vol_oi_prev_oi        REAL,
    vol_oi_status         TEXT,
    premium_rs            REAL,
    premium_cr            REAL,
    pcr_oi                REAL,
    pcr_volume            REAL,
    pcr_trend             TEXT,
    pcr_trend_change      REAL,
    total_ce_oi           REAL,
    total_pe_oi           REAL,
    max_pain_strike       REAL,
    max_pain_distance     REAL,
    max_pain_total_oi     REAL,
    max_pain_status       TEXT,
    fut_oi_avg            REAL,
    fut_oi_vs_avg         REAL,
    fut_oi_vs_avg_status  TEXT,
    basis                 REAL,
    basis_pct             REAL,
    basis_status          TEXT,
    contracts             INTEGER,
    floors_passed         INTEGER,
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
