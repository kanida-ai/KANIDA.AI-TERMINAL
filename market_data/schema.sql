-- market_data / schema.sql
-- Schema for db/market15.db -- the 15-minute base store described in
-- docs/DATA_PIPELINE_CONTRACT.md section 2.
--
-- NOTE: db/kanida.db is NEVER written by this package. It is opened mode=ro.
-- Pragmas (WAL, busy_timeout, synchronous) are applied in store.py, not here.

-- ---------------------------------------------------------------------------
-- Base interval: 15 minutes. Revisioned; the latest revision for a
-- (instrument_id, bar_start) wins. Old revisions are never deleted.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS candles_15m (
    instrument_id       INTEGER NOT NULL,
    symbol              TEXT    NOT NULL,
    exchange            TEXT    NOT NULL DEFAULT 'NSE',
    bar_start           TEXT    NOT NULL,          -- 'YYYY-MM-DD HH:MM:SS' IST, naive
    bar_end             TEXT    NOT NULL,
    open                REAL    NOT NULL,
    high                REAL    NOT NULL,
    low                 REAL    NOT NULL,
    close               REAL    NOT NULL,
    volume              INTEGER NOT NULL,
    candle_complete     INTEGER NOT NULL DEFAULT 1,
    quality_flags       TEXT    NOT NULL DEFAULT '',
    adjustment_basis_id TEXT    NOT NULL,
    vendor_id           TEXT    NOT NULL,
    vendor_revision     TEXT,
    fetched_at          TEXT,
    snapshot_id         TEXT,
    revision            INTEGER NOT NULL DEFAULT 1,
    source_request_id   TEXT,                      -- -> raw_archive.request_id
    run_id              TEXT,                      -- -> ingest_runs.run_id
    PRIMARY KEY (instrument_id, bar_start, revision)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_candles15_sym_bar   ON candles_15m (symbol, bar_start);
CREATE INDEX IF NOT EXISTS idx_candles15_snapshot  ON candles_15m (snapshot_id);
CREATE INDEX IF NOT EXISTS idx_candles15_sym_rev   ON candles_15m (symbol, revision);
CREATE INDEX IF NOT EXISTS idx_candles15_flags     ON candles_15m (symbol, quality_flags)
    WHERE quality_flags <> '';

-- ---------------------------------------------------------------------------
-- Provenance the PIIND audit could not find: the raw vendor payload on disk.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_archive (
    request_id   TEXT PRIMARY KEY,
    provider     TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    timeframe    TEXT NOT NULL,
    start        TEXT NOT NULL,
    end          TEXT NOT NULL,
    fetched_at   TEXT NOT NULL,
    sha256       TEXT NOT NULL,
    payload_path TEXT NOT NULL,
    run_id       TEXT,
    rows         INTEGER
);

CREATE INDEX IF NOT EXISTS idx_raw_archive_sym ON raw_archive (symbol, timeframe, start);

-- ---------------------------------------------------------------------------
-- Every change to a stored value. Never an in-place silent edit.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corrections (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL,
    timeframe           TEXT NOT NULL,
    bar_start           TEXT NOT NULL,
    field               TEXT NOT NULL,
    old_value           TEXT,
    new_value           TEXT,
    reason              TEXT NOT NULL,
    evidence_request_id TEXT,                      -- -> raw_archive.request_id
    run_id              TEXT,
    old_revision        INTEGER,
    new_revision        INTEGER,
    created_at          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_corrections_sym ON corrections (symbol, bar_start);

-- ---------------------------------------------------------------------------
-- Frozen inputs for a research run.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id         TEXT PRIMARY KEY,
    created_at          TEXT NOT NULL,
    universe            TEXT NOT NULL,
    adjustment_basis_id TEXT,
    provider            TEXT,
    first_bar           TEXT,
    last_bar            TEXT,
    symbol_count        INTEGER,
    row_count           INTEGER,
    checksum            TEXT,
    status              TEXT NOT NULL DEFAULT 'open',   -- open | frozen
    frozen_at           TEXT,
    notes               TEXT
);

CREATE TABLE IF NOT EXISTS snapshot_members (
    snapshot_id   TEXT    NOT NULL,
    instrument_id INTEGER NOT NULL,
    symbol        TEXT    NOT NULL,
    bar_start     TEXT    NOT NULL,
    revision      INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, instrument_id, bar_start)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_snapmem_snapshot ON snapshot_members (snapshot_id);
-- NOTE: there is deliberately no (snapshot_id, symbol, bar_start) index. The
-- exclusions arrive keyed by symbol, but `symbol` is not in the PK, so a delete
-- by symbol is a full scan of the snapshot's ~28 M members -- per labelled bar.
-- `store.exclude_snapshot_rows` resolves symbol -> instrument_id first and
-- deletes on the PK instead, which is a seek and needs no second index over a
-- table this size.

-- What a snapshot deliberately does NOT contain, and why.  Rows the repair
-- labelled unusable (`vendor_zero_print`, `vendor_bad_print`, `wrong_instrument`,
-- `unresolved`) stay in `candles_15m` -- deleting them would be the silent edit
-- this store exists to prevent -- so a research snapshot has to drop them by
-- label.  A snapshot that silently dropped rows would be indistinguishable
-- from one with a coverage hole; this table is the difference.
CREATE TABLE IF NOT EXISTS snapshot_exclusions (
    snapshot_id TEXT    NOT NULL,
    label       TEXT    NOT NULL,
    rows        INTEGER NOT NULL,
    symbols     INTEGER NOT NULL,
    source      TEXT,                       -- where the label list came from
    note        TEXT,
    created_at  TEXT    NOT NULL,
    PRIMARY KEY (snapshot_id, label)
) WITHOUT ROWID;

-- ---------------------------------------------------------------------------
-- Ingest bookkeeping.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id      TEXT PRIMARY KEY,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    provider    TEXT,
    plan        TEXT,
    requests    INTEGER DEFAULT 0,
    rows        INTEGER DEFAULT 0,
    errors      INTEGER DEFAULT 0,
    status      TEXT NOT NULL DEFAULT 'running'
);

-- ---------------------------------------------------------------------------
-- Additions beyond contract section 2 (kept small and explicit):
--   quality_findings : structured validate.py output, so a flag can be traced
--                      back to the evidence that produced it.
--   seed_progress    : resumability for seed_from_legacy.py.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quality_findings (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id     TEXT,
    symbol     TEXT NOT NULL,
    timeframe  TEXT NOT NULL,
    bar_start  TEXT,
    code       TEXT NOT NULL,
    severity   TEXT NOT NULL,
    message    TEXT NOT NULL,
    evidence   TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_findings_sym  ON quality_findings (symbol, code);
CREATE INDEX IF NOT EXISTS idx_findings_run  ON quality_findings (run_id, severity);

CREATE TABLE IF NOT EXISTS seed_progress (
    symbol      TEXT PRIMARY KEY,
    run_id      TEXT,
    status      TEXT NOT NULL,        -- done | error | skipped
    rows        INTEGER DEFAULT 0,
    first_bar   TEXT,
    last_bar    TEXT,
    source_mix  TEXT,                 -- e.g. '5min=68120,1min=225'
    findings    INTEGER DEFAULT 0,
    error       TEXT,
    updated_at  TEXT NOT NULL
);

-- Session regimes (contract 2A). The expected bar count is per symbol AND per
-- date from 2026-08-03: a cash stock with F&O contracts stops continuous
-- trading at 15:15 (24 bars) because its close comes from the 15:30-15:35
-- auction. Derived empirically from the bars; `source`/`evidence` record how.
CREATE TABLE IF NOT EXISTS session_regimes (
    symbol         TEXT NOT NULL,
    effective_from TEXT NOT NULL,        -- 'YYYY-MM-DD'
    regime         TEXT NOT NULL,        -- regular | cas
    source         TEXT NOT NULL DEFAULT 'observed',
    evidence       TEXT,
    run_id         TEXT,
    updated_at     TEXT NOT NULL,
    PRIMARY KEY (symbol, effective_from)
) WITHOUT ROWID;

-- Symbols the provider cannot serve at all (not in its instrument list,
-- delisted, renamed, invisible to this account).  They are NOT a data error we
-- can fix by fetching harder, and left in the live universe every cycle logs
-- an error for each of them, which makes the app's data panel show a warning
-- that is about us, not about the data.
--
-- This is the queryable index of the decision; the decision itself is still
-- recorded the way the repair pass recorded it, as a `corrections` row with
-- `field='quarantine_status'`, so there is one audit trail and not two.
-- `last_checked` is what makes the quarantine self-healing: the live loop
-- re-probes a quarantined symbol once a day and releases it the moment the
-- provider starts serving it.
CREATE TABLE IF NOT EXISTS quarantine (
    symbol       TEXT NOT NULL PRIMARY KEY,
    reason       TEXT NOT NULL,          -- machine-readable class
    detail       TEXT,                   -- the measured evidence, in words
    status       TEXT NOT NULL DEFAULT 'quarantined',   -- quarantined | released
    first_seen   TEXT NOT NULL,
    last_checked TEXT NOT NULL,
    checks       INTEGER NOT NULL DEFAULT 1,
    last_error   TEXT,
    released_at  TEXT,
    provider     TEXT,
    run_id       TEXT
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_quarantine_status ON quarantine (status);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
