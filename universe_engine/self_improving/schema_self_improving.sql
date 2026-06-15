-- ============================================================================
-- schema_self_improving.sql
-- Self-Improving Multi-Persona Trading Intelligence System — Step 1 schema.
--
-- SQLite DDL, ADDITIVE ONLY. Faithful Postgres->SQLite translation of the spec
-- "Database Tables — Complete Schema". These tables live in the RND research DB
-- (SQLite for now; Postgres is Phase 4 per STEP0_AUDIT.md §E).
--
-- Translation rules applied (see S1-build-log.md):
--   BIGSERIAL PRIMARY KEY        -> INTEGER PRIMARY KEY AUTOINCREMENT
--   BOOLEAN                      -> INTEGER         (0/1)
--   FLOAT                        -> REAL
--   VARCHAR(n) / DATE / TIME     -> TEXT
--   INTEGER[] / TEXT[] / JSONB   -> TEXT            (store JSON)
--   TIMESTAMP DEFAULT NOW()      -> TEXT DEFAULT (datetime('now'))
--   BIGINT  (FK ref columns)     -> INTEGER         (matches AUTOINCREMENT PK)
--   INTEGER DEFAULT n            -> INTEGER DEFAULT n  (preserved)
--   FLOAT   DEFAULT n            -> REAL    DEFAULT n  (preserved)
--   UNIQUE(...) / FOREIGN KEY(...) REFERENCES ... -> preserved verbatim
--
-- Persona rule: every table MUST have a `persona` column. Two spec tables
--   (index_expiry_calendar, fo_stock_master) lacked one; `persona TEXT` was
--   ADDED to each (flagged in the build log).
--
-- The falcon_pattern_taxonomy ALTER columns are NOT here — they live in
--   taxonomy_columns.sql (SQLite has no multi-column ADD; applied guarded by
--   apply_schema.py because SQLite has no ADD COLUMN IF NOT EXISTS).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. falcon_baseline_trades
--    Per-trade record incl. full intra-hold journey (open/high/low/close/day).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS falcon_baseline_trades (
  id                            INTEGER PRIMARY KEY AUTOINCREMENT,
  persona                       TEXT NOT NULL DEFAULT 'falcon_top10',
  signal_date                   TEXT NOT NULL,
  entry_date                    TEXT NOT NULL,
  exit_date                     TEXT,
  symbol                        TEXT NOT NULL,
  stock_name                    TEXT,
  sector                        TEXT,
  engine_rank                   INTEGER,
  avg_lift                      REAL,
  n_fires                       INTEGER,
  sum_lift                      REAL,
  top_3_pattern_ids             TEXT,   -- INTEGER[] -> JSON array
  pattern_mined_years_used      TEXT,   -- INTEGER[] -> JSON array
  entry_price                   REAL,
  exit_price                    REAL,
  exit_reason                   TEXT,   -- TIME_STOP / INIT_STOP / TRAIL_GIVEBACK
  hold_days_calendar            INTEGER,
  hold_days_trading             INTEGER,
  shares                        INTEGER,
  actual_deployed               REAL,
  gross_pnl                     REAL,
  fees                          REAL,
  net_pnl                       REAL,
  net_ret_pct                   REAL,
  -- INTRA-HOLD JOURNEY (open, high, low, close per day)
  d1_open_ret REAL, d1_high_ret REAL, d1_low_ret REAL, d1_close_ret REAL,
  d2_open_ret REAL, d2_high_ret REAL, d2_low_ret REAL, d2_close_ret REAL,
  d3_open_ret REAL, d3_high_ret REAL, d3_low_ret REAL, d3_close_ret REAL,
  d4_open_ret REAL, d4_high_ret REAL, d4_low_ret REAL, d4_close_ret REAL,
  d5_open_ret REAL, d5_high_ret REAL, d5_low_ret REAL, d5_close_ret REAL,
  d6_open_ret REAL, d6_high_ret REAL, d6_low_ret REAL, d6_close_ret REAL,
  d7_open_ret REAL, d7_high_ret REAL, d7_low_ret REAL, d7_close_ret REAL,
  -- Journey summary
  peak_ret_during_hold          REAL,
  peak_day_during_hold          INTEGER,
  trough_ret_during_hold        REAL,
  trough_day_during_hold        INTEGER,
  peak_before_trough            INTEGER,   -- BOOLEAN
  trough_before_peak            INTEGER,   -- BOOLEAN
  peak_sustained                INTEGER,   -- BOOLEAN
  -- Intraday (15-min intervals as JSON, persona='intraday' only)
  intraday_intervals_json       TEXT,
  peak_ret_during_session       REAL,
  peak_time_during_session      TEXT,      -- TIME
  trough_ret_during_session     REAL,
  trough_time_during_session    TEXT,      -- TIME
  -- Big winner / big loser flags (per persona thresholds)
  big_winner_flag               INTEGER,   -- BOOLEAN
  big_loser_flag                INTEGER,   -- BOOLEAN
  big_winner_peak_day           INTEGER,
  big_loser_trough_day          INTEGER,
  big_winner_sustained          INTEGER,   -- BOOLEAN
  big_loser_recovered           INTEGER,   -- BOOLEAN
  -- Sector and market regime
  sector_rank_on_signal_date    INTEGER,
  sector_20d_rs                 REAL,
  sector_tailwind               INTEGER,   -- BOOLEAN
  n_sector_peers_in_top10       INTEGER,
  market_regime_on_signal_date  TEXT,
  sector_regime_on_signal_date  TEXT,
  -- Sector attribution
  sector_ret_same_period        REAL,
  stock_vs_sector               REAL,
  move_type                     TEXT,      -- STOCK_LED / SECTOR_DRIVEN / SECTOR_HEADWIND / SECTOR_DRAGGED / STOCK_WEAKNESS
  sector_ret_post_hold          REAL,
  stock_vs_sector_post_hold     REAL,
  post_hold_move_type           TEXT,
  -- Signal timing
  signal_still_valid_at_open    INTEGER,   -- BOOLEAN
  signal_validity_days          INTEGER,
  entry_timing_quality          TEXT,      -- FRESH / STALE / DEGRADED
  intraday_confirmed            INTEGER,   -- BOOLEAN
  intraday_confirmation_time    TEXT,      -- TIME
  intraday_vs_eod_delta         REAL,
  -- Repeater intelligence
  prior_appearances_30d         INTEGER,
  prior_appearances_60d         INTEGER,
  consecutive_days_in_top50     INTEGER,
  last_outcome_when_repeated    REAL,
  repeater_type                 TEXT,      -- FRESH / HEALTHY_REPEATER / EXTENDED_TREND / STALE / PERSISTENCE_TRAP
  -- Self-improved ranking (Phase 2, null during Phase 1)
  improved_score                REAL,
  improved_rank                 INTEGER,
  pattern_weight_multiplier     REAL,
  sector_multiplier             REAL,
  repeater_multiplier           REAL,
  rank_change                   INTEGER,
  rank_change_reason            TEXT,
  -- Auto-trade readiness
  autotrade_decision            TEXT,
  autotrade_block_reason        TEXT,
  created_at                    TEXT DEFAULT (datetime('now'))
);

-- ----------------------------------------------------------------------------
-- 2. falcon_pattern_contributions
--    Which patterns contributed to each trade + each pattern's realized outcome.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS falcon_pattern_contributions (
  id                   INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id             INTEGER REFERENCES falcon_baseline_trades(id),
  signal_date          TEXT NOT NULL,
  symbol               TEXT NOT NULL,
  persona              TEXT NOT NULL,
  pattern_id           INTEGER NOT NULL,
  pattern_mined_year   INTEGER NOT NULL,
  regime               TEXT,
  lift_pp              REAL,
  oos_hit_rate         REAL,
  realized_outcome     REAL,
  sector               TEXT,
  move_type            TEXT,
  market_regime        TEXT,
  sector_regime        TEXT,
  big_winner_flag      INTEGER,   -- BOOLEAN
  big_loser_flag       INTEGER,   -- BOOLEAN
  created_at           TEXT DEFAULT (datetime('now'))
);

-- ----------------------------------------------------------------------------
-- 3. falcon_post_exit_tracking  (SEPARATE CHILD TABLE)
--    Post-exit data arrives incrementally over D+60 / W+26. Update per arrival.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS falcon_post_exit_tracking (
  id                        INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id                  INTEGER REFERENCES falcon_baseline_trades(id),
  symbol                    TEXT NOT NULL,
  exit_date                 TEXT NOT NULL,
  persona                   TEXT,
  d1_close REAL, d1_ret_pct REAL,
  d3_close REAL, d3_ret_pct REAL,
  d7_close REAL, d7_ret_pct REAL,
  d14_close REAL, d14_ret_pct REAL,
  d21_close REAL, d21_ret_pct REAL,
  d30_close REAL, d30_ret_pct REAL,
  d45_close REAL, d45_ret_pct REAL,
  d60_close REAL, d60_ret_pct REAL,
  w1_close REAL, w1_ret_pct REAL,
  w2_close REAL, w2_ret_pct REAL,
  w4_close REAL, w4_ret_pct REAL,
  w8_close REAL, w8_ret_pct REAL,
  w12_close REAL, w12_ret_pct REAL,
  w26_close REAL, w26_ret_pct REAL,
  peak_close_d60            REAL,
  peak_date_d60             TEXT,      -- DATE
  early_exit_flag           INTEGER,   -- BOOLEAN
  extended_trend_candidate  INTEGER,   -- BOOLEAN
  sector_ret_d60            REAL,
  stock_vs_sector_d60       REAL,
  updated_at                TEXT DEFAULT (datetime('now'))
);

-- ----------------------------------------------------------------------------
-- 4. falcon_near_miss_tracking
--    Ranks 11-50 — stores INDIVIDUAL day returns for hidden-gem analysis.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS falcon_near_miss_tracking (
  id                      INTEGER PRIMARY KEY AUTOINCREMENT,
  signal_date             TEXT NOT NULL,
  persona                 TEXT NOT NULL,
  symbol                  TEXT NOT NULL,
  sector                  TEXT,
  engine_rank             INTEGER,
  rank_bucket             TEXT,      -- '11-20' or '21-50'
  avg_lift                REAL,
  n_fires                 INTEGER,
  -- Individual day returns (NOT just summary — needed for hidden gem analysis)
  actual_d7_ret           REAL,
  actual_d14_ret          REAL,
  actual_d21_ret          REAL,
  actual_d30_ret          REAL,
  actual_d60_ret          REAL,
  -- Journey summary
  peak_ret_during_hold    REAL,
  trough_ret_during_hold  REAL,
  close_ret_at_hold_end   REAL,
  post_hold_high_ret      REAL,
  -- Sector attribution
  sector_ret_same_period  REAL,
  stock_vs_sector         REAL,
  move_type               TEXT,
  outperformed_top10      INTEGER,   -- BOOLEAN
  big_winner_flag         INTEGER,   -- BOOLEAN
  hidden_gem_flag         INTEGER,   -- BOOLEAN
  created_at              TEXT DEFAULT (datetime('now'))
);

-- ----------------------------------------------------------------------------
-- 5. falcon_signal_validity
--    Signal freshness / decay + intraday-vs-EOD detection comparison.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS falcon_signal_validity (
  id                              INTEGER PRIMARY KEY AUTOINCREMENT,
  signal_date                     TEXT NOT NULL,
  symbol                          TEXT NOT NULL,
  persona                         TEXT NOT NULL,
  engine_rank                     INTEGER,
  avg_lift                        REAL,
  valid_at_next_open              INTEGER,   -- BOOLEAN
  valid_at_next_10am              INTEGER,   -- BOOLEAN
  valid_at_next_11am              INTEGER,   -- BOOLEAN
  valid_at_next_1pm               INTEGER,   -- BOOLEAN
  n_days_signal_remained_valid    INTEGER,
  detected_intraday               INTEGER,   -- BOOLEAN
  intraday_detection_time         TEXT,      -- TIME
  intraday_entry_price            REAL,
  next_day_open_price             REAL,
  entry_price_delta_pct           REAL,
  intraday_early_entry_outcome    REAL,
  eod_signal_outcome              REAL,
  intraday_false_positive         INTEGER,   -- BOOLEAN
  false_positive_reason           TEXT,
  created_at                      TEXT DEFAULT (datetime('now'))
);

-- ----------------------------------------------------------------------------
-- 6. falcon_big_winner_loser_study
--    Weekly per-(pattern,persona) HFCL-disciplined big winner/loser study.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS falcon_big_winner_loser_study (
  id                            INTEGER PRIMARY KEY AUTOINCREMENT,
  pattern_id                    INTEGER NOT NULL,
  persona                       TEXT NOT NULL,
  week_ending                   TEXT NOT NULL,
  n_total_occurrences           INTEGER,
  n_big_winners                 INTEGER,
  n_big_losers                  INTEGER,
  n_neutral                     INTEGER,
  big_winner_rate               REAL,
  avg_peak_ret_when_winner      REAL,
  median_peak_ret_when_winner   REAL,
  avg_peak_day_when_winner      REAL,
  peak_sustained_rate           REAL,
  sector_tailwind_rate_winners  REAL,
  stock_led_rate_winners        REAL,
  big_loser_rate                REAL,
  avg_trough_ret_when_loser     REAL,
  avg_trough_day_when_loser     REAL,
  recovery_rate_after_trough    REAL,
  stop_hit_rate                 REAL,
  sector_headwind_rate_losers   REAL,
  stock_weakness_rate_losers    REAL,
  expected_value_at_hold_end    REAL,
  expected_value_at_peak        REAL,
  big_loser_risk                INTEGER,   -- BOOLEAN
  big_winner_candidate          INTEGER,   -- BOOLEAN
  pattern_maturity              TEXT,      -- insufficient_data / emerging / established / stable
  UNIQUE (pattern_id, persona, week_ending)
);

-- ----------------------------------------------------------------------------
-- 7. falcon_pattern_weekly_state
--    Weekly snapshot of each pattern's realized state + weight multiplier.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS falcon_pattern_weekly_state (
  id                        INTEGER PRIMARY KEY AUTOINCREMENT,
  week_ending               TEXT NOT NULL,
  pattern_id                INTEGER NOT NULL,
  persona                   TEXT NOT NULL,
  n_resolved_trades_60d     INTEGER,
  realized_lift_60d         REAL,
  oos_lift_at_mining        REAL,
  win_rate_60d              REAL,
  stop_rate_60d             REAL,
  win_rate_sector_tailwind  REAL,
  win_rate_sector_headwind  REAL,
  big_winner_rate           REAL,
  big_loser_rate            REAL,
  avg_peak_day              REAL,
  weight_multiplier         REAL DEFAULT 1.0,
  previous_multiplier       REAL,
  multiplier_change         REAL,
  status                    TEXT,
  status_change             INTEGER,   -- BOOLEAN
  status_change_reason      TEXT,
  human_approved            INTEGER DEFAULT 0,   -- BOOLEAN DEFAULT FALSE
  approved_by               TEXT,
  approved_at               TEXT,                -- TIMESTAMP
  UNIQUE (week_ending, pattern_id, persona)
);

-- ----------------------------------------------------------------------------
-- 8. falcon_weekly_review_log
--    Weekly human-review report. Many TEXT[] columns -> JSON arrays.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS falcon_weekly_review_log (
  id                          INTEGER PRIMARY KEY AUTOINCREMENT,
  week_ending                 TEXT NOT NULL,
  persona                     TEXT,
  patterns_promoted           INTEGER,
  patterns_demoted            INTEGER,
  patterns_disabled           INTEGER,
  new_healthy_repeaters       TEXT,   -- TEXT[] -> JSON array
  new_persistence_traps       TEXT,   -- TEXT[] -> JSON array
  new_early_exit_flags        TEXT,   -- TEXT[] -> JSON array
  new_big_winner_candidates   TEXT,   -- TEXT[] -> JSON array
  new_big_loser_risk_patterns TEXT,   -- TEXT[] -> JSON array
  sector_regime_shifts        TEXT,   -- TEXT[] -> JSON array
  market_regime_shifts        TEXT,   -- TEXT[] -> JSON array
  quality_flag_changes        TEXT,   -- TEXT[] -> JSON array
  signal_validity_changes     TEXT,   -- TEXT[] -> JSON array
  constitutional_violations   TEXT,   -- TEXT[] -> JSON array
  summary_text                TEXT,
  review_status               TEXT DEFAULT 'PENDING',
  reviewed_at                 TEXT,                       -- TIMESTAMP
  created_at                  TEXT DEFAULT (datetime('now'))
);

-- ----------------------------------------------------------------------------
-- 9. index_expiry_calendar
--    Spec DDL had NO persona column; `persona TEXT` ADDED per the persona rule.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS index_expiry_calendar (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  index_name    TEXT NOT NULL,   -- 'NIFTY' / 'BANKNIFTY'
  expiry_date   TEXT NOT NULL,
  expiry_type   TEXT NOT NULL,   -- 'weekly' / 'monthly'
  expiry_day    TEXT,
  lot_size      INTEGER,
  persona       TEXT,            -- ADDED (not in spec DDL) — persona rule
  UNIQUE (index_name, expiry_date)
);

-- ----------------------------------------------------------------------------
-- 10. fo_stock_master
--     Spec DDL had NO persona column; `persona TEXT` ADDED per the persona rule.
--     PK is `symbol` (VARCHAR PRIMARY KEY -> TEXT PRIMARY KEY) — no AUTOINCREMENT.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fo_stock_master (
  symbol            TEXT PRIMARY KEY,
  stock_name        TEXT,
  lot_size          INTEGER,
  margin_span       REAL,
  margin_exposure   REAL,
  total_margin      REAL,
  fo_eligible       INTEGER DEFAULT 1,   -- BOOLEAN DEFAULT TRUE
  as_of_date        TEXT NOT NULL,
  persona           TEXT,                -- ADDED (not in spec DDL) — persona rule
  created_at        TEXT DEFAULT (datetime('now'))
);
