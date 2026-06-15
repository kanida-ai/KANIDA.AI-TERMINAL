-- ============================================================================
-- taxonomy_columns.sql
-- falcon_pattern_taxonomy ADDITIONS — Step 1, self-improving engine.
--
-- The spec ("falcon_pattern_taxonomy — Extend, Do Not Replace") issues ONE
-- Postgres "ALTER TABLE ... ADD COLUMNS" with a multi-column list. SQLite has
-- NO multi-column ADD and NO "ADD COLUMN IF NOT EXISTS", so each column is
-- emitted as its own statement below and applied GUARDED (per-statement
-- try/except) by apply_schema.py — mirroring backend/power_user/db_init.py.
--
-- ADDITIVE ONLY. Never drop/rename/retype an existing column. The 865/1943
-- existing rows are never touched.
--
-- Type translation: BOOLEAN->INTEGER (0/1), FLOAT->REAL,
--   VARCHAR(n)/DATE->TEXT, INTEGER DEFAULT n / FLOAT DEFAULT n preserved.
--
-- Total: 37 new columns.
-- ============================================================================

-- Persona suitability (minimum 18 resolved trades each) — BOOLEAN -> INTEGER
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN swing_suitable INTEGER;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN positional_suitable INTEGER;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN longterm_suitable INTEGER;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN btst_suitable INTEGER;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN intraday_suitable INTEGER;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN short_suitable INTEGER;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN fo_only INTEGER;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN index_suitable INTEGER;

-- Journey intelligence
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN typical_peak_day REAL;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN typical_peak_interval TEXT;   -- VARCHAR(20)
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN peak_sustained_rate REAL;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN early_peak_rate REAL;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN late_peak_rate REAL;

-- Sector and regime attribution
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN quality_flag TEXT;            -- VARCHAR(30): STOCK_SPECIFIC_ALPHA / SECTOR_FOLLOWER / REGIME_SPECIFIC / INSUFFICIENT_DATA
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN win_rate_sector_tailwind REAL;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN win_rate_sector_headwind REAL;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN win_rate_sector_neutral REAL;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN regime_best_fit TEXT;         -- VARCHAR(20)
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN sector_regime_fit TEXT;

-- Signal timing
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN signal_validity_next_day_pct REAL;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN avg_signal_decay_days REAL;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN signal_best_entry_window TEXT;   -- VARCHAR(30)
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN intraday_detectable INTEGER;     -- BOOLEAN
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN intraday_detection_accuracy REAL;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN intraday_false_positive_rate REAL;

-- Big winner/loser rates per persona (JSON) + risk flag
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN big_winner_rate_by_persona TEXT;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN big_loser_rate_by_persona TEXT;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN big_loser_risk INTEGER;          -- BOOLEAN

-- Pattern maturity
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN pattern_maturity TEXT;           -- VARCHAR(25): insufficient_data / emerging / established / stable

-- Self-improvement (updated weekly)
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN realized_lift_60d REAL;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN weight_multiplier REAL DEFAULT 1.0;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN previous_multiplier REAL;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN status TEXT;                     -- VARCHAR(20)
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN last_classification_date TEXT;   -- DATE
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN classification_version INTEGER DEFAULT 0;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN n_resolved_trades_total INTEGER DEFAULT 0;
ALTER TABLE falcon_pattern_taxonomy ADD COLUMN n_resolved_trades_60d INTEGER DEFAULT 0;
