-- ============================================================================
-- schema_signal_day_study.sql
-- Self-Improving Engine — S2B: per-signal-day efficacy study.
--
-- SQLite DDL, ADDITIVE ONLY. Self-contained CREATE TABLE IF NOT EXISTS for
-- falcon_signal_day_study. Lives in the RND research DB (SQLite).
--
-- WHAT THIS TABLE IS (vs falcon_baseline_trades):
--   falcon_baseline_trades  = a MANAGED-PORTFOLIO sim (fixed ₹5L/yr, cash-
--     constrained, skip_already_held=True via held_syms). Each year is one
--     wallet; a held symbol is NOT re-entered while open.
--   falcon_signal_day_study = a PER-SIGNAL-DAY EFFICACY study. We take the
--     engine's top-10 picks on EVERY signal day and simulate EACH pick
--     INDEPENDENTLY with REPLICATED capital (₹50k per pick), NO skip_already_held,
--     NO cash constraint. The SAME stock can appear on many signal days → many
--     independent rows (duplicates expected). It answers: "if a customer acts on
--     a pick on ANY given day, what happens to that stock + its outcome — and
--     what happens AFTER our exit (D+8 → D+60)."
--
-- The exit MECHANICS are byte-for-byte the engine's (₹50k notional, integer
-- shares, next-open entry, 7-day hold, −7% init stop, +12% trail = max(entry,
-- 10d-low) Donchian, gap-down honored, ×(1−5bps) slippage, 30bps fee). They are
-- REUSED from persona_engine_core.simulate_year — not re-implemented. A parity
-- cross-check in build_signal_day_study.py asserts that for any (signal_date,
-- symbol) shared with falcon_baseline_trades the outcomes match EXACTLY.
--
-- Translation rules identical to schema_self_improving.sql (BOOLEAN -> INTEGER
-- 0/1, FLOAT -> REAL, DATE -> TEXT, INTEGER[] -> TEXT JSON, NOW() ->
-- datetime('now')). Applied with CREATE TABLE IF NOT EXISTS by the script
-- itself before any write (SQLite has no DDL-IF-EXISTS guard issue here).
-- ============================================================================

CREATE TABLE IF NOT EXISTS falcon_signal_day_study (
  -- identity
  id                            INTEGER PRIMARY KEY AUTOINCREMENT,
  persona                       TEXT NOT NULL DEFAULT 'falcon_top10_daily',
  signal_date                   TEXT NOT NULL,
  entry_date                    TEXT NOT NULL,
  exit_date                     TEXT,
  symbol                        TEXT NOT NULL,
  sector                        TEXT,
  engine_rank                   INTEGER,   -- pick's rank within its signal_date cohort (1..top_n)
  avg_lift                      REAL,
  n_fires                       INTEGER,
  sum_lift                      REAL,
  -- execution (engine-reused mechanics — see build_signal_day_study.py)
  entry_price                   REAL,
  exit_price                    REAL,
  exit_reason                   TEXT,      -- TIME_STOP / INIT_STOP / TRAIL_GIVEBACK
  hold_days_trading             INTEGER,
  shares                        INTEGER,
  net_pnl                       REAL,
  net_ret_pct                   REAL,
  -- intra-hold journey (same definitions as falcon_baseline_trades)
  d1_open_ret REAL, d1_high_ret REAL, d1_low_ret REAL, d1_close_ret REAL,
  d2_open_ret REAL, d2_high_ret REAL, d2_low_ret REAL, d2_close_ret REAL,
  d3_open_ret REAL, d3_high_ret REAL, d3_low_ret REAL, d3_close_ret REAL,
  d4_open_ret REAL, d4_high_ret REAL, d4_low_ret REAL, d4_close_ret REAL,
  d5_open_ret REAL, d5_high_ret REAL, d5_low_ret REAL, d5_close_ret REAL,
  d6_open_ret REAL, d6_high_ret REAL, d6_low_ret REAL, d6_close_ret REAL,
  d7_open_ret REAL, d7_high_ret REAL, d7_low_ret REAL, d7_close_ret REAL,
  peak_ret_during_hold          REAL,
  peak_day_during_hold          INTEGER,
  trough_ret_during_hold        REAL,
  trough_day_during_hold        INTEGER,
  peak_before_trough            INTEGER,   -- BOOLEAN
  trough_before_peak            INTEGER,   -- BOOLEAN
  peak_sustained                INTEGER,   -- BOOLEAN
  big_winner_flag               INTEGER,   -- BOOLEAN
  big_loser_flag                INTEGER,   -- BOOLEAN
  -- POST-ENTRY "what happened next" (the key addition). All vs entry_price
  -- (close/entry_px − 1, in %). NULL where that future bar has not arrived yet
  -- (NEVER imputed).
  d8_close_ret                  REAL,
  d10_close_ret                 REAL,
  d15_close_ret                 REAL,
  d20_close_ret                 REAL,
  d30_close_ret                 REAL,
  d45_close_ret                 REAL,
  d60_close_ret                 REAL,
  post_hold_high_ret            REAL,      -- max close_ret over D+8..D+60 (available bars)
  post_hold_peak_day            INTEGER,   -- the D (8..60) at which post_hold_high_ret occurred
  early_exit_flag               INTEGER,   -- BOOLEAN: 1 if post_hold_high_ret > 15% (stock kept running after exit)
  kept_running_d30              INTEGER,   -- BOOLEAN: 1 if d30_close_ret > net_ret_pct
  -- POST-HOLD INTRADAY HIGH/LOW + CIRCUIT (S2C addition). High/low vs entry_price
  -- at each post-hold offset; the TRUE intraday post-exit peak/trough scanning the
  -- full D+8..D+60 window; and the largest single-day close-over-prev-close move
  -- across the FULL path (entry→last available bar) with circuit-day flag. The
  -- close-based cols above are KEPT UNCHANGED for continuity — these are additive.
  -- All vs entry_price (high/entry_px−1, low/entry_px−1, in %). NULL where the bar
  -- has not arrived (NEVER imputed).
  d8_high_ret                   REAL,  d8_low_ret  REAL,
  d10_high_ret                  REAL,  d10_low_ret REAL,
  d15_high_ret                  REAL,  d15_low_ret REAL,
  d20_high_ret                  REAL,  d20_low_ret REAL,
  d30_high_ret                  REAL,  d30_low_ret REAL,
  d45_high_ret                  REAL,  d45_low_ret REAL,
  d60_high_ret                  REAL,  d60_low_ret REAL,
  post_hold_peak_high_ret       REAL,      -- MAX(high/entry_px−1) over D+8..D+60 available bars (TRUE intraday post-exit peak)
  post_hold_peak_high_day       INTEGER,   -- the D (8..60) at which post_hold_peak_high_ret occurred
  post_hold_trough_low_ret      REAL,      -- MIN(low/entry_px−1) over D+8..D+60 available bars (intraday post-exit trough)
  max_up_day_move_pct           REAL,      -- largest single-day close/prev_close−1 (%) over FULL path entry→last bar (captures circuit days in hold AND post-hold)
  max_up_day_date               TEXT,      -- date of that max single-day up move
  hit_circuit_flag              INTEGER,   -- BOOLEAN: 1 if max_up_day_move_pct >= 19.5 (~20% upper circuit)
  -- dup context
  prior_appearances_30d         INTEGER,   -- # times this symbol was a pick in the prior 30 calendar days
  created_at                    TEXT DEFAULT (datetime('now'))
);

-- Helpful (additive) indexes for the audit agent's queries. IF NOT EXISTS keeps
-- this idempotent and additive.
CREATE INDEX IF NOT EXISTS idx_fsds_persona_sd_sym
  ON falcon_signal_day_study (persona, signal_date, symbol);
CREATE INDEX IF NOT EXISTS idx_fsds_symbol
  ON falcon_signal_day_study (symbol);

-- ============================================================================
-- S2C ADDITIVE ALTERs — post-hold intraday high/low + circuit detection.
--
-- CREATE TABLE IF NOT EXISTS (above) only creates these columns on a FRESH DB;
-- it will NOT add them to a falcon_signal_day_study table that already exists
-- from a prior S2B build. SQLite has no "ADD COLUMN IF NOT EXISTS", so the
-- builder parses the ALTER statements below and applies them GUARDED (skipping
-- columns already present via PRAGMA table_info), mirroring apply_schema.py's
-- taxonomy-column applier. Re-running is a no-op. Never drops/retypes a column.
--
-- These mirror, 1:1, the new columns in the CREATE TABLE above, so a fresh DB
-- (cols already created) and an existing DB (cols added here) converge to the
-- same schema. The close-based post-hold columns are NOT touched (continuity).
-- ============================================================================
ALTER TABLE falcon_signal_day_study ADD COLUMN d8_high_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN d8_low_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN d10_high_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN d10_low_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN d15_high_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN d15_low_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN d20_high_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN d20_low_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN d30_high_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN d30_low_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN d45_high_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN d45_low_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN d60_high_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN d60_low_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN post_hold_peak_high_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN post_hold_peak_high_day INTEGER;
ALTER TABLE falcon_signal_day_study ADD COLUMN post_hold_trough_low_ret REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN max_up_day_move_pct REAL;
ALTER TABLE falcon_signal_day_study ADD COLUMN max_up_day_date TEXT;
ALTER TABLE falcon_signal_day_study ADD COLUMN hit_circuit_flag INTEGER;
